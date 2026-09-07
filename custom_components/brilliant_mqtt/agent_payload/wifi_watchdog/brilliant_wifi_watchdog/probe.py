"""Connectivity probes (stdlib). Gateway derived from the routing table, never hardcoded."""

from __future__ import annotations

import enum
import errno
import socket
import sys
import time
from collections.abc import Callable
from typing import Any

from . import bounded

# errnos that mean a LOCAL resource ran out (fd/buffer/memory exhaustion), not a
# broker refusal — a connect failing with one of these proves nothing about the
# broker, so it must read as INCONCLUSIVE rather than a conclusive "down".
_LOCAL_ERRNOS = frozenset({errno.EMFILE, errno.ENFILE, errno.ENOBUFS, errno.ENOMEM})

# Wall-clock bound for a single probe child. `ip route`/`ping` are local and
# fast; the bound guards the pathological case where the child never returns
# (a hung network stack), which would otherwise wedge the watchdog loop.
_PROBE_TIMEOUT = 5.0


def _run_probe(argv: list[str], capture: bool) -> bounded.Completed:
    return bounded.run_bounded(argv, timeout=_PROBE_TIMEOUT, capture=capture)


def _run_out(argv: list[str]) -> tuple[int, str]:
    r = bounded.run_bounded(argv, timeout=_PROBE_TIMEOUT, capture=True)
    return r.returncode, r.stdout


def _run_rc(argv: list[str]) -> int:
    return bounded.run_bounded(argv, timeout=_PROBE_TIMEOUT).returncode


def _parse_default_gateway(out: str) -> str | None:
    for line in out.splitlines():
        parts = line.split()
        if "via" in parts:
            idx = parts.index("via") + 1
            if idx < len(parts):  # guard: "default via" with no following token
                return parts[idx]
    return None


def default_gateway(run: Callable[[list[str]], tuple[int, str]] = _run_out) -> str | None:
    rc, out = run(["ip", "route", "show", "default"])
    if rc != 0:
        return None
    return _parse_default_gateway(out)


def ping(host: str, run: Callable[[list[str]], int] = _run_rc) -> bool:
    return run(["ping", "-c", "1", "-W", "2", host]) == 0


class TcpProbe(enum.Enum):
    """Conclusive or inconclusive result from a bounded connectivity probe."""

    OPEN = "open"  # the requested connectivity check succeeded
    CLOSED = "closed"  # the check completed and failed conclusively, in budget
    INCONCLUSIVE = "inconclusive"  # the local check could not complete — proves nothing


_GatewayRun = Callable[[list[str], bool], bounded.Completed]


def _completed_state(result: bounded.Completed) -> TcpProbe:
    if result.timed_out:
        return TcpProbe.INCONCLUSIVE
    return TcpProbe.OPEN if result.returncode == 0 else TcpProbe.CLOSED


def gateway_probe(
    configured_gateway: str | None,
    *,
    run: _GatewayRun = _run_probe,
) -> tuple[str | None, TcpProbe]:
    """Discover and ping the gateway without losing timeout information."""
    gateway = configured_gateway
    if gateway is None:
        route = run(["ip", "route", "show", "default"], True)
        route_state = _completed_state(route)
        if route_state != TcpProbe.OPEN:
            return None, route_state
        gateway = _parse_default_gateway(route.stdout)
        if gateway is None:
            return None, TcpProbe.CLOSED

    ping_result = run(["ping", "-c", "1", "-W", "2", gateway], False)
    return gateway, _completed_state(ping_result)


_AddrInfo = tuple[Any, ...]
_Resolve = Callable[[str, int, float], "list[_AddrInfo] | None"]
_Connect = Callable[[int, int, int, Any, float], None]

# A dependency-free child that resolves a hostname to its IP addresses. It runs
# as a killed-and-reaped subprocess (see _resolve_bounded) precisely because a
# stuck getaddrinfo cannot be aborted in-process: glibc runs the resolver in C
# with the GIL released and retries poll/recvfrom on EINTR, so a Python SIGALRM
# only fires at the next bytecode boundary — after the C call finally returns on
# glibc's own (>=10s, or unbounded with a wedged NSS backend) timeout. Killing
# the child aborts that C call; a child (not a daemon thread we would have to
# abandon) leaks neither a thread nor a process once reaped.
_RESOLVER_SCRIPT = (
    "import socket, sys\n"
    "try:\n"
    "    infos = socket.getaddrinfo("
    "sys.argv[1], int(sys.argv[2]), socket.AF_UNSPEC, socket.SOCK_STREAM)\n"
    "except OSError:\n"
    "    sys.exit(3)\n"
    "seen = set()\n"
    "for info in infos:\n"
    "    ip = info[4][0]\n"
    "    if ip not in seen:\n"
    "        seen.add(ip)\n"
    "        print(ip)\n"
)


def _numeric_addrinfo(host: str, port: int) -> list[_AddrInfo] | None:
    """``getaddrinfo`` for a NUMERIC host/port only, so it never touches DNS and
    never blocks. Returns the addrinfo list for an IP literal, or None if *host*
    is a name that needs resolving."""
    try:
        return socket.getaddrinfo(
            host,
            port,
            socket.AF_UNSPEC,
            socket.SOCK_STREAM,
            flags=socket.AI_NUMERICHOST | socket.AI_NUMERICSERV,
        )
    except socket.gaierror:
        return None


def _resolve_bounded(
    host: str,
    port: int,
    budget: float,
    *,
    run: Callable[..., bounded.Completed] = bounded.run_bounded,
) -> list[_AddrInfo] | None:
    """Resolve *host* to connectable addrinfos within *budget* seconds.

    An IP literal is resolved in-process (numeric, no DNS, no child). A hostname
    is resolved in a bounded, killed-and-reaped child so a stuck DNS query cannot
    block the caller past the deadline and leaves nothing behind; each returned
    IP is then rebuilt numerically (instant) into a connectable addrinfo. Returns
    None when the budget is exhausted, the child is killed, or resolution fails —
    all of which the caller treats as INCONCLUSIVE (never proof the broker is down).
    """
    numeric = _numeric_addrinfo(host, port)
    if numeric is not None:
        return numeric
    if budget <= 0:
        return None
    try:
        # -I -S: isolated, no site — the child ignores the panel's PYTHONPATH,
        # cwd, PYTHON* env and any .pth/sitecustomize, so a stray module cannot
        # corrupt this dependency-free probe (and startup is cheaper under CPUQuota).
        result = run(
            [sys.executable, "-I", "-S", "-c", _RESOLVER_SCRIPT, host, str(port)],
            timeout=budget,
            capture=True,
        )
    except OSError:
        # Fork/exec failure (EAGAIN/ENOMEM under MemoryMax/pids pressure, or a
        # missing/unexecutable interpreter). The optional diagnostic must never
        # crash the poll loop — treat it as inconclusive.
        return None
    if result.timed_out or result.returncode != 0:
        return None  # stuck DNS child killed+reaped, or resolution failed
    infos: list[_AddrInfo] = []
    for ip in result.stdout.split():
        rebuilt = _numeric_addrinfo(ip, port)
        if rebuilt is not None:
            infos.extend(rebuilt)
    # Try IPv4 candidates first. getaddrinfo often returns AAAA before A, and a
    # home router advertising an IPv6 prefix with no routed upstream would else
    # spend the first attempt (and its budget slice) on a blackholed v6 before the
    # reachable IPv4 broker. AI_ADDRCONFIG does NOT avoid this — glibc counts a
    # link-local fe80:: (present on every up interface) as "IPv6 configured", so
    # AAAA is returned even on IPv4-only panels; the ordering here is what helps.
    # Stable sort keeps each family's own order.
    infos.sort(key=lambda ai: ai[0] != socket.AF_INET)
    return infos or None


def _connect(family: int, socktype: int, proto: int, sockaddr: Any, timeout: float) -> None:
    """Open and immediately close one TCP connection, bounded by *timeout* (a real
    socket timeout, poll-based in C — unlike a signal, it actually bounds connect).
    Raises on refusal/unreachable (OSError) or per-candidate timeout (TimeoutError)."""
    sock = socket.socket(family, socktype, proto)
    try:
        sock.settimeout(timeout)
        sock.connect(sockaddr)
    finally:
        sock.close()


def tcp_open(
    host: str,
    port: int,
    timeout: float = 3.0,
    *,
    resolve: _Resolve = _resolve_bounded,
    connect: _Connect = _connect,
    monotonic: Callable[[], float] = time.monotonic,
) -> TcpProbe:
    """Probe whether *host:port* accepts a TCP connection under ONE total
    *timeout* covering DNS resolution AND every connection attempt.

    A single monotonic deadline bounds the whole call. DNS is bounded by
    resolving in a killed-and-reaped child (:func:`_resolve_bounded`) — not a
    Python SIGALRM, which cannot abort glibc's in-C resolver — and each connect
    gets the *shrinking* remaining budget (a real socket timeout) so multiple
    resolved addresses cannot multiply the wait. This works on any thread (no
    signals). Returns:

    * ``OPEN`` — a connection was established;
    * ``CLOSED`` — every resolved address refused/failed conclusively in budget;
    * ``INCONCLUSIVE`` — resolution could not complete (stuck/killed/failed DNS)
      or the deadline was hit. This is NOT proof the broker is down, so a caller
      must not treat it as a local-failure signal (never, on its own, a reboot).
    """
    if timeout <= 0:
        return TcpProbe.INCONCLUSIVE
    deadline = monotonic() + timeout
    try:
        infos = resolve(host, port, deadline - monotonic())
    except OSError:
        # The optional diagnostic must never crash the poll loop; a resolver
        # failure is never proof the broker is down.
        return TcpProbe.INCONCLUSIVE
    if infos is None:
        return TcpProbe.INCONCLUSIVE  # resolution could not complete in budget
    attempted = False
    timed_out_any = False
    total = len(infos)
    for i, (family, socktype, proto, _canon, sockaddr) in enumerate(infos):
        remaining = deadline - monotonic()
        if remaining <= 0:
            return TcpProbe.INCONCLUSIVE  # total deadline hit; a partial probe
        attempted = True
        # Fair share of the remaining budget across the candidates not yet tried,
        # so one slow/blackholed address (e.g. an IPv6 with no routed upstream,
        # returned before the reachable IPv4) cannot starve the rest — while the
        # sum of attempts stays within the one total deadline (contract 1).
        per_candidate = remaining / (total - i)
        try:
            connect(family, socktype, proto, sockaddr, per_candidate)
            return TcpProbe.OPEN
        except TimeoutError:
            timed_out_any = True
            continue  # slow/blackholed — try the next resolved address in budget
        except OSError as exc:
            if exc.errno in _LOCAL_ERRNOS:
                # Local resource exhaustion (fd/buffer/memory) is host-wide, not a
                # broker refusal — inconclusive, and retrying won't help.
                return TcpProbe.INCONCLUSIVE
            continue  # refused/unreachable — try the next resolved address
    if not attempted:
        return TcpProbe.INCONCLUSIVE
    # Every candidate was tried without connecting: a timed-out candidate makes
    # this inconclusive (never proof of down); only all-conclusive refusals are CLOSED.
    return TcpProbe.INCONCLUSIVE if timed_out_any else TcpProbe.CLOSED
