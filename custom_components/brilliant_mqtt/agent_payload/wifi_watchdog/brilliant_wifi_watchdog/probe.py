"""Connectivity probes (stdlib). Gateway derived from the routing table, never hardcoded."""

from __future__ import annotations

import contextlib
import enum
import signal
import socket
import threading
import time
from collections.abc import Callable, Iterator
from typing import Any

from . import bounded

# Wall-clock bound for a single probe child. `ip route`/`ping` are local and
# fast; the bound guards the pathological case where the child never returns
# (a hung network stack), which would otherwise wedge the watchdog loop.
_PROBE_TIMEOUT = 5.0


def _run_out(argv: list[str]) -> tuple[int, str]:
    r = bounded.run_bounded(argv, timeout=_PROBE_TIMEOUT, capture=True)
    return r.returncode, r.stdout


def _run_rc(argv: list[str]) -> int:
    return bounded.run_bounded(argv, timeout=_PROBE_TIMEOUT).returncode


def default_gateway(run: Callable[[list[str]], tuple[int, str]] = _run_out) -> str | None:
    rc, out = run(["ip", "route", "show", "default"])
    if rc != 0:
        return None
    for line in out.splitlines():
        parts = line.split()
        if "via" in parts:
            idx = parts.index("via") + 1
            if idx < len(parts):  # guard: "default via" with no following token
                return parts[idx]
    return None


def ping(host: str, run: Callable[[list[str]], int] = _run_rc) -> bool:
    return run(["ping", "-c", "1", "-W", "2", host]) == 0


class TcpProbe(enum.Enum):
    """Result of a bounded TCP reachability probe."""

    OPEN = "open"  # a connection was established
    CLOSED = "closed"  # every resolved address refused/failed conclusively, in budget
    INCONCLUSIVE = "inconclusive"  # DNS failed or the deadline was hit — proves nothing


class _DeadlineReached(Exception):
    """Raised by the SIGALRM handler to unblock a hung getaddrinfo/connect."""


_AddrInfo = tuple[Any, ...]
_Resolver = Callable[[str, int, int, int], list[_AddrInfo]]
_Connect = Callable[[int, int, int, Any, float], None]


def _supports_alarm() -> bool:
    """Whether a fractional-second SIGALRM can be armed here. setitimer/SIGALRM
    exist only on Unix and only the main thread may install signal handlers, so
    guard both — the module stays importable and testable off-panel, and off the
    main thread the caller falls back to the monotonic connect budget alone."""
    return (
        hasattr(signal, "SIGALRM")
        and hasattr(signal, "setitimer")
        and threading.current_thread() is threading.main_thread()
    )


@contextlib.contextmanager
def _alarm(seconds: float) -> Iterator[None]:
    """Arm a one-shot SIGALRM *seconds* from now (fractional, via setitimer) so a
    hung syscall — getaddrinfo honors no timeout of its own — is interrupted with
    :class:`_DeadlineReached`. A no-op when unsupported (see :func:`_supports_alarm`).
    The timer is always disarmed and the previous handler restored on exit, so no
    alarm state leaks between probes."""
    if seconds <= 0 or not _supports_alarm():
        yield
        return

    def _fire(signum: int, frame: Any) -> None:
        raise _DeadlineReached

    previous = signal.signal(signal.SIGALRM, _fire)
    try:
        signal.setitimer(signal.ITIMER_REAL, seconds)
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def _connect(family: int, socktype: int, proto: int, sockaddr: Any, timeout: float) -> None:
    """Open and immediately close one TCP connection, bounded by *timeout*.
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
    resolver: _Resolver = socket.getaddrinfo,
    connect: _Connect = _connect,
    monotonic: Callable[[], float] = time.monotonic,
) -> TcpProbe:
    """Probe whether *host:port* accepts a TCP connection under ONE total
    *timeout* covering DNS resolution AND every connection attempt.

    A single monotonic deadline bounds the whole call: a fractional-second
    SIGALRM (main thread only) interrupts a hung ``getaddrinfo``, and each
    connect is given the *shrinking* remaining budget so multiple resolved
    addresses cannot multiply the wait. Returns:

    * ``OPEN`` — a connection was established;
    * ``CLOSED`` — every resolved address refused/failed conclusively in budget;
    * ``INCONCLUSIVE`` — DNS failed or the deadline was hit. This is NOT proof
      the broker is down, so a caller must not treat it as a local-failure
      signal (it must never, on its own, drive a reboot).
    """
    if timeout <= 0:
        return TcpProbe.INCONCLUSIVE
    deadline = monotonic() + timeout
    try:
        with _alarm(timeout):
            infos = resolver(host, port, socket.AF_UNSPEC, socket.SOCK_STREAM)
            attempted = False
            for family, socktype, proto, _canon, sockaddr in infos:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    return TcpProbe.INCONCLUSIVE  # deadline passed mid-list; a partial probe
                attempted = True
                try:
                    connect(family, socktype, proto, sockaddr, remaining)
                    return TcpProbe.OPEN
                except _DeadlineReached:
                    return TcpProbe.INCONCLUSIVE
                except TimeoutError:
                    return TcpProbe.INCONCLUSIVE  # ran out this candidate's budget
                except OSError:
                    continue  # refused/unreachable — try the next resolved address
            return TcpProbe.CLOSED if attempted else TcpProbe.INCONCLUSIVE
    except _DeadlineReached:
        return TcpProbe.INCONCLUSIVE  # the alarm fired inside getaddrinfo
    except socket.gaierror:
        return TcpProbe.INCONCLUSIVE  # name resolution failed — proves nothing
    except OSError:
        return TcpProbe.INCONCLUSIVE
