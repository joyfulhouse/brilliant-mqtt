"""tcp_open: one total deadline over DNS resolution AND every connection
attempt, returning a tri-state so a timed-out diagnostic is never mistaken for
proof the broker is down.

DNS is bounded by resolving in a killed-and-reaped child (a Python SIGALRM
cannot abort glibc's in-C resolver). Most tests inject the resolve/connect seams
for deterministic logic coverage; the tests at the end exercise the REAL
_RESOLVER_SCRIPT child end-to-end (localhost via /etc/hosts, a poisoned
PYTHONPATH, and a hung child that must be killed) so its syntax, argv, exit
codes and isolation are actually verified — no real network is required."""

from __future__ import annotations

import errno
import os
import socket
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from brilliant_wifi_watchdog import bounded, probe
from brilliant_wifi_watchdog.probe import TcpProbe

_A1 = (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.1", 1883))
_A2 = (socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("::1", 1883, 0, 0))

_LINUX = pytest.mark.skipif(
    not sys.platform.startswith("linux"), reason="/proc cmdline scan is Linux-only"
)


def _resolve(*infos: tuple[Any, ...]) -> probe._Resolve:
    def resolve(host: str, port: int, budget: float) -> list[tuple[Any, ...]] | None:
        return list(infos)

    return resolve


def _never_connect(family: int, socktype: int, proto: int, sockaddr: Any, timeout: float) -> None:
    raise AssertionError("connect should not be called")


# ---------------------------------------------------------------------------
# tcp_open — connect-loop logic over injected resolution
# ---------------------------------------------------------------------------


def test_open_on_first_candidate() -> None:
    seen: list[Any] = []

    def connect(family: int, socktype: int, proto: int, sockaddr: Any, timeout: float) -> None:
        seen.append(sockaddr)

    result = probe.tcp_open("broker", 1883, timeout=1.0, resolve=_resolve(_A1), connect=connect)
    assert result is TcpProbe.OPEN
    assert seen == [_A1[4]]


def test_tries_next_candidate_then_open() -> None:
    seen: list[Any] = []

    def connect(family: int, socktype: int, proto: int, sockaddr: Any, timeout: float) -> None:
        seen.append(sockaddr)
        if sockaddr == _A1[4]:
            raise OSError("connection refused")  # first address fails

    result = probe.tcp_open(
        "broker", 1883, timeout=1.0, resolve=_resolve(_A1, _A2), connect=connect
    )
    assert result is TcpProbe.OPEN
    assert seen == [_A1[4], _A2[4]]  # both candidates attempted, in order


def test_closed_when_all_candidates_refuse() -> None:
    seen: list[Any] = []

    def connect(family: int, socktype: int, proto: int, sockaddr: Any, timeout: float) -> None:
        seen.append(sockaddr)
        raise OSError("connection refused")

    result = probe.tcp_open(
        "broker", 1883, timeout=1.0, resolve=_resolve(_A1, _A2), connect=connect
    )
    assert result is TcpProbe.CLOSED  # a conclusive, in-budget "down"
    assert len(seen) == 2


def test_blackholed_first_candidate_falls_through_to_next() -> None:
    """A blackholed candidate (e.g. an IPv6 address returned first, whose upstream
    isn't routed) that TIMES OUT must not starve a reachable later candidate: the
    loop falls through within the one deadline, exactly as create_connection did
    per-address. Regression guard for the IPv6-first home shape."""
    seen: list[Any] = []

    def connect(family: int, socktype: int, proto: int, sockaddr: Any, timeout: float) -> None:
        seen.append(sockaddr)
        if family == socket.AF_INET6:
            raise TimeoutError("blackholed IPv6")  # first candidate hangs to its budget

    clock = iter([0.0, 0.0, 0.0, 1.0])
    result = probe.tcp_open(
        "broker",
        1883,
        resolve=_resolve(_A2, _A1),  # AAAA (v6) before A (v4), as getaddrinfo orders
        connect=connect,
        monotonic=lambda: next(clock),
    )
    assert result is TcpProbe.OPEN
    assert seen == [_A2[4], _A1[4]]  # BOTH attempted; v6 timed out, then v4 connected


def test_all_candidates_timing_out_is_inconclusive_not_closed() -> None:
    """If every candidate times out (none conclusively refuse), the verdict is
    INCONCLUSIVE — a timeout is never proof the broker is down."""

    def connect(family: int, socktype: int, proto: int, sockaddr: Any, timeout: float) -> None:
        raise TimeoutError("blackholed")

    clock = iter([0.0, 0.0, 0.0, 1.0])
    result = probe.tcp_open(
        "broker",
        1883,
        resolve=_resolve(_A1, _A2),
        connect=connect,
        monotonic=lambda: next(clock),
    )
    assert result is TcpProbe.INCONCLUSIVE


def test_inconclusive_when_no_addresses() -> None:
    result = probe.tcp_open("broker", 1883, timeout=1.0, resolve=_resolve(), connect=_never_connect)
    assert result is TcpProbe.INCONCLUSIVE


def test_inconclusive_when_resolution_fails() -> None:
    """resolve() returns None when the (bounded) resolver could not complete —
    a stuck/killed/failed DNS. That is INCONCLUSIVE, never proof of down."""

    def resolve(host: str, port: int, budget: float) -> list[tuple[Any, ...]] | None:
        return None

    result = probe.tcp_open("broker", 1883, timeout=1.0, resolve=resolve, connect=_never_connect)
    assert result is TcpProbe.INCONCLUSIVE


def test_inconclusive_when_timeout_non_positive() -> None:
    calls: list[str] = []

    def resolve(host: str, port: int, budget: float) -> list[tuple[Any, ...]] | None:
        calls.append(host)
        return [_A1]

    result = probe.tcp_open("broker", 1883, timeout=0.0, resolve=resolve, connect=_never_connect)
    assert result is TcpProbe.INCONCLUSIVE
    assert calls == []  # no budget to even resolve


def test_connect_timeout_is_inconclusive_not_closed() -> None:
    def connect(family: int, socktype: int, proto: int, sockaddr: Any, timeout: float) -> None:
        raise TimeoutError("connect timed out")

    result = probe.tcp_open("broker", 1883, timeout=1.0, resolve=_resolve(_A1), connect=connect)
    assert result is TcpProbe.INCONCLUSIVE  # a budget-exhausted connect proves nothing


def test_resolution_shares_the_total_deadline() -> None:
    """resolve() is handed the remaining budget, so a slow resolution eats into
    the same deadline the connects use (one total deadline)."""
    seen_budget: list[float] = []

    def resolve(host: str, port: int, budget: float) -> list[tuple[Any, ...]] | None:
        seen_budget.append(budget)
        return [_A1]

    probe.tcp_open("broker", 1883, timeout=2.5, resolve=resolve, connect=lambda *a: None)
    assert seen_budget and seen_budget[0] == pytest.approx(2.5, abs=0.2)


def test_budget_is_a_fair_share_across_candidates() -> None:
    """Each connect is capped at remaining/(candidates left), not the full
    remaining budget, so one slow candidate cannot starve the rest — while the
    sum stays within the one total deadline."""
    timeouts: list[float] = []

    def connect(family: int, socktype: int, proto: int, sockaddr: Any, timeout: float) -> None:
        timeouts.append(timeout)
        if sockaddr == _A1[4]:
            raise OSError("refused")

    # monotonic: deadline calc (0.0) -> resolve budget (0.0) -> iter1 (0.5) -> iter2 (0.9)
    clock = iter([0.0, 0.0, 0.5, 0.9])
    result = probe.tcp_open(
        "broker",
        1883,
        timeout=1.0,
        resolve=_resolve(_A1, _A2),
        connect=connect,
        monotonic=lambda: next(clock),
    )
    assert result is TcpProbe.OPEN
    # iter1: remaining 0.5 / 2 left = 0.25 ; iter2: remaining 0.1 / 1 left = 0.1
    assert timeouts == [pytest.approx(0.25), pytest.approx(0.1)]


def test_stops_when_total_deadline_exceeded_between_candidates() -> None:
    """The single deadline spans every candidate: once it passes, no further
    address is attempted and the result is inconclusive (a partial probe)."""
    seen: list[Any] = []

    def connect(family: int, socktype: int, proto: int, sockaddr: Any, timeout: float) -> None:
        seen.append(sockaddr)
        raise OSError("refused")

    # deadline (0.0) -> resolve budget (0.0) -> iter1 remaining=0.5 -> iter2 remaining=-0.5
    clock = iter([0.0, 0.0, 0.5, 1.5])
    result = probe.tcp_open(
        "broker",
        1883,
        timeout=1.0,
        resolve=_resolve(_A1, _A2),
        connect=connect,
        monotonic=lambda: next(clock),
    )
    assert result is TcpProbe.INCONCLUSIVE
    assert len(seen) == 1  # second candidate skipped — the total deadline had passed


def test_off_main_thread_shrinking_budget_holds() -> None:
    """No signals are used, so the deadline/shrinking-budget behaviour holds on a
    worker thread too — asserted via recorded connect timeouts, not merely 'no
    crash'. (DNS bounding off the main thread relies on the child resolver, which
    is likewise thread-agnostic.)"""
    box: dict[str, Any] = {}
    timeouts: list[float] = []

    def connect(family: int, socktype: int, proto: int, sockaddr: Any, timeout: float) -> None:
        timeouts.append(timeout)
        if sockaddr == _A1[4]:
            raise OSError("refused")

    def worker() -> None:
        clock = iter([0.0, 0.0, 0.5, 0.9])
        box["r"] = probe.tcp_open(
            "broker",
            1883,
            timeout=1.0,
            resolve=_resolve(_A1, _A2),
            connect=connect,
            monotonic=lambda: next(clock),
        )

    t = threading.Thread(target=worker)
    t.start()
    t.join(timeout=5.0)
    assert not t.is_alive()
    assert box["r"] is TcpProbe.OPEN
    assert timeouts == [pytest.approx(0.25), pytest.approx(0.1)]  # fair-share budget, on a thread


# ---------------------------------------------------------------------------
# _resolve_bounded — IP literals resolve in-process; hostnames via a bounded,
# killed-and-reaped child (the real bounding is proven in test_wd_bounded.py).
# ---------------------------------------------------------------------------


def _boom_run(*args: Any, **kwargs: Any) -> bounded.Completed:
    raise AssertionError("run_bounded must not be called for an IP literal")


def test_resolve_ip_literal_uses_no_child() -> None:
    infos = probe._resolve_bounded("127.0.0.1", 1883, 1.0, run=_boom_run)
    assert infos is not None
    assert any(ai[4][0] == "127.0.0.1" for ai in infos)  # resolved numerically, no child spawned


def test_resolve_ipv6_literal_uses_no_child() -> None:
    infos = probe._resolve_bounded("::1", 1883, 1.0, run=_boom_run)
    assert infos is not None
    assert any(ai[4][0] == "::1" for ai in infos)


def test_resolve_hostname_timeout_returns_none() -> None:
    """A DNS child that hangs is killed by run_bounded (timed_out=True); the
    resolver reports None so tcp_open is bounded regardless of whether the stuck
    C resolver would honour any signal."""

    def run(*args: Any, **kwargs: Any) -> bounded.Completed:
        return bounded.Completed(returncode=bounded.TIMEOUT_RC, stdout="", timed_out=True)

    assert probe._resolve_bounded("broker.invalid", 1883, 1.0, run=run) is None


def test_resolve_hostname_failure_returns_none() -> None:
    def run(*args: Any, **kwargs: Any) -> bounded.Completed:
        return bounded.Completed(
            returncode=3, stdout="", timed_out=False
        )  # child getaddrinfo error

    assert probe._resolve_bounded("broker.invalid", 1883, 1.0, run=run) is None


def test_resolve_hostname_success_parses_ips_via_python_child() -> None:
    seen: dict[str, Any] = {}

    def run(argv: Any, *, timeout: float, capture: bool = False) -> bounded.Completed:
        seen["argv"] = list(argv)
        seen["timeout"] = timeout
        seen["capture"] = capture
        return bounded.Completed(returncode=0, stdout="127.0.0.1\n", timed_out=False)

    infos = probe._resolve_bounded("broker.local", 1883, 2.0, run=run)
    assert infos is not None
    assert any(ai[4][0] == "127.0.0.1" for ai in infos)  # child IP rebuilt into an addrinfo
    assert seen["argv"][0] == sys.executable  # dependency-free child (no assumed tool)
    assert "-I" in seen["argv"] and "-S" in seen["argv"]  # isolated, no site
    assert seen["timeout"] == 2.0  # the child gets the full remaining budget
    assert seen["capture"] is True


def test_resolve_hostname_no_budget_skips_child() -> None:
    def run(*args: Any, **kwargs: Any) -> bounded.Completed:
        raise AssertionError("no budget: the resolver child must not be spawned")

    assert probe._resolve_bounded("broker.local", 1883, 0.0, run=run) is None


def test_resolve_orders_ipv4_before_ipv6() -> None:
    """getaddrinfo may return AAAA before A; the resolver reorders so the reachable
    IPv4 path is attempted before a possibly-blackholed IPv6 (the first candidate
    would otherwise burn the first attempt/budget slice on it)."""

    def run(argv: Any, *, timeout: float, capture: bool = False) -> bounded.Completed:
        return bounded.Completed(returncode=0, stdout="::1\n127.0.0.1\n", timed_out=False)

    infos = probe._resolve_bounded("dual.example", 1883, 2.0, run=run)
    assert infos is not None
    families = [ai[0] for ai in infos]
    assert families[0] == socket.AF_INET  # IPv4 first, despite the child listing ::1 first
    assert socket.AF_INET6 in families  # v6 still present, just after v4


# ---------------------------------------------------------------------------
# The optional diagnostic must NEVER crash the poll loop, and a local resource
# failure must not be logged as a broker outage.
# ---------------------------------------------------------------------------


def test_resolve_child_fork_failure_returns_none() -> None:
    """A fork/exec failure (EAGAIN/ENOMEM under MemoryMax/pids pressure, or a
    missing interpreter) must be swallowed as INCONCLUSIVE, not propagate and kill
    the watchdog (which would reset the in-memory escalation ladder)."""

    def run(*args: Any, **kwargs: Any) -> bounded.Completed:
        raise OSError(errno.ENOMEM, "Cannot allocate memory")

    assert probe._resolve_bounded("broker.local", 1883, 1.0, run=run) is None


def test_tcp_open_inconclusive_when_resolver_raises() -> None:
    def resolve(host: str, port: int, budget: float) -> list[tuple[Any, ...]] | None:
        raise OSError(errno.ENOMEM, "Cannot allocate memory")

    result = probe.tcp_open("broker", 1883, timeout=1.0, resolve=resolve, connect=_never_connect)
    assert result is TcpProbe.INCONCLUSIVE  # diagnostic failure never crashes the loop


def test_local_socket_exhaustion_is_inconclusive_not_closed() -> None:
    """A local fd/buffer/memory exhaustion on connect is not a broker refusal, so
    it reads INCONCLUSIVE — not CLOSED, which would log a false broker outage."""

    def connect(family: int, socktype: int, proto: int, sockaddr: Any, timeout: float) -> None:
        raise OSError(errno.EMFILE, "Too many open files")

    result = probe.tcp_open(
        "broker", 1883, timeout=1.0, resolve=_resolve(_A1, _A2), connect=connect
    )
    assert result is TcpProbe.INCONCLUSIVE


# ---------------------------------------------------------------------------
# The REAL _RESOLVER_SCRIPT child (no injected run) — proves its syntax, argv,
# exit codes, stdout format, and isolation actually work end-to-end.
# ---------------------------------------------------------------------------


def test_resolve_localhost_with_real_child() -> None:
    """Default run spawns the real child, which resolves 'localhost' via
    /etc/hosts (no network) — exercising the actual _RESOLVER_SCRIPT. Asserts
    127.0.0.1 is among the results (robust on any runner) and, only when ::1 is
    also present, that IPv4 is ordered before IPv6."""
    infos = probe._resolve_bounded("localhost", 1883, 5.0)
    assert infos is not None
    v4_positions = [i for i, ai in enumerate(infos) if ai[4][0] == "127.0.0.1"]
    v6_positions = [i for i, ai in enumerate(infos) if ai[0] == socket.AF_INET6]
    assert v4_positions  # 127.0.0.1 resolved
    if v6_positions:
        assert min(v4_positions) < min(v6_positions)  # IPv4 before IPv6 when both present


def test_resolver_child_ignores_poisoned_pythonpath(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """-I -S makes the child ignore PYTHONPATH/site, so a stray module on the
    panel's path cannot corrupt or block the dependency-free probe."""
    (tmp_path / "socket.py").write_text("raise RuntimeError('poisoned socket module')\n")
    (tmp_path / "sitecustomize.py").write_text("raise RuntimeError('poisoned site')\n")
    monkeypatch.setenv("PYTHONPATH", str(tmp_path))
    infos = probe._resolve_bounded("localhost", 1883, 5.0)  # real child, isolated
    assert infos is not None
    assert any(ai[4][0] == "127.0.0.1" for ai in infos)


def _cmdline_present(marker: str) -> bool:
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/cmdline", "rb") as f:
                cmd = f.read()
        except OSError:
            continue
        if marker.encode() in cmd:
            return True
    return False


@_LINUX
def test_tcp_open_bounded_when_real_resolver_child_hangs(monkeypatch: pytest.MonkeyPatch) -> None:
    """A real resolver child that hangs is killed and reaped: tcp_open returns
    INCONCLUSIVE within budget and no resolver process lingers."""
    monkeypatch.setattr(probe, "_RESOLVER_SCRIPT", "import time; time.sleep(30)")
    start = time.monotonic()
    result = probe.tcp_open("stuck.example.test", 1883, timeout=0.5, connect=_never_connect)
    elapsed = time.monotonic() - start
    assert result is TcpProbe.INCONCLUSIVE
    assert elapsed < 5.0  # bounded near 0.5s, not the 30s sleep
    deadline = time.monotonic() + 5.0
    while _cmdline_present("stuck.example.test") and time.monotonic() < deadline:
        time.sleep(0.05)
    assert not _cmdline_present("stuck.example.test")  # no lingering resolver child
