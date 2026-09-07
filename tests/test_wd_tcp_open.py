"""tcp_open: one total deadline over DNS resolution AND every connection
attempt, returning a tri-state so a timed-out diagnostic is never mistaken for
proof the broker is down.

DNS is bounded by resolving in a killed-and-reaped child (a Python SIGALRM
cannot abort glibc's in-C resolver), so these tests inject the resolve/connect
seams for deterministic logic coverage and drive `_resolve_bounded` with an
injected child runner — no real DNS, no real network. The real killed-and-reaped
child behaviour is proven in test_wd_bounded.py."""

from __future__ import annotations

import socket
import sys
import threading
from typing import Any

import pytest

from brilliant_wifi_watchdog import bounded, probe
from brilliant_wifi_watchdog.probe import TcpProbe

_A1 = (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.1", 1883))
_A2 = (socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("::1", 1883, 0, 0))


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


def test_budget_shrinks_across_candidates() -> None:
    """Each connect gets the remaining budget, not a fresh full timeout, so
    multiple resolved addresses cannot multiply the total wait."""
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
    assert timeouts == [pytest.approx(0.5), pytest.approx(0.1)]  # shrinking remaining budget


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
    assert timeouts == [pytest.approx(0.5), pytest.approx(0.1)]  # shrinking budget, on a thread


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
    assert seen["timeout"] == 2.0  # the child gets the full remaining budget
    assert seen["capture"] is True


def test_resolve_hostname_no_budget_skips_child() -> None:
    def run(*args: Any, **kwargs: Any) -> bounded.Completed:
        raise AssertionError("no budget: the resolver child must not be spawned")

    assert probe._resolve_bounded("broker.local", 1883, 0.0, run=run) is None
