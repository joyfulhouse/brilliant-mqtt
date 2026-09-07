"""tcp_open: one total deadline over DNS resolution AND every connection
attempt, returning a tri-state so a timed-out diagnostic is never mistaken for
proof the broker is down.

Logic is exercised with injected resolver/connect/monotonic (no real network);
one integration test drives the real fractional-second SIGALRM to prove a hung
getaddrinfo is actually interrupted."""

from __future__ import annotations

import signal
import socket
import threading
import time
from typing import Any

import pytest

from brilliant_wifi_watchdog import probe
from brilliant_wifi_watchdog.probe import TcpProbe

_A1 = (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.1", 1883))
_A2 = (socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("::1", 1883, 0, 0))


def _resolver(*infos: tuple[Any, ...]) -> Any:
    def resolve(host: str, port: int, family: int, socktype: int) -> list[tuple[Any, ...]]:
        return list(infos)

    return resolve


def test_open_on_first_candidate() -> None:
    called: list[Any] = []

    def connect(family: int, socktype: int, proto: int, sockaddr: Any, timeout: float) -> None:
        called.append(sockaddr)

    result = probe.tcp_open("broker", 1883, timeout=1.0, resolver=_resolver(_A1), connect=connect)
    assert result is TcpProbe.OPEN
    assert called == [_A1[4]]


def test_tries_next_candidate_then_open() -> None:
    seen: list[Any] = []

    def connect(family: int, socktype: int, proto: int, sockaddr: Any, timeout: float) -> None:
        seen.append(sockaddr)
        if sockaddr == _A1[4]:
            raise OSError("connection refused")  # first address fails

    result = probe.tcp_open(
        "broker", 1883, timeout=1.0, resolver=_resolver(_A1, _A2), connect=connect
    )
    assert result is TcpProbe.OPEN
    assert seen == [_A1[4], _A2[4]]  # both candidates attempted, in order


def test_closed_when_all_candidates_refuse() -> None:
    seen: list[Any] = []

    def connect(family: int, socktype: int, proto: int, sockaddr: Any, timeout: float) -> None:
        seen.append(sockaddr)
        raise OSError("connection refused")

    result = probe.tcp_open(
        "broker", 1883, timeout=1.0, resolver=_resolver(_A1, _A2), connect=connect
    )
    assert result is TcpProbe.CLOSED  # a conclusive, in-budget "down"
    assert len(seen) == 2


def test_inconclusive_when_no_addresses() -> None:
    result = probe.tcp_open(
        "broker", 1883, timeout=1.0, resolver=_resolver(), connect=_never_called
    )
    assert result is TcpProbe.INCONCLUSIVE


def test_inconclusive_on_dns_failure() -> None:
    def resolve(host: str, port: int, family: int, socktype: int) -> list[tuple[Any, ...]]:
        raise socket.gaierror("name resolution failed")

    result = probe.tcp_open("broker", 1883, timeout=1.0, resolver=resolve, connect=_never_called)
    assert result is TcpProbe.INCONCLUSIVE  # DNS failure proves nothing about the broker


def test_inconclusive_when_timeout_non_positive() -> None:
    resolver_calls: list[Any] = []

    def resolve(host: str, port: int, family: int, socktype: int) -> list[tuple[Any, ...]]:
        resolver_calls.append(host)
        return [_A1]

    result = probe.tcp_open("broker", 1883, timeout=0.0, resolver=resolve, connect=_never_called)
    assert result is TcpProbe.INCONCLUSIVE
    assert resolver_calls == []  # no budget to even resolve


def test_connect_timeout_is_inconclusive_not_closed() -> None:
    def connect(family: int, socktype: int, proto: int, sockaddr: Any, timeout: float) -> None:
        raise TimeoutError("connect timed out")

    result = probe.tcp_open("broker", 1883, timeout=1.0, resolver=_resolver(_A1), connect=connect)
    # A connect that ran out its budget is inconclusive — not proof the broker is down.
    assert result is TcpProbe.INCONCLUSIVE


def test_budget_shrinks_across_candidates() -> None:
    """Each connect gets the remaining budget, not a fresh full timeout, so
    multiple resolved addresses cannot multiply the total wait."""
    timeouts: list[float] = []

    def connect(family: int, socktype: int, proto: int, sockaddr: Any, timeout: float) -> None:
        timeouts.append(timeout)
        if sockaddr == _A1[4]:
            raise OSError("refused")

    # monotonic: deadline calc (0.0) -> iter1 remaining (0.5) -> iter2 remaining (0.9)
    clock = iter([0.0, 0.5, 0.9])
    result = probe.tcp_open(
        "broker",
        1883,
        timeout=1.0,
        resolver=_resolver(_A1, _A2),
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

    # deadline calc (0.0) -> iter1 remaining=0.5 (>0, attempt) -> iter2 remaining=-0.5 (stop)
    clock = iter([0.0, 0.5, 1.5])
    result = probe.tcp_open(
        "broker",
        1883,
        timeout=1.0,
        resolver=_resolver(_A1, _A2),
        connect=connect,
        monotonic=lambda: next(clock),
    )
    assert result is TcpProbe.INCONCLUSIVE
    assert len(seen) == 1  # second candidate skipped — the total deadline had passed


def test_signal_state_restored_after_call() -> None:
    """No lingering alarm or handler: the itimer is cleared and the previous
    SIGALRM handler is restored, so repeated probes don't accumulate signal
    state (analogous to not leaking resolver threads)."""
    if not hasattr(signal, "setitimer"):
        pytest.skip("no SIGALRM/setitimer on this platform")
    before = signal.getsignal(signal.SIGALRM)

    def connect(family: int, socktype: int, proto: int, sockaddr: Any, timeout: float) -> None:
        pass

    probe.tcp_open("broker", 1883, timeout=1.0, resolver=_resolver(_A1), connect=connect)
    assert signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)  # disarmed
    assert signal.getsignal(signal.SIGALRM) is before  # handler restored


def test_off_main_thread_does_not_crash_and_still_bounds() -> None:
    """setitimer/signal are main-thread only; off the main thread the alarm is a
    graceful no-op and the monotonic budget still bounds the connect loop."""
    box: dict[str, TcpProbe] = {}

    def connect(family: int, socktype: int, proto: int, sockaddr: Any, timeout: float) -> None:
        pass

    def worker() -> None:
        box["r"] = probe.tcp_open(
            "broker", 1883, timeout=1.0, resolver=_resolver(_A1), connect=connect
        )

    t = threading.Thread(target=worker)
    t.start()
    t.join(timeout=5.0)
    assert not t.is_alive()
    assert box["r"] is TcpProbe.OPEN


@pytest.mark.skipif(not hasattr(signal, "setitimer"), reason="no SIGALRM/setitimer")
def test_delayed_dns_is_interrupted_within_deadline() -> None:
    """The core fix: a hung getaddrinfo is interrupted by the fractional-second
    SIGALRM instead of blocking far past the intended timeout."""

    def slow_resolver(host: str, port: int, family: int, socktype: int) -> list[tuple[Any, ...]]:
        time.sleep(3.0)  # simulate a wedged resolver
        return [_A1]

    start = time.monotonic()
    result = probe.tcp_open(
        "broker", 1883, timeout=0.05, resolver=slow_resolver, connect=_never_called
    )
    elapsed = time.monotonic() - start
    assert result is TcpProbe.INCONCLUSIVE
    assert elapsed < 1.0  # returned near the 0.05s deadline, not after the 3s sleep


def _never_called(family: int, socktype: int, proto: int, sockaddr: Any, timeout: float) -> None:
    raise AssertionError("connect should not be called")
