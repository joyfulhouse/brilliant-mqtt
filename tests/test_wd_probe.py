from collections.abc import Callable
from typing import Any

import pytest

from brilliant_wifi_watchdog import bounded, probe


def test_ping_true_on_zero_rc() -> None:
    assert probe.ping("192.168.1.1", run=lambda argv: 0) is True
    assert probe.ping("192.168.1.1", run=lambda argv: 1) is False


# ---------------------------------------------------------------------------
# The default runner must be bounded — no probe child may hang the watchdog.
# ---------------------------------------------------------------------------


def _spy_run_bounded(
    monkeypatch: pytest.MonkeyPatch,
    *,
    returncode: int = 0,
    stdout: str = "",
    timed_out: bool = False,
) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []

    def spy(argv: Any, *, timeout: float, capture: bool = False) -> bounded.Completed:
        calls.append({"argv": list(argv), "timeout": timeout, "capture": capture})
        return bounded.Completed(returncode=returncode, stdout=stdout, timed_out=timed_out)

    monkeypatch.setattr(bounded, "run_bounded", spy)
    return calls


def _gateway_runner(
    *results: bounded.Completed,
) -> tuple[Callable[[list[str], bool], bounded.Completed], list[tuple[list[str], bool]]]:
    remaining = iter(results)
    calls: list[tuple[list[str], bool]] = []

    def run(argv: list[str], capture: bool) -> bounded.Completed:
        calls.append((argv, capture))
        return next(remaining)

    return run, calls


def test_ping_default_runner_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _spy_run_bounded(monkeypatch, returncode=0)
    assert probe.ping("192.168.1.1") is True
    assert calls[0]["argv"] == ["ping", "-c", "1", "-W", "2", "192.168.1.1"]
    assert calls[0]["timeout"] > 0


def test_ping_timeout_reads_as_down(monkeypatch: pytest.MonkeyPatch) -> None:
    """The compatibility bool API stays false; the poller uses the tri-state API."""
    _spy_run_bounded(monkeypatch, returncode=bounded.TIMEOUT_RC, timed_out=True)
    assert probe.ping("192.168.1.1") is False


def test_gateway_probe_configured_gateway_preserves_completed_state() -> None:
    cases = (
        (bounded.Completed(0, "", False), probe.TcpProbe.OPEN),
        (bounded.Completed(1, "", False), probe.TcpProbe.CLOSED),
        (bounded.Completed(bounded.TIMEOUT_RC, "", True), probe.TcpProbe.INCONCLUSIVE),
    )
    for completed, expected in cases:
        run, calls = _gateway_runner(completed)
        assert probe.gateway_probe("192.0.2.1", run=run) == ("192.0.2.1", expected)
        assert calls == [(["ping", "-c", "1", "-W", "2", "192.0.2.1"], False)]


def test_gateway_probe_discovers_and_pings_default_route() -> None:
    run, calls = _gateway_runner(
        bounded.Completed(0, "default via 192.0.2.1 dev wlan0\n", False),
        bounded.Completed(0, "", False),
    )

    assert probe.gateway_probe(None, run=run) == ("192.0.2.1", probe.TcpProbe.OPEN)
    assert calls == [
        (["ip", "route", "show", "default"], True),
        (["ping", "-c", "1", "-W", "2", "192.0.2.1"], False),
    ]


def test_gateway_probe_classifies_route_failures() -> None:
    cases = (
        (bounded.Completed(0, "default dev wlan0\n", False), probe.TcpProbe.CLOSED),
        (
            bounded.Completed(bounded.TIMEOUT_RC, "", True),
            probe.TcpProbe.INCONCLUSIVE,
        ),
        (bounded.Completed(1, "", False), probe.TcpProbe.CLOSED),
    )
    for completed, expected in cases:
        run, calls = _gateway_runner(completed)
        assert probe.gateway_probe(None, run=run) == (None, expected)
        assert calls == [(["ip", "route", "show", "default"], True)]
