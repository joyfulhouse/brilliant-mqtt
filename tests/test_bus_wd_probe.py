"""The bus watchdog's probe children must be bounded (mirror of the Wi-Fi
watchdog probe: each package is deployed self-contained on its own PYTHONPATH)."""

from __future__ import annotations

from typing import Any

import pytest

from brilliant_bus_watchdog import bounded, probe


def test_default_gateway_parses_ip_route() -> None:
    out = "default via 192.168.1.1 dev wlan0 proto dhcp metric 600\n"
    assert probe.default_gateway(run=lambda argv: (0, out)) == "192.168.1.1"


def test_ping_true_on_zero_rc() -> None:
    assert probe.ping("192.168.1.1", run=lambda argv: 0) is True
    assert probe.ping("192.168.1.1", run=lambda argv: 1) is False


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


def test_default_gateway_default_runner_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _spy_run_bounded(monkeypatch, stdout="default via 10.0.0.1 dev wlan0\n")
    assert probe.default_gateway() == "10.0.0.1"
    assert calls[0]["argv"] == ["ip", "route", "show", "default"]
    assert calls[0]["timeout"] > 0
    assert calls[0]["capture"] is True


def test_ping_default_runner_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _spy_run_bounded(monkeypatch, returncode=0)
    assert probe.ping("192.168.1.1") is True
    assert calls[0]["argv"] == ["ping", "-c", "1", "-W", "2", "192.168.1.1"]
    assert calls[0]["timeout"] > 0


def test_ping_timeout_reads_as_down(monkeypatch: pytest.MonkeyPatch) -> None:
    _spy_run_bounded(monkeypatch, returncode=bounded.TIMEOUT_RC, timed_out=True)
    assert probe.ping("192.168.1.1") is False
