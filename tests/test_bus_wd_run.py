from __future__ import annotations

from types import SimpleNamespace
from typing import Any, NoReturn

import pytest

from brilliant_bus_watchdog import bounded
from brilliant_bus_watchdog.run import (
    _service_active,
    _service_started_at,
    handle,
    load_config,
    should_reboot,
)


@pytest.mark.parametrize(
    ("age", "bridge_active", "gateway_up", "bus_failure_age", "expected"),
    [
        (1900.0, True, True, 1900.0, True),
        (100.0, True, True, 1900.0, False),
        (9999.0, False, True, 9999.0, False),
        (9999.0, True, False, 9999.0, False),
        (9999.0, True, True, None, False),
        (9999.0, True, True, 1799.9, False),
    ],
)
def test_should_reboot_requires_every_bus_wedge_signal(
    age: float,
    bridge_active: bool,
    gateway_up: bool,
    bus_failure_age: float | None,
    expected: bool,
) -> None:
    assert (
        should_reboot(
            age=age,
            bridge_active=bridge_active,
            gateway_up=gateway_up,
            bus_failure_age=bus_failure_age,
            stale_after=1800.0,
        )
        is expected
    )


def test_handle_reboots_when_guard_allows_record_before_reboot() -> None:
    calls: list[str] = []

    class G:
        def can_reboot(self, now: float) -> bool:
            return True

        def record(self, now: float) -> None:
            calls.append("record")

    handle(should=True, guard=G(), now=1.0, reboot_fn=lambda: calls.append("reboot"))
    assert calls == ["record", "reboot"]


def test_handle_blocked_by_guard() -> None:
    calls: list[str] = []

    class G:
        def can_reboot(self, now: float) -> bool:
            return False

        def record(self, now: float) -> None:
            calls.append("record")

    handle(should=True, guard=G(), now=1.0, reboot_fn=lambda: calls.append("reboot"))
    assert calls == []


def test_handle_noop_when_should_false() -> None:
    calls: list[str] = []

    class G:
        def can_reboot(self, now: float) -> bool:
            calls.append("checked")
            return True

        def record(self, now: float) -> None:
            calls.append("record")

    handle(should=False, guard=G(), now=1.0, reboot_fn=lambda: calls.append("reboot"))
    assert calls == []


def test_load_config_defaults() -> None:
    c = load_config({})
    assert c.interval == 60.0 and c.stale_after == 1800.0
    assert c.heartbeat_path == "/run/brilliant-mqtt/bus-heartbeat"
    assert c.state_path == "/var/brilliant-mqtt/bus-watchdog.state"
    assert c.bridge_service == "brilliant-mqtt"


def test_load_config_overrides() -> None:
    c = load_config({"BUS_WATCHDOG_STALE_AFTER": "600", "BUS_HEARTBEAT_FILE": "/x"})
    assert c.stale_after == 600.0 and c.heartbeat_path == "/x"


@pytest.mark.parametrize(
    ("environ", "expected"),
    [({}, "/run/brilliant-mqtt/bus-phase"), ({"BUS_PHASE_FILE": "/phase"}, "/phase")],
)
def test_load_config_bus_phase_path(environ: dict[str, str], expected: str) -> None:
    assert load_config(environ).phase_path == expected


@pytest.mark.parametrize("state", ["active", "activating"])
def test_service_active_true_while_running_or_restarting(state: str) -> None:
    def run(argv: list[str]) -> SimpleNamespace:
        return SimpleNamespace(stdout=f"{state}\n")

    assert _service_active("brilliant-mqtt", run=run) is True


def test_service_active_false_when_stdout_inactive() -> None:
    def run(argv: list[str]) -> SimpleNamespace:
        return SimpleNamespace(stdout="inactive")

    assert _service_active("brilliant-mqtt", run=run) is False


def test_service_active_false_when_runner_raises_oserror() -> None:
    def runner(argv: list[str]) -> NoReturn:
        raise OSError("systemctl not found")

    assert _service_active("brilliant-mqtt", run=runner) is False


def test_service_active_default_runner_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    def spy(argv: Any, *, timeout: float, capture: bool = False) -> bounded.Completed:
        calls.append({"argv": list(argv), "timeout": timeout, "capture": capture})
        return bounded.Completed(returncode=0, stdout="active\n", timed_out=False)

    monkeypatch.setattr(bounded, "run_bounded", spy)
    assert _service_active("brilliant-mqtt") is True
    assert calls[0]["argv"] == ["systemctl", "is-active", "brilliant-mqtt"]
    assert calls[0]["timeout"] > 0
    assert calls[0]["capture"] is True


def test_service_active_timeout_reads_as_inactive(monkeypatch: pytest.MonkeyPatch) -> None:
    """A hung `systemctl is-active` must not read as active — that would let the
    watchdog believe the bridge is up when it cannot actually tell."""

    def spy(argv: Any, *, timeout: float, capture: bool = False) -> bounded.Completed:
        return bounded.Completed(returncode=bounded.TIMEOUT_RC, stdout="", timed_out=True)

    monkeypatch.setattr(bounded, "run_bounded", spy)
    assert _service_active("brilliant-mqtt") is False


def test_service_start_generation_uses_systemd_monotonic_timestamp() -> None:
    calls: list[list[str]] = []

    def run(argv: list[str]) -> SimpleNamespace:
        calls.append(argv)
        return SimpleNamespace(stdout="1901000000\n")

    assert _service_started_at("brilliant-mqtt", run=run) == 1901.0
    assert calls == [
        [
            "systemctl",
            "show",
            "--property=ExecMainStartTimestampMonotonic",
            "--value",
            "brilliant-mqtt",
        ]
    ]


@pytest.mark.parametrize("stdout", ["", "garbage", "0", "-1"])
def test_unknown_service_start_generation_fails_closed(stdout: str) -> None:
    def run(argv: list[str]) -> SimpleNamespace:
        del argv
        return SimpleNamespace(stdout=stdout)

    assert _service_started_at("brilliant-mqtt", run=run) is None
