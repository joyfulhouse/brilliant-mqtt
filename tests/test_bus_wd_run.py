from __future__ import annotations

from types import SimpleNamespace
from typing import NoReturn

from brilliant_bus_watchdog.run import _service_active, handle, load_config, should_reboot


def test_should_reboot_all_true() -> None:
    assert should_reboot(age=1900.0, bridge_active=True, gateway_up=True, stale_after=1800.0)


def test_not_stale() -> None:
    assert not should_reboot(age=100.0, bridge_active=True, gateway_up=True, stale_after=1800.0)


def test_bridge_inactive_blocks() -> None:
    assert not should_reboot(age=9999.0, bridge_active=False, gateway_up=True, stale_after=1800.0)


def test_gateway_down_blocks() -> None:
    assert not should_reboot(age=9999.0, bridge_active=True, gateway_up=False, stale_after=1800.0)


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


def test_service_active_true_when_stdout_active() -> None:
    def run(argv: list[str]) -> SimpleNamespace:
        return SimpleNamespace(stdout="active\n")

    assert _service_active("brilliant-mqtt", run=run) is True


def test_service_active_false_when_stdout_inactive() -> None:
    def run(argv: list[str]) -> SimpleNamespace:
        return SimpleNamespace(stdout="inactive")

    assert _service_active("brilliant-mqtt", run=run) is False


def test_service_active_false_when_runner_raises_oserror() -> None:
    def run(argv: list[str]) -> NoReturn:
        raise OSError("systemctl not found")

    assert _service_active("brilliant-mqtt", run=run) is False
