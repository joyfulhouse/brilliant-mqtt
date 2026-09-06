"""Cross-component regressions from bridge startup phase to reboot decisions."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import cast

import pytest

import brilliant_mqtt.__main__ as main_mod
from brilliant_bus_watchdog.health import bus_confirmed, heartbeat_age
from brilliant_bus_watchdog.reboot_guard import GuardPolicy, RebootGuard
from brilliant_bus_watchdog.run import handle, should_reboot
from brilliant_mqtt.config import Settings
from brilliant_mqtt.model import BrilliantDevice
from brilliant_mqtt.retained_topics import RetainedLedgerError


class _Bus:
    def __init__(self, start_error: Exception | None = None) -> None:
        self.start_error = start_error
        self.start_calls = 0
        self.read_calls = 0

    def on_reconnect(self, callback: Callable[[], Awaitable[None]]) -> None:
        del callback

    async def start(self) -> None:
        self.start_calls += 1
        if self.start_error is not None:
            raise self.start_error

    async def get_all(self) -> list[BrilliantDevice]:
        self.read_calls += 1
        return []

    async def shutdown(self) -> None:
        return


class _Mqtt:
    def __init__(self, connect_error: Exception | None = None) -> None:
        self.connect_error = connect_error
        self.connect_calls = 0

    async def connect(self) -> None:
        self.connect_calls += 1
        if self.connect_error is not None:
            raise self.connect_error

    async def disconnect(self) -> None:
        return

    async def publish(
        self,
        topic: str,
        payload: str,
        retain: bool = False,
        qos: int = 0,
    ) -> None:
        del topic, payload, retain, qos


class _NoopBridge:
    def __init__(self, *args: object, **kwargs: object) -> None:
        del args, kwargs

    async def reconcile(self) -> None:
        return


class _ReadOnceBridge:
    def __init__(self, *args: object, **kwargs: object) -> None:
        self._bus = cast(_Bus, args[0])
        self._heartbeat = cast(Callable[[], None], kwargs["heartbeat"])

    async def reconcile(self) -> None:
        await self._bus.get_all()
        self._heartbeat()
        raise asyncio.CancelledError


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        panel="office",
        mqtt_host="broker",
        mqtt_username="user",
        mqtt_password="password",
        retained_topics_file=str(tmp_path / "owned-topics.json"),
        bus_heartbeat_file=str(tmp_path / "bus-heartbeat"),
        bus_phase_file=str(tmp_path / "bus-phase"),
    )


def _install_session_fakes(
    monkeypatch: pytest.MonkeyPatch,
    bus: _Bus,
    mqtt: _Mqtt,
    bridge: type[_NoopBridge] | type[_ReadOnceBridge] = _NoopBridge,
) -> None:
    monkeypatch.setattr(main_mod, "RpcBusAdapter", lambda **kwargs: bus)
    monkeypatch.setattr(main_mod, "AioMqttAdapter", lambda settings: mqtt)
    monkeypatch.setattr(main_mod, "Bridge", bridge)


async def test_broker_outage_never_qualifies_as_a_bus_wedge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bus = _Bus()
    mqtt = _Mqtt(ConnectionError("broker refused"))
    settings = _settings(tmp_path)
    _install_session_fakes(monkeypatch, bus, mqtt)

    for _ in range(3):
        with pytest.raises(ConnectionError, match="broker refused"):
            await main_mod._run_session(settings, None, None)

    confirmed = bus_confirmed(settings.bus_phase_file)
    assert bus.start_calls == 0
    assert mqtt.connect_calls == 3
    assert confirmed is False
    assert not should_reboot(
        age=1900.0,
        stale_after=1800.0,
        bridge_active=True,
        gateway_up=True,
        bus_confirmed=confirmed,
    )


async def test_retained_ledger_failure_stays_pre_bus(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ledger_error = RetainedLedgerError("invalid retained ledger")

    class _FailingLedger:
        def __init__(self, panel: str, path: Path) -> None:
            del self, panel, path

        async def async_load(self) -> None:
            raise ledger_error

    bus = _Bus()
    mqtt = _Mqtt()
    settings = _settings(tmp_path)
    _install_session_fakes(monkeypatch, bus, mqtt)
    monkeypatch.setattr(main_mod, "RetainedTopicLedger", _FailingLedger)

    with pytest.raises(RetainedLedgerError) as raised:
        await main_mod._run_session(settings, None, None)

    confirmed = bus_confirmed(settings.bus_phase_file)
    assert raised.value is ledger_error
    assert bus.start_calls == 0
    assert mqtt.connect_calls == 1  # diagnostic publish only
    assert confirmed is False
    assert not should_reboot(
        age=99_999.0,
        stale_after=1800.0,
        bridge_active=True,
        gateway_up=True,
        bus_confirmed=confirmed,
    )


async def test_sustained_bus_handshake_failure_still_uses_reboot_guard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bus = _Bus(ConnectionError("bus handshake failed"))
    mqtt = _Mqtt()
    settings = _settings(tmp_path)
    _install_session_fakes(monkeypatch, bus, mqtt)

    for _ in range(3):
        with pytest.raises(ConnectionError, match="bus handshake failed"):
            await main_mod._run_session(settings, None, None)

    confirmed = bus_confirmed(settings.bus_phase_file)
    age = heartbeat_age(settings.bus_heartbeat_file, now=1900.0, started_at=0.0)
    decision = should_reboot(
        age=age,
        stale_after=1800.0,
        bridge_active=True,
        gateway_up=True,
        bus_confirmed=confirmed,
    )
    assert bus.start_calls == 3
    assert mqtt.connect_calls == 3
    assert confirmed is True
    assert decision is True

    reboots: list[str] = []
    guard = RebootGuard(
        str(tmp_path / "reboot-guard.json"),
        GuardPolicy(cooldown=0.0, cap=1, window=21_600.0),
    )
    handle(should=decision, guard=guard, now=1900.0, reboot_fn=lambda: reboots.append("reboot"))
    handle(should=decision, guard=guard, now=1901.0, reboot_fn=lambda: reboots.append("reboot"))
    assert reboots == ["reboot"]


async def test_successful_bus_read_refreshes_heartbeat_without_resetting_phase(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bus = _Bus()
    mqtt = _Mqtt()
    settings = _settings(tmp_path)
    _install_session_fakes(monkeypatch, bus, mqtt, _ReadOnceBridge)

    with pytest.raises(asyncio.CancelledError):
        await main_mod._run_session(settings, None, None)

    confirmed = bus_confirmed(settings.bus_phase_file)
    age = heartbeat_age(settings.bus_heartbeat_file, now=time.time(), started_at=0.0)
    assert bus.start_calls == 1
    assert bus.read_calls == 1
    assert confirmed is True
    assert 0.0 <= age < 1.0
    assert not should_reboot(
        age=age,
        stale_after=1800.0,
        bridge_active=True,
        gateway_up=True,
        bus_confirmed=confirmed,
    )
