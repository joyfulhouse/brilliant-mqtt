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
    def __init__(
        self,
        connect_error: Exception | None = None,
        subscribe_error: Exception | None = None,
    ) -> None:
        self.connect_error = connect_error
        self.subscribe_error = subscribe_error
        self.connect_calls = 0
        self.subscribe_calls = 0

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

    def on_command(self, callback: Callable[[str, str], Awaitable[None]]) -> None:
        del callback

    async def subscribe(self, topic: str) -> None:
        del topic
        self.subscribe_calls += 1
        if self.subscribe_error is not None:
            raise self.subscribe_error


class _NoopBridge:
    def __init__(self, *args: object, **kwargs: object) -> None:
        del args, kwargs

    async def reconcile(self) -> None:
        return

    async def withdraw(self) -> None:
        return


class _ReadOnceBridge:
    def __init__(self, *args: object, **kwargs: object) -> None:
        self._bus = cast(_Bus, args[0])
        self._heartbeat = cast(Callable[[], None], kwargs["heartbeat"])

    async def reconcile(self) -> None:
        await self._bus.get_all()
        self._heartbeat()
        raise asyncio.CancelledError


def _settings(tmp_path: Path, mesh_priority: int = 0) -> Settings:
    return Settings(
        panel="office",
        mqtt_host="broker",
        mqtt_username="user",
        mqtt_password="password",
        retained_topics_file=str(tmp_path / "owned-topics.json"),
        bus_heartbeat_file=str(tmp_path / "bus-heartbeat"),
        bus_phase_file=str(tmp_path / "bus-phase"),
        mesh_priority=mesh_priority,
    )


def _seed(path: str, contents: str) -> None:
    """Write a runtime file synchronously (setup helper; keeps blocking file
    I/O out of the async test bodies, per ruff ASYNC240)."""
    Path(path).write_text(contents, encoding="utf-8")


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


async def test_stale_bus_phase_and_heartbeat_do_not_reboot_on_broker_outage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A prior HEALTHY session leaves "bus" and a fresh heartbeat on disk;
    later the heartbeat goes stale while the broker is down. Today's
    broker-only outage must NOT read those leftovers as a bus wedge: entering
    _run_session stamps "pre_bus" BEFORE mqtt.connect(), so once the broker
    refusal short-circuits startup, bus_confirmed is False even though the
    stale heartbeat age alone would otherwise qualify. Unlike its empty-file
    siblings above, this seeds the fail-unsafe leftovers explicitly so the
    test would catch a startup that wrote a fake heartbeat or let a stale
    "bus" marker survive the refusal."""
    settings = _settings(tmp_path)
    # Leftovers from a prior successful session, before this outage begins.
    _seed(settings.bus_phase_file, "bus")
    _seed(settings.bus_heartbeat_file, "100.0")

    bus = _Bus()
    mqtt = _Mqtt(ConnectionError("broker refused"))
    _install_session_fakes(monkeypatch, bus, mqtt)

    with pytest.raises(ConnectionError, match="broker refused"):
        await main_mod._run_session(settings, None, None)

    confirmed = bus_confirmed(settings.bus_phase_file)
    age = heartbeat_age(settings.bus_heartbeat_file, now=100_000.0, started_at=0.0)
    assert bus.start_calls == 0  # the local bus was never reached
    assert mqtt.connect_calls == 1
    # The pre_bus stamp at session entry overwrote the stale "bus" marker...
    assert confirmed is False
    # ...and the seeded heartbeat is genuinely stale, so this test proves it is
    # bus_confirmed (not a fresh heartbeat) that holds the reboot back.
    assert age >= 1800.0
    assert not should_reboot(
        age=age,
        stale_after=1800.0,
        bridge_active=True,
        gateway_up=True,
        bus_confirmed=confirmed,
    )


async def test_mesh_election_failure_stays_pre_bus_despite_mqtt_connect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A mesh-election join failure is an MQTT/mesh-side startup failure, not
    a bus failure — it must not leave the phase stamped "bus" even though
    mqtt.connect() already succeeded (issue #87 acceptance: distinguish
    explicit local bus failure from inability to reach MQTT)."""
    bus = _Bus()
    mqtt = _Mqtt(subscribe_error=ConnectionError("mesh claim subscribe failed"))
    settings = _settings(tmp_path, mesh_priority=1)
    _install_session_fakes(monkeypatch, bus, mqtt)

    with pytest.raises(ConnectionError, match="mesh claim subscribe failed"):
        await main_mod._run_session(settings, None, None)

    confirmed = bus_confirmed(settings.bus_phase_file)
    assert mqtt.connect_calls == 1
    assert mqtt.subscribe_calls == 1
    assert bus.start_calls == 0
    assert confirmed is False
    assert not should_reboot(
        age=99_999.0,
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
        # Assumes the systemd supervisor's restart backoff keeps
        # brilliant-mqtt.service reported "active" through repeated
        # in-process bus-handshake failures — see the finally-block comment
        # in __main__.py about backoff being far shorter than stale_after.
        # This test exercises the predicate's boolean math only; it does not
        # exercise systemd's actual crash-loop/backoff behavior end to end.
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
        # cap=2 leaves headroom so the second decision's suppression can only be
        # the cooldown, not the cap; a non-zero cooldown makes that gate real.
        GuardPolicy(cooldown=300.0, cap=2, window=21_600.0),
    )
    # First qualifying decision reboots and records the stamp.
    handle(should=decision, guard=guard, now=1900.0, reboot_fn=lambda: reboots.append("reboot"))
    # 100s later — inside the 300s cooldown — the cap (2) would still allow a
    # reboot, so this suppression proves the cooldown is what gates it.
    handle(should=decision, guard=guard, now=2000.0, reboot_fn=lambda: reboots.append("reboot"))
    assert reboots == ["reboot"]
    # Once the cooldown elapses (400s > 300s) and the cap still has headroom, the
    # next decision reboots again — confirming the cooldown, not the cap, held
    # the second one back.
    handle(should=decision, guard=guard, now=2300.0, reboot_fn=lambda: reboots.append("reboot"))
    assert reboots == ["reboot", "reboot"]


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
