"""Current routing and pre-handoff guards for mesh availability on the real adapter."""

from __future__ import annotations

import asyncio

import pytest

from brilliant_mqtt.discovery import command_topic
from brilliant_mqtt.model import BrilliantDevice
from tests.fakes import _RawPeripheral, _RawVariable
from tests.mesh_regression_support import turns
from tests.test_mesh_bridge_integration import Connected, connected

__all__ = ["connected"]

_AVAILABILITY = "brilliant/mesh/availability"


def _add_load(rig: Connected) -> None:
    assert rig.raw.peripherals is not None
    peripheral = _RawPeripheral("1")
    peripheral.peripheral_type = 27
    rig.raw.peripherals["newload"] = peripheral


def _online_published(rig: Connected) -> bool:
    return any(
        topic == _AVAILABILITY and payload == "online" for topic, payload, _ in rig.mqtt.published
    )


@pytest.mark.parametrize("cooperative", [True, False])
async def test_fitting_recovery_cancels_old_offline_before_handoff_and_bounds_join(
    connected: Connected,
    monkeypatch: pytest.MonkeyPatch,
    cooperative: bool,
) -> None:
    rig = connected
    entered, release, cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original = rig.mqtt.publish
    fitting = rig.raw.peripherals
    recovery: asyncio.Task[None] | None = None

    async def publish(topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        if topic == _AVAILABILITY and payload == "offline":
            entered.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancelled.set()
                if cooperative:
                    raise
                await release.wait()
        await original(topic, payload, retain, qos)

    monkeypatch.setattr(rig.mqtt, "publish", publish)
    try:
        rig.raw.peripherals = {str(i): _RawPeripheral("1") for i in range(257)}
        await rig.adapter.get_all()
        await entered.wait()
        await turns(100)
        rig.raw.peripherals = fitting
        carriers = await rig.adapter.get_all()
        rig.mqtt.published.clear()
        recovery = asyncio.create_task(rig.bridge.poll_once(carriers))
        await turns(200)
        assert rig.bridge._mesh_routing.records[_AVAILABILITY].desired.payload == "online"
        assert cancelled.is_set(), "fitting recovery must revoke obsolete offline before handoff"
        if cooperative:
            await recovery
            assert rig.mqtt.published[-1] == (_AVAILABILITY, "online", True)
            assert rig.adapter.mesh_admission_open
            assert not rig.adapter.consume_mesh_fatal()
        else:
            await asyncio.sleep(1.05)
            assert not rig.adapter.mesh_admission_open
            assert rig.adapter.consume_mesh_fatal()
            assert not _online_published(rig)
    finally:
        release.set()
        if recovery is not None:
            await asyncio.gather(recovery, return_exceptions=True)
        await turns(100)


@pytest.mark.parametrize("repair", ["blocked_config", "failed_config", "blocked_subscribe"])
async def test_older_carriers_cannot_advertise_online_before_current_routing_repair(
    connected: Connected,
    monkeypatch: pytest.MonkeyPatch,
    repair: str,
) -> None:
    rig = connected
    await rig.bridge.withdraw()
    registry = rig.bridge.mesh_registry
    assert registry is not None
    registry.acquire()
    older = await rig.adapter.get_all()
    _add_load(rig)
    current = await rig.adapter.get_all()
    entered, release = asyncio.Event(), asyncio.Event()
    original_publish, original_subscribe = rig.mqtt.publish, rig.mqtt.subscribe

    async def publish(topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        if "newload" in topic and topic.endswith("/config"):
            if repair == "failed_config":
                raise RuntimeError("temporary current config failure")
            if repair == "blocked_config":
                entered.set()
                await release.wait()
        await original_publish(topic, payload, retain, qos)

    async def subscribe(topic: str) -> None:
        if topic == command_topic("mesh", "newload") and repair == "blocked_subscribe":
            entered.set()
            await release.wait()
        await original_subscribe(topic)

    monkeypatch.setattr(rig.mqtt, "publish", publish)
    monkeypatch.setattr(rig.mqtt, "subscribe", subscribe)
    repairing = asyncio.create_task(rig.bridge.poll_once(current))
    try:
        if repair == "failed_config":
            with pytest.raises(RuntimeError, match="temporary current config"):
                await repairing
        else:
            await entered.wait()
        await turns(100)
        rig.mqtt.published.clear()
        await rig.bridge.poll_once(older)
        assert not _online_published(rig), "current config and membership must finish before online"
        assert rig.adapter.mesh_admission_open
        assert not rig.adapter.consume_mesh_fatal()
    finally:
        release.set()
        await asyncio.gather(repairing, return_exceptions=True)
    monkeypatch.setattr(rig.mqtt, "publish", original_publish)
    await rig.bridge.poll_once(current)
    assert rig.mqtt.published[-1] == (_AVAILABILITY, "online", True)
    assert rig.bridge._resolve_mesh_owner(command_topic("mesh", "newload")) is rig.owner("newload")


async def test_current_owner_with_no_requested_topics_keeps_availability_offline(
    connected: Connected,
) -> None:
    rig = connected
    await rig.bridge.withdraw()
    registry, driver = rig.bridge.mesh_registry, rig.bridge.mesh_driver
    assert registry is not None and driver is not None
    registry.acquire()
    older = await rig.adapter.get_all()
    _add_load(rig)
    current = await rig.adapter.get_all()
    await driver.flush(rig.owner("newload"))
    assert command_topic("mesh", "newload") not in rig.bridge._mesh_routing.records
    rig.mqtt.published.clear()
    await rig.bridge.poll_once(older)
    assert not _online_published(rig), "current owners lacking requested topics are not repaired"
    await rig.bridge.poll_once(current)
    assert rig.mqtt.published[-1] == (_AVAILABILITY, "online", True)


@pytest.mark.parametrize("change", ["new_owner", "rename", "new_aux", "power_sentinel"])
async def test_queued_online_is_revoked_by_current_descriptor_change_before_handoff(
    connected: Connected,
    monkeypatch: pytest.MonkeyPatch,
    change: str,
) -> None:
    rig = connected
    await rig.bridge.withdraw()
    assert rig.raw.peripherals is not None
    load = rig.raw.peripherals["load"]
    if change == "power_sentinel":
        load.variables["power"] = _RawVariable("-1")
    entered, release, cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original = rig.mqtt.publish
    first = True

    async def publish(topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        nonlocal first
        if first and topic == _AVAILABILITY and payload == "online":
            first = False
            entered.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise
        await original(topic, payload, retain, qos)

    monkeypatch.setattr(rig.mqtt, "publish", publish)
    initial = asyncio.create_task(rig.bridge.reconcile())
    try:
        await entered.wait()
        rig.mqtt.published.clear()
        if change == "new_owner":
            _add_load(rig)
        elif change == "rename":
            load.variables["display_name"] = _RawVariable("Current load name")
        elif change == "new_aux":
            load.variables["motion_high_threshold"] = _RawVariable("70")
        else:
            load.variables["power"].value = "10"
        current = await rig.adapter.get_all()
        await turns(100)
        assert cancelled.is_set(), "queued online must lose authorization on descriptor change"
        await initial
        assert not _online_published(rig)
        await rig.bridge.poll_once(current)
        assert rig.mqtt.published[-1] == (_AVAILABILITY, "online", True)
    finally:
        release.set()
        await asyncio.gather(initial, return_exceptions=True)


async def test_current_config_readiness_preserves_shared_bridge_firmware_metadata(
    connected: Connected,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.fakes import _RawDevice

    rig = connected
    await rig.bridge.withdraw()
    original = rig.harness.observers[0].get_device
    hardware = _RawPeripheral("1")
    hardware.peripheral_type = 22
    hardware.variables["current_release_tag"] = _RawVariable("v26.05.20.2")

    async def get_device(device_id: str) -> _RawDevice:
        if device_id == rig.adapter.own_device_id:
            return _RawDevice(device_id, {"hardware_peripheral": hardware})
        result = await original(device_id)
        assert isinstance(result, _RawDevice)
        return result

    monkeypatch.setattr(rig.harness.observers[0], "get_device", get_device)
    rig.bridge._include = None
    rig.mqtt.published.clear()
    await rig.bridge.reconcile()
    assert _online_published(rig), (
        "matching current descriptors retain the shared discovery metadata"
    )
    configs = [
        payload
        for topic, payload, _ in rig.mqtt.published
        if topic == "homeassistant/light/brilliant_mesh_load/config"
    ]
    assert configs and '"sw_version": "v26.05.20.2"' in configs[-1]


async def test_queued_online_revalidates_aux_removal_when_newer_power_keeps_binding(
    connected: Connected,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rig = connected
    await rig.bridge.withdraw()
    assert rig.raw.peripherals is not None
    rig.raw.peripherals["load"].variables["power"] = _RawVariable("10")
    entered, release, cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()
    absent_started, absent_release = asyncio.Event(), asyncio.Event()
    original = rig.mqtt.publish
    first = True

    async def publish(topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        nonlocal first
        if first and topic == _AVAILABILITY and payload == "online":
            first = False
            entered.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise
        await original(topic, payload, retain, qos)

    async def get_peripheral(device_id: str, peripheral_id: str) -> None:
        absent_started.set()
        await absent_release.wait()
        return None

    monkeypatch.setattr(rig.mqtt, "publish", publish)
    monkeypatch.setattr(rig.harness.observers[0], "get_peripheral", get_peripheral, raising=False)
    initial = asyncio.create_task(rig.bridge.reconcile())
    absent: asyncio.Task[BrilliantDevice | None] | None = None
    try:
        await entered.wait()
        original_owner = rig.owner()
        absent = asyncio.create_task(rig.adapter.get_peripheral("ble_mesh", "load"))
        await absent_started.wait()
        await rig.notify(power="11")
        assert not cancelled.is_set()
        absent_release.set()
        assert await absent is None
        assert rig.owner() is original_owner
        await turns(100)
        assert cancelled.is_set(), "unshadowed auxiliary removal invalidates current discovery"
        rig.mqtt.published.clear()
        await initial
        assert not _online_published(rig)
        await rig.bridge.poll_once(await rig.adapter.get_all())
        assert rig.mqtt.published[-1] == (_AVAILABILITY, "online", True)
    finally:
        release.set()
        absent_release.set()
        if absent is not None:
            await asyncio.gather(absent, return_exceptions=True)
        await asyncio.gather(initial, return_exceptions=True)


async def test_online_publish_child_rechecks_current_repair_after_worker_authorization(
    connected: Connected,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rig = connected
    await rig.bridge.withdraw()
    registry = rig.bridge.mesh_registry
    assert registry is not None
    registry.acquire()
    carriers = await rig.adapter.get_all()
    for carrier in carriers:
        await rig.bridge._repair_mesh(carrier.peripheral_id, force=False)
    assert rig.bridge._mesh_online_ready()
    entered, release = asyncio.Event(), asyncio.Event()
    original = rig.mqtt.publish

    async def publish(topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        if "load" in topic and topic.endswith("/config"):
            entered.set()
            await release.wait()
        await original(topic, payload, retain, qos)

    monkeypatch.setattr(rig.mqtt, "publish", publish)
    rig.mqtt.published.clear()
    online = rig.bridge._queue_mesh_availability("online")
    # The topic worker authorizes online, then yields to this force request
    # before the separately scheduled availability publish child runs.
    repairing = asyncio.create_task(rig.bridge._repair_mesh("load", force=True))
    try:
        await entered.wait()
        await online
        assert not _online_published(rig)
        record = rig.bridge._mesh_routing.records[_AVAILABILITY]
        assert record.payload == "offline"
        assert record.applied is not record.desired
    finally:
        release.set()
        await asyncio.gather(online, repairing, return_exceptions=True)
    await rig.bridge._repair_mesh_availability()
    assert rig.mqtt.published[-1] == (_AVAILABILITY, "online", True)
