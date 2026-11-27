"""Production adapter schedules for bounded inventory and current mesh routes."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator

import pytest

from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.discovery import command_topic
from brilliant_mqtt.write_admission import AdmissionTicket, WriteCancelled
from tests.fakes import _RawPeripheral
from tests.mesh_regression_support import turns
from tests.test_mesh_bridge_integration import Connected, connected

__all__ = ["connected"]


class CountOnlyInventory(dict[str, _RawPeripheral]):
    def __len__(self) -> int:
        return 257

    def __iter__(self) -> Iterator[str]:
        raise AssertionError("oversized raw IDs must not be cloned or normalized")


async def test_oversized_inventory_preflights_before_ids_and_recovers_same_read(
    connected: Connected,
) -> None:
    rig = connected
    registry = rig.bridge.mesh_registry
    driver = rig.bridge.mesh_driver
    assert registry is not None and driver is not None
    original = rig.owner()
    raw = rig.raw.peripherals
    assert raw is not None
    rig.raw.peripherals = CountOnlyInventory()

    for _ in range(4):
        assert await rig.adapter.get_all() == []
        await turns()
        assert registry.disabled
        assert len(registry.owners) + len(registry.retired) <= 256
        assert (
            len(
                {owner.metadata.peripheral_id for owner in registry.owners}
                | set(registry.retired)
                | driver.peripheral_ids
                | rig.bridge._mesh_routing.peripheral_ids
            )
            <= 256
        )
        assert rig.adapter.mesh_admission_open
        assert not rig.adapter.consume_mesh_fatal()
        assert rig.harness.write_starts == []
    assert registry.current("load") is None
    assert original.view.window is None

    rig.raw.peripherals = raw
    carriers = await rig.adapter.get_all()
    assert not registry.disabled
    assert {device.peripheral_id for device in carriers} == {"load", "blocker"}
    assert rig.owner() is not original
    assert rig.owner().materialize(rig.clock()).variables["on"].value == "1"


async def test_capacity_revokes_waiting_ticket_and_finishes_admitted_cleanup(
    connected: Connected,
) -> None:
    rig = connected
    registry = rig.bridge.mesh_registry
    assert registry is not None
    blocker_ticket = AdmissionTicket(mesh_owner=rig.owner("blocker"))
    blocker = asyncio.create_task(
        rig.adapter.set_variables("ble_mesh", "blocker", [VarSet("on", "1")], ticket=blocker_ticket)
    )
    await turns()
    waiting_ticket = AdmissionTicket(mesh_owner=rig.owner())
    waiting = asyncio.create_task(
        rig.adapter.set_variables("ble_mesh", "load", [VarSet("on", "0")], ticket=waiting_ticket)
    )
    await turns()
    raw = rig.raw.peripherals
    assert raw is not None
    rig.raw.peripherals = {f"overflow-{index}": _RawPeripheral("1") for index in range(257)}
    await rig.adapter.get_all()
    await turns(100)

    assert waiting_ticket.settled
    assert registry.disabled
    assert waiting_ticket.issued_at is None
    assert isinstance(waiting_ticket.outcome, WriteCancelled)
    assert registry.current("load") is None
    assert not registry.retired
    assert command_topic("mesh", "load") in rig.mqtt.unsubscriptions
    assert command_topic("mesh", "load") not in rig.bridge._by_cmd_topic
    assert not rig.bridge.mesh_driver or not rig.bridge.mesh_driver.timers
    assert rig.adapter.mesh_admission_open
    assert blocker_ticket.issued_at is not None
    rig.harness.write_release.set()
    await blocker
    await asyncio.gather(waiting, return_exceptions=True)
    assert rig.harness.write_starts == ["ble_mesh"]


async def test_reappearance_repairs_delayed_old_unsubscribe_without_another_resync(
    connected: Connected,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rig = connected
    topic = command_topic("mesh", "load")
    actual = set(rig.bridge._subscribed)
    entered, release = asyncio.Event(), asyncio.Event()
    original_unsubscribe, original_subscribe = rig.mqtt.unsubscribe, rig.mqtt.subscribe

    async def unsubscribe(command: str) -> None:
        if command == topic:
            entered.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                await release.wait()
        actual.discard(command)
        await original_unsubscribe(command)

    async def subscribe(command: str) -> None:
        actual.add(command)
        await original_subscribe(command)

    monkeypatch.setattr(rig.mqtt, "unsubscribe", unsubscribe)
    monkeypatch.setattr(rig.mqtt, "subscribe", subscribe)
    assert rig.raw.peripherals is not None
    load = rig.raw.peripherals.pop("load")
    await rig.adapter.get_all()
    await entered.wait()
    old = rig.bridge.mesh_registry.retired["load"] if rig.bridge.mesh_registry else None
    rig.raw.peripherals["load"] = load
    await rig.adapter.get_all()
    successor = rig.owner()
    assert successor is not old
    repair = asyncio.create_task(rig.bridge._repair_mesh("load", force=True))
    await turns()
    release.set()
    await repair
    await turns()
    assert topic in actual
    assert rig.bridge._resolve_mesh_owner(topic) is successor
    rig.harness.write_release.set()
    await rig.mqtt.inject(topic, '{"state":"OFF"}')
    assert rig.harness.write_starts == ["ble_mesh"]


async def test_concurrent_reconcile_serializes_each_config_topic(
    connected: Connected,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rig = connected
    entered, release = asyncio.Event(), asyncio.Event()
    active: dict[str, int] = {}
    maximum = 0
    original = rig.mqtt.publish

    async def publish(topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        nonlocal maximum
        if topic.endswith("/config") and "load" in topic:
            active[topic] = active.get(topic, 0) + 1
            maximum = max(maximum, active[topic])
            entered.set()
            await release.wait()
            try:
                await original(topic, payload, retain, qos)
            finally:
                active[topic] -= 1
        else:
            await original(topic, payload, retain, qos)

    monkeypatch.setattr(rig.mqtt, "publish", publish)
    first = asyncio.create_task(rig.bridge.reconcile())
    await entered.wait()
    second = asyncio.create_task(rig.bridge.reconcile())
    await turns()
    release.set()
    await asyncio.gather(first, second)
    assert maximum == 1


async def test_older_fitting_read_cannot_reopen_newer_capacity_disable(
    connected: Connected,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.fakes import _RawDevice

    rig = connected
    original = rig.harness.observers[0].get_device
    entered, release = asyncio.Event(), asyncio.Event()
    snapshot = _RawDevice("ble_mesh", dict(rig.raw.peripherals or {}))
    first = True

    async def get_device(device_id: str) -> _RawDevice:
        nonlocal first
        if device_id == "ble_mesh" and first:
            first = False
            entered.set()
            await release.wait()
            return snapshot
        result = await original(device_id)
        assert isinstance(result, _RawDevice)
        return result

    monkeypatch.setattr(rig.harness.observers[0], "get_device", get_device)
    old = asyncio.create_task(rig.adapter.get_all())
    await entered.wait()
    rig.raw.peripherals = {f"overflow-{i}": _RawPeripheral("1") for i in range(257)}
    await rig.adapter.get_all()
    release.set()
    assert await old == []
    registry = rig.bridge.mesh_registry
    assert registry is not None and registry.disabled
    assert not registry.owners
    await turns(100)
    rig.raw.peripherals = snapshot.peripherals
    assert await rig.adapter.get_all()
    assert not registry.disabled


async def test_cold_overflow_offline_precedes_read_without_blocking_heartbeat_then_repairs_online(
    connected: Connected,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from brilliant_mqtt.bridge import Bridge
    from tests.fakes import _RawDevice

    rig = connected
    await rig.bridge.withdraw()
    registry = rig.bridge.mesh_registry
    assert registry is not None
    fitting = rig.raw.peripherals
    rig.raw.peripherals = {f"overflow-{i}": _RawPeripheral("1") for i in range(257)}
    rig.mqtt.published.clear()
    entered, release, read = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original_publish = rig.mqtt.publish
    original_read = rig.harness.observers[0].get_device
    wired_load = _RawPeripheral("1")
    wired_load.peripheral_type = 27
    beats: list[bool] = []
    rig.bridge._heartbeat = lambda: beats.append(True)

    async def publish(topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        if topic == "brilliant/mesh/availability" and payload == "offline":
            entered.set()
            await release.wait()
        await original_publish(topic, payload, retain, qos)

    async def get_device(device_id: str) -> _RawDevice:
        if device_id == "ble_mesh":
            read.set()
        else:
            return _RawDevice(device_id, {"wired-load": wired_load})
        result = await original_read(device_id)
        assert isinstance(result, _RawDevice)
        return result

    monkeypatch.setattr(rig.mqtt, "publish", publish)
    monkeypatch.setattr(rig.harness.observers[0], "get_device", get_device)
    cold = asyncio.create_task(rig.bridge.reconcile())
    await read.wait()
    await turns()
    assert entered.is_set(), "cold acquisition must enqueue offline before inventory completes"
    assert beats
    assert registry.disabled
    release.set()
    await cold
    for _ in range(3):
        await rig.bridge.reconcile()
        await rig.bridge.poll_once(await rig.adapter.get_all())
    assert [
        payload
        for topic, payload, _ in rig.mqtt.published
        if topic == "brilliant/mesh/availability"
    ] == ["offline"]
    assert not registry.owners and not registry.retired
    assert not rig.adapter.consume_mesh_fatal()

    wired = Bridge(rig.adapter, rig.mqtt, "own", include=lambda device_id: device_id != "ble_mesh")
    await wired.reconcile()
    assert ("brilliant/own/availability", "online", True) in rig.mqtt.published
    rig.harness.write_release.set()
    await rig.mqtt.inject(command_topic("own", "wired-load"), '{"state":"OFF"}')
    assert rig.harness.write_starts == [rig.adapter.own_device_id]
    await wired.shutdown_mesh_feedback()
    await wired.shutdown_wired_feedback()

    rig.raw.peripherals = fitting
    config_entered, config_release = asyncio.Event(), asyncio.Event()

    async def recovering_publish(
        topic: str,
        payload: str,
        retain: bool = False,
        qos: int = 0,
    ) -> None:
        if topic.endswith("/config"):
            config_entered.set()
            await config_release.wait()
        await original_publish(topic, payload, retain, qos)

    monkeypatch.setattr(rig.mqtt, "publish", recovering_publish)
    recovery = asyncio.create_task(rig.bridge.reconcile())
    await config_entered.wait()
    assert not any(
        topic == "brilliant/mesh/availability" and payload == "online"
        for topic, payload, _ in rig.mqtt.published
    )
    config_release.set()
    await recovery
    assert ("brilliant/mesh/availability", "online", True) in rig.mqtt.published
    assert rig.bridge._resolve_mesh_owner(command_topic("mesh", "load")) is rig.owner()


async def test_oversized_push_counts_before_copying_masks_or_fields(
    connected: Connected,
) -> None:
    from types import SimpleNamespace

    class PoisonMask:
        @property
        def peripheral_id(self) -> str:
            raise AssertionError("overflow must preflight before copying mask IDs")

    rig = connected
    rig.raw.peripherals = CountOnlyInventory()
    await rig.harness.observers[0].handle_notification(
        SimpleNamespace(
            updated_device=rig.raw,
            deleted=False,
            modified_peripherals=[PoisonMask()],
        )
    )
    registry = rig.bridge.mesh_registry
    assert registry is not None
    assert registry.current("load") is None
    assert registry.disabled
    assert rig.adapter.mesh_admission_open
    assert not rig.adapter.consume_mesh_fatal()


async def test_withdrawn_pending_topics_share_capacity_with_next_acquisition(
    connected: Connected,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rig = connected
    entered, release = asyncio.Event(), asyncio.Event()
    original = rig.mqtt.unsubscribe

    async def unsubscribe(topic: str) -> None:
        entered.set()
        await release.wait()
        await original(topic)

    monkeypatch.setattr(rig.mqtt, "unsubscribe", unsubscribe)
    withdrawal = asyncio.create_task(rig.bridge.withdraw())
    await entered.wait()
    rig.raw.peripherals = {f"new-{i}": _RawPeripheral("1") for i in range(255)}
    acquiring = asyncio.create_task(rig.bridge.reconcile())
    await turns(100)
    registry = rig.bridge.mesh_registry
    assert registry is not None
    try:
        assert not registry.owners, "255 new IDs plus two pending old IDs exceed the shared ceiling"
        assert registry.disabled
        assert rig.adapter.mesh_admission_open
    finally:
        release.set()
        await asyncio.gather(withdrawal, acquiring)


@pytest.mark.parametrize("blocked", ["config", "subscribe"])
async def test_old_topic_completion_across_withdraw_acquire_and_retype_repairs_current(
    connected: Connected,
    monkeypatch: pytest.MonkeyPatch,
    blocked: str,
) -> None:
    from brilliant_mqtt.discovery import config_topic
    from brilliant_mqtt.mapping import entities_for

    rig = connected
    if blocked == "subscribe":
        await rig.bridge.withdraw()
    entered, canceled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    first = True
    retained = {topic: payload for topic, payload, _ in rig.mqtt.published}
    actual = set(rig.bridge._subscribed)
    original_publish, original_subscribe = rig.mqtt.publish, rig.mqtt.subscribe
    topic = command_topic("mesh", "load")
    light_config = "homeassistant/light/brilliant_mesh_load/config"

    async def gate() -> None:
        entered.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            canceled.set()
            await release.wait()

    async def publish(name: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        nonlocal first
        if blocked == "config" and first and name == light_config and payload:
            first = False
            await gate()
        retained[name] = payload
        await original_publish(name, payload, retain, qos)

    async def subscribe(name: str) -> None:
        nonlocal first
        if blocked == "subscribe" and first and name == topic:
            first = False
            await gate()
        actual.add(name)
        await original_subscribe(name)

    monkeypatch.setattr(rig.mqtt, "publish", publish)
    monkeypatch.setattr(rig.mqtt, "subscribe", subscribe)
    old = asyncio.create_task(rig.bridge.reconcile())
    await entered.wait()
    withdrawal = asyncio.create_task(rig.bridge.withdraw())
    await canceled.wait()
    assert rig.raw.peripherals is not None
    rig.raw.peripherals["load"].peripheral_type = 40
    acquiring = asyncio.create_task(rig.bridge.reconcile())
    await turns(60)
    successor = rig.owner()
    release.set()
    await asyncio.gather(old, withdrawal, acquiring)
    assert topic in actual
    assert rig.bridge._resolve_mesh_owner(topic) is successor
    if blocked == "config":
        assert retained[light_config] == ""
    configs = {
        config_topic(descriptor)
        for descriptor in entities_for(successor.materialize(rig.clock()), "mesh")
    }
    assert configs
    assert all(retained[name] for name in configs)
    rig.harness.write_release.set()
    await rig.mqtt.inject(topic, '{"state":"OFF"}')
    assert rig.harness.write_starts == ["ble_mesh"]
    assert rig.adapter.mesh_admission_open


async def test_capacity_recovers_through_existing_hot_poll_carriers_without_extra_read(
    connected: Connected,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rig = connected
    fitting = rig.raw.peripherals
    rig.raw.peripherals = {f"overflow-{i}": _RawPeripheral("1") for i in range(257)}
    await rig.bridge.poll_once(await rig.adapter.get_all())
    registry = rig.bridge.mesh_registry
    assert registry is not None and registry.disabled
    assert ("brilliant/mesh/availability", "offline", True) in rig.mqtt.published
    rig.raw.peripherals = fitting
    reads: list[str] = []
    native = rig.harness.observers[0].get_device

    async def get_device(device_id: str) -> object:
        reads.append(device_id)
        return await native(device_id)

    monkeypatch.setattr(rig.harness.observers[0], "get_device", get_device)
    carriers = await rig.adapter.get_all()
    before = len(reads)
    await rig.bridge.poll_once(carriers)
    assert reads == [rig.adapter.own_device_id, "ble_mesh"]
    assert len(reads) == before
    assert not registry.disabled
    assert rig.bridge._resolve_mesh_owner(command_topic("mesh", "load")) is rig.owner()
    assert rig.mqtt.published[-1] == ("brilliant/mesh/availability", "online", True)


async def test_churn_reconnect_cleanup_and_topic_maps_stay_within_shared_ceiling(
    connected: Connected,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rig = connected
    registry, driver = rig.bridge.mesh_registry, rig.bridge.mesh_driver
    assert registry is not None and driver is not None
    original = rig.mqtt.unsubscribe

    async def unavailable(topic: str) -> None:
        raise RuntimeError("broker temporarily unavailable")

    monkeypatch.setattr(rig.mqtt, "unsubscribe", unavailable)
    for index in range(260):
        pid = f"churn-{index}"
        peripheral = _RawPeripheral("1")
        peripheral.peripheral_type = 27
        rig.raw.peripherals = {pid: peripheral}
        await rig.adapter.get_all()
        await rig.bridge._repair_mesh(pid, force=False)
        await turns(60)
        if index % 40 == 0:
            rig.harness.procs[0].reconnect_cbs[0]()
            await turns(60)
        assert (
            len(
                {owner.metadata.peripheral_id for owner in registry.owners}
                | set(registry.retired)
                | driver.peripheral_ids
                | rig.bridge._mesh_routing.peripheral_ids
            )
            <= 256
        )
        assert len(registry.owners) + len(registry.retired) <= 256
        assert len(rig.bridge._devices) <= 256
        assert len(rig.bridge._mesh_descriptors) <= 256
        assert len(driver.peripheral_ids) <= 256
        assert len(driver._lanes) <= 256
        assert len(driver.tasks) <= 4 * 256
        assert len(rig.bridge._mesh_routing.peripheral_ids) <= 256
        assert len(rig.bridge._mesh_routing.records) <= 4 * 256 + 1
        assert len({pid for pid, _ in rig.bridge._by_cmd_topic.values()}) <= 256
        assert (
            len({owner.metadata.peripheral_id for owner in rig.bridge._mesh_route_owners.values()})
            <= 256
        )
        assert not rig.adapter._field_eras
        assert not rig.adapter._write_admissions
        assert rig.adapter.mesh_admission_open
        assert not rig.adapter.consume_mesh_fatal()
    assert registry.disabled
    assert not registry.owners
    assert len(registry.retired) == 256
    monkeypatch.setattr(rig.mqtt, "unsubscribe", original)
    await rig.bridge._flush_retired_mesh()
    await turns(60)
    assert not registry.retired
    assert not rig.bridge._devices
    assert not rig.bridge._mesh_descriptors
    assert not rig.bridge._mesh_route_owners
    assert not rig.bridge._by_cmd_topic
    assert not rig.bridge._mesh_routing.peripheral_ids
    assert not driver.peripheral_ids
    await rig.bridge.poll_once(await rig.adapter.get_all())
    assert not registry.disabled
    assert len(registry.owners) == 1
    assert rig.bridge._resolve_mesh_owner(command_topic("mesh", "churn-259")) is rig.owner(
        "churn-259"
    )
    assert rig.harness.write_starts == []


async def test_withdraw_cancels_unhanded_availability_without_publishing_shared_status(
    connected: Connected,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rig = connected
    await rig.bridge.withdraw()
    rig.mqtt.published.clear()
    entered = asyncio.Event()
    original = rig.mqtt.publish

    async def publish(topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        if topic == "brilliant/mesh/availability":
            entered.set()
            await asyncio.Event().wait()
        await original(topic, payload, retain, qos)

    monkeypatch.setattr(rig.mqtt, "publish", publish)
    reconcile = asyncio.create_task(rig.bridge.reconcile())
    await entered.wait()
    await rig.bridge.withdraw()
    await reconcile
    assert not any(topic == "brilliant/mesh/availability" for topic, _, _ in rig.mqtt.published)
    assert not rig.bridge._mesh_routing.records
    assert rig.adapter.mesh_admission_open


async def test_obsolete_noncooperative_topic_child_latches_existing_fatal(
    connected: Connected,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rig = connected
    entered, release = asyncio.Event(), asyncio.Event()
    original = rig.mqtt.publish

    async def publish(topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        if topic == "homeassistant/light/brilliant_mesh_load/config":
            entered.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                await release.wait()
        await original(topic, payload, retain, qos)

    monkeypatch.setattr(rig.mqtt, "publish", publish)
    reconcile = asyncio.create_task(rig.bridge.reconcile())
    await entered.wait()
    try:
        await rig.bridge.withdraw()
        await asyncio.sleep(0.05)
        assert not rig.adapter.mesh_admission_open
        assert rig.adapter.consume_mesh_fatal()
        assert rig.harness.write_starts == []
    finally:
        release.set()
        await asyncio.gather(reconcile, return_exceptions=True)
        await turns(60)


async def test_stable_oversized_inventory_remains_offline_across_reacquisitions(
    connected: Connected,
) -> None:
    rig = connected
    rig.raw.peripherals = {f"overflow-{i}": _RawPeripheral("1") for i in range(257)}
    rig.mqtt.published.clear()
    registry, driver = rig.bridge.mesh_registry, rig.bridge.mesh_driver
    assert registry is not None and driver is not None
    for _ in range(5):
        await rig.bridge.reconcile()
        await rig.bridge.poll_once(await rig.adapter.get_all())
        assert registry.disabled
        assert not registry.owners and not registry.retired
        assert not driver.tasks
        assert not rig.bridge._mesh_routing.peripheral_ids
        assert not rig.adapter.consume_mesh_fatal()
        before = len(rig.mqtt.published)
        await rig.bridge.withdraw()
        assert len(rig.mqtt.published) == before
    assert rig.harness.write_starts == []
    assert not any(
        topic == "brilliant/mesh/availability" and payload == "online"
        for topic, payload, _ in rig.mqtt.published
    )


async def test_retype_churn_with_blocked_state_keeps_owner_and_worker_maps_bounded(
    connected: Connected,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rig = connected
    registry, driver = rig.bridge.mesh_registry, rig.bridge.mesh_driver
    assert registry is not None and driver is not None
    entered, release = asyncio.Event(), asyncio.Event()
    first = True
    original = rig.mqtt.publish

    async def publish(topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        nonlocal first
        if first and topic == "brilliant/mesh/load/state":
            first = False
            entered.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                await release.wait()
        await original(topic, payload, retain, qos)

    monkeypatch.setattr(rig.mqtt, "publish", publish)
    assert rig.raw.peripherals is not None
    load = rig.raw.peripherals["load"]
    load.variables["on"].value = "0"
    await rig.adapter.get_all()
    await entered.wait()
    for index in range(40):
        load.peripheral_type = 40 if index % 2 == 0 else 27
        await rig.adapter.get_all()
        await turns(20)
        assert len(registry.owners) == 2
        assert not registry.retired
        assert len(driver._lanes) <= 3
        assert len(driver.tasks) <= 6
        assert len(rig.bridge._devices) <= 2
        assert len(rig.bridge._mesh_descriptors) <= 2
        assert len(rig.bridge._mesh_routing.peripheral_ids) <= 2
        assert rig.adapter.mesh_admission_open
        assert not rig.adapter.consume_mesh_fatal()
    release.set()
    await turns(100)
    await rig.bridge.poll_once(await rig.adapter.get_all())
    assert len(driver._lanes) <= 2
    assert rig.bridge._resolve_mesh_owner(command_topic("mesh", "load")) is rig.owner()
    assert rig.harness.write_starts == []
