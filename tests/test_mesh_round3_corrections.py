"""Round-three probes through production adapter, native and MQTT schedules."""

from __future__ import annotations

import asyncio
import copy
from dataclasses import replace
from types import SimpleNamespace

import pytest

from brilliant_mqtt.mesh_routing import TopicRepair
from tests.fakes import _RawDevice, _RawPeripheral, _RawVariable
from tests.mesh_regression_support import turns
from tests.test_mesh_bridge_integration import Connected, connected
from tests.test_mesh_bus_ingress import delta
from tests.test_mesh_discovery_metadata import Firmware, firmware
from tests.test_mesh_review_corrections import VisitedRecords, assert_routing_indexes, delayed_poll

__all__ = ["connected", "firmware"]


def last_availability(rig: Connected) -> str:
    return [p for t, p, _ in rig.mqtt.published if t == "brilliant/mesh/availability"][-1]


async def test_return_during_oversize_read_cannot_maintain_online_write_service(
    connected: Connected, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = connected
    registry = rig.bridge.mesh_registry
    assert registry is not None
    snapshot = copy.deepcopy(rig.raw)
    assert snapshot.peripherals is not None
    snapshot.peripherals.update(
        {f"new-{i}": copy.deepcopy(snapshot.peripherals["blocker"]) for i in range(255)}
    )
    assert len(snapshot.peripherals) == 257
    rig.raw.peripherals = snapshot.peripherals
    topic = "brilliant/mesh/load/set"
    rig.harness.write_release.set()
    async with delayed_poll(rig, monkeypatch, snapshot):
        await rig.mqtt.inject(topic, '{"state":"OFF"}')
        assert len(rig.harness.write_starts) == 1
    await rig.mqtt.inject(topic, '{"state":"ON"}')
    observed = (
        registry.disabled,
        registry.inventory_count,
        last_availability(rig),
        len(rig.harness.write_starts),
    )
    assert observed == (True, 257, "offline", 1), observed


async def test_omitted_owner_repairs_routes_in_both_bridge_scopes(
    firmware: Firmware, monkeypatch: pytest.MonkeyPatch
) -> None:
    fw = firmware
    rig = replace(fw.rig, bridge=fw.bridge)
    await rig.bridge.reconcile()
    owner = rig.owner()
    topic = f"brilliant/{fw.panel}/load/set_enable_motion_score"
    config = f"homeassistant/switch/brilliant_{fw.panel}_load_enable_motion_score/config"
    assert topic in rig.mqtt.subscriptions
    snapshot = copy.deepcopy(rig.raw)
    assert snapshot.peripherals is not None and rig.raw.peripherals is not None
    del snapshot.peripherals["load"]
    async with delayed_poll(rig, monkeypatch, snapshot):
        rig.raw.peripherals["load"].variables.pop("enable_motion_score")
        await rig.notify(on="0")
        await turns(100)
        assert rig.owner() is owner and topic in rig.mqtt.subscriptions
    assert (
        rig.owner() is owner
        and "enable_motion_score" not in owner.materialize(rig.clock()).variables
    )
    records = [p for t, p, _ in rig.mqtt.published if t == config]
    observed = (topic in rig.mqtt.subscriptions, bool(records[-1]))
    assert observed == (False, False), observed


@pytest.mark.parametrize("repair", ["membership", "config"])
async def test_known_owner_with_no_carrier_retries_actual_topic_debt(
    connected: Connected, monkeypatch: pytest.MonkeyPatch, repair: str
) -> None:
    rig = connected
    name = "enable_motion_score"
    topic = f"brilliant/mesh/load/set_{name}"
    config = f"homeassistant/switch/brilliant_mesh_load_{name}/config"
    original_pub, original_unsub = rig.mqtt.publish, rig.mqtt.unsubscribe
    attempts: list[bool] = []

    def fail_once() -> None:
        attempts.append(True)
        if len(attempts) == 1:
            raise RuntimeError("temporary broker error")

    async def unsub(target: str) -> None:
        if repair == "membership" and target == topic:
            fail_once()
        await original_unsub(target)

    async def publish(target: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        if repair == "config" and target == config and payload == "":
            fail_once()
        await original_pub(target, payload, retain, qos)

    monkeypatch.setattr(rig.mqtt, "unsubscribe", unsub)
    monkeypatch.setattr(rig.mqtt, "publish", publish)
    assert rig.raw.peripherals is not None
    rig.raw.peripherals["load"].variables.pop(name)
    obs = rig.harness.observers[0]
    await obs.handle_notification(
        SimpleNamespace(
            updated_device=rig.raw,
            deleted=False,
            modified_peripherals=[delta("load", removed=(name,))],
        )
    )
    await turns(150)
    assert len(attempts) == 1 and name not in rig.owner().materialize(rig.clock()).variables
    native = obs.get_device

    async def ambiguous(device_id: str) -> _RawDevice | None:
        if device_id == "ble_mesh":
            return None
        result = await native(device_id)
        assert result is None or isinstance(result, _RawDevice)
        return result

    monkeypatch.setattr(obs, "get_device", ambiguous)
    await rig.bridge.poll_once()
    await rig.bridge.reconcile()
    latest = [p for t, p, _ in rig.mqtt.published if t == config][-1]
    actual = (len(attempts), topic in rig.mqtt.subscriptions, bool(latest))
    assert actual == (2, False, False), actual


@pytest.mark.parametrize("size", [10, 40])
async def test_single_reconnect_is_linear_in_registered_routes(
    connected: Connected, monkeypatch: pytest.MonkeyPatch, size: int
) -> None:
    rig = connected
    assert rig.raw.peripherals is not None
    rig.raw.peripherals.update(
        {f"extra-{i}": copy.deepcopy(rig.raw.peripherals["blocker"]) for i in range(size - 2)}
    )
    await rig.bridge.poll_once()
    routing = rig.bridge._mesh_routing

    class Counted(VisitedRecords):
        def __getitem__(self, key: str) -> TopicRepair:
            self.visited.append(key)
            return super().__getitem__(key)

    records = Counted(routing.records)
    monkeypatch.setattr(routing, "records", records)
    registry = rig.bridge.mesh_registry
    assert registry is not None
    old = registry.owners
    rig.harness.procs[0].reconnect_cbs[0]()
    rig.adapter.reserve_mesh_boundary("review-check-generation")
    assert all(registry.current(owner.metadata.peripheral_id) is not owner for owner in old)
    count = (size, len(records), len(records.visited))
    assert 0 < len(records.visited) <= 2 * len(records), count
    print(f"reconnect owners/topics/actual visits: {count}")


async def test_second_ordinary_failure_retains_debt_until_next_existing_trigger(
    connected: Connected, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = connected
    topic = "brilliant/mesh/blocker/set"
    attempts: list[bool] = []
    unsub = rig.mqtt.unsubscribe

    async def fail_twice(target: str) -> None:
        if target == topic:
            attempts.append(True)
            if len(attempts) <= 2:
                raise RuntimeError("temporary broker failure")
        await unsub(target)

    monkeypatch.setattr(rig.mqtt, "unsubscribe", fail_twice)
    await rig.bridge.withdraw()
    assert rig.raw.peripherals is not None
    rig.raw.peripherals.pop("blocker")
    await rig.bridge.reconcile()
    assert len(attempts) == 2 and last_availability(rig) == "offline"
    assert topic in rig.bridge._mesh_routing.records and not rig.adapter.consume_mesh_fatal()
    await rig.bridge.poll_once()
    assert len(attempts) == 3 and last_availability(rig) == "online"
    assert topic not in rig.bridge._mesh_routing.records and rig.adapter.mesh_admission_open


async def test_metadata_only_presence_survives_two_prestarted_reads_and_scale_rebind(
    connected: Connected, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = connected
    old = rig.owner()
    assert rig.raw.peripherals is not None
    saved = copy.deepcopy(rig.raw.peripherals["load"])
    saved.variables["max_intensity_value"].value = "1000"
    missing = copy.deepcopy(rig.raw)
    assert missing.peripherals is not None
    missing.peripherals.pop("load")
    entered, release = asyncio.Event(), asyncio.Event()
    observer = rig.harness.observers[0]

    async def read_config(device_id: str, peripheral_id: str) -> _RawPeripheral:
        entered.set()
        await release.wait()
        return saved

    monkeypatch.setattr(observer, "get_peripheral", read_config, raising=False)
    first = asyncio.create_task(rig.adapter.get_peripheral("ble_mesh", "load"))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        async with delayed_poll(rig, monkeypatch, missing):
            live = rig.raw.peripherals["load"]
            live.variables["max_intensity_value"].value = "1000"
            live.variables["display_name"] = _RawVariable("Desk returned")
            await observer.handle_notification(
                SimpleNamespace(
                    updated_device=rig.raw,
                    deleted=False,
                    modified_peripherals=[delta("load", "display_name")],
                )
            )
            await turns(50)
            assert rig.owner() is old and old.metadata.name == "Desk returned"
            release.set()
            await first
            successor = rig.owner()
            assert successor is not old and successor.metadata.name == "Desk returned"
        await turns(100)
        rig.harness.write_release.set()
        topic = "brilliant/mesh/load/set"
        await rig.mqtt.inject(topic, '{"state":"OFF"}')
        assert rig.bridge.mesh_registry is not None
        actual = (
            rig.bridge.mesh_registry.current("load") is successor,
            topic in rig.mqtt.subscriptions,
            len(rig.harness.write_starts),
        )
        assert actual == (True, True, 1), actual
    finally:
        release.set()
        await asyncio.gather(first, return_exceptions=True)


async def test_newer_fitting_inventory_does_supersede_older_oversize(
    connected: Connected, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = connected
    owner = rig.owner()
    snapshot = copy.deepcopy(rig.raw)
    assert snapshot.peripherals is not None and rig.raw.peripherals is not None
    snapshot.peripherals.update(
        {f"gone-{i}": copy.deepcopy(snapshot.peripherals["blocker"]) for i in range(255)}
    )
    async with delayed_poll(rig, monkeypatch, snapshot):
        await rig.adapter.get_all()
    reg = rig.bridge.mesh_registry
    assert reg is not None
    assert reg.inventory_ready and not reg.disabled and reg.inventory_count == 2
    assert rig.owner() is owner and last_availability(rig) == "online"


@pytest.mark.parametrize("channel", ["primary", "auxiliary"])
@pytest.mark.parametrize("failure", [False, True], ids=["accepted", "rejected"])
async def test_capacity_classification_after_native_return_and_later_fitting_recovery(
    connected: Connected, monkeypatch: pytest.MonkeyPatch, channel: str, failure: bool
) -> None:
    rig = connected
    registry = rig.bridge.mesh_registry
    assert registry is not None and rig.raw.peripherals is not None
    fitting = copy.deepcopy(rig.raw.peripherals)
    snapshot = copy.deepcopy(rig.raw)
    assert snapshot.peripherals is not None
    snapshot.peripherals.update(
        {f"oversize-{i}": copy.deepcopy(fitting["blocker"]) for i in range(255)}
    )
    rig.raw.peripherals = snapshot.peripherals
    observer = rig.harness.observers[0]
    native = observer.request_set_variables_in_peripheral

    async def write(peripheral_id: str, values: dict[str, str], *, device_id: str) -> str:
        receipt = await native(peripheral_id, values, device_id=device_id)
        if failure:
            raise RuntimeError("native rejection")
        assert isinstance(receipt, str)
        return receipt

    monkeypatch.setattr(observer, "request_set_variables_in_peripheral", write)
    rig.harness.write_release.set()
    command, payload = (
        ("brilliant/mesh/load/set", '{"state":"OFF"}')
        if channel == "primary"
        else ("brilliant/mesh/load/set_enable_motion_score", "ON")
    )
    async with delayed_poll(rig, monkeypatch, snapshot):
        if failure and channel == "auxiliary":
            with pytest.raises(RuntimeError, match="native rejection"):
                await rig.mqtt.inject(command, payload)
        else:
            await rig.mqtt.inject(command, payload)
        assert rig.harness.write_starts == ["ble_mesh"]
    assert registry.disabled and registry.inventory_count == 257
    assert not registry.inventory_ready and last_availability(rig) == "offline"
    await rig.mqtt.inject("brilliant/mesh/load/set", '{"state":"ON"}')
    assert rig.harness.write_starts == ["ble_mesh"]
    assert_routing_indexes(rig)
    rig.raw.peripherals = fitting
    await rig.bridge.poll_once()
    assert not registry.disabled and registry.inventory_ready and registry.inventory_count == 2
    assert last_availability(rig) == "online"
    await rig.mqtt.inject("brilliant/mesh/load/set", '{"state":"OFF"}')
    assert rig.harness.write_starts == ["ble_mesh", "ble_mesh"]


@pytest.mark.parametrize("repair", ["membership", "config"])
@pytest.mark.parametrize("trigger", ["poll", "reconcile"])
async def test_each_scope_repairs_known_debt_without_carrier_or_healthy_topic_force(
    firmware: Firmware, monkeypatch: pytest.MonkeyPatch, repair: str, trigger: str
) -> None:
    fw = firmware
    rig = replace(fw.rig, bridge=fw.bridge)
    await rig.bridge.reconcile()
    assert rig.raw.peripherals is not None
    owner = rig.owner()
    routing = rig.bridge._mesh_routing
    command = f"brilliant/{fw.panel}/load/set_enable_motion_score"
    config = f"homeassistant/switch/brilliant_{fw.panel}_load_enable_motion_score/config"
    healthy = set(routing.topics("blocker")) | set(routing.topics("load")) - {command, config}
    operations: list[str] = []
    attempts = 0
    publish, subscribe, unsubscribe = rig.mqtt.publish, rig.mqtt.subscribe, rig.mqtt.unsubscribe

    def attempt(target: str) -> None:
        nonlocal attempts
        operations.append(target)
        attempts += 1
        if attempts == 1:
            raise RuntimeError("temporary broker failure")

    async def pub(target: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        if target in healthy:
            operations.append(target)
        if repair == "config" and target == config and not payload:
            attempt(target)
        await publish(target, payload, retain, qos)

    async def sub(target: str) -> None:
        operations.append(target)
        await subscribe(target)

    async def unsub(target: str) -> None:
        if repair == "membership" and target == command:
            attempt(target)
        else:
            operations.append(target)
        await unsubscribe(target)

    monkeypatch.setattr(rig.mqtt, "publish", pub)
    monkeypatch.setattr(rig.mqtt, "subscribe", sub)
    monkeypatch.setattr(rig.mqtt, "unsubscribe", unsub)
    rig.raw.peripherals["load"].variables.pop("enable_motion_score")
    await rig.harness.observers[0].handle_notification(
        SimpleNamespace(
            updated_device=rig.raw,
            deleted=False,
            modified_peripherals=[delta("load", removed=("enable_motion_score",))],
        )
    )
    await turns(150)
    assert attempts == 1
    rig.harness.write_release.set()
    await rig.mqtt.inject(command, "ON")
    assert not rig.harness.write_starts
    native = rig.harness.observers[0].get_device

    async def absent(device_id: str) -> _RawDevice | None:
        if device_id == "ble_mesh":
            fw.reads.append(device_id)
            return None
        result = await native(device_id)
        assert result is None or isinstance(result, _RawDevice)
        return result

    monkeypatch.setattr(rig.harness.observers[0], "get_device", absent)
    before_reads = len(fw.reads)
    await (rig.bridge.poll_once() if trigger == "poll" else rig.bridge.reconcile())
    assert len(fw.reads) == before_reads + 2
    assert rig.owner() is owner and attempts == 2
    assert command not in rig.mqtt.subscriptions
    assert command not in routing.records and config not in routing.records
    assert [p for t, p, _ in rig.mqtt.published if t == config][-1] == ""
    assert not healthy.intersection(operations)
    assert_routing_indexes(rig)


async def test_known_config_update_retries_settled_debt_without_carrier(
    firmware: Firmware, monkeypatch: pytest.MonkeyPatch
) -> None:
    fw = firmware
    rig = replace(fw.rig, bridge=fw.bridge)
    await rig.bridge.reconcile()
    owner = rig.owner()
    attempts = 0
    publish = rig.mqtt.publish

    async def fail_once(topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        nonlocal attempts
        if topic == fw.topic:
            attempts += 1
            if attempts == 1:
                raise RuntimeError("temporary known config failure")
        await publish(topic, payload, retain, qos)

    monkeypatch.setattr(rig.mqtt, "publish", fail_once)
    await rig.notify(display_name="New known name")
    await turns(150)
    assert attempts == 1
    record = rig.bridge._mesh_routing.records[fw.topic]
    assert record.applied is not None and record.applied is not record.desired
    assert record.worker is not None and record.worker.done()
    native = rig.harness.observers[0].get_device

    async def absent(device_id: str) -> _RawDevice | None:
        if device_id == "ble_mesh":
            fw.reads.append(device_id)
            return None
        result = await native(device_id)
        assert result is None or isinstance(result, _RawDevice)
        return result

    monkeypatch.setattr(rig.harness.observers[0], "get_device", absent)
    before = len(fw.reads)
    await rig.bridge.poll_once()
    assert rig.owner() is owner and attempts == 2
    assert len(fw.reads) == before + 2
    assert record.applied is record.desired
    assert "New known name" in fw.configs()[-1]
    assert_routing_indexes(rig)


@pytest.mark.parametrize("presence", ["metadata", "existence"])
async def test_shadowed_rebind_keeps_only_exact_outstanding_completion(
    connected: Connected, monkeypatch: pytest.MonkeyPatch, presence: str
) -> None:
    rig = connected
    assert rig.raw.peripherals is not None
    old = rig.owner()
    command = asyncio.create_task(rig.mqtt.inject("brilliant/mesh/load/set", '{"state":"OFF"}'))
    await turns()
    ticket = rig.adapter.outstanding_mesh_ticket("load")
    assert ticket is not None and ticket.issued_at is not None
    saved = copy.deepcopy(rig.raw.peripherals["load"])
    saved.variables["max_intensity_value"].value = "1000"
    missing = copy.deepcopy(rig.raw)
    assert missing.peripherals is not None
    missing.peripherals.pop("load")
    entered, release = asyncio.Event(), asyncio.Event()

    async def scoped(device_id: str, peripheral_id: str) -> _RawPeripheral:
        entered.set()
        await release.wait()
        return saved

    observer = rig.harness.observers[0]
    monkeypatch.setattr(observer, "get_peripheral", scoped, raising=False)
    first = asyncio.create_task(rig.adapter.get_peripheral("ble_mesh", "load"))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        async with delayed_poll(rig, monkeypatch, missing):
            rig.raw.peripherals["load"].variables["max_intensity_value"].value = "1000"
            rig.raw.peripherals["load"].variables["display_name"] = _RawVariable("Present")
            await observer.handle_notification(
                SimpleNamespace(
                    updated_device=rig.raw,
                    deleted=False,
                    modified_peripherals=[
                        delta("load", *(["display_name"] if presence == "metadata" else []))
                    ],
                )
            )
            await turns()
            assert rig.owner() is old
            release.set()
            await first
            successor = rig.owner()
            assert successor is not old
            assert successor.view.completion_hold is not None
            assert successor.view.completion_hold.ticket is ticket
            assert successor.view.command_id == 0 and successor.view.window is None
            assert successor.view.observed == () and successor.view.evidence == ()
            assert successor.view.targets == () and successor.view.receipt is None
        assert rig.owner() is successor
        assert rig.bridge._resolve_mesh_owner("brilliant/mesh/load/set") is successor
        rig.harness.write_release.set()
        await command
        await turns()
        assert ticket.returned_at is not None and successor.view.completion_hold is None
        assert successor.view.window is None and successor.view.receipt is None
        assert successor.render(rig.clock())["state"] is None
        await rig.notify(on="0")
        assert successor.render(rig.clock())["state"] == "OFF"
        assert rig.harness.write_starts == ["ble_mesh"]
        assert rig.harness.max_writes_in_flight == 1
    finally:
        release.set()
        rig.harness.write_release.set()
        await asyncio.gather(first, command, return_exceptions=True)


async def test_indexed_revoke_cancels_current_child_only_with_exact_owner_identity(
    connected: Connected, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = connected
    old = rig.owner()
    rig.harness.procs[0].reconnect_cbs[0]()
    await rig.bridge.poll_once()
    current = rig.owner()
    assert current is not old
    configs = {
        "homeassistant/light/brilliant_mesh_load/config": "load",
        "homeassistant/light/brilliant_mesh_blocker/config": "blocker",
    }
    entered = {pid: asyncio.Event() for pid in configs.values()}
    canceled = {pid: asyncio.Event() for pid in configs.values()}
    release = asyncio.Event()
    publish = rig.mqtt.publish

    async def gated(topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        if topic in configs:
            pid = configs[topic]
            entered[pid].set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                canceled[pid].set()
                raise
        await publish(topic, payload, retain, qos)

    monkeypatch.setattr(rig.mqtt, "publish", gated)
    tasks = [asyncio.create_task(rig.bridge._repair_mesh(pid, force=True)) for pid in entered]
    try:
        for event in entered.values():
            await asyncio.wait_for(event.wait(), 2)
        routing = rig.bridge._mesh_routing
        routing.revoke(old)
        await turns()
        assert not any(event.is_set() for event in canceled.values())
        routing.revoke(current)
        await asyncio.wait_for(canceled["load"].wait(), 2)
        assert not canceled["blocker"].is_set()
    finally:
        release.set()
        await asyncio.gather(*tasks)
    assert_routing_indexes(rig)
    assert not rig.adapter.consume_mesh_fatal()
