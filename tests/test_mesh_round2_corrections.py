"""Round-two review schedules through production adapter and MQTT boundaries."""

from __future__ import annotations

import asyncio
import copy
import json
from collections.abc import Awaitable, Coroutine
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest

from brilliant_mqtt.mesh_driver import _Lane
from brilliant_mqtt.mesh_routing import TopicRepair
from brilliant_mqtt.mesh_settlement import MeshSettlement
from tests.fakes import _RawDevice, _RawPeripheral
from tests.mesh_regression_support import turns
from tests.test_mesh_bridge_integration import Connected, connected
from tests.test_mesh_bus_ingress import delta
from tests.test_mesh_review_corrections import cold_connected, delayed_poll, states

if TYPE_CHECKING:
    from _collections_abc import dict_items

__all__ = ["cold_connected", "connected"]


def native_load() -> _RawPeripheral:
    raw = _RawPeripheral("1")
    raw.peripheral_type = 27
    return raw


def availability(rig: Connected) -> str:
    return [
        payload
        for topic, payload, _ in rig.mqtt.published
        if topic == "brilliant/mesh/availability"
    ][-1]


async def test_cold_floor_does_not_authorize_incomplete_online(
    cold_connected: Connected, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = cold_connected
    assert rig.raw.peripherals is not None
    rig.raw.peripherals["survives"] = native_load()
    snapshot = copy.deepcopy(rig.raw)
    assert snapshot.peripherals is not None
    snapshot.peripherals["deleted"] = native_load()
    async with delayed_poll(rig, monkeypatch, snapshot, cold=True):
        await rig.harness.observers[0].handle_notification(
            SimpleNamespace(
                updated_device=rig.raw,
                deleted=False,
                modified_peripherals=[delta("deleted", deleted=True)],
            )
        )
    registry = rig.bridge.mesh_registry
    assert registry is not None
    assert availability(rig) == "offline"
    assert not registry.inventory_ready
    assert not registry.owners
    assert not states(rig, "survives")
    await rig.bridge.poll_once()
    assert registry.inventory_ready and len(registry.owners) == 1
    assert states(rig, "survives")[-1]["state"] == "ON"
    assert "brilliant/mesh/survives/set" in rig.bridge._subscribed
    assert availability(rig) == "online"


async def test_stale_first_sighting_does_not_evict_full_fitting_inventory(
    connected: Connected, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = connected
    registry = rig.bridge.mesh_registry
    assert registry is not None and rig.raw.peripherals is not None
    owner = rig.owner()
    snapshot = copy.deepcopy(rig.raw)
    assert snapshot.peripherals is not None
    snapshot.peripherals["deleted"] = native_load()
    async with delayed_poll(rig, monkeypatch, snapshot):
        rig.raw.peripherals.update({f"current-{idx}": native_load() for idx in range(254)})
        await rig.adapter.get_all()
        assert not registry.disabled and len(registry.owners) == 256
        await rig.notify(enable_motion_score="1")
    assert (registry.disabled, len(registry.owners), registry.inventory_count) == (False, 256, 256)
    assert rig.owner() is owner
    assert owner.materialize(rig.clock()).variables["enable_motion_score"].value == "1"
    assert rig.bridge._resolve_mesh_owner("brilliant/mesh/load/set") is owner
    # A genuinely current 257th peripheral must still disable the combined inventory.
    rig.raw.peripherals["newer-257th"] = native_load()
    await rig.adapter.get_all()
    assert registry.disabled and not registry.inventory_ready
    assert len(registry.owners) + len(registry.retired) <= 256


async def test_valid_unsuback_can_take_longer_than_obsolete_join(
    connected: Connected, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = connected
    topic = "brilliant/mesh/blocker/set"
    original = rig.mqtt.unsubscribe
    count = 0
    accepted: list[str] = []

    async def delayed(target: str) -> None:
        nonlocal count
        if target == topic:
            count += 1
            if count == 1:
                raise RuntimeError("temporary broker failure")
            await asyncio.sleep(1.15)
        await original(target)
        accepted.append(target)

    monkeypatch.setattr(rig.mqtt, "unsubscribe", delayed)
    await rig.bridge.withdraw()
    assert rig.raw.peripherals is not None
    del rig.raw.peripherals["blocker"]
    await rig.bridge.reconcile()
    assert (count, topic in accepted, rig.adapter.consume_mesh_fatal()) == (2, True, False)
    assert rig.adapter.mesh_admission_open
    assert availability(rig) == "online"


@pytest.mark.parametrize("command", [False, True], ids=["field_absence", "native_command"])
async def test_scope_unknown_does_not_discard_older_unshadowed_field_absence(
    connected: Connected, monkeypatch: pytest.MonkeyPatch, command: bool
) -> None:
    rig = connected
    snapshot = copy.deepcopy(rig.raw)
    assert snapshot.peripherals is not None and rig.raw.peripherals is not None
    del snapshot.peripherals["load"]
    owner = rig.owner()
    assert owner.materialize(rig.clock()).variables["enable_motion_score"].value == "0"
    observer = rig.harness.observers[0]
    async with delayed_poll(rig, monkeypatch, snapshot):
        if command:
            rig.raw.peripherals["load"].variables.pop("enable_motion_score")
        await rig.notify(on="0")
        actual_read = observer.get_device

        async def unknown(device_id: str) -> _RawDevice | None:
            if device_id == "ble_mesh":
                return None
            result = await actual_read(device_id)
            assert isinstance(result, _RawDevice)
            return result

        with monkeypatch.context() as scope:
            scope.setattr(observer, "get_device", unknown)
            await rig.adapter.get_all()
    registry = rig.bridge.mesh_registry
    assert registry is not None and not registry.inventory_ready
    assert rig.owner() is owner
    assert states(rig)[-1]["state"] is None
    if command:
        rig.harness.write_release.set()
        await rig.mqtt.inject("brilliant/mesh/load/set_enable_motion_score", "ON")
        assert not rig.harness.write_starts
    assert "enable_motion_score" not in owner.materialize(rig.clock()).variables
    assert "brilliant/mesh/load/set_enable_motion_score" not in rig.bridge._by_cmd_topic


class VisitedLanes(dict[MeshSettlement, _Lane]):
    def __init__(self, lanes: dict[MeshSettlement, _Lane]) -> None:
        super().__init__(lanes)
        self.visited: list[str] = []

    def items(self) -> dict_items[MeshSettlement, _Lane]:
        self.visited.extend(owner.metadata.peripheral_id for owner in super().keys())
        return super().items()


@pytest.mark.parametrize("size", [10, 40])
async def test_one_changed_field_does_not_scan_all_healthy_state_lanes(
    connected: Connected, monkeypatch: pytest.MonkeyPatch, size: int
) -> None:
    rig = connected
    assert rig.raw.peripherals is not None
    rig.raw.peripherals.update({f"extra-{i}": native_load() for i in range(size - 2)})
    await rig.bridge.poll_once()
    registry, driver = rig.bridge.mesh_registry, rig.bridge.mesh_driver
    assert registry is not None and driver is not None and len(registry.owners) == size
    lanes = VisitedLanes(driver._lanes)
    monkeypatch.setattr(driver, "_lanes", lanes)
    create_task = asyncio.create_task
    created: list[str | None] = []

    def count_task(coro: Coroutine[Any, Any, Any], *, name: str | None = None) -> asyncio.Task[Any]:
        created.append(name)
        return create_task(coro, name=name)

    monkeypatch.setattr(asyncio, "create_task", count_task)
    for value in ("0", "1", "0"):
        await rig.notify(on=value)
        await driver.flush(rig.owner())
    assert states(rig)[-1]["state"] == "OFF"
    assert not (set(lanes.visited) - {"load"}), (size, lanes.visited)
    assert created.count("mesh-publish-load") == 3
    assert created.count("mesh-topic-repair") == 0


async def test_readded_aux_does_not_issue_obsolete_unsubscribe_for_same_owner(
    connected: Connected, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = connected
    name = "enable_motion_score"
    topic = f"brilliant/mesh/load/set_{name}"
    owner = rig.owner()
    assert topic in rig.mqtt.subscriptions and rig.raw.peripherals is not None
    routing = rig.bridge._mesh_routing
    record = routing.records[topic]
    saved = rig.raw.peripherals["load"].variables.pop(name)
    captured = await rig.adapter.get_all()
    assert rig.owner() is owner and name not in owner.materialize(rig.clock()).variables
    issued: list[tuple[bool, bool]] = []
    original = rig.mqtt.unsubscribe

    async def track(target: str) -> None:
        if target == topic:
            issued.append((rig.owner() is owner, name in owner.materialize(rig.clock()).variables))
        await original(target)

    monkeypatch.setattr(rig.mqtt, "unsubscribe", track)
    operation = routing._operation
    restored = False

    async def gate(target: TopicRepair, child: Awaitable[None]) -> None:
        nonlocal restored
        if target is record and record.desired.membership is False and not restored:
            restored = True
            assert rig.raw.peripherals is not None
            rig.raw.peripherals["load"].variables[name] = saved
            await rig.adapter.get_all()
            assert rig.owner() is owner
        await operation(target, child)

    monkeypatch.setattr(routing, "_operation", gate)
    await rig.bridge.poll_once(captured)
    assert restored and topic in rig.mqtt.subscriptions
    assert issued == []


@pytest.mark.parametrize(
    "change", ["remove_config", "remove_subscription", "readd_config", "rename"]
)
async def test_topic_child_rechecks_current_descriptor_and_preserves_skipped_debt(
    connected: Connected, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    rig = connected
    assert rig.raw.peripherals is not None
    raw = rig.raw.peripherals["load"]
    owner = rig.owner()
    routing = rig.bridge._mesh_routing
    name = "enable_motion_score"
    saved = raw.variables[name]
    command = f"brilliant/mesh/load/set_{name}"
    config = "homeassistant/switch/brilliant_mesh_load_enable_motion_score/config"
    topic = command if change == "remove_subscription" else config
    if change == "remove_subscription":
        raw.variables.pop(name)
        await rig.bridge.poll_once()
        assert command not in routing.records
        raw.variables[name] = saved
    elif change == "readd_config":
        raw.variables.pop(name)
    captured = await rig.adapter.get_all()
    operation = routing._operation
    injected = False
    skipped: list[TopicRepair] = []
    issued: list[str] = []
    publish, subscribe = rig.mqtt.publish, rig.mqtt.subscribe

    async def track_publish(target: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        if target == topic:
            issued.append(payload)
        await publish(target, payload, retain, qos)

    async def track_subscribe(target: str) -> None:
        if target == topic:
            issued.append("subscribe")
        await subscribe(target)

    async def change_before_issue(record: TopicRepair, child: Awaitable[None]) -> None:
        nonlocal injected
        if routing.records.get(topic) is record and not injected:
            injected = True
            selected, membership, applied = record.desired, record.membership, record.applied
            if change == "readd_config":
                raw.variables[name] = saved
            elif change == "rename":
                raw.name = "Renamed mesh load"
            else:
                raw.variables.pop(name)
            await rig.adapter.get_all()
            assert rig.owner() is owner
            await operation(record, child)
            assert issued == []
            assert record.desired is selected and record.applied is applied
            assert record.membership is membership
            assert record.applied is not selected
            skipped.append(record)
            return
        await operation(record, child)

    monkeypatch.setattr(rig.mqtt, "publish", track_publish)
    monkeypatch.setattr(rig.mqtt, "subscribe", track_subscribe)
    monkeypatch.setattr(routing, "_operation", change_before_issue)
    if change in ("rename", "remove_config"):
        await rig.bridge.reconcile()
    else:
        await rig.bridge.poll_once(captured)
    assert injected and len(skipped) == 1
    if change.startswith("remove"):
        assert command not in rig.mqtt.subscriptions
        assert topic not in routing.records
        assert issued == ([""] if change == "remove_config" else [])
    else:
        assert issued and all(issued)
        if change == "rename":
            assert all("Renamed mesh load" in payload for payload in issued)
        assert routing.records[topic].applied is routing.records[topic].desired


async def test_revoked_child_index_blocks_successor_until_actual_completion_and_then_clears(
    connected: Connected, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = connected
    driver = rig.bridge.mesh_driver
    registry = rig.bridge.mesh_registry
    assert driver is not None and registry is not None
    entered, canceled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    publish = rig.mqtt.publish
    calls: list[str] = []

    async def delayed(topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        if topic == "brilliant/mesh/load/state":
            calls.append(payload)
            if len(calls) == 1:
                entered.set()
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    canceled.set()
                    await release.wait()
        await publish(topic, payload, retain, qos)

    monkeypatch.setattr(rig.mqtt, "publish", delayed)
    try:
        assert not driver._revoked_children
        old = rig.owner()
        await rig.notify(on="0")
        await entered.wait()
        assert not driver._revoked_children
        rig.adapter._on_proc_reconnect()
        await rig.adapter.get_all()
        successor = rig.owner()
        assert successor is not old
        await canceled.wait()
        await turns()
        assert list(driver._revoked_children.values()) == [old]
        assert len(calls) == 1
        release.set()
        await driver.flush(successor)
        await turns()
        assert [json.loads(payload)["mesh_write_status"] for payload in calls] == [
            "idle",
            "unconfirmed",
            "idle",
        ]
        assert not driver._revoked_children
        assert not rig.adapter.consume_mesh_fatal()
    finally:
        release.set()


@pytest.mark.parametrize("newer_field", ["on", "enable_motion_score"])
@pytest.mark.parametrize("scope", ["unknown", "exception"])
async def test_negative_inventory_keeps_newer_independent_field_and_merges_other_absence(
    connected: Connected,
    monkeypatch: pytest.MonkeyPatch,
    newer_field: str,
    scope: str,
) -> None:
    rig = connected
    snapshot = copy.deepcopy(rig.raw)
    assert snapshot.peripherals is not None
    del snapshot.peripherals["load"]
    owner = rig.owner()
    observer = rig.harness.observers[0]
    native = observer.get_device
    async with delayed_poll(rig, monkeypatch, snapshot):
        await rig.notify(**{newer_field: "1"})

        async def unknown(device_id: str) -> _RawDevice | None:
            if device_id == "ble_mesh":
                if scope == "exception":
                    raise RuntimeError("parent read failed")
                return None
            result = await native(device_id)
            assert isinstance(result, _RawDevice)
            return result

        with monkeypatch.context() as patch:
            patch.setattr(observer, "get_device", unknown)
            if scope == "exception":
                with pytest.raises(RuntimeError, match="parent read failed"):
                    await rig.adapter.get_all()
            else:
                await rig.adapter.get_all()
    assert rig.owner() is owner
    variables = owner.materialize(rig.clock()).variables
    if newer_field == "enable_motion_score":
        assert variables["enable_motion_score"].value == "1"
        assert "on" not in variables
    else:
        assert "enable_motion_score" not in variables
        if scope == "exception":
            assert variables["on"].value == "1"
    assert states(rig)[-1]["state"] == (
        "ON" if scope == "exception" and newer_field == "on" else None
    )


@pytest.mark.parametrize("operation", ["membership", "config"])
async def test_same_owner_already_issued_topic_operation_settles_before_current_repair(
    connected: Connected, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    rig = connected
    assert rig.raw.peripherals is not None
    raw = rig.raw.peripherals["load"]
    owner = rig.owner()
    name = "enable_motion_score"
    command = f"brilliant/mesh/load/set_{name}"
    config = "homeassistant/switch/brilliant_mesh_load_enable_motion_score/config"
    entered, release = asyncio.Event(), asyncio.Event()
    order: list[str] = []
    publish, unsubscribe, subscribe = rig.mqtt.publish, rig.mqtt.unsubscribe, rig.mqtt.subscribe
    saved = raw.variables[name]

    async def block_publish(target: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        if operation == "config" and target == config:
            if not entered.is_set():
                order.append("old_started")
                entered.set()
                await release.wait()
                order.append("old_accepted")
            else:
                assert release.is_set()
                assert "Renamed mesh load" in payload
                order.append("current")
        await publish(target, payload, retain, qos)

    async def block_unsubscribe(target: str) -> None:
        if operation == "membership" and target == command:
            order.append("old_started")
            entered.set()
            await release.wait()
            order.append("old_accepted")
        await unsubscribe(target)

    async def track_subscribe(target: str) -> None:
        if operation == "membership" and target == command:
            assert release.is_set()
            order.append("current")
        await subscribe(target)

    monkeypatch.setattr(rig.mqtt, "publish", block_publish)
    monkeypatch.setattr(rig.mqtt, "unsubscribe", block_unsubscribe)
    monkeypatch.setattr(rig.mqtt, "subscribe", track_subscribe)
    if operation == "membership":
        raw.variables.pop(name)
    first = asyncio.create_task(rig.bridge.reconcile())
    current: asyncio.Task[None] | None = None
    try:
        await asyncio.wait_for(entered.wait(), 2)
        if operation == "membership":
            raw.variables[name] = saved
        else:
            raw.name = "Renamed mesh load"
        current = asyncio.create_task(rig.bridge.poll_once())
        await turns()
        assert rig.owner() is owner
        assert order == ["old_started"]
        release.set()
        await asyncio.gather(first, current)
        assert order[:3] == ["old_started", "old_accepted", "current"]
        assert command in rig.mqtt.subscriptions
        record = rig.bridge._mesh_routing.records[config]
        assert record.applied is record.desired
        assert not rig.adapter.consume_mesh_fatal()
    finally:
        release.set()
        await asyncio.gather(first, *([current] if current is not None else []))
