"""Round-one review schedules at the production adapter and MQTT boundaries."""

from __future__ import annotations

import asyncio
import copy
import json
from collections.abc import AsyncIterator, Coroutine, Iterator
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from _collections_abc import dict_items, dict_values

import pytest

from brilliant_mqtt.bridge import Bridge
from brilliant_mqtt.bus import RpcBusAdapter
from brilliant_mqtt.mesh_routing import TopicRepair
from tests.fakes import (
    FakeClock,
    FakeMqtt,
    FakeSleeper,
    _RawDevice,
    _RawPeripheral,
    _RawVariable,
    _StartHarness,
)
from tests.mesh_regression_support import Rig, turns
from tests.test_bridge import MESH_STATE_TOPIC, _mesh_states
from tests.test_mesh_bridge_integration import Connected, connected
from tests.test_mesh_bus_ingress import delta
from tests.test_mesh_capacity_routing import CountOnlyInventory
from tests.test_mesh_settlement_regressions import (
    rig,
)
from tests.test_mesh_settlement_regressions import (
    test_late_acceptance_then_matching_value_confirms as historical_port,
)

__all__ = ["connected", "rig"]


@pytest.fixture
async def cold_connected(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[Connected]:
    clock, sleeper = FakeClock(), FakeSleeper()
    harness = _StartHarness(monkeypatch, gated_writes=True)
    adapter = RpcBusAdapter(extra_device_ids=("ble_mesh",), clock=clock)
    await adapter.start()
    raw = _RawDevice("ble_mesh", {})

    async def get_device(device_id: str) -> _RawDevice:
        return raw if device_id == "ble_mesh" else _RawDevice(device_id, {})

    monkeypatch.setattr(harness.observers[0], "get_device", get_device, raising=False)
    mqtt = FakeMqtt()
    bridge = Bridge(
        adapter,
        mqtt,
        "mesh",
        include=lambda device_id: device_id == "ble_mesh",
        clock=clock,
        wall_clock=lambda: 1000 + clock(),
        sleep=sleeper,
    )
    try:
        yield Connected(bridge, adapter, mqtt, harness, clock, raw, sleeper)
    finally:
        harness.write_release.set()
        await bridge.withdraw()
        await adapter.shutdown()


def states(rig: Connected, pid: str = "load") -> list[dict[str, object]]:
    return [
        json.loads(payload)
        for topic, payload, _ in rig.mqtt.published
        if topic == f"brilliant/mesh/{pid}/state"
    ]


def assert_routing_indexes(rig: Connected) -> None:
    routing = rig.bridge._mesh_routing
    expected: dict[str, set[str]] = {}
    for topic, record in routing.records.items():
        if record.peripheral_id is not None:
            expected.setdefault(record.peripheral_id, set()).add(topic)
    assert routing._topics_by_peripheral == expected
    assert routing._withdrawn == {
        topic
        for topic, record in routing.records.items()
        if record.desired.owner is routing and record.desired.forget
    }
    registry = rig.bridge.mesh_registry
    driver = rig.bridge.mesh_driver
    assert registry is not None and driver is not None
    assert (
        len(
            set(expected)
            | {owner.metadata.peripheral_id for owner in registry.owners}
            | set(registry.retired)
            | driver.peripheral_ids
        )
        <= 256
    )


@asynccontextmanager
async def delayed_poll(
    rig: Connected, monkeypatch: pytest.MonkeyPatch, snapshot: _RawDevice, *, cold: bool = False
) -> AsyncIterator[asyncio.Task[None]]:
    entered, release = asyncio.Event(), asyncio.Event()
    observer = rig.harness.observers[0]
    native = observer.get_device

    async def delayed(device_id: str) -> _RawDevice:
        if device_id == "ble_mesh" and not entered.is_set():
            entered.set()
            await release.wait()
            return snapshot
        result = await native(device_id)
        assert isinstance(result, _RawDevice)
        return result

    monkeypatch.setattr(observer, "get_device", delayed)
    poll = asyncio.create_task(rig.bridge.reconcile() if cold else rig.bridge.poll_once())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        yield poll
    finally:
        release.set()
        await asyncio.wait_for(poll, 2)


@pytest.mark.parametrize("boundary", ["push_deletion", "scoped_absence", "inventory"])
async def test_authoritative_unknown_id_absence_fences_delayed_first_sighting(
    connected: Connected, monkeypatch: pytest.MonkeyPatch, boundary: str
) -> None:
    rig = connected
    assert rig.raw.peripherals is not None
    before = _RawPeripheral("1")
    before.peripheral_type = 27
    snapshot = copy.deepcopy(rig.raw)
    assert snapshot.peripherals is not None
    snapshot.peripherals["new"] = before
    registry = rig.bridge.mesh_registry
    assert registry is not None
    async with delayed_poll(rig, monkeypatch, snapshot):
        if boundary == "push_deletion":
            await rig.harness.observers[0].handle_notification(
                SimpleNamespace(
                    updated_device=rig.raw,
                    deleted=False,
                    modified_peripherals=[SimpleNamespace(peripheral_id="new", deleted=True)],
                )
            )
        elif boundary == "scoped_absence":

            async def absent(device_id: str, peripheral_id: str) -> None:
                return None

            monkeypatch.setattr(rig.harness.observers[0], "get_peripheral", absent, raising=False)
            assert await rig.adapter.get_peripheral("ble_mesh", "new") is None
        else:
            await rig.adapter.get_all()
        # The topology floor must not reject a known owner's independent fields.
        await rig.notify(enable_motion_score="1")
    assert registry.current("new") is None
    assert "brilliant/mesh/new/set" not in rig.bridge._subscribed
    assert not states(rig, "new")
    assert not any(
        "new" in topic and topic.endswith("/config") for topic, _, _ in rig.mqtt.published
    )
    assert rig.owner().materialize(rig.clock()).variables["enable_motion_score"].value == "1"
    rig.raw.peripherals["new"] = before
    await rig.bridge.poll_once()
    assert registry.current("new") is not None
    assert "brilliant/mesh/new/set" in rig.bridge._subscribed
    assert states(rig, "new")[-1]["state"] == "ON"


@pytest.mark.parametrize("boundary", ["push_deletion", "scoped_absence", "empty_inventory"])
@pytest.mark.parametrize("recovery", ["inventory", "explicit_creation"])
async def test_cold_unbound_absence_blocks_mqtt_native_admission_and_recovers_after_cleanup(
    cold_connected: Connected, monkeypatch: pytest.MonkeyPatch, boundary: str, recovery: str
) -> None:
    rig = cold_connected
    registry = rig.bridge.mesh_registry
    assert registry is not None and not registry.owners and not registry.inventory_ready
    assert rig.raw.peripherals == {}
    pid = "deleted-load"
    topic = f"brilliant/mesh/{pid}/set"
    peripheral = _RawPeripheral("1")
    peripheral.peripheral_type = 27
    snapshot = _RawDevice("ble_mesh", {pid: peripheral})
    observer = rig.harness.observers[0]
    native = observer.request_set_variables_in_peripheral
    issued: list[tuple[str, str, dict[str, str]]] = []
    read_device = observer.get_device
    device_reads: list[str] = []

    async def record_read(device_id: str) -> _RawDevice:
        device_reads.append(device_id)
        result = await read_device(device_id)
        assert isinstance(result, _RawDevice)
        return result

    async def record_native(peripheral_id: str, values: dict[str, str], *, device_id: str) -> str:
        issued.append((device_id, peripheral_id, dict(values)))
        result = await native(peripheral_id, values, device_id=device_id)
        assert isinstance(result, str)
        return result

    monkeypatch.setattr(observer, "request_set_variables_in_peripheral", record_native)
    monkeypatch.setattr(observer, "get_device", record_read)
    async with delayed_poll(rig, monkeypatch, snapshot, cold=True):
        assert registry.enabled and not registry.owners and not registry.inventory_ready
        if boundary == "push_deletion":
            await observer.handle_notification(
                SimpleNamespace(
                    updated_device=rig.raw,
                    deleted=False,
                    modified_peripherals=[delta(pid, deleted=True)],
                )
            )
        elif boundary == "scoped_absence":

            async def absent(device_id: str, peripheral_id: str) -> None:
                return None

            monkeypatch.setattr(observer, "get_peripheral", absent, raising=False)
            assert await rig.adapter.get_peripheral("ble_mesh", pid) is None
        else:
            assert await rig.adapter.get_all() == []
        assert not registry.owners
    rig.harness.write_release.set()
    await rig.mqtt.inject(topic, '{"state":"OFF"}')
    print(
        {
            "boundary": boundary,
            "recovery": recovery,
            "owners": len(registry.owners),
            "command_route": topic in rig.bridge._by_cmd_topic,
            "native": issued,
        }
    )
    assert (len(registry.owners), topic in rig.bridge._by_cmd_topic, issued) == (0, False, [])
    assert topic not in rig.bridge._subscribed
    assert rig.bridge._resolve_mesh_owner(topic) is None
    assert rig.harness.write_starts == []
    assert not states(rig, pid)

    async def create() -> None:
        assert rig.raw.peripherals is not None
        rig.raw.peripherals[pid] = peripheral
        offset = len(device_reads)
        if recovery == "inventory":
            await rig.bridge.poll_once()
        else:
            await observer.handle_notification(
                SimpleNamespace(
                    updated_device=rig.raw,
                    deleted=False,
                    modified_peripherals=[delta(pid, "on")],
                )
            )
            await turns()
        assert device_reads[offset:] == (
            ["own-device", "ble_mesh"] if recovery == "inventory" else []
        )

    await create()
    owner = registry.current(pid)
    assert owner is not None
    assert topic in rig.bridge._subscribed and rig.bridge._resolve_mesh_owner(topic) is owner
    assert states(rig, pid)[-1]["state"] == "ON"
    await rig.mqtt.inject(topic, '{"state":"OFF"}')
    assert issued == [("ble_mesh", pid, {"on": "0"})]
    assert rig.harness.write_starts == ["ble_mesh"]

    # Completed removal must also permit a genuinely new owner and native write.
    assert rig.raw.peripherals is not None
    del rig.raw.peripherals[pid]
    await rig.bridge.poll_once()
    assert registry.current(pid) is None and not registry.retired
    assert topic not in rig.bridge._subscribed and topic not in rig.bridge._by_cmd_topic
    await rig.mqtt.inject(topic, '{"state":"OFF"}')
    assert len(issued) == 1
    await create()
    successor = registry.current(pid)
    assert successor is not None and successor is not owner
    assert rig.bridge._resolve_mesh_owner(topic) is successor
    await rig.mqtt.inject(topic, '{"state":"OFF"}')
    assert issued == [("ble_mesh", pid, {"on": "0"})] * 2
    assert rig.harness.write_starts == ["ble_mesh"] * 2


@pytest.mark.parametrize("cold", [False, True], ids=["known_topology", "cold_acquisition"])
@pytest.mark.parametrize("shape", ["none", "incomplete", "exception"])
async def test_unknown_parent_fences_first_sighting_and_inventory_without_erasing_known_fields(
    connected: Connected, monkeypatch: pytest.MonkeyPatch, cold: bool, shape: str
) -> None:
    rig = connected
    registry = rig.bridge.mesh_registry
    assert registry is not None and rig.raw.peripherals is not None
    original = rig.owner()
    snapshot = copy.deepcopy(rig.raw)
    assert snapshot.peripherals is not None
    newcomer = _RawPeripheral("1")
    newcomer.peripheral_type = 27
    snapshot.peripherals["new"] = newcomer
    snapshot.peripherals["load"].variables["enable_motion_score"] = _RawVariable("1")
    if cold:
        await rig.bridge.withdraw()
    native = rig.harness.observers[0].get_device

    async def unknown(device_id: str) -> _RawDevice | None:
        if device_id == "ble_mesh":
            if shape == "exception":
                raise RuntimeError("native read failed")
            return None if shape == "none" else _RawDevice("ble_mesh", None)
        result = await native(device_id)
        assert isinstance(result, _RawDevice)
        return result

    async with delayed_poll(rig, monkeypatch, snapshot, cold=cold):
        with monkeypatch.context() as scope:
            scope.setattr(rig.harness.observers[0], "get_device", unknown)
            if shape == "exception":
                with pytest.raises(RuntimeError, match="native read failed"):
                    await rig.adapter.get_all()
            else:
                assert await rig.adapter.get_all() == []
        if not cold:
            assert registry.current("load") is original
            assert "brilliant/mesh/load/set" in rig.bridge._subscribed
    if shape == "exception":
        assert registry.current("new") is not None
        assert states(rig, "new")[-1]["state"] == "ON"
        assert registry.inventory_ready
    else:
        assert registry.current("new") is None
        assert not states(rig, "new")
        assert not registry.inventory_ready
        assert [p for t, p, _ in rig.mqtt.published if t == "brilliant/mesh/availability"][
            -1
        ] == "offline"
        if not cold:
            assert rig.owner() is original
            assert states(rig)[-1]["state"] is None
            assert original.materialize(rig.clock()).variables["enable_motion_score"].value == "1"
    rig.raw.peripherals["new"] = newcomer
    await rig.bridge.poll_once()
    assert registry.inventory_ready
    assert states(rig, "new")[-1]["state"] == "ON"
    assert [p for t, p, _ in rig.mqtt.published if t == "brilliant/mesh/availability"][
        -1
    ] == "online"


@pytest.mark.parametrize("capacity", [False, True], ids=["disappearance", "capacity"])
async def test_already_unknown_retirement_publishes_exact_debt_then_cleans_up(
    connected: Connected, capacity: bool
) -> None:
    rig = connected
    registry, driver = rig.bridge.mesh_registry, rig.bridge.mesh_driver
    assert registry is not None and driver is not None and rig.raw.peripherals is not None
    rig.raw.peripherals["blocker"].variables.clear()
    await rig.bridge.poll_once()
    owner = rig.owner("blocker")
    before = len(states(rig, "blocker"))
    assert states(rig, "blocker")[-1]["state"] is None
    if capacity:
        rig.raw.peripherals = CountOnlyInventory()
        await rig.adapter.get_all()
        await turns(100)
    else:
        del rig.raw.peripherals["blocker"]
        await rig.bridge.poll_once()
    for _ in range(3):
        await rig.bridge._flush_retired_mesh()
    assert "blocker" not in registry.retired
    assert owner.view.ordinary_debt is None
    assert len(states(rig, "blocker")) == before + 1
    assert "brilliant/mesh/blocker/set" not in rig.bridge._subscribed
    assert any("blocker" in topic and payload == "" for topic, payload, _ in rig.mqtt.published)
    if not capacity:
        await rig.bridge.poll_once()
        assert [p for t, p, _ in rig.mqtt.published if t == "brilliant/mesh/availability"][
            -1
        ] == "online"
    assert_routing_indexes(rig)


@pytest.mark.parametrize("older,newer", [("1", "0"), ("0", "1")])
async def test_delayed_scale_replacement_cannot_seed_shadowed_primary_or_auxiliary(
    connected: Connected, monkeypatch: pytest.MonkeyPatch, older: str, newer: str
) -> None:
    rig = connected
    assert rig.raw.peripherals is not None
    await rig.notify(on=older)
    old = rig.owner()
    snapshot = copy.deepcopy(rig.raw)
    assert snapshot.peripherals is not None
    snapshot.peripherals["load"].variables["max_intensity_value"].value = "1000"
    rig.raw.peripherals["load"].variables["max_intensity_value"].value = "1000"
    async with delayed_poll(rig, monkeypatch, snapshot):
        await rig.notify(on=newer, enable_motion_score="1")
        assert states(rig)[-1]["state"] == ("OFF" if newer == "0" else "ON")
        offset = len(states(rig))
    successor = rig.owner()
    assert successor is not old
    assert successor.metadata.binding_max_intensity == 1000
    assert states(rig)[offset:]
    assert all(
        document["state"] != ("ON" if older == "1" else "OFF") for document in states(rig)[offset:]
    )
    assert all(
        document.get("enable_motion_score") is not False for document in states(rig)[offset:]
    )
    assert successor.view.command_id == 0 and successor.view.window is None
    await rig.bridge.poll_once()
    assert states(rig)[-1]["state"] == ("OFF" if newer == "0" else "ON")
    assert successor.materialize(rig.clock()).variables["enable_motion_score"].value == "1"


@pytest.mark.parametrize("removed", [False, True], ids=["newer_values", "newer_absence"])
async def test_shadowed_replacement_fences_other_already_started_reads(
    connected: Connected, monkeypatch: pytest.MonkeyPatch, removed: bool
) -> None:
    rig = connected
    assert rig.raw.peripherals is not None
    snapshots = [copy.deepcopy(rig.raw), copy.deepcopy(rig.raw)]
    assert snapshots[0].peripherals is not None
    snapshots[0].peripherals["load"].variables["max_intensity_value"].value = "1000"
    entered = [asyncio.Event(), asyncio.Event()]
    release = [asyncio.Event(), asyncio.Event()]
    native = rig.harness.observers[0].get_device
    reads = 0

    async def delayed(device_id: str) -> _RawDevice:
        nonlocal reads
        if device_id == "ble_mesh" and reads < 2:
            index = reads
            reads += 1
            entered[index].set()
            await release[index].wait()
            return snapshots[index]
        result = await native(device_id)
        assert isinstance(result, _RawDevice)
        return result

    monkeypatch.setattr(rig.harness.observers[0], "get_device", delayed)
    first = asyncio.create_task(rig.bridge.poll_once())
    second: asyncio.Task[None] | None = None
    try:
        await asyncio.wait_for(entered[0].wait(), 2)
        second = asyncio.create_task(rig.bridge.poll_once())
        await asyncio.wait_for(entered[1].wait(), 2)
        if removed:
            for name in ("on", "enable_motion_score"):
                rig.raw.peripherals["load"].variables.pop(name)
            await rig.harness.observers[0].handle_notification(
                SimpleNamespace(
                    updated_device=rig.raw,
                    deleted=False,
                    modified_peripherals=[
                        SimpleNamespace(
                            peripheral_id="load",
                            deleted=False,
                            modified_variables=[
                                SimpleNamespace(variable_name=name, deletion=True)
                                for name in ("on", "enable_motion_score")
                            ],
                            deleted_variables=[],
                        )
                    ],
                )
            )
            await turns()
        else:
            await rig.notify(on="0", enable_motion_score="1")
        offset = len(states(rig))
        release[0].set()
        await asyncio.wait_for(first, 2)
        successor = rig.owner()
        assert successor.metadata.binding_max_intensity == 1000
        release[1].set()
        await asyncio.wait_for(second, 2)
        assert rig.owner() is successor
        assert all(document["state"] != "ON" for document in states(rig)[offset:])
        assert all(
            document.get("enable_motion_score") is not False for document in states(rig)[offset:]
        )
        assert successor.metadata.binding_max_intensity == 1000
        rig.raw.peripherals["load"].variables["max_intensity_value"].value = "1000"
        await rig.notify(on="0", enable_motion_score="1")
        assert states(rig)[-1]["state"] == "OFF"
    finally:
        for gate in release:
            gate.set()
        await asyncio.wait_for(asyncio.gather(first, *([second] if second is not None else [])), 2)


@pytest.mark.parametrize("failures,reappear", [(1, False), (3, False), (1, True)])
async def test_existing_repairs_retry_ownerless_withdrawal_without_harming_reappearance(
    connected: Connected, monkeypatch: pytest.MonkeyPatch, failures: int, reappear: bool
) -> None:
    rig = connected
    topic = "brilliant/mesh/blocker/set"
    assert rig.raw.peripherals is not None
    registry = rig.bridge.mesh_registry
    assert registry is not None
    unsubscribe = rig.mqtt.unsubscribe
    attempts = 0

    async def failing(target: str) -> None:
        nonlocal attempts
        if target == topic:
            attempts += 1
            if attempts <= failures:
                raise RuntimeError("temporary unsubscribe failure")
        await unsubscribe(target)

    monkeypatch.setattr(rig.mqtt, "unsubscribe", failing)
    await rig.bridge.withdraw()
    assert attempts == 1
    if not reappear:
        del rig.raw.peripherals["blocker"]
    # Repair failure is ordinary debt, so existing triggers contain/retry it.
    await rig.bridge.reconcile()
    for _ in range(failures + 1):
        await rig.bridge.poll_once()
    assert len(registry.owners) + len(registry.retired) <= 256
    assert len(rig.bridge._mesh_routing.records) < 20
    assert not rig.adapter.consume_mesh_fatal()
    if reappear:
        assert attempts == 1
        assert topic in rig.bridge._subscribed
        assert rig.bridge._resolve_mesh_owner(topic) is rig.owner("blocker")
    else:
        assert attempts == failures + 1
        assert topic not in rig.bridge._subscribed
        assert registry.current("blocker") is None and not registry.retired
    assert [p for t, p, _ in rig.mqtt.published if t == "brilliant/mesh/availability"][
        -1
    ] == "online"
    assert_routing_indexes(rig)


async def test_routing_work_indexes_follow_distinct_id_churn_and_withdrawal(
    connected: Connected,
) -> None:
    rig = connected
    assert rig.raw.peripherals is not None
    peripheral = copy.deepcopy(rig.raw.peripherals["blocker"])
    for index in range(12):
        pid = f"churn-{index}"
        rig.raw.peripherals[pid] = copy.deepcopy(peripheral)
        await rig.bridge.poll_once()
        assert_routing_indexes(rig)
        del rig.raw.peripherals[pid]
        await rig.bridge.poll_once()
        assert_routing_indexes(rig)
        assert pid not in rig.bridge._mesh_routing.peripheral_ids
    await rig.bridge.withdraw()
    assert_routing_indexes(rig)
    assert not rig.bridge._mesh_routing.records
    assert not rig.bridge._mesh_routing._topics_by_peripheral
    assert not rig.bridge._mesh_routing._withdrawn
    await rig.bridge.reconcile()
    assert_routing_indexes(rig)


async def test_withdrawn_cleanup_rechecks_sync_reappearance_before_retry(
    connected: Connected, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = connected
    assert rig.raw.peripherals is not None
    topic = "brilliant/mesh/blocker/set"
    unsubscribe = rig.mqtt.unsubscribe
    attempts = 0

    async def fail_once(target: str) -> None:
        nonlocal attempts
        if target == topic:
            attempts += 1
            if attempts == 1:
                raise RuntimeError("temporary unsubscribe failure")
        await unsubscribe(target)

    monkeypatch.setattr(rig.mqtt, "unsubscribe", fail_once)
    await rig.bridge.withdraw()
    peripheral = rig.raw.peripherals.pop("blocker")
    flush = rig.bridge._flush_retired_mesh

    async def reappear() -> None:
        await flush()
        assert rig.raw.peripherals is not None
        rig.raw.peripherals["blocker"] = peripheral
        await rig.adapter.get_all()

    monkeypatch.setattr(rig.bridge, "_flush_retired_mesh", reappear)
    await rig.bridge.reconcile()
    assert attempts == 1
    assert topic in rig.bridge._subscribed
    assert rig.bridge._resolve_mesh_owner(topic) is rig.owner("blocker")


class VisitedRecords(dict[str, TopicRepair]):
    """Count actual unrelated entries, including changes of iteration spelling."""

    def __init__(self, records: dict[str, TopicRepair]) -> None:
        super().__init__(records)
        self.visited: list[str] = []

    def __iter__(self) -> Iterator[str]:
        for key in super().__iter__():
            self.visited.append(key)
            yield key

    def items(self) -> dict_items[str, TopicRepair]:
        self.visited.extend(super().keys())
        return super().items()

    def values(self) -> dict_values[str, TopicRepair]:
        self.visited.extend(super().keys())
        return super().values()


@pytest.mark.parametrize("size", [10, 40])
async def test_healthy_push_and_unchanged_poll_bound_cumulative_routing_work(
    connected: Connected, monkeypatch: pytest.MonkeyPatch, size: int
) -> None:
    rig = connected
    assert rig.raw.peripherals is not None
    for index in range(size - 2):
        rig.raw.peripherals[f"stable-{index}"] = copy.deepcopy(rig.raw.peripherals["blocker"])
    await rig.bridge.poll_once()
    await turns(100)
    records = VisitedRecords(rig.bridge._mesh_routing.records)
    monkeypatch.setattr(rig.bridge._mesh_routing, "records", records)
    native = asyncio.create_task
    created: list[str | None] = []

    def create_task(
        coro: Coroutine[Any, Any, Any], *, name: str | None = None
    ) -> asyncio.Task[Any]:
        created.append(name)
        return native(coro, name=name)

    monkeypatch.setattr(asyncio, "create_task", create_task)
    for _ in range(3):
        await rig.notify(on="1")
    assert records.visited == []
    assert created.count("mesh-topic-repair") == 0
    created.clear()
    for _ in range(3):
        await rig.bridge.poll_once()
    # Shared readiness can visit each record once per poll, never once per load.
    assert len(records.visited) <= 3 * len(records)
    assert created.count("mesh-topic-repair") == 0
    print(
        f"healthy routing size={size}: push visits=0 tasks=0; "
        f"polls visits={len(records.visited)} tasks=0"
    )


@pytest.mark.parametrize("injection", ["duplicate_terminal", "early_confirmation"])
async def test_historical_matching_port_rejects_invalid_wire_sequences(
    rig: Rig, monkeypatch: pytest.MonkeyPatch, injection: str
) -> None:
    if injection == "duplicate_terminal":
        published = rig.mqtt.publish

        async def duplicate(topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
            await published(topic, payload, retain, qos)
            if topic == MESH_STATE_TOPIC and json.loads(payload)["state"] == "OFF":
                await published(topic, payload, retain, qos)

        monkeypatch.setattr(rig.mqtt, "publish", duplicate)
    else:
        report = rig.report

        async def flash(value: str) -> None:
            await report(value)
            if rig.clock() == 77:
                pending = json.loads(_mesh_states(rig.mqtt)[-1][1])
                early: dict[str, object] = dict(
                    pending,
                    state="OFF",
                    mesh_write_status="idle",
                    mesh_requested={},
                    mesh_write_deadline=None,
                )
                for document in (early, pending):
                    await rig.mqtt.publish(MESH_STATE_TOPIC, json.dumps(document), retain=True)

        monkeypatch.setattr(rig, "report", flash)
    with pytest.raises(AssertionError):
        await historical_port(rig)
