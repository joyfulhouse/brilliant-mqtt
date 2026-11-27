"""Round-four capacity, routing isolation and authoritative metadata schedules."""

from __future__ import annotations

import asyncio
import copy
import json
from dataclasses import replace
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from _collections_abc import dict_values

import pytest

from brilliant_mqtt.mesh_routing import TopicRepair
from tests.fakes import _RawDevice, _RawPeripheral
from tests.mesh_regression_support import turns
from tests.test_mesh_bridge_integration import Connected, connected
from tests.test_mesh_discovery_metadata import Firmware, firmware
from tests.test_mesh_review_corrections import delayed_poll, states

__all__ = ["connected", "firmware"]


async def test_delayed_overcapacity_retires_owner_born_during_read(
    connected: Connected, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = connected
    registry = rig.bridge.mesh_registry
    assert registry is not None
    old = rig.owner()
    snapshot = copy.deepcopy(rig.raw)
    assert snapshot.peripherals is not None and rig.raw.peripherals is not None
    snapshot.peripherals.update(
        {f"overflow-{i}": copy.deepcopy(snapshot.peripherals["blocker"]) for i in range(255)}
    )
    async with delayed_poll(rig, monkeypatch, snapshot):
        await rig.notify(max_intensity_value="1000", on="1")
        newer = rig.owner()
        assert newer is not old
        assert states(rig)[-1]["state"] == "ON"
    assert registry.disabled and registry.inventory_count == 257
    rig.raw.peripherals.pop("load")
    for _ in range(2):
        await rig.bridge.poll_once()
    alive = [p for t, p, _ in rig.mqtt.published if t == "brilliant/mesh/availability"][-1]
    actual = (
        list(registry.retired),
        "brilliant/mesh/load/set" in rig.mqtt.subscriptions,
        states(rig)[-1]["state"],
        alive,
    )
    assert actual == ([], False, None, "online"), actual


async def test_omission_shape_change_does_not_wait_for_unrelated_running_topic(
    firmware: Firmware, monkeypatch: pytest.MonkeyPatch
) -> None:
    await omission_shape_change(firmware, monkeypatch, "poll", False)


@pytest.mark.parametrize("trigger", ["poll", "reconcile"])
@pytest.mark.parametrize("same_topic", [False, True], ids=["unrelated", "same_topic"])
async def test_omission_repairs_only_needed_topics_and_preserves_serialization(
    firmware: Firmware, monkeypatch: pytest.MonkeyPatch, trigger: str, same_topic: bool
) -> None:
    await omission_shape_change(firmware, monkeypatch, trigger, same_topic)


async def omission_shape_change(
    fw: Firmware, monkeypatch: pytest.MonkeyPatch, trigger: str, same_topic: bool
) -> None:
    rig = replace(fw.rig, bridge=fw.bridge)
    await rig.bridge.reconcile()
    snapshot = copy.deepcopy(rig.raw)
    assert snapshot.peripherals is not None
    snapshot.peripherals.pop("load")
    original_read = rig.harness.observers[0].get_device
    reading, finish_read, entered, release = (asyncio.Event() for _ in range(4))

    async def delayed(device: str) -> _RawDevice | None:
        if device == "ble_mesh":
            reading.set()
            await finish_read.wait()
            return snapshot
        result = await original_read(device)
        assert result is None or isinstance(result, _RawDevice)
        return result

    monkeypatch.setattr(rig.harness.observers[0], "get_device", delayed)
    pub = rig.mqtt.publish
    blocked = (
        f"homeassistant/switch/brilliant_{fw.panel}_load_enable_motion_score/config"
        if same_topic
        else fw.topic
    )

    async def block(topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        if topic == blocked:
            entered.set()
            await release.wait()
        await pub(topic, payload, retain, qos)

    monkeypatch.setattr(rig.mqtt, "publish", block)
    reads = len(fw.reads)
    task = asyncio.create_task(
        rig.bridge.poll_once() if trigger == "poll" else rig.bridge.reconcile()
    )
    try:
        await asyncio.wait_for(reading.wait(), 2)
        await rig.notify(display_name="Updated desk", on="0")
        await asyncio.wait_for(entered.wait(), 2)
        record = rig.bridge._mesh_routing.records[blocked]
        worker, child = record.worker, record.child
        assert child is not None
        finish_read.set()
        await turns(250)
        assert "enable_motion_score" not in rig.owner().materialize(rig.clock()).variables
        assert f"brilliant/{fw.panel}/load/set_enable_motion_score" not in rig.mqtt.subscriptions
        assert record.worker is worker and record.child is child and not child.done()
        assert task.done() is not same_topic, (
            "cleanup must join changed work on the same topic, and exclude unrelated work"
        )
        assert len(fw.reads) == reads + 1  # The mesh gate replaces that existing read.
    finally:
        finish_read.set()
        release.set()
        await asyncio.wait_for(task, 2)
    if same_topic:
        assert [p for t, p, _ in rig.mqtt.published if t == blocked][-1] == ""
    assert not rig.harness.write_starts


async def test_fenced_fitting_read_cannot_reuse_warm_inventory_readiness(
    connected: Connected, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = connected
    registry = rig.bridge.mesh_registry
    assert registry is not None
    snapshot = copy.deepcopy(rig.raw)
    assert snapshot.peripherals is not None
    snapshot.peripherals = {"replacement": copy.deepcopy(snapshot.peripherals["blocker"])}
    rig.raw.peripherals = snapshot.peripherals
    rig.harness.write_release.set()
    assert registry.inventory_ready
    async with delayed_poll(rig, monkeypatch, snapshot):
        await rig.mqtt.inject("brilliant/mesh/load/set", '{"state":"OFF"}')
        assert len(rig.harness.write_starts) == 1
    await turns(100)
    last = [p for t, p, _ in rig.mqtt.published if t == "brilliant/mesh/availability"][-1]
    actual = (registry.inventory_count, registry.inventory_ready, len(registry.owners), last)
    assert actual == (1, False, 0, "offline"), actual
    await rig.bridge.poll_once()
    assert rig.bridge._resolve_mesh_owner("brilliant/mesh/replacement/set") is rig.owner(
        "replacement"
    )


async def test_overlapping_reconciles_do_not_rollback_current_firmware_debt(
    firmware: Firmware, monkeypatch: pytest.MonkeyPatch
) -> None:
    await overlapping_firmware(firmware, monkeypatch, "F3", False)


@pytest.mark.parametrize("newer", [None, "", "missing_hardware", "missing_parent"])
async def test_newer_firmware_clearing_wins_older_pending_reconcile(
    firmware: Firmware, monkeypatch: pytest.MonkeyPatch, newer: str | None
) -> None:
    await overlapping_firmware(firmware, monkeypatch, newer, False)


async def test_older_firmware_clearing_cannot_overwrite_newer_tag(
    firmware: Firmware, monkeypatch: pytest.MonkeyPatch
) -> None:
    await overlapping_firmware(firmware, monkeypatch, "F3", True)


async def overlapping_firmware(
    fw: Firmware, monkeypatch: pytest.MonkeyPatch, newer: str | None, older_clear: bool
) -> None:
    rig = replace(fw.rig, bridge=fw.bridge)
    await rig.bridge.reconcile()
    attempts: list[str | None] = []
    publish = rig.mqtt.publish

    async def fail_update(topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        if topic == fw.topic:
            attempts.append(json.loads(payload)["device"].get("sw_version"))
            if len(attempts) in (1, 2):
                raise RuntimeError("temporary config failure")
        await publish(topic, payload, retain, qos)

    monkeypatch.setattr(rig.mqtt, "publish", fail_update)
    await rig.notify(display_name="Known desk")
    await turns(120)
    assert len(attempts) == 1
    entered, release = asyncio.Event(), asyncio.Event()
    native = rig.harness.observers[0].get_device

    async def missing(device_id: str) -> _RawDevice | None:
        if device_id == "ble_mesh":
            fw.reads.append(device_id)
            if not entered.is_set():
                entered.set()
                await release.wait()
            return None
        result = await native(device_id)
        assert result is None or isinstance(result, _RawDevice)
        return result

    monkeypatch.setattr(rig.harness.observers[0], "get_device", missing)
    if older_clear:
        assert fw.own is not None and fw.own.peripherals is not None
        fw.own.peripherals.pop("hardware_peripheral")
    else:
        fw.hardware.variables["current_release_tag"].value = "F2"
    reads = len(fw.reads)
    older = asyncio.create_task(rig.bridge.reconcile())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        if older_clear:
            assert fw.own is not None and fw.own.peripherals is not None
            fw.own.peripherals["hardware_peripheral"] = fw.hardware
        if newer == "missing_hardware":
            assert fw.own is not None and fw.own.peripherals is not None
            fw.own.peripherals.pop("hardware_peripheral")
        elif newer == "missing_parent":
            fw.own = None
        elif newer is None:
            fw.hardware.variables.pop("current_release_tag")
        else:
            fw.hardware.variables["current_release_tag"].value = newer
        expected = None if newer in (None, "", "missing_hardware", "missing_parent") else newer
        with pytest.raises(RuntimeError, match="temporary config failure"):
            await rig.bridge.reconcile()
        assert attempts[-1] == expected and rig.bridge._discovery_sw_version == expected
    finally:
        release.set()
        await asyncio.wait_for(older, 2)
    actual = (attempts[-2:], fw.version(), rig.bridge._discovery_sw_version)
    assert actual == ([expected, expected], expected, expected), actual
    # Both reconciles perform only their existing own/mesh reads.
    assert len(fw.reads) == reads + 4
    assert not rig.harness.write_starts


async def test_scoped_rebind_during_real_oversize_inventory_retires_cleanly(
    connected: Connected, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = connected
    registry = rig.bridge.mesh_registry
    assert registry is not None
    old = rig.owner()
    raw = rig.raw.peripherals
    assert raw is not None
    raw.update({f"overflow-{i}": copy.deepcopy(raw["blocker"]) for i in range(255)})
    snapshot = copy.deepcopy(rig.raw)
    obs = rig.harness.observers[0]

    async def scoped(device_id: str, pid: str) -> _RawPeripheral:
        assert device_id == "ble_mesh" and len(raw) == 257
        return raw[pid]

    monkeypatch.setattr(obs, "get_peripheral", scoped, raising=False)
    async with delayed_poll(rig, monkeypatch, snapshot):
        raw["load"].variables["max_intensity_value"].value = "1000"
        await rig.adapter.get_peripheral("ble_mesh", "load")
        successor = rig.owner()
        assert successor is not old
        driver = rig.bridge.mesh_driver
        assert driver is not None
        await driver.flush(successor)
        assert states(rig)[-1]["state"] == "ON"
    assert registry.disabled and registry.inventory_count == 257
    rig.raw.peripherals = {"blocker": raw["blocker"]}
    for _ in range(3):
        await rig.bridge.poll_once()
    observed = (
        registry.disabled,
        registry.inventory_ready,
        list(registry.retired),
        successor.retired,
        states(rig)[-1]["state"],
        [p for t, p, _ in rig.mqtt.published if t == "brilliant/mesh/availability"][-1],
    )
    assert observed[:3] == (False, True, []) and observed[-2:] == (None, "online"), observed


@pytest.mark.parametrize("size", [10, 40])
async def test_reconnect_looks_up_only_indexed_topics_once(
    connected: Connected, monkeypatch: pytest.MonkeyPatch, size: int
) -> None:
    rig = connected
    routing = rig.bridge._mesh_routing
    assert rig.raw.peripherals is not None
    rig.raw.peripherals.update(
        {f"load-{i}": copy.deepcopy(rig.raw.peripherals["blocker"]) for i in range(size - 2)}
    )
    await rig.bridge.poll_once()
    lookups: list[str] = []
    enumerated: list[TopicRepair] = []

    class Traced(dict[str, TopicRepair]):
        def __getitem__(self, key: str) -> TopicRepair:
            lookups.append(key)
            return super().__getitem__(key)

        def values(self) -> dict_values[str, TopicRepair]:
            # Enumerating unrelated records must be visible even without children.
            enumerated.extend(super().values())
            return super().values()

    per_load = {t for t, record in routing.records.items() if record.peripheral_id is not None}
    monkeypatch.setattr(routing, "records", Traced(routing.records))
    registry = rig.bridge.mesh_registry
    assert registry is not None
    before = registry.owners
    rig.harness.procs[0].reconnect_cbs[0]()
    rig.adapter.reserve_mesh_boundary("review-flush-pending-hook")
    assert len(before) == size
    assert all(rig.owner(owner.metadata.peripheral_id) is not owner for owner in before)
    assert len(lookups) == len(set(lookups)) and set(lookups) == per_load and not enumerated


async def test_warm_inventory_repairs_missing_replacement_from_next_genuine_read(
    connected: Connected, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = connected
    registry = rig.bridge.mesh_registry
    assert registry is not None
    raw = copy.deepcopy(rig.raw)
    assert raw.peripherals is not None
    raw.peripherals = {"other": copy.deepcopy(raw.peripherals["blocker"])}
    rig.raw.peripherals = raw.peripherals
    rig.harness.write_release.set()
    async with delayed_poll(rig, monkeypatch, raw):
        await rig.mqtt.inject("brilliant/mesh/load/set", '{"state":"OFF"}')
    assert not registry.owners
    await rig.bridge.poll_once()
    assert rig.bridge._resolve_mesh_owner("brilliant/mesh/other/set") is rig.owner("other")
    assert registry.inventory_ready and states(rig, "other")[-1]["state"] == "ON"


async def test_older_fenced_inventory_does_not_demote_newer_ready_inventory(
    connected: Connected, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = connected
    registry = rig.bridge.mesh_registry
    assert registry is not None
    snapshot = copy.deepcopy(rig.raw)
    rig.harness.write_release.set()
    async with delayed_poll(rig, monkeypatch, snapshot):
        await rig.adapter.get_all()
        assert registry.inventory_ready
        await rig.mqtt.inject("brilliant/mesh/load/set", '{"state":"OFF"}')
        assert len(rig.harness.write_starts) == 1
    assert registry.inventory_ready and not registry.disabled
    assert len(registry.owners) == 2


async def test_omission_shape_does_not_retry_unchanged_failed_initial_topic(
    firmware: Firmware, monkeypatch: pytest.MonkeyPatch
) -> None:
    fw = firmware
    rig = replace(fw.rig, bridge=fw.bridge)
    attempts = 0
    publish = rig.mqtt.publish

    async def failed(topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        nonlocal attempts
        if topic == fw.topic:
            attempts += 1
            if attempts <= 2:
                raise RuntimeError("initial config failure")
        await publish(topic, payload, retain, qos)

    monkeypatch.setattr(rig.mqtt, "publish", failed)
    with pytest.raises(RuntimeError, match="initial config failure"):
        await rig.bridge.reconcile()
    snapshot = copy.deepcopy(rig.raw)
    assert snapshot.peripherals is not None
    snapshot.peripherals.pop("load")
    async with delayed_poll(rig, monkeypatch, snapshot):
        await rig.notify(display_name="Updated desk", on="0")
        await turns(100)
        record = rig.bridge._mesh_routing.records[fw.topic]
        worker, desired = record.worker, record.desired
        assert worker is not None and worker.done() and record.applied is None
        assert attempts == 2
    assert attempts == 2 and record.worker is worker and record.desired is desired
    assert f"brilliant/{fw.panel}/load/set_enable_motion_score" not in rig.mqtt.subscriptions
