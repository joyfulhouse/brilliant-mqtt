"""Existing config debt uses current firmware even without a mesh carrier."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable

import pytest

from brilliant_mqtt.mesh_routing import TopicRepair
from tests.fakes import _RawDevice
from tests.mesh_regression_support import turns
from tests.test_mesh_bridge_integration import connected
from tests.test_mesh_discovery_metadata import Firmware, firmware

__all__ = ["connected", "firmware"]


def omit_mesh(fw: Firmware, monkeypatch: pytest.MonkeyPatch) -> None:
    native = fw.rig.harness.observers[0].get_device

    async def absent(device_id: str) -> _RawDevice | None:
        if device_id == "ble_mesh":
            fw.reads.append(device_id)
            return None
        result = await native(device_id)
        assert result is None or isinstance(result, _RawDevice)
        return result

    monkeypatch.setattr(fw.rig.harness.observers[0], "get_device", absent)


@pytest.mark.parametrize("updated", ["v26.11.27.2", None, ""], ids=["changed", "removed", "empty"])
@pytest.mark.parametrize("running", [False, True], ids=["settled", "running_sibling"])
async def test_existing_config_debt_refreshes_firmware_without_carrier(
    firmware: Firmware, monkeypatch: pytest.MonkeyPatch, updated: str | None, running: bool
) -> None:
    fw = firmware
    rig = fw.rig
    await fw.bridge.reconcile()
    before = fw.version()
    attempts = 0
    publish = rig.mqtt.publish
    auxiliary = f"homeassistant/switch/brilliant_{fw.panel}_load_enable_motion_score/config"
    entered, release = asyncio.Event(), asyncio.Event()

    async def fail_once(topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        nonlocal attempts
        if topic == fw.topic:
            attempts += 1
            if attempts == 1:
                raise RuntimeError("temporary known config failure")
            assert json.loads(payload)["device"].get("sw_version") == (updated or None)
        if running and topic == auxiliary:
            entered.set()
            await release.wait()
        await publish(topic, payload, retain, qos)

    monkeypatch.setattr(rig.mqtt, "publish", fail_once)
    sweep: asyncio.Task[None] | None = None
    try:
        await rig.notify(display_name="New known name")
        if running:
            await asyncio.wait_for(entered.wait(), 2)
        await turns(150)
        records = fw.bridge._mesh_routing.records
        record = records[fw.topic]
        assert record.worker is not None and record.worker.done() and attempts == 1
        assert record.applied is not None and record.applied is not record.desired
        stale = record.desired
        assert stale.valid()
        registry = fw.bridge.mesh_registry
        assert registry is not None
        owner = registry.current("load")
        assert owner is not None
        sibling_worker, sibling_child = records[auxiliary].worker, records[auxiliary].child
        if running:
            assert sibling_worker is not None and not sibling_worker.done()
            assert sibling_child is not None and not sibling_child.done()
        healthy = tuple(
            (item, item.worker, item.desired)
            for item in records.values()
            if item.desired.owner is owner and item.applied is item.desired
        )
        omit_mesh(fw, monkeypatch)
        if updated is None:
            fw.hardware.variables.pop("current_release_tag")
        else:
            fw.hardware.variables["current_release_tag"].value = updated
        reads = len(fw.reads)
        sweep = asyncio.create_task(fw.bridge.reconcile())
        await turns(150)
        assert len(fw.reads) == reads + 2
        assert registry.current("load") is owner
        assert fw.bridge._discovery_sw_version == (updated or None)
        assert not stale.valid()
        print(
            f"scope={fw.panel} attempts={attempts} "
            f"desired_valid={record.desired.valid()} firmware={fw.version()}"
        )
        assert attempts == 2, "config debt must repair using current firmware without a carrier"
        assert record.applied is record.desired and record.desired is not stale
        assert record.desired.valid()
        assert fw.version() == (updated or None) and fw.version() != before
        assert "New known name" in fw.configs()[-1]
        assert all(
            item.worker is worker and item.desired is desired for item, worker, desired in healthy
        )
        if running:
            assert records[auxiliary].worker is sibling_worker
            assert records[auxiliary].child is sibling_child
            assert sibling_worker is not None and not sibling_worker.done()
            assert sibling_child is not None and not sibling_child.done()
        assert sweep.done(), "refreshed debt must not join running siblings"
        await sweep
        if fw.panel == "mesh":
            assert [
                payload
                for topic, payload, _ in rig.mqtt.published
                if topic == "brilliant/mesh/availability"
            ][-1] == "offline"
        assert rig.adapter.mesh_admission_open and not rig.adapter.consume_mesh_fatal()
        assert not rig.harness.write_starts
    finally:
        release.set()
        if sweep is not None:
            await asyncio.gather(sweep, return_exceptions=True)
        await turns(100)


async def test_firmware_change_without_debt_does_not_force_healthy_topics(
    firmware: Firmware, monkeypatch: pytest.MonkeyPatch
) -> None:
    fw = firmware
    await fw.bridge.reconcile()
    before = fw.version()
    registry = fw.bridge.mesh_registry
    assert registry is not None
    healthy = tuple(
        (record, record.worker, record.desired)
        for record in fw.bridge._mesh_routing.records.values()
        if record.peripheral_id is not None
    )
    assert healthy and all(record.applied is desired for record, _, desired in healthy)
    configs = len(fw.configs())
    fw.hardware.variables["current_release_tag"].value = "v26.11.27.2"
    omit_mesh(fw, monkeypatch)
    reads = len(fw.reads)
    await fw.bridge.reconcile()
    assert fw.bridge._discovery_sw_version == "v26.11.27.2"
    assert len(fw.reads) == reads + 2
    assert len(fw.configs()) == configs and fw.version() == before
    assert all(
        record.worker is worker and record.desired is desired for record, worker, desired in healthy
    )


async def test_refreshed_payload_rechecks_firmware_at_actual_child_issue(
    firmware: Firmware, monkeypatch: pytest.MonkeyPatch
) -> None:
    fw = firmware
    await fw.bridge.reconcile()
    published = fw.rig.mqtt.publish
    attempts = 0

    async def fail_once(topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        nonlocal attempts
        if topic == fw.topic:
            attempts += 1
            if attempts == 1:
                raise RuntimeError("temporary config failure")
            assert json.loads(payload)["device"]["sw_version"] == "F3"
        await published(topic, payload, retain, qos)

    monkeypatch.setattr(fw.rig.mqtt, "publish", fail_once)
    await fw.rig.notify(display_name="Known current name")
    await turns(150)
    record = fw.bridge._mesh_routing.records[fw.topic]
    assert record.worker is not None and record.worker.done() and attempts == 1
    old_applied = record.applied
    assert old_applied is not None and old_applied is not record.desired
    omit_mesh(fw, monkeypatch)
    routing = fw.bridge._mesh_routing
    operation = routing._operation
    changed = False

    async def refresh_before_child(target: TopicRepair, child: Awaitable[None]) -> None:
        nonlocal changed
        if target is record and not changed:
            changed = True
            fw.hardware.variables["current_release_tag"].value = "F3"
            await fw.bridge.reconcile()
        await operation(target, child)

    monkeypatch.setattr(routing, "_operation", refresh_before_child)
    fw.hardware.variables["current_release_tag"].value = "F2"
    reads = len(fw.reads)
    await fw.bridge.reconcile()
    assert changed and attempts == 1
    assert len(fw.reads) == reads + 4, "two explicit reconciles use their existing reads"
    assert fw.bridge._discovery_sw_version == "F3"
    assert record.applied is old_applied and record.applied is not record.desired
    assert not record.desired.valid(), "the F2 child cannot issue after authoritative F3"
    await fw.bridge.poll_once()
    assert len(fw.reads) == reads + 6
    assert attempts == 2 and record.applied is record.desired and record.desired.valid()
    assert fw.version() == "F3"
