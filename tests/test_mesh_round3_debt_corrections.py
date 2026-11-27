"""Settled topic debt must not join another topic's running MQTT operation."""

from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from tests.fakes import _RawDevice
from tests.mesh_regression_support import turns
from tests.test_mesh_bridge_integration import connected
from tests.test_mesh_discovery_metadata import Firmware, firmware

__all__ = ["connected", "firmware"]


@pytest.mark.parametrize("trigger", ["poll", "reconcile"])
async def test_settled_debt_must_not_join_other_running_topic(
    firmware: Firmware, monkeypatch: pytest.MonkeyPatch, trigger: str
) -> None:
    fw = firmware
    rig = replace(fw.rig, bridge=fw.bridge)
    await rig.bridge.reconcile()
    primary = fw.topic
    auxiliary = f"homeassistant/switch/brilliant_{fw.panel}_load_enable_motion_score/config"
    publish = rig.mqtt.publish
    entered, release = asyncio.Event(), asyncio.Event()
    attempts = 0

    async def gated(topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        nonlocal attempts
        if topic == primary:
            attempts += 1
            if attempts == 1:
                raise RuntimeError("temporary known primary config failure")
        if topic == auxiliary:
            entered.set()
            await release.wait()
        await publish(topic, payload, retain, qos)

    monkeypatch.setattr(rig.mqtt, "publish", gated)
    sweep: asyncio.Task[None] | None = None
    try:
        await rig.notify(display_name="Updated name")
        await asyncio.wait_for(entered.wait(), 2)
        await turns(100)
        owner = rig.owner()
        records = rig.bridge._mesh_routing.records
        assert records[primary].applied is not None
        assert records[primary].applied is not records[primary].desired
        settled = records[primary].worker
        assert settled is not None and settled.done()
        running = records[auxiliary].worker
        assert running is not None and not running.done()
        child = records[auxiliary].child
        assert child is not None and not child.done()
        healthy = tuple(
            (record, record.worker)
            for record in records.values()
            if record.desired.owner is owner and record.applied is record.desired
        )
        native = rig.harness.observers[0].get_device

        async def absent(device_id: str) -> _RawDevice | None:
            if device_id == "ble_mesh":
                fw.reads.append(device_id)
                return None
            result = await native(device_id)
            assert result is None or isinstance(result, _RawDevice)
            return result

        monkeypatch.setattr(rig.harness.observers[0], "get_device", absent)
        read_count = len(fw.reads)
        sweep = asyncio.create_task(
            rig.bridge.poll_once() if trigger == "poll" else rig.bridge.reconcile()
        )
        await turns(150)
        assert attempts == 2, "eligible settled debt must still retry once"
        assert len(fw.reads) == read_count + 2
        assert rig.owner() is owner
        assert records[primary].applied is records[primary].desired
        assert records[auxiliary].worker is running and not running.done()
        assert records[auxiliary].child is child and not child.done()
        assert all(record.worker is worker for record, worker in healthy)
        assert rig.adapter.mesh_admission_open and not rig.adapter.consume_mesh_fatal()
        print(
            f"scope={fw.panel} trigger={trigger} retries={attempts} "
            f"reads=2 sweep_done={sweep.done()}"
        )
        assert sweep.done(), "no-carrier sweep joined unrelated running config child"
        await sweep
    finally:
        release.set()
        if sweep is not None:
            await asyncio.gather(sweep, return_exceptions=True)
        await turns(100)
