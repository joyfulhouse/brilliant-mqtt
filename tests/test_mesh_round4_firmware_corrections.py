"""Firmware clearing ties and local publications across reconcile awaits."""

from __future__ import annotations

import asyncio
import copy
import json

import pytest

from tests.fakes import _RawDevice, _RawPeripheral, _RawVariable
from tests.mesh_regression_support import turns
from tests.test_mesh_bridge_integration import connected
from tests.test_mesh_discovery_metadata import Firmware, firmware

__all__ = ["connected", "firmware"]


async def test_older_hardware_cannot_replace_newer_absence_at_equal_fallback_order(
    firmware: Firmware, monkeypatch: pytest.MonkeyPatch
) -> None:
    await hardware_tie(firmware, monkeypatch, "missing_parent")


@pytest.mark.parametrize("shape", ["incomplete_parent", "absent_hardware", "excluded_hardware"])
async def test_clearing_shapes_preserve_context_on_equal_old_hardware_capture(
    firmware: Firmware, monkeypatch: pytest.MonkeyPatch, shape: str
) -> None:
    await hardware_tie(firmware, monkeypatch, shape)


async def hardware_tie(fw: Firmware, monkeypatch: pytest.MonkeyPatch, shape: str) -> None:
    await fw.bridge.reconcile()
    fw.hardware.variables["current_release_tag"].value = "F2"
    native = fw.rig.harness.observers[0].get_device
    entered, release = asyncio.Event(), asyncio.Event()
    old_raw = copy.deepcopy(fw.own)
    first = True

    async def gate(device_id: str) -> _RawDevice | None:
        nonlocal first
        if device_id == fw.rig.adapter.own_device_id and first:
            first = False
            entered.set()
            await release.wait()
            return old_raw
        result = await native(device_id)
        assert result is None or isinstance(result, _RawDevice)
        return result

    monkeypatch.setattr(fw.rig.harness.observers[0], "get_device", gate)
    older = asyncio.create_task(fw.bridge.reconcile())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        old_read_order = fw.rig.adapter.capture_sequence
        included = fw.bridge._include
        if shape == "missing_parent":
            fw.own = None
        elif shape == "incomplete_parent":
            fw.own = _RawDevice(fw.rig.adapter.own_device_id, None)
        elif shape == "absent_hardware":
            assert fw.own is not None and fw.own.peripherals is not None
            fw.own.peripherals.pop("hardware_peripheral")
        else:
            assert shape == "excluded_hardware"
            fw.bridge._include = lambda did: did == "ble_mesh"
        await fw.bridge.reconcile()
        assert fw.bridge._discovery_sw_version is None
        assert fw.version() is None
        accepted_order = fw.bridge._discovery_capture_order
        assert accepted_order[1] == old_read_order
        fw.bridge._include = included
    finally:
        release.set()
        await asyncio.wait_for(older, 2)
    assert (fw.bridge._discovery_sw_version, fw.version()) == (None, None), {
        "accepted_absence": accepted_order,
        "final_order": fw.bridge._discovery_capture_order,
        "final_context": fw.bridge._discovery_sw_version,
        "final_mesh_config": fw.version(),
    }


@pytest.mark.parametrize("firmware", ["default"], indirect=True)
async def test_reconcile_resumed_after_newer_context_publishes_current_local_metadata(
    firmware: Firmware, monkeypatch: pytest.MonkeyPatch
) -> None:
    fw = firmware
    assert fw.panel == "own"
    await fw.bridge.reconcile()
    fw.hardware.variables["current_release_tag"].value = "F2"
    publish = fw.rig.mqtt.publish
    entered, release = asyncio.Event(), asyncio.Event()
    first = True

    async def gate(topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        nonlocal first
        if first and topic == "brilliant/own/availability":
            first = False
            entered.set()
            await release.wait()
        await publish(topic, payload, retain, qos)

    monkeypatch.setattr(fw.rig.mqtt, "publish", gate)
    older = asyncio.create_task(fw.bridge.reconcile())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        assert fw.bridge._discovery_sw_version == "F2"
        fw.hardware.variables["current_release_tag"].value = "F3"
        await fw.bridge.reconcile()
        assert fw.bridge._discovery_sw_version == "F3"
    finally:
        release.set()
        await asyncio.wait_for(older, 2)
    meta = [json.loads(p) for t, p, _ in fw.rig.mqtt.published if t == "brilliant/own/bridge"][-1]
    local = [
        json.loads(p)["device"].get("sw_version")
        for t, p, _ in fw.rig.mqtt.published
        if "hardware_peripheral" in t and t.endswith("/config")
    ][-1]
    actual = (fw.bridge._discovery_sw_version, meta.get("panel_firmware"), local, fw.version())
    assert actual == ("F3", "F3", "F3", "F3"), actual


async def test_delayed_hardware_read_cannot_undo_newer_hardware_absence(
    firmware: Firmware, monkeypatch: pytest.MonkeyPatch
) -> None:
    fw = firmware
    rig = fw.rig
    await fw.bridge.reconcile()
    attempts: list[str | None] = []
    publish = rig.mqtt.publish

    async def fail_twice(topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        if topic == fw.topic:
            attempts.append(json.loads(payload)["device"].get("sw_version"))
            if len(attempts) <= 2:
                raise RuntimeError("temporary config failure")
        await publish(topic, payload, retain, qos)

    monkeypatch.setattr(rig.mqtt, "publish", fail_twice)
    await rig.notify(display_name="Known desk")
    await turns(120)
    assert len(attempts) == 1
    fw.hardware.variables["current_release_tag"].value = "F2"
    older_own = copy.deepcopy(fw.own)
    entered, release = asyncio.Event(), asyncio.Event()
    own_reads = 0

    async def controlled(device_id: str) -> _RawDevice | None:
        nonlocal own_reads
        fw.reads.append(device_id)
        if device_id == "ble_mesh":
            return None
        own_reads += 1
        if own_reads == 1:
            entered.set()
            await release.wait()
            return older_own
        return None

    monkeypatch.setattr(rig.harness.observers[0], "get_device", controlled)
    reads = len(fw.reads)
    older = asyncio.create_task(fw.bridge.reconcile())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        with pytest.raises(RuntimeError, match="temporary config failure"):
            await fw.bridge.reconcile()
        assert fw.bridge._discovery_sw_version is None
    finally:
        release.set()
        await asyncio.wait_for(older, 2)
    assert len(fw.reads) == reads + 4
    actual = (attempts[-2:], fw.version(), fw.bridge._discovery_sw_version)
    assert actual == ([None, None], None, None), actual


async def test_genuinely_newer_hardware_restores_context_after_tied_clear(
    firmware: Firmware, monkeypatch: pytest.MonkeyPatch
) -> None:
    fw = firmware
    await hardware_tie(fw, monkeypatch, "missing_parent")
    cleared_order = fw.bridge._discovery_capture_order
    fw.hardware.variables["current_release_tag"].value = "F3"
    fw.own = _RawDevice(fw.rig.adapter.own_device_id, {"hardware_peripheral": fw.hardware})
    reads = len(fw.reads)
    await fw.bridge.reconcile()
    assert fw.bridge._discovery_capture_order > cleared_order
    assert fw.bridge._discovery_sw_version == fw.version() == "F3"
    assert len(fw.reads) == reads + 2
    assert not fw.rig.harness.write_starts


@pytest.mark.parametrize("firmware", ["default"], indirect=True)
@pytest.mark.parametrize("updated", ["F3", None, ""], ids=["tag", "removed", "empty"])
@pytest.mark.parametrize(
    "pause", ["availability", "meta", "hardware_config", "wired_config", "subscription"]
)
async def test_local_publications_after_each_await_use_current_firmware(
    firmware: Firmware, monkeypatch: pytest.MonkeyPatch, updated: str | None, pause: str
) -> None:
    fw = firmware
    await fw.bridge.reconcile()
    assert fw.own is not None and fw.own.peripherals is not None
    for pid in ("wired_a", "wired_b"):
        load = _RawPeripheral("1")
        load.peripheral_type = 27
        load.variables["enable_motion_score"] = _RawVariable("0")
        fw.own.peripherals[pid] = load
    fw.hardware.variables["muted"] = _RawVariable("0")
    fw.hardware.variables["screen_on"] = _RawVariable("1")
    fw.hardware.variables["current_release_tag"].value = "F2"
    entered, release = asyncio.Event(), asyncio.Event()
    publish, subscribe = fw.rig.mqtt.publish, fw.rig.mqtt.subscribe
    issued: tuple[str, str] | None = None
    first = True

    async def gate_publish(topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        nonlocal first, issued
        matches = (
            pause == "availability"
            and topic == "brilliant/own/availability"
            or pause == "meta"
            and topic == "brilliant/own/bridge"
            or pause == "hardware_config"
            and "hardware_peripheral" in topic
            and topic.endswith("/config")
            or pause == "wired_config"
            and topic == "homeassistant/light/brilliant_own_wired_a/config"
        )
        if first and matches:
            first = False
            issued = (topic, payload)
            entered.set()
            await release.wait()
        await publish(topic, payload, retain, qos)

    async def gate_subscribe(topic: str) -> None:
        nonlocal first
        if first and pause == "subscription" and topic == "brilliant/own/wired_a/set":
            first = False
            entered.set()
            await release.wait()
        await subscribe(topic)

    monkeypatch.setattr(fw.rig.mqtt, "publish", gate_publish)
    monkeypatch.setattr(fw.rig.mqtt, "subscribe", gate_subscribe)
    reads = len(fw.reads)
    older = asyncio.create_task(fw.bridge.reconcile())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        assert fw.bridge._discovery_sw_version == "F2"
        if updated is None:
            fw.hardware.variables.pop("current_release_tag")
        else:
            fw.hardware.variables["current_release_tag"].value = updated
        await fw.bridge.reconcile()
        assert fw.bridge._discovery_sw_version == (updated or None)
        after_newer = len(fw.rig.mqtt.published)
    finally:
        release.set()
        await asyncio.wait_for(older, 2)
    publications = fw.rig.mqtt.published[after_newer:]
    if issued is not None:
        # Already issued transport keeps its bytes; subsequent calls must refresh.
        assert publications[0][:2] == issued
        if pause == "meta":
            assert json.loads(issued[1])["panel_firmware"] == "F2"
        elif pause.endswith("config"):
            assert json.loads(issued[1])["device"]["sw_version"] == "F2"
        publications = publications[1:]
    configs = [json.loads(p) for t, p, _ in publications if t.endswith("/config")]
    assert configs and all(
        item["device"].get("sw_version") == (updated or None) for item in configs
    )
    assert any("wired_b" in t and t.endswith("/config") for t, _, _ in publications)
    for topic, payload, _ in publications:
        if topic == "brilliant/own/bridge":
            assert json.loads(payload).get("panel_firmware") == (updated or None)
    assert fw.bridge._discovery_sw_version == fw.version() == (updated or None)
    assert len(fw.reads) == reads + 4
    assert not fw.rig.harness.write_starts
