"""Authoritative firmware absence with the production panel-only adapter scope."""

from __future__ import annotations

import asyncio
import copy
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass

import pytest

from brilliant_mqtt.bridge import Bridge
from brilliant_mqtt.bus import RpcBusAdapter
from tests.fakes import FakeClock, FakeMqtt, _RawDevice, _RawPeripheral, _RawVariable, _StartHarness


@dataclass
class PanelFirmware:
    bridge: Bridge
    adapter: RpcBusAdapter
    mqtt: FakeMqtt
    harness: _StartHarness
    hardware: _RawPeripheral
    light: _RawPeripheral
    own: _RawDevice | None
    reads: list[str]


@pytest.fixture
async def panel_firmware(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[PanelFirmware]:
    clock = FakeClock()
    harness = _StartHarness(monkeypatch)
    adapter = RpcBusAdapter(clock=clock)
    await adapter.start()
    hardware = _RawPeripheral("1")
    hardware.peripheral_type = 22
    hardware.variables["current_release_tag"] = _RawVariable("F2")
    light = _RawPeripheral("1")
    light.peripheral_type = 27
    own = _RawDevice(adapter.own_device_id, {"hardware_peripheral": hardware, "wired": light})
    mqtt = FakeMqtt()
    bridge = Bridge(adapter, mqtt, "own", include=lambda did: did != "ble_mesh", clock=clock)
    rig = PanelFirmware(bridge, adapter, mqtt, harness, hardware, light, own, [])

    async def get_device(device_id: str) -> _RawDevice | None:
        rig.reads.append(device_id)
        return rig.own

    monkeypatch.setattr(harness.observers[0], "get_device", get_device, raising=False)
    try:
        yield rig
    finally:
        await bridge.withdraw()
        await bridge.shutdown_wired_feedback()
        await adapter.shutdown()


def omit_hardware(rig: PanelFirmware, shape: str) -> None:
    if shape == "missing_parent":
        rig.own = None
    elif shape == "incomplete_parent":
        rig.own = _RawDevice(rig.adapter.own_device_id, None)
    else:
        assert shape == "absent_hardware"
        assert rig.own is not None and rig.own.peripherals is not None
        rig.own.peripherals.pop("hardware_peripheral")


def firmware_meta(rig: PanelFirmware) -> str | None:
    meta = [json.loads(p) for t, p, _ in rig.mqtt.published if t == "brilliant/own/bridge"][-1]
    version = meta.get("panel_firmware")
    assert version is None or isinstance(version, str)
    return version


def wired_firmware(rig: PanelFirmware) -> str | None:
    config = [
        json.loads(p)
        for t, p, _ in rig.mqtt.published
        if t == "homeassistant/light/brilliant_own_wired/config"
    ][-1]
    version = config["device"].get("sw_version")
    assert version is None or isinstance(version, str)
    return version


@pytest.mark.parametrize("shape", ["absent_hardware", "missing_parent", "incomplete_parent"])
async def test_panel_only_authoritative_absence_clears_previous_firmware(
    panel_firmware: PanelFirmware, shape: str
) -> None:
    rig = panel_firmware
    await rig.bridge.reconcile()
    assert rig.bridge._discovery_sw_version == "F2"
    before_order = rig.bridge._discovery_capture_order
    assert before_order[1] == rig.adapter.capture_sequence == 1
    omit_hardware(rig, shape)
    await rig.bridge.reconcile()
    assert len(rig.reads) == 2 and not rig.harness.write_starts
    actual = (rig.bridge._discovery_sw_version, firmware_meta(rig))
    if shape == "absent_hardware":
        assert wired_firmware(rig) is None
    assert rig.bridge._discovery_capture_order == before_order
    await rig.bridge.reconcile()
    assert rig.bridge._discovery_sw_version is None, "a genuinely later fallback recovers"
    assert actual == (None, None), (shape, before_order, actual)


@pytest.mark.parametrize("shape", ["absent_hardware", "missing_parent", "incomplete_parent"])
async def test_panel_only_old_real_capture_cannot_undo_equal_order_clear(
    panel_firmware: PanelFirmware, monkeypatch: pytest.MonkeyPatch, shape: str
) -> None:
    rig = panel_firmware
    await rig.bridge.reconcile()
    old_raw = copy.deepcopy(rig.own)
    entered, release = asyncio.Event(), asyncio.Event()
    first = True

    async def get_device(device_id: str) -> _RawDevice | None:
        nonlocal first
        rig.reads.append(device_id)
        if first:
            first = False
            entered.set()
            await release.wait()
            return old_raw
        return rig.own

    monkeypatch.setattr(rig.harness.observers[0], "get_device", get_device)
    older = asyncio.create_task(rig.bridge.reconcile())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        sequence = rig.adapter.capture_sequence
        omit_hardware(rig, shape)
        await rig.bridge.reconcile()
        assert rig.bridge._discovery_capture_order[1] == sequence
        assert rig.bridge._discovery_sw_version is firmware_meta(rig) is None
    finally:
        release.set()
        await asyncio.wait_for(older, 2)
    assert rig.bridge._discovery_sw_version is firmware_meta(rig) is wired_firmware(rig) is None
    assert len(rig.reads) == 3 and not rig.harness.write_starts
    rig.hardware.variables["current_release_tag"].value = "F3"
    rig.own = _RawDevice(
        rig.adapter.own_device_id, {"hardware_peripheral": rig.hardware, "wired": rig.light}
    )
    await rig.bridge.reconcile()
    assert rig.bridge._discovery_sw_version == firmware_meta(rig) == wired_firmware(rig) == "F3"
    assert rig.bridge._discovery_capture_order[1] > sequence
    assert len(rig.reads) == 4 and not rig.harness.write_starts


@pytest.mark.parametrize("shape", ["absent_hardware", "missing_parent", "incomplete_parent"])
async def test_panel_only_old_absence_cannot_clear_newer_real_hardware(
    panel_firmware: PanelFirmware, monkeypatch: pytest.MonkeyPatch, shape: str
) -> None:
    rig = panel_firmware
    await rig.bridge.reconcile()
    omit_hardware(rig, shape)
    old_raw = copy.deepcopy(rig.own)
    entered, release = asyncio.Event(), asyncio.Event()
    first = True

    async def get_device(device_id: str) -> _RawDevice | None:
        nonlocal first
        rig.reads.append(device_id)
        if first:
            first = False
            entered.set()
            await release.wait()
            return old_raw
        return rig.own

    monkeypatch.setattr(rig.harness.observers[0], "get_device", get_device)
    older = asyncio.create_task(rig.bridge.reconcile())
    try:
        await asyncio.wait_for(entered.wait(), 2)
        rig.hardware.variables["current_release_tag"].value = "F3"
        rig.own = _RawDevice(
            rig.adapter.own_device_id, {"hardware_peripheral": rig.hardware, "wired": rig.light}
        )
        await rig.bridge.reconcile()
        accepted_order = rig.bridge._discovery_capture_order
    finally:
        release.set()
        await asyncio.wait_for(older, 2)
    assert rig.bridge._discovery_capture_order == accepted_order
    assert rig.bridge._discovery_sw_version == firmware_meta(rig) == wired_firmware(rig) == "F3"
    assert len(rig.reads) == 3 and not rig.harness.write_starts
