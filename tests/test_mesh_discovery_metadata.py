"""Discovery metadata survives ordinary mesh repair on the production adapter."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass

import pytest

from brilliant_mqtt.bridge import Bridge
from tests.fakes import _RawDevice, _RawPeripheral, _RawVariable
from tests.mesh_regression_support import turns
from tests.test_mesh_bridge_integration import Connected, connected

__all__ = ["connected"]

_OLD = "v26.05.20.2"
_NEW = "v26.11.26.1"


@dataclass
class Firmware:
    rig: Connected
    bridge: Bridge
    panel: str
    hardware: _RawPeripheral
    own: _RawDevice | None
    reads: list[str]

    @property
    def topic(self) -> str:
        return f"homeassistant/light/brilliant_{self.panel}_load/config"

    def configs(self) -> list[str]:
        return [payload for topic, payload, _ in self.rig.mqtt.published if topic == self.topic]

    def version(self) -> str | None:
        payloads = self.configs()
        assert payloads
        version = json.loads(payloads[-1])["device"].get("sw_version")
        assert version is None or isinstance(version, str)
        return version


@pytest.fixture(params=["shared", "default"])
async def firmware(
    connected: Connected,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> AsyncIterator[Firmware]:
    rig = connected
    await rig.bridge.withdraw()
    await rig.bridge.shutdown_mesh_feedback()
    hardware = _RawPeripheral("1")
    hardware.peripheral_type = 22
    hardware.variables["current_release_tag"] = _RawVariable(_OLD)
    own = _RawDevice(rig.adapter.own_device_id, {"hardware_peripheral": hardware})
    panel = "own" if request.param == "default" else "mesh"
    if request.param == "default":
        bridge = Bridge(rig.adapter, rig.mqtt, panel, clock=rig.clock)
    else:
        bridge = Bridge(rig.adapter, rig.mqtt, panel, include=lambda _did: True, clock=rig.clock)
    context = Firmware(rig, bridge, panel, hardware, own, [])

    async def get_device(device_id: str) -> _RawDevice | None:
        context.reads.append(device_id)
        return context.own if device_id == rig.adapter.own_device_id else rig.raw

    monkeypatch.setattr(rig.harness.observers[0], "get_device", get_device)
    try:
        rig.mqtt.published.clear()
        yield context
    finally:
        await bridge.withdraw()
        await bridge.shutdown_mesh_feedback()
        await bridge.shutdown_wired_feedback()


@pytest.mark.parametrize("repair", ["poll", "push"])
async def test_ordinary_repair_keeps_firmware_without_extra_config_or_reads(
    firmware: Firmware,
    repair: str,
) -> None:
    fw = firmware
    await fw.bridge.reconcile()
    assert fw.version() == _OLD
    publications = len(fw.configs())
    before_reads = len(fw.reads)
    if repair == "poll":
        carriers = await fw.rig.adapter.get_all()
        assert len(fw.reads) == before_reads + 2
        before_reads = len(fw.reads)
        await fw.bridge.poll_once(carriers)
    else:
        await fw.rig.notify(on="0")
        await turns(100)
    assert fw.version() == _OLD
    assert len(fw.configs()) == publications
    assert len(fw.reads) == before_reads
    assert fw.rig.harness.write_starts == []


@pytest.mark.parametrize("shape", ["scoped_batch", "parent_none", "parent_incomplete"])
async def test_omitted_or_incomplete_poll_context_does_not_clear_firmware(
    firmware: Firmware,
    shape: str,
) -> None:
    fw = firmware
    await fw.bridge.reconcile()
    if shape == "parent_none":
        fw.own = None
    elif shape == "parent_incomplete":
        fw.own = _RawDevice(fw.rig.adapter.own_device_id, None)
    carriers = await fw.rig.adapter.get_all()
    if shape == "scoped_batch":
        carriers = [device for device in carriers if device.device_id == "ble_mesh"]
    before_reads, publications = len(fw.reads), len(fw.configs())
    await fw.bridge.poll_once(carriers)
    assert fw.version() == _OLD
    assert len(fw.configs()) == publications
    assert len(fw.reads) == before_reads


@pytest.mark.parametrize("replacement", [_NEW, None, ""])
async def test_authoritative_reconcile_refreshes_or_clears_firmware_for_later_repairs(
    firmware: Firmware,
    replacement: str | None,
) -> None:
    fw = firmware
    await fw.bridge.reconcile()
    assert fw.version() == _OLD
    if replacement is None:
        fw.hardware.variables.pop("current_release_tag")
    else:
        fw.hardware.variables["current_release_tag"].value = replacement
    await fw.bridge.reconcile()
    expected = replacement or None
    assert fw.version() == expected
    publications = len(fw.configs())
    carriers = await fw.rig.adapter.get_all()
    before_reads = len(fw.reads)
    await fw.bridge.poll_once(carriers)
    await fw.rig.notify(on="0")
    await turns(100)
    assert fw.version() == expected
    assert len(fw.configs()) == publications
    assert len(fw.reads) == before_reads


async def test_mesh_only_reconcile_clears_excluded_hardware_context(
    firmware: Firmware,
) -> None:
    fw = firmware
    await fw.bridge.reconcile()
    assert fw.version() == _OLD
    fw.bridge._include = lambda did: did == "ble_mesh"
    await fw.bridge.reconcile()
    assert fw.version() is None
    publications = len(fw.configs())
    await fw.bridge.poll_once(await fw.rig.adapter.get_all())
    await fw.rig.notify(on="0")
    await turns(100)
    assert fw.version() is None
    assert len(fw.configs()) == publications


async def test_reconcile_context_refresh_blocks_online_before_mesh_configs_are_requested(
    firmware: Firmware,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fw = firmware
    await fw.bridge.reconcile()
    assert fw.version() == _OLD
    assert fw.bridge._mesh_online_ready()
    entered, release = asyncio.Event(), asyncio.Event()
    original = fw.rig.mqtt.publish

    async def publish(topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        if "hardware_peripheral" in topic and topic.endswith("/config"):
            entered.set()
            await release.wait()
        await original(topic, payload, retain, qos)

    monkeypatch.setattr(fw.rig.mqtt, "publish", publish)
    fw.hardware.variables["current_release_tag"].value = _NEW
    refreshing = asyncio.create_task(fw.bridge.reconcile())
    try:
        await entered.wait()
        assert fw.version() == _OLD
        assert not fw.bridge._mesh_online_ready(), (
            "accepted old config cannot satisfy refreshed context"
        )
    finally:
        release.set()
        await asyncio.gather(refreshing, return_exceptions=True)
    assert fw.version() == _NEW
    assert fw.bridge._mesh_online_ready()


async def test_concurrent_poll_and_reconcile_finish_with_latest_firmware(
    firmware: Firmware,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fw = firmware
    await fw.bridge.reconcile()
    assert fw.version() == _OLD
    entered, release = asyncio.Event(), asyncio.Event()
    original = fw.rig.mqtt.publish
    first = True

    async def publish(topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        nonlocal first
        if first and topic == fw.topic:
            first = False
            entered.set()
            await release.wait()
        await original(topic, payload, retain, qos)

    monkeypatch.setattr(fw.rig.mqtt, "publish", publish)
    assert fw.rig.raw.peripherals is not None
    fw.rig.raw.peripherals["load"].variables["display_name"] = _RawVariable("Current mesh name")
    older = asyncio.create_task(fw.bridge.poll_once(await fw.rig.adapter.get_all()))
    await entered.wait()
    fw.hardware.variables["current_release_tag"].value = _NEW
    refreshing = asyncio.create_task(fw.bridge.reconcile())
    try:
        await turns(100)
    finally:
        release.set()
        await asyncio.gather(older, refreshing)
    assert fw.version() == _NEW
    publications = len(fw.configs())
    carriers = await fw.rig.adapter.get_all()
    before_reads = len(fw.reads)
    await fw.bridge.poll_once(carriers)
    await fw.rig.notify(on="0")
    await turns(100)
    assert fw.version() == _NEW
    assert len(fw.configs()) == publications
    assert len(fw.reads) == before_reads
