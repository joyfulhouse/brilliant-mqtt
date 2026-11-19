"""Camera authorization and teardown regressions, without panel access."""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from brilliant_mqtt.ambilight import AmbilightController, AmbilightCrop
from brilliant_mqtt.bridge import Bridge
from brilliant_mqtt.model import BrilliantDevice, CaptureProvenance, DeviceKind, Variable
from tests.fakes import FakeBus, FakeMqtt


def hardware(value: str | None = "0") -> BrilliantDevice:
    return BrilliantDevice(
        device_id="panel_1",
        peripheral_id="hardware",
        name="Hardware",
        kind=DeviceKind.HARDWARE,
        variables={} if value is None else {"privacy_toggle": Variable("privacy_toggle", value)},
    )


class HeldCamera:
    def __init__(self, *, ignore_cancel: bool = False) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.settled = asyncio.Event()
        self.calls = 0
        self.ignore_cancel = ignore_cancel

    async def _capture(self) -> bytes:
        self.calls += 1
        self.started.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            if not self.ignore_cancel:
                raise
        finally:
            self.settled.set()
        return b"\xff\xd8\xff\xd9"

    async def capture_jpeg_snapshot(self, crop: AmbilightCrop) -> bytes:
        return await self._capture()

    async def capture_i420_frame(self, crop: AmbilightCrop) -> bytes:
        await self._capture()
        return bytes([120]) * (crop.width * crop.height * 3 // 2)


@pytest.mark.parametrize("reader", ["poll", "reconcile", "shared_poll"])
@pytest.mark.parametrize("missing", ["hardware", "value", "unknown", "invalid_generation"])
async def test_current_snapshot_retires_privacy_permission(reader: str, missing: str) -> None:
    bus = FakeBus([hardware()])
    mqtt = FakeMqtt()
    bridge = Bridge(bus, mqtt, "living_room")
    await bridge.reconcile()
    assert bridge.is_privacy_off()
    if missing == "hardware":
        bus._devices = []
    elif missing == "value":
        bus._devices = [hardware(None)]
    elif missing == "unknown":
        bus._devices = [hardware("unknown")]
    else:
        bus._devices = [replace(hardware(), capture_provenance=CaptureProvenance(0, 99))]
    if reader == "reconcile":
        await bridge.reconcile()
    elif reader == "shared_poll":
        await bridge.poll_once(await bus.get_all())
    else:
        await bridge.poll_once()
    assert not bridge.is_privacy_off()
    camera = HeldCamera()
    controller = AmbilightController(mqtt, "living_room", camera, privacy_off=bridge.is_privacy_off)
    assert not await controller.trigger_snapshot()
    assert await controller.sample_once() is None
    assert await controller.auto_calibrate_crop() is None
    assert camera.calls == 0
    # A fresh explicit observation can restore permission.
    bus._devices = [hardware()]
    await bridge.poll_once()
    assert bridge.is_privacy_off()
    await bridge.shutdown_wired_feedback()


async def test_generation_change_revokes_before_reconcile_and_rejects_old_push() -> None:
    bus = FakeBus([hardware()])
    bridge = Bridge(bus, FakeMqtt(), "living_room")
    await bridge.reconcile()
    assert bridge.is_privacy_off()
    bus._capture_generation += 1
    assert not bridge.is_privacy_off()
    await bridge._on_change(replace(hardware(), capture_provenance=CaptureProvenance(1, 100)))
    assert not bridge.is_privacy_off()
    await bridge._on_change(replace(hardware(), capture_provenance=CaptureProvenance(2, 1)))
    assert bridge.is_privacy_off()
    # A late older observation cannot reopen privacy after a newer explicit ON.
    await bridge._on_change(replace(hardware("1"), capture_provenance=CaptureProvenance(2, 3)))
    await bridge._on_change(replace(hardware(), capture_provenance=CaptureProvenance(2, 2)))
    assert not bridge.is_privacy_off()
    await bridge.shutdown_wired_feedback()


async def test_reconnect_revokes_permission_before_waiting_for_snapshot() -> None:
    bus = FakeBus([hardware()])
    bridge = Bridge(bus, FakeMqtt(), "living_room")
    await bridge.reconcile()
    entered = asyncio.Event()
    release = asyncio.Event()

    async def held_get_all() -> list[BrilliantDevice]:
        entered.set()
        await release.wait()
        return []

    bus.get_all = held_get_all  # type: ignore[method-assign]
    task = asyncio.create_task(bridge.reconcile_after_reconnect())
    await asyncio.wait_for(entered.wait(), 1)
    assert not bridge.is_privacy_off()
    release.set()
    await task
    assert not bridge.is_privacy_off()
    await bridge.shutdown_wired_feedback()


async def test_hardware_push_without_entities_and_bridge_shutdown_revoke_permission() -> None:
    bus = FakeBus([hardware()])
    bridge = Bridge(bus, FakeMqtt(), "living_room")
    await bridge.reconcile()
    await bridge._on_change(hardware(None))
    assert not bridge.is_privacy_off()
    await bridge._on_change(hardware())
    assert bridge.is_privacy_off()
    await bridge.shutdown_wired_feedback()
    await bridge._on_change(hardware())
    assert not bridge.is_privacy_off()


@pytest.mark.parametrize("operation", ["snapshot", "auto_crop", "sample", "direct_snapshot"])
@pytest.mark.parametrize("ignore_cancel", [False, True])
async def test_shutdown_settles_camera_work_and_rejects_new_commands(
    operation: str, ignore_cancel: bool, tmp_path: Path
) -> None:
    mqtt = FakeMqtt()
    camera = HeldCamera(ignore_cancel=ignore_cancel)
    state_path = tmp_path / "camera.json"
    controller = AmbilightController(
        mqtt, "living_room", camera, state_path=state_path, privacy_off=lambda: True
    )
    original_crop = controller.crop
    work: Coroutine[Any, Any, object]
    if operation == "sample":
        work = controller.sample_once()
    elif operation == "direct_snapshot":
        work = controller.trigger_snapshot()
    else:
        work = controller.on_mqtt_command(
            f"brilliant/living_room/ambilight/set_{operation}", "PRESS"
        )
    task = asyncio.create_task(work)
    await asyncio.wait_for(camera.started.wait(), 1)
    await asyncio.wait_for(controller.shutdown(), 1)
    await asyncio.wait_for(task, 1)
    assert camera.settled.is_set()
    assert mqtt.published == [("brilliant/living_room/ambilight/snapshot", "", True)]

    assert controller.crop == original_crop
    assert not controller.auto_crop_enabled
    assert controller.last_sample is None
    assert not state_path.exists()
    for command, value in [("snapshot", "PRESS"), ("auto_crop", "ON"), ("enabled", "ON")]:
        await controller.on_mqtt_command(f"brilliant/living_room/ambilight/set_{command}", value)
    assert not await controller.trigger_snapshot()
    assert await controller.sample_once() is None
    assert await controller.auto_calibrate_crop() is None
    assert camera.calls == 1
    assert not controller.enabled
    assert controller._poll_task is None
    assert mqtt.published == [("brilliant/living_room/ambilight/snapshot", "", True)]


async def test_shutdown_settles_poll_without_sleeping_again() -> None:
    camera = HeldCamera(ignore_cancel=True)
    sleeps: list[float] = []

    async def sleep(interval: float) -> None:
        sleeps.append(interval)

    controller = AmbilightController(
        FakeMqtt(),
        "living_room",
        camera,
        default_enabled=True,
        privacy_off=lambda: True,
        sleep=sleep,
    )
    await controller.start()
    await asyncio.wait_for(camera.started.wait(), 1)
    await asyncio.wait_for(controller.shutdown(), 1)
    assert camera.settled.is_set()
    assert not sleeps
    assert controller._poll_task is None
