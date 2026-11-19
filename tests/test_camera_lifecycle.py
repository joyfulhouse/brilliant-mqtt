"""Camera authorization and teardown regressions, without panel access."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Coroutine
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from brilliant_mqtt.ambilight import AmbilightController, AmbilightCrop, GStreamerCameraAdapter
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


@pytest.mark.parametrize("ambiguous", [False, True])
async def test_omission_fences_captured_but_undelivered_same_generation_push(
    ambiguous: bool,
) -> None:
    bus = FakeBus([hardware()])
    bridge = Bridge(bus, FakeMqtt(), "living_room")
    await bridge.reconcile()
    delayed = replace(hardware(), capture_provenance=bus._next_provenance())
    # Captured after the last applied evidence, but queued before an authoritative read.
    bus._devices = [delayed, replace(delayed)] if ambiguous else []
    await bridge.poll_once()
    assert not bridge.is_privacy_off()
    await bridge._on_change(delayed)
    assert not bridge.is_privacy_off()
    # A later complete read supplies new explicit evidence and restores permission.
    bus._devices = [hardware()]
    await bridge.poll_once()
    assert bridge.is_privacy_off()
    await bridge.shutdown_wired_feedback()


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


async def test_privacy_revoked_during_state_publish_suppresses_color() -> None:
    privacy_off = True
    entered = asyncio.Event()
    release = asyncio.Event()

    class HeldMqtt(FakeMqtt):
        async def publish(
            self, topic: str, payload: str, retain: bool = False, qos: int = 0
        ) -> None:
            if topic.endswith("/ambilight/state"):
                entered.set()
                await release.wait()
            await super().publish(topic, payload, retain=retain, qos=qos)

    mqtt = HeldMqtt()
    camera = HeldCamera()
    camera.release.set()
    controller = AmbilightController(mqtt, "living_room", camera, privacy_off=lambda: privacy_off)
    task = asyncio.create_task(controller.sample_once())
    await asyncio.wait_for(entered.wait(), 1)
    privacy_off = False
    release.set()
    assert await task is None
    assert not any(topic.endswith("/color") for topic, _, _ in mqtt.published)
    await controller.shutdown()


async def test_cancelled_camera_lock_waiter_cleans_allocated_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    allocated = asyncio.Event()
    output = tmp_path / "camera.jpg"

    def mkstemp(*args: Any, **kwargs: Any) -> tuple[int, str]:
        fd = os.open(output, os.O_CREAT | os.O_WRONLY, 0o600)
        allocated.set()
        return fd, str(output)

    monkeypatch.setattr("brilliant_mqtt.ambilight.tempfile.mkstemp", mkstemp)
    monkeypatch.setattr("brilliant_mqtt.ambilight._device_exists_on_panel", lambda _: True)
    adapter = GStreamerCameraAdapter(privacy_off=lambda: True)
    await adapter._lock.acquire()
    task = asyncio.create_task(adapter.capture_jpeg_snapshot(AmbilightCrop()))
    try:
        await asyncio.wait_for(allocated.wait(), 1)
        assert output.exists()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not output.exists()
    finally:
        adapter._lock.release()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_real_bus_empty_read_fences_push_waiting_for_delivery() -> None:
    from brilliant_mqtt.bus import RpcBusAdapter
    from tests.fakes import _RawDevice, _RawPeripheral

    raw_hardware = _RawPeripheral("0", "privacy_toggle")
    raw_hardware.peripheral_type = 22
    raw = _RawDevice("panel_1", {"hardware": raw_hardware})

    class Observer:
        current = raw

        async def get_device(self, device_id: str) -> _RawDevice:
            return self.current

    observer = Observer()
    adapter = RpcBusAdapter()
    adapter._obs = observer
    adapter._own_device_id = "panel_1"
    bridge = Bridge(adapter, FakeMqtt(), "living_room")
    await bridge.reconcile()
    assert bridge.is_privacy_off()
    callback, coalesce, want = adapter._change_cbs[0]
    entered = asyncio.Event()
    release = asyncio.Event()

    async def held_callback(device: BrilliantDevice) -> None:
        entered.set()
        await release.wait()
        await callback(device)

    adapter._change_cbs[0] = (held_callback, coalesce, want)
    adapter._dispatch_raw_device(raw)
    try:
        await asyncio.wait_for(entered.wait(), 1)
        push_sequence = adapter.capture_sequence
        observer.current = _RawDevice("panel_1", {})
        await bridge.poll_once()
        assert adapter.capture_sequence > push_sequence
        assert not bridge.is_privacy_off()
        release.set()
        await asyncio.gather(*adapter._pending_tasks)
        assert not bridge.is_privacy_off()
        observer.current = raw
        await bridge.poll_once()
        assert bridge.is_privacy_off()
    finally:
        release.set()
        await asyncio.gather(*adapter._pending_tasks, return_exceptions=True)
        await bridge.shutdown_wired_feedback()


@pytest.mark.parametrize("values", [("0", "1"), ("1", "0"), ("0", "0")])
@pytest.mark.parametrize("previously_allowed", [False, True])
async def test_real_bus_ambiguous_push_never_authorizes_between_callbacks(
    values: tuple[str, str],
    previously_allowed: bool,
) -> None:
    from brilliant_mqtt.bus import RpcBusAdapter
    from tests.fakes import _RawDevice, _RawPeripheral

    def raw_hardware(value: str) -> _RawPeripheral:
        peripheral = _RawPeripheral(value, "privacy_toggle")
        peripheral.peripheral_type = 22
        return peripheral

    unambiguous = _RawDevice("panel_1", {"hardware": raw_hardware("0")})
    ambiguous = _RawDevice(
        "panel_1",
        {
            "hardware_a": raw_hardware(values[0]),
            "hardware_b": raw_hardware(values[1]),
        },
    )

    class Observer:
        current = unambiguous if previously_allowed else ambiguous

        async def get_device(self, device_id: str) -> _RawDevice:
            return self.current

    observer = Observer()
    adapter = RpcBusAdapter()
    adapter._obs = observer
    adapter._own_device_id = "panel_1"
    mqtt = FakeMqtt()
    bridge = Bridge(adapter, mqtt, "living_room")
    await bridge.reconcile()
    assert bridge.is_privacy_off() is previously_allowed
    callback, coalesce, want = adapter._change_cbs[0]
    decisions: list[bool] = []
    camera = HeldCamera()
    camera.release.set()
    controller = AmbilightController(mqtt, "living_room", camera, privacy_off=bridge.is_privacy_off)

    async def observe_each_callback(device: BrilliantDevice) -> None:
        await callback(device)
        decisions.append(bridge.is_privacy_off())
        # Exercise the admission window after each per-peripheral MQTT await.
        await controller.trigger_snapshot()

    adapter._change_cbs[0] = (observe_each_callback, coalesce, want)
    try:
        adapter._dispatch_raw_device(ambiguous)
        await asyncio.gather(*adapter._pending_tasks)
        assert decisions == [False, False]
        assert camera.calls == 0
        assert not bridge.is_privacy_off()
        # Fresh unambiguous notification still restores permission without a poll.
        adapter._dispatch_raw_device(unambiguous)
        await asyncio.gather(*adapter._pending_tasks)
        assert decisions == [False, False, True]
        assert camera.calls == 1
        observer.current = unambiguous
        await bridge.poll_once()
        assert bridge.is_privacy_off()
    finally:
        await controller.shutdown()
        await bridge.shutdown_wired_feedback()


@pytest.mark.parametrize("inventory", ["missing", "ambiguous", "privacy_on", "unknown"])
@pytest.mark.parametrize("check_before_delivery", [False, True])
async def test_real_bus_captured_denial_revokes_before_any_hardware_callback(
    inventory: str,
    check_before_delivery: bool,
) -> None:
    from brilliant_mqtt.bus import RpcBusAdapter
    from tests.fakes import _RawDevice, _RawPeripheral

    def raw_hardware(value: str) -> _RawPeripheral:
        peripheral = _RawPeripheral(value, "privacy_toggle")
        peripheral.peripheral_type = 22
        return peripheral

    valid = _RawDevice("panel_1", {"hardware": raw_hardware("0")})
    first = _RawPeripheral("1")
    first.peripheral_type = 27
    peripherals = {"first": first}
    if inventory == "ambiguous":
        peripherals.update(hardware_a=raw_hardware("0"), hardware_b=raw_hardware("1"))
    elif inventory != "missing":
        peripherals["hardware"] = raw_hardware("1" if inventory == "privacy_on" else "unknown")
    denied = _RawDevice("panel_1", peripherals)

    class Observer:
        current = valid

        async def get_device(self, device_id: str) -> _RawDevice:
            return self.current

    entered = asyncio.Event()
    release = asyncio.Event()

    class HeldMqtt(FakeMqtt):
        async def publish(
            self,
            topic: str,
            payload: str,
            retain: bool = False,
            qos: int = 0,
        ) -> None:
            if topic == "brilliant/living_room/first/state":
                entered.set()
                await release.wait()
            await super().publish(topic, payload, retain=retain, qos=qos)

    observer = Observer()
    adapter = RpcBusAdapter()
    adapter._obs = observer
    adapter._own_device_id = "panel_1"
    mqtt = HeldMqtt()
    bridge = Bridge(adapter, mqtt, "living_room")
    camera = HeldCamera()
    controller = AmbilightController(mqtt, "living_room", camera, privacy_off=bridge.is_privacy_off)
    await bridge.reconcile()
    assert bridge.is_privacy_off()
    # An unrelated device's missing HARDWARE must never revoke this panel's grant.
    adapter._dispatch_raw_device(_RawDevice("ble_mesh", {"other": first}))
    await asyncio.gather(*adapter._pending_tasks)
    assert bridge.is_privacy_off()
    flight = asyncio.create_task(controller.trigger_snapshot())
    await asyncio.wait_for(camera.started.wait(), 1)
    try:
        adapter._dispatch_raw_device(denied)
        if check_before_delivery:
            assert not bridge.is_privacy_off()
        await asyncio.wait_for(entered.wait(), 1)
        # First non-HARDWARE callback is blocked on MQTT; no HARDWARE callback ran.
        assert not bridge.is_privacy_off()
        camera.release.set()
        assert not await flight
        assert not await controller.trigger_snapshot()
        assert camera.calls == 1
        release.set()
        await asyncio.gather(*adapter._pending_tasks)
        assert not bridge.is_privacy_off()
        # Older already captured off evidence cannot cross the new source denial.
        await bridge._on_change(replace(hardware(), capture_provenance=CaptureProvenance(0, 1)))
        assert not bridge.is_privacy_off()
        adapter._dispatch_raw_device(valid)
        await asyncio.gather(*adapter._pending_tasks)
        assert bridge.is_privacy_off()
        assert await controller.trigger_snapshot()
        assert camera.calls == 2
        adapter._advance_capture_generation()
        assert not bridge.is_privacy_off()
        await bridge.poll_once()
        assert bridge.is_privacy_off()
        adapter._shutting_down = True
        assert not bridge.is_privacy_off()
    finally:
        release.set()
        await asyncio.gather(*adapter._pending_tasks, return_exceptions=True)
        await controller.shutdown()
        await asyncio.gather(flight, return_exceptions=True)
        await bridge.shutdown_wired_feedback()
