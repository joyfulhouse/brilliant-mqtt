"""Capacity stays in-session; actual fatal mesh effects use coordinator teardown."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from brilliant_mqtt import __main__ as main_mod
from brilliant_mqtt import mesh_driver
from brilliant_mqtt.bridge import Bridge
from brilliant_mqtt.discovery import command_topic
from brilliant_mqtt.mesh_driver import MeshDriver
from brilliant_mqtt.mesh_leader import MeshLeader
from brilliant_mqtt.mesh_registry import MeshRegistry
from brilliant_mqtt.virtual_sliders import VirtualSliderController, VirtualSliderSpec
from tests.fakes import FakeClock, FakeMqtt, FakeSleeper, _RawDevice, _RawPeripheral
from tests.mesh_regression_support import turns
from tests.test_bus_adapter import _adapter_for
from tests.test_main import _hot_poll_settings, _real_bus_session
from tests.test_mesh_capacity_routing import CountOnlyInventory
from tests.test_mesh_slider_admission import SliderObserver
from tests.test_virtual_sliders import REAL_PANEL_SLIDER_CONFIG_0_B64 as ORIGINAL
from tests.test_virtual_sliders import FakeVirtualPeripheralHost


async def test_noncooperative_publisher_fatal_rebuilds_with_healthy_transports(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(mesh_driver, "_SHUTDOWN_WORKER_SETTLE_S", 0)
    harness, adapter, observer = _real_bus_session(monkeypatch)
    peripheral = _RawPeripheral("1")
    peripheral.peripheral_type = 27

    async def get_device(device_id: str) -> _RawDevice:
        return _RawDevice(device_id, {"load": peripheral} if device_id == "ble_mesh" else {})

    monkeypatch.setattr(observer, "get_device", get_device)
    adapter._extra_device_ids = ("ble_mesh",)
    registry = MeshRegistry(adapter, clock=lambda: 0, wall_clock=lambda: 1000)
    release, started = asyncio.Event(), asyncio.Event()
    mqtt = FakeMqtt()
    calls = 0

    async def publish(topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        nonlocal calls
        calls += 1
        started.set()
        while not release.is_set():
            try:
                await release.wait()
            except asyncio.CancelledError:
                continue

    monkeypatch.setattr(mqtt, "publish", publish)
    driver = MeshDriver(registry, mqtt, panel="mesh", sleep=FakeSleeper())
    try:
        registry.acquire()
        await adapter.get_all()
        await started.wait()
        registry.close()
        await driver.shutdown()
        assert not adapter.mesh_admission_open
        with pytest.raises(main_mod.MeshFatalError):
            await asyncio.wait_for(
                main_mod._run_session(_hot_poll_settings(mesh=True), None, None), 1
            )
        assert calls == 1
        assert "leader_tick" not in harness.events
        assert "panel_poll" not in harness.events
        assert harness.events[-1] == "mqtt_disconnect"
    finally:
        release.set()
        await turns()
        registry.dispose()
        await driver.shutdown()


async def test_stable_257_inventory_survives_coordinator_ticks_and_wired_commands(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    observer = SliderObserver()
    observer.raw.peripherals = CountOnlyInventory()
    _, adapter = _adapter_for(observer)
    wired = _RawPeripheral("1")
    wired.peripheral_type = 27
    original_read = observer.get_device

    async def get_device(device_id: str) -> _RawDevice:
        if device_id == "own-device":
            observer.reads.append(device_id)
            return _RawDevice(device_id, {"wired-load": wired})
        return await original_read(device_id)

    monkeypatch.setattr(observer, "get_device", get_device)
    residents_ready = asyncio.Event()

    class SessionMqtt(FakeMqtt):
        connects = 0
        disconnects = 0

        async def connect(self) -> None:
            self.connects += 1

        async def disconnect(self) -> None:
            self.disconnects += 1

        def consume_reader_failure(self) -> bool:
            return False

        def consume_transport_overload(self) -> bool:
            return False

        async def publish(
            self, topic: str, payload: str, retain: bool = False, qos: int = 0
        ) -> None:
            await super().publish(topic, payload, retain, qos)
            if topic == "brilliant/office/virtual_slider_configured/state" and json.loads(
                payload
            ).get("binding_ready"):
                residents_ready.set()

    mqtt = SessionMqtt()
    clock = FakeClock()
    ticks = 0
    acquires = 0
    enough_ticks = asyncio.Event()
    stop = asyncio.Event()
    bridges: list[Bridge] = []
    controllers: list[VirtualSliderController] = []
    spec = VirtualSliderSpec.for_ble_switch("configured", "Configured", "configured")

    def bridge_factory(*args: Any, **kwargs: Any) -> Bridge:
        bridge = Bridge(*args, **kwargs)
        bridges.append(bridge)
        return bridge

    def controller_factory(*args: Any, **kwargs: Any) -> VirtualSliderController:
        controller = VirtualSliderController(*args, **kwargs)
        controllers.append(controller)
        return controller

    def leader_factory(
        mqtt: FakeMqtt,
        panel: str,
        priority: int,
        heartbeat_seconds: float,
        *,
        on_acquire: Callable[[], Awaitable[None]],
        on_lose: Callable[[], Awaitable[None]],
    ) -> MeshLeader:
        async def acquired() -> None:
            nonlocal acquires
            acquires += 1
            await on_acquire()

        class CountingLeader(MeshLeader):
            async def tick(self) -> None:
                nonlocal ticks
                if stop.is_set():
                    raise asyncio.CancelledError
                ticks += 1
                clock.advance(2)
                await super().tick()
                if ticks >= 20:
                    enough_ticks.set()

        return CountingLeader(mqtt, panel, priority, 1, acquired, on_lose, clock=clock)

    async def no_start() -> None:
        return

    monkeypatch.setattr(adapter, "start", no_start)
    monkeypatch.setattr(main_mod, "RpcBusAdapter", lambda **kwargs: adapter)
    monkeypatch.setattr(main_mod, "AioMqttAdapter", lambda *args, **kwargs: mqtt)
    monkeypatch.setattr(main_mod, "Bridge", bridge_factory)
    monkeypatch.setattr(main_mod, "MeshLeader", leader_factory)
    monkeypatch.setattr(main_mod, "VirtualSliderController", controller_factory)
    monkeypatch.setattr(main_mod, "HostedVirtualPeripheralAdapter", FakeVirtualPeripheralHost)
    (tmp_path / "virtual-sliders.json").write_text(
        json.dumps(
            {
                "removed": {
                    "orphaned": True,
                    "slider_id": "removed",
                    "original_slider_config": ORIGINAL,
                    "config_device_id": "ble_mesh",
                    "config_peripheral_id": "switch_config:removed",
                    "config_var_name": "slider_config",
                }
            }
        )
    )
    settings = replace(
        _hot_poll_settings(mesh=True),
        retained_topics_file=str(tmp_path / "owned.json"),
        motion_desired_state_dir=str(tmp_path),
        virtual_sliders_json=json.dumps([{"slider_id": "configured", "switch_id": "configured"}]),
    )
    object.__setattr__(settings, "resync_seconds", 0.004)
    session = asyncio.create_task(main_mod._run_session(settings, None, None))
    try:
        await asyncio.wait_for(enough_ticks.wait(), 2)
        await turns()
        assert not session.done()
        registry = bridges[1].mesh_registry
        assert registry is not None and registry.disabled
        assert registry.enabled and not registry.owners and not registry.retired
        assert mqtt.connects == 1 and mqtt.disconnects == 0
        assert acquires == 1
        assert not adapter.consume_mesh_fatal()
        await mqtt.inject(command_topic(settings.panel, "wired-load"), '{"state":"OFF"}')
        assert observer.payloads == [("own-device", "wired-load", {"on": "0"})]
        assert any(topic.endswith("/wired-load/state") for topic, _, _ in mqtt.published)
        assert not any(device_id == "ble_mesh" for device_id, _, _ in observer.payloads)
        assert bridges[1].mesh_driver is not None and not bridges[1].mesh_driver.tasks
        assert len(controllers) == 1
        controller = controllers[0]
        assert not controller.state_for("configured").binding_ready
        assert len(controller._orphaned_bindings) == 1
        assert not any(device_id == "ble_mesh" for device_id, _ in observer.peripheral_reads)

        observer.peripheral_reads.clear()
        observer.raw.peripherals = {
            spec.config_peripheral_id: _RawPeripheral(ORIGINAL, "slider_config"),
            "switch_config:removed": _RawPeripheral(
                spec.virtual_slider_config_b64("own-device"), "slider_config"
            ),
        }

        await asyncio.wait_for(residents_ready.wait(), 1)
        assert controller.state_for("configured").binding_ready
        assert not controller._orphaned_bindings
        assert acquires == 1
        assert mqtt.connects == 1 and mqtt.disconnects == 0
        assert observer.payloads[1:] == [
            ("ble_mesh", "switch_config:removed", {"slider_config": ORIGINAL}),
            (
                "ble_mesh",
                spec.config_peripheral_id,
                {"slider_config": spec.virtual_slider_config_b64("own-device")},
            ),
        ]
        assert observer.peripheral_reads == [
            ("ble_mesh", "switch_config:removed"),
            ("ble_mesh", spec.config_peripheral_id),
            ("ble_mesh", spec.config_peripheral_id),
        ]
    finally:
        stop.set()
        await asyncio.wait_for(asyncio.gather(session, return_exceptions=True), 1)
    assert mqtt.disconnects == 1
