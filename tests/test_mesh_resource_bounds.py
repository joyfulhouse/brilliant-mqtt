"""Connected operation counts, not live CPU claims, behind blocked state MQTT."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import pytest

from brilliant_mqtt import bus as bus_module
from brilliant_mqtt.bridge import Bridge
from brilliant_mqtt.bus import RpcBusAdapter
from brilliant_mqtt.mesh_ingress import MeshCapture
from brilliant_mqtt.mesh_registry import MeshRegistry
from brilliant_mqtt.mesh_settlement import MeshSettlement
from brilliant_mqtt.mesh_settlement_types import PublicationToken
from brilliant_mqtt.model import BrilliantDevice, Variable
from tests.fakes import (
    FakeClock,
    FakeMqtt,
    _RawDevice,
    _RawPeripheral,
    _RawVariable,
    _StartHarness,
)
from tests.mesh_regression_support import turns


class _BlockedStates(FakeMqtt):
    def __init__(self) -> None:
        super().__init__()
        self.blocked = False
        self.release = asyncio.Event()
        self.attempts = 0

    async def publish(self, topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        if self.blocked and topic.startswith("brilliant/mesh/") and topic.endswith("/state"):
            self.attempts += 1
            await self.release.wait()
        await super().publish(topic, payload, retain=retain, qos=qos)


@dataclass
class _Counts:
    normalizations: int = 0
    variables: int = 0
    captures: int = 0
    fields: int = 0
    renders: int = 0


async def test_forty_load_storm_has_bounded_work_and_no_extra_reads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    count, frames = 40, 100
    clock = FakeClock()
    harness = _StartHarness(monkeypatch)
    adapter = RpcBusAdapter(extra_device_ids=("ble_mesh",), clock=clock)
    mqtt = _BlockedStates()
    bridge = Bridge(
        adapter, mqtt, "mesh", include=lambda device_id: device_id == "ble_mesh", clock=clock
    )
    raw_peripherals: dict[str, _RawPeripheral] = {}
    for index in range(count):
        peripheral = _RawPeripheral("1")
        peripheral.peripheral_type = 27
        peripheral.variables.update({f"irrelevant_{n}": _RawVariable(str(n)) for n in range(29)})
        assert len(peripheral.variables) == 30
        raw_peripherals[f"load-{index}"] = peripheral
    raw = _RawDevice("ble_mesh", raw_peripherals)
    reads: list[str] = []

    async def get_device(device_id: str) -> _RawDevice:
        reads.append(device_id)
        return raw if device_id == "ble_mesh" else _RawDevice(device_id, {})

    counts = _Counts()
    original_normalize = bus_module.normalize_peripheral
    original_variable = Variable
    original_capture = MeshRegistry._capture
    original_render = MeshSettlement.render

    def normalize(device_id: str, peripheral_id: str, raw: Any) -> BrilliantDevice:
        counts.normalizations += 1
        return original_normalize(device_id, peripheral_id, raw)

    def variable(*args: Any, **kwargs: Any) -> Variable:
        counts.variables += 1
        return original_variable(*args, **kwargs)

    def capture(registry: MeshRegistry, event: MeshCapture) -> None:
        counts.captures += 1
        counts.fields += len(event.capture.fields)
        original_capture(registry, event)

    def render(
        owner: MeshSettlement, now: float, *, token: PublicationToken | None = None
    ) -> Mapping[str, object]:
        counts.renders += 1
        return original_render(owner, now, token=token)

    try:
        await adapter.start()
        monkeypatch.setattr(harness.observers[0], "get_device", get_device, raising=False)
        await bridge.reconcile()
        assert reads == ["own-device", "ble_mesh"]
        monkeypatch.setattr(bus_module, "normalize_peripheral", normalize)
        monkeypatch.setattr(bus_module, "Variable", variable)
        monkeypatch.setattr(MeshRegistry, "_capture", capture)
        monkeypatch.setattr(MeshSettlement, "render", render)
        mqtt.blocked = True
        initial_reads = list(reads)
        initial_tasks = set(asyncio.all_tasks())

        async def notify(value: str) -> None:
            deltas = []
            for pid, peripheral in raw_peripherals.items():
                peripheral.variables["on"].value = value
                deltas.append(
                    SimpleNamespace(
                        peripheral_id=pid,
                        deleted=False,
                        modified_variables=[
                            SimpleNamespace(
                                variable_name="on",
                                deletion=False,
                                variable=SimpleNamespace(value=value),
                            )
                        ],
                        deleted_variables=[],
                    )
                )
            await harness.observers[0].handle_notification(
                SimpleNamespace(updated_device=raw, deleted=False, modified_peripherals=deltas)
            )

        await notify("0")
        await turns()
        registry, driver = bridge.mesh_registry, bridge.mesh_driver
        assert registry is not None and driver is not None
        assert mqtt.attempts == count
        for frame in range(frames):
            clock.advance(0.001)
            await notify(str(frame % 2))
        await turns()
        assert counts.captures == count * (frames + 1) == 4040
        assert counts.fields == counts.captures
        assert counts.normalizations == counts.variables == 0
        # This bound covers work while transport is blocked; flushing renders again.
        assert counts.renders <= count * 2
        assert mqtt.attempts == count
        assert len(registry.owners) == count and not registry.retired
        assert len(driver._lanes) == count
        assert len(driver.tasks) <= count * 2
        assert len(set(asyncio.all_tasks()) - initial_tasks) <= count * 3 + 4
        assert reads == initial_reads
        assert not any(device_id == "ble_mesh" for device_id, _pid, _name in adapter._field_eras)
        blocked_renders = counts.renders
        mqtt.release.set()
        await turns(100)
        for owner in registry.owners:
            await driver.flush(owner)
        final = {
            topic: json.loads(payload)
            for topic, payload, _retain in mqtt.published
            if topic.startswith("brilliant/mesh/") and topic.endswith("/state")
        }
        assert len(final) == count
        assert all(
            payload["state"] == "ON" and payload["mesh_write_status"] == "idle"
            for payload in final.values()
        )
        assert reads == initial_reads
        assert mqtt.attempts <= count * 2
        print(
            {
                "loads": count,
                "variables_per_raw_load": 30,
                "frames": frames + 1,
                "captures": counts.captures,
                "scoped_fields": counts.fields,
                "bus_normalizations": counts.normalizations,
                "bus_variable_allocations": counts.variables,
                "blocked_renders": blocked_renders,
                "renders_after_flush": counts.renders,
                "state_attempts": mqtt.attempts,
                "reads": reads,
            }
        )
    finally:
        mqtt.release.set()
        try:
            await bridge.withdraw()
        finally:
            await adapter.shutdown()
