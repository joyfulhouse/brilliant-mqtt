"""Real observer/read boundaries feed the synchronous scoped mesh seam."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

from brilliant_mqtt import bus as bus_mod
from brilliant_mqtt.bus import RpcBusAdapter
from tests.fakes import FakeClock, _RawDevice, _RawPeripheral, _RawVariable


class Native:
    def __init__(self, raw: object) -> None:
        self.raw = raw
        self.reads: list[str] = []
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.release.set()

    async def get_device(self, device_id: str) -> object:
        self.reads.append(device_id)
        self.started.set()
        await self.release.wait()
        if device_id == "ble_mesh" and isinstance(self.raw, BaseException):
            raise self.raw
        return self.raw if device_id == "ble_mesh" else _RawDevice(device_id, {})

    async def get_peripheral(self, device_id: str, peripheral_id: str) -> None:
        return None

    async def shutdown(self) -> None:
        pass


def adapter_for(raw: object) -> tuple[RpcBusAdapter, Native, FakeClock]:
    clock = FakeClock()
    adapter = RpcBusAdapter(extra_device_ids=("ble_mesh",), clock=clock)
    native = Native(raw)
    adapter._obs = native
    adapter._own_device_id = "own"
    return adapter, native, clock


class ObserverBase:
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        pass


async def notify(
    adapter: RpcBusAdapter, raw: _RawDevice, modified: object, *, deleted: bool = False
) -> None:
    observer_cls = bus_mod._make_observer_class(ObserverBase)
    observer = observer_cls(asyncio.get_running_loop(), adapter._dispatch_raw_device, lambda: None)
    await observer.handle_notification(
        SimpleNamespace(updated_device=raw, modified_peripherals=modified, deleted=deleted)
    )


def delta(pid: str, *names: str, removed: tuple[str, ...] = (), deleted: bool = False) -> object:
    return SimpleNamespace(
        peripheral_id=pid,
        deleted=deleted,
        modified_variables=[
            SimpleNamespace(
                variable_name=name, deletion=False, variable=SimpleNamespace(value="UNTRUSTED")
            )
            for name in names
        ],
        deleted_variables=[SimpleNamespace(variable_name=name, deletion=True) for name in removed],
    )


@pytest.mark.parametrize("reported", ["on", "power"])
async def test_observer_mask_is_frozen_and_values_come_from_mirror(reported: str) -> None:
    from brilliant_mqtt.mesh_ingress import MeshCapture

    peripheral = _RawPeripheral("0")
    peripheral.variables["power"] = _RawVariable("42")
    raw = _RawDevice("ble_mesh", {"p": peripheral})
    adapter, _, clock = adapter_for(raw)
    seen: list[Any] = []
    remove = adapter.register_mesh_sink(
        seen.append, want_fields=lambda _pid: frozenset({"on", "power"})
    )
    clock.advance(12)
    await notify(adapter, raw, [delta("p", reported)])
    event = seen[0]
    assert isinstance(event, MeshCapture)
    assert event.capture.reported == frozenset({reported})
    assert dict((field.name, field.value) for field in event.capture.fields) == {
        "on": "0",
        "power": "42",
    }
    assert event.provenance.captured_at == 12
    assert event.provenance.origin == "push"
    assert event.provenance.incarnation == adapter.capture_source
    peripheral.variables["on"].value = "1"
    assert {field.name: field.value for field in event.capture.fields}["on"] == "0"
    remove()
    await notify(adapter, raw, [delta("p", "on")])
    assert len(seen) == 1
    await adapter.shutdown()


async def test_observer_explicit_deletions_and_missing_mask_are_distinct() -> None:
    from brilliant_mqtt.mesh_ingress import MeshCapture, MeshInventory, MeshPeripheralAbsent

    raw = _RawDevice("ble_mesh", {"p": _RawPeripheral("0")})
    adapter, _, _ = adapter_for(raw)
    seen: list[Any] = []
    adapter.register_mesh_sink(seen.append, want_fields=lambda _pid: frozenset({"on"}))
    await notify(adapter, raw, None)
    assert isinstance(seen[-1], MeshCapture)
    assert not seen[-1].capture.reliable
    assert not seen[-1].capture.reported
    assert raw.peripherals is not None
    raw.peripherals["p"].variables.clear()
    await notify(adapter, raw, [delta("p", removed=("on",))])
    assert seen[-1].capture.removed == frozenset({"on"})
    raw.peripherals.clear()
    await notify(adapter, raw, [delta("p", deleted=True)])
    assert isinstance(seen[-1], MeshPeripheralAbsent)
    await notify(adapter, raw, [], deleted=True)
    assert isinstance(seen[-1], MeshInventory)
    assert seen[-1].peripheral_ids == frozenset()
    await adapter.shutdown()


@pytest.mark.parametrize("shape", ["empty", "none", "partial", "exception"])
async def test_get_all_native_inventory_shapes(shape: str) -> None:
    from brilliant_mqtt.mesh_ingress import MeshInventory, MeshScopeUnknown

    raw = {
        "empty": _RawDevice("ble_mesh", {}),
        "none": None,
        "partial": _RawDevice("ble_mesh", None),
        "exception": RuntimeError("read"),
    }[shape]
    adapter, native, _ = adapter_for(raw)
    seen: list[Any] = []
    adapter.register_mesh_sink(seen.append, want_fields=lambda _pid: None)
    if shape == "exception":
        with pytest.raises(RuntimeError):
            await adapter.get_all()
        assert seen == []
    else:
        assert await adapter.get_all() == []
        assert len(seen) == 1
        if shape == "empty":
            assert isinstance(seen[0], MeshInventory)
            assert seen[0].peripheral_ids == frozenset()
        else:
            assert isinstance(seen[0], MeshScopeUnknown)
    assert native.reads == ["own", "ble_mesh"]
    await adapter.shutdown()


async def test_inventory_preflight_closes_fields_before_extraction_and_can_recover() -> None:
    from brilliant_mqtt.mesh_ingress import MeshCapture, MeshInventory

    class Poison:
        @property
        def peripheral_type(self) -> int:
            raise AssertionError("field extraction before capacity preflight")

    adapter, native, _ = adapter_for(_RawDevice("ble_mesh", {}))
    native.raw = SimpleNamespace(peripherals={str(i): Poison() for i in range(257)})
    seen: list[Any] = []
    enabled = False

    def ingress(event: object) -> None:
        nonlocal enabled
        seen.append(event)
        if isinstance(event, MeshInventory):
            enabled = len(event.peripheral_ids) <= 256

    adapter.register_mesh_sink(
        ingress, want_fields=lambda _pid: frozenset({"on"}) if enabled else None
    )
    assert await adapter.get_all() == []
    assert len(seen) == 1
    native.raw = _RawDevice("ble_mesh", {"p": _RawPeripheral()})
    result = await adapter.get_all()
    assert len(result) == 1
    assert isinstance(seen[-1], MeshCapture)
    assert native.reads == ["own", "ble_mesh", "own", "ble_mesh"]
    await adapter.shutdown()


async def test_power_push_only_extracts_scoped_peripheral(monkeypatch: pytest.MonkeyPatch) -> None:
    raw = _RawDevice("ble_mesh", {str(i): _RawPeripheral() for i in range(40)})
    adapter, _, _ = adapter_for(raw)
    seen: list[Any] = []
    adapter.register_mesh_sink(seen.append, want_fields=lambda _pid: frozenset({"on"}))
    snapshots: list[object] = []
    original = bus_mod._snapshot_peripheral

    def snapshot(raw: object, *args: Any, **kwargs: Any) -> Any:
        snapshots.append(raw)
        return original(raw, *args, **kwargs)

    monkeypatch.setattr(bus_mod, "_snapshot_peripheral", snapshot)
    await notify(adapter, raw, [delta("0", "on")])
    assert len(snapshots) == 1
    assert len(seen) == 1
    assert adapter._field_eras == {}
    await adapter.shutdown()


async def test_read_start_provenance_survives_reconnect_and_scoped_absence() -> None:
    from brilliant_mqtt.mesh_ingress import MeshCapture, MeshPeripheralAbsent

    adapter, native, clock = adapter_for(_RawDevice("ble_mesh", {"p": _RawPeripheral()}))
    seen: list[Any] = []
    adapter.register_mesh_sink(seen.append, want_fields=lambda _pid: frozenset({"on"}))
    native.release.clear()
    read = asyncio.create_task(adapter.get_all())
    await native.started.wait()
    native.release.set()
    # The first read is own; let it finish and reserve the mesh read boundary.
    for _ in range(4):
        await asyncio.sleep(0)
    old_generation = adapter.capture_generation
    adapter._on_proc_reconnect()
    clock.advance(5)
    await read
    captures = [event for event in seen if isinstance(event, MeshCapture)]
    assert captures[0].provenance.source_generation == old_generation
    assert captures[0].provenance.captured_at == 0
    assert captures[0].provenance.completed_at == 5
    previous = adapter.capture_sequence
    await adapter.get_peripheral("ble_mesh", "missing")
    assert isinstance(seen[-1], MeshPeripheralAbsent)
    assert seen[-1].provenance.sequence > previous
    await adapter.shutdown()
