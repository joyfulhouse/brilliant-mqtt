"""Synchronous lifecycle admission through the production adapter ingress."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import cast

import pytest

from brilliant_mqtt.bus import RpcBusAdapter
from brilliant_mqtt.commands import VarSet, translate_command
from brilliant_mqtt.mesh_registry import MeshRegistry
from tests.fakes import FakeClock, _GatedRpcObserver, _RawDevice, _RawPeripheral, _RawVariable
from tests.mesh_regression_support import turns
from tests.test_bus_adapter import _gated_adapter
from tests.test_mesh_bus_issue import bind


@pytest.fixture
async def registry_rig(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[tuple[MeshRegistry, _RawDevice]]:
    observer, adapter = _gated_adapter()
    clock = FakeClock()
    adapter._clock = clock
    peripheral = _RawPeripheral("1")
    peripheral.peripheral_type = 27
    peripheral.variables["intensity"] = _RawVariable("600")
    raw = _RawDevice("ble_mesh", {"load": peripheral})

    async def get_device(device_id: str) -> _RawDevice:
        return raw if device_id == "ble_mesh" else _RawDevice(device_id, {})

    monkeypatch.setattr(observer, "get_device", get_device)
    registry = MeshRegistry(adapter, clock=clock, wall_clock=lambda: 1000 + clock())
    try:
        yield registry, raw
    finally:
        registry.dispose()
        await adapter.shutdown()


async def test_standby_scope_closed_and_acquisition_reduces_before_carrier_return(
    registry_rig: tuple[MeshRegistry, _RawDevice],
) -> None:
    registry, _ = registry_rig
    await cast(RpcBusAdapter, registry.bus).get_all()
    assert registry.current("load") is None
    registry.acquire()
    carriers = await cast(RpcBusAdapter, registry.bus).get_all()
    slot = registry.current("load")
    assert slot is not None
    assert slot.render(0)["state"] == "ON"
    assert slot.materialize(0).is_dimmable
    assert all(carrier.variables == {} for carrier in carriers if carrier.device_id == "ble_mesh")


async def test_old_acquisition_read_cannot_create_owner_after_reacquisition(
    registry_rig: tuple[MeshRegistry, _RawDevice],
) -> None:
    registry, raw = registry_rig
    from brilliant_mqtt.mesh_ingress import MeshCapture
    from brilliant_mqtt.mesh_settlement_types import MeshMetadata
    from brilliant_mqtt.model import DeviceKind, PrimitiveCapture, PrimitiveField

    registry.acquire()
    stale = registry.bus.reserve_mesh_boundary("read")
    registry.close()
    registry.acquire()
    registry.ingest(
        MeshCapture(
            stale,
            MeshMetadata("ble_mesh", "old", "Old", DeviceKind.LIGHT),
            PrimitiveCapture((PrimitiveField("on", "1"),)),
        )
    )
    assert registry.current("old") is None
    assert raw.peripherals is not None
    await cast(RpcBusAdapter, registry.bus).get_all()
    assert registry.current("load") is not None


async def test_removal_retains_only_absence_debt_and_reappearance_revokes_old_owner(
    registry_rig: tuple[MeshRegistry, _RawDevice],
) -> None:
    registry, raw = registry_rig
    registry.acquire()
    await cast(RpcBusAdapter, registry.bus).get_all()
    old = registry.current("load")
    assert old is not None
    raw.peripherals = {}
    await cast(RpcBusAdapter, registry.bus).get_all()
    assert registry.current("load") is None
    assert registry.retired.get("load") is old
    assert old.retired
    assert old.render(0)["state"] is None
    raw.peripherals = {"load": _RawPeripheral("0")}
    raw.peripherals["load"].peripheral_type = 27
    await cast(RpcBusAdapter, registry.bus).get_all()
    new = registry.current("load")
    assert new is not None and new is not old
    assert not old.active
    assert "load" not in registry.retired


async def test_generation_replacement_revokes_before_fresh_read(
    registry_rig: tuple[MeshRegistry, _RawDevice],
) -> None:
    registry, _ = registry_rig
    registry.acquire()
    await cast(RpcBusAdapter, registry.bus).get_all()
    old = registry.current("load")
    assert old is not None
    adapter = cast(RpcBusAdapter, registry.bus)
    adapter._on_proc_reconnect()
    successor = registry.current("load")
    assert successor is not None and successor is not old
    assert not old.active
    assert successor.render(0)["mesh_write_status"] == "unconfirmed"
    assert successor.view.observed == ()


async def test_ingress_never_materializes_or_renders_current_owner(
    registry_rig: tuple[MeshRegistry, _RawDevice],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry, _ = registry_rig
    registry.acquire()
    adapter = cast(RpcBusAdapter, registry.bus)
    await adapter.get_all()
    owner = registry.current("load")
    assert owner is not None

    def forbidden(now: float) -> None:
        raise AssertionError("ingress cannot allocate a full owner view")

    monkeypatch.setattr(owner, "materialize", forbidden)
    before = adapter.capture_sequence
    await adapter.get_all()
    assert adapter.mesh_admission_open
    assert adapter.capture_sequence > before
    assert registry.current("load") is owner


async def test_pre_return_binding_repair_seeds_no_state_and_older_reads_cannot_rebind_or_remove(
    registry_rig: tuple[MeshRegistry, _RawDevice],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry, raw = registry_rig
    adapter = cast(RpcBusAdapter, registry.bus)
    observer = cast(_GatedRpcObserver, adapter._obs)
    registry.acquire()
    await adapter.get_all()
    old = registry.current("load")
    assert old is not None
    ticket = bind(adapter, old)
    native = asyncio.create_task(
        adapter.set_variables("ble_mesh", "load", [VarSet("on", "0")], ticket=ticket)
    )
    await turns()
    assert observer.in_flight == 1
    releases = [asyncio.Event() for _ in range(3)]
    starts = [asyncio.Event() for _ in range(3)]
    changed = _RawPeripheral("1")
    changed.peripheral_type = 27
    changed.variables["intensity"] = _RawVariable("128")
    changed.variables["max_intensity_value"] = _RawVariable("255")
    current = _RawDevice("ble_mesh", {"load": changed})
    snapshots = [raw, _RawDevice("ble_mesh", {}), current]
    count = 0

    async def delayed(device_id: str) -> _RawDevice:
        nonlocal count
        if device_id != "ble_mesh":
            return _RawDevice(device_id, {})
        index = count
        count += 1
        starts[index].set()
        await releases[index].wait()
        return snapshots[index]

    monkeypatch.setattr(observer, "get_device", delayed)
    try:
        tasks = [asyncio.create_task(adapter.get_all())]
        await starts[0].wait()
        tasks.append(asyncio.create_task(adapter.get_all()))
        await starts[1].wait()
        tasks.append(asyncio.create_task(adapter.get_all()))
        await starts[2].wait()
        observer.release.set()
        await native

        assert ticket.returned_at is not None
        releases[2].set()
        await tasks[2]
        replacement = registry.current("load")
        assert replacement is not None and replacement is not old
        device = replacement.materialize(registry.clock())
        assert device.is_dimmable and device.max_intensity == 255
        assert "on" not in device.variables and "intensity" not in device.variables
        assert replacement.view.observed == ()
        assert replacement.view.evidence == ()
        assert replacement.render(registry.clock())["state"] is None
        assert translate_command(device, {"brightness": 128}) == [VarSet("intensity", "128")]
        releases[0].set()
        await tasks[0]
        releases[1].set()
        await tasks[1]
        assert registry.current("load") is replacement
        assert replacement.metadata.binding_max_intensity == 255
        assert replacement.view.observed == ()

        async def fresh(device_id: str) -> _RawDevice:
            return current if device_id == "ble_mesh" else _RawDevice(device_id, {})

        monkeypatch.setattr(observer, "get_device", fresh)
        await adapter.get_all()
        assert registry.current("load") is replacement
        assert replacement.render(registry.clock())["state"] == "ON"
        assert replacement.materialize(registry.clock()).intensity == 128
    finally:
        for release in releases:
            release.set()
        observer.release.set()
        await native


async def test_dispose_removes_registrations_and_prevents_accidental_reacquisition(
    registry_rig: tuple[MeshRegistry, _RawDevice],
) -> None:
    registry, _ = registry_rig
    adapter = cast(RpcBusAdapter, registry.bus)
    registry.acquire()
    await adapter.get_all()
    registry.close()
    registry.acquire()
    await adapter.get_all()
    assert registry.current("load") is not None
    registry.dispose()
    registry.dispose()
    assert adapter._mesh_sinks == []
    assert adapter._mesh_write_guard is None
    with pytest.raises(RuntimeError, match="disposed"):
        registry.acquire()
