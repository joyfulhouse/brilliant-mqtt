"""Registered reconnect hooks exercise registry inheritance of real native work."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass

import pytest

from brilliant_mqtt.bus import RpcBusAdapter
from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.mesh_driver import MeshDriver
from brilliant_mqtt.mesh_registry import MeshRegistry
from tests.fakes import FakeClock, FakeMqtt, _RawDevice, _RawPeripheral, _RawVariable, _StartHarness
from tests.mesh_regression_support import turns
from tests.test_mesh_bus_issue import bind
from tests.test_mesh_bus_reconnect import foreign


@dataclass
class NativeRig:
    harness: _StartHarness
    adapter: RpcBusAdapter
    registry: MeshRegistry
    clock: FakeClock


@pytest.fixture
async def native_registry_rig(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[NativeRig]:
    harness = _StartHarness(monkeypatch, gated_writes=True)
    clock = FakeClock()
    adapter = RpcBusAdapter(extra_device_ids=("ble_mesh",), clock=clock)
    await adapter.start()
    peripheral = _RawPeripheral("1")
    peripheral.peripheral_type = 27
    peripheral.variables["intensity"] = _RawVariable("600")
    raw = _RawDevice("ble_mesh", {"load": peripheral})

    async def get_device(device_id: str) -> _RawDevice:
        return raw if device_id == "ble_mesh" else _RawDevice(device_id, {})

    monkeypatch.setattr(harness.observers[0], "get_device", get_device, raising=False)
    registry = MeshRegistry(adapter, clock=clock, wall_clock=lambda: 1000 + clock())
    try:
        yield NativeRig(harness, adapter, registry, clock)
    finally:
        registry.dispose()
        harness.write_release.set()
        await adapter.shutdown()


@pytest.mark.parametrize("on_thread", [False, True], ids=["loop", "foreign"])
async def test_registered_hook_successor_inherits_only_exact_physical_hold(
    native_registry_rig: NativeRig,
    on_thread: bool,
) -> None:
    rig = native_registry_rig
    rig.registry.acquire()
    await rig.adapter.get_all()
    old = rig.registry.current("load")
    assert old is not None
    ticket = bind(rig.adapter, old)
    native = asyncio.create_task(
        rig.adapter.set_variables("ble_mesh", "load", [VarSet("on", "0")], ticket=ticket)
    )
    try:
        await turns()
        assert rig.harness.writes_in_flight == 1
        assert rig.adapter.outstanding_mesh_ticket("load") is ticket
        assert ticket.issued_at is not None
        hook = rig.harness.procs[0].reconnect_cbs[0]
        foreign(hook) if on_thread else hook()
        # Reservation may apply a foreign hook's fanout inline. Revalidate the
        # captured owner after it rather than issuing an event to that object.
        assert rig.registry.reserve(old, "old_reference") is None
        successor = rig.registry.current("load")
        assert successor is not None and successor is not old
        assert not old.active
        assert successor.view.completion_hold is not None
        assert successor.view.completion_hold.ticket is ticket
        assert successor.view.targets == ()
        assert successor.view.evidence == ()
        assert successor.view.observed == ()
        assert successor.view.receipt is None
        assert successor.view.return_boundary is None
        assert successor.view.deadline is None
        await rig.adapter.get_all()
        assert successor.render(rig.clock())["state"] is None
        rig.clock.advance(1)
        rig.harness.write_release.set()
        await native
        assert ticket.returned_at is not None
        assert ticket.returned_at.incarnation == rig.adapter.capture_source
        assert successor.view.completion_hold is None
        assert successor.view.retirement_sequence == ticket.returned_at.sequence
        assert rig.adapter.mesh_creation_floor == ticket.returned_at.sequence
        assert successor.render(rig.clock())["state"] is None
        assert successor.view.receipt is None and successor.view.outcome is None
        assert rig.adapter.outstanding_mesh_ticket("load") is None
        assert ticket._on_settled is None
        await rig.adapter.get_all()
        assert successor.render(rig.clock())["state"] == "ON"
        assert rig.harness.max_writes_in_flight == 1
    finally:
        rig.harness.write_release.set()
        await native


async def test_registered_hooks_before_worker_start_leave_only_current_lane(
    native_registry_rig: NativeRig,
) -> None:
    rig = native_registry_rig
    driver = MeshDriver(rig.registry, FakeMqtt(), panel="mesh")
    try:
        rig.registry.acquire()
        await rig.adapter.get_all()
        hook = rig.harness.procs[0].reconnect_cbs[0]
        for _ in range(6):
            hook()
        await turns()
        current = rig.registry.current("load")
        assert current is not None
        assert tuple(driver._lanes) == (current,)
        assert current.generation == rig.adapter.capture_generation
    finally:
        rig.registry.close()
        await driver.shutdown()


async def test_withdrawal_releases_inherited_listener_but_real_native_work_retains_lock(
    native_registry_rig: NativeRig,
) -> None:
    rig = native_registry_rig
    rig.registry.acquire()
    await rig.adapter.get_all()
    owner = rig.registry.current("load")
    assert owner is not None
    ticket = bind(rig.adapter, owner)
    native = asyncio.create_task(
        rig.adapter.set_variables("ble_mesh", "load", [VarSet("on", "0")], ticket=ticket)
    )
    try:
        await turns()
        rig.harness.procs[0].reconnect_cbs[0]()
        successor = rig.registry.current("load")
        assert successor is not None and successor is not owner
        assert ticket._on_settled is not None
        rig.registry.close()
        assert ticket._on_settled is None
        assert not successor.active
        assert ticket.returned_at is None and not ticket.settled
        assert rig.harness.writes_in_flight == 1
        assert rig.adapter.outstanding_mesh_ticket("load") is ticket
        rig.harness.write_release.set()
        await native
        assert ticket.returned_at is not None
        assert rig.adapter.mesh_creation_floor == ticket.returned_at.sequence
        assert rig.registry.owners == ()
        assert not rig.registry.retired
    finally:
        rig.harness.write_release.set()
        await native
