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
        if device_id != "ble_mesh":
            return _RawDevice(device_id, {})
        self.started.set()
        await self.release.wait()
        if isinstance(self.raw, BaseException):
            raise self.raw
        return self.raw

    async def get_peripheral(self, device_id: str, peripheral_id: str) -> _RawPeripheral | None:
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
            enabled = event.peripheral_ids is not None and event.count <= 256

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
    old_generation = adapter.capture_generation
    adapter._on_proc_reconnect()
    clock.advance(5)
    native.release.set()
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


async def test_capture_sink_failure_closes_mesh_admission_but_keeps_wired_service() -> None:
    from brilliant_mqtt.commands import VarSet
    from brilliant_mqtt.write_admission import AdmissionTicket, WriteCancelled
    from tests.test_write_admission import _Harness

    harness = _Harness()

    def fail(_event: object) -> None:
        raise RuntimeError("sink")

    harness.adapter.register_mesh_sink(fail, want_fields=lambda _pid: frozenset({"on"}))
    await notify(
        harness.adapter, _RawDevice("ble_mesh", {"p": _RawPeripheral()}), [delta("p", "on")]
    )
    assert not harness.adapter.mesh_admission_open
    assert harness.adapter.consume_mesh_fatal()
    ticket = AdmissionTicket()
    with pytest.raises(WriteCancelled):
        await harness.adapter.set_variables("ble_mesh", "p", [VarSet("on", "0")], ticket=ticket)
    assert ticket.settled
    assert ticket.returned_at is None
    assert harness.observer.calls == []
    harness.observer.release_all = True
    assert await harness.adapter.set_variables("own-device", "p", [VarSet("on", "0")]) == "'ok'"
    await harness.adapter.shutdown()


async def test_mesh_reads_pushes_and_writes_never_scan_or_allocate_wired_eras() -> None:
    from collections.abc import Iterator

    from brilliant_mqtt.commands import VarSet

    class NoMeshAccess(dict[tuple[str, str, str], int]):
        def __iter__(self) -> Iterator[tuple[str, str, str]]:
            raise AssertionError("mesh era scan")

        def __setitem__(self, key: tuple[str, str, str], value: int) -> None:
            raise AssertionError("mesh era allocation")

    adapter, native, _ = adapter_for(_RawDevice("ble_mesh", {"p": _RawPeripheral()}))
    adapter._field_eras = NoMeshAccess({("own", "wired", "on"): 4})
    captured: list[Any] = []
    adapter.register_mesh_sink(captured.append, want_fields=lambda _pid: frozenset({"on"}))
    await adapter.get_all()
    assert native.reads == ["own", "ble_mesh"]
    await notify(adapter, _RawDevice("ble_mesh", {"p": _RawPeripheral()}), [delta("p", "on")])
    from tests.fakes import _GatedRpcObserver

    writer = _GatedRpcObserver()
    writer.release.set()
    adapter._obs = writer
    assert await adapter.set_variables("ble_mesh", "p", [VarSet("on", "0")]) == "'ok'"
    assert len(adapter._field_eras) == 1
    await adapter.shutdown()


async def test_registered_mesh_reads_and_opted_callbacks_use_metadata_carriers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from brilliant_mqtt.model import BrilliantDevice

    raw = _RawDevice("ble_mesh", {"p": _RawPeripheral()})
    adapter, _, _ = adapter_for(raw)
    events: list[object] = []
    delivered: list[BrilliantDevice] = []
    adapter.register_mesh_sink(events.append, want_fields=lambda _pid: frozenset({"on"}))

    async def callback(device: BrilliantDevice) -> None:
        delivered.append(device)

    adapter.on_change(callback, mesh_metadata_only=True)

    def forbid(*args: object, **kwargs: object) -> Any:
        raise AssertionError("mesh ingress normalized variables")

    monkeypatch.setattr(bus_mod, "normalize_peripheral", forbid)
    monkeypatch.setattr(bus_mod, "Variable", forbid)
    result = await adapter.get_all()
    assert len(result) == 1
    assert result[0].variables == {}
    assert result[0].peripheral_id == "p"
    await notify(adapter, raw, [delta("p", "on")])
    await asyncio.gather(*adapter._pending_tasks)
    assert len(delivered) == 1
    assert delivered[0].variables == {}
    await adapter.shutdown()


async def test_registered_mesh_preserves_explicit_legacy_callback_variables() -> None:
    from brilliant_mqtt.model import BrilliantDevice

    peripheral = _RawPeripheral()
    peripheral.variables["power"] = _RawVariable("42")
    raw = _RawDevice("ble_mesh", {"p": peripheral})
    adapter, _, _ = adapter_for(raw)
    adapter.register_mesh_sink(lambda _event: None, want_fields=lambda _pid: frozenset({"on"}))
    delivered: list[BrilliantDevice] = []

    async def callback(device: BrilliantDevice) -> None:
        delivered.append(device)

    adapter.on_change(callback)
    await notify(adapter, raw, [delta("p", "on")])
    await asyncio.gather(*adapter._pending_tasks)
    assert delivered[0].variables["on"].value == "1"
    assert delivered[0].variables["power"].value == "42"
    await adapter.shutdown()


async def test_resident_slider_scoped_read_retains_real_config_without_load_owner() -> None:
    class ScopedNative(Native):
        async def get_peripheral(self, device_id: str, peripheral_id: str) -> _RawPeripheral:
            return _RawPeripheral("encoded-config", "slider_config")

    adapter, _, _ = adapter_for(None)
    adapter._obs = ScopedNative(None)
    adapter.register_mesh_sink(lambda _event: None, want_fields=lambda _pid: None)
    device = await adapter.get_peripheral("ble_mesh", "switch_config:mac")
    assert device is not None
    assert device.variables["slider_config"].value == "encoded-config"
    await adapter.shutdown()


async def test_unrequested_delta_fields_do_not_change_admitted_target_reliability() -> None:
    from brilliant_mqtt.mesh_ingress import MeshCapture

    peripheral = _RawPeripheral()
    peripheral.variables["power"] = _RawVariable("42")
    adapter, _, _ = adapter_for(None)
    seen: list[object] = []
    adapter.register_mesh_sink(seen.append, want_fields=lambda _pid: frozenset({"on"}))
    await notify(adapter, _RawDevice("ble_mesh", {"p": peripheral}), [delta("p", "on", "power")])
    assert isinstance(seen[0], MeshCapture)
    assert seen[0].capture.reliable
    assert seen[0].capture.reported == frozenset({"on"})
    await adapter.shutdown()


async def test_read_timeout_has_no_inventory_or_absence_contribution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter, native, _ = adapter_for(_RawDevice("ble_mesh", {}))
    native.release.clear()
    seen: list[object] = []
    adapter.register_mesh_sink(seen.append, want_fields=lambda _pid: None)
    monkeypatch.setattr(bus_mod, "_READ_DEADLINE_S", 0.01)
    with pytest.raises(asyncio.TimeoutError):
        await adapter.get_all()
    assert seen == []
    assert native.reads == ["own", "ble_mesh"]
    await adapter.shutdown()


async def test_mesh_registration_cleanup_and_churn_leave_no_per_id_adapter_state() -> None:
    from brilliant_mqtt.commands import VarSet
    from tests.fakes import _GatedRpcObserver

    observer = _GatedRpcObserver()
    observer.release.set()
    adapter = RpcBusAdapter()
    adapter._obs = observer
    adapter._own_device_id = "own"
    capture_count = 0

    def capture(_event: object) -> None:
        nonlocal capture_count
        capture_count += 1

    remove = adapter.register_mesh_sink(capture, want_fields=lambda _pid: frozenset({"on"}))
    for index in range(40):
        pid = str(index)
        await notify(adapter, _RawDevice("ble_mesh", {pid: _RawPeripheral()}), [delta(pid, "on")])
        await adapter.set_variables("ble_mesh", pid, [VarSet("on", "0")])
        await notify(adapter, _RawDevice("ble_mesh", {}), [delta(pid, deleted=True)])
    assert capture_count == 80
    assert adapter._field_eras == {}
    assert adapter._write_admissions == {}
    assert adapter._write_serving == {}
    assert not adapter._write_tasks
    assert adapter._write_waiters == {"ble_mesh": []}
    assert list(adapter._write_locks) == ["ble_mesh"]
    assert not adapter._pending_tasks
    assert not adapter._pending_pushes
    remove()
    remove()
    assert adapter._mesh_sinks == []
    adapter.register_mesh_sink(capture, want_fields=lambda _pid: None)
    adapter.register_mesh_write_guard(lambda _ticket, _pid: True)
    await adapter.shutdown()
    assert adapter._mesh_sinks == []
    assert adapter._mesh_write_guard is None


@pytest.mark.parametrize("newer_on", [False, True], ids=["disjoint", "overlap"])
async def test_actual_read_push_overlap_merges_owner_fields_independently(newer_on: bool) -> None:
    from brilliant_mqtt.commands import VarSet
    from brilliant_mqtt.mesh_ingress import MeshCapture
    from brilliant_mqtt.mesh_settlement_types import Observe
    from tests.test_mesh_bus_issue import bind, envelope
    from tests.test_mesh_bus_reconnect import owner_for

    class WritingNative(Native):
        async def request_set_variables_in_peripheral(
            self, peripheral_id: str, values: dict[str, str], *, device_id: str
        ) -> str:
            return "ok"

    old = _RawPeripheral("1")
    old.variables["power"] = _RawVariable("0")
    adapter, _, clock = adapter_for(None)
    native = WritingNative(_RawDevice("ble_mesh", {"p": old}))
    adapter._obs = native
    owner = owner_for(adapter)
    ticket = bind(adapter, owner)

    def ingress(event: object) -> None:
        if isinstance(event, MeshCapture):
            owner.dispatch(Observe(envelope(owner, event.provenance), event.capture))

    adapter.register_mesh_sink(ingress, want_fields=lambda _pid: frozenset({"on", "power"}))
    await adapter.set_variables("ble_mesh", "p", [VarSet("on", "0")], ticket=ticket)
    clock.advance(10)
    native.release.clear()
    read = asyncio.create_task(adapter.get_all())
    await native.started.wait()
    current = _RawPeripheral("0")
    current.variables["power"] = _RawVariable("200")
    clock.advance(10)
    await notify(
        adapter,
        _RawDevice("ble_mesh", {"p": current}),
        [delta("p", *(("on", "power") if newer_on else ("power",)))],
    )
    clock.advance(10)
    native.release.set()
    await read
    rendered = owner.render(clock())
    assert rendered["power"] == 20
    if newer_on:
        assert owner.view.status == "pending"
        assert rendered["state"] is None
    else:
        assert owner.view.status == "contradicted"
        assert rendered["state"] == "ON"
    await adapter.shutdown()


async def test_metadata_only_field_scope_does_not_infer_target_absence() -> None:
    from brilliant_mqtt.mesh_ingress import MeshCapture

    adapter, _, _ = adapter_for(_RawDevice("ble_mesh", {"p": _RawPeripheral()}))
    seen: list[object] = []
    adapter.register_mesh_sink(seen.append, want_fields=lambda _pid: frozenset())
    await adapter.get_all()
    capture = next(event for event in seen if isinstance(event, MeshCapture))
    assert capture.capture.fields == ()
    assert not capture.capture.complete
    assert not capture.capture.reliable
    await adapter.shutdown()


@pytest.mark.parametrize("shape", ["capture", "peripheral_deleted", "device_deleted", "empty"])
async def test_fully_closed_mesh_push_allocates_no_boundary_or_clock(shape: str) -> None:
    clock_reads = 0
    scope_calls = {"fields": 0, "control": 0, "legacy": 0}

    def clock() -> float:
        nonlocal clock_reads
        clock_reads += 1
        return 0

    def fields(_pid: str) -> None:
        scope_calls["fields"] += 1
        return None

    def control() -> bool:
        scope_calls["control"] += 1
        return False

    def legacy(_device: str) -> bool:
        scope_calls["legacy"] += 1
        return False

    adapter = RpcBusAdapter(clock=clock)
    adapter._own_device_id = "own"
    events: list[object] = []
    adapter.register_mesh_sink(events.append, want_fields=fields, want_inventory=control)

    async def callback(_device: object) -> None:
        raise AssertionError("closed legacy callback was delivered")

    adapter.on_change(callback, want_device=legacy)
    raw = _RawDevice("ble_mesh", {"p": _RawPeripheral()} if shape == "capture" else {})
    modified = [delta("p", "on")] if shape == "capture" else []
    if shape == "peripheral_deleted":
        modified = [delta("p", deleted=True)]
    before_clock, before_sequence = clock_reads, adapter.capture_sequence
    await notify(adapter, raw, modified, deleted=shape == "device_deleted")
    assert events == []
    assert not adapter._pending_tasks
    assert not adapter._pending_pushes
    assert adapter.capture_sequence == before_sequence
    assert clock_reads == before_clock
    assert scope_calls == {
        "fields": int(shape == "capture"),
        "control": int(shape in {"peripheral_deleted", "device_deleted"}),
        "legacy": 1,
    }
    await adapter.shutdown()


async def test_admitted_mesh_push_reuses_one_lazy_boundary_before_legacy_fanout() -> None:
    from brilliant_mqtt.mesh_ingress import MeshCapture, MeshPeripheralAbsent
    from brilliant_mqtt.model import BrilliantDevice

    clock_reads = 0
    scope_calls = {"fields": 0, "control": 0, "legacy": 0}

    def clock() -> float:
        nonlocal clock_reads
        clock_reads += 1
        return 7

    def fields(_pid: str) -> frozenset[str]:
        scope_calls["fields"] += 1
        return frozenset({"on"})

    def control() -> bool:
        scope_calls["control"] += 1
        return True

    def legacy(_device: str) -> bool:
        scope_calls["legacy"] += 1
        return True

    adapter = RpcBusAdapter(clock=clock)
    adapter._own_device_id = "own"
    events: list[object] = []
    delivered: list[BrilliantDevice] = []
    adapter.register_mesh_sink(events.append, want_fields=fields, want_inventory=control)

    async def callback(device: BrilliantDevice) -> None:
        assert len(events) == 3
        delivered.append(device)

    adapter.on_change(callback, want_device=legacy)
    await notify(
        adapter,
        _RawDevice("ble_mesh", {"present": _RawPeripheral()}),
        [delta("absent-a", deleted=True), delta("present", "on"), delta("absent-b", deleted=True)],
    )
    assert [type(event) for event in events] == [
        MeshPeripheralAbsent,
        MeshCapture,
        MeshPeripheralAbsent,
    ]
    first, capture, last = events
    assert isinstance(first, MeshPeripheralAbsent)
    assert isinstance(capture, MeshCapture)
    assert isinstance(last, MeshPeripheralAbsent)
    assert first.provenance is capture.provenance is last.provenance
    assert first.provenance.sequence == adapter.capture_sequence == 1
    assert clock_reads == 1
    assert scope_calls == {"fields": 1, "control": 2, "legacy": 1}
    assert delivered == []
    await asyncio.gather(*adapter._pending_tasks)
    assert len(delivered) == 1
    assert delivered[0].variables["on"].value == "1"
    await adapter.shutdown()


async def test_generation_flush_precedes_replacement_scope_decision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from brilliant_mqtt.mesh_ingress import MeshGeneration
    from tests.fakes import _StartHarness
    from tests.test_mesh_bus_reconnect import foreign

    native = _StartHarness(monkeypatch)
    adapter = RpcBusAdapter()
    await adapter.start()
    old_events: list[object] = []
    new_events: list[object] = []
    field_calls = 0
    new_field_calls = 0

    def fields(_pid: str) -> frozenset[str]:
        nonlocal field_calls
        field_calls += 1
        return frozenset({"on"})

    def new_fields(_pid: str) -> None:
        nonlocal new_field_calls
        new_field_calls += 1

    def old_sink(event: object) -> None:
        old_events.append(event)
        if isinstance(event, MeshGeneration):
            remove_old()
            adapter.register_mesh_sink(new_events.append, want_fields=new_fields)

    remove_old = adapter.register_mesh_sink(old_sink, want_fields=fields)
    foreign(native.procs[0].reconnect_cbs[0])
    await notify(adapter, _RawDevice("ble_mesh", {"p": _RawPeripheral()}), [delta("p", "on")])
    assert len(old_events) == 1 and isinstance(old_events[0], MeshGeneration)
    assert new_events == []
    assert field_calls == 0
    assert new_field_calls == 1
    assert not adapter._pending_pushes
    await adapter.shutdown()


@pytest.mark.parametrize("shape", ["capture", "peripheral_deleted", "device_deleted"])
async def test_pending_generation_closes_retained_mesh_scope_before_push_decisions(
    monkeypatch: pytest.MonkeyPatch,
    shape: str,
) -> None:
    from brilliant_mqtt.mesh_ingress import MeshGeneration
    from tests.fakes import _StartHarness
    from tests.test_mesh_bus_reconnect import foreign

    native = _StartHarness(monkeypatch)
    adapter = RpcBusAdapter()
    await adapter.start()
    enabled = True
    events: list[object] = []
    calls = {"fields": 0, "control": 0}
    snapshots = 0
    legacy_decisions: list[bool] = []
    original_snapshot = bus_mod._snapshot_peripheral

    def snapshot(raw: object, *args: Any, **kwargs: Any) -> Any:
        nonlocal snapshots
        snapshots += 1
        return original_snapshot(raw, *args, **kwargs)

    monkeypatch.setattr(bus_mod, "_snapshot_peripheral", snapshot)

    def fields(_pid: str) -> frozenset[str] | None:
        calls["fields"] += 1
        return frozenset({"on"}) if enabled else None

    def control() -> bool:
        calls["control"] += 1
        return enabled

    def sink(event: object) -> None:
        nonlocal enabled
        events.append(event)
        if isinstance(event, MeshGeneration):
            enabled = False

    def legacy(_device: str) -> bool:
        legacy_decisions.append(enabled)
        return enabled

    async def callback(_device: object) -> None:
        raise AssertionError("generation-closed legacy scope was delivered")

    adapter.register_mesh_sink(sink, want_fields=fields, want_inventory=control)
    adapter.on_change(callback, want_device=legacy)
    foreign(native.procs[0].reconnect_cbs[0])
    await notify(
        adapter,
        _RawDevice("ble_mesh", {"p": _RawPeripheral()} if shape == "capture" else {}),
        [delta("p", "on")] if shape == "capture" else [delta("p", deleted=True)],
        deleted=shape == "device_deleted",
    )
    try:
        assert not enabled
        assert len(events) == 1 and isinstance(events[0], MeshGeneration)
        assert snapshots == 0
        assert legacy_decisions == [False]
        assert calls == {"fields": int(shape == "capture"), "control": 1 + int(shape != "capture")}
        assert not adapter._pending_pushes
    finally:
        await adapter.shutdown()


@pytest.mark.parametrize("shape", ["capture", "peripheral_deleted", "device_deleted"])
async def test_hook_after_push_generation_snapshot_cannot_stamp_stale_scope_current(
    monkeypatch: pytest.MonkeyPatch,
    shape: str,
) -> None:
    from brilliant_mqtt.mesh_ingress import MeshCapture, MeshInventory, MeshPeripheralAbsent
    from tests.fakes import _StartHarness
    from tests.test_mesh_bus_reconnect import foreign

    native = _StartHarness(monkeypatch)
    adapter = RpcBusAdapter()
    await adapter.start()
    generation = adapter.capture_generation
    fired = False
    decisions = 0
    events: list[object] = []

    def racing_decision() -> bool:
        nonlocal fired, decisions
        decisions += 1
        if not fired:
            fired = True
            foreign(native.procs[0].reconnect_cbs[0])
        return True

    def fields(_pid: str) -> frozenset[str] | None:
        return frozenset({"on"}) if racing_decision() else None

    adapter.register_mesh_sink(
        events.append,
        want_fields=fields if shape == "capture" else lambda _pid: None,
        want_inventory=(lambda: True) if shape == "capture" else racing_decision,
    )
    await notify(
        adapter,
        _RawDevice("ble_mesh", {"p": _RawPeripheral()} if shape == "capture" else {}),
        [delta("p", "on")] if shape == "capture" else [delta("p", deleted=True)],
        deleted=shape == "device_deleted",
    )
    try:
        assert fired and decisions == 1
        assert adapter.capture_generation == generation + 1
        assert len(events) == 1
        event = events[0]
        assert isinstance(event, (MeshCapture, MeshInventory, MeshPeripheralAbsent))
        assert event.provenance.source_generation == generation
        assert not adapter._pending_pushes
    finally:
        await adapter.shutdown()
