"""Resident slider schedules through the production shared mesh write lock."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path

import pytest

from brilliant_mqtt.bridge import Bridge
from brilliant_mqtt.bus import RpcBusAdapter
from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.mesh_registry import MeshRegistry
from brilliant_mqtt.virtual_sliders import (
    SliderMode,
    VirtualSliderController,
    VirtualSliderSpec,
    restore_all_persisted_slider_bindings,
)
from brilliant_mqtt.write_admission import AdmissionTicket, WriteCancelled
from tests.fakes import FakeMqtt, _GatedRpcObserver, _RawDevice, _RawPeripheral, _RawVariable
from tests.mesh_regression_support import turns
from tests.test_bus_adapter import _adapter_for
from tests.test_mesh_capacity_routing import CountOnlyInventory
from tests.test_virtual_sliders import (
    REAL_PANEL_SLIDER_CONFIG_0_B64 as ORIGINAL,
)
from tests.test_virtual_sliders import (
    FakeVirtualPeripheralHost,
)


class SliderObserver(_GatedRpcObserver):
    def __init__(self) -> None:
        super().__init__()
        self.raw = _RawDevice("ble_mesh", {})
        self.peripheral_reads: list[tuple[str, str]] = []
        self.payloads: list[tuple[str, str, dict[str, str]]] = []
        self.error: BaseException | None = None
        self.reflect = True
        self.release.set()

    async def get_device(self, device_id: str) -> _RawDevice:
        self.reads.append(device_id)
        return self.raw if device_id == "ble_mesh" else _RawDevice(device_id, {})

    async def get_peripheral(self, device_id: str, peripheral_id: str) -> _RawPeripheral | None:
        self.peripheral_reads.append((device_id, peripheral_id))
        return (self.raw.peripherals or {}).get(peripheral_id)

    async def request_set_variables_in_peripheral(
        self, peripheral_id: str, values: dict[str, str], *, device_id: str
    ) -> str:
        self.payloads.append((device_id, peripheral_id, dict(values)))
        await super().request_set_variables_in_peripheral(
            peripheral_id, values, device_id=device_id
        )
        if self.error is not None:
            raise self.error
        if self.reflect and self.raw.peripherals is not None:
            peripheral = self.raw.peripherals.get(peripheral_id)
            if peripheral is not None:
                peripheral.variables.update(
                    {name: _RawVariable(value) for name, value in values.items()}
                )
        return "ok"


@dataclass
class SliderRig:
    adapter: RpcBusAdapter
    observer: SliderObserver
    bridge: Bridge
    controller: VirtualSliderController
    registry: MeshRegistry
    specs: list[VirtualSliderSpec]
    path: Path

    async def start(self) -> None:
        await self.controller.start()
        self.bridge.on_mesh_capacity_recovered = self.controller.retry_deferred_mesh_bindings

    async def overflow(self) -> dict[str, _RawPeripheral]:
        original = self.observer.raw.peripherals
        assert original is not None
        self.observer.raw.peripherals = CountOnlyInventory()
        await self.bridge.poll_once(await self.adapter.get_all())
        assert self.registry.disabled
        return original

    async def recover(self, original: dict[str, _RawPeripheral]) -> None:
        self.observer.raw.peripherals = original
        await self.bridge.poll_once(await self.adapter.get_all())
        assert not self.registry.disabled


@pytest.fixture
async def slider_rig(tmp_path: Path) -> AsyncIterator[SliderRig]:
    observer = SliderObserver()
    _, adapter = _adapter_for(observer)
    mqtt = FakeMqtt()
    bridge = Bridge(adapter, mqtt, "mesh", include=lambda device_id: device_id == "ble_mesh")
    registry = bridge.mesh_registry
    assert registry is not None
    specs = [VirtualSliderSpec.for_ble_switch(str(i), f"Slider {i}", str(i)) for i in range(3)]
    assert observer.raw.peripherals is not None
    for spec in specs:
        raw = _RawPeripheral(ORIGINAL, "slider_config")
        raw.peripheral_type = 70
        observer.raw.peripherals[spec.config_peripheral_id] = raw
    path = tmp_path / "sliders.json"
    controller = VirtualSliderController(
        adapter,
        mqtt,
        "panel",
        "own-device",
        specs,
        FakeVirtualPeripheralHost,
        state_path=path,
        is_mesh_leader=lambda: registry.enabled and not registry.disabled,
    )
    try:
        yield SliderRig(adapter, observer, bridge, controller, registry, specs, path)
    finally:
        observer.release.set()
        await controller.shutdown()
        await bridge.withdraw()
        await adapter.shutdown()


async def test_ownerless_config_obeys_acquisition_capacity_and_fatal(slider_rig: SliderRig) -> None:
    rig = slider_rig
    with pytest.raises(WriteCancelled):
        await rig.adapter.set_variables("ble_mesh", "config", [VarSet("slider_config", ORIGINAL)])
    rig.registry.acquire()
    await rig.adapter.set_variables("ble_mesh", "config", [VarSet("slider_config", ORIGINAL)])
    raw = await rig.overflow()
    with pytest.raises(WriteCancelled):
        await rig.adapter.set_variables("ble_mesh", "config", [VarSet("slider_config", ORIGINAL)])
    await rig.recover(raw)
    await rig.adapter.set_variables("ble_mesh", "config", [VarSet("slider_config", ORIGINAL)])
    rig.adapter.fail_mesh(RuntimeError("sink failed"))
    with pytest.raises(WriteCancelled):
        await rig.adapter.set_variables("ble_mesh", "config", [VarSet("slider_config", ORIGINAL)])
    assert len(rig.observer.payloads) == 2


async def test_acquire_applies_deferred_and_orphan_mesh_config(slider_rig: SliderRig) -> None:
    rig = slider_rig
    await rig.start()
    assert not any(rig.controller.state_for(s.slider_id).binding_ready for s in rig.specs)
    assert rig.observer.payloads == []
    await rig.bridge.reconcile()
    await rig.controller.on_mesh_leadership_acquired()
    assert all(rig.controller.state_for(s.slider_id).binding_ready for s in rig.specs)
    restored = VirtualSliderController(
        rig.adapter,
        FakeMqtt(),
        "panel",
        "own-device",
        [],
        FakeVirtualPeripheralHost,
        state_path=rig.path,
        is_mesh_leader=lambda: rig.registry.enabled and not rig.registry.disabled,
    )
    raw = await rig.overflow()
    await restored.start()
    assert len(restored._orphaned_bindings) == 3
    rig.bridge.on_mesh_capacity_recovered = restored.retry_deferred_mesh_bindings
    await rig.recover(raw)
    assert restored._orphaned_bindings == {}
    assert all(payload[2] == {"slider_config": ORIGINAL} for payload in rig.observer.payloads[-3:])


async def test_disabled_shared_restore_retains_backups(slider_rig: SliderRig) -> None:
    rig = slider_rig
    await rig.bridge.reconcile()
    await rig.start()
    before = rig.path.read_text()
    await rig.overflow()
    count = len(rig.observer.payloads)
    assert not await restore_all_persisted_slider_bindings(rig.path, bus=rig.adapter)
    assert rig.path.read_text() == before
    assert len(rig.observer.payloads) == count


async def test_queued_binding_refused_permanently_across_disable_recovery(
    slider_rig: SliderRig,
) -> None:
    rig = slider_rig
    await rig.bridge.reconcile()
    rig.observer.release.clear()
    blocker = asyncio.create_task(
        rig.adapter.set_variables("ble_mesh", "blocker", [VarSet("on", "1")])
    )
    await turns()
    st = rig.controller.state_for("0")
    binding = asyncio.create_task(rig.controller._apply_slider_binding(rig.specs[0], st.mode))
    await turns()
    raw = await rig.overflow()
    await rig.recover(raw)
    rig.observer.release.set()
    await blocker
    assert not await binding
    assert len(rig.observer.payloads) == 1
    assert st.original_slider_config == ORIGINAL


async def test_mode_preissue_refusal_preserves_ready_without_replay(slider_rig: SliderRig) -> None:
    rig = slider_rig
    await rig.bridge.reconcile()
    await rig.start()
    st = rig.controller.state_for("0")
    assert st.binding_ready
    rig.observer.payloads.clear()
    rig.observer.release.clear()
    blocker = asyncio.create_task(
        rig.adapter.set_variables("ble_mesh", "blocker", [VarSet("on", "1")])
    )
    await turns()
    request = asyncio.create_task(rig.controller._handle_mode_command("0", "passthrough"))
    await turns()
    raw = await rig.overflow()
    await rig.recover(raw)
    rig.observer.release.set()
    await blocker
    await request
    assert st.mode is SliderMode.VIRTUAL and st.binding_ready
    assert len(rig.observer.payloads) == 1
    await rig.controller._handle_mode_command("0", "passthrough")
    assert rig.controller.state_for("0").mode is SliderMode.PASSTHROUGH and st.binding_ready
    assert rig.observer.payloads[-1][2] == {"slider_config": ORIGINAL}
    await rig.controller._handle_mode_command("0", "virtual")
    assert rig.observer.payloads[-1][2] == {
        "slider_config": rig.specs[0].virtual_slider_config_b64("own-device")
    }


@pytest.mark.parametrize("error", [RuntimeError("attempted"), WriteCancelled()])
async def test_attempted_mode_failure_keeps_existing_unready_semantics(
    slider_rig: SliderRig, error: BaseException
) -> None:
    rig = slider_rig
    await rig.bridge.reconcile()
    await rig.start()
    rig.observer.error = error
    await rig.controller._handle_mode_command("0", "passthrough")
    assert rig.controller.state_for("0").mode is SliderMode.VIRTUAL
    assert not rig.controller.state_for("0").binding_ready


async def test_genuine_mode_caller_cancellation_propagates(slider_rig: SliderRig) -> None:
    rig = slider_rig
    await rig.bridge.reconcile()
    await rig.start()
    rig.observer.release.clear()
    task = asyncio.create_task(rig.controller._handle_mode_command("0", "passthrough"))
    await turns()
    task.cancel()
    with pytest.raises(asyncio.CancelledError) as error:
        await task
    assert type(error.value) is asyncio.CancelledError
    assert rig.controller.state_for("0").mode is SliderMode.VIRTUAL
    rig.observer.release.set()
    await turns()


async def test_two_crossings_ready_sliders_do_zero_scoped_reads_or_writes(
    slider_rig: SliderRig,
) -> None:
    rig = slider_rig
    await rig.bridge.reconcile()
    await rig.start()
    rig.observer.peripheral_reads.clear()
    rig.observer.payloads.clear()
    rig.observer.reads.clear()
    for _ in range(2):
        raw = await rig.overflow()
        await rig.recover(raw)
    assert rig.observer.peripheral_reads == []
    assert rig.observer.payloads == []
    assert rig.observer.reads == ["own-device", "ble_mesh"] * 4


async def test_recovery_retries_only_existing_deferred_mesh_target(slider_rig: SliderRig) -> None:
    rig = slider_rig
    await rig.bridge.reconcile()
    await rig.start()
    raw = await rig.overflow()
    spec = rig.specs[1]
    rig.controller.state_for(spec.slider_id).binding_ready = False
    raw[spec.config_peripheral_id].variables["slider_config"] = _RawVariable(ORIGINAL)
    rig.observer.peripheral_reads.clear()
    rig.observer.payloads.clear()
    rig.observer.reads.clear()
    await rig.recover(raw)
    assert rig.controller.state_for(spec.slider_id).binding_ready
    assert rig.observer.reads == ["own-device", "ble_mesh"]
    assert rig.observer.peripheral_reads == [("ble_mesh", spec.config_peripheral_id)] * 2
    assert rig.observer.payloads == [
        (
            "ble_mesh",
            spec.config_peripheral_id,
            {"slider_config": spec.virtual_slider_config_b64("own-device")},
        )
    ]
    assert json.loads(rig.path.read_text())[spec.slider_id]["original_slider_config"] == ORIGINAL


async def test_owner_bound_load_still_requires_exact_current_owner(slider_rig: SliderRig) -> None:
    rig = slider_rig
    assert rig.observer.raw.peripherals is not None
    load = _RawPeripheral("1")
    load.peripheral_type = 27
    rig.observer.raw.peripherals["load"] = load
    await rig.bridge.reconcile()
    owner = rig.registry.current("load")
    assert owner is not None
    await rig.adapter.set_variables(
        "ble_mesh", "load", [VarSet("on", "0")], ticket=AdmissionTicket(mesh_owner=owner)
    )
    raw = await rig.overflow()
    await rig.recover(raw)
    assert rig.registry.current("load") is not owner
    with pytest.raises(WriteCancelled):
        await rig.adapter.set_variables(
            "ble_mesh", "load", [VarSet("on", "1")], ticket=AdmissionTicket(mesh_owner=owner)
        )
    assert rig.observer.payloads == [("ble_mesh", "load", {"on": "0"})]


async def test_custom_resident_target_needs_no_load_owner(slider_rig: SliderRig) -> None:
    rig = slider_rig
    spec = VirtualSliderSpec("custom", "Custom", "ble_mesh", "resident-target", "binding")
    assert rig.observer.raw.peripherals is not None
    rig.observer.raw.peripherals[spec.config_peripheral_id] = _RawPeripheral(ORIGINAL, "binding")
    controller = VirtualSliderController(
        rig.adapter,
        FakeMqtt(),
        "panel",
        "own-device",
        [spec],
        FakeVirtualPeripheralHost,
        state_path=rig.path,
        is_mesh_leader=lambda: rig.registry.enabled and not rig.registry.disabled,
    )
    try:
        await rig.bridge.reconcile()
        await controller.start()
        assert controller.state_for(spec.slider_id).binding_ready
        assert rig.registry.current(spec.config_peripheral_id) is None
        assert rig.observer.payloads == [
            (
                "ble_mesh",
                spec.config_peripheral_id,
                {"binding": spec.virtual_slider_config_b64("own-device")},
            )
        ]
    finally:
        await controller.shutdown()


async def test_ordinary_inventory_changes_do_not_retry_nonready_slider(
    slider_rig: SliderRig,
) -> None:
    rig = slider_rig
    await rig.bridge.reconcile()
    await rig.start()
    spec = rig.specs[0]
    rig.controller.state_for(spec.slider_id).binding_ready = False
    assert rig.observer.raw.peripherals is not None
    rig.observer.raw.peripherals[spec.config_peripheral_id].variables["slider_config"] = (
        _RawVariable(ORIGINAL)
    )
    load = _RawPeripheral("1")
    load.peripheral_type = 27
    rig.observer.raw.peripherals["new-load"] = load
    rig.observer.peripheral_reads.clear()
    rig.observer.payloads.clear()
    await rig.bridge.poll_once(await rig.adapter.get_all())
    assert not rig.controller.state_for(spec.slider_id).binding_ready
    assert rig.observer.peripheral_reads == []
    assert rig.observer.payloads == []


async def test_final_issue_rechecks_live_leadership_outside_generation_lock(
    slider_rig: SliderRig,
) -> None:
    rig = slider_rig
    leader = [True]

    def included(device_id: str) -> bool:
        assert rig.adapter._generation_lock.acquire(blocking=False)
        rig.adapter._generation_lock.release()
        return device_id == "ble_mesh" and leader[0]

    rig.bridge._include = included
    await rig.bridge.reconcile()
    rig.observer.release.clear()
    blocker = asyncio.create_task(
        rig.adapter.set_variables("ble_mesh", "blocker", [VarSet("on", "1")])
    )
    await turns()
    ticket = AdmissionTicket()
    queued = asyncio.create_task(
        rig.adapter.set_variables(
            "ble_mesh", "resident-config", [VarSet("binding", ORIGINAL)], ticket=ticket
        )
    )
    await turns()
    leader[0] = False
    rig.observer.release.set()
    await blocker
    with pytest.raises(asyncio.CancelledError):
        await queued
    assert ticket.issued_at is None and isinstance(ticket.outcome, WriteCancelled)
    assert rig.observer.payloads == [("ble_mesh", "blocker", {"on": "1"})]


@pytest.mark.parametrize("restore_orphans", [False, True])
async def test_queued_restore_refusal_retains_backup_across_reopening(
    slider_rig: SliderRig, restore_orphans: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = slider_rig
    await rig.bridge.reconcile()
    await rig.start()
    orphaned = VirtualSliderController(
        rig.adapter,
        FakeMqtt(),
        "panel",
        "own-device",
        [],
        FakeVirtualPeripheralHost,
        state_path=rig.path,
    )
    rig.observer.payloads.clear()
    rig.observer.release.clear()
    blocker = asyncio.create_task(
        rig.adapter.set_variables("ble_mesh", "blocker", [VarSet("on", "1")])
    )
    await turns()
    admitted = asyncio.Event()
    serve = rig.adapter._serve_next

    def serve_next(device_id: str) -> None:
        serve(device_id)
        if len(rig.adapter._write_admissions) >= 2:
            admitted.set()

    monkeypatch.setattr(rig.adapter, "_serve_next", serve_next)
    pending = asyncio.create_task(
        orphaned._restore_orphaned_bindings()
        if restore_orphans
        else restore_all_persisted_slider_bindings(rig.path, bus=rig.adapter)
    )

    await asyncio.wait_for(admitted.wait(), 1)
    raw = await rig.overflow()
    await rig.recover(raw)
    rig.observer.release.set()
    await blocker
    result = await pending
    if restore_orphans:
        assert len(orphaned._orphaned_bindings) == 3
    else:
        assert result is False
    assert len(json.loads(rig.path.read_text())) == 3
    assert all(
        entry["original_slider_config"] == ORIGINAL
        for entry in json.loads(rig.path.read_text()).values()
    )
    assert rig.observer.payloads == [("ble_mesh", "blocker", {"on": "1"})]


async def test_binding_equal_read_during_disable_cannot_report_ready(
    slider_rig: SliderRig, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = slider_rig
    await rig.bridge.reconcile()
    await rig.start()
    entered, release = asyncio.Event(), asyncio.Event()
    read = rig.observer.get_peripheral

    async def blocked(device_id: str, peripheral_id: str) -> _RawPeripheral | None:
        entered.set()
        await release.wait()
        return await read(device_id, peripheral_id)

    monkeypatch.setattr(rig.observer, "get_peripheral", blocked)
    rig.observer.payloads.clear()
    pending = asyncio.create_task(
        rig.controller._apply_slider_binding(rig.specs[0], SliderMode.VIRTUAL)
    )
    await entered.wait()
    await rig.overflow()
    release.set()
    assert not await pending
    assert rig.observer.payloads == []


async def test_overlapping_capacity_recoveries_serialize_deferred_sweeps(
    slider_rig: SliderRig,
) -> None:
    rig = slider_rig
    await rig.bridge.reconcile()
    await rig.start()
    spec = rig.specs[0]
    rig.controller.state_for(spec.slider_id).binding_ready = False
    assert rig.observer.raw.peripherals is not None
    rig.observer.raw.peripherals[spec.config_peripheral_id].variables["slider_config"] = (
        _RawVariable(ORIGINAL)
    )
    raw = await rig.overflow()
    rig.observer.peripheral_reads.clear()
    rig.observer.payloads.clear()
    entered = asyncio.Event()
    running = 0
    maximum = 0
    callbacks = 0

    async def recovered() -> None:
        nonlocal running, maximum, callbacks
        running += 1
        callbacks += 1
        maximum = max(maximum, running)
        entered.set()
        try:
            await rig.controller.retry_deferred_mesh_bindings()
        finally:
            running -= 1

    rig.bridge.on_mesh_capacity_recovered = recovered
    rig.observer.release.clear()
    first = asyncio.create_task(rig.recover(raw))
    second: asyncio.Task[None] | None = None
    try:
        await entered.wait()
        await turns()
        assert len(rig.observer.payloads) == 1
        await rig.overflow()
        second = asyncio.create_task(rig.recover(raw))
        await turns()
        assert maximum == 1
    finally:
        rig.observer.release.set()
        await asyncio.gather(first, *([second] if second is not None else []))
    assert callbacks == 2
    assert rig.controller.state_for(spec.slider_id).binding_ready
    assert rig.observer.peripheral_reads == [("ble_mesh", spec.config_peripheral_id)] * 2
    assert len(rig.observer.payloads) == 1


@pytest.mark.parametrize("restore_orphans", [False, True])
async def test_restore_backup_survives_native_success_without_matching_readback(
    slider_rig: SliderRig, restore_orphans: bool
) -> None:
    rig = slider_rig
    await rig.bridge.reconcile()
    await rig.start()
    orphaned = VirtualSliderController(
        rig.adapter,
        FakeMqtt(),
        "panel",
        "own-device",
        [],
        FakeVirtualPeripheralHost,
        state_path=rig.path,
    )
    rig.observer.reflect = False
    if restore_orphans:
        await orphaned._restore_orphaned_bindings()
        assert len(orphaned._orphaned_bindings) == 3
    else:
        assert not await restore_all_persisted_slider_bindings(rig.path, bus=rig.adapter)
    assert all(
        row["original_slider_config"] == ORIGINAL
        for row in json.loads(rig.path.read_text()).values()
    )
    rig.observer.reflect = True
    if restore_orphans:
        await orphaned._restore_orphaned_bindings()
        assert not orphaned._orphaned_bindings
    else:
        assert await restore_all_persisted_slider_bindings(rig.path, bus=rig.adapter)
