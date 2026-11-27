"""Complete resident slider operations retain revocation and commit ordering."""

from __future__ import annotations

import asyncio
import copy
import json

import pytest

from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.virtual_sliders import SliderMode
from brilliant_mqtt.write_admission import AdmissionTicket, TicketAdoptionError, WriteCancelled
from tests.fakes import FakeMqtt, _RawPeripheral
from tests.mesh_regression_support import turns
from tests.test_bus_adapter import _adapter_for
from tests.test_mesh_slider_admission import SliderObserver, SliderRig, slider_rig

__all__ = ["slider_rig"]

_MODE_TOPIC = "brilliant/panel/virtual_slider/0/set_mode"


def mqtt_for(rig: SliderRig) -> FakeMqtt:
    mqtt = rig.controller._mqtt
    assert isinstance(mqtt, FakeMqtt)
    return mqtt


async def assert_passthrough_agrees(rig: SliderRig) -> None:
    state = rig.controller.state_for("0")
    assert state.mode is SliderMode.PASSTHROUGH and state.binding_ready
    assert json.loads(rig.path.read_text())["0"]["mode"] == "passthrough"
    payload = next(
        payload
        for topic, payload, _ in reversed(mqtt_for(rig).published)
        if topic == "brilliant/panel/virtual_slider_0/state"
    )
    assert json.loads(payload)["mode"] == "passthrough"
    assert json.loads(payload)["binding_ready"] is True
    actual = await rig.adapter.get_peripheral("ble_mesh", rig.specs[0].config_peripheral_id)
    assert actual is not None
    assert actual.variables["slider_config"].value == state.original_slider_config
    assert not rig.adapter._mesh_preparing
    assert not state.binding_lock.locked()


@pytest.mark.parametrize("loss", ["capacity", "scope", "generation", "fatal"])
async def test_mode_preparatory_read_cannot_survive_mesh_loss_and_recovery(
    slider_rig: SliderRig, monkeypatch: pytest.MonkeyPatch, loss: str
) -> None:
    rig = slider_rig
    await rig.bridge.reconcile()
    await rig.start()
    state = rig.controller.state_for("0")
    assert state.mode is SliderMode.VIRTUAL and state.binding_ready
    entered, release = asyncio.Event(), asyncio.Event()
    read = rig.observer.get_peripheral
    first = True

    async def blocked(device_id: str, peripheral_id: str) -> _RawPeripheral | None:
        nonlocal first
        if first:
            first = False
            entered.set()
            await release.wait()
        return await read(device_id, peripheral_id)

    monkeypatch.setattr(rig.observer, "get_peripheral", blocked)
    rig.observer.payloads.clear()
    request = asyncio.create_task(mqtt_for(rig).inject(_MODE_TOPIC, "passthrough"))
    try:
        await entered.wait()
        assert not rig.adapter._write_admissions
        if loss == "capacity":
            raw = await rig.overflow()
            await rig.recover(raw)
        elif loss == "scope":
            rig.registry.close()
            await rig.bridge.reconcile()
        elif loss == "generation":
            rig.adapter._on_proc_reconnect()
            await rig.bridge.poll_once(await rig.adapter.get_all())
        else:
            rig.adapter.fail_mesh(RuntimeError("actual fatal during preparation"))
        release.set()
        await request
        assert rig.observer.payloads == []
        assert state.mode is SliderMode.VIRTUAL and state.binding_ready
        assert not rig.adapter._mesh_preparing
        if loss == "fatal":
            assert not rig.adapter.mesh_admission_open
            return
        await mqtt_for(rig).inject(_MODE_TOPIC, "passthrough")
        assert rig.observer.payloads == [
            (
                "ble_mesh",
                rig.specs[0].config_peripheral_id,
                {"slider_config": state.original_slider_config},
            )
        ]
        await assert_passthrough_agrees(rig)
    finally:
        release.set()
        await asyncio.gather(request, return_exceptions=True)


async def test_recovery_waits_for_mode_readback_commit_then_rechecks_deferred_work(
    slider_rig: SliderRig, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = slider_rig
    await rig.bridge.reconcile()
    await rig.start()
    state = rig.controller.state_for("0")
    rig.observer.error = RuntimeError("actual attempted mode failure")
    await mqtt_for(rig).inject(_MODE_TOPIC, "passthrough")
    rig.observer.error = None
    assert state.mode is SliderMode.VIRTUAL and not state.binding_ready
    entered, release = asyncio.Event(), asyncio.Event()
    read = rig.observer.get_peripheral
    calls = 0

    async def delayed_readback(device_id: str, peripheral_id: str) -> _RawPeripheral | None:
        nonlocal calls
        if peripheral_id == rig.specs[0].config_peripheral_id:
            calls += 1
            if calls == 2:
                snapshot = copy.deepcopy(await read(device_id, peripheral_id))
                entered.set()
                await release.wait()
                return snapshot
        return await read(device_id, peripheral_id)

    monkeypatch.setattr(rig.observer, "get_peripheral", delayed_readback)
    rig.observer.payloads.clear()
    rig.observer.peripheral_reads.clear()
    request = asyncio.create_task(mqtt_for(rig).inject(_MODE_TOPIC, "passthrough"))
    recovery: asyncio.Task[None] | None = None
    try:
        await entered.wait()
        raw = await rig.overflow()
        recovery = asyncio.create_task(rig.recover(raw))
        await turns()
        assert len(rig.observer.payloads) == 1, "recovery must not overwrite the in-flight mode"
        assert not recovery.done()
        release.set()
        await asyncio.gather(request, recovery)
        assert calls == 2, "the now-ready slider needs no recovery backup or readback"
        await assert_passthrough_agrees(rig)
        assert len(rig.observer.payloads) == 1
    finally:
        release.set()
        await asyncio.gather(
            request, *([recovery] if recovery is not None else []), return_exceptions=True
        )


async def test_mode_caller_cancel_during_preparation_propagates_and_releases_tracking(
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
    request = asyncio.create_task(mqtt_for(rig).inject(_MODE_TOPIC, "passthrough"))
    await entered.wait()
    request.cancel()
    with pytest.raises(asyncio.CancelledError) as error:
        await request
    assert type(error.value) is asyncio.CancelledError
    assert not rig.adapter._mesh_preparing
    assert not rig.adapter._write_admissions
    assert rig.observer.payloads == []
    release.set()
    await mqtt_for(rig).inject(_MODE_TOPIC, "passthrough")
    await assert_passthrough_agrees(rig)


@pytest.mark.parametrize("stop", ["loss", "cancel"])
async def test_mode_waiting_for_slider_mutex_is_revocable_and_cleans_up(
    slider_rig: SliderRig, stop: str
) -> None:
    rig = slider_rig
    await rig.bridge.reconcile()
    await rig.start()
    state = rig.controller.state_for("0")
    await state.binding_lock.acquire()
    rig.observer.payloads.clear()
    request = asyncio.create_task(mqtt_for(rig).inject(_MODE_TOPIC, "passthrough"))
    try:
        await turns()
        assert len(rig.adapter._mesh_preparing) == 1
        if stop == "loss":
            raw = await rig.overflow()
            await rig.recover(raw)
        else:
            request.cancel()
            with pytest.raises(asyncio.CancelledError):
                await request
        state.binding_lock.release()
        await asyncio.gather(request, return_exceptions=True)
        assert rig.observer.payloads == []
        assert state.mode is SliderMode.VIRTUAL and state.binding_ready
        assert not rig.adapter._mesh_preparing
        await mqtt_for(rig).inject(_MODE_TOPIC, "passthrough")
        await assert_passthrough_agrees(rig)
    finally:
        if state.binding_lock.locked():
            state.binding_lock.release()
        await asyncio.gather(request, return_exceptions=True)


async def test_already_issued_mode_survives_capacity_loss_without_fake_refusal(
    slider_rig: SliderRig,
) -> None:
    rig = slider_rig
    await rig.bridge.reconcile()
    await rig.start()
    rig.observer.payloads.clear()
    rig.observer.release.clear()
    request = asyncio.create_task(mqtt_for(rig).inject(_MODE_TOPIC, "passthrough"))
    try:
        await turns()
        ticket = next(iter(rig.adapter._mesh_preparing))
        assert ticket.issued_at is not None and ticket.outcome is None
        raw = await rig.overflow()
        await rig.recover(raw)
        assert ticket.outcome is None
        assert not request.done()
        rig.observer.release.set()
        await request
        assert isinstance(ticket.outcome, str)
        assert len(rig.observer.payloads) == 1
        await assert_passthrough_agrees(rig)
    finally:
        rig.observer.release.set()
        await asyncio.gather(request, return_exceptions=True)


async def test_preparation_ticket_has_one_adapter_owner_and_cannot_be_reused(
    slider_rig: SliderRig,
) -> None:
    rig = slider_rig
    await rig.bridge.reconcile()
    _, other = _adapter_for(SliderObserver())
    ticket = AdmissionTicket()
    remove = rig.adapter.register_mesh_write_operation(ticket, "resident-config")

    def other_remove() -> None:
        return

    try:
        with pytest.raises(TicketAdoptionError):
            other_remove = other.register_mesh_write_operation(ticket, "resident-config")
        with pytest.raises(TicketAdoptionError):
            await other.set_variables(
                "ble_mesh", "resident-config", [VarSet("binding", "other")], ticket=ticket
            )
        with pytest.raises(TicketAdoptionError):
            rig.adapter.register_mesh_write_operation(ticket, "resident-config")
        await rig.adapter.set_variables(
            "ble_mesh", "resident-config", [VarSet("binding", "first")], ticket=ticket
        )
        assert ticket.issued_at is not None
        assert isinstance(ticket.outcome, str)
        remove()
        with pytest.raises(TicketAdoptionError):
            await rig.adapter.set_variables(
                "ble_mesh", "resident-config", [VarSet("binding", "first")], ticket=ticket
            )
    finally:
        other_remove()
        remove()
        await other.shutdown()
    refused = AdmissionTicket()
    remove_refused = rig.adapter.register_mesh_write_operation(refused, "resident-config")
    try:
        rig.adapter.revoke_mesh_waiting()
        with pytest.raises(WriteCancelled):
            await rig.adapter.set_variables(
                "ble_mesh", "resident-config", [VarSet("binding", "refused")], ticket=refused
            )
    finally:
        remove_refused()
    with pytest.raises(TicketAdoptionError):
        rig.adapter.register_mesh_write_operation(refused, "resident-config")
    abandoned = AdmissionTicket()
    remove_abandoned = rig.adapter.register_mesh_write_operation(abandoned, "resident-config")
    remove_abandoned()
    with pytest.raises(TicketAdoptionError):
        await rig.adapter.set_variables(
            "ble_mesh", "resident-config", [VarSet("binding", "abandoned")], ticket=abandoned
        )
    assert not rig.adapter._mesh_preparing


async def test_fatal_refusal_callbacks_do_not_reenter_revocation_or_log_storm(
    slider_rig: SliderRig, caplog: pytest.LogCaptureFixture
) -> None:
    rig = slider_rig
    await rig.bridge.reconcile()
    tickets = [AdmissionTicket() for _ in range(3)]
    removals = [
        rig.adapter.register_mesh_write_operation(ticket, "resident-config") for ticket in tickets
    ]
    callbacks = 0

    def refused() -> None:
        nonlocal callbacks
        callbacks += 1
        raise RuntimeError("refusal callback failed")

    for ticket in tickets:
        ticket._on_refused = refused
    try:
        rig.adapter.fail_mesh(RuntimeError("initial sink failure"))
        assert callbacks == 3
        assert all(isinstance(ticket.outcome, WriteCancelled) for ticket in tickets)
        assert sum("MESH_SINK_FAILED" in record.message for record in caplog.records) == 1
        assert rig.adapter.consume_mesh_fatal()
        rig.adapter.fail_mesh(RuntimeError("repeated failure"))
        assert not rig.adapter.consume_mesh_fatal()
    finally:
        for remove in removals:
            remove()
    assert not rig.adapter._mesh_preparing


async def test_adapter_teardown_refuses_and_drops_live_preparation_without_native_issue(
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
    request = asyncio.create_task(mqtt_for(rig).inject(_MODE_TOPIC, "passthrough"))
    try:
        await entered.wait()
        ticket = next(iter(rig.adapter._mesh_preparing))
        await rig.adapter.shutdown()
        assert not rig.adapter._mesh_preparing
        assert ticket.issued_at is None and isinstance(ticket.outcome, WriteCancelled)
        release.set()
        await request
        state = rig.controller.state_for("0")
        assert state.mode is SliderMode.VIRTUAL and state.binding_ready
        assert not state.binding_lock.locked()
        assert rig.observer.payloads == []
    finally:
        release.set()
        await asyncio.gather(request, return_exceptions=True)
