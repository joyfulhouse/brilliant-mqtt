"""Historical #232 schedules through the production adapter and bridge.

Tests use main's diagnostic registries only to check cleanup/identity; transport
and wire assertions are the settlement oracle. No fake makes settlement choices.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable, MutableMapping
from dataclasses import FrozenInstanceError
from types import MappingProxyType
from typing import cast

import pytest

from brilliant_mqtt import bus as bus_mod
from brilliant_mqtt.bridge import Bridge
from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.mesh_settlement_types import OutcomeKind, Phase
from brilliant_mqtt.model import BrilliantDevice, CaptureProvenance
from brilliant_mqtt.write_admission import (
    AdmissionTicket,
    CommandAdmission,
    WriteCancelled,
    WriteClass,
    WriteResult,
    command_admission,
)
from tests.fakes import FakeClock, FakeSleeper, _RawDevice, _RawPeripheral, _RawVariable
from tests.mesh_regression_support import (
    Rig,
    commands,
    last_observed,
    owners,
    pending,
    publishers,
    settlement,
    turns,
)
from tests.test_bridge import (
    MESH_PID,
    MESH_SET_TOPIC,
    _BlockingStatePublishMqtt,
    _mesh_feedback_payload,
    _mesh_states,
)
from tests.test_bus_adapter import _gated_adapter


@pytest.fixture
async def rig(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[Rig]:
    monkeypatch.setattr(bus_mod, "_WRITE_DEADLINE_S", 0)
    observer, adapter = _gated_adapter()
    peripheral = _RawPeripheral("1")
    peripheral.peripheral_type = 27
    peripheral.variables["intensity"] = _RawVariable("600")
    raw = _RawDevice("ble_mesh", {MESH_PID: peripheral})

    async def get_device(device_id: str) -> _RawDevice:
        observer.reads.append(device_id)
        return raw if device_id == "ble_mesh" else _RawDevice(device_id, {})

    monkeypatch.setattr(observer, "get_device", get_device)
    mqtt = _BlockingStatePublishMqtt()
    clock, sleeper = FakeClock(), FakeSleeper()
    adapter._clock = clock
    bridge = Bridge(
        adapter,
        mqtt,
        "mesh",
        include=lambda dev: dev == "ble_mesh",
        clock=clock,
        wall_clock=lambda: 1000 + clock(),
        sleep=sleeper,
    )
    result = Rig(observer, adapter, raw, mqtt, bridge, clock, sleeper)
    original_set = adapter.set_variables
    original_run = adapter._run_write
    original_native = observer.request_set_variables_in_peripheral

    async def record_set(
        device_id: str,
        peripheral_id: str,
        sets: list[VarSet],
        *,
        write_class: WriteClass = WriteClass.INTERACTIVE_FIFO,
        ticket: AdmissionTicket | None = None,
    ) -> WriteResult:
        if ticket is not None:
            result.tickets.append(ticket)
        return await original_set(
            device_id, peripheral_id, sets, write_class=write_class, ticket=ticket
        )

    async def record_native(
        peripheral_id: str,
        values: dict[str, str],
        *,
        device_id: str,
    ) -> str:
        result.native_payloads.append(dict(values))
        return await original_native(peripheral_id, values, device_id=device_id)

    async def record_run(observer_boundary: object, admission: bus_mod._WriteAdmission) -> str:
        if admission.ticket not in result.tickets:
            result.tickets.append(admission.ticket)
        return await original_run(observer_boundary, admission)

    monkeypatch.setattr(adapter, "set_variables", record_set)
    monkeypatch.setattr(adapter, "_run_write", record_run)
    monkeypatch.setattr(observer, "request_set_variables_in_peripheral", record_native)
    try:
        await bridge.reconcile()
        token = command_admission.set(CommandAdmission())
        try:
            await mqtt.inject(MESH_SET_TOPIC, '{"state":"OFF"}')
        finally:
            command_admission.reset(token)
        result.assert_pending(deadline=1080)
        assert observer.in_flight == 1
        yield result
    finally:
        observer.release.set()
        mqtt.release_publish.set()
        await bridge.withdraw()
        await adapter.shutdown()


async def test_pre_acceptance_contradiction_is_ignored(rig: Rig) -> None:
    before = len(_mesh_states(rig.mqtt))
    await rig.report("1")
    rig.assert_pending(deadline=1080)
    assert len(_mesh_states(rig.mqtt)) == before
    assert rig.observer.in_flight == 1
    assert rig.sleeper.requested == [80]


async def test_pre_acceptance_snapshot_is_not_contradiction(rig: Rig) -> None:
    captured = await rig.adapter.get_all()
    assert captured[0].capture_provenance is not None
    await rig.accept()
    await rig.bridge.poll_once(captured)
    rig.assert_pending(deadline=1080)


async def test_fresh_contradiction_after_native_return_is_not_lost(rig: Rig) -> None:
    (native,) = rig.adapter._write_tasks
    delivered: list[tuple[bool, bool]] = []

    async def deliver() -> None:
        delivered.append(
            (native.done() and native.result() == "'ok'", not rig.mqtt.publish_started.is_set())
        )
        await rig.notify()

    rig.clock.advance(5)
    rig.mqtt.block_next_state_publish = True
    rig.observer.release.set()
    delivery = asyncio.create_task(deliver())
    await delivery
    await turns()
    assert delivered == [(True, True)]
    assert rig.mqtt.publish_started.is_set()
    assert not rig.mqtt.release_publish.is_set()
    assert owners(rig.bridge)[MESH_PID].view.status == "contradicted"
    rig.mqtt.release_publish.set()
    await turns()
    rig.assert_terminal("contradicted", "ON")
    assert not pending(rig.bridge)
    assert not rig.timers()


async def test_expiry_does_not_wait_for_rearm_publish(rig: Rig) -> None:
    rig.clock.advance(5)
    rig.mqtt.block_next_state_publish = True
    await rig.accept()
    # On main there is no rearm; fail semantically rather than wait forever.
    assert rig.mqtt.publish_started.is_set(), "late acceptance must publish a fresh pending window"
    reads = list(rig.observer.reads)
    rig.bridge._wall_clock = lambda: -1_000_000
    await rig.expire()
    assert not rig.mqtt.release_publish.is_set()
    assert not pending(rig.bridge)
    assert owners(rig.bridge)[MESH_PID].view.status == "unconfirmed"
    rig.mqtt.release_publish.set()
    await turns()
    rig.assert_terminal("unconfirmed", None)
    assert rig.observer.reads == reads


async def test_late_acceptance_rearms_then_is_contradicted(rig: Rig) -> None:
    await rig.report("0")
    rig.clock.advance(10)
    await rig.accept()
    rig.assert_pending(deadline=1090)
    assert rig.sleeper.requested == [80, 80]
    assert pending(rig.bridge)[MESH_PID].view.receipt == "'ok'"
    assert last_observed(pending(rig.bridge)[MESH_PID]) is None
    await rig.report("1")
    rig.assert_terminal("contradicted", "ON")
    before = len(_mesh_states(rig.mqtt))
    await rig.expire()
    assert len(_mesh_states(rig.mqtt)) == before
    assert not rig.timers()


async def test_late_acceptance_then_matching_value_confirms(rig: Rig) -> None:
    await rig.report("0")  # pre-return evidence must be discarded
    rig.clock.advance(3)
    await rig.accept()
    rig.assert_pending(deadline=1083)
    offset = len(_mesh_states(rig.mqtt))
    rig.clock.advance(74)
    await turns()
    rig.assert_pending()
    await rig.report("0")
    rig.assert_pending()
    assert _mesh_states(rig.mqtt)[offset:] == []
    await rig.expire(6)
    rig.assert_terminal("idle", "OFF")
    assert [json.loads(payload) for _, payload, _ in _mesh_states(rig.mqtt)[offset:]] == [
        _mesh_feedback_payload("OFF")
    ]
    assert not pending(rig.bridge)
    assert not rig.timers()


async def test_late_failure_publishes_failed_with_observed_state(rig: Rig) -> None:
    await rig.report("1")
    rig.observer._fail_with = RuntimeError("late rejection")
    rig.mqtt.published.clear()
    await rig.accept()
    rig.assert_terminal("failed", "ON")
    assert len(_mesh_states(rig.mqtt)) == 1
    assert not pending(rig.bridge)
    assert not rig.timers()
    await rig.expire()
    assert len(_mesh_states(rig.mqtt)) == 1


@pytest.mark.parametrize(
    "outcome",
    [asyncio.TimeoutError(), TimeoutError("native"), asyncio.CancelledError(), WriteCancelled()],
    ids=["asyncio_timeout", "native_timeout", "cancelled", "write_cancelled"],
)
async def test_late_timeout_or_cancellation_resumes_contradiction(
    rig: Rig,
    monkeypatch: pytest.MonkeyPatch,
    outcome: BaseException,
) -> None:
    async def settled_native() -> None:
        await rig.observer.release.wait()
        raise outcome

    # The issued observer is already waiting inside _block; its transport
    # continuation is replaced only for the next write. Finish the initial one.
    await rig.accept()
    rig.clock.advance(1)
    rig.observer.release.clear()
    monkeypatch.setattr(rig.observer, "_block", settled_native)
    await rig.mqtt.inject(MESH_SET_TOPIC, '{"state":"OFF"}')
    record = pending(rig.bridge)[MESH_PID]
    deadline = record.view.wire_deadline
    rig.mqtt.published.clear()
    rig.clock.advance(4)
    rig.observer.release.set()
    await turns()
    assert pending(rig.bridge)[MESH_PID] is record
    assert record.view.wire_deadline == deadline == 1081
    assert _mesh_states(rig.mqtt) == []
    await rig.report("1")
    rig.assert_terminal("contradicted", "ON")


@pytest.mark.parametrize("settled", [False, True], ids=["caller_timeout", "native_timeout"])
async def test_matching_observation_holds_null_until_confirm_window(
    rig: Rig,
    monkeypatch: pytest.MonkeyPatch,
    settled: bool,
) -> None:
    if settled:
        rig.observer._fail_with = TimeoutError("No response received!")
        await rig.accept()
    rig.clock.advance(75)
    await rig.report("0")
    rig.assert_pending()
    rig.clock.advance(5)
    await rig.sleeper.release_all()
    await turns()
    rig.assert_terminal("idle" if settled else "unconfirmed", "OFF" if settled else None)
    assert not pending(rig.bridge)
    assert not rig.timers()


async def test_unresolved_write_without_observations_closes_unconfirmed(rig: Rig) -> None:
    await rig.expire()
    rig.assert_terminal("unconfirmed", None)
    assert not pending(rig.bridge)
    assert not rig.timers()
    assert rig.observer.in_flight == 1
    assert not rig.observer.write_cancelled


@pytest.mark.parametrize("late_error", [False, True], ids=["receipt", "error"])
@pytest.mark.parametrize("revoke", [False, True], ids=["pending", "withdrawn"])
async def test_detached_rpc_late_outcome_settles_only_its_own_pending(
    rig: Rig,
    late_error: bool,
    revoke: bool,
) -> None:
    record = pending(rig.bridge)[MESH_PID]
    old_window = record.view.window
    if late_error:
        rig.observer._fail_with = RuntimeError("late rejection")
    if revoke:
        await rig.bridge.withdraw()
    rig.mqtt.published.clear()
    rig.clock.advance(10)
    await rig.accept()
    if revoke:
        assert rig.mqtt.published == []
        assert not pending(rig.bridge)
        assert not rig.timers()
        assert rig.sleeper.requested == [80]
    elif late_error:
        rig.assert_terminal("failed", "ON")
        assert not pending(rig.bridge)
        assert not rig.timers()
    else:
        rig.assert_pending(deadline=1090)
        rearmed = pending(rig.bridge)[MESH_PID]
        assert rearmed is record
        assert rearmed.view.window is not old_window
        assert rearmed.view.receipt == "'ok'"
        assert last_observed(rearmed) is None
        assert rig.sleeper.requested == [80, 80]


@pytest.mark.parametrize("late", ["accepted", "failed"])
async def test_superseded_write_late_outcome_is_ignored(
    rig: Rig, monkeypatch: pytest.MonkeyPatch, late: str
) -> None:
    old = pending(rig.bridge)[MESH_PID]
    old_command = old.view.command_id
    old_timer = rig.timers()[MESH_PID]
    (native,) = rig.adapter._write_tasks
    successor_entered, successor_release = asyncio.Event(), asyncio.Event()

    async def successor_native() -> None:
        successor_entered.set()
        await successor_release.wait()

    monkeypatch.setattr(rig.observer, "_block", successor_native)
    # Keep the replacement caller waiting throughout the stale-only interval;
    # its own native return remains behind an independent transport gate.
    monkeypatch.setattr(bus_mod, "_WRITE_DEADLINE_S", 60)
    if late == "failed":
        rig.observer._fail_with = RuntimeError("late")
        native.add_done_callback(lambda _task: setattr(rig.observer, "_fail_with", None))
    command = CommandAdmission()
    token = command_admission.set(command)
    try:
        replacement = asyncio.create_task(rig.mqtt.inject(MESH_SET_TOPIC, '{"state":"ON"}'))
    finally:
        command_admission.reset(token)
    try:
        await turns()
        assert old.view.command_id != old_command
        assert pending(rig.bridge).get(MESH_PID) is None
        successor = owners(rig.bridge)[MESH_PID]
        generation = commands(rig.bridge)[MESH_PID]
        admission = rig.adapter._write_admissions[command.ticket]
        assert admission.ticket is command.ticket
        assert admission.values == {"on": "1"}
        assert not admission.issued
        assert not successor_entered.is_set()
        assert not native.done() and rig.observer.in_flight == 1
        assert rig.adapter._write_locks["ble_mesh"].locked()
        assert not rig.observer.write_cancelled
        assert old_timer.cancelled()
        assert not rig.timers()
        assert rig.sleeper.requested == [80]

        rig.mqtt.published.clear()
        rig.clock.advance(5)
        rig.observer.release.set()
        await asyncio.wait_for(successor_entered.wait(), 1)
        await turns()
        assert native.done()
        if late == "failed":
            assert isinstance(native.exception(), RuntimeError)
        else:
            assert native.result() == "'ok'"
        assert not successor_release.is_set()
        assert not replacement.done()
        assert rig.observer.in_flight == 1
        assert rig.adapter._write_locks["ble_mesh"].locked()
        assert owners(rig.bridge)[MESH_PID] is successor
        assert commands(rig.bridge)[MESH_PID] == generation
        assert rig.adapter._write_admissions.get(command.ticket) is admission
        assert admission.ticket is command.ticket
        assert admission.values == {"on": "1"} and admission.issued
        assert _mesh_states(rig.mqtt) == []
        assert not rig.timers()
        assert rig.sleeper.requested == [80]

        successor_release.set()
        await replacement
        await turns()
        rig.assert_pending(target="1", deadline=1085)
        current = pending(rig.bridge)[MESH_PID]
        assert current is old
        assert current.view.command_id != old_command
        assert dict(current.view.targets) == {"on": "1"}
        assert commands(rig.bridge)[MESH_PID] == generation
        assert rig.tickets[-1] is command.ticket
        assert rig.sleeper.requested == [80, 80]
        assert len(rig.timers()) == 1
        assert rig.observer.max_in_flight == 1
        assert rig.native_payloads == [{"on": "0"}, {"on": "1"}]
    finally:
        rig.observer.release.set()
        successor_release.set()
        await replacement


@pytest.mark.parametrize("revocation", ["supersede", "withdraw"])
async def test_adapter_rearm_publish_stays_cancellable(rig: Rig, revocation: str) -> None:
    rig.clock.advance(5)
    rig.mqtt.published.clear()
    rig.mqtt.block_next_state_publish = True
    await rig.accept()
    assert rig.mqtt.publish_started.is_set(), "late acceptance must reach the gated publication"
    old_timer = rig.timers()[MESH_PID]
    blocked = list(publishers(rig.bridge))
    old = pending(rig.bridge)[MESH_PID]
    old_command = old.view.command_id
    assert blocked
    if revocation == "withdraw":
        await rig.bridge.withdraw()
    else:
        rig.clock.advance(1)
        await rig.mqtt.inject(MESH_SET_TOPIC, '{"state":"ON"}')
    await turns()
    assert not rig.mqtt.release_publish.is_set()
    assert all(task.cancelled() for task in blocked)
    assert old_timer.done()
    if revocation == "withdraw":
        assert not pending(rig.bridge)
        assert not rig.timers()
    else:
        current = pending(rig.bridge)[MESH_PID]
        assert current is old
        assert current.view.command_id != old_command
        assert dict(current.view.targets) == {"on": "1"}
        assert rig.timers()[MESH_PID] is not old_timer
        assert not rig.timers()[MESH_PID].done()
    rig.mqtt.release_publish.set()
    await turns()
    assert not any('"on": "0"' in payload for _, payload, _ in _mesh_states(rig.mqtt))
    if revocation == "withdraw":
        await rig.expire()
        assert not rig.timers()
    else:
        assert pending(rig.bridge)[MESH_PID] is current


async def test_pre_acceptance_match_delivered_near_expiry_cannot_confirm(rig: Rig) -> None:
    rig.set_on("0")
    captured = await rig.adapter.get_all()
    rig.clock.advance(5)
    await rig.accept()
    rig.clock.advance(79)
    await rig.bridge.poll_once(captured)
    rig.assert_pending()
    assert last_observed(pending(rig.bridge)[MESH_PID]) is None
    await rig.expire(1)
    rig.assert_terminal("unconfirmed", None)
    assert not rig.timers()


async def test_pre_acceptance_push_delayed_until_after_acceptance(
    rig: Rig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered, release = asyncio.Event(), asyncio.Event()
    original = rig.adapter._drain_pushes

    async def gated(
        key: tuple[str | None, Callable[[BrilliantDevice], Awaitable[None]]],
        callback: Callable[[BrilliantDevice], Awaitable[None]],
        session: int,
    ) -> None:
        entered.set()
        await release.wait()
        await original(key, callback, session)

    monkeypatch.setattr(rig.adapter, "_drain_pushes", gated)
    try:
        await rig.notify()
        await asyncio.wait_for(entered.wait(), 1)
        rig.clock.advance(5)
        await rig.accept()
        release.set()
        await turns()
        rig.assert_pending(deadline=1085)
        assert last_observed(pending(rig.bridge)[MESH_PID]) is None
    finally:
        release.set()


async def test_matching_push_at_expiry_cannot_confirm(rig: Rig) -> None:
    await rig.accept()
    rig.clock.advance(80)
    await rig.report("0")
    rig.assert_pending()
    assert last_observed(pending(rig.bridge)[MESH_PID]) is None
    await rig.sleeper.release_all()
    await turns()
    rig.assert_terminal("unconfirmed", None)


async def test_reconnect_new_generation_cannot_echo_old_matching_off(rig: Rig) -> None:
    await rig.accept()
    rig.clock.advance(75)
    await rig.report("0")
    rig.adapter.on_reconnect(rig.bridge.reconcile)
    rig.set_on("1")
    rig.adapter._on_proc_reconnect()
    await turns(30)
    await rig.report("1")
    before = len(_mesh_states(rig.mqtt))
    await rig.expire(5)
    assert rig.state()["state"] == "ON"
    assert not any('"state": "OFF"' in payload for _, payload, _ in _mesh_states(rig.mqtt)[before:])


@pytest.mark.parametrize("resolver_order", ["before_observation", "after_observation"])
async def test_fresh_contradiction_at_expiry_cannot_be_overwritten(
    rig: Rig,
    resolver_order: str,
) -> None:
    await rig.accept()
    rig.clock.advance(75)
    await rig.report("0")
    rig.clock.advance(5)
    if resolver_order == "before_observation":
        await rig.sleeper.release_all()
        await turns()
    await rig.report("1")
    assert rig.state()["state"] == "ON"
    before = len(_mesh_states(rig.mqtt))
    if resolver_order == "after_observation":
        await rig.sleeper.release_all()
        await turns()
    assert not pending(rig.bridge)
    assert rig.state()["state"] == "ON"
    assert not any('"state": "OFF"' in payload for _, payload, _ in _mesh_states(rig.mqtt)[before:])


async def test_early_post_acceptance_capture_delivered_at_79_is_stale_at_80(rig: Rig) -> None:
    await rig.accept()
    rig.clock.advance(1)
    rig.set_on("0")
    captured = await rig.adapter.get_all()
    rig.clock.advance(78)
    await rig.bridge.poll_once(captured)
    await rig.expire(1)
    rig.assert_terminal("unconfirmed", None)
    assert not rig.timers()


@pytest.mark.parametrize("older_on,newer_on", [("0", "1"), ("1", "0")])
async def test_older_same_generation_capture_cannot_replace_newer_evidence(
    rig: Rig,
    older_on: str,
    newer_on: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await rig.accept()
    rig.clock.advance(74)
    rig.set_on(older_on)
    from copy import deepcopy

    old_raw = deepcopy(rig.raw)
    entered, release = asyncio.Event(), asyncio.Event()
    original_read = rig.observer.get_device

    async def delayed_read(device_id: str) -> _RawDevice:
        if device_id == "ble_mesh" and not entered.is_set():
            entered.set()
            await release.wait()
            return old_raw
        return await original_read(device_id)

    monkeypatch.setattr(rig.observer, "get_device", delayed_read)
    old_task = asyncio.create_task(rig.adapter.get_all())
    await entered.wait()
    rig.clock.advance(1)
    rig.set_on(newer_on)
    newer = await rig.adapter.get_all()
    release.set()
    older = await old_task
    old_provenance, new_provenance = older[0].capture_provenance, newer[0].capture_provenance
    assert old_provenance is not None and new_provenance is not None
    assert old_provenance.source_generation == new_provenance.source_generation
    assert old_provenance.sequence < new_provenance.sequence
    await rig.bridge.poll_once(newer)
    rig.clock.advance(4)
    await rig.bridge.poll_once(older)
    if newer_on == "0":
        rig.assert_pending()
        assert last_observed(pending(rig.bridge)[MESH_PID]) == 75
    else:
        rig.assert_terminal("contradicted", "ON")
    await rig.expire(1)
    rig.assert_terminal(
        "idle" if newer_on == "0" else "contradicted", "OFF" if newer_on == "0" else "ON"
    )


async def test_older_source_generation_capture_cannot_replace_newer_evidence(rig: Rig) -> None:
    await rig.accept()
    rig.clock.advance(1)
    rig.set_on("0")
    older = await rig.adapter.get_all()
    rig.adapter.on_reconnect(rig.bridge.reconcile)
    rig.set_on("1")
    rig.adapter._on_proc_reconnect()
    await turns(30)
    newer = await rig.adapter.get_all()
    old_provenance, new_provenance = older[0].capture_provenance, newer[0].capture_provenance
    assert old_provenance is not None and new_provenance is not None
    assert old_provenance.source_generation < new_provenance.source_generation
    await rig.bridge.poll_once(newer)
    await rig.bridge.poll_once(older)
    assert rig.state()["state"] == "ON"


async def test_complete_capture_missing_on_at_expiry_cannot_echo_old_match(rig: Rig) -> None:
    await rig.accept()
    rig.clock.advance(75)
    await rig.report("0")
    rig.clock.advance(5)
    rig.set_on(None)
    captured = await rig.adapter.get_all()
    assert "on" not in captured[0].variables
    await rig.bridge.poll_once(captured)
    before = len(_mesh_states(rig.mqtt))
    await rig.sleeper.release_all()
    await turns()
    rig.assert_terminal("unconfirmed", None)
    assert not any('"state": "OFF"' in payload for _, payload, _ in _mesh_states(rig.mqtt)[before:])


async def test_old_source_capture_cannot_contradict_new_acceptance(rig: Rig) -> None:
    await rig.accept()
    rig.clock.advance(1)
    older = await rig.adapter.get_all()
    rig.adapter.on_reconnect(rig.bridge.reconcile)
    rig.adapter._on_proc_reconnect()
    await turns(30)
    await rig.mqtt.inject(MESH_SET_TOPIC, '{"state":"OFF"}')
    await turns()
    rig.clock.advance(75)
    await rig.report("0")
    await rig.bridge.poll_once(older)
    rig.assert_pending()
    assert last_observed(pending(rig.bridge)[MESH_PID]) == rig.clock()
    await rig.expire(5)
    rig.assert_terminal("idle", "OFF")


async def test_normal_success_preserves_native_receipt_and_acceptance_fence(
    rig: Rig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await rig.accept()
    rig.clock.advance(1)
    captured = await rig.adapter.get_all()
    rig.clock.advance(4)
    monkeypatch.setattr(bus_mod, "_WRITE_DEADLINE_S", 1)
    command = CommandAdmission()
    token = command_admission.set(command)
    try:
        await rig.mqtt.inject(MESH_SET_TOPIC, '{"state":"OFF"}')
    finally:
        command_admission.reset(token)
    record = pending(rig.bridge)[MESH_PID]
    accepted = command.ticket.accepted_at
    assert record.view.return_boundary is not None
    assert accepted is not None
    assert (
        record.view.return_boundary.sequence,
        record.view.return_boundary.captured_at,
        record.view.return_boundary.source.incarnation,
        record.view.return_boundary.generation,
    ) == (accepted.sequence, accepted.captured_at, accepted.incarnation, accepted.source_generation)
    ticket_accepted = getattr(command.ticket, "accepted_at", None)
    resolved = record.view.phase is Phase.UNRESOLVED
    assert record.view.receipt == "'ok'"
    await rig.bridge.poll_once(captured)
    rig.assert_pending(deadline=1085)
    assert pending(rig.bridge)[MESH_PID] is record
    assert last_observed(record) is None
    assert rig.tickets[-1] is command.ticket
    assert resolved is False
    assert settlement(command.ticket).settled
    assert isinstance(accepted, CaptureProvenance)
    assert accepted == ticket_accepted
    provenance = captured[0].capture_provenance
    assert provenance is not None
    assert provenance.source_generation == accepted.source_generation
    assert provenance.sequence < accepted.sequence


async def test_native_acceptance_before_caller_timeout_preserves_receipt_and_fence(
    rig: Rig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await rig.accept()
    rig.clock.advance(1)
    captured = await rig.adapter.get_all()
    rig.clock.advance(4)
    deferred: list[tuple[bus_mod._WriteAdmission, asyncio.Task[str]]] = []
    boundary: list[tuple[object, str, bool, object]] = []
    original = rig.adapter._finish_admission

    def hold(admission: bus_mod._WriteAdmission, native: asyncio.Task[str]) -> None:
        assert native.done() and native.result() == "'ok'"
        assert not admission.result.done()
        # Record missing future facts as missing, rather than raising inside
        # the callback or substituting settlement after the caller wakes.
        boundary.append(
            (
                getattr(admission.ticket, "settled", None),
                native.result(),
                admission.result.done(),
                getattr(admission.ticket, "accepted_at", None),
            )
        )
        deferred.append((admission, native))

    monkeypatch.setattr(rig.adapter, "_finish_admission", hold)
    try:
        await rig.mqtt.inject(MESH_SET_TOPIC, '{"state":"OFF"}')
        assert len(deferred) == 1
        admission, native = deferred[0]
        assert admission.record.detached and not admission.result.done()
        record = pending(rig.bridge)[MESH_PID]
        assert record.view.receipt == native.result()
        await rig.bridge.poll_once(captured)
        rig.assert_pending(deadline=1085)
        assert pending(rig.bridge)[MESH_PID] is record
        assert last_observed(record) is None
        assert admission.ticket is rig.tickets[-1]
        assert len(boundary) == 1
        settled, receipt, result_done, accepted = boundary[0]
        assert (settled, receipt, result_done) == (True, "'ok'", False)
        assert isinstance(accepted, CaptureProvenance)
        assert record.view.return_boundary is not None
        assert (
            record.view.return_boundary.sequence,
            record.view.return_boundary.captured_at,
            record.view.return_boundary.source.incarnation,
            record.view.return_boundary.generation,
        ) == (
            accepted.sequence,
            accepted.captured_at,
            accepted.incarnation,
            accepted.source_generation,
        )
        assert record.view.phase is Phase.ACCEPTED
        assert record.view.outcome is OutcomeKind.ACCEPTED
        provenance = captured[0].capture_provenance
        assert provenance is not None
        assert provenance.source_generation == accepted.source_generation
        assert provenance.sequence < accepted.sequence
    finally:
        for admission, native in deferred:
            original(admission, native)


@pytest.mark.parametrize("queued", [False, True], ids=["sleeping", "expiry_already_ready"])
async def test_shutdown_fences_obsolete_expiry_and_cleans_workers(rig: Rig, queued: bool) -> None:
    timer = rig.timers()[MESH_PID]
    rig.clock.advance(80)
    if queued:
        # Queue expiry without giving it a turn before synchronous revocation.
        for future in list(rig.sleeper._waiters):
            if not future.done():
                future.set_result(None)
    await rig.bridge.shutdown_mesh_feedback()
    rig.mqtt.published.clear()
    await rig.sleeper.release_all()
    await turns()
    assert rig.mqtt.published == []
    assert timer.done()
    assert not rig.timers()
    assert not publishers(rig.bridge)


async def test_resolver_losing_ownership_releases_its_entry(rig: Rig) -> None:
    await rig.bridge.shutdown_mesh_feedback()
    rig.mqtt.published.clear()
    await rig.expire()
    assert rig.mqtt.published == []
    assert not rig.timers()


async def test_settle_task_losing_ownership_clears_its_registry_entry(rig: Rig) -> None:
    rig.observer.release.set()
    await rig.bridge.shutdown_mesh_feedback()
    rig.mqtt.published.clear()
    await turns()
    await rig.expire()
    assert rig.mqtt.published == []
    assert not rig.timers()
    assert not publishers(rig.bridge)


async def test_views_are_read_only(rig: Rig) -> None:
    registry = rig.bridge.mesh_registry
    assert registry is not None
    owner = registry.current(MESH_PID)
    assert owner is not None
    assert isinstance(registry.retired, MappingProxyType)
    with pytest.raises(TypeError):
        cast(MutableMapping[str, object], registry.retired)["foreign"] = object()
    with pytest.raises(FrozenInstanceError):
        field_name = "command_id"
        setattr(owner.view, field_name, -1)
    with pytest.raises(TypeError):
        cast(MutableMapping[str, object], owner.render(rig.clock()))["state"] = "ON"
    assert isinstance(owner.view.targets, tuple)
    assert isinstance(owner.view.evidence, tuple)


def gate_publish(
    rig: Rig,
    monkeypatch: pytest.MonkeyPatch,
    predicate: Callable[[str], bool],
) -> tuple[asyncio.Event, asyncio.Event]:
    entered, release = asyncio.Event(), asyncio.Event()
    original = rig.mqtt.publish

    async def publish(topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        if not entered.is_set() and predicate(topic):
            entered.set()
            await release.wait()
        await original(topic, payload, retain, qos)

    monkeypatch.setattr(rig.mqtt, "publish", publish)
    return entered, release


async def test_acquisition_push_cannot_skip_discovery_and_command_route(
    rig: Rig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await rig.accept()
    await rig.bridge.withdraw()
    rig.mqtt.published.clear()
    entered, release = gate_publish(rig, monkeypatch, lambda topic: "availability" in topic)
    task = asyncio.create_task(rig.bridge.reconcile())
    try:
        await asyncio.wait_for(entered.wait(), 1)
        await rig.report("1")
    finally:
        release.set()
        await task
    assert MESH_SET_TOPIC in rig.bridge._subscribed
    assert any(topic.endswith("/config") for topic, _, _ in rig.mqtt.published)
    # The installed route must be usable without a second resync.
    before = len(rig.observer.writes)
    await rig.mqtt.inject(MESH_SET_TOPIC, '{"state":"OFF"}')
    assert len(rig.observer.writes) == before + 1


async def test_reconcile_must_not_republish_old_snapshot_after_newer_contradiction(
    rig: Rig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await rig.accept()
    rig.set_on("0")
    entered, release = gate_publish(rig, monkeypatch, lambda topic: topic.endswith("/config"))
    task = asyncio.create_task(rig.bridge.reconcile())
    try:
        await asyncio.wait_for(entered.wait(), 1)
        await rig.report("1")
        rig.assert_terminal("contradicted", "ON")
    finally:
        release.set()
        await task
    assert rig.state()["state"] == "ON"


async def test_reconnect_cannot_confirm_while_reconciliation_is_blocked(
    rig: Rig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await rig.accept()
    rig.clock.advance(75)
    await rig.report("0")
    entered, release = asyncio.Event(), asyncio.Event()

    async def resubscribe() -> None:
        entered.set()
        await release.wait()

    monkeypatch.setattr(rig.adapter, "_resubscribe", resubscribe)
    rig.adapter.on_reconnect(rig.bridge.reconcile)
    rig.set_on("1")
    rig.adapter._on_proc_reconnect()
    try:
        await asyncio.wait_for(entered.wait(), 1)
        await rig.expire(5)
        assert rig.state()["state"] is None
        assert rig.state()["mesh_write_status"] == "unconfirmed"
    finally:
        release.set()
        await turns(30)


async def test_native_success_cannot_lose_post_return_push_before_waiter_arms(
    rig: Rig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await rig.accept()
    reported: list[asyncio.Task[None]] = []

    async def native_return() -> None:
        asyncio.get_running_loop().call_soon(
            lambda: reported.append(asyncio.create_task(rig.notify()))
        )

    monkeypatch.setattr(rig.observer, "_block", native_return)
    monkeypatch.setattr(bus_mod, "_WRITE_DEADLINE_S", 1)
    await rig.mqtt.inject(MESH_SET_TOPIC, '{"state":"OFF"}')
    await turns()
    await asyncio.gather(*reported)
    rig.assert_terminal("contradicted", "ON")


@pytest.mark.parametrize("stale_first", [True, False], ids=["before", "after"])
async def test_abandoned_reused_ticket_never_settles_the_reissue(
    rig: Rig, monkeypatch: pytest.MonkeyPatch, stale_first: bool
) -> None:
    stale = rig.tickets[0]
    if stale_first:
        await rig.accept()
    fresh_gate = asyncio.Event()

    async def fresh_native() -> None:
        await fresh_gate.wait()
        raise RuntimeError("fresh rejection")

    monkeypatch.setattr(rig.observer, "_block", fresh_native)
    token = command_admission.set(CommandAdmission(ticket=stale))
    try:
        command = asyncio.create_task(rig.mqtt.inject(MESH_SET_TOPIC, '{"state":"OFF"}'))
    finally:
        command_admission.reset(token)
    await turns()
    # Production rejects reuse and the bridge retries admission on a fresh
    # identity. No obsolete MeshPending helper or fake ticket rejection.
    assert len(rig.tickets) >= 3
    assert rig.tickets[-2] is stale
    fresh = rig.tickets[-1]
    assert fresh is not stale
    if not stale_first:
        rig.observer.release.set()
    await command
    await turns()
    assert rig.native_payloads == [{"on": "0"}, {"on": "0"}]
    assert rig.observer.max_in_flight == 1
    current = pending(rig.bridge)[MESH_PID]
    rig.assert_pending()
    # The old completion must not make the fresh unresolved request eligible
    # for contradiction; only the fresh native rejection may settle it.
    before = len(_mesh_states(rig.mqtt))
    try:
        await rig.report("1")
        rig.assert_pending()
        assert pending(rig.bridge).get(MESH_PID) is current
        assert len(_mesh_states(rig.mqtt)) == before
        assert len(rig.timers()) == 1
        fresh_gate.set()
        await turns()
        rig.assert_terminal("failed", "ON")
        assert not rig.timers()
        assert settlement(fresh).settled
    finally:
        fresh_gate.set()


async def test_fast_native_timeout_is_contradicted_within_one_poll(
    rig: Rig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await rig.accept()
    rig.observer._fail_with = TimeoutError("No response received!")
    monkeypatch.setattr(bus_mod, "_WRITE_DEADLINE_S", 1)
    await rig.mqtt.inject(MESH_SET_TOPIC, '{"state":"OFF"}')
    rig.assert_pending()
    assert rig.observer.in_flight == 0
    before = len(_mesh_states(rig.mqtt))
    await rig.report("1")
    rig.assert_terminal("contradicted", "ON")
    assert len(_mesh_states(rig.mqtt)) == before + 1
    assert not rig.timers()
    await rig.expire()
    assert len(_mesh_states(rig.mqtt)) == before + 1


@pytest.mark.parametrize(
    "outcome",
    [asyncio.TimeoutError(), TimeoutError("native"), asyncio.CancelledError(), WriteCancelled()],
    ids=["asyncio_timeout", "native_timeout", "cancelled", "write_cancelled"],
)
@pytest.mark.parametrize("fresh", [False, True], ids=["pre_return_match", "post_return_match"])
async def test_inconclusive_outcome_uses_return_fence_and_original_expiry(
    rig: Rig,
    monkeypatch: pytest.MonkeyPatch,
    outcome: BaseException,
    fresh: bool,
) -> None:
    await rig.accept()
    rig.clock.advance(1)
    rig.observer.release.clear()

    async def native_outcome() -> None:
        await rig.observer.release.wait()
        raise outcome

    monkeypatch.setattr(rig.observer, "_block", native_outcome)
    await rig.mqtt.inject(MESH_SET_TOPIC, '{"state":"OFF"}')
    rig.set_on("0")
    old_match = await rig.adapter.get_all()
    timer = rig.timers()[MESH_PID]
    rig.clock.advance(4)
    rig.observer.release.set()
    await turns()
    rig.assert_pending(deadline=1081)
    assert rig.timers()[MESH_PID] is timer
    rig.clock.advance(71)
    await rig.bridge.poll_once(old_match)
    if fresh:
        await rig.report("0")
    rig.assert_pending(deadline=1081)
    await rig.expire(5)
    rig.assert_terminal("idle" if fresh else "unconfirmed", "OFF" if fresh else None)
    assert not rig.timers()
