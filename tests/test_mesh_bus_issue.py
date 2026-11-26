"""Actual adapter issue/return schedules against the synchronous mesh owner."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import replace

import pytest

from brilliant_mqtt import bus as bus_mod
from brilliant_mqtt.bus import RpcBusAdapter
from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.mesh_settlement import MeshSettlement
from brilliant_mqtt.mesh_settlement_types import (
    Begin,
    Envelope,
    Issue,
    NativeOutcome,
    OutcomeKind,
    Phase,
    Revoke,
    Source,
)
from brilliant_mqtt.model import CaptureProvenance
from brilliant_mqtt.write_admission import AdmissionTicket, WriteCancelled
from tests.fakes import _StartHarness
from tests.test_bus_adapter import _gated_adapter
from tests.test_mesh_bus_reconnect import foreign, owner_for
from tests.test_write_admission import _Harness, _settle


def envelope(owner: MeshSettlement, boundary: CaptureProvenance) -> Envelope:
    return Envelope(
        owner,
        Source(boundary.incarnation, boundary.origin),
        boundary.source_generation,
        boundary.sequence,
        boundary.captured_at,
        owner.metadata.peripheral_id,
    )


def bind(
    adapter: RpcBusAdapter,
    owner: MeshSettlement,
    *,
    prepare: Callable[[], None] = lambda: None,
) -> AdmissionTicket:
    ticket = AdmissionTicket()
    begin = adapter._next_capture_provenance("mqtt")
    owner.dispatch(Begin(envelope(owner, begin), ticket, (("on", "0"),)))
    command_id = owner.view.command_id

    def issue(boundary: CaptureProvenance) -> None:
        owner.dispatch(Issue(envelope(owner, boundary), command_id, ticket, (("on", "0"),)))
        prepare()

    def settle(outcome: str | BaseException) -> None:
        boundary = ticket.returned_at
        if boundary is None:  # definite preissue refusal has its own lifecycle event
            return
        owner.dispatch(
            NativeOutcome(
                envelope(owner, boundary),
                command_id,
                ticket,
                OutcomeKind.ACCEPTED if isinstance(outcome, str) else OutcomeKind.REJECTED,
                outcome if isinstance(outcome, str) else None,
            )
        )

    def refuse() -> None:
        boundary = replace(
            adapter._next_capture_provenance("lifecycle"), source_generation=owner.generation
        )
        owner.dispatch(Revoke(envelope(owner, boundary)))

    adapter.bind_mesh_ticket(
        ticket,
        owner,
        on_issue=issue,
        on_settle=settle,
        on_refused=refuse,
    )
    return ticket


@pytest.mark.parametrize("on_thread", [False, True], ids=["loop", "foreign"])
async def test_hook_wins_during_issue_preparation_never_marks_actual_issue(
    monkeypatch: pytest.MonkeyPatch,
    on_thread: bool,
) -> None:
    harness = _StartHarness(monkeypatch, gated_writes=True)
    adapter = RpcBusAdapter()
    await adapter.start()
    owner = owner_for(adapter)
    hook = harness.procs[0].reconnect_cbs[0]
    ticket = bind(adapter, owner, prepare=lambda: foreign(hook) if on_thread else hook())
    admission_seen: list[object] = []
    ticket.set_settle_callback(lambda _outcome: admission_seen.append(ticket.issued_at))
    try:
        with pytest.raises(WriteCancelled):
            await adapter.set_variables("ble_mesh", "p", [VarSet("on", "0")], ticket=ticket)
        assert harness.write_starts == []
        assert ticket.issued_at is None
        assert ticket.returned_at is None
        assert isinstance(ticket.outcome, WriteCancelled)
        assert owner.view.phase is Phase.INACTIVE
        assert admission_seen == [None]
        assert adapter.outstanding_mesh_ticket("p") is None
    finally:
        harness.write_release.set()
        await adapter.shutdown()


async def test_actual_foreign_hook_fences_write_already_waiting_for_device_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _StartHarness(monkeypatch, gated_writes=True)
    adapter = RpcBusAdapter()
    await adapter.start()
    lock = adapter._write_locks.setdefault("ble_mesh", asyncio.Lock())
    await lock.acquire()
    owner = owner_for(adapter)
    ticket = bind(adapter, owner)
    caller = asyncio.create_task(
        adapter.set_variables("ble_mesh", "p", [VarSet("on", "0")], ticket=ticket)
    )
    await _settle()
    foreign(harness.procs[0].reconnect_cbs[0])
    lock.release()
    try:
        with pytest.raises(asyncio.CancelledError):
            await caller
        assert harness.write_starts == []
        assert ticket.issued_at is None
        assert ticket.returned_at is None
    finally:
        harness.write_release.set()
        await adapter.shutdown()


async def test_authorized_issue_retains_physical_lock_after_hook_and_worker_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _StartHarness(monkeypatch, gated_writes=True)
    adapter = RpcBusAdapter()
    await adapter.start()
    owner = owner_for(adapter)
    ticket = bind(adapter, owner)
    caller = asyncio.create_task(
        adapter.set_variables("ble_mesh", "p", [VarSet("on", "0")], ticket=ticket)
    )
    await _settle()
    assert ticket.issued_at is not None
    assert adapter.outstanding_mesh_ticket("p") is ticket
    foreign(harness.procs[0].reconnect_cbs[0])
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller
    assert not ticket.settled
    assert adapter._write_locks["ble_mesh"].locked()
    assert adapter.outstanding_mesh_ticket("p") is ticket
    harness.write_release.set()
    await asyncio.gather(*adapter._write_tasks)
    assert ticket.settled
    assert ticket.returned_at is not None
    assert adapter.mesh_creation_floor == ticket.returned_at.sequence
    assert adapter.outstanding_mesh_ticket("p") is None
    await adapter.shutdown()


async def test_mesh_issue_sink_exception_fails_closed_and_wired_is_best_effort() -> None:
    harness = _Harness()
    owner = owner_for(harness.adapter)

    def fail() -> None:
        raise RuntimeError("private")

    ticket = bind(harness.adapter, owner, prepare=fail)
    with pytest.raises(WriteCancelled):
        await harness.adapter.set_variables("ble_mesh", "p", [VarSet("on", "0")], ticket=ticket)
    assert harness.observer.calls == []
    assert ticket.issued_at is None
    assert not harness.adapter.mesh_admission_open
    assert harness.adapter.consume_mesh_fatal()
    assert not harness.adapter.consume_mesh_fatal()
    assert not harness.adapter.mesh_admission_open
    wired = AdmissionTicket()
    wired.set_issue_callback(lambda _boundary: fail())
    harness.observer.release_all = True
    assert (
        await harness.adapter.set_variables("own-device", "p", [VarSet("on", "0")], ticket=wired)
        == "'ok'"
    )
    await harness.adapter.shutdown()


@pytest.mark.parametrize("outcome", ["ok", "rejected", "timeout"])
async def test_native_outcome_is_stored_before_finish_waiter_and_lock_release(
    monkeypatch: pytest.MonkeyPatch,
    outcome: str,
) -> None:
    error = {"ok": None, "rejected": RuntimeError("reject"), "timeout": asyncio.TimeoutError()}[
        outcome
    ]
    observer, adapter = _gated_adapter(fail_with=error)
    ticket = AdmissionTicket()
    seen: list[tuple[bool, bool, int]] = []
    ticket.set_settle_callback(
        lambda _raw: seen.append(
            (
                adapter._write_locks["ble_mesh"].locked(),
                ticket.returned_at is not None,
                adapter.mesh_creation_floor,
            )
        )
    )
    held: list[tuple[bus_mod._WriteAdmission, asyncio.Task[str]]] = []
    original = adapter._finish_admission
    monkeypatch.setattr(
        adapter, "_finish_admission", lambda admission, task: held.append((admission, task))
    )
    caller = asyncio.create_task(
        adapter.set_variables("ble_mesh", "p", [VarSet("on", "0")], ticket=ticket)
    )
    await _settle()
    observer.release.set()
    await asyncio.gather(*adapter._write_tasks, return_exceptions=True)
    await _settle()
    assert ticket.returned_at is not None
    assert seen == [(True, True, ticket.returned_at.sequence)]
    assert ticket.outcome == "'ok'" if error is None else ticket.outcome is error
    assert not held[0][0].result.done()
    assert not caller.done()
    original(*held[0])
    if error is None:
        assert await caller == "'ok'"
    else:
        with pytest.raises(type(error)):
            await caller
    await adapter.shutdown()


async def test_waiting_cancellation_has_no_return_boundary_or_rpc() -> None:
    harness = _Harness()
    await harness.queue("blocker", {"on": "1"})
    ticket = AdmissionTicket()
    caller = await harness.queue("p", {"on": "0"}, ticket=ticket)
    ticket.cancel_waiting()
    assert ticket.settled
    assert ticket.issued_at is None
    assert ticket.returned_at is None
    with pytest.raises(asyncio.CancelledError):
        await caller
    assert len(harness.observer.calls) == 1
    await harness.release(0)
    await harness.adapter.shutdown()


async def test_owner_settlement_dispatch_failure_latches_adapter_fatal() -> None:
    observer, adapter = _gated_adapter()
    ticket = AdmissionTicket()
    owner = owner_for(adapter)

    def fail(_outcome: str | BaseException) -> None:
        raise RuntimeError("dispatch")

    adapter.bind_mesh_ticket(
        ticket, owner, on_issue=lambda _boundary: None, on_settle=fail, on_refused=lambda: None
    )
    observer.release.set()
    assert (
        await adapter.set_variables("ble_mesh", "p", [VarSet("on", "0")], ticket=ticket) == "'ok'"
    )
    assert adapter.consume_mesh_fatal()
    assert not adapter.mesh_owner_live(owner)
    await adapter.shutdown()


async def test_teardown_native_cancellation_settles_before_waiter_future() -> None:
    _, adapter = _gated_adapter()
    ticket = AdmissionTicket()
    caller = asyncio.create_task(
        adapter.set_variables("ble_mesh", "p", [VarSet("on", "0")], ticket=ticket)
    )
    await _settle()
    admission = adapter._write_serving["ble_mesh"]
    seen: list[bool] = []
    ticket.set_settle_callback(lambda _outcome: seen.append(admission.result.done()))
    await adapter.shutdown()
    assert seen == [False]
    assert isinstance(ticket.outcome, WriteCancelled)
    assert ticket.returned_at is not None
    with pytest.raises(asyncio.CancelledError):
        await caller


async def test_guard_disabled_during_preparation_refuses_once_without_replay() -> None:
    from brilliant_mqtt.write_admission import TicketAdoptionError

    harness = _Harness()
    owner = owner_for(harness.adapter)
    enabled = True

    def disable() -> None:
        nonlocal enabled
        enabled = False

    remove = harness.adapter.register_mesh_write_guard(lambda _ticket, _pid: enabled)
    ticket = bind(harness.adapter, owner, prepare=disable)
    with pytest.raises(WriteCancelled):
        await harness.adapter.set_variables("ble_mesh", "p", [VarSet("on", "0")], ticket=ticket)
    assert ticket.issued_at is None
    assert ticket.returned_at is None
    assert harness.observer.calls == []
    enabled = True
    with pytest.raises(TicketAdoptionError):
        await harness.adapter.set_variables("ble_mesh", "p", [VarSet("on", "0")], ticket=ticket)
    remove()
    remove()
    await harness.adapter.shutdown()


async def test_preparation_can_tombstone_waiting_admission_without_native_call() -> None:
    harness = _Harness()
    ticket = AdmissionTicket()
    ticket.set_issue_callback(lambda _boundary: ticket.cancel_waiting())
    with pytest.raises(WriteCancelled):
        await harness.adapter.set_variables("ble_mesh", "p", [VarSet("on", "0")], ticket=ticket)
    assert ticket.issued_at is None
    assert ticket.returned_at is None
    assert harness.observer.calls == []
    await harness.adapter.shutdown()


async def test_bounded_teardown_detaches_straggler_waiter_without_native_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(bus_mod, "_WRITE_SETTLE_TIMEOUT_S", 0.01)
    harness = _Harness()
    harness.observer.delay_cancellation = True
    ticket = AdmissionTicket()
    caller = await harness.queue("p", {"on": "0"}, ticket=ticket)
    await harness.adapter.shutdown()
    with pytest.raises(asyncio.CancelledError):
        await caller
    assert not ticket.settled
    assert ticket.returned_at is None
    assert harness.adapter.outstanding_mesh_ticket("p") is ticket
    assert harness.adapter._write_locks["ble_mesh"].locked()
    seen: list[str | BaseException] = []
    ticket.set_settle_callback(seen.append)
    await harness.release(0)
    assert seen == ["'ok'"]
    assert ticket.returned_at is not None
    assert harness.adapter.outstanding_mesh_ticket("p") is None
    assert not harness.adapter._write_locks["ble_mesh"].locked()
