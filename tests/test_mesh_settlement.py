"""Pure owner schedules: captures, native outcomes, retirement and publication debt."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import FrozenInstanceError, replace
from itertools import permutations
from typing import cast

import pytest

from brilliant_mqtt.mesh_settlement import MeshSettlement
from brilliant_mqtt.mesh_settlement_types import (
    AuxAccepted,
    AuxCancelled,
    AuxIssue,
    AuxRejected,
    Begin,
    CallerTimeout,
    Completion,
    Envelope,
    Expire,
    Issue,
    MeshMetadata,
    NativeOutcome,
    Observe,
    OutcomeKind,
    PeripheralAbsent,
    Phase,
    PublishAccepted,
    PublishFailed,
    PublishStarted,
    Revoke,
    RevokeEffects,
    ScopeUnknown,
    SeedBinding,
    Source,
    WorkerCancelled,
)
from brilliant_mqtt.model import DeviceKind, PrimitiveCapture, PrimitiveField


def owner() -> MeshSettlement:
    return MeshSettlement(
        MeshMetadata("ble_mesh", "load", "Load", DeviceKind.LIGHT, 27),
        source="adapter",
        generation=1,
        birth_sequence=1,
        born_at=0,
    )


def env(slot: MeshSettlement, seq: int, at: float) -> Envelope:
    return Envelope(slot, Source("adapter", "test"), 1, seq, at, "load", 1000 + at)


def observe(
    slot: MeshSettlement,
    seq: int,
    at: float,
    *,
    complete: bool = False,
    reliable: bool = True,
    reported: frozenset[str] | None = None,
    removed: frozenset[str] = frozenset(),
    fields: Mapping[str, str] | None = None,
    **values: str,
) -> None:
    merged = dict(fields or {}) | values
    slot.dispatch(
        Observe(
            env(slot, seq, at),
            PrimitiveCapture(
                tuple(PrimitiveField(k, v) for k, v in merged.items()),
                complete,
                reliable,
                reported if reported is not None else frozenset(merged),
                removed,
            ),
        )
    )


def begin(
    slot: MeshSettlement,
    *,
    seq: int = 2,
    at: float = 1,
    targets: tuple[tuple[str, str], ...] = (("on", "1"),),
) -> object:
    ticket = object()
    slot.dispatch(Begin(env(slot, seq, at), ticket, targets))
    return ticket


def issue(slot: MeshSettlement, ticket: object, seq: int = 3, at: float = 2) -> None:
    slot.dispatch(Issue(env(slot, seq, at), slot.view.command_id, ticket, slot.view.targets))


def outcome(
    slot: MeshSettlement,
    ticket: object,
    kind: OutcomeKind = OutcomeKind.ACCEPTED,
    seq: int = 4,
    at: float = 3,
) -> None:
    slot.dispatch(NativeOutcome(env(slot, seq, at), slot.view.command_id, ticket, kind))


def accepted() -> tuple[MeshSettlement, object]:
    slot = owner()
    ticket = begin(slot)
    issue(slot, ticket)
    outcome(slot, ticket)
    return slot, ticket


def expire(slot: MeshSettlement, seq: int, at: float) -> tuple[object, ...]:
    window = slot.view.window
    assert window is not None
    return slot.dispatch(Expire(env(slot, seq, at), window, at))


def test_acquisition_unknown_and_immutable_views() -> None:
    slot = owner()
    assert slot.render(0)["state"] is None
    with pytest.raises(FrozenInstanceError):
        attribute = "phase"
        setattr(slot.view, attribute, Phase.ACCEPTED)
    with pytest.raises(TypeError):
        cast(dict[str, object], slot.render(0))["state"] = "ON"
    observe(slot, 2, 1, on="0", power="100")
    assert slot.render(1)["state"] == "OFF"
    assert slot.render(1)["power"] == 10


@pytest.mark.parametrize("order", list(permutations((0, 1, 2))))
def test_disjoint_fields_merge_without_peripheral_watermark(order: tuple[int, ...]) -> None:
    slot, _ = accepted()
    schedules = ((20, 20, {"on": "0"}), (21, 21, {"power": "200"}), (19, 19, {"intensity": "100"}))
    for index in order:
        seq, at, fields = schedules[index]
        observe(slot, seq, at, fields=fields)
    assert slot.render(22)["state"] == "OFF"
    assert slot.render(22)["power"] == 20
    assert dict(slot.view.observed)["intensity"].value == "100"
    assert slot.view.status == "contradicted"


@pytest.mark.parametrize("unknown", [False, True])
@pytest.mark.parametrize("newer_on", [False, True])
def test_versioned_negative_evidence_is_per_field(unknown: bool, newer_on: bool) -> None:
    slot, _ = accepted()
    observe(slot, 19, 73, on="1")
    observe(slot, 21, 75, fields=({"on": "1"} if newer_on else {"power": "100"}))
    event = ScopeUnknown if unknown else PeripheralAbsent
    slot.dispatch(event(env(slot, 20, 74)))
    expire(slot, 22, 83)
    assert slot.view.status == ("idle" if newer_on else "unconfirmed")


def test_complete_read_infers_absence_but_unreliable_mirror_cannot() -> None:
    slot, _ = accepted()
    observe(slot, 19, 74, on="1")
    observe(slot, 20, 75, reliable=False, complete=True, power="100")
    assert dict(slot.view.evidence)["on"].sequence == 19
    observe(slot, 21, 76, complete=True, power="200")
    assert "on" not in dict(slot.view.evidence)
    expire(slot, 22, 83)
    assert slot.view.status == "unconfirmed"


@pytest.mark.parametrize("change", ["owner", "source", "generation", "birth", "time"])
def test_invalid_capture_rejected_before_motion_and_fields(change: str) -> None:
    slot = owner()
    envelope = env(slot, 3, 3)
    if change == "owner":
        envelope = replace(envelope, owner=owner())
    elif change == "source":
        envelope = replace(envelope, source=Source("other", "push"))
    elif change == "generation":
        envelope = replace(envelope, generation=0)
    elif change == "birth":
        envelope = replace(envelope, sequence=1)
    else:
        envelope = replace(envelope, captured_at=float("nan"))
    slot.dispatch(Observe(envelope, PrimitiveCapture((PrimitiveField("on", "1"),), True)))
    assert slot.view.observed == ()
    assert slot.render(3)["state"] is None


def test_begin_issue_timeout_are_distinct_and_never_issued_cancel_has_no_failure() -> None:
    slot = owner()
    ticket = begin(slot)
    initial = slot.view
    assert initial.phase is Phase.ISSUING
    assert slot.view.window is None
    slot.dispatch(WorkerCancelled(env(slot, 3, 2), slot.view.command_id, ticket))
    assert slot.view.phase is Phase.QUIET
    assert slot.view.status == "idle"
    assert slot.view.window is None


def test_late_acceptance_after_unresolved_expiry_has_fresh_full_window() -> None:
    slot = owner()
    ticket = begin(slot)
    issue(slot, ticket)
    slot.dispatch(CallerTimeout(env(slot, 4, 5), slot.view.command_id, ticket))
    old = slot.view.window
    assert old is not None and slot.view.deadline == 85
    expire(slot, 5, 85)
    old_terminal = slot.publication_token(object())
    assert old_terminal is not None
    slot.dispatch(PublishStarted(env(slot, 6, 86), old_terminal))
    effects = slot.dispatch(
        NativeOutcome(env(slot, 6, 90), slot.view.command_id, ticket, OutcomeKind.ACCEPTED)
    )
    assert slot.view.phase is Phase.ACCEPTED
    assert slot.view.deadline == 170
    assert slot.view.window != old
    assert any(isinstance(effect, RevokeEffects) for effect in effects)
    slot.dispatch(PublishAccepted(env(slot, 7, 91), old_terminal))
    assert slot.view.ordinary_debt is not None
    assert slot.render(91)["mesh_write_status"] == "pending"
    assert slot.render(91)["mesh_requested"] == {"on": "1"}


@pytest.mark.parametrize("kind", [OutcomeKind.TIMEOUT, OutcomeKind.CANCELLED])
def test_real_inconclusive_return_never_rearms_expired_window(kind: OutcomeKind) -> None:
    slot = owner()
    ticket = begin(slot)
    issue(slot, ticket)
    slot.dispatch(CallerTimeout(env(slot, 4, 5), slot.view.command_id, ticket))
    expire(slot, 5, 85)
    outcome(slot, ticket, kind, seq=6, at=90)
    assert slot.view.status == "unconfirmed"
    assert slot.view.window is None
    assert slot.view.deadline is None
    assert slot.view.return_sequence == 6


def test_rejection_after_expiry_and_newer_observation_is_not_globally_dropped() -> None:
    slot = owner()
    ticket = begin(slot)
    issue(slot, ticket)
    slot.dispatch(CallerTimeout(env(slot, 4, 5), slot.view.command_id, ticket))
    expire(slot, 5, 85)
    observe(slot, 8, 92, on="0")
    outcome(slot, ticket, OutcomeKind.REJECTED, seq=6, at=90)
    assert slot.view.status == "failed"
    assert slot.render(92)["state"] == "OFF"


@pytest.mark.parametrize(
    ("capture", "handling", "expected"),
    [
        (73, 83, "idle"),
        (72.999, 83, "unconfirmed"),
        (82, 93, "unconfirmed"),
        (83, 83, "unconfirmed"),
        (84, 84, "unconfirmed"),
    ],
)
def test_matching_only_at_expiry_with_actual_handling_age(
    capture: float,
    handling: float,
    expected: str,
) -> None:
    slot, _ = accepted()
    observe(slot, 5, capture, on="1")
    assert slot.view.status == "pending"
    assert slot.render(capture)["state"] is None
    expire(slot, 6, handling)
    assert slot.view.status == expected
    assert slot.render(handling)["state"] == ("ON" if expected == "idle" else None)


def test_every_target_required_and_pre_return_match_not_evidence() -> None:
    slot = owner()
    ticket = begin(slot, targets=(("on", "1"), ("intensity", "100")))
    issue(slot, ticket)
    observe(slot, 4, 3, on="1", intensity="100")
    outcome(slot, ticket, seq=5, at=4)
    observe(slot, 6, 80, on="1")
    expire(slot, 7, 84)
    assert slot.view.status == "unconfirmed"


def test_at_deadline_contradiction_applies_but_match_does_not() -> None:
    slot, _ = accepted()
    observe(slot, 5, 83, on="0")
    assert slot.view.status == "contradicted"
    assert slot.render(83)["state"] == "OFF"


def test_expiry_retirement_rejects_read_started_before_it_but_merges_aux() -> None:
    slot, _ = accepted()
    expire(slot, 7, 83)
    observe(slot, 6, 82, on="1", power="123")
    assert slot.render(84)["state"] is None
    assert slot.render(84)["power"] == 12.3
    observe(slot, 8, 84, reliable=False, on="1")
    assert slot.render(84)["state"] is None
    observe(slot, 9, 85, on="1")
    assert slot.render(85)["state"] == "ON"


def test_stale_command_outcome_and_expiry_cannot_replace_new_intent() -> None:
    slot, first = accepted()
    old_window, old_id = slot.view.window, slot.view.command_id
    second = begin(slot, seq=5, at=4, targets=(("on", "0"),))
    issue(slot, second, seq=6, at=5)
    outcome(slot, second, seq=7, at=6)
    slot.dispatch(NativeOutcome(env(slot, 8, 7), old_id, first, OutcomeKind.REJECTED))
    assert old_window is not None
    slot.dispatch(Expire(env(slot, 9, 83), old_window, 83))
    assert slot.view.status == "pending"
    assert slot.view.deadline == 86


@pytest.mark.parametrize("return_first", [False, True])
def test_worker_abandonment_keeps_real_completion_floor(return_first: bool) -> None:
    slot = owner()
    ticket = begin(slot)
    issue(slot, ticket)
    if return_first:
        outcome(slot, ticket, seq=4, at=3)
    slot.dispatch(WorkerCancelled(env(slot, 5, 4), slot.view.command_id, ticket))
    assert slot.view.status == "unconfirmed"
    assert slot.view.targets == () and slot.view.deadline is None
    observe(slot, 6, 5, on="0")
    if not return_first:
        assert slot.render(5)["state"] is None
        slot.dispatch(Completion(env(slot, 7, 6), ticket))
        assert slot.render(6)["state"] is None
        observe(slot, 8, 7, on="1")
    assert slot.render(7)["state"] == ("ON" if not return_first else "OFF")
    assert slot.view.window is None


def test_successor_only_inherits_physical_hold_and_old_completion_cannot_settle_new() -> None:
    slot = owner()
    ticket = begin(slot)
    issue(slot, ticket)
    slot.dispatch(WorkerCancelled(env(slot, 4, 3), slot.view.command_id, ticket))
    hold = slot.view.completion_hold
    slot.dispatch(Revoke(env(slot, 5, 4)))
    successor = MeshSettlement(
        slot.metadata,
        source="adapter",
        generation=2,
        birth_sequence=5,
        born_at=4,
        completion_hold=hold,
    )

    def fresh(seq: int, at: float) -> Envelope:
        return replace(env(successor, seq, at), generation=2)

    successor.dispatch(Observe(fresh(6, 5), PrimitiveCapture((PrimitiveField("on", "1"),), True)))
    assert successor.render(5)["state"] is None
    second = object()
    successor.dispatch(Begin(fresh(7, 6), second, (("on", "0"),)))
    successor.dispatch(Completion(replace(env(successor, 8, 7), generation=1), ticket))
    assert successor.view.phase is Phase.ISSUING
    assert successor.view.targets == (("on", "0"),)
    assert successor.render(7)["state"] is None
    successor.dispatch(Observe(fresh(9, 8), PrimitiveCapture((PrimitiveField("on", "1"),), True)))
    assert successor.render(8)["state"] == "ON"
    slot.dispatch(Observe(env(slot, 10, 9), PrimitiveCapture((PrimitiveField("on", "1"),), True)))
    assert slot.view.phase is Phase.INACTIVE


@pytest.mark.parametrize("result", ["accept", "reject", "cancel"])
def test_auxiliary_outcomes_are_field_owned_and_do_not_refresh_primary(result: str) -> None:
    slot, _ = accepted()
    window = slot.view.window
    ticket = object()
    slot.dispatch(AuxIssue(env(slot, 5, 4), ticket, (("enable_motion_score", "1"),)))
    events: dict[str, type[AuxAccepted] | type[AuxRejected] | type[AuxCancelled]] = {
        "accept": AuxAccepted,
        "reject": AuxRejected,
        "cancel": AuxCancelled,
    }
    event = events[result]
    slot.dispatch(event(env(slot, 6, 5), ticket))
    assert slot.view.window == window
    assert slot.view.evidence == ()
    assert slot.render(5)["state"] is None
    device = slot.materialize(5)
    if result == "accept":
        assert device.variables["enable_motion_score"].timestamp_ms is None
    else:
        assert "enable_motion_score" not in device.variables


def test_aux_newer_native_wins_replay_and_later_repeat_clears_projection() -> None:
    slot = owner()
    ticket = object()
    slot.dispatch(AuxIssue(env(slot, 2, 1), ticket, (("enable_motion_score", "1"),)))
    observe(slot, 4, 3, enable_motion_score="0")
    slot.dispatch(AuxAccepted(env(slot, 3, 2), ticket))
    assert slot.materialize(3).variables["enable_motion_score"].value == "0"
    ticket = object()
    slot.dispatch(AuxIssue(env(slot, 5, 4), ticket, (("enable_motion_score", "1"),)))
    slot.dispatch(AuxAccepted(env(slot, 6, 5), ticket))
    assert slot.view.projections
    observe(slot, 7, 6, enable_motion_score="1")
    assert slot.view.projections == ()


def test_motion_capture_time_and_stale_high_cannot_refresh_hold() -> None:
    slot = owner()
    observe(
        slot,
        2,
        1,
        movement_detected="0",
        enable_motion_score="1",
        motion_high_threshold="45",
        motion_score="0",
    )
    observe(slot, 4, 4, motion_score="0")
    observe(slot, 3, 3, motion_score="255")
    assert slot.render(4)["motion"] is False
    observe(slot, 5, 5, motion_score="255")
    observe(slot, 6, 6, motion_score="0")
    assert slot.render(65)["motion"] is True
    assert slot.render(65.01)["motion"] is False


def test_terminal_obligation_survives_aux_stream_ack_preserves_new_ordinary_debt() -> None:
    slot, _ = accepted()
    observe(slot, 5, 4, on="0")
    token = slot.publication_token(object())
    assert token is not None
    slot.dispatch(PublishStarted(env(slot, 6, 4.5), token))
    observe(slot, 6, 5, on="1", power="100")
    assert slot.render(5, token=token)["mesh_write_status"] == "contradicted"
    revision = slot.view.ordinary_debt
    slot.dispatch(PublishAccepted(env(slot, 7, 6), token))
    assert slot.view.terminal is None
    assert slot.view.ordinary_debt == revision
    assert slot.render(6)["mesh_write_status"] == "idle"


def test_unconfirmed_terminal_render_never_borrows_known_ordinary_primary() -> None:
    slot, _ = accepted()
    expire(slot, 5, 83)
    token = slot.publication_token(object())
    assert token is not None
    observe(slot, 6, 84, on="1")
    assert slot.render(84)["state"] == "ON"
    assert slot.render(84, token=token)["state"] is None
    assert slot.render(84, token=token)["mesh_write_status"] == "unconfirmed"


def test_successful_idle_expiry_also_revokes_old_window_child() -> None:
    slot, _ = accepted()
    observe(slot, 5, 75, on="1")
    effects = expire(slot, 6, 83)
    assert slot.view.status == "idle"
    assert any(isinstance(effect, RevokeEffects) for effect in effects)


def test_nonfinite_wall_time_does_not_change_monotonic_deadline() -> None:
    slot = owner()
    ticket = begin(slot)
    issue(slot, ticket)
    slot.dispatch(
        NativeOutcome(
            replace(env(slot, 4, 3), wall_time=float("inf")),
            slot.view.command_id,
            ticket,
            OutcomeKind.ACCEPTED,
        )
    )
    assert slot.view.deadline == 83
    assert slot.render(3)["mesh_write_deadline"] is None


def test_capture_and_debt_storage_stays_bounded_over_storm() -> None:
    slot = owner()
    for seq in range(2, 1002):
        observe(slot, seq, float(seq), on=str(seq % 2), power=str(seq), ignored=str(seq))
    assert len(slot.view.observed) == 2
    assert slot.view.ordinary_debt == slot.view.revision
    assert slot.view.terminal is None


def test_numeric_fixed_deadline_and_native_brightness_survive_primary_mask() -> None:
    slot = owner()
    observe(slot, 2, 1, on="0", intensity="600")
    ticket = begin(slot, seq=3, at=2)
    issue(slot, ticket, seq=4, at=3)
    outcome(slot, ticket, seq=5, at=4)
    document = slot.render(5)
    assert document["mesh_write_deadline"] == 1084.0
    assert document["state"] is None
    assert document["brightness"] == 153
    observe(slot, 6, 70, power="123")
    assert slot.render(70)["mesh_write_deadline"] == 1084.0
    expire(slot, 7, 84)
    assert slot.render(84)["brightness"] == 153


def test_stale_begin_and_duplicate_begin_do_not_replace_newer_command() -> None:
    slot = owner()
    ticket = begin(slot, seq=20, at=20)
    command_id = slot.view.command_id
    slot.dispatch(Begin(env(slot, 19, 19), object(), (("on", "0"),)))
    slot.dispatch(Begin(env(slot, 20, 20), ticket, (("on", "1"),)))
    assert slot.view.command_id == command_id
    assert slot.view.ticket is ticket


def test_new_command_cannot_replace_outstanding_physical_hold_by_issuing() -> None:
    slot = owner()
    first = begin(slot)
    issue(slot, first)
    slot.dispatch(WorkerCancelled(env(slot, 4, 3), slot.view.command_id, first))
    second = begin(slot, seq=5, at=4)
    issue(slot, second, seq=6, at=5)
    assert slot.view.completion_hold is not None
    assert slot.view.completion_hold.ticket is first
    slot.dispatch(Completion(env(slot, 7, 6), first))
    assert slot.render(6)["state"] is None
    observe(slot, 8, 7, on="0")
    assert slot.render(7)["state"] == "OFF"


def test_new_generation_successor_starts_with_fresh_unconfirmed_debt() -> None:
    slot, _ = accepted()
    observe(slot, 5, 4, on="1", motion_score="255")
    slot.dispatch(Revoke(env(slot, 6, 5)))
    successor = MeshSettlement(
        slot.metadata,
        source="adapter",
        generation=2,
        birth_sequence=6,
        born_at=5,
        initial_unconfirmed=True,
    )
    assert successor.view.phase is Phase.TERMINAL
    assert successor.view.command_id == 0
    assert successor.view.targets == () and successor.view.observed == ()
    assert successor.view.evidence == () and successor.view.last_hot is None
    assert successor.view.terminal is not None
    assert successor.view.terminal.command_id == 0
    assert successor.render(5)["mesh_write_status"] == "unconfirmed"
    assert successor.render(5)["state"] is None
    assert slot.publication_token(object()) is None


def test_completion_from_different_source_requires_current_replay_boundary() -> None:
    slot = owner()
    ticket = begin(slot)
    issue(slot, ticket)
    hold = slot.view.completion_hold
    slot.dispatch(Revoke(env(slot, 4, 3)))
    successor = MeshSettlement(
        slot.metadata,
        source="new-adapter",
        generation=1,
        birth_sequence=1,
        born_at=4,
        completion_hold=hold,
    )
    old = env(successor, 10000, 5)
    successor.dispatch(Completion(old, ticket))
    assert successor.view.completion_hold is hold
    current = replace(old, source=Source("new-adapter", "completion-replay"), sequence=2)
    successor.dispatch(Completion(current, ticket))
    assert successor.view.completion_hold is None
    assert successor.view.retirement_sequence == 2
    successor.dispatch(
        Observe(
            replace(current, sequence=3, captured_at=6),
            PrimitiveCapture((PrimitiveField("on", "0"),), True),
        )
    )
    assert successor.render(6)["state"] == "OFF"


def test_old_ordinary_ack_cannot_clear_new_debt_and_failed_publish_keeps_debt() -> None:
    slot = owner()
    token = slot.publication_token(object())
    assert token is not None
    slot.dispatch(PublishStarted(env(slot, 2, 1), token))
    observe(slot, 3, 2, on="1")
    debt = slot.view.ordinary_debt
    slot.dispatch(PublishAccepted(env(slot, 4, 3), token))
    assert slot.view.ordinary_debt == debt
    current = slot.publication_token(object())
    assert current is not None
    slot.dispatch(PublishStarted(env(slot, 5, 4), current))
    slot.dispatch(PublishFailed(env(slot, 6, 5), current))
    assert slot.view.ordinary_debt == debt
    slot.dispatch(PublishStarted(env(slot, 7, 6), current))
    slot.dispatch(PublishAccepted(env(slot, 8, 7), current))
    assert slot.view.ordinary_debt is None
    observe(slot, 9, 8, power="123")
    newer = slot.view.ordinary_debt
    slot.dispatch(PublishAccepted(env(slot, 10, 9), current))
    assert slot.view.ordinary_debt == newer


def test_terminal_ack_requires_registered_exact_session_attempt() -> None:
    slot, _ = accepted()
    observe(slot, 5, 4, on="0")
    token = slot.publication_token(object())
    assert token is not None
    slot.dispatch(PublishStarted(env(slot, 6, 5), token))
    slot.dispatch(PublishAccepted(env(slot, 7, 6), replace(token, client_session=object())))
    assert slot.view.terminal is not None
    slot.dispatch(PublishAccepted(env(slot, 8, 7), token))
    assert slot.view.terminal is None


def test_materialized_compatibility_view_cannot_mutate_owner() -> None:
    slot = owner()
    observe(slot, 2, 1, on="0")
    device = slot.materialize(1)
    device.variables.clear()
    device.name = "Changed"
    assert slot.render(1)["state"] == "OFF"
    assert slot.metadata.name == "Load"


def test_dispatched_capture_never_materializes_or_renders(monkeypatch: pytest.MonkeyPatch) -> None:
    import brilliant_mqtt.mesh_settlement_render as renderer

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("dispatch materialized a device")

    monkeypatch.setattr(renderer, "materialize", forbidden)
    monkeypatch.setattr(renderer, "render", forbidden)
    slot = owner()
    observe(slot, 2, 1, on="0", power="100")
    assert dict(slot.view.observed)["on"].value == "0"


def test_motion_uses_entire_admitted_capture_before_evaluating_score() -> None:
    slot = owner()
    observe(
        slot,
        2,
        1,
        motion_score="255",
        motion_high_threshold="45",
        enable_motion_score="1",
        movement_detected="0",
    )
    assert slot.render(1)["motion"] is True


def test_other_loads_derive_motion_without_primary_windows() -> None:
    slot = MeshSettlement(
        MeshMetadata("ble_mesh", "load", "Load", DeviceKind.ALWAYS_ON, 46),
        source="adapter",
        generation=1,
        birth_sequence=1,
        born_at=0,
    )
    observe(
        slot,
        2,
        1,
        motion_score="100",
        motion_high_threshold="45",
        enable_motion_score="1",
        movement_detected="0",
    )
    begin(slot, seq=3, at=2)
    assert slot.view.phase is Phase.QUIET
    assert slot.view.window is None
    assert "state" not in slot.render(2)
    assert "mesh_write_status" not in slot.render(2)
    assert slot.render(2)["motion"] is True


@pytest.mark.parametrize("receipt", ["accepted", "failed", "timeout", "unconfirmed", ""])
def test_success_is_typed_not_parsed_from_receipt_and_retains_full_fence(receipt: str) -> None:
    slot = owner()
    ticket = begin(slot)
    issue(slot, ticket)
    boundary = env(slot, 4, 3)
    slot.dispatch(
        NativeOutcome(boundary, slot.view.command_id, ticket, OutcomeKind.ACCEPTED, receipt)
    )
    assert slot.view.phase is Phase.ACCEPTED
    assert slot.view.receipt == receipt
    assert slot.view.return_boundary is boundary
    assert slot.view.issue_boundary is not None
    assert slot.view.issue_boundary.sequence == 3
    begin(slot, seq=5, at=4)
    assert slot.view.receipt is None and slot.view.return_boundary is None
    slot.dispatch(NativeOutcome(env(slot, 6, 5), 1, ticket, OutcomeKind.ACCEPTED, "old"))
    assert slot.view.receipt is None and slot.view.return_boundary is None


def test_inconclusive_window_uses_real_return_fence_and_original_deadline() -> None:
    slot = owner()
    ticket = begin(slot)
    issue(slot, ticket)
    slot.dispatch(CallerTimeout(env(slot, 4, 5), slot.view.command_id, ticket))
    observe(slot, 5, 73, on="1")
    outcome(slot, ticket, OutcomeKind.TIMEOUT, seq=6, at=74)
    assert slot.view.phase is Phase.INCONCLUSIVE
    assert slot.view.deadline == 85
    assert slot.view.evidence == ()
    observe(slot, 7, 75, on="1")
    expire(slot, 8, 85)
    assert slot.view.status == "idle"


def test_after_retirement_later_sequence_with_equal_monotonic_time_can_release() -> None:
    slot, _ = accepted()
    expire(slot, 5, 83)
    observe(slot, 6, 83, on="0")
    assert slot.render(83)["state"] == "OFF"


def test_atomic_binding_seed_accepts_only_its_birth_boundary() -> None:
    slot = owner()
    capture = PrimitiveCapture((PrimitiveField("on", "1"),), True)
    slot.dispatch(SeedBinding(env(slot, 1, 0), capture))
    assert slot.render(0)["state"] == "ON"
    assert slot.view.evidence == ()
    slot.dispatch(
        SeedBinding(env(slot, 1, 0), PrimitiveCapture((PrimitiveField("on", "0"),), True))
    )
    assert slot.render(0)["state"] == "ON"


def test_binding_seed_does_not_release_inherited_completion_hold() -> None:
    slot = owner()
    ticket = begin(slot)
    issue(slot, ticket)
    successor = MeshSettlement(
        slot.metadata,
        source="adapter",
        generation=2,
        birth_sequence=4,
        born_at=3,
        completion_hold=slot.view.completion_hold,
    )
    boundary = replace(env(successor, 4, 3), generation=2)
    successor.dispatch(SeedBinding(boundary, PrimitiveCapture((PrimitiveField("on", "1"),), True)))
    assert successor.render(3)["state"] is None
    assert dict(successor.view.observed)["on"].value == "1"


def test_unreported_copied_aux_field_cannot_remove_success_projection() -> None:
    slot = owner()
    ticket = object()
    slot.dispatch(AuxIssue(env(slot, 2, 1), ticket, (("enable_motion_score", "1"),)))
    slot.dispatch(AuxAccepted(env(slot, 3, 2), ticket))
    observe(slot, 4, 3, reported=frozenset(("power",)), power="10", enable_motion_score="0")
    assert slot.materialize(3).variables["enable_motion_score"].value == "1"
    observe(slot, 5, 4, enable_motion_score="0")
    assert slot.materialize(4).variables["enable_motion_score"].value == "0"


def test_encoding_uses_immutable_render_and_current_terminal_obligation() -> None:
    slot, _ = accepted()
    encoded = json.loads(slot.encode(3))
    assert encoded["state"] is None
    assert encoded["mesh_requested"] == {"on": "1"}
    assert encoded["mesh_write_deadline"] == 1083
    expire(slot, 5, 83)
    token = slot.publication_token(object())
    assert token is not None
    observe(slot, 6, 84, on="1")
    assert json.loads(slot.encode(84, token=token))["state"] is None


def test_after_successful_idle_expiry_next_command_has_no_superseded_feedback() -> None:
    slot, _ = accepted()
    observe(slot, 5, 75, on="1")
    expire(slot, 6, 83)
    begin(slot, seq=7, at=84)
    assert slot.view.status == "idle"


def test_rejected_late_return_preserves_expiry_hold_until_trusted_primary_capture() -> None:
    slot = owner()
    observe(slot, 2, 1, on="0")
    ticket = begin(slot, seq=3, at=2)
    issue(slot, ticket, seq=4, at=3)
    slot.dispatch(CallerTimeout(env(slot, 5, 5), slot.view.command_id, ticket))
    expire(slot, 6, 85)
    outcome(slot, ticket, OutcomeKind.REJECTED, seq=7, at=90)
    assert slot.render(90)["mesh_write_status"] == "failed"
    assert slot.render(90)["state"] is None
    token = slot.publication_token(object())
    assert token is not None
    observe(slot, 8, 91, on="0")
    assert slot.render(91)["state"] == "OFF"
    assert slot.render(91, token=token)["mesh_write_status"] == "failed"


def test_completion_cannot_precede_its_same_source_issue_boundary() -> None:
    slot = owner()
    ticket = begin(slot)
    issue(slot, ticket, seq=4, at=3)
    slot.dispatch(WorkerCancelled(env(slot, 5, 4), slot.view.command_id, ticket))
    slot.dispatch(Completion(env(slot, 3, 2), ticket))
    assert slot.view.completion_hold is not None


def test_abandoned_native_outcome_does_not_rearm_and_exact_completion_is_required() -> None:
    slot = owner()
    ticket = begin(slot)
    issue(slot, ticket)
    command_id = slot.view.command_id
    slot.dispatch(WorkerCancelled(env(slot, 4, 3), command_id, ticket))
    slot.dispatch(NativeOutcome(env(slot, 5, 4), command_id, ticket, OutcomeKind.ACCEPTED, "ack"))
    assert slot.view.status == "unconfirmed"
    assert slot.view.window is None and slot.view.receipt is None
    slot.dispatch(Completion(env(slot, 6, 5), object()))
    assert slot.view.completion_hold is not None
    slot.dispatch(Completion(env(slot, 7, 6), ticket))
    assert slot.view.completion_hold is None
    assert slot.render(6)["state"] is None
