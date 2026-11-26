"""Synchronous, per-peripheral authority for mesh write settlement.

There is no transport here. Dispatch reduces one immutable event and returns
bounded descriptions for the bridge's timer/publication workers. An instance
is also its lifetime identity: revoked instances can never be reactivated.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping

from brilliant_mqtt import mesh_settlement_render
from brilliant_mqtt.mapping import AUX_SPECS
from brilliant_mqtt.mesh_settlement_types import (
    ArmExpiry,
    AuxAccepted,
    AuxCancelled,
    AuxIssue,
    AuxRejected,
    Begin,
    CallerTimeout,
    CancelExpiry,
    Completion,
    CompletionHold,
    Effect,
    Envelope,
    Event,
    Expire,
    FieldEvidence,
    Issue,
    MeshMetadata,
    NativeOutcome,
    Observe,
    OutcomeKind,
    OwnerView,
    PeripheralAbsent,
    Phase,
    PublicationToken,
    PublishAccepted,
    PublishFailed,
    PublishStarted,
    RequestPublication,
    RetireAbsent,
    Revoke,
    RevokeEffects,
    ScopeUnknown,
    SeedBinding,
    TerminalObligation,
    UpdateMetadata,
    WakePublication,
    WindowToken,
    WorkerCancelled,
)
from brilliant_mqtt.model import BrilliantDevice, CaptureProvenance, DeviceKind, PrimitiveField

_PRIMARY = frozenset(("on", "intensity"))
_WINDOW_S = 80.0
_MATCH_AGE_S = 10.0


class MeshSettlement:
    """One loop-confined owner. Public state is frozen and demand-materialized."""

    @property
    def source(self) -> str:
        return self._source

    @property
    def generation(self) -> int:
        return self._generation

    @property
    def active(self) -> bool:
        return self._phase is not Phase.INACTIVE

    @property
    def retired(self) -> bool:
        """An absent binding remains live solely to finish bounded cleanup debt."""
        return self._retired

    def field_watermark(self, name: str) -> int:
        """Cheap binding admission query without allocating an immutable full view."""
        return self._watermarks.get(name, -1)

    def accepts_capture(self, boundary: CaptureProvenance) -> bool:
        """Fence lifecycle/binding admission before any stateful capture work."""
        return (
            self.active
            and not self._retired
            and boundary.incarnation == self._source
            and boundary.source_generation == self._generation
            and boundary.sequence > self._birth_sequence
            and math.isfinite(boundary.captured_at)
            and boundary.captured_at >= self._born_at
        )

    def __init__(
        self,
        metadata: MeshMetadata,
        *,
        source: str,
        generation: int,
        birth_sequence: int,
        born_at: float,
        completion_hold: CompletionHold | None = None,
        initial_unconfirmed: bool = False,
        motion_hold_s: float = 60.0,
        native_motion_passthrough: bool = False,
        derive_motion: bool = True,
    ) -> None:
        if (
            not source
            or generation < 0
            or birth_sequence < 0
            or not math.isfinite(born_at)
            or born_at < 0
            or not math.isfinite(motion_hold_s)
            or motion_hold_s < 0
        ):
            raise ValueError("invalid mesh owner birth provenance or motion hold")
        self._metadata = metadata
        self._source = source
        self._generation = generation
        self._birth_sequence = birth_sequence
        self._seeded = False
        self._born_at = born_at
        self._primary = metadata.kind in (DeviceKind.LIGHT, DeviceKind.SWITCH)
        self._auxiliary = frozenset(spec.var for spec in AUX_SPECS.get(metadata.kind, ()))
        self._supported = self._auxiliary | frozenset(
            (
                "on",
                "intensity",
                "max_intensity_value",
                "movement_detected",
                "motion_score",
                "enable_motion_score",
                "motion_high_threshold",
                "motion_low_threshold",
                "lux",
            )
        )
        self._fields: dict[str, PrimitiveField] = {}
        self._watermarks: dict[str, int] = {}
        self._evidence: dict[str, FieldEvidence] = {}
        self._projections: dict[str, PrimitiveField] = {}
        self._projection_fences: dict[str, int] = {}
        self._aux_tickets: dict[str, tuple[object, str, int]] = {}
        self._phase = Phase.TERMINAL if initial_unconfirmed else Phase.QUIET
        self._status = "unconfirmed" if initial_unconfirmed else "idle"
        self._command_id = 0
        self._command_sequence = birth_sequence
        self._ticket: object | None = None
        self._targets: tuple[tuple[str, str], ...] = ()
        self._issued_targets: tuple[tuple[str, str], ...] = ()
        self._issue: Envelope | None = None
        self._outcome: OutcomeKind | None = None
        self._receipt: str | None = None
        self._return: Envelope | None = None
        self._completion_hold = completion_hold
        self._hold_blocks_primary = completion_hold is not None
        self._window: WindowToken | None = None
        self._deadline: float | None = None
        self._wire_deadline: float | None = None
        self._original_deadline: float | None = None
        self._window_identity: WindowToken | None = None
        self._unknown = self._primary
        self._retirement_sequence = birth_sequence
        self._retired_at = born_at
        self._release_required = frozenset(("on",)) if self._primary else frozenset()
        self._release_seen: set[str] = set()
        self._last_hot: float | None = None
        self._motion_hold_s = motion_hold_s
        self._native_motion_passthrough = native_motion_passthrough
        self._derive_motion = derive_motion
        self._retired = False
        self._revision = 1
        self._ordinary_debt: int | None = 1
        self._terminal: TerminalObligation | None = (
            TerminalObligation(0, None, birth_sequence, "unconfirmed")
            if initial_unconfirmed
            else None
        )
        self._in_flight: PublicationToken | None = None

    @property
    def metadata(self) -> MeshMetadata:
        return self._metadata

    @property
    def view(self) -> OwnerView:
        """Immutable diagnostics, never a mutable compatibility authority."""
        return OwnerView(
            self._phase,
            self._status,
            self._command_id,
            self._ticket,
            self._targets,
            tuple(self._fields.items()),
            tuple(self._watermarks.items()),
            tuple(self._evidence.items()),
            tuple(self._projections.items()),
            self._window,
            self._deadline,
            self._wire_deadline,
            self._return.sequence if self._return is not None else None,
            self._completion_hold,
            self._retirement_sequence,
            self._revision,
            self._ordinary_debt,
            self._terminal,
            self._last_hot,
            self._in_flight,
            self._issue,
            self._return,
            self._outcome,
            self._receipt,
        )

    def materialize(self, now: float) -> BrilliantDevice:
        """Disposable legacy view; mutations cannot feed back into the owner."""
        return mesh_settlement_render.materialize(
            self._metadata,
            self.view,
            now,
            self._motion_hold_s,
            self._native_motion_passthrough,
            self._derive_motion,
        )

    def render(self, now: float, *, token: PublicationToken | None = None) -> Mapping[str, object]:
        if token is not None and not self._publication_current(token):
            raise ValueError("obsolete mesh publication token")
        return mesh_settlement_render.render(
            self._metadata,
            self.view,
            now,
            self._unknown or self._hold_blocks_primary,
            self._motion_hold_s,
            self._native_motion_passthrough,
            token,
            self._derive_motion,
        )

    def publication_token(self, client_session: object) -> PublicationToken | None:
        """Mint once per attempt; carry this exact fresh object through start/result."""
        if self._phase is Phase.INACTIVE or (
            self._terminal is None and self._ordinary_debt is None
        ):
            return None
        return PublicationToken(
            self,
            self._generation,
            self._command_id,
            self._window_identity,
            self._revision,
            client_session,
            self._terminal,
        )

    def encode(self, now: float, *, token: PublicationToken | None = None) -> str:
        """Encode the immutable render on demand, including its frozen request map."""
        fields = dict(self.render(now, token=token))
        requested = fields.get("mesh_requested")
        if isinstance(requested, Mapping):
            fields["mesh_requested"] = dict(requested)
        return json.dumps(fields)

    def dispatch(self, event: Event) -> tuple[Effect, ...]:
        envelope = event.envelope
        effects: list[Effect] = []
        if self._phase is Phase.INACTIVE:
            return ()
        if self._retired and not isinstance(
            event, (PublishStarted, PublishAccepted, PublishFailed, Revoke)
        ):
            return ()
        if isinstance(event, Completion):
            return self._complete(event)
        if isinstance(event, SeedBinding):
            if (
                self._seeded
                or self._revision != 1
                or not self._admitted(envelope, birth=True)
                or envelope.sequence != self._birth_sequence
                or envelope.captured_at != self._born_at
            ):
                return ()
            self._seeded = True
            self._observe(Observe(envelope, event.capture), effects, seed=True)
            return tuple(effects)
        if not self._admitted(envelope):
            return ()
        if isinstance(event, UpdateMetadata):
            if (
                event.metadata.device_id == self._metadata.device_id
                and event.metadata.peripheral_id == self._metadata.peripheral_id
                and event.metadata.kind is self._metadata.kind
                and event.metadata.peripheral_type == self._metadata.peripheral_type
                and event.metadata.binding_dimmable == self._metadata.binding_dimmable
                and event.metadata.binding_max_intensity == self._metadata.binding_max_intensity
            ):
                self._metadata = event.metadata
        elif isinstance(event, RequestPublication):
            self._touch(effects)
        elif isinstance(event, RetireAbsent):
            tracked = (
                self._phase in (Phase.ISSUING, Phase.UNRESOLVED, Phase.INCONCLUSIVE, Phase.ACCEPTED)
                or self._terminal is not None
            )
            self._end_window(effects)
            self._retired = True
            self._phase = Phase.TERMINAL
            self._status = "unconfirmed" if tracked and self._primary else "idle"
            self._targets = ()
            self._issued_targets = ()
            self._ticket = None
            self._issue = None
            self._outcome = None
            self._receipt = None
            self._return = None
            self._completion_hold = None
            self._hold_blocks_primary = False
            self._fields.clear()
            self._watermarks.clear()
            self._evidence.clear()
            self._aux_tickets.clear()
            self._projections.clear()
            self._projection_fences.clear()
            self._last_hot = None
            self._retire(envelope)
            if tracked and self._primary:
                self._terminal = TerminalObligation(
                    self._command_id, self._window_identity, envelope.sequence, "unconfirmed"
                )
            self._touch(effects)
        elif isinstance(event, Observe):
            self._observe(event, effects)
        elif isinstance(event, (PeripheralAbsent, ScopeUnknown)):
            names = self._supported if isinstance(event, PeripheralAbsent) else _PRIMARY
            changed = False
            for name in names:
                changed |= self._merge(name, None, envelope, effects)
            if changed:
                self._touch(effects)
        elif isinstance(event, Begin):
            self._begin(event, effects)
        elif isinstance(event, Issue):
            if self._owns(event.command_id, event.ticket) and self._issue is None:
                if (
                    envelope.sequence > self._command_sequence
                    and self._valid_targets(event.targets)
                    and self._completion_hold is None
                ):
                    self._issue = envelope
                    self._targets = event.targets
                    self._issued_targets = event.targets
                    self._completion_hold = CompletionHold(
                        event.ticket, self._source, self._generation, envelope.sequence
                    )
        elif isinstance(event, CallerTimeout):
            if (
                self._owns(event.command_id, event.ticket)
                and self._issue is not None
                and self._outcome is None
                and self._original_deadline is None
            ):
                self._phase = Phase.UNRESOLVED
                self._status = "pending"
                self._arm(envelope, effects)
                self._original_deadline = self._deadline
                self._touch(effects)
        elif isinstance(event, NativeOutcome):
            self._native_outcome(event, effects)
        elif isinstance(event, WorkerCancelled):
            self._cancel(event, effects)
        elif isinstance(event, Expire):
            self._expire(event, effects)
        elif isinstance(event, Revoke):
            self._end_window(effects)
            self._phase = Phase.INACTIVE
            self._status = "unconfirmed"
            self._targets = ()
            self._issued_targets = ()
            self._ticket = None
            self._issue = None
            self._outcome = None
            self._receipt = None
            self._return = None
            self._evidence.clear()
            self._fields.clear()
            self._watermarks.clear()
            self._aux_tickets.clear()
            self._projections.clear()
            self._projection_fences.clear()
            self._last_hot = None
            self._retire(envelope)
            self._ordinary_debt = None
        elif isinstance(event, AuxIssue):
            for name, value in event.targets:
                if name in self._auxiliary and name not in _PRIMARY:
                    previous = self._aux_tickets.get(name)
                    if previous is None or envelope.sequence > previous[2]:
                        self._aux_tickets[name] = (event.ticket, value, envelope.sequence)
        elif isinstance(event, (AuxAccepted, AuxRejected, AuxCancelled)):
            self._aux_outcome(event, effects)
        elif isinstance(event, PublishStarted):
            if self._in_flight is None and self._publication_current(event.token):
                self._in_flight = event.token
        elif isinstance(event, (PublishAccepted, PublishFailed)):
            self._publish_result(event, effects)
        return tuple(effects)

    def _admitted(self, envelope: Envelope, *, birth: bool = False) -> bool:
        return (
            envelope.owner is self
            and envelope.source.incarnation == self._source
            and envelope.generation == self._generation
            and envelope.peripheral_id == self._metadata.peripheral_id
            and envelope.sequence >= self._birth_sequence + (0 if birth else 1)
            and math.isfinite(envelope.captured_at)
            and envelope.captured_at >= self._born_at
        )

    def _owns(self, command_id: int, ticket: object) -> bool:
        return command_id == self._command_id and ticket is self._ticket

    @staticmethod
    def _valid_targets(targets: tuple[tuple[str, str], ...]) -> bool:
        return (
            bool(targets)
            and len(targets) <= 2
            and len(dict(targets)) == len(targets)
            and all(name in _PRIMARY for name, _ in targets)
        )

    def _touch(self, effects: list[Effect]) -> None:
        self._revision += 1
        self._ordinary_debt = self._revision
        effects.append(WakePublication(self))

    def _end_window(self, effects: list[Effect]) -> None:
        effects.append(RevokeEffects(self, self._window_identity))
        if self._window is not None:
            effects.append(CancelExpiry(self._window))
        self._window = None
        self._deadline = None
        self._wire_deadline = None
        self._terminal = None
        self._in_flight = None

    def _arm(self, envelope: Envelope, effects: list[Effect]) -> None:
        self._end_window(effects)
        self._window = WindowToken(self, self._generation, self._command_id, envelope.sequence)
        self._window_identity = self._window
        self._deadline = envelope.captured_at + _WINDOW_S
        self._wire_deadline = self._deadline_wire(envelope)
        effects.append(ArmExpiry(self._window, self._deadline))

    @staticmethod
    def _deadline_wire(envelope: Envelope) -> float | None:
        if envelope.wall_time is None or not math.isfinite(envelope.wall_time):
            return None
        deadline = envelope.wall_time + _WINDOW_S
        return deadline if math.isfinite(deadline) else None

    def _begin(self, event: Begin, effects: list[Effect]) -> None:
        if (
            not self._primary
            or not self._valid_targets(event.targets)
            or event.envelope.sequence <= self._command_sequence
        ):
            return
        replacing = (
            self._phase is not Phase.QUIET or self._terminal is not None or self._status != "idle"
        )
        self._end_window(effects)
        self._command_id += 1
        self._command_sequence = event.envelope.sequence
        self._ticket = event.ticket
        self._targets = event.targets
        self._issue = None
        self._issued_targets = ()
        self._outcome = None
        self._receipt = None
        self._return = None
        self._original_deadline = None
        self._window_identity = None
        self._evidence.clear()
        self._phase = Phase.ISSUING
        self._status = "superseded" if replacing else "idle"
        self._touch(effects)

    def _native_outcome(self, event: NativeOutcome, effects: list[Effect]) -> None:
        if (
            not self._owns(event.command_id, event.ticket)
            or self._issue is None
            or self._outcome is not None
            or event.envelope.sequence <= self._issue.sequence
        ):
            return
        self._outcome = event.kind
        self._receipt = event.receipt if event.kind is OutcomeKind.ACCEPTED else None
        self._return = event.envelope
        if self._completion_hold is not None and self._completion_hold.ticket is event.ticket:
            self._completion_hold = None
        self._evidence.clear()
        if event.kind is OutcomeKind.ACCEPTED:
            self._targets = self._issued_targets
            self._phase = Phase.ACCEPTED
            self._status = "pending"
            self._arm(event.envelope, effects)
        elif event.kind is OutcomeKind.REJECTED:
            self._terminalize("failed", event.envelope, effects)
        elif self._original_deadline is not None and self._window is None:
            # The unresolved window already expired. Only acceptance can rearm it.
            self._retire(event.envelope)
        else:
            self._phase = Phase.INCONCLUSIVE
            self._status = "pending"
            if self._window is None:
                self._arm(event.envelope, effects)
                self._original_deadline = self._deadline
        self._touch(effects)

    def _retire(self, envelope: Envelope, required: frozenset[str] | None = None) -> None:
        self._retirement_sequence = max(self._retirement_sequence, envelope.sequence)
        self._retired_at = max(self._retired_at, envelope.captured_at)
        self._unknown = self._primary
        self._release_required = required if required is not None else frozenset(("on",))
        self._release_seen.clear()

    def _terminalize(self, reason: str, envelope: Envelope, effects: list[Effect]) -> None:
        required = frozenset(name for name, _ in self._targets) | frozenset(("on",))
        self._end_window(effects)
        self._terminal = TerminalObligation(
            self._command_id, self._window_identity, envelope.sequence, reason
        )
        self._phase = Phase.TERMINAL
        self._status = reason
        self._targets = ()
        self._evidence.clear()
        if reason == "unconfirmed":
            self._retire(envelope, required)

    def _cancel(self, event: WorkerCancelled, effects: list[Effect]) -> None:
        if not self._owns(event.command_id, event.ticket):
            return
        if self._issue is None:
            self._end_window(effects)
            self._phase = Phase.QUIET
            self._status = "superseded" if self._status == "superseded" else "idle"
            self._targets = ()
            self._ticket = None
            self._issued_targets = ()
        else:
            returned = self._return
            self._terminalize("unconfirmed", event.envelope, effects)
            self._hold_blocks_primary = self._completion_hold is not None
            if returned is not None:
                self._retire(returned)
            self._ticket = None
            self._issue = None
            self._outcome = None
            self._receipt = None
            self._return = None
            self._issued_targets = ()
        self._touch(effects)

    def _complete(self, event: Completion) -> tuple[Effect, ...]:
        hold = self._completion_hold
        envelope = event.envelope
        if (
            hold is None
            or event.ticket is not hold.ticket
            or envelope.owner is not self
            or envelope.peripheral_id != self._metadata.peripheral_id
            or not math.isfinite(envelope.captured_at)
        ):
            return ()
        if (
            envelope.source.incarnation == hold.source == self._source
            and envelope.sequence <= hold.issue_sequence
        ):
            return ()
        # Same-incarnation native return is comparable even across a reconnect.
        # Cross-incarnation replay must reserve a boundary in the current source.
        same_source_return = (
            envelope.source.incarnation == hold.source == self._source
            and envelope.generation == hold.generation
            and envelope.sequence > hold.issue_sequence
        )
        if not same_source_return and not self._admitted(envelope):
            return ()
        self._completion_hold = None
        if not self._hold_blocks_primary:
            # Ordinary superseded work releases only the physical issue barrier.
            return ()
        self._hold_blocks_primary = False
        self._retire(envelope)
        effects: list[Effect] = []
        self._touch(effects)
        return tuple(effects)

    def _observe(self, event: Observe, effects: list[Effect], *, seed: bool = False) -> None:
        capture = event.capture
        reported = (
            frozenset(field.name for field in capture.fields)
            if capture.complete and capture.reliable
            else capture.reported
        )
        changed = False
        admitted_score: PrimitiveField | None = None
        present: set[str] = set()
        for field in capture.fields:
            name = field.name
            if name not in self._supported:
                continue
            present.add(name)
            reliable = capture.reliable and name in reported
            if not reliable:
                continue
            merged = self._merge(name, field, event.envelope, effects, seed=seed)
            changed |= merged
            if merged and name == "motion_score":
                admitted_score = field
        removed = capture.removed if capture.reliable else frozenset()
        if capture.complete and capture.reliable:
            removed = removed | (self._supported - present)
        for name in removed & self._supported:
            changed |= self._merge(name, None, event.envelope, effects, seed=seed)
        if admitted_score is not None and self._derive_motion:
            self._motion_score(admitted_score, event.envelope.captured_at)
        if changed:
            self._touch(effects)

    def _merge(
        self,
        name: str,
        field: PrimitiveField | None,
        envelope: Envelope,
        effects: list[Effect],
        *,
        seed: bool = False,
    ) -> bool:
        if envelope.sequence <= self._watermarks.get(
            name, self._birth_sequence - (1 if seed else 0)
        ):
            return False
        self._watermarks[name] = envelope.sequence
        if field is None:
            self._fields.pop(name, None)
        else:
            self._fields[name] = field
        if envelope.sequence > self._projection_fences.get(name, envelope.sequence):
            self._projection_fences.pop(name, None)
            self._projections.pop(name, None)
        if name == "enable_motion_score" and (field is None or field.value != "1"):
            self._last_hot = None
        if name in _PRIMARY:
            if field is None:
                self._evidence.pop(name, None)
                if name in self._release_required:
                    self._unknown = self._primary
                self._release_seen.discard(name)
            elif (
                not self._hold_blocks_primary
                and (
                    envelope.sequence > self._retirement_sequence
                    or seed
                    and envelope.sequence == self._retirement_sequence
                )
                and envelope.captured_at >= self._retired_at
            ):
                self._release_seen.add(name)
                if self._release_required <= self._release_seen:
                    self._unknown = False
                    if self._phase is Phase.TERMINAL and (
                        self._terminal is None or envelope.sequence > self._terminal.sequence
                    ):
                        self._phase = Phase.QUIET
                        self._status = "idle"
        targets = dict(self._targets)
        if (
            name not in targets
            or self._phase not in (Phase.ACCEPTED, Phase.INCONCLUSIVE)
            or self._return is None
            or envelope.sequence <= self._return.sequence
            or envelope.captured_at < self._return.captured_at
        ):
            return True
        self._evidence.pop(name, None)
        if field is not None:
            if field.value != targets[name]:
                self._terminalize("contradicted", envelope, effects)
            elif self._deadline is not None and envelope.captured_at < self._deadline:
                self._evidence[name] = FieldEvidence(
                    envelope.sequence, envelope.captured_at, envelope.generation, field.value
                )
        return True

    def _motion_score(self, field: PrimitiveField, at: float) -> None:
        enable = self._fields.get("enable_motion_score")
        threshold = self._fields.get("motion_high_threshold")
        if enable is None or enable.value != "1" or threshold is None:
            return
        try:
            if int(field.value) >= int(threshold.value):
                self._last_hot = at
        except ValueError:
            pass

    def _expire(self, event: Expire, effects: list[Effect]) -> None:
        if (
            event.window != self._window
            or self._deadline is None
            or not math.isfinite(event.handled_at)
            or event.handled_at < self._deadline
        ):
            return
        matched = (
            self._phase in (Phase.ACCEPTED, Phase.INCONCLUSIVE)
            and bool(self._targets)
            and all(
                name in self._evidence
                and self._evidence[name].generation == self._generation
                and self._evidence[name].captured_at < self._deadline
                and 0 <= event.handled_at - self._evidence[name].captured_at <= _MATCH_AGE_S
                for name, _ in self._targets
            )
        )
        if matched:
            self._end_window(effects)
            self._phase = Phase.QUIET
            self._status = "idle"
            self._targets = ()
            self._evidence.clear()
            self._retirement_sequence = max(self._retirement_sequence, event.envelope.sequence)
            self._retired_at = max(self._retired_at, event.handled_at)
        else:
            self._terminalize("unconfirmed", event.envelope, effects)
        self._touch(effects)

    def _aux_outcome(
        self, event: AuxAccepted | AuxRejected | AuxCancelled, effects: list[Effect]
    ) -> None:
        changed = False
        for name, (ticket, value, issued) in tuple(self._aux_tickets.items()):
            if ticket is not event.ticket or event.envelope.sequence <= issued:
                continue
            del self._aux_tickets[name]
            if (
                isinstance(event, AuxAccepted)
                and self._watermarks.get(name, self._birth_sequence) < event.envelope.sequence
            ):
                self._projections[name] = PrimitiveField(name, value)
                self._projection_fences[name] = event.envelope.sequence
                changed = True
        if changed:
            self._touch(effects)

    def _publication_current(self, token: PublicationToken) -> bool:
        return (
            self._phase is not Phase.INACTIVE
            and token.owner is self
            and token.generation == self._generation
            and token.command_id == self._command_id
            and token.window == self._window_identity
            and (
                token.terminal == self._terminal
                if token.terminal is not None
                else token.revision == self._ordinary_debt
            )
        )

    def _publish_result(
        self, event: PublishAccepted | PublishFailed, effects: list[Effect]
    ) -> None:
        token = event.token
        if token is not self._in_flight:
            return
        self._in_flight = None
        if not isinstance(event, PublishAccepted) or not self._publication_current(token):
            return
        if token.terminal is not None:
            self._terminal = None
        if token.revision == self._ordinary_debt:
            self._ordinary_debt = None
        if self._ordinary_debt is not None:
            effects.append(WakePublication(self))
