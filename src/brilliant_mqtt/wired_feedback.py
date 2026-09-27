"""Pure state transitions for wired primary observations and request feedback."""

from __future__ import annotations

from dataclasses import dataclass, replace

from brilliant_mqtt.mapping import payload_fields
from brilliant_mqtt.model import BrilliantDevice, CaptureProvenance, Variable

# The pilot observer mirror was measured about 20 seconds behind the bus.
WIRED_PROVISIONAL_SECONDS = 20.0


@dataclass(frozen=True)
class Attempt:
    generation: int
    targets: tuple[tuple[str, str], ...]
    issued_eras: tuple[tuple[str, int], ...] = ()


@dataclass(frozen=True)
class Feedback:
    targets: tuple[tuple[str, str], ...]
    issued_eras: tuple[tuple[str, int], ...]
    projected: frozenset[str]
    unresolved: frozenset[str]
    ambiguous: frozenset[str]
    status: str


@dataclass(frozen=True)
class Record:
    native: BrilliantDevice | None = None
    source_generation: int = 0
    native_eras: tuple[tuple[str, int], ...] = ()
    unknown: frozenset[str] = frozenset()
    generation: int = 0
    attempt: Attempt | None = None
    feedback: Feedback | None = None
    deadline_at: float | None = None
    deadline_wall: float | None = None
    native_debt: bool = False
    native_debt_epoch: int = 0
    revision: int = 0
    closed: bool = False


@dataclass(frozen=True)
class Capture:
    device: BrilliantDevice


@dataclass(frozen=True)
class Begin:
    targets: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class Issue:
    generation: int
    fields: tuple[tuple[str, int], ...]
    values: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class Outcome:
    generation: int
    kind: str
    now: float
    wall_now: float


@dataclass(frozen=True)
class Expire:
    now: float


@dataclass(frozen=True)
class SourceGeneration:
    generation: int


@dataclass(frozen=True)
class Rebind:
    pass


@dataclass(frozen=True)
class PublishAccepted:
    revision: int
    native: bool
    debt_epoch: int | None = None


@dataclass(frozen=True)
class PublishFailed:
    revision: int
    native: bool


@dataclass(frozen=True)
class Shutdown:
    pass


Event = (
    Capture
    | Begin
    | Issue
    | Outcome
    | Expire
    | SourceGeneration
    | Rebind
    | PublishAccepted
    | PublishFailed
    | Shutdown
)


@dataclass(frozen=True)
class Effects:
    publish: bool
    deadline: float | None


@dataclass(frozen=True)
class Transition:
    record: Record
    effects: Effects


@dataclass(frozen=True)
class Rendered:
    fields: dict[str, object]
    native: bool


def timer_deadline(record: Record) -> float | None:
    """The one non-renewing timer, independent of pending command admission."""
    return None if record.closed else record.deadline_at


def render(record: Record) -> Rendered | None:
    """Build the only wired publication view; native values stay untouched."""
    if record.closed or record.native is None:
        return None
    feedback = record.feedback
    variables = dict(record.native.variables)
    if feedback is not None and (not record.native_debt or not feedback.projected):
        for name, value in feedback.targets:
            if name in feedback.projected:
                old = variables.get(name)
                variables[name] = Variable(
                    name,
                    value,
                    externally_settable=old.externally_settable if old is not None else True,
                )
    fields = payload_fields(replace(record.native, variables=variables))
    if record.unknown & {"intensity", "max_intensity_value"}:
        fields.pop("brightness", None)
    if "on" not in variables or (
        "on" in record.unknown
        and not (feedback is not None and not record.native_debt and "on" in feedback.projected)
    ):
        fields["state"] = None
    if record.native_debt and feedback is not None and feedback.projected:
        fields.update(
            wired_write_status="unconfirmed",
            wired_requested={},
            wired_write_deadline=None,
        )
    elif feedback is not None:
        pending = feedback.status in ("provisional", "ambiguous")
        fields.update(
            wired_write_status=feedback.status,
            wired_requested=dict(feedback.targets) if pending else {},
            wired_write_deadline=record.deadline_wall if pending else None,
        )
    elif record.native_debt:
        fields.update(
            wired_write_status="unconfirmed",
            wired_requested={},
            wired_write_deadline=None,
        )
    return Rendered(fields, feedback is None or not feedback.projected or record.native_debt)


def _binding(device: BrilliantDevice) -> tuple[str, object, bool, int]:
    return device.device_id, device.kind, device.is_dimmable, device.max_intensity


def _era(provenance: CaptureProvenance | None, name: str) -> int | None:
    if provenance is None:
        return None
    return dict(provenance.field_eras).get(name, 0)


def _not_known_preissue(
    provenance: CaptureProvenance | None, name: str, issued: dict[str, int]
) -> bool:
    captured = _era(provenance, name)
    return name not in issued or captured is None or captured >= issued[name]


def _capture(record: Record, incoming: BrilliantDevice) -> Record:
    provenance = incoming.capture_provenance
    generation = (
        provenance.source_generation if provenance is not None else record.source_generation
    )
    if generation < record.source_generation:
        return record
    newer_source = generation > record.source_generation
    previous = record.native
    same_owner_kind = (
        previous is not None
        and previous.device_id == incoming.device_id
        and previous.kind == incoming.kind
        and not newer_source
    )
    variables = dict(previous.variables) if same_owner_kind and previous is not None else {}
    eras = dict(record.native_eras) if same_owner_kind else {}
    unknown = set(record.unknown) if same_owner_kind else set()
    present = incoming.capture_present or frozenset(incoming.variables)
    changed: set[str] = set()

    if incoming.capture_complete and same_owner_kind and previous is not None:
        for name in previous.variables:
            if name not in present:
                era = _era(provenance, name)
                old = eras.get(name)
                if era is not None and old is not None and era < old:
                    continue
                variables.pop(name, None)
                unknown.add(name)
                if era is not None:
                    eras[name] = era
                changed.add(name)
    for name in present:
        era = _era(provenance, name)
        old = eras.get(name)
        if era is not None and old is not None and era < old:
            continue
        if name in incoming.capture_unknown:
            unknown.add(name)
            if name == "on":
                variables.pop(name, None)
            elif name in ("intensity", "max_intensity_value"):
                if name not in variables:
                    variables[name] = Variable(name, "")
            else:
                variables.pop(name, None)
        else:
            value = incoming.variables.get(name)
            if value is not None:
                variables[name] = value
                unknown.discard(name)
        if era is not None:
            eras[name] = era
        changed.add(name)

    native = replace(incoming, variables=variables)
    rebound = previous is not None and _binding(previous) != _binding(native)
    if rebound and previous is not None:
        # Never carry old-owner or old-translation fields into a new binding.
        variables = dict(incoming.variables)
        if same_owner_kind and not incoming.capture_complete:
            for name in ("intensity", "max_intensity_value"):
                if name not in present and name in previous.variables:
                    variables[name] = previous.variables[name]
        native = replace(incoming, variables=variables)
        eras = {name: era for name, era in eras.items() if name in present}
        unknown = set(incoming.capture_unknown) | (set(previous.variables) - set(present))

    feedback = None if newer_source or rebound else record.feedback
    attempt = None if newer_source or rebound else record.attempt
    deadline_at = None if newer_source or rebound else record.deadline_at
    deadline_wall = None if newer_source or rebound else record.deadline_wall
    debt = record.native_debt or ((newer_source or rebound) and record.feedback is not None)
    new_obligation = (newer_source or rebound) and record.feedback is not None
    if feedback is not None and feedback.status in ("observed", "unconfirmed") and not debt:
        issued = dict(feedback.issued_eras)
        if any(
            name in changed and _not_known_preissue(provenance, name, issued)
            for name, _value in feedback.targets
        ):
            feedback = None
    if feedback is not None and feedback.status != "unconfirmed":
        prior_projected = feedback.projected
        projected = set(feedback.projected)
        unresolved = set(feedback.unresolved)
        ambiguous = set(feedback.ambiguous)
        issued = dict(feedback.issued_eras)
        targets = dict(feedback.targets)
        for name in changed & targets.keys():
            capture_era = _era(provenance, name)
            issue_era = issued.get(name)
            if capture_era is not None and issue_era is not None and capture_era < issue_era:
                continue
            projected.discard(name)
            variable = native.variables.get(name)
            if variable is not None and name not in unknown and variable.value == targets[name]:
                unresolved.discard(name)
                ambiguous.discard(name)
            else:
                unresolved.add(name)
                ambiguous.add(name)
        status = "observed" if not unresolved else "ambiguous" if ambiguous else "provisional"
        if not unresolved:
            projected.clear()
            debt = True
            new_obligation = True
        elif prior_projected and not projected:
            debt = True
            new_obligation = True
        feedback = replace(
            feedback,
            projected=frozenset(projected),
            unresolved=frozenset(unresolved),
            ambiguous=frozenset(ambiguous),
            status=status,
        )
    return replace(
        record,
        native=native,
        source_generation=generation,
        native_eras=tuple(eras.items()),
        unknown=frozenset(unknown),
        generation=record.generation + int(newer_source or rebound),
        feedback=feedback,
        attempt=attempt,
        deadline_at=deadline_at,
        deadline_wall=deadline_wall,
        native_debt=debt,
        native_debt_epoch=record.native_debt_epoch + int(new_obligation),
        revision=record.revision + 1,
    )


def reduce(record: Record, event: Event) -> Transition:
    """Apply one event without I/O; effects describe publication and timer ownership."""
    if record.closed:
        return Transition(record, Effects(False, None))
    updated = record
    publish = False
    if isinstance(event, Capture):
        updated = _capture(record, event.device)
        publish = updated is not record
    elif isinstance(event, Begin):
        generation = record.generation + 1
        updated = replace(record, generation=generation, attempt=Attempt(generation, event.targets))
    elif isinstance(event, Issue):
        if record.attempt is not None and record.attempt.generation == event.generation:
            updated = replace(
                record,
                attempt=replace(
                    record.attempt,
                    issued_eras=event.fields,
                    targets=event.values or record.attempt.targets,
                ),
            )
    elif isinstance(event, Outcome):
        attempt = record.attempt
        if attempt is not None and attempt.generation == event.generation:
            if event.kind == "superseded":
                updated = replace(record, attempt=None)
            elif event.kind != "success":
                updated = replace(
                    record,
                    attempt=None,
                    feedback=None,
                    deadline_at=None,
                    deadline_wall=None,
                    native_debt=True,
                    native_debt_epoch=record.native_debt_epoch + 1,
                    revision=record.revision + 1,
                )
                publish = True
            else:
                targets = dict(attempt.targets)
                issued = dict(attempt.issued_eras)
                native_eras = dict(record.native_eras)
                projected = set(targets)
                unresolved = set(targets)
                ambiguous: set[str] = set()
                for name, target in targets.items():
                    if name not in issued or native_eras.get(name, -1) < issued[name]:
                        continue
                    projected.discard(name)
                    native = (
                        record.native.variables.get(name) if record.native is not None else None
                    )
                    if native is not None and name not in record.unknown and native.value == target:
                        unresolved.discard(name)
                    else:
                        ambiguous.add(name)
                status = (
                    "observed" if not unresolved else "ambiguous" if ambiguous else "provisional"
                )
                deadline = record.deadline_at
                wall = record.deadline_wall
                if deadline is None:
                    deadline = event.now + WIRED_PROVISIONAL_SECONDS
                    wall = event.wall_now + WIRED_PROVISIONAL_SECONDS
                debt = record.native_debt or not unresolved
                updated = replace(
                    record,
                    attempt=None,
                    feedback=Feedback(
                        attempt.targets,
                        attempt.issued_eras,
                        frozenset(projected),
                        frozenset(unresolved),
                        frozenset(ambiguous),
                        status,
                    ),
                    deadline_at=deadline,
                    deadline_wall=wall,
                    native_debt=debt,
                    native_debt_epoch=record.native_debt_epoch
                    + int(not record.native_debt and debt),
                    revision=record.revision + 1,
                )
                publish = True
    elif isinstance(event, Expire):
        if record.deadline_at is not None and event.now >= record.deadline_at:
            feedback = record.feedback
            if feedback is not None:
                feedback = replace(
                    feedback,
                    projected=frozenset(),
                    status="unconfirmed",
                )
            updated = replace(
                record,
                feedback=feedback,
                deadline_at=None,
                deadline_wall=None,
                native_debt=True,
                native_debt_epoch=record.native_debt_epoch + 1,
                revision=record.revision + 1,
            )
            publish = True
    elif isinstance(event, SourceGeneration):
        if event.generation > record.source_generation:
            updated = replace(
                record,
                source_generation=event.generation,
                native_eras=(),
                generation=record.generation + 1,
                attempt=None,
                feedback=None,
                deadline_at=None,
                deadline_wall=None,
                native_debt=record.native_debt or record.feedback is not None,
                native_debt_epoch=record.native_debt_epoch + int(record.feedback is not None),
                revision=record.revision + 1,
            )
            publish = record.feedback is not None
    elif isinstance(event, Rebind):
        updated = replace(
            record,
            generation=record.generation + 1,
            attempt=None,
            feedback=None,
            deadline_at=None,
            deadline_wall=None,
            native_eras=(),
            native_debt=record.native_debt or record.feedback is not None,
            native_debt_epoch=record.native_debt_epoch + int(record.feedback is not None),
            revision=record.revision + 1,
        )
        publish = record.feedback is not None
    elif isinstance(event, PublishAccepted):
        matches_debt = (
            event.debt_epoch == record.native_debt_epoch
            if event.debt_epoch is not None
            else event.revision == record.revision
        )
        if event.native and matches_debt:
            feedback = record.feedback
            if feedback is not None and feedback.status in ("observed", "unconfirmed"):
                updated = replace(
                    record,
                    native_debt=False,
                    deadline_at=None,
                    deadline_wall=None,
                )
            else:
                updated = replace(record, native_debt=False)
            if record.native_debt and feedback is not None and feedback.projected:
                updated = replace(updated, revision=record.revision + 1)
                publish = True
    elif isinstance(event, PublishFailed):
        if event.revision == record.revision and event.native:
            updated = replace(
                record,
                native_debt=True,
                native_debt_epoch=record.native_debt_epoch + int(not record.native_debt),
            )
    elif isinstance(event, Shutdown):
        updated = replace(
            record,
            closed=True,
            attempt=None,
            feedback=None,
            deadline_at=None,
            deadline_wall=None,
            native_debt=False,
            revision=record.revision + 1,
        )
    return Transition(updated, Effects(publish, timer_deadline(updated)))
