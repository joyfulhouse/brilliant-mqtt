"""Immutable event, diagnostic and effect vocabulary for a mesh owner."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from brilliant_mqtt.model import DeviceKind, PrimitiveCapture, PrimitiveField

if TYPE_CHECKING:
    from brilliant_mqtt.mesh_settlement import MeshSettlement


class Phase(str, Enum):
    QUIET = "quiet"
    ISSUING = "issuing"
    UNRESOLVED = "unresolved"
    INCONCLUSIVE = "inconclusive"
    ACCEPTED = "accepted"
    TERMINAL = "terminal"
    INACTIVE = "inactive"


class OutcomeKind(str, Enum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class MeshMetadata:
    device_id: str
    peripheral_id: str
    name: str
    kind: DeviceKind
    peripheral_type: int = 0
    binding_dimmable: bool | None = None
    binding_max_intensity: int | None = None


@dataclass(frozen=True)
class Source:
    incarnation: str
    origin: str


@dataclass(frozen=True)
class Envelope:
    owner: MeshSettlement
    source: Source
    generation: int
    sequence: int
    captured_at: float
    peripheral_id: str
    wall_time: float | None = None


@dataclass(frozen=True)
class WindowToken:
    owner: MeshSettlement
    generation: int
    command_id: int
    sequence: int


@dataclass(frozen=True)
class TerminalObligation:
    command_id: int
    window: WindowToken | None
    sequence: int
    reason: str


@dataclass(frozen=True, eq=False)
class PublicationToken:
    """One fresh publication attempt; workers retain this exact object reference."""

    owner: MeshSettlement
    generation: int
    command_id: int
    window: WindowToken | None
    revision: int
    client_session: object
    terminal: TerminalObligation | None = None


@dataclass(frozen=True)
class CompletionHold:
    """Only the exact physically outstanding ticket crosses owner lifetimes."""

    ticket: object
    source: str
    generation: int
    issue_sequence: int


@dataclass(frozen=True)
class FieldEvidence:
    sequence: int
    captured_at: float
    generation: int
    value: str


@dataclass(frozen=True)
class OwnerView:
    phase: Phase
    status: str
    command_id: int
    ticket: object | None
    targets: tuple[tuple[str, str], ...]
    observed: tuple[tuple[str, PrimitiveField], ...]
    watermarks: tuple[tuple[str, int], ...]
    evidence: tuple[tuple[str, FieldEvidence], ...]
    projections: tuple[tuple[str, PrimitiveField], ...]
    window: WindowToken | None
    deadline: float | None
    wire_deadline: float | None
    return_sequence: int | None
    completion_hold: CompletionHold | None
    retirement_sequence: int
    revision: int
    ordinary_debt: int | None
    terminal: TerminalObligation | None
    last_hot: float | None
    in_flight: PublicationToken | None
    issue_boundary: Envelope | None
    return_boundary: Envelope | None
    outcome: OutcomeKind | None
    receipt: str | None


@dataclass(frozen=True)
class Observe:
    envelope: Envelope
    capture: PrimitiveCapture


@dataclass(frozen=True)
class SeedBinding:
    """Atomic creation snapshot, admitted only at this owner's own birth."""

    envelope: Envelope
    capture: PrimitiveCapture


@dataclass(frozen=True)
class UpdateMetadata:
    envelope: Envelope
    metadata: MeshMetadata


@dataclass(frozen=True)
class RequestPublication:
    envelope: Envelope


@dataclass(frozen=True)
class RetireAbsent:
    """Irreversibly close ingress, retaining only bounded absence publication."""

    envelope: Envelope


@dataclass(frozen=True)
class PeripheralAbsent:
    envelope: Envelope


@dataclass(frozen=True)
class ScopeUnknown:
    envelope: Envelope


@dataclass(frozen=True)
class Begin:
    envelope: Envelope
    ticket: object
    targets: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class Issue:
    envelope: Envelope
    command_id: int
    ticket: object
    targets: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class CallerTimeout:
    envelope: Envelope
    command_id: int
    ticket: object


@dataclass(frozen=True)
class NativeOutcome:
    envelope: Envelope
    command_id: int
    ticket: object
    kind: OutcomeKind
    receipt: str | None = None


@dataclass(frozen=True)
class WorkerCancelled:
    envelope: Envelope
    command_id: int
    ticket: object


@dataclass(frozen=True)
class Completion:
    envelope: Envelope
    ticket: object


@dataclass(frozen=True)
class Expire:
    envelope: Envelope
    window: WindowToken
    handled_at: float


@dataclass(frozen=True)
class Revoke:
    envelope: Envelope


@dataclass(frozen=True)
class AuxIssue:
    envelope: Envelope
    ticket: object
    targets: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class AuxAccepted:
    envelope: Envelope
    ticket: object


@dataclass(frozen=True)
class AuxRejected:
    envelope: Envelope
    ticket: object


@dataclass(frozen=True)
class AuxCancelled:
    envelope: Envelope
    ticket: object


@dataclass(frozen=True)
class PublishAccepted:
    envelope: Envelope
    token: PublicationToken


@dataclass(frozen=True)
class PublishStarted:
    envelope: Envelope
    token: PublicationToken


@dataclass(frozen=True)
class PublishFailed:
    envelope: Envelope
    token: PublicationToken


@dataclass(frozen=True)
class ArmExpiry:
    window: WindowToken
    deadline: float


@dataclass(frozen=True)
class CancelExpiry:
    window: WindowToken


@dataclass(frozen=True)
class RevokeEffects:
    """Join any obsolete lane child before starting another publish."""

    owner: MeshSettlement
    window: WindowToken | None


@dataclass(frozen=True)
class WakePublication:
    owner: MeshSettlement


Event = (
    Observe
    | SeedBinding
    | UpdateMetadata
    | RequestPublication
    | RetireAbsent
    | PeripheralAbsent
    | ScopeUnknown
    | Begin
    | Issue
    | CallerTimeout
    | NativeOutcome
    | WorkerCancelled
    | Completion
    | Expire
    | Revoke
    | AuxIssue
    | AuxAccepted
    | AuxRejected
    | AuxCancelled
    | PublishStarted
    | PublishAccepted
    | PublishFailed
)
Effect = ArmExpiry | CancelExpiry | RevokeEffects | WakePublication
