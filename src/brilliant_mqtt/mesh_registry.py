"""Loop-owned mesh lifetime and topology admission, before state reduction."""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from types import MappingProxyType

from brilliant_mqtt.mapping import AUX_SPECS
from brilliant_mqtt.mesh_ingress import (
    MeshCapture,
    MeshGeneration,
    MeshIngress,
    MeshInventory,
    MeshPeripheralAbsent,
    MeshScopeUnknown,
)
from brilliant_mqtt.mesh_settlement import MeshSettlement
from brilliant_mqtt.mesh_settlement_types import (
    Completion,
    CompletionHold,
    Effect,
    Envelope,
    Event,
    MeshMetadata,
    Observe,
    PeripheralAbsent,
    RetireAbsent,
    Revoke,
    ScopeUnknown,
    SeedBinding,
    Source,
    UpdateMetadata,
    WakePublication,
)
from brilliant_mqtt.model import CaptureProvenance
from brilliant_mqtt.protocols import MeshBusClient
from brilliant_mqtt.write_admission import AdmissionTicket

_FIELDS = frozenset(spec.var for specs in AUX_SPECS.values() for spec in specs) | frozenset(
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


@dataclass
class _Binding:
    owner: MeshSettlement
    metadata_sequence: int
    existence_sequence: int


class MeshRegistry:
    """Current bindings and bounded retired absence obligations; no state cache."""

    def __init__(
        self,
        bus: MeshBusClient,
        *,
        clock: Callable[[], float],
        wall_clock: Callable[[], float],
        derive_motion: bool = False,
        motion_hold_s: float = 60,
        native_motion_passthrough: bool = False,
    ) -> None:
        self.bus = bus
        self.clock = clock
        self.wall_clock = wall_clock
        self._derive_motion = derive_motion
        self._motion_hold_s = motion_hold_s
        self._native_motion_passthrough = native_motion_passthrough
        self.enabled = False
        self._disposed = False
        self._floor = -1
        self._born_at = 0.0
        self._bindings: dict[str, _Binding] = {}
        self._retired: dict[str, MeshSettlement] = {}
        self.effects: Callable[[MeshSettlement, tuple[Effect, ...]], None] = self._ignore_effects
        self.revoked: Callable[[MeshSettlement], None] = lambda owner: None
        self.removed: Callable[[MeshSettlement], None] = lambda owner: None
        self._remove_sink = bus.register_mesh_sink(
            self.ingest, want_fields=self.want_fields, want_inventory=lambda: self.enabled
        )
        self._remove_guard = bus.register_mesh_write_guard(self._write_allowed)

    @staticmethod
    def _ignore_effects(owner: MeshSettlement, effects: tuple[Effect, ...]) -> None:
        pass

    @property
    def owners(self) -> tuple[MeshSettlement, ...]:
        return tuple(binding.owner for binding in self._bindings.values())

    @property
    def retired(self) -> Mapping[str, MeshSettlement]:
        return MappingProxyType(self._retired)

    def current(self, peripheral_id: str) -> MeshSettlement | None:
        binding = self._bindings.get(peripheral_id)
        return binding.owner if binding is not None else None

    def live(self, owner: MeshSettlement, *, publication: bool = False) -> bool:
        pid = owner.metadata.peripheral_id
        registered = self.current(pid) is owner
        if publication:
            registered |= self._retired.get(pid) is owner
        return (
            self.enabled
            and registered
            and self.bus.mesh_admission_open
            and self.bus.mesh_owner_live(owner)
        )

    def want_fields(self, peripheral_id: str) -> frozenset[str] | None:
        return _FIELDS if self.enabled else None

    def acquire(self) -> None:
        if self._disposed:
            raise RuntimeError("mesh registry is disposed")
        if self.enabled:
            return
        boundary = self.bus.reserve_mesh_boundary("acquire")
        self._floor, self._born_at = boundary.sequence, boundary.captured_at
        self.enabled = self.bus.mesh_admission_open

    def envelope(self, owner: MeshSettlement, boundary: CaptureProvenance) -> Envelope:
        return Envelope(
            owner,
            Source(boundary.incarnation, boundary.origin),
            boundary.source_generation,
            boundary.sequence,
            boundary.captured_at,
            owner.metadata.peripheral_id,
            self.wall_clock() - self.clock() + boundary.captured_at,
        )

    def reserve(self, owner: MeshSettlement, origin: str) -> Envelope | None:
        boundary = self.bus.reserve_mesh_boundary(origin)
        return self.envelope(owner, boundary) if self.live(owner, publication=True) else None

    def dispatch(self, owner: MeshSettlement, event: Event) -> None:
        if self.live(owner, publication=True):
            self.effects(owner, owner.dispatch(event))

    def _write_allowed(self, ticket: AdmissionTicket, peripheral_id: str) -> bool:
        owner = ticket.mesh_owner
        return owner is not None and self.current(peripheral_id) is owner and self.live(owner)

    def _admitted(self, boundary: CaptureProvenance) -> bool:
        return (
            self.enabled
            and self.bus.mesh_admission_open
            and boundary.incarnation == self.bus.capture_source
            and boundary.source_generation == self.bus.capture_generation
            and boundary.sequence > self._floor
            and math.isfinite(boundary.captured_at)
            and boundary.captured_at >= self._born_at
        )

    def ingest(self, event: MeshIngress) -> None:
        boundary = event.provenance
        if isinstance(event, MeshGeneration):
            if self.enabled and boundary.incarnation == self.bus.capture_source:
                self._generation(boundary)
            return
        if not self._admitted(boundary):
            return
        if isinstance(event, MeshCapture):
            self._capture(event)
        elif isinstance(event, MeshScopeUnknown):
            for owner in self.owners:
                self.dispatch(owner, ScopeUnknown(self.envelope(owner, boundary)))
        elif isinstance(event, MeshPeripheralAbsent):
            binding = self._bindings.get(event.peripheral_id)
            if binding is not None:
                self._absence(binding, boundary)
        elif isinstance(event, MeshInventory):
            for pid, binding in tuple(self._bindings.items()):
                if pid not in event.peripheral_ids:
                    self._absence(binding, boundary)

    def _absence(self, binding: _Binding, boundary: CaptureProvenance) -> None:
        owner = binding.owner
        if not owner.accepts_capture(boundary):
            return
        # Field absence remains useful even if newer disjoint presence kept topology.
        self.dispatch(owner, PeripheralAbsent(self.envelope(owner, boundary)))
        if boundary.sequence <= binding.existence_sequence:
            return
        pid = owner.metadata.peripheral_id
        self._bindings.pop(pid, None)
        self._retired[pid] = owner
        self.bus.advance_mesh_creation_floor(boundary)
        self._release_native_listener(owner)
        self.revoked(owner)
        self.dispatch(owner, RetireAbsent(self.envelope(owner, boundary)))
        self.removed(owner)

    @staticmethod
    def _identity(metadata: MeshMetadata) -> tuple[object, ...]:
        return (
            metadata.device_id,
            metadata.peripheral_id,
            metadata.kind,
            metadata.peripheral_type,
            metadata.binding_dimmable,
            metadata.binding_max_intensity,
        )

    def _metadata(self, event: MeshCapture, binding: _Binding | None) -> MeshMetadata:
        metadata = event.metadata
        current = binding.owner if binding is not None else None
        dimmable = False if current is None else bool(current.metadata.binding_dimmable)
        scale = 1000 if current is None else current.metadata.binding_max_intensity or 1000
        capture, sequence = event.capture, event.provenance.sequence
        reported = (
            frozenset(field.name for field in capture.fields)
            if capture.complete
            else capture.reported
        )
        if capture.reliable:
            for field in capture.fields:
                if (
                    field.name not in reported
                    or current is not None
                    and sequence <= current.field_watermark(field.name)
                ):
                    continue
                if field.name == "intensity":
                    dimmable = True
                elif field.name == "max_intensity_value":
                    try:
                        parsed = int(field.value)
                    except ValueError:
                        parsed = 1000
                    scale = parsed if parsed > 0 else 1000
            removed = capture.removed
            if capture.complete:
                removed |= _FIELDS - frozenset(field.name for field in capture.fields)
            for name in removed:
                if current is not None and sequence <= current.field_watermark(name):
                    continue
                if name == "intensity":
                    dimmable = False
                elif name == "max_intensity_value":
                    scale = 1000
        return replace(
            metadata,
            binding_dimmable=dimmable,
            binding_max_intensity=scale,
        )

    def _capture(self, event: MeshCapture) -> None:
        boundary, pid = event.provenance, event.metadata.peripheral_id
        binding = self._bindings.get(pid)
        if binding is not None and (
            not self.live(binding.owner) or not binding.owner.accepts_capture(boundary)
        ):
            return
        if binding is None and boundary.sequence < self.bus.mesh_creation_floor:
            return
        metadata = self._metadata(event, binding)
        if binding is not None and boundary.sequence <= binding.metadata_sequence:
            metadata = replace(
                binding.owner.metadata,
                binding_dimmable=metadata.binding_dimmable,
                binding_max_intensity=metadata.binding_max_intensity,
            )
        changed = binding is not None and self._identity(metadata) != self._identity(
            binding.owner.metadata
        )
        if binding is None or changed:
            if binding is not None:
                self._revoke(binding.owner, boundary)
                self.bus.advance_mesh_creation_floor(boundary)
            retired = self._retired.pop(pid, None)
            if retired is not None:
                self._revoke(retired, boundary)
            seed = boundary.sequence >= self.bus.mesh_creation_floor
            birth = boundary if seed else self.bus.reserve_mesh_boundary("binding")
            # Reservation can apply reconnect fanout inline and replace the binding.
            if not self._admitted(birth) or (
                binding is not None and self._bindings.get(pid) is not binding
            ):
                return
            owner = self._new(metadata, birth)
            binding = _Binding(owner, boundary.sequence, boundary.sequence)
            self._bindings[pid] = binding
            if seed:
                self.dispatch(owner, SeedBinding(self.envelope(owner, birth), event.capture))
            self.effects(owner, (WakePublication(owner),))
            return
        owner = binding.owner
        binding.existence_sequence = max(binding.existence_sequence, boundary.sequence)
        if boundary.sequence > binding.metadata_sequence:
            binding.metadata_sequence = boundary.sequence
            self.dispatch(owner, UpdateMetadata(self.envelope(owner, boundary), metadata))
        self.dispatch(owner, Observe(self.envelope(owner, boundary), event.capture))

    def _new(
        self, metadata: MeshMetadata, boundary: CaptureProvenance, *, replaced: bool = False
    ) -> MeshSettlement:
        ticket = self.bus.outstanding_mesh_ticket(metadata.peripheral_id)
        issue = ticket.issued_at if ticket is not None else None
        hold = (
            CompletionHold(ticket, issue.incarnation, issue.source_generation, issue.sequence)
            if ticket is not None and issue is not None
            else None
        )
        owner = MeshSettlement(
            metadata,
            source=boundary.incarnation,
            generation=boundary.source_generation,
            birth_sequence=boundary.sequence,
            born_at=boundary.captured_at,
            completion_hold=hold,
            initial_unconfirmed=replaced,
            motion_hold_s=self._motion_hold_s,
            native_motion_passthrough=self._native_motion_passthrough,
            derive_motion=self._derive_motion,
        )
        if ticket is not None and hold is not None:

            def complete(raw: str | BaseException) -> None:
                returned = ticket.returned_at
                if returned is None or not self.live(owner):
                    return
                if returned.incarnation != owner.source:
                    returned = self.bus.reserve_mesh_boundary("inherited_completion")
                if self.live(owner):
                    self.dispatch(owner, Completion(self.envelope(owner, returned), ticket))

            self.bus.set_mesh_settle_callback(ticket, complete)
        return owner

    def _revoke(self, owner: MeshSettlement, boundary: CaptureProvenance) -> None:
        envelope = replace(
            self.envelope(owner, boundary),
            source=Source(owner.source, "revoke"),
            generation=owner.generation,
        )
        self.effects(owner, owner.dispatch(Revoke(envelope)))
        self._release_native_listener(owner)
        self.revoked(owner)

    def _release_native_listener(self, owner: MeshSettlement) -> None:
        ticket = self.bus.outstanding_mesh_ticket(owner.metadata.peripheral_id)
        if ticket is not None:
            ticket.clear_callbacks()

    def _generation(self, boundary: CaptureProvenance) -> None:
        if boundary.source_generation != self.bus.capture_generation:
            return
        for pid, binding in tuple(self._bindings.items()):
            old = binding.owner
            if old.generation == boundary.source_generation:
                continue
            self._revoke(old, boundary)
            successor = self._new(old.metadata, boundary, replaced=True)
            self._bindings[pid] = _Binding(successor, boundary.sequence, boundary.sequence)
            self.effects(successor, (WakePublication(successor),))
        for owner in tuple(self._retired.values()):
            self._revoke(owner, boundary)
        self._retired.clear()

    def finish_retired(self, owner: MeshSettlement) -> None:
        pid = owner.metadata.peripheral_id
        if self._retired.get(pid) is owner:
            boundary = self.bus.reserve_mesh_boundary("absence_cleanup")
            if self._retired.get(pid) is owner:
                self._retired.pop(pid)
                self._revoke(owner, boundary)

    def close(self) -> None:
        self.enabled = False
        boundary = self.bus.reserve_mesh_boundary("withdraw")
        for owner in self.owners + tuple(self._retired.values()):
            self._revoke(owner, boundary)
        self._bindings.clear()
        self._retired.clear()

    def dispose(self) -> None:
        """End this registry's session registrations; close() alone permits reacquisition."""
        if self._disposed:
            return
        self._disposed = True
        self.close()
        self._remove_sink()
        self._remove_guard()
