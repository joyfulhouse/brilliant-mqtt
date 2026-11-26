"""Bridge command associations; the registry's owner alone retains settlement state."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass

from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.mesh_driver import MeshDriver
from brilliant_mqtt.mesh_registry import MeshRegistry
from brilliant_mqtt.mesh_settlement import MeshSettlement
from brilliant_mqtt.mesh_settlement_types import (
    AdmissionRefused,
    AuxAccepted,
    AuxCancelled,
    AuxIssue,
    AuxRejected,
    Begin,
    CallerTimeout,
    Completion,
    Envelope,
    Issue,
    NativeOutcome,
    OutcomeKind,
    WorkerCancelled,
)
from brilliant_mqtt.model import CaptureProvenance
from brilliant_mqtt.protocols import BusClient
from brilliant_mqtt.write_admission import (
    AdmissionTicket,
    CommandAdmission,
    Superseded,
    TicketAdoptionError,
    WriteCancelled,
    WriteClass,
    WriteResult,
    command_admission,
)


@dataclass(eq=False)
class _Association:
    owner: MeshSettlement
    ticket: AdmissionTicket
    command_id: int
    targets: tuple[tuple[str, str], ...]
    primary: bool


@dataclass
class _Fold:
    association: _Association
    replace: Callable[[str], bool]

    def __call__(self, payload: str) -> bool:
        return self.replace(payload)


class MeshCommands:
    """Bounded live listeners, never a second mesh pending/evidence cache."""

    def __init__(self, registry: MeshRegistry, driver: MeshDriver, bus: BusClient) -> None:
        self.registry, self.driver, self.bus = registry, driver, bus
        self._associations: dict[AdmissionTicket, _Association] = {}
        registry.revoked = self.revoke

    def revoke(self, owner: MeshSettlement) -> None:
        for ticket, association in tuple(self._associations.items()):
            if association.owner is owner:
                ticket.clear_callbacks()
                ticket.cancel_waiting()
                self._associations.pop(ticket, None)

    @staticmethod
    def _targets(sets: list[VarSet]) -> tuple[tuple[str, str], ...]:
        return tuple((item.name, item.value) for item in sets)

    def _associate(
        self,
        owner: MeshSettlement,
        ticket: AdmissionTicket,
        sets: list[VarSet],
        *,
        primary: bool,
        envelope: Envelope,
    ) -> _Association:
        if primary:
            # Preserve the physically outstanding predecessor listener. Its return
            # clears only its exact hold, even when the new intent already exists.
            for previous in tuple(self._associations.values()):
                if previous.owner is owner and previous.primary and previous.ticket is not ticket:
                    self._completion_only(previous)
            self.registry.dispatch(owner, Begin(envelope, ticket, self._targets(sets)))
        association = _Association(
            owner, ticket, owner.view.command_id, self._targets(sets), primary
        )
        self._associations[ticket] = association
        self._bind(association)
        return association

    def _bind(self, association: _Association) -> None:
        owner, ticket = association.owner, association.ticket

        def issue(boundary: CaptureProvenance) -> None:
            if not self.registry.live(owner):
                return
            envelope = self.registry.envelope(owner, boundary)
            if association.primary:
                self.registry.dispatch(
                    owner, Issue(envelope, association.command_id, ticket, association.targets)
                )
            else:
                self.registry.dispatch(owner, AuxIssue(envelope, ticket, association.targets))

        def settled(raw: str | BaseException) -> None:
            if self._associations.get(ticket) is association:
                self._associations.pop(ticket)
            boundary = ticket.returned_at
            if boundary is None or not self.registry.live(owner):
                return
            envelope = self.registry.envelope(owner, boundary)
            if association.primary:
                kind = (
                    OutcomeKind.ACCEPTED
                    if isinstance(raw, str)
                    else OutcomeKind.TIMEOUT
                    if isinstance(raw, (TimeoutError, asyncio.TimeoutError))
                    else OutcomeKind.CANCELLED
                    if isinstance(raw, asyncio.CancelledError)
                    else OutcomeKind.REJECTED
                )
                self.registry.dispatch(
                    owner,
                    NativeOutcome(
                        envelope,
                        association.command_id,
                        ticket,
                        kind,
                        raw if isinstance(raw, str) else None,
                    ),
                )
                self.registry.dispatch(owner, Completion(envelope, ticket))
            else:
                event = AuxAccepted if isinstance(raw, str) else AuxRejected
                self.registry.dispatch(owner, event(envelope, ticket))

        def refused() -> None:
            if self._associations.get(ticket) is association:
                self._associations.pop(ticket)
            envelope = self.registry.reserve(owner, "refused")
            if envelope is not None and association.primary and ticket.issued_at is None:
                self.registry.dispatch(
                    owner, AdmissionRefused(envelope, association.command_id, ticket)
                )

        self.registry.bus.bind_mesh_ticket(
            ticket, owner, on_issue=issue, on_settle=settled, on_refused=refused
        )

    def _completion_only(self, association: _Association) -> None:
        owner, ticket = association.owner, association.ticket
        if not self.registry.live(owner):
            # Registry replacement already handed this physical ticket's listener
            # to the successor. An obsolete caller must not revoke that handoff.
            if self._associations.get(ticket) is association:
                self._associations.pop(ticket)
            return
        ticket.clear_callbacks()
        ticket.cancel_waiting()
        if self._associations.get(ticket) is association:
            self._associations.pop(ticket)
        if ticket.issued_at is None:
            return

        def complete(raw: str | BaseException) -> None:
            if self._associations.get(ticket) is association:
                self._associations.pop(ticket)
            boundary = ticket.returned_at
            if boundary is not None and self.registry.live(owner):
                self.registry.dispatch(
                    owner, Completion(self.registry.envelope(owner, boundary), ticket)
                )

        self._associations[ticket] = association
        self.registry.bus.set_mesh_settle_callback(ticket, complete)

    def _install_fold(
        self,
        admission: CommandAdmission,
        association: _Association,
        decode: Callable[[str], list[VarSet]],
    ) -> None:
        owner = association.owner

        def replace(payload: str) -> bool:
            if admission.mesh_owner is not owner or not self.registry.live(owner):
                return False
            envelope = self.registry.reserve(owner, "fold")
            if envelope is None or not self.registry.live(owner):
                return False
            replacement = decode(payload)
            if not replacement or not self.bus.try_supersede(admission.ticket, replacement):
                return False
            # No await after replacement: issue sees this command and these listeners.
            current = self._associate(
                owner, admission.ticket, replacement, primary=association.primary, envelope=envelope
            )
            self._install_fold(admission, current, decode)
            return True

        admission.try_supersede = _Fold(association, replace)

    def _abandon(self, association: _Association) -> None:
        owner, ticket = association.owner, association.ticket
        current = self._associations.get(ticket, association)
        envelope = self.registry.reserve(owner, "worker_cancelled")
        if envelope is not None:
            if current.primary:
                event = WorkerCancelled if ticket.issued_at is not None else AdmissionRefused
                self.registry.dispatch(owner, event(envelope, current.command_id, ticket))
            else:
                self.registry.dispatch(owner, AuxCancelled(envelope, ticket))
        self._completion_only(current)

    async def _flush(self, association: _Association) -> None:
        try:
            await self.driver.flush(association.owner)
        except asyncio.CancelledError:
            self._abandon(association)
            raise

    async def execute(
        self,
        owner: MeshSettlement,
        sets: list[VarSet],
        *,
        primary: bool,
        decode: Callable[[str], list[VarSet]] | None = None,
        maintenance: bool = False,
    ) -> WriteResult:
        if not self.registry.live(owner):
            return Superseded()
        admission = command_admission.get() if not maintenance else None
        if admission is not None and admission.mesh_owner is not owner:
            return Superseded()
        ticket = AdmissionTicket() if admission is None else admission.ticket
        hook = admission.try_supersede if admission is not None else None
        association = (
            hook.association if isinstance(hook, _Fold) else self._associations.get(ticket)
        )
        if association is not None and association.owner is not owner:
            return Superseded()
        if association is None or association.targets != self._targets(sets):
            envelope = self.registry.reserve(owner, "command")
            if envelope is None or not self.registry.live(owner):
                return Superseded()
            association = self._associate(owner, ticket, sets, primary=primary, envelope=envelope)
        if admission is not None and decode is not None:
            self._install_fold(admission, association, decode)
        write_class = (
            WriteClass.MAINTENANCE
            if maintenance
            else WriteClass.INTERACTIVE_LATEST
            if decode is not None
            else WriteClass.INTERACTIVE_FIFO
        )
        try:
            try:
                result = await self.bus.set_variables(
                    "ble_mesh",
                    owner.metadata.peripheral_id,
                    sets,
                    write_class=write_class,
                    ticket=ticket,
                )
            except TicketAdoptionError:
                if not self.registry.live(owner):
                    return Superseded()
                self._completion_only(association)
                envelope = self.registry.reserve(owner, "adoption_replacement")
                if envelope is None or not self.registry.live(owner):
                    return Superseded()
                ticket = AdmissionTicket()
                association = self._associate(
                    owner, ticket, sets, primary=primary, envelope=envelope
                )
                if admission is not None:
                    admission.ticket = ticket
                    if decode is not None:
                        self._install_fold(admission, association, decode)
                result = await self.bus.set_variables(
                    "ble_mesh",
                    owner.metadata.peripheral_id,
                    sets,
                    write_class=write_class,
                    ticket=ticket,
                )
        except (TimeoutError, asyncio.TimeoutError):
            envelope = self.registry.reserve(owner, "caller_timeout")
            if (
                envelope is not None
                and primary
                and not ticket.settled
                and ticket.issued_at is not None
            ):
                self.registry.dispatch(
                    owner, CallerTimeout(envelope, association.command_id, ticket)
                )
            await self._flush(association)
            raise
        except WriteCancelled:
            await self._flush(association)
            return Superseded()
        except asyncio.CancelledError:
            self._abandon(association)
            raise
        except Exception:
            await self._flush(association)
            raise
        await self._flush(association)
        return result
