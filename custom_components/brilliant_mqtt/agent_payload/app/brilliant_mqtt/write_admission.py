"""Narrow admission ticket shared by MQTT command lanes and bus writes."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextvars import ContextVar
from dataclasses import dataclass, field
from enum import Enum

from brilliant_mqtt.model import CaptureProvenance


class WriteClass(Enum):
    INTERACTIVE_FIFO = "interactive_fifo"
    INTERACTIVE_LATEST = "interactive_latest"
    MAINTENANCE = "maintenance"


@dataclass(frozen=True)
class Superseded:
    """The waiting caller's payload was replaced before native issue; not an error."""


WriteResult = str | Superseded


class TicketAdoptionError(ValueError):
    """The supplied ticket cannot be adopted for this write."""


class WriteCancelled(asyncio.CancelledError):
    """The adapter aborted this write; it did not cancel the caller's task.

    DELIBERATELY an ``asyncio.CancelledError`` subclass so the admission/lock
    machinery can tell an INTERNAL supersession apart from a genuine
    ``asyncio.Task.cancel()`` (which raises a base ``CancelledError``). This
    discriminator must be preserved; contain it at the PUBLIC maintenance/
    reconcile boundary as a benign no-op rather than by widening it.
    """


@dataclass(eq=False)
class AdmissionTicket:
    """Opaque identity: routing, payload and session ownership stay in the adapter."""

    _owner: object | None = field(default=None, init=False, repr=False)
    _cancel_waiting: Callable[[], None] | None = field(default=None, init=False, repr=False)
    _issued_at: CaptureProvenance | None = field(default=None, init=False, repr=False)
    _on_issued: Callable[[CaptureProvenance], None] | None = field(
        default=None, init=False, repr=False
    )

    def cancel_waiting(self) -> None:
        """Relinquish lane ownership; the adapter never cancels an issued RPC."""
        if self._cancel_waiting is not None:
            self._cancel_waiting()

    def set_issue_callback(self, callback: Callable[[CaptureProvenance], None]) -> None:
        """Observe the adapter's actual native-issue boundary for this ticket."""
        self._on_issued = callback
        if self._issued_at is not None:
            callback(self._issued_at)

    def mark_issued(self, provenance: CaptureProvenance) -> None:
        """Record the issue boundary once; called only by a BusClient."""
        if self._issued_at is not None:
            return
        self._issued_at = provenance
        if self._on_issued is not None:
            self._on_issued(provenance)


@dataclass
class CommandAdmission:
    """One dequeued MQTT message; its bridge installs the validated replacement hook."""

    ticket: AdmissionTicket = field(default_factory=AdmissionTicket)
    try_supersede: Callable[[str], bool] | None = None


command_admission: ContextVar[CommandAdmission | None] = ContextVar(
    "command_admission", default=None
)
