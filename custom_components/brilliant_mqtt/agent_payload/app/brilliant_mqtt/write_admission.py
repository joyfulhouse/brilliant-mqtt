"""Narrow admission ticket shared by MQTT command lanes and bus writes."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextvars import ContextVar
from dataclasses import dataclass, field
from enum import Enum


class WriteClass(Enum):
    INTERACTIVE_FIFO = "interactive_fifo"
    INTERACTIVE_LATEST = "interactive_latest"
    MAINTENANCE = "maintenance"


@dataclass(frozen=True)
class Superseded:
    """The waiting caller's payload was replaced before native issue; not an error."""


WriteResult = str | Superseded


class WriteCancelled(asyncio.CancelledError):
    """The adapter aborted this write; it did not cancel the caller's task.

    DELIBERATELY an ``asyncio.CancelledError`` subclass so the admission/lock
    machinery can tell an INTERNAL supersession apart from a genuine
    ``asyncio.Task.cancel()`` (which raises a base ``CancelledError``). This
    discriminator must be preserved; contain it at the PUBLIC maintenance/
    reconcile boundary (see :class:`WriteAborted`) rather than by widening it.
    """


class WriteAborted(RuntimeError):
    """A write ended WITHOUT issuing (e.g. an internal supersession) surfaced to a
    maintenance/reconcile caller as an ordinary Exception.

    :class:`WriteCancelled` is a BaseException (a ``CancelledError`` subclass) so
    it escapes every ``except Exception`` boundary — which would let an internal
    supersession propagate a stray cancellation into ``reconcile()``, the
    reconnect re-reconcile task, or the mesh-leader tick (terminating the
    supervisor). Converting it to this RuntimeError at that boundary lets the
    existing ``except Exception`` contain it, while a genuine
    ``asyncio.CancelledError`` still propagates untouched.
    """


@dataclass(eq=False)
class AdmissionTicket:
    """Opaque identity: routing, payload and session ownership stay in the adapter."""

    _owner: object | None = field(default=None, init=False, repr=False)
    _cancel_waiting: Callable[[], None] | None = field(default=None, init=False, repr=False)

    def cancel_waiting(self) -> None:
        """Relinquish lane ownership; the adapter never cancels an issued RPC."""
        if self._cancel_waiting is not None:
            self._cancel_waiting()


@dataclass
class CommandAdmission:
    """One dequeued MQTT message; its bridge installs the validated replacement hook."""

    ticket: AdmissionTicket = field(default_factory=AdmissionTicket)
    try_supersede: Callable[[str], bool] | None = None


command_admission: ContextVar[CommandAdmission | None] = ContextVar(
    "command_admission", default=None
)
