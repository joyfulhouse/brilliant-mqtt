"""Observation-confirmed mesh primary writes: the pending-write state machine.

A mesh ``set_variables`` can be RPC-accepted yet never actuate: the BLE layer
drops it silently and the notification-fed mirror carries the accepted value
until the backend reconciles (75.2 s observed maximum) — so a matching
observation proves nothing, while a CONTRADICTING one proves failure (#46/#47).

:class:`MeshPending` owns every record that state machine needs — pending
writes, the confirm/settle task registry, per-load write generations, the
published feedback records and the feedback-enabled flag — and exposes them
only through behaviour methods and read-only views. It has no bus or MQTT
dependency: publishing is injected (``republish``/``echo``), as are the clocks
and the sleeper, so it is unit-testable in isolation.

A write that outlives the bus adapter's caller deadline is UNRESOLVED (#72),
not failed: its native RPC keeps running detached. While it is unresolved the
mirror still carries the PRE-write value, so a differing observation is not a
contradiction; the RPC's late outcome, delivered through the admission
ticket's settle callback (#225), decides what happens next.
"""

from __future__ import annotations

import asyncio
import logging
import math
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, replace
from types import MappingProxyType

from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.model import BrilliantDevice
from brilliant_mqtt.write_admission import SettleOutcome

# Deliberately the bridge's logger name: this module was extracted from
# bridge.py, and operators' log filters and alerts (and the tests) key the
# mesh confirmation receipts on "brilliant_mqtt.bridge".
logger = logging.getLogger("brilliant_mqtt.bridge")

# How long a commanded mesh value must be observed stable before it is
# published as truth. Clears the 75.2s measured revert tail (495 recorded
# false-off events, p99 59.5s). Fixed by design — deliberately not
# configurable.
MESH_CONFIRM_SECONDS = 80.0

# An observation older than this at the confirm deadline cannot attest the
# outcome — the pending closes unconfirmed and state stays unknown (the
# stale-stream watchdog owns stream recovery).
MESH_CONFIRM_MAX_OBSERVATION_AGE_S = 10.0

# Confirm receipts log at INFO once every N confirmations (contradiction
# receipts always log at WARNING — those are the known-failed deliveries).
_MESH_RECEIPT_LOG_SAMPLE_EVERY = 10

# Placeholder receipt for a mesh write whose RPC outran the bus adapter's
# caller deadline (#72): the write is UNRESOLVED, not failed — it keeps running
# detached in the adapter — so the pending record is armed as for an accepted
# write. Logged wherever a receipt is; replaced by the late receipt (#225).
PENDING_RPC_RECEIPT = "<rpc pending after caller deadline>"


@dataclass
class PendingMeshWrite:
    """One in-flight mesh primary write awaiting observation-based confirmation.

    Each command creates a fresh record, so object identity IS the generation
    tag: a stale confirm task detects supersession by identity check.
    """

    # Commanded variable values (name -> string value), compared verbatim
    # against observed snapshots: any differing value is a contradiction.
    targets: dict[str, str]
    # Normalized transport-ack receipt from BusClient.set_variables — logged
    # on contradiction (known-failed delivery) and sampled on confirm.
    receipt: str
    generation: int
    status: str = "pending"
    deadline: float | None = None
    # Bridge-clock time of the newest observation matching the targets.
    last_observed_at: float | None = None
    # True only while the native RPC is still running past the caller deadline
    # (#225): the mirror then still carries the PRE-write value, so a differing
    # observation is ignored rather than treated as a contradiction.
    rpc_unresolved: bool = False


def _is_inconclusive(outcome: SettleOutcome) -> bool:
    """A late native timeout or cancellation tells nothing about actuation."""
    return isinstance(outcome, (asyncio.TimeoutError, TimeoutError, asyncio.CancelledError))


class MeshPending:
    """Pending-write records, confirm timers, generations and feedback state."""

    def __init__(
        self,
        *,
        clock: Callable[[], float],
        wall_clock: Callable[[], float],
        sleep: Callable[[float], Awaitable[None]],
        republish: Callable[[str, bool], Awaitable[None]],
        echo: Callable[[str, list[VarSet]], Awaitable[None]],
        before_feedback_change: Callable[[str], None],
    ) -> None:
        self._clock = clock
        self._wall_clock = wall_clock
        self._sleep = sleep
        # republish(peripheral_id, force): publish the stored snapshot.
        self._republish = republish
        # echo(peripheral_id, sets): fold confirmed targets into the snapshot.
        self._echo = echo
        # Runs BEFORE every feedback record change: cancels in-flight feedback
        # publications for the load and invalidates its diff caches.
        self._before_feedback_change = before_feedback_change
        # peripheral_id → in-flight write awaiting observation-based confirmation.
        self._pending: dict[str, PendingMeshWrite] = {}
        # Confirm (and settle) tasks, keyed OUTSIDE the pending record: a
        # resolver deletes its pending entry before publishing the
        # confirmation, and must stay cancellable (supersession, withdraw)
        # through that final publish.
        self._confirm_tasks: dict[str, asyncio.Task[None]] = {}
        # Monotonic per-peripheral command counter — NEVER cleared or reused
        # within a Bridge lifetime, so a stalled bus write from before a
        # withdraw can never match a generation minted after re-acquisition.
        self._generation: dict[str, int] = {}
        self._feedback: dict[str, PendingMeshWrite] = {}
        self._feedback_enabled = True
        self._confirm_count = 0

    # -- read-only views (tests and diagnostics) ---------------------------

    @property
    def pending_view(self) -> Mapping[str, PendingMeshWrite]:
        return MappingProxyType(self._pending)

    @property
    def confirm_tasks_view(self) -> Mapping[str, asyncio.Task[None]]:
        return MappingProxyType(self._confirm_tasks)

    @property
    def feedback_view(self) -> Mapping[str, PendingMeshWrite]:
        return MappingProxyType(self._feedback)

    def generation(self, peripheral_id: str) -> int:
        return self._generation.get(peripheral_id, 0)

    # -- feedback ownership -------------------------------------------------

    @property
    def feedback_enabled(self) -> bool:
        return self._feedback_enabled

    def enable_feedback(self) -> None:
        self._feedback_enabled = True

    def feedback_owner(self, peripheral_id: str) -> tuple[int, PendingMeshWrite | None]:
        """The (generation, record) a feedback publication must still match."""
        return self._generation.get(peripheral_id, 0), self._feedback.get(peripheral_id)

    def owns(self, peripheral_id: str, generation: int, record: PendingMeshWrite | None) -> bool:
        return (
            self._feedback_enabled
            and self._generation.get(peripheral_id, 0) == generation
            and self._feedback.get(peripheral_id) is record
        )

    def is_current(self, peripheral_id: str, generation: int) -> bool:
        return self._generation.get(peripheral_id, 0) == generation

    def set_feedback(self, peripheral_id: str, record: PendingMeshWrite | None) -> None:
        self._before_feedback_change(peripheral_id)
        if record is None:
            self._feedback.pop(peripheral_id, None)
        else:
            self._feedback[peripheral_id] = record

    def revoke_feedback(self) -> None:
        """Fence every feedback publication (withdrawal / session teardown).

        Existing confirm timers are NOT swept here; they find their ownership
        gone and release their own registry entries.
        """
        self._feedback_enabled = False
        self._feedback.clear()
        for peripheral_id in self._generation:
            self._generation[peripheral_id] += 1

    def project(self, peripheral_id: str, fields: dict[str, object]) -> dict[str, object]:
        """Overlay ``state: null`` while a mesh write awaits confirmation.

        ONLY the primary state key is withheld — null renders `unknown`
        through the primary light/switch template. Every aux key keeps its
        real observed value: the aux value templates collapse null into a
        false OFF (issue #47), which would invert safety-meaning entities.
        """
        if "mesh_write_status" not in fields:
            return fields
        feedback = self._feedback.get(peripheral_id)
        if feedback is None:
            return fields
        projected = {
            **fields,
            "mesh_write_status": feedback.status,
            "mesh_requested": feedback.targets,
            "mesh_write_deadline": feedback.deadline,
        }
        if feedback.status in ("pending", "unconfirmed"):
            projected["state"] = None
        return projected

    # -- write lifecycle ----------------------------------------------------

    def begin(self, peripheral_id: str) -> int:
        """Start a new write: bump the generation and revoke the old pending.

        Runs before any await: a newer command supersedes an older one the
        moment it starts, even while that write is in flight — the old pending
        must not keep confirming, contradicting, or expiring against a target
        that is no longer wanted.
        """
        generation = self._generation.get(peripheral_id, 0) + 1
        self._generation[peripheral_id] = generation
        self.drop(peripheral_id)
        self.set_feedback(
            peripheral_id,
            PendingMeshWrite(targets={}, receipt="", generation=generation, status="superseded")
            if peripheral_id in self._feedback
            else None,
        )
        return generation

    def arm(
        self,
        peripheral_id: str,
        generation: int,
        targets: Mapping[str, str],
        receipt: str,
        *,
        rpc_unresolved: bool = False,
    ) -> PendingMeshWrite | None:
        """Arm the pending record and its confirm timer; None when superseded."""
        if not self.is_current(peripheral_id, generation):
            # Superseded while this write awaited; the newer command's pending
            # (and confirm timer) owns the peripheral now.
            return None
        pending = PendingMeshWrite(
            targets=dict(targets),
            receipt=receipt,
            generation=generation,
            deadline=self._fresh_deadline(),
            rpc_unresolved=rpc_unresolved,
        )
        self._start_window(peripheral_id, pending)
        return pending

    async def publish_failure(self, peripheral_id: str, generation: int) -> None:
        """Publish ``failed`` with the last observed state, forced past every cache.

        Nothing was fabricated, so the last observed snapshot is still the
        best truth. Shared by the immediate and the late (#225) failure path.
        """
        self.set_feedback(
            peripheral_id,
            PendingMeshWrite(targets={}, receipt="", generation=generation, status="failed"),
        )
        await self._republish(peripheral_id, True)

    def observe(self, device: BrilliantDevice) -> None:
        """Feed one bus observation into the pending-write state machine.

        A value CONTRADICTING the commanded target proves the write did not
        take (the mirror only ever lies TOWARD the accepted value) — cancel
        immediately; the caller's publish that follows surfaces the observed
        truth. Exception (#225): while the native RPC is still unresolved the
        mirror still carries the PRE-write value, so a differing value proves
        nothing and is ignored. A MATCHING value is deliberately NOT
        confirmation (the mirror can carry the accepted-but-undelivered value
        for ~75s); it only refreshes the observation clock the confirm
        deadline checks — and not even that while the RPC is unresolved.
        """
        peripheral_id = device.peripheral_id
        pending = self._pending.get(peripheral_id)
        if pending is None:
            feedback = self._feedback.get(peripheral_id)
            if feedback is not None and feedback.status != "superseded":
                self.set_feedback(peripheral_id, None)
            return
        if not self.owns(peripheral_id, pending.generation, pending):
            return
        fully_matched = True
        for name, want in pending.targets.items():
            var = device.variables.get(name)
            if var is None:
                fully_matched = False
            elif var.value != want:
                if pending.rpc_unresolved:
                    return
                self.drop(peripheral_id)
                self.set_feedback(
                    peripheral_id,
                    replace(pending, targets={}, status="contradicted", deadline=None),
                )
                logger.warning(
                    "mesh write contradicted by observation for %s (%s=%s, wanted %s); "
                    "publishing observed state (receipt: %s)",
                    peripheral_id,
                    name,
                    var.value,
                    want,
                    pending.receipt,
                )
                return
        # A match seen while the RPC is unresolved predates any acceptance:
        # it must not let the resolver confirm at expiry (#225).
        if fully_matched and not pending.rpc_unresolved:
            pending.last_observed_at = self._clock()

    def on_rpc_settled(
        self, peripheral_id: str, pending: PendingMeshWrite, outcome: SettleOutcome
    ) -> None:
        """Settle callback for a detached RPC (#225); runs inside the bus adapter.

        Synchronous and non-blocking. A late timeout or cancellation learns
        nothing: contradiction handling resumes against the ORIGINAL deadline.
        A late acceptance or failure replaces the confirm timer with a settle
        task registered in the same registry, so supersession and withdrawal
        reach it exactly as they reach a resolver.
        """
        if not pending.rpc_unresolved or not self._owns_pending(peripheral_id, pending):
            return
        if _is_inconclusive(outcome):
            pending.rpc_unresolved = False
            logger.warning(
                "mesh write for %s ended without an outcome after the caller deadline (%s); "
                "contradicting observations count again",
                peripheral_id,
                type(outcome).__name__,
            )
            return
        prior = self._confirm_tasks.pop(peripheral_id, None)
        if prior is not None:
            prior.cancel()
        self._confirm_tasks[peripheral_id] = asyncio.create_task(
            self.settle_unresolved(peripheral_id, pending, outcome),
            name=f"brilliant-mqtt-mesh-settle-{peripheral_id}",
        )

    async def settle_unresolved(
        self, peripheral_id: str, pending: PendingMeshWrite, outcome: SettleOutcome
    ) -> None:
        """Apply a detached RPC's late acceptance or failure (background task)."""
        if self._confirm_tasks.get(
            peripheral_id
        ) is not asyncio.current_task() or not self._owns_pending(peripheral_id, pending):
            self.release_confirm_task(peripheral_id)
            return
        if isinstance(outcome, str):
            # Late acceptance: the native RPC only now took the write, so the
            # #46 revert tail starts now — a FULL fresh window, and no
            # pre-acceptance observation may back the confirmation.
            rearmed = replace(
                pending,
                receipt=outcome,
                deadline=self._fresh_deadline(),
                last_observed_at=None,
                rpc_unresolved=False,
            )
            logger.info(
                "mesh write for %s accepted after the caller deadline; confirm window "
                "restarted (receipt: %s)",
                peripheral_id,
                outcome,
            )
            # Publish the re-armed window while THIS task is still the
            # registered handle, so supersession or withdrawal cancels it
            # mid-publish; only then hand the registry over to the resolver.
            self._pending[peripheral_id] = rearmed
            self.set_feedback(peripheral_id, rearmed)
            try:
                await self._republish(peripheral_id, False)
            except Exception:
                logger.warning("mesh re-arm publish failed for %s", peripheral_id, exc_info=True)
            if self._confirm_tasks.get(peripheral_id) is not asyncio.current_task():
                # Only drop() takes a settle task's registry entry, and it
                # cancels the task. A cancellation landing mid-publish arrives
                # with the publication's revocation and is swallowed there
                # (py3.10 cannot tell them apart), so restore it here.
                raise asyncio.CancelledError
            if not self._owns_pending(peripheral_id, rearmed):
                self.release_confirm_task(peripheral_id)
                return
            self._start_resolver(peripheral_id, rearmed)
            return
        del self._pending[peripheral_id]
        logger.warning(
            "mesh write failed after the caller deadline for %s (%s); "
            "republishing last observed state",
            peripheral_id,
            type(outcome).__name__,
        )
        try:
            await self.publish_failure(peripheral_id, pending.generation)
        except Exception:
            logger.warning("mesh failure publish failed for %s", peripheral_id, exc_info=True)
        finally:
            self.release_confirm_task(peripheral_id)

    async def resolve(self, peripheral_id: str, pending: PendingMeshWrite) -> None:
        """Deadline arm of the pending-write state machine (background task).

        Runs OUTSIDE the MQTT command lane so confirmation never blocks the
        next command. Sleeps out the confirmation window, then — if *pending*
        still owns the peripheral — publishes the commanded value only when a
        fresh observation backs it; otherwise closes unconfirmed and leaves
        state unknown (the stale-stream watchdog owns stream recovery).

        Ownership gate at publish time: the identity check below and the start
        of the confirm publish sit in one event-loop slice (no await between
        them), and any later ownership change (supersession, withdraw) reaches
        this task through the confirm-task registry as cancellation —
        including while the final publish is still in flight.
        """
        await self._sleep(MESH_CONFIRM_SECONDS)
        if not self._owns_pending(peripheral_id, pending):
            self.release_confirm_task(peripheral_id)
            return
        del self._pending[peripheral_id]
        if (
            pending.last_observed_at is None
            or (self._clock() - pending.last_observed_at) > MESH_CONFIRM_MAX_OBSERVATION_AGE_S
        ):
            logger.warning(
                "mesh write for %s unconfirmed after %.0fs (no fresh observation); "
                "state stays unknown (receipt: %s)",
                peripheral_id,
                MESH_CONFIRM_SECONDS,
                pending.receipt,
            )
            self.set_feedback(
                peripheral_id,
                replace(pending, targets={}, status="unconfirmed", deadline=None),
            )
            try:
                await self._republish(peripheral_id, False)
            except Exception:
                logger.warning("mesh expiry publish failed for %s", peripheral_id, exc_info=True)
        else:
            self.set_feedback(peripheral_id, None)
            self._confirm_count += 1
            if self._confirm_count % _MESH_RECEIPT_LOG_SAMPLE_EVERY == 1:
                logger.info(
                    "mesh write confirmed by observation for %s: %s (receipt: %s)",
                    peripheral_id,
                    pending.targets,
                    pending.receipt,
                )
            try:
                await self._echo(
                    peripheral_id,
                    [VarSet(name, value) for name, value in pending.targets.items()],
                )
            except Exception:
                # The diff caches only commit after a successful publish, so
                # the next observation republishes and repairs the retained
                # topic (no auto-retry by design). CancelledError passes
                # through untouched.
                logger.warning(
                    "mesh confirm publish failed for %s; retained state repairs "
                    "on the next observation",
                    peripheral_id,
                    exc_info=True,
                )
        self.release_confirm_task(peripheral_id)

    def release_confirm_task(self, peripheral_id: str) -> None:
        """Registry hygiene: a finished task removes its OWN handle only.

        The identity guard means this cleanup can never strip a successor's
        cancellation handle. (A cancelled task's canceller already popped it.)
        """
        if self._confirm_tasks.get(peripheral_id) is asyncio.current_task():
            del self._confirm_tasks[peripheral_id]

    def drop(self, peripheral_id: str) -> None:
        """Revoke any pending confirmation for *peripheral_id* and its timer.

        The timer is cancelled through the registry, not the record, so a
        resolver that already removed its pending entry to publish the
        confirmation is still reached — even mid-publish.
        """
        self._pending.pop(peripheral_id, None)
        task = self._confirm_tasks.pop(peripheral_id, None)
        if task is not None:
            task.cancel()

    def drop_all(self) -> None:
        """Revoke every pending and every confirm/settle task (withdrawal).

        The registry union also reaches a resolver already past its pending
        entry, mid-publish.
        """
        for peripheral_id in set(self._pending) | set(self._confirm_tasks):
            self.drop(peripheral_id)

    # -- internals ----------------------------------------------------------

    def _owns_pending(self, peripheral_id: str, pending: PendingMeshWrite) -> bool:
        return self._pending.get(peripheral_id) is pending and self.owns(
            peripheral_id, pending.generation, pending
        )

    def _fresh_deadline(self) -> float | None:
        deadline = self._wall_clock() + MESH_CONFIRM_SECONDS
        return deadline if math.isfinite(deadline) else None

    def _start_window(self, peripheral_id: str, pending: PendingMeshWrite) -> None:
        self._pending[peripheral_id] = pending
        self.set_feedback(peripheral_id, pending)
        self._start_resolver(peripheral_id, pending)

    def _start_resolver(self, peripheral_id: str, pending: PendingMeshWrite) -> None:
        self._confirm_tasks[peripheral_id] = asyncio.create_task(
            self.resolve(peripheral_id, pending),
            name=f"brilliant-mqtt-mesh-confirm-{peripheral_id}",
        )
