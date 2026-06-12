"""Mesh leader election: one fleet-wide publisher for the ble_mesh namespace.

Every panel's bus exposes the SAME whole-home ble_mesh devices, so any panel
can serve them — but exactly one may publish ``brilliant/mesh/...`` at a time
or the retained topics and command handling would fight. This module elects
that one panel over plain MQTT: each participant maintains a retained claim
(``{"panel": ..., "priority": ...}``) on :data:`MESH_LEADER_TOPIC` and
heartbeats it; standbys watch the topic and take over when the incumbent's
claim goes stale (3 x heartbeat) or when they outrank it. The retained claim
plus heartbeat gives deterministic, observable ownership with no extra
infrastructure, and preemption (lower priority number wins; the
lexicographically smaller panel name breaks ties) returns control to the
preferred panel after it recovers.

Accepted trade-offs, documented in docs/CONFIGURATION.md (BLE mesh loads
section): commands
are lost during a takeover window of up to 3 x heartbeat; a cold-starting
standby honors a dead leader's retained ghost claim for one stale window; and
the claim carries no MQTT LWT — the panel's single MQTT Will is already used
by its own availability topic.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Awaitable, Callable

from brilliant_mqtt.protocols import MqttClient

logger = logging.getLogger(__name__)

# Retained leadership claim topic, shared by every participating panel.
MESH_LEADER_TOPIC = "brilliant/mesh/leader"


class MeshLeader:
    """Priority-based leader election over a retained MQTT claim topic.

    Three states, advanced by :meth:`tick` from the run loop:

    - STANDBY: no claim outstanding; claims as soon as no live better claim
      from another panel is known.
    - PENDING: claimed, waiting one heartbeat for a better claimant to object
      before taking over (keeps a simultaneous fleet start from flapping
      ownership).
    - LEADER: owns the mesh namespace; republishes the retained claim every
      heartbeat and steps down the moment a better fresh claim appears.

    All timing derives from the injectable *clock* (monotonic seconds), so the
    machine is fully deterministic under test and indifferent to tick cadence.
    """

    def __init__(
        self,
        mqtt: MqttClient,
        panel: str,
        priority: int,
        heartbeat_seconds: float,
        on_acquire: Callable[[], Awaitable[None]],
        on_lose: Callable[[], Awaitable[None]],
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._mqtt = mqtt
        self._panel = panel
        self._priority = priority
        self._heartbeat_seconds = heartbeat_seconds
        self._on_acquire = on_acquire
        self._on_lose = on_lose
        self._clock = clock

        self._is_leader = False
        # Set while a claim awaits its confirmation heartbeat (PENDING state).
        self._pending_since: float | None = None
        # clock() of the last heartbeat publish while leader.
        self._last_beat = 0.0
        # Freshest claim seen from ANOTHER panel: ((priority, panel), received_at).
        self._other_claim: tuple[tuple[int, str], float] | None = None

    @property
    def is_leader(self) -> bool:
        """True while this panel owns the mesh namespace."""
        return self._is_leader

    async def start(self) -> None:
        """Join the election by watching the claim topic.

        priority < 1 means this panel must never publish mesh data, so it
        stays fully inert — no callback, no subscription — and :meth:`tick`
        is a no-op forever.
        """
        if self._priority < 1:
            return
        # Register BEFORE subscribing so the retained claim the broker
        # delivers on subscribe cannot race past us.
        self._mqtt.on_command(self._on_message)
        await self._mqtt.subscribe(MESH_LEADER_TOPIC)

    async def _on_message(self, topic: str, payload: str) -> None:
        """Record another panel's claim.

        The MQTT adapter fans EVERY inbound message to all registered
        callbacks, so anything off the claim topic is ignored here.
        """
        if topic != MESH_LEADER_TOPIC:
            return
        try:
            parsed = json.loads(payload)
        except (json.JSONDecodeError, ValueError):
            logger.debug("ignoring non-JSON mesh leader claim %r", payload)
            return
        if not isinstance(parsed, dict):
            logger.debug("ignoring non-object mesh leader claim %r", payload)
            return
        panel = parsed.get("panel")
        priority = parsed.get("priority")
        # bool subclasses int, but a JSON true/false is not a priority.
        if (
            not isinstance(panel, str)
            or isinstance(priority, bool)
            or not isinstance(priority, int)
        ):
            logger.debug("ignoring malformed mesh leader claim %r", payload)
            return
        if panel == self._panel:
            # Our own echo — or our own ghost retained claim from a previous
            # process. A dead leader's retained ghost must never block its own
            # restart from re-claiming, so own-panel claims carry no signal.
            return
        self._other_claim = ((priority, panel), self._clock())

    async def tick(self) -> None:
        """Advance the election state machine (call every couple of seconds).

        Every decision depends only on clock() deltas, never on call counts,
        so the cadence the run loop happens to achieve cannot change the
        protocol's timing.
        """
        if self._priority < 1:
            return
        now = self._clock()
        other = self._live_other()
        mine = (self._priority, self._panel)

        if self._is_leader:
            if other is not None and other < mine:
                # A better claimant is alive: step down. The retained claim is
                # deliberately NOT cleared or republished — the better
                # claimant overwrites it with its own.
                self._is_leader = False
                await self._invoke("on_lose", self._on_lose)
                logger.warning("lost mesh leadership to panel %s (priority %d)", other[1], other[0])
            elif now - self._last_beat >= self._heartbeat_seconds:
                await self._publish_claim()
                self._last_beat = now
            return

        if self._pending_since is not None:
            if other is not None and other < mine:
                self._pending_since = None
                logger.info(
                    "yielding pending mesh claim to panel %s (priority %d)", other[1], other[0]
                )
            elif now - self._pending_since >= self._heartbeat_seconds:
                # Nobody better objected for a full heartbeat: take over. The
                # confirmation publish doubles as the first heartbeat.
                self._pending_since = None
                self._is_leader = True
                await self._publish_claim()
                self._last_beat = now
                await self._invoke("on_acquire", self._on_acquire)
                logger.warning(
                    "acquired mesh leadership as panel %s (priority %d)",
                    self._panel,
                    self._priority,
                )
            return

        # STANDBY: claim unless somebody better is demonstrably alive.
        if other is None or mine < other:
            await self._publish_claim()
            self._pending_since = now
            logger.info(
                "claiming mesh leadership as panel %s (priority %d)",
                self._panel,
                self._priority,
            )

    def _live_other(self) -> tuple[int, str] | None:
        """The freshest other-panel claim's rank, or None when absent/stale.

        Stale means 3 heartbeats old: the incumbent gets to miss two beats
        before standbys treat it as dead.
        """
        if self._other_claim is None:
            return None
        rank, received_at = self._other_claim
        if self._clock() - received_at >= 3 * self._heartbeat_seconds:
            return None
        return rank

    async def _publish_claim(self) -> None:
        """Publish our retained claim (sorted keys keep the payload byte-stable)."""
        payload = json.dumps({"panel": self._panel, "priority": self._priority}, sort_keys=True)
        await self._mqtt.publish(MESH_LEADER_TOPIC, payload, retain=True)

    async def _invoke(self, name: str, cb: Callable[[], Awaitable[None]]) -> None:
        """Await a transition callback, isolating its exceptions.

        A failing on_acquire/on_lose must not wedge the election: the state
        transition has already happened, a failed reconcile is repaired by
        the next leader-resync, and a failed withdraw leaves only harmless
        subscriptions behind.
        """
        try:
            await cb()
        except Exception:
            logger.exception("mesh leadership %s callback failed; continuing", name)
