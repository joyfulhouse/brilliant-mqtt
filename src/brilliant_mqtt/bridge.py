"""Bridge orchestrator for the Brilliant MQTT bridge.

Consumes BusClient and MqttClient Protocols; never imports real adapter
implementations. Runs entirely on stdlib + project modules so the full
test suite executes off-panel.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from dataclasses import replace

from brilliant_mqtt import __version__
from brilliant_mqtt.commands import VarSet, translate_aux, translate_command
from brilliant_mqtt.desired_state import RECONCILED_VARS, DesiredState
from brilliant_mqtt.discovery import (
    aux_command_topic,
    availability_topic,
    command_topic,
    config_payload,
    config_topic,
    meta_topic,
    state_topic,
)
from brilliant_mqtt.mapping import EntityDescriptor, entities_for, payload_fields
from brilliant_mqtt.model import BrilliantDevice, DeviceKind, Variable
from brilliant_mqtt.protocols import BusClient, MqttClient

logger = logging.getLogger(__name__)

# Reserved pseudo-panel slug — the mesh bridge instance publishes no meta topic
# (there is no single host behind it; see discovery.config_payload's mesh branch).
_MESH_PANEL = "mesh"


def _state_payload(device: BrilliantDevice) -> str:
    """Build a sorted-keys JSON state payload for *device*.

    Delegates field selection entirely to :func:`payload_fields` (the single
    source of truth shared with discovery) and serialises the result. Returns
    ``"{}"`` for kinds that contribute no fields.
    """
    return json.dumps(payload_fields(device), sort_keys=True)


def _sw_version_from(devices: list[BrilliantDevice]) -> str | None:
    """Return the panel firmware string from the HARDWARE peripheral, if present.

    The HARDWARE peripheral carries ``current_release_tag`` (e.g. "v26.05.20.2");
    it is surfaced as ``sw_version`` on every entity's HA device block. None when
    no HARDWARE device (or no tag) is in *devices*.
    """
    for device in devices:
        if device.kind is DeviceKind.HARDWARE:
            tag = device.variables.get("current_release_tag")
            if tag is not None and tag.value:
                return tag.value
    return None


class Bridge:
    """Orchestrates the Brilliant bus ↔ MQTT bridge lifecycle.

    Responsibilities:
    - reconcile(): publish HA discovery configs + initial state on startup.
    - withdraw(): step down as publisher — drop command subscriptions and
      cached publish state (mesh leadership loss).
    - _on_change(): update MQTT state when the bus reports a change.
    - _on_command(): translate inbound MQTT commands to bus variable writes.
    """

    def __init__(
        self,
        bus: BusClient,
        mqtt: MqttClient,
        panel: str,
        *,
        include: Callable[[BrilliantDevice], bool] | None = None,
        desired: DesiredState | None = None,
        reconcile_min_interval_s: float = 60.0,
        reconcile_max_writes_per_tick: int = 4,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._bus = bus
        self._mqtt = mqtt
        self._panel = panel
        # Scope filter; None means everything (the single-bridge default).
        # The mesh milestone runs TWO Bridge instances on the SAME bus in one
        # process — the panel bridge excludes "ble_mesh", the mesh bridge
        # selects only it — and the bus fan-out delivers every device to both,
        # so each bridge must drop out-of-scope devices before computing
        # entities or storing snapshots.
        self._include = include
        # Desired-state reconciliation (None => disabled; behaves as before).
        self._desired = desired
        self._reconcile_min_interval_s = reconcile_min_interval_s
        self._reconcile_max_writes_per_tick = reconcile_max_writes_per_tick
        self._clock = clock
        # (peripheral_id, var) -> monotonic time of last re-assert attempt.
        self._last_reassert: dict[tuple[str, str], float] = {}

        # peripheral_id → most recent BrilliantDevice snapshot.
        self._devices: dict[str, BrilliantDevice] = {}
        # command topic → (peripheral_id, descriptor). descriptor is None for the
        # PRIMARY JSON light/switch topic; an EntityDescriptor for each aux topic.
        self._by_cmd_topic: dict[str, tuple[str, EntityDescriptor | None]] = {}
        # peripheral_id → last published state payload. Lets the hot poll (and
        # pushes/echoes) skip MQTT publishes when nothing actually changed.
        self._last_state_payload: dict[str, str] = {}

        bus.on_change(self._on_change)
        mqtt.on_command(self._on_command)

    def _included(self, device: BrilliantDevice) -> bool:
        """True when *device* is in this bridge's scope (no filter = everything)."""
        return self._include is None or self._include(device)

    async def reconcile(self) -> None:
        """Publish availability, discovery configs, and initial state for all devices.

        Idempotent for re-publishing and additions: safe to call repeatedly, and
        duplicate subscribe() calls on the MQTT client are acceptable. Stale
        peripherals are NOT pruned — out of scope; removal is handled
        operationally by clearing the retained config topic (see
        docs/reference/deployment.md).
        """
        await self._mqtt.publish(
            availability_topic(self._panel),
            "online",
            retain=True,
        )

        # Scope filter BEFORE the sw_version pre-pass and entity computation:
        # the mesh bridge must not pick up the panel's HARDWARE firmware tag
        # through the shared get_all (ble_mesh has no HARDWARE peripheral, so
        # its sw_version is naturally None).
        devices = [d for d in await self._bus.get_all() if self._included(d)]
        # Pre-pass: the panel firmware version (from the HARDWARE peripheral) is
        # attached to every entity's HA device block so the device page shows it.
        sw_version = _sw_version_from(devices)

        # Bridge meta: the companion integration's machine contract. Retained and
        # republished on every reconcile (idempotent, like discovery configs).
        if self._panel != _MESH_PANEL:
            meta: dict[str, str] = {"agent_version": __version__}
            if sw_version is not None:
                meta["panel_firmware"] = sw_version
            await self._mqtt.publish(
                meta_topic(self._panel),
                json.dumps(meta, sort_keys=True),
                retain=True,
            )

        n_devices = n_entities = 0
        for device in devices:
            descriptors = entities_for(device, self._panel)
            if not descriptors:
                # UNKNOWN / SENSOR — no HA entity; skip entirely.
                continue
            n_devices += 1
            n_entities += len(descriptors)

            self._devices[device.peripheral_id] = device

            # Publish one discovery config per entity descriptor.
            for descriptor in descriptors:
                await self._mqtt.publish(
                    config_topic(descriptor),
                    config_payload(descriptor, sw_version=sw_version),
                    retain=True,
                )

            # Publish exactly ONE shared state payload per peripheral, whenever
            # the device contributes any payload fields. Forced: reconcile is
            # the level-triggered repair pass, so it republishes even when the
            # payload is unchanged (and re-primes the diff cache).
            if payload_fields(device):
                await self._publish_state(device, force=True)

            # Subscribe command topics: the primary JSON topic for light/switch,
            # plus a per-variable topic for every aux switch/number/button.
            for descriptor in descriptors:
                self._register_command_topic(device.peripheral_id, descriptor)
                topic = self._command_topic_for(device.peripheral_id, descriptor)
                if topic is not None:
                    await self._mqtt.subscribe(topic)

        logger.info(
            "reconcile: %d devices -> %d entities, %d command topics registered",
            n_devices,
            n_entities,
            len(self._by_cmd_topic),
        )
        await self._enforce_desired(devices)

    def _command_topic_for(self, peripheral_id: str, d: EntityDescriptor) -> str | None:
        """The command topic a descriptor subscribes to, or None if it has none."""
        if d.component in ("light", "switch") and d.command_var is None:
            return command_topic(self._panel, peripheral_id)
        if d.command_var is not None:
            return aux_command_topic(self._panel, peripheral_id, d.command_var)
        return None

    def _register_command_topic(self, peripheral_id: str, d: EntityDescriptor) -> None:
        """Record the (peripheral_id, descriptor) route for a descriptor's topic.

        The PRIMARY light/switch JSON topic maps to (peripheral_id, None); each
        aux switch/number/button maps its per-variable topic to (peripheral_id, d).
        """
        if d.component in ("light", "switch") and d.command_var is None:
            self._by_cmd_topic[command_topic(self._panel, peripheral_id)] = (peripheral_id, None)
        elif d.command_var is not None:
            topic = aux_command_topic(self._panel, peripheral_id, d.command_var)
            self._by_cmd_topic[topic] = (peripheral_id, d)

    async def withdraw(self) -> None:
        """Step down as publisher: drop command subscriptions and cached state.

        Called when this node loses the mesh leader election: it must stop
        consuming command topics immediately (the new leader owns them) and
        forget cached publish state so a future re-acquisition force-republishes
        everything fresh via reconcile(). Safe to call on a bridge that never
        reconciled (no-op).
        """
        unsubscribed = 0
        for topic in list(self._by_cmd_topic):
            # A failed unsubscribe must not abort the rest — a step-down must
            # always complete so the routing/state caches are reliably cleared.
            try:
                await self._mqtt.unsubscribe(topic)
                unsubscribed += 1
            except Exception:
                logger.exception("withdraw: unsubscribe failed for %s; continuing", topic)
        self._by_cmd_topic.clear()
        self._last_state_payload.clear()
        self._devices.clear()
        logger.info("withdraw: %d command topics unsubscribed", unsubscribed)

    async def poll_once(self) -> None:
        """Hot poll: fetch the current devices and publish only changed payloads.

        This bounds state staleness at the poll cadence even when the bus push
        stream is silently dead (pilot finding 2026-06-12: the notification
        stream can die without an error, freezing pushes until the processor
        reconnects). Discovery/subscribe stay reconcile-only; the diff cache
        keeps the fast cadence from spamming identical retained payloads.
        """
        devices = await self._bus.get_all()
        for device in devices:
            # Same scope filter as reconcile: the shared get_all returns every
            # bus device, including the other bridge's.
            if not self._included(device):
                continue
            if not entities_for(device, self._panel):
                continue
            self._devices[device.peripheral_id] = device
            if payload_fields(device):
                await self._publish_state(device, force=False)
        await self._enforce_desired(devices)

    async def _enforce_desired(self, devices: list[BrilliantDevice]) -> None:
        """Re-assert drifted reconciled vars (firmware reverts the enable flags).

        Per peripheral, batch every drifted desired var into ONE set_variables
        call (avoids the same-peripheral rapid-write race). Rate-limit per
        (pid, var) and cap the number of peripherals written per tick so a fleet
        of drifted devices ramps gently instead of bursting the Thrift bus.
        """
        if self._desired is None:
            return
        now = self._clock()
        writes = 0
        for device in devices:
            if not self._included(device):
                continue
            wanted = self._desired.wanted(device.peripheral_id)
            if not wanted:
                continue
            drifted: list[VarSet] = []
            for var, want in wanted.items():
                cur = device.variables.get(var)
                if cur is None or str(cur.value) == str(want):
                    continue
                last = self._last_reassert.get((device.peripheral_id, var))
                if last is not None and (now - last) < self._reconcile_min_interval_s:
                    continue
                drifted.append(VarSet(var, want))
            if not drifted:
                continue
            if writes >= self._reconcile_max_writes_per_tick:
                return
            # Mark the rate-limit window BEFORE the write.  If the write
            # fails we still consume the window — intentional: a persistently-
            # failing peripheral is not hammered on every tick.
            for vs in drifted:
                self._last_reassert[(device.peripheral_id, vs.name)] = now
            writes += 1
            try:
                await self._bus.set_variables(device.device_id, device.peripheral_id, drifted)
                logger.info(
                    "reconcile-desired %s/%s: %s",
                    device.device_id,
                    device.peripheral_id,
                    {vs.name: vs.value for vs in drifted},
                )
            except Exception:
                logger.exception(
                    "reconcile-desired write failed for %s/%s; continuing",
                    device.device_id,
                    device.peripheral_id,
                )

    async def _publish_state(self, device: BrilliantDevice, *, force: bool) -> None:
        """Publish *device*'s shared state payload through the diff cache.

        Skips the publish when the payload matches the last one sent for this
        peripheral, unless *force* (reconcile's level-triggered repair).
        """
        payload = _state_payload(device)
        if not force and self._last_state_payload.get(device.peripheral_id) == payload:
            return
        self._last_state_payload[device.peripheral_id] = payload
        logger.debug("state publish for %s%s", device.peripheral_id, " (forced)" if force else "")
        await self._mqtt.publish(
            state_topic(self._panel, device.peripheral_id),
            payload,
            retain=True,
        )

    async def _on_change(self, device: BrilliantDevice) -> None:
        """Handle a bus change event: update stored snapshot and re-publish state."""
        # The shared-bus fan-out delivers every device's changes to every
        # bridge — an out-of-scope device must not even be snapshotted.
        if not self._included(device):
            return

        # Discovery/subscribe is reconcile-only by design: a change event for a
        # never-reconciled peripheral publishes state HA ignores until the
        # periodic resync re-runs reconcile and closes the gap.
        descriptors = entities_for(device, self._panel)
        if not descriptors:
            return

        self._devices[device.peripheral_id] = device

        if payload_fields(device):
            await self._publish_state(device, force=False)

    async def _on_command(self, topic: str, payload: str) -> None:
        """Handle an inbound MQTT command: translate and write to the bus.

        Two paths: the PRIMARY JSON light/switch topic (descriptor None) uses the
        full :func:`translate_command` JSON path; each aux topic (descriptor set)
        uses :func:`translate_aux` on its single command variable.
        """
        route = self._by_cmd_topic.get(topic)
        if route is None:
            logger.debug("command on unknown topic %s; ignoring", topic)
            return
        peripheral_id, descriptor = route

        if descriptor is None:
            await self._handle_primary_command(topic, peripheral_id, payload)
        else:
            await self._handle_aux_command(topic, peripheral_id, descriptor, payload)

    async def _handle_primary_command(self, topic: str, peripheral_id: str, payload: str) -> None:
        """PRIMARY JSON light/switch path (unchanged behaviour)."""
        try:
            parsed = json.loads(payload)
        except (json.JSONDecodeError, ValueError):
            logger.debug("command on %s is not JSON; ignoring", topic)
            return

        if not isinstance(parsed, dict):
            logger.debug("command on %s is not a JSON object; ignoring", topic)
            return

        device = self._devices.get(peripheral_id)
        if device is None:
            logger.debug("command for unknown peripheral %s; ignoring", peripheral_id)
            return

        sets: list[VarSet] = translate_command(device, parsed)
        if sets:
            logger.info(
                "command %s -> %s: %s",
                topic,
                peripheral_id,
                {s.name: s.value for s in sets},
            )
            # Route the write to the bus device owning the peripheral (the
            # panel's own CONTROL device, or "ble_mesh" for mesh loads).
            await self._bus.set_variables(device.device_id, peripheral_id, sets)
            await self._echo_state(peripheral_id, sets)

    async def _handle_aux_command(
        self, topic: str, peripheral_id: str, d: EntityDescriptor, payload: str
    ) -> None:
        """Aux switch/number/button path: a single variable write via translate_aux."""
        if d.command_var is None:
            # Defensive: only descriptors with a command_var are registered here.
            return
        value = translate_aux(payload, d.value_kind, d.invert, d.min_value, d.max_value)
        if value is None:
            logger.debug("aux command on %s (%r) did not translate; ignoring", topic, payload)
            return
        if self._desired is not None and d.command_var in RECONCILED_VARS:
            self._desired.record(peripheral_id, d.command_var, value)
        device = self._devices.get(peripheral_id)
        if device is None:
            # Without a snapshot we cannot know which bus device owns the
            # peripheral, so the write cannot be routed — mirror the primary
            # path's unknown-peripheral guard.
            logger.debug("aux command for unknown peripheral %s; ignoring", peripheral_id)
            return
        logger.info("aux command %s -> %s: %s=%s", topic, peripheral_id, d.command_var, value)
        sets = [VarSet(d.command_var, value)]
        await self._bus.set_variables(device.device_id, peripheral_id, sets)
        await self._echo_state(peripheral_id, sets)

    async def _echo_state(self, peripheral_id: str, sets: list[VarSet]) -> None:
        """Optimistically fold written VarSets into the snapshot and republish state.

        The bus does not push notifications for some written variables (pilot
        finding 2026-06-12: a muted=1 write succeeded but no notification ever
        arrived), so HA would stay stale until the periodic resync. Echoing the
        commanded values immediately keeps HA in sync; real bus notifications and
        the resync still own externally-caused changes.
        """
        device = self._devices.get(peripheral_id)
        if device is None:
            # Write already happened; nothing to echo without a snapshot.
            return

        # Build a NEW variables dict — never mutate one a pending callback may share.
        new_vars = dict(device.variables)
        for s in sets:
            old = new_vars.get(s.name)
            settable = old.externally_settable if old is not None else True
            new_vars[s.name] = Variable(s.name, s.value, externally_settable=settable)
        updated = replace(device, variables=new_vars)
        self._devices[peripheral_id] = updated

        if payload_fields(updated):
            await self._publish_state(updated, force=False)
