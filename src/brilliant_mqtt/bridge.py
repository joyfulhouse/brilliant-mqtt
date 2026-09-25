"""Bridge orchestrator for the Brilliant MQTT bridge.

Consumes BusClient and MqttClient Protocols; never imports real adapter
implementations. Runs entirely on stdlib + project modules so the full
test suite executes off-panel.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace

from brilliant_mqtt import __version__
from brilliant_mqtt.commands import VarSet, translate_aux, translate_command
from brilliant_mqtt.desired_state import RECONCILED_VARS, DesiredState
from brilliant_mqtt.diagnostics import ResponseDiagnostics
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
from brilliant_mqtt.model import BrilliantDevice, CaptureProvenance, DeviceKind, Variable
from brilliant_mqtt.motion_derive import MotionDeriver
from brilliant_mqtt.protocols import BusClient, MqttClient
from brilliant_mqtt.retained_topics import RetainedTopicLedger
from brilliant_mqtt.write_admission import (
    AdmissionTicket,
    Superseded,
    TicketAdoptionError,
    WriteCancelled,
    WriteClass,
    WriteResult,
    command_admission,
)

logger = logging.getLogger(__name__)

# Reserved pseudo-panel slug — the mesh bridge instance publishes no meta topic
# (there is no single host behind it; see discovery.config_payload's mesh branch).
_MESH_PANEL = "mesh"

# The virtual bus device carrying every BLE-mesh load. Primary commands routed
# here get observation-confirmed writes instead of the optimistic echo (issues
# #46/#47): the BLE layer can silently drop an RPC-accepted write, and the
# notification-fed mirror carries the accepted value until the backend
# reconciles (75.2s observed maximum), so neither the ack nor an early read-back
# can attest the outcome.
_MESH_DEVICE_ID = "ble_mesh"

# How long a commanded mesh value must be observed stable before it is
# published as truth. Clears the 75.2s measured revert tail (495 recorded
# false-off events, p99 59.5s). Fixed by design — deliberately not
# configurable.
MESH_CONFIRM_SECONDS = 80.0

# An observation older than this at the confirm deadline cannot attest the
# outcome — the pending closes unconfirmed and state stays unknown (the
# stale-stream watchdog owns stream recovery).
MESH_CONFIRM_MAX_OBSERVATION_AGE_S = 10.0

# The pilot's frozen observer mirror was about 20 seconds old. Keep a wired
# request projection for at most that fixed interval, then fall back to the
# latest native observation as explicitly unconfirmed. Observations never
# renew this monotonic deadline.
WIRED_PROVISIONAL_SECONDS = 20.0

# Confirm receipts log at INFO once every N confirmations (contradiction
# receipts always log at WARNING — those are the known-failed deliveries).
_MESH_RECEIPT_LOG_SAMPLE_EVERY = 10

# Placeholder receipt for a mesh write whose RPC outrun the bus adapter's
# caller deadline (#72): the write is UNRESOLVED, not failed — it keeps running
# detached in the adapter — so the pending record is armed exactly as for an
# accepted write and observations settle it. Logged wherever a receipt is.
_PENDING_RPC_RECEIPT = "<rpc pending after caller deadline>"


@dataclass
class _CommandSupersession:
    """Keep the original decoder and binding epoch for the ticket's lifetime."""

    generation: int
    supersede: Callable[[str], bool]
    enabled: bool = True

    def __call__(self, payload: str) -> bool:
        return self.enabled and self.supersede(payload)


@dataclass
class _PendingMeshWrite:
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


@dataclass
class _WiredWriteFeedback:
    """Request-owned wired-primary projection and its uncertainty metadata."""

    targets: dict[str, str]
    unresolved: set[str]
    projected: set[str]
    ambiguous: set[str]
    generation: int
    issue_provenance: CaptureProvenance | None
    status: str = "provisional"
    deadline: float | None = None


@dataclass
class _WiredWriteAttempt:
    """A command generation before its transport outcome is known."""

    targets: dict[str, str]
    generation: int
    issue_provenance: CaptureProvenance | None = None
    native_at_issue: dict[str, Variable] | None = None


@dataclass
class WriteThrottle:
    """Shared last-write timestamp so the reconciler's global write-spacing
    bounds the rate on the shared bus across all Bridge instances."""

    last_ts: float | None = None


class HotPollReadTimeout(RuntimeError):
    """A hot-poll snapshot read missed its panel RPC deadline."""


def _encode_fields(fields: dict[str, object]) -> str:
    """Serialise projected state fields — THE wire encoding for state payloads."""
    return json.dumps(fields, sort_keys=True)


def _state_payload(device: BrilliantDevice) -> str:
    """Build a sorted-keys JSON state payload for *device*.

    Delegates field selection entirely to :func:`payload_fields` (the single
    source of truth shared with discovery) and serialises the result. Returns
    ``"{}"`` for kinds that contribute no fields.
    """
    return _encode_fields(payload_fields(device))


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
        include: Callable[[str], bool] | None = None,
        desired: DesiredState | None = None,
        deriver: MotionDeriver | None = None,
        heartbeat: Callable[[], None] | None = None,
        reconcile_min_interval_s: float = 60.0,
        reconcile_max_writes_per_tick: int = 4,
        reconcile_min_write_spacing_s: float = 0.5,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        write_throttle: WriteThrottle | None = None,
        owned_topics: RetainedTopicLedger | None = None,
        deployment_id: str | None = None,
        diagnostics: ResponseDiagnostics | None = None,
    ) -> None:
        self._bus = bus
        self._mqtt = mqtt
        self._panel = panel
        self._deployment_id = deployment_id
        self._diagnostics = diagnostics
        # Scope filter keyed by bus DEVICE ID; None means everything (the
        # single-bridge default). The mesh milestone runs TWO Bridge instances
        # on the SAME bus in one process — the panel bridge excludes "ble_mesh",
        # the mesh bridge selects only it — and the bus fan-out delivers every
        # device to both, so each bridge must drop out-of-scope devices before
        # computing entities or storing snapshots. This SAME predicate is handed
        # to the bus as ``want_device`` (issue #98) so an out-of-scope push is
        # skipped before normalization; one source of truth means the pre-filter
        # can never be narrower than this downstream check and drop real state.
        self._include = include
        # Desired-state reconciliation (None => disabled; behaves as before).
        self._desired = desired
        # Score-derived motion (None => firmware movement_detected passes
        # through as before). Applied at the top of every snapshot path so
        # the stored snapshot, payload_fields, and the diff cache all see
        # the derived value.
        self._deriver = deriver
        # Bus-liveness heartbeat (None => no-op): offered after successful
        # bridge-owned reads; shared-read callers own the single offer.
        self._heartbeat = heartbeat
        self._reconcile_min_interval_s = reconcile_min_interval_s
        self._reconcile_max_writes_per_tick = reconcile_max_writes_per_tick
        self._reconcile_min_write_spacing_s = reconcile_min_write_spacing_s
        self._clock = clock
        self._wall_clock = wall_clock
        # Test seam only (like clock): the mesh confirm deadline sleeps here.
        self._sleep: Callable[[float], Awaitable[None]] = asyncio.sleep if sleep is None else sleep
        self._owned_topics = owned_topics
        # (peripheral_id, var) -> monotonic time of last re-assert attempt.
        self._last_reassert: dict[tuple[str, str], float] = {}
        # Failed writes double their per-variable retry delay up to five minutes.
        self._reassert_retry_interval_s: dict[tuple[str, str], float] = {}
        # (peripheral_id, var) pairs already reported as not exposed by their
        # snapshot — a peripheral that stops advertising a desired var exits
        # reconciliation silently otherwise (log once, not every tick).
        self._missing_var_logged: set[tuple[str, str]] = set()
        # Shared write-spacing holder; if none is supplied each Bridge instance
        # gets its own (existing tests and single-bridge deployments are unaffected).
        # Pass the SAME WriteThrottle to both Bridge instances in a two-bridge
        # process so the global min-write-spacing is enforced across the shared bus.
        self._throttle = write_throttle if write_throttle is not None else WriteThrottle()

        # peripheral_id → most recent BrilliantDevice snapshot.
        self._devices: dict[str, BrilliantDevice] = {}
        # command topic → (peripheral_id, descriptor). descriptor is None for the
        # PRIMARY JSON light/switch topic; an EntityDescriptor for each aux topic.
        self._by_cmd_topic: dict[str, tuple[str, EntityDescriptor | None]] = {}
        # Admission ownership, separate from mesh confirmation generations.
        # Keep epochs across withdrawal/rebinding to reject an A -> B -> A route.
        self._command_generation: dict[str, int] = {}
        # Command topics this session has already SUBSCRIBEd (#76): the periodic
        # resync must not re-issue them. Bridge lifetime == MQTT session, and
        # withdraw() clears it, so a rebuilt session re-subscribes everything.
        self._subscribed: set[str] = set()
        # peripheral_id → last published state payload. Lets the hot poll (and
        # pushes/echoes) skip MQTT publishes when nothing actually changed.
        self._last_state_payload: dict[str, str] = {}
        # peripheral_id → projected state fields. The hot poll compares this
        # before JSON serialization; the payload cache preserves wire-level
        # publish decisions for any projections with equivalent JSON.
        self._last_state_fields: dict[str, dict[str, object]] = {}
        # Mesh primaries only (issues #46/#47): peripheral_id → in-flight write
        # awaiting observation-based confirmation.
        self._pending_mesh: dict[str, _PendingMeshWrite] = {}
        # Confirm timers, keyed OUTSIDE the pending record: a resolver deletes
        # its pending entry before publishing the confirmation, and must stay
        # cancellable (supersession, withdraw) through that final publish.
        self._mesh_confirm_tasks: dict[str, asyncio.Task[None]] = {}
        # Monotonic per-peripheral command counter — NEVER cleared or reused
        # within a Bridge lifetime, so a stalled bus write from before a
        # withdraw can never match a generation minted after re-acquisition.
        self._mesh_write_generation: dict[str, int] = {}
        self._mesh_feedback: dict[str, _PendingMeshWrite] = {}
        # Only the new feedback publications, NOT the existing confirm timers.
        self._mesh_feedback_tasks: dict[asyncio.Task[None], str] = {}
        self._mesh_feedback_enabled = True
        self._mesh_confirm_count = 0
        # Wired-primary state keeps native Variables in _devices. Successful
        # requests live only in this separate projection/feedback state.
        self._wired_write_generation: dict[str, int] = {}
        self._pending_wired: dict[str, _WiredWriteFeedback] = {}
        self._wired_feedback: dict[str, _WiredWriteFeedback] = {}
        self._wired_deadline_tasks: dict[str, asyncio.Task[None]] = {}
        self._wired_feedback_tasks: dict[asyncio.Task[None], str] = {}
        self._wired_feedback_enabled = True
        # Provenance is per native variable even though every variable in one
        # normalized snapshot shares the same local capture marker.
        self._wired_native_provenance: dict[str, dict[str, CaptureProvenance | None]] = {}

        bus.on_change(self._on_change, want_device=self._include)
        mqtt.on_command(self._on_command)

    def _included(self, device: BrilliantDevice) -> bool:
        """True when *device* is in this bridge's scope (no filter = everything).

        Keyed by device id — the SAME predicate the bus uses as ``want_device``
        (issue #98), so the pre-filter and this check can never disagree.
        """
        return self._include is None or self._include(device.device_id)

    def _derived(self, device: BrilliantDevice) -> BrilliantDevice:
        """Apply score-derived motion to *device* (identity when disabled)."""
        return device if self._deriver is None else self._deriver.apply(device)

    def _remember_device(self, device: BrilliantDevice) -> None:
        previous = self._devices.get(device.peripheral_id)
        rebound = (
            previous is None
            or (previous.device_id, previous.peripheral_id)
            != (device.device_id, device.peripheral_id)
            or (previous.kind, previous.is_dimmable, previous.max_intensity)
            != (device.kind, device.is_dimmable, device.max_intensity)
        )
        if rebound:
            # These are translate_command's snapshot inputs. Observed values
            # and a new snapshot object do not change the command binding.
            self._command_generation[device.peripheral_id] = (
                self._command_generation.get(device.peripheral_id, 0) + 1
            )
            if previous is not None:
                self._retire_wired_feedback(device.peripheral_id)
                self._wired_native_provenance.pop(device.peripheral_id, None)
        self._devices[device.peripheral_id] = device

    @staticmethod
    def _is_wired_primary(device: BrilliantDevice) -> bool:
        return device.device_id != _MESH_DEVICE_ID and device.kind in (
            DeviceKind.LIGHT,
            DeviceKind.SWITCH,
        )

    @staticmethod
    def _capture_precedes(
        capture: CaptureProvenance | None,
        issue: CaptureProvenance | None,
    ) -> bool:
        return (
            capture is not None
            and issue is not None
            and capture.source_generation == issue.source_generation
            and capture.sequence < issue.sequence
        )

    @staticmethod
    def _capture_is_older(
        capture: CaptureProvenance | None,
        previous: CaptureProvenance | None,
    ) -> bool:
        return (
            capture is not None
            and previous is not None
            and capture.source_generation == previous.source_generation
            and capture.sequence < previous.sequence
        )

    def _remember_native_observation(self, observed: BrilliantDevice) -> BrilliantDevice:
        """Retain native fields by local capture order, never native timestamp."""
        if not self._is_wired_primary(observed):
            self._remember_device(observed)
            return observed

        peripheral_id = observed.peripheral_id
        previous = self._devices.get(peripheral_id)
        provenance = self._wired_native_provenance.setdefault(peripheral_id, {})
        capture = observed.capture_provenance
        if capture is not None and any(
            marker is not None and marker.source_generation != capture.source_generation
            for marker in provenance.values()
        ):
            provenance.clear()
        variables = dict(observed.variables)
        if previous is not None and self._is_wired_primary(previous):
            for name in observed.variables:
                if self._capture_is_older(
                    observed.capture_provenance,
                    provenance.get(name),
                ):
                    old = previous.variables.get(name)
                    if old is not None:
                        variables[name] = old
                        continue
                provenance[name] = observed.capture_provenance
            pending = self._pending_wired.get(peripheral_id)
            if pending is not None:
                for name in pending.targets:
                    if name not in variables and name in previous.variables:
                        variables[name] = previous.variables[name]
        else:
            provenance.clear()
            provenance.update({name: observed.capture_provenance for name in observed.variables})
        for name in list(provenance):
            if name not in variables:
                del provenance[name]
        native = replace(observed, variables=variables)
        self._remember_device(native)
        return native

    def _beat(self) -> None:
        if self._heartbeat is not None:
            self._heartbeat()

    async def reconcile(self) -> None:
        """Publish availability, discovery configs, and initial state for all devices.

        Idempotent for re-publishing and additions: safe to call repeatedly;
        already-subscribed command topics are skipped. Stale
        peripherals are NOT pruned — out of scope; removal is handled
        operationally by clearing the retained config topic (see
        docs/reference/deployment.md).
        """
        # Read the bus and stamp the liveness heartbeat BEFORE any broker-dependent
        # publish. A broker that rejects or hangs the availability PUBLISH (or
        # never PUBACKs it) must not starve the heartbeat while the phase reads
        # "bus" — otherwise the bus watchdog reboots a healthy panel over a
        # broker-only fault (#87). A failing bus read here is a genuine bus fault
        # and still raises before the beat, so a dead bus does not get a false
        # heartbeat. The scope filter also runs here (before the sw_version
        # pre-pass and entity computation) so the mesh bridge does not pick up the
        # panel's HARDWARE firmware tag through the shared get_all (ble_mesh has no
        # HARDWARE peripheral, so its sw_version is naturally None).
        devices = [d for d in await self._bus.get_all() if self._included(d)]
        self._beat()
        self._mesh_feedback_enabled = True
        self._wired_feedback_enabled = True

        await self._async_publish_retained(
            availability_topic(self._panel),
            "online",
        )

        # Pre-pass: the panel firmware version (from the HARDWARE peripheral) is
        # attached to every entity's HA device block so the device page shows it.
        sw_version = _sw_version_from(devices)

        # Bridge meta: the companion integration's machine contract. Retained and
        # republished on every reconcile (idempotent, like discovery configs).
        if self._panel != _MESH_PANEL:
            meta: dict[str, object] = {"agent_version": __version__}
            if self._deployment_id is not None:
                meta["deployment_id"] = self._deployment_id
            if sw_version is not None:
                meta["panel_firmware"] = sw_version
            if self._diagnostics is not None:
                meta["diag"] = self._diagnostics.snapshot()
            await self._async_publish_retained(
                meta_topic(self._panel),
                json.dumps(meta, sort_keys=True),
            )

        n_devices = n_entities = 0
        for device in devices:
            descriptors = entities_for(device, self._panel)
            if not descriptors:
                # UNKNOWN / SENSOR — no HA entity; skip entirely.
                continue
            n_devices += 1
            n_entities += len(descriptors)

            observed = self._derived(device)
            device = self._remember_native_observation(observed)
            self._observe_wired_pending(observed)
            self._observe_mesh_pending(device)

            # Publish one discovery config per entity descriptor.
            for descriptor in descriptors:
                await self._async_publish_retained(
                    config_topic(descriptor),
                    config_payload(descriptor, sw_version=sw_version),
                )

            # Publish exactly ONE shared state payload per peripheral, whenever
            # the device contributes any payload fields. Forced: reconcile is
            # the level-triggered repair pass, so it republishes even when the
            # payload is unchanged (and re-primes the diff cache).
            fields = payload_fields(device)
            if fields:
                await self._publish_state(device, fields, force=True)

            # Subscribe command topics: the primary JSON topic for light/switch,
            # plus a per-variable topic for every aux switch/number/button.
            for descriptor in descriptors:
                self._register_command_topic(device.peripheral_id, descriptor)
                topic = self._command_topic_for(device.peripheral_id, descriptor)
                if topic is None or topic in self._subscribed:
                    continue
                await self._mqtt.subscribe(topic)
                self._subscribed.add(topic)

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
        topic = self._command_topic_for(peripheral_id, d)
        if topic is None:
            return
        route = (peripheral_id, d if d.command_var is not None else None)
        previous = self._by_cmd_topic.get(topic)
        if previous is not None and previous != route:
            self._command_generation[peripheral_id] = (
                self._command_generation.get(peripheral_id, 0) + 1
            )
        self._by_cmd_topic[topic] = route

    async def withdraw(self) -> None:
        """Step down as publisher: drop command subscriptions and cached state.

        Called when this node loses the mesh leader election: it must stop
        consuming command topics immediately (the new leader owns them) and
        forget cached publish state so a future re-acquisition force-republishes
        everything fresh via reconcile(). Safe to call on a bridge that never
        reconciled (no-op).
        """
        # Step down SYNCHRONOUSLY, before ANY await. Clearing the command
        # routing first means a command already queued in the MQTT lane that
        # begins mid-withdraw finds no route — it can neither perform an
        # ex-leader bus write nor arm a fresh pending/confirm task the sweep
        # below would never see. Then revoke mesh pendings and their confirm
        # timers: an ex-leader must not resolve confirmations it no longer
        # owns, and a deadline elapsing during the unsubscribe handoff must
        # not publish one (the registry union also reaches a resolver already
        # past its pending entry, mid-publish).
        topics = list(self._by_cmd_topic)
        for peripheral_id in self._command_generation:
            self._command_generation[peripheral_id] += 1
        self._by_cmd_topic.clear()
        self._subscribed.clear()
        for peripheral_id in set(self._pending_mesh) | set(self._mesh_confirm_tasks):
            self._drop_pending_mesh(peripheral_id)
        # Invalidate — never clear — the write generations: a pre-withdraw bus
        # write still in flight (it looked up its route before this point)
        # must not match a generation minted after re-acquisition and
        # resurrect its stale command (ABA); its completion is rejected, so it
        # cannot arm a pending or publish either.
        await self.shutdown_mesh_feedback()
        await self.shutdown_wired_feedback()
        unsubscribed = 0
        for topic in topics:
            # A failed unsubscribe must not abort the rest — a step-down must
            # always complete so the routing/state caches are reliably cleared.
            try:
                await self._mqtt.unsubscribe(topic)
                unsubscribed += 1
            except Exception:
                logger.exception("withdraw: unsubscribe failed for %s; continuing", topic)
        self._last_state_payload.clear()
        self._last_state_fields.clear()
        self._devices.clear()
        self._wired_native_provenance.clear()
        if self._deriver is not None:
            # An ex-leader must not carry hold state into a re-acquisition;
            # the new session starts cold (motion off until the next spike).
            self._deriver.clear()
        logger.info("withdraw: %d command topics unsubscribed", unsubscribed)

    async def poll_once(self, devices: list[BrilliantDevice] | None = None) -> None:
        """Hot poll: fetch the current devices and publish only changed payloads.

        This bounds state staleness at the poll cadence even when the bus push
        stream is silently dead (pilot finding 2026-06-12: the notification
        stream can die without an error, freezing pushes until the processor
        reconnects). Discovery/subscribe stay reconcile-only; the diff cache
        keeps the fast cadence from spamming identical retained payloads.

        A pre-fetched *devices* snapshot lets multiple bridge scopes share one
        bus read. The caller that owns that read also owns its heartbeat.
        """
        if devices is None:
            try:
                devices = await self._bus.get_all()
            except (TimeoutError, asyncio.TimeoutError) as error:
                # Type only the scoped READ boundary. The session coordinator may
                # grant one retry without also swallowing MQTT publish or desired
                # write timeouts from the rest of this method.
                raise HotPollReadTimeout("hot poll bus read timed out") from error
            self._beat()
        for device in devices:
            # Same scope filter as reconcile: the shared get_all returns every
            # bus device, including the other bridge's.
            if not self._included(device):
                continue
            if not entities_for(device, self._panel):
                continue
            observed = self._derived(device)
            device = self._remember_native_observation(observed)
            self._observe_wired_pending(observed)
            self._observe_mesh_pending(device)
            fields = payload_fields(device)
            if fields:
                await self._publish_state(device, fields, force=False)
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
                if cur is None:
                    key = (device.peripheral_id, var)
                    if key not in self._missing_var_logged:
                        self._missing_var_logged.add(key)
                        logger.debug(
                            "reconcile-desired: %s does not expose %s; will not re-assert it",
                            device.peripheral_id,
                            var,
                        )
                    continue
                if str(cur.value) == str(want):
                    continue
                key = (device.peripheral_id, var)
                last = self._last_reassert.get(key)
                interval = self._reassert_retry_interval_s.get(key, self._reconcile_min_interval_s)
                if last is not None and (now - last) < interval:
                    continue
                drifted.append(VarSet(var, want))
            if not drifted:
                continue
            if writes >= self._reconcile_max_writes_per_tick:
                return
            # Global min-spacing bounds the write rate across ticks, independent
            # of the poll cadence. A single tick writes at most one peripheral
            # when spacing > 0; the rest catch up on subsequent ticks.
            # NOTE: the per-(pid,var) rate-limit (_last_reassert / min_interval_s)
            # provides round-robin fairness across peripherals over time; keep
            # reconcile_min_interval_s > 0 (the default 60 s) to preserve this.
            # Fairness bound: the scan restarts from devices[0] each tick, so
            # with more than ~min_interval_s / poll-cadence peripherals drifting
            # PERSISTENTLY (~30 at defaults — only plausible with a frozen
            # get_all mirror) the head devices re-enter their windows before the
            # tail is reached; the stale-stream watchdog rebuild is the backstop
            # in that regime.
            if (
                self._throttle.last_ts is not None
                and (now - self._throttle.last_ts) < self._reconcile_min_write_spacing_s
            ):
                return
            # Mark both the per-var rate-limit window and the global
            # write-spacing window BEFORE the write attempt.  A failed write
            # still consumes both — intentional: a persistently-failing
            # peripheral or bus is not hammered on every tick, and the spacing
            # window is anchored to the tick's `now` (not a post-await clock
            # read) so the check `(now - _throttle.last_ts) < spacing` stays
            # non-negative and behaves correctly.
            for vs in drifted:
                self._last_reassert[(device.peripheral_id, vs.name)] = now
            self._throttle.last_ts = now
            writes += 1
            try:
                await self._bus.set_variables(
                    device.device_id,
                    device.peripheral_id,
                    drifted,
                    write_class=WriteClass.MAINTENANCE,
                )
            except WriteCancelled:
                # Keep desired state and retry timing; skip the unconfirmed echo.
                # A genuine Task.cancel() raises the base CancelledError and escapes.
                logger.debug(
                    "reconcile-desired write superseded internally for %s/%s; continuing",
                    device.device_id,
                    device.peripheral_id,
                )
                continue
            except Exception:
                for vs in drifted:
                    key = (device.peripheral_id, vs.name)
                    interval = self._reassert_retry_interval_s.get(
                        key, self._reconcile_min_interval_s
                    )
                    self._reassert_retry_interval_s[key] = min(interval * 2.0, 300.0)
                logger.exception(
                    "reconcile-desired write failed for %s/%s; continuing",
                    device.device_id,
                    device.peripheral_id,
                )
                continue
            for vs in drifted:
                self._reassert_retry_interval_s.pop((device.peripheral_id, vs.name), None)
            logger.info(
                "reconcile-desired %s/%s: %s",
                device.device_id,
                device.peripheral_id,
                {vs.name: vs.value for vs in drifted},
            )
            try:
                # Echo like the command path does: without it, HA shows the
                # firmware's reverted value until the next poll — a phantom
                # OFF blip in history/automations on every revert cycle.
                await self._echo_state(device.peripheral_id, drifted)
            except Exception:
                logger.exception(
                    "reconcile-desired echo failed for %s/%s; continuing",
                    device.device_id,
                    device.peripheral_id,
                )

    async def _publish_state(
        self,
        device: BrilliantDevice,
        fields: dict[str, object],
        *,
        force: bool,
    ) -> None:
        """Publish *device*'s shared state payload through the diff cache.

        Skips serialization when the projected fields match, then preserves
        the wire-payload comparison for publish parity. *force* bypasses both
        checks for reconcile's level-triggered repair.

        The pending-mesh overlay is applied BEFORE either cache, so every
        publish path — echo, push, poll, and forced reconcile alike — holds
        `state: null` while a mesh write awaits confirmation.

        The caches commit only AFTER the broker accepted the publish: a failed
        publish must not leave them claiming the wire carries this payload, or
        every identical next tick would be diff-suppressed and the retained
        topic would stay stale until the value changed again.
        """
        peripheral_id = device.peripheral_id
        fields = self._project_wired_feedback(device, fields)
        wired_feedback = "wired_write_status" in fields
        if wired_feedback and (not self._wired_feedback_enabled or not self._included(device)):
            return
        fields = self._project_pending_mesh(peripheral_id, fields)
        mesh_feedback = "mesh_write_status" in fields
        if mesh_feedback and (not self._mesh_feedback_enabled or not self._included(device)):
            return
        # Accepted parity exception: dict equality treats numerically-equal
        # values as equal (-0.0 == 0.0, 1 == 1.0), so such a re-rendering keeps
        # the older wire bytes. Every payload key's TYPE is static per spec
        # (mapping._render_aux), so no HA-observable state can be suppressed.
        if not force and self._last_state_fields.get(peripheral_id) == fields:
            return
        payload = _encode_fields(fields)
        if not force and self._last_state_payload.get(peripheral_id) == payload:
            # Equivalent re-rendering: adopt the new projection, keep the wire.
            self._last_state_fields[peripheral_id] = fields
            return
        logger.debug("state publish for %s%s", peripheral_id, " (forced)" if force else "")
        if wired_feedback:
            generation = self._wired_write_generation.get(peripheral_id, 0)
            wired_record = self._wired_feedback.get(peripheral_id)
            task = asyncio.create_task(
                self._publish_wired_feedback(device, fields, payload, generation, wired_record),
                name=f"brilliant-mqtt-wired-feedback-{peripheral_id}",
            )
            self._wired_feedback_tasks[task] = peripheral_id
            try:
                await task
            except asyncio.CancelledError:
                if self._owns_wired_feedback(peripheral_id, generation, wired_record):
                    raise
            finally:
                self._wired_feedback_tasks.pop(task, None)
            return
        if mesh_feedback:
            generation = self._mesh_write_generation.get(peripheral_id, 0)
            mesh_record = self._mesh_feedback.get(peripheral_id)
            task = asyncio.create_task(
                self._publish_mesh_feedback(device, fields, payload, generation, mesh_record),
                name=f"brilliant-mqtt-mesh-feedback-{peripheral_id}",
            )
            self._mesh_feedback_tasks[task] = peripheral_id
            try:
                await task
            except asyncio.CancelledError:
                if self._owns_mesh_feedback(peripheral_id, generation, mesh_record):
                    raise
                # Revocation cancelled this publication, not its command/poll owner.
            finally:
                self._mesh_feedback_tasks.pop(task, None)
            return
        await self._async_publish_retained(
            state_topic(self._panel, peripheral_id),
            payload,
        )
        self._last_state_fields[peripheral_id] = fields
        self._last_state_payload[peripheral_id] = payload

    def _owns_wired_feedback(
        self,
        peripheral_id: str,
        generation: int,
        record: _WiredWriteFeedback | None,
    ) -> bool:
        return (
            self._wired_feedback_enabled
            and self._wired_write_generation.get(peripheral_id, 0) == generation
            and self._wired_feedback.get(peripheral_id) is record
        )

    async def _publish_wired_feedback(
        self,
        device: BrilliantDevice,
        fields: dict[str, object],
        payload: str,
        generation: int,
        record: _WiredWriteFeedback | None,
    ) -> None:
        peripheral_id = device.peripheral_id
        if not self._owns_wired_feedback(peripheral_id, generation, record) or not self._included(
            device
        ):
            return
        await self._async_publish_retained(state_topic(self._panel, peripheral_id), payload)
        if self._owns_wired_feedback(peripheral_id, generation, record):
            self._last_state_fields[peripheral_id] = fields
            self._last_state_payload[peripheral_id] = payload

    def _set_wired_feedback(
        self,
        peripheral_id: str,
        record: _WiredWriteFeedback | None,
    ) -> None:
        for task, pid in list(self._wired_feedback_tasks.items()):
            if pid == peripheral_id and task.cancel():
                self._last_state_fields.pop(peripheral_id, None)
                self._last_state_payload.pop(peripheral_id, None)
        if record is None:
            self._wired_feedback.pop(peripheral_id, None)
        else:
            self._wired_feedback[peripheral_id] = record

    def _drop_pending_wired(self, peripheral_id: str) -> None:
        self._pending_wired.pop(peripheral_id, None)
        task = self._wired_deadline_tasks.pop(peripheral_id, None)
        if task is not None:
            task.cancel()

    def _retire_wired_feedback(self, peripheral_id: str) -> None:
        self._wired_write_generation[peripheral_id] = (
            self._wired_write_generation.get(peripheral_id, 0) + 1
        )
        self._drop_pending_wired(peripheral_id)
        self._set_wired_feedback(peripheral_id, None)

    async def shutdown_wired_feedback(self) -> None:
        """Revoke wired feedback deadlines and in-flight publications."""
        self._wired_feedback_enabled = False
        peripheral_ids = (
            set(self._wired_write_generation) | set(self._pending_wired) | set(self._wired_feedback)
        )
        for peripheral_id in peripheral_ids:
            self._retire_wired_feedback(peripheral_id)
        tasks = list(self._wired_feedback_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _begin_wired_attempt(
        self,
        peripheral_id: str,
        sets: list[VarSet],
    ) -> _WiredWriteAttempt:
        generation = self._wired_write_generation.get(peripheral_id, 0) + 1
        self._wired_write_generation[peripheral_id] = generation
        self._drop_pending_wired(peripheral_id)
        self._set_wired_feedback(peripheral_id, None)
        return _WiredWriteAttempt(
            targets={item.name: item.value for item in sets},
            generation=generation,
        )

    def _note_wired_issue(
        self,
        peripheral_id: str,
        attempt: _WiredWriteAttempt,
        provenance: CaptureProvenance,
    ) -> None:
        attempt.issue_provenance = provenance
        native = self._devices.get(peripheral_id)
        attempt.native_at_issue = None if native is None else dict(native.variables)

    def _classify_wired_field(
        self,
        pending: _WiredWriteFeedback,
        name: str,
        variable: Variable | None,
        capture: CaptureProvenance | None,
    ) -> None:
        if self._capture_precedes(capture, pending.issue_provenance):
            return
        pending.projected.discard(name)
        if variable is not None and variable.value == pending.targets[name]:
            pending.unresolved.discard(name)
            pending.ambiguous.discard(name)
        else:
            pending.unresolved.add(name)
            pending.ambiguous.add(name)

    async def _arm_wired_feedback(
        self,
        peripheral_id: str,
        attempt: _WiredWriteAttempt,
    ) -> None:
        if self._wired_write_generation.get(peripheral_id) != attempt.generation:
            return
        device = self._devices.get(peripheral_id)
        if device is None or not self._is_wired_primary(device):
            return
        issue = attempt.issue_provenance
        if (
            issue is not None
            and device.capture_provenance is not None
            and device.capture_provenance.source_generation != issue.source_generation
        ):
            return
        deadline = self._wall_clock() + WIRED_PROVISIONAL_SECONDS
        pending = _WiredWriteFeedback(
            targets=dict(attempt.targets),
            unresolved=set(attempt.targets),
            projected=set(attempt.targets),
            ambiguous=set(),
            generation=attempt.generation,
            issue_provenance=issue,
            deadline=deadline if math.isfinite(deadline) else None,
        )
        provenance = self._wired_native_provenance.get(peripheral_id, {})
        for name in pending.targets:
            capture = provenance.get(name)
            if self._capture_precedes(capture, issue):
                continue
            self._classify_wired_field(
                pending,
                name,
                device.variables.get(name),
                capture,
            )
        if not pending.unresolved:
            pending.status = "observed"
            pending.projected.clear()
            self._set_wired_feedback(peripheral_id, pending)
            await self._republish_snapshot(peripheral_id, force=False)
            return
        pending.status = "ambiguous" if pending.ambiguous else "provisional"
        self._pending_wired[peripheral_id] = pending
        self._set_wired_feedback(peripheral_id, pending)
        self._wired_deadline_tasks[peripheral_id] = asyncio.create_task(
            self._resolve_pending_wired(peripheral_id, pending),
            name=f"brilliant-mqtt-wired-deadline-{peripheral_id}",
        )
        await self._republish_snapshot(peripheral_id, force=False)

    async def _resolve_pending_wired(
        self,
        peripheral_id: str,
        pending: _WiredWriteFeedback,
    ) -> None:
        await self._sleep(WIRED_PROVISIONAL_SECONDS)
        if self._pending_wired.get(peripheral_id) is not pending or not self._owns_wired_feedback(
            peripheral_id, pending.generation, pending
        ):
            return
        del self._pending_wired[peripheral_id]
        pending.status = "unconfirmed"
        pending.unresolved.clear()
        pending.projected.clear()
        pending.ambiguous.clear()
        pending.deadline = None
        try:
            await self._republish_snapshot(peripheral_id, force=True)
        except Exception:
            logger.warning(
                "wired feedback expiry publish failed for %s",
                peripheral_id,
                exc_info=True,
            )
        if self._wired_deadline_tasks.get(peripheral_id) is asyncio.current_task():
            del self._wired_deadline_tasks[peripheral_id]

    def _observe_wired_pending(self, observed: BrilliantDevice) -> None:
        if not self._is_wired_primary(observed):
            return
        peripheral_id = observed.peripheral_id
        pending = self._pending_wired.get(peripheral_id)
        if pending is None:
            if peripheral_id in self._wired_feedback:
                self._set_wired_feedback(peripheral_id, None)
            return
        capture = observed.capture_provenance
        issue = pending.issue_provenance
        if (
            capture is not None
            and issue is not None
            and capture.source_generation != issue.source_generation
        ):
            self._retire_wired_feedback(peripheral_id)
            return
        native = self._devices.get(peripheral_id)
        provenance = self._wired_native_provenance.get(peripheral_id, {})
        for name in pending.targets:
            if name not in observed.variables:
                continue
            field_capture = provenance.get(name)
            if self._capture_precedes(field_capture, issue):
                continue
            self._classify_wired_field(
                pending,
                name,
                None if native is None else native.variables.get(name),
                field_capture,
            )
        if not pending.unresolved:
            self._drop_pending_wired(peripheral_id)
            pending.status = "observed"
            pending.projected.clear()
            pending.deadline = None
            self._set_wired_feedback(peripheral_id, pending)
            return
        pending.status = "ambiguous" if pending.ambiguous else "provisional"

    def _project_wired_feedback(
        self,
        device: BrilliantDevice,
        fields: dict[str, object],
    ) -> dict[str, object]:
        feedback = self._wired_feedback.get(device.peripheral_id)
        if feedback is None or not self._is_wired_primary(device):
            return fields
        pending = self._pending_wired.get(device.peripheral_id)
        projected_fields = fields
        if pending is feedback and feedback.projected:
            variables = dict(device.variables)
            for name in feedback.projected:
                old = variables.get(name)
                variables[name] = Variable(
                    name,
                    feedback.targets[name],
                    externally_settable=(old.externally_settable if old is not None else True),
                )
            projected_fields = payload_fields(replace(device, variables=variables))
        return {
            **projected_fields,
            "wired_write_status": feedback.status,
            "wired_requested": feedback.targets if pending is feedback else {},
            "wired_write_deadline": feedback.deadline if pending is feedback else None,
        }

    def _owns_mesh_feedback(
        self, peripheral_id: str, generation: int, record: _PendingMeshWrite | None
    ) -> bool:
        return (
            self._mesh_feedback_enabled
            and self._mesh_write_generation.get(peripheral_id, 0) == generation
            and self._mesh_feedback.get(peripheral_id) is record
        )

    async def _publish_mesh_feedback(
        self,
        device: BrilliantDevice,
        fields: dict[str, object],
        payload: str,
        generation: int,
        record: _PendingMeshWrite | None,
    ) -> None:
        peripheral_id = device.peripheral_id
        if not self._owns_mesh_feedback(peripheral_id, generation, record) or not self._included(
            device
        ):
            return
        await self._async_publish_retained(state_topic(self._panel, peripheral_id), payload)
        if self._owns_mesh_feedback(peripheral_id, generation, record):
            self._last_state_fields[peripheral_id] = fields
            self._last_state_payload[peripheral_id] = payload

    def _set_mesh_feedback(self, peripheral_id: str, record: _PendingMeshWrite | None) -> None:
        for task, pid in self._mesh_feedback_tasks.items():
            if pid == peripheral_id and task.cancel():
                # Bytes may already be retained before local completion/cache commit.
                # The next publish must repair that uncertain broker state.
                self._last_state_fields.pop(peripheral_id, None)
                self._last_state_payload.pop(peripheral_id, None)
        if record is None:
            self._mesh_feedback.pop(peripheral_id, None)
        else:
            self._mesh_feedback[peripheral_id] = record

    async def shutdown_mesh_feedback(self) -> None:
        """Revoke and join feedback publishes without sweeping existing confirm timers."""
        self._mesh_feedback_enabled = False
        self._mesh_feedback.clear()
        for peripheral_id in self._mesh_write_generation:
            self._mesh_write_generation[peripheral_id] += 1
        tasks = list(self._mesh_feedback_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _async_publish_retained(self, topic: str, payload: str) -> None:
        """Publish through the panel ledger, or directly for the mesh bridge."""
        if self._owned_topics is None:
            await self._mqtt.publish(topic, payload, retain=True)
            return
        await self._owned_topics.async_publish(self._mqtt, topic, payload)

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

        observed = self._derived(device)
        device = self._remember_native_observation(observed)
        self._observe_wired_pending(observed)
        self._observe_mesh_pending(device)

        fields = payload_fields(device)
        if fields:
            await self._publish_state(device, fields, force=False)

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
            route = self._by_cmd_topic.get(topic)
            target = (device.device_id, device.peripheral_id)
            translation = (device.kind, device.is_dimmable, device.max_intensity)
            generation = self._command_generation.setdefault(peripheral_id, 0)

            def decode(new_payload: str) -> list[VarSet]:
                current = self._devices.get(peripheral_id)
                if (
                    self._command_generation.get(peripheral_id) != generation
                    or self._by_cmd_topic.get(topic) != route
                    or current is None
                    or (current.device_id, current.peripheral_id) != target
                    or (current.kind, current.is_dimmable, current.max_intensity) != translation
                ):
                    return []
                try:
                    value = json.loads(new_payload)
                except (json.JSONDecodeError, ValueError):
                    return []
                return translate_command(current, value) if isinstance(value, dict) else []

            logger.info(
                "command %s -> %s: %s",
                topic,
                peripheral_id,
                {s.name: s.value for s in sets},
            )
            if device.device_id == _MESH_DEVICE_ID:
                # Mesh primaries: no optimistic echo — observation-confirmed
                # write with a pending-visible (state: null) window instead.
                await self._write_mesh_primary(device, peripheral_id, sets, decode=decode)
                return
            # A wired primary gets request-owned provisional feedback. The
            # ticket callback records the boundary where the adapter actually
            # issues the native RPC, after any admission/lock wait.
            attempt = self._begin_wired_attempt(peripheral_id, sets)

            def on_issued(provenance: CaptureProvenance) -> None:
                self._note_wired_issue(peripheral_id, attempt, provenance)

            try:
                result = await self._write_command(
                    device,
                    peripheral_id,
                    sets,
                    decode,
                    on_issued=on_issued,
                )
            except BaseException:
                if self._wired_write_generation.get(peripheral_id) == attempt.generation:
                    self._drop_pending_wired(peripheral_id)
                    self._set_wired_feedback(peripheral_id, None)
                raise
            if isinstance(result, Superseded):
                return
            await self._arm_wired_feedback(peripheral_id, attempt)

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
        route = self._by_cmd_topic.get(topic)
        target = (device.device_id, device.peripheral_id)
        generation = self._command_generation.setdefault(peripheral_id, 0)

        def decode(new_payload: str) -> list[VarSet]:
            current = self._devices.get(peripheral_id)
            if (
                self._command_generation.get(peripheral_id) != generation
                or self._by_cmd_topic.get(topic) != route
                or current is None
                or (current.device_id, current.peripheral_id) != target
            ):
                return []
            value = translate_aux(new_payload, d.value_kind, d.invert, d.min_value, d.max_value)
            return [VarSet(d.command_var, value)] if value is not None and d.command_var else []

        result = await self._write_command(
            device, peripheral_id, sets, decode if d.component == "number" else None
        )
        if isinstance(result, Superseded):
            return
        await self._echo_state(peripheral_id, sets)

    async def _write_command(
        self,
        device: BrilliantDevice,
        peripheral_id: str,
        sets: list[VarSet],
        decode: Callable[[str], list[VarSet]] | None,
        *,
        on_issued: Callable[[CaptureProvenance], None] | None = None,
    ) -> WriteResult:
        admission = command_admission.get() if decode is not None else None
        write_class = WriteClass.INTERACTIVE_LATEST if decode else WriteClass.INTERACTIVE_FIFO
        reused_ticket = False
        if admission is not None and decode is not None:
            hook = admission.try_supersede
            if isinstance(hook, _CommandSupersession):
                # Re-handle of a folded write reusing the same ticket. Adoption
                # still settles the accepted replacement; the hook cannot refresh
                # the ticket's binding after a fold/rebinding race.
                reused_ticket = True
                hook.enabled &= hook.generation == self._command_generation.get(peripheral_id)
            elif hook is None:

                def supersede(payload: str) -> bool:
                    replacement = decode(payload)
                    return bool(replacement) and self._bus.try_supersede(
                        admission.ticket, replacement
                    )

                admission.try_supersede = _CommandSupersession(
                    self._command_generation[peripheral_id], supersede
                )
        ticket = admission.ticket if admission is not None else None
        if on_issued is not None:
            ticket = AdmissionTicket() if ticket is None else ticket
            ticket.set_issue_callback(on_issued)
        try:
            return await self._bus.set_variables(
                device.device_id,
                peripheral_id,
                sets,
                write_class=write_class,
                ticket=ticket,
            )
        except TicketAdoptionError:
            if not reused_ticket or ticket is None:
                raise
            # Re-handling a folded write after a mid-burst re-type: the target's
            # translation/route rebound since the fold, so the reused ticket
            # still carries the payload translated against the OLD snapshot and
            # the bus rejected adoption of the freshly-decoded NEWEST value
            # (#149). Without recovery the worker's cancel_waiting() then drops
            # the folded write and the newest interactive intent is silently
            # lost. Abandon the stale waiting write and re-issue the newest value
            # on a FRESH ticket so it still reaches the bus — and, while the
            # stale write is still WAITING, only the newest: cancel_waiting()
            # tombstones the stale intermediate so it is never issued ahead of it
            # (D12/#150 retain-position ordering).
            #
            # Known limitation (inherent, accepted): cancel_waiting() can only
            # tombstone a write that has NOT been issued. If the folded write
            # already acquired the device lock in the fold->re-handle gap
            # (admission.issued is True), cancel_waiting() no-ops (bus
            # _cancel_waiting only cancels a not-yet-issued write) and the
            # already-issued stale value cannot be retracted — the observed
            # native sequence is then [stale, newest] rather than [newest]. This
            # is still a strict improvement over the pre-fix behaviour (which
            # left the STALE value as the FINAL state and lost the newest): the
            # newest re-issue here still wins as the FINAL state. Removing the
            # residual would require PREEMPTING an already-issued RPC — forbidden
            # by the per-device single-native-writer contract (#72) — or
            # reintroducing the wrong-final-value bug, so it is left as-is.
            ticket.cancel_waiting()
            fresh_ticket = AdmissionTicket() if on_issued is not None else None
            if fresh_ticket is not None and on_issued is not None:
                fresh_ticket.set_issue_callback(on_issued)
            return await self._bus.set_variables(
                device.device_id,
                peripheral_id,
                sets,
                write_class=write_class,
                ticket=fresh_ticket,
            )

    async def _write_mesh_primary(
        self,
        device: BrilliantDevice,
        peripheral_id: str,
        sets: list[VarSet],
        *,
        decode: Callable[[str], list[VarSet]] | None = None,
    ) -> None:
        """Mesh primary write with observation-confirmed publication (#46/#47).

        A mesh ``set_variables`` can be RPC-accepted yet never actuate: the BLE
        layer drops it silently and the notification-fed mirror carries the
        accepted value until the backend reconciles (75.2s observed maximum) —
        so a matching observation proves nothing, while a CONTRADICTING one
        proves failure. Until :meth:`_resolve_pending_mesh` settles the write,
        every publish for the peripheral holds ``state: null`` (HA `unknown`);
        aux keys keep their live values throughout (a null aux key would render
        as a false OFF through the aux value templates — exactly the defect
        this exists to eliminate).
        """
        # Generation bump AND old-pending revocation FIRST, before any await:
        # a newer command supersedes an older one the moment it starts, even
        # while this write is in flight — the old pending must not keep
        # confirming, contradicting, or expiring against a target that is no
        # longer wanted.
        generation = self._mesh_write_generation.get(peripheral_id, 0) + 1
        self._mesh_write_generation[peripheral_id] = generation
        self._drop_pending_mesh(peripheral_id)
        self._set_mesh_feedback(
            peripheral_id,
            _PendingMeshWrite(targets={}, receipt="", generation=generation, status="superseded")
            if peripheral_id in self._mesh_feedback
            else None,
        )
        try:
            result = await self._write_command(device, peripheral_id, sets, decode)
            if isinstance(result, Superseded):
                return
            receipt = result
        except (asyncio.TimeoutError, TimeoutError):
            # Both classes: they are distinct on the panel's Python 3.10, and
            # the bus adapter raises the asyncio one while the panel lib's own
            # "No response received!" is the builtin.
            if self._mesh_write_generation.get(peripheral_id) != generation:
                raise
            # Unresolved, not failed: the RPC may still actuate (it keeps
            # running detached in the adapter), so the outcome is unknown —
            # exactly what the pending hold expresses. Republishing the last
            # snapshot here would assert a state nobody can attest.
            logger.warning(
                "mesh write for %s/%s unresolved at the caller deadline; holding state "
                "unknown until an observation settles it",
                device.device_id,
                peripheral_id,
            )
            receipt = _PENDING_RPC_RECEIPT
        except Exception:
            if self._mesh_write_generation.get(peripheral_id) != generation:
                # Superseded mid-flight — the newer command owns the pending
                # state now; surface the failure through the command lane only.
                raise
            # Mode A (bounded write failure): nothing was fabricated, so the
            # last observed snapshot is still the best truth — force it back
            # out past every cache.
            logger.warning(
                "mesh write failed for %s/%s; republishing last observed state",
                device.device_id,
                peripheral_id,
                exc_info=True,
            )
            self._set_mesh_feedback(
                peripheral_id,
                _PendingMeshWrite(targets={}, receipt="", generation=generation, status="failed"),
            )
            await self._republish_snapshot(peripheral_id, force=True)
            return

        if self._mesh_write_generation.get(peripheral_id) != generation:
            # Superseded while this write awaited; the newer command's pending
            # (and confirm timer) owns the peripheral now.
            return
        deadline = self._wall_clock() + MESH_CONFIRM_SECONDS
        pending = _PendingMeshWrite(
            targets={s.name: s.value for s in sets},
            receipt=receipt,
            generation=generation,
            deadline=deadline if math.isfinite(deadline) else None,
        )
        self._pending_mesh[peripheral_id] = pending
        self._set_mesh_feedback(peripheral_id, pending)
        self._mesh_confirm_tasks[peripheral_id] = asyncio.create_task(
            self._resolve_pending_mesh(peripheral_id, pending),
            name=f"brilliant-mqtt-mesh-confirm-{peripheral_id}",
        )
        await self._republish_snapshot(peripheral_id, force=False)

    async def _resolve_pending_mesh(self, peripheral_id: str, pending: _PendingMeshWrite) -> None:
        """Deadline arm of the pending-write state machine (background task).

        Runs OUTSIDE the MQTT command lane so confirmation never blocks the
        next command. Sleeps out the confirmation window, then — if *pending*
        still owns the peripheral — publishes the commanded value only when a
        fresh observation backs it; otherwise closes unconfirmed and leaves
        state unknown (the stale-stream watchdog owns stream recovery).

        Ownership gate at publish time: the identity check below and the start
        of the confirm publish sit in one event-loop slice (no await between
        them), and any later ownership change (supersession, withdraw) reaches
        this task through the _mesh_confirm_tasks registry as cancellation —
        including while the final publish is still in flight.
        """
        await self._sleep(MESH_CONFIRM_SECONDS)
        if self._pending_mesh.get(peripheral_id) is not pending or not self._owns_mesh_feedback(
            peripheral_id, pending.generation, pending
        ):
            return
        del self._pending_mesh[peripheral_id]
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
            self._set_mesh_feedback(
                peripheral_id,
                replace(pending, targets={}, status="unconfirmed", deadline=None),
            )
            try:
                await self._republish_snapshot(peripheral_id, force=False)
            except Exception:
                logger.warning("mesh expiry publish failed for %s", peripheral_id, exc_info=True)
        else:
            self._set_mesh_feedback(peripheral_id, None)
            self._mesh_confirm_count += 1
            if self._mesh_confirm_count % _MESH_RECEIPT_LOG_SAMPLE_EVERY == 1:
                logger.info(
                    "mesh write confirmed by observation for %s: %s (receipt: %s)",
                    peripheral_id,
                    pending.targets,
                    pending.receipt,
                )
            try:
                await self._echo_state(
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
        # Registry hygiene: a completed resolver removes its own handle — but
        # ONLY under an identity guard, so this cleanup can never strip a
        # successor's cancellation handle. (A cancelled resolver never gets
        # here; its canceller already popped the entry.)
        if self._mesh_confirm_tasks.get(peripheral_id) is asyncio.current_task():
            del self._mesh_confirm_tasks[peripheral_id]

    def _observe_mesh_pending(self, device: BrilliantDevice) -> None:
        """Feed one bus observation into the pending-write state machine.

        A value CONTRADICTING the commanded target proves the write did not
        take (the mirror only ever lies TOWARD the accepted value) — cancel
        immediately; the caller's publish that follows surfaces the observed
        truth. A MATCHING value is deliberately NOT confirmation (the mirror
        can carry the accepted-but-undelivered value for ~75s); it only
        refreshes the observation clock the confirm deadline checks.
        """
        pending = self._pending_mesh.get(device.peripheral_id)
        if pending is None:
            feedback = self._mesh_feedback.get(device.peripheral_id)
            if feedback is not None and feedback.status != "superseded":
                self._set_mesh_feedback(device.peripheral_id, None)
            return
        if not self._owns_mesh_feedback(device.peripheral_id, pending.generation, pending):
            return
        fully_matched = True
        for name, want in pending.targets.items():
            var = device.variables.get(name)
            if var is None:
                fully_matched = False
            elif var.value != want:
                self._drop_pending_mesh(device.peripheral_id)
                self._set_mesh_feedback(
                    device.peripheral_id,
                    replace(pending, targets={}, status="contradicted", deadline=None),
                )
                logger.warning(
                    "mesh write contradicted by observation for %s (%s=%s, wanted %s); "
                    "publishing observed state (receipt: %s)",
                    device.peripheral_id,
                    name,
                    var.value,
                    want,
                    pending.receipt,
                )
                return
        if fully_matched:
            pending.last_observed_at = self._clock()

    def _drop_pending_mesh(self, peripheral_id: str) -> None:
        """Revoke any pending confirmation for *peripheral_id* and its timer.

        The timer is cancelled through the registry, not the record, so a
        resolver that already removed its pending entry to publish the
        confirmation is still reached — even mid-publish.
        """
        self._pending_mesh.pop(peripheral_id, None)
        task = self._mesh_confirm_tasks.pop(peripheral_id, None)
        if task is not None:
            task.cancel()

    async def _republish_snapshot(self, peripheral_id: str, *, force: bool) -> None:
        """Publish the stored snapshot's current payload (no-op without one)."""
        device = self._devices.get(peripheral_id)
        if device is None:
            return
        fields = payload_fields(device)
        if fields:
            await self._publish_state(device, fields, force=force)

    def _project_pending_mesh(
        self, peripheral_id: str, fields: dict[str, object]
    ) -> dict[str, object]:
        """Overlay ``state: null`` while a mesh write awaits confirmation.

        ONLY the primary state key is withheld — null renders `unknown`
        through the primary light/switch template. Every aux key keeps its
        real observed value: the aux value templates collapse null into a
        false OFF (issue #47), which would invert safety-meaning entities.
        """
        if "mesh_write_status" not in fields:
            return fields
        feedback = self._mesh_feedback.get(peripheral_id)
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
        self._remember_device(updated)

        fields = payload_fields(updated)
        if fields:
            await self._publish_state(updated, fields, force=False)
