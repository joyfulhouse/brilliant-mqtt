"""Bridge orchestrator for the Brilliant MQTT bridge.

Consumes BusClient and MqttClient Protocols; never imports real adapter
implementations. Runs entirely on stdlib + project modules so the full
test suite executes off-panel.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from typing import cast

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
from brilliant_mqtt.mapping import (
    EntityDescriptor,
    entities_for,
    has_calibrated_motion_thresholds,
    payload_fields,
)
from brilliant_mqtt.mesh_commands import MeshCommands
from brilliant_mqtt.mesh_driver import MeshDriver
from brilliant_mqtt.mesh_registry import MeshRegistry
from brilliant_mqtt.mesh_routing import DesiredTopic, MeshRouting
from brilliant_mqtt.mesh_settlement import MeshSettlement
from brilliant_mqtt.model import BrilliantDevice, CaptureProvenance, DeviceKind, Variable
from brilliant_mqtt.motion_derive import MotionDeriver
from brilliant_mqtt.protocols import BusClient, MeshBusClient, MqttClient
from brilliant_mqtt.retained_topics import RetainedTopicLedger
from brilliant_mqtt.wired_feedback import (
    Begin,
    Capture,
    Event,
    Expire,
    Issue,
    Outcome,
    PublishAccepted,
    PublishFailed,
    Record,
    Shutdown,
    SourceGeneration,
    timer_deadline,
)
from brilliant_mqtt.wired_feedback import (
    reduce as reduce_wired,
)
from brilliant_mqtt.wired_feedback import (
    render as render_wired,
)
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


@dataclass
class _CommandSupersession:
    """Keep the original decoder and binding epoch for the ticket's lifetime."""

    generation: int
    supersede: Callable[[str], bool]
    enabled: bool = True

    def __call__(self, payload: str) -> bool:
        return self.enabled and self.supersede(payload)


@dataclass
class _WiredSlot:
    """One reducer record, deadline and serialized publisher for a wired load."""

    record: Record
    timer: asyncio.Task[None] | None = None
    timer_at: float | None = None
    publisher: asyncio.Task[None] | None = None
    dirty: bool = False
    force: bool = False


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
        motion_native_passthrough: bool = False,
        motion_default_high_threshold: int | None = None,
        motion_default_low_threshold: int | None = None,
    ) -> None:
        self._bus = bus
        self._mqtt = mqtt
        self._panel = panel
        self._deployment_id = deployment_id
        self._diagnostics = diagnostics
        self._motion_native_passthrough = motion_native_passthrough
        self._motion_default_high_threshold = motion_default_high_threshold
        self._motion_default_low_threshold = motion_default_low_threshold
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
        # Only a filtered full reconcile refreshes or clears discovery firmware.
        self._discovery_sw_version: str | None = None
        # Camera permission must not inherit discovery's intentionally unpruned cache.
        self._privacy_off = False
        self._privacy_source_generation = getattr(bus, "capture_generation", 0)
        self._privacy_sequence = -1
        self._privacy_invalid_through = -1
        # command topic → (peripheral_id, descriptor). descriptor is None for the
        # PRIMARY JSON light/switch topic; an EntityDescriptor for each aux topic.
        self._by_cmd_topic: dict[str, tuple[str, EntityDescriptor | None]] = {}
        # Wired admission epochs survive withdrawal/rebinding. Mesh commands use
        # the exact registry owner stamped at MQTT ingress instead.
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
        self._mesh: MeshRegistry | None = None
        self._mesh_driver: MeshDriver | None = None
        self._mesh_commands: MeshCommands | None = None
        self._mesh_route_owners: dict[str, MeshSettlement] = {}
        self._mesh_descriptors: dict[MeshSettlement, tuple[EntityDescriptor, ...]] = {}
        self._mesh_routing = MeshRouting(
            mqtt, self._subscribed, self._fail_mesh_routing, self._async_publish_retained
        )
        if panel == _MESH_PANEL or include is None or include(_MESH_DEVICE_ID):
            mesh_bus = cast(MeshBusClient, bus)
            self._mesh = MeshRegistry(
                mesh_bus,
                clock=clock,
                wall_clock=wall_clock,
                derive_motion=deriver is not None,
                motion_hold_s=deriver.hold_s if deriver is not None else 60,
                native_motion_passthrough=motion_native_passthrough,
            )
            self._mesh_driver = MeshDriver(self._mesh, mqtt, panel=panel, sleep=self._sleep)
            self._mesh_driver.cleanup = self._cleanup_mesh
            self._mesh_commands = MeshCommands(self._mesh, self._mesh_driver, bus)
            self._mesh.revoked = self._mesh_revoked
            self._mesh.changed = self._mesh_status_changed
            self._mesh.retained_ids = self._mesh_retained_ids
            mqtt.set_mesh_owner_resolver(self._resolve_mesh_owner)
        self._wired: dict[str, _WiredSlot] = {}
        self._wired_source_generation = getattr(bus, "capture_generation", 0)
        self._wired_feedback_enabled = True
        self._wired_lifecycle = 0

        if self._mesh is not None:
            mesh_bus.on_change(self._on_change, want_device=self._include, mesh_metadata_only=True)
        else:
            bus.on_change(self._on_change, want_device=self._include)
        mqtt.on_command(self._on_command)

    @property
    def mesh_registry(self) -> MeshRegistry | None:
        """Actual owner registry for read-only integration diagnostics."""
        return self._mesh

    @property
    def mesh_driver(self) -> MeshDriver | None:
        return self._mesh_driver

    def _fail_mesh_routing(self, error: Exception) -> None:
        if self._mesh is not None:
            self._mesh.bus.fail_mesh(error)

    def _mesh_retained_ids(self) -> frozenset[str]:
        ids = self._mesh_routing.peripheral_ids
        if self._mesh_driver is not None:
            ids |= self._mesh_driver.peripheral_ids
        return ids

    def _mesh_status_changed(self) -> None:
        registry = self._mesh
        if registry is not None and registry.enabled and self._panel == _MESH_PANEL:
            self._queue_mesh_availability("offline")

    def _queue_mesh_availability(self, payload: str) -> asyncio.Task[None]:
        registry = self._mesh
        assert registry is not None
        acquisition = registry.acquisition
        assert acquisition is not None
        return self._mesh_routing.request(
            availability_topic(self._panel),
            DesiredTopic(
                acquisition,
                lambda: (
                    registry.enabled
                    and registry.acquisition is acquisition
                    and registry.bus.mesh_admission_open
                    and (payload == "offline" or self._mesh_online_ready())
                ),
                payload=payload,
            ),
            revoke=True,
        )

    def _mesh_online_ready(self) -> bool:
        registry = self._mesh
        if (
            registry is None
            or not registry.enabled
            or registry.disabled
            or not registry.inventory_ready
            or registry.retired
            or self._mesh_driver is None
        ):
            return False
        expected: set[str] = set()
        for owner in registry.owners:
            if (
                not registry.live(owner)
                or self._mesh_driver.published_fields(owner) is None
                or owner.view.terminal is not None
                or owner.view.in_flight is not None
            ):
                return False
            pid = owner.metadata.peripheral_id
            descriptors = entities_for(owner.materialize(self._clock()), self._panel)
            if self._mesh_descriptors.get(owner) != tuple(descriptors):
                return False
            for descriptor in descriptors:
                topic = config_topic(descriptor)
                expected.add(topic)
                if not self._mesh_routing.repaired(
                    topic,
                    owner,
                    payload=config_payload(descriptor, sw_version=self._discovery_sw_version),
                ):
                    return False
                command = self._command_topic_for(pid, descriptor)
                if command is None:
                    continue
                expected.add(command)
                if (
                    not self._mesh_routing.repaired(command, owner, membership=True)
                    or command not in self._subscribed
                    or self._resolve_mesh_owner(command) is not owner
                    or self._by_cmd_topic.get(command)
                    != (pid, descriptor if descriptor.command_var is not None else None)
                ):
                    return False
        return all(
            record.peripheral_id is None or topic in expected
            for topic, record in self._mesh_routing.records.items()
        )

    async def _repair_mesh_availability(self) -> None:
        registry = self._mesh
        if registry is None or not registry.enabled or self._panel != _MESH_PANEL:
            return
        payload = "online" if self._mesh_online_ready() else "offline"
        await asyncio.shield(self._queue_mesh_availability(payload))

    def _mesh_revoked(self, owner: MeshSettlement) -> None:
        self._mesh_status_changed()
        self._mesh_routing.revoke(owner)
        if self._mesh_commands is not None:
            self._mesh_commands.revoke(owner)
        if (
            self._mesh is not None
            and self._mesh.retired.get(owner.metadata.peripheral_id) is not owner
        ):
            self._mesh_descriptors.pop(owner, None)

    def _current_device(self, peripheral_id: str) -> BrilliantDevice | None:
        if self._mesh is not None:
            owner = self._mesh.current(peripheral_id)
            if owner is not None:
                return owner.materialize(self._clock()) if self._mesh.live(owner) else None
        device = self._devices.get(peripheral_id)
        return device if device is None or device.device_id != _MESH_DEVICE_ID else None

    def _resolve_mesh_owner(self, topic: str) -> MeshSettlement | None:
        route = self._by_cmd_topic.get(topic)
        if route is None or self._mesh is None:
            return None
        peripheral_id, _descriptor = route
        owner = self._mesh.current(peripheral_id)
        if (
            owner is None
            or not self._mesh.live(owner)
            or self._mesh_route_owners.get(topic) is not owner
        ):
            return None
        return owner

    async def _flush_mesh(self, peripheral_id: str, *, force: bool = False) -> None:
        if self._mesh is not None and self._mesh_driver is not None:
            owner = self._mesh.current(peripheral_id)
            if owner is not None:
                await self._mesh_driver.flush(owner, force=force)

    async def _flush_retired_mesh(self) -> None:
        if self._mesh is not None and self._mesh_driver is not None:
            for owner in tuple(self._mesh.retired.values()):
                await self._mesh_driver.flush(owner)

    async def _repair_mesh(self, peripheral_id: str, *, force: bool) -> None:
        registry = self._mesh
        if registry is None:
            return
        while True:
            owner = registry.current(peripheral_id)
            if owner is None or not registry.live(owner):
                return
            sw_version = self._discovery_sw_version
            device = owner.materialize(self._clock())
            descriptors = entities_for(device, self._panel)
            self._remember_device(device)
            self._mesh_descriptors[owner] = tuple(descriptors)
            if self._mesh_driver is not None:
                self._mesh_driver.wake(owner, force=force)
            desired: dict[str, DesiredTopic] = {}

            def current(owner: MeshSettlement = owner) -> bool:
                return registry.current(peripheral_id) is owner and registry.live(owner)

            def current_config(sw_version: str | None = sw_version) -> bool:
                return current() and self._discovery_sw_version == sw_version

            for descriptor in descriptors:
                desired[config_topic(descriptor)] = DesiredTopic(
                    owner, current_config, payload=config_payload(descriptor, sw_version=sw_version)
                )
                topic = self._command_topic_for(peripheral_id, descriptor)
                self._register_command_topic(peripheral_id, descriptor)
                if topic is None:
                    continue
                self._mesh_route_owners[topic] = owner
                desired[topic] = DesiredTopic(owner, current, membership=True)
            for topic, record in tuple(self._mesh_routing.records.items()):
                if record.peripheral_id == peripheral_id and topic not in desired:
                    desired[topic] = DesiredTopic(
                        owner,
                        current,
                        payload="" if record.desired.membership is None else None,
                        membership=False if record.desired.membership is not None else None,
                        forget=True,
                    )
                    self._mesh_route_owners.pop(topic, None)
                    self._by_cmd_topic.pop(topic, None)
            tasks = [
                self._mesh_routing.request(topic, target, force=force)
                for topic, target in desired.items()
            ]
            for task in tasks:
                await asyncio.shield(task)
            if self._discovery_sw_version != sw_version:
                continue
            if registry.current(peripheral_id) is not owner:
                continue
            if registry.live(owner) and descriptors != entities_for(
                owner.materialize(self._clock()), self._panel
            ):
                continue
            if registry.live(owner):
                await self._flush_mesh(peripheral_id)
            if self._discovery_sw_version != sw_version:
                continue
            return

    async def _cleanup_mesh(self, owner: MeshSettlement) -> None:
        registry = self._mesh
        peripheral_id = owner.metadata.peripheral_id
        if registry is None:
            return

        def current() -> bool:
            return (
                registry.enabled
                and registry.bus.mesh_admission_open
                and registry.retired.get(peripheral_id) is owner
                and registry.current(peripheral_id) is None
            )

        if not current():
            return
        records = [
            (topic, record)
            for topic, record in tuple(self._mesh_routing.records.items())
            if record.peripheral_id == peripheral_id
        ]
        records.sort(key=lambda item: item[1].desired.membership is None)
        for topic, record in records:
            if not current():
                return
            await asyncio.shield(
                self._mesh_routing.request(
                    topic,
                    DesiredTopic(
                        owner,
                        current,
                        payload="" if record.desired.payload is not None else None,
                        membership=False if record.desired.membership is not None else None,
                        forget=True,
                    ),
                )
            )
        if registry.retired.get(peripheral_id) is owner:
            for topic, route_owner in tuple(self._mesh_route_owners.items()):
                if route_owner.metadata.peripheral_id == peripheral_id:
                    self._mesh_route_owners.pop(topic)
                    self._by_cmd_topic.pop(topic, None)
            self._mesh_descriptors.pop(owner, None)
            self._devices.pop(peripheral_id, None)
            self._command_generation.pop(peripheral_id, None)
            self._last_state_payload.pop(peripheral_id, None)
            self._last_state_fields.pop(peripheral_id, None)
            self._last_reassert = {
                key: at for key, at in self._last_reassert.items() if key[0] != peripheral_id
            }
            self._reassert_retry_interval_s = {
                key: at
                for key, at in self._reassert_retry_interval_s.items()
                if key[0] != peripheral_id
            }
            self._missing_var_logged = {
                key for key in self._missing_var_logged if key[0] != peripheral_id
            }
            registry.finish_retired(owner)

    def _included(self, device: BrilliantDevice) -> bool:
        """True when *device* is in this bridge's scope (no filter = everything).

        Keyed by device id — the SAME predicate the bus uses as ``want_device``
        (issue #98), so the pre-filter and this check can never disagree.
        """
        return self._include is None or self._include(device.device_id)

    def _derived(self, device: BrilliantDevice) -> BrilliantDevice:
        """Apply score-derived motion to *device* (identity when disabled)."""
        if device.device_id == _MESH_DEVICE_ID:
            return device
        return device if self._deriver is None else self._deriver.apply(device)

    def _payload_fields(self, device: BrilliantDevice) -> dict[str, object]:
        """Render payload fields with this bridge's motion passthrough configuration."""
        if self._motion_native_passthrough:
            return payload_fields(device, native_motion_passthrough=True)
        return payload_fields(device)

    @staticmethod
    def _same_binding(previous: BrilliantDevice, device: BrilliantDevice) -> bool:
        return (
            previous.device_id == device.device_id
            and previous.peripheral_id == device.peripheral_id
            and previous.kind == device.kind
            and previous.is_dimmable == device.is_dimmable
            and previous.max_intensity == device.max_intensity
        )

    def _remember_device(self, device: BrilliantDevice) -> None:
        if device.device_id == _MESH_DEVICE_ID:
            self._devices[device.peripheral_id] = replace(device, variables={})
            return
        previous = self._devices.get(device.peripheral_id)
        if previous is None or not self._same_binding(previous, device):
            # These are translate_command's snapshot inputs. Observed values
            # and a new snapshot object do not change the command binding.
            self._command_generation[device.peripheral_id] = (
                self._command_generation.get(device.peripheral_id, 0) + 1
            )
        self._devices[device.peripheral_id] = device

    def is_privacy_off(self) -> bool:
        """Require explicit privacy-off evidence from the current bus generation."""
        self._sync_privacy_generation()
        return self._wired_feedback_enabled and self._privacy_off

    def camera_privacy_epoch(self) -> tuple[int, int, int]:
        """Identity for work that must not span a denial or source replacement."""
        self._sync_privacy_generation()
        return (
            self._privacy_source_generation,
            self._privacy_invalid_through,
            self._wired_lifecycle,
        )

    def _sync_privacy_generation(self) -> None:
        generation = getattr(self._bus, "capture_generation", 0)
        if generation != self._privacy_source_generation:
            self._privacy_off = False
            self._privacy_sequence = -1
            self._privacy_invalid_through = -1
            self._privacy_source_generation = generation
        revoked = self._bus.privacy_revocation
        if revoked is not None and revoked.source_generation == generation:
            self._privacy_invalid_through = max(self._privacy_invalid_through, revoked.sequence)
            if self._privacy_sequence <= revoked.sequence:
                self._privacy_off = False

    def _invalidate_privacy(self, devices: list[BrilliantDevice] | None = None) -> None:
        self._sync_privacy_generation()
        self._privacy_off = False
        # Delivery can lag capture. Fence the live source's high-water mark,
        # including empty reads that have no peripheral to carry provenance.
        sequences = [self._privacy_invalid_through, self._bus.capture_sequence]
        sequences.extend(
            d.capture_provenance.sequence
            for d in devices or ()
            if d.capture_provenance is not None
            and d.capture_provenance.source_generation == self._privacy_source_generation
        )
        self._privacy_invalid_through = max(sequences)

    def _observe_privacy(self, device: BrilliantDevice) -> None:
        if device.kind is not DeviceKind.HARDWARE:
            return
        self._sync_privacy_generation()
        provenance = device.capture_provenance
        if provenance is None and self._privacy_invalid_through >= 0:
            return
        if provenance is not None:
            if provenance.source_generation != self._privacy_source_generation:
                return
            if provenance.sequence < self._privacy_sequence:
                return
            if provenance.sequence <= self._privacy_invalid_through:
                return
            self._privacy_sequence = provenance.sequence
        if device.capture_hardware_count is not None and device.capture_hardware_count != 1:
            self._invalidate_privacy([device])
            return
        var = device.variables.get("privacy_toggle")
        self._privacy_off = (
            var is not None
            and var.value.strip() == "0"
            and "privacy_toggle" not in device.capture_unknown
            and (device.capture_present is None or "privacy_toggle" in device.capture_present)
        )

    def _observe_privacy_snapshot(self, devices: list[BrilliantDevice]) -> None:
        self._sync_privacy_generation()
        hardware = [d for d in devices if self._included(d) and d.kind is DeviceKind.HARDWARE]
        if len(hardware) != 1:
            self._invalidate_privacy(devices)
            return
        provenance = hardware[0].capture_provenance
        if provenance is not None and (
            provenance.source_generation != self._privacy_source_generation
        ):
            self._invalidate_privacy(devices)
            return
        self._observe_privacy(hardware[0])

    @staticmethod
    def _is_wired_primary(device: BrilliantDevice) -> bool:
        return device.device_id != _MESH_DEVICE_ID and device.kind in (
            DeviceKind.LIGHT,
            DeviceKind.SWITCH,
        )

    def _wired_slot(self, peripheral_id: str) -> _WiredSlot:
        return self._wired.setdefault(
            peripheral_id, _WiredSlot(Record(source_generation=self._wired_source_generation))
        )

    def _feedback_alive(self, lifecycle: int) -> bool:
        return self._panel == _MESH_PANEL or (
            self._wired_feedback_enabled and lifecycle == self._wired_lifecycle
        )

    def _wired_event(self, peripheral_id: str, event: Event) -> _WiredSlot:
        self._wired_source_generation = getattr(self._bus, "capture_generation", 0)
        slot = self._wired_slot(peripheral_id)
        if isinstance(event, Capture):
            provenance = event.device.capture_provenance
            if (
                provenance is not None
                and provenance.source_generation > self._wired_source_generation
            ):
                return slot
        if slot.record.source_generation < self._wired_source_generation:
            slot.record = reduce_wired(
                slot.record, SourceGeneration(self._wired_source_generation)
            ).record
        slot.record = reduce_wired(slot.record, event).record
        self._sync_wired_timer(peripheral_id, slot)
        return slot

    def _remember_native_observation(self, observed: BrilliantDevice) -> BrilliantDevice:
        """Reduce wired captures before any broker await or publication."""
        if not self._is_wired_primary(observed):
            self._remember_device(observed)
            return observed
        peripheral_id = observed.peripheral_id
        if not self._wired_feedback_enabled:
            return self._devices.get(peripheral_id, observed)
        slot = self._wired_event(peripheral_id, Capture(observed))
        native = slot.record.native
        if native is None:
            return self._devices.get(peripheral_id, observed)
        if self._devices.get(peripheral_id) is not native:
            self._remember_device(native)
        return native

    def _beat(self) -> None:
        if self._heartbeat is not None:
            self._heartbeat()

    async def reconcile(self) -> None:
        """Publish availability, discovery configs, and initial state for all devices.

        Already-subscribed command topics are skipped. Mesh inventory removal
        retains a bounded absence publication and exact route/config cleanup;
        wired discovery retains its existing operational cleanup policy.
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
        lifecycle = self._wired_lifecycle
        if not self._feedback_alive(lifecycle):
            return
        if (
            self._panel == _MESH_PANEL
            and self._include is not None
            and not self._include(_MESH_DEVICE_ID)
        ):
            return
        if self._mesh is not None:
            self._mesh.acquire()
        devices = [d for d in await self._bus.get_all() if self._included(d)]
        if not self._feedback_alive(lifecycle):
            return
        self._beat()
        # Refresh before broker awaits so ordinary poll/push repairs and online
        # readiness use the current filtered HARDWARE context, including absence.
        sw_version = _sw_version_from(devices)
        if self._discovery_sw_version != sw_version:
            self._discovery_sw_version = sw_version
            self._mesh_status_changed()
        self._observe_privacy_snapshot(devices)

        # A broker await may admit a command or native push. Apply this captured
        # wired batch before those awaits so it cannot later overwrite their state.
        for captured in devices:
            if self._is_wired_primary(captured) and entities_for(captured, self._panel):
                observed = self._derived(captured)
                self._remember_native_observation(observed)

        if self._panel != _MESH_PANEL:
            await self._async_publish_retained(availability_topic(self._panel), "online")
        if not self._feedback_alive(lifecycle):
            return

        # Bridge meta: the companion integration's machine contract. Retained and
        # republished on every reconcile (idempotent, like discovery configs).
        if self._panel != _MESH_PANEL:
            meta: dict[str, object] = {"agent_version": __version__}
            if self._deployment_id is not None:
                meta["deployment_id"] = self._deployment_id
            if sw_version is not None:
                meta["panel_firmware"] = sw_version
            if self._diagnostics is not None:
                meta["diag"] = self._diagnostics.meta()
            await self._async_publish_retained(
                meta_topic(self._panel),
                json.dumps(meta, sort_keys=True),
            )
            if not self._feedback_alive(lifecycle):
                return

        n_devices = n_entities = 0
        for device in devices:
            if device.device_id == _MESH_DEVICE_ID:
                await self._repair_mesh(device.peripheral_id, force=True)
                if not self._feedback_alive(lifecycle):
                    return
                continue
            descriptors = entities_for(device, self._panel)
            if not descriptors:
                # UNKNOWN / SENSOR — no HA entity; skip entirely.
                continue
            if self._is_wired_primary(device):
                current = self._devices.get(device.peripheral_id)
                if current is None:
                    continue
                device = current
                # Discovery may await the broker while a push rebinds this load.
                # Publish the new binding's config before using its descriptors.
                while True:
                    descriptors = entities_for(device, self._panel)
                    if not descriptors:
                        break
                    for descriptor in descriptors:
                        await self._async_publish_retained(
                            config_topic(descriptor),
                            config_payload(descriptor, sw_version=sw_version),
                        )
                        if not self._feedback_alive(lifecycle):
                            return
                    current = self._devices.get(device.peripheral_id)
                    if current is None or self._same_binding(current, device):
                        break
                    device = current
                if current is None or not descriptors:
                    continue
                device = current
            else:
                observed = self._derived(device)
                device = self._remember_native_observation(observed)
                for descriptor in descriptors:
                    await self._async_publish_retained(
                        config_topic(descriptor),
                        config_payload(descriptor, sw_version=sw_version),
                    )
                    if not self._feedback_alive(lifecycle):
                        return

            n_devices += 1
            n_entities += len(descriptors)

            # Publish exactly ONE shared state payload per peripheral, whenever
            # the device contributes any payload fields. Forced: reconcile is
            # the level-triggered repair pass, so it republishes even when the
            # payload is unchanged (and re-primes the diff cache).
            fields = self._payload_fields(device)
            if fields:
                await self._publish_state(device, fields, force=True)
                if not self._feedback_alive(lifecycle):
                    return

            # Subscribe command topics: the primary JSON topic for light/switch,
            # plus a per-variable topic for every aux switch/number/button.
            for descriptor in descriptors:
                self._register_command_topic(device.peripheral_id, descriptor)
                topic = self._command_topic_for(device.peripheral_id, descriptor)
                if topic is None or topic in self._subscribed:
                    continue
                await self._mqtt.subscribe(topic)
                if not self._feedback_alive(lifecycle):
                    return
                self._subscribed.add(topic)

        logger.info(
            "reconcile: %d devices -> %d entities, %d command topics registered",
            n_devices,
            n_entities,
            len(self._by_cmd_topic),
        )
        if self._feedback_alive(lifecycle):
            await self._flush_retired_mesh()
            await self._enforce_desired(devices)
            await self._repair_mesh_availability()

    async def reconcile_after_reconnect(self) -> None:
        """Retire session-local wired evidence before reading a new bus generation."""
        self._invalidate_privacy()
        if not self._wired_feedback_enabled:
            return
        lifecycle = self._wired_lifecycle
        self._wired_source_generation = getattr(self._bus, "capture_generation", 0)
        for peripheral_id in list(self._wired):
            self._wired_event(peripheral_id, SourceGeneration(self._wired_source_generation))
            await self._publish_wired_slot(peripheral_id, force=True)
            if not self._wired_feedback_enabled or lifecycle != self._wired_lifecycle:
                return
        await self.reconcile()

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
        if (
            previous is not None
            and previous != route
            and (self._mesh is None or self._mesh.current(peripheral_id) is None)
        ):
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
        # Close routing and registry synchronously before joining effects or
        # awaiting the broker. Queued commands and ready expiry lose ownership
        # at this boundary, while actual native RPCs retain their physical lock.
        topics = [topic for topic in self._by_cmd_topic if topic not in self._mesh_routing.records]
        for peripheral_id in self._command_generation:
            self._command_generation[peripheral_id] += 1
        self._by_cmd_topic.clear()
        self._subscribed.difference_update(topics)
        if self._mesh is not None:
            self._mesh.close()
        mesh_repairs = self._mesh_routing.withdraw()
        self._mesh_route_owners.clear()
        self._mesh_descriptors.clear()
        self._last_state_payload.clear()
        self._last_state_fields.clear()
        self._devices.clear()
        self._discovery_sw_version = None
        if self._deriver is not None:
            self._deriver.clear()
        if self._panel == _MESH_PANEL:
            self._wired_lifecycle += 1
        if self._mesh_driver is not None:
            await self._mesh_driver.shutdown()
        if self._panel != _MESH_PANEL:
            await self.shutdown_wired_feedback()
        unsubscribed = 0
        await self._mesh_routing.join(mesh_repairs)
        for topic in topics:
            # A failed unsubscribe must not abort the rest — a step-down must
            # always complete so the routing/state caches are reliably cleared.
            try:
                await self._mqtt.unsubscribe(topic)
                self._subscribed.discard(topic)
                unsubscribed += 1
            except Exception:
                logger.exception("withdraw: unsubscribe failed for %s; continuing", topic)
        self._wired_feedback_enabled = True
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
        lifecycle = self._wired_lifecycle
        if not self._feedback_alive(lifecycle):
            return
        if devices is None:
            try:
                devices = await self._bus.get_all()
            except (TimeoutError, asyncio.TimeoutError) as error:
                # Type only the scoped READ boundary. The session coordinator may
                # grant one retry without also swallowing MQTT publish or desired
                # write timeouts from the rest of this method.
                raise HotPollReadTimeout("hot poll bus read timed out") from error
            self._beat()
        if not self._feedback_alive(lifecycle):
            return
        self._observe_privacy_snapshot(devices)
        for device in devices:
            # Same scope filter as reconcile: the shared get_all returns every
            # bus device, including the other bridge's.
            if not self._included(device):
                continue
            if device.device_id == _MESH_DEVICE_ID:
                await self._repair_mesh(device.peripheral_id, force=False)
                if not self._feedback_alive(lifecycle):
                    return
                continue
            if not entities_for(device, self._panel):
                continue
            observed = self._derived(device)
            device = self._remember_native_observation(observed)
            fields = self._payload_fields(device)
            if fields:
                await self._publish_state(device, fields, force=False)
                if not self._feedback_alive(lifecycle):
                    return
        if self._feedback_alive(lifecycle):
            await self._flush_retired_mesh()
            await self._enforce_desired(devices)
            await self._repair_mesh_availability()

    def _motion_default_wants(self, device: BrilliantDevice) -> dict[str, str]:
        """Config-driven motion defaults for *device*, overlaid under recorded intent.

        Never persisted: they are configuration, not user intent, so unsetting
        the env vars stops asserting them. Scoring is only turned off on a switch
        whose observed thresholds are already calibrated — an uncalibrated switch
        with scoring off would have its motion gated off for good.
        """
        defaults: dict[str, str] = {}
        variables = device.variables
        if self._motion_default_high_threshold is not None and "motion_high_threshold" in variables:
            defaults["motion_high_threshold"] = str(self._motion_default_high_threshold)
        if self._motion_default_low_threshold is not None and "motion_low_threshold" in variables:
            defaults["motion_low_threshold"] = str(self._motion_default_low_threshold)
        if (
            self._motion_native_passthrough
            and "enable_motion_score" in variables
            and has_calibrated_motion_thresholds(device)
        ):
            defaults["enable_motion_score"] = "0"
        return defaults

    async def _enforce_desired(self, devices: list[BrilliantDevice]) -> None:
        """Re-assert drifted reconciled vars (firmware reverts the enable flags).

        Per peripheral, batch every drifted desired var into ONE set_variables
        call (avoids the same-peripheral rapid-write race). Rate-limit per
        (pid, var) and cap the number of peripherals written per tick so a fleet
        of drifted devices ramps gently instead of bursting the Thrift bus.
        """
        if self._desired is None:
            return
        lifecycle = self._wired_lifecycle
        now = self._clock()
        writes = 0
        for device in devices:
            if not self._feedback_alive(lifecycle):
                return
            if not self._included(device):
                continue
            if device.device_id == _MESH_DEVICE_ID:
                current = self._current_device(device.peripheral_id)
                if current is None:
                    continue
                device = current
            wanted = {
                **self._motion_default_wants(device),
                **self._desired.wanted(device.peripheral_id),
            }
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
                if device.device_id == _MESH_DEVICE_ID:
                    owner = (
                        self._mesh.current(device.peripheral_id) if self._mesh is not None else None
                    )
                    if owner is None or self._mesh_commands is None:
                        continue
                    result = await self._mesh_commands.execute(
                        owner, drifted, primary=False, maintenance=True
                    )
                    if isinstance(result, Superseded):
                        continue
                else:
                    await self._bus.set_variables(
                        device.device_id,
                        device.peripheral_id,
                        drifted,
                        write_class=WriteClass.MAINTENANCE,
                    )
                if not self._feedback_alive(lifecycle):
                    return
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

        Mesh publication delegates entirely to its owner and final driver lane.

        The caches commit only AFTER the broker accepted the publish: a failed
        publish must not leave them claiming the wire carries this payload, or
        every identical next tick would be diff-suppressed and the retained
        topic would stay stale until the value changed again.
        """
        peripheral_id = device.peripheral_id
        if device.device_id == _MESH_DEVICE_ID:
            await self._flush_mesh(peripheral_id, force=force)
            return
        wired_primary = self._is_wired_primary(device)
        if wired_primary and (not self._wired_feedback_enabled or not self._included(device)):
            return
        if wired_primary:
            await self._publish_wired_slot(peripheral_id, force=force)
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
        await self._async_publish_retained(
            state_topic(self._panel, peripheral_id),
            payload,
        )
        self._last_state_fields[peripheral_id] = fields
        self._last_state_payload[peripheral_id] = payload

    def _sync_wired_timer(self, peripheral_id: str, slot: _WiredSlot) -> None:
        deadline = timer_deadline(slot.record)
        if deadline == slot.timer_at and slot.timer is not None and not slot.timer.done():
            return
        prior = slot.timer
        if prior is not None and prior is not asyncio.current_task():
            prior.cancel()
        slot.timer = None
        slot.timer_at = deadline
        if deadline is not None and self._wired_feedback_enabled:
            slot.timer = asyncio.create_task(self._wired_expire(peripheral_id, slot, deadline))

    async def _wired_expire(self, peripheral_id: str, slot: _WiredSlot, deadline: float) -> None:
        await self._sleep(max(0.0, deadline - self._clock()))
        if self._wired.get(peripheral_id) is not slot or not self._wired_feedback_enabled:
            return
        if slot.record.deadline_at != deadline:
            return
        self._wired_event(peripheral_id, Expire(self._clock()))
        try:
            await self._republish_snapshot(peripheral_id, force=True)
        except Exception as error:
            logger.warning("WIRED_PUBLISH_FAILED %s", type(error).__name__)

    async def _publish_wired_slot(self, peripheral_id: str, *, force: bool) -> None:
        slot = self._wired.get(peripheral_id)
        if slot is None or slot.record.closed or not self._wired_feedback_enabled:
            return
        slot.dirty = True
        slot.force |= force
        if slot.publisher is not None and not slot.publisher.done():
            return
        first: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        slot.publisher = asyncio.create_task(self._run_wired_publisher(peripheral_id, slot, first))

        def settle_if_never_started(_task: asyncio.Task[None]) -> None:
            if not first.done():
                first.set_result(None)

        slot.publisher.add_done_callback(settle_if_never_started)
        await asyncio.shield(first)

    async def _run_wired_publisher(
        self, peripheral_id: str, slot: _WiredSlot, first: asyncio.Future[None]
    ) -> None:
        def complete_first() -> None:
            if not first.done():
                first.set_result(None)

        try:
            while slot.dirty and self._wired.get(peripheral_id) is slot:
                if slot.record.closed or not self._wired_feedback_enabled:
                    return
                slot.dirty = False
                force = slot.force
                slot.force = False
                rendered = render_wired(slot.record)
                if rendered is None or not rendered.fields:
                    complete_first()
                    continue
                fields = rendered.fields
                if (
                    not force
                    and not slot.record.native_debt
                    and self._last_state_fields.get(peripheral_id) == fields
                ):
                    complete_first()
                    continue
                payload = _encode_fields(fields)
                revision = slot.record.revision
                debt_epoch = slot.record.native_debt_epoch
                try:
                    await self._async_publish_retained(
                        state_topic(self._panel, peripheral_id), payload
                    )
                except asyncio.CancelledError:
                    # MQTT may have accepted the bytes before local cancellation.
                    self._last_state_fields.pop(peripheral_id, None)
                    self._last_state_payload.pop(peripheral_id, None)
                    if slot.record.closed or not self._wired_feedback_enabled:
                        raise
                    slot.dirty = True
                    continue
                except Exception as error:
                    self._last_state_fields.pop(peripheral_id, None)
                    self._last_state_payload.pop(peripheral_id, None)
                    if self._wired.get(peripheral_id) is slot:
                        slot.record = reduce_wired(
                            slot.record, PublishFailed(revision, rendered.native)
                        ).record
                    logger.warning("WIRED_PUBLISH_FAILED %s", type(error).__name__)
                    if slot.dirty:
                        continue
                    return
                finally:
                    complete_first()
                if self._wired.get(peripheral_id) is not slot or slot.record.closed:
                    return
                transition = reduce_wired(
                    slot.record,
                    PublishAccepted(revision, rendered.native, debt_epoch=debt_epoch),
                )
                slot.record = transition.record
                self._last_state_fields[peripheral_id] = fields
                self._last_state_payload[peripheral_id] = payload
                self._sync_wired_timer(peripheral_id, slot)
                if transition.effects.publish or slot.record.revision != revision:
                    # Accepted old bytes still need a current retained publication.
                    slot.dirty = True
        except Exception as error:
            logger.warning("WIRED_PUBLISH_FAILED %s", type(error).__name__)
        finally:
            complete_first()

    async def shutdown_wired_feedback(self) -> None:
        """Terminally retire wired work and join its one timer/publisher per load."""
        self._privacy_off = False
        self._wired_feedback_enabled = False
        self._wired_lifecycle += 1
        slots = list(self._wired.values())
        tasks: list[asyncio.Task[None]] = []
        for slot in slots:
            slot.record = reduce_wired(slot.record, Shutdown()).record
            for task in (slot.timer, slot.publisher):
                if task is not None:
                    task.cancel()
                    tasks.append(task)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._wired.clear()

    async def shutdown_mesh_feedback(self) -> None:
        if self._mesh is not None:
            self._mesh.close()
        await self._mesh_routing.join(self._mesh_routing.withdraw())
        if self._mesh_driver is not None:
            await self._mesh_driver.shutdown()
        if self._mesh is not None:
            self._mesh.dispose()

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
        if device.device_id == _MESH_DEVICE_ID:
            await self._repair_mesh(device.peripheral_id, force=False)
            await self._flush_retired_mesh()
            return
        if self._wired_feedback_enabled:
            self._observe_privacy(device)
        if self._is_wired_primary(device) and not self._wired_feedback_enabled:
            return

        # Discovery/subscribe is reconcile-only by design: a change event for a
        # never-reconciled peripheral publishes state HA ignores until the
        # periodic resync re-runs reconcile and closes the gap.
        descriptors = entities_for(device, self._panel)
        if not descriptors:
            return

        observed = self._derived(device)
        device = self._remember_native_observation(observed)

        fields = self._payload_fields(device)
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
            logger.debug("COMMAND_ROUTE_UNKNOWN")
            return
        peripheral_id, descriptor = route
        owner = self._mesh.current(peripheral_id) if self._mesh is not None else None
        cached = self._devices.get(peripheral_id)
        if owner is not None or cached is not None and cached.device_id == _MESH_DEVICE_ID:
            admission = command_admission.get()
            if owner is None or admission is None or admission.mesh_owner is not owner:
                return
            if self._resolve_mesh_owner(topic) is not owner:
                return
            if descriptor is not None and descriptor not in entities_for(
                owner.materialize(self._clock()), self._panel
            ):
                return

        if descriptor is None:
            await self._handle_primary_command(topic, peripheral_id, payload)
        else:
            await self._handle_aux_command(topic, peripheral_id, descriptor, payload)

    async def _handle_primary_command(self, topic: str, peripheral_id: str, payload: str) -> None:
        """PRIMARY JSON light/switch path (unchanged behaviour)."""
        try:
            parsed = json.loads(payload)
        except (json.JSONDecodeError, ValueError):
            logger.debug("COMMAND_JSON_INVALID")
            return

        if not isinstance(parsed, dict):
            logger.debug("COMMAND_OBJECT_INVALID")
            return

        device = self._current_device(peripheral_id)
        if device is None:
            logger.debug("COMMAND_DEVICE_UNKNOWN")
            return
        if self._is_wired_primary(device) and not self._wired_feedback_enabled:
            return

        sets: list[VarSet] = translate_command(device, parsed)
        if sets:
            route = self._by_cmd_topic.get(topic)
            target = (device.device_id, device.peripheral_id)
            translation = (device.kind, device.is_dimmable, device.max_intensity)
            generation = (
                self._command_generation.setdefault(peripheral_id, 0)
                if device.device_id != _MESH_DEVICE_ID
                else 0
            )

            def decode(new_payload: str) -> list[VarSet]:
                current = self._current_device(peripheral_id)
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

            logger.info("COMMAND_ACCEPTED")
            if device.device_id == _MESH_DEVICE_ID:
                # Mesh primaries: no optimistic echo — observation-confirmed
                # write with a pending-visible (state: null) window instead.
                owner = self._mesh.current(peripheral_id) if self._mesh is not None else None
                if owner is not None and self._mesh_commands is not None:

                    def decode_mesh(new_payload: str) -> list[VarSet]:
                        if self._resolve_mesh_owner(topic) is not owner:
                            return []
                        try:
                            value = json.loads(new_payload)
                        except (json.JSONDecodeError, ValueError):
                            return []
                        return (
                            translate_command(owner.materialize(self._clock()), value)
                            if isinstance(value, dict)
                            else []
                        )

                    try:
                        await self._mesh_commands.execute(
                            owner, sets, primary=True, decode=decode_mesh
                        )
                    except (TimeoutError, asyncio.TimeoutError):
                        logger.warning(
                            "mesh write unresolved at caller deadline: %s", peripheral_id
                        )
                    except Exception as error:
                        logger.warning(
                            "mesh write failed %s %s", peripheral_id, type(error).__name__
                        )
                return
            if peripheral_id not in self._wired or self._wired[peripheral_id].record.native is None:
                self._wired_event(peripheral_id, Capture(device))
            slot = self._wired_event(
                peripheral_id, Begin(tuple((item.name, item.value) for item in sets))
            )
            attempt_generation = slot.record.generation

            def on_issued(provenance: CaptureProvenance) -> None:
                if self._wired.get(peripheral_id) is slot:
                    self._wired_event(
                        peripheral_id,
                        Issue(
                            attempt_generation,
                            provenance.field_eras,
                            provenance.issued_values,
                        ),
                    )

            try:
                result = await self._write_command(
                    device,
                    peripheral_id,
                    sets,
                    decode,
                    on_issued=on_issued,
                )
            except BaseException:
                if self._wired.get(peripheral_id) is slot:
                    self._wired_event(
                        peripheral_id,
                        Outcome(attempt_generation, "failed", self._clock(), self._wall_clock()),
                    )
                    try:
                        await self._republish_snapshot(peripheral_id, force=True)
                    except Exception as error:
                        logger.warning("WIRED_PUBLISH_FAILED %s", type(error).__name__)
                raise
            if self._wired.get(peripheral_id) is not slot:
                return
            if isinstance(result, Superseded):
                self._wired_event(
                    peripheral_id,
                    Outcome(attempt_generation, "superseded", self._clock(), self._wall_clock()),
                )
                return
            self._wired_event(
                peripheral_id,
                Outcome(attempt_generation, "success", self._clock(), self._wall_clock()),
            )
            await self._republish_snapshot(peripheral_id, force=False)

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
        if self._by_cmd_topic.get(topic) != (peripheral_id, d):
            return
        device = self._current_device(peripheral_id)
        owner = self._mesh.current(peripheral_id) if self._mesh is not None else None
        cached = self._devices.get(peripheral_id)
        if (
            owner is not None
            or cached is not None
            and cached.device_id == _MESH_DEVICE_ID
            or topic in self._mesh_route_owners
        ):
            # Rejected mesh intent must not become a later maintenance write.
            admission = command_admission.get()
            if (
                owner is None
                or admission is None
                or admission.mesh_owner is not owner
                or self._resolve_mesh_owner(topic) is not owner
                or device is None
                or d not in entities_for(device, self._panel)
            ):
                return
        if self._desired is not None and d.command_var in RECONCILED_VARS:
            self._desired.record(peripheral_id, d.command_var, value)
        if device is None:
            # Without a snapshot we cannot know which bus device owns the
            # peripheral, so the write cannot be routed — mirror the primary
            # path's unknown-peripheral guard.
            logger.debug("aux command for unknown peripheral %s; ignoring", peripheral_id)
            return
        if self._is_wired_primary(device) and not self._wired_feedback_enabled:
            return
        logger.info("aux command %s -> %s: %s=%s", topic, peripheral_id, d.command_var, value)
        sets = [VarSet(d.command_var, value)]
        route = self._by_cmd_topic.get(topic)
        target = (device.device_id, device.peripheral_id)
        generation = (
            self._command_generation.setdefault(peripheral_id, 0)
            if device.device_id != _MESH_DEVICE_ID
            else 0
        )

        def decode(new_payload: str) -> list[VarSet]:
            current = self._current_device(peripheral_id)
            if (
                self._command_generation.get(peripheral_id) != generation
                or self._by_cmd_topic.get(topic) != route
                or current is None
                or (current.device_id, current.peripheral_id) != target
            ):
                return []
            value = translate_aux(new_payload, d.value_kind, d.invert, d.min_value, d.max_value)
            return [VarSet(d.command_var, value)] if value is not None and d.command_var else []

        if device.device_id == _MESH_DEVICE_ID:
            owner = self._resolve_mesh_owner(topic)
            if owner is None or self._mesh_commands is None:
                return

            def decode_mesh_aux(new_payload: str) -> list[VarSet]:
                if self._resolve_mesh_owner(topic) is not owner:
                    return []
                value = translate_aux(new_payload, d.value_kind, d.invert, d.min_value, d.max_value)
                return [VarSet(d.command_var, value)] if value is not None and d.command_var else []

            await self._mesh_commands.execute(
                owner,
                sets,
                primary=False,
                decode=decode_mesh_aux if d.component == "number" else None,
            )
            return
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

    async def _republish_snapshot(self, peripheral_id: str, *, force: bool) -> None:
        """Publish the stored snapshot's current payload (no-op without one)."""
        device = self._current_device(peripheral_id)
        if device is None:
            return
        fields = self._payload_fields(device)
        if fields:
            await self._publish_state(device, fields, force=force)

    async def _echo_state(self, peripheral_id: str, sets: list[VarSet]) -> None:
        """Optimistically fold written VarSets into the snapshot and republish state.

        The bus does not push notifications for some written variables (pilot
        finding 2026-06-12: a muted=1 write succeeded but no notification ever
        arrived), so HA would stay stale until the periodic resync. Echoing the
        commanded values immediately keeps HA in sync; real bus notifications and
        the resync still own externally-caused changes.
        """
        device = self._current_device(peripheral_id)
        if device is None:
            # Write already happened; nothing to echo without a snapshot.
            return

        if device.device_id == _MESH_DEVICE_ID:
            await self._flush_mesh(peripheral_id)
            return

        # Build a NEW variables dict — never mutate one a pending callback may share.
        new_vars = dict(device.variables)
        for s in sets:
            old = new_vars.get(s.name)
            settable = old.externally_settable if old is not None else True
            new_vars[s.name] = Variable(s.name, s.value, externally_settable=settable)
        if self._is_wired_primary(device):
            aux = {
                s.name: new_vars[s.name]
                for s in sets
                if s.name not in {"on", "intensity", "max_intensity_value"}
            }
            if aux and self._wired_feedback_enabled:
                if (
                    peripheral_id not in self._wired
                    or self._wired[peripheral_id].record.native is None
                ):
                    self._wired_event(peripheral_id, Capture(device))
                partial = replace(
                    device,
                    variables=aux,
                    capture_provenance=None,
                    capture_complete=False,
                    capture_present=frozenset(aux),
                    capture_unknown=frozenset(),
                )
                self._remember_native_observation(partial)
                await self._publish_wired_slot(peripheral_id, force=False)
            return
        updated = replace(device, variables=new_vars)
        self._remember_device(updated)

        fields = self._payload_fields(updated)
        if fields:
            await self._publish_state(updated, fields, force=False)
