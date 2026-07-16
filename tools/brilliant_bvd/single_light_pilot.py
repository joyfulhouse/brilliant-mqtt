"""Current-owner-only BVD pilot for one Home Assistant light.

The module is deliberately importable off-panel.  Its bus Protocol is
read-only: this pilot never bids for, refreshes, clears, or restores the
``brilliant_virtual_device`` owner variable.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Protocol, TypeVar, cast
from uuid import UUID, uuid4

from brilliant_mqtt.ha_control_protocol import (
    MAPPING_VERSION,
    SCHEMA_VERSION,
    command_topic,
    encode_json,
    result_topic,
    stable_id,
    state_topic,
)

OFFICE_DEVICE_ID = "017ff60733f100038e04fa0fbab29096"
BVD_DEVICE_ID = "brilliant_virtual_device"
BVD_CONFIGURATION_PERIPHERAL_ID = "brilliant_virtual_device_configuration"
TARGET_ENTITY_ID = "light.backyard_light_group"

_ONLINE_STATUS = 1
_MAX_OWNER_AGE_MS = 30_000
_MAX_FUTURE_SKEW_MS = 5_000
_LINK_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}")

EXPECTED_BVD_PERIPHERAL_TYPES: dict[str, int] = {
    "device_groups_configuration": 92,
    "remote_bridge": 9,
    "request_dispatcher": 93,
    "solar_peripheral": 97,
    "thirdparty_discovery_peripheral": 99,
    "weather_peripheral": 79,
}
EXPECTED_PROCESS_CONFIGS = frozenset(
    {
        "device_groups_configuration",
        "request_dispatcher",
        "solar_peripheral",
        "thirdparty_discovery_peripheral",
        "weather_peripheral",
    }
)


class PilotGuardError(ValueError):
    """Raised before the pilot may cross an unsafe boundary."""


@dataclass(frozen=True, slots=True)
class PilotConfig:
    """Immutable user-selectable values inside the fixed pilot boundary."""

    room_assignment_id: str
    display_name: str
    active_runtime_s: int

    def __post_init__(self) -> None:
        if _LINK_ID.fullmatch(self.room_assignment_id) is None:
            raise PilotGuardError("room assignment ID is invalid")
        if not 1 <= len(self.display_name) <= 80 or any(
            ord(character) < 32 for character in self.display_name
        ):
            raise PilotGuardError("display name must contain 1 to 80 printable characters")
        if type(self.active_runtime_s) is not int or not 60 <= self.active_runtime_s <= 120:
            raise PilotGuardError("active runtime must be an integer from 60 to 120 seconds")


@dataclass(frozen=True, slots=True)
class PeripheralFact:
    """Minimal normalized fact for one BVD peripheral."""

    peripheral_id: str
    peripheral_type: int
    status: int
    variables: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class BvdTopology:
    """Read-only facts needed to admit the current-owner pilot."""

    owning_device_id: str
    configuration_owner: str
    owner_timestamp_ms: int
    bvd_device_type: int
    stock_host_running: bool
    stock_host_identity: str
    process_config_peripheral_ids: frozenset[str]
    peripherals: tuple[PeripheralFact, ...]


@dataclass(frozen=True, slots=True)
class ManifestAuthority:
    """Committed HA manifest coordinate authorizing the fixed light route."""

    revision: int
    generated_at_ms: int


class BvdBus(Protocol):
    """Read-only bus seam; ownership mutation is intentionally absent."""

    async def start(self) -> None: ...

    async def snapshot(self) -> BvdTopology: ...

    async def room_ids(self) -> frozenset[str]: ...

    async def peripheral_exists(self, device_id: str, peripheral_id: str) -> bool: ...

    def on_reconnect(self, callback: Callable[[], Awaitable[None]]) -> None: ...

    def notification_marker(self) -> int: ...

    async def wait_for_notification_after(self, marker: int, timeout_s: float) -> None: ...

    def seconds_since_last_notification(self) -> float | None: ...

    async def shutdown(self) -> None: ...


@dataclass(frozen=True, slots=True)
class VariableDefinition:
    """Framework-independent representation of one native VariableSpec."""

    value_type: type
    externally_settable: bool
    default_value: object


@dataclass(frozen=True, slots=True)
class DecodedState:
    """One strictly validated HA-authoritative state publication."""

    sequence: int
    generated_at_ms: int
    available: bool
    on: int
    intensity: int | None


class Publisher(Protocol):
    """Minimal MQTT publication seam used by the pure controller."""

    async def publish(self, topic: str, payload: str, retain: bool = False) -> None: ...


class StateSink(Protocol):
    """Internal-only native state reflection seam."""

    async def update_variables(self, values: Mapping[str, object]) -> None: ...


class VirtualLightHost(StateSink, Protocol):
    """One native LIGHT host fixed to an explicit virtual-device target."""

    async def start(
        self,
        *,
        peripheral_id: str,
        virtual_device_id: str,
        variables: Mapping[str, VariableDefinition],
        on_command: Callable[[str, object], Awaitable[bool]],
    ) -> None: ...

    async def delete(self, peripheral_id: str, deletion_time_ms: int) -> None: ...

    async def shutdown(self) -> None: ...


class ScopedPeripheralProbe(Protocol):
    """A fresh, narrow bus reader used once for cleanup proof."""

    async def contains(self, device_id: str, peripheral_id: str) -> bool: ...

    async def shutdown(self) -> None: ...


@dataclass(frozen=True, slots=True)
class CleanupReport:
    """Result of an idempotent two-observation absence proof."""

    already_clean: bool
    absent_first: bool
    absent_second: bool


class CleanupError(RuntimeError):
    """Raised when the persistent pilot peripheral cannot be proven absent."""


_T = TypeVar("_T")


async def _await_operation(operation: Awaitable[_T], *, timeout_s: float, description: str) -> _T:
    try:
        return await asyncio.wait_for(operation, timeout=timeout_s)
    except asyncio.TimeoutError:
        raise PilotGuardError(f"{description} exceeded its bounded timeout") from None


def peripheral_id_for_entity(entity_id: str) -> str:
    """Return the one stable native identity permitted by this pilot."""

    if entity_id != TARGET_ENTITY_ID:
        raise PilotGuardError("pilot entity must be light.backyard_light_group")
    return f"ha_bvd_{stable_id(entity_id).replace('-', '')}"


def validate_preflight(config: PilotConfig, topology: BvdTopology, *, now_ms: int) -> None:
    """Require fresh natural ownership and the intact stock BVD host."""

    del config  # Construction already validates every configurable value.
    if type(now_ms) is not int:
        raise PilotGuardError("current time must be an integer")
    _validate_stock_topology(topology, phase="preflight")
    if type(topology.owner_timestamp_ms) is not int:
        raise PilotGuardError("BVD owner timestamp must be an integer")
    owner_age_ms = now_ms - topology.owner_timestamp_ms
    if owner_age_ms < -_MAX_FUTURE_SKEW_MS:
        raise PilotGuardError("BVD owner timestamp is too far in the future")
    if owner_age_ms > _MAX_OWNER_AGE_MS:
        raise PilotGuardError("BVD owner lease is not fresh enough")


def _validate_stock_topology(topology: BvdTopology, *, phase: str) -> None:
    if topology.owning_device_id != OFFICE_DEVICE_ID:
        raise PilotGuardError("pilot must run on the exact Office panel")
    if topology.configuration_owner != OFFICE_DEVICE_ID:
        raise PilotGuardError("Office must already be the naturally elected owner")
    if topology.bvd_device_type != 3:
        raise PilotGuardError("BVD must be DeviceType 3")
    if not topology.stock_host_running or not topology.stock_host_identity:
        raise PilotGuardError("stock BVD host is not running locally")
    if topology.process_config_peripheral_ids != EXPECTED_PROCESS_CONFIGS:
        raise PilotGuardError("BVD process configuration set is not exact")

    facts = {fact.peripheral_id: fact for fact in topology.peripherals}
    if len(facts) != len(topology.peripherals):
        raise PilotGuardError("BVD peripheral IDs must be unique")
    expected_ids = set(EXPECTED_BVD_PERIPHERAL_TYPES)
    pilot_id = peripheral_id_for_entity(TARGET_ENTITY_ID)
    if phase == "active":
        expected_ids.add(pilot_id)
    if set(facts) != expected_ids:
        if phase == "active":
            raise PilotGuardError("BVD active peripheral set is not exact")
        if phase == "postflight":
            raise PilotGuardError("BVD postflight peripheral set is not exact")
        raise PilotGuardError("BVD does not contain the exact built-in set")

    for peripheral_id, expected_type in EXPECTED_BVD_PERIPHERAL_TYPES.items():
        fact = facts[peripheral_id]
        if fact.peripheral_type != expected_type:
            raise PilotGuardError(f"BVD peripheral {peripheral_id} has the wrong type")
        if fact.status != _ONLINE_STATUS:
            raise PilotGuardError(f"BVD peripheral {peripheral_id} is not ONLINE")
        if peripheral_id == "remote_bridge":
            if fact.variables.get("relay_device") != OFFICE_DEVICE_ID:
                raise PilotGuardError("BVD remote_bridge relay is not Office")
        elif fact.variables.get("configuration_peripheral_id") != BVD_CONFIGURATION_PERIPHERAL_ID:
            raise PilotGuardError(
                f"BVD peripheral {peripheral_id} has the wrong configuration link"
            )

    if phase == "active":
        pilot = facts[pilot_id]
        if pilot.peripheral_type != 27 or pilot.status != _ONLINE_STATUS:
            raise PilotGuardError("BVD pilot LIGHT is not type 27 and ONLINE")
        if pilot.variables.get("configuration_peripheral_id") != BVD_CONFIGURATION_PERIPHERAL_ID:
            raise PilotGuardError("BVD pilot LIGHT has the wrong configuration link")


def _validate_runtime_continuity(
    baseline: BvdTopology, observed: BvdTopology, *, phase: str
) -> None:
    _validate_stock_topology(observed, phase=phase)
    if observed.stock_host_identity != baseline.stock_host_identity:
        raise PilotGuardError("stock BVD host identity changed during the pilot")
    if type(observed.owner_timestamp_ms) is not int:
        raise PilotGuardError("BVD owner timestamp must be an integer")
    if observed.owner_timestamp_ms < baseline.owner_timestamp_ms:
        raise PilotGuardError("BVD owner timestamp regressed during the pilot")


def validate_active_topology(baseline: BvdTopology, observed: BvdTopology) -> None:
    """Require the six stock services plus exactly the one pilot LIGHT."""

    _validate_runtime_continuity(baseline, observed, phase="active")


def validate_postflight(baseline: BvdTopology, observed: BvdTopology) -> None:
    """Require the exact stock baseline after the pilot LIGHT is deleted."""

    _validate_runtime_continuity(baseline, observed, phase="postflight")


def validate_manifest_authority(payload: str, *, retained: bool) -> ManifestAuthority:
    """Require the committed HA manifest to authorize all fixed light commands."""

    if not retained:
        raise PilotGuardError("HA manifest authority must be retained")
    try:
        decoded: object = json.loads(payload)
    except (json.JSONDecodeError, TypeError) as decode_error:
        raise PilotGuardError("manifest payload must be valid JSON") from decode_error
    if not isinstance(decoded, dict):
        raise PilotGuardError("manifest payload must be an object")
    data = cast(dict[str, object], decoded)
    expected = {
        "schema_version",
        "mapping_version",
        "revision",
        "generated_at_ms",
        "entities",
        "unsupported_domains",
    }
    if set(data) != expected:
        raise PilotGuardError("manifest payload fields do not match the v1 contract")
    if (
        type(data["schema_version"]) is not int
        or data["schema_version"] != SCHEMA_VERSION
        or type(data["mapping_version"]) is not int
        or data["mapping_version"] != MAPPING_VERSION
    ):
        raise PilotGuardError("manifest payload version is unsupported")
    revision = _bounded_integer(
        data["revision"], minimum=1, maximum=2**63 - 1, name="manifest revision"
    )
    generated_at_ms = _bounded_integer(
        data["generated_at_ms"],
        minimum=0,
        maximum=2**63 - 1,
        name="manifest generated_at_ms",
    )
    entities = data["entities"]
    if not isinstance(entities, list):
        raise PilotGuardError("manifest entities must be a list")
    candidates: list[dict[str, object]] = []
    for raw_entity in entities:
        if not isinstance(raw_entity, dict):
            raise PilotGuardError("manifest entity must be an object")
        entity = cast(dict[str, object], raw_entity)
        if entity.get("stable_id") == stable_id(TARGET_ENTITY_ID):
            candidates.append(entity)
    if len(candidates) != 1:
        raise PilotGuardError("manifest must contain the exact pilot entity once")
    entity = candidates[0]
    entity_fields = {
        "stable_id",
        "entity_id",
        "domain",
        "device_class",
        "friendly_name",
        "ha_area",
        "brilliant_room",
        "commands",
        "capabilities",
    }
    if set(entity) != entity_fields:
        raise PilotGuardError("manifest entity fields do not match the v1 contract")
    if entity["entity_id"] != TARGET_ENTITY_ID or entity["domain"] != "light":
        raise PilotGuardError("manifest entity route is outside the pilot allowlist")
    commands = entity["commands"]
    expected_commands = {"turn_on", "turn_off", "set_brightness"}
    if (
        not isinstance(commands, list)
        or not all(isinstance(command, str) for command in commands)
        or len(commands) != len(expected_commands)
        or set(commands) != expected_commands
    ):
        raise PilotGuardError("manifest light commands are incomplete")
    if entity["capabilities"] != {"brightness": True}:
        raise PilotGuardError("manifest light must have the exact brightness capability")
    if not isinstance(data["unsupported_domains"], list):
        raise PilotGuardError("manifest unsupported_domains must be a list")
    return ManifestAuthority(revision, generated_at_ms)


def build_light_variables(
    config: PilotConfig, *, room_assignment_type: type
) -> dict[str, VariableDefinition]:
    """Build the exact type-27 LIGHT schema used by native slider surfaces."""

    room_assignment = room_assignment_type(room_ids=[config.room_assignment_id])
    return {
        "on": VariableDefinition(int, True, 0),
        "intensity": VariableDefinition(int, True, 500),
        "dimmable": VariableDefinition(int, False, 1),
        "max_intensity_value": VariableDefinition(int, False, 1_000),
        "minimum_dim_level": VariableDefinition(int, True, 100),
        "maximum_dim_level": VariableDefinition(int, True, 1_000),
        "display_name": VariableDefinition(str, True, config.display_name),
        "room_assignment": VariableDefinition(
            room_assignment_type,
            True,
            room_assignment,
        ),
        "mode_transition_settings": VariableDefinition(str, True, "{}"),
        "configuration_peripheral_id": VariableDefinition(
            str,
            False,
            BVD_CONFIGURATION_PERIPHERAL_ID,
        ),
    }


def _bounded_integer(value: object, *, minimum: int, maximum: int, name: str) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise PilotGuardError(f"{name} must be an integer from {minimum} to {maximum}")
    return value


def _canonical_uuid(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise PilotGuardError(f"{name} must be a UUID")
    try:
        canonical = str(UUID(value))
    except (AttributeError, TypeError, ValueError) as error:
        raise PilotGuardError(f"{name} must be a UUID") from error
    if canonical != value:
        raise PilotGuardError(f"{name} must use canonical UUID form")
    return canonical


def brightness_to_intensity(value: object) -> int:
    """Scale HA 0-255 to Brilliant 0-1000 with half-up rounding."""

    brightness = _bounded_integer(value, minimum=0, maximum=255, name="brightness")
    return (brightness * 2_000 + 255) // 510


def intensity_to_brightness(value: object) -> int:
    """Scale Brilliant 0-1000 to HA 0-255 with half-up rounding."""

    intensity = _bounded_integer(value, minimum=0, maximum=1_000, name="intensity")
    return (intensity * 510 + 1_000) // 2_000


def decode_state_payload(payload: str, *, stable_id: str) -> DecodedState:
    """Validate the one allowlisted retained HA light-state payload."""

    try:
        decoded: object = json.loads(payload)
    except (json.JSONDecodeError, TypeError) as error:
        raise PilotGuardError("state payload must be valid JSON") from error
    if not isinstance(decoded, dict):
        raise PilotGuardError("state payload must be an object")
    data = cast(dict[str, object], decoded)
    expected = {
        "schema_version",
        "mapping_version",
        "stable_id",
        "entity_id",
        "sequence",
        "generated_at_ms",
        "available",
        "state",
        "attributes",
    }
    if set(data) != expected:
        raise PilotGuardError("state payload fields do not match the v1 contract")
    if (
        type(data["schema_version"]) is not int
        or data["schema_version"] != SCHEMA_VERSION
        or type(data["mapping_version"]) is not int
        or data["mapping_version"] != MAPPING_VERSION
    ):
        raise PilotGuardError("state payload version is unsupported")
    canonical_stable_id = _canonical_uuid(stable_id, "stable_id")
    if data["stable_id"] != canonical_stable_id:
        raise PilotGuardError("state payload stable_id does not match the route")
    if data["entity_id"] != TARGET_ENTITY_ID:
        raise PilotGuardError("state payload entity is outside the pilot allowlist")
    sequence = _bounded_integer(data["sequence"], minimum=0, maximum=2**63 - 1, name="sequence")
    generated_at_ms = _bounded_integer(
        data["generated_at_ms"],
        minimum=0,
        maximum=2**63 - 1,
        name="generated_at_ms",
    )
    available = data["available"]
    if type(available) is not bool:
        raise PilotGuardError("state availability must be a boolean")
    state = data["state"]
    if available and state not in {"on", "off"}:
        raise PilotGuardError("available HA light state must be on or off")
    if not available and state not in {"unavailable", "unknown"}:
        raise PilotGuardError("unavailable HA light state must be unavailable or unknown")
    attributes = data["attributes"]
    if not isinstance(attributes, dict):
        raise PilotGuardError("state attributes must be an object")
    brightness = attributes.get("brightness") if available else None
    if available and brightness is None:
        raise PilotGuardError("available pilot light state must contain authoritative brightness")
    intensity = None if brightness is None else brightness_to_intensity(brightness)
    return DecodedState(
        sequence=sequence,
        generated_at_ms=generated_at_ms,
        available=available,
        on=1 if available and state == "on" else 0,
        intensity=intensity,
    )


def _normalize_framework_push(variable: str, value: object) -> int:
    """Normalize the closed native callback without accepting loose numbers."""

    if type(value) is int:
        normalized = value
    elif isinstance(value, str) and re.fullmatch(r"0|[1-9][0-9]*", value):
        normalized = int(value)
    else:
        raise PilotGuardError("framework push must be a canonical decimal integer")
    if variable == "on":
        return _bounded_integer(normalized, minimum=0, maximum=1, name="on")
    if variable == "intensity":
        return _bounded_integer(normalized, minimum=0, maximum=1_000, name="intensity")
    raise PilotGuardError("only on and intensity may become HA commands")


def build_command_payload(
    *,
    variable: str,
    value: object,
    observed_sequence: int,
    command_id: str,
    issued_at_ms: int,
) -> dict[str, object]:
    """Map one native type-27 push to the fixed MQTT control-plane entity."""

    canonical_command_id = _canonical_uuid(command_id, "command_id")
    sequence = _bounded_integer(
        observed_sequence,
        minimum=0,
        maximum=2**63 - 1,
        name="observed_sequence",
    )
    timestamp = _bounded_integer(issued_at_ms, minimum=0, maximum=2**63 - 1, name="issued_at_ms")
    normalized = _normalize_framework_push(variable, value)
    if variable == "on":
        kind = "turn_on" if normalized else "turn_off"
        command_value: object = None
    else:
        kind = "set_brightness"
        command_value = intensity_to_brightness(normalized)
    return {
        "schema_version": SCHEMA_VERSION,
        "mapping_version": MAPPING_VERSION,
        "command_id": canonical_command_id,
        "stable_id": stable_id(TARGET_ENTITY_ID),
        "kind": kind,
        "value": command_value,
        "observed_sequence": sequence,
        "issued_at_ms": timestamp,
    }


def _decode_result(payload: str) -> tuple[str, bool]:
    try:
        decoded: object = json.loads(payload)
    except (json.JSONDecodeError, TypeError) as decode_error:
        raise PilotGuardError("result payload must be valid JSON") from decode_error
    if not isinstance(decoded, dict):
        raise PilotGuardError("result payload must be an object")
    data = cast(dict[str, object], decoded)
    required = {
        "schema_version",
        "mapping_version",
        "command_id",
        "stable_id",
        "accepted",
        "resulting_sequence",
        "timestamp_ms",
    }
    optional = {"error", "elapsed_ms"}
    if not required.issubset(data) or set(data) - required - optional:
        raise PilotGuardError("result payload fields do not match the v1 contract")
    if (
        type(data["schema_version"]) is not int
        or data["schema_version"] != SCHEMA_VERSION
        or type(data["mapping_version"]) is not int
        or data["mapping_version"] != MAPPING_VERSION
    ):
        raise PilotGuardError("result payload version is unsupported")
    command_id = _canonical_uuid(data["command_id"], "command_id")
    if data["stable_id"] != stable_id(TARGET_ENTITY_ID):
        raise PilotGuardError("result payload stable_id is outside the pilot route")
    accepted = data["accepted"]
    if type(accepted) is not bool:
        raise PilotGuardError("result accepted must be a boolean")
    _bounded_integer(
        data["resulting_sequence"],
        minimum=0,
        maximum=2**63 - 1,
        name="resulting_sequence",
    )
    _bounded_integer(data["timestamp_ms"], minimum=0, maximum=2**63 - 1, name="timestamp_ms")
    if "elapsed_ms" in data:
        _bounded_integer(
            data["elapsed_ms"],
            minimum=0,
            maximum=2**63 - 1,
            name="elapsed_ms",
        )
    if "error" in data:
        error = data["error"]
        if error is not None and (not isinstance(error, str) or not error):
            raise PilotGuardError("result error must be null or a non-empty string")
        if accepted and error is not None:
            raise PilotGuardError("accepted result must not contain an error")
    return command_id, accepted


class PilotController:
    """Serialize HA feedback and panel pushes without feedback loops."""

    def __init__(
        self,
        *,
        publisher: Publisher,
        state_sink: StateSink,
        command_id_factory: Callable[[], str] = lambda: str(uuid4()),
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
        monotonic_ms: Callable[[], int] = lambda: time.monotonic_ns() // 1_000_000,
    ) -> None:
        self._publisher = publisher
        self._state_sink = state_sink
        self._command_id_factory = command_id_factory
        self._clock_ms = clock_ms
        self._monotonic_ms = monotonic_ms
        self._entity_stable_id = stable_id(TARGET_ENTITY_ID)
        self._observed_sequence: int | None = None
        self._observed_generated_at_ms: int | None = None
        self._observed_state: DecodedState | None = None
        self._authoritative_available = False
        self._awaiting_transport_state = True
        self._seen_pushes: set[tuple[str, int]] = set()
        self._pending: dict[str, tuple[tuple[str, int], int]] = {}
        self._queued_push: tuple[str, int] | None = None
        self._lock = asyncio.Lock()
        self._state_ready = asyncio.Event()

    @property
    def observed_sequence(self) -> int | None:
        return self._observed_sequence

    @property
    def authority_available(self) -> bool:
        """Whether the latest transport-fenced HA state is available."""

        return self._authoritative_available

    def pending_command_age_ms(self, *, now_ms: int | None = None) -> int | None:
        """Return the sole in-flight command age for confirmation watchdogs."""

        if not self._pending:
            return None
        current = self._monotonic_ms() if now_ms is None else now_ms
        current = _bounded_integer(current, minimum=0, maximum=2**63 - 1, name="current time")
        issued_at_ms = next(iter(self._pending.values()))[1]
        return max(0, current - issued_at_ms)

    def accepts_result_topic(self, topic: str) -> bool:
        """Return whether a wildcard result topic belongs to our in-flight ID."""

        command_id = topic.rsplit("/", maxsplit=1)[-1]
        try:
            canonical = _canonical_uuid(command_id, "command_id")
        except PilotGuardError:
            return False
        return topic == result_topic(canonical) and canonical in self._pending

    async def wait_for_state(self, timeout_s: float) -> None:
        try:
            await asyncio.wait_for(self._state_ready.wait(), timeout_s)
        except asyncio.TimeoutError:
            raise PilotGuardError("no authoritative HA state arrived before timeout") from None

    async def fence_transport(self) -> None:
        """Fence commands until the MQTT session replays authoritative state."""

        async with self._lock:
            self._authoritative_available = False
            self._awaiting_transport_state = True
            self._state_ready.clear()
            self._seen_pushes.clear()
            self._pending.clear()
            self._queued_push = None
            await self._state_sink.update_variables({"on": 0})

    @staticmethod
    def _native_values(state: DecodedState) -> dict[str, object]:
        values: dict[str, object] = {"on": state.on if state.available else 0}
        if state.available and state.intensity is not None:
            values["intensity"] = state.intensity
        return values

    async def reapply_authoritative_state(self) -> None:
        """Overwrite any optimistic framework value with the last HA state."""

        async with self._lock:
            if self._observed_state is None or not self._authoritative_available:
                raise PilotGuardError("available HA state is required for native restoration")
            await self._state_sink.update_variables(self._native_values(self._observed_state))

    async def handle_state_message(self, topic: str, payload: str, retained: bool) -> bool:
        """Reflect HA state internally; never publish an MQTT command."""

        if topic != state_topic(self._entity_stable_id):
            raise PilotGuardError("state topic does not match the pilot entity")
        state = decode_state_payload(payload, stable_id=self._entity_stable_id)
        async with self._lock:
            if self._awaiting_transport_state and not retained:
                raise PilotGuardError("initial HA state authority must be retained")
            if self._observed_sequence is not None:
                assert self._observed_generated_at_ms is not None
                assert self._observed_state is not None
                if state.generated_at_ms < self._observed_generated_at_ms:
                    raise PilotGuardError("state publication predates the observed HA epoch")
                if (
                    state.generated_at_ms == self._observed_generated_at_ms
                    and state.sequence < self._observed_sequence
                ):
                    raise PilotGuardError("state sequence regressed outside a newer HA epoch")
                if (
                    state.generated_at_ms == self._observed_generated_at_ms
                    and state.sequence == self._observed_sequence
                    and state != self._observed_state
                ):
                    raise PilotGuardError("state payload conflicts at the observed HA coordinate")
                if (
                    state.generated_at_ms == self._observed_generated_at_ms
                    and state.sequence == self._observed_sequence
                    and not self._awaiting_transport_state
                ):
                    return False

            await self._state_sink.update_variables(self._native_values(state))
            self._observed_sequence = state.sequence
            self._observed_generated_at_ms = state.generated_at_ms
            self._observed_state = state
            self._authoritative_available = state.available
            self._awaiting_transport_state = False
            if state.available:
                self._state_ready.set()
                queued: tuple[str, int] | None = None
                if self._pending:
                    pending_signature, _ = next(iter(self._pending.values()))
                    if self._signature_matches_state(pending_signature, state):
                        self._pending.clear()
                        self._seen_pushes.clear()
                        queued = self._queued_push
                        self._queued_push = None
                else:
                    self._seen_pushes.clear()
                if queued is not None and not self._signature_matches_state(queued, state):
                    await self._publish_locked(queued)
            else:
                self._state_ready.clear()
                self._seen_pushes.clear()
                self._pending.clear()
                self._queued_push = None
            return True

    async def handle_panel_push(self, variable: str, value: object) -> bool:
        """Publish one deduplicated, non-retained command after HA state exists."""

        async with self._lock:
            if self._observed_sequence is None or not self._authoritative_available:
                raise PilotGuardError(
                    "available authoritative HA state is required before a panel command"
                )
            normalized = _normalize_framework_push(variable, value)
            signature = (variable, normalized)
            if self._pending:
                self._queued_push = signature
                return False
            if signature in self._seen_pushes:
                return False
            assert self._observed_state is not None
            if self._signature_matches_state(signature, self._observed_state):
                if variable == "intensity" and self._observed_state.intensity != normalized:
                    await self._state_sink.update_variables(
                        self._native_values(self._observed_state)
                    )
                return False
            await self._publish_locked(signature)
            return True

    @staticmethod
    def _signature_matches_state(signature: tuple[str, int], state: DecodedState) -> bool:
        if not state.available:
            return False
        variable, normalized = signature
        if variable == "on":
            return state.on == normalized
        return (
            state.on == 1
            and state.intensity is not None
            and intensity_to_brightness(state.intensity) == intensity_to_brightness(normalized)
        )

    async def _publish_locked(self, signature: tuple[str, int]) -> None:
        """Publish while the controller lock establishes one-command flight."""

        assert self._observed_sequence is not None
        variable, normalized = signature
        command_id = self._command_id_factory()
        issued_at_ms = self._clock_ms()
        issued_at_monotonic_ms = self._monotonic_ms()
        command = build_command_payload(
            variable=variable,
            value=normalized,
            observed_sequence=self._observed_sequence,
            command_id=command_id,
            issued_at_ms=issued_at_ms,
        )
        self._seen_pushes.add(signature)
        self._pending[command_id] = (signature, issued_at_monotonic_ms)
        try:
            await self._publisher.publish(
                command_topic(self._entity_stable_id),
                encode_json(command),
                retain=False,
            )
        except BaseException:
            self._pending.pop(command_id, None)
            self._seen_pushes.discard(signature)
            raise

    async def handle_result_message(self, topic: str, payload: str, retained: bool) -> bool:
        """Abort and restore cached HA state on rejection; acceptance is not state."""

        if retained:
            raise PilotGuardError("retained command results are not allowed")
        command_id, accepted = _decode_result(payload)
        if topic != result_topic(command_id):
            raise PilotGuardError("result topic does not match command_id")
        async with self._lock:
            pending = self._pending.get(command_id)
            if pending is None:
                return False
            signature, _ = pending
            if not accepted:
                self._pending.pop(command_id, None)
                self._seen_pushes.discard(signature)
                self._queued_push = None
                if self._observed_state is not None and self._authoritative_available:
                    await self._state_sink.update_variables(
                        self._native_values(self._observed_state)
                    )
                raise PilotGuardError("HA rejected the pilot command")
            return True


class PilotLifecycle:
    """Own one partial-start-safe registration and its persistent teardown."""

    def __init__(
        self,
        *,
        config: PilotConfig,
        host: VirtualLightHost,
        room_assignment_type: type,
        on_command: Callable[[str, object], Awaitable[bool]],
        probe_factory: Callable[[], Awaitable[ScopedPeripheralProbe]],
        before_probes: Callable[[], Awaitable[None]] | None = None,
        initial_values: Mapping[str, object] | None = None,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        absence_interval_s: float = 30.0,
        operation_timeout_s: float = 10.0,
    ) -> None:
        if absence_interval_s < 0:
            raise ValueError("absence interval must be non-negative")
        if operation_timeout_s <= 0:
            raise ValueError("operation timeout must be positive")
        self._host = host
        self._probe_factory = probe_factory
        self._before_probes = before_probes
        self._clock_ms = clock_ms
        self._sleep = sleep
        self._absence_interval_s = absence_interval_s
        self._operation_timeout_s = operation_timeout_s
        self._peripheral_id = peripheral_id_for_entity(TARGET_ENTITY_ID)
        self._variables = build_light_variables(
            config,
            room_assignment_type=room_assignment_type,
        )
        if initial_values is not None:
            for name, value in initial_values.items():
                if name not in {"on", "intensity"}:
                    raise PilotGuardError("initial native state is outside on/intensity")
                normalized = _normalize_framework_push(name, value)
                definition = self._variables[name]
                self._variables[name] = VariableDefinition(
                    definition.value_type,
                    definition.externally_settable,
                    normalized,
                )
        self._on_command = on_command
        self._start_attempted = False
        self._started = False
        self._delete_succeeded = False
        self._host_shutdown = False
        self._before_probes_complete = False
        self._cleaned = False

    async def start(self) -> None:
        if self._started:
            return
        if self._start_attempted:
            raise PilotGuardError("pilot start already failed; cleanup is required")
        self._start_attempted = True
        await _await_operation(
            self._host.start(
                peripheral_id=self._peripheral_id,
                virtual_device_id=BVD_DEVICE_ID,
                variables=self._variables,
                on_command=self._on_command,
            ),
            timeout_s=self._operation_timeout_s,
            description="native peripheral registration",
        )
        self._started = True

    async def _observe_absence(self) -> bool:
        probe = await _await_operation(
            self._probe_factory(),
            timeout_s=self._operation_timeout_s,
            description="scoped absence probe creation",
        )
        try:
            present = await _await_operation(
                probe.contains(BVD_DEVICE_ID, self._peripheral_id),
                timeout_s=self._operation_timeout_s,
                description="scoped pilot peripheral read",
            )
            return not present
        finally:
            await _await_operation(
                probe.shutdown(),
                timeout_s=self._operation_timeout_s,
                description="scoped absence probe shutdown",
            )

    async def cleanup(self) -> CleanupReport:
        """Delete, stop the extra host, then prove absence with fresh readers."""

        if self._cleaned:
            return CleanupReport(True, True, True)

        deletion_error: Exception | None = None
        if self._start_attempted and not self._delete_succeeded:
            deletion_time_ms = _bounded_integer(
                self._clock_ms(),
                minimum=0,
                maximum=2**63 - 1,
                name="deletion_time_ms",
            )
            for _attempt in range(2):
                try:
                    await _await_operation(
                        self._host.delete(self._peripheral_id, deletion_time_ms),
                        timeout_s=self._operation_timeout_s,
                        description="native peripheral deletion",
                    )
                except Exception as error:
                    deletion_error = error
                else:
                    self._delete_succeeded = True
                    deletion_error = None
                    break

        shutdown_error: Exception | None = None
        if not self._host_shutdown:
            for _attempt in range(2):
                try:
                    await _await_operation(
                        self._host.shutdown(),
                        timeout_s=self._operation_timeout_s,
                        description="native host shutdown",
                    )
                except Exception as error:
                    shutdown_error = error
                else:
                    self._host_shutdown = True
                    self._started = False
                    shutdown_error = None
                    break

        guard_error: Exception | None = None
        if not self._before_probes_complete:
            if self._before_probes is not None:
                for _attempt in range(2):
                    try:
                        await _await_operation(
                            self._before_probes(),
                            timeout_s=self._operation_timeout_s,
                            description="pre-proof guard shutdown",
                        )
                    except Exception as error:
                        guard_error = error
                    else:
                        guard_error = None
                        self._before_probes_complete = True
                        break
            else:
                self._before_probes_complete = True

        if deletion_error is not None:
            raise CleanupError("native peripheral deletion failed after two attempts") from (
                deletion_error
            )
        if shutdown_error is not None:
            raise CleanupError("native host shutdown failed after two attempts") from shutdown_error
        if guard_error is not None:
            raise CleanupError("guard shutdown failed after two attempts") from guard_error

        absent_first = await self._observe_absence()
        if not absent_first:
            raise CleanupError("pilot peripheral is still present after deletion")
        await _await_operation(
            self._sleep(self._absence_interval_s),
            timeout_s=self._absence_interval_s + self._operation_timeout_s,
            description="absence observation interval",
        )
        absent_second = await self._observe_absence()
        if not absent_second:
            raise CleanupError("pilot peripheral is still present after deletion")
        self._cleaned = True
        return CleanupReport(False, absent_first, absent_second)
