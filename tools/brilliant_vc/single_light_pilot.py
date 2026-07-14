"""Fail-closed one-light Virtual Control feasibility pilot.

The module is deliberately importable off-panel: Brilliant framework and MQTT
imports are deferred to live adapter methods.  It never accepts a Home
Assistant token and never writes a physical slider binding.  HA remains the
authority through the versioned MQTT control-plane contract.
"""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import os
import re
import secrets
import signal
import stat
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, cast
from uuid import UUID, uuid4

from tools.brilliant_vc.gates import GateLedger, GateName, GateStatus

SCHEMA_VERSION = 1
TOPOLOGY_SCHEMA_VERSION = 2
MAPPING_VERSION = 1
LIGHT_PERIPHERAL_TYPE = 27
DEVICE_CONFIG_PERIPHERAL_TYPE = 19
DEVICE_CONFIG_PERIPHERAL_ID = "device_config_peripheral"
PHYSICAL_BUS_SOCKET = "/var/run/brilliant/server_socket"
_VC_SOCKET_ROOTS = (Path("/run/brilliant-vc"), Path("/var/run/brilliant-vc"))
_VC_CONTROL_ROOTS = (
    Path("/run/brilliant-vc-control"),
    Path("/var/run/brilliant-vc-control"),
)
_TOPIC_PREFIX = "brilliant/ha-control/v1"
_ID = re.compile(r"[0-9a-f]{32}")
_LINK_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}")
_RESERVED_DEVICE_IDS = frozenset(
    {"brilliant_virtual_device", "configuration_virtual_device", "ble_mesh"}
)
_SHARED_CONFIGURATION_IDS = frozenset({"brilliant_virtual_device_configuration"})
# PeripheralType values whose firmware enum name ends in CONFIGURATION.  The
# list is intentionally pinned to v26.06.03.1 and used only to classify
# peripherals already scoped to the VC's own Device record.
_CONFIGURATION_PERIPHERAL_TYPES = frozenset(
    {
        14,
        16,
        17,
        18,
        19,
        20,
        21,
        23,
        24,
        25,
        26,
        31,
        32,
        34,
        35,
        36,
        38,
        39,
        41,
        42,
        48,
        52,
        54,
        57,
        61,
        63,
        64,
        65,
        67,
        68,
        69,
        72,
        73,
        76,
        77,
        78,
        81,
        85,
        87,
        88,
        90,
        91,
        92,
        96,
        100,
        103,
        104,
        105,
    }
)
_MAX_PRIVATE_FILE_BYTES = 64 * 1024
_OPERATION_TIMEOUT_S = 10.0
_SHUTDOWN_RESERVE_S = 120.0
_RECONNECT_BACKOFF_S = 1.0


class PilotGuardError(ValueError):
    """Raised before a pilot can cross an unproven or unsafe boundary."""


class CleanupError(RuntimeError):
    """Raised when two absence observations cannot prove cleanup."""


class PilotLease:
    """One process-wide live-pilot lease held from preflight through cleanup."""

    _NAME = "single-light-pilot.lock"

    def __init__(self, descriptor: int) -> None:
        self._descriptor: int | None = descriptor

    @classmethod
    def acquire(
        cls,
        runtime_dir: Path,
        *,
        required_uid: int,
        allowed_roots: Sequence[Path] = _VC_CONTROL_ROOTS,
    ) -> PilotLease:
        try:
            parent = runtime_dir.lstat()
        except FileNotFoundError:
            raise PilotGuardError("pilot control directory does not exist") from None
        if stat.S_ISLNK(parent.st_mode) or not stat.S_ISDIR(parent.st_mode):
            raise PilotGuardError("pilot control directory must be real")
        if parent.st_uid != required_uid or stat.S_IMODE(parent.st_mode) != 0o700:
            raise PilotGuardError("pilot control directory must be owner-only mode 0700")
        resolved_runtime = runtime_dir.resolve(strict=True)
        if resolved_runtime not in {root.resolve(strict=False) for root in allowed_roots}:
            raise PilotGuardError("pilot lease directory is outside the allowed control roots")
        path = resolved_runtime / cls._NAME
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
        try:
            descriptor = os.open(path, flags, 0o600)
        except OSError as error:
            raise PilotGuardError("live pilot lease must be a regular non-symlink file") from error
        try:
            opened = os.fstat(descriptor)
            if not stat.S_ISREG(opened.st_mode):
                raise PilotGuardError("live pilot lease must be a regular file")
            if opened.st_uid != required_uid or stat.S_IMODE(opened.st_mode) != 0o600:
                raise PilotGuardError("live pilot lease must be owner-only mode 0600")
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise PilotGuardError("another single-light pilot is already active") from None
            return cls(descriptor)
        except BaseException:
            os.close(descriptor)
            raise

    def release(self) -> None:
        descriptor = self._descriptor
        if descriptor is None:
            return
        self._descriptor = None
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


@dataclass(frozen=True, slots=True)
class PeripheralRecord:
    """Minimal, already-scoped home-graph fact used by preflight."""

    owner_device_id: str
    peripheral_id: str
    role: str
    peripheral_type: int

    def __post_init__(self) -> None:
        if not self.owner_device_id or not _LINK_ID.fullmatch(self.owner_device_id):
            raise PilotGuardError("peripheral owner_device_id is invalid")
        if not _LINK_ID.fullmatch(self.peripheral_id):
            raise PilotGuardError("peripheral_id is invalid")
        if self.role not in {"configuration", "other"}:
            raise PilotGuardError("peripheral role must be configuration or other")
        if type(self.peripheral_type) is not int or not 0 <= self.peripheral_type <= 255:
            raise PilotGuardError("peripheral_type must be an integer from 0 to 255")


@dataclass(frozen=True, slots=True)
class TopologySnapshot:
    """Scoped VC device and room facts obtained before registration."""

    owner_device_id: str
    device_type: int
    peripherals: tuple[PeripheralRecord, ...]
    room_ids: frozenset[str]

    @classmethod
    def from_payload(cls, payload: object) -> TopologySnapshot:
        if not isinstance(payload, dict):
            raise PilotGuardError("topology snapshot must be an object")
        data = cast(dict[str, object], payload)
        expected = {"schema_version", "owner_device_id", "device_type", "peripherals", "room_ids"}
        if set(data) != expected or data.get("schema_version") != TOPOLOGY_SCHEMA_VERSION:
            raise PilotGuardError("topology snapshot fields or schema version are invalid")
        raw_peripherals = data["peripherals"]
        raw_rooms = data["room_ids"]
        if not isinstance(raw_peripherals, list) or not isinstance(raw_rooms, list):
            raise PilotGuardError("topology peripherals and room_ids must be lists")
        peripherals: list[PeripheralRecord] = []
        for item in raw_peripherals:
            if not isinstance(item, dict) or set(item) != {
                "owner_device_id",
                "peripheral_id",
                "role",
                "peripheral_type",
            }:
                raise PilotGuardError("topology peripheral record is invalid")
            values = cast(dict[str, object], item)
            peripherals.append(
                PeripheralRecord(
                    owner_device_id=_required_str(values, "owner_device_id"),
                    peripheral_id=_required_str(values, "peripheral_id"),
                    role=_required_str(values, "role"),
                    peripheral_type=_bounded_integer(
                        values.get("peripheral_type"),
                        minimum=0,
                        maximum=255,
                        name="peripheral_type",
                    ),
                )
            )
        rooms: set[str] = set()
        for room in raw_rooms:
            if not isinstance(room, str) or not _LINK_ID.fullmatch(room):
                raise PilotGuardError("topology room ID is invalid")
            rooms.add(room)
        owner = _required_str(data, "owner_device_id")
        device_type = data["device_type"]
        if type(device_type) is not int:
            raise PilotGuardError("topology device_type must be an integer")
        return cls(owner, device_type, tuple(peripherals), frozenset(rooms))


@dataclass(frozen=True, slots=True)
class PilotConfig:
    """One immutable and bounded pilot request."""

    stable_id: str
    display_name: str
    room_id: str
    vc_device_id: str
    office_device_id: str
    vc_socket: str
    runtime_s: int

    def __post_init__(self) -> None:
        try:
            canonical_stable_id = str(UUID(self.stable_id))
        except (AttributeError, TypeError, ValueError) as error:
            raise PilotGuardError("stable_id must be a UUID") from error
        if canonical_stable_id != self.stable_id:
            raise PilotGuardError("stable_id must use canonical UUID form")
        _validate_device_id(self.office_device_id, "Office device ID")
        if self.vc_device_id in _RESERVED_DEVICE_IDS:
            raise PilotGuardError("VC device ID is reserved")
        _validate_device_id(self.vc_device_id, "VC device ID")
        if self.vc_device_id == self.office_device_id:
            raise PilotGuardError("VC device ID must not be the physical Office device ID")
        if not self.display_name or len(self.display_name) > 80:
            raise PilotGuardError("display_name must contain 1 to 80 characters")
        if any(ord(character) < 32 for character in self.display_name):
            raise PilotGuardError("display_name must not contain control characters")
        if not _LINK_ID.fullmatch(self.room_id):
            raise PilotGuardError("room_id is invalid")
        object.__setattr__(self, "vc_socket", _canonical_vc_socket(self.vc_socket))
        if type(self.runtime_s) is not int or not 180 <= self.runtime_s <= 1_800:
            raise PilotGuardError("runtime_s must be an integer from 180 to at most 1800 seconds")


@dataclass(frozen=True, slots=True)
class VariableDefinition:
    """Framework-independent representation of one typed VariableSpec."""

    value_type: type
    externally_settable: bool
    default_value: object


@dataclass(frozen=True, slots=True)
class DecodedState:
    """One validated authoritative HA light state."""

    sequence: int
    generated_at_ms: int
    available: bool
    on: int | None
    intensity: int | None


@dataclass(frozen=True, slots=True)
class CleanupReport:
    """Secret-free result of local deletion and two absence reads."""

    already_clean: bool
    absent_first: bool
    absent_second: bool


class Publisher(Protocol):
    async def publish(self, topic: str, payload: str, retain: bool = False) -> None: ...


class StateSink(Protocol):
    async def update_variables(self, values: Mapping[str, object]) -> None: ...


class PeripheralHostRoute(StateSink, Protocol):
    async def start(
        self,
        *,
        peripheral_id: str,
        virtual_device_id: str,
        variables: Mapping[str, VariableDefinition],
        on_command: Callable[[str, object], Awaitable[bool]],
    ) -> None: ...

    async def delete(self, peripheral_id: str) -> None: ...

    async def contains(self, peripheral_id: str) -> bool: ...

    async def shutdown(self) -> None: ...


def peripheral_id_for(stable_id: str) -> str:
    """Derive the immutable native peripheral ID from the HA stable UUID."""

    try:
        parsed = UUID(stable_id)
    except (AttributeError, TypeError, ValueError) as error:
        raise PilotGuardError("stable_id must be a UUID") from error
    return f"ha_vc_{parsed.hex}"


def state_topic(stable_id: str) -> str:
    """Return the exact retained HA-authoritative state topic."""

    return f"{_TOPIC_PREFIX}/state/{_canonical_uuid(stable_id, 'stable_id')}"


def command_topic(stable_id: str) -> str:
    """Return the exact non-retained panel-originated command topic."""

    return f"{_TOPIC_PREFIX}/command/{_canonical_uuid(stable_id, 'stable_id')}"


def _canonical_vc_socket(
    value: str,
    *,
    allowed_roots: Sequence[Path] = _VC_SOCKET_ROOTS,
    physical_socket: Path = Path(PHYSICAL_BUS_SOCKET),
) -> str:
    path = Path(value)
    if not path.is_absolute():
        raise PilotGuardError("VC message bus socket must be absolute")
    resolved = path.resolve(strict=False)
    physical = physical_socket.resolve(strict=False)
    if resolved == physical:
        raise PilotGuardError("refusing to use the physical Control bus socket")
    for root in allowed_roots:
        resolved_root = root.resolve(strict=False)
        try:
            resolved.relative_to(resolved_root)
        except ValueError:
            continue
        if resolved != resolved_root:
            return str(resolved)
    raise PilotGuardError("VC message bus socket must resolve inside an isolated VC runtime")


def brightness_to_intensity(value: object) -> int:
    """Scale HA 0–255 to Brilliant 0–1000, rounding exact halves upward."""

    brightness = _bounded_integer(value, minimum=0, maximum=255, name="brightness")
    return (brightness * 2_000 + 255) // 510


def intensity_to_brightness(value: object) -> int:
    """Scale Brilliant 0–1000 to HA 0–255, rounding exact halves upward."""

    intensity = _bounded_integer(value, minimum=0, maximum=1_000, name="intensity")
    return (intensity * 510 + 1_000) // 2_000


def discover_configuration_peripheral(
    vc_device_id: str, peripherals: Sequence[PeripheralRecord]
) -> str:
    """Select the stock type-19 Device Configuration owned by this VC."""

    candidates = [
        item
        for item in peripherals
        if item.role == "configuration"
        and item.owner_device_id == vc_device_id
        and item.peripheral_type == DEVICE_CONFIG_PERIPHERAL_TYPE
    ]
    if not candidates:
        raise PilotGuardError("VC has no own configuration peripheral of type 19")
    if len(candidates) != 1:
        raise PilotGuardError("VC must have exactly one own type-19 device configuration")
    peripheral_id = candidates[0].peripheral_id
    if peripheral_id != DEVICE_CONFIG_PERIPHERAL_ID:
        raise PilotGuardError("VC type-19 configuration has the wrong peripheral ID")
    if peripheral_id in _SHARED_CONFIGURATION_IDS:
        raise PilotGuardError("VC configuration points at a protected shared peripheral")
    return peripheral_id


def validate_topology(config: PilotConfig, topology: TopologySnapshot) -> str:
    """Prove identity, type, room, and own-configuration linkage before a write."""

    if topology.owner_device_id != config.vc_device_id:
        raise PilotGuardError("topology owner does not match the VC identity")
    if topology.device_type != 6:
        raise PilotGuardError("topology owner must be DeviceType 6 VIRTUAL_CONTROL")
    if config.room_id not in topology.room_ids:
        raise PilotGuardError("requested room is absent from the scoped room catalog")
    return discover_configuration_peripheral(config.vc_device_id, topology.peripherals)


def build_variable_definitions(
    config: PilotConfig,
    *,
    configuration_peripheral_id: str,
    room_assignment_type: type,
) -> dict[str, VariableDefinition]:
    """Build the exact typed LIGHT schema consumed by native UI/slider paths."""

    if not _LINK_ID.fullmatch(configuration_peripheral_id):
        raise PilotGuardError("configuration peripheral ID is invalid")
    room_assignment = room_assignment_type(room_ids=[config.room_id])
    return {
        "on": VariableDefinition(int, True, 0),
        "intensity": VariableDefinition(int, True, 500),
        "dimmable": VariableDefinition(int, False, 1),
        "max_intensity_value": VariableDefinition(int, False, 1_000),
        "minimum_dim_level": VariableDefinition(int, True, 100),
        "maximum_dim_level": VariableDefinition(int, True, 1_000),
        "display_name": VariableDefinition(str, True, config.display_name),
        "room_assignment": VariableDefinition(room_assignment_type, True, room_assignment),
        "mode_transition_settings": VariableDefinition(str, True, "{}"),
        "configuration_peripheral_id": VariableDefinition(str, False, configuration_peripheral_id),
    }


def decode_state_payload(payload: str, *, stable_id: str) -> DecodedState:
    """Validate the allowlisted v1 HA light state used by the native surface."""

    try:
        decoded: object = json.loads(payload)
    except (json.JSONDecodeError, TypeError) as error:
        raise PilotGuardError("state payload must be valid JSON") from error
    if not isinstance(decoded, dict):
        raise PilotGuardError("state payload must be an object")
    data = cast(dict[str, object], decoded)
    required = {
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
    if set(data) != required:
        raise PilotGuardError("state payload fields do not match the v1 contract")
    if data["schema_version"] != SCHEMA_VERSION or data["mapping_version"] != MAPPING_VERSION:
        raise PilotGuardError("state payload version is unsupported")
    if data["stable_id"] != stable_id:
        raise PilotGuardError("state payload stable_id does not match the route")
    entity_id = data["entity_id"]
    if not isinstance(entity_id, str) or not entity_id.startswith("light."):
        raise PilotGuardError("state payload must describe a light entity")
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
    intensity = None if brightness is None else brightness_to_intensity(brightness)
    return DecodedState(
        sequence=sequence,
        generated_at_ms=generated_at_ms,
        available=available,
        on=(1 if state == "on" else 0) if available else None,
        intensity=intensity,
    )


def build_command_payload(
    *,
    stable_id: str,
    variable: str,
    value: object,
    observed_sequence: int,
    command_id: str,
    issued_at_ms: int,
) -> dict[str, object]:
    """Map one native LIGHT push to the existing HA control-plane contract."""

    stable_id = _canonical_uuid(stable_id, "stable_id")
    command_id = _canonical_uuid(command_id, "command_id")
    observed_sequence = _bounded_integer(
        observed_sequence, minimum=0, maximum=2**63 - 1, name="observed_sequence"
    )
    issued_at_ms = _bounded_integer(issued_at_ms, minimum=0, maximum=2**63 - 1, name="issued_at_ms")
    if variable == "on":
        on = _bounded_integer(value, minimum=0, maximum=1, name="on")
        kind = "turn_on" if on else "turn_off"
        command_value: object = None
    elif variable == "intensity":
        kind = "set_brightness"
        command_value = intensity_to_brightness(value)
    else:
        raise PilotGuardError("only on and intensity may become HA commands")
    return {
        "schema_version": SCHEMA_VERSION,
        "mapping_version": MAPPING_VERSION,
        "command_id": command_id,
        "stable_id": stable_id,
        "kind": kind,
        "value": command_value,
        "observed_sequence": observed_sequence,
        "issued_at_ms": issued_at_ms,
    }


class PilotController:
    """Serialize state feedback and panel commands without a feedback loop."""

    def __init__(
        self,
        *,
        config: PilotConfig,
        publisher: Publisher,
        state_sink: StateSink,
        command_id_factory: Callable[[], str] = lambda: str(uuid4()),
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self._config = config
        self._publisher = publisher
        self._state_sink = state_sink
        self._command_id_factory = command_id_factory
        self._clock_ms = clock_ms
        self._observed_sequence: int | None = None
        self._observed_generated_at_ms: int | None = None
        self._authoritative_available = False
        self._awaiting_transport_state = True
        self._seen_pushes: set[tuple[str, int]] = set()
        self._lock = asyncio.Lock()
        self._state_ready = asyncio.Event()

    @property
    def observed_sequence(self) -> int | None:
        return self._observed_sequence

    async def wait_for_state(self, timeout_s: float) -> None:
        try:
            await asyncio.wait_for(self._state_ready.wait(), timeout_s)
        except asyncio.TimeoutError:
            raise PilotGuardError("no authoritative HA state arrived before timeout") from None

    async def fence_transport(self) -> None:
        """Fence commands until this MQTT session replays authoritative state."""

        async with self._lock:
            self._authoritative_available = False
            self._awaiting_transport_state = True
            self._state_ready.clear()
            self._seen_pushes.clear()
            await self._state_sink.update_variables({"on": 0})

    async def handle_state_message(self, topic: str, payload: str, retained: bool) -> bool:
        """Apply a newer authoritative state internally; never publish a command."""

        del retained  # Both retained replay and live retained publications are authoritative.
        if topic != state_topic(self._config.stable_id):
            raise PilotGuardError("state topic does not match the pilot entity")
        state = decode_state_payload(payload, stable_id=self._config.stable_id)
        async with self._lock:
            if self._observed_sequence is not None:
                assert self._observed_generated_at_ms is not None
                if state.generated_at_ms < self._observed_generated_at_ms:
                    raise PilotGuardError("state publication predates the observed HA epoch")
                if state.sequence < self._observed_sequence:
                    if state.generated_at_ms == self._observed_generated_at_ms:
                        raise PilotGuardError("state sequence regressed outside a newer HA epoch")
                if state.sequence == self._observed_sequence:
                    if state.generated_at_ms == self._observed_generated_at_ms:
                        if self._awaiting_transport_state:
                            self._authoritative_available = state.available
                            self._awaiting_transport_state = False
                            if state.available:
                                self._state_ready.set()
                        return False
            values: dict[str, object] = {"on": state.on if state.available else 0}
            if state.available and state.intensity is not None:
                values["intensity"] = state.intensity
            await self._state_sink.update_variables(values)
            self._observed_sequence = state.sequence
            self._observed_generated_at_ms = state.generated_at_ms
            self._authoritative_available = state.available
            self._awaiting_transport_state = False
            self._seen_pushes.clear()
            if state.available:
                self._state_ready.set()
            else:
                self._state_ready.clear()
            return True

    async def handle_panel_push(self, variable: str, value: object) -> bool:
        """Publish one deduplicated, non-retained command after HA state exists."""

        async with self._lock:
            if self._observed_sequence is None or not self._authoritative_available:
                raise PilotGuardError(
                    "available authoritative HA state is required before a panel command"
                )
            signature = _normalized_push_signature(variable, value)
            if signature in self._seen_pushes:
                return False
            command = build_command_payload(
                stable_id=self._config.stable_id,
                variable=variable,
                value=value,
                observed_sequence=self._observed_sequence,
                command_id=self._command_id_factory(),
                issued_at_ms=self._clock_ms(),
            )
            encoded = json.dumps(command, separators=(",", ":"), sort_keys=True)
            await self._publisher.publish(
                command_topic(self._config.stable_id), encoded, retain=False
            )
            self._seen_pushes.add(signature)
            return True


def _normalized_push_signature(variable: str, value: object) -> tuple[str, int]:
    if variable == "on":
        return variable, _bounded_integer(value, minimum=0, maximum=1, name="on")
    if variable == "intensity":
        return variable, _bounded_integer(value, minimum=0, maximum=1_000, name="intensity")
    raise PilotGuardError("only on and intensity may become HA commands")


def _normalize_framework_push(variable: str, value: object) -> int:
    """Normalize the closed bus callback boundary without accepting loose numbers."""

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


async def _await_operation(operation: Awaitable[Any], *, timeout_s: float, description: str) -> Any:
    try:
        return await asyncio.wait_for(operation, timeout=timeout_s)
    except asyncio.TimeoutError:
        raise PilotGuardError(f"{description} exceeded its bounded timeout") from None


class PilotLifecycle:
    """Exactly one host/registration with bounded, idempotent deletion."""

    def __init__(
        self,
        *,
        config: PilotConfig,
        topology: TopologySnapshot,
        host: PeripheralHostRoute,
        room_assignment_type: type,
        on_command: Callable[[str, object], Awaitable[bool]],
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        absence_interval_s: float = 30.0,
        operation_timeout_s: float = _OPERATION_TIMEOUT_S,
    ) -> None:
        if absence_interval_s < 0:
            raise ValueError("absence interval must be non-negative")
        if operation_timeout_s <= 0:
            raise ValueError("operation timeout must be positive")
        self._config = config
        self._host = host
        self._on_command = on_command
        self._sleep = sleep
        self._absence_interval_s = absence_interval_s
        self._operation_timeout_s = operation_timeout_s
        configuration_id = validate_topology(config, topology)
        self._variables = build_variable_definitions(
            config,
            configuration_peripheral_id=configuration_id,
            room_assignment_type=room_assignment_type,
        )
        self._peripheral_id = peripheral_id_for(config.stable_id)
        self._start_attempted = False
        self._started = False
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
                virtual_device_id=self._config.vc_device_id,
                variables=self._variables,
                on_command=self._on_command,
            ),
            timeout_s=self._operation_timeout_s,
            description="native peripheral registration",
        )
        self._started = True

    async def cleanup(self) -> CleanupReport:
        if self._cleaned:
            return CleanupReport(already_clean=True, absent_first=True, absent_second=True)
        failure: str | None = None
        absent_first = False
        absent_second = False
        try:
            if self._start_attempted:
                await _await_operation(
                    self._host.delete(self._peripheral_id),
                    timeout_s=self._operation_timeout_s,
                    description="native peripheral deletion",
                )
            absent_first = not await _await_operation(
                self._host.contains(self._peripheral_id),
                timeout_s=self._operation_timeout_s,
                description="first scoped absence read",
            )
            await _await_operation(
                self._sleep(self._absence_interval_s),
                timeout_s=self._absence_interval_s + self._operation_timeout_s,
                description="absence observation interval",
            )
            absent_second = not await _await_operation(
                self._host.contains(self._peripheral_id),
                timeout_s=self._operation_timeout_s,
                description="second scoped absence read",
            )
            if not absent_first or not absent_second:
                failure = "pilot peripheral is still present after deletion"
        finally:
            await _await_operation(
                self._host.shutdown(),
                timeout_s=self._operation_timeout_s,
                description="native host shutdown",
            )
            self._started = False
        if failure is not None:
            raise CleanupError(failure)
        self._cleaned = True
        return CleanupReport(
            already_clean=False,
            absent_first=absent_first,
            absent_second=absent_second,
        )


class LivePeripheralHost:
    """One deferred-import PeripheralHost attached only to the dedicated VC bus."""

    def __init__(self, *, loop: asyncio.AbstractEventLoop, socket_path: str) -> None:
        self._loop = loop
        self._socket_path = _canonical_vc_socket(socket_path)
        self._host: Any = None
        self._instance: Any = None
        self._peripheral_id: str | None = None
        self._virtual_device_id: str | None = None

    def _point_socket_flag_at_vc(self) -> None:
        import gflags

        flags = gflags.FLAGS
        try:
            _ = flags.message_bus_server_socket_path
        except gflags.UnparsedFlagAccessError:
            flags([""])
        flags.message_bus_server_socket_path = self._socket_path

    async def start(
        self,
        *,
        peripheral_id: str,
        virtual_device_id: str,
        variables: Mapping[str, VariableDefinition],
        on_command: Callable[[str, object], Awaitable[bool]],
    ) -> None:
        if self._host is not None:
            return
        from lib.startables.startable import HostedStartableSpec
        from peripherals.lib.peripheral_service.peripheral import Peripheral, VariableSpec
        from peripherals.lib.peripheral_service.peripheral_host import (
            PeripheralConfig,
            PeripheralHost,
        )

        self._point_socket_flag_at_vc()
        adapter = self

        def push_factory(name: str) -> Callable[[object], Awaitable[None]]:
            async def push(value: object) -> None:
                if name not in {"on", "intensity"}:
                    raise PilotGuardError("metadata changes are outside this bounded pilot")
                await on_command(name, _normalize_framework_push(name, value))

            return push

        def build_variables() -> dict[str, Any]:
            return {
                name: VariableSpec(
                    definition.value_type,
                    definition.externally_settable,
                    default_value=definition.default_value,
                    push_func=push_factory(name) if definition.externally_settable else None,
                )
                for name, definition in variables.items()
            }

        class PilotLightPeripheral(Peripheral):  # type: ignore[misc]
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                super().__init__(*args, **kwargs)
                adapter._instance = self

            @property
            def name(self) -> str:
                return peripheral_id

            @property
            def peripheral_type(self) -> int:
                return LIGHT_PERIPHERAL_TYPE

            def _my_variables(self) -> dict[str, Any]:
                return build_variables()

        config = PeripheralConfig(
            peripheral_id,
            PilotLightPeripheral,
            virtual_device_id=virtual_device_id,
        )
        host = PeripheralHost(
            loop=self._loop,
            startable_id=peripheral_id,
            startables_to_host=[HostedStartableSpec(peripheral_id, config, {})],
            parallel_registration_limit=1,
            raise_errors_for_lost_user_configured_data=False,
            message_bus_address_override=None,
        )
        try:
            self._peripheral_id = peripheral_id
            self._virtual_device_id = virtual_device_id
            self._host = host
            await host.start()
        except BaseException:
            # Keep the host/identity references intact. PilotLifecycle treats a
            # start attempt as potentially registered and will issue explicit
            # deletion plus two scoped absence reads before shutdown.
            raise

    async def update_variables(self, values: Mapping[str, object]) -> None:
        if self._instance is None:
            return
        from peripherals.lib.peripheral_service.peripheral import Peripheral

        update = Peripheral.__dict__["_set_value_internal"]
        for name, value in values.items():
            result = update(self._instance, name, value, notify=True)
            if hasattr(result, "__await__"):
                await _await_operation(
                    result,
                    timeout_s=_OPERATION_TIMEOUT_S,
                    description=f"native {name} state reflection",
                )

    async def delete(self, peripheral_id: str) -> None:
        if self._host is None:
            return
        from peripherals.lib.peripheral_service.conditional_peripheral_host import (
            ConditionalPeripheralHost,
        )

        delete_impl = ConditionalPeripheralHost.__dict__["delete_peripheral"]
        result = delete_impl(self._host, peripheral_id, time.time_ns() // 1_000_000)
        if hasattr(result, "__await__"):
            await _await_operation(
                result,
                timeout_s=_OPERATION_TIMEOUT_S,
                description="framework peripheral deletion",
            )
        self._instance = None

    async def contains(self, peripheral_id: str) -> bool:
        if self._virtual_device_id is None:
            return False
        return await _scoped_peripheral_exists(
            socket_path=self._socket_path,
            device_id=self._virtual_device_id,
            peripheral_id=peripheral_id,
            loop=self._loop,
        )

    async def shutdown(self) -> None:
        if self._host is None:
            return
        try:
            await _await_operation(
                self._host.shutdown(),
                timeout_s=_OPERATION_TIMEOUT_S,
                description="framework host shutdown",
            )
        finally:
            self._host = None
            self._instance = None
            self._peripheral_id = None
            self._virtual_device_id = None


async def _scoped_peripheral_exists(
    *,
    socket_path: str,
    device_id: str,
    peripheral_id: str,
    loop: asyncio.AbstractEventLoop,
) -> bool:
    """Read one exact VC-owned peripheral through a short-lived observer."""

    observer, processor = await _open_scoped_observer(
        socket_path=socket_path,
        loop=loop,
        name_prefix="brilliant_vc_absence",
    )
    try:
        if observer.get_owning_device_id() != device_id:
            raise PilotGuardError("dedicated VC bus owner does not match the VC identity")
        try:
            return (
                await _await_operation(
                    observer.get_peripheral(device_id, peripheral_id),
                    timeout_s=_OPERATION_TIMEOUT_S,
                    description="scoped peripheral read",
                )
                is not None
            )
        except KeyError:
            return False
    finally:
        await _shutdown_components(observer, processor)


async def _shutdown_components(*components: Any) -> None:
    for component in components:
        try:
            await asyncio.wait_for(component.shutdown(), timeout=_OPERATION_TIMEOUT_S)
        except (Exception, asyncio.CancelledError):
            pass


async def _open_scoped_observer(
    *,
    socket_path: str,
    loop: asyncio.AbstractEventLoop,
    name_prefix: str,
) -> tuple[Any, Any]:
    """Open one bounded read-only observer against the isolated VC socket."""

    import lib.protocol.message_bus_peer_service as mbps
    from lib.message_bus_api.observer_interface import RPCObserver
    from lib.protocol.processor import SinglePeerProcessor

    socket_path = _canonical_vc_socket(socket_path)
    observer = RPCObserver(loop)
    processor = SinglePeerProcessor(
        socket_path=socket_path,
        my_name=f"{name_prefix}-{secrets.token_hex(4)}",
        handler=mbps.PeripheralServer(observer),
        client_class=mbps.MessageBusClient,
        loop=loop,
    )
    try:
        await _await_operation(
            processor.start(),
            timeout_s=_OPERATION_TIMEOUT_S,
            description="scoped observer transport start",
        )
        deadline = loop.time() + 10.0
        while not processor.is_connected():
            if loop.time() >= deadline:
                raise TimeoutError("scoped VC observer did not connect")
            await asyncio.sleep(0.25)
        await _await_operation(
            observer.start(processor, None),
            timeout_s=_OPERATION_TIMEOUT_S,
            description="scoped observer start",
        )
        return observer, processor
    except BaseException:
        await _shutdown_components(observer, processor)
        raise


def _record_live_peripheral(
    owner_device_id: str,
    peripheral_id: object,
    peripheral: object,
) -> PeripheralRecord:
    """Normalize one scoped firmware record without coercing bad type metadata."""

    peripheral_type = _bounded_integer(
        getattr(peripheral, "peripheral_type", None),
        minimum=0,
        maximum=255,
        name="live peripheral_type",
    )
    return PeripheralRecord(
        owner_device_id=owner_device_id,
        peripheral_id=str(peripheral_id),
        role=("configuration" if peripheral_type in _CONFIGURATION_PERIPHERAL_TYPES else "other"),
        peripheral_type=peripheral_type,
    )


async def _probe_live_topology(config: PilotConfig) -> TopologySnapshot:
    """Discover the VC's own config candidate and room catalog with scoped reads."""

    from lib.serialization import deserialize
    from thrift_types.configuration.ttypes import Rooms

    loop = asyncio.get_running_loop()
    observer, processor = await _open_scoped_observer(
        socket_path=config.vc_socket,
        loop=loop,
        name_prefix="brilliant_vc_preflight",
    )
    try:
        if observer.get_owning_device_id() != config.vc_device_id:
            raise PilotGuardError("dedicated VC bus owner does not match the VC identity")
        device = await _await_operation(
            observer.get_device(config.vc_device_id),
            timeout_s=_OPERATION_TIMEOUT_S,
            description="scoped VC device read",
        )
        if device is None or getattr(device, "id", None) != config.vc_device_id:
            raise PilotGuardError("scoped VC device record is missing")
        raw_peripherals = getattr(device, "peripherals", None)
        if not isinstance(raw_peripherals, Mapping):
            raise PilotGuardError("scoped VC device has no peripheral map")
        peripherals = tuple(
            _record_live_peripheral(config.vc_device_id, peripheral_id, peripheral)
            for peripheral_id, peripheral in raw_peripherals.items()
        )

        home_configuration = await _await_operation(
            observer.get_peripheral("configuration_virtual_device", "home_configuration"),
            timeout_s=_OPERATION_TIMEOUT_S,
            description="scoped room catalog read",
        )
        variables = getattr(home_configuration, "variables", None)
        if not isinstance(variables, Mapping) or "rooms" not in variables:
            raise PilotGuardError("scoped home_configuration.rooms is missing")
        rooms_value = getattr(variables["rooms"], "value", None)
        if not isinstance(rooms_value, str):
            raise PilotGuardError("scoped room catalog value is invalid")
        decoded_rooms = deserialize(Rooms, rooms_value)
        room_map = getattr(decoded_rooms, "rooms", None)
        if not isinstance(room_map, Mapping):
            raise PilotGuardError("decoded room catalog is invalid")
        room_ids = frozenset(str(room_id) for room_id in room_map)
        return TopologySnapshot(
            owner_device_id=config.vc_device_id,
            device_type=_bounded_integer(
                getattr(device, "device_type", None),
                minimum=0,
                maximum=255,
                name="live device_type",
            ),
            peripherals=peripherals,
            room_ids=room_ids,
        )
    finally:
        await _shutdown_components(observer, processor)


def _require_matching_live_topology(
    expected: TopologySnapshot, observed: TopologySnapshot, config: PilotConfig
) -> None:
    """Reject a stale/operator-authored snapshot before registration."""

    expected_configuration = validate_topology(config, expected)
    observed_configuration = validate_topology(config, observed)
    if expected_configuration != observed_configuration:
        raise PilotGuardError("live VC configuration linkage changed after the snapshot")
    if expected.room_ids != observed.room_ids:
        raise PilotGuardError("live room catalog changed after the topology snapshot")

    def key(item: PeripheralRecord) -> tuple[str, str, str, int]:
        return item.owner_device_id, item.peripheral_id, item.role, item.peripheral_type

    if sorted(expected.peripherals, key=key) != sorted(observed.peripherals, key=key):
        raise PilotGuardError("live VC peripheral set changed after the topology snapshot")


class _LivePublisher:
    def __init__(self) -> None:
        self._client: Any = None

    def attach(self, client: Any) -> None:
        self._client = client

    def detach(self, client: Any) -> None:
        if self._client is client:
            self._client = None

    async def publish(self, topic: str, payload: str, retain: bool = False) -> None:
        if self._client is None:
            raise PilotGuardError("MQTT transport is disconnected")
        await self._client.publish(
            topic,
            payload=payload,
            retain=retain,
            timeout=_OPERATION_TIMEOUT_S,
        )


async def _finish_live_resources(
    reader: asyncio.Task[None] | None,
    lifecycle: PilotLifecycle,
    unsubscribe: Callable[[], Awaitable[None]],
    *,
    deadline: float | None = None,
) -> None:
    """Drain reader failures, then always delete, prove absence, and unsubscribe."""

    if reader is not None:
        reader.cancel()
        await asyncio.gather(reader, return_exceptions=True)
    try:
        if deadline is None:
            await lifecycle.cleanup()
        else:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise CleanupError("pilot cleanup deadline elapsed before deletion")
            cleanup = asyncio.create_task(lifecycle.cleanup())
            try:
                await asyncio.wait_for(asyncio.shield(cleanup), timeout=remaining)
            except asyncio.CancelledError as cancellation:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    cleanup.cancel()
                    await asyncio.gather(cleanup, return_exceptions=True)
                    raise CleanupError(
                        "pilot cleanup deadline elapsed during cancellation"
                    ) from cancellation
                try:
                    await asyncio.wait_for(asyncio.shield(cleanup), timeout=remaining)
                except asyncio.TimeoutError:
                    cleanup.cancel()
                    await asyncio.gather(cleanup, return_exceptions=True)
                    raise CleanupError(
                        "pilot cleanup exceeded the deadline during cancellation"
                    ) from None
                raise
            except asyncio.TimeoutError:
                cleanup.cancel()
                await asyncio.gather(cleanup, return_exceptions=True)
                raise CleanupError("pilot cleanup exceeded the total runtime deadline") from None
    finally:
        await unsubscribe()


class _TransportEnded(ConnectionError):
    pass


class _StateReplayTimeout(ConnectionError):
    pass


async def _wait_for_session_authority(
    *,
    controller: PilotController,
    reader: asyncio.Task[None],
    stop: asyncio.Event,
    deadline: float,
) -> bool:
    remaining = deadline - asyncio.get_running_loop().time()
    if remaining <= 0:
        raise _StateReplayTimeout("no time remains for authoritative MQTT state")
    state_waiter = asyncio.create_task(controller.wait_for_state(min(15.0, remaining)))
    stop_waiter = asyncio.create_task(stop.wait())
    try:
        done, _ = await asyncio.wait(
            {reader, state_waiter, stop_waiter},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if reader in done:
            reader.result()
            raise _TransportEnded("MQTT state stream ended")
        if stop_waiter in done:
            return False
        try:
            state_waiter.result()
        except PilotGuardError as error:
            raise _StateReplayTimeout("MQTT session did not replay authoritative state") from error
        return True
    finally:
        for task in (state_waiter, stop_waiter):
            task.cancel()
        await asyncio.gather(state_waiter, stop_waiter, return_exceptions=True)


async def _wait_for_stop_or_disconnect(
    *, reader: asyncio.Task[None], stop: asyncio.Event, deadline: float
) -> None:
    remaining = deadline - asyncio.get_running_loop().time()
    if remaining <= 0:
        return
    stop_waiter = asyncio.create_task(stop.wait())
    try:
        done, _ = await asyncio.wait(
            {reader, stop_waiter},
            timeout=remaining,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if reader in done:
            reader.result()
            raise _TransportEnded("MQTT state stream ended")
    finally:
        stop_waiter.cancel()
        await asyncio.gather(stop_waiter, return_exceptions=True)


async def _wait_reconnect_backoff(
    *, stop: asyncio.Event, deadline: float, backoff_s: float = _RECONNECT_BACKOFF_S
) -> None:
    remaining = deadline - asyncio.get_running_loop().time()
    if remaining <= 0 or stop.is_set():
        return
    try:
        await asyncio.wait_for(stop.wait(), timeout=min(backoff_s, remaining))
    except asyncio.TimeoutError:
        pass


async def _run_reconnecting_transport(
    *,
    config: PilotConfig,
    lifecycle: PilotLifecycle,
    controller: PilotController,
    publisher: _LivePublisher,
    client_factory: Callable[[], Any],
    retryable_errors: tuple[type[BaseException], ...],
    stop: asyncio.Event,
    deadline: float,
    reconnect_backoff_s: float = _RECONNECT_BACKOFF_S,
) -> None:
    """Keep one native host while MQTT reconnects within the active deadline."""

    host_started = False
    await controller.fence_transport()
    while not stop.is_set() and asyncio.get_running_loop().time() < deadline:
        client = client_factory()
        reader: asyncio.Task[None] | None = None
        subscribed = False
        try:
            async with client:
                publisher.attach(client)
                await controller.fence_transport()
                try:
                    remaining = deadline - asyncio.get_running_loop().time()
                    if remaining <= 0:
                        return
                    await client.subscribe(
                        state_topic(config.stable_id),
                        timeout=min(_OPERATION_TIMEOUT_S, remaining),
                    )
                    subscribed = True
                    if not host_started:
                        remaining = deadline - asyncio.get_running_loop().time()
                        if remaining <= 0:
                            return
                        await _await_operation(
                            lifecycle.start(),
                            timeout_s=min(_OPERATION_TIMEOUT_S, remaining),
                            description="bounded native host start",
                        )
                        host_started = True

                    async def read_messages(session: Any = client) -> None:
                        async for message in session.messages:
                            await controller.handle_state_message(
                                str(message.topic),
                                _decode_mqtt_payload(message.payload),
                                bool(message.retain),
                            )

                    reader = asyncio.create_task(read_messages())
                    if not await _wait_for_session_authority(
                        controller=controller,
                        reader=reader,
                        stop=stop,
                        deadline=deadline,
                    ):
                        return
                    await _wait_for_stop_or_disconnect(
                        reader=reader,
                        stop=stop,
                        deadline=deadline,
                    )
                    return
                finally:
                    if reader is not None:
                        reader.cancel()
                        await asyncio.gather(reader, return_exceptions=True)
                    await controller.fence_transport()
                    publisher.detach(client)
                    if subscribed:
                        try:
                            await client.unsubscribe(
                                state_topic(config.stable_id),
                                timeout=_OPERATION_TIMEOUT_S,
                            )
                        except Exception:
                            pass
        except retryable_errors:
            await _wait_reconnect_backoff(
                stop=stop,
                deadline=deadline,
                backoff_s=reconnect_backoff_s,
            )
    if not stop.is_set():
        raise PilotGuardError("MQTT/HA authority was not restored before the active deadline")


def validate_gate_ledger(path: Path, *, expected_run_id: str, required_uid: int) -> None:
    """Require every earlier feasibility gate to pass before a VC5 live write."""

    raw = _read_private_regular(
        path,
        required_uid=required_uid,
        description="gate ledger",
    )
    try:
        ledger = GateLedger.loads(raw.decode("utf-8"))
    except UnicodeDecodeError as error:
        raise PilotGuardError("gate ledger is not valid UTF-8") from error
    finally:
        _wipe(raw)
    if ledger.run_id != expected_run_id:
        raise PilotGuardError("gate ledger run_id does not match this pilot")
    for gate in (GateName.VC0, GateName.VC1, GateName.VC2, GateName.VC3, GateName.VC4):
        if ledger.status(gate) is not GateStatus.PASS:
            raise PilotGuardError(f"{gate.value} must pass before the single-light pilot")


async def _run_live(
    *,
    config: PilotConfig,
    topology: TopologySnapshot,
    mqtt_host: str,
    mqtt_port: int,
    mqtt_username: str | None,
    mqtt_password: str | None,
) -> None:
    import aiomqtt
    from thrift_types.configuration.ttypes import RoomAssignment

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    total_deadline = loop.time() + config.runtime_s
    active_deadline = total_deadline - _SHUTDOWN_RESERVE_S
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):
            pass

    publisher = _LivePublisher()
    host = LivePeripheralHost(loop=loop, socket_path=config.vc_socket)
    controller = PilotController(config=config, publisher=publisher, state_sink=host)
    lifecycle = PilotLifecycle(
        config=config,
        topology=topology,
        host=host,
        room_assignment_type=RoomAssignment,
        on_command=controller.handle_panel_push,
    )

    def client_factory() -> Any:
        return aiomqtt.Client(
            hostname=mqtt_host,
            port=mqtt_port,
            username=mqtt_username,
            password=mqtt_password,
            identifier=f"brilliant-vc-pilot-{UUID(config.stable_id).hex[-8:]}",
            timeout=_OPERATION_TIMEOUT_S,
            keepalive=15,
        )

    async def no_unsubscribe() -> None:
        return None

    try:
        await _run_reconnecting_transport(
            config=config,
            lifecycle=lifecycle,
            controller=controller,
            publisher=publisher,
            client_factory=client_factory,
            retryable_errors=(aiomqtt.MqttError, _TransportEnded, _StateReplayTimeout),
            stop=stop,
            deadline=active_deadline,
        )
    finally:
        await _finish_live_resources(
            None,
            lifecycle,
            no_unsubscribe,
            deadline=total_deadline,
        )


def _decode_mqtt_payload(payload: object) -> str:
    if isinstance(payload, (bytes, bytearray)):
        return bytes(payload).decode("utf-8")
    return str(payload)


def _load_topology(path: Path, *, required_uid: int) -> TopologySnapshot:
    raw = _read_private_regular(path, required_uid=required_uid, description="topology snapshot")
    try:
        return TopologySnapshot.from_payload(json.loads(raw.decode("utf-8")))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PilotGuardError("topology snapshot is not valid UTF-8 JSON") from error
    finally:
        _wipe(raw)


def _load_identity_device_id(identity_dir: Path, *, required_uid: int) -> str:
    try:
        metadata = identity_dir.lstat()
    except FileNotFoundError:
        raise PilotGuardError("VC identity directory does not exist") from None
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise PilotGuardError("VC identity directory must be a real directory")
    if metadata.st_uid != required_uid or stat.S_IMODE(metadata.st_mode) != 0o700:
        raise PilotGuardError("VC identity directory must be owner-only mode 0700")
    raw = _read_private_regular(
        identity_dir / "device_id",
        required_uid=required_uid,
        description="VC device ID",
    )
    try:
        value = raw.decode("ascii").strip()
        _validate_device_id(value, "VC device ID")
        return value
    except UnicodeDecodeError as error:
        raise PilotGuardError("VC device ID is not ASCII") from error
    finally:
        _wipe(raw)


def _read_private_regular(path: Path, *, required_uid: int, description: str) -> bytearray:
    try:
        before = path.lstat()
    except FileNotFoundError:
        raise PilotGuardError(f"{description} does not exist") from None
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise PilotGuardError(f"{description} must be a regular non-symlink file")
    if before.st_uid != required_uid or stat.S_IMODE(before.st_mode) not in {0o400, 0o600}:
        raise PilotGuardError(f"{description} must be owner-only mode 0400 or 0600")
    if before.st_size > _MAX_PRIVATE_FILE_BYTES:
        raise PilotGuardError(f"{description} exceeds 64 KiB")
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    data = bytearray()
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise PilotGuardError(f"{description} changed during open")
        while True:
            chunk = os.read(descriptor, min(8192, _MAX_PRIVATE_FILE_BYTES + 1 - len(data)))
            if not chunk:
                break
            data.extend(chunk)
            if len(data) > _MAX_PRIVATE_FILE_BYTES:
                raise PilotGuardError(f"{description} exceeds 64 KiB")
        return data
    except Exception:
        _wipe(data)
        raise
    finally:
        os.close(descriptor)


def _wipe(value: bytearray) -> None:
    for index in range(len(value)):
        value[index] = 0


def _validate_device_id(value: str, name: str) -> None:
    if not isinstance(value, str) or not _ID.fullmatch(value):
        raise PilotGuardError(f"{name} must be 32 lowercase hex characters")


def _canonical_uuid(value: str, name: str) -> str:
    try:
        result = str(UUID(value))
    except (AttributeError, TypeError, ValueError) as error:
        raise PilotGuardError(f"{name} must be a UUID") from error
    if result != value:
        raise PilotGuardError(f"{name} must use canonical UUID form")
    return result


def _bounded_integer(value: object, *, minimum: int, maximum: int, name: str) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise PilotGuardError(f"{name} must be an integer from {minimum} to {maximum}")
    return value


def _required_str(values: Mapping[str, object], name: str) -> str:
    value = values.get(name)
    if not isinstance(value, str) or not value:
        raise PilotGuardError(f"{name} must be a non-empty string")
    return value


def _redact_device_id(value: str) -> str:
    return f"{value[:4]}…{value[-4:]}"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vc-identity-dir", type=Path, required=True)
    parser.add_argument("--topology-json", type=Path, required=True)
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--stable-id", required=True)
    parser.add_argument("--display-name", required=True)
    parser.add_argument("--room-id", required=True)
    parser.add_argument("--office-device-id", required=True)
    parser.add_argument("--vc-socket", default="/run/brilliant-vc/server_socket")
    parser.add_argument("--runtime-s", type=int, default=1_800)
    parser.add_argument("--mqtt-host")
    parser.add_argument("--mqtt-port", type=int, default=1883)
    parser.add_argument("--mqtt-username")
    parser.add_argument("--mqtt-password-file", type=Path)
    parser.add_argument(
        "--lease-dir",
        type=Path,
        default=Path("/run/brilliant-vc-control"),
        help="root-only control directory for the cross-process pilot lease",
    )
    parser.add_argument("--apply", action="store_true", help="start the one-light live host")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    required_uid = os.geteuid()
    vc_device_id = _load_identity_device_id(args.vc_identity_dir, required_uid=required_uid)
    config = PilotConfig(
        stable_id=args.stable_id,
        display_name=args.display_name,
        room_id=args.room_id,
        vc_device_id=vc_device_id,
        office_device_id=args.office_device_id,
        vc_socket=args.vc_socket,
        runtime_s=args.runtime_s,
    )
    topology = _load_topology(args.topology_json, required_uid=required_uid)
    configuration_id = validate_topology(config, topology)
    validate_gate_ledger(
        args.ledger,
        expected_run_id=args.run_id,
        required_uid=required_uid,
    )
    lease: PilotLease | None = None
    if args.apply:
        if os.geteuid() != 0:
            raise PilotGuardError("live pilot must run as root on the approved identity host")
        if not args.mqtt_host:
            raise PilotGuardError("--mqtt-host is required with --apply")
        if not 1 <= args.mqtt_port <= 65_535:
            raise PilotGuardError("MQTT port must be from 1 to 65535")
        lease = PilotLease.acquire(args.lease_dir, required_uid=required_uid)
    try:
        observed_topology = asyncio.run(_probe_live_topology(config))
        _require_matching_live_topology(topology, observed_topology, config)
        public = {
            "dry_run": not args.apply,
            "vc_device_id": _redact_device_id(vc_device_id),
            "peripheral_id": peripheral_id_for(config.stable_id),
            "display_name": config.display_name,
            "room_link_validated": config.room_id in topology.room_ids,
            "configuration_link_validated": bool(configuration_id),
            "runtime_s": config.runtime_s,
            "state_topic": state_topic(config.stable_id),
            "command_topic": command_topic(config.stable_id),
        }
        if not args.apply:
            public["status"] = "DRY RUN — no host started"
            print(json.dumps(public, indent=2, sort_keys=True))
            return 0
        password: str | None = None
        raw_password: bytearray | None = None
        if args.mqtt_password_file is not None:
            raw_password = _read_private_regular(
                args.mqtt_password_file,
                required_uid=required_uid,
                description="MQTT password file",
            )
            try:
                password = raw_password.decode("utf-8").rstrip("\r\n")
            except UnicodeDecodeError as error:
                raise PilotGuardError("MQTT password file is not valid UTF-8") from error
            finally:
                _wipe(raw_password)
        asyncio.run(
            _run_live(
                config=config,
                topology=observed_topology,
                mqtt_host=args.mqtt_host,
                mqtt_port=args.mqtt_port,
                mqtt_username=args.mqtt_username,
                mqtt_password=password,
            )
        )
        public["status"] = "pilot stopped and local absence checked twice"
        print(json.dumps(public, indent=2, sort_keys=True))
        return 0
    finally:
        if lease is not None:
            lease.release()


if __name__ == "__main__":
    raise SystemExit(main())
