"""Durable, strictly validated state for the scene MQTT transport."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast
from uuid import UUID

from brilliant_mqtt.ha_control_protocol import (
    MAPPING_VERSION,
    SCHEMA_VERSION,
    encode_json,
    mode_event_topic,
    mode_result_topic,
    scene_event_topic,
    scene_result_topic,
)

STATE_VERSION = 1
MAX_STATE_BYTES = 4 * 1024 * 1024
SCENE_WATERMARK_LIMIT = 4_096
MODE_WATERMARK_LIMIT = 1_024
EVENT_OUTBOX_LIMIT = 1_024
RESULT_OUTBOX_LIMIT = 1_024
PENDING_LIMIT = 1_024

_STATE_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="scene-state")

StateKind = Literal["scene", "mode"]
StateKey = tuple[StateKind, str]


@dataclass(frozen=True, slots=True)
class StateWatermark:
    """Newest durable execution identity for one panel scene."""

    executed_at_ms: int
    payload_sha256: str


@dataclass(frozen=True, slots=True)
class StateEvent:
    """One durable activation event in the delivery outbox."""

    topic: str
    payload: str
    delivered: bool
    created_at_ms: int


@dataclass(frozen=True, slots=True)
class StateResult:
    """One durable terminal command result."""

    kind: StateKind
    command_id: str
    fingerprint: str
    command_panel: str
    command_value: str
    issued_at_ms: int
    topic: str
    payload: str
    delivered: bool
    expires_at_ms: int
    event_key: str | None


@dataclass(frozen=True, slots=True)
class StatePending:
    """One durable command intent written before the physical bus write."""

    kind: StateKind
    command_id: str
    value: str
    fingerprint: str
    panel: str
    issued_at_ms: int
    expires_at_ms: int


@dataclass(frozen=True, slots=True)
class SceneState:
    """Immutable snapshot handed from async orchestration to the state writer."""

    watermarks: tuple[tuple[tuple[str, str], StateWatermark], ...] = ()
    mode_watermarks: tuple[tuple[str, tuple[int, str]], ...] = ()
    events: tuple[tuple[str, StateEvent], ...] = ()
    results: tuple[tuple[StateKey, StateResult], ...] = ()
    pending: tuple[tuple[StateKey, StatePending], ...] = ()


@dataclass(frozen=True, slots=True)
class LoadedSceneState:
    """Outcome of loading a whole state file."""

    state: SceneState
    trusted: bool
    reason: str | None


class StateValidationError(ValueError):
    """The whole persisted snapshot failed strict validation."""


def state_executor() -> ThreadPoolExecutor:
    """Return the process-wide total order for all scene state filesystem I/O."""
    return _STATE_EXECUTOR


def command_fingerprint_fields(
    kind: str,
    command_id: str,
    panel: str,
    identifier: str,
    issued_at_ms: int,
) -> str:
    """Return the canonical identity of all command fields relevant to replay."""
    canonical = encode_json(
        {
            "kind": kind,
            "command_id": command_id,
            "panel": panel,
            f"{kind}_id": identifier,
            "issued_at_ms": issued_at_ms,
        }
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def state_payload(state: SceneState) -> dict[str, object]:
    """Serialize an immutable snapshot into the versioned JSON shape."""
    watermarks: dict[str, dict[str, dict[str, object]]] = {}
    for (panel, scene_id), watermark in state.watermarks:
        watermarks.setdefault(panel, {})[scene_id] = {
            "executed_at_ms": watermark.executed_at_ms,
            "payload_sha256": watermark.payload_sha256,
        }
    mode_watermarks = {
        panel: {"executed_at_ms": watermark[0], "mode_id": watermark[1]}
        for panel, watermark in state.mode_watermarks
    }
    events = {
        key: {
            "topic": event.topic,
            "payload": event.payload,
            "delivered": event.delivered,
            "created_at_ms": event.created_at_ms,
        }
        for key, event in state.events
    }
    results = {
        f"{kind}:{command_id}": {
            "kind": result.kind,
            "command_id": result.command_id,
            "fingerprint": result.fingerprint,
            "command_panel": result.command_panel,
            "command_value": result.command_value,
            "issued_at_ms": result.issued_at_ms,
            "topic": result.topic,
            "payload": result.payload,
            "delivered": result.delivered,
            "expires_at_ms": result.expires_at_ms,
            "event_key": result.event_key,
        }
        for (kind, command_id), result in state.results
    }
    pending = {
        f"{kind}:{command_id}": {
            "kind": record.kind,
            "command_id": record.command_id,
            "value": record.value,
            "fingerprint": record.fingerprint,
            "panel": record.panel,
            "issued_at_ms": record.issued_at_ms,
            "expires_at_ms": record.expires_at_ms,
        }
        for (kind, command_id), record in state.pending
    }
    return {
        "version": STATE_VERSION,
        "watermarks": watermarks,
        "mode_watermarks": mode_watermarks,
        "events": events,
        "results": results,
        "pending": pending,
    }


def load_state(path: Path) -> LoadedSceneState:
    """Read at most ``MAX_STATE_BYTES`` and validate the snapshot as one unit."""
    descriptor = -1
    try:
        descriptor = os.open(path, os.O_RDONLY)
        if os.fstat(descriptor).st_size > MAX_STATE_BYTES:
            raise StateValidationError("state file exceeds byte limit")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            content = handle.read(MAX_STATE_BYTES + 1)
            if len(content) > MAX_STATE_BYTES:
                raise StateValidationError("state file exceeds byte limit")
        raw = json.loads(content.decode("utf-8"))
        state = _parse_state(raw)
        _normalize_permissions(path)
    except FileNotFoundError:
        return LoadedSceneState(SceneState(), True, None)
    except (OSError, UnicodeError, json.JSONDecodeError, StateValidationError, ValueError):
        return LoadedSceneState(SceneState(), False, "state_untrusted")
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    return LoadedSceneState(state, True, None)


def atomic_write_state(path: Path, state: SceneState) -> None:
    """Synchronously validate and atomically replace one private state file."""
    validated = _parse_state(state_payload(state))
    encoded = json.dumps(state_payload(validated), separators=(",", ":"), sort_keys=True).encode(
        "utf-8"
    )
    if len(encoded) > MAX_STATE_BYTES:
        raise OSError("state snapshot exceeds byte limit")

    descriptor = -1
    temporary = ""
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path.parent, 0o700)
    _fsync_directory(path.parent)
    try:
        descriptor, temporary = tempfile.mkstemp(
            dir=path.parent,
            prefix=f".{path.name}.",
        )
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        temporary = ""
        os.chmod(path, 0o600)
        file_descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(file_descriptor)
        finally:
            os.close(file_descriptor)
        os.chmod(path.parent, 0o700)
        _fsync_directory(path.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if temporary:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass


def _normalize_permissions(path: Path) -> None:
    os.chmod(path, 0o600)
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.chmod(path.parent, 0o700)
    _fsync_directory(path.parent)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _parse_state(raw: object) -> SceneState:
    if not isinstance(raw, dict) or set(raw) != {
        "version",
        "watermarks",
        "mode_watermarks",
        "events",
        "results",
        "pending",
    }:
        raise StateValidationError("invalid state schema")
    if type(raw["version"]) is not int or raw["version"] != STATE_VERSION:
        raise StateValidationError("invalid state version")
    watermarks_raw = raw["watermarks"]
    mode_watermarks_raw = raw["mode_watermarks"]
    events_raw, results_raw, pending_raw = raw["events"], raw["results"], raw["pending"]
    if not all(
        isinstance(item, dict)
        for item in (
            watermarks_raw,
            mode_watermarks_raw,
            events_raw,
            results_raw,
            pending_raw,
        )
    ):
        raise StateValidationError("invalid state collections")
    watermarks_dict = cast(dict[object, object], watermarks_raw)
    mode_watermarks_dict = cast(dict[object, object], mode_watermarks_raw)
    events_dict = cast(dict[object, object], events_raw)
    results_dict = cast(dict[object, object], results_raw)
    pending_dict = cast(dict[object, object], pending_raw)
    if (
        sum(len(item) for item in watermarks_dict.values() if isinstance(item, dict))
        > SCENE_WATERMARK_LIMIT
        or len(mode_watermarks_dict) > MODE_WATERMARK_LIMIT
        or len(events_dict) > EVENT_OUTBOX_LIMIT
        or len(results_dict) > RESULT_OUTBOX_LIMIT
        or len(pending_dict) > PENDING_LIMIT
    ):
        raise StateValidationError("state collections exceed limits")

    watermarks: list[tuple[tuple[str, str], StateWatermark]] = []
    scene_watermark_count = 0
    for panel, panel_records in watermarks_dict.items():
        if not isinstance(panel, str) or not panel or not isinstance(panel_records, dict):
            raise StateValidationError("invalid watermark panel")
        scene_watermark_count += len(panel_records)
        if scene_watermark_count > SCENE_WATERMARK_LIMIT:
            raise StateValidationError("scene watermark capacity exceeded")
        for scene_id, value in panel_records.items():
            if (
                not isinstance(scene_id, str)
                or not scene_id
                or not isinstance(value, dict)
                or set(value) != {"executed_at_ms", "payload_sha256"}
            ):
                raise StateValidationError("invalid watermark entry")
            executed_at_ms = value["executed_at_ms"]
            payload_sha256 = value["payload_sha256"]
            if not (
                type(executed_at_ms) is int
                and executed_at_ms >= 0
                and isinstance(payload_sha256, str)
                and _is_sha256(payload_sha256)
            ):
                raise StateValidationError("invalid watermark value")
            watermarks.append(((panel, scene_id), StateWatermark(executed_at_ms, payload_sha256)))

    mode_watermarks: list[tuple[str, tuple[int, str]]] = []
    for panel, value in mode_watermarks_dict.items():
        if (
            not isinstance(panel, str)
            or not panel
            or not isinstance(value, dict)
            or set(value) != {"executed_at_ms", "mode_id"}
        ):
            raise StateValidationError("invalid mode watermark entry")
        executed_at_ms, mode_id = value["executed_at_ms"], value["mode_id"]
        if not (
            type(executed_at_ms) is int
            and executed_at_ms >= 0
            and isinstance(mode_id, str)
            and mode_id
        ):
            raise StateValidationError("invalid mode watermark value")
        mode_watermarks.append((panel, (executed_at_ms, mode_id)))

    events: list[tuple[str, StateEvent]] = []
    event_semantics: dict[str, tuple[StateKind, str, str, bool]] = {}
    for event_key, value in events_dict.items():
        if (
            not isinstance(event_key, str)
            or not isinstance(value, dict)
            or set(value) != {"topic", "payload", "delivered", "created_at_ms"}
        ):
            raise StateValidationError("invalid event entry")
        topic, payload = value["topic"], value["payload"]
        delivered, created_at_ms = value["delivered"], value["created_at_ms"]
        if not (
            isinstance(topic, str)
            and isinstance(payload, str)
            and type(delivered) is bool
            and type(created_at_ms) is int
            and created_at_ms >= 0
        ):
            raise StateValidationError("invalid event value")
        kind, panel, identifier, executed_at_ms = _parse_event_payload(payload)
        expected_topic = scene_event_topic(panel) if kind == "scene" else mode_event_topic(panel)
        deduplication_key = f"{panel}:{identifier}:{executed_at_ms}"
        decoded_event = json.loads(payload)
        if (
            topic != expected_topic
            or decoded_event.get("deduplication_key") != deduplication_key
            or event_key != f"{kind}:{deduplication_key}"
        ):
            raise StateValidationError("invalid event identity")
        event_semantics[event_key] = (kind, panel, identifier, delivered)
        events.append((event_key, StateEvent(topic, payload, delivered, created_at_ms)))

    results: list[tuple[StateKey, StateResult]] = []
    result_keys: set[StateKey] = set()
    for result_key, value in results_dict.items():
        required = {
            "kind",
            "command_id",
            "fingerprint",
            "command_panel",
            "command_value",
            "issued_at_ms",
            "topic",
            "payload",
            "delivered",
            "expires_at_ms",
            "event_key",
        }
        if not isinstance(result_key, str) or not isinstance(value, dict) or set(value) != required:
            raise StateValidationError("invalid result entry")
        kind = _validated_kind(value["kind"])
        command_id = value["command_id"]
        fingerprint, topic, payload = value["fingerprint"], value["topic"], value["payload"]
        command_panel = value["command_panel"]
        command_value, issued_at_ms = value["command_value"], value["issued_at_ms"]
        delivered, expires_at_ms = value["delivered"], value["expires_at_ms"]
        event_key = value["event_key"]
        if not (
            isinstance(command_id, str)
            and result_key == f"{kind}:{command_id}"
            and isinstance(fingerprint, str)
            and _is_sha256(fingerprint)
            and isinstance(command_panel, str)
            and command_panel
            and isinstance(command_value, str)
            and command_value
            and type(issued_at_ms) is int
            and issued_at_ms >= 0
            and isinstance(topic, str)
            and isinstance(payload, str)
            and type(delivered) is bool
            and type(expires_at_ms) is int
            and expires_at_ms >= 0
            and (event_key is None or isinstance(event_key, str))
        ):
            raise StateValidationError("invalid result value")
        UUID(command_id)
        if fingerprint != command_fingerprint_fields(
            kind, command_id, command_panel, command_value, issued_at_ms
        ):
            raise StateValidationError("invalid result fingerprint")
        accepted = _validate_result_payload(
            kind, command_id, command_panel, command_value, topic, payload
        )
        if event_key is not None:
            dependency = event_semantics.get(event_key)
            expected_dependency = (kind, command_panel, command_value)
            if (
                dependency is None
                or dependency[:3] != expected_dependency
                or not accepted
                or (delivered and not dependency[3])
            ):
                raise StateValidationError("invalid result event dependency")
        key = (kind, command_id)
        result_keys.add(key)
        results.append(
            (
                key,
                StateResult(
                    kind,
                    command_id,
                    fingerprint,
                    command_panel,
                    command_value,
                    issued_at_ms,
                    topic,
                    payload,
                    delivered,
                    expires_at_ms,
                    event_key,
                ),
            )
        )

    pending: list[tuple[StateKey, StatePending]] = []
    for pending_key, value in pending_dict.items():
        required = {
            "kind",
            "command_id",
            "value",
            "fingerprint",
            "panel",
            "issued_at_ms",
            "expires_at_ms",
        }
        if (
            not isinstance(pending_key, str)
            or not isinstance(value, dict)
            or set(value) != required
        ):
            raise StateValidationError("invalid pending entry")
        kind = _validated_kind(value["kind"])
        command_id, command_value = value["command_id"], value["value"]
        fingerprint, expires_at_ms = value["fingerprint"], value["expires_at_ms"]
        command_panel, issued_at_ms = value["panel"], value["issued_at_ms"]
        if not (
            isinstance(command_id, str)
            and pending_key == f"{kind}:{command_id}"
            and isinstance(command_value, str)
            and command_value
            and isinstance(fingerprint, str)
            and _is_sha256(fingerprint)
            and isinstance(command_panel, str)
            and command_panel
            and type(issued_at_ms) is int
            and issued_at_ms >= 0
            and type(expires_at_ms) is int
            and expires_at_ms >= 0
        ):
            raise StateValidationError("invalid pending value")
        UUID(command_id)
        if fingerprint != command_fingerprint_fields(
            kind, command_id, command_panel, command_value, issued_at_ms
        ):
            raise StateValidationError("invalid pending fingerprint")
        key = (kind, command_id)
        if key in result_keys:
            raise StateValidationError("command cannot be pending and complete")
        pending.append(
            (
                key,
                StatePending(
                    kind,
                    command_id,
                    command_value,
                    fingerprint,
                    command_panel,
                    issued_at_ms,
                    expires_at_ms,
                ),
            )
        )

    return SceneState(
        watermarks=tuple(sorted(watermarks)),
        mode_watermarks=tuple(sorted(mode_watermarks)),
        events=tuple(sorted(events)),
        results=tuple(results),
        pending=tuple(sorted(pending)),
    )


def _parse_event_payload(payload: str) -> tuple[StateKind, str, str, int]:
    decoded = json.loads(payload)
    if not isinstance(decoded, dict):
        raise StateValidationError("invalid event payload")
    panel, executed_at_ms = decoded.get("panel"), decoded.get("executed_at_ms")
    if (
        not isinstance(panel, str)
        or not panel
        or type(decoded.get("schema_version")) is not int
        or decoded["schema_version"] != SCHEMA_VERSION
        or type(decoded.get("mapping_version")) is not int
        or decoded["mapping_version"] != MAPPING_VERSION
        or type(executed_at_ms) is not int
        or executed_at_ms < 0
    ):
        raise StateValidationError("invalid event payload metadata")
    if "scene_id" in decoded:
        kind: StateKind = "scene"
    elif "mode_id" in decoded:
        kind = "mode"
    else:
        raise StateValidationError("invalid event kind")
    identifier = decoded.get(f"{kind}_id")
    if (
        not isinstance(identifier, str)
        or not identifier
        or set(decoded)
        != {
            "schema_version",
            "mapping_version",
            "panel",
            f"{kind}_id",
            "executed_at_ms",
            "deduplication_key",
        }
    ):
        raise StateValidationError("invalid event payload fields")
    return kind, panel, identifier, executed_at_ms


def _validate_result_payload(
    kind: StateKind,
    command_id: str,
    command_panel: str,
    command_value: str,
    topic: str,
    payload: str,
) -> bool:
    decoded = json.loads(payload)
    expected_topic = (
        scene_result_topic(command_id) if kind == "scene" else mode_result_topic(command_id)
    )
    expected_fields = {
        "schema_version",
        "mapping_version",
        "command_id",
        "panel",
        f"{kind}_id",
        "accepted",
        "timestamp_ms",
    }
    if isinstance(decoded, dict) and decoded.get("accepted") is False:
        expected_fields.add("error")
    if (
        topic != expected_topic
        or not isinstance(decoded, dict)
        or set(decoded) != expected_fields
        or decoded.get("command_id") != command_id
        or type(decoded.get("schema_version")) is not int
        or decoded["schema_version"] != SCHEMA_VERSION
        or type(decoded.get("mapping_version")) is not int
        or decoded["mapping_version"] != MAPPING_VERSION
        or decoded.get("panel") != command_panel
        or decoded.get(f"{kind}_id") != command_value
        or type(decoded.get("accepted")) is not bool
        or type(decoded.get("timestamp_ms")) is not int
        or decoded["timestamp_ms"] < 0
    ):
        raise StateValidationError("invalid result payload")
    accepted = cast(bool, decoded["accepted"])
    if not accepted:
        error = decoded.get("error")
        if not isinstance(error, str) or not error:
            raise StateValidationError("invalid result error")
    return accepted


def _validated_kind(value: object) -> StateKind:
    if value == "scene" or value == "mode":
        return value
    raise StateValidationError("invalid state kind")


def _is_sha256(value: str) -> bool:
    if len(value) != 64:
        return False
    try:
        bytes.fromhex(value)
    except ValueError:
        return False
    return True


__all__ = [
    "EVENT_OUTBOX_LIMIT",
    "MAX_STATE_BYTES",
    "MODE_WATERMARK_LIMIT",
    "PENDING_LIMIT",
    "RESULT_OUTBOX_LIMIT",
    "SCENE_WATERMARK_LIMIT",
    "LoadedSceneState",
    "SceneState",
    "StateEvent",
    "StateKey",
    "StateKind",
    "StatePending",
    "StateResult",
    "StateWatermark",
    "atomic_write_state",
    "command_fingerprint_fields",
    "load_state",
    "state_executor",
    "state_payload",
]
