"""Versioned MQTT wire contract for Home Assistant control traffic."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast
from uuid import UUID, uuid5

SCHEMA_VERSION = 1
MAPPING_VERSION = 1
COMMAND_TTL_MS = 15_000
COMMAND_FUTURE_SKEW_MS = 5_000
_STABLE_NAMESPACE = UUID("ddd06dfa-168a-5a0b-b8b3-4c5f742b0354")
_TOPIC_PREFIX = "brilliant/ha-control/v1"
_PANEL_PATTERN = re.compile(r"[a-z0-9][a-z0-9_-]{0,62}")


@dataclass(frozen=True)
class EntityCommand:
    """A validated panel-originated entity command."""

    command_id: str
    stable_id: str
    kind: str
    value: object
    observed_sequence: int
    issued_at_ms: int


@dataclass(frozen=True)
class SceneCommand:
    """A validated request to run a Brilliant scene."""

    command_id: str
    panel: str
    scene_id: str
    issued_at_ms: int


@dataclass(frozen=True)
class ModeCommand:
    """A validated request to activate a Brilliant mode."""

    command_id: str
    panel: str
    mode_id: str
    issued_at_ms: int


def stable_id(entity_id: str) -> str:
    """Return the deterministic wire ID for a Home Assistant entity ID."""
    return str(uuid5(_STABLE_NAMESPACE, entity_id))


def encode_json(value: Mapping[str, object]) -> str:
    """Encode a mapping using the canonical compact wire representation."""
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def manifest_topic() -> str:
    """Return the retained entity-manifest topic."""
    return f"{_TOPIC_PREFIX}/manifest"


def state_topic(value: str) -> str:
    """Return the retained state topic for a stable entity ID."""
    return f"{_TOPIC_PREFIX}/state/{_validated_uuid(value, 'stable_id')}"


def command_topic(value: str) -> str:
    """Return the entity-command topic for a stable entity ID."""
    return f"{_TOPIC_PREFIX}/command/{_validated_uuid(value, 'stable_id')}"


def result_topic(value: str) -> str:
    """Return the entity-result topic for a command ID."""
    return f"{_TOPIC_PREFIX}/result/{_validated_uuid(value, 'command_id')}"


def scene_catalog_topic(panel: str) -> str:
    """Return a panel's retained scene-catalog topic."""
    return f"{_TOPIC_PREFIX}/scene/catalog/{_validated_panel(panel)}"


def scene_event_topic(panel: str) -> str:
    """Return a panel's scene-execution event topic."""
    return f"{_TOPIC_PREFIX}/scene/event/{_validated_panel(panel)}"


def scene_command_topic(panel: str) -> str:
    """Return a panel's scene-command topic."""
    return f"{_TOPIC_PREFIX}/scene/command/{_validated_panel(panel)}"


def scene_result_topic(command_id: str) -> str:
    """Return the result topic for a scene command."""
    return f"{_TOPIC_PREFIX}/scene/result/{_validated_uuid(command_id, 'command_id')}"


def mode_catalog_topic(panel: str) -> str:
    """Return a panel's retained mode-catalog topic."""
    return f"{_TOPIC_PREFIX}/mode/catalog/{_validated_panel(panel)}"


def mode_event_topic(panel: str) -> str:
    """Return a panel's mode-change event topic."""
    return f"{_TOPIC_PREFIX}/mode/event/{_validated_panel(panel)}"


def mode_command_topic(panel: str) -> str:
    """Return a panel's mode-command topic."""
    return f"{_TOPIC_PREFIX}/mode/command/{_validated_panel(panel)}"


def mode_result_topic(command_id: str) -> str:
    """Return the result topic for a mode command."""
    return f"{_TOPIC_PREFIX}/mode/result/{_validated_uuid(command_id, 'command_id')}"


def transport_status_topic(transport: str, panel: str | None = None) -> str:
    """Return a retained transport-status topic, optionally scoped to a panel."""
    topic = f"{_TOPIC_PREFIX}/status/{_validated_panel(transport)}"
    if panel is not None:
        topic = f"{topic}/{_validated_panel(panel)}"
    return topic


def decode_command(payload: str, *, now_ms: int) -> EntityCommand:
    """Decode and validate an entity command payload."""
    value = _decode_envelope(payload)
    command_id = _validated_uuid(_required_str(value, "command_id"), "command_id")
    entity_stable_id = _validated_uuid(_required_str(value, "stable_id"), "stable_id")
    kind = _required_str(value, "kind")
    if "value" not in value:
        raise ValueError("missing value")
    observed_sequence = _required_int(value, "observed_sequence")
    if observed_sequence < 0:
        raise ValueError("observed_sequence must be non-negative")
    issued_at_ms = _validated_timestamp(value, now_ms=now_ms)
    return EntityCommand(
        command_id=command_id,
        stable_id=entity_stable_id,
        kind=kind,
        value=value["value"],
        observed_sequence=observed_sequence,
        issued_at_ms=issued_at_ms,
    )


def decode_scene_command(payload: str, *, now_ms: int) -> SceneCommand:
    """Decode and validate a scene command payload."""
    value = _decode_envelope(payload)
    return SceneCommand(
        command_id=_validated_uuid(_required_str(value, "command_id"), "command_id"),
        panel=_validated_panel(_required_str(value, "panel")),
        scene_id=_required_str(value, "scene_id"),
        issued_at_ms=_validated_timestamp(value, now_ms=now_ms),
    )


def decode_mode_command(payload: str, *, now_ms: int) -> ModeCommand:
    """Decode and validate a mode command payload."""
    value = _decode_envelope(payload)
    return ModeCommand(
        command_id=_validated_uuid(_required_str(value, "command_id"), "command_id"),
        panel=_validated_panel(_required_str(value, "panel")),
        mode_id=_required_str(value, "mode_id"),
        issued_at_ms=_validated_timestamp(value, now_ms=now_ms),
    )


def validate_entity_command_context(
    command: EntityCommand, *, topic_stable_id: str, retained: bool
) -> None:
    """Reject unsafe MQTT metadata or a topic/payload stable-ID mismatch."""
    _reject_retained(retained)
    if _validated_uuid(topic_stable_id, "topic_stable_id") != command.stable_id:
        raise ValueError("topic stable_id does not match command stable_id")


def validate_scene_command_context(
    command: SceneCommand, *, topic_panel: str, retained: bool
) -> None:
    """Reject unsafe MQTT metadata or a topic/payload scene panel mismatch."""
    _reject_retained(retained)
    if _validated_panel(topic_panel) != command.panel:
        raise ValueError("topic panel does not match command panel")


def validate_mode_command_context(
    command: ModeCommand, *, topic_panel: str, retained: bool
) -> None:
    """Reject unsafe MQTT metadata or a topic/payload mode panel mismatch."""
    _reject_retained(retained)
    if _validated_panel(topic_panel) != command.panel:
        raise ValueError("topic panel does not match command panel")


def _validated_uuid(value: str, field: str) -> str:
    try:
        parsed = UUID(value)
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError(f"{field} must be a UUID") from error
    return str(parsed)


def _validated_panel(panel: str) -> str:
    if _PANEL_PATTERN.fullmatch(panel) is None:
        raise ValueError("panel must be a percent-free lowercase slug")
    return panel


def _decode_envelope(payload: str) -> dict[str, object]:
    try:
        decoded = json.loads(payload)
    except (json.JSONDecodeError, TypeError) as error:
        raise ValueError("payload must be valid JSON") from error
    if not isinstance(decoded, dict) or not all(isinstance(key, str) for key in decoded):
        raise ValueError("payload must be a JSON object")
    value = cast(dict[str, object], decoded)
    if _required_int(value, "schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported schema_version")
    if _required_int(value, "mapping_version") != MAPPING_VERSION:
        raise ValueError("unsupported mapping_version")
    return value


def _required_str(value: Mapping[str, object], field: str) -> str:
    result = value.get(field)
    if not isinstance(result, str) or not result:
        raise ValueError(f"{field} must be a non-empty string")
    return result


def _required_int(value: Mapping[str, object], field: str) -> int:
    result = value.get(field)
    if type(result) is not int:
        raise ValueError(f"{field} must be an integer")
    return result


def _validated_timestamp(value: Mapping[str, object], *, now_ms: int) -> int:
    if type(now_ms) is not int:
        raise ValueError("now_ms must be an integer")
    issued_at_ms = _required_int(value, "issued_at_ms")
    if issued_at_ms > now_ms + COMMAND_FUTURE_SKEW_MS:
        raise ValueError("command timestamp is too far in the future")
    if now_ms - issued_at_ms > COMMAND_TTL_MS:
        raise ValueError("command has expired")
    return issued_at_ms


def _reject_retained(retained: bool) -> None:
    if retained:
        raise ValueError("retained commands are not allowed")
