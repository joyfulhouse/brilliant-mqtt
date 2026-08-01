"""Versioned MQTT wire contract for fleet setup validation."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import cast
from uuid import UUID

SCHEMA_VERSION = 1
MAX_SETUP_PAYLOAD_BYTES = 16 * 1024
_REQUEST_KEYS = frozenset({"schema_version", "setup_id", "nonce"})
_RESULT_KEYS = frozenset({"schema_version", "setup_id", "nonce", "reply_to_nonce"})


@dataclass(frozen=True, slots=True)
class SetupTopics:
    """MQTT topics reserved for one setup validation run."""

    panel_to_ha: str
    ha_to_panel: str
    retained: str
    discovery_probe: str

    @classmethod
    def for_id(cls, setup_id: UUID) -> SetupTopics:
        """Build the fixed topics for a version-4 setup UUID."""
        canonical_id = str(_validated_setup_id(setup_id))
        return cls(
            panel_to_ha=f"brilliant/setup/{canonical_id}/panel_to_ha",
            ha_to_panel=f"brilliant/setup/{canonical_id}/ha_to_panel",
            retained=f"brilliant/setup/{canonical_id}/retained",
            discovery_probe=(f"homeassistant/brilliant_mqtt_setup/{canonical_id}/probe"),
        )


@dataclass(frozen=True, slots=True)
class SetupRequest:
    """Panel-to-Home-Assistant setup validation request."""

    setup_id: UUID
    nonce: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "setup_id", _validated_setup_id(self.setup_id))
        _validated_nonce(self.nonce, "nonce")

    def to_payload(self) -> str:
        """Serialize the request as canonical compact JSON."""
        return _encode_payload(
            {
                "schema_version": SCHEMA_VERSION,
                "setup_id": str(self.setup_id),
                "nonce": self.nonce,
            }
        )

    @classmethod
    def from_payload(cls, payload: bytes | str) -> SetupRequest:
        """Parse and validate an exact setup request payload."""
        value = _decode_payload(payload, _REQUEST_KEYS)
        return cls(
            setup_id=_validated_setup_id(value["setup_id"]),
            nonce=_validated_nonce(value["nonce"], "nonce"),
        )


@dataclass(frozen=True, slots=True)
class SetupResult:
    """Home-Assistant-to-panel setup validation result."""

    setup_id: UUID
    nonce: str
    reply_to_nonce: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "setup_id", _validated_setup_id(self.setup_id))
        _validated_nonce(self.nonce, "nonce")
        _validated_nonce(self.reply_to_nonce, "reply_to_nonce")

    def to_payload(self) -> str:
        """Serialize the result as canonical compact JSON."""
        return _encode_payload(
            {
                "schema_version": SCHEMA_VERSION,
                "setup_id": str(self.setup_id),
                "nonce": self.nonce,
                "reply_to_nonce": self.reply_to_nonce,
            }
        )

    @classmethod
    def from_payload(cls, payload: bytes | str) -> SetupResult:
        """Parse and validate an exact setup result payload."""
        value = _decode_payload(payload, _RESULT_KEYS)
        return cls(
            setup_id=_validated_setup_id(value["setup_id"]),
            nonce=_validated_nonce(value["nonce"], "nonce"),
            reply_to_nonce=_validated_nonce(value["reply_to_nonce"], "reply_to_nonce"),
        )


def _encode_payload(value: dict[str, object]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _decode_payload(payload: bytes | str, expected_keys: frozenset[str]) -> dict[str, object]:
    payload_size = _payload_size(payload)
    if payload_size > MAX_SETUP_PAYLOAD_BYTES:
        raise ValueError("setup_payload_too_large: maximum is 16384 bytes")

    try:
        decoded = json.loads(payload)
    except (json.JSONDecodeError, RecursionError, TypeError, UnicodeDecodeError) as error:
        raise ValueError("invalid_setup_payload: expected a JSON object") from error
    if not isinstance(decoded, dict):
        raise ValueError("invalid_setup_payload: expected a JSON object")

    value = cast(dict[str, object], decoded)
    if set(value) != expected_keys:
        raise ValueError("invalid_setup_payload: unexpected or missing keys")
    if type(value["schema_version"]) is not int or value["schema_version"] != SCHEMA_VERSION:
        raise ValueError("unsupported_setup_schema: expected schema_version 1")
    return value


def _payload_size(payload: bytes | str) -> int:
    if isinstance(payload, bytes):
        return len(payload)
    if isinstance(payload, str):
        try:
            return len(payload.encode("utf-8"))
        except UnicodeEncodeError as error:
            raise ValueError("invalid_setup_payload: payload is not valid UTF-8") from error
    raise ValueError("invalid_setup_payload: payload must be bytes or str")


def _validated_setup_id(value: object) -> UUID:
    try:
        setup_id = UUID(str(value))
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError("invalid_setup_id: setup_id must be a UUID") from error
    if setup_id.version != 4:
        raise ValueError("invalid_setup_id: setup_id must be a version-4 UUID")
    return setup_id


def _validated_nonce(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"invalid_setup_payload: {field} must be a non-empty string")
    return value
