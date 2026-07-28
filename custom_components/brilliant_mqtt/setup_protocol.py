"""Versioned MQTT wire contract for fleet setup validation."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import cast
from uuid import UUID

SCHEMA_VERSION = 1
MAX_SETUP_PAYLOAD_BYTES = 16 * 1024
MAX_PREFLIGHT_REPORT_BYTES = 16 * 1024
MAX_PREFLIGHT_NONCE_CHARS = 256
_REQUEST_KEYS = frozenset({"schema_version", "setup_id", "nonce"})
_RESULT_KEYS = frozenset({"schema_version", "setup_id", "nonce", "reply_to_nonce"})
_PREFLIGHT_REQUEST_KEYS = frozenset(
    {"schema_version", "setup_id", "panel_nonce", "ha_nonce", "timeout_seconds"}
)
_PREFLIGHT_SUCCESS_KEYS = frozenset(
    {
        "schema_version",
        "setup_id",
        "success",
        "completed_stages",
        "stage_elapsed_ms",
        "last_stage",
    }
)
_PREFLIGHT_FAILURE_KEYS = frozenset(
    {
        "schema_version",
        "setup_id",
        "success",
        "completed_stages",
        "stage_elapsed_ms",
        "failed_stage",
        "error_code",
        "detail",
    }
)
_PREFLIGHT_ERROR_DETAILS = {
    "settings_invalid": "Panel configuration invalid",
    "mqtt_connect": "MQTT connection failed",
    "mqtt_subscribe": "MQTT subscription failed",
    "mqtt_publish": "MQTT publish failed",
    "mqtt_timeout": "MQTT stage timed out",
    "mqtt_payload": "MQTT payload validation failed",
    "mqtt_authorization": "MQTT authorization rejected",
    "retained_flag_missing": "Retained replay flag was missing",
    "cleanup_failed": "MQTT cleanup failed",
}


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


class PreflightStage(str, Enum):  # noqa: UP042 - mirrored panel runtime is Python 3.10
    """Ordered panel-side setup validation stages."""

    FLEET_AUTH = "fleet_auth"
    PANEL_TO_HA = "panel_to_ha"
    HA_TO_PANEL = "ha_to_panel"
    DISCOVERY_WRITE = "discovery_write"
    RETAINED_MESSAGE = "retained_message"
    CLEANUP = "cleanup"


@dataclass(frozen=True, slots=True)
class PreflightRequest:
    """Canonical input passed to the temporary panel process."""

    setup_id: UUID
    panel_nonce: str
    ha_nonce: str
    timeout_seconds: float

    def __post_init__(self) -> None:
        invalid = False
        setup_id: UUID | None = None
        panel_nonce: str | None = None
        ha_nonce: str | None = None
        timeout_seconds: float | None = None
        try:
            setup_id = _validated_setup_id(self.setup_id)
            panel_nonce = _validated_preflight_nonce(self.panel_nonce)
            ha_nonce = _validated_preflight_nonce(self.ha_nonce)
            timeout_seconds = _validated_timeout(self.timeout_seconds)
        except (TypeError, ValueError):
            invalid = True
        if (
            invalid
            or setup_id is None
            or panel_nonce is None
            or ha_nonce is None
            or timeout_seconds is None
        ):
            raise ValueError("invalid_preflight_request")
        object.__setattr__(self, "setup_id", setup_id)
        object.__setattr__(self, "panel_nonce", panel_nonce)
        object.__setattr__(self, "ha_nonce", ha_nonce)
        object.__setattr__(self, "timeout_seconds", timeout_seconds)

    def to_json(self) -> str:
        """Serialize the request as canonical compact JSON."""
        return _encode_payload(
            {
                "schema_version": SCHEMA_VERSION,
                "setup_id": str(self.setup_id),
                "panel_nonce": self.panel_nonce,
                "ha_nonce": self.ha_nonce,
                "timeout_seconds": self.timeout_seconds,
            }
        )

    @classmethod
    def from_json(cls, raw: str) -> PreflightRequest:
        """Parse one exact canonical request without retaining raw input."""
        request: PreflightRequest | None = None
        try:
            value = _decode_canonical_object(
                raw,
                _PREFLIGHT_REQUEST_KEYS,
                MAX_SETUP_PAYLOAD_BYTES,
            )
            if (
                type(value["schema_version"]) is not int
                or value["schema_version"] != SCHEMA_VERSION
                or not isinstance(value["setup_id"], str)
            ):
                raise ValueError
            request = cls(
                setup_id=UUID(value["setup_id"]),
                panel_nonce=cast(str, value["panel_nonce"]),
                ha_nonce=cast(str, value["ha_nonce"]),
                timeout_seconds=cast(float, value["timeout_seconds"]),
            )
            if request.to_json() != raw:
                raise ValueError
        except (OverflowError, TypeError, ValueError):
            request = None
        if request is None:
            raise ValueError("invalid_preflight_request")
        return request


@dataclass(frozen=True, slots=True)
class PreflightReport:
    """Canonical, bounded, and redacted panel process result."""

    setup_id: UUID
    success: bool
    completed_stages: tuple[PreflightStage, ...]
    stage_elapsed_ms: Mapping[PreflightStage, int]
    last_stage: PreflightStage | None = None
    failed_stage: PreflightStage | None = None
    error_code: str | None = None
    detail: str | None = None

    def __post_init__(self) -> None:
        invalid = False
        setup_id: UUID | None = None
        completed: tuple[PreflightStage, ...] | None = None
        elapsed: dict[PreflightStage, int] | None = None
        try:
            setup_id = _validated_setup_id(self.setup_id)
            if type(self.success) is not bool:
                raise ValueError
            completed = tuple(_validated_preflight_stage(stage) for stage in self.completed_stages)
            if len(completed) != len(set(completed)):
                raise ValueError
            elapsed = {
                _validated_preflight_stage(stage): _validated_elapsed_ms(value)
                for stage, value in self.stage_elapsed_ms.items()
            }
            _validate_preflight_report_shape(
                success=self.success,
                completed=completed,
                elapsed=elapsed,
                last_stage=self.last_stage,
                failed_stage=self.failed_stage,
                error_code=self.error_code,
                detail=self.detail,
            )
        except (AttributeError, TypeError, ValueError):
            invalid = True
        if invalid or setup_id is None or completed is None or elapsed is None:
            raise ValueError("invalid_preflight_report")
        object.__setattr__(self, "setup_id", setup_id)
        object.__setattr__(self, "completed_stages", completed)
        object.__setattr__(self, "stage_elapsed_ms", MappingProxyType(elapsed))

    def to_json(self) -> str:
        """Serialize one canonical compact report object."""
        value: dict[str, object] = {
            "schema_version": SCHEMA_VERSION,
            "setup_id": str(self.setup_id),
            "success": self.success,
            "completed_stages": [stage.value for stage in self.completed_stages],
            "stage_elapsed_ms": {
                stage.value: elapsed for stage, elapsed in self.stage_elapsed_ms.items()
            },
        }
        if self.success:
            assert self.last_stage is not None
            value["last_stage"] = self.last_stage.value
        else:
            assert self.failed_stage is not None
            assert self.error_code is not None
            assert self.detail is not None
            value["failed_stage"] = self.failed_stage.value
            value["error_code"] = self.error_code
            value["detail"] = self.detail
        return _encode_payload(value)

    @classmethod
    def from_json(cls, raw: bytes | str) -> PreflightReport:
        """Parse exactly one canonical report without retaining raw output."""
        report: PreflightReport | None = None
        try:
            if not isinstance(raw, str):
                raise ValueError
            value = _decode_json_object(raw, MAX_PREFLIGHT_REPORT_BYTES)
            if (
                type(value.get("schema_version")) is not int
                or value["schema_version"] != SCHEMA_VERSION
                or type(value.get("success")) is not bool
                or not isinstance(value.get("setup_id"), str)
            ):
                raise ValueError
            expected_keys = _PREFLIGHT_SUCCESS_KEYS if value["success"] else _PREFLIGHT_FAILURE_KEYS
            if set(value) != expected_keys:
                raise ValueError
            completed_raw = value["completed_stages"]
            elapsed_raw = value["stage_elapsed_ms"]
            if not isinstance(completed_raw, list) or not isinstance(elapsed_raw, dict):
                raise ValueError
            completed = tuple(
                PreflightStage(stage)
                for stage in cast(list[object], completed_raw)
                if isinstance(stage, str)
            )
            if len(completed) != len(completed_raw):
                raise ValueError
            elapsed = {
                PreflightStage(stage): elapsed_value
                for stage, elapsed_value in cast(dict[object, object], elapsed_raw).items()
                if isinstance(stage, str)
            }
            if len(elapsed) != len(elapsed_raw):
                raise ValueError
            success = cast(bool, value["success"])
            report = cls(
                setup_id=UUID(cast(str, value["setup_id"])),
                success=success,
                completed_stages=completed,
                stage_elapsed_ms=cast(dict[PreflightStage, int], elapsed),
                last_stage=(PreflightStage(cast(str, value["last_stage"])) if success else None),
                failed_stage=(
                    None if success else PreflightStage(cast(str, value["failed_stage"]))
                ),
                error_code=None if success else cast(str, value["error_code"]),
                detail=None if success else cast(str, value["detail"]),
            )
            if report.to_json() != raw:
                raise ValueError
        except (KeyError, OverflowError, TypeError, ValueError):
            report = None
        if report is None:
            raise ValueError("invalid_preflight_report")
        return report


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


def _decode_canonical_object(
    raw: str,
    expected_keys: frozenset[str],
    maximum_bytes: int,
) -> dict[str, object]:
    value = _decode_json_object(raw, maximum_bytes)
    if set(value) != expected_keys or _encode_payload(value) != raw:
        raise ValueError
    return value


def _decode_json_object(raw: str, maximum_bytes: int) -> dict[str, object]:
    if not isinstance(raw, str):
        raise ValueError
    invalid = False
    decoded: object | None = None
    try:
        if len(raw.encode("utf-8")) > maximum_bytes:
            raise ValueError
        decoded = json.loads(raw)
    except (
        json.JSONDecodeError,
        RecursionError,
        TypeError,
        UnicodeDecodeError,
        UnicodeError,
        ValueError,
    ):
        invalid = True
    if invalid or not isinstance(decoded, dict):
        raise ValueError
    return cast(dict[str, object], decoded)


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
    if not isinstance(value, str) or not value or len(value) > MAX_PREFLIGHT_NONCE_CHARS:
        raise ValueError(f"invalid_setup_payload: {field} must be a non-empty string")
    return value


def _validated_preflight_nonce(value: object) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > MAX_PREFLIGHT_NONCE_CHARS
        or not value.isascii()
        or not value.isprintable()
    ):
        raise ValueError
    return value


def _validated_timeout(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError
    timeout = float(value)
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError
    return timeout


def _validated_preflight_stage(value: object) -> PreflightStage:
    if isinstance(value, PreflightStage):
        return value
    raise ValueError


def _validated_elapsed_ms(value: object) -> int:
    if type(value) is not int or value < 0 or value > 2_147_483_647:
        raise ValueError
    return value


def _validate_preflight_report_shape(
    *,
    success: bool,
    completed: tuple[PreflightStage, ...],
    elapsed: dict[PreflightStage, int],
    last_stage: PreflightStage | None,
    failed_stage: PreflightStage | None,
    error_code: str | None,
    detail: str | None,
) -> None:
    ordered_work = tuple(PreflightStage)[:-1]
    completed_work = tuple(stage for stage in completed if stage is not PreflightStage.CLEANUP)
    if completed_work != ordered_work[: len(completed_work)]:
        raise ValueError
    if PreflightStage.CLEANUP in completed and completed[-1] is not PreflightStage.CLEANUP:
        raise ValueError

    if success:
        if (
            completed != tuple(PreflightStage)
            or last_stage is not PreflightStage.CLEANUP
            or failed_stage is not None
            or error_code is not None
            or detail is not None
            or set(elapsed) != set(PreflightStage)
        ):
            raise ValueError
        return

    if (
        last_stage is not None
        or not isinstance(failed_stage, PreflightStage)
        or not isinstance(error_code, str)
        or not isinstance(detail, str)
        or _PREFLIGHT_ERROR_DETAILS.get(error_code) != detail
        or PreflightStage.CLEANUP not in elapsed
        or failed_stage not in elapsed
        or not set(completed).issubset(elapsed)
        or set(elapsed) != set(completed) | {failed_stage, PreflightStage.CLEANUP}
    ):
        raise ValueError
    if failed_stage is PreflightStage.CLEANUP:
        if completed_work != ordered_work or PreflightStage.CLEANUP in completed:
            raise ValueError
        return
    if completed_work != ordered_work[: ordered_work.index(failed_stage)]:
        raise ValueError
