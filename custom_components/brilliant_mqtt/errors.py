"""Stable, redacted errors shared by MQTT onboarding operations."""

from __future__ import annotations

import socket
import ssl
from dataclasses import dataclass, replace
from enum import StrEnum

import voluptuous as vol
from aiomqtt.exceptions import MqttConnectError, MqttError
from paho.mqtt.reasoncodes import ReasonCode


class OperationStage(StrEnum):
    """Ordered stages shared by broker setup and validation."""

    BROKER_PROFILE = "broker_profile"
    HA_MQTT_READY = "ha_mqtt_ready"
    FLEET_AUTH = "fleet_auth"
    PANEL_TO_HA = "panel_to_ha"
    HA_TO_PANEL = "ha_to_panel"
    DISCOVERY_WRITE = "discovery_write"
    RETAINED_MESSAGE = "retained_message"
    CLEANUP = "cleanup"


@dataclass(frozen=True, slots=True)
class _ErrorMetadata:
    retryable: bool
    summary_key: str
    documentation_slug: str
    redacted_detail: str


_ERROR_METADATA = {
    "invalid_broker_profile": _ErrorMetadata(
        retryable=False,
        summary_key="mqtt_broker_profile_invalid",
        documentation_slug="mqtt-broker-profile",
        redacted_detail="The MQTT broker profile is invalid.",
    ),
    "broker_connect_failed": _ErrorMetadata(
        retryable=True,
        summary_key="mqtt_broker_connect_failed",
        documentation_slug="mqtt-broker-connect",
        redacted_detail="The MQTT broker connection failed.",
    ),
    "broker_tls_verification_failed": _ErrorMetadata(
        retryable=False,
        summary_key="mqtt_broker_tls_verification_failed",
        documentation_slug="mqtt-broker-tls",
        redacted_detail="The MQTT broker certificate could not be verified.",
    ),
    "broker_timeout": _ErrorMetadata(
        retryable=True,
        summary_key="mqtt_broker_timeout",
        documentation_slug="mqtt-broker-connect",
        redacted_detail="The MQTT broker operation timed out.",
    ),
    "broker_authentication_failed": _ErrorMetadata(
        retryable=False,
        summary_key="mqtt_broker_authentication_failed",
        documentation_slug="mqtt-broker-authentication",
        redacted_detail="The MQTT broker rejected the device credentials.",
    ),
    "broker_unavailable": _ErrorMetadata(
        retryable=True,
        summary_key="mqtt_broker_unavailable",
        documentation_slug="mqtt-broker-connect",
        redacted_detail="The MQTT broker is temporarily unavailable.",
    ),
    "broker_connection_rejected": _ErrorMetadata(
        retryable=False,
        summary_key="mqtt_broker_connection_rejected",
        documentation_slug="mqtt-broker-connect",
        redacted_detail="The MQTT broker rejected the connection settings.",
    ),
    "operation_failed": _ErrorMetadata(
        retryable=False,
        summary_key="mqtt_operation_failed",
        documentation_slug="mqtt-validation",
        redacted_detail="The MQTT operation failed.",
    ),
    # Task 6 consumes these allowlisted results without expanding this security
    # boundary or constructing message-bearing errors itself.
    "ha_mqtt_unavailable": _ErrorMetadata(
        retryable=True,
        summary_key="ha_mqtt_unavailable",
        documentation_slug="mqtt-validation",
        redacted_detail="Home Assistant MQTT is unavailable.",
    ),
    "unsupported_discovery_prefix": _ErrorMetadata(
        retryable=False,
        summary_key="unsupported_discovery_prefix",
        documentation_slug="mqtt-discovery-prefix",
        redacted_detail="The MQTT discovery prefix is unsupported.",
    ),
    "fleet_auth_failed": _ErrorMetadata(
        retryable=False,
        summary_key="fleet_auth_failed",
        documentation_slug="mqtt-broker-authentication",
        redacted_detail="The MQTT broker rejected the fleet credentials.",
    ),
    "panel_to_ha_timeout": _ErrorMetadata(
        retryable=True,
        summary_key="panel_to_ha_timeout",
        documentation_slug="mqtt-validation",
        redacted_detail=(
            "No panel-to-Home-Assistant validation message arrived; confirm both "
            "use the same MQTT broker."
        ),
    ),
    "ha_to_panel_timeout": _ErrorMetadata(
        retryable=True,
        summary_key="ha_to_panel_timeout",
        documentation_slug="mqtt-validation",
        redacted_detail=(
            "No Home-Assistant-to-panel validation message arrived; confirm both "
            "use the same MQTT broker."
        ),
    ),
    "discovery_write_denied": _ErrorMetadata(
        retryable=False,
        summary_key="discovery_write_denied",
        documentation_slug="mqtt-broker-acl",
        redacted_detail="The broker denied the discovery validation write.",
    ),
    "retained_message_invalid": _ErrorMetadata(
        retryable=False,
        summary_key="retained_message_invalid",
        documentation_slug="mqtt-retained-messages",
        redacted_detail="The retained-message validation failed.",
    ),
    "cleanup_failed": _ErrorMetadata(
        retryable=False,
        summary_key="cleanup_failed",
        documentation_slug="mqtt-validation-cleanup",
        redacted_detail="MQTT validation cleanup failed.",
    ),
}


@dataclass(frozen=True, slots=True)
class OperationError(Exception):
    """Immutable operation failure containing allowlisted metadata only."""

    stage: OperationStage
    code: str
    retryable: bool
    summary_key: str
    documentation_slug: str
    redacted_detail: str
    cleanup_error: OperationError | None = None

    def __post_init__(self) -> None:
        """Reject ad-hoc metadata that could carry raw exception text."""
        _validate_stage(self.stage)
        metadata = _ERROR_METADATA.get(self.code)
        if metadata is None or (
            self.retryable,
            self.summary_key,
            self.documentation_slug,
            self.redacted_detail,
        ) != (
            metadata.retryable,
            metadata.summary_key,
            metadata.documentation_slug,
            metadata.redacted_detail,
        ):
            raise ValueError("invalid_operation_error_metadata")
        if self.cleanup_error is not None and not isinstance(self.cleanup_error, OperationError):
            raise TypeError("cleanup_error must be an OperationError")
        Exception.__init__(self, self.code)
        object.__setattr__(self, "__suppress_context__", True)

    @classmethod
    def for_code(
        cls,
        stage: OperationStage,
        code: str,
        *,
        cleanup_error: OperationError | None = None,
    ) -> OperationError:
        """Construct an error only from the fixed metadata table."""
        _validate_stage(stage)
        metadata = _ERROR_METADATA.get(code)
        if metadata is None:
            raise ValueError("unknown_operation_error_code")
        return cls(
            stage=stage,
            code=code,
            retryable=metadata.retryable,
            summary_key=metadata.summary_key,
            documentation_slug=metadata.documentation_slug,
            redacted_detail=metadata.redacted_detail,
            cleanup_error=cleanup_error,
        )

    def with_cleanup_error(self, cleanup_error: OperationError) -> OperationError:
        """Return an immutable copy carrying a separately redacted cleanup error."""
        return replace(self, cleanup_error=cleanup_error)

    def redacted_dict(self) -> dict[str, object]:
        """Return the complete public diagnostic representation."""
        return {
            "stage": self.stage.value,
            "code": self.code,
            "retryable": self.retryable,
            "summary_key": self.summary_key,
            "documentation_slug": self.documentation_slug,
            "redacted_detail": self.redacted_detail,
            "cleanup_error": (
                self.cleanup_error.redacted_dict() if self.cleanup_error is not None else None
            ),
        }

    def __str__(self) -> str:
        """Render stable identifiers only."""
        return f"{self.stage.value}:{self.code}"

    def __repr__(self) -> str:
        """Render stable identifiers only, including a nested code at most."""
        cleanup_code = self.cleanup_error.code if self.cleanup_error is not None else None
        return (
            "OperationError("
            f"stage={self.stage.value!r}, "
            f"code={self.code!r}, "
            f"retryable={self.retryable!r}, "
            f"cleanup_code={cleanup_code!r})"
        )


def from_exception(stage: OperationStage, error: BaseException) -> OperationError:
    """Classify an exception without retaining it or raising the mapped result.

    Public boundaries must capture the raw failure, leave its exception handler,
    and only then raise the returned error so Python cannot attach the raw
    exception as implicit context.
    """
    _validate_stage(stage)
    if isinstance(error, OperationError):
        return error
    if not isinstance(error, Exception):
        raise error
    if isinstance(error, ssl.SSLCertVerificationError):
        return OperationError.for_code(stage, "broker_tls_verification_failed")
    if stage is OperationStage.BROKER_PROFILE and isinstance(
        error,
        (ValueError, TypeError, vol.Invalid),
    ):
        return OperationError.for_code(stage, "invalid_broker_profile")
    if isinstance(error, TimeoutError):
        return OperationError.for_code(stage, "broker_timeout")
    if isinstance(error, MqttConnectError):
        reason_code = _mqtt_reason_code(error)
        if reason_code in {4, 5, 134, 135, 138, 140}:
            return OperationError.for_code(stage, "broker_authentication_failed")
        if reason_code in {1, 2, 129, 130, 132, 133}:
            return OperationError.for_code(stage, "broker_connection_rejected")
        return OperationError.for_code(stage, "broker_unavailable")
    if isinstance(error, MqttError):
        if stage is OperationStage.FLEET_AUTH:
            return OperationError.for_code(stage, "broker_connect_failed")
        return OperationError.for_code(stage, "operation_failed")
    if isinstance(error, (socket.gaierror, ConnectionError, OSError)):
        return OperationError.for_code(stage, "broker_connect_failed")
    return OperationError.for_code(stage, "operation_failed")


def _validate_stage(stage: object) -> None:
    if not isinstance(stage, OperationStage):
        raise TypeError("invalid_operation_stage")


def _mqtt_reason_code(error: MqttConnectError) -> int | None:
    reason_code = error.rc
    if isinstance(reason_code, ReasonCode):
        return reason_code.value
    return reason_code
