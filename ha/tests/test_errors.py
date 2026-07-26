"""Stable, fully redacted MQTT operation errors."""

from __future__ import annotations

import socket
import ssl
from dataclasses import FrozenInstanceError
from typing import cast

import pytest
import voluptuous as vol
from aiomqtt.exceptions import MqttConnectError, MqttError
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.reasoncodes import ReasonCode

from custom_components.brilliant_mqtt.errors import (
    OperationError,
    OperationStage,
    from_exception,
)

SECRET_VALUES = (
    "mqtt-user-secret",
    "mqtt-password-secret",
    "-----BEGIN CERTIFICATE-----CA-SECRET",
    "nonce-secret",
    "mqtt.secret.example",
    "MQTT_PASSWORD=environment-secret",
)


def test_operation_stage_values_are_the_shared_stage_table() -> None:
    assert [stage.value for stage in OperationStage] == [
        "broker_profile",
        "ha_mqtt_ready",
        "fleet_auth",
        "panel_to_ha",
        "ha_to_panel",
        "discovery_write",
        "retained_message",
        "cleanup",
    ]


@pytest.mark.parametrize(
    ("error", "expected_code", "expected_retryable"),
    [
        (
            socket.gaierror(-2, "mqtt.secret.example mqtt-password-secret"),
            "broker_connect_failed",
            True,
        ),
        (
            ConnectionRefusedError("mqtt.secret.example mqtt-password-secret"),
            "broker_connect_failed",
            True,
        ),
        (
            ssl.SSLCertVerificationError(
                1,
                "certificate verify failed for mqtt.secret.example "
                "-----BEGIN CERTIFICATE-----CA-SECRET",
            ),
            "broker_tls_verification_failed",
            False,
        ),
        (
            TimeoutError("nonce-secret MQTT_PASSWORD=environment-secret"),
            "broker_timeout",
            True,
        ),
        (
            MqttConnectError(4),
            "broker_authentication_failed",
            False,
        ),
        (
            MqttConnectError(5),
            "broker_authentication_failed",
            False,
        ),
        (
            MqttConnectError(3),
            "broker_unavailable",
            True,
        ),
        (
            MqttConnectError(ReasonCode(PacketTypes.CONNACK, "Bad user name or password")),
            "broker_authentication_failed",
            False,
        ),
        (
            MqttConnectError(ReasonCode(PacketTypes.CONNACK, "Server unavailable")),
            "broker_unavailable",
            True,
        ),
        (
            MqttError("mqtt.secret.example mqtt-password-secret"),
            "broker_connect_failed",
            True,
        ),
        (
            RuntimeError("mqtt-user-secret mqtt-password-secret nonce-secret"),
            "operation_failed",
            False,
        ),
    ],
)
def test_exception_mapping_uses_stable_allowlisted_metadata(
    error: Exception,
    expected_code: str,
    expected_retryable: bool,
) -> None:
    mapped = from_exception(OperationStage.FLEET_AUTH, error)

    assert mapped.stage is OperationStage.FLEET_AUTH
    assert mapped.code == expected_code
    assert mapped.retryable is expected_retryable
    assert mapped.summary_key
    assert mapped.documentation_slug
    assert mapped.redacted_detail
    _assert_secret_free(mapped)


@pytest.mark.parametrize(
    "error",
    [
        ValueError("mqtt-user-secret mqtt-password-secret MQTT_PASSWORD=environment-secret"),
        TypeError("mqtt-user-secret mqtt-password-secret MQTT_PASSWORD=environment-secret"),
        vol.Invalid("mqtt-user-secret mqtt-password-secret MQTT_PASSWORD=environment-secret"),
    ],
)
def test_profile_validation_errors_map_without_raw_text(error: Exception) -> None:
    mapped = from_exception(
        OperationStage.BROKER_PROFILE,
        error,
    )

    assert mapped.code == "invalid_broker_profile"
    assert mapped.retryable is False
    _assert_secret_free(mapped)


@pytest.mark.parametrize(
    "error",
    [
        ValueError("mqtt-user-secret mqtt-password-secret"),
        TypeError("mqtt-user-secret mqtt-password-secret"),
        vol.Invalid("mqtt-user-secret mqtt-password-secret"),
    ],
)
def test_non_profile_validation_errors_do_not_claim_profile_is_invalid(
    error: Exception,
) -> None:
    mapped = from_exception(OperationStage.RETAINED_MESSAGE, error)

    assert mapped.stage is OperationStage.RETAINED_MESSAGE
    assert mapped.code == "operation_failed"
    assert mapped.retryable is False
    _assert_secret_free(mapped)


@pytest.mark.parametrize(
    "control_error",
    [
        GeneratorExit("generator-secret"),
        BaseExceptionGroup(
            "base-group-secret",
            [GeneratorExit("nested-generator-secret")],
        ),
    ],
)
def test_non_exception_base_exceptions_propagate_unchanged(
    control_error: BaseException,
) -> None:
    with pytest.raises(type(control_error)) as caught:
        from_exception(OperationStage.FLEET_AUTH, control_error)

    assert caught.value is control_error


def test_operation_error_rejects_invalid_stage_with_fixed_text() -> None:
    valid = OperationError.for_code(
        OperationStage.FLEET_AUTH,
        "broker_connect_failed",
    )
    invalid_stage = cast(OperationStage, "mqtt-password-secret")

    with pytest.raises(TypeError, match="^invalid_operation_stage$") as direct:
        OperationError(
            stage=invalid_stage,
            code=valid.code,
            retryable=valid.retryable,
            summary_key=valid.summary_key,
            documentation_slug=valid.documentation_slug,
            redacted_detail=valid.redacted_detail,
        )
    with pytest.raises(TypeError, match="^invalid_operation_stage$") as factory:
        OperationError.for_code(invalid_stage, "broker_connect_failed")

    for caught in (direct, factory):
        assert "mqtt-password-secret" not in str(caught.value)
        assert "mqtt-password-secret" not in repr(caught.value)


@pytest.mark.parametrize(
    ("stage", "code", "expected_detail"),
    [
        (
            OperationStage.PANEL_TO_HA,
            "panel_to_ha_timeout",
            "No panel-to-Home-Assistant validation message arrived; the panel "
            "and Home Assistant may use different MQTT brokers, or a broker ACL "
            "may block panel-to-Home-Assistant traffic.",
        ),
        (
            OperationStage.HA_TO_PANEL,
            "ha_to_panel_timeout",
            "No Home-Assistant-to-panel validation message arrived; Home "
            "Assistant and the panel may use different MQTT brokers, or a broker "
            "ACL may block Home-Assistant-to-panel traffic.",
        ),
    ],
)
def test_routing_timeout_guidance_covers_broker_and_directional_acl_failures(
    stage: OperationStage,
    code: str,
    expected_detail: str,
) -> None:
    error = OperationError.for_code(stage, code)

    assert error.redacted_detail == expected_detail
    assert "Mosquitto" not in error.redacted_detail
    _assert_secret_free(error)


def test_existing_operation_error_is_not_double_wrapped() -> None:
    mapped = from_exception(
        OperationStage.BROKER_PROFILE,
        ValueError("mqtt-password-secret"),
    )

    assert from_exception(OperationStage.CLEANUP, mapped) is mapped


def test_nested_cleanup_error_remains_fully_redacted() -> None:
    primary = from_exception(
        OperationStage.FLEET_AUTH,
        MqttConnectError(4),
    )
    cleanup = from_exception(
        OperationStage.CLEANUP,
        RuntimeError(
            "mqtt-user-secret mqtt-password-secret nonce-secret "
            "-----BEGIN CERTIFICATE-----CA-SECRET"
        ),
    )
    combined = primary.with_cleanup_error(cleanup)

    assert combined.cleanup_error is cleanup
    diagnostics = combined.redacted_dict()
    assert diagnostics["cleanup_error"] == cleanup.redacted_dict()
    _assert_secret_free(combined)


def test_from_exception_is_a_chain_free_classifier_for_deferred_raising() -> None:
    mapped: OperationError | None = None
    try:
        raise RuntimeError("mqtt-user-secret mqtt-password-secret mqtt.secret.example nonce-secret")
    except RuntimeError as raw_error:
        mapped = from_exception(OperationStage.FLEET_AUTH, raw_error)

    assert mapped is not None
    assert mapped.__context__ is None
    assert mapped.__cause__ is None
    with pytest.raises(OperationError) as caught:
        raise mapped

    assert caught.value is mapped
    assert caught.value.__context__ is None
    assert caught.value.__cause__ is None
    _assert_secret_free(caught.value)


def test_operation_error_is_frozen() -> None:
    error = from_exception(OperationStage.BROKER_PROFILE, ValueError("bad"))

    field = "code"
    with pytest.raises(FrozenInstanceError):
        setattr(error, field, "changed")


def _assert_secret_free(error: OperationError) -> None:
    surfaces = (
        str(error),
        repr(error),
        repr(error.redacted_dict()),
    )
    for secret in SECRET_VALUES:
        assert all(secret not in surface for surface in surfaces)
