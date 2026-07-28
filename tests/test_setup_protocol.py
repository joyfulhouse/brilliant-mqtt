from __future__ import annotations

import json
import traceback
from dataclasses import FrozenInstanceError, asdict
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from brilliant_mqtt.setup_protocol import (
    MAX_PREFLIGHT_REPORT_BYTES,
    PreflightReport,
    PreflightRequest,
    PreflightStage,
    SetupRequest,
    SetupResult,
    SetupTopics,
)

FIXTURE_PATH = Path(__file__).parent / "fixtures/mqtt_setup_v1_vectors.json"
VECTORS = json.loads(FIXTURE_PATH.read_text())
SETUP_ID = "12345678-1234-4abc-8def-1234567890ab"
REQUEST = {"schema_version": 1, "setup_id": SETUP_ID, "nonce": "panel-nonce"}
RESULT = {
    "schema_version": 1,
    "setup_id": SETUP_ID,
    "nonce": "ha-nonce",
    "reply_to_nonce": "panel-nonce",
}
PREFLIGHT_REQUEST = {
    "schema_version": 1,
    "setup_id": SETUP_ID,
    "panel_nonce": "panel-nonce",
    "ha_nonce": "ha-nonce",
    "timeout_seconds": 10.0,
}
SUCCESS_REPORT = {
    "completed_stages": [stage.value for stage in PreflightStage],
    "last_stage": "cleanup",
    "schema_version": 1,
    "setup_id": SETUP_ID,
    "stage_elapsed_ms": {stage.value: 1 for stage in PreflightStage},
    "success": True,
}


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def test_golden_vectors_match_topics_and_canonical_payloads() -> None:
    assert VECTORS["schema_version"] == 1
    for vector in VECTORS["vectors"]:
        setup_id = UUID(vector["setup_id"])
        request = SetupRequest.from_payload(_canonical(vector["request"]))
        result = SetupResult.from_payload(_canonical(vector["result"]).encode())

        assert asdict(SetupTopics.for_id(setup_id)) == vector["topics"]
        assert request == SetupRequest(setup_id=setup_id, nonce=vector["request"]["nonce"])
        assert result == SetupResult(
            setup_id=setup_id,
            nonce=vector["result"]["nonce"],
            reply_to_nonce=vector["result"]["reply_to_nonce"],
        )
        assert request.to_payload() == _canonical(vector["request"])
        assert result.to_payload() == _canonical(vector["result"])


def test_payload_output_canonicalizes_setup_id() -> None:
    value = dict(REQUEST)
    value["setup_id"] = SETUP_ID.upper()

    assert SetupRequest.from_payload(_canonical(value)).to_payload() == _canonical(REQUEST)


@pytest.mark.parametrize(
    ("contract_type", "payload", "error_prefix"),
    [
        (SetupRequest, {**REQUEST, "setup_id": "not-a-uuid"}, "invalid_setup_id"),
        (SetupResult, {**RESULT, "setup_id": "not-a-uuid"}, "invalid_setup_id"),
        (SetupRequest, {**REQUEST, "schema_version": 2}, "unsupported_setup_schema"),
        (SetupResult, {**RESULT, "schema_version": 2}, "unsupported_setup_schema"),
        (SetupRequest, {**REQUEST, "extra": True}, "invalid_setup_payload"),
        (SetupResult, {**RESULT, "extra": True}, "invalid_setup_payload"),
        (SetupRequest, {"schema_version": 1, "setup_id": SETUP_ID}, "invalid_setup_payload"),
        (
            SetupResult,
            {"schema_version": 1, "setup_id": SETUP_ID, "nonce": "ha-nonce"},
            "invalid_setup_payload",
        ),
        (SetupRequest, {**REQUEST, "nonce": ""}, "invalid_setup_payload"),
        (SetupResult, {**RESULT, "reply_to_nonce": ""}, "invalid_setup_payload"),
    ],
)
def test_payloads_reject_invalid_contract_shapes(
    contract_type: type[SetupRequest] | type[SetupResult],
    payload: dict[str, Any],
    error_prefix: str,
) -> None:
    with pytest.raises(ValueError, match=rf"^{error_prefix}"):
        contract_type.from_payload(_canonical(payload))


@pytest.mark.parametrize(
    ("contract_type", "payload", "field"),
    [
        (SetupRequest, REQUEST, "nonce"),
        (SetupResult, RESULT, "nonce"),
        (SetupResult, RESULT, "reply_to_nonce"),
    ],
)
@pytest.mark.parametrize("invalid_nonce", [None, 0, False])
def test_payloads_reject_non_string_nonces(
    contract_type: type[SetupRequest] | type[SetupResult],
    payload: dict[str, object],
    field: str,
    invalid_nonce: object,
) -> None:
    value = dict(payload)
    value[field] = invalid_nonce

    with pytest.raises(ValueError, match=r"^invalid_setup_payload"):
        contract_type.from_payload(_canonical(value))


def test_deeply_nested_json_maps_parser_recursion_to_stable_error() -> None:
    payload = (
        '{"schema_version":1,"setup_id":"'
        + SETUP_ID
        + '","nonce":'
        + "[" * 1_100
        + "null"
        + "]" * 1_100
        + "}"
    )

    with pytest.raises(ValueError, match=r"^invalid_setup_payload"):
        SetupRequest.from_payload(payload)


@pytest.mark.parametrize("contract_type", [SetupRequest, SetupResult])
def test_payloads_reject_16_385_bytes(
    contract_type: type[SetupRequest] | type[SetupResult],
) -> None:
    with pytest.raises(ValueError, match=r"^setup_payload_too_large"):
        contract_type.from_payload(b"x" * 16_385)


def test_topics_reject_non_v4_setup_id() -> None:
    with pytest.raises(ValueError, match=r"^invalid_setup_id"):
        SetupTopics.for_id(UUID("12345678-1234-1abc-8def-1234567890ab"))


@pytest.mark.parametrize(
    ("value", "field"),
    [
        (SetupTopics.for_id(UUID(SETUP_ID)), "panel_to_ha"),
        (SetupRequest(UUID(SETUP_ID), "panel-nonce"), "nonce"),
        (SetupResult(UUID(SETUP_ID), "ha-nonce", "panel-nonce"), "reply_to_nonce"),
    ],
)
def test_contract_objects_are_frozen_and_slotted(value: object, field: str) -> None:
    assert not hasattr(value, "__dict__")
    with pytest.raises(FrozenInstanceError):
        setattr(value, field, "changed")


def test_preflight_request_round_trips_exact_canonical_json() -> None:
    raw = _canonical(PREFLIGHT_REQUEST)

    request = PreflightRequest.from_json(raw)

    assert request.to_json() == raw
    assert request.setup_id == UUID(SETUP_ID)
    assert request.timeout_seconds == 10.0


@pytest.mark.parametrize(
    "raw",
    [
        json.dumps(PREFLIGHT_REQUEST),
        _canonical(PREFLIGHT_REQUEST) + "\n",
        _canonical({**PREFLIGHT_REQUEST, "extra": True}),
        _canonical({**PREFLIGHT_REQUEST, "panel_nonce": "x" * 257}),
        _canonical({**PREFLIGHT_REQUEST, "timeout_seconds": True}),
    ],
)
def test_preflight_request_rejects_noncanonical_or_unbounded_input(raw: str) -> None:
    with pytest.raises(ValueError, match=r"^invalid_preflight_request$"):
        PreflightRequest.from_json(raw)


def test_preflight_report_round_trips_exact_success_contract() -> None:
    raw = _canonical(SUCCESS_REPORT)

    report = PreflightReport.from_json(raw)

    assert report.to_json() == raw
    assert report.success is True
    assert report.completed_stages == tuple(PreflightStage)
    assert report.last_stage is PreflightStage.CLEANUP


def test_preflight_report_round_trips_exact_failure_contract() -> None:
    value = {
        "completed_stages": ["fleet_auth", "cleanup"],
        "detail": "MQTT stage timed out",
        "error_code": "mqtt_timeout",
        "failed_stage": "panel_to_ha",
        "schema_version": 1,
        "setup_id": SETUP_ID,
        "stage_elapsed_ms": {
            "cleanup": 1,
            "fleet_auth": 1,
            "panel_to_ha": 1,
        },
        "success": False,
    }
    raw = _canonical(value)

    report = PreflightReport.from_json(raw)

    assert report.to_json() == raw
    assert report.failed_stage is PreflightStage.PANEL_TO_HA
    assert report.error_code == "mqtt_timeout"


def test_preflight_report_accepts_fixed_settings_failure_contract() -> None:
    value = {
        "completed_stages": ["cleanup"],
        "detail": "Panel configuration invalid",
        "error_code": "settings_invalid",
        "failed_stage": "fleet_auth",
        "schema_version": 1,
        "setup_id": SETUP_ID,
        "stage_elapsed_ms": {
            "cleanup": 0,
            "fleet_auth": 0,
        },
        "success": False,
    }
    raw = _canonical(value)

    report = PreflightReport.from_json(raw)

    assert report.to_json() == raw
    assert report.error_code == "settings_invalid"


@pytest.mark.parametrize(
    "mutation",
    [
        {"extra": True},
        {"schema_version": 2},
        {"success": 1},
        {"completed_stages": ["fleet_auth", "ha_to_panel", "cleanup"]},
        {"completed_stages": [stage.value for stage in tuple(PreflightStage)[:-1]]},
        {"last_stage": "retained_message"},
        {"stage_elapsed_ms": {"cleanup": True}},
    ],
)
def test_preflight_report_rejects_wrong_schema_types_order_and_missing_cleanup(
    mutation: dict[str, object],
) -> None:
    value = dict(SUCCESS_REPORT)
    value.update(mutation)

    with pytest.raises(ValueError, match=r"^invalid_preflight_report$"):
        PreflightReport.from_json(_canonical(value))


@pytest.mark.parametrize(
    "raw",
    [
        json.dumps(SUCCESS_REPORT),
        _canonical(SUCCESS_REPORT) + "\n",
        "{}{}",
        "[]",
        b"x" * (MAX_PREFLIGHT_REPORT_BYTES + 1),
    ],
)
def test_preflight_report_rejects_noncanonical_excess_or_oversized_output(
    raw: bytes | str,
) -> None:
    with pytest.raises(ValueError, match=r"^invalid_preflight_report$"):
        PreflightReport.from_json(raw)


@pytest.mark.parametrize(
    ("error_code", "detail"),
    [
        ("mqtt_timeout", "raw timeout secret"),
        ("unknown", "MQTT stage timed out"),
        ("mqtt_authorization", "raw broker denial"),
    ],
)
def test_preflight_report_failure_detail_is_fixed_by_allowlisted_code(
    error_code: str,
    detail: str,
) -> None:
    value = {
        "completed_stages": ["fleet_auth", "cleanup"],
        "detail": detail,
        "error_code": error_code,
        "failed_stage": "panel_to_ha",
        "schema_version": 1,
        "setup_id": SETUP_ID,
        "stage_elapsed_ms": {
            "cleanup": 1,
            "fleet_auth": 1,
            "panel_to_ha": 1,
        },
        "success": False,
    }

    with pytest.raises(ValueError, match=r"^invalid_preflight_report$"):
        PreflightReport.from_json(_canonical(value))


@pytest.mark.parametrize(
    ("parser", "raw", "message"),
    [
        (
            PreflightRequest.from_json,
            '{"panel_nonce":"request-secret-do-not-retain"',
            "invalid_preflight_request",
        ),
        (
            PreflightReport.from_json,
            '{"detail":"report-secret-do-not-retain"',
            "invalid_preflight_report",
        ),
    ],
)
def test_preflight_parsers_drop_raw_json_exception_context(
    parser: Any,
    raw: str,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=rf"^{message}$") as raised:
        parser(raw)

    error = raised.value
    assert error.__cause__ is None
    assert error.__context__ is None
    surface = f"{error!r}\n{''.join(traceback.format_exception(error))}"
    assert raw not in surface
    assert "secret-do-not-retain" not in surface
