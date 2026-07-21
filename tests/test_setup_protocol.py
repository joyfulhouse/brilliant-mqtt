from __future__ import annotations

import json
from dataclasses import FrozenInstanceError, asdict
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from brilliant_mqtt.setup_protocol import SetupRequest, SetupResult, SetupTopics

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
