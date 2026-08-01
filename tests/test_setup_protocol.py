"""Unit tests for the versioned MQTT setup wire contract (setup_protocol.py)."""

from __future__ import annotations

import json
from uuid import uuid1, uuid4

import pytest

from brilliant_mqtt.setup_protocol import (
    MAX_SETUP_PAYLOAD_BYTES,
    SCHEMA_VERSION,
    SetupRequest,
    SetupResult,
    SetupTopics,
)


def _uuid4_str() -> str:
    return str(uuid4())


# -- SetupTopics --------------------------------------------------------------


def test_setup_topics_for_id_builds_expected_shape() -> None:
    setup_id = uuid4()
    topics = SetupTopics.for_id(setup_id)

    canonical = str(setup_id)
    assert topics.panel_to_ha == f"brilliant/setup/{canonical}/panel_to_ha"
    assert topics.ha_to_panel == f"brilliant/setup/{canonical}/ha_to_panel"
    assert topics.retained == f"brilliant/setup/{canonical}/retained"
    assert topics.discovery_probe == f"homeassistant/brilliant_mqtt_setup/{canonical}/probe"


def test_setup_topics_for_id_rejects_non_v4_uuid() -> None:
    with pytest.raises(ValueError, match="invalid_setup_id"):
        SetupTopics.for_id(uuid1())


# -- SetupRequest ---------------------------------------------------------------


def test_setup_request_round_trip_via_str_payload() -> None:
    setup_id = uuid4()
    request = SetupRequest(setup_id, "panel-nonce-1")

    parsed = SetupRequest.from_payload(request.to_payload())

    assert parsed == request


def test_setup_request_round_trip_via_bytes_payload() -> None:
    request = SetupRequest(uuid4(), "panel-nonce-bytes")

    parsed = SetupRequest.from_payload(request.to_payload().encode("utf-8"))

    assert parsed == request


def test_setup_request_to_payload_is_canonical_compact_json() -> None:
    setup_id = uuid4()
    request = SetupRequest(setup_id, "n1")

    assert request.to_payload() == json.dumps(
        {"schema_version": SCHEMA_VERSION, "setup_id": str(setup_id), "nonce": "n1"},
        sort_keys=True,
        separators=(",", ":"),
    )


def test_setup_request_rejects_non_v4_setup_id() -> None:
    with pytest.raises(ValueError, match="invalid_setup_id"):
        SetupRequest(uuid1(), "n1")


def test_setup_request_rejects_empty_nonce() -> None:
    with pytest.raises(ValueError, match="invalid_setup_payload: nonce"):
        SetupRequest(uuid4(), "")


def test_setup_request_from_payload_rejects_non_string_nonce() -> None:
    raw = json.dumps({"schema_version": SCHEMA_VERSION, "setup_id": _uuid4_str(), "nonce": 123})
    with pytest.raises(ValueError, match="invalid_setup_payload: nonce"):
        SetupRequest.from_payload(raw)


def test_setup_request_from_payload_rejects_non_string_setup_id() -> None:
    raw = json.dumps({"schema_version": SCHEMA_VERSION, "setup_id": 42, "nonce": "n1"})
    with pytest.raises(ValueError, match="invalid_setup_id"):
        SetupRequest.from_payload(raw)


# -- SetupResult ------------------------------------------------------------------


def test_setup_result_round_trip() -> None:
    setup_id = uuid4()
    result = SetupResult(setup_id, "ha-nonce", "panel-nonce")

    parsed = SetupResult.from_payload(result.to_payload())

    assert parsed == result


def test_setup_result_to_payload_is_canonical_compact_json() -> None:
    setup_id = uuid4()
    result = SetupResult(setup_id, "n1", "n2")

    assert result.to_payload() == json.dumps(
        {
            "schema_version": SCHEMA_VERSION,
            "setup_id": str(setup_id),
            "nonce": "n1",
            "reply_to_nonce": "n2",
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def test_setup_result_rejects_non_v4_setup_id() -> None:
    with pytest.raises(ValueError, match="invalid_setup_id"):
        SetupResult(uuid1(), "n1", "n2")


def test_setup_result_rejects_empty_nonce() -> None:
    with pytest.raises(ValueError, match="invalid_setup_payload: nonce"):
        SetupResult(uuid4(), "", "n2")


def test_setup_result_rejects_empty_reply_to_nonce() -> None:
    with pytest.raises(ValueError, match="invalid_setup_payload: reply_to_nonce"):
        SetupResult(uuid4(), "n1", "")


# -- Shared payload envelope validation (_decode_payload / _payload_size) -----


def test_from_payload_rejects_malformed_json() -> None:
    with pytest.raises(ValueError, match="invalid_setup_payload: expected a JSON object"):
        SetupRequest.from_payload("{not json")


def test_from_payload_rejects_non_dict_json() -> None:
    with pytest.raises(ValueError, match="invalid_setup_payload: expected a JSON object"):
        SetupRequest.from_payload("[1, 2, 3]")


def test_from_payload_rejects_scalar_json() -> None:
    with pytest.raises(ValueError, match="invalid_setup_payload: expected a JSON object"):
        SetupResult.from_payload('"just a string"')


def test_from_payload_rejects_missing_key() -> None:
    raw = json.dumps({"schema_version": SCHEMA_VERSION, "setup_id": _uuid4_str()})
    with pytest.raises(ValueError, match="unexpected or missing keys"):
        SetupRequest.from_payload(raw)


def test_from_payload_rejects_extra_key() -> None:
    raw = json.dumps(
        {
            "schema_version": SCHEMA_VERSION,
            "setup_id": _uuid4_str(),
            "nonce": "n1",
            "unexpected": "x",
        }
    )
    with pytest.raises(ValueError, match="unexpected or missing keys"):
        SetupRequest.from_payload(raw)


def test_from_payload_rejects_result_wrong_keys() -> None:
    # A well-formed *request* payload is missing reply_to_nonce for a *result*.
    raw = SetupRequest(uuid4(), "n1").to_payload()
    with pytest.raises(ValueError, match="unexpected or missing keys"):
        SetupResult.from_payload(raw)


def test_from_payload_rejects_wrong_schema_version_value() -> None:
    raw = json.dumps({"schema_version": 2, "setup_id": _uuid4_str(), "nonce": "n1"})
    with pytest.raises(ValueError, match="unsupported_setup_schema"):
        SetupRequest.from_payload(raw)


def test_from_payload_rejects_schema_version_as_bool() -> None:
    # bool is an int subclass in Python; `type(x) is not int` must still reject it.
    raw_dict: dict[str, object] = {
        "schema_version": True,
        "setup_id": _uuid4_str(),
        "nonce": "n1",
    }
    with pytest.raises(ValueError, match="unsupported_setup_schema"):
        SetupRequest.from_payload(json.dumps(raw_dict))


def test_from_payload_rejects_schema_version_as_string() -> None:
    raw = json.dumps({"schema_version": "1", "setup_id": _uuid4_str(), "nonce": "n1"})
    with pytest.raises(ValueError, match="unsupported_setup_schema"):
        SetupRequest.from_payload(raw)


def test_from_payload_rejects_oversize_payload() -> None:
    raw = json.dumps(
        {"schema_version": SCHEMA_VERSION, "setup_id": _uuid4_str(), "nonce": "x" * 20_000}
    )
    assert len(raw.encode("utf-8")) > MAX_SETUP_PAYLOAD_BYTES
    with pytest.raises(ValueError, match="setup_payload_too_large"):
        SetupRequest.from_payload(raw)


def test_from_payload_rejects_bytes_at_the_size_boundary() -> None:
    # A raw bytes payload skips the JSON-decode step's own re-encode, so the
    # byte-length check must still be evaluated first.
    oversized = b"{" + b"x" * MAX_SETUP_PAYLOAD_BYTES + b"}"
    with pytest.raises(ValueError, match="setup_payload_too_large"):
        SetupRequest.from_payload(oversized)


def test_from_payload_rejects_lone_surrogate_payload() -> None:
    # A lone UTF-16 surrogate is a legal Python str but cannot be strict-UTF-8
    # encoded; the size check must fail closed rather than raise UnicodeEncodeError.
    with pytest.raises(ValueError, match="invalid_setup_payload: payload is not valid UTF-8"):
        SetupRequest.from_payload("\ud800")
