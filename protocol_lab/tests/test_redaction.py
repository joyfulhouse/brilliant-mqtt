import json

from brilliant_protocol_lab.redaction import sanitize


def test_recursive_redaction_removes_secret_values_and_hashes_ids() -> None:
    result = sanitize(
        {
            "access_token": "header.payload.signature",
            "device_id": "0123456789abcdef0123456789abcdef",
            "method": "join_home",
            "blob": b"private bytes",
        }
    )
    assert isinstance(result, dict)
    encoded = json.dumps(result)
    assert "header.payload.signature" not in encoded
    assert "0123456789abcdef0123456789abcdef" not in encoded
    assert "private bytes" not in encoded
    assert result["method"] == "join_home"
