import json

from brilliant_protocol_lab.redaction import safe_id, sanitize


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


def test_embedded_hex_id_in_url_is_redacted_and_surrounding_text_preserved() -> None:
    device_id = "0123456789abcdef0123456789abcdef"
    url = f"https://api.brilliant.tech/device/{device_id}/info"
    result = sanitize(url)
    assert isinstance(result, str)
    assert device_id not in result
    assert safe_id(device_id) in result
    assert result == f"https://api.brilliant.tech/device/{safe_id(device_id)}/info"


def test_embedded_hex_id_mid_sentence_is_redacted() -> None:
    device_id = "0123456789abcdef0123456789abcdef"
    message = f"joined device {device_id} via bus"
    result = sanitize(message)
    assert isinstance(result, str)
    assert device_id not in result
    assert result == f"joined device {safe_id(device_id)} via bus"


def test_bare_hex_id_value_still_uses_safe_id() -> None:
    device_id = "0123456789abcdef0123456789abcdef"
    result = sanitize(device_id)
    assert result == safe_id(device_id)


def test_forty_hex_git_sha_like_run_is_not_partially_redacted() -> None:
    # A 40-hex-char run is NOT a 32-hex id; it must pass through untouched,
    # not have some 32-char substring of it swapped out.
    sha = "0123456789abcdef0123456789abcdef01234567"
    result = sanitize(f"commit {sha} applied")
    assert result == f"commit {sha} applied"
