from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path

import pytest

from tools.brilliant_vc.token_check import (
    SELF_BOOTSTRAP_PATH,
    TokenCheckError,
    inspect_token,
    inspect_token_file,
)


def _segment(value: object) -> str:
    raw = json.dumps(value, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _token(claims: dict[str, object]) -> bytes:
    return f"{_segment({'alg': 'RS256'})}.{_segment(claims)}.syntheticsignature".encode()


def _claims(**overrides: object) -> dict[str, object]:
    claims: dict[str, object] = {
        "iss": "https://accounts.example.invalid",
        "aud": "brilliant-provisioning",
        "iat": 1_000,
        "exp": 2_000,
        "allowed_paths": [SELF_BOOTSTRAP_PATH],
    }
    claims.update(overrides)
    return claims


def test_account_graphql_token_does_not_pass_vc1() -> None:
    report = inspect_token(
        _token(_claims(allowed_paths=["/graphql"])),
        now_s=1_500,
    )

    assert report.jwt_shape is True
    assert report.time_valid is True
    assert report.allows_self_bootstrap is False
    assert report.vc1_claims_pass is False


def test_official_workflow_token_with_exact_path_passes_claims_check() -> None:
    raw = _token(_claims())

    report = inspect_token(raw, now_s=1_500)

    assert report.vc1_claims_pass is True
    assert report.claims_only is True
    assert report.fingerprint8 == hashlib.sha256(raw).hexdigest()[:8]
    assert report.issuer_sha256 == hashlib.sha256(b"https://accounts.example.invalid").hexdigest()
    assert report.audience_sha256 == hashlib.sha256(b"brilliant-provisioning").hexdigest()
    assert report.to_public_dict()["verification"] == "claims-only"
    assert raw.decode() not in json.dumps(report.to_public_dict())


@pytest.mark.parametrize(
    ("claims", "message"),
    [
        (_claims(exp=1_499), "expired"),
        (_claims(iat=1_600), "future"),
        (_claims(exp=None), "exp"),
        (_claims(iat=None), "iat"),
    ],
)
def test_invalid_token_times_fail(claims: dict[str, object], message: str) -> None:
    with pytest.raises(TokenCheckError, match=message):
        inspect_token(_token(claims), now_s=1_500)


@pytest.mark.parametrize(
    "raw",
    [b"not-a-jwt", b"a.b.c.d", b"!!!!.e30.signature", b"e30.bm90LWpzb24.signature"],
)
def test_malformed_tokens_are_rejected(raw: bytes) -> None:
    with pytest.raises(TokenCheckError):
        inspect_token(raw, now_s=1_500)


def test_audience_list_is_hashed_canonically() -> None:
    report = inspect_token(
        _token(_claims(aud=["second", "first"])),
        now_s=1_500,
    )

    assert report.audience_sha256 == hashlib.sha256(b'["first","second"]').hexdigest()


def test_token_file_requires_private_regular_file(tmp_path: Path) -> None:
    path = tmp_path / "bootstrap.token"
    path.write_bytes(_token(_claims()))
    path.chmod(0o640)

    with pytest.raises(TokenCheckError, match="mode 0600"):
        inspect_token_file(path, now_s=1_500, required_uid=os.getuid())


def test_token_file_rejects_symlink(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.write_bytes(_token(_claims()))
    target.chmod(0o600)
    link = tmp_path / "link"
    link.symlink_to(target)

    with pytest.raises(TokenCheckError, match="symlink"):
        inspect_token_file(link, now_s=1_500, required_uid=os.getuid())


def test_token_file_rejects_wrong_owner(tmp_path: Path) -> None:
    path = tmp_path / "bootstrap.token"
    path.write_bytes(_token(_claims()))
    path.chmod(0o600)

    with pytest.raises(TokenCheckError, match="owned by UID"):
        inspect_token_file(path, now_s=1_500, required_uid=os.getuid() + 1)


def test_token_file_is_capped_at_64_kib(tmp_path: Path) -> None:
    path = tmp_path / "bootstrap.token"
    path.write_bytes(b"A" * (65_536 + 1))
    path.chmod(0o600)

    with pytest.raises(TokenCheckError, match="64 KiB"):
        inspect_token_file(path, now_s=1_500, required_uid=os.getuid())


def test_private_token_file_returns_only_sanitized_report(tmp_path: Path) -> None:
    path = tmp_path / "bootstrap.token"
    raw = _token(_claims())
    path.write_bytes(raw)
    path.chmod(0o600)

    report = inspect_token_file(path, now_s=1_500, required_uid=os.getuid())

    serialized = json.dumps(report.to_public_dict(), sort_keys=True)
    assert report.vc1_claims_pass is True
    assert raw.decode() not in serialized
    assert "accounts.example.invalid" not in serialized
