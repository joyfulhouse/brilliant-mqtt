from __future__ import annotations

import base64
import json
import os
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import NoReturn, cast

import pytest

from tools.brilliant_vc.identity_materializer import (
    DecodedPKCS12,
    IdentityMaterializationError,
    decode_pkcs12,
    validate_and_materialize,
)

DEVICE_ID = "a" * 32
NOW_S = 1_800_000_000
DER = b"\x30\x03\x02\x01\x00"
KEY_PEM = b"-----BEGIN PRIVATE KEY-----\nprivate\n-----END PRIVATE KEY-----\n"
CERT_PEM = b"-----BEGIN CERTIFICATE-----\ncertificate\n-----END CERTIFICATE-----\n"


def _private_file(path: Path, data: bytes) -> None:
    path.write_bytes(data)
    path.chmod(0o600)


def _identity(tmp_path: Path, *, pkcs12_value: bytes | None = None) -> tuple[Path, Path]:
    identity_dir = tmp_path / "identity"
    certificate_dir = tmp_path / "certificates"
    identity_dir.mkdir(mode=0o700, parents=True)
    certificate_dir.mkdir(mode=0o700)
    _private_file(identity_dir / "device_id", DEVICE_ID.encode())
    _private_file(
        identity_dir / "pkcs12_certificate",
        pkcs12_value if pkcs12_value is not None else base64.b64encode(DER),
    )
    _private_file(identity_dir / "bootstrap", b"opaque-private-bootstrap")
    _private_file(
        identity_dir / "metadata.json",
        json.dumps(
            {
                "device_id_redacted": "aaaa…aaaa",
                "target_home_match": True,
            }
        ).encode(),
    )
    return identity_dir, certificate_dir


def _decoded() -> DecodedPKCS12:
    return DecodedPKCS12(
        private_key_pem=KEY_PEM,
        certificate_pem=CERT_PEM,
        certificate_fingerprint_sha256="b" * 64,
        common_names=(f"{DEVICE_ID}.device.brilliant.tech",),
        key_matches_certificate=True,
        not_before_s=NOW_S - 60,
        not_after_s=NOW_S + 60,
        additional_certificate_count=0,
        certificate_is_ca=False,
    )


def test_dry_run_validates_without_writing_or_exposing_identity(tmp_path: Path) -> None:
    identity_dir, certificate_dir = _identity(tmp_path)
    decoded_inputs: list[bytes] = []

    def decoder(value: bytes) -> DecodedPKCS12:
        decoded_inputs.append(value)
        return _decoded()

    result = validate_and_materialize(
        identity_dir,
        certificate_dir,
        now_s=NOW_S,
        apply=False,
        decoder=decoder,
        required_uid=os.getuid(),
    )

    assert decoded_inputs == [DER]
    assert list(certificate_dir.iterdir()) == []
    assert result.to_public_dict() == {
        "dry_run": True,
        "identity_validated": True,
        "materialized": False,
        "certificate_file_count": 0,
        "device_id_redacted": "aaaa…aaaa",
        "certificate_fingerprint_redacted": "bbbbbbbb…bbbbbbbb",
        "additional_certificate_count": 0,
    }
    public = json.dumps(result.to_public_dict())
    assert DEVICE_ID not in public
    assert "private" not in public


def test_apply_writes_only_the_private_runtime_pair(tmp_path: Path) -> None:
    identity_dir, certificate_dir = _identity(tmp_path)

    result = validate_and_materialize(
        identity_dir,
        certificate_dir,
        now_s=NOW_S,
        apply=True,
        decoder=lambda value: _decoded(),
        required_uid=os.getuid(),
    )

    assert result.materialized is True
    assert result.certificate_file_count == 2
    assert {path.name for path in certificate_dir.iterdir()} == {"device.key", "device.cert"}
    assert (certificate_dir / "device.key").read_bytes() == KEY_PEM
    assert (certificate_dir / "device.cert").read_bytes() == CERT_PEM
    assert (certificate_dir / "device.key").stat().st_mode & 0o777 == 0o600
    assert (certificate_dir / "device.cert").stat().st_mode & 0o777 == 0o600


def test_invalid_base64_is_rejected_before_decoder(tmp_path: Path) -> None:
    identity_dir, certificate_dir = _identity(tmp_path, pkcs12_value=b"not base64!")

    def decoder(value: bytes) -> NoReturn:
        raise AssertionError("decoder must not be called")

    with pytest.raises(IdentityMaterializationError, match="base64"):
        validate_and_materialize(
            identity_dir,
            certificate_dir,
            now_s=NOW_S,
            apply=False,
            decoder=decoder,
            required_uid=os.getuid(),
        )


@pytest.mark.parametrize(
    ("decoded", "message"),
    [
        (replace(_decoded(), common_names=("wrong.device.brilliant.tech",)), "common name"),
        (replace(_decoded(), common_names=()), "common name"),
        (replace(_decoded(), key_matches_certificate=False), "private key"),
        (replace(_decoded(), not_before_s=NOW_S + 1), "not yet valid"),
        (replace(_decoded(), not_after_s=NOW_S), "expired"),
        (replace(_decoded(), not_before_s=cast(int, True)), "validity timestamp"),
        (replace(_decoded(), not_after_s=cast(int, "invalid")), "validity timestamp"),
        (replace(_decoded(), certificate_is_ca=True), "CA certificate"),
        (replace(_decoded(), additional_certificate_count=9), "certificate chain"),
        (replace(_decoded(), private_key_pem=b"not pem"), "private key PEM"),
        (replace(_decoded(), certificate_pem=b"not pem"), "certificate PEM"),
        (replace(_decoded(), certificate_fingerprint_sha256="not-a-hash"), "fingerprint"),
        (replace(_decoded(), certificate_fingerprint_sha256=cast(str, None)), "fingerprint"),
    ],
)
def test_certificate_contract_fails_closed(
    tmp_path: Path,
    decoded: DecodedPKCS12,
    message: str,
) -> None:
    identity_dir, certificate_dir = _identity(tmp_path)

    with pytest.raises(IdentityMaterializationError, match=message):
        validate_and_materialize(
            identity_dir,
            certificate_dir,
            now_s=NOW_S,
            apply=False,
            decoder=lambda value: decoded,
            required_uid=os.getuid(),
        )


def test_private_input_and_empty_output_contracts_are_enforced(tmp_path: Path) -> None:
    identity_dir, certificate_dir = _identity(tmp_path / "extra")
    _private_file(identity_dir / "unexpected", b"no")
    with pytest.raises(IdentityMaterializationError, match="exactly"):
        validate_and_materialize(
            identity_dir,
            certificate_dir,
            now_s=NOW_S,
            apply=False,
            decoder=lambda value: _decoded(),
            required_uid=os.getuid(),
        )

    identity_dir, certificate_dir = _identity(tmp_path / "mode")
    (identity_dir / "bootstrap").chmod(0o644)
    with pytest.raises(IdentityMaterializationError, match="0600"):
        validate_and_materialize(
            identity_dir,
            certificate_dir,
            now_s=NOW_S,
            apply=False,
            decoder=lambda value: _decoded(),
            required_uid=os.getuid(),
        )

    identity_dir, certificate_dir = _identity(tmp_path / "symlink")
    (identity_dir / "bootstrap").unlink()
    (identity_dir / "bootstrap").symlink_to(identity_dir / "pkcs12_certificate")
    with pytest.raises(IdentityMaterializationError, match="symlink"):
        validate_and_materialize(
            identity_dir,
            certificate_dir,
            now_s=NOW_S,
            apply=False,
            decoder=lambda value: _decoded(),
            required_uid=os.getuid(),
        )

    identity_dir, certificate_dir = _identity(tmp_path / "hardlink")
    (identity_dir / "bootstrap").unlink()
    os.link(identity_dir / "pkcs12_certificate", identity_dir / "bootstrap")
    with pytest.raises(IdentityMaterializationError, match="hard link"):
        validate_and_materialize(
            identity_dir,
            certificate_dir,
            now_s=NOW_S,
            apply=False,
            decoder=lambda value: _decoded(),
            required_uid=os.getuid(),
        )

    identity_dir, certificate_dir = _identity(tmp_path / "nonempty")
    _private_file(certificate_dir / "stale", b"stale")
    with pytest.raises(IdentityMaterializationError, match="empty"):
        validate_and_materialize(
            identity_dir,
            certificate_dir,
            now_s=NOW_S,
            apply=False,
            decoder=lambda value: _decoded(),
            required_uid=os.getuid(),
        )


def test_partial_materialization_is_rolled_back(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity_dir, certificate_dir = _identity(tmp_path)
    from tools.brilliant_vc import identity_materializer

    real_write = identity_materializer._exclusive_write
    calls = 0

    def fail_second_write(path: Path, data: bytes) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            path.write_bytes(b"partial-private-output")
            path.chmod(0o600)
            raise OSError("simulated write failure")
        real_write(path, data)

    monkeypatch.setattr(identity_materializer, "_exclusive_write", fail_second_write)
    with pytest.raises(IdentityMaterializationError, match="could not atomically materialize"):
        validate_and_materialize(
            identity_dir,
            certificate_dir,
            now_s=NOW_S,
            apply=True,
            decoder=lambda value: _decoded(),
            required_uid=os.getuid(),
        )
    assert list(certificate_dir.iterdir()) == []


def test_real_decoder_accepts_matching_unencrypted_pkcs12(tmp_path: Path) -> None:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives.serialization import pkcs12
    from cryptography.x509.oid import NameOID

    now = datetime.fromtimestamp(NOW_S, tz=timezone.utc)
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, f"{DEVICE_ID}.device.brilliant.tech")]
    )
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(minutes=1))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    p12 = pkcs12.serialize_key_and_certificates(
        name=None,
        key=key,
        cert=certificate,
        cas=None,
        encryption_algorithm=serialization.NoEncryption(),
    )
    identity_dir, certificate_dir = _identity(
        tmp_path,
        pkcs12_value=base64.b64encode(p12),
    )

    result = validate_and_materialize(
        identity_dir,
        certificate_dir,
        now_s=NOW_S,
        apply=True,
        decoder=decode_pkcs12,
        required_uid=os.getuid(),
    )

    assert result.materialized is True
    loaded_key = serialization.load_pem_private_key(
        (certificate_dir / "device.key").read_bytes(),
        password=None,
    )
    loaded_certificate = x509.load_pem_x509_certificate(
        (certificate_dir / "device.cert").read_bytes()
    )
    assert loaded_key.public_key().public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    ) == loaded_certificate.public_key().public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )
