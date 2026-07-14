from __future__ import annotations

import grp
import json
import os
import pwd
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import NoReturn

import pytest

from tools.brilliant_vc.runtime_handoff import (
    PEMIdentityValidator,
    RuntimeHandoffError,
    RuntimeHandoffPaths,
    RuntimeHandoffResult,
    handoff_runtime_credentials,
    main,
    runtime_credential_bundle_sha256,
    validate_pem_identity,
)

DEVICE_ID = "a" * 32
NOW_S = 1_800_000_000
KEY_PEM = b"-----BEGIN PRIVATE KEY-----\nprivate\n-----END PRIVATE KEY-----\n"
CERT_PEM = b"-----BEGIN CERTIFICATE-----\ncertificate\n-----END CERTIFICATE-----\n"
BOOTSTRAP = b"opaque-private-bootstrap"


def _private_file(path: Path, data: bytes) -> None:
    path.write_bytes(data)
    path.chmod(0o600)


def _paths(tmp_path: Path) -> RuntimeHandoffPaths:
    private_root = tmp_path / "private"
    identity = private_root / "identity"
    materialized = private_root / "materialized-certificates"
    private_root.mkdir(mode=0o700, parents=True)
    identity.mkdir(mode=0o700)
    materialized.mkdir(mode=0o700)
    _private_file(identity / "device_id", (DEVICE_ID + "\n").encode())
    _private_file(identity / "pkcs12_certificate", b"opaque-private-pkcs12")
    _private_file(identity / "bootstrap", BOOTSTRAP)
    _private_file(
        identity / "metadata.json",
        json.dumps(
            {
                "device_id_redacted": "aaaa…aaaa",
                "target_home_match": True,
            }
        ).encode(),
    )
    _private_file(materialized / "device.key", KEY_PEM)
    _private_file(materialized / "device.cert", CERT_PEM)
    return RuntimeHandoffPaths(
        private_root=private_root,
        identity_dir=identity,
        materialized_certificate_dir=materialized,
        runtime_credential_dir=tmp_path / "runtime-credentials",
    )


def _handoff(
    paths: RuntimeHandoffPaths,
    *,
    apply: bool,
    pair_validator: PEMIdentityValidator | None = None,
) -> RuntimeHandoffResult:
    validator = pair_validator or (lambda key, cert, device_id, now_s: "b" * 64)
    return handoff_runtime_credentials(
        paths,
        now_s=NOW_S,
        apply=apply,
        runtime_gid=os.getgid(),
        required_uid=os.getuid(),
        allowed_private_roots=(paths.private_root,),
        allowed_runtime_credential_paths=(paths.runtime_credential_dir,),
        pair_validator=validator,
    )


def test_dry_run_validates_sources_without_copying_or_exposing_identity(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    observed: list[tuple[bytes, bytes, str, int]] = []

    def validator(key: bytes, cert: bytes, device_id: str, now_s: int) -> str:
        observed.append((key, cert, device_id, now_s))
        return "b" * 64

    result = _handoff(paths, apply=False, pair_validator=validator)

    assert observed == [(KEY_PEM, CERT_PEM, DEVICE_ID, NOW_S)]
    assert not paths.runtime_credential_dir.exists()
    assert result.to_public_dict() == {
        "dry_run": True,
        "sources_validated": True,
        "handoff_complete": False,
        "already_complete": False,
        "runtime_file_count": 0,
        "device_id_redacted": "aaaa…aaaa",
        "certificate_fingerprint_redacted": "bbbbbbbb…bbbbbbbb",
        "bootstrap_sha256_redacted": result.bootstrap_sha256_redacted,
        "runtime_credential_bundle_sha256": result.runtime_credential_bundle_sha256,
    }
    public = json.dumps(result.to_public_dict())
    assert DEVICE_ID not in public
    assert "private" not in public
    assert len(result.bootstrap_sha256_redacted) == 17
    assert result.runtime_credential_bundle_sha256 == runtime_credential_bundle_sha256(
        {
            "device_id": (DEVICE_ID + "\n").encode(),
            "bootstrap": BOOTSTRAP,
            "device.key": KEY_PEM,
            "device.cert": CERT_PEM,
        }
    )
    assert len(result.runtime_credential_bundle_sha256) == 64


def test_apply_creates_only_root_owned_group_readable_runtime_inputs(tmp_path: Path) -> None:
    paths = _paths(tmp_path)

    result = _handoff(paths, apply=True)

    certificate_dir = paths.runtime_credential_dir / "certificates"
    assert result.handoff_complete is True
    assert result.runtime_file_count == 4
    assert {path.name for path in paths.runtime_credential_dir.iterdir()} == {
        "device_id",
        "bootstrap",
        "certificates",
    }
    assert {path.name for path in certificate_dir.iterdir()} == {"device.key", "device.cert"}
    assert (paths.runtime_credential_dir / "device_id").read_bytes() == (DEVICE_ID + "\n").encode()
    assert (paths.runtime_credential_dir / "bootstrap").read_bytes() == BOOTSTRAP
    assert (certificate_dir / "device.key").read_bytes() == KEY_PEM
    assert (certificate_dir / "device.cert").read_bytes() == CERT_PEM
    for directory in (paths.runtime_credential_dir, certificate_dir):
        metadata = directory.stat()
        assert metadata.st_uid == os.getuid()
        assert metadata.st_gid == os.getgid()
        assert metadata.st_mode & 0o777 == 0o750
    for path in (
        paths.runtime_credential_dir / "device_id",
        paths.runtime_credential_dir / "bootstrap",
        certificate_dir / "device.key",
        certificate_dir / "device.cert",
    ):
        metadata = path.stat()
        assert metadata.st_uid == os.getuid()
        assert metadata.st_gid == os.getgid()
        assert metadata.st_mode & 0o777 == 0o640


def test_apply_keeps_each_file_owner_only_while_writing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _paths(tmp_path)
    real_write = os.write
    modes_during_write: list[int] = []

    def observe_write(descriptor: int, data: bytes | bytearray | memoryview) -> int:
        modes_during_write.append(os.fstat(descriptor).st_mode & 0o777)
        return real_write(descriptor, data)

    monkeypatch.setattr(os, "write", observe_write)

    _handoff(paths, apply=True)

    assert modes_during_write
    assert set(modes_during_write) == {0o600}


def test_matching_existing_handoff_is_idempotent_but_never_overwritten(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    first = _handoff(paths, apply=True)
    second = _handoff(paths, apply=True)

    assert first.already_complete is False
    assert second.already_complete is True
    assert second.handoff_complete is True

    (paths.runtime_credential_dir / "bootstrap").chmod(0o600)
    with pytest.raises(RuntimeHandoffError, match="runtime bootstrap"):
        _handoff(paths, apply=True)


def test_source_and_destination_boundaries_fail_closed(tmp_path: Path) -> None:
    paths = _paths(tmp_path / "extra")
    _private_file(paths.identity_dir / "unexpected", b"no")
    with pytest.raises(RuntimeHandoffError, match="exactly four"):
        _handoff(paths, apply=False)

    paths = _paths(tmp_path / "source-mode")
    (paths.materialized_certificate_dir / "device.key").chmod(0o644)
    with pytest.raises(RuntimeHandoffError, match="0600"):
        _handoff(paths, apply=False)

    paths = _paths(tmp_path / "source-link")
    (paths.identity_dir / "bootstrap").unlink()
    (paths.identity_dir / "bootstrap").symlink_to(paths.identity_dir / "device_id")
    with pytest.raises(RuntimeHandoffError, match="symlink"):
        _handoff(paths, apply=False)

    paths = _paths(tmp_path / "wrong-output")
    with pytest.raises(RuntimeHandoffError, match="allowed runtime credential path"):
        handoff_runtime_credentials(
            paths,
            now_s=NOW_S,
            apply=False,
            runtime_gid=os.getgid(),
            required_uid=os.getuid(),
            allowed_private_roots=(paths.private_root,),
            allowed_runtime_credential_paths=(tmp_path / "somewhere-else",),
            pair_validator=lambda key, cert, device_id, now_s: "b" * 64,
        )

    paths = _paths(tmp_path / "overlap")
    overlapping_destination = paths.private_root / "runtime-credentials"
    with pytest.raises(RuntimeHandoffError, match="must not overlap"):
        handoff_runtime_credentials(
            RuntimeHandoffPaths(
                private_root=paths.private_root,
                identity_dir=paths.identity_dir,
                materialized_certificate_dir=paths.materialized_certificate_dir,
                runtime_credential_dir=overlapping_destination,
            ),
            now_s=NOW_S,
            apply=False,
            runtime_gid=os.getgid(),
            required_uid=os.getuid(),
            allowed_private_roots=(paths.private_root,),
            allowed_runtime_credential_paths=(overlapping_destination,),
            pair_validator=lambda key, cert, device_id, now_s: "b" * 64,
        )


def test_partial_handoff_rolls_back_only_the_new_runtime_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _paths(tmp_path)
    from tools.brilliant_vc import runtime_handoff

    real_write = runtime_handoff._exclusive_runtime_write
    calls = 0

    def fail_second_write(path: Path, data: bytes | bytearray, *, uid: int, gid: int) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("simulated write failure")
        real_write(path, data, uid=uid, gid=gid)

    monkeypatch.setattr(runtime_handoff, "_exclusive_runtime_write", fail_second_write)
    with pytest.raises(RuntimeHandoffError, match="could not atomically hand off"):
        _handoff(paths, apply=True)

    assert not paths.runtime_credential_dir.exists()
    assert (paths.identity_dir / "bootstrap").read_bytes() == BOOTSTRAP


def test_default_pem_validator_accepts_only_matching_current_leaf_identity() -> None:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
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
    key_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    cert_pem = certificate.public_bytes(serialization.Encoding.PEM)

    assert len(validate_pem_identity(key_pem, cert_pem, DEVICE_ID, NOW_S)) == 64

    other_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    wrong_key_pem = other_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    with pytest.raises(RuntimeHandoffError, match="does not match"):
        validate_pem_identity(wrong_key_pem, cert_pem, DEVICE_ID, NOW_S)


def test_validator_exceptions_are_generic_and_do_not_leak_private_inputs(tmp_path: Path) -> None:
    paths = _paths(tmp_path)

    def fail(key: bytes, cert: bytes, device_id: str, now_s: int) -> NoReturn:
        raise RuntimeError(f"do not expose {device_id} {key!r}")

    with pytest.raises(RuntimeHandoffError, match="could not be validated") as captured:
        _handoff(paths, apply=False, pair_validator=fail)
    assert DEVICE_ID not in str(captured.value)
    assert "private" not in str(captured.value)


def test_already_loaded_secret_buffers_are_wiped_when_a_later_read_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _paths(tmp_path)
    from tools.brilliant_vc import runtime_handoff

    real_read = runtime_handoff._read_file
    loaded_bootstrap: list[bytearray] = []

    def fail_after_bootstrap(
        path: Path,
        *,
        description: str,
        uid: int,
        gid: int | None,
        mode: int,
        maximum_bytes: int,
    ) -> bytearray:
        if description == "materialized certificate device.key":
            raise RuntimeHandoffError("simulated later read failure")
        value = real_read(
            path,
            description=description,
            uid=uid,
            gid=gid,
            mode=mode,
            maximum_bytes=maximum_bytes,
        )
        if description == "identity bootstrap":
            loaded_bootstrap.append(value)
        return value

    monkeypatch.setattr(runtime_handoff, "_read_file", fail_after_bootstrap)

    with pytest.raises(RuntimeHandoffError, match="simulated later read failure"):
        _handoff(paths, apply=False)

    assert loaded_bootstrap == [bytearray(len(BOOTSTRAP))]


def test_cli_rejects_a_runtime_group_shared_with_another_account(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    account = SimpleNamespace(
        pw_name="brilliant-vc",
        pw_uid=1001,
        pw_gid=1001,
        pw_dir="/nonexistent",
        pw_shell="/usr/sbin/nologin",
    )
    group = SimpleNamespace(
        gr_name="brilliant-vc",
        gr_gid=1001,
        gr_mem=["another-user"],
    )
    monkeypatch.setattr(os, "geteuid", lambda: 0)
    monkeypatch.setattr(pwd, "getpwnam", lambda name: account)
    monkeypatch.setattr(grp, "getgrgid", lambda gid: group)
    monkeypatch.setattr(pwd, "getpwall", lambda: [account])
    monkeypatch.setattr(grp, "getgrall", lambda: [group])

    with pytest.raises(RuntimeHandoffError, match="must not include another account"):
        main([])
