"""Validate and materialize an officially provisioned Virtual Control identity.

The Brilliant provisioning endpoint returns a base64-encoded PKCS#12 value,
while the captured runtime opens ``device.key`` and ``device.cert`` PEM files.
This tool performs that conversion in a private, isolated directory. It has no
network, message-bus, process-launch, or panel-control capability.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import json
import os
import re
import secrets
import stat
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_IDENTITY_FILES = frozenset({"device_id", "pkcs12_certificate", "bootstrap", "metadata.json"})
_DEVICE_ID = re.compile(r"^[0-9a-f]{32}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_MAX_IDENTITY_BYTES = 1024 * 1024
_MAX_METADATA_BYTES = 64 * 1024
_MAX_PEM_BYTES = 128 * 1024
_MAX_ADDITIONAL_CERTIFICATES = 8
_PRIVATE_KEY_BEGIN = b"-----BEGIN PRIVATE KEY-----\n"
_PRIVATE_KEY_END = b"-----END PRIVATE KEY-----\n"
_CERTIFICATE_BEGIN = b"-----BEGIN CERTIFICATE-----\n"
_CERTIFICATE_END = b"-----END CERTIFICATE-----\n"


class IdentityMaterializationError(ValueError):
    """Raised when private identity input or certificate output is unsafe."""


@dataclass(frozen=True, slots=True)
class DecodedPKCS12:
    """Security-relevant fields extracted from a PKCS#12 container."""

    private_key_pem: bytes = field(repr=False)
    certificate_pem: bytes = field(repr=False)
    certificate_fingerprint_sha256: str
    common_names: tuple[str, ...]
    key_matches_certificate: bool
    not_before_s: int
    not_after_s: int
    additional_certificate_count: int
    certificate_is_ca: bool


PKCS12Decoder = Callable[[bytes], DecodedPKCS12]


@dataclass(frozen=True, slots=True)
class MaterializationResult:
    """Redacted result safe to print in a private gate log."""

    dry_run: bool
    identity_validated: bool
    materialized: bool
    certificate_file_count: int
    device_id_redacted: str
    certificate_fingerprint_redacted: str
    additional_certificate_count: int

    def to_public_dict(self) -> dict[str, object]:
        return {
            "dry_run": self.dry_run,
            "identity_validated": self.identity_validated,
            "materialized": self.materialized,
            "certificate_file_count": self.certificate_file_count,
            "device_id_redacted": self.device_id_redacted,
            "certificate_fingerprint_redacted": self.certificate_fingerprint_redacted,
            "additional_certificate_count": self.additional_certificate_count,
        }


def validate_and_materialize(
    identity_dir: Path,
    certificate_dir: Path,
    *,
    now_s: int,
    apply: bool,
    decoder: PKCS12Decoder | None = None,
    required_uid: int = 0,
) -> MaterializationResult:
    """Validate the official identity and optionally write two PEM files."""

    if decoder is None:
        decoder = decode_pkcs12
    _private_directory(identity_dir, description="identity directory", required_uid=required_uid)
    _private_directory(
        certificate_dir,
        description="certificate directory",
        required_uid=required_uid,
    )
    if identity_dir.resolve(strict=True) == certificate_dir.resolve(strict=True):
        raise IdentityMaterializationError("identity and certificate directories must be distinct")
    if any(certificate_dir.iterdir()):
        raise IdentityMaterializationError("certificate directory must be empty")

    entries = {entry.name: entry for entry in identity_dir.iterdir()}
    if set(entries) != _IDENTITY_FILES:
        raise IdentityMaterializationError(
            "identity directory must contain exactly four expected files"
        )
    for name, path in entries.items():
        _validate_private_file(
            path,
            description=f"identity {name}",
            required_uid=required_uid,
            maximum_bytes=(
                _MAX_METADATA_BYTES
                if name in {"device_id", "metadata.json"}
                else _MAX_IDENTITY_BYTES
            ),
        )

    device_id = _load_device_id(entries["device_id"], required_uid=required_uid)
    redacted_device_id = _redact(device_id)
    _validate_metadata(
        entries["metadata.json"],
        redacted_device_id=redacted_device_id,
        required_uid=required_uid,
    )
    encoded = _read_private_file(
        entries["pkcs12_certificate"],
        required_uid=required_uid,
        maximum_bytes=_MAX_IDENTITY_BYTES,
    )
    decoded_buffer = bytearray()
    try:
        try:
            decoded_buffer.extend(base64.b64decode(bytes(encoded).strip(), validate=True))
        except (binascii.Error, ValueError):
            raise IdentityMaterializationError("PKCS#12 identity is not strict base64") from None
        if not decoded_buffer or len(decoded_buffer) > _MAX_IDENTITY_BYTES:
            raise IdentityMaterializationError("decoded PKCS#12 identity has an invalid size")
        try:
            material = decoder(bytes(decoded_buffer))
        except IdentityMaterializationError:
            raise
        except Exception:
            raise IdentityMaterializationError("PKCS#12 identity could not be decoded") from None
    finally:
        _wipe(encoded)
        _wipe(decoded_buffer)

    _validate_decoded(material, device_id=device_id, now_s=now_s)
    if apply:
        _materialize(certificate_dir, material)
    return MaterializationResult(
        dry_run=not apply,
        identity_validated=True,
        materialized=apply,
        certificate_file_count=2 if apply else 0,
        device_id_redacted=redacted_device_id,
        certificate_fingerprint_redacted=_redact_fingerprint(
            material.certificate_fingerprint_sha256
        ),
        additional_certificate_count=material.additional_certificate_count,
    )


def decode_pkcs12(value: bytes) -> DecodedPKCS12:
    """Decode the pinned firmware's null-password PKCS#12 format."""

    try:
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.serialization import pkcs12
        from cryptography.x509.oid import NameOID
    except ImportError:
        raise IdentityMaterializationError(
            "the firmware cryptography package is unavailable"
        ) from None

    try:
        private_key, certificate, additional = pkcs12.load_key_and_certificates(value, None)
    except (TypeError, ValueError):
        raise IdentityMaterializationError(
            "PKCS#12 is not readable with the firmware's null-password contract"
        ) from None
    if private_key is None or certificate is None:
        raise IdentityMaterializationError("PKCS#12 lacks a private key or leaf certificate")

    try:
        private_key_pem = private_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        certificate_pem = certificate.public_bytes(serialization.Encoding.PEM)
        key_public = private_key.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        certificate_public = certificate.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        common_names = tuple(
            attribute.value
            for attribute in certificate.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
            if isinstance(attribute.value, str)
        )
        try:
            basic_constraints = certificate.extensions.get_extension_for_class(
                x509.BasicConstraints
            ).value
            certificate_is_ca = bool(basic_constraints.ca)
        except x509.ExtensionNotFound:
            certificate_is_ca = False
    except Exception:
        raise IdentityMaterializationError("PKCS#12 certificate fields are unsupported") from None

    return DecodedPKCS12(
        private_key_pem=private_key_pem,
        certificate_pem=certificate_pem,
        certificate_fingerprint_sha256=certificate.fingerprint(hashes.SHA256()).hex(),
        common_names=common_names,
        key_matches_certificate=secrets.compare_digest(key_public, certificate_public),
        not_before_s=_utc_timestamp(certificate.not_valid_before),
        not_after_s=_utc_timestamp(certificate.not_valid_after),
        additional_certificate_count=len(additional or ()),
        certificate_is_ca=certificate_is_ca,
    )


def _validate_decoded(material: DecodedPKCS12, *, device_id: str, now_s: int) -> None:
    if not isinstance(material, DecodedPKCS12):
        raise IdentityMaterializationError("PKCS#12 decoder returned an invalid result")
    if not _valid_pem(
        material.private_key_pem,
        begin=_PRIVATE_KEY_BEGIN,
        end=_PRIVATE_KEY_END,
    ):
        raise IdentityMaterializationError("decoded private key PEM is invalid")
    if not _valid_pem(
        material.certificate_pem,
        begin=_CERTIFICATE_BEGIN,
        end=_CERTIFICATE_END,
    ):
        raise IdentityMaterializationError("decoded certificate PEM is invalid")
    if (
        not isinstance(material.certificate_fingerprint_sha256, str)
        or _SHA256.fullmatch(material.certificate_fingerprint_sha256) is None
    ):
        raise IdentityMaterializationError("certificate fingerprint is invalid")
    expected_common_name = f"{device_id}.device.brilliant.tech"
    if material.common_names != (expected_common_name,):
        raise IdentityMaterializationError("certificate common name does not match device ID")
    if material.key_matches_certificate is not True:
        raise IdentityMaterializationError("private key does not match certificate")
    if material.certificate_is_ca is not False:
        raise IdentityMaterializationError("refusing a CA certificate as a device identity")
    if any(
        isinstance(value, bool) or not isinstance(value, int)
        for value in (material.not_before_s, material.not_after_s)
    ):
        raise IdentityMaterializationError("certificate validity timestamp is invalid")
    if material.not_before_s > now_s:
        raise IdentityMaterializationError("device certificate is not yet valid")
    if material.not_after_s <= now_s:
        raise IdentityMaterializationError("device certificate is expired")
    if (
        isinstance(material.additional_certificate_count, bool)
        or not isinstance(material.additional_certificate_count, int)
        or not 0 <= material.additional_certificate_count <= _MAX_ADDITIONAL_CERTIFICATES
    ):
        raise IdentityMaterializationError("PKCS#12 certificate chain is unreasonably large")


def _valid_pem(value: bytes, *, begin: bytes, end: bytes) -> bool:
    return (
        isinstance(value, bytes)
        and 0 < len(value) <= _MAX_PEM_BYTES
        and value.startswith(begin)
        and value.endswith(end)
    )


def _materialize(certificate_dir: Path, material: DecodedPKCS12) -> None:
    if any(certificate_dir.iterdir()):
        raise IdentityMaterializationError("certificate directory changed and is no longer empty")
    outputs = (
        (certificate_dir / "device.key", material.private_key_pem),
        (certificate_dir / "device.cert", material.certificate_pem),
    )
    try:
        for path, data in outputs:
            _exclusive_write(path, data)
        descriptor = os.open(
            certificate_dir,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
        )
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except BaseException as exc:
        for path, _data in outputs:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
        if isinstance(exc, OSError):
            raise IdentityMaterializationError(
                "could not atomically materialize certificate files"
            ) from None
        raise


def _exclusive_write(path: Path, data: bytes) -> None:
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    descriptor = os.open(path, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        view = memoryview(data)
        written = 0
        while written < len(view):
            count = os.write(descriptor, view[written:])
            if count <= 0:
                raise OSError("short write while materializing identity")
            written += count
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _private_directory(path: Path, *, description: str, required_uid: int) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        raise IdentityMaterializationError(f"{description} does not exist") from None
    if stat.S_ISLNK(metadata.st_mode):
        raise IdentityMaterializationError(f"{description} must not be a symlink")
    if not stat.S_ISDIR(metadata.st_mode):
        raise IdentityMaterializationError(f"{description} must be a directory")
    if metadata.st_uid != required_uid or stat.S_IMODE(metadata.st_mode) != 0o700:
        raise IdentityMaterializationError(
            f"{description} must have the required owner and mode 0700"
        )


def _validate_private_file(
    path: Path,
    *,
    description: str,
    required_uid: int,
    maximum_bytes: int,
) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        raise IdentityMaterializationError(f"{description} does not exist") from None
    if stat.S_ISLNK(metadata.st_mode):
        raise IdentityMaterializationError(f"{description} must not be a symlink")
    if not stat.S_ISREG(metadata.st_mode):
        raise IdentityMaterializationError(f"{description} must be a regular file")
    if metadata.st_uid != required_uid or stat.S_IMODE(metadata.st_mode) != 0o600:
        raise IdentityMaterializationError(
            f"{description} must have the required owner and mode 0600"
        )
    if metadata.st_nlink != 1:
        raise IdentityMaterializationError(f"{description} must not be a hard link")
    if not 0 < metadata.st_size <= maximum_bytes:
        raise IdentityMaterializationError(f"{description} has an invalid size")


def _read_private_file(path: Path, *, required_uid: int, maximum_bytes: int) -> bytearray:
    _validate_private_file(
        path,
        description=path.name,
        required_uid=required_uid,
        maximum_bytes=maximum_bytes,
    )
    before = path.lstat()
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        raise IdentityMaterializationError("could not safely open private identity file") from None
    data = bytearray()
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise IdentityMaterializationError("private identity file changed during open")
        while True:
            chunk = os.read(descriptor, min(8192, maximum_bytes + 1 - len(data)))
            if not chunk:
                break
            data.extend(chunk)
            if len(data) > maximum_bytes:
                raise IdentityMaterializationError("private identity file exceeds its size bound")
        return data
    except BaseException:
        _wipe(data)
        raise
    finally:
        os.close(descriptor)


def _load_device_id(path: Path, *, required_uid: int) -> str:
    raw = _read_private_file(
        path,
        required_uid=required_uid,
        maximum_bytes=_MAX_METADATA_BYTES,
    )
    try:
        try:
            device_id = bytes(raw).strip().decode("ascii")
        except UnicodeDecodeError:
            raise IdentityMaterializationError("identity device ID is not ASCII") from None
        if _DEVICE_ID.fullmatch(device_id) is None:
            raise IdentityMaterializationError("identity device ID is invalid")
        return device_id
    finally:
        _wipe(raw)


def _validate_metadata(path: Path, *, redacted_device_id: str, required_uid: int) -> None:
    raw = _read_private_file(
        path,
        required_uid=required_uid,
        maximum_bytes=_MAX_METADATA_BYTES,
    )
    try:
        try:
            parsed: Any = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise IdentityMaterializationError("identity metadata is invalid JSON") from None
    finally:
        _wipe(raw)
    if not isinstance(parsed, dict) or set(parsed) != {
        "device_id_redacted",
        "target_home_match",
    }:
        raise IdentityMaterializationError("identity metadata schema is invalid")
    if (
        parsed["device_id_redacted"] != redacted_device_id
        or parsed["target_home_match"] is not True
    ):
        raise IdentityMaterializationError("identity metadata does not match provisioned identity")


def _utc_timestamp(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp())


def _redact(value: str) -> str:
    return f"{value[:4]}…{value[-4:]}"


def _redact_fingerprint(value: str) -> str:
    return f"{value[:8]}…{value[-8:]}"


def _wipe(value: bytearray) -> None:
    for index in range(len(value)):
        value[index] = 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--identity-dir",
        type=Path,
        default=Path("/data/brilliant-vc/identity"),
    )
    parser.add_argument(
        "--certificate-dir",
        type=Path,
        default=Path("/data/brilliant-vc/certificates"),
    )
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    result = validate_and_materialize(
        args.identity_dir,
        args.certificate_dir,
        now_s=int(datetime.now(tz=timezone.utc).timestamp()),
        apply=args.apply,
        required_uid=os.geteuid(),
    )
    print(json.dumps(result.to_public_dict(), sort_keys=True))
    if not args.apply:
        print("DRY RUN — no certificate files written")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except IdentityMaterializationError as exc:
        print(f"VC identity materialization blocked: {exc}", file=sys.stderr)
        sys.exit(2)
