"""Hand off validated Virtual Control credentials to a dedicated non-root runtime.

The source provisioning identity and materialized PEM pair remain root-only.
This tool copies only the canonical device ID, saved bootstrap blob, private
key, and leaf certificate into a root-owned, dedicated-group-readable runtime
directory. It has no network, firmware import, subprocess, socket, command
builder, or process-start capability.
"""

from __future__ import annotations

import argparse
import grp
import hashlib
import json
import os
import pwd
import re
import secrets
import stat
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from tools.brilliant_vc.launcher_preflight import (
    LauncherPreflightError,
    _validate_runtime_account_contract,
)

_RUNTIME_USER = "brilliant-vc"
_IDENTITY_FILES = frozenset({"device_id", "pkcs12_certificate", "bootstrap", "metadata.json"})
_CERTIFICATE_FILES = frozenset({"device.key", "device.cert"})
_RUNTIME_ENTRIES = frozenset({"device_id", "bootstrap", "certificates"})
_DEVICE_ID = re.compile(r"^[0-9a-f]{32}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_MAX_IDENTITY_BYTES = 1024 * 1024
_MAX_METADATA_BYTES = 64 * 1024
_MAX_PEM_BYTES = 128 * 1024
_PRIVATE_KEY_BEGIN = b"-----BEGIN PRIVATE KEY-----\n"
_PRIVATE_KEY_END = b"-----END PRIVATE KEY-----\n"
_CERTIFICATE_BEGIN = b"-----BEGIN CERTIFICATE-----\n"
_CERTIFICATE_END = b"-----END CERTIFICATE-----\n"
_DEFAULT_PRIVATE_ROOTS = (Path("/data/brilliant-vc-private"),)
_DEFAULT_RUNTIME_CREDENTIAL_PATHS = (Path("/data/brilliant-vc-credentials"),)
_RUNTIME_BUNDLE_ORDER = ("device_id", "bootstrap", "device.key", "device.cert")
_RUNTIME_BUNDLE_DOMAIN = b"brilliant-vc-runtime-credentials-v1\x00"


class RuntimeHandoffError(ValueError):
    """Raised when the source or runtime credential boundary is unsafe."""


PEMIdentityValidator = Callable[[bytes, bytes, str, int], str]


@dataclass(frozen=True, slots=True)
class RuntimeHandoffPaths:
    """Root-private inputs and the exact non-writable runtime credential root."""

    private_root: Path
    identity_dir: Path
    materialized_certificate_dir: Path
    runtime_credential_dir: Path


@dataclass(frozen=True, slots=True)
class RuntimeHandoffResult:
    """Secret-free handoff report suitable for a private gate ledger."""

    dry_run: bool
    sources_validated: bool
    handoff_complete: bool
    already_complete: bool
    runtime_file_count: int
    device_id_redacted: str
    certificate_fingerprint_redacted: str
    bootstrap_sha256_redacted: str
    runtime_credential_bundle_sha256: str

    def to_public_dict(self) -> dict[str, object]:
        return {
            "dry_run": self.dry_run,
            "sources_validated": self.sources_validated,
            "handoff_complete": self.handoff_complete,
            "already_complete": self.already_complete,
            "runtime_file_count": self.runtime_file_count,
            "device_id_redacted": self.device_id_redacted,
            "certificate_fingerprint_redacted": self.certificate_fingerprint_redacted,
            "bootstrap_sha256_redacted": self.bootstrap_sha256_redacted,
            "runtime_credential_bundle_sha256": self.runtime_credential_bundle_sha256,
        }


def runtime_credential_bundle_sha256(
    values: Mapping[str, bytes | bytearray],
) -> str:
    """Hash the exact four-file runtime bundle with names and lengths."""

    if set(values) != set(_RUNTIME_BUNDLE_ORDER):
        raise RuntimeHandoffError("runtime credential bundle inventory is invalid")
    digest = hashlib.sha256(_RUNTIME_BUNDLE_DOMAIN)
    for name in _RUNTIME_BUNDLE_ORDER:
        value = values[name]
        if not isinstance(value, (bytes, bytearray)) or not value:
            raise RuntimeHandoffError("runtime credential bundle value is invalid")
        encoded_name = name.encode("ascii")
        digest.update(len(encoded_name).to_bytes(2, "big"))
        digest.update(encoded_name)
        digest.update(len(value).to_bytes(8, "big"))
        digest.update(value)
    return digest.hexdigest()


def handoff_runtime_credentials(
    paths: RuntimeHandoffPaths,
    *,
    now_s: int,
    apply: bool,
    runtime_gid: int,
    pair_validator: PEMIdentityValidator | None = None,
    required_uid: int = 0,
    allowed_private_roots: Sequence[Path] = _DEFAULT_PRIVATE_ROOTS,
    allowed_runtime_credential_paths: Sequence[Path] = _DEFAULT_RUNTIME_CREDENTIAL_PATHS,
) -> RuntimeHandoffResult:
    """Validate private sources and optionally create the exact runtime copy."""

    if pair_validator is None:
        pair_validator = validate_pem_identity
    if (
        isinstance(now_s, bool)
        or not isinstance(now_s, int)
        or now_s <= 0
        or isinstance(runtime_gid, bool)
        or not isinstance(runtime_gid, int)
        or runtime_gid < 0
    ):
        raise RuntimeHandoffError("handoff timestamp or runtime group is invalid")

    _validate_path_topology(
        paths,
        required_uid=required_uid,
        allowed_private_roots=allowed_private_roots,
        allowed_runtime_credential_paths=allowed_runtime_credential_paths,
    )
    identity_entries = _exact_entries(
        paths.identity_dir,
        expected=_IDENTITY_FILES,
        description="identity directory",
    )
    certificate_entries = _exact_entries(
        paths.materialized_certificate_dir,
        expected=_CERTIFICATE_FILES,
        description="materialized certificate directory",
    )
    for name, path in identity_entries.items():
        _validate_file(
            path,
            description=f"identity {name}",
            uid=required_uid,
            gid=None,
            mode=0o600,
            maximum_bytes=(
                _MAX_METADATA_BYTES
                if name in {"device_id", "metadata.json"}
                else _MAX_IDENTITY_BYTES
            ),
        )
    for name, path in certificate_entries.items():
        _validate_file(
            path,
            description=f"materialized certificate {name}",
            uid=required_uid,
            gid=None,
            mode=0o600,
            maximum_bytes=_MAX_PEM_BYTES,
        )

    device_id = _load_device_id(identity_entries["device_id"], required_uid=required_uid)
    redacted_device_id = _redact(device_id)
    _validate_metadata(
        identity_entries["metadata.json"],
        redacted_device_id=redacted_device_id,
        required_uid=required_uid,
    )

    bootstrap = bytearray()
    private_key = bytearray()
    certificate = bytearray()
    runtime_device_id = bytearray()
    try:
        bootstrap = _read_file(
            identity_entries["bootstrap"],
            description="identity bootstrap",
            uid=required_uid,
            gid=None,
            mode=0o600,
            maximum_bytes=_MAX_IDENTITY_BYTES,
        )
        private_key = _read_file(
            certificate_entries["device.key"],
            description="materialized certificate device.key",
            uid=required_uid,
            gid=None,
            mode=0o600,
            maximum_bytes=_MAX_PEM_BYTES,
        )
        certificate = _read_file(
            certificate_entries["device.cert"],
            description="materialized certificate device.cert",
            uid=required_uid,
            gid=None,
            mode=0o600,
            maximum_bytes=_MAX_PEM_BYTES,
        )
        runtime_device_id = bytearray(f"{device_id}\n".encode("ascii"))
        try:
            fingerprint = pair_validator(
                bytes(private_key),
                bytes(certificate),
                device_id,
                now_s,
            )
        except RuntimeHandoffError:
            raise
        except Exception:
            raise RuntimeHandoffError("materialized PEM identity could not be validated") from None
        if not isinstance(fingerprint, str) or _SHA256.fullmatch(fingerprint) is None:
            raise RuntimeHandoffError("materialized certificate fingerprint is invalid")
        bootstrap_digest = hashlib.sha256(bootstrap).hexdigest()
        source_values = {
            "device_id": runtime_device_id,
            "bootstrap": bootstrap,
            "device.key": private_key,
            "device.cert": certificate,
        }
        bundle_digest = runtime_credential_bundle_sha256(source_values)
        already_complete = False
        if paths.runtime_credential_dir.exists() or paths.runtime_credential_dir.is_symlink():
            _validate_existing_runtime(
                paths.runtime_credential_dir,
                source_values=source_values,
                required_uid=required_uid,
                runtime_gid=runtime_gid,
            )
            already_complete = True
        elif apply:
            _materialize_runtime(
                paths.runtime_credential_dir,
                source_values=source_values,
                required_uid=required_uid,
                runtime_gid=runtime_gid,
            )
            _validate_existing_runtime(
                paths.runtime_credential_dir,
                source_values=source_values,
                required_uid=required_uid,
                runtime_gid=runtime_gid,
            )
        complete = apply or already_complete
        return RuntimeHandoffResult(
            dry_run=not apply,
            sources_validated=True,
            handoff_complete=complete,
            already_complete=already_complete,
            runtime_file_count=4 if complete else 0,
            device_id_redacted=redacted_device_id,
            certificate_fingerprint_redacted=_redact_digest(fingerprint),
            bootstrap_sha256_redacted=_redact_digest(bootstrap_digest),
            runtime_credential_bundle_sha256=bundle_digest,
        )
    finally:
        for value in (bootstrap, private_key, certificate, runtime_device_id):
            _wipe(value)


def validate_pem_identity(
    private_key_pem: bytes,
    certificate_pem: bytes,
    device_id: str,
    now_s: int,
) -> str:
    """Revalidate the materialized key/leaf pair at the handoff boundary."""

    if not _valid_pem(
        private_key_pem,
        begin=_PRIVATE_KEY_BEGIN,
        end=_PRIVATE_KEY_END,
    ):
        raise RuntimeHandoffError("materialized private key PEM is invalid")
    if not _valid_pem(
        certificate_pem,
        begin=_CERTIFICATE_BEGIN,
        end=_CERTIFICATE_END,
    ):
        raise RuntimeHandoffError("materialized certificate PEM is invalid")
    try:
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.x509.oid import NameOID

        private_key = serialization.load_pem_private_key(private_key_pem, password=None)
        certificate = x509.load_pem_x509_certificate(certificate_pem)
        key_public = private_key.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        certificate_public = certificate.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        if not secrets.compare_digest(key_public, certificate_public):
            raise RuntimeHandoffError("materialized private key does not match certificate")
        common_names = tuple(
            attribute.value
            for attribute in certificate.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
            if isinstance(attribute.value, str)
        )
        if common_names != (f"{device_id}.device.brilliant.tech",):
            raise RuntimeHandoffError("materialized certificate common name is invalid")
        try:
            constraints = certificate.extensions.get_extension_for_class(
                x509.BasicConstraints
            ).value
            if constraints.ca:
                raise RuntimeHandoffError("refusing a CA certificate as a runtime identity")
        except x509.ExtensionNotFound:
            pass
        if _timestamp(certificate.not_valid_before) > now_s:
            raise RuntimeHandoffError("materialized certificate is not yet valid")
        if _timestamp(certificate.not_valid_after) <= now_s:
            raise RuntimeHandoffError("materialized certificate is expired")
        return certificate.fingerprint(hashes.SHA256()).hex()
    except RuntimeHandoffError:
        raise
    except Exception:
        raise RuntimeHandoffError("materialized PEM identity could not be validated") from None


def _validate_path_topology(
    paths: RuntimeHandoffPaths,
    *,
    required_uid: int,
    allowed_private_roots: Sequence[Path],
    allowed_runtime_credential_paths: Sequence[Path],
) -> None:
    private_root = _directory(
        paths.private_root,
        description="private root",
        uid=required_uid,
        gid=None,
        mode=0o700,
    )
    if private_root not in {root.resolve(strict=False) for root in allowed_private_roots}:
        raise RuntimeHandoffError("private root is outside the allowed roots")
    identity = _directory(
        paths.identity_dir,
        description="identity directory",
        uid=required_uid,
        gid=None,
        mode=0o700,
    )
    materialized = _directory(
        paths.materialized_certificate_dir,
        description="materialized certificate directory",
        uid=required_uid,
        gid=None,
        mode=0o700,
    )
    if (
        identity.parent != private_root
        or materialized.parent != private_root
        or identity == materialized
    ):
        raise RuntimeHandoffError("private input directories must be distinct direct children")

    destination = paths.runtime_credential_dir.resolve(strict=False)
    if destination not in {path.resolve(strict=False) for path in allowed_runtime_credential_paths}:
        raise RuntimeHandoffError("destination is outside the allowed runtime credential path")
    if _paths_overlap(destination, private_root):
        raise RuntimeHandoffError("private and runtime credential roots must not overlap")
    parent = destination.parent
    resolved_parent = _directory(
        parent,
        description="runtime credential parent",
        uid=required_uid,
        gid=None,
        mode=None,
    )
    parent_mode = stat.S_IMODE(parent.lstat().st_mode)
    if resolved_parent != parent.resolve(strict=False) or parent_mode & 0o022:
        raise RuntimeHandoffError("runtime credential parent must not be group/world writable")


def _exact_entries(path: Path, *, expected: frozenset[str], description: str) -> dict[str, Path]:
    try:
        entries = {entry.name: entry for entry in path.iterdir()}
    except OSError:
        raise RuntimeHandoffError(f"could not inspect {description}") from None
    if set(entries) != expected:
        count = "four" if len(expected) == 4 else "two"
        raise RuntimeHandoffError(f"{description} must contain exactly {count} expected files")
    return entries


def _load_device_id(path: Path, *, required_uid: int) -> str:
    raw = _read_file(
        path,
        description="identity device_id",
        uid=required_uid,
        gid=None,
        mode=0o600,
        maximum_bytes=_MAX_METADATA_BYTES,
    )
    try:
        try:
            device_id = bytes(raw).strip().decode("ascii")
        except UnicodeDecodeError:
            raise RuntimeHandoffError("identity device ID is not ASCII") from None
        if _DEVICE_ID.fullmatch(device_id) is None:
            raise RuntimeHandoffError("identity device ID is invalid")
        return device_id
    finally:
        _wipe(raw)


def _validate_metadata(path: Path, *, redacted_device_id: str, required_uid: int) -> None:
    raw = _read_file(
        path,
        description="identity metadata.json",
        uid=required_uid,
        gid=None,
        mode=0o600,
        maximum_bytes=_MAX_METADATA_BYTES,
    )
    try:
        try:
            parsed: object = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise RuntimeHandoffError("identity metadata is invalid JSON") from None
    finally:
        _wipe(raw)
    if not isinstance(parsed, dict) or set(parsed) != {
        "device_id_redacted",
        "target_home_match",
    }:
        raise RuntimeHandoffError("identity metadata schema is invalid")
    if (
        parsed["device_id_redacted"] != redacted_device_id
        or parsed["target_home_match"] is not True
    ):
        raise RuntimeHandoffError("identity metadata does not match the provisioned identity")


def _materialize_runtime(
    path: Path,
    *,
    source_values: Mapping[str, bytearray],
    required_uid: int,
    runtime_gid: int,
) -> None:
    created = False
    certificate_dir = path / "certificates"
    try:
        os.mkdir(path, 0o750)
        created = True
        os.chown(path, required_uid, runtime_gid)
        os.chmod(path, 0o750)
        os.mkdir(certificate_dir, 0o750)
        os.chown(certificate_dir, required_uid, runtime_gid)
        os.chmod(certificate_dir, 0o750)
        _exclusive_runtime_write(
            path / "device_id",
            source_values["device_id"],
            uid=required_uid,
            gid=runtime_gid,
        )
        _exclusive_runtime_write(
            path / "bootstrap",
            source_values["bootstrap"],
            uid=required_uid,
            gid=runtime_gid,
        )
        _exclusive_runtime_write(
            certificate_dir / "device.key",
            source_values["device.key"],
            uid=required_uid,
            gid=runtime_gid,
        )
        _exclusive_runtime_write(
            certificate_dir / "device.cert",
            source_values["device.cert"],
            uid=required_uid,
            gid=runtime_gid,
        )
        _fsync_directory(certificate_dir)
        _fsync_directory(path)
        _fsync_directory(path.parent)
    except BaseException as error:
        if created:
            _rollback_created_runtime(path)
        if isinstance(error, RuntimeHandoffError):
            raise
        raise RuntimeHandoffError("could not atomically hand off runtime credentials") from None


def _exclusive_runtime_write(
    path: Path,
    data: bytes | bytearray,
    *,
    uid: int,
    gid: int,
) -> None:
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
                raise OSError("short runtime credential write")
            written += count
        os.fsync(descriptor)
        os.fchown(descriptor, uid, gid)
        os.fchmod(descriptor, 0o640)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _validate_existing_runtime(
    path: Path,
    *,
    source_values: Mapping[str, bytearray],
    required_uid: int,
    runtime_gid: int,
) -> None:
    runtime_root = _directory(
        path,
        description="runtime credential directory",
        uid=required_uid,
        gid=runtime_gid,
        mode=0o750,
    )
    entries = {entry.name: entry for entry in runtime_root.iterdir()}
    if set(entries) != _RUNTIME_ENTRIES:
        raise RuntimeHandoffError("runtime credential directory has unexpected entries")
    certificate_dir = _directory(
        entries["certificates"],
        description="runtime certificate directory",
        uid=required_uid,
        gid=runtime_gid,
        mode=0o750,
    )
    certificate_entries = _exact_entries(
        certificate_dir,
        expected=_CERTIFICATE_FILES,
        description="runtime certificate directory",
    )
    runtime_files = {
        "device_id": entries["device_id"],
        "bootstrap": entries["bootstrap"],
        "device.key": certificate_entries["device.key"],
        "device.cert": certificate_entries["device.cert"],
    }
    for name, runtime_path in runtime_files.items():
        expected = source_values[name]
        maximum_bytes = {
            "device_id": _MAX_METADATA_BYTES,
            "bootstrap": _MAX_IDENTITY_BYTES,
            "device.key": _MAX_PEM_BYTES,
            "device.cert": _MAX_PEM_BYTES,
        }[name]
        actual = _read_file(
            runtime_path,
            description=f"runtime {name}",
            uid=required_uid,
            gid=runtime_gid,
            mode=0o640,
            maximum_bytes=maximum_bytes,
        )
        try:
            if not secrets.compare_digest(actual, expected):
                raise RuntimeHandoffError(f"runtime {name} does not match its private source")
        finally:
            _wipe(actual)


def _directory(
    path: Path,
    *,
    description: str,
    uid: int,
    gid: int | None,
    mode: int | None,
) -> Path:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        raise RuntimeHandoffError(f"{description} does not exist") from None
    except OSError:
        raise RuntimeHandoffError(f"could not inspect {description}") from None
    if stat.S_ISLNK(metadata.st_mode):
        raise RuntimeHandoffError(f"{description} must not be a symlink")
    if not stat.S_ISDIR(metadata.st_mode):
        raise RuntimeHandoffError(f"{description} must be a directory")
    if metadata.st_uid != uid or (gid is not None and metadata.st_gid != gid):
        raise RuntimeHandoffError(f"{description} has the wrong owner or group")
    if mode is not None and stat.S_IMODE(metadata.st_mode) != mode:
        raise RuntimeHandoffError(f"{description} must have mode {mode:04o}")
    return path.resolve(strict=True)


def _validate_file(
    path: Path,
    *,
    description: str,
    uid: int,
    gid: int | None,
    mode: int,
    maximum_bytes: int,
) -> os.stat_result:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        raise RuntimeHandoffError(f"{description} does not exist") from None
    except OSError:
        raise RuntimeHandoffError(f"could not inspect {description}") from None
    if stat.S_ISLNK(metadata.st_mode):
        raise RuntimeHandoffError(f"{description} must not be a symlink")
    if not stat.S_ISREG(metadata.st_mode):
        raise RuntimeHandoffError(f"{description} must be a regular file")
    if metadata.st_uid != uid or (gid is not None and metadata.st_gid != gid):
        raise RuntimeHandoffError(f"{description} has the wrong owner or group")
    if stat.S_IMODE(metadata.st_mode) != mode:
        raise RuntimeHandoffError(f"{description} must have mode {mode:04o}")
    if metadata.st_nlink != 1:
        raise RuntimeHandoffError(f"{description} must not be a hard link")
    if not 0 < metadata.st_size <= maximum_bytes:
        raise RuntimeHandoffError(f"{description} has an invalid size")
    return metadata


def _read_file(
    path: Path,
    *,
    description: str,
    uid: int,
    gid: int | None,
    mode: int,
    maximum_bytes: int,
) -> bytearray:
    before = _validate_file(
        path,
        description=description,
        uid=uid,
        gid=gid,
        mode=mode,
        maximum_bytes=maximum_bytes,
    )
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        raise RuntimeHandoffError(f"could not safely open {description}") from None
    value = bytearray()
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise RuntimeHandoffError(f"{description} changed during open")
        while True:
            chunk = os.read(descriptor, min(8192, maximum_bytes + 1 - len(value)))
            if not chunk:
                break
            value.extend(chunk)
            if len(value) > maximum_bytes:
                raise RuntimeHandoffError(f"{description} exceeds its size bound")
        after = os.fstat(descriptor)
        if (
            after.st_size != opened.st_size
            or after.st_mtime_ns != opened.st_mtime_ns
            or after.st_ctime_ns != opened.st_ctime_ns
        ):
            raise RuntimeHandoffError(f"{description} changed while reading")
        return value
    except BaseException:
        _wipe(value)
        raise
    finally:
        os.close(descriptor)


def _rollback_created_runtime(path: Path) -> None:
    certificate_dir = path / "certificates"
    for candidate in (
        path / "device_id",
        path / "bootstrap",
        certificate_dir / "device.key",
        certificate_dir / "device.cert",
    ):
        try:
            candidate.unlink(missing_ok=True)
        except OSError:
            pass
    for directory in (certificate_dir, path):
        try:
            directory.rmdir()
        except OSError:
            pass


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _valid_pem(value: bytes, *, begin: bytes, end: bytes) -> bool:
    return (
        isinstance(value, bytes)
        and 0 < len(value) <= _MAX_PEM_BYTES
        and value.startswith(begin)
        and value.endswith(end)
        and value.count(begin) == 1
        and value.count(end) == 1
    )


def _paths_overlap(left: Path, right: Path) -> bool:
    try:
        left.relative_to(right)
        return True
    except ValueError:
        pass
    try:
        right.relative_to(left)
        return True
    except ValueError:
        return False


def _timestamp(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return int(value.timestamp())


def _redact(value: str) -> str:
    return f"{value[:4]}…{value[-4:]}"


def _redact_digest(value: str) -> str:
    return f"{value[:8]}…{value[-8:]}"


def _wipe(value: bytearray) -> None:
    for index in range(len(value)):
        value[index] = 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--private-root",
        type=Path,
        default=Path("/data/brilliant-vc-private"),
    )
    parser.add_argument(
        "--identity-dir",
        type=Path,
        default=Path("/data/brilliant-vc-private/identity"),
    )
    parser.add_argument(
        "--materialized-certificate-dir",
        type=Path,
        default=Path("/data/brilliant-vc-private/materialized-certificates"),
    )
    parser.add_argument(
        "--runtime-credential-dir",
        type=Path,
        default=Path("/data/brilliant-vc-credentials"),
    )
    parser.add_argument("--runtime-user", default=_RUNTIME_USER)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    if os.geteuid() != 0:
        raise RuntimeHandoffError("runtime credential handoff must run as root")
    if args.runtime_user != _RUNTIME_USER:
        raise RuntimeHandoffError("runtime user must match the pinned dedicated account")
    try:
        account = pwd.getpwnam(args.runtime_user)
    except KeyError:
        raise RuntimeHandoffError("dedicated runtime account does not exist") from None
    try:
        runtime_group = grp.getgrgid(account.pw_gid)
    except KeyError:
        raise RuntimeHandoffError("dedicated runtime group does not exist") from None
    try:
        _validate_runtime_account_contract(
            account,
            runtime_group,
            all_accounts=pwd.getpwall(),
            all_groups=grp.getgrall(),
            shadow_path=Path("/etc/shadow"),
            required_uid=0,
        )
    except LauncherPreflightError as error:
        raise RuntimeHandoffError(str(error)) from None
    result = handoff_runtime_credentials(
        RuntimeHandoffPaths(
            private_root=args.private_root,
            identity_dir=args.identity_dir,
            materialized_certificate_dir=args.materialized_certificate_dir,
            runtime_credential_dir=args.runtime_credential_dir,
        ),
        now_s=int(time.time()),
        apply=args.apply,
        runtime_gid=account.pw_gid,
        required_uid=0,
    )
    print(json.dumps(result.to_public_dict(), sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except RuntimeHandoffError as error:
        print(f"VC runtime credential handoff blocked: {error}", file=sys.stderr)
        sys.exit(2)
