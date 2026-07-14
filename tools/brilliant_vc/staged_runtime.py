"""Validate the exact root-owned coordinated-session app and MQTT vendor tree.

The validator is read-only: it opens no panel socket, imports no firmware or
vendor package, and has no process-start, approval, provisioning, or write
primitive.  It rejects every unlisted file and directory before later staged
code is allowed to prepare or coordinate a session.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

_APP_ROOT = Path("/var/brilliant-vc/app")
_VENDOR_ROOT = Path("/var/brilliant-vc/vendor")
_MANIFEST_PATH = Path("/var/brilliant-vc/session-app-manifest.sha256")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_MAX_MANIFEST_BYTES = 256 * 1024
_MAX_STAGED_FILE_BYTES = 2 * 1024 * 1024

EXPECTED_APP_ENTRIES = (
    "tools/__init__.py",
    "tools/brilliant_vc/__init__.py",
    "tools/brilliant_vc/gates.py",
    "tools/brilliant_vc/launcher_preflight.py",
    "tools/brilliant_vc/monitor.py",
    "tools/brilliant_vc/runtime_handoff.py",
    "tools/brilliant_vc/runtime_prepare.py",
    "tools/brilliant_vc/session_approval.py",
    "tools/brilliant_vc/session_coordinator.py",
    "tools/brilliant_vc/session_prepare.py",
    "tools/brilliant_vc/single_light_pilot.py",
    "tools/brilliant_vc/staged_runtime.py",
    "tools/brilliant_vc/start_approval.py",
    "tools/brilliant_vc/vassal_manifest.py",
)
EXPECTED_VENDOR_ENTRIES = (
    "aiomqtt/__init__.py",
    "aiomqtt/client.py",
    "aiomqtt/exceptions.py",
    "aiomqtt/message.py",
    "aiomqtt/py.typed",
    "aiomqtt/topic.py",
    "aiomqtt/types.py",
    "paho/__init__.py",
    "paho/mqtt/__init__.py",
    "paho/mqtt/client.py",
    "paho/mqtt/enums.py",
    "paho/mqtt/matcher.py",
    "paho/mqtt/packettypes.py",
    "paho/mqtt/properties.py",
    "paho/mqtt/publish.py",
    "paho/mqtt/py.typed",
    "paho/mqtt/reasoncodes.py",
    "paho/mqtt/subscribe.py",
    "paho/mqtt/subscribeoptions.py",
)


class StagedRuntimeError(ValueError):
    """Raised when staged code differs from the exact reviewed surface."""


@dataclass(frozen=True, slots=True)
class StagedRuntimeReport:
    staging_valid: bool
    app_file_count: int
    vendor_file_count: int
    manifest_sha256: str

    def to_public_dict(self) -> dict[str, object]:
        return {
            "staging_valid": self.staging_valid,
            "app_file_count": self.app_file_count,
            "vendor_file_count": self.vendor_file_count,
            "manifest_sha256": self.manifest_sha256,
            "firmware_imported": False,
            "process_started": False,
        }


def validate_staged_runtime(
    *,
    app_root: Path = _APP_ROOT,
    vendor_root: Path = _VENDOR_ROOT,
    manifest_path: Path = _MANIFEST_PATH,
    required_uid: int = 0,
    required_gid: int = 0,
    allowed_app_roots: Sequence[Path] = (_APP_ROOT,),
    allowed_vendor_roots: Sequence[Path] = (_VENDOR_ROOT,),
    allowed_manifest_paths: Sequence[Path] = (_MANIFEST_PATH,),
    expected_app_entries: Sequence[str] = EXPECTED_APP_ENTRIES,
    expected_vendor_entries: Sequence[str] = EXPECTED_VENDOR_ENTRIES,
) -> StagedRuntimeReport:
    """Hash and inventory the complete staged surface without importing it."""

    if any(
        isinstance(value, bool) or not isinstance(value, int) or value < 0
        for value in (required_uid, required_gid)
    ):
        raise StagedRuntimeError("required staging identity is invalid")
    app = _allowed_root(app_root, allowed_app_roots, "app")
    vendor = _allowed_root(vendor_root, allowed_vendor_roots, "vendor")
    manifest = manifest_path.resolve(strict=False)
    if manifest not in {path.resolve(strict=False) for path in allowed_manifest_paths}:
        raise StagedRuntimeError("staging manifest is outside the allowed paths")
    if _overlap(app, vendor) or _overlap(app, manifest) or _overlap(vendor, manifest):
        raise StagedRuntimeError("staged roots and manifest must be disjoint")
    _validate_directory(
        manifest.parent,
        description="staging manifest directory",
        uid=required_uid,
        gid=required_gid,
    )
    app_entries = _validated_entry_set(expected_app_entries, "app")
    vendor_entries = _validated_entry_set(expected_vendor_entries, "vendor")
    _validate_tree(app, app_entries, uid=required_uid, gid=required_gid)
    _validate_tree(vendor, vendor_entries, uid=required_uid, gid=required_gid)

    manifest_bytes = _read_regular(
        manifest,
        description="staging manifest",
        uid=required_uid,
        gid=required_gid,
        mode=0o644,
        maximum_bytes=_MAX_MANIFEST_BYTES,
    )
    expected_targets = {str(app / relative): relative for relative in app_entries} | {
        str(vendor / relative): relative for relative in vendor_entries
    }
    parsed = _parse_manifest(manifest_bytes, expected_targets=set(expected_targets))
    for target, expected_digest in parsed.items():
        digest = _hash_regular(
            Path(target),
            uid=required_uid,
            gid=required_gid,
        )
        if digest != expected_digest:
            raise StagedRuntimeError("staged file digest does not match the manifest")
    return StagedRuntimeReport(
        staging_valid=True,
        app_file_count=len(app_entries),
        vendor_file_count=len(vendor_entries),
        manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
    )


def _allowed_root(path: Path, allowed: Sequence[Path], description: str) -> Path:
    resolved = path.resolve(strict=False)
    if resolved not in {candidate.resolve(strict=False) for candidate in allowed}:
        raise StagedRuntimeError(f"staged {description} root is outside the allowed roots")
    return resolved


def _validated_entry_set(entries: Sequence[str], description: str) -> frozenset[str]:
    values = tuple(entries)
    if not values or len(values) != len(set(values)):
        raise StagedRuntimeError(f"expected {description} inventory is empty or duplicated")
    for value in values:
        if not isinstance(value, str):
            raise StagedRuntimeError(f"expected {description} entry is not a string")
        parsed = PurePosixPath(value)
        if parsed.is_absolute() or ".." in parsed.parts or str(parsed) != value:
            raise StagedRuntimeError(f"expected {description} entry is not canonical")
        if not value or value.endswith("/") or "__pycache__" in parsed.parts:
            raise StagedRuntimeError(f"expected {description} entry is forbidden")
    return frozenset(values)


def _validate_tree(root: Path, entries: frozenset[str], *, uid: int, gid: int) -> None:
    directories = {"."}
    children: dict[str, set[str]] = {".": set()}
    for relative in entries:
        parsed = PurePosixPath(relative)
        parent = parsed.parent
        while str(parent) != ".":
            directories.add(str(parent))
            parent = parent.parent
        current = PurePosixPath(".")
        for part in parsed.parts[:-1]:
            parent_key = str(current)
            children.setdefault(parent_key, set()).add(part)
            current = current / part
            children.setdefault(str(current), set())
        children.setdefault(str(parsed.parent), set()).add(parsed.name)
    for relative in sorted(directories, key=lambda value: (value.count("/"), value)):
        directory = root if relative == "." else root / relative
        _validate_directory(
            directory,
            description="staged directory",
            uid=uid,
            gid=gid,
        )
        try:
            actual = {entry.name for entry in directory.iterdir()}
        except OSError:
            raise StagedRuntimeError("could not inspect staged directory") from None
        if actual != children.get(relative, set()):
            raise StagedRuntimeError("staged directory inventory is not exact")
    for relative in entries:
        _validate_regular_metadata(root / relative, uid=uid, gid=gid, mode=0o644)


def _validate_directory(path: Path, *, description: str, uid: int, gid: int) -> None:
    try:
        metadata = path.lstat()
    except OSError:
        raise StagedRuntimeError(f"could not inspect {description}") from None
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise StagedRuntimeError(f"{description} must be a real directory")
    if metadata.st_uid != uid or metadata.st_gid != gid or stat.S_IMODE(metadata.st_mode) != 0o755:
        raise StagedRuntimeError(f"{description} must have the exact root identity and mode")


def _validate_regular_metadata(path: Path, *, uid: int, gid: int, mode: int) -> os.stat_result:
    try:
        metadata = path.lstat()
    except OSError:
        raise StagedRuntimeError("could not inspect staged file") from None
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise StagedRuntimeError("staged file must be regular and not a symlink")
    if (
        metadata.st_uid != uid
        or metadata.st_gid != gid
        or stat.S_IMODE(metadata.st_mode) != mode
        or metadata.st_nlink != 1
        or not 0 <= metadata.st_size <= _MAX_STAGED_FILE_BYTES
    ):
        raise StagedRuntimeError("staged file has invalid identity, mode, links, or size")
    return metadata


def _read_regular(
    path: Path,
    *,
    description: str,
    uid: int,
    gid: int,
    mode: int,
    maximum_bytes: int,
) -> bytes:
    before = _validate_regular_metadata(path, uid=uid, gid=gid, mode=mode)
    if before.st_size > maximum_bytes:
        raise StagedRuntimeError(f"{description} is too large")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        raise StagedRuntimeError(f"could not open {description}") from None
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise StagedRuntimeError(f"{description} changed during open")
        value = bytearray()
        while True:
            chunk = os.read(descriptor, min(8192, maximum_bytes + 1 - len(value)))
            if not chunk:
                break
            value.extend(chunk)
            if len(value) > maximum_bytes:
                raise StagedRuntimeError(f"{description} exceeds its size bound")
        after = os.fstat(descriptor)
        if (
            after.st_size != opened.st_size
            or after.st_mtime_ns != opened.st_mtime_ns
            or after.st_ctime_ns != opened.st_ctime_ns
        ):
            raise StagedRuntimeError(f"{description} changed while reading")
        return bytes(value)
    finally:
        os.close(descriptor)


def _hash_regular(path: Path, *, uid: int, gid: int) -> str:
    value = _read_regular(
        path,
        description="staged file",
        uid=uid,
        gid=gid,
        mode=0o644,
        maximum_bytes=_MAX_STAGED_FILE_BYTES,
    )
    return hashlib.sha256(value).hexdigest()


def _parse_manifest(raw: bytes, *, expected_targets: set[str]) -> dict[str, str]:
    try:
        text = raw.decode("ascii")
    except UnicodeDecodeError:
        raise StagedRuntimeError("staging manifest is not ASCII") from None
    if not text.endswith("\n"):
        raise StagedRuntimeError("staging manifest must end with one newline")
    parsed: dict[str, str] = {}
    for line in text.splitlines():
        digest, separator, target = line.partition("  ")
        if separator != "  " or _SHA256.fullmatch(digest) is None or not target:
            raise StagedRuntimeError("staging manifest line is invalid")
        if target in parsed:
            raise StagedRuntimeError("staging manifest target is duplicated")
        parsed[target] = digest
    if set(parsed) != expected_targets:
        raise StagedRuntimeError("staging manifest inventory is not exact")
    return parsed


def _overlap(left: Path, right: Path) -> bool:
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


def main() -> int:
    report = validate_staged_runtime()
    print(json.dumps(report.to_public_dict(), sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except StagedRuntimeError as error:
        print(f"VC staged runtime blocked: {error}", file=sys.stderr)
        sys.exit(2)
