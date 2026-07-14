"""Validate one protected, already-consumed Virtual Control start approval.

This module reads and validates one exact approval marker. It has no write,
rename, link, subprocess, socket, firmware-import, or process-start capability.
The reference unit atomically renames the source with the pinned stock mover
before its non-root preparer imports this validator.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from tools.brilliant_vc._common import PINNED_FIRMWARE as _PINNED_FIRMWARE
from tools.brilliant_vc._common import wipe as _wipe

_PANEL = "office"
_PURPOSE = "bounded_virtual_control_bootstrap"
_SCHEMA_VERSION = 1
_MAX_AGE_S = 600
_RUNTIME_LIMIT_S = 600
_MAX_BYTES = 64 * 1024
_SAFE_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class StartApprovalError(ValueError):
    """Raised when a protected approval marker cannot be trusted."""


@dataclass(frozen=True, slots=True)
class StartApproval:
    """Non-secret identity of one validated start approval."""

    run_id: str
    sha256: str
    runtime_credential_bundle_sha256: str


def validate_start_approval(
    path: Path,
    *,
    now_s: int,
    credential_uid: int,
    runtime_gid: int,
    allowed_paths: Sequence[Path],
) -> StartApproval:
    """Validate one exact root/dedicated-group approval file."""

    if isinstance(now_s, bool) or not isinstance(now_s, int) or now_s <= 0:
        raise StartApprovalError("current timestamp is invalid")
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value < 0
        for value in (credential_uid, runtime_gid)
    ):
        raise StartApprovalError("approval identity is invalid")
    if path.resolve(strict=False) not in {
        candidate.resolve(strict=False) for candidate in allowed_paths
    }:
        raise StartApprovalError("approval file is outside the allowed paths")
    raw = _read_approval_file(
        path,
        credential_uid=credential_uid,
        runtime_gid=runtime_gid,
    )
    try:
        digest = hashlib.sha256(raw).hexdigest()
        try:
            parsed: object = json.loads(raw, object_pairs_hook=_unique_json_object)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise StartApprovalError("approval file is invalid JSON") from None
    finally:
        _wipe(raw)
    if not isinstance(parsed, dict):
        raise StartApprovalError("approval file must contain an object")
    approval = cast(dict[str, object], parsed)
    expected_fields = {
        "schema_version",
        "approved",
        "approved_at_s",
        "run_id",
        "panel",
        "firmware_version",
        "purpose",
        "runtime_limit_s",
        "runtime_credential_bundle_sha256",
        "physical_device_actions_permitted",
        "hosted_light_permitted",
    }
    if set(approval) != expected_fields:
        raise StartApprovalError("approval scope fields are invalid")
    approved_at = approval["approved_at_s"]
    run_id = approval["run_id"]
    bundle_digest = approval["runtime_credential_bundle_sha256"]
    if isinstance(approved_at, bool) or not isinstance(approved_at, int):
        raise StartApprovalError("approval timestamp is invalid")
    if approved_at > now_s + 30:
        raise StartApprovalError("approval timestamp is in the future")
    if now_s - approved_at > _MAX_AGE_S:
        raise StartApprovalError("approval is older than 10 minutes")
    if not isinstance(run_id, str) or _SAFE_RUN_ID.fullmatch(run_id) is None:
        raise StartApprovalError("approval run ID is invalid")
    if not isinstance(bundle_digest, str) or _SHA256.fullmatch(bundle_digest) is None:
        raise StartApprovalError("approval credential-bundle digest is invalid")
    expected: dict[str, object] = {
        "schema_version": _SCHEMA_VERSION,
        "approved": True,
        "approved_at_s": approved_at,
        "run_id": run_id,
        "panel": _PANEL,
        "firmware_version": _PINNED_FIRMWARE,
        "purpose": _PURPOSE,
        "runtime_limit_s": _RUNTIME_LIMIT_S,
        "runtime_credential_bundle_sha256": bundle_digest,
        "physical_device_actions_permitted": False,
        "hosted_light_permitted": False,
    }
    if any(
        type(approval[name]) is not type(expected_value) or approval[name] != expected_value
        for name, expected_value in expected.items()
    ):
        raise StartApprovalError("approval scope does not match a bootstrap-only run")
    return StartApproval(
        run_id=run_id,
        sha256=digest,
        runtime_credential_bundle_sha256=bundle_digest,
    )


def _read_approval_file(path: Path, *, credential_uid: int, runtime_gid: int) -> bytearray:
    try:
        before = path.lstat()
    except FileNotFoundError:
        raise StartApprovalError("approval file does not exist") from None
    except OSError:
        raise StartApprovalError("could not inspect approval file") from None
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise StartApprovalError("approval file must be a regular non-symlink file")
    if before.st_uid != credential_uid or before.st_gid != runtime_gid:
        raise StartApprovalError("approval file has the wrong owner or group")
    if stat.S_IMODE(before.st_mode) != 0o640:
        raise StartApprovalError("approval file must have mode 0640")
    if before.st_nlink != 1 or not 0 < before.st_size <= _MAX_BYTES:
        raise StartApprovalError("approval file has an invalid link count or size")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        raise StartApprovalError("could not safely open approval file") from None
    value = bytearray()
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise StartApprovalError("approval file changed during open")
        while True:
            chunk = os.read(descriptor, min(8192, _MAX_BYTES + 1 - len(value)))
            if not chunk:
                break
            value.extend(chunk)
            if len(value) > _MAX_BYTES:
                raise StartApprovalError("approval file exceeds its size bound")
        after = os.fstat(descriptor)
        if (
            after.st_size != opened.st_size
            or after.st_mtime_ns != opened.st_mtime_ns
            or after.st_ctime_ns != opened.st_ctime_ns
        ):
            raise StartApprovalError("approval file changed while reading")
        return value
    except BaseException:
        _wipe(value)
        raise
    finally:
        os.close(descriptor)


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise StartApprovalError("approval file contains a duplicate field")
        result[key] = value
    return result
