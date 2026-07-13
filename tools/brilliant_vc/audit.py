"""VC0 prior-state and credential-permission audit.

The collector calls ``lstat`` only. It never opens or hashes credential files.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
import sys
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

SAFE_STAT_FIELDS = ("path", "exists", "uid", "gid", "mode", "size", "mtime_ns")
SENSITIVE_PATHS = (
    "/tmp/mirror_poc/.access",
    "/tmp/mirror_poc/.vc_record.json",
    "/data/brilliant-vc/identity",
)

_SNAPSHOT_FIELDS = {
    "firmware_version",
    "bus_home_id_sha256",
    "physical_control_count",
    "bus_device_type_6_ids",
    "app_device_type_6_ids",
    "known_preexisting_device_type_6_ids",
    "july9_app_inventory_confirms_no_new_vc",
    "july9_bus_inventory_confirms_no_new_vc",
}
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_REDACTED_ID = re.compile(r"^[A-Za-z0-9]{4}…[A-Za-z0-9]{4}$")
_SECRET_FIELD = re.compile(
    r"(?:^|_)(?:token|password|secret|certificate|private_key|pkcs12|jwt|credential)(?:_|$)",
    re.IGNORECASE,
)
_JWT_SHAPE = re.compile(r"[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}")
_PEM_MARKER = re.compile(r"-----BEGIN [A-Z0-9 ]+-----")
_LONG_BASE64 = re.compile(r"^[A-Za-z0-9_+/=-]{257,}$")


class AuditInputError(ValueError):
    """Raised when purportedly sanitized audit input is unsafe or malformed."""


@dataclass(frozen=True, slots=True)
class AuditResult:
    """Sanitized VC0 decision and evidence summary."""

    passed: bool
    reasons: tuple[str, ...]
    report: dict[str, object]


def collect_stat_inventory(paths: Sequence[str] = SENSITIVE_PATHS) -> list[dict[str, object]]:
    """Return allowlisted metadata from ``lstat`` without opening any path."""

    inventory: list[dict[str, object]] = []
    for path in paths:
        try:
            metadata = os.lstat(path)
        except FileNotFoundError:
            inventory.append(
                {
                    "path": path,
                    "exists": False,
                    "uid": None,
                    "gid": None,
                    "mode": None,
                    "size": None,
                    "mtime_ns": None,
                }
            )
            continue
        inventory.append(
            {
                "path": path,
                "exists": True,
                "uid": metadata.st_uid,
                "gid": metadata.st_gid,
                "mode": stat.S_IMODE(metadata.st_mode),
                "size": metadata.st_size,
                "mtime_ns": metadata.st_mtime_ns,
            }
        )
    return inventory


def audit_prior_state(
    snapshot: Mapping[str, object],
    stat_inventory: Sequence[Mapping[str, object]],
    *,
    retained_paths: Mapping[str, str] | None = None,
) -> AuditResult:
    """Validate sanitized inputs and determine whether VC0 may pass."""

    _reject_unsafe_tree(snapshot)
    _validate_snapshot_fields(snapshot)
    stats = _validate_stat_inventory(stat_inventory)
    retained = dict(retained_paths or {})
    _validate_retention_reasons(retained)

    firmware_version = _require_str(snapshot["firmware_version"], "firmware_version")
    home_hash = _require_str(snapshot["bus_home_id_sha256"], "bus_home_id_sha256")
    if not _SHA256.fullmatch(home_hash):
        raise AuditInputError("bus_home_id_sha256 must be lowercase SHA-256")
    physical_count = _require_nonnegative_int(
        snapshot["physical_control_count"], "physical_control_count"
    )
    bus_type6 = _redacted_id_set(snapshot["bus_device_type_6_ids"], "bus IDs")
    app_type6 = _redacted_id_set(snapshot["app_device_type_6_ids"], "app IDs")
    known_type6 = _redacted_id_set(snapshot["known_preexisting_device_type_6_ids"], "known IDs")
    unexplained = (bus_type6 | app_type6) - known_type6

    app_confirmation = _require_bool(
        snapshot["july9_app_inventory_confirms_no_new_vc"],
        "july9_app_inventory_confirms_no_new_vc",
    )
    bus_confirmation = _require_bool(
        snapshot["july9_bus_inventory_confirms_no_new_vc"],
        "july9_bus_inventory_confirms_no_new_vc",
    )

    reasons: list[str] = []
    if bus_type6 != app_type6:
        reasons.append("app and bus DeviceType 6 inventories disagree")
    if unexplained:
        reasons.append(f"{len(unexplained)} unexplained DeviceType 6 identity found")

    permission_issue_count = 0
    retained_count = 0
    prior_record_exists = False
    for item in stats:
        if not cast(bool, item["exists"]):
            continue
        path = cast(str, item["path"])
        uid = cast(int, item["uid"])
        mode = cast(int, item["mode"])
        if path == "/tmp/mirror_poc/.vc_record.json":
            prior_record_exists = True
            reasons.append("July 9 Virtual Control record still exists")
        if uid != 0:
            permission_issue_count += 1
            reasons.append(f"{path} is not root-owned")
        if mode & 0o077:
            permission_issue_count += 1
            reasons.append(f"{path} has group/world permissions")
        if path in retained:
            retained_count += 1
        elif path != "/tmp/mirror_poc/.vc_record.json":
            reasons.append(f"{path} requires an explicit delete-or-retain action")

    unknown_retained = set(retained) - {
        cast(str, item["path"]) for item in stats if cast(bool, item["exists"])
    }
    if unknown_retained:
        reasons.append("retention reason references a path that is not present")

    july9_confirmed = (
        app_confirmation
        and bus_confirmation
        and bus_type6 == app_type6
        and not unexplained
        and not prior_record_exists
    )
    if not app_confirmation or not bus_confirmation:
        reasons.append("July 9 result lacks independent app and bus confirmation")

    report: dict[str, object] = {
        "firmware_version": firmware_version,
        "bus_home_id_sha256": home_hash,
        "physical_control_count": physical_count,
        "device_type_6_count": len(bus_type6 | app_type6),
        "unexplained_device_type_6_count": len(unexplained),
        "app_bus_type_6_inventory_match": bus_type6 == app_type6,
        "july9_no_vc_confirmed": july9_confirmed,
        "sensitive_path_count": len(stats),
        "existing_sensitive_path_count": sum(1 for item in stats if cast(bool, item["exists"])),
        "permission_issue_count": permission_issue_count,
        "retained_root_only_count": retained_count,
        "prior_vc_record_exists": prior_record_exists,
    }
    return AuditResult(passed=not reasons, reasons=tuple(reasons), report=report)


def _validate_snapshot_fields(snapshot: Mapping[str, object]) -> None:
    fields = set(snapshot)
    if fields != _SNAPSHOT_FIELDS:
        extra = fields - _SNAPSHOT_FIELDS
        missing = _SNAPSHOT_FIELDS - fields
        if any(_SECRET_FIELD.search(field) for field in extra):
            raise AuditInputError("snapshot contains unsafe field")
        raise AuditInputError(
            f"snapshot fields do not match schema; extra={sorted(extra)}, missing={sorted(missing)}"
        )


def _validate_stat_inventory(
    inventory: Sequence[Mapping[str, object]],
) -> tuple[dict[str, object], ...]:
    if len(inventory) != len(SENSITIVE_PATHS):
        raise AuditInputError("stat inventory must cover every sensitive path exactly once")
    normalized: list[dict[str, object]] = []
    seen: set[str] = set()
    for raw in inventory:
        if set(raw) != set(SAFE_STAT_FIELDS):
            raise AuditInputError("stat fields do not match the safe allowlist")
        path = _require_str(raw["path"], "stat path")
        if path not in SENSITIVE_PATHS or path in seen:
            raise AuditInputError("stat inventory path is unexpected or duplicated")
        seen.add(path)
        exists = _require_bool(raw["exists"], "stat exists")
        item = dict(raw)
        if exists:
            for field in ("uid", "gid", "mode", "size", "mtime_ns"):
                _require_nonnegative_int(raw[field], f"stat {field}")
        elif any(raw[field] is not None for field in ("uid", "gid", "mode", "size", "mtime_ns")):
            raise AuditInputError("missing stat path must use null metadata")
        normalized.append(item)
    return tuple(normalized)


def _validate_retention_reasons(retained: Mapping[str, str]) -> None:
    for path, reason in retained.items():
        if path not in SENSITIVE_PATHS:
            raise AuditInputError("retention path is not allowlisted")
        if not reason or len(reason) > 200:
            raise AuditInputError("retention reason must contain 1 to 200 characters")
        _reject_unsafe_string(reason)


def _reject_unsafe_tree(value: object, *, field: str = "root") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            if not isinstance(key, str):
                raise AuditInputError("input object keys must be strings")
            if _SECRET_FIELD.search(key):
                raise AuditInputError(f"input contains unsafe field at {field}")
            _reject_unsafe_tree(child, field=f"{field}.{key}")
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, child in enumerate(value):
            _reject_unsafe_tree(child, field=f"{field}[{index}]")
    elif isinstance(value, str):
        _reject_unsafe_string(value)


def _reject_unsafe_string(value: str) -> None:
    if _JWT_SHAPE.search(value) or _PEM_MARKER.search(value) or _LONG_BASE64.fullmatch(value):
        raise AuditInputError("input contains unsafe value")


def _redacted_id_set(value: object, field: str) -> set[str]:
    if not isinstance(value, list):
        raise AuditInputError(f"{field} must be a list")
    ids: set[str] = set()
    for item in value:
        text = _require_str(item, field)
        if not _REDACTED_ID.fullmatch(text):
            raise AuditInputError(f"{field} must contain only first4…last4 identifiers")
        if text in ids:
            raise AuditInputError(f"{field} contains a duplicate identifier")
        ids.add(text)
    return ids


def _require_str(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise AuditInputError(f"{field} must be a string")
    return value


def _require_bool(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        raise AuditInputError(f"{field} must be a boolean")
    return value


def _require_nonnegative_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise AuditInputError(f"{field} must be a non-negative integer")
    return value


def _atomic_json_write(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temp_name = handle.name
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    finally:
        if temp_name is not None:
            Path(temp_name).unlink(missing_ok=True)


def _load_object(path: Path, description: str) -> dict[str, object]:
    raw: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise AuditInputError(f"{description} must be a JSON object")
    return cast(dict[str, object], raw)


def _load_list(path: Path, description: str) -> list[dict[str, object]]:
    raw: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list) or not all(isinstance(item, dict) for item in raw):
        raise AuditInputError(f"{description} must be a JSON array of objects")
    return cast(list[dict[str, object]], raw)


def main(argv: Sequence[str] | None = None) -> int:
    """Collect stat-only input or generate a sanitized VC0 report."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collect-stats", type=Path)
    parser.add_argument("--panel")
    parser.add_argument("--snapshot-json", type=Path)
    parser.add_argument("--stat-json", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)

    if args.collect_stats is not None:
        if any(
            value is not None
            for value in (args.panel, args.snapshot_json, args.stat_json, args.output)
        ):
            parser.error("--collect-stats cannot be combined with audit arguments")
        _atomic_json_write(args.collect_stats, collect_stat_inventory())
        return 0

    if any(
        value is None for value in (args.panel, args.snapshot_json, args.stat_json, args.output)
    ):
        parser.error("audit requires --panel, --snapshot-json, --stat-json, and --output")
    panel = cast(str, args.panel)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", panel):
        parser.error("--panel must be a safe label")
    result = audit_prior_state(
        _load_object(cast(Path, args.snapshot_json), "snapshot"),
        _load_list(cast(Path, args.stat_json), "stat inventory"),
    )
    output = dict(result.report)
    output.update({"panel": panel, "passed": result.passed, "reasons": list(result.reasons)})
    _atomic_json_write(cast(Path, args.output), output)
    return 0 if result.passed else 2


if __name__ == "__main__":
    sys.exit(main())
