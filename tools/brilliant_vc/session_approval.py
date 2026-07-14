"""Validate one consumed coordinated-session approval without changing it.

The schema authorizes one bounded Virtual Control bootstrap plus one hosted
HA-backed light. It never authorizes physical device actions, slider writes, or
panel gestures. This module has no write, rename, subprocess, socket, firmware
import, or process-start capability.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast
from uuid import UUID

from tools.brilliant_vc.start_approval import (
    StartApprovalError,
    _read_approval_file,
    _unique_json_object,
    _wipe,
)

_SCHEMA_VERSION = 1
_PANEL = "office"
_PINNED_FIRMWARE = "v26.06.03.1"
_PURPOSE = "coordinated_virtual_control_single_light_session"
_AGGREGATE_RUNTIME_LIMIT_S = 2520
_BOOTSTRAP_TIMEOUT_S = 600
_PILOT_RUNTIME_S = 1800
_MAX_START_AGE_S = 600
_SAFE_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_LINK_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_DEVICE_ID = re.compile(r"^[0-9a-f]{32}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_DNS_LABEL = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$")


class SessionApprovalError(ValueError):
    """Raised when a coordinated-session marker cannot be trusted."""


@dataclass(frozen=True, slots=True)
class SessionApproval:
    """Immutable plan extracted from one exact coordinated-session approval."""

    run_id: str
    sha256: str
    approved_at_s: int
    deadline_s: int
    bootstrap_timeout_s: int
    pilot_runtime_s: int
    runtime_credential_bundle_sha256: str
    vc2_gate_ledger_sha256: str
    mqtt_password_sha256: str | None
    stable_id: str
    display_name: str
    room_id: str
    office_device_id: str
    mqtt_host: str
    mqtt_port: int
    mqtt_username: str | None


def validate_session_approval(
    path: Path,
    *,
    now_s: int,
    credential_uid: int,
    runtime_gid: int,
    allowed_paths: Sequence[Path],
    phase: str,
) -> SessionApproval:
    """Validate the exact start or active phase of one consumed approval."""

    if phase not in {"start", "active"}:
        raise SessionApprovalError("session approval validation phase is invalid")
    if isinstance(now_s, bool) or not isinstance(now_s, int) or now_s <= 0:
        raise SessionApprovalError("current timestamp is invalid")
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value < 0
        for value in (credential_uid, runtime_gid)
    ):
        raise SessionApprovalError("approval identity is invalid")
    if path.resolve(strict=False) not in {
        candidate.resolve(strict=False) for candidate in allowed_paths
    }:
        raise SessionApprovalError("approval file is outside the allowed paths")

    try:
        raw = _read_approval_file(
            path,
            credential_uid=credential_uid,
            runtime_gid=runtime_gid,
        )
    except StartApprovalError as error:
        raise SessionApprovalError(str(error)) from None
    try:
        digest = hashlib.sha256(raw).hexdigest()
        try:
            parsed: object = json.loads(raw, object_pairs_hook=_unique_json_object)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise SessionApprovalError("approval file is invalid JSON") from None
        except StartApprovalError as error:
            raise SessionApprovalError(str(error)) from None
    finally:
        _wipe(raw)
    if not isinstance(parsed, dict):
        raise SessionApprovalError("approval file must contain an object")
    approval = cast(dict[str, object], parsed)
    expected_fields = {
        "schema_version",
        "approved",
        "approved_at_s",
        "run_id",
        "panel",
        "firmware_version",
        "purpose",
        "aggregate_runtime_limit_s",
        "bootstrap_timeout_s",
        "pilot_runtime_s",
        "runtime_credential_bundle_sha256",
        "vc2_gate_ledger_sha256",
        "mqtt_password_sha256",
        "stable_id",
        "display_name",
        "room_id",
        "office_device_id",
        "mqtt_host",
        "mqtt_port",
        "mqtt_username",
        "hosted_light_permitted",
        "physical_device_actions_permitted",
        "slider_binding_permitted",
        "panel_gestures_permitted",
    }
    if set(approval) != expected_fields:
        raise SessionApprovalError("approval scope fields are invalid")

    approved_at = approval["approved_at_s"]
    if isinstance(approved_at, bool) or not isinstance(approved_at, int):
        raise SessionApprovalError("approval timestamp is invalid")
    if approved_at > now_s + 30:
        raise SessionApprovalError("approval timestamp is in the future")
    if phase == "start" and now_s - approved_at > _MAX_START_AGE_S:
        raise SessionApprovalError("approval is older than 10 minutes")
    deadline_s = approved_at + _AGGREGATE_RUNTIME_LIMIT_S
    if phase == "active" and now_s > deadline_s:
        raise SessionApprovalError("coordinated session deadline has elapsed")

    expected_scope: dict[str, object] = {
        "schema_version": _SCHEMA_VERSION,
        "approved": True,
        "approved_at_s": approved_at,
        "panel": _PANEL,
        "firmware_version": _PINNED_FIRMWARE,
        "purpose": _PURPOSE,
        "aggregate_runtime_limit_s": _AGGREGATE_RUNTIME_LIMIT_S,
        "bootstrap_timeout_s": _BOOTSTRAP_TIMEOUT_S,
        "pilot_runtime_s": _PILOT_RUNTIME_S,
        "hosted_light_permitted": True,
        "physical_device_actions_permitted": False,
        "slider_binding_permitted": False,
        "panel_gestures_permitted": False,
    }
    if any(
        type(approval[name]) is not type(expected_value) or approval[name] != expected_value
        for name, expected_value in expected_scope.items()
    ):
        raise SessionApprovalError("approval scope does not match the coordinated session")

    run_id = approval["run_id"]
    if not isinstance(run_id, str) or _SAFE_RUN_ID.fullmatch(run_id) is None:
        raise SessionApprovalError("approval run ID is invalid")
    credential_digest = _required_sha256(
        approval["runtime_credential_bundle_sha256"],
        "runtime credential-bundle digest",
    )
    ledger_digest = _required_sha256(
        approval["vc2_gate_ledger_sha256"],
        "VC2 gate-ledger digest",
    )
    raw_password_digest = approval["mqtt_password_sha256"]
    password_digest = (
        None
        if raw_password_digest is None
        else _required_sha256(raw_password_digest, "MQTT password digest")
    )
    stable_id = _canonical_uuid(approval["stable_id"])
    display_name = _safe_display_name(approval["display_name"])
    room_id = approval["room_id"]
    if not isinstance(room_id, str) or _LINK_ID.fullmatch(room_id) is None:
        raise SessionApprovalError("approval room ID is invalid")
    office_device_id = approval["office_device_id"]
    if not isinstance(office_device_id, str) or _DEVICE_ID.fullmatch(office_device_id) is None:
        raise SessionApprovalError("approval Office device ID is invalid")
    mqtt_host = _safe_mqtt_host(approval["mqtt_host"])
    mqtt_port = approval["mqtt_port"]
    if isinstance(mqtt_port, bool) or not isinstance(mqtt_port, int) or not 1 <= mqtt_port <= 65535:
        raise SessionApprovalError("approval MQTT port is invalid")
    mqtt_username = _safe_optional_username(approval["mqtt_username"])

    return SessionApproval(
        run_id=run_id,
        sha256=digest,
        approved_at_s=approved_at,
        deadline_s=deadline_s,
        bootstrap_timeout_s=_BOOTSTRAP_TIMEOUT_S,
        pilot_runtime_s=_PILOT_RUNTIME_S,
        runtime_credential_bundle_sha256=credential_digest,
        vc2_gate_ledger_sha256=ledger_digest,
        mqtt_password_sha256=password_digest,
        stable_id=stable_id,
        display_name=display_name,
        room_id=room_id,
        office_device_id=office_device_id,
        mqtt_host=mqtt_host,
        mqtt_port=mqtt_port,
        mqtt_username=mqtt_username,
    )


def _required_sha256(value: object, description: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise SessionApprovalError(f"approval {description} is invalid")
    return value


def _canonical_uuid(value: object) -> str:
    if not isinstance(value, str):
        raise SessionApprovalError("approval stable ID is invalid")
    try:
        canonical = str(UUID(value))
    except ValueError:
        raise SessionApprovalError("approval stable ID is invalid") from None
    if canonical != value:
        raise SessionApprovalError("approval stable ID must use canonical UUID form")
    return canonical


def _safe_display_name(value: object) -> str:
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= 80
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise SessionApprovalError("approval display name is invalid")
    return value


def _safe_mqtt_host(value: object) -> str:
    if not isinstance(value, str) or not value or len(value) > 253:
        raise SessionApprovalError("approval MQTT host is invalid")
    if any(ord(character) < 33 or ord(character) == 127 for character in value):
        raise SessionApprovalError("approval MQTT host is invalid")
    try:
        ipaddress.ip_address(value)
        return value
    except ValueError:
        pass
    labels = value.removesuffix(".").split(".")
    if not labels or any(_DNS_LABEL.fullmatch(label) is None for label in labels):
        raise SessionApprovalError("approval MQTT host is invalid")
    return value


def _safe_optional_username(value: object) -> str | None:
    if value is None:
        return None
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= 128
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise SessionApprovalError("approval MQTT username is invalid")
    return value
