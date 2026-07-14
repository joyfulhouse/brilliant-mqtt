from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from tools.brilliant_vc.session_approval import (
    SessionApproval,
    SessionApprovalError,
    validate_session_approval,
)

NOW_S = 1_800_000_000
STABLE_ID = "11111111-2222-4333-8444-555555555555"


def _payload() -> dict[str, object]:
    example = (
        Path(__file__).parents[1] / "docs/brilliant-panel/coordinated-session-approval.example.json"
    )
    payload: dict[str, object] = json.loads(example.read_text(encoding="utf-8"))
    payload.update(
        {
            "approved_at_s": NOW_S,
            "run_id": "office-vc-session-01",
            "runtime_credential_bundle_sha256": "a" * 64,
            "vc2_gate_ledger_sha256": "b" * 64,
            "room_id": "backyard-room",
            "office_device_id": "c" * 32,
            "mqtt_host": "mqtt.lan",
        }
    )
    return payload


def _approval(path: Path, payload: dict[str, object] | None = None) -> None:
    path.write_text(json.dumps(payload or _payload()), encoding="utf-8")
    path.chmod(0o640)
    os.chown(path, os.getuid(), os.getgid())


def _validate(
    path: Path,
    *,
    now_s: int = NOW_S,
    phase: str = "start",
) -> SessionApproval:
    return validate_session_approval(
        path,
        now_s=now_s,
        credential_uid=os.getuid(),
        runtime_gid=os.getgid(),
        allowed_paths=(path,),
        phase=phase,
    )


def test_exact_session_approval_exposes_only_the_bounded_plan(tmp_path: Path) -> None:
    marker = tmp_path / "session-approval-consumed.json"
    _approval(marker)

    approval = _validate(marker)

    assert approval.run_id == "office-vc-session-01"
    assert approval.approved_at_s == NOW_S
    assert approval.deadline_s == NOW_S + 2520
    assert approval.bootstrap_timeout_s == 600
    assert approval.pilot_runtime_s == 1800
    assert approval.runtime_credential_bundle_sha256 == "a" * 64
    assert approval.vc2_gate_ledger_sha256 == "b" * 64
    assert approval.mqtt_password_sha256 is None
    assert approval.stable_id == STABLE_ID
    assert approval.display_name == "HA VC Pilot Light"
    assert approval.room_id == "backyard-room"
    assert approval.office_device_id == "c" * 32
    assert approval.mqtt_host == "mqtt.lan"
    assert approval.mqtt_port == 1883
    assert approval.mqtt_username is None
    assert len(approval.sha256) == 64


def test_start_phase_is_fresh_and_active_phase_never_exceeds_absolute_deadline(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "session-approval-consumed.json"
    _approval(marker)

    with pytest.raises(SessionApprovalError, match="older than 10 minutes"):
        _validate(marker, now_s=NOW_S + 601, phase="start")

    active = _validate(marker, now_s=NOW_S + 2519, phase="active")
    assert active.deadline_s == NOW_S + 2520

    with pytest.raises(SessionApprovalError, match="session deadline"):
        _validate(marker, now_s=NOW_S + 2521, phase="active")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema_version", True),
        ("approved", 1),
        ("aggregate_runtime_limit_s", 2520.0),
        ("aggregate_runtime_limit_s", 2521),
        ("bootstrap_timeout_s", 601),
        ("pilot_runtime_s", 1799),
        ("hosted_light_permitted", 1),
        ("physical_device_actions_permitted", True),
        ("slider_binding_permitted", True),
        ("panel_gestures_permitted", True),
        ("panel", "backyard"),
    ],
)
def test_session_approval_rejects_type_confusion_or_broader_scope(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    marker = tmp_path / "session-approval-consumed.json"
    payload = _payload()
    payload[field] = value
    _approval(marker, payload)

    with pytest.raises(SessionApprovalError, match="coordinated session"):
        _validate(marker)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("stable_id", "11111111222243338444555555555555", "stable ID"),
        ("display_name", "", "display name"),
        ("display_name", "bad\nname", "display name"),
        ("room_id", "../backyard", "room ID"),
        ("office_device_id", "C" * 32, "Office device ID"),
        ("mqtt_host", "mqtt.lan/path", "MQTT host"),
        ("mqtt_port", 0, "MQTT port"),
        ("mqtt_username", "bad\nuser", "MQTT username"),
        ("mqtt_password_sha256", "not-a-hash", "MQTT password digest"),
    ],
)
def test_session_approval_rejects_unsafe_plan_values(
    tmp_path: Path,
    field: str,
    value: object,
    message: str,
) -> None:
    marker = tmp_path / "session-approval-consumed.json"
    payload = _payload()
    payload[field] = value
    _approval(marker, payload)

    with pytest.raises(SessionApprovalError, match=message):
        _validate(marker)


def test_session_approval_rejects_extra_or_duplicate_fields(tmp_path: Path) -> None:
    marker = tmp_path / "session-approval-consumed.json"
    payload = _payload()
    payload["slider_name"] = "office-slider"
    _approval(marker, payload)
    with pytest.raises(SessionApprovalError, match="scope fields"):
        _validate(marker)

    raw = json.dumps(_payload()).replace('"approved": true', '"approved": true, "approved": true')
    marker.write_text(raw, encoding="utf-8")
    marker.chmod(0o640)
    with pytest.raises(SessionApprovalError, match="duplicate field"):
        _validate(marker)


def test_session_approval_requires_exact_protected_marker_metadata(tmp_path: Path) -> None:
    marker = tmp_path / "session-approval-consumed.json"
    _approval(marker)
    marker.chmod(0o600)

    with pytest.raises(SessionApprovalError, match="mode 0640"):
        _validate(marker)

    marker.chmod(0o640)
    outside = tmp_path / "outside.json"
    _approval(outside)
    with pytest.raises(SessionApprovalError, match="outside the allowed paths"):
        validate_session_approval(
            outside,
            now_s=NOW_S,
            credential_uid=os.getuid(),
            runtime_gid=os.getgid(),
            allowed_paths=(marker,),
            phase="start",
        )


def test_session_approval_rejects_invalid_phase(tmp_path: Path) -> None:
    marker = tmp_path / "session-approval-consumed.json"
    _approval(marker)

    with pytest.raises(SessionApprovalError, match="validation phase"):
        _validate(marker, phase="cleanup")
