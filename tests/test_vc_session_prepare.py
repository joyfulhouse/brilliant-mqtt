from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

import pytest

from tools.brilliant_vc.gates import Evidence, GateLedger, GateName, GateStatus
from tools.brilliant_vc.launcher_preflight import LauncherPaths
from tools.brilliant_vc.runtime_prepare import RuntimePrepareResult
from tools.brilliant_vc.session_coordinator import (
    SessionCoordinatorError,
    load_coordinator_session,
)
from tools.brilliant_vc.session_prepare import (
    CoordinatedSessionPrepareError,
    CoordinatedSessionPrepareResult,
    SessionPreparePaths,
    prepare_coordinated_session_no_start,
)

NOW_S = 1_800_000_000
RUN_ID = "office-vc-session-01"
STABLE_ID = "11111111-2222-4333-8444-555555555555"


def _launcher_paths(tmp_path: Path) -> LauncherPaths:
    persistent = tmp_path / "brilliant-vc"
    runtime = tmp_path / "run/brilliant-vc"
    credentials = tmp_path / "brilliant-vc-credentials"
    private = tmp_path / "private"
    return LauncherPaths(
        private_root=private,
        persistent_root=persistent,
        identity_dir=private / "identity",
        materialized_certificate_dir=private / "materialized-certificates",
        runtime_credential_dir=credentials,
        bootstrap_path=credentials / "bootstrap",
        state_dir=persistent / "state",
        certificate_dir=credentials / "certificates",
        process_config_dir=persistent / "process-config",
        process_flagfile_dir=persistent / "flagfiles",
        startable_config_dir=persistent / "startable-configs",
        log_dir=persistent / "logs",
        error_log_dir=persistent / "errors",
        trace_dir=persistent / "traces",
        runtime_dir=runtime,
        socket_path=runtime / "server_socket",
        stats_socket_path=runtime / "uwsgi_stats_socket",
        release_info_path=tmp_path / "release_info.json",
        tracking_branch_path=tmp_path / "tracking_branch",
        art_preload_dir=tmp_path / "art-library",
    )


def _session_paths(tmp_path: Path) -> SessionPreparePaths:
    input_root = tmp_path / "session-input"
    output_root = tmp_path / "session-output"
    control_root = tmp_path / "session-control"
    approval_root = tmp_path / "session-approval"
    for path, mode in (
        (input_root, 0o750),
        (output_root, 0o700),
        (control_root, 0o700),
        (approval_root, 0o750),
    ):
        path.mkdir(parents=True, mode=mode)
        path.chmod(mode)
        os.chown(path, os.getuid(), os.getgid())
    return SessionPreparePaths(
        launcher=_launcher_paths(tmp_path),
        input_root=input_root,
        vc2_ledger_path=input_root / "gate-ledger-vc2.json",
        mqtt_password_path=input_root / "mqtt-password",
        output_root=output_root,
        control_root=control_root,
        approval_source_path=approval_root / "session-approval.json",
        approval_marker_path=approval_root / "session-approval-consumed.json",
    )


def _ledger(path: Path, *, include_vc3: bool = False, run_id: str = RUN_ID) -> str:
    ledger = GateLedger.new(run_id=run_id)
    for gate in (GateName.VC0, GateName.VC1, GateName.VC2):
        ledger.record(
            gate,
            GateStatus.PASS,
            f"{gate.value} passed",
            [Evidence(kind="result", value=f"{gate.value.lower()}.json")],
        )
    if include_vc3:
        ledger.record(GateName.VC3, GateStatus.PASS, "VC3 passed", [])
    ledger.save(path)
    path.chmod(0o640)
    os.chown(path, os.getuid(), os.getgid())
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _approval_payload(
    *, ledger_sha256: str, password_sha256: str | None = None
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "approved": True,
        "approved_at_s": NOW_S,
        "run_id": RUN_ID,
        "panel": "office",
        "firmware_version": "v26.06.03.1",
        "purpose": "coordinated_virtual_control_single_light_session",
        "aggregate_runtime_limit_s": 2520,
        "bootstrap_timeout_s": 600,
        "pilot_runtime_s": 1800,
        "runtime_credential_bundle_sha256": "a" * 64,
        "vc2_gate_ledger_sha256": ledger_sha256,
        "mqtt_password_sha256": password_sha256,
        "stable_id": STABLE_ID,
        "display_name": "HA VC Pilot Light",
        "room_id": "backyard-room",
        "office_device_id": "c" * 32,
        "mqtt_host": "mqtt.lan",
        "mqtt_port": 1883,
        "mqtt_username": None,
        "hosted_light_permitted": True,
        "physical_device_actions_permitted": False,
        "slider_binding_permitted": False,
        "panel_gestures_permitted": False,
    }


def _write_approval(paths: SessionPreparePaths, payload: dict[str, object]) -> None:
    paths.approval_source_path.write_text(json.dumps(payload), encoding="utf-8")
    paths.approval_source_path.chmod(0o640)
    os.chown(paths.approval_source_path, os.getuid(), os.getgid())
    paths.approval_source_path.rename(paths.approval_marker_path)


def _runtime_result(*, apply: bool, approval_run_id: str | None = None) -> RuntimePrepareResult:
    return RuntimePrepareResult(
        dry_run=not apply,
        firmware_matches=True,
        runtime_identity_valid=True,
        runtime_credentials_valid=True,
        approval_validated=apply,
        preparation_complete=apply,
        approval_consumed=apply,
        initial_vassals=("message_bus.ini",) if apply else (),
        device_id_redacted="aaaa…aaaa",
        runtime_credential_bundle_sha256="a" * 64,
        approval_run_id=approval_run_id,
        approval_sha256="d" * 64 if apply else None,
        disabled_process_count=34,
        contains_emperor_start_primitive=False,
        emperor_started=False,
        blocked_reason=None if apply else "fresh_start_approval_required",
    )


def _stub_runtime_prepare(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []

    def fake(*args: object, **kwargs: Any) -> RuntimePrepareResult:
        del args
        calls.append(dict(kwargs))
        apply = bool(kwargs["apply"])
        return _runtime_result(apply=apply, approval_run_id=RUN_ID if apply else None)

    monkeypatch.setattr(
        "tools.brilliant_vc.session_prepare.prepare_runtime_no_start",
        fake,
    )
    return calls


def _prepare(paths: SessionPreparePaths, *, apply: bool) -> CoordinatedSessionPrepareResult:
    return prepare_coordinated_session_no_start(
        paths,
        now_s=NOW_S,
        apply=apply,
        runtime_user="brilliant-vc",
        runtime_uid=os.getuid(),
        runtime_gid=os.getgid(),
        credential_uid=os.getuid(),
        actual_module_hashes={},
        allowed_input_roots=(paths.input_root,),
        allowed_output_roots=(paths.output_root,),
        allowed_control_roots=(paths.control_root,),
        allowed_approval_marker_paths=(paths.approval_marker_path,),
        allowed_persistent_roots=(paths.launcher.persistent_root,),
        allowed_runtime_roots=(paths.launcher.runtime_dir,),
        allowed_runtime_credential_paths=(paths.launcher.runtime_credential_dir,),
    )


def test_session_dry_run_validates_vc2_inputs_and_never_selects_session_approval(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _session_paths(tmp_path)
    ledger_digest = _ledger(paths.vc2_ledger_path)
    calls = _stub_runtime_prepare(monkeypatch)

    result = _prepare(paths, apply=False)

    assert result.to_public_dict() == {
        "dry_run": True,
        "session_inputs_valid": True,
        "session_roots_valid": True,
        "runtime_preparation_complete": False,
        "approval_validated": False,
        "approval_consumed": False,
        "vc2_ledger_run_id": RUN_ID,
        "vc2_gate_ledger_sha256": ledger_digest,
        "mqtt_password_sha256": None,
        "runtime_credential_bundle_sha256": "a" * 64,
        "approval_run_id": None,
        "approval_sha256": None,
        "initial_vassals": [],
        "emperor_started": False,
        "blocked_reason": "fresh_coordinated_session_approval_required",
    }
    assert calls[0]["apply"] is False
    assert "approval_validator" in calls[0]


def test_session_apply_binds_ledger_password_run_and_runtime_bundle_before_pre_exec(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _session_paths(tmp_path)
    ledger_digest = _ledger(paths.vc2_ledger_path)
    password = b"synthetic-mqtt-password\n"
    paths.mqtt_password_path.write_bytes(password)
    paths.mqtt_password_path.chmod(0o640)
    os.chown(paths.mqtt_password_path, os.getuid(), os.getgid())
    password_digest = hashlib.sha256(password).hexdigest()
    _write_approval(
        paths,
        _approval_payload(
            ledger_sha256=ledger_digest,
            password_sha256=password_digest,
        ),
    )
    calls = _stub_runtime_prepare(monkeypatch)

    result = _prepare(paths, apply=True)

    public = result.to_public_dict()
    assert public["dry_run"] is False
    assert public["approval_validated"] is True
    assert public["approval_consumed"] is True
    assert public["runtime_preparation_complete"] is True
    assert public["vc2_gate_ledger_sha256"] == ledger_digest
    assert public["mqtt_password_sha256"] == password_digest
    assert public["blocked_reason"] is None
    assert calls[0]["apply"] is True
    assert calls[0]["approval_marker"] == paths.approval_marker_path
    assert callable(calls[0]["approval_validator"])


def test_session_apply_rejects_ledger_or_password_digest_drift_before_runtime_prepare(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _session_paths(tmp_path)
    ledger_digest = _ledger(paths.vc2_ledger_path)
    _write_approval(paths, _approval_payload(ledger_sha256="0" * 64))
    calls = _stub_runtime_prepare(monkeypatch)

    with pytest.raises(CoordinatedSessionPrepareError, match="gate ledger"):
        _prepare(paths, apply=True)
    assert calls == []

    paths.approval_marker_path.unlink()
    password = b"synthetic-mqtt-password\n"
    paths.mqtt_password_path.write_bytes(password)
    paths.mqtt_password_path.chmod(0o640)
    _write_approval(
        paths,
        _approval_payload(
            ledger_sha256=ledger_digest,
            password_sha256="0" * 64,
        ),
    )
    with pytest.raises(CoordinatedSessionPrepareError, match="MQTT password"):
        _prepare(paths, apply=True)
    assert calls == []


def test_session_inputs_require_vc0_through_vc2_only(tmp_path: Path) -> None:
    paths = _session_paths(tmp_path)
    _ledger(paths.vc2_ledger_path, include_vc3=True)

    with pytest.raises(CoordinatedSessionPrepareError, match="VC3.*not run"):
        _prepare(paths, apply=False)


def test_session_roots_reject_any_extra_or_preexisting_runtime_entry(tmp_path: Path) -> None:
    paths = _session_paths(tmp_path)
    _ledger(paths.vc2_ledger_path)
    (paths.input_root / "unexpected").write_text("unexpected", encoding="utf-8")

    with pytest.raises(CoordinatedSessionPrepareError, match="input.*inventory"):
        _prepare(paths, apply=False)

    (paths.input_root / "unexpected").unlink()
    (paths.output_root / "stale-result.json").write_text("{}", encoding="utf-8")
    with pytest.raises(CoordinatedSessionPrepareError, match="output.*empty"):
        _prepare(paths, apply=False)

    (paths.output_root / "stale-result.json").unlink()
    (paths.control_root / "stale.lock").touch(mode=0o600)
    with pytest.raises(CoordinatedSessionPrepareError, match="control.*empty"):
        _prepare(paths, apply=False)


def test_mqtt_password_must_be_bounded_nonempty_utf8(tmp_path: Path) -> None:
    paths = _session_paths(tmp_path)
    _ledger(paths.vc2_ledger_path)
    paths.mqtt_password_path.write_bytes(b"\xff\xfe")
    paths.mqtt_password_path.chmod(0o640)

    with pytest.raises(CoordinatedSessionPrepareError, match="UTF-8"):
        _prepare(paths, apply=False)

    paths.mqtt_password_path.write_bytes(b"\n")
    with pytest.raises(CoordinatedSessionPrepareError, match="must not be empty"):
        _prepare(paths, apply=False)


def test_coordinator_loader_revalidates_consumed_inputs_before_bus_access(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _session_paths(tmp_path)
    ledger_digest = _ledger(paths.vc2_ledger_path)
    _write_approval(paths, _approval_payload(ledger_sha256=ledger_digest))
    monkeypatch.setattr(
        "tools.brilliant_vc.session_coordinator._validate_runtime_credentials",
        lambda *_args, **_kwargs: ("b" * 32, "a" * 64),
    )
    monkeypatch.setattr(
        "tools.brilliant_vc.single_light_pilot._canonical_vc_socket",
        lambda value: value,
    )

    loaded = load_coordinator_session(
        paths,
        now_s=NOW_S,
        runtime_uid=os.getuid(),
        runtime_gid=os.getgid(),
        credential_uid=os.getuid(),
        allowed_input_roots=(paths.input_root,),
        allowed_output_roots=(paths.output_root,),
        allowed_control_roots=(paths.control_root,),
        allowed_approval_marker_paths=(paths.approval_marker_path,),
    )

    assert loaded.approval.run_id == RUN_ID
    assert loaded.vc2_ledger_sha256 == ledger_digest
    assert loaded.config.vc_device_id == "b" * 32
    assert loaded.config.stable_id == STABLE_ID
    assert loaded.mqtt_password is None

    paths.approval_source_path.write_text("{}", encoding="utf-8")
    with pytest.raises(SessionCoordinatorError, match="unconsumed"):
        load_coordinator_session(
            paths,
            now_s=NOW_S,
            runtime_uid=os.getuid(),
            runtime_gid=os.getgid(),
            credential_uid=os.getuid(),
            allowed_input_roots=(paths.input_root,),
            allowed_output_roots=(paths.output_root,),
            allowed_control_roots=(paths.control_root,),
            allowed_approval_marker_paths=(paths.approval_marker_path,),
        )
