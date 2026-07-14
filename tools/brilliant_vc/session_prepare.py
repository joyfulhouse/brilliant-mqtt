"""Validate and prepare one coordinated VC session without starting it.

Dry-run mode validates the immutable VC2 ledger, optional broker-password
input, empty session roots, pinned runtime, and credentials. Apply additionally
requires the consumed coordinated-session approval and then delegates only the
captured ``run.pre_exec`` operation to :mod:`runtime_prepare`. This module has
no socket, uWSGI, Emperor, or managed-process start capability.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from tools.brilliant_vc.gates import GateLedger, GateName, GateStatus
from tools.brilliant_vc.launcher_preflight import (
    LauncherPaths,
    LauncherPreflightError,
    hash_firmware_modules,
)
from tools.brilliant_vc.runtime_prepare import (
    FirmwarePreparer,
    RuntimeApproval,
    RuntimePrepareError,
    _default_paths,
    _directory,
    _entry_names,
    _paths_overlap,
    _read_file,
    _runtime_account,
    prepare_runtime_no_start,
)
from tools.brilliant_vc.session_approval import (
    SessionApproval,
    SessionApprovalError,
    validate_session_approval,
)

_RUNTIME_USER = "brilliant-vc"
_MAX_LEDGER_BYTES = 256 * 1024
_MAX_PASSWORD_BYTES = 4 * 1024
_DEFAULT_INPUT_ROOTS = (Path("/data/brilliant-vc-session-input"),)
_DEFAULT_OUTPUT_ROOTS = (Path("/data/brilliant-vc-session"),)
_DEFAULT_CONTROL_ROOTS = (Path("/run/brilliant-vc-session"),)
_DEFAULT_APPROVAL_MARKERS = (
    Path("/run/brilliant-vc-session-approval/session-approval-consumed.json"),
)
_DEFAULT_PERSISTENT_ROOTS = (Path("/data/brilliant-vc"),)
_DEFAULT_RUNTIME_ROOTS = (Path("/run/brilliant-vc"), Path("/var/run/brilliant-vc"))
_DEFAULT_CREDENTIAL_PATHS = (Path("/data/brilliant-vc-credentials"),)


class CoordinatedSessionPrepareError(ValueError):
    """Raised before a coordinated session can reach process start."""


@dataclass(frozen=True, slots=True)
class SessionPreparePaths:
    """Complete fixed path surface for one coordinated session."""

    launcher: LauncherPaths
    input_root: Path
    vc2_ledger_path: Path
    mqtt_password_path: Path
    output_root: Path
    control_root: Path
    approval_source_path: Path
    approval_marker_path: Path


@dataclass(frozen=True, slots=True)
class CoordinatedSessionPrepareResult:
    """Secret-free validation/preparation result."""

    dry_run: bool
    session_inputs_valid: bool
    session_roots_valid: bool
    runtime_preparation_complete: bool
    approval_validated: bool
    approval_consumed: bool
    vc2_ledger_run_id: str
    vc2_gate_ledger_sha256: str
    mqtt_password_sha256: str | None
    runtime_credential_bundle_sha256: str
    approval_run_id: str | None
    approval_sha256: str | None
    initial_vassals: tuple[str, ...]
    emperor_started: bool
    blocked_reason: str | None

    def to_public_dict(self) -> dict[str, object]:
        return {
            "dry_run": self.dry_run,
            "session_inputs_valid": self.session_inputs_valid,
            "session_roots_valid": self.session_roots_valid,
            "runtime_preparation_complete": self.runtime_preparation_complete,
            "approval_validated": self.approval_validated,
            "approval_consumed": self.approval_consumed,
            "vc2_ledger_run_id": self.vc2_ledger_run_id,
            "vc2_gate_ledger_sha256": self.vc2_gate_ledger_sha256,
            "mqtt_password_sha256": self.mqtt_password_sha256,
            "runtime_credential_bundle_sha256": self.runtime_credential_bundle_sha256,
            "approval_run_id": self.approval_run_id,
            "approval_sha256": self.approval_sha256,
            "initial_vassals": list(self.initial_vassals),
            "emperor_started": self.emperor_started,
            "blocked_reason": self.blocked_reason,
        }


@dataclass(frozen=True, slots=True)
class ValidatedSessionInputs:
    """Digests and run identity from the fixed immutable session inputs."""

    vc2_ledger_run_id: str
    vc2_gate_ledger_sha256: str
    mqtt_password_sha256: str | None
    vc2_ledger: GateLedger


def validate_coordinated_session_inputs(
    paths: SessionPreparePaths,
    *,
    runtime_uid: int,
    runtime_gid: int,
    credential_uid: int = 0,
    allowed_input_roots: Sequence[Path] = _DEFAULT_INPUT_ROOTS,
    allowed_output_roots: Sequence[Path] = _DEFAULT_OUTPUT_ROOTS,
    allowed_control_roots: Sequence[Path] = _DEFAULT_CONTROL_ROOTS,
) -> ValidatedSessionInputs:
    """Validate only fixed session roots, the VC2 ledger, and password input."""

    _validate_session_roots(
        paths,
        runtime_uid=runtime_uid,
        runtime_gid=runtime_gid,
        credential_uid=credential_uid,
        allowed_input_roots=allowed_input_roots,
        allowed_output_roots=allowed_output_roots,
        allowed_control_roots=allowed_control_roots,
    )
    ledger, ledger_digest = _validate_vc2_ledger(
        paths.vc2_ledger_path,
        credential_uid=credential_uid,
        runtime_gid=runtime_gid,
    )
    password_digest = _validate_optional_password(
        paths,
        credential_uid=credential_uid,
        runtime_gid=runtime_gid,
    )
    return ValidatedSessionInputs(
        vc2_ledger_run_id=ledger.run_id,
        vc2_gate_ledger_sha256=ledger_digest,
        mqtt_password_sha256=password_digest,
        vc2_ledger=ledger,
    )


def prepare_coordinated_session_no_start(
    paths: SessionPreparePaths,
    *,
    now_s: int,
    apply: bool,
    runtime_user: str,
    runtime_uid: int,
    runtime_gid: int,
    actual_module_hashes: Mapping[str, object],
    firmware_preparer: FirmwarePreparer | None = None,
    credential_uid: int = 0,
    allowed_input_roots: Sequence[Path] = _DEFAULT_INPUT_ROOTS,
    allowed_output_roots: Sequence[Path] = _DEFAULT_OUTPUT_ROOTS,
    allowed_control_roots: Sequence[Path] = _DEFAULT_CONTROL_ROOTS,
    allowed_approval_marker_paths: Sequence[Path] = _DEFAULT_APPROVAL_MARKERS,
    allowed_persistent_roots: Sequence[Path] = _DEFAULT_PERSISTENT_ROOTS,
    allowed_runtime_roots: Sequence[Path] = _DEFAULT_RUNTIME_ROOTS,
    allowed_runtime_credential_paths: Sequence[Path] = _DEFAULT_CREDENTIAL_PATHS,
) -> CoordinatedSessionPrepareResult:
    """Validate session inputs and optionally run only stock ``pre_exec``."""

    if not isinstance(apply, bool):
        raise CoordinatedSessionPrepareError("apply must be a boolean")
    if runtime_user != _RUNTIME_USER:
        raise CoordinatedSessionPrepareError("runtime user must match brilliant-vc")
    if isinstance(now_s, bool) or not isinstance(now_s, int) or now_s <= 0:
        raise CoordinatedSessionPrepareError("current timestamp is invalid")
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value < 0
        for value in (runtime_uid, runtime_gid, credential_uid)
    ):
        raise CoordinatedSessionPrepareError("runtime or credential identity is invalid")

    inputs = validate_coordinated_session_inputs(
        paths,
        runtime_uid=runtime_uid,
        runtime_gid=runtime_gid,
        credential_uid=credential_uid,
        allowed_input_roots=allowed_input_roots,
        allowed_output_roots=allowed_output_roots,
        allowed_control_roots=allowed_control_roots,
    )
    ledger_run_id = inputs.vc2_ledger_run_id
    ledger_digest = inputs.vc2_gate_ledger_sha256
    password_digest = inputs.mqtt_password_sha256

    approval: SessionApproval | None = None
    if apply:
        if paths.approval_source_path.exists() or paths.approval_source_path.is_symlink():
            raise CoordinatedSessionPrepareError("unconsumed session approval still exists")
        try:
            approval = validate_session_approval(
                paths.approval_marker_path,
                now_s=now_s,
                credential_uid=credential_uid,
                runtime_gid=runtime_gid,
                allowed_paths=allowed_approval_marker_paths,
                phase="start",
            )
        except SessionApprovalError as error:
            raise CoordinatedSessionPrepareError(str(error)) from None
        if approval.run_id != ledger_run_id:
            raise CoordinatedSessionPrepareError(
                "session approval run ID does not match gate ledger"
            )
        if approval.vc2_gate_ledger_sha256 != ledger_digest:
            raise CoordinatedSessionPrepareError(
                "session approval does not bind the VC2 gate ledger"
            )
        if approval.mqtt_password_sha256 != password_digest:
            raise CoordinatedSessionPrepareError("session approval does not bind the MQTT password")

    def session_start_validator(
        path: Path,
        *,
        now_s: int,
        credential_uid: int,
        runtime_gid: int,
        allowed_paths: Sequence[Path],
    ) -> RuntimeApproval:
        try:
            return validate_session_approval(
                path,
                now_s=now_s,
                credential_uid=credential_uid,
                runtime_gid=runtime_gid,
                allowed_paths=allowed_paths,
                phase="start",
            )
        except SessionApprovalError as error:
            raise CoordinatedSessionPrepareError(str(error)) from None

    try:
        runtime = prepare_runtime_no_start(
            paths.launcher,
            now_s=now_s,
            apply=apply,
            approval_marker=paths.approval_marker_path if apply else None,
            runtime_user=runtime_user,
            runtime_uid=runtime_uid,
            runtime_gid=runtime_gid,
            credential_uid=credential_uid,
            actual_module_hashes=actual_module_hashes,
            firmware_preparer=firmware_preparer,
            approval_validator=session_start_validator,
            allowed_persistent_roots=allowed_persistent_roots,
            allowed_runtime_roots=allowed_runtime_roots,
            allowed_runtime_credential_paths=allowed_runtime_credential_paths,
            allowed_approval_marker_paths=allowed_approval_marker_paths,
            unconsumed_approval_paths=(paths.approval_source_path,),
        )
    except RuntimePrepareError as error:
        raise CoordinatedSessionPrepareError(str(error)) from None
    if apply:
        assert approval is not None
        if (
            not runtime.approval_validated
            or not runtime.approval_consumed
            or runtime.approval_run_id != approval.run_id
            or runtime.runtime_credential_bundle_sha256 != approval.runtime_credential_bundle_sha256
        ):
            raise CoordinatedSessionPrepareError(
                "runtime preparation approval result is inconsistent"
            )

    return CoordinatedSessionPrepareResult(
        dry_run=not apply,
        session_inputs_valid=True,
        session_roots_valid=True,
        runtime_preparation_complete=runtime.preparation_complete,
        approval_validated=runtime.approval_validated,
        approval_consumed=runtime.approval_consumed,
        vc2_ledger_run_id=ledger_run_id,
        vc2_gate_ledger_sha256=ledger_digest,
        mqtt_password_sha256=password_digest,
        runtime_credential_bundle_sha256=runtime.runtime_credential_bundle_sha256,
        approval_run_id=None if approval is None else approval.run_id,
        approval_sha256=None if approval is None else approval.sha256,
        initial_vassals=runtime.initial_vassals,
        emperor_started=runtime.emperor_started,
        blocked_reason=(None if apply else "fresh_coordinated_session_approval_required"),
    )


def _validate_session_roots(
    paths: SessionPreparePaths,
    *,
    runtime_uid: int,
    runtime_gid: int,
    credential_uid: int,
    allowed_input_roots: Sequence[Path],
    allowed_output_roots: Sequence[Path],
    allowed_control_roots: Sequence[Path],
) -> None:
    try:
        input_root = _directory(
            paths.input_root,
            description="session input root",
            uid=credential_uid,
            gid=runtime_gid,
            mode=0o750,
        )
        output_root = _directory(
            paths.output_root,
            description="session output root",
            uid=runtime_uid,
            gid=runtime_gid,
            mode=0o700,
        )
        control_root = _directory(
            paths.control_root,
            description="session control root",
            uid=runtime_uid,
            gid=runtime_gid,
            mode=0o700,
        )
        approval_root = _directory(
            paths.approval_marker_path.parent,
            description="session approval root",
            uid=credential_uid,
            gid=runtime_gid,
            mode=0o750,
        )
    except RuntimePrepareError as error:
        raise CoordinatedSessionPrepareError(str(error)) from None
    if input_root not in {path.resolve(strict=False) for path in allowed_input_roots}:
        raise CoordinatedSessionPrepareError("session input root is outside the allowed roots")
    if output_root not in {path.resolve(strict=False) for path in allowed_output_roots}:
        raise CoordinatedSessionPrepareError("session output root is outside the allowed roots")
    if control_root not in {path.resolve(strict=False) for path in allowed_control_roots}:
        raise CoordinatedSessionPrepareError("session control root is outside the allowed roots")
    if paths.vc2_ledger_path.resolve(strict=False) != input_root / "gate-ledger-vc2.json":
        raise CoordinatedSessionPrepareError("VC2 gate ledger path is not canonical")
    if paths.mqtt_password_path.resolve(strict=False) != input_root / "mqtt-password":
        raise CoordinatedSessionPrepareError("MQTT password path is not canonical")
    if paths.approval_source_path.parent.resolve(strict=False) != approval_root:
        raise CoordinatedSessionPrepareError("session approval source is outside its control root")
    if paths.approval_source_path.name != "session-approval.json":
        raise CoordinatedSessionPrepareError("session approval source path is not canonical")
    if paths.approval_marker_path.name != "session-approval-consumed.json":
        raise CoordinatedSessionPrepareError("session approval marker path is not canonical")
    roots = (
        input_root,
        output_root,
        control_root,
        approval_root,
        paths.launcher.persistent_root.resolve(strict=False),
        paths.launcher.runtime_dir.resolve(strict=False),
        paths.launcher.runtime_credential_dir.resolve(strict=False),
    )
    for index, left in enumerate(roots):
        if any(_paths_overlap(left, right) for right in roots[index + 1 :]):
            raise CoordinatedSessionPrepareError("coordinated session roots must be disjoint")
    try:
        input_entries = _entry_names(input_root, description="session input root")
        output_entries = _entry_names(output_root, description="session output root")
        control_entries = _entry_names(control_root, description="session control root")
    except RuntimePrepareError as error:
        raise CoordinatedSessionPrepareError(str(error)) from None
    expected_input = {"gate-ledger-vc2.json"}
    if "mqtt-password" in input_entries:
        expected_input.add("mqtt-password")
    if input_entries != expected_input:
        raise CoordinatedSessionPrepareError("session input root has an unexpected inventory")
    if output_entries:
        raise CoordinatedSessionPrepareError("session output root must be empty")
    if control_entries:
        raise CoordinatedSessionPrepareError("session control root must be empty")


def _validate_vc2_ledger(
    path: Path,
    *,
    credential_uid: int,
    runtime_gid: int,
) -> tuple[GateLedger, str]:
    try:
        raw = _read_file(
            path,
            description="VC2 gate ledger",
            uid=credential_uid,
            gid=runtime_gid,
            mode=0o640,
            maximum_bytes=_MAX_LEDGER_BYTES,
        )
    except RuntimePrepareError as error:
        raise CoordinatedSessionPrepareError(str(error)) from None
    try:
        digest = hashlib.sha256(raw).hexdigest()
        try:
            ledger = GateLedger.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            raise CoordinatedSessionPrepareError("VC2 gate ledger is invalid") from None
    finally:
        for index in range(len(raw)):
            raw[index] = 0
    for gate in (GateName.VC0, GateName.VC1, GateName.VC2):
        if ledger.status(gate) is not GateStatus.PASS:
            raise CoordinatedSessionPrepareError(f"{gate.value} must pass in the VC2 gate ledger")
    for gate in (GateName.VC3, GateName.VC4, GateName.VC5):
        if ledger.status(gate) is not GateStatus.NOT_RUN:
            raise CoordinatedSessionPrepareError(f"{gate.value} must remain not run")
    return ledger, digest


def _validate_optional_password(
    paths: SessionPreparePaths,
    *,
    credential_uid: int,
    runtime_gid: int,
) -> str | None:
    if not paths.mqtt_password_path.exists() and not paths.mqtt_password_path.is_symlink():
        return None
    try:
        raw = _read_file(
            paths.mqtt_password_path,
            description="MQTT password",
            uid=credential_uid,
            gid=runtime_gid,
            mode=0o640,
            maximum_bytes=_MAX_PASSWORD_BYTES,
        )
    except RuntimePrepareError as error:
        raise CoordinatedSessionPrepareError(str(error)) from None
    try:
        digest = hashlib.sha256(raw).hexdigest()
        try:
            decoded = raw.decode("utf-8")
        except UnicodeDecodeError:
            raise CoordinatedSessionPrepareError("MQTT password is not valid UTF-8") from None
        if not decoded.rstrip("\r\n"):
            raise CoordinatedSessionPrepareError("MQTT password must not be empty")
        if "\x00" in decoded:
            raise CoordinatedSessionPrepareError("MQTT password contains a null byte")
        return digest
    finally:
        for index in range(len(raw)):
            raw[index] = 0


def _default_session_paths() -> SessionPreparePaths:
    input_root = Path("/data/brilliant-vc-session-input")
    approval_root = Path("/run/brilliant-vc-session-approval")
    return SessionPreparePaths(
        launcher=_default_paths(),
        input_root=input_root,
        vc2_ledger_path=input_root / "gate-ledger-vc2.json",
        mqtt_password_path=input_root / "mqtt-password",
        output_root=Path("/data/brilliant-vc-session"),
        control_root=Path("/run/brilliant-vc-session"),
        approval_source_path=approval_root / "session-approval.json",
        approval_marker_path=approval_root / "session-approval-consumed.json",
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    runtime_uid, runtime_gid = _runtime_account()
    try:
        hashes = hash_firmware_modules(required_uid=0)
    except LauncherPreflightError as error:
        raise CoordinatedSessionPrepareError(str(error)) from None
    result = prepare_coordinated_session_no_start(
        _default_session_paths(),
        now_s=int(time.time()),
        apply=bool(args.apply),
        runtime_user=_RUNTIME_USER,
        runtime_uid=runtime_uid,
        runtime_gid=runtime_gid,
        credential_uid=0,
        actual_module_hashes=hashes,
    )
    print(json.dumps(result.to_public_dict(), sort_keys=True))
    if result.dry_run:
        print("DRY RUN — no firmware imported and no process started")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except CoordinatedSessionPrepareError as error:
        print(f"VC coordinated-session preparation blocked: {error}", file=sys.stderr)
        sys.exit(2)
