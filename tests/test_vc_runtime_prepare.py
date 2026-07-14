from __future__ import annotations

import ast
import hashlib
import json
import os
import shutil
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools.brilliant_vc.launcher_preflight import LauncherPaths
from tools.brilliant_vc.runtime_handoff import runtime_credential_bundle_sha256
from tools.brilliant_vc.runtime_prepare import (
    FirmwarePreparer,
    RuntimeApproval,
    RuntimePrepareError,
    RuntimePrepareResult,
    _runtime_account,
    _StockFirmwarePreparer,
    prepare_runtime_no_start,
)
from tools.brilliant_vc.start_approval import validate_start_approval
from tools.brilliant_vc.vassal_manifest import build_candidate_manifest

DEVICE_ID = "a" * 32
NOW_S = 1_800_000_000
RUNTIME_USER = "brilliant-vc"


def _private_file(path: Path, value: bytes, mode: int) -> None:
    path.write_bytes(value)
    path.chmod(mode)


def _certificate_pair() -> tuple[bytes, bytes]:
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
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    return (
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ),
        certificate.public_bytes(serialization.Encoding.PEM),
    )


def _paths(tmp_path: Path) -> LauncherPaths:
    private = tmp_path / "private-unused-by-service"
    persistent = tmp_path / "data" / "brilliant-vc"
    runtime = tmp_path / "run" / "brilliant-vc"
    credentials = tmp_path / "data" / "brilliant-vc-credentials"
    certificate_dir = credentials / "certificates"
    persistent.mkdir(parents=True, mode=0o700)
    runtime.mkdir(parents=True, mode=0o700)
    credentials.mkdir(parents=True, mode=0o750)
    certificate_dir.mkdir(mode=0o750)
    children = {
        "state_dir": persistent / "state",
        "process_config_dir": persistent / "process-config",
        "process_flagfile_dir": persistent / "flagfiles",
        "startable_config_dir": persistent / "startable-configs",
        "log_dir": persistent / "logs",
        "error_log_dir": persistent / "errors",
        "trace_dir": persistent / "traces",
    }
    for directory in children.values():
        directory.mkdir(mode=0o700)
    for directory in (persistent, runtime, *children.values()):
        os.chown(directory, os.getuid(), os.getgid())
    for directory in (credentials, certificate_dir):
        os.chown(directory, os.getuid(), os.getgid())
    key, certificate = _certificate_pair()
    for path, value in (
        (credentials / "device_id", f"{DEVICE_ID}\n".encode()),
        (credentials / "bootstrap", b"opaque-bootstrap"),
        (certificate_dir / "device.key", key),
        (certificate_dir / "device.cert", certificate),
    ):
        _private_file(path, value, 0o640)
        os.chown(path, os.getuid(), os.getgid())
    metadata = tmp_path / "metadata"
    metadata.mkdir(mode=0o700)
    release = metadata / "release_info.json"
    tracking = metadata / "tracking_branch"
    art_preload = metadata / "art_library"
    art_preload.mkdir(mode=0o755)
    _private_file(release, b"{}\n", 0o644)
    _private_file(tracking, b"stable\n", 0o644)
    return LauncherPaths(
        private_root=private,
        persistent_root=persistent,
        identity_dir=private / "identity",
        materialized_certificate_dir=private / "materialized-certificates",
        runtime_credential_dir=credentials,
        bootstrap_path=credentials / "bootstrap",
        state_dir=children["state_dir"],
        certificate_dir=certificate_dir,
        process_config_dir=children["process_config_dir"],
        process_flagfile_dir=children["process_flagfile_dir"],
        startable_config_dir=children["startable_config_dir"],
        log_dir=children["log_dir"],
        error_log_dir=children["error_log_dir"],
        trace_dir=children["trace_dir"],
        runtime_dir=runtime,
        socket_path=runtime / "server_socket",
        stats_socket_path=runtime / "uwsgi_stats_socket",
        release_info_path=release,
        tracking_branch_path=tracking,
        art_preload_dir=art_preload,
    )


def _module_hashes() -> dict[str, str]:
    snapshot = (
        Path(__file__).parents[1]
        / "docs/brilliant-panel/virtual-control-launcher-snapshot-v5.example.json"
    )
    payload = json.loads(snapshot.read_text(encoding="utf-8"))
    hashes = payload["runtime_sha256"]
    assert isinstance(hashes, dict)
    return {str(name): str(value) for name, value in hashes.items()}


def _approval(path: Path, *, approved_at_s: int = NOW_S) -> None:
    example = (
        Path(__file__).parents[1]
        / "docs/brilliant-panel/virtual-control-start-approval.example.json"
    )
    payload = json.loads(example.read_text(encoding="utf-8"))
    payload["approved_at_s"] = approved_at_s
    payload["run_id"] = "office-vc-bootstrap-01"
    credential_root = path.parents[1] / "data/brilliant-vc-credentials"
    payload["runtime_credential_bundle_sha256"] = runtime_credential_bundle_sha256(
        {
            "device_id": (credential_root / "device_id").read_bytes(),
            "bootstrap": (credential_root / "bootstrap").read_bytes(),
            "device.key": (credential_root / "certificates/device.key").read_bytes(),
            "device.cert": (credential_root / "certificates/device.cert").read_bytes(),
        }
    )
    path.parent.mkdir(mode=0o750, exist_ok=True)
    path.parent.chmod(0o750)
    os.chown(path.parent, os.getuid(), os.getgid())
    _private_file(path, json.dumps(payload).encode(), 0o640)
    os.chown(path, os.getuid(), os.getgid())


class FakeFirmwarePreparer(FirmwarePreparer):
    def __init__(
        self,
        paths: LauncherPaths,
        *,
        symlink_vassal: bool = False,
        omit_bootstrap_flagfile: bool = False,
        root_vassal_override: bool = False,
        unexpected_state_file: bool = False,
    ) -> None:
        self.paths = paths
        self.symlink_vassal = symlink_vassal
        self.omit_bootstrap_flagfile = omit_bootstrap_flagfile
        self.root_vassal_override = root_vassal_override
        self.unexpected_state_file = unexpected_state_file
        self.calls: list[tuple[tuple[str, ...], str]] = []

    def prepare(self, argv: Sequence[str], *, runtime_user: str) -> None:
        self.calls.append((tuple(argv), runtime_user))
        candidate_flags = dict(argument[2:].split("=", 1) for argument in argv[1:])
        for directory in (
            self.paths.process_config_dir,
            self.paths.process_flagfile_dir,
            self.paths.startable_config_dir,
            self.paths.error_log_dir,
        ):
            shutil.rmtree(directory)
            directory.mkdir(mode=0o777)
        message_bus = self.paths.process_config_dir / "message_bus.ini"
        if self.symlink_vassal:
            message_bus.symlink_to(self.paths.runtime_credential_dir / "device_id")
        else:
            _private_file(
                message_bus,
                (
                    "[uwsgi]\n"
                    "startable_module = bus.message_bus\n"
                    f"flagfile = {self.paths.process_flagfile_dir / 'message_bus_flagfile'}\n"
                    "additionalflags = \n"
                    "prio = -10\n"
                    f"user_override = {0 if self.root_vassal_override else os.getuid()}\n"
                    f"group_override = {0 if self.root_vassal_override else os.getgid()}\n"
                ).encode(),
                0o644,
            )
        base = {
            "asyncio_debug": "False",
            "enable_uwsgi_heartbeat": "True",
            "error_log_sample_rate": "0.0",
            "error_log_storage_dir": str(self.paths.error_log_dir),
            "log_level": "INFO",
            "log_output_directory": str(self.paths.log_dir),
            "message_bus_server_socket_path": str(self.paths.socket_path),
            "process_configs_dir": str(self.paths.process_config_dir),
            "release_info_filepath": str(self.paths.release_info_path),
            "socket_timeout_seconds": "5",
            "thrift_serialization_validation_mode": "loose",
            "trace_dir": str(self.paths.trace_dir),
            "tracking_branch_filepath": str(self.paths.tracking_branch_path),
        }
        flagfiles = {
            "message_bus_flagfile": {
                **base,
                "cert_dir": str(self.paths.certificate_dir),
                "device_id": candidate_flags["device_id"],
                "disable_peripherals": candidate_flags["disable_peripherals"],
                "home_id": "0",
                "mb_state_dir": str(self.paths.state_dir),
                "message_bus_unprivileged_user": runtime_user,
                "process_flagfiles_dir": str(self.paths.process_flagfile_dir),
                "start_as_virtual_control": "True",
                "startable_host_configs_dir": str(self.paths.startable_config_dir),
            },
            "discovery_peripheral_flagfile": {
                **base,
                "discovery_peripheral_enable_remote_bridge_service_discovery": "True",
            },
            "config_peripherals_flagfile": dict(base),
            "bootstrap_flagfile": {
                **base,
                "bootstrap_max_provisioning_attempts_per_code": "1",
                "bootstrap_web_api_homes_endpoint": "/homes",
                "cert_dir": str(self.paths.certificate_dir),
                "saved_bootstrap_parameters_path": str(self.paths.bootstrap_path),
                "stub_bootstrap": "False",
            },
        }
        for name, values in flagfiles.items():
            if name == "bootstrap_flagfile" and self.omit_bootstrap_flagfile:
                continue
            _private_file(
                self.paths.process_flagfile_dir / name,
                "".join(f"--{key}={value}\n" for key, value in values.items()).encode(),
                0o644,
            )
        _private_file(
            self.paths.startable_config_dir / "message_bus",
            (
                "[remote_bridge]\n"
                "module_path = bridge.remote_bridge\n"
                "listen_port = 15455\n"
                "enable_bluetooth_provisioning = False\n"
                "device_provisioning_ip_listen_port = 0\n"
                "enforce_strict_authentication = True\n"
                "ble_mesh_debug_interface_listen_port = 0\n"
                "stub_ble_peripheral = True\n"
                f"uwsgi_stats_socket_path = {self.paths.stats_socket_path}\n"
            ).encode(),
            0o644,
        )
        if self.unexpected_state_file:
            _private_file(self.paths.state_dir / "unexpected", b"unexpected", 0o600)
        _private_file(
            self.paths.startable_config_dir / "config_peripherals",
            (
                "[art_config_peripheral]\n"
                "module_path = peripherals.configs.art_config_peripheral\n"
                f"art_preload_dir = {self.paths.art_preload_dir}\n\n"
                "[device_config_peripheral]\n"
                "module_path = peripherals.configs.device_config_peripheral\n\n"
                "[motion_detection_config_peripheral]\n"
                "module_path = peripherals.configs.motion_detection_config_peripheral\n\n"
                "[alarm_config_peripheral]\n"
                "module_path = peripherals.configs.alarm_config_peripheral\n"
            ).encode(),
            0o644,
        )


def _prepare(
    paths: LauncherPaths,
    *,
    apply: bool,
    approval_file: Path | None = None,
    preparer: FirmwarePreparer | None = None,
    hashes: dict[str, str] | None = None,
) -> RuntimePrepareResult:
    approval_marker: Path | None = None
    if apply and approval_file is not None:
        control_dir = approval_file.parent
        approval_marker = control_dir / "start-approval-consumed.json"
        if not approval_marker.exists() and approval_file.exists():
            approval_file.rename(approval_marker)
    return prepare_runtime_no_start(
        paths,
        now_s=NOW_S,
        apply=apply,
        approval_marker=approval_marker,
        runtime_user=RUNTIME_USER,
        runtime_uid=os.getuid(),
        runtime_gid=os.getgid(),
        credential_uid=os.getuid(),
        actual_module_hashes=hashes or _module_hashes(),
        firmware_preparer=preparer,
        allowed_persistent_roots=(paths.persistent_root,),
        allowed_runtime_roots=(paths.runtime_dir,),
        allowed_runtime_credential_paths=(paths.runtime_credential_dir,),
        allowed_approval_marker_paths=(() if approval_marker is None else (approval_marker,)),
        unconsumed_approval_paths=(() if approval_file is None else (approval_file,)),
    )


def test_dry_run_validates_without_importing_firmware_or_writing(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    preparer = FakeFirmwarePreparer(paths)

    result = _prepare(paths, apply=False, preparer=preparer)

    assert preparer.calls == []
    assert not any(paths.runtime_dir.iterdir())
    assert result.to_public_dict() == {
        "dry_run": True,
        "firmware_matches": True,
        "runtime_identity_valid": True,
        "runtime_credentials_valid": True,
        "approval_validated": False,
        "preparation_complete": False,
        "approval_consumed": False,
        "initial_vassals": [],
        "device_id_redacted": "aaaa…aaaa",
        "runtime_credential_bundle_sha256": runtime_credential_bundle_sha256(
            {
                "device_id": f"{DEVICE_ID}\n".encode(),
                "bootstrap": b"opaque-bootstrap",
                "device.key": paths.certificate_dir.joinpath("device.key").read_bytes(),
                "device.cert": paths.certificate_dir.joinpath("device.cert").read_bytes(),
            }
        ),
        "approval_run_id": None,
        "approval_sha256": None,
        "disabled_process_count": 34,
        "contains_emperor_start_primitive": False,
        "emperor_started": False,
        "blocked_reason": "fresh_start_approval_required",
    }
    assert DEVICE_ID not in json.dumps(result.to_public_dict())


def test_apply_prepares_exact_nonroot_stock_contract_and_hardens_outputs(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    approval = tmp_path / "approval-control/start-approval.json"
    _approval(approval)
    preparer = FakeFirmwarePreparer(paths)

    result = _prepare(paths, apply=True, approval_file=approval, preparer=preparer)

    assert len(preparer.calls) == 1
    argv, user = preparer.calls[0]
    assert user == RUNTIME_USER
    assert argv[0] == "brilliant-vc-runtime-prepare"
    flags = dict(argument[2:].split("=", 1) for argument in argv[1:])
    assert flags["device_id"] == DEVICE_ID
    assert flags["home_id"] == "0"
    assert flags["start_as_virtual_control"] == "True"
    assert flags["message_bus_unprivileged_user"] == RUNTIME_USER
    assert flags["process_configs_dir"] == str(paths.process_config_dir)
    assert flags["cert_dir"] == str(paths.certificate_dir)
    assert flags["saved_bootstrap_parameters_path"] == str(paths.bootstrap_path)
    assert "message_bus_address_override" not in flags
    disabled = flags["disable_peripherals"].split(",")
    assert len(disabled) == 34
    assert "message_bus" not in disabled
    assert "discovery_peripheral" not in disabled
    assert "config_peripherals" not in disabled
    assert "bootstrap" not in disabled
    for directory in (
        paths.process_config_dir,
        paths.process_flagfile_dir,
        paths.startable_config_dir,
        paths.error_log_dir,
    ):
        assert directory.stat().st_mode & 0o777 == 0o700
        for child in directory.iterdir():
            assert child.stat().st_mode & 0o777 == 0o600
    assert not any(paths.runtime_dir.iterdir())
    assert result.preparation_complete is True
    assert result.initial_vassals == ("message_bus.ini",)
    assert result.emperor_started is False
    assert result.approval_run_id == "office-vc-bootstrap-01"
    assert result.approval_sha256 is not None and len(result.approval_sha256) == 64
    marker = approval.parent / "start-approval-consumed.json"
    assert marker.exists()
    assert approval.exists() is False
    assert marker.parent != paths.runtime_dir


def test_approval_validator_is_explicitly_injectable_without_changing_the_default(
    tmp_path: Path,
) -> None:
    paths = _paths(tmp_path)
    source = tmp_path / "approval-control/start-approval.json"
    marker = source.parent / "start-approval-consumed.json"
    _approval(source)
    source.rename(marker)
    preparer = FakeFirmwarePreparer(paths)
    calls: list[Path] = []

    def validator(
        path: Path,
        *,
        now_s: int,
        credential_uid: int,
        runtime_gid: int,
        allowed_paths: Sequence[Path],
    ) -> RuntimeApproval:
        calls.append(path)
        return validate_start_approval(
            path,
            now_s=now_s,
            credential_uid=credential_uid,
            runtime_gid=runtime_gid,
            allowed_paths=allowed_paths,
        )

    result = prepare_runtime_no_start(
        paths,
        now_s=NOW_S,
        apply=True,
        approval_marker=marker,
        runtime_user=RUNTIME_USER,
        runtime_uid=os.getuid(),
        runtime_gid=os.getgid(),
        credential_uid=os.getuid(),
        actual_module_hashes=_module_hashes(),
        firmware_preparer=preparer,
        approval_validator=validator,
        allowed_persistent_roots=(paths.persistent_root,),
        allowed_runtime_roots=(paths.runtime_dir,),
        allowed_runtime_credential_paths=(paths.runtime_credential_dir,),
        allowed_approval_marker_paths=(marker,),
        unconsumed_approval_paths=(source,),
    )

    assert calls == [marker]
    assert result.approval_validated is True


def test_stock_preparer_limits_pre_exec_discovery_to_candidate_processes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _paths(tmp_path)
    manifest = build_candidate_manifest(paths).to_public_dict()
    enabled = manifest["enabled_processes"]
    disabled = manifest["disabled_processes"]
    assert isinstance(enabled, list) and all(isinstance(name, str) for name in enabled)
    assert isinstance(disabled, list) and all(isinstance(name, str) for name in disabled)
    process_names = [*enabled, *disabled]
    configs = tuple(SimpleNamespace(process_name=name) for name in process_names)

    parsed = False

    def get_all_configs() -> tuple[SimpleNamespace, ...]:
        if parsed:
            return tuple(config for config in configs if config.process_name in enabled)
        return configs

    process_configs = SimpleNamespace(get_all_configs=get_all_configs)
    calls: list[tuple[str, object]] = []
    run_module = SimpleNamespace(process_configs=process_configs)
    run_module.add_undefined_gflags = lambda parameters: calls.append(
        ("add", tuple(config.process_name for config in process_configs.get_all_configs()))
    )

    def parse_flags(argv: Sequence[str]) -> None:
        nonlocal parsed
        calls.append(("flags", tuple(argv)))
        parsed = True

    run_module.FLAGS = parse_flags
    run_module.pre_exec = lambda *, unprivileged_user: calls.append(
        (
            "pre_exec",
            (
                unprivileged_user,
                tuple(config.process_name for config in process_configs.get_all_configs()),
            ),
        )
    )
    socket_parameters = SimpleNamespace(get_uwsgi_socket_parameters=lambda: ("stats",))

    def import_module(name: str) -> object:
        if name == "run":
            return run_module
        if name == "configs.socket_parameters":
            return socket_parameters
        raise AssertionError(f"unexpected import: {name}")

    monkeypatch.setattr("tools.brilliant_vc.runtime_prepare.importlib.import_module", import_module)
    argv = ("prepare", "--home_id=0", f"--disable_peripherals={','.join(disabled)}")
    _StockFirmwarePreparer().prepare(argv, runtime_user=RUNTIME_USER)

    expected = ("message_bus", "discovery_peripheral", "config_peripherals", "bootstrap")
    assert calls[0] == ("add", expected)
    assert calls[1] == ("flags", argv)
    assert calls[2] == ("pre_exec", (RUNTIME_USER, expected))
    assert process_configs.get_all_configs is get_all_configs


def test_stock_preparer_rejects_process_inventory_drift_before_pre_exec(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = build_candidate_manifest(_paths(tmp_path)).to_public_dict()
    disabled = manifest["disabled_processes"]
    assert isinstance(disabled, list) and all(isinstance(name, str) for name in disabled)
    process_configs = SimpleNamespace(
        get_all_configs=lambda: (SimpleNamespace(process_name="message_bus"),)
    )
    run_module = SimpleNamespace(
        process_configs=process_configs,
        add_undefined_gflags=lambda _: pytest.fail("flag discovery must not run"),
        FLAGS=lambda _: pytest.fail("flag parsing must not run"),
        pre_exec=lambda **_: pytest.fail("pre_exec must not run"),
    )
    monkeypatch.setattr(
        "tools.brilliant_vc.runtime_prepare.importlib.import_module",
        lambda name: run_module if name == "run" else pytest.fail(f"unexpected import: {name}"),
    )

    with pytest.raises(RuntimePrepareError, match="process inventory drift"):
        _StockFirmwarePreparer().prepare(
            ("prepare", f"--disable_peripherals={','.join(disabled)}"),
            runtime_user=RUNTIME_USER,
        )


def test_apply_requires_fresh_exact_root_group_approval(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    preparer = FakeFirmwarePreparer(paths)

    with pytest.raises(RuntimePrepareError, match="requires a consumed approval marker"):
        _prepare(paths, apply=True, preparer=preparer)

    approval = tmp_path / "approval-control/start-approval.json"
    _approval(approval, approved_at_s=NOW_S - 601)
    with pytest.raises(RuntimePrepareError, match="older than 10 minutes"):
        _prepare(paths, apply=True, approval_file=approval, preparer=preparer)

    (approval.parent / "start-approval-consumed.json").unlink()
    _approval(approval, approved_at_s=NOW_S)
    approval.chmod(0o600)
    with pytest.raises(RuntimePrepareError, match="mode 0640"):
        _prepare(paths, apply=True, approval_file=approval, preparer=preparer)
    assert preparer.calls == []


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("physical_device_actions_permitted", True),
        ("hosted_light_permitted", True),
        ("runtime_limit_s", 601),
        ("panel", "backyard"),
    ],
)
def test_apply_rejects_any_broader_approval_scope(
    tmp_path: Path, field: str, value: object
) -> None:
    paths = _paths(tmp_path)
    approval = tmp_path / "approval-control/start-approval.json"
    _approval(approval)
    payload = json.loads(approval.read_text(encoding="utf-8"))
    payload[field] = value
    _private_file(approval, json.dumps(payload).encode(), 0o640)
    preparer = FakeFirmwarePreparer(paths)

    with pytest.raises(RuntimePrepareError, match="bootstrap-only run"):
        _prepare(paths, apply=True, approval_file=approval, preparer=preparer)

    assert preparer.calls == []


def test_apply_rejects_duplicate_approval_fields(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    approval = tmp_path / "approval-control/start-approval.json"
    _approval(approval)
    raw = approval.read_text(encoding="utf-8")
    raw = raw.replace('"approved": true,', '"approved": true, "approved": true,', 1)
    _private_file(approval, raw.encode(), 0o640)
    preparer = FakeFirmwarePreparer(paths)

    with pytest.raises(RuntimePrepareError, match="duplicate field"):
        _prepare(paths, apply=True, approval_file=approval, preparer=preparer)

    assert preparer.calls == []


def test_firmware_hash_drift_blocks_before_preparation(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    approval = tmp_path / "approval-control/start-approval.json"
    _approval(approval)
    preparer = FakeFirmwarePreparer(paths)
    hashes = _module_hashes()
    hashes["runtime.uwsgi"] = "0" * 64

    with pytest.raises(RuntimePrepareError, match="module hash drift"):
        _prepare(
            paths,
            apply=True,
            approval_file=approval,
            preparer=preparer,
            hashes=hashes,
        )
    assert preparer.calls == []


def test_approval_must_bind_the_exact_runtime_credential_bundle(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    approval = tmp_path / "approval-control/start-approval.json"
    _approval(approval)
    payload = json.loads(approval.read_text(encoding="utf-8"))
    payload["runtime_credential_bundle_sha256"] = "0" * 64
    _private_file(approval, json.dumps(payload).encode(), 0o640)
    os.chown(approval, os.getuid(), os.getgid())
    preparer = FakeFirmwarePreparer(paths)

    with pytest.raises(RuntimePrepareError, match="credential bundle"):
        _prepare(paths, apply=True, approval_file=approval, preparer=preparer)

    assert preparer.calls == []


def test_writable_art_preload_directory_blocks_before_preparation(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    paths.art_preload_dir.chmod(0o775)
    preparer = FakeFirmwarePreparer(paths)

    with pytest.raises(RuntimePrepareError, match="art preload.*0755"):
        _prepare(paths, apply=False, preparer=preparer)

    assert preparer.calls == []


def test_service_metadata_must_be_readable_by_nonroot_runtime(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    paths.release_info_path.chmod(0o600)
    preparer = FakeFirmwarePreparer(paths)

    with pytest.raises(RuntimePrepareError, match="release metadata.*0644"):
        _prepare(paths, apply=False, preparer=preparer)

    assert preparer.calls == []


def test_pre_exec_cannot_leave_state_or_persistent_root_extras(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    approval = tmp_path / "approval-control/start-approval.json"
    _approval(approval)
    preparer = FakeFirmwarePreparer(paths, unexpected_state_file=True)

    with pytest.raises(RuntimePrepareError, match="state directory.*empty"):
        _prepare(paths, apply=True, approval_file=approval, preparer=preparer)

    assert len(preparer.calls) == 1


def test_generated_symlink_fails_closed_and_consumes_one_shot_approval(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    approval = tmp_path / "approval-control/start-approval.json"
    _approval(approval)
    preparer = FakeFirmwarePreparer(paths, symlink_vassal=True)

    with pytest.raises(RuntimePrepareError, match="generated file must be regular"):
        _prepare(paths, apply=True, approval_file=approval, preparer=preparer)

    assert (approval.parent / "start-approval-consumed.json").exists()
    with pytest.raises(RuntimePrepareError, match="must be empty before preparation"):
        _prepare(paths, apply=True, approval_file=approval, preparer=preparer)
    assert len(preparer.calls) == 1


def test_missing_selected_flagfile_fails_closed_after_consuming_approval(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    approval = tmp_path / "approval-control/start-approval.json"
    _approval(approval)
    preparer = FakeFirmwarePreparer(paths, omit_bootstrap_flagfile=True)

    with pytest.raises(RuntimePrepareError, match="unexpected file inventory"):
        _prepare(paths, apply=True, approval_file=approval, preparer=preparer)

    assert (approval.parent / "start-approval-consumed.json").exists()


def test_root_vassal_override_fails_closed_after_consuming_approval(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    approval = tmp_path / "approval-control/start-approval.json"
    _approval(approval)
    preparer = FakeFirmwarePreparer(paths, root_vassal_override=True)

    with pytest.raises(RuntimePrepareError, match="INI contract drift"):
        _prepare(paths, apply=True, approval_file=approval, preparer=preparer)

    assert (approval.parent / "start-approval-consumed.json").exists()


def test_runtime_preparer_source_has_no_direct_emperor_or_socket_start_primitive() -> None:
    source_path = Path(__file__).parents[1] / "tools/brilliant_vc/runtime_prepare.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    imported = {
        alias.name.split(".", 1)[0]
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }

    assert "subprocess" not in imported
    assert "socket" not in imported
    assert "asyncio" not in imported


def test_runtime_account_requires_exact_effective_primary_group_and_nologin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    account = SimpleNamespace(
        pw_name=RUNTIME_USER,
        pw_uid=1001,
        pw_gid=1001,
        pw_dir="/nonexistent",
        pw_shell="/usr/sbin/nologin",
    )
    monkeypatch.setattr(os, "geteuid", lambda: 1001)
    monkeypatch.setattr(os, "getegid", lambda: 1002)
    monkeypatch.setattr(os, "getgroups", lambda: [1001])
    monkeypatch.setattr("tools.brilliant_vc.runtime_prepare.pwd.getpwuid", lambda uid: account)
    monkeypatch.setattr(
        "tools.brilliant_vc.runtime_prepare.grp.getgrgid",
        lambda gid: SimpleNamespace(gr_name=RUNTIME_USER, gr_mem=[]),
    )
    monkeypatch.setattr("tools.brilliant_vc.runtime_prepare.pwd.getpwall", lambda: [account])

    with pytest.raises(RuntimePrepareError, match="effective primary group"):
        _runtime_account()


def test_runtime_account_rejects_duplicate_uid_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    account = SimpleNamespace(
        pw_name=RUNTIME_USER,
        pw_uid=1001,
        pw_gid=1001,
        pw_dir="/nonexistent",
        pw_shell="/usr/sbin/nologin",
    )
    alias = SimpleNamespace(pw_name="alias", pw_uid=1001, pw_gid=1002)
    group = SimpleNamespace(gr_name=RUNTIME_USER, gr_gid=1001, gr_mem=[])
    monkeypatch.setattr(os, "geteuid", lambda: 1001)
    monkeypatch.setattr(os, "getegid", lambda: 1001)
    monkeypatch.setattr(os, "getgroups", lambda: [1001])
    monkeypatch.setattr("tools.brilliant_vc.runtime_prepare.pwd.getpwuid", lambda uid: account)
    monkeypatch.setattr("tools.brilliant_vc.runtime_prepare.pwd.getpwall", lambda: [account, alias])
    monkeypatch.setattr("tools.brilliant_vc.runtime_prepare.grp.getgrgid", lambda gid: group)
    monkeypatch.setattr("tools.brilliant_vc.runtime_prepare.grp.getgrall", lambda: [group])

    with pytest.raises(RuntimePrepareError, match="UID must map"):
        _runtime_account()


def test_runtime_account_rejects_duplicate_same_name_uid_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    account = SimpleNamespace(
        pw_name=RUNTIME_USER,
        pw_uid=1001,
        pw_gid=1001,
        pw_dir="/nonexistent",
        pw_shell="/usr/sbin/nologin",
    )
    duplicate = SimpleNamespace(
        pw_name=RUNTIME_USER,
        pw_uid=1001,
        pw_gid=1001,
        pw_dir="/tmp/duplicate-home",
        pw_shell="/bin/sh",
    )
    group = SimpleNamespace(gr_name=RUNTIME_USER, gr_gid=1001, gr_mem=[])
    monkeypatch.setattr(os, "geteuid", lambda: 1001)
    monkeypatch.setattr(os, "getegid", lambda: 1001)
    monkeypatch.setattr(os, "getgroups", lambda: [1001])
    monkeypatch.setattr("tools.brilliant_vc.runtime_prepare.pwd.getpwuid", lambda uid: account)
    monkeypatch.setattr(
        "tools.brilliant_vc.runtime_prepare.pwd.getpwall",
        lambda: [account, duplicate],
    )
    monkeypatch.setattr("tools.brilliant_vc.runtime_prepare.grp.getgrgid", lambda gid: group)
    monkeypatch.setattr("tools.brilliant_vc.runtime_prepare.grp.getgrall", lambda: [group])

    with pytest.raises(RuntimePrepareError, match="UID must map"):
        _runtime_account()


def test_runtime_account_rejects_duplicate_same_name_primary_gid_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    account = SimpleNamespace(
        pw_name=RUNTIME_USER,
        pw_uid=1001,
        pw_gid=1001,
        pw_dir="/nonexistent",
        pw_shell="/usr/sbin/nologin",
    )
    duplicate = SimpleNamespace(
        pw_name=RUNTIME_USER,
        pw_uid=1002,
        pw_gid=1001,
        pw_dir="/tmp/duplicate-home",
        pw_shell="/bin/sh",
    )
    group = SimpleNamespace(gr_name=RUNTIME_USER, gr_gid=1001, gr_mem=[])
    monkeypatch.setattr(os, "geteuid", lambda: 1001)
    monkeypatch.setattr(os, "getegid", lambda: 1001)
    monkeypatch.setattr(os, "getgroups", lambda: [1001])
    monkeypatch.setattr("tools.brilliant_vc.runtime_prepare.pwd.getpwuid", lambda uid: account)
    monkeypatch.setattr(
        "tools.brilliant_vc.runtime_prepare.pwd.getpwall",
        lambda: [account, duplicate],
    )
    monkeypatch.setattr("tools.brilliant_vc.runtime_prepare.grp.getgrgid", lambda gid: group)
    monkeypatch.setattr("tools.brilliant_vc.runtime_prepare.grp.getgrall", lambda: [group])

    with pytest.raises(RuntimePrepareError, match="runtime group must not include"):
        _runtime_account()


def test_reference_service_is_nonroot_bounded_nonrestartable_and_not_enableable() -> None:
    unit_path = Path(__file__).parents[1] / "deploy/brilliant-vc-pilot.service"
    unit = unit_path.read_text(encoding="utf-8")

    assert "User=brilliant-vc" in unit
    assert "Group=brilliant-vc" in unit
    assert "ConditionPathExists=/run/brilliant-vc-approval/start-approval.json" in unit
    assert "ExecStartPre=" in unit and "runtime_prepare --apply" in unit
    assert "ExecStartPre=!/usr/bin/mv.coreutils --no-clobber --no-target-directory" in unit
    assert "tools.brilliant_vc.start_approval" not in unit
    assert "ExecStartPre=/usr/bin/python3.10 -m tools.brilliant_vc.runtime_prepare" in unit
    assert "ExecStart=/data/switch-embedded/env/bin/uwsgi" in unit
    assert "--home /data/switch-embedded/env" in unit
    assert (
        "--vassals-include /data/switch-embedded/lib/process_management/process-default.ini" in unit
    )
    assert "--emperor /data/brilliant-vc/process-config" in unit
    assert "--emperor-stats /run/brilliant-vc/uwsgi_stats_socket" in unit
    assert "--chmod-socket=600" in unit
    assert "--die-on-term" in unit
    assert "Restart=no" in unit
    assert "RuntimeMaxSec=600" in unit
    assert "MemoryMax=100M" in unit
    assert "CPUQuota=15%" in unit
    assert "PrivateDevices=yes" in unit
    assert "DevicePolicy=closed" in unit
    assert "ProtectSystem=strict" in unit
    assert "NoNewPrivileges=yes" in unit
    assert "CapabilityBoundingSet=" in unit
    assert "ReadWritePaths=/data/brilliant-vc /run/brilliant-vc" in unit
    assert "/run/brilliant-vc-approval" in unit
    assert "/run/brilliant-vc-control" not in unit
    assert "ReadOnlyPaths=/var/brilliant-vc/app" in unit
    assert "InaccessiblePaths=" in unit
    assert "/var/run/brilliant" in unit
    assert "/run/dbus" in unit
    assert "/run/udev" in unit
    assert "/bin/sh" not in unit
    assert "sh -c" not in unit
    assert "fork_server" not in unit
    assert "fork-server" not in unit
    assert "vassal-fork-base" not in unit
    assert "emperor.ini" not in unit
    assert "zygote.ini" not in unit
    assert "User=root" not in unit
    assert "Restart=always" not in unit
    assert "WantedBy=" not in unit
    assert "[Install]" not in unit


def test_pilot_app_manifest_pins_the_exact_staged_runtime_subset() -> None:
    repository = Path(__file__).parents[1]
    manifest = repository / "deploy/brilliant-vc-pilot-app-manifest.sha256"
    prefix = "/var/brilliant-vc/app/"
    expected = {
        "tools/__init__.py",
        "tools/brilliant_vc/__init__.py",
        "tools/brilliant_vc/launcher_preflight.py",
        "tools/brilliant_vc/runtime_handoff.py",
        "tools/brilliant_vc/runtime_prepare.py",
        "tools/brilliant_vc/start_approval.py",
        "tools/brilliant_vc/vassal_manifest.py",
    }
    entries: dict[str, str] = {}
    for line in manifest.read_text(encoding="ascii").splitlines():
        digest, separator, target = line.partition("  ")
        assert separator == "  " and len(digest) == 64
        assert all(character in "0123456789abcdef" for character in digest)
        assert target.startswith(prefix)
        relative = target.removeprefix(prefix)
        assert relative not in entries
        entries[relative] = digest

    assert set(entries) == expected
    for relative, digest in entries.items():
        assert hashlib.sha256((repository / relative).read_bytes()).hexdigest() == digest
