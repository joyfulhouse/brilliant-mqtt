from __future__ import annotations

import hashlib
import json
import os
from dataclasses import replace
from pathlib import Path

import pytest

from tools.brilliant_vc.launcher_preflight import (
    LauncherPaths,
    LauncherPreflightError,
    hash_firmware_modules,
    preflight_no_start,
)

DEVICE_ID = "a" * 32


def _firmware() -> dict[str, object]:
    return {
        "schema_version": 3,
        "firmware_version": "v26.06.03.1",
        "runtime_sha256": {
            "bridge.remote_bridge": (
                "94ac32df6184814950cc5bc3ebeac828518b858f8fd6ce76380b67f20ccf20e4"
            ),
            "bus.message_bus": "a85b7a2d0c2533db8d803a217027dbdd245bc104f221bf6955907dc0b8f6feb8",
            "bus.peripheral_process_manager": (
                "da8d7678c2a7d798aac2e3d735ac6e0789359bfb47226e5b8702f3923fcc7135"
            ),
            "configs.process_configs": (
                "a8ea4ad3885ac0d826da0073b2696a026f8d38a071418fe13196eb554b9e94a9"
            ),
            "lib.process_management.process_manager": (
                "38d26c0d300fab7af421731438c0bb9d97b7202760b089609d178a9e1db1b860"
            ),
            "lib.runner": "4ba40ac7d7695dc239590defbc6efd3d22efbf296fc1c2b40f139fb6e1fe3cb0",
            "peripherals.bootstrap.bootstrap_peripheral": (
                "313d526a3fe1ad1879137a83eaa55096d9b0fb7a08cac30e37a79ea3632d57db"
            ),
            "peripherals.discovery.discovery_peripheral": (
                "d6bc30e81430978f4b72779dd2c6927a7ceab094a951ce7bb907a20969b94e45"
            ),
            "peripherals.lib.peripheral_service.peripheral_host": (
                "354eed5d1135ab6175cc50f263bb380fc996b73915cd1297f64246554b4ff228"
            ),
            "peripherals.configs.art_config_peripheral": (
                "2a86f73fcada6b1ee488ffe43088894e1af21cba9eab9d947560adf98c628d91"
            ),
            "peripherals.configs.device_config_peripheral": (
                "6bb1a1c46b22d315450634e80b05811a4a766ce63d8b064beb3f47bbb7fe3861"
            ),
            "peripherals.configs.motion_detection_config_peripheral": (
                "b68966769342376f9f883433fcedde21444a423985cc9343fe8ecb6045a4967f"
            ),
            "peripherals.configs.alarm_config_peripheral": (
                "b6e59357305764fac73d83768315c187408adbb964311f556bd2a05850181c69"
            ),
            "runtime.run_py": ("70d03e29277862a93da7840ca2224b5b27293158d30e3054a8b17068dbb0d961"),
            "runtime.uwsgi": ("3384606e779e7a4216f4ff27e39e10221cd0b377c02ad1d9fd8ea61269ecbc43"),
        },
        "message_bus_parameters": [
            "home_id",
            "device_id",
            "mb_state_dir",
            "is_virtual_control",
        ],
        "runner_parameters": ["startable_config", "module_name_override"],
        "bootstrap_fields": [
            "target_home_id",
            "server_authentication_token",
            "wifi_variables",
        ],
        "virtual_control_flag": "start_as_virtual_control",
        "runtime_launcher": "uwsgi_emperor_vassal",
        "message_bus_requires_emperor": True,
        "certificate_files": ["device.key", "device.cert"],
        "remote_bridge_parameters": [
            "listen_port",
            "enable_bluetooth_provisioning",
            "enforce_strict_authentication",
            "message_bus_address_override",
            "device_provisioning_ip_listen_port",
            "ble_mesh_debug_interface_listen_port",
            "stub_ble_peripheral",
            "uwsgi_stats_socket_path",
        ],
        "discovery_fields": [
            "remote_bridge_port",
            "enable_remote_bridge_service_discovery",
            "message_bus_address_override",
        ],
        "known_process_count": 38,
        "candidate_processes": [
            "message_bus",
            "discovery_peripheral",
            "config_peripherals",
            "bootstrap",
        ],
        "candidate_disabled_process_count": 34,
        "process_manager_launch_mode": "message_bus_then_enabled_defaults",
        "message_bus_embedded_startables": ["remote_bridge"],
        "config_peripherals_embedded_startables": [
            "art_config_peripheral",
            "device_config_peripheral",
            "motion_detection_config_peripheral",
            "alarm_config_peripheral",
        ],
        "local_message_bus_address_mode": "server_socket_path_with_derived_unix_url",
        "local_message_bus_address_override": None,
        "vassal_user_fields": ["user_override", "group_override"],
        "runtime_path_flags": [
            "mb_state_dir",
            "cert_dir",
            "process_configs_dir",
            "process_flagfiles_dir",
            "startable_host_configs_dir",
            "message_bus_server_socket_path",
            "saved_bootstrap_parameters_path",
            "release_info_filepath",
            "tracking_branch_filepath",
            "uwsgi_stats_socket_path",
            "log_output_directory",
            "error_log_storage_dir",
            "trace_dir",
            "art_preload_dir",
        ],
    }


def _module_hashes() -> dict[str, str]:
    hashes = _firmware()["runtime_sha256"]
    assert isinstance(hashes, dict)
    return dict(hashes)


def _private_file(path: Path, data: bytes) -> None:
    path.write_bytes(data)
    path.chmod(0o600)


def _paths(tmp_path: Path) -> LauncherPaths:
    persistent = tmp_path / "data" / "brilliant-vc"
    persistent.mkdir(parents=True, mode=0o700)
    runtime = tmp_path / "run" / "brilliant-vc"
    runtime.mkdir(parents=True, mode=0o700)
    identity = persistent / "identity"
    state = persistent / "state"
    certificates = persistent / "certificates"
    process_config = persistent / "process-config"
    process_flagfiles = persistent / "flagfiles"
    startable_configs = persistent / "startable-configs"
    logs = persistent / "logs"
    errors = persistent / "errors"
    traces = persistent / "traces"
    for directory in (
        identity,
        state,
        certificates,
        process_config,
        process_flagfiles,
        startable_configs,
        logs,
        errors,
        traces,
    ):
        directory.mkdir(mode=0o700)

    read_only_metadata = tmp_path / "read-only-metadata"
    read_only_metadata.mkdir(mode=0o700)
    release_info = read_only_metadata / "release_info.json"
    tracking_branch = read_only_metadata / "tracking_branch"
    _private_file(release_info, b"{}\n")
    _private_file(tracking_branch, b"stable\n")

    _private_file(identity / "device_id", (DEVICE_ID + "\n").encode())
    _private_file(identity / "pkcs12_certificate", b"opaque-pkcs12")
    _private_file(identity / "bootstrap", b"opaque-bootstrap")
    _private_file(
        identity / "metadata.json",
        json.dumps(
            {
                "device_id_redacted": "aaaa…aaaa",
                "target_home_match": True,
            }
        ).encode(),
    )
    return LauncherPaths(
        persistent_root=persistent,
        identity_dir=identity,
        state_dir=state,
        certificate_dir=certificates,
        process_config_dir=process_config,
        process_flagfile_dir=process_flagfiles,
        startable_config_dir=startable_configs,
        log_dir=logs,
        error_log_dir=errors,
        trace_dir=traces,
        runtime_dir=runtime,
        socket_path=runtime / "server_socket",
        stats_socket_path=runtime / "uwsgi_stats_socket",
        release_info_path=release_info,
        tracking_branch_path=tracking_branch,
    )


def test_tracked_schema_v3_snapshot_example_matches_the_pinned_contract() -> None:
    example = (
        Path(__file__).parents[1]
        / "docs/brilliant-panel/virtual-control-launcher-snapshot-v3.example.json"
    )

    assert json.loads(example.read_text(encoding="utf-8")) == _firmware()


def test_valid_prerequisites_produce_a_redacted_plan_that_cannot_start(tmp_path: Path) -> None:
    paths = _paths(tmp_path)

    plan = preflight_no_start(
        paths,
        _firmware(),
        actual_module_hashes=_module_hashes(),
        required_uid=os.getuid(),
        allowed_persistent_roots=(paths.persistent_root,),
        allowed_runtime_roots=(paths.runtime_dir,),
    )

    assert plan.to_public_dict() == {
        "firmware_matches": True,
        "interfaces_match": True,
        "identity_inputs_valid": True,
        "paths_isolated": True,
        "private_modes_valid": True,
        "empty_runtime_paths": True,
        "certificate_material_present": False,
        "identity_file_count": 4,
        "device_id_redacted": "aaaa…aaaa",
        "uwsgi_contract_confirmed": True,
        "stock_process_manager_lifecycle_confirmed": True,
        "direct_runner_rejected": True,
        "identity_contract_complete": False,
        "full_path_surface_validated": True,
        "candidate_manifest_present": True,
        "runtime_user_handoff_complete": False,
        "launcher_implementation_present": False,
        "start_permitted": False,
        "blocked_reason": "identity_materialization_required",
    }
    assert DEVICE_ID not in json.dumps(plan.to_public_dict())
    assert not hasattr(plan, "command")


def test_hash_or_interface_drift_blocks_before_a_plan(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    firmware = _firmware()
    snapshot_hashes = firmware["runtime_sha256"]
    assert isinstance(snapshot_hashes, dict)
    snapshot_hashes["lib.runner"] = "0" * 64
    with pytest.raises(LauncherPreflightError, match="hash"):
        preflight_no_start(
            paths,
            firmware,
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
        )

    actual_hashes = _module_hashes()
    actual_hashes["lib.runner"] = "0" * 64
    with pytest.raises(LauncherPreflightError, match="actual module hash"):
        preflight_no_start(
            paths,
            _firmware(),
            actual_module_hashes=actual_hashes,
            required_uid=os.getuid(),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
        )

    firmware = _firmware()
    parameters = firmware["message_bus_parameters"]
    assert isinstance(parameters, list)
    parameters.remove("mb_state_dir")
    with pytest.raises(LauncherPreflightError, match="interface"):
        preflight_no_start(
            paths,
            firmware,
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
        )

    firmware = _firmware()
    firmware["message_bus_requires_emperor"] = False
    with pytest.raises(LauncherPreflightError, match="runtime contract"):
        preflight_no_start(
            paths,
            firmware,
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
        )


def test_materialized_certificate_pair_is_accepted_but_start_remains_blocked(
    tmp_path: Path,
) -> None:
    paths = _paths(tmp_path)
    _private_file(paths.certificate_dir / "device.key", b"private-key-pem")
    _private_file(paths.certificate_dir / "device.cert", b"certificate-pem")

    plan = preflight_no_start(
        paths,
        _firmware(),
        actual_module_hashes=_module_hashes(),
        required_uid=os.getuid(),
        allowed_persistent_roots=(paths.persistent_root,),
        allowed_runtime_roots=(paths.runtime_dir,),
    )

    assert plan.certificate_material_present is True
    assert plan.start_permitted is False
    assert plan.blocked_reason == "runtime_user_credential_handoff_unresolved"


def test_rejects_physical_socket_shared_or_colliding_paths(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    with pytest.raises(LauncherPreflightError, match="physical Control"):
        preflight_no_start(
            replace(paths, socket_path=Path("/var/run/brilliant/server_socket")),
            _firmware(),
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
        )

    with pytest.raises(LauncherPreflightError, match="distinct"):
        preflight_no_start(
            replace(paths, state_dir=paths.certificate_dir),
            _firmware(),
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
        )

    with pytest.raises(LauncherPreflightError, match="distinct"):
        preflight_no_start(
            replace(paths, stats_socket_path=paths.socket_path),
            _firmware(),
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
        )


def test_rejects_symlink_broad_mode_nonempty_dirs_and_existing_socket(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    paths.state_dir.rmdir()
    paths.state_dir.symlink_to(paths.certificate_dir, target_is_directory=True)
    with pytest.raises(LauncherPreflightError, match="symlink"):
        preflight_no_start(
            paths,
            _firmware(),
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
        )

    paths = _paths(tmp_path / "mode")
    paths.process_config_dir.chmod(0o755)
    with pytest.raises(LauncherPreflightError, match="0700"):
        preflight_no_start(
            paths,
            _firmware(),
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
        )

    paths = _paths(tmp_path / "nonempty")
    (paths.state_dir / "old-state").write_text("stale", encoding="utf-8")
    with pytest.raises(LauncherPreflightError, match="empty"):
        preflight_no_start(
            paths,
            _firmware(),
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
        )

    paths = _paths(tmp_path / "socket")
    paths.socket_path.touch(mode=0o600)
    with pytest.raises(LauncherPreflightError, match="socket"):
        preflight_no_start(
            paths,
            _firmware(),
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
        )

    paths = _paths(tmp_path / "stats-socket")
    paths.stats_socket_path.touch(mode=0o600)
    with pytest.raises(LauncherPreflightError, match="stats socket"):
        preflight_no_start(
            paths,
            _firmware(),
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
        )

    paths = _paths(tmp_path / "flagfiles")
    (paths.process_flagfile_dir / "stale").write_text("stale", encoding="utf-8")
    with pytest.raises(LauncherPreflightError, match="flagfile.*empty"):
        preflight_no_start(
            paths,
            _firmware(),
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
        )


def test_read_only_runtime_metadata_must_be_regular_bounded_and_private(
    tmp_path: Path,
) -> None:
    paths = _paths(tmp_path)
    paths.release_info_path.chmod(0o666)
    with pytest.raises(LauncherPreflightError, match="release metadata"):
        preflight_no_start(
            paths,
            _firmware(),
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
        )

    paths = _paths(tmp_path / "symlink")
    paths.tracking_branch_path.unlink()
    paths.tracking_branch_path.symlink_to(paths.release_info_path)
    with pytest.raises(LauncherPreflightError, match="tracking metadata.*symlink"):
        preflight_no_start(
            paths,
            _firmware(),
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
        )


def test_identity_contract_is_exact_private_and_self_consistent(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    (paths.identity_dir / "unexpected").touch(mode=0o600)
    with pytest.raises(LauncherPreflightError, match="exactly"):
        preflight_no_start(
            paths,
            _firmware(),
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
        )

    paths = _paths(tmp_path / "metadata")
    _private_file(
        paths.identity_dir / "metadata.json",
        json.dumps({"device_id_redacted": "bbbb…bbbb", "target_home_match": True}).encode(),
    )
    with pytest.raises(LauncherPreflightError, match="metadata"):
        preflight_no_start(
            paths,
            _firmware(),
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
        )

    paths = _paths(tmp_path / "hardlink")
    (paths.identity_dir / "bootstrap").unlink()
    os.link(
        paths.identity_dir / "pkcs12_certificate",
        paths.identity_dir / "bootstrap",
    )
    with pytest.raises(LauncherPreflightError, match="hard link"):
        preflight_no_start(
            paths,
            _firmware(),
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
        )


def test_hashes_actual_regular_module_files_without_following_symlinks(tmp_path: Path) -> None:
    module_paths: dict[str, Path] = {}
    expected: dict[str, str] = {}
    for index, name in enumerate(_module_hashes()):
        path = tmp_path / f"module-{index}.so"
        content = f"module-{index}".encode()
        path.write_bytes(content)
        module_paths[name] = path
        expected[name] = hashlib.sha256(content).hexdigest()

    assert (
        hash_firmware_modules(
            module_paths=module_paths,
            required_uid=os.getuid(),
        )
        == expected
    )

    target = module_paths["lib.runner"]
    target.unlink()
    target.symlink_to(module_paths["bus.message_bus"])
    with pytest.raises(LauncherPreflightError, match="symlink"):
        hash_firmware_modules(module_paths=module_paths, required_uid=os.getuid())
