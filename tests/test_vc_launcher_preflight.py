from __future__ import annotations

import grp
import hashlib
import json
import os
import pwd
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools.brilliant_vc.launcher_preflight import (
    LauncherPaths,
    LauncherPreflightError,
    _validate_runtime_account_contract,
    hash_firmware_modules,
    main,
    preflight_no_start,
)

DEVICE_ID = "a" * 32


def _firmware() -> dict[str, object]:
    return {
        "schema_version": 5,
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
            "configs.socket_parameters": (
                "18898d871ab1eacf52fae3ebc4dc0bc4cfc937da2848de0445c6879abe422967"
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
            "runtime.process_default_ini": (
                "28ed9ff992040ccf14f9004e3ca139abe16c471547900046fb57d2455996099a"
            ),
            "runtime.run_startable_py": (
                "828a3d1d088e8db597e88c4f9be31f2cb6013efab6ee1d6669937d0c09c1f60c"
            ),
            "runtime.uwsgi": ("3384606e779e7a4216f4ff27e39e10221cd0b377c02ad1d9fd8ea61269ecbc43"),
            "runtime.approval_move": (
                "dd06bbb44fb05d8f82edf91be54b0785f33c43e48b756a552ef80e17760f93bb"
            ),
            "runtime.python": ("2d78bffcfbe8c92d169c4aa615364661bddc9afae5a3cf4ab336d66a2b95e179"),
        },
        "runtime_file_modes": {
            "bridge.remote_bridge": "0755",
            "bus.message_bus": "0755",
            "bus.peripheral_process_manager": "0755",
            "configs.process_configs": "0644",
            "configs.socket_parameters": "0644",
            "lib.process_management.process_manager": "0755",
            "lib.runner": "0755",
            "peripherals.bootstrap.bootstrap_peripheral": "0755",
            "peripherals.discovery.discovery_peripheral": "0755",
            "peripherals.lib.peripheral_service.peripheral_host": "0755",
            "peripherals.configs.art_config_peripheral": "0755",
            "peripherals.configs.device_config_peripheral": "0755",
            "peripherals.configs.motion_detection_config_peripheral": "0755",
            "peripherals.configs.alarm_config_peripheral": "0755",
            "runtime.run_py": "0644",
            "runtime.process_default_ini": "0644",
            "runtime.run_startable_py": "0644",
            "runtime.uwsgi": "0755",
            "runtime.approval_move": "0755",
            "runtime.python": "0755",
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
        "candidate_supervisor_mode": "dedicated_nonroot_emperor",
        "candidate_root_emperor_permitted": False,
        "candidate_generated_directory_mode": "0700",
        "runtime_credential_layout": [
            "device_id",
            "bootstrap",
            "certificates/device.key",
            "certificates/device.cert",
        ],
        "runtime_credential_access": "root_owner_dedicated_group_read_only",
    }


def _module_hashes() -> dict[str, str]:
    hashes = _firmware()["runtime_sha256"]
    assert isinstance(hashes, dict)
    return dict(hashes)


def _private_file(path: Path, data: bytes) -> None:
    path.write_bytes(data)
    path.chmod(0o600)


def _paths(tmp_path: Path) -> LauncherPaths:
    private_root = tmp_path / "data" / "brilliant-vc-private"
    private_root.mkdir(parents=True, mode=0o700)
    identity = private_root / "identity"
    materialized_certificates = private_root / "materialized-certificates"
    identity.mkdir(mode=0o700)
    materialized_certificates.mkdir(mode=0o700)

    persistent = tmp_path / "data" / "brilliant-vc"
    persistent.mkdir(parents=True, mode=0o700)
    runtime = tmp_path / "run" / "brilliant-vc"
    runtime.mkdir(parents=True, mode=0o700)
    state = persistent / "state"
    process_config = persistent / "process-config"
    process_flagfiles = persistent / "flagfiles"
    startable_configs = persistent / "startable-configs"
    logs = persistent / "logs"
    errors = persistent / "errors"
    traces = persistent / "traces"
    for directory in (
        state,
        process_config,
        process_flagfiles,
        startable_configs,
        logs,
        errors,
        traces,
    ):
        directory.mkdir(mode=0o700)
    for directory in (
        persistent,
        runtime,
        state,
        process_config,
        process_flagfiles,
        startable_configs,
        logs,
        errors,
        traces,
    ):
        os.chown(directory, os.getuid(), os.getgid())

    runtime_credentials = tmp_path / "data" / "brilliant-vc-credentials"

    read_only_metadata = tmp_path / "read-only-metadata"
    read_only_metadata.mkdir(mode=0o700)
    release_info = read_only_metadata / "release_info.json"
    tracking_branch = read_only_metadata / "tracking_branch"
    art_preload = read_only_metadata / "art_library"
    art_preload.mkdir(mode=0o755)
    _private_file(release_info, b"{}\n")
    _private_file(tracking_branch, b"stable\n")
    release_info.chmod(0o644)
    tracking_branch.chmod(0o644)

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
        private_root=private_root,
        persistent_root=persistent,
        identity_dir=identity,
        materialized_certificate_dir=materialized_certificates,
        runtime_credential_dir=runtime_credentials,
        bootstrap_path=runtime_credentials / "bootstrap",
        state_dir=state,
        certificate_dir=runtime_credentials / "certificates",
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
        art_preload_dir=art_preload,
    )


def test_tracked_schema_v5_snapshot_example_matches_the_pinned_contract() -> None:
    example = (
        Path(__file__).parents[1]
        / "docs/brilliant-panel/virtual-control-launcher-snapshot-v5.example.json"
    )

    assert json.loads(example.read_text(encoding="utf-8")) == _firmware()


def test_runtime_account_contract_requires_locked_nologin_identity(tmp_path: Path) -> None:
    shadow = tmp_path / "shadow"
    shadow.write_text("brilliant-vc:!:20000:0:99999:7:::\n", encoding="utf-8")
    shadow.chmod(0o600)
    account = SimpleNamespace(
        pw_name="brilliant-vc",
        pw_uid=1001,
        pw_gid=1001,
        pw_dir="/nonexistent",
        pw_shell="/usr/sbin/nologin",
    )
    group = SimpleNamespace(gr_name="brilliant-vc", gr_gid=1001, gr_mem=[])

    _validate_runtime_account_contract(
        account,
        group,
        all_accounts=[account],
        all_groups=[group],
        shadow_path=shadow,
        required_uid=os.getuid(),
    )

    shadow.write_text("brilliant-vc:$6$live-hash:20000:0:99999:7:::\n", encoding="utf-8")
    with pytest.raises(LauncherPreflightError, match="password must be locked"):
        _validate_runtime_account_contract(
            account,
            group,
            all_accounts=[account],
            all_groups=[group],
            shadow_path=shadow,
            required_uid=os.getuid(),
        )


def test_runtime_account_contract_rejects_login_shell(tmp_path: Path) -> None:
    shadow = tmp_path / "shadow"
    shadow.write_text("brilliant-vc:*:20000:0:99999:7:::\n", encoding="utf-8")
    shadow.chmod(0o600)
    account = SimpleNamespace(
        pw_name="brilliant-vc",
        pw_uid=1001,
        pw_gid=1001,
        pw_dir="/nonexistent",
        pw_shell="/bin/sh",
    )

    with pytest.raises(LauncherPreflightError, match="nologin"):
        _validate_runtime_account_contract(
            account,
            SimpleNamespace(gr_name="brilliant-vc", gr_gid=1001, gr_mem=[]),
            all_accounts=[account],
            all_groups=[],
            shadow_path=shadow,
            required_uid=os.getuid(),
        )


def test_runtime_account_contract_rejects_foreign_supplementary_group(
    tmp_path: Path,
) -> None:
    shadow = tmp_path / "shadow"
    shadow.write_text("brilliant-vc:!:20000:0:99999:7:::\n", encoding="utf-8")
    shadow.chmod(0o600)
    account = SimpleNamespace(
        pw_name="brilliant-vc",
        pw_uid=1001,
        pw_gid=1001,
        pw_dir="/nonexistent",
        pw_shell="/usr/sbin/nologin",
    )
    primary = SimpleNamespace(gr_name="brilliant-vc", gr_gid=1001, gr_mem=[])
    foreign = SimpleNamespace(gr_name="dialout", gr_gid=20, gr_mem=["brilliant-vc"])

    with pytest.raises(LauncherPreflightError, match="supplementary groups"):
        _validate_runtime_account_contract(
            account,
            primary,
            all_accounts=[account],
            all_groups=[primary, foreign],
            shadow_path=shadow,
            required_uid=os.getuid(),
        )


def test_runtime_account_contract_rejects_uid_or_gid_aliases(tmp_path: Path) -> None:
    shadow = tmp_path / "shadow"
    shadow.write_text("brilliant-vc:!:20000:0:99999:7:::\n", encoding="utf-8")
    shadow.chmod(0o600)
    account = SimpleNamespace(
        pw_name="brilliant-vc",
        pw_uid=1001,
        pw_gid=1001,
        pw_dir="/nonexistent",
        pw_shell="/usr/sbin/nologin",
    )
    primary = SimpleNamespace(gr_name="brilliant-vc", gr_gid=1001, gr_mem=[])
    uid_alias = SimpleNamespace(pw_name="alias", pw_uid=1001, pw_gid=1002)

    with pytest.raises(LauncherPreflightError, match="UID must map"):
        _validate_runtime_account_contract(
            account,
            primary,
            all_accounts=[account, uid_alias],
            all_groups=[primary],
            shadow_path=shadow,
            required_uid=os.getuid(),
        )

    gid_alias = SimpleNamespace(gr_name="alias", gr_gid=1001, gr_mem=[])
    with pytest.raises(LauncherPreflightError, match="GID must map"):
        _validate_runtime_account_contract(
            account,
            primary,
            all_accounts=[account],
            all_groups=[primary, gid_alias],
            shadow_path=shadow,
            required_uid=os.getuid(),
        )


def test_runtime_account_contract_rejects_duplicate_same_name_uid_record(
    tmp_path: Path,
) -> None:
    shadow = tmp_path / "shadow"
    shadow.write_text("brilliant-vc:!:20000:0:99999:7:::\n", encoding="utf-8")
    shadow.chmod(0o600)
    account = SimpleNamespace(
        pw_name="brilliant-vc",
        pw_uid=1001,
        pw_gid=1001,
        pw_dir="/nonexistent",
        pw_shell="/usr/sbin/nologin",
    )
    duplicate = SimpleNamespace(
        pw_name="brilliant-vc",
        pw_uid=1001,
        pw_gid=1001,
        pw_dir="/tmp/duplicate-home",
        pw_shell="/bin/sh",
    )
    primary = SimpleNamespace(gr_name="brilliant-vc", gr_gid=1001, gr_mem=[])

    with pytest.raises(LauncherPreflightError, match="UID must map"):
        _validate_runtime_account_contract(
            account,
            primary,
            all_accounts=[account, duplicate],
            all_groups=[primary],
            shadow_path=shadow,
            required_uid=os.getuid(),
        )


def test_runtime_account_contract_rejects_duplicate_same_name_primary_gid_record(
    tmp_path: Path,
) -> None:
    shadow = tmp_path / "shadow"
    shadow.write_text("brilliant-vc:!:20000:0:99999:7:::\n", encoding="utf-8")
    shadow.chmod(0o600)
    account = SimpleNamespace(
        pw_name="brilliant-vc",
        pw_uid=1001,
        pw_gid=1001,
        pw_dir="/nonexistent",
        pw_shell="/usr/sbin/nologin",
    )
    duplicate = SimpleNamespace(
        pw_name="brilliant-vc",
        pw_uid=1002,
        pw_gid=1001,
        pw_dir="/tmp/duplicate-home",
        pw_shell="/bin/sh",
    )
    primary = SimpleNamespace(gr_name="brilliant-vc", gr_gid=1001, gr_mem=[])

    with pytest.raises(LauncherPreflightError, match="runtime group must not include"):
        _validate_runtime_account_contract(
            account,
            primary,
            all_accounts=[account, duplicate],
            all_groups=[primary],
            shadow_path=shadow,
            required_uid=os.getuid(),
        )


@pytest.mark.parametrize("mode", [0o604, 0o644, 0o655, 0o700])
def test_runtime_account_contract_rejects_exposed_or_executable_shadow_mode(
    tmp_path: Path,
    mode: int,
) -> None:
    shadow = tmp_path / "shadow"
    shadow.write_text("brilliant-vc:!:20000:0:99999:7:::\n", encoding="utf-8")
    shadow.chmod(mode)
    account = SimpleNamespace(
        pw_name="brilliant-vc",
        pw_uid=1001,
        pw_gid=1001,
        pw_dir="/nonexistent",
        pw_shell="/usr/sbin/nologin",
    )
    primary = SimpleNamespace(gr_name="brilliant-vc", gr_gid=1001, gr_mem=[])

    with pytest.raises(LauncherPreflightError, match="password database ownership or mode"):
        _validate_runtime_account_contract(
            account,
            primary,
            all_accounts=[account],
            all_groups=[primary],
            shadow_path=shadow,
            required_uid=os.getuid(),
        )


def test_valid_prerequisites_produce_a_redacted_plan_that_cannot_start(tmp_path: Path) -> None:
    paths = _paths(tmp_path)

    plan = preflight_no_start(
        paths,
        _firmware(),
        actual_module_hashes=_module_hashes(),
        required_uid=os.getuid(),
        runtime_uid=os.getuid(),
        runtime_gid=os.getgid(),
        allowed_private_roots=(paths.private_root,),
        allowed_persistent_roots=(paths.persistent_root,),
        allowed_runtime_roots=(paths.runtime_dir,),
        allowed_runtime_credential_paths=(paths.runtime_credential_dir,),
    )

    assert plan.to_public_dict() == {
        "firmware_matches": True,
        "interfaces_match": True,
        "identity_inputs_valid": True,
        "paths_isolated": True,
        "private_modes_valid": True,
        "empty_runtime_paths": True,
        "certificate_material_present": False,
        "runtime_credentials_present": False,
        "identity_file_count": 4,
        "device_id_redacted": "aaaa…aaaa",
        "uwsgi_contract_confirmed": True,
        "stock_process_manager_lifecycle_confirmed": True,
        "nonroot_emperor_confirmed": True,
        "direct_runner_rejected": True,
        "identity_contract_complete": False,
        "full_path_surface_validated": True,
        "candidate_manifest_present": True,
        "runtime_user_handoff_complete": False,
        "launcher_implementation_present": True,
        "start_permitted": False,
        "blocked_reason": "identity_materialization_required",
    }
    assert DEVICE_ID not in json.dumps(plan.to_public_dict())
    assert not hasattr(plan, "command")


def test_runtime_supervisor_identity_must_be_nonroot(tmp_path: Path) -> None:
    paths = _paths(tmp_path)

    with pytest.raises(LauncherPreflightError, match="must be non-root"):
        preflight_no_start(
            paths,
            _firmware(),
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            runtime_uid=0,
            runtime_gid=os.getgid(),
            allowed_private_roots=(paths.private_root,),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
            allowed_runtime_credential_paths=(paths.runtime_credential_dir,),
        )


def test_cli_rejects_a_runtime_group_shared_with_another_account(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    account = SimpleNamespace(
        pw_name="brilliant-vc",
        pw_uid=1001,
        pw_gid=1001,
        pw_dir="/nonexistent",
        pw_shell="/usr/sbin/nologin",
    )
    group = SimpleNamespace(
        gr_name="brilliant-vc",
        gr_gid=1001,
        gr_mem=["another-user"],
    )
    monkeypatch.setattr(os, "geteuid", lambda: 0)
    monkeypatch.setattr(pwd, "getpwnam", lambda name: account)
    monkeypatch.setattr(grp, "getgrgid", lambda gid: group)
    monkeypatch.setattr(pwd, "getpwall", lambda: [account])
    monkeypatch.setattr(grp, "getgrall", lambda: [group])

    with pytest.raises(LauncherPreflightError, match="must not include another account"):
        main(["--firmware-snapshot", str(tmp_path / "unused.json")])


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
            runtime_uid=os.getuid(),
            runtime_gid=os.getgid(),
            allowed_private_roots=(paths.private_root,),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
            allowed_runtime_credential_paths=(paths.runtime_credential_dir,),
        )

    actual_hashes = _module_hashes()
    actual_hashes["lib.runner"] = "0" * 64
    with pytest.raises(LauncherPreflightError, match="actual module hash"):
        preflight_no_start(
            paths,
            _firmware(),
            actual_module_hashes=actual_hashes,
            required_uid=os.getuid(),
            runtime_uid=os.getuid(),
            runtime_gid=os.getgid(),
            allowed_private_roots=(paths.private_root,),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
            allowed_runtime_credential_paths=(paths.runtime_credential_dir,),
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
            runtime_uid=os.getuid(),
            runtime_gid=os.getgid(),
            allowed_private_roots=(paths.private_root,),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
            allowed_runtime_credential_paths=(paths.runtime_credential_dir,),
        )

    firmware = _firmware()
    firmware["message_bus_requires_emperor"] = False
    with pytest.raises(LauncherPreflightError, match="runtime contract"):
        preflight_no_start(
            paths,
            firmware,
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            runtime_uid=os.getuid(),
            runtime_gid=os.getgid(),
            allowed_private_roots=(paths.private_root,),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
            allowed_runtime_credential_paths=(paths.runtime_credential_dir,),
        )

    firmware = _firmware()
    firmware["candidate_root_emperor_permitted"] = True
    with pytest.raises(LauncherPreflightError, match="runtime contract"):
        preflight_no_start(
            paths,
            firmware,
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            runtime_uid=os.getuid(),
            runtime_gid=os.getgid(),
            allowed_private_roots=(paths.private_root,),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
            allowed_runtime_credential_paths=(paths.runtime_credential_dir,),
        )


def test_materialized_certificate_pair_is_accepted_but_start_remains_blocked(
    tmp_path: Path,
) -> None:
    paths = _paths(tmp_path)
    _private_file(paths.materialized_certificate_dir / "device.key", b"private-key-pem")
    _private_file(paths.materialized_certificate_dir / "device.cert", b"certificate-pem")

    plan = preflight_no_start(
        paths,
        _firmware(),
        actual_module_hashes=_module_hashes(),
        required_uid=os.getuid(),
        runtime_uid=os.getuid(),
        runtime_gid=os.getgid(),
        allowed_private_roots=(paths.private_root,),
        allowed_persistent_roots=(paths.persistent_root,),
        allowed_runtime_roots=(paths.runtime_dir,),
        allowed_runtime_credential_paths=(paths.runtime_credential_dir,),
    )

    assert plan.certificate_material_present is True
    assert plan.start_permitted is False
    assert plan.runtime_credentials_present is False
    assert plan.blocked_reason == "runtime_credential_handoff_required"


def test_exact_runtime_handoff_completes_identity_but_still_cannot_start(
    tmp_path: Path,
) -> None:
    paths = _paths(tmp_path)
    _private_file(paths.materialized_certificate_dir / "device.key", b"private-key-pem")
    _private_file(paths.materialized_certificate_dir / "device.cert", b"certificate-pem")
    paths.runtime_credential_dir.mkdir(mode=0o750)
    paths.certificate_dir.mkdir(mode=0o750)
    os.chown(paths.runtime_credential_dir, os.getuid(), os.getgid())
    os.chown(paths.certificate_dir, os.getuid(), os.getgid())
    for path, value in (
        (paths.runtime_credential_dir / "device_id", (DEVICE_ID + "\n").encode()),
        (paths.bootstrap_path, b"opaque-bootstrap"),
        (paths.certificate_dir / "device.key", b"private-key-pem"),
        (paths.certificate_dir / "device.cert", b"certificate-pem"),
    ):
        path.write_bytes(value)
        path.chmod(0o640)
        os.chown(path, os.getuid(), os.getgid())

    plan = preflight_no_start(
        paths,
        _firmware(),
        actual_module_hashes=_module_hashes(),
        required_uid=os.getuid(),
        runtime_uid=os.getuid(),
        runtime_gid=os.getgid(),
        allowed_private_roots=(paths.private_root,),
        allowed_persistent_roots=(paths.persistent_root,),
        allowed_runtime_roots=(paths.runtime_dir,),
        allowed_runtime_credential_paths=(paths.runtime_credential_dir,),
    )

    assert plan.certificate_material_present is True
    assert plan.runtime_credentials_present is True
    assert plan.runtime_user_handoff_complete is True
    assert plan.identity_contract_complete is True
    assert plan.start_permitted is False
    assert plan.launcher_implementation_present is True
    assert plan.blocked_reason == "nonroot_service_install_and_compatibility_validation_required"


def test_runtime_handoff_must_match_private_sources_and_remain_group_read_only(
    tmp_path: Path,
) -> None:
    paths = _paths(tmp_path)
    _private_file(paths.materialized_certificate_dir / "device.key", b"private-key-pem")
    _private_file(paths.materialized_certificate_dir / "device.cert", b"certificate-pem")
    paths.runtime_credential_dir.mkdir(mode=0o750)
    paths.certificate_dir.mkdir(mode=0o750)
    os.chown(paths.runtime_credential_dir, os.getuid(), os.getgid())
    os.chown(paths.certificate_dir, os.getuid(), os.getgid())
    for path, value in (
        (paths.runtime_credential_dir / "device_id", (DEVICE_ID + "\n").encode()),
        (paths.bootstrap_path, b"wrong-bootstrap"),
        (paths.certificate_dir / "device.key", b"private-key-pem"),
        (paths.certificate_dir / "device.cert", b"certificate-pem"),
    ):
        path.write_bytes(value)
        path.chmod(0o640)
        os.chown(path, os.getuid(), os.getgid())

    with pytest.raises(LauncherPreflightError, match="bootstrap does not match"):
        preflight_no_start(
            paths,
            _firmware(),
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            runtime_uid=os.getuid(),
            runtime_gid=os.getgid(),
            allowed_private_roots=(paths.private_root,),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
            allowed_runtime_credential_paths=(paths.runtime_credential_dir,),
        )

    paths.bootstrap_path.write_bytes(b"opaque-bootstrap")
    paths.bootstrap_path.chmod(0o640)
    (paths.certificate_dir / "device.key").chmod(0o600)
    with pytest.raises(LauncherPreflightError, match="runtime device.key.*0640"):
        preflight_no_start(
            paths,
            _firmware(),
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            runtime_uid=os.getuid(),
            runtime_gid=os.getgid(),
            allowed_private_roots=(paths.private_root,),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
            allowed_runtime_credential_paths=(paths.runtime_credential_dir,),
        )


def test_rejects_physical_socket_shared_or_colliding_paths(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    with pytest.raises(LauncherPreflightError, match="physical Control"):
        preflight_no_start(
            replace(paths, socket_path=Path("/var/run/brilliant/server_socket")),
            _firmware(),
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            runtime_uid=os.getuid(),
            runtime_gid=os.getgid(),
            allowed_private_roots=(paths.private_root,),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
            allowed_runtime_credential_paths=(paths.runtime_credential_dir,),
        )

    with pytest.raises(LauncherPreflightError, match="distinct"):
        preflight_no_start(
            replace(paths, state_dir=paths.process_config_dir),
            _firmware(),
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            runtime_uid=os.getuid(),
            runtime_gid=os.getgid(),
            allowed_private_roots=(paths.private_root,),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
            allowed_runtime_credential_paths=(paths.runtime_credential_dir,),
        )

    with pytest.raises(LauncherPreflightError, match="distinct"):
        preflight_no_start(
            replace(paths, stats_socket_path=paths.socket_path),
            _firmware(),
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            runtime_uid=os.getuid(),
            runtime_gid=os.getgid(),
            allowed_private_roots=(paths.private_root,),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
            allowed_runtime_credential_paths=(paths.runtime_credential_dir,),
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
            runtime_uid=os.getuid(),
            runtime_gid=os.getgid(),
            allowed_private_roots=(paths.private_root,),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
            allowed_runtime_credential_paths=(paths.runtime_credential_dir,),
        )

    paths = _paths(tmp_path / "mode")
    paths.process_config_dir.chmod(0o755)
    with pytest.raises(LauncherPreflightError, match="0700"):
        preflight_no_start(
            paths,
            _firmware(),
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            runtime_uid=os.getuid(),
            runtime_gid=os.getgid(),
            allowed_private_roots=(paths.private_root,),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
            allowed_runtime_credential_paths=(paths.runtime_credential_dir,),
        )

    paths = _paths(tmp_path / "nonempty")
    (paths.state_dir / "old-state").write_text("stale", encoding="utf-8")
    with pytest.raises(LauncherPreflightError, match="empty"):
        preflight_no_start(
            paths,
            _firmware(),
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            runtime_uid=os.getuid(),
            runtime_gid=os.getgid(),
            allowed_private_roots=(paths.private_root,),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
            allowed_runtime_credential_paths=(paths.runtime_credential_dir,),
        )

    paths = _paths(tmp_path / "socket")
    paths.socket_path.touch(mode=0o600)
    with pytest.raises(LauncherPreflightError, match="socket"):
        preflight_no_start(
            paths,
            _firmware(),
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            runtime_uid=os.getuid(),
            runtime_gid=os.getgid(),
            allowed_private_roots=(paths.private_root,),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
            allowed_runtime_credential_paths=(paths.runtime_credential_dir,),
        )

    paths = _paths(tmp_path / "stats-socket")
    paths.stats_socket_path.touch(mode=0o600)
    with pytest.raises(LauncherPreflightError, match="stats socket"):
        preflight_no_start(
            paths,
            _firmware(),
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            runtime_uid=os.getuid(),
            runtime_gid=os.getgid(),
            allowed_private_roots=(paths.private_root,),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
            allowed_runtime_credential_paths=(paths.runtime_credential_dir,),
        )

    paths = _paths(tmp_path / "flagfiles")
    (paths.process_flagfile_dir / "stale").write_text("stale", encoding="utf-8")
    with pytest.raises(LauncherPreflightError, match="flagfile.*empty"):
        preflight_no_start(
            paths,
            _firmware(),
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            runtime_uid=os.getuid(),
            runtime_gid=os.getgid(),
            allowed_private_roots=(paths.private_root,),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
            allowed_runtime_credential_paths=(paths.runtime_credential_dir,),
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
            runtime_uid=os.getuid(),
            runtime_gid=os.getgid(),
            allowed_private_roots=(paths.private_root,),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
            allowed_runtime_credential_paths=(paths.runtime_credential_dir,),
        )

    paths = _paths(tmp_path / "not-runtime-readable")
    paths.release_info_path.chmod(0o600)
    with pytest.raises(LauncherPreflightError, match="release metadata.*0644"):
        preflight_no_start(
            paths,
            _firmware(),
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            runtime_uid=os.getuid(),
            runtime_gid=os.getgid(),
            allowed_private_roots=(paths.private_root,),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
            allowed_runtime_credential_paths=(paths.runtime_credential_dir,),
        )


def test_art_preload_directory_must_be_read_only_stock_metadata(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    paths.art_preload_dir.chmod(0o775)

    with pytest.raises(LauncherPreflightError, match="art preload.*0755"):
        preflight_no_start(
            paths,
            _firmware(),
            actual_module_hashes=_module_hashes(),
            required_uid=os.getuid(),
            runtime_uid=os.getuid(),
            runtime_gid=os.getgid(),
            allowed_private_roots=(paths.private_root,),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
            allowed_runtime_credential_paths=(paths.runtime_credential_dir,),
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
            runtime_uid=os.getuid(),
            runtime_gid=os.getgid(),
            allowed_private_roots=(paths.private_root,),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
            allowed_runtime_credential_paths=(paths.runtime_credential_dir,),
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
            runtime_uid=os.getuid(),
            runtime_gid=os.getgid(),
            allowed_private_roots=(paths.private_root,),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
            allowed_runtime_credential_paths=(paths.runtime_credential_dir,),
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
            runtime_uid=os.getuid(),
            runtime_gid=os.getgid(),
            allowed_private_roots=(paths.private_root,),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
            allowed_runtime_credential_paths=(paths.runtime_credential_dir,),
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
            runtime_uid=os.getuid(),
            runtime_gid=os.getgid(),
            allowed_private_roots=(paths.private_root,),
            allowed_persistent_roots=(paths.persistent_root,),
            allowed_runtime_roots=(paths.runtime_dir,),
            allowed_runtime_credential_paths=(paths.runtime_credential_dir,),
        )


def test_hashes_actual_regular_module_files_without_following_symlinks(tmp_path: Path) -> None:
    module_paths: dict[str, Path] = {}
    expected: dict[str, str] = {}
    modes = _firmware()["runtime_file_modes"]
    assert isinstance(modes, dict)
    for index, name in enumerate(_module_hashes()):
        path = tmp_path / f"module-{index}.so"
        content = f"module-{index}".encode()
        path.write_bytes(content)
        path.chmod(int(str(modes[name]), 8))
        module_paths[name] = path
        expected[name] = hashlib.sha256(content).hexdigest()

    assert (
        hash_firmware_modules(
            module_paths=module_paths,
            required_uid=os.getuid(),
        )
        == expected
    )

    module_paths["runtime.uwsgi"].chmod(0o644)
    with pytest.raises(LauncherPreflightError, match="mode 0755"):
        hash_firmware_modules(module_paths=module_paths, required_uid=os.getuid())
    module_paths["runtime.uwsgi"].chmod(0o755)

    target = module_paths["lib.runner"]
    target.unlink()
    target.symlink_to(module_paths["bus.message_bus"])
    with pytest.raises(LauncherPreflightError, match="symlink"):
        hash_firmware_modules(module_paths=module_paths, required_uid=os.getuid())

    target.unlink()
    target.write_bytes(b"module-drift-surface")
    target.chmod(0o666)
    with pytest.raises(LauncherPreflightError, match="mode 0755"):
        hash_firmware_modules(module_paths=module_paths, required_uid=os.getuid())
