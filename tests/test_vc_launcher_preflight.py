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
        "schema_version": 1,
        "firmware_version": "v26.06.03.1",
        "module_sha256": {
            "bus.message_bus": "a85b7a2d0c2533db8d803a217027dbdd245bc104f221bf6955907dc0b8f6feb8",
            "lib.runner": "4ba40ac7d7695dc239590defbc6efd3d22efbf296fc1c2b40f139fb6e1fe3cb0",
            "peripherals.bootstrap.bootstrap_peripheral": (
                "313d526a3fe1ad1879137a83eaa55096d9b0fb7a08cac30e37a79ea3632d57db"
            ),
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
    }


def _module_hashes() -> dict[str, str]:
    hashes = _firmware()["module_sha256"]
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
    for directory in (identity, state, certificates, process_config):
        directory.mkdir(mode=0o700)

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
        runtime_dir=runtime,
        socket_path=runtime / "server_socket",
    )


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
        "identity_file_count": 4,
        "device_id_redacted": "aaaa…aaaa",
        "identity_contract_complete": False,
        "launcher_implementation_present": False,
        "start_permitted": False,
        "blocked_reason": "official_identity_consumer_unresolved",
    }
    assert DEVICE_ID not in json.dumps(plan.to_public_dict())
    assert not hasattr(plan, "command")


def test_hash_or_interface_drift_blocks_before_a_plan(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    firmware = _firmware()
    snapshot_hashes = firmware["module_sha256"]
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
