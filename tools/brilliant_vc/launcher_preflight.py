"""No-start preflight for an isolated Brilliant Virtual Control runtime.

This module deliberately has no process creation, firmware import, socket
connection, or executable command builder. It validates the pinned uWSGI
runtime surface, provisioned private identity, and isolated filesystem
topology, then reports the next unresolved contract without starting anything.
"""

from __future__ import annotations

import argparse
import grp
import hashlib
import json
import os
import pwd
import re
import secrets
import stat
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

_SCHEMA_VERSION = 5
_PINNED_FIRMWARE = "v26.06.03.1"
_PINNED_HASHES = {
    "bridge.remote_bridge": ("94ac32df6184814950cc5bc3ebeac828518b858f8fd6ce76380b67f20ccf20e4"),
    "bus.message_bus": "a85b7a2d0c2533db8d803a217027dbdd245bc104f221bf6955907dc0b8f6feb8",
    "bus.peripheral_process_manager": (
        "da8d7678c2a7d798aac2e3d735ac6e0789359bfb47226e5b8702f3923fcc7135"
    ),
    "configs.process_configs": ("a8ea4ad3885ac0d826da0073b2696a026f8d38a071418fe13196eb554b9e94a9"),
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
    "runtime.run_py": "70d03e29277862a93da7840ca2224b5b27293158d30e3054a8b17068dbb0d961",
    "runtime.process_default_ini": (
        "28ed9ff992040ccf14f9004e3ca139abe16c471547900046fb57d2455996099a"
    ),
    "runtime.run_startable_py": (
        "828a3d1d088e8db597e88c4f9be31f2cb6013efab6ee1d6669937d0c09c1f60c"
    ),
    "runtime.uwsgi": "3384606e779e7a4216f4ff27e39e10221cd0b377c02ad1d9fd8ea61269ecbc43",
    "runtime.approval_move": ("dd06bbb44fb05d8f82edf91be54b0785f33c43e48b756a552ef80e17760f93bb"),
    "runtime.python": "2d78bffcfbe8c92d169c4aa615364661bddc9afae5a3cf4ab336d66a2b95e179",
}
_PINNED_MODES = {
    "bridge.remote_bridge": 0o755,
    "bus.message_bus": 0o755,
    "bus.peripheral_process_manager": 0o755,
    "configs.process_configs": 0o644,
    "configs.socket_parameters": 0o644,
    "lib.process_management.process_manager": 0o755,
    "lib.runner": 0o755,
    "peripherals.bootstrap.bootstrap_peripheral": 0o755,
    "peripherals.discovery.discovery_peripheral": 0o755,
    "peripherals.lib.peripheral_service.peripheral_host": 0o755,
    "peripherals.configs.art_config_peripheral": 0o755,
    "peripherals.configs.device_config_peripheral": 0o755,
    "peripherals.configs.motion_detection_config_peripheral": 0o755,
    "peripherals.configs.alarm_config_peripheral": 0o755,
    "runtime.run_py": 0o644,
    "runtime.process_default_ini": 0o644,
    "runtime.run_startable_py": 0o644,
    "runtime.uwsgi": 0o755,
    "runtime.approval_move": 0o755,
    "runtime.python": 0o755,
}
_MESSAGE_BUS_PARAMETERS = frozenset({"home_id", "device_id", "mb_state_dir", "is_virtual_control"})
_RUNNER_PARAMETERS = frozenset({"startable_config", "module_name_override"})
_BOOTSTRAP_FIELDS = frozenset({"target_home_id", "server_authentication_token", "wifi_variables"})
_IDENTITY_FILES = frozenset({"device_id", "pkcs12_certificate", "bootstrap", "metadata.json"})
_CERTIFICATE_FILES = frozenset({"device.key", "device.cert"})
_RUNTIME_CREDENTIAL_ENTRIES = frozenset({"device_id", "bootstrap", "certificates"})
_RUNTIME_CREDENTIAL_LAYOUT = frozenset(
    {
        "device_id",
        "bootstrap",
        "certificates/device.key",
        "certificates/device.cert",
    }
)
_REMOTE_BRIDGE_PARAMETERS = frozenset(
    {
        "listen_port",
        "enable_bluetooth_provisioning",
        "enforce_strict_authentication",
        "message_bus_address_override",
        "device_provisioning_ip_listen_port",
        "ble_mesh_debug_interface_listen_port",
        "stub_ble_peripheral",
        "uwsgi_stats_socket_path",
    }
)
_DISCOVERY_FIELDS = frozenset(
    {
        "remote_bridge_port",
        "enable_remote_bridge_service_discovery",
        "message_bus_address_override",
    }
)
_CANDIDATE_PROCESSES = (
    "message_bus",
    "discovery_peripheral",
    "config_peripherals",
    "bootstrap",
)
_MESSAGE_BUS_EMBEDDED_STARTABLES = frozenset({"remote_bridge"})
_CONFIG_EMBEDDED_STARTABLES = frozenset(
    {
        "art_config_peripheral",
        "device_config_peripheral",
        "motion_detection_config_peripheral",
        "alarm_config_peripheral",
    }
)
_VASSAL_USER_FIELDS = frozenset({"user_override", "group_override"})
_RUNTIME_PATH_FLAGS = frozenset(
    {
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
    }
)
_DEVICE_ID = re.compile(r"^[0-9a-f]{32}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_PHYSICAL_SOCKET = Path("/var/run/brilliant/server_socket")
_DEFAULT_PERSISTENT_ROOTS = (Path("/data/brilliant-vc"),)
_DEFAULT_PRIVATE_ROOTS = (Path("/data/brilliant-vc-private"),)
_DEFAULT_RUNTIME_CREDENTIAL_PATHS = (Path("/data/brilliant-vc-credentials"),)
_DEFAULT_RUNTIME_ROOTS = (Path("/run/brilliant-vc"), Path("/var/run/brilliant-vc"))
_PROTECTED_ROOTS = (
    Path("/var/device_variables"),
    Path("/var/run/brilliant"),
    Path("/var/brilliant"),
    Path("/data/switch-embedded"),
)
_PINNED_MODULE_PATHS = {
    "bridge.remote_bridge": Path(
        "/data/switch-embedded/env/lib/python3.10/site-packages/"
        "bridge/remote_bridge.cpython-310-arm-linux-gnueabi.so"
    ),
    "bus.message_bus": Path(
        "/data/switch-embedded/env/lib/python3.10/site-packages/"
        "bus/message_bus.cpython-310-arm-linux-gnueabi.so"
    ),
    "bus.peripheral_process_manager": Path(
        "/data/switch-embedded/env/lib/python3.10/site-packages/"
        "bus/peripheral_process_manager.cpython-310-arm-linux-gnueabi.so"
    ),
    "configs.process_configs": Path(
        "/data/switch-embedded/env/lib/python3.10/site-packages/configs/process_configs.py"
    ),
    "configs.socket_parameters": Path(
        "/data/switch-embedded/env/lib/python3.10/site-packages/configs/socket_parameters.py"
    ),
    "lib.process_management.process_manager": Path(
        "/data/switch-embedded/env/lib/python3.10/site-packages/lib/process_management/"
        "process_manager.cpython-310-arm-linux-gnueabi.so"
    ),
    "lib.runner": Path(
        "/data/switch-embedded/env/lib/python3.10/site-packages/"
        "lib/runner.cpython-310-arm-linux-gnueabi.so"
    ),
    "peripherals.bootstrap.bootstrap_peripheral": Path(
        "/data/switch-embedded/env/lib/python3.10/site-packages/peripherals/"
        "bootstrap/bootstrap_peripheral.cpython-310-arm-linux-gnueabi.so"
    ),
    "peripherals.discovery.discovery_peripheral": Path(
        "/data/switch-embedded/env/lib/python3.10/site-packages/peripherals/discovery/"
        "discovery_peripheral.cpython-310-arm-linux-gnueabi.so"
    ),
    "peripherals.lib.peripheral_service.peripheral_host": Path(
        "/data/switch-embedded/env/lib/python3.10/site-packages/peripherals/lib/"
        "peripheral_service/peripheral_host.cpython-310-arm-linux-gnueabi.so"
    ),
    "peripherals.configs.art_config_peripheral": Path(
        "/data/switch-embedded/env/lib/python3.10/site-packages/peripherals/configs/"
        "art_config_peripheral.cpython-310-arm-linux-gnueabi.so"
    ),
    "peripherals.configs.device_config_peripheral": Path(
        "/data/switch-embedded/env/lib/python3.10/site-packages/peripherals/configs/"
        "device_config_peripheral.cpython-310-arm-linux-gnueabi.so"
    ),
    "peripherals.configs.motion_detection_config_peripheral": Path(
        "/data/switch-embedded/env/lib/python3.10/site-packages/peripherals/configs/"
        "motion_detection_config_peripheral.cpython-310-arm-linux-gnueabi.so"
    ),
    "peripherals.configs.alarm_config_peripheral": Path(
        "/data/switch-embedded/env/lib/python3.10/site-packages/peripherals/configs/"
        "alarm_config_peripheral.cpython-310-arm-linux-gnueabi.so"
    ),
    "runtime.run_py": Path("/data/switch-embedded/run.py"),
    "runtime.process_default_ini": Path(
        "/data/switch-embedded/lib/process_management/process-default.ini"
    ),
    "runtime.run_startable_py": Path(
        "/data/switch-embedded/env/lib/python3.10/site-packages/lib/startables/run_startable.py"
    ),
    "runtime.uwsgi": Path("/data/switch-embedded/env/bin/uwsgi"),
    "runtime.approval_move": Path("/usr/bin/mv.coreutils"),
    "runtime.python": Path("/usr/bin/python3.10"),
}
_MAX_IDENTITY_BYTES = 1024 * 1024
_MAX_METADATA_BYTES = 64 * 1024
_MAX_PEM_BYTES = 128 * 1024
_MAX_MODULE_BYTES = 64 * 1024 * 1024
_MAX_SHADOW_BYTES = 4 * 1024 * 1024
_RUNTIME_HOME = "/nonexistent"
_RUNTIME_SHELL = "/usr/sbin/nologin"


class LauncherPreflightError(ValueError):
    """Raised when a no-start prerequisite cannot be trusted."""


@dataclass(frozen=True, slots=True)
class LauncherPaths:
    """Every path that a future launcher would be permitted to use."""

    private_root: Path
    persistent_root: Path
    identity_dir: Path
    materialized_certificate_dir: Path
    runtime_credential_dir: Path
    bootstrap_path: Path
    state_dir: Path
    certificate_dir: Path
    process_config_dir: Path
    process_flagfile_dir: Path
    startable_config_dir: Path
    log_dir: Path
    error_log_dir: Path
    trace_dir: Path
    runtime_dir: Path
    socket_path: Path
    stats_socket_path: Path
    release_info_path: Path
    tracking_branch_path: Path
    art_preload_dir: Path


@dataclass(frozen=True, slots=True)
class NoStartPlan:
    """Redacted prerequisites report that cannot be turned into a command."""

    firmware_matches: bool
    interfaces_match: bool
    identity_inputs_valid: bool
    paths_isolated: bool
    private_modes_valid: bool
    empty_runtime_paths: bool
    certificate_material_present: bool
    runtime_credentials_present: bool
    identity_file_count: int
    device_id_redacted: str
    uwsgi_contract_confirmed: bool
    stock_process_manager_lifecycle_confirmed: bool
    nonroot_emperor_confirmed: bool
    direct_runner_rejected: bool
    identity_contract_complete: bool
    full_path_surface_validated: bool
    candidate_manifest_present: bool
    runtime_user_handoff_complete: bool
    launcher_implementation_present: bool
    start_permitted: bool
    blocked_reason: str

    def to_public_dict(self) -> dict[str, object]:
        return {
            "firmware_matches": self.firmware_matches,
            "interfaces_match": self.interfaces_match,
            "identity_inputs_valid": self.identity_inputs_valid,
            "paths_isolated": self.paths_isolated,
            "private_modes_valid": self.private_modes_valid,
            "empty_runtime_paths": self.empty_runtime_paths,
            "certificate_material_present": self.certificate_material_present,
            "runtime_credentials_present": self.runtime_credentials_present,
            "identity_file_count": self.identity_file_count,
            "device_id_redacted": self.device_id_redacted,
            "uwsgi_contract_confirmed": self.uwsgi_contract_confirmed,
            "stock_process_manager_lifecycle_confirmed": (
                self.stock_process_manager_lifecycle_confirmed
            ),
            "nonroot_emperor_confirmed": self.nonroot_emperor_confirmed,
            "direct_runner_rejected": self.direct_runner_rejected,
            "identity_contract_complete": self.identity_contract_complete,
            "full_path_surface_validated": self.full_path_surface_validated,
            "candidate_manifest_present": self.candidate_manifest_present,
            "runtime_user_handoff_complete": self.runtime_user_handoff_complete,
            "launcher_implementation_present": self.launcher_implementation_present,
            "start_permitted": self.start_permitted,
            "blocked_reason": self.blocked_reason,
        }


def preflight_no_start(
    paths: LauncherPaths,
    firmware_snapshot: Mapping[str, object],
    *,
    actual_module_hashes: Mapping[str, object],
    required_uid: int = 0,
    runtime_uid: int | None = None,
    runtime_gid: int | None = None,
    allowed_private_roots: Sequence[Path] = _DEFAULT_PRIVATE_ROOTS,
    allowed_persistent_roots: Sequence[Path] = _DEFAULT_PERSISTENT_ROOTS,
    allowed_runtime_roots: Sequence[Path] = _DEFAULT_RUNTIME_ROOTS,
    allowed_runtime_credential_paths: Sequence[Path] = _DEFAULT_RUNTIME_CREDENTIAL_PATHS,
) -> NoStartPlan:
    """Validate known prerequisites without creating a runnable launch command."""

    _validate_firmware_snapshot(firmware_snapshot)
    validate_actual_module_hashes(actual_module_hashes)
    if runtime_uid is None or runtime_gid is None:
        raise LauncherPreflightError("dedicated runtime identity is required")
    certificate_material_present, runtime_credentials_present = _validate_path_topology(
        paths,
        required_uid=required_uid,
        runtime_uid=runtime_uid,
        runtime_gid=runtime_gid,
        allowed_private_roots=allowed_private_roots,
        allowed_persistent_roots=allowed_persistent_roots,
        allowed_runtime_roots=allowed_runtime_roots,
        allowed_runtime_credential_paths=allowed_runtime_credential_paths,
    )
    redacted_device_id = _validate_identity(paths.identity_dir, required_uid=required_uid)
    return NoStartPlan(
        firmware_matches=True,
        interfaces_match=True,
        identity_inputs_valid=True,
        paths_isolated=True,
        private_modes_valid=True,
        empty_runtime_paths=True,
        certificate_material_present=certificate_material_present,
        runtime_credentials_present=runtime_credentials_present,
        identity_file_count=len(_IDENTITY_FILES),
        device_id_redacted=redacted_device_id,
        uwsgi_contract_confirmed=True,
        stock_process_manager_lifecycle_confirmed=True,
        nonroot_emperor_confirmed=True,
        direct_runner_rejected=True,
        identity_contract_complete=runtime_credentials_present,
        full_path_surface_validated=True,
        candidate_manifest_present=True,
        runtime_user_handoff_complete=runtime_credentials_present,
        launcher_implementation_present=True,
        start_permitted=False,
        blocked_reason=(
            "nonroot_service_install_and_compatibility_validation_required"
            if runtime_credentials_present
            else (
                "runtime_credential_handoff_required"
                if certificate_material_present
                else "identity_materialization_required"
            )
        ),
    )


def validate_actual_module_hashes(actual_module_hashes: Mapping[str, object]) -> None:
    """Reject any launcher/configuration file drift from the pinned firmware."""

    actual_hashes = _hash_inventory(actual_module_hashes, "actual module hash")
    if actual_hashes != _PINNED_HASHES:
        raise LauncherPreflightError("actual module hash drift blocks the launcher")


def _validate_firmware_snapshot(snapshot: Mapping[str, object]) -> None:
    expected_fields = {
        "schema_version",
        "firmware_version",
        "runtime_sha256",
        "runtime_file_modes",
        "message_bus_parameters",
        "runner_parameters",
        "bootstrap_fields",
        "virtual_control_flag",
        "runtime_launcher",
        "message_bus_requires_emperor",
        "certificate_files",
        "remote_bridge_parameters",
        "discovery_fields",
        "known_process_count",
        "candidate_processes",
        "candidate_disabled_process_count",
        "process_manager_launch_mode",
        "message_bus_embedded_startables",
        "config_peripherals_embedded_startables",
        "local_message_bus_address_mode",
        "local_message_bus_address_override",
        "vassal_user_fields",
        "runtime_path_flags",
        "candidate_supervisor_mode",
        "candidate_root_emperor_permitted",
        "candidate_generated_directory_mode",
        "runtime_credential_layout",
        "runtime_credential_access",
    }
    if set(snapshot) != expected_fields or snapshot.get("schema_version") != _SCHEMA_VERSION:
        raise LauncherPreflightError("firmware snapshot schema is invalid")
    if snapshot["firmware_version"] != _PINNED_FIRMWARE:
        raise LauncherPreflightError("firmware version does not match the pinned build")

    hashes = _hash_inventory(snapshot["runtime_sha256"], "firmware runtime hash")
    if hashes != _PINNED_HASHES:
        raise LauncherPreflightError("firmware module hash drift blocks the launcher")
    modes = snapshot["runtime_file_modes"]
    if not isinstance(modes, Mapping) or set(modes) != set(_PINNED_MODES):
        raise LauncherPreflightError("firmware runtime mode inventory is invalid")
    rendered_modes = {name: f"{mode:04o}" for name, mode in _PINNED_MODES.items()}
    if dict(modes) != rendered_modes:
        raise LauncherPreflightError("firmware runtime mode drift blocks the launcher")

    message_bus_parameters = _parameter_set(
        snapshot["message_bus_parameters"], "message-bus interface"
    )
    runner_parameters = _parameter_set(snapshot["runner_parameters"], "runner interface")
    bootstrap_fields = _parameter_set(snapshot["bootstrap_fields"], "bootstrap interface")
    certificate_files = _literal_set(snapshot["certificate_files"], "certificate file contract")
    remote_bridge_parameters = _parameter_set(
        snapshot["remote_bridge_parameters"], "remote-bridge interface"
    )
    discovery_fields = _parameter_set(snapshot["discovery_fields"], "discovery interface")
    candidate_processes = _literal_set(
        snapshot["candidate_processes"], "candidate process contract"
    )
    message_bus_startables = _literal_set(
        snapshot["message_bus_embedded_startables"], "message-bus embedded startables"
    )
    config_startables = _literal_set(
        snapshot["config_peripherals_embedded_startables"],
        "configuration embedded startables",
    )
    vassal_user_fields = _parameter_set(
        snapshot["vassal_user_fields"], "vassal user override contract"
    )
    runtime_path_flags = _parameter_set(
        snapshot["runtime_path_flags"], "runtime path flag contract"
    )
    runtime_credential_layout = _literal_set(
        snapshot["runtime_credential_layout"], "runtime credential layout"
    )
    if (
        not _MESSAGE_BUS_PARAMETERS <= message_bus_parameters
        or not _RUNNER_PARAMETERS <= runner_parameters
        or bootstrap_fields != _BOOTSTRAP_FIELDS
        or snapshot["virtual_control_flag"] != "start_as_virtual_control"
    ):
        raise LauncherPreflightError("firmware interface drift blocks the launcher")
    if (
        snapshot["runtime_launcher"] != "uwsgi_emperor_vassal"
        or snapshot["message_bus_requires_emperor"] is not True
        or certificate_files != _CERTIFICATE_FILES
        or remote_bridge_parameters != _REMOTE_BRIDGE_PARAMETERS
        or discovery_fields != _DISCOVERY_FIELDS
        or type(snapshot["known_process_count"]) is not int
        or snapshot["known_process_count"] != 38
        or candidate_processes != frozenset(_CANDIDATE_PROCESSES)
        or type(snapshot["candidate_disabled_process_count"]) is not int
        or snapshot["candidate_disabled_process_count"] != 34
        or snapshot["process_manager_launch_mode"] != "message_bus_then_enabled_defaults"
        or message_bus_startables != _MESSAGE_BUS_EMBEDDED_STARTABLES
        or config_startables != _CONFIG_EMBEDDED_STARTABLES
        or snapshot["local_message_bus_address_mode"] != "server_socket_path_with_derived_unix_url"
        or snapshot["local_message_bus_address_override"] is not None
        or vassal_user_fields != _VASSAL_USER_FIELDS
        or runtime_path_flags != _RUNTIME_PATH_FLAGS
        or snapshot["candidate_supervisor_mode"] != "dedicated_nonroot_emperor"
        or snapshot["candidate_root_emperor_permitted"] is not False
        or snapshot["candidate_generated_directory_mode"] != "0700"
        or runtime_credential_layout != _RUNTIME_CREDENTIAL_LAYOUT
        or snapshot["runtime_credential_access"] != "root_owner_dedicated_group_read_only"
    ):
        raise LauncherPreflightError("firmware runtime contract drift blocks the launcher")


def _hash_inventory(value: object, description: str) -> dict[str, str]:
    if not isinstance(value, Mapping) or set(value) != set(_PINNED_HASHES):
        raise LauncherPreflightError(f"{description} inventory is invalid")
    hashes: dict[str, str] = {}
    for name, digest in value.items():
        if (
            not isinstance(name, str)
            or not isinstance(digest, str)
            or not _SHA256.fullmatch(digest)
        ):
            raise LauncherPreflightError(f"{description} is invalid")
        hashes[name] = digest
    return hashes


def _parameter_set(value: object, description: str) -> frozenset[str]:
    if not isinstance(value, list) or not value or len(value) > 64:
        raise LauncherPreflightError(f"{description} parameter list is invalid")
    parameters: set[str] = set()
    for parameter in value:
        if not isinstance(parameter, str) or _SAFE_NAME.fullmatch(parameter) is None:
            raise LauncherPreflightError(f"{description} parameter is invalid")
        if parameter in parameters:
            raise LauncherPreflightError(f"{description} parameter is duplicated")
        parameters.add(parameter)
    return frozenset(parameters)


def _literal_set(value: object, description: str) -> frozenset[str]:
    if not isinstance(value, list) or not value or len(value) > 16:
        raise LauncherPreflightError(f"{description} list is invalid")
    result: set[str] = set()
    for item in value:
        if not isinstance(item, str) or not item or len(item) > 128 or item in result:
            raise LauncherPreflightError(f"{description} value is invalid")
        result.add(item)
    return frozenset(result)


def _validate_path_topology(
    paths: LauncherPaths,
    *,
    required_uid: int,
    runtime_uid: int,
    runtime_gid: int,
    allowed_private_roots: Sequence[Path],
    allowed_persistent_roots: Sequence[Path],
    allowed_runtime_roots: Sequence[Path],
    allowed_runtime_credential_paths: Sequence[Path],
) -> tuple[bool, bool]:
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value < 0
        for value in (required_uid, runtime_uid, runtime_gid)
    ):
        raise LauncherPreflightError("orchestrator or runtime identity is invalid")
    if runtime_uid == 0 or runtime_gid == 0:
        raise LauncherPreflightError("runtime supervisor identity must be non-root")
    physical_socket = _PHYSICAL_SOCKET.resolve(strict=False)
    socket = paths.socket_path.resolve(strict=False)
    stats_socket = paths.stats_socket_path.resolve(strict=False)
    if socket == physical_socket or stats_socket == physical_socket:
        raise LauncherPreflightError("refusing the physical Control message-bus socket")

    private_root = _owned_directory(
        paths.private_root,
        description="private root",
        required_uid=required_uid,
        required_gid=None,
        required_mode=0o700,
    )
    if private_root not in {root.resolve(strict=False) for root in allowed_private_roots}:
        raise LauncherPreflightError("private root is outside the allowed VC roots")
    identity_dir = _owned_directory(
        paths.identity_dir,
        description="identity directory",
        required_uid=required_uid,
        required_gid=None,
        required_mode=0o700,
    )
    materialized_certificate_dir = _owned_directory(
        paths.materialized_certificate_dir,
        description="materialized certificate directory",
        required_uid=required_uid,
        required_gid=None,
        required_mode=0o700,
    )
    if (
        identity_dir.parent != private_root
        or materialized_certificate_dir.parent != private_root
        or identity_dir == materialized_certificate_dir
    ):
        raise LauncherPreflightError(
            "private identity and materialized certificates must be distinct direct children"
        )

    persistent_root = _owned_directory(
        paths.persistent_root,
        description="persistent root",
        required_uid=runtime_uid,
        required_gid=runtime_gid,
        required_mode=0o700,
    )
    runtime_root = _owned_directory(
        paths.runtime_dir,
        description="runtime root",
        required_uid=runtime_uid,
        required_gid=runtime_gid,
        required_mode=0o700,
    )
    if persistent_root not in {root.resolve(strict=False) for root in allowed_persistent_roots}:
        raise LauncherPreflightError("persistent root is outside the allowed VC roots")
    if runtime_root not in {root.resolve(strict=False) for root in allowed_runtime_roots}:
        raise LauncherPreflightError("runtime root is outside the allowed VC roots")

    runtime_credential_root = paths.runtime_credential_dir.resolve(strict=False)
    if runtime_credential_root not in {
        path.resolve(strict=False) for path in allowed_runtime_credential_paths
    }:
        raise LauncherPreflightError(
            "runtime credential root is outside the allowed VC credential paths"
        )
    root_pairs = (
        (private_root, persistent_root),
        (private_root, runtime_root),
        (persistent_root, runtime_root),
    )
    if any(_paths_overlap(left, right) for left, right in root_pairs):
        raise LauncherPreflightError("private, persistent, and runtime roots must not overlap")
    if paths.bootstrap_path.resolve(strict=False) != runtime_credential_root / "bootstrap":
        raise LauncherPreflightError("runtime bootstrap path is not canonical")
    if paths.certificate_dir.resolve(strict=False) != runtime_credential_root / "certificates":
        raise LauncherPreflightError("runtime certificate path is not canonical")

    resolved_directories: list[Path] = []
    for description, directory in (
        ("state directory", paths.state_dir),
        ("process-config directory", paths.process_config_dir),
        ("process-flagfile directory", paths.process_flagfile_dir),
        ("startable-config directory", paths.startable_config_dir),
        ("log directory", paths.log_dir),
        ("error-log directory", paths.error_log_dir),
        ("trace directory", paths.trace_dir),
    ):
        resolved = _owned_directory(
            directory,
            description=description,
            required_uid=runtime_uid,
            required_gid=runtime_gid,
            required_mode=0o700,
        )
        if resolved.parent != persistent_root:
            raise LauncherPreflightError(f"{description} must be directly below persistent root")
        resolved_directories.append(resolved)
    if len(set(resolved_directories)) != len(resolved_directories):
        raise LauncherPreflightError("all isolated VC directories must be distinct")

    if socket.parent != runtime_root or stats_socket.parent != runtime_root:
        raise LauncherPreflightError("VC sockets must be directly below the isolated runtime root")
    if socket == stats_socket:
        raise LauncherPreflightError("VC message-bus and stats sockets must be distinct")
    if paths.socket_path.exists() or paths.socket_path.is_symlink():
        raise LauncherPreflightError("VC socket path must not already exist")
    if paths.stats_socket_path.exists() or paths.stats_socket_path.is_symlink():
        raise LauncherPreflightError("VC stats socket path must not already exist")

    release_metadata = _validate_readonly_metadata_file(
        paths.release_info_path,
        description="release metadata",
        required_uid=required_uid,
    )
    tracking_metadata = _validate_readonly_metadata_file(
        paths.tracking_branch_path,
        description="tracking metadata",
        required_uid=required_uid,
    )
    if release_metadata == tracking_metadata:
        raise LauncherPreflightError("release and tracking metadata paths must be distinct")
    art_preload = _validate_readonly_directory(
        paths.art_preload_dir,
        description="art preload directory",
        required_uid=required_uid,
        required_mode=0o755,
    )

    for read_only_path, description in (
        (runtime_credential_root, "runtime credentials"),
        (art_preload, "art preload directory"),
    ):
        for disallowed_parent in (private_root, persistent_root, runtime_root):
            if not _paths_overlap(read_only_path, disallowed_parent):
                continue
            raise LauncherPreflightError(
                f"{description} must be outside private and service-writable roots"
            )
    if _paths_overlap(art_preload, runtime_credential_root):
        raise LauncherPreflightError("art preload directory must be outside runtime credentials")

    protected = tuple(root.resolve(strict=False) for root in _PROTECTED_ROOTS)
    for candidate in (
        *resolved_directories,
        private_root,
        identity_dir,
        materialized_certificate_dir,
        persistent_root,
        runtime_root,
        runtime_credential_root,
        art_preload,
        socket,
        stats_socket,
    ):
        for protected_root in protected:
            try:
                candidate.relative_to(protected_root)
            except ValueError:
                continue
            raise LauncherPreflightError("VC path collides with a protected physical-Control path")

    for description, directory in (
        ("state directory", paths.state_dir),
        ("process-config directory", paths.process_config_dir),
        ("process-flagfile directory", paths.process_flagfile_dir),
        ("startable-config directory", paths.startable_config_dir),
        ("log directory", paths.log_dir),
        ("error-log directory", paths.error_log_dir),
        ("trace directory", paths.trace_dir),
        ("runtime directory", paths.runtime_dir),
    ):
        if any(directory.iterdir()):
            raise LauncherPreflightError(f"{description} must be empty before launch")
    certificate_material_present = _validate_materialized_certificate_directory(
        paths.materialized_certificate_dir,
        required_uid=required_uid,
    )
    runtime_credentials_present = False
    if paths.runtime_credential_dir.exists() or paths.runtime_credential_dir.is_symlink():
        if not certificate_material_present:
            raise LauncherPreflightError(
                "runtime credentials exist before materialized certificates"
            )
        _validate_runtime_credentials(
            paths,
            required_uid=required_uid,
            runtime_gid=runtime_gid,
        )
        runtime_credentials_present = True
    return certificate_material_present, runtime_credentials_present


def _validate_readonly_metadata_file(
    path: Path,
    *,
    description: str,
    required_uid: int,
) -> Path:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        raise LauncherPreflightError(f"{description} does not exist") from None
    if stat.S_ISLNK(metadata.st_mode):
        raise LauncherPreflightError(f"{description} must not be a symlink")
    if not stat.S_ISREG(metadata.st_mode):
        raise LauncherPreflightError(f"{description} must be a regular file")
    if metadata.st_uid != required_uid or stat.S_IMODE(metadata.st_mode) != 0o644:
        raise LauncherPreflightError(
            f"{description} must have the required owner and runtime-readable mode 0644"
        )
    if metadata.st_nlink != 1:
        raise LauncherPreflightError(f"{description} must not be a hard link")
    if not 0 < metadata.st_size <= _MAX_METADATA_BYTES:
        raise LauncherPreflightError(f"{description} has an invalid size")
    return path.resolve(strict=True)


def _validate_readonly_directory(
    path: Path,
    *,
    description: str,
    required_uid: int,
    required_mode: int,
) -> Path:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        raise LauncherPreflightError(f"{description} does not exist") from None
    except OSError:
        raise LauncherPreflightError(f"could not inspect {description}") from None
    if stat.S_ISLNK(metadata.st_mode):
        raise LauncherPreflightError(f"{description} must not be a symlink")
    if not stat.S_ISDIR(metadata.st_mode):
        raise LauncherPreflightError(f"{description} must be a directory")
    if metadata.st_uid != required_uid or stat.S_IMODE(metadata.st_mode) != required_mode:
        raise LauncherPreflightError(
            f"{description} must have the required owner and mode {required_mode:04o}"
        )
    return path.resolve(strict=True)


def _validate_materialized_certificate_directory(path: Path, *, required_uid: int) -> bool:
    entries = {entry.name: entry for entry in path.iterdir()}
    if not entries:
        return False
    if set(entries) != _CERTIFICATE_FILES:
        raise LauncherPreflightError(
            "certificate directory must be empty or contain exactly device.key and device.cert"
        )
    for name, certificate_path in entries.items():
        _validate_private_file(
            certificate_path,
            description=f"materialized certificate {name}",
            required_uid=required_uid,
            maximum_bytes=_MAX_PEM_BYTES,
        )
    return True


def _validate_runtime_credentials(
    paths: LauncherPaths,
    *,
    required_uid: int,
    runtime_gid: int,
) -> None:
    runtime_root = _owned_directory(
        paths.runtime_credential_dir,
        description="runtime credential directory",
        required_uid=required_uid,
        required_gid=runtime_gid,
        required_mode=0o750,
    )
    entries = {entry.name: entry for entry in runtime_root.iterdir()}
    if set(entries) != _RUNTIME_CREDENTIAL_ENTRIES:
        raise LauncherPreflightError("runtime credential directory has unexpected entries")
    if entries["bootstrap"].resolve(strict=False) != paths.bootstrap_path.resolve(strict=False):
        raise LauncherPreflightError("runtime bootstrap path changed")
    certificate_dir = _owned_directory(
        entries["certificates"],
        description="runtime certificate directory",
        required_uid=required_uid,
        required_gid=runtime_gid,
        required_mode=0o750,
    )
    if certificate_dir != paths.certificate_dir.resolve(strict=False):
        raise LauncherPreflightError("runtime certificate path changed")
    certificate_entries = {entry.name: entry for entry in certificate_dir.iterdir()}
    if set(certificate_entries) != _CERTIFICATE_FILES:
        raise LauncherPreflightError(
            "runtime certificate directory must contain exactly device.key and device.cert"
        )

    source_paths = {
        "device_id": paths.identity_dir / "device_id",
        "bootstrap": paths.identity_dir / "bootstrap",
        "device.key": paths.materialized_certificate_dir / "device.key",
        "device.cert": paths.materialized_certificate_dir / "device.cert",
    }
    runtime_paths = {
        "device_id": entries["device_id"],
        "bootstrap": entries["bootstrap"],
        "device.key": certificate_entries["device.key"],
        "device.cert": certificate_entries["device.cert"],
    }
    for name in source_paths:
        maximum_bytes = {
            "bootstrap": _MAX_IDENTITY_BYTES,
            "device_id": _MAX_METADATA_BYTES,
            "device.key": _MAX_PEM_BYTES,
            "device.cert": _MAX_PEM_BYTES,
        }[name]
        expected = _read_private_file(
            source_paths[name],
            required_uid=required_uid,
            maximum_bytes=maximum_bytes,
        )
        if name == "device_id":
            canonical = bytearray(bytes(expected).strip() + b"\n")
            _wipe(expected)
            expected = canonical
        actual = _read_constrained_file(
            runtime_paths[name],
            description=f"runtime {name}",
            required_uid=required_uid,
            required_gid=runtime_gid,
            required_mode=0o640,
            maximum_bytes=maximum_bytes,
        )
        try:
            if not secrets.compare_digest(actual, expected):
                raise LauncherPreflightError(
                    f"runtime {name} does not match its root-private source"
                )
        finally:
            _wipe(actual)
            _wipe(expected)


def _owned_directory(
    path: Path,
    *,
    description: str,
    required_uid: int,
    required_gid: int | None,
    required_mode: int,
) -> Path:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        raise LauncherPreflightError(f"{description} does not exist") from None
    if stat.S_ISLNK(metadata.st_mode):
        raise LauncherPreflightError(f"{description} must not be a symlink")
    if not stat.S_ISDIR(metadata.st_mode):
        raise LauncherPreflightError(f"{description} must be a directory")
    if metadata.st_uid != required_uid or (
        required_gid is not None and metadata.st_gid != required_gid
    ):
        raise LauncherPreflightError(f"{description} has the wrong owner or group")
    if stat.S_IMODE(metadata.st_mode) != required_mode:
        raise LauncherPreflightError(f"{description} must have mode {required_mode:04o}")
    return path.resolve(strict=True)


def _paths_overlap(left: Path, right: Path) -> bool:
    try:
        left.relative_to(right)
        return True
    except ValueError:
        pass
    try:
        right.relative_to(left)
        return True
    except ValueError:
        return False


def _validate_identity(identity_dir: Path, *, required_uid: int) -> str:
    entries = {entry.name: entry for entry in identity_dir.iterdir()}
    if set(entries) != _IDENTITY_FILES:
        raise LauncherPreflightError("identity directory must contain exactly four expected files")
    for name, path in entries.items():
        _validate_private_file(
            path,
            description=f"identity {name}",
            required_uid=required_uid,
            maximum_bytes=(
                _MAX_METADATA_BYTES
                if name in {"device_id", "metadata.json"}
                else _MAX_IDENTITY_BYTES
            ),
        )

    raw_device_id = _read_private_file(
        entries["device_id"],
        required_uid=required_uid,
        maximum_bytes=_MAX_METADATA_BYTES,
    )
    try:
        try:
            device_id = bytes(raw_device_id).strip().decode("ascii")
        except UnicodeDecodeError:
            raise LauncherPreflightError("identity device ID is not ASCII") from None
        if _DEVICE_ID.fullmatch(device_id) is None:
            raise LauncherPreflightError("identity device ID is invalid")
    finally:
        _wipe(raw_device_id)

    raw_metadata = _read_private_file(
        entries["metadata.json"],
        required_uid=required_uid,
        maximum_bytes=_MAX_METADATA_BYTES,
    )
    try:
        try:
            parsed: object = json.loads(raw_metadata)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise LauncherPreflightError("identity metadata is invalid JSON") from None
    finally:
        _wipe(raw_metadata)
    if not isinstance(parsed, dict) or set(parsed) != {
        "device_id_redacted",
        "target_home_match",
    }:
        raise LauncherPreflightError("identity metadata schema is invalid")
    expected_redacted = _redact(device_id)
    if parsed["device_id_redacted"] != expected_redacted or parsed["target_home_match"] is not True:
        raise LauncherPreflightError("identity metadata does not match the provisioned identity")
    return expected_redacted


def _validate_private_file(
    path: Path,
    *,
    description: str,
    required_uid: int,
    maximum_bytes: int,
) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        raise LauncherPreflightError(f"{description} does not exist") from None
    if stat.S_ISLNK(metadata.st_mode):
        raise LauncherPreflightError(f"{description} must not be a symlink")
    if not stat.S_ISREG(metadata.st_mode):
        raise LauncherPreflightError(f"{description} must be a regular file")
    if metadata.st_uid != required_uid or stat.S_IMODE(metadata.st_mode) != 0o600:
        raise LauncherPreflightError(f"{description} must have the required owner and mode 0600")
    if metadata.st_nlink != 1:
        raise LauncherPreflightError(f"{description} must not be a hard link")
    if not 0 < metadata.st_size <= maximum_bytes:
        raise LauncherPreflightError(f"{description} has an invalid size")


def _read_private_file(path: Path, *, required_uid: int, maximum_bytes: int) -> bytearray:
    _validate_private_file(
        path,
        description=path.name,
        required_uid=required_uid,
        maximum_bytes=maximum_bytes,
    )
    before = path.lstat()
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        raise LauncherPreflightError("could not safely open private identity file") from None
    data = bytearray()
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise LauncherPreflightError("private identity file changed during open")
        while True:
            chunk = os.read(descriptor, min(8192, maximum_bytes + 1 - len(data)))
            if not chunk:
                break
            data.extend(chunk)
            if len(data) > maximum_bytes:
                raise LauncherPreflightError("private identity file exceeds its size bound")
        after = os.fstat(descriptor)
        if (
            after.st_size != opened.st_size
            or after.st_mtime_ns != opened.st_mtime_ns
            or after.st_ctime_ns != opened.st_ctime_ns
        ):
            raise LauncherPreflightError("private identity file changed while reading")
        return data
    except BaseException:
        _wipe(data)
        raise
    finally:
        os.close(descriptor)


def _read_constrained_file(
    path: Path,
    *,
    description: str,
    required_uid: int,
    required_gid: int,
    required_mode: int,
    maximum_bytes: int,
) -> bytearray:
    try:
        before = path.lstat()
    except FileNotFoundError:
        raise LauncherPreflightError(f"{description} does not exist") from None
    if stat.S_ISLNK(before.st_mode):
        raise LauncherPreflightError(f"{description} must not be a symlink")
    if not stat.S_ISREG(before.st_mode):
        raise LauncherPreflightError(f"{description} must be a regular file")
    if before.st_uid != required_uid or before.st_gid != required_gid:
        raise LauncherPreflightError(f"{description} has the wrong owner or group")
    if stat.S_IMODE(before.st_mode) != required_mode:
        raise LauncherPreflightError(f"{description} must have mode {required_mode:04o}")
    if before.st_nlink != 1:
        raise LauncherPreflightError(f"{description} must not be a hard link")
    if not 0 < before.st_size <= maximum_bytes:
        raise LauncherPreflightError(f"{description} has an invalid size")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        raise LauncherPreflightError(f"could not safely open {description}") from None
    data = bytearray()
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise LauncherPreflightError(f"{description} changed during open")
        while True:
            chunk = os.read(descriptor, min(8192, maximum_bytes + 1 - len(data)))
            if not chunk:
                break
            data.extend(chunk)
            if len(data) > maximum_bytes:
                raise LauncherPreflightError(f"{description} exceeds its size bound")
        after = os.fstat(descriptor)
        if (
            after.st_size != opened.st_size
            or after.st_mtime_ns != opened.st_mtime_ns
            or after.st_ctime_ns != opened.st_ctime_ns
        ):
            raise LauncherPreflightError(f"{description} changed while reading")
        return data
    except BaseException:
        _wipe(data)
        raise
    finally:
        os.close(descriptor)


def _redact(device_id: str) -> str:
    return f"{device_id[:4]}…{device_id[-4:]}"


def _wipe(value: bytearray) -> None:
    for index in range(len(value)):
        value[index] = 0


def hash_firmware_modules(
    *,
    module_paths: Mapping[str, Path] = _PINNED_MODULE_PATHS,
    required_uid: int = 0,
) -> dict[str, str]:
    """Hash every pinned runtime file without following links."""

    if set(module_paths) != set(_PINNED_HASHES):
        raise LauncherPreflightError("actual module path inventory is invalid")
    result: dict[str, str] = {}
    for name, path in module_paths.items():
        try:
            before = path.lstat()
        except FileNotFoundError:
            raise LauncherPreflightError("actual module file does not exist") from None
        if stat.S_ISLNK(before.st_mode):
            raise LauncherPreflightError("actual module file must not be a symlink")
        if not stat.S_ISREG(before.st_mode):
            raise LauncherPreflightError("actual module file must be regular")
        if before.st_uid != required_uid:
            raise LauncherPreflightError("actual module file has the wrong owner")
        expected_mode = _PINNED_MODES[name]
        if stat.S_IMODE(before.st_mode) != expected_mode:
            raise LauncherPreflightError(f"actual module file must have mode {expected_mode:04o}")
        if not 0 < before.st_size <= _MAX_MODULE_BYTES:
            raise LauncherPreflightError("actual module file has an invalid size")

        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
        try:
            descriptor = os.open(path, flags)
        except OSError:
            raise LauncherPreflightError("could not safely open actual module file") from None
        digest = hashlib.sha256()
        try:
            opened = os.fstat(descriptor)
            if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
                raise LauncherPreflightError("actual module file changed during open")
            while True:
                chunk = os.read(descriptor, 1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
            after = os.fstat(descriptor)
            if (
                after.st_size != opened.st_size
                or after.st_mtime_ns != opened.st_mtime_ns
                or after.st_ctime_ns != opened.st_ctime_ns
            ):
                raise LauncherPreflightError("actual module file changed while hashing")
        finally:
            os.close(descriptor)
        result[name] = digest.hexdigest()
    return result


def load_firmware_snapshot(path: Path, *, required_uid: int = 0) -> dict[str, object]:
    """Load a bounded private introspection snapshot without following links."""

    raw = _read_private_file(
        path,
        required_uid=required_uid,
        maximum_bytes=_MAX_METADATA_BYTES,
    )
    try:
        try:
            parsed: object = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise LauncherPreflightError("firmware snapshot is not valid JSON") from None
    finally:
        _wipe(raw)
    if not isinstance(parsed, dict):
        raise LauncherPreflightError("firmware snapshot must be an object")
    return parsed


def _validate_runtime_account_contract(
    account: object,
    runtime_group: object,
    *,
    all_accounts: Sequence[object],
    all_groups: Sequence[object],
    shadow_path: Path,
    required_uid: int,
) -> None:
    """Validate the locked, login-disabled, single-member service account."""

    uid = getattr(account, "pw_uid", None)
    gid = getattr(account, "pw_gid", None)
    name = getattr(account, "pw_name", None)
    home = getattr(account, "pw_dir", None)
    shell = getattr(account, "pw_shell", None)
    group_name = getattr(runtime_group, "gr_name", None)
    group_gid = getattr(runtime_group, "gr_gid", None)
    group_members = getattr(runtime_group, "gr_mem", None)
    if (
        name != "brilliant-vc"
        or isinstance(uid, bool)
        or not isinstance(uid, int)
        or uid <= 0
        or isinstance(gid, bool)
        or not isinstance(gid, int)
        or gid <= 0
    ):
        raise LauncherPreflightError("dedicated runtime account must be non-root")
    if group_name != name or group_gid != gid or not isinstance(group_members, list):
        raise LauncherPreflightError("runtime account must use its same-name dedicated group")
    if home != _RUNTIME_HOME or shell != _RUNTIME_SHELL:
        raise LauncherPreflightError("runtime account must use /nonexistent and /usr/sbin/nologin")
    if Path(home).exists() or Path(home).is_symlink():
        raise LauncherPreflightError("runtime account home must remain nonexistent")
    uid_accounts = [entry for entry in all_accounts if getattr(entry, "pw_uid", None) == uid]
    if (
        len(uid_accounts) != 1
        or getattr(uid_accounts[0], "pw_name", None) != name
        or getattr(uid_accounts[0], "pw_gid", None) != gid
        or getattr(uid_accounts[0], "pw_dir", None) != home
        or getattr(uid_accounts[0], "pw_shell", None) != shell
    ):
        raise LauncherPreflightError("runtime UID must map to exactly one account")
    primary_accounts = [entry for entry in all_accounts if getattr(entry, "pw_gid", None) == gid]
    if (
        len(primary_accounts) != 1
        or getattr(primary_accounts[0], "pw_name", None) != name
        or getattr(primary_accounts[0], "pw_uid", None) != uid
        or getattr(primary_accounts[0], "pw_dir", None) != home
        or getattr(primary_accounts[0], "pw_shell", None) != shell
        or set(group_members) - {name}
    ):
        raise LauncherPreflightError("runtime group must not include another account")
    primary_group_entries = [entry for entry in all_groups if getattr(entry, "gr_gid", None) == gid]
    if (
        len(primary_group_entries) != 1
        or getattr(primary_group_entries[0], "gr_name", None) != name
        or set(getattr(primary_group_entries[0], "gr_mem", ())) - {name}
    ):
        raise LauncherPreflightError("runtime GID must map to exactly one dedicated group")
    supplementary_groups = {
        getattr(entry, "gr_name", None)
        for entry in all_groups
        if getattr(entry, "gr_gid", None) != gid and name in set(getattr(entry, "gr_mem", ()))
    }
    if supplementary_groups:
        raise LauncherPreflightError("runtime account must not have supplementary groups")
    _validate_locked_shadow_entry(
        shadow_path,
        username=name,
        required_uid=required_uid,
    )


def _validate_locked_shadow_entry(
    path: Path,
    *,
    username: str,
    required_uid: int,
) -> None:
    """Require one locked password entry without following or racing the file."""

    try:
        before = path.lstat()
    except OSError:
        raise LauncherPreflightError("could not inspect the account password database") from None
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise LauncherPreflightError("account password database must be a regular non-symlink file")
    if before.st_uid != required_uid or stat.S_IMODE(before.st_mode) not in {0o600, 0o640}:
        raise LauncherPreflightError("account password database ownership or mode is unsafe")
    if before.st_nlink != 1 or not 0 < before.st_size <= _MAX_SHADOW_BYTES:
        raise LauncherPreflightError("account password database has an invalid link count or size")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        raise LauncherPreflightError(
            "could not safely open the account password database"
        ) from None
    buffer = bytearray(8192)
    line = bytearray()
    match_count = 0
    locked = False
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise LauncherPreflightError("account password database changed during open")
        prefix = f"{username}:".encode("ascii")
        while True:
            count = os.readv(descriptor, [buffer])
            if count == 0:
                break
            view = memoryview(buffer)[:count]
            try:
                for value in view:
                    if value == 0x0A:
                        matched, line_locked = _shadow_line_state(line, prefix)
                        match_count += int(matched)
                        locked = locked or line_locked
                        _wipe(line)
                        line.clear()
                    else:
                        line.append(value)
                        if len(line) > _MAX_METADATA_BYTES:
                            raise LauncherPreflightError(
                                "account password database contains an oversized entry"
                            )
            finally:
                view.release()
                _wipe(buffer)
        if line:
            matched, line_locked = _shadow_line_state(line, prefix)
            match_count += int(matched)
            locked = locked or line_locked
        after = os.fstat(descriptor)
        if (
            after.st_size != opened.st_size
            or after.st_mtime_ns != opened.st_mtime_ns
            or after.st_ctime_ns != opened.st_ctime_ns
        ):
            raise LauncherPreflightError("account password database changed while reading")
    finally:
        _wipe(line)
        _wipe(buffer)
        os.close(descriptor)
    if match_count != 1 or not locked:
        raise LauncherPreflightError("runtime account password must be locked")


def _shadow_line_state(line: bytearray, prefix: bytes) -> tuple[bool, bool]:
    """Inspect only the target entry's first password byte without copying it."""

    if len(line) <= len(prefix) or any(line[index] != value for index, value in enumerate(prefix)):
        return False, False
    password_start = len(prefix)
    password_end = password_start
    while password_end < len(line) and line[password_end] != 0x3A:
        password_end += 1
    return True, (password_end > password_start and line[password_start] in (ord("!"), ord("*")))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--firmware-snapshot", type=Path, required=True)
    parser.add_argument(
        "--private-root",
        type=Path,
        default=Path("/data/brilliant-vc-private"),
    )
    parser.add_argument("--persistent-root", type=Path, default=Path("/data/brilliant-vc"))
    parser.add_argument(
        "--identity-dir",
        type=Path,
        default=Path("/data/brilliant-vc-private/identity"),
    )
    parser.add_argument(
        "--materialized-certificate-dir",
        type=Path,
        default=Path("/data/brilliant-vc-private/materialized-certificates"),
    )
    parser.add_argument(
        "--runtime-credential-dir",
        type=Path,
        default=Path("/data/brilliant-vc-credentials"),
    )
    parser.add_argument(
        "--bootstrap-path",
        type=Path,
        default=Path("/data/brilliant-vc-credentials/bootstrap"),
    )
    parser.add_argument("--state-dir", type=Path, default=Path("/data/brilliant-vc/state"))
    parser.add_argument(
        "--certificate-dir",
        type=Path,
        default=Path("/data/brilliant-vc-credentials/certificates"),
    )
    parser.add_argument(
        "--process-config-dir",
        type=Path,
        default=Path("/data/brilliant-vc/process-config"),
    )
    parser.add_argument(
        "--process-flagfile-dir",
        type=Path,
        default=Path("/data/brilliant-vc/flagfiles"),
    )
    parser.add_argument(
        "--startable-config-dir",
        type=Path,
        default=Path("/data/brilliant-vc/startable-configs"),
    )
    parser.add_argument("--log-dir", type=Path, default=Path("/data/brilliant-vc/logs"))
    parser.add_argument(
        "--error-log-dir",
        type=Path,
        default=Path("/data/brilliant-vc/errors"),
    )
    parser.add_argument("--trace-dir", type=Path, default=Path("/data/brilliant-vc/traces"))
    parser.add_argument("--runtime-dir", type=Path, default=Path("/run/brilliant-vc"))
    parser.add_argument("--socket-path", type=Path, default=Path("/run/brilliant-vc/server_socket"))
    parser.add_argument(
        "--stats-socket-path",
        type=Path,
        default=Path("/run/brilliant-vc/uwsgi_stats_socket"),
    )
    parser.add_argument(
        "--release-info-path",
        type=Path,
        default=Path("/etc/release_info.json"),
    )
    parser.add_argument(
        "--tracking-branch-path",
        type=Path,
        default=Path("/var/lib/update_manager/tracking_branch"),
    )
    parser.add_argument("--runtime-user", default="brilliant-vc")
    args = parser.parse_args(argv)
    required_uid = os.geteuid()
    if required_uid != 0:
        raise LauncherPreflightError("launcher preflight must run as root")
    if args.runtime_user != "brilliant-vc":
        raise LauncherPreflightError("runtime user must match the pinned dedicated account")
    try:
        runtime_account = pwd.getpwnam(args.runtime_user)
    except KeyError:
        raise LauncherPreflightError("dedicated runtime account does not exist") from None
    try:
        runtime_group = grp.getgrgid(runtime_account.pw_gid)
    except KeyError:
        raise LauncherPreflightError("dedicated runtime group does not exist") from None
    _validate_runtime_account_contract(
        runtime_account,
        runtime_group,
        all_accounts=pwd.getpwall(),
        all_groups=grp.getgrall(),
        shadow_path=Path("/etc/shadow"),
        required_uid=required_uid,
    )
    snapshot = load_firmware_snapshot(
        args.firmware_snapshot,
        required_uid=required_uid,
    )
    actual_module_hashes = hash_firmware_modules(required_uid=required_uid)
    plan = preflight_no_start(
        LauncherPaths(
            private_root=args.private_root,
            persistent_root=args.persistent_root,
            identity_dir=args.identity_dir,
            materialized_certificate_dir=args.materialized_certificate_dir,
            runtime_credential_dir=args.runtime_credential_dir,
            bootstrap_path=args.bootstrap_path,
            state_dir=args.state_dir,
            certificate_dir=args.certificate_dir,
            process_config_dir=args.process_config_dir,
            process_flagfile_dir=args.process_flagfile_dir,
            startable_config_dir=args.startable_config_dir,
            log_dir=args.log_dir,
            error_log_dir=args.error_log_dir,
            trace_dir=args.trace_dir,
            runtime_dir=args.runtime_dir,
            socket_path=args.socket_path,
            stats_socket_path=args.stats_socket_path,
            release_info_path=args.release_info_path,
            tracking_branch_path=args.tracking_branch_path,
            art_preload_dir=Path("/etc/brilliant/content_preload/art_library"),
        ),
        snapshot,
        actual_module_hashes=actual_module_hashes,
        required_uid=required_uid,
        runtime_uid=runtime_account.pw_uid,
        runtime_gid=runtime_account.pw_gid,
    )
    print(json.dumps(plan.to_public_dict(), sort_keys=True))
    return 0 if plan.start_permitted else 2


if __name__ == "__main__":
    try:
        sys.exit(main())
    except LauncherPreflightError as exc:
        print(f"VC launcher preflight blocked: {exc}", file=sys.stderr)
        sys.exit(2)
