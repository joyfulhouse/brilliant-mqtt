"""Data-only candidate manifest for an isolated Brilliant Virtual Control.

The manifest records the pinned process topology and isolated flag surface. It
does not read identity files, import firmware, write configuration, build an
executable command, open a socket, or start a process. Unresolved live gates
remain explicit and ``start_permitted`` is always false.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

from tools.brilliant_vc.launcher_preflight import LauncherPaths

_SCHEMA_VERSION = 2
_PINNED_FIRMWARE = "v26.06.03.1"
_RUNTIME_USER = "brilliant-vc"
_PHYSICAL_SOCKET = Path("/var/run/brilliant/server_socket")
_PHYSICAL_REMOTE_BRIDGE_PORT = 5455
_DEVICE_ID_PLACEHOLDER = "<private:device_id>"

_KNOWN_PROCESSES = (
    "message_bus",
    "discovery_peripheral",
    "object_store_peripheral",
    "gangbox_peripherals",
    "faceplate_peripheral",
    "voice",
    "art",
    "config_peripherals",
    "control_notification",
    "execution",
    "hardware",
    "homekit",
    "wifi",
    "analytics",
    "bootstrap",
    "nest_peripherals",
    "honeywell_peripherals",
    "honeywell_tc2_peripherals",
    "hunter_douglas_peripherals",
    "ecobee_peripherals",
    "enphase_peripherals",
    "schlage_peripherals",
    "ring_peripherals",
    "wemo_peripherals",
    "hue_bridge_peripherals",
    "sonos_peripherals",
    "smartthings_peripherals",
    "monitor",
    "somfy_peripherals",
    "august_peripherals",
    "lifx_peripherals",
    "tplink_peripherals",
    "butterflymx_peripherals",
    "genie_peripherals",
    "spectrum_brands_peripherals",
    "brilliant_virtual_device_peripherals",
    "remotelock_peripherals",
    "bluesound_peripherals",
)
_ENABLED_PROCESSES = (
    "message_bus",
    "discovery_peripheral",
    "config_peripherals",
    "bootstrap",
)
_PROCESS_MANAGER_GENERATED = (
    "discovery_peripheral",
    "config_peripherals",
    "bootstrap",
)
_DISABLED_PROCESSES = tuple(
    process for process in _KNOWN_PROCESSES if process not in _ENABLED_PROCESSES
)
_EMBEDDED_STARTABLES = {
    "message_bus": ("remote_bridge",),
    "config_peripherals": (
        "art_config_peripheral",
        "device_config_peripheral",
        "motion_detection_config_peripheral",
        "alarm_config_peripheral",
    ),
}
_BLOCKERS = (
    "runtime_credential_handoff_not_applied",
    "runtime_preparation_not_applied",
    "nonroot_service_install_and_compatibility_validation_required",
    "fresh_bootstrap_start_approval_required",
    "coordinated_session_unit_and_approval_not_implemented",
    "arm_hardware_bootstrap_validation_required",
    "config_peripherals_live_validation_required",
    "remote_bridge_stub_live_validation_required",
    "supported_virtual_control_removal_unconfirmed",
)


class ManifestError(ValueError):
    """Raised when a data-only candidate would collide with the physical runtime."""


@dataclass(frozen=True, slots=True)
class CandidateManifest:
    """Redacted candidate topology with no command, apply, or start capability."""

    paths: LauncherPaths
    remote_bridge_port: int

    def to_public_dict(self) -> dict[str, object]:
        """Render only deterministic, non-secret candidate data."""

        paths = self.paths
        flags: dict[str, object] = {
            "device_id": _DEVICE_ID_PLACEHOLDER,
            "home_id": "0",
            "start_as_virtual_control": True,
            "mb_state_dir": str(paths.state_dir),
            "cert_dir": str(paths.certificate_dir),
            "process_configs_dir": str(paths.process_config_dir),
            "process_flagfiles_dir": str(paths.process_flagfile_dir),
            "startable_host_configs_dir": str(paths.startable_config_dir),
            "message_bus_server_socket_path": str(paths.socket_path),
            # Co-located vassals derive a percent-encoded UNIX URL from the
            # server socket. A global override would also make RemoteBridge
            # dial its own message bus and is intentionally absent.
            "message_bus_address_override": None,
            "saved_bootstrap_parameters_path": str(paths.bootstrap_path),
            "release_info_filepath": str(paths.release_info_path),
            "tracking_branch_filepath": str(paths.tracking_branch_path),
            "uwsgi_stats_socket_path": str(paths.stats_socket_path),
            "log_output_directory": str(paths.log_dir),
            "error_log_storage_dir": str(paths.error_log_dir),
            "trace_dir": str(paths.trace_dir),
            "art_preload_dir": str(paths.art_preload_dir),
            "message_bus_unprivileged_user": _RUNTIME_USER,
            "disable_peripherals": list(_DISABLED_PROCESSES),
            "bootstrap_max_provisioning_attempts_per_code": 1,
            "bootstrap_web_api_homes_endpoint": "/homes",
            "stub_bootstrap": False,
            "remote_bridge_listen_port": self.remote_bridge_port,
            "remote_bridge_enable_bluetooth_provisioning": False,
            "remote_bridge_enforce_strict_authentication": True,
            "remote_bridge_device_provisioning_ip_listen_port": 0,
            "ble_mesh_debug_interface_listen_port": 0,
            "stub_ble_peripheral": True,
            "discovery_peripheral_enable_remote_bridge_service_discovery": True,
        }
        return {
            "schema_version": _SCHEMA_VERSION,
            "firmware_version": _PINNED_FIRMWARE,
            "candidate_only": True,
            "runtime_user": _RUNTIME_USER,
            "supervisor": {
                "mode": "dedicated_nonroot_emperor",
                "runtime_user": _RUNTIME_USER,
                "runs_as_root": False,
                "vassals_use_same_identity": True,
                "generated_directory_mode": "0700",
                "credential_access": "root_owner_dedicated_group_read_only",
            },
            "initial_vassals": ["message_bus"],
            "process_manager_generated_vassals": list(_PROCESS_MANAGER_GENERATED),
            "enabled_processes": list(_ENABLED_PROCESSES),
            "disabled_processes": list(_DISABLED_PROCESSES),
            "embedded_startables": {
                process: list(startables) for process, startables in _EMBEDDED_STARTABLES.items()
            },
            "device_configuration_candidate": {
                "peripheral_id": "device_config_peripheral",
                "peripheral_type": 19,
                "live_validated": False,
            },
            "derived_local_client_address": (f"unix://{quote(str(paths.socket_path), safe='')}"),
            "flags": flags,
            "blockers": list(_BLOCKERS),
            "contains_start_primitive": False,
            "start_permitted": False,
        }


def build_candidate_manifest(
    paths: LauncherPaths,
    *,
    remote_bridge_port: int = 15455,
) -> CandidateManifest:
    """Build a redacted candidate after collision-only validation."""

    socket = paths.socket_path.resolve(strict=False)
    stats_socket = paths.stats_socket_path.resolve(strict=False)
    runtime = paths.runtime_dir.resolve(strict=False)
    credential_root = paths.runtime_credential_dir.resolve(strict=False)
    if socket == _PHYSICAL_SOCKET.resolve(strict=False):
        raise ManifestError("refusing the physical Control message-bus socket")
    if socket == stats_socket:
        raise ManifestError("message-bus and stats sockets must be distinct")
    if socket.parent != runtime or stats_socket.parent != runtime:
        raise ManifestError("candidate sockets must be direct children of the runtime directory")
    if (
        paths.bootstrap_path.resolve(strict=False) != credential_root / "bootstrap"
        or paths.certificate_dir.resolve(strict=False) != credential_root / "certificates"
    ):
        raise ManifestError("runtime credential paths must use the canonical isolated layout")
    if (
        isinstance(remote_bridge_port, bool)
        or not isinstance(remote_bridge_port, int)
        or not 1024 <= remote_bridge_port <= 65535
        or remote_bridge_port == _PHYSICAL_REMOTE_BRIDGE_PORT
    ):
        raise ManifestError("remote-bridge port must be non-default and between 1024 and 65535")
    return CandidateManifest(paths=paths, remote_bridge_port=remote_bridge_port)
