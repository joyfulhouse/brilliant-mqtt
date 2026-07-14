from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools.brilliant_vc.launcher_preflight import LauncherPaths
from tools.brilliant_vc.vassal_manifest import ManifestError, build_candidate_manifest


def _paths() -> LauncherPaths:
    persistent = Path("/data/brilliant-vc")
    runtime = Path("/run/brilliant-vc")
    return LauncherPaths(
        persistent_root=persistent,
        identity_dir=persistent / "identity",
        state_dir=persistent / "state",
        certificate_dir=persistent / "certificates",
        process_config_dir=persistent / "processes",
        process_flagfile_dir=persistent / "flagfiles",
        startable_config_dir=persistent / "startable-configs",
        log_dir=persistent / "logs",
        error_log_dir=persistent / "errors",
        trace_dir=persistent / "traces",
        runtime_dir=runtime,
        socket_path=runtime / "server_socket",
        stats_socket_path=runtime / "uwsgi_stats_socket",
        release_info_path=Path("/etc/release_info.json"),
        tracking_branch_path=Path("/var/lib/update_manager/tracking_branch"),
    )


def test_candidate_manifest_pins_stock_lifecycle_without_a_start_primitive() -> None:
    manifest = build_candidate_manifest(_paths())
    public = manifest.to_public_dict()

    assert public["schema_version"] == 1
    assert public["firmware_version"] == "v26.06.03.1"
    assert public["runtime_user"] == "brilliant-vc"
    assert public["initial_vassals"] == ["message_bus"]
    assert public["process_manager_generated_vassals"] == [
        "discovery_peripheral",
        "config_peripherals",
        "bootstrap",
    ]
    assert public["enabled_processes"] == [
        "message_bus",
        "discovery_peripheral",
        "config_peripherals",
        "bootstrap",
    ]
    disabled = public["disabled_processes"]
    assert isinstance(disabled, list)
    assert len(disabled) == 34
    assert len(set(disabled) | set(public["enabled_processes"])) == 38
    assert not set(disabled) & set(public["enabled_processes"])
    assert public["embedded_startables"] == {
        "message_bus": ["remote_bridge"],
        "config_peripherals": [
            "art_config_peripheral",
            "device_config_peripheral",
            "motion_detection_config_peripheral",
            "alarm_config_peripheral",
        ],
    }
    assert public["device_configuration_candidate"] == {
        "peripheral_id": "device_config_peripheral",
        "peripheral_type": 19,
        "live_validated": False,
    }
    assert public["start_permitted"] is False
    assert public["contains_start_primitive"] is False
    assert not hasattr(manifest, "command")
    assert not hasattr(manifest, "argv")
    assert not hasattr(manifest, "apply")

    rendered = json.dumps(public, sort_keys=True)
    assert "<private:device_id>" in rendered
    assert "a" * 32 not in rendered


def test_candidate_manifest_uses_local_socket_derivation_and_isolates_every_path() -> None:
    public = build_candidate_manifest(_paths()).to_public_dict()
    flags = public["flags"]
    assert isinstance(flags, dict)

    assert flags["message_bus_server_socket_path"] == "/run/brilliant-vc/server_socket"
    assert flags["message_bus_address_override"] is None
    assert public["derived_local_client_address"] == (
        "unix://%2Frun%2Fbrilliant-vc%2Fserver_socket"
    )
    assert flags["process_configs_dir"] == "/data/brilliant-vc/processes"
    assert flags["process_flagfiles_dir"] == "/data/brilliant-vc/flagfiles"
    assert flags["startable_host_configs_dir"] == "/data/brilliant-vc/startable-configs"
    assert flags["mb_state_dir"] == "/data/brilliant-vc/state"
    assert flags["cert_dir"] == "/data/brilliant-vc/certificates"
    assert flags["log_output_directory"] == "/data/brilliant-vc/logs"
    assert flags["error_log_storage_dir"] == "/data/brilliant-vc/errors"
    assert flags["trace_dir"] == "/data/brilliant-vc/traces"
    assert flags["uwsgi_stats_socket_path"] == "/run/brilliant-vc/uwsgi_stats_socket"
    assert flags["saved_bootstrap_parameters_path"] == "/data/brilliant-vc/identity/bootstrap"
    assert flags["release_info_filepath"] == "/etc/release_info.json"
    assert flags["tracking_branch_filepath"] == "/var/lib/update_manager/tracking_branch"
    assert flags["remote_bridge_listen_port"] == 15455
    assert flags["remote_bridge_enable_bluetooth_provisioning"] is False
    assert flags["remote_bridge_enforce_strict_authentication"] is True
    assert flags["remote_bridge_device_provisioning_ip_listen_port"] == 0
    assert flags["ble_mesh_debug_interface_listen_port"] == 0
    assert flags["stub_ble_peripheral"] is True
    assert flags["start_as_virtual_control"] is True
    assert flags["home_id"] == "0"
    assert flags["device_id"] == "<private:device_id>"


def test_candidate_manifest_keeps_live_blockers_explicit() -> None:
    public = build_candidate_manifest(_paths()).to_public_dict()

    assert public["blockers"] == [
        "runtime_user_credential_handoff_unresolved",
        "arm_hardware_bootstrap_validation_required",
        "config_peripherals_live_validation_required",
        "remote_bridge_stub_live_validation_required",
        "supported_virtual_control_removal_unconfirmed",
    ]


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("socket_path", Path("/var/run/brilliant/server_socket"), "physical"),
        ("stats_socket_path", Path("/run/brilliant-vc/server_socket"), "distinct"),
    ],
)
def test_manifest_rejects_physical_or_colliding_runtime_paths(
    field: str, value: Path, message: str
) -> None:
    paths = _paths()
    values = {name: getattr(paths, name) for name in paths.__dataclass_fields__}
    values[field] = value

    with pytest.raises(ManifestError, match=message):
        build_candidate_manifest(LauncherPaths(**values))


@pytest.mark.parametrize("port", [0, 5455, 65536, True])
def test_manifest_rejects_unsafe_remote_bridge_port(port: object) -> None:
    with pytest.raises(ManifestError, match="remote-bridge port"):
        build_candidate_manifest(_paths(), remote_bridge_port=port)  # type: ignore[arg-type]
