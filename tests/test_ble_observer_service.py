"""Contract tests for the isolated, read-only BLE observer systemd unit."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).parents[1]
UNIT_PATH = ROOT / "deploy" / "brilliant-ble-observer.service"


def _settings() -> dict[str, str]:
    settings: dict[str, str] = {}
    for raw_line in UNIT_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith(("#", "[")) or "=" not in line:
            continue
        key, value = line.split("=", 1)
        settings[key] = value
    return settings


def test_observer_has_a_separate_resource_capped_unit() -> None:
    settings = _settings()

    assert settings["Description"] == "Brilliant read-only BLE advertisement observer"
    assert settings["After"].split() == ["bluetooth.target", "network-online.target"]
    assert settings["EnvironmentFile"] == "/etc/brilliant-mqtt.env"
    assert "PYTHONDONTWRITEBYTECODE=1" in settings["Environment"]
    assert settings["WorkingDirectory"] == "/var/brilliant-mqtt/ble_observer"
    assert settings["ExecStart"].endswith(" -m brilliant_ble_observer")
    assert settings["Restart"] == "on-failure"
    assert int(settings["RestartSec"]) >= 5
    assert settings["Nice"] == "10"
    assert settings["MemoryMax"] == "48M"
    assert settings["CPUQuota"] == "10%"


def test_observer_unit_is_read_only_and_has_no_adapter_management_capabilities() -> None:
    settings = _settings()
    unit = UNIT_PATH.read_text(encoding="utf-8")

    assert settings["NoNewPrivileges"] == "true"
    assert settings["ProtectSystem"] == "strict"
    assert settings["ProtectHome"] == "true"
    assert settings["PrivateDevices"] == "true"
    assert settings["PrivateTmp"] == "true"
    assert settings["DevicePolicy"] == "closed"
    assert settings["CapabilityBoundingSet"] == ""
    assert settings["AmbientCapabilities"] == ""
    assert settings["RestrictAddressFamilies"].split() == ["AF_UNIX", "AF_INET", "AF_INET6"]
    assert "PrivateNetwork=true" not in unit
    assert "ReadWritePaths=" not in unit
    assert "ExecStartPre=" not in unit
    for forbidden in (
        "bluetoothctl",
        "bluetoothd",
        "systemctl",
        "StartDiscovery",
        "StopDiscovery",
        "CAP_NET_ADMIN",
        "CAP_NET_RAW",
        "/sys/",
    ):
        assert forbidden not in unit
