"""Tests for const.py — constants and contracts."""

from custom_components.brilliant_mqtt import const


def test_wifi_watchdog_on_panel_paths() -> None:
    """Test Wi-Fi watchdog on-panel path constants exist with exact values."""
    assert const.PANEL_WIFI_WATCHDOG_DIR == "/var/brilliant-mqtt/wifi_watchdog"
    unit_file = "/etc/systemd/system/brilliant-wifi-watchdog.service"
    assert const.PANEL_WIFI_WATCHDOG_UNIT_FILE == unit_file
    assert const.WIFI_WATCHDOG_SERVICE_NAME == "brilliant-wifi-watchdog"
