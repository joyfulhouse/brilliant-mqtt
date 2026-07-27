"""Tests for const.py — constants and contracts."""

from custom_components.brilliant_mqtt import const


def test_wifi_watchdog_on_panel_paths() -> None:
    """Test Wi-Fi watchdog on-panel path constants exist with exact values."""
    assert const.PANEL_WIFI_WATCHDOG_DIR == "/var/brilliant-mqtt/wifi_watchdog"
    unit_file = "/etc/systemd/system/brilliant-wifi-watchdog.service"
    assert const.PANEL_WIFI_WATCHDOG_UNIT_FILE == unit_file
    assert const.WIFI_WATCHDOG_SERVICE_NAME == "brilliant-wifi-watchdog"


def test_fleet_storage_contract_constants_are_stable() -> None:
    """Fleet/subentry discriminators and compatibility version are persistent API."""
    assert const.CONFIG_ENTRY_VERSION == 4
    assert const.ENTRY_KIND_FLEET == "fleet"
    assert const.ENTRY_KIND_LEGACY_PENDING_CONSOLIDATION == "legacy_pending_consolidation"
    assert const.SUBENTRY_TYPE_PANEL == "panel"
    assert const.FLEET_UNIQUE_ID == "brilliant_mqtt_fleet"


def test_fleet_release_layout_is_separate_from_legacy_fixed_paths() -> None:
    """Fleet activation uses a versioned release root and one atomic current link."""
    assert const.PANEL_RELEASES_DIR == "/var/brilliant-mqtt/releases"
    assert const.PANEL_CURRENT_LINK == "/var/brilliant-mqtt/current"
    assert const.PANEL_APP_DIR == "/var/brilliant-mqtt/app"
    assert const.PANEL_VENDOR_DIR == "/var/brilliant-mqtt/vendor"
    assert const.PANEL_WIFI_WATCHDOG_DIR == "/var/brilliant-mqtt/wifi_watchdog"
    assert const.PANEL_BUS_WATCHDOG_DIR == "/var/brilliant-mqtt/bus_watchdog"
