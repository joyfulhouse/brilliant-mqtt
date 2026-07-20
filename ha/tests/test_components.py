from pathlib import Path

import pytest
from homeassistant.core import HomeAssistant

from custom_components.brilliant_mqtt import components as comp
from custom_components.brilliant_mqtt import const, panel_ops
from custom_components.brilliant_mqtt.component_payloads import (
    SINGLE_UNIT_PAYLOAD_BY_ID,
    SINGLE_UNIT_PAYLOAD_SPECS,
)
from custom_components.brilliant_mqtt.const import (
    COMPONENT_BLE_OBSERVER,
    COMPONENT_BRIDGE,
    COMPONENT_BUS_WATCHDOG,
    COMPONENT_HA_MIRROR,
    COMPONENT_HUE_CA,
    COMPONENT_VOICE,
    COMPONENT_WIFI_WATCHDOG,
    CONF_BLE_OBSERVER_ALLOWLIST_JSON,
    CONF_COMPONENTS,
    CONF_HOST,
    CONF_MESH_PRIORITY,
    CONF_MQTT_HOST,
    CONF_MQTT_PASSWORD,
    CONF_MQTT_PORT,
    CONF_MQTT_USERNAME,
    CONF_PANEL,
    CONF_ROOT_PASSWORD,
    PANEL_ENV_FILE,
)
from custom_components.brilliant_mqtt.shell import RunResult
from tests.fakes import FakeShell


def test_component_id_constants() -> None:
    assert const.CONF_COMPONENTS == "components"
    assert const.COMPONENT_BRIDGE == "bridge"
    assert const.COMPONENT_VOICE == "voice"
    assert const.COMPONENT_HA_MIRROR == "ha_mirror"
    assert const.COMPONENT_WIFI_WATCHDOG == "wifi_watchdog"
    assert const.COMPONENT_BUS_WATCHDOG == "bus_watchdog"
    assert const.COMPONENT_BLE_OBSERVER == "ble_observer"


def test_single_unit_payload_metadata_is_one_immutable_registry() -> None:
    """Install and relay paths share one drift-resistant unit/payload operation spec."""
    expected = (
        COMPONENT_WIFI_WATCHDOG,
        COMPONENT_BUS_WATCHDOG,
        COMPONENT_BLE_OBSERVER,
    )
    assert tuple(spec.component_id for spec in SINGLE_UNIT_PAYLOAD_SPECS) == expected
    assert tuple(SINGLE_UNIT_PAYLOAD_BY_ID) == expected
    for spec in SINGLE_UNIT_PAYLOAD_SPECS:
        row = comp.REGISTRY[spec.component_id]
        assert row.label == spec.label
        assert spec.service_filename.endswith(".service")
        assert spec.payload_subdir == spec.component_id


def test_registry_has_bridge_and_voice() -> None:
    assert comp.REGISTRY[COMPONENT_BRIDGE].locked is True
    assert comp.REGISTRY[COMPONENT_VOICE].locked is False
    assert comp.REGISTRY[COMPONENT_VOICE].default_enabled is False


def test_default_components_bridge_on_voice_off() -> None:
    d = comp.default_components()
    assert d[COMPONENT_BRIDGE] is True
    assert d[COMPONENT_VOICE] is False


def test_selected_ids_includes_bridge_always() -> None:
    assert comp.selected_ids({}) == [COMPONENT_BRIDGE]
    sel = comp.selected_ids({CONF_COMPONENTS: {COMPONENT_VOICE: True}})
    assert COMPONENT_BRIDGE in sel and COMPONENT_VOICE in sel


def test_wifi_watchdog_registry_row_default_enabled() -> None:
    row = comp.REGISTRY[COMPONENT_WIFI_WATCHDOG]
    assert row.default_enabled is True
    assert row.locked is False


def test_default_components_wifi_watchdog_on() -> None:
    d = comp.default_components()
    assert d[COMPONENT_WIFI_WATCHDOG] is True


def test_bus_watchdog_registry_row_default_enabled() -> None:
    row = comp.REGISTRY[COMPONENT_BUS_WATCHDOG]
    assert row.default_enabled is True
    assert row.locked is False


def test_default_components_bus_watchdog_on() -> None:
    d = comp.default_components()
    assert d[COMPONENT_BUS_WATCHDOG] is True


async def test_ble_observer_registry_row_is_optional_and_default_off() -> None:
    row = comp.REGISTRY[COMPONENT_BLE_OBSERVER]
    assert row.label == "BLE observer"
    assert row.locked is False
    assert row.default_enabled is False
    shell = FakeShell(
        responses={
            panel_ops.BLE_OBSERVER_INSPECT_COMMAND: RunResult(
                0,
                "unit=1\nenabled=1\nactive=1\nsunit=1\npayload=1\n",
                "",
            )
        }
    )
    await shell.connect()
    assert await row.present(shell) is True
    assert row.remove is panel_ops.uninstall_ble_observer
    assert comp.default_components()[COMPONENT_BLE_OBSERVER] is False


def test_ble_observer_is_selected_only_by_explicit_opt_in() -> None:
    assert COMPONENT_BLE_OBSERVER not in comp.selected_ids({})
    selected = comp.selected_ids({CONF_COMPONENTS: {COMPONENT_BLE_OBSERVER: True}})
    assert selected == [COMPONENT_BRIDGE, COMPONENT_BLE_OBSERVER]


async def test_ble_observer_install_deploys_configures_then_starts_only_observer(
    hass: HomeAssistant,
    payload_dir: Path,
) -> None:
    existing = panel_ops.render_env(
        panel="office",
        mesh_priority=1,
        mqtt_host="broker",
        mqtt_port=1883,
        mqtt_username="user",
        mqtt_password="private",
    )
    shell = FakeShell(
        responses={
            f"cat {PANEL_ENV_FILE}": RunResult(0, existing, ""),
            panel_ops.BLE_OBSERVER_INSPECT_COMMAND: RunResult(
                0,
                "unit=1\nenabled=1\nactive=1\nsunit=1\npayload=1\n",
                "",
            ),
        }
    )
    await shell.connect()
    allowlist = '[{"address":"AA:BB:CC:DD:EE:FF"}]'
    data = {
        CONF_PANEL: "office",
        CONF_HOST: "panel",
        CONF_ROOT_PASSWORD: "root",
        CONF_MESH_PRIORITY: 1,
        CONF_MQTT_HOST: "broker",
        CONF_MQTT_PORT: 1883,
        CONF_MQTT_USERNAME: "user",
        CONF_MQTT_PASSWORD: "private",
        CONF_BLE_OBSERVER_ALLOWLIST_JSON: allowlist,
    }

    await comp.REGISTRY[COMPONENT_BLE_OBSERVER].install(hass, shell, data)

    assert shell.dir_uploads == [
        (str(payload_dir) + "/ble_observer", "/var/brilliant-mqtt/ble_observer.staging")
    ]
    observer_enable = f"systemctl enable --now {const.BLE_OBSERVER_SERVICE_NAME}"
    assert observer_enable in shell.commands
    assert shell.commands[-1] == panel_ops.BLE_OBSERVER_INSPECT_COMMAND
    assert not any(
        forbidden in command
        for command in shell.commands
        for forbidden in ("systemctl restart brilliant-mqtt", "bluetoothctl", "bluetoothd")
    )
    written = panel_ops.parse_env(
        next(data for path, data, _mode in shell.uploads if path == PANEL_ENV_FILE).decode()
    )
    assert written["BLE_OBSERVER_ENABLED"] == "1"
    assert written["BLE_OBSERVER_ALLOWLIST_JSON"] == allowlist


def test_ha_mirror_registry_row_is_deprecated_but_keeps_removal_recipe() -> None:
    row = comp.REGISTRY[COMPONENT_HA_MIRROR]
    assert row.id == COMPONENT_HA_MIRROR
    assert row.label == "HA mirror"
    assert row.default_enabled is False
    assert row.locked is False
    assert row.deprecated is True
    assert row.remove is panel_ops.uninstall_ha_mirror


def test_default_components_excludes_deprecated_ha_mirror() -> None:
    assert COMPONENT_HA_MIRROR not in comp.default_components()


def test_optional_order_hides_deprecated_ha_mirror() -> None:
    opts = comp.optional()
    ids = [c.id for c in opts]
    assert ids == [
        COMPONENT_VOICE,
        COMPONENT_WIFI_WATCHDOG,
        COMPONENT_BUS_WATCHDOG,
        COMPONENT_HUE_CA,
        COMPONENT_BLE_OBSERVER,
    ]


async def test_deprecated_ha_mirror_install_always_fails_closed(
    hass: HomeAssistant,
) -> None:
    shell = FakeShell()
    await shell.connect()
    with pytest.raises(panel_ops.PanelOpError, match="deprecated"):
        await comp.REGISTRY[COMPONENT_HA_MIRROR].install(hass, shell, {})
    assert not shell.commands
    assert not shell.uploads
    assert not shell.dir_uploads


def test_selected_ids_never_selects_deprecated_ha_mirror() -> None:
    selected = comp.selected_ids(
        {CONF_COMPONENTS: {COMPONENT_BRIDGE: True, COMPONENT_HA_MIRROR: True}}
    )
    assert selected == [COMPONENT_BRIDGE]
