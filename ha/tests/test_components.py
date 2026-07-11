from pathlib import Path
from unittest.mock import AsyncMock, patch

from homeassistant.core import HomeAssistant

from custom_components.brilliant_mqtt import components as comp
from custom_components.brilliant_mqtt import const, panel_ops
from custom_components.brilliant_mqtt.const import (
    COMPONENT_BRIDGE,
    COMPONENT_BUS_WATCHDOG,
    COMPONENT_HA_MIRROR,
    COMPONENT_VOICE,
    COMPONENT_WIFI_WATCHDOG,
    CONF_COMPONENTS,
    CONF_HA_MIRROR_LABEL,
    CONF_HA_MIRROR_LEADER_PRIORITY,
    CONF_HA_MIRROR_TOKEN,
    CONF_HA_MIRROR_WS_URL,
    CONF_MQTT_HOST,
    CONF_MQTT_PASSWORD,
    CONF_MQTT_PORT,
    CONF_MQTT_USERNAME,
    CONF_PANEL,
)
from tests.fakes import FakeShell


def test_component_id_constants() -> None:
    assert const.CONF_COMPONENTS == "components"
    assert const.COMPONENT_BRIDGE == "bridge"
    assert const.COMPONENT_VOICE == "voice"
    assert const.COMPONENT_HA_MIRROR == "ha_mirror"
    assert const.COMPONENT_WIFI_WATCHDOG == "wifi_watchdog"
    assert const.COMPONENT_BUS_WATCHDOG == "bus_watchdog"


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


def test_ha_mirror_registry_row_default_disabled() -> None:
    row = comp.REGISTRY[COMPONENT_HA_MIRROR]
    assert row.id == COMPONENT_HA_MIRROR
    assert row.label == "HA mirror"
    assert row.default_enabled is False
    assert row.locked is False


def test_default_components_ha_mirror_off() -> None:
    assert comp.default_components()[COMPONENT_HA_MIRROR] is False


def test_optional_order_includes_ha_mirror() -> None:
    opts = comp.optional()
    ids = [c.id for c in opts]
    assert ids == [
        COMPONENT_VOICE,
        COMPONENT_WIFI_WATCHDOG,
        COMPONENT_BUS_WATCHDOG,
        COMPONENT_HA_MIRROR,
    ]


async def test_ha_mirror_install_drives_deploy_config_and_enable(
    hass: HomeAssistant, payload_dir: Path
) -> None:
    (payload_dir / "brilliant-ha-mirror.service").write_text("MIRROR_UNIT")
    shell = FakeShell()
    await shell.connect()
    data = {
        CONF_PANEL: "office",
        CONF_HA_MIRROR_WS_URL: "ws://homeassistant.local:8123/api/websocket",
        CONF_HA_MIRROR_TOKEN: "long-lived-token",
        CONF_HA_MIRROR_LABEL: "home",
        CONF_HA_MIRROR_LEADER_PRIORITY: 9,
        CONF_MQTT_HOST: "192.168.1.250",
        CONF_MQTT_PORT: 1883,
        CONF_MQTT_USERNAME: "brilliant",
        CONF_MQTT_PASSWORD: "mqtt-secret",
    }
    expected_env = (
        'PANEL="office"\n'
        'HA_WS_URL="ws://homeassistant.local:8123/api/websocket"\n'
        'HA_TOKEN="long-lived-token"\n'
        'MIRROR_LABEL="home"\n'
        'LEADER_PRIORITY="9"\n'
        'MQTT_HOST="192.168.1.250"\n'
        'MQTT_PORT="1883"\n'
        'MQTT_USERNAME="brilliant"\n'
        'MQTT_PASSWORD="mqtt-secret"\n'
        'LOG_LEVEL="INFO"\n'
    )

    with (
        patch.object(panel_ops, "deploy_ha_mirror", new_callable=AsyncMock) as deploy,
        patch.object(panel_ops, "ensure_ha_mirror_config", new_callable=AsyncMock) as ensure,
        patch.object(panel_ops, "enable_ha_mirror", new_callable=AsyncMock) as enable,
    ):
        await comp._hamirror_install(hass, shell, data)

    deploy.assert_awaited_once_with(shell, str(payload_dir / "ha_mirror"))
    ensure.assert_awaited_once_with(shell, "MIRROR_UNIT", expected_env)
    enable.assert_awaited_once_with(shell)
