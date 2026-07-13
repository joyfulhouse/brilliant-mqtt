import pytest
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
