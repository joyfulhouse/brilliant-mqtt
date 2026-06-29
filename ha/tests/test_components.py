from custom_components.brilliant_mqtt import components as comp
from custom_components.brilliant_mqtt import const
from custom_components.brilliant_mqtt.const import (
    COMPONENT_BRIDGE,
    COMPONENT_VOICE,
    CONF_COMPONENTS,
)


def test_component_id_constants() -> None:
    assert const.CONF_COMPONENTS == "components"
    assert const.COMPONENT_BRIDGE == "bridge"
    assert const.COMPONENT_VOICE == "voice"
    assert const.COMPONENT_WIFI_WATCHDOG == "wifi_watchdog"


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
