"""Config flow: per-panel entries, TOFU pinning, reserved slug, dedupe, validation."""

from __future__ import annotations

from unittest.mock import patch

from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.brilliant_mqtt.const import (
    CONF_HOST,
    CONF_MESH_PRIORITY,
    CONF_MQTT_HOST,
    CONF_MQTT_PASSWORD,
    CONF_MQTT_PORT,
    CONF_MQTT_USERNAME,
    CONF_PANEL,
    CONF_ROOT_PASSWORD,
    DATA_SSH_HOST_KEY,
    DOMAIN,
)

USER_INPUT = {
    CONF_HOST: "10.100.0.10",
    CONF_ROOT_PASSWORD: "panelpass",
    CONF_PANEL: "Office",
    CONF_MESH_PRIORITY: 1,
    CONF_MQTT_HOST: "172.16.1.205",
    CONF_MQTT_PORT: 1883,
    CONF_MQTT_USERNAME: "brilliant",
    CONF_MQTT_PASSWORD: "mqttpass",
}

VALIDATE = "custom_components.brilliant_mqtt.config_flow._validate_ssh"


async def test_user_flow_creates_entry_with_pinned_key(hass: HomeAssistant) -> None:
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    assert result["type"] == "form"

    with patch(VALIDATE, return_value="ssh-ed25519 PINNED"):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], USER_INPUT)

    assert result["type"] == "create_entry"
    assert result["title"] == "Brilliant office"
    assert result["data"][CONF_PANEL] == "office"  # slug normalized to lowercase
    assert result["data"][DATA_SSH_HOST_KEY] == "ssh-ed25519 PINNED"
    assert result["data"][CONF_ROOT_PASSWORD] == "panelpass"


async def test_mesh_slug_is_rejected(hass: HomeAssistant) -> None:
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(VALIDATE, return_value="ssh-ed25519 PINNED"):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {**USER_INPUT, CONF_PANEL: "mesh"}
        )
    assert result["type"] == "form"
    assert result["errors"] == {CONF_PANEL: "reserved_panel"}


async def test_invalid_panel_slug_is_rejected(hass: HomeAssistant) -> None:
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(VALIDATE, return_value="ssh-ed25519 PINNED"):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {**USER_INPUT, CONF_PANEL: "Office Front!"}
        )
    assert result["type"] == "form"
    assert result["errors"] == {CONF_PANEL: "invalid_panel"}


async def test_control_character_in_field_is_rejected(hass: HomeAssistant) -> None:
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(VALIDATE, return_value="ssh-ed25519 PINNED"):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {**USER_INPUT, CONF_MQTT_PASSWORD: "bad\npass"}
        )
    assert result["type"] == "form"
    assert result["errors"] == {CONF_MQTT_PASSWORD: "invalid_value"}


async def test_duplicate_panel_aborts(hass: HomeAssistant) -> None:
    MockConfigEntry(domain=DOMAIN, unique_id="office", data={CONF_PANEL: "office"}).add_to_hass(
        hass
    )
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(VALIDATE, return_value="ssh-ed25519 PINNED"):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], USER_INPUT)
    assert result["type"] == "abort"
    assert result["reason"] == "already_configured"


async def test_ssh_failure_shows_cannot_connect(hass: HomeAssistant) -> None:
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(VALIDATE, side_effect=OSError("nope")):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], USER_INPUT)
    assert result["type"] == "form"
    assert result["errors"] == {"base": "cannot_connect"}
