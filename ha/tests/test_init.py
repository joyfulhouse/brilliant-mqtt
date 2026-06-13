"""The integration is discoverable and its manifest is coherent."""

from __future__ import annotations

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.loader import async_get_integration
from pytest_homeassistant_custom_component.common import MockConfigEntry
from pytest_homeassistant_custom_component.typing import MqttMockHAClient

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


async def test_integration_discoverable(hass: HomeAssistant) -> None:
    """The HA loader resolves the integration and the manifest carries the contract."""
    integration = await async_get_integration(hass, DOMAIN)
    assert integration.domain == DOMAIN
    assert integration.integration_type == "device"
    assert "mqtt" in (integration.dependencies or [])
    assert any(r.startswith("asyncssh==") for r in integration.requirements or [])


ENTRY_DATA = {
    CONF_HOST: "192.168.1.10",
    CONF_ROOT_PASSWORD: "panelpass",
    CONF_PANEL: "office",
    CONF_MESH_PRIORITY: 1,
    CONF_MQTT_HOST: "192.168.1.250",
    CONF_MQTT_PORT: 1883,
    CONF_MQTT_USERNAME: "brilliant",
    CONF_MQTT_PASSWORD: "mqttpass",
    DATA_SSH_HOST_KEY: "ssh-ed25519 PINNED",
}


@pytest.mark.allow_lingering_timers
async def test_entry_sets_up_and_tracks_availability(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    from pytest_homeassistant_custom_component.common import async_fire_mqtt_message

    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    manager = entry.runtime_data
    assert manager.availability is None

    async_fire_mqtt_message(hass, "brilliant/office/availability", "online")
    await hass.async_block_till_done()
    assert manager.availability == "online"

    async_fire_mqtt_message(
        hass,
        "brilliant/office/bridge",
        '{"agent_version": "0.2.0", "panel_firmware": "v26.05.20.2"}',
    )
    await hass.async_block_till_done()
    assert manager.meta == {"agent_version": "0.2.0", "panel_firmware": "v26.05.20.2"}

    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_non_object_meta_is_ignored(hass: HomeAssistant, mqtt_mock: MqttMockHAClient) -> None:
    """Valid JSON that isn't an object must not be stored (Task 9 entities do .get())."""
    from pytest_homeassistant_custom_component.common import async_fire_mqtt_message

    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    manager = entry.runtime_data
    assert manager.meta is None

    async_fire_mqtt_message(hass, "brilliant/office/bridge", "42")
    await hass.async_block_till_done()
    assert manager.meta is None  # non-object payload left meta unchanged

    assert await hass.config_entries.async_unload(entry.entry_id)
