"""The integration is discoverable and its manifest is coherent."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.loader import async_get_integration
from pytest_homeassistant_custom_component.common import MockConfigEntry
from pytest_homeassistant_custom_component.typing import MqttMockHAClient

from custom_components.brilliant_mqtt import async_migrate_entry
from custom_components.brilliant_mqtt.const import (
    COMPONENT_BRIDGE,
    COMPONENT_VOICE,
    CONF_COMPONENTS,
    CONF_HOST,
    CONF_MESH_PRIORITY,
    CONF_MQTT_HOST,
    CONF_MQTT_PASSWORD,
    CONF_MQTT_PORT,
    CONF_MQTT_USERNAME,
    CONF_PANEL,
    CONF_ROOT_PASSWORD,
    CONF_VOICE_ENABLED,
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
    CONF_HOST: "10.100.0.10",
    CONF_ROOT_PASSWORD: "panelpass",
    CONF_PANEL: "office",
    CONF_MESH_PRIORITY: 1,
    CONF_MQTT_HOST: "172.16.1.205",
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


# HA parks the entry in SETUP_RETRY with its own internal retry timer (not ours).
@pytest.mark.allow_lingering_timers
async def test_setup_retries_when_mqtt_unavailable(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    """If the MQTT client isn't ready, setup must raise ConfigEntryNotReady.

    The config-entries machinery catches ConfigEntryNotReady and parks the entry in
    SETUP_RETRY, so assert on that terminal state (test-before-setup quality rule).
    """
    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)

    with patch(
        "homeassistant.components.mqtt.async_wait_for_mqtt_client",
        return_value=False,
    ):
        assert not await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    assert entry.state is ConfigEntryState.SETUP_RETRY


async def test_migrate_v1_folds_voice_enabled_into_components(hass: HomeAssistant) -> None:
    """v1 entry with voice_enabled=True must become v2 with components dict."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        version=1,
        data={"panel": "office", CONF_VOICE_ENABLED: True},
    )
    entry.add_to_hass(hass)
    assert await async_migrate_entry(hass, entry) is True
    assert entry.version == 2
    assert entry.data[CONF_COMPONENTS][COMPONENT_BRIDGE] is True
    assert entry.data[CONF_COMPONENTS][COMPONENT_VOICE] is True


async def test_migrate_v1_no_voice_defaults_components_off(hass: HomeAssistant) -> None:
    """v1 entry without voice_enabled must produce bridge=True, voice=False."""
    entry = MockConfigEntry(domain=DOMAIN, version=1, data={"panel": "kitchen"})
    entry.add_to_hass(hass)
    assert await async_migrate_entry(hass, entry) is True
    assert entry.data[CONF_COMPONENTS] == {COMPONENT_BRIDGE: True, COMPONENT_VOICE: False}
