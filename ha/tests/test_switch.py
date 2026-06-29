"""Tests for the voice satellite switch."""

from __future__ import annotations

import pytest
from homeassistant.core import HomeAssistant

from custom_components.brilliant_mqtt.const import COMPONENT_VOICE, CONF_COMPONENTS
from custom_components.brilliant_mqtt.manager import PanelManager
from custom_components.brilliant_mqtt.switch import VoiceSatelliteSwitch


@pytest.mark.asyncio
async def test_switch_reads_components_dict(
    manager_with_fake_panel: PanelManager, hass: HomeAssistant
) -> None:
    entry = manager_with_fake_panel.entry
    entry.runtime_data = manager_with_fake_panel
    sw = VoiceSatelliteSwitch(entry)
    # default: not selected
    assert sw.is_on is False
    # Update entry data via hass config entry API
    hass.config_entries.async_update_entry(
        entry, data={**entry.data, CONF_COMPONENTS: {COMPONENT_VOICE: True}}
    )
    await hass.async_block_till_done()
    assert sw.is_on is True
