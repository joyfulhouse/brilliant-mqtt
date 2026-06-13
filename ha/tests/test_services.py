"""Domain services resolve their target device to the right PanelManager."""

from __future__ import annotations

from pathlib import Path

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr
from pytest_homeassistant_custom_component.common import MockConfigEntry
from pytest_homeassistant_custom_component.typing import MqttMockHAClient

from custom_components.brilliant_mqtt.const import DOMAIN
from tests.fakes import FakeShell
from tests.test_init import ENTRY_DATA


@pytest.mark.allow_lingering_timers
async def test_repair_and_uninstall_services(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    fake_shell: FakeShell,
    payload_dir: Path,
) -> None:
    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    device = dr.async_get(hass).async_get_device(identifiers={("mqtt", "brilliant_panel_office")})
    assert device is not None

    await hass.services.async_call(DOMAIN, "repair", {"device_id": device.id}, blocking=True)
    assert "systemctl enable --now brilliant-mqtt" in fake_shell.commands

    await hass.services.async_call(DOMAIN, "uninstall", {"device_id": device.id}, blocking=True)
    assert any(c.startswith("systemctl disable --now") for c in fake_shell.commands)
    assert (
        "rm -f /etc/systemd/system/brilliant-mqtt.service /etc/brilliant-mqtt.env"
        in fake_shell.commands
    )

    assert await hass.config_entries.async_unload(entry.entry_id)
