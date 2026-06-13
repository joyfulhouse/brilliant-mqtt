"""Diagnostics never leak the root password or broker password."""

from __future__ import annotations

from pathlib import Path

import pytest
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry
from pytest_homeassistant_custom_component.typing import MqttMockHAClient

from custom_components.brilliant_mqtt.const import DOMAIN
from custom_components.brilliant_mqtt.diagnostics import async_get_config_entry_diagnostics
from tests.fakes import FakeShell
from tests.test_init import ENTRY_DATA


@pytest.mark.allow_lingering_timers
async def test_diagnostics_redact_secrets(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    fake_shell: FakeShell,
    payload_dir: Path,
) -> None:
    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    diag = await async_get_config_entry_diagnostics(hass, entry)
    assert diag["entry"]["root_password"] == "**REDACTED**"
    assert diag["entry"]["mqtt_password"] == "**REDACTED**"
    assert diag["entry"]["host"] == "10.100.0.10"  # non-secrets stay visible

    assert await hass.config_entries.async_unload(entry.entry_id)
