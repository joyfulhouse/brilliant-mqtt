"""Diagnostics never leak the root password or broker password."""

from __future__ import annotations

from pathlib import Path

import pytest
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry
from pytest_homeassistant_custom_component.typing import MqttMockHAClient

from custom_components.brilliant_mqtt.const import CONF_HA_MIRROR_TOKEN, DOMAIN
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
    mirror_token = "diagnostics-must-never-expose-this-token"
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="office",
        data={**ENTRY_DATA, CONF_HA_MIRROR_TOKEN: mirror_token},
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    diag = await async_get_config_entry_diagnostics(hass, entry)
    assert diag["entry"]["root_password"] == "**REDACTED**"
    assert diag["entry"]["mqtt_password"] == "**REDACTED**"
    assert diag["entry"][CONF_HA_MIRROR_TOKEN] == "**REDACTED**"
    assert mirror_token not in repr(diag)
    assert "**REDACTED**" in repr(diag)
    assert diag["entry"]["host"] == "192.168.1.10"  # non-secrets stay visible

    assert await hass.config_entries.async_unload(entry.entry_id)
