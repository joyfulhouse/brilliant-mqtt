"""The update platform must resolve the bundled payload at call time, not import time.

This module imports `update` at the TOP (module scope), before any fixture runs, to
pin order-independence: the conftest `payload_dir` fixture patches
`manager._payload_dir`, and the update platform must read that patched value — not a
copy bound into its own namespace by a `from .manager import _payload_dir`.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry
from pytest_homeassistant_custom_component.typing import MqttMockHAClient

# Deliberately at module scope (the regression): importing the platform here used to
# freeze the real `_payload_dir`, so the `payload_dir` patch never reached the entity
# and setup read the Task-11-only agent_payload/VERSION (FileNotFoundError).
import custom_components.brilliant_mqtt.update  # noqa: F401
from custom_components.brilliant_mqtt.const import DOMAIN
from tests.fakes import FakeShell
from tests.test_init import ENTRY_DATA

UPDATE = "update.brilliant_office_bridge"


@pytest.mark.allow_lingering_timers
async def test_update_module_top_level_import_is_safe(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    fake_shell: FakeShell,
    payload_dir: Path,
) -> None:
    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    state = hass.states.get(UPDATE)
    assert state is not None
    # latest_version comes from the patched payload fixture, proving late binding.
    assert state.attributes["latest_version"] == "0.2.0"

    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_agent_update_declares_progress_feature(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    fake_shell: FakeShell,
    payload_dir: Path,
) -> None:
    """PROGRESS must be in supported_features — without it HA ignores in_progress /
    update_percentage, so the install card shows no progress (the reported bug)."""
    from homeassistant.components.update import UpdateEntityFeature

    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    state = hass.states.get(UPDATE)
    assert state is not None
    feats = state.attributes["supported_features"]
    assert feats & UpdateEntityFeature.INSTALL
    assert feats & UpdateEntityFeature.PROGRESS
    assert await hass.config_entries.async_unload(entry.entry_id)
