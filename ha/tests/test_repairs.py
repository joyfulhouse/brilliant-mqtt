"""Repair issues + translated exceptions: the operator-facing escalation surface.

Escalation logic and triggers are covered in test_manager.py; here we pin only the
SURFACE: that an escalation raises a repair issue in the issue registry (not a
persistent notification), that recovery deletes it, and that the user-surfaced
service errors carry a translation_domain/translation_key instead of a hardcoded
message string.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
from homeassistant.components.mqtt.models import ReceiveMessage
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import issue_registry as ir
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import (
    MockConfigEntry,
    async_fire_mqtt_message,
    async_fire_time_changed,
)
from pytest_homeassistant_custom_component.typing import MqttMockHAClient

from custom_components.brilliant_mqtt.const import DOMAIN
from custom_components.brilliant_mqtt.manager import PanelManager
from tests.fakes import FakeShell
from tests.test_init import ENTRY_DATA


def _offline_msg() -> ReceiveMessage:
    return ReceiveMessage(
        topic="brilliant/office/availability",
        payload="offline",
        qos=0,
        retain=True,
        subscribed_topic="brilliant/office/availability",
        timestamp=dt_util.utcnow().timestamp(),
    )


@pytest.mark.allow_lingering_timers
async def test_escalation_raises_repair_issue_and_recovery_clears_it(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    payload_dir: Path,
) -> None:
    """An auto-repair-off outage past grace must surface a repair ISSUE (not a
    persistent notification), and a later recovery must delete it."""
    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)
    hass.config_entries.async_update_entry(entry, options={"auto_repair": False})
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    registry = ir.async_get(hass)
    issue_id = f"needs_attention_{entry.entry_id}"
    assert registry.async_get_issue(DOMAIN, issue_id) is None

    async_fire_mqtt_message(hass, "brilliant/office/availability", "offline")
    await hass.async_block_till_done()
    async_fire_time_changed(hass, dt_util.utcnow() + timedelta(minutes=11))
    await hass.async_block_till_done()

    issue = registry.async_get_issue(DOMAIN, issue_id)
    assert issue is not None
    assert issue.translation_key == "needs_attention"
    assert issue.translation_placeholders is not None
    assert issue.translation_placeholders["panel"] == "office"
    assert "grace" in issue.translation_placeholders["reason"]
    assert issue.severity == ir.IssueSeverity.ERROR
    assert entry.runtime_data.problem is True

    # Bridge recovers → the problem clears → the issue is deleted.
    async_fire_mqtt_message(hass, "brilliant/office/availability", "online")
    await hass.async_block_till_done()
    assert registry.async_get_issue(DOMAIN, issue_id) is None
    assert entry.runtime_data.problem is False

    assert await hass.config_entries.async_unload(entry.entry_id)


async def test_update_already_in_progress_raises_translated_error(
    hass: HomeAssistant, payload_dir: Path
) -> None:
    """The re-entrancy guard raises a translated HomeAssistantError, not a raw string."""
    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)
    manager = PanelManager(hass, entry, asyncio.Lock())
    manager._repairing = True  # simulate a concurrent repair/update in flight

    with pytest.raises(HomeAssistantError) as err:
        await manager.async_update_agent()
    assert err.value.translation_domain == DOMAIN
    assert err.value.translation_key == "already_in_progress"


@pytest.mark.allow_lingering_timers
async def test_update_step_failure_raises_translated_update_failed(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    payload_dir: Path,
) -> None:
    """A failed update step raises HomeAssistantError(translation_key='update_failed')
    with the underlying error as a placeholder, and escalates a repair issue."""
    from custom_components.brilliant_mqtt.shell import RunResult

    shell = FakeShell(responses={"systemctl restart brilliant-mqtt": RunResult(1, "", "boom")})
    with patch("custom_components.brilliant_mqtt.manager.AsyncsshShell", return_value=shell):
        entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
        entry.add_to_hass(hass)
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

        with pytest.raises(HomeAssistantError) as err:
            await entry.runtime_data.async_update_agent()
        await hass.async_block_till_done()

    assert err.value.translation_domain == DOMAIN
    assert err.value.translation_key == "update_failed"
    assert err.value.translation_placeholders is not None
    assert "boom" in err.value.translation_placeholders["error"]
    # The same outage was surfaced as a repair issue.
    issue = ir.async_get(hass).async_get_issue(DOMAIN, f"needs_attention_{entry.entry_id}")
    assert issue is not None

    assert await hass.config_entries.async_unload(entry.entry_id)


async def test_uninstall_failure_raises_translated_uninstall_failed(
    hass: HomeAssistant, payload_dir: Path
) -> None:
    """A failed uninstall raises HomeAssistantError(translation_key='uninstall_failed')."""
    shell = FakeShell(connect_error=OSError("unreachable"))
    with patch("custom_components.brilliant_mqtt.manager.AsyncsshShell", return_value=shell):
        entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
        entry.add_to_hass(hass)
        manager = PanelManager(hass, entry, asyncio.Lock())

        with pytest.raises(HomeAssistantError) as err:
            await manager.async_uninstall()
    assert err.value.translation_domain == DOMAIN
    assert err.value.translation_key == "uninstall_failed"
    assert err.value.translation_placeholders is not None
    assert "unreachable" in err.value.translation_placeholders["error"]


async def test_update_host_key_changed_raises_translated_error(
    hass: HomeAssistant,
    payload_dir: Path,
    repin_shells: object,
) -> None:
    """A rotated host key during update raises translation_key='host_key_changed'."""
    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)
    manager = PanelManager(hass, entry, asyncio.Lock())

    with pytest.raises(HomeAssistantError) as err:
        await manager.async_update_agent()
    assert err.value.translation_domain == DOMAIN
    assert err.value.translation_key == "host_key_changed"
