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

from custom_components.brilliant_mqtt import panel_ops
from custom_components.brilliant_mqtt.const import DOMAIN
from custom_components.brilliant_mqtt.manager import PanelManager
from custom_components.brilliant_mqtt.shell import RunResult
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
    fake_shell: FakeShell,
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


@pytest.mark.allow_lingering_timers
async def test_removing_entry_deletes_its_repair_issue(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    payload_dir: Path,
    fake_shell: FakeShell,
) -> None:
    """Deleting a config entry with an active repair issue must not orphan the issue."""
    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)
    hass.config_entries.async_update_entry(entry, options={"auto_repair": False})
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    registry = ir.async_get(hass)
    issue_id = f"needs_attention_{entry.entry_id}"
    legacy_issue_id = f"ha_mirror_retired_{entry.entry_id}"

    # Drive an escalation so the repair issue exists.
    async_fire_mqtt_message(hass, "brilliant/office/availability", "offline")
    await hass.async_block_till_done()
    async_fire_time_changed(hass, dt_util.utcnow() + timedelta(minutes=11))
    await hass.async_block_till_done()
    assert registry.async_get_issue(DOMAIN, issue_id) is not None
    ir.async_create_issue(
        hass,
        DOMAIN,
        legacy_issue_id,
        is_fixable=False,
        severity=ir.IssueSeverity.WARNING,
        translation_key="ha_mirror_retired",
        translation_placeholders={"panel": "office", "reason": "responsiveness safety"},
    )
    assert registry.async_get_issue(DOMAIN, legacy_issue_id) is not None

    # Removing the entry must clean the issue up (async_remove_entry).
    assert await hass.config_entries.async_remove(entry.entry_id)
    await hass.async_block_till_done()
    assert registry.async_get_issue(DOMAIN, issue_id) is None
    assert registry.async_get_issue(DOMAIN, legacy_issue_id) is None


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
    # The same failure was surfaced as a repair issue.
    issue = ir.async_get(hass).async_get_issue(DOMAIN, f"needs_attention_{entry.entry_id}")
    assert issue is not None


async def test_uninstall_retains_agent_when_observer_stop_is_unproven(
    hass: HomeAssistant,
    payload_dir: Path,
) -> None:
    """Never delete the main payload around an ambiguously active observer process."""
    shell = FakeShell(
        responses={
            "systemctl disable --now brilliant-ble-observer": RunResult(
                1,
                "",
                "disable failed",
            ),
            panel_ops.BLE_OBSERVER_ACTIVE_COMMAND: RunResult(
                255,
                "inactive\n",
                "transport lost",
            ),
        }
    )
    with patch("custom_components.brilliant_mqtt.manager.AsyncsshShell", return_value=shell):
        entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
        entry.add_to_hass(hass)
        manager = PanelManager(hass, entry, asyncio.Lock())

        with pytest.raises(HomeAssistantError) as err:
            await manager.async_uninstall()

    assert err.value.translation_key == "uninstall_failed"
    assert shell.commands[:2] == [
        "systemctl disable --now brilliant-ble-observer",
        panel_ops.BLE_OBSERVER_ACTIVE_COMMAND,
    ]
    assert not any("brilliant-mqtt.service /etc/brilliant-mqtt.env" in c for c in shell.commands)
    assert not any("rm -rf /var/brilliant-mqtt " in c for c in shell.commands)
    assert shell.uploads == []


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


async def test_uninstall_host_key_changed_raises_translated_error(
    hass: HomeAssistant,
    repin_shells: object,
) -> None:
    """A rotated host key during uninstall raises translation_key='host_key_changed'.

    Before this fix, async_uninstall connected via a plain pinned shell and mapped
    every connect/op failure (including HostKeyNotVerifiable) to the generic
    uninstall_failed key. It now routes through _connect_for_repair(), like
    async_update_agent and _voice_ssh_session, so a rotated host key surfaces its
    own dedicated error instead of being folded into uninstall_failed.
    """
    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)
    manager = PanelManager(hass, entry, asyncio.Lock())

    with pytest.raises(HomeAssistantError) as err:
        await manager.async_uninstall()
    assert err.value.translation_domain == DOMAIN
    assert err.value.translation_key == "host_key_changed"
