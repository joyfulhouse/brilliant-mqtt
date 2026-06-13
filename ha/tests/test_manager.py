"""OTA state machine: grace → repair → recover/escalate, cooldown, firmware events."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
from homeassistant.core import Event, HomeAssistant
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import (
    MockConfigEntry,
    async_fire_mqtt_message,
    async_fire_time_changed,
)
from pytest_homeassistant_custom_component.typing import MqttMockHAClient

from custom_components.brilliant_mqtt.const import DOMAIN, EVENT_TYPE
from tests.fakes import FakeShell
from tests.test_init import ENTRY_DATA


async def _setup(hass: HomeAssistant) -> MockConfigEntry:
    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    return entry


def _capture_events(hass: HomeAssistant) -> list[Event]:
    events: list[Event] = []
    hass.bus.async_listen(EVENT_TYPE, events.append)
    return events


def _types(events: list[Event]) -> list[str]:
    return [e.data["type"] for e in events]


@pytest.mark.allow_lingering_timers
async def test_offline_grace_triggers_auto_repair_then_recovery(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    fake_shell: FakeShell,
    payload_dir: Path,
) -> None:
    entry = await _setup(hass)
    events = _capture_events(hass)

    async_fire_mqtt_message(hass, "brilliant/office/availability", "offline")
    await hass.async_block_till_done()
    assert not fake_shell.commands  # grace period: no SSH yet

    async_fire_time_changed(hass, dt_util.utcnow() + timedelta(minutes=11))
    await hass.async_block_till_done()

    # Repair ran: inspect → configs to /etc + staged → daemon-reload → enable --now.
    assert any(c.startswith("test -f /etc/systemd") for c in fake_shell.commands)
    assert "systemctl enable --now brilliant-mqtt" in fake_shell.commands
    etc_uploads = [p for (p, _d, _m) in fake_shell.uploads]
    assert "/etc/systemd/system/brilliant-mqtt.service" in etc_uploads
    assert "/etc/brilliant-mqtt.env" in etc_uploads
    assert "/var/brilliant-mqtt/system/brilliant-mqtt.env" in etc_uploads
    assert _types(events) == ["repair_started"]

    # Bridge comes back inside the recovery window → success, problem cleared.
    async_fire_mqtt_message(hass, "brilliant/office/availability", "online")
    await hass.async_block_till_done()
    assert _types(events) == ["repair_started", "repair_succeeded"]
    assert entry.runtime_data.problem is False

    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_repair_timeout_escalates_and_cooldown_blocks_retry(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    fake_shell: FakeShell,
    payload_dir: Path,
) -> None:
    entry = await _setup(hass)
    events = _capture_events(hass)

    async_fire_mqtt_message(hass, "brilliant/office/availability", "offline")
    await hass.async_block_till_done()
    async_fire_time_changed(hass, dt_util.utcnow() + timedelta(minutes=11))
    await hass.async_block_till_done()

    # No "online" arrives → recovery deadline passes → escalation. The recovery
    # timeout fires repair_failed (the mechanical "deadline passed" signal) AND
    # escalates with needs_attention (the human-facing alert + persistent notice).
    async_fire_time_changed(hass, dt_util.utcnow() + timedelta(minutes=13))
    await hass.async_block_till_done()
    assert _types(events) == ["repair_started", "repair_failed", "needs_attention"]
    assert entry.runtime_data.problem is True
    assert any("journalctl -u brilliant-mqtt" in c for c in fake_shell.commands)

    # A second offline→grace within the cooldown must NOT repair again.
    n_commands = len(fake_shell.commands)
    async_fire_mqtt_message(hass, "brilliant/office/availability", "offline")
    await hass.async_block_till_done()
    async_fire_time_changed(hass, dt_util.utcnow() + timedelta(minutes=25))
    await hass.async_block_till_done()
    assert len(fake_shell.commands) == n_commands
    assert _types(events)[-1] == "needs_attention"

    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_auto_repair_off_notifies_only(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    fake_shell: FakeShell,
    payload_dir: Path,
) -> None:
    entry = await _setup(hass)
    hass.config_entries.async_update_entry(entry, options={"auto_repair": False})
    await hass.async_block_till_done()
    events = _capture_events(hass)

    async_fire_mqtt_message(hass, "brilliant/office/availability", "offline")
    await hass.async_block_till_done()
    async_fire_time_changed(hass, dt_util.utcnow() + timedelta(minutes=11))
    await hass.async_block_till_done()

    assert not fake_shell.commands
    assert _types(events) == ["needs_attention"]
    assert entry.runtime_data.problem is True

    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_firmware_change_fires_event_and_refreshes_staged_copies(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    fake_shell: FakeShell,
    payload_dir: Path,
) -> None:
    entry = await _setup(hass)
    events = _capture_events(hass)

    async_fire_mqtt_message(
        hass, "brilliant/office/bridge", '{"agent_version": "0.2.0", "panel_firmware": "v1"}'
    )
    await hass.async_block_till_done()
    # First sighting: just recorded, no event (nothing to compare against).
    assert _types(events) == []
    assert entry.data["last_firmware"] == "v1"

    async_fire_mqtt_message(
        hass, "brilliant/office/bridge", '{"agent_version": "0.2.0", "panel_firmware": "v2"}'
    )
    await hass.async_block_till_done()
    assert _types(events) == ["panel_updated"]
    assert events[0].data["old_firmware"] == "v1"
    assert events[0].data["new_firmware"] == "v2"
    assert entry.data["last_firmware"] == "v2"
    # Staged copies refreshed over SSH (idempotent ensure_configs ran).
    assert "/var/brilliant-mqtt/system/brilliant-mqtt.service" in [
        p for (p, _d, _m) in fake_shell.uploads
    ]

    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_unreachable_panel_reports_and_schedules_recheck(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    payload_dir: Path,
) -> None:
    from unittest.mock import patch

    shell = FakeShell(connect_error=OSError("unreachable"))
    with patch("custom_components.brilliant_mqtt.manager.AsyncsshShell", return_value=shell):
        entry = await _setup(hass)
        events = _capture_events(hass)
        async_fire_mqtt_message(hass, "brilliant/office/availability", "offline")
        await hass.async_block_till_done()
        async_fire_time_changed(hass, dt_util.utcnow() + timedelta(minutes=11))
        await hass.async_block_till_done()

    assert _types(events) == ["repair_started", "repair_failed"]
    assert events[-1].data["reason"] == "unreachable"
    assert entry.runtime_data.problem is True

    # The failed repair scheduled an unreachable-recheck timer; unloading the
    # entry must cancel it so it does not linger past the test (the marker only
    # excuses mqtt's own timer, never a manager timer).
    assert await hass.config_entries.async_unload(entry.entry_id)


async def test_unload_cancels_pending_timers(hass: HomeAssistant) -> None:
    """Going offline schedules a grace timer; async_shutdown() must cancel it.

    NOT marked allow_lingering_timers on purpose: it runs under the strict guard,
    proving the manager's own timers are gone after shutdown — a leaked grace timer
    would fail the guard here. Built without mqtt_mock so the only timer in flight is
    the manager's (mqtt_mock would start its own recurring timer that the guard would
    flag, masking what we are testing); the availability handler is exercised directly.
    """
    import asyncio

    from homeassistant.components.mqtt.models import ReceiveMessage

    from custom_components.brilliant_mqtt.manager import PanelManager

    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)
    manager = PanelManager(hass, entry, asyncio.Lock())

    msg = ReceiveMessage(
        topic="brilliant/office/availability",
        payload="offline",
        qos=0,
        retain=True,
        subscribed_topic="brilliant/office/availability",
        timestamp=dt_util.utcnow().timestamp(),
    )
    await manager._on_availability(msg)
    assert manager._grace_cancel is not None  # grace timer scheduled, not yet fired

    await manager.async_shutdown()
    assert manager._grace_cancel is None  # cancelled on shutdown — nothing lingers
