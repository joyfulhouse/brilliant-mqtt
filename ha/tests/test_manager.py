"""OTA state machine: grace → repair → recover/escalate, cooldown, firmware events."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

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
        assert shell.connect_count == 1  # one real SSH attempt so far

        # I1: the connect-fail path recorded the cooldown. When the 5-min recheck
        # fires _grace_expired, the panel is still offline but within the cooldown,
        # so it must ESCALATE (needs_attention) — NOT open a second SSH connection
        # (which would re-offer the root password to a flapping host → lockout).
        async_fire_time_changed(hass, dt_util.utcnow() + timedelta(seconds=301))
        await hass.async_block_till_done()
        assert shell.connect_count == 1  # bounded: no SSH storm on the recheck cadence
        assert _types(events)[-1] == "needs_attention"

    # The recheck timer was consumed by the escalate path; nothing scheduled a new
    # one (cooldown holds), so unloading the entry leaves no manager timer.
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


@pytest.mark.allow_lingering_timers
async def test_repair_step_failure_escalates_and_sets_problem(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    payload_dir: Path,
) -> None:
    """C2: a checked SSH step failing mid-repair must escalate, not fail silently.

    Connect succeeds, then `systemctl enable --now` exits non-zero → panel_ops
    raises PanelOpError out of enable_now. The repair must fire repair_failed +
    needs_attention and set problem=True — never fall through to repair_succeeded
    (the panel is half-broken; showing it green would be the real hazard).
    """
    from custom_components.brilliant_mqtt.shell import RunResult

    shell = FakeShell(responses={"systemctl enable --now brilliant-mqtt": RunResult(1, "", "boom")})
    with patch("custom_components.brilliant_mqtt.manager.AsyncsshShell", return_value=shell):
        entry = await _setup(hass)
        events = _capture_events(hass)

        async_fire_mqtt_message(hass, "brilliant/office/availability", "offline")
        await hass.async_block_till_done()
        async_fire_time_changed(hass, dt_util.utcnow() + timedelta(minutes=11))
        await hass.async_block_till_done()

    assert "repair_started" in _types(events)
    assert "repair_failed" in _types(events)
    assert "needs_attention" in _types(events)
    assert "repair_succeeded" not in _types(events)
    failed = next(e for e in events if e.data["type"] == "repair_failed")
    assert failed.data["reason"] == "repair_step_failed"
    assert entry.runtime_data.problem is True

    # No recovery timer was scheduled (we returned before the success path), and the
    # cooldown was recorded so a retry within it would be gated — unloading is clean.
    assert entry.runtime_data._recovery_cancel is None
    assert await hass.config_entries.async_unload(entry.entry_id)


async def test_shutdown_during_inflight_repair_leaks_no_timer(
    hass: HomeAssistant, payload_dir: Path
) -> None:
    """C1 (success site): shutdown mid-repair must not arm the recovery timer.

    NOT marked allow_lingering_timers: runs under the strict guard. Without the
    _shutting_down flag, the repair resumes after async_shutdown's single up-front
    cancel and schedules a recovery timer (manager.py success site, ~267) that fires
    on a torn-down entry and SSHes a removed panel. Built without mqtt_mock so the only
    timer that could linger is the manager's. The repair is wedged by gating
    FakeShell.connect on an event.

    `payload_dir` is REQUIRED: it makes _config_contents() succeed so the gated repair
    traverses the SUCCESS path to the recovery-timer schedule site. Without it,
    _config_contents() raises FileNotFoundError, the C2 step handler returns first, and
    the success-site guard is never reached (the bug the re-review caught).
    """
    import asyncio

    from custom_components.brilliant_mqtt.manager import PanelManager

    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)

    gate = asyncio.Event()
    shell = FakeShell(connect_gate=gate)
    with patch("custom_components.brilliant_mqtt.manager.AsyncsshShell", return_value=shell):
        manager = PanelManager(hass, entry, asyncio.Lock())

        repair = hass.async_create_task(manager.async_repair(trigger="auto"))
        # Deterministically wait until the repair is wedged inside connect() (gated)
        # — at which point _repairing is already True and it holds the ssh_lock.
        await shell.connect_entered.wait()
        assert manager._repairing is True

        # Entry torn down mid-repair.
        await manager.async_shutdown()
        assert manager._shutting_down is True

        # Let the wedged repair resume and run to completion down the SUCCESS path.
        gate.set()
        await repair

    # Prove the repair genuinely reached the success path (so the success-site guard,
    # not the C2 handler, is what suppressed the schedule) ...
    assert shell.uploads  # ensure_configs wrote unit/env → success path was traversed
    assert "systemctl enable --now brilliant-mqtt" in shell.commands
    # ... and that it did NOT arm a recovery timer on the dead entry.
    assert manager._recovery_cancel is None
    assert manager._grace_cancel is None


@pytest.mark.allow_lingering_timers
async def test_agent_update_step_failure_escalates_and_arms_no_timer(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    payload_dir: Path,
) -> None:
    """async_update_agent mirrors the C2 repair fix: a failed SSH step escalates.

    `systemctl restart` exits non-zero → panel_ops raises PanelOpError out of
    restart(). The update must escalate (needs_attention + problem) instead of
    letting the exception escape, and must NOT arm a recovery timer (we returned
    before the success path) — so unloading is clean.
    """
    from custom_components.brilliant_mqtt.shell import RunResult

    shell = FakeShell(responses={"systemctl restart brilliant-mqtt": RunResult(1, "", "boom")})
    with patch("custom_components.brilliant_mqtt.manager.AsyncsshShell", return_value=shell):
        entry = await _setup(hass)
        events = _capture_events(hass)
        await entry.runtime_data.async_update_agent()
        await hass.async_block_till_done()

    assert "agent_updated" not in _types(events)
    assert _types(events)[-1] == "needs_attention"
    assert entry.runtime_data.problem is True
    assert entry.runtime_data.problem_reason is not None
    assert "agent update failed" in entry.runtime_data.problem_reason
    assert shell.dir_uploads  # payload reached the panel before the failing restart
    assert entry.runtime_data._recovery_cancel is None  # no timer armed on the failure path

    assert await hass.config_entries.async_unload(entry.entry_id)


async def test_shutdown_during_inflight_agent_update_leaks_no_timer(
    hass: HomeAssistant, payload_dir: Path
) -> None:
    """C1 (success site) for async_update_agent: shutdown mid-update arms no timer.

    NOT marked allow_lingering_timers: runs under the strict guard. The update is
    wedged inside the gated connect(); async_shutdown latches _shutting_down while it
    is held in the ssh_lock, then the resumed update runs the full SUCCESS path and
    the _shutting_down guard (no await before the schedule) must suppress the recovery
    timer. No mqtt_mock → the only timer that could linger is the manager's.
    """
    import asyncio

    from custom_components.brilliant_mqtt.manager import PanelManager

    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)

    gate = asyncio.Event()
    shell = FakeShell(connect_gate=gate)
    with patch("custom_components.brilliant_mqtt.manager.AsyncsshShell", return_value=shell):
        manager = PanelManager(hass, entry, asyncio.Lock())

        update = hass.async_create_task(manager.async_update_agent())
        await shell.connect_entered.wait()  # wedged inside connect(), holding the ssh_lock

        await manager.async_shutdown()
        assert manager._shutting_down is True

        gate.set()
        await update

    # Reached the success path (payload + configs written, service restarted) ...
    assert shell.dir_uploads
    assert "systemctl restart brilliant-mqtt" in shell.commands
    # ... but did NOT arm a recovery timer on the torn-down entry.
    assert manager._recovery_cancel is None
    assert manager._grace_cancel is None


async def test_shutdown_during_inflight_repair_connect_fail_leaks_no_timer(
    hass: HomeAssistant,
) -> None:
    """C1 (connect-fail site): shutdown mid-repair must not arm the recheck timer.

    NOT marked allow_lingering_timers: runs under the strict guard. The gated connect
    RAISES once released, so the repair takes the connect-fail branch (manager.py ~241)
    whose recheck schedule into _grace_cancel must be suppressed by the success-of-the-
    other-kind _shutting_down guard. No mqtt_mock → the only timer that could linger is
    the manager's. No payload_dir needed: this path never renders config.
    """
    import asyncio

    from custom_components.brilliant_mqtt.manager import PanelManager

    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)

    gate = asyncio.Event()
    # Wedge inside connect() (connect_entered fires before the gate), then fail once
    # released — so _shutting_down is already True when the connect-fail branch runs.
    shell = FakeShell(connect_gate=gate, connect_error=OSError("unreachable"))
    with patch("custom_components.brilliant_mqtt.manager.AsyncsshShell", return_value=shell):
        manager = PanelManager(hass, entry, asyncio.Lock())

        repair = hass.async_create_task(manager.async_repair(trigger="auto"))
        await shell.connect_entered.wait()
        assert manager._repairing is True

        await manager.async_shutdown()
        assert manager._shutting_down is True

        gate.set()
        await repair

    # The connect-fail branch must NOT have armed the unreachable-recheck timer.
    assert manager._grace_cancel is None
    assert manager._recovery_cancel is None
