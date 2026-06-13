"""OTA state machine: grace → repair → recover/escalate, cooldown, firmware events."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from pathlib import Path
from typing import Any, cast
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

from custom_components.brilliant_mqtt.const import (
    DATA_SSH_HOST_KEY,
    DOMAIN,
    EVENT_TYPE,
    OPT_TRUST_HOST_KEY_CHANGES,
)
from tests.conftest import REPIN_NEW_KEY, RepinShells
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


def _timer_cancelled(cancel: object) -> bool:
    """Whether the asyncio.TimerHandle behind an async_call_later() cancel is cancelled.

    async_call_later returns `loop.call_at(...).cancel` — a bound method whose
    __self__ is the TimerHandle. We reach it through an Any cast so the manager's
    _recovery_cancel can keep its narrow CALLBACK_TYPE annotation (it really is a bound
    method at runtime) without a type: ignore.
    """
    handle: asyncio.TimerHandle = cast(Any, cancel).__self__
    return handle.cancelled()


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
    # I3: _refresh_staged_copies runs via entry.async_create_background_task; the plain
    # async_block_till_done() does NOT await background tasks, so fake_shell.uploads was
    # racily empty (~1-in-4). wait_background_tasks=True makes the SSH assertion stable.
    await hass.async_block_till_done(wait_background_tasks=True)
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
async def test_agent_update_step_failure_escalates_and_raises(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    payload_dir: Path,
) -> None:
    """I4: async_update_agent runs from the update.install service, so a failed step
    must escalate AND raise so HA surfaces "install failed" (not a false success).

    `systemctl restart` exits non-zero → panel_ops raises PanelOpError out of
    restart(). The update must escalate (needs_attention + problem) and re-raise as
    HomeAssistantError, and must NOT arm a recovery timer (we returned before the
    success path) — so unloading is clean. (async_repair, called from timers/button,
    stays swallow+escalate; only the service-call entry point raises.)
    """
    from homeassistant.exceptions import HomeAssistantError

    from custom_components.brilliant_mqtt.shell import RunResult

    shell = FakeShell(responses={"systemctl restart brilliant-mqtt": RunResult(1, "", "boom")})
    with patch("custom_components.brilliant_mqtt.manager.AsyncsshShell", return_value=shell):
        entry = await _setup(hass)
        events = _capture_events(hass)
        with pytest.raises(HomeAssistantError, match="agent update failed"):
            await entry.runtime_data.async_update_agent()
        await hass.async_block_till_done()

    assert "agent_updated" not in _types(events)
    assert _types(events)[-1] == "needs_attention"
    assert entry.runtime_data.problem is True
    assert entry.runtime_data.problem_reason is not None
    assert "agent update failed" in entry.runtime_data.problem_reason
    assert shell.dir_uploads  # payload reached the panel before the failing restart
    assert entry.runtime_data._recovery_cancel is None  # no timer armed on the failure path
    assert entry.runtime_data._repairing is False  # mutex released even though we raised

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


async def test_update_during_repair_recovery_window_leaks_no_timer(
    hass: HomeAssistant, payload_dir: Path
) -> None:
    """C1: an update during a repair's recovery window must not orphan a timer.

    NOT marked allow_lingering_timers: runs under the strict guard. Two facets of the
    same bug, both closed by giving async_update_agent the _repairing mutex + a
    _recovery_cancel cancel before re-arming:

    (a) ORPHAN: a repair arms _recovery_cancel; an update soon after re-arms it. Without
        cancelling the prior handle first, the original TimerHandle is overwritten and
        survives async_shutdown (which only cancels the *current* handle), firing
        _recovery_timeout on a torn-down entry → SSH to a removed panel. We assert the
        prior handle is cancelled, exactly one live recovery timer remains, and shutdown
        leaves nothing (the strict guard fails if the orphan lives).
    (b) CONCURRENT GRACE: during the update's restart the agent goes offline; without the
        mutex held, _on_availability arms a grace timer (grace + recovery for one panel).
        We fire an offline LWT while the update is wedged in its SSH section and assert
        _grace_cancel stays None.
    """
    import asyncio

    from homeassistant.components.mqtt.models import ReceiveMessage

    from custom_components.brilliant_mqtt.manager import PanelManager

    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)
    manager = PanelManager(hass, entry, asyncio.Lock())

    # First a clean repair so we are genuinely inside its recovery window.
    repair_shell = FakeShell()
    with patch("custom_components.brilliant_mqtt.manager.AsyncsshShell", return_value=repair_shell):
        await manager.async_repair(trigger="manual")
    assert manager._recovery_cancel is not None  # recovery timer armed by the repair
    prior_recovery = manager._recovery_cancel
    assert _timer_cancelled(prior_recovery) is False  # the repair's timer is live

    # Now an update lands inside that window. Gate its connect so we can prove facet (b)
    # (an offline LWT mid-update arms NO grace timer) before letting it finish.
    gate = asyncio.Event()
    update_shell = FakeShell(connect_gate=gate)
    with patch("custom_components.brilliant_mqtt.manager.AsyncsshShell", return_value=update_shell):
        update = hass.async_create_task(manager.async_update_agent())
        await update_shell.connect_entered.wait()  # wedged: _repairing True, holds ssh_lock
        assert manager._repairing is True

        # Facet (b): the restart-induced offline arrives mid-update → must NOT arm grace.
        offline = ReceiveMessage(
            topic="brilliant/office/availability",
            payload="offline",
            qos=0,
            retain=True,
            subscribed_topic="brilliant/office/availability",
            timestamp=dt_util.utcnow().timestamp(),
        )
        await manager._on_availability(offline)
        assert manager._grace_cancel is None  # mutex held → no concurrent grace timer

        gate.set()
        await update

    # Facet (a): the repair's recovery handle was cancelled before the update re-armed,
    # so exactly ONE live recovery timer remains (the update's, a NEW handle).
    assert _timer_cancelled(prior_recovery) is True  # old timer killed, not orphaned
    assert manager._recovery_cancel is not None
    assert manager._recovery_cancel is not prior_recovery  # a fresh handle
    assert _timer_cancelled(manager._recovery_cancel) is False  # the update's timer is live
    assert manager._grace_cancel is None

    # Shutdown cancels the one live timer; the strict guard proves nothing lingers.
    await manager.async_shutdown()
    assert manager._recovery_cancel is None


async def test_repair_cancels_pending_grace_timer(
    hass: HomeAssistant,
    payload_dir: Path,
) -> None:
    """A button/service repair while a grace timer is pending must cancel that timer.

    Otherwise the grace timer later fires _grace_expired → within-cooldown (the repair
    set _last_repair_mono) → a SPURIOUS needs_attention during the very recovery window
    the repair opened. We assert the grace TimerHandle is genuinely cancelled (not just
    overtaken) so it cannot fire, distinct from the repair's own recovery timer.

    Built directly (no mqtt_mock) so the offline LWT goes straight to _on_availability
    and the only timers in flight are the manager's; NOT allow_lingering_timers — the
    strict guard proves we leak nothing once the recovery timer is cancelled at shutdown.
    """
    import asyncio

    from homeassistant.components.mqtt.models import ReceiveMessage

    from custom_components.brilliant_mqtt.manager import PanelManager

    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)

    with patch("custom_components.brilliant_mqtt.manager.AsyncsshShell", return_value=FakeShell()):
        manager = PanelManager(hass, entry, asyncio.Lock())

        offline = ReceiveMessage(
            topic="brilliant/office/availability",
            payload="offline",
            qos=0,
            retain=True,
            subscribed_topic="brilliant/office/availability",
            timestamp=dt_util.utcnow().timestamp(),
        )
        await manager._on_availability(offline)
        assert manager._grace_cancel is not None  # grace armed by the offline LWT
        pending_grace = manager._grace_cancel
        assert _timer_cancelled(pending_grace) is False

        # A manual repair starts: it must cancel the pending grace timer so it can never
        # fire the within-cooldown spurious escalation.
        await manager.async_repair(trigger="button")
        assert manager._grace_cancel is None
        assert _timer_cancelled(pending_grace) is True  # killed, not orphaned/overtaken
        # The repair armed its own recovery timer (success path) — a different timer.
        assert manager._recovery_cancel is not None

    await manager.async_shutdown()
    assert manager._recovery_cancel is None  # strict guard: nothing lingers


async def test_on_availability_ignored_after_shutdown(hass: HomeAssistant) -> None:
    """Defense-in-depth: an offline LWT arriving after async_shutdown arms no grace timer.

    Constructed directly like the other shutdown-race tests; under the strict timer
    guard a leaked grace timer would fail the test. No mqtt_mock so the only possible
    timer is the manager's.
    """
    import asyncio

    from homeassistant.components.mqtt.models import ReceiveMessage

    from custom_components.brilliant_mqtt.manager import PanelManager

    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)
    manager = PanelManager(hass, entry, asyncio.Lock())

    await manager.async_shutdown()
    assert manager._shutting_down is True

    offline = ReceiveMessage(
        topic="brilliant/office/availability",
        payload="offline",
        qos=0,
        retain=True,
        subscribed_topic="brilliant/office/availability",
        timestamp=dt_util.utcnow().timestamp(),
    )
    await manager._on_availability(offline)
    assert manager._grace_cancel is None  # no timer armed post-shutdown


async def test_on_meta_ignored_after_shutdown(hass: HomeAssistant) -> None:
    """Defense-in-depth parity with _on_availability: a bridge-meta message arriving
    after async_shutdown must be ignored — the guard returns before storing meta (so it
    also can't spawn a staged-copy task on a torn-down entry).

    Constructed directly like the other shutdown tests; no mqtt_mock.
    """
    import asyncio

    from homeassistant.components.mqtt.models import ReceiveMessage

    from custom_components.brilliant_mqtt.manager import PanelManager

    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)
    manager = PanelManager(hass, entry, asyncio.Lock())

    await manager.async_shutdown()
    assert manager._shutting_down is True

    meta = ReceiveMessage(
        topic="brilliant/office/bridge",
        payload='{"agent_version": "0.2.0", "panel_firmware": "v1"}',
        qos=0,
        retain=True,
        subscribed_topic="brilliant/office/bridge",
        timestamp=dt_util.utcnow().timestamp(),
    )
    await manager._on_meta(meta)
    assert manager.meta is None  # guard returned before storing


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


async def test_repair_host_key_changed_without_optin_escalates_and_never_repins(
    hass: HomeAssistant,
    payload_dir: Path,
    repin_shells: RepinShells,
) -> None:
    """SECURITY: auto-re-pin OFF (default). A rotated host key (pinned connect raises
    HostKeyNotVerifiable) must escalate as repair_failed:host_key_changed WITHOUT ever
    constructing an unpinned shell — the root password is NEVER offered to the new-key
    host — and must NOT silently re-pin or arm a recheck (a rotated key won't un-rotate).

    Built directly (no mqtt_mock) under the strict timer guard so a leaked recheck timer
    would fail loudly. async_repair swallows (button/timer context), so no raise here.
    """
    import asyncio

    from custom_components.brilliant_mqtt.manager import PanelManager

    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)
    manager = PanelManager(hass, entry, asyncio.Lock())
    events = _capture_events(hass)

    await manager.async_repair(trigger="button")

    assert _types(events) == ["repair_started", "repair_failed", "needs_attention"]
    failed = next(e for e in events if e.data["type"] == "repair_failed")
    assert failed.data["reason"] == "host_key_changed"
    assert manager.problem is True
    # The pin is UNCHANGED — no silent re-pin (the TOFU bypass).
    assert entry.data[DATA_SSH_HOST_KEY] == "ssh-ed25519 PINNED"
    # SECURITY INVARIANT: an unpinned shell was NEVER constructed (the factory only ever
    # saw the stored pin, never None), so the password was never offered to a new-key host.
    assert repin_shells.unpinned_shell is None
    assert None not in repin_shells.pins_seen
    # A rotated key needs operator action; no recheck timer was armed.
    assert manager._grace_cancel is None
    assert manager._recovery_cancel is None


async def test_repair_host_key_changed_with_optin_repins_and_proceeds(
    hass: HomeAssistant,
    payload_dir: Path,
    repin_shells: RepinShells,
) -> None:
    """Opt-in ON: a rotated key is auto-trusted. The pinned connect raises
    HostKeyNotVerifiable, one UNPINNED connect captures the new key, the entry pin is
    updated, host_key_repinned fires (with new_host_key), the repair PROCEEDS on the
    re-pinned shell (ensure_configs/enable run), and a recovery timer is armed.

    Built directly under the strict guard; shutdown at the end cancels the recovery timer
    so nothing lingers.
    """
    import asyncio

    from custom_components.brilliant_mqtt.manager import PanelManager

    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="office",
        data=ENTRY_DATA,
        options={OPT_TRUST_HOST_KEY_CHANGES: True},
    )
    entry.add_to_hass(hass)
    manager = PanelManager(hass, entry, asyncio.Lock())
    events = _capture_events(hass)

    await manager.async_repair(trigger="button")

    # Re-pinned: the entry now stores the NEW key, and an auditable event fired.
    assert entry.data[DATA_SSH_HOST_KEY] == REPIN_NEW_KEY
    repinned = next(e for e in events if e.data["type"] == "host_key_repinned")
    assert repinned.data["new_host_key"] == REPIN_NEW_KEY
    # The repair PROCEEDED on the re-pinned (unpinned) shell.
    assert repin_shells.unpinned_shell is not None
    assert "systemctl enable --now brilliant-mqtt" in repin_shells.unpinned_shell.commands
    assert "repair_failed" not in _types(events)
    # Success path armed a recovery timer.
    assert manager._recovery_cancel is not None

    await manager.async_shutdown()
    assert manager._recovery_cancel is None


async def test_repair_repin_connect_failure_is_unreachable(
    hass: HomeAssistant,
    payload_dir: Path,
    repin_shells: RepinShells,
) -> None:
    """Opt-in ON but the unpinned re-pin connect itself fails (panel genuinely down on
    the second attempt) → the OSError propagates to the unreachable handler:
    repair_failed:unreachable, and the entry pin is left UNCHANGED (nothing to pin).

    Built directly under the strict guard; the unreachable branch arms a recheck timer,
    so shutdown at the end cancels it.
    """
    import asyncio

    from custom_components.brilliant_mqtt.manager import PanelManager

    repin_shells.unpinned_connect_error = OSError("unreachable on re-pin")

    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="office",
        data=ENTRY_DATA,
        options={OPT_TRUST_HOST_KEY_CHANGES: True},
    )
    entry.add_to_hass(hass)
    manager = PanelManager(hass, entry, asyncio.Lock())
    events = _capture_events(hass)

    await manager.async_repair(trigger="button")

    failed = next(e for e in events if e.data["type"] == "repair_failed")
    assert failed.data["reason"] == "unreachable"
    assert "host_key_repinned" not in _types(events)
    # The re-pin attempt failed before any key could be persisted → pin UNCHANGED.
    assert entry.data[DATA_SSH_HOST_KEY] == "ssh-ed25519 PINNED"

    await manager.async_shutdown()
    assert manager._grace_cancel is None


async def test_update_host_key_changed_without_optin_raises(
    hass: HomeAssistant,
    payload_dir: Path,
    repin_shells: RepinShells,
) -> None:
    """SECURITY: async_update_agent with auto-re-pin OFF. A rotated host key must raise
    HomeAssistantError carrying the reconfigure guidance AND escalate, WITHOUT ever
    offering the password to the new-key host (no unpinned shell constructed) and without
    touching the stored pin.
    """
    import asyncio

    from homeassistant.exceptions import HomeAssistantError

    from custom_components.brilliant_mqtt.manager import PanelManager

    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)
    manager = PanelManager(hass, entry, asyncio.Lock())
    events = _capture_events(hass)

    with pytest.raises(HomeAssistantError, match="host key changed"):
        await manager.async_update_agent()

    assert _types(events)[-1] == "needs_attention"
    assert manager.problem is True
    assert "agent_updated" not in _types(events)
    # SECURITY INVARIANT: no unpinned shell → password never offered to a new-key host.
    assert repin_shells.unpinned_shell is None
    assert None not in repin_shells.pins_seen
    assert entry.data[DATA_SSH_HOST_KEY] == "ssh-ed25519 PINNED"
    assert manager._recovery_cancel is None
    assert manager._repairing is False
