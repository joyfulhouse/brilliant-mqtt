"""Interrupted agent updates: durable window record, restore and Repair menu (#219)."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from homeassistant.components.repairs import repairs_flow_manager
from homeassistant.components.repairs.issue_handler import async_process_repairs_platforms
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.storage import Store
from homeassistant.helpers.translation import async_get_translations
from homeassistant.setup import async_setup_component
from pytest_homeassistant_custom_component.common import MockConfigEntry
from pytest_homeassistant_custom_component.typing import MqttMockHAClient

from custom_components.brilliant_mqtt import update_state
from custom_components.brilliant_mqtt.const import DOMAIN
from custom_components.brilliant_mqtt.diagnostics import async_get_config_entry_diagnostics
from custom_components.brilliant_mqtt.provisioning_journal import ProvisioningJournal
from custom_components.brilliant_mqtt.update_intent import (
    InterruptedUpdates,
    UpdateIntent,
    UpdateIntentStore,
    UpdateTarget,
    async_journal_concerns_panel,
    compute_update_target,
    pinned_fingerprint,
    request_concerns_panel,
    update_intent_store,
)
from tests.test_init import ENTRY_DATA
from tests.test_provisioning_journal import _FINGERPRINT, _PUBLIC_KEY, _record
from tests.update_harness import Shells, Spies

_TARGET = UpdateTarget("0.2.0", 1, "a" * 64, "b" * 64)
_OTHER_KEY = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIBsF8pQ8C3EWxF7bGrXWvqJXV1fbNHo6p5FC0gIjJbqv"


def _intent(**changes: Any) -> UpdateIntent:
    base = UpdateIntent(
        panel="office",
        target_version="0.2.0",
        target_ordinal=1,
        target_digest="a" * 64,
        target_set_digest="b" * 64,
        stage="connect",
        attempts=1,
        max_attempts=5,
        started_at=datetime(2026, 10, 23, 12, 0, tzinfo=UTC),
        deadline=datetime(2026, 10, 23, 12, 2, tzinfo=UTC),
    )
    return replace(base, **changes)


@dataclass
class _Panel:
    """A minimal UpdatePanel: records retries and notifications."""

    hass: HomeAssistant
    panel: str = "office"
    management_id: str = "mgmt-office"
    target: UpdateTarget | None = _TARGET
    journal_owns: bool = False
    running: bool = False
    updates: int = 0
    notified: int = 0
    gate: asyncio.Event | None = None
    events: list[str] = field(default_factory=list)

    @property
    def interrupted_issue_id(self) -> str:
        return f"interrupted_update_{self.management_id}"

    @property
    def update_running(self) -> bool:
        return self.running

    async def async_update_target(self) -> UpdateTarget | None:
        return self.target

    async def async_journal_owns_panel(self) -> bool:
        return self.journal_owns

    def notify_update_state(self) -> None:
        self.notified += 1

    async def async_update_agent(self) -> None:
        self.running = True
        self.updates += 1
        try:
            if self.gate is not None:
                await self.gate.wait()
        finally:
            self.running = False


def _issue(hass: HomeAssistant, panel: _Panel) -> ir.IssueEntry | None:
    return ir.async_get(hass).async_get_issue(DOMAIN, panel.interrupted_issue_id)


async def _surfaced(hass: HomeAssistant, panel: _Panel) -> InterruptedUpdates:
    store = UpdateIntentStore(hass)
    await store.async_set(panel.management_id, _intent())
    interrupted = InterruptedUpdates(panel, store)
    await interrupted.async_restore()
    assert interrupted.surfaced is not None
    return interrupted


def test_intent_round_trips_and_rejects_anything_not_exact() -> None:
    intent = _intent()
    assert UpdateIntent.from_dict(intent.as_dict()) == intent
    for broken in (
        {**intent.as_dict(), "extra": 1},
        {key: value for key, value in intent.as_dict().items() if key != "deadline"},
        {**intent.as_dict(), "stage": "baseline"},
        {**intent.as_dict(), "attempts": True},
        {**intent.as_dict(), "started_at": "2026-10-23T12:00:00"},
        {**intent.as_dict(), "target_digest": "A" * 64},
        {**intent.as_dict(), "panel": "Office Panel"},
        "not a mapping",
    ):
        assert UpdateIntent.from_dict(broken) is None


async def test_compute_update_target_hashes_every_selected_component(payload_dir: Path) -> None:
    bridge_only = await compute_update_target(str(payload_dir), ())
    with_watchdog = await compute_update_target(str(payload_dir), ("wifi_watchdog",))

    assert bridge_only is not None and with_watchdog is not None
    assert bridge_only.version == "0.2.0"
    assert bridge_only.ordinal == 1
    assert (bridge_only.version, bridge_only.digest) == (
        with_watchdog.version,
        with_watchdog.digest,
    )
    assert bridge_only.set_digest != with_watchdog.set_digest
    (payload_dir / "wifi_watchdog/brilliant_wifi_watchdog/run.py").write_text("# changed\n")
    changed = await compute_update_target(str(payload_dir), ("wifi_watchdog",))
    assert changed is not None and changed.set_digest != with_watchdog.set_digest
    assert await compute_update_target(str(payload_dir / "missing"), ()) is None


def test_request_concern_matches_identity_slug_or_host_and_fails_closed() -> None:
    request = _record().panel_request
    assert pinned_fingerprint(_PUBLIC_KEY) == _FINGERPRINT
    assert pinned_fingerprint("not a key") is None
    assert pinned_fingerprint(None) is None

    def concerns(**changes: object) -> bool:
        values: dict[str, object] = {
            "slug": "kitchen",
            "host": "kitchen.iot.example",
            "pinned_key": _OTHER_KEY,
        }
        values.update(changes)
        return request_concerns_panel(
            request,
            slug=str(values["slug"]),
            host=str(values["host"]),
            pinned_key=values["pinned_key"],
        )

    assert concerns() is False
    assert concerns(pinned_key=_PUBLIC_KEY) is True
    assert concerns(slug="office") is True
    assert concerns(host="office.iot.example") is True
    assert concerns(pinned_key="garbage") is True
    assert concerns(pinned_key=None) is True


async def test_journal_concern_checks_active_journal_retained_and_unreadable(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert (
        await async_journal_concerns_panel(hass, slug="office", host="h", pinned_key=_OTHER_KEY)
        is False
    )

    async def load_record(_self: ProvisioningJournal) -> object:
        return _record()

    monkeypatch.setattr(ProvisioningJournal, "async_load", load_record)
    assert (
        await async_journal_concerns_panel(hass, slug="office", host="h", pinned_key=_OTHER_KEY)
        is True
    )
    assert (
        await async_journal_concerns_panel(hass, slug="kitchen", host="h", pinned_key=_OTHER_KEY)
        is False
    )

    async def unreadable(_self: ProvisioningJournal) -> object:
        raise ValueError("journal_unreadable")

    monkeypatch.setattr(ProvisioningJournal, "async_load", unreadable)
    assert (
        await async_journal_concerns_panel(hass, slug="kitchen", host="h", pinned_key=_OTHER_KEY)
        is True
    )


async def test_store_saves_before_swapping_the_cache(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = UpdateIntentStore(hass)
    await store.async_set("p", _intent())

    async def failing_save(_self: Store[Any], _data: object) -> None:
        raise RuntimeError("serialization failed")

    monkeypatch.setattr(Store, "async_save", failing_save)
    with pytest.raises(RuntimeError):
        await store.async_set("p", _intent(attempts=3))
    with pytest.raises(RuntimeError):
        await store.async_clear("p")
    assert await store.async_get("p") == _intent()


async def test_store_survives_reload_and_forgets_cleared_records(hass: HomeAssistant) -> None:
    await UpdateIntentStore(hass).async_set("p", _intent())
    reloaded = UpdateIntentStore(hass)
    assert await reloaded.async_get("p") == _intent()
    await reloaded.async_clear("p")
    assert await UpdateIntentStore(hass).async_get("p") is None


async def test_window_save_failure_keeps_the_surfaced_record_and_issue(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> None:
    panel = _Panel(hass)
    interrupted = await _surfaced(hass, panel)

    async def failing_save(_self: Store[Any], _data: object) -> None:
        raise RuntimeError("serialization failed")

    monkeypatch.setattr(Store, "async_save", failing_save)
    with pytest.raises(RuntimeError):
        await interrupted.async_begin_window("connect", 0, 5, datetime.now(UTC))
    assert interrupted.surfaced is not None
    assert _issue(hass, panel) is not None


async def test_window_cancelled_before_save_keeps_the_old_record_and_issue(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> None:
    panel = _Panel(hass)
    interrupted = await _surfaced(hass, panel)
    entered = asyncio.Event()

    async def hanging_save(_self: Store[Any], _data: object) -> None:
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(Store, "async_save", hanging_save)
    task = asyncio.create_task(interrupted.async_begin_window("connect", 0, 5, datetime.now(UTC)))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert interrupted.surfaced == _intent()
    assert _issue(hass, panel) is not None
    monkeypatch.undo()
    assert await interrupted._store.async_get(panel.management_id) == _intent()


async def test_window_without_a_target_overwrites_nothing(hass: HomeAssistant) -> None:
    panel = _Panel(hass)
    interrupted = await _surfaced(hass, panel)
    panel.target = None

    assert await interrupted.async_begin_window("connect", 0, 5, datetime.now(UTC)) is False
    assert await interrupted._store.async_get(panel.management_id) == _intent()
    assert _issue(hass, panel) is not None


async def test_window_lifecycle_replaces_then_clears(hass: HomeAssistant) -> None:
    panel = _Panel(hass)
    interrupted = await _surfaced(hass, panel)
    deadline = datetime.now(UTC) + timedelta(minutes=2)

    assert await interrupted.async_begin_window("admission", 2, 5, deadline) is True
    stored = await interrupted._store.async_get(panel.management_id)
    assert stored is not None and (stored.stage, stored.attempts) == ("admission", 2)
    assert stored.target_set_digest == _TARGET.set_digest
    assert interrupted.surfaced is None
    assert _issue(hass, panel) is None

    await interrupted.async_end_window()
    assert await interrupted._store.async_get(panel.management_id) is None


async def test_restore_surfaces_without_connecting_and_flags_expiry(hass: HomeAssistant) -> None:
    panel = _Panel(hass)
    interrupted = await _surfaced(hass, panel)

    issue = _issue(hass, panel)
    assert issue is not None and issue.is_fixable
    assert issue.translation_key == "interrupted_update"
    diagnostics = interrupted.diagnostics()
    assert diagnostics is not None and diagnostics["expired"] is True
    assert update_state.safe_interrupted(diagnostics) == diagnostics
    future = _intent(deadline=datetime.now(UTC) + timedelta(hours=1))
    interrupted.surfaced = future
    refreshed = interrupted.diagnostics()
    assert refreshed is not None and refreshed["expired"] is False
    assert panel.updates == 0


async def test_restore_lets_a_journal_win(hass: HomeAssistant) -> None:
    panel = _Panel(hass, journal_owns=True)
    store = UpdateIntentStore(hass)
    await store.async_set(panel.management_id, _intent())
    interrupted = InterruptedUpdates(panel, store)

    await interrupted.async_restore()

    assert interrupted.surfaced is None
    assert _issue(hass, panel) is None
    assert await store.async_get(panel.management_id) is None


async def test_restore_never_fails_setup(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> None:
    panel = _Panel(hass)
    store = UpdateIntentStore(hass)

    async def broken(_self: UpdateIntentStore, _key: str) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(UpdateIntentStore, "async_get", broken)
    await InterruptedUpdates(panel, store).async_restore()
    assert _issue(hass, panel) is None


async def test_retry_starts_one_fresh_update_and_refuses_a_second(hass: HomeAssistant) -> None:
    panel = _Panel(hass, gate=asyncio.Event())
    interrupted = await _surfaced(hass, panel)

    assert await interrupted.async_retry() is None
    assert panel.running is True  # eager start: running before any await
    assert await interrupted.async_retry() == "already_in_progress"
    assert interrupted.surfaced is not None
    assert _issue(hass, panel) is not None
    assert panel.gate is not None
    panel.gate.set()
    await hass.async_block_till_done()
    assert panel.updates == 1


@pytest.mark.parametrize(
    "target",
    [None, replace(_TARGET, digest="c" * 64), replace(_TARGET, set_digest="d" * 64)],
)
async def test_retry_refuses_a_changed_target(
    hass: HomeAssistant, target: UpdateTarget | None
) -> None:
    panel = _Panel(hass)
    interrupted = await _surfaced(hass, panel)
    panel.target = target

    assert await interrupted.async_retry() == "interrupted_update_target_changed"
    assert panel.updates == 0


async def test_retry_defers_to_journal_recovery_and_drops_the_record(
    hass: HomeAssistant,
) -> None:
    panel = _Panel(hass)
    interrupted = await _surfaced(hass, panel)
    panel.journal_owns = True

    assert await interrupted.async_retry() == "interrupted_update_recovery_owned"
    assert panel.updates == 0
    assert interrupted.surfaced is None
    assert await interrupted._store.async_get(panel.management_id) is None


async def test_dismiss_clears_and_a_failed_clear_raises(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> None:
    panel = _Panel(hass)
    interrupted = await _surfaced(hass, panel)

    async def failing_save(_self: Store[Any], _data: object) -> None:
        raise RuntimeError("serialization failed")

    with monkeypatch.context() as patched:
        patched.setattr(Store, "async_save", failing_save)
        with pytest.raises(RuntimeError):
            await interrupted.async_dismiss()
    assert interrupted.surfaced is not None
    await interrupted.async_dismiss()
    assert interrupted.surfaced is None
    assert await interrupted._store.async_get(panel.management_id) is None


def test_retry_state_and_last_error_allowlists() -> None:
    state: dict[str, object] = {
        "stage": "connect",
        "attempt": 1,
        "max_attempts": 5,
        "reason": "host_unreachable",
        "retry_in_seconds": 5.0,
        "next_attempt_at": "2026-10-23T12:00:05+00:00",
    }
    assert update_state.safe_retry_state(state) == state
    assert update_state.safe_retry_state({**state, "retry_in_seconds": None}) is not None
    for broken in (
        {**state, "stage": "baseline"},
        {**state, "reason": "No route to host 10.0.0.5"},
        {**state, "attempt": True},
        {**state, "attempt": 6},
        {**state, "next_attempt_at": "2026-10-23T12:00:05"},
        {**state, "retry_in_seconds": "5"},
        {key: value for key, value in state.items() if key != "next_attempt_at"},
        {**state, "extra": 1},
    ):
        assert update_state.safe_retry_state(broken) is None
    error = {"stage": "admission", "code": "admission_connection_lost", "attempts": 2}
    assert update_state.safe_last_error(error) == error
    assert update_state.safe_last_error({**error, "code": "free text"}) is None
    assert update_state.safe_last_error({**error, "stage": "elsewhere"}) is None
    assert update_state.safe_last_error({**error, "attempts": -1}) is None


# --- Through a real entry, HA's repairs flow manager and the Update entity --------

_UPDATE_ENTITY = "update.brilliant_office_bridge"


async def _surface_through_reload(hass: HomeAssistant) -> tuple[MockConfigEntry, str]:
    """Set up a real entry, record an in-window update, reload (an HA restart)."""
    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    manager = entry.runtime_data.panels[entry.entry_id]
    target = await manager.async_update_target()
    assert target is not None
    await update_intent_store(hass).async_set(
        manager.management_id,
        _intent(
            target_version=target.version,
            target_ordinal=target.ordinal,
            target_digest=target.digest,
            target_set_digest=target.set_digest,
            stage="admission",
            attempts=2,
        ),
    )
    assert await hass.config_entries.async_reload(entry.entry_id)
    await hass.async_block_till_done()
    issue_id = f"interrupted_update_{manager.management_id}"
    assert ir.async_get(hass).async_get_issue(DOMAIN, issue_id) is not None
    return entry, issue_id


async def _open_menu(hass: HomeAssistant, issue_id: str) -> dict[str, Any]:
    assert await async_setup_component(hass, "repairs", {})
    await async_process_repairs_platforms(hass)
    flows = repairs_flow_manager(hass)
    assert flows is not None
    result = await flows.async_init(DOMAIN, data={"issue_id": issue_id})
    assert result["type"] is FlowResultType.MENU
    assert result["menu_options"] == ["retry", "dismiss"]
    translations = await async_get_translations(hass, "en", "issues", [DOMAIN])
    prefix = f"component.{DOMAIN}.issues.interrupted_update.fix_flow.step.init"
    assert translations[f"{prefix}.menu_options.retry"]
    assert translations[f"{prefix}.menu_options.dismiss"]
    assert "{target_version}" in translations[f"{prefix}.description"]
    return dict(result)


@pytest.mark.allow_lingering_timers
async def test_repairs_flow_dismiss_through_real_entry_setup(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    payload_dir: Path,
    shells: Shells,
    spies: Spies,
) -> None:
    entry, issue_id = await _surface_through_reload(hass)
    menu = await _open_menu(hass, issue_id)
    flows = repairs_flow_manager(hass)
    assert flows is not None

    result = await flows.async_configure(menu["flow_id"], {"next_step_id": "dismiss"})

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert ir.async_get(hass).async_get_issue(DOMAIN, issue_id) is None
    assert await update_intent_store(hass).async_get(entry.entry_id) is None
    assert spies.admissions == 0
    assert shells.created == []
    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_repairs_flow_retry_runs_exactly_one_update_with_entity_progress(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    payload_dir: Path,
    shells: Shells,
    spies: Spies,
) -> None:
    entry, issue_id = await _surface_through_reload(hass)
    menu = await _open_menu(hass, issue_id)
    flows = repairs_flow_manager(hass)
    assert flows is not None
    held = asyncio.Event()
    release = asyncio.Event()

    async def hold(_shell: object) -> None:
        held.set()
        await release.wait()

    spies.on_baseline = hold

    result = await flows.async_configure(menu["flow_id"], {"next_step_id": "retry"})

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert ir.async_get(hass).async_get_issue(DOMAIN, issue_id) is None
    await held.wait()
    await asyncio.sleep(0)
    state = hass.states.get(_UPDATE_ENTITY)
    assert state is not None
    assert state.attributes["in_progress"] is True
    assert state.attributes["update_percentage"] == 25
    release.set()
    await hass.async_block_till_done()
    state = hass.states.get(_UPDATE_ENTITY)
    assert state is not None
    assert state.attributes["in_progress"] is False
    assert state.attributes["update_percentage"] is None
    assert spies.admissions == 1
    assert spies.baselines == 1  # exactly one update and one transaction
    assert await update_intent_store(hass).async_get(entry.entry_id) is None
    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_repairs_flow_without_a_loaded_owner_aborts(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    payload_dir: Path,
    shells: Shells,
    spies: Spies,
) -> None:
    entry, issue_id = await _surface_through_reload(hass)
    menu = await _open_menu(hass, issue_id)
    flows = repairs_flow_manager(hass)
    assert flows is not None
    assert await hass.config_entries.async_unload(entry.entry_id)

    result = await flows.async_configure(menu["flow_id"], {"next_step_id": "retry"})

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "interrupted_update_unavailable"
    assert spies.admissions == 0


@pytest.mark.allow_lingering_timers
async def test_repairs_flow_dismiss_failure_aborts_and_keeps_the_issue(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    payload_dir: Path,
    shells: Shells,
    spies: Spies,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entry, issue_id = await _surface_through_reload(hass)
    menu = await _open_menu(hass, issue_id)
    flows = repairs_flow_manager(hass)
    assert flows is not None

    async def failing_save(_self: Store[Any], _data: object) -> None:
        raise RuntimeError("serialization failed")

    with monkeypatch.context() as patched:
        patched.setattr(Store, "async_save", failing_save)
        result = await flows.async_configure(menu["flow_id"], {"next_step_id": "dismiss"})

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "interrupted_update_clear_failed"
    assert ir.async_get(hass).async_get_issue(DOMAIN, issue_id) is not None
    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_entity_attributes_and_diagnostics_carry_the_allowlisted_block(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    payload_dir: Path,
    shells: Shells,
    spies: Spies,
) -> None:
    entry, _issue_id = await _surface_through_reload(hass)
    state = hass.states.get(_UPDATE_ENTITY)
    assert state is not None
    block = state.attributes["update"]
    assert block["in_progress"] is False
    assert block["interrupted"]["stage"] == "admission"
    assert block["interrupted"]["attempts"] == 2
    diagnostics = await async_get_config_entry_diagnostics(hass, entry)
    assert diagnostics["panels"][entry.entry_id]["update"] == block
    assert await hass.config_entries.async_unload(entry.entry_id)


def test_update_diagnostics_allowlist_accepts_and_rejects() -> None:
    good: dict[str, object] = {
        "in_progress": True,
        "progress": 25,
        "retry": {
            "stage": "admission",
            "attempt": 1,
            "max_attempts": 5,
            "reason": "connection_lost",
            "retry_in_seconds": 5.0,
            "next_attempt_at": "2026-10-23T12:00:05+00:00",
        },
        "last_error": {"stage": "connect", "code": "connect_failed", "attempts": 5},
        "interrupted": None,
    }
    assert update_state.safe_update_diagnostics(good) == good
    assert update_state.safe_update_diagnostics({**good, "host": "10.0.0.5"}) is None
    assert update_state.safe_update_diagnostics({**good, "progress": 101}) is None
    assert update_state.safe_update_diagnostics({**good, "in_progress": 1}) is None
    leaky = update_state.safe_update_diagnostics(
        {**good, "last_error": {"stage": "connect", "code": "10.0.0.5 refused", "attempts": 1}}
    )
    assert leaky is not None and leaky["last_error"] is None
