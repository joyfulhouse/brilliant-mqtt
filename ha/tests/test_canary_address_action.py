"""Explicit current-address actions preserve the original retained provenance."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest
from homeassistant.components import mqtt
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import issue_registry as ir

from custom_components.brilliant_mqtt.const import CONF_HOST, DOMAIN
from custom_components.brilliant_mqtt.entry_data import FleetPanelStore
from custom_components.brilliant_mqtt.fleet_manager import FleetManager
from custom_components.brilliant_mqtt.manager import PanelManager
from custom_components.brilliant_mqtt.panel_provisioner import (
    PanelProvisioner,
    PanelProvisioningError,
)
from custom_components.brilliant_mqtt.provisioning_journal import (
    ProvisioningJournal,
    ProvisioningPhase,
)
from tests.test_fleet_manager import _fleet_entry, _panel
from tests.test_panel_provisioner import (
    _fleet,
    _Harness,
    _record,
    _release_link_snapshot,
)
from tests.test_provisioning_journal import _install_real_store


@pytest.fixture(autouse=True)
def durable_store(hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_real_store(hass, monkeypatch, tmp_path)


async def _setup(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> tuple[PanelManager, PanelProvisioner, ProvisioningJournal, _Harness, list[tuple[str, str]]]:
    panel = _panel("office", "SHA256:office", subentry_id="office", host="new.example")
    entry = _fleet_entry(panel)
    entry.add_to_hass(hass)
    owner = entry.subentries["office"]
    manager = PanelManager(hass, FleetPanelStore(hass, entry, owner), _fleet(), asyncio.Lock())
    journal = ProvisioningJournal(hass)
    record = replace(_record(ProvisioningPhase.STAGED), prior_snapshot=_release_link_snapshot())
    await journal.async_arm(record)
    await journal.async_create(record)
    for phase in (
        ProvisioningPhase.ACTIVATION_PENDING,
        ProvisioningPhase.ACTIVATED,
        ProvisioningPhase.VERIFYING,
        ProvisioningPhase.PENDING_CONFIG_COMMIT,
    ):
        await journal.async_transition(record.transaction_id, phase)
    await journal.async_complete_commit(record.transaction_id, subentry_id="office")
    harness = _Harness()
    monkeypatch.setattr(harness, "journal", journal)
    contacts: list[tuple[str, str]] = []

    def shell_factory(host: str, login: str, pin: str) -> Any:
        assert pin == record.panel_request.public_key
        contacts.append((host, login))
        return harness.shell

    monkeypatch.setattr(harness, "shell_factory", shell_factory)
    provisioner = harness.provisioner()
    monkeypatch.setattr(
        "custom_components.brilliant_mqtt.fleet_manager._get_recovery_provisioner",
        lambda _hass, **kwargs: provisioner,
    )
    return manager, provisioner, journal, harness, contacts


@pytest.mark.parametrize("operation", ["finalize", "rollback"])
@pytest.mark.parametrize("layout", ["release", "absent"])
async def test_explicit_action_uses_current_host_without_retargeting(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, operation: str, layout: str
) -> None:
    manager, _provisioner, journal, harness, contacts = await _setup(hass, monkeypatch)
    original = (await journal.async_retained_records())[0]
    if layout == "absent":
        original = replace(
            original,
            record=replace(
                original.record, prior_snapshot=_record(ProvisioningPhase.STAGED).prior_snapshot
            ),
        )
        await journal._async_retained_save({str(original.record.transaction_id): original})
    await manager.async_canary_operation(operation, str(original.record.transaction_id))
    assert contacts == [("new.example", "office-secret")]
    assert ("identity", "new.example") in harness.events
    assert ("shell_connect",) in harness.events
    remaining = await journal.async_retained(original.record.transaction_id)
    if operation == "finalize":
        assert remaining is None
    else:
        assert remaining is not None and remaining.state == "restored"
        assert remaining.record == original.record
        assert (remaining.recovery_deployment_id is None) is (layout == "absent")
    assert await journal.async_load() is None


@pytest.mark.parametrize("operation", ["finalize", "rollback"])
@pytest.mark.parametrize("boundary", ["initial", "preconnect", "precleanup"])
async def test_owner_disappearing_in_final_retained_read_never_connects(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, operation: str, boundary: str
) -> None:
    manager, _provisioner, journal, harness, _contacts = await _setup(hass, monkeypatch)
    original = (await journal.async_retained_records())[0]
    read = ProvisioningJournal.async_retained
    removed = False

    async def remove_owner(instance: ProvisioningJournal, transaction: Any) -> Any:
        nonlocal removed
        row = await read(instance, transaction)
        entry = cast(FleetPanelStore, manager.store).entry
        reached = (
            boundary == "initial"
            or boundary == "preconnect"
            and ("identity", "new.example") in harness.events
            or boundary == "precleanup"
            and ("shell_connect",) in harness.events
        )
        if reached and not removed:
            removed = True
            assert hass.config_entries.async_remove_subentry(entry, "office")
        return row

    monkeypatch.setattr(ProvisioningJournal, "async_retained", remove_owner)
    with pytest.raises(HomeAssistantError):
        await manager.async_canary_operation(operation, str(original.record.transaction_id))
    assert removed
    assert (("shell_connect",) in harness.events) is (boundary == "precleanup")
    assert not any(event[0] in {"finalize_baseline", "rollback"} for event in harness.events)


@pytest.mark.parametrize("cut", ["connect", "finalize_baseline"])
async def test_owner_removed_after_await_preserves_retained_cleanup_evidence(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, cut: str
) -> None:
    manager, _provisioner, journal, harness, _contacts = await _setup(hass, monkeypatch)
    original = (await journal.async_retained_records())[0]
    target = harness.shell if cut == "connect" else harness.operations
    action: Callable[..., Any] = getattr(target, cut)

    async def remove_owner(*args: Any, **kwargs: Any) -> None:
        await action(*args, **kwargs)
        assert hass.config_entries.async_remove_subentry(
            cast(FleetPanelStore, manager.store).entry, "office"
        )

    monkeypatch.setattr(target, cut, remove_owner)
    with pytest.raises(HomeAssistantError):
        await manager.async_canary_operation("finalize", str(original.record.transaction_id))
    assert await journal.async_retained(original.record.transaction_id) is not None
    if cut == "connect":
        assert not any(event[0] == "finalize_baseline" for event in harness.events)


async def test_changed_retained_row_during_identity_refuses_before_login(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager, provisioner, journal, harness, _contacts = await _setup(hass, monkeypatch)
    original = (await journal.async_retained_records())[0]
    fetch = provisioner._identity_fetcher

    async def changed(host: str) -> Any:
        await journal.async_retained_state(original.record.transaction_id, "restore_requested")
        return await fetch(host)

    monkeypatch.setattr(provisioner, "_identity_fetcher", changed)
    with pytest.raises(HomeAssistantError):
        await manager.async_canary_operation("finalize", str(original.record.transaction_id))
    assert ("shell_connect",) not in harness.events


@pytest.mark.parametrize("state", ["armed", "restore_requested", "rollback_failed", "restored"])
async def test_unfinished_or_unproven_rows_refuse_without_restarting_restore(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, state: str
) -> None:
    manager, _provisioner, journal, harness, _contacts = await _setup(hass, monkeypatch)
    original = (await journal.async_retained_records())[0]
    if state == "armed":
        await journal._async_retained_save(
            {str(original.record.transaction_id): replace(original, state="armed")}
        )
    elif state == "rollback_failed":
        await journal.async_retained_state(original.record.transaction_id, "restore_requested")
        await journal.async_retained_state(original.record.transaction_id, state)
    else:
        await journal.async_retained_state(original.record.transaction_id, "restore_requested")
        if state == "restored":
            await journal.async_retained_state(original.record.transaction_id, state)
    before = await journal.async_retained(original.record.transaction_id)
    with pytest.raises(HomeAssistantError):
        await manager.async_canary_operation("rollback", str(original.record.transaction_id))
    assert ("shell_connect",) not in harness.events
    assert await journal.async_retained(original.record.transaction_id) == before


async def test_startup_publishes_one_repair_without_contact_or_retargeting(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager, _provisioner, journal, harness, contacts = await _setup(hass, monkeypatch)
    before = (await journal.async_retained_records())[0]
    entry = cast(FleetPanelStore, manager.store).entry
    runtime = FleetManager(hass, entry)
    await runtime._async_prepare_provisioning()
    issues = [issue for (domain, _), issue in ir.async_get(hass).issues.items() if domain == DOMAIN]
    assert len(issues) == 1
    assert issues[0].translation_key == "canary_new_address"
    assert issues[0].is_fixable
    assert await journal.async_retained(before.record.transaction_id) == before
    assert contacts == []
    assert ("shell_connect",) not in harness.events


@pytest.mark.parametrize("owner_change", ["reverted", "removed"])
async def test_interrupted_action_is_never_replayed_on_startup(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, owner_change: str
) -> None:
    manager, provisioner, journal, harness, contacts = await _setup(hass, monkeypatch)
    retained = (await journal.async_retained_records())[0]
    harness.observer.fail_wait = True
    with pytest.raises(HomeAssistantError):
        await manager.async_canary_operation("rollback", str(retained.record.transaction_id))
    active = await journal.async_load()
    assert active is not None
    assert active.panel_request.host == retained.record.panel_request.host
    assert active.last_error is not None
    assert active.last_error.code == "canary_manual_action_required"
    store = cast(FleetPanelStore, manager.store)
    if owner_change == "reverted":
        store.update_data({**store.data, CONF_HOST: retained.record.panel_request.host})
    else:
        assert hass.config_entries.async_remove_subentry(store.entry, "office")
    contacts.clear()
    runtime = FleetManager(hass, store.entry)
    await runtime._async_prepare_provisioning()
    assert runtime._recovery_task is None
    assert contacts == []
    with pytest.raises(Exception, match="canary_manual_action_required"):
        await provisioner.async_recover()
    assert contacts == []
    assert await journal.async_load() == active
    if owner_change == "reverted":
        with pytest.raises(HomeAssistantError, match="canary_manual_action_required"):
            await manager.async_canary_operation("rollback", str(retained.record.transaction_id))
        assert contacts == []
        assert await journal.async_load() == active


@pytest.mark.parametrize("operation", ["finalize", "rollback"])
async def test_repair_only_executes_the_explicitly_selected_action(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    from custom_components.brilliant_mqtt.repairs import async_create_fix_flow

    manager, _provisioner, journal, _harness, contacts = await _setup(hass, monkeypatch)
    store = cast(FleetPanelStore, manager.store)
    retained = (await journal.async_retained_records())[0]
    await FleetManager(hass, store.entry)._async_prepare_provisioning()
    issue = next(
        issue for (domain, _), issue in ir.async_get(hass).issues.items() if domain == DOMAIN
    )
    flow = await async_create_fix_flow(hass, issue.issue_id, issue.data)
    flow.hass = hass
    initial = await flow.async_step_init()
    assert initial["type"] == "form"
    assert contacts == []
    result = await flow.async_step_init({"action": operation})
    assert result["type"] == ("create_entry" if operation == "finalize" else "abort")
    assert contacts == [("new.example", "office-secret")]
    assert (await journal.async_retained(retained.record.transaction_id) is None) is (
        operation == "finalize"
    )


async def test_moved_active_update_is_deferred_without_blocking_startup(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager, _provisioner, journal, harness, contacts = await _setup(hass, monkeypatch)
    retained = (await journal.async_retained_records())[0]
    from custom_components.brilliant_mqtt.provisioning_journal import ProvisioningOperation

    active = replace(retained.record, operation=ProvisioningOperation.UPDATE)
    await journal._async_retained_save(
        {str(active.transaction_id): replace(retained, record=active)}
    )
    await journal.async_create(active)
    entry = cast(FleetPanelStore, manager.store).entry
    runtime = FleetManager(hass, entry)
    await runtime._async_prepare_provisioning()
    assert await journal.async_load() == active
    assert runtime._recovery_task is None
    assert contacts == []
    assert ("shell_connect",) not in harness.events


async def test_queued_old_host_finalize_cannot_replay_after_address_edit(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> None:
    from custom_components.brilliant_mqtt import fleet_manager

    _manager, _provisioner, journal, harness, contacts = await _setup(hass, monkeypatch)
    retained = (await journal.async_retained_records())[0]
    await journal.async_retained_state(retained.record.transaction_id, "finalizing")
    monkeypatch.setattr(fleet_manager, "AsyncsshShell", lambda *args: contacts.append(args))
    provisioner = fleet_manager._build_panel_provisioner(hass, harness.fetch_identity)
    with pytest.raises(PanelProvisioningError, match="provisioning_recovery_required"):
        await provisioner.async_recover(expected_record=retained.record)
    assert contacts == []
    assert await journal.async_retained(retained.record.transaction_id) is not None


async def test_cancelled_cleanup_settles_without_removing_retained_row(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager, provisioner, journal, harness, _contacts = await _setup(hass, monkeypatch)
    retained = (await journal.async_retained_records())[0]
    entered = asyncio.Event()
    release = asyncio.Event()
    settling = asyncio.Event()

    async def finalize(*args: Any) -> None:
        entered.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            settling.set()
            await release.wait()
            raise

    monkeypatch.setattr(harness.operations, "finalize_baseline", finalize)
    action = asyncio.create_task(
        manager.async_canary_operation("finalize", str(retained.record.transaction_id))
    )
    await entered.wait()
    action.cancel()
    await asyncio.wait_for(settling.wait(), 2)
    assert provisioner._lock.locked()
    assert not action.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await action
    assert await journal.async_retained(retained.record.transaction_id) == retained
    assert not provisioner._lock.locked()
    assert ("shell_close",) in harness.events
    assert (
        ir.async_get(hass).async_get_issue(
            DOMAIN, f"canary_new_address_{retained.record.transaction_id.hex}"
        )
        is not None
    )


@pytest.mark.parametrize("problem", ["pin", "slug", "duplicate", "duplicate_key", "active"])
async def test_ambiguous_owner_or_active_operation_refuses_typed_before_contact(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, problem: str
) -> None:
    from custom_components.brilliant_mqtt.const import CONF_PANEL, CONF_SSH_HOST_KEY
    from custom_components.brilliant_mqtt.shell import HostIdentity
    from tests.test_fleet_manager import _OTHER_FINGERPRINT, _OTHER_PUBLIC_KEY

    manager, provisioner, journal, harness, contacts = await _setup(hass, monkeypatch)
    retained = (await journal.async_retained_records())[0]
    store = cast(FleetPanelStore, manager.store)
    if problem == "pin":
        monkeypatch.setattr(
            provisioner,
            "_identity_fetcher",
            AsyncMock(return_value=HostIdentity(_OTHER_PUBLIC_KEY, _OTHER_FINGERPRINT)),
        )
    elif problem == "slug":
        hass.config_entries.async_update_subentry(
            store.entry, store.subentry, data={**store.data, CONF_PANEL: "another"}
        )
    elif problem in {"duplicate", "duplicate_key"}:
        duplicate = _panel("office", "SHA256:office", subentry_id="duplicate")
        other = _fleet_entry(duplicate)
        other.add_to_hass(hass)
        if problem == "duplicate_key":
            duplicate = other.subentries["duplicate"]
            hass.config_entries.async_update_subentry(
                other,
                duplicate,
                data={**duplicate.data, CONF_PANEL: "another", "identity_fingerprint": "old"},
            )
    else:
        await journal.async_create(retained.record)
    before = await journal.async_retained(retained.record.transaction_id)
    with pytest.raises(
        HomeAssistantError, match="canary_(owner|record)_changed|panel_identity_mismatch"
    ):
        await manager.async_canary_operation("finalize", str(retained.record.transaction_id))
    assert contacts == []
    assert ("shell_connect",) not in harness.events
    assert await journal.async_retained(retained.record.transaction_id) == before
    assert store.data[CONF_SSH_HOST_KEY] == retained.record.panel_request.public_key


@pytest.mark.parametrize("problem", ["ownerless", "absent_evidence", "staged_recovery", "phase"])
async def test_inconsistent_or_separately_granted_provenance_is_never_adopted(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, problem: str
) -> None:
    from custom_components.brilliant_mqtt.provisioning_journal import RetainedRollback

    manager, _provisioner, journal, harness, contacts = await _setup(hass, monkeypatch)
    retained = (await journal.async_retained_records())[0]
    if problem == "ownerless":
        row = RetainedRollback(retained.record, "finalizing", "a" * 32, "ownerless_restored")
    elif problem == "absent_evidence":
        row = RetainedRollback(
            replace(
                retained.record, prior_snapshot=_record(ProvisioningPhase.STAGED).prior_snapshot
            ),
            "restored",
            "a" * 32,
        )
    elif problem == "phase":
        row = replace(retained, record=replace(retained.record, phase=ProvisioningPhase.VERIFYING))
    else:
        row = RetainedRollback(retained.record, "restore_requested")
    await journal._async_retained_save({str(row.record.transaction_id): row})
    with pytest.raises(HomeAssistantError, match="canary_action_unavailable"):
        await manager.async_canary_operation("finalize", str(row.record.transaction_id))
    assert contacts == []
    assert ("shell_connect",) not in harness.events
    assert await journal.async_retained(row.record.transaction_id) == row


@pytest.mark.parametrize("cut", ["rollback_snapshot", "cleanup_staged"])
async def test_cancelled_rollback_keeps_manual_journal_and_verified_evidence(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, cut: str
) -> None:
    manager, provisioner, journal, harness, _contacts = await _setup(hass, monkeypatch)
    retained = (await journal.async_retained_records())[0]
    entered = asyncio.Event()
    release = asyncio.Event()
    settling = asyncio.Event()
    operation: Callable[..., Any] = getattr(harness.operations, cut)

    async def blocked(*args: Any, **kwargs: Any) -> None:
        await operation(*args, **kwargs)
        entered.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            settling.set()
            await release.wait()
            raise

    monkeypatch.setattr(harness.operations, cut, blocked)
    action = asyncio.create_task(
        manager.async_canary_operation("rollback", str(retained.record.transaction_id))
    )
    await entered.wait()
    action.cancel()
    await asyncio.wait_for(settling.wait(), 2)
    assert provisioner._lock.locked()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await action
    active = await journal.async_load()
    current = await journal.async_retained(retained.record.transaction_id)
    assert active is not None and active.last_error is not None
    assert active.last_error.code == "canary_manual_action_required"
    assert current is not None and current.record == retained.record
    assert current.state == ("restored" if cut == "cleanup_staged" else "restore_requested")
    assert (current.recovery_deployment_id is not None) is (cut == "cleanup_staged")
    assert not provisioner._lock.locked()


@pytest.mark.parametrize("change", ["replacement", "login"])
async def test_owner_change_during_fresh_identity_does_not_adopt_new_access(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    from custom_components.brilliant_mqtt.const import CONF_ROOT_PASSWORD

    manager, provisioner, journal, harness, contacts = await _setup(hass, monkeypatch)
    retained = (await journal.async_retained_records())[0]
    store = cast(FleetPanelStore, manager.store)
    fetch = provisioner._identity_fetcher

    async def changed(host: str) -> Any:
        if change == "replacement":
            assert hass.config_entries.async_remove_subentry(store.entry, "office")
            replacement = _panel(
                "office", "SHA256:office", subentry_id="office", host="new.example"
            )
            hass.config_entries.async_add_subentry(store.entry, replacement)
        else:
            store.update_data({**store.data, CONF_ROOT_PASSWORD: "new-login"})
        return await fetch(host)

    monkeypatch.setattr(provisioner, "_identity_fetcher", changed)
    with pytest.raises(HomeAssistantError, match="canary_owner_changed"):
        await manager.async_canary_operation("finalize", str(retained.record.transaction_id))
    assert contacts == []
    assert ("shell_connect",) not in harness.events
    assert await journal.async_retained(retained.record.transaction_id) == retained


async def test_interrupted_moved_action_with_removed_owner_keeps_healthy_sibling_running(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> None:
    from custom_components.brilliant_mqtt import fleet_manager
    from custom_components.brilliant_mqtt.const import CONF_SCENE_PANEL

    manager, _provisioner, journal, harness, contacts = await _setup(hass, monkeypatch)
    retained = (await journal.async_retained_records())[0]
    harness.observer.fail_wait = True
    with pytest.raises(HomeAssistantError):
        await manager.async_canary_operation("rollback", str(retained.record.transaction_id))
    store = cast(FleetPanelStore, manager.store)
    sibling = _panel("kitchen", "SHA256:kitchen", subentry_id="kitchen")
    hass.config_entries.async_add_subentry(store.entry, sibling)
    hass.config_entries.async_update_entry(
        store.entry, data={**store.entry.data, CONF_SCENE_PANEL: "kitchen"}
    )
    assert hass.config_entries.async_remove_subentry(store.entry, "office")
    monkeypatch.setattr(PanelManager, "async_setup", AsyncMock())
    monkeypatch.setattr(PanelManager, "async_shutdown", AsyncMock())
    monkeypatch.setattr(mqtt, "is_connected", lambda _hass: False)
    monkeypatch.setattr(mqtt, "async_subscribe_connection_status", lambda *args: Mock())
    monkeypatch.setattr(fleet_manager, "async_purge_stale_discovery_configs", AsyncMock())
    runtime = FleetManager(hass, store.entry)
    contacts.clear()
    try:
        await runtime.async_setup()
        assert "kitchen" in runtime.panels
        assert runtime._recovery_task is None
        assert contacts == []
    finally:
        await runtime.async_shutdown()


@pytest.mark.parametrize("fault", ["owner", "record", "cancel"])
async def test_final_retained_removal_rechecks_exact_row_and_authority_after_storage_read(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    manager, _provisioner, journal, harness, _contacts = await _setup(hass, monkeypatch)
    retained = (await journal.async_retained_records())[0]
    original_remove = ProvisioningJournal.async_remove_retained
    original_load = ProvisioningJournal._async_retained_load
    inside_remove = False
    reached = False
    replacement = replace(retained, state="rollback_failed")

    async def remove(instance: ProvisioningJournal, *args: Any, **kwargs: Any) -> None:
        nonlocal inside_remove
        inside_remove = True
        try:
            await original_remove(instance, *args, **kwargs)
        finally:
            inside_remove = False

    async def load(instance: ProvisioningJournal) -> Any:
        nonlocal reached
        rows = await original_load(instance)
        if inside_remove and not reached:
            reached = True
            if fault == "owner":
                entry = cast(FleetPanelStore, manager.store).entry
                assert hass.config_entries.async_remove_subentry(entry, "office")
            elif fault == "record":
                rows[str(retained.record.transaction_id)] = replacement
                await instance._async_retained_save(rows)
            else:
                task = asyncio.current_task()
                assert task is not None
                task.cancel()
                await asyncio.sleep(0)
        return rows

    monkeypatch.setattr(ProvisioningJournal, "async_remove_retained", remove)
    monkeypatch.setattr(ProvisioningJournal, "_async_retained_load", load)
    error = asyncio.CancelledError if fault == "cancel" else HomeAssistantError
    with pytest.raises(error):
        await manager.async_canary_operation("finalize", str(retained.record.transaction_id))
    assert reached
    assert sum(event[0] == "finalize_baseline" for event in harness.events) == 1
    assert await journal.async_retained(retained.record.transaction_id) == (
        replacement if fault == "record" else retained
    )


@pytest.mark.parametrize(
    "phase", [ProvisioningPhase.ROLLBACK_PENDING, ProvisioningPhase.ROLLED_BACK]
)
async def test_rollback_completion_rechecks_owner_after_final_retained_proof(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, phase: ProvisioningPhase
) -> None:
    manager, _provisioner, journal, _harness, _contacts = await _setup(hass, monkeypatch)
    retained = (await journal.async_retained_records())[0]
    original_complete = ProvisioningJournal.async_complete_rollback
    original_load = ProvisioningJournal._async_retained_load
    inside_complete = False
    reached = False

    async def complete(instance: ProvisioningJournal, *args: Any, **kwargs: Any) -> Any:
        nonlocal inside_complete
        inside_complete = True
        try:
            return await original_complete(instance, *args, **kwargs)
        finally:
            inside_complete = False

    async def load(instance: ProvisioningJournal) -> Any:
        nonlocal reached
        rows = await original_load(instance)
        active = instance._coordinator.record
        if inside_complete and not reached and active is not None and active.phase is phase:
            reached = True
            entry = cast(FleetPanelStore, manager.store).entry
            assert hass.config_entries.async_remove_subentry(entry, "office")
        return rows

    monkeypatch.setattr(ProvisioningJournal, "async_complete_rollback", complete)
    monkeypatch.setattr(ProvisioningJournal, "_async_retained_load", load)
    with pytest.raises(HomeAssistantError, match="canary_owner_changed"):
        await manager.async_canary_operation("rollback", str(retained.record.transaction_id))
    assert reached
    active = await journal.async_load()
    row = await journal.async_retained(retained.record.transaction_id)
    assert active is not None and active.phase is phase
    assert (
        active.last_error is not None and active.last_error.code == "canary_manual_action_required"
    )
    assert row is not None and row.record == retained.record
    assert row.state == "restored" and row.recovery_deployment_id is not None


async def test_interruption_after_restore_intent_already_has_manual_only_journal(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager, _provisioner, journal, _harness, contacts = await _setup(hass, monkeypatch)
    retained = (await journal.async_retained_records())[0]
    original_state = ProvisioningJournal._async_retained_state
    interrupted = False

    async def state(
        instance: ProvisioningJournal, transaction: Any, value: str, **kwargs: Any
    ) -> None:
        nonlocal interrupted
        await original_state(instance, transaction, value, **kwargs)
        if value == "restore_requested" and not interrupted:
            interrupted = True
            raise asyncio.CancelledError

    monkeypatch.setattr(ProvisioningJournal, "_async_retained_state", state)
    with pytest.raises(asyncio.CancelledError):
        await manager.async_canary_operation("rollback", str(retained.record.transaction_id))
    assert interrupted
    active = await journal.async_load()
    assert active is not None and active.last_error is not None
    assert active.last_error.code == "canary_manual_action_required"
    store = cast(FleetPanelStore, manager.store)
    store.update_data({**store.data, CONF_HOST: retained.record.panel_request.host})
    contacts.clear()
    runtime = FleetManager(hass, store.entry)
    await runtime._async_prepare_provisioning()
    assert runtime._recovery_task is None
    assert contacts == []
