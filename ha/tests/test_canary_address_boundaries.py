"""Current-address grants stop at actual transport and storage boundaries."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from functools import partial
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest
from homeassistant.components import mqtt
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import issue_registry as ir

from custom_components.brilliant_mqtt import fleet_manager as fm
from custom_components.brilliant_mqtt import panel_ops, panel_provisioner, shell
from custom_components.brilliant_mqtt.canary_actions import (
    address_issue_id,
    async_defer_address_action,
)
from custom_components.brilliant_mqtt.const import CONF_HOST, DOMAIN
from custom_components.brilliant_mqtt.entry_data import FleetPanelStore
from custom_components.brilliant_mqtt.manager import PanelManager
from custom_components.brilliant_mqtt.panel_provisioner import (
    PanelProvisioningError,
    TransactionLookup,
    TransactionLookupState,
)
from custom_components.brilliant_mqtt.provisioning_journal import (
    ProvisioningJournal,
    ProvisioningJournalError,
    ProvisioningOperation,
    ProvisioningPhase,
    StoredJournalError,
)
from custom_components.brilliant_mqtt.repairs import async_create_fix_flow
from custom_components.brilliant_mqtt.shell import HostIdentity, RunResult
from tests.test_canary_address_action import _setup
from tests.test_fleet_manager import (
    _OTHER_FINGERPRINT,
    _OTHER_PUBLIC_KEY,
    _empty_fleet_entry,
    _fleet_entry,
    _panel,
    _pending_record,
)
from tests.test_panel_provisioner import _fleet, _Harness, _record
from tests.test_provisioning_journal import _install_real_store
from tests.test_services import _setup_fleet_entry


@pytest.fixture(autouse=True)
def durable_store(hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_real_store(hass, monkeypatch, tmp_path)


class _Process:
    def __init__(self, result: RunResult | None = None, *, blocked: bool = False) -> None:
        self.result = result if result is not None else RunResult(0, "", "")
        self.entered = asyncio.Event()
        self.terminated = asyncio.Event()
        self.release = asyncio.Event()
        if not blocked:
            self.release.set()

    @property
    def running(self) -> bool:
        return not self.release.is_set()

    async def wait(self) -> RunResult:
        self.entered.set()
        await self.release.wait()
        return self.result

    def terminate(self) -> None:
        self.terminated.set()


@pytest.mark.parametrize("manual", [False, True])
async def test_guarded_restore_keeps_same_transaction_and_manual_slot_rules(
    hass: HomeAssistant, manual: bool
) -> None:
    journal = ProvisioningJournal(hass)
    record = _record(ProvisioningPhase.STAGED)
    transaction = record.transaction_id
    await journal.async_create(record)
    await journal.async_transition(transaction, ProvisioningPhase.ACTIVATION_PENDING)
    if manual:
        await journal.async_retained_state(transaction, "soak")
    else:
        await journal.async_request_restore(transaction)
        await journal.async_begin_restore(transaction, record.panel_request.root_password)
    retained = await journal.async_retained(transaction)
    active = await journal.async_load()
    assert retained is not None
    authorize = Mock()
    if manual:
        with pytest.raises(ProvisioningJournalError, match="journal_transaction_mismatch"):
            await journal.async_begin_restore(
                transaction,
                record.panel_request.root_password,
                expected=retained,
                authorize=authorize,
                manual_action=True,
            )
        authorize.assert_not_called()
    else:
        await journal.async_begin_restore(
            transaction,
            record.panel_request.root_password,
            expected=retained,
            authorize=authorize,
        )
        authorize.assert_called_once_with()
    assert await journal.async_load() == active
    assert await journal.async_retained(transaction) == retained


@pytest.mark.parametrize("initial_state", ["rollback_failed", "restored"])
@pytest.mark.parametrize(
    "cut",
    [
        "retained_intent",
        "active_intent",
        "active_intent_final",
        "diagnostic",
        "diagnostic_retained",
    ],
)
async def test_generic_restore_intent_preserves_evidence_at_each_final_read(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, initial_state: str, cut: str
) -> None:
    journal = ProvisioningJournal(hass)
    record = _record(ProvisioningPhase.STAGED)
    transaction = record.transaction_id
    entry = _empty_fleet_entry()
    entry.add_to_hass(hass)
    await journal.async_create(record)
    await journal.async_transition(transaction, ProvisioningPhase.ACTIVATION_PENDING)
    await journal.async_request_restore(transaction)
    await journal.async_begin_restore(transaction, record.panel_request.root_password)
    await journal.async_retained_state(transaction, initial_state)
    harness = _Harness()
    monkeypatch.setattr(fm, "AsyncsshShell", lambda *args: harness.shell)
    provisioner = fm._build_panel_provisioner(hass, AsyncMock())
    monkeypatch.setattr(provisioner, "_operations", harness.operations)
    target = {
        "retained_intent": "_async_retained_state",
        "active_intent": "async_begin_restore",
        "active_intent_final": "async_begin_restore",
        "diagnostic": "async_record_error",
        "diagnostic_retained": "async_record_error",
    }[cut]
    mutate = getattr(ProvisioningJournal, target)
    read = ProvisioningJournal._async_retained_load
    current = ProvisioningJournal._async_current
    inside = False
    reads = 0
    captured: list[tuple[Any, Any, list[tuple[object, ...]]]] = []

    def insert(instance: ProvisioningJournal, row: Any) -> None:
        captured.append((instance._coordinator.record, row, list(harness.events)))
        hass.config_entries.async_add_subentry(
            entry, _panel("office", "SHA256:office", subentry_id="new", host="new.example")
        )

    async def enter(instance: ProvisioningJournal, *args: Any, **kwargs: Any) -> Any:
        nonlocal inside
        inside = True
        try:
            return await mutate(instance, *args, **kwargs)
        finally:
            inside = False

    async def final_read(instance: ProvisioningJournal) -> Any:
        nonlocal reads
        rows = await read(instance)
        if inside:
            reads += 1
        if (
            inside
            and not captured
            and cut != "diagnostic"
            and (cut != "active_intent_final" or reads == 2)
        ):
            insert(instance, rows[str(transaction)])
        return rows

    async def current_read(instance: ProvisioningJournal, *args: Any) -> Any:
        result = await current(instance, *args)
        if inside and not captured and cut == "diagnostic":
            insert(instance, (await read(instance))[str(transaction)])
        return result

    monkeypatch.setattr(ProvisioningJournal, target, enter)
    monkeypatch.setattr(ProvisioningJournal, "_async_retained_load", final_read)
    monkeypatch.setattr(ProvisioningJournal, "_async_current", current_read)
    with pytest.raises(PanelProvisioningError):
        await provisioner.async_recover()
    assert captured
    active, retained, events = captured[0]
    assert await journal.async_load() == active
    assert await journal.async_retained(transaction) == retained
    assert [event for event in harness.events[len(events) :] if event != ("shell_close",)] == []
    assert [
        issue_id
        for (domain, issue_id) in ir.async_get(hass).issues
        if domain == DOMAIN and issue_id.startswith("canary_new_address_")
    ] == [address_issue_id(retained)]


@pytest.mark.parametrize("route", ["generic", "fleet"])
@pytest.mark.parametrize("cut", ["initial_read", "final_read"])
async def test_direct_restore_intent_defers_late_owner_and_starts_sibling(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, route: str, cut: str
) -> None:
    from custom_components.brilliant_mqtt.const import CONF_SCENE_PANEL

    journal = ProvisioningJournal(hass)
    record = _record(ProvisioningPhase.STAGED)
    transaction = record.transaction_id
    entry = _empty_fleet_entry()
    entry.add_to_hass(hass)
    sibling = _panel("kitchen", "SHA256:kitchen", subentry_id="kitchen")
    hass.config_entries.async_add_subentry(entry, sibling)
    hass.config_entries.async_update_entry(entry, data={**entry.data, CONF_SCENE_PANEL: "kitchen"})
    await journal.async_create(record)
    await journal.async_transition(transaction, ProvisioningPhase.ACTIVATION_PENDING)
    await journal.async_request_restore(transaction)
    await journal.async_begin_restore(transaction, record.panel_request.root_password)
    await journal.async_record_error(
        transaction, StoredJournalError(stage="recovery", code="prior_failure")
    )
    harness = _Harness()
    factory = Mock(return_value=harness.shell)
    monkeypatch.setattr(fm, "AsyncsshShell", factory)
    provisioner = fm._build_panel_provisioner(hass, AsyncMock())
    monkeypatch.setattr(provisioner, "_operations", harness.operations)
    mutate = ProvisioningJournal.async_begin_restore
    read = ProvisioningJournal._async_retained_load
    inside = False
    reads = 0
    captured: list[tuple[Any, Any]] = []

    async def enter(instance: ProvisioningJournal, *args: Any, **kwargs: Any) -> Any:
        nonlocal inside
        inside = True
        try:
            return await mutate(instance, *args, **kwargs)
        finally:
            inside = False

    async def final_read(instance: ProvisioningJournal) -> Any:
        nonlocal reads
        rows = await read(instance)
        if inside:
            reads += 1
        if inside and not captured and (cut == "initial_read" or reads == 2):
            captured.append((instance._coordinator.record, rows[str(transaction)]))
            hass.config_entries.async_add_subentry(
                entry, _panel("office", "SHA256:office", subentry_id="new", host="new.example")
            )
        return rows

    monkeypatch.setattr(ProvisioningJournal, "async_begin_restore", enter)
    monkeypatch.setattr(ProvisioningJournal, "_async_retained_load", final_read)
    if route == "generic":
        with pytest.raises(PanelProvisioningError):
            await provisioner.async_recover()
    else:
        started: list[str] = []

        async def setup(manager: PanelManager) -> None:
            started.append(manager.store.panel_id)

        monkeypatch.setattr(PanelManager, "async_setup", setup)
        monkeypatch.setattr(PanelManager, "async_shutdown", AsyncMock())
        monkeypatch.setattr(mqtt, "is_connected", lambda _hass: False)
        monkeypatch.setattr(mqtt, "async_subscribe_connection_status", lambda *args: Mock())
        monkeypatch.setattr(fm, "async_purge_stale_discovery_configs", AsyncMock())
        runtime = fm.FleetManager(hass, entry)
        try:
            await runtime.async_setup()
            assert "kitchen" in started
            assert runtime._recovery_task is None
        finally:
            await runtime.async_shutdown()
    assert captured
    assert await journal.async_load() == captured[0][0]
    assert await journal.async_retained(transaction) == captured[0][1]
    factory.assert_not_called()
    assert [
        issue_id
        for (domain, issue_id) in ir.async_get(hass).issues
        if domain == DOMAIN and issue_id.startswith("canary_new_address_")
    ] == [address_issue_id(captured[0][1])]


@pytest.mark.parametrize("cut", ["queued", "artifacts", "retained_read"])
async def test_onboarding_worker_rechecks_moved_owner_at_final_admission(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, cut: str
) -> None:
    journal = ProvisioningJournal(hass)
    record = _record(ProvisioningPhase.STAGED)
    transaction = record.transaction_id
    entry = _empty_fleet_entry()
    entry.add_to_hass(hass)
    await journal.async_create(record)
    await journal.async_transition(transaction, ProvisioningPhase.ACTIVATION_PENDING)
    await journal.async_request_restore(transaction)
    await journal.async_begin_restore(transaction, record.panel_request.root_password)
    retained, active = await journal.async_retained(transaction), await journal.async_load()
    assert retained is not None and active is not None
    harness, contacts = _Harness(), []

    def factory(host: str, *args: Any) -> shell.PanelShell:
        contacts.append(host)
        return harness.shell

    monkeypatch.setattr(fm, "AsyncsshShell", factory)
    monkeypatch.setattr(panel_ops, "rollback_snapshot", harness.operations.rollback_snapshot)
    monkeypatch.setattr(panel_ops, "cleanup_staged", harness.operations.cleanup_staged)
    provisioner = fm._build_panel_provisioner(hass, AsyncMock())
    monkeypatch.setattr(fm, "_get_recovery_provisioner", lambda *args, **kwargs: provisioner)
    artifacts = provisioner._async_recovery_artifacts
    read = ProvisioningJournal.async_retained
    artifact_done = False
    inserted = False

    def insert() -> None:
        nonlocal inserted
        inserted = True
        hass.config_entries.async_add_subentry(
            entry, _panel("office", "SHA256:office", subentry_id="new", host="new.example")
        )

    async def artifact_cut(*args: Any) -> Any:
        nonlocal artifact_done
        result = await artifacts(*args)
        artifact_done = True
        if cut == "artifacts":
            insert()
        return result

    async def read_cut(instance: ProvisioningJournal, *args: Any) -> Any:
        result = await read(instance, *args)
        if cut == "retained_read" and artifact_done and not inserted:
            insert()
        return result

    monkeypatch.setattr(provisioner, "_async_recovery_artifacts", artifact_cut)
    monkeypatch.setattr(ProvisioningJournal, "async_retained", read_cut)
    fleet = fm.FleetManager(hass, entry)
    monkeypatch.setattr(fleet, "_async_schedule_reload_once", Mock())
    async with provisioner._lock:
        await fleet._async_prepare_provisioning()
        worker = fleet._recovery_task
        assert worker is not None
        if cut == "queued":
            insert()
            await fleet._async_prepare_provisioning()
    await worker
    assert inserted
    assert contacts == []
    assert await journal.async_load() == active
    assert await journal.async_retained(transaction) == retained
    issues = ir.async_get(hass).issues
    assert [
        issue_id
        for (domain, issue_id) in issues
        if domain == DOMAIN and issue_id.startswith("canary_new_address_")
    ] == [address_issue_id(retained)]


async def test_completed_ownerless_compensation_cannot_be_adopted(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = ProvisioningJournal(hass)
    record = _record(ProvisioningPhase.STAGED)
    transaction = record.transaction_id
    await journal.async_create(record)
    await journal.async_transition(transaction, ProvisioningPhase.ACTIVATION_PENDING)
    await journal.async_request_restore(transaction)
    await journal.async_begin_restore(transaction, record.panel_request.root_password)
    await journal.async_complete_absent_rollback(transaction)
    retained = await journal.async_retained(transaction)
    assert retained is not None and retained.ownerless_finalization_allowed
    entry = _fleet_entry(_panel("office", "SHA256:office", subentry_id="new", host="new.example"))
    entry.add_to_hass(hass)
    manager = PanelManager(
        hass, FleetPanelStore(hass, entry, entry.subentries["new"]), _fleet(), asyncio.Lock()
    )
    verify, factory = AsyncMock(), Mock()
    monkeypatch.setattr(fm, "async_verify_host_identity", verify)
    monkeypatch.setattr(fm, "AsyncsshShell", factory)
    with pytest.raises(HomeAssistantError, match="canary_action_unavailable"):
        await manager.async_canary_operation("finalize", str(transaction))
    verify.assert_not_awaited()
    factory.assert_not_called()
    assert await journal.async_retained(transaction) == retained
    assert await journal.async_load() is None


@pytest.mark.parametrize("fault", ["owner", "row"])
async def test_real_finalize_dispatch_checks_after_source_preparation(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    manager, provisioner, journal, harness, _contacts = await _setup(hass, monkeypatch)
    retained = (await journal.async_retained_records())[0]
    transaction = retained.record.transaction_id

    async def source() -> str:
        if fault == "owner":
            assert hass.config_entries.async_remove_subentry(
                cast(FleetPanelStore, manager.store).entry, "office"
            )
        else:
            await journal.async_retained_state(transaction, "finalizing")
        return ""

    monkeypatch.setattr(panel_ops, "_identity_source", source)
    monkeypatch.setattr(provisioner, "_operations", panel_ops)
    start = AsyncMock(return_value=_Process())
    monkeypatch.setattr(harness.shell, "start", start)
    with pytest.raises(
        HomeAssistantError, match=f"canary_{'owner' if fault == 'owner' else 'record'}_changed"
    ):
        await manager.async_canary_operation("finalize", str(transaction))
    start.assert_not_awaited()
    assert await journal.async_retained(transaction) is not None


@pytest.mark.parametrize("verify_cut", [1, 2])
@pytest.mark.parametrize("fault", ["owner", "row"])
async def test_real_rollback_checks_after_each_live_verification(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, verify_cut: int, fault: str
) -> None:
    manager, provisioner, journal, harness, _contacts = await _setup(hass, monkeypatch)
    retained = (await journal.async_retained_records())[0]
    monkeypatch.setattr(provisioner, "_operations", panel_ops)
    reads = 0

    async def verify(*args: Any) -> None:
        nonlocal reads
        reads += 1
        if reads == verify_cut:
            if fault == "owner":
                assert hass.config_entries.async_remove_subentry(
                    cast(FleetPanelStore, manager.store).entry, "office"
                )
            else:
                await journal.async_retained_state(
                    retained.record.transaction_id, "rollback_failed"
                )

    monkeypatch.setattr(panel_ops, "verify_baseline", verify)
    start, run = AsyncMock(return_value=_Process()), AsyncMock(return_value=RunResult(0, "", ""))
    monkeypatch.setattr(harness.shell, "start", start)
    monkeypatch.setattr(harness.shell, "run", run)
    with pytest.raises(
        HomeAssistantError, match=f"canary_{'owner' if fault == 'owner' else 'record'}_changed"
    ):
        await manager.async_canary_operation("rollback", str(retained.record.transaction_id))
    assert reads == verify_cut
    start.assert_not_awaited()
    run.assert_not_awaited()
    active = await journal.async_load()
    assert (active is None) is (verify_cut == 1)
    assert await journal.async_retained(retained.record.transaction_id) is not None


@pytest.mark.parametrize("interruption", ["cancel", "deadline"])
async def test_real_process_is_terminated_then_settled_before_unlock(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, interruption: str
) -> None:
    manager, provisioner, journal, harness, _contacts = await _setup(hass, monkeypatch)
    retained = (await journal.async_retained_records())[0]
    process = _Process(blocked=True)
    closed = asyncio.Event()
    close = harness.shell.close

    async def observe_close() -> None:
        await close()
        closed.set()

    monkeypatch.setattr(harness.shell, "close", observe_close)
    monkeypatch.setattr(harness.shell, "start", AsyncMock(return_value=process))
    monkeypatch.setattr(panel_ops, "_identity_source", AsyncMock(return_value=""))
    monkeypatch.setattr(provisioner, "_operations", panel_ops)
    if interruption == "deadline":
        monkeypatch.setattr(panel_provisioner, "RECOVERY_TIMEOUT_SECONDS", 0.1)
    action = asyncio.create_task(
        manager.async_canary_operation("finalize", str(retained.record.transaction_id))
    )
    try:
        await asyncio.wait_for(process.entered.wait(), 2)
        if interruption == "cancel":
            action.cancel()
        await asyncio.wait_for(process.terminated.wait(), 2)
        await asyncio.wait_for(closed.wait(), 2)
        assert provisioner._lock.locked()
        assert not action.done()
        assert harness.shell.close_count > 0
        process.release.set()
        if interruption == "cancel":
            with pytest.raises(asyncio.CancelledError):
                await action
        else:
            with pytest.raises(HomeAssistantError, match="rollback_deadline_exceeded"):
                await action
        assert not provisioner._lock.locked()
        assert await journal.async_retained(retained.record.transaction_id) == retained
    finally:
        process.release.set()
        await asyncio.gather(action, return_exceptions=True)


async def test_missing_remote_baseline_never_publishes_manual_restore_intent(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager, provisioner, journal, harness, _contacts = await _setup(hass, monkeypatch)
    retained = (await journal.async_retained_records())[0]
    finalized = _Process(blocked=True)
    commands: list[str] = []

    async def start(command: str) -> _Process:
        if "'finalize')" in command:
            commands.append("finalize")
            return finalized
        assert "'verify')" in command
        commands.append("verify_missing")
        return _Process(RunResult(1, "", ""))

    monkeypatch.setattr(harness.shell, "start", start)
    monkeypatch.setattr(panel_ops, "_identity_source", AsyncMock(return_value=""))
    monkeypatch.setattr(provisioner, "_operations", panel_ops)
    action = asyncio.create_task(
        manager.async_canary_operation("finalize", str(retained.record.transaction_id))
    )
    await finalized.entered.wait()
    action.cancel()
    finalized.release.set()
    with pytest.raises(asyncio.CancelledError):
        await action
    assert await journal.async_load() is None
    with pytest.raises(HomeAssistantError):
        await manager.async_canary_operation("rollback", str(retained.record.transaction_id))
    assert commands == ["finalize", "verify_missing"]
    assert await journal.async_load() is None
    assert await journal.async_retained(retained.record.transaction_id) == retained


@pytest.mark.parametrize("operation", ["finalize", "rollback"])
async def test_capture_without_installed_bridge_identity_refuses_before_contact(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    manager, _provisioner, journal, _harness, contacts = await _setup(hass, monkeypatch)
    retained = (await journal.async_retained_records())[0]
    snapshot = retained.record.prior_snapshot
    assert snapshot.baseline is not None
    row = replace(
        retained,
        record=replace(
            retained.record,
            prior_snapshot=replace(snapshot, baseline=replace(snapshot.baseline, identities={})),
        ),
    )
    await journal._async_retained_save({str(row.record.transaction_id): row})
    with pytest.raises(HomeAssistantError, match="canary_action_unavailable"):
        await manager.async_canary_operation(operation, str(row.record.transaction_id))
    assert contacts == []
    assert await journal.async_retained(row.record.transaction_id) == row
    assert await journal.async_load() is None


async def test_original_deployment_is_not_fresh_restore_evidence(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager, _provisioner, journal, _harness, contacts = await _setup(hass, monkeypatch)
    retained = (await journal.async_retained_records())[0]
    baseline = retained.record.prior_snapshot.baseline
    assert baseline is not None
    captured = baseline.identities["bridge"].deployment_id
    assert captured is not None
    # UPDATE provenance remains eligible; the evidence itself must be fresh.
    row = replace(
        retained,
        record=replace(retained.record, operation=ProvisioningOperation.UPDATE),
        state="restored",
        recovery_deployment_id=captured,
    )
    await journal._async_retained_save({str(row.record.transaction_id): row})
    with pytest.raises(HomeAssistantError, match="canary_action_unavailable"):
        await manager.async_canary_operation("finalize", str(row.record.transaction_id))
    assert contacts == []
    assert await journal.async_retained(row.record.transaction_id) == row


@pytest.mark.parametrize("layout", ["release", "absent"])
async def test_restored_update_remains_explicitly_finalizable(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, layout: str
) -> None:
    manager, _provisioner, journal, harness, contacts = await _setup(hass, monkeypatch)
    retained = (await journal.async_retained_records())[0]
    snapshot = retained.record.prior_snapshot
    if layout == "absent":
        snapshot = _record(ProvisioningPhase.STAGED).prior_snapshot
    # UPDATE is unambiguous; restored onboarding captures lack historical owner evidence.
    row = replace(
        retained,
        record=replace(
            retained.record, operation=ProvisioningOperation.UPDATE, prior_snapshot=snapshot
        ),
    )
    transaction = row.record.transaction_id
    await journal._async_retained_save({str(transaction): row})
    await manager.async_canary_operation("rollback", str(transaction))
    restored = await journal.async_retained(transaction)
    assert restored is not None and restored.state == "restored"
    assert restored.record == row.record
    await manager.async_canary_operation("finalize", str(transaction))
    assert len(contacts) == 2
    assert ("finalize_baseline",) in harness.events
    assert await journal.async_retained(transaction) is None
    assert await journal.async_load() is None


@pytest.mark.parametrize(
    "operation", [ProvisioningOperation.INSTALL, ProvisioningOperation.UPGRADE]
)
async def test_restored_onboarding_without_historical_owner_proof_is_refused(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, operation: ProvisioningOperation
) -> None:
    manager, _provisioner, journal, _harness, contacts = await _setup(hass, monkeypatch)
    retained = (await journal.async_retained_records())[0]
    row = replace(
        retained,
        record=replace(retained.record, operation=operation),
        state="restored",
        recovery_deployment_id="a" * 32,
    )
    await journal._async_retained_save({str(row.record.transaction_id): row})
    with pytest.raises(HomeAssistantError, match="canary_action_unavailable"):
        await manager.async_canary_operation("finalize", str(row.record.transaction_id))
    assert contacts == []
    assert await journal.async_retained(row.record.transaction_id) == row
    assert await journal.async_load() is None


@pytest.mark.parametrize("signal", ["transaction", "management"])
async def test_other_ownership_markers_are_independent_candidates(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, signal: str
) -> None:
    manager, _provisioner, journal, _harness, contacts = await _setup(hass, monkeypatch)
    row = (await journal.async_retained_records())[0]
    marker_owner = _panel(
        "bedroom",
        "SHA256:bedroom",
        subentry_id="bedroom",
        transaction_id=row.record.transaction_id if signal == "transaction" else None,
        management_id=row.record.panel_request.fingerprint if signal == "management" else None,
    )
    _fleet_entry(marker_owner).add_to_hass(hass)
    with pytest.raises(HomeAssistantError, match="canary_owner_changed"):
        await manager.async_canary_operation("finalize", str(row.record.transaction_id))
    assert contacts == []
    assert await journal.async_retained(row.record.transaction_id) == row


async def test_stale_address_repair_cannot_fall_through_to_ordinary_action(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager, _provisioner, journal, harness, contacts = await _setup(hass, monkeypatch)
    row = (await journal.async_retained_records())[0]
    store = cast(FleetPanelStore, manager.store)
    await fm.FleetManager(hass, store.entry)._async_prepare_provisioning()
    issue = ir.async_get(hass).async_get_issue(DOMAIN, address_issue_id(row))
    assert issue is not None
    flow = await async_create_fix_flow(hass, issue.issue_id, issue.data)
    flow.hass = hass
    await flow.async_step_init()
    store.update_data({**store.data, CONF_HOST: row.record.panel_request.host})
    result = await flow.async_step_init({"action": "finalize"})
    assert result["type"] == "form"
    assert result["description_placeholders"] == {"code": "canary_action_unavailable"}
    assert contacts == []
    assert ("finalize_baseline",) not in harness.events
    assert await journal.async_retained(row.record.transaction_id) == row


async def test_wrong_service_target_preserves_actual_panels_repair(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager, _provisioner, journal, _harness, contacts = await _setup(hass, monkeypatch)
    row = (await journal.async_retained_records())[0]
    store = cast(FleetPanelStore, manager.store)
    store.update_data({**store.data, CONF_HOST: row.record.panel_request.host})
    sibling = _panel("bedroom", "SHA256:bedroom", subentry_id="bedroom")
    hass.config_entries.async_add_subentry(store.entry, sibling)
    wrong = PanelManager(
        hass, FleetPanelStore(hass, store.entry, sibling), _fleet(), asyncio.Lock()
    )
    await fm._ProvisioningRepairReporter(hass).async_report_rollback_failure(
        row.record.transaction_id, original_code="restart_recovery", rollback_code="recovery_failed"
    )
    generic = fm._provisioning_repair_issue_id(row.record.transaction_id)
    issues = ir.async_get(hass)
    with pytest.raises(HomeAssistantError, match="canary_owner_changed"):
        await wrong.async_canary_operation("rollback", str(row.record.transaction_id))
    assert issues.async_get_issue(DOMAIN, generic) is not None
    assert issues.async_get_issue(DOMAIN, address_issue_id(row)) is None
    assert contacts == []
    assert await journal.async_retained(row.record.transaction_id) == row


@pytest.mark.parametrize("failure", ["mismatch", "unreachable"])
@pytest.mark.parametrize("route", ["service", "repair"])
@pytest.mark.allow_lingering_timers
async def test_real_verifier_errors_stay_typed_through_registered_routes(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
    mqtt_mock: Any,
    fake_shell: Any,
    payload_dir: Path,
    failure: str,
    route: str,
) -> None:
    entry = await _setup_fleet_entry(hass)
    try:
        journal = ProvisioningJournal(hass)
        record = _record(ProvisioningPhase.STAGED)
        transaction = record.transaction_id
        await journal.async_create(record)
        for phase in (
            ProvisioningPhase.ACTIVATION_PENDING,
            ProvisioningPhase.ACTIVATED,
            ProvisioningPhase.VERIFYING,
            ProvisioningPhase.PENDING_CONFIG_COMMIT,
        ):
            await journal.async_transition(transaction, phase)
        await journal.async_complete_commit(transaction, subentry_id="panel-office")
        row = await journal.async_retained(transaction)
        assert row is not None
        fetch = AsyncMock(
            return_value=HostIdentity(_OTHER_PUBLIC_KEY, _OTHER_FINGERPRINT),
            side_effect=shell.PanelIdentityError("host_unreachable")
            if failure == "unreachable"
            else None,
        )
        monkeypatch.setattr(shell, "_async_fetch_host_identity", fetch)
        factory = Mock()
        monkeypatch.setattr(fm, "AsyncsshShell", factory)
        expected = (
            "panel_identity_mismatch" if failure == "mismatch" else "recovery_connection_failed"
        )
        if route == "service":
            entity = er.async_get(hass).async_get_entity_id(
                "binary_sensor", DOMAIN, "SHA256:office_bridge_health"
            )
            assert entity is not None
            with pytest.raises(HomeAssistantError, match=expected):
                await hass.services.async_call(
                    DOMAIN,
                    "canary_finalize",
                    {"entity_id": entity, "name": str(transaction)},
                    blocking=True,
                )
        else:
            await fm.FleetManager(hass, entry)._async_prepare_provisioning()
            issue = ir.async_get(hass).async_get_issue(DOMAIN, address_issue_id(row))
            assert issue is not None
            flow = await async_create_fix_flow(hass, issue.issue_id, issue.data)
            flow.hass = hass
            result = await flow.async_step_init({"action": "finalize"})
            assert result["description_placeholders"] == {"code": expected}
        factory.assert_not_called()
        assert await journal.async_retained(transaction) == row
        assert ir.async_get(hass).async_get_issue(DOMAIN, address_issue_id(row)) is not None
    finally:
        await hass.config_entries.async_unload(entry.entry_id)


async def test_busy_manual_action_refuses_instead_of_queuing(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager, provisioner, journal, harness, contacts = await _setup(hass, monkeypatch)
    row = (await journal.async_retained_records())[0]
    async with provisioner._lock:
        action = asyncio.create_task(
            manager.async_canary_operation("finalize", str(row.record.transaction_id))
        )
        done, _pending = await asyncio.wait({action}, timeout=0.2)
    with pytest.raises(HomeAssistantError, match="transaction_in_progress"):
        await action
    assert done == {action}
    assert contacts == []
    assert ("finalize_baseline",) not in harness.events
    assert await journal.async_retained(row.record.transaction_id) == row


@pytest.mark.parametrize("late_failure", ["close", "cancel_after_retirement"])
async def test_retired_baseline_does_not_leave_an_unresolvable_address_issue(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, late_failure: str
) -> None:
    manager, _provisioner, journal, harness, _contacts = await _setup(hass, monkeypatch)
    row = (await journal.async_retained_records())[0]
    remove = ProvisioningJournal.async_remove_retained

    async def cancel_after_retirement(
        instance: ProvisioningJournal, *args: Any, **kwargs: Any
    ) -> None:
        await remove(instance, *args, **kwargs)
        raise asyncio.CancelledError

    if late_failure == "close":
        harness.shell.fail_close_once = True
    else:
        monkeypatch.setattr(ProvisioningJournal, "async_remove_retained", cancel_after_retirement)
    error = HomeAssistantError if late_failure == "close" else asyncio.CancelledError
    with pytest.raises(error):
        await manager.async_canary_operation("finalize", str(row.record.transaction_id))
    assert await journal.async_retained(row.record.transaction_id) is None
    assert ir.async_get(hass).async_get_issue(DOMAIN, address_issue_id(row)) is None


@pytest.mark.parametrize("outcome", ["retained", "storage_unknown"])
@pytest.mark.parametrize("route", ["service", "repair"])
async def test_issue_reconciliation_requires_confirmed_retained_absence(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, outcome: str, route: str
) -> None:
    manager, _provisioner, journal, harness, _contacts = await _setup(hass, monkeypatch)
    row = (await journal.async_retained_records())[0]
    read = ProvisioningJournal.async_retained
    remove = ProvisioningJournal.async_remove_retained
    retired = False

    async def note_retirement(instance: ProvisioningJournal, *args: Any, **kwargs: Any) -> None:
        nonlocal retired
        await remove(instance, *args, **kwargs)
        retired = True

    async def failed_confirmation(instance: ProvisioningJournal, *args: Any) -> Any:
        if retired:
            raise ProvisioningJournalError("journal_load_failed")
        return await read(instance, *args)

    if outcome == "retained":
        harness.operations.fail_at.add("finalize_baseline")
    else:
        monkeypatch.setattr(ProvisioningJournal, "async_remove_retained", note_retirement)
        monkeypatch.setattr(ProvisioningJournal, "async_retained", failed_confirmation)
    if route == "service":
        with pytest.raises(HomeAssistantError):
            await manager.async_canary_operation("finalize", str(row.record.transaction_id))
    else:
        await fm.FleetManager(
            hass, cast(FleetPanelStore, manager.store).entry
        )._async_prepare_provisioning()
        issue = ir.async_get(hass).async_get_issue(DOMAIN, address_issue_id(row))
        assert issue is not None
        flow = await async_create_fix_flow(hass, issue.issue_id, issue.data)
        flow.hass = hass
        result = await flow.async_step_init({"action": "finalize"})
        assert result["type"] == "form"
    assert retired is (outcome == "storage_unknown")
    assert (await read(journal, row.record.transaction_id) is None) is retired
    assert ir.async_get(hass).async_get_issue(DOMAIN, address_issue_id(row)) is not None


@pytest.mark.parametrize(
    ("state", "code"),
    [
        ("finalizing", "provisioning_recovery_required"),
        ("restore_requested", "rollback_credentials_unavailable"),
    ],
)
async def test_retained_only_generic_recovery_refuses_typed_without_contact(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, state: str, code: str
) -> None:
    _manager, provisioner, journal, harness, contacts = await _setup(hass, monkeypatch)
    row = (await journal.async_retained_records())[0]
    await journal.async_retained_state(row.record.transaction_id, state)
    before = await journal.async_retained(row.record.transaction_id)
    monkeypatch.setattr(
        provisioner, "_retained_deferred", partial(async_defer_address_action, hass)
    )
    with pytest.raises(PanelProvisioningError, match=code):
        await provisioner.async_recover()
    assert contacts == []
    assert ("shell_connect",) not in harness.events
    assert await journal.async_load() is None
    assert await journal.async_retained(row.record.transaction_id) == before


async def test_queued_moved_finalize_does_not_report_completion_or_reload(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> None:
    manager, provisioner, journal, harness, contacts = await _setup(hass, monkeypatch)
    row = (await journal.async_retained_records())[0]
    await journal.async_retained_state(row.record.transaction_id, "finalizing")
    before = await journal.async_retained(row.record.transaction_id)
    assert before is not None
    monkeypatch.setattr(
        provisioner, "_retained_deferred", partial(async_defer_address_action, hass)
    )
    runtime = fm.FleetManager(hass, cast(FleetPanelStore, manager.store).entry)
    reload = Mock()
    monkeypatch.setattr(runtime, "_async_schedule_reload_once", reload)
    await runtime._async_background_recovery(before.record)
    assert contacts == []
    assert ("shell_connect",) not in harness.events
    assert await journal.async_retained(row.record.transaction_id) == before
    reload.assert_not_called()


@pytest.mark.parametrize(
    "terminal", ["finalize", "cleanup", "commit", "committed", "rollback", "absent"]
)
async def test_generic_retirement_rechecks_owner_after_final_storage_read(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, terminal: str
) -> None:
    manager, _provisioner, journal, harness, _contacts = await _setup(hass, monkeypatch)
    retained = (await journal.async_retained_records())[0]
    transaction = retained.record.transaction_id
    store = cast(FleetPanelStore, manager.store)
    from custom_components.brilliant_mqtt.const import CONF_MANAGEMENT_ID, FLEET_UNIQUE_ID

    hass.config_entries.async_update_entry(store.entry, unique_id=FLEET_UNIQUE_ID)
    hass.config_entries.async_update_subentry(
        store.entry,
        store.subentry,
        data={
            **store.data,
            CONF_HOST: retained.record.panel_request.host,
            CONF_MANAGEMENT_ID: retained.record.panel_request.fingerprint,
        },
    )
    request = replace(
        retained.record.panel_request,
        root_password=store.data["root_password"],
        selected_components=("bridge", "bus_watchdog", "wifi_watchdog"),
    )
    record = replace(
        retained.record,
        operation=ProvisioningOperation.UPDATE,
        panel_request=request,
        fleet_profile=_pending_record().fleet_profile,
    )
    retained = replace(retained, record=record)
    target = {
        "finalize": "async_remove_retained",
        "cleanup": "async_complete_cleanup",
        "commit": "async_complete_commit",
        "committed": "async_clear_committed",
        "rollback": "async_complete_rollback",
        "absent": "async_complete_absent_rollback",
    }[terminal]
    if terminal == "finalize":
        retained = replace(retained, state="finalizing")
    elif terminal == "commit":
        record = replace(record, phase=ProvisioningPhase.PENDING_CONFIG_COMMIT)
    elif terminal == "committed":
        record = replace(record, phase=ProvisioningPhase.COMMITTED)
    elif terminal == "rollback":
        retained = replace(retained, state="restored", recovery_deployment_id="a" * 32)
        record = replace(
            record, operation=ProvisioningOperation.ROLLBACK, phase=ProvisioningPhase.ROLLED_BACK
        )
    elif terminal == "absent":
        snapshot = _record(ProvisioningPhase.STAGED).prior_snapshot
        retained = replace(
            retained,
            record=replace(retained.record, prior_snapshot=snapshot),
            state="restore_requested",
        )
        record = replace(
            record,
            operation=ProvisioningOperation.ROLLBACK,
            phase=ProvisioningPhase.ROLLBACK_PENDING,
            prior_snapshot=snapshot,
        )
    await journal._async_retained_save({str(transaction): retained})
    if terminal != "finalize":
        # Seed a durable crash checkpoint; create only accepts fresh STAGED work.
        await journal._async_save(record)
    monkeypatch.setattr(fm, "AsyncsshShell", lambda *args: harness.shell)
    provisioner = fm._build_panel_provisioner(hass, harness.fetch_identity)
    monkeypatch.setattr(provisioner, "_operations", harness.operations)
    monkeypatch.setattr(
        provisioner,
        "_transaction_lookup",
        AsyncMock(return_value=TransactionLookup(TransactionLookupState.MATCHED, "office")),
    )
    mutate = getattr(ProvisioningJournal, target)
    load = ProvisioningJournal._async_retained_load
    inside = False
    captured: list[tuple[Any, Any]] = []

    async def enter(instance: ProvisioningJournal, *args: Any, **kwargs: Any) -> Any:
        nonlocal inside
        inside = True
        try:
            return await mutate(instance, *args, **kwargs)
        finally:
            inside = False

    async def final_read(instance: ProvisioningJournal) -> Any:
        rows = await load(instance)
        if inside and not captured:
            captured.append((instance._coordinator.record, rows[str(transaction)]))
            store.update_data({**store.data, CONF_HOST: "new.example"})
        return rows

    monkeypatch.setattr(ProvisioningJournal, target, enter)
    monkeypatch.setattr(ProvisioningJournal, "_async_retained_load", final_read)
    with pytest.raises(PanelProvisioningError):
        await provisioner.async_recover()
    assert captured
    assert await journal.async_load() == captured[0][0]
    assert await journal.async_retained(transaction) == captured[0][1]
