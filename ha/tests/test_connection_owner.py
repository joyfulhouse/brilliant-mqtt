"""Verified connection changes retain durable authority across crash boundaries."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest
from homeassistant.components import mqtt
from homeassistant.core import HomeAssistant

from custom_components.brilliant_mqtt.const import CONF_HOST, CONF_ROOT_PASSWORD
from custom_components.brilliant_mqtt.flow import gateway
from custom_components.brilliant_mqtt.provisioning_journal import (
    JOURNAL_STORAGE_KEY,
    ProvisioningJournal,
    ProvisioningOperation,
    Store,
)
from custom_components.brilliant_mqtt.release_identity import RollbackBaseline
from tests.test_config_flow import (
    _fleet_entry,
    _identity,
    _start_panel_reconfigure_step,
    _subentry,
)
from tests.test_provisioning_journal import _record


def disk(hass: HomeAssistant, key: str) -> dict[str, Any]:
    raw = json.loads(Path(hass.config.path(".storage", key)).read_text())
    assert isinstance(raw, dict)
    data = raw["data"]
    assert isinstance(data, dict)
    return data


@pytest.mark.parametrize("step", ["address", "repair_credentials"])
async def test_verified_connection_retargets_baseline_and_active_journal(
    hass: HomeAssistant, durable_connection_storage: None, step: str
) -> None:
    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    stored = entry.subentries[panel.subentry_id]
    record = _record()
    record = replace(
        record,
        operation=ProvisioningOperation.UPDATE,
        panel_request=replace(
            record.panel_request,
            host=stored.data[CONF_HOST],
            root_password=stored.data[CONF_ROOT_PASSWORD],
            slug="office",
        ),
        prior_snapshot=replace(
            record.prior_snapshot, baseline=RollbackBaseline("c" * 32, "d" * 64, {})
        ),
    )
    journal = ProvisioningJournal(hass)
    await journal.async_create(record)
    form = await _start_panel_reconfigure_step(hass, entry, stored, step)
    values = {CONF_HOST: "moved.office.test"}
    if step == "repair_credentials":
        values[CONF_ROOT_PASSWORD] = "new-root-password"
    with (
        patch.object(gateway, "async_fetch_host_identity", AsyncMock(return_value=_identity())),
        patch.object(gateway, "_async_inspect_candidate", AsyncMock()),
    ):
        result = await hass.config_entries.subentries.async_configure(form["flow_id"], values)
    assert result["type"] == "abort", result
    assert result["reason"] == "reconfigure_successful"
    active = await journal.async_load()
    assert active is not None
    assert active.panel_request.host == "moved.office.test"
    assert active.panel_request.root_password == stored.data[CONF_ROOT_PASSWORD]
    retained = await journal.async_retained(record.transaction_id)
    assert retained is not None
    assert retained.record.panel_request.host == "moved.office.test"
    assert retained.record.prior_snapshot == record.prior_snapshot
    assert "new-root-password" not in json.dumps(disk(hass, JOURNAL_STORAGE_KEY + ".retained"))
    assert stored.data.get("connection_commit_id")
    assert "moved.office.test" in json.dumps(disk(hass, "core.config_entries"))


async def test_delayed_config_save_keeps_pending_intent(
    hass: HomeAssistant,
    durable_connection_storage: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    stored = entry.subentries[panel.subentry_id]
    form = await _start_panel_reconfigure_step(hass, entry, stored, "address")
    monkeypatch.setattr(hass.config_entries._store, "async_delay_save", Mock())
    with (
        patch.object(gateway, "async_fetch_host_identity", AsyncMock(return_value=_identity())),
        patch.object(gateway, "_async_inspect_candidate", AsyncMock()),
    ):
        result = await hass.config_entries.subentries.async_configure(
            form["flow_id"], {CONF_HOST: "moved.office.test"}
        )
    assert result["type"] == "form", "RAM equality must not retire an unpersisted intent"
    payload = disk(hass, JOURNAL_STORAGE_KEY)
    assert payload.get("connection_intents"), payload
    assert stored.data[CONF_HOST] == "moved.office.test"


async def pending_move(hass: HomeAssistant) -> tuple[Any, Any, Any]:
    from uuid import uuid4

    from custom_components.brilliant_mqtt.connection_owner import PanelConnectionOwner
    from custom_components.brilliant_mqtt.connection_state import ConnectionIntent

    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    stored = entry.subentries[panel.subentry_id]
    owner = PanelConnectionOwner(hass, entry, stored)
    intent = ConnectionIntent(
        str(uuid4()), owner.owner, owner.prior, replace(owner.prior, host="moved.office.test"), ()
    )
    await owner.journal.async_begin_connection_change(intent)
    return entry, stored, intent


async def test_pending_only_intent_blocks_recorded_recovery(
    hass: HomeAssistant, durable_connection_storage: None
) -> None:
    from custom_components.brilliant_mqtt.fleet_manager import _async_authorize_recorded_recovery
    from custom_components.brilliant_mqtt.panel_provisioner import PanelProvisioningError

    _entry, _stored, _intent = await pending_move(hass)
    record = replace(_record(), operation=ProvisioningOperation.INSTALL)
    with pytest.raises(PanelProvisioningError, match="address_change_incomplete"):
        await _async_authorize_recorded_recovery(hass, record)


async def test_pending_only_intent_blocks_migration(
    hass: HomeAssistant, durable_connection_storage: None
) -> None:
    from custom_components.brilliant_mqtt.migration_coordinator import MigrationCoordinator

    entry, _stored, _intent = await pending_move(hass)
    coordinator = MigrationCoordinator(hass)
    blockers = await coordinator._async_blockers([entry])
    assert any(item["code"] == "address_change_incomplete" for item in blockers), blockers


async def test_key_only_legacy_sibling_refuses_candidate_before_login(
    hass: HomeAssistant, durable_connection_storage: None
) -> None:
    from pytest_homeassistant_custom_component.common import MockConfigEntry

    from custom_components.brilliant_mqtt.const import CONF_PANEL, DOMAIN
    from tests.test_init import ENTRY_DATA

    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    stored = entry.subentries[panel.subentry_id]
    sibling = MockConfigEntry(
        domain=DOMAIN,
        unique_id="sibling",
        data={**ENTRY_DATA, CONF_PANEL: "sibling", "ssh_host_key": _identity().public_key},
        version=3,
    )
    sibling.add_to_hass(hass)
    form = await _start_panel_reconfigure_step(hass, entry, stored, "address")
    inspect = AsyncMock()
    with (
        patch.object(gateway, "async_fetch_host_identity", AsyncMock(return_value=_identity())),
        patch.object(gateway, "_async_inspect_candidate", inspect),
    ):
        result = await hass.config_entries.subentries.async_configure(
            form["flow_id"], {CONF_HOST: "moved.office.test"}
        )
    assert result["type"] == "form"
    assert result["errors"] == {"base": "duplicate_panel"}
    inspect.assert_not_awaited()


async def test_legacy_connection_always_checks_stored_pin(
    hass: HomeAssistant, durable_connection_storage: None
) -> None:
    from tests.test_config_flow import _legacy_entry, _open_legacy_reconfigure_step

    entry = _legacy_entry(hass)
    form = await _open_legacy_reconfigure_step(hass, entry, "reconfigure_connection")
    pinned = entry.data["ssh_host_key"]
    apply = AsyncMock(return_value=pinned)
    with patch.object(gateway, "_apply_config", apply):
        result = await hass.config_entries.flow.async_configure(
            form["flow_id"], {CONF_HOST: "moved.office.test", CONF_ROOT_PASSWORD: ""}
        )
    assert result["type"] == "abort", result
    assert apply.call_args.kwargs["pinned_key"] == pinned


async def test_journal_completion_preserves_independent_intents(
    hass: HomeAssistant, durable_connection_storage: None
) -> None:
    _entry, _stored, intent = await pending_move(hass)
    journal = ProvisioningJournal(hass)
    active = _record()
    await journal.async_create(active)
    await journal.async_complete_cleanup(active.transaction_id)
    assert await journal.async_load() is None
    assert await journal.async_connection_intents() == (intent,)


def test_connection_state_preserves_unicode_login() -> None:
    from custom_components.brilliant_mqtt.connection_state import PanelConnection

    value = PanelConnection("office.test", "root", "p\u00e4ss\u2603\n", "ssh-key")
    assert PanelConnection.from_storage(value.to_storage()) == value


async def test_verified_form_runs_real_gateway_without_recursive_operation_lock(
    hass: HomeAssistant, durable_connection_storage: None
) -> None:
    import asyncio

    from tests.fakes import FakeShell
    from tests.test_config_flow import _facts

    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    stored = entry.subentries[panel.subentry_id]
    form = await _start_panel_reconfigure_step(hass, entry, stored, "address")
    with (
        patch.object(gateway, "async_fetch_host_identity", AsyncMock(return_value=_identity())),
        patch.object(
            gateway, "FleetAsyncsshShell", return_value=FakeShell(pinned=_identity().public_key)
        ),
        patch.object(gateway, "async_inspect_panel", AsyncMock(return_value=_facts(_identity()))),
    ):
        async with asyncio.timeout(4):
            result = await hass.config_entries.subentries.async_configure(
                form["flow_id"], {CONF_HOST: "moved.office.test"}
            )
    assert result["type"] == "abort", result


@pytest.mark.parametrize("phase", ["staged", "rollback_pending"])
async def test_named_finalize_refuses_same_panel_active_transaction(
    hass: HomeAssistant,
    durable_connection_storage: None,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
) -> None:
    from uuid import uuid4

    from custom_components.brilliant_mqtt.panel_provisioner import PanelProvisioningError
    from custom_components.brilliant_mqtt.provisioning_journal import ProvisioningPhase
    from tests.test_panel_provisioner import _Harness

    record = _record()
    retained_record = replace(
        record,
        prior_snapshot=replace(
            record.prior_snapshot, baseline=RollbackBaseline("c" * 32, "d" * 64, {})
        ),
    )
    journal = ProvisioningJournal(hass)
    await journal.async_arm(retained_record)
    active = replace(record, transaction_id=uuid4(), phase=ProvisioningPhase(phase))
    await journal.async_create(replace(active, phase=ProvisioningPhase.STAGED))
    if phase == "rollback_pending":
        await journal.async_transition(active.transaction_id, ProvisioningPhase.ROLLBACK_PENDING)
    before = await journal.async_retained(retained_record.transaction_id)
    harness = _Harness()
    monkeypatch.setattr(harness, "journal", journal)
    provisioner = harness.provisioner()
    with pytest.raises(PanelProvisioningError, match="provisioning_recovery_required"):
        await provisioner.async_finalize(retained_record.transaction_id)
    assert await journal.async_retained(retained_record.transaction_id) == before


async def test_named_rollback_of_other_panel_uses_retained_scoped_record(
    hass: HomeAssistant, durable_connection_storage: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    from uuid import uuid4

    from tests.test_config_flow import _OTHER_FINGERPRINT, _OTHER_PUBLIC_KEY
    from tests.test_panel_provisioner import _Harness

    record = _record()
    target = replace(
        record,
        prior_snapshot=replace(
            record.prior_snapshot, baseline=RollbackBaseline("c" * 32, "d" * 64, {})
        ),
    )
    active = replace(
        record,
        transaction_id=uuid4(),
        panel_request=replace(
            record.panel_request,
            slug="kitchen",
            public_key=_OTHER_PUBLIC_KEY,
            fingerprint=_OTHER_FINGERPRINT,
        ),
    )
    journal = ProvisioningJournal(hass)
    await journal.async_arm(target)
    await journal.async_create(active)
    harness = _Harness()
    monkeypatch.setattr(harness, "journal", journal)
    provisioner = harness.provisioner()
    monkeypatch.setattr(
        provisioner, "_credential_resolver", AsyncMock(return_value="current-login")
    )
    execute = AsyncMock()
    monkeypatch.setattr(provisioner, "_async_recover_record", execute)
    await provisioner.async_rollback(target.transaction_id)
    assert await journal.async_load() == active
    retained = await journal.async_retained(target.transaction_id)
    assert retained is not None
    assert getattr(retained, "recovery_record", None) is not None
    execute.assert_awaited_once()
    assert execute.call_args.args[0].transaction_id == target.transaction_id
    assert "current-login" not in json.dumps(disk(hass, JOURNAL_STORAGE_KEY + ".retained"))


async def test_cancellation_before_intent_never_enqueues_committed_runtime(
    hass: HomeAssistant, durable_connection_storage: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    import asyncio

    from custom_components.brilliant_mqtt.connection_owner import PanelConnectionOwner
    from custom_components.brilliant_mqtt.connection_state import ConnectionIntent
    from custom_components.brilliant_mqtt.fleet_manager import _shared_ssh_lock

    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    owner = PanelConnectionOwner(hass, entry, entry.subentries[panel.subentry_id])
    notify = Mock()
    monkeypatch.setattr(owner, "_enqueue_runtime", notify)

    async def interrupted(_journal: ProvisioningJournal, _intent: ConnectionIntent) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(ProvisioningJournal, "async_begin_connection_change", interrupted)
    async with _shared_ssh_lock(hass):
        with pytest.raises(asyncio.CancelledError):
            await owner.async_commit(host="moved.office.test", password="fresh-value")
    notify.assert_not_called()


async def test_postcommit_cancel_enqueue_failure_retains_retry_repair(
    hass: HomeAssistant, durable_connection_storage: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    import asyncio

    from homeassistant.helpers import issue_registry as ir

    from custom_components.brilliant_mqtt.connection_owner import (
        ConnectionOwnershipError,
        PanelConnectionOwner,
        _issue_id,
    )
    from custom_components.brilliant_mqtt.connection_state import ConnectionIntent
    from custom_components.brilliant_mqtt.const import DOMAIN
    from custom_components.brilliant_mqtt.fleet_manager import _shared_ssh_lock

    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    entry.runtime_data = Mock()
    owner = PanelConnectionOwner(hass, entry, entry.subentries[panel.subentry_id])
    finish = owner.journal.async_finish_connection_change

    async def interrupted(
        _journal: ProvisioningJournal, intent: ConnectionIntent, **kwargs: Any
    ) -> None:
        await finish(intent, **kwargs)
        raise asyncio.CancelledError

    monkeypatch.setattr(ProvisioningJournal, "async_finish_connection_change", interrupted)
    monkeypatch.setattr(
        hass.config_entries, "async_schedule_reload", Mock(side_effect=RuntimeError)
    )
    async with _shared_ssh_lock(hass):
        with pytest.raises((ConnectionOwnershipError, asyncio.CancelledError)):
            await owner.async_commit(host="moved.office.test", password="fresh-value")
    assert not await owner.journal.async_connection_intents()
    assert ir.async_get(hass).async_get_issue(DOMAIN, _issue_id(owner.owner)) is not None


@pytest.mark.parametrize("cancel_proof_read", [False, True])
async def test_cancelled_commit_proof_read_failure_restores_exact_intent(
    hass: HomeAssistant,
    durable_connection_storage: None,
    monkeypatch: pytest.MonkeyPatch,
    cancel_proof_read: bool,
) -> None:
    import asyncio

    from homeassistant.helpers import issue_registry as ir

    from custom_components.brilliant_mqtt.connection_owner import PanelConnectionOwner, _issue_id
    from custom_components.brilliant_mqtt.connection_state import ConnectionIntent
    from custom_components.brilliant_mqtt.const import DOMAIN
    from custom_components.brilliant_mqtt.fleet_manager import _shared_ssh_lock
    from custom_components.brilliant_mqtt.provisioning_journal import ProvisioningJournalError

    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    stored = entry.subentries[panel.subentry_id]
    owner = PanelConnectionOwner(hass, entry, stored)
    finish = ProvisioningJournal.async_finish_connection_change
    read = ProvisioningJournal.async_connection_intents
    failed_read_pending = False
    retired: list[ConnectionIntent] = []
    notify = Mock()
    monkeypatch.setattr(owner, "_enqueue_runtime", notify)

    async def interrupted(
        journal: ProvisioningJournal, intent: ConnectionIntent, **kwargs: Any
    ) -> None:
        nonlocal failed_read_pending
        await finish(journal, intent, **kwargs)
        retired.append(intent)
        failed_read_pending = True
        raise asyncio.CancelledError

    async def failed_proof_read(journal: ProvisioningJournal) -> tuple[ConnectionIntent, ...]:
        nonlocal failed_read_pending
        if failed_read_pending:
            failed_read_pending = False
            if cancel_proof_read:
                task = asyncio.current_task()
                assert task is not None
                task.cancel()
                await asyncio.sleep(0)
            raise ProvisioningJournalError("journal_load_failed")
        return await read(journal)

    monkeypatch.setattr(ProvisioningJournal, "async_finish_connection_change", interrupted)
    monkeypatch.setattr(ProvisioningJournal, "async_connection_intents", failed_proof_read)
    async with _shared_ssh_lock(hass):
        task = asyncio.create_task(
            owner.async_commit(host="moved.office.test", password="fresh-login")
        )
        with pytest.raises(asyncio.CancelledError):
            await task
    assert len(retired) == 1
    assert await owner.journal.async_connection_intents() == tuple(retired)
    assert set(disk(hass, JOURNAL_STORAGE_KEY)["connection_intents"]) == {retired[0].commit_id}
    assert stored.data[CONF_HOST] == "moved.office.test"
    assert stored.data[CONF_ROOT_PASSWORD] == "fresh-login"
    assert stored.data["connection_commit_id"] == retired[0].commit_id
    notify.assert_not_called()
    assert ir.async_get(hass).async_get_issue(DOMAIN, _issue_id(owner.owner)) is not None
    with pytest.raises(ProvisioningJournalError, match="address_change_incomplete"):
        await owner.journal.async_assert_connection_available(owner=owner.owner)


async def test_key_only_legacy_duplicate_with_accepted_outer_whitespace(
    hass: HomeAssistant,
) -> None:
    from pytest_homeassistant_custom_component.common import MockConfigEntry

    from custom_components.brilliant_mqtt.connection_owner import (
        ConnectionOwnershipError,
        PanelConnectionOwner,
    )
    from custom_components.brilliant_mqtt.const import DATA_SSH_HOST_KEY, DOMAIN
    from custom_components.brilliant_mqtt.shell import _legacy_known_hosts_line
    from tests.test_init import ENTRY_DATA

    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    owner = PanelConnectionOwner(hass, entry, entry.subentries[panel.subentry_id])
    alternate = " " + owner.owner.public_key + " "
    assert _legacy_known_hosts_line("sibling.test", alternate)
    sibling = MockConfigEntry(domain=DOMAIN, data={**ENTRY_DATA, DATA_SSH_HOST_KEY: alternate})
    sibling.add_to_hass(hass)
    with pytest.raises(ConnectionOwnershipError, match="duplicate_panel"):
        owner.assert_unique()


async def test_owner_removal_during_final_validation_preserves_intent(
    hass: HomeAssistant, durable_connection_storage: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    from custom_components.brilliant_mqtt.connection_owner import (
        ConnectionOwnershipError,
        PanelConnectionOwner,
    )
    from custom_components.brilliant_mqtt.connection_state import ConnectionIntent
    from custom_components.brilliant_mqtt.fleet_manager import _shared_ssh_lock

    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    owner = PanelConnectionOwner(hass, entry, entry.subentries[panel.subentry_id])
    finish = owner.journal.async_finish_connection_change

    async def remove_before_finish(
        _journal: ProvisioningJournal, intent: ConnectionIntent, **kwargs: Any
    ) -> None:
        hass.config_entries.async_remove_subentry(entry, panel.subentry_id)
        await finish(intent, **kwargs)

    monkeypatch.setattr(ProvisioningJournal, "async_finish_connection_change", remove_before_finish)
    async with _shared_ssh_lock(hass):
        with pytest.raises(ConnectionOwnershipError):
            await owner.async_commit(host="moved.office.test", password="current-login")
    intents = await ProvisioningJournal(hass).async_connection_intents()
    assert len(intents) == 1
    assert intents[0].owner.subentry_id == panel.subentry_id


async def test_scoped_failed_rollback_cold_resume_keeps_unrelated_active_slot(
    hass: HomeAssistant, durable_connection_storage: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    from functools import partial
    from uuid import uuid4

    from custom_components.brilliant_mqtt.connection_owner import async_resolve_panel_owner
    from custom_components.brilliant_mqtt.fleet_manager import _async_rollback_credential
    from custom_components.brilliant_mqtt.panel_provisioner import PanelProvisioningError
    from custom_components.brilliant_mqtt.provisioning_journal import ProvisioningPhase
    from tests.test_config_flow import _OTHER_FINGERPRINT, _OTHER_PUBLIC_KEY
    from tests.test_panel_provisioner import _Harness
    from tests.test_panel_provisioner import _record as canary_record

    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    stored = entry.subentries[panel.subentry_id]
    target = canary_record(ProvisioningPhase.STAGED)
    target = replace(
        target,
        panel_request=replace(target.panel_request, root_password=stored.data[CONF_ROOT_PASSWORD]),
    )
    active = replace(
        target,
        transaction_id=uuid4(),
        panel_request=replace(
            target.panel_request,
            slug="kitchen",
            public_key=_OTHER_PUBLIC_KEY,
            fingerprint=_OTHER_FINGERPRINT,
        ),
    )
    journal = ProvisioningJournal(hass)
    await journal.async_arm(target)
    await journal.async_create(active)

    def provisioner_for(current_journal: ProvisioningJournal, fail: bool = False) -> Any:
        harness = _Harness()
        monkeypatch.setattr(harness, "journal", current_journal)
        if fail:
            harness.operations.fail_at.add("rollback")
        provisioner = harness.provisioner()
        monkeypatch.setattr(provisioner, "_shell_factory", Mock(return_value=harness.shell))
        monkeypatch.setattr(
            provisioner, "_credential_resolver", partial(_async_rollback_credential, hass)
        )
        monkeypatch.setattr(
            provisioner, "_named_owner_resolver", partial(async_resolve_panel_owner, hass)
        )
        return provisioner

    with pytest.raises(PanelProvisioningError, match="rollback_failed"):
        await provisioner_for(journal, True).async_rollback(target.transaction_id)
    failed = await journal.async_retained(target.transaction_id)
    assert failed is not None and failed.state == "rollback_failed"
    assert failed.recovery_record is not None
    assert failed.recovery_record.panel_request.bound_owner is not None
    assert failed.recovery_record.panel_request.bound_owner.subentry_id == panel.subentry_id
    with pytest.raises(PanelProvisioningError, match="provisioning_recovery_required"):
        await provisioner_for(journal).async_finalize(target.transaction_id)
    assert await journal.async_retained(target.transaction_id) == failed
    hass.data.pop("brilliant_mqtt.provisioning_journal.coordinator")
    cold = ProvisioningJournal(hass)
    await provisioner_for(cold).async_rollback(target.transaction_id)
    assert await cold.async_load() == active
    restored = await cold.async_retained(target.transaction_id)
    assert restored is not None and restored.state == "restored"
    assert restored.recovery_record is None
    assert stored.data[CONF_ROOT_PASSWORD] not in json.dumps(
        disk(hass, JOURNAL_STORAGE_KEY + ".retained")
    )


@pytest.mark.parametrize("with_active", [False, True])
async def test_unproven_intent_removal_restores_same_evidence(
    hass: HomeAssistant,
    durable_connection_storage: None,
    monkeypatch: pytest.MonkeyPatch,
    with_active: bool,
) -> None:
    from custom_components.brilliant_mqtt.connection_owner import (
        ConnectionOwnershipError,
        PanelConnectionOwner,
    )
    from custom_components.brilliant_mqtt.fleet_manager import _shared_ssh_lock

    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    owner = PanelConnectionOwner(hass, entry, entry.subentries[panel.subentry_id])
    if with_active:
        await owner.journal.async_create(_record())
    original_remove = Store.async_remove
    original_write = Store._async_write_data
    original_load = Store.async_load
    fail_readback = False
    failed = False

    async def remove(store: Store) -> None:
        nonlocal fail_readback
        await original_remove(store)
        if store.key == JOURNAL_STORAGE_KEY:
            fail_readback = True

    async def write(store: Store, data: dict[str, Any]) -> None:
        nonlocal fail_readback
        await original_write(store, data)
        payload = data["data"]
        if (
            store.key == JOURNAL_STORAGE_KEY
            and payload.get("record") is not None
            and payload.get("connection_intents") == {}
        ):
            fail_readback = True

    async def load(store: Store) -> dict[str, Any] | None:
        nonlocal fail_readback, failed
        if store.key == JOURNAL_STORAGE_KEY and fail_readback and not failed:
            fail_readback = False
            failed = True
            raise OSError("readback unavailable")
        return await original_load(store)

    monkeypatch.setattr(Store, "async_remove", remove)
    monkeypatch.setattr(Store, "_async_write_data", write)
    monkeypatch.setattr(Store, "async_load", load)
    async with _shared_ssh_lock(hass):
        with pytest.raises(ConnectionOwnershipError, match="address_change_incomplete"):
            await owner.async_commit(host="moved.office.test", password="fresh-value")
    assert failed
    assert await owner.journal.async_connection_intents()
    assert entry.subentries[panel.subentry_id].data[CONF_HOST] == "moved.office.test"


async def test_unrelated_issue_reconciliation_preserves_failed_notification_repair(
    hass: HomeAssistant, durable_connection_storage: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    from homeassistant.helpers import issue_registry as ir

    from custom_components.brilliant_mqtt.connection_owner import (
        ConnectionOwnershipError,
        PanelConnectionOwner,
        _issue_id,
        async_reconcile_connection_issues,
    )
    from custom_components.brilliant_mqtt.const import DOMAIN
    from custom_components.brilliant_mqtt.fleet_manager import _shared_ssh_lock

    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    entry.runtime_data = Mock()
    owner = PanelConnectionOwner(hass, entry, entry.subentries[panel.subentry_id])
    monkeypatch.setattr(
        hass.config_entries, "async_schedule_reload", Mock(side_effect=RuntimeError)
    )
    async with _shared_ssh_lock(hass):
        with pytest.raises(ConnectionOwnershipError):
            await owner.async_commit(host="moved.office.test", password="fresh-value")
    await async_reconcile_connection_issues(hass)
    assert ir.async_get(hass).async_get_issue(DOMAIN, _issue_id(owner.owner)) is not None


@pytest.mark.parametrize(
    "state", ["pending_only", "pending_active", "restore_requested", "finalizing"]
)
async def test_fleet_starts_healthy_sibling_while_affected_owner_is_deferred(
    hass: HomeAssistant,
    durable_connection_storage: None,
    monkeypatch: pytest.MonkeyPatch,
    state: str,
) -> None:
    from uuid import uuid4

    from homeassistant.helpers import issue_registry as ir

    from custom_components.brilliant_mqtt import fleet_manager as fm
    from custom_components.brilliant_mqtt.connection_owner import PanelConnectionOwner, _issue_id
    from custom_components.brilliant_mqtt.connection_state import ConnectionIntent
    from custom_components.brilliant_mqtt.const import DOMAIN
    from custom_components.brilliant_mqtt.manager import PanelManager
    from tests.test_config_flow import _OTHER_FINGERPRINT, _OTHER_PUBLIC_KEY

    office = _subentry()
    kitchen = _subentry(
        slug="kitchen",
        subentry_id="panel-kitchen",
        priority=2,
        fingerprint=_OTHER_FINGERPRINT,
        public_key=_OTHER_PUBLIC_KEY,
    )
    entry = _fleet_entry(hass, office, kitchen)
    owner = PanelConnectionOwner(hass, entry, entry.subentries[office.subentry_id])
    record = replace(
        _record(),
        prior_snapshot=replace(
            _record().prior_snapshot, baseline=RollbackBaseline("c" * 32, "d" * 64, {})
        ),
    )
    if state in {"restore_requested", "finalizing"}:
        await owner.journal.async_arm(record)
        await owner.journal.async_retained_state(record.transaction_id, state)
    else:
        if state == "pending_active":
            await owner.journal.async_create(record)
        intent = ConnectionIntent(
            str(uuid4()),
            owner.owner,
            owner.prior,
            replace(owner.prior, host="moved.office.test"),
            (),
        )
        await owner.journal.async_begin_connection_change(intent)
    setup = AsyncMock()
    monkeypatch.setattr(PanelManager, "async_setup", setup)
    monkeypatch.setattr(PanelManager, "async_shutdown", AsyncMock())
    monkeypatch.setattr(mqtt, "async_subscribe_connection_status", Mock(return_value=lambda: None))
    monkeypatch.setattr(mqtt, "is_connected", lambda _hass: True)
    monkeypatch.setattr(fm, "async_purge_stale_discovery_configs", AsyncMock())
    fleet = fm.FleetManager(hass, entry)
    await fleet.async_setup()
    expected_panels = {kitchen.subentry_id}
    if state in {"restore_requested", "finalizing"}:
        expected_panels.add(office.subentry_id)
    assert set(fleet.panels) == expected_panels
    assert setup.await_count == len(expected_panels)
    assert fleet._recovery_task is None
    issue_id = (
        _issue_id(owner.owner)
        if state.startswith("pending")
        else fm._provisioning_repair_issue_id(record.transaction_id)
    )
    assert ir.async_get(hass).async_get_issue(DOMAIN, issue_id) is not None
    if state in {"restore_requested", "finalizing"}:
        from functools import partial

        from homeassistant.config_entries import ConfigEntryState
        from homeassistant.core import ServiceCall
        from homeassistant.helpers import entity_registry as er

        from custom_components.brilliant_mqtt import _resolve_target_managers
        from custom_components.brilliant_mqtt.connection_owner import async_resolve_panel_owner
        from tests.test_panel_provisioner import _Harness

        entry.runtime_data = fleet
        entry.mock_state(hass, ConfigEntryState.LOADED)
        entity = er.async_get(hass).async_get_or_create(
            "binary_sensor",
            DOMAIN,
            "current-office",
            config_entry=entry,
            config_subentry_id=office.subentry_id,
        )
        call = ServiceCall(
            hass=hass,
            domain=DOMAIN,
            service="canary_rollback",
            data={"entity_id": entity.entity_id, "name": str(record.transaction_id)},
        )
        selected = _resolve_target_managers(hass, call)
        assert selected == [fleet.panels[office.subentry_id]]
        harness = _Harness()
        monkeypatch.setattr(harness, "journal", owner.journal)
        provisioner = harness.provisioner()
        monkeypatch.setattr(
            provisioner, "_named_owner_resolver", partial(async_resolve_panel_owner, hass)
        )
        monkeypatch.setattr(provisioner, "_async_recover_scoped", AsyncMock())
        monkeypatch.setattr(provisioner, "_async_finalize", AsyncMock())
        monkeypatch.setattr(fm, "_get_recovery_provisioner", lambda _hass: provisioner)
        await selected[0].async_canary_operation(
            "rollback" if state == "restore_requested" else "finalize", str(record.transaction_id)
        )
        retained = await owner.journal.async_retained(record.transaction_id)
        assert retained is not None and retained.record.panel_request.bound_owner == owner.owner
    await fleet.async_shutdown()


async def test_original_named_owner_removal_defers_scoped_dispatch(
    hass: HomeAssistant, durable_connection_storage: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    from custom_components.brilliant_mqtt import fleet_manager as fm
    from custom_components.brilliant_mqtt.connection_owner import PanelConnectionOwner
    from custom_components.brilliant_mqtt.connection_state import PanelOwner
    from tests.test_config_flow import _OTHER_FINGERPRINT, _OTHER_PUBLIC_KEY

    kitchen = _subentry(
        slug="kitchen",
        subentry_id="panel-kitchen",
        priority=2,
        fingerprint=_OTHER_FINGERPRINT,
        public_key=_OTHER_PUBLIC_KEY,
    )
    entry = _fleet_entry(hass, kitchen)
    journal = ProvisioningJournal(hass)
    record = replace(
        _record(),
        prior_snapshot=replace(
            _record().prior_snapshot, baseline=RollbackBaseline("c" * 32, "d" * 64, {})
        ),
    )
    await journal.async_arm(record)
    row = await journal.async_retained(record.transaction_id)
    assert row is not None
    row = await journal.async_bind_retained(
        row, PanelOwner(entry.entry_id, "removed-office", "office", record.panel_request.public_key)
    )
    await journal.async_begin_scoped_restore(row)
    fleet = fm.FleetManager(hass, entry)
    schedule = Mock()
    monkeypatch.setattr(fleet, "_async_schedule_background_recovery", schedule)
    await fleet._async_prepare_provisioning()
    schedule.assert_not_called()
    assert (
        PanelConnectionOwner(hass, entry, entry.subentries[kitchen.subentry_id]).owner.slug
        == "kitchen"
    )
    assert await journal.async_retained(record.transaction_id) is not None


@pytest.mark.parametrize("state", ["restore_requested", "finalizing"])
async def test_removed_owner_of_bound_legacy_marker_preserves_sibling_startup(
    hass: HomeAssistant,
    durable_connection_storage: None,
    monkeypatch: pytest.MonkeyPatch,
    state: str,
) -> None:
    from homeassistant.helpers import issue_registry as ir

    from custom_components.brilliant_mqtt import fleet_manager as fm
    from custom_components.brilliant_mqtt.connection_state import PanelOwner
    from custom_components.brilliant_mqtt.const import DOMAIN
    from custom_components.brilliant_mqtt.manager import PanelManager
    from tests.test_config_flow import _OTHER_FINGERPRINT, _OTHER_PUBLIC_KEY

    kitchen = _subentry(
        slug="kitchen",
        subentry_id="panel-kitchen",
        priority=2,
        fingerprint=_OTHER_FINGERPRINT,
        public_key=_OTHER_PUBLIC_KEY,
    )
    entry = _fleet_entry(hass, kitchen)
    journal = ProvisioningJournal(hass)
    record = replace(
        _record(),
        prior_snapshot=replace(
            _record().prior_snapshot, baseline=RollbackBaseline("c" * 32, "d" * 64, {})
        ),
    )
    await journal.async_arm(record)
    await journal.async_retained_state(record.transaction_id, state)
    row = await journal.async_retained(record.transaction_id)
    assert row is not None
    before = await journal.async_bind_retained(
        row, PanelOwner(entry.entry_id, "removed-office", "office", record.panel_request.public_key)
    )
    setup = AsyncMock()
    monkeypatch.setattr(PanelManager, "async_setup", setup)
    monkeypatch.setattr(PanelManager, "async_shutdown", AsyncMock())
    monkeypatch.setattr(mqtt, "async_subscribe_connection_status", Mock(return_value=lambda: None))
    monkeypatch.setattr(mqtt, "is_connected", lambda _hass: True)
    monkeypatch.setattr(fm, "async_purge_stale_discovery_configs", AsyncMock())
    fleet = fm.FleetManager(hass, entry)
    try:
        await fleet.async_setup()
        assert set(fleet.panels) == {kitchen.subentry_id}
        setup.assert_awaited_once()
        assert fleet._recovery_task is None
        assert await journal.async_retained(record.transaction_id) == before
        assert await journal.async_load() is None
        assert (
            ir.async_get(hass).async_get_issue(
                DOMAIN, fm._provisioning_repair_issue_id(record.transaction_id)
            )
            is not None
        )
    finally:
        await fleet.async_shutdown()


@pytest.mark.parametrize("state", ["restore_requested", "finalizing"])
async def test_removed_owner_of_bound_legacy_marker_preserves_unrelated_recovery(
    hass: HomeAssistant,
    durable_connection_storage: None,
    monkeypatch: pytest.MonkeyPatch,
    state: str,
) -> None:
    from functools import partial
    from uuid import uuid4

    from custom_components.brilliant_mqtt.connection_owner import async_resolve_panel_owner
    from custom_components.brilliant_mqtt.connection_state import PanelOwner
    from custom_components.brilliant_mqtt.fleet_manager import _async_rollback_credential
    from tests.test_config_flow import _OTHER_FINGERPRINT, _OTHER_PUBLIC_KEY
    from tests.test_panel_provisioner import _Harness

    kitchen = _subentry(
        slug="kitchen",
        subentry_id="panel-kitchen",
        priority=2,
        fingerprint=_OTHER_FINGERPRINT,
        public_key=_OTHER_PUBLIC_KEY,
    )
    entry = _fleet_entry(hass, kitchen)
    journal = ProvisioningJournal(hass)
    record = replace(
        _record(),
        prior_snapshot=replace(
            _record().prior_snapshot, baseline=RollbackBaseline("c" * 32, "d" * 64, {})
        ),
    )
    await journal.async_arm(record)
    row = await journal.async_retained(record.transaction_id)
    assert row is not None
    await journal.async_bind_retained(
        row, PanelOwner(entry.entry_id, "removed-office", "office", record.panel_request.public_key)
    )
    await journal.async_retained_state(record.transaction_id, state)
    before = await journal.async_retained(record.transaction_id)
    active = replace(
        _record(),
        transaction_id=uuid4(),
        panel_request=replace(
            record.panel_request,
            slug="kitchen",
            public_key=_OTHER_PUBLIC_KEY,
            fingerprint=_OTHER_FINGERPRINT,
        ),
    )
    await journal.async_create(active)
    harness = _Harness()
    monkeypatch.setattr(harness, "journal", journal)
    provisioner = harness.provisioner()
    monkeypatch.setattr(
        provisioner, "_named_owner_resolver", partial(async_resolve_panel_owner, hass)
    )
    monkeypatch.setattr(
        provisioner, "_credential_resolver", partial(_async_rollback_credential, hass)
    )
    monkeypatch.setattr(
        provisioner, "_connection_admission", journal.async_assert_connection_available
    )
    recover = AsyncMock()
    monkeypatch.setattr(provisioner, "_async_recover_record", recover)
    await provisioner.async_recover()
    recover.assert_awaited_once()
    assert recover.await_args is not None
    assert recover.await_args.args[0] == active
    assert await journal.async_retained(record.transaction_id) == before
    assert await journal.async_load() == active
    assert not any(event[0] == "shell_factory" for event in harness.events)


async def test_fresh_verified_notification_retry_uses_committed_tuple_without_new_intent(
    hass: HomeAssistant, durable_connection_storage: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    from custom_components.brilliant_mqtt.connection_owner import (
        ConnectionOwnershipError,
        PanelConnectionOwner,
    )
    from custom_components.brilliant_mqtt.fleet_manager import _shared_ssh_lock

    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    entry.runtime_data = Mock()
    stored = entry.subentries[panel.subentry_id]
    monkeypatch.setattr(
        hass.config_entries, "async_schedule_reload", Mock(side_effect=RuntimeError)
    )
    async with _shared_ssh_lock(hass):
        with pytest.raises(ConnectionOwnershipError):
            await PanelConnectionOwner(hass, entry, stored).async_commit(
                host="moved.office.test", password="fresh-login"
            )
    commit_id = stored.data["connection_commit_id"]
    assert not await ProvisioningJournal(hass).async_connection_intents()
    begin = AsyncMock(side_effect=AssertionError("already committed notification retry"))
    monkeypatch.setattr(ProvisioningJournal, "async_begin_connection_change", begin)
    notify = Mock()
    monkeypatch.setattr(hass.config_entries, "async_schedule_reload", notify)
    async with _shared_ssh_lock(hass):
        await PanelConnectionOwner(hass, entry, stored).async_commit(
            host="moved.office.test", password="fresh-login"
        )
    assert stored.data["connection_commit_id"] == commit_id
    begin.assert_not_awaited()
    notify.assert_called_once_with(entry.entry_id)


async def test_named_rollback_bounds_authorization_before_any_marker(
    hass: HomeAssistant, durable_connection_storage: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    import asyncio

    from custom_components.brilliant_mqtt import panel_provisioner as pp
    from custom_components.brilliant_mqtt.connection_state import PanelOwner
    from custom_components.brilliant_mqtt.provisioning_journal import StoredPanelRequest
    from tests.test_panel_provisioner import _Harness

    record = replace(
        _record(),
        prior_snapshot=replace(
            _record().prior_snapshot, baseline=RollbackBaseline("c" * 32, "d" * 64, {})
        ),
    )
    journal = ProvisioningJournal(hass)
    await journal.async_arm(record)
    before = await journal.async_retained(record.transaction_id)
    harness = _Harness()
    monkeypatch.setattr(harness, "journal", journal)
    provisioner = harness.provisioner()

    async def blocked(_request: StoredPanelRequest) -> PanelOwner:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    monkeypatch.setattr(provisioner, "_named_owner_resolver", blocked)
    monkeypatch.setattr(pp, "RECOVERY_TIMEOUT_SECONDS", 0.01)
    async with asyncio.timeout(0.25):
        with pytest.raises(pp.PanelProvisioningError, match="rollback_deadline_exceeded"):
            await provisioner.async_rollback(record.transaction_id)
    assert await journal.async_retained(record.transaction_id) == before
    assert not provisioner._lock.locked()


async def test_named_finalize_bounds_authorization_before_any_marker(
    hass: HomeAssistant, durable_connection_storage: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    import asyncio

    from custom_components.brilliant_mqtt import panel_provisioner as pp
    from custom_components.brilliant_mqtt.connection_state import PanelOwner
    from custom_components.brilliant_mqtt.provisioning_journal import StoredPanelRequest
    from tests.test_panel_provisioner import _Harness

    record = replace(
        _record(),
        prior_snapshot=replace(
            _record().prior_snapshot, baseline=RollbackBaseline("c" * 32, "d" * 64, {})
        ),
    )
    journal = ProvisioningJournal(hass)
    await journal.async_arm(record)
    before = await journal.async_retained(record.transaction_id)
    harness = _Harness()
    monkeypatch.setattr(harness, "journal", journal)
    provisioner = harness.provisioner()

    async def blocked(_request: StoredPanelRequest) -> PanelOwner:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    monkeypatch.setattr(provisioner, "_named_owner_resolver", blocked)
    monkeypatch.setattr(pp, "RECOVERY_TIMEOUT_SECONDS", 0.01)
    async with asyncio.timeout(5):
        with pytest.raises(pp.PanelProvisioningError, match="rollback_deadline_exceeded"):
            await provisioner.async_finalize(record.transaction_id)
    assert await journal.async_retained(record.transaction_id) == before
    assert not provisioner._lock.locked()
    assert not any(event[0] == "shell_factory" for event in harness.events)


async def test_queued_explicit_finalize_refuses_disappeared_target(
    hass: HomeAssistant, durable_connection_storage: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    import asyncio

    from custom_components.brilliant_mqtt.panel_provisioner import PanelProvisioningError
    from tests.test_panel_provisioner import _Harness

    record = replace(
        _record(),
        prior_snapshot=replace(
            _record().prior_snapshot, baseline=RollbackBaseline("c" * 32, "d" * 64, {})
        ),
    )
    journal = ProvisioningJournal(hass)
    await journal.async_arm(record)
    harness = _Harness()
    monkeypatch.setattr(harness, "journal", journal)
    provisioner = harness.provisioner()
    await provisioner._lock.acquire()
    task = asyncio.create_task(provisioner.async_finalize(record.transaction_id))
    await asyncio.sleep(0)
    await journal.async_retained_state(record.transaction_id, "finalizing")
    await journal.async_remove_retained(record.transaction_id)
    provisioner._lock.release()
    with pytest.raises(PanelProvisioningError, match="baseline_incomplete"):
        await task
    assert not any(event[0] == "shell_factory" for event in harness.events)


async def test_named_finalize_cancellation_keeps_lock_until_remote_write_settles(
    hass: HomeAssistant, durable_connection_storage: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    import asyncio

    from tests.test_panel_provisioner import _Harness

    record = replace(
        _record(),
        prior_snapshot=replace(
            _record().prior_snapshot, baseline=RollbackBaseline("c" * 32, "d" * 64, {})
        ),
    )
    journal = ProvisioningJournal(hass)
    await journal.async_arm(record)
    harness = _Harness()
    monkeypatch.setattr(harness, "journal", journal)
    provisioner = harness.provisioner()
    monkeypatch.setattr(provisioner, "_shell_factory", Mock(return_value=harness.shell))
    monkeypatch.setattr(provisioner, "_credential_resolver", AsyncMock(return_value="fresh-login"))
    started, release, settled = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def remote_write(*_args: Any) -> None:
        started.set()
        await release.wait()
        settled.set()

    monkeypatch.setattr(harness.operations, "finalize_baseline", remote_write)
    task = asyncio.create_task(provisioner.async_finalize(record.transaction_id))
    await started.wait()
    task.cancel()
    await asyncio.sleep(0)
    try:
        assert not task.done()
        assert provisioner._lock.locked()
        assert not settled.is_set()
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert settled.is_set()
    assert await journal.async_retained(record.transaction_id) is None


@pytest.mark.parametrize("replace_id", [False, True])
@pytest.mark.parametrize("during_removal", [False, True])
async def test_commit_id_drift_preserves_intent_through_retirement(
    hass: HomeAssistant,
    durable_connection_storage: None,
    monkeypatch: pytest.MonkeyPatch,
    replace_id: bool,
    during_removal: bool,
) -> None:
    from uuid import uuid4

    from custom_components.brilliant_mqtt import connection_owner as co

    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    stored = entry.subentries[panel.subentry_id]
    owner = co.PanelConnectionOwner(hass, entry, stored)
    finish = ProvisioningJournal.async_finish_connection_change
    remove = Store.async_remove
    notified = Mock()
    monkeypatch.setattr(owner, "_enqueue_runtime", notified)

    def drift() -> None:
        data = dict(stored.data)
        if replace_id:
            data["connection_commit_id"] = str(uuid4())
        else:
            data.pop("connection_commit_id")
        hass.config_entries.async_update_subentry(entry, stored, data=data)

    async def finish_with_drift(journal: ProvisioningJournal, intent: Any, **kwargs: Any) -> None:
        if not during_removal:
            drift()
        await finish(journal, intent, **kwargs)

    async def remove_with_drift(store: Store) -> None:
        if store.key == JOURNAL_STORAGE_KEY and during_removal:
            drift()
        await remove(store)

    monkeypatch.setattr(ProvisioningJournal, "async_finish_connection_change", finish_with_drift)
    monkeypatch.setattr(Store, "async_remove", remove_with_drift)
    with pytest.raises(co.ConnectionOwnershipError):
        await owner.async_commit(host="moved.office.test", password="fresh-login")
    pending = await owner.journal.async_connection_intents()
    assert len(pending) == 1
    assert pending[0].owner == owner.owner
    assert stored.data[CONF_HOST] == "moved.office.test"
    assert stored.data[CONF_ROOT_PASSWORD] == "fresh-login"
    notified.assert_not_called()


async def test_commit_retirement_requires_current_commit_id_on_disk(
    hass: HomeAssistant, durable_connection_storage: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    from custom_components.brilliant_mqtt import connection_owner as co
    from custom_components.brilliant_mqtt import fleet_manager as fm

    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    owner = co.PanelConnectionOwner(hass, entry, entry.subentries[panel.subentry_id])
    finish = ProvisioningJournal.async_finish_connection_change
    drifted = False

    async def disk_drift(journal: ProvisioningJournal, intent: Any, **kwargs: Any) -> None:
        nonlocal drifted
        path = Path(hass.config.path(".storage", "core.config_entries"))
        raw = json.loads(await hass.async_add_executor_job(path.read_text))
        stored = next(item for item in raw["data"]["entries"] if item["entry_id"] == entry.entry_id)
        subentry = next(
            item for item in stored["subentries"] if item["subentry_id"] == panel.subentry_id
        )
        subentry["data"].pop("connection_commit_id")
        await hass.async_add_executor_job(path.write_text, json.dumps(raw))
        drifted = True
        monkeypatch.setattr(fm, "_CONFIG_ENTRY_PERSISTENCE_TIMEOUT_SECONDS", 0.01)
        await finish(journal, intent, **kwargs)

    monkeypatch.setattr(ProvisioningJournal, "async_finish_connection_change", disk_drift)
    notify = Mock()
    monkeypatch.setattr(owner, "_enqueue_runtime", notify)
    with pytest.raises(co.ConnectionOwnershipError, match="address_change_incomplete"):
        await owner.async_commit(host="moved.office.test", password="fresh-login")
    assert drifted
    assert len(await owner.journal.async_connection_intents()) == 1
    notify.assert_not_called()


async def test_cancelled_retirement_with_disk_drift_restores_pending_evidence(
    hass: HomeAssistant, durable_connection_storage: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    import asyncio

    from custom_components.brilliant_mqtt import connection_owner as co
    from custom_components.brilliant_mqtt import fleet_manager as fm

    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    owner = co.PanelConnectionOwner(hass, entry, entry.subentries[panel.subentry_id])
    remove = ProvisioningJournal._async_remove

    async def cancel_after_drift(journal: ProvisioningJournal) -> None:
        await remove(journal)
        path = Path(hass.config.path(".storage", "core.config_entries"))
        raw = json.loads(await hass.async_add_executor_job(path.read_text))
        stored = next(item for item in raw["data"]["entries"] if item["entry_id"] == entry.entry_id)
        subentry = next(
            item for item in stored["subentries"] if item["subentry_id"] == panel.subentry_id
        )
        subentry["data"].pop("connection_commit_id")
        await hass.async_add_executor_job(path.write_text, json.dumps(raw))
        monkeypatch.setattr(fm, "_CONFIG_ENTRY_PERSISTENCE_TIMEOUT_SECONDS", 0.01)
        raise asyncio.CancelledError

    monkeypatch.setattr(ProvisioningJournal, "_async_remove", cancel_after_drift)
    notify = Mock()
    monkeypatch.setattr(owner, "_enqueue_runtime", notify)
    with pytest.raises(asyncio.CancelledError):
        await owner.async_commit(host="moved.office.test", password="fresh-login")
    assert len(await owner.journal.async_connection_intents()) == 1
    notify.assert_not_called()


async def test_pending_intent_login_is_redacted_from_active_save_failure(
    hass: HomeAssistant, durable_connection_storage: None, caplog: pytest.LogCaptureFixture
) -> None:
    from custom_components.brilliant_mqtt.provisioning_journal import (
        ProvisioningJournalError,
        ProvisioningPhase,
    )

    _entry, _stored, intent = await pending_move(hass)
    journal = ProvisioningJournal(hass)
    active = _record()
    await journal.async_create(active)
    secret = intent.proposed.password
    with patch.object(journal._store, "async_save", AsyncMock(side_effect=OSError(secret))):
        with pytest.raises(ProvisioningJournalError) as caught:
            await journal.async_transition(
                active.transaction_id, ProvisioningPhase.ACTIVATION_PENDING
            )
    assert secret not in str(caught.value)
    assert secret not in str(caught.value.__cause__)
    assert secret not in caplog.text


async def test_named_admission_storage_error_is_typed_before_transport() -> None:
    from custom_components.brilliant_mqtt.panel_provisioner import PanelProvisioningError
    from custom_components.brilliant_mqtt.provisioning_journal import ProvisioningJournalError
    from tests.test_panel_provisioner import _Harness

    harness = _Harness()
    provisioner = harness.provisioner()
    provisioner._connection_admission = AsyncMock(
        side_effect=ProvisioningJournalError("journal_load_failed")
    )
    with pytest.raises(PanelProvisioningError, match="journal_failed"):
        await provisioner._async_admit_connection(_record().panel_request)
    assert not any(event[0] == "shell_factory" for event in harness.events)


async def test_independent_intents_survive_other_owner_completion_and_cold_load(
    hass: HomeAssistant, durable_connection_storage: None
) -> None:
    from uuid import uuid4

    from custom_components.brilliant_mqtt.connection_state import ConnectionIntent, PanelOwner

    _entry, _stored, first = await pending_move(hass)
    second = ConnectionIntent(
        str(uuid4()),
        PanelOwner("second-entry", None, "kitchen", "ssh-second-key"),
        replace(first.prior, host="kitchen.test", public_key="ssh-second-key"),
        replace(first.proposed, host="moved.kitchen.test", public_key="ssh-second-key"),
        (),
    )
    journal = ProvisioningJournal(hass)
    await journal.async_begin_connection_change(second)
    await journal.async_finish_connection_change(first)
    assert await ProvisioningJournal(hass).async_connection_intents() == (second,)
    assert set(disk(hass, JOURNAL_STORAGE_KEY)["connection_intents"]) == {second.commit_id}
    active = _record()
    await journal.async_create(active)
    await journal.async_complete_cleanup(active.transaction_id)
    assert await ProvisioningJournal(hass).async_load() is None
    assert await ProvisioningJournal(hass).async_connection_intents() == (second,)


async def test_passive_journal_reads_never_replay_pending_connection_over_later_config(
    hass: HomeAssistant, durable_connection_storage: None
) -> None:
    from custom_components.brilliant_mqtt.connection_owner import async_reconcile_connection_issues

    entry, stored, intent = await pending_move(hass)
    later = {**stored.data, CONF_HOST: "later.office.test", CONF_ROOT_PASSWORD: "later-login"}
    hass.config_entries.async_update_subentry(entry, stored, data=later)
    await ProvisioningJournal(hass).async_load()
    await async_reconcile_connection_issues(hass)
    await ProvisioningJournal(hass).async_diagnostics()
    assert dict(stored.data) == later
    assert await ProvisioningJournal(hass).async_connection_intents() == (intent,)


@pytest.mark.parametrize("algorithm", ["ecdsa-sha2-nistp256", "ssh-rsa"])
async def test_legacy_same_key_long_slug_connection_keeps_identity(
    hass: HomeAssistant, durable_connection_storage: None, algorithm: str
) -> None:
    from tests.test_config_flow import _legacy_entry, _open_legacy_reconfigure_step
    from tests.test_migration import host_key

    entry = _legacy_entry(hass)
    long_slug = "legacy-" + "a" * 80
    key = host_key("legacy", algorithm)
    hass.config_entries.async_update_entry(
        entry, data={**entry.data, "panel": long_slug, "ssh_host_key": key}
    )
    form = await _open_legacy_reconfigure_step(hass, entry, "reconfigure_connection")
    apply = AsyncMock(return_value=key)
    with patch.object(gateway, "_apply_config", apply):
        result = await hass.config_entries.flow.async_configure(
            form["flow_id"], {CONF_HOST: "moved.office.test", CONF_ROOT_PASSWORD: ""}
        )
    assert result["type"] == "abort", result
    assert result["reason"] == "reconfigure_successful"
    assert entry.data["panel"] == long_slug
    assert entry.data["ssh_host_key"] == key
    assert apply.call_args.kwargs["pinned_key"] == key


async def test_connection_preflight_storage_failure_is_retryable_without_config_write(
    hass: HomeAssistant, durable_connection_storage: None
) -> None:
    from custom_components.brilliant_mqtt.connection_owner import (
        ConnectionOwnershipError,
        PanelConnectionOwner,
    )
    from custom_components.brilliant_mqtt.provisioning_journal import ProvisioningJournalError

    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    stored = entry.subentries[panel.subentry_id]
    before = dict(stored.data)
    owner = PanelConnectionOwner(hass, entry, stored)
    with patch.object(
        ProvisioningJournal,
        "async_load",
        AsyncMock(side_effect=ProvisioningJournalError("journal_load_failed")),
    ):
        with pytest.raises(ConnectionOwnershipError, match="address_change_incomplete"):
            await owner.async_commit(host="moved.office.test", password="fresh-login")
    assert dict(stored.data) == before


async def test_legacy_pending_connection_refuses_other_section_before_transport(
    hass: HomeAssistant, durable_connection_storage: None
) -> None:
    from uuid import uuid4

    from custom_components.brilliant_mqtt.connection_owner import PanelConnectionOwner
    from custom_components.brilliant_mqtt.connection_state import ConnectionIntent
    from tests.test_config_flow import _legacy_entry, _open_legacy_reconfigure_step

    entry = _legacy_entry(hass)
    owner = PanelConnectionOwner(hass, entry)
    await owner.journal.async_begin_connection_change(
        ConnectionIntent(str(uuid4()), owner.owner, owner.prior, owner.prior, ())
    )
    form = await _open_legacy_reconfigure_step(hass, entry, "reconfigure_advanced")
    apply = AsyncMock()
    with patch.object(gateway, "_apply_config", apply):
        result = await hass.config_entries.flow.async_configure(
            form["flow_id"], {"mesh_priority": 2}
        )
    assert result["type"] == "form", result
    assert result["errors"] == {"base": "address_change_incomplete"}
    apply.assert_not_awaited()


async def test_scoped_restored_evidence_still_refuses_finalize_before_cleanup(
    hass: HomeAssistant, durable_connection_storage: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    from custom_components.brilliant_mqtt.panel_provisioner import PanelProvisioningError
    from tests.test_panel_provisioner import _Harness, _release_link_snapshot

    target = replace(_record(), prior_snapshot=_release_link_snapshot())
    journal = ProvisioningJournal(hass)
    await journal.async_arm(target)
    retained = await journal.async_retained(target.transaction_id)
    assert retained is not None
    await journal.async_begin_scoped_restore(retained)
    await journal.async_retained_state(target.transaction_id, "restored", evidence="a" * 32)
    before = await journal.async_retained(target.transaction_id)
    assert before is not None and before.recovery_record is not None
    harness = _Harness()
    monkeypatch.setattr(harness, "journal", journal)
    provisioner = harness.provisioner()
    authorize = AsyncMock(side_effect=AssertionError("must refuse before new authorization"))
    monkeypatch.setattr(provisioner, "_named_owner_resolver", authorize)
    with pytest.raises(PanelProvisioningError, match="provisioning_recovery_required"):
        await provisioner.async_finalize(target.transaction_id)
    authorize.assert_not_awaited()
    assert not any(event[0] == "shell_factory" for event in harness.events)
    assert await journal.async_retained(target.transaction_id) == before


async def test_same_transaction_active_rollback_continues_without_scoped_reseed(
    hass: HomeAssistant, durable_connection_storage: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tests.test_panel_provisioner import _Harness, _release_link_snapshot

    target = replace(_record(), prior_snapshot=_release_link_snapshot())
    journal = ProvisioningJournal(hass)
    await journal.async_arm(target)
    await journal.async_request_restore(target.transaction_id)
    await journal.async_begin_restore(target.transaction_id, "current-login")
    active = await journal.async_load()
    assert active is not None and active.operation is ProvisioningOperation.ROLLBACK
    harness = _Harness()
    monkeypatch.setattr(harness, "journal", journal)
    provisioner = harness.provisioner()
    execute = AsyncMock()
    monkeypatch.setattr(provisioner, "_async_recover_record", execute)
    with patch.object(
        ProvisioningJournal,
        "async_begin_scoped_restore",
        AsyncMock(side_effect=AssertionError("no reseed")),
    ):
        await provisioner.async_rollback(target.transaction_id)
    execute.assert_awaited_once()
    assert execute.call_args.args[0] == active
    assert await journal.async_load() == active
    retained = await journal.async_retained(target.transaction_id)
    assert retained is not None and retained.recovery_record is None


@pytest.mark.parametrize("legacy", [False, True])
async def test_verified_connection_preserves_concurrent_rename_and_unrelated_section(
    hass: HomeAssistant, durable_connection_storage: None, legacy: bool
) -> None:
    from tests.test_config_flow import _legacy_entry, _open_legacy_reconfigure_step

    if legacy:
        entry = _legacy_entry(hass)
        form = await _open_legacy_reconfigure_step(hass, entry, "reconfigure_connection")
        owner_id = None
    else:
        panel = _subentry()
        entry = _fleet_entry(hass, panel)
        owner_id = panel.subentry_id
        form = await _start_panel_reconfigure_step(
            hass, entry, entry.subentries[owner_id], "address"
        )

    async def rename(*_args: Any, **_kwargs: Any) -> str:
        data = entry.data if owner_id is None else entry.subentries[owner_id].data
        fresh = {**data, "name": "New concurrent name", "mesh_priority": 7}
        if owner_id is None:
            hass.config_entries.async_update_entry(entry, data=fresh)
        else:
            hass.config_entries.async_update_subentry(entry, entry.subentries[owner_id], data=fresh)
        return _identity().public_key

    result: Any
    with (
        patch.object(gateway, "async_fetch_host_identity", AsyncMock(return_value=_identity())),
        patch.object(gateway, "_async_inspect_candidate", rename),
        patch.object(gateway, "_apply_config", rename),
    ):
        values = {CONF_HOST: "moved.office.test"}
        if legacy:
            values[CONF_ROOT_PASSWORD] = ""
            result = await hass.config_entries.flow.async_configure(form["flow_id"], values)
        else:
            result = await hass.config_entries.subentries.async_configure(form["flow_id"], values)
    assert result["type"] == "abort", result
    assert result["reason"] == "reconfigure_successful"
    current = entry.data if owner_id is None else entry.subentries[owner_id].data
    assert current[CONF_HOST] == "moved.office.test"
    assert current["name"] == "New concurrent name"
    assert current["mesh_priority"] == 7


@pytest.mark.parametrize("cancelled", [False, True])
async def test_postcommit_issue_failure_still_enqueues_committed_runtime(
    hass: HomeAssistant, durable_connection_storage: None, cancelled: bool
) -> None:
    import asyncio

    from custom_components.brilliant_mqtt import connection_owner as co
    from custom_components.brilliant_mqtt.provisioning_journal import ProvisioningJournalError

    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    owner = co.PanelConnectionOwner(hass, entry, entry.subentries[panel.subentry_id])
    failure = (
        asyncio.CancelledError() if cancelled else ProvisioningJournalError("journal_load_failed")
    )
    notified = Mock()
    expected = asyncio.CancelledError if cancelled else co.ConnectionOwnershipError
    with (
        patch.object(owner, "_enqueue_runtime", notified),
        patch.object(co, "async_reconcile_connection_issues", AsyncMock(side_effect=failure)),
    ):
        with pytest.raises(expected):
            await owner.async_commit(host="moved.office.test", password="fresh-login")
    assert not await owner.journal.async_connection_intents()
    notified.assert_called_once()


async def test_migration_startup_reuses_held_lock_and_still_defers_pending_owner(
    hass: HomeAssistant, durable_connection_storage: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    import asyncio

    from custom_components.brilliant_mqtt.fleet_manager import FleetManager, _shared_ssh_lock
    from custom_components.brilliant_mqtt.migration_coordinator import migration_fence

    entry, stored, _intent = await pending_move(hass)
    fence = migration_fence(hass)
    monkeypatch.setattr(fence, "operation_lock_entries", frozenset({entry.entry_id}), raising=False)
    fleet = FleetManager(hass, entry)
    async with _shared_ssh_lock(hass):
        async with asyncio.timeout(5):
            await fleet._async_prepare_provisioning()
            assert await fleet._async_complete_pending_handoff() is False
    assert fleet._deferred_panel_ids == {stored.subentry_id}


async def test_queued_reconcile_does_not_hold_lifecycle_while_waiting_for_operation_lock(
    hass: HomeAssistant,
) -> None:
    import asyncio

    from custom_components.brilliant_mqtt.fleet_manager import FleetManager, _shared_ssh_lock

    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    fleet = FleetManager(hass, entry)
    async with _shared_ssh_lock(hass):
        task = asyncio.create_task(fleet._async_reconcile_once())
        try:
            for _ in range(10):
                await asyncio.sleep(0)
            assert not fleet._lifecycle_lock.locked()
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
