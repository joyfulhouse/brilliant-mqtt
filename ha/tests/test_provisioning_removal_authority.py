"""Removal capabilities and committed cleanup must preserve exact ownership."""

import asyncio
import json
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers import issue_registry as ir

from custom_components.brilliant_mqtt import fleet_manager as fm
from custom_components.brilliant_mqtt import panel_ops
from custom_components.brilliant_mqtt.const import (
    CONF_PROVISIONING_TRANSACTION_ID,
    CONF_SCENE_PANEL,
    DOMAIN,
)
from custom_components.brilliant_mqtt.entry_data import EntryDataError
from custom_components.brilliant_mqtt.panel_provisioner import (
    PanelProvisioner,
    PanelProvisioningError,
)
from custom_components.brilliant_mqtt.provisioning_journal import (
    ProvisioningJournal,
    ProvisioningJournalError,
    ProvisioningOperation,
    ProvisioningPhase,
    ProvisioningRecord,
    Store,
    StoredJournalError,
)
from tests.fakes import FakeShell
from tests.test_fleet_manager import (
    _OFFICE_FINGERPRINT,
    _OTHER_FINGERPRINT,
    _OTHER_PUBLIC_KEY,
    _disk_config_entries_envelope,
    _empty_fleet_entry,
    _panel,
    _pending_owner_entry,
    _pending_record,
    _provisioning_entry,
)
from tests.test_panel_provisioner import _record
from tests.test_provisioning_journal import _install_real_store


@pytest.mark.parametrize(
    "operation", (ProvisioningOperation.INSTALL, ProvisioningOperation.UPGRADE)
)
@pytest.mark.parametrize("change", (None, "replacement", "journal", "survivor", "error"))
async def test_actual_removal_uses_only_exact_retired_owner_authority(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    enable_custom_integrations: None,
    operation: ProvisioningOperation,
    change: str | None,
) -> None:
    _install_real_store(hass, monkeypatch, tmp_path)
    journal = ProvisioningJournal(hass)
    record = replace(
        _record(ProvisioningPhase.STAGED),
        operation=operation,
        fleet_profile=_pending_record().fleet_profile,
    )
    await journal.async_create(record)
    entry = _empty_fleet_entry()
    entry.add_to_hass(hass)
    shell = Mock(side_effect=lambda host, password, pin: FakeShell(pinned=pin))
    cleanup = AsyncMock()
    monkeypatch.setattr(fm, "AsyncsshShell", shell)
    monkeypatch.setattr(panel_ops, "cleanup_staged", cleanup)
    if change == "survivor":
        _empty_fleet_entry().add_to_hass(hass)
        await hass.config_entries.async_remove(entry.entry_id)
    else:
        queued = asyncio.Event()
        factory = fm._get_recovery_provisioner

        def queued_factory(
            target_hass: HomeAssistant,
            *,
            removed_entry: ConfigEntry[Any] | None = None,
            expected_record: ProvisioningRecord | None = None,
        ) -> PanelProvisioner:
            result = factory(
                target_hass, removed_entry=removed_entry, expected_record=expected_record
            )
            queued.set()
            return result

        monkeypatch.setattr(fm, "_get_recovery_provisioner", queued_factory)
        async with fm._shared_ssh_lock(hass):
            removal = hass.async_create_task(hass.config_entries.async_remove(entry.entry_id))
            await asyncio.wait_for(queued.wait(), timeout=2)
            assert hass.config_entries.async_get_entry(entry.entry_id) is None
            if change == "replacement":
                _empty_fleet_entry().add_to_hass(hass)
            elif change == "journal":
                await journal.async_complete_cleanup(record.transaction_id)
                record = replace(
                    record,
                    transaction_id=uuid4(),
                    setup_id=uuid4(),
                    panel_request=replace(
                        record.panel_request,
                        public_key=_OTHER_PUBLIC_KEY,
                        fingerprint=_OTHER_FINGERPRINT,
                    ),
                )
                await journal.async_create(record)
                _empty_fleet_entry().add_to_hass(hass)
            elif change == "error":
                record = await journal.async_record_error(
                    record.transaction_id,
                    StoredJournalError(stage="recovery", code="recovery_connection_failed"),
                )
                await fm._ProvisioningRepairReporter(hass).async_report_rollback_failure(
                    record.transaction_id,
                    original_code="restart_recovery",
                    rollback_code="recovery_connection_failed",
                )
        await removal
    remaining = await journal.async_load()
    if change is None:
        assert remaining is None
        shell.assert_called_once()
        cleanup.assert_awaited_once()
        assert (
            ir.async_get(hass).async_get_issue(
                DOMAIN, fm._provisioning_repair_issue_id(record.transaction_id)
            )
            is None
        )
    else:
        assert remaining == record
        shell.assert_not_called()
        cleanup.assert_not_awaited()
        if change == "error":
            issue = ir.async_get(hass).async_get_issue(
                DOMAIN, fm._provisioning_repair_issue_id(record.transaction_id)
            )
            assert issue is not None and issue.translation_placeholders is not None
            assert issue.translation_placeholders["reason"].endswith(
                "restart_recovery; recovery_connection_failed"
            )


@pytest.mark.parametrize(
    "operation", (ProvisioningOperation.INSTALL, ProvisioningOperation.UPGRADE)
)
@pytest.mark.parametrize("owner", ("missing", "marked", "dangling_scene", "incumbent", "exact"))
async def test_generic_committed_cleanup_requires_complete_config_ownership(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    operation: ProvisioningOperation,
    owner: str,
) -> None:
    _install_real_store(hass, monkeypatch, tmp_path)
    journal = ProvisioningJournal(hass)
    record = replace(_pending_record(phase=ProvisioningPhase.STAGED), operation=operation)
    await journal.async_create(record)
    for phase in (
        ProvisioningPhase.ACTIVATION_PENDING,
        ProvisioningPhase.ACTIVATED,
        ProvisioningPhase.VERIFYING,
        ProvisioningPhase.PENDING_CONFIG_COMMIT,
    ):
        await journal.async_transition(record.transaction_id, phase)
    with monkeypatch.context() as fault:
        fault.setattr(
            journal._store, "async_remove", AsyncMock(side_effect=OSError("remove failed"))
        )
        with pytest.raises(ProvisioningJournalError):
            await journal.async_complete_commit(record.transaction_id, subentry_id="panel-office")
    committed = await journal.async_load()
    assert committed is not None and committed.phase is ProvisioningPhase.COMMITTED
    if owner == "missing":
        entry = _empty_fleet_entry()
    else:
        office = _pending_owner_entry().subentries["panel-office"]
        entry = _provisioning_entry(
            office,
            _panel("kitchen", "SHA256:kitchen", subentry_id="panel-kitchen"),
            scene_owner=(
                "missing-panel"
                if owner == "dangling_scene"
                else "panel-kitchen"
                if owner == "incumbent"
                else "panel-office"
            ),
        )
    entry.add_to_hass(hass)
    if owner not in {"missing", "marked"}:
        panel = entry.subentries["panel-office"]
        data = dict(panel.data)
        del data[CONF_PROVISIONING_TRANSACTION_ID]
        hass.config_entries.async_update_subentry(entry, panel, data=data)
    shell = Mock(side_effect=AssertionError("committed cleanup never needs SSH"))
    monkeypatch.setattr(fm, "AsyncsshShell", shell)
    provisioner = fm._get_recovery_provisioner(hass)
    if owner in {"exact", "incumbent"}:
        await provisioner.async_recover()
        assert await journal.async_load() is None
    else:
        with pytest.raises(EntryDataError, match="provisioning_ownership_mismatch"):
            await fm.FleetManager(hass, entry)._async_prepare_provisioning()
        with pytest.raises(PanelProvisioningError, match="provisioning_recovery_required"):
            await provisioner.async_recover()
        assert await journal.async_load() == committed
        assert (
            ir.async_get(hass).async_get_issue(
                DOMAIN, fm._provisioning_repair_issue_id(record.transaction_id)
            )
            is not None
        )
    shell.assert_not_called()


@pytest.mark.parametrize(
    "operation", (ProvisioningOperation.INSTALL, ProvisioningOperation.UPGRADE)
)
@pytest.mark.parametrize("recovery", ("generic", "fleet"))
async def test_second_panel_handoff_retries_committed_cleanup_with_incumbent_scene_owner(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    operation: ProvisioningOperation,
    recovery: str,
) -> None:
    _install_real_store(hass, monkeypatch, tmp_path)
    journal = ProvisioningJournal(hass)
    entry = _provisioning_entry(
        _panel("kitchen", "SHA256:kitchen", subentry_id="panel-kitchen", mesh_priority=1),
        scene_owner="panel-kitchen",
        next_mesh_priority=2,
    )
    entry.add_to_hass(hass)
    fleet = fm.FleetManager(hass, entry)
    record = replace(_pending_record(phase=ProvisioningPhase.STAGED), operation=operation)
    await journal.async_create(record)
    for phase in (
        ProvisioningPhase.ACTIVATION_PENDING,
        ProvisioningPhase.ACTIVATED,
        ProvisioningPhase.VERIFYING,
        ProvisioningPhase.PENDING_CONFIG_COMMIT,
    ):
        await journal.async_transition(record.transaction_id, phase)
    added = _panel(
        "office",
        "SHA256:office",
        subentry_id="panel-office",
        management_id=_OFFICE_FINGERPRINT,
        mesh_priority=2,
        transaction_id=record.transaction_id,
    )
    assert hass.config_entries.async_add_subentry(entry, added)
    wait = fm.async_wait_config_entry_persisted

    async def persist(
        target_hass: HomeAssistant,
        target_entry: ConfigEntry[Any],
        *,
        subentry_id: str | None = None,
    ) -> None:
        assert target_entry is entry
        path = Path(target_hass.config.path(".storage/core.config_entries"))
        await target_hass.async_add_executor_job(
            path.write_text, json.dumps(_disk_config_entries_envelope(entry))
        )
        await wait(target_hass, target_entry, subentry_id=subentry_id)

    monkeypatch.setattr(fm, "async_wait_config_entry_persisted", persist)
    await fleet._async_prepare_provisioning()
    assert fleet._pending_handoff is not None
    assert fleet._pending_handoff.subentry_id == added.subentry_id
    fleet._panels[added.subentry_id] = Mock(
        store=Mock(subentry_id=added.subentry_id), panel="office", management_id=_OFFICE_FINGERPRINT
    )
    fleet._runtime_owned_panel_ids.add(added.subentry_id)
    with monkeypatch.context() as fault:
        fault.setattr(Store, "async_remove", AsyncMock(side_effect=OSError("final unlink failed")))
        with pytest.raises(ConfigEntryNotReady, match="journal commit has not completed"):
            await fleet._async_complete_pending_handoff()
    committed = await journal.async_load()
    assert committed is not None and committed.phase is ProvisioningPhase.COMMITTED
    assert entry.data[CONF_SCENE_PANEL] == "panel-kitchen"
    assert CONF_PROVISIONING_TRANSACTION_ID not in entry.subentries[added.subentry_id].data
    shell = Mock(side_effect=AssertionError("committed cleanup never needs SSH"))
    monkeypatch.setattr(fm, "AsyncsshShell", shell)
    if recovery == "generic":
        await fm._get_recovery_provisioner(hass).async_recover()
    else:
        await fleet._async_prepare_provisioning()
    assert await journal.async_load() is None
    assert entry.data[CONF_SCENE_PANEL] == "panel-kitchen"
    shell.assert_not_called()
