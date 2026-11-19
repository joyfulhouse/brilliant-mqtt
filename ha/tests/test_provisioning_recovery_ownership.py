"""Queued fleet recovery must retain the exact transaction's config authority."""

from dataclasses import replace
from functools import partial
from pathlib import Path
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.helpers import issue_registry as ir

from custom_components.brilliant_mqtt import fleet_manager as fm
from custom_components.brilliant_mqtt.const import CONF_HOST, CONF_ROOT_PASSWORD, DOMAIN
from custom_components.brilliant_mqtt.entry_data import EntryDataError
from custom_components.brilliant_mqtt.panel_provisioner import PanelProvisioningError
from custom_components.brilliant_mqtt.provisioning_journal import (
    JOURNAL_STORAGE_KEY,
    ProvisioningJournal,
    ProvisioningOperation,
    ProvisioningPhase,
    ProvisioningRecord,
)
from tests.fakes import FakeShell
from tests.test_fleet_manager import (
    _OTHER_FINGERPRINT,
    _OTHER_PUBLIC_KEY,
    _panel,
    _pending_record,
    _provisioning_entry,
)
from tests.test_panel_provisioner import _fleet, _Harness, _record, _request
from tests.test_provisioning_journal import _install_real_store


async def test_retained_admission_read_failure_is_a_journal_failure(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _install_real_store(hass, monkeypatch, tmp_path)
    harness = _Harness()
    monkeypatch.setattr(harness, "journal", ProvisioningJournal(hass))
    target = tmp_path / ".storage" / (JOURNAL_STORAGE_KEY + ".retained")
    await hass.async_add_executor_job(target.parent.mkdir, 0o700, True, True)
    await hass.async_add_executor_job(target.write_bytes, b"invalid-json")
    capture = AsyncMock()
    monkeypatch.setattr(harness.operations, "capture_baseline", capture)
    with pytest.raises(PanelProvisioningError, match="journal_failed"):
        await harness.provisioner().async_install(_request(), _fleet(), harness.progress)
    capture.assert_not_awaited()


@pytest.mark.parametrize("reject_when", (None, "before", "queued", "finalize"))
@pytest.mark.parametrize("origin", ("unactivated", "restored"))
async def test_fleet_recovery_keeps_ownerless_marker_separate_and_rechecks_owner(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reject_when: str | None,
    origin: str,
) -> None:
    _install_real_store(hass, monkeypatch, tmp_path)
    journal = ProvisioningJournal(hass)
    first = _record(ProvisioningPhase.STAGED)
    await journal.async_create(first)
    await journal.async_complete_cleanup(first.transaction_id)
    retained = await journal.async_retained(first.transaction_id)
    assert retained is not None
    if origin == "unactivated":
        marked = await journal.async_begin_ownerless_unactivated_discard(retained)
    else:
        await journal.async_retained_state(first.transaction_id, "restored", evidence="a" * 32)
        restored = await journal.async_retained(first.transaction_id)
        assert restored is not None
        marked = await journal.async_begin_ownerless_finalize(restored)
    entry = _provisioning_entry(
        _panel(
            "kitchen",
            "SHA256:kitchen",
            subentry_id="panel-kitchen",
            management_id=_OTHER_FINGERPRINT,
        ),
        scene_owner="panel-kitchen",
    )
    entry.add_to_hass(hass)
    second = replace(
        first,
        transaction_id=uuid4(),
        setup_id=uuid4(),
        operation=ProvisioningOperation.UPDATE,
        fleet_profile=_pending_record().fleet_profile,
        panel_request=replace(
            first.panel_request,
            host="prior-kitchen.example.com" if reject_when == "before" else "kitchen.example.com",
            slug="kitchen",
            public_key=_OTHER_PUBLIC_KEY,
            fingerprint=_OTHER_FINGERPRINT,
            root_password="kitchen-secret",
            selected_components=("bridge", "bus_watchdog", "wifi_watchdog"),
        ),
    )
    await journal.async_create(second)
    if reject_when == "finalize":
        await journal.async_complete_cleanup(second.transaction_id)
        await journal.async_retained_state(second.transaction_id, "finalizing")
    harness = _Harness()
    monkeypatch.setattr(harness, "journal", journal)
    provisioner = harness.provisioner()
    shell_factory = Mock(side_effect=lambda host, password, pin: FakeShell(pinned=pin))
    monkeypatch.setattr(provisioner, "_shell_factory", shell_factory)
    monkeypatch.setattr(provisioner, "_repair_reporter", fm._ProvisioningRepairReporter(hass))
    monkeypatch.setattr(
        provisioner, "_credential_resolver", partial(fm._async_rollback_credential, hass)
    )
    monkeypatch.setattr(fm, "_get_recovery_provisioner", lambda _hass: provisioner)
    fleet = fm.FleetManager(hass, entry)
    authorize = fleet._async_authorize_background_recovery

    async def locked_authorize(record: ProvisioningRecord) -> None:
        assert provisioner._lock.locked()
        await authorize(record)

    monkeypatch.setattr(fleet, "_async_authorize_background_recovery", locked_authorize)
    monkeypatch.setattr(fleet, "_async_schedule_reload_once", Mock())
    if reject_when == "before":
        with pytest.raises(EntryDataError, match="provisioning_ownership_mismatch"):
            await fleet._async_reconcile()
        assert fleet._recovery_task is None
    else:
        await fleet._async_prepare_provisioning()
        assert fleet._recovery_transaction_id == second.transaction_id
        assert fleet._recovery_task is not None
        if reject_when == "queued":
            panel = entry.subentries["panel-kitchen"]
            hass.config_entries.async_update_subentry(
                entry, panel, data={**panel.data, CONF_HOST: "changed-kitchen.example.com"}
            )
        await fleet._recovery_task
    if reject_when in {"before", "queued"}:
        assert await journal.async_load() == second
        shell_factory.assert_not_called()
        assert not any(event[0] == "cleanup_staged" for event in harness.events)
    else:
        if reject_when == "finalize":
            assert await journal.async_retained(second.transaction_id) is None
            shell_factory.assert_called_once()
        else:
            assert ("cleanup_staged", second.transaction_id) in harness.events
        assert await journal.async_load() is None
        issue = ir.async_get(hass).async_get_issue(
            DOMAIN, fm._provisioning_repair_issue_id(second.transaction_id)
        )
        assert issue is None
    assert await journal.async_retained(first.transaction_id) == marked


async def test_scoped_recovery_cannot_dispatch_a_different_active_transaction(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _install_real_store(hass, monkeypatch, tmp_path)
    journal = ProvisioningJournal(hass)
    current = _record(ProvisioningPhase.STAGED)
    await journal.async_create(current)
    expected = replace(current, transaction_id=uuid4(), setup_id=uuid4())
    harness = _Harness()
    monkeypatch.setattr(harness, "journal", journal)
    provisioner = harness.provisioner()
    shell = Mock()
    authorize = AsyncMock()
    monkeypatch.setattr(provisioner, "_shell_factory", shell)
    await provisioner.async_recover(expected_record=expected, authorize=authorize)
    assert await journal.async_load() == current
    shell.assert_not_called()
    authorize.assert_not_awaited()


@pytest.mark.parametrize(
    ("onboarding", "revoke"),
    (
        (False, None),
        (False, "host"),
        (False, "password"),
        (True, None),
        (True, "retained"),
        (True, "unreadable"),
    ),
)
async def test_queued_explicit_rollback_revalidates_its_current_authority(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    onboarding: bool,
    revoke: str | None,
) -> None:
    _install_real_store(hass, monkeypatch, tmp_path)
    journal = ProvisioningJournal(hass)
    entry = _provisioning_entry(
        _panel(
            "kitchen",
            "SHA256:kitchen",
            subentry_id="panel-kitchen",
            management_id=_OTHER_FINGERPRINT,
        ),
        scene_owner="panel-kitchen",
    )
    entry.add_to_hass(hass)
    base = _record(ProvisioningPhase.STAGED)
    original = replace(
        base,
        operation=ProvisioningOperation.INSTALL if onboarding else ProvisioningOperation.UPDATE,
        fleet_profile=_pending_record().fleet_profile,
        panel_request=replace(
            base.panel_request,
            host="kitchen.example.com",
            slug="kitchen",
            public_key=_OTHER_PUBLIC_KEY,
            fingerprint=_OTHER_FINGERPRINT,
            root_password="kitchen-secret",
            selected_components=("bridge", "bus_watchdog", "wifi_watchdog"),
        ),
    )
    await journal.async_create(original)
    if not onboarding:
        await journal.async_complete_cleanup(original.transaction_id)
    await journal.async_request_restore(original.transaction_id)
    await journal.async_begin_restore(original.transaction_id, "kitchen-secret")
    rollback = await journal.async_load()
    assert rollback is not None
    assert rollback.operation is (
        ProvisioningOperation.ONBOARDING_ROLLBACK if onboarding else ProvisioningOperation.ROLLBACK
    )
    harness = _Harness()
    monkeypatch.setattr(harness, "journal", journal)
    provisioner = harness.provisioner()
    dispatch = AsyncMock()
    monkeypatch.setattr(provisioner, "_async_recover_record", dispatch)
    monkeypatch.setattr(fm, "_get_recovery_provisioner", lambda _hass: provisioner)
    fleet = fm.FleetManager(hass, entry)
    monkeypatch.setattr(fleet, "_async_schedule_reload_once", Mock())
    async with provisioner._lock:
        await fleet._async_prepare_provisioning()
        assert fleet._recovery_task is not None
        if revoke in {"host", "password"}:
            owner = entry.subentries["panel-kitchen"]
            field = CONF_HOST if revoke == "host" else CONF_ROOT_PASSWORD
            hass.config_entries.async_update_subentry(
                entry, owner, data={**owner.data, field: "changed-owner-value"}
            )
        elif revoke == "retained":
            await journal._async_retained_save({})
        elif revoke == "unreadable":
            target = tmp_path / ".storage" / (JOURNAL_STORAGE_KEY + ".retained")
            await hass.async_add_executor_job(target.write_bytes, b"invalid-json")
    await fleet._recovery_task
    if revoke is None:
        dispatch.assert_awaited_once()
        assert dispatch.await_args is not None
        assert dispatch.await_args.args[0] == rollback
    else:
        dispatch.assert_not_awaited()
    assert await journal.async_load() == rollback
