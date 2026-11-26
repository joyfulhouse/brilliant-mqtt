"""Named recovery resumes exact authority and proof across continuation boundaries."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from functools import partial
from typing import Any
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.brilliant_mqtt import fleet_manager as fm
from custom_components.brilliant_mqtt import panel_provisioner as pp
from custom_components.brilliant_mqtt.connection_owner import (
    PanelConnectionOwner,
    async_resolve_panel_owner,
)
from custom_components.brilliant_mqtt.connection_state import ConnectionIntent
from custom_components.brilliant_mqtt.const import CONF_HOST, CONF_SSH_HOST_KEY
from custom_components.brilliant_mqtt.entry_data import LegacyPanelStore
from custom_components.brilliant_mqtt.manager import PanelManager
from custom_components.brilliant_mqtt.panel_ops import PanelOpError
from custom_components.brilliant_mqtt.provisioning_journal import (
    ProvisioningJournal,
    ProvisioningOperation,
    ProvisioningPhase,
    ProvisioningRecord,
    RetainedRollback,
    ScopedProvisioningJournal,
)
from tests.test_config_flow import (
    _OTHER_FINGERPRINT,
    _OTHER_PUBLIC_KEY,
    _fleet_entry,
    _legacy_entry,
    _subentry,
)
from tests.test_panel_provisioner import (
    _absent_snapshot,
    _Harness,
    _record,
    _release_link_snapshot,
)

_EVIDENCE = "b" * 32


def _owner(hass: HomeAssistant) -> tuple[MockConfigEntry, PanelConnectionOwner]:
    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    return entry, PanelConnectionOwner(hass, entry, entry.subentries[panel.subentry_id])


def _baseline(*, absent: bool = False) -> ProvisioningRecord:
    snapshot = _release_link_snapshot()
    if absent:
        snapshot = replace(_absent_snapshot(), baseline=snapshot.baseline)
    return replace(_record(ProvisioningPhase.STAGED), prior_snapshot=snapshot)


async def _scoped(
    owner: PanelConnectionOwner, *, restored: bool = False, absent: bool = False
) -> RetainedRollback:
    journal = owner.journal
    record = _baseline(absent=absent)
    await journal.async_arm(record)
    row = await journal.async_retained(record.transaction_id)
    assert row is not None
    row = await journal.async_bind_retained(row, owner.owner)
    await journal.async_begin_scoped_restore(row)
    if restored:
        await journal.async_retained_state(
            record.transaction_id, "restored", evidence=None if absent else _EVIDENCE
        )
    row = await journal.async_retained(record.transaction_id)
    assert row is not None and row.recovery_record is not None
    return row


def _runner(
    hass: HomeAssistant, journal: ProvisioningJournal, monkeypatch: pytest.MonkeyPatch
) -> tuple[pp.PanelProvisioner, _Harness, Mock, AsyncMock]:
    harness = _Harness()
    monkeypatch.setattr(harness, "journal", journal)
    provisioner = harness.provisioner()
    transport = Mock(return_value=harness.shell)
    credentials = AsyncMock(side_effect=partial(fm._async_rollback_credential, hass))
    monkeypatch.setattr(provisioner, "_shell_factory", transport)
    monkeypatch.setattr(provisioner, "_credential_resolver", credentials)
    monkeypatch.setattr(
        provisioner, "_named_owner_resolver", partial(async_resolve_panel_owner, hass)
    )
    monkeypatch.setattr(
        provisioner, "_connection_admission", journal.async_assert_connection_available
    )
    monkeypatch.setattr(fm, "_get_recovery_provisioner", lambda *_args: provisioner)
    return provisioner, harness, transport, credentials


async def _dispatch(
    provisioner: pp.PanelProvisioner,
    row: RetainedRollback,
    owner: PanelConnectionOwner,
    reader: str,
) -> None:
    if reader == "explicit":
        await provisioner.async_rollback(row.record.transaction_id, expected_owner=owner.owner)
    elif reader == "cold":
        assert row.recovery_record is not None
        await provisioner.async_recover(expected_record=row.recovery_record)
    else:
        await provisioner.async_recover()


@pytest.mark.parametrize("reader", ["explicit", "cold", "generic"])
@pytest.mark.parametrize("conflict", ["key", "slug", None])
async def test_scoped_continuation_checks_global_active_before_credentials(
    hass: HomeAssistant,
    durable_connection_storage: None,
    monkeypatch: pytest.MonkeyPatch,
    reader: str,
    conflict: str | None,
) -> None:
    _entry, owner = _owner(hass)
    row = await _scoped(owner)
    active = replace(
        row.record,
        transaction_id=uuid4(),
        operation=ProvisioningOperation.UPDATE,
        prior_snapshot=replace(row.record.prior_snapshot, baseline=None),
        panel_request=replace(
            row.record.panel_request,
            bound_owner=None,
            slug="kitchen" if conflict != "slug" else row.record.panel_request.slug,
            public_key=(
                row.record.panel_request.public_key if conflict == "key" else _OTHER_PUBLIC_KEY
            ),
            fingerprint=(
                row.record.panel_request.fingerprint if conflict == "key" else _OTHER_FINGERPRINT
            ),
        ),
    )
    await owner.journal.async_create(active)
    # A cold coordinator must reach the same decision as explicit admission.
    hass.data.pop("brilliant_mqtt.provisioning_journal.coordinator")
    journal = ProvisioningJournal(hass)
    provisioner, harness, transport, credentials = _runner(hass, journal, monkeypatch)
    recover_active = AsyncMock()
    recover_record = provisioner._async_recover_record

    async def recover(record: ProvisioningRecord, *args: Any, **kwargs: Any) -> None:
        if record == active:
            await recover_active(record)
        else:
            await recover_record(record, *args, **kwargs)

    monkeypatch.setattr(provisioner, "_async_recover_record", recover)
    if conflict is not None and reader != "generic":
        with pytest.raises(pp.PanelProvisioningError, match="provisioning_recovery_required"):
            await _dispatch(provisioner, row, owner, reader)
    else:
        await _dispatch(provisioner, row, owner, reader)
    assert await journal.async_load() == active
    if reader == "generic":
        recover_active.assert_awaited_once_with(active)
    else:
        recover_active.assert_not_awaited()
    if conflict is not None:
        assert await journal.async_retained(row.record.transaction_id) == row
        credentials.assert_not_awaited()
        transport.assert_not_called()
        assert not harness.events
    else:
        after = await journal.async_retained(row.record.transaction_id)
        assert after is not None and after.recovery_record is None
        assert any(event[0] == "rollback" for event in harness.events)


@pytest.mark.parametrize("origin", ["ownerless_restored", "ownerless_unactivated"])
async def test_named_finalize_never_claims_an_ownerless_continuation(
    hass: HomeAssistant,
    durable_connection_storage: None,
    monkeypatch: pytest.MonkeyPatch,
    origin: str,
) -> None:
    journal = ProvisioningJournal(hass)
    record = _baseline()
    await journal.async_arm(record)
    row = await journal.async_retained(record.transaction_id)
    assert row is not None
    if origin == "ownerless_restored":
        await journal.async_retained_state(record.transaction_id, "restored", evidence=_EVIDENCE)
        row = await journal.async_retained(record.transaction_id)
        assert row is not None
        marker = await journal.async_begin_ownerless_finalize(row)
    else:
        marker = await journal.async_begin_ownerless_unactivated_discard(row)
    active = replace(
        record,
        transaction_id=uuid4(),
        operation=ProvisioningOperation.UPDATE,
        panel_request=replace(
            record.panel_request,
            slug="kitchen",
            public_key=_OTHER_PUBLIC_KEY,
            fingerprint=_OTHER_FINGERPRINT,
        ),
    )
    await journal.async_create(active)
    _entry, owner = _owner(hass)
    provisioner, _harness, transport, credentials = _runner(hass, journal, monkeypatch)
    bind = AsyncMock(side_effect=AssertionError("ownerless marker must precede binding"))
    monkeypatch.setattr(provisioner, "_async_bind_named", bind)
    with pytest.raises(
        pp.PanelProvisioningError, match="ownerless_finalize_reauthentication_required"
    ):
        await provisioner.async_finalize(record.transaction_id, expected_owner=owner.owner)
    bind.assert_not_awaited()
    credentials.assert_not_awaited()
    transport.assert_not_called()
    assert await journal.async_retained(record.transaction_id) == marker
    assert await journal.async_load() == active


@pytest.mark.parametrize("reader", ["explicit", "cold", "generic"])
@pytest.mark.parametrize("absent", [False, True])
async def test_restored_scoped_continuation_only_completes_cleanup(
    hass: HomeAssistant,
    durable_connection_storage: None,
    monkeypatch: pytest.MonkeyPatch,
    reader: str,
    absent: bool,
) -> None:
    _entry, owner = _owner(hass)
    row = await _scoped(owner, restored=True, absent=absent)
    provisioner, harness, _transport, _credentials = _runner(hass, owner.journal, monkeypatch)
    await _dispatch(provisioner, row, owner, reader)
    after = await owner.journal.async_retained(row.record.transaction_id)
    assert after == replace(row, recovery_record=None)
    assert [event[0] for event in harness.events] == [
        "shell_connect",
        "cleanup_staged",
        "shell_close",
    ]
    assert harness.shell.close_count == 1
    assert await owner.journal.async_load() is None


@pytest.mark.parametrize("reader", ["explicit", "cold"])
async def test_restored_scoped_completion_write_resumes_without_restoring(
    hass: HomeAssistant,
    durable_connection_storage: None,
    monkeypatch: pytest.MonkeyPatch,
    reader: str,
) -> None:
    _entry, owner = _owner(hass)
    row = await _scoped(owner, restored=True)
    provisioner, harness, _transport, _credentials = _runner(hass, owner.journal, monkeypatch)
    remove = ScopedProvisioningJournal._async_remove
    interrupted = False

    async def fail_first_remove(journal: ScopedProvisioningJournal) -> None:
        nonlocal interrupted
        if not interrupted:
            interrupted = True
            raise OSError("completion interrupted after durable phase write")
        await remove(journal)

    monkeypatch.setattr(ScopedProvisioningJournal, "_async_remove", fail_first_remove)
    with pytest.raises(pp.PanelProvisioningError, match="cleanup_failed"):
        await _dispatch(provisioner, row, owner, reader)
    after = await owner.journal.async_retained(row.record.transaction_id)
    assert row.recovery_record is not None
    assert after == replace(
        row, recovery_record=replace(row.recovery_record, phase=ProvisioningPhase.ROLLED_BACK)
    )
    assert after is not None
    await _dispatch(provisioner, after, owner, reader)
    assert await owner.journal.async_retained(row.record.transaction_id) == replace(
        row, recovery_record=None
    )
    assert [event[0] for event in harness.events].count("cleanup_staged") == 2
    assert not any(event[0] == "rollback" for event in harness.events)
    assert harness.shell.close_count == 2


async def test_restored_scope_change_during_login_resolution_refuses_contact(
    hass: HomeAssistant,
    durable_connection_storage: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _entry, owner = _owner(hass)
    row = await _scoped(owner, restored=True)
    provisioner, _harness, transport, credentials = _runner(hass, owner.journal, monkeypatch)

    async def resolve_then_change(request: Any) -> str | None:
        password = await fm._async_rollback_credential(hass, request)
        await owner.journal.async_retained_state(row.record.transaction_id, "rollback_failed")
        return password

    credentials.side_effect = resolve_then_change
    with pytest.raises(pp.PanelProvisioningError, match="provisioning_recovery_required"):
        await provisioner.async_rollback(row.record.transaction_id, expected_owner=owner.owner)
    transport.assert_not_called()
    assert await owner.journal.async_retained(row.record.transaction_id) == replace(
        row, state="rollback_failed"
    )


@pytest.mark.parametrize("reader", ["explicit", "cold", "generic"])
async def test_unbound_restored_scope_requires_fresh_named_binding(
    hass: HomeAssistant,
    durable_connection_storage: None,
    monkeypatch: pytest.MonkeyPatch,
    reader: str,
) -> None:
    _entry, owner = _owner(hass)
    record = _baseline()
    await owner.journal.async_arm(record)
    row = await owner.journal.async_retained(record.transaction_id)
    assert row is not None
    await owner.journal.async_begin_scoped_restore(row)
    await owner.journal.async_retained_state(record.transaction_id, "restored", evidence=_EVIDENCE)
    row = await owner.journal.async_retained(record.transaction_id)
    assert row is not None
    provisioner, _harness, transport, credentials = _runner(hass, owner.journal, monkeypatch)
    if reader == "cold":
        with pytest.raises(pp.PanelProvisioningError, match="provisioning_recovery_required"):
            await _dispatch(provisioner, row, owner, reader)
    else:
        await _dispatch(provisioner, row, owner, reader)
    after = await owner.journal.async_retained(record.transaction_id)
    if reader == "explicit":
        assert after is not None and after.recovery_record is None
        assert after.record.panel_request.bound_owner == owner.owner
        assert after.recovery_deployment_id == _EVIDENCE
        transport.assert_called_once()
    else:
        assert after == row
        credentials.assert_not_awaited()
        transport.assert_not_called()


@pytest.mark.parametrize("boundary", ["after_connect", "after_cleanup"])
@pytest.mark.parametrize("change", ["evidence", "scope", "intent"])
async def test_restored_cleanup_revalidates_durable_scope_and_admission(
    hass: HomeAssistant,
    durable_connection_storage: None,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
    change: str,
) -> None:
    _entry, owner = _owner(hass)
    row = await _scoped(owner, restored=True)
    provisioner, harness, _transport, _credentials = _runner(hass, owner.journal, monkeypatch)
    expected = row
    cleanup = AsyncMock()
    monkeypatch.setattr(harness.operations, "cleanup_staged", cleanup)

    async def change_durable_state() -> None:
        nonlocal expected
        if change == "evidence":
            await owner.journal.async_retained_state(
                row.record.transaction_id, "restored", evidence="c" * 32
            )
            expected = replace(row, recovery_deployment_id="c" * 32)
        elif change == "scope":
            await owner.journal.scoped(row.record.transaction_id).async_complete_rollback(
                row.record.transaction_id, verified=True
            )
            expected = replace(row, recovery_record=None)
        else:
            await owner.journal.async_begin_connection_change(
                ConnectionIntent(
                    str(uuid4()),
                    owner.owner,
                    owner.prior,
                    replace(owner.prior, host="later.test"),
                    (str(row.record.transaction_id),),
                )
            )

    if boundary == "after_connect":
        connect = harness.shell.connect

        async def connect_then_change() -> None:
            await connect()
            await change_durable_state()

        monkeypatch.setattr(harness.shell, "connect", connect_then_change)
    else:

        async def cleanup_then_change(*_args: Any) -> None:
            await change_durable_state()

        cleanup.side_effect = cleanup_then_change
    code = "address_change_incomplete" if change == "intent" else "provisioning_recovery_required"
    with pytest.raises(pp.PanelProvisioningError, match=code):
        await provisioner.async_rollback(row.record.transaction_id, expected_owner=owner.owner)
    assert await owner.journal.async_retained(row.record.transaction_id) == expected
    assert not any(event[0] == "rollback" for event in harness.events)
    assert harness.shell.close_count == 1
    if boundary == "after_connect":
        cleanup.assert_not_awaited()
    else:
        cleanup.assert_awaited_once()


@pytest.mark.parametrize("boundary", ["connect", "cleanup"])
async def test_restored_cleanup_cancellation_settles_before_unlocking(
    hass: HomeAssistant,
    durable_connection_storage: None,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    _entry, owner = _owner(hass)
    row = await _scoped(owner, restored=True)
    provisioner, harness, _transport, _credentials = _runner(hass, owner.journal, monkeypatch)
    started, release = asyncio.Event(), asyncio.Event()
    operation = (
        harness.shell.connect if boundary == "connect" else harness.operations.cleanup_staged
    )

    async def pause(*args: Any) -> None:
        started.set()
        await release.wait()
        await operation(*args)

    if boundary == "connect":
        monkeypatch.setattr(harness.shell, "connect", pause)
    else:
        monkeypatch.setattr(harness.operations, "cleanup_staged", pause)
    task = asyncio.create_task(
        provisioner.async_rollback(row.record.transaction_id, expected_owner=owner.owner)
    )
    try:
        await asyncio.wait_for(started.wait(), 1)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and provisioner._lock.locked()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not provisioner._lock.locked()
        assert harness.shell.close_count == 1
        assert not any(event[0] == "rollback" for event in harness.events)
        assert await owner.journal.async_retained(row.record.transaction_id) == replace(
            row, recovery_record=None
        )
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("fault", ["cleanup", "connect_timeout", "cleanup_timeout", "close"])
async def test_restored_scoped_cleanup_fault_keeps_original_proof(
    hass: HomeAssistant,
    durable_connection_storage: None,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    _entry, owner = _owner(hass)
    row = await _scoped(owner, restored=True)
    provisioner, harness, _transport, _credentials = _runner(hass, owner.journal, monkeypatch)
    if fault == "cleanup":
        harness.operations.fail_at.add("cleanup")
    elif fault == "close":
        harness.shell.fail_close_once = True
    else:

        async def blocked(*_args: Any) -> None:
            await asyncio.Event().wait()

        monkeypatch.setattr(pp, "RECOVERY_TIMEOUT_SECONDS", 0.03)
        if fault == "connect_timeout":
            monkeypatch.setattr(harness.shell, "connect", blocked)
        else:
            monkeypatch.setattr(harness.operations, "cleanup_staged", blocked)
    expected_code = "rollback_deadline_exceeded" if fault.endswith("timeout") else "cleanup_failed"
    with pytest.raises(pp.PanelProvisioningError, match=expected_code):
        await provisioner.async_rollback(row.record.transaction_id, expected_owner=owner.owner)
    after = await owner.journal.async_retained(row.record.transaction_id)
    if fault == "close":
        assert after == replace(row, recovery_record=None)
    else:
        assert after == row
    assert not any(event[0] in {"rollback", "restart_restored"} for event in harness.events)
    assert harness.shell.close_count == 1
    assert await owner.journal.async_load() is None


@pytest.mark.parametrize("boundary", ["before_contact", "after_connect", "after_cleanup"])
@pytest.mark.parametrize("change", ["host", "key", "remove"])
async def test_restored_scoped_cleanup_rechecks_original_owner(
    hass: HomeAssistant,
    durable_connection_storage: None,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
    change: str,
) -> None:
    entry, owner = _owner(hass)
    row = await _scoped(owner, restored=True)
    provisioner, harness, transport, credentials = _runner(hass, owner.journal, monkeypatch)
    subentry = entry.subentries[str(owner.owner.subentry_id)]
    cleanup = AsyncMock()
    monkeypatch.setattr(harness.operations, "cleanup_staged", cleanup)

    def change_owner() -> None:
        if change == "remove":
            hass.config_entries.async_remove_subentry(entry, subentry.subentry_id)
        else:
            key = CONF_HOST if change == "host" else CONF_SSH_HOST_KEY
            value = "later.test" if change == "host" else _OTHER_PUBLIC_KEY
            hass.config_entries.async_update_subentry(
                entry, subentry, data={**subentry.data, key: value}
            )

    if boundary == "before_contact":
        change_owner()
    elif boundary == "after_connect":
        connect = harness.shell.connect

        async def connect_then_change() -> None:
            await connect()
            change_owner()

        monkeypatch.setattr(harness.shell, "connect", connect_then_change)
    else:

        async def cleanup_then_change(*_args: Any) -> None:
            change_owner()

        cleanup.side_effect = cleanup_then_change
    with pytest.raises(pp.PanelProvisioningError, match="baseline_owner_mismatch"):
        await provisioner.async_rollback(row.record.transaction_id, expected_owner=owner.owner)
    assert await owner.journal.async_retained(row.record.transaction_id) == row
    assert not any(event[0] == "rollback" for event in harness.events)
    if boundary == "before_contact":
        credentials.assert_not_awaited()
        transport.assert_not_called()
    else:
        assert harness.shell.close_count == 1
    if boundary == "after_cleanup":
        cleanup.assert_awaited_once()
    else:
        cleanup.assert_not_awaited()


@pytest.mark.parametrize("reader", ["explicit", "cold", "generic"])
@pytest.mark.parametrize("proof", ["missing_evidence", "invalid_phase", "invalid_absent_phase"])
async def test_unproven_restored_scope_refuses_without_contact(
    hass: HomeAssistant,
    durable_connection_storage: None,
    monkeypatch: pytest.MonkeyPatch,
    reader: str,
    proof: str,
) -> None:
    _entry, owner = _owner(hass)
    row = await _scoped(
        owner, restored=proof != "missing_evidence", absent=proof == "invalid_absent_phase"
    )
    if proof == "missing_evidence":
        await owner.journal.async_retained_state(row.record.transaction_id, "restored")
    else:
        assert row.recovery_record is not None
        # A valid stored DTO with the wrong continuation phase must not gain authority.
        await owner.journal.scoped(row.record.transaction_id)._async_save(
            replace(
                row.recovery_record,
                phase=(
                    ProvisioningPhase.ROLLED_BACK
                    if proof == "invalid_absent_phase"
                    else ProvisioningPhase.STAGED
                ),
            )
        )
    fresh = await owner.journal.async_retained(row.record.transaction_id)
    assert fresh is not None
    row = fresh
    provisioner, _harness, transport, credentials = _runner(hass, owner.journal, monkeypatch)
    if reader == "generic":
        await _dispatch(provisioner, row, owner, reader)
    else:
        with pytest.raises(pp.PanelProvisioningError, match="provisioning_recovery_required"):
            await _dispatch(provisioner, row, owner, reader)
    assert await owner.journal.async_retained(row.record.transaction_id) == row
    credentials.assert_not_awaited()
    transport.assert_not_called()


@pytest.mark.parametrize("operation", ["rollback", "finalize"])
@pytest.mark.parametrize("pending", [False, True])
async def test_public_canary_keeps_actionable_connection_refusals(
    hass: HomeAssistant,
    durable_connection_storage: None,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    pending: bool,
) -> None:
    entry = _legacy_entry(hass)
    owner = PanelConnectionOwner(hass, entry)
    target = _baseline()
    target = replace(target, panel_request=replace(target.panel_request, host="old.test"))
    await owner.journal.async_arm(target)
    if pending:
        await owner.journal.async_begin_connection_change(
            ConnectionIntent(
                str(uuid4()),
                owner.owner,
                owner.prior,
                replace(owner.prior, host="new.test"),
                (str(target.transaction_id),),
            )
        )
    manager = PanelManager(
        hass,
        LegacyPanelStore(hass, entry),
        fm.legacy_fleet_config(entry),
        fm._shared_ssh_lock(hass),
        lambda: True,
    )
    _provisioner, _harness, transport, credentials = _runner(hass, owner.journal, monkeypatch)
    code = "address_change_incomplete" if pending else "baseline_owner_mismatch"
    with pytest.raises(HomeAssistantError, match=f"^{code}: see docs/canary-rollback.md$"):
        await manager.async_canary_operation(operation, str(target.transaction_id))
    credentials.assert_not_awaited()
    transport.assert_not_called()


@pytest.mark.parametrize("operation", ["rollback", "finalize"])
async def test_public_canary_uses_admitted_canonical_legacy_pin(
    hass: HomeAssistant,
    durable_connection_storage: None,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    entry = _legacy_entry(hass)
    target = _baseline()
    hass.config_entries.async_update_entry(
        entry, data={**entry.data, CONF_SSH_HOST_KEY: target.panel_request.public_key + "\n"}
    )
    journal = ProvisioningJournal(hass)
    await journal.async_arm(target)
    assert (await async_resolve_panel_owner(hass, target.panel_request)).entry_id == entry.entry_id
    manager = PanelManager(
        hass,
        LegacyPanelStore(hass, entry),
        fm.legacy_fleet_config(entry),
        fm._shared_ssh_lock(hass),
        lambda: True,
    )
    _provisioner, _harness, transport, _credentials = _runner(hass, journal, monkeypatch)
    await manager.async_canary_operation(operation, str(target.transaction_id))
    transport.assert_called_once()


@pytest.mark.parametrize("error", [PanelOpError("SECRET shell output"), RuntimeError("SECRET")])
async def test_public_canary_redacts_unexpected_errors(
    hass: HomeAssistant,
    durable_connection_storage: None,
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
) -> None:
    entry = _legacy_entry(hass)
    manager = PanelManager(
        hass,
        LegacyPanelStore(hass, entry),
        fm.legacy_fleet_config(entry),
        fm._shared_ssh_lock(hass),
        lambda: True,
    )
    monkeypatch.setattr(manager, "_async_admit_connection", AsyncMock(side_effect=error))
    with pytest.raises(
        HomeAssistantError, match="^canary_operation_failed: see docs/canary-rollback.md$"
    ):
        await manager.async_canary_operation("finalize", str(uuid4()))
