"""Connection ownership remains safe at lifecycle and retained-state boundaries."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from functools import partial
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

import asyncssh
import pytest
from homeassistant.components import mqtt
from homeassistant.core import HomeAssistant
from homeassistant.helpers import issue_registry as ir
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.brilliant_mqtt import binary_sensor, ha_control
from custom_components.brilliant_mqtt import fleet_manager as fm
from custom_components.brilliant_mqtt.connection_owner import (
    PanelConnectionOwner,
    _issue_id,
    async_resolve_panel_owner,
)
from custom_components.brilliant_mqtt.connection_state import (
    ConnectionIntent,
    PanelConnection,
    PanelOwner,
)
from custom_components.brilliant_mqtt.const import (
    CONF_HOST,
    CONF_ROOT_PASSWORD,
    CONF_SSH_HOST_KEY,
    DOMAIN,
)
from custom_components.brilliant_mqtt.entry_data import EntryDataError, LegacyPanelStore
from custom_components.brilliant_mqtt.flow import gateway
from custom_components.brilliant_mqtt.flow import reconfigure as legacy_flow
from custom_components.brilliant_mqtt.manager import PanelManager
from custom_components.brilliant_mqtt.panel_provisioner import PanelProvisioningError
from custom_components.brilliant_mqtt.provisioning_journal import (
    ProvisioningJournal,
    ProvisioningJournalError,
    ProvisioningOperation,
    ProvisioningRecord,
)
from tests import test_fleet_manager as fleet_fixtures
from tests.test_config_flow import (
    _OTHER_FINGERPRINT,
    _OTHER_PUBLIC_KEY,
    _fleet_entry,
    _identity,
    _legacy_entry,
    _open_legacy_reconfigure_step,
    _start_panel_reconfigure_step,
    _subentry,
)
from tests.test_panel_provisioner import _Harness
from tests.test_provisioning_journal import _baselined_record


def _pair(
    hass: HomeAssistant,
) -> tuple[MockConfigEntry, PanelConnectionOwner, PanelConnectionOwner]:
    office = _subentry()
    kitchen = _subentry(
        slug="kitchen",
        subentry_id="kitchen-id",
        priority=2,
        fingerprint=_OTHER_FINGERPRINT,
        public_key=_OTHER_PUBLIC_KEY,
    )
    entry = _fleet_entry(hass, office, kitchen)
    return (
        entry,
        PanelConnectionOwner(hass, entry, entry.subentries[office.subentry_id]),
        PanelConnectionOwner(hass, entry, entry.subentries[kitchen.subentry_id]),
    )


def _offline_fleet(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(PanelManager, "async_setup", AsyncMock())
    monkeypatch.setattr(PanelManager, "async_shutdown", AsyncMock())
    monkeypatch.setattr(mqtt, "async_subscribe_connection_status", Mock(return_value=lambda: None))
    monkeypatch.setattr(mqtt, "is_connected", lambda _hass: True)
    monkeypatch.setattr(fm, "async_purge_stale_discovery_configs", AsyncMock())


async def _mark_finalizing(owner: PanelConnectionOwner, *, bind: bool = True) -> ProvisioningRecord:
    record = _baselined_record()
    await owner.journal.async_arm(record)
    if bind:
        row = await owner.journal.async_retained(record.transaction_id)
        assert row is not None
        await owner.journal.async_bind_retained(row, owner.owner)
    await owner.journal.async_retained_state(record.transaction_id, "finalizing")
    return record


@pytest.mark.parametrize("bound", [False, True])
async def test_finalizing_rollback_refuses_before_binding(
    hass: HomeAssistant,
    durable_connection_storage: None,
    monkeypatch: pytest.MonkeyPatch,
    bound: bool,
) -> None:
    _entry, office, _kitchen = _pair(hass)
    record = await _mark_finalizing(office, bind=bound)
    before = await office.journal.async_retained(record.transaction_id)
    harness = _Harness()
    monkeypatch.setattr(harness, "journal", office.journal)
    provisioner = harness.provisioner()
    bind = AsyncMock(side_effect=AssertionError("terminal marker must precede owner binding"))
    monkeypatch.setattr(provisioner, "_async_bind_named", bind)
    with pytest.raises(PanelProvisioningError, match="provisioning_recovery_required"):
        await provisioner.async_rollback(record.transaction_id, expected_owner=office.owner)
    bind.assert_not_awaited()
    assert await office.journal.async_retained(record.transaction_id) == before
    assert await office.journal.async_load() is None
    assert not any(event[0] == "shell_factory" for event in harness.events)


async def test_atomic_scoped_writer_refuses_finalizing(
    hass: HomeAssistant,
    durable_connection_storage: None,
) -> None:
    _entry, office, _kitchen = _pair(hass)
    record = await _mark_finalizing(office)
    before = await office.journal.async_retained(record.transaction_id)
    assert before is not None
    with pytest.raises(ProvisioningJournalError, match="invalid_journal_transition"):
        await office.journal.async_begin_scoped_restore(before)
    assert await office.journal.async_retained(record.transaction_id) == before
    assert await office.journal.async_load() is None


async def test_partial_finalization_cannot_be_reversed_and_original_finalize_continues(
    hass: HomeAssistant,
    durable_connection_storage: None,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _entry, office, _kitchen = _pair(hass)
    record = _baselined_record()
    await office.journal.async_arm(record)
    harness = _Harness()
    monkeypatch.setattr(harness, "journal", office.journal)
    provisioner = harness.provisioner()
    monkeypatch.setattr(
        provisioner, "_named_owner_resolver", partial(async_resolve_panel_owner, hass)
    )
    monkeypatch.setattr(
        provisioner, "_credential_resolver", partial(fm._async_rollback_credential, hass)
    )
    monkeypatch.setattr(provisioner, "_shell_factory", Mock(return_value=harness.shell))
    first, second = tmp_path / "baseline-code", tmp_path / "baseline-state"
    first.write_text("retained code")
    second.write_text("retained state")
    interrupted = True

    async def finalize(*_args: Any) -> None:
        nonlocal interrupted
        first.unlink(missing_ok=True)
        if interrupted:
            interrupted = False
            raise OSError("partial baseline deletion")
        second.unlink()

    monkeypatch.setattr(harness.operations, "finalize_baseline", finalize)
    with pytest.raises(PanelProvisioningError, match="recovery_failed"):
        await provisioner.async_finalize(record.transaction_id, expected_owner=office.owner)
    before = await office.journal.async_retained(record.transaction_id)
    assert before is not None and before.state == "finalizing"
    assert not first.exists() and second.exists()
    with pytest.raises(PanelProvisioningError, match="provisioning_recovery_required"):
        await provisioner.async_rollback(record.transaction_id, expected_owner=office.owner)
    assert await office.journal.async_retained(record.transaction_id) == before
    assert not first.exists() and second.exists()
    await provisioner.async_finalize(record.transaction_id, expected_owner=office.owner)
    assert await office.journal.async_retained(record.transaction_id) is None
    assert not second.exists()


async def test_live_handoff_and_rebind_both_finish_without_cancellation(
    hass: HomeAssistant,
    durable_connection_storage: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entry = fleet_fixtures._pending_owner_entry()
    entry.add_to_hass(hass)
    fleet = fm.FleetManager(hass, entry)
    journal = Mock(
        async_connection_intents=AsyncMock(return_value=()),
        async_assert_connection_available=AsyncMock(),
        async_retained_records=AsyncMock(return_value=()),
        async_load=AsyncMock(return_value=fleet_fixtures._pending_record()),
        async_complete_commit=AsyncMock(),
    )
    monkeypatch.setattr(fm, "ProvisioningJournal", lambda *_args: journal)
    monkeypatch.setattr(fm, "async_wait_config_entry_persisted", AsyncMock())
    monkeypatch.setattr(hass.config_entries, "async_schedule_reload", Mock())
    _offline_fleet(monkeypatch)
    at_handoff, resume = asyncio.Event(), asyncio.Event()
    complete = fleet._async_complete_pending_handoff

    async def handoff_gap() -> bool:
        assert fleet._pending_handoff is not None
        at_handoff.set()
        await resume.wait()
        return await complete()

    monkeypatch.setattr(fleet, "_async_complete_pending_handoff", handoff_gap)
    reconcile = asyncio.create_task(fleet._async_reconcile_once())
    rebind: asyncio.Task[None] | None = None
    try:
        await asyncio.wait_for(at_handoff.wait(), 5)
        assert fleet._lifecycle_lock.locked() and not fleet._ssh_lock.locked()
        # The expected snapshot is irrelevant: the occupied journal must refuse first.
        rebind = asyncio.create_task(
            fleet.async_rebind_panel(
                "panel-office",
                Mock(),
                host="fake.test",
                root_password="fake",
                candidate=_identity(),
            )
        )
        await asyncio.sleep(0)
        resume.set()
        done, pending = await asyncio.wait({reconcile, rebind}, timeout=1)
        assert not pending, "operation/lifecycle cycle prevents both requests from settling"
        assert done == {reconcile, rebind}
        assert reconcile.result()
        with pytest.raises(EntryDataError, match="panel_rebind_blocked_by_panel_onboarding"):
            rebind.result()
        assert not fleet._lifecycle_lock.locked() and not fleet._ssh_lock.locked()
    finally:
        resume.set()
        for task in (reconcile, rebind):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (reconcile, rebind) if task is not None), return_exceptions=True
        )
        await fleet.async_shutdown()


@pytest.mark.parametrize("deferred_at_setup", [False, True])
async def test_public_platform_is_reloaded_only_for_newly_admitted_membership(
    hass: HomeAssistant,
    durable_connection_storage: None,
    monkeypatch: pytest.MonkeyPatch,
    deferred_at_setup: bool,
) -> None:
    entry, office, kitchen = _pair(hass)
    if deferred_at_setup:
        await office.journal.async_begin_connection_change(
            ConnectionIntent(
                str(uuid4()),
                office.owner,
                office.prior,
                replace(office.prior, host="new.test"),
                (),
            )
        )
    fleet = fm.FleetManager(hass, entry)
    entry.runtime_data = fleet
    _offline_fleet(monkeypatch)
    monkeypatch.setattr(
        ha_control, "get_control_plane", lambda *_args: Mock(async_reload_settings=AsyncMock())
    )
    scheduled = Mock()
    monkeypatch.setattr(hass.config_entries, "async_schedule_reload", scheduled)
    added: list[str | None] = []

    def add(
        new_entities: Any, update_before_add: bool = False, *, config_subentry_id: str | None = None
    ) -> None:
        added.append(config_subentry_id)

    try:
        await fleet.async_setup()
        await binary_sensor.async_setup_entry(hass, entry, add)
        assert kitchen.owner.subentry_id in added
        assert (office.owner.subentry_id in added) is not deferred_at_setup
        form = await _start_panel_reconfigure_step(
            hass, entry, entry.subentries[str(office.owner.subentry_id)], "address"
        )
        with (
            patch.object(gateway, "async_fetch_host_identity", AsyncMock(return_value=_identity())),
            patch.object(gateway, "_async_inspect_candidate", AsyncMock()),
        ):
            result = await hass.config_entries.subentries.async_configure(
                form["flow_id"], {CONF_HOST: office.prior.host}
            )
        assert result.get("reason") == "reconfigure_successful"
        assert await office.journal.async_connection_intents() == ()
        assert fleet._connection_reconcile_task is not None
        await fleet._connection_reconcile_task
        await hass.async_block_till_done()
        assert office.owner.subentry_id in fleet.panels
        if deferred_at_setup:
            scheduled.assert_called_once_with(entry.entry_id)
            await binary_sensor.async_setup_entry(hass, entry, add)
            assert office.owner.subentry_id in added
        else:
            scheduled.assert_not_called()
    finally:
        await fleet.async_shutdown()


@pytest.mark.parametrize("revoke", [None, "host", "password", "active_conflict"])
async def test_cold_finalizing_beside_pending_sibling_revalidates_when_queued(
    hass: HomeAssistant,
    durable_connection_storage: None,
    monkeypatch: pytest.MonkeyPatch,
    revoke: str | None,
) -> None:
    entry, office, kitchen = _pair(hass)
    record = await _mark_finalizing(office)
    before = await office.journal.async_retained(record.transaction_id)
    active = replace(
        record,
        transaction_id=uuid4(),
        operation=ProvisioningOperation.UPDATE,
        panel_request=replace(
            record.panel_request,
            slug="kitchen",
            host=kitchen.prior.host,
            public_key=kitchen.prior.public_key,
            fingerprint=_OTHER_FINGERPRINT,
        ),
    )
    await office.journal.async_create(active)
    intent = ConnectionIntent(
        str(uuid4()),
        kitchen.owner,
        kitchen.prior,
        replace(kitchen.prior, host="new.test"),
        (str(active.transaction_id),),
    )
    await office.journal.async_begin_connection_change(intent)
    harness = _Harness()
    monkeypatch.setattr(harness, "journal", office.journal)
    provisioner = harness.provisioner()
    monkeypatch.setattr(
        provisioner, "_named_owner_resolver", partial(async_resolve_panel_owner, hass)
    )
    monkeypatch.setattr(
        provisioner, "_credential_resolver", partial(fm._async_rollback_credential, hass)
    )
    shell_factory = Mock(return_value=harness.shell)
    monkeypatch.setattr(provisioner, "_shell_factory", shell_factory)
    finalize = AsyncMock()
    monkeypatch.setattr(harness.operations, "finalize_baseline", finalize)
    monkeypatch.setattr(fm, "_get_recovery_provisioner", lambda *_args: provisioner)
    _offline_fleet(monkeypatch)
    fleet = fm.FleetManager(hass, entry)
    monkeypatch.setattr(fleet, "_async_schedule_reload_once", Mock())
    try:
        async with provisioner._lock:
            await fleet.async_setup()
            assert set(fleet.panels) == {office.owner.subentry_id}
            assert fleet._recovery_task is not None
            if revoke in {"host", "password"}:
                subentry = entry.subentries[str(office.owner.subentry_id)]
                field = CONF_HOST if revoke == "host" else CONF_ROOT_PASSWORD
                hass.config_entries.async_update_subentry(
                    entry, subentry, data={**subentry.data, field: "changed"}
                )
            elif revoke == "active_conflict":
                active = replace(active, panel_request=record.panel_request)
                await office.journal._async_save(active)
        await fleet._recovery_task
        if revoke in {None, "password"}:
            finalize.assert_awaited_once()
            assert shell_factory.call_args.args[1] == (
                "changed" if revoke == "password" else office.prior.password
            )
            assert await office.journal.async_retained(record.transaction_id) is None
        else:
            finalize.assert_not_awaited()
            assert await office.journal.async_retained(record.transaction_id) == before
        assert await office.journal.async_load() == active
        assert await office.journal.async_connection_intents() == (intent,)
    finally:
        await fleet.async_shutdown()


@pytest.mark.parametrize("algorithm", ["ssh-ed25519", "ecdsa-sha2-nistp256"])
async def test_legacy_target_pin_outer_whitespace_keeps_same_identity(
    hass: HomeAssistant,
    durable_connection_storage: None,
    algorithm: str,
) -> None:
    entry = _legacy_entry(hass)
    key = asyncssh.generate_private_key(algorithm).export_public_key().decode().strip()
    raw = f" \t{key}\n"
    hass.config_entries.async_update_entry(entry, data={**entry.data, CONF_SSH_HOST_KEY: raw})
    form = await _open_legacy_reconfigure_step(hass, entry, "reconfigure_connection")
    with patch.object(gateway, "_apply_config", AsyncMock(return_value=raw)):
        result = await hass.config_entries.flow.async_configure(
            form["flow_id"], {CONF_HOST: "moved.test", CONF_ROOT_PASSWORD: ""}
        )
    assert result.get("reason") == "reconfigure_successful"
    owner = PanelConnectionOwner(hass, entry)
    assert owner.owner.public_key == key
    owner.assert_current()
    assert await owner.journal.async_connection_intents() == ()


async def test_malformed_target_pin_returns_typed_flow_refusal(
    hass: HomeAssistant,
    durable_connection_storage: None,
) -> None:
    entry = _legacy_entry(hass)
    hass.config_entries.async_update_entry(
        entry, data={**entry.data, CONF_SSH_HOST_KEY: "invalid\npin"}
    )
    before = dict(entry.data)
    form = await _open_legacy_reconfigure_step(hass, entry, "reconfigure_connection")
    with patch.object(gateway, "_apply_config", AsyncMock()) as apply:
        result = await hass.config_entries.flow.async_configure(
            form["flow_id"], {CONF_HOST: "moved.test", CONF_ROOT_PASSWORD: ""}
        )
    assert result["type"] == "form" and result["errors"] == {"base": "host_key_changed"}
    apply.assert_not_awaited()
    assert dict(entry.data) == before


@pytest.mark.parametrize("bound", [False, True])
@pytest.mark.parametrize("sibling_pin", ["invalid\npin", "whitespace"])
async def test_owner_resolution_uses_canonical_target_and_ignores_malformed_unrelated_pin(
    hass: HomeAssistant,
    durable_connection_storage: None,
    bound: bool,
    sibling_pin: str,
) -> None:
    entry, office, kitchen = _pair(hass)
    target = entry.subentries[str(office.owner.subentry_id)]
    sibling = entry.subentries[str(kitchen.owner.subentry_id)]
    raw_sibling = f" {_OTHER_PUBLIC_KEY}\n" if sibling_pin == "whitespace" else sibling_pin
    hass.config_entries.async_update_subentry(
        entry, target, data={**target.data, CONF_SSH_HOST_KEY: office.owner.public_key + "\n"}
    )
    hass.config_entries.async_update_subentry(
        entry, sibling, data={**sibling.data, CONF_SSH_HOST_KEY: raw_sibling}
    )
    request = replace(
        _baselined_record().panel_request,
        host=office.prior.host,
        bound_owner=office.owner if bound else None,
    )
    assert await async_resolve_panel_owner(hass, request) == office.owner
    assert target.data[CONF_SSH_HOST_KEY].endswith("\n")
    assert sibling.data[CONF_SSH_HOST_KEY] == raw_sibling


@pytest.mark.parametrize(
    "operation", [ProvisioningOperation.INSTALL, ProvisioningOperation.UPGRADE]
)
async def test_actual_removal_pending_connection_keeps_only_original_typed_repair(
    hass: HomeAssistant,
    durable_connection_storage: None,
    enable_custom_integrations: None,
    monkeypatch: pytest.MonkeyPatch,
    operation: ProvisioningOperation,
) -> None:
    entry, office, _kitchen = _pair(hass)
    record = replace(_baselined_record(), operation=operation)
    await office.journal.async_create(record)
    intent = ConnectionIntent(
        str(uuid4()),
        office.owner,
        office.prior,
        replace(office.prior, host="new.test"),
        (str(record.transaction_id),),
    )
    await office.journal.async_begin_connection_change(intent)
    transport = Mock(side_effect=AssertionError("pending connection must refuse before transport"))
    monkeypatch.setattr(fm, "AsyncsshShell", transport)
    await hass.config_entries.async_remove(entry.entry_id)
    transport.assert_not_called()
    assert await office.journal.async_connection_intents() == (intent,)
    assert await office.journal.async_load() == record
    registry = ir.async_get(hass)
    assert registry.async_get_issue(DOMAIN, _issue_id(office.owner)) is not None
    assert (
        registry.async_get_issue(DOMAIN, fm._provisioning_repair_issue_id(record.transaction_id))
        is None
    )


@pytest.mark.parametrize("drift", [False, True])
async def test_active_rollback_suppresses_marker_repair_only_for_exact_provenance(
    hass: HomeAssistant,
    durable_connection_storage: None,
    monkeypatch: pytest.MonkeyPatch,
    drift: bool,
) -> None:
    entry, office, _kitchen = _pair(hass)
    record = replace(
        _baselined_record(),
        operation=ProvisioningOperation.UPDATE,
        panel_request=replace(
            _baselined_record().panel_request,
            host=office.prior.host,
            root_password=office.prior.password,
            bound_owner=office.owner,
        ),
    )
    await office.journal.async_arm(record)
    await office.journal.async_request_restore(record.transaction_id)
    await office.journal.async_begin_restore(record.transaction_id, office.prior.password)
    active = await office.journal.async_load()
    assert active is not None
    if drift:
        active = replace(active, staged_version="0.99.0")
        await office.journal._async_save(active)
    before = await office.journal.async_retained(record.transaction_id)
    fleet = fm.FleetManager(hass, entry)
    schedule = Mock()
    report = AsyncMock()
    monkeypatch.setattr(fleet, "_async_schedule_background_recovery", schedule)
    monkeypatch.setattr(fm._ProvisioningRepairReporter, "async_report_rollback_failure", report)
    with (
        patch.object(
            ProvisioningJournal,
            "async_begin_restore",
            AsyncMock(side_effect=AssertionError("no global reseed")),
        ),
        patch.object(
            ProvisioningJournal,
            "async_begin_scoped_restore",
            AsyncMock(side_effect=AssertionError("no scoped reseed")),
        ),
    ):
        await fleet._async_prepare_provisioning()
    schedule.assert_called_once_with(active)
    if drift:
        report.assert_awaited_once_with(
            record.transaction_id,
            original_code="restart_recovery",
            rollback_code="ownership_unproven",
        )
    else:
        report.assert_not_awaited()
    assert await office.journal.async_load() == active
    assert await office.journal.async_retained(record.transaction_id) == before


async def test_legacy_runtime_guidance_matches_same_key_connection_form(
    hass: HomeAssistant,
) -> None:
    entry = _legacy_entry(hass)
    manager = PanelManager(
        hass,
        LegacyPanelStore(hass, entry),
        fm.legacy_fleet_config(entry),
        fm._shared_ssh_lock(hass),
        lambda: None,
    )
    guidance = manager._host_key_changed_guidance().lower()
    assert "reconfigure to re-pin" not in guidance
    assert "remove" in guidance and "re-add" in guidance


@pytest.mark.parametrize("conflict", ["key", "slug", None])
async def test_generic_finalize_admission_defers_same_panel_and_continues_active_work(
    hass: HomeAssistant,
    durable_connection_storage: None,
    monkeypatch: pytest.MonkeyPatch,
    conflict: str | None,
) -> None:
    _entry, office, kitchen = _pair(hass)
    record = await _mark_finalizing(office)
    before = await office.journal.async_retained(record.transaction_id)
    active = replace(
        record,
        transaction_id=uuid4(),
        operation=ProvisioningOperation.UPDATE,
        prior_snapshot=replace(record.prior_snapshot, baseline=None),
        panel_request=replace(
            record.panel_request,
            slug=office.owner.slug if conflict == "slug" else kitchen.owner.slug,
            host=kitchen.prior.host,
            public_key=office.owner.public_key if conflict == "key" else kitchen.owner.public_key,
            fingerprint=_identity().fingerprint if conflict == "key" else _OTHER_FINGERPRINT,
        ),
    )
    await office.journal.async_create(active)
    harness = _Harness()
    monkeypatch.setattr(harness, "journal", office.journal)
    provisioner = harness.provisioner()
    monkeypatch.setattr(
        provisioner, "_named_owner_resolver", partial(async_resolve_panel_owner, hass)
    )
    monkeypatch.setattr(
        provisioner, "_credential_resolver", partial(fm._async_rollback_credential, hass)
    )
    shell_factory = Mock(return_value=harness.shell)
    monkeypatch.setattr(provisioner, "_shell_factory", shell_factory)
    finalize = AsyncMock()
    recover = AsyncMock()
    monkeypatch.setattr(harness.operations, "finalize_baseline", finalize)
    monkeypatch.setattr(provisioner, "_async_recover_record", recover)
    await provisioner.async_recover()
    recover.assert_awaited_once()
    assert recover.await_args is not None and recover.await_args.args[0] == active
    assert await office.journal.async_load() == active
    if conflict is None:
        finalize.assert_awaited_once()
        assert await office.journal.async_retained(record.transaction_id) is None
    else:
        shell_factory.assert_not_called()
        finalize.assert_not_awaited()
        assert await office.journal.async_retained(record.transaction_id) == before


async def test_queued_legacy_connection_keeps_original_prelock_tuple(
    hass: HomeAssistant,
    durable_connection_storage: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entry = _legacy_entry(hass)
    form = await _open_legacy_reconfigure_step(hass, entry, "reconfigure_connection")
    captured = asyncio.Event()
    owner_class = PanelConnectionOwner

    def capture_owner(*args: Any, **kwargs: Any) -> PanelConnectionOwner:
        owner = owner_class(*args, **kwargs)
        captured.set()
        return owner

    monkeypatch.setattr(legacy_flow, "PanelConnectionOwner", capture_owner)
    apply = AsyncMock()
    monkeypatch.setattr(gateway, "_apply_config", apply)
    task: asyncio.Task[Any] | None = None
    try:
        async with fm._shared_ssh_lock(hass):
            task = asyncio.create_task(
                hass.config_entries.flow.async_configure(
                    form["flow_id"], {CONF_HOST: "older-request.test", CONF_ROOT_PASSWORD: ""}
                )
            )
            await asyncio.wait_for(captured.wait(), 1)
            assert not task.done()
            hass.config_entries.async_update_entry(
                entry, data={**entry.data, CONF_ROOT_PASSWORD: "newer-login"}
            )
        result = await task
        assert result["type"] == "form" and result["errors"] == {"base": "parent_changed"}
        apply.assert_not_awaited()
        assert entry.data[CONF_ROOT_PASSWORD] == "newer-login"
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_rebind_cancellation_during_fair_lifecycle_acquire_releases_operation_lock(
    hass: HomeAssistant,
    durable_connection_storage: None,
) -> None:
    entry, _office, _kitchen = _pair(hass)
    fleet = fm.FleetManager(hass, entry)
    await fleet._lifecycle_lock.acquire()
    release = asyncio.Event()

    async def prior_waiter() -> None:
        async with fleet._lifecycle_lock:
            await release.wait()

    waiter = asyncio.create_task(prior_waiter())
    await asyncio.sleep(0)

    async def rebind() -> None:
        fleet._lifecycle_lock.release()
        current = asyncio.current_task()
        assert current is not None
        asyncio.get_running_loop().call_soon(current.cancel)
        await fleet.async_rebind_panel(
            "panel-office", Mock(), host="fake.test", root_password="fake", candidate=_identity()
        )

    try:
        with pytest.raises(asyncio.CancelledError):
            await asyncio.create_task(rebind())
        assert not fleet._ssh_lock.locked()
    finally:
        release.set()
        await waiter
        if fleet._ssh_lock.locked():
            fleet._ssh_lock.release()


@pytest.mark.parametrize(
    "operation", [ProvisioningOperation.INSTALL, ProvisioningOperation.UPGRADE]
)
async def test_actual_removal_later_pending_gate_keeps_only_original_typed_repair(
    hass: HomeAssistant,
    durable_connection_storage: None,
    enable_custom_integrations: None,
    monkeypatch: pytest.MonkeyPatch,
    operation: ProvisioningOperation,
) -> None:
    record = replace(
        _baselined_record(),
        operation=operation,
        fleet_profile=fleet_fixtures._pending_record().fleet_profile,
    )
    journal = ProvisioningJournal(hass)
    await journal.async_create(record)
    entry = fleet_fixtures._empty_fleet_entry()
    entry.add_to_hass(hass)
    request = record.panel_request
    owner = PanelOwner(entry.entry_id, None, request.slug, request.public_key)
    prior = PanelConnection(
        request.host, request.ssh_username, request.root_password, request.public_key
    )
    intent = ConnectionIntent(
        str(uuid4()), owner, prior, replace(prior, host="new.test"), (str(record.transaction_id),)
    )
    queued = asyncio.Event()
    factory = fm._get_recovery_provisioner

    def queued_factory(*args: Any, **kwargs: Any) -> Any:
        result = factory(*args, **kwargs)
        queued.set()
        return result

    monkeypatch.setattr(fm, "_get_recovery_provisioner", queued_factory)
    transport = Mock(side_effect=AssertionError("pending connection must refuse before transport"))
    monkeypatch.setattr(fm, "AsyncsshShell", transport)
    removal: asyncio.Task[Any] | None = None
    try:
        async with fm._shared_ssh_lock(hass):
            removal = asyncio.create_task(hass.config_entries.async_remove(entry.entry_id))
            await asyncio.wait_for(queued.wait(), 2)
            await journal.async_begin_connection_change(intent)
        await removal
        transport.assert_not_called()
        assert await journal.async_connection_intents() == (intent,)
        assert await journal.async_load() == record
        registry = ir.async_get(hass)
        assert registry.async_get_issue(DOMAIN, _issue_id(owner)) is not None
        assert (
            registry.async_get_issue(
                DOMAIN, fm._provisioning_repair_issue_id(record.transaction_id)
            )
            is None
        )
    finally:
        if removal is not None and not removal.done():
            removal.cancel()
            await asyncio.gather(removal, return_exceptions=True)
