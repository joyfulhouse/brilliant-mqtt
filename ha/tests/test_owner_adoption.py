"""Crash and concurrency boundaries of a verified panel owner change."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType
from typing import Any
from unittest.mock import ANY, AsyncMock, Mock
from uuid import uuid4

import asyncssh
import pytest
from homeassistant import config_entries
from homeassistant.const import CONF_NAME
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import issue_registry as ir
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.brilliant_mqtt import fleet_manager, panel_ops
from custom_components.brilliant_mqtt.const import (
    CONF_HOST,
    CONF_PANEL,
    CONF_ROOT_PASSWORD,
    DATA_SSH_HOST_KEY,
    DOMAIN,
    OPT_TRUST_HOST_KEY_CHANGES,
)
from custom_components.brilliant_mqtt.flow import gateway
from custom_components.brilliant_mqtt.panel_provisioner import PanelProvisioningError
from custom_components.brilliant_mqtt.provisioning_journal import (
    _JOURNAL_COORDINATOR_KEY,
    JOURNAL_STORAGE_KEY,
    ProvisioningJournal,
    ProvisioningJournalError,
    ProvisioningPhase,
)
from tests.fakes import FakeShell
from tests.test_address_baseline_host import (
    _NEW_HOST,
    _OLD_HOST,
    _arm_soak,
    _baseline_record,
    _legacy_runtime,
    _managed_record,
    _manager,
    _storage_data,
)
from tests.test_address_baseline_host import durable_journal as durable_journal
from tests.test_config_flow import (
    _FINGERPRINT,
    _OTHER_FINGERPRINT,
    _OTHER_PUBLIC_KEY,
    _PUBLIC_KEY,
    _fleet_entry,
    _identity,
    _open_legacy_reconfigure_step,
    _start_panel_reconfigure_step,
    _subentry,
    _submit_legacy_reconfigure,
)
from tests.test_init import ENTRY_DATA
from tests.test_manager import _legacy_manager

pytestmark = pytest.mark.usefixtures("durable_journal")


@pytest.mark.parametrize("later_edit", ["none", "host", "login"])
@pytest.mark.parametrize("old_format", [False, True])
async def test_pending_address_change_requires_fresh_verified_flow(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, later_edit: str, old_format: bool
) -> None:
    entry, panel = _fleet(hass)
    record = _baseline_record(host=_OLD_HOST)
    await _arm_soak(hass, record)
    with monkeypatch.context() as fault:
        fault.setattr(
            ProvisioningJournal,
            "_async_retained_save",
            AsyncMock(side_effect=ProvisioningJournalError("journal_save_failed")),
        )
        result = await _change(
            hass,
            entry,
            panel,
            **{CONF_HOST: _NEW_HOST, CONF_ROOT_PASSWORD: panel.data[CONF_ROOT_PASSWORD]},
        )
        assert result["errors"]["base"] == "journal_failed"
    journal = ProvisioningJournal(hass)
    saved = await journal._store.async_load()
    assert saved is not None
    if old_format:
        saved["pending_owner"].pop("entry_id")
        saved["pending_owner"].pop("subentry_id")
        await journal._store.async_save(saved)
    if later_edit != "none":
        changed = (
            {CONF_HOST: "later-office.example"}
            if later_edit == "host"
            else {CONF_ROOT_PASSWORD: "later-login"}
        )
        hass.config_entries.async_update_subentry(entry, panel, data={**panel.data, **changed})
    await hass.config_entries._store.async_save(hass.config_entries._data_to_save())
    before = dict(panel.data)
    retained_before = _storage_data(hass, JOURNAL_STORAGE_KEY + ".retained")
    for _ in range(2):
        hass.data.pop(_JOURNAL_COORDINATOR_KEY, None)
        assert await ProvisioningJournal(hass).async_load() is None
    assert dict(panel.data) == before
    assert _storage_data(hass, JOURNAL_STORAGE_KEY) == saved
    assert _storage_data(hass, JOURNAL_STORAGE_KEY + ".retained") == retained_before
    issues = [issue for (domain, _), issue in ir.async_get(hass).issues.items() if domain == DOMAIN]
    assert len(issues) == 1
    assert issues[0].translation_key == "address_change_incomplete"
    assert issues[0].translation_placeholders == {"panel": "office"}
    assert issues[0].severity == ir.IssueSeverity.ERROR
    assert await fleet_manager._async_rollback_credential(hass, record.panel_request) is None
    result = await _change(
        hass,
        entry,
        panel,
        **{CONF_HOST: _NEW_HOST, CONF_ROOT_PASSWORD: panel.data[CONF_ROOT_PASSWORD]},
    )
    assert result["reason"] == "reconfigure_successful"
    retained = await ProvisioningJournal(hass).async_retained(record.transaction_id)
    assert retained is not None and retained.record.panel_request.host == _NEW_HOST
    assert not [
        issue for (domain, _), issue in ir.async_get(hass).issues.items() if domain == DOMAIN
    ]
    assert not await hass.async_add_executor_job(Path(journal._store.path).exists)


async def test_two_interrupted_panels_keep_independent_pending_records(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> None:
    entry, office = _fleet(hass)
    kitchen = _subentry(
        slug="kitchen",
        subentry_id="panel-kitchen",
        priority=2,
        public_key=_OTHER_PUBLIC_KEY,
        fingerprint=_OTHER_FINGERPRINT,
    )
    hass.config_entries.async_add_subentry(entry, kitchen)
    kitchen = entry.subentries[kitchen.subentry_id]
    first = _baseline_record(host=office.data[CONF_HOST])
    second = _baseline_record(
        slug="kitchen",
        host=kitchen.data[CONF_HOST],
        public_key=_OTHER_PUBLIC_KEY,
        fingerprint=_OTHER_FINGERPRINT,
        transaction_id=uuid4(),
    )
    await _arm_soak(hass, first)
    await _arm_soak(hass, second)
    with monkeypatch.context() as fault:
        fault.setattr(
            ProvisioningJournal,
            "_async_retained_save",
            AsyncMock(side_effect=ProvisioningJournalError("journal_save_failed")),
        )
        for panel, host, key, fingerprint in (
            (office, _NEW_HOST, _PUBLIC_KEY, _FINGERPRINT),
            (kitchen, "new-kitchen.example", _OTHER_PUBLIC_KEY, _OTHER_FINGERPRINT),
        ):
            with pytest.raises(ProvisioningJournalError, match="journal_save_failed"):
                await ProvisioningJournal(hass).async_adopt_verified_host(
                    entry_id=entry.entry_id,
                    subentry_id=panel.subentry_id,
                    slug=panel.data[CONF_PANEL],
                    public_key=key,
                    fingerprint=fingerprint,
                    host=host,
                    expected_host=panel.data[CONF_HOST],
                    expected_password=panel.data[CONF_ROOT_PASSWORD],
                    root_password=panel.data[CONF_ROOT_PASSWORD],
                    repair_pending=True,
                )
    saved = _storage_data(hass, JOURNAL_STORAGE_KEY)
    assert len(saved["pending_owners"]) == 2
    hass.data.pop(_JOURNAL_COORDINATOR_KEY)
    assert await ProvisioningJournal(hass).async_load() is None
    assert _storage_data(hass, JOURNAL_STORAGE_KEY) == saved
    # Resolving kitchen leaves office's evidence and repair in place.
    await ProvisioningJournal(hass).async_adopt_verified_host(
        entry_id=entry.entry_id,
        subentry_id=kitchen.subentry_id,
        slug="kitchen",
        public_key=_OTHER_PUBLIC_KEY,
        fingerprint=_OTHER_FINGERPRINT,
        host="new-kitchen.example",
        expected_host=kitchen.data[CONF_HOST],
        expected_password=kitchen.data[CONF_ROOT_PASSWORD],
        root_password=kitchen.data[CONF_ROOT_PASSWORD],
        repair_pending=True,
    )
    assert _storage_data(hass, JOURNAL_STORAGE_KEY)["pending_owner"] == saved["pending_owners"][0]
    assert (
        len([issue for (domain, _), issue in ir.async_get(hass).issues.items() if domain == DOMAIN])
        == 1
    )
    retained = await ProvisioningJournal(hass).async_retained(first.transaction_id)
    assert retained is not None and retained.record.panel_request.host == office.data[CONF_HOST]


@pytest.mark.parametrize("later_edit", [False, True])
@pytest.mark.parametrize("old_format", [False, True])
async def test_legacy_pending_change_preserves_later_saved_address(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, later_edit: bool, old_format: bool
) -> None:
    monkeypatch.setattr(config_entries, "SAVE_DELAY", 0.01)
    entry, manager = _legacy_runtime(hass, host=_NEW_HOST, public_key=_PUBLIC_KEY)
    await hass.config_entries._store.async_save(hass.config_entries._data_to_save())
    record = _baseline_record(host=_OLD_HOST)
    await _arm_soak(hass, record)
    with monkeypatch.context() as fault:
        fault.setattr(
            ProvisioningJournal,
            "_async_retained_save",
            AsyncMock(side_effect=ProvisioningJournalError("journal_save_failed")),
        )
        with pytest.raises(HomeAssistantError, match="journal_save_failed"):
            await manager.async_canary_operation("finalize", str(record.transaction_id))
    if old_format:
        journal = ProvisioningJournal(hass)
        saved = await journal._store.async_load()
        assert saved is not None
        saved["pending_owner"].pop("expected_host")
        await journal._store.async_save(saved)
    expected_host = _NEW_HOST
    if later_edit:
        expected_host = "later-office.example"
        hass.config_entries.async_update_entry(entry, data={**entry.data, CONF_HOST: expected_host})
        await hass.config_entries._store.async_save(hass.config_entries._data_to_save())
    assert entry.data[CONF_HOST] == expected_host
    assert await ProvisioningJournal(hass).async_load() is None
    assert entry.data[CONF_HOST] == expected_host
    retained = await ProvisioningJournal(hass).async_retained(record.transaction_id)
    assert retained is not None
    assert retained.record.panel_request.host == _OLD_HOST
    assert retained.state == "soak"
    assert "pending_owner" in _storage_data(hass, JOURNAL_STORAGE_KEY)
    form = await _open_legacy_reconfigure_step(hass, entry, "reconfigure_connection")
    result = await _submit_legacy_reconfigure(
        hass,
        form,
        {CONF_HOST: expected_host, CONF_ROOT_PASSWORD: ""},
        AsyncMock(return_value=_PUBLIC_KEY),
    )
    assert result["reason"] == "reconfigure_successful"
    retained = await ProvisioningJournal(hass).async_retained(record.transaction_id)
    assert retained is not None and retained.record.panel_request.host == expected_host
    assert not await hass.async_add_executor_job(
        Path(hass.config.path(".storage", JOURNAL_STORAGE_KEY)).exists
    )


@pytest.mark.parametrize("durable_removal", [False, True])
async def test_interactive_adoption_rejects_disappearing_owner(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, durable_removal: bool
) -> None:
    entry, panel = _fleet(hass)
    await hass.config_entries._store.async_save(hass.config_entries._data_to_save())
    record = _baseline_record(host=_OLD_HOST)
    await _arm_soak(hass, record)
    saved, resume = asyncio.Event(), asyncio.Event()
    persist = ProvisioningJournal._async_persist

    async def gated(journal: ProvisioningJournal, active: Any, pending: Any) -> None:
        await persist(journal, active, pending)
        if pending is not None and not saved.is_set():
            saved.set()
            await resume.wait()

    monkeypatch.setattr(ProvisioningJournal, "_async_persist", gated)
    monkeypatch.setattr(config_entries, "SAVE_DELAY", 60)
    monkeypatch.setattr(fleet_manager, "_CONFIG_ENTRY_PERSISTENCE_TIMEOUT_SECONDS", 0.03)
    task = asyncio.create_task(
        _change(hass, entry, panel, **{CONF_HOST: _NEW_HOST, CONF_ROOT_PASSWORD: "verified"})
    )
    await asyncio.wait_for(saved.wait(), 10)
    try:
        hass.config_entries.async_remove_subentry(entry, panel.subentry_id)
        if durable_removal:
            await hass.config_entries._store.async_save(hass.config_entries._data_to_save())
    finally:
        resume.set()
    result = await task
    assert result.get("reason") == "parent_changed"
    retained = await ProvisioningJournal(hass).async_retained(record.transaction_id)
    assert retained is not None and retained.record.panel_request.host == _OLD_HOST


@pytest.mark.parametrize("action", ["finalize", "rollback"])
async def test_dispatched_canary_cannot_use_replacement_owner(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, action: str
) -> None:
    entry, panel = _fleet(hass)
    record = _baseline_record(host=_OLD_HOST)
    await _arm_soak(hass, record)
    manager = _manager(hass, entry, panel)
    provisioner = fleet_manager._get_recovery_provisioner(hass)
    monkeypatch.setattr(fleet_manager, "_get_recovery_provisioner", lambda _: provisioner)
    reading, resume = asyncio.Event(), asyncio.Event()
    load = ProvisioningJournal._async_retained_load

    async def gated(journal: ProvisioningJournal) -> Any:
        rows = await load(journal)
        if journal is provisioner._journal and not reading.is_set():
            reading.set()
            await resume.wait()
        return rows

    monkeypatch.setattr(ProvisioningJournal, "_async_retained_load", gated)
    transport = Mock(side_effect=lambda host, password, key: FakeShell(pinned=key))
    monkeypatch.setattr(provisioner, "_shell_factory", transport)
    monkeypatch.setattr(provisioner, "async_recover", AsyncMock())
    monkeypatch.setattr(panel_ops, "finalize_baseline", AsyncMock())
    task = asyncio.create_task(manager.async_canary_operation(action, str(record.transaction_id)))
    await asyncio.wait_for(reading.wait(), 10)
    try:
        hass.config_entries.async_remove_subentry(entry, panel.subentry_id)
        replacement = config_entries.ConfigSubentry(
            data=MappingProxyType({**panel.data, CONF_ROOT_PASSWORD: "replacement-login"}),
            subentry_id=uuid4().hex,
            subentry_type=panel.subentry_type,
            title=panel.title,
            unique_id=panel.unique_id,
        )
        hass.config_entries.async_add_subentry(entry, replacement)
    finally:
        resume.set()
    with pytest.raises(HomeAssistantError, match="rollback_credentials_unavailable"):
        await task
    transport.assert_not_called()
    retained = await ProvisioningJournal(hass).async_retained(record.transaction_id)
    assert retained is not None and retained.state == "soak"
    assert await ProvisioningJournal(hass).async_load() is None


async def test_legacy_repin_keeps_evidence_and_does_not_block_another_panel(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> None:
    office, manager = _legacy_runtime(hass, host=_NEW_HOST, public_key=_PUBLIC_KEY)
    hass.config_entries.async_update_entry(office, options={OPT_TRUST_HOST_KEY_CHANGES: True})
    kitchen = MockConfigEntry(
        domain=DOMAIN,
        unique_id="kitchen",
        version=3,
        data={
            **ENTRY_DATA,
            CONF_PANEL: "kitchen",
            CONF_HOST: "192.0.2.30",
            DATA_SSH_HOST_KEY: _OTHER_PUBLIC_KEY,
        },
    )
    kitchen.add_to_hass(hass)
    other_manager = _legacy_manager(hass, kitchen, fleet_manager._shared_ssh_lock(hass))
    first = _baseline_record(host=_OLD_HOST)
    other = _baseline_record(
        slug="kitchen",
        host=kitchen.data[CONF_HOST],
        public_key=_OTHER_PUBLIC_KEY,
        fingerprint=_OTHER_FINGERPRINT,
        transaction_id=uuid4(),
    )
    await _arm_soak(hass, first)
    await _arm_soak(hass, other)
    with monkeypatch.context() as fault:
        fault.setattr(
            ProvisioningJournal,
            "_async_retained_save",
            AsyncMock(side_effect=ProvisioningJournalError("journal_save_failed")),
        )
        with pytest.raises(HomeAssistantError, match="journal_save_failed"):
            await manager.async_canary_operation("finalize", str(first.transaction_id))
    rotated = asyncssh.generate_private_key("ssh-ed25519").export_public_key().decode().strip()
    monkeypatch.setattr(
        manager, "_shell", lambda: FakeShell(connect_error=asyncssh.HostKeyNotVerifiable("fixture"))
    )
    monkeypatch.setattr(manager, "_shell_unpinned", lambda: FakeShell(pinned=rotated))
    async with fleet_manager._shared_ssh_lock(hass):
        shell = await manager._connect_for_repair()
        await shell.close()
    await hass.config_entries._store.async_save(hass.config_entries._data_to_save())
    assert await ProvisioningJournal(hass).async_load() is None
    provisioner = AsyncMock()
    monkeypatch.setattr(fleet_manager, "_get_recovery_provisioner", lambda _: provisioner)
    await other_manager.async_canary_operation("rollback", str(other.transaction_id))
    provisioner.async_rollback.assert_awaited_once()
    retained = await ProvisioningJournal(hass).async_retained(first.transaction_id)
    assert retained is not None
    assert retained.record.panel_request.public_key == _PUBLIC_KEY
    assert retained.record.panel_request.host == _OLD_HOST
    assert "pending_owner" in _storage_data(hass, JOURNAL_STORAGE_KEY)


@pytest.mark.parametrize("boundary", ["unflushed", "invalid_pin", "active"])
async def test_legacy_repin_preserves_unproven_or_active_intent(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, boundary: str
) -> None:
    entry, manager = _legacy_runtime(hass, host=_NEW_HOST, public_key=_PUBLIC_KEY)
    await hass.config_entries._store.async_save(hass.config_entries._data_to_save())
    record = (
        _managed_record(host=_OLD_HOST)
        if boundary == "active"
        else _baseline_record(host=_OLD_HOST)
    )
    await _arm_soak(hass, record)
    if boundary == "active":
        await ProvisioningJournal(hass).async_create(record)
    with monkeypatch.context() as fault:
        fault.setattr(
            ProvisioningJournal,
            "_async_retained_save",
            AsyncMock(side_effect=ProvisioningJournalError("journal_save_failed")),
        )
        with pytest.raises(HomeAssistantError, match="journal_save_failed"):
            await manager.async_canary_operation("finalize", str(record.transaction_id))
    key = "invalid-key" if boundary == "invalid_pin" else _OTHER_PUBLIC_KEY
    hass.config_entries.async_update_entry(entry, data={**entry.data, DATA_SSH_HOST_KEY: key})
    if boundary == "unflushed":
        monkeypatch.setattr(hass.config_entries._store, "async_delay_save", Mock())
        monkeypatch.setattr(fleet_manager, "_CONFIG_ENTRY_PERSISTENCE_TIMEOUT_SECONDS", 0.03)
    else:
        await hass.config_entries._store.async_save(hass.config_entries._data_to_save())
    await ProvisioningJournal(hass).async_load()
    assert "pending_owner" in _storage_data(hass, JOURNAL_STORAGE_KEY)
    retained = await ProvisioningJournal(hass).async_retained(record.transaction_id)
    assert retained is not None and retained.record.panel_request.public_key == _PUBLIC_KEY


@pytest.fixture(autouse=True)
def offline_owner_changes(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Inspect only fixtures; a test must explicitly opt into a fake shell."""
    monkeypatch.setattr(gateway, "async_fetch_host_identity", AsyncMock(return_value=_identity()))
    monkeypatch.setattr(gateway, "_async_inspect_candidate", AsyncMock(return_value=None))
    monkeypatch.setattr(fleet_manager, "AsyncsshShell", Mock(side_effect=AssertionError("shell")))
    yield


async def _change(hass: HomeAssistant, entry: Any, panel: Any, **data: Any) -> Any:
    form = await _start_panel_reconfigure_step(hass, entry, panel, "repair_credentials")
    return await hass.config_entries.subentries.async_configure(form["flow_id"], data)


def _fleet(hass: HomeAssistant) -> tuple[Any, Any]:
    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    panel = entry.subentries[panel.subentry_id]
    hass.config_entries.async_update_subentry(
        entry, panel, data={**panel.data, CONF_HOST: _OLD_HOST}
    )
    return entry, panel


async def test_adoption_keeps_intent_until_config_reaches_disk(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> None:
    entry, panel = _fleet(hass)
    journal = ProvisioningJournal(hass)
    await journal.async_create(_managed_record(host=_OLD_HOST))
    core_store = hass.config_entries._store
    await core_store.async_save(hass.config_entries._data_to_save())
    before = await hass.async_add_executor_job(Path(core_store.path).read_bytes)
    monkeypatch.setattr(core_store, "async_delay_save", Mock())
    monkeypatch.setattr(fleet_manager, "_CONFIG_ENTRY_PERSISTENCE_TIMEOUT_SECONDS", 0.02)
    result = await _change(
        hass, entry, panel, **{CONF_HOST: _NEW_HOST, CONF_ROOT_PASSWORD: "new-login"}
    )
    assert result["type"] == "form"
    assert result["errors"] == {"base": "journal_failed"}
    assert await hass.async_add_executor_job(Path(core_store.path).read_bytes) == before
    assert "pending_owner" in _storage_data(hass, JOURNAL_STORAGE_KEY)

    await core_store.async_save(hass.config_entries._data_to_save())
    pending = _storage_data(hass, JOURNAL_STORAGE_KEY)
    loaded = await ProvisioningJournal(hass).async_load()
    assert loaded is not None
    with pytest.raises(PanelProvisioningError, match="provisioning_recovery_required"):
        await fleet_manager._async_authorize_recorded_recovery(hass, loaded)
    assert _storage_data(hass, JOURNAL_STORAGE_KEY) == pending


async def test_cancelled_disk_proof_requires_fresh_flow(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> None:
    entry, panel = _fleet(hass)
    await ProvisioningJournal(hass).async_create(_managed_record(host=_OLD_HOST))
    proving = asyncio.Event()
    original = fleet_manager.async_wait_config_entry_persisted

    async def paused(*args: Any, **kwargs: Any) -> None:
        proving.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(fleet_manager, "async_wait_config_entry_persisted", paused)
    changing = asyncio.create_task(
        _change(hass, entry, panel, **{CONF_HOST: _NEW_HOST, CONF_ROOT_PASSWORD: "new-login"})
    )
    await asyncio.wait_for(proving.wait(), 5)
    changing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await changing
    assert "pending_owner" in _storage_data(hass, JOURNAL_STORAGE_KEY)
    monkeypatch.setattr(fleet_manager, "async_wait_config_entry_persisted", original)
    await hass.config_entries._store.async_save(hass.config_entries._data_to_save())
    record = await ProvisioningJournal(hass).async_load()
    assert record is not None
    with pytest.raises(PanelProvisioningError, match="provisioning_recovery_required"):
        await fleet_manager._async_authorize_recorded_recovery(hass, record)
    assert "pending_owner" in _storage_data(hass, JOURNAL_STORAGE_KEY)
    result = await _change(
        hass, entry, panel, **{CONF_HOST: _NEW_HOST, CONF_ROOT_PASSWORD: "new-login"}
    )
    assert result["reason"] == "reconfigure_successful"
    assert "pending_owner" not in _storage_data(hass, JOURNAL_STORAGE_KEY)


@pytest.mark.parametrize("active", [False, True])
async def test_adoption_preserves_rename_during_storage_read(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, active: bool
) -> None:
    entry, panel = _fleet(hass)
    if active:
        await ProvisioningJournal(hass).async_create(_managed_record(host=_OLD_HOST))
    address = await _start_panel_reconfigure_step(hass, entry, panel, "address")
    rename = await _start_panel_reconfigure_step(hass, entry, panel, "rename")
    reading, resume = asyncio.Event(), asyncio.Event()
    original = ProvisioningJournal._async_retained_load
    count = 0

    async def paused(journal: ProvisioningJournal) -> Any:
        nonlocal count
        count += 1
        if count == (2 if active else 1):
            reading.set()
            await resume.wait()
        return await original(journal)

    monkeypatch.setattr(ProvisioningJournal, "_async_retained_load", paused)
    moving = asyncio.create_task(
        hass.config_entries.subentries.async_configure(address["flow_id"], {CONF_HOST: _NEW_HOST})
    )
    await asyncio.wait_for(reading.wait(), 5)
    try:
        result = await hass.config_entries.subentries.async_configure(
            rename["flow_id"], {CONF_NAME: "New Office"}
        )
        assert result["reason"] == "reconfigure_successful"
    finally:
        resume.set()
    assert (await moving)["reason"] == "reconfigure_successful"
    assert panel.title == panel.data[CONF_NAME] == "New Office"


@pytest.mark.parametrize("action", ["repair", "rollback"])
async def test_removed_owner_intent_does_not_block_another_panel(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, action: str
) -> None:
    first = _subentry()
    second = _subentry(
        slug="kitchen",
        subentry_id="panel-kitchen",
        priority=2,
        public_key=_OTHER_PUBLIC_KEY,
        fingerprint=_OTHER_FINGERPRINT,
    )
    entry = _fleet_entry(hass, first, second)
    first, second = entry.subentries[first.subentry_id], entry.subentries[second.subentry_id]
    record = _baseline_record(host=first.data[CONF_HOST])
    other = _baseline_record(
        host=second.data[CONF_HOST],
        slug="kitchen",
        public_key=_OTHER_PUBLIC_KEY,
        fingerprint=_OTHER_FINGERPRINT,
        transaction_id=uuid4(),
    )
    await _arm_soak(hass, record)
    await _arm_soak(hass, other)
    with monkeypatch.context() as fault:
        fault.setattr(
            ProvisioningJournal,
            "_async_retained_save",
            AsyncMock(side_effect=ProvisioningJournalError("journal_save_failed")),
        )
        with pytest.raises(ProvisioningJournalError):
            await ProvisioningJournal(hass).async_adopt_verified_host(
                entry_id=entry.entry_id,
                subentry_id=first.subentry_id,
                slug="office",
                public_key=_PUBLIC_KEY,
                fingerprint=_FINGERPRINT,
                host=_NEW_HOST,
                expected_host=first.data[CONF_HOST],
                expected_password=first.data[CONF_ROOT_PASSWORD],
                root_password=first.data[CONF_ROOT_PASSWORD],
            )
    hass.config_entries.async_remove_subentry(entry, first.subentry_id)
    assert await ProvisioningJournal(hass).async_load() is None
    if action == "rollback":
        provisioner = AsyncMock()
        monkeypatch.setattr(fleet_manager, "_get_recovery_provisioner", lambda _: provisioner)
        await _manager(hass, entry, second).async_canary_operation(
            "rollback", str(other.transaction_id)
        )
        provisioner.async_rollback.assert_awaited_once_with(
            other.transaction_id, credential_resolver=ANY
        )
    else:
        await ProvisioningJournal(hass).async_adopt_verified_host(
            entry_id=entry.entry_id,
            subentry_id=second.subentry_id,
            slug="kitchen",
            public_key=_OTHER_PUBLIC_KEY,
            fingerprint=_OTHER_FINGERPRINT,
            host=_NEW_HOST,
            expected_host=second.data[CONF_HOST],
            expected_password=second.data[CONF_ROOT_PASSWORD],
            root_password=second.data[CONF_ROOT_PASSWORD],
        )
        assert second.data[CONF_HOST] == _NEW_HOST
    retained = await ProvisioningJournal(hass).async_retained(record.transaction_id)
    assert retained is not None
    assert retained.record.panel_request.host == first.data[CONF_HOST]


@pytest.mark.parametrize("action", ["rollback", "finalize"])
async def test_canary_dispatch_serializes_with_verified_repair(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch, action: str
) -> None:
    entry, panel = _fleet(hass)
    record = _baseline_record(host=_OLD_HOST)
    await _arm_soak(hass, record)
    form = await _start_panel_reconfigure_step(hass, entry, panel, "repair_credentials")
    monkeypatch.setattr(fleet_manager, "AsyncsshShell", lambda host, pw, key: FakeShell(pinned=key))
    monkeypatch.setattr(panel_ops, "finalize_baseline", AsyncMock())
    provisioner = fleet_manager._get_recovery_provisioner(hass)
    monkeypatch.setattr(fleet_manager, "_get_recovery_provisioner", lambda _: provisioner)
    reading, queued = asyncio.Event(), asyncio.Event()
    original_read = ProvisioningJournal._async_retained_load
    original_adopt = ProvisioningJournal.async_adopt_verified_host

    async def paused(journal: ProvisioningJournal) -> Any:
        rows = await original_read(journal)
        if journal is provisioner._journal and not reading.is_set():
            reading.set()
            await queued.wait()
        return rows

    async def adopting(journal: ProvisioningJournal, **kwargs: Any) -> None:
        if kwargs["host"] == _NEW_HOST:
            queued.set()
        await original_adopt(journal, **kwargs)

    monkeypatch.setattr(ProvisioningJournal, "_async_retained_load", paused)
    monkeypatch.setattr(ProvisioningJournal, "async_adopt_verified_host", adopting)
    if action == "rollback":
        monkeypatch.setattr(
            provisioner, "async_recover", AsyncMock(side_effect=asyncio.CancelledError)
        )
    command = asyncio.create_task(
        _manager(hass, entry, panel).async_canary_operation(action, str(record.transaction_id))
    )
    await asyncio.wait_for(reading.wait(), 5)
    repair = asyncio.create_task(
        hass.config_entries.subentries.async_configure(
            form["flow_id"], {CONF_HOST: _NEW_HOST, CONF_ROOT_PASSWORD: "rotated-login"}
        )
    )
    if action == "rollback":
        with pytest.raises(asyncio.CancelledError):
            await command
    else:
        await command
    assert (await repair)["reason"] == "reconfigure_successful"
    if action == "rollback":
        active = await ProvisioningJournal(hass).async_load()
        assert active is not None
        assert active.panel_request.host == _NEW_HOST
        assert active.panel_request.root_password == "rotated-login"
        await fleet_manager._async_authorize_recorded_recovery(hass, active)
    else:
        assert await ProvisioningJournal(hass).async_retained(record.transaction_id) is None


async def test_unflushed_owner_removal_preserves_recovery_intent(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> None:
    entry, panel = _fleet(hass)
    await hass.config_entries._store.async_save(hass.config_entries._data_to_save())
    record = _baseline_record(host=_OLD_HOST)
    await _arm_soak(hass, record)
    monkeypatch.setattr(config_entries, "SAVE_DELAY", 60)
    monkeypatch.setattr(fleet_manager, "_CONFIG_ENTRY_PERSISTENCE_TIMEOUT_SECONDS", 0.05)
    result = await _change(
        hass,
        entry,
        panel,
        **{
            CONF_HOST: _NEW_HOST,
            CONF_ROOT_PASSWORD: "verified-new-login",
        },
    )
    assert result["errors"]["base"] == "journal_failed"
    hass.config_entries.async_remove_subentry(entry, panel.subentry_id)
    assert await ProvisioningJournal(hass).async_load() is None
    assert "pending_owner" in _storage_data(hass, JOURNAL_STORAGE_KEY)
    disk = _storage_data(hass, "core.config_entries")["entries"][0]["subentries"][0]
    restored = config_entries.ConfigSubentry(
        data=MappingProxyType(disk["data"]),
        subentry_id=disk["subentry_id"],
        subentry_type=disk["subentry_type"],
        title=disk["title"],
        unique_id=disk["unique_id"],
    )
    monkeypatch.setattr(config_entries, "SAVE_DELAY", 0.01)
    monkeypatch.setattr(fleet_manager, "_CONFIG_ENTRY_PERSISTENCE_TIMEOUT_SECONDS", 1.0)
    # A cold HA restart also replaces the core Store's in-memory delay state.
    hass.config_entries._store = config_entries.ConfigEntryStore(hass)
    hass.config_entries.async_add_subentry(entry, restored)
    hass.data.pop(_JOURNAL_COORDINATOR_KEY)
    await ProvisioningJournal(hass).async_load()
    retained = await ProvisioningJournal(hass).async_retained(record.transaction_id)
    assert retained is not None
    assert (
        await fleet_manager._async_rollback_credential(hass, retained.record.panel_request) is None
    )
    assert "pending_owner" in _storage_data(hass, JOURNAL_STORAGE_KEY)


async def test_queued_repair_refuses_replacement_subentry(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> None:
    entry, panel = _fleet(hass)
    form = await _start_panel_reconfigure_step(hass, entry, panel, "repair_credentials")
    entered = asyncio.Event()
    original = ProvisioningJournal.async_adopt_verified_host

    async def queued(journal: ProvisioningJournal, **kwargs: Any) -> None:
        entered.set()
        await original(journal, **kwargs)

    monkeypatch.setattr(ProvisioningJournal, "async_adopt_verified_host", queued)
    async with fleet_manager._shared_ssh_lock(hass):
        changing = asyncio.create_task(
            hass.config_entries.subentries.async_configure(
                form["flow_id"], {CONF_HOST: _NEW_HOST, CONF_ROOT_PASSWORD: "old-flow-login"}
            )
        )
        await asyncio.wait_for(entered.wait(), 5)
        hass.config_entries.async_remove_subentry(entry, panel.subentry_id)
        added = config_entries.ConfigSubentry(
            data=MappingProxyType(dict(panel.data)),
            subentry_id=uuid4().hex,
            subentry_type=panel.subentry_type,
            title=panel.title,
            unique_id=panel.unique_id,
        )
        hass.config_entries.async_add_subentry(entry, added)
    result = await changing
    assert result["reason"] == "parent_changed"
    assert added.data == panel.data


async def test_other_panel_handoff_preserves_pending_owner(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> None:
    entry, office = _fleet(hass)
    kitchen = _subentry(
        slug="kitchen",
        subentry_id="panel-kitchen",
        public_key=_OTHER_PUBLIC_KEY,
        fingerprint=_OTHER_FINGERPRINT,
        priority=2,
    )
    hass.config_entries.async_add_subentry(entry, kitchen)
    await hass.config_entries._store.async_save(hass.config_entries._data_to_save())
    journal = ProvisioningJournal(hass)
    office_record = _baseline_record(host=_OLD_HOST)
    await _arm_soak(hass, office_record)
    other = _baseline_record(
        host=kitchen.data[CONF_HOST],
        slug="kitchen",
        public_key=_OTHER_PUBLIC_KEY,
        fingerprint=_OTHER_FINGERPRINT,
        transaction_id=uuid4(),
    )
    other = replace(
        other,
        fleet_profile=_managed_record(host=_OLD_HOST).fleet_profile,
        panel_request=replace(other.panel_request, root_password=kitchen.data[CONF_ROOT_PASSWORD]),
    )
    await journal.async_create(other)
    for phase in (
        ProvisioningPhase.ACTIVATION_PENDING,
        ProvisioningPhase.ACTIVATED,
        ProvisioningPhase.VERIFYING,
        ProvisioningPhase.PENDING_CONFIG_COMMIT,
    ):
        await journal.async_transition(other.transaction_id, phase)
    runtime = fleet_manager.FleetManager(hass, entry)
    runtime._panels[kitchen.subentry_id] = _manager(hass, entry, kitchen)
    runtime._runtime_owned_panel_ids.add(kitchen.subentry_id)
    await runtime._async_prepare_provisioning()
    assert runtime._pending_handoff is not None
    with monkeypatch.context() as fault:
        fault.setattr(
            hass.config_entries, "async_update_subentry", Mock(side_effect=OSError("test update"))
        )
        result = await _change(
            hass,
            entry,
            office,
            **{
                CONF_HOST: _NEW_HOST,
                CONF_ROOT_PASSWORD: "verified-office-login",
            },
        )
        assert result["errors"]["base"] == "journal_failed"
    assert await runtime._async_complete_pending_handoff()
    hass.data.pop(_JOURNAL_COORDINATOR_KEY)
    assert await ProvisioningJournal(hass).async_load() is None
    retained = await ProvisioningJournal(hass).async_retained(office_record.transaction_id)
    assert retained is not None
    assert (
        await fleet_manager._async_rollback_credential(hass, retained.record.panel_request) is None
    )
    assert "pending_owner" in _storage_data(hass, JOURNAL_STORAGE_KEY)
