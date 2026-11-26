"""Crash and concurrency boundaries of a verified panel owner change."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from homeassistant.const import CONF_NAME
from homeassistant.core import HomeAssistant

from custom_components.brilliant_mqtt import fleet_manager, panel_ops
from custom_components.brilliant_mqtt.const import CONF_HOST, CONF_ROOT_PASSWORD
from custom_components.brilliant_mqtt.flow import gateway
from custom_components.brilliant_mqtt.provisioning_journal import (
    JOURNAL_STORAGE_KEY,
    ProvisioningJournal,
    ProvisioningJournalError,
)
from tests.fakes import FakeShell
from tests.test_address_baseline_host import (
    _NEW_HOST,
    _OLD_HOST,
    _arm_soak,
    _baseline_record,
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
    _start_panel_reconfigure_step,
    _subentry,
)

pytestmark = pytest.mark.usefixtures("durable_journal")


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

    # A later load retries the durable handoff rather than losing the intent.
    await core_store.async_save(hass.config_entries._data_to_save())
    healed = await ProvisioningJournal(hass).async_load()
    assert healed is not None
    await fleet_manager._async_authorize_recorded_recovery(hass, healed)
    assert "pending_owner" not in _storage_data(hass, JOURNAL_STORAGE_KEY)


async def test_cancelled_disk_proof_preserves_replayable_intent(
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
    await fleet_manager._async_authorize_recorded_recovery(hass, record)
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
        provisioner.async_rollback.assert_awaited_once_with(other.transaction_id)
    else:
        await ProvisioningJournal(hass).async_adopt_verified_host(
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
