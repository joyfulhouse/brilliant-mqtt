"""A verified address change must not strand a retained baseline on the old host."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch
from uuid import UUID

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import json as json_util
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.brilliant_mqtt import fleet_manager, panel_ops
from custom_components.brilliant_mqtt.broker import BrokerKind
from custom_components.brilliant_mqtt.const import (
    CONF_HOST,
    CONF_IDENTITY_FINGERPRINT,
    CONF_PANEL,
    CONF_ROOT_PASSWORD,
    DATA_SSH_HOST_KEY,
    DOMAIN,
)
from custom_components.brilliant_mqtt.entry_data import (
    FleetConfig,
    FleetPanelStore,
    LegacyPanelStore,
)
from custom_components.brilliant_mqtt.fleet_manager import (
    _async_authorize_recorded_recovery,
    legacy_fleet_config,
)
from custom_components.brilliant_mqtt.flow import gateway as flow_gateway
from custom_components.brilliant_mqtt.manager import PanelManager
from custom_components.brilliant_mqtt.panel_provisioner import PanelProvisioningError
from custom_components.brilliant_mqtt.provisioning_journal import (
    JOURNAL_STORAGE_KEY,
    ProvisioningJournal,
    ProvisioningOperation,
    ProvisioningRecord,
    Store,
    StoredFleetProfile,
)
from custom_components.brilliant_mqtt.release_identity import RollbackBaseline
from tests.fakes import FakeShell
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
from tests.test_init import ENTRY_DATA
from tests.test_provisioning_journal import _record

_OLD_HOST = "192.0.2.10"
_NEW_HOST = "192.0.2.20"
_NEWER_HOST = "192.0.2.30"
_PANEL_PASSWORD = "office-root-password"
_OTHER_TRANSACTION = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")


class _CanaryDouble:
    def __init__(self) -> None:
        self.finalized: list[UUID] = []
        self.rolled_back: list[UUID] = []

    async def async_finalize(self, transaction_id: UUID) -> None:
        self.finalized.append(transaction_id)

    async def async_rollback(self, transaction_id: UUID) -> None:
        self.rolled_back.append(transaction_id)


@pytest.fixture
def durable_journal(
    hass: HomeAssistant,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Persist the provisioning journal the way a real Home Assistant store does."""
    hass.config.config_dir = str(tmp_path / "ha-config")

    async def write(store: Store, data: dict[str, object]) -> None:
        def persist() -> None:
            Path(store.path).parent.mkdir(parents=True, exist_ok=True)
            json_util.save_json(store.path, data, private=True, atomic_writes=True)

        await hass.async_add_executor_job(persist)

    async def remove(store: Store) -> None:
        await hass.async_add_executor_job(Path(store.path).unlink, True)

    monkeypatch.setattr(Store, "_async_write_data", write)
    monkeypatch.setattr(Store, "async_remove", remove)


def _baseline_record(
    *,
    host: str,
    slug: str = "office",
    public_key: str = _PUBLIC_KEY,
    fingerprint: str = _FINGERPRINT,
    transaction_id: UUID | None = None,
) -> Any:
    record = _record()
    if transaction_id is not None:
        record = replace(record, transaction_id=transaction_id)
    return replace(
        record,
        panel_request=replace(
            record.panel_request,
            host=host,
            slug=slug,
            public_key=public_key,
            fingerprint=fingerprint,
        ),
        prior_snapshot=replace(
            record.prior_snapshot,
            baseline=RollbackBaseline("c" * 32, "d" * 64, {}),
        ),
    )


async def _arm_soak(hass: HomeAssistant, record: Any) -> None:
    journal = ProvisioningJournal(hass)
    await journal.async_arm(record)
    await journal.async_retained_state(record.transaction_id, "soak")


def _manager(hass: HomeAssistant, entry: Any, stored: Any) -> PanelManager:
    return PanelManager(
        hass,
        FleetPanelStore(hass, entry, stored),
        FleetConfig.from_entry(entry),
        asyncio.Lock(),
    )


async def test_verified_address_change_then_finalize_uses_the_new_host(
    hass: HomeAssistant,
    durable_journal: None,
) -> None:
    """Change network address, then finalize the soak baseline that captured the old host."""
    del durable_journal
    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    stored = entry.subentries[panel.subentry_id]
    hass.config_entries.async_update_subentry(
        entry,
        stored,
        data={**stored.data, CONF_HOST: _OLD_HOST},
    )
    record = _baseline_record(host=_OLD_HOST)
    await _arm_soak(hass, record)
    active = replace(
        record,
        prior_snapshot=replace(record.prior_snapshot, baseline=None),
    )
    journal = ProvisioningJournal(hass)
    await journal.async_create(active)
    stranger = _baseline_record(
        host=_OLD_HOST,
        slug="kitchen",
        public_key=_OTHER_PUBLIC_KEY,
        fingerprint=_OTHER_FINGERPRINT,
        transaction_id=_OTHER_TRANSACTION,
    )
    await journal.async_arm(stranger)
    form = await _start_panel_reconfigure_step(hass, entry, stored, "address")
    canary = _CanaryDouble()

    with (
        patch.object(
            flow_gateway,
            "async_fetch_host_identity",
            AsyncMock(return_value=_identity()),
        ),
        patch.object(
            flow_gateway,
            "_async_inspect_candidate",
            AsyncMock(return_value=None),
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager._get_recovery_provisioner",
            return_value=canary,
        ),
    ):
        result = await hass.config_entries.subentries.async_configure(
            form["flow_id"],
            {CONF_HOST: _NEW_HOST},
        )
        assert result["type"] == "abort"
        assert result["reason"] == "reconfigure_successful"
        retained = await journal.async_retained(record.transaction_id)
        assert retained is not None
        assert retained.state == "soak"
        assert retained.record.panel_request.host == _NEW_HOST
        loaded = await journal.async_load()
        assert loaded is not None
        assert loaded.panel_request.host == _NEW_HOST
        untouched = await journal.async_retained(stranger.transaction_id)
        assert untouched is not None
        assert untouched.record.panel_request.host == _OLD_HOST
        await journal.async_complete_cleanup(record.transaction_id)
        manager = _manager(hass, entry, entry.subentries[panel.subentry_id])
        await manager.async_canary_operation("finalize", str(record.transaction_id))

    assert canary.finalized == [record.transaction_id]
    assert stored.data[CONF_HOST] == _NEW_HOST


@pytest.mark.parametrize("operation", ["finalize", "rollback"])
async def test_stranded_baseline_is_repaired_when_slug_and_identity_match(
    hass: HomeAssistant,
    durable_journal: None,
    operation: str,
) -> None:
    """The address was already saved; finalize or rollback still accepts the old host."""
    del durable_journal
    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    stored = entry.subentries[panel.subentry_id]
    hass.config_entries.async_update_subentry(
        entry,
        stored,
        data={**stored.data, CONF_HOST: _NEW_HOST},
    )
    record = _baseline_record(host=_OLD_HOST)
    await _arm_soak(hass, record)
    canary = _CanaryDouble()
    manager = _manager(hass, entry, stored)

    with patch(
        "custom_components.brilliant_mqtt.fleet_manager._get_recovery_provisioner",
        return_value=canary,
    ):
        await manager.async_canary_operation(operation, str(record.transaction_id))

    retained = await ProvisioningJournal(hass).async_retained(record.transaction_id)
    assert retained is not None
    assert retained.record.panel_request.host == _NEW_HOST
    assert retained.record.panel_request.slug == "office"
    assert retained.record.panel_request.public_key == _PUBLIC_KEY
    if operation == "finalize":
        assert canary.finalized == [record.transaction_id]
    else:
        assert canary.rolled_back == [record.transaction_id]


@pytest.mark.parametrize("mismatch", ["identity", "slug"])
async def test_canary_finalize_rejects_identity_or_slug_mismatch(
    hass: HomeAssistant,
    durable_journal: None,
    caplog: pytest.LogCaptureFixture,
    mismatch: str,
) -> None:
    del durable_journal
    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    stored = entry.subentries[panel.subentry_id]
    hass.config_entries.async_update_subentry(
        entry,
        stored,
        data={**stored.data, CONF_HOST: _NEW_HOST},
    )
    if mismatch == "identity":
        record = _baseline_record(
            host=_OLD_HOST,
            public_key=_OTHER_PUBLIC_KEY,
            fingerprint=_OTHER_FINGERPRINT,
        )
    else:
        record = _baseline_record(host=_OLD_HOST, slug="kitchen")
    await _arm_soak(hass, record)
    canary = _CanaryDouble()
    manager = _manager(hass, entry, stored)

    with (
        caplog.at_level(logging.ERROR),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager._get_recovery_provisioner",
            return_value=canary,
        ),
        pytest.raises(HomeAssistantError, match="baseline_owner_mismatch") as raised,
    ):
        await manager.async_canary_operation("finalize", str(record.transaction_id))

    assert "canary_operation_failed" not in str(raised.value)
    assert "baseline_owner_mismatch" in caplog.text
    retained = await ProvisioningJournal(hass).async_retained(record.transaction_id)
    assert retained is not None
    assert retained.record.panel_request.host == _OLD_HOST
    assert canary.finalized == []
    assert stored.data[CONF_PANEL] == "office"


def _storage_data(hass: HomeAssistant, key: str) -> dict[str, Any]:
    loaded = json.loads(Path(hass.config.path(".storage", key)).read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    data = loaded["data"]
    assert isinstance(data, dict)
    return data


def _managed_record(*, host: str, password: str = _PANEL_PASSWORD) -> Any:
    record = _baseline_record(host=host)
    return replace(
        record,
        operation=ProvisioningOperation.UPDATE,
        fleet_profile=StoredFleetProfile(
            kind=BrokerKind.EXISTING_BROKER,
            host="mqtt.iot.example",
            port=1883,
            tls_enabled=False,
        ),
        panel_request=replace(record.panel_request, host=host, root_password=password),
    )


def _legacy_runtime(
    hass: HomeAssistant,
    *,
    host: str,
    public_key: str,
) -> tuple[MockConfigEntry, PanelManager]:
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="office",
        data={**ENTRY_DATA, CONF_HOST: host, DATA_SSH_HOST_KEY: public_key},
        version=3,
    )
    entry.add_to_hass(hass)
    manager = PanelManager(
        hass,
        LegacyPanelStore(hass, entry),
        legacy_fleet_config(entry),
        asyncio.Lock(),
    )
    return entry, manager


@pytest.mark.parametrize("operation", ["finalize", "rollback"])
async def test_legacy_v3_owner_without_fingerprint_can_finalize_or_rollback(
    hass: HomeAssistant,
    durable_journal: None,
    operation: str,
) -> None:
    """A key-only version-3 entry keeps both canary actions when the host is unchanged."""
    del durable_journal
    _entry, manager = _legacy_runtime(hass, host=_OLD_HOST, public_key=_PUBLIC_KEY)
    record = _baseline_record(host=_OLD_HOST)
    await _arm_soak(hass, record)
    canary = _CanaryDouble()

    with patch(
        "custom_components.brilliant_mqtt.fleet_manager._get_recovery_provisioner",
        return_value=canary,
    ):
        await manager.async_canary_operation(operation, str(record.transaction_id))

    retained = await ProvisioningJournal(hass).async_retained(record.transaction_id)
    assert retained is not None
    assert retained.record.panel_request.host == _OLD_HOST
    assert CONF_IDENTITY_FINGERPRINT not in _entry.data
    if operation == "finalize":
        assert canary.finalized == [record.transaction_id]
    else:
        assert canary.rolled_back == [record.transaction_id]


async def test_legacy_v3_owner_rejects_a_different_host_key(
    hass: HomeAssistant,
    durable_journal: None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Missing fingerprint does not accept a different persisted host key."""
    del durable_journal
    _entry, manager = _legacy_runtime(hass, host=_OLD_HOST, public_key=_OTHER_PUBLIC_KEY)
    record = _baseline_record(host=_OLD_HOST)
    await _arm_soak(hass, record)
    canary = _CanaryDouble()

    with (
        caplog.at_level(logging.ERROR),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager._get_recovery_provisioner",
            return_value=canary,
        ),
        pytest.raises(HomeAssistantError, match="baseline_owner_mismatch") as raised,
    ):
        await manager.async_canary_operation("finalize", str(record.transaction_id))

    assert "canary_operation_failed" not in str(raised.value)
    assert "baseline_owner_mismatch" in caplog.text
    retained = await ProvisioningJournal(hass).async_retained(record.transaction_id)
    assert retained is not None
    assert retained.record.panel_request.host == _OLD_HOST
    assert canary.finalized == []


def _journal_host(payload: dict[str, Any]) -> str:
    record = payload["record"]
    assert isinstance(record, dict)
    request = record["panel_request"]
    assert isinstance(request, dict)
    host = request["host"]
    assert isinstance(host, str)
    return host


def _retained_host(payload: dict[str, Any], transaction_id: UUID) -> str:
    records = payload["records"]
    assert isinstance(records, dict)
    item = records[str(transaction_id)]
    assert isinstance(item, dict)
    record = item["record"]
    assert isinstance(record, dict)
    request = record["panel_request"]
    assert isinstance(request, dict)
    host = request["host"]
    assert isinstance(host, str)
    return host


@pytest.mark.parametrize("boundary", ["journal", "retained", "config", "cancel"])
async def test_partial_verified_adoption_recovers_on_reload(
    hass: HomeAssistant,
    durable_journal: None,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    """A crash between the journal, retained store, and config owner finishes on load."""
    del durable_journal
    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    stored = entry.subentries[panel.subentry_id]
    hass.config_entries.async_update_subentry(
        entry,
        stored,
        data={**stored.data, CONF_HOST: _OLD_HOST},
    )
    record = _managed_record(host=_OLD_HOST)
    journal = ProvisioningJournal(hass)
    await journal.async_create(record)
    form = await _start_panel_reconfigure_step(hass, entry, stored, "address")
    original_write = Store._async_write_data
    failed = {"done": False}

    async def fail_boundary(store: Store, data: dict[str, object]) -> None:
        payload = data.get("data")
        if failed["done"] or not isinstance(payload, dict):
            await original_write(store, data)
            return
        record_host = None
        raw_record = payload.get("record")
        if isinstance(raw_record, dict):
            request = raw_record.get("panel_request")
            if isinstance(request, dict) and isinstance(request.get("host"), str):
                record_host = request["host"]
        retained_moved = store.key.endswith(".retained") and _NEW_HOST in json.dumps(payload)
        journal_moved = (
            store.key == JOURNAL_STORAGE_KEY
            and "pending_owner" in payload
            and record_host == _NEW_HOST
        )
        if boundary == "journal" and journal_moved:
            failed["done"] = True
            raise OSError("injected")
        if boundary == "retained" and retained_moved:
            failed["done"] = True
            raise OSError("injected")
        await original_write(store, data)

    def fail_config(config_entry: Any, subentry: Any, **kwargs: Any) -> None:
        updated = kwargs.get("data")
        if (
            boundary in {"config", "cancel"}
            and not failed["done"]
            and isinstance(updated, dict)
            and updated.get(CONF_HOST) == _NEW_HOST
        ):
            failed["done"] = True
            if boundary == "cancel":
                raise asyncio.CancelledError
            raise OSError("injected")
        original_config(config_entry, subentry, **kwargs)

    original_config = hass.config_entries.async_update_subentry
    monkeypatch.setattr(Store, "_async_write_data", fail_boundary)
    monkeypatch.setattr(hass.config_entries, "async_update_subentry", fail_config)
    configure = hass.config_entries.subentries.async_configure(
        form["flow_id"], {CONF_HOST: _NEW_HOST}
    )
    with (
        patch.object(
            flow_gateway,
            "async_fetch_host_identity",
            AsyncMock(return_value=_identity()),
        ),
        patch.object(flow_gateway, "_async_inspect_candidate", AsyncMock(return_value=None)),
    ):
        if boundary == "cancel":
            with pytest.raises(asyncio.CancelledError):
                await configure
        else:
            result = await configure
            assert result["type"] == "form"
            assert result["errors"] == {"base": "journal_failed"}
    monkeypatch.setattr(Store, "_async_write_data", original_write)
    monkeypatch.setattr(hass.config_entries, "async_update_subentry", original_config)

    journal_payload = _storage_data(hass, JOURNAL_STORAGE_KEY)
    retained_payload = _storage_data(hass, JOURNAL_STORAGE_KEY + ".retained")
    pending = journal_payload.get("pending_owner")
    assert isinstance(pending, dict)
    assert pending["host"] == _NEW_HOST
    config_host = entry.subentries[panel.subentry_id].data[CONF_HOST]
    if boundary == "journal":
        assert _journal_host(journal_payload) == _OLD_HOST
        assert _retained_host(retained_payload, record.transaction_id) == _OLD_HOST
        assert config_host == _OLD_HOST
        await _async_authorize_recorded_recovery(
            hass, ProvisioningRecord._from_storage(journal_payload["record"])
        )
    else:
        assert _journal_host(journal_payload) == _NEW_HOST
        if boundary == "retained":
            assert _retained_host(retained_payload, record.transaction_id) == _OLD_HOST
        else:
            assert _retained_host(retained_payload, record.transaction_id) == _NEW_HOST
        assert config_host == _OLD_HOST
        with pytest.raises(PanelProvisioningError, match="provisioning_recovery_required"):
            await _async_authorize_recorded_recovery(
                hass, ProvisioningRecord._from_storage(journal_payload["record"])
            )

    healed = await ProvisioningJournal(hass).async_load()
    assert healed is not None
    assert healed.panel_request.host == _NEW_HOST
    assert healed.panel_request.root_password == _PANEL_PASSWORD
    assert _storage_data(hass, JOURNAL_STORAGE_KEY).get("pending_owner") is None
    retained = await ProvisioningJournal(hass).async_retained(record.transaction_id)
    assert retained is not None
    assert retained.record.panel_request.host == _NEW_HOST
    assert entry.subentries[panel.subentry_id].data[CONF_HOST] == _NEW_HOST
    retained_text = json.dumps(_storage_data(hass, JOURNAL_STORAGE_KEY + ".retained"))
    assert _PANEL_PASSWORD not in retained_text
    await _async_authorize_recorded_recovery(hass, healed)


async def test_stale_canary_cannot_replace_a_newer_verified_address(
    hass: HomeAssistant,
    durable_journal: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A canary that observed host B must not undo a verified move to host C."""
    del durable_journal
    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    stored = entry.subentries[panel.subentry_id]
    hass.config_entries.async_update_subentry(
        entry,
        stored,
        data={**stored.data, CONF_HOST: _NEW_HOST},
    )
    record = _baseline_record(host=_OLD_HOST)
    await _arm_soak(hass, record)
    form = await _start_panel_reconfigure_step(hass, entry, stored, "address")
    entered = asyncio.Event()
    release = asyncio.Event()
    started = False
    original = ProvisioningJournal.async_adopt_verified_host
    opened: list[tuple[str, str]] = []

    async def paused(self: ProvisioningJournal, **kwargs: Any) -> None:
        nonlocal started
        if not started:
            started = True
            entered.set()
            await release.wait()
        await original(self, **kwargs)

    def shell_factory(host: str, password: str, pin: str) -> FakeShell:
        opened.append((host, password))
        return FakeShell(pinned=pin)

    monkeypatch.setattr(ProvisioningJournal, "async_adopt_verified_host", paused)
    monkeypatch.setattr(fleet_manager, "AsyncsshShell", shell_factory)
    monkeypatch.setattr(panel_ops, "finalize_baseline", AsyncMock())
    manager = _manager(hass, entry, stored)
    canary = asyncio.create_task(
        manager.async_canary_operation("finalize", str(record.transaction_id))
    )
    await asyncio.wait_for(entered.wait(), timeout=5)
    with (
        patch.object(
            flow_gateway,
            "async_fetch_host_identity",
            AsyncMock(return_value=_identity()),
        ),
        patch.object(flow_gateway, "_async_inspect_candidate", AsyncMock(return_value=None)),
    ):
        result = await hass.config_entries.subentries.async_configure(
            form["flow_id"],
            {CONF_HOST: _NEWER_HOST},
        )
    assert result["type"] == "abort"
    assert result["reason"] == "reconfigure_successful"
    release.set()
    await canary

    owner = entry.subentries[panel.subentry_id]
    assert owner.data[CONF_HOST] == _NEWER_HOST
    assert opened == [(_NEWER_HOST, _PANEL_PASSWORD)]
    assert await ProvisioningJournal(hass).async_retained(record.transaction_id) is None


async def test_older_repair_cannot_overwrite_a_newer_credential(
    hass: HomeAssistant,
    durable_journal: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A repair waiting on the journal lock must not replace a newer password."""
    del durable_journal
    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    stored = entry.subentries[panel.subentry_id]
    hass.config_entries.async_update_subentry(
        entry,
        stored,
        data={**stored.data, CONF_HOST: _OLD_HOST},
    )
    record = _managed_record(host=_OLD_HOST)
    await ProvisioningJournal(hass).async_create(record)
    older = await _start_panel_reconfigure_step(hass, entry, stored, "repair_credentials")
    newer = await _start_panel_reconfigure_step(hass, entry, stored, "repair_credentials")
    entered = asyncio.Event()
    release = asyncio.Event()
    started = False
    original = ProvisioningJournal.async_adopt_verified_host

    async def paused(self: ProvisioningJournal, **kwargs: Any) -> None:
        nonlocal started
        if not started:
            started = True
            entered.set()
            await release.wait()
        await original(self, **kwargs)

    monkeypatch.setattr(ProvisioningJournal, "async_adopt_verified_host", paused)
    newer_password = "newer-root-password"
    older_password = "older-root-password"
    with (
        patch.object(
            flow_gateway,
            "async_fetch_host_identity",
            AsyncMock(return_value=_identity()),
        ),
        patch.object(flow_gateway, "_async_inspect_candidate", AsyncMock(return_value=None)),
    ):
        older_task = asyncio.create_task(
            hass.config_entries.subentries.async_configure(
                older["flow_id"],
                {CONF_HOST: _NEW_HOST, CONF_ROOT_PASSWORD: older_password},
            )
        )
        await asyncio.wait_for(entered.wait(), timeout=5)
        newer_result = await hass.config_entries.subentries.async_configure(
            newer["flow_id"],
            {CONF_ROOT_PASSWORD: newer_password},
        )
        release.set()
        older_result = await older_task

    assert newer_result["type"] == "abort"
    assert newer_result["reason"] == "reconfigure_successful"
    assert older_result["type"] == "abort"
    assert older_result["reason"] == "parent_changed"
    owner = entry.subentries[panel.subentry_id]
    assert owner.data[CONF_HOST] == _OLD_HOST
    assert owner.data[CONF_ROOT_PASSWORD] == newer_password
    loaded = await ProvisioningJournal(hass).async_load()
    assert loaded is not None
    assert loaded.panel_request.host == _OLD_HOST
    assert loaded.panel_request.root_password == newer_password
    retained_text = json.dumps(_storage_data(hass, JOURNAL_STORAGE_KEY + ".retained"))
    assert newer_password not in retained_text
    assert older_password not in retained_text
    await _async_authorize_recorded_recovery(hass, loaded)


async def test_combined_host_password_repair_keeps_recovery_authority(
    hass: HomeAssistant,
    durable_journal: None,
) -> None:
    """A host and password repair rotates the active journal and still authorizes."""
    del durable_journal
    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    stored = entry.subentries[panel.subentry_id]
    hass.config_entries.async_update_subentry(
        entry,
        stored,
        data={**stored.data, CONF_HOST: _OLD_HOST},
    )
    record = _managed_record(host=_OLD_HOST)
    journal = ProvisioningJournal(hass)
    await journal.async_create(record)
    form = await _start_panel_reconfigure_step(hass, entry, stored, "repair_credentials")
    new_password = "rotated-root-password"

    with (
        patch.object(
            flow_gateway,
            "async_fetch_host_identity",
            AsyncMock(return_value=_identity()),
        ),
        patch.object(flow_gateway, "_async_inspect_candidate", AsyncMock(return_value=None)),
    ):
        result = await hass.config_entries.subentries.async_configure(
            form["flow_id"],
            {CONF_HOST: _NEW_HOST, CONF_ROOT_PASSWORD: new_password},
        )

    assert result["type"] == "abort"
    assert result["reason"] == "reconfigure_successful"
    loaded = await journal.async_load()
    assert loaded is not None
    assert loaded.panel_request.host == _NEW_HOST
    assert loaded.panel_request.root_password == new_password
    owner = entry.subentries[panel.subentry_id]
    assert owner.data[CONF_HOST] == _NEW_HOST
    assert owner.data[CONF_ROOT_PASSWORD] == new_password
    retained_text = json.dumps(_storage_data(hass, JOURNAL_STORAGE_KEY + ".retained"))
    assert new_password not in retained_text
    assert _PANEL_PASSWORD not in retained_text
    retained = await journal.async_retained(record.transaction_id)
    assert retained is not None
    assert retained.record.panel_request.host == _NEW_HOST
    await _async_authorize_recorded_recovery(hass, loaded)
