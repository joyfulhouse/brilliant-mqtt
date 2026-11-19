"""A verified address change must not strand a retained baseline on the old host."""

from __future__ import annotations

import asyncio
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

from custom_components.brilliant_mqtt.const import CONF_HOST, CONF_PANEL
from custom_components.brilliant_mqtt.entry_data import FleetConfig, FleetPanelStore
from custom_components.brilliant_mqtt.flow import gateway as flow_gateway
from custom_components.brilliant_mqtt.manager import PanelManager
from custom_components.brilliant_mqtt.provisioning_journal import ProvisioningJournal, Store
from custom_components.brilliant_mqtt.release_identity import RollbackBaseline
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
from tests.test_provisioning_journal import _record

_OLD_HOST = "192.0.2.10"
_NEW_HOST = "192.0.2.20"
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
