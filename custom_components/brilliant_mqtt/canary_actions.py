"""Manual retained-baseline actions at a configured panel's current address."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from homeassistant.config_entries import ConfigEntry, ConfigSubentry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import issue_registry as ir

from .const import (
    CONF_HOST,
    CONF_IDENTITY_FINGERPRINT,
    CONF_PANEL,
    CONF_ROOT_PASSWORD,
    CONF_SSH_HOST_KEY,
    DOMAIN,
)
from .entry_data import FleetPanelStore, PanelConfigStore
from .panel_provisioner import PanelProvisioningError
from .provisioning_journal import ProvisioningJournal, RetainedRollback
from .shell import HostIdentity, PanelIdentityError


def _pin(data: Mapping[str, Any], *, fingerprint: str | None = None) -> HostIdentity | None:
    try:
        return HostIdentity(data[CONF_SSH_HOST_KEY], fingerprint or data[CONF_IDENTITY_FINGERPRINT])
    except (KeyError, PanelIdentityError, TypeError, ValueError):
        return None


def _candidates(hass: HomeAssistant, retained: RetainedRollback) -> list[tuple[Any, Any]]:
    request = retained.record.panel_request
    identity = HostIdentity(request.public_key, request.fingerprint)
    result: list[tuple[Any, Any]] = []
    for entry in hass.config_entries.async_entries(DOMAIN):
        for owner in entry.subentries.values():
            if (
                owner.data.get(CONF_PANEL) == request.slug
                or owner.data.get(CONF_IDENTITY_FINGERPRINT) == request.fingerprint
                or _pin(owner.data, fingerprint=request.fingerprint) == identity
            ):
                result.append((entry, owner))
        if not entry.subentries and (
            entry.data.get(CONF_PANEL) == request.slug
            or entry.data.get(CONF_IDENTITY_FINGERPRINT) == request.fingerprint
            or _pin(entry.data, fingerprint=request.fingerprint) == identity
        ):
            result.append((entry, None))
    return result


@dataclass(frozen=True, slots=True, repr=False)
class _CurrentOwner:
    hass: HomeAssistant
    entry: ConfigEntry[Any]
    panel: ConfigSubentry
    data: dict[str, Any]

    def check(self, retained: RetainedRollback) -> None:
        """Run after the final await: HA ownership can change outside the action lock."""
        matches = _candidates(self.hass, retained)
        request = retained.record.panel_request
        if (
            len(matches) != 1
            or matches[0][0] is not self.entry
            or matches[0][1] is not self.panel
            or self.hass.config_entries.async_get_entry(self.entry.entry_id) is not self.entry
            or self.entry.subentries.get(self.panel.subentry_id) is not self.panel
            or dict(self.panel.data) != self.data
            or self.data.get(CONF_PANEL) != request.slug
            or _pin(self.data) != HostIdentity(request.public_key, request.fingerprint)
            or not isinstance(self.data.get(CONF_HOST), str)
            or not isinstance(self.data.get(CONF_ROOT_PASSWORD), str)
        ):
            raise PanelProvisioningError("canary_owner_changed")


def address_issue_id(retained: RetainedRollback) -> str:
    return f"canary_new_address_{retained.record.transaction_id.hex}"


@callback
def async_defer_address_action(
    hass: HomeAssistant, retained: RetainedRollback, *, required: bool = False
) -> bool:
    """Publish a repair instead of replaying old-host intent after an address edit."""
    matches = _candidates(hass, retained)
    moved = [
        (entry, owner)
        for entry, owner in matches
        if owner is not None and owner.data.get(CONF_HOST) != retained.record.panel_request.host
    ]
    if not moved and not required:
        return False
    entry, owner = moved[0] if moved else (matches[0] if len(matches) == 1 else (None, None))
    ir.async_create_issue(
        hass,
        DOMAIN,
        address_issue_id(retained),
        is_fixable=True,
        severity=ir.IssueSeverity.WARNING,
        translation_key="canary_new_address",
        translation_placeholders={"panel": retained.record.panel_request.slug},
        data={
            "entry_id": entry.entry_id if entry is not None else "",
            "subentry_id": owner.subentry_id if owner is not None else "",
            "transaction_id": str(retained.record.transaction_id),
        },
    )
    from .fleet_manager import _provisioning_repair_issue_id

    ir.async_delete_issue(
        hass, DOMAIN, _provisioning_repair_issue_id(retained.record.transaction_id)
    )
    return True


async def async_run_canary(
    hass: HomeAssistant, store: PanelConfigStore, operation: str, transaction: UUID
) -> None:
    """Use one ephemeral current-address grant; never rewrite stored ownership."""
    from .fleet_manager import _get_recovery_provisioner

    # Capture the owner before the first storage await, including removal/recreation.
    owner = (
        _CurrentOwner(hass, store.entry, store.subentry, deepcopy(dict(store.data)))
        if isinstance(store, FleetPanelStore)
        else None
    )
    retained = await ProvisioningJournal(hass).async_retained(transaction)
    if retained is None:
        raise PanelProvisioningError("baseline_incomplete")
    request = retained.record.panel_request
    if operation not in {"rollback", "finalize"}:
        raise PanelProvisioningError("canary_action_unavailable")
    target = owner.data if owner is not None else store.data
    if request.host != target.get(CONF_HOST):
        async_defer_address_action(hass, retained, required=True)
        if owner is None:
            raise PanelProvisioningError("canary_owner_changed")
        owner.check(retained)
        provisioner = _get_recovery_provisioner(
            hass, expected_identity=HostIdentity(request.public_key, request.fingerprint)
        )
        await provisioner.async_canary_at_address(
            operation,
            retained,
            owner.data[CONF_HOST],
            owner.data[CONF_ROOT_PASSWORD],
            lambda: owner.check(retained),
        )
        if await ProvisioningJournal(hass).async_retained(transaction) is None:
            ir.async_delete_issue(hass, DOMAIN, address_issue_id(retained))
        return
    if owner is not None:
        owner.check(retained)
    if request.slug != store.data.get(CONF_PANEL) or request.public_key != store.data.get(
        CONF_SSH_HOST_KEY
    ):
        raise PanelProvisioningError("canary_owner_changed")
    provisioner = _get_recovery_provisioner(hass)
    if operation == "rollback":
        await provisioner.async_rollback(transaction)
    else:
        await provisioner.async_finalize(transaction)
