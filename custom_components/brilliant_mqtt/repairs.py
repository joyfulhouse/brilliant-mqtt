"""Explicit choices for retained baselines whose panel address has changed."""

from __future__ import annotations

from typing import Any
from uuid import UUID

import voluptuous as vol
from homeassistant.components.repairs import RepairsFlow
from homeassistant.components.repairs.models import RepairsFlowResult
from homeassistant.core import HomeAssistant

from .canary_actions import async_run_canary
from .entry_data import FleetPanelStore
from .panel_provisioner import PanelProvisioningError


class CanaryAddressRepairFlow(RepairsFlow):
    """A repair grants only the action explicitly chosen in this form."""

    def __init__(self, data: dict[str, str | int | float | None] | None) -> None:
        self.data = data

    async def async_step_init(self, user_input: dict[str, Any] | None = None) -> RepairsFlowResult:
        errors: dict[str, str] = {}
        code = ""
        if user_input is not None:
            try:
                data = self.data or {}
                entry = self.hass.config_entries.async_get_entry(str(data.get("entry_id", "")))
                panel = entry.subentries.get(str(data.get("subentry_id", ""))) if entry else None
                if entry is None or panel is None:
                    raise PanelProvisioningError("canary_owner_changed")
                transaction = UUID(str(data.get("transaction_id", "")))
                operation = str(user_input.get("action", ""))
                await async_run_canary(
                    self.hass, FleetPanelStore(self.hass, entry, panel), operation, transaction
                )
            except PanelProvisioningError as error:
                errors["base"] = "action_refused"
                code = error.code
            except Exception:
                errors["base"] = "action_refused"
                code = "recovery_failed"
            else:
                if operation == "rollback":
                    # ABORT keeps the issue: the verified retained baseline still exists.
                    return self.async_abort(reason="rollback_complete")
                return self.async_create_entry(data={})
        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema({vol.Required("action"): vol.In(["finalize", "rollback"])}),
            errors=errors,
            description_placeholders={"code": code},
        )


async def async_create_fix_flow(
    hass: HomeAssistant,
    issue_id: str,
    data: dict[str, str | int | float | None] | None,
) -> CanaryAddressRepairFlow:
    """Build the manual repair without contacting a panel."""
    del hass, issue_id
    return CanaryAddressRepairFlow(data)
