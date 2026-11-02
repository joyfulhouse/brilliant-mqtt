"""Repair flows: the Retry / Dismiss menu for an interrupted agent update (#219)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from homeassistant import data_entry_flow
from homeassistant.components.repairs import RepairsFlow, RepairsFlowResult
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant

from .const import DOMAIN
from .manager import PanelManager

_INTERRUPTED_PREFIX = "interrupted_update_"


def _owner(hass: HomeAssistant, issue_id: str) -> PanelManager | None:
    """The loaded panel runtime that owns *issue_id*, if any."""
    for entry in hass.config_entries.async_entries(DOMAIN):
        if entry.state is not ConfigEntryState.LOADED:
            continue
        panels = getattr(getattr(entry, "runtime_data", None), "panels", None)
        # FleetManager.panels is a read-only Mapping view, not a dict.
        if not isinstance(panels, Mapping):
            continue
        for manager in panels.values():
            if isinstance(manager, PanelManager) and manager.interrupted_issue_id == issue_id:
                return manager
    return None


class InterruptedUpdateRepairFlow(RepairsFlow):
    """Offer a fresh update (Retry) or forgetting the interrupted one (Dismiss)."""

    def __init__(self, issue_id: str) -> None:
        self._issue_id = issue_id

    async def async_step_init(self, user_input: dict[str, Any] | None = None) -> RepairsFlowResult:
        manager = _owner(self.hass, self._issue_id)
        intent = manager.interrupted_updates.surfaced if manager is not None else None
        if manager is None or intent is None:
            return self.async_abort(reason="interrupted_update_unavailable")
        return self.async_show_menu(
            step_id="init",
            menu_options=["retry", "dismiss"],
            description_placeholders={
                "panel": manager.panel,
                "target_version": intent.target_version,
                "stage": intent.stage,
                "attempts": str(intent.attempts),
            },
        )

    async def async_step_retry(self, user_input: dict[str, Any] | None = None) -> RepairsFlowResult:
        manager = _owner(self.hass, self._issue_id)
        if manager is None:
            return self.async_abort(reason="interrupted_update_unavailable")
        # A fresh update with a fresh retry budget, started in the background; the
        # Update entity shows its progress. Every refusal keeps or drops the record
        # exactly as InterruptedUpdates.async_retry documents.
        reason = await manager.interrupted_updates.async_retry()
        if reason is not None:
            return self.async_abort(reason=reason)
        return self.async_create_entry(data={})

    async def async_step_dismiss(
        self, user_input: dict[str, Any] | None = None
    ) -> RepairsFlowResult:
        manager = _owner(self.hass, self._issue_id)
        if manager is None:
            return self.async_abort(reason="interrupted_update_unavailable")
        try:
            await manager.interrupted_updates.async_dismiss()
        except Exception:
            # HA's Store swallows write I/O errors itself, so this guard only sees
            # non-I/O failures (for example serialization). The issue is kept.
            return self.async_abort(reason="interrupted_update_clear_failed")
        return self.async_create_entry(data={})


async def async_create_fix_flow(
    hass: HomeAssistant,
    issue_id: str,
    data: dict[str, str | int | float | None] | None,
) -> RepairsFlow:
    """Create the fix flow for one of this integration's fixable issues."""
    del hass, data
    if not issue_id.startswith(_INTERRUPTED_PREFIX):
        raise data_entry_flow.UnknownHandler
    return InterruptedUpdateRepairFlow(issue_id)
