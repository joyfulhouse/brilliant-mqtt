"""Brilliant MQTT panel manager — lifecycle management for on-panel agents."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import voluptuous as vol
from homeassistant.components import mqtt
from homeassistant.config_entries import ConfigEntry, ConfigEntryState
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.exceptions import ConfigEntryNotReady, HomeAssistantError
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.service import async_extract_config_entry_ids
from homeassistant.helpers.typing import ConfigType

from .const import DOMAIN, PLATFORMS
from .manager import PanelManager

type BrilliantMqttConfigEntry = ConfigEntry[PanelManager]

# This integration is config-entry only (it registers services in async_setup but takes
# no YAML configuration), so declare the standard config-entry-only schema for hassfest.
CONFIG_SCHEMA = cv.config_entry_only_config_schema(DOMAIN)

_SERVICE_SCHEMA = vol.Schema(
    {vol.Required("device_id"): vol.Any(str, [str])}, extra=vol.ALLOW_EXTRA
)


def _fleet_lock(hass: HomeAssistant) -> asyncio.Lock:
    """One SSH operation at a time across the whole fleet (15-panel OTA waves)."""
    domain_data: dict[str, Any] = hass.data.setdefault(DOMAIN, {})
    lock: asyncio.Lock = domain_data.setdefault("ssh_lock", asyncio.Lock())
    return lock


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Register domain services once (entries come and go; services persist)."""

    async def _managers_for(call: ServiceCall) -> list[PanelManager]:
        managers: list[PanelManager] = []
        # In this HA version async_extract_config_entry_ids reads hass off the call.
        for entry_id in await async_extract_config_entry_ids(call):
            entry = hass.config_entries.async_get_entry(entry_id)
            if (
                entry is not None
                and entry.domain == DOMAIN
                and entry.state is ConfigEntryState.LOADED
            ):
                managers.append(entry.runtime_data)
        return managers

    async def _apply_to_all(
        call: ServiceCall, op: Callable[[PanelManager], Awaitable[None]]
    ) -> None:
        """Run op against every targeted panel, then raise ONE aggregated error.

        async_update_agent/async_uninstall raise HomeAssistantError per panel; a bare
        loop would abort on the first failure and silently skip the remaining targets
        (a fleet footgun). Each panel is already escalated individually by the manager,
        so state stays coherent — here we just make sure every target is attempted and
        surface a single error naming all that failed.
        """
        failures: list[str] = []
        for manager in await _managers_for(call):
            try:
                await op(manager)
            except HomeAssistantError as err:
                failures.append(f"{manager.panel}: {err}")
        if failures:
            raise HomeAssistantError("; ".join(failures))

    async def _repair(call: ServiceCall) -> None:
        # async_repair swallows failures and escalates, so a plain loop is correct.
        for manager in await _managers_for(call):
            await manager.async_repair(trigger="service")

    async def _redeploy(call: ServiceCall) -> None:
        await _apply_to_all(call, lambda m: m.async_update_agent())

    async def _uninstall(call: ServiceCall) -> None:
        await _apply_to_all(call, lambda m: m.async_uninstall())

    hass.services.async_register(DOMAIN, "repair", _repair, schema=_SERVICE_SCHEMA)
    hass.services.async_register(DOMAIN, "redeploy", _redeploy, schema=_SERVICE_SCHEMA)
    hass.services.async_register(DOMAIN, "uninstall", _uninstall, schema=_SERVICE_SCHEMA)
    return True


async def async_setup_entry(hass: HomeAssistant, entry: BrilliantMqttConfigEntry) -> bool:
    # Every panel surface (retained state/LWT subscriptions, command publishes) rides
    # HA's mqtt integration, so don't set up "green" against a broker that isn't up yet.
    if not await mqtt.async_wait_for_mqtt_client(hass):
        raise ConfigEntryNotReady("MQTT integration is not available")
    manager = PanelManager(hass, entry, _fleet_lock(hass))
    entry.runtime_data = manager
    await manager.async_setup()
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: BrilliantMqttConfigEntry) -> bool:
    if unloaded := await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        await entry.runtime_data.async_shutdown()
    return unloaded


async def async_remove_entry(hass: HomeAssistant, entry: BrilliantMqttConfigEntry) -> None:
    """Delete the panel's repair issue when its config entry is removed."""
    ir.async_delete_issue(hass, DOMAIN, f"needs_attention_{entry.entry_id}")
