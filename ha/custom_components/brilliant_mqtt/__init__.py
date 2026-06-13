"""Brilliant MQTT panel manager — lifecycle management for on-panel agents."""

from __future__ import annotations

import asyncio
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import ConfigEntry, ConfigEntryState
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers.service import async_extract_config_entry_ids
from homeassistant.helpers.typing import ConfigType

from .const import DOMAIN, PLATFORMS
from .manager import PanelManager

type BrilliantMqttConfigEntry = ConfigEntry[PanelManager]

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

    async def _repair(call: ServiceCall) -> None:
        for manager in await _managers_for(call):
            await manager.async_repair(trigger="service")

    async def _redeploy(call: ServiceCall) -> None:
        for manager in await _managers_for(call):
            await manager.async_update_agent()

    async def _uninstall(call: ServiceCall) -> None:
        for manager in await _managers_for(call):
            await manager.async_uninstall()

    hass.services.async_register(DOMAIN, "repair", _repair, schema=_SERVICE_SCHEMA)
    hass.services.async_register(DOMAIN, "redeploy", _redeploy, schema=_SERVICE_SCHEMA)
    hass.services.async_register(DOMAIN, "uninstall", _uninstall, schema=_SERVICE_SCHEMA)
    return True


async def async_setup_entry(hass: HomeAssistant, entry: BrilliantMqttConfigEntry) -> bool:
    manager = PanelManager(hass, entry, _fleet_lock(hass))
    entry.runtime_data = manager
    await manager.async_setup()
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: BrilliantMqttConfigEntry) -> bool:
    if unloaded := await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        await entry.runtime_data.async_shutdown()
    return unloaded
