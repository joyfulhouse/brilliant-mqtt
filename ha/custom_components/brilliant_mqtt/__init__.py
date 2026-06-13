"""Brilliant MQTT panel manager — lifecycle management for on-panel agents."""

from __future__ import annotations

import asyncio
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .const import DOMAIN, PLATFORMS
from .manager import PanelManager

type BrilliantMqttConfigEntry = ConfigEntry[PanelManager]


def _fleet_lock(hass: HomeAssistant) -> asyncio.Lock:
    """One SSH operation at a time across the whole fleet (15-panel OTA waves)."""
    domain_data: dict[str, Any] = hass.data.setdefault(DOMAIN, {})
    lock: asyncio.Lock = domain_data.setdefault("ssh_lock", asyncio.Lock())
    return lock


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
