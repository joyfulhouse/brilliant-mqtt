"""Agent update entity — installed from the bridge meta topic, latest from the payload."""

from __future__ import annotations

from typing import Any

from homeassistant.components.update import UpdateEntity, UpdateEntityFeature
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from . import BrilliantMqttConfigEntry, manager
from .entity import BrilliantPanelEntity


async def async_setup_entry(
    hass: HomeAssistant,
    entry: BrilliantMqttConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    # Late binding: resolve manager._payload_dir at call time so a test (or a future
    # caller) that patches it on the manager module is honored — a module-level
    # `from .manager import _payload_dir` would freeze the original in this namespace.
    latest = await hass.async_add_executor_job(
        lambda: (manager._payload_dir() / "VERSION").read_text().strip()
    )
    async_add_entities([AgentUpdate(entry, latest)])


class AgentUpdate(BrilliantPanelEntity, UpdateEntity):
    _attr_supported_features = UpdateEntityFeature.INSTALL
    _attr_name = "Bridge"
    _attr_title = "brilliant-mqtt agent"

    def __init__(self, entry: BrilliantMqttConfigEntry, latest: str) -> None:
        super().__init__(entry.runtime_data)
        self._attr_unique_id = f"{entry.entry_id}_agent_update"
        self._attr_latest_version = latest

    @property
    def installed_version(self) -> str | None:
        meta = self._manager.meta
        return meta.get("agent_version") if meta else None

    async def async_install(self, version: str | None, backup: bool, **kwargs: Any) -> None:
        # Engage HA's re-entrancy guard and show a spinner on the card for the duration
        # of the deploy; async_update_agent re-raises on failure so HA surfaces it.
        self._attr_in_progress = True
        self.async_write_ha_state()
        try:
            await self._manager.async_update_agent()
        finally:
            self._attr_in_progress = False
            self.async_write_ha_state()
