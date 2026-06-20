"""Manual repair trigger (bypasses the auto-repair cooldown)."""

from __future__ import annotations

from homeassistant.components.button import ButtonEntity
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from . import BrilliantMqttConfigEntry
from .entity import BrilliantPanelEntity

# Push-only entity (its state never polls); the press handler serializes panel SSH
# via the fleet-wide lock, so no per-platform parallel-update limit is needed.
PARALLEL_UPDATES = 0


async def async_setup_entry(
    hass: HomeAssistant,
    entry: BrilliantMqttConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    async_add_entities([RepairButton(entry)])


class RepairButton(BrilliantPanelEntity, ButtonEntity):
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_translation_key = "repair_bridge"

    def __init__(self, entry: BrilliantMqttConfigEntry) -> None:
        super().__init__(entry.runtime_data)
        self._attr_unique_id = f"{entry.entry_id}_repair_bridge"

    async def async_press(self) -> None:
        await self._manager.async_repair(trigger="button")
