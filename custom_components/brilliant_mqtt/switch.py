"""Voice satellite enable/disable switch."""

from __future__ import annotations

from homeassistant.components.switch import SwitchEntity
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from . import BrilliantMqttConfigEntry
from .const import COMPONENT_VOICE, CONF_COMPONENTS
from .entity import BrilliantPanelEntity

# Push-only; toggling serializes panel SSH via the fleet-wide lock in the manager.
PARALLEL_UPDATES = 0


async def async_setup_entry(
    hass: HomeAssistant,
    entry: BrilliantMqttConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    async_add_entities([VoiceSatelliteSwitch(entry)])


class VoiceSatelliteSwitch(BrilliantPanelEntity, SwitchEntity):
    _attr_entity_category = EntityCategory.CONFIG
    _attr_translation_key = "voice_enabled"

    def __init__(self, entry: BrilliantMqttConfigEntry) -> None:
        super().__init__(entry.runtime_data)
        self._attr_unique_id = f"{entry.entry_id}_voice_enabled"

    @property
    def is_on(self) -> bool:
        components = self._manager.entry.data.get(CONF_COMPONENTS, {})
        return bool(components.get(COMPONENT_VOICE, False))

    async def async_turn_on(self, **kwargs: object) -> None:
        await self._manager.async_set_voice_enabled(True)

    async def async_turn_off(self, **kwargs: object) -> None:
        await self._manager.async_set_voice_enabled(False)
