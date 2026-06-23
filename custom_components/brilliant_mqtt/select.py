"""Wake-word select for the voice satellite."""

from __future__ import annotations

from homeassistant.components.select import SelectEntity
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from . import BrilliantMqttConfigEntry
from .const import CONF_VOICE_WAKE_WORD, DEFAULT_VOICE_WAKE_WORD, VOICE_WAKE_WORDS
from .entity import BrilliantPanelEntity

# Push-only; selecting serializes panel SSH via the fleet-wide lock in the manager.
PARALLEL_UPDATES = 0


async def async_setup_entry(
    hass: HomeAssistant,
    entry: BrilliantMqttConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    async_add_entities([WakeWordSelect(entry)])


class WakeWordSelect(BrilliantPanelEntity, SelectEntity):
    _attr_entity_category = EntityCategory.CONFIG
    _attr_translation_key = "voice_wake_word"
    _attr_options = list(VOICE_WAKE_WORDS)

    def __init__(self, entry: BrilliantMqttConfigEntry) -> None:
        super().__init__(entry.runtime_data)
        self._attr_unique_id = f"{entry.entry_id}_voice_wake_word"

    @property
    def current_option(self) -> str:
        return str(self._manager.entry.data.get(CONF_VOICE_WAKE_WORD, DEFAULT_VOICE_WAKE_WORD))

    async def async_select_option(self, option: str) -> None:
        await self._manager.async_set_voice_wake_word(option)
