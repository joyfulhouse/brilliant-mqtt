"""Wake-word select for the voice satellite."""

from __future__ import annotations

from homeassistant.components.select import SelectEntity
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from . import BrilliantMqttConfigEntry
from .const import CONF_VOICE_WAKE_WORD, DEFAULT_VOICE_WAKE_WORD, VOICE_WAKE_WORDS
from .entity import BrilliantPanelEntity
from .ha_control import get_control_plane
from .scene_control import scene_control_signal

# Push-only; selecting serializes panel SSH via the fleet-wide lock in the manager.
PARALLEL_UPDATES = 0


async def async_setup_entry(
    hass: HomeAssistant,
    entry: BrilliantMqttConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    async_add_entities([WakeWordSelect(entry), SceneSelect(entry)])


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


class SceneSelect(BrilliantPanelEntity, SelectEntity):
    """Choose a stable scene ID locally while displaying the panel's friendly name."""

    _attr_translation_key = "scene"

    def __init__(self, entry: BrilliantMqttConfigEntry) -> None:
        super().__init__(entry.runtime_data)
        self._attr_unique_id = f"{entry.entry_id}_scene"

    @property
    def available(self) -> bool:
        return get_control_plane(self._manager.hass).scene_control.scene_transport_available(
            self._manager.panel
        )

    @property
    def options(self) -> list[str]:
        runtime = get_control_plane(self._manager.hass).scene_control
        return [item.display_name for item in runtime.scene_options(self._manager.panel)]

    @property
    def current_option(self) -> str | None:
        runtime = get_control_plane(self._manager.hass).scene_control
        selected = runtime.selected_scene(self._manager.panel)
        return next(
            (
                item.display_name
                for item in runtime.scene_options(self._manager.panel)
                if item.scene_id == selected
            ),
            None,
        )

    async def async_added_to_hass(self) -> None:
        await super().async_added_to_hass()
        self.async_on_remove(
            async_dispatcher_connect(
                self.hass,
                scene_control_signal(self._manager.panel),
                self._refresh,
            )
        )

    async def async_select_option(self, option: str) -> None:
        runtime = get_control_plane(self.hass).scene_control
        selected = next(
            (
                item.scene_id
                for item in runtime.scene_options(self._manager.panel)
                if item.display_name == option
            ),
            None,
        )
        if selected is None:
            raise HomeAssistantError("Scene option is not available on this Brilliant panel.")
        runtime.select_scene(self._manager.panel, selected)
