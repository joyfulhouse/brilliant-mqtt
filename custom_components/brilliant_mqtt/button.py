"""Manual repair trigger (bypasses the auto-repair cooldown)."""

from __future__ import annotations

from homeassistant.components.button import ButtonEntity
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from . import BrilliantMqttConfigEntry
from .const import DOMAIN
from .entity import BrilliantPanelEntity
from .ha_control import get_control_plane
from .scene_control import scene_control_signal

# Push-only entity (its state never polls); the press handler serializes panel SSH
# via the fleet-wide lock, so no per-platform parallel-update limit is needed.
PARALLEL_UPDATES = 0


async def async_setup_entry(
    hass: HomeAssistant,
    entry: BrilliantMqttConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    async_add_entities([RepairButton(entry), RunSelectedSceneButton(entry)])


class RepairButton(BrilliantPanelEntity, ButtonEntity):
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_translation_key = "repair_bridge"

    def __init__(self, entry: BrilliantMqttConfigEntry) -> None:
        super().__init__(entry.runtime_data)
        self._attr_unique_id = f"{entry.entry_id}_repair_bridge"

    async def async_press(self) -> None:
        await self._manager.async_repair(trigger="button")


class RunSelectedSceneButton(BrilliantPanelEntity, ButtonEntity):
    """Run the locally selected scene and wait for panel execution confirmation."""

    _attr_translation_key = "run_selected_scene"

    def __init__(self, entry: BrilliantMqttConfigEntry) -> None:
        super().__init__(entry.runtime_data)
        self._attr_unique_id = f"{entry.entry_id}_run_selected_scene"

    @property
    def available(self) -> bool:
        runtime = get_control_plane(self._manager.hass).scene_control
        return (
            runtime.scene_transport_available(self._manager.panel)
            and runtime.selected_scene(self._manager.panel) is not None
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

    async def async_press(self) -> None:
        runtime = get_control_plane(self.hass).scene_control
        scene_id = runtime.selected_scene(self._manager.panel)
        if scene_id is None:
            raise HomeAssistantError("Select a Brilliant scene before running it.")
        await self.hass.services.async_call(
            DOMAIN,
            "run_scene",
            {"panel": self._manager.panel, "scene_id": scene_id},
            blocking=True,
        )
