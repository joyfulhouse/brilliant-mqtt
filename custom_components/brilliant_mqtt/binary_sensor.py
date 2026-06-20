"""Bridge health — problem ON when the panel needs attention."""

from __future__ import annotations

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
)
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from . import BrilliantMqttConfigEntry
from .entity import BrilliantPanelEntity

# Push-only entities (refreshed via the manager's dispatcher signal), so there is
# nothing to rate-limit — there are no outbound polls to serialize.
PARALLEL_UPDATES = 0


async def async_setup_entry(
    hass: HomeAssistant,
    entry: BrilliantMqttConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    async_add_entities([BridgeHealthSensor(entry)])


class BridgeHealthSensor(BrilliantPanelEntity, BinarySensorEntity):
    _attr_device_class = BinarySensorDeviceClass.PROBLEM
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_translation_key = "bridge_health"

    def __init__(self, entry: BrilliantMqttConfigEntry) -> None:
        super().__init__(entry.runtime_data)
        self._attr_unique_id = f"{entry.entry_id}_bridge_health"

    @property
    def is_on(self) -> bool:
        return self._manager.problem

    @property
    def extra_state_attributes(self) -> dict[str, str | None]:
        return {
            "reason": self._manager.problem_reason,
            "availability": self._manager.availability,
        }
