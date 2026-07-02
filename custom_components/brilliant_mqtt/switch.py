"""Per-panel component switches (voice satellite, Wi-Fi watchdog)."""

from __future__ import annotations

import asyncssh
from homeassistant.components.switch import SwitchEntity
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from . import BrilliantMqttConfigEntry
from .const import COMPONENT_VOICE, COMPONENT_WIFI_WATCHDOG, CONF_COMPONENTS, DOMAIN
from .entity import BrilliantPanelEntity
from .manager import _HostKeyChanged
from .panel_ops import PanelOpError

# Push-only; toggling serializes panel SSH via the fleet-wide lock in the manager.
PARALLEL_UPDATES = 0


async def async_setup_entry(
    hass: HomeAssistant,
    entry: BrilliantMqttConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    async_add_entities([VoiceSatelliteSwitch(entry), WifiWatchdogSwitch(entry)])


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


class WifiWatchdogSwitch(BrilliantPanelEntity, SwitchEntity):
    """Install/remove the on-panel Wi-Fi watchdog — mirrors the voice switch.

    Unlike voice, there is no manager-level ``async_set_*_enabled`` wrapper for this
    component, so the SSH-error-to-HomeAssistantError mapping happens here instead,
    against the generic ``async_install_component``/``async_remove_component``.
    """

    _attr_entity_category = EntityCategory.CONFIG
    _attr_translation_key = "wifi_watchdog_enabled"

    def __init__(self, entry: BrilliantMqttConfigEntry) -> None:
        super().__init__(entry.runtime_data)
        self._attr_unique_id = f"{entry.entry_id}_wifi_watchdog_enabled"

    @property
    def is_on(self) -> bool:
        components = self._manager.entry.data.get(CONF_COMPONENTS, {})
        return bool(components.get(COMPONENT_WIFI_WATCHDOG, False))

    async def async_turn_on(self, **kwargs: object) -> None:
        await self._toggle(True)

    async def async_turn_off(self, **kwargs: object) -> None:
        await self._toggle(False)

    async def _toggle(self, enabled: bool) -> None:
        try:
            if enabled:
                await self._manager.async_install_component(COMPONENT_WIFI_WATCHDOG)
            else:
                await self._manager.async_remove_component(COMPONENT_WIFI_WATCHDOG)
        except _HostKeyChanged as err:
            raise HomeAssistantError(
                translation_domain=DOMAIN, translation_key="host_key_changed"
            ) from err
        except (OSError, asyncssh.Error, PanelOpError) as err:
            raise HomeAssistantError(
                translation_domain=DOMAIN,
                translation_key="wifi_watchdog_failed",
                translation_placeholders={"error": str(err)},
            ) from err
