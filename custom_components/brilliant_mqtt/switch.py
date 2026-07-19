"""Per-panel component switches (voice satellite, Wi-Fi watchdog, bus watchdog)."""

from __future__ import annotations

import asyncssh
from homeassistant.components.switch import SwitchEntity
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from . import BrilliantMqttConfigEntry
from .const import (
    COMPONENT_BUS_WATCHDOG,
    COMPONENT_HA_MIRROR,
    COMPONENT_HUE_CA,
    COMPONENT_VOICE,
    COMPONENT_WIFI_WATCHDOG,
    CONF_COMPONENTS,
    DOMAIN,
)
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
    async_add_entities(
        [
            VoiceSatelliteSwitch(entry),
            WifiWatchdogSwitch(entry),
            BusWatchdogSwitch(entry),
            HueCaSwitch(entry),
        ]
    )


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


class _ComponentInstallSwitch(BrilliantPanelEntity, SwitchEntity):
    """Install/remove an on-panel component through the manager registry.

    These components have no manager-level ``async_set_*_enabled`` wrappers, so
    this switch maps SSH failures to ``HomeAssistantError`` in ``_toggle``. Voice
    keeps that mapping in its manager wrapper instead.
    """

    _attr_entity_category = EntityCategory.CONFIG
    _component_id: str
    _unique_id_suffix: str
    _failure_translation_key: str

    def __init__(self, entry: BrilliantMqttConfigEntry) -> None:
        super().__init__(entry.runtime_data)
        self._attr_unique_id = f"{entry.entry_id}_{self._unique_id_suffix}"

    @property
    def is_on(self) -> bool:
        components = self._manager.entry.data.get(CONF_COMPONENTS, {})
        return bool(components.get(self._component_id, False))

    async def async_turn_on(self, **kwargs: object) -> None:
        await self._toggle(True)

    async def async_turn_off(self, **kwargs: object) -> None:
        await self._toggle(False)

    async def _toggle(self, enabled: bool) -> None:
        try:
            if enabled:
                await self._manager.async_install_component(self._component_id)
            else:
                await self._manager.async_remove_component(self._component_id)
        except _HostKeyChanged as err:
            raise HomeAssistantError(
                translation_domain=DOMAIN, translation_key="host_key_changed"
            ) from err
        except (OSError, asyncssh.Error, PanelOpError) as err:
            raise HomeAssistantError(
                translation_domain=DOMAIN,
                translation_key=self._failure_translation_key,
                translation_placeholders={"error": str(err)},
            ) from err


class WifiWatchdogSwitch(_ComponentInstallSwitch):
    """Install/remove the on-panel Wi-Fi watchdog."""

    _attr_translation_key = "wifi_watchdog_enabled"
    _component_id = COMPONENT_WIFI_WATCHDOG
    _unique_id_suffix = "wifi_watchdog_enabled"
    _failure_translation_key = "wifi_watchdog_failed"


class BusWatchdogSwitch(_ComponentInstallSwitch):
    """Reboots the panel if the Brilliant bus stays wedged for 30+ minutes."""

    _attr_translation_key = "bus_watchdog_enabled"
    _component_id = COMPONENT_BUS_WATCHDOG
    _unique_id_suffix = "bus_watchdog_enabled"
    _failure_translation_key = "bus_watchdog_failed"


class HueCaSwitch(_ComponentInstallSwitch):
    """Install/remove the on-panel diyHue CA recovery hook."""

    _attr_translation_key = "hue_ca_enabled"
    _component_id = COMPONENT_HUE_CA
    _unique_id_suffix = "hue_ca_enabled"
    _failure_translation_key = "hue_ca_failed"


class HaMirrorSwitch(_ComponentInstallSwitch):
    """Legacy entity retained for compatibility but no longer registered."""

    _attr_translation_key = "ha_mirror"
    _component_id = COMPONENT_HA_MIRROR
    _unique_id_suffix = "ha_mirror_enabled"
    _failure_translation_key = "ha_mirror_failed"
