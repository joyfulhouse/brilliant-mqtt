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
            HaMirrorSwitch(entry),
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
    """Install/remove an on-panel component through the manager registry."""

    _attr_entity_category = EntityCategory.CONFIG

    def __init__(
        self,
        entry: BrilliantMqttConfigEntry,
        *,
        component_id: str,
        unique_id_suffix: str,
        failure_translation_key: str,
    ) -> None:
        super().__init__(entry.runtime_data)
        self._component_id = component_id
        self._failure_translation_key = failure_translation_key
        self._attr_unique_id = f"{entry.entry_id}_{unique_id_suffix}"

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

    def __init__(self, entry: BrilliantMqttConfigEntry) -> None:
        super().__init__(
            entry,
            component_id=COMPONENT_WIFI_WATCHDOG,
            unique_id_suffix="wifi_watchdog_enabled",
            failure_translation_key="wifi_watchdog_failed",
        )


class BusWatchdogSwitch(_ComponentInstallSwitch):
    """Install/remove the on-panel bus watchdog — mirrors the Wi-Fi watchdog switch.

    Reboots the panel if the Brilliant bus stays wedged for 30+ minutes. Like the
    Wi-Fi watchdog, there is no manager-level ``async_set_*_enabled`` wrapper for this
    component, so the SSH-error-to-HomeAssistantError mapping happens here instead,
    against the generic ``async_install_component``/``async_remove_component``.
    """

    _attr_translation_key = "bus_watchdog_enabled"

    def __init__(self, entry: BrilliantMqttConfigEntry) -> None:
        super().__init__(
            entry,
            component_id=COMPONENT_BUS_WATCHDOG,
            unique_id_suffix="bus_watchdog_enabled",
            failure_translation_key="bus_watchdog_failed",
        )


class HaMirrorSwitch(_ComponentInstallSwitch):
    """Install/remove the on-panel Home Assistant entity mirror."""

    _attr_translation_key = "ha_mirror"

    def __init__(self, entry: BrilliantMqttConfigEntry) -> None:
        super().__init__(
            entry,
            component_id=COMPONENT_HA_MIRROR,
            unique_id_suffix="ha_mirror_enabled",
            failure_translation_key="ha_mirror_failed",
        )
