"""Brilliant MQTT panel manager — lifecycle management for on-panel agents."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import voluptuous as vol
from homeassistant.components import mqtt
from homeassistant.config_entries import ConfigEntry, ConfigEntryState
from homeassistant.const import ATTR_AREA_ID, ATTR_DEVICE_ID, ATTR_ENTITY_ID
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.exceptions import ConfigEntryNotReady, HomeAssistantError
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.service import async_extract_config_entry_ids
from homeassistant.helpers.typing import ConfigType

from .const import (
    COMPONENT_BRIDGE,
    COMPONENT_VOICE,
    CONF_COMPONENTS,
    CONF_VOICE_ENABLED,
    DOMAIN,
    PLATFORMS,
)
from .manager import PanelManager

_LOGGER = logging.getLogger(__name__)

type BrilliantMqttConfigEntry = ConfigEntry[PanelManager]

# This integration is config-entry only (it registers services in async_setup but takes
# no YAML configuration), so declare the standard config-entry-only schema for hassfest.
CONFIG_SCHEMA = cv.config_entry_only_config_schema(DOMAIN)

# Target services: HA merges entity/device/area target ids into the call data, and the
# handlers resolve config entries from any of them via async_extract_config_entry_ids.
# services.yaml targets by `entity` (hassfest), so a UI call supplies entity_id, NOT
# device_id — requiring device_id rejected those calls before they reached the handler.
_SERVICE_SCHEMA = vol.Schema(
    {
        vol.Optional(ATTR_ENTITY_ID): vol.Any(str, [str]),
        vol.Optional(ATTR_DEVICE_ID): vol.Any(str, [str]),
        vol.Optional(ATTR_AREA_ID): vol.Any(str, [str]),
    },
    extra=vol.ALLOW_EXTRA,
)


def _fleet_lock(hass: HomeAssistant) -> asyncio.Lock:
    """One SSH operation at a time across the whole fleet (15-panel OTA waves)."""
    domain_data: dict[str, Any] = hass.data.setdefault(DOMAIN, {})
    lock: asyncio.Lock = domain_data.setdefault("ssh_lock", asyncio.Lock())
    return lock


async def _async_cleanup_entry(hass: HomeAssistant, entry: BrilliantMqttConfigEntry) -> None:
    """Attempt control detach and manager shutdown as one drained cleanup unit.

    Shielding keeps caller cancellation from interrupting the cleanup task between
    the two owners. If the caller is cancelled, drain cleanup before re-raising that
    cancellation. An internally raised cancellation/error from detach still runs
    manager shutdown and then propagates as the primary cleanup failure.
    """
    from .ha_control import get_control_plane

    async def _run_cleanup() -> None:
        primary_error: BaseException | None = None
        try:
            await get_control_plane(hass).async_detach(entry.entry_id)
        except BaseException as error:
            primary_error = error

        shutdown_error: BaseException | None = None
        try:
            await entry.runtime_data.async_shutdown()
        except BaseException as error:
            shutdown_error = error

        if primary_error is not None:
            if shutdown_error is not None:
                _LOGGER.warning(
                    "Manager shutdown also failed after control detach failure (%s): %s",
                    type(shutdown_error).__name__,
                    shutdown_error,
                )
            raise primary_error
        if shutdown_error is not None:
            raise shutdown_error

    cleanup_task = hass.async_create_task(_run_cleanup())
    try:
        await asyncio.shield(cleanup_task)
    except asyncio.CancelledError:
        current_task = asyncio.current_task()
        if current_task is None or current_task.cancelling() == 0:
            # The cleanup task itself raised CancelledError; it already attempted
            # manager shutdown, so propagate that internal primary failure.
            await cleanup_task
            return

        # The caller was cancelled. Keep the cleanup task shielded from any repeated
        # cancellation request, drain it, then preserve the caller's cancellation.
        while not cleanup_task.done():
            try:
                await asyncio.shield(cleanup_task)
            except asyncio.CancelledError:
                continue
        try:
            cleanup_task.result()
        except BaseException as cleanup_error:
            _LOGGER.warning(
                "Entry cleanup failed while unload/setup was cancelled (%s): %s",
                type(cleanup_error).__name__,
                cleanup_error,
            )
        raise


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Register domain services once (entries come and go; services persist)."""

    async def _managers_for(call: ServiceCall) -> list[PanelManager]:
        managers: list[PanelManager] = []
        # In this HA version async_extract_config_entry_ids reads hass off the call.
        for entry_id in await async_extract_config_entry_ids(call):
            entry = hass.config_entries.async_get_entry(entry_id)
            if (
                entry is not None
                and entry.domain == DOMAIN
                and entry.state is ConfigEntryState.LOADED
            ):
                managers.append(entry.runtime_data)
        return managers

    async def _apply_to_all(
        call: ServiceCall, op: Callable[[PanelManager], Awaitable[None]]
    ) -> None:
        """Run op against every targeted panel, then raise ONE aggregated error.

        async_update_agent/async_uninstall raise HomeAssistantError per panel; a bare
        loop would abort on the first failure and silently skip the remaining targets
        (a fleet footgun). Each panel is already escalated individually by the manager,
        so state stays coherent — here we just make sure every target is attempted and
        surface a single error naming all that failed.
        """
        failures: list[str] = []
        for manager in await _managers_for(call):
            try:
                await op(manager)
            except HomeAssistantError as err:
                failures.append(f"{manager.panel}: {err}")
        if failures:
            raise HomeAssistantError("; ".join(failures))

    async def _repair(call: ServiceCall) -> None:
        # async_repair swallows failures and escalates, so a plain loop is correct.
        for manager in await _managers_for(call):
            await manager.async_repair(trigger="service")

    async def _redeploy(call: ServiceCall) -> None:
        await _apply_to_all(call, lambda m: m.async_update_agent())

    async def _uninstall(call: ServiceCall) -> None:
        await _apply_to_all(call, lambda m: m.async_uninstall())

    hass.services.async_register(DOMAIN, "repair", _repair, schema=_SERVICE_SCHEMA)
    hass.services.async_register(DOMAIN, "redeploy", _redeploy, schema=_SERVICE_SCHEMA)
    hass.services.async_register(DOMAIN, "uninstall", _uninstall, schema=_SERVICE_SCHEMA)
    return True


async def async_setup_entry(hass: HomeAssistant, entry: BrilliantMqttConfigEntry) -> bool:
    # Every panel surface (retained state/LWT subscriptions, command publishes) rides
    # HA's mqtt integration, so don't set up "green" against a broker that isn't up yet.
    if not await mqtt.async_wait_for_mqtt_client(hass):
        raise ConfigEntryNotReady("MQTT integration is not available")
    manager = PanelManager(hass, entry, _fleet_lock(hass))
    entry.runtime_data = manager
    from .ha_control import get_control_plane

    control_plane = get_control_plane(hass)
    try:
        await manager.async_setup()
        await control_plane.async_attach(entry)
        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    except BaseException:
        try:
            await _async_cleanup_entry(hass, entry)
        except BaseException as cleanup_error:
            _LOGGER.warning(
                "Entry cleanup failed after setup failure (%s): %s",
                type(cleanup_error).__name__,
                cleanup_error,
            )
        raise
    return True


async def async_migrate_entry(hass: HomeAssistant, entry: BrilliantMqttConfigEntry) -> bool:
    """v1 -> v2: fold the one-off voice_enabled flag into a components dict.

    Optional components not represented in v1 data are simply omitted (treated as
    not-selected), so migration never silently installs anything on an existing panel.
    """
    if entry.version > 2:
        return False  # downgrade not supported
    if entry.version == 1:
        data = dict(entry.data)
        data[CONF_COMPONENTS] = {
            COMPONENT_BRIDGE: True,
            COMPONENT_VOICE: bool(data.get(CONF_VOICE_ENABLED, False)),
        }
        hass.config_entries.async_update_entry(entry, data=data, version=2)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: BrilliantMqttConfigEntry) -> bool:
    if unloaded := await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        await _async_cleanup_entry(hass, entry)
    return unloaded


async def async_remove_entry(hass: HomeAssistant, entry: BrilliantMqttConfigEntry) -> None:
    """Delete the panel's repair issues when its config entry is removed."""
    ir.async_delete_issue(hass, DOMAIN, f"needs_attention_{entry.entry_id}")
    ir.async_delete_issue(hass, DOMAIN, f"voice_missing_{entry.entry_id}")
