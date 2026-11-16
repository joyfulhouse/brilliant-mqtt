"""Brilliant MQTT panel manager — lifecycle management for on-panel agents."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any, cast

import voluptuous as vol
from homeassistant.components import mqtt
from homeassistant.config_entries import ConfigEntry, ConfigEntryState
from homeassistant.const import (
    ATTR_AREA_ID,
    ATTR_DEVICE_ID,
    ATTR_ENTITY_ID,
    ATTR_FLOOR_ID,
    ATTR_LABEL_ID,
)
from homeassistant.core import HomeAssistant, ServiceCall, ServiceResponse, SupportsResponse
from homeassistant.exceptions import ConfigEntryNotReady, HomeAssistantError
from homeassistant.helpers import (
    config_validation as cv,
)
from homeassistant.helpers import (
    device_registry as dr,
)
from homeassistant.helpers import (
    entity_registry as er,
)
from homeassistant.helpers import (
    issue_registry as ir,
)
from homeassistant.helpers import target as target_helpers
from homeassistant.helpers.typing import ConfigType

from .const import (
    COMPONENT_BRIDGE,
    COMPONENT_HA_MIRROR,
    COMPONENT_VOICE,
    CONF_COMPONENTS,
    CONF_ENTRY_KIND,
    CONF_HA_CONTROL_DOMAINS,
    CONF_HA_CONTROL_ENABLED,
    CONF_HA_CONTROL_LABEL,
    CONF_HA_MIRROR_LABEL,
    CONF_MANAGEMENT_ID,
    CONF_MAX_MIRRORED_ENTITIES,
    CONF_PANEL,
    CONF_ROOM_OVERRIDES,
    CONF_SCENE_ACTIONS,
    CONF_SCENE_PANEL,
    CONF_VOICE_ENABLED,
    CONFIG_ENTRY_VERSION,
    DEFAULT_HA_CONTROL_DOMAINS,
    DEFAULT_HA_CONTROL_ENABLED,
    DEFAULT_HA_CONTROL_LABEL,
    DEFAULT_MAX_MIRRORED_ENTITIES,
    DEFAULT_REBOOT_JOURNAL_LINES,
    DOMAIN,
    ENTRY_KIND_LEGACY_PENDING_CONSOLIDATION,
    MAX_REBOOT_JOURNAL_LINES,
    MIN_REBOOT_JOURNAL_LINES,
    PLATFORMS,
)
from .fleet_manager import (
    FleetManager,
    async_recover_removed_entry,
)
from .fleet_manager import (
    get_panel_provisioner as get_panel_provisioner,
)
from .manager import PanelManager, async_delete_panel_issues
from .migration_coordinator import (
    ISSUE_MIGRATION_AVAILABLE,
    MigrationCoordinator,
    async_clear_reverted_issue,
    async_load_quiet_obligations,
    async_quiet_setup_succeeded,
    async_report_recovery_failure,
    async_update_migration_issue,
    migration_fence,
    migration_removing,
)

_LOGGER = logging.getLogger(__name__)

type BrilliantMqttConfigEntry = ConfigEntry[FleetManager]

# This integration is config-entry only (it registers services in async_setup but takes
# no YAML configuration), so declare the standard config-entry-only schema for hassfest.
CONFIG_SCHEMA = cv.config_entry_only_config_schema(DOMAIN)

# Target services: HA merges entity/device/area target ids into the call data, and the
# handler resolves their exact config-entry/subentry ownership through the registries.
# services.yaml targets by `entity` (hassfest), so a UI call supplies entity_id, not
# necessarily device_id.
_SERVICE_SCHEMA = vol.Schema(
    {
        vol.Optional(ATTR_ENTITY_ID): vol.Any(None, str, [str]),
        vol.Optional(ATTR_DEVICE_ID): vol.Any(None, str, [str]),
        vol.Optional(ATTR_AREA_ID): vol.Any(None, str, [str]),
    },
    extra=vol.ALLOW_EXTRA,
)

_RUN_SCENE_SCHEMA = vol.Schema(
    {
        vol.Optional("panel"): cv.string,
        vol.Required("scene_id"): cv.string,
    },
    extra=vol.PREVENT_EXTRA,
)

_SET_MODE_SCHEMA = vol.Schema(
    {
        vol.Optional("panel"): cv.string,
        vol.Required("mode_id"): cv.string,
    },
    extra=vol.PREVENT_EXTRA,
)

# Like _SERVICE_SCHEMA (entity/device/area target) plus the two reboot options. HA
# merges the target ids into the call data; the handler resolves managers from them.
_REBOOT_SCHEMA = vol.Schema(
    {
        vol.Optional(ATTR_ENTITY_ID): vol.Any(None, str, [str]),
        vol.Optional(ATTR_DEVICE_ID): vol.Any(None, str, [str]),
        vol.Optional(ATTR_AREA_ID): vol.Any(None, str, [str]),
        vol.Optional("collect_diagnostics", default=True): cv.boolean,
        vol.Optional("journal_lines", default=DEFAULT_REBOOT_JOURNAL_LINES): vol.All(
            vol.Coerce(int),
            vol.Range(min=MIN_REBOOT_JOURNAL_LINES, max=MAX_REBOOT_JOURNAL_LINES),
        ),
    },
    extra=vol.ALLOW_EXTRA,
)

# Operator-triggered legacy-to-fleet consolidation. Dry run (the default) only
# reports the plan or its conflicts; it never mutates anything.
_MIGRATE_SCHEMA = vol.Schema(
    {vol.Optional("dry_run", default=True): cv.boolean},
    extra=vol.PREVENT_EXTRA,
)

_TARGET_KEYS = frozenset(
    {
        ATTR_ENTITY_ID,
        ATTR_DEVICE_ID,
        ATTR_AREA_ID,
        ATTR_FLOOR_ID,
        ATTR_LABEL_ID,
    }
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
                    "Manager shutdown also failed after control detach failure (%s)",
                    type(shutdown_error).__name__,
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
                "Entry cleanup failed while unload/setup was cancelled (%s)",
                type(cleanup_error).__name__,
            )
        raise


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Register domain services once (entries come and go; services persist)."""

    async def _managers_for(call: ServiceCall) -> list[PanelManager]:
        """Resolve fleet subentries without collapsing them to their parent entry."""
        selection = target_helpers.TargetSelection(call.data)
        loaded: dict[str, BrilliantMqttConfigEntry] = {}
        for candidate in hass.config_entries.async_entries(DOMAIN):
            if candidate.state is ConfigEntryState.LOADED and isinstance(
                candidate.runtime_data, FleetManager
            ):
                loaded[candidate.entry_id] = cast(BrilliantMqttConfigEntry, candidate)

        selected_managers: dict[tuple[str, str], PanelManager] = {}

        def _add_entry(
            entry_id: str,
            subentry_ids: set[str | None] | None,
        ) -> None:
            entry = loaded.get(entry_id)
            if entry is None:
                return
            runtime = entry.runtime_data
            if subentry_ids is None or None in subentry_ids:
                for panel_id, manager in runtime.panels.items():
                    selected_managers[(entry_id, panel_id)] = manager
                return
            for subentry_id in subentry_ids:
                if subentry_id is None:
                    continue
                selected = runtime.panels.get(subentry_id)
                if selected is not None:
                    selected_managers[(entry_id, subentry_id)] = selected

        has_explicit_target = not _TARGET_KEYS.isdisjoint(call.data)
        if not has_explicit_target:
            for entry_id in loaded:
                _add_entry(entry_id, None)
            return list(selected_managers.values())
        if not selection.has_any_target:
            return []

        referenced = target_helpers.async_extract_referenced_entity_ids(
            hass,
            selection,
            primary_entities_only=False,
        )
        entity_registry = er.async_get(hass)
        for entity_id in referenced.referenced | referenced.indirectly_referenced:
            if (
                entity_entry := entity_registry.async_get(entity_id)
            ) is not None and entity_entry.config_entry_id is not None:
                _add_entry(
                    entity_entry.config_entry_id,
                    {entity_entry.config_subentry_id},
                )

        device_registry = dr.async_get(hass)
        for device_id in referenced.referenced_devices:
            if (device_entry := device_registry.async_get(device_id)) is None:
                continue
            for entry_id, subentry_ids in device_entry.config_entries_subentries.items():
                _add_entry(entry_id, set(subentry_ids))
        return list(selected_managers.values())

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
        override = call.data.get("release_override")
        if override is None:
            await _apply_to_all(call, lambda m: m.async_update_agent())
            return
        managers = await _managers_for(call)
        if len(managers) != 1 or not isinstance(override, dict):
            raise HomeAssistantError("release_override requires exactly one targeted panel")
        await managers[0].async_update_agent(release_override=override)

    async def _uninstall(call: ServiceCall) -> None:
        await _apply_to_all(call, lambda m: m.async_uninstall())

    async def _canary(call: ServiceCall) -> None:
        managers = await _managers_for(call)
        if len(managers) != 1 or _TARGET_KEYS.isdisjoint(call.data):
            raise HomeAssistantError(
                "canary recovery requires exactly one explicitly targeted panel"
            )
        name = call.data.get("name")
        if not isinstance(name, str):
            raise HomeAssistantError("canary recovery requires the retained baseline name")
        await managers[0].async_canary_operation(call.service.removeprefix("canary_"), name)

    async def _reboot(call: ServiceCall) -> None:
        collect = bool(call.data.get("collect_diagnostics", True))
        lines = int(call.data.get("journal_lines", DEFAULT_REBOOT_JOURNAL_LINES))
        await _apply_to_all(
            call,
            lambda m: m.async_reboot(collect_diagnostics=collect, journal_lines=lines),
        )

    async def _run_scene(call: ServiceCall) -> None:
        from .ha_control import get_control_plane

        panel = call.data.get("panel")
        await get_control_plane(hass).scene_control.async_run_scene(
            panel if isinstance(panel, str) else None,
            str(call.data["scene_id"]),
        )

    async def _set_mode(call: ServiceCall) -> None:
        from .ha_control import get_control_plane

        panel = call.data.get("panel")
        await get_control_plane(hass).scene_control.async_set_mode(
            panel if isinstance(panel, str) else None,
            str(call.data["mode_id"]),
        )

    hass.services.async_register(DOMAIN, "repair", _repair, schema=_SERVICE_SCHEMA)
    hass.services.async_register(DOMAIN, "redeploy", _redeploy, schema=_SERVICE_SCHEMA)
    hass.services.async_register(DOMAIN, "canary_rollback", _canary, schema=_SERVICE_SCHEMA)
    hass.services.async_register(DOMAIN, "canary_finalize", _canary, schema=_SERVICE_SCHEMA)
    hass.services.async_register(DOMAIN, "uninstall", _uninstall, schema=_SERVICE_SCHEMA)
    hass.services.async_register(DOMAIN, "reboot", _reboot, schema=_REBOOT_SCHEMA)
    hass.services.async_register(DOMAIN, "run_scene", _run_scene, schema=_RUN_SCENE_SCHEMA)

    async def _migrate_to_fleet(call: ServiceCall) -> ServiceResponse:
        return await MigrationCoordinator(hass).async_run(dry_run=bool(call.data["dry_run"]))

    hass.services.async_register(DOMAIN, "set_mode", _set_mode, schema=_SET_MODE_SCHEMA)
    hass.services.async_register(
        DOMAIN,
        "migrate_to_fleet",
        _migrate_to_fleet,
        schema=_MIGRATE_SCHEMA,
        supports_response=SupportsResponse.OPTIONAL,
    )

    # Settle an interrupted migration before HA sets up this domain's entries:
    # reverse to the exact originals before the commit point, or finish sibling
    # removal after it. Config/registry work only; never contacts a panel.
    # The fence is raised for the whole recovery and stays raised unless the
    # journal is resolved, so a failed (or crashed) recovery can never let both
    # the converted fleet and unremoved legacy siblings start.
    fence = migration_fence(hass)
    fence.activate()
    try:
        await MigrationCoordinator(hass).async_recover()
    except Exception as error:
        _LOGGER.error("Legacy fleet migration recovery failed (%s)", type(error).__name__)
        async_report_recovery_failure(hass, "recovery_crashed")
    # Restored entries whose quiet setup never succeeded stay quiet across
    # restarts; an unreadable obligation file fails closed (all quiet).
    await async_load_quiet_obligations(hass)
    async_update_migration_issue(hass)
    return True


async def async_setup_entry(hass: HomeAssistant, entry: BrilliantMqttConfigEntry) -> bool:
    fence = migration_fence(hass)
    if fence.blocks(entry.entry_id):
        # A legacy-to-fleet migration is running or awaiting recovery; starting
        # this entry could run a legacy panel next to the converted fleet.
        raise ConfigEntryNotReady("A Brilliant legacy fleet migration is unresolved")
    # Every panel surface (retained state/LWT subscriptions, command publishes) rides
    # HA's mqtt integration, so don't set up "green" against a broker that isn't up yet.
    if not await mqtt.async_wait_for_mqtt_client(hass):
        raise ConfigEntryNotReady("MQTT integration is not available")
    manager = FleetManager(
        hass,
        entry,
        defer_setup_panel_contact=fence.is_quiet(entry.entry_id),
    )
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
                "Entry cleanup failed after setup failure (%s)",
                type(cleanup_error).__name__,
            )
        raise
    # A migration-restoration setup stays quiet across failed attempts, HA's
    # automatic retries, and restarts; only a successful setup retires it. The
    # retirement runs after setup has returned, so neither its I/O nor a
    # cancellation can fail this setup or orphan the manager; if it never runs,
    # the obligation simply stays and the next setup is quiet again.
    if fence.is_quiet(entry.entry_id):
        hass.async_create_task(
            async_quiet_setup_succeeded(hass, entry.entry_id),
            f"brilliant-mqtt-quiet-retire-{entry.entry_id}",
            eager_start=False,
        )
    async_clear_reverted_issue(hass, entry.entry_id)
    return True


async def async_migrate_entry(hass: HomeAssistant, entry: BrilliantMqttConfigEntry) -> bool:
    """Migrate legacy entries to the HA-owned control plane without deleting secrets."""
    if entry.version > CONFIG_ENTRY_VERSION:
        return False  # downgrade not supported
    if entry.version == CONFIG_ENTRY_VERSION:
        return True

    data = dict(entry.data)
    if entry.version == 1:
        data[CONF_COMPONENTS] = {
            COMPONENT_BRIDGE: True,
            COMPONENT_VOICE: bool(data.get(CONF_VOICE_ENABLED, False)),
        }
    if entry.version < 3:
        components = dict(data.get(CONF_COMPONENTS) or {})
        components[COMPONENT_BRIDGE] = True
        components[COMPONENT_HA_MIRROR] = False
        data[CONF_COMPONENTS] = components

        if CONF_HA_CONTROL_LABEL not in data:
            legacy_label = data.get(CONF_HA_MIRROR_LABEL)
            data[CONF_HA_CONTROL_LABEL] = (
                legacy_label.strip()
                if isinstance(legacy_label, str) and legacy_label.strip()
                else DEFAULT_HA_CONTROL_LABEL
            )
        data.setdefault(CONF_HA_CONTROL_ENABLED, DEFAULT_HA_CONTROL_ENABLED)
        data.setdefault(CONF_ROOM_OVERRIDES, {})
        data.setdefault(CONF_HA_CONTROL_DOMAINS, list(DEFAULT_HA_CONTROL_DOMAINS))
        data.setdefault(CONF_MAX_MIRRORED_ENTITIES, DEFAULT_MAX_MIRRORED_ENTITIES)
        data.setdefault(CONF_SCENE_PANEL, data.get(CONF_PANEL, ""))
        data.setdefault(CONF_SCENE_ACTIONS, {})
    data[CONF_ENTRY_KIND] = ENTRY_KIND_LEGACY_PENDING_CONSOLIDATION
    hass.config_entries.async_update_entry(entry, data=data, version=CONFIG_ENTRY_VERSION)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: BrilliantMqttConfigEntry) -> bool:
    if unloaded := await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        await _async_cleanup_entry(hass, entry)
    return unloaded


async def async_remove_entry(hass: HomeAssistant, entry: BrilliantMqttConfigEntry) -> None:
    """Delete owned issues, then settle any exact uncommitted panel handoff."""
    if entry.entry_id in migration_removing(hass):
        # A legacy sibling consolidated into the fleet: its panel keeps the same
        # management ID, so the panel-owned issues now belong to the fleet runtime.
        # Only entry-keyed issues die with it, and no provisioning recovery runs.
        for issue_id in ("broker_unavailable", "runtime_setup_failed", "fleet_storage"):
            ir.async_delete_issue(hass, DOMAIN, f"{issue_id}_{entry.entry_id}")
        return
    if ir.async_get(hass).async_get_issue(DOMAIN, ISSUE_MIGRATION_AVAILABLE) is not None:
        async_update_migration_issue(hass)
    management_ids = {
        str(subentry.data[CONF_MANAGEMENT_ID])
        for subentry in entry.subentries.values()
        if CONF_MANAGEMENT_ID in subentry.data
    } or {entry.entry_id}
    for management_id in management_ids:
        async_delete_panel_issues(hass, management_id)
    ir.async_delete_issue(hass, DOMAIN, f"broker_unavailable_{entry.entry_id}")
    ir.async_delete_issue(hass, DOMAIN, f"runtime_setup_failed_{entry.entry_id}")
    ir.async_delete_issue(hass, DOMAIN, f"fleet_storage_{entry.entry_id}")
    await async_recover_removed_entry(hass, entry)
