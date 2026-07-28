"""One fleet runtime owning panel managers and shared lifecycle resources."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine, Iterable, Mapping
from types import MappingProxyType
from typing import Any

from homeassistant.components import mqtt
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import CALLBACK_TYPE, HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers import issue_registry as ir

from .broker import BrokerKind, BrokerProfile
from .const import (
    CONF_BROKER_KIND,
    CONF_ENTRY_KIND,
    CONF_HA_CONTROL_DOMAINS,
    CONF_HA_CONTROL_ENABLED,
    CONF_HA_CONTROL_LABEL,
    CONF_MAX_MIRRORED_ENTITIES,
    CONF_MESH_PRIORITY,
    CONF_MQTT_TLS_CA,
    CONF_PANEL,
    CONF_ROOM_OVERRIDES,
    CONF_SCENE_ACTIONS,
    CONF_SCENE_PANEL,
    CONFIG_ENTRY_VERSION,
    DEFAULT_HA_CONTROL_DOMAINS,
    DEFAULT_HA_CONTROL_ENABLED,
    DEFAULT_HA_CONTROL_LABEL,
    DEFAULT_MAX_MIRRORED_ENTITIES,
    DOMAIN,
    ENTRY_KIND_FLEET,
    ENTRY_KIND_LEGACY_PENDING_CONSOLIDATION,
)
from .entry_data import (
    EntryDataError,
    FleetConfig,
    FleetPanelStore,
    LegacyPanelStore,
    PanelConfig,
    PanelConfigStore,
)
from .manager import PanelManager
from .shell import HostIdentity, PanelIdentityError

_LOGGER = logging.getLogger(__name__)

_BROKER_ISSUE_REASON = "Home Assistant's MQTT broker connection is unavailable"
_RUNTIME_SETUP_ISSUE_REASON = (
    "No panel runtime could start. Home Assistant will retry automatically; "
    "verify MQTT connectivity and review the integration logs."
)


def _shared_ssh_lock(hass: HomeAssistant) -> asyncio.Lock:
    """Reuse the domain lock shared with legacy config-flow SSH operations."""
    domain_data: dict[str, Any] = hass.data.setdefault(DOMAIN, {})
    lock: asyncio.Lock = domain_data.setdefault("ssh_lock", asyncio.Lock())
    return lock


def _legacy_mapping(
    data: Mapping[str, Any],
    key: str,
) -> MappingProxyType[str, Any]:
    """Return one detached legacy global mapping or fail without exposing data."""
    value = data.get(key, {})
    if not isinstance(value, Mapping) or any(not isinstance(item, str) for item in value):
        raise EntryDataError("invalid_legacy_fleet_data")
    return MappingProxyType(dict(value))


def legacy_fleet_config(entry: ConfigEntry[Any]) -> FleetConfig:
    """Normalize one legacy panel entry into the immutable fleet-global contract."""
    data = entry.data
    panel = data.get(CONF_PANEL)
    if not isinstance(panel, str) or not panel:
        raise EntryDataError("invalid_legacy_fleet_data")

    broker_data = dict(data)
    broker_data.setdefault(CONF_BROKER_KIND, BrokerKind.EXISTING_BROKER.value)
    broker_data.setdefault(CONF_MQTT_TLS_CA, None)
    try:
        broker = BrokerProfile.from_mapping(broker_data)
    except Exception:
        raise EntryDataError("invalid_legacy_fleet_data") from None

    enabled = data.get(CONF_HA_CONTROL_ENABLED, DEFAULT_HA_CONTROL_ENABLED)
    label = data.get(CONF_HA_CONTROL_LABEL, DEFAULT_HA_CONTROL_LABEL)
    domains = data.get(CONF_HA_CONTROL_DOMAINS, DEFAULT_HA_CONTROL_DOMAINS)
    maximum = data.get(CONF_MAX_MIRRORED_ENTITIES, DEFAULT_MAX_MIRRORED_ENTITIES)
    scene_panel = data.get(CONF_SCENE_PANEL, panel)
    mesh_priority = data.get(CONF_MESH_PRIORITY, 0)
    if (
        type(enabled) is not bool
        or not isinstance(label, str)
        or not label
        or not isinstance(domains, (list, tuple))
        or any(not isinstance(domain, str) or not domain for domain in domains)
        or type(maximum) is not int
        or maximum < 1
        or not isinstance(scene_panel, str)
        or not scene_panel
        or type(mesh_priority) is not int
        or mesh_priority < 0
    ):
        raise EntryDataError("invalid_legacy_fleet_data")

    return FleetConfig(
        entry_kind=ENTRY_KIND_LEGACY_PENDING_CONSOLIDATION,
        broker=broker,
        next_mesh_priority=max(1, mesh_priority + 1),
        ha_control_enabled=enabled,
        ha_control_label=label,
        room_overrides=_legacy_mapping(data, CONF_ROOM_OVERRIDES),
        ha_control_domains=tuple(domains),
        max_mirrored_entities=maximum,
        scene_panel=scene_panel,
        scene_actions=_legacy_mapping(data, CONF_SCENE_ACTIONS),
        schema_version=entry.version,
    )


async def _async_drain(
    operations: Iterable[Coroutine[Any, Any, None]],
) -> list[BaseException]:
    """Settle every independent cleanup and return failures in input order."""
    tasks = [
        asyncio.create_task(operation, name="brilliant-mqtt-panel-shutdown")
        for operation in operations
    ]
    if not tasks:
        return []
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return [result for result in results if isinstance(result, BaseException)]


async def _async_settle_cleanup(operation: Coroutine[Any, Any, None]) -> None:
    """Drain cleanup through caller cancellation, then preserve its primary result."""
    cleanup_task = asyncio.create_task(operation, name="brilliant-mqtt-fleet-cleanup")
    owner = asyncio.current_task()
    baseline = owner.cancelling() if owner is not None else 0
    cancellation: asyncio.CancelledError | None = None
    try:
        await asyncio.shield(cleanup_task)
    except asyncio.CancelledError as error:
        if owner is None or owner.cancelling() <= baseline:
            await cleanup_task
            return
        cancellation = error
        while not cleanup_task.done():
            try:
                await asyncio.shield(cleanup_task)
            except asyncio.CancelledError as repeated:
                cancellation = repeated
                continue
    try:
        cleanup_task.result()
    except BaseException:
        if cancellation is None:
            raise
        _LOGGER.warning("Fleet cleanup also failed while its caller was cancelled")
    if cancellation is not None:
        raise cancellation


class FleetManager:
    """Own all panel managers below one fleet or one legacy compatibility entry."""

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry[Any]) -> None:
        self.hass = hass
        self.entry = entry
        self._ssh_lock = _shared_ssh_lock(hass)
        self._lifecycle_lock = asyncio.Lock()
        self._panels: dict[str, PanelManager] = {}
        self._panels_view: Mapping[str, PanelManager] = MappingProxyType(self._panels)
        self._panel_configs: dict[str, PanelConfig] = {}
        self._fleet: FleetConfig | None = None
        self._legacy = False
        self._update_unsub: CALLBACK_TYPE | None = None
        self._mqtt_unsub: CALLBACK_TYPE | None = None
        self._shutting_down = False
        self.broker_available: bool | None = None

    @property
    def panels(self) -> Mapping[str, PanelManager]:
        """Return a live, read-only view keyed by subentry or legacy entry ID."""
        return self._panels_view

    @property
    def fleet(self) -> FleetConfig:
        """Return the normalized fleet globals after setup has parsed them."""
        if self._fleet is None:
            raise RuntimeError("fleet_not_initialized")
        return self._fleet

    @property
    def broker_issue_id(self) -> str:
        """Return the one stable Home Assistant MQTT outage issue ID."""
        return f"broker_unavailable_{self.entry.entry_id}"

    @property
    def runtime_setup_issue_id(self) -> str:
        """Return the stable issue ID for an entry with no usable panel runtime."""
        return f"runtime_setup_failed_{self.entry.entry_id}"

    def _build_stores(
        self,
    ) -> tuple[FleetConfig, dict[str, tuple[PanelConfigStore, PanelConfig | None]], bool]:
        """Parse every owner and reject collisions before constructing a manager."""
        if self.entry.version != CONFIG_ENTRY_VERSION:
            raise EntryDataError("invalid_entry_version")
        entry_kind = self.entry.data.get(CONF_ENTRY_KIND)
        if entry_kind not in {
            ENTRY_KIND_FLEET,
            ENTRY_KIND_LEGACY_PENDING_CONSOLIDATION,
        }:
            raise EntryDataError("invalid_entry_kind")

        if entry_kind == ENTRY_KIND_FLEET:
            fleet = FleetConfig.from_entry(self.entry)
            built: dict[str, tuple[PanelConfigStore, PanelConfig | None]] = {}
            slugs: set[str] = set()
            fingerprints: set[str] = set()
            management_ids: set[str] = set()
            for subentry in self.entry.subentries.values():
                config = PanelConfig.from_subentry(subentry)
                if config.ssh_username != "root":
                    raise EntryDataError("invalid_panel_ssh_username")
                try:
                    HostIdentity(
                        public_key=config.ssh_host_key,
                        fingerprint=config.identity_fingerprint,
                    )
                except PanelIdentityError:
                    raise EntryDataError("invalid_panel_identity") from None
                if config.panel in slugs:
                    raise EntryDataError("duplicate_panel_slug")
                if config.identity_fingerprint in fingerprints:
                    raise EntryDataError("duplicate_panel_fingerprint")
                if config.management_id in management_ids:
                    raise EntryDataError("duplicate_panel_management_id")
                slugs.add(config.panel)
                fingerprints.add(config.identity_fingerprint)
                management_ids.add(config.management_id)
                fleet_store = FleetPanelStore(self.hass, self.entry, subentry)
                built[fleet_store.panel_id] = (fleet_store, config)
            if built and fleet.scene_panel not in built:
                raise EntryDataError("invalid_scene_panel")
            return fleet, built, False

        if self.entry.subentries:
            raise EntryDataError("legacy_subentries_unsupported")
        legacy_store = LegacyPanelStore(self.hass, self.entry)
        return (
            legacy_fleet_config(self.entry),
            {legacy_store.panel_id: (legacy_store, None)},
            True,
        )

    def _release_shared_owners(self) -> list[BaseException]:
        """Attempt every synchronous owner release and return failures in order."""
        failures: list[BaseException] = []
        for attribute in ("_update_unsub", "_mqtt_unsub"):
            unsubscribe = getattr(self, attribute)
            setattr(self, attribute, None)
            if unsubscribe is None:
                continue
            try:
                unsubscribe()
            except BaseException as error:
                failures.append(error)
        try:
            ir.async_delete_issue(self.hass, DOMAIN, self.broker_issue_id)
        except BaseException as error:
            failures.append(error)
        return failures

    async def _async_cleanup_failed_setup(
        self,
        managers: Iterable[PanelManager],
    ) -> None:
        """Release every partially registered owner without masking setup failure."""
        failures = self._release_shared_owners()
        failures.extend(await _async_drain(manager.async_shutdown() for manager in managers))
        self._panels.clear()
        self._panel_configs.clear()
        self._fleet = None
        self._legacy = False
        self.broker_available = None
        for failure in failures:
            _LOGGER.warning(
                "Fleet setup cleanup failed (%s)",
                type(failure).__name__,
            )

    async def _async_cleanup_staged_additions(
        self,
        managers: Iterable[PanelManager],
    ) -> None:
        """Drain every unpublished live addition without masking its failure."""
        failures = await _async_drain(manager.async_shutdown() for manager in managers)
        for failure in failures:
            _LOGGER.warning(
                "Live-add cleanup failed (%s)",
                type(failure).__name__,
            )

    async def _async_shutdown_removed(
        self,
        managers: Iterable[PanelManager],
    ) -> None:
        """Drain every removed runtime and surface the first cleanup failure."""
        failures = await _async_drain(manager.async_shutdown() for manager in managers)
        if not failures:
            return
        for failure in failures[1:]:
            _LOGGER.warning(
                "Additional removed-panel shutdown failed (%s)",
                type(failure).__name__,
            )
        raise failures[0]

    @staticmethod
    def _assert_immutable_panel(
        previous: PanelConfig,
        current: PanelConfig,
    ) -> None:
        """Fail closed if a live update bypasses the store's immutable fields."""
        if (
            current.panel != previous.panel
            or current.identity_fingerprint != previous.identity_fingerprint
            or current.management_id != previous.management_id
            or current.ssh_host_key != previous.ssh_host_key
        ):
            raise EntryDataError("immutable_panel_identity")

    async def _async_setup_manager(self, manager: PanelManager) -> bool:
        """Start one manager and report whether it owns a usable runtime."""
        try:
            await manager.async_setup()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            _LOGGER.warning(
                "%s: panel runtime loaded degraded after %s",
                manager.panel,
                type(error).__name__,
            )
            manager.mark_runtime_degraded(f"runtime setup failed ({type(error).__name__})")
            return False
        return True

    @callback
    def _async_create_runtime_setup_issue(self) -> None:
        """Surface a redacted, actionable issue while HA retries the entry."""
        ir.async_create_issue(
            self.hass,
            DOMAIN,
            self.runtime_setup_issue_id,
            is_fixable=False,
            severity=ir.IssueSeverity.ERROR,
            translation_key="needs_attention",
            translation_placeholders={
                "panel": "Brilliant MQTT fleet",
                "reason": _RUNTIME_SETUP_ISSUE_REASON,
            },
            learn_more_url=(
                "https://github.com/joyfulhouse/brilliant-mqtt/blob/main/docs/ha-integration.md"
            ),
        )

    @callback
    def _async_broker_status(self, connected: bool) -> None:
        """Maintain one broker issue rather than one issue per panel."""
        if self._shutting_down:
            return
        self.broker_available = connected
        if connected:
            ir.async_delete_issue(self.hass, DOMAIN, self.broker_issue_id)
            return
        ir.async_create_issue(
            self.hass,
            DOMAIN,
            self.broker_issue_id,
            is_fixable=False,
            severity=ir.IssueSeverity.ERROR,
            translation_key="needs_attention",
            translation_placeholders={
                "panel": "Brilliant MQTT fleet",
                "reason": _BROKER_ISSUE_REASON,
            },
            learn_more_url=(
                "https://github.com/joyfulhouse/brilliant-mqtt/blob/main/docs/ha-integration.md"
            ),
        )

    async def async_setup(self) -> None:
        """Build all stores first, then start panel runtimes independently."""
        async with self._lifecycle_lock:
            fleet, built, legacy = self._build_stores()
            managers = {
                panel_id: PanelManager(self.hass, store, fleet, self._ssh_lock)
                for panel_id, (store, _config) in built.items()
            }
            self._fleet = fleet
            self._legacy = legacy
            self._panel_configs = {
                panel_id: config
                for panel_id, (_store, config) in built.items()
                if config is not None
            }
            self._panels.update(managers)
            try:
                successful_starts = 0
                for manager in managers.values():
                    if await self._async_setup_manager(manager):
                        successful_starts += 1
                if managers and successful_starts == 0:
                    self._async_create_runtime_setup_issue()
                    raise ConfigEntryNotReady("No panel runtime could start")
                ir.async_delete_issue(
                    self.hass,
                    DOMAIN,
                    self.runtime_setup_issue_id,
                )
                self._update_unsub = self.entry.add_update_listener(self._async_entry_updated)
                self._mqtt_unsub = mqtt.async_subscribe_connection_status(
                    self.hass, self._async_broker_status
                )
                self._async_broker_status(mqtt.is_connected(self.hass))
            except BaseException:
                await _async_settle_cleanup(
                    self._async_cleanup_failed_setup(tuple(managers.values()))
                )
                raise

    async def _async_entry_updated(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry[Any],
    ) -> None:
        """Reconcile live subentry/global updates without restarting managers."""
        del hass
        if entry is not self.entry or self._shutting_down:
            return
        await self._async_reconcile()

    async def _async_reconcile(self) -> None:
        """Apply one fully validated entry snapshot under the lifecycle lock."""
        async with self._lifecycle_lock:
            if self._shutting_down:
                return
            fleet, built, legacy = self._build_stores()
            if legacy != self._legacy:
                raise EntryDataError("runtime_entry_kind_changed")

            for panel_id, previous in self._panel_configs.items():
                if panel_id in built:
                    current = built[panel_id][1]
                    assert current is not None
                    self._assert_immutable_panel(previous, current)

            staged: dict[str, PanelManager] = {}
            try:
                for panel_id, (store, _config) in built.items():
                    if panel_id not in self._panels:
                        staged[panel_id] = PanelManager(
                            self.hass,
                            store,
                            fleet,
                            self._ssh_lock,
                        )
                for manager in staged.values():
                    await self._async_setup_manager(manager)
            except BaseException:
                await _async_settle_cleanup(
                    self._async_cleanup_staged_additions(tuple(staged.values()))
                )
                raise

            removed = tuple(
                (panel_id, manager)
                for panel_id, manager in self._panels.items()
                if panel_id not in built
            )

            self._fleet = fleet
            for panel_id, (store, _config) in built.items():
                if existing := self._panels.get(panel_id):
                    existing.update_config(store, fleet)
            for panel_id, _manager in removed:
                self._panels.pop(panel_id)
            self._panels.update(staged)

            self._panel_configs = {
                panel_id: config
                for panel_id, (_store, config) in built.items()
                if config is not None
            }
            if removed:
                await _async_settle_cleanup(
                    self._async_shutdown_removed(tuple(manager for _panel_id, manager in removed))
                )

    async def async_panel_added(self, subentry_id: str) -> None:
        """Reconcile a newly persisted panel subentry."""
        if self._legacy:
            raise EntryDataError("legacy_subentries_unsupported")
        await self._async_reconcile()
        if subentry_id not in self._panels:
            raise EntryDataError("panel_subentry_missing")

    async def async_panel_updated(self, subentry_id: str) -> None:
        """Reconcile one persisted panel update in place."""
        await self._async_reconcile()
        if subentry_id not in self._panels:
            raise EntryDataError("panel_subentry_missing")

    async def async_panel_removed(self, panel_id: str) -> None:
        """Reconcile one subentry removal and drain its manager."""
        await self._async_reconcile()
        if panel_id in self._panels:
            raise EntryDataError("panel_subentry_still_present")

    async def _async_shutdown_owned(self) -> None:
        """Unregister shared owners and drain every panel despite failures."""
        async with self._lifecycle_lock:
            self._shutting_down = True
            managers = tuple(self._panels.values())
            failures = self._release_shared_owners()
            self._panels.clear()
            self._panel_configs.clear()
            failures.extend(await _async_drain(manager.async_shutdown() for manager in managers))
            if failures:
                for secondary in failures[1:]:
                    _LOGGER.warning(
                        "Additional panel shutdown failed (%s)",
                        type(secondary).__name__,
                    )
                raise failures[0]

    async def async_shutdown(self) -> None:
        """Drain all panel runtimes even through failure or caller cancellation."""
        await _async_settle_cleanup(self._async_shutdown_owned())
