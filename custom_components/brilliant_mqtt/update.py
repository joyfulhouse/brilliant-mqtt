"""Agent update entity — installed from the bridge meta topic, latest from the payload."""

from __future__ import annotations

from typing import Any

from homeassistant.components.update import UpdateEntity, UpdateEntityFeature
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from . import BrilliantMqttConfigEntry, manager
from .entity import BrilliantPanelEntity

# Push-only entity (installed version comes from the bridge meta topic); install
# serializes panel SSH via the fleet-wide lock, so no parallel-update limit is needed.
PARALLEL_UPDATES = 0


async def async_setup_entry(
    hass: HomeAssistant,
    entry: BrilliantMqttConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    # Late binding: resolve manager._payload_dir at call time so a test (or a future
    # caller) that patches it on the manager module is honored — a module-level
    # `from .manager import _payload_dir` would freeze the original in this namespace.
    def _read_latest() -> str | None:
        # agent_payload/ is built into release zips only (see release.yml) — a raw
        # git-clone install has no payload. Skip the update entity rather than crash
        # platform setup; every other platform still loads.
        try:
            return (manager._payload_dir() / "VERSION").read_text().strip()
        except OSError:
            return None

    latest = await hass.async_add_executor_job(_read_latest)
    if latest is None:
        return
    async_add_entities([AgentUpdate(entry, latest)])


class AgentUpdate(BrilliantPanelEntity, UpdateEntity):
    _attr_supported_features = UpdateEntityFeature.INSTALL | UpdateEntityFeature.PROGRESS
    _attr_translation_key = "bridge"
    _attr_title = "brilliant-mqtt agent"

    def __init__(self, entry: BrilliantMqttConfigEntry, latest: str) -> None:
        super().__init__(entry.runtime_data)
        self._attr_unique_id = f"{entry.entry_id}_agent_update"
        self._attr_latest_version = latest

    @property
    def installed_version(self) -> str | None:
        meta = self._manager.meta
        return meta.get("agent_version") if meta else None

    async def async_install(self, version: str | None, backup: bool, **kwargs: Any) -> None:
        # Render a determinate progress bar through the deploy stages (needs the
        # PROGRESS feature, above); async_update_agent re-raises on failure so HA
        # surfaces it.
        def _progress(pct: int) -> None:
            self._attr_update_percentage = pct
            self.async_write_ha_state()

        self._attr_in_progress = True
        self._attr_update_percentage = 0
        self.async_write_ha_state()
        try:
            await self._manager.async_update_agent(progress=_progress)
        finally:
            self._attr_in_progress = False
            self._attr_update_percentage = None
            self.async_write_ha_state()
