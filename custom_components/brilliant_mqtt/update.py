"""Agent update entity — installed from the bridge meta topic, latest from the payload."""

from __future__ import annotations

from typing import Any

from homeassistant.components.update import UpdateEntity, UpdateEntityFeature
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from . import BrilliantMqttConfigEntry, manager
from .entity import BrilliantPanelEntity
from .manager import PanelManager

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
    for panel_manager in entry.runtime_data.panels.values():
        entities = [AgentUpdate(panel_manager, latest)]
        if (subentry_id := panel_manager.store.subentry_id) is None:
            async_add_entities(entities)
        else:
            async_add_entities(entities, config_subentry_id=subentry_id)


class AgentUpdate(BrilliantPanelEntity, UpdateEntity):
    _attr_supported_features = UpdateEntityFeature.INSTALL | UpdateEntityFeature.PROGRESS
    _attr_translation_key = "bridge"
    _attr_title = "brilliant-mqtt agent"

    def __init__(self, manager: PanelManager, latest: str) -> None:
        super().__init__(manager)
        self._attr_unique_id = f"{manager.store.management_id}_agent_update"
        self._attr_latest_version = latest

    @property
    def installed_version(self) -> str | None:
        meta = self._manager.meta
        return meta.get("agent_version") if meta else None

    @property
    def in_progress(self) -> bool:
        # The manager owns progress, so an update started anywhere (this entity, the
        # redeploy service or an interrupted-update Retry) renders here.
        return self._manager.update_progress is not None

    @property
    def update_percentage(self) -> int | None:
        return self._manager.update_progress

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        # The allowlisted update block (retry state, typed last error, interrupted
        # update). Its ``expired`` flag is evaluated when the state is written, so it
        # only refreshes when the state is otherwise written.
        return {"update": self._manager.update_diagnostics()}

    async def async_install(self, version: str | None, backup: bool, **kwargs: Any) -> None:
        # async_update_agent re-raises on failure so HA surfaces it; progress is
        # published through the manager's state signal.
        await self._manager.async_update_agent()
