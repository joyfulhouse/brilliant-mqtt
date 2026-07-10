"""Reconcile labeled Home Assistant entities with hosted peripherals."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from brilliant_ha_mirror.config import Settings
from brilliant_ha_mirror.mapping import (
    HaEntity,
    PeripheralSpec,
    command_to_service,
    spec_for,
    state_to_variables,
)
from brilliant_ha_mirror.protocols import HaClient, PeripheralHostClient


class Mirror:
    """Keep hosted peripherals aligned with labeled Home Assistant entities."""

    def __init__(
        self,
        ha: HaClient,
        host: PeripheralHostClient,
        settings: Settings,
    ) -> None:
        self._ha = ha
        self._host = host
        self._settings = settings
        self._name_by_entity: dict[str, str] = {}
        self._entity_by_name: dict[str, str] = {}

    def _peripheral_name(self, entity: HaEntity) -> str:
        friendly = entity.attributes.get("friendly_name")
        if not isinstance(friendly, str) or not friendly:
            friendly = entity.entity_id
        return f"HA {friendly}"

    def _command_handler(
        self,
        entity_id: str,
    ) -> Callable[[str, str], Awaitable[None]]:
        async def on_command(var: str, value: str) -> None:
            call = command_to_service(entity_id, var, value)
            await self._ha.call_service(call)

        return on_command

    async def start(self) -> None:
        """Subscribe to state changes and perform the initial reconciliation."""
        self._ha.on_state_change(self._handle_state_change)
        await self.reconcile()

    async def reconcile(self) -> None:
        """Register, refresh, and delete peripherals to match the HA label."""
        entities = await self._ha.get_entities(self._settings.mirror_label)
        supported: dict[str, tuple[HaEntity, PeripheralSpec]] = {}
        for entity in entities:
            spec = spec_for(entity)
            if spec is not None:
                supported[entity.entity_id] = (entity, spec)

        for entity_id, (entity, spec) in supported.items():
            name = self._name_by_entity.get(entity_id)
            if name is None:
                name = self._peripheral_name(entity)
                await self._host.register(name, spec, self._command_handler(entity_id))
                self._name_by_entity[entity_id] = name
                self._entity_by_name[name] = entity_id
            else:
                await self._host.update_variables(name, state_to_variables(entity))

        stale_entity_ids = self._name_by_entity.keys() - supported.keys()
        for entity_id in list(stale_entity_ids):
            name = self._name_by_entity[entity_id]
            await self._host.delete(name)
            del self._name_by_entity[entity_id]
            del self._entity_by_name[name]

    async def _handle_state_change(self, entity: HaEntity) -> None:
        name = self._name_by_entity.get(entity.entity_id)
        if name is not None:
            await self._host.update_variables(name, state_to_variables(entity))

    async def stop(self) -> None:
        """Delete every hosted peripheral and forget all entity mappings."""
        for name in list(self._entity_by_name):
            await self._host.delete(name)
        self._name_by_entity.clear()
        self._entity_by_name.clear()
