"""Reconcile labeled Home Assistant entities with hosted peripherals."""

from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Awaitable, Callable

from brilliant_ha_mirror.config import Settings
from brilliant_ha_mirror.mapping import (
    HaEntity,
    PeripheralSpec,
    command_to_service,
    resolve_room_id,
    spec_for,
    state_to_variables,
)
from brilliant_ha_mirror.protocols import HaClient, PeripheralHostClient

logger = logging.getLogger(__name__)


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
        self._rooms: dict[str, str] = {}
        self._area_by_entity: dict[str, str | None] = {}
        self._room_id_by_entity: dict[str, str | None] = {}
        self._logged_unmatched: set[tuple[str, str | None]] = set()

    def _base_name(self, entity: HaEntity) -> str:
        friendly = entity.attributes.get("friendly_name")
        if not isinstance(friendly, str) or not friendly:
            friendly = entity.entity_id
        return f"HA {friendly}"

    def _assign_names(self, entities: list[HaEntity]) -> dict[str, str]:
        """Map entity_id -> a unique peripheral name.

        Peripheral names are the bus registry key AND the panel display label,
        so two entities that share a friendly name would otherwise collide (the
        second would be silently dropped). Base names that are unique are used
        as-is; only colliding ones are disambiguated with the entity's object_id
        (globally unique), so the common case stays clean.
        """
        base = {e.entity_id: self._base_name(e) for e in entities}
        counts = Counter(base.values())
        names: dict[str, str] = {}
        for entity_id, name in base.items():
            if counts[name] > 1:
                object_id = entity_id.split(".", 1)[-1]
                names[entity_id] = f"{name} ({object_id})"
            else:
                names[entity_id] = name
        return names

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
        names = self._assign_names([entity for entity, _ in supported.values()])
        registered_or_renamed: set[str] = set()

        for entity_id, (entity, spec) in supported.items():
            name = names[entity_id]
            current = self._name_by_entity.get(entity_id)
            if current == name:
                await self._host.update_variables(name, state_to_variables(entity))
                continue
            # The unique name changed (a collision appeared or resolved), or this
            # entity is new. Register the new peripheral BEFORE deleting the old
            # one and only mutate the maps once registration succeeds, so a
            # failed register cannot leave the entity deleted-but-still-tracked.
            await self._host.register(name, spec, self._command_handler(entity_id))
            registered_or_renamed.add(entity_id)
            if current is not None:
                await self._host.delete(current)
                del self._entity_by_name[current]
            self._name_by_entity[entity_id] = name
            self._entity_by_name[name] = entity_id

        rooms: dict[str, str] | None = None
        if supported:
            try:
                rooms = dict(await self._host.get_rooms())
            except Exception:
                logger.warning(
                    "failed to read Brilliant room catalog; skipping room assignment this cycle",
                    exc_info=True,
                )

        if rooms is not None:
            catalog_changed = rooms != self._rooms
            for entity_id, (entity, _) in supported.items():
                room_id = resolve_room_id(entity.area, rooms, self._settings.room_overrides)
                assignment_changed = (
                    entity_id not in self._room_id_by_entity
                    or self._room_id_by_entity[entity_id] != room_id
                )
                area_changed = (
                    entity_id not in self._area_by_entity
                    or self._area_by_entity[entity_id] != entity.area
                )
                if (
                    entity_id in registered_or_renamed
                    or catalog_changed
                    or assignment_changed
                    or area_changed
                ):
                    name = self._name_by_entity[entity_id]
                    await self._host.set_room_assignment(
                        name,
                        [room_id] if room_id is not None else [],
                    )
                if room_id is None:
                    unmatched = (entity_id, entity.area)
                    if unmatched not in self._logged_unmatched:
                        logger.debug(
                            "no Brilliant room matched HA area %r for %s; leaving unassigned",
                            entity.area,
                            entity_id,
                        )
                        self._logged_unmatched.add(unmatched)
                self._area_by_entity[entity_id] = entity.area
                self._room_id_by_entity[entity_id] = room_id
            self._rooms = rooms

        stale_entity_ids = self._name_by_entity.keys() - supported.keys()
        for entity_id in list(stale_entity_ids):
            name = self._name_by_entity[entity_id]
            await self._host.delete(name)
            del self._name_by_entity[entity_id]
            del self._entity_by_name[name]
            self._area_by_entity.pop(entity_id, None)
            self._room_id_by_entity.pop(entity_id, None)

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
        self._rooms.clear()
        self._area_by_entity.clear()
        self._room_id_by_entity.clear()
        self._logged_unmatched.clear()
