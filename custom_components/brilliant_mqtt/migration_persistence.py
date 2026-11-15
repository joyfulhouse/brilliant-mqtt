"""Prove Home Assistant storage writes instead of trusting a scheduled save.

``Store.async_save`` logs and swallows write/serialization errors and silently
defers while Home Assistant is stopping; config entries save after a 1 s delay
and the registries after 10 s (180 s during startup). A migration commit point
must therefore never rely on in-memory state: each boundary forces the write and
then re-reads the file through a fresh ``Store`` (no in-memory or preload cache)
and compares it with the expected content.

The flush uses Home Assistant's own storage objects (``_store``/``_data_to_save``
on ``ConfigEntries`` and the registries), which are not public API; the gate
tests run against the pinned Home Assistant release.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from homeassistant.core import CoreState, HomeAssistant
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.storage import Store

_NOT_WRITABLE = frozenset({CoreState.stopping, CoreState.final_write, CoreState.stopped})


class PersistenceUnprovenError(RuntimeError):
    """A required storage write could not be proven on disk."""

    def __init__(self, code: str = "storage_unproven") -> None:
        super().__init__(code)
        self.code = code


def require_writable(hass: HomeAssistant) -> None:
    """Refuse to start a durable step while HA can no longer write immediately."""
    if hass.state in _NOT_WRITABLE:
        raise PersistenceUnprovenError("home_assistant_stopping")


async def async_load_fresh(hass: HomeAssistant, store: Store[Any]) -> Any:
    """Read ``store``'s file through a new Store so no cached copy can answer."""
    fresh: Store[Any] = Store(hass, store.version, store.key, minor_version=store.minor_version)
    try:
        return await fresh.async_load()
    except Exception:
        raise PersistenceUnprovenError() from None


async def async_save_and_load(hass: HomeAssistant, store: Store[Any], data: Any) -> Any:
    """Write ``data`` now and return what the file holds afterwards."""
    require_writable(hass)
    await store.async_save(data)
    return await async_load_fresh(hass, store)


def _records(loaded: Any, key: str) -> list[Mapping[str, Any]]:
    if not isinstance(loaded, Mapping):
        raise PersistenceUnprovenError()
    records = loaded.get(key)
    if not isinstance(records, list) or not all(isinstance(item, Mapping) for item in records):
        raise PersistenceUnprovenError()
    return list(records)


async def async_persist_config_entries(hass: HomeAssistant) -> dict[str, Mapping[str, Any]]:
    """Flush core.config_entries and return its on-disk entries by entry ID."""
    manager = hass.config_entries
    loaded = await async_save_and_load(hass, manager._store, manager._data_to_save())
    return {str(item.get("entry_id")): item for item in _records(loaded, "entries")}


async def async_persist_entity_registry(hass: HomeAssistant) -> dict[str, Mapping[str, Any]]:
    """Flush core.entity_registry and return its on-disk entities by entity ID."""
    registry = er.async_get(hass)
    loaded = await async_save_and_load(hass, registry._store, registry._data_to_save())
    return {str(item.get("entity_id")): item for item in _records(loaded, "entities")}


async def async_persist_device_registry(hass: HomeAssistant) -> dict[str, Mapping[str, Any]]:
    """Flush core.device_registry and return its on-disk devices by device ID."""
    registry = dr.async_get(hass)
    loaded = await async_save_and_load(hass, registry._store, registry._data_to_save())
    return {str(item.get("id")): item for item in _records(loaded, "devices")}
