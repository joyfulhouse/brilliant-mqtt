"""Durable state for one legacy-to-fleet migration transaction.

The journal is private Home Assistant Store data. It holds the complete
pre-change snapshot (entry data/options, entity and device associations) so a
restart can either reverse to the exact original legacy set (before the
``fleet_verified`` commit point) or finish removing siblings (at/after it). It
contains credentials, so it is never logged or exposed and is deleted after a
committed success or a verified reversal.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import Store

from .migration_persistence import (
    PersistenceUnprovenError,
    async_load_fresh,
    require_writable,
)

MIGRATION_STORAGE_VERSION = 1
MIGRATION_STORAGE_KEY = "brilliant_mqtt.migration"
MIGRATION_SCHEMA_VERSION = 1
OUTCOME_MIGRATED = "migrated"
OUTCOME_REVERTED = "reverted"


class MigrationPhase(StrEnum):
    """Exact durable migration phases."""

    PLANNED = "planned"
    QUIESCED = "quiesced"
    ANCHOR_CONVERTED = "anchor_converted"
    REGISTRIES_RETARGETED = "registries_retargeted"
    FLEET_VERIFIED = "fleet_verified"
    SIBLINGS_REMOVING = "siblings_removing"
    COMPLETE = "complete"
    REVERSING = "reversing"


# Phases strictly before the commit point: recovery reverses to the originals.
REVERSIBLE_PHASES = frozenset(
    {
        MigrationPhase.PLANNED,
        MigrationPhase.QUIESCED,
        MigrationPhase.ANCHOR_CONVERTED,
        MigrationPhase.REGISTRIES_RETARGETED,
        MigrationPhase.REVERSING,
    }
)
# Phases at/after the commit point: recovery only finishes sibling removal.
COMMITTED_PHASES = frozenset({MigrationPhase.FLEET_VERIFIED, MigrationPhase.SIBLINGS_REMOVING})

_TRANSITIONS: Mapping[MigrationPhase, frozenset[MigrationPhase]] = {
    MigrationPhase.PLANNED: frozenset({MigrationPhase.QUIESCED, MigrationPhase.REVERSING}),
    MigrationPhase.QUIESCED: frozenset({MigrationPhase.ANCHOR_CONVERTED, MigrationPhase.REVERSING}),
    MigrationPhase.ANCHOR_CONVERTED: frozenset(
        {MigrationPhase.REGISTRIES_RETARGETED, MigrationPhase.REVERSING}
    ),
    MigrationPhase.REGISTRIES_RETARGETED: frozenset(
        {MigrationPhase.FLEET_VERIFIED, MigrationPhase.REVERSING}
    ),
    MigrationPhase.FLEET_VERIFIED: frozenset({MigrationPhase.SIBLINGS_REMOVING}),
    MigrationPhase.SIBLINGS_REMOVING: frozenset(
        {MigrationPhase.SIBLINGS_REMOVING, MigrationPhase.COMPLETE}
    ),
    MigrationPhase.REVERSING: frozenset({MigrationPhase.REVERSING, MigrationPhase.COMPLETE}),
    MigrationPhase.COMPLETE: frozenset(),
}


class MigrationJournalError(ValueError):
    """One stable, value-free journal failure."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True, slots=True)
class EntrySnapshot:
    """The exact persisted shape of one candidate entry before migration."""

    entry_id: str
    title: str
    unique_id: str | None
    version: int
    minor_version: int
    data: Mapping[str, Any]
    options: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class SubentryPlan:
    """One journaled panel subentry with its pre-generated ID."""

    entry_id: str
    subentry_id: str
    title: str
    unique_id: str
    data: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class EntityAssociation:
    """One integration-owned entity registry entry before migration."""

    entity_id: str
    unique_id: str
    platform: str
    config_entry_id: str
    config_subentry_id: str | None


@dataclass(frozen=True, slots=True)
class DeviceAssociation:
    """Every config-entry/subentry association of one device before migration."""

    device_id: str
    associations: Mapping[str, tuple[str | None, ...]]


@dataclass(frozen=True, slots=True)
class MigrationRecord:
    """One complete, recoverable migration transaction."""

    transaction_id: str
    plan_digest: str
    phase: MigrationPhase
    anchor_entry_id: str
    candidates: tuple[str, ...]
    entries: tuple[EntrySnapshot, ...]
    fleet_title: str
    fleet_unique_id: str
    fleet_data: Mapping[str, Any]
    fleet_options: Mapping[str, Any]
    subentries: tuple[SubentryPlan, ...]
    entities: tuple[EntityAssociation, ...]
    devices: tuple[DeviceAssociation, ...]
    removed: tuple[str, ...] = ()
    last_error: str | None = None
    # Terminal intent recorded with ``complete`` so a crash during finalization
    # (before the journal is deleted) reconciles in the same direction.
    outcome: str | None = None

    def __repr__(self) -> str:
        return (
            f"MigrationRecord(transaction_id={self.transaction_id!r}, "
            f"phase={self.phase.value!r}, candidates={len(self.candidates)})"
        )

    @property
    def siblings(self) -> tuple[str, ...]:
        """Candidates removed after verification, in recorded order."""
        return tuple(entry_id for entry_id in self.candidates if entry_id != self.anchor_entry_id)

    @property
    def subentry_by_entry(self) -> Mapping[str, str]:
        """Map each legacy entry ID to its planned panel subentry ID."""
        return {plan.entry_id: plan.subentry_id for plan in self.subentries}

    def entry(self, entry_id: str) -> EntrySnapshot:
        """Return one candidate's original snapshot."""
        for snapshot in self.entries:
            if snapshot.entry_id == entry_id:
                return snapshot
        raise MigrationJournalError("journal_entry_missing")

    def advanced(self, phase: MigrationPhase, **changes: Any) -> MigrationRecord:
        """Return this record in its next allowed phase."""
        if phase not in _TRANSITIONS[self.phase]:
            raise MigrationJournalError("invalid_migration_transition")
        return replace(self, phase=phase, **changes)

    def to_storage(self) -> dict[str, Any]:
        """Serialize to plain JSON-compatible data."""
        return {
            "schema_version": MIGRATION_SCHEMA_VERSION,
            "transaction_id": self.transaction_id,
            "plan_digest": self.plan_digest,
            "phase": self.phase.value,
            "anchor_entry_id": self.anchor_entry_id,
            "candidates": list(self.candidates),
            "entries": [
                {
                    "entry_id": item.entry_id,
                    "title": item.title,
                    "unique_id": item.unique_id,
                    "version": item.version,
                    "minor_version": item.minor_version,
                    "data": copy.deepcopy(dict(item.data)),
                    "options": copy.deepcopy(dict(item.options)),
                }
                for item in self.entries
            ],
            "fleet": {
                "title": self.fleet_title,
                "unique_id": self.fleet_unique_id,
                "data": copy.deepcopy(dict(self.fleet_data)),
                "options": copy.deepcopy(dict(self.fleet_options)),
            },
            "subentries": [
                {
                    "entry_id": item.entry_id,
                    "subentry_id": item.subentry_id,
                    "title": item.title,
                    "unique_id": item.unique_id,
                    "data": copy.deepcopy(dict(item.data)),
                }
                for item in self.subentries
            ],
            "entities": [
                {
                    "entity_id": item.entity_id,
                    "unique_id": item.unique_id,
                    "platform": item.platform,
                    "config_entry_id": item.config_entry_id,
                    "config_subentry_id": item.config_subentry_id,
                }
                for item in self.entities
            ],
            "devices": [
                {
                    "device_id": item.device_id,
                    "associations": {
                        entry_id: list(subentry_ids)
                        for entry_id, subentry_ids in sorted(item.associations.items())
                    },
                }
                for item in self.devices
            ],
            "removed": list(self.removed),
            "last_error": self.last_error,
            "outcome": self.outcome,
        }

    @classmethod
    def from_storage(cls, raw: object) -> MigrationRecord:
        """Parse persisted data, failing closed without exposing any value."""
        try:
            if not isinstance(raw, Mapping) or raw["schema_version"] != MIGRATION_SCHEMA_VERSION:
                raise MigrationJournalError("journal_corrupt")
            fleet = raw["fleet"]
            record = cls(
                transaction_id=_string(raw["transaction_id"]),
                plan_digest=_string(raw["plan_digest"]),
                phase=MigrationPhase(raw["phase"]),
                anchor_entry_id=_string(raw["anchor_entry_id"]),
                candidates=tuple(_string(item) for item in raw["candidates"]),
                entries=tuple(
                    EntrySnapshot(
                        entry_id=_string(item["entry_id"]),
                        title=_any_string(item["title"]),
                        unique_id=_optional_string(item["unique_id"]),
                        version=_integer(item["version"]),
                        minor_version=_integer(item["minor_version"]),
                        data=_mapping(item["data"]),
                        options=_mapping(item["options"]),
                    )
                    for item in raw["entries"]
                ),
                fleet_title=_string(fleet["title"]),
                fleet_unique_id=_string(fleet["unique_id"]),
                fleet_data=_mapping(fleet["data"]),
                fleet_options=_mapping(fleet["options"]),
                subentries=tuple(
                    SubentryPlan(
                        entry_id=_string(item["entry_id"]),
                        subentry_id=_string(item["subentry_id"]),
                        title=_string(item["title"]),
                        unique_id=_string(item["unique_id"]),
                        data=_mapping(item["data"]),
                    )
                    for item in raw["subentries"]
                ),
                entities=tuple(
                    EntityAssociation(
                        entity_id=_string(item["entity_id"]),
                        unique_id=_string(item["unique_id"]),
                        platform=_string(item["platform"]),
                        config_entry_id=_string(item["config_entry_id"]),
                        config_subentry_id=_optional_string(item["config_subentry_id"]),
                    )
                    for item in raw["entities"]
                ),
                devices=tuple(
                    DeviceAssociation(
                        device_id=_string(item["device_id"]),
                        associations={
                            _string(entry_id): tuple(
                                _optional_string(subentry_id) for subentry_id in subentry_ids
                            )
                            for entry_id, subentry_ids in item["associations"].items()
                        },
                    )
                    for item in raw["devices"]
                ),
                removed=tuple(_string(item) for item in raw["removed"]),
                last_error=_optional_string(raw["last_error"]),
                outcome=_optional_string(raw["outcome"]),
            )
        except MigrationJournalError:
            raise
        except Exception:
            raise MigrationJournalError("journal_corrupt") from None
        if (
            record.anchor_entry_id not in record.candidates
            or {item.entry_id for item in record.entries} != set(record.candidates)
            or {item.entry_id for item in record.subentries} != set(record.candidates)
            or record.outcome not in {None, OUTCOME_MIGRATED, OUTCOME_REVERTED}
            or (record.phase is MigrationPhase.COMPLETE) != (record.outcome is not None)
        ):
            raise MigrationJournalError("journal_corrupt")
        return record


def _string(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise MigrationJournalError("journal_corrupt")
    return value


def _any_string(value: object) -> str:
    if not isinstance(value, str):
        raise MigrationJournalError("journal_corrupt")
    return value


def _optional_string(value: object) -> str | None:
    return None if value is None else _string(value)


def _integer(value: object) -> int:
    if type(value) is not int:
        raise MigrationJournalError("journal_corrupt")
    return value


def _mapping(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise MigrationJournalError("journal_corrupt")
    return copy.deepcopy(dict(value))


class MigrationJournal:
    """Persist exactly one migration record in private HA storage.

    Every write is proven by re-reading the file: ``Store.async_save`` swallows
    write errors and defers while Home Assistant is stopping.
    """

    __slots__ = ("_hass", "_store")

    def __init__(self, hass: HomeAssistant) -> None:
        self._hass = hass
        self._store: Store[dict[str, Any]] = Store(
            hass,
            MIGRATION_STORAGE_VERSION,
            MIGRATION_STORAGE_KEY,
            atomic_writes=True,
            private=True,
        )

    async def async_load(self) -> MigrationRecord | None:
        """Return the durable record (read from disk), or None when none is pending."""
        try:
            raw = await async_load_fresh(self._hass, self._store)
        except PersistenceUnprovenError:
            raise MigrationJournalError("journal_unreadable") from None
        if raw is None:
            return None
        return MigrationRecord.from_storage(raw)

    async def async_save(self, record: MigrationRecord) -> None:
        """Write one record and prove the exact transaction and phase are on disk."""
        expected = record.to_storage()
        try:
            require_writable(self._hass)
            await self._store.async_save(expected)
            stored = await async_load_fresh(self._hass, self._store)
        except PersistenceUnprovenError:
            raise MigrationJournalError("journal_unproven") from None
        if stored != expected:
            raise MigrationJournalError("journal_unproven")

    async def async_remove(self) -> None:
        """Delete the record (and its credentials) and prove it is gone."""
        try:
            await self._store.async_remove()
            stored = await async_load_fresh(self._hass, self._store)
        except (OSError, PersistenceUnprovenError):
            raise MigrationJournalError("journal_unproven") from None
        if stored is not None:
            raise MigrationJournalError("journal_unproven")
