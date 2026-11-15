"""Durable migration journal: allowed phases, storage round-trip, fail-closed parsing."""

from __future__ import annotations

from typing import Any

import pytest
from homeassistant.core import HomeAssistant

from custom_components.brilliant_mqtt.migration_journal import (
    COMMITTED_PHASES,
    MIGRATION_STORAGE_KEY,
    REVERSIBLE_PHASES,
    DeviceAssociation,
    EntityAssociation,
    EntrySnapshot,
    MigrationJournal,
    MigrationJournalError,
    MigrationPhase,
    MigrationRecord,
    SubentryPlan,
)

SECRET = "journal-root-secret"


def _record(phase: MigrationPhase = MigrationPhase.PLANNED) -> MigrationRecord:
    return MigrationRecord(
        transaction_id="00000000-0000-4000-8000-000000000001",
        plan_digest="digest",
        phase=phase,
        anchor_entry_id="a",
        candidates=("a", "b"),
        entries=tuple(
            EntrySnapshot(
                entry_id=entry_id,
                title=f"Brilliant {entry_id}",
                unique_id=entry_id,
                version=4,
                minor_version=1,
                data={"panel": entry_id, "root_password": SECRET},
                options={"auto_repair": True},
            )
            for entry_id in ("a", "b")
        ),
        fleet_title="Brilliant MQTT",
        fleet_unique_id="brilliant_mqtt_fleet",
        fleet_data={"entry_kind": "fleet", "mqtt_password": SECRET},
        fleet_options={"auto_repair": True},
        subentries=tuple(
            SubentryPlan(
                entry_id=entry_id,
                subentry_id=f"sub-{entry_id}",
                title=f"Brilliant {entry_id}",
                unique_id=f"SHA256:{entry_id}",
                data={"panel": entry_id},
            )
            for entry_id in ("a", "b")
        ),
        entities=(
            EntityAssociation(
                entity_id="button.a_repair",
                unique_id="a_repair_bridge",
                platform="brilliant_mqtt",
                config_entry_id="a",
                config_subentry_id=None,
            ),
        ),
        devices=(DeviceAssociation(device_id="dev", associations={"a": (None,), "mqtt": (None,)}),),
    )


_ALLOWED = {
    (MigrationPhase.PLANNED, MigrationPhase.QUIESCED),
    (MigrationPhase.QUIESCED, MigrationPhase.ANCHOR_CONVERTED),
    (MigrationPhase.ANCHOR_CONVERTED, MigrationPhase.REGISTRIES_RETARGETED),
    (MigrationPhase.REGISTRIES_RETARGETED, MigrationPhase.FLEET_VERIFIED),
    (MigrationPhase.FLEET_VERIFIED, MigrationPhase.SIBLINGS_REMOVING),
    (MigrationPhase.SIBLINGS_REMOVING, MigrationPhase.SIBLINGS_REMOVING),
    (MigrationPhase.SIBLINGS_REMOVING, MigrationPhase.COMPLETE),
    (MigrationPhase.PLANNED, MigrationPhase.REVERSING),
    (MigrationPhase.QUIESCED, MigrationPhase.REVERSING),
    (MigrationPhase.ANCHOR_CONVERTED, MigrationPhase.REVERSING),
    (MigrationPhase.REGISTRIES_RETARGETED, MigrationPhase.REVERSING),
    (MigrationPhase.REVERSING, MigrationPhase.REVERSING),
    (MigrationPhase.REVERSING, MigrationPhase.COMPLETE),
}


@pytest.mark.parametrize("source", list(MigrationPhase))
@pytest.mark.parametrize("target", list(MigrationPhase))
def test_only_the_documented_transitions_are_allowed(
    source: MigrationPhase, target: MigrationPhase
) -> None:
    record = _record(source)
    if (source, target) in _ALLOWED:
        assert record.advanced(target).phase is target
    else:
        with pytest.raises(MigrationJournalError, match="invalid_migration_transition"):
            record.advanced(target)


def test_commit_point_partitions_recovery() -> None:
    assert MigrationPhase.FLEET_VERIFIED in COMMITTED_PHASES
    assert MigrationPhase.FLEET_VERIFIED not in REVERSIBLE_PHASES
    assert not COMMITTED_PHASES & REVERSIBLE_PHASES
    # Past the commit point there is no path back to reversal.
    with pytest.raises(MigrationJournalError):
        _record(MigrationPhase.FLEET_VERIFIED).advanced(MigrationPhase.REVERSING)


def test_record_round_trips_and_repr_is_redacted() -> None:
    record = _record().advanced(MigrationPhase.QUIESCED, last_error="x")

    assert MigrationRecord.from_storage(record.to_storage()) == record
    assert SECRET not in repr(record)
    assert record.siblings == ("b",)
    assert record.subentry_by_entry == {"a": "sub-a", "b": "sub-b"}
    assert record.entry("b").data["panel"] == "b"
    with pytest.raises(MigrationJournalError):
        record.entry("missing")


@pytest.mark.parametrize(
    "mutate",
    [
        lambda raw: raw.update(schema_version=2),
        lambda raw: raw.update(phase="exploded"),
        lambda raw: raw.update(anchor_entry_id="z"),
        lambda raw: raw["entries"].pop(),
        lambda raw: raw["subentries"].pop(),
        lambda raw: raw["entities"][0].update(entity_id=7),
        lambda raw: raw.pop("fleet"),
    ],
)
def test_malformed_storage_fails_closed(mutate: Any) -> None:
    raw = _record().to_storage()
    mutate(raw)

    with pytest.raises(MigrationJournalError, match="journal_corrupt"):
        MigrationRecord.from_storage(raw)


async def test_journal_persists_privately_and_removes(
    hass: HomeAssistant, hass_storage: dict[str, Any]
) -> None:
    journal = MigrationJournal(hass)
    assert await journal.async_load() is None

    await journal.async_save(_record())
    assert hass_storage[MIGRATION_STORAGE_KEY]["data"]["phase"] == "planned"
    assert await MigrationJournal(hass).async_load() == _record()

    await journal.async_remove()
    assert MIGRATION_STORAGE_KEY not in hass_storage
    assert await journal.async_load() is None


async def test_corrupt_store_raises_redacted_error(
    hass: HomeAssistant, hass_storage: dict[str, Any]
) -> None:
    hass_storage[MIGRATION_STORAGE_KEY] = {
        "version": 1,
        "minor_version": 1,
        "key": MIGRATION_STORAGE_KEY,
        "data": {"schema_version": 1, "mqtt_password": SECRET},
    }

    with pytest.raises(MigrationJournalError) as raised:
        await MigrationJournal(hass).async_load()
    assert SECRET not in str(raised.value)
