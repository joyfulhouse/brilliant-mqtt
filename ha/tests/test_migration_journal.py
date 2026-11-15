"""Durable migration journal: allowed phases, storage round-trip, fail-closed parsing."""

from __future__ import annotations

import json
import stat
from pathlib import Path
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


def _write(path: Path, text: str) -> None:
    """Sync file helpers (kept out of async tests for ruff's ASYNC240 rule)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _file_facts(path: Path) -> tuple[bool, str | None, int | None, list[Path]]:
    if not path.exists():
        return False, None, None, []
    return (
        True,
        path.read_text(encoding="utf-8"),
        stat.S_IMODE(path.stat().st_mode),
        list(path.parent.glob("*.corrupt*")),
    )


async def test_journal_is_a_private_atomic_file_that_is_proven_and_removed(
    hass: HomeAssistant, hass_storage: dict[str, Any]
) -> None:
    """Real filesystem: not Home Assistant's (mocked) Store loader."""
    journal = MigrationJournal(hass)
    path = Path(journal.path)
    assert path.parent == Path(hass.config.path(".storage"))
    assert await journal.async_load() is None

    await journal.async_save(_record())
    assert MIGRATION_STORAGE_KEY not in hass_storage
    exists, text, mode, _corrupt = _file_facts(path)
    assert exists and text is not None
    raw = json.loads(text)
    assert raw["key"] == MIGRATION_STORAGE_KEY and raw["data"]["phase"] == "planned"
    assert mode == 0o600
    assert await MigrationJournal(hass).async_load() == _record()

    await journal.async_remove()
    assert _file_facts(path)[0] is False
    assert await journal.async_load() is None


@pytest.mark.parametrize(
    "content",
    ["", "{", '{"version": 1, "key": "brilliant_mqtt.migration", "data": ', "\x00\xff"],
)
async def test_corrupt_file_is_unreadable_never_absent_and_never_renamed(
    hass: HomeAssistant, content: str
) -> None:
    path = Path(MigrationJournal(hass).path)
    _write(path, content)

    with pytest.raises(MigrationJournalError, match="journal_unreadable"):
        await MigrationJournal(hass).async_load()
    exists, text, _mode, corrupt = _file_facts(path)
    assert exists and text == content
    assert corrupt == []


async def test_wrong_envelope_or_record_is_corrupt_and_redacted(hass: HomeAssistant) -> None:
    path = Path(MigrationJournal(hass).path)
    for envelope in (
        {"version": 2, "key": MIGRATION_STORAGE_KEY, "data": {}},
        {"version": 1, "key": "other", "data": {}},
        {"version": 1, "key": MIGRATION_STORAGE_KEY, "data": {"mqtt_password": SECRET}},
    ):
        _write(path, json.dumps(envelope))
        with pytest.raises(MigrationJournalError) as raised:
            await MigrationJournal(hass).async_load()
        assert raised.value.code == "journal_corrupt"
        assert SECRET not in str(raised.value)


def test_complete_requires_a_terminal_outcome() -> None:
    done = _record(MigrationPhase.REVERSING).advanced(MigrationPhase.COMPLETE, outcome="reverted")
    assert MigrationRecord.from_storage(done.to_storage()) == done

    missing = _record(MigrationPhase.REVERSING).advanced(MigrationPhase.COMPLETE).to_storage()
    with pytest.raises(MigrationJournalError, match="journal_corrupt"):
        MigrationRecord.from_storage(missing)
    bogus = done.to_storage() | {"outcome": "sideways"}
    with pytest.raises(MigrationJournalError, match="journal_corrupt"):
        MigrationRecord.from_storage(bogus)


async def test_failed_write_keeps_the_previous_record(hass: HomeAssistant) -> None:
    from unittest.mock import patch

    from custom_components.brilliant_mqtt import migration_journal

    journal = MigrationJournal(hass)
    await journal.async_save(_record())

    def failing(path: str, content: str) -> None:
        raise OSError("disk full")

    with (
        patch.object(migration_journal, "_write_file", failing),
        pytest.raises(MigrationJournalError, match="journal_write_failed"),
    ):
        await journal.async_save(_record().advanced(MigrationPhase.QUIESCED))
    stored = await MigrationJournal(hass).async_load()
    assert stored is not None and stored.phase is MigrationPhase.PLANNED


async def test_landed_write_with_failed_verify_read_is_unproven(hass: HomeAssistant) -> None:
    from unittest.mock import patch

    from custom_components.brilliant_mqtt import migration_journal

    journal = MigrationJournal(hass)

    def unreadable(path: str) -> object:
        raise MigrationJournalError("journal_unreadable")

    with (
        patch.object(migration_journal, "_read_file", unreadable),
        pytest.raises(MigrationJournalError, match="journal_unproven"),
    ):
        await journal.async_save(_record())
    # The write itself landed; a later read establishes it.
    assert await journal.async_load() == _record()


async def test_writes_are_refused_while_home_assistant_stops(hass: HomeAssistant) -> None:
    from homeassistant.core import CoreState

    hass.set_state(CoreState.stopping)
    try:
        with pytest.raises(MigrationJournalError, match="home_assistant_stopping"):
            await MigrationJournal(hass).async_save(_record())
    finally:
        hass.set_state(CoreState.running)
