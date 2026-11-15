"""Recoverable legacy-to-fleet migration against the real HA entry/registry stack.

Restarts are modelled from what is actually on "disk" (``hass_storage`` for Home
Assistant's lazily written stores, plus the journal's real file): the in-memory
Brilliant entries and both registries are discarded and rebuilt from the
persisted stores, so any divergence between memory and disk is visible. A
"crash" restarts from the stores as they are; a clean restart first performs
Home Assistant's final write.
"""

from __future__ import annotations

import asyncio
import copy
import itertools
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from datetime import datetime, timedelta
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast
from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.config_entries import ConfigEntry, ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers import storage
from homeassistant.util import dt as dt_util
from homeassistant.util.file import WriteError
from pytest_homeassistant_custom_component.common import (
    MockConfigEntry,
    async_fire_time_changed,
)
from pytest_homeassistant_custom_component.typing import MqttMockHAClient

from custom_components import brilliant_mqtt
from custom_components.brilliant_mqtt import fleet_manager, migration_journal
from custom_components.brilliant_mqtt.const import (
    CONFIG_ENTRY_VERSION,
    DOMAIN,
    ENTRY_KIND_FLEET,
    FLEET_UNIQUE_ID,
    SUBENTRY_TYPE_PANEL,
)
from custom_components.brilliant_mqtt.fleet_manager import FleetManager
from custom_components.brilliant_mqtt.manager import PanelManager
from custom_components.brilliant_mqtt.migration import planned_subentry_id
from custom_components.brilliant_mqtt.migration_coordinator import (
    ISSUE_MIGRATION_AVAILABLE,
    ISSUE_MIGRATION_RECOVERY_FAILED,
    ISSUE_MIGRATION_REVERTED_NOT_LOADED,
    SINGLE_OWNER_DEVICES,
    MigrationCoordinator,
    device_links,
    migration_fence,
    migration_removing,
)
from custom_components.brilliant_mqtt.migration_journal import (
    MigrationJournal,
    MigrationJournalError,
    MigrationPhase,
    MigrationRecord,
)
from tests.test_migration import (
    FLEET_PASSWORD,
    FLEET_USERNAME,
    ROOT_PASSWORD,
    host_key,
    legacy_data,
    legacy_options,
)

pytestmark = pytest.mark.allow_lingering_timers

BEDROOM_ID = "legacy-bedroom"
KITCHEN_ID = "legacy-kitchen"
OFFICE_ID = "legacy-office"
ANCHOR_ID = BEDROOM_ID
SIBLINGS = (KITCHEN_ID, OFFICE_ID)
PANELS = {BEDROOM_ID: "bedroom", KITCHEN_ID: "kitchen", OFFICE_ID: "office"}

type State = dict[str, Any]


class SimulatedCrash(BaseException):
    """Models Home Assistant stopping mid-migration (no in-process handling)."""


class InjectedFailure(Exception):
    """Models an ordinary in-process failure."""


@pytest.fixture(autouse=True)
def no_panel_contact() -> Iterator[AsyncMock]:
    """No SSH ever; the one setup-time panel contact (HA-mirror audit) is counted."""
    audit = AsyncMock()
    with (
        patch("asyncssh.connect", side_effect=AssertionError("panel contacted")),
        patch.object(PanelManager, "async_retire_legacy_ha_mirror", audit),
    ):
        yield audit


@pytest.fixture(autouse=True)
def persistent_registries(hass: HomeAssistant, hass_storage: dict[str, Any]) -> None:
    """Give the registries real (mock-disk) stores; the harness's never write.

    Production registries persist; without this the "disk" would never hold an
    entity or device association and divergence could not be observed.
    """
    del hass_storage
    for registry in (er.async_get(hass), dr.async_get(hass)):
        old = registry._store
        registry._store = storage.Store(
            hass, old.version, old.key, atomic_writes=True, minor_version=old.minor_version
        )


@pytest.fixture
def failing_writes(hass_storage: dict[str, Any]) -> Iterator[set[str]]:
    """Store keys whose writes fail exactly like a full disk (logged, swallowed)."""
    del hass_storage
    failing: set[str] = set()
    original = storage.Store._async_write_data

    async def write(store: storage.Store[Any], data: dict[str, Any]) -> None:
        if store.key in failing:
            raise WriteError("injected write failure")
        await original(store, data)

    with patch.object(storage.Store, "_async_write_data", write):
        yield failing


def _entry(entry_id: str, mesh_priority: int, **overrides: Any) -> MockConfigEntry:
    slug = PANELS[entry_id]
    return MockConfigEntry(
        domain=DOMAIN,
        entry_id=entry_id,
        title=f"Brilliant {slug}",
        unique_id=slug,
        version=CONFIG_ENTRY_VERSION,
        data=legacy_data(slug, f"192.0.2.{10 + mesh_priority}", mesh_priority, **overrides),
        options=legacy_options(),
    )


async def _flush_disk(hass: HomeAssistant) -> None:
    """Model Home Assistant's final write on a clean stop (test-only internals)."""
    manager = hass.config_entries
    await manager._store.async_save(manager._data_to_save())
    entity_registry = er.async_get(hass)
    await entity_registry._store.async_save(entity_registry._data_to_save())
    device_registry = dr.async_get(hass)
    await device_registry._store.async_save(device_registry._data_to_save())


class JournalFaults:
    """Make the journal's own file writes or reads fail on demand."""

    def __init__(self) -> None:
        self.write_fails = False
        self.read_fails = False


@pytest.fixture
def journal_faults() -> Iterator[JournalFaults]:
    faults = JournalFaults()
    write = migration_journal._write_file
    read = migration_journal._read_file

    def failing_write(path: str, content: str) -> None:
        if faults.write_fails:
            raise OSError("injected journal write failure")
        write(path, content)

    def failing_read(path: str) -> object:
        if faults.read_fails:
            raise MigrationJournalError("journal_unreadable")
        return read(path)

    with (
        patch.object(migration_journal, "_write_file", failing_write),
        patch.object(migration_journal, "_read_file", failing_read),
    ):
        yield faults


async def _journal(hass: HomeAssistant) -> MigrationRecord | None:
    return await MigrationJournal(hass).async_load()


async def _drive_setup_retries(hass: HomeAssistant, entry_id: str) -> None:
    """Advance time until HA's own (backed-off) setup retry has run."""
    for step in range(1, 7):
        async_fire_time_changed(hass, dt_util.utcnow() + timedelta(minutes=10 * step))
        await hass.async_block_till_done()
        if hass.config_entries.async_get_known_entry(entry_id).state is ConfigEntryState.LOADED:
            return


def _journal_exists(hass: HomeAssistant) -> bool:
    return Path(MigrationJournal(hass).path).exists()


def _write_journal_text(hass: HomeAssistant, text: str) -> None:
    """Sync file helper (kept out of async tests for ruff's ASYNC240 rule)."""
    path = Path(MigrationJournal(hass).path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _read_journal_text(hass: HomeAssistant) -> str:
    return Path(MigrationJournal(hass).path).read_text(encoding="utf-8")


def _remove_journal_file(hass: HomeAssistant) -> None:
    Path(MigrationJournal(hass).path).unlink()


async def _setup_legacy(hass: HomeAssistant, *entries: MockConfigEntry) -> list[MockConfigEntry]:
    """Load legacy entries over MQTT-discovery devices, customize entities, flush."""
    mqtt_entry = hass.config_entries.async_entries("mqtt")[0]
    device_registry = dr.async_get(hass)
    entity_registry = er.async_get(hass)
    for entry in entries:
        panel = entry.data["panel"]
        device = device_registry.async_get_or_create(
            config_entry_id=mqtt_entry.entry_id,
            identifiers={("mqtt", f"brilliant_panel_{panel}")},
            name=f"Brilliant {panel.title()}",
        )
        entity_registry.async_get_or_create(
            "light", "mqtt", f"{panel}_light_1", config_entry=mqtt_entry, device_id=device.id
        )
        entry.add_to_hass(hass)
    for entry in entries:
        if entry.state is ConfigEntryState.NOT_LOADED:
            assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    assert all(entry.state is ConfigEntryState.LOADED for entry in entries)

    entity_ids = {entry.entry_id for entry in entries}
    if KITCHEN_ID in entity_ids:
        repair = entity_registry.async_get_entity_id(
            "button", DOMAIN, f"{KITCHEN_ID}_repair_bridge"
        )
        assert repair is not None
        entity_registry.async_update_entity(
            repair,
            disabled_by=er.RegistryEntryDisabler.USER,
            new_entity_id="button.kitchen_custom_repair",
        )
    if OFFICE_ID in entity_ids:
        health = entity_registry.async_get_entity_id(
            "binary_sensor", DOMAIN, f"{OFFICE_ID}_bridge_health"
        )
        assert health is not None
        entity_registry.async_update_entity(health, hidden_by=er.RegistryEntryHider.USER)
    await hass.async_block_till_done()
    await _flush_disk(hass)
    return list(entries)


async def _unload_all(hass: HomeAssistant) -> None:
    for entry in hass.config_entries.async_entries(DOMAIN):
        if entry.state is not ConfigEntryState.NOT_LOADED and entry.state.recoverable:
            await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()


@pytest.fixture
async def legacy_fleet(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    payload_dir: Path,
) -> AsyncIterator[list[MockConfigEntry]]:
    """Three legacy panels; Office carries the production HA-mirror history marker."""
    del mqtt_mock, payload_dir
    entries = await _setup_legacy(
        hass,
        _entry(KITCHEN_ID, 1),
        _entry(
            OFFICE_ID,
            2,
            hot_poll_seconds=5,
            resync_seconds=900,
            ha_mirror_retire_verified=True,
            last_firmware="v26.10.21.1",
        ),
        _entry(BEDROOM_ID, 3),
    )
    yield entries
    await _unload_all(hass)


def _entity_view(
    unique_id: object,
    platform: object,
    owner: object,
    subentry: object,
    device: object,
    disabled: object,
    hidden: object,
) -> tuple[object, ...]:
    return (
        unique_id,
        platform,
        owner,
        subentry,
        device,
        None if disabled is None else str(disabled),
        None if hidden is None else str(hidden),
    )


def _all_devices(registry: dr.DeviceRegistry) -> list[dr.DeviceEntry]:
    """Every device, on both registry models (2026.10+ iterates entries, not IDs)."""
    devices: list[dr.DeviceEntry] = []
    for item in list(cast(Any, registry.devices)):
        device = registry.async_get(item) if isinstance(item, str) else item
        if device is not None:
            devices.append(device)
    return devices


def _raw_device_links(raw: dict[str, Any]) -> dict[str, set[str | None]]:
    if SINGLE_OWNER_DEVICES and "config_entry_id" in raw:
        owner = raw["config_entry_id"]
        return {owner: {raw.get("config_subentry_id")}} if owner else {}
    return {owner: set(subs) for owner, subs in raw["config_entries_subentries"].items()}


def _state(hass: HomeAssistant) -> State:
    """Every in-memory fact migration may touch, in comparable form."""
    entity_registry = er.async_get(hass)
    device_registry = dr.async_get(hass)
    return {
        "entries": {
            entry.entry_id: {
                "title": entry.title,
                "unique_id": entry.unique_id,
                "version": entry.version,
                "data": json.loads(json.dumps(dict(entry.data))),
                "options": json.loads(json.dumps(dict(entry.options))),
                "subentries": {
                    subentry_id: json.loads(
                        json.dumps([subentry.title, subentry.unique_id, dict(subentry.data)])
                    )
                    for subentry_id, subentry in entry.subentries.items()
                },
            }
            for entry in hass.config_entries.async_entries(DOMAIN)
        },
        "entities": {
            item.entity_id: _entity_view(
                item.unique_id,
                item.platform,
                item.config_entry_id,
                item.config_subentry_id,
                item.device_id,
                item.disabled_by,
                item.hidden_by,
            )
            for item in entity_registry.entities.values()
            if item.platform in {DOMAIN, "mqtt"}
        },
        "devices": {device.id: device_links(device) for device in _all_devices(device_registry)},
    }


def _disk_state(disk: dict[str, Any]) -> State:
    """The same view, read only from the persisted stores."""
    entries = disk["core.config_entries"]["data"]["entries"]
    entities = disk["core.entity_registry"]["data"]["entities"]
    devices = disk["core.device_registry"]["data"]["devices"]
    return {
        "entries": {
            raw["entry_id"]: {
                "title": raw["title"],
                "unique_id": raw["unique_id"],
                "version": raw["version"],
                "data": raw["data"],
                "options": raw["options"],
                "subentries": {
                    sub["subentry_id"]: [sub["title"], sub["unique_id"], sub["data"]]
                    for sub in raw["subentries"]
                },
            }
            for raw in entries
            if raw["domain"] == DOMAIN
        },
        "entities": {
            raw["entity_id"]: _entity_view(
                raw["unique_id"],
                raw["platform"],
                raw["config_entry_id"],
                raw["config_subentry_id"],
                raw["device_id"],
                raw["disabled_by"],
                raw["hidden_by"],
            )
            for raw in entities
            if raw["platform"] in {DOMAIN, "mqtt"}
        },
        "devices": {raw["id"]: _raw_device_links(raw) for raw in devices},
    }


async def _restart_from_disk(
    hass: HomeAssistant,
    hass_storage: dict[str, Any],
    disk: dict[str, Any],
    *,
    recover: Callable[[], Awaitable[Any]] | None = None,
) -> None:
    """Model a process restart: only what ``disk`` holds survives.

    The Brilliant entries and both registries are rebuilt from the persisted
    stores, all Brilliant runtime state is discarded, then the integration's
    ``async_setup`` (startup recovery + fence) runs before HA sets up entries.
    """
    await _unload_all(hass)
    hass_storage.clear()
    hass_storage.update(copy.deepcopy(disk))
    manager = hass.config_entries
    for entry in list(manager.async_entries(DOMAIN)):
        entry.async_cancel_retry_setup()
        del manager._entries[entry.entry_id]
    for raw in disk["core.config_entries"]["data"]["entries"]:
        if raw["domain"] != DOMAIN:
            continue
        manager._entries[raw["entry_id"]] = ConfigEntry(
            created_at=datetime.fromisoformat(raw["created_at"]),
            data=raw["data"],
            disabled_by=None,
            discovery_keys=MappingProxyType({}),
            domain=raw["domain"],
            entry_id=raw["entry_id"],
            minor_version=raw["minor_version"],
            modified_at=datetime.fromisoformat(raw["modified_at"]),
            options=raw["options"],
            pref_disable_new_entities=raw["pref_disable_new_entities"],
            pref_disable_polling=raw["pref_disable_polling"],
            source=raw["source"],
            subentries_data=raw["subentries"],
            title=raw["title"],
            unique_id=raw["unique_id"],
            version=raw["version"],
        )
    hass.data.pop(er.DATA_REGISTRY)
    cast(Any, er.async_get).cache_clear()  # singleton is lru-cached per process
    hass.data.pop(dr.DATA_REGISTRY)
    dr.async_setup(hass)
    await dr.async_load(hass)
    await er.async_load(hass)
    hass.data.pop(DOMAIN, None)
    if recover is None:
        assert await brilliant_mqtt.async_setup(hass, {})
    else:
        await recover()
    for entry in manager.async_entries(DOMAIN):
        if entry.state is ConfigEntryState.NOT_LOADED:
            await manager.async_setup(entry.entry_id)
    await hass.async_block_till_done()


async def _run(
    hass: HomeAssistant,
    *,
    dry_run: bool,
    checkpoint: Callable[[str], Awaitable[None]] | None = None,
) -> dict[str, Any]:
    return await MigrationCoordinator(hass, checkpoint=checkpoint).async_run(dry_run=dry_run)


def _inject(point: str, error: BaseException) -> Callable[[str], Awaitable[None]]:
    fired = False

    async def checkpoint(name: str) -> None:
        nonlocal fired
        if name == point and not fired:
            fired = True
            raise error

    return checkpoint


def _fleet_managers(hass: HomeAssistant) -> list[FleetManager]:
    return [
        entry.runtime_data
        for entry in hass.config_entries.async_entries(DOMAIN)
        if entry.state is ConfigEntryState.LOADED and isinstance(entry.runtime_data, FleetManager)
    ]


def _check_migrated(
    hass: HomeAssistant,
    before: State,
    *,
    panels: dict[str, str] = PANELS,
    anchor_id: str = ANCHOR_ID,
    office_cadences: bool = True,
) -> ConfigEntry[Any]:
    entries = hass.config_entries.async_entries(DOMAIN)
    assert [entry.entry_id for entry in entries] == [anchor_id]
    fleet = entries[0]
    assert fleet.unique_id == FLEET_UNIQUE_ID
    assert fleet.title == "Brilliant MQTT"
    assert fleet.version == CONFIG_ENTRY_VERSION
    assert fleet.data["entry_kind"] == ENTRY_KIND_FLEET
    assert "panel" not in fleet.data and "root_password" not in fleet.data
    planned = {entry_id: planned_subentry_id(entry_id) for entry_id in panels}
    assert set(fleet.subentries) == set(planned.values())
    for entry_id, subentry_id in planned.items():
        subentry = fleet.subentries[subentry_id]
        assert subentry.subentry_type == SUBENTRY_TYPE_PANEL
        assert subentry.title == f"Brilliant {panels[entry_id]}"
        assert subentry.data["management_id"] == entry_id
        assert subentry.data["panel"] == panels[entry_id]
        assert subentry.unique_id == subentry.data["identity_fingerprint"]
        assert "last_firmware" not in subentry.data
        assert "ha_mirror_retire_verified" not in subentry.data
    if office_cadences:
        office = fleet.subentries[planned[OFFICE_ID]].data
        assert office["hot_poll_seconds"] == 5 and office["resync_seconds"] == 900

    after = _state(hass)
    domain_before = {k: v for k, v in before["entities"].items() if v[1] == DOMAIN}
    domain_after = {k: v for k, v in after["entities"].items() if v[1] == DOMAIN}
    assert set(domain_after) == set(domain_before)
    for entity_id, (
        unique_id,
        platform,
        owner,
        _sub,
        device,
        disabled,
        hidden,
    ) in domain_before.items():
        assert domain_after[entity_id] == (
            unique_id,
            platform,
            anchor_id,
            planned[str(owner)],
            device,
            disabled,
            hidden,
        )
    assert {k: v for k, v in after["entities"].items() if v[1] == "mqtt"} == {
        k: v for k, v in before["entities"].items() if v[1] == "mqtt"
    }
    for device_id, links in before["devices"].items():
        legacy_owner = next((owner for owner in links if owner in panels), None)
        if legacy_owner is None:
            assert after["devices"][device_id] == links
            continue
        # The MQTT association (shared device up to HA 2026.9; a separate MQTT
        # device from 2026.10) is untouched; only the legacy owner moves.
        assert after["devices"][device_id] == {
            **{owner: subs for owner, subs in links.items() if owner not in panels},
            anchor_id: {planned[legacy_owner]},
        }

    assert fleet.state is ConfigEntryState.LOADED
    runtime = fleet.runtime_data
    assert isinstance(runtime, FleetManager)
    assert runtime.runtime_owned_panel_ids == frozenset(planned.values())
    assert {manager.management_id for manager in runtime.panels.values()} == set(panels)
    assert len(_fleet_managers(hass)) == 1
    assert migration_fence(hass).active is False
    return fleet


async def _clean_restart(hass: HomeAssistant, hass_storage: dict[str, Any]) -> None:
    await _flush_disk(hass)
    await _restart_from_disk(hass, hass_storage, copy.deepcopy(hass_storage))


async def _settle(hass: HomeAssistant, hass_storage: dict[str, Any]) -> None:
    """Clean restart(s) until a later startup has confirmed and deleted the journal."""
    await _clean_restart(hass, hass_storage)
    for _ in range(2):
        if not _journal_exists(hass):
            return
        await _clean_restart(hass, hass_storage)
    assert not _journal_exists(hass)


async def _assert_migrated(
    hass: HomeAssistant,
    before: State,
    hass_storage: dict[str, Any],
    **kwargs: Any,
) -> ConfigEntry[Any]:
    _check_migrated(hass, before, **kwargs)
    record = await _journal(hass)
    assert record is None or (
        record.phase is MigrationPhase.COMPLETE and record.outcome == "migrated"
    )
    await _settle(hass, hass_storage)
    fleet = _check_migrated(hass, before, **kwargs)
    # Durable: after Home Assistant reloaded from disk, disk == memory == target.
    assert _disk_state(hass_storage) == _state(hass)
    return fleet


def _check_originals(hass: HomeAssistant, before: State) -> None:
    assert _state(hass) == before
    assert migration_fence(hass).active is False
    for entry in hass.config_entries.async_entries(DOMAIN):
        assert entry.state is ConfigEntryState.LOADED
        runtime = entry.runtime_data
        assert isinstance(runtime, FleetManager)
        assert set(runtime.panels) == {entry.entry_id}


async def _assert_originals(
    hass: HomeAssistant, before: State, hass_storage: dict[str, Any]
) -> None:
    _check_originals(hass, before)
    record = await _journal(hass)
    assert (
        record is None
        or record.outcome == "reverted"
        or record.phase
        in {
            MigrationPhase.PLANNED,
            MigrationPhase.QUIESCED,
            MigrationPhase.ANCHOR_CONVERTED,
            MigrationPhase.REGISTRIES_RETARGETED,
            MigrationPhase.REVERSING,
        }
    )
    await _settle(hass, hass_storage)
    _check_originals(hass, before)
    assert _disk_state(hass_storage) == before


# --------------------------------------------------------------------------- happy path


async def test_full_migration_preserves_every_identity_and_is_idempotent(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
    no_panel_contact: AsyncMock,
) -> None:
    del legacy_fleet
    before = _state(hass)
    assert _disk_state(hass_storage) == before
    audits = no_panel_contact.await_count

    outcome = await hass.services.async_call(
        DOMAIN, "migrate_to_fleet", {"dry_run": False}, blocking=True, return_response=True
    )
    await hass.async_block_till_done()

    assert outcome is not None
    assert outcome["status"] == "migrated"
    assert outcome["fleet_entry_id"] == ANCHOR_ID
    assert outcome["removed_entry_ids"] == list(SIBLINGS)
    await _assert_migrated(hass, before, hass_storage)
    assert no_panel_contact.await_count == audits
    rendered = json.dumps(outcome)
    for secret in (FLEET_PASSWORD, FLEET_USERNAME, ROOT_PASSWORD, host_key("kitchen").split()[1]):
        assert secret not in rendered

    again = await hass.services.async_call(
        DOMAIN, "migrate_to_fleet", {"dry_run": False}, blocking=True, return_response=True
    )
    assert again == {"status": "already_migrated", "dry_run": False, "fleet_entry_id": ANCHOR_ID}
    assert ir.async_get(hass).async_get_issue(DOMAIN, ISSUE_MIGRATION_AVAILABLE) is None

    # A real restart from disk keeps exactly one fleet and nothing else.
    await _restart_from_disk(hass, hass_storage, copy.deepcopy(hass_storage))
    await _assert_migrated(hass, before, hass_storage)


async def test_dry_run_reports_the_plan_and_mutates_nothing(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
) -> None:
    del legacy_fleet
    before = _state(hass)

    outcome = await hass.services.async_call(
        DOMAIN, "migrate_to_fleet", {}, blocking=True, return_response=True
    )

    assert outcome is not None
    assert outcome["status"] == "eligible"
    assert outcome["dry_run"] is True
    assert outcome["anchor_entry_id"] == ANCHOR_ID
    panels = cast(list[dict[str, Any]], outcome["panels"])
    assert {panel["panel"]: panel["subentry_id"] for panel in panels} == {
        slug: planned_subentry_id(entry_id) for entry_id, slug in PANELS.items()
    }
    assert {panel["panel"]: panel["mesh_priority"] for panel in panels} == {
        "kitchen": 1,
        "office": 2,
        "bedroom": 3,
    }
    rendered = json.dumps(outcome)
    for secret in (FLEET_PASSWORD, FLEET_USERNAME, ROOT_PASSWORD, host_key("office").split()[1]):
        assert secret not in rendered
    await _assert_originals(hass, before, hass_storage)
    assert ir.async_get(hass).async_get_issue(DOMAIN, ISSUE_MIGRATION_AVAILABLE) is not None


async def test_duplicate_mesh_priority_blocks_without_mutation(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    hass_storage: dict[str, Any],
) -> None:
    del mqtt_mock
    await _setup_legacy(hass, _entry(KITCHEN_ID, 1), _entry(OFFICE_ID, 1))
    before = _state(hass)

    for dry_run in (True, False):
        outcome = await _run(hass, dry_run=dry_run)
        assert outcome["status"] == "conflicts"
        assert outcome["conflicts"] == [
            {
                "code": "duplicate_mesh_priority",
                "field": "mesh_priority",
                "entries": [
                    {"entry_id": KITCHEN_ID, "title": "Brilliant kitchen"},
                    {"entry_id": OFFICE_ID, "title": "Brilliant office"},
                ],
                "detail": 1,
            }
        ]
    await _assert_originals(hass, before, hass_storage)
    await _unload_all(hass)


async def test_runtime_blockers_are_reported_without_mutation(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
) -> None:
    del legacy_fleet
    before = _state(hass)

    with patch(
        "custom_components.brilliant_mqtt.migration_coordinator.ProvisioningJournal.async_load",
        return_value=object(),
    ):
        outcome = await _run(hass, dry_run=False)
    assert [item["code"] for item in outcome["conflicts"]] == ["provisioning_in_progress"]

    with patch(
        "custom_components.brilliant_mqtt.migration_coordinator.mqtt.async_wait_for_mqtt_client",
        return_value=False,
    ):
        outcome = await _run(hass, dry_run=False)
    assert [item["code"] for item in outcome["conflicts"]] == ["mqtt_unavailable"]

    flow = await hass.config_entries.options.async_init(KITCHEN_ID)
    outcome = await _run(hass, dry_run=False)
    assert [item["code"] for item in outcome["conflicts"]] == ["config_flow_in_progress"]
    hass.config_entries.options.async_abort(flow["flow_id"])

    await _assert_originals(hass, before, hass_storage)


# ------------------------------------------------------------------ before the commit point

_REVERSIBLE_POINTS = (
    f"unload:{OFFICE_ID}",
    "before_save:planned",
    "after_save:planned",
    "after_save:quiesced",
    "convert_anchor",
    "before_save:anchor_converted",
    "after_save:anchor_converted",
    "retarget_registries",
    "after_save:registries_retargeted",
    "setup_fleet",
    "verify_fleet",
    "before_save:fleet_verified",
)


@pytest.mark.parametrize("point", _REVERSIBLE_POINTS)
async def test_in_process_failure_before_commit_reverses_exactly_without_panel_contact(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
    no_panel_contact: AsyncMock,
    point: str,
) -> None:
    del legacy_fleet
    before = _state(hass)
    audits = no_panel_contact.await_count

    outcome = await _run(hass, dry_run=False, checkpoint=_inject(point, InjectedFailure()))
    await hass.async_block_till_done()

    assert outcome == {"status": "reverted", "dry_run": False, "error": "unexpected_error"}
    # The restored Office entry carries ha_mirror_retire_verified, yet its
    # restoration setup must not run the panel-contacting retirement audit.
    assert no_panel_contact.await_count == audits
    await _assert_originals(hass, before, hass_storage)


@pytest.mark.parametrize("point", _REVERSIBLE_POINTS)
async def test_crash_before_commit_recovers_to_originals_after_restart_from_disk(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
    point: str,
) -> None:
    del legacy_fleet
    before = _state(hass)

    with pytest.raises(SimulatedCrash):
        await _run(hass, dry_run=False, checkpoint=_inject(point, SimulatedCrash()))
    disk = copy.deepcopy(hass_storage)
    record = await MigrationJournal(hass).async_load()
    assert record is None or record.phase is not MigrationPhase.FLEET_VERIFIED

    await _restart_from_disk(hass, hass_storage, disk)

    await _assert_originals(hass, before, hass_storage)
    assert (await _run(hass, dry_run=False))["status"] == "migrated"
    await _assert_migrated(hass, before, hass_storage)


# ------------------------------------------------------------------- after the commit point

_COMMITTED_POINTS = (
    "after_save:fleet_verified",
    "before_save:siblings_removing",
    "after_save:siblings_removing",
    f"remove:{KITCHEN_ID}",
    f"remove:{OFFICE_ID}",
    "complete",
    "before_save:complete",
    "after_save:complete",
)


@pytest.mark.parametrize("point", _COMMITTED_POINTS)
async def test_crash_after_commit_finishes_on_restart_from_disk(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
    point: str,
) -> None:
    del legacy_fleet
    before = _state(hass)

    with pytest.raises(SimulatedCrash):
        await _run(hass, dry_run=False, checkpoint=_inject(point, SimulatedCrash()))
    disk = copy.deepcopy(hass_storage)

    await _restart_from_disk(hass, hass_storage, disk)

    await _assert_migrated(hass, before, hass_storage)


@pytest.mark.parametrize("point", _COMMITTED_POINTS)
async def test_ordinary_exception_after_commit_never_reverses(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
    point: str,
) -> None:
    del legacy_fleet
    before = _state(hass)

    outcome = await _run(hass, dry_run=False, checkpoint=_inject(point, InjectedFailure()))
    await hass.async_block_till_done()

    assert outcome["status"] == "migrated"
    await _assert_migrated(hass, before, hass_storage)


# ------------------------------------------------------------------------- storage faults


async def test_unwritable_journal_never_starts_a_migration(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
    journal_faults: JournalFaults,
) -> None:
    del legacy_fleet
    before = _state(hass)
    journal_faults.write_fails = True

    outcome = await _run(hass, dry_run=False)
    await hass.async_block_till_done()
    journal_faults.write_fails = False

    assert outcome == {"status": "reverted", "dry_run": False, "error": "journal_write_failed"}
    assert not _journal_exists(hass)
    await _assert_originals(hass, before, hass_storage)


async def test_journal_failing_mid_migration_still_restores_and_recovers(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
    journal_faults: JournalFaults,
) -> None:
    """A reversible journal left behind is harmless: reversal is artifact-only."""
    del legacy_fleet
    before = _state(hass)

    async def checkpoint(name: str) -> None:
        if name == "after_save:anchor_converted":
            journal_faults.write_fails = True

    outcome = await _run(hass, dry_run=False, checkpoint=checkpoint)
    await hass.async_block_till_done()
    journal_faults.write_fails = False

    assert outcome == {"status": "reverted", "dry_run": False, "error": "journal_write_failed"}
    record = await _journal(hass)
    assert record is not None and record.phase is MigrationPhase.ANCHOR_CONVERTED
    # A crash right now still restores the originals from the journal.
    await _restart_from_disk(hass, hass_storage, copy.deepcopy(hass_storage))
    await _assert_originals(hass, before, hass_storage)


_PRECOMMIT_PHASES = ("planned", "quiesced", "anchor_converted", "registries_retargeted")


@pytest.mark.parametrize("phase", _PRECOMMIT_PHASES)
async def test_precommit_write_landing_with_failed_verify_read_reverses(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
    journal_faults: JournalFaults,
    phase: str,
) -> None:
    """Both candidates (old and new phase) are reversible, so reversal is safe."""
    del legacy_fleet
    before = _state(hass)

    async def checkpoint(name: str) -> None:
        if name == f"before_save:{phase}":
            journal_faults.read_fails = True

    outcome = await _run(hass, dry_run=False, checkpoint=checkpoint)
    await hass.async_block_till_done()
    journal_faults.read_fails = False

    assert outcome["status"] == "reverted"
    await _assert_originals(hass, before, hass_storage)


async def test_commit_write_landing_with_failed_verify_read_is_ambiguous_never_reversed(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
    journal_faults: JournalFaults,
) -> None:
    del legacy_fleet
    before = _state(hass)

    async def checkpoint(name: str) -> None:
        if name == "before_save:fleet_verified":
            journal_faults.read_fails = True

    outcome = await _run(hass, dry_run=False, checkpoint=checkpoint)
    await hass.async_block_till_done()
    journal_faults.read_fails = False

    assert outcome["status"] == "ambiguous"
    record = await _journal(hass)
    assert record is not None and record.phase is MigrationPhase.FLEET_VERIFIED
    # Journal and fleet kept; siblings exist but stay fenced.
    assert migration_fence(hass).active is True
    assert [manager.entry.entry_id for manager in _fleet_managers(hass)] == [ANCHOR_ID]
    assert not await hass.config_entries.async_setup(KITCHEN_ID)
    assert [manager.entry.entry_id for manager in _fleet_managers(hass)] == [ANCHOR_ID]
    assert ir.async_get(hass).async_get_issue(DOMAIN, ISSUE_MIGRATION_RECOVERY_FAILED)

    # Once the journal reads again, recovery finishes the committed migration.
    await _restart_from_disk(hass, hass_storage, copy.deepcopy(hass_storage))
    await _assert_migrated(hass, before, hass_storage)


@pytest.mark.parametrize("phase", ("siblings_removing", "complete"))
async def test_postcommit_write_with_failed_verify_read_still_finishes(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
    journal_faults: JournalFaults,
    phase: str,
) -> None:
    del legacy_fleet
    before = _state(hass)

    async def checkpoint(name: str) -> None:
        if name == f"before_save:{phase}":
            journal_faults.read_fails = True

    outcome = await _run(hass, dry_run=False, checkpoint=checkpoint)
    await hass.async_block_till_done()
    journal_faults.read_fails = False

    assert outcome["status"] == "migrated"
    await _assert_migrated(hass, before, hass_storage)


async def test_lazy_store_writes_failing_are_repaired_from_the_journal(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
    failing_writes: set[str],
) -> None:
    """Nothing trusts HA's lazily written stores: a crash with none of the
    migration on disk is re-applied from the journal on the next startup, and
    the journal is only forgotten once a later startup loads the target."""
    del legacy_fleet
    before = _state(hass)
    baseline = copy.deepcopy(hass_storage)
    failing_writes.update({"core.config_entries", "core.entity_registry", "core.device_registry"})

    outcome = await _run(hass, dry_run=False)
    await hass.async_block_till_done()
    assert outcome["status"] == "migrated"
    await _flush_disk(hass)  # Even the final write fails.
    assert _disk_state(hass_storage) == _disk_state(baseline)
    failing_writes.clear()

    # Crash: the disk still holds three legacy entries; the journal is complete.
    await _restart_from_disk(hass, hass_storage, copy.deepcopy(hass_storage))
    _check_migrated(hass, before)
    record = await _journal(hass)
    assert record is not None and record.phase is MigrationPhase.COMPLETE
    await _assert_migrated(hass, before, hass_storage)


async def test_complete_journal_is_kept_until_a_later_startup_confirms_it(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
) -> None:
    del legacy_fleet
    before = _state(hass)
    assert (await _run(hass, dry_run=False))["status"] == "migrated"
    record = await _journal(hass)
    assert record is not None and record.outcome == "migrated"
    assert migration_fence(hass).active is False

    await _clean_restart(hass, hass_storage)

    assert not _journal_exists(hass)
    _check_migrated(hass, before)


async def test_stopping_home_assistant_refuses_to_advance(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
) -> None:
    from homeassistant.core import CoreState

    del legacy_fleet
    before = _state(hass)

    async def checkpoint(name: str) -> None:
        if name == "after_save:quiesced":
            hass.set_state(CoreState.stopping)

    try:
        outcome = await _run(hass, dry_run=False, checkpoint=checkpoint)
    finally:
        hass.set_state(CoreState.running)

    assert outcome["status"] == "aborted" or outcome["status"].startswith("reverted")
    assert outcome["error"] == "home_assistant_stopping"
    await _restart_from_disk(hass, hass_storage, copy.deepcopy(hass_storage))
    await _assert_originals(hass, before, hass_storage)


async def test_corrupt_journal_file_is_not_absent_and_keeps_the_fence(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
) -> None:
    """Real decoding: a truncated journal over a converted anchor + siblings."""
    del legacy_fleet
    with pytest.raises(SimulatedCrash):
        await _run(
            hass, dry_run=False, checkpoint=_inject("after_save:fleet_verified", SimulatedCrash())
        )
    await _flush_disk(hass)
    disk = copy.deepcopy(hass_storage)
    assert set(_disk_state(disk)["entries"]) == set(PANELS)
    _write_journal_text(hass, _read_journal_text(hass)[:40])

    await _restart_from_disk(hass, hass_storage, disk)

    assert _fleet_managers(hass) == []
    assert migration_fence(hass).active is True
    issue = ir.async_get(hass).async_get_issue(DOMAIN, ISSUE_MIGRATION_RECOVERY_FAILED)
    assert issue is not None and issue.translation_placeholders == {"code": "journal_unreadable"}
    assert _journal_exists(hass)  # Never renamed aside like Store's corruption recovery.
    for entry in hass.config_entries.async_entries(DOMAIN):
        entry.async_cancel_retry_setup()


# ------------------------------------------------------------------- runtime verification


async def test_one_panel_failing_to_start_in_the_fleet_reverses(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
) -> None:
    del legacy_fleet
    before = _state(hass)
    original = FleetManager._async_setup_manager

    async def office_fails(self: FleetManager, manager: PanelManager) -> bool:
        if self.entry.data.get("entry_kind") == ENTRY_KIND_FLEET and manager.panel == "office":
            return False
        return await original(self, manager)

    with patch.object(FleetManager, "_async_setup_manager", office_fails):
        outcome = await _run(hass, dry_run=False)
    await hass.async_block_till_done()

    assert outcome == {"status": "reverted", "dry_run": False, "error": "fleet_runtime_mismatch"}
    await _assert_originals(hass, before, hass_storage)


async def test_fleet_setup_failure_reverses_and_reloads_originals(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
) -> None:
    del legacy_fleet
    before = _state(hass)
    original_setup = FleetManager.async_setup

    async def fleet_not_ready(self: FleetManager) -> None:
        if self.entry.data.get("entry_kind") == ENTRY_KIND_FLEET:
            raise ConfigEntryNotReady("broker down")
        await original_setup(self)

    with patch.object(FleetManager, "async_setup", fleet_not_ready):
        outcome = await _run(hass, dry_run=False)
    await hass.async_block_till_done()

    assert outcome == {"status": "reverted", "dry_run": False, "error": "fleet_setup_failed"}
    await _assert_originals(hass, before, hass_storage)


async def test_reversal_reports_restored_entries_that_did_not_start(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
) -> None:
    del legacy_fleet
    before = _state(hass)
    broken = False
    original_setup = FleetManager.async_setup

    async def legacy_not_ready(self: FleetManager) -> None:
        if broken and self.entry.entry_id == KITCHEN_ID:
            raise ConfigEntryNotReady("broker down")
        await original_setup(self)

    async def checkpoint(name: str) -> None:
        nonlocal broken
        if name == "verify_fleet":
            broken = True
            raise InjectedFailure

    with patch.object(FleetManager, "async_setup", legacy_not_ready):
        outcome = await _run(hass, dry_run=False, checkpoint=checkpoint)
        await hass.async_block_till_done()

    assert outcome == {
        "status": "reverted_config_only",
        "dry_run": False,
        "error": "unexpected_error",
        "not_loaded_entry_ids": [KITCHEN_ID],
    }
    issue = ir.async_get(hass).async_get_issue(DOMAIN, ISSUE_MIGRATION_REVERTED_NOT_LOADED)
    assert issue is not None and issue.translation_placeholders == {"count": "1"}
    kitchen = hass.config_entries.async_get_known_entry(KITCHEN_ID)
    assert kitchen.state is ConfigEntryState.SETUP_RETRY

    # HA's own setup retry now succeeds; the stale issue clears itself.
    broken = False
    await _drive_setup_retries(hass, KITCHEN_ID)
    retried = hass.config_entries.async_get_known_entry(KITCHEN_ID)
    assert retried.state is ConfigEntryState.LOADED
    assert ir.async_get(hass).async_get_issue(DOMAIN, ISSUE_MIGRATION_REVERTED_NOT_LOADED) is None
    await _assert_originals(hass, before, hass_storage)


async def test_restoration_stays_quiet_through_ha_setup_retry(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
    no_panel_contact: AsyncMock,
) -> None:
    """Office carries ha_mirror_retire_verified: its retried restoration must not SSH."""
    del legacy_fleet
    before = _state(hass)
    audits = no_panel_contact.await_count
    attempts = 0
    original_setup = FleetManager.async_setup

    async def office_flaky(self: FleetManager) -> None:
        nonlocal attempts
        if self.entry.entry_id == OFFICE_ID and self.entry.data.get("entry_kind") != "fleet":
            attempts += 1
            if attempts == 1:
                raise ConfigEntryNotReady("broker down")
        await original_setup(self)

    with patch.object(FleetManager, "async_setup", office_flaky):
        outcome = await _run(
            hass, dry_run=False, checkpoint=_inject("verify_fleet", InjectedFailure())
        )
        await hass.async_block_till_done()
        assert outcome["status"] == "reverted_config_only"
        await _drive_setup_retries(hass, OFFICE_ID)

    office = hass.config_entries.async_get_known_entry(OFFICE_ID)
    assert attempts == 2 and office.state is ConfigEntryState.LOADED
    assert no_panel_contact.await_count == audits
    assert not migration_fence(hass).is_quiet(OFFICE_ID)
    await _assert_originals(hass, before, hass_storage)


# ---------------------------------------------------------------------- concurrency/fence


async def test_edits_made_while_waiting_for_the_panel_lock_are_migrated(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
) -> None:
    del legacy_fleet
    lock = fleet_manager._shared_ssh_lock(hass)
    await lock.acquire()
    # Deliberately untracked: block_till_done must not wait on the blocked run.
    task = asyncio.ensure_future(_run(hass, dry_run=False))
    for _ in range(20):
        await asyncio.sleep(0)
    assert not task.done()
    kitchen = hass.config_entries.async_get_known_entry(KITCHEN_ID)
    hass.config_entries.async_update_entry(
        kitchen, data={**kitchen.data, "root_password": "rotated-root-password"}
    )
    for _ in range(20):
        await asyncio.sleep(0)
    lock.release()

    outcome = await task
    await hass.async_block_till_done()

    assert outcome["status"] == "migrated"
    fleet = hass.config_entries.async_get_known_entry(ANCHOR_ID)
    subentry = fleet.subentries[planned_subentry_id(KITCHEN_ID)]
    assert subentry.data["root_password"] == "rotated-root-password"
    await _settle(hass, hass_storage)


async def test_fenced_sibling_setup_between_phases_cannot_start_mixed_generations(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
) -> None:
    del legacy_fleet
    before = _state(hass)
    attempts: list[bool] = []

    async def checkpoint(name: str) -> None:
        if name in {"after_save:quiesced", "after_save:registries_retargeted", "verify_fleet"}:
            kitchen = hass.config_entries.async_get_known_entry(KITCHEN_ID)
            if kitchen.state is ConfigEntryState.NOT_LOADED:
                attempts.append(await hass.config_entries.async_setup(KITCHEN_ID))
            else:
                attempts.append(await hass.config_entries.async_reload(KITCHEN_ID))
            # A reload attempt is equally fenced.
            attempts.append(await hass.config_entries.async_reload(OFFICE_ID))
            assert len(_fleet_managers(hass)) <= 1

    outcome = await _run(hass, dry_run=False, checkpoint=checkpoint)
    await hass.async_block_till_done()

    assert outcome["status"] == "migrated", outcome
    assert attempts and not any(attempts)
    await _assert_migrated(hass, before, hass_storage)


async def test_failed_startup_recovery_fences_every_generation_until_resolved(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
) -> None:
    """The converted anchor and unremoved siblings are both on disk; none may start."""
    del legacy_fleet
    before = _state(hass)
    with pytest.raises(SimulatedCrash):
        await _run(
            hass,
            dry_run=False,
            checkpoint=_inject("before_save:fleet_verified", SimulatedCrash()),
        )
    await _flush_disk(hass)  # The converted anchor reached disk before the crash.
    disk = copy.deepcopy(hass_storage)
    assert _disk_state(disk)["entries"][ANCHOR_ID]["subentries"]
    assert set(_disk_state(disk)["entries"]) == set(PANELS)

    with patch.object(MigrationCoordinator, "_restore_registries", side_effect=InjectedFailure):
        await _restart_from_disk(hass, hass_storage, disk)

    assert _fleet_managers(hass) == []
    assert all(
        entry.state is not ConfigEntryState.LOADED
        for entry in hass.config_entries.async_entries(DOMAIN)
    )
    assert migration_fence(hass).active is True
    assert ir.async_get(hass).async_get_issue(DOMAIN, ISSUE_MIGRATION_RECOVERY_FAILED)
    assert hass.services.has_service(DOMAIN, "migrate_to_fleet")

    resumed = await hass.services.async_call(
        DOMAIN, "migrate_to_fleet", {"dry_run": False}, blocking=True, return_response=True
    )
    await hass.async_block_till_done()

    assert resumed is not None and resumed["status"] == "reverted"
    await _assert_originals(hass, before, hass_storage)


async def test_startup_recovery_exception_keeps_the_fence(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
) -> None:
    del legacy_fleet
    disk = copy.deepcopy(hass_storage)

    with patch.object(MigrationCoordinator, "async_recover", side_effect=RuntimeError):
        await _restart_from_disk(hass, hass_storage, disk)

    assert _fleet_managers(hass) == []
    assert migration_fence(hass).active is True
    issue = ir.async_get(hass).async_get_issue(DOMAIN, ISSUE_MIGRATION_RECOVERY_FAILED)
    assert issue is not None and issue.translation_placeholders == {"code": "recovery_crashed"}
    for entry in hass.config_entries.async_entries(DOMAIN):
        entry.async_cancel_retry_setup()


async def test_invalid_journal_at_startup_fails_closed_and_dry_run_only_observes(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
) -> None:
    del legacy_fleet
    disk = copy.deepcopy(hass_storage)
    _write_journal_text(
        hass,
        json.dumps(
            {
                "version": 1,
                "minor_version": 1,
                "key": "brilliant_mqtt.migration",
                "data": {"schema_version": 1, "mqtt_password": FLEET_PASSWORD},
            }
        ),
    )

    await _restart_from_disk(hass, hass_storage, disk)

    assert _fleet_managers(hass) == []
    issue = ir.async_get(hass).async_get_issue(DOMAIN, ISSUE_MIGRATION_RECOVERY_FAILED)
    assert issue is not None
    assert FLEET_PASSWORD not in json.dumps(issue.translation_placeholders)
    assert (await _run(hass, dry_run=False))["status"] == "recovery_failed"
    for entry in hass.config_entries.async_entries(DOMAIN):
        entry.async_cancel_retry_setup()


async def test_dry_run_with_unreadable_journal_changes_nothing(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
) -> None:
    del legacy_fleet
    _write_journal_text(hass, "{not json")

    outcome = await _run(hass, dry_run=True)

    assert outcome == {"status": "recovery_failed", "dry_run": True, "error": "journal_unreadable"}
    assert migration_fence(hass).active is False
    assert ir.async_get(hass).async_get_issue(DOMAIN, ISSUE_MIGRATION_RECOVERY_FAILED) is None
    assert len(_fleet_managers(hass)) == 3
    _remove_journal_file(hass)


async def test_flows_cannot_write_candidates_while_a_migration_is_active(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
) -> None:
    del legacy_fleet
    before = _state(hass)
    results: list[Any] = []

    async def checkpoint(name: str) -> None:
        if name == "after_save:quiesced":
            results.append(await hass.config_entries.options.async_init(KITCHEN_ID))
            results.append(
                await hass.config_entries.flow.async_init(
                    DOMAIN,
                    context={"source": "reconfigure", "entry_id": OFFICE_ID},
                )
            )

    outcome = await _run(hass, dry_run=False, checkpoint=checkpoint)
    await hass.async_block_till_done()

    assert [(result["type"], result["reason"]) for result in results] == [
        ("abort", "migration_in_progress"),
        ("abort", "migration_in_progress"),
    ]
    assert outcome["status"] == "migrated"
    await _assert_migrated(hass, before, hass_storage)


async def test_edit_detected_before_conversion_aborts_and_keeps_the_edit(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
) -> None:
    del legacy_fleet
    before = _state(hass)

    async def checkpoint(name: str) -> None:
        if name == "convert_anchor":
            office = hass.config_entries.async_get_known_entry(OFFICE_ID)
            hass.config_entries.async_update_entry(
                office, options={**office.options, "auto_repair": False}
            )

    outcome = await _run(hass, dry_run=False, checkpoint=checkpoint)
    await hass.async_block_till_done()

    assert outcome == {
        "status": "aborted",
        "dry_run": False,
        "error": "entry_changed_during_migration",
    }
    assert not _journal_exists(hass)
    expected = copy.deepcopy(before)
    expected["entries"][OFFICE_ID]["options"]["auto_repair"] = False
    await _assert_originals(hass, expected, hass_storage)


async def test_sibling_edit_after_conversion_survives_reversal(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
) -> None:
    """Reversal is drift-tolerant: only the converted anchor is restored."""
    del legacy_fleet
    before = _state(hass)

    async def checkpoint(name: str) -> None:
        if name == "retarget_registries":
            kitchen = hass.config_entries.async_get_known_entry(KITCHEN_ID)
            hass.config_entries.async_update_entry(
                kitchen, options={**kitchen.options, "offline_grace_minutes": 30}
            )
        if name == "setup_fleet":
            raise InjectedFailure

    outcome = await _run(hass, dry_run=False, checkpoint=checkpoint)
    await hass.async_block_till_done()

    assert outcome == {"status": "reverted", "dry_run": False, "error": "unexpected_error"}
    expected = copy.deepcopy(before)
    expected["entries"][KITCHEN_ID]["options"]["offline_grace_minutes"] = 30
    await _assert_originals(hass, expected, hass_storage)


# ------------------------------------------------------------------------- ownership/issues


async def test_sibling_removal_keeps_panel_issues_and_never_runs_handoff_recovery(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
) -> None:
    del legacy_fleet
    before = _state(hass)
    for issue_id in (f"voice_missing_{KITCHEN_ID}", f"broker_unavailable_{KITCHEN_ID}"):
        ir.async_create_issue(
            hass,
            DOMAIN,
            issue_id,
            is_fixable=False,
            severity=ir.IssueSeverity.WARNING,
            translation_key="voice_missing",
            translation_placeholders={"panel": "kitchen"},
        )

    with patch(
        "custom_components.brilliant_mqtt.async_recover_removed_entry",
        side_effect=AssertionError("handoff recovery must not run"),
    ):
        outcome = await _run(hass, dry_run=False)
    await hass.async_block_till_done()

    assert outcome["status"] == "migrated"
    await _assert_migrated(hass, before, hass_storage)
    issues = ir.async_get(hass)
    assert issues.async_get_issue(DOMAIN, f"voice_missing_{KITCHEN_ID}") is not None
    assert issues.async_get_issue(DOMAIN, f"broker_unavailable_{KITCHEN_ID}") is None
    assert migration_removing(hass) == set()


async def test_setup_runs_recovery_before_entries_load(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
) -> None:
    del mqtt_mock
    entry = _entry(OFFICE_ID, 1)
    entry.add_to_hass(hass)
    observed: list[ConfigEntryState] = []

    async def recover(self: MigrationCoordinator) -> None:
        observed.append(entry.state)
        migration_fence(self.hass).deactivate()

    with patch.object(MigrationCoordinator, "async_recover", recover):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    assert observed == [ConfigEntryState.NOT_LOADED]
    assert hass.services.has_service(DOMAIN, "migrate_to_fleet")
    issue = ir.async_get(hass).async_get_issue(DOMAIN, ISSUE_MIGRATION_AVAILABLE)
    assert issue is not None and issue.translation_placeholders == {"count": "1"}
    assert await hass.config_entries.async_unload(entry.entry_id)


async def test_nothing_to_migrate_and_availability_issue_lifecycle(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
) -> None:
    del mqtt_mock
    assert (await _run(hass, dry_run=True)) == {"status": "nothing_to_migrate", "dry_run": True}

    entries = await _setup_legacy(hass, _entry(OFFICE_ID, 1))
    issues = ir.async_get(hass)
    assert issues.async_get_issue(DOMAIN, ISSUE_MIGRATION_AVAILABLE) is not None

    with patch("custom_components.brilliant_mqtt.async_recover_removed_entry"):
        await hass.config_entries.async_remove(entries[0].entry_id)
    await hass.async_block_till_done()
    assert issues.async_get_issue(DOMAIN, ISSUE_MIGRATION_AVAILABLE) is None


async def test_managed_update_journal_resolves_only_exact_management_ids(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
) -> None:
    """A migrated panel keeps management_id = legacy entry ID, not its fingerprint.

    The canary update journal must still find it (or every OTA/recovery on a
    migrated fleet fails closed), but only because its subentry ID is exactly the
    one the migration derived; any other foreign management ID stays rejected.
    """
    from dataclasses import replace

    from custom_components.brilliant_mqtt.broker import BrokerKind
    from custom_components.brilliant_mqtt.entry_data import EntryDataError
    from custom_components.brilliant_mqtt.fleet_manager import _managed_update_owner
    from custom_components.brilliant_mqtt.provisioning_journal import (
        ProvisioningOperation,
        StoredFleetProfile,
        StoredPanelRequest,
    )
    from tests.test_fleet_manager import _pending_record

    del legacy_fleet
    assert (await _run(hass, dry_run=False))["status"] == "migrated"
    fleet = hass.config_entries.async_get_known_entry(ANCHOR_ID)
    office_subentry = fleet.subentries[planned_subentry_id(OFFICE_ID)]
    office = office_subentry.data
    record = replace(
        _pending_record(),
        operation=ProvisioningOperation.UPDATE,
        panel_request=StoredPanelRequest(
            host=office["host"],
            ssh_username="root",
            root_password=office["root_password"],
            public_key=office["ssh_host_key"],
            fingerprint=office["identity_fingerprint"],
            slug="office",
            selected_components=("bridge", "bus_watchdog"),
        ),
        fleet_profile=StoredFleetProfile(
            kind=BrokerKind.EXISTING_BROKER,
            host=fleet.data["mqtt_host"],
            port=fleet.data["mqtt_port"],
            tls_enabled=False,
        ),
    )

    owner, panel_id = _managed_update_owner(hass, record)
    assert owner is fleet
    assert panel_id == planned_subentry_id(OFFICE_ID)

    hass.config_entries.async_update_subentry(
        fleet, office_subentry, data={**office, "management_id": "some-other-entry"}
    )
    with pytest.raises(EntryDataError, match="provisioning_ownership_mismatch"):
        _managed_update_owner(hass, record)


async def test_ha_control_owner_and_scene_routing_survive_migration(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    payload_dir: Path,
    hass_storage: dict[str, Any],
) -> None:
    """The legacy election (smallest enabled slug) hands over to the one fleet owner."""
    from custom_components.brilliant_mqtt.ha_control import get_control_plane

    del mqtt_mock, payload_dir
    action = {"office:movie": {"domain": "light", "service": "turn_off", "target": {}, "data": {}}}
    control: dict[str, Any] = {"ha_control_enabled": True, "scene_actions": action}
    await _setup_legacy(
        hass,
        _entry(KITCHEN_ID, 1, **control),
        _entry(OFFICE_ID, 2, **control),
        _entry(BEDROOM_ID, 3, **control),
    )
    plane = get_control_plane(hass)
    assert plane.owner_entry_id == BEDROOM_ID  # smallest enabled legacy slug
    assert plane.scene_control.default_panel == "office"
    before = _state(hass)
    scene_select = er.async_get(hass).async_get_entity_id("select", DOMAIN, f"{OFFICE_ID}_scene")
    assert scene_select is not None

    outcome = await _run(hass, dry_run=False)
    await hass.async_block_till_done()

    assert outcome["status"] == "migrated"
    fleet = _check_migrated(hass, before, office_cadences=False)
    assert fleet.data["scene_actions"] == action
    assert plane.started is True
    assert plane.owner_entry_id == ANCHOR_ID
    assert plane.scene_control.default_panel == "office"
    assert plane.scene_control.attached_panels == frozenset(PANELS.values())
    entry = er.async_get(hass).async_get(scene_select)
    assert entry is not None and entry.config_subentry_id == planned_subentry_id(OFFICE_ID)
    await _assert_migrated(hass, before, hass_storage, office_cadences=False)
    await _unload_all(hass)


# ------------------------------------------------------------------ production-shaped fleet

_PRODUCTION_SLUGS = (
    "bedroom",
    "dining",
    "entry",
    "garage",
    "kitchen",
    "laundry",
    "living",
    "master-bath",
    "nursery",
    "office",
    "patio",
    "shed",
    "starry",
    "studio",
)


async def test_fourteen_panel_production_fleet_migrates_after_fixing_mesh_duplicate(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    payload_dir: Path,
    hass_storage: dict[str, Any],
    no_panel_contact: AsyncMock,
) -> None:
    del mqtt_mock, payload_dir
    hue_ca = "-----BEGIN CERTIFICATE-----\nMIIBdiyHue\n-----END CERTIFICATE-----\n"
    panels: dict[str, str] = {}
    entries: list[MockConfigEntry] = []
    for index, slug in enumerate(_PRODUCTION_SLUGS):
        entry_id = f"01PROD{index:02d}{slug.upper().replace('-', '')}"
        panels[entry_id] = slug
        overrides: dict[str, Any] = {
            "hue_ca_cert": hue_ca,
            "last_firmware": "v26.10.21.1",
            "scene_panel": "office",
        }
        if slug == "office":
            overrides |= {
                "hot_poll_seconds": 5,
                "resync_seconds": 900,
                "ha_mirror_retire_verified": True,
            }
        if slug in {"office", "starry"}:
            overrides["ssh_host_key"] = host_key(f"{slug}-rsa", "ssh-rsa")
        priority = 1 if slug in {"office", "shed"} else 0
        data = legacy_data(slug, f"192.0.2.{20 + index}", priority, **overrides)
        data["components"] = {
            **data["components"],
            "hue_ca": True,
            "wifi_watchdog": slug == "master-bath",
        }
        entry = MockConfigEntry(
            domain=DOMAIN,
            entry_id=entry_id,
            title=f"Brilliant {slug}",
            unique_id=slug,
            version=CONFIG_ENTRY_VERSION,
            data=data,
            options=legacy_options(),
        )
        entries.append(entry)
    await _setup_legacy(hass, *entries)
    registry = er.async_get(hass)
    garage = next(entry_id for entry_id, slug in panels.items() if slug == "garage")
    patio = next(entry_id for entry_id, slug in panels.items() if slug == "patio")
    reboot = registry.async_get_entity_id("button", DOMAIN, f"{garage}_reboot_panel")
    assert reboot is not None
    registry.async_update_entity(
        reboot, disabled_by=er.RegistryEntryDisabler.USER, new_entity_id="button.garage_reboot"
    )
    health = registry.async_get_entity_id("binary_sensor", DOMAIN, f"{patio}_bridge_health")
    assert health is not None
    registry.async_update_entity(health, hidden_by=er.RegistryEntryHider.USER)
    await _flush_disk(hass)

    blocked = await _run(hass, dry_run=True)
    assert blocked["status"] == "conflicts"
    assert [(item["code"], item.get("detail")) for item in blocked["conflicts"]] == [
        ("duplicate_mesh_priority", 1)
    ]
    assert {item["title"] for item in blocked["conflicts"][0]["entries"]} == {
        "Brilliant office",
        "Brilliant shed",
    }

    # The operator fixes Shed through its legacy Reconfigure (modelled directly).
    shed = next(entry_id for entry_id, slug in panels.items() if slug == "shed")
    shed_entry = hass.config_entries.async_get_known_entry(shed)
    hass.config_entries.async_update_entry(shed_entry, data={**shed_entry.data, "mesh_priority": 2})
    await hass.async_block_till_done()
    await _flush_disk(hass)
    before = _state(hass)
    audits = no_panel_contact.await_count
    assert (await _run(hass, dry_run=True))["status"] == "eligible"

    outcome = await _run(hass, dry_run=False)
    await hass.async_block_till_done()

    assert outcome["status"] == "migrated"
    anchor = min(panels)
    fleet = await _assert_migrated(
        hass, before, hass_storage, panels=panels, anchor_id=anchor, office_cadences=False
    )
    assert no_panel_contact.await_count == audits
    office_id = next(entry_id for entry_id, slug in panels.items() if slug == "office")
    office = fleet.subentries[planned_subentry_id(office_id)].data
    assert office["hot_poll_seconds"] == 5 and office["resync_seconds"] == 900
    assert office["feature_overrides"]["hue_ca_cert"] == hue_ca
    assert fleet.data["next_mesh_priority"] == 3
    assert fleet.data["scene_panel"] == planned_subentry_id(office_id)
    bath = next(entry_id for entry_id, slug in panels.items() if slug == "master-bath")
    assert fleet.subentries[planned_subentry_id(bath)].data["components"]["wifi_watchdog"]
    assert registry.async_get("button.garage_reboot") is not None
    assert len(fleet.subentries) == 14

    await _restart_from_disk(hass, hass_storage, copy.deepcopy(hass_storage))
    await _assert_migrated(
        hass, before, hass_storage, panels=panels, anchor_id=anchor, office_cadences=False
    )
    await _unload_all(hass)


async def test_startup_recovery_tolerates_drift_in_every_candidate(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
) -> None:
    """Edits to every sibling reached disk with the converted anchor, then a crash."""
    del legacy_fleet
    before = _state(hass)

    async def checkpoint(name: str) -> None:
        if name == "retarget_registries":
            for entry_id in SIBLINGS:
                entry = hass.config_entries.async_get_known_entry(entry_id)
                hass.config_entries.async_update_entry(
                    entry, options={**entry.options, "repair_cooldown_minutes": 90}
                )
        if name == "setup_fleet":
            raise SimulatedCrash

    with pytest.raises(SimulatedCrash):
        await _run(hass, dry_run=False, checkpoint=checkpoint)
    await _flush_disk(hass)

    await _restart_from_disk(hass, hass_storage, copy.deepcopy(hass_storage))

    expected = copy.deepcopy(before)
    for entry_id in SIBLINGS:
        expected["entries"][entry_id]["options"]["repair_cooldown_minutes"] = 90
    await _assert_originals(hass, expected, hass_storage)


async def test_leftover_reversible_journal_never_undoes_a_later_anchor_edit(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
    journal_faults: JournalFaults,
) -> None:
    """Recovery restores the anchor only when it is actually converted."""
    del legacy_fleet
    before = _state(hass)

    async def checkpoint(name: str) -> None:
        if name == "after_save:anchor_converted":
            journal_faults.write_fails = True

    assert (await _run(hass, dry_run=False, checkpoint=checkpoint))["status"] == "reverted"
    journal_faults.write_fails = False
    record = await _journal(hass)
    assert record is not None and record.phase is MigrationPhase.ANCHOR_CONVERTED
    anchor = hass.config_entries.async_get_known_entry(ANCHOR_ID)
    hass.config_entries.async_update_entry(anchor, options={**anchor.options, "auto_repair": False})
    await hass.async_block_till_done()

    await _clean_restart(hass, hass_storage)

    expected = copy.deepcopy(before)
    expected["entries"][ANCHOR_ID]["options"]["auto_repair"] = False
    await _assert_originals(hass, expected, hass_storage)


# --------------------------------------------------------- round 3: quiet obligation


async def test_quiet_restoration_survives_crash_after_reversal_completes(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
    no_panel_contact: AsyncMock,
) -> None:
    """Crash after the reverted ``complete`` lands, then again before the quiet setup."""
    del legacy_fleet
    before = _state(hass)
    audits = no_panel_contact.await_count

    async def checkpoint(name: str) -> None:
        if name == "verify_fleet":
            raise InjectedFailure
        if name == "after_save:complete":
            raise SimulatedCrash

    with pytest.raises(SimulatedCrash):
        await _run(hass, dry_run=False, checkpoint=checkpoint)
    record = await _journal(hass)
    assert record is not None and record.outcome == "reverted"
    await _flush_disk(hass)  # The originals reached disk before the crash.

    # Restart 1: target already present -> journal confirmed, but Office's quiet
    # setup fails (broker down) and HA crashes before any retry.
    original_setup = FleetManager.async_setup

    async def office_down(self: FleetManager) -> None:
        if self.entry.entry_id == OFFICE_ID:
            raise ConfigEntryNotReady("broker down")
        await original_setup(self)

    with patch.object(FleetManager, "async_setup", office_down):
        await _restart_from_disk(hass, hass_storage, copy.deepcopy(hass_storage))
    assert not _journal_exists(hass)
    assert no_panel_contact.await_count == audits

    # Restart 2: the outstanding obligation is still durable; Office starts quietly.
    await _clean_restart(hass, hass_storage)
    assert hass.config_entries.async_get_known_entry(OFFICE_ID).state is ConfigEntryState.LOADED
    assert no_panel_contact.await_count == audits
    _check_originals(hass, before)

    # The obligation is consumed: an ordinary restart audits again as before.
    await _clean_restart(hass, hass_storage)
    assert no_panel_contact.await_count == audits + 1


# --------------------------------------------------------- round 3: decode failures


class DecodeFaults:
    """Make the journal's real ``_read_file`` hit UnicodeDecodeError on read()."""

    def __init__(self) -> None:
        self.remaining = 0
        self.persistent = False


@pytest.fixture
def decode_faults() -> Iterator[DecodeFaults]:
    faults = DecodeFaults()
    real_open = open

    class Handle:
        def __init__(self, handle: Any) -> None:
            self._handle = handle

        def __enter__(self) -> Handle:
            return self

        def __exit__(self, *args: object) -> None:
            self._handle.close()

        def read(self) -> str:
            if faults.persistent or faults.remaining > 0:
                faults.remaining = max(0, faults.remaining - 1)
                raise UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte")
            return str(self._handle.read())

    def fake_open(path: str, *args: Any, **kwargs: Any) -> Any:
        handle = real_open(path, *args, **kwargs)
        if "migration" in str(path) and "r" in str(kwargs.get("mode", "r")):
            return Handle(handle)
        return handle

    with patch.object(migration_journal, "open", fake_open, create=True):
        yield faults


_ALL_SAVE_PHASES = (
    "planned",
    "quiesced",
    "anchor_converted",
    "registries_retargeted",
    "fleet_verified",
    "siblings_removing",
    "complete",
)


@pytest.mark.parametrize("phase", _ALL_SAVE_PHASES)
async def test_transient_decode_failure_on_verify_read_is_settled_by_reread(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
    decode_faults: DecodeFaults,
    phase: str,
) -> None:
    """One decode failure: the re-read proves the write landed; migration continues."""
    del legacy_fleet
    before = _state(hass)

    async def checkpoint(name: str) -> None:
        if name == f"before_save:{phase}":
            decode_faults.remaining = 1

    outcome = await _run(hass, dry_run=False, checkpoint=checkpoint)
    await hass.async_block_till_done()

    assert outcome["status"] == "migrated", outcome
    await _assert_migrated(hass, before, hass_storage)


@pytest.mark.parametrize("phase", _ALL_SAVE_PHASES)
async def test_persistent_decode_failure_never_overwrites_a_commit(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
    decode_faults: DecodeFaults,
    phase: str,
) -> None:
    del legacy_fleet
    before = _state(hass)

    async def checkpoint(name: str) -> None:
        if name == f"before_save:{phase}":
            decode_faults.persistent = True

    outcome = await _run(hass, dry_run=False, checkpoint=checkpoint)
    await hass.async_block_till_done()
    decode_faults.persistent = False

    record = await _journal(hass)
    if phase in _PRECOMMIT_PHASES:
        assert outcome["status"] == "reverted", outcome
        assert record is None or not record.outcome == "migrated"
        await _restart_from_disk(hass, hass_storage, copy.deepcopy(hass_storage))
        await _assert_originals(hass, before, hass_storage)
        return
    if phase == "fleet_verified":
        assert outcome["status"] == "ambiguous", outcome
        assert record is not None and record.phase is MigrationPhase.FLEET_VERIFIED
        assert migration_fence(hass).active is True
    else:
        assert outcome["status"] == "migrated", outcome
        assert record is not None and record.phase in {
            MigrationPhase.SIBLINGS_REMOVING,
            MigrationPhase.COMPLETE,
        }
    await _restart_from_disk(hass, hass_storage, copy.deepcopy(hass_storage))
    await _assert_migrated(hass, before, hass_storage)


async def test_management_device_with_child_devices_is_a_dry_run_blocker(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
) -> None:
    """HA 2026.10+ cannot move a parent device; report it before any mutation."""
    del legacy_fleet
    registry = dr.async_get(hass)
    office_device = next(iter(dr.async_entries_for_config_entry(registry, OFFICE_ID)))

    def children(_registry: dr.DeviceRegistry, device_id: str) -> list[object]:
        return [object()] if device_id == office_device.id else []

    with patch.object(dr, "async_entries_for_parent_device", children, create=True):
        outcome = await _run(hass, dry_run=True)

    assert outcome["status"] == "conflicts"
    assert outcome["conflicts"] == [
        {
            "code": "device_has_child_devices",
            "field": "device_registry",
            "entries": [{"entry_id": OFFICE_ID, "title": "Brilliant office"}],
        }
    ]


_STORES = ("core.config_entries", "core.entity_registry", "core.device_registry")


@pytest.mark.parametrize("journal_phase", ("registries_retargeted", "fleet_verified"))
@pytest.mark.parametrize(
    "new_stores",
    [
        frozenset(combo)
        for size in range(len(_STORES) + 1)
        for combo in itertools.combinations(_STORES, size)
    ],
    ids=lambda stores: "+".join(sorted(key.split(".")[1] for key in stores)) or "none",
)
async def test_mixed_persistence_at_startup_converges_on_the_journal(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
    journal_phase: str,
    new_stores: frozenset[str],
) -> None:
    """Config entries and both registries each independently old or new on disk."""
    del legacy_fleet
    before = _state(hass)
    old_disk = copy.deepcopy(hass_storage)
    with pytest.raises(SimulatedCrash):
        await _run(
            hass, dry_run=False, checkpoint=_inject(f"after_save:{journal_phase}", SimulatedCrash())
        )
    await _flush_disk(hass)
    new_disk = copy.deepcopy(hass_storage)
    mixed = copy.deepcopy(old_disk)
    for key in new_stores:
        mixed[key] = copy.deepcopy(new_disk[key])

    await _restart_from_disk(hass, hass_storage, mixed)

    if journal_phase == "fleet_verified":
        await _assert_migrated(hass, before, hass_storage)
    else:
        await _assert_originals(hass, before, hass_storage)


@pytest.mark.skipif(
    not SINGLE_OWNER_DEVICES, reason="split (single-owner) device registry is HA 2026.10+"
)
async def test_production_split_devices_move_only_the_management_twin(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    payload_dir: Path,
    hass_storage: dict[str, Any],
) -> None:
    """A real HA 2026.6 disk (shared devices) migrated by HA to split twins.

    Production shape after HA's own device-registry migration: per panel two
    devices with identifiers [["mqtt", "brilliant_panel_<slug>"]], one owned by
    the MQTT entry (MQTT entities), one by the legacy entry (management
    entities), both with composite_primary_config_entry = legacy entry ID.
    """
    del mqtt_mock, payload_dir
    mqtt_id = hass.config_entries.async_entries("mqtt")[0].entry_id
    fixture = Path(__file__).with_name("fixtures") / "legacy_fleet_ha_2026_6_disk.json"
    disk = {
        **copy.deepcopy(hass_storage),
        **json.loads(await hass.async_add_executor_job(fixture.read_text)),
    }
    disk = json.loads(json.dumps(disk).replace("MQTT_ENTRY_ID", mqtt_id))
    await _restart_from_disk(hass, hass_storage, disk)
    registry = dr.async_get(hass)
    entities = er.async_get(hass)

    def twins(slug: str) -> tuple[dr.DeviceEntry, dr.DeviceEntry]:
        matching = [
            device
            for device in _all_devices(registry)
            if device.identifiers == {("mqtt", f"brilliant_panel_{slug}")}
        ]
        assert len(matching) == 2
        mqtt_twin = next(d for d in matching if getattr(d, "config_entry_id", None) == mqtt_id)
        management = next(d for d in matching if d is not mqtt_twin)
        return mqtt_twin, management

    shape: dict[str, tuple[str, str]] = {}
    for entry_id, slug in PANELS.items():
        mqtt_twin, management = twins(slug)
        assert getattr(management, "config_entry_id", None) == entry_id
        assert getattr(mqtt_twin, "config_subentry_id", "x") is None
        for device in (mqtt_twin, management):
            assert getattr(device, "composite_primary_config_entry", None) == entry_id
        assert getattr(mqtt_twin, "has_composite_identifiers", None) is True
        # Management-entity setup re-registers the device and clears the flag.
        assert getattr(management, "has_composite_identifiers", None) is False
        owned = er.async_entries_for_device(entities, management.id, True)
        assert len(owned) >= 9 and {e.config_entry_id for e in owned} == {entry_id}
        assert {e.config_entry_id for e in er.async_entries_for_device(entities, mqtt_twin.id)} == {
            mqtt_id
        }
        shape[entry_id] = (mqtt_twin.id, management.id)
    before = _state(hass)

    outcome = await _run(hass, dry_run=False)
    await hass.async_block_till_done()

    assert outcome["status"] == "migrated"
    for entry_id, slug in PANELS.items():
        mqtt_twin, management = twins(slug)
        assert (mqtt_twin.id, management.id) == shape[entry_id]
        assert getattr(management, "config_entry_id", None) == ANCHOR_ID
        assert getattr(management, "config_subentry_id", None) == planned_subentry_id(entry_id)
        assert getattr(mqtt_twin, "config_entry_id", None) == mqtt_id
        assert getattr(mqtt_twin, "config_subentry_id", "x") is None
        assert getattr(mqtt_twin, "has_composite_identifiers", None) is True
        assert getattr(management, "has_composite_identifiers", None) is False
        # Removing a sibling entry clears the twins' reference to it; the anchor's
        # twins keep pointing at the anchor (now the fleet entry).
        expected_primary = ANCHOR_ID if entry_id == ANCHOR_ID else None
        for device in (mqtt_twin, management):
            assert getattr(device, "composite_primary_config_entry", "x") == expected_primary
        assert {e.config_entry_id for e in er.async_entries_for_device(entities, mqtt_twin.id)} == {
            mqtt_id
        }
    await _assert_migrated(hass, before, hass_storage, office_cadences=False)
    await _unload_all(hass)
