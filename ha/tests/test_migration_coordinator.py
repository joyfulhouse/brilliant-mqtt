"""Recoverable legacy-to-fleet migration against the real HA entry/registry stack.

Restarts are modelled from what is actually on "disk" (``hass_storage``): the
in-memory Brilliant entries and both registries are discarded and rebuilt from
the persisted stores, so any divergence between memory and disk is visible.
"""

from __future__ import annotations

import asyncio
import copy
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from datetime import datetime
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
from homeassistant.util.file import WriteError
from pytest_homeassistant_custom_component.common import MockConfigEntry
from pytest_homeassistant_custom_component.typing import MqttMockHAClient

from custom_components import brilliant_mqtt
from custom_components.brilliant_mqtt import fleet_manager
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
    MigrationCoordinator,
    migration_fence,
    migration_removing,
)
from custom_components.brilliant_mqtt.migration_journal import (
    MIGRATION_STORAGE_KEY,
    MigrationJournal,
    MigrationPhase,
)
from custom_components.brilliant_mqtt.migration_persistence import (
    async_persist_config_entries,
    async_persist_device_registry,
    async_persist_entity_registry,
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
    """Model a long-running HA: everything in memory has reached disk."""
    await async_persist_config_entries(hass)
    await async_persist_entity_registry(hass)
    await async_persist_device_registry(hass)


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
        "devices": {
            device.id: {
                owner: set(subentries)
                for owner, subentries in device.config_entries_subentries.items()
            }
            for device in device_registry.devices.values()
        },
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
        "devices": {
            raw["id"]: {
                owner: set(subentries)
                for owner, subentries in raw["config_entries_subentries"].items()
            }
            for raw in devices
        },
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


def _assert_migrated(
    hass: HomeAssistant,
    before: State,
    hass_storage: dict[str, Any],
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
    mqtt_entry_id = hass.config_entries.async_entries("mqtt")[0].entry_id
    for device_id, links in before["devices"].items():
        legacy_owner = next((owner for owner in links if owner in panels), None)
        if legacy_owner is None:
            assert after["devices"][device_id] == links
            continue
        assert after["devices"][device_id] == {
            mqtt_entry_id: {None},
            anchor_id: {planned[legacy_owner]},
        }

    assert fleet.state is ConfigEntryState.LOADED
    runtime = fleet.runtime_data
    assert isinstance(runtime, FleetManager)
    assert runtime.runtime_owned_panel_ids == frozenset(planned.values())
    assert {manager.management_id for manager in runtime.panels.values()} == set(panels)
    assert len(_fleet_managers(hass)) == 1
    assert MIGRATION_STORAGE_KEY not in hass_storage
    assert migration_fence(hass).active is False
    # Durable: what is on disk is exactly what is in memory.
    assert _disk_state(hass_storage) == after
    return fleet


def _assert_originals(hass: HomeAssistant, before: State, hass_storage: dict[str, Any]) -> None:
    assert _state(hass) == before
    assert _disk_state(hass_storage) == before
    assert MIGRATION_STORAGE_KEY not in hass_storage
    assert migration_fence(hass).active is False
    for entry in hass.config_entries.async_entries(DOMAIN):
        assert entry.state is ConfigEntryState.LOADED
        runtime = entry.runtime_data
        assert isinstance(runtime, FleetManager)
        assert set(runtime.panels) == {entry.entry_id}


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
    _assert_migrated(hass, before, hass_storage)
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
    _assert_migrated(hass, before, hass_storage)


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
    _assert_originals(hass, before, hass_storage)
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
    _assert_originals(hass, before, hass_storage)
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

    _assert_originals(hass, before, hass_storage)


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
    "prove",
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
    _assert_originals(hass, before, hass_storage)
    # The restored Office entry carries ha_mirror_retire_verified, yet its
    # restoration setup must not run the panel-contacting retirement audit.
    assert no_panel_contact.await_count == audits


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

    _assert_originals(hass, before, hass_storage)
    assert (await _run(hass, dry_run=False))["status"] == "migrated"
    _assert_migrated(hass, before, hass_storage)


# ------------------------------------------------------------------- after the commit point

_COMMITTED_POINTS = (
    "after_save:fleet_verified",
    "before_save:siblings_removing",
    "after_save:siblings_removing",
    f"remove:{KITCHEN_ID}",
    f"remove:{OFFICE_ID}",
    "prove_migrated",
    "before_save:complete",
    "after_save:complete",
    "remove_journal",
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

    _assert_migrated(hass, before, hass_storage)


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
    _assert_migrated(hass, before, hass_storage)


# ------------------------------------------------------------------------- storage faults


async def test_unwritable_journal_never_starts_a_migration(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
    failing_writes: set[str],
) -> None:
    del legacy_fleet
    before = _state(hass)
    failing_writes.add(MIGRATION_STORAGE_KEY)

    outcome = await _run(hass, dry_run=False)
    await hass.async_block_till_done()

    assert outcome == {"status": "reverted", "dry_run": False, "error": "journal_unproven"}
    _assert_originals(hass, before, hass_storage)


async def test_journal_failing_mid_migration_fences_then_restart_restores(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
    failing_writes: set[str],
) -> None:
    del legacy_fleet
    before = _state(hass)

    async def checkpoint(name: str) -> None:
        if name == "after_save:anchor_converted":
            failing_writes.add(MIGRATION_STORAGE_KEY)

    outcome = await _run(hass, dry_run=False, checkpoint=checkpoint)
    await hass.async_block_till_done()

    assert outcome["status"] == "recovery_failed"
    assert outcome["recovery_error"] == "journal_unproven"
    assert migration_fence(hass).active is True
    assert _fleet_managers(hass) == []
    assert ir.async_get(hass).async_get_issue(DOMAIN, ISSUE_MIGRATION_RECOVERY_FAILED)
    # A fenced entry cannot be started by anyone else meanwhile.
    assert not await hass.config_entries.async_setup(KITCHEN_ID)
    assert _fleet_managers(hass) == []

    failing_writes.clear()
    await _restart_from_disk(hass, hass_storage, copy.deepcopy(hass_storage))

    _assert_originals(hass, before, hass_storage)


async def test_registry_write_failure_blocks_the_commit_and_reverses(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
    failing_writes: set[str],
) -> None:
    del legacy_fleet
    before = _state(hass)
    failing_writes.add("core.entity_registry")

    outcome = await _run(hass, dry_run=False)
    await hass.async_block_till_done()

    assert outcome == {"status": "reverted", "dry_run": False, "error": "storage_unproven"}
    failing_writes.clear()
    _assert_originals(hass, before, hass_storage)


async def test_stopping_home_assistant_cannot_prove_and_never_commits(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
) -> None:
    """While HA stops, Store.async_save only defers; no boundary may be assumed."""
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

    assert outcome["status"] == "recovery_failed"
    assert hass_storage[MIGRATION_STORAGE_KEY]["data"]["phase"] == "quiesced"
    assert _fleet_managers(hass) == []
    await _restart_from_disk(hass, hass_storage, copy.deepcopy(hass_storage))
    _assert_originals(hass, before, hass_storage)


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
    _assert_originals(hass, before, hass_storage)


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
    _assert_originals(hass, before, hass_storage)


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
    assert _disk_state(hass_storage) == before
    entry = hass.config_entries.async_get_known_entry(KITCHEN_ID)
    entry.async_cancel_retry_setup()


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
    assert MIGRATION_STORAGE_KEY not in hass_storage


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
    _assert_migrated(hass, before, hass_storage)


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
    _assert_originals(hass, before, hass_storage)


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


async def test_corrupt_journal_at_startup_fails_closed(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
) -> None:
    del legacy_fleet
    disk = copy.deepcopy(hass_storage)
    disk[MIGRATION_STORAGE_KEY] = {
        "version": 1,
        "minor_version": 1,
        "key": MIGRATION_STORAGE_KEY,
        "data": {"schema_version": 1, "mqtt_password": FLEET_PASSWORD},
    }

    await _restart_from_disk(hass, hass_storage, disk)

    assert _fleet_managers(hass) == []
    issue = ir.async_get(hass).async_get_issue(DOMAIN, ISSUE_MIGRATION_RECOVERY_FAILED)
    assert issue is not None
    assert FLEET_PASSWORD not in json.dumps(issue.translation_placeholders)
    assert (await _run(hass, dry_run=False))["status"] == "recovery_failed"
    for entry in hass.config_entries.async_entries(DOMAIN):
        entry.async_cancel_retry_setup()


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
    _assert_migrated(hass, before, hass_storage)
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
    fleet = _assert_migrated(hass, before, hass_storage, office_cadences=False)
    assert fleet.data["scene_actions"] == action
    assert plane.started is True
    assert plane.owner_entry_id == ANCHOR_ID
    assert plane.scene_control.default_panel == "office"
    assert plane.scene_control.attached_panels == frozenset(PANELS.values())
    entry = er.async_get(hass).async_get(scene_select)
    assert entry is not None and entry.config_subentry_id == planned_subentry_id(OFFICE_ID)
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
    fleet = _assert_migrated(
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
    _assert_migrated(
        hass, before, hass_storage, panels=panels, anchor_id=anchor, office_cadences=False
    )
    await _unload_all(hass)
