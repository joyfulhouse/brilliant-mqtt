"""Recoverable legacy-to-fleet migration against the real HA entry/registry stack."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import issue_registry as ir
from pytest_homeassistant_custom_component.common import MockConfigEntry
from pytest_homeassistant_custom_component.typing import MqttMockHAClient

from custom_components.brilliant_mqtt.const import (
    CONFIG_ENTRY_VERSION,
    DOMAIN,
    ENTRY_KIND_FLEET,
    FLEET_UNIQUE_ID,
    SUBENTRY_TYPE_PANEL,
)
from custom_components.brilliant_mqtt.fleet_manager import FleetManager
from custom_components.brilliant_mqtt.migration import planned_subentry_id
from custom_components.brilliant_mqtt.migration_coordinator import (
    ISSUE_MIGRATION_AVAILABLE,
    ISSUE_MIGRATION_RECOVERY_FAILED,
    MigrationCoordinator,
    migration_removing,
)
from custom_components.brilliant_mqtt.migration_journal import (
    MIGRATION_STORAGE_KEY,
    MigrationJournal,
    MigrationPhase,
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
    """Models Home Assistant stopping mid-migration (no in-process reversal)."""


class InjectedFailure(Exception):
    """Models an ordinary in-process failure (reversed immediately)."""


async def _no_wait(hass: HomeAssistant, entry: Any) -> None:
    del hass, entry


@pytest.fixture(autouse=True)
def no_panel_contact() -> Iterator[None]:
    """Migration must never open an SSH connection to any panel."""
    with patch("asyncssh.connect", side_effect=AssertionError("panel contacted")):
        yield


@pytest.fixture(autouse=True)
def instant_persistence() -> Iterator[None]:
    """The HA harness keeps config entries in memory; prove the gate is awaited."""
    with patch(
        "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
        new=AsyncMock(),
    ):
        yield


def _entry(entry_id: str, mesh_priority: int, **overrides: Any) -> MockConfigEntry:
    panel = PANELS[entry_id]
    return MockConfigEntry(
        domain=DOMAIN,
        entry_id=entry_id,
        title=f"Brilliant {panel}",
        unique_id=panel,
        version=CONFIG_ENTRY_VERSION,
        data=legacy_data(panel, f"192.0.2.{10 + mesh_priority}", mesh_priority, **overrides),
        options=legacy_options(),
    )


async def _setup_legacy(
    hass: HomeAssistant,
    *entries: MockConfigEntry,
) -> list[MockConfigEntry]:
    """Load legacy entries over existing MQTT-discovery devices, then customize entities."""
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
            "light",
            "mqtt",
            f"{panel}_light_1",
            config_entry=mqtt_entry,
            device_id=device.id,
        )
        entry.add_to_hass(hass)
    for entry in entries:
        if entry.state is ConfigEntryState.NOT_LOADED:
            assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    assert all(entry.state is ConfigEntryState.LOADED for entry in entries)

    kitchen_repair = entity_registry.async_get_entity_id(
        "button", DOMAIN, f"{KITCHEN_ID}_repair_bridge"
    )
    office_health = entity_registry.async_get_entity_id(
        "binary_sensor", DOMAIN, f"{OFFICE_ID}_bridge_health"
    )
    if kitchen_repair is not None:
        entity_registry.async_update_entity(
            kitchen_repair,
            disabled_by=er.RegistryEntryDisabler.USER,
            new_entity_id="button.kitchen_custom_repair",
        )
    if office_health is not None:
        entity_registry.async_update_entity(office_health, hidden_by=er.RegistryEntryHider.USER)
    await hass.async_block_till_done()
    return list(entries)


@pytest.fixture
async def legacy_fleet(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    payload_dir: Path,
) -> AsyncIterator[list[MockConfigEntry]]:
    del mqtt_mock, payload_dir
    entries = await _setup_legacy(
        hass,
        _entry(KITCHEN_ID, 1),
        _entry(OFFICE_ID, 2, hot_poll_seconds=5, resync_seconds=900),
        _entry(BEDROOM_ID, 3),
    )
    yield entries
    for entry in hass.config_entries.async_entries(DOMAIN):
        if entry.state is ConfigEntryState.LOADED:
            await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()


def _state(hass: HomeAssistant) -> State:
    """Every persisted fact migration may touch, in comparable form."""
    entity_registry = er.async_get(hass)
    device_registry = dr.async_get(hass)
    return {
        "entries": {
            entry.entry_id: {
                "title": entry.title,
                "unique_id": entry.unique_id,
                "version": entry.version,
                "data": json.loads(json.dumps(dict(entry.data))),
                "options": dict(entry.options),
                "subentries": {
                    subentry_id: (subentry.title, subentry.unique_id, dict(subentry.data))
                    for subentry_id, subentry in entry.subentries.items()
                },
            }
            for entry in hass.config_entries.async_entries(DOMAIN)
        },
        "entities": {
            item.entity_id: (
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


def _domain_entities(hass: HomeAssistant) -> list[er.RegistryEntry]:
    return [item for item in er.async_get(hass).entities.values() if item.platform == DOMAIN]


async def _run(
    hass: HomeAssistant,
    *,
    dry_run: bool,
    checkpoint: Callable[[str], Awaitable[None]] | None = None,
) -> dict[str, Any]:
    return await MigrationCoordinator(
        hass, checkpoint=checkpoint, persistence_waiter=_no_wait
    ).async_run(dry_run=dry_run)


async def _restart(hass: HomeAssistant) -> dict[str, Any] | None:
    """Model a restart: nothing loaded, recover, then let HA load what remains."""
    for entry in hass.config_entries.async_entries(DOMAIN):
        if entry.state is not ConfigEntryState.NOT_LOADED and entry.state.recoverable:
            await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
    migration_removing(hass).clear()
    outcome = await MigrationCoordinator(hass).async_recover()
    for entry in hass.config_entries.async_entries(DOMAIN):
        if entry.state is ConfigEntryState.NOT_LOADED:
            await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    return outcome


def _assert_migrated(
    hass: HomeAssistant,
    before: State,
    hass_storage: dict[str, Any],
    *,
    office_cadences: bool = True,
) -> MockConfigEntry:
    entries = hass.config_entries.async_entries(DOMAIN)
    assert [entry.entry_id for entry in entries] == [ANCHOR_ID]
    fleet = cast(MockConfigEntry, entries[0])
    assert fleet.unique_id == FLEET_UNIQUE_ID
    assert fleet.title == "Brilliant MQTT"
    assert fleet.version == CONFIG_ENTRY_VERSION
    assert fleet.data["entry_kind"] == ENTRY_KIND_FLEET
    assert fleet.data["scene_panel"] == planned_subentry_id(OFFICE_ID)
    assert "panel" not in fleet.data and "root_password" not in fleet.data
    planned = {entry_id: planned_subentry_id(entry_id) for entry_id in PANELS}
    assert set(fleet.subentries) == set(planned.values())
    for entry_id, subentry_id in planned.items():
        subentry = fleet.subentries[subentry_id]
        assert subentry.subentry_type == SUBENTRY_TYPE_PANEL
        assert subentry.title == f"Brilliant {PANELS[entry_id]}"
        assert subentry.data["management_id"] == entry_id
        assert subentry.data["panel"] == PANELS[entry_id]
        assert subentry.unique_id == subentry.data["identity_fingerprint"]
    office = fleet.subentries[planned[OFFICE_ID]].data
    if office_cadences:
        assert office["hot_poll_seconds"] == 5 and office["resync_seconds"] == 900

    after = _state(hass)
    domain_before = {
        entity_id: values for entity_id, values in before["entities"].items() if values[1] == DOMAIN
    }
    domain_after = {
        entity_id: values for entity_id, values in after["entities"].items() if values[1] == DOMAIN
    }
    assert set(domain_after) == set(domain_before)
    assert "button.kitchen_custom_repair" in domain_after
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
            ANCHOR_ID,
            planned[owner],
            device,
            disabled,
            hidden,
        )
    assert domain_after["button.kitchen_custom_repair"][5] is er.RegistryEntryDisabler.USER
    # Physical MQTT entities are untouched.
    assert {k: v for k, v in after["entities"].items() if v[1] == "mqtt"} == {
        k: v for k, v in before["entities"].items() if v[1] == "mqtt"
    }
    mqtt_entry_id = hass.config_entries.async_entries("mqtt")[0].entry_id
    for device_id, links in before["devices"].items():
        legacy_owner = next((owner for owner in links if owner in PANELS), None)
        if legacy_owner is None:
            assert after["devices"][device_id] == links
            continue
        assert after["devices"][device_id] == {
            mqtt_entry_id: {None},
            ANCHOR_ID: {planned[legacy_owner]},
        }

    runtime = fleet.runtime_data
    assert isinstance(runtime, FleetManager)
    assert set(runtime.panels) == set(planned.values())
    assert {manager.management_id for manager in runtime.panels.values()} == set(PANELS)
    assert MIGRATION_STORAGE_KEY not in hass_storage
    return fleet


def _assert_originals(hass: HomeAssistant, before: State, hass_storage: dict[str, Any]) -> None:
    assert _state(hass) == before
    assert MIGRATION_STORAGE_KEY not in hass_storage
    for entry in hass.config_entries.async_entries(DOMAIN):
        assert entry.state is ConfigEntryState.LOADED
        runtime = entry.runtime_data
        assert isinstance(runtime, FleetManager)
        assert set(runtime.panels) == {entry.entry_id}


async def test_full_migration_preserves_every_identity_and_is_idempotent(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
) -> None:
    del legacy_fleet
    before = _state(hass)
    assert len(_domain_entities(hass)) >= 3 * 6

    outcome = await hass.services.async_call(
        DOMAIN,
        "migrate_to_fleet",
        {"dry_run": False},
        blocking=True,
        return_response=True,
    )
    await hass.async_block_till_done()

    assert outcome is not None
    assert outcome["status"] == "migrated"
    assert outcome["fleet_entry_id"] == ANCHOR_ID
    assert outcome["removed_entry_ids"] == list(SIBLINGS)
    _assert_migrated(hass, before, hass_storage)
    rendered = json.dumps(outcome)
    for secret in (FLEET_PASSWORD, FLEET_USERNAME, ROOT_PASSWORD, host_key("kitchen").split()[1]):
        assert secret not in rendered

    again = await hass.services.async_call(
        DOMAIN, "migrate_to_fleet", {"dry_run": False}, blocking=True, return_response=True
    )
    assert again == {"status": "already_migrated", "dry_run": False, "fleet_entry_id": ANCHOR_ID}
    assert ir.async_get(hass).async_get_issue(DOMAIN, ISSUE_MIGRATION_AVAILABLE) is None


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
    assert _state(hass) == before
    assert MIGRATION_STORAGE_KEY not in hass_storage
    assert all(
        entry.state is ConfigEntryState.LOADED
        for entry in hass.config_entries.async_entries(DOMAIN)
    )
    assert ir.async_get(hass).async_get_issue(DOMAIN, ISSUE_MIGRATION_AVAILABLE) is not None


async def test_duplicate_mesh_priority_blocks_without_mutation(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    hass_storage: dict[str, Any],
) -> None:
    del mqtt_mock
    await _setup_legacy(hass, _entry(KITCHEN_ID, 1), _entry(OFFICE_ID, 1, scene_panel="office"))
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
    assert _state(hass) == before
    assert MIGRATION_STORAGE_KEY not in hass_storage


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

    assert _state(hass) == before
    assert MIGRATION_STORAGE_KEY not in hass_storage


_REVERSIBLE_POINTS = (
    "after_save:planned",
    f"unload:{OFFICE_ID}",
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


def _inject(point: str, error: BaseException) -> Callable[[str], Awaitable[None]]:
    fired = False

    async def checkpoint(name: str) -> None:
        nonlocal fired
        if name == point and not fired:
            fired = True
            raise error

    return checkpoint


@pytest.mark.parametrize("point", _REVERSIBLE_POINTS)
async def test_in_process_failure_before_commit_reverses_exactly(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
    point: str,
) -> None:
    del legacy_fleet
    before = _state(hass)

    outcome = await _run(hass, dry_run=False, checkpoint=_inject(point, InjectedFailure()))
    await hass.async_block_till_done()

    assert outcome == {"status": "reverted", "dry_run": False, "error": "unexpected_error"}
    _assert_originals(hass, before, hass_storage)


@pytest.mark.parametrize("point", _REVERSIBLE_POINTS)
async def test_crash_before_commit_recovers_to_originals_on_restart(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
    point: str,
) -> None:
    del legacy_fleet
    before = _state(hass)

    with pytest.raises(SimulatedCrash):
        await _run(hass, dry_run=False, checkpoint=_inject(point, SimulatedCrash()))
    record = await MigrationJournal(hass).async_load()
    assert record is not None
    assert record.phase is not MigrationPhase.FLEET_VERIFIED

    outcome = await _restart(hass)

    assert outcome is not None and outcome["status"] == "reverted"
    _assert_originals(hass, before, hass_storage)
    # The restored originals migrate cleanly afterwards.
    assert (await _run(hass, dry_run=False))["status"] == "migrated"
    _assert_migrated(hass, before, hass_storage)


@pytest.mark.parametrize(
    "point",
    (
        "after_save:fleet_verified",
        "after_save:siblings_removing",
        f"remove:{KITCHEN_ID}",
        f"remove:{OFFICE_ID}",
        "before_save:complete",
        "after_save:complete",
    ),
)
async def test_crash_after_commit_finishes_sibling_removal_on_restart(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
    point: str,
) -> None:
    del legacy_fleet
    before = _state(hass)

    with pytest.raises(SimulatedCrash):
        await _run(hass, dry_run=False, checkpoint=_inject(point, SimulatedCrash()))

    outcome = await _restart(hass)

    assert outcome is not None
    assert outcome["status"] in {"migrated", "recovered"}
    _assert_migrated(hass, before, hass_storage)


async def test_recovery_retargets_stale_registry_before_removing_a_sibling(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
) -> None:
    """A registry write that never reached disk must not let removal delete entities."""
    del legacy_fleet
    before = _state(hass)
    with pytest.raises(SimulatedCrash):
        await _run(
            hass,
            dry_run=False,
            checkpoint=_inject("after_save:fleet_verified", SimulatedCrash()),
        )
    for entry in hass.config_entries.async_entries(DOMAIN):
        await hass.config_entries.async_unload(entry.entry_id)
    registry = er.async_get(hass)
    stale = registry.async_get_entity_id("binary_sensor", DOMAIN, f"{KITCHEN_ID}_bridge_health")
    assert stale is not None
    registry.async_update_entity(stale, config_entry_id=KITCHEN_ID, config_subentry_id=None)

    await _restart(hass)

    _assert_migrated(hass, before, hass_storage)


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


async def test_failed_reversal_keeps_journal_and_raises_one_issue_then_recovers(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
) -> None:
    del legacy_fleet
    before = _state(hass)

    async def checkpoint(name: str) -> None:
        if name in {"verify_fleet", "reverse_anchor"}:
            raise InjectedFailure

    outcome = await _run(hass, dry_run=False, checkpoint=checkpoint)

    assert outcome["status"] == "recovery_failed"
    record = await MigrationJournal(hass).async_load()
    assert record is not None and record.phase is MigrationPhase.REVERSING
    assert ir.async_get(hass).async_get_issue(DOMAIN, ISSUE_MIGRATION_RECOVERY_FAILED)
    assert (await _run(hass, dry_run=True))["status"] == "recovery_pending"

    resumed = await _run(hass, dry_run=False)
    await hass.async_block_till_done()

    assert resumed["status"] == "reverted"
    _assert_originals(hass, before, hass_storage)
    assert ir.async_get(hass).async_get_issue(DOMAIN, ISSUE_MIGRATION_RECOVERY_FAILED) is None


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

    with patch.object(MigrationCoordinator, "async_recover", recover):
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    assert observed == [ConfigEntryState.NOT_LOADED]
    assert hass.services.has_service(DOMAIN, "migrate_to_fleet")
    issue = ir.async_get(hass).async_get_issue(DOMAIN, ISSUE_MIGRATION_AVAILABLE)
    assert issue is not None and issue.translation_placeholders == {"count": "1"}
    assert await hass.config_entries.async_unload(entry.entry_id)


async def test_managed_update_journal_resolves_a_migrated_panel_owner(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
) -> None:
    """A migrated panel keeps management_id = legacy entry ID, not its fingerprint.

    The canary update journal binds its owner by identity; it must still find the
    migrated panel, or every OTA/recovery on a migrated fleet would fail closed.
    """
    from dataclasses import replace

    from custom_components.brilliant_mqtt.broker import BrokerKind
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
    office = fleet.subentries[planned_subentry_id(OFFICE_ID)].data
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


async def test_commit_waits_for_durable_config_entry_storage(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
) -> None:
    """The commit point is journaled only after the fleet entry is proven on disk."""
    del legacy_fleet
    before = _state(hass)
    waited: list[str] = []

    async def unproven(candidate_hass: HomeAssistant, entry: Any) -> None:
        assert candidate_hass is hass
        waited.append(entry.entry_id)
        record = await MigrationJournal(hass).async_load()
        assert record is not None and record.phase is MigrationPhase.REGISTRIES_RETARGETED
        raise TimeoutError

    outcome = await MigrationCoordinator(hass, persistence_waiter=unproven).async_run(dry_run=False)
    await hass.async_block_till_done()

    assert waited == [ANCHOR_ID]
    assert outcome == {
        "status": "reverted",
        "dry_run": False,
        "error": "config_entry_storage_unavailable",
    }
    _assert_originals(hass, before, hass_storage)


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


async def test_corrupt_journal_at_startup_fails_closed_with_one_issue(
    hass: HomeAssistant,
    legacy_fleet: list[MockConfigEntry],
    hass_storage: dict[str, Any],
) -> None:
    del legacy_fleet
    before = _state(hass)
    hass_storage[MIGRATION_STORAGE_KEY] = {
        "version": 1,
        "minor_version": 1,
        "key": MIGRATION_STORAGE_KEY,
        "data": {"schema_version": 1, "mqtt_password": FLEET_PASSWORD},
    }

    outcome = await MigrationCoordinator(hass).async_recover()

    assert outcome == {"status": "recovery_failed", "error": "journal_corrupt"}
    issue = ir.async_get(hass).async_get_issue(DOMAIN, ISSUE_MIGRATION_RECOVERY_FAILED)
    assert issue is not None
    assert FLEET_PASSWORD not in json.dumps(issue.translation_placeholders)
    assert (await _run(hass, dry_run=False))["status"] == "recovery_failed"
    assert _state(hass) == before


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
    control = {"ha_control_enabled": True, "scene_actions": action}
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
    for loaded in hass.config_entries.async_entries(DOMAIN):
        await hass.config_entries.async_unload(loaded.entry_id)
