"""Tests for the reconciling Home Assistant mirror orchestrator."""

import logging

import pytest

from brilliant_ha_mirror.config import Settings
from brilliant_ha_mirror.mapping import HaEntity
from brilliant_ha_mirror.mirror import Mirror
from tests.fakes import FakeHaClient, FakePeripheralHost


def _settings(room_overrides: dict[str, str] | None = None) -> Settings:
    return Settings(
        panel="p",
        ha_ws_url="ws://x",
        ha_token="t",
        room_overrides=room_overrides or {},
    )


async def test_start_registers_only_supported_entities() -> None:
    ha = FakeHaClient(
        entities=[
            HaEntity("light.k", "on", {"brightness": 200}, "Kitchen"),
            HaEntity("climate.t", "heat", {}, "Kitchen"),
        ]
    )
    host = FakePeripheralHost()
    await Mirror(ha, host, _settings()).start()
    assert host.registered_types == [27]
    assert len(host.registered) == 1


async def test_state_change_updates_variable() -> None:
    ha = FakeHaClient(entities=[HaEntity("switch.s", "off", {}, "Kitchen")])
    host = FakePeripheralHost()
    mirror = Mirror(ha, host, _settings())
    await mirror.start()
    await ha.emit_state(HaEntity("switch.s", "on", {}, "Kitchen"))
    name = host.registered[0]
    assert host.variables[name]["on"] == "1"


async def test_panel_command_calls_ha_service() -> None:
    ha = FakeHaClient(entities=[HaEntity("switch.s", "off", {}, "Kitchen")])
    host = FakePeripheralHost()
    mirror = Mirror(ha, host, _settings())
    await mirror.start()
    name = host.registered[0]
    await host.fire_command(name, "on", "1")
    assert ha.calls[-1].service == "turn_on"
    assert ha.calls[-1].data["entity_id"] == "switch.s"


async def test_reconcile_deletes_unlabeled_entity() -> None:
    ha = FakeHaClient(entities=[HaEntity("switch.s", "off", {}, "Kitchen")])
    host = FakePeripheralHost()
    mirror = Mirror(ha, host, _settings())
    await mirror.start()
    ha.entities = []
    await mirror.reconcile()
    assert host.deleted == host.registered


async def test_stop_deletes_all_hosted() -> None:
    ha = FakeHaClient(entities=[HaEntity("switch.s", "off", {}, "Kitchen")])
    host = FakePeripheralHost()
    mirror = Mirror(ha, host, _settings())
    await mirror.start()
    await mirror.stop()
    assert host.deleted == host.registered


async def test_friendly_name_used_when_present() -> None:
    ha = FakeHaClient(
        entities=[
            HaEntity(
                "light.k",
                "on",
                {"friendly_name": "Kitchen Light"},
                "Kitchen",
            )
        ]
    )
    host = FakePeripheralHost()
    await Mirror(ha, host, _settings()).start()
    assert host.registered == ["HA Kitchen Light"]


async def test_entity_id_used_when_friendly_name_is_empty() -> None:
    ha = FakeHaClient(entities=[HaEntity("switch.s", "off", {"friendly_name": ""}, "Kitchen")])
    host = FakePeripheralHost()
    await Mirror(ha, host, _settings()).start()
    assert host.registered == ["HA switch.s"]


async def test_reconcile_refreshes_existing_entity() -> None:
    ha = FakeHaClient(entities=[HaEntity("switch.s", "off", {}, "Kitchen")])
    host = FakePeripheralHost()
    mirror = Mirror(ha, host, _settings())
    await mirror.start()
    ha.entities = [HaEntity("switch.s", "on", {}, "Kitchen")]
    await mirror.reconcile()
    assert host.variables[host.registered[0]]["on"] == "1"


async def test_command_handlers_bind_their_own_entity_ids() -> None:
    ha = FakeHaClient(
        entities=[
            HaEntity("switch.first", "off", {}, "Kitchen"),
            HaEntity("switch.second", "off", {}, "Kitchen"),
        ]
    )
    host = FakePeripheralHost()
    await Mirror(ha, host, _settings()).start()
    await host.fire_command(host.registered[0], "on", "1")
    await host.fire_command(host.registered[1], "on", "1")
    assert [call.data["entity_id"] for call in ha.calls] == [
        "switch.first",
        "switch.second",
    ]


async def test_duplicate_friendly_names_get_unique_peripheral_names() -> None:
    # Two entities sharing a friendly name must NOT collide to one peripheral
    # (which would silently drop the second) — review finding I3.
    ha = FakeHaClient(
        entities=[
            HaEntity("light.a", "on", {"friendly_name": "Lamp"}, "Kitchen"),
            HaEntity("light.b", "on", {"friendly_name": "Lamp"}, "Bedroom"),
        ]
    )
    host = FakePeripheralHost()
    await Mirror(ha, host, _settings()).start()
    assert len(host.registered) == 2
    assert len(set(host.registered)) == 2  # distinct names, nothing dropped
    assert all(name.startswith("HA Lamp") for name in host.registered)


async def test_unique_friendly_name_stays_clean() -> None:
    ha = FakeHaClient(entities=[HaEntity("light.k", "on", {"friendly_name": "Kitchen"}, "Kitchen")])
    host = FakePeripheralHost()
    await Mirror(ha, host, _settings()).start()
    assert host.registered == ["HA Kitchen"]  # no disambiguation when unique


async def test_initial_hosting_assigns_matching_brilliant_room() -> None:
    ha = FakeHaClient(entities=[HaEntity("switch.s", "off", {}, "kitchen")])
    host = FakePeripheralHost(rooms={"opaque-kitchen-id": "Kitchen"})

    await Mirror(ha, host, _settings()).start()

    assert host.room_assignments[host.registered[0]] == ["opaque-kitchen-id"]


async def test_room_override_precedes_automatic_name_match() -> None:
    ha = FakeHaClient(entities=[HaEntity("switch.s", "off", {}, "Back Yard")])
    host = FakePeripheralHost(rooms={"automatic-id": "Back Yard", "override-id": "Patio"})

    await Mirror(
        ha,
        host,
        _settings({"Back Yard": "override-id"}),
    ).start()

    assert host.room_assignments[host.registered[0]] == ["override-id"]


async def test_unmatched_area_is_unassigned_and_logged_only_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    ha = FakeHaClient(entities=[HaEntity("switch.s", "off", {}, "Garage")])
    host = FakePeripheralHost(rooms={"kitchen-id": "Kitchen"})
    mirror = Mirror(ha, host, _settings())

    with caplog.at_level(logging.DEBUG):
        await mirror.start()
        await mirror.reconcile()

    assert host.room_assignments[host.registered[0]] == []
    messages = [
        record.message for record in caplog.records if "no Brilliant room" in record.message
    ]
    assert len(messages) == 1


async def test_reconcile_reassigns_when_ha_area_changes() -> None:
    ha = FakeHaClient(entities=[HaEntity("switch.s", "off", {}, "Kitchen")])
    host = FakePeripheralHost(rooms={"kitchen-id": "Kitchen", "office-id": "Office"})
    mirror = Mirror(ha, host, _settings())
    await mirror.start()
    host.room_assignment_calls.clear()

    ha.entities = [HaEntity("switch.s", "off", {}, "Office")]
    await mirror.reconcile()

    assert host.room_assignment_calls == [(host.registered[0], ["office-id"])]


async def test_reconcile_reasserts_when_brilliant_catalog_changes() -> None:
    ha = FakeHaClient(entities=[HaEntity("switch.s", "off", {}, "Kitchen")])
    host = FakePeripheralHost(rooms={"old-id": "Kitchen"})
    mirror = Mirror(ha, host, _settings())
    await mirror.start()
    host.room_assignment_calls.clear()

    host.rooms = {"new-id": "Kitchen"}
    await mirror.reconcile()

    assert host.room_assignment_calls == [(host.registered[0], ["new-id"])]


async def test_reconcile_does_not_rewrite_unchanged_room_assignment() -> None:
    ha = FakeHaClient(entities=[HaEntity("switch.s", "off", {}, "Kitchen")])
    host = FakePeripheralHost(rooms={"kitchen-id": "Kitchen"})
    mirror = Mirror(ha, host, _settings())
    await mirror.start()
    host.room_assignment_calls.clear()

    await mirror.reconcile()

    assert host.room_assignment_calls == []


async def test_reconcile_survives_room_catalog_failure_and_recovers(
    caplog: pytest.LogCaptureFixture,
) -> None:
    ha = FakeHaClient(entities=[HaEntity("switch.s", "off", {}, "Kitchen")])
    host = FakePeripheralHost(rooms={"kitchen-id": "Kitchen"})
    host.get_rooms_error = RuntimeError("room observer unavailable")
    mirror = Mirror(ha, host, _settings())

    with caplog.at_level(logging.WARNING):
        await mirror.start()

    assert host.registered == ["HA switch.s"]
    assert host.room_assignment_calls == []
    assert "room observer unavailable" in caplog.text

    host.get_rooms_error = None
    await mirror.reconcile()
    assert host.room_assignment_calls == [("HA switch.s", ["kitchen-id"])]
