"""Tests for the reconciling Home Assistant mirror orchestrator."""

from brilliant_ha_mirror.config import Settings
from brilliant_ha_mirror.mapping import HaEntity
from brilliant_ha_mirror.mirror import Mirror
from tests.fakes import FakeHaClient, FakePeripheralHost


def _settings() -> Settings:
    return Settings(panel="p", ha_ws_url="ws://x", ha_token="t")


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
