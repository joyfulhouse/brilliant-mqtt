"""Tests for the Home Assistant mirror fakes."""

from brilliant_ha_mirror.mapping import HaEntity, PeripheralSpec, ServiceCall
from tests.fakes import FakeHaClient, FakePeripheralHost


async def test_fake_host_register_update_command_delete() -> None:
    host = FakePeripheralHost(rooms={"room-kitchen": "Kitchen"})
    seen: list[tuple[str, str]] = []

    async def on_cmd(var: str, value: str) -> None:
        seen.append((var, value))

    spec = PeripheralSpec(27, {"on": "0"}, frozenset({"on"}))
    await host.register("HA L", spec, on_cmd)

    assert host.registered == ["HA L"]
    assert host.registered_types == [27]
    assert host.specs["HA L"] == spec
    assert host.variables["HA L"]["on"] == "0"

    await host.update_variables("HA L", {"on": "1"})
    assert host.variables["HA L"]["on"] == "1"

    assert await host.get_rooms() == {"room-kitchen": "Kitchen"}
    await host.set_room_assignment("HA L", ["room-kitchen"])
    assert host.room_assignments["HA L"] == ["room-kitchen"]

    await host.fire_command("HA L", "on", "0")
    assert seen == [("on", "0")]

    await host.delete("HA L")
    assert host.deleted == ["HA L"]


async def test_fake_ha_client_entities_calls_and_emit() -> None:
    got: list[str] = []
    entity = HaEntity("light.k", "on", {}, "Kitchen")
    ha = FakeHaClient(entities=[entity])

    async def on_state(changed: HaEntity) -> None:
        got.append(changed.entity_id)

    ha.on_state_change(on_state)

    assert await ha.get_entities("brilliant") == [entity]

    call = ServiceCall("light", "turn_off", {"entity_id": "light.k"})
    await ha.call_service(call)
    assert ha.calls == [call]

    await ha.emit_state(HaEntity("light.k", "off", {}, "Kitchen"))
    assert got == ["light.k"]
