"""Tests for the test fakes' Milestone 11 fan-out plumbing.

The bridge tests treat the fakes as the adapter contract, so the new
multi-consumer semantics are pinned here: several components (panel bridge +
mesh publisher) register their own change/command callbacks on ONE shared
adapter, and the mesh leader steps down by unsubscribing its command topics.
"""

from __future__ import annotations

from brilliant_mqtt.model import BrilliantDevice, DeviceKind, Variable
from tests.fakes import FakeBus, FakeMqtt


def _mesh_switch() -> BrilliantDevice:
    return BrilliantDevice(
        device_id="ble_mesh",
        peripheral_id="mesh_switch_1",
        name="Mesh Switch",
        kind=DeviceKind.SWITCH,
        variables={"on": Variable("on", "0")},
    )


class TestFakeBusFanout:
    async def test_emit_reaches_all_registered_callbacks(self) -> None:
        bus = FakeBus([])
        seen_a: list[str] = []
        seen_b: list[str] = []

        async def cb_a(device: BrilliantDevice) -> None:
            seen_a.append(device.peripheral_id)

        async def cb_b(device: BrilliantDevice) -> None:
            seen_b.append(device.peripheral_id)

        bus.on_change(cb_a)
        bus.on_change(cb_b)
        await bus.emit(_mesh_switch())

        assert seen_a == ["mesh_switch_1"]
        assert seen_b == ["mesh_switch_1"]


class TestFakeBusScopedRead:
    async def test_returns_matching_peripheral(self) -> None:
        scene = BrilliantDevice(
            device_id="configuration_virtual_device",
            peripheral_id="scene_configuration",
            name="Scene Configuration",
            kind=DeviceKind.UNKNOWN,
        )
        bus = FakeBus([_mesh_switch(), scene])

        result = await bus.get_peripheral("configuration_virtual_device", "scene_configuration")

        assert result is scene

    async def test_returns_none_when_ids_do_not_match(self) -> None:
        bus = FakeBus([_mesh_switch()])

        result = await bus.get_peripheral("configuration_virtual_device", "scene_configuration")

        assert result is None


class TestFakeMqttFanout:
    async def test_inject_reaches_all_registered_callbacks(self) -> None:
        mqtt = FakeMqtt()
        seen_a: list[tuple[str, str]] = []
        seen_b: list[tuple[str, str]] = []

        async def cb_a(topic: str, payload: str) -> None:
            seen_a.append((topic, payload))

        async def cb_b(topic: str, payload: str) -> None:
            seen_b.append((topic, payload))

        mqtt.on_command(cb_a)
        mqtt.on_command(cb_b)
        await mqtt.inject("brilliant/home/mesh_switch_1/set", "ON")

        assert seen_a == [("brilliant/home/mesh_switch_1/set", "ON")]
        assert seen_b == [("brilliant/home/mesh_switch_1/set", "ON")]


class TestFakeMqttUnsubscribe:
    async def test_unsubscribe_records_and_removes_subscription(self) -> None:
        mqtt = FakeMqtt()
        await mqtt.subscribe("brilliant/home/a/set")
        await mqtt.subscribe("brilliant/home/b/set")

        await mqtt.unsubscribe("brilliant/home/a/set")

        assert mqtt.unsubscriptions == ["brilliant/home/a/set"]
        assert mqtt.subscriptions == ["brilliant/home/b/set"]

    async def test_unsubscribe_unknown_topic_is_still_recorded(self) -> None:
        mqtt = FakeMqtt()

        await mqtt.unsubscribe("brilliant/home/never/set")

        assert mqtt.unsubscriptions == ["brilliant/home/never/set"]
        assert mqtt.subscriptions == []
