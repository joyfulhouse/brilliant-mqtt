"""Off-panel tests for the __main__ session wiring (M11 Step 3).

A real session needs the panel bus and a live broker, so only the pure
pieces are unit-tested here: the panel-scope predicate, and the leadership
gate the session builds — a mesh Bridge whose include predicate consults
``leader.is_leader``, so a non-leader (or fresh ex-leader, whose _on_change
stays registered after withdraw) publishes nothing on the mesh namespace.
"""

from __future__ import annotations

import json

from brilliant_mqtt.__main__ import _is_panel_device
from brilliant_mqtt.bridge import Bridge
from brilliant_mqtt.mesh_leader import MESH_LEADER_TOPIC, MeshLeader
from brilliant_mqtt.model import BrilliantDevice, DeviceKind, Variable
from tests.fakes import FakeBus, FakeClock, FakeMqtt

HB = 10.0


def _mesh_dimmer() -> BrilliantDevice:
    """A mesh load on the virtual ble_mesh bus device (live-verified shape)."""
    return BrilliantDevice(
        device_id="ble_mesh",
        peripheral_id="018691f1749b000701c4e689967b8e62",
        name="Office Desk Lights",
        kind=DeviceKind.LIGHT,
        peripheral_type=27,
        variables={
            "on": Variable("on", "0"),
            "intensity": Variable("intensity", "600"),
            "dimmable": Variable("dimmable", "1"),
        },
    )


def _panel_dimmer() -> BrilliantDevice:
    return BrilliantDevice(
        device_id="device_001",
        peripheral_id="gangbox_peripheral_0",
        name="Lights",
        kind=DeviceKind.LIGHT,
        variables={"on": Variable("on", "0")},
    )


class TestPanelScopePredicate:
    def test_panel_device_in_scope(self) -> None:
        assert _is_panel_device(_panel_dimmer()) is True

    def test_mesh_device_out_of_scope(self) -> None:
        assert _is_panel_device(_mesh_dimmer()) is False


def _data_topics(mqtt: FakeMqtt) -> list[str]:
    """Topics published OTHER than the leadership claim (the gated output)."""
    return [p[0] for p in mqtt.published if p[0] != MESH_LEADER_TOPIC]


class TestLeadershipGate:
    """The include-predicate gate the session wires for the mesh bridge.

    Step 2 left _on_change registered after withdraw(); checking
    leader.is_leader INSIDE the include predicate is what actually silences a
    non-leader/ex-leader for pushes AND polls — proven here end to end.
    """

    async def test_gate_silences_non_leader_then_opens_then_closes(self) -> None:
        device = _mesh_dimmer()
        bus = FakeBus([device])
        mqtt = FakeMqtt()
        clock = FakeClock()

        def _mesh_in_scope(d: BrilliantDevice) -> bool:
            # Mirrors the late-binding closure in __main__._run_session.
            return d.device_id == "ble_mesh" and leader.is_leader

        mesh_bridge = Bridge(bus, mqtt, "mesh", include=_mesh_in_scope)
        leader = MeshLeader(
            mqtt,
            "office",
            1,
            HB,
            on_acquire=mesh_bridge.reconcile,
            on_lose=mesh_bridge.withdraw,
            clock=clock,
        )
        await leader.start()

        # Before leadership: pushes and polls publish NOTHING on the mesh
        # namespace, although the bus fan-out delivers the device here.
        await bus.emit(device)
        await mesh_bridge.poll_once()
        assert _data_topics(mqtt) == []

        # Acquisition (on_acquire = reconcile) opens the gate and publishes.
        await leader.tick()
        clock.advance(HB)
        await leader.tick()
        assert leader.is_leader
        assert any(t.startswith("homeassistant/") for t in _data_topics(mqtt))
        assert f"brilliant/mesh/{device.peripheral_id}/state" in _data_topics(mqtt)

        # A better claim arrives: step-down withdraws — and the gate keeps
        # the STILL-REGISTERED _on_change and the polls silent afterwards.
        await mqtt.inject(MESH_LEADER_TOPIC, json.dumps({"panel": "attic", "priority": 1}))
        await leader.tick()
        assert leader.is_leader is False
        mqtt.published.clear()
        await bus.emit(device)
        await mesh_bridge.poll_once()
        assert _data_topics(mqtt) == []
