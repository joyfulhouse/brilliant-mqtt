"""Tests: Bridge records reconciled-var commands into DesiredState (Task 3)."""

from __future__ import annotations

from pathlib import Path

import pytest

from brilliant_mqtt.bridge import Bridge
from brilliant_mqtt.desired_state import DesiredState
from brilliant_mqtt.model import BrilliantDevice, DeviceKind, Variable
from tests.fakes import FakeBus, FakeMqtt


def _mesh_light(pid: str, **vars_: str) -> BrilliantDevice:
    return BrilliantDevice(
        device_id="ble_mesh",
        peripheral_id=pid,
        name=pid,
        kind=DeviceKind.LIGHT,
        peripheral_type=27,
        variables={k: Variable(k, v) for k, v in vars_.items()},
    )


@pytest.mark.asyncio
async def test_command_records_reconciled_var(tmp_path: Path) -> None:
    dev = _mesh_light("pidA", enable_motion_score="0", motion_low_threshold="20", on="0")
    bus = FakeBus([dev])
    mqtt = FakeMqtt()
    ds = DesiredState(tmp_path / "mesh.json")
    bridge = Bridge(bus, mqtt, "mesh", desired=ds)
    await bridge.reconcile()  # registers command topics + snapshots device

    await mqtt.inject("brilliant/mesh/pidA/set_enable_motion_score", "ON")

    assert ds.wanted("pidA") == {"enable_motion_score": "1"}


@pytest.mark.asyncio
async def test_command_ignores_non_reconciled_var(tmp_path: Path) -> None:
    dev = _mesh_light("pidA", enable_motion_score="0", on="0")
    bus = FakeBus([dev])
    mqtt = FakeMqtt()
    ds = DesiredState(tmp_path / "mesh.json")
    bridge = Bridge(bus, mqtt, "mesh", desired=ds)
    await bridge.reconcile()

    await mqtt.inject("brilliant/mesh/pidA/set_on", '{"state": "ON"}')

    assert ds.wanted("pidA") == {}


@pytest.mark.asyncio
async def test_command_without_desired_does_not_crash(tmp_path: Path) -> None:
    dev = _mesh_light("pidA", enable_motion_score="0", on="0")
    bus = FakeBus([dev])
    mqtt = FakeMqtt()
    bridge = Bridge(bus, mqtt, "mesh", desired=None)
    await bridge.reconcile()

    await mqtt.inject("brilliant/mesh/pidA/set_enable_motion_score", "ON")

    # write still routed to the bus; just nothing recorded
    assert any(vs.name == "enable_motion_score" for _, _, sets in bus.commands for vs in sets)
