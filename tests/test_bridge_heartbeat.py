"""Tests: Bridge fires the heartbeat callback after a successful bus read."""

from __future__ import annotations

import pytest

from brilliant_mqtt.bridge import Bridge
from brilliant_mqtt.model import BrilliantDevice, DeviceKind, Variable
from tests.fakes import FakeBus, FakeMqtt


def _light(pid: str = "p") -> BrilliantDevice:
    return BrilliantDevice("ble_mesh", pid, pid, DeviceKind.LIGHT, 27, {"on": Variable("on", "1")})


@pytest.mark.asyncio
async def test_reconcile_beats() -> None:
    beats = []
    b = Bridge(FakeBus([_light()]), FakeMqtt(), "mesh", heartbeat=lambda: beats.append(1))
    await b.reconcile()
    assert beats  # at least one beat after get_all


@pytest.mark.asyncio
async def test_poll_beats() -> None:
    beats = []
    b = Bridge(FakeBus([_light()]), FakeMqtt(), "mesh", heartbeat=lambda: beats.append(1))
    await b.poll_once()
    assert beats


@pytest.mark.asyncio
async def test_no_heartbeat_is_noop() -> None:
    b = Bridge(FakeBus([_light()]), FakeMqtt(), "mesh")  # heartbeat=None
    await b.reconcile()
    await b.poll_once()  # must not raise
