"""Tests: Bridge fires the heartbeat callback after a successful bus read."""

from __future__ import annotations

import pytest

from brilliant_mqtt.bridge import Bridge
from brilliant_mqtt.model import BrilliantDevice, DeviceKind, Variable
from tests.fakes import FakeBus, FakeMqtt


def _light(pid: str = "p") -> BrilliantDevice:
    return BrilliantDevice("ble_mesh", pid, pid, DeviceKind.LIGHT, 27, {"on": Variable("on", "1")})


class _RaisingBus(FakeBus):
    """FakeBus whose get_all() always raises — stands in for the message_bus
    wedge (in production bus.start() raises before get_all() is ever reached;
    a get_all()-raises is the equivalent load-bearing failure for the bridge:
    the heartbeat must not fire when the read that feeds it never completed)."""

    async def get_all(self) -> list[BrilliantDevice]:
        raise RuntimeError("bus wedged")


@pytest.mark.asyncio
async def test_reconcile_does_not_beat_when_get_all_raises() -> None:
    beats: list[int] = []
    b = Bridge(_RaisingBus([_light()]), FakeMqtt(), "mesh", heartbeat=lambda: beats.append(1))
    with pytest.raises(RuntimeError):
        await b.reconcile()
    assert beats == []


@pytest.mark.asyncio
async def test_poll_once_does_not_beat_when_get_all_raises() -> None:
    beats: list[int] = []
    b = Bridge(_RaisingBus([_light()]), FakeMqtt(), "mesh", heartbeat=lambda: beats.append(1))
    with pytest.raises(RuntimeError):
        await b.poll_once()
    assert beats == []


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
