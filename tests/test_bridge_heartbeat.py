"""Tests: Bridge fires the heartbeat callback after a successful bus read."""

from __future__ import annotations

import asyncio

import pytest

from brilliant_mqtt.bridge import Bridge, HotPollReadTimeout
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


class _TransientTimeoutBus(FakeBus):
    """Fail one scoped snapshot read, then recover on the same bus object."""

    def __init__(self, devices: list[BrilliantDevice], error: Exception) -> None:
        super().__init__(devices)
        self._error: Exception | None = error

    async def get_all(self) -> list[BrilliantDevice]:
        if self._error is not None:
            error, self._error = self._error, None
            raise error
        return await super().get_all()


class _TimeoutMqtt(FakeMqtt):
    def __init__(self, error: Exception) -> None:
        super().__init__()
        self._error = error

    async def publish(
        self,
        topic: str,
        payload: str,
        retain: bool = False,
        qos: int = 0,
    ) -> None:
        del topic, payload, retain, qos
        raise self._error


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
@pytest.mark.parametrize(
    "timeout_type",
    [TimeoutError, asyncio.TimeoutError],
    ids=["builtins", "asyncio"],
)
async def test_poll_once_wraps_bus_read_timeout_without_beating_then_recovers(
    timeout_type: type[Exception],
) -> None:
    beats: list[int] = []
    mqtt = FakeMqtt()
    b = Bridge(
        _TransientTimeoutBus([_light()], timeout_type("thrift request timed out")),
        mqtt,
        "mesh",
        heartbeat=lambda: beats.append(1),
    )

    with pytest.raises(HotPollReadTimeout, match="hot poll bus read timed out") as raised:
        await b.poll_once()

    assert isinstance(raised.value.__cause__, timeout_type)
    assert beats == []
    assert mqtt.published == []

    await b.poll_once()
    assert beats == [1]
    assert mqtt.published


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "timeout_type",
    [TimeoutError, asyncio.TimeoutError],
    ids=["builtins", "asyncio"],
)
async def test_poll_once_does_not_swallow_mqtt_publish_timeout(
    timeout_type: type[Exception],
) -> None:
    b = Bridge(
        FakeBus([_light()]),
        _TimeoutMqtt(timeout_type("mqtt publish timed out")),
        "mesh",
    )

    with pytest.raises(timeout_type, match="mqtt publish timed out") as raised:
        await b.poll_once()

    assert not isinstance(raised.value, HotPollReadTimeout)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "timeout_type",
    [TimeoutError, asyncio.TimeoutError],
    ids=["builtins", "asyncio"],
)
async def test_reconcile_does_not_swallow_bus_read_timeout(
    timeout_type: type[Exception],
) -> None:
    b = Bridge(
        _TransientTimeoutBus([_light()], timeout_type("thrift request timed out")),
        FakeMqtt(),
        "mesh",
    )

    with pytest.raises(timeout_type, match="thrift request timed out") as raised:
        await b.reconcile()

    assert not isinstance(raised.value, HotPollReadTimeout)


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
async def test_prefetched_poll_does_not_read_or_beat() -> None:
    beats: list[int] = []
    mqtt = FakeMqtt()
    b = Bridge(_RaisingBus([]), mqtt, "mesh", heartbeat=lambda: beats.append(1))

    await b.poll_once([_light()])

    assert beats == []
    assert mqtt.published


@pytest.mark.asyncio
async def test_no_heartbeat_is_noop() -> None:
    b = Bridge(FakeBus([_light()]), FakeMqtt(), "mesh")  # heartbeat=None
    await b.reconcile()
    await b.poll_once()  # must not raise
