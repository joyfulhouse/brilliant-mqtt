"""Bounded MQTT transport backlog upstream of the per-command lanes (issue #90).

aiomqtt 2.5.1 defaults its incoming-message queue to an UNBOUNDED
``asyncio.Queue`` (``maxsize=0``). When a lossless command lane fills and its
sole reader blocks in :meth:`_TopicDispatcher.dispatch`, paho keeps enqueueing
upstream, so a stalled bus plus an automation burst grows memory without bound
until the resource-capped agent (``MemoryMax=96M``) is restarted.

These tests exercise the custom bounded queue aiomqtt constructs via its
``queue_type`` hook: it bounds BOTH message count and cumulative payload bytes,
coalesces latest-wins command topics at admission (mirroring ``_LaneQueue.put``
one layer earlier), and — instead of aiomqtt's silent "Discarding message"
drop — trips an OBSERVABLE overload latch the runner turns into a session
rebuild. They follow the established mqttio pattern: a REAL ``AioMqttAdapter``
(real ``__init__``, real ``Settings``) with its ``aiomqtt`` client swapped for a
fake, so no network I/O ever occurs.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from typing import cast

import aiomqtt
import pytest

from brilliant_mqtt import mqttio
from brilliant_mqtt.config import Settings
from brilliant_mqtt.mqttio import AioMqttAdapter


def _settings(**overrides: object) -> Settings:
    base: dict[str, object] = {
        "panel": "office",
        "mqtt_host": "broker.invalid",
        "mqtt_username": "u",
        "mqtt_password": "p",
    }
    base.update(overrides)
    return Settings(**base)  # type: ignore[arg-type]


def _msg(topic: str, payload: bytes = b"x") -> aiomqtt.Message:
    """A production-shaped inbound message (paho hands aiomqtt bytes payloads)."""
    return aiomqtt.Message(
        topic=topic,
        payload=payload,
        qos=1,
        retain=False,
        mid=0,
        properties=None,
    )


# A non-coalescible command topic (a mute switch: ``set_muted`` is not a
# number-aux-var), matching the issue's own ``set_muted`` saturation repro.
_MUTE = "brilliant/office/media/set_muted"
# An idempotent absolute-state setter — the only class eligible for latest-wins.
_LIGHT = "brilliant/office/light/set"


def _queue(latch: mqttio._TransportOverloadLatch, *, maxsize: int) -> mqttio._BoundedTransportQueue:
    return cast(
        mqttio._BoundedTransportQueue,
        mqttio._transport_queue_type(latch)(maxsize=maxsize),
    )


# -- Count bound ---------------------------------------------------------------


def test_transport_queue_bounds_message_count_and_trips_latch() -> None:
    latch = mqttio._TransportOverloadLatch()
    queue = _queue(latch, maxsize=3)

    for index in range(3):
        queue.put_nowait(_msg(f"brilliant/office/scene{index}/trigger"))
    assert queue.qsize() == 3

    # aiomqtt calls put_nowait and catches asyncio.QueueFull (client._on_message)
    # — the bound MUST raise that exact type, not a bespoke error that would
    # escape into paho and break the read loop.
    with pytest.raises(asyncio.QueueFull):
        queue.put_nowait(_msg("brilliant/office/scene9/trigger"))

    assert queue.qsize() == 3  # bounded — not grown past maxsize
    assert latch.consume() is True  # observable overload
    assert latch.consume() is False  # latch cleared on consume


# -- Byte bound ----------------------------------------------------------------


def test_transport_queue_bounds_payload_bytes_independently_of_count() -> None:
    latch = mqttio._TransportOverloadLatch()
    # A generous count cap so the BYTE budget is the sole binding constraint.
    queue = _queue(latch, maxsize=10_000)
    chunk = b"z" * (100 * 1024)  # 100 KiB each

    queue.put_nowait(_msg("brilliant/office/scene0/trigger", chunk))
    queue.put_nowait(_msg("brilliant/office/scene1/trigger", chunk))
    assert queue.qsize() == 2

    with pytest.raises(asyncio.QueueFull):
        queue.put_nowait(_msg("brilliant/office/scene2/trigger", chunk))

    assert queue.qsize() == 2
    assert queue.queued_bytes <= mqttio._TRANSPORT_QUEUE_MAX_BYTES
    assert latch.consume() is True


def test_get_nowait_releases_the_byte_budget() -> None:
    latch = mqttio._TransportOverloadLatch()
    queue = _queue(latch, maxsize=8)
    message = _msg("brilliant/office/scene0/trigger", b"z" * 1000)

    queue.put_nowait(message)
    assert queue.queued_bytes == mqttio._message_bytes(message)

    queue.get_nowait()
    assert queue.queued_bytes == 0
    assert latch.consume() is False


# -- Latest-wins coalescing at admission --------------------------------------


def test_latest_wins_topic_coalesces_at_admission_without_growing_backlog() -> None:
    latch = mqttio._TransportOverloadLatch()
    queue = _queue(latch, maxsize=3)
    assert mqttio._is_latest_wins_topic(_LIGHT)

    queue.put_nowait(_msg(_LIGHT, b"ON"))
    queue.put_nowait(_msg(_LIGHT, b"OFF"))
    queue.put_nowait(_msg(_LIGHT, b"BRIGHT"))

    assert queue.qsize() == 1
    latest = queue.get_nowait()
    assert latest.payload == b"BRIGHT"  # latest wins
    assert queue.queued_bytes == 0
    assert latch.consume() is False  # coalescing never overloads


def test_latest_wins_coalescing_absorbs_a_flood_without_tripping_the_bound() -> None:
    latch = mqttio._TransportOverloadLatch()
    queue = _queue(latch, maxsize=8)

    for value in range(10_000):
        queue.put_nowait(_msg(_LIGHT, str(value).encode()))

    assert queue.qsize() == 1
    assert queue.get_nowait().payload == b"9999"
    assert latch.consume() is False


def test_non_number_aux_setter_is_not_coalesced() -> None:
    latch = mqttio._TransportOverloadLatch()
    queue = _queue(latch, maxsize=8)
    # set_muted targets a switch, not a number-aux-var: NOT latest-wins, so two
    # of them are distinct queued work even on the same topic.
    assert not mqttio._is_latest_wins_topic(_MUTE)

    queue.put_nowait(_msg(_MUTE, b"true"))
    queue.put_nowait(_msg(_MUTE, b"false"))

    assert queue.qsize() == 2


# -- The issue's repro at the queue layer -------------------------------------


def test_non_coalescible_flood_stays_bounded_and_observably_overloads() -> None:
    latch = mqttio._TransportOverloadLatch()
    queue = _queue(latch, maxsize=mqttio._TRANSPORT_QUEUE_MAXSIZE)

    admitted = 0
    overflowed = 0
    for _ in range(10_000):  # the issue injects 10,000 messages
        try:
            queue.put_nowait(_msg(_MUTE, b"true"))
            admitted += 1
        except asyncio.QueueFull:
            overflowed += 1

    # Bounded work / memory (issue: upstream pending would be 10000, maxsize=0).
    assert queue.qsize() == mqttio._TRANSPORT_QUEUE_MAXSIZE
    assert queue.queued_bytes <= mqttio._TRANSPORT_QUEUE_MAX_BYTES
    assert admitted == mqttio._TRANSPORT_QUEUE_MAXSIZE
    assert overflowed == 10_000 - mqttio._TRANSPORT_QUEUE_MAXSIZE
    # Distinct from a silent drop: the overload is latched for the runner.
    assert latch.consume() is True


# -- Latest-wins replacement over the byte budget keeps the NEWEST -------------


def test_latest_wins_replacement_over_budget_keeps_newest_then_trips() -> None:
    """A latest-wins replacement whose newer payload pushes total over the byte
    budget must keep the NEWEST value (the point of latest-wins) and only THEN
    trip — a pre-rebuild drain applies the freshest command, not a stale one."""
    latch = mqttio._TransportOverloadLatch()
    small = b"a" * 100
    big = b"b" * 400
    # Budget admits the small pending but not the larger replacement.
    budget = mqttio._message_bytes(_msg(_LIGHT, small)) + 50
    queue = mqttio._BoundedTransportQueue(maxsize=8, max_bytes=budget, overload=latch)

    queue.put_nowait(_msg(_LIGHT, small))
    with pytest.raises(asyncio.QueueFull):
        queue.put_nowait(_msg(_LIGHT, big))  # over budget -> trips

    assert queue.qsize() == 1  # coalesced in place, not grown
    assert queue.get_nowait().payload == big  # latest-wins: newest survives
    assert latch.consume() is True


# -- Adapter wiring: bounded queue on every client ----------------------------


def test_adapter_wires_bounded_transport_queue(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def client_factory(**kwargs: object) -> object:
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(aiomqtt, "Client", client_factory)

    adapter = AioMqttAdapter(_settings())

    assert captured["max_queued_incoming_messages"] == mqttio._TRANSPORT_QUEUE_MAXSIZE
    queue_type = captured["queue_type"]
    assert isinstance(queue_type, type)
    assert issubclass(queue_type, asyncio.Queue)

    # The queue aiomqtt would build shares THIS adapter's latch, so an upstream
    # overflow surfaces through the adapter's accessor.
    queue = cast(mqttio._BoundedTransportQueue, queue_type(maxsize=1))
    queue.put_nowait(_msg(_MUTE, b"true"))
    with pytest.raises(asyncio.QueueFull):
        queue.put_nowait(_msg(_MUTE, b"true"))
    assert adapter.consume_transport_overload() is True
    assert adapter.consume_transport_overload() is False


# -- End-to-end stress: blocked bus command + saturated lane + flood ----------


class _DrainIterator:
    """Async iterator draining a queue exactly like aiomqtt's MessagesIterator."""

    def __init__(self, queue: mqttio._BoundedTransportQueue) -> None:
        self._queue = queue

    def __aiter__(self) -> AsyncIterator[aiomqtt.Message]:
        return self

    async def __anext__(self) -> aiomqtt.Message:
        return await self._queue.get()


class _QueueDrainingClient:
    def __init__(self, queue: mqttio._BoundedTransportQueue) -> None:
        self.messages = _DrainIterator(queue)


async def test_blocked_bus_command_bounds_transport_backlog_end_to_end(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Issue repro shape: block the handler, saturate a lossless lane, inject
    sustained traffic through the real put_nowait/reader/dispatcher path, and
    verify bounded queued work/memory plus observable (not silent) overload."""
    caplog.set_level(logging.ERROR, logger=mqttio.__name__)
    adapter = AioMqttAdapter(_settings())
    latch = adapter._transport_overload
    # The bounded queue aiomqtt WOULD build, bound to the adapter's real latch.
    queue = _queue(latch, maxsize=mqttio._TRANSPORT_QUEUE_MAXSIZE)
    adapter._client = _QueueDrainingClient(queue)  # type: ignore[assignment]

    handler_entered = asyncio.Event()
    release = asyncio.Event()
    handled: list[str] = []

    async def stalled_bus_command(topic: str, payload: str) -> None:
        del payload
        handler_entered.set()
        await release.wait()  # models a wedged bus write
        handled.append(topic)

    adapter.on_command(stalled_bus_command)
    reader = asyncio.create_task(adapter._read_loop())
    try:
        # One command starts the single lane worker, which wedges in the handler.
        queue.put_nowait(_msg(_MUTE, b"true"))
        await asyncio.wait_for(handler_entered.wait(), timeout=1)

        overflowed = 0
        for index in range(10_000):
            try:
                queue.put_nowait(_msg(_MUTE, b"true"))
            except asyncio.QueueFull:
                overflowed += 1
            if index % 64 == 0:
                # Let the wedged reader drain what it can into the full lane.
                await asyncio.sleep(0)

        # Bounded queued work and memory despite sustained traffic.
        assert queue.qsize() <= mqttio._TRANSPORT_QUEUE_MAXSIZE
        assert queue.queued_bytes <= mqttio._TRANSPORT_QUEUE_MAX_BYTES
        # Overload is observable (a fail the runner can act on), not silent.
        assert overflowed > 0
        assert adapter.consume_transport_overload() is True
        assert any("transport backlog exceeded" in record.message for record in caplog.records)
        # The modeled bus write is genuinely wedged — no command silently ran.
        assert handled == []
    finally:
        release.set()
        reader.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await reader
