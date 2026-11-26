"""Owner identity survives every production command queue boundary."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import cast

import aiomqtt
import pytest

from brilliant_mqtt import mqttio
from brilliant_mqtt.write_admission import command_admission
from tests.test_mqttio import _settings

TOPIC = "brilliant/mesh/load/set"


def message(payload: str) -> aiomqtt.Message:
    return aiomqtt.Message(TOPIC, payload.encode(), 0, False, 1, None)


async def test_transport_stamps_before_coalescing_and_retains_exact_owner() -> None:
    current: object | None = object()
    first = current
    queue = mqttio._BoundedTransportQueue(
        8,
        max_bytes=4096,
        overload=mqttio._TransportOverloadLatch(),
        owner_for_topic=lambda topic: current,
    )
    queue.put_nowait(message('{"state":"ON"}'))
    queue.put_nowait(message('{"brightness":120}'))
    assert queue.qsize() == 1
    current = object()
    queue.put_nowait(message('{"brightness":130}'))
    assert queue.qsize() == 2
    older, newer = await queue.get(), await queue.get()
    assert getattr(older, "mesh_owner", None) is first
    assert getattr(newer, "mesh_owner", None) is current
    assert mqttio._decode_payload(older.payload) == '{"state":"ON","brightness":120}'


async def test_lane_different_owners_cannot_coalesce_or_fold() -> None:
    first, second = object(), object()
    entered, release = asyncio.Event(), asyncio.Event()
    seen: list[object | None] = []
    folds: list[str] = []

    def fold(payload: str) -> bool:
        folds.append(payload)
        return True

    async def handle(message: mqttio._InboundMessage) -> None:
        admission = command_admission.get()
        assert admission is not None
        seen.append(admission.mesh_owner)
        admission.try_supersede = fold
        entered.set()
        await release.wait()

    dispatcher = mqttio._TopicDispatcher(handle)
    original = mqttio._InboundMessage(TOPIC, '{"state":"ON"}', False, (), (), first)
    try:
        await dispatcher.dispatch(original, latest_wins=True)
        await entered.wait()
        replacement = replace(original, payload='{"brightness":120}', mesh_owner=second)
        await dispatcher.dispatch(replacement, latest_wins=True)
        assert folds == []
        release.set()
        await dispatcher._queues[mqttio._command_lane_key(TOPIC)].join()
        assert seen == [first, second]
    finally:
        release.set()
        await dispatcher.shutdown()


async def test_queue_constructed_before_resolver_registration_keeps_live_closure_and_unowned_stamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queues: list[asyncio.Queue[aiomqtt.Message]] = []

    def client(**kwargs: object) -> object:
        queue_type = cast(type[asyncio.Queue[aiomqtt.Message]], kwargs["queue_type"])
        queues.append(queue_type(8))
        return object()

    monkeypatch.setattr(aiomqtt, "Client", client)
    adapter = mqttio.AioMqttAdapter(_settings(tls_enabled=False))
    queue = queues[0]
    unowned = message('{"state":"ON"}')
    queue.put_nowait(unowned)
    owner = object()
    adapter.set_mesh_owner_resolver(lambda topic: owner)
    queue.put_nowait(message('{"state":"ON"}'))
    queue.put_nowait(message('{"brightness":120}'))
    assert queue.qsize() == 2
    older, newer = queue.get_nowait(), queue.get_nowait()
    assert getattr(older, "mesh_owner", None) is None
    assert getattr(newer, "mesh_owner", None) is owner
    assert older is unowned  # Unrelated/unowned messages need no wrapper allocation.
    adapter.set_mesh_owner_resolver(lambda topic: None)
    queue.put_nowait(newer)
    assert getattr(queue.get_nowait(), "mesh_owner", None) is None
