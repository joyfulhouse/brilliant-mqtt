from __future__ import annotations

import json

import pytest

from brilliant_mqtt import mqttio
from tests.fakes import _msg

TOPIC = "brilliant/test/light/set"


@pytest.mark.parametrize("layer", ["lane", "transport"])
@pytest.mark.parametrize(
    ("before", "after", "expected"),
    [
        (
            {"state": "ON", "brightness": 40},
            {"brightness": 120},
            {"state": "ON", "brightness": 120},
        ),
        ({"brightness": 40}, {"state": "ON"}, {"brightness": 40, "state": "ON"}),
        ({"state": "ON"}, {"brightness": 0}, {"state": "ON", "brightness": 0}),
    ],
)
async def test_partial_fields_survive_replacement(
    layer: str, before: dict[str, object], after: dict[str, object], expected: dict[str, object]
) -> None:
    values = await queued(layer, [json.dumps(before), json.dumps(after)])
    assert [json.loads(value) for value in values] == [expected]


async def queued(layer: str, payloads: list[str]) -> list[str]:
    if layer == "lane":
        lane = mqttio._LaneQueue(8)
        for payload in payloads:
            await lane.put(mqttio._InboundMessage(TOPIC, payload, False, (), ()), latest_wins=True)
        result = []
        while lane._pending:
            result.append((await lane.get()).payload)
            lane.task_done()
        await lane.join()
        return result
    transport = mqttio._BoundedTransportQueue(
        8, max_bytes=4096, overload=mqttio._TransportOverloadLatch()
    )
    for payload in payloads:
        transport.put_nowait(_msg(TOPIC, payload.encode()))
    assert transport.queued_bytes == sum(mqttio._message_bytes(item) for item in transport._queue)
    result = []
    while not transport.empty():
        result.append(transport.get_nowait().payload.decode())
        transport.task_done()
    assert transport.queued_bytes == 0
    await transport.join()
    return result


@pytest.mark.parametrize("layer", ["lane", "transport"])
@pytest.mark.parametrize(
    "payloads",
    [
        ['{"state":"OFF"}', '{"brightness":120}'],
        ['{"brightness":40}', '{"state":"OFF"}'],
        ['{"state":"ON","brightness":40}', '{"brightness":false}', '{"brightness":120}'],
        ['{"state":"ON"}', "not-json", '{"brightness":120}'],
        ['{"brightness":40}', '{"state":"TOGGLE"}', '{"brightness":120}'],
    ],
)
async def test_unsafe_partial_sequences_preserve_fifo(layer: str, payloads: list[str]) -> None:
    assert await queued(layer, payloads) == payloads


@pytest.mark.parametrize("layer", ["lane", "transport"])
async def test_retained_boundary_is_not_coalesced(layer: str) -> None:
    before, after = '{"state":"ON"}', '{"brightness":120}'
    if layer == "lane":
        lane = mqttio._LaneQueue(8)
        await lane.put(mqttio._InboundMessage(TOPIC, before, True, (), ()), latest_wins=True)
        await lane.put(mqttio._InboundMessage(TOPIC, after, False, (), ()), latest_wins=True)
        assert [(item.payload, item.retained) for item in lane._pending] == [
            (before, True),
            (after, False),
        ]
    else:
        transport = mqttio._BoundedTransportQueue(
            8, max_bytes=4096, overload=mqttio._TransportOverloadLatch()
        )
        first = _msg(TOPIC, before.encode())
        first.retain = True
        transport.put_nowait(first)
        transport.put_nowait(_msg(TOPIC, after.encode()))
        assert transport.qsize() == 2
        assert transport.get_nowait().retain is True
        assert transport.get_nowait().retain is False


async def test_callback_boundary_is_not_coalesced() -> None:
    async def callback(topic: str, payload: str) -> None:
        pass

    lane = mqttio._LaneQueue(8)
    before = mqttio._InboundMessage(TOPIC, '{"state":"ON"}', False, (callback,), ())
    after = mqttio._InboundMessage(TOPIC, '{"brightness":120}', False, (), ())
    await lane.put(before, latest_wins=True)
    await lane.put(after, latest_wins=True)
    assert list(lane._pending) == [before, after]


def test_merged_payload_must_fit_single_message_budget() -> None:
    import asyncio

    before = _msg(TOPIC, b'{"state":"ON"}')
    after = _msg(TOPIC, b'{"brightness":120}')
    budget = mqttio._message_bytes(after)
    latch = mqttio._TransportOverloadLatch()
    transport = mqttio._BoundedTransportQueue(8, max_bytes=budget, overload=latch)
    transport.put_nowait(before)
    with pytest.raises(asyncio.QueueFull):
        transport.put_nowait(after)
    assert transport.qsize() == 1
    assert transport.queued_bytes == mqttio._message_bytes(before)
    assert transport.get_nowait().payload == before.payload
    assert latch.consume()


@pytest.mark.parametrize("layer", ["lane", "transport"])
async def test_nested_invalid_json_stays_fifo(layer: str) -> None:
    payloads = ['{"state":"ON"}', "[" * 1100 + "0" + "]" * 1100]
    assert await queued(layer, payloads) == payloads


def test_transport_does_not_reencode_invalid_utf8() -> None:
    transport = mqttio._BoundedTransportQueue(
        8, max_bytes=4096, overload=mqttio._TransportOverloadLatch()
    )
    payloads = [b'{"state":"ON"}', '{"brightness":120}'.encode("utf-16")]
    for payload in payloads:
        transport.put_nowait(_msg(TOPIC, payload))
    assert [transport.get_nowait().payload for _ in payloads] == payloads


@pytest.mark.parametrize("layer", ["lane", "transport"])
@pytest.mark.parametrize("barrier", ["invalid-aux", "invalid-primary"])
async def test_invalid_command_is_a_barrier_across_its_lane(layer: str, barrier: str) -> None:
    aux = TOPIC.removesuffix("set") + "set_screen_brightness"
    messages = [(aux, "4")]
    messages.append(
        (aux, "not-number") if barrier == "invalid-aux" else (TOPIC, '{"state":"TOGGLE"}')
    )
    messages.append((aux, "8"))
    if layer == "lane":
        lane = mqttio._LaneQueue(8)
        for topic, payload in messages:
            await lane.put(mqttio._InboundMessage(topic, payload, False, (), ()), latest_wins=True)
        assert [(item.topic, item.payload) for item in lane._pending] == messages
    else:
        transport = mqttio._BoundedTransportQueue(
            8, max_bytes=4096, overload=mqttio._TransportOverloadLatch()
        )
        for topic, payload in messages:
            transport.put_nowait(_msg(topic, payload.encode()))
        assert [(str(item.topic), item.payload.decode()) for item in transport._queue] == messages
