"""Context-aware MQTT receive fan-out tests."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, cast

import pytest

from brilliant_mqtt import mqttio
from brilliant_mqtt.bridge import Bridge
from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.config import Settings
from brilliant_mqtt.model import BrilliantDevice, DeviceKind, Variable
from brilliant_mqtt.mqttio import AioMqttAdapter
from tests.fakes import FakeBus, FakeClock, FakeMqtt, FakeSleeper


@dataclass
class _Message:
    topic: str
    payload: bytes
    retain: bool


class _Messages:
    def __init__(self, messages: list[_Message]) -> None:
        self._messages = messages

    async def __aiter__(self) -> AsyncIterator[_Message]:
        for message in self._messages:
            yield message


class _Client:
    def __init__(self, messages: list[_Message]) -> None:
        self.messages = _Messages(messages)


class _PublishClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, bool, int]] = []
        self.completed = False

    async def publish(self, topic: str, *, payload: str, retain: bool, qos: int) -> None:
        await asyncio.sleep(0)
        self.calls.append((topic, payload, retain, qos))
        self.completed = True


class _StreamingMessages:
    def __init__(self, message: _Message) -> None:
        self._message = message
        self._sent = False

    def __aiter__(self) -> _StreamingMessages:
        return self

    async def __anext__(self) -> _Message:
        if not self._sent:
            self._sent = True
            return self._message
        await asyncio.Future()
        raise StopAsyncIteration


class _LifecycleClient:
    def __init__(self, message: _Message) -> None:
        self.messages = _StreamingMessages(message)
        self.exit_calls = 0

    async def __aenter__(self) -> _LifecycleClient:
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        del exc_type, exc, tb
        self.exit_calls += 1


class _BatchStreamingMessages:
    def __init__(self, messages: list[_Message]) -> None:
        self._messages = messages
        self.yielded = 0
        self.all_yielded = asyncio.Event()

    def __aiter__(self) -> _BatchStreamingMessages:
        return self

    async def __anext__(self) -> _Message:
        if self.yielded < len(self._messages):
            message = self._messages[self.yielded]
            self.yielded += 1
            if self.yielded == len(self._messages):
                self.all_yielded.set()
            return message
        await asyncio.Future()
        raise StopAsyncIteration


class _BatchLifecycleClient:
    def __init__(self, messages: list[_Message]) -> None:
        self.messages = _BatchStreamingMessages(messages)
        self.exit_calls = 0

    async def __aenter__(self) -> _BatchLifecycleClient:
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        del exc_type, exc, tb
        self.exit_calls += 1


@pytest.mark.parametrize(
    ("topic", "expected"),
    [
        ("brilliant/office/light-1/set", True),
        ("brilliant/office/hardware/set_screen_brightness", True),
        ("brilliant/office/hardware/set_screen_on", False),
        ("brilliant/office/ui/set_request_identify", False),
        ("brilliant/ha-control/v1/set", False),
        ("brilliant/ha-control/v1/scene/command/office", False),
        ("brilliant/mesh/leader", False),
    ],
)
def test_latest_wins_is_limited_to_safe_command_topics(topic: str, expected: bool) -> None:
    assert mqttio._is_latest_wins_topic(topic) is expected


async def test_read_loop_fans_out_retained_context_and_preserves_two_arg_callbacks() -> None:
    adapter = object.__new__(AioMqttAdapter)
    adapter._client = cast(Any, _Client([_Message("topic", b"payload", True)]))
    commands: list[tuple[str, str]] = []
    messages: list[tuple[str, str, bool]] = []

    async def command_cb(topic: str, payload: str) -> None:
        commands.append((topic, payload))

    async def message_cb(topic: str, payload: str, retained: bool) -> None:
        messages.append((topic, payload, retained))

    adapter._command_cbs = [command_cb]
    adapter._message_cbs = [message_cb]
    adapter._payload_decode_error_cbs = []
    adapter._redacted_logging = False

    await adapter._read_loop()

    assert commands == [("topic", "payload")]
    assert messages == [("topic", "payload", True)]


async def test_failing_context_callback_does_not_starve_other_callbacks() -> None:
    adapter = object.__new__(AioMqttAdapter)
    adapter._client = cast(Any, _Client([_Message("topic", b"payload", False)]))
    reached: list[bool] = []

    async def broken(_topic: str, _payload: str, _retained: bool) -> None:
        raise RuntimeError("broken")

    async def healthy(_topic: str, _payload: str, retained: bool) -> None:
        reached.append(retained)

    adapter._command_cbs = []
    adapter._message_cbs = [broken, healthy]
    adapter._payload_decode_error_cbs = []
    adapter._redacted_logging = False

    await adapter._read_loop()

    assert reached == [False]


async def test_slow_callback_on_one_topic_does_not_delay_another_topic() -> None:
    adapter = object.__new__(AioMqttAdapter)
    adapter._client = cast(
        Any,
        _Client(
            [
                _Message("brilliant/office/light-1/set", b"slow", False),
                _Message("brilliant/office/light-2/set", b"fast", False),
            ]
        ),
    )
    slow_started = asyncio.Event()
    release_slow = asyncio.Event()
    fast_finished = asyncio.Event()

    async def command_cb(topic: str, payload: str) -> None:
        del payload
        if topic.endswith("light-1/set"):
            slow_started.set()
            await release_slow.wait()
        else:
            fast_finished.set()

    adapter._command_cbs = [command_cb]
    adapter._message_cbs = []
    adapter._payload_decode_error_cbs = []
    adapter._redacted_logging = False
    reader = asyncio.create_task(adapter._read_loop())

    try:
        await asyncio.wait_for(slow_started.wait(), timeout=0.1)
        await asyncio.wait_for(fast_finished.wait(), timeout=0.1)
    finally:
        release_slow.set()
        await reader


async def test_primary_and_aux_commands_for_same_peripheral_never_overlap() -> None:
    primary = "brilliant/office/hardware/set"
    auxiliary = "brilliant/office/hardware/set_screen_brightness"
    adapter = object.__new__(AioMqttAdapter)
    adapter._client = cast(
        Any,
        _Client(
            [
                _Message(primary, b"primary", False),
                _Message(auxiliary, b"auxiliary", False),
            ]
        ),
    )
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    calls: list[str] = []

    async def command_cb(topic: str, payload: str) -> None:
        calls.append(f"start:{topic}:{payload}")
        if topic == primary:
            first_started.set()
            await release_first.wait()
        calls.append(f"finish:{topic}:{payload}")

    adapter._command_cbs = [command_cb]
    adapter._message_cbs = []
    adapter._payload_decode_error_cbs = []
    adapter._redacted_logging = False
    reader = asyncio.create_task(adapter._read_loop())

    await asyncio.wait_for(first_started.wait(), timeout=0.1)
    await asyncio.sleep(0)
    assert calls == [f"start:{primary}:primary"]
    release_first.set()
    await reader

    assert calls == [
        f"start:{primary}:primary",
        f"finish:{primary}:primary",
        f"start:{auxiliary}:auxiliary",
        f"finish:{auxiliary}:auxiliary",
    ]


async def test_two_commands_on_same_topic_run_strictly_in_order() -> None:
    topic = "brilliant/office/hardware/set_screen_on"
    adapter = object.__new__(AioMqttAdapter)
    adapter._client = cast(
        Any,
        _Client(
            [
                _Message(topic, b"first", False),
                _Message(topic, b"second", False),
            ]
        ),
    )
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    calls: list[str] = []

    async def command_cb(_topic: str, payload: str) -> None:
        calls.append(f"start:{payload}")
        if payload == "first":
            first_started.set()
            await release_first.wait()
        calls.append(f"finish:{payload}")

    adapter._command_cbs = [command_cb]
    adapter._message_cbs = []
    adapter._payload_decode_error_cbs = []
    adapter._redacted_logging = False
    reader = asyncio.create_task(adapter._read_loop())

    await asyncio.wait_for(first_started.wait(), timeout=0.1)
    await asyncio.sleep(0)
    assert calls == ["start:first"]
    release_first.set()
    await reader

    assert calls == ["start:first", "finish:first", "start:second", "finish:second"]


async def test_brightness_burst_always_executes_latest_payload() -> None:
    topic = "brilliant/office/light-1/set"
    adapter = object.__new__(AioMqttAdapter)
    adapter._client = cast(
        Any,
        _Client(
            [_Message(topic, f'{{"brightness": {value}}}'.encode(), False) for value in range(1, 6)]
        ),
    )
    payloads: list[str] = []

    async def command_cb(_topic: str, payload: str) -> None:
        payloads.append(payload)

    adapter._command_cbs = [command_cb]
    adapter._message_cbs = []
    adapter._payload_decode_error_cbs = []
    adapter._redacted_logging = False

    await adapter._read_loop()

    assert 1 <= len(payloads) <= 5
    assert payloads[-1] == '{"brightness": 5}'


async def test_latest_wins_replaces_only_its_topic_within_peripheral_lane() -> None:
    blocker = "brilliant/office/hardware/set_screen_on"
    primary = "brilliant/office/hardware/set"
    auxiliary = "brilliant/office/hardware/set_screen_brightness"
    adapter = object.__new__(AioMqttAdapter)
    adapter._client = cast(
        Any,
        _Client(
            [
                _Message(blocker, b"block", False),
                _Message(primary, b"primary-1", False),
                _Message(auxiliary, b"aux-1", False),
                _Message(primary, b"primary-2", False),
                _Message(auxiliary, b"aux-2", False),
            ]
        ),
    )
    blocker_started = asyncio.Event()
    release_blocker = asyncio.Event()
    calls: list[tuple[str, str]] = []

    async def command_cb(topic: str, payload: str) -> None:
        calls.append((topic, payload))
        if topic == blocker:
            blocker_started.set()
            await release_blocker.wait()

    adapter._command_cbs = [command_cb]
    adapter._message_cbs = []
    adapter._payload_decode_error_cbs = []
    adapter._redacted_logging = False
    reader = asyncio.create_task(adapter._read_loop())

    await asyncio.wait_for(blocker_started.wait(), timeout=0.1)
    await asyncio.sleep(0)
    release_blocker.set()
    await reader

    assert calls == [
        (blocker, "block"),
        (primary, "primary-2"),
        (auxiliary, "aux-2"),
    ]


async def test_scene_topic_burst_is_never_dropped() -> None:
    topic = "brilliant/ha-control/v1/scene/command/office"
    adapter = object.__new__(AioMqttAdapter)
    adapter._client = cast(
        Any,
        _Client([_Message(topic, str(value).encode(), False) for value in range(12)]),
    )
    payloads: list[str] = []

    async def message_cb(_topic: str, payload: str, retained: bool) -> None:
        assert retained is False
        payloads.append(payload)

    adapter._command_cbs = []
    adapter._message_cbs = [message_cb]
    adapter._payload_decode_error_cbs = []
    adapter._redacted_logging = False

    await adapter._read_loop()

    assert payloads == [str(value) for value in range(12)]


async def test_clean_shutdown_cancels_workers_without_losing_in_flight_command() -> None:
    topic = "brilliant/office/light-1/set"
    client = _LifecycleClient(_Message(topic, b'{"brightness": 42}', False))
    adapter = AioMqttAdapter(
        Settings(
            panel="office",
            mqtt_host="broker.invalid",
            mqtt_username="u",
            mqtt_password="p",
        ),
        publish_availability=False,
    )
    adapter._client = cast(Any, client)
    started = asyncio.Event()
    release = asyncio.Event()
    finished = asyncio.Event()

    async def command_cb(_topic: str, _payload: str) -> None:
        started.set()
        await release.wait()
        finished.set()

    adapter.on_command(command_cb)
    await adapter.connect()
    await asyncio.wait_for(started.wait(), timeout=0.1)
    disconnect = asyncio.create_task(adapter.disconnect())

    done_before_release, _ = await asyncio.wait({disconnect}, timeout=0.02)
    release.set()
    await asyncio.wait_for(disconnect, timeout=0.1)

    assert done_before_release == set()
    assert finished.is_set()
    assert adapter._topic_dispatcher._workers == {}
    assert client.exit_calls == 1


async def test_hung_callback_is_abandoned_after_bounded_shutdown(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(mqttio, "_SHUTDOWN_DRAIN_DEADLINE_S", 0.01)
    topic = "brilliant/office/light-1/set"
    client = _LifecycleClient(_Message(topic, b"hung", False))
    adapter = AioMqttAdapter(
        Settings(
            panel="office",
            mqtt_host="broker.invalid",
            mqtt_username="u",
            mqtt_password="p",
        ),
        publish_availability=False,
    )
    adapter._client = cast(Any, client)
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def command_cb(_topic: str, _payload: str) -> None:
        started.set()
        try:
            await asyncio.Future()
        finally:
            cancelled.set()

    adapter.on_command(command_cb)
    await adapter.connect()
    await asyncio.wait_for(started.wait(), timeout=0.1)

    with caplog.at_level(logging.WARNING, logger="brilliant_mqtt.mqttio"):
        await asyncio.wait_for(adapter.disconnect(), timeout=0.1)

    assert cancelled.is_set()
    assert "undrained commands were abandoned" in caplog.text
    assert client.exit_calls == 1


async def test_cancellation_swallowing_callback_cannot_wedge_shutdown(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Worker settlement is bounded: swallowing CancelledError must not block
    disconnect (cancellation is cooperative — review finding 4)."""
    monkeypatch.setattr(mqttio, "_SHUTDOWN_DRAIN_DEADLINE_S", 0.01)
    monkeypatch.setattr(mqttio, "_SHUTDOWN_WORKER_SETTLE_S", 0.01)
    topic = "brilliant/office/light-1/set"
    client = _LifecycleClient(_Message(topic, b"stubborn", False))
    adapter = AioMqttAdapter(
        Settings(
            panel="office",
            mqtt_host="broker.invalid",
            mqtt_username="u",
            mqtt_password="p",
        ),
        publish_availability=False,
    )
    adapter._client = cast(Any, client)
    started = asyncio.Event()
    release = asyncio.Event()

    async def command_cb(_topic: str, _payload: str) -> None:
        started.set()
        while True:
            try:
                await release.wait()
                return
            except asyncio.CancelledError:
                # Deliberately swallow the cancel — the worker never settles.
                continue

    adapter.on_command(command_cb)
    await adapter.connect()
    await asyncio.wait_for(started.wait(), timeout=0.1)

    with caplog.at_level(logging.WARNING, logger="brilliant_mqtt.mqttio"):
        await asyncio.wait_for(adapter.disconnect(), timeout=1.0)

    assert "workers did not settle" in caplog.text
    assert client.exit_calls == 1
    # Unblock the stubborn callback so the abandoned task can finish.
    release.set()


async def test_disconnect_cancels_reader_blocked_on_saturated_lane(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(mqttio, "_SHUTDOWN_DRAIN_DEADLINE_S", 0.01)
    topic = "brilliant/office/hardware/set_screen_on"
    messages = [
        _Message(topic, str(index).encode(), False)
        for index in range(mqttio._TOPIC_QUEUE_MAXSIZE + 2)
    ]
    client = _BatchLifecycleClient(messages)
    adapter = AioMqttAdapter(
        Settings(
            panel="office",
            mqtt_host="broker.invalid",
            mqtt_username="u",
            mqtt_password="p",
        ),
        publish_availability=False,
    )
    adapter._client = cast(Any, client)
    started = asyncio.Event()

    async def command_cb(_topic: str, _payload: str) -> None:
        started.set()
        await asyncio.Future()

    adapter.on_command(command_cb)
    await adapter.connect()
    await asyncio.wait_for(started.wait(), timeout=0.1)
    await asyncio.wait_for(client.messages.all_yielded.wait(), timeout=0.1)
    await asyncio.sleep(0)
    assert adapter._reader_task is not None
    assert adapter._reader_task.done() is False

    await asyncio.wait_for(adapter.disconnect(), timeout=0.1)

    assert client.exit_calls == 1


async def test_saturated_fifo_lane_backpressures_reader_without_losing_messages() -> None:
    saturated_topic = "brilliant/office/hardware/set_screen_on"
    other_lane_topic = "brilliant/office/other/set_screen_on"
    saturated_payloads = [str(index) for index in range(mqttio._TOPIC_QUEUE_MAXSIZE + 2)]
    adapter = object.__new__(AioMqttAdapter)
    adapter._client = cast(
        Any,
        _Client(
            [
                *[
                    _Message(saturated_topic, payload.encode(), False)
                    for payload in saturated_payloads
                ],
                _Message(other_lane_topic, b"other", False),
            ]
        ),
    )
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    seen: list[tuple[str, str]] = []

    async def command_cb(topic: str, payload: str) -> None:
        seen.append((topic, payload))
        if topic == saturated_topic and payload == "0":
            first_started.set()
            await release_first.wait()

    adapter._command_cbs = [command_cb]
    adapter._message_cbs = []
    adapter._payload_decode_error_cbs = []
    adapter._redacted_logging = False
    reader = asyncio.create_task(adapter._read_loop())

    await asyncio.wait_for(first_started.wait(), timeout=0.1)
    await asyncio.sleep(0)
    assert (other_lane_topic, "other") not in seen
    release_first.set()
    await reader

    assert [payload for topic, payload in seen if topic == saturated_topic] == (saturated_payloads)
    assert seen.count((other_lane_topic, "other")) == 1


async def test_invalid_utf8_uses_typed_error_signal_without_text_delivery_and_reader_continues(
    caplog: pytest.LogCaptureFixture,
) -> None:
    adapter = object.__new__(AioMqttAdapter)
    adapter._client = cast(
        Any,
        _Client(
            [
                _Message("unrelated/topic", b"\xffraw-payload-secret", False),
                _Message("valid/topic", b"valid-payload", True),
            ]
        ),
    )
    adapter._redacted_logging = True
    commands: list[tuple[str, str]] = []
    decode_errors: list[object] = []

    async def command_cb(topic: str, payload: str) -> None:
        commands.append((topic, payload))

    async def broken_decode_error_cb(error: object) -> None:
        raise RuntimeError("decode-callback-secret") from None

    async def healthy_decode_error_cb(error: object) -> None:
        decode_errors.append(error)

    adapter._command_cbs = [command_cb]
    adapter._message_cbs = []
    adapter._payload_decode_error_cbs = []
    adapter.on_payload_decode_error(broken_decode_error_cb)
    adapter.on_payload_decode_error(healthy_decode_error_cb)

    await adapter._read_loop()

    assert commands == [("valid/topic", "valid-payload")]
    assert decode_errors == [mqttio.MqttPayloadDecodeError(topic="unrelated/topic", retained=False)]
    assert not hasattr(decode_errors[0], "payload")
    assert "raw-payload-secret" not in caplog.text
    assert "decode-callback-secret" not in caplog.text


async def test_publish_forwards_qos_and_awaits_broker_acknowledgement() -> None:
    adapter = object.__new__(AioMqttAdapter)
    client = _PublishClient()
    adapter._client = cast(Any, client)

    await adapter.publish("probe/topic", "nonce", qos=1)

    assert client.calls == [("probe/topic", "nonce", False, 1)]
    assert client.completed is True


@pytest.mark.parametrize("qos", [-1, 3])
async def test_publish_rejects_invalid_qos_before_calling_client(qos: int) -> None:
    adapter = object.__new__(AioMqttAdapter)
    client = _PublishClient()
    adapter._client = cast(Any, client)

    with pytest.raises(ValueError, match="qos"):
        await adapter.publish("probe/topic", "nonce", qos=qos)

    assert client.calls == []


async def test_mesh_confirmation_never_blocks_the_command_lane() -> None:
    """Issues #46/#47: mesh confirmation is background bridge state — the
    per-peripheral command lane must stay free to run the newest queued
    command while the previous write is still unconfirmed, and no optimistic
    primary state may be fabricated meanwhile."""
    device = BrilliantDevice(
        device_id="ble_mesh",
        peripheral_id="mesh_light_1",
        name="Dining",
        kind=DeviceKind.LIGHT,
        peripheral_type=27,
        variables={"on": Variable("on", "1")},
    )
    bus = FakeBus([device])
    mqtt = FakeMqtt()
    sleeper = FakeSleeper()
    bridge = Bridge(bus, mqtt, "mesh", clock=FakeClock(), sleep=sleeper)
    await bridge.reconcile()
    mqtt.published.clear()

    topic = "brilliant/mesh/mesh_light_1/set"
    adapter = object.__new__(AioMqttAdapter)
    adapter._client = cast(
        Any,
        _Client(
            [
                _Message(topic, json.dumps({"state": "OFF"}).encode(), False),
                _Message(topic, json.dumps({"state": "ON"}).encode(), False),
            ]
        ),
    )
    # Wire the SAME callback the bridge registered on its MQTT client.
    adapter._command_cbs = list(mqtt._command_cbs)
    adapter._message_cbs = []
    adapter._payload_decode_error_cbs = []
    adapter._redacted_logging = False

    # Bounded: if confirmation ran inside the callback, the lane would sit on
    # the 80s window and this would time out.
    await asyncio.wait_for(adapter._read_loop(), timeout=1.0)

    # The newest command reached the bus through the lane...
    assert bus.commands[-1] == ("ble_mesh", "mesh_light_1", [VarSet("on", "1")])
    # ...while confirmation stays pending in the background: every state
    # publish so far withholds the primary (no optimistic ON/OFF escaped).
    states = [p for p in mqtt.published if p[0] == "brilliant/mesh/mesh_light_1/state"]
    assert states
    assert all(json.loads(p[1])["state"] is None for p in states)
    await bridge.withdraw()
