"""Context-aware MQTT receive fan-out tests."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, cast

import pytest

from brilliant_mqtt import mqttio
from brilliant_mqtt.mqttio import AioMqttAdapter


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

    await adapter._read_loop()

    assert reached == [False]


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
