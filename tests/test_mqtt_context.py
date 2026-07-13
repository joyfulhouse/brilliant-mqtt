"""Context-aware MQTT receive fan-out tests."""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, cast

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

    await adapter._read_loop()

    assert reached == [False]
