"""Unit tests for AioMqttAdapter's connect/disconnect lifecycle and TLS wiring.

A fake stands in for ``aiomqtt.Client`` (swapped onto a REAL ``AioMqttAdapter``
instance's ``_client`` after real, no-I/O construction) so these tests never
touch a network — matching the pattern in test_mqtt_context.py, extended to
drive the one-shot connect()/disconnect() state machine and its
cancellation-shield semantics.
"""

from __future__ import annotations

import asyncio
import ssl
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

import aiomqtt
import pytest
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.reasoncodes import ReasonCode

from brilliant_mqtt.config import Settings
from brilliant_mqtt.mqttio import AioMqttAdapter, build_tls_context
from brilliant_mqtt.protocols import CommandSubscribeError


async def _empty_messages() -> AsyncIterator[object]:
    return
    yield  # pragma: no cover - makes this an async generator, never reached


@dataclass
class _FakeAiomqttClient:
    """Stand-in for aiomqtt.Client's async-context-manager + publish surface."""

    enter_error: BaseException | None = None
    exit_error: BaseException | None = None
    publish_error: BaseException | None = None
    subscribe_error: BaseException | None = None
    subscribe_result: tuple[int, ...] | list[ReasonCode] = (0,)
    enter_gate: asyncio.Event | None = None
    exit_gate: asyncio.Event | None = None
    enter_calls: int = 0
    exit_calls: int = 0
    published: list[tuple[str, object, bool, int]] = field(default_factory=list)
    messages: AsyncIterator[object] = field(default_factory=_empty_messages)

    async def __aenter__(self) -> _FakeAiomqttClient:
        self.enter_calls += 1
        if self.enter_gate is not None:
            await self.enter_gate.wait()
        if self.enter_error is not None:
            raise self.enter_error
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.exit_calls += 1
        if self.exit_gate is not None:
            await self.exit_gate.wait()
        if self.exit_error is not None:
            raise self.exit_error

    async def publish(
        self, topic: str, payload: object = None, retain: bool = False, qos: int = 0
    ) -> None:
        if self.publish_error is not None:
            raise self.publish_error
        self.published.append((topic, payload, retain, qos))

    async def subscribe(self, topic: str) -> tuple[int, ...] | list[ReasonCode]:
        if self.subscribe_error is not None:
            raise self.subscribe_error
        return self.subscribe_result

    async def unsubscribe(self, topic: str) -> None:
        pass


def _settings(**overrides: object) -> Settings:
    base: dict[str, object] = {
        "panel": "office",
        "mqtt_host": "broker.invalid",
        "mqtt_username": "u",
        "mqtt_password": "p",
    }
    base.update(overrides)
    return Settings(**base)  # type: ignore[arg-type]


def _adapter(fake_client: _FakeAiomqttClient, **kwargs: object) -> AioMqttAdapter:
    """A REAL AioMqttAdapter (real __init__, real Settings) with its aiomqtt
    client swapped for *fake_client* — no network I/O ever occurs."""
    adapter = AioMqttAdapter(_settings(), **kwargs)  # type: ignore[arg-type]
    adapter._client = fake_client  # type: ignore[assignment]
    return adapter


async def _wait_until(predicate: object, timeout: float = 2.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():  # type: ignore[operator]
        if loop.time() > deadline:
            raise AssertionError("condition was not met in time")
        await asyncio.sleep(0.005)


# -- One-shot lifecycle guards --------------------------------------------------


async def test_connect_twice_raises() -> None:
    adapter = _adapter(_FakeAiomqttClient())
    await adapter.connect()

    with pytest.raises(RuntimeError, match="cannot be reused"):
        await adapter.connect()


async def test_disconnect_before_connect_is_a_noop() -> None:
    fake = _FakeAiomqttClient()
    adapter = _adapter(fake)

    await adapter.disconnect()  # must not raise

    assert fake.exit_calls == 0


async def test_disconnect_after_failed_connect_is_a_noop() -> None:
    fake = _FakeAiomqttClient(enter_error=RuntimeError("connect refused"))
    adapter = _adapter(fake)

    with pytest.raises(RuntimeError, match="connect refused"):
        await adapter.connect()
    assert adapter._closed is True

    await adapter.disconnect()  # already terminal — must not raise or touch exit

    assert fake.exit_calls == 0


async def test_connect_after_close_raises() -> None:
    fake = _FakeAiomqttClient()
    adapter = _adapter(fake)
    await adapter.connect()
    await adapter.disconnect()

    with pytest.raises(RuntimeError, match="cannot be reused"):
        await adapter.connect()


async def test_disconnect_after_close_is_a_noop() -> None:
    fake = _FakeAiomqttClient()
    adapter = _adapter(fake)
    await adapter.connect()
    await adapter.disconnect()
    assert fake.exit_calls == 1

    await adapter.disconnect()  # already closed — must not re-run teardown

    assert fake.exit_calls == 1


# -- checked_disconnect (preflight) vs best-effort (resident) ------------------


async def test_checked_disconnect_aggregates_availability_publish_failure() -> None:
    fake = _FakeAiomqttClient(publish_error=aiomqtt.MqttError("link down"))
    adapter = _adapter(fake, checked_disconnect=True)
    await adapter.connect()

    with pytest.raises(RuntimeError, match="MQTT disconnect failed"):
        await adapter.disconnect()

    # Checked mode still finishes every close step despite the earlier failure.
    assert fake.exit_calls == 1
    assert adapter._closed is True


async def test_checked_disconnect_aggregates_exit_failure() -> None:
    fake = _FakeAiomqttClient(exit_error=RuntimeError("exit boom"))
    adapter = _adapter(fake, checked_disconnect=True)
    await adapter.connect()

    with pytest.raises(RuntimeError, match="MQTT disconnect failed") as excinfo:
        await adapter.disconnect()

    # The raw exception text is not leaked — checked mode raises one generic error.
    assert "exit boom" not in str(excinfo.value)


async def test_best_effort_disconnect_swallows_availability_publish_failure() -> None:
    fake = _FakeAiomqttClient(publish_error=aiomqtt.MqttError("link down"))
    adapter = _adapter(fake, checked_disconnect=False)
    await adapter.connect()

    await adapter.disconnect()  # best-effort — must not raise

    assert fake.exit_calls == 1
    assert adapter._closed is True


async def test_best_effort_disconnect_swallows_generic_exit_exception() -> None:
    fake = _FakeAiomqttClient(exit_error=RuntimeError("exit boom"))
    adapter = _adapter(fake, checked_disconnect=False)
    await adapter.connect()

    await adapter.disconnect()  # best-effort — must not raise

    assert adapter._closed is True


async def test_publish_availability_false_skips_offline_publish() -> None:
    fake = _FakeAiomqttClient()
    adapter = _adapter(fake, publish_availability=False)
    await adapter.connect()

    await adapter.disconnect()

    assert fake.published == []


async def test_publish_availability_true_publishes_clean_offline_on_disconnect() -> None:
    fake = _FakeAiomqttClient()
    adapter = _adapter(fake, publish_availability=True)
    await adapter.connect()

    await adapter.disconnect()

    assert fake.published == [("brilliant/office/availability", "offline", True, 0)]


async def test_rejected_suback_surfaces_project_error() -> None:
    adapter = _adapter(_FakeAiomqttClient(subscribe_result=(0x80,)))

    with pytest.raises(CommandSubscribeError, match="brilliant/office/light/set"):
        await adapter.subscribe("brilliant/office/light/set")


async def test_rejected_mqtt_v5_suback_surfaces_project_error() -> None:
    rejection = ReasonCode(PacketTypes.SUBACK, identifier=0x87)
    adapter = _adapter(_FakeAiomqttClient(subscribe_result=[rejection]))

    with pytest.raises(CommandSubscribeError, match="Not authorized"):
        await adapter.subscribe("brilliant/office/light/set")


async def test_subscribe_transport_error_is_translated_to_project_error() -> None:
    cause = aiomqtt.MqttError("Operation timed out")
    adapter = _adapter(_FakeAiomqttClient(subscribe_error=cause))

    with pytest.raises(CommandSubscribeError) as raised:
        await adapter.subscribe("brilliant/office/light/set")

    assert raised.value.__cause__ is cause


# -- Cancellation shields the executor/lifecycle-backed work --------------------


async def test_connect_cancelled_still_completes_entry_in_background() -> None:
    fake = _FakeAiomqttClient(enter_gate=asyncio.Event())
    adapter = _adapter(fake)

    task = asyncio.ensure_future(adapter.connect())
    await _wait_until(lambda: fake.enter_calls == 1)
    task.cancel()
    fake.enter_gate.set()  # type: ignore[union-attr]

    with pytest.raises(asyncio.CancelledError):
        await task

    # The shielded __aenter__() ran to completion despite the cancellation.
    assert adapter._entered is True
    assert adapter._closed is False
    # But connect() never reached the reader-task/logging tail past the raise.
    assert adapter._reader_task is None


async def test_connect_cancelled_when_entry_ultimately_fails_still_closes() -> None:
    fake = _FakeAiomqttClient(enter_gate=asyncio.Event(), enter_error=RuntimeError("boom"))
    adapter = _adapter(fake)

    task = asyncio.ensure_future(adapter.connect())
    await _wait_until(lambda: fake.enter_calls == 1)
    task.cancel()
    fake.enter_gate.set()  # type: ignore[union-attr]

    # Cancellation takes priority over the entry failure at the call boundary...
    with pytest.raises(asyncio.CancelledError):
        await task
    # ...but the adapter still correctly records itself as permanently closed.
    assert adapter._closed is True
    assert adapter._entered is False


async def test_disconnect_cancelled_still_settles_exit_before_reraising() -> None:
    fake = _FakeAiomqttClient(exit_gate=asyncio.Event())
    adapter = _adapter(fake)
    await adapter.connect()

    task = asyncio.ensure_future(adapter.disconnect())
    await _wait_until(lambda: fake.exit_calls == 1)
    task.cancel()
    fake.exit_gate.set()  # type: ignore[union-attr]

    with pytest.raises(asyncio.CancelledError):
        await task

    # The shielded __aexit__() ran to completion before the cancellation
    # propagated — the adapter reaches its terminal closed state, not a
    # half-torn-down one that would wedge a caller retrying disconnect().
    assert adapter._closed is True
    assert adapter._closing is False


# -- build_tls_context -----------------------------------------------------------


def test_build_tls_context_none_when_disabled() -> None:
    settings = _settings(mqtt_tls_enabled=False)

    assert build_tls_context(settings) is None


def test_build_tls_context_strict_when_enabled() -> None:
    settings = _settings(mqtt_tls_enabled=True, mqtt_tls_ca_file=None)

    context = build_tls_context(settings)

    assert context is not None
    assert isinstance(context, ssl.SSLContext)
    assert context.check_hostname is True
    assert context.verify_mode == ssl.CERT_REQUIRED


# -- publish() qos validation ----------------------------------------------------


async def test_publish_rejects_invalid_qos() -> None:
    fake = _FakeAiomqttClient()
    adapter = _adapter(fake)

    with pytest.raises(ValueError, match="qos must be between 0 and 2"):
        await adapter.publish("t", "p", qos=3)

    assert fake.published == []


async def test_publish_rejects_negative_qos() -> None:
    fake = _FakeAiomqttClient()
    adapter = _adapter(fake)

    with pytest.raises(ValueError, match="qos must be between 0 and 2"):
        await adapter.publish("t", "p", qos=-1)


@pytest.mark.parametrize("qos", [0, 1, 2])
async def test_publish_passes_through_qos_and_retain(qos: int) -> None:
    fake = _FakeAiomqttClient()
    adapter = _adapter(fake)

    await adapter.publish("t", "p", retain=True, qos=qos)

    assert fake.published == [("t", "p", True, qos)]
