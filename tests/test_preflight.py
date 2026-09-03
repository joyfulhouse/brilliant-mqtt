"""Panel-side MQTT preflight state-machine and CLI tests."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import sys
import threading
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Any
from uuid import UUID

import aiomqtt
import pytest

import brilliant_mqtt.preflight as preflight_module
from brilliant_mqtt import mqttio
from brilliant_mqtt.config import Settings
from brilliant_mqtt.preflight import (
    PreflightReport,
    PreflightRequest,
    PreflightStage,
    async_run_preflight,
)
from brilliant_mqtt.setup_protocol import SetupRequest, SetupResult, SetupTopics
from tests.fakes import FakeMqtt

SETUP_ID = "12345678-1234-4abc-8def-1234567890ab"
SECOND_SETUP_ID = "87654321-4321-4abc-8def-ba0987654321"
PANEL_NONCE = "panel-nonce"
HA_NONCE = "ha-nonce"
REQUEST_OBJECT: dict[str, object] = {
    "schema_version": 1,
    "setup_id": SETUP_ID,
    "panel_nonce": PANEL_NONCE,
    "ha_nonce": HA_NONCE,
    "timeout_seconds": 10.0,
}

PublishHook = Callable[[str, str, bool, int], Awaitable[None]]
SubscribeHook = Callable[[str], Awaitable[None]]


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _settings() -> Settings:
    return Settings(
        panel="office",
        mqtt_host="broker.internal.example",
        mqtt_username="preflight-user",
        mqtt_password="password-do-not-leak",
        mqtt_port=8883,
        mqtt_tls_enabled=True,
        mqtt_tls_ca_file="/run/secrets/private-ca.pem",
    )


def _request(
    *,
    setup_id: str = SETUP_ID,
    timeout_seconds: float = 10.0,
) -> PreflightRequest:
    value = dict(REQUEST_OBJECT)
    value["setup_id"] = setup_id
    value["timeout_seconds"] = timeout_seconds
    return PreflightRequest.from_json(_canonical(value))


class RecordingMqtt(FakeMqtt):
    """Fake MQTT lifecycle with injectable failures, stalls, and message hooks."""

    def __init__(self) -> None:
        super().__init__()
        self.events: list[tuple[object, ...]] = []
        self.failures: dict[tuple[str, ...], BaseException] = {}
        self.blocked: set[tuple[str, ...]] = set()
        self.cancellation_delays: dict[tuple[str, ...], float] = {}
        self.cancellation_pauses: dict[tuple[str, ...], tuple[asyncio.Event, asyncio.Event]] = {}
        self.post_cancellation_failures: dict[tuple[str, ...], BaseException] = {}
        self.post_cancellation_failures_raised: list[tuple[str, ...]] = []
        self.pauses: dict[tuple[str, ...], tuple[asyncio.Event, asyncio.Event]] = {}
        self.publish_hook: PublishHook | None = None
        self.subscribe_hook: SubscribeHook | None = None
        self._payload_decode_error_cbs: list[
            Callable[[mqttio.MqttPayloadDecodeError], Awaitable[None]]
        ] = []

    async def _before(self, key: tuple[str, ...]) -> None:
        self.events.append(key)
        pause = self.pauses.get(key)
        if pause is not None:
            entered, release = pause
            entered.set()
            await release.wait()
        if key in self.blocked:
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                cancellation_pause = self.cancellation_pauses.get(key)
                if cancellation_pause is not None:
                    entered, release = cancellation_pause
                    entered.set()
                    await release.wait()
                delay = self.cancellation_delays.get(key)
                if delay is not None:
                    await asyncio.sleep(delay)
                post_cancellation_failure = self.post_cancellation_failures.get(key)
                if post_cancellation_failure is not None:
                    self.post_cancellation_failures_raised.append(key)
                    raise post_cancellation_failure from None
                raise
        failure = self.failures.get(key)
        if failure is not None:
            raise failure

    async def connect(self) -> None:
        await self._before(("connect",))
        await super().connect()

    async def disconnect(self) -> None:
        await self._before(("disconnect",))
        await super().disconnect()

    async def publish(
        self,
        topic: str,
        payload: str,
        retain: bool = False,
        qos: int = 0,
    ) -> None:
        await self._before(("publish", topic, payload, str(retain), str(qos)))
        await super().publish(topic, payload, retain=retain, qos=qos)
        if self.publish_hook is not None:
            await self.publish_hook(topic, payload, retain, qos)

    def on_message(self, cb: Callable[[str, str, bool], Awaitable[None]]) -> None:
        self.events.append(("on_message",))
        super().on_message(cb)

    def on_payload_decode_error(
        self,
        cb: Callable[[mqttio.MqttPayloadDecodeError], Awaitable[None]],
    ) -> None:
        self._payload_decode_error_cbs.append(cb)

    async def subscribe(self, topic: str) -> None:
        await self._before(("subscribe", topic))
        await super().subscribe(topic)
        if self.subscribe_hook is not None:
            await self.subscribe_hook(topic)

    async def unsubscribe(self, topic: str) -> None:
        await self._before(("unsubscribe", topic))
        await super().unsubscribe(topic)


def _successful_mqtt(request: PreflightRequest) -> RecordingMqtt:
    mqtt = RecordingMqtt()
    topics = SetupTopics.for_id(request.setup_id)
    result = SetupResult(
        setup_id=request.setup_id,
        nonce=request.ha_nonce,
        reply_to_nonce=request.panel_nonce,
    ).to_payload()

    async def publish_hook(topic: str, payload: str, retain: bool, qos: int) -> None:
        if topic == topics.panel_to_ha:
            # Deliver during publish to prove the receive subscription/future
            # already exists before an immediate HA response arrives.
            await mqtt.inject(topics.ha_to_panel, result)

    async def subscribe_hook(topic: str) -> None:
        if topic == topics.retained:
            # A broker may deliver retained replay before SUBACK handling returns.
            await mqtt.inject(topic, request.panel_nonce, retained=True)

    mqtt.publish_hook = publish_hook
    mqtt.subscribe_hook = subscribe_hook
    return mqtt


class _ConcreteMessageStream:
    def __init__(self) -> None:
        self._messages: asyncio.Queue[aiomqtt.Message] = asyncio.Queue()

    def __aiter__(self) -> _ConcreteMessageStream:
        return self

    async def __anext__(self) -> aiomqtt.Message:
        return await self._messages.get()

    def feed(self, topic: str, payload: bytes | str, *, retained: bool) -> None:
        self._messages.put_nowait(
            aiomqtt.Message(
                topic=topic,
                payload=payload.encode() if isinstance(payload, str) else payload,
                qos=1,
                retain=retained,
                mid=1,
                properties=None,
            )
        )


class _CloseDependentMessageStream(_ConcreteMessageStream):
    """aiomqtt-shaped iterator whose cancellation teardown needs disconnect."""

    def __init__(
        self,
        close_released: asyncio.Event,
        lifecycle: list[str],
        *,
        teardown_delay: float,
    ) -> None:
        super().__init__()
        self._close_released = close_released
        self._lifecycle = lifecycle
        self._teardown_delay = teardown_delay
        self.reader_started = asyncio.Event()
        self.reader_cancellation_started = asyncio.Event()
        self.reader_teardown_cancelled = asyncio.Event()
        self.reader_finished = asyncio.Event()

    async def __anext__(self) -> aiomqtt.Message:
        self.reader_started.set()
        try:
            return await super().__anext__()
        except asyncio.CancelledError:
            self._lifecycle.append("reader_cancellation_started")
            self.reader_cancellation_started.set()
            while not self._close_released.is_set():
                try:
                    await self._close_released.wait()
                except asyncio.CancelledError:
                    self._lifecycle.append("reader_teardown_cancelled")
                    self.reader_teardown_cancelled.set()
            deadline = asyncio.get_running_loop().time() + self._teardown_delay
            while (remaining := deadline - asyncio.get_running_loop().time()) > 0:
                try:
                    await asyncio.sleep(remaining)
                except asyncio.CancelledError:
                    self._lifecycle.append("reader_teardown_cancelled")
                    self.reader_teardown_cancelled.set()
            self._lifecycle.append("reader_finished")
            self.reader_finished.set()
            raise


class _ConcretePreflightClient:
    """Broker seam used while exercising the real AioMqttAdapter lifecycle."""

    def __init__(
        self,
        request: PreflightRequest,
        close_error: BaseException | None,
    ) -> None:
        self._request = request
        self._topics = SetupTopics.for_id(request.setup_id)
        self._close_error = close_error
        self._retained_payload: str | None = None
        self.messages = _ConcreteMessageStream()
        self.ha_payload: bytes | str = SetupResult(
            setup_id=request.setup_id,
            nonce=request.ha_nonce,
            reply_to_nonce=request.panel_nonce,
        ).to_payload()
        self.messages_before_ha: list[tuple[str, bytes | str, bool]] = []
        self.retained_replay_payload: bytes | str | None = None
        self.exit_attempts = 0
        self.unsubscriptions: list[str] = []

    async def __aenter__(self) -> _ConcretePreflightClient:
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.exit_attempts += 1
        if self._close_error is not None:
            raise self._close_error

    async def publish(
        self,
        topic: str,
        payload: str,
        retain: bool = False,
        qos: int = 0,
    ) -> None:
        if topic == self._topics.panel_to_ha:
            for incoming_topic, incoming_payload, incoming_retained in self.messages_before_ha:
                self.messages.feed(
                    incoming_topic,
                    incoming_payload,
                    retained=incoming_retained,
                )
            self.messages.feed(self._topics.ha_to_panel, self.ha_payload, retained=False)
        if topic == self._topics.retained and retain:
            self._retained_payload = payload

    async def subscribe(self, topic: str) -> tuple[int, ...]:
        if topic == self._topics.retained and self._retained_payload:
            payload = self.retained_replay_payload
            if payload is None:
                payload = self._retained_payload
            self.messages.feed(topic, payload, retained=True)
        return (0,)

    async def unsubscribe(self, topic: str) -> None:
        self.unsubscriptions.append(topic)


class _ThreadDelayedEntryPreflightClient(_ConcretePreflightClient):
    """Production-shaped raw client with executor-backed connection entry."""

    def __init__(
        self,
        request: PreflightRequest,
        *,
        enter_error: BaseException | None = None,
    ) -> None:
        super().__init__(request, None)
        self._enter_error = enter_error
        self._loop = asyncio.get_running_loop()
        self._release_enter_worker = threading.Event()
        self.enter_worker_started = asyncio.Event()
        self.enter_worker_finished = asyncio.Event()
        self.enter_worker_active = False
        self.enter_succeeded = False
        self.exit_while_enter_active = False
        self.exit_after_successful_enter = False

    def _enter_worker(self) -> None:
        self.enter_worker_active = True
        self._loop.call_soon_threadsafe(self.enter_worker_started.set)
        try:
            self._release_enter_worker.wait()
            if self._enter_error is not None:
                raise self._enter_error
            self.enter_succeeded = True
        finally:
            self.enter_worker_active = False
            self._loop.call_soon_threadsafe(self.enter_worker_finished.set)

    async def __aenter__(self) -> _ThreadDelayedEntryPreflightClient:
        await asyncio.to_thread(self._enter_worker)
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.exit_while_enter_active |= self.enter_worker_active
        self.exit_after_successful_enter |= self.enter_succeeded
        await super().__aexit__(exc_type, exc, traceback)

    def release_enter_worker(self) -> None:
        self._release_enter_worker.set()


class _CloseDependentReaderPreflightClient(_ConcretePreflightClient):
    """Concrete client whose reader can finish only after client close starts."""

    def __init__(
        self,
        request: PreflightRequest,
        *,
        reader_teardown_delay: float = 0.0,
    ) -> None:
        super().__init__(request, None)
        self.close_released = asyncio.Event()
        self.close_started = asyncio.Event()
        self.lifecycle: list[str] = []
        self.close_dependent_messages = _CloseDependentMessageStream(
            self.close_released,
            self.lifecycle,
            teardown_delay=reader_teardown_delay,
        )
        self.messages = self.close_dependent_messages

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.lifecycle.append("close_started")
        self.close_started.set()
        self.close_released.set()
        await super().__aexit__(exc_type, exc, traceback)


class _AckRacePreflightClient(_ConcretePreflightClient):
    """Concrete adapter seam with one broker acknowledgement held pending."""

    def __init__(
        self,
        request: PreflightRequest,
        *,
        pending_operation: tuple[str, str],
        post_cancellation_error: BaseException | None,
    ) -> None:
        super().__init__(request, None)
        self._pending_operation = pending_operation
        self._post_cancellation_error = post_cancellation_error
        self.ack_pending = asyncio.Event()
        self.cancellation_started = asyncio.Event()
        self.release_cancellation = asyncio.Event()
        self.cancellation_finished = asyncio.Event()
        self.close_started = asyncio.Event()
        self.operations: list[tuple[object, ...]] = []

    async def _hold_ack(self, operation: str, topic: str) -> None:
        if (operation, topic) != self._pending_operation:
            return
        self.ack_pending.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            self.cancellation_started.set()
            await self.release_cancellation.wait()
            self.cancellation_finished.set()
            if self._post_cancellation_error is not None:
                raise self._post_cancellation_error from None
            raise

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.operations.append(("disconnect",))
        self.close_started.set()
        await super().__aexit__(exc_type, exc, traceback)

    async def publish(
        self,
        topic: str,
        payload: str,
        retain: bool = False,
        qos: int = 0,
    ) -> None:
        self.operations.append(("publish", topic, payload, retain, qos))
        await super().publish(topic, payload, retain=retain, qos=qos)
        await self._hold_ack("publish", topic)

    async def subscribe(self, topic: str) -> tuple[int, ...]:
        self.operations.append(("subscribe", topic))
        result = await super().subscribe(topic)
        await self._hold_ack("subscribe", topic)
        return result

    async def unsubscribe(self, topic: str) -> None:
        self.operations.append(("unsubscribe", topic))
        await super().unsubscribe(topic)


async def test_fleet_auth_timeout_settles_successful_raw_entry_before_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings()
    request = _request(timeout_seconds=0.01)
    client = _ThreadDelayedEntryPreflightClient(request)
    monkeypatch.setattr(aiomqtt, "Client", lambda **kwargs: client)
    monkeypatch.setattr(mqttio, "build_tls_context", lambda received: None)
    preflight_task = asyncio.create_task(async_run_preflight(settings, request))

    try:
        await asyncio.wait_for(client.enter_worker_started.wait(), timeout=0.2)
        done_while_enter_active, _ = await asyncio.wait({preflight_task}, timeout=0.05)
        returned_while_enter_active = bool(done_while_enter_active)
        exit_raced_enter = client.exit_while_enter_active
    finally:
        client.release_enter_worker()

    await asyncio.wait_for(client.enter_worker_finished.wait(), timeout=0.2)
    report = await asyncio.wait_for(preflight_task, timeout=0.2)

    assert returned_while_enter_active is False
    assert exit_raced_enter is False
    assert report.success is False
    assert report.failed_stage is PreflightStage.FLEET_AUTH
    assert report.error_code == "mqtt_timeout"
    assert client.enter_succeeded is True
    assert client.exit_attempts == 1
    assert client.exit_after_successful_enter is True
    assert client.exit_while_enter_active is False
    assert client.enter_worker_active is False


async def test_fleet_auth_timeout_settles_failed_raw_entry_without_exit(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = _settings()
    request = _request(timeout_seconds=0.01)
    raw_error = "delayed raw entry failure password=must-not-leak"
    client = _ThreadDelayedEntryPreflightClient(
        request,
        enter_error=RuntimeError(raw_error),
    )
    monkeypatch.setattr(aiomqtt, "Client", lambda **kwargs: client)
    monkeypatch.setattr(mqttio, "build_tls_context", lambda received: None)
    preflight_task = asyncio.create_task(async_run_preflight(settings, request))

    try:
        await asyncio.wait_for(client.enter_worker_started.wait(), timeout=0.2)
        done_while_enter_active, _ = await asyncio.wait({preflight_task}, timeout=0.05)
        returned_while_enter_active = bool(done_while_enter_active)
        exit_raced_enter = client.exit_while_enter_active
    finally:
        client.release_enter_worker()

    await asyncio.wait_for(client.enter_worker_finished.wait(), timeout=0.2)
    report = await asyncio.wait_for(preflight_task, timeout=0.2)

    assert returned_while_enter_active is False
    assert exit_raced_enter is False
    assert report.success is False
    assert report.failed_stage is PreflightStage.FLEET_AUTH
    assert report.error_code == "mqtt_timeout"
    assert client.enter_succeeded is False
    assert client.exit_attempts == 0
    assert client.exit_while_enter_active is False
    assert client.enter_worker_active is False
    assert raw_error not in report.to_json()
    assert raw_error not in caplog.text


async def test_caller_cancellation_settles_successful_raw_entry_before_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings()
    request = _request(timeout_seconds=1.0)
    client = _ThreadDelayedEntryPreflightClient(request)
    monkeypatch.setattr(aiomqtt, "Client", lambda **kwargs: client)
    monkeypatch.setattr(mqttio, "build_tls_context", lambda received: None)
    preflight_task = asyncio.create_task(async_run_preflight(settings, request))

    try:
        await asyncio.wait_for(client.enter_worker_started.wait(), timeout=0.2)
        preflight_task.cancel()
        done_while_enter_active, _ = await asyncio.wait({preflight_task}, timeout=0.05)
        returned_while_enter_active = bool(done_while_enter_active)
        exit_raced_enter = client.exit_while_enter_active
    finally:
        client.release_enter_worker()

    await asyncio.wait_for(client.enter_worker_finished.wait(), timeout=0.2)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(preflight_task, timeout=0.2)

    assert returned_while_enter_active is False
    assert exit_raced_enter is False
    assert client.enter_succeeded is True
    assert client.exit_attempts == 1
    assert client.exit_after_successful_enter is True
    assert client.exit_while_enter_active is False
    assert client.enter_worker_active is False


async def test_checked_disconnect_closes_before_draining_close_dependent_reader(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings()
    client = _CloseDependentReaderPreflightClient(_request())
    monkeypatch.setattr(aiomqtt, "Client", lambda **kwargs: client)
    monkeypatch.setattr(mqttio, "build_tls_context", lambda received: None)
    adapter = mqttio.AioMqttAdapter(
        settings,
        publish_availability=False,
        checked_disconnect=True,
        redacted_logging=True,
    )
    await adapter.connect()
    await asyncio.wait_for(client.close_dependent_messages.reader_started.wait(), timeout=0.1)
    disconnect_task = asyncio.create_task(adapter.disconnect())

    try:
        await asyncio.wait_for(client.close_started.wait(), timeout=0.2)
    finally:
        client.close_released.set()
        await asyncio.wait_for(asyncio.shield(disconnect_task), timeout=0.2)

    assert client.exit_attempts == 1
    assert client.close_dependent_messages.reader_cancellation_started.is_set()
    assert client.close_dependent_messages.reader_finished.is_set()
    assert adapter._reader_task is None


async def test_preflight_cleanup_closes_before_reader_drain_without_pending_tasks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings()
    request = _request(timeout_seconds=0.12)
    client = _CloseDependentReaderPreflightClient(request)
    monkeypatch.setattr(aiomqtt, "Client", lambda **kwargs: client)
    monkeypatch.setattr(mqttio, "build_tls_context", lambda received: None)
    adapters: list[mqttio.AioMqttAdapter] = []

    def factory(
        settings: Settings,
        *,
        identifier: str,
        publish_availability: bool,
        checked_disconnect: bool,
        redacted_logging: bool,
    ) -> mqttio.AioMqttAdapter:
        adapter = mqttio.AioMqttAdapter(
            settings,
            identifier=identifier,
            publish_availability=publish_availability,
            checked_disconnect=checked_disconnect,
            redacted_logging=redacted_logging,
        )
        adapters.append(adapter)
        return adapter

    loop = asyncio.get_running_loop()
    loop_contexts: list[dict[str, Any]] = []
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: loop_contexts.append(context))
    started = loop.time()
    preflight_task = asyncio.create_task(
        async_run_preflight(settings, request, mqtt_factory=factory)
    )

    try:
        report = await asyncio.wait_for(asyncio.shield(preflight_task), timeout=0.5)
        elapsed = loop.time() - started
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert report.success is True
        assert elapsed < 0.3
        assert client.exit_attempts == 1
        assert client.close_dependent_messages.reader_finished.is_set()
        assert adapters[0]._reader_task is None
        assert preflight_module._detached_cleanup_tasks == set()
        assert preflight_module._detached_preflight_futures == set()
        assert loop_contexts == []
    finally:
        client.close_released.set()
        if not preflight_task.done():
            preflight_task.cancel()
        await asyncio.gather(preflight_task, return_exceptions=True)
        for _ in range(10):
            if not preflight_module._detached_cleanup_tasks:
                break
            await asyncio.sleep(0)
        loop.set_exception_handler(previous_handler)


async def test_preflight_keeps_slow_reader_settlement_owned_after_cleanup_slot_expires(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings()
    request = _request(timeout_seconds=0.12)
    client = _CloseDependentReaderPreflightClient(
        request,
        reader_teardown_delay=0.06,
    )
    monkeypatch.setattr(aiomqtt, "Client", lambda **kwargs: client)
    monkeypatch.setattr(mqttio, "build_tls_context", lambda received: None)
    started = asyncio.get_running_loop().time()

    try:
        report = await asyncio.wait_for(
            async_run_preflight(settings, request),
            timeout=0.5,
        )
    finally:
        client.close_released.set()
        await asyncio.wait_for(
            client.close_dependent_messages.reader_finished.wait(),
            timeout=0.2,
        )
        for _ in range(10):
            if not preflight_module._detached_cleanup_tasks:
                break
            await asyncio.sleep(0)

    assert report.success is False
    assert report.failed_stage is PreflightStage.CLEANUP
    assert report.error_code == "mqtt_timeout"
    assert asyncio.get_running_loop().time() - started < 0.3
    assert client.exit_attempts == 1
    assert not client.close_dependent_messages.reader_teardown_cancelled.is_set()
    assert client.lifecycle.index("close_started") < client.lifecycle.index("reader_finished")
    assert preflight_module._detached_cleanup_tasks == set()


def test_close_dependent_reader_does_not_hang_asyncio_run_shutdown() -> None:
    repository_root = Path(__file__).resolve().parents[1]
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join(
        (str(repository_root / "src"), str(repository_root))
    )
    script = """
import asyncio
import aiomqtt

from brilliant_mqtt import mqttio
from brilliant_mqtt.preflight import async_run_preflight
from tests.test_preflight import (
    _CloseDependentReaderPreflightClient,
    _request,
    _settings,
)


async def run() -> None:
    request = _request(timeout_seconds=0.12)
    client = _CloseDependentReaderPreflightClient(request)
    aiomqtt.Client = lambda **kwargs: client
    mqttio.build_tls_context = lambda received: None
    report = await async_run_preflight(_settings(), request)
    assert report.success
    assert client.exit_attempts == 1
    assert client.close_dependent_messages.reader_finished.is_set()


asyncio.run(run())
"""

    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
        timeout=5.0,
        env=environment,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout == ""
    assert completed.stderr == ""


def test_request_parses_exact_contract_and_stage_values() -> None:
    request = _request()

    assert request.setup_id == UUID(SETUP_ID)
    assert request.panel_nonce == PANEL_NONCE
    assert request.ha_nonce == HA_NONCE
    assert request.timeout_seconds == 10.0
    assert [stage.value for stage in PreflightStage] == [
        "fleet_auth",
        "panel_to_ha",
        "ha_to_panel",
        "discovery_write",
        "retained_message",
        "cleanup",
    ]


@pytest.mark.parametrize(
    "mutation",
    [
        {"schema_version": 2},
        {"setup_id": "not-a-uuid"},
        {"panel_nonce": ""},
        {"ha_nonce": 3},
        {"timeout_seconds": 0.0},
        {"timeout_seconds": True},
        {"extra": "rejected"},
    ],
)
def test_request_rejects_invalid_or_non_exact_contract(mutation: dict[str, object]) -> None:
    value = dict(REQUEST_OBJECT)
    value.update(mutation)

    with pytest.raises(ValueError, match=r"^invalid_preflight_request"):
        PreflightRequest.from_json(_canonical(value))


def test_request_rejects_missing_key_and_non_object_json() -> None:
    missing = dict(REQUEST_OBJECT)
    del missing["ha_nonce"]

    with pytest.raises(ValueError, match=r"^invalid_preflight_request"):
        PreflightRequest.from_json(_canonical(missing))
    with pytest.raises(ValueError, match=r"^invalid_preflight_request"):
        PreflightRequest.from_json("[]")


def test_request_requires_setup_id_to_be_a_json_string() -> None:
    value = dict(REQUEST_OBJECT)
    value["setup_id"] = 12345678123441238123123456789012

    with pytest.raises(ValueError, match=r"^invalid_preflight_request"):
        PreflightRequest.from_json(_canonical(value))


def test_request_maps_huge_integer_timeout_to_stable_validation_error() -> None:
    value = dict(REQUEST_OBJECT)
    value["timeout_seconds"] = 10**400

    with pytest.raises(ValueError, match=r"^invalid_preflight_request"):
        PreflightRequest.from_json(_canonical(value))


def test_request_maps_json_integer_digit_limit_to_stable_validation_error() -> None:
    raw = (
        '{"ha_nonce":"ha-nonce","panel_nonce":"panel-nonce","schema_version":1,'
        f'"setup_id":"{SETUP_ID}","timeout_seconds":' + "1" * 5_000 + "}"
    )

    with pytest.raises(ValueError, match=r"^invalid_preflight_request"):
        PreflightRequest.from_json(raw)


def test_internal_failure_is_raise_safe_and_logically_immutable() -> None:
    @contextmanager
    def passthrough() -> Iterator[None]:
        yield

    error = preflight_module._Failure(
        PreflightStage.DISCOVERY_WRITE,
        "mqtt_timeout",
        "MQTT stage timed out",
    )

    with pytest.raises(preflight_module._Failure) as raised:
        with passthrough():
            raise error

    assert raised.value is error
    assert raised.value.__traceback__ is not None
    with pytest.raises(FrozenInstanceError):
        error.args = ("raw-secret",)
    with pytest.raises(FrozenInstanceError):
        del error.code
    assert error.code == "mqtt_timeout"


async def test_success_uses_exact_order_nonces_qos_retain_and_canonical_report(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request()
    topics = SetupTopics.for_id(request.setup_id)
    mqtt = _successful_mqtt(request)
    ticks: Iterator[int] = iter([0, 11, 20, 32, 40, 53, 60, 74, 80, 95, 100, 116])
    monkeypatch.setattr(preflight_module, "_monotonic_ms", lambda: next(ticks))

    report = await async_run_preflight(_settings(), request, mqtt_factory=lambda *args, **kw: mqtt)

    assert report.success is True
    assert report.completed_stages == tuple(PreflightStage)
    assert report.stage_elapsed_ms == {
        PreflightStage.FLEET_AUTH: 11,
        PreflightStage.PANEL_TO_HA: 12,
        PreflightStage.HA_TO_PANEL: 13,
        PreflightStage.DISCOVERY_WRITE: 14,
        PreflightStage.RETAINED_MESSAGE: 15,
        PreflightStage.CLEANUP: 16,
    }
    assert mqtt.events == [
        ("on_message",),
        ("connect",),
        ("subscribe", topics.ha_to_panel),
        (
            "publish",
            topics.panel_to_ha,
            SetupRequest(request.setup_id, PANEL_NONCE).to_payload(),
            "False",
            "1",
        ),
        ("publish", topics.discovery_probe, PANEL_NONCE, "True", "1"),
        ("publish", topics.retained, PANEL_NONCE, "True", "1"),
        ("subscribe", topics.retained),
        ("publish", topics.discovery_probe, "", "True", "1"),
        ("publish", topics.retained, "", "True", "1"),
        ("unsubscribe", topics.ha_to_panel),
        ("unsubscribe", topics.retained),
        ("disconnect",),
    ]
    assert mqtt.published_qos == [1, 1, 1, 1, 1]
    assert mqtt.subscriptions == []
    assert report.to_json() == _canonical(
        {
            "completed_stages": [stage.value for stage in PreflightStage],
            "last_stage": "cleanup",
            "schema_version": 1,
            "setup_id": SETUP_ID,
            "stage_elapsed_ms": {
                "cleanup": 16,
                "discovery_write": 14,
                "fleet_auth": 11,
                "ha_to_panel": 13,
                "panel_to_ha": 12,
                "retained_message": 15,
            },
            "success": True,
        }
    )


async def test_factory_gets_unique_setup_client_id_and_no_availability_lwt() -> None:
    requests = [_request(), _request(setup_id=SECOND_SETUP_ID)]
    mqtt_clients = [_successful_mqtt(request) for request in requests]
    calls: list[tuple[Settings, dict[str, object]]] = []

    def factory(settings: Settings, **kwargs: object) -> RecordingMqtt:
        calls.append((settings, kwargs))
        return mqtt_clients[len(calls) - 1]

    for request in requests:
        assert (await async_run_preflight(_settings(), request, mqtt_factory=factory)).success

    assert calls == [
        (
            _settings(),
            {
                "identifier": f"brilliant-mqtt-setup-{SETUP_ID}",
                "publish_availability": False,
                "checked_disconnect": True,
                "redacted_logging": True,
            },
        ),
        (
            _settings(),
            {
                "identifier": f"brilliant-mqtt-setup-{SECOND_SETUP_ID}",
                "publish_availability": False,
                "checked_disconnect": True,
                "redacted_logging": True,
            },
        ),
    ]


@pytest.mark.parametrize(
    ("failure_key", "failed_stage", "error_code"),
    [
        (("connect",), PreflightStage.FLEET_AUTH, "mqtt_connect"),
        (("subscribe", "ha_to_panel"), PreflightStage.PANEL_TO_HA, "mqtt_subscribe"),
        (("publish", "panel_to_ha"), PreflightStage.PANEL_TO_HA, "mqtt_publish"),
        (("publish", "discovery_probe"), PreflightStage.DISCOVERY_WRITE, "mqtt_publish"),
        (("publish", "retained"), PreflightStage.RETAINED_MESSAGE, "mqtt_publish"),
        (("subscribe", "retained"), PreflightStage.RETAINED_MESSAGE, "mqtt_subscribe"),
    ],
)
async def test_mqtt_operation_failures_map_to_stable_stage_codes(
    failure_key: tuple[str, ...],
    failed_stage: PreflightStage,
    error_code: str,
) -> None:
    request = _request()
    topics = SetupTopics.for_id(request.setup_id)
    mqtt = _successful_mqtt(request)
    topic_aliases = {
        "ha_to_panel": topics.ha_to_panel,
        "panel_to_ha": topics.panel_to_ha,
        "discovery_probe": topics.discovery_probe,
        "retained": topics.retained,
    }
    operation, *parts = failure_key
    resolved = tuple(topic_aliases.get(part, part) for part in parts)
    key: tuple[str, ...]
    if operation == "publish":
        topic = resolved[0]
        payload = {
            topics.panel_to_ha: SetupRequest(request.setup_id, PANEL_NONCE).to_payload(),
            topics.discovery_probe: PANEL_NONCE,
            topics.retained: PANEL_NONCE,
        }[topic]
        key = (operation, topic, payload, "False" if topic == topics.panel_to_ha else "True", "1")
    else:
        key = (operation, *resolved)
    mqtt.failures[key] = RuntimeError("raw-broker-secret=password-do-not-leak")

    report = await async_run_preflight(_settings(), request, mqtt_factory=lambda *args, **kw: mqtt)

    assert report.success is False
    assert report.failed_stage is failed_stage
    assert report.error_code == error_code
    assert report.detail in {
        "MQTT connection failed",
        "MQTT publish failed",
        "MQTT subscription failed",
    }


@pytest.mark.parametrize(
    "payload",
    [
        "not-json",
        SetupResult(UUID(SECOND_SETUP_ID), HA_NONCE, PANEL_NONCE).to_payload(),
        SetupResult(UUID(SETUP_ID), "wrong-ha-nonce", PANEL_NONCE).to_payload(),
        SetupResult(UUID(SETUP_ID), HA_NONCE, "wrong-panel-nonce").to_payload(),
    ],
)
async def test_ha_result_requires_exact_valid_setup_and_nonces(payload: str) -> None:
    request = _request(timeout_seconds=0.01)
    topics = SetupTopics.for_id(request.setup_id)
    mqtt = _successful_mqtt(request)

    async def publish_hook(topic: str, sent: str, retain: bool, qos: int) -> None:
        if topic == topics.panel_to_ha:
            await mqtt.inject(topics.ha_to_panel, payload)

    mqtt.publish_hook = publish_hook

    report = await async_run_preflight(_settings(), request, mqtt_factory=lambda *args, **kw: mqtt)

    assert report.success is False
    assert report.failed_stage is PreflightStage.HA_TO_PANEL
    assert report.error_code == "mqtt_timeout"
    assert report.detail == "MQTT stage timed out"


@pytest.mark.parametrize(
    ("payload", "retained", "error_code"),
    [
        (PANEL_NONCE, False, "retained_flag_missing"),
        ("wrong-retained-payload", True, "mqtt_timeout"),
    ],
)
async def test_retained_replay_requires_exact_payload_and_retained_flag(
    payload: str,
    retained: bool,
    error_code: str,
) -> None:
    request = _request(timeout_seconds=0.01)
    topics = SetupTopics.for_id(request.setup_id)
    mqtt = _successful_mqtt(request)

    async def subscribe_hook(topic: str) -> None:
        if topic == topics.retained:
            await mqtt.inject(topic, payload, retained=retained)

    mqtt.subscribe_hook = subscribe_hook

    report = await async_run_preflight(_settings(), request, mqtt_factory=lambda *args, **kw: mqtt)

    assert report.success is False
    assert report.failed_stage is PreflightStage.RETAINED_MESSAGE
    assert report.error_code == error_code


@pytest.mark.parametrize(
    ("payload_target", "failed_stage"),
    [
        ("ha_to_panel", PreflightStage.HA_TO_PANEL),
        ("retained", PreflightStage.RETAINED_MESSAGE),
    ],
)
async def test_real_adapter_invalid_utf8_on_exact_validation_topic_times_out(
    payload_target: str,
    failed_stage: PreflightStage,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = _settings()
    request = _request(timeout_seconds=0.02)
    invalid_payload = b"\xffmalformed-payload-secret"
    client = _ConcretePreflightClient(request, None)
    if payload_target == "ha_to_panel":
        client.ha_payload = invalid_payload
    else:
        client.retained_replay_payload = invalid_payload
    monkeypatch.setattr(aiomqtt, "Client", lambda **kwargs: client)
    monkeypatch.setattr(mqttio, "build_tls_context", lambda received: None)
    caplog.set_level(logging.DEBUG, logger=mqttio.__name__)

    report = await asyncio.wait_for(
        async_run_preflight(settings, request),
        timeout=0.5,
    )

    assert report.success is False
    assert report.failed_stage is failed_stage
    assert report.error_code == "mqtt_timeout"
    assert report.detail == "MQTT stage timed out"
    assert client.exit_attempts == 1
    assert "malformed-payload-secret" not in report.to_json()
    assert "malformed-payload-secret" not in caplog.text


async def _assert_malformed_payload_does_not_override_pending_ack_timeout(
    *,
    pending_operation_name: str,
    failed_stage: PreflightStage,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = _settings()
    request = _request(timeout_seconds=0.05)
    topics = SetupTopics.for_id(request.setup_id)
    pending_topic = topics.panel_to_ha if pending_operation_name == "publish" else topics.retained
    raw_payload = b"\xffack-race-payload-secret"
    raw_cancellation_error = "ack-race-cancellation password=must-not-leak"
    client = _AckRacePreflightClient(
        request,
        pending_operation=(pending_operation_name, pending_topic),
        post_cancellation_error=RuntimeError(raw_cancellation_error),
    )
    if pending_operation_name == "publish":
        client.ha_payload = raw_payload
    else:
        client.retained_replay_payload = raw_payload
    monkeypatch.setattr(aiomqtt, "Client", lambda **kwargs: client)
    monkeypatch.setattr(mqttio, "build_tls_context", lambda received: None)
    caplog.set_level(logging.DEBUG, logger=mqttio.__name__)
    loop = asyncio.get_running_loop()
    loop_contexts: list[dict[str, Any]] = []
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: loop_contexts.append(context))
    preflight_task = asyncio.create_task(async_run_preflight(settings, request))
    report: PreflightReport | None = None
    cleanup_finished_before_release = False

    try:
        await asyncio.wait_for(client.ack_pending.wait(), timeout=0.2)
        await asyncio.wait_for(client.cancellation_started.wait(), timeout=0.2)
        try:
            await asyncio.wait_for(client.close_started.wait(), timeout=0.1)
        except asyncio.TimeoutError:
            pass
        else:
            cleanup_finished_before_release = True
            report = await asyncio.wait_for(asyncio.shield(preflight_task), timeout=0.2)
    finally:
        client.release_cancellation.set()

    try:
        if report is None:
            report = await asyncio.wait_for(preflight_task, timeout=0.3)
        await asyncio.wait_for(client.cancellation_finished.wait(), timeout=0.2)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
    finally:
        client.release_cancellation.set()
        if not preflight_task.done():
            preflight_task.cancel()
            await asyncio.gather(preflight_task, return_exceptions=True)
        loop.set_exception_handler(previous_handler)

    cleanup_operations = [
        ("publish", topics.discovery_probe, "", True, 1),
        ("publish", topics.retained, "", True, 1),
        ("unsubscribe", topics.ha_to_panel),
    ]
    if pending_operation_name == "subscribe":
        cleanup_operations.append(("unsubscribe", topics.retained))
    cleanup_operations.append(("disconnect",))

    assert cleanup_finished_before_release is True
    assert report.success is False
    assert report.failed_stage is failed_stage
    assert report.error_code == "mqtt_timeout"
    assert report.detail == "MQTT stage timed out"
    assert [item for item in client.operations if item in cleanup_operations] == cleanup_operations
    assert client.exit_attempts == 1
    assert loop_contexts == []
    assert "ack-race-payload-secret" not in report.to_json()
    assert "ack-race-payload-secret" not in caplog.text
    assert raw_cancellation_error not in report.to_json()
    assert raw_cancellation_error not in caplog.text


async def test_real_adapter_invalid_utf8_does_not_override_pending_request_puback(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    await _assert_malformed_payload_does_not_override_pending_ack_timeout(
        pending_operation_name="publish",
        failed_stage=PreflightStage.PANEL_TO_HA,
        monkeypatch=monkeypatch,
        caplog=caplog,
    )


async def test_real_adapter_invalid_utf8_does_not_override_pending_retained_suback(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    await _assert_malformed_payload_does_not_override_pending_ack_timeout(
        pending_operation_name="subscribe",
        failed_stage=PreflightStage.RETAINED_MESSAGE,
        monkeypatch=monkeypatch,
        caplog=caplog,
    )


async def test_valid_ha_response_does_not_waive_pending_request_puback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings()
    request = _request(timeout_seconds=0.02)
    topics = SetupTopics.for_id(request.setup_id)
    client = _AckRacePreflightClient(
        request,
        pending_operation=("publish", topics.panel_to_ha),
        post_cancellation_error=None,
    )
    client.release_cancellation.set()
    monkeypatch.setattr(aiomqtt, "Client", lambda **kwargs: client)
    monkeypatch.setattr(mqttio, "build_tls_context", lambda received: None)

    report = await asyncio.wait_for(
        async_run_preflight(settings, request),
        timeout=0.3,
    )

    assert report.success is False
    assert report.failed_stage is PreflightStage.PANEL_TO_HA
    assert report.error_code == "mqtt_timeout"
    assert client.exit_attempts == 1


async def test_real_adapter_ignores_unrelated_invalid_utf8_and_processes_later_valid_message(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = _settings()
    request = _request(timeout_seconds=0.1)
    client = _ConcretePreflightClient(request, None)
    client.messages_before_ha.append(
        ("brilliant/setup/unrelated/ha_to_panel", b"\xffunrelated-payload-secret", False)
    )
    monkeypatch.setattr(aiomqtt, "Client", lambda **kwargs: client)
    monkeypatch.setattr(mqttio, "build_tls_context", lambda received: None)
    caplog.set_level(logging.DEBUG, logger=mqttio.__name__)

    report = await asyncio.wait_for(
        async_run_preflight(settings, request),
        timeout=0.5,
    )

    assert report.success is True
    assert report.completed_stages == tuple(PreflightStage)
    assert client.exit_attempts == 1
    assert "unrelated-payload-secret" not in report.to_json()
    assert "unrelated-payload-secret" not in caplog.text


async def test_protocol_stages_use_request_timeout_and_cleanup_owns_its_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(timeout_seconds=0.01)
    mqtt = _successful_mqtt(request)
    topics = SetupTopics.for_id(request.setup_id)
    timeouts: list[tuple[float, bool]] = []
    real_wait_for = asyncio.wait_for

    async def record_wait_for(
        awaitable: Awaitable[Any],
        timeout: float,
        *,
        settle_on_cancel: bool = False,
    ) -> Any:
        timeouts.append((timeout, settle_on_cancel))
        return await real_wait_for(awaitable, timeout)

    monkeypatch.setattr(preflight_module, "_wait_for", record_wait_for)

    async def publish_without_response(topic: str, payload: str, retain: bool, qos: int) -> None:
        return None

    mqtt.publish_hook = publish_without_response

    report = await async_run_preflight(_settings(), request, mqtt_factory=lambda *args, **kw: mqtt)

    assert report.success is False
    assert report.failed_stage is PreflightStage.HA_TO_PANEL
    assert report.error_code == "mqtt_timeout"
    assert report.completed_stages == (
        PreflightStage.FLEET_AUTH,
        PreflightStage.PANEL_TO_HA,
        PreflightStage.CLEANUP,
    )
    # Stages that issue I/O on the shared adapter (fleet_auth, panel_to_ha —
    # and discovery_write/retained_message when reached) settle their
    # cancelled operation before cleanup touches the same client; ha_to_panel
    # only awaits an inbound message, so it has nothing to settle.
    assert timeouts == [
        (0.01, True),
        (0.01, True),
        (0.01, False),
    ]
    assert ("unsubscribe", topics.ha_to_panel) in mqtt.events
    assert ("disconnect",) in mqtt.events


async def test_every_io_stage_settles_cancelled_operations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(timeout_seconds=1.0)
    mqtt = _successful_mqtt(request)
    settle_flags: list[bool] = []
    real_wait_for = asyncio.wait_for

    async def record_wait_for(
        awaitable: Awaitable[Any],
        timeout: float,
        *,
        settle_on_cancel: bool = False,
    ) -> Any:
        settle_flags.append(settle_on_cancel)
        return await real_wait_for(awaitable, timeout)

    monkeypatch.setattr(preflight_module, "_wait_for", record_wait_for)

    report = await async_run_preflight(_settings(), request, mqtt_factory=lambda *args, **kw: mqtt)

    assert report.success is True
    # fleet_auth, panel_to_ha, ha_to_panel, discovery_write, retained_message —
    # every stage that issues adapter I/O settles its cancelled operation
    # before cleanup can touch the same client.
    assert settle_flags == [True, True, False, True, True]


async def test_cleanup_timeout_is_bounded_and_reported() -> None:
    request = _request(timeout_seconds=0.01)
    mqtt = _successful_mqtt(request)
    mqtt.blocked.add(("disconnect",))

    report = await asyncio.wait_for(
        async_run_preflight(_settings(), request, mqtt_factory=lambda *args, **kw: mqtt),
        timeout=0.2,
    )

    assert report.success is False
    assert report.failed_stage is PreflightStage.CLEANUP
    assert report.error_code == "mqtt_timeout"
    assert report.completed_stages == tuple(
        stage for stage in PreflightStage if stage is not PreflightStage.CLEANUP
    )


async def test_cleanup_advances_without_waiting_for_timed_out_action_teardown() -> None:
    request = _request(timeout_seconds=0.2)
    topics = SetupTopics.for_id(request.setup_id)
    mqtt = _successful_mqtt(request)
    cleanup_actions = [
        ("publish", topics.discovery_probe, "", "True", "1"),
        ("publish", topics.retained, "", "True", "1"),
        ("unsubscribe", topics.ha_to_panel),
        ("unsubscribe", topics.retained),
        ("disconnect",),
    ]
    mqtt.blocked.update(cleanup_actions[:4])
    mqtt.cancellation_delays.update(dict.fromkeys(cleanup_actions[:4], 0.03))
    loop = asyncio.get_running_loop()
    started = loop.time()

    report = await asyncio.wait_for(
        async_run_preflight(_settings(), request, mqtt_factory=lambda *args, **kw: mqtt),
        timeout=0.35,
    )
    elapsed = loop.time() - started
    await asyncio.sleep(0.05)

    assert report.success is False
    assert report.failed_stage is PreflightStage.CLEANUP
    assert report.error_code == "mqtt_timeout"
    assert [event for event in mqtt.events if event in cleanup_actions] == cleanup_actions
    assert elapsed < 0.3


async def test_detached_cleanup_consumes_post_cancellation_failure_and_drains_tracking(
    caplog: pytest.LogCaptureFixture,
) -> None:
    request = _request(timeout_seconds=0.1)
    topics = SetupTopics.for_id(request.setup_id)
    mqtt = _successful_mqtt(request)
    cleanup_actions = [
        ("publish", topics.discovery_probe, "", "True", "1"),
        ("publish", topics.retained, "", "True", "1"),
        ("unsubscribe", topics.ha_to_panel),
        ("unsubscribe", topics.retained),
        ("disconnect",),
    ]
    slow_action = cleanup_actions[0]
    raw_error = "post-cancellation password=must-not-leak"
    mqtt.blocked.add(slow_action)
    cancellation_started = asyncio.Event()
    release_cancellation = asyncio.Event()
    mqtt.cancellation_pauses[slow_action] = (cancellation_started, release_cancellation)
    mqtt.post_cancellation_failures[slow_action] = RuntimeError(raw_error)
    loop = asyncio.get_running_loop()
    loop_contexts: list[dict[str, Any]] = []
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: loop_contexts.append(context))

    try:
        preflight_task = asyncio.create_task(
            async_run_preflight(
                _settings(),
                request,
                mqtt_factory=lambda *args, **kw: mqtt,
            )
        )
        await asyncio.wait_for(cancellation_started.wait(), timeout=0.2)
        assert len(preflight_module._detached_cleanup_tasks) == 1
        assert next(iter(preflight_module._detached_cleanup_tasks)).done() is False
        report = await asyncio.wait_for(preflight_task, timeout=0.2)
        detached_cleanup_finished = asyncio.Event()
        next(iter(preflight_module._detached_cleanup_tasks)).add_done_callback(
            lambda _task: detached_cleanup_finished.set()
        )
        release_cancellation.set()
        await asyncio.wait_for(detached_cleanup_finished.wait(), timeout=0.2)
    finally:
        release_cancellation.set()
        loop.set_exception_handler(previous_handler)

    assert report.success is False
    assert report.failed_stage is PreflightStage.CLEANUP
    assert report.error_code == "mqtt_timeout"
    assert report.detail == "MQTT stage timed out"
    assert [event for event in mqtt.events if event in cleanup_actions] == cleanup_actions
    assert mqtt.post_cancellation_failures_raised == [slow_action]
    assert preflight_module._detached_cleanup_tasks == set()
    assert loop_contexts == []
    assert raw_error not in report.to_json()
    assert raw_error not in caplog.text


async def test_ordinary_failure_still_clears_both_probes_unsubscribes_and_disconnects() -> None:
    request = _request(timeout_seconds=0.01)
    topics = SetupTopics.for_id(request.setup_id)
    mqtt = _successful_mqtt(request)

    async def malformed_response(topic: str, payload: str, retain: bool, qos: int) -> None:
        if topic == topics.panel_to_ha:
            await mqtt.inject(topics.ha_to_panel, "credential=must-not-leak")

    mqtt.publish_hook = malformed_response

    report = await async_run_preflight(_settings(), request, mqtt_factory=lambda *args, **kw: mqtt)

    assert report.error_code == "mqtt_timeout"
    assert [item[:3] for item in mqtt.events if item[0] == "publish" and item[2] == ""] == [
        ("publish", topics.discovery_probe, ""),
        ("publish", topics.retained, ""),
    ]
    assert mqtt.unsubscriptions == [topics.ha_to_panel]
    assert mqtt.events[-1] == ("disconnect",)


async def test_partial_cleanup_failures_do_not_skip_later_cleanup_actions() -> None:
    request = _request()
    topics = SetupTopics.for_id(request.setup_id)
    mqtt = _successful_mqtt(request)
    mqtt.failures[("publish", topics.discovery_probe, "", "True", "1")] = RuntimeError(
        "first cleanup raw secret"
    )
    mqtt.failures[("unsubscribe", topics.ha_to_panel)] = RuntimeError("second raw secret")

    report = await async_run_preflight(_settings(), request, mqtt_factory=lambda *args, **kw: mqtt)

    assert report.success is False
    assert report.failed_stage is PreflightStage.CLEANUP
    assert report.error_code == "cleanup_failed"
    assert report.detail == "MQTT cleanup failed"
    assert ("publish", topics.retained, "", "True", "1") in mqtt.events
    assert ("unsubscribe", topics.retained) in mqtt.events
    assert mqtt.events[-1] == ("disconnect",)


async def test_internal_cleanup_action_cancellation_is_redacted_and_does_not_skip_actions(
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    request = _request()
    topics = SetupTopics.for_id(request.setup_id)
    mqtt = _successful_mqtt(request)
    raw_error = "cleanup-child-cancelled password=must-not-leak"
    cleanup_actions = [
        ("publish", topics.discovery_probe, "", "True", "1"),
        ("publish", topics.retained, "", "True", "1"),
        ("unsubscribe", topics.ha_to_panel),
        ("unsubscribe", topics.retained),
        ("disconnect",),
    ]
    mqtt.failures[cleanup_actions[0]] = asyncio.CancelledError(raw_error)

    report = await async_run_preflight(
        _settings(),
        request,
        mqtt_factory=lambda *args, **kw: mqtt,
    )

    async def runner(received: Settings, received_request: PreflightRequest) -> PreflightReport:
        assert received == _settings()
        assert received_request == request
        return report

    exit_code = await preflight_module.async_main(
        ["--request-json", _canonical(REQUEST_OBJECT)],
        settings_factory=_settings,
        runner=runner,
    )
    captured = capsys.readouterr()

    assert report.success is False
    assert report.failed_stage is PreflightStage.CLEANUP
    assert report.error_code == "cleanup_failed"
    assert report.detail == "MQTT cleanup failed"
    assert [event for event in mqtt.events if event in cleanup_actions] == cleanup_actions
    assert exit_code == 1
    assert captured.out == f"{report.to_json()}\n"
    assert captured.err == ""
    assert raw_error not in report.to_json()
    assert raw_error not in captured.out
    assert raw_error not in captured.err
    assert raw_error not in caplog.text


async def test_internal_cleanup_action_cancellation_preserves_primary_timeout() -> None:
    request = _request(timeout_seconds=0.01)
    topics = SetupTopics.for_id(request.setup_id)
    mqtt = _successful_mqtt(request)
    cleanup_actions = [
        ("publish", topics.discovery_probe, "", "True", "1"),
        ("publish", topics.retained, "", "True", "1"),
        ("unsubscribe", topics.ha_to_panel),
        ("disconnect",),
    ]

    async def malformed_response(topic: str, payload: str, retain: bool, qos: int) -> None:
        if topic == topics.panel_to_ha:
            await mqtt.inject(topics.ha_to_panel, "not-json")

    mqtt.publish_hook = malformed_response
    mqtt.failures[cleanup_actions[0]] = asyncio.CancelledError("cleanup-child-secret")

    report = await async_run_preflight(
        _settings(),
        request,
        mqtt_factory=lambda *args, **kw: mqtt,
    )

    assert report.success is False
    assert report.failed_stage is PreflightStage.HA_TO_PANEL
    assert report.error_code == "mqtt_timeout"
    assert report.detail == "MQTT stage timed out"
    assert [event for event in mqtt.events if event in cleanup_actions] == cleanup_actions


async def test_cleanup_action_timeout_error_maps_to_mqtt_timeout(
    caplog: pytest.LogCaptureFixture,
) -> None:
    request = _request()
    topics = SetupTopics.for_id(request.setup_id)
    mqtt = _successful_mqtt(request)
    raw_error = "cleanup timeout password=must-not-leak"
    mqtt.failures[("publish", topics.discovery_probe, "", "True", "1")] = asyncio.TimeoutError(
        raw_error
    )

    report = await async_run_preflight(
        _settings(),
        request,
        mqtt_factory=lambda *args, **kw: mqtt,
    )

    assert report.success is False
    assert report.failed_stage is PreflightStage.CLEANUP
    assert report.error_code == "mqtt_timeout"
    assert report.detail == "MQTT stage timed out"
    assert ("publish", topics.retained, "", "True", "1") in mqtt.events
    assert ("unsubscribe", topics.ha_to_panel) in mqtt.events
    assert ("unsubscribe", topics.retained) in mqtt.events
    assert mqtt.events[-1] == ("disconnect",)
    assert raw_error not in report.to_json()
    assert raw_error not in caplog.text


async def test_first_cancellation_during_cleanup_waits_for_every_action() -> None:
    request = _request(timeout_seconds=0.5)
    topics = SetupTopics.for_id(request.setup_id)
    mqtt = _successful_mqtt(request)
    cleanup_entered = asyncio.Event()
    release_cleanup = asyncio.Event()
    mqtt.pauses[("publish", topics.discovery_probe, "", "True", "1")] = (
        cleanup_entered,
        release_cleanup,
    )
    task = asyncio.create_task(
        async_run_preflight(_settings(), request, mqtt_factory=lambda *args, **kw: mqtt)
    )
    await asyncio.wait_for(cleanup_entered.wait(), timeout=0.2)

    task.cancel()
    await asyncio.sleep(0)

    assert task.done() is False
    release_cleanup.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=0.5)

    assert [event for event in mqtt.events if event[0] == "publish" and event[2] == ""] == [
        ("publish", topics.discovery_probe, "", "True", "1"),
        ("publish", topics.retained, "", "True", "1"),
    ]
    assert mqtt.unsubscriptions == [topics.ha_to_panel, topics.retained]
    assert mqtt.events[-1] == ("disconnect",)


async def test_repeated_cancellation_during_cleanup_waits_for_every_action() -> None:
    request = _request(timeout_seconds=0.5)
    topics = SetupTopics.for_id(request.setup_id)
    mqtt = _successful_mqtt(request)
    subscribe_entered = asyncio.Event()
    never_release_suback = asyncio.Event()
    cleanup_entered = asyncio.Event()
    release_cleanup = asyncio.Event()
    mqtt.pauses[("subscribe", topics.retained)] = (
        subscribe_entered,
        never_release_suback,
    )
    mqtt.pauses[("publish", topics.discovery_probe, "", "True", "1")] = (
        cleanup_entered,
        release_cleanup,
    )
    task = asyncio.create_task(
        async_run_preflight(_settings(), request, mqtt_factory=lambda *args, **kw: mqtt)
    )
    await asyncio.wait_for(subscribe_entered.wait(), timeout=0.2)

    task.cancel()
    await asyncio.wait_for(cleanup_entered.wait(), timeout=0.2)
    task.cancel()
    await asyncio.sleep(0)

    assert task.done() is False
    release_cleanup.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=0.5)

    assert [event for event in mqtt.events if event[0] == "publish" and event[2] == ""] == [
        ("publish", topics.discovery_probe, "", "True", "1"),
        ("publish", topics.retained, "", "True", "1"),
    ]
    assert mqtt.unsubscriptions == [topics.ha_to_panel, topics.retained]
    assert mqtt.events[-1] == ("disconnect",)


@pytest.mark.parametrize("subscription_name", ["ha_to_panel", "retained"])
async def test_broker_applied_subscription_is_cleaned_after_suback_timeout(
    subscription_name: str,
) -> None:
    request = _request(timeout_seconds=0.02)
    topics = SetupTopics.for_id(request.setup_id)
    mqtt = _successful_mqtt(request)
    topic = getattr(topics, subscription_name)
    subscribe_entered = asyncio.Event()
    never_deliver_suback = asyncio.Event()

    async def broker_applies_before_suback(subscribed_topic: str) -> None:
        if subscribed_topic == topic:
            assert subscribed_topic in mqtt.subscriptions
            subscribe_entered.set()
            await never_deliver_suback.wait()

    mqtt.subscribe_hook = broker_applies_before_suback

    report = await asyncio.wait_for(
        async_run_preflight(_settings(), request, mqtt_factory=lambda *args, **kw: mqtt),
        timeout=0.3,
    )

    assert subscribe_entered.is_set()
    assert report.success is False
    assert report.error_code == "mqtt_timeout"
    assert ("unsubscribe", topic) in mqtt.events


async def test_cancellation_propagates_after_independent_bounded_cleanup() -> None:
    request = _request(timeout_seconds=0.05)
    topics = SetupTopics.for_id(request.setup_id)
    mqtt = _successful_mqtt(request)
    request_published = asyncio.Event()

    async def no_response(topic: str, payload: str, retain: bool, qos: int) -> None:
        if topic == topics.panel_to_ha:
            request_published.set()

    mqtt.publish_hook = no_response
    mqtt.failures[("publish", topics.discovery_probe, "", "True", "1")] = RuntimeError(
        "cleanup failure during cancellation"
    )
    task = asyncio.create_task(
        async_run_preflight(_settings(), request, mqtt_factory=lambda *args, **kw: mqtt)
    )
    await asyncio.wait_for(request_published.wait(), timeout=0.1)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=0.2)

    assert ("publish", topics.discovery_probe, "", "True", "1") in mqtt.events
    assert ("publish", topics.retained, "", "True", "1") in mqtt.events
    assert ("unsubscribe", topics.ha_to_panel) in mqtt.events
    assert mqtt.events[-1] == ("disconnect",)


async def test_raw_settings_ca_and_broker_exception_text_are_redacted(
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = _settings()
    request = _request()
    mqtt = _successful_mqtt(request)
    raw_values = (
        settings.mqtt_host,
        settings.mqtt_username,
        settings.mqtt_password,
        str(settings.mqtt_tls_ca_file),
        "-----BEGIN CERTIFICATE-----private-ca-body",
    )
    mqtt.failures[("connect",)] = RuntimeError(" ".join(raw_values))

    report = await async_run_preflight(settings, request, mqtt_factory=lambda *args, **kw: mqtt)
    serialized = report.to_json()

    assert report.detail == "MQTT connection failed"
    for raw in raw_values:
        assert raw not in serialized
        assert raw not in caplog.text


async def test_real_adapter_close_failure_maps_to_redacted_cleanup_failure(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = _settings()
    request = _request()
    raw_close_error = "raw-close-error password-do-not-leak panel-nonce"
    client = _ConcretePreflightClient(request, RuntimeError(raw_close_error))
    modes: list[tuple[bool, bool]] = []
    monkeypatch.setattr(aiomqtt, "Client", lambda **kwargs: client)
    monkeypatch.setattr(mqttio, "build_tls_context", lambda received: None)
    caplog.set_level(logging.DEBUG, logger=mqttio.__name__)

    def factory(
        settings: Settings,
        *,
        identifier: str,
        publish_availability: bool,
        checked_disconnect: bool = False,
        redacted_logging: bool = False,
    ) -> mqttio.AioMqttAdapter:
        modes.append((checked_disconnect, redacted_logging))
        return mqttio.AioMqttAdapter(
            settings,
            identifier=identifier,
            publish_availability=publish_availability,
            checked_disconnect=checked_disconnect,
            redacted_logging=redacted_logging,
        )

    report = await async_run_preflight(settings, request, mqtt_factory=factory)

    assert report.success is False
    assert report.failed_stage is PreflightStage.CLEANUP
    assert report.error_code == "cleanup_failed"
    assert report.detail == "MQTT cleanup failed"
    assert modes == [(True, True)]
    assert client.exit_attempts == 1
    assert client.unsubscriptions == [
        SetupTopics.for_id(request.setup_id).ha_to_panel,
        SetupTopics.for_id(request.setup_id).retained,
    ]
    serialized = report.to_json()
    for raw in (
        settings.mqtt_host,
        settings.mqtt_username,
        settings.mqtt_password,
        str(settings.mqtt_tls_ca_file),
        request.panel_nonce,
        request.ha_nonce,
        raw_close_error,
    ):
        assert raw not in serialized
        assert raw not in caplog.text


async def test_real_adapter_internal_close_cancellation_maps_to_stable_cleanup_failure(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = _settings()
    request = _request()
    raw_close_error = "raw-cancelled-close password-do-not-leak panel-nonce"
    client = _ConcretePreflightClient(request, asyncio.CancelledError(raw_close_error))
    monkeypatch.setattr(aiomqtt, "Client", lambda **kwargs: client)
    monkeypatch.setattr(mqttio, "build_tls_context", lambda received: None)
    monkeypatch.setattr(preflight_module, "_monotonic_ms", iter(range(12)).__next__)
    caplog.set_level(logging.DEBUG, logger=mqttio.__name__)

    report = await async_run_preflight(settings, request)

    assert client.exit_attempts == 1
    assert report.to_json() == _canonical(
        {
            "completed_stages": [stage.value for stage in tuple(PreflightStage)[:-1]],
            "detail": "MQTT cleanup failed",
            "error_code": "cleanup_failed",
            "failed_stage": "cleanup",
            "schema_version": 1,
            "setup_id": SETUP_ID,
            "stage_elapsed_ms": {stage.value: 1 for stage in PreflightStage},
            "success": False,
        }
    )
    assert raw_close_error not in report.to_json()
    assert raw_close_error not in caplog.text


async def test_cli_prints_exactly_one_canonical_json_object_and_maps_failure_to_exit_one(
    capsys: pytest.CaptureFixture[str],
) -> None:
    settings = _settings()
    request = _request()
    success_report = await async_run_preflight(
        settings,
        request,
        mqtt_factory=lambda *args, **kw: _successful_mqtt(request),
    )
    failed_mqtt = _successful_mqtt(request)
    failed_mqtt.failures[("connect",)] = RuntimeError("secret broker error")
    failed_report = await async_run_preflight(
        settings,
        request,
        mqtt_factory=lambda *args, **kw: failed_mqtt,
    )

    async def success_runner(
        received_settings: Settings, received: PreflightRequest
    ) -> PreflightReport:
        assert received_settings == settings
        assert received == request
        return success_report

    code = await preflight_module.async_main(
        ["--request-json", _canonical(REQUEST_OBJECT)],
        settings_factory=lambda: settings,
        runner=success_runner,
    )
    captured = capsys.readouterr()
    assert code == 0
    assert captured.out == f"{success_report.to_json()}\n"
    assert captured.err == ""
    assert captured.out.count("\n") == 1

    async def failure_runner(
        received_settings: Settings, received: PreflightRequest
    ) -> PreflightReport:
        return failed_report

    code = await preflight_module.async_main(
        ["--request-json", _canonical(REQUEST_OBJECT)],
        settings_factory=lambda: settings,
        runner=failure_runner,
    )
    captured = capsys.readouterr()
    assert code == 1
    assert captured.out == f"{failed_report.to_json()}\n"
    assert captured.err == ""


async def test_cli_valid_request_maps_settings_failure_to_one_redacted_json_line(
    capsys: pytest.CaptureFixture[str],
) -> None:
    raw_secret = "settings-validation-secret-do-not-leak"

    def fail_settings() -> Settings:
        raise ValueError(raw_secret)

    async def runner(received: Settings, request: PreflightRequest) -> PreflightReport:
        raise AssertionError("runner must not be called when settings are invalid")

    code = await preflight_module.async_main(
        ["--request-json", _canonical(REQUEST_OBJECT)],
        settings_factory=fail_settings,
        runner=runner,
    )

    captured = capsys.readouterr()
    assert code == 1
    assert (
        captured.out
        == _canonical(
            {
                "completed_stages": ["cleanup"],
                "detail": "Panel configuration invalid",
                "error_code": "settings_invalid",
                "failed_stage": "fleet_auth",
                "schema_version": 1,
                "setup_id": SETUP_ID,
                "stage_elapsed_ms": {
                    "cleanup": 0,
                    "fleet_auth": 0,
                },
                "success": False,
            }
        )
        + "\n"
    )
    assert captured.err == ""
    assert raw_secret not in captured.out
    assert raw_secret not in captured.err
    assert PreflightReport.from_json(captured.out[:-1]).completed_stages == (
        PreflightStage.CLEANUP,
    )


async def test_cli_malformed_provided_request_maps_to_one_redacted_json_line(
    capsys: pytest.CaptureFixture[str],
) -> None:
    raw_secret = "malformed-request-secret-do-not-echo"

    code = await preflight_module.async_main(
        ["--request-json", "{" + raw_secret],
        settings_factory=lambda: _settings(),
    )

    captured = capsys.readouterr()
    assert code == 1
    assert (
        captured.out
        == _canonical(
            {
                "completed_stages": [],
                "detail": "MQTT payload validation failed",
                "error_code": "mqtt_payload",
                "failed_stage": "fleet_auth",
                "schema_version": 1,
                "setup_id": None,
                "stage_elapsed_ms": {},
                "success": False,
            }
        )
        + "\n"
    )
    assert captured.err == ""
    assert raw_secret not in captured.out
    assert raw_secret not in captured.err


def test_cli_subprocess_redacts_invalid_environment_value() -> None:
    raw_secret = "invalid-port-secret-do-not-leak"
    source_root = Path(__file__).resolve().parents[1] / "src"
    environment = {
        "PATH": os.environ.get("PATH", ""),
        "PYTHONPATH": str(source_root),
        "BRILLIANT_PANEL": "office",
        "MQTT_HOST": "broker.internal.example",
        "MQTT_USERNAME": "preflight-user",
        "MQTT_PASSWORD": "password-do-not-leak",
        "MQTT_PORT": raw_secret,
    }

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "brilliant_mqtt.preflight",
            "--request-json",
            _canonical(REQUEST_OBJECT),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=5.0,
        env=environment,
    )

    assert completed.returncode == 1
    assert completed.stdout.count("\n") == 1
    assert completed.stdout == _canonical(json.loads(completed.stdout)) + "\n"
    assert json.loads(completed.stdout) == {
        "completed_stages": ["cleanup"],
        "detail": "Panel configuration invalid",
        "error_code": "settings_invalid",
        "failed_stage": "fleet_auth",
        "schema_version": 1,
        "setup_id": SETUP_ID,
        "stage_elapsed_ms": {
            "cleanup": 0,
            "fleet_auth": 0,
        },
        "success": False,
    }
    assert completed.stderr == ""
    assert raw_secret not in completed.stdout
    assert raw_secret not in completed.stderr


def test_environment_file_loader_treats_shell_syntax_as_literal(
    tmp_path: Path,
) -> None:
    command_substitution = tmp_path / "command-substitution-ran"
    backtick = tmp_path / "backtick-ran"
    password = f"$(touch {command_substitution})`touch {backtick}`$HOME"
    environment_file = tmp_path / "brilliant-mqtt.env"
    environment_file.write_text(
        "\n".join(
            (
                'BRILLIANT_PANEL="office"',
                'MQTT_HOST="broker.internal.example"',
                "MQTT_PORT=8883",
                'MQTT_USERNAME="preflight-user"',
                f'MQTT_PASSWORD="{password}"',
                "MQTT_TLS_ENABLED=0",
                "RETAINED_TOPICS_FILE=/var/brilliant-mqtt/state/owned-topics.json",
                "MESH_PRIORITY=2",
                "SCENE_BRIDGE_ENABLED=0",
                "LOG_LEVEL=INFO",
                "",
            )
        ),
        encoding="utf-8",
    )

    settings = preflight_module._settings_from_environment_file(str(environment_file))

    assert settings.mqtt_password == password
    assert settings.mqtt_username == "preflight-user"
    assert not command_substitution.exists()
    assert not backtick.exists()


@pytest.mark.parametrize(
    "separator",
    ["\v", "\f", "\x85", "\u2028", "\u2029"],
    ids=["vertical-tab", "form-feed", "next-line", "line-separator", "paragraph-separator"],
)
def test_environment_file_loader_preserves_non_lf_password_characters(
    tmp_path: Path,
    separator: str,
) -> None:
    password = f"before{separator}after"
    environment_file = tmp_path / "brilliant-mqtt.env"
    environment_file.write_text(
        "\n".join(
            (
                'BRILLIANT_PANEL="office"',
                'MQTT_HOST="broker.internal.example"',
                'MQTT_USERNAME="preflight-user"',
                f'MQTT_PASSWORD="{password}"',
                "",
            )
        ),
        encoding="utf-8",
    )

    settings = preflight_module._settings_from_environment_file(str(environment_file))

    assert settings.mqtt_password == password


@pytest.mark.parametrize(
    "invalid_character",
    [
        "\ufeff",
        "\ufdd0",
        "\ufdef",
        "\ufffe",
        "\uffff",
        "\U0001fffe",
        "\U0010ffff",
    ],
)
def test_environment_file_loader_rejects_systemd_invalid_unicode(
    tmp_path: Path,
    invalid_character: str,
) -> None:
    environment_file = tmp_path / "brilliant-mqtt.env"
    environment_file.write_text(
        "\n".join(
            (
                'BRILLIANT_PANEL="office"',
                'MQTT_HOST="broker.internal.example"',
                'MQTT_USERNAME="preflight-user"',
                f'MQTT_PASSWORD="before{invalid_character}after"',
                "",
            )
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="invalid_preflight_environment"):
        preflight_module._settings_from_environment_file(str(environment_file))


@pytest.mark.parametrize(
    "line",
    [
        'LD_PRELOAD="/tmp/attacker.so"',
        "MQTT_PASSWORD=$(touch /tmp/SHOULD_NOT_EXIST)",
        'MQTT_PASSWORD="unterminated',
        'MQTT_PASSWORD="bad\\qescape"',
        'MQTT_PASSWORD="premature"quote"',
    ],
)
def test_environment_file_loader_rejects_noncanonical_input(
    tmp_path: Path,
    line: str,
) -> None:
    environment_file = tmp_path / "brilliant-mqtt.env"
    environment_file.write_text(
        "\n".join(
            (
                'BRILLIANT_PANEL="office"',
                'MQTT_HOST="broker.internal.example"',
                'MQTT_USERNAME="preflight-user"',
                line,
                "",
            )
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="invalid_preflight_environment"):
        preflight_module._settings_from_environment_file(str(environment_file))


async def test_cli_environment_file_supplies_literal_settings(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    environment_file = tmp_path / "brilliant-mqtt.env"
    password = "literal-$USER-`id`-$(id)"
    environment_file.write_text(
        "\n".join(
            (
                'BRILLIANT_PANEL="office"',
                'MQTT_HOST="broker.internal.example"',
                'MQTT_USERNAME="preflight-user"',
                f'MQTT_PASSWORD="{password}"',
                "",
            )
        ),
        encoding="utf-8",
    )
    expected = PreflightReport(
        setup_id=UUID(SETUP_ID),
        success=True,
        completed_stages=tuple(PreflightStage),
        stage_elapsed_ms={stage: 0 for stage in PreflightStage},
        last_stage=PreflightStage.CLEANUP,
    )

    async def runner(
        settings: Settings,
        request: PreflightRequest,
    ) -> PreflightReport:
        assert settings.mqtt_password == password
        assert request.setup_id == UUID(SETUP_ID)
        return expected

    code = await preflight_module.async_main(
        [
            "--environment-file",
            str(environment_file),
            "--request-json",
            _canonical(REQUEST_OBJECT),
        ],
        runner=runner,
    )

    captured = capsys.readouterr()
    assert code == 0
    assert captured.out == f"{expected.to_json()}\n"
    assert captured.err == ""


def test_cli_help_explains_required_argument_and_outer_process_deadline(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as raised:
        preflight_module.main(["--help"])

    captured = capsys.readouterr()
    assert raised.value.code == 0
    assert "--request-json" in captured.out
    assert "outer process deadline" in captured.out
    assert captured.err == ""


def test_cli_missing_required_argument_keeps_argparse_system_exit(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as raised:
        preflight_module.main([])

    captured = capsys.readouterr()
    assert raised.value.code == 2
    assert captured.out == ""
    assert "--request-json" in captured.err


def test_importing_preflight_does_not_import_or_reference_panel_bus() -> None:
    source_path = Path(preflight_module.__file__ or "")
    source = source_path.read_text(encoding="utf-8")
    assert "lib.message_bus_api" not in source
    assert "brilliant_mqtt.bus" not in source

    environment = dict(os.environ)
    source_root = Path(__file__).resolve().parents[1] / "src"
    environment["PYTHONPATH"] = str(source_root)
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import brilliant_mqtt.preflight; "
            "assert 'brilliant_mqtt.bus' not in sys.modules; "
            "assert 'lib.message_bus_api' not in sys.modules",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=2.0,
        env=environment,
    )
    assert completed.returncode == 0, completed.stderr
