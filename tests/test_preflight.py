"""Panel-side MQTT preflight state-machine and CLI tests."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import sys
from collections.abc import Awaitable, Callable, Iterator
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
        self.pauses: dict[tuple[str, ...], tuple[asyncio.Event, asyncio.Event]] = {}
        self.publish_hook: PublishHook | None = None
        self.subscribe_hook: SubscribeHook | None = None

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
            finally:
                delay = self.cancellation_delays.get(key)
                if delay is not None:
                    await asyncio.sleep(delay)
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

    def feed(self, topic: str, payload: str, *, retained: bool) -> None:
        self._messages.put_nowait(
            aiomqtt.Message(
                topic=topic,
                payload=payload.encode(),
                qos=1,
                retain=retained,
                mid=1,
                properties=None,
            )
        )


class _ConcretePreflightClient:
    """Broker seam used while exercising the real AioMqttAdapter lifecycle."""

    def __init__(self, request: PreflightRequest, close_error: BaseException) -> None:
        self._request = request
        self._topics = SetupTopics.for_id(request.setup_id)
        self._close_error = close_error
        self._retained_payload: str | None = None
        self.messages = _ConcreteMessageStream()
        self.exit_attempts = 0
        self.unsubscriptions: list[str] = []

    async def __aenter__(self) -> _ConcretePreflightClient:
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.exit_attempts += 1
        raise self._close_error

    async def publish(
        self,
        topic: str,
        payload: str,
        retain: bool = False,
        qos: int = 0,
    ) -> None:
        if topic == self._topics.panel_to_ha:
            self.messages.feed(
                self._topics.ha_to_panel,
                SetupResult(
                    setup_id=self._request.setup_id,
                    nonce=self._request.ha_nonce,
                    reply_to_nonce=self._request.panel_nonce,
                ).to_payload(),
                retained=False,
            )
        if topic == self._topics.retained and retain:
            self._retained_payload = payload

    async def subscribe(self, topic: str) -> None:
        if topic == self._topics.retained and self._retained_payload:
            self.messages.feed(topic, self._retained_payload, retained=True)

    async def unsubscribe(self, topic: str) -> None:
        self.unsubscriptions.append(topic)


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
    request = _request()
    topics = SetupTopics.for_id(request.setup_id)
    mqtt = _successful_mqtt(request)

    async def publish_hook(topic: str, sent: str, retain: bool, qos: int) -> None:
        if topic == topics.panel_to_ha:
            await mqtt.inject(topics.ha_to_panel, payload)

    mqtt.publish_hook = publish_hook

    report = await async_run_preflight(_settings(), request, mqtt_factory=lambda *args, **kw: mqtt)

    assert report.success is False
    assert report.failed_stage is PreflightStage.HA_TO_PANEL
    assert report.error_code == "mqtt_payload"
    assert report.detail == "MQTT payload validation failed"


@pytest.mark.parametrize(
    ("payload", "retained", "error_code"),
    [
        (PANEL_NONCE, False, "retained_flag_missing"),
        ("wrong-retained-payload", True, "mqtt_payload"),
    ],
)
async def test_retained_replay_requires_exact_payload_and_retained_flag(
    payload: str,
    retained: bool,
    error_code: str,
) -> None:
    request = _request()
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


async def test_protocol_stages_use_request_timeout_and_cleanup_owns_its_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(timeout_seconds=0.01)
    mqtt = _successful_mqtt(request)
    topics = SetupTopics.for_id(request.setup_id)
    timeouts: list[float] = []
    real_wait_for = asyncio.wait_for

    async def record_wait_for(awaitable: Awaitable[Any], timeout: float) -> Any:
        timeouts.append(timeout)
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
    assert timeouts == [0.01, 0.01, 0.01]
    assert ("unsubscribe", topics.ha_to_panel) in mqtt.events
    assert ("disconnect",) in mqtt.events


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


async def test_ordinary_failure_still_clears_both_probes_unsubscribes_and_disconnects() -> None:
    request = _request()
    topics = SetupTopics.for_id(request.setup_id)
    mqtt = _successful_mqtt(request)

    async def malformed_response(topic: str, payload: str, retain: bool, qos: int) -> None:
        if topic == topics.panel_to_ha:
            await mqtt.inject(topics.ha_to_panel, "credential=must-not-leak")

    mqtt.publish_hook = malformed_response

    report = await async_run_preflight(_settings(), request, mqtt_factory=lambda *args, **kw: mqtt)

    assert report.error_code == "mqtt_payload"
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
                "completed_stages": [],
                "detail": "MQTT connection failed",
                "error_code": "mqtt_connect",
                "failed_stage": "fleet_auth",
                "schema_version": 1,
                "setup_id": SETUP_ID,
                "stage_elapsed_ms": {},
                "success": False,
            }
        )
        + "\n"
    )
    assert captured.err == ""
    assert raw_secret not in captured.out
    assert raw_secret not in captured.err


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
        "completed_stages": [],
        "detail": "MQTT connection failed",
        "error_code": "mqtt_connect",
        "failed_stage": "fleet_auth",
        "schema_version": 1,
        "setup_id": SETUP_ID,
        "stage_elapsed_ms": {},
        "success": False,
    }
    assert completed.stderr == ""
    assert raw_secret not in completed.stdout
    assert raw_secret not in completed.stderr


def test_cli_help_exits_zero_and_lists_required_request_argument(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as raised:
        preflight_module.main(["--help"])

    captured = capsys.readouterr()
    assert raised.value.code == 0
    assert "--request-json" in captured.out
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
