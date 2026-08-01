"""Unit tests for the panel-side MQTT onboarding preflight state machine.

Covers PreflightRequest/PreflightReport validation and serialization, plus a
staged end-to-end run against a scripted fake `_PreflightMqtt` factory: the
happy path, per-stage failure mapping, malformed/missing inbound handling,
and — the regression test for the review's MAJOR finding — proof that a
stage's timed-out MQTT operation is fully settled before `_cleanup` ever
touches the shared client again.
"""

from __future__ import annotations

import asyncio
import json
import math
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from uuid import uuid4

import pytest

from brilliant_mqtt.config import Settings
from brilliant_mqtt.mqttio import MqttPayloadDecodeError
from brilliant_mqtt.preflight import (
    PreflightReport,
    PreflightRequest,
    PreflightStage,
    async_run_preflight,
)
from brilliant_mqtt.setup_protocol import SCHEMA_VERSION, SetupResult, SetupTopics


def _settings() -> Settings:
    return Settings(
        panel="office", mqtt_host="broker.invalid", mqtt_username="u", mqtt_password="p"
    )


def _request(timeout_seconds: float = 1.0) -> PreflightRequest:
    return PreflightRequest(
        setup_id=uuid4(),
        panel_nonce="panel-nonce",
        ha_nonce="ha-nonce",
        timeout_seconds=timeout_seconds,
    )


async def _wait_until(predicate: Callable[[], bool], timeout: float = 2.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("condition was not met in time")
        await asyncio.sleep(0.005)


@dataclass
class _ScriptedPreflightMqtt:
    """A fully scripted `_PreflightMqtt` for driving async_run_preflight.

    Subscribing to `auto_respond` topics schedules a background delivery of
    the configured response through the registered `on_message` callback —
    this lets most tests just configure responses up front and `await`
    `async_run_preflight` directly, with no manual interleaving. The two
    tests that need precise control (the timeout/overlap regression and the
    fail-fast-on-malformed-inbound test) use `hang_subscribe_topic` instead,
    which blocks until the test explicitly releases it.
    """

    connect_error: BaseException | None = None
    publish_errors: dict[str, BaseException] = field(default_factory=dict)
    subscribe_errors: dict[str, BaseException] = field(default_factory=dict)
    unsubscribe_error: BaseException | None = None
    disconnect_error: BaseException | None = None

    # topic -> (payload, retained) delivered shortly after that topic is subscribed.
    auto_respond: dict[str, tuple[str, bool]] = field(default_factory=dict)

    # A subscribe()/publish() call for this topic hangs (simulating "still
    # unwinding inside aiomqtt") until the test calls release_hang().
    # One-shot: consumed on first use so a later, unrelated call to the same
    # topic (e.g. _cleanup re-publishing an empty clear to the same topic)
    # behaves normally instead of hanging a second time.
    hang_subscribe_topic: str | None = None
    hang_publish_topic: str | None = None
    hang_consumed: bool = False
    hang_active: bool = False
    _hang_release: asyncio.Event = field(default_factory=asyncio.Event)

    connect_calls: int = 0
    disconnect_calls: int = 0
    published: list[tuple[str, str, bool, int]] = field(default_factory=list)
    subscribed: list[str] = field(default_factory=list)
    unsubscribed: list[str] = field(default_factory=list)

    # True if publish/unsubscribe/disconnect (i.e. _cleanup's operations) ever
    # ran while the hung subscribe call was still active/unsettled.
    cleanup_saw_active_hang: bool = False

    _message_cb: Callable[[str, str, bool], Awaitable[None]] | None = None
    _decode_error_cb: Callable[[MqttPayloadDecodeError], Awaitable[None]] | None = None

    async def connect(self) -> None:
        self.connect_calls += 1
        if self.connect_error is not None:
            raise self.connect_error

    async def disconnect(self) -> None:
        self._note_cleanup_touch()
        self.disconnect_calls += 1
        if self.disconnect_error is not None:
            raise self.disconnect_error

    async def publish(self, topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        if topic == self.hang_publish_topic and not self.hang_consumed:
            self.hang_consumed = True
            await self._hang()
            return

        self._note_cleanup_touch()
        error = self.publish_errors.pop(topic, None)
        if error is not None:
            raise error
        self.published.append((topic, payload, retain, qos))

    def on_message(self, cb: Callable[[str, str, bool], Awaitable[None]]) -> None:
        self._message_cb = cb

    def on_payload_decode_error(
        self, cb: Callable[[MqttPayloadDecodeError], Awaitable[None]]
    ) -> None:
        self._decode_error_cb = cb

    async def subscribe(self, topic: str) -> None:
        if topic == self.hang_subscribe_topic and not self.hang_consumed:
            self.hang_consumed = True
            await self._hang()
            return

        self._note_cleanup_touch()
        self.subscribed.append(topic)
        error = self.subscribe_errors.pop(topic, None)
        if error is not None:
            raise error
        response = self.auto_respond.get(topic)
        if response is not None:
            payload, retained = response
            asyncio.ensure_future(self._deliver(topic, payload, retained))

    async def _hang(self) -> None:
        """Hang until cancelled, then take real time to unwind (see class docstring)."""
        self.hang_active = True
        try:
            try:
                await asyncio.Event().wait()  # never-set: hangs until cancelled
            except asyncio.CancelledError:
                await asyncio.shield(self._hang_release.wait())
                raise
        finally:
            self.hang_active = False

    async def unsubscribe(self, topic: str) -> None:
        self._note_cleanup_touch()
        self.unsubscribed.append(topic)
        if self.unsubscribe_error is not None:
            raise self.unsubscribe_error

    def release_hang(self) -> None:
        self._hang_release.set()

    async def deliver(self, topic: str, payload: str, *, retained: bool = False) -> None:
        await self._deliver(topic, payload, retained)

    async def deliver_decode_error(self, topic: str, *, retained: bool = False) -> None:
        assert self._decode_error_cb is not None, "on_payload_decode_error never registered"
        await self._decode_error_cb(MqttPayloadDecodeError(topic=topic, retained=retained))

    async def _deliver(self, topic: str, payload: str, retained: bool) -> None:
        assert self._message_cb is not None, "on_message was never registered"
        await self._message_cb(topic, payload, retained)

    def _note_cleanup_touch(self) -> None:
        if self.hang_active:
            self.cleanup_saw_active_hang = True


def _factory(fake: _ScriptedPreflightMqtt) -> Callable[..., _ScriptedPreflightMqtt]:
    def factory(
        settings: Settings,
        *,
        identifier: str,
        publish_availability: bool,
        checked_disconnect: bool,
        redacted_logging: bool,
    ) -> _ScriptedPreflightMqtt:
        return fake

    return factory


def _happy_fake(request: PreflightRequest, topics: SetupTopics) -> _ScriptedPreflightMqtt:
    ha_result = SetupResult(request.setup_id, request.ha_nonce, request.panel_nonce).to_payload()
    return _ScriptedPreflightMqtt(
        auto_respond={
            topics.ha_to_panel: (ha_result, False),
            topics.retained: (request.panel_nonce, True),
        }
    )


# -- PreflightRequest validation -------------------------------------------------


def test_preflight_request_accepts_valid_input() -> None:
    request = _request(timeout_seconds=5.0)
    assert request.timeout_seconds == 5.0


def test_preflight_request_rejects_bool_timeout() -> None:
    with pytest.raises(ValueError, match="timeout_seconds must be positive"):
        PreflightRequest(uuid4(), "p", "h", True)


def test_preflight_request_rejects_nan_timeout() -> None:
    with pytest.raises(ValueError, match="timeout_seconds must be positive"):
        PreflightRequest(uuid4(), "p", "h", math.nan)


def test_preflight_request_rejects_infinite_timeout() -> None:
    with pytest.raises(ValueError, match="timeout_seconds must be positive"):
        PreflightRequest(uuid4(), "p", "h", math.inf)


def test_preflight_request_rejects_zero_timeout() -> None:
    with pytest.raises(ValueError, match="timeout_seconds must be positive"):
        PreflightRequest(uuid4(), "p", "h", 0.0)


def test_preflight_request_rejects_negative_timeout() -> None:
    with pytest.raises(ValueError, match="timeout_seconds must be positive"):
        PreflightRequest(uuid4(), "p", "h", -1.0)


def test_preflight_request_rejects_empty_nonce() -> None:
    with pytest.raises(ValueError, match="invalid setup identity or nonce"):
        PreflightRequest(uuid4(), "", "h", 1.0)


def test_preflight_request_from_json_round_trip() -> None:
    setup_id = uuid4()
    raw = json.dumps(
        {
            "schema_version": SCHEMA_VERSION,
            "setup_id": str(setup_id),
            "panel_nonce": "p",
            "ha_nonce": "h",
            "timeout_seconds": 2.5,
        }
    )

    request = PreflightRequest.from_json(raw)

    assert request == PreflightRequest(setup_id, "p", "h", 2.5)


def test_preflight_request_from_json_rejects_malformed_json() -> None:
    with pytest.raises(ValueError, match="expected a JSON object"):
        PreflightRequest.from_json("{not json")


def test_preflight_request_from_json_rejects_non_dict() -> None:
    with pytest.raises(ValueError, match="expected a JSON object"):
        PreflightRequest.from_json("[1, 2, 3]")


def test_preflight_request_from_json_rejects_wrong_keys() -> None:
    raw = json.dumps(
        {
            "schema_version": SCHEMA_VERSION,
            "setup_id": str(uuid4()),
            "panel_nonce": "p",
            "ha_nonce": "h",
        }
    )
    with pytest.raises(ValueError, match="unexpected or missing keys"):
        PreflightRequest.from_json(raw)


def test_preflight_request_from_json_rejects_wrong_schema_version() -> None:
    raw = json.dumps(
        {
            "schema_version": 2,
            "setup_id": str(uuid4()),
            "panel_nonce": "p",
            "ha_nonce": "h",
            "timeout_seconds": 1.0,
        }
    )
    with pytest.raises(ValueError, match="expected schema_version 1"):
        PreflightRequest.from_json(raw)


def test_preflight_request_from_json_rejects_non_string_setup_id() -> None:
    raw = json.dumps(
        {
            "schema_version": SCHEMA_VERSION,
            "setup_id": 42,
            "panel_nonce": "p",
            "ha_nonce": "h",
            "timeout_seconds": 1.0,
        }
    )
    with pytest.raises(ValueError, match="invalid setup identity or nonce"):
        PreflightRequest.from_json(raw)


def test_preflight_request_from_json_rejects_non_uuid_setup_id() -> None:
    raw = json.dumps(
        {
            "schema_version": SCHEMA_VERSION,
            "setup_id": "not-a-uuid",
            "panel_nonce": "p",
            "ha_nonce": "h",
            "timeout_seconds": 1.0,
        }
    )
    with pytest.raises(ValueError, match="invalid setup identity or nonce"):
        PreflightRequest.from_json(raw)


def test_preflight_request_from_json_rejects_bool_timeout() -> None:
    raw = json.dumps(
        {
            "schema_version": SCHEMA_VERSION,
            "setup_id": str(uuid4()),
            "panel_nonce": "p",
            "ha_nonce": "h",
            "timeout_seconds": True,
        }
    )
    with pytest.raises(ValueError, match="timeout_seconds must be positive"):
        PreflightRequest.from_json(raw)


# -- PreflightReport.to_json shape -----------------------------------------------


def test_preflight_report_success_shape_omits_failure_fields() -> None:
    setup_id = uuid4()
    report = PreflightReport(
        setup_id=setup_id,
        success=True,
        completed_stages=(PreflightStage.FLEET_AUTH, PreflightStage.CLEANUP),
        stage_elapsed_ms={PreflightStage.FLEET_AUTH: 12, PreflightStage.CLEANUP: 3},
        last_stage=PreflightStage.CLEANUP,
    )

    value = json.loads(report.to_json())

    assert value == {
        "schema_version": SCHEMA_VERSION,
        "setup_id": str(setup_id),
        "success": True,
        "completed_stages": ["fleet_auth", "cleanup"],
        "stage_elapsed_ms": {"fleet_auth": 12, "cleanup": 3},
        "last_stage": "cleanup",
    }


def test_preflight_report_failure_shape_omits_last_stage() -> None:
    setup_id = uuid4()
    report = PreflightReport(
        setup_id=setup_id,
        success=False,
        completed_stages=(PreflightStage.FLEET_AUTH,),
        stage_elapsed_ms={PreflightStage.FLEET_AUTH: 5},
        failed_stage=PreflightStage.PANEL_TO_HA,
        error_code="mqtt_timeout",
        detail="MQTT stage timed out",
    )

    value = json.loads(report.to_json())

    assert value == {
        "schema_version": SCHEMA_VERSION,
        "setup_id": str(setup_id),
        "success": False,
        "completed_stages": ["fleet_auth"],
        "stage_elapsed_ms": {"fleet_auth": 5},
        "failed_stage": "panel_to_ha",
        "error_code": "mqtt_timeout",
        "detail": "MQTT stage timed out",
    }


# -- Staged end-to-end run: happy path -------------------------------------------


async def test_all_stages_success() -> None:
    request = _request()
    topics = SetupTopics.for_id(request.setup_id)
    fake = _happy_fake(request, topics)

    report = await async_run_preflight(_settings(), request, _factory(fake))

    assert report.success is True
    assert report.last_stage == PreflightStage.CLEANUP
    assert list(report.completed_stages) == [
        PreflightStage.FLEET_AUTH,
        PreflightStage.PANEL_TO_HA,
        PreflightStage.HA_TO_PANEL,
        PreflightStage.DISCOVERY_WRITE,
        PreflightStage.RETAINED_MESSAGE,
        PreflightStage.CLEANUP,
    ]
    assert fake.connect_calls == 1
    assert fake.disconnect_calls == 1
    assert topics.panel_to_ha in [call[0] for call in fake.published]
    assert topics.discovery_probe in [call[0] for call in fake.published]


async def test_mqtt_factory_receives_preflight_identity() -> None:
    request = _request()
    topics = SetupTopics.for_id(request.setup_id)
    fake = _happy_fake(request, topics)
    seen: list[tuple[str, bool, bool, bool]] = []

    def factory(
        settings: Settings,
        *,
        identifier: str,
        publish_availability: bool,
        checked_disconnect: bool,
        redacted_logging: bool,
    ) -> _ScriptedPreflightMqtt:
        seen.append((identifier, publish_availability, checked_disconnect, redacted_logging))
        return fake

    await async_run_preflight(_settings(), request, factory)

    assert seen == [(f"brilliant-mqtt-setup-{request.setup_id}", False, True, True)]


# -- Per-stage failure mapping ----------------------------------------------------


async def test_connect_failure_maps_to_fleet_auth() -> None:
    request = _request()
    fake = _ScriptedPreflightMqtt(connect_error=RuntimeError("refused"))

    report = await async_run_preflight(_settings(), request, _factory(fake))

    assert report.success is False
    assert report.failed_stage == PreflightStage.FLEET_AUTH
    assert report.error_code == "mqtt_connect"
    # FLEET_AUTH itself never completes, but cleanup still runs (the factory
    # call had already assigned `mqtt` before connect() failed) and succeeds.
    assert report.completed_stages == (PreflightStage.CLEANUP,)
    assert fake.disconnect_calls == 1


async def test_subscribe_failure_maps_to_panel_to_ha() -> None:
    request = _request()
    topics = SetupTopics.for_id(request.setup_id)
    fake = _ScriptedPreflightMqtt(subscribe_errors={topics.ha_to_panel: RuntimeError("boom")})

    report = await async_run_preflight(_settings(), request, _factory(fake))

    assert report.failed_stage == PreflightStage.PANEL_TO_HA
    assert report.error_code == "mqtt_subscribe"


async def test_discovery_publish_failure_maps_to_discovery_write() -> None:
    request = _request()
    topics = SetupTopics.for_id(request.setup_id)
    fake = _happy_fake(request, topics)
    fake.publish_errors[topics.discovery_probe] = RuntimeError("boom")

    report = await async_run_preflight(_settings(), request, _factory(fake))

    assert report.failed_stage == PreflightStage.DISCOVERY_WRITE
    assert report.error_code == "mqtt_publish"


async def test_retained_publish_failure_maps_to_retained_message() -> None:
    request = _request()
    topics = SetupTopics.for_id(request.setup_id)
    fake = _happy_fake(request, topics)
    fake.publish_errors[topics.retained] = RuntimeError("boom")

    report = await async_run_preflight(_settings(), request, _factory(fake))

    assert report.failed_stage == PreflightStage.RETAINED_MESSAGE
    assert report.error_code == "mqtt_publish"


# -- Malformed / missing inbound handling -----------------------------------------


async def test_malformed_ha_response_fails_fast_at_ha_to_panel() -> None:
    request = _request(timeout_seconds=5.0)
    topics = SetupTopics.for_id(request.setup_id)
    fake = _ScriptedPreflightMqtt(hang_subscribe_topic=topics.retained)
    # ha_to_panel subscribes normally; deliver garbage on it once subscribed.
    report_task = asyncio.ensure_future(async_run_preflight(_settings(), request, _factory(fake)))

    await _wait_until(lambda: topics.ha_to_panel in fake.subscribed)
    await fake.deliver(topics.ha_to_panel, "not json", retained=False)

    # Nothing was ever configured to subscribe topics.retained's hang target
    # in this test's actual path (it fails before reaching that stage), so
    # release it defensively to avoid a dangling task at teardown.
    fake.release_hang()
    report = await report_task

    assert report.failed_stage == PreflightStage.HA_TO_PANEL
    assert report.error_code == "mqtt_payload"


async def test_ha_response_wrong_nonce_fails_fast_at_ha_to_panel() -> None:
    request = _request(timeout_seconds=5.0)
    topics = SetupTopics.for_id(request.setup_id)
    fake = _ScriptedPreflightMqtt()
    report_task = asyncio.ensure_future(async_run_preflight(_settings(), request, _factory(fake)))

    await _wait_until(lambda: topics.ha_to_panel in fake.subscribed)
    bad = SetupResult(request.setup_id, "wrong-nonce", request.panel_nonce).to_payload()
    await fake.deliver(topics.ha_to_panel, bad, retained=False)

    report = await report_task

    assert report.failed_stage == PreflightStage.HA_TO_PANEL
    assert report.error_code == "mqtt_payload"


async def test_ha_response_decode_error_fails_fast_at_ha_to_panel() -> None:
    request = _request(timeout_seconds=5.0)
    topics = SetupTopics.for_id(request.setup_id)
    fake = _ScriptedPreflightMqtt()
    report_task = asyncio.ensure_future(async_run_preflight(_settings(), request, _factory(fake)))

    await _wait_until(lambda: topics.ha_to_panel in fake.subscribed)
    await fake.deliver_decode_error(topics.ha_to_panel)

    report = await report_task

    assert report.failed_stage == PreflightStage.HA_TO_PANEL
    assert report.error_code == "mqtt_payload"


async def test_retained_flag_missing_fails_retained_message() -> None:
    request = _request(timeout_seconds=5.0)
    topics = SetupTopics.for_id(request.setup_id)
    ha_result = SetupResult(request.setup_id, request.ha_nonce, request.panel_nonce).to_payload()
    fake = _ScriptedPreflightMqtt(auto_respond={topics.ha_to_panel: (ha_result, False)})
    report_task = asyncio.ensure_future(async_run_preflight(_settings(), request, _factory(fake)))

    await _wait_until(lambda: topics.retained in fake.subscribed)
    # Correct nonce, but delivered as a LIVE (non-retained) message — the
    # retained-flag check must reject it even though the payload matches.
    await fake.deliver(topics.retained, request.panel_nonce, retained=False)

    report = await report_task

    assert report.failed_stage == PreflightStage.RETAINED_MESSAGE
    assert report.error_code == "retained_flag_missing"


async def test_retained_echo_wrong_payload_fails_retained_message() -> None:
    request = _request(timeout_seconds=5.0)
    topics = SetupTopics.for_id(request.setup_id)
    ha_result = SetupResult(request.setup_id, request.ha_nonce, request.panel_nonce).to_payload()
    fake = _ScriptedPreflightMqtt(auto_respond={topics.ha_to_panel: (ha_result, False)})
    report_task = asyncio.ensure_future(async_run_preflight(_settings(), request, _factory(fake)))

    await _wait_until(lambda: topics.retained in fake.subscribed)
    await fake.deliver(topics.retained, "some-other-value", retained=True)

    report = await report_task

    assert report.failed_stage == PreflightStage.RETAINED_MESSAGE
    assert report.error_code == "mqtt_payload"


# -- Stage timeout settles its cancelled operation before cleanup runs -----------
# Regression test for the review's MAJOR finding: PANEL_TO_HA/DISCOVERY_WRITE/
# RETAINED_MESSAGE must settle_on_cancel=True the same way FLEET_AUTH always
# did, so a timed-out publish/subscribe cannot still be unwinding inside
# aiomqtt while _cleanup touches the same client.


async def test_stage_timeout_settles_before_cleanup_touches_the_client() -> None:
    request = _request(timeout_seconds=0.05)
    topics = SetupTopics.for_id(request.setup_id)
    fake = _ScriptedPreflightMqtt(hang_subscribe_topic=topics.ha_to_panel)
    report_task = asyncio.ensure_future(async_run_preflight(_settings(), request, _factory(fake)))

    # Let the per-stage deadline elapse while the subscribe is hung — the
    # timeout fires and cancels it, but our fake's subscribe() catches that
    # cancellation and blocks (still "active") on a gate only this test
    # controls, modeling a library call that takes real time to unwind.
    await _wait_until(lambda: fake.hang_active)
    await asyncio.sleep(request.timeout_seconds + 0.05)
    assert fake.hang_active, "the hung subscribe should still be settling, not yet gone"
    assert report_task.done() is False, "settle_on_cancel must hold the stage open"

    fake.release_hang()
    report = await report_task

    assert fake.cleanup_saw_active_hang is False
    assert report.failed_stage == PreflightStage.PANEL_TO_HA
    assert report.error_code == "mqtt_timeout"
    # Cleanup still ran (disconnect etc.) despite the earlier timeout.
    assert fake.disconnect_calls == 1


async def test_discovery_write_timeout_settles_before_cleanup_touches_the_client() -> None:
    request = _request(timeout_seconds=0.05)
    topics = SetupTopics.for_id(request.setup_id)
    ha_result = SetupResult(request.setup_id, request.ha_nonce, request.panel_nonce).to_payload()
    fake = _ScriptedPreflightMqtt(
        auto_respond={topics.ha_to_panel: (ha_result, False)},
        hang_publish_topic=topics.discovery_probe,
    )
    report_task = asyncio.ensure_future(async_run_preflight(_settings(), request, _factory(fake)))

    await _wait_until(lambda: fake.hang_active)
    await asyncio.sleep(request.timeout_seconds + 0.05)
    assert fake.hang_active, "the hung discovery-write publish should still be settling"
    assert report_task.done() is False

    fake.release_hang()
    report = await report_task

    assert fake.cleanup_saw_active_hang is False
    assert report.failed_stage == PreflightStage.DISCOVERY_WRITE
    assert report.error_code == "mqtt_timeout"
    assert fake.disconnect_calls == 1
    # _cleanup's own unconditional clear of topics.discovery_probe ran cleanly
    # afterward — the one-shot hang did not re-trigger and wedge cleanup too.
    assert (topics.discovery_probe, "", True, 1) in fake.published


async def test_retained_message_publish_timeout_settles_before_cleanup_touches_the_client() -> None:
    request = _request(timeout_seconds=0.05)
    topics = SetupTopics.for_id(request.setup_id)
    ha_result = SetupResult(request.setup_id, request.ha_nonce, request.panel_nonce).to_payload()
    fake = _ScriptedPreflightMqtt(
        auto_respond={topics.ha_to_panel: (ha_result, False)},
        hang_publish_topic=topics.retained,
    )
    report_task = asyncio.ensure_future(async_run_preflight(_settings(), request, _factory(fake)))

    await _wait_until(lambda: fake.hang_active)
    await asyncio.sleep(request.timeout_seconds + 0.05)
    assert fake.hang_active, "the hung retained-claim publish should still be settling"
    assert report_task.done() is False

    fake.release_hang()
    report = await report_task

    assert fake.cleanup_saw_active_hang is False
    assert report.failed_stage == PreflightStage.RETAINED_MESSAGE
    assert report.error_code == "mqtt_timeout"
    # _cleanup's own unconditional publish("") to topics.retained runs AFTER
    # the hang settles (one-shot), not concurrently with it.
    assert fake.disconnect_calls == 1


# -- Cleanup always runs; its failure surfaces only when otherwise successful ----


async def test_cleanup_failure_surfaces_when_run_otherwise_succeeds() -> None:
    request = _request()
    topics = SetupTopics.for_id(request.setup_id)
    fake = _happy_fake(request, topics)
    fake.disconnect_error = RuntimeError("disconnect boom")

    report = await async_run_preflight(_settings(), request, _factory(fake))

    assert report.success is False
    assert report.failed_stage == PreflightStage.CLEANUP
    assert report.error_code == "cleanup_failed"


async def test_primary_failure_takes_precedence_over_cleanup_failure() -> None:
    request = _request()
    topics = SetupTopics.for_id(request.setup_id)
    fake = _happy_fake(request, topics)
    fake.publish_errors[topics.discovery_probe] = RuntimeError("primary boom")
    fake.disconnect_error = RuntimeError("cleanup boom too")

    report = await async_run_preflight(_settings(), request, _factory(fake))

    assert report.failed_stage == PreflightStage.DISCOVERY_WRITE
    assert report.error_code == "mqtt_publish"
    # Cleanup still ran despite the primary failure (disconnect was attempted
    # and DID fail, but that failure is superseded by the primary one above).
    assert fake.disconnect_calls == 1


async def test_cleanup_is_a_noop_when_the_mqtt_factory_itself_fails() -> None:
    request = _request()

    def failing_factory(
        settings: Settings,
        *,
        identifier: str,
        publish_availability: bool,
        checked_disconnect: bool,
        redacted_logging: bool,
    ) -> _ScriptedPreflightMqtt:
        raise RuntimeError("could not construct adapter")

    report = await async_run_preflight(_settings(), request, failing_factory)

    # `_cleanup` is a no-op when `mqtt` was never assigned — the factory call
    # itself failed before the nonlocal `mqtt` binding could happen — and the
    # FLEET_AUTH failure is what's reported, not a cleanup failure.
    assert report.failed_stage == PreflightStage.FLEET_AUTH
    assert report.error_code == "mqtt_connect"
    assert report.completed_stages == (PreflightStage.CLEANUP,)
