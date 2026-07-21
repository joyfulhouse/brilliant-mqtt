"""Panel-side MQTT preflight state-machine and CLI tests."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from collections.abc import Awaitable, Callable, Iterator
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

import brilliant_mqtt.preflight as preflight_module
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
        self.publish_hook: PublishHook | None = None
        self.subscribe_hook: SubscribeHook | None = None

    async def _before(self, key: tuple[str, ...]) -> None:
        self.events.append(key)
        if key in self.blocked:
            await asyncio.Future()
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
            },
        ),
        (
            _settings(),
            {
                "identifier": f"brilliant-mqtt-setup-{SECOND_SETUP_ID}",
                "publish_availability": False,
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


async def test_every_stage_uses_request_timeout_and_wait_timeout_is_reported(
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
    assert timeouts == [0.01, 0.01, 0.01, 0.01]
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


def test_cli_help_exits_zero_and_lists_required_request_argument(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as raised:
        preflight_module.main(["--help"])

    captured = capsys.readouterr()
    assert raised.value.code == 0
    assert "--request-json" in captured.out
    assert captured.err == ""


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
