"""Temporary panel-side MQTT validation state machine and JSON CLI."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Protocol, TypeVar, cast
from uuid import UUID

from brilliant_mqtt.config import Settings
from brilliant_mqtt.mqttio import AioMqttAdapter
from brilliant_mqtt.setup_protocol import SCHEMA_VERSION, SetupRequest, SetupResult, SetupTopics

_REQUEST_KEYS = frozenset(
    {"schema_version", "setup_id", "panel_nonce", "ha_nonce", "timeout_seconds"}
)

_CONNECT_DETAIL = "MQTT connection failed"
_PUBLISH_DETAIL = "MQTT publish failed"
_SUBSCRIBE_DETAIL = "MQTT subscription failed"
_TIMEOUT_DETAIL = "MQTT stage timed out"
_PAYLOAD_DETAIL = "MQTT payload validation failed"
_RETAINED_DETAIL = "Retained replay flag was missing"
_CLEANUP_DETAIL = "MQTT cleanup failed"

_T = TypeVar("_T")


class PreflightStage(str, Enum):
    """Ordered panel-side setup validation stages."""

    FLEET_AUTH = "fleet_auth"
    PANEL_TO_HA = "panel_to_ha"
    HA_TO_PANEL = "ha_to_panel"
    DISCOVERY_WRITE = "discovery_write"
    RETAINED_MESSAGE = "retained_message"
    CLEANUP = "cleanup"


@dataclass(frozen=True, slots=True)
class PreflightRequest:
    """Strict input contract passed to the temporary panel process."""

    setup_id: UUID
    panel_nonce: str
    ha_nonce: str
    timeout_seconds: float

    def __post_init__(self) -> None:
        try:
            SetupTopics.for_id(self.setup_id)
            SetupRequest(self.setup_id, self.panel_nonce)
            SetupResult(self.setup_id, self.ha_nonce, self.panel_nonce)
        except ValueError as error:
            raise ValueError(
                "invalid_preflight_request: invalid setup identity or nonce"
            ) from error
        if (
            isinstance(self.timeout_seconds, bool)
            or not isinstance(self.timeout_seconds, (int, float))
            or not math.isfinite(self.timeout_seconds)
            or self.timeout_seconds <= 0
        ):
            raise ValueError("invalid_preflight_request: timeout_seconds must be positive")
        object.__setattr__(self, "timeout_seconds", float(self.timeout_seconds))

    @classmethod
    def from_json(cls, raw: str) -> PreflightRequest:
        """Parse an exact schema-v1 preflight request without echoing input."""
        try:
            decoded = json.loads(raw)
        except (json.JSONDecodeError, RecursionError, TypeError) as error:
            raise ValueError("invalid_preflight_request: expected a JSON object") from error
        if not isinstance(decoded, dict):
            raise ValueError("invalid_preflight_request: expected a JSON object")

        value = cast(dict[str, object], decoded)
        if set(value) != _REQUEST_KEYS:
            raise ValueError("invalid_preflight_request: unexpected or missing keys")
        if type(value["schema_version"]) is not int or value["schema_version"] != SCHEMA_VERSION:
            raise ValueError("invalid_preflight_request: expected schema_version 1")
        try:
            setup_id = UUID(str(value["setup_id"]))
        except (AttributeError, TypeError, ValueError) as error:
            raise ValueError(
                "invalid_preflight_request: invalid setup identity or nonce"
            ) from error
        return cls(
            setup_id=setup_id,
            panel_nonce=cast(str, value["panel_nonce"]),
            ha_nonce=cast(str, value["ha_nonce"]),
            timeout_seconds=cast(float, value["timeout_seconds"]),
        )


@dataclass(frozen=True, slots=True)
class PreflightReport:
    """Deterministic, redacted result consumed by the HA coordinator."""

    setup_id: UUID
    success: bool
    completed_stages: tuple[PreflightStage, ...]
    stage_elapsed_ms: dict[PreflightStage, int]
    last_stage: PreflightStage | None = None
    failed_stage: PreflightStage | None = None
    error_code: str | None = None
    detail: str | None = None

    def to_json(self) -> str:
        """Serialize one canonical compact report object."""
        value: dict[str, object] = {
            "schema_version": SCHEMA_VERSION,
            "setup_id": str(self.setup_id),
            "success": self.success,
            "completed_stages": [stage.value for stage in self.completed_stages],
            "stage_elapsed_ms": {
                stage.value: elapsed for stage, elapsed in self.stage_elapsed_ms.items()
            },
        }
        if self.success:
            if self.last_stage is not None:
                value["last_stage"] = self.last_stage.value
        else:
            if self.failed_stage is not None:
                value["failed_stage"] = self.failed_stage.value
            if self.error_code is not None:
                value["error_code"] = self.error_code
            if self.detail is not None:
                value["detail"] = self.detail
        return json.dumps(value, sort_keys=True, separators=(",", ":"))


class _PreflightMqtt(Protocol):
    async def connect(self) -> None: ...

    async def disconnect(self) -> None: ...

    async def publish(
        self,
        topic: str,
        payload: str,
        retain: bool = False,
        qos: int = 0,
    ) -> None: ...

    def on_message(self, cb: Callable[[str, str, bool], Awaitable[None]]) -> None: ...

    async def subscribe(self, topic: str) -> None: ...

    async def unsubscribe(self, topic: str) -> None: ...


class _MqttFactory(Protocol):
    def __call__(
        self,
        settings: Settings,
        *,
        identifier: str,
        publish_availability: bool,
    ) -> _PreflightMqtt: ...


_Runner = Callable[[Settings, PreflightRequest], Awaitable[PreflightReport]]


@dataclass(frozen=True, slots=True)
class _Failure(Exception):
    stage: PreflightStage
    code: str
    detail: str


@dataclass(frozen=True, slots=True)
class _InboundResult:
    code: str | None = None
    detail: str | None = None


def _monotonic_ms() -> int:
    return round(time.monotonic() * 1_000)


async def _wait_for(awaitable: Awaitable[_T], timeout: float) -> _T:
    return await asyncio.wait_for(awaitable, timeout=timeout)


async def _mqtt_operation(
    stage: PreflightStage,
    code: str,
    detail: str,
    operation: Awaitable[None],
) -> None:
    try:
        await operation
    except asyncio.CancelledError:
        raise
    except asyncio.TimeoutError as error:
        raise _Failure(stage, "mqtt_timeout", _TIMEOUT_DETAIL) from error
    except Exception as error:
        raise _Failure(stage, code, detail) from error


async def _run_stage(
    stage: PreflightStage,
    operation: Callable[[], Awaitable[None]],
    timeout_seconds: float,
    completed: list[PreflightStage],
    elapsed: dict[PreflightStage, int],
) -> None:
    started = _monotonic_ms()
    try:
        await _wait_for(operation(), timeout_seconds)
    except asyncio.CancelledError:
        raise
    except asyncio.TimeoutError as error:
        raise _Failure(stage, "mqtt_timeout", _TIMEOUT_DETAIL) from error
    finally:
        elapsed[stage] = max(0, _monotonic_ms() - started)
    completed.append(stage)


async def _cleanup(
    mqtt: _PreflightMqtt | None,
    topics: SetupTopics,
    subscribed: Sequence[str],
    timeout_seconds: float,
) -> None:
    if mqtt is None:
        return

    operations: list[Callable[[], Awaitable[None]]] = [
        lambda: mqtt.publish(topics.discovery_probe, "", retain=True, qos=1),
        lambda: mqtt.publish(topics.retained, "", retain=True, qos=1),
    ]
    for topic in subscribed:

        async def unsubscribe(subscribed_topic: str = topic) -> None:
            await mqtt.unsubscribe(subscribed_topic)

        operations.append(unsubscribe)
    operations.append(mqtt.disconnect)

    # Give every independent cleanup action one share of the stage budget. A
    # hung clear cannot prevent the second clear, unsubscriptions, or disconnect.
    action_timeout = timeout_seconds / len(operations)
    timed_out = False
    failed = False
    for operation in operations:
        try:
            await asyncio.wait_for(operation(), timeout=action_timeout)
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            timed_out = True
        except Exception:
            failed = True

    if timed_out:
        raise _Failure(PreflightStage.CLEANUP, "mqtt_timeout", _TIMEOUT_DETAIL)
    if failed:
        raise _Failure(PreflightStage.CLEANUP, "cleanup_failed", _CLEANUP_DETAIL)


async def async_run_preflight(
    settings: Settings,
    request: PreflightRequest,
    mqtt_factory: _MqttFactory = AioMqttAdapter,
) -> PreflightReport:
    """Run one bounded MQTT setup validation and always clean its topics."""
    topics = SetupTopics.for_id(request.setup_id)
    loop = asyncio.get_running_loop()
    ha_result: asyncio.Future[_InboundResult] = loop.create_future()
    retained_result: asyncio.Future[_InboundResult] = loop.create_future()
    retained_subscription_active = False
    mqtt: _PreflightMqtt | None = None
    subscribed: list[str] = []
    completed: list[PreflightStage] = []
    elapsed: dict[PreflightStage, int] = {}
    primary_failure: _Failure | None = None
    cleanup_failure: _Failure | None = None
    cancellation: asyncio.CancelledError | None = None

    async def on_message(topic: str, payload: str, retained: bool) -> None:
        if topic == topics.ha_to_panel and not ha_result.done():
            try:
                result = SetupResult.from_payload(payload)
            except ValueError:
                ha_result.set_result(_InboundResult("mqtt_payload", _PAYLOAD_DETAIL))
                return
            if (
                result.setup_id != request.setup_id
                or result.nonce != request.ha_nonce
                or result.reply_to_nonce != request.panel_nonce
            ):
                ha_result.set_result(_InboundResult("mqtt_payload", _PAYLOAD_DETAIL))
                return
            ha_result.set_result(_InboundResult())
            return

        if topic == topics.retained and retained_subscription_active and not retained_result.done():
            if payload != request.panel_nonce:
                retained_result.set_result(_InboundResult("mqtt_payload", _PAYLOAD_DETAIL))
            elif not retained:
                retained_result.set_result(
                    _InboundResult("retained_flag_missing", _RETAINED_DETAIL)
                )
            else:
                retained_result.set_result(_InboundResult())

    async def fleet_auth() -> None:
        nonlocal mqtt
        try:
            mqtt = mqtt_factory(
                settings,
                identifier=f"brilliant-mqtt-setup-{request.setup_id}",
                publish_availability=False,
            )
            mqtt.on_message(on_message)
        except Exception as error:
            raise _Failure(PreflightStage.FLEET_AUTH, "mqtt_connect", _CONNECT_DETAIL) from error
        await _mqtt_operation(
            PreflightStage.FLEET_AUTH,
            "mqtt_connect",
            _CONNECT_DETAIL,
            mqtt.connect(),
        )

    async def panel_to_ha() -> None:
        assert mqtt is not None
        await _mqtt_operation(
            PreflightStage.PANEL_TO_HA,
            "mqtt_subscribe",
            _SUBSCRIBE_DETAIL,
            mqtt.subscribe(topics.ha_to_panel),
        )
        subscribed.append(topics.ha_to_panel)
        await _mqtt_operation(
            PreflightStage.PANEL_TO_HA,
            "mqtt_publish",
            _PUBLISH_DETAIL,
            mqtt.publish(
                topics.panel_to_ha,
                SetupRequest(request.setup_id, request.panel_nonce).to_payload(),
                retain=False,
                qos=1,
            ),
        )

    async def ha_to_panel() -> None:
        result = await ha_result
        if result.code is not None:
            raise _Failure(
                PreflightStage.HA_TO_PANEL,
                result.code,
                result.detail or _PAYLOAD_DETAIL,
            )

    async def discovery_write() -> None:
        assert mqtt is not None
        await _mqtt_operation(
            PreflightStage.DISCOVERY_WRITE,
            "mqtt_publish",
            _PUBLISH_DETAIL,
            mqtt.publish(
                topics.discovery_probe,
                request.panel_nonce,
                retain=True,
                qos=1,
            ),
        )

    async def retained_message() -> None:
        nonlocal retained_subscription_active
        assert mqtt is not None
        await _mqtt_operation(
            PreflightStage.RETAINED_MESSAGE,
            "mqtt_publish",
            _PUBLISH_DETAIL,
            mqtt.publish(topics.retained, request.panel_nonce, retain=True, qos=1),
        )
        retained_subscription_active = True
        await _mqtt_operation(
            PreflightStage.RETAINED_MESSAGE,
            "mqtt_subscribe",
            _SUBSCRIBE_DETAIL,
            mqtt.subscribe(topics.retained),
        )
        subscribed.append(topics.retained)
        result = await retained_result
        if result.code is not None:
            raise _Failure(
                PreflightStage.RETAINED_MESSAGE,
                result.code,
                result.detail or _PAYLOAD_DETAIL,
            )

    try:
        try:
            await _run_stage(
                PreflightStage.FLEET_AUTH,
                fleet_auth,
                request.timeout_seconds,
                completed,
                elapsed,
            )
            await _run_stage(
                PreflightStage.PANEL_TO_HA,
                panel_to_ha,
                request.timeout_seconds,
                completed,
                elapsed,
            )
            await _run_stage(
                PreflightStage.HA_TO_PANEL,
                ha_to_panel,
                request.timeout_seconds,
                completed,
                elapsed,
            )
            await _run_stage(
                PreflightStage.DISCOVERY_WRITE,
                discovery_write,
                request.timeout_seconds,
                completed,
                elapsed,
            )
            await _run_stage(
                PreflightStage.RETAINED_MESSAGE,
                retained_message,
                request.timeout_seconds,
                completed,
                elapsed,
            )
        except asyncio.CancelledError as error:
            cancellation = error
        except _Failure as error:
            primary_failure = error
    finally:
        try:
            await _run_stage(
                PreflightStage.CLEANUP,
                lambda: _cleanup(mqtt, topics, tuple(subscribed), request.timeout_seconds),
                request.timeout_seconds,
                completed,
                elapsed,
            )
        except asyncio.CancelledError:
            if cancellation is None:
                raise
        except _Failure as error:
            cleanup_failure = error

    if cancellation is not None:
        raise cancellation

    failure = primary_failure or cleanup_failure
    if failure is not None:
        return PreflightReport(
            setup_id=request.setup_id,
            success=False,
            completed_stages=tuple(completed),
            stage_elapsed_ms=elapsed,
            failed_stage=failure.stage,
            error_code=failure.code,
            detail=failure.detail,
        )
    return PreflightReport(
        setup_id=request.setup_id,
        success=True,
        completed_stages=tuple(completed),
        stage_elapsed_ms=elapsed,
        last_stage=PreflightStage.CLEANUP,
    )


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m brilliant_mqtt.preflight")
    parser.add_argument("--request-json", required=True)
    return parser


async def async_main(
    argv: Sequence[str] | None = None,
    *,
    settings_factory: Callable[[], Settings] = Settings.from_env,
    runner: _Runner = async_run_preflight,
) -> int:
    """Parse CLI input, execute one preflight, and print one JSON object."""
    args = _argument_parser().parse_args(argv)
    request = PreflightRequest.from_json(args.request_json)
    report = await runner(settings_factory(), request)
    print(report.to_json())
    return 0 if report.success else 1


def main(argv: Sequence[str] | None = None) -> int:
    """Synchronous CLI entry point."""
    return asyncio.run(async_main(argv))


if __name__ == "__main__":
    raise SystemExit(main())
