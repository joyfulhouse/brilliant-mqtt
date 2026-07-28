"""Temporary panel-side MQTT validation state machine and JSON CLI."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import stat
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import FrozenInstanceError, dataclass
from typing import Protocol, cast
from uuid import UUID

from brilliant_mqtt.config import Settings
from brilliant_mqtt.mqttio import AioMqttAdapter, MqttPayloadDecodeError
from brilliant_mqtt.setup_protocol import (
    SCHEMA_VERSION,
    SetupRequest,
    SetupResult,
    SetupTopics,
)
from brilliant_mqtt.setup_protocol import (
    PreflightReport as PreflightReport,
)
from brilliant_mqtt.setup_protocol import (
    PreflightRequest as PreflightRequest,
)
from brilliant_mqtt.setup_protocol import (
    PreflightStage as PreflightStage,
)

_CONNECT_DETAIL = "MQTT connection failed"
_SETTINGS_DETAIL = "Panel configuration invalid"
_PUBLISH_DETAIL = "MQTT publish failed"
_SUBSCRIBE_DETAIL = "MQTT subscription failed"
_TIMEOUT_DETAIL = "MQTT stage timed out"
_PAYLOAD_DETAIL = "MQTT payload validation failed"
_RETAINED_DETAIL = "Retained replay flag was missing"
_CLEANUP_DETAIL = "MQTT cleanup failed"
_RUNTIME_EXCEPTION_ATTRIBUTES = frozenset(
    {
        "__cause__",
        "__context__",
        "__notes__",
        "__suppress_context__",
        "__traceback__",
    }
)
_FAILURE_FIELDS = frozenset({"stage", "code", "detail"})
_MAX_ENVIRONMENT_FILE_BYTES = 16 * 1024
_ENVIRONMENT_KEY = re.compile(r"^[A-Z][A-Z0-9_]*$")
_BARE_ENVIRONMENT_VALUE = re.compile(r"^[A-Za-z0-9_./:+-]+$")
_PREFLIGHT_ENVIRONMENT_KEYS = frozenset(
    {
        "BRILLIANT_PANEL",
        "BRILLIANT_DEPLOYMENT_ID",
        "LOG_LEVEL",
        "MESH_PRIORITY",
        "MQTT_HOST",
        "MQTT_PASSWORD",
        "MQTT_PORT",
        "MQTT_TLS_CA_FILE",
        "MQTT_TLS_ENABLED",
        "MQTT_USERNAME",
        "RETAINED_TOPICS_FILE",
        "SCENE_BRIDGE_ENABLED",
    }
)

_detached_cleanup_tasks: set[asyncio.Task[None]] = set()
_detached_preflight_futures: set[asyncio.Future[None]] = set()


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

    def on_payload_decode_error(
        self,
        cb: Callable[[MqttPayloadDecodeError], Awaitable[None]],
    ) -> None: ...

    async def subscribe(self, topic: str) -> None: ...

    async def unsubscribe(self, topic: str) -> None: ...


class _MqttFactory(Protocol):
    def __call__(
        self,
        settings: Settings,
        *,
        identifier: str,
        publish_availability: bool,
        checked_disconnect: bool,
        redacted_logging: bool,
    ) -> _PreflightMqtt: ...


_Runner = Callable[[Settings, PreflightRequest], Awaitable[PreflightReport]]


@dataclass(slots=True)
class _Failure(Exception):
    stage: PreflightStage
    code: str
    detail: str

    def __post_init__(self) -> None:
        Exception.__init__(self, self.code)

    def __setattr__(self, name: str, value: object) -> None:
        if name in _FAILURE_FIELDS and not hasattr(self, name):
            object.__setattr__(self, name, value)
            return
        if name in _RUNTIME_EXCEPTION_ATTRIBUTES:
            BaseException.__setattr__(self, name, value)
            return
        raise FrozenInstanceError(f"cannot assign to field {name!r}")

    def __delattr__(self, name: str) -> None:
        if name in _RUNTIME_EXCEPTION_ATTRIBUTES:
            BaseException.__delattr__(self, name)
            return
        raise FrozenInstanceError(f"cannot delete field {name!r}")


@dataclass(frozen=True, slots=True)
class _InboundResult:
    code: str | None = None
    detail: str | None = None


def _monotonic_ms() -> int:
    return round(time.monotonic() * 1_000)


async def _wait_for(
    awaitable: Awaitable[None],
    timeout: float,
    *,
    settle_on_cancel: bool = False,
) -> None:
    """Apply a stage deadline while optionally settling cancellation-safe I/O.

    ``settle_on_cancel`` deliberately makes the deadline soft when the
    operation owns executor-backed lifecycle work that cannot be stopped.
    """
    operation = asyncio.ensure_future(awaitable)
    try:
        done, _ = await asyncio.wait({operation}, timeout=timeout)
    except asyncio.CancelledError:
        operation.cancel()
        if settle_on_cancel:
            await _settle_cancelled_preflight_future(operation)
        else:
            _track_detached_preflight_future(operation)
        raise
    if not done:
        operation.cancel()
        if settle_on_cancel:
            await _settle_cancelled_preflight_future(operation)
        else:
            _track_detached_preflight_future(operation)
        raise asyncio.TimeoutError
    operation.result()


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


async def _mqtt_operation_observing_inbound(
    operation_stage: PreflightStage,
    code: str,
    detail: str,
    operation: Callable[[], Awaitable[None]],
    inbound_result: asyncio.Future[_InboundResult],
    validation_stage: PreflightStage,
) -> None:
    """Await a broker acknowledgement while allowing malformed input to fail fast."""

    async def run_operation() -> None:
        await _mqtt_operation(operation_stage, code, detail, operation())

    operation_task = asyncio.create_task(run_operation())
    waitables = {
        cast(asyncio.Future[object], operation_task),
        cast(asyncio.Future[object], inbound_result),
    }
    try:
        await asyncio.wait(
            waitables,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if inbound_result.done():
            result = inbound_result.result()
            if result.code is not None:
                if operation_task.done():
                    _consume_future_result(operation_task)
                else:
                    operation_task.cancel()
                    _track_detached_preflight_future(operation_task)
                raise _Failure(
                    validation_stage,
                    result.code,
                    result.detail or _PAYLOAD_DETAIL,
                )
        await asyncio.shield(operation_task)
    except asyncio.CancelledError:
        if operation_task.done():
            _consume_future_result(operation_task)
        else:
            operation_task.cancel()
            _track_detached_preflight_future(operation_task)
        raise


async def _run_stage(
    stage: PreflightStage,
    operation: Callable[[], Awaitable[None]],
    timeout_seconds: float,
    completed: list[PreflightStage],
    elapsed: dict[PreflightStage, int],
    *,
    settle_on_cancel: bool = False,
) -> None:
    started = _monotonic_ms()
    try:
        await _wait_for(
            operation(),
            timeout_seconds,
            settle_on_cancel=settle_on_cancel,
        )
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

    # Give every independent action one share and reserve one additional share
    # for task-cancellation and scheduling overhead. A hung clear cannot consume
    # the outer stage boundary before later unsubscriptions or disconnect begin.
    action_timeout = timeout_seconds / (len(operations) + 1)
    timed_out = False
    failed = False
    for operation in operations:
        action_task: asyncio.Task[None] = asyncio.create_task(_run_cleanup_action(operation))
        try:
            done, _ = await asyncio.wait({action_task}, timeout=action_timeout)
        except asyncio.CancelledError:
            action_task.cancel()
            _track_detached_cleanup_task(action_task)
            raise
        if not done:
            timed_out = True
            action_task.cancel()
            _track_detached_cleanup_task(action_task)
            continue
        try:
            action_task.result()
        except asyncio.CancelledError:
            failed = True
        except asyncio.TimeoutError:
            timed_out = True
        except Exception:
            failed = True

    if timed_out:
        raise _Failure(PreflightStage.CLEANUP, "mqtt_timeout", _TIMEOUT_DETAIL)
    if failed:
        raise _Failure(PreflightStage.CLEANUP, "cleanup_failed", _CLEANUP_DETAIL)


async def _run_cleanup_action(operation: Callable[[], Awaitable[None]]) -> None:
    await operation()


def _track_detached_cleanup_task(task: asyncio.Task[None]) -> None:
    """Keep cancellation-pending cleanup work alive and consume its result."""
    _detached_cleanup_tasks.add(task)
    task.add_done_callback(_consume_detached_cleanup_task)


def _consume_detached_cleanup_task(task: asyncio.Task[None]) -> None:
    _detached_cleanup_tasks.discard(task)
    _consume_future_result(task)


def _track_detached_preflight_future(future: asyncio.Future[None]) -> None:
    """Keep canceled stage/broker work alive and consume its terminal state."""
    _detached_preflight_futures.add(future)
    future.add_done_callback(_consume_detached_preflight_future)


def _consume_detached_preflight_future(future: asyncio.Future[None]) -> None:
    _detached_preflight_futures.discard(future)
    _consume_future_result(future)


def _consume_future_result(future: asyncio.Future[None]) -> None:
    if not future.cancelled():
        future.exception()


async def _settle_cancelled_preflight_future(future: asyncio.Future[None]) -> None:
    """Settle connect work after cancellation without changing its primary result."""
    while not future.done():
        try:
            await asyncio.shield(future)
        except asyncio.CancelledError:
            if future.done():
                break
        except BaseException:
            break
        else:
            break
    _consume_future_result(future)


async def _run_cleanup_stage(
    operation: Callable[[], Awaitable[None]],
    completed: list[PreflightStage],
    elapsed: dict[PreflightStage, int],
) -> None:
    """Record cleanup without adding an outer timeout around its action loop."""
    started = _monotonic_ms()
    try:
        await operation()
    finally:
        elapsed[PreflightStage.CLEANUP] = max(0, _monotonic_ms() - started)
    completed.append(PreflightStage.CLEANUP)


async def async_run_preflight(
    settings: Settings,
    request: PreflightRequest,
    mqtt_factory: _MqttFactory = AioMqttAdapter,
) -> PreflightReport:
    """Run MQTT setup validation and always settle lifecycle cleanup.

    Protocol stages use ``request.timeout_seconds`` as result deadlines. The
    fleet-auth stage can return later when an OS-level connect worker cannot be
    cancelled safely; CLI callers must impose and enforce an outer process
    deadline.
    """
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
                return
            if (
                result.setup_id != request.setup_id
                or result.nonce != request.ha_nonce
                or result.reply_to_nonce != request.panel_nonce
            ):
                return
            ha_result.set_result(_InboundResult())
            return

        if topic == topics.retained and retained_subscription_active and not retained_result.done():
            if payload != request.panel_nonce:
                return
            if not retained:
                retained_result.set_result(
                    _InboundResult("retained_flag_missing", _RETAINED_DETAIL)
                )
            else:
                retained_result.set_result(_InboundResult())

    async def on_payload_decode_error(error: MqttPayloadDecodeError) -> None:
        # Malformed and stale payloads are not evidence about broker behavior.
        # Exact messages determine success; silence until the bound maps to a
        # retryable timeout rather than a definitive payload or ACL diagnosis.
        return

    async def fleet_auth() -> None:
        nonlocal mqtt
        try:
            mqtt = mqtt_factory(
                settings,
                identifier=f"brilliant-mqtt-setup-{request.setup_id}",
                publish_availability=False,
                checked_disconnect=True,
                redacted_logging=True,
            )
            mqtt.on_message(on_message)
            mqtt.on_payload_decode_error(on_payload_decode_error)
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
        subscribed.append(topics.ha_to_panel)
        await _mqtt_operation(
            PreflightStage.PANEL_TO_HA,
            "mqtt_subscribe",
            _SUBSCRIBE_DETAIL,
            mqtt.subscribe(topics.ha_to_panel),
        )
        await _mqtt_operation_observing_inbound(
            PreflightStage.PANEL_TO_HA,
            "mqtt_publish",
            _PUBLISH_DETAIL,
            lambda: mqtt.publish(
                topics.panel_to_ha,
                SetupRequest(request.setup_id, request.panel_nonce).to_payload(),
                retain=False,
                qos=1,
            ),
            ha_result,
            PreflightStage.HA_TO_PANEL,
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
        subscribed.append(topics.retained)
        await _mqtt_operation_observing_inbound(
            PreflightStage.RETAINED_MESSAGE,
            "mqtt_subscribe",
            _SUBSCRIBE_DETAIL,
            lambda: mqtt.subscribe(topics.retained),
            retained_result,
            PreflightStage.RETAINED_MESSAGE,
        )
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
                settle_on_cancel=True,
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

        async def run_cleanup_stage() -> _Failure | None:
            try:
                await _run_cleanup_stage(
                    lambda: _cleanup(mqtt, topics, tuple(subscribed), request.timeout_seconds),
                    completed,
                    elapsed,
                )
            except _Failure as error:
                return error
            return None

        cleanup_task = asyncio.create_task(run_cleanup_stage())
        while not cleanup_task.done():
            try:
                cleanup_failure = await asyncio.shield(cleanup_task)
            except asyncio.CancelledError as error:
                if cancellation is None:
                    cancellation = error
        cleanup_failure = cleanup_task.result()

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
    parser = argparse.ArgumentParser(
        prog="python -m brilliant_mqtt.preflight",
        description=(
            "An outer process deadline is required: timeout_seconds is a "
            "per-stage result deadline, while connection lifecycle settlement "
            "may take longer."
        ),
    )
    parser.add_argument("--environment-file")
    parser.add_argument("--request-json", required=True)
    return parser


def _decode_environment_value(raw: str) -> str:
    if len(raw) >= 2 and raw[0] == raw[-1] == '"':
        decoded: list[str] = []
        index = 1
        while index < len(raw) - 1:
            character = raw[index]
            if character == '"':
                raise ValueError("invalid_preflight_environment")
            if character != "\\":
                decoded.append(character)
                index += 1
                continue
            index += 1
            if index >= len(raw) - 1 or raw[index] not in {'"', "\\"}:
                raise ValueError("invalid_preflight_environment")
            decoded.append(raw[index])
            index += 1
        return "".join(decoded)
    if _BARE_ENVIRONMENT_VALUE.fullmatch(raw) is None:
        raise ValueError("invalid_preflight_environment")
    return raw


def _invalid_systemd_environment_character(character: str) -> bool:
    """Match systemd EnvironmentFile's excluded Unicode scalar values."""
    codepoint = ord(character)
    return (
        codepoint == 0
        or codepoint == 0xFEFF
        or 0xD800 <= codepoint <= 0xDFFF
        or 0xFDD0 <= codepoint <= 0xFDEF
        or codepoint & 0xFFFF in {0xFFFE, 0xFFFF}
    )


def _settings_from_environment_file(path: str) -> Settings:
    """Read the generated systemd env syntax without invoking a shell."""
    if not isinstance(path, str) or not path or "\0" in path:
        raise ValueError("invalid_preflight_environment")
    descriptor: int | None = None
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK,
        )
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > _MAX_ENVIRONMENT_FILE_BYTES:
            raise ValueError
        chunks: list[bytes] = []
        remaining = metadata.st_size
        while remaining:
            chunk = os.read(descriptor, min(remaining, 65536))
            if not chunk:
                raise ValueError
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise ValueError
        raw_environment = b"".join(chunks).decode("utf-8")
    except (OSError, UnicodeError, ValueError):
        raise ValueError("invalid_preflight_environment") from None
    finally:
        if descriptor is not None:
            os.close(descriptor)

    environment: dict[str, str] = {}
    try:
        if "\r" in raw_environment or any(
            _invalid_systemd_environment_character(character) for character in raw_environment
        ):
            raise ValueError
        for line in raw_environment.split("\n"):
            if not line:
                continue
            key, separator, raw_value = line.partition("=")
            if (
                not separator
                or _ENVIRONMENT_KEY.fullmatch(key) is None
                or key not in _PREFLIGHT_ENVIRONMENT_KEYS
                or key in environment
            ):
                raise ValueError
            environment[key] = _decode_environment_value(raw_value)
        return Settings.from_env(environment)
    except (KeyError, TypeError, ValueError):
        raise ValueError("invalid_preflight_environment") from None


def _cli_failure_json(*, setup_id: UUID | None, code: str, detail: str) -> str:
    if setup_id is not None:
        return PreflightReport(
            setup_id=setup_id,
            success=False,
            completed_stages=(PreflightStage.CLEANUP,),
            stage_elapsed_ms={
                PreflightStage.FLEET_AUTH: 0,
                PreflightStage.CLEANUP: 0,
            },
            failed_stage=PreflightStage.FLEET_AUTH,
            error_code=code,
            detail=detail,
        ).to_json()
    return json.dumps(
        {
            "schema_version": SCHEMA_VERSION,
            "setup_id": str(setup_id) if setup_id is not None else None,
            "success": False,
            "completed_stages": [],
            "stage_elapsed_ms": {},
            "failed_stage": PreflightStage.FLEET_AUTH.value,
            "error_code": code,
            "detail": detail,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


async def async_main(
    argv: Sequence[str] | None = None,
    *,
    settings_factory: Callable[[], Settings] | None = None,
    runner: _Runner = async_run_preflight,
) -> int:
    """Parse CLI input, execute one preflight, and print one JSON object."""
    args = _argument_parser().parse_args(argv)
    try:
        request = PreflightRequest.from_json(args.request_json)
    except ValueError:
        print(_cli_failure_json(setup_id=None, code="mqtt_payload", detail=_PAYLOAD_DETAIL))
        return 1
    try:
        if settings_factory is not None:
            settings = settings_factory()
        elif args.environment_file is not None:
            settings = _settings_from_environment_file(args.environment_file)
        else:
            settings = Settings.from_env()
    except Exception:
        print(
            _cli_failure_json(
                setup_id=request.setup_id,
                code="settings_invalid",
                detail=_SETTINGS_DETAIL,
            )
        )
        return 1
    report = await runner(settings, request)
    print(report.to_json())
    return 0 if report.success else 1


def main(argv: Sequence[str] | None = None) -> int:
    """Synchronous CLI entry point."""
    return asyncio.run(async_main(argv))


if __name__ == "__main__":
    raise SystemExit(main())
