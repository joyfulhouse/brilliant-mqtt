"""Bidirectional scene and mode transport on the shared bus/MQTT sessions."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import tempfile
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass, replace
from functools import partial
from pathlib import Path
from typing import cast
from uuid import UUID

from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.ha_control_protocol import (
    COMMAND_TTL_MS,
    MAPPING_VERSION,
    SCHEMA_VERSION,
    ModeCommand,
    SceneCommand,
    decode_mode_command,
    decode_scene_command,
    encode_json,
    mode_catalog_topic,
    mode_command_topic,
    mode_event_topic,
    mode_result_topic,
    scene_catalog_topic,
    scene_command_topic,
    scene_event_topic,
    scene_result_topic,
    transport_status_topic,
    validate_mode_command_context,
    validate_scene_command_context,
)
from brilliant_mqtt.model import BrilliantDevice
from brilliant_mqtt.protocols import BusClient, MqttClient
from brilliant_mqtt.scene_codec import (
    ModeExecution,
    SceneExecution,
    decode_mode_catalog,
    decode_mode_execution,
    decode_scene_catalog,
    decode_scene_execution,
)

logger = logging.getLogger(__name__)

_CONFIGURATION_DEVICE_ID = "configuration_virtual_device"
_EXECUTION_PERIPHERAL_ID = "execution_peripheral"
_SCENE_EXECUTION_PREFIX = "execution_state:scene_execution_handler:scene:"
_RESULT_CACHE_LIMIT = 1_024
_EVENT_OUTBOX_LIMIT = 1_024
_RESULT_EXPIRY_MS = 10 * 60 * 1_000
_RESULT_RETRY_SECONDS = 1.0
_SHUTDOWN_DRAIN_SECONDS = 0.05
_TIMEOUT_SECONDS = COMMAND_TTL_MS / 1_000


@dataclass(frozen=True, slots=True)
class Watermark:
    """Newest durable execution identity for one panel scene."""

    executed_at_ms: int
    payload_sha256: str


@dataclass(slots=True)
class _Pending:
    value: str
    fingerprint: str
    panel: str
    issued_at_ms: int
    timeout_task: asyncio.Task[None] | None
    write_task: asyncio.Task[None] | None = None


@dataclass(slots=True)
class _StoredEvent:
    topic: str
    payload: str
    delivered: bool
    created_at_ms: int


@dataclass(slots=True)
class _StoredResult:
    kind: str
    command_id: str
    fingerprint: str
    command_panel: str
    command_value: str
    issued_at_ms: int
    topic: str
    payload: str
    delivered: bool
    expires_at_ms: int
    event_key: str | None


@dataclass(slots=True)
class _StoredPending:
    kind: str
    command_id: str
    value: str
    fingerprint: str
    panel: str
    issued_at_ms: int
    expires_at_ms: int


def _is_new(previous: Watermark | None, current: SceneExecution) -> bool:
    return previous is None or (current.executed_at_ms, current.payload_sha256) > (
        previous.executed_at_ms,
        previous.payload_sha256,
    )


class SceneBridge:
    """Safely bridge panel scene/mode records and commands over existing clients."""

    def __init__(
        self,
        bus: BusClient,
        mqtt: MqttClient,
        panel: str,
        watermark_path: str | Path,
        clock_ms: Callable[[], int],
    ) -> None:
        self._bus = bus
        self._mqtt = mqtt
        self._panel = panel
        self._watermark_path = Path(watermark_path)
        self._clock_ms = clock_ms
        injected_sleep = getattr(clock_ms, "sleep", None)
        self._sleep = cast(
            Callable[[float], Awaitable[None]],
            injected_sleep if callable(injected_sleep) else asyncio.sleep,
        )

        self._lock = asyncio.Lock()
        self._started = False
        self._callbacks_registered = False
        self._subscribed_topics: list[str] = []
        self._execution: BrilliantDevice | None = None
        self._execution_available = False
        self._scene_ids: frozenset[str] = frozenset()
        self._mode_ids: frozenset[str] = frozenset()
        self._watermarks: dict[tuple[str, str], Watermark] = {}
        self._mode_watermarks: dict[str, tuple[int, str]] = {}
        self._scene_pending: dict[str, _Pending] = {}
        self._mode_pending: dict[str, _Pending] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._start_task: asyncio.Task[None] | None = None
        self._events: dict[str, _StoredEvent] = {}
        self._results: OrderedDict[tuple[str, str], _StoredResult] = OrderedDict()
        self._pending_records: dict[tuple[str, str], _StoredPending] = {}
        self._state_trusted = True
        self._state_reason: str | None = None
        self._delivery_task: asyncio.Task[None] | None = None
        self._status: dict[str, tuple[bool, str | None]] = {}
        self._status_pending: dict[str, tuple[bool, str | None]] = {}
        self._status_tasks: dict[str, asyncio.Task[None]] = {}
        self._scene_catalog_healthy = True
        self._scene_execution_healthy = True
        self._mode_catalog_healthy = True
        self._mode_execution_healthy = True
        self._stopping = False
        self._epoch = 0

    async def async_start(self) -> None:
        """Register callbacks, seed history, publish catalogs, and accept commands."""
        async with self._lock:
            if self._started:
                return
            if self._start_task is not None and not self._start_task.done():
                startup_task = self._start_task
            else:
                self._stopping = False
                self._epoch += 1
                if not self._callbacks_registered:
                    self._bus.on_change(self._bus_change_callback)
                    self._bus.on_reconnect(self._reconnect_callback)
                    self._mqtt.on_message(self._mqtt_message_callback)
                    self._callbacks_registered = True
                self._load_state()
                self._restore_pending_maps()
                startup_task = self._track_task(self._async_start_io(self._epoch))
                self._start_task = startup_task
                startup_task.add_done_callback(self._start_task_done)
        try:
            await startup_task
        except asyncio.CancelledError:
            if self._stopping:
                return
            raise

    def _start_task_done(self, task: asyncio.Task[None]) -> None:
        if self._start_task is task:
            self._start_task = None

    async def _async_start_io(self, epoch: int) -> None:
        subscribed: list[str] = []
        try:
            for topic in (scene_command_topic(self._panel), mode_command_topic(self._panel)):
                await self._mqtt.subscribe(topic)
                subscribed.append(topic)
                async with self._lock:
                    invalidated = epoch != self._epoch or self._stopping
                    if not invalidated:
                        self._subscribed_topics.append(topic)
                if invalidated:
                    await self._mqtt.unsubscribe(topic)
                    subscribed.remove(topic)
                    return
            await self._async_reconcile_work(epoch, emit_history=False, during_start=True)
        except BaseException:
            if not self._stopping:
                async with self._lock:
                    for topic in subscribed:
                        if topic in self._subscribed_topics:
                            self._subscribed_topics.remove(topic)
                for topic in reversed(subscribed):
                    try:
                        await self._mqtt.unsubscribe(topic)
                    except Exception:
                        logger.exception("scene bridge startup unsubscribe failed")
            raise
        async with self._lock:
            if epoch != self._epoch or self._stopping:
                return
            self._started = True
            self._schedule_pending_deadlines()
            self._schedule_delivery()
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    async def async_reconcile(self) -> None:
        """Re-read execution state and both scoped catalogs after reconnect."""
        epoch = self._epoch
        if not self._started or self._stopping:
            return
        await self._async_reconcile_work(epoch, emit_history=True, during_start=False)

    async def async_shutdown(self) -> None:
        """Fence callbacks, cancel deadlines, unsubscribe exact topics, and flush."""
        if self._stopping:
            return
        self._stopping = True
        self._started = False
        self._epoch += 1
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.wait(tasks, timeout=_SHUTDOWN_DRAIN_SECONDS)
        async with self._lock:
            if not self._subscribed_topics:
                return
            topics = list(self._subscribed_topics)
            self._subscribed_topics.clear()
            self._scene_pending.clear()
            self._mode_pending.clear()
        for topic in topics:
            try:
                await self._mqtt.unsubscribe(topic)
            except Exception:
                logger.exception("scene bridge unsubscribe failed; continuing")
        async with self._lock:
            self._persist_state()

    async def _bus_change_callback(self, device: BrilliantDevice) -> None:
        self._spawn_callback(self._async_bus_change(device))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    async def _reconnect_callback(self) -> None:
        self._spawn_callback(self.async_reconcile())
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    async def _mqtt_message_callback(self, topic: str, payload: str, retained: bool) -> None:
        self._spawn_callback(self._async_mqtt_message(topic, payload, retained))
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    def _spawn_callback(self, coroutine: Coroutine[object, object, None]) -> None:
        if not self._started or self._stopping:
            coroutine.close()
            return
        self._track_task(coroutine)

    def _restore_pending_maps(self) -> None:
        self._scene_pending.clear()
        self._mode_pending.clear()
        for (kind, command_id), record in self._pending_records.items():
            pending = _Pending(
                record.value,
                record.fingerprint,
                record.panel,
                record.issued_at_ms,
                None,
            )
            target = self._scene_pending if kind == "scene" else self._mode_pending
            target[command_id] = pending

    def _schedule_pending_deadlines(self) -> None:
        for (kind, command_id), record in self._pending_records.items():
            target = self._scene_pending if kind == "scene" else self._mode_pending
            pending = target.get(command_id)
            if pending is None or pending.timeout_task is not None:
                continue
            delay = max(0.0, (record.expires_at_ms - self._clock_ms()) / 1_000)
            pending.timeout_task = self._track_task(self._async_timeout(kind, command_id, delay))

    async def _async_reconcile_work(
        self, epoch: int, *, emit_history: bool, during_start: bool
    ) -> None:
        devices = await self._bus.get_all()
        execution = next(
            (device for device in devices if device.peripheral_id == _EXECUTION_PERIPHERAL_ID),
            None,
        )
        if epoch != self._epoch or self._stopping:
            return
        scene_ids, scene_healthy, mode_ids, mode_healthy = await self._async_read_catalogs(epoch)
        async with self._lock:
            if epoch != self._epoch or self._stopping:
                return
            if not during_start and not self._started:
                return
            self._execution = execution
            self._execution_available = execution is not None
            self._scene_ids = scene_ids
            self._scene_catalog_healthy = scene_healthy
            self._mode_ids = mode_ids
            self._mode_catalog_healthy = mode_healthy
            if execution is not None:
                await self._async_process_execution(execution, emit_events=emit_history)
            else:
                await self._async_health_status("scene")
                await self._async_health_status("mode")
            await self._async_health_status("scene")
            await self._async_health_status("mode")
            self._schedule_delivery()

    async def _async_read_catalogs(
        self,
        epoch: int,
    ) -> tuple[frozenset[str], bool, frozenset[str], bool]:
        scene_ids: frozenset[str] = frozenset()
        scene_healthy = False
        try:
            scene_device = await self._bus.get_peripheral(
                _CONFIGURATION_DEVICE_ID, "scene_configuration"
            )
            if epoch != self._epoch or self._stopping:
                raise asyncio.CancelledError
            scenes = () if scene_device is None else decode_scene_catalog(scene_device)
            await self._mqtt.publish(
                scene_catalog_topic(self._panel),
                encode_json(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "mapping_version": MAPPING_VERSION,
                        "panel": self._panel,
                        "generated_at_ms": self._clock_ms(),
                        "scenes": [
                            {
                                "scene_id": item.scene_id,
                                "display_name": item.display_name,
                                "icon": item.icon,
                            }
                            for item in scenes
                        ],
                    }
                ),
                retain=True,
            )
            scene_ids = frozenset(item.scene_id for item in scenes)
            scene_healthy = True
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("malformed scene catalog")

        mode_ids: frozenset[str] = frozenset()
        mode_healthy = False
        if epoch != self._epoch or self._stopping:
            raise asyncio.CancelledError
        try:
            mode_device = await self._bus.get_peripheral(
                _CONFIGURATION_DEVICE_ID, "mode_configuration"
            )
            if epoch != self._epoch or self._stopping:
                raise asyncio.CancelledError
            modes = () if mode_device is None else decode_mode_catalog(mode_device)
            await self._mqtt.publish(
                mode_catalog_topic(self._panel),
                encode_json(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "mapping_version": MAPPING_VERSION,
                        "panel": self._panel,
                        "generated_at_ms": self._clock_ms(),
                        "modes": [
                            {"mode_id": item.mode_id, "display_name": item.display_name}
                            for item in modes
                        ],
                    }
                ),
                retain=True,
            )
            mode_ids = frozenset(item.mode_id for item in modes)
            mode_healthy = True
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("malformed mode catalog")
        return scene_ids, scene_healthy, mode_ids, mode_healthy

    async def _async_bus_change(self, device: BrilliantDevice) -> None:
        if device.peripheral_id != _EXECUTION_PERIPHERAL_ID:
            return
        async with self._lock:
            if not self._started:
                return
            self._execution = device
            self._execution_available = True
            try:
                await self._async_process_execution(device, emit_events=True)
            except Exception:
                logger.exception("scene bridge execution callback failed; continuing")

    async def _async_process_execution(self, device: BrilliantDevice, *, emit_events: bool) -> None:
        seed_only = not self._state_trusted
        scenes, scene_malformed = _decode_scene_records(device)
        self._scene_execution_healthy = not scene_malformed
        if scene_malformed:
            logger.warning("malformed scene execution record")
        await self._async_health_status("scene")
        for execution in scenes:
            await self._async_scene_execution(
                execution,
                emit_event=emit_events and not seed_only,
                persist=not seed_only,
            )
        await self._async_health_status("scene")

        mode_malformed = False
        try:
            modes = decode_mode_execution(device)
        except Exception:
            mode_malformed = True
            logger.exception("malformed mode execution record")
            self._mode_execution_healthy = False
            await self._async_health_status("mode")
        else:
            self._mode_execution_healthy = True
            await self._async_health_status("mode")
            for mode_execution in modes:
                await self._async_mode_execution(
                    mode_execution,
                    emit_event=emit_events and not seed_only,
                    persist=not seed_only,
                )
        await self._async_health_status("mode")
        if (seed_only or not emit_events) and not scene_malformed and not mode_malformed:
            self._persist_state()

    async def _async_scene_execution(
        self, execution: SceneExecution, *, emit_event: bool, persist: bool = True
    ) -> None:
        key = (self._panel, execution.scene_id)
        previous = self._watermarks.get(key)
        if not _is_new(previous, execution):
            return
        event_payload = encode_json(
            {
                "schema_version": SCHEMA_VERSION,
                "mapping_version": MAPPING_VERSION,
                "panel": self._panel,
                "scene_id": execution.scene_id,
                "executed_at_ms": execution.executed_at_ms,
                "deduplication_key": (
                    f"{self._panel}:{execution.scene_id}:{execution.executed_at_ms}"
                ),
            }
        )
        event_key = f"scene:{self._panel}:{execution.scene_id}:{execution.executed_at_ms}"
        matching = [
            (command_id, pending)
            for command_id, pending in self._scene_pending.items()
            if pending.value == execution.scene_id
        ]
        publish_event = emit_event or bool(matching)
        if publish_event and not self._reserve_event(event_key):
            return
        self._watermarks[key] = Watermark(execution.executed_at_ms, execution.payload_sha256)
        if publish_event:
            self._events[event_key] = _StoredEvent(
                scene_event_topic(self._panel), event_payload, False, self._clock_ms()
            )
            for command_id, pending in matching:
                self._scene_pending.pop(command_id, None)
                self._pending_records.pop(("scene", command_id), None)
                if pending.timeout_task is not None:
                    pending.timeout_task.cancel()
                self._store_result(
                    "scene",
                    command_id,
                    execution.scene_id,
                    pending.fingerprint,
                    pending.panel,
                    pending.issued_at_ms,
                    accepted=True,
                    error=None,
                    event_key=event_key,
                )
        if persist and self._persist_state():
            self._schedule_delivery()

    async def _async_mode_execution(
        self, execution: ModeExecution, *, emit_event: bool, persist: bool = True
    ) -> None:
        current = (execution.executed_at_ms, execution.mode_id)
        previous = self._mode_watermarks.get(self._panel)
        if previous is not None and current <= previous:
            return
        event_payload = encode_json(
            {
                "schema_version": SCHEMA_VERSION,
                "mapping_version": MAPPING_VERSION,
                "panel": self._panel,
                "mode_id": execution.mode_id,
                "executed_at_ms": execution.executed_at_ms,
                "deduplication_key": (
                    f"{self._panel}:{execution.mode_id}:{execution.executed_at_ms}"
                ),
            }
        )
        event_key = f"mode:{self._panel}:{execution.mode_id}:{execution.executed_at_ms}"
        matching = [
            (command_id, pending)
            for command_id, pending in self._mode_pending.items()
            if pending.value == execution.mode_id
        ]
        publish_event = emit_event or bool(matching)
        if publish_event and not self._reserve_event(event_key):
            return
        self._mode_watermarks[self._panel] = current
        if publish_event:
            self._events[event_key] = _StoredEvent(
                mode_event_topic(self._panel), event_payload, False, self._clock_ms()
            )
            for command_id, pending in matching:
                self._mode_pending.pop(command_id, None)
                self._pending_records.pop(("mode", command_id), None)
                if pending.timeout_task is not None:
                    pending.timeout_task.cancel()
                self._store_result(
                    "mode",
                    command_id,
                    execution.mode_id,
                    pending.fingerprint,
                    pending.panel,
                    pending.issued_at_ms,
                    accepted=True,
                    error=None,
                    event_key=event_key,
                )
        if persist and self._persist_state():
            self._schedule_delivery()

    async def _async_mqtt_message(self, topic: str, payload: str, retained: bool) -> None:
        if topic not in (scene_command_topic(self._panel), mode_command_topic(self._panel)):
            return
        async with self._lock:
            if not self._started:
                return
            try:
                write_scheduled = await self._async_command(topic, payload, retained)
            except Exception:
                logger.exception("scene bridge command callback failed; continuing")
                return
        if write_scheduled:
            # Let an immediate fake/adapter write begin without making the
            # shared MQTT reader await a potentially hung bus RPC.
            await asyncio.sleep(0)

    async def _async_command(self, topic: str, payload: str, retained: bool) -> bool:
        kind = "scene" if topic == scene_command_topic(self._panel) else "mode"
        command: SceneCommand | ModeCommand
        try:
            if kind == "scene":
                command = decode_scene_command(payload, now_ms=self._clock_ms())
                validate_scene_command_context(command, topic_panel=self._panel, retained=retained)
                value = command.scene_id
                known = value in self._scene_ids
            else:
                kind = "mode"
                command = decode_mode_command(payload, now_ms=self._clock_ms())
                validate_mode_command_context(command, topic_panel=self._panel, retained=retained)
                value = command.mode_id
                known = value in self._mode_ids
        except ValueError:
            return False

        fingerprint = _command_fingerprint(kind, command)
        cache_key = (kind, command.command_id)
        if not self._state_trusted:
            await self._async_health_status(kind)
            return False
        cached = self._results.get(cache_key)
        if cached is not None:
            if not known or cached.fingerprint != fingerprint:
                return False
            if cached.delivered:
                cached.event_key = None
            cached.delivered = False
            if self._persist_state():
                self._schedule_delivery()
            return False
        stored_pending = self._pending_records.get(cache_key)
        if stored_pending is not None:
            return False
        pending = self._scene_pending if kind == "scene" else self._mode_pending
        existing = pending.get(command.command_id)
        if existing is not None:
            return False

        if not known:
            await self._async_result(
                kind,
                command.command_id,
                value,
                fingerprint,
                command.panel,
                command.issued_at_ms,
                accepted=False,
                error=f"unknown_{kind}",
            )
            return False
        if self._execution is None:
            await self._async_result(
                kind,
                command.command_id,
                value,
                fingerprint,
                command.panel,
                command.issued_at_ms,
                accepted=False,
                error="execution_unavailable",
            )
            return False
        if not self._reserve_result(cache_key):
            await self._async_health_status(kind)
            return False

        variable = "last_executed_scene_id" if kind == "scene" else "manual_mode_id"
        record = _StoredPending(
            kind,
            command.command_id,
            value,
            fingerprint,
            command.panel,
            command.issued_at_ms,
            self._clock_ms() + COMMAND_TTL_MS,
        )
        self._pending_records[cache_key] = record
        current = _Pending(value, fingerprint, command.panel, command.issued_at_ms, None)
        pending[command.command_id] = current
        if not self._persist_state():
            pending.pop(command.command_id, None)
            self._pending_records.pop(cache_key, None)
            await self._async_health_status(kind)
            return False
        timeout_task = self._track_task(
            self._async_timeout(kind, command.command_id, _TIMEOUT_SECONDS)
        )
        current.timeout_task = timeout_task
        write_task = self._track_task(
            self._async_write(
                kind,
                command.command_id,
                self._execution.device_id,
                variable,
                value,
            )
        )
        current.write_task = write_task
        return True

    async def _async_write(
        self,
        kind: str,
        command_id: str,
        device_id: str,
        variable: str,
        value: str,
    ) -> None:
        try:
            await self._bus.set_variables(
                device_id,
                _EXECUTION_PERIPHERAL_ID,
                [VarSet(name=variable, value=value)],
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            logger.warning("scene bridge write failed (%s)", type(error).__name__)
            async with self._lock:
                pending = self._scene_pending if kind == "scene" else self._mode_pending
                current = pending.get(command_id)
                if current is None or current.write_task is not asyncio.current_task():
                    return
                pending.pop(command_id)
                self._pending_records.pop((kind, command_id), None)
                if current.timeout_task is not None:
                    current.timeout_task.cancel()
                await self._async_result(
                    kind,
                    command_id,
                    value,
                    current.fingerprint,
                    current.panel,
                    current.issued_at_ms,
                    accepted=False,
                    error="write_failed",
                )

    async def _async_timeout(self, kind: str, command_id: str, delay_seconds: float) -> None:
        try:
            await self._sleep(delay_seconds)
            async with self._lock:
                if not self._started:
                    return
                pending_map = self._scene_pending if kind == "scene" else self._mode_pending
                pending = pending_map.pop(command_id, None)
                if pending is not None:
                    self._pending_records.pop((kind, command_id), None)
                    if pending.write_task is not None:
                        pending.write_task.cancel()
                    await self._async_result(
                        kind,
                        command_id,
                        pending.value,
                        pending.fingerprint,
                        pending.panel,
                        pending.issued_at_ms,
                        accepted=False,
                        error="timeout",
                    )
        except asyncio.CancelledError:
            raise

    async def _async_result(
        self,
        kind: str,
        command_id: str,
        value: str,
        fingerprint: str,
        command_panel: str,
        issued_at_ms: int,
        *,
        accepted: bool,
        error: str | None,
        event_key: str | None = None,
    ) -> None:
        self._store_result(
            kind,
            command_id,
            value,
            fingerprint,
            command_panel,
            issued_at_ms,
            accepted=accepted,
            error=error,
            event_key=event_key,
        )
        if self._persist_state():
            self._schedule_delivery()

    def _store_result(
        self,
        kind: str,
        command_id: str,
        value: str,
        fingerprint: str,
        command_panel: str,
        issued_at_ms: int,
        *,
        accepted: bool,
        error: str | None,
        event_key: str | None,
    ) -> bool:
        cache_key = (kind, command_id)
        if not self._reserve_result(cache_key):
            return False
        body: dict[str, object] = {
            "schema_version": SCHEMA_VERSION,
            "mapping_version": MAPPING_VERSION,
            "command_id": command_id,
            "panel": command_panel,
            f"{kind}_id": value,
            "accepted": accepted,
            "timestamp_ms": self._clock_ms(),
        }
        if error is not None:
            body["error"] = error
        payload = encode_json(body)
        topic = scene_result_topic(command_id) if kind == "scene" else mode_result_topic(command_id)
        self._results[cache_key] = _StoredResult(
            kind=kind,
            command_id=command_id,
            fingerprint=fingerprint,
            command_panel=command_panel,
            command_value=value,
            issued_at_ms=issued_at_ms,
            topic=topic,
            payload=payload,
            delivered=False,
            expires_at_ms=self._clock_ms() + _RESULT_EXPIRY_MS,
            event_key=event_key,
        )
        self._results.move_to_end(cache_key)
        return True

    def _reserve_result(self, cache_key: tuple[str, str]) -> bool:
        if cache_key in self._results:
            return True
        now = self._clock_ms()
        for key, result in list(self._results.items()):
            if result.delivered and result.expires_at_ms <= now:
                self._results.pop(key)
        pending_count = len(self._pending_records)
        while len(self._results) + pending_count >= _RESULT_CACHE_LIMIT:
            removable = next(
                (key for key, result in self._results.items() if result.delivered),
                None,
            )
            if removable is None:
                self._state_reason = "state_capacity"
                return False
            self._results.pop(removable)
        self._clear_capacity_if_room()
        return True

    def _reserve_event(self, event_key: str) -> bool:
        if event_key in self._events:
            return True
        self._prune_events()
        if len(self._events) >= _EVENT_OUTBOX_LIMIT:
            self._state_reason = "state_capacity"
            return False
        self._clear_capacity_if_room()
        return True

    def _prune_events(self) -> None:
        dependencies = {
            result.event_key
            for result in self._results.values()
            if not result.delivered and result.event_key is not None
        }
        for key, event in list(self._events.items()):
            if event.delivered and key not in dependencies:
                self._events.pop(key)

    def _schedule_delivery(self) -> None:
        if not self._started or self._stopping:
            return
        if self._delivery_task is not None and not self._delivery_task.done():
            return
        self._delivery_task = self._track_task(self._async_delivery_loop(self._epoch))

    async def _async_delivery_loop(self, epoch: int) -> None:
        while self._started and not self._stopping and epoch == self._epoch:
            async with self._lock:
                item = self._next_delivery()
            if item is None:
                return
            item_type, key, topic, payload = item
            try:
                await self._mqtt.publish(topic, payload, retain=False)
            except asyncio.CancelledError:
                raise
            except Exception:
                await self._sleep(_RESULT_RETRY_SECONDS)
                continue
            async with self._lock:
                if self._stopping or epoch != self._epoch:
                    return
                if item_type == "event":
                    event = self._events.get(cast(str, key))
                    if event is not None and event.payload == payload:
                        event.delivered = True
                else:
                    result = self._results.get(cast(tuple[str, str], key))
                    if result is not None and result.payload == payload:
                        result.delivered = True
                if not self._persist_state():
                    return
                self._prune_events()
                self._clear_capacity_if_room()
                await self._async_health_status("scene")
                await self._async_health_status("mode")

    def _next_delivery(
        self,
    ) -> tuple[str, str | tuple[str, str], str, str] | None:
        events = sorted(self._events.items(), key=lambda item: item[1].created_at_ms)
        for key, event in events:
            if not event.delivered:
                return ("event", key, event.topic, event.payload)
        for result_key, result in self._results.items():
            if result.delivered:
                continue
            if result.event_key is not None:
                dependency_event = self._events.get(result.event_key)
                if dependency_event is None or not dependency_event.delivered:
                    continue
            return ("result", result_key, result.topic, result.payload)
        return None

    def _clear_capacity_if_room(self) -> None:
        if (
            self._state_reason == "state_capacity"
            and len(self._events) < _EVENT_OUTBOX_LIMIT
            and len(self._results) + len(self._pending_records) < _RESULT_CACHE_LIMIT
        ):
            self._state_reason = None

    def _track_task(self, coroutine: Coroutine[object, object, None]) -> asyncio.Task[None]:
        task: asyncio.Task[None] = asyncio.create_task(coroutine)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def _async_status(self, transport: str, available: bool, reason: str | None) -> None:
        current = (available, reason)
        if self._status.get(transport) == current or self._status_pending.get(transport) == current:
            return
        prior_task = self._status_tasks.get(transport)
        if prior_task is not None:
            prior_task.cancel()
        self._status_pending[transport] = current
        payload = encode_json(
            {
                "schema_version": SCHEMA_VERSION,
                "mapping_version": MAPPING_VERSION,
                "transport": transport,
                "panel": self._panel,
                "available": available,
                "reason": reason,
                "timestamp_ms": self._clock_ms(),
            }
        )
        task = self._track_task(
            self._async_publish_status(transport, current, payload, self._epoch)
        )
        self._status_tasks[transport] = task
        task.add_done_callback(partial(self._status_task_done, transport))

    async def _async_publish_status(
        self,
        transport: str,
        current: tuple[bool, str | None],
        payload: str,
        epoch: int,
    ) -> None:
        published = False
        current_completion = False
        try:
            await self._mqtt.publish(
                transport_status_topic(transport, self._panel),
                payload,
                retain=True,
            )
            published = True
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("scene bridge status publication failed")
        finally:
            async with self._lock:
                if epoch == self._epoch and self._status_pending.get(transport) == current:
                    self._status_pending.pop(transport)
                    current_completion = True
        async with self._lock:
            if published and current_completion and epoch == self._epoch:
                self._status[transport] = current

    def _status_task_done(self, transport: str, task: asyncio.Task[None]) -> None:
        if self._status_tasks.get(transport) is task:
            self._status_tasks.pop(transport)

    async def _async_health_status(self, transport: str) -> None:
        if transport == "scene":
            available = self._scene_catalog_healthy and self._scene_execution_healthy
        else:
            available = self._mode_catalog_healthy and self._mode_execution_healthy
        if not self._state_trusted or self._state_reason is not None:
            available = False
            reason: str | None = self._state_reason or "state_untrusted"
        elif not self._execution_available:
            available = False
            reason = "execution_unavailable"
        else:
            reason = None if available else "malformed_data"
        await self._async_status(transport, available, reason)

    def _load_state(self) -> None:
        self._watermarks.clear()
        self._mode_watermarks.clear()
        self._events.clear()
        self._results.clear()
        self._pending_records.clear()
        self._state_reason = None
        try:
            raw = json.loads(self._watermark_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            self._state_trusted = True
            return
        except (OSError, UnicodeError, json.JSONDecodeError):
            self._state_trusted = False
            self._state_reason = "state_untrusted"
            return
        try:
            self._parse_state(raw)
            os.chmod(self._watermark_path, 0o600)
            os.chmod(self._watermark_path.parent, 0o700)
        except (OSError, ValueError):
            self._watermarks.clear()
            self._mode_watermarks.clear()
            self._events.clear()
            self._results.clear()
            self._pending_records.clear()
            self._state_trusted = False
            self._state_reason = "state_untrusted"
            return
        self._state_trusted = True
        self._prune_delivered_results()
        if (
            len(self._events) >= _EVENT_OUTBOX_LIMIT
            and all(not event.delivered for event in self._events.values())
        ) or (
            len(self._results) + len(self._pending_records) >= _RESULT_CACHE_LIMIT
            and all(not result.delivered for result in self._results.values())
        ):
            self._state_reason = "state_capacity"

    def _parse_state(self, raw: object) -> None:
        if not isinstance(raw, dict) or set(raw) != {
            "version",
            "watermarks",
            "mode_watermarks",
            "events",
            "results",
            "pending",
        }:
            raise ValueError("invalid state schema")
        if type(raw["version"]) is not int or raw["version"] != 1:
            raise ValueError("invalid state version")
        watermarks = raw["watermarks"]
        mode_watermarks = raw["mode_watermarks"]
        events, results, pending_records = raw["events"], raw["results"], raw["pending"]
        if not all(
            isinstance(item, dict)
            for item in (watermarks, mode_watermarks, events, results, pending_records)
        ):
            raise ValueError("invalid state collections")
        if (
            len(cast(dict[object, object], events)) > _EVENT_OUTBOX_LIMIT
            or len(cast(dict[object, object], results))
            + len(cast(dict[object, object], pending_records))
            > _RESULT_CACHE_LIMIT
        ):
            raise ValueError("state collections exceed limits")
        for panel, panel_records in cast(dict[object, object], watermarks).items():
            if not isinstance(panel, str) or not isinstance(panel_records, dict):
                raise ValueError("invalid watermark panel")
            for scene_id, value in panel_records.items():
                if (
                    not isinstance(scene_id, str)
                    or not isinstance(value, dict)
                    or set(value)
                    != {
                        "executed_at_ms",
                        "payload_sha256",
                    }
                ):
                    raise ValueError("invalid watermark entry")
                executed_at_ms, payload_sha256 = value["executed_at_ms"], value["payload_sha256"]
                if not (
                    type(executed_at_ms) is int
                    and executed_at_ms >= 0
                    and isinstance(payload_sha256, str)
                    and _is_sha256(payload_sha256)
                ):
                    raise ValueError("invalid watermark value")
                self._watermarks[(panel, scene_id)] = Watermark(executed_at_ms, payload_sha256)
        for panel, value in cast(dict[object, object], mode_watermarks).items():
            if (
                not isinstance(panel, str)
                or not isinstance(value, dict)
                or set(value) != {"executed_at_ms", "mode_id"}
            ):
                raise ValueError("invalid mode watermark entry")
            executed_at_ms, mode_id = value["executed_at_ms"], value["mode_id"]
            if not (
                type(executed_at_ms) is int
                and executed_at_ms >= 0
                and isinstance(mode_id, str)
                and mode_id
            ):
                raise ValueError("invalid mode watermark value")
            self._mode_watermarks[panel] = (executed_at_ms, mode_id)
        for event_key, value in cast(dict[object, object], events).items():
            if (
                not isinstance(event_key, str)
                or not isinstance(value, dict)
                or set(value)
                != {
                    "topic",
                    "payload",
                    "delivered",
                    "created_at_ms",
                }
            ):
                raise ValueError("invalid event entry")
            topic, payload = value["topic"], value["payload"]
            delivered, created_at_ms = value["delivered"], value["created_at_ms"]
            if not (
                isinstance(topic, str)
                and isinstance(payload, str)
                and type(delivered) is bool
                and type(created_at_ms) is int
                and created_at_ms >= 0
            ):
                raise ValueError("invalid event value")
            decoded_event = json.loads(payload)
            if not isinstance(decoded_event, dict):
                raise ValueError("invalid event payload")
            event_panel = decoded_event.get("panel")
            executed_at_ms = decoded_event.get("executed_at_ms")
            if not (
                isinstance(event_panel, str)
                and type(decoded_event.get("schema_version")) is int
                and decoded_event["schema_version"] == SCHEMA_VERSION
                and type(decoded_event.get("mapping_version")) is int
                and decoded_event["mapping_version"] == MAPPING_VERSION
                and type(executed_at_ms) is int
                and executed_at_ms >= 0
            ):
                raise ValueError("invalid event panel")
            if "scene_id" in decoded_event:
                kind = "scene"
                identifier = decoded_event["scene_id"]
                expected_topic = scene_event_topic(event_panel)
            elif "mode_id" in decoded_event:
                kind = "mode"
                identifier = decoded_event["mode_id"]
                expected_topic = mode_event_topic(event_panel)
            else:
                raise ValueError("invalid event kind")
            if not isinstance(identifier, str) or not identifier:
                raise ValueError("invalid event identifier")
            if set(decoded_event) != {
                "schema_version",
                "mapping_version",
                "panel",
                f"{kind}_id",
                "executed_at_ms",
                "deduplication_key",
            }:
                raise ValueError("invalid event payload fields")
            deduplication_key = f"{event_panel}:{identifier}:{executed_at_ms}"
            if (
                topic != expected_topic
                or decoded_event.get("deduplication_key") != deduplication_key
                or event_key != f"{kind}:{deduplication_key}"
            ):
                raise ValueError("invalid event topic")
            self._events[event_key] = _StoredEvent(topic, payload, delivered, created_at_ms)
        for result_key, value in cast(dict[object, object], results).items():
            required = {
                "kind",
                "command_id",
                "fingerprint",
                "command_panel",
                "command_value",
                "issued_at_ms",
                "topic",
                "payload",
                "delivered",
                "expires_at_ms",
                "event_key",
            }
            if (
                not isinstance(result_key, str)
                or not isinstance(value, dict)
                or set(value) != required
            ):
                raise ValueError("invalid result entry")
            kind, command_id = value["kind"], value["command_id"]
            fingerprint, topic, payload = value["fingerprint"], value["topic"], value["payload"]
            command_panel = value["command_panel"]
            command_value, issued_at_ms = value["command_value"], value["issued_at_ms"]
            delivered, expires_at_ms = value["delivered"], value["expires_at_ms"]
            event_key = value["event_key"]
            if not (
                kind in ("scene", "mode")
                and isinstance(command_id, str)
                and result_key == f"{kind}:{command_id}"
                and isinstance(fingerprint, str)
                and _is_sha256(fingerprint)
                and isinstance(command_panel, str)
                and isinstance(command_value, str)
                and command_value
                and type(issued_at_ms) is int
                and issued_at_ms >= 0
                and isinstance(topic, str)
                and isinstance(payload, str)
                and type(delivered) is bool
                and type(expires_at_ms) is int
                and expires_at_ms >= 0
                and (event_key is None or isinstance(event_key, str))
            ):
                raise ValueError("invalid result value")
            UUID(command_id)
            if kind == "scene":
                scene_command_topic(command_panel)
            else:
                mode_command_topic(command_panel)
            if fingerprint != _command_fingerprint_fields(
                cast(str, kind), command_id, command_panel, command_value, issued_at_ms
            ):
                raise ValueError("invalid result fingerprint")
            decoded_result = json.loads(payload)
            expected_topic = (
                scene_result_topic(command_id) if kind == "scene" else mode_result_topic(command_id)
            )
            expected_fields = {
                "schema_version",
                "mapping_version",
                "command_id",
                "panel",
                f"{kind}_id",
                "accepted",
                "timestamp_ms",
            }
            if isinstance(decoded_result, dict) and decoded_result.get("accepted") is False:
                expected_fields.add("error")
            result_identifier = (
                decoded_result.get(f"{kind}_id") if isinstance(decoded_result, dict) else None
            )
            if (
                topic != expected_topic
                or not isinstance(decoded_result, dict)
                or decoded_result.get("command_id") != command_id
                or set(decoded_result) != expected_fields
                or type(decoded_result.get("schema_version")) is not int
                or decoded_result["schema_version"] != SCHEMA_VERSION
                or type(decoded_result.get("mapping_version")) is not int
                or decoded_result["mapping_version"] != MAPPING_VERSION
                or decoded_result.get("panel") != command_panel
                or not isinstance(result_identifier, str)
                or result_identifier != command_value
                or type(decoded_result.get("accepted")) is not bool
                or type(decoded_result.get("timestamp_ms")) is not int
                or decoded_result["timestamp_ms"] < 0
            ):
                raise ValueError("invalid result payload")
            if decoded_result["accepted"] is False:
                error = decoded_result.get("error")
                if not isinstance(error, str) or not error:
                    raise ValueError("invalid result error")
            normalized_kind = cast(str, kind)
            self._results[(normalized_kind, command_id)] = _StoredResult(
                normalized_kind,
                command_id,
                fingerprint,
                command_panel,
                command_value,
                issued_at_ms,
                topic,
                payload,
                delivered,
                expires_at_ms,
                event_key,
            )
        for pending_key, value in cast(dict[object, object], pending_records).items():
            required = {
                "kind",
                "command_id",
                "value",
                "fingerprint",
                "panel",
                "issued_at_ms",
                "expires_at_ms",
            }
            if (
                not isinstance(pending_key, str)
                or not isinstance(value, dict)
                or set(value) != required
            ):
                raise ValueError("invalid pending entry")
            kind, command_id = value["kind"], value["command_id"]
            command_value = value["value"]
            fingerprint, expires_at_ms = value["fingerprint"], value["expires_at_ms"]
            command_panel, issued_at_ms = value["panel"], value["issued_at_ms"]
            if not (
                kind in ("scene", "mode")
                and isinstance(command_id, str)
                and pending_key == f"{kind}:{command_id}"
                and isinstance(command_value, str)
                and command_value
                and isinstance(fingerprint, str)
                and _is_sha256(fingerprint)
                and isinstance(command_panel, str)
                and type(issued_at_ms) is int
                and issued_at_ms >= 0
                and type(expires_at_ms) is int
                and expires_at_ms >= 0
            ):
                raise ValueError("invalid pending value")
            UUID(command_id)
            normalized_kind = cast(str, kind)
            if normalized_kind == "scene":
                scene_command_topic(command_panel)
            else:
                mode_command_topic(command_panel)
            if fingerprint != _command_fingerprint_fields(
                normalized_kind,
                command_id,
                command_panel,
                command_value,
                issued_at_ms,
            ):
                raise ValueError("invalid pending fingerprint")
            key = (normalized_kind, command_id)
            if key in self._results:
                raise ValueError("command cannot be pending and complete")
            self._pending_records[key] = _StoredPending(
                normalized_kind,
                command_id,
                command_value,
                fingerprint,
                command_panel,
                issued_at_ms,
                expires_at_ms,
            )
        for result in self._results.values():
            if (
                not result.delivered
                and result.event_key is not None
                and result.event_key not in self._events
            ):
                raise ValueError("missing result event dependency")

    def _state_payload(self) -> dict[str, object]:
        watermarks: dict[str, dict[str, dict[str, object]]] = {}
        for (panel, scene_id), watermark in sorted(self._watermarks.items()):
            watermarks.setdefault(panel, {})[scene_id] = {
                "executed_at_ms": watermark.executed_at_ms,
                "payload_sha256": watermark.payload_sha256,
            }
        mode_watermarks = {
            panel: {"executed_at_ms": watermark[0], "mode_id": watermark[1]}
            for panel, watermark in sorted(self._mode_watermarks.items())
        }
        events = {
            key: {
                "topic": event.topic,
                "payload": event.payload,
                "delivered": event.delivered,
                "created_at_ms": event.created_at_ms,
            }
            for key, event in sorted(self._events.items())
        }
        results = {
            f"{kind}:{command_id}": {
                "kind": result.kind,
                "command_id": result.command_id,
                "fingerprint": result.fingerprint,
                "command_panel": result.command_panel,
                "command_value": result.command_value,
                "issued_at_ms": result.issued_at_ms,
                "topic": result.topic,
                "payload": result.payload,
                "delivered": result.delivered,
                "expires_at_ms": result.expires_at_ms,
                "event_key": result.event_key,
            }
            for (kind, command_id), result in self._results.items()
        }
        pending = {
            f"{kind}:{command_id}": {
                "kind": record.kind,
                "command_id": record.command_id,
                "value": record.value,
                "fingerprint": record.fingerprint,
                "panel": record.panel,
                "issued_at_ms": record.issued_at_ms,
                "expires_at_ms": record.expires_at_ms,
            }
            for (kind, command_id), record in sorted(self._pending_records.items())
        }
        return {
            "version": 1,
            "watermarks": watermarks,
            "mode_watermarks": mode_watermarks,
            "events": events,
            "results": results,
            "pending": pending,
        }

    def _persist_state(self) -> bool:
        descriptor = -1
        temporary = ""
        try:
            self._watermark_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            os.chmod(self._watermark_path.parent, 0o700)
            descriptor, temporary = tempfile.mkstemp(
                dir=self._watermark_path.parent,
                prefix=f".{self._watermark_path.name}.",
            )
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                descriptor = -1
                json.dump(self._state_payload(), handle, separators=(",", ":"), sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self._watermark_path)
            os.chmod(self._watermark_path, 0o600)
            directory = os.open(self._watermark_path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        except OSError:
            self._state_trusted = False
            self._state_reason = "state_untrusted"
            return False
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            try:
                if temporary:
                    os.unlink(temporary)
            except FileNotFoundError:
                pass
        self._state_trusted = True
        if self._state_reason == "state_untrusted":
            self._state_reason = None
        return True

    def _prune_delivered_results(self) -> None:
        now = self._clock_ms()
        for key, result in list(self._results.items()):
            if result.delivered and result.expires_at_ms <= now:
                self._results.pop(key)
        self._prune_events()


def _command_fingerprint(kind: str, command: SceneCommand | ModeCommand) -> str:
    identifier = command.scene_id if isinstance(command, SceneCommand) else command.mode_id
    return _command_fingerprint_fields(
        kind,
        command.command_id,
        command.panel,
        identifier,
        command.issued_at_ms,
    )


def _command_fingerprint_fields(
    kind: str,
    command_id: str,
    panel: str,
    identifier: str,
    issued_at_ms: int,
) -> str:
    canonical = encode_json(
        {
            "kind": kind,
            "command_id": command_id,
            "panel": panel,
            f"{kind}_id": identifier,
            "issued_at_ms": issued_at_ms,
        }
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def _is_sha256(value: str) -> bool:
    if len(value) != 64 or value != value.lower():
        return False
    try:
        int(value, 16)
    except ValueError:
        return False
    return True


def _decode_scene_records(device: BrilliantDevice) -> tuple[tuple[SceneExecution, ...], bool]:
    records: list[SceneExecution] = []
    malformed = False
    for name, variable in device.variables.items():
        if not name.startswith(_SCENE_EXECUTION_PREFIX):
            continue
        try:
            records.extend(decode_scene_execution(replace(device, variables={name: variable})))
        except Exception:
            malformed = True
    return tuple(sorted(records, key=lambda item: (item.executed_at_ms, item.scene_id))), malformed
