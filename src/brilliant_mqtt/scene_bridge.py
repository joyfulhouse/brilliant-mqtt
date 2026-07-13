"""Bidirectional scene and mode transport on the shared bus/MQTT sessions."""

from __future__ import annotations

import asyncio
import logging
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Coroutine
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from functools import partial
from pathlib import Path
from typing import cast

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
from brilliant_mqtt.scene_state import (
    EVENT_OUTBOX_LIMIT,
    MODE_WATERMARK_LIMIT,
    PENDING_LIMIT,
    RESULT_OUTBOX_LIMIT,
    SCENE_WATERMARK_LIMIT,
    LoadedSceneState,
    SceneState,
    StateEvent,
    StateKey,
    StateKind,
    StatePending,
    StateResult,
    StateWatermark,
    atomic_write_state,
    command_fingerprint_fields,
    load_state,
)

logger = logging.getLogger(__name__)

_CONFIGURATION_DEVICE_ID = "configuration_virtual_device"
_EXECUTION_PERIPHERAL_ID = "execution_peripheral"
_SCENE_EXECUTION_PREFIX = "execution_state:scene_execution_handler:scene:"
_RESULT_CACHE_LIMIT = RESULT_OUTBOX_LIMIT
_EVENT_OUTBOX_LIMIT = EVENT_OUTBOX_LIMIT
_PENDING_LIMIT = PENDING_LIMIT
_SCENE_WATERMARK_LIMIT = SCENE_WATERMARK_LIMIT
_MODE_WATERMARK_LIMIT = MODE_WATERMARK_LIMIT
_RESULT_EXPIRY_MS = 10 * 60 * 1_000
_RESULT_RETRY_SECONDS = 1.0
_SHUTDOWN_DRAIN_SECONDS = 0.05
_UNSUBSCRIBE_TIMEOUT_SECONDS = 0.05
_TIMEOUT_SECONDS = COMMAND_TTL_MS / 1_000

Watermark = StateWatermark
_StoredEvent = StateEvent
_StoredResult = StateResult
_StoredPending = StatePending


@dataclass(slots=True)
class _Pending:
    value: str
    fingerprint: str
    panel: str
    issued_at_ms: int
    timeout_task: asyncio.Task[None] | None
    write_task: asyncio.Task[None] | None = None


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
        self._startup_active = False
        self._startup_buffered_execution: BrilliantDevice | None = None
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
        self._results: OrderedDict[StateKey, _StoredResult] = OrderedDict()
        self._pending_records: dict[StateKey, _StoredPending] = {}
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
        self._operation_generation = 0
        self._state_version = 0
        self._persisted_version = 0
        self._state_executor: ThreadPoolExecutor | None = None

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
                self._startup_active = True
                self._startup_buffered_execution = None
                if self._state_executor is None:
                    self._state_executor = ThreadPoolExecutor(
                        max_workers=1,
                        thread_name_prefix="scene-state",
                    )
                if not self._callbacks_registered:
                    self._bus.on_change(self._bus_change_callback)
                    self._bus.on_reconnect(self._reconnect_callback)
                    self._mqtt.on_message(self._mqtt_message_callback)
                    self._callbacks_registered = True
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
            loaded = await self._async_load_state()
            async with self._lock:
                if epoch != self._epoch or self._stopping:
                    return
                self._state_trusted = loaded.trusted
                self._state_reason = loaded.reason
                self._install_loaded_state(loaded.state)
                self._state_version = 0
                self._persisted_version = 0
                self._restore_pending_maps()
            for topic in (scene_command_topic(self._panel), mode_command_topic(self._panel)):
                await self._mqtt.subscribe(topic)
                subscribed.append(topic)
                self._subscribed_topics.append(topic)
                invalidated = epoch != self._epoch or self._stopping
                if invalidated:
                    await self._async_release_topics(subscribed)
                    return
            await self._async_reconcile_work(epoch, emit_history=False, during_start=True)
        except BaseException:
            await self._async_release_topics(subscribed)
            raise
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    async def async_reconcile(self) -> None:
        """Re-read execution state and both scoped catalogs after reconnect."""
        epoch = self._epoch
        if not self._started or self._stopping:
            return
        await self._async_reconcile_work(epoch, emit_history=True, during_start=False)

    async def async_shutdown(self) -> None:
        """Fence callbacks, bound task drain, and release exact subscriptions."""
        if self._stopping:
            return
        self._stopping = True
        self._started = False
        self._startup_active = False
        self._startup_buffered_execution = None
        self._epoch += 1
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.wait(tasks, timeout=_SHUTDOWN_DRAIN_SECONDS)
        topics = list(self._subscribed_topics)
        async with self._lock:
            self._scene_pending.clear()
            self._mode_pending.clear()
        await self._async_release_topics(topics)
        executor = self._state_executor
        self._state_executor = None
        if executor is not None:
            executor.shutdown(wait=False)

    async def _async_release_topics(self, topics: list[str]) -> None:
        for topic in topics:
            if topic not in self._subscribed_topics:
                continue
            self._subscribed_topics.remove(topic)
            try:
                await asyncio.wait_for(
                    self._mqtt.unsubscribe(topic),
                    timeout=_UNSUBSCRIBE_TIMEOUT_SECONDS,
                )
            except asyncio.TimeoutError:
                logger.warning("scene bridge unsubscribe timed out; continuing")
            except Exception:
                logger.exception("scene bridge unsubscribe failed; continuing")

    async def _bus_change_callback(self, device: BrilliantDevice) -> None:
        task = self._spawn_callback(self._async_bus_change(device))
        if task is not None:
            await asyncio.wait((task,), timeout=_SHUTDOWN_DRAIN_SECONDS)
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    async def _reconnect_callback(self) -> None:
        task = self._spawn_callback(self.async_reconcile())
        if task is not None:
            await asyncio.wait((task,), timeout=_SHUTDOWN_DRAIN_SECONDS)
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    async def _mqtt_message_callback(self, topic: str, payload: str, retained: bool) -> None:
        task = self._spawn_callback(self._async_mqtt_message(topic, payload, retained))
        if task is not None:
            await asyncio.wait((task,), timeout=_SHUTDOWN_DRAIN_SECONDS)
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    def _spawn_callback(
        self, coroutine: Coroutine[object, object, None]
    ) -> asyncio.Task[None] | None:
        if (not self._started and not self._startup_active) or self._stopping:
            coroutine.close()
            return None
        return self._track_task(coroutine)

    async def _async_load_state(self) -> LoadedSceneState:
        executor = self._state_executor
        if executor is None:
            raise RuntimeError("scene state executor unavailable")
        loop = asyncio.get_running_loop()
        loaded: list[LoadedSceneState] = []

        async def run() -> None:
            loaded.append(await loop.run_in_executor(executor, load_state, self._watermark_path))

        await self._track_task(run())
        return loaded[0]

    def _install_loaded_state(self, state: SceneState) -> None:
        self._watermarks = dict(state.watermarks)
        self._mode_watermarks = dict(state.mode_watermarks)
        self._events = dict(state.events)
        self._results = OrderedDict(state.results)
        self._pending_records = dict(state.pending)
        self._prune_delivered_results()
        if not self._has_global_capacity():
            self._state_reason = "state_capacity"

    def _capture_state(self) -> tuple[int, SceneState]:
        self._state_version += 1
        state = SceneState(
            watermarks=tuple(sorted(self._watermarks.items())),
            mode_watermarks=tuple(sorted(self._mode_watermarks.items())),
            events=tuple(sorted(self._events.items())),
            results=tuple(self._results.items()),
            pending=tuple(sorted(self._pending_records.items())),
        )
        return self._state_version, state

    async def _async_persist_state(
        self,
        version: int,
        state: SceneState,
        epoch: int,
    ) -> bool:
        task = self._track_task(self._async_write_state(state))
        try:
            await task
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("scene bridge state persistence failed")
            async with self._lock:
                self._state_trusted = False
                self._state_reason = "state_untrusted"
            return False
        async with self._lock:
            self._persisted_version = max(self._persisted_version, version)
            if epoch != self._epoch or self._stopping:
                return False
            self._state_trusted = True
            if self._state_reason == "state_untrusted":
                self._state_reason = None
            return self._persisted_version >= version

    async def _async_write_state(self, state: SceneState) -> None:
        executor = self._state_executor
        if executor is None:
            raise RuntimeError("scene state executor unavailable")
        loop = asyncio.get_running_loop()
        write = loop.run_in_executor(executor, atomic_write_state, self._watermark_path, state)
        await asyncio.shield(write)

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
        async with self._lock:
            if epoch != self._epoch or self._stopping:
                return
            generation = self._operation_generation
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
            self._scene_ids = scene_ids
            self._scene_catalog_healthy = scene_healthy
            self._mode_ids = mode_ids
            self._mode_catalog_healthy = mode_healthy
            stale = not during_start and generation != self._operation_generation
            if not stale:
                self._execution = execution
                self._execution_available = execution is not None
        if stale:
            await self._async_health_status("scene")
            await self._async_health_status("mode")
            return
        if execution is not None:
            await self._async_process_execution(execution, emit_events=emit_history, epoch=epoch)
        if not during_start:
            await self._async_health_status("scene")
            await self._async_health_status("mode")
            async with self._lock:
                if epoch == self._epoch and self._started and not self._stopping:
                    self._schedule_delivery()
            return

        while True:
            async with self._lock:
                if epoch != self._epoch or self._stopping:
                    return
                buffered = self._startup_buffered_execution
                self._startup_buffered_execution = None
                if buffered is None:
                    self._startup_active = False
                    self._started = True
                    self._schedule_pending_deadlines()
                    self._schedule_delivery()
                    break
                self._execution = buffered
                self._execution_available = True
            await self._async_process_execution(buffered, emit_events=True, epoch=epoch)
        await self._async_health_status("scene")
        await self._async_health_status("mode")

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
            if self._stopping or (not self._started and not self._startup_active):
                return
            self._operation_generation += 1
            if self._startup_active:
                self._startup_buffered_execution = device
                return
            self._execution = device
            self._execution_available = True
            epoch = self._epoch
        try:
            await self._async_process_execution(device, emit_events=True, epoch=epoch)
        except Exception:
            logger.exception("scene bridge execution callback failed; continuing")

    async def _async_process_execution(
        self,
        device: BrilliantDevice,
        *,
        emit_events: bool,
        epoch: int,
    ) -> None:
        scenes, scene_malformed = _decode_scene_records(device)
        if scene_malformed:
            logger.warning("malformed scene execution record")
        mode_malformed = False
        try:
            modes = decode_mode_execution(device)
        except Exception:
            mode_malformed = True
            modes = ()
            logger.exception("malformed mode execution record")

        snapshot: tuple[int, SceneState] | None = None
        async with self._lock:
            if epoch != self._epoch or self._stopping:
                return
            seed_only = not self._state_trusted
            self._scene_execution_healthy = not scene_malformed
            self._mode_execution_healthy = not mode_malformed
            changed = False
            for execution in scenes:
                changed = (
                    self._apply_scene_execution(
                        execution,
                        emit_event=emit_events and not seed_only,
                    )
                    or changed
                )
            for mode_execution in modes:
                changed = (
                    self._apply_mode_execution(
                        mode_execution,
                        emit_event=emit_events and not seed_only,
                    )
                    or changed
                )
            if changed or (
                (seed_only or not emit_events) and not scene_malformed and not mode_malformed
            ):
                snapshot = self._capture_state()
        persisted = True
        if snapshot is not None:
            persisted = await self._async_persist_state(*snapshot, epoch)
        if persisted:
            async with self._lock:
                if epoch == self._epoch and not self._stopping:
                    self._schedule_delivery()
        await self._async_health_status("scene")
        await self._async_health_status("mode")

    def _apply_scene_execution(self, execution: SceneExecution, *, emit_event: bool) -> bool:
        key = (self._panel, execution.scene_id)
        previous = self._watermarks.get(key)
        if not _is_new(previous, execution):
            return False
        if key not in self._watermarks and len(self._watermarks) >= _SCENE_WATERMARK_LIMIT:
            self._state_reason = "state_capacity"
            return False
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
            return False
        new_results = sum(("scene", command_id) not in self._results for command_id, _ in matching)
        if publish_event and not self._reserve_result_slots(
            new_results, releasing_pending=len(matching)
        ):
            return False
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
        return True

    def _apply_mode_execution(self, execution: ModeExecution, *, emit_event: bool) -> bool:
        current = (execution.executed_at_ms, execution.mode_id)
        previous = self._mode_watermarks.get(self._panel)
        if previous is not None and current <= previous:
            return False
        if previous is None and len(self._mode_watermarks) >= _MODE_WATERMARK_LIMIT:
            self._state_reason = "state_capacity"
            return False
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
            return False
        new_results = sum(("mode", command_id) not in self._results for command_id, _ in matching)
        if publish_event and not self._reserve_result_slots(
            new_results, releasing_pending=len(matching)
        ):
            return False
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
        return True

    async def _async_mqtt_message(self, topic: str, payload: str, retained: bool) -> None:
        if topic not in (scene_command_topic(self._panel), mode_command_topic(self._panel)):
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
            else:
                kind = "mode"
                command = decode_mode_command(payload, now_ms=self._clock_ms())
                validate_mode_command_context(command, topic_panel=self._panel, retained=retained)
                value = command.mode_id
        except ValueError:
            return False

        fingerprint = _command_fingerprint(kind, command)
        state_kind = cast(StateKind, kind)
        cache_key: StateKey = (state_kind, command.command_id)
        snapshot: tuple[int, SceneState] | None = None
        record: _StoredPending | None = None
        execution_device_id: str | None = None
        needs_delivery = False
        needs_health = False
        epoch = self._epoch
        async with self._lock:
            if not self._started or self._stopping:
                return False
            epoch = self._epoch
            known = value in (self._scene_ids if kind == "scene" else self._mode_ids)
            if not self._state_trusted:
                needs_health = True
            else:
                cached = self._results.get(cache_key)
                if cached is not None:
                    if not known or cached.fingerprint != fingerprint:
                        return False
                    self._results[cache_key] = replace(
                        cached,
                        event_key=None if cached.delivered else cached.event_key,
                        delivered=False,
                    )
                    snapshot = self._capture_state()
                    needs_delivery = True
                elif cache_key in self._pending_records:
                    return False
                else:
                    pending = self._scene_pending if kind == "scene" else self._mode_pending
                    if command.command_id in pending:
                        return False
                    if not known:
                        if self._store_result(
                            kind,
                            command.command_id,
                            value,
                            fingerprint,
                            command.panel,
                            command.issued_at_ms,
                            accepted=False,
                            error=f"unknown_{kind}",
                            event_key=None,
                        ):
                            snapshot = self._capture_state()
                            needs_delivery = True
                        else:
                            needs_health = True
                    elif self._execution is None:
                        if self._store_result(
                            kind,
                            command.command_id,
                            value,
                            fingerprint,
                            command.panel,
                            command.issued_at_ms,
                            accepted=False,
                            error="execution_unavailable",
                            event_key=None,
                        ):
                            snapshot = self._capture_state()
                            needs_delivery = True
                        else:
                            needs_health = True
                    elif not self._has_global_capacity():
                        self._state_reason = "state_capacity"
                        needs_health = True
                    else:
                        record = _StoredPending(
                            state_kind,
                            command.command_id,
                            value,
                            fingerprint,
                            command.panel,
                            command.issued_at_ms,
                            self._clock_ms() + COMMAND_TTL_MS,
                        )
                        self._pending_records[cache_key] = record
                        pending[command.command_id] = _Pending(
                            value,
                            fingerprint,
                            command.panel,
                            command.issued_at_ms,
                            None,
                        )
                        execution_device_id = self._execution.device_id
                        snapshot = self._capture_state()
        if snapshot is None:
            if needs_health:
                await self._async_health_status(kind)
            return False
        persisted = await self._async_persist_state(*snapshot, epoch)
        if not persisted:
            if record is not None:
                async with self._lock:
                    pending = self._scene_pending if kind == "scene" else self._mode_pending
                    if self._pending_records.get(cache_key) == record:
                        self._pending_records.pop(cache_key, None)
                        pending.pop(command.command_id, None)
            await self._async_health_status(kind)
            return False
        if record is None:
            if needs_delivery:
                async with self._lock:
                    if epoch == self._epoch and self._started and not self._stopping:
                        self._schedule_delivery()
            return False

        variable = "last_executed_scene_id" if kind == "scene" else "manual_mode_id"
        async with self._lock:
            if (
                epoch != self._epoch
                or not self._started
                or self._stopping
                or self._pending_records.get(cache_key) != record
                or not self._state_trusted
            ):
                return False
            pending = self._scene_pending if kind == "scene" else self._mode_pending
            current = pending.get(command.command_id)
            if current is None or execution_device_id is None:
                return False
            timeout_task = self._track_task(
                self._async_timeout(kind, command.command_id, _TIMEOUT_SECONDS)
            )
            current.timeout_task = timeout_task
            write_task = self._track_task(
                self._async_write(
                    kind,
                    command.command_id,
                    execution_device_id,
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
            await self._async_fail_pending(
                kind,
                command_id,
                value,
                error="write_failed",
                expected_write=asyncio.current_task(),
            )

    async def _async_timeout(self, kind: str, command_id: str, delay_seconds: float) -> None:
        try:
            await self._sleep(delay_seconds)
            await self._async_fail_pending(kind, command_id, None, error="timeout")
        except asyncio.CancelledError:
            raise

    async def _async_fail_pending(
        self,
        kind: str,
        command_id: str,
        value: str | None,
        *,
        error: str,
        expected_write: asyncio.Task[None] | None = None,
    ) -> None:
        epoch = self._epoch
        async with self._lock:
            if not self._started or self._stopping:
                return
            pending_map = self._scene_pending if kind == "scene" else self._mode_pending
            pending = pending_map.get(command_id)
            if pending is None:
                return
            if expected_write is not None and pending.write_task is not expected_write:
                return
            pending_map.pop(command_id, None)
            state_key: StateKey = (cast(StateKind, kind), command_id)
            self._pending_records.pop(state_key, None)
            if error == "timeout" and pending.write_task is not None:
                pending.write_task.cancel()
            if error == "write_failed" and pending.timeout_task is not None:
                pending.timeout_task.cancel()
            stored = self._store_result(
                kind,
                command_id,
                pending.value if value is None else value,
                pending.fingerprint,
                pending.panel,
                pending.issued_at_ms,
                accepted=False,
                error=error,
                event_key=None,
            )
            if not stored:
                return
            snapshot = self._capture_state()
        if await self._async_persist_state(*snapshot, epoch):
            async with self._lock:
                if epoch == self._epoch and self._started and not self._stopping:
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
        normalized_kind = cast(StateKind, kind)
        cache_key = (normalized_kind, command_id)
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
            kind=normalized_kind,
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

    def _reserve_result(self, cache_key: StateKey) -> bool:
        if cache_key in self._results:
            return True
        return self._reserve_result_slots(1)

    def _reserve_result_slots(self, count: int, *, releasing_pending: int = 0) -> bool:
        now = self._clock_ms()
        for key, result in list(self._results.items()):
            if result.delivered and result.expires_at_ms <= now:
                self._results.pop(key)
        target = len(self._results) + len(self._pending_records) - releasing_pending + count
        while target > _RESULT_CACHE_LIMIT:
            removable = next(
                (key for key, result in self._results.items() if result.delivered),
                None,
            )
            if removable is None:
                self._state_reason = "state_capacity"
                return False
            self._results.pop(removable)
            target -= 1
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

    def _has_global_capacity(self) -> bool:
        self._prune_events()
        result_room = self._reserve_result_slots(1)
        has_room = (
            len(self._events) < _EVENT_OUTBOX_LIMIT
            and result_room
            and len(self._results) + len(self._pending_records) < _RESULT_CACHE_LIMIT
            and len(self._pending_records) < _PENDING_LIMIT
            and len(self._watermarks) < _SCENE_WATERMARK_LIMIT
            and len(self._mode_watermarks) < _MODE_WATERMARK_LIMIT
        )
        if not has_room:
            self._state_reason = "state_capacity"
        return has_room

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
            snapshot: tuple[int, SceneState] | None = None
            async with self._lock:
                if self._stopping or epoch != self._epoch:
                    return
                if item_type == "event":
                    event = self._events.get(cast(str, key))
                    if event is not None and event.payload == payload and not event.delivered:
                        self._events[cast(str, key)] = replace(event, delivered=True)
                        snapshot = self._capture_state()
                else:
                    result_key = cast(tuple[StateKind, str], key)
                    result = self._results.get(result_key)
                    if result is not None and result.payload == payload and not result.delivered:
                        self._results[result_key] = replace(result, delivered=True)
                        snapshot = self._capture_state()
            if snapshot is None:
                continue
            if not await self._async_persist_state(*snapshot, epoch):
                return
            async with self._lock:
                if self._stopping or epoch != self._epoch:
                    return
                self._prune_events()
                self._clear_capacity_if_room()
            await self._async_health_status("scene")
            await self._async_health_status("mode")

    def _next_delivery(
        self,
    ) -> tuple[str, str | StateKey, str, str] | None:
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
            and len(self._pending_records) < _PENDING_LIMIT
            and len(self._watermarks) < _SCENE_WATERMARK_LIMIT
            and len(self._mode_watermarks) < _MODE_WATERMARK_LIMIT
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
        async with self._lock:
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
    return command_fingerprint_fields(kind, command_id, panel, identifier, issued_at_ms)


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
