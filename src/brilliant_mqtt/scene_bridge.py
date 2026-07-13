"""Bidirectional scene and mode transport on the shared bus/MQTT sessions."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
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
_RESULT_CACHE_LIMIT = 1_024
_TIMEOUT_SECONDS = COMMAND_TTL_MS / 1_000


@dataclass(frozen=True, slots=True)
class Watermark:
    """Newest durable execution identity for one panel scene."""

    executed_at_ms: int
    payload_sha256: str


@dataclass(slots=True)
class _Pending:
    value: str
    task: asyncio.Task[None]


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
        self._scene_ids: frozenset[str] = frozenset()
        self._mode_ids: frozenset[str] = frozenset()
        self._watermarks: dict[tuple[str, str], Watermark] = {}
        self._mode_watermark: tuple[int, str] | None = None
        self._scene_pending: dict[str, _Pending] = {}
        self._mode_pending: dict[str, _Pending] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._results: OrderedDict[tuple[str, str], tuple[str, str]] = OrderedDict()
        self._status: dict[str, tuple[bool, str | None]] = {}
        self._scene_catalog_healthy = True
        self._scene_execution_healthy = True
        self._mode_catalog_healthy = True
        self._mode_execution_healthy = True

    async def async_start(self) -> None:
        """Register callbacks, seed history, publish catalogs, and accept commands."""
        async with self._lock:
            if self._started:
                return
            if not self._callbacks_registered:
                self._bus.on_change(self._async_bus_change)
                self._bus.on_reconnect(self.async_reconcile)
                self._mqtt.on_message(self._async_mqtt_message)
                self._callbacks_registered = True

            subscribed: list[str] = []
            try:
                self._load_watermarks()
                for topic in (scene_command_topic(self._panel), mode_command_topic(self._panel)):
                    await self._mqtt.subscribe(topic)
                    subscribed.append(topic)
                self._subscribed_topics = subscribed
                await self._async_reconcile_locked(emit_history=False)
            except BaseException:
                for topic in reversed(subscribed):
                    try:
                        await self._mqtt.unsubscribe(topic)
                    except Exception:
                        logger.exception("scene bridge startup unsubscribe failed")
                self._subscribed_topics = []
                self._started = False
                raise
            self._started = True

    async def async_reconcile(self) -> None:
        """Re-read execution state and both scoped catalogs after reconnect."""
        async with self._lock:
            if not self._started:
                return
            await self._async_reconcile_locked(emit_history=True)

    async def async_shutdown(self) -> None:
        """Fence callbacks, cancel deadlines, unsubscribe exact topics, and flush."""
        async with self._lock:
            if not self._started and not self._subscribed_topics:
                return
            self._started = False
            topics = list(self._subscribed_topics)
            self._subscribed_topics.clear()
            tasks = list(self._tasks)
            for task in tasks:
                task.cancel()
            self._scene_pending.clear()
            self._mode_pending.clear()

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for topic in topics:
            try:
                await self._mqtt.unsubscribe(topic)
            except Exception:
                logger.exception("scene bridge unsubscribe failed; continuing")
        async with self._lock:
            self._persist_watermarks()

    async def _async_reconcile_locked(self, *, emit_history: bool) -> None:
        devices = await self._bus.get_all()
        execution = next(
            (device for device in devices if device.peripheral_id == _EXECUTION_PERIPHERAL_ID),
            None,
        )
        if execution is not None:
            self._execution = execution
            await self._async_process_execution(execution, emit_events=emit_history)
        await self._async_publish_catalogs()

    async def _async_publish_catalogs(self) -> None:
        try:
            scene_device = await self._bus.get_peripheral(
                _CONFIGURATION_DEVICE_ID, "scene_configuration"
            )
            scenes = () if scene_device is None else decode_scene_catalog(scene_device)
            self._scene_ids = frozenset(item.scene_id for item in scenes)
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
            self._scene_catalog_healthy = True
        except Exception:
            self._scene_ids = frozenset()
            logger.exception("malformed scene catalog")
            self._scene_catalog_healthy = False
        await self._async_health_status("scene")

        try:
            mode_device = await self._bus.get_peripheral(
                _CONFIGURATION_DEVICE_ID, "mode_configuration"
            )
            modes = () if mode_device is None else decode_mode_catalog(mode_device)
            self._mode_ids = frozenset(item.mode_id for item in modes)
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
            self._mode_catalog_healthy = True
        except Exception:
            self._mode_ids = frozenset()
            logger.exception("malformed mode catalog")
            self._mode_catalog_healthy = False
        await self._async_health_status("mode")

    async def _async_bus_change(self, device: BrilliantDevice) -> None:
        if device.peripheral_id != _EXECUTION_PERIPHERAL_ID:
            return
        async with self._lock:
            if not self._started:
                return
            self._execution = device
            try:
                await self._async_process_execution(device, emit_events=True)
            except Exception:
                logger.exception("scene bridge execution callback failed; continuing")

    async def _async_process_execution(self, device: BrilliantDevice, *, emit_events: bool) -> None:
        try:
            scenes = decode_scene_execution(device)
        except Exception:
            logger.exception("malformed scene execution record")
            self._scene_execution_healthy = False
            await self._async_health_status("scene")
        else:
            self._scene_execution_healthy = True
            await self._async_health_status("scene")
            for execution in scenes:
                await self._async_scene_execution(execution, emit_event=emit_events)

        try:
            modes = decode_mode_execution(device)
        except Exception:
            logger.exception("malformed mode execution record")
            self._mode_execution_healthy = False
            await self._async_health_status("mode")
        else:
            self._mode_execution_healthy = True
            await self._async_health_status("mode")
            for mode_execution in modes:
                await self._async_mode_execution(mode_execution, emit_event=emit_events)

    async def _async_scene_execution(self, execution: SceneExecution, *, emit_event: bool) -> None:
        key = (self._panel, execution.scene_id)
        previous = self._watermarks.get(key)
        if not _is_new(previous, execution):
            return
        self._watermarks[key] = Watermark(execution.executed_at_ms, execution.payload_sha256)
        self._persist_watermarks()
        if not emit_event:
            return
        await self._mqtt.publish(
            scene_event_topic(self._panel),
            encode_json(
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
            ),
            retain=False,
        )
        for command_id, pending in list(self._scene_pending.items()):
            if pending.value == execution.scene_id:
                self._scene_pending.pop(command_id)
                pending.task.cancel()
                await self._async_result(
                    "scene", command_id, execution.scene_id, accepted=True, error=None
                )

    async def _async_mode_execution(self, execution: ModeExecution, *, emit_event: bool) -> None:
        current = (execution.executed_at_ms, execution.mode_id)
        if self._mode_watermark is not None and current <= self._mode_watermark:
            return
        self._mode_watermark = current
        if not emit_event:
            return
        await self._mqtt.publish(
            mode_event_topic(self._panel),
            encode_json(
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
            ),
            retain=False,
        )
        for command_id, pending in list(self._mode_pending.items()):
            if pending.value == execution.mode_id:
                self._mode_pending.pop(command_id)
                pending.task.cancel()
                await self._async_result(
                    "mode", command_id, execution.mode_id, accepted=True, error=None
                )

    async def _async_mqtt_message(self, topic: str, payload: str, retained: bool) -> None:
        if topic not in (scene_command_topic(self._panel), mode_command_topic(self._panel)):
            return
        async with self._lock:
            if not self._started:
                return
            try:
                await self._async_command(topic, payload, retained)
            except Exception:
                logger.exception("scene bridge command callback failed; continuing")

    async def _async_command(self, topic: str, payload: str, retained: bool) -> None:
        kind = "scene" if topic == scene_command_topic(self._panel) else "mode"
        raw_command_id = _extract_command_id(payload)
        cache_key = None if raw_command_id is None else (kind, raw_command_id)
        cached = None if cache_key is None else self._results.get(cache_key)
        if not retained and cached is not None:
            await self._mqtt.publish(cached[0], cached[1], retain=False)
            return
        pending = self._scene_pending if kind == "scene" else self._mode_pending
        if not retained and raw_command_id is not None and raw_command_id in pending:
            return

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
            return

        if not known:
            await self._async_result(
                kind, command.command_id, value, accepted=False, error=f"unknown_{kind}"
            )
            return
        if self._execution is None:
            await self._async_result(
                kind, command.command_id, value, accepted=False, error="execution_unavailable"
            )
            return

        variable = "last_executed_scene_id" if kind == "scene" else "manual_mode_id"
        task = asyncio.create_task(self._async_timeout(kind, command.command_id))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        pending[command.command_id] = _Pending(value, task)
        try:
            await self._bus.set_variables(
                self._execution.device_id,
                _EXECUTION_PERIPHERAL_ID,
                [VarSet(name=variable, value=value)],
            )
        except Exception as error:
            logger.warning("scene bridge write failed (%s)", type(error).__name__)
            pending.pop(command.command_id, None)
            task.cancel()
            await self._async_result(
                kind, command.command_id, value, accepted=False, error="write_failed"
            )

    async def _async_timeout(self, kind: str, command_id: str) -> None:
        try:
            await self._sleep(_TIMEOUT_SECONDS)
            async with self._lock:
                if not self._started:
                    return
                pending_map = self._scene_pending if kind == "scene" else self._mode_pending
                pending = pending_map.pop(command_id, None)
                if pending is not None:
                    await self._async_result(
                        kind, command_id, pending.value, accepted=False, error="timeout"
                    )
        except asyncio.CancelledError:
            raise

    async def _async_result(
        self,
        kind: str,
        command_id: str,
        value: str,
        *,
        accepted: bool,
        error: str | None,
    ) -> None:
        body: dict[str, object] = {
            "schema_version": SCHEMA_VERSION,
            "mapping_version": MAPPING_VERSION,
            "command_id": command_id,
            "panel": self._panel,
            f"{kind}_id": value,
            "accepted": accepted,
            "timestamp_ms": self._clock_ms(),
        }
        if error is not None:
            body["error"] = error
        payload = encode_json(body)
        topic = scene_result_topic(command_id) if kind == "scene" else mode_result_topic(command_id)
        cache_key = (kind, command_id)
        self._results[cache_key] = (topic, payload)
        self._results.move_to_end(cache_key)
        while len(self._results) > _RESULT_CACHE_LIMIT:
            self._results.popitem(last=False)
        await self._mqtt.publish(topic, payload, retain=False)

    async def _async_status(self, transport: str, available: bool, reason: str | None) -> None:
        current = (available, reason)
        if self._status.get(transport) == current:
            return
        try:
            await self._mqtt.publish(
                transport_status_topic(transport, self._panel),
                encode_json(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "mapping_version": MAPPING_VERSION,
                        "transport": transport,
                        "panel": self._panel,
                        "available": available,
                        "reason": reason,
                        "timestamp_ms": self._clock_ms(),
                    }
                ),
                retain=True,
            )
        except Exception:
            logger.exception("scene bridge status publication failed")
        else:
            self._status[transport] = current

    async def _async_health_status(self, transport: str) -> None:
        if transport == "scene":
            available = self._scene_catalog_healthy and self._scene_execution_healthy
        else:
            available = self._mode_catalog_healthy and self._mode_execution_healthy
        await self._async_status(transport, available, None if available else "malformed_data")

    def _load_watermarks(self) -> None:
        self._watermarks.clear()
        try:
            raw = json.loads(self._watermark_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return
        except (OSError, UnicodeError, json.JSONDecodeError):
            logger.warning("scene watermark file is unreadable; rebuilding")
            return
        if not isinstance(raw, dict):
            return
        for panel, panel_records in raw.items():
            if not isinstance(panel, str) or not isinstance(panel_records, dict):
                continue
            for scene_id, value in panel_records.items():
                if not isinstance(scene_id, str) or not isinstance(value, dict):
                    continue
                executed_at_ms = value.get("executed_at_ms")
                payload_sha256 = value.get("payload_sha256")
                if (
                    type(executed_at_ms) is int
                    and executed_at_ms >= 0
                    and isinstance(payload_sha256, str)
                    and _is_sha256(payload_sha256)
                ):
                    self._watermarks[(panel, scene_id)] = Watermark(executed_at_ms, payload_sha256)

    def _persist_watermarks(self) -> None:
        records: dict[str, dict[str, dict[str, object]]] = {}
        for (panel, scene_id), watermark in sorted(self._watermarks.items()):
            records.setdefault(panel, {})[scene_id] = {
                "executed_at_ms": watermark.executed_at_ms,
                "payload_sha256": watermark.payload_sha256,
            }
        self._watermark_path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(
            dir=self._watermark_path.parent,
            prefix=f".{self._watermark_path.name}.",
        )
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                descriptor = -1
                json.dump(records, handle, separators=(",", ":"), sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self._watermark_path)
            os.chmod(self._watermark_path, 0o600)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass


def _extract_command_id(payload: str) -> str | None:
    try:
        decoded = json.loads(payload)
        if not isinstance(decoded, dict):
            return None
        command_id = decoded.get("command_id")
        if not isinstance(command_id, str):
            return None
        return str(UUID(command_id))
    except (json.JSONDecodeError, TypeError, ValueError):
        return None


def _is_sha256(value: str) -> bool:
    if len(value) != 64:
        return False
    try:
        int(value, 16)
    except ValueError:
        return False
    return True
