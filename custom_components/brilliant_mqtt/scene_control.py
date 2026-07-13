"""Singleton HA-side Brilliant scene and mode MQTT transport."""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import math
import re
import time
from collections import OrderedDict
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from typing import Literal, cast
from uuid import UUID, uuid4

from homeassistant.components import mqtt
from homeassistant.components.mqtt.models import ReceiveMessage
from homeassistant.core import CALLBACK_TYPE, HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.dispatcher import async_dispatcher_send

from .const import DOMAIN
from .ha_control_protocol import (
    MAPPING_VERSION,
    SCHEMA_VERSION,
    encode_json,
    mode_command_topic,
    scene_command_topic,
)

_LOGGER = logging.getLogger(__name__)

EVENT_SCENE = f"{DOMAIN}_scene"
EVENT_MODE = f"{DOMAIN}_mode"

MAX_DEDUPLICATION_KEYS = 1_024
MAX_PENDING_COMMANDS = 128
_MAX_CATALOG_ITEMS = 256
_MAX_CONFIGURED_ACTIONS = 1_024
_MAX_PAYLOAD_CHARACTERS = 64 * 1024
_MAX_JSON_NODES = 2_048
_MAX_JSON_DEPTH = 12
_MAX_STRING_LENGTH = 4_096
_RESULT_TIMEOUT_SECONDS = 16.0

_TOPIC_PREFIX = ("brilliant", "ha-control", "v1")
_PANEL_PATTERN = re.compile(r"[a-z0-9][a-z0-9_-]{0,62}")
_SERVICE_PATTERN = re.compile(r"[a-z0-9_]+")
_RESULT_ERROR_PATTERN = re.compile(r"[a-z0-9_]{1,64}")
_TARGET_KEYS = frozenset({"entity_id", "device_id", "area_id"})

_SUBSCRIPTION_TOPICS = (
    "brilliant/ha-control/v1/scene/catalog/+",
    "brilliant/ha-control/v1/mode/catalog/+",
    "brilliant/ha-control/v1/scene/event/+",
    "brilliant/ha-control/v1/mode/event/+",
    "brilliant/ha-control/v1/scene/result/+",
    "brilliant/ha-control/v1/mode/result/+",
    "brilliant/ha-control/v1/status/scene/+",
    "brilliant/ha-control/v1/status/mode/+",
)

type _Kind = Literal["scene", "mode"]


@dataclass(frozen=True, slots=True)
class SceneOption:
    """One stable scene ID and its user-facing display name."""

    scene_id: str
    display_name: str


@dataclass(frozen=True, slots=True)
class ModeOption:
    """One stable mode ID and its user-facing display name."""

    mode_id: str
    display_name: str


@dataclass(frozen=True, slots=True)
class _Action:
    domain: str
    service: str
    target: dict[str, object]
    data: dict[str, object]


@dataclass(frozen=True, slots=True)
class _CommandResult:
    accepted: bool
    error: str | None


@dataclass(frozen=True, slots=True)
class _PendingCommand:
    kind: _Kind
    panel: str
    value: str
    issued_at_ms: int
    future: asyncio.Future[_CommandResult]


class SceneControl:
    """Own all HA MQTT subscriptions and scene/mode request correlation."""

    def __init__(self, hass: HomeAssistant) -> None:
        self.hass = hass
        self._lifecycle_lock = asyncio.Lock()
        self._unsubscribers: list[CALLBACK_TYPE] = []
        self._started = False
        self._command_fence_generation = 0
        self._active_command_fences: set[int] = set()
        self._attached_panels: frozenset[str] = frozenset()
        self._default_panel: str | None = None
        self._actions: dict[tuple[str, str], _Action] = {}
        self._scene_catalogs: dict[str, tuple[SceneOption, ...]] = {}
        self._selected_scenes: dict[str, str] = {}
        self._mode_catalogs: dict[str, tuple[ModeOption, ...]] = {}
        self._catalog_timestamps: dict[tuple[_Kind, str], int] = {}
        self._catalog_revisions: dict[tuple[_Kind, str], int] = {}
        self._status: dict[tuple[_Kind, str], bool] = {}
        self._status_timestamps: dict[tuple[_Kind, str], int] = {}
        self._event_timestamps: dict[tuple[_Kind, str], int] = {}
        self._deduplication_keys: OrderedDict[tuple[_Kind, str, str], None] = OrderedDict()
        self._pending: dict[str, _PendingCommand] = {}

    @property
    def started(self) -> bool:
        """Return whether all MQTT subscriptions are active."""
        return self._started

    @property
    def attached_panels(self) -> frozenset[str]:
        """Return the currently loaded Brilliant panel slugs."""
        return self._attached_panels

    @property
    def default_panel(self) -> str | None:
        """Return the attached panel selected for panel-less service calls."""
        return self._default_panel

    @property
    def pending_count(self) -> int:
        """Return the bounded number of service calls awaiting confirmation."""
        return len(self._pending)

    @property
    def deduplication_cache_size(self) -> int:
        """Return the bounded event deduplication-cache size."""
        return len(self._deduplication_keys)

    def scene_options(self, panel: str) -> tuple[SceneOption, ...]:
        """Return the latest atomic scene catalog for a panel."""
        return self._scene_catalogs.get(panel, ())

    def mode_options(self, panel: str) -> tuple[ModeOption, ...]:
        """Return the latest atomic mode catalog for a panel."""
        return self._mode_catalogs.get(panel, ())

    def scene_transport_available(self, panel: str) -> bool:
        """Return whether scene transport and a non-empty catalog are available."""
        return self._status.get(("scene", panel), False) and bool(self._scene_catalogs.get(panel))

    def selected_scene(self, panel: str) -> str | None:
        """Return the HA-local stable scene selection for a panel."""
        return self._selected_scenes.get(panel)

    def select_scene(self, panel: str, scene_id: str) -> None:
        """Update only HA-local selection; no MQTT command is published."""
        if scene_id not in {item.scene_id for item in self._scene_catalogs.get(panel, ())}:
            raise HomeAssistantError("Scene is not available on the selected Brilliant panel.")
        self._selected_scenes[panel] = scene_id
        async_dispatcher_send(self.hass, scene_control_signal(panel))

    def mode_transport_available(self, panel: str) -> bool:
        """Return whether mode transport and a non-empty catalog are available."""
        return self._status.get(("mode", panel), False) and bool(self._mode_catalogs.get(panel))

    def catalog_revision(self, kind: _Kind, panel: str) -> int | None:
        """Return the local revision of the most recently accepted whole catalog."""
        return self._catalog_revisions.get((kind, panel))

    def catalog_timestamp_ms(self, kind: _Kind, panel: str) -> int | None:
        """Return the panel timestamp of the most recently accepted catalog."""
        return self._catalog_timestamps.get((kind, panel))

    def last_event_timestamp_ms(self, kind: _Kind, panel: str) -> int | None:
        """Return the most recent accepted event timestamp for diagnostics."""
        return self._event_timestamps.get((kind, panel))

    def status_timestamp_ms(self, kind: _Kind, panel: str) -> int | None:
        """Return the most recent accepted transport-status timestamp."""
        return self._status_timestamps.get((kind, panel))

    async def async_start(
        self,
        panels: Collection[str],
        *,
        default_panel: str | None,
        actions: Mapping[str, object],
    ) -> None:
        """Configure the runtime and subscribe exactly once, rolling back on failure."""
        fence_token = self.fence_commands()
        try:
            async with self._lifecycle_lock:
                self._async_reconfigure_locked(panels, default_panel, actions)
                if self._started:
                    return
                unsubscribers: list[CALLBACK_TYPE] = []
                try:
                    for topic in _SUBSCRIPTION_TOPICS:
                        unsubscribe = await mqtt.async_subscribe(
                            self.hass, topic, self._async_message_received
                        )
                        unsubscribers.append(unsubscribe)
                except BaseException as primary_error:
                    for unsubscribe in reversed(unsubscribers):
                        try:
                            unsubscribe()
                        except BaseException as cleanup_error:
                            _LOGGER.warning(
                                "Scene subscription rollback also failed (%s)",
                                type(cleanup_error).__name__,
                            )
                    raise primary_error
                self._unsubscribers = unsubscribers
                self._started = True
        finally:
            self.release_command_fence(fence_token)

    async def async_reconfigure(
        self,
        panels: Collection[str],
        *,
        default_panel: str | None,
        actions: Mapping[str, object],
    ) -> None:
        """Atomically replace attached panels, default routing, and mapped actions."""
        fence_token = self.fence_commands()
        try:
            async with self._lifecycle_lock:
                self._async_reconfigure_locked(panels, default_panel, actions)
        finally:
            self.release_command_fence(fence_token)

    async def async_stop(self) -> None:
        """Fence MQTT input, fail pending calls, unsubscribe, and clear volatile state."""
        fence_token = self.fence_commands()
        try:
            panels_to_notify: frozenset[str]
            unsubscribe_error: BaseException | None = None
            async with self._lifecycle_lock:
                panels_to_notify = self._attached_panels
                self._started = False
                for unsubscribe in reversed(self._unsubscribers):
                    try:
                        unsubscribe()
                    except BaseException as error:
                        if unsubscribe_error is None:
                            unsubscribe_error = error
                self._unsubscribers.clear()
                self._fail_pending_locked(
                    tuple(self._pending), "Brilliant scene control stopped before confirmation."
                )
                self._clear_transport_state_locked()
                self._attached_panels = frozenset()
                self._default_panel = None
                self._actions.clear()
            for panel in panels_to_notify:
                async_dispatcher_send(self.hass, scene_control_signal(panel))
            if unsubscribe_error is not None:
                raise unsubscribe_error
        finally:
            self.release_command_fence(fence_token)

    async def async_run_scene(self, panel: str | None, scene_id: str) -> None:
        """Publish a scene command and require the panel's matching execution result."""
        await self._async_execute("scene", panel, scene_id)

    async def async_set_mode(self, panel: str | None, mode_id: str) -> None:
        """Publish a mode command and require the panel's matching execution result."""
        await self._async_execute("mode", panel, mode_id)

    def fence_commands(self) -> int:
        """Synchronously hard-fence new and already-queued service commands."""
        self._command_fence_generation += 1
        token = self._command_fence_generation
        self._active_command_fences.add(token)
        return token

    def release_command_fence(self, token: int) -> None:
        """Release only the fence token owned by a completed lifecycle call."""
        self._active_command_fences.discard(token)

    def _async_reconfigure_locked(
        self,
        panels: Collection[str],
        default_panel: str | None,
        actions: Mapping[str, object],
    ) -> None:
        attached = frozenset(panel for panel in panels if _is_panel(panel))
        selected = default_panel if default_panel in attached else None
        validated_actions = _validated_actions(actions)
        removed = self._attached_panels - attached
        for panel in removed:
            self._drop_panel_state_locked(panel)
        removed_commands = tuple(
            command_id
            for command_id, pending in self._pending.items()
            if pending.panel not in attached
        )
        self._fail_pending_locked(
            removed_commands, "The selected Brilliant panel is no longer attached."
        )
        changed = self._attached_panels | attached
        self._attached_panels = attached
        self._default_panel = selected
        self._actions = validated_actions
        for panel in changed:
            async_dispatcher_send(self.hass, scene_control_signal(panel))

    async def _async_execute(self, kind: _Kind, panel: str | None, value: str) -> None:
        generation = self._command_fence_generation
        fenced_at_receipt = not self._started or bool(self._active_command_fences)
        if not isinstance(value, str) or not value or len(value) > _MAX_STRING_LENGTH:
            raise HomeAssistantError(f"A valid Brilliant {kind} ID is required.")
        command_id = str(uuid4())
        future: asyncio.Future[_CommandResult]
        async with self._lifecycle_lock:
            if (
                fenced_at_receipt
                or not self._started
                or bool(self._active_command_fences)
                or generation != self._command_fence_generation
            ):
                raise HomeAssistantError("Brilliant scene control is reconfiguring.")
            selected = self._resolve_panel_locked(panel)
            catalog_ids = (
                {item.scene_id for item in self._scene_catalogs.get(selected, ())}
                if kind == "scene"
                else {item.mode_id for item in self._mode_catalogs.get(selected, ())}
            )
            if not self._status.get((kind, selected), False):
                raise HomeAssistantError(f"Brilliant {kind} transport is offline.")
            if not catalog_ids or value not in catalog_ids:
                raise HomeAssistantError(
                    f"{kind.title()} is not available on the selected Brilliant panel."
                )
            if len(self._pending) >= MAX_PENDING_COMMANDS:
                raise HomeAssistantError("Brilliant scene control is busy; try again shortly.")
            issued_at_ms = _timestamp_ms()
            future = self.hass.loop.create_future()
            self._pending[command_id] = _PendingCommand(kind, selected, value, issued_at_ms, future)
            payload = encode_json(
                {
                    "schema_version": SCHEMA_VERSION,
                    "mapping_version": MAPPING_VERSION,
                    "command_id": command_id,
                    "panel": selected,
                    f"{kind}_id": value,
                    "issued_at_ms": issued_at_ms,
                }
            )
            topic = (
                scene_command_topic(selected) if kind == "scene" else mode_command_topic(selected)
            )
            try:
                await mqtt.async_publish(self.hass, topic, payload, retain=False)
            except asyncio.CancelledError:
                self._pending.pop(command_id, None)
                future.cancel()
                raise
            except Exception as error:
                self._pending.pop(command_id, None)
                future.cancel()
                _LOGGER.warning(
                    "Brilliant %s command publication failed (%s)",
                    kind,
                    type(error).__name__,
                )
                raise HomeAssistantError(f"Brilliant {kind} command publish failed.") from error

        try:
            async with asyncio.timeout(_RESULT_TIMEOUT_SECONDS):
                result = await future
        except TimeoutError as error:
            raise HomeAssistantError(f"Brilliant {kind} confirmation timed out.") from error
        finally:
            async with self._lifecycle_lock:
                current = self._pending.get(command_id)
                if current is not None and current.future is future:
                    self._pending.pop(command_id, None)
                if not future.done():
                    future.cancel()
        if not result.accepted:
            detail = f": {result.error}" if result.error is not None else ""
            raise HomeAssistantError(f"Brilliant {kind} execution failed{detail}.")

    def _resolve_panel_locked(self, panel: str | None) -> str:
        if not self._started:
            raise HomeAssistantError("Brilliant scene control is not running.")
        selected = self._default_panel if panel is None else panel
        if selected is None:
            raise HomeAssistantError("No attached Brilliant panel is selected.")
        if selected not in self._attached_panels:
            raise HomeAssistantError("The selected Brilliant panel is not attached.")
        return selected

    async def _async_message_received(self, message: ReceiveMessage) -> None:
        try:
            route = _parse_topic(message.topic)
            if route is None:
                return
            category, kind, topic_value = route
            payload = _decode_payload(str(message.payload))
            if category == "catalog":
                await self._async_catalog(kind, topic_value, payload, retained=message.retain)
            elif category == "status":
                await self._async_status(kind, topic_value, payload, retained=message.retain)
            elif category == "event":
                await self._async_event(kind, topic_value, payload, retained=message.retain)
            else:
                await self._async_result(kind, topic_value, payload, retained=message.retain)
        except (TypeError, ValueError):
            _LOGGER.warning("Ignored invalid Brilliant scene control MQTT message")
        except Exception:
            _LOGGER.exception("Brilliant scene control MQTT callback failed; continuing")

    async def _async_catalog(
        self,
        kind: _Kind,
        topic_panel: str,
        payload: Mapping[str, object],
        *,
        retained: bool,
    ) -> None:
        # A retained publication is marked retained only during broker replay;
        # active subscribers receive later replacements as ordinary messages.
        del retained
        expected = {
            "schema_version",
            "mapping_version",
            "panel",
            "generated_at_ms",
            "scenes" if kind == "scene" else "modes",
        }
        _require_exact_keys(payload, expected)
        panel = _required_panel(payload, "panel")
        if panel != topic_panel:
            raise ValueError("catalog panel mismatch")
        generated_at_ms = _required_timestamp(payload, "generated_at_ms")
        raw_items = payload["scenes" if kind == "scene" else "modes"]
        if not isinstance(raw_items, list) or len(raw_items) > _MAX_CATALOG_ITEMS:
            raise ValueError("catalog items must be a bounded list")
        options: tuple[SceneOption, ...] | tuple[ModeOption, ...]
        if kind == "scene":
            options = _decode_scene_options(raw_items)
        else:
            options = _decode_mode_options(raw_items)
        async with self._lifecycle_lock:
            if not self._started or panel not in self._attached_panels:
                raise ValueError("catalog panel is not attached")
            key = (kind, panel)
            previous = self._catalog_timestamps.get(key)
            if previous is not None and generated_at_ms <= previous:
                return
            self._catalog_timestamps[key] = generated_at_ms
            self._catalog_revisions[key] = self._catalog_revisions.get(key, 0) + 1
            if kind == "scene":
                self._scene_catalogs[panel] = cast(tuple[SceneOption, ...], options)
                if self._selected_scenes.get(panel) not in {
                    item.scene_id for item in self._scene_catalogs[panel]
                }:
                    self._selected_scenes.pop(panel, None)
            else:
                self._mode_catalogs[panel] = cast(tuple[ModeOption, ...], options)
        async_dispatcher_send(self.hass, scene_control_signal(panel))

    async def _async_status(
        self,
        kind: _Kind,
        topic_panel: str,
        payload: Mapping[str, object],
        *,
        retained: bool,
    ) -> None:
        # See catalog handling: both retained replay and live replacement are valid.
        del retained
        _require_exact_keys(
            payload,
            {
                "schema_version",
                "mapping_version",
                "transport",
                "panel",
                "available",
                "reason",
                "timestamp_ms",
            },
        )
        panel = _required_panel(payload, "panel")
        transport = _required_string(payload, "transport")
        if panel != topic_panel or transport != kind:
            raise ValueError("status topic mismatch")
        available = payload["available"]
        if type(available) is not bool:
            raise ValueError("available must be a boolean")
        reason = payload["reason"]
        if available:
            if reason is not None:
                raise ValueError("available status cannot have a reason")
        elif not isinstance(reason, str) or not reason or len(reason) > 128:
            raise ValueError("unavailable status must have a reason")
        timestamp_ms = _required_timestamp(payload, "timestamp_ms")
        async with self._lifecycle_lock:
            if not self._started or panel not in self._attached_panels:
                raise ValueError("status panel is not attached")
            key = (kind, panel)
            previous = self._status_timestamps.get(key)
            if previous is not None and timestamp_ms <= previous:
                return
            self._status_timestamps[key] = timestamp_ms
            self._status[key] = available
        async_dispatcher_send(self.hass, scene_control_signal(panel))

    async def _async_event(
        self,
        kind: _Kind,
        topic_panel: str,
        payload: Mapping[str, object],
        *,
        retained: bool,
    ) -> None:
        if retained:
            raise ValueError("event must not be retained")
        identifier_key = f"{kind}_id"
        _require_exact_keys(
            payload,
            {
                "schema_version",
                "mapping_version",
                "panel",
                identifier_key,
                "executed_at_ms",
                "deduplication_key",
            },
        )
        panel = _required_panel(payload, "panel")
        if panel != topic_panel:
            raise ValueError("event panel mismatch")
        identifier = _required_string(payload, identifier_key)
        executed_at_ms = _required_timestamp(payload, "executed_at_ms")
        deduplication_key = _required_string(payload, "deduplication_key")
        if deduplication_key != f"{panel}:{identifier}:{executed_at_ms}":
            raise ValueError("event deduplication key is not canonical")
        action: _Action | None = None
        async with self._lifecycle_lock:
            if not self._started or panel not in self._attached_panels:
                raise ValueError("event panel is not attached")
            ordering_key = (kind, panel)
            previous = self._event_timestamps.get(ordering_key)
            dedup_key = (kind, panel, deduplication_key)
            if previous is not None and executed_at_ms <= previous:
                return
            if dedup_key in self._deduplication_keys:
                return
            self._event_timestamps[ordering_key] = executed_at_ms
            self._deduplication_keys[dedup_key] = None
            while len(self._deduplication_keys) > MAX_DEDUPLICATION_KEYS:
                self._deduplication_keys.popitem(last=False)
            if kind == "scene":
                action = self._actions.get((panel, identifier))
        event_data = {
            "panel": panel,
            identifier_key: identifier,
            "executed_at_ms": executed_at_ms,
            "deduplication_key": deduplication_key,
        }
        self.hass.bus.async_fire(EVENT_SCENE if kind == "scene" else EVENT_MODE, event_data)
        if action is not None:
            try:
                await self.hass.services.async_call(
                    action.domain,
                    action.service,
                    action.data,
                    blocking=False,
                    target=action.target,
                )
            except Exception as error:
                _LOGGER.warning(
                    "Configured Brilliant scene action could not be dispatched (%s)",
                    type(error).__name__,
                )

    async def _async_result(
        self,
        kind: _Kind,
        topic_command_id: str,
        payload: Mapping[str, object],
        *,
        retained: bool,
    ) -> None:
        if retained:
            raise ValueError("result must not be retained")
        command_id = _required_uuid(payload, "command_id")
        if command_id != topic_command_id:
            raise ValueError("result command ID mismatch")
        identifier_key = f"{kind}_id"
        accepted = payload.get("accepted")
        if type(accepted) is not bool:
            raise ValueError("accepted must be a boolean")
        expected = {
            "schema_version",
            "mapping_version",
            "command_id",
            "panel",
            identifier_key,
            "accepted",
            "timestamp_ms",
        }
        if not accepted:
            expected.add("error")
        _require_exact_keys(payload, expected)
        panel = _required_panel(payload, "panel")
        identifier = _required_string(payload, identifier_key)
        timestamp_ms = _required_timestamp(payload, "timestamp_ms")
        error_value: str | None = None
        if not accepted:
            error_value = _required_string(payload, "error")
            if _RESULT_ERROR_PATTERN.fullmatch(error_value) is None:
                raise ValueError("result error is invalid")
        async with self._lifecycle_lock:
            pending = self._pending.get(command_id)
            if pending is None:
                return
            if (
                pending.kind != kind
                or pending.panel != panel
                or pending.value != identifier
                or timestamp_ms < pending.issued_at_ms
            ):
                return
            if not pending.future.done():
                pending.future.set_result(_CommandResult(accepted, error_value))

    def _drop_panel_state_locked(self, panel: str) -> None:
        self._scene_catalogs.pop(panel, None)
        self._selected_scenes.pop(panel, None)
        self._mode_catalogs.pop(panel, None)
        for mapping in (
            self._catalog_timestamps,
            self._catalog_revisions,
            self._status,
            self._status_timestamps,
            self._event_timestamps,
        ):
            for key in tuple(mapping):
                if key[1] == panel:
                    mapping.pop(key, None)
        for dedup_key in tuple(self._deduplication_keys):
            if dedup_key[1] == panel:
                self._deduplication_keys.pop(dedup_key, None)

    def _clear_transport_state_locked(self) -> None:
        self._scene_catalogs.clear()
        self._selected_scenes.clear()
        self._mode_catalogs.clear()
        self._catalog_timestamps.clear()
        self._catalog_revisions.clear()
        self._status.clear()
        self._status_timestamps.clear()
        self._event_timestamps.clear()
        self._deduplication_keys.clear()

    def _fail_pending_locked(self, command_ids: Collection[str], message: str) -> None:
        for command_id in command_ids:
            pending = self._pending.pop(command_id, None)
            if pending is not None and not pending.future.done():
                pending.future.set_exception(HomeAssistantError(message))


def scene_control_signal(panel: str) -> str:
    """Return the dispatcher signal for one panel's scene-control state."""
    return f"{DOMAIN}_scene_control_{panel}"


def _timestamp_ms() -> int:
    return time.time_ns() // 1_000_000


def _parse_topic(topic: str) -> tuple[str, _Kind, str] | None:
    parts = topic.split("/")
    if len(parts) != 6 or tuple(parts[:3]) != _TOPIC_PREFIX:
        return None
    first, second, value = parts[3], parts[4], parts[5]
    if first in ("scene", "mode") and second in ("catalog", "event"):
        if not _is_panel(value):
            raise ValueError("invalid topic panel")
        return second, cast(_Kind, first), value
    if first in ("scene", "mode") and second == "result":
        return "result", cast(_Kind, first), _validated_uuid(value)
    if first == "status" and second in ("scene", "mode"):
        if not _is_panel(value):
            raise ValueError("invalid topic panel")
        return "status", cast(_Kind, second), value
    return None


def _decode_payload(raw_payload: str) -> Mapping[str, object]:
    if len(raw_payload) > _MAX_PAYLOAD_CHARACTERS:
        raise ValueError("payload is too large")
    try:
        decoded = json.loads(raw_payload)
    except (json.JSONDecodeError, TypeError) as error:
        raise ValueError("payload must be JSON") from error
    if not isinstance(decoded, dict) or not all(isinstance(key, str) for key in decoded):
        raise ValueError("payload must be an object")
    payload = cast(dict[str, object], decoded)
    if (
        type(payload.get("schema_version")) is not int
        or payload["schema_version"] != SCHEMA_VERSION
    ):
        raise ValueError("unsupported schema version")
    if (
        type(payload.get("mapping_version")) is not int
        or payload["mapping_version"] != MAPPING_VERSION
    ):
        raise ValueError("unsupported mapping version")
    _validate_json_value(payload, depth=0, remaining=[_MAX_JSON_NODES])
    return payload


def _validate_json_value(value: object, *, depth: int, remaining: list[int]) -> None:
    remaining[0] -= 1
    if remaining[0] < 0 or depth > _MAX_JSON_DEPTH:
        raise ValueError("JSON value exceeds bounds")
    if value is None or type(value) in (bool, int):
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("JSON number must be finite")
        return
    if isinstance(value, str):
        if len(value) > _MAX_STRING_LENGTH:
            raise ValueError("JSON string is too long")
        return
    if isinstance(value, list):
        for item in value:
            _validate_json_value(item, depth=depth + 1, remaining=remaining)
        return
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        for key, item in value.items():
            if len(key) > _MAX_STRING_LENGTH:
                raise ValueError("JSON key is too long")
            _validate_json_value(item, depth=depth + 1, remaining=remaining)
        return
    raise ValueError("value is not safe JSON")


def _require_exact_keys(payload: Mapping[str, object], keys: set[str]) -> None:
    if set(payload) != keys:
        raise ValueError("payload fields do not match the contract")


def _required_string(payload: Mapping[str, object], field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value or len(value) > _MAX_STRING_LENGTH:
        raise ValueError(f"{field} must be a bounded non-empty string")
    return value


def _required_timestamp(payload: Mapping[str, object], field: str) -> int:
    value = payload.get(field)
    if type(value) is not int or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def _required_panel(payload: Mapping[str, object], field: str) -> str:
    panel = _required_string(payload, field)
    if not _is_panel(panel):
        raise ValueError(f"{field} must be a panel slug")
    return panel


def _required_uuid(payload: Mapping[str, object], field: str) -> str:
    return _validated_uuid(_required_string(payload, field))


def _validated_uuid(value: str) -> str:
    try:
        return str(UUID(value))
    except ValueError as error:
        raise ValueError("value must be a UUID") from error


def _is_panel(value: object) -> bool:
    return isinstance(value, str) and _PANEL_PATTERN.fullmatch(value) is not None


def _decode_scene_options(raw_items: list[object]) -> tuple[SceneOption, ...]:
    options: list[SceneOption] = []
    ids: set[str] = set()
    display_names: set[str] = set()
    for raw_item in raw_items:
        if not isinstance(raw_item, dict):
            raise ValueError("scene catalog item must be an object")
        item = cast(dict[str, object], raw_item)
        _require_exact_keys(item, {"scene_id", "display_name", "icon"})
        scene_id = _required_string(item, "scene_id")
        display_name = _required_string(item, "display_name")
        icon = item["icon"]
        if icon is not None and (not isinstance(icon, str) or len(icon) > 256):
            raise ValueError("scene icon must be null or a bounded string")
        if scene_id in ids or display_name in display_names:
            raise ValueError("scene catalog IDs and names must be unique")
        ids.add(scene_id)
        display_names.add(display_name)
        options.append(SceneOption(scene_id, display_name))
    return tuple(options)


def _decode_mode_options(raw_items: list[object]) -> tuple[ModeOption, ...]:
    options: list[ModeOption] = []
    ids: set[str] = set()
    display_names: set[str] = set()
    for raw_item in raw_items:
        if not isinstance(raw_item, dict):
            raise ValueError("mode catalog item must be an object")
        item = cast(dict[str, object], raw_item)
        _require_exact_keys(item, {"mode_id", "display_name"})
        mode_id = _required_string(item, "mode_id")
        display_name = _required_string(item, "display_name")
        if mode_id in ids or display_name in display_names:
            raise ValueError("mode catalog IDs and names must be unique")
        ids.add(mode_id)
        display_names.add(display_name)
        options.append(ModeOption(mode_id, display_name))
    return tuple(options)


def _validated_actions(raw_actions: Mapping[str, object]) -> dict[tuple[str, str], _Action]:
    try:
        if len(raw_actions) > _MAX_CONFIGURED_ACTIONS:
            raise ValueError("too many configured actions")
        actions: dict[tuple[str, str], _Action] = {}
        for raw_key, raw_action in raw_actions.items():
            if not isinstance(raw_key, str) or raw_key.count(":") != 1:
                raise ValueError("action key must contain one colon")
            panel, scene_id = raw_key.split(":")
            if not _is_panel(panel) or not scene_id or len(scene_id) > _MAX_STRING_LENGTH:
                raise ValueError("action key is invalid")
            if not isinstance(raw_action, Mapping) or not all(
                isinstance(key, str) for key in raw_action
            ):
                raise ValueError("action must be an object")
            action = cast(Mapping[str, object], raw_action)
            _require_exact_keys(action, {"domain", "service", "target", "data"})
            domain = _required_string(action, "domain")
            service = _required_string(action, "service")
            if (
                _SERVICE_PATTERN.fullmatch(domain) is None
                or _SERVICE_PATTERN.fullmatch(service) is None
            ):
                raise ValueError("action domain and service are invalid")
            target = action["target"]
            data = action["data"]
            if not isinstance(target, Mapping) or not all(isinstance(key, str) for key in target):
                raise ValueError("action target must be an object")
            if not isinstance(data, Mapping) or not all(isinstance(key, str) for key in data):
                raise ValueError("action data must be an object")
            target_mapping = cast(Mapping[str, object], target)
            data_mapping = cast(Mapping[str, object], data)
            if not set(target_mapping).issubset(_TARGET_KEYS):
                raise ValueError("action target has an unsupported key")
            remaining = [_MAX_JSON_NODES]
            _validate_json_value(dict(target_mapping), depth=0, remaining=remaining)
            _validate_json_value(dict(data_mapping), depth=0, remaining=remaining)
            actions[(panel, scene_id)] = _Action(
                domain,
                service,
                copy.deepcopy(dict(target_mapping)),
                copy.deepcopy(dict(data_mapping)),
            )
        return actions
    except (TypeError, ValueError):
        _LOGGER.warning("Ignored invalid Brilliant configured scene actions")
        return {}


def get_scene_control(hass: HomeAssistant) -> SceneControl:
    """Return the integration singleton through its control-plane owner."""
    from .ha_control import get_control_plane

    return get_control_plane(hass).scene_control
