"""Singleton Home Assistant-owned MQTT entity control plane."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import OrderedDict, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

from homeassistant.components import mqtt
from homeassistant.components.mqtt.models import ReceiveMessage
from homeassistant.const import ATTR_ENTITY_ID, EVENT_STATE_CHANGED
from homeassistant.core import CALLBACK_TYPE, Event, HomeAssistant, callback
from homeassistant.helpers import area_registry as ar
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import label_registry as lr
from homeassistant.helpers.event import async_call_later

from .const import (
    CONF_HA_CONTROL_DOMAINS,
    CONF_HA_CONTROL_ENABLED,
    CONF_HA_CONTROL_LABEL,
    CONF_HA_MIRROR_LABEL,
    CONF_MAX_MIRRORED_ENTITIES,
    CONF_PANEL,
    CONF_ROOM_OVERRIDES,
    DATA_CONTROL_PLANE,
    DEFAULT_HA_CONTROL_DOMAINS,
    DEFAULT_HA_CONTROL_ENABLED,
    DEFAULT_HA_CONTROL_LABEL,
    DEFAULT_MAX_MIRRORED_ENTITIES,
    DOMAIN,
)
from .ha_control_manifest import (
    SUPPORTED_DOMAINS,
    ControlSettings,
    ManifestEntity,
    ManifestSnapshot,
    build_manifest,
    build_state_payload,
)
from .ha_control_protocol import (
    MAPPING_VERSION,
    SCHEMA_VERSION,
    decode_command,
    encode_json,
    manifest_topic,
    result_topic,
    state_topic,
    validate_entity_command_context,
)

if TYPE_CHECKING:
    from . import BrilliantMqttConfigEntry

_LOGGER = logging.getLogger(__name__)

_COMMAND_TOPIC = "brilliant/ha-control/v1/command/+"
_REGISTRY_DEBOUNCE_SECONDS = 0.5
_RESULT_CACHE_LIMIT = 1_024
_RESULT_CACHE_TTL_SECONDS = 10 * 60


def _monotonic() -> float:
    """Return monotonic time behind a narrow test seam."""
    return time.monotonic()


def _timestamp_ms() -> int:
    """Return current Unix time in whole milliseconds."""
    return time.time_ns() // 1_000_000


@dataclass(frozen=True, slots=True)
class _ServiceRoute:
    service: str
    data_key: str | None = None
    minimum: int | None = None
    maximum: int | None = None


_SERVICE_ROUTES: Mapping[tuple[str, str], _ServiceRoute] = {
    ("light", "turn_on"): _ServiceRoute("turn_on"),
    ("light", "turn_off"): _ServiceRoute("turn_off"),
    ("light", "set_brightness"): _ServiceRoute("turn_on", "brightness", 0, 255),
    ("switch", "turn_on"): _ServiceRoute("turn_on"),
    ("switch", "turn_off"): _ServiceRoute("turn_off"),
    ("lock", "lock"): _ServiceRoute("lock"),
    ("lock", "unlock"): _ServiceRoute("unlock"),
    ("cover", "open"): _ServiceRoute("open_cover"),
    ("cover", "close"): _ServiceRoute("close_cover"),
    ("cover", "set_position"): _ServiceRoute("set_cover_position", "position", 0, 100),
    ("cover", "set_tilt"): _ServiceRoute("set_cover_tilt_position", "tilt_position", 0, 100),
}


@dataclass(frozen=True, slots=True)
class _CachedResult:
    """One byte-stable command result and its insertion time."""

    created: float
    topic: str
    payload: str


class HaControlPlane:
    """Publish one whole-home HA manifest and execute constrained commands."""

    def __init__(self, hass: HomeAssistant) -> None:
        self.hass = hass
        self._entries: dict[str, BrilliantMqttConfigEntry] = {}
        self._manifest: ManifestSnapshot | None = None
        self._manifest_body: str | None = None
        self._state_sequences: defaultdict[str, int] = defaultdict(int)
        self._unsubscribers: list[CALLBACK_TYPE] = []
        self._debounce_cancel: CALLBACK_TYPE | None = None
        self._settings: ControlSettings | None = None
        self._owner_entry_id: str | None = None
        self._results: OrderedDict[str, _CachedResult] = OrderedDict()
        self._lifecycle_lock = asyncio.Lock()
        self._command_lock = asyncio.Lock()
        self._started = False
        self._accept_commands = False
        self._rebuilding = False
        self._hard_fenced = False
        self._command_fence_generation = 0

    @property
    def started(self) -> bool:
        """Return whether MQTT and HA event listeners are active."""
        return self._started

    @property
    def owner_entry_id(self) -> str | None:
        """Return the config entry currently supplying global settings."""
        return self._owner_entry_id

    @property
    def result_cache_size(self) -> int:
        """Return the current bounded idempotency-cache size."""
        return len(self._results)

    async def async_attach(self, entry: BrilliantMqttConfigEntry) -> None:
        """Attach one loaded panel entry and start when any entry is enabled."""
        self._accept_commands = False
        self._rebuilding = True
        try:
            async with self._lifecycle_lock:
                self._entries[entry.entry_id] = entry
                await self._async_reload_settings_locked()
        except BaseException:
            self._rebuilding = False
            self._accept_commands = (
                self._started and self._manifest is not None and not self._hard_fenced
            )
            raise
        self._hard_fenced = False
        self._rebuilding = False
        self._accept_commands = self._started and self._manifest is not None

    async def async_detach(self, entry_id: str) -> None:
        """Detach one panel and stop only after no enabled entries remain."""
        # Fence new/queued commands before waiting behind an in-flight command that
        # owns the lifecycle lock. The active command may drain; later callbacks fail
        # closed even if they entered the lock queue before this detach task.
        self._accept_commands = False
        self._rebuilding = False
        self._hard_fenced = True
        self._command_fence_generation += 1
        try:
            async with self._lifecycle_lock:
                self._entries.pop(entry_id, None)
                await self._async_reload_settings_locked()
        except BaseException:
            self._rebuilding = False
            raise
        self._hard_fenced = False
        self._rebuilding = False
        self._accept_commands = self._started and self._manifest is not None

    async def async_reload_settings(self) -> None:
        """Re-elect the settings owner and rebuild only when necessary."""
        self._accept_commands = False
        self._rebuilding = True
        try:
            async with self._lifecycle_lock:
                await self._async_reload_settings_locked()
        except BaseException:
            self._rebuilding = False
            self._accept_commands = (
                self._started and self._manifest is not None and not self._hard_fenced
            )
            raise
        self._hard_fenced = False
        self._rebuilding = False
        self._accept_commands = self._started and self._manifest is not None

    async def async_start(self) -> None:
        """Start publication if an enabled attached entry exists."""
        self._accept_commands = False
        self._rebuilding = True
        try:
            async with self._lifecycle_lock:
                await self._async_reload_settings_locked()
        except BaseException:
            self._rebuilding = False
            self._accept_commands = False
            raise
        self._hard_fenced = False
        self._rebuilding = False
        self._accept_commands = self._started and self._manifest is not None

    async def async_stop(self) -> None:
        """Stop subscriptions/listeners and cancel pending work."""
        self._accept_commands = False
        self._hard_fenced = True
        self._command_fence_generation += 1
        try:
            async with self._lifecycle_lock:
                await self._async_stop_locked()
        finally:
            self._hard_fenced = False

    async def _async_reload_settings_locked(self) -> None:
        owner = self._enabled_owner()
        if owner is None:
            self._owner_entry_id = None
            self._settings = None
            await self._async_stop_locked()
            return

        settings = _settings_from_entry(owner)
        self._owner_entry_id = owner.entry_id
        self._settings = settings
        if not self._started:
            await self._async_start_locked()
            return
        await self._async_rebuild_manifest()

    async def _async_start_locked(self) -> None:
        if self._started or self._settings is None:
            return
        self._accept_commands = False
        try:
            mqtt_unsubscribe = await mqtt.async_subscribe(
                self.hass, _COMMAND_TOPIC, self._async_command_received
            )
            self._unsubscribers.append(mqtt_unsubscribe)
            self._unsubscribers.extend(
                (
                    self.hass.bus.async_listen(EVENT_STATE_CHANGED, self._async_state_changed),
                    self.hass.bus.async_listen(
                        er.EVENT_ENTITY_REGISTRY_UPDATED, self._registry_changed
                    ),
                    self.hass.bus.async_listen(
                        dr.EVENT_DEVICE_REGISTRY_UPDATED, self._registry_changed
                    ),
                    self.hass.bus.async_listen(
                        ar.EVENT_AREA_REGISTRY_UPDATED, self._registry_changed
                    ),
                    self.hass.bus.async_listen(
                        lr.EVENT_LABEL_REGISTRY_UPDATED, self._registry_changed
                    ),
                )
            )
            self._started = True
            await self._async_rebuild_manifest()
        except BaseException:
            await self._async_stop_locked()
            raise

    async def _async_stop_locked(self) -> None:
        self._started = False
        self._accept_commands = False
        self._rebuilding = False
        if self._debounce_cancel is not None:
            self._debounce_cancel()
            self._debounce_cancel = None
        for unsubscribe in reversed(self._unsubscribers):
            unsubscribe()
        self._unsubscribers.clear()

    def _enabled_owner(self) -> BrilliantMqttConfigEntry | None:
        enabled = (
            entry
            for entry in self._entries.values()
            if entry.data.get(CONF_HA_CONTROL_ENABLED, DEFAULT_HA_CONTROL_ENABLED) is True
        )
        return min(
            enabled,
            key=lambda entry: (str(entry.data.get(CONF_PANEL, "")), entry.entry_id),
            default=None,
        )

    async def _async_rebuild_manifest(self) -> None:
        if not self._started or self._settings is None:
            self._rebuilding = False
            return
        self._accept_commands = False
        self._rebuilding = True
        previous_manifest = self._manifest
        try:
            revision = 1 if previous_manifest is None else previous_manifest.revision + 1
            generated_at_ms = _timestamp_ms()
            candidate = build_manifest(self.hass, self._settings, revision, generated_at_ms)
            body = _canonical_manifest_body(candidate)
            if body == self._manifest_body:
                self._accept_commands = (
                    self._started and previous_manifest is not None and not self._hard_fenced
                )
                return

            # States are staged first. They are harmless while unreferenced by the
            # retained manifest. Publishing the manifest last is the broker-visible
            # commit point for this complete candidate.
            for entity in candidate.entities:
                await self._async_publish_state(entity, generated_at_ms)
            await mqtt.async_publish(
                self.hass,
                manifest_topic(),
                encode_json(candidate.as_payload()),
                retain=True,
            )
            self._manifest = candidate
            self._manifest_body = body
        except BaseException:
            # Manifest-last means a failed candidate never displaced the previous
            # broker manifest. Reopen that old authority when it exists; initial
            # startup has no committed authority and therefore remains closed.
            self._accept_commands = (
                self._started and previous_manifest is not None and not self._hard_fenced
            )
            raise
        finally:
            self._rebuilding = False
        self._accept_commands = (
            self._started and self._manifest is not None and not self._hard_fenced
        )

    async def _async_publish_state(
        self, entity: ManifestEntity, generated_at_ms: int | None = None
    ) -> None:
        self._state_sequences[entity.stable_id] += 1
        payload = build_state_payload(
            self.hass.states.get(entity.entity_id),
            entity,
            self._state_sequences[entity.stable_id],
            generated_at_ms if generated_at_ms is not None else _timestamp_ms(),
        )
        await mqtt.async_publish(
            self.hass, state_topic(entity.stable_id), encode_json(payload), retain=True
        )

    async def _async_state_changed(self, event: Event[Any]) -> None:
        async with self._lifecycle_lock:
            if not self._started or self._manifest is None:
                return
            entity_id = event.data.get("entity_id")
            if not isinstance(entity_id, str):
                return
            entity = next(
                (item for item in self._manifest.entities if item.entity_id == entity_id),
                None,
            )
            if entity is not None:
                await self._async_publish_state(entity)

    @callback
    def _registry_changed(self, _event: Event[Any]) -> None:
        if not self._started or self._debounce_cancel is not None:
            return
        self._debounce_cancel = async_call_later(
            self.hass, _REGISTRY_DEBOUNCE_SECONDS, self._async_registry_debounce
        )

    async def _async_registry_debounce(self, _now: datetime) -> None:
        self._accept_commands = False
        self._rebuilding = True
        async with self._lifecycle_lock:
            self._debounce_cancel = None
            await self._async_rebuild_manifest()

    async def _async_command_received(self, message: ReceiveMessage) -> None:
        generation = self._command_fence_generation
        if self._hard_fenced or (not self._accept_commands and not self._rebuilding):
            return
        async with self._lifecycle_lock:
            if (
                not self._started
                or not self._accept_commands
                or self._hard_fenced
                or generation != self._command_fence_generation
            ):
                return
            async with self._command_lock:
                await self._async_execute_command(message)

    async def _async_execute_command(self, message: ReceiveMessage) -> None:
        started = _monotonic()
        raw_payload = str(message.payload)
        raw_command_id, raw_stable_id = _extract_wire_ids(raw_payload)
        self._purge_results(started)
        if raw_command_id is not None and (cached := self._results.get(raw_command_id)) is not None:
            await mqtt.async_publish(self.hass, cached.topic, cached.payload, retain=False)
            return

        command_id = raw_command_id
        entity_stable_id = raw_stable_id or _topic_stable_id(message.topic)
        accepted = False
        error: str | None = None
        try:
            command = decode_command(raw_payload, now_ms=_timestamp_ms())
            command_id = command.command_id
            entity_stable_id = command.stable_id
            validate_entity_command_context(
                command,
                topic_stable_id=_topic_stable_id(message.topic),
                retained=message.retain,
            )
            entity = self._manifest_entity(command.stable_id)
            if entity is None:
                raise ValueError("entity is not present in the current manifest")
            if command.kind not in entity.commands:
                raise ValueError("command is not allowed by the current manifest")
            route = _SERVICE_ROUTES.get((entity.domain, command.kind))
            if route is None:
                raise ValueError("command has no service route")
            service_data = _service_data(entity, route, command.value)
            try:
                await self.hass.services.async_call(
                    entity.domain,
                    route.service,
                    service_data,
                    blocking=True,
                )
            except Exception as service_error:
                _LOGGER.warning(
                    "HA control service call failed for %s (%s): %s",
                    entity.entity_id,
                    type(service_error).__name__,
                    _sanitize_log_message(service_error),
                )
                error = "service_call_failed"
            else:
                accepted = True
        except ValueError as validation_error:
            error = str(validation_error)

        if command_id is None or entity_stable_id is None:
            return
        elapsed_ms = max(0, int((_monotonic() - started) * 1_000))
        result = encode_json(
            {
                "schema_version": SCHEMA_VERSION,
                "mapping_version": MAPPING_VERSION,
                "command_id": command_id,
                "stable_id": entity_stable_id,
                "accepted": accepted,
                "resulting_sequence": self._state_sequences[entity_stable_id],
                "timestamp_ms": _timestamp_ms(),
                "error": error,
                "elapsed_ms": elapsed_ms,
            }
        )
        topic = result_topic(command_id)
        self._results[command_id] = _CachedResult(started, topic, result)
        while len(self._results) > _RESULT_CACHE_LIMIT:
            self._results.popitem(last=False)
        await mqtt.async_publish(self.hass, topic, result, retain=False)

    def _manifest_entity(self, entity_stable_id: str) -> ManifestEntity | None:
        if self._manifest is None:
            return None
        return next(
            (entity for entity in self._manifest.entities if entity.stable_id == entity_stable_id),
            None,
        )

    def _purge_results(self, now: float) -> None:
        while self._results:
            command_id, cached = next(iter(self._results.items()))
            if now - cached.created <= _RESULT_CACHE_TTL_SECONDS:
                break
            del self._results[command_id]


def get_control_plane(hass: HomeAssistant) -> HaControlPlane:
    """Return the integration-wide singleton control plane."""
    domain_data: dict[str, Any] = hass.data.setdefault(DOMAIN, {})
    plane = domain_data.get(DATA_CONTROL_PLANE)
    if isinstance(plane, HaControlPlane):
        return plane
    plane = HaControlPlane(hass)
    domain_data[DATA_CONTROL_PLANE] = plane
    return plane


def _settings_from_entry(entry: BrilliantMqttConfigEntry) -> ControlSettings:
    data = entry.data
    label = data.get(CONF_HA_CONTROL_LABEL)
    if not isinstance(label, str) or not label:
        legacy_label = data.get(CONF_HA_MIRROR_LABEL)
        label = (
            legacy_label
            if isinstance(legacy_label, str) and legacy_label
            else DEFAULT_HA_CONTROL_LABEL
        )

    raw_overrides = data.get(CONF_ROOM_OVERRIDES, {})
    room_overrides = (
        {
            key: value
            for key, value in raw_overrides.items()
            if isinstance(key, str) and isinstance(value, str)
        }
        if isinstance(raw_overrides, Mapping)
        else {}
    )
    raw_domains = data.get(CONF_HA_CONTROL_DOMAINS, DEFAULT_HA_CONTROL_DOMAINS)
    enabled_domains = (
        frozenset(domain for domain in raw_domains if domain in SUPPORTED_DOMAINS)
        if isinstance(raw_domains, Sequence) and not isinstance(raw_domains, str)
        else frozenset(DEFAULT_HA_CONTROL_DOMAINS)
    )
    raw_maximum = data.get(CONF_MAX_MIRRORED_ENTITIES, DEFAULT_MAX_MIRRORED_ENTITIES)
    maximum = raw_maximum if type(raw_maximum) is int else DEFAULT_MAX_MIRRORED_ENTITIES
    return ControlSettings(
        label_name=label,
        room_overrides=room_overrides,
        enabled_domains=enabled_domains,
        maximum_entities=max(1, min(200, maximum)),
    )


def _canonical_manifest_body(snapshot: ManifestSnapshot) -> str:
    payload = snapshot.as_payload()
    del payload["revision"]
    del payload["generated_at_ms"]
    return encode_json(payload)


def _topic_stable_id(topic: str) -> str:
    return topic.rsplit("/", maxsplit=1)[-1]


def _extract_wire_ids(payload: str) -> tuple[str | None, str | None]:
    try:
        value = json.loads(payload)
    except (json.JSONDecodeError, TypeError):
        return None, None
    if not isinstance(value, dict):
        return None, None
    return _normalized_uuid(value.get("command_id")), _normalized_uuid(value.get("stable_id"))


def _normalized_uuid(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        return str(UUID(value))
    except ValueError:
        return None


def _service_data(entity: ManifestEntity, route: _ServiceRoute, value: object) -> dict[str, object]:
    result: dict[str, object] = {ATTR_ENTITY_ID: entity.entity_id}
    if route.data_key is None:
        if value is not None:
            raise ValueError("command value must be null")
        return result
    if (
        type(value) is not int
        or route.minimum is None
        or route.maximum is None
        or not route.minimum <= value <= route.maximum
    ):
        raise ValueError(
            f"command value must be an integer from {route.minimum} to {route.maximum}"
        )
    result[route.data_key] = value
    return result


def _sanitize_log_message(error: Exception) -> str:
    message = " ".join(str(error).splitlines())
    return "".join(character for character in message if character.isprintable())[:200]
