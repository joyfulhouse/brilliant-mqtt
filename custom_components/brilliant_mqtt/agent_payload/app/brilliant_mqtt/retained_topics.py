"""Durable ownership ledger for panel-retained MQTT topics.

The ledger is the panel agent's fail-closed record of which retained messages
belong to one concrete panel. A new topic is durably recorded before either
the broker-side ownership manifest or the retained value is published.
"""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
import threading
import weakref
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

from brilliant_mqtt.ha_control_protocol import is_panel_slug
from brilliant_mqtt.protocols import MqttClient

SCHEMA_VERSION = 1
MAX_TOPICS = 4_096
MAX_MANIFEST_BYTES = 256 * 1024

_MANIFEST_KEYS = frozenset({"schema_version", "panel_slug", "topics"})


class RetainedLedgerError(RuntimeError):
    """The retained-topic ledger is invalid or could not be persisted."""


@dataclass(frozen=True, slots=True)
class OwnedTopicsManifest:
    """Validated version-1 ownership manifest."""

    schema_version: int
    panel_slug: str
    topics: frozenset[str]

    def to_payload(self) -> str:
        """Serialize as canonical compact UTF-8 JSON."""
        payload = json.dumps(
            {
                "schema_version": self.schema_version,
                "panel_slug": self.panel_slug,
                "topics": sorted(self.topics),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        if len(payload.encode("utf-8")) > MAX_MANIFEST_BYTES:
            raise RetainedLedgerError(
                f"retained ledger canonical JSON exceeds {MAX_MANIFEST_BYTES} bytes"
            )
        return payload


@dataclass
class _PathState:
    """Mutation state shared by live ledgers for one normalized path."""

    panel_slug: str
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    topics: frozenset[str] = frozenset()
    loaded: bool = False


_PATH_STATES: weakref.WeakValueDictionary[Path, _PathState] = weakref.WeakValueDictionary()
_PATH_STATES_LOCK = threading.Lock()


class RetainedTopicLedger:
    """Persist and publish the retained topics owned by one real panel."""

    def __init__(self, panel_slug: str, path: Path) -> None:
        if not is_panel_slug(panel_slug) or panel_slug == "mesh":
            raise RetainedLedgerError("invalid panel slug for retained ledger")
        self._panel_slug = panel_slug
        self._path = _normalize_path(path)
        self._state = _path_state(self._path, panel_slug)
        self._loaded = False
        self._manifest_acknowledged = False

    @property
    def ownership_topic(self) -> str:
        """Broker topic carrying this panel's ownership manifest."""
        return f"brilliant/{self._panel_slug}/ownership"

    @property
    def topics(self) -> frozenset[str]:
        """Immutable snapshot of the currently owned retained topics."""
        return self._state.topics

    async def async_load(self) -> None:
        """Load and strictly validate an existing ledger; missing means empty."""
        async with self._state.lock:
            self._loaded = False
            self._manifest_acknowledged = False
            self._state.loaded = False
            self._state.topics = frozenset()
            try:
                raw = await asyncio.to_thread(self._path.read_bytes)
            except FileNotFoundError:
                self._state.loaded = True
                self._loaded = True
                return
            except OSError as error:
                raise RetainedLedgerError("could not read retained ledger") from error

            manifest = _decode_manifest(raw, self._panel_slug)
            self._state.topics = manifest.topics
            self._state.loaded = True
            self._loaded = True

    async def async_publish(self, mqtt: MqttClient, topic: str, payload: str) -> None:
        """Claim *topic*, acknowledge changed ownership, then publish its value."""
        async with self._state.lock:
            self._require_loaded()
            _validate_topic(self._panel_slug, topic)
            enlarged = self._state.topics | {topic}
            ownership_changed = enlarged != self._state.topics
            manifest_payload: str | None = None
            if ownership_changed:
                manifest_payload = _new_manifest(self._panel_slug, enlarged).to_payload()
                self._manifest_acknowledged = False
                await self._async_persist(manifest_payload, enlarged)

            if not self._manifest_acknowledged:
                if manifest_payload is None:
                    manifest_payload = _new_manifest(
                        self._panel_slug,
                        self._state.topics,
                    ).to_payload()
                await mqtt.publish(self.ownership_topic, manifest_payload, retain=True, qos=1)
                self._manifest_acknowledged = True
            await mqtt.publish(topic, payload, retain=True, qos=0)

    async def async_clear(self, mqtt: MqttClient, topic: str) -> None:
        """Clear one owned retained topic, then persist and publish its removal."""
        async with self._state.lock:
            self._require_loaded()
            await self._async_clear_locked(mqtt, topic)

    async def async_clear_all(self, mqtt: MqttClient) -> None:
        """Clear every owned retained topic and clear the ownership topic last."""
        async with self._state.lock:
            self._require_loaded()
            for topic in sorted(self._state.topics):
                await self._async_clear_locked(mqtt, topic)
            self._manifest_acknowledged = False
            await mqtt.publish(self.ownership_topic, "", retain=True, qos=1)

    async def _async_clear_locked(self, mqtt: MqttClient, topic: str) -> None:
        _validate_topic(self._panel_slug, topic)
        if topic not in self._state.topics:
            raise RetainedLedgerError("refusing to clear a topic not owned by this ledger")

        await mqtt.publish(topic, "", retain=True, qos=1)
        self._manifest_acknowledged = False
        smaller = self._state.topics - {topic}
        manifest_payload = _new_manifest(self._panel_slug, smaller).to_payload()
        await self._async_persist(manifest_payload, smaller)
        await mqtt.publish(self.ownership_topic, manifest_payload, retain=True, qos=1)
        self._manifest_acknowledged = True

    async def _async_persist(self, payload: str, topics: frozenset[str]) -> None:
        write = asyncio.create_task(asyncio.to_thread(_write_payload, self._path, payload))
        cancellation: asyncio.CancelledError | None = None
        while not write.done():
            try:
                await asyncio.shield(write)
            except asyncio.CancelledError as error:
                if cancellation is None:
                    cancellation = error
            except OSError:
                break
        try:
            write.result()
        except OSError as error:
            raise RetainedLedgerError("could not persist retained ledger") from error
        self._state.topics = topics
        if cancellation is not None:
            raise cancellation

    def _require_loaded(self) -> None:
        if not self._loaded or not self._state.loaded:
            raise RetainedLedgerError("retained ledger must be loaded before use")


def _normalize_path(path: Path) -> Path:
    try:
        return path.expanduser().resolve(strict=False)
    except (OSError, RuntimeError) as error:
        raise RetainedLedgerError("could not normalize retained ledger path") from error


def _path_state(path: Path, panel_slug: str) -> _PathState:
    with _PATH_STATES_LOCK:
        state = _PATH_STATES.get(path)
        if state is None:
            state = _PathState(panel_slug=panel_slug)
            _PATH_STATES[path] = state
        elif state.panel_slug != panel_slug:
            raise RetainedLedgerError(
                "retained ledger path is already assigned to a different panel"
            )
        return state


def _new_manifest(panel_slug: str, topics: frozenset[str]) -> OwnedTopicsManifest:
    if len(topics) > MAX_TOPICS:
        raise RetainedLedgerError(f"retained ledger exceeds {MAX_TOPICS:,} topics")
    for topic in topics:
        _validate_topic(panel_slug, topic)
    return OwnedTopicsManifest(
        schema_version=SCHEMA_VERSION,
        panel_slug=panel_slug,
        topics=topics,
    )


def _decode_manifest(payload: bytes, expected_panel_slug: str) -> OwnedTopicsManifest:
    try:
        decoded = json.loads(payload)
    except (json.JSONDecodeError, RecursionError, UnicodeDecodeError) as error:
        raise RetainedLedgerError("invalid retained ledger JSON") from error
    if not isinstance(decoded, dict):
        raise RetainedLedgerError("invalid retained ledger: expected a JSON object")

    value = cast(dict[str, object], decoded)
    if set(value) != _MANIFEST_KEYS:
        raise RetainedLedgerError("invalid retained ledger keys")
    if type(value["schema_version"]) is not int or value["schema_version"] != SCHEMA_VERSION:
        raise RetainedLedgerError("unsupported retained ledger schema")
    if value["panel_slug"] != expected_panel_slug:
        raise RetainedLedgerError("retained ledger panel slug does not match runtime panel")

    raw_topics = value["topics"]
    if not isinstance(raw_topics, list):
        raise RetainedLedgerError("invalid retained ledger topics: expected a list")
    if len(raw_topics) > MAX_TOPICS:
        raise RetainedLedgerError(f"retained ledger exceeds {MAX_TOPICS:,} topics")
    if not all(isinstance(topic, str) for topic in raw_topics):
        raise RetainedLedgerError("invalid retained ledger topic: expected strings")

    topics = cast(list[str], raw_topics)
    if len(set(topics)) != len(topics):
        raise RetainedLedgerError("invalid retained ledger: duplicate topics")
    manifest = _new_manifest(expected_panel_slug, frozenset(topics))
    manifest.to_payload()
    return manifest


def _validate_topic(panel_slug: str, topic: str) -> None:
    if not topic or any(marker in topic for marker in ("+", "#", "\x00")):
        raise RetainedLedgerError("invalid retained topic: expected a concrete topic")
    parts = topic.split("/")
    if any(not part for part in parts):
        raise RetainedLedgerError("invalid retained topic shape")

    if parts == ["brilliant", panel_slug, "availability"]:
        return
    if parts == ["brilliant", panel_slug, "bridge"]:
        return
    if (
        len(parts) == 4
        and parts[0] == "brilliant"
        and parts[1] == panel_slug
        and parts[3] == "state"
    ):
        return
    if (
        len(parts) == 4
        and parts[0] == "homeassistant"
        and parts[3] == "config"
        and parts[2].startswith(f"brilliant_{panel_slug}_")
    ):
        return
    raise RetainedLedgerError("retained topic is not owned by this panel")


def _write_payload(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    temporary = Path(temporary_name)
    replaced = False
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as file_handle:
            descriptor = -1
            file_handle.write(payload)
            file_handle.flush()
            os.fsync(file_handle.fileno())
        os.replace(temporary, path)
        replaced = True
    finally:
        try:
            if descriptor >= 0:
                os.close(descriptor)
        finally:
            if not replaced:
                try:
                    temporary.unlink()
                except FileNotFoundError:
                    pass

    directory_descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)
