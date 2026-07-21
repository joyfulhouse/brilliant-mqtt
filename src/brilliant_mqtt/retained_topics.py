"""Durable ownership ledger for panel-retained MQTT topics.

The ledger is the panel agent's fail-closed record of which retained messages
belong to one concrete panel. A new topic is durably recorded before either
the broker-side ownership manifest or the retained value is published.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from brilliant_mqtt.protocols import MqttClient

SCHEMA_VERSION = 1
MAX_TOPICS = 4_096
MAX_MANIFEST_BYTES = 256 * 1024

_MANIFEST_KEYS = frozenset({"schema_version", "panel_slug", "topics"})
_PANEL_PATTERN = re.compile(r"[a-z0-9][a-z0-9_-]{0,62}")


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


class RetainedTopicLedger:
    """Persist and publish the retained topics owned by one real panel."""

    def __init__(self, panel_slug: str, path: Path) -> None:
        if _PANEL_PATTERN.fullmatch(panel_slug) is None or panel_slug == "mesh":
            raise RetainedLedgerError("invalid panel slug for retained ledger")
        self._panel_slug = panel_slug
        self._path = path
        self._topics: frozenset[str] = frozenset()
        self._loaded = False
        self._lock = asyncio.Lock()

    @property
    def ownership_topic(self) -> str:
        """Broker topic carrying this panel's ownership manifest."""
        return f"brilliant/{self._panel_slug}/ownership"

    @property
    def topics(self) -> frozenset[str]:
        """Immutable snapshot of the currently owned retained topics."""
        return self._topics

    async def async_load(self) -> None:
        """Load and strictly validate an existing ledger; missing means empty."""
        async with self._lock:
            try:
                raw = await asyncio.to_thread(self._path.read_bytes)
            except FileNotFoundError:
                self._topics = frozenset()
                self._loaded = True
                return
            except OSError as error:
                raise RetainedLedgerError("could not read retained ledger") from error

            manifest = _decode_manifest(raw, self._panel_slug)
            self._topics = manifest.topics
            self._loaded = True

    async def async_publish(self, mqtt: MqttClient, topic: str, payload: str) -> None:
        """Claim *topic*, acknowledge the manifest, then publish its value."""
        async with self._lock:
            self._require_loaded()
            _validate_topic(self._panel_slug, topic)
            enlarged = self._topics | {topic}
            manifest_payload = _new_manifest(self._panel_slug, enlarged).to_payload()
            if enlarged != self._topics:
                await self._async_persist(manifest_payload)
                self._topics = enlarged

            await mqtt.publish(self.ownership_topic, manifest_payload, retain=True, qos=1)
            await mqtt.publish(topic, payload, retain=True, qos=0)

    async def async_clear(self, mqtt: MqttClient, topic: str) -> None:
        """Clear one owned retained topic, then persist and publish its removal."""
        async with self._lock:
            self._require_loaded()
            await self._async_clear_locked(mqtt, topic)

    async def async_clear_all(self, mqtt: MqttClient) -> None:
        """Clear every owned retained topic and clear the ownership topic last."""
        async with self._lock:
            self._require_loaded()
            for topic in sorted(self._topics):
                await self._async_clear_locked(mqtt, topic)
            await mqtt.publish(self.ownership_topic, "", retain=True, qos=1)

    async def _async_clear_locked(self, mqtt: MqttClient, topic: str) -> None:
        _validate_topic(self._panel_slug, topic)
        if topic not in self._topics:
            raise RetainedLedgerError("refusing to clear a topic not owned by this ledger")

        await mqtt.publish(topic, "", retain=True, qos=0)
        smaller = self._topics - {topic}
        manifest_payload = _new_manifest(self._panel_slug, smaller).to_payload()
        await self._async_persist(manifest_payload)
        self._topics = smaller
        await mqtt.publish(self.ownership_topic, manifest_payload, retain=True, qos=1)

    async def _async_persist(self, payload: str) -> None:
        try:
            await asyncio.to_thread(_write_payload, self._path, payload)
        except OSError as error:
            raise RetainedLedgerError("could not persist retained ledger") from error

    def _require_loaded(self) -> None:
        if not self._loaded:
            raise RetainedLedgerError("retained ledger must be loaded before use")


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
    temporary = path.with_name(f"{path.name}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as file_handle:
            descriptor = -1
            file_handle.write(payload)
            file_handle.flush()
            os.fsync(file_handle.fileno())
    finally:
        if descriptor >= 0:
            os.close(descriptor)

    os.replace(temporary, path)
    directory_descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_descriptor)
    finally:
        os.close(directory_descriptor)
