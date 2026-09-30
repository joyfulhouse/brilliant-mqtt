"""Durable ownership ledger for panel-retained MQTT topics.

The ledger is the panel agent's fail-closed record of which retained messages
belong to one concrete panel. A new topic is durably recorded before either
the broker-side ownership manifest or the retained value is published.
"""

from __future__ import annotations

import asyncio
import functools
import json
import logging
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
# Bound on one durable ledger write, matching aiomqtt's default 10 s wait for a
# broker acknowledgement so storage cannot stall publication longer than MQTT.
PERSIST_DEADLINE_S = 10.0

_LOGGER = logging.getLogger(__name__)

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
    # True while a claim holds the lock only to wait for its durable write.
    claim_pending: bool = False
    # Callers waiting on the lock, per topic, so a later publish of the same
    # topic never overtakes an earlier one through the pending-claim fast path.
    queued: dict[str, int] = field(default_factory=dict)
    # A write that outlived PERSIST_DEADLINE_S. Its thread cannot be stopped,
    # so no other write for this path may start until it finishes.
    stalled_write: asyncio.Future[None] | None = None
    # Set when a write outlived its deadline: the file may no longer match
    # ``topics``. Every publish re-persists ``topics`` until one succeeds.
    disk_stale: bool = False


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
        self._failure: RetainedLedgerError | None = None
        # Topics in the last ownership manifest this instance had acknowledged.
        self._acknowledged_topics: frozenset[str] = frozenset()

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
            self._acknowledged_topics = frozenset()
            self._state.loaded = False
            self._state.topics = frozenset()
            stalled = self._state.stalled_write
            if stalled is not None and not stalled.done():
                raise RetainedLedgerError("retained ledger persistence is still stalled")
            self._state.disk_stale = False
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

    def consume_failure(self) -> RetainedLedgerError | None:
        """Return and clear the first publish failure since the last call.

        Callers that must keep running after a failed publish (the wired
        feedback publisher) cannot propagate it, so the session loop checks
        this each tick and fails closed through its retained-ledger handler.
        """
        failure, self._failure = self._failure, None
        return failure

    async def async_publish(self, mqtt: MqttClient, topic: str, payload: str) -> None:
        """Claim *topic*, acknowledge changed ownership, then publish its value."""
        try:
            await self._async_publish(mqtt, topic, payload)
        except RetainedLedgerError as error:
            if self._failure is None:
                self._failure = error
            raise

    async def _async_publish(self, mqtt: MqttClient, topic: str, payload: str) -> None:
        state = self._state
        if (
            state.claim_pending
            and not state.disk_stale
            and topic in state.topics
            and topic in self._acknowledged_topics
            and not state.queued.get(topic)
        ):
            # Another topic's claim holds the lock only while its write reaches
            # disk. This topic is already durable and in a manifest this ledger
            # acknowledged, so publishing it now cannot outrun its ownership
            # record. An earlier caller queued for the same topic keeps order.
            self._require_loaded()
            await mqtt.publish(topic, payload, retain=True, qos=0)
            return
        state.queued[topic] = state.queued.get(topic, 0) + 1
        try:
            async with state.lock:
                await self._async_publish_locked(mqtt, topic, payload)
        finally:
            state.queued[topic] -= 1
            if not state.queued[topic]:
                del state.queued[topic]

    async def _async_publish_locked(self, mqtt: MqttClient, topic: str, payload: str) -> None:
        state = self._state
        self._require_loaded()
        _validate_topic(self._panel_slug, topic)
        enlarged = state.topics | {topic}
        # A stale file may not record topics this ledger still publishes to.
        ownership_changed = enlarged != state.topics or state.disk_stale
        manifest_payload: str | None = None
        if ownership_changed:
            manifest_payload = _new_manifest(self._panel_slug, enlarged).to_payload()
            self._manifest_acknowledged = False
            state.claim_pending = True
            try:
                await self._async_persist(manifest_payload, enlarged)
            finally:
                state.claim_pending = False

        if not self._manifest_acknowledged:
            if manifest_payload is None:
                manifest_payload = _new_manifest(
                    self._panel_slug,
                    state.topics,
                ).to_payload()
            acknowledged = state.topics
            await mqtt.publish(self.ownership_topic, manifest_payload, retain=True, qos=1)
            self._manifest_acknowledged = True
            self._acknowledged_topics = acknowledged
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
        state = self._state
        if state.stalled_write is not None and not state.stalled_write.done():
            raise RetainedLedgerError("retained ledger persistence is still stalled")
        write = _start_write(self._path, payload)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + PERSIST_DEADLINE_S
        cancellation: asyncio.CancelledError | None = None
        while not write.done():
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            try:
                # asyncio.wait never cancels the write, so cancellation is
                # deferred until the write settles or the deadline passes.
                await asyncio.wait({write}, timeout=remaining)
            except asyncio.CancelledError as error:
                if cancellation is None:
                    cancellation = error
        if not write.done():
            # Fail closed without committing the change. The late write still
            # replaces the file atomically, and later writes wait for it. It may
            # leave the file out of step with ``topics`` (a late clear drops a
            # topic still held in memory), so mark the file stale: the next
            # publish re-persists ``topics`` before anything reaches the broker.
            state.stalled_write = write
            state.disk_stale = True
            write.add_done_callback(functools.partial(_settle_stalled_write, state))
            if cancellation is not None:
                raise cancellation
            raise RetainedLedgerError(
                f"retained ledger persistence exceeded {PERSIST_DEADLINE_S:g} s"
            )
        try:
            write.result()
        except OSError as error:
            if cancellation is not None:
                raise cancellation from error
            raise RetainedLedgerError("could not persist retained ledger") from error
        state.topics = topics
        state.disk_stale = False
        if cancellation is not None:
            raise cancellation

    def _require_loaded(self) -> None:
        if not self._loaded or not self._state.loaded:
            raise RetainedLedgerError("retained ledger must be loaded before use")


def _start_write(path: Path, payload: str) -> asyncio.Future[None]:
    """Run one durable write on a daemon thread and return its completion.

    Not the default executor: ``asyncio.run`` joins that at exit (unbounded on
    the panel's Python 3.10), so a stalled write would hold the process past
    its fail-closed deadline. A daemon thread killed at exit leaves the target
    whole, because only ``os.replace`` changes it; at worst a sibling
    temporary file is left behind.
    """
    loop = asyncio.get_running_loop()
    done: asyncio.Future[None] = loop.create_future()

    def settle(error: BaseException | None) -> None:
        if done.done():
            return
        if error is None:
            done.set_result(None)
        else:
            done.set_exception(error)

    def run() -> None:
        error: BaseException | None = None
        try:
            _write_payload(path, payload)
        except Exception as caught:
            error = caught
        try:
            loop.call_soon_threadsafe(settle, error)
        except RuntimeError:
            pass  # The loop closed while the write stalled; nobody awaits it.

    threading.Thread(target=run, name="retained-ledger-write", daemon=True).start()
    return done


def _settle_stalled_write(state: _PathState, write: asyncio.Future[None]) -> None:
    # This callback also keeps the path state alive until the write settles,
    # so a replacement ledger for the path still sees the stalled write.
    if state.stalled_write is write:
        state.stalled_write = None
    error = None if write.cancelled() else write.exception()
    if error is not None:
        _LOGGER.warning("RETAINED_LEDGER_LATE_WRITE_FAILED %s", type(error).__name__)


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
