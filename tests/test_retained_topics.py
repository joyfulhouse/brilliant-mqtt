from __future__ import annotations

import asyncio
import json
import os
import stat
import threading
from collections.abc import Callable
from pathlib import Path
from typing import NoReturn

import pytest

from brilliant_mqtt import retained_topics as retained_topics_module
from brilliant_mqtt.retained_topics import RetainedLedgerError, RetainedTopicLedger
from tests.fakes import FakeMqtt

PANEL = "kitchen"
MAX_TOPICS = 4_096
MAX_MANIFEST_BYTES = 256 * 1024


def _manifest(topics: list[str], *, panel: str = PANEL, **extra: object) -> str:
    value: dict[str, object] = {
        "schema_version": 1,
        "panel_slug": panel,
        "topics": topics,
        **extra,
    }
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


async def _loaded_ledger(path: Path) -> RetainedTopicLedger:
    ledger = RetainedTopicLedger(PANEL, path)
    await ledger.async_load()
    return ledger


class RecordingFailureMqtt(FakeMqtt):
    def __init__(self, fail_topic: str) -> None:
        super().__init__()
        self._fail_topic = fail_topic

    async def publish(
        self,
        topic: str,
        payload: str,
        retain: bool = False,
        qos: int = 0,
    ) -> None:
        await super().publish(topic, payload, retain, qos)
        if topic == self._fail_topic:
            raise RuntimeError(f"publish failed: {topic}")


class RecordingOneShotFailureMqtt(FakeMqtt):
    def __init__(self, fail_topic: str) -> None:
        super().__init__()
        self._fail_topic = fail_topic
        self._failed = False

    async def publish(
        self,
        topic: str,
        payload: str,
        retain: bool = False,
        qos: int = 0,
    ) -> None:
        await super().publish(topic, payload, retain, qos)
        if topic == self._fail_topic and not self._failed:
            self._failed = True
            raise RuntimeError(f"publish failed once: {topic}")


async def test_missing_ledger_loads_empty_without_creating_a_file(tmp_path: Path) -> None:
    path = tmp_path / "owned-topics.json"
    ledger = RetainedTopicLedger(PANEL, path)

    await ledger.async_load()

    assert ledger.ownership_topic == "brilliant/kitchen/ownership"
    assert ledger.topics == frozenset()
    assert not path.exists()
    for attribute, value in (
        ("topics", frozenset({"changed"})),
        ("ownership_topic", "changed"),
    ):
        with pytest.raises(AttributeError):
            setattr(ledger, attribute, value)


@pytest.mark.parametrize(
    "panel_slug",
    [
        "a" * 64,
        "panel_" + ("z" * 250),
    ],
    ids=["64-characters", "legacy-long"],
)
async def test_canonical_slugs_have_no_hidden_ledger_length_limit(
    tmp_path: Path,
    panel_slug: str,
) -> None:
    path = tmp_path / "owned-topics.json"
    ledger = RetainedTopicLedger(panel_slug, path)
    mqtt = FakeMqtt()
    topic = f"brilliant/{panel_slug}/availability"
    expected_manifest = json.dumps(
        {
            "panel_slug": panel_slug,
            "schema_version": 1,
            "topics": [topic],
        },
        sort_keys=True,
        separators=(",", ":"),
    )

    await ledger.async_load()
    await ledger.async_publish(mqtt, topic, "online")

    assert ledger.ownership_topic == f"brilliant/{panel_slug}/ownership"
    assert mqtt.published == [
        (ledger.ownership_topic, expected_manifest, True),
        (topic, "online", True),
    ]
    assert mqtt.published_qos == [1, 0]


async def test_valid_ledger_reloads_all_permitted_topic_families(tmp_path: Path) -> None:
    topics = [
        "brilliant/kitchen/availability",
        "brilliant/kitchen/bridge",
        "brilliant/kitchen/light-1/state",
        "homeassistant/light/brilliant_kitchen_light-1/config",
    ]
    path = tmp_path / "owned-topics.json"
    path.write_text(_manifest(topics), encoding="utf-8")

    ledger = await _loaded_ledger(path)

    assert ledger.topics == frozenset(topics)


async def test_failed_repeat_load_invalidates_prior_snapshot(tmp_path: Path) -> None:
    target = "brilliant/kitchen/bridge"
    path = tmp_path / "owned-topics.json"
    path.write_text(_manifest([target]), encoding="utf-8")
    ledger = await _loaded_ledger(path)
    corrupt = b"{corrupt-ledger"
    path.write_bytes(corrupt)

    with pytest.raises(RetainedLedgerError, match="invalid"):
        await ledger.async_load()

    mqtt = FakeMqtt()
    with pytest.raises(RetainedLedgerError, match="loaded"):
        await ledger.async_publish(mqtt, "brilliant/kitchen/availability", "online")

    assert ledger.topics == frozenset()
    assert mqtt.published == []
    assert path.read_bytes() == corrupt


def test_same_path_cannot_be_assigned_to_different_panels(tmp_path: Path) -> None:
    path = tmp_path / "owned-topics.json"
    first_ledger = RetainedTopicLedger(PANEL, path)

    with pytest.raises(RetainedLedgerError, match="path|panel"):
        RetainedTopicLedger("garage", tmp_path / "." / "owned-topics.json")
    assert first_ledger.topics == frozenset()


@pytest.mark.parametrize(
    ("raw", "error_match"),
    [
        ("{not json", "invalid"),
        (_manifest([], unexpected=True), "keys"),
        (_manifest([], panel="garage"), "panel"),
        (_manifest(["brilliant/kitchen/bridge", "brilliant/kitchen/bridge"]), "duplicate"),
        (
            json.dumps(
                {"schema_version": True, "panel_slug": PANEL, "topics": []},
                separators=(",", ":"),
            ),
            "schema",
        ),
        (
            json.dumps(
                {"schema_version": 1, "panel_slug": PANEL, "topics": "not-a-list"},
                separators=(",", ":"),
            ),
            "topics",
        ),
        (
            json.dumps(
                {"schema_version": 1, "panel_slug": PANEL, "topics": [1]},
                separators=(",", ":"),
            ),
            "topic",
        ),
    ],
)
async def test_invalid_existing_ledger_fails_closed(
    tmp_path: Path,
    raw: str,
    error_match: str,
) -> None:
    path = tmp_path / "owned-topics.json"
    path.write_text(raw, encoding="utf-8")
    ledger = RetainedTopicLedger(PANEL, path)

    with pytest.raises(RetainedLedgerError, match=error_match):
        await ledger.async_load()

    assert ledger.topics == frozenset()
    assert path.read_text(encoding="utf-8") == raw


async def test_ledger_rejects_4_097_topics(tmp_path: Path) -> None:
    topics = [f"brilliant/kitchen/peripheral-{index}/state" for index in range(MAX_TOPICS + 1)]
    path = tmp_path / "owned-topics.json"
    path.write_text(_manifest(topics), encoding="utf-8")

    with pytest.raises(RetainedLedgerError, match="4,096|4096|topics"):
        await RetainedTopicLedger(PANEL, path).async_load()


async def test_ledger_rejects_canonical_json_over_256_kib(tmp_path: Path) -> None:
    unique_id = "brilliant_kitchen_" + ("x" * MAX_MANIFEST_BYTES)
    raw = _manifest([f"homeassistant/light/{unique_id}/config"])
    assert len(raw.encode("utf-8")) > MAX_MANIFEST_BYTES
    path = tmp_path / "owned-topics.json"
    path.write_text(raw, encoding="utf-8")

    with pytest.raises(RetainedLedgerError, match="256|large|bytes"):
        await RetainedTopicLedger(PANEL, path).async_load()


@pytest.mark.parametrize(
    "topic",
    [
        "brilliant/garage/availability",
        "brilliant/mesh/light-1/state",
        "homeassistant/light/brilliant_garage_light-1/config",
        "homeassistant/light/brilliant_mesh_light-1/config",
        "homeassistant/light/brilliant_panel_mesh/config",
        "brilliant/kitchen/light-1/set",
        "brilliant/kitchen/ownership",
        "homeassistant/+/brilliant_kitchen_light-1/config",
    ],
)
async def test_publish_rejects_topics_not_owned_by_the_concrete_panel(
    tmp_path: Path,
    topic: str,
) -> None:
    path = tmp_path / "owned-topics.json"
    ledger = await _loaded_ledger(path)
    mqtt = FakeMqtt()

    with pytest.raises(RetainedLedgerError, match="topic"):
        await ledger.async_publish(mqtt, topic, "value")

    assert ledger.topics == frozenset()
    assert mqtt.published == []
    assert not path.exists()


async def test_first_publish_persists_then_publishes_manifest_before_target(
    tmp_path: Path,
) -> None:
    path = tmp_path / "owned-topics.json"
    ledger = await _loaded_ledger(path)
    mqtt = FakeMqtt()
    target = "brilliant/kitchen/light-1/state"
    expected_manifest = _manifest([target])

    await ledger.async_publish(mqtt, target, '{"on":true}')

    assert mqtt.published == [
        ("brilliant/kitchen/ownership", expected_manifest, True),
        (target, '{"on":true}', True),
    ]
    assert mqtt.published_qos == [1, 0]
    assert ledger.topics == frozenset({target})
    assert path.read_text(encoding="utf-8") == expected_manifest


async def test_loaded_manifest_is_acknowledged_once_before_unchanged_hot_path(
    tmp_path: Path,
) -> None:
    target = "brilliant/kitchen/light-1/state"
    expected_manifest = _manifest([target])
    path = tmp_path / "owned-topics.json"
    path.write_text(expected_manifest, encoding="utf-8")
    ledger = await _loaded_ledger(path)
    mqtt = FakeMqtt()

    await ledger.async_publish(mqtt, target, "one")
    await ledger.async_publish(mqtt, target, "two")

    assert mqtt.published == [
        (ledger.ownership_topic, expected_manifest, True),
        (target, "one", True),
        (target, "two", True),
    ]
    assert mqtt.published_qos == [1, 0, 0]


async def test_repeat_load_requires_a_fresh_manifest_ack_before_existing_target(
    tmp_path: Path,
) -> None:
    target = "brilliant/kitchen/light-1/state"
    expected_manifest = _manifest([target])
    path = tmp_path / "owned-topics.json"
    path.write_text(expected_manifest, encoding="utf-8")
    ledger = await _loaded_ledger(path)
    mqtt = FakeMqtt()
    await ledger.async_publish(mqtt, target, "before-reload")
    mqtt.published.clear()
    mqtt.published_qos.clear()

    await ledger.async_load()
    await ledger.async_publish(mqtt, target, "after-reload")

    assert mqtt.published == [
        (ledger.ownership_topic, expected_manifest, True),
        (target, "after-reload", True),
    ]
    assert mqtt.published_qos == [1, 0]


async def test_failed_manifest_ack_is_retried_before_target_publish(
    tmp_path: Path,
) -> None:
    path = tmp_path / "owned-topics.json"
    ledger = await _loaded_ledger(path)
    target = "brilliant/kitchen/light-1/state"
    expected_manifest = _manifest([target])
    mqtt = RecordingOneShotFailureMqtt(ledger.ownership_topic)

    with pytest.raises(RuntimeError, match="publish failed once"):
        await ledger.async_publish(mqtt, target, "one")
    await ledger.async_publish(mqtt, target, "two")
    await ledger.async_publish(mqtt, target, "three")

    assert mqtt.published == [
        (ledger.ownership_topic, expected_manifest, True),
        (ledger.ownership_topic, expected_manifest, True),
        (target, "two", True),
        (target, "three", True),
    ]
    assert mqtt.published_qos == [1, 1, 0, 0]
    assert ledger.topics == frozenset({target})
    assert path.read_text(encoding="utf-8") == expected_manifest


async def test_manifest_serializes_topics_in_sorted_order(tmp_path: Path) -> None:
    path = tmp_path / "owned-topics.json"
    ledger = await _loaded_ledger(path)
    mqtt = FakeMqtt()
    later = "brilliant/kitchen/z-light/state"
    earlier = "brilliant/kitchen/a-light/state"

    await ledger.async_publish(mqtt, later, "z")
    await ledger.async_publish(mqtt, earlier, "a")

    assert path.read_text(encoding="utf-8") == _manifest([earlier, later])


async def test_cancellation_keeps_write_serialized_until_thread_finishes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "owned-topics.json"
    ledger = await _loaded_ledger(path)
    mqtt = FakeMqtt()
    first = "brilliant/kitchen/light-1/state"
    second = "brilliant/kitchen/light-2/state"
    expected_manifest = _manifest([first, second])
    real_to_thread = asyncio.to_thread
    real_write = retained_topics_module._write_payload
    loop = asyncio.get_running_loop()
    release_first_write = threading.Event()
    first_thread_started = asyncio.Event()
    first_thread_finished = asyncio.Event()
    second_write_submitted = asyncio.Event()
    write_submissions = 0

    def blocking_write(write_path: Path, payload: str) -> None:
        if payload == _manifest([first]):
            loop.call_soon_threadsafe(first_thread_started.set)
            release_first_write.wait()
            try:
                real_write(write_path, payload)
            finally:
                loop.call_soon_threadsafe(first_thread_finished.set)
            return
        real_write(write_path, payload)

    async def track_to_thread(
        function: Callable[..., object],
        /,
        *args: object,
        **kwargs: object,
    ) -> object:
        nonlocal write_submissions
        if function is blocking_write:
            write_submissions += 1
            if write_submissions == 2:
                second_write_submitted.set()
        return await real_to_thread(function, *args, **kwargs)

    monkeypatch.setattr(retained_topics_module, "_write_payload", blocking_write)
    monkeypatch.setattr(asyncio, "to_thread", track_to_thread)

    first_publish = asyncio.create_task(ledger.async_publish(mqtt, first, "one"))
    await first_thread_started.wait()
    first_publish.cancel()
    await asyncio.sleep(0)
    pending_after_first_cancel = not first_publish.done()
    first_publish.cancel()
    await asyncio.sleep(0)
    pending_after_repeat_cancel = not first_publish.done()

    second_publish = asyncio.create_task(ledger.async_publish(mqtt, second, "two"))
    await asyncio.sleep(0)
    second_submitted_before_release = second_write_submitted.is_set()
    release_first_write.set()
    await first_thread_finished.wait()
    first_result, second_result = await asyncio.gather(
        first_publish,
        second_publish,
        return_exceptions=True,
    )

    assert pending_after_first_cancel
    assert pending_after_repeat_cancel
    assert not second_submitted_before_release
    assert isinstance(first_result, asyncio.CancelledError)
    assert second_result is None
    assert ledger.topics == frozenset({first, second})
    assert path.read_text(encoding="utf-8") == expected_manifest
    assert mqtt.published == [
        (ledger.ownership_topic, expected_manifest, True),
        (second, "two", True),
    ]
    assert mqtt.published_qos == [1, 0]


async def test_loaded_instances_for_normalized_path_publish_union(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    alias_parent = tmp_path / "alias"
    alias_parent.mkdir()
    path = tmp_path / "owned-topics.json"
    alias_path = alias_parent / ".." / path.name
    first_ledger = await _loaded_ledger(path)
    second_ledger = await _loaded_ledger(alias_path)
    mqtt = FakeMqtt()
    first = "brilliant/kitchen/light-1/state"
    second = "brilliant/kitchen/light-2/state"
    expected_manifest = _manifest([first, second])
    real_to_thread = asyncio.to_thread
    first_write_started = asyncio.Event()
    release_first_write = asyncio.Event()
    second_write_started = asyncio.Event()
    write_count = 0

    async def controlled_to_thread(
        function: Callable[..., object],
        /,
        *args: object,
        **kwargs: object,
    ) -> object:
        nonlocal write_count
        if function is retained_topics_module._write_payload:
            write_count += 1
            if write_count == 1:
                first_write_started.set()
                await release_first_write.wait()
            else:
                second_write_started.set()
            assert len(args) == 2
            assert not kwargs
            write_path, payload = args
            assert isinstance(write_path, Path)
            assert isinstance(payload, str)
            retained_topics_module._write_payload(write_path, payload)
            return None
        return await real_to_thread(function, *args, **kwargs)

    monkeypatch.setattr(asyncio, "to_thread", controlled_to_thread)
    first_publish = asyncio.create_task(first_ledger.async_publish(mqtt, first, "one"))
    await first_write_started.wait()
    second_publish = asyncio.create_task(second_ledger.async_publish(mqtt, second, "two"))
    await asyncio.sleep(0)
    second_started_before_release = second_write_started.is_set()
    release_first_write.set()
    await asyncio.gather(first_publish, second_publish)

    assert not second_started_before_release
    assert first_ledger.topics == frozenset({first, second})
    assert second_ledger.topics == frozenset({first, second})
    assert path.read_text(encoding="utf-8") == expected_manifest
    ownership_messages = [
        (payload, mqtt.published_qos[index])
        for index, (topic, payload, _retain) in enumerate(mqtt.published)
        if topic == first_ledger.ownership_topic
    ]
    assert ownership_messages[-1] == (expected_manifest, 1)
    assert (first, "one", True) in mqtt.published
    assert (second, "two", True) in mqtt.published
    assert mqtt.published_qos == [1, 0, 1, 0]


async def test_disk_write_failure_prevents_manifest_and_target_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "owned-topics.json"
    ledger = await _loaded_ledger(path)
    mqtt = FakeMqtt()

    def fail_replace(source: object, destination: object) -> NoReturn:
        del source, destination
        raise OSError("disk full")

    monkeypatch.setattr(os, "replace", fail_replace)

    with pytest.raises(RetainedLedgerError, match="persist"):
        await ledger.async_publish(mqtt, "brilliant/kitchen/light-1/state", "value")

    assert mqtt.published == []
    assert ledger.topics == frozenset()
    assert not path.exists()
    assert list(path.parent.iterdir()) == []


async def test_manifest_publish_failure_prevents_target_and_keeps_disk_claim(
    tmp_path: Path,
) -> None:
    path = tmp_path / "owned-topics.json"
    ledger = await _loaded_ledger(path)
    mqtt = RecordingFailureMqtt(ledger.ownership_topic)
    target = "brilliant/kitchen/light-1/state"

    with pytest.raises(RuntimeError, match="publish failed"):
        await ledger.async_publish(mqtt, target, "value")

    assert mqtt.published == [(ledger.ownership_topic, _manifest([target]), True)]
    assert mqtt.published_qos == [1]
    assert ledger.topics == frozenset({target})
    assert path.read_text(encoding="utf-8") == _manifest([target])


async def test_target_publish_failure_leaves_manifest_claiming_topic(tmp_path: Path) -> None:
    path = tmp_path / "owned-topics.json"
    ledger = await _loaded_ledger(path)
    target = "brilliant/kitchen/light-1/state"
    mqtt = RecordingFailureMqtt(target)

    with pytest.raises(RuntimeError, match="publish failed"):
        await ledger.async_publish(mqtt, target, "value")

    reloaded = await _loaded_ledger(path)
    assert reloaded.topics == frozenset({target})
    assert mqtt.published[-1] == (target, "value", True)


async def test_clear_publishes_tombstone_before_persisting_smaller_manifest(
    tmp_path: Path,
) -> None:
    path = tmp_path / "owned-topics.json"
    ledger = await _loaded_ledger(path)
    mqtt = FakeMqtt()
    removed = "brilliant/kitchen/light-1/state"
    kept = "brilliant/kitchen/light-2/state"
    await ledger.async_publish(mqtt, removed, "one")
    await ledger.async_publish(mqtt, kept, "two")
    mqtt.published.clear()
    mqtt.published_qos.clear()

    await ledger.async_clear(mqtt, removed)

    assert mqtt.published == [
        (removed, "", True),
        (ledger.ownership_topic, _manifest([kept]), True),
    ]
    assert mqtt.published_qos == [1, 1]
    assert ledger.topics == frozenset({kept})
    assert path.read_text(encoding="utf-8") == _manifest([kept])


async def test_clear_all_clears_ownership_last(tmp_path: Path) -> None:
    path = tmp_path / "owned-topics.json"
    ledger = await _loaded_ledger(path)
    mqtt = FakeMqtt()
    topics = {
        "brilliant/kitchen/light-1/state",
        "homeassistant/light/brilliant_kitchen_light-1/config",
    }
    for topic in topics:
        await ledger.async_publish(mqtt, topic, "value")
    mqtt.published.clear()
    mqtt.published_qos.clear()

    await ledger.async_clear_all(mqtt)

    assert mqtt.published[-1] == (ledger.ownership_topic, "", True)
    assert mqtt.published_qos[-1] == 1
    for topic in topics:
        assert (topic, "", True) in mqtt.published[:-1]
        index = mqtt.published.index((topic, "", True))
        assert mqtt.published_qos[index] == 1
    assert ledger.topics == frozenset()
    assert path.read_text(encoding="utf-8") == _manifest([])


async def test_persistence_is_atomic_durable_and_private(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    real_fsync = os.fsync
    real_replace = os.replace

    def spy_fsync(fd: int) -> None:
        events.append("fsync")
        real_fsync(fd)

    def spy_replace(source: str | os.PathLike[str], destination: str | os.PathLike[str]) -> None:
        events.append("replace")
        real_replace(source, destination)

    monkeypatch.setattr(os, "fsync", spy_fsync)
    monkeypatch.setattr(os, "replace", spy_replace)
    path = tmp_path / "nested" / "owned-topics.json"
    ledger = await _loaded_ledger(path)

    await ledger.async_publish(FakeMqtt(), "brilliant/kitchen/availability", "online")

    replace_index = events.index("replace")
    assert "fsync" in events[:replace_index]
    assert "fsync" in events[replace_index + 1 :]
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


async def test_each_persistence_uses_a_private_sibling_temporary_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    replaced_sources: list[Path] = []
    real_replace = os.replace

    def spy_replace(
        source: str | os.PathLike[str],
        destination: str | os.PathLike[str],
    ) -> None:
        replaced_sources.append(Path(source))
        real_replace(source, destination)

    monkeypatch.setattr(os, "replace", spy_replace)
    path = tmp_path / "nested" / "owned-topics.json"
    ledger = await _loaded_ledger(path)

    await ledger.async_publish(FakeMqtt(), "brilliant/kitchen/availability", "online")
    await ledger.async_publish(FakeMqtt(), "brilliant/kitchen/bridge", "{}")

    assert len(replaced_sources) == 2
    assert replaced_sources[0] != replaced_sources[1]
    assert all(source.parent == path.parent for source in replaced_sources)
    assert all(not source.exists() for source in replaced_sources)
