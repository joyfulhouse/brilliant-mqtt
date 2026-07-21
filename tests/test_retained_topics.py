from __future__ import annotations

import json
import os
import stat
from pathlib import Path
from typing import NoReturn

import pytest

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


async def test_manifest_serializes_topics_in_sorted_order(tmp_path: Path) -> None:
    path = tmp_path / "owned-topics.json"
    ledger = await _loaded_ledger(path)
    mqtt = FakeMqtt()
    later = "brilliant/kitchen/z-light/state"
    earlier = "brilliant/kitchen/a-light/state"

    await ledger.async_publish(mqtt, later, "z")
    await ledger.async_publish(mqtt, earlier, "a")

    assert path.read_text(encoding="utf-8") == _manifest([earlier, later])


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
    assert mqtt.published_qos == [0, 1]
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
