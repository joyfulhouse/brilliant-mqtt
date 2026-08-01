"""Unit tests for the durable retained-topic ownership ledger (retained_topics.py).

Covers: async_load's strict validation matrix, async_publish's
persist-before-publish ordering, the topic ownership whitelist, async_clear(_all),
_async_persist's cancel-mid-write semantics, and _PathState sharing across
ledger instances that point at the same file.
"""

from __future__ import annotations

import asyncio
import json
import threading
from pathlib import Path

import pytest

from brilliant_mqtt import retained_topics
from brilliant_mqtt.retained_topics import (
    MAX_MANIFEST_BYTES,
    MAX_TOPICS,
    SCHEMA_VERSION,
    RetainedLedgerError,
    RetainedTopicLedger,
    _validate_topic,
)
from tests.fakes import FakeMqtt

PANEL = "office"
AVAILABILITY = f"brilliant/{PANEL}/availability"
BRIDGE_META = f"brilliant/{PANEL}/bridge"
STATE_A = f"brilliant/{PANEL}/peripheral-a/state"
STATE_B = f"brilliant/{PANEL}/peripheral-b/state"
CONFIG_A = f"homeassistant/light/brilliant_{PANEL}_peripheral-a/config"


class _SnapshotMqtt(FakeMqtt):
    """Records the ledger file's on-disk bytes at the moment of each publish call.

    Lets a test assert that a disk write genuinely happened BEFORE a given
    publish, not merely that it happened at some point during the whole call.
    """

    def __init__(self, ledger_path: Path) -> None:
        super().__init__()
        self._ledger_path = ledger_path
        self.disk_snapshots_at_publish: list[bytes | None] = []

    async def publish(self, topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        self.disk_snapshots_at_publish.append(
            self._ledger_path.read_bytes() if self._ledger_path.exists() else None
        )
        await super().publish(topic, payload, retain, qos)


def _write_raw_manifest(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


async def _wait_until(predicate: object, timeout: float = 2.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():  # type: ignore[operator]
        if loop.time() > deadline:
            raise AssertionError("condition was not met in time")
        await asyncio.sleep(0.005)


# -- async_load: strict validation matrix -------------------------------------


async def test_async_load_missing_file_is_empty(tmp_path: Path) -> None:
    ledger = RetainedTopicLedger(PANEL, tmp_path / "owned-topics.json")

    await ledger.async_load()

    assert ledger.topics == frozenset()


async def test_async_load_malformed_json_raises(tmp_path: Path) -> None:
    path = tmp_path / "owned-topics.json"
    path.write_text("{not json")
    ledger = RetainedTopicLedger(PANEL, path)

    with pytest.raises(RetainedLedgerError, match="invalid retained ledger JSON"):
        await ledger.async_load()


async def test_async_load_non_dict_json_raises(tmp_path: Path) -> None:
    path = tmp_path / "owned-topics.json"
    path.write_text("[1, 2, 3]")
    ledger = RetainedTopicLedger(PANEL, path)

    with pytest.raises(RetainedLedgerError, match="expected a JSON object"):
        await ledger.async_load()


async def test_async_load_wrong_keys_raises(tmp_path: Path) -> None:
    path = tmp_path / "owned-topics.json"
    _write_raw_manifest(
        path, {"schema_version": SCHEMA_VERSION, "panel_slug": PANEL, "topics": [], "extra": 1}
    )
    ledger = RetainedTopicLedger(PANEL, path)

    with pytest.raises(RetainedLedgerError, match="invalid retained ledger keys"):
        await ledger.async_load()


async def test_async_load_missing_key_raises(tmp_path: Path) -> None:
    path = tmp_path / "owned-topics.json"
    _write_raw_manifest(path, {"schema_version": SCHEMA_VERSION, "panel_slug": PANEL})
    ledger = RetainedTopicLedger(PANEL, path)

    with pytest.raises(RetainedLedgerError, match="invalid retained ledger keys"):
        await ledger.async_load()


async def test_async_load_wrong_schema_version_raises(tmp_path: Path) -> None:
    path = tmp_path / "owned-topics.json"
    _write_raw_manifest(path, {"schema_version": 2, "panel_slug": PANEL, "topics": []})
    ledger = RetainedTopicLedger(PANEL, path)

    with pytest.raises(RetainedLedgerError, match="unsupported retained ledger schema"):
        await ledger.async_load()


async def test_async_load_schema_version_as_bool_raises(tmp_path: Path) -> None:
    # bool is an int subclass in Python; `type(x) is not int` must still reject it.
    path = tmp_path / "owned-topics.json"
    _write_raw_manifest(path, {"schema_version": True, "panel_slug": PANEL, "topics": []})
    ledger = RetainedTopicLedger(PANEL, path)

    with pytest.raises(RetainedLedgerError, match="unsupported retained ledger schema"):
        await ledger.async_load()


async def test_async_load_wrong_panel_raises(tmp_path: Path) -> None:
    path = tmp_path / "owned-topics.json"
    _write_raw_manifest(
        path, {"schema_version": SCHEMA_VERSION, "panel_slug": "kitchen", "topics": []}
    )
    ledger = RetainedTopicLedger(PANEL, path)

    with pytest.raises(RetainedLedgerError, match="panel slug does not match"):
        await ledger.async_load()


async def test_async_load_non_list_topics_raises(tmp_path: Path) -> None:
    path = tmp_path / "owned-topics.json"
    _write_raw_manifest(
        path, {"schema_version": SCHEMA_VERSION, "panel_slug": PANEL, "topics": {"a": 1}}
    )
    ledger = RetainedTopicLedger(PANEL, path)

    with pytest.raises(RetainedLedgerError, match="expected a list"):
        await ledger.async_load()


async def test_async_load_non_string_topic_entries_raises(tmp_path: Path) -> None:
    path = tmp_path / "owned-topics.json"
    _write_raw_manifest(
        path, {"schema_version": SCHEMA_VERSION, "panel_slug": PANEL, "topics": [1, 2]}
    )
    ledger = RetainedTopicLedger(PANEL, path)

    with pytest.raises(RetainedLedgerError, match="expected strings"):
        await ledger.async_load()


async def test_async_load_duplicate_topics_raises(tmp_path: Path) -> None:
    path = tmp_path / "owned-topics.json"
    _write_raw_manifest(
        path,
        {"schema_version": SCHEMA_VERSION, "panel_slug": PANEL, "topics": [STATE_A, STATE_A]},
    )
    ledger = RetainedTopicLedger(PANEL, path)

    with pytest.raises(RetainedLedgerError, match="duplicate topics"):
        await ledger.async_load()


async def test_async_load_too_many_topics_raises(tmp_path: Path) -> None:
    path = tmp_path / "owned-topics.json"
    # The count guard fires before per-topic shape validation, so placeholder
    # strings (not real ownable topics) are enough to exercise it.
    topics = [f"t{i}" for i in range(MAX_TOPICS + 1)]
    _write_raw_manifest(
        path, {"schema_version": SCHEMA_VERSION, "panel_slug": PANEL, "topics": topics}
    )
    ledger = RetainedTopicLedger(PANEL, path)

    with pytest.raises(RetainedLedgerError, match=f"exceeds {MAX_TOPICS:,} topics"):
        await ledger.async_load()


async def test_async_load_topic_failing_ownership_whitelist_raises(tmp_path: Path) -> None:
    path = tmp_path / "owned-topics.json"
    _write_raw_manifest(
        path,
        {
            "schema_version": SCHEMA_VERSION,
            "panel_slug": PANEL,
            "topics": ["brilliant/other-panel/x/state"],
        },
    )
    ledger = RetainedTopicLedger(PANEL, path)

    with pytest.raises(RetainedLedgerError, match="not owned by this panel"):
        await ledger.async_load()


async def test_async_load_oversize_manifest_raises(tmp_path: Path) -> None:
    path = tmp_path / "owned-topics.json"
    # A single topic whose JSON encoding alone exceeds the manifest byte cap,
    # while staying at 1 topic so the topic-count guard doesn't fire first.
    huge_topic = f"brilliant/{PANEL}/" + ("x" * (MAX_MANIFEST_BYTES + 1)) + "/state"
    _write_raw_manifest(
        path, {"schema_version": SCHEMA_VERSION, "panel_slug": PANEL, "topics": [huge_topic]}
    )
    ledger = RetainedTopicLedger(PANEL, path)

    with pytest.raises(RetainedLedgerError, match=f"exceeds {MAX_MANIFEST_BYTES} bytes"):
        await ledger.async_load()


async def test_async_load_valid_manifest_populates_topics(tmp_path: Path) -> None:
    path = tmp_path / "owned-topics.json"
    _write_raw_manifest(
        path, {"schema_version": SCHEMA_VERSION, "panel_slug": PANEL, "topics": [STATE_A, STATE_B]}
    )
    ledger = RetainedTopicLedger(PANEL, path)

    await ledger.async_load()

    assert ledger.topics == frozenset({STATE_A, STATE_B})


async def test_constructor_rejects_mesh_panel_slug(tmp_path: Path) -> None:
    with pytest.raises(RetainedLedgerError, match="invalid panel slug"):
        RetainedTopicLedger("mesh", tmp_path / "owned-topics.json")


async def test_constructor_rejects_non_canonical_panel_slug(tmp_path: Path) -> None:
    with pytest.raises(RetainedLedgerError, match="invalid panel slug"):
        RetainedTopicLedger("Office", tmp_path / "owned-topics.json")


# -- async_publish: claim-then-publish ordering --------------------------------


async def test_async_publish_persists_before_manifest_before_value(tmp_path: Path) -> None:
    path = tmp_path / "owned-topics.json"
    ledger = RetainedTopicLedger(PANEL, path)
    await ledger.async_load()
    mqtt = _SnapshotMqtt(path)

    await ledger.async_publish(mqtt, STATE_A, "v1")

    assert [call[0] for call in mqtt.published] == [ledger.ownership_topic, STATE_A]
    assert mqtt.published[0] == (
        ledger.ownership_topic,
        json.dumps(
            {"schema_version": SCHEMA_VERSION, "panel_slug": PANEL, "topics": [STATE_A]},
            sort_keys=True,
            separators=(",", ":"),
        ),
        True,
    )
    assert mqtt.published[1] == (STATE_A, "v1", True)
    # The ledger file already carried the claimed topic BEFORE the ownership
    # manifest was published — the durable record precedes the broker write.
    on_disk_at_first_publish = mqtt.disk_snapshots_at_publish[0]
    assert on_disk_at_first_publish is not None
    assert json.loads(on_disk_at_first_publish)["topics"] == [STATE_A]


async def test_async_publish_same_topic_again_skips_persist_and_manifest(tmp_path: Path) -> None:
    path = tmp_path / "owned-topics.json"
    ledger = RetainedTopicLedger(PANEL, path)
    await ledger.async_load()
    mqtt = _SnapshotMqtt(path)
    await ledger.async_publish(mqtt, STATE_A, "v1")
    mtime_after_first = path.stat().st_mtime_ns
    mqtt.published.clear()

    await ledger.async_publish(mqtt, STATE_A, "v2")

    # Ownership is unchanged and already acknowledged: only the value republishes.
    assert mqtt.published == [(STATE_A, "v2", True)]
    assert path.stat().st_mtime_ns == mtime_after_first


async def test_async_publish_new_topic_republishes_manifest_not_first_value(
    tmp_path: Path,
) -> None:
    path = tmp_path / "owned-topics.json"
    ledger = RetainedTopicLedger(PANEL, path)
    await ledger.async_load()
    mqtt = FakeMqtt()
    await ledger.async_publish(mqtt, STATE_A, "v1")
    mqtt.published.clear()

    await ledger.async_publish(mqtt, STATE_B, "v2")

    assert [call[0] for call in mqtt.published] == [ledger.ownership_topic, STATE_B]
    manifest_payload = mqtt.published[0][1]
    assert json.loads(manifest_payload)["topics"] == sorted([STATE_A, STATE_B])
    assert ledger.topics == frozenset({STATE_A, STATE_B})


async def test_async_publish_republishes_unacknowledged_manifest_without_reclaiming(
    tmp_path: Path,
) -> None:
    # Simulates a cold-start reload of an existing ledger: ownership is already
    # on disk (so no persist is needed), but this fresh instance has never
    # acknowledged the manifest to the broker this session.
    path = tmp_path / "owned-topics.json"
    _write_raw_manifest(
        path, {"schema_version": SCHEMA_VERSION, "panel_slug": PANEL, "topics": [STATE_A]}
    )
    ledger = RetainedTopicLedger(PANEL, path)
    await ledger.async_load()
    mtime_before = path.stat().st_mtime_ns
    mqtt = FakeMqtt()

    await ledger.async_publish(mqtt, STATE_A, "v1")

    assert [call[0] for call in mqtt.published] == [ledger.ownership_topic, STATE_A]
    assert path.stat().st_mtime_ns == mtime_before  # no re-persist; ownership unchanged


async def test_async_publish_rejects_topic_outside_ownership_whitelist(tmp_path: Path) -> None:
    ledger = RetainedTopicLedger(PANEL, tmp_path / "owned-topics.json")
    await ledger.async_load()
    mqtt = FakeMqtt()

    with pytest.raises(RetainedLedgerError, match="not owned by this panel"):
        await ledger.async_publish(mqtt, "brilliant/kitchen/x/state", "v1")

    assert mqtt.published == []


async def test_async_publish_requires_load_first(tmp_path: Path) -> None:
    ledger = RetainedTopicLedger(PANEL, tmp_path / "owned-topics.json")
    mqtt = FakeMqtt()

    with pytest.raises(RetainedLedgerError, match="must be loaded before use"):
        await ledger.async_publish(mqtt, STATE_A, "v1")


# -- Topic ownership whitelist (_validate_topic) --------------------------------


@pytest.mark.parametrize(
    "topic",
    [
        AVAILABILITY,
        BRIDGE_META,
        STATE_A,
        CONFIG_A,
        f"homeassistant/binary_sensor/brilliant_{PANEL}_peripheral-a_lux/config",
    ],
)
def test_validate_topic_accepts_real_bridge_topics(topic: str) -> None:
    _validate_topic(PANEL, topic)  # must not raise


@pytest.mark.parametrize(
    "topic",
    [
        "",
        f"brilliant/{PANEL}/+/state",
        f"brilliant/{PANEL}/#",
        f"brilliant/{PANEL}//state",
        "brilliant/kitchen/peripheral-a/state",
        f"brilliant/{PANEL}/ownership",
        f"frigate/{PANEL}/peripheral-a/state",
        "homeassistant/light/brilliant_kitchen_peripheral-a/config",
        f"homeassistant/light/other_{PANEL}_peripheral-a/config",
        f"brilliant/{PANEL}/peripheral-a/notstate",
        f"brilliant/{PANEL}/pe\x00ripheral/state",
    ],
)
def test_validate_topic_rejects_everything_else(topic: str) -> None:
    with pytest.raises(RetainedLedgerError):
        _validate_topic(PANEL, topic)


# -- async_clear / async_clear_all ----------------------------------------------


async def test_async_clear_refuses_unowned_topic(tmp_path: Path) -> None:
    ledger = RetainedTopicLedger(PANEL, tmp_path / "owned-topics.json")
    await ledger.async_load()
    mqtt = FakeMqtt()
    await ledger.async_publish(mqtt, STATE_A, "v1")
    mqtt.published.clear()

    with pytest.raises(RetainedLedgerError, match="refusing to clear a topic not owned"):
        await ledger.async_clear(mqtt, STATE_B)

    assert mqtt.published == []
    assert ledger.topics == frozenset({STATE_A})


async def test_async_clear_removes_topic_and_republishes_smaller_manifest(tmp_path: Path) -> None:
    path = tmp_path / "owned-topics.json"
    ledger = RetainedTopicLedger(PANEL, path)
    await ledger.async_load()
    mqtt = FakeMqtt()
    await ledger.async_publish(mqtt, STATE_A, "v1")
    await ledger.async_publish(mqtt, STATE_B, "v2")
    mqtt.published.clear()

    await ledger.async_clear(mqtt, STATE_A)

    assert mqtt.published == [
        (STATE_A, "", True),
        (
            ledger.ownership_topic,
            json.dumps(
                {"schema_version": SCHEMA_VERSION, "panel_slug": PANEL, "topics": [STATE_B]},
                sort_keys=True,
                separators=(",", ":"),
            ),
            True,
        ),
    ]
    assert ledger.topics == frozenset({STATE_B})
    assert json.loads(path.read_text())["topics"] == [STATE_B]


async def test_async_clear_all_clears_ownership_topic_last(tmp_path: Path) -> None:
    ledger = RetainedTopicLedger(PANEL, tmp_path / "owned-topics.json")
    await ledger.async_load()
    mqtt = FakeMqtt()
    await ledger.async_publish(mqtt, STATE_A, "v1")
    await ledger.async_publish(mqtt, STATE_B, "v2")
    mqtt.published.clear()

    await ledger.async_clear_all(mqtt)

    # Each per-topic clear also republishes a shrinking ownership manifest
    # (STATE_A < STATE_B, so it clears in that sorted order); only the FINAL
    # call clears the ownership topic itself with an empty payload.
    assert mqtt.published == [
        (STATE_A, "", True),
        (
            ledger.ownership_topic,
            json.dumps(
                {"schema_version": SCHEMA_VERSION, "panel_slug": PANEL, "topics": [STATE_B]},
                sort_keys=True,
                separators=(",", ":"),
            ),
            True,
        ),
        (STATE_B, "", True),
        (
            ledger.ownership_topic,
            json.dumps(
                {"schema_version": SCHEMA_VERSION, "panel_slug": PANEL, "topics": []},
                sort_keys=True,
                separators=(",", ":"),
            ),
            True,
        ),
        (ledger.ownership_topic, "", True),
    ]
    assert mqtt.published[-1] == (ledger.ownership_topic, "", True)
    assert ledger.topics == frozenset()


# -- _async_persist: cancel-mid-write semantics --------------------------------


async def test_publish_cancelled_mid_write_reraises_after_write_settles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "owned-topics.json"
    ledger = RetainedTopicLedger(PANEL, path)
    await ledger.async_load()
    mqtt = FakeMqtt()

    started = threading.Event()
    release = threading.Event()
    original_write = retained_topics._write_payload

    def blocking_write(write_path: Path, payload: str) -> None:
        started.set()
        assert release.wait(2.0), "test never released the blocked write"
        original_write(write_path, payload)

    monkeypatch.setattr(retained_topics, "_write_payload", blocking_write)

    task = asyncio.ensure_future(ledger.async_publish(mqtt, STATE_A, "v1"))
    await _wait_until(started.is_set)
    task.cancel()
    release.set()

    with pytest.raises(asyncio.CancelledError):
        await task

    # The shielded write ran to completion despite the cancellation: disk AND
    # in-memory state reflect the claim, but neither MQTT publish (which comes
    # strictly after the persist step) ever ran.
    assert ledger.topics == frozenset({STATE_A})
    assert json.loads(path.read_text())["topics"] == [STATE_A]
    assert mqtt.published == []

    # The lock was released correctly despite the exception unwinding through
    # `async with self._state.lock:` — a follow-up call must not deadlock.
    await asyncio.wait_for(ledger.async_publish(mqtt, STATE_B, "v2"), timeout=1.0)
    assert ledger.topics == frozenset({STATE_A, STATE_B})


async def test_publish_write_failure_raises_ledger_error_without_publishing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "owned-topics.json"
    ledger = RetainedTopicLedger(PANEL, path)
    await ledger.async_load()
    mqtt = FakeMqtt()

    def failing_write(write_path: Path, payload: str) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(retained_topics, "_write_payload", failing_write)

    with pytest.raises(RetainedLedgerError, match="could not persist retained ledger"):
        await ledger.async_publish(mqtt, STATE_A, "v1")

    assert ledger.topics == frozenset()
    assert mqtt.published == []
    assert not path.exists()


# -- _PathState: cross-instance sharing + panel mismatch -----------------------


async def test_two_ledgers_same_path_share_the_underlying_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "owned-topics.json"
    ledger1 = RetainedTopicLedger(PANEL, path)
    await ledger1.async_load()
    ledger2 = RetainedTopicLedger(PANEL, path)
    await ledger2.async_load()
    mqtt = FakeMqtt()

    started = threading.Event()
    release = threading.Event()
    original_write = retained_topics._write_payload

    def blocking_write(write_path: Path, payload: str) -> None:
        started.set()
        assert release.wait(2.0), "test never released the blocked write"
        original_write(write_path, payload)

    monkeypatch.setattr(retained_topics, "_write_payload", blocking_write)

    task1 = asyncio.ensure_future(ledger1.async_publish(mqtt, STATE_A, "v1"))
    await _wait_until(started.is_set)

    # ledger2 shares ledger1's asyncio.Lock (same normalized path) — its
    # publish must block behind ledger1's in-flight write, not race it.
    task2 = asyncio.ensure_future(ledger2.async_publish(mqtt, STATE_B, "v2"))
    await asyncio.sleep(0.05)
    assert not task2.done()

    release.set()
    await task1
    await task2

    assert ledger1.topics == ledger2.topics == frozenset({STATE_A, STATE_B})


async def test_second_ledger_different_panel_same_path_raises(tmp_path: Path) -> None:
    path = tmp_path / "owned-topics.json"
    ledger1 = RetainedTopicLedger(PANEL, path)
    await ledger1.async_load()

    with pytest.raises(RetainedLedgerError, match="already assigned to a different panel"):
        RetainedTopicLedger("kitchen", path)

    # ledger1 stays alive (referenced above) for the duration of the assertion
    # so the WeakValueDictionary entry for `path` cannot be collected early.
    assert ledger1.topics == frozenset()
