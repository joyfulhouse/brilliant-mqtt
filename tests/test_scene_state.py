from __future__ import annotations

import json
import os
import stat
from pathlib import Path

import pytest

from brilliant_mqtt.ha_control_protocol import (
    MAPPING_VERSION,
    SCHEMA_VERSION,
    encode_json,
    scene_event_topic,
    scene_result_topic,
)
from brilliant_mqtt.scene_state import (
    MAX_STATE_BYTES,
    MODE_WATERMARK_LIMIT,
    SCENE_WATERMARK_LIMIT,
    SceneState,
    StateEvent,
    StateKind,
    StatePending,
    StateResult,
    StateWatermark,
    atomic_write_state,
    command_fingerprint_fields,
    load_state,
    state_payload,
)

_PANEL = "panel-1"
_COMMAND_ID = "22222222-2222-4222-8222-222222222222"
_SCENE_ID = "all_off"
_ISSUED_AT_MS = 1_000
_EXECUTED_AT_MS = 1_100


def _event(
    *,
    kind: StateKind = "scene",
    panel: str = _PANEL,
    value: str = _SCENE_ID,
    delivered: bool = True,
) -> StateEvent:
    payload = encode_json(
        {
            "schema_version": SCHEMA_VERSION,
            "mapping_version": MAPPING_VERSION,
            "panel": panel,
            f"{kind}_id": value,
            "executed_at_ms": _EXECUTED_AT_MS,
            "deduplication_key": f"{panel}:{value}:{_EXECUTED_AT_MS}",
        }
    )
    topic = scene_event_topic(panel) if kind == "scene" else f"brilliant/v1/mode/event/{panel}"
    return StateEvent(topic, payload, delivered, _EXECUTED_AT_MS)


def _result(
    *,
    kind: StateKind = "scene",
    panel: str = _PANEL,
    value: str = _SCENE_ID,
    event_key: str,
    delivered: bool = True,
) -> StateResult:
    fingerprint = command_fingerprint_fields(kind, _COMMAND_ID, panel, value, _ISSUED_AT_MS)
    payload = encode_json(
        {
            "schema_version": SCHEMA_VERSION,
            "mapping_version": MAPPING_VERSION,
            "command_id": _COMMAND_ID,
            "panel": panel,
            f"{kind}_id": value,
            "accepted": True,
            "timestamp_ms": _EXECUTED_AT_MS,
        }
    )
    topic = (
        scene_result_topic(_COMMAND_ID)
        if kind == "scene"
        else f"brilliant/v1/mode/result/{_COMMAND_ID}"
    )
    return StateResult(
        kind,
        _COMMAND_ID,
        fingerprint,
        panel,
        value,
        _ISSUED_AT_MS,
        topic,
        payload,
        delivered,
        20_000,
        event_key,
    )


def _state() -> SceneState:
    event_key = f"scene:{_PANEL}:{_SCENE_ID}:{_EXECUTED_AT_MS}"
    return SceneState(
        watermarks=(((_PANEL, _SCENE_ID), StateWatermark(_EXECUTED_AT_MS, "a" * 64)),),
        mode_watermarks=((_PANEL, (_EXECUTED_AT_MS, "away")),),
        events=((event_key, _event()),),
        results=((("scene", _COMMAND_ID), _result(event_key=event_key)),),
        pending=(),
    )


def test_atomic_state_round_trip_normalizes_and_syncs_private_permissions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "private" / "state.json"
    fsynced: list[int] = []
    real_fsync = os.fsync

    def recording_fsync(descriptor: int) -> None:
        fsynced.append(descriptor)
        real_fsync(descriptor)

    monkeypatch.setattr(os, "fsync", recording_fsync)

    atomic_write_state(path, _state())
    loaded = load_state(path)

    assert loaded.trusted is True
    assert loaded.state == _state()
    assert state_payload(loaded.state) == json.loads(path.read_text())
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    assert len(fsynced) >= 4  # data, file chmod metadata, directory chmod/rename metadata


def test_load_rejects_oversized_file_without_reading_it_all(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    with path.open("wb") as handle:
        handle.truncate(MAX_STATE_BYTES + 1)

    loaded = load_state(path)

    assert loaded.trusted is False
    assert loaded.reason == "state_untrusted"


@pytest.mark.parametrize(
    ("field", "limit", "entry"),
    [
        (
            "watermarks",
            SCENE_WATERMARK_LIMIT,
            lambda index: {
                f"scene-{index}": {
                    "executed_at_ms": index,
                    "payload_sha256": "a" * 64,
                }
            },
        ),
        (
            "mode_watermarks",
            MODE_WATERMARK_LIMIT,
            lambda index: {f"panel-{index}": {"executed_at_ms": index, "mode_id": "away"}},
        ),
    ],
)
def test_load_rejects_watermark_collection_over_its_limit(
    tmp_path: Path, field: str, limit: int, entry: object
) -> None:
    raw = state_payload(SceneState())
    if field == "watermarks":
        records: dict[str, object] = {}
        for index in range(limit + 1):
            records.update(entry(index))  # type: ignore[operator]
        raw[field] = {_PANEL: records}
    else:
        records = {}
        for index in range(limit + 1):
            records.update(entry(index))  # type: ignore[operator]
        raw[field] = records
    path = tmp_path / "state.json"
    path.write_text(json.dumps(raw))

    loaded = load_state(path)

    assert loaded.trusted is False


@pytest.mark.parametrize(
    ("result_kind", "result_panel", "result_value"),
    [
        ("mode", _PANEL, _SCENE_ID),
        ("scene", "panel-2", _SCENE_ID),
        ("scene", _PANEL, "all_on"),
    ],
)
def test_load_rejects_cross_event_result_dependency(
    tmp_path: Path, result_kind: StateKind, result_panel: str, result_value: str
) -> None:
    event_key = f"scene:{_PANEL}:{_SCENE_ID}:{_EXECUTED_AT_MS}"
    state = SceneState(
        events=((event_key, _event()),),
        results=(
            (
                (result_kind, _COMMAND_ID),
                _result(
                    kind=result_kind,
                    panel=result_panel,
                    value=result_value,
                    event_key=event_key,
                ),
            ),
        ),
    )
    path = tmp_path / "state.json"
    path.write_text(json.dumps(state_payload(state)))

    loaded = load_state(path)

    assert loaded.trusted is False


def test_load_rejects_delivered_result_with_undelivered_event_dependency(
    tmp_path: Path,
) -> None:
    event_key = f"scene:{_PANEL}:{_SCENE_ID}:{_EXECUTED_AT_MS}"
    state = SceneState(
        events=((event_key, _event(delivered=False)),),
        results=((("scene", _COMMAND_ID), _result(event_key=event_key, delivered=True)),),
    )
    path = tmp_path / "state.json"
    path.write_text(json.dumps(state_payload(state)))

    loaded = load_state(path)

    assert loaded.trusted is False


def test_scene_state_is_immutable() -> None:
    state = _state()

    with pytest.raises(AttributeError):
        state.pending = (  # type: ignore[misc]
            (
                ("scene", _COMMAND_ID),
                StatePending("scene", _COMMAND_ID, _SCENE_ID, "a" * 64, _PANEL, 1, 2),
            ),
        )
