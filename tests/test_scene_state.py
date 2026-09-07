from __future__ import annotations

import json
import os
import stat
from collections.abc import Callable, Mapping
from pathlib import Path

import pytest

from brilliant_mqtt.ha_control_protocol import (
    COMMAND_TTL_MS,
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
    tmp_path: Path, field: str, limit: int, entry: Callable[[int], Mapping[str, object]]
) -> None:
    raw = state_payload(SceneState())
    if field == "watermarks":
        records: dict[str, object] = {}
        for index in range(limit + 1):
            records.update(entry(index))
        raw[field] = {_PANEL: records}
    else:
        records = {}
        for index in range(limit + 1):
            records.update(entry(index))
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

    # Assign through setattr with a runtime field name: a static assignment to
    # the frozen dataclass is (correctly) rejected by mypy; this test exercises
    # the runtime immutability guard.
    field_name = "pending"
    new_value = (
        (
            ("scene", _COMMAND_ID),
            StatePending("scene", _COMMAND_ID, _SCENE_ID, "a" * 64, _PANEL, 1, 2, 0),
        ),
    )
    with pytest.raises(AttributeError):
        setattr(state, field_name, new_value)


# --- #94: confirm_after_ms durable baseline + strict-loader migration (F5) ---


def _pending_entry(
    *,
    expires_at_ms: int,
    confirm_after_ms: int | None = None,
    equal_at_request: bool | None = None,
    kind: StateKind = "scene",
) -> dict[str, object]:
    entry: dict[str, object] = {
        "kind": kind,
        "command_id": _COMMAND_ID,
        "value": _SCENE_ID,
        "fingerprint": command_fingerprint_fields(
            kind, _COMMAND_ID, _PANEL, _SCENE_ID, _ISSUED_AT_MS
        ),
        "panel": _PANEL,
        "issued_at_ms": _ISSUED_AT_MS,
        "expires_at_ms": expires_at_ms,
    }
    if confirm_after_ms is not None:
        entry["confirm_after_ms"] = confirm_after_ms
    if equal_at_request is not None:
        entry["equal_at_request"] = equal_at_request
    return entry


def _pending_state_file(tmp_path: Path, entry: dict[str, object]) -> Path:
    raw = state_payload(SceneState())
    raw["pending"] = {f"{entry['kind']}:{_COMMAND_ID}": entry}
    path = tmp_path / "state.json"
    path.write_text(json.dumps(raw))
    return path


def test_pending_confirmation_fields_round_trip(tmp_path: Path) -> None:
    fingerprint = command_fingerprint_fields("mode", _COMMAND_ID, _PANEL, _SCENE_ID, _ISSUED_AT_MS)
    pending = StatePending(
        "mode",
        _COMMAND_ID,
        _SCENE_ID,
        fingerprint,
        _PANEL,
        _ISSUED_AT_MS,
        20_000,
        5_000,
        True,
    )
    state = SceneState(pending=((("mode", _COMMAND_ID), pending),))
    path = tmp_path / "private" / "state.json"

    atomic_write_state(path, state)
    loaded = load_state(path)

    assert loaded.trusted is True
    assert loaded.state == state
    persisted = json.loads(path.read_text())["pending"][f"mode:{_COMMAND_ID}"]
    assert persisted["confirm_after_ms"] == 5_000
    assert persisted["equal_at_request"] is True


def test_load_pending_with_confirm_after_ms_is_preserved(tmp_path: Path) -> None:
    path = _pending_state_file(
        tmp_path, _pending_entry(expires_at_ms=COMMAND_TTL_MS + 5_000, confirm_after_ms=5_000)
    )

    loaded = load_state(path)

    assert loaded.trusted is True
    assert loaded.reason is None
    ((_, pending),) = loaded.state.pending
    assert pending.confirm_after_ms == 5_000


def test_load_pending_with_equal_at_request_is_preserved(tmp_path: Path) -> None:
    path = _pending_state_file(
        tmp_path,
        _pending_entry(expires_at_ms=20_000, equal_at_request=True, kind="mode"),
    )

    loaded = load_state(path)

    assert loaded.trusted is True
    ((_, pending),) = loaded.state.pending
    assert pending.equal_at_request is True

    for bad in (0, 1, "true", None):
        entry = _pending_entry(expires_at_ms=20_000, kind="mode")
        entry["equal_at_request"] = bad
        invalid = load_state(_pending_state_file(tmp_path, entry))
        assert invalid.trusted is False
        assert invalid.reason == "state_untrusted"


def test_load_pending_without_confirm_after_ms_reconstructs_fallback_baseline(
    tmp_path: Path,
) -> None:
    # An old fleet file predates the field: STATE_VERSION stays 1, the file loads
    # trusted, and the baseline is reconstructed exactly as expires_at_ms - TTL
    # (creation used a single shared clock read).
    expires_at_ms = COMMAND_TTL_MS + 1_000
    path = _pending_state_file(tmp_path, _pending_entry(expires_at_ms=expires_at_ms))

    loaded = load_state(path)

    assert loaded.trusted is True
    assert loaded.reason is None
    ((_, pending),) = loaded.state.pending
    assert pending.confirm_after_ms == expires_at_ms - COMMAND_TTL_MS
    assert pending.equal_at_request is False


@pytest.mark.parametrize("bad", [-1, "5000", 20_001, 1.5])
def test_load_rejects_invalid_confirm_after_ms(tmp_path: Path, bad: object) -> None:
    entry = _pending_entry(expires_at_ms=20_000)
    entry["confirm_after_ms"] = bad
    path = _pending_state_file(tmp_path, entry)

    loaded = load_state(path)

    assert loaded.trusted is False
    assert loaded.reason == "state_untrusted"


def test_load_rejects_unknown_pending_key_even_with_confirm_after_ms(tmp_path: Path) -> None:
    entry = _pending_entry(expires_at_ms=20_000, confirm_after_ms=5_000)
    entry["bogus"] = 1
    path = _pending_state_file(tmp_path, entry)

    loaded = load_state(path)

    assert loaded.trusted is False


def test_load_rejects_absent_confirm_after_ms_when_expires_below_ttl(tmp_path: Path) -> None:
    # An old-format entry whose reconstructed baseline (expires_at_ms - TTL) would
    # be negative must be rejected AT LOAD, not deferred to the writer.
    path = _pending_state_file(tmp_path, _pending_entry(expires_at_ms=COMMAND_TTL_MS - 1))

    loaded = load_state(path)

    assert loaded.trusted is False
    assert loaded.reason == "state_untrusted"
