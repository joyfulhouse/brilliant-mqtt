"""Poison-frame quarantine sidecar model and persistence (#113)."""

from __future__ import annotations

import hashlib
import json
import logging
import stat
from pathlib import Path

import pytest

from brilliant_mqtt import outbox_quarantine as oq
from tests.test_scene_outbox_quarantine import (
    _POISON_SCENE,
    _failures_reach,
    _Harness,
    _offered,
    _quarantine_poison,
)

_SHA = "a" * 64
_OTHER_SHA = "b" * 64
_EVENT = oq.event_record_key("scene:office:all_off:1700000000000")
_RESULT = oq.result_record_key("scene", "22222222-2222-4222-8222-222222222222")


def _fail(
    state: oq.QuarantineState, session: str, at_ms: int, sha: str = _SHA
) -> oq.QuarantineState:
    return oq.record_attributed_failure(state, _EVENT, sha, session, at_ms)


def test_record_keys_are_namespaced_hashes_and_round_trip() -> None:
    event_digest = hashlib.sha256(b"scene:office:all_off:1700000000000").hexdigest()
    result_digest = hashlib.sha256(b"22222222-2222-4222-8222-222222222222").hexdigest()
    assert _EVENT == f"event:{event_digest}"
    assert _RESULT == f"result:scene:{result_digest}"
    assert oq.parse_record_key(_EVENT) == ("event", event_digest)
    assert oq.parse_record_key(_RESULT) == (
        "result",
        ("scene", result_digest),
    )
    for bad in ("", "event:", "result:scene", "result:light:x", "other:x"):
        with pytest.raises(ValueError):
            oq.parse_record_key(bad)


def test_payload_hash_is_sha256_of_utf8_payload() -> None:
    assert oq.payload_sha256("{}") == (
        "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
    )


def test_long_record_identities_fit_the_bounded_sidecar(tmp_path: Path) -> None:
    key = oq.event_record_key("scene:office:" + "s" * 65_536 + ":1")
    result_key = oq.result_record_key("scene", "c" * 65_536)
    assert len(key) <= 80
    assert len(result_key) <= 80
    state = oq.QuarantineState()
    for index in range(3):
        state = oq.record_attributed_failure(state, key, _SHA, str(index), index * 30_000)
    path = tmp_path / "sidecar.json"
    oq.write_sidecar(path, state)
    assert oq.load_sidecar(path) == state
    assert path.stat().st_size < 1024
    suspect = state.get(key)
    assert suspect is not None and suspect.quarantined_at_ms is not None


def test_three_failures_in_distinct_sessions_spanning_a_minute_quarantine() -> None:
    state = oq.QuarantineState()
    state = _fail(state, "s1", 0)
    state = _fail(state, "s2", 30_000)
    suspect = state.get(_EVENT)
    assert suspect is not None and suspect.quarantined_at_ms is None
    state = _fail(state, "s3", 60_000)
    suspect = state.get(_EVENT)
    assert suspect is not None
    assert suspect.quarantined_at_ms == 60_000
    assert [failure.session for failure in suspect.failures] == ["s1", "s2", "s3"]


def test_three_failures_inside_a_minute_do_not_quarantine() -> None:
    state = oq.QuarantineState()
    for index, at_ms in enumerate((0, 20_000, 59_999)):
        state = _fail(state, f"s{index}", at_ms)
    suspect = state.get(_EVENT)
    assert suspect is not None and suspect.quarantined_at_ms is None
    # A fourth failure that stretches the span past a minute completes it.
    state = _fail(state, "s9", 60_000)
    suspect = state.get(_EVENT)
    assert suspect is not None and suspect.quarantined_at_ms == 60_000


def test_repeated_failures_in_one_session_count_once() -> None:
    state = oq.QuarantineState()
    for at_ms in (0, 30_000, 90_000):
        state = _fail(state, "same-session", at_ms)
    suspect = state.get(_EVENT)
    assert suspect is not None
    assert len(suspect.failures) == 1
    assert suspect.quarantined_at_ms is None


def test_changed_payload_restarts_the_count() -> None:
    state = _fail(_fail(oq.QuarantineState(), "s1", 0), "s2", 30_000)
    state = _fail(state, "s3", 90_000, sha=_OTHER_SHA)
    suspect = state.get(_EVENT)
    assert suspect is not None
    assert suspect.sha256 == _OTHER_SHA
    assert [failure.session for failure in suspect.failures] == ["s3"]


def test_failure_history_is_bounded() -> None:
    state = oq.QuarantineState()
    for index in range(oq.FAILURE_HISTORY_LIMIT + 5):
        # Inside one minute so nothing quarantines and the history keeps growing.
        state = _fail(state, f"s{index}", index)
    suspect = state.get(_EVENT)
    assert suspect is not None
    assert len(suspect.failures) == oq.FAILURE_HISTORY_LIMIT
    # The bound keeps the FIRST failure (the span anchor) plus the newest ones.
    assert suspect.failures[0].session == "s0"
    assert suspect.failures[-1].session == f"s{oq.FAILURE_HISTORY_LIMIT + 4}"


@pytest.mark.parametrize("period_ms", [5_000, 7_000, 8_000])
def test_session_rebuild_cadence_quarantines_once_span_reaches_a_minute(period_ms: int) -> None:
    # Real rebuild cadence: one attributable failure per session every few
    # seconds. The bounded history must not shrink the span below 60 s.
    state = oq.QuarantineState()
    index = 0
    while True:
        at_ms = index * period_ms
        state = _fail(state, f"s{index}", at_ms)
        suspect = state.get(_EVENT)
        assert suspect is not None
        if at_ms < oq.QUARANTINE_SPAN_MS:
            assert suspect.quarantined_at_ms is None
        else:
            assert index + 1 >= oq.QUARANTINE_FAILURES
            assert suspect.quarantined_at_ms == at_ms
            break
        index += 1
    assert len(suspect.failures) <= oq.FAILURE_HISTORY_LIMIT
    # The persisted bounded history round-trips and stays quarantined.
    assert oq.parse_sidecar(oq.sidecar_payload(state)) == state


def test_quarantined_record_is_not_modified_by_later_failures() -> None:
    state = oq.QuarantineState()
    for index, at_ms in enumerate((0, 30_000, 60_000)):
        state = _fail(state, f"s{index}", at_ms)
    assert _fail(state, "s9", 120_000) == state


def test_clear_record_resets_counter_and_blocked_marker() -> None:
    state = _fail(oq.QuarantineState(), "s1", 0)
    state = oq.mark_blocked(state, _EVENT, _SHA, 5)
    cleared = oq.clear_record(state, _EVENT)
    assert cleared.get(_EVENT) is None
    assert cleared.blocked is None
    assert oq.clear_record(cleared, _EVENT) == cleared


def test_attributable_failure_supersedes_blocked_marker_for_that_record() -> None:
    state = oq.mark_blocked(oq.QuarantineState(), _EVENT, _SHA, 5)
    assert state.blocked == oq.BlockedMarker(_EVENT, _SHA, 5)
    state = _fail(state, "s1", 10)
    assert state.blocked is None


def test_retain_live_drops_missing_delivered_or_changed_records() -> None:
    state = _fail(oq.QuarantineState(), "s1", 0)
    state = oq.record_attributed_failure(state, _RESULT, _OTHER_SHA, "s1", 0)
    state = oq.mark_blocked(state, _RESULT, _OTHER_SHA, 1)
    kept = oq.retain_live(state, {_EVENT: _SHA})
    assert kept.get(_EVENT) is not None
    assert kept.get(_RESULT) is None
    assert kept.blocked is None
    assert oq.retain_live(state, {_EVENT: _OTHER_SHA}).get(_EVENT) is None


def test_active_quarantine_requires_live_matching_record() -> None:
    state = oq.QuarantineState()
    for index, at_ms in enumerate((0, 30_000, 60_000)):
        state = _fail(state, f"s{index}", at_ms)
    assert [key for key, _ in oq.active_quarantine(state, {_EVENT: _SHA})] == [_EVENT]
    assert oq.active_quarantine(state, {_EVENT: _OTHER_SHA}) == []
    assert oq.active_quarantine(state, {}) == []


def test_sidecar_path_sits_next_to_the_state_file(tmp_path: Path) -> None:
    state_path = tmp_path / "scene-watermarks.json"
    assert oq.sidecar_path(state_path) == tmp_path / "scene-watermarks.json.quarantine.json"


def test_sidecar_round_trips_atomically_with_private_permissions(tmp_path: Path) -> None:
    state = oq.QuarantineState()
    for index, at_ms in enumerate((0, 30_000, 60_000)):
        state = _fail(state, f"s{index}", at_ms)
    state = oq.mark_blocked(state, _RESULT, _OTHER_SHA, 7)
    path = oq.sidecar_path(tmp_path / "private" / "state.json")

    oq.write_sidecar(path, state)

    assert oq.load_sidecar(path) == state
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    assert [entry.name for entry in path.parent.iterdir()] == [path.name]
    raw = json.loads(path.read_text())
    assert raw["version"] == oq.SIDECAR_VERSION
    assert raw["suspects"][_EVENT]["quarantined_at_ms"] == 60_000


def test_missing_sidecar_loads_empty_without_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.WARNING)
    assert oq.load_sidecar(tmp_path / "absent.json") == oq.QuarantineState()
    assert caplog.records == []


@pytest.mark.parametrize(
    "content",
    [
        b"{corrupt",
        b"\xff\xfe",
        b"[]",
        json.dumps({"version": 999, "suspects": {}, "blocked": None}).encode(),
        json.dumps({"version": 1, "suspects": {}, "blocked": None}).encode(),
        json.dumps({"version": oq.SIDECAR_VERSION, "suspects": {}}).encode(),
        json.dumps(
            {"version": oq.SIDECAR_VERSION, "suspects": {"bogus": {}}, "blocked": None},
        ).encode(),
        json.dumps(
            {
                "version": oq.SIDECAR_VERSION,
                "suspects": {
                    _EVENT: {"sha256": "nothex", "failures": [], "quarantined_at_ms": None}
                },
                "blocked": None,
            }
        ).encode(),
        json.dumps(
            {
                "version": oq.SIDECAR_VERSION,
                "suspects": {
                    _EVENT: {
                        "sha256": _SHA,
                        "failures": [{"session": "s", "at_ms": -1}],
                        "quarantined_at_ms": None,
                    }
                },
                "blocked": None,
            }
        ).encode(),
        b" " * (oq.MAX_SIDECAR_BYTES + 1),
    ],
)
def test_corrupt_sidecar_fails_safe_to_not_quarantined_with_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, content: bytes
) -> None:
    path = tmp_path / "state.json.quarantine.json"
    path.write_bytes(content)
    caplog.set_level(logging.WARNING, logger=oq.__name__)

    assert oq.load_sidecar(path) == oq.QuarantineState()

    assert any("quarantine sidecar" in record.getMessage() for record in caplog.records)
    # Fail-safe never deletes or rewrites the operator-visible evidence.
    assert path.read_bytes() == content


@pytest.mark.parametrize(
    ("failures", "quarantined_at_ms"),
    [
        ((), 60_000),
        ((("s1", 0),), 60_000),
        ((("s1", 0), ("s1", 30_000), ("s2", 60_000)), 60_000),
        ((("s1", 0), ("s2", 30_000), ("s3", 59_999)), 59_999),
        ((("s1", 0), ("s2", 30_000), ("s3", 60_000)), 60_001),
        ((("s1", 0), ("s2", 30_000), ("s3", 60_000)), 0),
    ],
    ids=["empty", "one-session", "repeated-session", "short-span", "later-time", "earlier-time"],
)
def test_persisted_halt_requires_qualifying_evidence_and_its_last_failure_time(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    failures: tuple[tuple[str, int], ...],
    quarantined_at_ms: int,
) -> None:
    suspect = oq.SuspectRecord(
        _SHA,
        tuple(oq.AttributedFailure(session, at_ms) for session, at_ms in failures),
        quarantined_at_ms,
    )
    raw = oq.sidecar_payload(oq.QuarantineState(suspects=((_EVENT, suspect),)))
    path = tmp_path / "sidecar.json"
    content = json.dumps(raw).encode()
    path.write_bytes(content)

    with pytest.raises(ValueError, match="quarantine"):
        oq.parse_sidecar(raw)
    assert oq.load_sidecar(path) == oq.QuarantineState()
    assert "quarantine sidecar" in caplog.text
    assert path.read_bytes() == content


@pytest.mark.parametrize(
    "times",
    [
        (0, 30_000, 60_000),
        (90_000, 60_000, 30_000),
        (60_000, 90_000, 0),
        (100_000, 150_000, 110_000, 110_000, 110_000, 110_000, 110_000, 110_000, 90_000),
    ],
    ids=["forward", "backward", "nonmonotonic", "bounded-history-clock-step"],
)
def test_genuine_halt_round_trips_with_wall_clock_adjustments(
    tmp_path: Path, times: tuple[int, ...]
) -> None:
    state = oq.QuarantineState()
    for index, at_ms in enumerate(times):
        state = _fail(state, f"s{index}", at_ms)
    suspect = state.get(_EVENT)
    assert suspect is not None and suspect.quarantined_at_ms == times[-1]
    assert len(suspect.failures) <= oq.FAILURE_HISTORY_LIMIT
    retained_times = [failure.at_ms for failure in suspect.failures]
    assert max(retained_times) - min(retained_times) >= oq.QUARANTINE_SPAN_MS
    path = tmp_path / "private" / "sidecar.json"
    oq.write_sidecar(path, state)
    assert oq.load_sidecar(path) == state
    assert oq.active_quarantine(oq.load_sidecar(path), {_EVENT: _SHA}) == [(_EVENT, suspect)]


@pytest.mark.parametrize("genuine", [False, True], ids=["corrupt-single-failure", "genuine"])
async def test_restart_honors_only_a_halt_with_qualifying_evidence(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, genuine: bool
) -> None:
    harness = _Harness(tmp_path)
    if genuine:
        await _quarantine_poison(harness)
    else:
        bridge, _, mqtt = await harness.session(execution_scene=_POISON_SCENE)
        await _offered(mqtt)
        await _failures_reach(harness, 1)
        await bridge.async_shutdown()
        raw = json.loads(harness.sidecar.read_bytes())
        for suspect in raw["suspects"].values():
            suspect["quarantined_at_ms"] = harness.clock.now_ms
        harness.sidecar.write_text(json.dumps(raw))
    content = harness.sidecar.read_bytes()
    caplog.clear()

    bridge, _, mqtt = await harness.session("accept")
    try:
        if genuine:
            assert bridge._outbox_reason() == "outbox_quarantine"
            assert mqtt.poison_offers == 0
            assert "quarantine sidecar" not in caplog.text
        else:
            assert bridge._outbox_reason() != "outbox_quarantine"
            await _offered(mqtt)
            assert "quarantine sidecar" in caplog.text
        assert not mqtt.connection_lost
    finally:
        await bridge.async_shutdown()
    assert harness.sidecar.read_bytes() == content


def test_deep_json_under_byte_limit_is_ignored_with_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    content = b"[" * 10_000 + b"0" + b"]" * 10_000
    assert len(content) < oq.MAX_SIDECAR_BYTES
    # Exercise the real decoder and prove this fixture reaches its recursion limit.
    with pytest.raises(RecursionError):
        json.loads(content)
    path = tmp_path / "sidecar.json"
    path.write_bytes(content)

    assert oq.load_sidecar(path) == oq.QuarantineState()
    assert "RecursionError" in caplog.text
    assert path.read_bytes() == content


async def test_deep_sidecar_does_not_abort_startup_and_pending_record_is_retried(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    harness = _Harness(tmp_path)
    bridge, _, mqtt = await harness.session("hold", execution_scene=_POISON_SCENE)
    await _offered(mqtt)
    await bridge.async_shutdown()
    content = b"[" * 10_000 + b"0" + b"]" * 10_000
    harness.sidecar.write_bytes(content)
    caplog.clear()

    bridge, _, mqtt = await harness.session("accept")
    try:
        await _offered(mqtt)
        assert not mqtt.connection_lost
        assert bridge._outbox_reason() != "outbox_quarantine"
        assert "RecursionError" in caplog.text
    finally:
        await bridge.async_shutdown()
    assert harness.sidecar.read_bytes() == content
