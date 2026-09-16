"""Bounded, process-lifetime response observations (issue #152)."""

from __future__ import annotations

import json

from brilliant_mqtt.diagnostics import ResponseDiagnostics, WriteOutcome
from tests.fakes import FakeClock


def test_snapshot_has_fixed_numeric_schema_and_is_an_independent_copy() -> None:
    clock = FakeClock()
    recorder = ResponseDiagnostics(clock=clock)
    clock.advance(12.5)
    expected = {
        "v": 1,
        "uptime_s": 12.5,
        "write_total": 0,
        "write_ok": 0,
        "write_error": 0,
        "write_timeout_bus": 0,
        "write_timeout_async": 0,
        "write_cancelled": 0,
        "write_detached_late_ok": 0,
        "write_detached_late_error": 0,
        "write_hard_cap_total": 0,
        "superseded_before_dispatch": 0,
        "bus_reconnect_total": 0,
        "session_rebuild": {
            "mqtt_reader_dead": 0,
            "bus_stale": 0,
            "bus_write_stuck": 0,
            "bus_reconnect_storm": 0,
            "mqtt_transport_overload": 0,
            "other": 0,
        },
        "queue_wait_s_sum": 0.0,
        "queue_wait_s_count": 0,
        "rpc_s_sum": 0.0,
        "rpc_s_count": 0,
        "queue_wait_s_recent_max": None,
        "rpc_s_recent_max": None,
    }
    snapshot = recorder.snapshot()
    assert snapshot == expected
    for value in snapshot.values():
        if isinstance(value, dict):
            assert all(type(count) is int for count in value.values())
        else:
            assert value is None or type(value) in (int, float)
    assert len(json.dumps(snapshot).encode()) < 4096
    snapshot["write_total"] = 999
    reasons = snapshot["session_rebuild"]
    assert isinstance(reasons, dict)
    reasons["other"] = 999
    assert recorder.snapshot() == expected


def test_recorder_totals_are_cumulative_and_outcomes_are_exhaustive() -> None:
    recorder = ResponseDiagnostics(clock=FakeClock())
    outcomes: tuple[WriteOutcome, ...] = (
        "ok",
        "error",
        "timeout_bus",
        "timeout_async",
        "cancelled",
        "detached_late_ok",
        "detached_late_error",
    )
    for outcome in outcomes:
        recorder.note_write_settled(outcome, 2.0, 3.0)
    recorder.note_superseded()
    recorder.note_bus_reconnect()
    recorder.note_hard_cap()
    recorder.note_session_rebuild("bus_stale")
    snapshot = recorder.snapshot()
    assert snapshot["write_total"] == len(outcomes)
    assert all(snapshot[f"write_{outcome}"] == 1 for outcome in outcomes)
    assert snapshot["queue_wait_s_sum"] == 14.0
    assert snapshot["queue_wait_s_count"] == 7
    assert snapshot["rpc_s_sum"] == 21.0
    assert snapshot["rpc_s_count"] == 7
    assert snapshot["superseded_before_dispatch"] == 1
    assert snapshot["bus_reconnect_total"] == 1
    assert snapshot["write_hard_cap_total"] == 1
    reasons = snapshot["session_rebuild"]
    assert isinstance(reasons, dict)
    assert reasons["bus_stale"] == 1
    assert recorder.snapshot() == snapshot


def test_recent_max_keeps_only_64_measured_samples_without_fabricated_zeroes() -> None:
    recorder = ResponseDiagnostics(clock=FakeClock())
    recorder.note_write_settled("ok", 100.0, 200.0)
    for _ in range(63):
        recorder.note_write_settled("ok", 2.0, 3.0)
    recorder.note_write_settled("cancelled", None, None)
    snapshot = recorder.snapshot()
    assert snapshot["write_total"] == 65
    assert snapshot["queue_wait_s_count"] == snapshot["rpc_s_count"] == 64
    assert snapshot["queue_wait_s_recent_max"] == 100.0
    assert snapshot["rpc_s_recent_max"] == 200.0
    recorder.note_write_settled("ok", 2.0, 3.0)
    snapshot = recorder.snapshot()
    assert snapshot["queue_wait_s_recent_max"] == 2.0
    assert snapshot["rpc_s_recent_max"] == 3.0
    assert snapshot["queue_wait_s_sum"] == 228.0
    assert snapshot["rpc_s_sum"] == 392.0
