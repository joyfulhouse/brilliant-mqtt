"""Bounded, process-lifetime response observations (issue #152)."""

from __future__ import annotations

import asyncio
import json

import pytest

from brilliant_mqtt import bus as bus_mod
from brilliant_mqtt.bus import RpcBusAdapter
from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.diagnostics import ResponseDiagnostics, WriteOutcome
from tests.fakes import FakeClock
from tests.test_bus_adapter import _GatedRpcObserver, _settle, _StartHarness


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


def _write_adapter(
    recorder: ResponseDiagnostics, clock: FakeClock, error: Exception | None = None
) -> tuple[_GatedRpcObserver, RpcBusAdapter]:
    observer = _GatedRpcObserver(fail_with=error)
    adapter = RpcBusAdapter(clock=clock, diagnostics=recorder)
    adapter._obs = observer
    adapter._own_device_id = "synthetic-device"
    return observer, adapter


def _start_write(adapter: RpcBusAdapter, peripheral: str = "synthetic-light") -> asyncio.Task[str]:
    return asyncio.create_task(
        adapter.set_variables("synthetic-device", peripheral, [VarSet("on", "1")])
    )


class _BuiltinTimeoutSubclass(TimeoutError):
    pass


class _AsyncTimeoutSubclass(asyncio.TimeoutError):
    pass


@pytest.mark.parametrize(
    ("error", "outcome"),
    [
        (None, "ok"),
        (RuntimeError("synthetic failure"), "error"),
        (TimeoutError("synthetic timeout"), "timeout_bus"),
        (asyncio.TimeoutError("synthetic timeout"), "timeout_async"),
        (_BuiltinTimeoutSubclass(), "error"),
        (_AsyncTimeoutSubclass(), "error"),
    ],
)
async def test_write_outcomes_include_timeouts_in_rpc_population(
    error: Exception | None, outcome: str
) -> None:
    clock = FakeClock()
    recorder = ResponseDiagnostics(clock=clock)
    observer, adapter = _write_adapter(recorder, clock, error)
    caller = _start_write(adapter)
    await _settle()
    clock.advance(2.5)
    observer.release.set()
    if error is None:
        assert await caller == "'ok'"
    else:
        with pytest.raises(type(error)):
            await caller
    snapshot = recorder.snapshot()
    assert snapshot["write_total"] == snapshot[f"write_{outcome}"] == 1
    assert snapshot["rpc_s_sum"] == snapshot["rpc_s_recent_max"] == 2.5
    assert snapshot["rpc_s_count"] == 1
    assert snapshot["queue_wait_s_count"] == 1
    assert snapshot["queue_wait_s_sum"] == 0.0
    assert "synthetic" not in json.dumps(snapshot)
    await adapter.shutdown()


async def test_queue_wait_ends_at_lock_acquisition_and_reads_bypass_writes() -> None:
    clock = FakeClock()
    recorder = ResponseDiagnostics(clock=clock)
    observer, adapter = _write_adapter(recorder, clock)
    first = _start_write(adapter, "first")
    await _settle()
    clock.advance(2.0)
    second = _start_write(adapter, "second")
    await _settle()
    assert observer.writes == [("synthetic-device", "first")]
    assert await adapter.get_all()
    clock.advance(3.0)
    observer.release.set()
    await asyncio.gather(first, second)
    snapshot = recorder.snapshot()
    assert observer.writes == [("synthetic-device", "first"), ("synthetic-device", "second")]
    assert observer.max_in_flight == 1
    assert snapshot["write_ok"] == snapshot["write_total"] == 2
    assert snapshot["queue_wait_s_sum"] == snapshot["queue_wait_s_recent_max"] == 3.0
    assert snapshot["rpc_s_sum"] == 5.0
    assert snapshot["rpc_s_count"] == snapshot["queue_wait_s_count"] == 2
    await adapter.shutdown()


@pytest.mark.parametrize("error", [None, RuntimeError("synthetic late failure")])
async def test_caller_deadline_counts_only_the_later_detached_settlement(
    monkeypatch: pytest.MonkeyPatch, error: Exception | None
) -> None:
    monkeypatch.setattr(bus_mod, "_WRITE_DEADLINE_S", 0)
    clock = FakeClock()
    recorder = ResponseDiagnostics(clock=clock)
    observer, adapter = _write_adapter(recorder, clock, error)
    with pytest.raises(asyncio.TimeoutError):
        await adapter.set_variables("synthetic-device", "synthetic-light", [VarSet("on", "1")])
    assert recorder.snapshot()["write_total"] == 0
    (task,) = adapter._write_tasks
    assert not task.done()
    clock.advance(7.0)
    observer.release.set()
    if error is None:
        await task
    else:
        with pytest.raises(RuntimeError):
            await task
    await _settle()
    snapshot = recorder.snapshot()
    outcome = "write_detached_late_ok" if error is None else "write_detached_late_error"
    assert snapshot["write_total"] == snapshot[outcome] == 1
    assert snapshot["write_timeout_async"] == snapshot["write_hard_cap_total"] == 0
    assert snapshot["rpc_s_count"] == 1
    assert snapshot["rpc_s_sum"] == 7.0
    await adapter.shutdown()
    assert recorder.snapshot() == snapshot


@pytest.mark.parametrize("phase", ["before_step", "queued", "rpc"])
async def test_write_cancellation_counts_once_and_omits_unmeasured_timings(phase: str) -> None:
    clock = FakeClock()
    recorder = ResponseDiagnostics(clock=clock)
    observer, adapter = _write_adapter(recorder, clock)
    lock = asyncio.Lock()
    if phase == "queued":
        await lock.acquire()
        adapter._write_locks["synthetic-device"] = lock
    caller = _start_write(adapter)
    await asyncio.sleep(0)
    (task,) = adapter._write_tasks
    if phase != "before_step":
        await _settle()
    clock.advance(4.0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller
    if lock.locked():
        lock.release()
    await _settle()
    snapshot = recorder.snapshot()
    assert snapshot["write_cancelled"] == snapshot["write_total"] == 1
    assert snapshot["queue_wait_s_count"] == (1 if phase == "rpc" else 0)
    assert snapshot["queue_wait_s_recent_max"] == (0.0 if phase == "rpc" else None)
    assert snapshot["rpc_s_count"] == (1 if phase == "rpc" else 0)
    assert snapshot["rpc_s_sum"] == (4.0 if phase == "rpc" else 0.0)
    assert snapshot["rpc_s_recent_max"] == (4.0 if phase == "rpc" else None)
    assert bool(observer.writes) == (phase == "rpc")
    await adapter.shutdown()
    assert recorder.snapshot() == snapshot


async def test_caller_cancellation_leaves_write_attached_until_shutdown() -> None:
    clock = FakeClock()
    recorder = ResponseDiagnostics(clock=clock)
    observer, adapter = _write_adapter(recorder, clock)
    caller = _start_write(adapter)
    await _settle(6)
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller
    assert recorder.snapshot()["write_total"] == 0
    (task,) = adapter._write_tasks
    clock.advance(3.0)
    observer.release.set()
    await task
    assert recorder.snapshot()["write_ok"] == 1
    assert recorder.snapshot()["write_detached_late_ok"] == 0
    await adapter.shutdown()


async def test_hard_cap_is_separate_from_the_settlement_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(bus_mod, "_WRITE_HARD_CAP_S", 0)
    clock = FakeClock()
    recorder = ResponseDiagnostics(clock=clock)
    observer, adapter = _write_adapter(recorder, clock)
    caller = _start_write(adapter)
    await _settle(6)
    assert adapter.consume_write_timeout() is True
    assert recorder.snapshot()["write_hard_cap_total"] == 1
    assert recorder.snapshot()["write_total"] == 0
    clock.advance(15.0)
    observer.release.set()
    await caller
    assert recorder.snapshot()["write_total"] == recorder.snapshot()["write_ok"] == 1
    assert recorder.snapshot()["write_hard_cap_total"] == 1
    await adapter.shutdown()


async def test_bus_reconnect_counts_only_admitted_callbacks_after_initial_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _StartHarness(monkeypatch)
    recorder = ResponseDiagnostics(clock=FakeClock())
    adapter = RpcBusAdapter(diagnostics=recorder)
    await adapter.start()
    assert recorder.snapshot()["bus_reconnect_total"] == 0
    callback = harness.procs[0].reconnect_cbs[0]
    callback()
    await _settle()
    assert recorder.snapshot()["bus_reconnect_total"] == 1
    await adapter.shutdown()
    callback()
    await _settle()
    assert recorder.snapshot()["bus_reconnect_total"] == 1
    await adapter.start()
    callback()
    await _settle()
    assert recorder.snapshot()["bus_reconnect_total"] == 1
    harness.procs[1].reconnect_cbs[0]()
    await _settle()
    assert recorder.snapshot()["bus_reconnect_total"] == 2
    await adapter.shutdown()
