"""Bounded, process-lifetime response observations (issue #152)."""

from __future__ import annotations

import asyncio
import json
import re
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from brilliant_mqtt import __main__ as main_mod
from brilliant_mqtt import __version__, mqttio
from brilliant_mqtt import bus as bus_mod
from brilliant_mqtt.bridge import Bridge
from brilliant_mqtt.bus import RpcBusAdapter
from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.config import Settings
from brilliant_mqtt.desired_state import DesiredState
from brilliant_mqtt.diagnostics import ResponseDiagnostics, SessionRebuildReason, WriteOutcome
from brilliant_mqtt.retained_topics import RetainedLedgerError
from tests.fakes import FakeBus, FakeClock, FakeMqtt, FakeSleeper
from tests.test_bus_adapter import _GatedRpcObserver, _settle, _StartHarness
from tests.test_main import _panel_dimmer
from tests.test_mqttio_transport_backlog import _AiomqttClientInternals, _msg


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
    assert snapshot["write_total"] == sum(
        cast(int, snapshot[f"write_{outcome}"]) for outcome in outcomes
    )
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


def test_populated_snapshot_stays_under_size_bound_as_observations_grow() -> None:
    clock = FakeClock()
    recorder = ResponseDiagnostics(clock=clock)
    outcomes: tuple[WriteOutcome, ...] = (
        "ok",
        "error",
        "timeout_bus",
        "timeout_async",
        "cancelled",
        "detached_late_ok",
        "detached_late_error",
    )
    rebuild_reasons: tuple[SessionRebuildReason, ...] = (
        "mqtt_reader_dead",
        "bus_stale",
        "bus_write_stuck",
        "bus_reconnect_storm",
        "mqtt_transport_overload",
        "other",
    )
    # Arithmetic leaves long decimal representations, unlike rounded fixture values.
    queue_wait_s = 0.1 + 0.2
    rpc_s = 15.1 - 0.3

    def observe(count: int) -> None:
        for index in range(count):
            recorder.note_write_settled(outcomes[index % len(outcomes)], queue_wait_s, rpc_s)
            recorder.note_superseded()
            recorder.note_bus_reconnect()
            recorder.note_hard_cap()
            recorder.note_session_rebuild(rebuild_reasons[index % len(rebuild_reasons)])
            clock.advance(queue_wait_s + rpc_s)

    observe(64)
    snapshot = recorder.snapshot()
    assert snapshot["queue_wait_s_count"] == snapshot["rpc_s_count"] == 64
    for value in snapshot.values():
        if isinstance(value, dict):
            assert all(count > 0 for count in value.values())
        else:
            assert value is not None and value > 0
    assert snapshot["queue_wait_s_recent_max"] == queue_wait_s
    assert snapshot["rpc_s_recent_max"] == rpc_s
    assert len(json.dumps(snapshot["queue_wait_s_recent_max"])) >= 18
    assert len(json.dumps(snapshot["rpc_s_recent_max"])) >= 18
    populated_size = len(json.dumps(snapshot).encode())
    assert populated_size < 4096

    observe(64_000)
    later_snapshot = recorder.snapshot()
    assert (
        later_snapshot["write_total"]
        == later_snapshot["queue_wait_s_count"]
        == later_snapshot["rpc_s_count"]
        == 64_064
    )
    later_size = len(json.dumps(later_snapshot).encode())
    assert later_size < 4096


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
    clock_calls = 0

    def counted_clock() -> float:
        nonlocal clock_calls
        clock_calls += 1
        return clock()

    adapter._clock = counted_clock
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
    # Clock-read work is part of #152's low-overhead contract: reject extra
    # per-write metric reads. Enqueue + start + settlement, plus success logging.
    assert clock_calls == (4 if error is None else 3)
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


@pytest.mark.parametrize("phase", ["before_step", "queued", "rpc", "shutdown"])
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
    if phase == "shutdown":
        await adapter.shutdown()
    else:
        task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller
    if lock.locked():
        lock.release()
    await _settle()
    snapshot = recorder.snapshot()
    ran_rpc = phase in ("rpc", "shutdown")
    assert snapshot["write_cancelled"] == snapshot["write_total"] == 1
    assert snapshot["queue_wait_s_count"] == (1 if ran_rpc else 0)
    assert snapshot["queue_wait_s_recent_max"] == (0.0 if ran_rpc else None)
    assert snapshot["rpc_s_count"] == (1 if ran_rpc else 0)
    assert snapshot["rpc_s_sum"] == (4.0 if ran_rpc else 0.0)
    assert snapshot["rpc_s_recent_max"] == (4.0 if ran_rpc else None)
    assert bool(observer.writes) == ran_rpc
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


def _settings() -> Settings:
    return Settings(
        panel="synthetic-panel",
        mqtt_host="broker.invalid",
        mqtt_username="u",
        mqtt_password="p",
        motion_reconcile_enabled=False,
        bus_heartbeat_file="",
        bus_phase_file="",
    )


async def test_supersession_moves_between_disjoint_transport_and_lane_pending_sets() -> None:
    recorder = ResponseDiagnostics(clock=FakeClock())
    adapter = mqttio.AioMqttAdapter(_settings(), diagnostics=recorder)
    client = cast(_AiomqttClientInternals, adapter._client)
    transport = client._queue
    topic = "brilliant/synthetic-panel/light/set"
    transport.put_nowait(_msg(topic, b"first"))
    transport.put_nowait(_msg(topic, b"second"))
    assert recorder.snapshot()["superseded_before_dispatch"] == 1

    # The first command died in transport; only the second can reach the lane.
    lane = mqttio._LaneQueue(8, diagnostics=recorder)
    moved = transport.get_nowait()
    await lane.put(
        mqttio._InboundMessage(str(moved.topic), "second", False, (), ()), latest_wins=True
    )
    transport.task_done()
    assert transport.empty()
    third = mqttio._InboundMessage(topic, "third", False, (), ())
    await lane.put(third, latest_wins=True)
    assert recorder.snapshot()["superseded_before_dispatch"] == 2
    assert await lane.get() is third
    lane.task_done()
    await lane.join()
    # Arrival behind an already-dispatched command replaces nothing.
    await lane.put(third, latest_wins=True)
    assert recorder.snapshot()["superseded_before_dispatch"] == 2
    await lane.get()
    lane.task_done()
    await adapter.disconnect()


async def test_adapter_dispatcher_injects_recorder_without_counting_lossless_commands() -> None:
    recorder = ResponseDiagnostics(clock=FakeClock())
    adapter = mqttio.AioMqttAdapter(_settings(), diagnostics=recorder)
    released = asyncio.Event()
    entered = asyncio.Event()
    seen: list[str] = []

    async def handle(topic: str, payload: str) -> None:
        seen.append(payload)
        entered.set()
        await released.wait()

    adapter.on_command(handle)
    dispatcher = adapter._get_topic_dispatcher()
    topic = "brilliant/synthetic-panel/light/set"
    for payload in ("running", "superseded", "newest"):
        await dispatcher.dispatch(
            mqttio._InboundMessage(topic, payload, False, (handle,), ()), latest_wins=True
        )
        if payload == "running":
            await entered.wait()
    for payload in ("lossless-one", "lossless-two"):
        await dispatcher.dispatch(
            mqttio._InboundMessage(topic, payload, False, (handle,), ()), latest_wins=False
        )
    assert recorder.snapshot()["superseded_before_dispatch"] == 1
    released.set()
    await dispatcher.shutdown()
    assert seen == ["running", "newest", "lossless-one", "lossless-two"]
    assert recorder.snapshot()["superseded_before_dispatch"] == 1
    await adapter.disconnect()


@pytest.mark.parametrize("oversized", [False, True])
def test_transport_overload_counts_only_payloads_actually_replaced(oversized: bool) -> None:
    recorder = ResponseDiagnostics(clock=FakeClock())
    topic = "brilliant/synthetic-panel/light/set"
    old = _msg(topic, b"a" * 40)
    filler = _msg("brilliant/synthetic-panel/media/set_muted", b"b" * 40)
    newest = _msg(topic, b"c" * 90)
    budget = mqttio._message_bytes(filler) + mqttio._message_bytes(newest) - 1
    queue = mqttio._BoundedTransportQueue(
        maxsize=8,
        max_bytes=budget,
        overload=mqttio._TransportOverloadLatch(),
        diagnostics=recorder,
    )
    queue.put_nowait(old)
    queue.put_nowait(filler)
    if oversized:
        with pytest.raises(asyncio.QueueFull):
            queue.put_nowait(_msg(topic, b"d" * (budget + 1)))
        assert recorder.snapshot()["superseded_before_dispatch"] == 0
    # A kept replacement counts even when the cumulative byte budget trips.
    with pytest.raises(asyncio.QueueFull):
        queue.put_nowait(newest)
    assert recorder.snapshot()["superseded_before_dispatch"] == 1
    assert queue.get_nowait() is newest


@pytest.mark.parametrize(
    ("error", "reason"),
    [
        (main_mod.MqttReaderDeadError(), "mqtt_reader_dead"),
        (main_mod.BusStaleError(), "bus_stale"),
        (main_mod.BusWriteStuckError(), "bus_write_stuck"),
        (main_mod.BusReconnectStormError(), "bus_reconnect_storm"),
        (main_mod.MqttTransportOverloadError(), "mqtt_transport_overload"),
        (RuntimeError(), "other"),
        (RetainedLedgerError("synthetic ledger failure"), "other"),
    ],
)
async def test_supervisor_recorder_survives_rebuild_and_counts_typed_reason(
    monkeypatch: pytest.MonkeyPatch, error: Exception, reason: str
) -> None:
    seen: list[ResponseDiagnostics] = []

    async def session(
        settings: Settings,
        desired_panel: DesiredState | None,
        desired_mesh: DesiredState | None,
        diagnostics: ResponseDiagnostics | None = None,
    ) -> None:
        # Cancel instead of raising an assertion in the retry loop if not wired.
        if diagnostics is None:
            raise asyncio.CancelledError
        seen.append(diagnostics)
        if len(seen) == 1:
            diagnostics.note_write_settled("ok", 2.0, 3.0)
            diagnostics.note_bus_reconnect()
            raise error
        raise asyncio.CancelledError

    monkeypatch.setattr(main_mod, "_run_session", session)
    monkeypatch.setattr(main_mod, "_BACKOFF_S", 0)
    monkeypatch.setattr(main_mod, "_LEDGER_BACKOFF_S", 0)
    with pytest.raises(asyncio.CancelledError):
        await main_mod.run(_settings())
    assert len(seen) == 2
    assert seen[0] is seen[1]
    snapshot = seen[1].snapshot()
    assert snapshot["write_total"] == snapshot["bus_reconnect_total"] == 1
    assert snapshot["rpc_s_sum"] == 3.0
    reasons = snapshot["session_rebuild"]
    assert isinstance(reasons, dict)
    assert reasons[reason] == 1
    assert sum(reasons.values()) == 1


async def test_session_wires_one_recorder_and_shutdown_adds_no_diagnostic_publish(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    recorder = ResponseDiagnostics(clock=FakeClock())
    bus = FakeBus([_panel_dimmer()])
    mqtt = FakeMqtt()
    injected: list[ResponseDiagnostics | None] = []

    def bus_factory(
        *, extra_device_ids: tuple[str, ...], diagnostics: ResponseDiagnostics | None = None
    ) -> FakeBus:
        injected.append(diagnostics)
        return bus

    def mqtt_factory(
        settings: Settings, *, diagnostics: ResponseDiagnostics | None = None
    ) -> FakeMqtt:
        injected.append(diagnostics)
        return mqtt

    sleeper = FakeSleeper()
    ready = asyncio.Event()

    async def sleep(seconds: float) -> None:
        ready.set()
        await sleeper(seconds)

    monkeypatch.setattr(main_mod, "RpcBusAdapter", bus_factory)
    monkeypatch.setattr(main_mod, "AioMqttAdapter", mqtt_factory)
    # Patch the supervisor's module reference, preserving asyncio in the fakes.
    monkeypatch.setattr(
        main_mod, "asyncio", SimpleNamespace(sleep=sleep, CancelledError=asyncio.CancelledError)
    )
    settings = replace(
        _settings(), retained_topics_file=str(tmp_path / "owned.json"), deployment_id="a" * 32
    )
    task = asyncio.create_task(main_mod._run_session(settings, None, None, recorder))
    task.add_done_callback(lambda _: ready.set())
    try:
        await asyncio.wait_for(ready.wait(), timeout=2)
        if task.done():
            await task
        assert sleeper.requested, "session never reached its normal loop"
        assert injected == [recorder, recorder]
        meta = [
            json.loads(payload) for topic, payload, _ in mqtt.published if topic.endswith("/bridge")
        ]
        assert len(meta) == 1
        assert meta[0]["diag"] == recorder.snapshot()
        before_shutdown = list(mqtt.published)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert mqtt.disconnect_count == 1
    assert mqtt.published == before_shutdown
    assert [
        (payload, retained)
        for topic, payload, retained in mqtt.published
        if topic.endswith("/availability")
    ] == [("online", True)]


async def test_meta_diag_is_additive_panel_only_and_preserves_command_and_entity_behavior() -> None:
    recorder = ResponseDiagnostics(clock=FakeClock())
    recorder.note_write_settled("ok", 1.0, 2.0)
    mqtt = FakeMqtt()
    baseline_mqtt = FakeMqtt()
    bus = FakeBus([_panel_dimmer()])
    baseline_bus = FakeBus([_panel_dimmer()])
    bridge = Bridge(bus, mqtt, "synthetic-panel", diagnostics=recorder, deployment_id="a" * 32)
    baseline = Bridge(baseline_bus, baseline_mqtt, "synthetic-panel", deployment_id="a" * 32)
    await baseline.reconcile()
    await bridge.reconcile()
    assert len(mqtt.published) == len(baseline_mqtt.published)
    for actual, expected in zip(mqtt.published, baseline_mqtt.published, strict=True):
        topic, payload, retained = actual
        if topic.endswith("/bridge"):
            meta = json.loads(payload)
            assert meta.pop("diag") == recorder.snapshot()
            assert meta == json.loads(expected[1])
            assert meta["agent_version"] == __version__
            assert re.fullmatch(r"[0-9a-f]{32}", meta["deployment_id"])
            assert retained is True
        else:
            assert actual == expected
    await mqtt.inject("brilliant/synthetic-panel/gangbox_peripheral_0/set", '{"state":"ON"}')
    await baseline_mqtt.inject(
        "brilliant/synthetic-panel/gangbox_peripheral_0/set", '{"state":"ON"}'
    )
    assert bus.commands == baseline_bus.commands
    assert bus.commands
    # Publication doesn't reset counters; the mesh pseudo-panel publishes no meta.
    await bridge.reconcile()
    assert recorder.snapshot()["write_total"] == 1
    mesh_mqtt = FakeMqtt()
    mesh = Bridge(FakeBus([]), mesh_mqtt, "mesh", diagnostics=recorder)
    await mesh.reconcile()
    assert all(not topic.endswith("/bridge") for topic, _, _ in mesh_mqtt.published)
