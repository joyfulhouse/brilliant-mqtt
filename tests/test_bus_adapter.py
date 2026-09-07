"""Tests for RpcBusAdapter's off-panel-testable plumbing.

The connection itself needs the panel libraries, but the push-liveness
tracking and the reconnect fan-out are plain code: RpcBusAdapter constructs
fine anywhere (all panel imports are deferred into start()).

Why this exists (pilot, 2026-06-12): the observer's notification stream can
die silently while the process keeps running — pushes stop AND the get_all
mirror freezes — until the processor auto-reconnects. The adapter therefore
(a) timestamps every inbound push so the run loop can detect a stale stream,
and (b) fans the processor's reconnect signal out to re-subscribe + a
bridge-level callback.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import types
from typing import Any, TypeVar

import pytest

from brilliant_mqtt import bus as bus_mod
from brilliant_mqtt.bus import RpcBusAdapter, _session_client_name
from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.model import BrilliantDevice
from tests.fakes import FakeClock


class TestSessionClientName:
    """The bus peer name (``<owning_device_id>.<my_name>``) must differ per
    session, or a half-bound registration left by a connect that timed out
    mid-handshake becomes a permanent ghost that rejects every later
    connection with NameInUseError (adu-bath incident, 2026-07-05)."""

    def test_suffix_appended_to_base(self) -> None:
        name = _session_client_name("brilliant_mqtt")
        assert name.startswith("brilliant_mqtt-")
        assert name != "brilliant_mqtt"

    def test_each_call_is_unique(self) -> None:
        names = {_session_client_name("brilliant_mqtt") for _ in range(50)}
        assert len(names) == 50

    def test_adapter_gets_a_unique_client_name_per_session(self) -> None:
        # Two sessions (run() builds a fresh adapter each loop) must not share a
        # name, so a stale ghost from one can never lock out the next.
        a = RpcBusAdapter()
        b = RpcBusAdapter()
        assert a._my_name.startswith("brilliant_mqtt-")
        assert a._my_name != b._my_name


class TestPushLiveness:
    def test_no_pushes_yet_returns_none(self) -> None:
        adapter = RpcBusAdapter()
        assert adapter.seconds_since_last_push() is None

    def test_note_push_starts_the_clock(self) -> None:
        adapter = RpcBusAdapter()
        adapter._note_push()
        age = adapter.seconds_since_last_push()
        assert age is not None
        assert 0.0 <= age < 5.0


class TestReconnectFanout:
    async def test_reaches_all_registered_callbacks(self) -> None:
        adapter = RpcBusAdapter()
        calls: list[str] = []

        async def first() -> None:
            calls.append("first")

        async def second() -> None:
            calls.append("second")

        adapter.on_reconnect(first)
        adapter.on_reconnect(second)

        await adapter._after_reconnect()

        assert calls == ["first", "second"]

    async def test_one_callback_failure_does_not_starve_later_callbacks(self) -> None:
        adapter = RpcBusAdapter()
        calls: list[str] = []

        async def broken() -> None:
            calls.append("broken")
            raise RuntimeError("broken")

        async def healthy() -> None:
            calls.append("healthy")

        adapter.on_reconnect(broken)
        adapter.on_reconnect(healthy)

        await adapter._after_reconnect()

        assert calls == ["broken", "healthy"]

    async def test_resubscribe_runs_before_callback(self) -> None:
        adapter = RpcBusAdapter()
        calls: list[str] = []

        async def fake_resubscribe() -> None:
            calls.append("resubscribe")

        async def reconnect_cb() -> None:
            calls.append("callback")

        adapter._resubscribe = fake_resubscribe
        adapter.on_reconnect(reconnect_cb)

        await adapter._after_reconnect()
        assert calls == ["resubscribe", "callback"]

    async def test_resubscribe_failure_still_fires_callback(self) -> None:
        adapter = RpcBusAdapter()
        fired: list[str] = []

        async def failing_resubscribe() -> None:
            raise RuntimeError("bus says no")

        async def reconnect_cb() -> None:
            fired.append("callback")

        adapter._resubscribe = failing_resubscribe
        adapter.on_reconnect(reconnect_cb)

        await adapter._after_reconnect()  # must not raise
        assert fired == ["callback"]

    async def test_callback_failure_is_swallowed(self) -> None:
        adapter = RpcBusAdapter()

        async def failing_cb() -> None:
            raise RuntimeError("bridge says no")

        adapter.on_reconnect(failing_cb)
        await adapter._after_reconnect()  # must not raise

    async def test_without_registrations_is_a_no_op(self) -> None:
        adapter = RpcBusAdapter()
        await adapter._after_reconnect()  # nothing registered; must not raise

    async def test_proc_reconnect_schedules_async_handler(self) -> None:
        """The lib invokes its reconnect callbacks synchronously on the loop;
        the adapter must bounce that into an async task."""
        adapter = RpcBusAdapter()
        fired = asyncio.Event()

        async def reconnect_cb() -> None:
            fired.set()

        adapter.on_reconnect(reconnect_cb)
        adapter._on_proc_reconnect()
        await asyncio.wait_for(fired.wait(), timeout=2.0)

    async def test_proc_reconnect_marks_push_liveness(self) -> None:
        """A reconnect proves the stream is alive again: reset the stale clock
        so the watchdog does not immediately tear down the fresh session."""
        adapter = RpcBusAdapter()
        assert adapter.seconds_since_last_push() is None
        adapter._on_proc_reconnect()
        age = adapter.seconds_since_last_push()
        assert age is not None
        assert age < 5.0


class TestReconnectRate:
    """The adapter counts processor reconnects in a sliding window so the run
    loop can trip a circuit breaker on a reconnect STORM — a failure mode the
    stale watchdog misses because every reconnect also resets the push clock
    (live incident, 2026-06-13: ~5 reconnects/sec masked staleness)."""

    def test_no_reconnects_yet_is_zero(self) -> None:
        adapter = RpcBusAdapter()
        assert adapter.recent_reconnects(60.0) == 0

    async def test_counts_reconnects_within_window(self) -> None:
        clock = FakeClock()
        adapter = RpcBusAdapter(clock=clock)
        for _ in range(5):
            adapter._on_proc_reconnect()
            clock.advance(1.0)
        await asyncio.gather(*adapter._pending_tasks)
        # All five landed within the last 60s (now t=5).
        assert adapter.recent_reconnects(60.0) == 5

    async def test_excludes_reconnects_older_than_window(self) -> None:
        clock = FakeClock()
        adapter = RpcBusAdapter(clock=clock)
        adapter._on_proc_reconnect()  # t=0 — ages out
        clock.advance(100.0)
        adapter._on_proc_reconnect()  # t=100
        adapter._on_proc_reconnect()  # t=100
        await asyncio.gather(*adapter._pending_tasks)
        # Window is 60s back from now (t=100): only the two at t=100 count.
        assert adapter.recent_reconnects(60.0) == 2


class TestExtraDeviceIds:
    """M11: the adapter can bridge EXTRA bus devices beyond the panel's own —
    e.g. the virtual "ble_mesh" device carrying the home's plug-in mesh loads.
    Only construction is testable off-panel; subscribe/get_all need the libs."""

    def test_defaults_to_no_extras(self) -> None:
        adapter = RpcBusAdapter()
        assert adapter._extra_device_ids == ()

    def test_extras_are_stored(self) -> None:
        adapter = RpcBusAdapter(extra_device_ids=("ble_mesh",))
        assert adapter._extra_device_ids == ("ble_mesh",)


class _ScopedReadObserver:
    """Observer double that rejects every read except get_peripheral()."""

    def __init__(self, peripheral: _RawPeripheral | None) -> None:
        self.peripheral = peripheral
        self.calls: list[tuple[str, str, str]] = []

    async def get_peripheral(self, device_id: str, peripheral_id: str) -> _RawPeripheral | None:
        self.calls.append(("get_peripheral", device_id, peripheral_id))
        return self.peripheral

    async def get_all(self) -> None:
        raise AssertionError("scoped read must not call get_all()")

    async def get_device(self, device_id: str) -> None:
        raise AssertionError(f"scoped read must not call get_device({device_id!r})")


class TestGetPeripheral:
    async def test_calls_only_observer_get_peripheral(self) -> None:
        adapter = RpcBusAdapter()
        observer = _ScopedReadObserver(_RawPeripheral())
        adapter._obs = observer
        adapter._own_device_id = "own-device"

        device = await adapter.get_peripheral("configuration_virtual_device", "scene_configuration")

        assert device is not None
        assert device.device_id == "configuration_virtual_device"
        assert device.peripheral_id == "scene_configuration"
        assert observer.calls == [
            (
                "get_peripheral",
                "configuration_virtual_device",
                "scene_configuration",
            )
        ]

    async def test_missing_peripheral_returns_none(self) -> None:
        adapter = RpcBusAdapter()
        observer = _ScopedReadObserver(None)
        adapter._obs = observer
        adapter._own_device_id = "own-device"

        device = await adapter.get_peripheral("configuration_virtual_device", "scene_configuration")

        assert device is None


class _RawVariable:
    """Duck-typed stand-in for a bus Variable (normalize_peripheral contract)."""

    def __init__(self, value: str) -> None:
        self.value = value
        self.externally_settable = True


class _RawPeripheral:
    """Duck-typed stand-in for a bus Peripheral."""

    def __init__(self, value: str = "1", variable_name: str = "on") -> None:
        self.name = "Mesh Switch"
        self.peripheral_type = 1
        self.variables = {variable_name: _RawVariable(value)}


class _RawDevice:
    """Duck-typed stand-in for a bus Device push; ``id`` is optional so the
    fallback path (no id on the raw struct) is exercisable, and ``peripherals``
    may be None to exercise the housekeeping-notification guard."""

    def __init__(
        self, device_id: str | None, peripherals: dict[str, _RawPeripheral] | None
    ) -> None:
        if device_id is not None:
            self.id = device_id
        self.peripherals = peripherals


class _DeviceReadObserver:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def get_device(self, device_id: str) -> _RawDevice:
        self.calls.append(device_id)
        return _RawDevice(device_id, {f"{device_id}-peripheral": _RawPeripheral()})


class TestGetAllScope:
    async def test_without_extras_reads_only_own_device(self) -> None:
        adapter = RpcBusAdapter(extra_device_ids=("ble_mesh",))
        observer = _DeviceReadObserver()
        adapter._obs = observer
        adapter._own_device_id = "own-device"

        devices = await adapter.get_all(include_extras=False)

        assert observer.calls == ["own-device"]
        assert [device.device_id for device in devices] == ["own-device"]

    async def test_default_read_still_includes_extras(self) -> None:
        adapter = RpcBusAdapter(extra_device_ids=("ble_mesh",))
        observer = _DeviceReadObserver()
        adapter._obs = observer
        adapter._own_device_id = "own-device"

        devices = await adapter.get_all()

        assert observer.calls == ["own-device", "ble_mesh"]
        assert [device.device_id for device in devices] == ["own-device", "ble_mesh"]


class _BlockingRpcObserver:
    def __init__(self) -> None:
        self.read_cancelled = False
        self.write_cancelled = False

    async def get_device(self, device_id: str) -> _RawDevice:
        del device_id
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            self.read_cancelled = True
            raise
        raise AssertionError("unreachable")

    async def request_set_variables_in_peripheral(
        self,
        peripheral_id: str,
        values: dict[str, str],
        *,
        device_id: str,
    ) -> None:
        del peripheral_id, values, device_id
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            self.write_cancelled = True
            raise


class TestRpcDeadlines:
    def test_defaults_are_fixed_backstops(self) -> None:
        assert bus_mod._READ_DEADLINE_S == 5.0
        assert bus_mod._WRITE_DEADLINE_S == 5.0
        assert bus_mod._WRITE_HARD_CAP_S == 15.0

    async def test_get_all_scoped_read_times_out_and_cancels_rpc(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(bus_mod, "_READ_DEADLINE_S", 0.01)
        adapter = RpcBusAdapter()
        observer = _BlockingRpcObserver()
        adapter._obs = observer
        adapter._own_device_id = "own-device"

        with pytest.raises((TimeoutError, asyncio.TimeoutError)):
            await adapter.get_all()

        assert observer.read_cancelled is True

    async def test_set_variables_deadline_detaches_without_cancelling_rpc(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Issue #72: the caller deadline no longer cancels the closed-source
        write nor latches a session rebuild — the RPC keeps running detached."""
        monkeypatch.setattr(bus_mod, "_WRITE_DEADLINE_S", 0.01)
        adapter = RpcBusAdapter()
        observer = _BlockingRpcObserver()
        adapter._obs = observer
        adapter._own_device_id = "own-device"

        with pytest.raises(asyncio.TimeoutError):
            await adapter.set_variables(
                "own-device",
                "gangbox_peripheral_0",
                [VarSet("on", "1")],
            )

        assert observer.write_cancelled is False
        assert len(adapter._write_tasks) == 1
        assert not next(iter(adapter._write_tasks)).done()
        assert adapter.consume_write_timeout() is False
        await adapter.shutdown()


class _GatedRpcObserver:
    """Observer whose writes block until ``release`` is set.

    Tracks write concurrency (the per-device lock contract), cancellation
    (the detach contract) and whether reads got through while a write was
    blocked (reads must never queue behind a stalled write).
    """

    def __init__(self, *, fail_with: Exception | None = None) -> None:
        self.release = asyncio.Event()
        self.in_flight = 0
        self.max_in_flight = 0
        self.write_cancelled = False
        self.writes: list[tuple[str, str]] = []
        self.reads: list[str] = []
        self.shutdowns = 0
        self._fail_with = fail_with

    async def get_device(self, device_id: str) -> _RawDevice:
        self.reads.append(device_id)
        return _RawDevice(device_id, {f"{device_id}-peripheral": _RawPeripheral()})

    async def request_set_variables_in_peripheral(
        self,
        peripheral_id: str,
        values: dict[str, str],
        *,
        device_id: str,
    ) -> str:
        del values
        self.writes.append((device_id, peripheral_id))
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            await self._block()
        finally:
            self.in_flight -= 1
        if self._fail_with is not None:
            raise self._fail_with
        return "ok"

    async def _block(self) -> None:
        """The blocking core of a write; subclasses vary the cancellation reaction."""
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.write_cancelled = True
            raise

    async def shutdown(self) -> None:
        self.shutdowns += 1


class _StickyRpcObserver(_GatedRpcObserver):
    """Gated observer whose write SWALLOWS cancellation and keeps waiting on
    ``cancel_release`` — models a closed-source coroutine that delays or
    suppresses cancellation during session teardown."""

    def __init__(self) -> None:
        super().__init__()
        self.cancel_release = asyncio.Event()
        self.cancel_swallowed = False

    async def _block(self) -> None:
        try:
            await super()._block()
        except asyncio.CancelledError:
            self.cancel_swallowed = True
            await self.cancel_release.wait()


_ObserverT = TypeVar("_ObserverT", bound=_GatedRpcObserver)


def _adapter_for(observer: _ObserverT) -> tuple[_ObserverT, RpcBusAdapter]:
    """A started-looking adapter wired to ``observer`` (no real bus)."""
    adapter = RpcBusAdapter(extra_device_ids=("ble_mesh",))
    adapter._obs = observer
    adapter._own_device_id = "own-device"
    return observer, adapter


def _gated_adapter(
    *, fail_with: Exception | None = None
) -> tuple[_GatedRpcObserver, RpcBusAdapter]:
    return _adapter_for(_GatedRpcObserver(fail_with=fail_with))


async def _detached_write(
    adapter: RpcBusAdapter, device_id: str = "ble_mesh", peripheral_id: str = "mesh_light_1"
) -> asyncio.Task[str]:
    """Issue a write that hits the caller deadline; return its still-running task."""
    before = set(adapter._write_tasks)
    with pytest.raises(asyncio.TimeoutError):
        await adapter.set_variables(device_id, peripheral_id, [VarSet("on", "0")])
    (task,) = adapter._write_tasks - before
    return task


async def _settle(n: int = 3) -> None:
    """Yield a few loop iterations so freshly created tasks reach their awaits."""
    for _ in range(n):
        await asyncio.sleep(0)


class TestDetachedWrites:
    """Issue #72: writes past the caller deadline run on detached; only the
    fixed hard cap latches a session rebuild."""

    async def test_late_completion_resolves_and_logs_without_latching(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        monkeypatch.setattr(bus_mod, "_WRITE_DEADLINE_S", 0.01)
        observer, adapter = _gated_adapter()

        with caplog.at_level(logging.DEBUG, logger="brilliant_mqtt.bus"):
            task = await _detached_write(adapter)
            observer.release.set()
            assert await task == "'ok'"
            await _settle()

        assert adapter._write_tasks == set()
        assert adapter.consume_write_timeout() is False
        messages = [r.getMessage() for r in caplog.records]
        assert any("detach" in m for m in messages)
        late = [m for m in messages if "completed" in m and "ble_mesh/mesh_light_1" in m]
        assert late and "'ok'" in late[0]
        assert any("queue wait" in m for m in messages)

    async def test_late_exception_is_retrieved_and_logged(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        monkeypatch.setattr(bus_mod, "_WRITE_DEADLINE_S", 0.01)
        observer, adapter = _gated_adapter(fail_with=RuntimeError("late boom"))

        with caplog.at_level(logging.WARNING, logger="brilliant_mqtt.bus"):
            task = await _detached_write(adapter)
            observer.release.set()
            with pytest.raises(RuntimeError, match="late boom"):
                await task
            await _settle()

        assert adapter._write_tasks == set()
        assert adapter.consume_write_timeout() is False
        failed = [r for r in caplog.records if "failed" in r.getMessage()]
        assert failed and failed[0].exc_info is not None
        # The task's exception was retrieved (no "never retrieved" at GC).
        assert task.exception() is not None

    async def test_hard_cap_latches_once_for_several_capped_writes(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(bus_mod, "_WRITE_DEADLINE_S", 0.005)
        monkeypatch.setattr(bus_mod, "_WRITE_HARD_CAP_S", 0.02)
        observer, adapter = _gated_adapter()

        for device_id in ("own-device", "ble_mesh"):
            await _detached_write(adapter, device_id, "p")
        assert adapter.consume_write_timeout() is False  # deadline alone never latches

        await asyncio.sleep(0.05)  # both writes cross the cap

        assert adapter.consume_write_timeout() is True
        assert adapter.consume_write_timeout() is False
        observer.release.set()
        await asyncio.gather(*adapter._write_tasks)
        assert adapter.consume_write_timeout() is False  # completion after the cap adds nothing

    async def test_shutdown_settles_outstanding_detached_write(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(bus_mod, "_WRITE_DEADLINE_S", 0.01)
        observer, adapter = _gated_adapter()

        task = await _detached_write(adapter)

        await adapter.shutdown()

        assert task.done()
        assert adapter._write_tasks == set()
        assert observer.write_cancelled is True
        assert observer.shutdowns == 1


class TestShutdownLifecycle:
    """Tribunal round 1 (#73): teardown must be race-free and bounded."""

    async def test_shutdown_rejects_writes_issued_during_settlement(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A lane callback that reaches set_variables() after shutdown()
        started must be rejected — never a fresh task nobody settles."""
        monkeypatch.setattr(bus_mod, "_WRITE_DEADLINE_S", 0.01)
        observer, adapter = _adapter_for(_StickyRpcObserver())
        outstanding = await _detached_write(adapter)

        shutting_down = asyncio.create_task(adapter.shutdown())
        await _settle()
        assert observer.cancel_swallowed is True  # settlement is blocked mid-way

        with pytest.raises(RuntimeError, match="shutting down"):
            await asyncio.wait_for(
                adapter.set_variables("ble_mesh", "mesh_light_2", [VarSet("on", "1")]),
                timeout=0.5,
            )

        assert adapter._write_tasks == {outstanding}
        observer.cancel_release.set()
        await asyncio.wait_for(shutting_down, timeout=1)
        assert observer.shutdowns == 1
        assert adapter._write_tasks == set()

    async def test_shutdown_bounds_settlement_and_still_closes_observer(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A write that never honours cancellation must not hang teardown:
        the observer is still closed after the fixed settlement bound."""
        monkeypatch.setattr(bus_mod, "_WRITE_DEADLINE_S", 0.01)
        monkeypatch.setattr(bus_mod, "_WRITE_SETTLE_TIMEOUT_S", 0.05)
        observer, adapter = _adapter_for(_StickyRpcObserver())
        straggler = await _detached_write(adapter)

        with caplog.at_level(logging.WARNING, logger="brilliant_mqtt.bus"):
            await asyncio.wait_for(adapter.shutdown(), timeout=1)

        assert observer.shutdowns == 1
        assert not straggler.done()
        assert adapter._write_tasks == {straggler}
        stragglers = [r.getMessage() for r in caplog.records if "did not settle" in r.getMessage()]
        assert stragglers and "ble_mesh/mesh_light_1" in stragglers[0]

        # Whenever it finally settles, the done-callback still consumes it.
        observer.cancel_release.set()
        assert await straggler == "'ok'"
        await _settle()
        assert adapter._write_tasks == set()

    async def test_caller_released_when_write_task_cancelled_before_start(self) -> None:
        """A write task cancelled before its first step never runs _run_write,
        so nothing inside it can release the caller — the caller must still
        not hang."""
        observer, adapter = _gated_adapter()
        caller = asyncio.create_task(
            adapter.set_variables("ble_mesh", "mesh_light_1", [VarSet("on", "0")])
        )
        await asyncio.sleep(0)  # the caller created its write task; it has not run yet
        (task,) = adapter._write_tasks
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(caller, timeout=0.5)

        assert observer.writes == []
        assert adapter._write_tasks == set()


class TestPerDeviceWriteLock:
    async def test_same_device_writes_serialize(self) -> None:
        observer, adapter = _gated_adapter()

        first = asyncio.create_task(
            adapter.set_variables("ble_mesh", "mesh_light_1", [VarSet("on", "0")])
        )
        second = asyncio.create_task(
            adapter.set_variables("ble_mesh", "mesh_light_2", [VarSet("on", "0")])
        )
        await _settle()
        assert observer.in_flight == 1
        observer.release.set()
        assert list(await asyncio.gather(first, second)) == ["'ok'", "'ok'"]

        assert observer.max_in_flight == 1
        assert observer.writes == [("ble_mesh", "mesh_light_1"), ("ble_mesh", "mesh_light_2")]

    async def test_different_devices_write_concurrently(self) -> None:
        observer, adapter = _gated_adapter()

        mesh = asyncio.create_task(
            adapter.set_variables("ble_mesh", "mesh_light_1", [VarSet("on", "0")])
        )
        own = asyncio.create_task(
            adapter.set_variables("own-device", "gangbox_peripheral_0", [VarSet("on", "0")])
        )
        await _settle()
        assert observer.in_flight == 2
        observer.release.set()
        await asyncio.gather(mesh, own)

        assert observer.max_in_flight == 2

    async def test_read_is_not_queued_behind_a_blocked_write(self) -> None:
        observer, adapter = _gated_adapter()

        write = asyncio.create_task(
            adapter.set_variables("ble_mesh", "mesh_light_1", [VarSet("on", "0")])
        )
        await _settle()
        assert observer.in_flight == 1

        devices = await asyncio.wait_for(adapter.get_all(), timeout=0.1)

        assert observer.reads == ["own-device", "ble_mesh"]
        assert [d.device_id for d in devices] == ["own-device", "ble_mesh"]
        assert observer.in_flight == 1  # the write is still blocked, untouched
        observer.release.set()
        await write

    async def test_deadline_is_measured_after_lock_acquisition(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A writer queued behind a slow same-device write must not burn its
        own deadline while waiting for the lock — and the slow write keeps
        the lock past its caller's deadline (asserted event-based, before
        the observer is released; no timer-order dependence)."""
        monkeypatch.setattr(bus_mod, "_WRITE_DEADLINE_S", 0.05)
        observer, adapter = _gated_adapter()

        first = asyncio.create_task(
            adapter.set_variables("ble_mesh", "mesh_light_1", [VarSet("on", "0")])
        )
        second = asyncio.create_task(
            adapter.set_variables("ble_mesh", "mesh_light_2", [VarSet("on", "0")])
        )
        with pytest.raises(asyncio.TimeoutError):
            await first  # the slow write detached at its deadline...

        # ...and STILL holds the device lock: exactly one observer write has
        # started, the second is queued behind it, not running concurrently.
        assert observer.writes == [("ble_mesh", "mesh_light_1")]
        assert observer.in_flight == 1
        assert observer.max_in_flight == 1
        assert not second.done()

        await asyncio.sleep(0.06)  # total queue wait now exceeds the deadline
        observer.release.set()
        assert await second == "'ok'"  # queue wait did not count against it
        assert observer.max_in_flight == 1
        await asyncio.gather(*adapter._write_tasks)


class TestDispatchFanout:
    """_dispatch_raw_device is plain code (its input is duck-typed), so the M11
    changes are pinned off-panel: the normalized device_id comes from the RAW
    device (mesh pushes carry "ble_mesh", not our own id), and every registered
    change callback receives every peripheral."""

    async def test_uses_raw_device_id_and_fires_all_callbacks(self) -> None:
        adapter = RpcBusAdapter()
        seen: list[tuple[str, str, str]] = []

        async def cb_a(device: BrilliantDevice) -> None:
            seen.append(("a", device.device_id, device.peripheral_id))

        async def cb_b(device: BrilliantDevice) -> None:
            seen.append(("b", device.device_id, device.peripheral_id))

        adapter.on_change(cb_a)
        adapter.on_change(cb_b)

        raw = _RawDevice("ble_mesh", {"mesh_switch_1": _RawPeripheral()})
        adapter._dispatch_raw_device(raw)
        await asyncio.gather(*adapter._pending_tasks)

        assert sorted(seen) == [
            ("a", "ble_mesh", "mesh_switch_1"),
            ("b", "ble_mesh", "mesh_switch_1"),
        ]

    async def test_missing_raw_id_falls_back_to_own_device_id(self) -> None:
        adapter = RpcBusAdapter()
        adapter._own_device_id = "0123456789abcdef"
        seen: list[str] = []

        async def cb(device: BrilliantDevice) -> None:
            seen.append(device.device_id)

        adapter.on_change(cb)
        adapter._dispatch_raw_device(_RawDevice(None, {"p0": _RawPeripheral()}))
        await asyncio.gather(*adapter._pending_tasks)

        assert seen == ["0123456789abcdef"]

    async def test_peripheral_less_device_is_silently_ignored(self) -> None:
        """A housekeeping push without peripherals must not reach callbacks
        (and must not rely on the handler's broad exception catch)."""
        adapter = RpcBusAdapter()
        seen: list[str] = []

        async def cb(device: BrilliantDevice) -> None:
            seen.append(device.peripheral_id)

        adapter.on_change(cb)
        adapter._dispatch_raw_device(_RawDevice("ble_mesh", None))
        adapter._dispatch_raw_device(_RawDevice("ble_mesh", {}))
        await asyncio.gather(*adapter._pending_tasks)

        assert seen == []


class TestPushDispatchCoalescing:
    async def test_callbacks_choose_coalesced_or_lossless_push_delivery(self) -> None:
        adapter = RpcBusAdapter()
        coalescing_started = asyncio.Event()
        scene_started = asyncio.Event()
        release = asyncio.Event()
        coalesced: list[str] = []
        scene_events: list[str] = []
        variable_name = "execution_state:scene_execution_handler:scene:movie"

        async def coalescing_consumer(device: BrilliantDevice) -> None:
            value = device.variables[variable_name].value
            coalesced.append(value)
            if value == "watermark-1":
                coalescing_started.set()
                await release.wait()

        async def scene_consumer(device: BrilliantDevice) -> None:
            value = device.variables[variable_name].value
            scene_events.append(value)
            if value == "watermark-1":
                scene_started.set()
                await release.wait()

        adapter.on_change(coalescing_consumer)
        adapter.on_change(scene_consumer, coalesce_pushes=False)

        def push(watermark: str) -> None:
            adapter._dispatch_raw_device(
                _RawDevice(
                    "configuration_virtual_device",
                    {"execution_peripheral": _RawPeripheral(watermark, variable_name)},
                )
            )

        push("watermark-1")
        await asyncio.wait_for(coalescing_started.wait(), timeout=0.1)
        await asyncio.wait_for(scene_started.wait(), timeout=0.1)
        push("watermark-2")
        push("watermark-3")
        release.set()
        await asyncio.gather(*adapter._pending_tasks)

        assert coalesced == ["watermark-1", "watermark-3"]
        assert scene_events == ["watermark-1", "watermark-2", "watermark-3"]

    async def test_push_during_callback_keeps_only_one_trailing_snapshot(self) -> None:
        adapter = RpcBusAdapter()
        started = asyncio.Event()
        release = asyncio.Event()
        seen: list[str] = []

        async def consumer(device: BrilliantDevice) -> None:
            value = device.variables["on"].value
            seen.append(value)
            if value == "1":
                started.set()
                await release.wait()

        adapter.on_change(consumer)
        adapter._dispatch_raw_device(_RawDevice("ble_mesh", {"load": _RawPeripheral("1")}))
        await asyncio.wait_for(started.wait(), timeout=0.1)
        task = next(iter(adapter._pending_tasks))

        adapter._dispatch_raw_device(_RawDevice("ble_mesh", {"load": _RawPeripheral("2")}))
        adapter._dispatch_raw_device(_RawDevice("ble_mesh", {"load": _RawPeripheral("3")}))

        assert adapter._pending_tasks == {task}
        release.set()
        await task

        assert seen == ["1", "3"]


class _AckRpcObserver:
    """Observer whose set-variables RPC resolves with a canned response object."""

    def __init__(self, response: object) -> None:
        self._response = response
        self.calls: list[tuple[str, str, dict[str, str]]] = []

    async def request_set_variables_in_peripheral(
        self,
        peripheral_id: str,
        values: dict[str, str],
        *,
        device_id: str,
    ) -> object:
        self.calls.append((device_id, peripheral_id, dict(values)))
        return self._response


class _ReprBomb:
    """Response whose repr raises — the receipt must still normalize."""

    def __repr__(self) -> str:
        raise RuntimeError("closed-source repr exploded")


class TestSetVariablesReceipt:
    """Issue #46 passive ack instrumentation: set_variables returns a SMALL
    normalized receipt string — never the closed-source response object —
    bounded against pathological reprs. Nothing gates on its content."""

    async def _write(self, response: object) -> str:
        adapter = RpcBusAdapter()
        adapter._obs = _AckRpcObserver(response)
        adapter._own_device_id = "own-device"
        return await adapter.set_variables("ble_mesh", "mesh_light_1", [VarSet("on", "0")])

    async def test_receipt_is_the_response_repr(self) -> None:
        class SetVariablesResponse:
            def __repr__(self) -> str:
                return "SetVariablesResponse(status=0)"

        receipt = await self._write(SetVariablesResponse())
        assert receipt == "SetVariablesResponse(status=0)"

    async def test_none_response_normalizes(self) -> None:
        assert await self._write(None) == "None"

    async def test_oversized_repr_is_truncated(self) -> None:
        receipt = await self._write("x" * 500)
        assert len(receipt) == bus_mod._RECEIPT_MAX_CHARS + len("...")
        assert receipt.endswith("...")

    async def test_raising_repr_normalizes(self) -> None:
        assert await self._write(_ReprBomb()) == "<unreprable response>"


class _StartHarness:
    """Fakes the panel libraries into ``sys.modules`` so ``RpcBusAdapter.start()``
    runs off-panel, and records every processor/observer it constructs.

    #88: start() must own the observer/processor before starting them and unwind
    a partial startup on any failure. ``fail_at`` injects a failure at a chosen
    stage; the recorded ``procs``/``observers`` then prove the constructed
    resources were shut down (``live_*`` empty) rather than stranded.
    """

    def __init__(
        self,
        monkeypatch: pytest.MonkeyPatch,
        *,
        fail_at: str | tuple[str, str] | None = None,
        block_proc_start: bool = False,
        gated_writes: bool = False,
        obs_shutdown_raises: BaseException | None = None,
        hang_obs_shutdown: bool = False,
        hang_proc_shutdown: bool = False,
    ) -> None:
        self.fail_at = fail_at
        self.block_proc_start = block_proc_start
        self.gated_writes = gated_writes
        # When set, the observer's shutdown() raises this (e.g. CancelledError)
        # to prove _close_bus_resources still closes the processor afterwards.
        self.obs_shutdown_raises = obs_shutdown_raises
        self.hang_obs_shutdown = hang_obs_shutdown
        self.hang_proc_shutdown = hang_proc_shutdown
        self.procs: list[Any] = []
        self.observers: list[Any] = []
        self.subscribed: list[str] = []
        self.obs_shutdown_started = asyncio.Event()
        self.proc_shutdown_started = asyncio.Event()
        self.shutdown_release = asyncio.Event()
        # Gated-write bookkeeping (gated_writes=True): every set-variables RPC
        # blocks on write_release, recording the device order it actually
        # started on and the peak concurrency, so a test can prove same-device
        # writes serialize (max_writes_in_flight stays 1) across a restart.
        self.write_release = asyncio.Event()
        self.writes_in_flight = 0
        self.max_writes_in_flight = 0
        self.write_starts: list[str] = []
        self._proc_start_gate = asyncio.Event()
        self._install(monkeypatch)

    @property
    def live_procs(self) -> list[Any]:
        return [p for p in self.procs if not p.shut_down]

    @property
    def live_observers(self) -> list[Any]:
        return [o for o in self.observers if not o.shut_down]

    def _install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        harness = self

        class _FakeObserverBase:
            def __init__(self, loop: Any) -> None:
                self.loop = loop
                self.started = False
                self.shut_down = False
                harness.observers.append(self)

            async def start(self, proc: Any, _extra: Any) -> None:
                if harness.fail_at == "obs_start":
                    raise RuntimeError("obs start boom")
                self.started = True

            def get_owning_device_id(self) -> str:
                return "own-device"

            async def subscribe(self, request: Any) -> None:
                device_id = str(request.device_id)
                harness.subscribed.append(device_id)
                if harness.fail_at == "own_sub" and device_id == "own-device":
                    raise RuntimeError("own subscribe boom")
                if harness.fail_at == ("extra_sub", device_id):
                    raise RuntimeError(f"extra subscribe boom: {device_id}")

            async def request_set_variables_in_peripheral(
                self, peripheral_id: str, values: dict[str, str], *, device_id: str
            ) -> str:
                del peripheral_id, values
                if not harness.gated_writes:
                    return "ok"
                harness.write_starts.append(device_id)
                harness.writes_in_flight += 1
                harness.max_writes_in_flight = max(
                    harness.max_writes_in_flight, harness.writes_in_flight
                )
                try:
                    try:
                        await harness.write_release.wait()
                    except asyncio.CancelledError:
                        # Wedged closed-source write: swallow the teardown cancel
                        # and keep holding the device lock until finally released
                        # (models the #73 detached-straggler case).
                        await harness.write_release.wait()
                finally:
                    harness.writes_in_flight -= 1
                return "ok"

            async def shutdown(self) -> None:
                harness.obs_shutdown_started.set()
                if harness.obs_shutdown_raises is not None:
                    raise harness.obs_shutdown_raises
                if harness.hang_obs_shutdown:
                    try:
                        await harness.shutdown_release.wait()
                    except asyncio.CancelledError:
                        await harness.shutdown_release.wait()
                self.shut_down = True

        class _FakeProc:
            def __init__(
                self,
                *,
                socket_path: str,
                my_name: str,
                handler: Any,
                client_class: Any,
                loop: Any,
            ) -> None:
                del socket_path, handler, client_class, loop
                if harness.fail_at == "proc_construct":
                    raise RuntimeError("proc construct boom")
                self.my_name = my_name
                self.started = False
                self.shut_down = False
                self.reconnect_cbs: list[Any] = []
                harness.procs.append(self)

            async def start(self) -> None:
                if harness.fail_at == "proc_start":
                    raise RuntimeError("proc start boom")
                if harness.block_proc_start:
                    await harness._proc_start_gate.wait()
                self.started = True

            def is_connected(self) -> bool:
                return harness.fail_at != "handshake"

            def add_reconnect_callback(self, cb: Any) -> None:
                self.reconnect_cbs.append(cb)

            async def shutdown(self) -> None:
                harness.proc_shutdown_started.set()
                if harness.hang_proc_shutdown:
                    try:
                        await harness.shutdown_release.wait()
                    except asyncio.CancelledError:
                        await harness.shutdown_release.wait()
                self.shut_down = True

        class _FakeSubscriptionRequest:
            def __init__(self, device_id: str) -> None:
                self.device_id = device_id

        class _FakePeripheralServer:
            def __init__(self, observer: Any) -> None:
                self.observer = observer

        class _FakeMessageBusClient:
            pass

        def mod(name: str, **attrs: Any) -> types.ModuleType:
            module = types.ModuleType(name)
            for key, value in attrs.items():
                setattr(module, key, value)
            monkeypatch.setitem(sys.modules, name, module)
            if "." in name:
                parent_name, child = name.rsplit(".", 1)
                setattr(sys.modules[parent_name], child, module)
            return module

        mod("lib")
        mod("lib.protocol")
        mod(
            "lib.protocol.message_bus_peer_service",
            PeripheralServer=_FakePeripheralServer,
            MessageBusClient=_FakeMessageBusClient,
        )
        mod("lib.protocol.processor", SinglePeerProcessor=_FakeProc)
        mod("lib.message_bus_api")
        mod("lib.message_bus_api.observer_interface", RPCObserver=_FakeObserverBase)
        mod("thrift_types")
        mod("thrift_types.message_bus")
        mod("thrift_types.message_bus.ttypes", SubscriptionRequest=_FakeSubscriptionRequest)


def _assert_unwound(harness: _StartHarness, adapter: RpcBusAdapter) -> None:
    """A failed start() left no live processor/observer and never marked ready."""
    assert harness.live_procs == []
    assert harness.live_observers == []
    assert adapter._own_device_id is None


class TestPartialStartupUnwind:
    """#88: own obs/proc BEFORE starting them and unwind on any
    exception/cancellation, so a failure between the first allocation and the
    final commit never strands a live processor (which owns automatic-reconnect
    work). Reproduces the issue's own injection points."""

    async def test_processor_start_failure_closes_everything(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        harness = _StartHarness(monkeypatch, fail_at="proc_start")
        adapter = RpcBusAdapter()

        with pytest.raises(RuntimeError, match="proc start boom"):
            await adapter.start()

        _assert_unwound(harness, adapter)  # constructed proc + observer both shut down
        assert adapter._obs is None  # cleared by the shared close helper
        assert adapter._proc is None
        assert adapter._pending_tasks == set()
        assert adapter._write_tasks == set()

    async def test_handshake_timeout_closes_everything(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(bus_mod, "_CONNECT_TIMEOUT_S", 0.02)
        monkeypatch.setattr(bus_mod, "_CONNECT_POLL_S", 0.005)
        harness = _StartHarness(monkeypatch, fail_at="handshake")
        adapter = RpcBusAdapter()

        with pytest.raises(TimeoutError):
            await adapter.start()

        _assert_unwound(harness, adapter)  # proc started then shut down

    async def test_observer_start_failure_closes_everything(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        harness = _StartHarness(monkeypatch, fail_at="obs_start")
        adapter = RpcBusAdapter()

        with pytest.raises(RuntimeError, match="obs start boom"):
            await adapter.start()

        _assert_unwound(harness, adapter)

    async def test_own_subscription_failure_closes_everything(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        harness = _StartHarness(monkeypatch, fail_at="own_sub")
        adapter = RpcBusAdapter()

        with pytest.raises(RuntimeError, match="own subscribe boom"):
            await adapter.start()

        assert harness.subscribed == ["own-device"]  # failed on the own-device sub
        _assert_unwound(harness, adapter)

    async def test_extra_subscription_failure_closes_everything(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        harness = _StartHarness(monkeypatch, fail_at=("extra_sub", "ble_mesh"))
        adapter = RpcBusAdapter(extra_device_ids=("ble_mesh",))

        with pytest.raises(RuntimeError, match="extra subscribe boom"):
            await adapter.start()

        assert harness.subscribed == ["own-device", "ble_mesh"]  # own ok, extra failed
        _assert_unwound(harness, adapter)

    async def test_cancellation_during_startup_unwinds(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        harness = _StartHarness(monkeypatch, block_proc_start=True)
        adapter = RpcBusAdapter()

        task = asyncio.create_task(adapter.start())
        await _settle()  # reaches the blocked proc.start()
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        _assert_unwound(harness, adapter)

    async def test_three_failed_starts_do_not_accumulate(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The issue's own reproduction: repeated failed attempts on the SAME
        instance must not accumulate live processors/observers/tasks."""
        harness = _StartHarness(monkeypatch, fail_at="proc_start")
        adapter = RpcBusAdapter()

        for _ in range(3):
            with pytest.raises(RuntimeError, match="proc start boom"):
                await adapter.start()

        assert len(harness.procs) == 3  # one per attempt
        assert len(harness.observers) == 3
        assert harness.live_procs == []  # every one shut down: zero accumulated
        assert harness.live_observers == []
        assert adapter._pending_tasks == set()
        assert adapter._write_tasks == set()

    async def test_processor_construction_failure_closes_observer(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """grok r1 #3: the observer is owned before the processor is
        constructed, so a raising SinglePeerProcessor(...) constructor must
        still unwind (close) the already-owned observer rather than leak it."""
        harness = _StartHarness(monkeypatch, fail_at="proc_construct")
        adapter = RpcBusAdapter()

        with pytest.raises(RuntimeError, match="proc construct boom"):
            await adapter.start()

        assert harness.procs == []  # the processor never finished constructing
        _assert_unwound(harness, adapter)  # the owned observer was still closed
        assert adapter._obs is None

    async def test_processor_closed_even_when_observer_close_is_cancelled(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If observer close raises cancellation, its independent processor
        close task must still run before the cancellation reaches the caller.
        """
        harness = _StartHarness(monkeypatch, obs_shutdown_raises=asyncio.CancelledError())
        adapter = RpcBusAdapter()

        await adapter.start()
        proc = harness.procs[0]
        with pytest.raises(asyncio.CancelledError):
            await adapter.shutdown()

        assert proc.shut_down is True  # closed despite the observer close cancel


class TestBoundedResourceShutdown:
    @pytest.mark.parametrize("hung_resource", ["observer", "processor"])
    async def test_normal_teardown_has_one_total_resource_deadline(
        self, monkeypatch: pytest.MonkeyPatch, hung_resource: str
    ) -> None:
        """Issue #130: either closed-source close may ignore cancellation,
        while observer and processor cleanup must both be attempted promptly.
        """
        monkeypatch.setattr(bus_mod, "_RESOURCE_CLOSE_TIMEOUT_S", 0.05, raising=False)
        harness = _StartHarness(
            monkeypatch,
            hang_obs_shutdown=hung_resource == "observer",
            hang_proc_shutdown=hung_resource == "processor",
        )
        adapter = RpcBusAdapter()
        await adapter.start()
        shutdown = asyncio.create_task(adapter.shutdown())

        try:
            await asyncio.wait_for(harness.obs_shutdown_started.wait(), timeout=1)
            await asyncio.wait_for(harness.proc_shutdown_started.wait(), timeout=1)
            await asyncio.sleep(0.1)

            assert shutdown.done()
            await shutdown
            assert len(adapter._resource_close_tasks) == 1
        finally:
            harness.shutdown_release.set()
            await asyncio.gather(shutdown, return_exceptions=True)
            await _settle(5)

    @pytest.mark.parametrize("hung_resource", ["observer", "processor"])
    async def test_partial_startup_unwind_has_one_total_resource_deadline(
        self, monkeypatch: pytest.MonkeyPatch, hung_resource: str
    ) -> None:
        """Issue #130: failure unwind is bounded by the same total resource
        deadline and still reports the original startup failure afterward.
        """
        monkeypatch.setattr(bus_mod, "_RESOURCE_CLOSE_TIMEOUT_S", 0.05, raising=False)
        harness = _StartHarness(
            monkeypatch,
            fail_at="proc_start",
            hang_obs_shutdown=hung_resource == "observer",
            hang_proc_shutdown=hung_resource == "processor",
        )
        adapter = RpcBusAdapter()
        startup = asyncio.create_task(adapter.start())

        try:
            await asyncio.wait_for(harness.obs_shutdown_started.wait(), timeout=1)
            await asyncio.wait_for(harness.proc_shutdown_started.wait(), timeout=1)
            await asyncio.sleep(0.1)

            assert startup.done()
            with pytest.raises(RuntimeError, match="proc start boom"):
                await startup
            assert len(adapter._resource_close_tasks) == 1
        finally:
            harness.shutdown_release.set()
            await asyncio.gather(startup, return_exceptions=True)
            await _settle(5)

    async def test_unresolved_close_fences_fresh_adapter_until_it_settles(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Issue #130: the supervisor constructs a fresh adapter per attempt,
        which must still see and fence an unresolved peer close from its retry.
        """
        monkeypatch.setattr(bus_mod, "_RESOURCE_CLOSE_TIMEOUT_S", 0.05, raising=False)
        harness = _StartHarness(
            monkeypatch,
            fail_at="proc_start",
            hang_obs_shutdown=True,
        )
        first_adapter = RpcBusAdapter()
        retry_adapter = RpcBusAdapter()
        startup = asyncio.create_task(first_adapter.start())

        try:
            await asyncio.wait_for(harness.obs_shutdown_started.wait(), timeout=1)
            await asyncio.sleep(0.1)
            assert startup.done()
            with pytest.raises(RuntimeError, match="proc start boom"):
                await startup
            assert len(first_adapter._resource_close_tasks) == 1

            with pytest.raises(RuntimeError, match="resource shutdown still pending"):
                await retry_adapter.start()
            assert len(harness.observers) == 1
            assert len(harness.procs) == 1
            assert retry_adapter._resource_close_tasks is first_adapter._resource_close_tasks
            assert len(retry_adapter._resource_close_tasks) == 1

            harness.shutdown_release.set()
            await _settle(5)
            assert retry_adapter._resource_close_tasks == set()

            harness.fail_at = None
            await retry_adapter.start()
            assert len(harness.observers) == 2
            assert len(harness.procs) == 2
            await retry_adapter.shutdown()
        finally:
            harness.shutdown_release.set()
            await asyncio.gather(startup, return_exceptions=True)
            await _settle(5)


class TestReusedSessionSafety:
    """#88 criterion 4: a reused adapter's second start() must not stay stuck in
    the first shutdown()'s ``_shutting_down`` state (which would reject every
    write), and repeated start/shutdown cycles must not accumulate resources."""

    async def test_cycles_do_not_accumulate_and_writes_work_after_restart(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        harness = _StartHarness(monkeypatch)
        adapter = RpcBusAdapter()

        for _ in range(3):
            await adapter.start()
            # A write must succeed after each (re)start: the shutting-down fence
            # from the previous cycle has to have been reset for the new session.
            receipt = await adapter.set_variables("own-device", "p", [VarSet("on", "1")])
            assert receipt == "'ok'"
            await adapter.shutdown()

        assert len(harness.procs) == 3
        assert len(harness.observers) == 3
        assert harness.live_procs == []
        assert harness.live_observers == []
        assert adapter._pending_tasks == set()
        assert adapter._write_tasks == set()

    async def test_each_start_regenerates_the_peer_name(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A fresh peer-name suffix per start() so a ghost registration left by a
        failed attempt can never lock the reattempt out (#88, adu-bath ghost)."""
        harness = _StartHarness(monkeypatch)
        adapter = RpcBusAdapter()

        await adapter.start()
        await adapter.shutdown()
        await adapter.start()
        await adapter.shutdown()

        names = [proc.my_name for proc in harness.procs]
        assert len(names) == 2
        assert names[0] != names[1]
        assert all(name.startswith("brilliant_mqtt-") for name in names)

    async def test_restart_serializes_write_behind_prior_detached_write(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A prior session's detached write keeps holding its per-device lock
        (#73); a restart must NOT hand the same device a fresh lock, or the new
        session's write would race the still-live straggler (#88 tribunal)."""
        monkeypatch.setattr(bus_mod, "_WRITE_DEADLINE_S", 0.01)
        monkeypatch.setattr(bus_mod, "_WRITE_SETTLE_TIMEOUT_S", 0.05)
        harness = _StartHarness(monkeypatch, gated_writes=True)
        adapter = RpcBusAdapter()
        second_caller: asyncio.Task[str] | None = None

        try:
            # Session 1: a write to own-device blocks, detaches at its deadline,
            # and (swallowing the teardown cancel) keeps holding the device lock.
            await adapter.start()
            with pytest.raises(asyncio.TimeoutError):
                await adapter.set_variables("own-device", "p", [VarSet("on", "1")])
            (straggler,) = adapter._write_tasks
            assert harness.write_starts == ["own-device"]
            await asyncio.wait_for(adapter.shutdown(), timeout=1)
            assert not straggler.done()  # survived teardown, still holds the lock

            # Session 2 on the SAME adapter: a write to the SAME device must
            # queue behind the straggler, not race it on a fresh lock.
            await adapter.start()
            second_caller = asyncio.create_task(
                adapter.set_variables("own-device", "p", [VarSet("on", "0")])
            )
            await _settle(5)  # the second write reaches the device lock and blocks
            assert len(adapter._write_tasks) == 2
            assert harness.write_starts == ["own-device"]  # second RPC has NOT started
            assert harness.max_writes_in_flight == 1  # never concurrent

            # Release: the straggler finishes, THEN the second write runs serially.
            harness.write_release.set()
            assert await asyncio.wait_for(second_caller, timeout=1) == "'ok'"
            assert harness.write_starts == ["own-device", "own-device"]
            assert harness.max_writes_in_flight == 1
        finally:
            # Release the non-cooperative straggler so a failed assertion can't
            # leave it (and the queued write) blocking the event-loop teardown.
            harness.write_release.set()
            await _settle(5)
            pending = list(adapter._write_tasks)
            if second_caller is not None:
                pending.append(second_caller)
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)


class TestStaleCallbackFencing:
    """#88 criteria 2/3: a tracked callback that swallows cancellation must not
    hang teardown, and — once torn down — must not touch a subsequent session's
    state."""

    async def test_blocked_push_callback_is_bounded_and_cleared_at_shutdown(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(bus_mod, "_PENDING_SETTLE_TIMEOUT_S", 0.05, raising=False)
        harness = _StartHarness(monkeypatch)
        adapter = RpcBusAdapter()
        started = asyncio.Event()
        cancelled = asyncio.Event()
        release = asyncio.Event()
        seen: list[str] = []

        async def sticky(device: BrilliantDevice) -> None:
            seen.append(device.variables["on"].value)
            started.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancelled.set()
                await release.wait()  # non-cooperative: swallow the cancel

        adapter.on_change(sticky)
        await adapter.start()
        adapter._dispatch_raw_device(_RawDevice("own-device", {"load": _RawPeripheral("v1")}))
        await asyncio.wait_for(started.wait(), timeout=1)

        try:
            # Bounded even though the callback swallows cancellation.
            await asyncio.wait_for(adapter.shutdown(), timeout=1)
            assert cancelled.is_set()  # settle_pending did cancel the drain worker
            assert harness.live_observers == []  # bus closed despite the straggler
            assert adapter._push_tasks == {}  # snapshots cleared, not leaked
            assert adapter._pending_pushes == {}

            # When it finally settles its done-callback still discards it.
            release.set()
            await _settle()
            assert adapter._pending_tasks == set()
            assert seen == ["v1"]
        finally:
            # Always release the non-cooperative worker so a failed assertion
            # can't leave it blocking the event-loop teardown (it would swallow
            # the teardown cancel and re-block forever).
            release.set()
            await _settle()

    async def test_straggler_does_not_touch_the_next_session(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(bus_mod, "_PENDING_SETTLE_TIMEOUT_S", 0.05, raising=False)
        _StartHarness(monkeypatch)  # installs the fake panel libs (side effect)
        adapter = RpcBusAdapter()
        release = asyncio.Event()
        started = asyncio.Event()
        seen: list[str] = []

        async def sticky(device: BrilliantDevice) -> None:
            seen.append(device.variables["on"].value)
            started.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                await release.wait()  # swallow: survive this session's teardown

        adapter.on_change(sticky)

        try:
            # Session 1: a push whose callback blocks and swallows cancellation.
            await adapter.start()
            adapter._dispatch_raw_device(_RawDevice("own-device", {"load": _RawPeripheral("s1")}))
            await asyncio.wait_for(started.wait(), timeout=1)
            (straggler,) = adapter._pending_tasks
            await asyncio.wait_for(adapter.shutdown(), timeout=1)
            assert adapter._push_tasks == {}  # fenced/cleared at teardown

            # Session 2: a fresh start + push must get a NEW drain worker.
            started.clear()
            await adapter.start()
            adapter._dispatch_raw_device(_RawDevice("own-device", {"load": _RawPeripheral("s2")}))
            await asyncio.wait_for(started.wait(), timeout=1)
            (key,) = adapter._push_tasks
            session2_worker = adapter._push_tasks[key]
            assert session2_worker is not straggler

            # Release both: the session-1 straggler must NOT evict session-2's live
            # worker entry, nor drain session-2's push with the stale worker.
            release.set()
            await _settle()
            assert straggler.done()
            assert adapter._push_tasks == {}  # session-2 worker cleaned up its own entry
            assert adapter._pending_tasks == set()
            assert seen == ["s1", "s2"]  # the straggler did not drain session-2's push
        finally:
            # Always release the non-cooperative worker(s) so a failed assertion
            # can't leave one blocking the event-loop teardown.
            release.set()
            await _settle()

    async def test_stale_reconnect_fanout_does_not_fire_into_next_session(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """grok r1 #2: _after_reconnect must be session-scoped like _drain_pushes
        — a reconnect callback blocked past its session's teardown must not let
        the fan-out continue firing LATER callbacks against the next session."""
        monkeypatch.setattr(bus_mod, "_PENDING_SETTLE_TIMEOUT_S", 0.05, raising=False)
        _StartHarness(monkeypatch)  # installs the fake panel libs (side effect)
        adapter = RpcBusAdapter()
        release = asyncio.Event()
        cb1_started = asyncio.Event()
        cb2_sessions: list[int] = []

        async def cb1() -> None:
            cb1_started.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                await release.wait()  # swallow: survive this session's teardown

        async def cb2() -> None:
            cb2_sessions.append(adapter._session)

        adapter.on_reconnect(cb1)
        adapter.on_reconnect(cb2)

        try:
            await adapter.start()
            adapter._on_proc_reconnect()  # session-1 reconnect fan-out
            await asyncio.wait_for(cb1_started.wait(), timeout=1)  # parked in cb1
            await asyncio.wait_for(adapter.shutdown(), timeout=1)  # cancels fan-out; cb1 swallows

            await adapter.start()  # session 2
            release.set()  # cb1 returns; an unfenced fan-out would reach cb2 next
            await _settle(5)
            assert cb2_sessions == []  # cb2 was NOT fired against the new session
        finally:
            release.set()
            await _settle(5)

    async def test_drain_stops_mid_snapshot_when_session_moves(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """grok r1 #4: _drain_pushes must re-check the session token inside the
        per-device loop, so a session boundary that lands between two devices of
        one snapshot stops delivery instead of draining the rest into the cb."""
        monkeypatch.setattr(bus_mod, "_PENDING_SETTLE_TIMEOUT_S", 0.05, raising=False)
        _StartHarness(monkeypatch)  # installs the fake panel libs (side effect)
        adapter = RpcBusAdapter()
        release = asyncio.Event()
        first_started = asyncio.Event()
        seen: list[str] = []

        async def cb(device: BrilliantDevice) -> None:
            seen.append(device.peripheral_id)
            if device.peripheral_id == "p1":
                first_started.set()
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    await release.wait()  # swallow: survive this session's teardown

        # Lossless so the whole [p1, p2] snapshot is retained and drained in order.
        adapter.on_change(cb, coalesce_pushes=False)

        try:
            await adapter.start()
            # One raw device with TWO peripherals => one snapshot of [p1, p2].
            adapter._dispatch_raw_device(
                _RawDevice("own-device", {"p1": _RawPeripheral(), "p2": _RawPeripheral()})
            )
            await asyncio.wait_for(first_started.wait(), timeout=1)  # parked in cb(p1)
            await asyncio.wait_for(adapter.shutdown(), timeout=1)  # cancels drain; cb swallows

            await adapter.start()  # session 2
            release.set()  # cb(p1) returns; the stale drain would deliver p2 next
            await _settle(5)
            assert seen == ["p1"]  # p2 (rest of the session-1 snapshot) not delivered
        finally:
            release.set()
            await _settle(5)

    async def test_shutdown_fences_rest_of_snapshot_without_restart(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Issue #131: production discards the old adapter after shutdown, so
        its session token never advances to fence a cancellation-resistant
        drain worker. The shutdown fence itself must stop both normalization
        and callback delivery for the rest of the already-popped snapshot.
        """
        monkeypatch.setattr(bus_mod, "_PENDING_SETTLE_TIMEOUT_S", 0.05)
        _StartHarness(monkeypatch)
        adapter = RpcBusAdapter()
        release = asyncio.Event()
        first_started = asyncio.Event()
        normalized: list[str] = []
        seen: list[str] = []
        real_normalize = bus_mod.normalize_peripheral

        def recording_normalize(
            device_id: str, peripheral_id: str, raw_peripheral: Any
        ) -> BrilliantDevice:
            normalized.append(peripheral_id)
            return real_normalize(device_id, peripheral_id, raw_peripheral)

        async def callback(device: BrilliantDevice) -> None:
            seen.append(device.peripheral_id)
            if device.peripheral_id != "first":
                return
            first_started.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                await release.wait()

        monkeypatch.setattr(bus_mod, "normalize_peripheral", recording_normalize)
        adapter.on_change(callback, coalesce_pushes=False)

        try:
            await adapter.start()
            adapter._dispatch_raw_device(
                _RawDevice(
                    "own-device",
                    {"first": _RawPeripheral(), "second": _RawPeripheral()},
                )
            )
            await asyncio.wait_for(first_started.wait(), timeout=1)
            await asyncio.wait_for(adapter.shutdown(), timeout=1)

            release.set()
            await _settle(5)

            assert seen == ["first"]
            assert normalized == ["first"]
        finally:
            release.set()
            await _settle(5)

    async def test_shutdown_fences_reconnect_fanout_without_restart(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Issue #131: a reconnect fan-out that survives shutdown must not
        continue to its remaining callbacks on an old, never-restarted adapter.
        """
        monkeypatch.setattr(bus_mod, "_PENDING_SETTLE_TIMEOUT_S", 0.05)
        _StartHarness(monkeypatch)
        adapter = RpcBusAdapter()
        release = asyncio.Event()
        first_started = asyncio.Event()
        seen: list[str] = []

        async def first() -> None:
            seen.append("first")
            first_started.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                await release.wait()

        async def second() -> None:
            seen.append("second")

        adapter.on_reconnect(first)
        adapter.on_reconnect(second)

        try:
            await adapter.start()
            adapter._on_proc_reconnect()
            await asyncio.wait_for(first_started.wait(), timeout=1)
            await asyncio.wait_for(adapter.shutdown(), timeout=1)

            release.set()
            await _settle(5)

            assert seen == ["first"]
        finally:
            release.set()
            await _settle(5)
