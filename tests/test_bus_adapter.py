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

from brilliant_mqtt.bus import RpcBusAdapter, _session_client_name
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


class _RawVariable:
    """Duck-typed stand-in for a bus Variable (normalize_peripheral contract)."""

    def __init__(self, value: str) -> None:
        self.value = value
        self.externally_settable = True


class _RawPeripheral:
    """Duck-typed stand-in for a bus Peripheral."""

    def __init__(self) -> None:
        self.name = "Mesh Switch"
        self.peripheral_type = 1
        self.variables = {"on": _RawVariable("1")}


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
