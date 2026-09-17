"""Regression tests for two concurrency defects in interactive write scheduling.

Both were introduced by the #149/#150 interactive-write-scheduling change:

Q1 (#149): a folded (superseded) not-yet-issued interactive write that is
re-handled AFTER a mid-burst re-type (the target's EntityDescriptor route or
translation rebinds) reuses a stale ticket whose payload was translated against
the OLD snapshot. Adoption of the freshly-decoded newest value raises
``ValueError``; that error is swallowed by the reader's per-callback boundary,
and the worker's ``cancel_waiting()`` then drops the folded write — so the
newest interactive intent silently never reaches the bus.

Q2 (#150): ``WriteCancelled`` is an ``asyncio.CancelledError`` subclass (a
BaseException, DELIBERATELY, so the admission machinery can distinguish an
internal supersession from a genuine ``Task.cancel()``). When it surfaces out of
the write path to a maintenance/reconcile caller it escapes every ``except
Exception`` boundary — ``_enforce_desired``'s, the reconnect re-reconcile task's,
and, worst, the mesh-leader tick's, terminating the whole supervisor.

These tests use deterministic fakes and controlled scheduling; no live devices.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import pytest

from brilliant_mqtt.bridge import Bridge
from brilliant_mqtt.bus import RpcBusAdapter
from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.desired_state import DesiredState
from brilliant_mqtt.mesh_leader import MeshLeader
from brilliant_mqtt.model import BrilliantDevice, DeviceKind, Variable
from brilliant_mqtt.mqttio import _InboundMessage, _TopicDispatcher
from brilliant_mqtt.write_admission import (
    AdmissionTicket,
    TicketAdoptionError,
    WriteCancelled,
    WriteClass,
)
from tests.fakes import FakeBus, FakeClock, FakeMqtt
from tests.test_bus_adapter import _adapter_for, _SchedulingObserver, _settle

# --------------------------------------------------------------------------- #
# Q1: silent lost / stale interactive write on the fold -> re-handle re-type race
# --------------------------------------------------------------------------- #


def _light(max_intensity_value: str) -> BrilliantDevice:
    return BrilliantDevice(
        device_id="shared",
        peripheral_id="slider",
        name="Synthetic light",
        kind=DeviceKind.LIGHT,
        variables={
            "on": Variable("on", "1"),
            "intensity": Variable("intensity", "0"),
            "max_intensity_value": Variable("max_intensity_value", max_intensity_value),
        },
    )


async def _dispatch_handler(bridge: Bridge, message: _InboundMessage) -> None:
    # Mirror AioMqttAdapter._dispatch_inbound's per-callback error boundary
    # (mqttio.py:645-654): a command-callback exception is logged and swallowed
    # so the worker survives. This is the boundary that HIDES the #149 ValueError
    # while the folded write is silently dropped.
    try:
        await bridge._on_command(message.topic, message.payload)
    except Exception:
        logging.getLogger(__name__).exception("command callback failed; continuing")


async def test_folded_retype_reissues_newest_interactive_write() -> None:
    """A folded brightness write re-handled after a mid-burst re-type must still
    deliver the NEWEST value to the bus, not silently vanish (#149).

    Timeline (all on the ONE slider command lane, blocker holding the device
    lock so writes stay pending):
      1. A = brightness 100 reaches its admission wait (max_intensity 255).
      2. B = brightness 200 folds into A's not-yet-issued admission.
      3. The peripheral is re-typed (max_intensity 255 -> 100) BEFORE the worker
         re-handles the folded B — bumping the command generation and re-scaling
         brightness 200 to a DIFFERENT intensity (78 instead of 200).
      4. The worker re-handles B; the newest value must not be lost.
    """
    observer, adapter = _adapter_for(_SchedulingObserver())
    bridge = Bridge(adapter, FakeMqtt(), "test")
    topic = "brilliant/test/slider/set"
    bridge._devices["slider"] = _light("255")
    bridge._by_cmd_topic[topic] = ("slider", None)

    dispatcher = _TopicDispatcher(lambda message: _dispatch_handler(bridge, message))
    blocker = asyncio.create_task(adapter.set_variables("shared", "blocker", [VarSet("on", "1")]))
    try:
        await _settle(10)
        # A: first slider write reaches its admission wait.
        await dispatcher.dispatch(
            _InboundMessage(topic, '{"state":"ON","brightness":100}', False, (), ()),
            latest_wins=True,
        )
        await _settle(10)
        # B folds into A's not-yet-issued admission (generation still current).
        await dispatcher.dispatch(
            _InboundMessage(topic, '{"state":"ON","brightness":200}', False, (), ()),
            latest_wins=True,
        )
        # Re-type BEFORE the worker re-handles the folded B: max_intensity
        # 255 -> 100 bumps the command generation and re-scales brightness 200.
        bridge._remember_device(_light("100"))
        await _settle(10)
        observer.release.set()
        await blocker
        await dispatcher.shutdown()

        slider_writes = [values for _, pid, values in observer.payloads if pid == "slider"]
        # brightness 200 scaled to the NEW max_intensity 100: round(200/255*100)=78.
        assert slider_writes == [{"on": "1", "intensity": "78"}]
        assert observer.max_in_flight == 1
        assert not adapter._write_admissions
    finally:
        observer.release.set()
        await dispatcher.shutdown()
        await adapter.shutdown()
        await asyncio.gather(blocker, return_exceptions=True)


async def test_folded_retype_does_not_issue_stale_intermediate_before_newest() -> None:
    """The re-issued newest value must not be preceded by the stale folded
    intermediate (D12 / #150 retain-position ordering)."""
    observer, adapter = _adapter_for(_SchedulingObserver())
    bridge = Bridge(adapter, FakeMqtt(), "test")
    topic = "brilliant/test/slider/set"
    bridge._devices["slider"] = _light("255")
    bridge._by_cmd_topic[topic] = ("slider", None)

    dispatcher = _TopicDispatcher(lambda message: _dispatch_handler(bridge, message))
    blocker = asyncio.create_task(adapter.set_variables("shared", "blocker", [VarSet("on", "1")]))
    try:
        await _settle(10)
        await dispatcher.dispatch(
            _InboundMessage(topic, '{"state":"ON","brightness":100}', False, (), ()),
            latest_wins=True,
        )
        await _settle(10)
        await dispatcher.dispatch(
            _InboundMessage(topic, '{"state":"ON","brightness":200}', False, (), ()),
            latest_wins=True,
        )
        bridge._remember_device(_light("100"))
        await _settle(10)
        observer.release.set()
        await blocker
        await dispatcher.shutdown()

        slider_intensities = [
            values.get("intensity") for _, pid, values in observer.payloads if pid == "slider"
        ]
        # Exactly one slider write, and it is the NEWEST (78) — never the stale
        # 200 folded against the pre-re-type snapshot, and never 200-then-78.
        assert slider_intensities == ["78"]
    finally:
        observer.release.set()
        await dispatcher.shutdown()
        await adapter.shutdown()
        await asyncio.gather(blocker, return_exceptions=True)


async def _ready_turns() -> None:
    """Drain ready callbacks without sleeps or wall-clock deadlines."""
    loop = asyncio.get_running_loop()
    for _ in range(10):
        checkpoint: asyncio.Future[None] = loop.create_future()
        loop.call_soon(checkpoint.set_result, None)
        await checkpoint


async def test_already_issued_folded_retype_converges_to_newest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recovery waits for the in-flight stale fold without cancelling it."""
    observer, adapter = _adapter_for(_SchedulingObserver())
    stale_started = asyncio.Event()
    release_stale = asyncio.Event()
    original_block = observer._block

    async def block_stale_separately() -> None:
        if observer.payloads[-1][2].get("intensity") != "200":
            await original_block()
            return
        stale_started.set()
        try:
            await release_stale.wait()
        except asyncio.CancelledError:
            observer.write_cancelled = True
            raise

    monkeypatch.setattr(observer, "_block", block_stale_separately)
    bridge = Bridge(adapter, FakeMqtt(), "test")
    topic = "brilliant/test/slider/set"
    bridge._devices["slider"] = _light("255")
    bridge._by_cmd_topic[topic] = ("slider", None)
    dispatcher = _TopicDispatcher(lambda message: _dispatch_handler(bridge, message))
    blocker = asyncio.create_task(adapter.set_variables("shared", "blocker", [VarSet("on", "1")]))
    try:
        await _ready_turns()
        await dispatcher.dispatch(
            _InboundMessage(topic, '{"state":"ON","brightness":100}', False, (), ()),
            latest_wins=True,
        )
        await _ready_turns()
        stale_admission = next(
            admission
            for admission in adapter._write_admissions.values()
            if admission.peripheral_id == "slider"
        )
        # Queue the blocker release before folding wakes the lane worker.
        # The stale fold issues first and stays gated throughout recovery.
        observer.release.set()
        await dispatcher.dispatch(
            _InboundMessage(topic, '{"state":"ON","brightness":200}', False, (), ()),
            latest_wins=True,
        )
        bridge._remember_device(_light("100"))
        await blocker
        await _ready_turns()

        assert stale_started.is_set()
        assert not observer.write_cancelled
        assert stale_admission.issued
        assert stale_admission.task is not None and not stale_admission.task.done()
        # Recovery has admitted 78, but it cannot issue until 200 finishes.
        (fresh_admission,) = adapter._write_admissions.values()
        assert fresh_admission is not stale_admission
        assert fresh_admission.values == {"on": "1", "intensity": "78"}
        assert not fresh_admission.issued
        assert [values["intensity"] for _, pid, values in observer.payloads if pid == "slider"] == [
            "200"
        ]
        assert observer.in_flight == observer.max_in_flight == 1

        release_stale.set()
        await dispatcher.shutdown()

        intensities = [
            values["intensity"] for _, pid, values in observer.payloads if pid == "slider"
        ]
        assert intensities == ["200", "78"]
        assert intensities[-1] == "78"
        assert not observer.write_cancelled
        assert observer.in_flight == 0
        assert observer.max_in_flight == 1
        assert not adapter._write_admissions
    finally:
        release_stale.set()
        observer.release.set()
        await dispatcher.shutdown()
        await adapter.shutdown()
        await asyncio.gather(blocker, return_exceptions=True)


async def test_reused_ticket_native_value_error_propagates_without_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observer, adapter = _adapter_for(_SchedulingObserver())
    error = ValueError("native RPC rejected value")
    native_write = observer.request_set_variables_in_peripheral

    async def fail_slider(peripheral_id: str, values: dict[str, str], *, device_id: str) -> str:
        result = await native_write(peripheral_id, values, device_id=device_id)
        if peripheral_id == "slider":
            raise error
        return result

    monkeypatch.setattr(observer, "request_set_variables_in_peripheral", fail_slider)
    bridge = Bridge(adapter, FakeMqtt(), "test")
    topic = "brilliant/test/slider/set"
    bridge._devices["slider"] = _light("255")
    bridge._by_cmd_topic[topic] = ("slider", None)
    propagated: list[ValueError] = []

    async def handler(message: _InboundMessage) -> None:
        try:
            await bridge._on_command(message.topic, message.payload)
        except ValueError as caught:
            propagated.append(caught)

    dispatcher = _TopicDispatcher(handler)
    blocker = asyncio.create_task(adapter.set_variables("shared", "blocker", [VarSet("on", "1")]))
    try:
        await _ready_turns()
        await dispatcher.dispatch(
            _InboundMessage(topic, '{"state":"ON","brightness":100}', False, (), ()),
            latest_wins=True,
        )
        await _ready_turns()
        await dispatcher.dispatch(
            _InboundMessage(topic, '{"state":"ON","brightness":200}', False, (), ()),
            latest_wins=True,
        )
        await _ready_turns()  # The folded write adopts the ticket before native issue.
        observer.release.set()
        await blocker
        await dispatcher.shutdown()

        intensities = [
            values["intensity"] for _, pid, values in observer.payloads if pid == "slider"
        ]
        assert intensities == ["200"]
        assert propagated == [error]
        assert observer.max_in_flight == 1
        assert not adapter._write_admissions
    finally:
        observer.release.set()
        await dispatcher.shutdown()
        await adapter.shutdown()
        await asyncio.gather(blocker, return_exceptions=True)


@pytest.mark.parametrize("mismatch", ["payload", "completed", "foreign"])
async def test_ticket_adoption_errors_remain_value_errors(mismatch: str) -> None:
    observer, adapter = _adapter_for(_SchedulingObserver())
    ticket = AdmissionTicket()
    original = asyncio.create_task(
        adapter.set_variables("shared", "slider", [VarSet("intensity", "100")], ticket=ticket)
    )
    try:
        await _ready_turns()
        if mismatch == "completed":
            observer.release.set()
            await original
        elif mismatch == "foreign":
            ticket = AdmissionTicket()
            ticket._owner = object()
        with pytest.raises(ValueError) as caught:
            await adapter.set_variables(
                "shared", "slider", [VarSet("intensity", "200")], ticket=ticket
            )
        assert isinstance(caught.value, TicketAdoptionError)
    finally:
        observer.release.set()
        await asyncio.gather(original, return_exceptions=True)
        await adapter.shutdown()


# --------------------------------------------------------------------------- #
# Q2: WriteCancelled (a BaseException) escaping the maintenance/reconcile
# boundaries — enforce_desired, the reconnect re-reconcile, and the mesh-leader
# tick that terminates the whole supervisor.
# --------------------------------------------------------------------------- #


class _RaisingBus(FakeBus):
    """A bus whose ``set_variables`` always raises a configured exception.

    Drives the maintenance boundary with either the real ``WriteCancelled`` (the
    internal-supersession discriminator, a BaseException) or a plain
    ``RuntimeError`` control arm, without a live bus.
    """

    def __init__(self, error: BaseException) -> None:
        super().__init__([])
        self._error = error
        self.set_variables_calls = 0

    async def set_variables(
        self,
        device_id: str,
        peripheral_id: str,
        sets: list[VarSet],
        *,
        write_class: WriteClass = WriteClass.INTERACTIVE_FIFO,
        ticket: AdmissionTicket | None = None,
    ) -> str:
        self.set_variables_calls += 1
        raise self._error


def _drifted_bridge(bus: FakeBus, tmp_path: Path) -> tuple[Bridge, BrilliantDevice]:
    """A bridge with one drifted desired var, so _enforce_desired issues a write."""
    desired = DesiredState(tmp_path / "desired.json")
    desired.record("repair", "enable_motion_score", "1")
    bridge = Bridge(bus, FakeMqtt(), "test", desired=desired)
    device = BrilliantDevice(
        device_id="shared",
        peripheral_id="repair",
        name="Synthetic repair",
        kind=DeviceKind.LIGHT,
        variables={"enable_motion_score": Variable("enable_motion_score", "0")},
    )
    return bridge, device


@pytest.mark.parametrize("retry_interval", [60.0, 120.0])
async def test_internal_supersession_retains_desired_and_retry_interval(
    retry_interval: float, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    bus = _RaisingBus(WriteCancelled())
    bridge, device = _drifted_bridge(bus, tmp_path)
    clock = FakeClock()
    bridge._clock = clock
    bridge._devices[device.peripheral_id] = device
    key = ("repair", "enable_motion_score")
    bridge._reassert_retry_interval_s[key] = retry_interval

    with caplog.at_level(logging.DEBUG, logger="brilliant_mqtt.bridge"):
        await bridge._enforce_desired([device])

    assert bus.set_variables_calls == 1
    assert bridge._reassert_retry_interval_s[key] == retry_interval
    assert bridge._desired is not None
    assert bridge._desired.wanted("repair") == {"enable_motion_score": "1"}
    assert bridge._devices["repair"].variables["enable_motion_score"].value == "0"
    assert "superseded internally" in caplog.text
    assert all(
        record.exc_info is None and record.levelno <= logging.INFO for record in caplog.records
    )

    clock.advance(retry_interval - 1)
    await bridge._enforce_desired([device])
    assert bus.set_variables_calls == 1
    clock.advance(1)
    await bridge._enforce_desired([device])
    assert bus.set_variables_calls == 2


async def test_enforce_desired_propagates_genuine_task_cancellation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    bus = FakeBus([])
    started = asyncio.Event()
    pending = asyncio.Event()

    async def blocked_write(
        device_id: str,
        peripheral_id: str,
        sets: list[VarSet],
        *,
        write_class: WriteClass = WriteClass.INTERACTIVE_FIFO,
        ticket: AdmissionTicket | None = None,
    ) -> str:
        started.set()
        await pending.wait()
        return "ok"

    monkeypatch.setattr(bus, "set_variables", blocked_write)
    bridge, device = _drifted_bridge(bus, tmp_path)
    bridge._devices[device.peripheral_id] = device
    task = asyncio.create_task(bridge._enforce_desired([device]))
    try:
        await started.wait()
        with caplog.at_level(logging.DEBUG, logger="brilliant_mqtt.bridge"):
            task.cancel()
            with pytest.raises(asyncio.CancelledError) as caught:
                await task
        assert type(caught.value) is asyncio.CancelledError
        assert task.cancelled()
        assert not caplog.records
        assert not bridge._reassert_retry_interval_s
        assert bridge._devices["repair"].variables["enable_motion_score"].value == "0"
    finally:
        pending.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_real_enforce_desired_contains_internal_write_cancelled(
    tmp_path: Path,
) -> None:
    """End-to-end with the REAL admission machinery: an internal settlement
    surfaces WriteCancelled into _enforce_desired's DIRECT `await set_variables`
    (no Task boundary in between, so the raw BaseException — not a bare
    CancelledError — is what propagates). _enforce_desired's own `except
    Exception` must contain it. On HEAD it does NOT (WriteCancelled is a
    BaseException), so the enforce coroutine is reported cancelled."""
    observer, adapter = _adapter_for(_SchedulingObserver())
    desired = DesiredState(tmp_path / "desired.json")
    desired.record("repair", "enable_motion_score", "1")
    bridge = Bridge(adapter, FakeMqtt(), "test", desired=desired)
    device = BrilliantDevice(
        device_id="shared",
        peripheral_id="repair",
        name="Synthetic repair",
        kind=DeviceKind.LIGHT,
        variables={"enable_motion_score": Variable("enable_motion_score", "0")},
    )
    blocker = asyncio.create_task(adapter.set_variables("shared", "blocker", [VarSet("on", "1")]))
    enforce = asyncio.create_task(bridge._enforce_desired([device]))
    try:
        await _settle(10)  # the maintenance write is now waiting on the device lock
        # Internal settlement surfaces WriteCancelled into the direct await.
        await adapter._settle_writes()
        # After the fix _enforce_desired returns None; on HEAD the un-contained
        # WriteCancelled propagates and the task is reported cancelled.
        await asyncio.wait_for(enforce, timeout=2.0)
        assert enforce.cancelled() is False
    finally:
        observer.release.set()
        await adapter.shutdown()
        await asyncio.gather(blocker, enforce, return_exceptions=True)


@pytest.mark.parametrize(
    "error",
    [WriteCancelled(), RuntimeError("boom")],
    ids=["writecancelled-defect", "runtimeerror-control"],
)
async def test_enforce_desired_contains_write_path_exception(
    error: BaseException, tmp_path: Path
) -> None:
    """Path (a): a write-path exception must not escape _enforce_desired.

    Control arm: a plain RuntimeError IS already contained on HEAD; only the
    BaseException WriteCancelled escapes — isolating the BaseException nature as
    the cause.
    """
    bus = _RaisingBus(error)
    bridge, device = _drifted_bridge(bus, tmp_path)

    # Must not raise (after the fix for WriteCancelled; on both HEAD and fixed
    # for the RuntimeError control arm).
    await bridge._enforce_desired([device])
    assert bus.set_variables_calls == 1, "maintenance write path was not exercised"


async def test_reconnect_re_reconcile_survives_write_cancelled(tmp_path: Path) -> None:
    """Path (b): a WriteCancelled from a reconnect re-reconcile callback must not
    escape the real bus reconnect fan-out (bus._after_reconnect) and silently
    lose the rest of the post-reconnect re-reconcile."""
    bus = _RaisingBus(WriteCancelled())
    bridge, device = _drifted_bridge(bus, tmp_path)

    adapter = RpcBusAdapter()
    later_callback_ran = False

    async def re_reconcile() -> None:
        # Mirrors __main__._reconcile_after_bus_reconnect -> reconcile ->
        # _enforce_desired, whose maintenance write raises WriteCancelled.
        await bridge._enforce_desired([device])

    async def later_callback() -> None:
        nonlocal later_callback_ran
        later_callback_ran = True

    adapter.on_reconnect(re_reconcile)
    adapter.on_reconnect(later_callback)

    # On HEAD this raises WriteCancelled out of the fan-out and never reaches the
    # later callback; after the fix it returns and every callback runs.
    await adapter._after_reconnect()

    assert bus.set_variables_calls == 1, "reconnect re-reconcile path was not exercised"
    assert later_callback_ran, "post-reconnect re-reconcile was silently lost"


@pytest.mark.parametrize(
    "error",
    [WriteCancelled(), RuntimeError("boom")],
    ids=["writecancelled-defect", "runtimeerror-control"],
)
async def test_mesh_leader_acquire_does_not_terminate_supervisor(
    error: BaseException, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Path (c) — the worst one: on_acquire=reconcile runs on the mesh-leader
    tick in the MAIN loop. A WriteCancelled propagating out of tick() hits the
    supervisor's `except asyncio.CancelledError: raise` and terminates the WHOLE
    bridge with no backoff/reconnect. The RuntimeError control arm is contained
    by the existing `except Exception` boundaries, proving the BaseException
    nature is the cause.
    """
    bus = _RaisingBus(error)
    bridge, device = _drifted_bridge(bus, tmp_path)
    clock = FakeClock()
    acquired = False

    async def on_acquire() -> None:
        nonlocal acquired
        acquired = True
        await bridge._enforce_desired([device])

    async def on_lose() -> None:  # pragma: no cover - not exercised here
        return None

    leader = MeshLeader(
        FakeMqtt(),
        "test-panel",
        priority=1,
        heartbeat_seconds=1.0,
        on_acquire=on_acquire,
        on_lose=on_lose,
        clock=clock,
    )
    await leader.start()
    await leader.tick()  # STANDBY -> PENDING (claim published)
    clock.advance(2.0)  # a full heartbeat with nobody better objecting

    supervisor_terminated = False
    supervisor_contained = False
    # Mirror __main__.run's per-session supervisor around the main-loop tick:
    #   except asyncio.CancelledError: raise        (whole supervisor dies)
    #   except Exception: log + backoff             (contained, retried)
    try:
        await leader.tick()  # PENDING -> LEADER -> on_acquire -> _enforce_desired
    except asyncio.CancelledError:
        supervisor_terminated = True
    except Exception:
        supervisor_contained = True

    assert acquired, "on_acquire (the maintenance write path) was not exercised"
    assert bus.set_variables_calls == 1
    # After the fix: the tick returns normally, the supervisor survives, and
    # leadership is retained regardless of the internal supersession.
    assert not supervisor_terminated
    assert not supervisor_contained
    assert leader.is_leader
    if isinstance(error, WriteCancelled):
        assert not bridge._reassert_retry_interval_s
        assert not any(record.exc_info for record in caplog.records)
        assert bridge._desired is not None
        assert bridge._desired.wanted("repair") == {"enable_motion_score": "1"}
