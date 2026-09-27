"""Adversarial release-gate interleavings for wired feedback."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import pytest

from brilliant_mqtt.bridge import Bridge
from brilliant_mqtt.bus import RpcBusAdapter
from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.model import BrilliantDevice, Variable
from brilliant_mqtt.mqttio import _InboundMessage, _TopicDispatcher
from brilliant_mqtt.retained_topics import RetainedTopicLedger
from tests.fakes import FakeBus, FakeClock, FakeMqtt, FakeSleeper, _settle
from tests.test_wired_feedback_adapter_capture import (
    DEVICE_ID,
    _Observer,
    _RawDevice,
    _RawPeripheral,
)
from tests.test_wired_feedback_freshness import (
    PANEL,
    PID,
    SET_TOPIC,
    STATE_TOPIC,
    _BlockingPublishMqtt,
    _bridged,
    _dimmer,
    _states,
)
from tests.test_wired_feedback_redesign import _PausedReconnectReadBus


async def test_concurrent_pushes_keep_one_publisher_and_both_callers() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer(on="0"))
    try:
        results = await asyncio.wait_for(
            asyncio.gather(
                bus.emit(_dimmer(on="1", on_timestamp=2000)),
                bus.emit(_dimmer(on="0", power="22", on_timestamp=3000)),
                return_exceptions=True,
            ),
            2,
        )
        assert all(result is None for result in results)
        assert _states(mqtt)[-1]["power"] == 2.2
    finally:
        await bridge.shutdown_wired_feedback()


async def test_hot_poll_and_push_do_not_cancel_each_other() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer(on="0"))
    try:
        results = await asyncio.wait_for(
            asyncio.gather(
                bridge.poll_once([bus._captured(_dimmer(on="1"), complete=True)]),
                bus.emit(_dimmer(on="0", power="22")),
                return_exceptions=True,
            ),
            2,
        )
        assert all(result is None for result in results)
        assert _states(mqtt)[-1]["power"] == 2.2
    finally:
        await bridge.shutdown_wired_feedback()


async def test_shutdown_of_blocked_publisher_does_not_cancel_command_caller() -> None:
    mqtt = _BlockingPublishMqtt("/state")
    _, _, bridge = await _bridged(_dimmer(), mqtt=mqtt)
    mqtt.armed = True
    command = asyncio.create_task(mqtt.inject(SET_TOPIC, '{"state":"ON"}'))
    try:
        await asyncio.wait_for(mqtt.blocked.wait(), 2)
        await bridge.shutdown_wired_feedback()
        await asyncio.wait_for(command, 2)
        assert command.exception() is None
    finally:
        mqtt.release.set()
        await asyncio.gather(command, return_exceptions=True)
        await bridge.shutdown_wired_feedback()


async def test_command_lane_survives_concurrent_wired_push() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer(on="0"))
    dispatcher = _TopicDispatcher(
        lambda message: bridge._on_command(message.topic, message.payload)
    )
    try:
        await dispatcher.dispatch(
            _InboundMessage(SET_TOPIC, '{"state":"ON"}', False, (), ()), latest_wins=True
        )
        push = asyncio.create_task(bus.emit(_dimmer(on="0", power="5")))
        await _settle(30)
        await asyncio.wait_for(push, 2)
        await dispatcher.dispatch(
            _InboundMessage(SET_TOPIC, '{"state":"OFF"}', False, (), ()), latest_wins=True
        )
        await _settle(30)
        assert len(bus.commands) == 2
        assert not dispatcher._closed_lanes
    finally:
        await dispatcher.shutdown()
        await bridge.shutdown_wired_feedback()


class _QuietObserver(_Observer):
    async def shutdown(self) -> None:
        pass


async def _adapter_bridge(observer: _Observer) -> tuple[RpcBusAdapter, FakeMqtt, Bridge]:
    adapter = RpcBusAdapter()
    adapter._obs = observer
    adapter._own_device_id = DEVICE_ID
    adapter._advance_capture_generation()
    mqtt = FakeMqtt()
    bridge = Bridge(
        adapter, mqtt, "office", clock=FakeClock(), wall_clock=FakeClock(), sleep=FakeSleeper()
    )
    adapter.on_reconnect(bridge.reconcile_after_reconnect)
    await bridge.reconcile()
    mqtt.published.clear()
    return adapter, mqtt, bridge


async def test_push_during_resubscribe_does_not_overtake_adapter_generation() -> None:
    observer = _QuietObserver(_RawDevice({PID: _RawPeripheral("Lights", "1", 1000)}))
    adapter, mqtt, bridge = await _adapter_bridge(observer)
    entered, release = asyncio.Event(), asyncio.Event()

    async def resubscribe() -> None:
        entered.set()
        await release.wait()

    adapter._resubscribe = resubscribe
    try:
        adapter._on_proc_reconnect()
        await asyncio.wait_for(entered.wait(), 2)
        assert adapter.capture_generation == 2
        observer.mirror = _RawDevice({PID: _RawPeripheral("Lights", "0", 2000)})
        adapter._dispatch_raw_device(observer.mirror)
        await _settle(15)
        release.set()
        await asyncio.wait_for(asyncio.gather(*list(adapter._pending_tasks)), 2)
        observer.mirror = _RawDevice({PID: _RawPeripheral("Lights", "1", 3000)})
        await asyncio.wait_for(bridge.poll_once(), 2)
        assert bridge._wired_source_generation == adapter._capture_generation
        assert _states(mqtt)[-1]["state"] == "ON"
    finally:
        release.set()
        await bridge.shutdown_wired_feedback()
        await adapter.shutdown()


class _ReadAcrossReconnect(_QuietObserver):
    def __init__(self, mirror: _RawDevice) -> None:
        super().__init__(mirror)
        self.arm = False
        self.blocked = asyncio.Event()
        self.release = asyncio.Event()

    async def get_device(self, device_id: str) -> _RawDevice:
        snapshot = self.mirror
        if self.arm:
            self.arm = False
            self.blocked.set()
            await self.release.wait()
        return snapshot


class _ScopedReadAcrossReconnect(_QuietObserver):
    def __init__(self, mirror: _RawDevice) -> None:
        super().__init__(mirror)
        self.blocked = asyncio.Event()
        self.release = asyncio.Event()

    async def get_peripheral(self, device_id: str, peripheral_id: str) -> _RawPeripheral:
        assert device_id == DEVICE_ID
        snapshot = self.mirror.peripherals[peripheral_id]
        self.blocked.set()
        await self.release.wait()
        return snapshot


class _ReadAfterIssue(_QuietObserver):
    def __init__(self, mirror: _RawDevice) -> None:
        super().__init__(mirror)
        self.arm = False
        self.blocked = asyncio.Event()
        self.release = asyncio.Event()

    async def get_device(self, device_id: str) -> _RawDevice:
        if self.arm:
            self.arm = False
            self.blocked.set()
            await self.release.wait()
        return self.mirror

    async def get_peripheral(self, device_id: str, peripheral_id: str) -> _RawPeripheral:
        device = await self.get_device(device_id)
        return device.peripherals[peripheral_id]


@pytest.mark.parametrize("scope", ["all", "peripheral"])
async def test_read_captured_after_issue_does_not_mask_genuine_off(scope: str) -> None:
    observer = _ReadAfterIssue(_RawDevice({PID: _RawPeripheral("Lights", "0", 1000)}))
    adapter, mqtt, bridge = await _adapter_bridge(observer)
    read: asyncio.Task[list[BrilliantDevice] | BrilliantDevice | None] | None = None
    try:
        observer.arm = True
        if scope == "all":
            read = asyncio.create_task(adapter.get_all())
        else:
            read = asyncio.create_task(adapter.get_peripheral(DEVICE_ID, PID))
        await asyncio.wait_for(observer.blocked.wait(), 2)
        await mqtt.inject(SET_TOPIC, '{"state":"ON"}')
        observer.mirror = _RawDevice({PID: _RawPeripheral("Lights", "0", 2000)})
        observer.release.set()
        result = await asyncio.wait_for(read, 2)
        captures = result if isinstance(result, list) else [result] if result is not None else []
        assert captures
        await bridge.poll_once(captures)
        assert _states(mqtt)[-1]["state"] == "OFF"
        assert _states(mqtt)[-1]["wired_write_status"] == "ambiguous"
    finally:
        observer.release.set()
        if read is not None:
            await asyncio.gather(read, return_exceptions=True)
        await bridge.shutdown_wired_feedback()
        await adapter.shutdown()


async def test_scoped_read_started_before_reconnect_keeps_old_generation() -> None:
    observer = _ScopedReadAcrossReconnect(_RawDevice({PID: _RawPeripheral("Lights", "1", 1000)}))
    adapter = RpcBusAdapter()
    adapter._obs = observer
    adapter._own_device_id = DEVICE_ID
    adapter._advance_capture_generation()
    task = asyncio.create_task(adapter.get_peripheral(DEVICE_ID, PID))
    try:
        await asyncio.wait_for(observer.blocked.wait(), 2)
        adapter._on_proc_reconnect()
        observer.release.set()
        stale = await asyncio.wait_for(task, 2)
        assert stale is not None and stale.capture_provenance is not None
        assert stale.capture_provenance.source_generation == 1
        assert adapter.capture_generation == 2
    finally:
        observer.release.set()
        await asyncio.gather(task, return_exceptions=True)
        await adapter.shutdown()


async def test_read_started_before_reconnect_keeps_old_generation() -> None:
    observer = _ReadAcrossReconnect(_RawDevice({PID: _RawPeripheral("Lights", "1", 1000)}))
    adapter, mqtt, bridge = await _adapter_bridge(observer)
    task: asyncio.Task[list[BrilliantDevice]] | None = None
    try:
        observer.arm = True
        task = asyncio.create_task(adapter.get_all())
        await asyncio.wait_for(observer.blocked.wait(), 2)
        observer.mirror = _RawDevice({PID: _RawPeripheral("Lights", "0", 2000)})
        adapter._on_proc_reconnect()
        await asyncio.wait_for(asyncio.gather(*list(adapter._pending_tasks)), 2)
        observer.release.set()
        stale = await asyncio.wait_for(task, 2)
        await bridge.poll_once(stale)
        assert _states(mqtt)[-1]["state"] == "OFF"
    finally:
        observer.release.set()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        await bridge.shutdown_wired_feedback()
        await adapter.shutdown()


async def test_real_adapter_full_omission_after_write_is_unknown() -> None:
    observer = _QuietObserver(_RawDevice({PID: _RawPeripheral("Lights", "0", 1000)}))
    adapter, mqtt, bridge = await _adapter_bridge(observer)
    try:
        await adapter.set_variables(DEVICE_ID, PID, [VarSet("on", "1")])
        observer.mirror = _RawDevice({PID: _RawPeripheral("Lights", "1", 1500)})
        await bridge.poll_once()
        missing = _RawPeripheral("Lights", "0", 2000)
        del missing.variables["on"]
        observer.mirror = _RawDevice({PID: missing})
        await bridge.poll_once()
        assert _states(mqtt)[-1]["state"] is None
    finally:
        await bridge.shutdown_wired_feedback()
        await adapter.shutdown()


class _AcceptedThenInterruptedMqtt(FakeMqtt):
    def __init__(self, *, fail: bool) -> None:
        super().__init__()
        self.fail = fail
        self.arm = False
        self.accepted = asyncio.Event()
        self.release = asyncio.Event()

    async def publish(self, topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        await super().publish(topic, payload, retain, qos)
        if self.arm and topic.endswith("/state"):
            self.arm = False
            self.accepted.set()
            if self.fail:
                raise RuntimeError("synthetic publish interruption")
            await self.release.wait()


@pytest.mark.parametrize("fail", [False, True])
async def test_interrupted_accepted_publish_must_not_hide_genuine_off(fail: bool) -> None:
    mqtt = _AcceptedThenInterruptedMqtt(fail=fail)
    bus, _, bridge = await _bridged(_dimmer(on="0"), mqtt=mqtt)
    mqtt.arm = True
    first = asyncio.create_task(bus.emit(_dimmer(on="1")))
    try:
        await asyncio.wait_for(mqtt.accepted.wait(), 2)
        if fail:
            await asyncio.wait_for(first, 2)
        await asyncio.wait_for(bus.emit(_dimmer(on="0")), 2)
        await asyncio.wait_for(first, 2)
        await bridge.poll_once()
        assert _states(mqtt)[-1]["state"] == "OFF"
    finally:
        mqtt.release.set()
        await asyncio.gather(first, return_exceptions=True)
        await bridge.shutdown_wired_feedback()


class _SlowStateMqtt(FakeMqtt):
    def __init__(self) -> None:
        super().__init__()
        self.delay = 0.0
        self.started = 0

    async def publish(self, topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        if topic == STATE_TOPIC and self.delay:
            self.started += 1
            await asyncio.sleep(self.delay)
        await super().publish(topic, payload, retain, qos)


async def _poll_while_publish_pending(bridge: Bridge, stop: asyncio.Event) -> None:
    while not stop.is_set():
        await bridge.poll_once()
        await asyncio.sleep(0.02)


@pytest.mark.parametrize("owner", ["push", "command"])
async def test_identical_hot_polls_do_not_livelock_slow_publish(owner: str) -> None:
    mqtt = _SlowStateMqtt()
    bus, _, bridge = await _bridged(_dimmer(power="10"), mqtt=mqtt)
    truth = _dimmer(power="99") if owner == "push" else _dimmer(on="1", power="10")
    bus.set_devices([truth])
    mqtt.delay = 0.03
    if owner == "push":
        caller = asyncio.create_task(bus.emit(truth))
    else:
        caller = asyncio.create_task(mqtt.inject(SET_TOPIC, '{"state":"ON"}'))
    stop = asyncio.Event()
    poller: asyncio.Task[None] | None = None
    try:
        await asyncio.sleep(0.005)
        poller = asyncio.create_task(_poll_while_publish_pending(bridge, stop))
        await asyncio.sleep(0.25)
        assert mqtt.started >= 1
        assert _states(mqtt)
        assert caller.done()
    finally:
        stop.set()
        if poller is not None:
            await poller
        await asyncio.wait_for(caller, 2)
        await bridge.shutdown_wired_feedback()


async def test_identical_hot_polls_do_not_livelock_retained_ledger(tmp_path: Path) -> None:
    bus = FakeBus([_dimmer(power="10")])
    mqtt = _SlowStateMqtt()
    ledger = RetainedTopicLedger(PANEL, tmp_path / "owned-topics.json")
    await ledger.async_load()
    bridge = Bridge(
        bus,
        mqtt,
        PANEL,
        owned_topics=ledger,
        clock=FakeClock(),
        wall_clock=FakeClock(),
        sleep=FakeSleeper(),
    )
    await bridge.reconcile()
    mqtt.published.clear()
    truth = _dimmer(power="99")
    bus.set_devices([truth])
    mqtt.delay = 0.03
    caller = asyncio.create_task(bus.emit(truth))
    stop = asyncio.Event()
    poller: asyncio.Task[None] | None = None
    try:
        await asyncio.sleep(0.005)
        poller = asyncio.create_task(_poll_while_publish_pending(bridge, stop))
        await asyncio.sleep(0.25)
        assert mqtt.started >= 1
        assert _states(mqtt)
        assert caller.done()
    finally:
        stop.set()
        if poller is not None:
            await poller
        await asyncio.wait_for(caller, 2)
        await bridge.shutdown_wired_feedback()


async def test_aux_echo_on_wired_light_remains_visible() -> None:
    device = _dimmer(on="1")
    device.variables["enable_motion_score"] = Variable("enable_motion_score", "0", True)
    bus, mqtt, bridge = await _bridged(device)
    try:
        await mqtt.inject(f"brilliant/office/{PID}/set_enable_motion_score", "ON")
        assert _states(mqtt)[-1]["enable_motion_score"] is True
    finally:
        await bridge.shutdown_wired_feedback()


async def test_scale_rebind_does_not_restore_fenced_old_on() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer(on="1"))
    try:
        old = _dimmer(on="1")
        old.variables["max_intensity_value"] = Variable("max_intensity_value", "2000")
        bus.set_devices([old])
        captured = (await bus.get_all())[0]
        await mqtt.inject(SET_TOPIC, '{"state":"OFF"}')
        await bus.emit(_dimmer(on="0", on_timestamp=2000))
        await bus.emit(captured)
        assert _states(mqtt)[-1]["state"] != "ON"
    finally:
        await bridge.shutdown_wired_feedback()


@pytest.mark.parametrize("missing", ["on", "intensity"])
async def test_full_omission_after_resolved_write_is_unknown(missing: str) -> None:
    bus, mqtt, bridge = await _bridged(_dimmer())
    try:
        await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')
        await bus.emit(_dimmer(on="1"))
        complete = _dimmer(on="1")
        del complete.variables[missing]
        bus.set_devices([complete])
        await bridge.poll_once()
        assert missing not in bridge._devices[PID].variables
        if missing == "on":
            assert _states(mqtt)[-1]["state"] is None
        else:
            assert "brightness" not in _states(mqtt)[-1]
    finally:
        await bridge.shutdown_wired_feedback()


async def test_reconnect_without_load_settles_native_publication_debt() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer())
    try:
        await mqtt.inject(SET_TOPIC, '{"state":"ON"}')
        bus.set_devices([])
        bus.on_reconnect(bridge.reconcile_after_reconnect)
        await bus.fire_reconnect()
        assert _states(mqtt)[-1]["state"] != "ON"
        assert _states(mqtt)[-1]["wired_write_status"] != "provisional"
    finally:
        await bridge.shutdown_wired_feedback()


async def test_shutdown_during_reconcile_read_is_terminal() -> None:
    bus = _PausedReconnectReadBus()
    _, mqtt, bridge = await _bridged(_dimmer(), bus=bus)
    bus.pause = True
    task = asyncio.create_task(bridge.reconcile())
    try:
        await asyncio.wait_for(bus.reading.wait(), 2)
        await bridge.shutdown_wired_feedback()
        bus.release.set()
        await asyncio.wait_for(task, 2)
        await mqtt.inject(SET_TOPIC, '{"state":"ON"}')
        assert not bus.commands
        assert not bridge._wired_feedback_enabled
    finally:
        bus.release.set()
        await asyncio.gather(task, return_exceptions=True)
        await bridge.shutdown_wired_feedback()


async def test_shutdown_mid_read_prevents_later_availability_publication() -> None:
    bus = _PausedReconnectReadBus()
    _, mqtt, bridge = await _bridged(_dimmer(), bus=bus)
    bus.pause = True
    reconcile = asyncio.create_task(bridge.reconcile())
    try:
        await asyncio.wait_for(bus.reading.wait(), 2)
        await bridge.shutdown_wired_feedback()
        mqtt.published.clear()
        bus.release.set()
        await asyncio.wait_for(reconcile, 2)
        assert not mqtt.published
    finally:
        bus.release.set()
        await asyncio.gather(reconcile, return_exceptions=True)
        await bridge.shutdown_wired_feedback()


async def test_cancelled_publish_caller_propagates_cancellation() -> None:
    mqtt = _SlowStateMqtt()
    bus, _, bridge = await _bridged(_dimmer(), mqtt=mqtt)
    mqtt.delay = 0.05
    caller = asyncio.create_task(bus.emit(_dimmer(power="50")))
    try:
        await asyncio.sleep(0.01)
        caller.cancel()
        result = await asyncio.gather(caller, return_exceptions=True)
        await asyncio.sleep(0.1)
        assert isinstance(result[0], asyncio.CancelledError)
        assert caller.cancelled()
        assert _states(mqtt)
    finally:
        await asyncio.gather(caller, return_exceptions=True)
        await bridge.shutdown_wired_feedback()


@pytest.mark.parametrize("topic_suffix", ["/availability", "/bridge"])
async def test_shutdown_during_reconcile_broker_await_stops_later_publications(
    topic_suffix: str,
) -> None:
    mqtt = _BlockingPublishMqtt(topic_suffix)
    _, _, bridge = await _bridged(_dimmer(), mqtt=mqtt)
    mqtt.armed = True
    reconcile = asyncio.create_task(bridge.reconcile())
    try:
        await asyncio.wait_for(mqtt.blocked.wait(), 2)
        before = len(mqtt.published)
        await bridge.shutdown_wired_feedback()
        mqtt.release.set()
        await asyncio.wait_for(reconcile, 2)
        assert len(mqtt.published) == before + 1
        assert not bridge._wired
    finally:
        mqtt.release.set()
        await asyncio.gather(reconcile, return_exceptions=True)
        await bridge.shutdown_wired_feedback()


async def test_shutdown_failure_log_is_metadata_only(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from tests.test_main import _cancel_ready_session, _hot_poll_settings, _SessionHarness

    harness = _SessionHarness(monkeypatch)

    async def failing_shutdown(_self: object) -> None:
        raise RuntimeError("PRIVATE_WIRED_SHUTDOWN_SENTINEL")

    monkeypatch.setattr(harness.bridge_type, "shutdown_wired_feedback", failing_shutdown)
    with caplog.at_level(logging.ERROR):
        await _cancel_ready_session(harness, _hot_poll_settings())
    assert "PRIVATE_WIRED_SHUTDOWN_SENTINEL" not in caplog.text
