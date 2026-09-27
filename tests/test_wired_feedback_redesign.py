"""Round-5 integration regressions for the reducer-owned wired path."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import replace

import pytest

from brilliant_mqtt.model import BrilliantDevice, Variable
from tests.fakes import FakeBus, FakeClock, FakeMqtt, FakeSleeper, _settle
from tests.test_wired_feedback_freshness import (
    PID,
    SET_TOPIC,
    STATE_TOPIC,
    _BlockingPublishMqtt,
    _bridged,
    _dimmer,
    _states,
    _WaitingReplacementBus,
)


async def test_a5_x1_replacement_during_expiry_keeps_native_publish_debt() -> None:
    mqtt = _BlockingPublishMqtt("/state")
    clock = FakeClock()
    sleeper = FakeSleeper()
    bus = _WaitingReplacementBus([_dimmer()])
    _, _, bridge = await _bridged(
        _dimmer(), bus=bus, mqtt=mqtt, clock=clock, wall_clock=clock, sleeper=sleeper
    )
    replacement: asyncio.Task[None] | None = None
    try:
        await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')
        mqtt.armed = True
        clock.advance(20)
        await sleeper.release_all()
        await asyncio.wait_for(mqtt.blocked.wait(), 2)
        replacement = asyncio.create_task(mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":200}'))
        await asyncio.wait_for(bus.blocked.wait(), 2)
        mqtt.release.set()
        await _settle(10)
        state = _states(mqtt)[-1]
        assert state["state"] == "OFF"
        assert state["wired_write_status"] == "unconfirmed"
    finally:
        mqtt.release.set()
        bus.release.set()
        if replacement is not None:
            await asyncio.gather(replacement, return_exceptions=True)
        await bridge.shutdown_wired_feedback()


async def test_success_while_expiry_publish_waits_starts_window_at_success() -> None:
    mqtt = _BlockingPublishMqtt("/state")
    clock = FakeClock()
    sleeper = FakeSleeper()
    _, _, bridge = await _bridged(
        _dimmer(), mqtt=mqtt, clock=clock, wall_clock=clock, sleeper=sleeper
    )
    try:
        await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')
        mqtt.armed = True
        clock.advance(20)
        await sleeper.release_all()
        await asyncio.wait_for(mqtt.blocked.wait(), 2)
        await asyncio.wait_for(mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":200}'), 2)
        await _settle(10)

        states = _states(mqtt)
        assert any(
            state["state"] == "OFF" and state["wired_write_status"] == "unconfirmed"
            for state in states
        )
        assert states[-1]["state"] == "ON"
        assert states[-1]["wired_write_status"] == "provisional"
        assert bridge._wired[PID].record.deadline_at == 40.0
    finally:
        mqtt.release.set()
        await bridge.shutdown_wired_feedback()


async def test_a5_x2_late_partial_missing_on_preserves_native_on() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer())
    partial = _dimmer(power="22")
    del partial.variables["on"]
    bus.set_devices([partial])
    old_partial = (await bus.get_all())[0]
    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')
    await bus.emit(_dimmer(on="1", on_timestamp=2000))
    await bus.emit(old_partial)

    assert _states(mqtt)[-1]["state"] == "ON"
    assert _states(mqtt)[-1]["power"] == 2.2
    assert bridge._devices[PID].variables["on"].timestamp_ms == 2000
    await bridge.shutdown_wired_feedback()


async def test_complete_read_missing_on_publishes_unknown_not_off() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer(on="1"))
    complete = _dimmer(on="1")
    del complete.variables["on"]
    bus.set_devices([complete])
    await bridge.poll_once()

    assert _states(mqtt)[-1]["state"] is None
    await bridge.shutdown_wired_feedback()


async def test_a5_x3_old_generation_cannot_restore_retired_binding() -> None:
    initial = _dimmer(on="1")
    initial.variables["max_intensity_value"] = Variable("max_intensity_value", "1000")
    bus, mqtt, bridge = await _bridged(initial)
    old = (await bus.get_all())[0]
    fresh = _dimmer(on="0")
    fresh.variables["max_intensity_value"] = Variable("max_intensity_value", "2000")
    bus.set_devices([fresh])
    bus._capture_generation += 1
    bus._capture_sequence = 0
    await bridge.reconcile_after_reconnect()
    await bridge.poll_once([old])

    assert bridge._devices[PID].max_intensity == 2000
    assert _states(mqtt)[-1]["state"] == "OFF"
    await bridge.shutdown_wired_feedback()


class _PausedReconnectReadBus(FakeBus):
    def __init__(self) -> None:
        super().__init__([_dimmer()])
        self.pause = False
        self.reading = asyncio.Event()
        self.release = asyncio.Event()

    async def get_all(self) -> list[BrilliantDevice]:
        if self.pause:
            self.reading.set()
            await self.release.wait()
        return await super().get_all()


async def test_reconnect_fence_is_advanced_before_full_read_await() -> None:
    bus = _PausedReconnectReadBus()
    _bus, mqtt, bridge = await _bridged(_dimmer(), bus=bus)
    old = (await bus.get_all())[0]
    await mqtt.inject(SET_TOPIC, '{"state":"ON"}')
    fresh = _dimmer(on="0")
    fresh.variables["max_intensity_value"] = Variable("max_intensity_value", "2000")
    bus.set_devices([fresh])
    bus._capture_generation += 1
    bus._capture_sequence = 0
    bus.pause = True
    reconnect = asyncio.create_task(bridge.reconcile_after_reconnect())
    try:
        await asyncio.wait_for(bus.reading.wait(), 2)
        await asyncio.wait_for(bus.emit(old), 2)
        bus.release.set()
        await asyncio.wait_for(reconnect, 2)
        assert bridge._devices[PID].max_intensity == 2000
        assert _states(mqtt)[-1]["state"] == "OFF"
    finally:
        bus.release.set()
        await asyncio.gather(reconnect, return_exceptions=True)
        await bridge.shutdown_wired_feedback()


async def test_rebind_during_blocked_provisional_publish_retains_new_native() -> None:
    mqtt = _BlockingPublishMqtt("/state")
    bus, _, bridge = await _bridged(_dimmer(), mqtt=mqtt)
    mqtt.armed = True
    command = asyncio.create_task(mqtt.inject(SET_TOPIC, '{"state":"ON"}'))
    try:
        await asyncio.wait_for(mqtt.blocked.wait(), 2)
        rebound = _dimmer(on="0")
        rebound.variables["max_intensity_value"] = Variable("max_intensity_value", "2000")
        await asyncio.wait_for(bus.emit(rebound), 2)
        mqtt.release.set()
        await asyncio.wait_for(command, 2)
        assert _states(mqtt)[-1]["state"] == "OFF"
        assert bridge._devices[PID].max_intensity == 2000
    finally:
        mqtt.release.set()
        await asyncio.gather(command, return_exceptions=True)
        await bridge.shutdown_wired_feedback()


class _RejectingMqtt(FakeMqtt):
    reject = False

    async def publish(self, topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        if self.reject and topic == STATE_TOPIC:
            raise RuntimeError("PRIVATE_SENTINEL " + payload)
        await super().publish(topic, payload, retain, qos)


async def test_a5_x4_failure_logs_only_event_and_exception_class(
    caplog: pytest.LogCaptureFixture,
) -> None:
    mqtt = _RejectingMqtt()
    sleeper = FakeSleeper()
    _, _, bridge = await _bridged(_dimmer(), mqtt=mqtt, sleeper=sleeper)
    with caplog.at_level(logging.INFO, logger="brilliant_mqtt.bridge"):
        await mqtt.inject(SET_TOPIC, '{"state":"ON"}')
        mqtt.reject = True
        await sleeper.release_all()
        await _settle(10)
    for record in caplog.records:
        if record.name != "brilliant_mqtt.bridge":
            continue
        assert record.exc_info is None
        assert "PRIVATE_SENTINEL" not in record.getMessage()
        assert PID not in record.getMessage()
        assert SET_TOPIC not in record.getMessage()
        assert "{" not in record.getMessage()
        assert record.getMessage() in (
            "COMMAND_ACCEPTED",
            "WIRED_PUBLISH_FAILED RuntimeError",
        )
    await bridge.shutdown_wired_feedback()


async def test_a5_c1_complete_read_revokes_dimming() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer())
    complete = _dimmer()
    del complete.variables["intensity"]
    bus.set_devices([complete])
    await bridge.poll_once()
    assert not bridge._devices[PID].is_dimmable
    await bridge.shutdown_wired_feedback()


async def test_a5_c2_first_write_era_fences_capture_after_second_issue() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer())
    old = (await bus.get_all())[0]
    await mqtt.inject(SET_TOPIC, '{"state":"ON"}')
    await bus.emit(_dimmer(on="1", on_timestamp=2000))
    await mqtt.inject(SET_TOPIC, '{"brightness":200}')
    await bus.emit(old)
    assert _states(mqtt)[-1]["state"] == "ON"
    assert bridge._devices[PID].variables["on"].timestamp_ms == 2000
    await bridge.shutdown_wired_feedback()


async def test_a5_c3_shutdown_is_terminal_for_late_command() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer())
    await bridge.shutdown_wired_feedback()
    before = len(bus.commands)
    await mqtt.inject(SET_TOPIC, '{"state":"ON"}')
    assert len(bus.commands) == before
    assert all(slot.timer is None for slot in bridge._wired.values())


async def test_a5_c4_equal_era_capture_is_not_sequence_high_water_filtered() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer())
    await mqtt.inject(SET_TOPIC, '{"state":"ON"}')
    after = (await bus.get_all())[0]
    newer = replace(after, variables=dict(after.variables))
    newer.variables["on"] = Variable("on", "1", timestamp_ms=2000)
    await bus.emit(newer)
    equal_era_older_sequence = replace(after, variables=dict(after.variables))
    await bus.emit(equal_era_older_sequence)
    assert _states(mqtt)[-1]["state"] == "OFF"
    assert bridge._devices[PID].variables["on"].timestamp_ms == 1000
    await bridge.shutdown_wired_feedback()
