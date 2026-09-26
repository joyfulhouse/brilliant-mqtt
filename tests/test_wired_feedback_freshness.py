"""Deterministic wired-primary feedback freshness regressions for issue #172."""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from typing import cast

import pytest

from brilliant_mqtt.bridge import Bridge
from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.model import BrilliantDevice, DeviceKind, Variable
from brilliant_mqtt.write_admission import (
    AdmissionTicket,
    Superseded,
    WriteCancelled,
    WriteClass,
)
from tests.fakes import FakeBus, FakeClock, FakeMqtt, FakeSleeper

PANEL = "office"
PID = "gangbox_peripheral_0"
SET_TOPIC = f"brilliant/{PANEL}/{PID}/set"
STATE_TOPIC = f"brilliant/{PANEL}/{PID}/state"
MESH_PID = "018691f1749b000701c4e689967b8e62"
MESH_SET_TOPIC = f"brilliant/mesh/{MESH_PID}/set"
MESH_STATE_TOPIC = f"brilliant/mesh/{MESH_PID}/state"


def _dimmer(
    on: str = "0",
    intensity: str = "333",
    *,
    on_timestamp: int | None = 1000,
    intensity_timestamp: int | None = 1000,
    power: str | None = None,
    power_timestamp: int | None = 1000,
) -> BrilliantDevice:
    variables = {
        "on": Variable("on", on, True, on_timestamp),
        "intensity": Variable("intensity", intensity, True, intensity_timestamp),
    }
    if power is not None:
        variables["power"] = Variable("power", power, False, power_timestamp)
    return BrilliantDevice(
        device_id="device_001",
        peripheral_id=PID,
        name="Lights",
        kind=DeviceKind.LIGHT,
        peripheral_type=27,
        variables=variables,
    )


def _mesh_dimmer() -> BrilliantDevice:
    return replace(_dimmer(), device_id="ble_mesh", peripheral_id=MESH_PID)


def _states(mqtt: FakeMqtt, topic: str = STATE_TOPIC) -> list[dict[str, object]]:
    return [json.loads(payload) for name, payload, _retain in mqtt.published if name == topic]


async def _bridged(
    device: BrilliantDevice,
    *,
    bus: FakeBus | None = None,
    mqtt: FakeMqtt | None = None,
    clock: FakeClock | None = None,
    wall_clock: FakeClock | None = None,
    sleeper: FakeSleeper | None = None,
) -> tuple[FakeBus, FakeMqtt, Bridge]:
    bus = FakeBus([device]) if bus is None else bus
    mqtt = FakeMqtt() if mqtt is None else mqtt
    bridge = Bridge(
        bus,
        mqtt,
        PANEL,
        clock=FakeClock() if clock is None else clock,
        wall_clock=FakeClock() if wall_clock is None else wall_clock,
        sleep=FakeSleeper() if sleeper is None else sleeper,
    )
    await bridge.reconcile()
    mqtt.published.clear()
    return bus, mqtt, bridge


async def _shutdown_feedback(bridge: Bridge) -> None:
    shutdown = getattr(bridge, "shutdown_wired_feedback", None)
    if shutdown is not None:
        await shutdown()


def _assert_feedback(
    payload: dict[str, object],
    status: str,
    requested: dict[str, str],
    deadline: float | None,
) -> None:
    assert payload["wired_write_status"] == status
    assert payload["wired_requested"] == requested
    assert payload["wired_write_deadline"] == deadline


# RED -> GREEN: positively pre-issue captures cannot masquerade as later state.
async def test_known_pre_command_snapshot_does_not_override_provisional_request() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer())
    captured = (await bus.get_all())[0]

    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')
    await bus.emit(captured)

    states = _states(mqtt)
    assert [state["state"] for state in states] == ["ON"]
    _assert_feedback(
        states[-1],
        "provisional",
        {"intensity": "333", "on": "1"},
        20.0,
    )
    await _shutdown_feedback(bridge)


async def test_stale_then_post_issue_observation_never_flickers_off() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer())
    captured = (await bus.get_all())[0]

    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')
    await bus.emit(captured)
    await bus.emit(_dimmer(on="1", on_timestamp=2000))

    states = _states(mqtt)
    assert "OFF" not in [state["state"] for state in states]
    assert states[-1]["wired_write_status"] == "observed"
    assert bridge._devices[PID].variables["on"].timestamp_ms == 2000
    await _shutdown_feedback(bridge)


async def test_preissue_snapshot_cannot_replace_a_resolved_field_of_active_request() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer())
    captured_before_issue = (await bus.get_all())[0]

    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":200}')
    partial = _dimmer(on="1", on_timestamp=2000)
    del partial.variables["intensity"]
    await bus.emit(partial)
    await bus.emit(captured_before_issue)

    state = _states(mqtt)[-1]
    assert state["state"] == "ON"
    assert state["brightness"] == 200
    _assert_feedback(
        state,
        "provisional",
        {"intensity": "784", "on": "1"},
        20.0,
    )

    final = _dimmer(intensity="784", intensity_timestamp=2001)
    del final.variables["on"]
    await bus.emit(final)
    state = _states(mqtt)[-1]
    assert state["state"] == "ON"
    assert state["wired_write_status"] == "observed"
    await _shutdown_feedback(bridge)


async def test_preissue_capture_cannot_restore_projection_after_native_off() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer())
    before = (await bus.get_all())[0]
    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":200}')
    await bus.emit(_dimmer(on="0", on_timestamp=2000))
    await bus.emit(before)

    state = _states(mqtt)[-1]
    assert state["state"] == "OFF"
    assert state["wired_write_status"] == "ambiguous"
    assert bridge._devices[PID].variables["on"].timestamp_ms == 2000
    await _shutdown_feedback(bridge)


async def test_preissue_capture_cannot_replace_native_after_status_clears() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer())
    before = (await bus.get_all())[0]
    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')
    await bus.emit(_dimmer(on="1", on_timestamp=2000))
    await bus.emit(_dimmer(on="1", on_timestamp=2001))
    assert "wired_write_status" not in _states(mqtt)[-1]
    await bus.emit(before)

    assert _states(mqtt)[-1]["state"] == "ON"
    assert bridge._devices[PID].variables["on"].timestamp_ms == 2001
    await _shutdown_feedback(bridge)


async def test_terminal_observed_has_no_projection_after_late_preissue_capture() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer())
    before = (await bus.get_all())[0]
    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":200}')
    partial = _dimmer(on="1", on_timestamp=2000)
    del partial.variables["intensity"]
    await bus.emit(partial)
    await bus.emit(before)
    final = _dimmer(intensity="784", intensity_timestamp=2001)
    del final.variables["on"]
    await bus.emit(final)

    record = bridge._wired_feedback[PID]
    assert record.status == "observed"
    assert not record.projected
    assert PID not in bridge._wired_deadline_tasks
    assert _states(mqtt)[-1]["state"] == "ON"
    await _shutdown_feedback(bridge)


async def test_postissue_contradiction_is_not_fenced_by_later_capture() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer())
    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":200}')
    captured_after_issue = (await bus.get_all())[0]
    await bus.emit(_dimmer(on="1", intensity="333", on_timestamp=2000))

    await bus.emit(captured_after_issue)

    states = _states(mqtt)
    await _shutdown_feedback(bridge)
    assert states[-1]["state"] == "OFF"
    assert states[-1]["wired_write_status"] == "ambiguous"


async def test_hot_poll_pre_read_snapshot_is_capture_fenced() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer())
    captured = await bus.get_all()

    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')
    await bridge.poll_once(captured)

    states = _states(mqtt)
    assert [state["state"] for state in states] == ["ON"]
    assert states[-1]["wired_write_status"] == "provisional"
    await _shutdown_feedback(bridge)


class _AdmissionWaitBus(FakeBus):
    def __init__(self, devices: list[BrilliantDevice]) -> None:
        super().__init__(devices)
        self.admitted = asyncio.Event()
        self.release_issue = asyncio.Event()

    async def set_variables(
        self,
        device_id: str,
        peripheral_id: str,
        sets: list[VarSet],
        *,
        write_class: WriteClass = WriteClass.INTERACTIVE_FIFO,
        ticket: AdmissionTicket | None = None,
    ) -> str:
        self.admitted.set()
        await self.release_issue.wait()
        if ticket is not None:
            ticket.mark_issued(self._next_provenance())
        self.commands.append((device_id, peripheral_id, list(sets)))
        return self.set_variables_receipt


async def test_capture_during_admission_wait_is_fenced_at_actual_issue() -> None:
    bus = _AdmissionWaitBus([_dimmer()])
    _bus, mqtt, bridge = await _bridged(_dimmer(), bus=bus)

    command = asyncio.create_task(mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}'))
    await bus.admitted.wait()
    captured_before_issue = (await bus.get_all())[0]
    bus.release_issue.set()
    await command
    await bus.emit(captured_before_issue)

    states = _states(mqtt)
    assert [state["state"] for state in states] == ["ON"]
    assert states[-1]["wired_write_status"] == "provisional"
    await _shutdown_feedback(bridge)


# A mirror read captured after issue is not freshness proof. Preserve its OFF,
# but label the contradiction rather than presenting it as confirmed truth.
async def test_hot_poll_frozen_mirror_publishes_observed_off_as_ambiguous() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer())

    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')
    await bridge.poll_once()

    states = _states(mqtt)
    assert [state["state"] for state in states] == ["ON", "OFF"]
    _assert_feedback(
        states[-1],
        "ambiguous",
        {"intensity": "333", "on": "1"},
        20.0,
    )
    await _shutdown_feedback(bridge)


async def test_forced_reconcile_exposes_frozen_mirror_uncertainty() -> None:
    _bus, mqtt, bridge = await _bridged(_dimmer())

    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')
    await bridge.reconcile()

    states = _states(mqtt)
    assert [state["state"] for state in states] == ["ON", "OFF"]
    assert states[-1]["wired_write_status"] == "ambiguous"
    await _shutdown_feedback(bridge)


async def test_mixed_age_fields_are_classified_per_commanded_field() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer())

    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":200}')
    await bus.emit(
        _dimmer(
            on="1",
            intensity="333",
            on_timestamp=2000,
            intensity_timestamp=1000,
        )
    )
    await bus.emit(
        _dimmer(
            on="1",
            intensity="784",
            on_timestamp=2000,
            intensity_timestamp=2001,
        )
    )

    states = _states(mqtt)
    assert [(state["state"], state["brightness"]) for state in states] == [
        ("ON", 200),
        ("ON", 85),
        ("ON", 200),
    ]
    assert states[1]["wired_write_status"] == "ambiguous"
    assert states[-1]["wired_write_status"] == "observed"
    await _shutdown_feedback(bridge)


async def test_missing_commanded_field_does_not_count_as_a_contradiction() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer(power="0"))
    partial = _dimmer(power="12", power_timestamp=2000)
    del partial.variables["on"]

    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')
    await bus.emit(partial)

    state = _states(mqtt)[-1]
    assert state["state"] == "ON"
    assert state["power"] == 1.2
    _assert_feedback(
        state,
        "provisional",
        {"intensity": "333", "on": "1"},
        20.0,
    )
    await _shutdown_feedback(bridge)


async def test_later_contradiction_keeps_partially_resolved_request_ambiguous() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer())

    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":200}')
    await bus.emit(_dimmer(on="1", intensity="333", on_timestamp=2000))
    await bus.emit(
        _dimmer(
            on="0",
            intensity="784",
            on_timestamp=2001,
            intensity_timestamp=2001,
        )
    )

    state = _states(mqtt)[-1]
    assert state["state"] == "OFF"
    assert state["brightness"] == 200
    _assert_feedback(
        state,
        "ambiguous",
        {"intensity": "784", "on": "1"},
        20.0,
    )
    await _shutdown_feedback(bridge)


async def test_brightness_only_command_fences_only_brightness() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer(on="1"))
    captured = (await bus.get_all())[0]

    await mqtt.inject(SET_TOPIC, '{"brightness":200}')
    await bus.emit(captured)

    states = _states(mqtt)
    assert [(state["state"], state["brightness"]) for state in states] == [("ON", 200)]
    assert states[-1]["wired_requested"] == {"intensity": "784"}
    await _shutdown_feedback(bridge)


async def test_unrelated_field_from_pre_command_capture_still_applies() -> None:
    original = _dimmer(power="0")
    bus, mqtt, bridge = await _bridged(original)
    bus.set_devices([_dimmer(power="12", power_timestamp=1500)])
    captured = (await bus.get_all())[0]

    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')
    await bus.emit(captured)

    states = _states(mqtt)
    assert states[-1]["state"] == "ON"
    assert states[-1]["power"] == 1.2
    assert states[-1]["wired_write_status"] == "provisional"
    await _shutdown_feedback(bridge)


class _ObservationBeforeAckBus(FakeBus):
    async def set_variables(
        self,
        device_id: str,
        peripheral_id: str,
        sets: list[VarSet],
        *,
        write_class: WriteClass = WriteClass.INTERACTIVE_FIFO,
        ticket: AdmissionTicket | None = None,
    ) -> str:
        result = await super().set_variables(
            device_id,
            peripheral_id,
            sets,
            write_class=write_class,
            ticket=ticket,
        )
        await self.emit(_dimmer(on="1", on_timestamp=2000))
        return result


async def test_echo_does_not_erase_native_observation_captured_after_issue() -> None:
    bus = _ObservationBeforeAckBus([_dimmer()])
    _bus, mqtt, bridge = await _bridged(_dimmer(), bus=bus)

    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')

    assert bridge._devices[PID].variables["on"].timestamp_ms == 2000
    assert _states(mqtt)[-1]["wired_write_status"] == "observed"
    await _shutdown_feedback(bridge)


@pytest.mark.parametrize("timestamp", [None, 1000])
async def test_missing_or_equal_timestamp_contradiction_is_explicitly_ambiguous(
    timestamp: int | None,
) -> None:
    bus, mqtt, bridge = await _bridged(_dimmer())

    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')
    await bus.emit(
        _dimmer(
            on="0",
            on_timestamp=timestamp,
            intensity_timestamp=timestamp,
        )
    )

    states = _states(mqtt)
    assert states[-1]["state"] == "OFF"
    assert states[-1]["wired_write_status"] == "ambiguous"
    await _shutdown_feedback(bridge)


@pytest.mark.parametrize("timestamp", [999, 1001])
async def test_different_timestamp_is_not_treated_as_freshness_proof(timestamp: int) -> None:
    bus, mqtt, bridge = await _bridged(_dimmer())

    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')
    await bus.emit(_dimmer(on="0", on_timestamp=timestamp))

    states = _states(mqtt)
    assert states[-1]["state"] == "OFF"
    assert states[-1]["wired_write_status"] == "ambiguous"
    await _shutdown_feedback(bridge)


async def test_provisional_deadline_expires_to_latest_native_state() -> None:
    clock = FakeClock()
    wall_clock = FakeClock()
    sleeper = FakeSleeper()
    bus, mqtt, bridge = await _bridged(
        _dimmer(),
        clock=clock,
        wall_clock=wall_clock,
        sleeper=sleeper,
    )
    captured = (await bus.get_all())[0]

    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')
    await bus.emit(captured)
    clock.advance(20.0)
    wall_clock.advance(20.0)
    await sleeper.release_all()

    states = _states(mqtt)
    assert states[-1]["state"] == "OFF"
    _assert_feedback(states[-1], "unconfirmed", {}, None)
    assert sleeper.requested == [20.0]
    await _shutdown_feedback(bridge)


class _FirstWriteBlockedBus(FakeBus):
    def __init__(self, devices: list[BrilliantDevice]) -> None:
        super().__init__(devices)
        self.first_started = asyncio.Event()
        self.release_first = asyncio.Event()
        self.calls = 0

    async def set_variables(
        self,
        device_id: str,
        peripheral_id: str,
        sets: list[VarSet],
        *,
        write_class: WriteClass = WriteClass.INTERACTIVE_FIFO,
        ticket: AdmissionTicket | None = None,
    ) -> str:
        self.calls += 1
        result = await super().set_variables(
            device_id,
            peripheral_id,
            sets,
            write_class=write_class,
            ticket=ticket,
        )
        if self.calls == 1:
            self.first_started.set()
            await self.release_first.wait()
        return result


class _BlockingPublishMqtt(FakeMqtt):
    def __init__(self, topic_suffix: str) -> None:
        super().__init__()
        self.topic_suffix = topic_suffix
        self.armed = False
        self.blocked = asyncio.Event()
        self.release = asyncio.Event()

    async def publish(
        self,
        topic: str,
        payload: str,
        retain: bool = False,
        qos: int = 0,
    ) -> None:
        if self.armed and topic.endswith(self.topic_suffix) and not self.blocked.is_set():
            self.blocked.set()
            await self.release.wait()
        await super().publish(topic, payload, retain, qos)


@pytest.mark.parametrize(
    ("old_on", "new_on", "command_state"),
    [("0", "1", "ON"), ("1", "0", "OFF")],
)
async def test_reconcile_rebuilds_wired_state_after_discovery_await(
    old_on: str,
    new_on: str,
    command_state: str,
) -> None:
    mqtt = _BlockingPublishMqtt("/config")
    bus, _mqtt, bridge = await _bridged(_dimmer(on=old_on), mqtt=mqtt)
    mqtt.armed = True

    reconcile = asyncio.create_task(bridge.reconcile())
    await mqtt.blocked.wait()
    await mqtt.inject(
        SET_TOPIC,
        json.dumps({"state": command_state, "brightness": 85}),
    )
    await bus.emit(_dimmer(on=new_on, on_timestamp=2000))
    mqtt.release.set()
    await reconcile

    state = _states(mqtt)[-1]
    assert state["state"] == command_state
    assert state["wired_write_status"] == "observed"
    await _shutdown_feedback(bridge)


@pytest.mark.parametrize("suffix", ["/availability", "/bridge"])
async def test_reconcile_early_await_does_not_apply_obsolete_capture(suffix: str) -> None:
    mqtt = _BlockingPublishMqtt(suffix)
    bus, _mqtt, bridge = await _bridged(_dimmer(), mqtt=mqtt)
    mqtt.armed = True
    reconcile = asyncio.create_task(bridge.reconcile())
    await mqtt.blocked.wait()
    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')
    await bus.emit(_dimmer(on="1", on_timestamp=2000))
    mqtt.release.set()
    await reconcile

    state = _states(mqtt)[-1]
    assert state["state"] == "ON"
    assert bridge._devices[PID].variables["on"].timestamp_ms == 2000
    await _shutdown_feedback(bridge)


async def test_obsolete_command_completion_cannot_revive_older_projection() -> None:
    bus = _FirstWriteBlockedBus([_dimmer()])
    _bus, mqtt, bridge = await _bridged(_dimmer(), bus=bus)

    first = asyncio.create_task(mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}'))
    await bus.first_started.wait()
    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":200}')
    bus.release_first.set()
    await first

    states = _states(mqtt)
    assert states[-1]["brightness"] == 200
    assert 85 not in [state["brightness"] for state in states]
    await _shutdown_feedback(bridge)


async def test_new_request_cancels_blocked_older_feedback_publish() -> None:
    mqtt = _BlockingPublishMqtt("/state")
    _bus, _mqtt, bridge = await _bridged(_dimmer(), mqtt=mqtt)
    mqtt.armed = True

    older = asyncio.create_task(mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}'))
    await mqtt.blocked.wait()
    await mqtt.inject(SET_TOPIC, '{"state":"OFF"}')
    mqtt.release.set()
    await older

    states = _states(mqtt)
    assert [state["state"] for state in states] == ["OFF"]
    _assert_feedback(states[-1], "provisional", {"on": "0"}, 20.0)
    await _shutdown_feedback(bridge)


async def test_contradiction_invalidates_blocked_provisional_publish() -> None:
    mqtt = _BlockingPublishMqtt("/state")
    bus, _mqtt, bridge = await _bridged(_dimmer(), mqtt=mqtt)
    mqtt.armed = True

    command = asyncio.create_task(mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}'))
    await mqtt.blocked.wait()
    await bus.emit(_dimmer(on="0", on_timestamp=3000))
    mqtt.release.set()
    await command

    states = _states(mqtt)
    await _shutdown_feedback(bridge)
    assert states[-1]["state"] == "OFF"
    assert states[-1]["wired_write_status"] == "ambiguous"


async def test_expiry_invalidates_blocked_provisional_publish() -> None:
    mqtt = _BlockingPublishMqtt("/state")
    sleeper = FakeSleeper()
    clock = FakeClock()
    _bus, _mqtt, bridge = await _bridged(
        _dimmer(),
        mqtt=mqtt,
        wall_clock=clock,
        sleeper=sleeper,
    )
    mqtt.armed = True

    command = asyncio.create_task(mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}'))
    await mqtt.blocked.wait()
    clock.advance(20)
    await sleeper.release_all()
    mqtt.release.set()
    await command

    states = _states(mqtt)
    await _shutdown_feedback(bridge)
    assert states[-1]["state"] == "OFF"
    assert states[-1]["wired_write_status"] == "unconfirmed"


async def test_resolution_invalidates_blocked_provisional_publish() -> None:
    mqtt = _BlockingPublishMqtt("/state")
    bus, _mqtt, bridge = await _bridged(_dimmer(), mqtt=mqtt)
    mqtt.armed = True

    command = asyncio.create_task(mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}'))
    await mqtt.blocked.wait()
    await bus.emit(_dimmer(on="1", intensity="333", on_timestamp=3000))
    mqtt.release.set()
    await command

    states = _states(mqtt)
    await _shutdown_feedback(bridge)
    assert states[-1]["state"] == "ON"
    assert states[-1]["wired_write_status"] == "observed"


async def test_preissue_native_publish_rechecks_projection_after_await() -> None:
    mqtt = _BlockingPublishMqtt("/state")
    bus, _mqtt, bridge = await _bridged(_dimmer(), mqtt=mqtt)
    mqtt.armed = True

    observation = asyncio.create_task(bus.emit(_dimmer(intensity="400")))
    await mqtt.blocked.wait()
    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')
    mqtt.release.set()
    await observation

    states = _states(mqtt)
    await _shutdown_feedback(bridge)
    assert states[-1]["state"] == "ON"
    assert states[-1]["wired_write_status"] == "provisional"


async def test_newer_plain_native_publish_revokes_blocked_older_one() -> None:
    mqtt = _BlockingPublishMqtt("/state")
    bus, _mqtt, bridge = await _bridged(_dimmer(), mqtt=mqtt)
    mqtt.armed = True

    older = asyncio.create_task(bus.emit(_dimmer(intensity="400")))
    await mqtt.blocked.wait()
    newer = asyncio.create_task(bus.emit(_dimmer(intensity="500")))
    await asyncio.sleep(0)
    mqtt.release.set()
    await older
    await newer

    states = _states(mqtt)
    assert [state["brightness"] for state in states] == [128]
    await _shutdown_feedback(bridge)


async def test_inflight_completion_cannot_revive_feedback_after_reacquisition() -> None:
    bus = _FirstWriteBlockedBus([_dimmer()])
    _bus, mqtt, bridge = await _bridged(_dimmer(), bus=bus)

    command = asyncio.create_task(mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}'))
    await bus.first_started.wait()
    await bridge.withdraw()
    await bridge.reconcile()
    mqtt.published.clear()

    bus.release_first.set()
    await command

    assert _states(mqtt) == []
    assert PID not in bridge._wired_feedback
    await _shutdown_feedback(bridge)


# Stay-GREEN regressions: physical observations, errors, writer ownership and
# mesh semantics retain their existing behavior.
@pytest.mark.parametrize("timestamp", [None, 3000])
async def test_external_off_remains_promptly_publishable(timestamp: int | None) -> None:
    bus, mqtt, bridge = await _bridged(_dimmer())

    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')
    await bus.emit(_dimmer(on="0", on_timestamp=timestamp))

    assert [state["state"] for state in _states(mqtt)] == ["ON", "OFF"]
    await _shutdown_feedback(bridge)


async def test_matching_native_observation_remains_stored() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer())

    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')
    await bus.emit(_dimmer(on="1", on_timestamp=2000))

    assert _states(mqtt)[-1]["state"] == "ON"
    assert bridge._devices[PID].variables["on"].timestamp_ms == 2000
    await _shutdown_feedback(bridge)


async def test_write_failure_still_publishes_no_success_projection() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer())
    bus.set_variables_error = RuntimeError("bus boom")

    with pytest.raises(RuntimeError, match="bus boom"):
        await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')

    assert _states(mqtt) == []
    assert bridge._devices[PID].variables["on"].value == "0"
    await _shutdown_feedback(bridge)


class _SupersededBus(FakeBus):
    async def set_variables(
        self,
        device_id: str,
        peripheral_id: str,
        sets: list[VarSet],
        *,
        write_class: WriteClass = WriteClass.INTERACTIVE_FIFO,
        ticket: AdmissionTicket | None = None,
    ) -> str:
        return cast(str, Superseded())


class _CancelledBus(FakeBus):
    async def set_variables(
        self,
        device_id: str,
        peripheral_id: str,
        sets: list[VarSet],
        *,
        write_class: WriteClass = WriteClass.INTERACTIVE_FIFO,
        ticket: AdmissionTicket | None = None,
    ) -> str:
        raise WriteCancelled


class _ReplacementOutcomeBus(FakeBus):
    def __init__(self, devices: list[BrilliantDevice], outcome: str) -> None:
        super().__init__(devices)
        self.outcome = outcome

    async def set_variables(
        self,
        device_id: str,
        peripheral_id: str,
        sets: list[VarSet],
        *,
        write_class: WriteClass = WriteClass.INTERACTIVE_FIFO,
        ticket: AdmissionTicket | None = None,
    ) -> str:
        if not self.commands:
            return await super().set_variables(
                device_id,
                peripheral_id,
                sets,
                write_class=write_class,
                ticket=ticket,
            )
        if ticket is not None:
            ticket.mark_issued(self._next_provenance())
        if self.outcome == "failed":
            raise RuntimeError("replacement failed")
        if self.outcome == "cancelled":
            raise WriteCancelled
        return cast(str, Superseded())


class _WaitingReplacementBus(FakeBus):
    def __init__(self, devices: list[BrilliantDevice]) -> None:
        super().__init__(devices)
        self.blocked = asyncio.Event()
        self.release = asyncio.Event()

    async def set_variables(
        self,
        device_id: str,
        peripheral_id: str,
        sets: list[VarSet],
        *,
        write_class: WriteClass = WriteClass.INTERACTIVE_FIFO,
        ticket: AdmissionTicket | None = None,
    ) -> str:
        if self.commands:
            self.blocked.set()
            await self.release.wait()
            raise RuntimeError("replacement failed")
        return await super().set_variables(
            device_id, peripheral_id, sets, write_class=write_class, ticket=ticket
        )


async def test_waiting_replacement_preserves_old_deadline_fallback() -> None:
    clock = FakeClock()
    sleeper = FakeSleeper()
    bus = _WaitingReplacementBus([_dimmer()])
    _bus, mqtt, bridge = await _bridged(
        _dimmer(), bus=bus, clock=clock, wall_clock=clock, sleeper=sleeper
    )
    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')
    replacement = asyncio.create_task(mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":200}'))
    await bus.blocked.wait()
    clock.advance(20)
    await sleeper.release_all()

    state = _states(mqtt)[-1]
    _assert_feedback(state, "unconfirmed", {}, None)
    assert state["state"] == "OFF"
    assert PID not in bridge._wired_deadline_tasks
    bus.release.set()
    with pytest.raises(RuntimeError, match="replacement failed"):
        await replacement
    await _shutdown_feedback(bridge)


@pytest.mark.parametrize("outcome", ["failed", "cancelled", "superseded"])
async def test_unsuccessful_replacement_retires_prior_projection_to_native(
    outcome: str,
) -> None:
    clock = FakeClock()
    sleeper = FakeSleeper()
    bus = _ReplacementOutcomeBus([_dimmer()], outcome)
    _bus, mqtt, bridge = await _bridged(
        _dimmer(),
        bus=bus,
        wall_clock=clock,
        sleeper=sleeper,
    )
    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')

    if outcome == "failed":
        with pytest.raises(RuntimeError, match="replacement failed"):
            await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":200}')
    elif outcome == "cancelled":
        with pytest.raises(asyncio.CancelledError):
            await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":200}')
    else:
        await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":200}')
    clock.advance(25.0)
    await sleeper.release_all()

    state = _states(mqtt)[-1]
    assert state["state"] == "OFF"
    assert state["brightness"] == 85
    _assert_feedback(state, "unconfirmed", {}, None)
    await _shutdown_feedback(bridge)


@pytest.mark.parametrize("bus_type", [_SupersededBus, _CancelledBus])
async def test_superseded_or_cancelled_write_still_has_no_echo(
    bus_type: type[FakeBus],
) -> None:
    bus = bus_type([_dimmer()])
    _bus, mqtt, bridge = await _bridged(_dimmer(), bus=bus)

    if bus_type is _CancelledBus:
        with pytest.raises(asyncio.CancelledError):
            await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')
    else:
        await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')

    assert _states(mqtt) == []
    await _shutdown_feedback(bridge)


async def test_latest_sequential_request_still_wins() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer())

    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')
    await mqtt.inject(SET_TOPIC, '{"state":"OFF"}')

    assert [state["state"] for state in _states(mqtt)] == ["ON", "OFF"]
    assert [sets for _device, _pid, sets in bus.commands] == [
        [VarSet("on", "1"), VarSet("intensity", "333")],
        [VarSet("on", "0")],
    ]
    await _shutdown_feedback(bridge)


async def test_owner_rebinding_retires_wired_projection() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer())
    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')
    switch = replace(_dimmer(on="0", on_timestamp=2000), kind=DeviceKind.SWITCH, peripheral_type=26)
    del switch.variables["intensity"]
    await bus.emit(switch)

    state = _states(mqtt)[-1]
    assert PID not in bridge._pending_wired
    assert PID not in bridge._wired_deadline_tasks
    assert state["state"] == "OFF"
    assert "wired_write_status" not in state
    await _shutdown_feedback(bridge)


@pytest.mark.parametrize("binding", ["owner", "scale"])
async def test_late_preissue_rebinding_keeps_new_native_off(binding: str) -> None:
    bus, mqtt, bridge = await _bridged(_dimmer())
    rebound = _dimmer(on="0")
    if binding == "owner":
        rebound.device_id = "new_owner"
    else:
        rebound.variables["max_intensity_value"] = Variable("max_intensity_value", "2000")
    bus.set_devices([rebound])
    captured_rebound = (await bus.get_all())[0]

    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')
    await bus.emit(_dimmer(on="1", on_timestamp=2000))
    await bus.emit(captured_rebound)

    stored = bridge._devices[PID]
    state = _states(mqtt)[-1]
    assert stored.variables["on"].value == "0"
    assert state["state"] == "OFF"
    assert "wired_write_status" not in state
    assert PID not in bridge._wired_issue_boundary
    assert PID not in bridge._pending_wired
    if binding == "owner":
        assert stored.device_id == "new_owner"
    else:
        assert stored.max_intensity == 2000
    await _shutdown_feedback(bridge)


async def test_successful_request_still_echoes_immediately() -> None:
    _bus, mqtt, bridge = await _bridged(_dimmer())

    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":255}')

    states = _states(mqtt)
    assert len(states) == 1
    assert states[0]["state"] == "ON"
    assert states[0]["brightness"] == 255
    await _shutdown_feedback(bridge)


async def test_wired_feedback_adds_no_reassert_or_retry_writes() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer())

    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')
    await bus.emit(_dimmer(on="0", on_timestamp=None))

    assert len(bus.commands) == 1
    await _shutdown_feedback(bridge)


async def test_new_bridge_session_still_accepts_current_mirror() -> None:
    original = _dimmer()
    _bus, mqtt, bridge = await _bridged(original)
    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')

    fresh_mqtt = FakeMqtt()
    fresh_bridge = Bridge(FakeBus([original]), fresh_mqtt, PANEL)
    await fresh_bridge.reconcile()

    assert _states(fresh_mqtt)[-1]["state"] == "OFF"
    await _shutdown_feedback(bridge)
    await _shutdown_feedback(fresh_bridge)


async def test_soft_reconnect_retires_old_request_ownership() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer())
    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')
    bus.on_reconnect(bridge.reconcile)

    await bus.fire_reconnect()

    assert _states(mqtt)[-1]["state"] == "OFF"
    assert "wired_write_status" not in _states(mqtt)[-1]
    await _shutdown_feedback(bridge)


async def test_soft_reconnect_retires_old_comparison_provenance() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer())
    partial = _dimmer()
    del partial.variables["on"]
    bus.set_devices([partial])
    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')
    bus.on_reconnect(bridge.reconcile)

    await bus.fire_reconnect()

    provenance = bridge._wired_native_provenance[PID]
    assert set(provenance) == {"intensity"}
    marker = provenance["intensity"]
    assert marker is not None
    assert marker.source_generation == 2
    await _shutdown_feedback(bridge)


async def test_reconnect_without_device_retires_wired_state() -> None:
    bus, mqtt, bridge = await _bridged(_dimmer())
    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')
    bus.on_reconnect(bridge.reconcile_after_reconnect)
    bus.set_devices([])

    await bus.fire_reconnect()

    assert PID not in bridge._pending_wired
    assert PID not in bridge._wired_native_provenance
    await bus.emit(_dimmer(on="0", on_timestamp=4000))
    assert _states(mqtt)[-1]["state"] == "OFF"
    assert "wired_write_status" not in _states(mqtt)[-1]
    await _shutdown_feedback(bridge)


class _FailingReconnectReadBus(FakeBus):
    def __init__(self, devices: list[BrilliantDevice]) -> None:
        super().__init__(devices)
        self.fail_reads = False

    async def get_all(self) -> list[BrilliantDevice]:
        if self.fail_reads:
            raise TimeoutError("reconnect read failed")
        return await super().get_all()


async def test_failed_reconnect_read_still_retires_wired_state() -> None:
    bus = _FailingReconnectReadBus([_dimmer()])
    _bus, mqtt, bridge = await _bridged(_dimmer(), bus=bus)
    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')
    bus.on_reconnect(bridge.reconcile_after_reconnect)
    bus.fail_reads = True

    await bus.fire_reconnect()

    assert PID not in bridge._pending_wired
    assert PID not in bridge._wired_native_provenance
    await bus.emit(_dimmer(on="0", on_timestamp=4000))
    assert _states(mqtt)[-1]["state"] == "OFF"
    assert "wired_write_status" not in _states(mqtt)[-1]
    await _shutdown_feedback(bridge)


async def test_reconnect_retires_preboundary_command_completion() -> None:
    bus = _FirstWriteBlockedBus([_dimmer()])
    _bus, mqtt, bridge = await _bridged(_dimmer(), bus=bus)
    bus.on_reconnect(bridge.reconcile_after_reconnect)

    command = asyncio.create_task(mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}'))
    await bus.first_started.wait()
    await bus.fire_reconnect()
    mqtt.published.clear()
    bus.release_first.set()
    await command

    assert _states(mqtt) == []
    assert PID not in bridge._wired_feedback
    await _shutdown_feedback(bridge)


async def test_mesh_primary_feedback_is_unchanged() -> None:
    bus = FakeBus([_mesh_dimmer()])
    mqtt = FakeMqtt()
    clock = FakeClock()
    sleeper = FakeSleeper()
    bridge = Bridge(
        bus,
        mqtt,
        "mesh",
        include=lambda device_id: device_id == "ble_mesh",
        clock=clock,
        wall_clock=lambda: 1000.0,
        sleep=sleeper,
    )
    await bridge.reconcile()
    mqtt.published.clear()

    await mqtt.inject(MESH_SET_TOPIC, '{"state":"ON","brightness":85}')
    pending = _states(mqtt, MESH_STATE_TOPIC)[-1]
    await bus.emit(_mesh_dimmer())
    contradicted = _states(mqtt, MESH_STATE_TOPIC)[-1]

    assert pending == {
        "brightness": 85,
        "mesh_requested": {"intensity": "333", "on": "1"},
        "mesh_write_deadline": 1080.0,
        "mesh_write_status": "pending",
        "state": None,
    }
    assert contradicted == {
        "brightness": 85,
        "mesh_requested": {},
        "mesh_write_deadline": None,
        "mesh_write_status": "contradicted",
        "state": "OFF",
    }
    await bridge.withdraw()


async def test_mesh_aux_echo_is_unchanged() -> None:
    device = _mesh_dimmer()
    device.variables.update(
        {
            "movement_detected": Variable("movement_detected", "1", True, 1000),
            "motion_score": Variable("motion_score", "42", True, 1000),
            "enable_motion_score": Variable("enable_motion_score", "0", True, 1000),
        }
    )
    bus = FakeBus([device])
    mqtt = FakeMqtt()
    bridge = Bridge(bus, mqtt, "mesh", include=lambda device_id: device_id == "ble_mesh")
    await bridge.reconcile()
    mqtt.published.clear()

    await mqtt.inject(f"brilliant/mesh/{MESH_PID}/set_enable_motion_score", "ON")

    state = _states(mqtt, MESH_STATE_TOPIC)[-1]
    assert state["enable_motion_score"] is True
    assert state["state"] == "OFF"
    assert "wired_write_status" not in state
    assert bridge._devices[MESH_PID].variables["enable_motion_score"].timestamp_ms is None
