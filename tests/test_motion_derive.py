"""Tests: MotionDeriver — score-derived motion for mesh loads."""

from __future__ import annotations

from brilliant_mqtt.model import BrilliantDevice, DeviceKind, Variable
from brilliant_mqtt.motion_derive import MotionDeriver
from tests.fakes import FakeClock


def _mesh_light(pid: str = "pidA", **vars_: str) -> BrilliantDevice:
    return BrilliantDevice(
        device_id="ble_mesh",
        peripheral_id=pid,
        name=pid,
        kind=DeviceKind.LIGHT,
        peripheral_type=27,
        variables={k: Variable(k, v) for k, v in vars_.items()},
    )


def _scoring(
    pid: str = "pidA", *, score: str, high: str = "50", enable: str = "1", motion: str = "0"
) -> BrilliantDevice:
    return _mesh_light(
        pid,
        movement_detected=motion,
        motion_score=score,
        enable_motion_score=enable,
        motion_high_threshold=high,
    )


def test_trip_at_threshold() -> None:
    d = MotionDeriver(60.0, clock=FakeClock())
    out = d.apply(_scoring(score="50"))
    assert out.variables["movement_detected"].value == "1"


def test_below_threshold_stays_off() -> None:
    d = MotionDeriver(60.0, clock=FakeClock())
    out = d.apply(_scoring(score="49"))
    assert out.variables["movement_detected"].value == "0"


def test_hold_keeps_motion_on() -> None:
    clock = FakeClock()
    d = MotionDeriver(60.0, clock=clock)
    d.apply(_scoring(score="200"))
    clock.advance(60.0)  # inclusive window: exactly hold_s is still on
    out = d.apply(_scoring(score="10"))
    assert out.variables["movement_detected"].value == "1"


def test_hold_expires() -> None:
    clock = FakeClock()
    d = MotionDeriver(60.0, clock=clock)
    d.apply(_scoring(score="200"))
    clock.advance(60.1)
    out = d.apply(_scoring(score="10"))
    assert out.variables["movement_detected"].value == "0"


def test_gate_off_forces_off_and_drops_hold() -> None:
    clock = FakeClock()
    d = MotionDeriver(60.0, clock=clock)
    d.apply(_scoring(score="200"))
    out = d.apply(_scoring(score="200", enable="0", motion="1"))
    assert out.variables["movement_detected"].value == "0"
    # Re-enabling with a cold score must NOT resurrect the old hold.
    out2 = d.apply(_scoring(score="10"))
    assert out2.variables["movement_detected"].value == "0"


def test_firmware_latch_value_is_overridden() -> None:
    d = MotionDeriver(60.0, clock=FakeClock())
    out = d.apply(_scoring(score="10", motion="1"))  # stale latch says on
    assert out.variables["movement_detected"].value == "0"


def test_threshold_from_current_snapshot() -> None:
    d = MotionDeriver(60.0, clock=FakeClock())
    assert d.apply(_scoring(score="45", high="50")).variables["movement_detected"].value == "0"
    assert d.apply(_scoring(score="45", high="40")).variables["movement_detected"].value == "1"


def test_unparsable_score_is_off() -> None:
    d = MotionDeriver(60.0, clock=FakeClock())
    out = d.apply(_scoring(score="garbage"))
    assert out.variables["movement_detected"].value == "0"


def test_missing_threshold_is_off() -> None:
    d = MotionDeriver(60.0, clock=FakeClock())
    dev = _mesh_light(movement_detected="0", motion_score="200", enable_motion_score="1")
    out = d.apply(dev)
    assert out.variables["movement_detected"].value == "0"


def test_non_mesh_device_passes_through_same_object() -> None:
    d = MotionDeriver(60.0, clock=FakeClock())
    dev = _mesh_light(on="1", power="3.2")  # no motion subsystem vars
    assert d.apply(dev) is dev


def test_unchanged_value_returns_same_object() -> None:
    d = MotionDeriver(60.0, clock=FakeClock())
    dev = _scoring(score="10", motion="0")
    assert d.apply(dev) is dev


def test_input_device_never_mutated_and_settable_preserved() -> None:
    d = MotionDeriver(60.0, clock=FakeClock())
    dev = _mesh_light(
        movement_detected="0",
        motion_score="200",
        enable_motion_score="1",
        motion_high_threshold="50",
    )
    dev.variables["movement_detected"] = Variable(
        "movement_detected", "0", externally_settable=True
    )
    out = d.apply(dev)
    assert dev.variables["movement_detected"].value == "0"
    assert out is not dev
    assert out.variables["movement_detected"].externally_settable is True


def test_state_is_per_peripheral() -> None:
    clock = FakeClock()
    d = MotionDeriver(60.0, clock=clock)
    d.apply(_scoring(score="200", pid="hot"))
    out_cold = d.apply(_scoring(score="10", pid="cold"))
    assert out_cold.variables["movement_detected"].value == "0"


def test_forget_and_clear_drop_hold() -> None:
    clock = FakeClock()
    d = MotionDeriver(60.0, clock=clock)
    d.apply(_scoring(score="200", pid="a"))
    d.apply(_scoring(score="200", pid="b"))
    d.forget("a")
    assert d.apply(_scoring(score="10", pid="a")).variables["movement_detected"].value == "0"
    assert d.apply(_scoring(score="10", pid="b")).variables["movement_detected"].value == "1"
    d.clear()
    assert d.apply(_scoring(score="10", pid="b")).variables["movement_detected"].value == "0"


def test_hold_zero_pulses_on_spike_tick() -> None:
    d = MotionDeriver(0.0, clock=FakeClock())
    out = d.apply(_scoring(score="200"))
    assert out.variables["movement_detected"].value == "1"
