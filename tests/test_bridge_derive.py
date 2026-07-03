"""Tests: Bridge applies MotionDeriver in every snapshot path."""

from __future__ import annotations

import json

import pytest

from brilliant_mqtt.bridge import Bridge
from brilliant_mqtt.discovery import state_topic
from brilliant_mqtt.model import BrilliantDevice, DeviceKind, Variable
from brilliant_mqtt.motion_derive import MotionDeriver
from tests.fakes import FakeBus, FakeClock, FakeMqtt


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
        on="0",
        movement_detected=motion,
        motion_score=score,
        enable_motion_score=enable,
        motion_high_threshold=high,
    )


def _last_motion(mqtt: FakeMqtt, pid: str = "pidA") -> bool:
    topic = state_topic("mesh", pid)
    payloads = [p for t, p, _ in mqtt.published if t == topic]
    assert payloads, f"no state publish on {topic}"
    field = json.loads(payloads[-1])["motion"]
    assert isinstance(field, bool)
    return field


@pytest.mark.asyncio
async def test_reconcile_publishes_derived_motion() -> None:
    dev = _scoring(score="200", motion="0")
    bus = FakeBus([dev])
    mqtt = FakeMqtt()
    deriver = MotionDeriver(60.0, clock=FakeClock())
    bridge = Bridge(bus, mqtt, "mesh", deriver=deriver)
    await bridge.reconcile()
    assert _last_motion(mqtt) is True  # firmware latch said 0; derived wins


@pytest.mark.asyncio
async def test_on_change_publishes_derived_motion() -> None:
    dev = _scoring(score="10")
    bus = FakeBus([dev])
    mqtt = FakeMqtt()
    deriver = MotionDeriver(60.0, clock=FakeClock())
    bridge = Bridge(bus, mqtt, "mesh", deriver=deriver)
    await bridge.reconcile()
    await bus.emit(_scoring(score="222"))
    assert _last_motion(mqtt) is True


@pytest.mark.asyncio
async def test_poll_tick_publishes_expiry_off_flip() -> None:
    clock = FakeClock()
    dev = _scoring(score="200")
    bus = FakeBus([dev])
    mqtt = FakeMqtt()
    deriver = MotionDeriver(60.0, clock=clock)
    bridge = Bridge(bus, mqtt, "mesh", deriver=deriver)
    await bridge.reconcile()
    assert _last_motion(mqtt) is True

    bus.set_devices([_scoring(score="10")])  # bus quiet again
    clock.advance(61.0)
    n = len(mqtt.published)
    await bridge.poll_once()
    assert _last_motion(mqtt) is False
    assert len(mqtt.published) > n  # the off-flip was actually published

    # Steady state: a second identical poll publishes nothing (diff cache).
    n2 = len(mqtt.published)
    await bridge.poll_once()
    assert len(mqtt.published) == n2


@pytest.mark.asyncio
async def test_no_deriver_passes_firmware_value_through() -> None:
    dev = _scoring(score="200", motion="0")
    bus = FakeBus([dev])
    mqtt = FakeMqtt()
    bridge = Bridge(bus, mqtt, "mesh")
    await bridge.reconcile()
    assert _last_motion(mqtt) is False  # back-compat: latch value, gated only


@pytest.mark.asyncio
async def test_withdraw_clears_hold_state() -> None:
    clock = FakeClock()
    dev = _scoring(score="200")
    bus = FakeBus([dev])
    mqtt = FakeMqtt()
    deriver = MotionDeriver(60.0, clock=clock)
    bridge = Bridge(bus, mqtt, "mesh", deriver=deriver)
    await bridge.reconcile()
    await bridge.withdraw()
    # Re-acquisition with a cold score: the old hold must not leak through.
    bus.set_devices([_scoring(score="10")])
    await bridge.reconcile()
    assert _last_motion(mqtt) is False


@pytest.mark.asyncio
async def test_panel_device_without_motion_vars_unaffected() -> None:
    dev = BrilliantDevice(
        device_id="ctrl",
        peripheral_id="lamp",
        name="lamp",
        kind=DeviceKind.LIGHT,
        peripheral_type=1,
        variables={"on": Variable("on", "1")},
    )
    bus = FakeBus([dev])
    mqtt = FakeMqtt()
    bridge = Bridge(bus, mqtt, "office", deriver=MotionDeriver(60.0, clock=FakeClock()))
    await bridge.reconcile()
    topic = state_topic("office", "lamp")
    payloads = [p for t, p, _ in mqtt.published if t == topic]
    assert payloads and "motion" not in json.loads(payloads[-1])
