"""Synthetic mesh feedback, replay and ownership regressions (no HA core)."""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import datetime, timezone

import pytest
from jinja2 import Environment, StrictUndefined

from brilliant_mqtt import bus as bus_mod
from brilliant_mqtt.bridge import Bridge
from brilliant_mqtt.discovery import config_payload
from brilliant_mqtt.mapping import entities_for, payload_fields
from brilliant_mqtt.model import DeviceKind
from tests.fakes import (
    FakeBus,
    FakeClock,
    FakeMqtt,
    FakeSleeper,
    _RawDevice,
    _RawPeripheral,
    _RawVariable,
)
from tests.mesh_regression_support import commands, owners, pending, publishers, timers
from tests.test_bridge import (
    MESH_PID,
    MESH_SET_TOPIC,
    MESH_STATE_TOPIC,
    _BlockingStatePublishMqtt,
    _mesh_bridge,
    _mesh_bridge_parts,
    _mesh_dimmer_at,
    _mesh_feedback_payload,
    _mesh_motion_dimmer_at,
    _mesh_states,
    _StallableBus,
)
from tests.test_bus_adapter import _gated_adapter


def _feedback_config() -> dict[str, object]:
    descriptors = entities_for(_mesh_dimmer_at("1"), "mesh")
    status = [d for d in descriptors if d.value_key == "mesh_write_status"]
    assert len(status) == 1
    return dict(json.loads(config_payload(status[0])))


def _render_status(fields: dict[str, object], epoch: float = 1000.0) -> str:
    """Stub HA inputs only; stock Jinja tests/filters remain unmodified.

    value_json is a decoded JSON mapping. now() supplies an aware UTC datetime
    (HA normally uses its configured local timezone). as_timestamp(datetime)
    converts that aware instant to float Unix seconds, independent of timezone.
    These narrow stubs do not emulate HA's template environment or scheduling.
    """
    config = _feedback_config()
    env = Environment(undefined=StrictUndefined)
    # Jinja 3.1.6 treats bool as BOTH number and boolean: explicit exclusion matters.
    assert env.from_string("{{ true is number }}/{{ true is boolean }}").render() == "True/True"
    template = env.from_string(str(config["value_template"]))

    def now() -> datetime:
        return datetime.fromtimestamp(epoch, timezone.utc)

    def as_timestamp(value: datetime) -> float:
        return value.timestamp()

    return template.render(value_json=fields, now=now, as_timestamp=as_timestamp).strip()


@pytest.mark.parametrize("kind", [DeviceKind.LIGHT, DeviceKind.SWITCH])
def test_mesh_feedback_discovery_and_legacy_payload(kind: DeviceKind) -> None:
    device = replace(_mesh_dimmer_at("1"), kind=kind, peripheral_id="Example load / 1")
    entities = entities_for(device, "mesh")
    sensor = next(d for d in entities if d.value_key == "mesh_write_status")
    assert sensor.unique_id == "brilliant_mesh_Example_load___1_mesh_write_status"
    assert sensor.name == f"{device.name} Write status"
    config = json.loads(config_payload(sensor))
    assert config["entity_category"] == "diagnostic"
    assert config["enabled_by_default"] is False
    assert config["expire_after"] == 80
    assert config["json_attributes_topic"] == config["state_topic"]
    assert "command_topic" not in config
    fields = payload_fields(device)
    assert fields == _mesh_feedback_payload("ON")
    # Ignoring the additive keys yields exactly the wired/legacy payload.
    wired = replace(device, device_id="synthetic-panel")
    assert {k: v for k, v in fields.items() if not k.startswith("mesh_")} == payload_fields(wired)
    assert not any(d.value_key == "mesh_write_status" for d in entities_for(wired, "panel"))
    assert not any(
        d.value_key == "mesh_write_status"
        for d in entities_for(replace(device, kind=DeviceKind.ALWAYS_ON), "mesh")
    )


@pytest.mark.parametrize(
    ("fields", "expected"),
    [
        ({}, "unknown"),
        ({"mesh_write_status": None}, "unknown"),
        ({"mesh_write_status": "success"}, "unknown"),
        ({"mesh_write_status": True}, "unknown"),
        ({"mesh_write_status": "idle"}, "idle"),
        ({"mesh_write_status": "failed"}, "failed"),
        ({"mesh_write_status": "contradicted"}, "contradicted"),
        ({"mesh_write_status": "superseded"}, "superseded"),
        ({"mesh_write_status": "unconfirmed"}, "unconfirmed"),
        ({"mesh_write_status": "pending"}, "unknown"),
        ({"mesh_write_status": "pending", "mesh_write_deadline": None}, "unknown"),
        ({"mesh_write_status": "pending", "mesh_write_deadline": True}, "unknown"),
        ({"mesh_write_status": "pending", "mesh_write_deadline": "1080"}, "unknown"),
        ({"mesh_write_status": "pending", "mesh_write_deadline": []}, "unknown"),
        ({"mesh_write_status": "pending", "mesh_write_deadline": float("nan")}, "unknown"),
        ({"mesh_write_status": "pending", "mesh_write_deadline": float("inf")}, "unknown"),
        ({"mesh_write_status": "pending", "mesh_write_deadline": -float("inf")}, "unknown"),
        ({"mesh_write_status": "pending", "mesh_write_deadline": 1081}, "unknown"),
        ({"mesh_write_status": "pending", "mesh_write_deadline": 1080}, "pending"),
        ({"mesh_write_status": "pending", "mesh_write_deadline": 1000}, "unconfirmed"),
        ({"mesh_write_status": "pending", "mesh_write_deadline": 999}, "unconfirmed"),
    ],
)
def test_retained_feedback_template(fields: dict[str, object], expected: str) -> None:
    assert _render_status(fields) == expected


@pytest.mark.parametrize(
    ("fields", "expected"),
    [
        ({}, {"mesh_requested": {}, "mesh_write_deadline": None}),
        (
            {"mesh_requested": {"on": "0"}, "mesh_write_deadline": 1080},
            {"mesh_requested": {"on": "0"}, "mesh_write_deadline": 1080},
        ),
        (
            {"mesh_requested": None, "mesh_write_deadline": True},
            {"mesh_requested": {}, "mesh_write_deadline": None},
        ),
    ],
)
def test_feedback_attributes_template(
    fields: dict[str, object], expected: dict[str, object]
) -> None:
    config = _feedback_config()
    env = Environment(undefined=StrictUndefined)
    template = env.from_string(str(config["json_attributes_template"]))
    assert json.loads(template.render(value_json=fields)) == expected


async def test_arm_normalizes_request_and_never_renews_deadline() -> None:
    wall = FakeClock()
    wall.now = 1000.0
    clock = FakeClock()
    sleeper = FakeSleeper()
    bus = FakeBus([_mesh_dimmer_at("1")], clock=clock)
    mqtt = FakeMqtt()
    bridge = Bridge(bus, mqtt, "mesh", clock=clock, wall_clock=wall, sleep=sleeper)
    await bridge.reconcile()
    await mqtt.inject(MESH_SET_TOPIC, '{"brightness": 128}')
    first = json.loads(_mesh_states(mqtt)[-1][1])
    assert first["mesh_requested"] == {"intensity": "502"}
    assert first["mesh_write_status"] == "pending"
    assert first["mesh_write_deadline"] == 1080.0
    assert first["state"] is None
    wall.advance(-100.0)
    assert _render_status(first, wall.now) == "unknown"
    clock.advance(75.0)
    bus.set_devices([_mesh_dimmer_at("1", "502")])
    await bridge.reconcile()
    assert json.loads(_mesh_states(mqtt)[-1][1])["mesh_write_deadline"] == 1080.0
    clock.advance(5.0)
    await sleeper.release_all()
    assert sleeper.requested == [80.0]
    assert json.loads(_mesh_states(mqtt)[-1][1])["mesh_write_status"] == "idle"


@pytest.mark.parametrize("epoch", [float("nan"), float("inf"), -float("inf")])
async def test_nonfinite_wall_clock_cannot_supply_a_confirmation_deadline(epoch: float) -> None:
    clock = FakeClock()
    bus = FakeBus([_mesh_dimmer_at("1")], clock=clock)
    mqtt = FakeMqtt()
    sleeper = FakeSleeper()
    bridge = Bridge(bus, mqtt, "mesh", clock=clock, wall_clock=lambda: epoch, sleep=sleeper)
    await bridge.reconcile()
    await mqtt.inject(MESH_SET_TOPIC, '{"state":"OFF"}')
    fields = json.loads(_mesh_states(mqtt)[-1][1])
    assert fields["state"] is None
    assert fields["mesh_write_status"] == "pending"
    assert fields["mesh_write_deadline"] is None
    assert _render_status(fields) == "unknown"
    assert sleeper.requested == [80.0]
    await bridge.withdraw()


async def test_expiry_preserves_null_and_aux_until_next_observation() -> None:
    bus, mqtt, bridge, clock, sleeper = _mesh_bridge([_mesh_motion_dimmer_at("1")])
    await bridge.reconcile()
    await mqtt.inject(MESH_SET_TOPIC, '{"state":"OFF"}')
    clock.advance(80.0)
    await sleeper.release_all()
    terminal = json.loads(_mesh_states(mqtt)[-1][1])
    assert terminal["mesh_write_status"] == "unconfirmed"
    assert terminal["mesh_requested"] == {}
    assert terminal["mesh_write_deadline"] is None
    assert terminal["state"] is None
    assert terminal["motion_score"] == 42
    assert terminal["enable_motion_score"] is True
    await bus.emit(_mesh_motion_dimmer_at("1"))
    assert json.loads(_mesh_states(mqtt)[-1][1])["mesh_write_status"] == "idle"


async def test_supersession_projects_no_request_before_bus_reply() -> None:
    bus = _StallableBus([_mesh_dimmer_at("1")])
    mqtt = FakeMqtt()
    bridge, clock, sleeper = _mesh_bridge_parts(bus, mqtt)
    await bridge.reconcile()
    await mqtt.inject(MESH_SET_TOPIC, '{"state":"OFF"}')
    gate = asyncio.Event()
    bus.stall = gate
    replacement = asyncio.create_task(mqtt.inject(MESH_SET_TOPIC, '{"state":"ON"}'))
    await asyncio.sleep(0)
    # The bus has entered before ANY feedback publish/await.
    assert bus.stall is None
    assert json.loads(_mesh_states(mqtt)[-1][1])["mesh_requested"] == {"on": "0"}
    await bus.emit(_mesh_dimmer_at("1"))
    assert json.loads(_mesh_states(mqtt)[-1][1]) == _mesh_feedback_payload("ON", "superseded")
    clock.advance(80.0)
    await sleeper.release_all()
    gate.set()
    await replacement
    assert json.loads(_mesh_states(mqtt)[-1][1]) == _mesh_feedback_payload(
        None, "pending", {"on": "1"}, 1080.0
    )
    await bridge.withdraw()


@pytest.mark.parametrize("shutdown", [False, True], ids=["withdraw", "session_shutdown"])
async def test_feedback_publish_is_cancelled_and_joined(shutdown: bool) -> None:
    bus = FakeBus([_mesh_dimmer_at("1")])
    mqtt = _BlockingStatePublishMqtt()
    bridge, clock, sleeper = _mesh_bridge_parts(bus, mqtt)
    await bridge.reconcile()
    await mqtt.inject(MESH_SET_TOPIC, '{"state":"OFF"}')
    mqtt.published.clear()
    mqtt.block_next_state_publish = True
    clock.advance(80.0)
    await sleeper.release_all()
    await asyncio.wait_for(mqtt.publish_started.wait(), 1)
    blocked_publishers = list(publishers(bridge))
    assert blocked_publishers
    if shutdown:
        await bridge.shutdown_mesh_feedback()
    else:
        await bridge.withdraw()
    assert all(task.done() for task in blocked_publishers)
    mqtt.release_publish.set()
    await asyncio.sleep(0)
    assert mqtt.published == []  # no terminal clear or shared offline


async def test_shutdown_fences_sleeping_resolver_without_sweeping_it() -> None:
    _bus, mqtt, bridge, clock, sleeper = _mesh_bridge([_mesh_dimmer_at("1")])
    await bridge.reconcile()
    await mqtt.inject(MESH_SET_TOPIC, '{"state":"OFF"}')
    resolver = timers(bridge)[MESH_PID]
    await bridge.shutdown_mesh_feedback()
    mqtt.published.clear()
    clock.advance(80.0)
    await sleeper.release_all()
    assert resolver.done()
    assert mqtt.published == []
    assert timers(bridge) == {}
    assert publishers(bridge) == {}


async def test_restart_replaces_retained_pending_with_unrelated_idle() -> None:
    bus, mqtt, bridge, _clock, _sleeper = _mesh_bridge([_mesh_dimmer_at("1")])
    await bridge.reconcile()
    await mqtt.inject(MESH_SET_TOPIC, '{"state":"OFF"}')
    replay = json.loads(_mesh_states(mqtt)[-1][1])
    assert replay["mesh_write_status"] == "pending"
    assert _render_status(replay, 1100.0) == "unconfirmed"
    fresh = Bridge(FakeBus([_mesh_dimmer_at("1")]), mqtt, "mesh")
    await fresh.reconcile()
    assert json.loads(_mesh_states(mqtt)[-1][1]) == _mesh_feedback_payload("ON")
    await bridge.withdraw()


async def test_supersession_cancels_blocked_old_pending_publish() -> None:
    bus = FakeBus([_mesh_dimmer_at("1")])
    mqtt = _BlockingStatePublishMqtt()
    bridge, _clock, _sleeper = _mesh_bridge_parts(bus, mqtt)
    await bridge.reconcile()
    mqtt.published.clear()
    mqtt.block_next_state_publish = True
    old = asyncio.create_task(mqtt.inject(MESH_SET_TOPIC, '{"state":"OFF"}'))
    await asyncio.wait_for(mqtt.publish_started.wait(), 1)
    await mqtt.inject(MESH_SET_TOPIC, '{"state":"ON"}')
    mqtt.release_publish.set()
    await old
    assert [json.loads(p[1]) for p in _mesh_states(mqtt)] == [
        _mesh_feedback_payload(None, "pending", {"on": "1"}, 1080.0)
    ]
    await bridge.withdraw()


async def test_contradiction_revokes_blocked_publish_with_same_generation() -> None:
    bus = FakeBus([_mesh_dimmer_at("1")])
    mqtt = _BlockingStatePublishMqtt()
    bridge, _clock, _sleeper = _mesh_bridge_parts(bus, mqtt)
    await bridge.reconcile()
    mqtt.published.clear()
    mqtt.block_next_state_publish = True
    command = asyncio.create_task(mqtt.inject(MESH_SET_TOPIC, '{"state":"OFF"}'))
    await asyncio.wait_for(mqtt.publish_started.wait(), 1)
    generation = commands(bridge)[MESH_PID]
    blocked = list(publishers(bridge))
    await bus.emit(_mesh_dimmer_at("1"))
    assert commands(bridge)[MESH_PID] == generation
    assert not mqtt.release_publish.is_set()
    assert blocked and all(task.cancelled() for task in blocked)
    mqtt.release_publish.set()
    await command
    assert [json.loads(p[1]) for p in _mesh_states(mqtt)] == [
        _mesh_feedback_payload("ON", "contradicted")
    ]
    await bridge.withdraw()


class _DelayedPendingCompletionMqtt(FakeMqtt):
    """Record pending at the broker, then delay local publish completion."""

    def __init__(self) -> None:
        super().__init__()
        self.delay_pending = False
        self.pending_recorded = asyncio.Event()
        self.release_pending = asyncio.Event()
        self.retained_state: dict[str, object] = {}

    async def publish(self, topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        await super().publish(topic, payload, retain, qos)
        if topic == MESH_STATE_TOPIC and retain:
            self.retained_state = json.loads(payload)
            if self.delay_pending and self.retained_state["mesh_write_status"] == "pending":
                self.pending_recorded.set()
                await self.release_pending.wait()


@pytest.mark.parametrize("confirm", [True, False], ids=["confirmed", "unconfirmed"])
async def test_terminal_publish_repairs_cancelled_pending_already_at_broker(confirm: bool) -> None:
    bus = FakeBus([_mesh_dimmer_at("1")])
    mqtt = _DelayedPendingCompletionMqtt()
    bridge, clock, sleeper = _mesh_bridge_parts(bus, mqtt)
    await bridge.reconcile()
    expected = _mesh_feedback_payload("ON")
    if not confirm:
        # Cache a prior unconfirmed result, identical to the next expiry's fields.
        await mqtt.inject(MESH_SET_TOPIC, '{"state":"ON"}')
        clock.advance(80.0)
        await sleeper.release_all()
        expected = _mesh_feedback_payload(None, "unconfirmed")
    assert mqtt.retained_state == expected

    mqtt.delay_pending = True
    command = asyncio.create_task(mqtt.inject(MESH_SET_TOPIC, '{"state":"ON"}'))
    observation: asyncio.Task[None] | None = None
    try:
        await asyncio.wait_for(mqtt.pending_recorded.wait(), 1)
        assert mqtt.retained_state == _mesh_feedback_payload(None, "pending", {"on": "1"}, 1080.0)
        clock.advance(75.0)
        if confirm:
            observation = asyncio.create_task(bus.emit(_mesh_dimmer_at("1")))
            from tests.mesh_regression_support import turns

            await turns()
            assert dict(owners(bridge)[MESH_PID].view.evidence)["on"].captured_at == clock()
        assert bridge.mesh_driver is not None
        assert bridge.mesh_driver.published_fields(owners(bridge)[MESH_PID]) == expected
        published = len(_mesh_states(mqtt))
        resolver = timers(bridge)[MESH_PID]
        clock.advance(5.0)
        await sleeper.release_all()
        await resolver
        await command
        if observation is not None:
            await observation

        # No reconcile or extra observation: the resolver's next publish must repair it.
        assert mqtt.retained_state == expected
        assert len(_mesh_states(mqtt)) == published + 1
        assert all(seconds == 80.0 for seconds in sleeper.requested)
    finally:
        await bridge.withdraw()
        await asyncio.gather(command, *([observation] if observation is not None else []))


@pytest.mark.parametrize("late_error", [False, True], ids=["late_receipt", "late_error"])
@pytest.mark.parametrize("revoke", [False, True], ids=["pending", "withdrawn"])
async def test_detached_rpc_outcome_cannot_settle_or_rearm_feedback(
    monkeypatch: pytest.MonkeyPatch, late_error: bool, revoke: bool
) -> None:
    # Zero expires deterministically AFTER the real adapter acquires its lock.
    # The fake observer stays blocked on an Event; no native service is used.
    monkeypatch.setattr(bus_mod, "_WRITE_DEADLINE_S", 0.0)
    observer, adapter = _gated_adapter(
        fail_with=RuntimeError("synthetic late rejection") if late_error else None
    )
    clock, sleeper = FakeClock(), FakeSleeper()
    adapter._clock = clock
    peripheral = _RawPeripheral("1")
    peripheral.peripheral_type = 27
    peripheral.variables["intensity"] = _RawVariable("600")
    raw = _RawDevice("ble_mesh", {MESH_PID: peripheral})

    async def get_device(device_id: str) -> _RawDevice:
        return raw if device_id == "ble_mesh" else _RawDevice(device_id, {})

    monkeypatch.setattr(observer, "get_device", get_device)
    mqtt = FakeMqtt()
    bridge = Bridge(
        adapter, mqtt, "mesh", clock=clock, wall_clock=lambda: 1000 + clock(), sleep=sleeper
    )
    try:
        await bridge.reconcile()
        await mqtt.inject(MESH_SET_TOPIC, '{"state":"OFF"}')
        assert json.loads(_mesh_states(mqtt)[-1][1]) == _mesh_feedback_payload(
            None, "pending", {"on": "0"}, 1080.0
        )
        record = pending(bridge)[MESH_PID]
        old_window = record.view.window
        clock.advance(10)
        (rpc,) = adapter._write_tasks
        assert not rpc.done()
        if revoke:
            await bridge.withdraw()
        mqtt.published.clear()
        observer.release.set()
        if late_error:
            with pytest.raises(RuntimeError, match="synthetic late rejection"):
                await rpc
        else:
            assert await rpc == "'ok'"
        from tests.mesh_regression_support import turns

        await turns()
        if revoke:
            assert mqtt.published == []
            assert sleeper.requested == [80.0]
            assert MESH_PID not in pending(bridge)
            assert not timers(bridge)
        elif late_error:
            assert json.loads(_mesh_states(mqtt)[-1][1]) == _mesh_feedback_payload("ON", "failed")
            assert sleeper.requested == [80.0]
            assert not pending(bridge)
            assert not timers(bridge)
        else:
            assert pending(bridge)[MESH_PID] is record
            assert record.view.window is not old_window
            assert record.view.receipt == "'ok'"
            assert record.view.evidence == ()
            assert sleeper.requested == [80.0, 80.0]
            assert json.loads(_mesh_states(mqtt)[-1][1]) == _mesh_feedback_payload(
                None, "pending", {"on": "0"}, 1090
            )
    finally:
        await bridge.withdraw()
        await adapter.shutdown()
