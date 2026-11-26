"""Final design schedules across real native capture, owner and MQTT boundaries.

These supporting integration tests were added after runtime implementation;
the historical test-only RED port is recorded separately.
"""

from __future__ import annotations

import asyncio
import copy
import json
from types import SimpleNamespace

import pytest

from brilliant_mqtt import bus as bus_module
from brilliant_mqtt.discovery import command_topic
from brilliant_mqtt.model import BrilliantDevice
from tests.fakes import _RawDevice, _RawVariable
from tests.mesh_regression_support import turns
from tests.test_mesh_bridge_integration import Connected
from tests.test_mesh_bridge_integration import connected as connected
from tests.test_mesh_bus_ingress import delta


def _states(rig: Connected) -> list[dict[str, object]]:
    return [
        json.loads(payload)
        for topic, payload, _retain in rig.mqtt.published
        if topic == "brilliant/mesh/load/state"
    ]


async def _accept_off(rig: Connected) -> None:
    rig.harness.write_release.set()
    await rig.mqtt.inject(command_topic("mesh", "load"), '{"state":"OFF"}')
    assert rig.owner().view.deadline == rig.clock() + 80


async def test_native_read_from_old_acquisition_cannot_seed_reacquired_owner_or_routes(
    connected: Connected, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = connected
    registry = rig.bridge.mesh_registry
    assert registry is not None
    old = rig.owner()
    snapshot = copy.deepcopy(rig.raw)
    assert snapshot.peripherals is not None
    snapshot.peripherals["stale-only"] = snapshot.peripherals.pop("load")
    entered, release = asyncio.Event(), asyncio.Event()

    async def gated(device_id: str) -> _RawDevice:
        if device_id != "ble_mesh":
            return _RawDevice(device_id, {})
        entered.set()
        await release.wait()
        return snapshot

    monkeypatch.setattr(rig.harness.observers[0], "get_device", gated)
    read = asyncio.create_task(rig.adapter.get_all())
    try:
        await asyncio.wait_for(entered.wait(), 1)
        await rig.bridge.withdraw()
        registry.acquire()
        release.set()
        await read
        await turns()
        assert not old.active
        assert registry.owners == ()
        assert not registry.retired
        assert command_topic("mesh", "stale-only") not in rig.bridge._by_cmd_topic
        assert not any("stale-only" in topic for topic, _payload, _retain in rig.mqtt.published)
        assert "stale-only" not in rig.bridge._devices

        async def fresh(device_id: str) -> _RawDevice:
            return rig.raw if device_id == "ble_mesh" else _RawDevice(device_id, {})

        monkeypatch.setattr(rig.harness.observers[0], "get_device", fresh)
        await rig.bridge.reconcile()
        assert rig.owner() is not old
        assert rig.owner().render(rig.clock())["state"] == "ON"
        assert command_topic("mesh", "load") in rig.bridge._by_cmd_topic
    finally:
        release.set()
        await asyncio.gather(read, return_exceptions=True)


async def test_pre_expiry_native_read_and_aux_only_reconnect_cannot_release_unknown(
    connected: Connected, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = connected
    await _accept_off(rig)
    rig.clock.advance(79)
    snapshot = copy.deepcopy(rig.raw)
    entered, release = asyncio.Event(), asyncio.Event()

    async def gated(device_id: str) -> _RawDevice:
        if device_id != "ble_mesh":
            return _RawDevice(device_id, {})
        entered.set()
        await release.wait()
        return snapshot

    monkeypatch.setattr(rig.harness.observers[0], "get_device", gated)
    read = asyncio.create_task(rig.adapter.get_all())
    try:
        await asyncio.wait_for(entered.wait(), 1)
        rig.clock.advance(1)
        await rig.sleeper.release_all()
        await turns()
        assert rig.owner().view.status == "unconfirmed"
        release.set()
        await read
        assert rig.owner().render(rig.clock())["state"] is None
        rig.harness.procs[0].reconnect_cbs[0]()
        await rig.notify(power="123")
        assert rig.owner().render(rig.clock())["state"] is None
        assert rig.owner().render(rig.clock())["power"] == 12.3
        await rig.notify(on="1")
        assert rig.owner().render(rig.clock())["state"] == "ON"
    finally:
        release.set()
        await asyncio.gather(read, return_exceptions=True)


async def test_stale_high_motion_native_read_between_low_pushes_cannot_refresh_history(
    connected: Connected, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = connected
    registry = rig.bridge.mesh_registry
    assert registry is not None
    registry._derive_motion = True
    # Reacquire so the production registry creates the owner with derivation on.
    await rig.bridge.withdraw()
    assert rig.raw.peripherals is not None
    variables = rig.raw.peripherals["load"].variables
    variables.update(
        {
            "movement_detected": _RawVariable("0"),
            "enable_motion_score": _RawVariable("1"),
            "motion_high_threshold": _RawVariable("45"),
            "motion_score": _RawVariable("0"),
        }
    )
    await rig.bridge.reconcile()
    assert rig.owner().render(rig.clock())["motion"] is False
    variables["motion_score"].value = "255"
    snapshot = copy.deepcopy(rig.raw)
    entered, release = asyncio.Event(), asyncio.Event()

    async def gated(device_id: str) -> _RawDevice:
        if device_id != "ble_mesh":
            return _RawDevice(device_id, {})
        entered.set()
        await release.wait()
        return snapshot

    monkeypatch.setattr(rig.harness.observers[0], "get_device", gated)
    read = asyncio.create_task(rig.adapter.get_all())
    try:
        await asyncio.wait_for(entered.wait(), 1)
        rig.clock.advance(1)
        await rig.notify(motion_score="0")
        release.set()
        await read
        assert rig.owner().view.last_hot is None
        rig.clock.advance(1)
        await rig.notify(motion_score="0")
        assert rig.owner().view.last_hot is None
        assert rig.owner().render(rig.clock())["motion"] is False
    finally:
        release.set()
        await asyncio.gather(read, return_exceptions=True)


@pytest.mark.parametrize("shape", ["none", "empty", "partial", "exception", "scoped_absent"])
@pytest.mark.parametrize("newer_target", [False, True], ids=["unshadowed", "newer-target"])
async def test_native_negative_read_shapes_respect_per_field_evidence_and_topology(
    connected: Connected,
    monkeypatch: pytest.MonkeyPatch,
    shape: str,
    newer_target: bool,
) -> None:
    rig = connected
    await _accept_off(rig)
    rig.clock.advance(70)
    await rig.notify(on="0")
    owner = rig.owner()
    entered, release = asyncio.Event(), asyncio.Event()

    async def negative(device_id: str) -> _RawDevice | None:
        if device_id != "ble_mesh":
            return _RawDevice(device_id, {})
        entered.set()
        await release.wait()
        if shape == "exception":
            raise RuntimeError("native read failure")
        if shape == "none":
            return None
        return _RawDevice(device_id, None if shape == "partial" else {})

    async def absent(device_id: str, peripheral_id: str) -> None:
        assert device_id == "ble_mesh" and peripheral_id == "load"
        entered.set()
        await release.wait()

    observer = rig.harness.observers[0]
    monkeypatch.setattr(observer, "get_device", negative)
    monkeypatch.setattr(observer, "get_peripheral", absent, raising=False)
    read = asyncio.create_task(
        rig.adapter.get_peripheral("ble_mesh", "load")
        if shape == "scoped_absent"
        else rig.adapter.get_all()
    )
    try:
        await asyncio.wait_for(entered.wait(), 1)
        rig.clock.advance(5)
        await rig.notify(**({"on": "0"} if newer_target else {"power": "123"}))
        release.set()
        if shape == "exception":
            with pytest.raises(RuntimeError, match="native read failure"):
                await read
        else:
            await read
        # The newer presence keeps the binding, but only its explicit on report
        # can shadow older on absence; an exception contributes no absence.
        assert rig.owner() is owner
        rig.clock.advance(5)
        await rig.sleeper.release_all()
        await turns()
        confirmed = newer_target or shape == "exception"
        assert owner.view.status == ("idle" if confirmed else "unconfirmed")
        assert owner.render(rig.clock())["state"] == ("OFF" if confirmed else None)
        assert _states(rig)[-1]["state"] == ("OFF" if confirmed else None)
    finally:
        release.set()
        await asyncio.gather(read, return_exceptions=True)


@pytest.mark.parametrize("terminal", [False, True], ids=["pending", "expired-terminal"])
async def test_late_native_acceptance_revokes_blocked_old_window_before_handoff(
    connected: Connected, monkeypatch: pytest.MonkeyPatch, terminal: bool
) -> None:
    rig = connected
    monkeypatch.setattr(bus_module, "_WRITE_DEADLINE_S", 0)
    entered, release, cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original = rig.mqtt.publish
    attempts: list[dict[str, object]] = []

    async def publish(topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        if topic == "brilliant/mesh/load/state":
            fields: dict[str, object] = json.loads(payload)
            attempts.append(fields)
            if fields["mesh_write_status"] == ("unconfirmed" if terminal else "pending"):
                if not entered.is_set():
                    entered.set()
                    try:
                        await release.wait()
                    except asyncio.CancelledError:
                        cancelled.set()
                        raise
        await original(topic, payload, retain, qos)

    monkeypatch.setattr(rig.mqtt, "publish", publish)
    rig.mqtt.published.clear()
    command = asyncio.create_task(rig.mqtt.inject(command_topic("mesh", "load"), '{"state":"OFF"}'))
    try:
        await turns()
        ticket = rig.adapter.outstanding_mesh_ticket("load")
        assert ticket is not None and not ticket.settled
        if terminal:
            await command
            rig.clock.advance(80)
            await rig.sleeper.release_all()
        await asyncio.wait_for(entered.wait(), 1)
        old_window = rig.owner().view.window
        rig.clock.advance(7)
        rig.harness.write_release.set()
        await turns()
        assert ticket.settled
        assert cancelled.is_set()
        assert not release.is_set()
        owner = rig.owner()
        assert owner.view.status == "pending"
        assert owner.view.deadline == rig.clock() + 80
        assert owner.view.window is not old_window
        release.set()
        await command
        await turns()
        wire = _states(rig)
        assert wire[-1]["mesh_write_status"] == "pending"
        assert wire[-1]["mesh_write_deadline"] == 1000 + rig.clock() + 80
        assert all(fields["mesh_write_status"] == "pending" for fields in wire)
        assert len([fields for fields in attempts if fields["mesh_write_status"] == "pending"]) == 2
        assert owner.view.ordinary_debt is None
        assert owner.view.terminal is None
    finally:
        release.set()
        rig.harness.write_release.set()
        await asyncio.gather(command, return_exceptions=True)


async def test_successful_native_idle_expiry_revokes_blocked_pending_before_handoff(
    connected: Connected, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = connected
    entered, release, cancelled = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original = rig.mqtt.publish

    async def publish(topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        if (
            topic == "brilliant/mesh/load/state"
            and json.loads(payload)["mesh_write_status"] == "pending"
        ):
            entered.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise
        await original(topic, payload, retain, qos)

    monkeypatch.setattr(rig.mqtt, "publish", publish)
    rig.harness.write_release.set()
    rig.mqtt.published.clear()
    command = asyncio.create_task(rig.mqtt.inject(command_topic("mesh", "load"), '{"state":"OFF"}'))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        rig.clock.advance(75)
        await rig.notify(on="0")
        rig.clock.advance(5)
        await rig.sleeper.release_all()
        await turns()
        assert cancelled.is_set()
        assert not release.is_set()
        assert rig.owner().view.status == "idle"
        release.set()
        await command
        await turns()
        assert _states(rig) == [
            fields for fields in _states(rig) if fields["mesh_write_status"] == "idle"
        ]
        assert _states(rig)[-1]["state"] == "OFF"
    finally:
        release.set()
        rig.harness.write_release.set()
        await asyncio.gather(command, return_exceptions=True)


async def test_native_return_without_owner_fences_already_started_reappearance_read(
    connected: Connected, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = connected
    registry = rig.bridge.mesh_registry
    assert registry is not None
    command = asyncio.create_task(rig.mqtt.inject(command_topic("mesh", "load"), '{"state":"OFF"}'))
    entered, release = asyncio.Event(), asyncio.Event()
    read: asyncio.Task[list[BrilliantDevice]] | None = None
    original = rig.harness.observers[0].get_device
    snapshot = copy.deepcopy(rig.raw)
    try:
        await turns()
        ticket = rig.adapter.outstanding_mesh_ticket("load")
        assert ticket is not None and ticket.issued_at is not None
        command.cancel()
        await asyncio.gather(command, return_exceptions=True)
        assert rig.raw.peripherals is not None
        rig.raw.peripherals.pop("load")
        await rig.adapter.get_all()
        await rig.bridge._flush_retired_mesh()
        assert registry.current("load") is None and "load" not in registry.retired

        async def gated(device_id: str) -> _RawDevice:
            if device_id != "ble_mesh":
                return _RawDevice(device_id, {})
            entered.set()
            await release.wait()
            return snapshot

        monkeypatch.setattr(rig.harness.observers[0], "get_device", gated)
        read = asyncio.create_task(rig.adapter.get_all())
        await asyncio.wait_for(entered.wait(), 1)
        rig.harness.write_release.set()
        await turns()
        assert ticket.settled and ticket.returned_at is not None
        assert rig.adapter.mesh_creation_floor >= ticket.returned_at.sequence
        release.set()
        await read
        assert registry.current("load") is None
        assert "load" not in registry.retired
        assert snapshot.peripherals is not None
        rig.raw.peripherals["load"] = snapshot.peripherals["load"]
        monkeypatch.setattr(rig.harness.observers[0], "get_device", original)
        await rig.bridge.reconcile()
        assert rig.owner().render(rig.clock())["state"] == "ON"
        assert rig.owner().view.window is None
        assert rig.adapter.outstanding_mesh_ticket("load") is None
    finally:
        release.set()
        rig.harness.write_release.set()
        await asyncio.gather(command, *([read] if read is not None else []), return_exceptions=True)


@pytest.mark.parametrize("scoped", [False, True], ids=["inventory", "scoped-none"])
async def test_native_absence_retires_matched_window_before_expiry(
    connected: Connected, monkeypatch: pytest.MonkeyPatch, scoped: bool
) -> None:
    rig = connected
    await _accept_off(rig)
    rig.clock.advance(75)
    await rig.notify(on="0")
    old = rig.owner()
    assert dict(old.view.evidence)["on"].value == "0"
    rig.mqtt.published.clear()
    if scoped:

        async def absent(device_id: str, peripheral_id: str) -> None:
            assert device_id == "ble_mesh" and peripheral_id == "load"

        monkeypatch.setattr(rig.harness.observers[0], "get_peripheral", absent, raising=False)
        await rig.adapter.get_peripheral("ble_mesh", "load")
    else:
        assert rig.raw.peripherals is not None
        rig.raw.peripherals.pop("load")
        await rig.adapter.get_all()
    await rig.bridge._flush_retired_mesh()
    assert rig.bridge.mesh_registry is not None
    assert rig.bridge.mesh_registry.current("load") is None
    assert "load" not in rig.bridge.mesh_registry.retired
    assert not old.active
    assert old.view.evidence == ()
    rig.clock.advance(5)
    await rig.sleeper.release_all()
    await turns()
    states = _states(rig)
    assert states
    assert all(
        fields["state"] is None and fields["mesh_write_status"] == "unconfirmed"
        for fields in states
    )
    assert command_topic("mesh", "load") not in rig.bridge._by_cmd_topic


async def test_blocked_terminal_native_contradiction_precedes_ordinary_idle_debt(
    connected: Connected, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = connected
    await _accept_off(rig)
    entered, release = asyncio.Event(), asyncio.Event()
    original = rig.mqtt.publish

    async def publish(topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        if (
            topic == "brilliant/mesh/load/state"
            and json.loads(payload)["mesh_write_status"] == "contradicted"
        ):
            entered.set()
            await release.wait()
        await original(topic, payload, retain, qos)

    monkeypatch.setattr(rig.mqtt, "publish", publish)
    rig.mqtt.published.clear()
    try:
        await rig.notify(on="1")
        await asyncio.wait_for(entered.wait(), 1)
        await rig.notify(on="0", power="123")
        owner = rig.owner()
        assert owner.view.status == "idle"
        assert owner.view.terminal is not None
        assert owner.view.ordinary_debt is not None
        assert _states(rig) == []
        release.set()
        assert rig.bridge.mesh_driver is not None
        await rig.bridge.mesh_driver.flush(owner)
        wire = _states(rig)
        assert [fields["mesh_write_status"] for fields in wire] == ["contradicted", "idle"]
        assert wire[-1]["state"] == "OFF" and wire[-1]["power"] == 12.3
        assert owner.view.terminal is None and owner.view.ordinary_debt is None
    finally:
        release.set()


@pytest.mark.parametrize("replacement", ["reconnect", "reacquire"])
async def test_abandoned_native_work_keeps_successor_unknown_until_post_return_capture(
    connected: Connected, replacement: str
) -> None:
    rig = connected
    command = asyncio.create_task(rig.mqtt.inject(command_topic("mesh", "load"), '{"state":"OFF"}'))
    try:
        await turns()
        old = rig.owner()
        ticket = rig.adapter.outstanding_mesh_ticket("load")
        assert ticket is not None and ticket.issued_at is not None
        command.cancel()
        await asyncio.gather(command, return_exceptions=True)
        assert old.view.status == "unconfirmed"
        if replacement == "reconnect":
            rig.harness.procs[0].reconnect_cbs[0]()
        else:
            await rig.bridge.withdraw()
        await rig.bridge.reconcile()
        successor = rig.owner()
        assert successor is not old
        assert successor.view.completion_hold is not None
        assert successor.view.completion_hold.ticket is ticket
        assert successor.view.targets == ()
        assert successor.view.evidence == ()
        await rig.notify(on="1")
        assert successor.render(rig.clock())["state"] is None
        rig.harness.write_release.set()
        await turns()
        assert ticket.settled
        assert successor.view.completion_hold is None
        assert successor.render(rig.clock())["state"] is None
        assert successor.view.window is None
        await rig.notify(on="1")
        assert successor.render(rig.clock())["state"] == "ON"
        assert successor.view.window is None
        assert rig.harness.max_writes_in_flight == 1
    finally:
        rig.harness.write_release.set()
        await asyncio.gather(command, return_exceptions=True)


@pytest.mark.parametrize("mask", ["target", "power", "deleted", "unreliable"])
async def test_identical_native_mirror_refreshes_only_explicit_reliable_target_evidence(
    connected: Connected, mask: str
) -> None:
    rig = connected
    await _accept_off(rig)
    rig.clock.advance(75)
    assert rig.raw.peripherals is not None
    variables = rig.raw.peripherals["load"].variables
    variables["on"].value = "0"
    variables["power"] = _RawVariable("123")
    modified = {
        "target": [delta("load", "on")],
        "power": [delta("load", "power")],
        "deleted": [delta("load", removed=("on",))],
        "unreliable": None,
    }[mask]
    await rig.harness.observers[0].handle_notification(
        SimpleNamespace(updated_device=rig.raw, deleted=False, modified_peripherals=modified)
    )
    await turns()
    owner = rig.owner()
    assert owner.view.status == "pending"
    assert owner.render(rig.clock())["state"] is None
    assert bool(owner.view.evidence) is (mask == "target")
    rig.clock.advance(5)
    await rig.sleeper.release_all()
    await turns()
    assert owner.view.status == ("idle" if mask == "target" else "unconfirmed")
    assert _states(rig)[-1]["state"] == ("OFF" if mask == "target" else None)
