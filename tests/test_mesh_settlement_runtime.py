"""Late mesh RPC settlement through the production adapter and bridge."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass

import pytest

from brilliant_mqtt import bus as bus_mod
from brilliant_mqtt import mesh_pending
from brilliant_mqtt.bridge import Bridge
from brilliant_mqtt.bus import RpcBusAdapter
from brilliant_mqtt.model import BrilliantDevice
from tests.fakes import FakeClock, FakeSleeper, _GatedRpcObserver, _RawDevice, _RawPeripheral
from tests.test_bridge import (
    MESH_PID,
    MESH_SET_TOPIC,
    _BlockingStatePublishMqtt,
    _mesh_states,
    _turns,
)
from tests.test_bus_adapter import _gated_adapter


@dataclass
class _Rig:
    observer: _GatedRpcObserver
    adapter: RpcBusAdapter
    raw: _RawDevice
    mqtt: _BlockingStatePublishMqtt
    bridge: Bridge
    clock: FakeClock
    sleeper: FakeSleeper


@pytest.fixture
async def rig(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[_Rig]:
    monkeypatch.setattr(bus_mod, "_WRITE_DEADLINE_S", 0)
    observer, adapter = _gated_adapter()
    peripheral = _RawPeripheral("1")
    peripheral.peripheral_type = 27
    raw = _RawDevice("ble_mesh", {MESH_PID: peripheral})

    async def get_device(device_id: str) -> _RawDevice:
        observer.reads.append(device_id)
        return raw if device_id == "ble_mesh" else _RawDevice(device_id, {})

    monkeypatch.setattr(observer, "get_device", get_device)
    mqtt = _BlockingStatePublishMqtt()
    clock = FakeClock()
    sleeper = FakeSleeper()
    bridge = Bridge(
        adapter, mqtt, "mesh", include=lambda dev: dev == "ble_mesh", clock=clock, sleep=sleeper
    )
    bridge._wall_clock = lambda: 1000.0 + clock()
    try:
        await bridge.reconcile()
        await mqtt.inject(MESH_SET_TOPIC, '{"state":"OFF"}')
        assert bridge._mesh.pending_view[MESH_PID].rpc_unresolved
        yield _Rig(observer, adapter, raw, mqtt, bridge, clock, sleeper)
    finally:
        observer.release.set()
        mqtt.release_publish.set()
        await bridge.withdraw()
        await adapter.shutdown()


async def test_pre_acceptance_snapshot_is_not_contradiction(rig: _Rig) -> None:
    captured = await rig.adapter.get_all()
    assert captured[0].capture_provenance is not None
    rig.observer.release.set()
    await _turns(16)
    assert not rig.bridge._mesh.pending_view[MESH_PID].rpc_unresolved
    await rig.bridge.poll_once(captured)
    assert json.loads(_mesh_states(rig.mqtt)[-1][1])["mesh_write_status"] == "pending"


async def test_fresh_contradiction_after_native_return_is_not_lost(rig: _Rig) -> None:
    (native,) = rig.adapter._write_tasks
    delivered: list[tuple[bool, bool]] = []
    rig.clock.advance(5)
    rig.mqtt.block_next_state_publish = True

    def deliver() -> None:
        delivered.append(
            (native.done() and native.result() == "'ok'", not rig.mqtt.publish_started.is_set())
        )
        rig.adapter._dispatch_raw_device(rig.raw)

    rig.observer.release.set()
    # Releasing the native waiter queues it before deliver; its done callbacks
    # and settlement publication are queued after deliver in the next loop turn.
    asyncio.get_running_loop().call_soon(deliver)
    await _turns(20)
    assert delivered == [(True, True)]
    assert rig.mqtt.publish_started.is_set()
    assert not rig.mqtt.release_publish.is_set()
    assert rig.bridge._mesh.feedback_view[MESH_PID].status == "contradicted"
    rig.mqtt.release_publish.set()
    await _turns(20)
    assert json.loads(_mesh_states(rig.mqtt)[-1][1])["mesh_write_status"] == "contradicted"


async def test_expiry_does_not_wait_for_rearm_publish(rig: _Rig) -> None:
    # A later wall deadline makes the re-arm payload differ from the first pending publish.
    rig.clock.advance(5)
    rig.mqtt.block_next_state_publish = True
    rig.observer.release.set()
    await asyncio.wait_for(rig.mqtt.publish_started.wait(), timeout=1)
    reads_after_acceptance = list(rig.observer.reads)
    rig.bridge._wall_clock = lambda: -1_000_000.0
    rig.clock.advance(mesh_pending.MESH_CONFIRM_SECONDS)
    await rig.sleeper.release_all()
    await _turns(20)
    assert not rig.mqtt.release_publish.is_set()
    assert MESH_PID not in rig.bridge._mesh.pending_view
    assert rig.bridge._mesh.feedback_view[MESH_PID].status == "unconfirmed"
    rig.mqtt.release_publish.set()
    await _turns(20)
    assert rig.observer.reads == reads_after_acceptance
    assert json.loads(_mesh_states(rig.mqtt)[-1][1])["mesh_write_status"] == "unconfirmed"


async def test_pre_acceptance_match_delivered_near_expiry_cannot_confirm(rig: _Rig) -> None:
    assert rig.raw.peripherals is not None
    rig.raw.peripherals[MESH_PID].variables["on"].value = "0"
    captured = await rig.adapter.get_all()
    assert captured[0].capture_provenance is not None

    rig.clock.advance(5)
    rig.observer.release.set()
    await _turns(20)
    pending = rig.bridge._mesh.pending_view[MESH_PID]
    assert not pending.rpc_unresolved
    assert pending.last_observed_at is None

    rig.clock.advance(mesh_pending.MESH_CONFIRM_SECONDS - 1)
    await rig.bridge.poll_once(captured)
    assert rig.bridge._mesh.pending_view[MESH_PID] is pending
    assert pending.last_observed_at is None

    rig.clock.advance(1)
    await rig.sleeper.release_all()
    await _turns(20)
    assert MESH_PID not in rig.bridge._mesh.pending_view
    assert rig.bridge._mesh.feedback_view[MESH_PID].status == "unconfirmed"
    assert json.loads(_mesh_states(rig.mqtt)[-1][1])["mesh_write_status"] == "unconfirmed"


@pytest.mark.parametrize("revocation", ["supersede", "withdraw"])
async def test_adapter_rearm_publish_stays_cancellable(rig: _Rig, revocation: str) -> None:
    rig.clock.advance(5)
    rig.mqtt.published.clear()
    rig.mqtt.block_next_state_publish = True
    rig.observer.release.set()
    await asyncio.wait_for(rig.mqtt.publish_started.wait(), timeout=1)
    old_timer = rig.bridge._mesh.confirm_tasks_view[MESH_PID]
    publication = rig.bridge._mesh._publication_tasks[MESH_PID]
    (blocked_publish,) = rig.bridge._mesh_feedback_tasks
    old_pending = rig.bridge._mesh.pending_view[MESH_PID]

    if revocation == "withdraw":
        await rig.bridge.withdraw()
    else:
        rig.clock.advance(1)
        await rig.mqtt.inject(MESH_SET_TOPIC, '{"state":"ON"}')
    await _turns(20)

    # Cancellation must reach the blocked gate before it is released.
    assert not rig.mqtt.release_publish.is_set()
    assert blocked_publish.cancelled()
    assert publication.done()
    assert old_timer.cancelled()
    if revocation == "withdraw":
        assert rig.bridge._mesh.pending_view == {}
        assert rig.bridge._mesh.confirm_tasks_view == {}
    else:
        replacement = rig.bridge._mesh.pending_view[MESH_PID]
        assert replacement is not old_pending
        assert replacement.targets == {"on": "1"}
        replacement_timer = rig.bridge._mesh.confirm_tasks_view[MESH_PID]
        assert replacement_timer is not old_timer
        assert not replacement_timer.done()

    rig.mqtt.release_publish.set()
    await _turns(20)
    assert not any(
        json.loads(payload).get("mesh_requested") == {"on": "0"}
        for _topic, payload, _retain in _mesh_states(rig.mqtt)
    )
    if revocation == "withdraw":
        await rig.sleeper.release_all()
        assert rig.bridge._mesh.pending_view == {}
        assert rig.bridge._mesh.confirm_tasks_view == {}
    else:
        assert rig.bridge._mesh.pending_view[MESH_PID] is replacement
        assert rig.bridge._mesh.confirm_tasks_view[MESH_PID] is replacement_timer
        assert not replacement_timer.done()


async def test_pre_acceptance_push_delayed_until_after_acceptance(
    rig: _Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    drain_started = asyncio.Event()
    release_drain = asyncio.Event()
    original_drain = rig.adapter._drain_pushes

    async def gated_drain(
        key: tuple[str | None, Callable[[BrilliantDevice], Awaitable[None]]],
        cb: Callable[[BrilliantDevice], Awaitable[None]],
        session: int,
    ) -> None:
        drain_started.set()
        await release_drain.wait()
        await original_drain(key, cb, session)

    monkeypatch.setattr(rig.adapter, "_drain_pushes", gated_drain)
    try:
        rig.adapter._dispatch_raw_device(rig.raw)
        await asyncio.wait_for(drain_started.wait(), timeout=1)
        assert rig.bridge._mesh.pending_view[MESH_PID].rpc_unresolved
        rig.clock.advance(5)
        rig.observer.release.set()
        await _turns(20)
        pending = rig.bridge._mesh.pending_view[MESH_PID]
        assert not pending.rpc_unresolved

        release_drain.set()
        await _turns(20)
        assert rig.bridge._mesh.pending_view[MESH_PID] is pending
        assert pending.last_observed_at is None
        assert json.loads(_mesh_states(rig.mqtt)[-1][1])["mesh_write_status"] == "pending"
    finally:
        release_drain.set()


async def test_matching_push_at_expiry_cannot_confirm(rig: _Rig) -> None:
    rig.clock.advance(5)
    rig.observer.release.set()
    await _turns(20)
    pending = rig.bridge._mesh.pending_view[MESH_PID]
    assert not pending.rpc_unresolved
    assert pending.expires_at is not None
    rig.clock.advance(pending.expires_at - rig.clock())
    assert rig.clock() == pending.expires_at

    assert rig.raw.peripherals is not None
    rig.raw.peripherals[MESH_PID].variables["on"].value = "0"
    rig.adapter._dispatch_raw_device(rig.raw)
    await _turns(20)
    assert rig.bridge._mesh.pending_view[MESH_PID] is pending
    assert pending.last_observed_at is None

    await rig.sleeper.release_all()
    await _turns(20)
    assert MESH_PID not in rig.bridge._mesh.pending_view
    assert rig.bridge._mesh.feedback_view[MESH_PID].status == "unconfirmed"
    assert json.loads(_mesh_states(rig.mqtt)[-1][1])["mesh_write_status"] == "unconfirmed"
