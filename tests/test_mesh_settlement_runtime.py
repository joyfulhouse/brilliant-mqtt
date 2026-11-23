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
    adapter._clock = clock
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


async def _accept_native(rig: _Rig) -> None:
    rig.observer.release.set()
    await _turns(20)
    pending = rig.bridge._mesh.pending_view[MESH_PID]
    assert not pending.rpc_unresolved
    assert pending.accepted_at is not None
    assert pending.expires_at == rig.clock() + mesh_pending.MESH_CONFIRM_SECONDS


def _set_on(rig: _Rig, value: str) -> None:
    assert rig.raw.peripherals is not None
    rig.raw.peripherals[MESH_PID].variables["on"].value = value


async def _push_on(rig: _Rig, value: str) -> None:
    _set_on(rig, value)
    rig.adapter._dispatch_raw_device(rig.raw)
    await _turns(20)


def _latest_state(rig: _Rig) -> dict[str, object]:
    state: dict[str, object] = json.loads(_mesh_states(rig.mqtt)[-1][1])
    return state


async def test_reconnect_new_generation_cannot_echo_old_matching_off(rig: _Rig) -> None:
    await _accept_native(rig)
    accepted = rig.bridge._mesh.pending_view[MESH_PID].accepted_at
    assert accepted is not None
    rig.clock.advance(75)
    await _push_on(rig, "0")
    assert rig.bridge._mesh.pending_view[MESH_PID].last_observed_at == 75

    rig.adapter.on_reconnect(rig.bridge.reconcile)
    _set_on(rig, "1")
    rig.adapter._on_proc_reconnect()
    await _turns(30)
    captured = await rig.adapter.get_all()
    provenance = captured[0].capture_provenance
    assert provenance is not None
    assert provenance.source_generation > accepted.source_generation
    await _push_on(rig, "1")
    published_before_expiry = len(_mesh_states(rig.mqtt))
    rig.clock.advance(5)
    await rig.sleeper.release_all()
    await _turns(20)

    assert _latest_state(rig)["state"] == "ON"
    assert not any(
        json.loads(payload)["state"] == "OFF"
        for _topic, payload, _retain in _mesh_states(rig.mqtt)[published_before_expiry:]
    )


@pytest.mark.parametrize("resolver_order", ["before_observation", "after_observation"])
async def test_fresh_contradiction_at_expiry_cannot_be_overwritten(
    rig: _Rig, resolver_order: str
) -> None:
    await _accept_native(rig)
    rig.clock.advance(75)
    await _push_on(rig, "0")
    pending = rig.bridge._mesh.pending_view[MESH_PID]
    assert pending.last_observed_at == 75
    rig.clock.advance(5)
    assert rig.clock() == pending.expires_at

    if resolver_order == "before_observation":
        await rig.sleeper.release_all()
        await _turns(20)
        assert MESH_PID not in rig.bridge._mesh.pending_view
    else:
        assert rig.bridge._mesh.pending_view[MESH_PID] is pending
    await _push_on(rig, "1")
    assert _latest_state(rig)["state"] == "ON"
    published_after_observation = len(_mesh_states(rig.mqtt))
    if resolver_order == "after_observation":
        await rig.sleeper.release_all()
        await _turns(20)

    assert MESH_PID not in rig.bridge._mesh.pending_view
    assert _latest_state(rig)["state"] == "ON"
    assert not any(
        json.loads(payload)["state"] == "OFF"
        for _topic, payload, _retain in _mesh_states(rig.mqtt)[published_after_observation:]
    )


async def test_early_post_acceptance_capture_delivered_at_79_is_stale_at_80(rig: _Rig) -> None:
    await _accept_native(rig)
    rig.clock.advance(1)
    _set_on(rig, "0")
    captured = await rig.adapter.get_all()
    provenance = captured[0].capture_provenance
    accepted = rig.bridge._mesh.pending_view[MESH_PID].accepted_at
    assert provenance is not None and accepted is not None
    assert provenance.source_generation == accepted.source_generation
    assert provenance.sequence > accepted.sequence
    rig.clock.advance(78)
    await rig.bridge.poll_once(captured)
    rig.clock.advance(1)
    await rig.sleeper.release_all()
    await _turns(20)

    assert MESH_PID not in rig.bridge._mesh.pending_view
    assert _latest_state(rig)["mesh_write_status"] == "unconfirmed"
    assert _latest_state(rig)["state"] is None


async def test_native_acceptance_before_caller_timeout_preserves_receipt_and_fence(
    rig: _Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    await _accept_native(rig)
    rig.clock.advance(1)
    captured = await rig.adapter.get_all()
    rig.clock.advance(4)
    finish_reached = asyncio.Event()
    deferred: list[tuple[bus_mod._WriteAdmission, asyncio.Task[str]]] = []
    boundary: list[tuple[bool, str, bool]] = []
    original_finish = rig.adapter._finish_admission

    def hold_result(admission: bus_mod._WriteAdmission, native: asyncio.Task[str]) -> None:
        # Hold only the result-delivery callback: the real native task has
        # returned and synchronously settled its ticket before the caller's
        # zero-second deadline sees the still-pending result Future.
        boundary.append((admission.ticket.settled, native.result(), admission.result.done()))
        assert admission.ticket.accepted_at is not None
        finish_reached.set()
        deferred.append((admission, native))

    monkeypatch.setattr(rig.adapter, "_finish_admission", hold_result)
    try:
        await rig.mqtt.inject(MESH_SET_TOPIC, '{"state":"OFF"}')
        assert finish_reached.is_set()
        assert boundary == [(True, "'ok'", False)]
        (admission, native) = deferred[0]
        assert admission.record.detached
        assert not admission.result.done()
        assert native.done()
        pending = rig.bridge._mesh.pending_view[MESH_PID]
        assert not pending.rpc_unresolved
        assert pending.receipt == native.result()
        assert pending.accepted_at == admission.ticket.accepted_at
        assert pending.accepted_at is not None
        provenance = captured[0].capture_provenance
        assert provenance is not None
        assert provenance.sequence < pending.accepted_at.sequence
        await rig.bridge.poll_once(captured)
        assert rig.bridge._mesh.pending_view[MESH_PID] is pending
        assert pending.last_observed_at is None
        assert _latest_state(rig)["mesh_write_status"] == "pending"
    finally:
        for admission, native in deferred:
            original_finish(admission, native)


@pytest.mark.parametrize("older_on,newer_on", [("0", "1"), ("1", "0")])
async def test_older_same_generation_capture_cannot_replace_newer_evidence(
    rig: _Rig, older_on: str, newer_on: str
) -> None:
    await _accept_native(rig)
    rig.clock.advance(74)
    _set_on(rig, older_on)
    older = await rig.adapter.get_all()
    rig.clock.advance(1)
    _set_on(rig, newer_on)
    newer = await rig.adapter.get_all()
    old_provenance = older[0].capture_provenance
    new_provenance = newer[0].capture_provenance
    assert old_provenance is not None and new_provenance is not None
    assert old_provenance.source_generation == new_provenance.source_generation
    assert old_provenance.sequence < new_provenance.sequence
    await rig.bridge.poll_once(newer)
    if newer_on == "0":
        pending = rig.bridge._mesh.pending_view[MESH_PID]
        assert pending.last_observed_at == 75
    else:
        assert _latest_state(rig)["mesh_write_status"] == "contradicted"
    rig.clock.advance(4)
    await rig.bridge.poll_once(older)
    if newer_on == "0":
        assert MESH_PID in rig.bridge._mesh.pending_view
        assert rig.bridge._mesh.pending_view[MESH_PID] is pending
        assert pending.last_observed_at == 75
    else:
        assert MESH_PID not in rig.bridge._mesh.pending_view
        assert _latest_state(rig)["state"] == "ON"
    rig.clock.advance(1)
    await rig.sleeper.release_all()
    await _turns(20)

    assert _latest_state(rig)["state"] == ("ON" if newer_on == "1" else "OFF")
    if newer_on == "1":
        assert _latest_state(rig)["mesh_write_status"] == "contradicted"


async def test_normal_success_preserves_native_receipt_and_acceptance_fence(
    rig: _Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    await _accept_native(rig)
    rig.clock.advance(1)
    captured = await rig.adapter.get_all()
    rig.clock.advance(4)
    monkeypatch.setattr(bus_mod, "_WRITE_DEADLINE_S", 1)
    await rig.mqtt.inject(MESH_SET_TOPIC, '{"state":"OFF"}')
    pending = rig.bridge._mesh.pending_view[MESH_PID]
    assert not pending.rpc_unresolved
    assert pending.receipt == "'ok'"
    assert pending.accepted_at is not None
    provenance = captured[0].capture_provenance
    assert provenance is not None
    assert provenance.source_generation == pending.accepted_at.source_generation
    assert provenance.sequence < pending.accepted_at.sequence
    await rig.bridge.poll_once(captured)
    assert rig.bridge._mesh.pending_view[MESH_PID] is pending
    assert pending.last_observed_at is None
    assert _latest_state(rig)["mesh_write_status"] == "pending"


async def test_older_source_generation_capture_cannot_replace_newer_evidence(rig: _Rig) -> None:
    await _accept_native(rig)
    rig.clock.advance(1)
    _set_on(rig, "0")
    older = await rig.adapter.get_all()
    rig.adapter.on_reconnect(rig.bridge.reconcile)
    _set_on(rig, "1")
    rig.adapter._on_proc_reconnect()
    await _turns(30)
    newer = await rig.adapter.get_all()
    old_provenance = older[0].capture_provenance
    new_provenance = newer[0].capture_provenance
    assert old_provenance is not None and new_provenance is not None
    assert old_provenance.source_generation < new_provenance.source_generation
    await rig.bridge.poll_once(newer)
    assert _latest_state(rig)["state"] == "ON"
    await rig.bridge.poll_once(older)
    assert _latest_state(rig)["state"] == "ON"
    assert MESH_PID not in rig.bridge._mesh.pending_view


async def test_complete_capture_missing_on_at_expiry_cannot_echo_old_match(rig: _Rig) -> None:
    await _accept_native(rig)
    rig.clock.advance(75)
    await _push_on(rig, "0")
    pending = rig.bridge._mesh.pending_view[MESH_PID]
    assert pending.last_observed_at == 75
    rig.clock.advance(5)
    assert rig.clock() == pending.expires_at
    assert rig.raw.peripherals is not None
    del rig.raw.peripherals[MESH_PID].variables["on"]
    captured = await rig.adapter.get_all()
    assert "on" not in captured[0].variables
    await rig.bridge.poll_once(captured)
    assert rig.bridge._mesh.pending_view[MESH_PID] is pending
    published_after_observation = len(_mesh_states(rig.mqtt))
    await rig.sleeper.release_all()
    await _turns(20)
    assert _latest_state(rig)["mesh_write_status"] == "unconfirmed"
    assert _latest_state(rig)["state"] is None
    assert not any(
        json.loads(payload)["state"] == "OFF"
        for _topic, payload, _retain in _mesh_states(rig.mqtt)[published_after_observation:]
    )


async def test_old_source_capture_cannot_contradict_new_acceptance(rig: _Rig) -> None:
    await _accept_native(rig)
    rig.clock.advance(1)
    older = await rig.adapter.get_all()
    assert older[0].variables["on"].value == "1"
    rig.adapter.on_reconnect(rig.bridge.reconcile)
    rig.adapter._on_proc_reconnect()
    await _turns(30)
    await rig.mqtt.inject(MESH_SET_TOPIC, '{"state":"OFF"}')
    await _turns(20)
    pending = rig.bridge._mesh.pending_view[MESH_PID]
    assert not pending.rpc_unresolved
    accepted = pending.accepted_at
    old_provenance = older[0].capture_provenance
    assert accepted is not None and old_provenance is not None
    assert old_provenance.source_generation < accepted.source_generation
    rig.clock.advance(75)
    await _push_on(rig, "0")
    observed_at = pending.last_observed_at
    assert observed_at == rig.clock()
    await rig.bridge.poll_once(older)
    assert rig.bridge._mesh.pending_view[MESH_PID] is pending
    assert pending.last_observed_at == observed_at
    rig.clock.advance(5)
    await rig.sleeper.release_all()
    await _turns(20)
    assert _latest_state(rig)["state"] == "OFF"
