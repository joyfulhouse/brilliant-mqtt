"""The registered native hook fences authorization at entry, across threads."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable

import pytest

from brilliant_mqtt.bus import RpcBusAdapter
from brilliant_mqtt.mesh_settlement import MeshSettlement
from brilliant_mqtt.mesh_settlement_types import MeshMetadata
from brilliant_mqtt.model import DeviceKind
from tests.fakes import _StartHarness


def owner_for(adapter: RpcBusAdapter) -> MeshSettlement:
    return MeshSettlement(
        MeshMetadata("ble_mesh", "p", "Load", DeviceKind.SWITCH, 1),
        source=adapter.capture_source,
        generation=adapter.capture_generation,
        birth_sequence=adapter.capture_sequence,
        born_at=0,
    )


def foreign(callback: Callable[[], None]) -> None:
    thread = threading.Thread(target=callback)
    thread.start()
    thread.join(timeout=1)
    assert not thread.is_alive()


@pytest.mark.parametrize("on_thread", [False, True], ids=["loop", "foreign"])
async def test_actual_registered_hook_fences_ready_authorization_before_fanout(
    monkeypatch: pytest.MonkeyPatch, on_thread: bool
) -> None:
    from brilliant_mqtt.mesh_ingress import MeshGeneration

    harness = _StartHarness(monkeypatch)
    adapter = RpcBusAdapter()
    await adapter.start()
    owner = owner_for(adapter)
    seen: list[object] = []
    adapter.register_mesh_sink(seen.append, want_fields=lambda _pid: None)
    ready: list[bool] = []
    asyncio.get_running_loop().call_soon(lambda: ready.append(adapter.mesh_owner_live(owner)))
    generation, sequence = adapter.capture_generation, adapter.capture_sequence
    hook = harness.procs[0].reconnect_cbs[0]
    if on_thread:
        foreign(hook)
        assert seen == []
    else:
        hook()
        assert isinstance(seen[0], MeshGeneration)
    assert adapter.capture_generation == generation + 1
    assert not adapter.mesh_owner_live(owner)
    assert adapter.capture_sequence >= sequence
    await asyncio.sleep(0)
    assert ready == [False]
    assert len(seen) == 1
    await adapter.shutdown()


async def test_queued_hook_fanout_cannot_move_live_generation_backwards(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _StartHarness(monkeypatch)
    adapter = RpcBusAdapter()
    await adapter.start()
    generations: list[int] = []
    adapter.register_mesh_sink(
        lambda event: generations.append(event.provenance.source_generation),
        want_fields=lambda _pid: None,
    )
    hook = harness.procs[0].reconnect_cbs[0]
    foreign(hook)
    foreign(hook)
    current = adapter.capture_generation
    await asyncio.sleep(0)
    assert generations == [current]
    assert adapter.capture_generation == current
    await adapter.shutdown()


async def test_replaced_session_hook_cannot_invalidate_successor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _StartHarness(monkeypatch)
    adapter = RpcBusAdapter()
    await adapter.start()
    old_hook = harness.procs[0].reconnect_cbs[0]
    first = adapter._next_capture_provenance("read")
    await adapter.shutdown()
    await adapter.start()
    generation = adapter.capture_generation
    successor = owner_for(adapter)
    foreign(old_hook)
    old_hook()
    assert adapter.capture_generation == generation
    assert adapter.mesh_owner_live(successor)
    assert adapter._next_capture_provenance("read").sequence > first.sequence
    await asyncio.sleep(0)
    assert adapter.capture_generation == generation
    await adapter.shutdown()


async def test_public_local_boundaries_share_read_push_issue_sequence_domain() -> None:
    adapter = RpcBusAdapter(clock=lambda: 7)
    first = adapter.reserve_mesh_boundary("mqtt")
    adapter._on_proc_reconnect()
    second = adapter.reserve_mesh_boundary("timer")
    assert first.incarnation == second.incarnation == adapter.capture_source
    assert second.source_generation > first.source_generation
    assert second.sequence > first.sequence
    assert (first.origin, second.origin) == ("mqtt", "timer")
    assert first.captured_at == second.captured_at == 7
    await adapter.shutdown()


@pytest.mark.parametrize("on_thread", [False, True], ids=["loop", "foreign"])
@pytest.mark.parametrize("reconnect", [False, True], ids=["control", "hook"])
async def test_actual_hook_fences_an_already_ready_real_owner_expiry(
    monkeypatch: pytest.MonkeyPatch,
    on_thread: bool,
    reconnect: bool,
) -> None:
    from dataclasses import replace

    from brilliant_mqtt.mesh_ingress import MeshGeneration
    from brilliant_mqtt.mesh_settlement_types import (
        Begin,
        Envelope,
        Expire,
        Issue,
        NativeOutcome,
        Observe,
        OutcomeKind,
        Revoke,
        Source,
    )
    from brilliant_mqtt.model import CaptureProvenance, PrimitiveCapture, PrimitiveField
    from brilliant_mqtt.write_admission import AdmissionTicket
    from tests.fakes import FakeClock

    harness = _StartHarness(monkeypatch)
    clock = FakeClock()
    adapter = RpcBusAdapter(clock=clock)
    await adapter.start()
    owner = owner_for(adapter)

    def env(boundary: CaptureProvenance) -> Envelope:
        return Envelope(
            owner,
            Source(boundary.incarnation, boundary.origin),
            boundary.source_generation,
            boundary.sequence,
            boundary.captured_at,
            "p",
        )

    ticket = AdmissionTicket()
    owner.dispatch(Begin(env(adapter.reserve_mesh_boundary("mqtt")), ticket, (("on", "0"),)))
    command = owner.view.command_id
    owner.dispatch(
        Issue(env(adapter.reserve_mesh_boundary("issue")), command, ticket, (("on", "0"),))
    )
    owner.dispatch(
        NativeOutcome(
            env(adapter.reserve_mesh_boundary("native")),
            command,
            ticket,
            OutcomeKind.ACCEPTED,
            "ok",
        )
    )
    clock.advance(75)
    owner.dispatch(
        Observe(
            env(adapter.reserve_mesh_boundary("push")),
            PrimitiveCapture((PrimitiveField("on", "0"),)),
        )
    )
    clock.advance(5)
    window = owner.view.window
    assert window is not None
    expiry = Expire(env(adapter.reserve_mesh_boundary("timer")), window, clock())

    def generation(event: object) -> None:
        if isinstance(event, MeshGeneration):
            owner.dispatch(
                Revoke(env(replace(event.provenance, source_generation=owner.generation)))
            )

    adapter.register_mesh_sink(generation, want_fields=lambda _pid: None)

    def ready_expiry() -> None:
        if adapter.mesh_owner_live(owner):
            owner.dispatch(expiry)

    asyncio.get_running_loop().call_soon(ready_expiry)
    if reconnect:
        hook = harness.procs[0].reconnect_cbs[0]
        foreign(hook) if on_thread else hook()
    await asyncio.sleep(0)
    if reconnect:
        assert owner.view.status == "unconfirmed"
        assert owner.render(clock())["state"] is None
        assert owner.view.window is None
    else:
        assert owner.view.status == "idle"
        assert owner.render(clock())["state"] == "OFF"
    await adapter.shutdown()


async def test_foreign_hook_cannot_erase_current_generation_wired_issue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from brilliant_mqtt.commands import VarSet
    from brilliant_mqtt.write_admission import AdmissionTicket
    from tests.fakes import _RawPeripheral
    from tests.test_write_admission import _settle

    native = _StartHarness(monkeypatch, gated_writes=True)
    adapter = RpcBusAdapter()
    await adapter.start()
    device_id = adapter.own_device_id
    assert device_id is not None
    lock = adapter._write_locks.setdefault(device_id, asyncio.Lock())
    await lock.acquire()
    ticket = AdmissionTicket()
    caller = asyncio.create_task(
        adapter.set_variables(device_id, "p", [VarSet("on", "0")], ticket=ticket)
    )

    async def read(device_id: str, peripheral_id: str) -> _RawPeripheral:
        return _RawPeripheral("0")

    monkeypatch.setattr(adapter._obs, "get_peripheral", read, raising=False)
    try:
        await _settle()
        lock.release()  # queues the writer before the foreign hook's loop fanout
        foreign(native.procs[0].reconnect_cbs[0])
        await _settle()
        issued = ticket.issued_at
        assert issued is not None
        assert issued.source_generation == adapter.capture_generation
        captured = await adapter.get_peripheral(device_id, "p")
        assert captured is not None and captured.capture_provenance is not None
        assert (
            dict(captured.capture_provenance.field_eras)["on"] == dict(issued.field_eras)["on"] == 1
        )
        assert adapter.recent_reconnects(60) == 1
    finally:
        native.write_release.set()
        await asyncio.gather(caller, return_exceptions=True)
        await adapter.shutdown()


async def test_foreign_hook_cannot_erase_new_generation_privacy_denial(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.fakes import _RawDevice, _RawPeripheral

    native = _StartHarness(monkeypatch)
    adapter = RpcBusAdapter()
    await adapter.start()
    peripheral = _RawPeripheral("1", "privacy_toggle")
    peripheral.peripheral_type = 22
    raw = _RawDevice(adapter.own_device_id, {"hardware": peripheral})
    asyncio.get_running_loop().call_soon(adapter._dispatch_raw_device, raw)
    foreign(native.procs[0].reconnect_cbs[0])
    await asyncio.sleep(0)
    denial = adapter.privacy_revocation
    assert denial is not None
    assert denial.source_generation == adapter.capture_generation
    assert adapter.recent_reconnects(60) == 1
    await adapter.shutdown()
