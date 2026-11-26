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
