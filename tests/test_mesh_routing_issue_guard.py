"""Withdrawal cleanup must recheck ownership at actual membership issue."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable

import pytest

from brilliant_mqtt.mesh_routing import TopicRepair
from tests.test_mesh_bridge_integration import Connected, connected
from tests.test_mesh_review_corrections import assert_routing_indexes

__all__ = ["connected"]


async def failed_withdrawal(
    rig: Connected, monkeypatch: pytest.MonkeyPatch, pids: tuple[str, ...]
) -> dict[str, int]:
    unsubscribe = rig.mqtt.unsubscribe
    attempts = dict.fromkeys(pids, 0)

    async def fail_once(topic: str) -> None:
        for pid in attempts:
            if topic == f"brilliant/mesh/{pid}/set":
                attempts[pid] += 1
                if attempts[pid] == 1:
                    raise RuntimeError("temporary unsubscribe failure")
        await unsubscribe(topic)

    monkeypatch.setattr(rig.mqtt, "unsubscribe", fail_once)
    await rig.bridge.withdraw()
    assert attempts == dict.fromkeys(pids, 1)
    return attempts


async def assert_later_poll_repairs_current_owner(rig: Connected, pid: str) -> None:
    topic = f"brilliant/mesh/{pid}/set"
    owner = rig.owner(pid)
    await asyncio.wait_for(rig.bridge.poll_once(), 2)
    assert rig.owner(pid) is owner
    assert topic in rig.bridge._subscribed
    assert rig.bridge._resolve_mesh_owner(topic) is owner
    assert [p for t, p, _ in rig.mqtt.published if t == "brilliant/mesh/availability"][
        -1
    ] == "online"
    assert_routing_indexes(rig)
    assert topic not in rig.bridge._mesh_routing._withdrawn
    assert not rig.adapter.consume_mesh_fatal()
    rig.harness.write_release.set()
    await rig.mqtt.inject(topic, '{"state":"OFF"}')
    assert rig.harness.write_starts == ["ble_mesh"]


async def test_reappearance_during_other_withdrawn_owner_repair_keeps_cleanup_debt(
    connected: Connected, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = connected
    registry, routing = rig.bridge.mesh_registry, rig.bridge._mesh_routing
    assert registry is not None and rig.raw.peripherals is not None
    attempts = await failed_withdrawal(rig, monkeypatch, ("load", "blocker"))
    first, second = tuple(routing.withdrawn_peripheral_ids)
    saved = dict(rig.raw.peripherals)
    rig.raw.peripherals.clear()
    flush_retired = rig.bridge._flush_retired_mesh
    seeded = False

    async def seed_second() -> None:
        nonlocal seeded
        await flush_retired()
        if not seeded:
            seeded = True
            assert rig.raw.peripherals is not None
            rig.raw.peripherals[second] = saved[second]
            await rig.adapter.get_all()
            assert registry.current(first) is None and registry.current(second) is not None

    monkeypatch.setattr(rig.bridge, "_flush_retired_mesh", seed_second)
    subscribe = rig.mqtt.subscribe
    injected = False

    async def reappear_during_subscribe(topic: str) -> None:
        nonlocal injected
        if not injected and topic == f"brilliant/mesh/{second}/set":
            injected = True
            assert rig.raw.peripherals is not None
            rig.raw.peripherals[first] = saved[first]
            await rig.adapter.get_all()
            assert registry.current(first) is not None
        await subscribe(topic)

    monkeypatch.setattr(rig.mqtt, "subscribe", reappear_during_subscribe)
    topic = f"brilliant/mesh/{first}/set"
    record = routing.records[topic]
    desired, membership = record.desired, record.membership
    await asyncio.wait_for(rig.bridge.reconcile(), 2)
    assert injected
    assert attempts[first] == 1
    assert topic in rig.bridge._subscribed
    assert routing.records[topic] is record
    assert record.desired is desired and record.applied is not desired
    assert record.membership is membership
    assert topic in routing._withdrawn
    assert_routing_indexes(rig)
    await assert_later_poll_repairs_current_owner(rig, first)
    assert attempts[first] == 1


async def test_reappearance_after_worker_check_before_child_issue_keeps_cleanup_debt(
    connected: Connected, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = connected
    registry, routing = rig.bridge.mesh_registry, rig.bridge._mesh_routing
    assert registry is not None and rig.raw.peripherals is not None
    attempts = await failed_withdrawal(rig, monkeypatch, ("blocker",))
    saved = rig.raw.peripherals.pop("blocker")
    topic = "brilliant/mesh/blocker/set"
    record = routing.records[topic]
    desired, membership = record.desired, record.membership
    operation = routing._operation
    injected = False

    async def reappear_before_child(target: TopicRepair, child: Awaitable[None]) -> None:
        nonlocal injected
        if target is record and not injected:
            injected = True
            assert target.desired is desired and desired.forget
            assert desired.membership is False and registry.current("blocker") is None
            assert rig.raw.peripherals is not None
            rig.raw.peripherals["blocker"] = saved
            await rig.adapter.get_all()
            assert registry.current("blocker") is not None
        await operation(target, child)

    monkeypatch.setattr(routing, "_operation", reappear_before_child)
    await asyncio.wait_for(rig.bridge.reconcile(), 2)
    assert injected
    assert attempts["blocker"] == 1
    assert topic in rig.bridge._subscribed
    assert routing.records[topic] is record
    assert record.desired is desired and record.applied is not desired
    assert record.membership is membership
    assert topic in routing._withdrawn
    assert_routing_indexes(rig)
    await assert_later_poll_repairs_current_owner(rig, "blocker")
    assert attempts["blocker"] == 1
