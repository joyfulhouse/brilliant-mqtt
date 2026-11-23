"""MeshPending in isolation: fake clocks and sleeper, recording callbacks (#225)."""

from __future__ import annotations

import asyncio
from types import MappingProxyType

import pytest

from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.mesh_pending import (
    MESH_CONFIRM_SECONDS,
    PENDING_RPC_RECEIPT,
    MeshPending,
    PendingMeshWrite,
)
from brilliant_mqtt.model import BrilliantDevice, DeviceKind, Variable
from brilliant_mqtt.write_admission import WriteCancelled
from tests.fakes import FakeClock, FakeSleeper

PID = "load"


class _Harness:
    def __init__(self) -> None:
        self.clock = FakeClock()
        self.wall = 1000.0
        self.sleeper = FakeSleeper()
        self.events: list[tuple[object, ...]] = []
        self.mesh = MeshPending(
            clock=self.clock,
            wall_clock=lambda: self.wall,
            sleep=self.sleeper,
            republish=self._republish,
            echo=self._echo,
            before_feedback_change=self._before_change,
        )

    async def _republish(self, peripheral_id: str, force: bool) -> None:
        feedback = self.mesh.feedback_view.get(peripheral_id)
        self.events.append(("republish", peripheral_id, force, _status(feedback)))

    async def _echo(self, peripheral_id: str, sets: list[VarSet]) -> None:
        self.events.append(("echo", peripheral_id, tuple(sets)))

    def _before_change(self, peripheral_id: str) -> None:
        # Records the record still in place: the hook must run FIRST.
        feedback = self.mesh.feedback_view.get(peripheral_id)
        self.events.append(("before", peripheral_id, _status(feedback)))

    def arm(self, *, unresolved: bool) -> tuple[int, PendingMeshWrite]:
        generation = self.mesh.begin(PID)
        pending = self.mesh.arm(
            PID, generation, {"on": "0"}, PENDING_RPC_RECEIPT, rpc_unresolved=unresolved
        )
        assert pending is not None
        self.events.clear()
        return generation, pending


def _status(record: PendingMeshWrite | None) -> str | None:
    return None if record is None else record.status


def _observed(on: str) -> BrilliantDevice:
    variables = {"on": Variable("on", on)}
    return BrilliantDevice("ble_mesh", PID, "Load", DeviceKind.LIGHT, 27, variables)


async def _turns() -> None:
    for _ in range(6):
        await asyncio.sleep(0)


async def test_hook_runs_before_every_feedback_change() -> None:
    harness = _Harness()
    generation = harness.mesh.begin(PID)
    assert harness.events == [("before", PID, None)]
    harness.events.clear()

    harness.mesh.arm(PID, generation, {"on": "0"}, "receipt")
    harness.mesh.observe(_observed("1"))  # contradiction (resolved RPC)

    assert harness.events == [("before", PID, None), ("before", PID, "pending")]
    assert _status(harness.mesh.feedback_view[PID]) == "contradicted"
    assert harness.mesh.pending_view == {}
    assert harness.mesh.confirm_tasks_view == {}


async def test_begin_supersedes_and_stale_generation_cannot_arm() -> None:
    harness = _Harness()
    old_generation, _pending = harness.arm(unresolved=False)
    resolver = harness.mesh.confirm_tasks_view[PID]

    generation = harness.mesh.begin(PID)
    await _turns()

    assert generation == old_generation + 1 == harness.mesh.generation(PID)
    assert resolver.cancelled()
    assert _status(harness.mesh.feedback_view[PID]) == "superseded"
    assert harness.mesh.arm(PID, old_generation, {"on": "0"}, "late") is None
    assert harness.mesh.pending_view == {}


async def test_views_are_read_only() -> None:
    harness = _Harness()
    harness.arm(unresolved=False)
    for view in (
        harness.mesh.pending_view,
        harness.mesh.confirm_tasks_view,
        harness.mesh.feedback_view,
    ):
        assert isinstance(view, MappingProxyType)
    harness.mesh.drop_all()


async def test_unresolved_rpc_ignores_a_differing_observation() -> None:
    harness = _Harness()
    _generation, pending = harness.arm(unresolved=True)

    harness.mesh.observe(_observed("1"))

    assert harness.mesh.pending_view[PID] is pending
    assert harness.events == []
    harness.mesh.drop_all()


async def test_late_acceptance_rearms_a_full_window() -> None:
    harness = _Harness()
    _generation, pending = harness.arm(unresolved=True)
    await _turns()  # the first resolver starts its window
    harness.clock.advance(1.0)
    harness.mesh.observe(_observed("0"))
    harness.wall = 1010.0

    harness.mesh.on_rpc_settled(PID, pending, "'ok'")
    await _turns()

    rearmed = harness.mesh.pending_view[PID]
    assert rearmed is not pending
    assert (rearmed.receipt, rearmed.deadline) == ("'ok'", 1010.0 + MESH_CONFIRM_SECONDS)
    assert rearmed.last_observed_at is None
    assert not rearmed.rpc_unresolved
    assert harness.events == [("before", PID, "pending"), ("republish", PID, False, "pending")]
    assert harness.sleeper.requested == [MESH_CONFIRM_SECONDS, MESH_CONFIRM_SECONDS]
    harness.mesh.drop_all()


async def test_late_failure_publishes_failed_forced() -> None:
    harness = _Harness()
    _generation, pending = harness.arm(unresolved=True)

    harness.mesh.on_rpc_settled(PID, pending, RuntimeError("late"))
    await _turns()

    assert harness.events == [("before", PID, "pending"), ("republish", PID, True, "failed")]
    assert harness.mesh.pending_view == {}
    assert harness.mesh.confirm_tasks_view == {}


@pytest.mark.parametrize(
    "outcome",
    [asyncio.TimeoutError(), TimeoutError("native"), asyncio.CancelledError(), WriteCancelled()],
)
async def test_inconclusive_outcome_only_clears_the_flag(outcome: BaseException) -> None:
    harness = _Harness()
    _generation, pending = harness.arm(unresolved=True)
    resolver = harness.mesh.confirm_tasks_view[PID]

    harness.mesh.on_rpc_settled(PID, pending, outcome)
    await _turns()

    assert not pending.rpc_unresolved
    assert harness.mesh.confirm_tasks_view[PID] is resolver
    assert pending.deadline == 1000.0 + MESH_CONFIRM_SECONDS
    assert harness.events == []
    harness.mesh.observe(_observed("1"))
    assert _status(harness.mesh.feedback_view[PID]) == "contradicted"


async def test_outcome_for_a_resolved_or_foreign_record_is_ignored() -> None:
    harness = _Harness()
    _generation, pending = harness.arm(unresolved=False)
    harness.mesh.on_rpc_settled(PID, pending, RuntimeError("late"))
    stranger = PendingMeshWrite(targets={}, receipt="", generation=99, rpc_unresolved=True)
    harness.mesh.on_rpc_settled(PID, stranger, "'ok'")
    await _turns()

    assert harness.mesh.pending_view[PID] is pending
    assert harness.events == []
    harness.mesh.drop_all()


async def test_resolver_confirms_with_a_fresh_match() -> None:
    harness = _Harness()
    harness.arm(unresolved=False)
    harness.clock.advance(75.0)
    harness.mesh.observe(_observed("0"))
    harness.clock.advance(5.0)

    await harness.sleeper.release_all()

    assert harness.events == [("before", PID, "pending"), ("echo", PID, (VarSet("on", "0"),))]
    assert harness.mesh.feedback_view == {}
    assert harness.mesh.confirm_tasks_view == {}


async def test_resolver_without_fresh_match_closes_unconfirmed() -> None:
    harness = _Harness()
    harness.arm(unresolved=True)
    harness.clock.advance(80.0)

    await harness.sleeper.release_all()

    assert harness.events == [("before", PID, "pending"), ("republish", PID, False, "unconfirmed")]
    assert harness.mesh.confirm_tasks_view == {}


async def test_revoke_feedback_fences_and_resolver_releases_its_entry() -> None:
    harness = _Harness()
    generation, pending = harness.arm(unresolved=False)

    harness.mesh.revoke_feedback()

    assert not harness.mesh.feedback_enabled
    assert harness.mesh.feedback_owner(PID) == (generation + 1, None)
    assert not harness.mesh.owns(PID, generation, pending)
    await harness.sleeper.release_all()
    assert harness.mesh.confirm_tasks_view == {}
    assert harness.events == []
    harness.mesh.enable_feedback()
    assert harness.mesh.feedback_enabled
