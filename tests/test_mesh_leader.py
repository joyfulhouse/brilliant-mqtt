"""Tests for the MeshLeader election state machine (M11 Step 3).

Everything is driven by an injected FakeClock and FakeMqtt: no sleeps, no
broker. Claims are injected through FakeMqtt.inject exactly as the real
adapter's fan-out would deliver them; ticks advance the machine the way the
run loop does, so every assertion depends only on clock deltas.

Ranking refresher: a (priority, panel) tuple is BETTER when less-than —
lower priority number wins, lexicographically smaller panel name breaks ties.
A claim is live while younger than 3 heartbeats; stale at/after that.
"""

from __future__ import annotations

import json

import pytest

from brilliant_mqtt.mesh_leader import MESH_LEADER_TOPIC, MeshLeader
from tests.fakes import FakeClock, FakeMqtt

PANEL = "office"
HB = 10.0
STALE = 3 * HB


class _Callbacks:
    """Counting on_acquire/on_lose recorders, optionally raising.

    The raising variants prove exception isolation: a failing reconcile or
    withdraw must not wedge the election state machine.
    """

    def __init__(self, *, acquire_raises: bool = False, lose_raises: bool = False) -> None:
        self.acquired = 0
        self.lost = 0
        self._acquire_raises = acquire_raises
        self._lose_raises = lose_raises

    async def on_acquire(self) -> None:
        self.acquired += 1
        if self._acquire_raises:
            raise RuntimeError("reconcile blew up")

    async def on_lose(self) -> None:
        self.lost += 1
        if self._lose_raises:
            raise RuntimeError("withdraw blew up")


def _claim(panel: str, priority: int) -> str:
    return json.dumps({"panel": panel, "priority": priority}, sort_keys=True)


def _claims(mqtt: FakeMqtt) -> list[tuple[str, str, bool]]:
    return [p for p in mqtt.published if p[0] == MESH_LEADER_TOPIC]


def _make(
    mqtt: FakeMqtt,
    clock: FakeClock,
    *,
    panel: str = PANEL,
    priority: int = 1,
    acquire_raises: bool = False,
    lose_raises: bool = False,
) -> tuple[MeshLeader, _Callbacks]:
    cbs = _Callbacks(acquire_raises=acquire_raises, lose_raises=lose_raises)
    leader = MeshLeader(
        mqtt,
        panel,
        priority,
        HB,
        on_acquire=cbs.on_acquire,
        on_lose=cbs.on_lose,
        clock=clock,
    )
    return leader, cbs


async def _drive_to_leader(leader: MeshLeader, clock: FakeClock) -> None:
    """start -> claim -> wait one heartbeat -> confirm: an uncontested election."""
    await leader.start()
    await leader.tick()
    clock.advance(HB)
    await leader.tick()
    assert leader.is_leader


class TestColdStart:
    async def test_start_subscribes_leader_topic(self) -> None:
        mqtt = FakeMqtt()
        leader, _ = _make(mqtt, FakeClock())
        await leader.start()
        assert mqtt.subscriptions == [MESH_LEADER_TOPIC]

    async def test_solo_cold_start_claims_then_acquires(self) -> None:
        mqtt = FakeMqtt()
        clock = FakeClock()
        leader, cbs = _make(mqtt, clock)
        await leader.start()

        await leader.tick()
        assert leader.is_leader is False
        assert cbs.acquired == 0
        assert _claims(mqtt) == [(MESH_LEADER_TOPIC, '{"panel": "office", "priority": 1}', True)]

        clock.advance(HB)
        await leader.tick()
        assert leader.is_leader is True
        assert cbs.acquired == 1
        assert cbs.lost == 0
        # The confirmation publish doubles as the first heartbeat.
        assert len(_claims(mqtt)) == 2

    async def test_own_retained_ghost_does_not_block_claim(self) -> None:
        """A dead previous process leaves OUR retained claim behind: a restart
        must read it as noise, not as a live competitor."""
        mqtt = FakeMqtt()
        clock = FakeClock()
        leader, cbs = _make(mqtt, clock)
        await leader.start()
        await mqtt.inject(MESH_LEADER_TOPIC, _claim(PANEL, 1))

        await leader.tick()
        assert len(_claims(mqtt)) == 1  # claimed immediately
        clock.advance(HB)
        await leader.tick()
        assert leader.is_leader is True
        assert cbs.acquired == 1


class TestHeartbeat:
    async def test_no_republish_before_interval(self) -> None:
        mqtt = FakeMqtt()
        clock = FakeClock()
        leader, _ = _make(mqtt, clock)
        await _drive_to_leader(leader, clock)
        n = len(_claims(mqtt))

        clock.advance(HB / 2)
        await leader.tick()
        assert len(_claims(mqtt)) == n

    async def test_republishes_every_heartbeat(self) -> None:
        mqtt = FakeMqtt()
        clock = FakeClock()
        leader, _ = _make(mqtt, clock)
        await _drive_to_leader(leader, clock)
        n = len(_claims(mqtt))

        for i in range(1, 4):
            clock.advance(HB)
            await leader.tick()
            assert len(_claims(mqtt)) == n + i
        # Every heartbeat is the same retained claim.
        assert _claims(mqtt)[-1] == (
            MESH_LEADER_TOPIC,
            '{"panel": "office", "priority": 1}',
            True,
        )

    async def test_own_echo_while_leader_is_ignored(self) -> None:
        """The broker echoes our own retained heartbeats back to us — they must
        never read as a competitor."""
        mqtt = FakeMqtt()
        clock = FakeClock()
        leader, cbs = _make(mqtt, clock)
        await _drive_to_leader(leader, clock)

        await mqtt.inject(MESH_LEADER_TOPIC, _claim(PANEL, 1))
        await leader.tick()
        assert leader.is_leader is True
        assert cbs.lost == 0


class TestStandby:
    async def test_defers_to_fresh_better_claim(self) -> None:
        mqtt = FakeMqtt()
        clock = FakeClock()
        leader, cbs = _make(mqtt, clock, priority=2)
        await leader.start()
        await mqtt.inject(MESH_LEADER_TOPIC, _claim("kitchen", 1))

        # Tick repeatedly while the claim is still fresh (< 3 heartbeats old).
        await leader.tick()
        clock.advance(HB)
        await leader.tick()
        clock.advance(HB)
        await leader.tick()

        assert _claims(mqtt) == []
        assert leader.is_leader is False
        assert cbs.acquired == 0

    async def test_takes_over_after_better_claim_goes_stale(self) -> None:
        mqtt = FakeMqtt()
        clock = FakeClock()
        leader, cbs = _make(mqtt, clock, priority=2)
        await leader.start()
        await mqtt.inject(MESH_LEADER_TOPIC, _claim("kitchen", 1))

        await leader.tick()
        assert _claims(mqtt) == []  # deferring while fresh

        clock.advance(STALE + 1)
        await leader.tick()  # the dead leader's claim is stale: claim
        assert len(_claims(mqtt)) == 1
        clock.advance(HB)
        await leader.tick()
        assert leader.is_leader is True
        assert cbs.acquired == 1

    async def test_preempts_fresh_worse_claim(self) -> None:
        """Priority 1 outranks a LIVE priority-2 incumbent: preemption returns
        the mesh to the preferred panel after it recovers."""
        mqtt = FakeMqtt()
        clock = FakeClock()
        leader, cbs = _make(mqtt, clock, priority=1)
        await leader.start()
        await mqtt.inject(MESH_LEADER_TOPIC, _claim("kitchen", 2))

        await leader.tick()
        assert len(_claims(mqtt)) == 1  # claimed despite the live worse claim
        clock.advance(HB)
        await leader.tick()
        assert leader.is_leader is True
        assert cbs.acquired == 1

    async def test_tiebreak_defers_to_smaller_panel_name(self) -> None:
        mqtt = FakeMqtt()
        clock = FakeClock()
        leader, cbs = _make(mqtt, clock, panel="office", priority=1)
        await leader.start()
        await mqtt.inject(MESH_LEADER_TOPIC, _claim("attic", 1))

        await leader.tick()
        assert _claims(mqtt) == []
        assert leader.is_leader is False
        assert cbs.acquired == 0

    async def test_tiebreak_preempts_larger_panel_name(self) -> None:
        mqtt = FakeMqtt()
        clock = FakeClock()
        leader, cbs = _make(mqtt, clock, panel="attic", priority=1)
        await leader.start()
        await mqtt.inject(MESH_LEADER_TOPIC, _claim("office", 1))

        await leader.tick()
        assert len(_claims(mqtt)) == 1
        clock.advance(HB)
        await leader.tick()
        assert leader.is_leader is True
        assert cbs.acquired == 1


class TestLeaderStepDown:
    async def test_steps_down_for_fresh_better_claim(self) -> None:
        mqtt = FakeMqtt()
        clock = FakeClock()
        leader, cbs = _make(mqtt, clock, priority=2)
        await _drive_to_leader(leader, clock)

        await mqtt.inject(MESH_LEADER_TOPIC, _claim("kitchen", 1))
        n = len(_claims(mqtt))
        await leader.tick()

        assert leader.is_leader is False
        assert cbs.lost == 1
        # Step-down neither clears nor republishes the retained claim — the
        # better claimant overwrites it.
        assert len(_claims(mqtt)) == n

    async def test_no_heartbeat_after_step_down(self) -> None:
        mqtt = FakeMqtt()
        clock = FakeClock()
        leader, cbs = _make(mqtt, clock, priority=2)
        await _drive_to_leader(leader, clock)
        await mqtt.inject(MESH_LEADER_TOPIC, _claim("kitchen", 1))
        await leader.tick()
        n = len(_claims(mqtt))

        clock.advance(HB)
        await leader.tick()
        clock.advance(HB)
        await leader.tick()

        assert len(_claims(mqtt)) == n  # standby defers: no beats, no claims
        assert leader.is_leader is False
        assert cbs.acquired == 1  # never re-acquired

    async def test_worse_fresh_claim_does_not_dethrone(self) -> None:
        mqtt = FakeMqtt()
        clock = FakeClock()
        leader, cbs = _make(mqtt, clock, priority=1)
        await _drive_to_leader(leader, clock)

        await mqtt.inject(MESH_LEADER_TOPIC, _claim("kitchen", 2))
        await leader.tick()
        assert leader.is_leader is True
        assert cbs.lost == 0


class TestPendingYield:
    async def test_yields_to_better_then_reclaims_after_stale(self) -> None:
        mqtt = FakeMqtt()
        clock = FakeClock()
        leader, cbs = _make(mqtt, clock, priority=2)
        await leader.start()

        await leader.tick()  # t=0: uncontested -> claim -> pending
        assert len(_claims(mqtt)) == 1
        await mqtt.inject(MESH_LEADER_TOPIC, _claim("kitchen", 1))  # better, fresh

        clock.advance(HB)
        await leader.tick()  # t=10: pending sees a live better claim -> yield
        assert leader.is_leader is False
        assert cbs.acquired == 0

        clock.advance(HB)
        await leader.tick()  # t=20: standby still deferring (claim age 20 < 30)
        assert len(_claims(mqtt)) == 1

        clock.advance(HB + 1)
        await leader.tick()  # t=31: the better claim is stale -> claim again
        assert len(_claims(mqtt)) == 2
        clock.advance(HB)
        await leader.tick()  # t=41: confirm
        assert leader.is_leader is True
        assert cbs.acquired == 1


class TestNonParticipant:
    @pytest.mark.parametrize("priority", [0, -3])
    async def test_below_one_fully_inert(self, priority: int) -> None:
        mqtt = FakeMqtt()
        clock = FakeClock()
        leader, cbs = _make(mqtt, clock, priority=priority)
        await leader.start()
        assert mqtt.subscriptions == []

        for _ in range(5):
            await leader.tick()
            clock.advance(HB)

        assert mqtt.published == []
        assert leader.is_leader is False
        assert cbs.acquired == 0


class TestRobustness:
    @pytest.mark.parametrize(
        "payload",
        [
            "not json",
            json.dumps([1, 2, 3]),
            json.dumps("kitchen"),
            json.dumps({}),
            json.dumps({"panel": "kitchen"}),
            json.dumps({"priority": 1}),
            json.dumps({"panel": "kitchen", "priority": "1"}),
            json.dumps({"panel": "kitchen", "priority": 1.5}),
            json.dumps({"panel": "kitchen", "priority": True}),
            json.dumps({"panel": 7, "priority": 1}),
        ],
    )
    async def test_malformed_claim_ignored(self, payload: str) -> None:
        """A malformed claim records nothing: the standby still claims as if
        the topic were empty (a recorded better claim would make it defer)."""
        mqtt = FakeMqtt()
        clock = FakeClock()
        leader, _ = _make(mqtt, clock, priority=2)
        await leader.start()
        await mqtt.inject(MESH_LEADER_TOPIC, payload)

        await leader.tick()
        assert len(_claims(mqtt)) == 1

    async def test_wrong_topic_ignored(self) -> None:
        mqtt = FakeMqtt()
        clock = FakeClock()
        leader, _ = _make(mqtt, clock, priority=2)
        await leader.start()
        # A better claim on the WRONG topic must not register.
        await mqtt.inject("brilliant/office/some_peripheral/state", _claim("kitchen", 1))

        await leader.tick()
        assert len(_claims(mqtt)) == 1  # still claimed: nothing was recorded

    async def test_on_acquire_exception_still_acquires_and_heartbeats(self) -> None:
        mqtt = FakeMqtt()
        clock = FakeClock()
        leader, cbs = _make(mqtt, clock, acquire_raises=True)
        await leader.start()
        await leader.tick()
        clock.advance(HB)
        await leader.tick()  # on_acquire raises — must not wedge the machine

        assert leader.is_leader is True
        assert cbs.acquired == 1
        n = len(_claims(mqtt))
        clock.advance(HB)
        await leader.tick()
        assert len(_claims(mqtt)) == n + 1  # heartbeats continue normally

    async def test_on_lose_exception_still_steps_down(self) -> None:
        mqtt = FakeMqtt()
        clock = FakeClock()
        leader, cbs = _make(mqtt, clock, priority=2, lose_raises=True)
        await _drive_to_leader(leader, clock)

        await mqtt.inject(MESH_LEADER_TOPIC, _claim("kitchen", 1))
        await leader.tick()  # on_lose raises — the step-down must still happen

        assert leader.is_leader is False
        assert cbs.lost == 1
