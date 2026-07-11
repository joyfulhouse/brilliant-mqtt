"""Tests for the HA mirror's distinct-topic leader election."""

from __future__ import annotations

import json

import pytest

from brilliant_ha_mirror.leader import HA_MIRROR_LEADER_TOPIC, MirrorLeader
from brilliant_mqtt.mesh_leader import MESH_LEADER_TOPIC
from tests.fakes import FakeClock, FakeMqtt

PANEL = "office"
HB = 10.0


class _Callbacks:
    def __init__(self) -> None:
        self.acquired = 0
        self.lost = 0

    async def on_acquire(self) -> None:
        self.acquired += 1

    async def on_lose(self) -> None:
        self.lost += 1


def _claim(panel: str, priority: int) -> str:
    return json.dumps({"panel": panel, "priority": priority}, sort_keys=True)


def _claims(mqtt: FakeMqtt) -> list[tuple[str, str, bool]]:
    return [published for published in mqtt.published if published[0] == HA_MIRROR_LEADER_TOPIC]


def _make(
    mqtt: FakeMqtt,
    clock: FakeClock,
    *,
    panel: str = PANEL,
    priority: int = 1,
) -> tuple[MirrorLeader, _Callbacks]:
    callbacks = _Callbacks()
    leader = MirrorLeader(
        mqtt,
        panel,
        priority,
        HB,
        on_acquire=callbacks.on_acquire,
        on_lose=callbacks.on_lose,
        clock=clock,
    )
    return leader, callbacks


async def _drive_to_leader(leader: MirrorLeader, clock: FakeClock) -> None:
    await leader.start()
    await leader.tick()
    clock.advance(HB)
    await leader.tick()
    assert leader.is_leader


async def test_solo_participant_acquires_after_confirmation_heartbeat() -> None:
    mqtt = FakeMqtt()
    clock = FakeClock()
    leader, callbacks = _make(mqtt, clock)
    await leader.start()

    await leader.tick()
    assert leader.is_leader is False
    assert callbacks.acquired == 0
    assert _claims(mqtt) == [(HA_MIRROR_LEADER_TOPIC, '{"panel": "office", "priority": 1}', True)]

    clock.advance(HB)
    await leader.tick()
    assert leader.is_leader is True
    assert callbacks.acquired == 1
    assert len(_claims(mqtt)) == 2


@pytest.mark.parametrize("priority", [0, -1])
async def test_priority_below_one_never_participates(priority: int) -> None:
    mqtt = FakeMqtt()
    clock = FakeClock()
    leader, callbacks = _make(mqtt, clock, priority=priority)
    await leader.start()

    for _ in range(3):
        await leader.tick()
        clock.advance(HB)

    assert mqtt.subscriptions == []
    assert mqtt.published == []
    assert leader.is_leader is False
    assert callbacks.acquired == 0


async def test_lower_priority_number_claim_preempts_leader() -> None:
    mqtt = FakeMqtt()
    clock = FakeClock()
    leader, callbacks = _make(mqtt, clock, priority=2)
    await _drive_to_leader(leader, clock)

    await mqtt.inject(HA_MIRROR_LEADER_TOPIC, _claim("kitchen", 1))
    await leader.tick()

    assert leader.is_leader is False
    assert callbacks.lost == 1


async def test_higher_priority_number_yields_to_lower_claim() -> None:
    mqtt = FakeMqtt()
    clock = FakeClock()
    leader, callbacks = _make(mqtt, clock, priority=2)
    await leader.start()
    await mqtt.inject(HA_MIRROR_LEADER_TOPIC, _claim("kitchen", 1))

    await leader.tick()
    clock.advance(HB)
    await leader.tick()

    assert _claims(mqtt) == []
    assert leader.is_leader is False
    assert callbacks.acquired == 0


async def test_claims_use_mirror_topic_and_ignore_mesh_topic() -> None:
    mqtt = FakeMqtt()
    clock = FakeClock()
    leader, _ = _make(mqtt, clock, priority=2)
    await leader.start()
    await mqtt.inject(MESH_LEADER_TOPIC, _claim("kitchen", 1))

    await leader.tick()

    assert mqtt.subscriptions == [HA_MIRROR_LEADER_TOPIC]
    assert len(_claims(mqtt)) == 1
    assert all(topic != MESH_LEADER_TOPIC for topic, _, _ in mqtt.published)
