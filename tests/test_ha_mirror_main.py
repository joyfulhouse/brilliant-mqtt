"""Tests for the HA mirror's supervised, leader-gated entrypoint."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import brilliant_ha_mirror.__main__ as main_module
from brilliant_ha_mirror.config import Settings
from brilliant_ha_mirror.mapping import HaEntity
from brilliant_ha_mirror.protocols import HaClient, PeripheralHostClient
from brilliant_mqtt.protocols import MqttClient
from tests.fakes import FakeClock, FakeHaClient, FakeMqtt, FakePeripheralHost


class FakeLeader:
    """Scripted leader that fires transitions or raises from ``tick``."""

    def __init__(
        self,
        on_acquire: Callable[[], Awaitable[None]],
        on_lose: Callable[[], Awaitable[None]],
        actions: list[str | Exception],
    ) -> None:
        self._on_acquire = on_acquire
        self._on_lose = on_lose
        self._actions = list(actions)
        self.started = False
        self.tick_count = 0

    async def start(self) -> None:
        self.started = True

    async def tick(self) -> None:
        action = self._actions.pop(0)
        self.tick_count += 1
        if isinstance(action, Exception):
            raise action
        if action == "acquire":
            await self._on_acquire()
        elif action == "lose":
            await self._on_lose()


class FakeLeaderFactory:
    """Build one FakeLeader per supervised session from scripted actions."""

    def __init__(self, scripts: list[list[str | Exception]]) -> None:
        self._scripts = list(scripts)
        self.leaders: list[FakeLeader] = []

    def __call__(
        self,
        mqtt: MqttClient,
        on_acquire: Callable[[], Awaitable[None]],
        on_lose: Callable[[], Awaitable[None]],
    ) -> FakeLeader:
        del mqtt
        leader = FakeLeader(on_acquire, on_lose, self._scripts.pop(0))
        self.leaders.append(leader)
        return leader

    @property
    def total_ticks(self) -> int:
        return sum(leader.tick_count for leader in self.leaders)


class AdapterFactory:
    """Record every fresh fake adapter constructed by the supervisor."""

    def __init__(self, entities: list[HaEntity]) -> None:
        self._entities = entities
        self.ha_clients: list[FakeHaClient] = []
        self.hosts: list[FakePeripheralHost] = []

    def make_ha(self) -> HaClient:
        client = FakeHaClient(self._entities)
        self.ha_clients.append(client)
        return client

    def make_host(self) -> PeripheralHostClient:
        host = FakePeripheralHost()
        self.hosts.append(host)
        return host


def _settings() -> Settings:
    return Settings(
        panel="office",
        ha_ws_url="ws://ha.local:8123/api/websocket",
        ha_token="token",
        leader_priority=1,
        leader_heartbeat_seconds=10.0,
    )


def _adapters() -> AdapterFactory:
    return AdapterFactory(
        [
            HaEntity(
                entity_id="light.desk",
                state="on",
                attributes={"friendly_name": "Desk Light", "brightness": 128},
                area="Office",
            )
        ]
    )


class FakeMqttFactory:
    """Record every fresh MQTT adapter the supervisor builds per session."""

    def __init__(self) -> None:
        self.created: list[FakeMqtt] = []

    def __call__(self) -> FakeMqtt:
        mqtt = FakeMqtt()
        self.created.append(mqtt)
        return mqtt


async def _run_until_ticks(
    leader_factory: FakeLeaderFactory,
    adapters: AdapterFactory,
    tick_limit: int,
    mqtt_factory: FakeMqttFactory | None = None,
) -> list[float]:
    clock = FakeClock()
    sleeps: list[float] = []

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock.advance(seconds)

    await main_module.run(
        _settings(),
        ha_factory=adapters.make_ha,
        host_factory=adapters.make_host,
        mqtt_factory=mqtt_factory or FakeMqttFactory(),
        leader_factory=leader_factory,
        clock=clock,
        sleep=sleep,
        should_continue=lambda: leader_factory.total_ticks < tick_limit,
    )
    return sleeps


async def test_each_session_builds_and_disconnects_its_own_mqtt() -> None:
    # Two sessions (a failure forces a rebuild). Each must get its OWN fresh
    # MQTT adapter and disconnect it — a shared process-lifetime adapter would
    # leak a MeshLeader election callback per reconnect (review finding I2).
    adapters = _adapters()
    mqtt_factory = FakeMqttFactory()
    leaders = FakeLeaderFactory(
        [
            ["acquire", RuntimeError("transient failure")],
            ["acquire"],
        ]
    )
    await _run_until_ticks(leaders, adapters, 3, mqtt_factory=mqtt_factory)

    assert len(mqtt_factory.created) == 2
    for mqtt in mqtt_factory.created:
        assert mqtt.connect_count == 1
        assert mqtt.disconnect_count == 1


async def test_acquire_starts_mirror_and_registers_labeled_entities() -> None:
    adapters = _adapters()
    leaders = FakeLeaderFactory([["acquire"]])

    await _run_until_ticks(leaders, adapters, 1)

    assert leaders.leaders[0].started
    assert adapters.hosts[0].registered == ["HA Desk Light"]


async def test_leadership_loss_deletes_hosted_peripherals() -> None:
    adapters = _adapters()
    leaders = FakeLeaderFactory([["acquire", "lose"]])

    await _run_until_ticks(leaders, adapters, 2)

    assert adapters.hosts[0].deleted == ["HA Desk Light"]


async def test_tick_failure_backs_off_rebuilds_and_continues() -> None:
    adapters = _adapters()
    leaders = FakeLeaderFactory(
        [
            ["acquire", RuntimeError("transient broker failure")],
            ["acquire"],
        ]
    )

    sleeps = await _run_until_ticks(leaders, adapters, 3)

    assert main_module._BACKOFF_SECONDS in sleeps
    assert len(leaders.leaders) == 2
    assert len(adapters.ha_clients) == 2
    assert len(adapters.hosts) == 2
    assert adapters.hosts[0].deleted == ["HA Desk Light"]
    assert adapters.hosts[1].registered == ["HA Desk Light"]


async def test_dead_ha_reader_triggers_session_rebuild() -> None:
    # When the leader's HA connection dies silently (reader task ended, e.g. HA
    # restarted), the supervisor must detect it and rebuild the session — the
    # leader/MQTT path alone would never surface it (review finding I1/#1).
    adapters = _adapters()
    clock = FakeClock()
    sleeps: list[float] = []
    leaders = FakeLeaderFactory([["acquire", "idle"], ["acquire"]])

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock.advance(seconds)
        # Kill the first session's HA connection during its post-acquire sleep;
        # the next tick's liveness check should raise and force a rebuild.
        if len(sleeps) == 1:
            adapters.ha_clients[0].running = False

    await main_module.run(
        _settings(),
        ha_factory=adapters.make_ha,
        host_factory=adapters.make_host,
        mqtt_factory=FakeMqttFactory(),
        leader_factory=leaders,
        clock=clock,
        sleep=sleep,
        should_continue=lambda: leaders.total_ticks < 3,
    )

    # Session 1's HA client + host were torn down and a fresh pair built.
    assert len(adapters.ha_clients) == 2
    assert len(adapters.hosts) == 2
    assert adapters.hosts[0].deleted == ["HA Desk Light"]  # stale mirror cleaned up
    assert main_module._BACKOFF_SECONDS in sleeps  # backed off before rebuild
