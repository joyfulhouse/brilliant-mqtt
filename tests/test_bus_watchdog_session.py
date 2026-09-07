"""Cross-component regressions from bridge startup phase to reboot decisions."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import cast

import pytest

import brilliant_mqtt.__main__ as main_mod
from brilliant_bus_watchdog.health import bus_failure_age, heartbeat_age
from brilliant_bus_watchdog.reboot_guard import GuardPolicy, RebootGuard
from brilliant_bus_watchdog.run import handle, should_reboot
from brilliant_mqtt import heartbeat
from brilliant_mqtt.bridge import Bridge
from brilliant_mqtt.config import Settings
from brilliant_mqtt.heartbeat import BusPhase, write_heartbeat, write_phase
from brilliant_mqtt.model import BrilliantDevice
from brilliant_mqtt.retained_topics import RetainedLedgerError
from tests.fakes import FakeBus, FakeClock, FakeMqtt


class _Bus:
    def __init__(
        self,
        start_error: Exception | None = None,
        *,
        block_start_until: asyncio.Event | None = None,
        entered_start: asyncio.Event | None = None,
        read_error: Exception | None = None,
    ) -> None:
        self.start_error = start_error
        self.start_calls = 0
        self.read_calls = 0
        self.read_error = read_error
        # When set, start() parks on this event (a bus still handshaking), and
        # signals entered_start once it has been reached — so a test can inspect
        # the phase/heartbeat mid-handshake.
        self._block_start_until = block_start_until
        self._entered_start = entered_start

    def on_reconnect(self, callback: Callable[[], Awaitable[None]]) -> None:
        del callback

    def on_change(
        self,
        callback: Callable[[BrilliantDevice], Awaitable[None]],
        *,
        coalesce_pushes: bool = True,
        want_device: Callable[[str], bool] | None = None,
    ) -> None:
        del callback, coalesce_pushes, want_device

    async def start(self) -> None:
        self.start_calls += 1
        if self._entered_start is not None:
            self._entered_start.set()
        if self._block_start_until is not None:
            await self._block_start_until.wait()
        if self.start_error is not None:
            raise self.start_error

    async def get_all(self) -> list[BrilliantDevice]:
        self.read_calls += 1
        if self.read_error is not None:
            raise self.read_error
        return []

    async def shutdown(self) -> None:
        return


class _Mqtt:
    def __init__(
        self,
        connect_error: Exception | None = None,
        subscribe_error: Exception | None = None,
    ) -> None:
        self.connect_error = connect_error
        self.subscribe_error = subscribe_error
        self.connect_calls = 0
        self.subscribe_calls = 0

    async def connect(self) -> None:
        self.connect_calls += 1
        if self.connect_error is not None:
            raise self.connect_error

    async def disconnect(self) -> None:
        return

    async def publish(
        self,
        topic: str,
        payload: str,
        retain: bool = False,
        qos: int = 0,
    ) -> None:
        del topic, payload, retain, qos

    def on_command(self, callback: Callable[[str, str], Awaitable[None]]) -> None:
        del callback

    def on_message(self, callback: Callable[[str, str, bool], Awaitable[None]]) -> None:
        del callback

    async def subscribe(self, topic: str) -> None:
        del topic
        self.subscribe_calls += 1
        if self.subscribe_error is not None:
            raise self.subscribe_error


class _NoopBridge:
    def __init__(self, *args: object, **kwargs: object) -> None:
        del args, kwargs

    async def reconcile(self) -> None:
        return

    async def withdraw(self) -> None:
        return


class _ReadOnceBridge:
    def __init__(self, *args: object, **kwargs: object) -> None:
        self._bus = cast(_Bus, args[0])
        self._heartbeat = cast(Callable[[], None], kwargs["heartbeat"])

    async def reconcile(self) -> None:
        await self._bus.get_all()
        self._heartbeat()
        raise asyncio.CancelledError


class _ReadingBridge:
    def __init__(self, *args: object, **kwargs: object) -> None:
        self._bus = cast(_Bus, args[0])
        self._heartbeat = cast(Callable[[], None], kwargs["heartbeat"])

    async def reconcile(self) -> None:
        await self._bus.get_all()
        self._heartbeat()

    async def withdraw(self) -> None:
        return


def _settings(
    tmp_path: Path,
    mesh_priority: int = 0,
    *,
    scene_enabled: bool = False,
    scene_watermark: Path | None = None,
) -> Settings:
    return Settings(
        panel="office",
        mqtt_host="broker",
        mqtt_username="user",
        mqtt_password="password",
        retained_topics_file=str(tmp_path / "owned-topics.json"),
        bus_heartbeat_file=str(tmp_path / "bus-heartbeat"),
        bus_phase_file=str(tmp_path / "bus-phase"),
        mesh_priority=mesh_priority,
        scene_bridge_enabled=scene_enabled,
        scene_watermark_file=str(scene_watermark or (tmp_path / "scene-watermarks.json")),
    )


def _seed(path: str, contents: str) -> None:
    """Write a runtime file synchronously (setup helper; keeps blocking file
    I/O out of the async test bodies, per ruff ASYNC240)."""
    Path(path).write_text(contents, encoding="utf-8")


def _install_session_fakes(
    monkeypatch: pytest.MonkeyPatch,
    bus: _Bus,
    mqtt: _Mqtt,
    bridge: type[_NoopBridge] | type[_ReadOnceBridge] | type[_ReadingBridge] | type[Bridge] = (
        _NoopBridge
    ),
) -> None:
    monkeypatch.setattr(main_mod, "RpcBusAdapter", lambda **kwargs: bus)
    monkeypatch.setattr(main_mod, "AioMqttAdapter", lambda settings: mqtt)
    monkeypatch.setattr(main_mod, "Bridge", bridge)


def _install_watchdog_clock(monkeypatch: pytest.MonkeyPatch, clock: FakeClock) -> None:
    def _write_phase(
        path: str,
        phase: BusPhase,
        *,
        bus_read_succeeded: bool = False,
    ) -> None:
        write_phase(
            path,
            phase,
            bus_read_succeeded=bus_read_succeeded,
            monotonic_clock=clock,
        )

    def _write_heartbeat(path: str, ignored_clock: Callable[[], float]) -> None:
        del ignored_clock
        write_heartbeat(path, lambda: clock() + 100.0, clock)

    monkeypatch.setattr(main_mod, "write_phase", _write_phase)
    monkeypatch.setattr(main_mod, "write_heartbeat", _write_heartbeat)


async def test_broker_outage_never_qualifies_as_a_bus_wedge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bus = _Bus()
    mqtt = _Mqtt(ConnectionError("broker refused"))
    settings = _settings(tmp_path)
    _install_session_fakes(monkeypatch, bus, mqtt)

    for _ in range(3):
        with pytest.raises(ConnectionError, match="broker refused"):
            await main_mod._run_session(settings, None, None)

    failure_age = bus_failure_age(settings.bus_phase_file, now=time.monotonic())
    assert bus.start_calls == 0
    assert mqtt.connect_calls == 3
    assert failure_age is None
    assert not should_reboot(
        age=1900.0,
        stale_after=1800.0,
        bridge_active=True,
        gateway_up=True,
        bus_failure_age=failure_age,
    )


async def test_stale_bus_phase_and_heartbeat_do_not_reboot_on_broker_outage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A prior HEALTHY session leaves "bus" and a fresh heartbeat on disk;
    later the heartbeat goes stale while the broker is down. Today's
    broker-only outage must NOT read those leftovers as a bus wedge: entering
    _run_session stamps "pre_bus" BEFORE mqtt.connect(), so once the broker
    refusal short-circuits startup, bus_failure_age returns None even though the
    stale heartbeat age alone would otherwise qualify. Unlike its empty-file
    siblings above, this seeds the fail-unsafe leftovers explicitly so the
    test would catch a startup that wrote a fake heartbeat or let a stale
    "bus" marker survive the refusal."""
    settings = _settings(tmp_path)
    # Leftovers from a prior successful session, before this outage begins. The
    # phase carries THIS (live) process's pid so it would read as confirmed on
    # its own — proving the pre_bus stamp, not a dead-writer check, is what
    # clears it.
    write_phase(
        settings.bus_phase_file,
        "bus",
        bus_read_succeeded=True,
        monotonic_clock=lambda: 100.0,
    )
    _seed(settings.bus_heartbeat_file, "100.0")

    bus = _Bus()
    mqtt = _Mqtt(ConnectionError("broker refused"))
    _install_session_fakes(monkeypatch, bus, mqtt)

    with pytest.raises(ConnectionError, match="broker refused"):
        await main_mod._run_session(settings, None, None)

    failure_age = bus_failure_age(settings.bus_phase_file, now=time.monotonic())
    age = heartbeat_age(settings.bus_heartbeat_file, now=100_000.0, started_at=0.0)
    assert bus.start_calls == 0  # the local bus was never reached
    assert mqtt.connect_calls == 1
    # The pre_bus stamp at session entry overwrote the stale "bus" marker...
    assert failure_age is None
    # ...and the seeded heartbeat is genuinely stale, so this test proves it is
    # missing bus-failure attribution (not a fresh heartbeat) that holds the reboot back.
    assert age >= 1800.0
    assert not should_reboot(
        age=age,
        stale_after=1800.0,
        bridge_active=True,
        gateway_up=True,
        bus_failure_age=failure_age,
    )


async def test_failed_pre_bus_restamp_does_not_reboot_on_broker_outage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Issue #87 regression, the in-process retry path: run() retries
    _run_session in the SAME process and teardown deliberately keeps the "bus"
    marker, so a leftover marker carries THIS still-live pid. If session N's
    pre_bus re-stamp FAILS (ENOSPC/EROFS/perm on the /run tmpfs) and is merely
    swallowed, that live-pid "bus" marker stays readable and could retain
    attribution, so a stale heartbeat during a broker-only outage could
    reboots a healthy panel in a loop. Proving the failed stamp actively clears
    the marker: seed a live-pid "bus" + stale heartbeat, make the pre_bus stamp
    fail, and assert the outage does NOT qualify as a bus wedge."""
    settings = _settings(tmp_path)
    write_phase(settings.bus_phase_file, "bus", bus_read_succeeded=True)
    _seed(settings.bus_heartbeat_file, "100.0")

    def _fail_stamp(*args: object, **kwargs: object) -> None:
        raise OSError(28, "No space left on device")

    monkeypatch.setattr("brilliant_mqtt.heartbeat._atomic_write", _fail_stamp)

    bus = _Bus()
    mqtt = _Mqtt(ConnectionError("broker refused"))
    _install_session_fakes(monkeypatch, bus, mqtt)

    with pytest.raises(ConnectionError, match="broker refused"):
        await main_mod._run_session(settings, None, None)

    failure_age = bus_failure_age(settings.bus_phase_file, now=time.monotonic())
    age = heartbeat_age(settings.bus_heartbeat_file, now=100_000.0, started_at=0.0)
    assert bus.start_calls == 0  # the local bus was never reached
    # The failed pre_bus stamp cleared the live-pid "bus" leftover...
    assert failure_age is None
    # ...and the heartbeat is genuinely stale, so it is the cleared marker (not
    # a fresh heartbeat) that holds the reboot back.
    assert age >= 1800.0
    assert not should_reboot(
        age=age,
        stale_after=1800.0,
        bridge_active=True,
        gateway_up=True,
        bus_failure_age=failure_age,
    )


async def test_failed_phase_write_and_unlink_do_not_reboot_on_broker_outage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = _settings(tmp_path)
    write_phase(
        settings.bus_phase_file,
        "bus",
        bus_read_succeeded=True,
        monotonic_clock=lambda: 100.0,
    )
    _seed(settings.bus_heartbeat_file, "100.0")

    def _denied(*args: object, **kwargs: object) -> None:
        raise PermissionError("runtime directory is read-only")

    monkeypatch.setattr("brilliant_mqtt.heartbeat._atomic_write", _denied)
    monkeypatch.setattr("brilliant_mqtt.heartbeat.os.unlink", _denied)
    bus = _Bus()
    mqtt = _Mqtt(ConnectionError("broker refused"))
    _install_session_fakes(monkeypatch, bus, mqtt)

    with caplog.at_level(logging.WARNING, logger="brilliant_mqtt.heartbeat"):
        with pytest.raises(ConnectionError, match="broker refused"):
            await main_mod._run_session(settings, None, None)

    failure_age = bus_failure_age(settings.bus_phase_file, now=1900.0)
    assert await asyncio.to_thread(Path(settings.bus_phase_file).is_file)
    assert not should_reboot(
        age=1900.0,
        stale_after=1800.0,
        bridge_active=True,
        gateway_up=True,
        bus_failure_age=failure_age,
    )
    messages = [record.getMessage() for record in caplog.records]
    assert any("invalidation failed" in message for message in messages)
    assert not any("reboot guard disabled" in message for message in messages)


async def test_mesh_election_failure_stays_pre_bus_despite_mqtt_connect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A mesh-election join failure is an MQTT/mesh-side startup failure, not
    a bus failure — it must not leave the phase stamped "bus" even though
    mqtt.connect() already succeeded (issue #87 acceptance: distinguish
    explicit local bus failure from inability to reach MQTT)."""
    bus = _Bus()
    mqtt = _Mqtt(subscribe_error=ConnectionError("mesh claim subscribe failed"))
    settings = _settings(tmp_path, mesh_priority=1)
    _install_session_fakes(monkeypatch, bus, mqtt)

    with pytest.raises(ConnectionError, match="mesh claim subscribe failed"):
        await main_mod._run_session(settings, None, None)

    failure_age = bus_failure_age(settings.bus_phase_file, now=time.monotonic())
    assert mqtt.connect_calls == 1
    assert mqtt.subscribe_calls == 1
    assert bus.start_calls == 0
    assert failure_age is None
    assert not should_reboot(
        age=99_999.0,
        stale_after=1800.0,
        bridge_active=True,
        gateway_up=True,
        bus_failure_age=failure_age,
    )


async def test_retained_ledger_failure_stays_pre_bus(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ledger_error = RetainedLedgerError("invalid retained ledger")

    class _FailingLedger:
        def __init__(self, panel: str, path: Path) -> None:
            del self, panel, path

        async def async_load(self) -> None:
            raise ledger_error

    bus = _Bus()
    mqtt = _Mqtt()
    settings = _settings(tmp_path)
    _install_session_fakes(monkeypatch, bus, mqtt)
    monkeypatch.setattr(main_mod, "RetainedTopicLedger", _FailingLedger)

    with pytest.raises(RetainedLedgerError) as raised:
        await main_mod._run_session(settings, None, None)

    failure_age = bus_failure_age(settings.bus_phase_file, now=time.monotonic())
    assert raised.value is ledger_error
    assert bus.start_calls == 0
    assert mqtt.connect_calls == 1  # diagnostic publish only
    assert failure_age is None
    assert not should_reboot(
        age=99_999.0,
        stale_after=1800.0,
        bridge_active=True,
        gateway_up=True,
        bus_failure_age=failure_age,
    )


async def test_sustained_bus_handshake_failure_still_uses_reboot_guard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = FakeClock()
    _install_watchdog_clock(monkeypatch, clock)
    bus = _Bus(ConnectionError("bus handshake failed"))
    mqtt = _Mqtt()
    settings = _settings(tmp_path)
    _install_session_fakes(monkeypatch, bus, mqtt)

    for attempt in range(7):
        with pytest.raises(ConnectionError, match="bus handshake failed"):
            await main_mod._run_session(settings, None, None)
        if attempt < 6:
            clock.advance(301.0)

    failure_age = bus_failure_age(settings.bus_phase_file, now=clock())
    age = heartbeat_age(settings.bus_heartbeat_file, now=1900.0, started_at=0.0)
    decision = should_reboot(
        age=age,
        stale_after=1800.0,
        # Assumes the systemd supervisor's restart backoff keeps
        # brilliant-mqtt.service reported "active" through repeated
        # in-process bus-handshake failures — see the finally-block comment
        # in __main__.py about backoff being far shorter than stale_after.
        bridge_active=True,
        gateway_up=True,
        bus_failure_age=failure_age,
    )
    assert bus.start_calls == 7
    assert mqtt.connect_calls == 7
    assert failure_age == 1806.0
    assert decision is True

    reboots: list[str] = []
    guard = RebootGuard(
        str(tmp_path / "reboot-guard.json"),
        # cap=2 leaves headroom so the second decision's suppression can only be
        # the cooldown, not the cap; a non-zero cooldown makes that gate real.
        GuardPolicy(cooldown=300.0, cap=2, window=21_600.0),
    )
    # First qualifying decision reboots and records the stamp.
    handle(should=decision, guard=guard, now=1900.0, reboot_fn=lambda: reboots.append("reboot"))
    # 100s later — inside the 300s cooldown — the cap (2) would still allow a
    # reboot, so this suppression proves the cooldown is what gates it.
    handle(should=decision, guard=guard, now=2000.0, reboot_fn=lambda: reboots.append("reboot"))
    assert reboots == ["reboot"]
    # Once the cooldown elapses (400s > 300s) and the cap still has headroom, the
    # next decision reboots again — confirming the cooldown, not the cap, held
    # the second one back.
    handle(should=decision, guard=guard, now=2300.0, reboot_fn=lambda: reboots.append("reboot"))
    assert reboots == ["reboot", "reboot"]


async def test_successful_bus_read_refreshes_heartbeat_without_resetting_phase(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bus = _Bus()
    mqtt = _Mqtt()
    settings = _settings(tmp_path)
    _install_session_fakes(monkeypatch, bus, mqtt, _ReadOnceBridge)

    with pytest.raises(asyncio.CancelledError):
        await main_mod._run_session(settings, None, None)

    failure_age = bus_failure_age(settings.bus_phase_file)
    age = heartbeat_age(settings.bus_heartbeat_file, now=time.time(), started_at=0.0)
    assert bus.start_calls == 1
    assert bus.read_calls == 1
    assert failure_age is not None
    assert 0.0 <= failure_age < 1.0
    assert 0.0 <= age < 1.0
    assert not should_reboot(
        age=age,
        stale_after=1800.0,
        bridge_active=True,
        gateway_up=True,
        bus_failure_age=failure_age,
    )


async def test_broker_recovery_bus_start_window_does_not_reboot_healthy_panel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A recovered broker cannot lend outage age to a fresh bus attempt."""
    settings = _settings(tmp_path)
    clock = FakeClock()
    _install_watchdog_clock(monkeypatch, clock)
    _seed(settings.bus_heartbeat_file, "100.0")

    outage_bus = _Bus()
    outage_mqtt = _Mqtt(ConnectionError("broker refused"))
    _install_session_fakes(monkeypatch, outage_bus, outage_mqtt)
    with pytest.raises(ConnectionError, match="broker refused"):
        await main_mod._run_session(settings, None, None)
    clock.advance(1900.0)

    stale_age = heartbeat_age(
        settings.bus_heartbeat_file,
        now=clock() + 100.0,
        started_at=0.0,
    )
    assert stale_age == 1900.0
    assert not should_reboot(
        age=stale_age,
        stale_after=1800.0,
        bridge_active=True,
        gateway_up=True,
        bus_failure_age=bus_failure_age(settings.bus_phase_file, now=clock()),
    )

    entered_start = asyncio.Event()
    still_handshaking = asyncio.Event()
    bus = _Bus(block_start_until=still_handshaking, entered_start=entered_start)
    mqtt = _Mqtt()  # the broker has recovered: connect succeeds
    _install_session_fakes(monkeypatch, bus, mqtt, _ReadingBridge)

    task = asyncio.create_task(main_mod._run_session(settings, None, None))
    try:
        await asyncio.wait_for(entered_start.wait(), timeout=1)
        failure_age = bus_failure_age(settings.bus_phase_file, now=clock())
        assert bus.start_calls == 1  # inside bus.start(); first read not yet reached
        assert failure_age == 0.0
        assert not should_reboot(
            age=stale_age,
            stale_after=1800.0,
            bridge_active=True,
            gateway_up=True,
            bus_failure_age=failure_age,
        )

        still_handshaking.set()
        for _ in range(100):
            if bus.read_calls == 1:
                break
            await asyncio.sleep(0)
        assert bus.read_calls == 1
        fresh_age = heartbeat_age(
            settings.bus_heartbeat_file,
            now=clock() + 100.0,
            started_at=0.0,
        )
        assert fresh_age == 0.0
        assert bus_failure_age(settings.bus_phase_file, now=clock()) == 0.0
        assert not should_reboot(
            age=fresh_age,
            stale_after=1800.0,
            bridge_active=True,
            gateway_up=True,
            bus_failure_age=bus_failure_age(settings.bus_phase_file, now=clock()),
        )
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_scene_subscribe_failure_never_reboots_a_healthy_panel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Scene subscription failures cannot stale a healthy local bus."""
    settings = _settings(tmp_path, scene_enabled=True)
    clock = FakeClock()
    _install_watchdog_clock(monkeypatch, clock)
    _seed(settings.bus_heartbeat_file, "100.0")

    bus = _Bus()
    mqtt = _Mqtt(subscribe_error=TimeoutError("scene subscribe timed out"))
    _install_session_fakes(monkeypatch, bus, mqtt, Bridge)

    decisions: list[bool] = []
    for attempt in range(31):
        with pytest.raises(TimeoutError, match="scene subscribe timed out"):
            await main_mod._run_session(settings, None, None)
        age = heartbeat_age(
            settings.bus_heartbeat_file,
            now=clock() + 100.0,
            started_at=0.0,
        )
        decisions.append(
            should_reboot(
                age=age,
                stale_after=1800.0,
                bridge_active=True,
                gateway_up=True,
                bus_failure_age=bus_failure_age(settings.bus_phase_file, now=clock()),
            )
        )
        if attempt < 30:
            clock.advance(60.0)

    age = heartbeat_age(
        settings.bus_heartbeat_file,
        now=clock() + 100.0,
        started_at=0.0,
    )
    assert clock() == 1800.0
    assert decisions == [False] * 31
    assert bus.start_calls == 31
    assert bus.read_calls == 31
    assert age < 1800.0


async def test_failed_initial_bus_read_does_not_write_success_heartbeat_or_phase(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _settings(tmp_path)
    clock = FakeClock()
    _install_watchdog_clock(monkeypatch, clock)
    bus = _Bus(read_error=ConnectionError("panel read failed"))
    mqtt = _Mqtt()
    _install_session_fakes(monkeypatch, bus, mqtt, Bridge)

    with pytest.raises(ConnectionError, match="panel read failed"):
        await main_mod._run_session(settings, None, None)

    assert bus.read_calls == 1
    assert not await asyncio.to_thread(Path(settings.bus_heartbeat_file).exists)
    record = await asyncio.to_thread(heartbeat.read_phase_record, settings.bus_phase_file)
    assert record is not None
    assert record.bus_read_succeeded is False


class _AvailabilityPublishFailsMqtt(FakeMqtt):
    """A broker that accepts everything except the retained availability PUBLISH
    — models a broker that CONNECTs but rejects/hangs the first publish (an ACL
    denial, or a QoS-1 PUBACK that never arrives)."""

    async def publish(self, topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        if topic.endswith("/availability"):
            raise ConnectionError("broker rejected the availability publish")
        await super().publish(topic, payload, retain, qos)


async def test_reconcile_beats_before_broker_publish(tmp_path: Path) -> None:
    """DEFECT #2 root cause (issue #87 audit follow-up): ``Bridge.reconcile``
    must stamp the liveness heartbeat BEFORE its first broker-dependent publish.

    Reconcile's first act is a retained availability PUBLISH. A broker that
    CONNECTs but rejects or hangs that publish makes reconcile raise; if the beat
    came after the publish, no heartbeat is ever written and the heartbeat goes
    stale while the phase reads "bus" — the bus watchdog then reboots a HEALTHY
    panel over a broker-only fault. The fix reads the bus and beats FIRST, so a
    broker-only publish failure leaves the heartbeat fresh.

    Uses the REAL ``Bridge`` (not a fake) so it exercises reconcile's true
    ordering; it fails without the beat-first fix and passes with it.
    """
    heartbeat_file = str(tmp_path / "bus-heartbeat")
    # A prior healthy session's heartbeat; only a reconcile beat can refresh it.
    _seed(heartbeat_file, "100.0")

    def _beat() -> None:
        write_heartbeat(heartbeat_file, time.time)

    bus = FakeBus([])  # the bus answers a read fine
    mqtt = _AvailabilityPublishFailsMqtt()
    bridge = Bridge(bus, mqtt, "office", heartbeat=_beat)

    with pytest.raises(ConnectionError, match="broker rejected the availability publish"):
        await bridge.reconcile()

    age = heartbeat_age(heartbeat_file, now=time.time(), started_at=0.0)
    # Beat-first: the bus read + heartbeat happen before the availability publish,
    # so a broker-only publish failure leaves the heartbeat fresh.
    assert age < 1800.0
    assert not should_reboot(
        age=age,
        stale_after=1800.0,
        bridge_active=True,
        gateway_up=True,
        # The session stamps the phase "bus" before this reconcile, so the
        # watchdog would treat the panel as confirmed; only the fresh heartbeat
        # holds the reboot back.
        bus_failure_age=0.0,
    )
