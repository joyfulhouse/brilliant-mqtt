"""Cross-component regressions from bridge startup phase to reboot decisions."""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import cast

import pytest

import brilliant_mqtt.__main__ as main_mod
from brilliant_bus_watchdog.health import bus_confirmed, heartbeat_age
from brilliant_bus_watchdog.reboot_guard import GuardPolicy, RebootGuard
from brilliant_bus_watchdog.run import handle, should_reboot
from brilliant_mqtt.bridge import Bridge
from brilliant_mqtt.config import Settings
from brilliant_mqtt.heartbeat import write_heartbeat
from brilliant_mqtt.model import BrilliantDevice
from brilliant_mqtt.protocols import CommandSubscribeError
from brilliant_mqtt.retained_topics import RetainedLedgerError
from tests.fakes import FakeBus, FakeMqtt


class _Bus:
    def __init__(
        self,
        start_error: Exception | None = None,
        *,
        block_start_until: asyncio.Event | None = None,
        entered_start: asyncio.Event | None = None,
    ) -> None:
        self.start_error = start_error
        self.start_calls = 0
        self.read_calls = 0
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
        # Registered by the real SceneBridge at startup; it never fires in these
        # tests (startup fails at the MQTT subscribe, before any bus push).
        # Signature mirrors ``BusClient.on_change`` / ``tests.fakes.FakeBus``
        # (``want_device`` added by #98) so the real bridge can register.
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
        # Registered by the real SceneBridge at startup.
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
    bridge: type[_NoopBridge] | type[_ReadOnceBridge] | type[_ReadingBridge] = _NoopBridge,
) -> None:
    monkeypatch.setattr(main_mod, "RpcBusAdapter", lambda **kwargs: bus)
    monkeypatch.setattr(main_mod, "AioMqttAdapter", lambda settings: mqtt)
    monkeypatch.setattr(main_mod, "Bridge", bridge)


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

    confirmed = bus_confirmed(settings.bus_phase_file)
    assert bus.start_calls == 0
    assert mqtt.connect_calls == 3
    assert confirmed is False
    assert not should_reboot(
        age=1900.0,
        stale_after=1800.0,
        bridge_active=True,
        gateway_up=True,
        bus_confirmed=confirmed,
    )


async def test_stale_bus_phase_and_heartbeat_do_not_reboot_on_broker_outage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A prior HEALTHY session leaves "bus" and a fresh heartbeat on disk;
    later the heartbeat goes stale while the broker is down. Today's
    broker-only outage must NOT read those leftovers as a bus wedge: entering
    _run_session stamps "pre_bus" BEFORE mqtt.connect(), so once the broker
    refusal short-circuits startup, bus_confirmed is False even though the
    stale heartbeat age alone would otherwise qualify. Unlike its empty-file
    siblings above, this seeds the fail-unsafe leftovers explicitly so the
    test would catch a startup that wrote a fake heartbeat or let a stale
    "bus" marker survive the refusal."""
    settings = _settings(tmp_path)
    # Leftovers from a prior successful session, before this outage begins. The
    # phase carries THIS (live) process's pid so it would read as confirmed on
    # its own — proving the pre_bus stamp, not a dead-writer check, is what
    # clears it.
    _seed(settings.bus_phase_file, f"bus {os.getpid()}")
    _seed(settings.bus_heartbeat_file, "100.0")

    bus = _Bus()
    mqtt = _Mqtt(ConnectionError("broker refused"))
    _install_session_fakes(monkeypatch, bus, mqtt)

    with pytest.raises(ConnectionError, match="broker refused"):
        await main_mod._run_session(settings, None, None)

    confirmed = bus_confirmed(settings.bus_phase_file)
    age = heartbeat_age(settings.bus_heartbeat_file, now=100_000.0, started_at=0.0)
    assert bus.start_calls == 0  # the local bus was never reached
    assert mqtt.connect_calls == 1
    # The pre_bus stamp at session entry overwrote the stale "bus" marker...
    assert confirmed is False
    # ...and the seeded heartbeat is genuinely stale, so this test proves it is
    # bus_confirmed (not a fresh heartbeat) that holds the reboot back.
    assert age >= 1800.0
    assert not should_reboot(
        age=age,
        stale_after=1800.0,
        bridge_active=True,
        gateway_up=True,
        bus_confirmed=confirmed,
    )


async def test_failed_pre_bus_restamp_does_not_reboot_on_broker_outage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Issue #87 regression, the in-process retry path: run() retries
    _run_session in the SAME process and teardown deliberately keeps the "bus"
    marker, so a leftover marker carries THIS still-live pid. If session N's
    pre_bus re-stamp FAILS (ENOSPC/EROFS/perm on the /run tmpfs) and is merely
    swallowed, that live-pid "bus" marker stays readable — bus_confirmed returns
    True (the pid is alive), and a stale heartbeat during a broker-only outage
    reboots a healthy panel in a loop. Proving the failed stamp actively clears
    the marker: seed a live-pid "bus" + stale heartbeat, make the pre_bus stamp
    fail, and assert the outage does NOT qualify as a bus wedge."""
    settings = _settings(tmp_path)
    _seed(settings.bus_phase_file, f"bus {os.getpid()}")  # leftover, live pid
    _seed(settings.bus_heartbeat_file, "100.0")

    def _fail_stamp(*args: object, **kwargs: object) -> None:
        raise OSError(28, "No space left on device")

    monkeypatch.setattr("brilliant_mqtt.heartbeat._atomic_write", _fail_stamp)

    bus = _Bus()
    mqtt = _Mqtt(ConnectionError("broker refused"))
    _install_session_fakes(monkeypatch, bus, mqtt)

    with pytest.raises(ConnectionError, match="broker refused"):
        await main_mod._run_session(settings, None, None)

    confirmed = bus_confirmed(settings.bus_phase_file)
    age = heartbeat_age(settings.bus_heartbeat_file, now=100_000.0, started_at=0.0)
    assert bus.start_calls == 0  # the local bus was never reached
    # The failed pre_bus stamp cleared the live-pid "bus" leftover...
    assert confirmed is False
    # ...and the heartbeat is genuinely stale, so it is the cleared marker (not
    # a fresh heartbeat) that holds the reboot back.
    assert age >= 1800.0
    assert not should_reboot(
        age=age,
        stale_after=1800.0,
        bridge_active=True,
        gateway_up=True,
        bus_confirmed=confirmed,
    )


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

    confirmed = bus_confirmed(settings.bus_phase_file)
    assert mqtt.connect_calls == 1
    assert mqtt.subscribe_calls == 1
    assert bus.start_calls == 0
    assert confirmed is False
    assert not should_reboot(
        age=99_999.0,
        stale_after=1800.0,
        bridge_active=True,
        gateway_up=True,
        bus_confirmed=confirmed,
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

    confirmed = bus_confirmed(settings.bus_phase_file)
    assert raised.value is ledger_error
    assert bus.start_calls == 0
    assert mqtt.connect_calls == 1  # diagnostic publish only
    assert confirmed is False
    assert not should_reboot(
        age=99_999.0,
        stale_after=1800.0,
        bridge_active=True,
        gateway_up=True,
        bus_confirmed=confirmed,
    )


async def test_sustained_bus_handshake_failure_still_uses_reboot_guard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bus = _Bus(ConnectionError("bus handshake failed"))
    mqtt = _Mqtt()
    settings = _settings(tmp_path)
    _install_session_fakes(monkeypatch, bus, mqtt)

    for _ in range(3):
        with pytest.raises(ConnectionError, match="bus handshake failed"):
            await main_mod._run_session(settings, None, None)

    confirmed = bus_confirmed(settings.bus_phase_file)
    age = heartbeat_age(settings.bus_heartbeat_file, now=1900.0, started_at=0.0)
    decision = should_reboot(
        age=age,
        stale_after=1800.0,
        # Assumes the systemd supervisor's restart backoff keeps
        # brilliant-mqtt.service reported "active" through repeated
        # in-process bus-handshake failures — see the finally-block comment
        # in __main__.py about backoff being far shorter than stale_after.
        # This test exercises the predicate's boolean math only; it does not
        # exercise systemd's actual crash-loop/backoff behavior end to end.
        bridge_active=True,
        gateway_up=True,
        bus_confirmed=confirmed,
    )
    assert bus.start_calls == 3
    assert mqtt.connect_calls == 3
    assert confirmed is True
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

    confirmed = bus_confirmed(settings.bus_phase_file)
    age = heartbeat_age(settings.bus_heartbeat_file, now=time.time(), started_at=0.0)
    assert bus.start_calls == 1
    assert bus.read_calls == 1
    assert confirmed is True
    assert 0.0 <= age < 1.0
    assert not should_reboot(
        age=age,
        stale_after=1800.0,
        bridge_active=True,
        gateway_up=True,
        bus_confirmed=confirmed,
    )


# --- issue #87 audit follow-up: broker-only faults that can reboot a HEALTHY
# panel while the phase reads "bus". The ROOT cause is that the liveness
# heartbeat is not stamped until AFTER broker-dependent work. The fix stamps it
# first inside Bridge.reconcile (see test_reconcile_beats_before_broker_publish,
# GREEN); two residual windows that a tight, AC3-preserving fix cannot close are
# kept as strict xfail demonstrations. AC3
# (test_sustained_bus_handshake_failure_still_uses_reboot_guard) stays green.


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason=(
        "issue #87 DEFECT #1: the phase is stamped 'bus' before bus.start(), so "
        "bus_confirmed is True during the Thrift handshake window; a clean fix "
        "cannot be phase-only because a slow-but-succeeding bus.start() and a "
        "sustained handshake failure (AC3) leave the SAME pre-handshake phase "
        "with a stale heartbeat — see PR body. Demonstration kept red on purpose."
    ),
)
async def test_broker_recovery_bus_start_window_does_not_reboot_healthy_panel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DEFECT #1 (issue #87 audit follow-up): the session stamps the phase
    "bus" BEFORE ``bus.start()`` and before the first bus read/heartbeat.

    After a long broker outage the heartbeat is legitimately stale while the
    phase stays "pre_bus" (no reboot — correct). When the broker returns, the
    next session stamps "bus" and then sits inside ``bus.start()`` (the Thrift
    connect, up to ``_CONNECT_TIMEOUT_S`` = 10s). During that window the
    heartbeat is still the pre-outage stale value, yet ``bus_confirmed`` already
    reads True — so a watchdog cycle that samples the phase inside the window
    sees ``(age >= stale_after, bus_confirmed=True)`` and reboots a HEALTHY
    panel whose bus is merely still handshaking.

    Reproduced with a bus whose ``start()`` blocks (broker recovered, bus still
    connecting); the assertion is made at the ``should_reboot`` predicate.
    """
    settings = _settings(tmp_path)
    # A prior healthy session's heartbeat, now stale after the long outage.
    _seed(settings.bus_heartbeat_file, "100.0")

    entered_start = asyncio.Event()
    still_handshaking = asyncio.Event()  # never set: the bus stays mid-handshake
    bus = _Bus(block_start_until=still_handshaking, entered_start=entered_start)
    mqtt = _Mqtt()  # the broker has recovered: connect succeeds
    _install_session_fakes(monkeypatch, bus, mqtt)

    task = asyncio.create_task(main_mod._run_session(settings, None, None))
    try:
        await asyncio.wait_for(entered_start.wait(), timeout=1)
        confirmed = bus_confirmed(settings.bus_phase_file)
        age = heartbeat_age(settings.bus_heartbeat_file, now=time.time(), started_at=0.0)
        assert bus.start_calls == 1  # inside bus.start(); first read not yet reached
        assert age >= 1800.0  # the heartbeat is genuinely stale from the outage
        assert not should_reboot(
            age=age,
            stale_after=1800.0,
            bridge_active=True,
            gateway_up=True,
            bus_confirmed=confirmed,
        )
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_scene_subscribe_failure_never_reboots_a_healthy_panel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DEFECT #2 (issue #87 audit follow-up), scene-subscribe path: with the
    scene bridge enabled the session runs ``scene_bridge.async_start()`` — which
    SUBSCRIBEs to MQTT — BEFORE the first panel reconcile/bus read.

    A persistent subscribe rejection (a broker ACL that forbids the scene topic,
    or a SUBACK that never arrives) raises ``CommandSubscribeError`` every
    attempt: ``bus.start()`` has already succeeded and the phase is "bus", but
    NO heartbeat is ever written (only reconcile/poll beat, and reconcile is
    never reached), so the heartbeat goes stale while the phase reads confirmed
    and the watchdog reboots a HEALTHY panel over a broker-only fault.
    Deterministic, not a timing window.

    Reproduced with the REAL SceneBridge and a fake MQTT whose subscribe raises.
    Every retry must complete an independent panel read and heartbeat before the
    scene subscription can fail.
    """
    settings = _settings(tmp_path, scene_enabled=True)
    # A prior healthy session's heartbeat; nothing refreshes it because the scene
    # subscribe fails before the beating reconcile, so it reads as stale.
    _seed(settings.bus_heartbeat_file, "100.0")

    bus = _Bus()  # the local bus handshakes fine
    mqtt = _Mqtt(subscribe_error=CommandSubscribeError("scene command topic rejected"))
    _install_session_fakes(monkeypatch, bus, mqtt, _ReadingBridge)

    for _ in range(3):
        with pytest.raises(CommandSubscribeError, match="scene command topic rejected"):
            await main_mod._run_session(settings, None, None)

    confirmed = bus_confirmed(settings.bus_phase_file)
    age = heartbeat_age(settings.bus_heartbeat_file, now=time.time(), started_at=0.0)
    assert bus.start_calls == 3  # the local bus was healthy on every retry...
    assert bus.read_calls == 3
    assert confirmed is True  # ...and the phase reached "bus"
    assert age < 1800.0
    assert not should_reboot(
        age=age,
        stale_after=1800.0,
        bridge_active=True,
        gateway_up=True,
        bus_confirmed=confirmed,
    )


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
        bus_confirmed=True,
    )
