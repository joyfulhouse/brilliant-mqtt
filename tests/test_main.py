"""Off-panel tests for the __main__ session wiring (M11 Step 3).

A real session needs the panel bus and a live broker, so only the pure
pieces are unit-tested here: the panel-scope predicate, and the leadership
gate the session builds — a mesh Bridge whose include predicate consults
``leader.is_leader``, so a non-leader (or fresh ex-leader, whose _on_change
stays registered after withdraw) publishes nothing on the mesh namespace.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from pathlib import Path
from types import SimpleNamespace

import pytest
from aiomqtt import MqttError

import brilliant_mqtt.__main__ as main_mod
from brilliant_mqtt import __version__
from brilliant_mqtt import bus as bus_mod
from brilliant_mqtt.__main__ import _is_panel_device, _is_reconnect_storm, _make_desired
from brilliant_mqtt.bridge import Bridge, CommandSubscribeError, HotPollReadTimeout
from brilliant_mqtt.bus import RpcBusAdapter
from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.config import Settings
from brilliant_mqtt.desired_state import DesiredState
from brilliant_mqtt.ha_control_protocol import mode_command_topic, scene_command_topic
from brilliant_mqtt.mesh_leader import MESH_LEADER_TOPIC, MeshLeader
from brilliant_mqtt.model import BrilliantDevice, DeviceKind, Variable
from brilliant_mqtt.retained_topics import RetainedLedgerError
from tests.fakes import FakeBus, FakeClock, FakeMqtt

HB = 10.0


def _settings(
    reconnect_storm_threshold: int = 20,
    reconnect_storm_window_seconds: float = 60.0,
) -> Settings:
    """A Settings with required fields filled and the breaker knobs overridable."""
    return Settings(
        panel="office",
        mqtt_host="h",
        mqtt_username="u",
        mqtt_password="p",
        reconnect_storm_threshold=reconnect_storm_threshold,
        reconnect_storm_window_seconds=reconnect_storm_window_seconds,
    )


def _mesh_dimmer() -> BrilliantDevice:
    """A mesh load on the virtual ble_mesh bus device (live-verified shape)."""
    return BrilliantDevice(
        device_id="ble_mesh",
        peripheral_id="018691f1749b000701c4e689967b8e62",
        name="Office Desk Lights",
        kind=DeviceKind.LIGHT,
        peripheral_type=27,
        variables={
            "on": Variable("on", "0"),
            "intensity": Variable("intensity", "600"),
            "dimmable": Variable("dimmable", "1"),
        },
    )


def _panel_dimmer() -> BrilliantDevice:
    return BrilliantDevice(
        device_id="device_001",
        peripheral_id="gangbox_peripheral_0",
        name="Lights",
        kind=DeviceKind.LIGHT,
        variables={"on": Variable("on", "0")},
    )


class TestPanelScopePredicate:
    def test_panel_device_in_scope(self) -> None:
        assert _is_panel_device(_panel_dimmer()) is True

    def test_mesh_device_out_of_scope(self) -> None:
        assert _is_panel_device(_mesh_dimmer()) is False


class TestReconnectStormBreaker:
    """The run loop trips a session rebuild when the bus reconnects too many
    times in the window — the breaker the stale watchdog can't be (a storm
    keeps resetting the push clock). Threshold <= 0 disables it."""

    def test_trips_at_threshold(self) -> None:
        bus = FakeBus([])
        bus.reconnect_count = 20
        assert _is_reconnect_storm(bus, _settings(reconnect_storm_threshold=20)) is True

    def test_below_threshold_does_not_trip(self) -> None:
        bus = FakeBus([])
        bus.reconnect_count = 19
        assert _is_reconnect_storm(bus, _settings(reconnect_storm_threshold=20)) is False

    def test_zero_threshold_disables_breaker(self) -> None:
        bus = FakeBus([])
        bus.reconnect_count = 10_000
        assert _is_reconnect_storm(bus, _settings(reconnect_storm_threshold=0)) is False

    def test_queries_the_configured_window(self) -> None:
        bus = FakeBus([])
        bus.reconnect_count = 20
        _is_reconnect_storm(bus, _settings(reconnect_storm_window_seconds=42.0))
        assert bus.reconnect_window_queried == 42.0


class TestReconnectReconcileCoalescing:
    async def test_burst_runs_one_in_flight_and_exactly_one_trailing_call(self) -> None:
        first_started = asyncio.Event()
        release_first = asyncio.Event()
        calls = 0
        active = 0
        max_active = 0

        async def reconcile() -> None:
            nonlocal calls, active, max_active
            calls += 1
            active += 1
            max_active = max(max_active, active)
            try:
                if calls == 1:
                    first_started.set()
                    await release_first.wait()
            finally:
                active -= 1

        coalesced = main_mod._CoalescingCallback(reconcile)
        first = asyncio.create_task(coalesced())
        await asyncio.wait_for(first_started.wait(), timeout=0.1)

        await asyncio.gather(coalesced(), coalesced(), coalesced())
        assert calls == 1
        release_first.set()
        await first

        assert calls == 2
        assert max_active == 1

    async def test_request_after_idle_starts_a_new_call(self) -> None:
        calls = 0

        async def reconcile() -> None:
            nonlocal calls
            calls += 1

        coalesced = main_mod._CoalescingCallback(reconcile)

        await coalesced()
        await coalesced()

        assert calls == 2


def _data_topics(mqtt: FakeMqtt) -> list[str]:
    """Topics published OTHER than the leadership claim (the gated output)."""
    return [p[0] for p in mqtt.published if p[0] != MESH_LEADER_TOPIC]


class TestLeadershipGate:
    """The include-predicate gate the session wires for the mesh bridge.

    Step 2 left _on_change registered after withdraw(); checking
    leader.is_leader INSIDE the include predicate is what actually silences a
    non-leader/ex-leader for pushes AND polls — proven here end to end.
    """

    async def test_gate_silences_non_leader_then_opens_then_closes(self) -> None:
        device = _mesh_dimmer()
        bus = FakeBus([device])
        mqtt = FakeMqtt()
        clock = FakeClock()

        def _mesh_in_scope(d: BrilliantDevice) -> bool:
            # Mirrors the late-binding closure in __main__._run_session.
            return d.device_id == "ble_mesh" and leader.is_leader

        mesh_bridge = Bridge(bus, mqtt, "mesh", include=_mesh_in_scope)
        leader = MeshLeader(
            mqtt,
            "office",
            1,
            HB,
            on_acquire=mesh_bridge.reconcile,
            on_lose=mesh_bridge.withdraw,
            clock=clock,
        )
        await leader.start()

        # Before leadership: pushes and polls publish NOTHING on the mesh
        # namespace, although the bus fan-out delivers the device here.
        await bus.emit(device)
        await mesh_bridge.poll_once()
        assert _data_topics(mqtt) == []

        # Acquisition (on_acquire = reconcile) opens the gate and publishes.
        await leader.tick()
        clock.advance(HB)
        await leader.tick()
        assert leader.is_leader
        assert any(t.startswith("homeassistant/") for t in _data_topics(mqtt))
        assert f"brilliant/mesh/{device.peripheral_id}/state" in _data_topics(mqtt)

        # A better claim arrives: step-down withdraws — and the gate keeps
        # the STILL-REGISTERED _on_change and the polls silent afterwards.
        await mqtt.inject(MESH_LEADER_TOPIC, json.dumps({"panel": "attic", "priority": 1}))
        await leader.tick()
        assert leader.is_leader is False
        mqtt.published.clear()
        await bus.emit(device)
        await mesh_bridge.poll_once()
        assert _data_topics(mqtt) == []


def _desired_settings(
    motion_reconcile_enabled: bool = True,
    motion_desired_state_dir: str = "/var/brilliant-mqtt/state",
) -> Settings:
    """Build a minimal Settings for _make_desired tests."""
    return Settings(
        panel="office",
        mqtt_host="h",
        mqtt_username="u",
        mqtt_password="p",
        motion_reconcile_enabled=motion_reconcile_enabled,
        motion_desired_state_dir=motion_desired_state_dir,
    )


def test_make_desired_disabled_returns_none(tmp_path: Path) -> None:
    s = _desired_settings(motion_reconcile_enabled=False, motion_desired_state_dir=str(tmp_path))
    assert _make_desired(s, "office-faceplate") is None


def test_make_desired_enabled_builds_loaded_store(tmp_path: Path) -> None:
    s = _desired_settings(motion_reconcile_enabled=True, motion_desired_state_dir=str(tmp_path))
    ds = _make_desired(s, "mesh")
    assert ds is not None
    assert ds.wanted("any") == {}  # loaded (empty) without error


class TestProcessLifetimeDesiredState:
    """Desired-state stores are PROCESS-lifetime, not session-lifetime: a
    session rebuild (stale watchdog / storm breaker — routine on this fleet)
    must not discard in-memory intent recorded while persistence was failing,
    nor resurrect stale disk state over the operator's last command."""

    async def test_run_reuses_desired_state_across_session_rebuilds(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        loads: list[DesiredState] = []
        orig_load = DesiredState.load

        def counting_load(ds: DesiredState) -> None:
            loads.append(ds)
            orig_load(ds)

        monkeypatch.setattr(DesiredState, "load", counting_load)
        monkeypatch.setattr(main_mod, "_BACKOFF_S", 0)

        seen: list[DesiredState | None] = []
        calls = 0

        async def fake_session(
            settings: Settings,
            desired_panel: DesiredState | None = None,
            desired_mesh: DesiredState | None = None,
        ) -> None:
            nonlocal calls
            calls += 1
            seen.append(desired_panel)
            seen.append(desired_mesh)
            if calls == 1:
                raise RuntimeError("session died (storm)")
            raise asyncio.CancelledError

        monkeypatch.setattr(main_mod, "_run_session", fake_session)

        s = _desired_settings(motion_desired_state_dir=str(tmp_path))
        with pytest.raises(asyncio.CancelledError):
            await main_mod.run(s)

        assert calls == 2
        # The SAME store instance is handed to both sessions...
        assert seen[0] is not None and seen[0] is seen[2]
        assert seen[1] is seen[3]  # mesh not participating -> None both times
        # ...and the disk was read exactly once, at process start — a rebuild
        # must never re-load stale disk state over live in-memory intent.
        assert len(loads) == 1

    async def test_run_builds_separate_stores_when_mesh_participates(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(main_mod, "_BACKOFF_S", 0)
        seen: list[DesiredState | None] = []

        async def fake_session(
            settings: Settings,
            desired_panel: DesiredState | None = None,
            desired_mesh: DesiredState | None = None,
        ) -> None:
            seen.append(desired_panel)
            seen.append(desired_mesh)
            raise asyncio.CancelledError

        monkeypatch.setattr(main_mod, "_run_session", fake_session)

        s = Settings(
            panel="office",
            mqtt_host="h",
            mqtt_username="u",
            mqtt_password="p",
            mesh_priority=1,
            motion_desired_state_dir=str(tmp_path),
        )
        with pytest.raises(asyncio.CancelledError):
            await main_mod.run(s)

        assert seen[0] is not None and seen[1] is not None
        assert seen[0] is not seen[1]  # faceplate and mesh stores stay separate


class TestSupervisorBackoff:
    async def test_retained_ledger_failure_uses_sixty_second_backoff(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        session_calls = 0
        sleeps: list[float] = []

        async def failing_session(
            settings: Settings,
            desired_panel: DesiredState | None,
            desired_mesh: DesiredState | None,
        ) -> None:
            del settings, desired_panel, desired_mesh
            nonlocal session_calls
            session_calls += 1
            raise RetainedLedgerError("persistent ledger failure")

        async def cancel_on_sleep(delay: float) -> None:
            sleeps.append(delay)
            raise asyncio.CancelledError

        monkeypatch.setattr(main_mod, "_run_session", failing_session)
        monkeypatch.setattr(asyncio, "sleep", cancel_on_sleep)

        with pytest.raises(asyncio.CancelledError):
            await main_mod.run(_desired_settings(motion_reconcile_enabled=False))

        assert session_calls == 1
        assert sleeps == [60.0]

    async def test_transient_session_failure_keeps_five_second_backoff(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        sleeps: list[float] = []

        async def failing_session(
            settings: Settings,
            desired_panel: DesiredState | None,
            desired_mesh: DesiredState | None,
        ) -> None:
            del settings, desired_panel, desired_mesh
            raise RuntimeError("transient session failure")

        async def cancel_on_sleep(delay: float) -> None:
            sleeps.append(delay)
            raise asyncio.CancelledError

        monkeypatch.setattr(main_mod, "_run_session", failing_session)
        monkeypatch.setattr(asyncio, "sleep", cancel_on_sleep)

        with pytest.raises(asyncio.CancelledError):
            await main_mod.run(_desired_settings(motion_reconcile_enabled=False))

        assert sleeps == [5]


def _scene_settings(enabled: bool, watermark_file: str) -> Settings:
    """Build settings for scene session tests before and after the fields exist."""
    settings = _settings()
    object.__setattr__(settings, "scene_bridge_enabled", enabled)
    object.__setattr__(settings, "scene_watermark_file", watermark_file)
    return settings


class _SessionBus:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.reconnect_callback: Callable[[], Awaitable[None]] | None = None
        self.snapshot = [_panel_dimmer(), _mesh_dimmer()]
        self.get_all_calls: list[bool] = []
        self.get_all_effects: list[BaseException | None] = []
        self.get_device_calls: list[str] = []
        self.write_timeout_latched = False
        self.write_timeout_checks = 0

    def on_reconnect(self, callback: Callable[[], Awaitable[None]]) -> None:
        self.events.append("bus_reconnect_callback")
        self.reconnect_callback = callback

    def on_change(
        self,
        callback: Callable[[BrilliantDevice], Awaitable[None]],
        *,
        coalesce_pushes: bool = True,
    ) -> None:
        del callback, coalesce_pushes

    async def start(self) -> None:
        self.events.append("bus_start")

    async def shutdown(self) -> None:
        self.events.append("bus_shutdown")

    async def get_all(self, *, include_extras: bool = True) -> list[BrilliantDevice]:
        self.events.append("bus_get_all")
        self.get_all_calls.append(include_extras)
        self.get_device_calls.append("own-device")
        if include_extras:
            self.get_device_calls.append("ble_mesh")
        if self.get_all_effects:
            effect = self.get_all_effects.pop(0)
            if effect is not None:
                raise effect
        return self.snapshot

    def seconds_since_last_push(self) -> float | None:
        return None

    def recent_reconnects(self, window_seconds: float) -> int:
        del window_seconds
        return 0

    def consume_write_timeout(self) -> bool:
        self.write_timeout_checks += 1
        latched = self.write_timeout_latched
        self.write_timeout_latched = False
        return latched


class _SessionMqtt:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.subscriptions: list[str] = []
        self.published: list[tuple[str, str, bool, int]] = []
        self.connect_error: Exception | None = None
        self.publish_error: Exception | None = None
        self.subscribe_effects: list[BaseException | None] = []

    def on_command(self, callback: Callable[[str, str], Awaitable[None]]) -> None:
        del callback

    async def connect(self) -> None:
        self.events.append("mqtt_connect")
        if self.connect_error is not None:
            raise self.connect_error

    async def disconnect(self) -> None:
        self.events.append("mqtt_disconnect")

    async def publish(
        self,
        topic: str,
        payload: str,
        retain: bool = False,
        qos: int = 0,
    ) -> None:
        self.events.append("mqtt_publish")
        if self.publish_error is not None:
            raise self.publish_error
        self.published.append((topic, payload, retain, qos))

    async def subscribe(self, topic: str) -> None:
        self.events.append("mqtt_subscribe")
        if self.subscribe_effects:
            effect = self.subscribe_effects.pop(0)
            if effect is not None:
                raise effect
        self.subscriptions.append(topic)


class _SessionHarness:
    def __init__(
        self,
        monkeypatch: pytest.MonkeyPatch,
        *,
        scene_start_error: RuntimeError | None = None,
        scene_shutdown_error: RuntimeError | None = None,
        bridge_reconcile_error: Exception | None = None,
        bridge_reconcile_effects: dict[str, list[BaseException | None]] | None = None,
        bridge_poll_effects: dict[str, list[BaseException | None]] | None = None,
        bus_get_all_effects: list[BaseException | None] | None = None,
        leader_is_leader: bool = True,
        real_bridge: bool = False,
    ) -> None:
        self.events: list[str] = []
        self.ready = asyncio.Event()
        self.bus = _SessionBus(self.events)
        self.bus.get_all_effects = list(bus_get_all_effects or ())
        self.mqtt = _SessionMqtt(self.events)
        self.scene_start_error = scene_start_error
        self.scene_shutdown_error = scene_shutdown_error
        self.bridge_reconcile_error = bridge_reconcile_error
        self.bridge_reconcile_effects = {
            scope: list(effects) for scope, effects in (bridge_reconcile_effects or {}).items()
        }
        self.bridge_poll_effects = {
            scope: list(effects) for scope, effects in (bridge_poll_effects or {}).items()
        }
        self.poll_snapshots: dict[str, list[list[BrilliantDevice] | None]] = {
            "panel": [],
            "mesh": [],
        }
        self.scene_poll_snapshots: list[list[BrilliantDevice]] = []
        self.scene_instances: list[object] = []
        self.scene_bus: object | None = None
        self.scene_mqtt: object | None = None
        self.scene_panel: str | None = None
        self.scene_watermark_path: object | None = None
        self.scene_clock_ms: Callable[[], int] | None = None

        harness = self

        class SessionBridge:
            def __init__(self, *args: object, **kwargs: object) -> None:
                del kwargs
                self._scope = "mesh" if args[2] == "mesh" else "panel"
                harness.events.append(f"{self._scope}_bridge_construct")

            async def reconcile(self) -> None:
                harness.events.append(f"{self._scope}_reconcile")
                if self._scope == "panel" and harness.bridge_reconcile_error is not None:
                    raise harness.bridge_reconcile_error
                effects = harness.bridge_reconcile_effects.get(self._scope)
                if effects:
                    effect = effects.pop(0)
                    if effect is not None:
                        raise effect
                if self._scope == "panel":
                    harness.ready.set()

            async def poll_once(self, devices: list[BrilliantDevice] | None = None) -> None:
                harness.events.append(f"{self._scope}_poll")
                harness.poll_snapshots[self._scope].append(devices)
                effects = harness.bridge_poll_effects.get(self._scope)
                if effects:
                    effect = effects.pop(0)
                    if effect is not None:
                        raise effect

            async def withdraw(self) -> None:
                return

        class SessionLeader:
            def __init__(self, *args: object, **kwargs: object) -> None:
                del args, kwargs
                self.is_leader = leader_is_leader

            async def start(self) -> None:
                harness.events.append("leader_start")

            async def tick(self) -> None:
                harness.events.append("leader_tick")

        class SessionSceneBridge:
            def __init__(
                self,
                bus: object,
                mqtt: object,
                panel: str,
                watermark_path: str | Path,
                clock_ms: Callable[[], int],
            ) -> None:
                harness.events.append("scene_bridge_construct")
                harness.scene_instances.append(self)
                harness.scene_bus = bus
                harness.scene_mqtt = mqtt
                harness.scene_panel = panel
                harness.scene_watermark_path = watermark_path
                harness.scene_clock_ms = clock_ms

            async def async_start(self) -> None:
                harness.events.append("scene_bridge_start")
                if harness.scene_start_error is not None:
                    raise harness.scene_start_error

            async def async_shutdown(self) -> None:
                harness.events.append("scene_bridge_shutdown")
                if harness.scene_shutdown_error is not None:
                    raise harness.scene_shutdown_error

            async def poll_executions(self, devices: list[BrilliantDevice]) -> None:
                harness.events.append("scene_poll")
                harness.scene_poll_snapshots.append(devices)

        def mqtt_factory(settings: Settings) -> _SessionMqtt:
            del settings
            self.events.append("mqtt_construct")
            return self.mqtt

        def bus_factory(*, extra_device_ids: tuple[str, ...]) -> _SessionBus:
            del extra_device_ids
            self.events.append("bus_construct")
            return self.bus

        monkeypatch.setattr(main_mod, "AioMqttAdapter", mqtt_factory)
        monkeypatch.setattr(main_mod, "RpcBusAdapter", bus_factory)
        if not real_bridge:
            monkeypatch.setattr(main_mod, "Bridge", SessionBridge)
        monkeypatch.setattr(main_mod, "MeshLeader", SessionLeader)
        monkeypatch.setattr(main_mod, "SceneBridge", SessionSceneBridge, raising=False)


async def _cancel_ready_session(harness: _SessionHarness, settings: Settings) -> None:
    task = asyncio.create_task(main_mod._run_session(settings, None, None))
    await asyncio.wait_for(harness.ready.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


def _hot_poll_settings(*, mesh: bool = False, resync_seconds: int = 3_600) -> Settings:
    settings = _settings()
    object.__setattr__(settings, "hot_poll_seconds", 0.001)
    object.__setattr__(settings, "bus_stale_seconds", 0)
    object.__setattr__(settings, "resync_seconds", resync_seconds)
    object.__setattr__(settings, "mesh_priority", 1 if mesh else 0)
    return settings


class TestSharedHotPollSnapshot:
    async def test_leader_tick_reads_beats_once_and_shares_snapshot(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        harness = _SessionHarness(
            monkeypatch,
            bridge_poll_effects={"mesh": [asyncio.CancelledError()]},
        )
        beats: list[str] = []
        monkeypatch.setattr(
            main_mod,
            "write_heartbeat",
            lambda path, clock: beats.append(path),
        )

        with pytest.raises(asyncio.CancelledError):
            await main_mod._run_session(_hot_poll_settings(mesh=True), None, None)

        assert harness.bus.get_all_calls == [True]
        assert harness.poll_snapshots == {
            "panel": [harness.bus.snapshot],
            "mesh": [harness.bus.snapshot],
        }
        assert harness.poll_snapshots["panel"][0] is harness.poll_snapshots["mesh"][0]
        assert harness.bus.get_device_calls.count("ble_mesh") == 1
        assert len(beats) == 1

    async def test_standby_tick_never_reads_ble_mesh(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        harness = _SessionHarness(
            monkeypatch,
            bridge_poll_effects={"panel": [asyncio.CancelledError()]},
            leader_is_leader=False,
        )

        with pytest.raises(asyncio.CancelledError):
            await main_mod._run_session(_hot_poll_settings(mesh=True), None, None)

        assert harness.bus.get_all_calls == [False]
        assert harness.bus.get_device_calls.count("ble_mesh") == 0

    async def test_hot_poll_snapshot_reaches_scene_bridge_only_when_enabled(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        enabled = _SessionHarness(
            monkeypatch,
            bridge_poll_effects={"panel": [None, asyncio.CancelledError()]},
        )
        settings = _scene_settings(True, str(tmp_path / "scene-state.json"))
        object.__setattr__(settings, "hot_poll_seconds", 0.001)
        object.__setattr__(settings, "bus_stale_seconds", 0)

        with pytest.raises(asyncio.CancelledError):
            await main_mod._run_session(settings, None, None)

        disabled = _SessionHarness(
            monkeypatch,
            bridge_poll_effects={"panel": [asyncio.CancelledError()]},
        )
        with pytest.raises(asyncio.CancelledError):
            await main_mod._run_session(_hot_poll_settings(), None, None)

        assert enabled.scene_poll_snapshots == [enabled.bus.snapshot]
        assert enabled.scene_poll_snapshots[0] is enabled.poll_snapshots["panel"][0]
        assert disabled.scene_poll_snapshots == []

    @pytest.mark.parametrize(
        "timeout_error",
        # Python 3.10: asyncio.TimeoutError is a DISTINCT class from the
        # builtin (unified only in 3.11) — the shared-read boundary must
        # catch both.
        [TimeoutError("first"), asyncio.TimeoutError()],
        ids=["builtin", "asyncio"],
    )
    async def test_shared_read_timeout_retries_without_half_applying(
        self,
        monkeypatch: pytest.MonkeyPatch,
        timeout_error: BaseException,
    ) -> None:
        harness = _SessionHarness(
            monkeypatch,
            bus_get_all_effects=[timeout_error, asyncio.CancelledError()],
        )
        beats: list[str] = []
        monkeypatch.setattr(
            main_mod,
            "write_heartbeat",
            lambda path, clock: beats.append(path),
        )

        with pytest.raises(asyncio.CancelledError):
            await main_mod._run_session(_hot_poll_settings(mesh=True), None, None)

        assert harness.bus.get_all_calls == [True, True]
        assert harness.poll_snapshots == {"panel": [], "mesh": []}
        assert beats == []

    async def test_second_shared_read_timeout_rebuilds_without_half_applying(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        first = TimeoutError("first")
        second = TimeoutError("second")
        harness = _SessionHarness(
            monkeypatch,
            bus_get_all_effects=[first, second],
        )

        with pytest.raises(HotPollReadTimeout) as raised:
            await main_mod._run_session(_hot_poll_settings(mesh=True), None, None)

        assert raised.value.__cause__ is second
        assert harness.bus.get_all_calls == [True, True]
        assert harness.poll_snapshots == {"panel": [], "mesh": []}


class TestWriteTimeoutRecovery:
    async def test_latched_write_timeout_rebuilds_session_on_next_tick(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        harness = _SessionHarness(monkeypatch)
        harness.bus.write_timeout_latched = True

        with pytest.raises(main_mod.BusWriteStuckError):
            await asyncio.wait_for(
                main_mod._run_session(_hot_poll_settings(), None, None),
                timeout=1,
            )

        assert harness.bus.write_timeout_checks == 1
        assert harness.events[-2:] == ["bus_shutdown", "mqtt_disconnect"]

    async def test_session_without_write_timeout_never_trips_breaker(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        harness = _SessionHarness(
            monkeypatch,
            bridge_poll_effects={"panel": [asyncio.CancelledError()]},
        )

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(
                main_mod._run_session(_hot_poll_settings(), None, None),
                timeout=1,
            )

        assert harness.bus.write_timeout_checks == 1


class _LatchProbeObserver:
    """Fake RPCObserver: reads answer at once, writes block until released."""

    def __init__(self) -> None:
        self.release = asyncio.Event()
        self.write_cancelled = False

    async def get_device(self, device_id: str) -> object:
        return SimpleNamespace(id=device_id, peripherals={})

    async def request_set_variables_in_peripheral(
        self,
        peripheral_id: str,
        values: dict[str, str],
        *,
        device_id: str,
    ) -> str:
        del peripheral_id, values, device_id
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.write_cancelled = True
            raise
        return "ok"

    async def shutdown(self) -> None:
        return


def _real_bus_session(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[_SessionHarness, RpcBusAdapter, _LatchProbeObserver]:
    """Session harness whose bus is the REAL adapter over a fake observer, so
    the coordinator's latch check exercises the genuine write-timeout path."""
    harness = _SessionHarness(
        monkeypatch,
        bridge_poll_effects={"panel": [asyncio.CancelledError()]},
    )
    observer = _LatchProbeObserver()
    adapter = RpcBusAdapter()
    adapter._obs = observer
    adapter._own_device_id = "own-device"

    async def no_start() -> None:
        return

    monkeypatch.setattr(adapter, "start", no_start)

    def bus_factory(*, extra_device_ids: tuple[str, ...]) -> RpcBusAdapter:
        del extra_device_ids
        return adapter

    monkeypatch.setattr(main_mod, "RpcBusAdapter", bus_factory)
    return harness, adapter, observer


class TestWriteTimeoutLatchSource:
    """Issue #72: only the fixed hard cap latches; a single caller deadline
    (5 s) leaves the session running."""

    async def test_tick_after_single_write_deadline_does_not_rebuild(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(bus_mod, "_WRITE_DEADLINE_S", 0.005)
        harness, adapter, observer = _real_bus_session(monkeypatch)
        with pytest.raises(asyncio.TimeoutError):
            await adapter.set_variables("own-device", "gangbox_peripheral_0", [VarSet("on", "1")])

        with pytest.raises(asyncio.CancelledError):  # the poll effect, not the breaker
            await asyncio.wait_for(main_mod._run_session(_hot_poll_settings(), None, None), 1)

        assert "panel_poll" in harness.events
        assert observer.write_cancelled is True  # settled at session teardown only

    async def test_tick_after_hard_cap_rebuilds(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(bus_mod, "_WRITE_DEADLINE_S", 0.005)
        monkeypatch.setattr(bus_mod, "_WRITE_HARD_CAP_S", 0.02)
        harness, adapter, _observer = _real_bus_session(monkeypatch)
        with pytest.raises(asyncio.TimeoutError):
            await adapter.set_variables("own-device", "gangbox_peripheral_0", [VarSet("on", "1")])
        await asyncio.sleep(0.05)

        with pytest.raises(main_mod.BusWriteStuckError):
            await asyncio.wait_for(main_mod._run_session(_hot_poll_settings(), None, None), 1)

        assert "panel_poll" not in harness.events
        assert harness.events[-1] == "mqtt_disconnect"


class TestHotPollReadTimeoutPolicy:
    async def test_first_timeout_gets_one_grace_and_skips_resync(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        timeout = HotPollReadTimeout("private panel detail")
        harness = _SessionHarness(
            monkeypatch,
            bridge_poll_effects={"panel": [timeout, asyncio.CancelledError()]},
        )

        with caplog.at_level("WARNING", logger="brilliant_mqtt.__main__"):
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(
                    main_mod._run_session(
                        _hot_poll_settings(resync_seconds=0),
                        None,
                        None,
                    ),
                    timeout=1,
                )

        assert harness.events.count("panel_poll") == 2
        assert harness.events.count("panel_reconcile") == 1
        assert (
            caplog.messages.count(
                "hot poll bus read timed out; retrying once before rebuilding session"
            )
            == 1
        )
        assert "private panel detail" not in caplog.text

    async def test_second_consecutive_combined_cycle_timeout_rebuilds_session(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        first = HotPollReadTimeout("first")
        second = HotPollReadTimeout("second")
        harness = _SessionHarness(
            monkeypatch,
            bridge_poll_effects={
                "panel": [None, None],
                "mesh": [first, second],
            },
        )

        with pytest.raises(HotPollReadTimeout) as raised:
            await asyncio.wait_for(
                main_mod._run_session(_hot_poll_settings(mesh=True), None, None),
                timeout=1,
            )

        assert raised.value is second
        poll_events = [event for event in harness.events if event.endswith("_poll")]
        assert poll_events == ["panel_poll", "mesh_poll", "panel_poll", "mesh_poll"]
        assert harness.events[-2:] == ["bus_shutdown", "mqtt_disconnect"]

    async def test_full_combined_cycle_success_resets_timeout_grace(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        harness = _SessionHarness(
            monkeypatch,
            bridge_poll_effects={
                "panel": [None, None, None, asyncio.CancelledError()],
                "mesh": [
                    HotPollReadTimeout("first"),
                    None,
                    HotPollReadTimeout("after success"),
                ],
            },
        )

        with caplog.at_level("WARNING", logger="brilliant_mqtt.__main__"):
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(
                    main_mod._run_session(_hot_poll_settings(mesh=True), None, None),
                    timeout=1,
                )

        poll_events = [event for event in harness.events if event.endswith("_poll")]
        assert poll_events == [
            "panel_poll",
            "mesh_poll",
            "panel_poll",
            "mesh_poll",
            "panel_poll",
            "mesh_poll",
            "panel_poll",
        ]
        assert (
            caplog.messages.count(
                "hot poll bus read timed out; retrying once before rebuilding session"
            )
            == 2
        )

    async def test_non_timeout_poll_failure_rebuilds_immediately(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        failure = RuntimeError("bus session broke")
        harness = _SessionHarness(
            monkeypatch,
            bridge_poll_effects={"panel": [failure]},
        )

        with pytest.raises(RuntimeError) as raised:
            await asyncio.wait_for(
                main_mod._run_session(_hot_poll_settings(), None, None),
                timeout=1,
            )

        assert raised.value is failure
        assert harness.events.count("panel_poll") == 1
        assert harness.events[-2:] == ["bus_shutdown", "mqtt_disconnect"]


class TestResyncSubscribePolicy:
    """Issue #76: a periodic-resync SUBACK timeout gets one grace tick."""

    async def test_first_periodic_subscribe_failure_warns_and_retries_next_tick(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        error = CommandSubscribeError("subscribe failed for brilliant/office/p0/set")
        harness = _SessionHarness(
            monkeypatch,
            bridge_reconcile_effects={"panel": [None, error, None, asyncio.CancelledError()]},
        )

        with caplog.at_level("WARNING", logger="brilliant_mqtt.__main__"):
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(
                    main_mod._run_session(_hot_poll_settings(resync_seconds=0), None, None),
                    timeout=1,
                )

        assert harness.events.count("panel_reconcile") == 4
        warnings = [record for record in caplog.records if record.levelname == "WARNING"]
        assert len(warnings) == 1
        assert "brilliant/office/p0/set" in warnings[0].getMessage()
        assert "retrying" in warnings[0].getMessage()
        # The session outlived the failure: the only teardown is the final cancel.
        assert harness.events.count("mqtt_disconnect") == 1
        assert harness.events.index("mqtt_disconnect") > harness.events.index("panel_reconcile", 1)

    async def test_two_consecutive_periodic_failures_rebuild_session(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        first = CommandSubscribeError("first")
        second = CommandSubscribeError("second")
        harness = _SessionHarness(
            monkeypatch,
            bridge_reconcile_effects={"panel": [None, first, second]},
        )

        with pytest.raises(CommandSubscribeError) as raised:
            await asyncio.wait_for(
                main_mod._run_session(_hot_poll_settings(resync_seconds=0), None, None),
                timeout=1,
            )

        assert raised.value is second
        assert harness.events.count("panel_reconcile") == 3
        assert harness.events[-2:] == ["bus_shutdown", "mqtt_disconnect"]

    async def test_successful_resync_resets_the_strike(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        harness = _SessionHarness(
            monkeypatch,
            bridge_reconcile_effects={
                "panel": [
                    None,
                    CommandSubscribeError("first"),
                    None,
                    CommandSubscribeError("after success"),
                    asyncio.CancelledError(),
                ]
            },
        )

        with caplog.at_level("WARNING", logger="brilliant_mqtt.__main__"):
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(
                    main_mod._run_session(_hot_poll_settings(resync_seconds=0), None, None),
                    timeout=1,
                )

        assert harness.events.count("panel_reconcile") == 5
        assert sum(1 for r in caplog.records if r.levelname == "WARNING") == 2

    async def test_mesh_resync_subscribe_failure_gets_the_same_grace(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        harness = _SessionHarness(
            monkeypatch,
            bridge_reconcile_effects={
                "panel": [None, None, None, asyncio.CancelledError()],
                "mesh": [CommandSubscribeError("mesh topic"), None],
            },
        )

        with caplog.at_level("WARNING", logger="brilliant_mqtt.__main__"):
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(
                    main_mod._run_session(
                        _hot_poll_settings(mesh=True, resync_seconds=0), None, None
                    ),
                    timeout=1,
                )

        assert harness.events.count("mesh_reconcile") == 2
        assert sum(1 for r in caplog.records if r.levelname == "WARNING") == 1

    async def test_initial_reconcile_subscribe_failure_stays_fail_fast(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        error = CommandSubscribeError("initial")
        harness = _SessionHarness(monkeypatch, bridge_reconcile_effects={"panel": [error]})

        with pytest.raises(CommandSubscribeError) as raised:
            await asyncio.wait_for(
                main_mod._run_session(_hot_poll_settings(resync_seconds=0), None, None),
                timeout=1,
            )

        assert raised.value is error
        assert harness.events.count("panel_reconcile") == 1
        assert harness.events[-2:] == ["bus_shutdown", "mqtt_disconnect"]

    async def test_other_resync_failures_rebuild_immediately(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        failure = RuntimeError("bus session broke")
        harness = _SessionHarness(
            monkeypatch,
            bridge_reconcile_effects={"panel": [None, failure]},
        )

        with pytest.raises(RuntimeError) as raised:
            await asyncio.wait_for(
                main_mod._run_session(_hot_poll_settings(resync_seconds=0), None, None),
                timeout=1,
            )

        assert raised.value is failure
        assert harness.events.count("panel_reconcile") == 2
        assert harness.events[-2:] == ["bus_shutdown", "mqtt_disconnect"]


class _OfflineOnDisconnectMqtt(_SessionMqtt):
    """_SessionMqtt that mirrors the adapter's clean-disconnect retained offline."""

    def __init__(self, events: list[str]) -> None:
        super().__init__(events)
        self.retried = asyncio.Event()

    async def disconnect(self) -> None:
        await super().disconnect()
        self.published.append(("brilliant/office/availability", "offline", True, 0))

    async def subscribe(self, topic: str) -> None:
        await super().subscribe(topic)
        if len(self.subscriptions) == 2:
            self.retried.set()


class _GrowingSessionBus(_SessionBus):
    """A second commandable device appears after the initial reconcile."""

    async def get_all(self, *, include_extras: bool = True) -> list[BrilliantDevice]:
        devices = await super().get_all(include_extras=include_extras)
        if len(self.get_all_calls) == 1:
            return [_panel_dimmer()]
        return [
            _panel_dimmer(),
            BrilliantDevice(
                device_id="device_001",
                peripheral_id="gangbox_peripheral_1",
                name="Fan",
                kind=DeviceKind.SWITCH,
                variables={"on": Variable("on", "0")},
            ),
        ] + [d for d in devices if d.device_id == "ble_mesh"]


class TestResyncSubscribeEndToEnd:
    async def test_real_bridge_survives_one_suback_timeout_during_periodic_resync(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        harness = _SessionHarness(monkeypatch, real_bridge=True)
        harness.bus = _GrowingSessionBus(harness.events)
        harness.mqtt = mqtt = _OfflineOnDisconnectMqtt(harness.events)
        # Initial reconcile subscribes the dimmer; the periodic resync finds the
        # new switch and its SUBSCRIBE gets no SUBACK once.
        mqtt.subscribe_effects = [None, MqttError("Operation timed out")]
        settings = _hot_poll_settings(resync_seconds=0)
        object.__setattr__(settings, "retained_topics_file", str(tmp_path / "owned.json"))
        object.__setattr__(settings, "bus_heartbeat_file", "")

        session = asyncio.create_task(main_mod._run_session(settings, None, None))
        retried = asyncio.create_task(mqtt.retried.wait())
        with caplog.at_level("WARNING", logger="brilliant_mqtt.__main__"):
            done, _pending = await asyncio.wait(
                {session, retried}, timeout=1, return_when=asyncio.FIRST_COMPLETED
            )
        try:
            assert retried in done  # RED today: the session task ends with MqttError
            assert not session.done()
            assert "mqtt_disconnect" not in harness.events
            assert all(payload != "offline" for _t, payload, _r, _q in mqtt.published)
            assert mqtt.subscriptions == [
                "brilliant/office/gangbox_peripheral_0/set",
                "brilliant/office/gangbox_peripheral_1/set",
            ]
            warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
            assert len(warnings) == 1
            assert "brilliant/office/gangbox_peripheral_1/set" in warnings[0]
        finally:
            retried.cancel()
            session.cancel()
            with pytest.raises((asyncio.CancelledError, MqttError)):
                await session


class TestSceneBridgeSessionWiring:
    async def test_enabled_bridge_uses_shared_adapters_and_ordered_lifecycle(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        harness = _SessionHarness(monkeypatch)
        watermark_file = tmp_path / "scene-state.json"
        settings = _scene_settings(True, str(watermark_file))

        await _cancel_ready_session(harness, settings)

        assert len(harness.scene_instances) == 1
        assert harness.scene_bus is harness.bus
        assert harness.scene_mqtt is harness.mqtt
        assert harness.scene_panel == "office"
        assert harness.scene_watermark_path == watermark_file
        assert isinstance(harness.scene_watermark_path, Path)
        assert harness.scene_clock_ms is not None
        assert isinstance(harness.scene_clock_ms(), int)

        order = harness.events.index
        assert order("panel_bridge_construct") < order("mqtt_connect")
        assert order("scene_bridge_construct") < order("mqtt_connect")
        assert order("mqtt_connect") < order("bus_start")
        assert order("bus_start") < order("scene_bridge_start")
        assert harness.events[-3:] == [
            "scene_bridge_shutdown",
            "bus_shutdown",
            "mqtt_disconnect",
        ]

    async def test_disabled_session_never_constructs_or_starts_scene_bridge(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        harness = _SessionHarness(monkeypatch)
        settings = _scene_settings(False, str(tmp_path / "unused.json"))

        await _cancel_ready_session(harness, settings)

        assert harness.scene_instances == []
        assert not any(event.startswith("scene_bridge_") for event in harness.events)
        assert scene_command_topic("office") not in harness.mqtt.subscriptions
        assert mode_command_topic("office") not in harness.mqtt.subscriptions

    async def test_scene_startup_failure_unwinds_shared_session_in_order(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        start_error = RuntimeError("scene startup failed")
        harness = _SessionHarness(monkeypatch, scene_start_error=start_error)
        settings = _scene_settings(True, str(tmp_path / "scene-state.json"))

        with pytest.raises(RuntimeError, match="scene startup failed") as raised:
            await asyncio.wait_for(main_mod._run_session(settings, None, None), timeout=1)

        assert raised.value is start_error
        assert harness.events[-3:] == [
            "scene_bridge_shutdown",
            "bus_shutdown",
            "mqtt_disconnect",
        ]

    async def test_scene_shutdown_failure_does_not_skip_shared_adapter_cleanup(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        harness = _SessionHarness(
            monkeypatch, scene_shutdown_error=RuntimeError("scene shutdown failed")
        )
        settings = _scene_settings(True, str(tmp_path / "scene-state.json"))

        await _cancel_ready_session(harness, settings)

        assert harness.events[-3:] == [
            "scene_bridge_shutdown",
            "bus_shutdown",
            "mqtt_disconnect",
        ]


class TestRetainedLedgerDegradedDiagnostic:
    async def test_load_failure_publishes_retained_diagnostic_then_preserves_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ledger_error = RetainedLedgerError("invalid retained ledger")

        class FailingLedger:
            def __init__(self, panel_slug: str, path: Path) -> None:
                del self, panel_slug, path

            async def async_load(self) -> None:
                raise ledger_error

        monkeypatch.setattr(main_mod, "RetainedTopicLedger", FailingLedger)
        harness = _SessionHarness(monkeypatch)

        with pytest.raises(RetainedLedgerError) as raised:
            await main_mod._run_session(_settings(), None, None)

        assert raised.value is ledger_error
        assert harness.mqtt.published == [
            (
                "brilliant/office/bridge",
                json.dumps(
                    {
                        "agent_version": __version__,
                        "degraded": "retained_ledger",
                    },
                    sort_keys=True,
                ),
                True,
                1,
            )
        ]
        assert harness.events == [
            "mqtt_construct",
            "bus_construct",
            "mqtt_connect",
            "mqtt_publish",
            "bus_shutdown",
            "mqtt_disconnect",
        ]

    async def test_runtime_failure_uses_existing_connection_for_diagnostic(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ledger_error = RetainedLedgerError("could not persist retained ledger")
        harness = _SessionHarness(
            monkeypatch,
            bridge_reconcile_error=ledger_error,
        )
        with pytest.raises(RetainedLedgerError) as raised:
            await main_mod._run_session(_settings(), None, None)

        assert raised.value is ledger_error
        assert harness.events.count("mqtt_connect") == 1
        assert harness.mqtt.published[0][0] == "brilliant/office/bridge"
        assert harness.mqtt.published[0][2:] == (True, 1)
        assert harness.events[-2:] == ["bus_shutdown", "mqtt_disconnect"]

    @pytest.mark.parametrize("failure_point", ["connect", "publish"])
    async def test_diagnostic_failure_does_not_replace_ledger_error(
        self, monkeypatch: pytest.MonkeyPatch, failure_point: str
    ) -> None:
        ledger_error = RetainedLedgerError("invalid retained ledger")

        class FailingLedger:
            def __init__(self, panel_slug: str, path: Path) -> None:
                del self, panel_slug, path

            async def async_load(self) -> None:
                raise ledger_error

        monkeypatch.setattr(main_mod, "RetainedTopicLedger", FailingLedger)
        harness = _SessionHarness(monkeypatch)
        failure = RuntimeError(f"diagnostic {failure_point} failed")
        setattr(harness.mqtt, f"{failure_point}_error", failure)

        with pytest.raises(RetainedLedgerError) as raised:
            await main_mod._run_session(_settings(), None, None)

        assert raised.value is ledger_error
        assert harness.events[-2:] == ["bus_shutdown", "mqtt_disconnect"]
