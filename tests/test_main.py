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

import pytest

import brilliant_mqtt.__main__ as main_mod
from brilliant_mqtt.__main__ import _is_panel_device, _is_reconnect_storm, _make_desired
from brilliant_mqtt.bridge import Bridge
from brilliant_mqtt.config import Settings
from brilliant_mqtt.desired_state import DesiredState
from brilliant_mqtt.ha_control_protocol import mode_command_topic, scene_command_topic
from brilliant_mqtt.mesh_leader import MESH_LEADER_TOPIC, MeshLeader
from brilliant_mqtt.model import BrilliantDevice, DeviceKind, Variable
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

    def on_reconnect(self, callback: Callable[[], Awaitable[None]]) -> None:
        self.events.append("bus_reconnect_callback")
        self.reconnect_callback = callback

    async def start(self) -> None:
        self.events.append("bus_start")

    async def shutdown(self) -> None:
        self.events.append("bus_shutdown")

    def seconds_since_last_push(self) -> float | None:
        return None

    def recent_reconnects(self, window_seconds: float) -> int:
        del window_seconds
        return 0


class _SessionMqtt:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.subscriptions: list[str] = []

    async def connect(self) -> None:
        self.events.append("mqtt_connect")

    async def disconnect(self) -> None:
        self.events.append("mqtt_disconnect")

    async def subscribe(self, topic: str) -> None:
        self.subscriptions.append(topic)


class _SessionHarness:
    def __init__(
        self,
        monkeypatch: pytest.MonkeyPatch,
        *,
        scene_start_error: RuntimeError | None = None,
        scene_shutdown_error: RuntimeError | None = None,
    ) -> None:
        self.events: list[str] = []
        self.ready = asyncio.Event()
        self.bus = _SessionBus(self.events)
        self.mqtt = _SessionMqtt(self.events)
        self.scene_start_error = scene_start_error
        self.scene_shutdown_error = scene_shutdown_error
        self.scene_instances: list[object] = []
        self.scene_bus: object | None = None
        self.scene_mqtt: object | None = None
        self.scene_panel: str | None = None
        self.scene_watermark_path: object | None = None
        self.scene_clock_ms: Callable[[], int] | None = None

        harness = self

        class SessionBridge:
            def __init__(self, *args: object, **kwargs: object) -> None:
                del self, args, kwargs
                harness.events.append("panel_bridge_construct")

            async def reconcile(self) -> None:
                harness.events.append("panel_reconcile")
                harness.ready.set()

            async def poll_once(self) -> None:
                return

            async def withdraw(self) -> None:
                return

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
        monkeypatch.setattr(main_mod, "Bridge", SessionBridge)
        monkeypatch.setattr(main_mod, "SceneBridge", SessionSceneBridge, raising=False)


async def _cancel_ready_session(harness: _SessionHarness, settings: Settings) -> None:
    task = asyncio.create_task(main_mod._run_session(settings, None, None))
    await asyncio.wait_for(harness.ready.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


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
