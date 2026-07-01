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
from pathlib import Path

import pytest

import brilliant_mqtt.__main__ as main_mod
from brilliant_mqtt.__main__ import _is_panel_device, _is_reconnect_storm, _make_desired
from brilliant_mqtt.bridge import Bridge
from brilliant_mqtt.config import Settings
from brilliant_mqtt.desired_state import DesiredState
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
