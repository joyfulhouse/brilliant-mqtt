"""Tests for run.py: guard-gate logic and load_config defaults/env coverage."""

from __future__ import annotations

import pytest

from brilliant_wifi_watchdog import probe, run
from brilliant_wifi_watchdog.ladder import Action, Thresholds
from brilliant_wifi_watchdog.reboot_guard import GuardPolicy


class FakeGuard:
    def __init__(self, ok: bool) -> None:
        self.ok = ok
        self.recorded: list[float] = []

    def can_reboot(self, now: float) -> bool:
        return self.ok

    def record(self, now: float) -> None:
        self.recorded.append(now)


class FakeRecovery:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def gpio_reset_and_reboot(self) -> None:
        self.calls.append("reboot")

    def soft_reconnect(self) -> None:
        self.calls.append("soft")

    def restart_services(self) -> None:
        self.calls.append("restart")


# ---------------------------------------------------------------------------
# handle — reboot guard gate (the safety-critical path; from the brief)
# ---------------------------------------------------------------------------


def test_reboot_blocked_when_guard_denies() -> None:
    g, rec = FakeGuard(False), FakeRecovery()
    run.handle(Action.GPIO_RESET_REBOOT, guard=g, now=0.0, recovery_mod=rec)
    assert rec.calls == [] and g.recorded == []  # no reboot, not recorded


def test_reboot_runs_and_records_when_allowed() -> None:
    """Reboot guard is recorded BEFORE the reboot fires (crash-safe stamp ordering)."""
    order: list[str] = []

    class TrackingGuard(FakeGuard):
        def record(self, now: float) -> None:
            super().record(now)
            order.append("record")

    class TrackingRecovery(FakeRecovery):
        def gpio_reset_and_reboot(self) -> None:
            super().gpio_reset_and_reboot()
            order.append("reboot")

    g, rec = TrackingGuard(True), TrackingRecovery()
    run.handle(Action.GPIO_RESET_REBOOT, guard=g, now=5.0, recovery_mod=rec)
    assert rec.calls == ["reboot"] and g.recorded == [5.0]
    # Stamp written to disk before the reboot command fires so a crash/power cut
    # during reboot still counts against the cap (no infinite reboot loop).
    assert order == ["record", "reboot"]


# ---------------------------------------------------------------------------
# handle — other action dispatches (full branch coverage)
# ---------------------------------------------------------------------------


def test_soft_reconnect_dispatches() -> None:
    g, rec = FakeGuard(True), FakeRecovery()
    run.handle(Action.SOFT_RECONNECT, guard=g, now=0.0, recovery_mod=rec)
    assert rec.calls == ["soft"]


def test_restart_services_dispatches() -> None:
    g, rec = FakeGuard(True), FakeRecovery()
    run.handle(Action.RESTART_SERVICES, guard=g, now=0.0, recovery_mod=rec)
    assert rec.calls == ["restart"]


# ---------------------------------------------------------------------------
# load_config
# ---------------------------------------------------------------------------


def test_load_config_defaults() -> None:
    cfg = run.load_config({})
    assert cfg.interval == 30.0
    assert cfg.gateway is None
    assert cfg.broker_host is None
    assert cfg.broker_port == 1883
    assert cfg.log_path == "/var/brilliant-mqtt/wifi-watchdog.log"
    assert cfg.state_path == "/var/brilliant-mqtt/wifi-watchdog.state"
    assert cfg.thresholds == Thresholds(soft_after=90.0, restart_after=180.0, reboot_after=360.0)
    assert cfg.policy == GuardPolicy(cooldown=3600.0, cap=3, window=21600.0)


def test_load_config_from_env() -> None:
    env: dict[str, str] = {
        "WIFI_WATCHDOG_INTERVAL": "15",
        "WIFI_WATCHDOG_GATEWAY": "10.0.0.1",
        "MQTT_HOST": "broker.local",
        "MQTT_PORT": "1884",
        "WIFI_WATCHDOG_LOG": "/tmp/ww.log",
        "WIFI_WATCHDOG_STATE": "/tmp/ww.state",
        "WIFI_WATCHDOG_SOFT_AFTER": "60",
        "WIFI_WATCHDOG_RESTART_AFTER": "120",
        "WIFI_WATCHDOG_REBOOT_AFTER": "240",
        "WIFI_WATCHDOG_REBOOT_COOLDOWN": "7200",
        "WIFI_WATCHDOG_REBOOT_CAP": "5",
        "WIFI_WATCHDOG_REBOOT_WINDOW": "43200",
    }
    cfg = run.load_config(env)
    assert cfg.interval == 15.0
    assert cfg.gateway == "10.0.0.1"
    assert cfg.broker_host == "broker.local"
    assert cfg.broker_port == 1884
    assert cfg.thresholds.soft_after == 60.0
    assert cfg.policy.cap == 5


def test_load_config_invalid_float_falls_back_to_default() -> None:
    cfg = run.load_config({"WIFI_WATCHDOG_INTERVAL": "not-a-number"})
    assert cfg.interval == 30.0


# ---------------------------------------------------------------------------
# _poll_once — one watchdog cycle. The optional broker diagnostic must never
# block the recovery decision nor, on its own, drive recovery; the loop keeps
# polling across cycles even when the diagnostic times out.
# ---------------------------------------------------------------------------


class SpyLadder:
    def __init__(self, action: Action = Action.NONE) -> None:
        self._action = action
        self.observed: list[bool] = []

    def observe(self, *, gateway_up: bool, now: float) -> Action:
        self.observed.append(gateway_up)
        return self._action


def test_poll_once_reaches_ladder_when_broker_diagnostic_is_inconclusive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(probe, "ping", lambda gw: True)  # gateway healthy
    monkeypatch.setattr(probe, "tcp_open", lambda h, p: probe.TcpProbe.INCONCLUSIVE)
    cfg = run.load_config({"MQTT_HOST": "broker", "WIFI_WATCHDOG_GATEWAY": "10.0.0.1"})
    guard, ladder = FakeGuard(True), SpyLadder(Action.NONE)

    run._poll_once(cfg, guard=guard, ladder=ladder)
    run._poll_once(cfg, guard=guard, ladder=ladder)  # continues polling after the diagnostic

    # The ladder was reached on every cycle (diagnostic never blocked it), and it
    # saw only the ping-derived health — a timed-out diagnostic drove no reboot.
    assert ladder.observed == [True, True]
    assert guard.recorded == []


def test_poll_once_dispatches_the_action_the_ladder_returns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(probe, "ping", lambda gw: False)  # gateway down
    handled: list[Action] = []
    monkeypatch.setattr(run, "handle", lambda action, **kw: handled.append(action))
    cfg = run.load_config({"WIFI_WATCHDOG_GATEWAY": "10.0.0.1"})  # no broker diagnostic
    run._poll_once(cfg, guard=FakeGuard(True), ladder=SpyLadder(Action.SOFT_RECONNECT))
    assert handled == [Action.SOFT_RECONNECT]


def test_poll_once_skips_broker_diagnostic_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(probe, "ping", lambda gw: True)

    def _boom(host: str, port: int) -> probe.TcpProbe:
        raise AssertionError("tcp_open must not be called when no broker host is configured")

    monkeypatch.setattr(probe, "tcp_open", _boom)
    cfg = run.load_config({"WIFI_WATCHDOG_GATEWAY": "10.0.0.1"})
    run._poll_once(cfg, guard=FakeGuard(True), ladder=SpyLadder(Action.NONE))
