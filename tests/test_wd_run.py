"""Tests for run.py: guard-gate logic and load_config defaults/env coverage."""

from __future__ import annotations

import logging

import pytest

from brilliant_wifi_watchdog import probe, run
from brilliant_wifi_watchdog.ladder import Action, Ladder, Thresholds
from brilliant_wifi_watchdog.reboot_guard import GuardPolicy


class FakeGuard:
    def __init__(self, ok: bool) -> None:
        self.ok = ok
        self.recorded: list[float] = []

    def can_reboot(self, now: float) -> bool:
        return self.ok

    def record(self, now: float) -> None:
        self.recorded.append(now)


class FlappingGuard:
    """A guard whose reads flap; an independent second read could catch a different
    answer than the first, as a transient flash hiccup (OSError → []) can too."""

    def __init__(self, answers: list[bool]) -> None:
        self.answers = list(answers)
        self.recorded: list[float] = []

    def can_reboot(self, now: float) -> bool:
        return self.answers.pop(0) if self.answers else True

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


def test_reboot_blocked_when_guard_denies(caplog: pytest.LogCaptureFixture) -> None:
    """handle() acts on the eligibility passed in (the ladder's own guard read this
    poll), not a second independent read that could desync.  An ineligible reboot is
    never silent — it logs the blocked path and does nothing (issue #91)."""
    g, rec = FakeGuard(True), FakeRecovery()  # guard.can_reboot is not consulted here
    with caplog.at_level(logging.ERROR, logger="brilliant_wifi_watchdog"):
        run.handle(
            Action.GPIO_RESET_REBOOT, guard=g, now=0.0, recovery_mod=rec, reboot_eligible=False
        )
    assert rec.calls == [] and g.recorded == []  # no reboot, not recorded
    assert any("blocked" in r.getMessage() for r in caplog.records)  # observable, not silent


def test_escalate_notify_logs_once_without_side_effects(caplog: pytest.LogCaptureFixture) -> None:
    """A deferred reboot notifies (one log line) and touches neither guard nor
    recovery — the retry re-arms automatically once the guard clears (issue #91)."""
    g, rec = FakeGuard(True), FakeRecovery()
    with caplog.at_level(logging.ERROR, logger="brilliant_wifi_watchdog"):
        run.handle(Action.ESCALATE_NOTIFY, guard=g, now=0.0, recovery_mod=rec)
    assert rec.calls == [] and g.recorded == []  # no reboot, nothing recorded
    notices = [r for r in caplog.records if "deferred" in r.getMessage()]
    assert len(notices) == 1


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
    run.handle(Action.GPIO_RESET_REBOOT, guard=g, now=5.0, recovery_mod=rec, reboot_eligible=True)
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
# ladder + handle wiring — a single guard read shared per poll (issue #91)
# ---------------------------------------------------------------------------


def test_reboot_not_lost_when_guard_read_would_flap() -> None:
    """One guard read per poll, shared by observe() and handle() (the exact wiring
    main() uses).  A guard whose answer flaps — which a second, independent read in
    handle() could catch mid-False after the ladder already committed "reboot" to
    _fired — can no longer strand the reboot and drop it silently (issue #91)."""
    g = FlappingGuard([True] * 13 + [False])  # a double read would disagree at t=360
    rec, lad = FakeRecovery(), Ladder(Thresholds())
    for i in range(60):
        wall = 30.0 * i
        eligible = g.can_reboot(wall)  # the ONLY read this poll
        action = lad.observe(gateway_up=False, now=wall, reboot_eligible=eligible)
        if action != Action.NONE:
            run.handle(action, guard=g, now=wall, recovery_mod=rec, reboot_eligible=eligible)
        if action == Action.GPIO_RESET_REBOOT:
            break  # a successful request replaces the running process
    assert rec.calls.count("reboot") == 1  # fired once, never lost
    assert g.recorded == [360.0]  # and recorded against the cap


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
        self.eligibility: list[bool] = []

    def observe(self, *, gateway_up: bool, now: float, reboot_eligible: bool) -> Action:
        self.observed.append(gateway_up)
        self.eligibility.append(reboot_eligible)
        return self._action


@pytest.mark.parametrize(
    "broker_result",
    [probe.TcpProbe.OPEN, probe.TcpProbe.CLOSED, probe.TcpProbe.INCONCLUSIVE],
)
def test_poll_once_broker_diagnostic_never_influences_the_decision(
    monkeypatch: pytest.MonkeyPatch, broker_result: probe.TcpProbe
) -> None:
    """Whatever the broker diagnostic reports — up, down, or timed out — the ladder
    is reached each cycle and observes ONLY the ping-derived health, and no reboot
    is driven. The diagnostic is log-only (contract 5)."""
    monkeypatch.setattr(probe, "ping", lambda gw: True)  # gateway healthy
    monkeypatch.setattr(probe, "tcp_open", lambda h, p: broker_result)
    cfg = run.load_config({"MQTT_HOST": "broker", "WIFI_WATCHDOG_GATEWAY": "10.0.0.1"})
    guard, ladder = FakeGuard(True), SpyLadder(Action.NONE)

    run._poll_once(cfg, guard=guard, ladder=ladder)
    run._poll_once(cfg, guard=guard, ladder=ladder)  # continues polling after the diagnostic

    assert ladder.observed == [True, True]  # ping-only health, independent of broker_result
    assert ladder.eligibility == [True, True]  # the single shared guard read reached the ladder
    assert guard.recorded == []  # diagnostic drove no reboot in any state


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
