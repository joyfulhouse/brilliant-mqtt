"""Tests for run.py: guard-gate logic and load_config defaults/env coverage."""

from __future__ import annotations

import logging
from pathlib import Path
from types import SimpleNamespace

import pytest

from brilliant_wifi_watchdog import bounded, probe, recovery, run
from brilliant_wifi_watchdog.ladder import Action, Ladder, Thresholds
from brilliant_wifi_watchdog.reboot_guard import GuardPolicy, RebootGuard


class FakeGuard:
    def __init__(self, ok: bool) -> None:
        self.ok = ok
        self.recorded: list[float] = []

    def can_request(self, now: float) -> bool:
        return self.ok

    def record_request(self, now: float) -> None:
        self.recorded.append(now)


class FlappingGuard:
    """A guard whose reads flap; an independent second read could catch a different
    answer than the first, as a transient flash hiccup (OSError → []) can too."""

    def __init__(self, answers: list[bool]) -> None:
        self.answers = list(answers)
        self.recorded: list[float] = []
        self.requested = False

    def can_request(self, now: float) -> bool:
        if self.requested:
            return False
        return self.answers.pop(0) if self.answers else True

    def record_request(self, now: float) -> None:
        self.recorded.append(now)
        self.requested = True


class FakeRecovery:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def gpio_reset_and_reboot(self) -> int:
        self.calls.append("reboot")
        return 0

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
    g, rec = FakeGuard(True), FakeRecovery()  # guard.can_request is not consulted here
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
        def record_request(self, now: float) -> None:
            super().record_request(now)
            order.append("record")

    class TrackingRecovery(FakeRecovery):
        def gpio_reset_and_reboot(self) -> int:
            result = super().gpio_reset_and_reboot()
            order.append("reboot")
            return result

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
        eligible = g.can_request(wall)  # the ONLY read this poll
        action = lad.observe(gateway_up=False, now=wall, reboot_eligible=eligible)
        if action != Action.NONE:
            run.handle(action, guard=g, now=wall, recovery_mod=rec, reboot_eligible=eligible)
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
        self.observed: list[bool | None] = []
        self.eligibility: list[bool] = []

    def observe(self, *, gateway_up: bool | None, now: float, reboot_eligible: bool) -> Action:
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
    is reached each cycle and observes ONLY gateway-derived health, and no reboot
    is driven. The diagnostic is log-only (contract 5)."""
    monkeypatch.setattr(
        probe, "gateway_probe", lambda gw: (gw, probe.TcpProbe.OPEN)
    )  # gateway healthy
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
    monkeypatch.setattr(probe, "gateway_probe", lambda gw: (gw, probe.TcpProbe.CLOSED))
    handled: list[Action] = []
    monkeypatch.setattr(run, "handle", lambda action, **kw: handled.append(action))
    cfg = run.load_config({"WIFI_WATCHDOG_GATEWAY": "10.0.0.1"})  # no broker diagnostic
    run._poll_once(cfg, guard=FakeGuard(True), ladder=SpyLadder(Action.SOFT_RECONNECT))
    assert handled == [Action.SOFT_RECONNECT]


def test_poll_once_skips_broker_diagnostic_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(probe, "gateway_probe", lambda gw: (gw, probe.TcpProbe.OPEN))

    def _boom(host: str, port: int) -> probe.TcpProbe:
        raise AssertionError("tcp_open must not be called when no broker host is configured")

    monkeypatch.setattr(probe, "tcp_open", _boom)
    cfg = run.load_config({"WIFI_WATCHDOG_GATEWAY": "10.0.0.1"})
    run._poll_once(cfg, guard=FakeGuard(True), ladder=SpyLadder(Action.NONE))


def _run_reboot_schedule(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    times: list[float],
    cooldown: float,
) -> list[float]:
    cfg = run.load_config(
        {
            "WIFI_WATCHDOG_GATEWAY": "192.0.2.1",
            "WIFI_WATCHDOG_REBOOT_COOLDOWN": str(cooldown),
        }
    )
    guard = RebootGuard(str(tmp_path / "guard"), cfg.policy)
    ladder = Ladder(cfg.thresholds)
    now = [0.0]
    requests: list[float] = []

    monkeypatch.setattr(probe, "gateway_probe", lambda gateway: (gateway, probe.TcpProbe.CLOSED))
    monkeypatch.setattr(recovery, "soft_reconnect", lambda: None)
    monkeypatch.setattr(recovery, "restart_services", lambda: None)

    def request_reboot() -> int:
        requests.append(now[0])
        return bounded.TIMEOUT_RC

    monkeypatch.setattr(recovery, "gpio_reset_and_reboot", request_reboot)
    monkeypatch.setattr(run, "time", SimpleNamespace(time=lambda: now[0], monotonic=lambda: now[0]))
    for value in times:
        now[0] = value
        run._poll_once(cfg, guard=guard, ladder=ladder)
    return requests


def test_sparse_polling_retries_at_the_next_eligible_sample(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    requests = _run_reboot_schedule(
        tmp_path,
        monkeypatch,
        times=[0.0, 30.0, 60.0, 90.0, 180.0, 360.0, 4000.0],
        cooldown=3600.0,
    )
    assert requests == [360.0, 4000.0]


def test_zero_cooldown_retries_until_the_attempt_cap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    requests = _run_reboot_schedule(
        tmp_path,
        monkeypatch,
        times=[float(value) for value in range(0, 451, 30)],
        cooldown=0.0,
    )
    assert requests == [360.0, 390.0, 420.0]


@pytest.mark.parametrize(
    "gateway_env",
    [{}, {"WIFI_WATCHDOG_GATEWAY": "192.0.2.1"}],
    ids=["route-timeout", "ping-timeout"],
)
def test_timed_out_gateway_probes_do_not_authorize_wifi_reboot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    gateway_env: dict[str, str],
) -> None:
    cfg = run.load_config({**gateway_env, "MQTT_HOST": "broker"})
    guard = RebootGuard(str(tmp_path / "guard"), GuardPolicy())
    ladder = Ladder(Thresholds())
    actions: list[Action] = []
    now = [0.0]

    monkeypatch.setattr(
        bounded,
        "run_bounded",
        lambda *args, **kwargs: bounded.Completed(bounded.TIMEOUT_RC, "", True),
    )
    monkeypatch.setattr(probe, "tcp_open", lambda host, port: probe.TcpProbe.OPEN)
    monkeypatch.setattr(run, "handle", lambda action, **kwargs: actions.append(action))
    monkeypatch.setattr(run, "time", SimpleNamespace(time=lambda: now[0], monotonic=lambda: now[0]))

    assert probe.ping("192.0.2.1") is False
    for value in range(0, 391, 30):
        now[0] = float(value)
        run._poll_once(cfg, guard=guard, ladder=ladder)

    assert Action.GPIO_RESET_REBOOT not in actions, actions


def test_completed_gateway_failure_still_escalates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    completed_failure = bounded.Completed(1, "", False)
    assert probe.gateway_probe("192.0.2.1", run=lambda argv, capture: completed_failure) == (
        "192.0.2.1",
        probe.TcpProbe.CLOSED,
    )

    cfg = run.load_config({"WIFI_WATCHDOG_GATEWAY": "192.0.2.1"})
    guard = RebootGuard(str(tmp_path / "guard"), GuardPolicy())
    ladder = Ladder(Thresholds())
    actions: list[Action] = []
    now = [0.0]
    monkeypatch.setattr(bounded, "run_bounded", lambda *args, **kwargs: completed_failure)
    monkeypatch.setattr(run, "handle", lambda action, **kwargs: actions.append(action))
    monkeypatch.setattr(run, "time", SimpleNamespace(time=lambda: now[0], monotonic=lambda: now[0]))

    for value in range(0, 391, 30):
        now[0] = float(value)
        run._poll_once(cfg, guard=guard, ladder=ladder)

    assert Action.GPIO_RESET_REBOOT in actions


def test_completed_gateway_success_resets_the_outage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    completed_failure = bounded.Completed(1, "", False)
    completed_success = bounded.Completed(0, "", False)
    assert probe.gateway_probe("192.0.2.1", run=lambda argv, capture: completed_success) == (
        "192.0.2.1",
        probe.TcpProbe.OPEN,
    )

    cfg = run.load_config({"WIFI_WATCHDOG_GATEWAY": "192.0.2.1"})
    guard = RebootGuard(str(tmp_path / "guard"), GuardPolicy())
    ladder = Ladder(Thresholds())
    actions: list[tuple[float, Action]] = []
    now = [0.0]
    result = [completed_failure]
    monkeypatch.setattr(bounded, "run_bounded", lambda *args, **kwargs: result[0])
    monkeypatch.setattr(run, "handle", lambda action, **kwargs: actions.append((now[0], action)))
    monkeypatch.setattr(run, "time", SimpleNamespace(time=lambda: now[0], monotonic=lambda: now[0]))

    for value in range(0, 331, 30):
        now[0] = float(value)
        run._poll_once(cfg, guard=guard, ladder=ladder)
    result[0] = completed_success
    now[0] = 360.0
    run._poll_once(cfg, guard=guard, ladder=ladder)
    result[0] = completed_failure
    for value in range(390, 751, 30):
        now[0] = float(value)
        run._poll_once(cfg, guard=guard, ladder=ladder)

    reboot_times = [when for when, action in actions if action == Action.GPIO_RESET_REBOOT]
    assert reboot_times == [750.0]
