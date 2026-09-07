"""Integration: Ladder + REAL RebootGuard, proving deferred reboots re-arm.

Regression coverage for issue #91.  ``Ladder.observe`` marked the reboot rung
fired the instant its threshold elapsed, BEFORE ``run.handle`` consulted the
persistent ``RebootGuard``.  When cooldown or the rolling cap blocked the
reboot, ``"reboot"`` was already in ``_fired`` for that outage, so the ladder
never asked again — an outage that never recovered got exactly one blocked
attempt and then permanent silence, even after cooldown/cap later cleared.

These tests drive the real ``Ladder`` and the real ``RebootGuard`` through the
same coupling ``run.main()`` uses (eligibility read from the guard and fed into
``observe``; GPIO reboots recorded by ``handle``).  The issue's acceptance
criterion is explicit that guard-only expiry tests cannot detect this bug —
the defect lives in how the ladder and the guard are wired together.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from brilliant_wifi_watchdog import bounded, recovery, run
from brilliant_wifi_watchdog.ladder import Action, Ladder, Thresholds
from brilliant_wifi_watchdog.reboot_guard import GuardPolicy, RebootGuard

T = Thresholds()  # defaults: soft 90s, restart 180s, reboot 360s
P = GuardPolicy(cooldown=3600.0, cap=3, window=21600.0)


class FakeRecovery:
    """Records recovery side effects instead of touching the panel."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def gpio_reset_and_reboot(self) -> int:
        self.calls.append("reboot")
        return 0

    def soft_reconnect(self) -> None:
        self.calls.append("soft")

    def restart_services(self) -> None:
        self.calls.append("restart")


def _poll(
    ladder: Ladder,
    guard: RebootGuard,
    rec: FakeRecovery,
    *,
    gateway_up: bool,
    t: float,
) -> tuple[Action, bool]:
    """One watchdog iteration, mirroring ``run.main()``'s ladder/guard coupling.

    ``run.main()`` reads request eligibility ONCE from wall-clock time and shares it
    with both ``observe`` and ``handle`` (no second, independent guard read that
    could desync); ``handle`` records the attempt and pending boot identity before
    recovery. A single ``t`` stands in for both the wall clock (guard) and the
    monotonic clock (ladder); in production they advance together. Returns the
    action and this poll's eligibility.
    """
    eligible = guard.can_request(t)
    action = ladder.observe(gateway_up=gateway_up, now=t, reboot_eligible=eligible)
    if action != Action.NONE:
        result = run.handle(action, guard=guard, now=t, recovery_mod=rec, reboot_eligible=eligible)
        if result is not None:
            ladder.reboot_request_returned()
    return action, eligible


def _stamps(path: Path) -> list[float]:
    return [float(x) for x in json.loads(path.read_text(encoding="utf-8"))]


# ---------------------------------------------------------------------------
# Acceptance 1 — cooldown expiry re-arms a deferred reboot (the issue's repro)
# ---------------------------------------------------------------------------


def test_deferred_reboot_rearms_after_cooldown_expires(tmp_path: Path) -> None:
    state = tmp_path / "s.json"
    boot_id = ["boot-a"]
    guard = RebootGuard(str(state), P, read_boot_id=lambda: boot_id[0])
    guard.record(0.0)  # a previous reboot → cooldown blocks reboots until t>=3600
    ladder, rec = Ladder(T), FakeRecovery()

    log: list[tuple[float, Action, bool]] = []
    rebooted = False
    t = 100.0  # continuous outage begins at t=100 (issue's numbers)
    while t <= 25000.0:  # poll well past the 3600s cooldown
        action, eligible = _poll(ladder, guard, rec, gateway_up=rebooted, t=t)
        log.append((t, action, eligible))
        if action == Action.GPIO_RESET_REBOOT:
            assert _stamps(state) == [0.0, t]
            boot_id[0] = "boot-b"
            ladder = Ladder(T)  # the new boot starts a fresh daemon process
            rebooted = True
        t += 30.0

    escalations = [(ts, e) for ts, a, e in log if a == Action.ESCALATE_NOTIFY]
    reboots = [(ts, e) for ts, a, e in log if a == Action.GPIO_RESET_REBOOT]

    # Deferred exactly once, at the reboot threshold (~t=460), while blocked.
    assert len(escalations) == 1
    assert escalations[0] == (460.0, False)
    # The ladder never returned a reboot while the guard blocked it (it defends
    # the guard: handle's own can_reboot check is therefore never the blocker).
    assert all(eligible is True for _, eligible in reboots)
    # Exactly one request, only after cooldown expired. Polling continues after a
    # real boot transition and confirmed recovery without re-firing or notifying.
    assert len(reboots) == 1
    reboot_t, reboot_eligible = reboots[0]
    assert reboot_t >= 3600.0
    assert reboot_eligible is True
    assert rec.calls == ["soft", "restart", "reboot"]
    assert _stamps(state) == [0.0, reboot_t]
    assert not Path(f"{state}.request").exists()
    assert guard.can_reboot(reboot_t + 30.0) is False
    assert guard.can_request(reboot_t + 30.0) is False


# ---------------------------------------------------------------------------
# Acceptance 2 — cap-window expiry re-arms a deferred reboot
# ---------------------------------------------------------------------------


def test_deferred_reboot_rearms_after_cap_window_expires(tmp_path: Path) -> None:
    state = tmp_path / "s.json"
    boot_id = ["boot-a"]
    guard = RebootGuard(str(state), P, read_boot_id=lambda: boot_id[0])
    for stamp in (0.0, 3601.0, 7202.0):  # fill the cap, cooldown-spaced
        guard.record(stamp)
    ladder, rec = Ladder(T), FakeRecovery()

    log: list[tuple[float, Action, bool]] = []
    rebooted = False
    t = 8000.0  # outage begins while the cap is full
    while t <= 30000.0:  # poll past the 21600s rolling window
        action, eligible = _poll(ladder, guard, rec, gateway_up=rebooted, t=t)
        log.append((t, action, eligible))
        if action == Action.GPIO_RESET_REBOOT:
            assert _stamps(state) == [3601.0, 7202.0, t]
            boot_id[0] = "boot-b"
            ladder = Ladder(T)
            rebooted = True
        t += 30.0

    escalations = [(ts, e) for ts, a, e in log if a == Action.ESCALATE_NOTIFY]
    reboots = [(ts, e) for ts, a, e in log if a == Action.GPIO_RESET_REBOOT]

    # Deferred exactly once while capped; never rebooted while the guard blocked.
    assert len(escalations) == 1
    assert escalations[0][1] is False
    assert all(eligible is True for _, eligible in reboots)
    # Re-armed once the oldest stamp aged out and reopened the cap.
    assert len(reboots) == 1
    reboot_t, reboot_eligible = reboots[0]
    assert reboot_eligible is True
    assert _stamps(state) == [3601.0, 7202.0, reboot_t]
    assert not Path(f"{state}.request").exists()


# ---------------------------------------------------------------------------
# Acceptance 3 — repeated outages: recovery resets, then fresh escalation defers
# ---------------------------------------------------------------------------


def test_recovery_between_outages_rearms_fresh_escalation(tmp_path: Path) -> None:
    guard = RebootGuard(str(tmp_path / "s.json"), P)
    guard.record(0.0)  # cooldown blocks reboots until t>=3600
    ladder, rec = Ladder(T), FakeRecovery()

    # Outage 1: climbs to a reboot that the cooldown defers (never actually reboots).
    o1: list[Action] = []
    t = 100.0
    while t <= 700.0:  # past the reboot threshold at ~460
        action, _ = _poll(ladder, guard, rec, gateway_up=False, t=t)
        o1.append(action)
        t += 30.0
    assert Action.ESCALATE_NOTIFY in o1
    assert Action.GPIO_RESET_REBOOT not in o1  # blocked by cooldown, deferred

    # Genuine recovery mid-deferral → reset() (clears _reboot_deferred too).
    assert _poll(ladder, guard, rec, gateway_up=True, t=730.0)[0] == Action.NONE

    # Outage 2 (still inside the cooldown): escalates soft→restart→deferred-reboot
    # afresh, independent of outage 1, deferring again exactly once (no spam loop).
    o2: list[Action] = []
    t = 1000.0
    while t <= 1600.0:
        action, _ = _poll(ladder, guard, rec, gateway_up=False, t=t)
        o2.append(action)
        t += 30.0
    assert Action.SOFT_RECONNECT in o2
    assert Action.RESTART_SERVICES in o2
    assert o2.count(Action.ESCALATE_NOTIFY) == 1  # deferred once, not per-poll
    assert Action.GPIO_RESET_REBOOT not in o2  # still within cooldown
    # Two full climbs, each soft+restart; the deferred reboots never touch recovery.
    assert rec.calls == ["soft", "restart", "soft", "restart"]


def test_reboot_without_recovery_does_not_refire_or_loop(tmp_path: Path) -> None:
    """A reboot that fires but doesn't fix connectivity must not re-fire or loop."""
    guard = RebootGuard(str(tmp_path / "s.json"), P)  # empty → first reboot allowed
    ladder, rec = Ladder(T), FakeRecovery()

    log: list[tuple[float, Action, bool]] = []
    t = 0.0
    while t <= 2000.0:  # past the reboot threshold, still inside the fresh cooldown
        action, eligible = _poll(ladder, guard, rec, gateway_up=False, t=t)
        log.append((t, action, eligible))
        t += 30.0

    reboots = [ts for ts, a, _ in log if a == Action.GPIO_RESET_REBOOT]
    escalations = [ts for ts, a, _ in log if a == Action.ESCALATE_NOTIFY]

    # It was eligible the moment it hit the threshold, so it fired without deferring.
    assert len(reboots) == 1
    assert escalations == []
    # The recorded attempt blocks every following poll, and the ladder neither
    # re-fires nor loop-notifies during the cooldown.
    after = [(a, e) for ts, a, e in log if ts > reboots[0]]
    assert all(e is False for _, e in after)  # fresh cooldown blocks the guard
    assert all(a == Action.NONE for a, _ in after)  # no re-fire, no notify loop
    assert _stamps(tmp_path / "s.json") == reboots


@pytest.mark.parametrize("reboot_rc", [bounded.TIMEOUT_RC, 0], ids=["failed", "unconfirmed"])
def test_failed_or_unconfirmed_reboot_request_retries_within_cap(
    tmp_path: Path, reboot_rc: int
) -> None:
    guard = RebootGuard(str(tmp_path / "guard"), P)
    ladder = Ladder(T)
    calls: list[tuple[float, list[str]]] = []
    writes: list[tuple[str, str]] = []
    outcomes: list[int | None] = []
    clock = [0.0]

    def command(argv: list[str]) -> int:
        calls.append((clock[0], argv))
        return reboot_rc

    class Recovery:
        @staticmethod
        def soft_reconnect() -> None:
            pass

        @staticmethod
        def restart_services() -> None:
            pass

        @staticmethod
        def gpio_reset_and_reboot() -> int:
            return recovery.gpio_reset_and_reboot(
                run=command,
                write=lambda path, value: writes.append((path, value)),
                read_alias=lambda: "fake.mmc",
                sleep=lambda _: None,
            )

    for now in range(0, 25001, 30):
        clock[0] = float(now)
        eligible = guard.can_request(now)
        action = ladder.observe(gateway_up=False, now=now, reboot_eligible=eligible)
        if action != Action.NONE:
            outcomes.append(
                run.handle(
                    action,
                    guard=guard,
                    now=now,
                    reboot_eligible=eligible,
                    recovery_mod=Recovery,
                )
            )

    request_times = [when for when, argv in calls if argv == ["systemctl", "reboot"]]
    assert writes, "GPIO reset path was exercised with all writes intercepted"
    assert len(request_times) > 1, f"reboot request was stranded: {calls}"
    assert all(
        later - earlier >= P.cooldown
        for earlier, later in zip(request_times, request_times[1:], strict=False)
    )
    assert all(
        sum(now - earlier <= P.window for earlier in request_times if earlier <= now) <= P.cap
        for now in request_times
    ), request_times
    assert [outcome for outcome in outcomes if outcome is not None] == [reboot_rc] * len(calls)


def test_request_attempt_is_counted_once_before_and_after_boot_change(tmp_path: Path) -> None:
    state = tmp_path / "guard"
    boot_id = ["boot-a"]
    guard = RebootGuard(str(state), P, read_boot_id=lambda: boot_id[0])

    guard.record_request(100.0)
    assert guard.can_reboot(101.0) is False
    assert guard.can_request(101.0) is False
    assert _stamps(state) == [100.0]

    boot_id[0] = "boot-b"
    restarted = RebootGuard(str(state), P, read_boot_id=lambda: boot_id[0])
    assert restarted.can_reboot(101.0) is False
    assert _stamps(state) == [100.0]
    assert not Path(f"{state}.request").exists()
