from brilliant_wifi_watchdog import __version__
from brilliant_wifi_watchdog.ladder import Action, Ladder, Thresholds


def test_version() -> None:
    assert __version__ == "0.1.0"


T = Thresholds()  # defaults


def _down(ladder: Ladder, start: float, secs: float, step: float = 30.0) -> list[Action]:
    """Feed gateway-down observations from start..start+secs; return actions."""
    out, t = [], start
    while t <= start + secs:
        out.append(ladder.observe(gateway_up=False, now=t))
        t += step
    return out


def test_debounce_no_action_before_three_fails() -> None:
    lad = Ladder(T)
    assert lad.observe(gateway_up=False, now=0) == Action.NONE
    assert lad.observe(gateway_up=False, now=30) == Action.NONE
    # 3rd consecutive fail crosses ~soft_after only after enough elapsed; still NONE here
    assert lad.observe(gateway_up=False, now=60) == Action.NONE


def test_soft_and_restart_fire_once_while_eligible_reboot_rearms() -> None:
    lad = Ladder(T)
    actions = _down(lad, 0.0, 400.0)
    assert actions.count(Action.SOFT_RECONNECT) == 1
    assert actions.count(Action.RESTART_SERVICES) == 1
    assert actions[-2:] == [Action.GPIO_RESET_REBOOT, Action.GPIO_RESET_REBOOT]


def test_recovery_resets_ladder() -> None:
    lad = Ladder(T)
    _down(lad, 0.0, 200.0)
    assert lad.observe(gateway_up=True, now=210.0) == Action.NONE
    # fresh outage starts the ladder over (soft fires again later)
    assert _down(lad, 240.0, 120.0).count(Action.SOFT_RECONNECT) == 1


def test_reboot_deferred_once_then_rearms_while_eligible() -> None:
    """When the guard blocks the reboot, the rung stays pending: it defers once
    (ESCALATE_NOTIFY), stays silent while still blocked, and re-arms the instant
    the guard clears — no connectivity recovery required (issue #91)."""
    lad = Ladder(T)
    out, t = [], 0.0
    while t <= 400.0:  # climb to the reboot threshold with the guard ineligible
        out.append(lad.observe(gateway_up=False, now=t, reboot_eligible=False))
        t += 30.0
    assert out.count(Action.ESCALATE_NOTIFY) == 1  # deferred once, not per-poll
    assert Action.GPIO_RESET_REBOOT not in out  # never rebooted while blocked
    # Still blocked → keeps deferring silently (no repeat notify).
    assert lad.observe(gateway_up=False, now=430.0, reboot_eligible=False) == Action.NONE
    # Guard clears → the pending reboot re-arms immediately and remains pending.
    assert (
        lad.observe(gateway_up=False, now=460.0, reboot_eligible=True) == Action.GPIO_RESET_REBOOT
    )
    assert (
        lad.observe(gateway_up=False, now=490.0, reboot_eligible=True) == Action.GPIO_RESET_REBOOT
    )


def test_blocked_reboot_does_not_starve_cheaper_rungs() -> None:
    """A blocked reboot must not short-circuit the cheaper rungs.  If a poll gap
    (e.g. a starved daemon) crosses the reboot threshold before soft/restart have
    fired, the ladder still climbs down to them on later polls instead of only ever
    doing the most destructive action once the guard clears (issue #91)."""
    lad = Ladder(Thresholds())
    for t in (0.0, 30.0, 60.0):
        assert lad.observe(gateway_up=False, now=t, reboot_eligible=False) == Action.NONE
    # First poll after a long gap: reboot is due but blocked → notify once.
    assert lad.observe(gateway_up=False, now=400.0, reboot_eligible=False) == Action.ESCALATE_NOTIFY
    # Subsequent blocked polls fire the still-unfired restart/soft rungs, not silence.
    got = [
        lad.observe(gateway_up=False, now=t, reboot_eligible=False) for t in (430.0, 460.0, 490.0)
    ]
    assert Action.RESTART_SERVICES in got and Action.SOFT_RECONNECT in got


def test_inconclusive_samples_pause_the_outage_clock() -> None:
    ladder = Ladder(T)
    for now in (0.0, 30.0, 60.0):
        assert ladder.observe(gateway_up=False, now=now) == Action.NONE

    for now in range(90, 391, 30):
        assert ladder.observe(gateway_up=None, now=float(now)) == Action.NONE

    # Sixty seconds of confirmed failure before the pause plus thirty after it
    # reaches the 90-second soft rung; the inconclusive interval contributes zero.
    assert ladder.observe(gateway_up=False, now=420.0) == Action.SOFT_RECONNECT
