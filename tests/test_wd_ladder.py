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


def test_soft_then_restart_then_reboot_each_once() -> None:
    lad = Ladder(T)
    actions = _down(lad, 0.0, 400.0)
    assert Action.SOFT_RECONNECT in actions
    assert Action.RESTART_SERVICES in actions
    assert Action.GPIO_RESET_REBOOT in actions
    assert actions.count(Action.SOFT_RECONNECT) == 1
    assert actions.count(Action.RESTART_SERVICES) == 1
    assert actions.count(Action.GPIO_RESET_REBOOT) == 1


def test_recovery_resets_ladder() -> None:
    lad = Ladder(T)
    _down(lad, 0.0, 200.0)
    assert lad.observe(gateway_up=True, now=210.0) == Action.NONE
    # fresh outage starts the ladder over (soft fires again later)
    assert _down(lad, 240.0, 120.0).count(Action.SOFT_RECONNECT) == 1


def test_reboot_deferred_when_ineligible_then_rearms_when_eligible() -> None:
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
    # Guard clears → the pending reboot re-arms immediately...
    assert (
        lad.observe(gateway_up=False, now=460.0, reboot_eligible=True) == Action.GPIO_RESET_REBOOT
    )
    # ...and only once (now marked fired for this outage).
    assert lad.observe(gateway_up=False, now=490.0, reboot_eligible=True) == Action.NONE
