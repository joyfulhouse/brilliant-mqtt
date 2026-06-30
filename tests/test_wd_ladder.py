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
