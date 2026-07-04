from pathlib import Path

from brilliant_bus_watchdog.reboot_guard import GuardPolicy, RebootGuard

P = GuardPolicy(cooldown=3600.0, cap=3, window=21600.0)


def test_cooldown_blocks(tmp_path: Path) -> None:
    g = RebootGuard(str(tmp_path / "s.json"), P)
    assert g.can_reboot(0.0) is True
    g.record(0.0)
    assert g.can_reboot(1800.0) is False  # within 1h cooldown
    assert g.can_reboot(3601.0) is True  # past cooldown


def test_cap_blocks_within_window(tmp_path: Path) -> None:
    g = RebootGuard(str(tmp_path / "s.json"), P)
    for t in (0.0, 3601.0, 7202.0):  # 3 reboots, cooldown-spaced
        assert g.can_reboot(t) is True
        g.record(t)
    assert g.can_reboot(10803.0) is False  # 4th within 6h window -> capped


def test_cap_resets_after_window_expires(tmp_path: Path) -> None:
    """Safety property: once all stamps age past the window, the cap resets.

    Without this the guard would permanently block reboots after cap exhaustion,
    making the watchdog useless for long-running panels.  A fresh 6-hour window
    must be able to accumulate cap-many reboots again.
    """
    g = RebootGuard(str(tmp_path / "s.json"), P)
    for t in (0.0, 3601.0, 7202.0):  # fill the cap (3 reboots, cooldown-spaced)
        g.record(t)
    assert g.can_reboot(10803.0) is False  # 4th within window → capped
    # Advance past the window (21600 s from first stamp at 0.0)
    past_window = 0.0 + P.window + 1.0  # = 21601.0
    # All 3 stamps are now older than the window → cap resets → reboot allowed
    assert g.can_reboot(past_window) is True


def test_persists_across_instances(tmp_path: Path) -> None:
    path = str(tmp_path / "s.json")
    RebootGuard(path, P).record(0.0)
    assert RebootGuard(path, P).can_reboot(1800.0) is False
