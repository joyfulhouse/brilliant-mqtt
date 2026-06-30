from pathlib import Path

from brilliant_wifi_watchdog.reboot_guard import GuardPolicy, RebootGuard

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


def test_persists_across_instances(tmp_path: Path) -> None:
    path = str(tmp_path / "s.json")
    RebootGuard(path, P).record(0.0)
    assert RebootGuard(path, P).can_reboot(1800.0) is False
