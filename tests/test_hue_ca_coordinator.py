from pathlib import Path

from brilliant_hue_ca.coordinator import RealCoordinator


def test_coordinator_running_reflects_ini_presence(tmp_path: Path) -> None:
    ini = tmp_path / "hue_bridge_peripherals.ini"
    coord = RealCoordinator(str(ini))
    assert coord.is_running() is False
    ini.write_text("")
    assert coord.is_running() is True


def test_coordinator_restart_touches_ini(tmp_path: Path) -> None:
    ini = tmp_path / "hue_bridge_peripherals.ini"
    ini.write_text("")
    import os

    os.utime(str(ini), (1000, 1000))
    before = os.stat(str(ini)).st_mtime
    coord = RealCoordinator(str(ini))
    coord.restart()
    after = os.stat(str(ini)).st_mtime
    assert after > before
