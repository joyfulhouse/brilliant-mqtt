import json
from pathlib import Path

import pytest

from brilliant_wifi_watchdog.reboot_guard import GuardPolicy, RebootGuard

P = GuardPolicy(cooldown=3600.0, cap=3, window=21600.0)


def _stamps(path: Path) -> list[float]:
    return [float(value) for value in json.loads(path.read_text(encoding="utf-8"))]


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


def test_boot_change_persistence_failure_keeps_decisions_conservative(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    boot_id = ["boot-a"]
    guard = RebootGuard(str(tmp_path / "state"), P, read_boot_id=lambda: boot_id[0])
    guard.record_request(100.0)
    boot_id[0] = "boot-b"

    def read_only(*args: object) -> None:
        raise OSError("read-only state directory")

    monkeypatch.setattr("brilliant_wifi_watchdog.reboot_guard.os.replace", read_only)
    monkeypatch.setattr("brilliant_wifi_watchdog.reboot_guard.os.unlink", read_only)

    assert guard.can_reboot(101.0) is False
    assert guard.can_request(101.0) is False


def test_boot_id_fallback_clears_the_previous_request(tmp_path: Path) -> None:
    state = tmp_path / "state"
    boot_id: list[str | None] = ["boot-a"]
    guard = RebootGuard(str(state), P, read_boot_id=lambda: boot_id[0])
    guard.record_request(100.0)

    boot_id[0] = None
    guard.record_request(3700.0)

    assert _stamps(state) == [100.0, 3700.0]
    assert not Path(f"{state}.request").exists()


def test_unconfirmed_request_sidecar_expires_after_cooldown(tmp_path: Path) -> None:
    state = tmp_path / "state"
    guard = RebootGuard(str(state), P, read_boot_id=lambda: "boot-a")
    guard.record_request(100.0)
    request = Path(f"{state}.request")
    assert request.exists()

    assert guard.can_request(3700.0) is True
    assert not request.exists()
