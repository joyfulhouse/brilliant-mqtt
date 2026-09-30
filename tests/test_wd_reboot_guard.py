import json
import logging
from pathlib import Path

import pytest

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


def test_missing_state_allows_reboot(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.ERROR, logger="brilliant_wifi_watchdog.reboot_guard")
    guard = RebootGuard(str(tmp_path / "absent.json"), P)
    assert guard.can_request(100.0) is True
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


# Valid JSON that is not a list; {} and "" must be rejected by type, not by items.
WRONG_TYPES = {
    "wrong_type": json.dumps({"stamps": [1.0]}),
    "wrong_type_empty_dict": "{}",
    "wrong_type_empty_str": '""',
    "wrong_type_number": "123",
}


def _bad_state(tmp_path: Path, state_kind: str) -> Path:
    state = tmp_path / "state"
    if state_kind == "unreadable":
        state.mkdir()  # present, but open() raises IsADirectoryError
    elif state_kind == "corrupt":
        state.write_text("not JSON", encoding="utf-8")
    elif state_kind in WRONG_TYPES:
        state.write_text(WRONG_TYPES[state_kind], encoding="utf-8")
    else:  # wrong_shape: a list whose items are not timestamps
        state.write_text(json.dumps([["x"]]), encoding="utf-8")
    return state


BAD_KINDS = ["unreadable", "corrupt", *WRONG_TYPES, "wrong_shape"]


@pytest.mark.parametrize("state_kind", BAD_KINDS)
def test_bad_state_fails_closed_for_one_cooldown(
    tmp_path: Path, state_kind: str, caplog: pytest.LogCaptureFixture
) -> None:
    guard = RebootGuard(str(_bad_state(tmp_path, state_kind)), P)
    caplog.set_level(logging.ERROR, logger="brilliant_wifi_watchdog.reboot_guard")

    assert guard.can_request(100.0) is False
    assert guard.can_reboot(100.0 + P.cooldown - 1.0) is False
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1  # logged once, on first observation


@pytest.mark.parametrize("state_kind", BAD_KINDS)
def test_bad_state_recovers_after_fail_closed_period(tmp_path: Path, state_kind: str) -> None:
    state = _bad_state(tmp_path, state_kind)
    guard = RebootGuard(str(state), P)
    assert guard.can_request(100.0) is False

    after = 100.0 + P.cooldown
    assert guard.can_request(after) is True  # bounded: never wedged
    if state_kind != "unreadable":
        guard.record_request(after)  # a reboot rewrites valid state
        assert guard.can_request(after + 1.0) is False  # normal cooldown
        assert guard.can_request(after + P.cooldown) is True


@pytest.mark.parametrize(
    "raw",
    [
        "[NaN]",
        "[true]",
        "[Infinity]",
        '["1e309"]',
        '["100"]',
        "[1" + "0" * 400 + "]",
        pytest.param("[" * 100000, id="deep-nesting"),
    ],
)
def test_malformed_stamps_fail_closed_then_recover(tmp_path: Path, raw: str) -> None:
    state = tmp_path / "state"
    state.write_text(raw, encoding="utf-8")
    guard = RebootGuard(str(state), P)
    now = 1_790_000_000.0  # realistic wall clock: small coerced stamps age out
    assert guard.can_request(now) is False  # not trusted as history
    assert guard.can_request(now + P.cooldown) is True  # never wedged


NOW = 1_790_000_000.0


@pytest.mark.parametrize("stamp", [1e18, NOW + 10 * P.window], ids=["1e18", "now+10w"])
def test_future_stamp_fails_closed_then_recovers(tmp_path: Path, stamp: float) -> None:
    state = tmp_path / "state"
    state.write_text(json.dumps([stamp]), encoding="utf-8")
    guard = RebootGuard(str(state), P)
    assert guard.can_request(NOW) is False
    after = NOW + P.cooldown
    assert guard.can_request(after) is True  # never wedged by a future stamp
    guard.record_request(after)  # prunes the future stamp
    assert guard.can_request(after + 1.0) is False  # normal cooldown
    assert guard.can_request(after + P.cooldown) is True


def test_fail_closed_period_restarts_if_clock_steps_back(tmp_path: Path) -> None:
    guard = RebootGuard(str(_bad_state(tmp_path, "corrupt")), P)
    assert guard.can_request(1_000_000.0) is False
    assert guard.can_request(100.0) is False
    assert guard.can_request(100.0 + P.cooldown) is True


def test_second_bad_episode_fails_closed_and_logs_again(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.ERROR, logger="brilliant_wifi_watchdog.reboot_guard")
    state = _bad_state(tmp_path, "corrupt")
    guard = RebootGuard(str(state), P)
    t0 = 100.0
    assert guard.can_request(t0) is False
    assert len(caplog.records) == 1

    state.write_text("[]", encoding="utf-8")  # valid state restored
    healthy = t0 + P.cooldown
    assert guard.can_request(healthy) is True

    state.write_text("not JSON", encoding="utf-8")  # a second bad episode
    assert guard.can_request(healthy + 1.0) is False
    assert len(caplog.records) == 2
