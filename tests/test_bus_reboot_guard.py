import json
import logging
import os
from pathlib import Path

import pytest

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


def test_missing_state_allows_reboot(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.ERROR, logger="brilliant_bus_watchdog.reboot_guard")
    guard = RebootGuard(str(tmp_path / "absent.json"), P)
    assert guard.can_reboot(100.0) is True
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
    caplog.set_level(logging.ERROR, logger="brilliant_bus_watchdog.reboot_guard")

    assert guard.can_reboot(100.0) is False
    assert guard.can_reboot(100.0 + P.cooldown - 1.0) is False
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1  # logged once, on first observation


@pytest.mark.parametrize("state_kind", BAD_KINDS)
def test_bad_state_recovers_after_fail_closed_period(tmp_path: Path, state_kind: str) -> None:
    state = _bad_state(tmp_path, state_kind)
    guard = RebootGuard(str(state), P)
    assert guard.can_reboot(100.0) is False

    after = 100.0 + P.cooldown
    assert guard.can_reboot(after) is True  # bounded: never wedged
    if state_kind != "unreadable":
        guard.record(after)  # a reboot rewrites valid state
        assert guard.can_reboot(after + 1.0) is False  # normal cooldown
        assert guard.can_reboot(after + P.cooldown) is True


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
    assert guard.can_reboot(now) is False  # not trusted as history
    assert guard.can_reboot(now + P.cooldown) is True  # never wedged


NOW = 1_790_000_000.0


@pytest.mark.parametrize("stamp", [1e18, NOW + 10 * P.window], ids=["1e18", "now+10w"])
def test_future_stamp_fails_closed_then_recovers(tmp_path: Path, stamp: float) -> None:
    state = tmp_path / "state"
    state.write_text(json.dumps([stamp]), encoding="utf-8")
    guard = RebootGuard(str(state), P)
    assert guard.can_reboot(NOW) is False
    after = NOW + P.cooldown
    assert guard.can_reboot(after) is False  # retained history starts a normal cooldown
    after += P.cooldown
    assert guard.can_reboot(after) is True
    guard.record(after)  # persists the clamped future stamp
    assert guard.can_reboot(after + 1.0) is False  # normal cooldown
    assert guard.can_reboot(after + P.cooldown) is True


def test_second_bad_episode_fails_closed_and_logs_again(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.ERROR, logger="brilliant_bus_watchdog.reboot_guard")
    state = _bad_state(tmp_path, "corrupt")
    guard = RebootGuard(str(state), P)
    t0 = 100.0
    assert guard.can_reboot(t0) is False
    assert len(caplog.records) == 1

    state.write_text("[]", encoding="utf-8")  # valid state restored
    healthy = t0 + P.cooldown
    assert guard.can_reboot(healthy) is True

    state.write_text("not JSON", encoding="utf-8")  # a second bad episode
    assert guard.can_reboot(healthy + 1.0) is False
    assert len(caplog.records) == 2


def _write_history(state: Path, stamps: list[float]) -> None:
    state.write_text(json.dumps(stamps), encoding="utf-8")


@pytest.mark.parametrize("repair", ["empty", "valid_history", "deleted"])
def test_repair_inside_fail_closed_window_stays_blocked(
    tmp_path: Path, repair: str, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.ERROR, logger="brilliant_bus_watchdog.reboot_guard")
    state = _bad_state(tmp_path, "corrupt")
    guard = RebootGuard(str(state), P)
    assert guard.can_reboot(NOW) is False

    if repair == "empty":
        _write_history(state, [])
    elif repair == "valid_history":
        _write_history(state, [NOW - 2 * P.cooldown])  # alone, would allow a reboot
    else:
        state.unlink()
    assert guard.can_reboot(NOW + 1.0) is False  # window holds despite the repair
    assert guard.can_reboot(NOW + P.cooldown - 1.0) is False
    assert guard.can_reboot(NOW + P.cooldown) is True  # then normal history applies
    assert len(caplog.records) == 1


def test_stamp_just_after_now_is_bad_state(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.ERROR, logger="brilliant_bus_watchdog.reboot_guard")
    state = tmp_path / "state"
    _write_history(state, [NOW + 0.5])
    assert RebootGuard(str(state), P).can_reboot(NOW) is False
    assert len(caplog.records) == 1


def test_stamp_exactly_now_is_valid_history(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.ERROR, logger="brilliant_bus_watchdog.reboot_guard")
    state = tmp_path / "state"
    _write_history(state, [NOW])
    guard = RebootGuard(str(state), P)
    assert guard.can_reboot(NOW) is False  # normal cooldown, not lost history
    assert guard.can_reboot(NOW + P.cooldown) is True
    assert caplog.records == []


def test_record_persists_only_the_rolling_window(tmp_path: Path) -> None:
    state = tmp_path / "state"
    recent = NOW - P.cooldown
    _write_history(state, [NOW - P.window - 1.0, recent, NOW + 5.0])
    RebootGuard(str(state), P).record(NOW)
    assert json.loads(state.read_text(encoding="utf-8")) == [recent, NOW, NOW]


def test_persistently_bad_state_authorizes_at_most_once_per_cooldown(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.ERROR, logger="brilliant_bus_watchdog.reboot_guard")
    guard = RebootGuard(str(_bad_state(tmp_path, "corrupt")), P)  # bad throughout

    assert guard.can_reboot(NOW) is False
    assert guard.can_reboot(NOW + P.cooldown - 1.0) is False
    assert guard.can_reboot(NOW + P.cooldown) is True  # once, at expiry
    assert len(caplog.records) == 1

    rearmed = NOW + P.cooldown + 1.0
    assert guard.can_reboot(rearmed) is False  # still bad: a fresh window
    assert len(caplog.records) == 2
    assert guard.can_reboot(rearmed + P.cooldown - 1.0) is False
    assert guard.can_reboot(rearmed + P.cooldown) is True


@pytest.mark.parametrize(
    "history",
    [
        [NOW + P.cooldown - 1.0],  # still inside the cooldown at expiry
        [NOW - 2 * P.cooldown, NOW - P.cooldown - 1.0, NOW - 1.0],  # full cap in window
    ],
    ids=["cooldown", "cap"],
)
def test_repaired_history_still_governs_at_window_expiry(
    tmp_path: Path, history: list[float]
) -> None:
    state = _bad_state(tmp_path, "corrupt")
    guard = RebootGuard(str(state), P)
    assert guard.can_reboot(NOW) is False

    _write_history(state, history)  # repaired inside the window
    assert guard.can_reboot(NOW + P.cooldown - 1.0) is False
    assert guard.can_reboot(NOW + P.cooldown) is False  # normal guard resumes


def test_last_read_bad_tracks_the_latest_read_not_the_window(tmp_path: Path) -> None:
    """last_read_bad reports what the most recent read found, even while the
    fail-closed window armed by an earlier bad read is still running."""
    path = tmp_path / "s.json"
    path.write_text("not json", encoding="utf-8")
    g = RebootGuard(str(path), P)
    assert g.can_reboot(100.0) is False
    assert g.last_read_bad is True
    path.write_text("[]", encoding="utf-8")
    assert g.can_reboot(200.0) is False  # still inside the window
    assert g.last_read_bad is False


@pytest.mark.parametrize("kind", ["fifo", "oversized"])
def test_unsafe_state_is_rejected_without_opening(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kind: str
) -> None:
    state = tmp_path / "state"
    if kind == "fifo":
        os.mkfifo(state)
    else:
        state.write_bytes(b" " * (65536 + 1))

    def forbidden_open(*args: object, **kwargs: object) -> None:
        raise AssertionError("unsafe state must not be opened")

    monkeypatch.setattr("brilliant_bus_watchdog.reboot_guard.os.open", forbidden_open)
    assert RebootGuard(str(state), P).can_reboot(NOW) is False


def test_permission_error_is_bad_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    state = tmp_path / "state"
    state.write_text("[]", encoding="utf-8")

    def denied(*args: object, **kwargs: object) -> None:
        raise PermissionError("denied")

    monkeypatch.setattr("brilliant_bus_watchdog.reboot_guard.os.open", denied)
    assert RebootGuard(str(state), P).can_reboot(NOW) is False


def test_exact_window_boundary_keeps_cap(tmp_path: Path) -> None:
    state = tmp_path / "state"
    _write_history(state, [NOW - P.window, NOW - 2 * P.cooldown, NOW - P.cooldown])
    assert RebootGuard(str(state), P).can_reboot(NOW) is False


def test_record_keeps_window_boundary_and_equal_now(tmp_path: Path) -> None:
    state = tmp_path / "state"
    _write_history(state, [NOW - P.window, NOW])
    RebootGuard(str(state), P).record(NOW)
    assert json.loads(state.read_text()) == [NOW - P.window, NOW, NOW]


def test_wall_jump_does_not_expire_bad_state_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("brilliant_bus_watchdog.reboot_guard.time.monotonic", lambda: 10.0)
    guard = RebootGuard(str(_bad_state(tmp_path, "corrupt")), P)
    assert guard.can_reboot(0.0) is False
    assert guard.can_reboot(NOW) is False


def test_backward_step_preserves_full_cap(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    tick = [0.0]
    monkeypatch.setattr("brilliant_bus_watchdog.reboot_guard.time.monotonic", lambda: tick[0])
    state = tmp_path / "state"
    _write_history(state, [NOW, NOW + P.cooldown, NOW + 2 * P.cooldown])
    guard = RebootGuard(str(state), P)
    assert guard.can_reboot(0.0) is False
    tick[0] = P.cooldown
    assert guard.can_reboot(P.cooldown) is False
    tick[0] = 2 * P.cooldown
    assert guard.can_reboot(2 * P.cooldown) is False
    guard.record(2 * P.cooldown)
    assert len(json.loads(state.read_text())) == 4


@pytest.fixture(autouse=True)
def simulated_monotonic(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    """Legacy histories use one simulated timeline for both clocks."""
    if request.node.name.startswith(("test_wall_jump", "test_backward_step")):
        return
    original = RebootGuard.can_reboot

    def at(self: RebootGuard, now: float) -> bool:
        monkeypatch.setattr("brilliant_bus_watchdog.reboot_guard.time.monotonic", lambda: now)
        return original(self, now)

    monkeypatch.setattr(RebootGuard, "can_reboot", at)


def test_backward_step_new_history_rearms_and_preserves_cap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tick = [0.0]
    monkeypatch.setattr("brilliant_bus_watchdog.reboot_guard.time.monotonic", lambda: tick[0])
    policy = GuardPolicy(cooldown=10, cap=3, window=60)
    state = tmp_path / "state"
    _write_history(state, [1000])
    guard = RebootGuard(str(state), policy)
    assert guard.can_reboot(0) is False
    tick[0] = 10
    assert guard.can_reboot(10) is False
    _write_history(state, [100, 110, 120])
    tick[0] = 100
    assert guard.can_reboot(90) is False
    tick[0] = 110
    assert guard.can_reboot(90) is False
    guard.record(90)
    assert json.loads(state.read_text()) == [90, 90, 90, 90]


def test_backward_step_second_correction_recovers_on_new_timeline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tick = [0.0]
    monkeypatch.setattr("brilliant_bus_watchdog.reboot_guard.time.monotonic", lambda: tick[0])
    policy = GuardPolicy(cooldown=10, cap=3, window=60)
    state = tmp_path / "state"
    _write_history(state, [1000])
    guard = RebootGuard(str(state), policy)
    assert guard.can_reboot(100) is False
    tick[0] = 10
    assert guard.can_reboot(110) is False
    tick[0] = 20
    assert guard.can_reboot(0) is False
    tick[0] = 30
    assert guard.can_reboot(10) is False
    tick[0] = 40
    assert guard.can_reboot(20) is True


def test_fifo_swap_between_stat_and_open_is_nonblocking(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = tmp_path / "state"
    state.write_text("[]", encoding="utf-8")
    original_stat, original_open = os.stat, os.open
    opened: list[int] = []

    def swapped_stat(path: str) -> os.stat_result:
        result = original_stat(path)
        state.unlink()
        os.mkfifo(state)
        return result

    def nonblocking_open(path: str, flags: int) -> int:
        assert flags & os.O_NONBLOCK
        fd = original_open(path, flags)
        opened.append(fd)
        return fd

    def blocking_open(*args: object, **kwargs: object) -> None:
        raise AssertionError("must use a nonblocking descriptor")

    monkeypatch.setattr("builtins.open", blocking_open)
    monkeypatch.setattr("brilliant_bus_watchdog.reboot_guard.os.stat", swapped_stat)
    monkeypatch.setattr("brilliant_bus_watchdog.reboot_guard.os.open", nonblocking_open)
    assert RebootGuard(str(state), P).can_reboot(NOW) is False

    assert len(opened) == 1
    with pytest.raises(OSError):
        os.fstat(opened[0])


@pytest.mark.parametrize("grow_after", ["stat", "fstat"])
@pytest.mark.parametrize(
    "content",
    [b" " * 65537, ('"' + "\u00e9" * 40000 + '"').encode()],
    ids=["ascii", "multibyte"],
)
def test_replaced_or_growing_state_has_byte_bound(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, grow_after: str, content: bytes
) -> None:
    state = tmp_path / "state"
    state.write_bytes(b"[]")
    original_stat, original_fstat = os.stat, os.fstat

    def growing_stat(path: str) -> os.stat_result:
        result = original_stat(path)
        if grow_after == "stat":
            state.write_bytes(content)
        return result

    def growing_fstat(fd: int) -> os.stat_result:
        result = original_fstat(fd)
        if grow_after == "fstat":
            state.write_bytes(content)
        return result

    def forbidden_parse(raw: bytes) -> None:
        raise AssertionError("oversized bytes must not be parsed")

    monkeypatch.setattr("brilliant_bus_watchdog.reboot_guard.json.loads", forbidden_parse)
    monkeypatch.setattr("brilliant_bus_watchdog.reboot_guard.os.stat", growing_stat)
    monkeypatch.setattr("brilliant_bus_watchdog.reboot_guard.os.fstat", growing_fstat)
    assert RebootGuard(str(state), P).can_reboot(NOW) is False
