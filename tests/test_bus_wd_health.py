from __future__ import annotations

import os
from pathlib import Path

import pytest

from brilliant_bus_watchdog.health import bus_confirmed, heartbeat_age


def test_fresh(tmp_path: Path) -> None:
    p = tmp_path / "hb"
    p.write_text("100.0")
    assert heartbeat_age(str(p), now=130.0, started_at=0.0) == 30.0


def test_stale(tmp_path: Path) -> None:
    p = tmp_path / "hb"
    p.write_text("100.0")
    assert heartbeat_age(str(p), now=2000.0, started_at=0.0) == 1900.0


def test_missing_file_measures_from_start(tmp_path: Path) -> None:
    # no file: age is now - started_at, so a never-seen heartbeat only ages
    # relative to the watchdog's own start (not epoch 0)
    assert heartbeat_age(str(tmp_path / "nope"), now=500.0, started_at=200.0) == 300.0


def test_unparsable_measures_from_start(tmp_path: Path) -> None:
    p = tmp_path / "hb"
    p.write_text("garbage")
    assert heartbeat_age(str(p), now=500.0, started_at=200.0) == 300.0


@pytest.mark.parametrize(
    ("contents", "directory", "expected"),
    [
        (None, False, False),
        (None, True, False),
        ("pre_bus", False, False),
        (f"pre_bus {os.getpid()}", False, False),
        ("bus", False, False),  # old bare-string format: no pid -> fail closed
        ("bus unavailable", False, False),  # non-integer pid -> fail closed
        ("bus 0", False, False),  # non-positive pid -> fail closed
        (f" bus {os.getpid()}\n", False, True),  # live writer (whitespace-tolerant)
    ],
)
def test_bus_confirmed_fails_closed(
    tmp_path: Path,
    contents: str | None,
    directory: bool,
    expected: bool,
) -> None:
    phase = tmp_path / "bus-phase"
    if directory:
        phase.mkdir()
    elif contents is not None:
        phase.write_text(contents, encoding="utf-8")

    assert bus_confirmed(str(phase)) is expected


@pytest.mark.parametrize("pid", [1, 0, -1])
def test_bus_confirmed_fails_closed_on_pid_le_one(tmp_path: Path, pid: int) -> None:
    """A marker naming pid <= 1 must read as unconfirmed. pid 1 (init) always
    exists, so a torn/truncated ``bus 1`` marker would otherwise read as
    confirmed forever and suppress reboots indefinitely (fail-open). pid 0 and
    negative pids would target a process group rather than an individual
    writer, so they are rejected too."""
    phase = tmp_path / "bus-phase"
    phase.write_text(f"bus {pid}", encoding="utf-8")

    assert bus_confirmed(str(phase)) is False


def test_bus_confirmed_false_for_dead_writer_pid(tmp_path: Path) -> None:
    """A leftover ``bus <pid>`` whose pid is no live process (a dead or reverted
    writer, whose tmpfs marker survives until the next reboot) must read as
    unconfirmed — not as a live confirmed bus."""
    phase = tmp_path / "bus-phase"
    dead_pid = 2**31 - 1  # far beyond /proc/sys/kernel/pid_max: no such process
    phase.write_text(f"bus {dead_pid}", encoding="utf-8")

    assert bus_confirmed(str(phase)) is False


@pytest.mark.parametrize("pid", [2**31, 2**64])
def test_bus_confirmed_fails_closed_on_out_of_range_pid(tmp_path: Path, pid: int) -> None:
    """A pid beyond the C ``pid_t``/``long`` range makes ``os.kill`` raise
    ``OverflowError`` (an ``ArithmeticError``, not an ``OSError``). A torn/corrupt
    phase file like ``bus 2147483648`` must still fail closed rather than raise
    and crash the watchdog loop."""
    phase = tmp_path / "bus-phase"
    phase.write_text(f"bus {pid}", encoding="utf-8")

    assert bus_confirmed(str(phase)) is False


def test_bus_confirmed_fails_closed_on_invalid_utf8(tmp_path: Path) -> None:
    """Invalid UTF-8 bytes raise UnicodeDecodeError (a UnicodeError, NOT an
    OSError). bus_confirmed must fail closed rather than let that kill the
    watchdog run.py loop."""
    phase = tmp_path / "bus-phase"
    phase.write_bytes(b"\xff\xfe bus")  # not decodable as UTF-8

    assert bus_confirmed(str(phase)) is False
