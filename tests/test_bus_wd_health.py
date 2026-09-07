from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from brilliant_bus_watchdog.health import bus_confirmed, bus_failure_age, heartbeat_age
from brilliant_bus_watchdog.run import should_reboot
from brilliant_mqtt.heartbeat import DEAD_WRITER_RETENTION_S, write_phase

_CHILD_PHASE_WRITER = """
import sys
from brilliant_mqtt.heartbeat import write_phase

path, phase, raw_now = sys.argv[1:]
now = float(raw_now)
if phase == "bus":
    write_phase(path, "pre_bus", monotonic_clock=lambda: now)
write_phase(path, phase, monotonic_clock=lambda: now)
"""


def _write_phase_in_short_lived_process(path: Path, phase: str, now: float) -> None:
    subprocess.run(
        [sys.executable, "-c", _CHILD_PHASE_WRITER, str(path), phase, str(now)],
        check=True,
    )


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
        (f" bus {os.getpid()}\n", False, False),  # retired untimed format
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


def test_bus_confirmed_accepts_live_generation_with_active_lease(tmp_path: Path) -> None:
    phase = tmp_path / "bus-phase"
    write_phase(str(phase), "bus")

    assert bus_confirmed(str(phase)) is True


def test_repeated_dead_bus_writers_preserve_bounded_failure_history(tmp_path: Path) -> None:
    phase = tmp_path / "bus-phase"
    samples = [100.0, 400.0, 700.0, 1000.0, 1300.0, 1600.0, 1900.0]

    decisions: list[bool] = []
    for now in samples:
        _write_phase_in_short_lived_process(phase, "bus", now)
        failure_age = bus_failure_age(str(phase), now=now)
        decisions.append(
            should_reboot(
                age=1900.0,
                stale_after=1800.0,
                bridge_active=True,
                gateway_up=True,
                bus_failure_age=failure_age,
            )
        )

    assert decisions[:-1] == [False] * 6
    assert bus_confirmed(str(phase)) is False
    assert bus_failure_age(str(phase), now=samples[-1]) == 1800.0
    assert decisions[-1] is True


def test_dead_pre_bus_writers_never_attribute_broker_only_startup(tmp_path: Path) -> None:
    phase = tmp_path / "bus-phase"
    _write_phase_in_short_lived_process(phase, "bus", 100.0)
    for now in (200.0, 300.0, 400.0):
        _write_phase_in_short_lived_process(phase, "pre_bus", now)

    assert bus_failure_age(str(phase), now=400.0) is None


def test_dead_bus_record_expires_after_its_service_generation_window(tmp_path: Path) -> None:
    phase = tmp_path / "bus-phase"
    _write_phase_in_short_lived_process(phase, "bus", 100.0)

    assert (
        bus_failure_age(str(phase), now=100.0 + DEAD_WRITER_RETENTION_S) == DEAD_WRITER_RETENTION_S
    )
    assert bus_failure_age(str(phase), now=100.0 + DEAD_WRITER_RETENTION_S + 0.001) is None


def test_bus_record_from_an_earlier_boot_is_rejected(tmp_path: Path) -> None:
    phase = tmp_path / "bus-phase"
    write_phase(str(phase), "bus", monotonic_clock=lambda: 100.0)
    parts = phase.read_text(encoding="utf-8").split()
    parts[4] = "earlier-boot"
    phase.write_text(" ".join(parts), encoding="utf-8")

    assert bus_failure_age(str(phase), now=1900.0) is None


def test_bus_record_from_an_earlier_process_generation_is_rejected(tmp_path: Path) -> None:
    phase = tmp_path / "bus-phase"
    write_phase(str(phase), "bus", monotonic_clock=lambda: 100.0)
    parts = phase.read_text(encoding="utf-8").split()
    parts[3] = str(int(parts[3]) + 1)
    phase.write_text(" ".join(parts), encoding="utf-8")

    assert bus_failure_age(str(phase), now=1900.0) is None


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
