"""Tests: bus-liveness heartbeat writer."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from brilliant_bus_watchdog.health import bus_confirmed
from brilliant_mqtt.heartbeat import write_heartbeat, write_phase
from tests.fakes import FakeClock


def test_writes_epoch_and_creates_parent(tmp_path: Path) -> None:
    p = tmp_path / "sub" / "bus-heartbeat"
    write_heartbeat(str(p), lambda: 1751630400.5)
    assert p.read_text().strip() == "1751630400.5"


def test_overwrites(tmp_path: Path) -> None:
    p = tmp_path / "hb"
    monotonic = FakeClock()
    write_heartbeat(str(p), lambda: 1.0, monotonic)
    monotonic.advance(10.0)
    write_heartbeat(str(p), lambda: 2.0, monotonic)
    assert p.read_text().strip() == "2.0"


def test_rate_limits_writes_to_once_per_ten_seconds(tmp_path: Path) -> None:
    p = tmp_path / "hb"
    monotonic = FakeClock()

    write_heartbeat(str(p), lambda: 1.0, monotonic)
    write_heartbeat(str(p), lambda: 2.0, monotonic)
    monotonic.advance(9.999)
    write_heartbeat(str(p), lambda: 3.0, monotonic)
    assert p.read_text().strip() == "1.0"

    monotonic.advance(0.001)
    write_heartbeat(str(p), lambda: 4.0, monotonic)
    assert p.read_text().strip() == "4.0"


def test_empty_path_is_noop(tmp_path: Path) -> None:
    write_heartbeat("", lambda: 1.0)  # must not raise, must not create anything


def test_never_raises_on_unwritable(tmp_path: Path) -> None:
    # parent is a file, so mkdir/replace will fail — must be swallowed
    blocker = tmp_path / "blocker"
    blocker.write_text("x")
    write_heartbeat(str(blocker / "hb"), lambda: 1.0)  # no exception


def test_swallows_permission_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """A genuine PermissionError (e.g. a locked-down /run) must be swallowed —
    it is an OSError subclass, but assert it explicitly rather than relying on
    a real unwritable path to happen to raise the right subclass."""

    def _raise(*args: object, **kwargs: object) -> None:
        raise PermissionError("denied")

    monkeypatch.setattr("brilliant_mqtt.heartbeat.os.makedirs", _raise)
    p = tmp_path / "sub" / "bus-heartbeat"
    write_heartbeat(str(p), lambda: 1.0)  # no exception
    assert not p.exists()


def test_write_phase_atomically_replaces_the_current_phase(tmp_path: Path) -> None:
    phase = tmp_path / "runtime" / "bus-phase"

    write_phase(str(phase), "pre_bus")
    write_phase(str(phase), "bus")

    # write_phase appends the writer's pid so the reader can check liveness.
    assert phase.read_text(encoding="utf-8") == f"bus {os.getpid()}"


def test_write_phase_is_best_effort(tmp_path: Path) -> None:
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory", encoding="utf-8")

    write_phase("", "pre_bus")
    write_phase(str(blocker / "bus-phase"), "bus")


def test_write_phase_pre_bus_is_best_effort_when_parent_is_file(tmp_path: Path) -> None:
    """A pre_bus stamp whose parent path is a regular file must be swallowed,
    never re-raised. The stamp happens at ``__main__`` *before* the session
    ``try``: a re-raise would make the supervisor back off and retry forever,
    so the bridge would never connect to anything. A missing/failed stamp
    instead reads as unconfirmed (reboot guard disabled), which is fail-safe."""
    blocker = tmp_path / "blocker"
    blocker.write_text("x", encoding="utf-8")

    write_phase(str(blocker / "bus-phase"), "pre_bus")  # must not raise


def test_write_phase_pre_bus_is_best_effort_when_path_is_dir(tmp_path: Path) -> None:
    """A pre_bus stamp whose target path is itself a directory must be
    swallowed (``os.replace`` raises ``IsADirectoryError``) and must not leak
    the ``bus-phase.tmp`` scratch file left behind by the failed replace."""
    phase = tmp_path / "bus-phase"
    phase.mkdir()

    write_phase(str(phase), "pre_bus")  # must not raise

    assert not (tmp_path / "bus-phase.tmp").exists()


def test_failed_pre_bus_restamp_must_not_leave_live_pid_bus_marker_confirmed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A failed pre_bus re-stamp must ACTIVELY clear any leftover ``bus``
    marker, not merely swallow the error. ``run()`` retries ``_run_session`` in
    the SAME process while teardown keeps the ``bus`` marker, so a leftover
    ``bus <pid>`` names THIS still-live process — the reader's pid-liveness
    check would read it as confirmed. If the re-stamp fails and the marker is
    left in place, a stale heartbeat during a broker-only outage would reboot a
    healthy panel in a loop (issue #87). So after a failed pre_bus write over a
    live-pid marker, the file must be gone and bus_confirmed must be False, with
    no exception raised."""
    phase = tmp_path / "bus-phase"
    phase.write_text(f"bus {os.getpid()}", encoding="utf-8")  # leftover, live pid
    assert bus_confirmed(str(phase)) is True  # would fire a reboot if left

    def _raise(*args: object, **kwargs: object) -> None:
        raise OSError(28, "No space left on device")

    monkeypatch.setattr("brilliant_mqtt.heartbeat._atomic_write", _raise)
    write_phase(str(phase), "pre_bus")  # must not raise

    assert not phase.exists()
    assert bus_confirmed(str(phase)) is False
