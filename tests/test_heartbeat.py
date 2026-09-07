"""Tests: bus-liveness heartbeat writer."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

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


def test_write_phase_pre_bus_failure_clears_stale_bus_marker(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A failed pre_bus write must not leave a prior session's "bus" marker
    readable — otherwise a broker-only outage would still read as
    bus-confirmed and could reboot a healthy panel. Remove it so the watchdog
    fails closed."""
    phase = tmp_path / "bus-phase"
    phase.write_text(f"bus {os.getpid()}", encoding="utf-8")  # leftover session

    def _raise(*args: object, **kwargs: object) -> None:
        raise OSError("write failed")

    monkeypatch.setattr("brilliant_mqtt.heartbeat._atomic_write", _raise)
    write_phase(str(phase), "pre_bus")  # must not raise

    assert not phase.exists()


def test_write_phase_pre_bus_reraises_when_cleanup_also_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """If clearing the stale marker itself fails, phase tracking is broken —
    re-raise so the session doesn't silently continue in a fail-unsafe state."""
    phase = tmp_path / "bus-phase"
    phase.write_text(f"bus {os.getpid()}", encoding="utf-8")

    def _raise_write(*args: object, **kwargs: object) -> None:
        raise OSError("write failed")

    def _raise_unlink(*args: object, **kwargs: object) -> None:
        raise PermissionError("cannot remove")

    monkeypatch.setattr("brilliant_mqtt.heartbeat._atomic_write", _raise_write)
    monkeypatch.setattr("brilliant_mqtt.heartbeat.os.unlink", _raise_unlink)

    with pytest.raises(PermissionError, match="cannot remove"):
        write_phase(str(phase), "pre_bus")
