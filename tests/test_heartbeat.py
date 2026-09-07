"""Tests: bus-liveness heartbeat writer."""

from __future__ import annotations

import fcntl
import logging
import os
import time
from pathlib import Path

import pytest

from brilliant_bus_watchdog.health import bus_failure_age
from brilliant_bus_watchdog.run import should_reboot
from brilliant_mqtt import heartbeat
from brilliant_mqtt.heartbeat import DEAD_WRITER_RETENTION_S, write_heartbeat, write_phase
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

    record = heartbeat.read_phase_record(str(phase))
    assert record is not None
    assert record.phase == "bus"
    assert record.pid == os.getpid()
    assert record.failure_started_at is not None
    assert record.bus_read_succeeded is False


def test_successful_bus_read_resets_preserved_failure_history(tmp_path: Path) -> None:
    phase = tmp_path / "bus-phase"
    write_phase(str(phase), "bus", monotonic_clock=lambda: 100.0)
    write_phase(str(phase), "pre_bus", monotonic_clock=lambda: 300.0)
    write_phase(str(phase), "bus", monotonic_clock=lambda: 300.0)
    assert bus_failure_age(str(phase), now=400.0) == 300.0

    write_phase(
        str(phase),
        "bus",
        bus_read_succeeded=True,
        monotonic_clock=lambda: 400.0,
    )

    assert bus_failure_age(str(phase), now=400.0) == 0.0
    record = heartbeat.read_phase_record(str(phase))
    assert record is not None
    assert record.bus_read_succeeded is True


def test_failure_after_success_starts_fresh_attribution(tmp_path: Path) -> None:
    phase = tmp_path / "bus-phase"
    write_phase(str(phase), "bus", monotonic_clock=lambda: 100.0)
    write_phase(
        str(phase),
        "bus",
        bus_read_succeeded=True,
        monotonic_clock=lambda: 200.0,
    )

    write_phase(str(phase), "pre_bus", monotonic_clock=lambda: 250.0)
    write_phase(str(phase), "bus", monotonic_clock=lambda: 250.0)

    assert bus_failure_age(str(phase), now=250.0) == 0.0


@pytest.mark.parametrize(
    "spacing",
    [DEAD_WRITER_RETENTION_S - 1.0, DEAD_WRITER_RETENTION_S + 1.0],
)
def test_live_writer_failure_history_survives_retry_spacing(tmp_path: Path, spacing: float) -> None:
    phase = tmp_path / f"bus-phase-{spacing}"
    write_phase(str(phase), "bus", monotonic_clock=lambda: 100.0)
    retry_at = 100.0 + spacing

    write_phase(str(phase), "pre_bus", monotonic_clock=lambda: retry_at)
    write_phase(str(phase), "bus", monotonic_clock=lambda: retry_at)

    assert bus_failure_age(str(phase), now=retry_at) == spacing


def test_phase_lease_retries_a_transient_watchdog_lock_collision(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    phase = tmp_path / "bus-phase"
    real_flock = fcntl.flock
    collisions = 0

    def _flock(fd: int, operation: int) -> None:
        nonlocal collisions
        if operation == fcntl.LOCK_EX | fcntl.LOCK_NB and collisions == 0:
            collisions += 1
            raise BlockingIOError
        real_flock(fd, operation)

    monkeypatch.setattr("brilliant_mqtt.heartbeat.fcntl.flock", _flock)
    write_phase(
        str(phase),
        "bus",
        bus_read_succeeded=True,
        monotonic_clock=lambda: 100.0,
    )

    assert collisions == 1
    assert bus_failure_age(str(phase), now=100.0) == 0.0


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
    live-pid marker, the file must be gone and bus_failure_age must return None, with
    no exception raised."""
    phase = tmp_path / "bus-phase"
    write_phase(
        str(phase),
        "bus",
        bus_read_succeeded=True,
        monotonic_clock=lambda: 100.0,
    )
    assert bus_failure_age(str(phase), now=100.0) == 0.0

    def _raise(*args: object, **kwargs: object) -> None:
        raise OSError(28, "No space left on device")

    monkeypatch.setattr("brilliant_mqtt.heartbeat._atomic_write", _raise)
    write_phase(str(phase), "pre_bus")  # must not raise

    assert not phase.exists()
    assert bus_failure_age(str(phase), now=100.0) is None


def test_real_write_failure_clears_live_pid_bus_marker(tmp_path: Path) -> None:
    """The REAL failure -> propagate -> clear chain, with no monkeypatch of
    _atomic_write: a pre-existing ``bus-phase.tmp`` directory makes the real
    ``open(tmp, "w")`` inside _atomic_write raise IsADirectoryError, which must
    propagate out of _atomic_write so write_phase's handler clears the leftover
    live-pid marker. This pins _atomic_write's re-raise (a mutant that swallows
    its OSError instead of re-raising leaves the marker confirmed)."""
    phase = tmp_path / "bus-phase"
    write_phase(str(phase), "bus", bus_read_succeeded=True)
    (tmp_path / "bus-phase.tmp").mkdir()  # real open() failure, no monkeypatch
    assert bus_failure_age(str(phase), now=time.monotonic()) is not None

    write_phase(str(phase), "pre_bus")  # must not raise

    assert not phase.exists()
    assert bus_failure_age(str(phase), now=time.monotonic()) is None


def test_write_phase_bus_failure_also_clears_marker(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The contract is best-effort clear for EVERY phase, not just pre_bus: a
    failed ``bus`` upgrade must also clear the leftover marker (a failed bus
    stamp that left the prior marker readable would misreport the phase). This
    pins the clear against a mutant that guards the unlink with
    ``if phase != "pre_bus": return``."""
    phase = tmp_path / "bus-phase"
    write_phase(str(phase), "bus", bus_read_succeeded=True)

    def _raise(*args: object, **kwargs: object) -> None:
        raise OSError(28, "No space left on device")

    monkeypatch.setattr("brilliant_mqtt.heartbeat._atomic_write", _raise)
    write_phase(str(phase), "bus")  # must not raise

    assert not phase.exists()


def test_failed_stamp_logs_warning_reboot_guard_disabled(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A failed stamp now means the reboot guard is disabled, and the default
    LOG_LEVEL is INFO, so the message must be logged at WARNING (not debug) or
    it is invisible in the journal. Pins the level against a warning->debug
    downgrade mutant."""
    phase = tmp_path / "bus-phase"

    def _raise(*args: object, **kwargs: object) -> None:
        raise OSError(28, "No space left on device")

    monkeypatch.setattr("brilliant_mqtt.heartbeat._atomic_write", _raise)
    with caplog.at_level(logging.WARNING, logger="brilliant_mqtt.heartbeat"):
        write_phase(str(phase), "pre_bus")

    warnings = [
        r
        for r in caplog.records
        if r.levelname == "WARNING" and "reboot guard disabled" in r.getMessage()
    ]
    assert len(warnings) == 1


def test_failed_phase_write_and_failed_unlink_do_not_authorize_reboot(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    phase = tmp_path / "bus-phase"
    write_phase(
        str(phase),
        "bus",
        bus_read_succeeded=True,
        monotonic_clock=lambda: 100.0,
    )

    def _denied(*args: object, **kwargs: object) -> None:
        raise PermissionError("readable marker in unwritable directory")

    monkeypatch.setattr(heartbeat, "_atomic_write", _denied)
    monkeypatch.setattr("brilliant_mqtt.heartbeat.os.unlink", _denied)
    with caplog.at_level(logging.WARNING, logger="brilliant_mqtt.heartbeat"):
        heartbeat.write_phase(str(phase), "pre_bus")

    failure_age = bus_failure_age(str(phase), now=1900.0)
    assert not should_reboot(
        age=1900.0,
        stale_after=1800.0,
        bridge_active=True,
        gateway_up=True,
        bus_failure_age=failure_age,
    )
    messages = [record.getMessage() for record in caplog.records]
    assert any("invalidation failed" in message for message in messages)
    assert not any("reboot guard disabled" in message for message in messages)


def test_real_unwritable_phase_directory_does_not_leave_reboot_armed(tmp_path: Path) -> None:
    if os.geteuid() == 0:
        pytest.skip("permission reproduction requires an unprivileged process")
    directory = tmp_path / "readonly"
    directory.mkdir()
    phase = directory / "bus-phase"
    write_phase(
        str(phase),
        "bus",
        bus_read_succeeded=True,
        monotonic_clock=lambda: 100.0,
    )
    directory.chmod(0o500)
    try:
        heartbeat.write_phase(str(phase), "pre_bus")
        assert bus_failure_age(str(phase), now=1900.0) is None
        assert not should_reboot(
            age=1900.0,
            stale_after=1800.0,
            bridge_active=True,
            gateway_up=True,
            bus_failure_age=bus_failure_age(str(phase), now=1900.0),
        )
    finally:
        directory.chmod(0o700)
