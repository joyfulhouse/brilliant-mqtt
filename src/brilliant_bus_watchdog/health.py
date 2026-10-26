"""Watchdog health reads: fail closed and never raise."""

from __future__ import annotations

import fcntl
import os
import time
from dataclasses import dataclass

from .phase_record import (
    DEAD_WRITER_RETENTION_S,
    PhaseRecord,
    current_boot_id,
    process_generation,
    process_is_absent,
)


@dataclass
class BusFailureObserver:
    """One watchdog's evidence of a live writer releasing its marker lease."""

    _revoked: PhaseRecord | None = None
    _leased: tuple[PhaseRecord, tuple[int, int]] | None = None


def _read_phase_record_and_lease(
    path: str,
) -> tuple[PhaseRecord | None, bool, tuple[int, int] | None]:
    try:
        with open(path, encoding="utf-8") as stream:
            try:
                fcntl.flock(stream.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
            except BlockingIOError:
                leased = True
            else:
                leased = False
            # Keep a successful shared lock through the read: an unleased
            # observation must describe the inode whose record we parsed.
            stat = os.fstat(stream.fileno())
            return PhaseRecord.parse(stream.read()), leased, (stat.st_dev, stat.st_ino)
    except (OSError, UnicodeError):
        return None, False, None


def bus_failure_age(
    path: str,
    *,
    now: float | None = None,
    service_started_at: float | None = None,
    observer: BusFailureObserver | None = None,
) -> float | None:
    """Age of attributable local-bus failure, including bounded crash evidence."""
    sampled_at = time.monotonic() if now is None else now
    record, leased, inode = _read_phase_record_and_lease(path)
    if (
        record is None
        or record.phase != "bus"
        or record.boot_id != current_boot_id()
        or record.failure_started_at is None
        or record.bus_updated_at is None
        or record.bus_read_succeeded is None
        or (service_started_at is not None and record.bus_updated_at < service_started_at)
        or record.failure_started_at > sampled_at
        or record.bus_updated_at > sampled_at
    ):
        return None
    live_generation = process_generation(record.pid)
    if observer is not None:
        revoked = observer._revoked
        same_writer = revoked is not None and (
            record.boot_id,
            record.pid,
            record.process_generation,
        ) == (revoked.boot_id, revoked.pid, revoked.process_generation)
        if live_generation == record.process_generation:
            if not leased and observer._leased == (record, inode):
                observer._revoked = record
                return None
            if leased and inode is not None:
                observer._leased = (record, inode)
        if same_writer:
            if live_generation == record.process_generation and leased and record != revoked:
                observer._revoked = None
            else:
                return None
    if live_generation is not None:
        if live_generation != record.process_generation or not leased:
            return None
    # Without a live release sample, recovered-then-dead and native-crash
    # attempts still have identical observables. Preserve bounded #133 recovery.
    elif not process_is_absent(record.pid) or (
        leased
        or record.bus_read_succeeded
        or sampled_at > record.bus_updated_at + DEAD_WRITER_RETENTION_S
    ):
        return None
    return sampled_at - record.failure_started_at


def heartbeat_age(path: str, *, now: float, started_at: float) -> float:
    """Seconds since the bridge last stamped *path*. If the file is absent or
    unparsable, age is measured from *started_at* (the watchdog's own start) so
    a never-seen heartbeat can't read as infinitely stale right after boot."""
    try:
        with open(path, encoding="utf-8") as f:
            stamped = float(f.read().strip())
    except (OSError, ValueError):
        return now - started_at
    return now - stamped
