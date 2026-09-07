"""Bus-liveness heartbeat for the independent message-bus watchdog.

Successful reads offer a beat; writes are capped at a ten-second cadence so
the hot poll does not churn tmpfs metadata. The watchdog's stale threshold is
far longer. tmpfs remains the default, so there is no flash wear.
"""

from __future__ import annotations

import fcntl
import logging
import math
import os
import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Literal, TextIO

logger = logging.getLogger(__name__)

_MIN_WRITE_INTERVAL_S = 10.0
_PHASE_LEASE_ATTEMPTS = 3
_PHASE_LEASE_RETRY_S = 0.01
MAX_SESSION_RETRY_BACKOFF_S = 60.0
# Must exceed systemd's deploy/brilliant-mqtt.service RestartSec so a crashed
# bus-attempt writer can hand evidence to its replacement.
BRIDGE_SERVICE_RESTART_SEC = 5.0
DEAD_WRITER_RETENTION_S = BRIDGE_SERVICE_RESTART_SEC * 60
PHASE_RECORD_VERSION = "v2"
PHASE_ATTEMPT = "attempt"
PHASE_SUCCESS = "success"
MAX_PID = 2**31 - 1
_last_attempt: dict[str, float] = {}

BusPhase = Literal["pre_bus", "bus"]


@dataclass(frozen=True, slots=True)
class PhaseRecord:
    phase: BusPhase
    pid: int
    process_generation: str
    boot_id: str
    failure_started_at: float | None
    bus_updated_at: float | None
    bus_read_succeeded: bool | None

    def encode(self) -> str:
        timing = (
            ("-", "-", "-")
            if self.failure_started_at is None
            else (
                repr(self.failure_started_at),
                repr(self.bus_updated_at),
                PHASE_SUCCESS if self.bus_read_succeeded else PHASE_ATTEMPT,
            )
        )
        return " ".join(
            (
                PHASE_RECORD_VERSION,
                self.phase,
                str(self.pid),
                self.process_generation,
                self.boot_id,
                *timing,
            )
        )

    @classmethod
    def parse(cls, text: str) -> PhaseRecord | None:
        parts = text.split()
        if (
            len(parts) != 8
            or parts[0] != PHASE_RECORD_VERSION
            or parts[1] not in ("pre_bus", "bus")
        ):
            return None
        try:
            pid = int(parts[2])
        except ValueError:
            return None
        generation = parts[3]
        boot_id = parts[4]
        if (
            not 1 < pid <= MAX_PID
            or not generation.isdecimal()
            or not boot_id
            or len(boot_id) > 128
        ):
            return None
        timing_parts = parts[5:]
        if timing_parts == ["-", "-", "-"]:
            if parts[1] == "bus":
                return None
            timing: tuple[float | None, float | None] = (None, None)
            read_succeeded: bool | None = None
        else:
            if timing_parts[2] not in (PHASE_ATTEMPT, PHASE_SUCCESS):
                return None
            try:
                parsed = (float(timing_parts[0]), float(timing_parts[1]))
            except ValueError:
                return None
            if (
                not all(math.isfinite(value) and value >= 0.0 for value in parsed)
                or parsed[0] > parsed[1]
            ):
                return None
            timing = parsed
            read_succeeded = timing_parts[2] == PHASE_SUCCESS
        phase: BusPhase = "pre_bus" if parts[1] == "pre_bus" else "bus"
        return cls(
            phase=phase,
            pid=pid,
            process_generation=generation,
            boot_id=boot_id,
            failure_started_at=timing[0],
            bus_updated_at=timing[1],
            bus_read_succeeded=read_succeeded,
        )


@dataclass(slots=True)
class _PhaseLease:
    stream: TextIO
    last_success_write: float | None


_phase_leases: dict[str, _PhaseLease] = {}
_owned_phase_records: dict[str, PhaseRecord] = {}


def current_boot_id() -> str | None:
    try:
        with open("/proc/sys/kernel/random/boot_id", encoding="utf-8") as stream:
            boot_id = stream.read().strip()
    except (OSError, UnicodeError):
        return None
    return boot_id or None


def process_generation(pid: int) -> str | None:
    """Return Linux's per-process start tick, which disambiguates PID reuse."""
    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8") as stream:
            stat = stream.read()
    except (OSError, UnicodeError):
        return None
    command_end = stat.rfind(")")
    if command_end < 0:
        return None
    fields_after_command = stat[command_end + 1 :].split()
    if len(fields_after_command) <= 19:
        return None
    start_tick = fields_after_command[19]
    return start_tick if start_tick.isdecimal() else None


def process_is_absent(pid: int) -> bool:
    """Return true only when the kernel confirms that *pid* does not exist."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except (OSError, OverflowError):
        return False
    return False


def read_phase_record(path: str) -> PhaseRecord | None:
    try:
        with open(path, encoding="utf-8") as stream:
            return PhaseRecord.parse(stream.read())
    except (OSError, UnicodeError):
        return None


def _release_phase_lease(path: str) -> None:
    lease = _phase_leases.pop(path, None)
    if lease is not None:
        lease.stream.close()


def _acquire_phase_lease(path: str, last_success_write: float | None) -> None:
    stream = open(path, "r+", encoding="utf-8")
    try:
        for attempt in range(_PHASE_LEASE_ATTEMPTS):
            try:
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                if attempt == _PHASE_LEASE_ATTEMPTS - 1:
                    raise
                time.sleep(_PHASE_LEASE_RETRY_S)
            else:
                break
    except BaseException:
        stream.close()
        raise
    _phase_leases[path] = _PhaseLease(stream, last_success_write)


def _rewrite_phase_lease(path: str, text: str) -> bool:
    lease = _phase_leases.get(path)
    if lease is None:
        return False
    try:
        lease.stream.seek(0)
        lease.stream.write(text)
        lease.stream.truncate()
        lease.stream.flush()
    except (OSError, UnicodeError, ValueError):
        return False
    return True


def _inheritable_history(
    record: PhaseRecord | None,
    *,
    boot_id: str,
    pid: int,
    generation: str,
    owned: bool,
    now: float,
) -> PhaseRecord | None:
    if (
        record is None
        or record.boot_id != boot_id
        or record.failure_started_at is None
        or record.bus_updated_at is None
        or record.bus_read_succeeded is None
        or record.failure_started_at > now
        or record.bus_updated_at > now
        or now - record.bus_updated_at > DEAD_WRITER_RETENTION_S
    ):
        return None
    if record.pid == pid:
        if not owned or record.process_generation != generation:
            return None
        return None if record.bus_read_succeeded else record
    if record.bus_read_succeeded or not process_is_absent(record.pid):
        return None
    return record


def _invalidate_failed_write(path: str, write_error: BaseException) -> None:
    try:
        os.unlink(path)
    except FileNotFoundError:
        logger.warning(
            "bus phase write failed for %s; marker absent and reboot guard disabled",
            path,
            exc_info=(type(write_error), write_error, write_error.__traceback__),
        )
    except OSError as invalidation_error:
        logger.error(
            "bus phase write failed for %s and marker invalidation failed: %s; "
            "stale marker remains but its live-writer lease was released",
            path,
            invalidation_error,
            exc_info=(type(write_error), write_error, write_error.__traceback__),
        )
    else:
        logger.warning(
            "bus phase write failed for %s; marker cleared and reboot guard disabled",
            path,
            exc_info=(type(write_error), write_error, write_error.__traceback__),
        )


def _atomic_write(path: str, text: str) -> None:
    """Write *text* into *path* via tmp-file + ``os.replace``.

    Both heartbeat and phase files live under the same runtime directory
    (tmpfs, e.g. ``/run/brilliant-mqtt/``), which may not exist yet on first
    boot — creating the parent here is deliberate for both callers, not an
    accident of one writer. Raises ``OSError`` on failure; callers decide
    whether/how to swallow it. On a failed write the tmp scratch file is
    removed best-effort first, so a failed ``os.replace`` (e.g. the destination
    is a directory) never leaks ``<path>.tmp``.
    """
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    tmp = f"{path}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp, path)
    except OSError:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def write_heartbeat(
    path: str,
    clock: Callable[[], float],
    monotonic_clock: Callable[[], float] = time.monotonic,
) -> None:
    """Atomically stamp *path* with the current epoch seconds. Best-effort:
    writes are limited to one per ten seconds, an empty path is a no-op, and
    any I/O error is swallowed (a heartbeat failure must never disrupt the
    bridge)."""
    if not path:
        return
    now = monotonic_clock()
    last_attempt = _last_attempt.get(path)
    if last_attempt is not None and now - last_attempt < _MIN_WRITE_INTERVAL_S:
        return
    _last_attempt[path] = now
    try:
        _atomic_write(path, f"{clock()}")
    except OSError:
        logger.debug("heartbeat write failed for %s", path, exc_info=True)


def write_phase(
    path: str,
    phase: BusPhase,
    *,
    bus_read_succeeded: bool = False,
    monotonic_clock: Callable[[], float] = time.monotonic,
) -> bool:
    """Record timed, boot-aware local-bus attribution before bus startup.

    A live ``bus`` writer holds an exclusive lock on the marker inode. A failed
    ``pre_bus`` replacement rewrites that leased inode before releasing it, so
    the old bus attempt cannot become valid evidence after writer death. Recent
    records from dead writers remain bounded evidence for native-crash loops;
    boot IDs, process start ticks, and retention reject PID reuse and ancient
    records. A false return disables reboot attribution; it must not stop the
    bridge.
    """
    if not path:
        return True
    now = monotonic_clock()
    lease = _phase_leases.get(path)
    if (
        phase == "bus"
        and bus_read_succeeded
        and lease is not None
        and lease.last_success_write is not None
        and now - lease.last_success_write < _MIN_WRITE_INTERVAL_S
    ):
        return True
    previous = read_phase_record(path)
    owned = _owned_phase_records.get(path) == previous
    _owned_phase_records.pop(path, None)
    try:
        boot_id = current_boot_id()
        pid = os.getpid()
        generation = process_generation(pid)
        if boot_id is None or generation is None:
            raise OSError("cannot establish boot/process generation")
        if phase == "pre_bus":
            history = _inheritable_history(
                previous,
                boot_id=boot_id,
                pid=pid,
                generation=generation,
                owned=owned,
                now=now,
            )
            failure_started_at = history.failure_started_at if history is not None else None
            bus_updated_at = history.bus_updated_at if history is not None else None
            read_succeeded = history.bus_read_succeeded if history is not None else None
        else:
            history = _inheritable_history(
                previous,
                boot_id=boot_id,
                pid=pid,
                generation=generation,
                owned=owned,
                now=now,
            )
            failure_started_at = (
                history.failure_started_at
                if history is not None and not bus_read_succeeded
                else now
            )
            bus_updated_at = now
            read_succeeded = bus_read_succeeded
        record = PhaseRecord(
            phase=phase,
            pid=pid,
            process_generation=generation,
            boot_id=boot_id,
            failure_started_at=failure_started_at,
            bus_updated_at=bus_updated_at,
            bus_read_succeeded=read_succeeded,
        )
        encoded = record.encode()
        written_record = replace(record, phase="pre_bus") if phase == "bus" else record
        try:
            _atomic_write(path, written_record.encode())
        except (OSError, UnicodeError) as write_error:
            if _rewrite_phase_lease(path, encoded):
                _owned_phase_records[path] = record
                if phase == "pre_bus":
                    _release_phase_lease(path)
                else:
                    current_lease = _phase_leases[path]
                    current_lease.last_success_write = now if bus_read_succeeded else None
                logger.warning(
                    "bus phase atomic replacement failed for %s; marker updated in place",
                    path,
                    exc_info=(type(write_error), write_error, write_error.__traceback__),
                )
                return True
            _owned_phase_records.pop(path, None)
            _release_phase_lease(path)
            _invalidate_failed_write(path, write_error)
            return False
        _owned_phase_records[path] = written_record
        _release_phase_lease(path)
        if phase == "bus":
            try:
                _acquire_phase_lease(path, now if bus_read_succeeded else None)
            except BlockingIOError:
                _owned_phase_records.pop(path, None)
                logger.warning(
                    "bus phase lease unavailable for %s after bounded retries; "
                    "reboot guard disabled",
                    path,
                    exc_info=True,
                )
                return False
            if not _rewrite_phase_lease(path, encoded):
                raise OSError("cannot arm bus phase marker")
            _owned_phase_records[path] = record
        return True
    except BlockingIOError:
        _release_phase_lease(path)
        return False
    except (OSError, UnicodeError) as error:
        _owned_phase_records.pop(path, None)
        _release_phase_lease(path)
        _invalidate_failed_write(path, error)
        return False
