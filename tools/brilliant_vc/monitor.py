"""Bounded, redaction-safe monitor for a disposable Virtual Control process."""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import stat
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

_PEER_TIMEOUT = re.compile(
    r"(?:peer.*add.*tim(?:e|ed).*out|timed out.*(?:add|contacting).*peer)", re.IGNORECASE
)
_CLOUD_DISCONNECT = re.compile(
    r"(?:cloud.*peer.*disconnect|disconnect.*cloud.*peer)", re.IGNORECASE
)
_RECONNECT = re.compile(r"\breconnect(?:ing|ed|ion)?\b", re.IGNORECASE)
_PROTECTED_COMM = frozenset({"message_bus", "switch-ui", "brilliant-mqtt"})


@dataclass(frozen=True, slots=True)
class ProcSnapshot:
    """Allowlisted process and system counters from one `/proc` read."""

    pid: int
    process_ticks: int
    total_ticks: int
    rss_bytes: int
    start_ticks: int
    comm: str


@dataclass(frozen=True, slots=True)
class LogCounters:
    """Counts derived from logs without retaining any source line."""

    peer_add_timeouts: int = 0
    cloud_disconnects: int = 0
    reconnect_events: int = 0

    def to_public_dict(self) -> dict[str, int]:
        return {
            "peer_add_timeouts": self.peer_add_timeouts,
            "cloud_disconnects": self.cloud_disconnects,
            "reconnect_events": self.reconnect_events,
        }


@dataclass(frozen=True, slots=True)
class Thresholds:
    """Fail-closed bounds for the disposable process and bus health."""

    cpu_percent: float = 15.0
    rss_bytes: int = 100 * 1024 * 1024
    peer_add_timeouts: int = 1
    cloud_disconnects: int = 1
    reconnect_events: int = 1
    sustained_cpu_samples: int = 5

    def __post_init__(self) -> None:
        if self.cpu_percent <= 0 or self.rss_bytes <= 0:
            raise ValueError("resource thresholds must be positive")
        if (
            min(
                self.peer_add_timeouts,
                self.cloud_disconnects,
                self.reconnect_events,
                self.sustained_cpu_samples,
            )
            < 1
        ):
            raise ValueError("event and sustained-sample thresholds must be positive")


@dataclass(frozen=True, slots=True)
class Observation:
    """One already-sanitized runtime observation."""

    timestamp_s: int
    process: ProcSnapshot
    load_average: tuple[float, float, float]
    peer_count: int | None
    event_deltas: LogCounters
    mqtt_round_trip_ms: float | None
    physical_lag: bool


@dataclass(frozen=True, slots=True)
class PublicSample:
    """Secret-free JSONL record emitted by the monitor."""

    timestamp_s: int
    pid: int
    cpu_percent: float | None
    rss_bytes: int
    load_average: tuple[float, float, float]
    peer_count: int | None
    peer_add_timeouts: int
    cloud_disconnects: int
    reconnect_events: int
    mqtt_round_trip_ms: float | None
    physical_lag: bool
    abort_reason: str | None

    def to_public_dict(self) -> dict[str, object]:
        return {
            "timestamp_s": self.timestamp_s,
            "pid": self.pid,
            "cpu_percent": self.cpu_percent,
            "rss_bytes": self.rss_bytes,
            "load_average": list(self.load_average),
            "peer_count": self.peer_count,
            "peer_add_timeouts": self.peer_add_timeouts,
            "cloud_disconnects": self.cloud_disconnects,
            "reconnect_events": self.reconnect_events,
            "mqtt_round_trip_ms": self.mqtt_round_trip_ms,
            "physical_lag": self.physical_lag,
            "abort_reason": self.abort_reason,
        }


class Monitor:
    """Stateful threshold evaluator that invokes one exact-PID terminator."""

    def __init__(
        self,
        *,
        thresholds: Thresholds,
        terminator: Callable[[ProcSnapshot], None],
    ) -> None:
        self._thresholds = thresholds
        self._terminator = terminator
        self._previous: ProcSnapshot | None = None
        self._cpu_violation_streak = 0
        self._abort_reason: str | None = None
        self._terminated = False

    def observe(self, observation: Observation) -> PublicSample:
        """Evaluate one observation and terminate once on the first violation."""

        cpu_percent: float | None = None
        if self._previous is not None:
            cpu_percent = calculate_cpu_percent(self._previous, observation.process)
            if cpu_percent > self._thresholds.cpu_percent:
                self._cpu_violation_streak += 1
            else:
                self._cpu_violation_streak = 0

        if self._abort_reason is None:
            self._abort_reason = self._find_violation(observation)
            if (
                self._abort_reason is None
                and self._cpu_violation_streak >= self._thresholds.sustained_cpu_samples
            ):
                self._abort_reason = "sustained_cpu_percent"

        if self._abort_reason is not None and not self._terminated:
            self._terminated = True
            self._terminator(observation.process)

        self._previous = observation.process
        return PublicSample(
            timestamp_s=observation.timestamp_s,
            pid=observation.process.pid,
            cpu_percent=None if cpu_percent is None else round(cpu_percent, 3),
            rss_bytes=observation.process.rss_bytes,
            load_average=(
                round(observation.load_average[0], 3),
                round(observation.load_average[1], 3),
                round(observation.load_average[2], 3),
            ),
            peer_count=observation.peer_count,
            peer_add_timeouts=observation.event_deltas.peer_add_timeouts,
            cloud_disconnects=observation.event_deltas.cloud_disconnects,
            reconnect_events=observation.event_deltas.reconnect_events,
            mqtt_round_trip_ms=observation.mqtt_round_trip_ms,
            physical_lag=observation.physical_lag,
            abort_reason=self._abort_reason,
        )

    def _find_violation(self, observation: Observation) -> str | None:
        if observation.process.rss_bytes > self._thresholds.rss_bytes:
            return "rss_bytes"
        if observation.event_deltas.peer_add_timeouts >= self._thresholds.peer_add_timeouts:
            return "peer_add_timeouts"
        if observation.event_deltas.cloud_disconnects >= self._thresholds.cloud_disconnects:
            return "cloud_disconnects"
        if observation.event_deltas.reconnect_events >= self._thresholds.reconnect_events:
            return "reconnect_events"
        if observation.physical_lag:
            return "physical_lag"
        return None


def read_proc_snapshot(
    proc_root: Path,
    pid: int,
    *,
    page_size: int | None = None,
) -> ProcSnapshot:
    """Read only required fields, correctly handling spaces/parentheses in comm."""

    if pid <= 0:
        raise ValueError("pid must be positive")
    process_root = proc_root / str(pid)
    process_stat = (process_root / "stat").read_text(encoding="ascii", errors="strict").strip()
    closing = process_stat.rfind(")")
    if closing < 0:
        raise RuntimeError("malformed process stat")
    tail = process_stat[closing + 2 :].split()
    if len(tail) <= 19:
        raise RuntimeError("process stat is missing required fields")
    try:
        process_ticks = int(tail[11]) + int(tail[12])
        start_ticks = int(tail[19])
    except ValueError:
        raise RuntimeError("process stat contains a non-integer counter") from None

    system_lines = (proc_root / "stat").read_text(encoding="ascii", errors="strict").splitlines()
    aggregate = next((line for line in system_lines if line.startswith("cpu ")), None)
    if aggregate is None:
        raise RuntimeError("system stat lacks aggregate CPU counters")
    try:
        total_ticks = sum(int(value) for value in aggregate.split()[1:])
        resident_pages = int(
            (process_root / "statm").read_text(encoding="ascii", errors="strict").split()[1]
        )
    except (ValueError, IndexError):
        raise RuntimeError("proc counter contains an invalid integer") from None

    resolved_page_size = page_size if page_size is not None else os.sysconf("SC_PAGE_SIZE")
    if not isinstance(resolved_page_size, int) or resolved_page_size <= 0:
        raise RuntimeError("could not determine a valid memory page size")
    comm = (process_root / "comm").read_text(encoding="utf-8", errors="replace").strip()
    return ProcSnapshot(
        pid=pid,
        process_ticks=process_ticks,
        total_ticks=total_ticks,
        rss_bytes=resident_pages * resolved_page_size,
        start_ticks=start_ticks,
        comm=comm,
    )


def calculate_cpu_percent(previous: ProcSnapshot, current: ProcSnapshot) -> float:
    """Calculate delta process ticks divided by delta aggregate CPU ticks."""

    if (
        previous.pid != current.pid
        or previous.start_ticks != current.start_ticks
        or previous.comm != current.comm
    ):
        raise RuntimeError("monitored process identity changed")
    process_delta = current.process_ticks - previous.process_ticks
    total_delta = current.total_ticks - previous.total_ticks
    if process_delta < 0 or total_delta <= 0:
        raise RuntimeError("proc counters moved backwards or did not advance")
    return process_delta / total_delta * 100.0


def count_allowlisted_log_events(lines: Iterable[str]) -> LogCounters:
    """Count known health events and discard all line contents immediately."""

    peer_timeouts = 0
    cloud_disconnects = 0
    reconnect_events = 0
    for line in lines:
        if _PEER_TIMEOUT.search(line):
            peer_timeouts += 1
        if _CLOUD_DISCONNECT.search(line):
            cloud_disconnects += 1
        if _RECONNECT.search(line):
            reconnect_events += 1
    return LogCounters(
        peer_add_timeouts=peer_timeouts,
        cloud_disconnects=cloud_disconnects,
        reconnect_events=reconnect_events,
    )


def terminate_exact_process(
    expected: ProcSnapshot,
    *,
    proc_root: Path = Path("/proc"),
    timeout_s: float = 10.0,
) -> None:
    """Terminate only the still-matching PID/start-time identity."""

    if expected.comm in _PROTECTED_COMM:
        raise RuntimeError("refusing to terminate a protected panel process")
    try:
        current = read_proc_snapshot(proc_root, expected.pid)
    except FileNotFoundError:
        return
    _assert_same_identity(expected, current)
    os.kill(expected.pid, signal.SIGTERM)
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            current = read_proc_snapshot(proc_root, expected.pid)
        except FileNotFoundError:
            return
        _assert_same_identity(expected, current)
        time.sleep(0.1)
    os.kill(expected.pid, signal.SIGKILL)


def _assert_same_identity(expected: ProcSnapshot, current: ProcSnapshot) -> None:
    if (
        current.pid != expected.pid
        or current.start_ticks != expected.start_ticks
        or current.comm != expected.comm
    ):
        raise RuntimeError("refusing to signal a changed process identity")


def _read_new_lines(path: Path | None, offset: int) -> tuple[list[str], int]:
    if path is None:
        return [], offset
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        handle.seek(offset)
        lines = handle.readlines()
        return lines, handle.tell()


def _read_optional_number(path: Path | None, *, integer: bool) -> int | float | None:
    if path is None or not path.exists():
        return None
    text = path.read_text(encoding="ascii", errors="strict").strip()
    if not text:
        return None
    try:
        return int(text) if integer else float(text)
    except ValueError:
        raise RuntimeError("sanitized marker file contains an invalid number") from None


def _read_optional_bool(path: Path | None) -> bool:
    value = _read_optional_number(path, integer=True)
    if value is None:
        return False
    if value not in (0, 1):
        raise RuntimeError("physical-lag marker must contain 0 or 1")
    return bool(value)


def _open_private_jsonl(path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink():
        raise RuntimeError("output path must not be a symlink")
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    metadata = os.fstat(descriptor)
    if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) & 0o077:
        os.close(descriptor)
        raise RuntimeError("output must be a private regular file")
    return descriptor


def run_bounded_monitor(
    *,
    pid: int,
    duration_s: float,
    interval_s: float,
    output_jsonl: Path,
    journal_file: Path | None = None,
    peer_count_file: Path | None = None,
    mqtt_latency_file: Path | None = None,
    physical_lag_file: Path | None = None,
    proc_root: Path = Path("/proc"),
) -> str | None:
    """Run the bounded monitor and return the first abort reason, if any."""

    if not 60 <= duration_s <= 90_000:
        raise ValueError("duration_s must be between 60 and 90000")
    if interval_s <= 0 or interval_s > duration_s:
        raise ValueError("interval_s must be positive and no longer than duration_s")

    monitor = Monitor(
        thresholds=Thresholds(),
        terminator=lambda sample: terminate_exact_process(sample, proc_root=proc_root),
    )
    descriptor = _open_private_jsonl(output_jsonl)
    started = time.monotonic()
    journal_offset = (
        journal_file.stat().st_size if journal_file is not None and journal_file.exists() else 0
    )
    abort_reason: str | None = None
    try:
        while time.monotonic() - started <= duration_s:
            lines, journal_offset = _read_new_lines(journal_file, journal_offset)
            peer_count_value = _read_optional_number(peer_count_file, integer=True)
            mqtt_latency_value = _read_optional_number(mqtt_latency_file, integer=False)
            observation = Observation(
                timestamp_s=int(time.time()),
                process=read_proc_snapshot(proc_root, pid),
                load_average=os.getloadavg(),
                peer_count=None if peer_count_value is None else int(peer_count_value),
                event_deltas=count_allowlisted_log_events(lines),
                mqtt_round_trip_ms=(
                    None if mqtt_latency_value is None else float(mqtt_latency_value)
                ),
                physical_lag=_read_optional_bool(physical_lag_file),
            )
            sample = monitor.observe(observation)
            serialized = json.dumps(sample.to_public_dict(), sort_keys=True) + "\n"
            os.write(descriptor, serialized.encode("utf-8"))
            os.fsync(descriptor)
            abort_reason = sample.abort_reason
            if abort_reason is not None:
                break
            remaining = duration_s - (time.monotonic() - started)
            if remaining <= 0:
                break
            time.sleep(min(interval_s, remaining))
    finally:
        os.close(descriptor)
    return abort_reason


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pid", required=True, type=int)
    parser.add_argument("--duration-s", required=True, type=float)
    parser.add_argument("--interval-s", default=5.0, type=float)
    parser.add_argument("--output-jsonl", required=True, type=Path)
    parser.add_argument("--journal-file", type=Path)
    parser.add_argument("--peer-count-file", type=Path)
    parser.add_argument("--mqtt-latency-file", type=Path)
    parser.add_argument("--physical-lag-file", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    abort_reason = run_bounded_monitor(
        pid=args.pid,
        duration_s=args.duration_s,
        interval_s=args.interval_s,
        output_jsonl=args.output_jsonl,
        journal_file=args.journal_file,
        peer_count_file=args.peer_count_file,
        mqtt_latency_file=args.mqtt_latency_file,
        physical_lag_file=args.physical_lag_file,
    )
    if abort_reason is not None:
        print(f"monitor aborted: {abort_reason}")
        return 2
    print("monitor completed without an abort threshold")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
