from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools.brilliant_vc.monitor import (
    LogCounters,
    Monitor,
    Observation,
    ProcSnapshot,
    Thresholds,
    calculate_cpu_percent,
    count_allowlisted_log_events,
    read_proc_snapshot,
    terminate_exact_process,
)


def _write_proc_tree(root: Path, *, pid: int, process_ticks: int, total_ticks: int) -> None:
    process = root / str(pid)
    process.mkdir(parents=True, exist_ok=True)
    # Fields 14+15 are process CPU ticks; field 22 is process start time.
    fields = ["S"] + ["0"] * 19
    fields[11] = str(process_ticks - 3)
    fields[12] = "3"
    fields[19] = "4242"
    (process / "stat").write_text(f"{pid} (pilot worker (vc)) {' '.join(fields)}\n")
    (process / "statm").write_text("1000 25 0 0 0 0 0\n")
    (process / "comm").write_text("vc-pilot\n")
    (root / "stat").write_text(
        f"cpu  {total_ticks - 10} 1 2 3 4 0 0 0 0 0\ncpu0 1 1 1 1 1 0 0 0 0 0\n"
    )


def test_reads_proc_snapshot_and_calculates_delta_cpu(tmp_path: Path) -> None:
    _write_proc_tree(tmp_path, pid=321, process_ticks=20, total_ticks=1000)
    previous = read_proc_snapshot(tmp_path, 321, page_size=4096)
    _write_proc_tree(tmp_path, pid=321, process_ticks=50, total_ticks=1200)
    current = read_proc_snapshot(tmp_path, 321, page_size=4096)

    assert previous.rss_bytes == 25 * 4096
    assert previous.start_ticks == current.start_ticks == 4242
    assert calculate_cpu_percent(previous, current) == pytest.approx(15.0)


def test_proc_snapshot_rejects_pid_reuse() -> None:
    previous = ProcSnapshot(1, 10, 100, 1, 50, "vc-pilot")
    current = ProcSnapshot(1, 20, 200, 1, 51, "vc-pilot")

    with pytest.raises(RuntimeError, match="identity changed"):
        calculate_cpu_percent(previous, current)


def test_terminator_refuses_protected_panel_process() -> None:
    snapshot = ProcSnapshot(1, 10, 100, 1, 50, "message_bus")

    with pytest.raises(RuntimeError, match="protected panel process"):
        terminate_exact_process(snapshot)


def test_log_parser_returns_counts_only_and_never_retains_lines() -> None:
    secret = "Bearer synthetic-sensitive-value"
    counters = count_allowlisted_log_events(
        [
            f"peer add timed out Authorization={secret}",
            f"cloud peer disconnected token={secret}",
            f"reconnecting message bus password={secret}",
            f"ordinary line certificate={secret}",
        ]
    )

    assert counters == LogCounters(
        peer_add_timeouts=1,
        cloud_disconnects=1,
        reconnect_events=1,
    )
    assert secret not in json.dumps(counters.to_public_dict())


def _observation(
    *,
    process_ticks: int,
    total_ticks: int,
    rss_bytes: int = 1024,
    counters: LogCounters | None = None,
    physical_lag: bool = False,
) -> Observation:
    return Observation(
        timestamp_s=1_700_000_000,
        process=ProcSnapshot(
            pid=77,
            process_ticks=process_ticks,
            total_ticks=total_ticks,
            rss_bytes=rss_bytes,
            start_ticks=900,
            comm="vc-pilot",
        ),
        load_average=(0.1, 0.2, 0.3),
        peer_count=4,
        event_deltas=LogCounters() if counters is None else counters,
        mqtt_round_trip_ms=12.5,
        physical_lag=physical_lag,
    )


def test_sustained_cpu_violation_terminates_exactly_once() -> None:
    terminated: list[ProcSnapshot] = []
    monitor = Monitor(
        thresholds=Thresholds(cpu_percent=15.0, sustained_cpu_samples=3),
        terminator=terminated.append,
    )

    monitor.observe(_observation(process_ticks=0, total_ticks=0))
    monitor.observe(_observation(process_ticks=20, total_ticks=100))
    monitor.observe(_observation(process_ticks=40, total_ticks=200))
    result = monitor.observe(_observation(process_ticks=60, total_ticks=300))
    later = monitor.observe(_observation(process_ticks=80, total_ticks=400))

    assert result.abort_reason == "sustained_cpu_percent"
    assert later.abort_reason == "sustained_cpu_percent"
    assert len(terminated) == 1
    assert terminated[0].pid == 77


@pytest.mark.parametrize(
    ("rss_bytes", "counters", "physical_lag", "reason"),
    [
        (101 * 1024 * 1024, LogCounters(), False, "rss_bytes"),
        (1024, LogCounters(peer_add_timeouts=1), False, "peer_add_timeouts"),
        (1024, LogCounters(cloud_disconnects=1), False, "cloud_disconnects"),
        (1024, LogCounters(reconnect_events=1), False, "reconnect_events"),
        (1024, LogCounters(), True, "physical_lag"),
    ],
)
def test_hard_threshold_violation_terminates_once(
    rss_bytes: int,
    counters: LogCounters,
    physical_lag: bool,
    reason: str,
) -> None:
    terminated: list[ProcSnapshot] = []
    monitor = Monitor(thresholds=Thresholds(), terminator=terminated.append)
    monitor.observe(_observation(process_ticks=0, total_ticks=0))

    result = monitor.observe(
        _observation(
            process_ticks=1,
            total_ticks=100,
            rss_bytes=rss_bytes,
            counters=counters,
            physical_lag=physical_lag,
        )
    )

    assert result.abort_reason == reason
    assert len(terminated) == 1


def test_public_sample_contains_only_allowlisted_scalar_fields() -> None:
    monitor = Monitor(thresholds=Thresholds(), terminator=lambda _: None)
    monitor.observe(_observation(process_ticks=0, total_ticks=0))
    result = monitor.observe(_observation(process_ticks=1, total_ticks=100))

    payload = result.to_public_dict()
    assert set(payload) == {
        "timestamp_s",
        "pid",
        "cpu_percent",
        "rss_bytes",
        "load_average",
        "peer_count",
        "peer_add_timeouts",
        "cloud_disconnects",
        "reconnect_events",
        "mqtt_round_trip_ms",
        "physical_lag",
        "abort_reason",
    }
    assert all(
        isinstance(value, (int, float, bool, str, list, type(None))) for value in payload.values()
    )
