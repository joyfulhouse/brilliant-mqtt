"""The snapshot sampler: production bounds, the real generated script, the classifier."""

from __future__ import annotations

import json
import math
import os
import shlex
import subprocess
import sys
import time
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from custom_components.brilliant_mqtt import panel_ops
from custom_components.brilliant_mqtt.const import (
    COMPONENT_BRIDGE,
    COMPONENT_BUS_WATCHDOG,
    COMPONENT_WIFI_WATCHDOG,
)
from custom_components.brilliant_mqtt.panel_ops import (
    ServiceActivity,
    ServiceSample,
    SnapshotSampling,
    classify_service_samples,
)

_COMPONENTS = (COMPONENT_BRIDGE, COMPONENT_WIFI_WATCHDOG, COMPONENT_BUS_WATCHDOG)
_RUNNING = "active|running|0"
_STOPPED = "inactive|dead|0"


def _sampling(
    *,
    window: float = 100.0,
    minimum: int = 2,
    interval: float = 0.0,
    deadline: float = 200.0,
    call_timeout: float = 3.0,
    maximum: int = 32,
) -> SnapshotSampling:
    return SnapshotSampling(window, minimum, interval, deadline, call_timeout, maximum)


# --- production bounds ---------------------------------------------------------


def test_production_sampling_values_are_pinned() -> None:
    assert panel_ops.SNAPSHOT_SAMPLING == SnapshotSampling(
        window_seconds=13.5,
        minimum_samples=2,
        interval_seconds=0.5,
        deadline_seconds=40,
        call_timeout_seconds=3,
        maximum_rounds=32,
    )
    assert panel_ops._MAX_SAMPLE_ROUNDS == 32
    assert panel_ops._MAX_LAYOUT_WIRE_BYTES == 16 * 1024


def test_layout_probe_defaults_to_production_sampling() -> None:
    assert panel_ops._layout_probe_command() == panel_ops._layout_probe_command(
        panel_ops.SNAPSHOT_SAMPLING
    )
    command = panel_ops._layout_probe_command()
    assert command.startswith(
        "/data/switch-embedded/env/bin/python3 - <<'BRILLIANT_MQTT_SNAPSHOT'\n"
    )
    for line in (
        "window = 13.5\n",
        "interval = 0.5\n",
        "call_timeout = 3.0\n",
        "minimum_rounds = 2\n",
        "maximum_rounds = 32\n",
        "maximum_gap = 4.0\n",
        "deadline = started + 40.0\n",
    ):
        assert line in command


def test_shared_state_vocabulary_is_defined_once() -> None:
    assert panel_ops._KNOWN_ACTIVE_STATES == (
        panel_ops._RUNNING_ACTIVE_STATES
        | panel_ops._STOPPED_ACTIVE_STATES
        | panel_ops._TRANSITIONAL_ACTIVE_STATES
        | {"unknown"}
    )
    command = panel_ops._layout_probe_command()
    assert f"known_states = {tuple(sorted(panel_ops._KNOWN_ACTIVE_STATES))!r}" in command
    assert repr(panel_ops._SUB_STATE_PATTERN) in command
    assert repr(panel_ops._RESTARTS_PATTERN) in command
    stop = panel_ops._stop_service_command("brilliant-mqtt")
    assert "|".join(sorted(panel_ops._SETTLED_FOR_STOP_STATES)) + ")" in stop


@pytest.mark.parametrize(
    "changes",
    [
        {"minimum_samples": 0},
        {"maximum_rounds": 33},
        {"minimum_samples": 5, "maximum_rounds": 4},
        {"interval_seconds": -0.1},
        {"window_seconds": -1.0},
        {"call_timeout_seconds": 0.0},
        {"call_timeout_seconds": -1.0},
        {"window_seconds": 10.0, "call_timeout_seconds": 3.0, "deadline_seconds": 12.9},
        {"window_seconds": math.nan},
        {"interval_seconds": math.nan},
        {"deadline_seconds": math.nan},
        {"call_timeout_seconds": math.nan},
        {"window_seconds": 16.0, "interval_seconds": 0.5, "maximum_rounds": 32},
    ],
)
def test_sampling_rejects_unsafe_bounds(changes: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="invalid_snapshot_sampling"):
        replace(_sampling(), **changes)


def test_sampling_allows_zero_interval_for_tests() -> None:
    assert _sampling(interval=0.0, window=0.0, deadline=3.0).interval_seconds == 0.0
    assert _sampling(window=15.5, interval=0.5, deadline=40.0).maximum_rounds == 32


def test_worst_case_layout_payload_fits_half_the_wire_bound() -> None:
    sample = "deactivating|" + "a" * 32 + "|" + "9" * 12
    assert ServiceSample.parse(sample) is not None
    target = "/var/brilliant-mqtt/releases/" + "v" * 128 + "--" + "f" * 32
    payload = json.dumps(
        {
            "active_release_target": target,
            "enabled": dict.fromkeys(_COMPONENTS, False),
            "layout": "release_link",
            "samples": {component: [sample] * 32 for component in _COMPONENTS},
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    assert len(payload.encode()) < panel_ops._MAX_LAYOUT_WIRE_BYTES // 2


# --- the real generated sampler against a fake batched systemctl ---------------

_FAKE_SYSTEMCTL = """\
import json, sys, time
from pathlib import Path
plan_path = Path({plan!r})
plan = json.loads(plan_path.read_text())
op = sys.argv[1]
if op == 'is-enabled':
    time.sleep(plan.get('enabled_delay', 0))
    print('enabled')
    raise SystemExit(0)
if op != 'show':
    raise SystemExit(2)
count = plan.get('count', 0)
plan['count'] = count + 1
plan.setdefault('times', []).append(time.monotonic())
plan_path.write_text(json.dumps(plan))
delays = plan.get('show_delays', [])
if count < len(delays):
    time.sleep(delays[count])
if 'raw' in plan:
    sys.stdout.write(plan['raw'])
    raise SystemExit(plan.get('exit', 0))
rounds = plan['rounds']
current = rounds[count % len(rounds)] if plan.get('cycle') else rounds[min(count, len(rounds) - 1)]
order = plan.get('order', ['ActiveState', 'SubState', 'NRestarts'])
blocks = []
for reading in current:
    values = dict(zip(['ActiveState', 'SubState', 'NRestarts'], reading.split('|')))
    blocks.append(''.join(key + '=' + values[key] + '\\n' for key in order))
sys.stdout.write('\\n'.join(blocks))
raise SystemExit(plan.get('exit', 0))
"""


def _run_sampler(
    tmp_path: Path,
    sampling: SnapshotSampling,
    plan: Mapping[str, object],
) -> tuple[subprocess.CompletedProcess[str], dict[str, Any]]:
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(plan))
    control = tmp_path / "systemctl"
    control.write_text(f"#!{sys.executable}\n" + _FAKE_SYSTEMCTL.format(plan=str(plan_path)))
    control.chmod(0o755)
    command = panel_ops._layout_probe_command(sampling).replace(
        panel_ops._PANEL_PYTHON, shlex.quote(sys.executable)
    )
    result = subprocess.run(
        ["/bin/sh", "-c", command],
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
        cwd=tmp_path,
        env={**os.environ, "PATH": f"{tmp_path}{os.pathsep}{os.environ['PATH']}"},
    )
    return result, json.loads(plan_path.read_text())


def _rounds(*rows: tuple[str, str, str]) -> list[list[str]]:
    return [list(row) for row in rows]


def _steady(reading: str) -> list[list[str]]:
    return [[reading, reading, reading]]


def _activities(stdout: str) -> dict[str, ServiceActivity]:
    value = json.loads(stdout)
    assert set(value) == {"active_release_target", "enabled", "layout", "samples"}
    samples = panel_ops._parse_service_samples(value["samples"])
    assert samples is not None
    return {component: classify_service_samples(samples[component]) for component in samples}


def _bridge_crash_cycle() -> list[list[str]]:
    # RestartSec=5 at a 0.5 s interval: ten auto-restart samples, then ~3 s up.
    waiting = [f"activating|auto-restart|{n}" for n in range(10)]
    running = ["active|running|1"] * 6
    return [[reading, _RUNNING, _RUNNING] for reading in waiting + running]


@pytest.mark.parametrize("offset", [0, 10], ids=["starts_waiting", "starts_running"])
def test_realistic_bridge_crash_loop_is_refused(tmp_path: Path, offset: int) -> None:
    cycle = _bridge_crash_cycle()
    plan = {"rounds": cycle[offset:] + cycle[:offset], "cycle": True}

    result, observed = _run_sampler(tmp_path, _sampling(maximum=28), plan)

    assert result.returncode == 0, result.stderr
    assert observed["count"] == 28
    activities = _activities(result.stdout)
    assert activities[COMPONENT_BRIDGE] is ServiceActivity.CRASH_LOOPING
    assert activities[COMPONENT_WIFI_WATCHDOG] is ServiceActivity.RUNNING


def test_normal_start_is_accepted(tmp_path: Path) -> None:
    plan = {
        "rounds": _rounds(
            ("activating|start|0", _RUNNING, _RUNNING),
            ("activating|start|0", _RUNNING, _RUNNING),
            (_RUNNING, _RUNNING, _RUNNING),
        )
    }

    result, _ = _run_sampler(tmp_path, _sampling(maximum=3), plan)

    assert result.returncode == 0, result.stderr
    assert set(_activities(result.stdout).values()) == {ServiceActivity.RUNNING}


def test_transient_failed_then_auto_restart_is_refused(tmp_path: Path) -> None:
    plan = {
        "rounds": _rounds(
            ("failed|failed|0", _RUNNING, _RUNNING),
            ("activating|auto-restart|1", _RUNNING, _RUNNING),
            ("active|running|1", _RUNNING, _RUNNING),
        )
    }

    result, _ = _run_sampler(tmp_path, _sampling(maximum=3), plan)

    assert result.returncode == 0, result.stderr
    assert _activities(result.stdout)[COMPONENT_BRIDGE] is ServiceActivity.CRASH_LOOPING


def test_two_quiet_rounds_stop_early(tmp_path: Path) -> None:
    result, observed = _run_sampler(tmp_path, _sampling(), {"rounds": _steady(_STOPPED)})

    assert result.returncode == 0, result.stderr
    assert observed["count"] == 2
    assert set(_activities(result.stdout).values()) == {ServiceActivity.STOPPED}


def test_quiet_rounds_must_be_consecutive(tmp_path: Path) -> None:
    plan = {
        "rounds": _rounds(
            (_STOPPED, _STOPPED, _STOPPED),
            (_RUNNING, _STOPPED, _STOPPED),
            ("failed|failed|0", _STOPPED, _STOPPED),
            (_STOPPED, _STOPPED, _STOPPED),
        )
    }

    result, observed = _run_sampler(tmp_path, _sampling(), plan)

    assert result.returncode == 0, result.stderr
    assert observed["count"] == 4


def test_properties_are_accepted_in_any_order(tmp_path: Path) -> None:
    plan = {"rounds": _steady(_RUNNING), "order": ["NRestarts", "ActiveState", "SubState"]}

    result, _ = _run_sampler(tmp_path, _sampling(maximum=2), plan)

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["samples"][COMPONENT_BRIDGE] == [_RUNNING, _RUNNING]


def test_unparseable_restart_counter_is_reported_unknown(tmp_path: Path) -> None:
    plan = {"rounds": _steady("active|running|[not set]")}

    result, _ = _run_sampler(tmp_path, _sampling(maximum=2), plan)

    assert result.returncode == 0, result.stderr
    samples = json.loads(result.stdout)["samples"]
    assert samples[COMPONENT_BRIDGE] == ["active|running|", "active|running|"]
    assert set(_activities(result.stdout).values()) == {ServiceActivity.RUNNING}


_BLOCK = "ActiveState=active\nSubState=running\nNRestarts=0\n"


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "Failed to get properties: Connection refused\n",
        _BLOCK + "\n" + _BLOCK,
        _BLOCK * 3,
        "\n".join([_BLOCK] * 4),
        "\n".join([_BLOCK, _BLOCK, _BLOCK + "garbage\n"]),
        "\n".join([_BLOCK, _BLOCK, _BLOCK + "SubState=running\n"]),
        "\n".join([_BLOCK, _BLOCK, _BLOCK.replace("NRestarts", "Restarts")]),
        "\n".join([_BLOCK, _BLOCK, _BLOCK + "Foo=bar\n"]),
        "\n".join([_BLOCK, _BLOCK, _BLOCK.replace("=active", "=bogus")]),
        "\n".join([_BLOCK, _BLOCK, _BLOCK.replace("=running", "=Running State")]),
        "\n".join([_BLOCK, _BLOCK, _BLOCK]).removesuffix("\n"),
    ],
    ids=[
        "empty",
        "error_text",
        "short",
        "unseparated",
        "extra_block",
        "garbage_line",
        "duplicate_key",
        "missing_key",
        "unknown_key",
        "unknown_active_state",
        "invalid_sub_state",
        "no_trailing_newline",
    ],
)
def test_malformed_show_output_fails_closed(tmp_path: Path, raw: str) -> None:
    result, _ = _run_sampler(tmp_path, _sampling(), {"raw": raw})

    assert result.returncode == 45
    assert result.stdout == ""


def test_failed_show_call_fails_closed(tmp_path: Path) -> None:
    raw = "\n".join([_BLOCK] * 3)
    result, _ = _run_sampler(tmp_path, _sampling(), {"raw": raw, "exit": 1})

    assert result.returncode == 45


def test_passed_deadline_fails_closed(tmp_path: Path) -> None:
    sampling = _sampling()
    object.__setattr__(sampling, "deadline_seconds", 0)

    result, observed = _run_sampler(tmp_path, sampling, {"rounds": _steady(_RUNNING)})

    assert result.returncode == 45
    assert observed.get("count", 0) == 0


def test_systemctl_timeout_fails_closed(tmp_path: Path) -> None:
    sampling = _sampling(call_timeout=0.5)
    plan = {"rounds": _steady(_RUNNING), "show_delays": [5.0]}

    result, _ = _run_sampler(tmp_path, sampling, plan)

    assert result.returncode == 45


def test_round_cap_bounds_the_rounds(tmp_path: Path) -> None:
    result, observed = _run_sampler(tmp_path, _sampling(maximum=7), {"rounds": _steady(_RUNNING)})

    assert result.returncode == 0, result.stderr
    assert observed["count"] == 7
    samples = json.loads(result.stdout)["samples"]
    assert all(len(values) == 7 for values in samples.values())


def test_slow_enabled_reads_do_not_shrink_the_window(tmp_path: Path) -> None:
    sampling = _sampling(window=1.0, interval=0.1, deadline=10.0, maximum=32)
    plan = {"rounds": _steady(_RUNNING), "enabled_delay": 0.5}

    started = time.monotonic()
    result, observed = _run_sampler(tmp_path, sampling, plan)

    assert result.returncode == 0, result.stderr
    times = observed["times"]
    assert isinstance(times, list)
    # The enabled reads alone took 1.5 s (> window), yet a full window was sampled.
    assert times[0] - started >= 1.5
    assert observed["count"] >= 4
    assert times[-1] - times[0] >= 0.5


def test_round_gap_over_four_seconds_fails_closed(tmp_path: Path) -> None:
    sampling = _sampling(call_timeout=6.0)
    plan = {"rounds": _steady(_RUNNING), "show_delays": [0, 4.2]}

    result, observed = _run_sampler(tmp_path, sampling, plan)

    assert result.returncode == 45
    assert observed["count"] == 2


# --- integration-side parser and classifier ------------------------------------


def _samples(*raw: str) -> tuple[ServiceSample, ...]:
    parsed = tuple(ServiceSample.parse(value) for value in raw)
    complete = tuple(sample for sample in parsed if sample is not None)
    assert len(complete) == len(parsed)
    return complete


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ((_RUNNING,), ServiceActivity.RUNNING),
        (("reloading|reload|0",), ServiceActivity.RUNNING),
        ((_STOPPED,), ServiceActivity.STOPPED),
        (("failed|failed|3",), ServiceActivity.STOPPED),
        (("activating|start|0", _RUNNING), ServiceActivity.RUNNING),
        ((_RUNNING, "deactivating|stop-sigterm|0", _STOPPED), ServiceActivity.STOPPED),
        (("activating|auto-restart|4", _RUNNING), ServiceActivity.CRASH_LOOPING),
        ((_RUNNING, "activating|auto-restart-queued|4"), ServiceActivity.CRASH_LOOPING),
        (("failed|auto-restart|1", _STOPPED), ServiceActivity.CRASH_LOOPING),
        (("active|running|3", "active|running|5"), ServiceActivity.CRASH_LOOPING),
        (("active|running|3", "active|running|4"), ServiceActivity.RUNNING),
        (("active|running|3", "active|running|", "active|running|9"), ServiceActivity.RUNNING),
        (("active|running|", "active|running|"), ServiceActivity.RUNNING),
    ],
)
def test_classifier_table(raw: tuple[str, ...], expected: ServiceActivity) -> None:
    assert classify_service_samples(_samples(*raw)) is expected


@pytest.mark.parametrize(
    "raw",
    [
        (_RUNNING, "activating|start|0"),
        (_RUNNING, "deactivating|stop|0"),
        (_RUNNING, "unknown||0"),
    ],
)
def test_classifier_refuses_unsettled_final_state(raw: tuple[str, ...]) -> None:
    with pytest.raises(panel_ops.PanelOpError, match="^snapshot_probe_failed$"):
        classify_service_samples(_samples(*raw))


def _valid_samples() -> dict[str, object]:
    return {component: [_RUNNING, _RUNNING] for component in _COMPONENTS}


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: [],
        lambda value: {**value, "extra": [_RUNNING, _RUNNING]},
        lambda value: {k: v for k, v in value.items() if k != COMPONENT_BRIDGE},
        lambda value: {**value, COMPONENT_BRIDGE: []},
        lambda value: {**value, COMPONENT_BRIDGE: [_RUNNING] * 33},
        lambda value: {**value, COMPONENT_BRIDGE: [_RUNNING]},
        lambda value: {**value, COMPONENT_BRIDGE: (_RUNNING, _RUNNING)},
        lambda value: {**value, COMPONENT_BRIDGE: [_RUNNING, 1]},
        lambda value: {**value, COMPONENT_BRIDGE: [_RUNNING, "bogus|running|0"]},
        lambda value: {**value, COMPONENT_BRIDGE: [_RUNNING, "active|running"]},
        lambda value: {**value, COMPONENT_BRIDGE: [_RUNNING, "active|Running|0"]},
        lambda value: {**value, COMPONENT_BRIDGE: [_RUNNING, "active|running|-1"]},
        lambda value: {**value, COMPONENT_BRIDGE: [_RUNNING, "active|running|0|0"]},
        lambda value: {**value, COMPONENT_BRIDGE: [_RUNNING, "active|" + "a" * 33 + "|0"]},
        lambda value: {**value, COMPONENT_BRIDGE: [_RUNNING, "active|running|" + "1" * 13]},
    ],
)
def test_parser_rejects_malformed_sample_shapes(mutate: object) -> None:
    assert callable(mutate)
    assert panel_ops._parse_service_samples(mutate(_valid_samples())) is None


def test_parser_accepts_strict_rounds() -> None:
    parsed = panel_ops._parse_service_samples(_valid_samples())
    assert parsed is not None
    assert parsed[COMPONENT_BRIDGE] == (
        ServiceSample("active", "running", 0),
        ServiceSample("active", "running", 0),
    )
    assert replace(parsed[COMPONENT_BRIDGE][0], restarts=None) == ServiceSample(
        "active", "running", None
    )
