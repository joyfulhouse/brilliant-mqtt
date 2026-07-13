"""Offline acceptance analysis for one-light Virtual Control gesture trials.

The analyzer never connects to MQTT, Home Assistant, or a panel.  It consumes a
private transcript collected during separately approved operator gestures and
emits only aggregate pass/fail evidence.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TypeVar, cast
from uuid import UUID

from tools.brilliant_vc.single_light_pilot import brightness_to_intensity

_SCHEMA_VERSION = 1
_MAX_TIMESTAMP = 2**63 - 1
_MAX_TRIALS = 16
_MAX_EVENTS = 128
_MAX_PRIVATE_FILE_BYTES = 1024 * 1024
_MIN_SETTLE_MS = 1_000
_PANEL_LABEL = re.compile(r"^[a-z][a-z0-9_-]{0,31}$")
_KINDS = frozenset({"turn_on", "turn_off", "set_brightness"})
_EventT = TypeVar("_EventT")


class AcceptanceEvidenceError(ValueError):
    """Raised when a purported trial transcript is ambiguous or malformed."""


@dataclass(frozen=True, slots=True)
class _Limits:
    command: int
    result: int
    state: int
    panel: int


@dataclass(frozen=True, slots=True)
class _Command:
    observed_at_ms: int
    issued_at_ms: int
    command_id: str
    stable_id: str
    kind: str
    value: int | None
    observed_sequence: int


@dataclass(frozen=True, slots=True)
class _Result:
    observed_at_ms: int
    timestamp_ms: int
    elapsed_ms: int
    command_id: str
    stable_id: str
    accepted: bool
    resulting_sequence: int
    error: str | None


@dataclass(frozen=True, slots=True)
class _State:
    observed_at_ms: int
    generated_at_ms: int
    stable_id: str
    sequence: int
    available: bool
    state: str
    brightness: int | None


@dataclass(frozen=True, slots=True)
class _PanelState:
    observed_at_ms: int
    panel: str
    source_sequence: int
    on: int
    intensity: int | None


@dataclass(frozen=True, slots=True)
class _Trial:
    gesture_at_ms: int
    ended_at_ms: int
    baseline_sequence: int
    expected_kind: str
    expected_value: int | None
    commands: tuple[_Command, ...]
    results: tuple[_Result, ...]
    states: tuple[_State, ...]
    panel_states: tuple[_PanelState, ...]


@dataclass(frozen=True, slots=True)
class _Evidence:
    stable_id: str
    panels: tuple[str, str]
    limits: _Limits
    trials: tuple[_Trial, ...]


@dataclass(frozen=True, slots=True)
class AcceptanceReport:
    """Identifier-free aggregate E2E acceptance result."""

    trial_count: int
    panel_count: int
    required_gesture_coverage: bool
    exactly_one_command_per_gesture: bool
    command_mapping_matches: bool
    result_correlation_matches: bool
    results_accepted: bool
    sequences_advance: bool
    ha_state_converges: bool
    panels_converge: bool
    no_command_echo: bool
    no_state_oscillation: bool
    latency_within_limits: bool
    failures: tuple[str, ...]
    passed: bool

    def to_public_dict(self) -> dict[str, object]:
        return {
            "trial_count": self.trial_count,
            "panel_count": self.panel_count,
            "required_gesture_coverage": self.required_gesture_coverage,
            "exactly_one_command_per_gesture": self.exactly_one_command_per_gesture,
            "command_mapping_matches": self.command_mapping_matches,
            "result_correlation_matches": self.result_correlation_matches,
            "results_accepted": self.results_accepted,
            "sequences_advance": self.sequences_advance,
            "ha_state_converges": self.ha_state_converges,
            "panels_converge": self.panels_converge,
            "no_command_echo": self.no_command_echo,
            "no_state_oscillation": self.no_state_oscillation,
            "latency_within_limits": self.latency_within_limits,
            "failures": list(self.failures),
            "passed": self.passed,
        }


def analyze_evidence(payload: object) -> AcceptanceReport:
    """Validate a private transcript and evaluate every E2E acceptance gate."""

    evidence = _parse_evidence(payload)
    expected_kinds = {trial.expected_kind for trial in evidence.trials}
    required_gesture_coverage = "set_brightness" in expected_kinds and bool(
        expected_kinds & {"turn_on", "turn_off"}
    )
    checks = {
        "required_gesture_coverage": required_gesture_coverage,
        "exactly_one_command_per_gesture": True,
        "command_mapping_matches": True,
        "result_correlation_matches": True,
        "results_accepted": True,
        "sequences_advance": True,
        "ha_state_converges": True,
        "panels_converge": True,
        "no_command_echo": True,
        "no_state_oscillation": True,
        "latency_within_limits": True,
    }
    seen_command_ids: set[str] = set()
    prior_resulting_sequence: int | None = None

    for trial in evidence.trials:
        if (
            prior_resulting_sequence is not None
            and trial.baseline_sequence < prior_resulting_sequence
        ):
            checks["sequences_advance"] = False

        all_events = (
            *(event.observed_at_ms for event in trial.commands),
            *(event.observed_at_ms for event in trial.results),
            *(event.observed_at_ms for event in trial.states),
            *(event.observed_at_ms for event in trial.panel_states),
        )
        if any(
            not trial.gesture_at_ms <= timestamp <= trial.ended_at_ms for timestamp in all_events
        ):
            checks["latency_within_limits"] = False
        if not all_events or trial.ended_at_ms - max(all_events) < _MIN_SETTLE_MS:
            checks["no_command_echo"] = False
            checks["no_state_oscillation"] = False
            checks["latency_within_limits"] = False

        if len(trial.commands) != 1:
            checks["exactly_one_command_per_gesture"] = False
            checks["command_mapping_matches"] = False
            checks["result_correlation_matches"] = False
            checks["results_accepted"] = False
            checks["sequences_advance"] = False
            checks["ha_state_converges"] = False
            checks["panels_converge"] = False
            checks["no_command_echo"] = False
            checks["no_state_oscillation"] = False
            checks["latency_within_limits"] = False
            continue

        command = trial.commands[0]
        if command.command_id in seen_command_ids:
            checks["result_correlation_matches"] = False
        seen_command_ids.add(command.command_id)
        command_mapping = (
            command.stable_id == evidence.stable_id
            and command.kind == trial.expected_kind
            and command.value == trial.expected_value
            and command.observed_sequence == trial.baseline_sequence
            and trial.gesture_at_ms <= command.issued_at_ms <= command.observed_at_ms
        )
        if not command_mapping:
            checks["command_mapping_matches"] = False
        if not 0 <= command.observed_at_ms - trial.gesture_at_ms <= evidence.limits.command:
            checks["latency_within_limits"] = False

        if len(trial.results) != 1:
            checks["result_correlation_matches"] = False
            checks["results_accepted"] = False
            checks["sequences_advance"] = False
            checks["ha_state_converges"] = False
            checks["panels_converge"] = False
            checks["no_command_echo"] = False
            checks["no_state_oscillation"] = False
            checks["latency_within_limits"] = False
            continue

        result = trial.results[0]
        result_correlates = (
            result.command_id == command.command_id
            and result.stable_id == evidence.stable_id
            and result.observed_at_ms >= command.observed_at_ms
        )
        if not result_correlates:
            checks["result_correlation_matches"] = False
        if not result.accepted or result.error is not None:
            checks["results_accepted"] = False
        if not 0 <= result.observed_at_ms - command.observed_at_ms <= evidence.limits.result:
            checks["latency_within_limits"] = False

        result_advances = result.resulting_sequence > trial.baseline_sequence
        if not result_advances:
            checks["sequences_advance"] = False
        prior_resulting_sequence = result.resulting_sequence

        ordered_states = sorted(trial.states, key=lambda event: event.observed_at_ms)
        previous_sequence = trial.baseline_sequence
        values_by_sequence: dict[int, tuple[bool, str, int | None]] = {}
        state_sequence_ok = True
        for state in ordered_states:
            if state.stable_id != evidence.stable_id or state.sequence < previous_sequence:
                state_sequence_ok = False
            prior_values = values_by_sequence.get(state.sequence)
            current_values = (state.available, state.state, state.brightness)
            if prior_values is not None and prior_values != current_values:
                state_sequence_ok = False
            values_by_sequence[state.sequence] = current_values
            previous_sequence = max(previous_sequence, state.sequence)
        if not state_sequence_ok:
            checks["sequences_advance"] = False
            checks["no_state_oscillation"] = False

        expected_on = trial.expected_kind != "turn_off"
        expected_brightness = (
            trial.expected_value if trial.expected_kind == "set_brightness" else None
        )
        matching_states = [
            state
            for state in ordered_states
            if state.sequence == result.resulting_sequence
            and _state_matches(
                state,
                expected_on=expected_on,
                expected_brightness=expected_brightness,
                stable_id=evidence.stable_id,
            )
        ]
        if not matching_states:
            checks["ha_state_converges"] = False
            checks["panels_converge"] = False
            checks["no_command_echo"] = False
            checks["no_state_oscillation"] = False
            checks["sequences_advance"] = False
            checks["latency_within_limits"] = False
            continue

        target_state = matching_states[0]
        if not 0 <= target_state.observed_at_ms - command.observed_at_ms <= evidence.limits.state:
            checks["latency_within_limits"] = False
        for later_state in ordered_states:
            if later_state.observed_at_ms < target_state.observed_at_ms:
                continue
            if later_state.sequence < result.resulting_sequence or not _state_matches(
                later_state,
                expected_on=expected_on,
                expected_brightness=expected_brightness,
                stable_id=evidence.stable_id,
            ):
                checks["no_state_oscillation"] = False

        expected_intensity = (
            brightness_to_intensity(expected_brightness)
            if expected_brightness is not None
            else None
        )
        for panel in evidence.panels:
            matching_panels = [
                state
                for state in trial.panel_states
                if state.panel == panel
                and state.observed_at_ms >= target_state.observed_at_ms
                and state.source_sequence == result.resulting_sequence
                and state.on == int(expected_on)
                and (expected_intensity is None or state.intensity == expected_intensity)
            ]
            if not matching_panels:
                checks["panels_converge"] = False
                checks["no_state_oscillation"] = False
                checks["latency_within_limits"] = False
                continue
            first_panel = min(matching_panels, key=lambda state: state.observed_at_ms)
            if first_panel.observed_at_ms - target_state.observed_at_ms > evidence.limits.panel:
                checks["latency_within_limits"] = False
            for later_panel in trial.panel_states:
                if (
                    later_panel.panel == panel
                    and later_panel.observed_at_ms >= first_panel.observed_at_ms
                    and (
                        later_panel.source_sequence < result.resulting_sequence
                        or later_panel.on != int(expected_on)
                        or (
                            expected_intensity is not None
                            and later_panel.intensity != expected_intensity
                        )
                    )
                ):
                    checks["no_state_oscillation"] = False

    failures = tuple(name for name, passed in checks.items() if not passed)
    passed = not failures
    return AcceptanceReport(
        trial_count=len(evidence.trials),
        panel_count=len(evidence.panels),
        required_gesture_coverage=checks["required_gesture_coverage"],
        exactly_one_command_per_gesture=checks["exactly_one_command_per_gesture"],
        command_mapping_matches=checks["command_mapping_matches"],
        result_correlation_matches=checks["result_correlation_matches"],
        results_accepted=checks["results_accepted"],
        sequences_advance=checks["sequences_advance"],
        ha_state_converges=checks["ha_state_converges"],
        panels_converge=checks["panels_converge"],
        no_command_echo=checks["no_command_echo"],
        no_state_oscillation=checks["no_state_oscillation"],
        latency_within_limits=checks["latency_within_limits"],
        failures=failures,
        passed=passed,
    )


def analyze_private_evidence(
    path: Path,
    *,
    safe_root: Path,
    required_uid: int = 0,
) -> AcceptanceReport:
    """Read one private mode-0600 transcript and return its public analysis."""

    root = safe_root.absolute()
    target = path.absolute()
    try:
        root_metadata = root.lstat()
    except FileNotFoundError:
        raise AcceptanceEvidenceError("safe root does not exist") from None
    if stat.S_ISLNK(root_metadata.st_mode) or not stat.S_ISDIR(root_metadata.st_mode):
        raise AcceptanceEvidenceError("safe root must be a real directory")
    if root_metadata.st_uid != required_uid or stat.S_IMODE(root_metadata.st_mode) != 0o700:
        raise AcceptanceEvidenceError("safe root must have the required owner and mode 0700")
    if target.parent != root:
        raise AcceptanceEvidenceError("private evidence must be directly below the safe root")
    try:
        before = target.lstat()
    except FileNotFoundError:
        raise AcceptanceEvidenceError("private evidence does not exist") from None
    if stat.S_ISLNK(before.st_mode):
        raise AcceptanceEvidenceError("private evidence must not be a symlink")
    if not stat.S_ISREG(before.st_mode):
        raise AcceptanceEvidenceError("private evidence must be a regular file")
    if before.st_uid != required_uid or stat.S_IMODE(before.st_mode) != 0o600:
        raise AcceptanceEvidenceError("private evidence must have the required owner and mode 0600")
    if before.st_size > _MAX_PRIVATE_FILE_BYTES:
        raise AcceptanceEvidenceError("private evidence exceeds 1 MiB")

    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        descriptor = os.open(target, flags)
    except OSError:
        raise AcceptanceEvidenceError("could not safely open private evidence") from None
    raw = bytearray()
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise AcceptanceEvidenceError("private evidence changed during open")
        while True:
            chunk = os.read(
                descriptor,
                min(8192, _MAX_PRIVATE_FILE_BYTES + 1 - len(raw)),
            )
            if not chunk:
                break
            raw.extend(chunk)
            if len(raw) > _MAX_PRIVATE_FILE_BYTES:
                raise AcceptanceEvidenceError("private evidence exceeds 1 MiB")
    finally:
        os.close(descriptor)
    try:
        try:
            payload: object = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise AcceptanceEvidenceError("private evidence is not valid JSON") from None
        return analyze_evidence(payload)
    finally:
        for index in range(len(raw)):
            raw[index] = 0


def _state_matches(
    state: _State,
    *,
    expected_on: bool,
    expected_brightness: int | None,
    stable_id: str,
) -> bool:
    return (
        state.stable_id == stable_id
        and state.available
        and state.state == ("on" if expected_on else "off")
        and (expected_brightness is None or state.brightness == expected_brightness)
    )


def _parse_evidence(payload: object) -> _Evidence:
    data = _object(
        payload,
        {"schema_version", "stable_id", "panels", "limits_ms", "trials"},
        "evidence",
    )
    if data["schema_version"] != _SCHEMA_VERSION:
        raise AcceptanceEvidenceError("evidence schema version is unsupported")
    stable_id = _uuid(data["stable_id"], "stable_id")
    raw_panels = data["panels"]
    if not isinstance(raw_panels, list) or len(raw_panels) != 2:
        raise AcceptanceEvidenceError("evidence must name exactly two panels")
    panels: list[str] = []
    for raw_panel in raw_panels:
        if not isinstance(raw_panel, str) or _PANEL_LABEL.fullmatch(raw_panel) is None:
            raise AcceptanceEvidenceError("panel label is invalid")
        panels.append(raw_panel)
    if len(set(panels)) != 2:
        raise AcceptanceEvidenceError("panel labels must be distinct")

    limit_data = _object(
        data["limits_ms"],
        {"command", "result", "state", "panel"},
        "latency limits",
    )
    limits = _Limits(
        command=_integer(limit_data["command"], "command limit", minimum=1, maximum=60_000),
        result=_integer(limit_data["result"], "result limit", minimum=1, maximum=60_000),
        state=_integer(limit_data["state"], "state limit", minimum=1, maximum=60_000),
        panel=_integer(limit_data["panel"], "panel limit", minimum=1, maximum=60_000),
    )

    raw_trials = data["trials"]
    if not isinstance(raw_trials, list) or not 0 < len(raw_trials) <= _MAX_TRIALS:
        raise AcceptanceEvidenceError("trial count is outside the supported range")
    trials = tuple(_parse_trial(raw_trial, panels=tuple(panels)) for raw_trial in raw_trials)
    for previous, current in zip(trials, trials[1:], strict=False):
        if current.gesture_at_ms <= previous.ended_at_ms:
            raise AcceptanceEvidenceError("gesture trial windows overlap or are unordered")
    return _Evidence(stable_id, (panels[0], panels[1]), limits, trials)


def _parse_trial(payload: object, *, panels: tuple[str, ...]) -> _Trial:
    data = _object(
        payload,
        {
            "gesture_at_ms",
            "ended_at_ms",
            "baseline_sequence",
            "expected_kind",
            "expected_value",
            "commands",
            "results",
            "states",
            "panel_states",
        },
        "trial",
    )
    gesture = _timestamp(data["gesture_at_ms"], "gesture timestamp")
    ended = _timestamp(data["ended_at_ms"], "trial end timestamp")
    if ended <= gesture:
        raise AcceptanceEvidenceError("trial end must follow the gesture")
    baseline = _integer(data["baseline_sequence"], "baseline sequence")
    kind = data["expected_kind"]
    if not isinstance(kind, str) or kind not in _KINDS:
        raise AcceptanceEvidenceError("expected command kind is invalid")
    value = data["expected_value"]
    if kind == "set_brightness":
        value = _integer(value, "expected brightness", maximum=255)
    elif value is not None:
        raise AcceptanceEvidenceError("on/off command value must be null")

    commands = _event_list(data["commands"], _parse_command, "commands")
    results = _event_list(data["results"], _parse_result, "results")
    states = _event_list(data["states"], _parse_state, "states")
    panel_states = _event_list(data["panel_states"], _parse_panel_state, "panel states")
    if any(state.panel not in panels for state in panel_states):
        raise AcceptanceEvidenceError("panel observation names an unexpected panel")
    return _Trial(
        gesture,
        ended,
        baseline,
        kind,
        value,
        commands,
        results,
        states,
        panel_states,
    )


def _parse_command(payload: object) -> _Command:
    data = _object(
        payload,
        {
            "observed_at_ms",
            "issued_at_ms",
            "command_id",
            "stable_id",
            "kind",
            "value",
            "observed_sequence",
        },
        "command event",
    )
    kind = data["kind"]
    if not isinstance(kind, str) or kind not in _KINDS:
        raise AcceptanceEvidenceError("command event kind is invalid")
    value = data["value"]
    if kind == "set_brightness":
        value = _integer(value, "command brightness", maximum=255)
    elif value is not None:
        raise AcceptanceEvidenceError("command on/off value must be null")
    return _Command(
        _timestamp(data["observed_at_ms"], "command observation timestamp"),
        _timestamp(data["issued_at_ms"], "command issue timestamp"),
        _uuid(data["command_id"], "command_id"),
        _uuid(data["stable_id"], "command stable_id"),
        kind,
        value,
        _integer(data["observed_sequence"], "observed sequence"),
    )


def _parse_result(payload: object) -> _Result:
    data = _object(
        payload,
        {
            "observed_at_ms",
            "timestamp_ms",
            "elapsed_ms",
            "command_id",
            "stable_id",
            "accepted",
            "resulting_sequence",
            "error",
        },
        "result event",
    )
    accepted = data["accepted"]
    if type(accepted) is not bool:
        raise AcceptanceEvidenceError("result accepted flag must be a boolean")
    error = data["error"]
    if error is not None and (
        not isinstance(error, str)
        or not error
        or len(error) > 128
        or any(ord(character) < 32 for character in error)
    ):
        raise AcceptanceEvidenceError("result error is invalid")
    return _Result(
        _timestamp(data["observed_at_ms"], "result observation timestamp"),
        _timestamp(data["timestamp_ms"], "result timestamp"),
        _integer(data["elapsed_ms"], "result elapsed time", maximum=60_000),
        _uuid(data["command_id"], "result command_id"),
        _uuid(data["stable_id"], "result stable_id"),
        accepted,
        _integer(data["resulting_sequence"], "resulting sequence"),
        error,
    )


def _parse_state(payload: object) -> _State:
    data = _object(
        payload,
        {
            "observed_at_ms",
            "generated_at_ms",
            "stable_id",
            "sequence",
            "available",
            "state",
            "brightness",
        },
        "state event",
    )
    available = data["available"]
    if type(available) is not bool:
        raise AcceptanceEvidenceError("state availability must be a boolean")
    state = data["state"]
    if state not in {"on", "off", "unknown", "unavailable"}:
        raise AcceptanceEvidenceError("state value is invalid")
    brightness = data["brightness"]
    if brightness is not None:
        brightness = _integer(brightness, "state brightness", maximum=255)
    return _State(
        _timestamp(data["observed_at_ms"], "state observation timestamp"),
        _timestamp(data["generated_at_ms"], "state generation timestamp"),
        _uuid(data["stable_id"], "state stable_id"),
        _integer(data["sequence"], "state sequence"),
        available,
        state,
        brightness,
    )


def _parse_panel_state(payload: object) -> _PanelState:
    data = _object(
        payload,
        {"observed_at_ms", "panel", "source_sequence", "on", "intensity"},
        "panel state event",
    )
    panel = data["panel"]
    if not isinstance(panel, str) or _PANEL_LABEL.fullmatch(panel) is None:
        raise AcceptanceEvidenceError("panel observation label is invalid")
    intensity = data["intensity"]
    if intensity is not None:
        intensity = _integer(intensity, "panel intensity", maximum=1_000)
    return _PanelState(
        _timestamp(data["observed_at_ms"], "panel observation timestamp"),
        panel,
        _integer(data["source_sequence"], "panel source sequence"),
        _integer(data["on"], "panel on value", maximum=1),
        intensity,
    )


def _event_list(
    payload: object,
    parser: Callable[[object], _EventT],
    description: str,
) -> tuple[_EventT, ...]:
    if not isinstance(payload, list) or len(payload) > _MAX_EVENTS:
        raise AcceptanceEvidenceError(f"{description} list is invalid")
    return tuple(parser(item) for item in payload)


def _object(payload: object, fields: set[str], description: str) -> dict[str, object]:
    if not isinstance(payload, dict) or set(payload) != fields:
        raise AcceptanceEvidenceError(f"{description} fields do not match the schema")
    return cast(dict[str, object], payload)


def _integer(
    value: object,
    description: str,
    *,
    minimum: int = 0,
    maximum: int = _MAX_TIMESTAMP,
) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise AcceptanceEvidenceError(f"{description} is outside the supported range")
    return value


def _timestamp(value: object, description: str) -> int:
    return _integer(value, description)


def _uuid(value: object, description: str) -> str:
    if not isinstance(value, str):
        raise AcceptanceEvidenceError(f"{description} must be a UUID")
    try:
        normalized = str(UUID(value))
    except (AttributeError, TypeError, ValueError):
        raise AcceptanceEvidenceError(f"{description} must be a UUID") from None
    if value != normalized:
        raise AcceptanceEvidenceError(f"{description} must use canonical UUID form")
    return value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--safe-root", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args(argv)
    report = analyze_private_evidence(
        cast(Path, args.evidence),
        safe_root=cast(Path, args.safe_root),
        required_uid=os.getuid(),
    )
    print(json.dumps(report.to_public_dict(), sort_keys=True))
    return 0 if report.passed else 2


if __name__ == "__main__":
    try:
        sys.exit(main())
    except AcceptanceEvidenceError as exc:
        print(f"E2E evidence rejected: {exc}", file=sys.stderr)
        sys.exit(2)
