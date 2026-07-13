from __future__ import annotations

import json
import os
from collections.abc import Callable
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from tools.brilliant_vc.e2e_acceptance import (
    AcceptanceEvidenceError,
    analyze_evidence,
    analyze_private_evidence,
)

STABLE_ID = "d353e38a-793e-5b6f-813b-17a1c38aba96"
COMMAND_1 = "11111111-1111-4111-8111-111111111111"
COMMAND_2 = "22222222-2222-4222-8222-222222222222"


def _trial(
    *,
    gesture_at_ms: int,
    ended_at_ms: int,
    baseline_sequence: int,
    command_id: str,
    kind: str,
    value: int | None,
    resulting_sequence: int,
) -> dict[str, Any]:
    expected_on = kind != "turn_off"
    brightness = value if kind == "set_brightness" else 120
    intensity = 502 if kind == "set_brightness" else 470
    return {
        "gesture_at_ms": gesture_at_ms,
        "ended_at_ms": ended_at_ms,
        "baseline_sequence": baseline_sequence,
        "expected_kind": kind,
        "expected_value": value,
        "commands": [
            {
                "observed_at_ms": gesture_at_ms + 100,
                "issued_at_ms": gesture_at_ms + 50,
                "command_id": command_id,
                "stable_id": STABLE_ID,
                "kind": kind,
                "value": value,
                "observed_sequence": baseline_sequence,
            }
        ],
        "results": [
            {
                "observed_at_ms": gesture_at_ms + 250,
                "timestamp_ms": gesture_at_ms + 240,
                "elapsed_ms": 30,
                "command_id": command_id,
                "stable_id": STABLE_ID,
                "accepted": True,
                "resulting_sequence": resulting_sequence,
                "error": None,
            }
        ],
        "states": [
            {
                "observed_at_ms": gesture_at_ms + 300,
                "generated_at_ms": gesture_at_ms + 280,
                "stable_id": STABLE_ID,
                "sequence": resulting_sequence,
                "available": True,
                "state": "on" if expected_on else "off",
                "brightness": brightness,
            },
            {
                "observed_at_ms": gesture_at_ms + 450,
                "generated_at_ms": gesture_at_ms + 280,
                "stable_id": STABLE_ID,
                "sequence": resulting_sequence,
                "available": True,
                "state": "on" if expected_on else "off",
                "brightness": brightness,
            },
        ],
        "panel_states": [
            {
                "observed_at_ms": gesture_at_ms + 400,
                "panel": "office",
                "source_sequence": resulting_sequence,
                "on": 1 if expected_on else 0,
                "intensity": intensity,
            },
            {
                "observed_at_ms": gesture_at_ms + 500,
                "panel": "peer",
                "source_sequence": resulting_sequence,
                "on": 1 if expected_on else 0,
                "intensity": intensity,
            },
        ],
    }


def _evidence() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "stable_id": STABLE_ID,
        "panels": ["office", "peer"],
        "limits_ms": {
            "command": 500,
            "result": 1_000,
            "state": 1_500,
            "panel": 2_000,
        },
        "trials": [
            _trial(
                gesture_at_ms=1_000,
                ended_at_ms=5_000,
                baseline_sequence=7,
                command_id=COMMAND_1,
                kind="turn_on",
                value=None,
                resulting_sequence=8,
            ),
            _trial(
                gesture_at_ms=6_000,
                ended_at_ms=10_000,
                baseline_sequence=8,
                command_id=COMMAND_2,
                kind="set_brightness",
                value=128,
                resulting_sequence=9,
            ),
        ],
    }


def test_analyze_evidence_accepts_exactly_once_two_panel_convergence() -> None:
    result = analyze_evidence(_evidence())

    assert result.passed is True
    assert result.to_public_dict() == {
        "trial_count": 2,
        "panel_count": 2,
        "required_gesture_coverage": True,
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
        "failures": [],
        "passed": True,
    }


def test_duplicate_command_and_feedback_echo_fail_closed() -> None:
    evidence = _evidence()
    trial = evidence["trials"][0]
    duplicate = deepcopy(trial["commands"][0])
    duplicate["observed_at_ms"] = 1_600
    duplicate["command_id"] = "33333333-3333-4333-8333-333333333333"
    trial["commands"].append(duplicate)

    result = analyze_evidence(evidence)

    assert result.exactly_one_command_per_gesture is False
    assert result.no_command_echo is False
    assert result.passed is False


def test_requires_both_tap_and_brightness_trials() -> None:
    evidence = _evidence()
    evidence["trials"] = [evidence["trials"][0]]

    result = analyze_evidence(evidence)

    assert result.required_gesture_coverage is False
    assert result.passed is False


def test_requires_a_post_convergence_settle_window() -> None:
    evidence = _evidence()
    evidence["trials"][0]["ended_at_ms"] = 2_000

    result = analyze_evidence(evidence)

    assert result.no_command_echo is False
    assert result.no_state_oscillation is False
    assert result.latency_within_limits is False
    assert result.passed is False


def test_rejected_or_mismatched_result_fails_correlation_and_acceptance() -> None:
    rejected = _evidence()
    result_event = rejected["trials"][0]["results"][0]
    result_event["accepted"] = False
    result_event["error"] = "service_call_failed"
    result = analyze_evidence(rejected)
    assert result.results_accepted is False
    assert result.passed is False

    mismatched = _evidence()
    mismatched["trials"][0]["results"][0]["command_id"] = COMMAND_2
    assert analyze_evidence(mismatched).result_correlation_matches is False


def test_state_regression_or_target_oscillation_fails() -> None:
    evidence = _evidence()
    states = evidence["trials"][0]["states"]
    states[1]["sequence"] = 7
    states[1]["state"] = "off"

    result = analyze_evidence(evidence)

    assert result.no_state_oscillation is False
    assert result.sequences_advance is False
    assert result.passed is False


def test_missing_peer_panel_or_wrong_native_brightness_fails_convergence() -> None:
    missing = _evidence()
    missing["trials"][1]["panel_states"].pop()
    assert analyze_evidence(missing).panels_converge is False

    wrong = _evidence()
    wrong["trials"][1]["panel_states"][0]["intensity"] = 501
    assert analyze_evidence(wrong).panels_converge is False


def test_each_latency_limit_is_enforced() -> None:
    evidence = _evidence()
    evidence["trials"][0]["commands"][0]["observed_at_ms"] = 1_501

    result = analyze_evidence(evidence)

    assert result.latency_within_limits is False
    assert result.passed is False


@pytest.mark.parametrize(
    "mutation",
    [
        lambda value: value.update({"unexpected": True}),
        lambda value: value.update({"panels": ["office"]}),
        lambda value: value.update({"stable_id": "not-a-uuid"}),
        lambda value: value["trials"][0].update({"expected_value": 256}),
    ],
)
def test_schema_and_bounds_are_strict(
    mutation: Callable[[dict[str, Any]], None],
) -> None:
    evidence = _evidence()
    mutation(evidence)

    with pytest.raises(AcceptanceEvidenceError):
        analyze_evidence(evidence)


def test_private_evidence_reader_requires_safe_root_owner_and_modes(tmp_path: Path) -> None:
    safe_root = tmp_path / "evidence"
    safe_root.mkdir(mode=0o700)
    path = safe_root / "gesture-trials.json"
    path.write_text(json.dumps(_evidence()), encoding="utf-8")
    path.chmod(0o600)

    assert analyze_private_evidence(
        path,
        safe_root=safe_root,
        required_uid=os.getuid(),
    ).passed

    path.chmod(0o644)
    with pytest.raises(AcceptanceEvidenceError, match="mode 0600"):
        analyze_private_evidence(path, safe_root=safe_root, required_uid=os.getuid())

    link = safe_root / "link.json"
    link.symlink_to(path)
    with pytest.raises(AcceptanceEvidenceError, match="symlink"):
        analyze_private_evidence(link, safe_root=safe_root, required_uid=os.getuid())
