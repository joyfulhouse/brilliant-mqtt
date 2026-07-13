from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools.brilliant_vc.gates import (
    Evidence,
    GateLedger,
    GateName,
    GateProgressionError,
    GateStatus,
    UnsafeEvidenceError,
)


def test_cannot_pass_vc2_before_vc1() -> None:
    ledger = GateLedger.new(run_id="20260713-office")
    ledger.record(GateName.VC0, GateStatus.PASS, summary="audit passed", evidence=[])

    with pytest.raises(GateProgressionError, match="VC1 must pass before VC2"):
        ledger.record(
            GateName.VC2,
            GateStatus.PASS,
            summary="provisioned",
            evidence=[],
        )


@pytest.mark.parametrize(
    ("kind", "value"),
    [
        ("note", "eyJhbGciOiJIUzI1NiJ9.abcdefgh.signature"),
        ("note", "-----BEGIN PRIVATE KEY-----"),
        ("access_token", "redacted"),
        ("note", "/data/brilliant-vc/identity/device.p12"),
        ("note", "A" * 300),
    ],
)
def test_secret_shaped_evidence_is_rejected(kind: str, value: str) -> None:
    with pytest.raises(UnsafeEvidenceError):
        Evidence(kind=kind, value=value)


def test_safe_evidence_shapes_are_accepted() -> None:
    evidence = (
        Evidence(kind="artifact", value="run-01/vc0-audit.json"),
        Evidence(kind="count", value=0),
        Evidence(kind="duration_s", value=1.25),
        Evidence(kind="target_home_match", value=True),
        Evidence(kind="firmware_version", value="v26.06.03.1"),
        Evidence(kind="http_status", value=200),
        Evidence(kind="device_id_redacted", value="abcd…1234"),
        Evidence(kind="report", value="vc0.json", sha256="a" * 64),
    )

    assert len(evidence) == 8


def test_passed_gate_is_immutable() -> None:
    ledger = GateLedger.new(run_id="run-01")
    ledger.record(GateName.VC0, GateStatus.PASS, "passed", [])

    with pytest.raises(GateProgressionError, match="VC0 pass is immutable"):
        ledger.record(GateName.VC0, GateStatus.FAIL, "changed", [])


@pytest.mark.parametrize("status", [GateStatus.FAIL, GateStatus.BLOCKED])
def test_failed_or_blocked_gate_prevents_later_gate(status: GateStatus) -> None:
    ledger = GateLedger.new(run_id="run-01")
    ledger.record(GateName.VC0, status, "stopped", [])

    with pytest.raises(GateProgressionError, match="VC0 must pass before VC1"):
        ledger.record(GateName.VC1, GateStatus.PASS, "invalid", [])


def test_new_ledger_reports_not_run_for_every_gate() -> None:
    ledger = GateLedger.new(run_id="run-01")

    assert {gate: ledger.status(gate) for gate in GateName} == {
        gate: GateStatus.NOT_RUN for gate in GateName
    }


def test_save_load_round_trip_uses_stable_secret_free_json(tmp_path: Path) -> None:
    path = tmp_path / "gate-ledger.json"
    ledger = GateLedger.new(run_id="run-01")
    ledger.record(
        GateName.VC0,
        GateStatus.PASS,
        "No pre-existing Virtual Control",
        [Evidence(kind="device_type_6_count", value=0)],
    )

    ledger.save(path)
    loaded = GateLedger.load(path)
    payload = json.loads(path.read_text())

    assert loaded.run_id == "run-01"
    assert loaded.status(GateName.VC0) is GateStatus.PASS
    assert payload["schema_version"] == 1
    assert payload["records"][0]["gate"] == "VC0"
    assert payload["records"][0]["evidence"] == [{"kind": "device_type_6_count", "value": 0}]
    assert not list(tmp_path.glob(".*.tmp"))


def test_invalid_ledger_payload_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "gate-ledger.json"
    path.write_text('{"schema_version": 1, "run_id": "x", "records": "bad"}')

    with pytest.raises(ValueError, match="records must be a list"):
        GateLedger.load(path)


def test_committed_schema_has_exact_gate_and_status_enums() -> None:
    schema_path = (
        Path(__file__).parents[1] / "docs" / "brilliant-panel" / "virtual-control-gate-schema.json"
    )
    schema = json.loads(schema_path.read_text())
    record = schema["$defs"]["gateRecord"]["properties"]

    assert record["gate"]["enum"] == [gate.value for gate in GateName]
    assert record["status"]["enum"] == [status.value for status in GateStatus]
