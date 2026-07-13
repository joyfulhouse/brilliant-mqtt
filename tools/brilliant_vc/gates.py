"""Ordered, secret-free evidence ledger for Virtual Control feasibility gates."""

from __future__ import annotations

import json
import os
import re
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import cast


class GateName(str, Enum):
    """Ordered Virtual Control feasibility gates."""

    VC0 = "VC0"
    VC1 = "VC1"
    VC2 = "VC2"
    VC3 = "VC3"
    VC4 = "VC4"
    VC5 = "VC5"


class GateStatus(str, Enum):
    """Terminal and initial states for a gate."""

    NOT_RUN = "not_run"
    PASS = "pass"
    FAIL = "fail"
    BLOCKED = "blocked"


GATE_ORDER = tuple(GateName)

_SAFE_KIND = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_SAFE_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_SECRET_KIND = re.compile(
    r"(?:^|_)(?:token|password|secret|certificate|private_key|pkcs12|jwt|credential)(?:_|$)"
)
_JWT_SHAPE = re.compile(r"(?:^|\s)[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}(?:$|\s)")
_PEM_MARKER = re.compile(r"-----BEGIN [A-Z0-9 ]+-----")
_LONG_BASE64 = re.compile(r"^[A-Za-z0-9_+/=-]{257,}$")


class GateProgressionError(ValueError):
    """Raised when a gate is recorded out of order or rewritten."""


class UnsafeEvidenceError(ValueError):
    """Raised when evidence could disclose a credential or sensitive path."""


def _assert_safe_text(value: str, *, field: str, reject_absolute: bool) -> None:
    if _JWT_SHAPE.search(value):
        raise UnsafeEvidenceError(f"{field} contains a JWT-shaped value")
    if _PEM_MARKER.search(value):
        raise UnsafeEvidenceError(f"{field} contains a PEM marker")
    if _LONG_BASE64.fullmatch(value):
        raise UnsafeEvidenceError(f"{field} contains a long base64-shaped value")
    if reject_absolute and Path(value).is_absolute():
        raise UnsafeEvidenceError(f"{field} contains an absolute path")


@dataclass(frozen=True, slots=True)
class Evidence:
    """One sanitized fact or relative artifact reference."""

    kind: str
    value: str | int | float | bool
    sha256: str | None = None

    def __post_init__(self) -> None:
        if not _SAFE_KIND.fullmatch(self.kind):
            raise UnsafeEvidenceError("evidence kind must be lower snake case")
        if _SECRET_KIND.search(self.kind):
            raise UnsafeEvidenceError("evidence kind is credential-shaped")
        if isinstance(self.value, str):
            _assert_safe_text(self.value, field="evidence", reject_absolute=True)
        if self.sha256 is not None and not _SAFE_SHA256.fullmatch(self.sha256):
            raise UnsafeEvidenceError("sha256 must be 64 lowercase hexadecimal characters")


@dataclass(frozen=True, slots=True)
class GateRecord:
    """Immutable record for one gate outcome."""

    gate: GateName
    status: GateStatus
    recorded_at: str
    summary: str
    evidence: tuple[Evidence, ...]


class GateLedger:
    """An ordered gate ledger that fails closed on unsafe or invalid state."""

    SCHEMA_VERSION = 1

    def __init__(self, *, run_id: str, records: Sequence[GateRecord] = ()) -> None:
        if not _RUN_ID.fullmatch(run_id):
            raise ValueError("run_id must contain only safe identifier characters")
        self.run_id = run_id
        self._records: dict[GateName, GateRecord] = {}
        for record in records:
            if record.gate in self._records:
                raise ValueError(f"duplicate gate record: {record.gate.value}")
            self._records[record.gate] = record
        self._validate_loaded_progression()

    @classmethod
    def new(cls, *, run_id: str) -> GateLedger:
        """Create an empty ledger with every gate implicitly not run."""

        return cls(run_id=run_id)

    def status(self, gate: GateName) -> GateStatus:
        """Return a gate's status, including implicit initial state."""

        record = self._records.get(gate)
        return record.status if record is not None else GateStatus.NOT_RUN

    def record(
        self,
        gate: GateName,
        status: GateStatus,
        summary: str,
        evidence: Sequence[Evidence],
    ) -> None:
        """Record one outcome after proving every earlier gate passed."""

        existing = self._records.get(gate)
        if existing is not None:
            if existing.status is GateStatus.PASS:
                raise GateProgressionError(f"{gate.value} pass is immutable")
            raise GateProgressionError(f"{gate.value} has already been recorded")
        for prerequisite in GATE_ORDER[: GATE_ORDER.index(gate)]:
            if self.status(prerequisite) is not GateStatus.PASS:
                raise GateProgressionError(f"{prerequisite.value} must pass before {gate.value}")
        if not summary or len(summary) > 500:
            raise UnsafeEvidenceError("summary must contain 1 to 500 characters")
        _assert_safe_text(summary, field="summary", reject_absolute=False)
        self._records[gate] = GateRecord(
            gate=gate,
            status=status,
            recorded_at=_utc_now(),
            summary=summary,
            evidence=tuple(evidence),
        )

    def save(self, path: Path) -> None:
        """Validate and atomically persist the sanitized ledger."""

        payload = self._to_payload()
        self._validate_payload(payload)
        path.parent.mkdir(parents=True, exist_ok=True)
        serialized = json.dumps(payload, indent=2, sort_keys=True) + "\n"
        temp_name: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=path.parent,
                prefix=f".{path.name}.",
                suffix=".tmp",
                delete=False,
            ) as handle:
                temp_name = handle.name
                handle.write(serialized)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_name, path)
        finally:
            if temp_name is not None:
                Path(temp_name).unlink(missing_ok=True)

    @classmethod
    def load(cls, path: Path) -> GateLedger:
        """Load and fully validate a ledger from disk."""

        payload: object = json.loads(path.read_text(encoding="utf-8"))
        cls._validate_payload(payload)
        data = cast(dict[str, object], payload)
        raw_records = cast(list[object], data["records"])
        records = tuple(cls._record_from_payload(item) for item in raw_records)
        return cls(run_id=cast(str, data["run_id"]), records=records)

    def _to_payload(self) -> dict[str, object]:
        records: list[dict[str, object]] = []
        for gate in GATE_ORDER:
            record = self._records.get(gate)
            if record is None:
                continue
            evidence: list[dict[str, object]] = []
            for item in record.evidence:
                serialized: dict[str, object] = {
                    "kind": item.kind,
                    "value": item.value,
                }
                if item.sha256 is not None:
                    serialized["sha256"] = item.sha256
                evidence.append(serialized)
            records.append(
                {
                    "gate": record.gate.value,
                    "status": record.status.value,
                    "recorded_at": record.recorded_at,
                    "summary": record.summary,
                    "evidence": evidence,
                }
            )
        return {
            "schema_version": self.SCHEMA_VERSION,
            "run_id": self.run_id,
            "records": records,
        }

    @classmethod
    def _record_from_payload(cls, payload: object) -> GateRecord:
        if not isinstance(payload, dict):
            raise ValueError("gate record must be an object")
        data = cast(dict[str, object], payload)
        required = {"gate", "status", "recorded_at", "summary", "evidence"}
        if set(data) != required:
            raise ValueError("gate record fields do not match schema")
        raw_evidence = data["evidence"]
        if not isinstance(raw_evidence, list):
            raise ValueError("evidence must be a list")
        return GateRecord(
            gate=GateName(_require_str(data["gate"], "gate")),
            status=GateStatus(_require_str(data["status"], "status")),
            recorded_at=_require_str(data["recorded_at"], "recorded_at"),
            summary=_safe_summary(data["summary"]),
            evidence=tuple(_evidence_from_payload(item) for item in raw_evidence),
        )

    @classmethod
    def _validate_payload(cls, payload: object) -> None:
        if not isinstance(payload, dict):
            raise ValueError("ledger must be an object")
        data = cast(dict[str, object], payload)
        if set(data) != {"schema_version", "run_id", "records"}:
            raise ValueError("ledger fields do not match schema")
        if data["schema_version"] != cls.SCHEMA_VERSION:
            raise ValueError("unsupported schema_version")
        run_id = _require_str(data["run_id"], "run_id")
        if not _RUN_ID.fullmatch(run_id):
            raise ValueError("run_id must contain only safe identifier characters")
        if not isinstance(data["records"], list):
            raise ValueError("records must be a list")

    def _validate_loaded_progression(self) -> None:
        for gate, record in self._records.items():
            for prerequisite in GATE_ORDER[: GATE_ORDER.index(gate)]:
                prerequisite_record = self._records.get(prerequisite)
                if prerequisite_record is None or prerequisite_record.status is not GateStatus.PASS:
                    raise GateProgressionError(
                        f"{prerequisite.value} must pass before {gate.value}"
                    )
            if record.status is GateStatus.NOT_RUN:
                raise GateProgressionError("not_run is implicit and must not be recorded")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _require_str(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    return value


def _safe_summary(value: object) -> str:
    summary = _require_str(value, "summary")
    if not summary or len(summary) > 500:
        raise UnsafeEvidenceError("summary must contain 1 to 500 characters")
    _assert_safe_text(summary, field="summary", reject_absolute=False)
    return summary


def _evidence_from_payload(payload: object) -> Evidence:
    if not isinstance(payload, dict):
        raise ValueError("evidence item must be an object")
    data = cast(dict[str, object], payload)
    if not {"kind", "value"} <= set(data) or not set(data) <= {"kind", "value", "sha256"}:
        raise ValueError("evidence fields do not match schema")
    value = data["value"]
    if not isinstance(value, (str, int, float, bool)):
        raise ValueError("evidence value must be scalar")
    raw_sha = data.get("sha256")
    if raw_sha is not None and not isinstance(raw_sha, str):
        raise ValueError("sha256 must be a string")
    return Evidence(
        kind=_require_str(data["kind"], "kind"),
        value=value,
        sha256=raw_sha,
    )
