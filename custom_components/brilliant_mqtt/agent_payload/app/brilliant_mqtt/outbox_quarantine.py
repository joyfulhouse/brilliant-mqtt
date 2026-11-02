"""Durable poison-frame quarantine sidecar for the scene/mode outbox (#113).

The outbox state file is never given new fields for this: quarantine evidence
lives in a small sidecar next to it, so a canary rollback to an older agent
ignores the sidecar and simply retries instead of rejecting its state file.

Everything here is pure data plus two filesystem helpers. The bridge decides
WHEN a failure is attributable (observed broker behaviour only, see
``scene_bridge``); this module records it and applies the fixed quarantine
threshold. A corrupt or unreadable sidecar always fails safe toward "not
quarantined", which means the bridge keeps retrying exactly as before #113.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Literal, cast

from brilliant_mqtt.ha_control_protocol import encode_json
from brilliant_mqtt.scene_state import StateKey, atomic_write_private_file

logger = logging.getLogger(__name__)

SIDECAR_VERSION = 1
MAX_SIDECAR_BYTES = 64 * 1024
# A connection drop counts against the in-flight head record only when it is
# observed within this window after the record was handed to the broker.
ATTRIBUTION_WINDOW_MS = 2_000
# Quarantine needs this many attributable failures, each in a different MQTT
# session, spanning at least QUARANTINE_SPAN_MS of wall-clock time.
QUARANTINE_FAILURES = 3
QUARANTINE_SPAN_MS = 60_000
FAILURE_HISTORY_LIMIT = 8
SUSPECT_LIMIT = 64
_SESSION_LIMIT = 64

RecordType = Literal["event", "result"]
_EVENT_PREFIX = "event:"
_RESULT_PREFIX = "result:"


@dataclass(frozen=True, slots=True)
class AttributedFailure:
    """One connection drop attributed to a record in one MQTT session."""

    session: str
    at_ms: int


@dataclass(frozen=True, slots=True)
class SuspectRecord:
    """Failure evidence for one outbox record, bound to its exact payload hash."""

    sha256: str
    failures: tuple[AttributedFailure, ...] = ()
    quarantined_at_ms: int | None = None


@dataclass(frozen=True, slots=True)
class BlockedMarker:
    """A head-record drop in a session that never proved itself healthy."""

    record: str
    sha256: str
    at_ms: int


@dataclass(frozen=True, slots=True)
class QuarantineState:
    """Immutable sidecar snapshot: per-record evidence plus the blocked marker."""

    suspects: tuple[tuple[str, SuspectRecord], ...] = ()
    blocked: BlockedMarker | None = None

    def get(self, record: str) -> SuspectRecord | None:
        """Return the evidence for *record*, if any."""
        return dict(self.suspects).get(record)


def event_record_key(event_key: str) -> str:
    """Return the namespaced quarantine key for one outbox event."""
    return f"{_EVENT_PREFIX}{event_key}"


def result_record_key(kind: str, command_id: str) -> str:
    """Return the namespaced quarantine key for one outbox command result."""
    return f"{_RESULT_PREFIX}{kind}:{command_id}"


ParsedRecordKey = tuple[Literal["event"], str] | tuple[Literal["result"], StateKey]


def parse_record_key(record: str) -> ParsedRecordKey:
    """Split a namespaced key into its outbox map and native key, or raise ValueError."""
    if record.startswith(_EVENT_PREFIX) and len(record) > len(_EVENT_PREFIX):
        return ("event", record[len(_EVENT_PREFIX) :])
    if record.startswith(_RESULT_PREFIX):
        kind, separator, command_id = record[len(_RESULT_PREFIX) :].partition(":")
        if separator and command_id and kind in ("scene", "mode"):
            return ("result", (cast(Literal["scene", "mode"], kind), command_id))
    raise ValueError("invalid outbox record key")


def payload_sha256(payload: str) -> str:
    """Return the hex SHA-256 of the exact UTF-8 payload offered to the broker."""
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def sidecar_path(state_path: Path) -> Path:
    """Return the quarantine sidecar that belongs to one outbox state file."""
    return state_path.with_name(f"{state_path.name}.quarantine.json")


def _qualifies(failures: tuple[AttributedFailure, ...]) -> bool:
    sessions = {failure.session for failure in failures}
    if len(sessions) < QUARANTINE_FAILURES:
        return False
    times = [failure.at_ms for failure in failures]
    return max(times) - min(times) >= QUARANTINE_SPAN_MS


def _with_suspect(state: QuarantineState, record: str, suspect: SuspectRecord) -> QuarantineState:
    suspects = dict(state.suspects)
    suspects[record] = suspect
    return replace(state, suspects=tuple(sorted(suspects.items())))


def record_attributed_failure(
    state: QuarantineState,
    record: str,
    sha256: str,
    session: str,
    at_ms: int,
) -> QuarantineState:
    """Count one attributable failure; quarantine once the threshold is met."""
    existing = state.get(record)
    if existing is not None and existing.sha256 == sha256:
        if existing.quarantined_at_ms is not None:
            return state
        if any(failure.session == session for failure in existing.failures):
            return state
        failures = existing.failures
    else:
        failures = ()
    failures = (*failures, AttributedFailure(session, at_ms))[-FAILURE_HISTORY_LIMIT:]
    suspect = SuspectRecord(sha256, failures, at_ms if _qualifies(failures) else None)
    updated = _with_suspect(state, record, suspect)
    if updated.blocked is not None and updated.blocked.record == record:
        updated = replace(updated, blocked=None)
    return updated


def mark_blocked(state: QuarantineState, record: str, sha256: str, at_ms: int) -> QuarantineState:
    """Record that the head record dropped in a never-healthy session (counts nothing)."""
    return replace(state, blocked=BlockedMarker(record, sha256, at_ms))


def clear_record(state: QuarantineState, record: str) -> QuarantineState:
    """Forget every piece of evidence about *record* (delivery or operator release)."""
    suspects = tuple((key, value) for key, value in state.suspects if key != record)
    blocked = state.blocked
    if blocked is not None and blocked.record == record:
        blocked = None
    return QuarantineState(suspects, blocked)


def retain_live(state: QuarantineState, live: Mapping[str, str]) -> QuarantineState:
    """Keep only evidence for undelivered records whose payload hash still matches."""
    suspects = tuple((key, value) for key, value in state.suspects if live.get(key) == value.sha256)
    blocked = state.blocked
    if blocked is not None and live.get(blocked.record) != blocked.sha256:
        blocked = None
    return QuarantineState(suspects, blocked)


def active_quarantine(
    state: QuarantineState, live: Mapping[str, str]
) -> list[tuple[str, SuspectRecord]]:
    """Return quarantined records that still exist undelivered with the same payload."""
    return [
        (key, value)
        for key, value in state.suspects
        if value.quarantined_at_ms is not None and live.get(key) == value.sha256
    ]


def sidecar_payload(state: QuarantineState) -> dict[str, object]:
    """Serialize a snapshot into the versioned sidecar JSON shape."""
    return {
        "version": SIDECAR_VERSION,
        "suspects": {
            key: {
                "sha256": value.sha256,
                "failures": [
                    {"session": failure.session, "at_ms": failure.at_ms}
                    for failure in value.failures
                ],
                "quarantined_at_ms": value.quarantined_at_ms,
            }
            for key, value in state.suspects
        },
        "blocked": (
            None
            if state.blocked is None
            else {
                "record": state.blocked.record,
                "sha256": state.blocked.sha256,
                "at_ms": state.blocked.at_ms,
            }
        ),
    }


def write_sidecar(path: Path, state: QuarantineState) -> None:
    """Validate and atomically replace the sidecar exactly like the state file."""
    validated = parse_sidecar(sidecar_payload(state))
    encoded = encode_json(sidecar_payload(validated)).encode("utf-8")
    if len(encoded) > MAX_SIDECAR_BYTES:
        raise OSError("quarantine sidecar exceeds byte limit")
    atomic_write_private_file(path, encoded)


def load_sidecar(path: Path) -> QuarantineState:
    """Load the sidecar; absent is empty, anything invalid is empty plus a warning."""
    try:
        with open(path, "rb") as handle:
            if os.fstat(handle.fileno()).st_size > MAX_SIDECAR_BYTES:
                raise ValueError("sidecar exceeds byte limit")
            content = handle.read(MAX_SIDECAR_BYTES + 1)
        if len(content) > MAX_SIDECAR_BYTES:
            raise ValueError("sidecar exceeds byte limit")
        return parse_sidecar(json.loads(content.decode("utf-8")))
    except FileNotFoundError:
        return QuarantineState()
    except (OSError, ValueError) as error:
        # UnicodeError and JSONDecodeError are ValueError subclasses.
        logger.warning(
            "outbox quarantine sidecar %s is unreadable (%s); treating every record "
            "as not quarantined and retrying",
            path,
            type(error).__name__,
        )
        return QuarantineState()


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _is_ms(value: object) -> bool:
    return type(value) is int and value >= 0


def _parse_failure(raw: object) -> AttributedFailure:
    if not isinstance(raw, dict) or set(raw) != {"session", "at_ms"}:
        raise ValueError("invalid failure entry")
    session, at_ms = raw["session"], raw["at_ms"]
    if not isinstance(session, str) or not 0 < len(session) <= _SESSION_LIMIT:
        raise ValueError("invalid failure session")
    if not _is_ms(at_ms):
        raise ValueError("invalid failure time")
    return AttributedFailure(session, cast(int, at_ms))


def _parse_suspect(raw: object) -> SuspectRecord:
    if not isinstance(raw, dict) or set(raw) != {"sha256", "failures", "quarantined_at_ms"}:
        raise ValueError("invalid suspect entry")
    sha256, failures_raw, quarantined_at_ms = (
        raw["sha256"],
        raw["failures"],
        raw["quarantined_at_ms"],
    )
    if not _is_sha256(sha256):
        raise ValueError("invalid suspect hash")
    if not isinstance(failures_raw, list) or len(failures_raw) > FAILURE_HISTORY_LIMIT:
        raise ValueError("invalid suspect failures")
    if quarantined_at_ms is not None and not _is_ms(quarantined_at_ms):
        raise ValueError("invalid quarantine time")
    failures = tuple(_parse_failure(item) for item in failures_raw)
    return SuspectRecord(cast(str, sha256), failures, cast("int | None", quarantined_at_ms))


def parse_sidecar(raw: object) -> QuarantineState:
    """Strictly validate one decoded sidecar document, or raise ValueError."""
    if not isinstance(raw, dict) or set(raw) != {"version", "suspects", "blocked"}:
        raise ValueError("invalid sidecar schema")
    if type(raw["version"]) is not int or raw["version"] != SIDECAR_VERSION:
        raise ValueError("invalid sidecar version")
    suspects_raw, blocked_raw = raw["suspects"], raw["blocked"]
    if not isinstance(suspects_raw, dict) or len(suspects_raw) > SUSPECT_LIMIT:
        raise ValueError("invalid sidecar suspects")
    suspects: list[tuple[str, SuspectRecord]] = []
    for key, value in suspects_raw.items():
        if not isinstance(key, str):
            raise ValueError("invalid suspect key")
        parse_record_key(key)
        suspects.append((key, _parse_suspect(value)))
    blocked: BlockedMarker | None = None
    if blocked_raw is not None:
        if not isinstance(blocked_raw, dict) or set(blocked_raw) != {"record", "sha256", "at_ms"}:
            raise ValueError("invalid blocked marker")
        record, sha256, at_ms = blocked_raw["record"], blocked_raw["sha256"], blocked_raw["at_ms"]
        if not isinstance(record, str) or not _is_sha256(sha256) or not _is_ms(at_ms):
            raise ValueError("invalid blocked marker value")
        parse_record_key(record)
        blocked = BlockedMarker(record, cast(str, sha256), cast(int, at_ms))
    return QuarantineState(tuple(sorted(suspects)), blocked)
