"""Allowlisted, operator-safe shapes for agent-update progress and errors (#219).

Everything here is exported to diagnostics and entity attributes, so every value
is a fixed token, a bounded number or a timezone-aware timestamp: never a host,
an address, an exception message or other panel-controlled text.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import datetime, timedelta

from homeassistant.util import dt as dt_util

from .connect_retry import TRANSIENT_REASONS

# Stages a retry can wait at: the pre-mutation connect and release admission.
RETRY_STAGES = frozenset({"connect", "admission"})
# Fixed retry reasons: every transient connect classification plus a lost session.
RETRY_REASONS = TRANSIENT_REASONS | {"connection_lost"}
# Stages a terminal update error can be attributed to.
ERROR_STAGES = frozenset({"payload", "connect", "admission", "baseline", "deployment"})
# Every code an agent update can record as its typed final error.
UPDATE_ERROR_CODES = frozenset(
    {
        "payload_unreadable",
        "connect_failed",
        "host_key_changed",
        "update_stopped",
        "release_candidate_changed",
        "admission_connection_lost",
        "admission_failed",
        "release_override_used_or_write_failed",
        "release_identity_blocked",
        "baseline_capture_interrupted",
        "incumbent_unhealthy",
        "incumbent_unhealthy_unverified",
        "incumbent_rehearsal_failed",
        "incumbent_rehearsal_interrupted",
        "incumbent_health_unobservable",
        "release_identity_changed",
        "provisioning_recovery_required",
        "deployment_failed",
        "baseline_capture_failed",
        "baseline_capture_invalid",
        "baseline_changed",
        "baseline_incomplete",
        "baseline_correlation_unsupported",
        "baseline_verify_failed",
        "baseline_read_failed",
        "baseline_retained_finalize_first",
        "transaction_in_progress",
        "rollback_failed",
        "rollback_health_failed",
        "rollback_deadline_exceeded",
        "health_failed",
        "recovery_connection_failed",
        "recovery_interrupted",
        "panel_identity_mismatch",
    }
)

RETRY_STATE_KEYS = frozenset(
    {"stage", "attempt", "max_attempts", "reason", "retry_in_seconds", "next_attempt_at"}
)
LAST_ERROR_KEYS = frozenset({"stage", "code", "attempts"})
INTERRUPTED_KEYS = frozenset(
    {
        "target_version",
        "stage",
        "attempts",
        "max_attempts",
        "started_at",
        "deadline",
        "expired",
    }
)
UPDATE_DIAGNOSTICS_KEYS = frozenset(
    {"in_progress", "progress", "retry", "last_error", "interrupted"}
)
_MAX_ATTEMPTS_BOUND = 100
_VERSION = re.compile(r"^[0-9A-Za-z][0-9A-Za-z._+-]{0,127}$")


def wall_clock_in(deadline: float, now: float) -> datetime:
    """Convert a monotonic *deadline* to wall-clock UTC, given monotonic *now*.

    The single conversion used for every exported "next attempt"/deadline time.
    """
    return dt_util.utcnow() + timedelta(seconds=max(0.0, deadline - now))


def _count(value: object, *, minimum: int = 0) -> bool:
    return type(value) is int and minimum <= value <= _MAX_ATTEMPTS_BOUND


def _aware_iso(value: object) -> bool:
    if not isinstance(value, str) or len(value) > 64:
        return False
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def safe_retry_state(value: object) -> dict[str, object] | None:
    """Return a copy of a valid pending-retry state, else None.

    Exact keys; ``retry_in_seconds`` and ``next_attempt_at`` are always present and
    None when unknown.
    """
    if not isinstance(value, Mapping) or frozenset(value) != RETRY_STATE_KEYS:
        return None
    delay = value["retry_in_seconds"]
    when = value["next_attempt_at"]
    if (
        value["stage"] not in RETRY_STAGES
        or value["reason"] not in RETRY_REASONS
        or not _count(value["attempt"], minimum=1)
        or not _count(value["max_attempts"], minimum=1)
        or value["attempt"] > value["max_attempts"]
        or (delay is not None and (type(delay) not in (int, float) or not 0 <= delay <= 3600))
        or (when is not None and not _aware_iso(when))
    ):
        return None
    return {key: value[key] for key in sorted(RETRY_STATE_KEYS)}


def safe_last_error(value: object) -> dict[str, object] | None:
    """Return a copy of a valid typed final update error, else None."""
    if not isinstance(value, Mapping) or frozenset(value) != LAST_ERROR_KEYS:
        return None
    if (
        value["stage"] not in ERROR_STAGES
        or value["code"] not in UPDATE_ERROR_CODES
        or not _count(value["attempts"])
    ):
        return None
    return {key: value[key] for key in sorted(LAST_ERROR_KEYS)}


def safe_interrupted(value: object) -> dict[str, object] | None:
    """Return a copy of a valid surfaced interrupted-update summary, else None."""
    if not isinstance(value, Mapping) or frozenset(value) != INTERRUPTED_KEYS:
        return None
    version = value["target_version"]
    if (
        not isinstance(version, str)
        or _VERSION.fullmatch(version) is None
        or value["stage"] not in RETRY_STAGES
        or not _count(value["attempts"])
        or not _count(value["max_attempts"], minimum=1)
        or not _aware_iso(value["started_at"])
        or not _aware_iso(value["deadline"])
        or type(value["expired"]) is not bool
    ):
        return None
    return {key: value[key] for key in sorted(INTERRUPTED_KEYS)}


def safe_update_diagnostics(value: object) -> dict[str, object] | None:
    """Re-check the whole per-panel update block; any invalid part reads as None."""
    if not isinstance(value, Mapping) or frozenset(value) != UPDATE_DIAGNOSTICS_KEYS:
        return None
    progress = value["progress"]
    in_progress = value["in_progress"]
    if type(in_progress) is not bool or (
        progress is not None and (type(progress) is not int or not 0 <= progress <= 100)
    ):
        return None
    return {
        "in_progress": in_progress,
        "progress": progress,
        "retry": safe_retry_state(value["retry"]),
        "last_error": safe_last_error(value["last_error"]),
        "interrupted": safe_interrupted(value["interrupted"]),
    }
