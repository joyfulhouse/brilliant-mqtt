"""Durable pending-reload state for the Hue CA oneshot.

Each timer tick is a *fresh* oneshot process (see run.py) — nothing survives in
memory between runs. So when a run appends the CA and owes the coordinator a
reload, that owed-reload has to be recorded on disk, or a crash / a failed
`coordinator.restart()` between "append the cert" and "confirm the reload" would
be invisible to the next run (the cert is already present, so the naive check
short-circuits and the stale TLS trust is served forever — issue #96).

The marker is keyed to the exact (bundle_path, fingerprint) *generation* being
reconciled, so a leftover marker for an older CA can never misfire against a
newer one. A missing OR unreadable/corrupt state file is deliberately treated as
"no pending reload": the worst case is one wasted reconcile pass, never a crash
of the oneshot. Stdlib-only so it runs on the panel's Python 3.10."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass

from .fs import FileSystem


@dataclass(frozen=True)
class PendingReload:
    bundle_path: str
    fingerprint: str
    last_attempt_at: float


def load_pending(fs: FileSystem, state_path: str) -> PendingReload | None:
    """Return the persisted pending-reload marker, or None when none is owed.

    Anything that isn't a well-formed marker — missing file, invalid JSON, a
    non-object, or missing/mis-typed fields — is reported as "no pending"
    rather than raised, so a garbled state file can never crash the oneshot."""
    if not fs.exists(state_path):
        return None
    try:
        raw = json.loads(fs.read_text(state_path))
        return PendingReload(
            bundle_path=str(raw["bundle_path"]),
            fingerprint=str(raw["fingerprint"]),
            last_attempt_at=float(raw["last_attempt_at"]),
        )
    except (ValueError, TypeError, KeyError):
        return None


def save_pending(fs: FileSystem, state_path: str, pending: PendingReload) -> None:
    """Overwrite the state file with the given marker (latest generation wins)."""
    fs.write_text(state_path, json.dumps(asdict(pending)))


def clear_pending(fs: FileSystem, state_path: str) -> None:
    """Mark no reload as owed. Written (not deleted) as an empty JSON object so
    only `write_text` is required of the FileSystem boundary; load_pending reads
    it back as "no pending" (no marker fields)."""
    fs.write_text(state_path, "{}")
