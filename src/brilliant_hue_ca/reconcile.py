"""Pure reconcile: ensure our CA is in the pinned Hue bundle (match by DER
fingerprint), and restart the local Hue coordinator when the reload is owed.

The reload is owed not only right after we append the CA, but also on a *later*
run if an earlier run appended it yet never confirmed the restart — because it
raised, or the oneshot was killed in between (issue #96). To survive that, the
owed-reload is recorded to disk *before* the restart is attempted, keyed to the
exact (bundle_path, fingerprint) generation, and cleared only once a restart
actually succeeds. Retries are paced off the persisted last-attempt time so a
persistently-failing restart can't storm the coordinator.

Stdlib-only so it runs on the panel's Python 3.10 and off-panel in tests."""

from __future__ import annotations

import hashlib
import ssl
import time
from dataclasses import dataclass

from .coordinator import Coordinator
from .fs import FileSystem
from .state import PendingReload, clear_pending, load_pending, save_pending

_BEGIN = "-----BEGIN CERTIFICATE-----"
_END = "-----END CERTIFICATE-----"


@dataclass(frozen=True)
class Outcome:
    bundle_found: bool
    appended: bool
    coordinator_restarted: bool
    bundle_path: str | None
    # True when, after this pass, a coordinator reload is still owed (persisted
    # marker set): the restart failed, was killed, or was skipped for pacing.
    reload_pending: bool = False


def cert_fingerprint(pem: str) -> str:
    """SHA-256 hex of the certificate's DER encoding. Raises ssl.SSLError /
    ValueError on an unparseable PEM (callers guard where skipping is wanted)."""
    der = ssl.PEM_cert_to_DER_cert(pem)
    return hashlib.sha256(der).hexdigest()


def split_pem_certs(text: str) -> list[str]:
    certs: list[str] = []
    idx = 0
    while True:
        start = text.find(_BEGIN, idx)
        if start == -1:
            break
        end = text.find(_END, start)
        if end == -1:
            break
        certs.append(text[start : end + len(_END)] + "\n")
        idx = end + len(_END)
    return certs


def _bundle_contains(bundle_text: str, want_fp: str) -> bool:
    for block in split_pem_certs(bundle_text):
        try:
            if cert_fingerprint(block) == want_fp:
                return True
        except (ssl.SSLError, ValueError):
            continue  # skip unparseable block, keep scanning
    return False


def _attempt_restart(
    fs: FileSystem,
    coordinator: Coordinator,
    *,
    state_path: str | None,
    pending: PendingReload,
    now: float,
) -> bool:
    """Attempt one coordinator reload for `pending`. Returns whether it actually
    succeeded. When state tracking is on, the attempt is stamped to disk *before*
    the restart (so even a hard kill mid-restart leaves a paced, durable marker),
    and the marker is cleared only on success — a caught OSError leaves it set so
    a later run retries."""
    if state_path is not None:
        save_pending(
            fs,
            state_path,
            PendingReload(pending.bundle_path, pending.fingerprint, now),
        )
    try:
        coordinator.restart()
    except OSError:
        return False  # transient (e.g. touching the vassal file); marker stays
    if state_path is not None:
        clear_pending(fs, state_path)
    return True


def reconcile(
    fs: FileSystem,
    coordinator: Coordinator,
    *,
    bundle_path: str,
    site_packages_root: str,
    ca_pem: str,
    state_path: str | None = None,
    min_retry_interval_s: float = 300.0,
    now: float | None = None,
) -> Outcome:
    now = time.time() if now is None else now

    path = (
        bundle_path
        if fs.exists(bundle_path)
        else fs.glob(site_packages_root, "hue-bridge-ca-certs.pem")
    )
    if path is None:
        return Outcome(False, False, False, None)

    want_fp = cert_fingerprint(ca_pem)
    if _bundle_contains(fs.read_text(path), want_fp):
        # Cert already present. Normally a no-op — but if a prior run appended it
        # for THIS generation and never confirmed the reload, a durable marker
        # tells us the reload is still owed. Retry it (paced), never a silent
        # no-op (issue #96).
        if state_path is not None:
            pending = load_pending(fs, state_path)
            if (
                pending is not None
                and pending.bundle_path == path
                and pending.fingerprint == want_fp
                and coordinator.is_running()
            ):
                if now - pending.last_attempt_at < min_retry_interval_s:
                    return Outcome(True, False, False, path, reload_pending=True)
                restarted = _attempt_restart(
                    fs, coordinator, state_path=state_path, pending=pending, now=now
                )
                return Outcome(True, False, restarted, path, reload_pending=not restarted)
        return Outcome(True, False, False, path)

    fs.append_text(path, "\n" + ca_pem if not ca_pem.startswith("\n") else ca_pem)
    if not coordinator.is_running():
        # Not the Hue host: no coordinator to reload, so nothing is owed and no
        # marker is created (a stray marker here would misfire later).
        return Outcome(True, True, False, path)

    # Owed a reload. _attempt_restart persists the marker before touching the
    # coordinator, so a kill during restart still leaves durable evidence.
    pending = PendingReload(path, want_fp, now)
    restarted = _attempt_restart(fs, coordinator, state_path=state_path, pending=pending, now=now)
    return Outcome(True, True, restarted, path, reload_pending=not restarted)
