"""Pure reconcile: ensure our CA is in the pinned Hue bundle (match by DER
fingerprint), and reload the local Hue coordinator whenever a reload is owed.

A reload is owed the moment we decide to append the CA (the coordinator is
running and the cert is missing), and stays owed until a `coordinator.restart()`
actually returns success. Because each timer tick is a *fresh* oneshot process,
that owed-reload is recorded to disk to survive a crash or a failed restart
(issue #96). Two ordering rules make it crash-safe:

  * the marker is written *before* `append_text`, so a kill between the marker
    write and the append self-heals (the next run still sees the cert absent and
    re-appends), and a kill between append and restart is caught by the
    cert-present retry path (the marker is already durable);
  * the marker is cleared *only* after a restart succeeds.

The marker is keyed to the exact (bundle_path, fingerprint) generation, retries
are paced off its last-attempt timestamp, and state-file writes never abort the
reload they guard (a read-only/full /var must not strand the coordinator on
stale trust). Stdlib-only so it runs on the panel's Python 3.10 and in tests."""

from __future__ import annotations

import hashlib
import logging
import ssl
import time
from dataclasses import dataclass

from .coordinator import Coordinator
from .fs import FileSystem
from .state import PendingReload, clear_pending, load_pending, save_pending

_LOG = logging.getLogger(__name__)

_BEGIN = "-----BEGIN CERTIFICATE-----"
_END = "-----END CERTIFICATE-----"
_CLEARED = "{}"  # what clear_pending writes; the "nothing owed" sentinel


@dataclass(frozen=True)
class Outcome:
    bundle_found: bool
    appended: bool
    coordinator_restarted: bool
    bundle_path: str | None
    # True when, after this pass, a coordinator reload is still owed (marker set
    # or a torn marker seen): the restart failed, was killed, or was paced/held.
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


def _appendable(ca_pem: str) -> str:
    """ca_pem with a leading newline unless it already has one, so an append
    never fuses onto the last line of the existing bundle."""
    return ca_pem if ca_pem.startswith("\n") else "\n" + ca_pem


def _save_marker(fs: FileSystem, state_path: str, pending: PendingReload) -> None:
    """Persist the owed-reload marker, swallowing a state-write OSError so a
    read-only or full /var can never abort the reload it guards — the restart is
    still attempted this run (issue #96)."""
    try:
        save_pending(fs, state_path, pending)
    except OSError:
        _LOG.warning(
            "could not persist pending-reload marker at %s; reload still attempted",
            state_path,
            exc_info=True,
        )


def _clear_marker(fs: FileSystem, state_path: str) -> None:
    """Clear the owed-reload marker, swallowing OSError — a failed clear costs at
    most one extra paced restart on the next tick, never a lost reload."""
    try:
        clear_pending(fs, state_path)
    except OSError:
        _LOG.warning(
            "could not clear pending-reload marker at %s; may retry once more",
            state_path,
            exc_info=True,
        )


def _restart_and_clear(fs: FileSystem, coordinator: Coordinator, state_path: str) -> bool:
    """Restart the coordinator; clear the marker only on success. A transient
    OSError leaves the marker for a later paced retry. Returns whether the
    restart succeeded."""
    try:
        coordinator.restart()
    except OSError:
        return False
    _clear_marker(fs, state_path)
    return True


def _marker_is_torn(fs: FileSystem, state_path: str) -> bool:
    """True when the state file is present but neither a valid marker nor the
    cleared sentinel — i.e. torn/garbled. Absent file or clean sentinel is not
    torn (nothing owed)."""
    if not fs.exists(state_path):
        return False
    if load_pending(fs, state_path) is not None:
        return False
    try:
        return fs.read_text(state_path).strip() != _CLEARED
    except OSError:
        return True


def _reconcile_present(
    fs: FileSystem,
    coordinator: Coordinator,
    *,
    path: str,
    want_fp: str,
    state_path: str,
    min_retry_interval_s: float,
    now: float,
) -> Outcome:
    """Cert already in the bundle: normally a no-op, unless a marker says a
    reload for this generation is still owed (issue #96)."""
    pending = load_pending(fs, state_path)
    if pending is not None:
        if pending.bundle_path != path or pending.fingerprint != want_fp:
            return Outcome(True, False, False, path)  # marker is for another CA
        if not coordinator.is_running():
            # Coordinator gone / not the Hue host: it owes no reload. Drop the
            # stale marker so it can't fire a spurious restart if the vassal
            # returns (consistent with the append branch's non-host rule).
            _clear_marker(fs, state_path)
            return Outcome(True, False, False, path)
        elapsed = now - pending.last_attempt_at
        if 0 <= elapsed < min_retry_interval_s:
            return Outcome(True, False, False, path, reload_pending=True)  # paced
        _save_marker(fs, state_path, PendingReload(path, want_fp, now))  # re-stamp
        restarted = _restart_and_clear(fs, coordinator, state_path)
        return Outcome(True, False, restarted, path, reload_pending=not restarted)
    if _marker_is_torn(fs, state_path):
        # A torn marker while the cert is already present may hide an owed reload
        # — do not treat it as a clean no-op.
        _LOG.warning("pending-reload marker at %s is unreadable; a reload may be owed", state_path)
        if not coordinator.is_running():
            return Outcome(True, False, False, path, reload_pending=True)
        _save_marker(fs, state_path, PendingReload(path, want_fp, now))
        restarted = _restart_and_clear(fs, coordinator, state_path)
        return Outcome(True, False, restarted, path, reload_pending=not restarted)
    return Outcome(True, False, False, path)  # healthy steady state


def reconcile(
    fs: FileSystem,
    coordinator: Coordinator,
    *,
    bundle_path: str,
    site_packages_root: str,
    ca_pem: str,
    state_path: str,
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
        return _reconcile_present(
            fs,
            coordinator,
            path=path,
            want_fp=want_fp,
            state_path=state_path,
            min_retry_interval_s=min_retry_interval_s,
            now=now,
        )

    running = coordinator.is_running()
    if running:
        # Reload will be owed: persist the marker BEFORE the append so a kill in
        # the marker->append->restart window is always recoverable.
        _save_marker(fs, state_path, PendingReload(path, want_fp, now))
    fs.append_text(path, _appendable(ca_pem))
    if not running:
        # Not the Hue host: no coordinator to reload, so nothing is owed and no
        # marker is created.
        return Outcome(True, True, False, path)
    restarted = _restart_and_clear(fs, coordinator, state_path)
    return Outcome(True, True, restarted, path, reload_pending=not restarted)
