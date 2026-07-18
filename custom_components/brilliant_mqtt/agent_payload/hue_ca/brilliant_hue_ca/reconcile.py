"""Pure reconcile: ensure our CA is in the pinned Hue bundle (match by DER
fingerprint), and restart the local Hue coordinator only when we just appended.
Stdlib-only so it runs on the panel's Python 3.10 and off-panel in tests."""

from __future__ import annotations

import hashlib
import ssl
from dataclasses import dataclass

from .coordinator import Coordinator
from .fs import FileSystem

_BEGIN = "-----BEGIN CERTIFICATE-----"
_END = "-----END CERTIFICATE-----"


@dataclass(frozen=True)
class Outcome:
    bundle_found: bool
    appended: bool
    coordinator_restarted: bool
    bundle_path: str | None


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


def reconcile(
    fs: FileSystem,
    coordinator: Coordinator,
    *,
    bundle_path: str,
    site_packages_root: str,
    ca_pem: str,
) -> Outcome:
    path = (
        bundle_path
        if fs.exists(bundle_path)
        else fs.glob(site_packages_root, "hue-bridge-ca-certs.pem")
    )
    if path is None:
        return Outcome(False, False, False, None)

    want_fp = cert_fingerprint(ca_pem)
    if _bundle_contains(fs.read_text(path), want_fp):
        return Outcome(True, False, False, path)

    fs.append_text(path, "\n" + ca_pem if not ca_pem.startswith("\n") else ca_pem)
    if coordinator.is_running():
        coordinator.restart()
        return Outcome(True, True, True, path)
    return Outcome(True, True, False, path)
