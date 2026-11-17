"""One substantive validator for operator-supplied public X.509 certificate chains.

Shared by the config/options flows (broker CA, fleet and per-panel diyHue CA), the
strict fleet entry parser, and the legacy-to-fleet migration, so stored, accepted,
and promoted certificates obey one contract: only ``CERTIFICATE`` PEM blocks, each
strict base64 of exactly one DER X.509 certificate; nothing else (no private or
public keys, no stray text, no truncated or undecodable blocks).
"""

from __future__ import annotations

import base64
import binascii
import unicodedata

from cryptography import x509

# Matches the panel-side staging limit for a custom broker CA (panel_ops).
MAX_CERTIFICATE_CHAIN_BYTES = 256 * 1024

_BEGIN = "-----BEGIN CERTIFICATE-----"
_END = "-----END CERTIFICATE-----"


def _certificate_bodies(value: str) -> list[str] | None:
    """Split strictly framed CERTIFICATE blocks into their base64 bodies."""
    lines = value.strip(" \r\n").splitlines()
    bodies: list[str] = []
    body: list[str] | None = None
    for line in lines:
        if body is None:
            if not line.strip():
                continue
            if line != _BEGIN:
                return None
            body = []
            continue
        if line == _END:
            if not body:
                return None
            bodies.append("".join(body))
            body = None
            continue
        if not line or line.isspace() or line.startswith("-----"):
            return None
        body.append(line.strip())
    if body is not None or not bodies:
        return None
    return bodies


def is_public_certificate_chain(value: object) -> bool:
    """Return whether *value* is a bounded chain of parseable public certificates."""
    if not isinstance(value, str):
        return False
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    if (
        len(encoded) > MAX_CERTIFICATE_CHAIN_BYTES
        or "PRIVATE KEY" in value.upper()
        or any(
            unicodedata.category(character) == "Cc" and character not in "\r\n"
            for character in value
        )
    ):
        return False
    bodies = _certificate_bodies(value)
    if bodies is None:
        return False
    for body in bodies:
        try:
            der = base64.b64decode(body, validate=True)
            x509.load_der_x509_certificate(der)
        except (binascii.Error, ValueError):
            return False
    return True
