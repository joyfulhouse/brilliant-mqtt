"""One substantive validator for operator-supplied public X.509 certificate chains.

Shared by the config/options flows (broker CA, fleet and per-panel diyHue CA), the
strict stored-data parsers (broker profile, fleet diyHue CA), the manager's deploy
path, and the legacy-to-fleet migration, so stored, accepted, promoted and deployed
certificates obey one contract.

The contract is deliberately byte-strict, because the accepted text is stored and
later consumed verbatim by the panel's Python 3.10 hue-ca tool
(``ssl.PEM_cert_to_DER_cert``, ASCII-only) and by TLS loaders. Nothing is ever
silently stripped during validation:

* pure ASCII; lines end in ``\\n`` or ``\\r\\n`` only (no lone ``\\r``, no Unicode
  line or paragraph separators);
* only ``CERTIFICATE`` PEM blocks; empty lines may surround or separate blocks,
  but no other text (no Unicode or ASCII whitespace-only lines, no indentation,
  no trailing spaces, no keys);
* every body line is pure base64, each block decodes strictly to exactly one
  parseable DER X.509 certificate.

``canonical_certificate_chain`` additionally returns the canonical rendering
(LF line endings, 64-column base64 re-encoded from the parsed DER, one trailing
newline per block) — what the diyHue CA paths persist and deploy.
"""

from __future__ import annotations

import base64
import binascii
import re

from cryptography import x509

# Matches the panel-side staging limit for a custom broker CA (panel_ops).
MAX_CERTIFICATE_CHAIN_BYTES = 256 * 1024

_BEGIN = "-----BEGIN CERTIFICATE-----"
_END = "-----END CERTIFICATE-----"
_BODY_LINE = re.compile(r"[A-Za-z0-9+/]+={0,2}")
_PRINTABLE_ASCII_OR_LF = re.compile(r"[\x20-\x7e\n]*")
_LINE_WIDTH = 64


def _certificate_ders(value: object) -> list[bytes] | None:
    """Return each block's DER for strictly valid input, else None."""
    if (
        not isinstance(value, str)
        or not value.isascii()
        or len(value) > MAX_CERTIFICATE_CHAIN_BYTES
    ):
        return None
    text = value.replace("\r\n", "\n")
    if _PRINTABLE_ASCII_OR_LF.fullmatch(text) is None:
        return None  # a lone CR, tab, or any other control character
    ders: list[bytes] = []
    body: list[str] | None = None
    for line in text.split("\n"):
        if body is None:
            if not line:
                continue
            if line != _BEGIN:
                return None
            body = []
            continue
        if line == _END:
            if not body:
                return None
            try:
                der = base64.b64decode("".join(body), validate=True)
                x509.load_der_x509_certificate(der)
            except (binascii.Error, ValueError):
                return None
            ders.append(der)
            body = None
            continue
        if _BODY_LINE.fullmatch(line) is None:
            return None
        body.append(line)
    if body is not None or not ders:
        return None
    return ders


def _pem(der: bytes) -> str:
    encoded = base64.b64encode(der).decode("ascii")
    lines = [encoded[index : index + _LINE_WIDTH] for index in range(0, len(encoded), _LINE_WIDTH)]
    return "\n".join([_BEGIN, *lines, _END]) + "\n"


def canonical_certificate_chain(
    value: object,
    *,
    max_certificates: int | None = None,
) -> str | None:
    """Return the canonical PEM for a strictly valid chain, else None."""
    ders = _certificate_ders(value)
    if ders is None or (max_certificates is not None and len(ders) > max_certificates):
        return None
    return "".join(_pem(der) for der in ders)


def is_public_certificate_chain(value: object) -> bool:
    """Return whether *value* is a strictly valid chain of public certificates."""
    return _certificate_ders(value) is not None


def canonical_hue_ca_certificate(value: object) -> str | None:
    """Return the canonical PEM of exactly one diyHue CA certificate, else None.

    The panel's hue-ca tool fingerprints the whole file as ONE certificate and
    matches it against single bundle blocks, so a chain would never match and is
    refused here rather than re-appended on every timer run.
    """
    return canonical_certificate_chain(value, max_certificates=1)


def is_hue_ca_certificate(value: object) -> bool:
    """Return whether *value* is exactly one strictly valid public certificate."""
    return canonical_hue_ca_certificate(value) is not None
