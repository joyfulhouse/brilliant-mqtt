"""Real, generated X.509 test certificates (public PEM only, never a private key)."""

from __future__ import annotations

import base64
import datetime
from functools import cache

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID


@cache
def _key(name: str) -> ec.EllipticCurvePrivateKey:
    del name  # one deterministic cache slot per certificate name
    return ec.generate_private_key(ec.SECP256R1())


@cache
def ca_pem(name: str) -> str:
    """A self-signed public CA certificate named *name*, PEM-encoded."""
    key = _key(name)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
    now = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(1)
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=3650))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    return certificate.public_bytes(serialization.Encoding.PEM).decode()


def private_key_pem(name: str) -> str:
    """The PKCS#8 private key behind ``ca_pem(name)`` (for rejection tests only)."""
    return (
        _key(name)
        .private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        .decode()
    )


def pem_block(der: bytes) -> str:
    """Wrap arbitrary bytes in CERTIFICATE PEM framing (64-column base64)."""
    encoded = base64.b64encode(der).decode()
    lines = [encoded[index : index + 64] for index in range(0, len(encoded), 64)]
    return "-----BEGIN CERTIFICATE-----\n" + "\n".join(lines) + "\n-----END CERTIFICATE-----\n"


def truncated_ca_pem(name: str) -> str:
    """A validly base64-framed CERTIFICATE block whose DER is cut in half."""
    der = x509.load_pem_x509_certificate(ca_pem(name).encode()).public_bytes(
        serialization.Encoding.DER
    )
    return pem_block(der[: len(der) // 2])


def unicode_whitespace_variants(name: str) -> dict[str, str]:
    """Otherwise-valid PEM text smuggling non-ASCII whitespace (must be rejected)."""
    pem = ca_pem(name)
    other = ca_pem(f"{name}-second")
    return {
        "nbsp-indent": pem.replace("\n", "\n ", 1),
        "u2028-line-separator": pem.replace("\n", " "),
        "u2029-paragraph-separator": pem.replace("\n", " "),
        "unicode-only-line-between-blocks": pem + "　\n" + other,
        "zero-width-space": pem.replace("\n", "\n​", 1),
        "ascii-space-indent": pem.replace("\n", "\n ", 1),
        "lone-cr-line-endings": pem.replace("\n", "\r"),
        "trailing-space-on-line": pem.replace("\n", " \n", 1),
    }
