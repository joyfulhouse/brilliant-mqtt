"""The one shared, substantive public X.509 certificate-chain validator."""

from __future__ import annotations

import base64
import hashlib
import importlib
import ssl
import sys
from pathlib import Path
from typing import Any

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import serialization

from custom_components.brilliant_mqtt.certificates import (
    canonical_certificate_chain,
    canonical_hue_ca_certificate,
    is_public_certificate_chain,
)
from tests.cert_fixtures import (
    ca_pem,
    pem_block,
    private_key_pem,
    truncated_ca_pem,
    unicode_whitespace_variants,
)


def test_accepts_real_certificates_and_chains() -> None:
    assert is_public_certificate_chain(ca_pem("hue"))
    assert is_public_certificate_chain(ca_pem("hue") + ca_pem("intermediate"))
    assert is_public_certificate_chain(ca_pem("hue").replace("\n", "\r\n"))
    assert is_public_certificate_chain("\n" + ca_pem("hue") + "\n\n")


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(
            "-----BEGIN CERTIFICATE-----\nnot a certificate\n-----END CERTIFICATE-----",
            id="framing-only-placeholder",
        ),
        pytest.param(
            "-----BEGIN CERTIFICATE-----\n@@@@####\n-----END CERTIFICATE-----\n",
            id="invalid-base64",
        ),
        pytest.param(pem_block(b"definitely not DER encoded X.509"), id="base64-non-x509"),
        pytest.param(truncated_ca_pem("hue"), id="truncated-certificate"),
        pytest.param(ca_pem("hue") + private_key_pem("hue"), id="certificate-plus-private-key"),
        pytest.param(private_key_pem("hue"), id="private-key-only"),
        pytest.param(
            ca_pem("hue") + pem_block(b"\x30\x03\x02\x01\x01"),
            id="valid-then-invalid-block",
        ),
        pytest.param(
            ca_pem("hue").replace("-----END CERTIFICATE-----\n", ""),
            id="missing-end-marker",
        ),
        pytest.param("trailing text\n" + ca_pem("hue"), id="text-outside-blocks"),
        pytest.param("", id="empty"),
        pytest.param("   \n", id="blank"),
    ],
)
def test_rejects_anything_but_a_parseable_public_certificate_chain(value: str) -> None:
    assert not is_public_certificate_chain(value)


@pytest.mark.parametrize("value", [None, 1, b"bytes", ["pem"]])
def test_rejects_non_strings(value: object) -> None:
    assert not is_public_certificate_chain(value)


@pytest.mark.parametrize(
    "value",
    list(unicode_whitespace_variants("strict").values()),
    ids=list(unicode_whitespace_variants("strict")),
)
def test_rejects_non_ascii_or_non_pem_whitespace(value: str) -> None:
    """Validation never strips text the panel would later choke on."""
    assert not is_public_certificate_chain(value)
    assert canonical_certificate_chain(value) is None
    assert canonical_hue_ca_certificate(value) is None


def test_canonical_form_is_lf_64_column_and_idempotent() -> None:
    pem = ca_pem("canonical")
    der = x509.load_pem_x509_certificate(pem.encode()).public_bytes(serialization.Encoding.DER)
    rewrapped = pem_block(der).replace("\n", "\r\n")
    joined = "\n".join(
        [
            "-----BEGIN CERTIFICATE-----",
            base64.b64encode(der).decode(),
            "-----END CERTIFICATE-----",
        ]
    )

    for variant in (pem, rewrapped, joined, "\n\n" + pem + "\n\n"):
        canonical = canonical_certificate_chain(variant)
        assert canonical == pem
        assert canonical_certificate_chain(canonical) == canonical
    chain = canonical_certificate_chain(pem.replace("\n", "\r\n") + "\r\n" + ca_pem("two"))
    assert chain == pem + ca_pem("two")


def test_hue_ca_accepts_exactly_one_certificate() -> None:
    """The panel recovery tool fingerprints one certificate, so chains are refused."""
    assert canonical_hue_ca_certificate(ca_pem("hue")) == ca_pem("hue")
    assert canonical_hue_ca_certificate(ca_pem("hue") + ca_pem("other")) is None


def _hue_ca_reconcile() -> Any:
    """Import the panel's actual hue-ca recovery module from the shipped payload."""
    root = Path(__file__).parents[2] / "custom_components/brilliant_mqtt/agent_payload/hue_ca"
    sys.path.insert(0, str(root))
    try:
        return importlib.import_module("brilliant_hue_ca.reconcile")
    finally:
        sys.path.remove(str(root))


@pytest.mark.parametrize(
    "variant",
    ["lf", "crlf", "rewrapped", "surrounding-blank-lines"],
)
def test_accepted_hue_ca_is_consumable_by_panel_tool_and_tls(variant: str) -> None:
    """The exact deployed bytes parse in the panel tool and in an ssl context."""
    pem = ca_pem("deployable")
    der = x509.load_pem_x509_certificate(pem.encode()).public_bytes(serialization.Encoding.DER)
    submitted = {
        "lf": pem,
        "crlf": pem.replace("\n", "\r\n"),
        "rewrapped": "-----BEGIN CERTIFICATE-----\n"
        + base64.b64encode(der).decode()
        + "\n-----END CERTIFICATE-----",
        "surrounding-blank-lines": "\n\n" + pem + "\n",
    }[variant]
    canonical = canonical_hue_ca_certificate(submitted)
    assert canonical is not None
    deployed = canonical.strip()  # exactly what the manager uploads to the panel
    reconcile = _hue_ca_reconcile()

    fingerprint = reconcile.cert_fingerprint(deployed)
    assert fingerprint == hashlib.sha256(der).hexdigest()
    bundle = ca_pem("existing-bundle-root") + reconcile._appendable(deployed)
    assert [reconcile.cert_fingerprint(block) for block in reconcile.split_pem_certs(bundle)][
        -1
    ] == fingerprint
    assert reconcile._bundle_contains(bundle, fingerprint) is True
    deployed.encode("ascii")  # the tool's ssl.PEM_cert_to_DER_cert needs pure ASCII
    ssl.create_default_context(cadata=deployed)
