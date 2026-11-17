"""The one shared, substantive public X.509 certificate-chain validator."""

from __future__ import annotations

import pytest

from custom_components.brilliant_mqtt.certificates import is_public_certificate_chain
from tests.cert_fixtures import ca_pem, pem_block, private_key_pem, truncated_ca_pem


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
