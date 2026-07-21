"""Strict TLS construction tests for the real MQTT adapter."""

from __future__ import annotations

import ssl
from dataclasses import dataclass
from types import SimpleNamespace
from typing import cast

import aiomqtt
import pytest

from brilliant_mqtt import mqttio
from brilliant_mqtt.config import Settings


@dataclass
class _TlsContext:
    check_hostname: bool = False
    verify_mode: ssl.VerifyMode = ssl.CERT_NONE


def _settings(*, tls_enabled: bool, ca_file: str | None = None) -> Settings:
    return cast(
        Settings,
        SimpleNamespace(
            panel="office",
            mqtt_host="mqtt.example.test",
            mqtt_port=8883,
            mqtt_username="brilliant",
            mqtt_password="s3cr3t",
            mqtt_tls_enabled=tls_enabled,
            mqtt_tls_ca_file=ca_file,
        ),
    )


def test_plaintext_settings_do_not_build_tls_context(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str | None] = []

    def create_default_context(*, cafile: str | None = None) -> ssl.SSLContext:
        calls.append(cafile)
        return cast(ssl.SSLContext, _TlsContext())

    monkeypatch.setattr(ssl, "create_default_context", create_default_context)

    assert mqttio.build_tls_context(_settings(tls_enabled=False)) is None
    assert calls == []


@pytest.mark.parametrize(
    ("ca_file", "expected_cafile"),
    [
        (None, None),
        ("/tmp/brilliant-mqtt-test-ca.pem", "/tmp/brilliant-mqtt-test-ca.pem"),
    ],
)
def test_tls_context_uses_selected_ca_and_requires_server_authentication(
    monkeypatch: pytest.MonkeyPatch,
    ca_file: str | None,
    expected_cafile: str | None,
) -> None:
    raw_context = _TlsContext()
    context = cast(ssl.SSLContext, raw_context)
    calls: list[str | None] = []

    def create_default_context(*, cafile: str | None = None) -> ssl.SSLContext:
        calls.append(cafile)
        return context

    monkeypatch.setattr(ssl, "create_default_context", create_default_context)

    result = mqttio.build_tls_context(_settings(tls_enabled=True, ca_file=ca_file))

    assert result is context
    assert calls == [expected_cafile]
    assert raw_context.check_hostname is True
    assert raw_context.verify_mode is ssl.CERT_REQUIRED


def test_tls_context_creation_failure_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    def create_default_context(*, cafile: str | None = None) -> ssl.SSLContext:
        raise FileNotFoundError(cafile)

    monkeypatch.setattr(ssl, "create_default_context", create_default_context)

    with pytest.raises(FileNotFoundError):
        mqttio.build_tls_context(
            _settings(tls_enabled=True, ca_file="/tmp/brilliant-mqtt-test-ca.pem")
        )


def test_adapter_passes_tls_context_without_insecure_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tls_context = cast(ssl.SSLContext, _TlsContext())
    client = object()
    client_calls: list[dict[str, object]] = []

    def client_factory(**kwargs: object) -> object:
        client_calls.append(kwargs)
        return client

    monkeypatch.setattr(mqttio, "build_tls_context", lambda _settings: tls_context, raising=False)
    monkeypatch.setattr(aiomqtt, "Client", client_factory)

    adapter = mqttio.AioMqttAdapter(_settings(tls_enabled=True))

    assert adapter._client is client
    assert client_calls[0]["tls_context"] is tls_context
    assert "tls_insecure" not in client_calls[0]


def test_adapter_does_not_fall_back_to_plaintext_when_tls_setup_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client_calls: list[dict[str, object]] = []

    def fail_tls(_settings: Settings) -> ssl.SSLContext | None:
        raise FileNotFoundError("missing CA")

    def client_factory(**kwargs: object) -> object:
        client_calls.append(kwargs)
        return object()

    monkeypatch.setattr(mqttio, "build_tls_context", fail_tls, raising=False)
    monkeypatch.setattr(aiomqtt, "Client", client_factory)

    with pytest.raises(FileNotFoundError):
        mqttio.AioMqttAdapter(_settings(tls_enabled=True))

    assert client_calls == []
