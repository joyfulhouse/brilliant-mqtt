"""Strict TLS construction tests for the real MQTT adapter."""

from __future__ import annotations

import asyncio
import logging
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


class _CloseFailingClient:
    def __init__(self, close_error: BaseException) -> None:
        self._close_error = close_error
        self.exit_attempts = 0

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.exit_attempts += 1
        raise self._close_error


class _BlockingCloseClient:
    def __init__(self) -> None:
        self.close_started = asyncio.Event()
        self.exit_attempts = 0

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.exit_attempts += 1
        self.close_started.set()
        await asyncio.Future()


class _SuccessfulCloseClient:
    def __init__(self) -> None:
        self.exit_attempts = 0

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.exit_attempts += 1


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


async def test_checked_redacted_disconnect_finishes_reader_and_client_close(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    raw_reader_error = "raw-reader-error password=s3cr3t"
    raw_close_error = "raw-close-error host=mqtt.example.test"
    client = _CloseFailingClient(RuntimeError(raw_close_error))
    monkeypatch.setattr(aiomqtt, "Client", lambda **kwargs: client)
    caplog.set_level(logging.DEBUG, logger=mqttio.__name__)
    adapter = mqttio.AioMqttAdapter(
        _settings(tls_enabled=False),
        publish_availability=False,
        checked_disconnect=True,
        redacted_logging=True,
    )

    async def fail_reader() -> None:
        raise RuntimeError(raw_reader_error)

    reader_task = asyncio.create_task(fail_reader())
    await asyncio.sleep(0)
    adapter._reader_task = reader_task

    with pytest.raises(RuntimeError, match=r"^MQTT disconnect failed$") as raised:
        await adapter.disconnect()

    assert client.exit_attempts == 1
    assert adapter._reader_task is None
    assert raw_reader_error not in str(raised.value)
    assert raw_close_error not in str(raised.value)
    assert raw_reader_error not in caplog.text
    assert raw_close_error not in caplog.text


async def test_checked_disconnect_maps_reader_only_failure_to_generic_redacted_error(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    raw_reader_error = "reader-only-error password=s3cr3t"
    client = _SuccessfulCloseClient()
    monkeypatch.setattr(aiomqtt, "Client", lambda **kwargs: client)
    caplog.set_level(logging.DEBUG, logger=mqttio.__name__)
    adapter = mqttio.AioMqttAdapter(
        _settings(tls_enabled=False),
        publish_availability=False,
        checked_disconnect=True,
        redacted_logging=True,
    )

    async def fail_reader_during_cancellation() -> None:
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            raise RuntimeError(raw_reader_error) from None

    adapter._reader_task = asyncio.create_task(fail_reader_during_cancellation())
    await asyncio.sleep(0)

    with pytest.raises(RuntimeError, match=r"^MQTT disconnect failed$") as raised:
        await adapter.disconnect()

    assert client.exit_attempts == 1
    assert adapter._reader_task is None
    assert raw_reader_error not in str(raised.value)
    assert raw_reader_error not in caplog.text


async def test_default_disconnect_remains_best_effort(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _CloseFailingClient(RuntimeError("resident close failure"))
    monkeypatch.setattr(aiomqtt, "Client", lambda **kwargs: client)
    adapter = mqttio.AioMqttAdapter(
        _settings(tls_enabled=False),
        publish_availability=False,
    )

    await adapter.disconnect()

    assert client.exit_attempts == 1


async def test_checked_disconnect_maps_internal_close_cancellation_to_generic_failure(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    raw_close_error = "raw-cancelled-close password=s3cr3t"
    client = _CloseFailingClient(asyncio.CancelledError(raw_close_error))
    monkeypatch.setattr(aiomqtt, "Client", lambda **kwargs: client)
    caplog.set_level(logging.DEBUG, logger=mqttio.__name__)
    adapter = mqttio.AioMqttAdapter(
        _settings(tls_enabled=False),
        publish_availability=False,
        checked_disconnect=True,
        redacted_logging=True,
    )

    with pytest.raises(RuntimeError, match=r"^MQTT disconnect failed$") as raised:
        await adapter.disconnect()

    assert client.exit_attempts == 1
    assert raw_close_error not in str(raised.value)
    assert raw_close_error not in caplog.text


async def test_default_disconnect_preserves_internal_close_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _CloseFailingClient(asyncio.CancelledError("resident cancellation"))
    monkeypatch.setattr(aiomqtt, "Client", lambda **kwargs: client)
    adapter = mqttio.AioMqttAdapter(
        _settings(tls_enabled=False),
        publish_availability=False,
    )

    with pytest.raises(asyncio.CancelledError):
        await adapter.disconnect()

    assert client.exit_attempts == 1


async def test_checked_disconnect_propagates_caller_cancellation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _BlockingCloseClient()
    monkeypatch.setattr(aiomqtt, "Client", lambda **kwargs: client)
    adapter = mqttio.AioMqttAdapter(
        _settings(tls_enabled=False),
        publish_availability=False,
        checked_disconnect=True,
        redacted_logging=True,
    )
    disconnect_task = asyncio.create_task(adapter.disconnect())
    await asyncio.wait_for(client.close_started.wait(), timeout=0.1)

    disconnect_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await disconnect_task
    assert client.exit_attempts == 1


async def test_checked_disconnect_propagates_caller_cancellation_during_reader_teardown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _SuccessfulCloseClient()
    monkeypatch.setattr(aiomqtt, "Client", lambda **kwargs: client)
    adapter = mqttio.AioMqttAdapter(
        _settings(tls_enabled=False),
        publish_availability=False,
        checked_disconnect=True,
        redacted_logging=True,
    )
    reader_cleanup_started = asyncio.Event()
    release_reader_cleanup = asyncio.Event()

    async def slow_reader_cancellation() -> None:
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            reader_cleanup_started.set()
            await release_reader_cleanup.wait()
            raise

    reader_task = asyncio.create_task(slow_reader_cancellation())
    adapter._reader_task = reader_task
    disconnect_task = asyncio.create_task(adapter.disconnect())
    await asyncio.wait_for(reader_cleanup_started.wait(), timeout=0.1)

    disconnect_task.cancel()

    try:
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(disconnect_task, timeout=0.1)
    finally:
        release_reader_cleanup.set()
        if not reader_task.done():
            reader_task.cancel()
        try:
            await reader_task
        except asyncio.CancelledError:
            pass
