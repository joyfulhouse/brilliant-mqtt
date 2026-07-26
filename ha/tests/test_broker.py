"""Normalized and redacted HA broker profiles."""

from __future__ import annotations

import asyncio
import ssl
from collections.abc import AsyncIterator
from dataclasses import FrozenInstanceError, asdict, dataclass
from importlib.metadata import version
from types import TracebackType
from typing import Self
from unittest.mock import patch

import aiomqtt
import pytest

from custom_components.brilliant_mqtt.broker import (
    BrokerKind,
    BrokerProfile,
    DeviceMqttMessage,
)
from custom_components.brilliant_mqtt.errors import OperationError, OperationStage

PASSWORD = " password-secret "
USERNAME = " username-secret "
CA_PEM = """-----BEGIN CERTIFICATE-----
CA-PEM-SECRET
-----END CERTIFICATE-----"""


@dataclass(slots=True)
class _RawMessage:
    topic: str
    payload: bytes
    qos: int
    retain: bool


class _Messages:
    def __init__(self, messages: list[_RawMessage]) -> None:
        self._messages = iter(messages)

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> _RawMessage:
        try:
            return next(self._messages)
        except StopIteration as error:
            raise StopAsyncIteration from error


class _FakeAioClient:
    def __init__(self, messages: list[_RawMessage] | None = None) -> None:
        self.enter_count = 0
        self.exit_count = 0
        self.exit_types: list[type[BaseException] | None] = []
        self.subscriptions: list[tuple[str, int]] = []
        self.unsubscriptions: list[str] = []
        self.publications: list[tuple[str, bytes | str | None, int, bool]] = []
        self.messages: AsyncIterator[_RawMessage] = _Messages(messages or [])

    async def __aenter__(self) -> Self:
        self.enter_count += 1
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.exit_count += 1
        self.exit_types.append(exc_type)

    async def subscribe(self, topic: str, qos: int = 0) -> tuple[int, ...]:
        self.subscriptions.append((topic, qos))
        return (qos,)

    async def unsubscribe(self, topic: str) -> None:
        self.unsubscriptions.append(topic)

    async def publish(
        self,
        topic: str,
        payload: bytes | str | None = None,
        qos: int = 0,
        retain: bool = False,
    ) -> None:
        self.publications.append((topic, payload, qos, retain))


@dataclass(slots=True)
class _FakeSslContext:
    verify_mode: ssl.VerifyMode = ssl.CERT_NONE
    check_hostname: bool = False


def _profile_data(**overrides: object) -> dict[str, object]:
    data: dict[str, object] = {
        "broker_kind": "official_mosquitto",
        "mqtt_host": " MQTT.Example.COM ",
        "mqtt_port": 1884,
        "mqtt_username": USERNAME,
        "mqtt_password": PASSWORD,
        "mqtt_tls_enabled": False,
    }
    data.update(overrides)
    return data


def test_dependency_versions_are_exact_in_ha_environment() -> None:
    assert version("aiomqtt") == "2.5.1"
    assert version("paho-mqtt") == "2.1.0"


def test_broker_kind_values_are_exact() -> None:
    assert [kind.value for kind in BrokerKind] == [
        "official_mosquitto",
        "existing_broker",
    ]


@pytest.mark.asyncio
async def test_kind_changes_guidance_only_and_connection_arguments_are_identical() -> None:
    constructor_calls: list[dict[str, object]] = []
    clients = [_FakeAioClient(), _FakeAioClient()]

    def factory(**kwargs: object) -> _FakeAioClient:
        constructor_calls.append(kwargs)
        return clients[len(constructor_calls) - 1]

    official = BrokerProfile.from_mapping(_profile_data())
    existing = BrokerProfile.from_mapping(
        _profile_data(broker_kind=BrokerKind.EXISTING_BROKER.value)
    )

    with patch("custom_components.brilliant_mqtt.broker.aiomqtt.Client", side_effect=factory):
        async with official.device_client("setup-client"):
            pass
        async with existing.device_client("setup-client"):
            pass

    assert official.kind is BrokerKind.OFFICIAL_MOSQUITTO
    assert existing.kind is BrokerKind.EXISTING_BROKER
    assert constructor_calls[0] == constructor_calls[1]
    assert constructor_calls[0]["hostname"] == "mqtt.example.com"
    assert constructor_calls[0]["port"] == 1884


def test_profile_is_frozen_slotted_and_all_diagnostic_surfaces_are_redacted() -> None:
    profile = BrokerProfile.from_mapping(_profile_data(mqtt_tls_enabled=True, mqtt_tls_ca=CA_PEM))

    assert not hasattr(profile, "__dict__")
    field = "host"
    with pytest.raises(FrozenInstanceError):
        setattr(profile, field, "changed")

    assert profile.redacted_dict() == {
        "kind": "official_mosquitto",
        "host": "mqtt.example.com",
        "port": 1884,
        "tls_enabled": True,
        "has_custom_ca": True,
        "username_configured": True,
    }
    assert asdict(profile) == {
        "kind": BrokerKind.OFFICIAL_MOSQUITTO,
        "host": "mqtt.example.com",
        "port": 1884,
        "tls_enabled": True,
    }
    surfaces = (repr(profile), repr(asdict(profile)), repr(profile.redacted_dict()))
    for secret in (PASSWORD, USERNAME, CA_PEM, "CA-PEM-SECRET"):
        assert all(secret not in surface for surface in surfaces)


@pytest.mark.parametrize(
    "data",
    [
        _profile_data(broker_kind="unknown"),
        _profile_data(broker_kind="Official_Mosquitto"),
        {key: value for key, value in _profile_data().items() if key != "broker_kind"},
        _profile_data(mqtt_host=""),
        _profile_data(mqtt_host="   "),
        _profile_data(mqtt_host=123),
        _profile_data(mqtt_port=0),
        _profile_data(mqtt_port=65536),
        _profile_data(mqtt_port=True),
        _profile_data(mqtt_port="1883"),
        _profile_data(mqtt_port=1883.0),
        _profile_data(mqtt_port=None),
        _profile_data(mqtt_username=""),
        _profile_data(mqtt_username=123),
        _profile_data(mqtt_password=""),
        _profile_data(mqtt_password=123),
        _profile_data(mqtt_tls_enabled="true"),
        _profile_data(mqtt_tls_enabled=1),
        _profile_data(mqtt_tls_ca=CA_PEM),
        _profile_data(mqtt_tls_enabled=True, mqtt_tls_ca=""),
        _profile_data(mqtt_tls_enabled=True, mqtt_tls_ca="not a public certificate"),
        _profile_data(
            mqtt_tls_enabled=True,
            mqtt_tls_ca="-----BEGIN PRIVATE KEY-----\nprivate-secret\n-----END PRIVATE KEY-----",
        ),
    ],
)
def test_invalid_profile_shapes_map_to_stable_operation_error(
    data: dict[str, object],
) -> None:
    with pytest.raises(OperationError) as caught:
        BrokerProfile.from_mapping(data)

    assert caught.value.stage is OperationStage.BROKER_PROFILE
    assert caught.value.code == "invalid_broker_profile"
    assert caught.value.retryable is False
    surface = f"{caught.value!s} {caught.value!r} {caught.value.redacted_dict()!r}"
    for secret in (PASSWORD, USERNAME, CA_PEM, "private-secret"):
        assert secret not in surface


def test_default_port_follows_tls_only_when_port_is_absent() -> None:
    plaintext = _profile_data()
    plaintext.pop("mqtt_port")
    tls = _profile_data(mqtt_tls_enabled=True)
    tls.pop("mqtt_port")

    assert BrokerProfile.from_mapping(plaintext).port == 1883
    assert BrokerProfile.from_mapping(tls).port == 8883
    assert (
        BrokerProfile.from_mapping(_profile_data(mqtt_port=1883, mqtt_tls_enabled=True)).port
        == 1883
    )


@pytest.mark.asyncio
async def test_plaintext_client_uses_exact_credentials_mqtt_311_and_no_lwt() -> None:
    raw_client = _FakeAioClient()
    constructor_calls: list[dict[str, object]] = []

    def factory(**kwargs: object) -> _FakeAioClient:
        constructor_calls.append(kwargs)
        return raw_client

    profile = BrokerProfile.from_mapping(_profile_data())
    with (
        patch("custom_components.brilliant_mqtt.broker.ssl.create_default_context") as create_tls,
        patch("custom_components.brilliant_mqtt.broker.aiomqtt.Client", side_effect=factory),
    ):
        async with profile.device_client("brilliant-mqtt-setup-id"):
            pass

    create_tls.assert_not_called()
    assert constructor_calls == [
        {
            "hostname": "mqtt.example.com",
            "port": 1884,
            "username": USERNAME,
            "password": PASSWORD,
            "identifier": "brilliant-mqtt-setup-id",
            "protocol": aiomqtt.ProtocolVersion.V311,
            "will": None,
            "tls_context": None,
        }
    ]
    assert "tls_insecure" not in constructor_calls[0]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("ca_pem", "expected_call"),
    [
        (None, {}),
        (CA_PEM, {"cadata": CA_PEM}),
    ],
)
async def test_tls_uses_default_or_direct_custom_ca_with_strict_verification(
    ca_pem: str | None,
    expected_call: dict[str, str],
) -> None:
    profile_data = _profile_data(mqtt_tls_enabled=True)
    if ca_pem is not None:
        profile_data["mqtt_tls_ca"] = ca_pem
    profile = BrokerProfile.from_mapping(profile_data)
    context = _FakeSslContext()
    raw_client = _FakeAioClient()
    constructor_calls: list[dict[str, object]] = []

    def factory(**kwargs: object) -> _FakeAioClient:
        constructor_calls.append(kwargs)
        return raw_client

    with (
        patch(
            "custom_components.brilliant_mqtt.broker.ssl.create_default_context",
            return_value=context,
        ) as create_tls,
        patch("custom_components.brilliant_mqtt.broker.aiomqtt.Client", side_effect=factory),
        patch(
            "tempfile.NamedTemporaryFile",
            side_effect=AssertionError("custom CA must never use a temporary file"),
        ),
    ):
        async with profile.device_client("strict-tls-client"):
            pass

    create_tls.assert_called_once_with(**expected_call)
    assert context.verify_mode is ssl.CERT_REQUIRED
    assert context.check_hostname is True
    assert constructor_calls[0]["tls_context"] is context
    assert "tls_insecure" not in constructor_calls[0]
    assert CA_PEM not in repr(profile.redacted_dict())
    assert CA_PEM not in repr(constructor_calls[0] | {"tls_context": "<context>"})


@pytest.mark.asyncio
async def test_typed_device_client_seam_normalizes_messages_and_forwards_operations() -> None:
    raw_client = _FakeAioClient([_RawMessage("probe/result", b'{"nonce":"nonce-secret"}', 1, True)])
    profile = BrokerProfile.from_mapping(_profile_data())

    with patch("custom_components.brilliant_mqtt.broker.aiomqtt.Client", return_value=raw_client):
        async with profile.device_client("typed-seam") as client:
            await client.subscribe("probe/result", qos=1)
            await client.publish("probe/request", "request", qos=1, retain=True)
            message = await anext(client.messages)
            await client.unsubscribe("probe/result")

    assert isinstance(message, DeviceMqttMessage)
    assert message.topic == "probe/result"
    assert message.payload == b'{"nonce":"nonce-secret"}'
    assert message.qos == 1
    assert message.retain is True
    assert raw_client.subscriptions == [("probe/result", 1)]
    assert raw_client.publications == [("probe/request", "request", 1, True)]
    assert raw_client.unsubscriptions == ["probe/result"]


@pytest.mark.asyncio
async def test_context_and_explicit_disconnect_close_the_raw_client_exactly_once() -> None:
    raw_client = _FakeAioClient()
    profile = BrokerProfile.from_mapping(_profile_data())

    with patch("custom_components.brilliant_mqtt.broker.aiomqtt.Client", return_value=raw_client):
        async with profile.device_client("close-once") as client:
            await client.disconnect()
            await client.disconnect()

    assert raw_client.enter_count == 1
    assert raw_client.exit_count == 1


@pytest.mark.asyncio
async def test_context_closes_exactly_once_after_ordinary_body_failure() -> None:
    class BodyFailure(RuntimeError):
        pass

    raw_client = _FakeAioClient()
    profile = BrokerProfile.from_mapping(_profile_data())

    with (
        patch("custom_components.brilliant_mqtt.broker.aiomqtt.Client", return_value=raw_client),
        pytest.raises(BodyFailure),
    ):
        async with profile.device_client("failure-close"):
            raise BodyFailure

    assert raw_client.exit_count == 1
    assert raw_client.exit_types == [BodyFailure]


@pytest.mark.asyncio
async def test_context_closes_exactly_once_after_caller_cancellation() -> None:
    raw_client = _FakeAioClient()
    profile = BrokerProfile.from_mapping(_profile_data())
    entered = asyncio.Event()

    async def run_until_cancelled() -> None:
        async with profile.device_client("cancel-close"):
            entered.set()
            await asyncio.Future()

    with patch("custom_components.brilliant_mqtt.broker.aiomqtt.Client", return_value=raw_client):
        task = asyncio.create_task(run_until_cancelled())
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert raw_client.exit_count == 1
    assert raw_client.exit_types == [asyncio.CancelledError]


def test_tls_construction_failure_maps_to_redacted_invalid_profile() -> None:
    profile = BrokerProfile.from_mapping(_profile_data(mqtt_tls_enabled=True, mqtt_tls_ca=CA_PEM))

    with (
        patch(
            "custom_components.brilliant_mqtt.broker.ssl.create_default_context",
            side_effect=ssl.SSLError(f"bad CA {CA_PEM} password={PASSWORD}"),
        ),
        pytest.raises(OperationError) as caught,
    ):
        profile.device_client("tls-error")

    assert caught.value.code == "invalid_broker_profile"
    assert CA_PEM not in str(caught.value)
    assert PASSWORD not in repr(caught.value)


@pytest.mark.parametrize("client_id", ["", "   ", 123, True])
def test_invalid_client_id_maps_to_stable_profile_error(client_id: object) -> None:
    profile = BrokerProfile.from_mapping(_profile_data())

    with pytest.raises(OperationError, match="invalid_broker_profile"):
        profile.device_client(client_id)  # type: ignore[arg-type]
