"""Normalized and redacted HA broker profiles."""

from __future__ import annotations

import asyncio
import copy
import pickle
import ssl
from collections.abc import AsyncIterator
from dataclasses import FrozenInstanceError, asdict, dataclass
from importlib.metadata import version
from types import TracebackType
from typing import Self, cast
from unittest.mock import patch

import aiomqtt
import pytest
from aiomqtt.exceptions import MqttConnectError, MqttError

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


class _FailingLifecycleClient(_FakeAioClient):
    def __init__(
        self,
        *,
        enter_error: BaseException | None = None,
        exit_error: BaseException | None = None,
    ) -> None:
        super().__init__()
        self._enter_error = enter_error
        self._exit_error = exit_error

    async def __aenter__(self) -> Self:
        self.enter_count += 1
        if self._enter_error is not None:
            raise self._enter_error
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await super().__aexit__(exc_type, exc, traceback)
        if self._exit_error is not None:
            raise self._exit_error


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


def _direct_profile(**overrides: object) -> BrokerProfile:
    values: dict[str, object] = {
        "kind": BrokerKind.OFFICIAL_MOSQUITTO,
        "host": " MQTT.Example.COM ",
        "port": 1884,
        "tls_enabled": False,
        "_username_value": USERNAME,
        "_password_value": PASSWORD,
        "_ca_pem_value": None,
    }
    values.update(overrides)
    return BrokerProfile(
        kind=cast(BrokerKind, values["kind"]),
        host=cast(str, values["host"]),
        port=cast(int, values["port"]),
        tls_enabled=cast(bool, values["tls_enabled"]),
        _username_value=cast(str, values["_username_value"]),
        _password_value=cast(str, values["_password_value"]),
        _ca_pem_value=cast(str | None, values["_ca_pem_value"]),
    )


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


def test_direct_profile_construction_normalizes_and_preserves_exact_credentials() -> None:
    profile = _direct_profile()

    assert profile.host == "mqtt.example.com"
    with patch("custom_components.brilliant_mqtt.broker.aiomqtt.Client") as client:
        profile.device_client("direct-profile")

    assert client.call_args.kwargs["username"] == USERNAME
    assert client.call_args.kwargs["password"] == PASSWORD


@pytest.mark.parametrize(
    "overrides",
    [
        {"kind": "official_mosquitto"},
        {"kind": object()},
        {"host": ""},
        {"host": "   "},
        {"host": 123},
        {"port": 0},
        {"port": 65536},
        {"port": True},
        {"port": "1883"},
        {"tls_enabled": 1},
        {"tls_enabled": "true"},
        {"_username_value": ""},
        {"_username_value": 123},
        {"_password_value": ""},
        {"_password_value": 123},
        {"_ca_pem_value": CA_PEM},
        {"tls_enabled": True, "_ca_pem_value": 123},
        {"tls_enabled": True, "_ca_pem_value": ""},
        {"tls_enabled": True, "_ca_pem_value": "not a public certificate"},
        {
            "tls_enabled": True,
            "_ca_pem_value": (
                "-----BEGIN PRIVATE KEY-----\nprivate-secret\n-----END PRIVATE KEY-----"
            ),
        },
    ],
)
def test_direct_profile_construction_cannot_bypass_invariants(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="^invalid_broker_profile$") as caught:
        _direct_profile(**overrides)

    assert caught.value.__context__ is None
    assert caught.value.__cause__ is None
    for secret in (PASSWORD, USERNAME, CA_PEM, "private-secret"):
        assert secret not in str(caught.value)
        assert secret not in repr(caught.value)


def test_profile_equality_includes_all_hidden_connection_material() -> None:
    equivalent = BrokerProfile.from_mapping(_profile_data())
    normalized_equivalent = BrokerProfile.from_mapping(_profile_data(mqtt_host="mqtt.example.com"))
    different_username = BrokerProfile.from_mapping(
        _profile_data(mqtt_username="different-username-secret")
    )
    different_password = BrokerProfile.from_mapping(
        _profile_data(mqtt_password="different-password-secret")
    )
    first_ca = BrokerProfile.from_mapping(_profile_data(mqtt_tls_enabled=True, mqtt_tls_ca=CA_PEM))
    second_ca = BrokerProfile.from_mapping(
        _profile_data(
            mqtt_tls_enabled=True,
            mqtt_tls_ca=CA_PEM.replace("CA-PEM-SECRET", "DIFFERENT-CA-SECRET"),
        )
    )

    assert equivalent == normalized_equivalent
    assert equivalent != different_username
    assert equivalent != different_password
    assert first_ca != second_ca
    assert equivalent != BrokerProfile.from_mapping(
        _profile_data(broker_kind=BrokerKind.EXISTING_BROKER.value)
    )
    with pytest.raises(TypeError):
        hash(equivalent)
    surfaces = repr(
        (
            equivalent == different_username,
            equivalent == different_password,
            first_ca == second_ca,
        )
    )
    for secret in (
        USERNAME,
        PASSWORD,
        "different-username-secret",
        "different-password-secret",
        "DIFFERENT-CA-SECRET",
    ):
        assert secret not in surfaces


def test_profile_copy_is_safe_and_serialization_is_rejected_without_secrets() -> None:
    profile = BrokerProfile.from_mapping(_profile_data(mqtt_tls_enabled=True, mqtt_tls_ca=CA_PEM))

    assert copy.copy(profile) is profile
    assert copy.deepcopy(profile) is profile
    with pytest.raises(TypeError, match="^broker_profile_not_serializable$") as caught:
        pickle.dumps(profile)

    assert caught.value.__context__ is None
    assert caught.value.__cause__ is None
    for secret in (USERNAME, PASSWORD, CA_PEM):
        assert secret not in str(caught.value)
        assert secret not in repr(caught.value)


def test_sensitive_values_reject_ordinary_mutation_and_serialization() -> None:
    profile = BrokerProfile.from_mapping(_profile_data())

    with pytest.raises(AttributeError, match="^sensitive_text_is_immutable$"):
        profile._password._SensitiveText__value = "changed-password-secret"
    with pytest.raises(TypeError, match="^sensitive_text_not_serializable$"):
        pickle.dumps(profile._password)

    assert repr(profile._password) == "<redacted>"


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


def test_mapping_validation_does_not_retain_secret_bearing_context() -> None:
    with pytest.raises(OperationError) as caught:
        BrokerProfile.from_mapping(_profile_data(broker_kind="mqtt-password-secret"))

    assert caught.value.stage is OperationStage.BROKER_PROFILE
    assert caught.value.code == "invalid_broker_profile"
    assert caught.value.__context__ is None
    assert caught.value.__cause__ is None


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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("raw_error", "expected_code", "expected_retryable"),
    [
        (MqttConnectError(4), "broker_authentication_failed", False),
        (MqttConnectError(3), "broker_unavailable", True),
        (
            MqttError("mqtt.secret.example password-secret CA-PEM-SECRET"),
            "broker_connect_failed",
            True,
        ),
        (
            ConnectionRefusedError("mqtt.secret.example password-secret"),
            "broker_connect_failed",
            True,
        ),
        (
            TimeoutError("mqtt.secret.example password-secret"),
            "broker_timeout",
            True,
        ),
        (
            ssl.SSLCertVerificationError(
                1,
                "mqtt.secret.example password-secret CA-PEM-SECRET",
            ),
            "broker_tls_verification_failed",
            False,
        ),
    ],
)
async def test_context_entry_maps_broker_failures_and_closes_once(
    raw_error: Exception,
    expected_code: str,
    expected_retryable: bool,
) -> None:
    raw_client = _FailingLifecycleClient(enter_error=raw_error)
    profile = BrokerProfile.from_mapping(_profile_data())

    with (
        patch("custom_components.brilliant_mqtt.broker.aiomqtt.Client", return_value=raw_client),
        pytest.raises(OperationError) as caught,
    ):
        async with profile.device_client("entry-failure"):
            pytest.fail("entry failure must not yield a client")

    assert caught.value.stage is OperationStage.FLEET_AUTH
    assert caught.value.code == expected_code
    assert caught.value.retryable is expected_retryable
    assert caught.value.__context__ is None
    assert caught.value.__cause__ is None
    assert raw_client.enter_count == 1
    assert raw_client.exit_count == 1
    assert raw_client.exit_types == [type(raw_error)]
    for secret in (
        "mqtt.secret.example",
        "password-secret",
        "CA-PEM-SECRET",
    ):
        assert secret not in str(caught.value)
        assert secret not in repr(caught.value)
        assert secret not in repr(caught.value.redacted_dict())


@pytest.mark.asyncio
async def test_real_aiomqtt_wrapped_connect_error_maps_without_message_parsing() -> None:
    profile = BrokerProfile.from_mapping(_profile_data())

    with (
        patch.object(
            aiomqtt.Client,
            "_client_connect",
            side_effect=ssl.SSLCertVerificationError(
                1,
                "mqtt.secret.example password-secret CA-PEM-SECRET",
            ),
        ),
        pytest.raises(OperationError) as caught,
    ):
        async with profile.device_client("real-wrapped-connect-error"):
            pytest.fail("entry failure must not yield a client")

    assert caught.value.stage is OperationStage.FLEET_AUTH
    assert caught.value.code == "broker_connect_failed"
    assert caught.value.retryable is True
    assert caught.value.__context__ is None
    assert caught.value.__cause__ is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "control_error",
    [
        asyncio.CancelledError("cancel-secret"),
        KeyboardInterrupt("keyboard-secret"),
        SystemExit("system-exit-secret"),
    ],
)
async def test_context_entry_preserves_control_exceptions_after_one_cleanup(
    control_error: BaseException,
) -> None:
    raw_client = _FailingLifecycleClient(enter_error=control_error)
    profile = BrokerProfile.from_mapping(_profile_data())

    with (
        patch("custom_components.brilliant_mqtt.broker.aiomqtt.Client", return_value=raw_client),
        pytest.raises(type(control_error)) as caught,
    ):
        async with profile.device_client("entry-control-error"):
            pytest.fail("entry failure must not yield a client")

    assert caught.value is control_error
    assert raw_client.enter_count == 1
    assert raw_client.exit_count == 1
    assert raw_client.exit_types == [type(control_error)]


@pytest.mark.asyncio
async def test_entry_failure_preserves_primary_and_attaches_cleanup_failure() -> None:
    raw_client = _FailingLifecycleClient(
        enter_error=MqttConnectError(4),
        exit_error=RuntimeError("cleanup-password-secret"),
    )
    profile = BrokerProfile.from_mapping(_profile_data())

    with (
        patch("custom_components.brilliant_mqtt.broker.aiomqtt.Client", return_value=raw_client),
        pytest.raises(OperationError) as caught,
    ):
        async with profile.device_client("entry-and-cleanup-failure"):
            pytest.fail("entry failure must not yield a client")

    assert caught.value.stage is OperationStage.FLEET_AUTH
    assert caught.value.code == "broker_authentication_failed"
    assert caught.value.cleanup_error is not None
    assert caught.value.cleanup_error.stage is OperationStage.CLEANUP
    assert caught.value.cleanup_error.code == "cleanup_failed"
    assert caught.value.__context__ is None
    assert caught.value.__cause__ is None
    assert raw_client.exit_count == 1
    assert "cleanup-password-secret" not in repr(caught.value)
    assert "cleanup-password-secret" not in repr(caught.value.redacted_dict())


@pytest.mark.asyncio
async def test_close_failure_maps_to_cleanup_error_and_never_double_closes() -> None:
    raw_client = _FailingLifecycleClient(exit_error=RuntimeError("cleanup-password-secret"))
    profile = BrokerProfile.from_mapping(_profile_data())

    with (
        patch("custom_components.brilliant_mqtt.broker.aiomqtt.Client", return_value=raw_client),
        pytest.raises(OperationError) as caught,
    ):
        async with profile.device_client("close-failure") as client:
            await client.disconnect()

    assert caught.value.stage is OperationStage.CLEANUP
    assert caught.value.code == "cleanup_failed"
    assert caught.value.__context__ is None
    assert caught.value.__cause__ is None
    assert raw_client.exit_count == 1
    assert "cleanup-password-secret" not in repr(caught.value)


@pytest.mark.asyncio
async def test_implicit_close_failure_maps_to_cleanup_error_once() -> None:
    raw_client = _FailingLifecycleClient(
        exit_error=RuntimeError("implicit-cleanup-password-secret")
    )
    profile = BrokerProfile.from_mapping(_profile_data())

    with (
        patch("custom_components.brilliant_mqtt.broker.aiomqtt.Client", return_value=raw_client),
        pytest.raises(OperationError) as caught,
    ):
        async with profile.device_client("implicit-close-failure"):
            pass

    assert caught.value.stage is OperationStage.CLEANUP
    assert caught.value.code == "cleanup_failed"
    assert caught.value.__context__ is None
    assert caught.value.__cause__ is None
    assert raw_client.exit_count == 1
    assert "implicit-cleanup-password-secret" not in repr(caught.value)


@pytest.mark.asyncio
async def test_body_operation_error_remains_primary_when_cleanup_fails() -> None:
    raw_client = _FailingLifecycleClient(exit_error=RuntimeError("cleanup-password-secret"))
    profile = BrokerProfile.from_mapping(_profile_data())
    primary = OperationError.for_code(
        OperationStage.PANEL_TO_HA,
        "panel_to_ha_timeout",
    )

    with (
        patch("custom_components.brilliant_mqtt.broker.aiomqtt.Client", return_value=raw_client),
        pytest.raises(OperationError) as caught,
    ):
        async with profile.device_client("primary-and-cleanup-failure"):
            raise primary

    assert caught.value.stage is OperationStage.PANEL_TO_HA
    assert caught.value.code == "panel_to_ha_timeout"
    assert caught.value.cleanup_error is not None
    assert caught.value.cleanup_error.code == "cleanup_failed"
    assert caught.value.__context__ is primary
    assert caught.value.__cause__ is None
    assert primary.__context__ is None
    assert primary.__cause__ is None
    assert raw_client.exit_count == 1
    assert "cleanup-password-secret" not in repr(caught.value)
    assert "cleanup-password-secret" not in repr(caught.value.redacted_dict())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "control_error",
    [
        asyncio.CancelledError("cancel-secret"),
        KeyboardInterrupt("keyboard-secret"),
        SystemExit("system-exit-secret"),
    ],
)
async def test_cleanup_failure_never_swallows_body_control_exception(
    control_error: BaseException,
) -> None:
    raw_client = _FailingLifecycleClient(exit_error=RuntimeError("cleanup-password-secret"))
    profile = BrokerProfile.from_mapping(_profile_data())

    with (
        patch("custom_components.brilliant_mqtt.broker.aiomqtt.Client", return_value=raw_client),
        pytest.raises(type(control_error)) as caught,
    ):
        async with profile.device_client("body-control-error"):
            raise control_error

    assert caught.value is control_error
    assert raw_client.exit_count == 1


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
