"""Normalized and redacted HA broker profiles."""

from __future__ import annotations

import asyncio
import copy
import pickle
import ssl
import threading
from collections.abc import AsyncIterator
from dataclasses import FrozenInstanceError, asdict, dataclass
from importlib.metadata import version
from types import TracebackType
from typing import Self, cast
from unittest.mock import patch

import aiomqtt
import pytest
from aiomqtt.exceptions import MqttConnectError, MqttError

from custom_components.brilliant_mqtt import panel_ops
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


class _BlockedSuccessfulEntryClient(_FakeAioClient):
    def __init__(self) -> None:
        super().__init__()
        self.enter_started = asyncio.Event()
        self.release_enter = asyncio.Event()
        self.exit_started = asyncio.Event()
        self.release_exit = asyncio.Event()

    async def __aenter__(self) -> Self:
        self.enter_count += 1
        self.enter_started.set()
        await self.release_enter.wait()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.exit_count += 1
        self.exit_types.append(exc_type)
        self.exit_started.set()
        await self.release_exit.wait()


class _BlockedExitClient(_FakeAioClient):
    def __init__(self) -> None:
        super().__init__()
        self.exit_started = asyncio.Event()
        self.release_exit = asyncio.Event()
        self.exit_errors: list[BaseException | None] = []

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.exit_count += 1
        self.exit_types.append(exc_type)
        self.exit_errors.append(exc)
        self.exit_started.set()
        await self.release_exit.wait()


def _exception_chain(error: BaseException) -> list[BaseException]:
    chain: list[BaseException] = []
    current: BaseException | None = error
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        chain.append(current)
        current = current.__cause__ or current.__context__
    return chain


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


def test_panel_release_settings_render_without_exporting_individual_secrets() -> None:
    profile = _direct_profile()

    settings = profile.panel_release_settings(
        panel="office",
        mesh_priority=1,
        scene_bridge_enabled=False,
    )

    parsed = panel_ops.parse_env(settings.environment)
    assert parsed["MQTT_USERNAME"] == USERNAME
    assert parsed["MQTT_PASSWORD"] == PASSWORD
    assert parsed["BRILLIANT_PANEL"] == "office"
    assert parsed["MESH_PRIORITY"] == "1"
    assert settings.mqtt_ca is None
    assert settings.mqtt_ca_path is None
    assert repr(settings) == "PanelBrokerReleaseSettings(<redacted>)"
    assert USERNAME not in repr(settings)
    assert PASSWORD not in repr(settings)


def test_panel_release_settings_binds_candidate_deployment_id() -> None:
    profile = _direct_profile()
    deployment_id = "1234567812344abc8def1234567890ab"

    settings = profile.panel_release_settings(
        panel="office",
        mesh_priority=1,
        scene_bridge_enabled=False,
        deployment_id=deployment_id,
    )

    assert settings.deployment_id == deployment_id
    assert panel_ops.parse_env(settings.environment)["BRILLIANT_DEPLOYMENT_ID"] == deployment_id


def test_panel_release_settings_bind_custom_ca_to_immutable_path() -> None:
    profile = _direct_profile(
        tls_enabled=True,
        _ca_pem_value=CA_PEM,
    )
    expected_path = (
        "/var/brilliant-mqtt/releases/0.7.0--1234567812344abc8def1234567890ab/mqtt-ca.pem"
    )

    settings = profile.panel_release_settings(
        panel="office",
        mesh_priority=2,
        scene_bridge_enabled=True,
        mqtt_ca_path=expected_path,
    )

    expected_ca = CA_PEM.encode()
    assert settings.mqtt_ca == expected_ca
    assert settings.mqtt_ca_path == expected_path
    parsed = panel_ops.parse_env(settings.environment)
    assert parsed["MQTT_TLS_ENABLED"] == "1"
    assert parsed["MQTT_TLS_CA_FILE"] == expected_path
    assert parsed["SCENE_BRIDGE_ENABLED"] == "1"


def test_panel_release_settings_requires_path_only_for_custom_ca() -> None:
    custom = _direct_profile(tls_enabled=True, _ca_pem_value=CA_PEM)
    default_trust = _direct_profile(tls_enabled=True)
    release_ca = "/var/brilliant-mqtt/releases/0.7.0--1234567812344abc8def1234567890ab/mqtt-ca.pem"

    with pytest.raises(OperationError) as missing:
        custom.panel_release_settings(
            panel="office",
            mesh_priority=2,
            scene_bridge_enabled=True,
        )
    with pytest.raises(OperationError) as unexpected:
        default_trust.panel_release_settings(
            panel="office",
            mesh_priority=2,
            scene_bridge_enabled=True,
            mqtt_ca_path=release_ca,
        )

    assert missing.value.code == "invalid_broker_profile"
    assert unexpected.value.code == "invalid_broker_profile"


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


def test_profile_equality_supports_equal_and_unequal_unicode_credentials() -> None:
    unicode_credentials = _profile_data(
        mqtt_username="brýån-用户",
        mqtt_password="påssword-🔒",
    )
    profile = BrokerProfile.from_mapping(unicode_credentials)
    equal_profile = BrokerProfile.from_mapping(unicode_credentials)
    different_username = BrokerProfile.from_mapping(
        {
            **unicode_credentials,
            "mqtt_username": "brýån-別",
        }
    )
    different_password = BrokerProfile.from_mapping(
        {
            **unicode_credentials,
            "mqtt_password": "påssword-🔓",
        }
    )

    assert profile == equal_profile
    assert profile != different_username
    assert profile != different_password


def test_profile_equality_supports_lone_surrogate_credentials() -> None:
    surrogate_credentials = _profile_data(
        mqtt_username="brilliant-\ud800",
        mqtt_password="password-\udfff",
    )
    profile = BrokerProfile.from_mapping(surrogate_credentials)
    equal_profile = BrokerProfile.from_mapping(surrogate_credentials)
    different_username = BrokerProfile.from_mapping(
        {
            **surrogate_credentials,
            "mqtt_username": "brilliant-\ud801",
        }
    )
    different_password = BrokerProfile.from_mapping(
        {
            **surrogate_credentials,
            "mqtt_password": "password-\udffe",
        }
    )

    assert profile == equal_profile
    assert profile != different_username
    assert profile != different_password


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
async def test_repeated_cancellation_during_exit_preserves_the_body_cancellation() -> None:
    raw_client = _BlockedExitClient()
    profile = BrokerProfile.from_mapping(_profile_data())
    entered = asyncio.Event()

    async def run_until_cancelled() -> None:
        async with profile.device_client("repeat-cancel-close"):
            entered.set()
            await asyncio.Future()

    with patch("custom_components.brilliant_mqtt.broker.aiomqtt.Client", return_value=raw_client):
        task = asyncio.create_task(run_until_cancelled())
        await entered.wait()
        task.cancel("original-body-cancellation")
        await raw_client.exit_started.wait()
        assert len(raw_client.exit_errors) == 1
        original_cancellation = raw_client.exit_errors[0]
        assert isinstance(original_cancellation, asyncio.CancelledError)

        task.cancel("repeated-exit-cancellation")
        await asyncio.sleep(0)
        assert not task.done()

        raw_client.release_exit.set()
        with pytest.raises(asyncio.CancelledError) as caught:
            await task

    assert caught.value is original_cancellation
    assert caught.value.args == ("original-body-cancellation",)
    assert raw_client.exit_count == 1
    assert raw_client.exit_types == [asyncio.CancelledError]
    assert all(
        candidate.done()
        for candidate in asyncio.all_tasks()
        if candidate.get_name() == "brilliant-mqtt-device-exit"
    )


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
async def test_context_entry_maps_broker_failures_without_unowned_cleanup(
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
    assert raw_client.exit_count == 0
    assert raw_client.exit_types == []
    for secret in (
        "mqtt.secret.example",
        "password-secret",
        "CA-PEM-SECRET",
    ):
        assert secret not in str(caught.value)
        assert secret not in repr(caught.value)
        assert secret not in repr(caught.value.redacted_dict())


@pytest.mark.asyncio
async def test_real_aiomqtt_wrapped_tls_error_maps_without_message_parsing() -> None:
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
    assert caught.value.code == "broker_tls_verification_failed"
    assert caught.value.retryable is False
    assert caught.value.__context__ is None
    assert caught.value.__cause__ is None


@pytest.mark.asyncio
async def test_cancelling_real_aiomqtt_blocked_connect_never_disconnects_concurrently() -> None:
    profile = BrokerProfile.from_mapping(_profile_data())
    connect_started = threading.Event()
    release_connect = threading.Event()
    connect_finished = threading.Event()
    disconnect_attempted = threading.Event()
    raw_clients: list[aiomqtt.Client] = []

    def blocked_connect(raw_client: aiomqtt.Client) -> None:
        raw_clients.append(raw_client)
        connect_started.set()
        release_connect.wait(timeout=5)
        raw_client._loop.call_soon_threadsafe(  # noqa: SLF001
            raw_client._connected.set_exception,  # noqa: SLF001
            MqttError("blocked-connect-secret"),
        )
        connect_finished.set()

    def reject_concurrent_disconnect(*_args: object, **_kwargs: object) -> None:
        disconnect_attempted.set()
        if not connect_finished.is_set():
            raise RuntimeError("disconnect raced blocked connect")

    async def enter_client() -> None:
        async with profile.device_client("blocked-connect-cancellation"):
            pytest.fail("a cancelled entry must not yield a client")

    with (
        patch.object(
            aiomqtt.Client,
            "_client_connect",
            autospec=True,
            side_effect=blocked_connect,
        ),
        patch(
            "paho.mqtt.client.Client.disconnect",
            autospec=True,
            side_effect=reject_concurrent_disconnect,
        ),
    ):
        task = asyncio.create_task(enter_client())
        try:
            assert await asyncio.to_thread(connect_started.wait, 1)
            task.cancel("caller-cancelled")
            await asyncio.sleep(0)
            assert not task.done()
            release_connect.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(asyncio.shield(task), timeout=2)
            assert task.done()
            assert not disconnect_attempted.is_set()
        finally:
            release_connect.set()
            assert await asyncio.to_thread(connect_finished.wait, 1)

    assert len(raw_clients) == 1
    assert not raw_clients[0]._lock.locked()  # noqa: SLF001
    assert all(
        candidate.done()
        for candidate in asyncio.all_tasks()
        if candidate.get_name() == "brilliant-mqtt-device-enter"
    )


@pytest.mark.asyncio
async def test_cancellation_waits_for_successful_entry_and_exactly_one_exit() -> None:
    raw_client = _BlockedSuccessfulEntryClient()
    profile = BrokerProfile.from_mapping(_profile_data())

    async def enter_client() -> None:
        async with profile.device_client("blocked-successful-entry"):
            pytest.fail("a cancelled entry must not yield a client")

    with patch("custom_components.brilliant_mqtt.broker.aiomqtt.Client", return_value=raw_client):
        task = asyncio.create_task(enter_client())
        await raw_client.enter_started.wait()
        task.cancel("original-cancellation")
        await asyncio.sleep(0)
        assert not task.done()

        raw_client.release_enter.set()
        await raw_client.exit_started.wait()
        task.cancel("repeated-cancellation")
        await asyncio.sleep(0)
        assert not task.done()

        raw_client.release_exit.set()
        with pytest.raises(asyncio.CancelledError) as caught:
            await task

    assert caught.value.args == ("original-cancellation",)
    assert raw_client.enter_count == 1
    assert raw_client.exit_count == 1
    assert raw_client.exit_types == [asyncio.CancelledError]
    assert all(
        candidate.done()
        for candidate in asyncio.all_tasks()
        if candidate.get_name() in {"brilliant-mqtt-device-enter", "brilliant-mqtt-device-exit"}
    )


@pytest.mark.asyncio
async def test_context_rejects_active_and_closed_reentry_with_stable_error() -> None:
    raw_client = _FakeAioClient()
    profile = BrokerProfile.from_mapping(_profile_data())

    with patch("custom_components.brilliant_mqtt.broker.aiomqtt.Client", return_value=raw_client):
        context = profile.device_client("reject-reentry")
        await context.__aenter__()

        with pytest.raises(OperationError) as active_error:
            await context.__aenter__()

        await context.__aexit__(None, None, None)

        with pytest.raises(OperationError) as closed_error:
            await context.__aenter__()

    for error in (active_error.value, closed_error.value):
        assert error.stage is OperationStage.FLEET_AUTH
        assert error.code == "operation_failed"
        assert error.__context__ is None
        assert error.__cause__ is None
        assert _exception_chain(error) == [error]
    assert raw_client.enter_count == 1
    assert raw_client.exit_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "control_error",
    [
        asyncio.CancelledError("cancel-secret"),
        KeyboardInterrupt("keyboard-secret"),
        SystemExit("system-exit-secret"),
    ],
)
async def test_context_entry_preserves_control_exceptions_without_unowned_cleanup(
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
    assert raw_client.exit_count == 0
    assert raw_client.exit_types == []


@pytest.mark.asyncio
async def test_entry_failure_never_invokes_exit_owned_only_by_successful_entry() -> None:
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
    assert caught.value.cleanup_error is None
    assert caught.value.__context__ is None
    assert caught.value.__cause__ is None
    assert raw_client.exit_count == 0
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
    assert caught.value.__context__ is None
    assert caught.value.__cause__ is None
    assert _exception_chain(caught.value) == [caught.value]
    chain_surface = repr(_exception_chain(caught.value))
    assert CA_PEM not in chain_surface
    assert PASSWORD not in chain_surface


def test_aiomqtt_construction_failure_has_no_raw_exception_chain() -> None:
    profile = BrokerProfile.from_mapping(_profile_data())
    constructor_secret = "constructor-password-secret"

    with (
        patch(
            "custom_components.brilliant_mqtt.broker.aiomqtt.Client",
            side_effect=RuntimeError(constructor_secret),
        ),
        pytest.raises(OperationError) as caught,
    ):
        profile.device_client("constructor-error")

    assert caught.value.code == "invalid_broker_profile"
    assert caught.value.__context__ is None
    assert caught.value.__cause__ is None
    assert _exception_chain(caught.value) == [caught.value]
    assert constructor_secret not in repr(_exception_chain(caught.value))


@pytest.mark.parametrize("client_id", ["", "   ", 123, True])
def test_invalid_client_id_maps_to_stable_profile_error(client_id: object) -> None:
    profile = BrokerProfile.from_mapping(_profile_data())

    with pytest.raises(OperationError, match="invalid_broker_profile") as caught:
        profile.device_client(client_id)  # type: ignore[arg-type]

    assert caught.value.__context__ is None
    assert caught.value.__cause__ is None
    assert _exception_chain(caught.value) == [caught.value]
