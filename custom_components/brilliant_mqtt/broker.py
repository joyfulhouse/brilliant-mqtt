"""Normalized broker profiles and the temporary device-principal MQTT seam."""

from __future__ import annotations

import asyncio
import hmac
import ssl
from collections.abc import AsyncIterator, Mapping
from contextlib import AbstractAsyncContextManager
from dataclasses import InitVar, dataclass
from enum import StrEnum
from types import TracebackType
from typing import Any, Never, Protocol, Self, SupportsIndex

import aiomqtt

from .errors import OperationError, OperationStage, from_exception


class BrokerKind(StrEnum):
    """Operator guidance choice; never a runtime or security branch."""

    OFFICIAL_MOSQUITTO = "official_mosquitto"
    EXISTING_BROKER = "existing_broker"


@dataclass(frozen=True, slots=True)
class DeviceMqttMessage:
    """Broker-library-independent incoming MQTT message."""

    topic: str
    payload: bytes
    qos: int
    retain: bool


class DeviceMqttClient(Protocol):
    """Temporary device-principal operations consumed by Task 6."""

    @property
    def messages(self) -> AsyncIterator[DeviceMqttMessage]:
        """Yield normalized incoming messages."""
        ...

    async def subscribe(self, topic: str, qos: int = 0) -> None:
        """Subscribe and wait for broker acknowledgement."""
        ...

    async def publish(
        self,
        topic: str,
        payload: bytes | str | None = None,
        qos: int = 0,
        retain: bool = False,
    ) -> None:
        """Publish and wait for broker acknowledgement."""
        ...

    async def unsubscribe(self, topic: str) -> None:
        """Unsubscribe and wait for broker acknowledgement."""
        ...

    async def disconnect(self) -> None:
        """Close the temporary connection idempotently."""
        ...


class _SensitiveText:
    """Immutable text whose string, repr, and deepcopy surfaces stay redacted."""

    __slots__ = ("__value",)
    __value: str

    def __init__(self, value: str) -> None:
        object.__setattr__(self, "_SensitiveText__value", value)

    def __setattr__(self, name: str, value: object) -> Never:
        raise AttributeError("sensitive_text_is_immutable")

    def _reveal(self) -> str:
        return self.__value

    def __repr__(self) -> str:
        return "<redacted>"

    def __str__(self) -> str:
        return "<redacted>"

    def __copy__(self) -> Self:
        return self

    def __deepcopy__(self, memo: dict[int, Any]) -> Self:
        return self

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _SensitiveText) and hmac.compare_digest(
            self.__value.encode("utf-8", errors="surrogatepass"),
            other.__value.encode("utf-8", errors="surrogatepass"),
        )

    __hash__ = None  # type: ignore[assignment]

    def __reduce__(self) -> Never:
        raise TypeError("sensitive_text_not_serializable")

    def __reduce_ex__(self, protocol: SupportsIndex) -> Never:
        raise TypeError("sensitive_text_not_serializable")


class _BrokerSecrets:
    """Non-dataclass slots keep raw connection material out of asdict()."""

    __slots__ = ("_ca_pem", "_password", "_username")

    _username: _SensitiveText
    _password: _SensitiveText
    _ca_pem: _SensitiveText | None


@dataclass(frozen=True, slots=True, eq=False)
class BrokerProfile(_BrokerSecrets):
    """One immutable, normalized MQTT connection profile."""

    kind: BrokerKind
    host: str
    port: int
    tls_enabled: bool
    _username_value: InitVar[str]
    _password_value: InitVar[str]
    _ca_pem_value: InitVar[str | None]

    __hash__ = None  # type: ignore[assignment]

    def __post_init__(
        self,
        _username_value: str,
        _password_value: str,
        _ca_pem_value: str | None,
    ) -> None:
        if not isinstance(self.kind, BrokerKind):
            raise ValueError("invalid_broker_profile")
        if not isinstance(self.host, str):
            raise ValueError("invalid_broker_profile")
        normalized_host = self.host.strip().lower()
        if not normalized_host:
            raise ValueError("invalid_broker_profile")
        if type(self.port) is not int or not 1 <= self.port <= 65535:
            raise ValueError("invalid_broker_profile")
        if type(self.tls_enabled) is not bool:
            raise ValueError("invalid_broker_profile")
        if not isinstance(_username_value, str) or not _username_value:
            raise ValueError("invalid_broker_profile")
        if not isinstance(_password_value, str) or not _password_value:
            raise ValueError("invalid_broker_profile")
        if _ca_pem_value is not None and (
            not isinstance(_ca_pem_value, str)
            or not _ca_pem_value.strip()
            or not self.tls_enabled
            or "-----BEGIN CERTIFICATE-----" not in _ca_pem_value
            or "-----END CERTIFICATE-----" not in _ca_pem_value
            or "PRIVATE KEY-----" in _ca_pem_value.upper()
        ):
            raise ValueError("invalid_broker_profile")

        object.__setattr__(self, "host", normalized_host)
        object.__setattr__(self, "_username", _SensitiveText(_username_value))
        object.__setattr__(self, "_password", _SensitiveText(_password_value))
        object.__setattr__(
            self,
            "_ca_pem",
            _SensitiveText(_ca_pem_value) if _ca_pem_value is not None else None,
        )

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any]) -> BrokerProfile:
        """Normalize persisted fleet keys or raise one stable public error."""
        try:
            raw_kind = data["broker_kind"]
            if not isinstance(raw_kind, str):
                raise ValueError
            if raw_kind == BrokerKind.OFFICIAL_MOSQUITTO.value:
                kind = BrokerKind.OFFICIAL_MOSQUITTO
            elif raw_kind == BrokerKind.EXISTING_BROKER.value:
                kind = BrokerKind.EXISTING_BROKER
            else:
                raise ValueError

            raw_host = data["mqtt_host"]
            if not isinstance(raw_host, str):
                raise ValueError
            host = raw_host.strip().lower()
            if not host:
                raise ValueError

            raw_tls_enabled = data.get("mqtt_tls_enabled", False)
            if type(raw_tls_enabled) is not bool:
                raise ValueError
            tls_enabled = raw_tls_enabled

            if "mqtt_port" in data:
                raw_port = data["mqtt_port"]
                if type(raw_port) is not int or not 1 <= raw_port <= 65535:
                    raise ValueError
                port = raw_port
            else:
                port = 8883 if tls_enabled else 1883

            raw_username = data["mqtt_username"]
            if not isinstance(raw_username, str) or not raw_username:
                raise ValueError

            raw_password = data["mqtt_password"]
            if not isinstance(raw_password, str) or not raw_password:
                raise ValueError

            raw_ca_pem = data.get("mqtt_tls_ca")
            if raw_ca_pem is not None:
                if (
                    not isinstance(raw_ca_pem, str)
                    or not raw_ca_pem.strip()
                    or not tls_enabled
                    or "-----BEGIN CERTIFICATE-----" not in raw_ca_pem
                    or "-----END CERTIFICATE-----" not in raw_ca_pem
                    or "PRIVATE KEY-----" in raw_ca_pem.upper()
                ):
                    raise ValueError

            profile = cls(
                kind=kind,
                host=host,
                port=port,
                tls_enabled=tls_enabled,
                _username_value=raw_username,
                _password_value=raw_password,
                _ca_pem_value=raw_ca_pem if isinstance(raw_ca_pem, str) else None,
            )
        except Exception:
            profile = None
        if profile is None:
            raise OperationError.for_code(
                OperationStage.BROKER_PROFILE,
                "invalid_broker_profile",
            ) from None
        return profile

    def __eq__(self, other: object) -> bool:
        if other.__class__ is not self.__class__:
            return NotImplemented
        assert isinstance(other, BrokerProfile)
        secrets_equal = (
            (self._username == other._username)
            & (self._password == other._password)
            & (self._ca_pem == other._ca_pem)
        )
        return (
            self.kind,
            self.host,
            self.port,
            self.tls_enabled,
        ) == (
            other.kind,
            other.host,
            other.port,
            other.tls_enabled,
        ) and bool(secrets_equal)

    def __copy__(self) -> Self:
        return self

    def __deepcopy__(self, memo: dict[int, Any]) -> Self:
        return self

    def __reduce__(self) -> Never:
        raise TypeError("broker_profile_not_serializable")

    def __reduce_ex__(self, protocol: SupportsIndex) -> Never:
        raise TypeError("broker_profile_not_serializable")

    @property
    def has_custom_ca(self) -> bool:
        """Whether this profile supplies a custom public trust root."""
        return self._ca_pem is not None

    @property
    def username_configured(self) -> bool:
        """Whether credentials are configured, without returning their value."""
        return True

    def redacted_dict(self) -> dict[str, str | int | bool]:
        """Return the exact public diagnostic surface."""
        return {
            "kind": self.kind.value,
            "host": self.host,
            "port": self.port,
            "tls_enabled": self.tls_enabled,
            "has_custom_ca": self.has_custom_ca,
            "username_configured": self.username_configured,
        }

    def device_client(
        self,
        client_id: str,
    ) -> AbstractAsyncContextManager[DeviceMqttClient]:
        """Build one strict temporary MQTT 3.1.1 device-principal client."""
        raw_client: aiomqtt.Client | None = None
        construction_failed = False
        try:
            if not isinstance(client_id, str) or not client_id.strip():
                raise ValueError
            tls_context = self._build_tls_context()
            raw_client = aiomqtt.Client(
                hostname=self.host,
                port=self.port,
                username=self._username._reveal(),
                password=self._password._reveal(),
                identifier=client_id,
                protocol=aiomqtt.ProtocolVersion.V311,
                will=None,
                tls_context=tls_context,
            )
        except Exception:
            construction_failed = True
        if construction_failed:
            raise OperationError.for_code(
                OperationStage.BROKER_PROFILE,
                "invalid_broker_profile",
            ) from None
        assert raw_client is not None
        return _DeviceClientContext(raw_client)

    def _build_tls_context(self) -> ssl.SSLContext | None:
        if not self.tls_enabled:
            return None
        if self._ca_pem is None:
            context = ssl.create_default_context()
        else:
            context = ssl.create_default_context(cadata=self._ca_pem._reveal())
        context.verify_mode = ssl.CERT_REQUIRED
        context.check_hostname = True
        return context


class _DeviceClientContext(AbstractAsyncContextManager[DeviceMqttClient]):
    def __init__(self, raw_client: aiomqtt.Client) -> None:
        self._raw_client = raw_client
        self._entry_started = False
        self._entered = False
        self._closed = False
        self._client = _AioMqttDeviceClient(raw_client, self)

    async def __aenter__(self) -> DeviceMqttClient:
        if self._entry_started or self._closed:
            raise OperationError.for_code(
                OperationStage.FLEET_AUTH,
                "operation_failed",
            ) from None
        self._entry_started = True

        entry_task = asyncio.create_task(
            self._attempt_raw_enter(),
            name="brilliant-mqtt-device-enter",
        )
        entry_error, cancellation = await _settle_lifecycle_task(entry_task)

        if cancellation is not None:
            if entry_error is None:
                self._entered = True
                exit_task = asyncio.create_task(
                    self._attempt_raw_exit(
                        type(cancellation),
                        cancellation,
                        cancellation.__traceback__,
                    ),
                    name="brilliant-mqtt-device-exit",
                )
                await _settle_lifecycle_task(exit_task)
            else:
                self._closed = True
            raise cancellation from None

        if entry_error is not None:
            self._closed = True
            if not isinstance(entry_error, Exception):
                raise entry_error

            mapped = from_exception(OperationStage.FLEET_AUTH, entry_error)
            raise mapped from None

        self._entered = True
        return self._client

    async def _attempt_raw_enter(self) -> BaseException | None:
        try:
            await self._raw_client.__aenter__()
        except BaseException as error:
            return error
        return None

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self._close(exc_type, exc, traceback)

    async def _close(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if not self._entered or self._closed:
            return
        exit_task = asyncio.create_task(
            self._attempt_raw_exit(exc_type, exc, traceback),
            name="brilliant-mqtt-device-exit",
        )
        cleanup_error, cancellation = await _settle_lifecycle_task(exit_task)
        if exc is not None and not isinstance(exc, Exception):
            return
        if cancellation is not None:
            raise cancellation from None
        if cleanup_error is None:
            return

        if not isinstance(cleanup_error, Exception):
            raise cleanup_error
        cleanup = _cleanup_failure()
        if isinstance(exc, OperationError):
            raise exc.with_cleanup_error(cleanup) from None
        if exc is not None:
            return
        raise cleanup from None

    async def _attempt_raw_exit(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> BaseException | None:
        if not self._entered or self._closed:
            return None
        self._closed = True
        cleanup_error: BaseException | None
        try:
            await self._raw_client.__aexit__(exc_type, exc, traceback)
        except BaseException as error:
            cleanup_error = error
        else:
            cleanup_error = None
        return cleanup_error


class _AioMqttDeviceClient:
    def __init__(
        self,
        raw_client: aiomqtt.Client,
        lifecycle: _DeviceClientContext,
    ) -> None:
        self._raw_client = raw_client
        self._lifecycle = lifecycle

    @property
    def messages(self) -> AsyncIterator[DeviceMqttMessage]:
        return self._normalized_messages()

    async def _normalized_messages(self) -> AsyncIterator[DeviceMqttMessage]:
        async for message in self._raw_client.messages:
            yield DeviceMqttMessage(
                topic=str(message.topic),
                payload=message.payload,
                qos=message.qos,
                retain=message.retain,
            )

    async def subscribe(self, topic: str, qos: int = 0) -> None:
        await self._raw_client.subscribe(topic, qos=qos)

    async def publish(
        self,
        topic: str,
        payload: bytes | str | None = None,
        qos: int = 0,
        retain: bool = False,
    ) -> None:
        await self._raw_client.publish(topic, payload, qos=qos, retain=retain)

    async def unsubscribe(self, topic: str) -> None:
        await self._raw_client.unsubscribe(topic)

    async def disconnect(self) -> None:
        await self._lifecycle._close(None, None, None)


def _cleanup_failure() -> OperationError:
    return OperationError.for_code(OperationStage.CLEANUP, "cleanup_failed")


async def _settle_lifecycle_task(
    task: asyncio.Task[BaseException | None],
) -> tuple[BaseException | None, asyncio.CancelledError | None]:
    """Wait for MQTT lifecycle I/O without cancelling it with its caller."""
    cancellation: asyncio.CancelledError | None = None
    while True:
        try:
            lifecycle_error = await asyncio.shield(task)
        except asyncio.CancelledError as error:
            if task.cancelled():
                return error, cancellation
            if cancellation is None:
                cancellation = error
            continue
        except BaseException as error:
            return error, cancellation
        return lifecycle_error, cancellation
