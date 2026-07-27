"""Live contracts for MQTT validation against disposable Mosquitto brokers."""

from __future__ import annotations

import asyncio
import inspect
import os
import ssl
from collections.abc import AsyncIterator, Callable, Coroutine, Iterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from time import monotonic
from typing import Any, cast
from urllib.parse import SplitResult, urlsplit
from uuid import uuid4

import aiomqtt
import pytest
import pytest_socket  # type: ignore[import-untyped]
from homeassistant.components import mqtt
from homeassistant.components.mqtt.models import ReceiveMessage
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.brilliant_mqtt.broker import BrokerKind, BrokerProfile
from custom_components.brilliant_mqtt.broker_validation import BrokerValidator
from custom_components.brilliant_mqtt.errors import OperationError, OperationStage

_BROKER_SENTINEL = "BRILLIANT_MQTT_TEST_BROKER_URL"
_VALIDATION_TIMEOUT_SECONDS = 2.0
_skip_without_live_broker = pytest.mark.skipif(
    not os.environ.get(_BROKER_SENTINEL),
    reason=f"{_BROKER_SENTINEL} is not configured",
)

_MessageCallback = Callable[
    [ReceiveMessage],
    Coroutine[Any, Any, None] | None,
]


@pytest.fixture
def _allow_live_loopback(socket_enabled: None) -> Iterator[None]:
    """Restore both loopback families after the HA harness installs its guard."""
    pytest_socket.socket_allow_hosts(["localhost", "127.0.0.1"])
    yield


@dataclass(frozen=True, slots=True)
class _BrokerEndpoint:
    host: str
    port: int
    tls_enabled: bool


@dataclass(frozen=True, slots=True)
class _LiveEnvironment:
    plain: _BrokerEndpoint
    tls: _BrokerEndpoint
    deny: _BrokerEndpoint
    mismatch: _BrokerEndpoint
    ca_file: Path
    ca_pem: str
    ha_username: str
    ha_password: str
    device_username: str
    device_password: str

    @classmethod
    def from_environ(cls) -> _LiveEnvironment:
        """Load the strict, credential-free live-test environment contract."""
        ca_file = Path(_required_env("BRILLIANT_MQTT_TEST_CA_FILE"))
        if not ca_file.is_absolute() or not ca_file.is_file():
            raise ValueError("invalid_live_mqtt_ca_file")
        return cls(
            plain=_parse_broker_url(_required_env(_BROKER_SENTINEL), "mqtt"),
            tls=_parse_broker_url(
                _required_env("BRILLIANT_MQTT_TEST_TLS_BROKER_URL"),
                "mqtts",
            ),
            deny=_parse_broker_url(
                _required_env("BRILLIANT_MQTT_TEST_DENY_BROKER_URL"),
                "mqtt",
            ),
            mismatch=_parse_broker_url(
                _required_env("BRILLIANT_MQTT_TEST_MISMATCH_BROKER_URL"),
                "mqtt",
            ),
            ca_file=ca_file,
            ca_pem=ca_file.read_text(encoding="utf-8"),
            ha_username=_required_env("BRILLIANT_MQTT_TEST_HA_USERNAME"),
            ha_password=_required_env("BRILLIANT_MQTT_TEST_HA_PASSWORD"),
            device_username=_required_env("BRILLIANT_MQTT_TEST_DEVICE_USERNAME"),
            device_password=_required_env("BRILLIANT_MQTT_TEST_DEVICE_PASSWORD"),
        )


@dataclass(slots=True)
class _Subscription:
    topic: str
    callback: _MessageCallback
    qos: int


@dataclass(slots=True)
class _RealHaMqttSeam:
    """Patch the validator's HA APIs onto one real aiomqtt connection."""

    client: aiomqtt.Client
    subscriptions: dict[str, list[_Subscription]] = field(default_factory=dict)
    subscribe_done: dict[tuple[str, int], list[Callable[[], None]]] = field(default_factory=dict)
    unsubscribe_tasks: list[asyncio.Task[None]] = field(default_factory=list)

    async def async_wait_for_mqtt_client(self, hass: HomeAssistant) -> bool:
        return True

    def is_connected(self, hass: HomeAssistant) -> bool:
        return True

    def async_on_subscribe_done(
        self,
        hass: HomeAssistant,
        topic: str,
        qos: int,
        callback: Callable[[], None],
    ) -> Callable[[], None]:
        key = (topic, qos)
        callbacks = self.subscribe_done.setdefault(key, [])
        callbacks.append(callback)

        def remove() -> None:
            current = self.subscribe_done.get(key)
            if current is None or callback not in current:
                return
            current.remove(callback)
            if not current:
                self.subscribe_done.pop(key, None)

        return remove

    async def async_subscribe(
        self,
        hass: HomeAssistant,
        topic: str,
        callback: _MessageCallback,
        qos: int = 0,
        encoding: str | None = "utf-8",
    ) -> Callable[[], None]:
        subscription = _Subscription(topic, callback, qos)
        self.subscriptions.setdefault(topic, []).append(subscription)
        try:
            granted = await self.client.subscribe(topic, qos=qos)
            if any(_suback_failed(reason) for reason in granted):
                raise RuntimeError("live_mqtt_subscribe_denied")
        except BaseException:
            self._remove_local_subscription(subscription)
            raise

        for done_callback in tuple(self.subscribe_done.get((topic, qos), ())):
            done_callback()

        active = True

        def unsubscribe() -> None:
            nonlocal active
            if not active:
                return
            active = False
            if not self._remove_local_subscription(subscription):
                return
            self.unsubscribe_tasks.append(
                asyncio.create_task(
                    self.client.unsubscribe(topic),
                    name=f"brilliant-mqtt-live-ha-unsubscribe-{len(self.unsubscribe_tasks)}",
                )
            )

        return unsubscribe

    async def async_publish(
        self,
        hass: HomeAssistant,
        topic: str,
        payload: bytes | str | None,
        qos: int = 0,
        retain: bool = False,
        encoding: str | None = "utf-8",
        *,
        message_expiry_interval: int | None = None,
    ) -> None:
        if message_expiry_interval is not None:
            raise ValueError("live_mqtt_message_expiry_not_supported")
        await self.client.publish(topic, payload, qos=qos, retain=retain)

    async def dispatch_messages(self) -> None:
        async for message in self.client.messages:
            topic = str(message.topic)
            for subscribed_topic, subscriptions in tuple(self.subscriptions.items()):
                if not message.topic.matches(subscribed_topic):
                    continue
                for subscription in tuple(subscriptions):
                    receive_message = ReceiveMessage(
                        topic=topic,
                        payload=message.payload,
                        qos=int(message.qos),
                        retain=bool(message.retain),
                        subscribed_topic=subscribed_topic,
                        timestamp=monotonic(),
                    )
                    callback_result = subscription.callback(receive_message)
                    if inspect.isawaitable(callback_result):
                        await callback_result

    def schedule_remaining_unsubscribes(self) -> None:
        for topic in tuple(self.subscriptions):
            self.subscriptions.pop(topic, None)
            self.unsubscribe_tasks.append(
                asyncio.create_task(
                    self.client.unsubscribe(topic),
                    name=f"brilliant-mqtt-live-ha-unsubscribe-{len(self.unsubscribe_tasks)}",
                )
            )

    async def settle_unsubscribes(self) -> list[BaseException]:
        if not self.unsubscribe_tasks:
            return []
        outcomes = await asyncio.gather(
            *self.unsubscribe_tasks,
            return_exceptions=True,
        )
        return [outcome for outcome in outcomes if isinstance(outcome, BaseException)]

    def _remove_local_subscription(self, subscription: _Subscription) -> bool:
        current = self.subscriptions.get(subscription.topic)
        if current is None or subscription not in current:
            return False
        current.remove(subscription)
        if current:
            return False
        self.subscriptions.pop(subscription.topic, None)
        return True


@asynccontextmanager
async def _real_ha_mqtt(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
    endpoint: _BrokerEndpoint,
    environment: _LiveEnvironment,
) -> AsyncIterator[None]:
    """Expose one real HA-principal client through the validator's public seam."""
    if hass.config_entries.async_entries("mqtt"):
        raise AssertionError("live_mqtt_test_requires_no_existing_mqtt_entries")
    entry = MockConfigEntry(
        domain="mqtt",
        data={"discovery_prefix": "homeassistant"},
        state=ConfigEntryState.LOADED,
    )

    tls_context: ssl.SSLContext | None = None
    if endpoint.tls_enabled:
        tls_context = ssl.create_default_context(cafile=str(environment.ca_file))
        tls_context.verify_mode = ssl.CERT_REQUIRED
        tls_context.check_hostname = True
    client = aiomqtt.Client(
        hostname=endpoint.host,
        port=endpoint.port,
        username=environment.ha_username,
        password=environment.ha_password,
        identifier=f"brilliant-mqtt-live-ha-{uuid4()}",
        protocol=aiomqtt.ProtocolVersion.V5,
        tls_context=tls_context,
    )
    entry_added = False
    seam = _RealHaMqttSeam(client)
    reader_task: asyncio.Task[None] | None = None
    entered = False
    primary: BaseException | None = None
    try:
        entry.add_to_hass(hass)
        entry_added = True
        entry_task = asyncio.create_task(
            _attempt_ha_client_entry(client),
            name="brilliant-mqtt-live-ha-enter",
        )
        entry_error, entry_cancellation = await _settle_ha_client_entry(entry_task)
        if entry_error is None:
            entered = True
        if entry_cancellation is not None:
            primary = entry_cancellation
        elif entry_error is not None:
            primary = entry_error
        else:
            reader_task = asyncio.create_task(
                seam.dispatch_messages(),
                name="brilliant-mqtt-live-ha-reader",
            )
            with monkeypatch.context() as seam_patch:
                seam_patch.setattr(
                    mqtt,
                    "async_wait_for_mqtt_client",
                    seam.async_wait_for_mqtt_client,
                )
                seam_patch.setattr(mqtt, "is_connected", seam.is_connected)
                seam_patch.setattr(
                    mqtt,
                    "async_on_subscribe_done",
                    seam.async_on_subscribe_done,
                )
                seam_patch.setattr(mqtt, "async_subscribe", seam.async_subscribe)
                seam_patch.setattr(mqtt, "async_publish", seam.async_publish)
                try:
                    yield
                except BaseException as error:
                    primary = error
    except BaseException as error:
        if primary is None:
            primary = error
    finally:
        cleanup_errors: list[BaseException] = []
        if entered:
            try:
                seam.schedule_remaining_unsubscribes()
            except BaseException as error:
                cleanup_errors.append(error)
            try:
                cleanup_errors.extend(await seam.settle_unsubscribes())
            except BaseException as error:
                cleanup_errors.append(error)
        if reader_task is not None:
            reader_was_done = reader_task.done()
            if not reader_was_done:
                reader_task.cancel()
            try:
                await reader_task
            except asyncio.CancelledError as error:
                if reader_was_done:
                    cleanup_errors.append(error)
            except BaseException as error:
                cleanup_errors.append(error)
        if entered:
            try:
                await client.__aexit__(None, None, None)
            except BaseException as error:
                cleanup_errors.append(error)
        if entry_added:
            try:
                entry.mock_state(hass, ConfigEntryState.NOT_LOADED)
            except BaseException as error:
                cleanup_errors.append(error)
            try:
                removed = await hass.config_entries.async_remove(entry.entry_id)
                if not removed:
                    raise AssertionError("live_mqtt_config_entry_cleanup_failed")
            except BaseException as error:
                cleanup_errors.append(error)

    _raise_after_ha_seam_teardown(primary, cleanup_errors)


async def _attempt_ha_client_entry(client: aiomqtt.Client) -> BaseException | None:
    try:
        await client.__aenter__()
    except BaseException as error:
        return error
    return None


async def _settle_ha_client_entry(
    entry_task: asyncio.Task[BaseException | None],
) -> tuple[BaseException | None, asyncio.CancelledError | None]:
    """Let an in-flight connect settle before propagating caller cancellation."""
    cancellation: asyncio.CancelledError | None = None
    while True:
        try:
            entry_error = await asyncio.shield(entry_task)
        except asyncio.CancelledError as error:
            if entry_task.cancelled():
                return error, cancellation
            if cancellation is None:
                cancellation = error
            continue
        except BaseException as error:
            return error, cancellation
        return entry_error, cancellation


def _raise_after_ha_seam_teardown(
    primary: BaseException | None,
    cleanup_errors: list[BaseException],
) -> None:
    """Apply validator-style control/primary precedence after full settlement."""
    if primary is not None and not isinstance(primary, Exception):
        raise primary.with_traceback(primary.__traceback__) from None
    cleanup_control = next(
        (error for error in cleanup_errors if not isinstance(error, Exception)),
        None,
    )
    if cleanup_control is not None:
        raise cleanup_control.with_traceback(cleanup_control.__traceback__) from None
    if primary is not None:
        raise primary.with_traceback(primary.__traceback__) from None
    if cleanup_errors:
        cleanup_error = cleanup_errors[0]
        raise cleanup_error.with_traceback(cleanup_error.__traceback__) from None


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None or not value:
        raise ValueError(f"missing_live_mqtt_environment:{name}")
    return value


def _parse_broker_url(value: str, expected_scheme: str) -> _BrokerEndpoint:
    if (
        expected_scheme not in {"mqtt", "mqtts"}
        or "?" in value
        or "#" in value
        or any(ord(character) <= 0x20 or ord(character) == 0x7F for character in value)
    ):
        raise ValueError("invalid_live_mqtt_broker_url")
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        raise ValueError("invalid_live_mqtt_broker_url") from None
    if (
        parsed.scheme != expected_scheme
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path
        or parsed.query
        or parsed.fragment
        or parsed.hostname is None
        or port is None
        or not 1 <= port <= 65535
        or _has_unsupported_netloc(parsed)
    ):
        raise ValueError("invalid_live_mqtt_broker_url")
    return _BrokerEndpoint(
        host=parsed.hostname,
        port=port,
        tls_enabled=expected_scheme == "mqtts",
    )


def _has_unsupported_netloc(parsed: SplitResult) -> bool:
    """Reject userinfo even when malformed percent escapes obscure parsing."""
    return "@" in parsed.netloc


def _suback_failed(reason: int | object) -> bool:
    is_failure = getattr(reason, "is_failure", None)
    if isinstance(is_failure, bool):
        return is_failure
    return isinstance(reason, int) and reason >= 0x80


@pytest.mark.parametrize(
    ("value", "expected_scheme"),
    [
        ("mqtt://device:secret@127.0.0.1:1883", "mqtt"),
        ("mqtt://127.0.0.1:1883/path", "mqtt"),
        ("mqtt://127.0.0.1:1883/", "mqtt"),
        ("mqtt://127.0.0.1:1883?query=yes", "mqtt"),
        ("mqtt://127.0.0.1:1883#fragment", "mqtt"),
        ("mqtt://127.0.0.1", "mqtt"),
        ("ws://127.0.0.1:1883", "mqtt"),
        ("mqtt://127.0.0.1:1883", "mqtts"),
        (" mqtt://127.0.0.1:1883", "mqtt"),
        ("\tmqtt://127.0.0.1:1883", "mqtt"),
        ("mqtt://127.0.0.\n1:1883", "mqtt"),
        ("mqtt://127.0.0.1:1883\r\n", "mqtt"),
    ],
)
def test_live_broker_urls_reject_unsafe_or_incomplete_values(
    value: str,
    expected_scheme: str,
) -> None:
    with pytest.raises(ValueError, match="^invalid_live_mqtt_broker_url$"):
        _parse_broker_url(value, expected_scheme)


def _profile(
    endpoint: _BrokerEndpoint,
    environment: _LiveEnvironment,
    *,
    password: str | None = None,
    trust_tls_ca: bool = True,
) -> BrokerProfile:
    return BrokerProfile(
        kind=BrokerKind.EXISTING_BROKER,
        host=endpoint.host,
        port=endpoint.port,
        tls_enabled=endpoint.tls_enabled,
        _username_value=environment.device_username,
        _password_value=environment.device_password if password is None else password,
        _ca_pem_value=(environment.ca_pem if endpoint.tls_enabled and trust_tls_ca else None),
    )


def _device_client(
    profile: BrokerProfile,
    client_id: str,
) -> Any:
    return profile.device_client(client_id)


async def _validate_success(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
    endpoint: _BrokerEndpoint,
    environment: _LiveEnvironment,
) -> None:
    async with _real_ha_mqtt(hass, monkeypatch, endpoint, environment):
        result = await BrokerValidator(
            hass,
            _device_client,
            timeout_seconds=_VALIDATION_TIMEOUT_SECONDS,
        ).async_validate(_profile(endpoint, environment))

    assert result.completed_stages == tuple(OperationStage)[1:]


@pytest.mark.mqtt_live
@pytest.mark.usefixtures("_allow_live_loopback")
@_skip_without_live_broker
async def test_plaintext_validation_succeeds(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment = _LiveEnvironment.from_environ()
    await _validate_success(hass, monkeypatch, environment.plain, environment)


@pytest.mark.mqtt_live
@pytest.mark.usefixtures("_allow_live_loopback")
@_skip_without_live_broker
async def test_ca_trusted_tls_validation_succeeds(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment = _LiveEnvironment.from_environ()
    await _validate_success(hass, monkeypatch, environment.tls, environment)


@pytest.mark.mqtt_live
@pytest.mark.usefixtures("_allow_live_loopback")
@_skip_without_live_broker
async def test_bad_device_password_preserves_authentication_failure(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment = _LiveEnvironment.from_environ()
    wrong_password = f"{environment.device_password}-guaranteed-wrong"
    async with _real_ha_mqtt(hass, monkeypatch, environment.plain, environment):
        with pytest.raises(OperationError) as raised:
            await BrokerValidator(
                hass,
                _device_client,
                timeout_seconds=_VALIDATION_TIMEOUT_SECONDS,
            ).async_validate(
                _profile(
                    environment.plain,
                    environment,
                    password=wrong_password,
                )
            )

    assert raised.value.stage is OperationStage.FLEET_AUTH
    assert raised.value.code == "broker_authentication_failed"


@pytest.mark.mqtt_live
@pytest.mark.usefixtures("_allow_live_loopback")
@_skip_without_live_broker
async def test_untrusted_ca_tls_preserves_verification_failure(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment = _LiveEnvironment.from_environ()
    async with _real_ha_mqtt(hass, monkeypatch, environment.tls, environment):
        with pytest.raises(OperationError) as raised:
            await BrokerValidator(
                hass,
                _device_client,
                timeout_seconds=_VALIDATION_TIMEOUT_SECONDS,
            ).async_validate(
                _profile(
                    environment.tls,
                    environment,
                    trust_tls_ca=False,
                )
            )

    assert raised.value.stage is OperationStage.FLEET_AUTH
    assert raised.value.code == "broker_tls_verification_failed"


@pytest.mark.mqtt_live
@pytest.mark.usefixtures("_allow_live_loopback")
@_skip_without_live_broker
async def test_discovery_write_acl_denial_maps_to_typed_failure(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment = _LiveEnvironment.from_environ()
    async with _real_ha_mqtt(hass, monkeypatch, environment.deny, environment):
        with pytest.raises(OperationError) as raised:
            await BrokerValidator(
                hass,
                _device_client,
                timeout_seconds=_VALIDATION_TIMEOUT_SECONDS,
            ).async_validate(_profile(environment.deny, environment))

    assert raised.value.stage is OperationStage.DISCOVERY_WRITE
    assert raised.value.code == "discovery_write_denied"


@pytest.mark.mqtt_live
@pytest.mark.usefixtures("_allow_live_loopback")
@_skip_without_live_broker
async def test_broker_mismatch_maps_to_panel_to_ha_guidance(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    environment = _LiveEnvironment.from_environ()
    async with _real_ha_mqtt(hass, monkeypatch, environment.plain, environment):
        with pytest.raises(OperationError) as raised:
            await BrokerValidator(
                hass,
                _device_client,
                timeout_seconds=_VALIDATION_TIMEOUT_SECONDS,
            ).async_validate(_profile(environment.mismatch, environment))

    error = raised.value
    assert error.stage is OperationStage.PANEL_TO_HA
    assert error.code == "panel_to_ha_timeout"
    assert "different MQTT brokers" in error.redacted_detail
    assert "ACL" in error.redacted_detail


class _OneMessageClient:
    @property
    def messages(self) -> AsyncIterator[aiomqtt.Message]:
        return self._messages()

    async def _messages(self) -> AsyncIterator[aiomqtt.Message]:
        yield aiomqtt.Message(
            "brilliant/kitchen/state",
            b"on",
            qos=1,
            retain=False,
            mid=1,
            properties=None,
        )


async def test_ha_seam_dispatches_topics_against_wildcard_subscriptions() -> None:
    received: list[ReceiveMessage] = []

    def on_message(message: ReceiveMessage) -> None:
        received.append(message)

    seam = _RealHaMqttSeam(cast(Any, _OneMessageClient()))
    seam.subscriptions["brilliant/+/state"] = [_Subscription("brilliant/+/state", on_message, 1)]

    await seam.dispatch_messages()

    assert len(received) == 1
    assert received[0].topic == "brilliant/kitchen/state"
    assert received[0].subscribed_topic == "brilliant/+/state"
    assert received[0].payload == b"on"


class _FailingTeardownClient:
    def __init__(self) -> None:
        self.reader_settled = False
        self.exit_count = 0

    async def __aenter__(self) -> _FailingTeardownClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: Any,
    ) -> None:
        self.exit_count += 1

    @property
    def messages(self) -> AsyncIterator[Any]:
        return self._messages()

    async def _messages(self) -> AsyncIterator[Any]:
        try:
            await asyncio.Event().wait()
            yield None
        finally:
            self.reader_settled = True

    async def subscribe(self, topic: str, qos: int = 0) -> tuple[int, ...]:
        return (qos,)

    async def unsubscribe(self, topic: str) -> None:
        raise RuntimeError("forced_live_unsubscribe_failure")


class _BlockedEnterClient:
    def __init__(self) -> None:
        self.enter_started = asyncio.Event()
        self.release_enter = asyncio.Event()
        self.exit_count = 0

    async def __aenter__(self) -> _BlockedEnterClient:
        self.enter_started.set()
        await self.release_enter.wait()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: Any,
    ) -> None:
        self.exit_count += 1


def _plaintext_test_environment(endpoint: _BrokerEndpoint) -> _LiveEnvironment:
    return _LiveEnvironment(
        plain=endpoint,
        tls=_BrokerEndpoint("localhost", 8883, True),
        deny=endpoint,
        mismatch=endpoint,
        ca_file=Path("/not-used-by-plaintext-test"),
        ca_pem="not-used-by-plaintext-test",
        ha_username="homeassistant",
        ha_password="test-only",
        device_username="brilliant_device",
        device_password="test-only",
    )


async def test_ha_seam_settles_successful_entry_before_propagating_cancellation(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    endpoint = _BrokerEndpoint("127.0.0.1", 1883, False)
    client = _BlockedEnterClient()
    body_entered = False

    def client_factory(**kwargs: Any) -> _BlockedEnterClient:
        return client

    async def use_seam() -> None:
        nonlocal body_entered
        async with _real_ha_mqtt(
            hass,
            monkeypatch,
            endpoint,
            _plaintext_test_environment(endpoint),
        ):
            body_entered = True

    monkeypatch.setattr(aiomqtt, "Client", client_factory)
    task = asyncio.create_task(use_seam())
    try:
        await client.enter_started.wait()
        task.cancel()
        await asyncio.sleep(0)
        client.release_enter.set()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert not body_entered
        assert client.exit_count == 1
        assert hass.config_entries.async_entries("mqtt") == []
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        for entry in hass.config_entries.async_entries("mqtt"):
            if isinstance(entry, MockConfigEntry):
                entry.mock_state(hass, ConfigEntryState.NOT_LOADED)
            await hass.config_entries.async_remove(entry.entry_id)


@pytest.mark.parametrize(
    ("primary", "cleanup_errors", "expected"),
    [
        (
            asyncio.CancelledError("primary-control"),
            [KeyboardInterrupt("cleanup-control")],
            "primary-control",
        ),
        (
            RuntimeError("primary-error"),
            [asyncio.CancelledError("cleanup-control")],
            "cleanup-control",
        ),
        (
            RuntimeError("primary-error"),
            [ValueError("cleanup-error")],
            "primary-error",
        ),
        (
            None,
            [ValueError("cleanup-error")],
            "cleanup-error",
        ),
    ],
)
def test_ha_seam_teardown_applies_control_then_primary_precedence(
    primary: BaseException | None,
    cleanup_errors: list[BaseException],
    expected: str,
) -> None:
    with pytest.raises(BaseException, match=f"^{expected}$"):
        _raise_after_ha_seam_teardown(primary, cleanup_errors)


async def test_ha_seam_teardown_attempts_every_resource_after_unsubscribe_failure(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    endpoint = _BrokerEndpoint("127.0.0.1", 1883, False)
    environment = _plaintext_test_environment(endpoint)
    client = _FailingTeardownClient()

    def client_factory(**kwargs: Any) -> _FailingTeardownClient:
        return client

    monkeypatch.setattr(aiomqtt, "Client", client_factory)
    try:
        with pytest.raises(RuntimeError, match="^forced_live_unsubscribe_failure$"):
            async with _real_ha_mqtt(hass, monkeypatch, endpoint, environment):

                def on_message(message: ReceiveMessage) -> None:
                    return None

                unsubscribe = await mqtt.async_subscribe(
                    hass,
                    "brilliant/teardown-contract",
                    on_message,
                    qos=1,
                )
                unsubscribe()

        assert client.reader_settled
        assert client.exit_count == 1
        assert hass.config_entries.async_entries("mqtt") == []
    finally:
        leaked_readers = [
            task
            for task in asyncio.all_tasks()
            if task.get_name() == "brilliant-mqtt-live-ha-reader" and not task.done()
        ]
        for task in leaked_readers:
            task.cancel()
        await asyncio.gather(*leaked_readers, return_exceptions=True)
        for entry in hass.config_entries.async_entries("mqtt"):
            if isinstance(entry, MockConfigEntry):
                entry.mock_state(hass, ConfigEntryState.NOT_LOADED)
            await hass.config_entries.async_remove(entry.entry_id)
