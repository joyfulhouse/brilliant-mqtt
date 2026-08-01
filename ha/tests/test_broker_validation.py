"""Behavioral contract for end-to-end MQTT broker validation."""

from __future__ import annotations

import asyncio
import contextvars
import inspect
import json
import traceback as traceback_module
from collections.abc import AsyncIterator, Callable, Coroutine, Iterator
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from time import monotonic
from typing import Any, cast
from unittest.mock import AsyncMock, Mock, patch
from uuid import UUID, uuid1

import pytest
from homeassistant.components import mqtt
from homeassistant.config_entries import ConfigEntryDisabler, ConfigEntryState
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.brilliant_mqtt import broker_validation
from custom_components.brilliant_mqtt.broker import (
    BrokerKind,
    BrokerProfile,
    DeviceMqttClient,
    DeviceMqttMessage,
)
from custom_components.brilliant_mqtt.broker_validation import BrokerValidator
from custom_components.brilliant_mqtt.errors import (
    OperationError,
    OperationStage,
)
from custom_components.brilliant_mqtt.setup_protocol import (
    MAX_PREFLIGHT_REPORT_BYTES,
    PreflightReport,
    PreflightRequest,
    PreflightStage,
    SetupRequest,
    SetupResult,
    SetupTopics,
)
from custom_components.brilliant_mqtt.shell import RunResult

SETUP_ID = UUID("c0a80101-7c5e-4aca-8e21-0123456789ab")
OTHER_SETUP_ID = UUID("d0a80101-7c5e-4aca-8e21-0123456789ab")


@pytest.fixture(autouse=True)
def _reset_mock_mqtt_entry_state(hass: HomeAssistant) -> Iterator[None]:
    yield
    for entry in hass.config_entries.async_entries("mqtt"):
        if isinstance(entry, MockConfigEntry):
            entry.mock_state(hass, ConfigEntryState.NOT_LOADED)


def _profile(
    kind: BrokerKind = BrokerKind.EXISTING_BROKER,
) -> BrokerProfile:
    return BrokerProfile(
        kind=kind,
        host="mqtt.example.test",
        port=1883,
        tls_enabled=False,
        _username_value="fleet-user",
        _password_value="fleet-password",
        _ca_pem_value=None,
    )


def _mqtt_entry(
    hass: HomeAssistant,
    *,
    data_prefix: str = "homeassistant",
    options_prefix: str | None = None,
) -> MockConfigEntry:
    options: dict[str, Any] = {}
    if options_prefix is not None:
        options["discovery_prefix"] = options_prefix
    entry = MockConfigEntry(
        domain="mqtt",
        data={"discovery_prefix": data_prefix},
        options=options,
        state=ConfigEntryState.LOADED,
    )
    entry.add_to_hass(hass)
    return entry


async def _wait_for_event(
    events: _EventLog,
    expected: tuple[object, ...],
) -> None:
    await events.wait_for(lambda event: event == expected)


def _direct_exception_chain(error: BaseException) -> list[BaseException]:
    chain: list[BaseException] = []
    pending: list[BaseException] = [error]
    while pending:
        current = pending.pop()
        if current in chain:
            continue
        chain.append(current)
        if current.__cause__ is not None:
            pending.append(current.__cause__)
        if current.__context__ is not None:
            pending.append(current.__context__)
    return chain


class _EventLog(list[tuple[object, ...]]):
    def __init__(self) -> None:
        super().__init__()
        self._changed = asyncio.Event()

    def append(self, event: tuple[object, ...]) -> None:
        super().append(event)
        self._changed.set()

    async def wait_for(
        self,
        predicate: Callable[[tuple[object, ...]], bool],
    ) -> None:
        async with asyncio.timeout(0.5):
            while not any(predicate(event) for event in self):
                self._changed.clear()
                if any(predicate(event) for event in self):
                    return
                await self._changed.wait()


@dataclass(slots=True)
class _HaMessage:
    topic: str
    payload: bytes | str
    qos: int
    retain: bool
    subscribed_topic: str
    timestamp: float = 0.0


_HaCallback = Callable[[_HaMessage], Coroutine[Any, Any, None] | None]


@dataclass(slots=True)
class _FakeHaMqtt:
    events: _EventLog = field(default_factory=_EventLog)
    subscriptions: dict[str, _HaCallback] = field(default_factory=dict)
    retained: dict[str, bytes | str] = field(default_factory=dict)
    status_callbacks: dict[tuple[str, int], Callable[[], None]] = field(default_factory=dict)
    device: _FakeDeviceClient | None = None
    wait_result: bool = True
    connected: bool = True
    drop_device_to_ha: set[str] = field(default_factory=set)
    drop_ha_to_device: set[str] = field(default_factory=set)
    retained_replay_flag: bool = True
    device_to_ha_overrides: dict[str, list[_HaMessage]] = field(default_factory=dict)
    ha_to_device_overrides: dict[str, list[DeviceMqttMessage]] = field(default_factory=dict)
    retained_replay_overrides: dict[str, list[_HaMessage]] = field(default_factory=dict)
    suppress_suback: set[str] = field(default_factory=set)
    ha_clear_errors: dict[str, BaseException] = field(default_factory=dict)
    ha_clear_blockers: set[str] = field(default_factory=set)
    ha_unsubscribe_errors: dict[str, BaseException] = field(default_factory=dict)
    wait_blocker: asyncio.Event | None = None

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(mqtt, "async_wait_for_mqtt_client", self.async_wait)
        monkeypatch.setattr(mqtt, "is_connected", self.is_connected)
        monkeypatch.setattr(mqtt, "async_on_subscribe_done", self.on_subscribe_done)
        monkeypatch.setattr(mqtt, "async_subscribe", self.async_subscribe)
        monkeypatch.setattr(mqtt, "async_publish", self.async_publish)

    async def async_wait(self, hass: HomeAssistant) -> bool:
        self.events.append(("ha_wait",))
        if self.wait_blocker is not None:
            await self.wait_blocker.wait()
        return self.wait_result

    def is_connected(self, hass: HomeAssistant) -> bool:
        self.events.append(("ha_connected",))
        return self.connected

    def on_subscribe_done(
        self,
        hass: HomeAssistant,
        topic: str,
        qos: int,
        callback: Callable[[], None],
    ) -> Callable[[], None]:
        self.events.append(("ha_status_watch", topic, qos))
        key = (topic, qos)
        self.status_callbacks[key] = callback

        def remove() -> None:
            self.events.append(("ha_status_unwatch", topic, qos))
            self.status_callbacks.pop(key, None)

        return remove

    async def async_subscribe(
        self,
        hass: HomeAssistant,
        topic: str,
        callback: _HaCallback,
        qos: int = 0,
        encoding: str | None = "utf-8",
    ) -> Callable[[], None]:
        self.events.append(("ha_subscribe", topic, qos))
        self.subscriptions[topic] = callback
        if topic not in self.suppress_suback:
            self.events.append(("ha_suback", topic, qos))
            self.status_callbacks[(topic, qos)]()
        replay = self.retained_replay_overrides.get(topic)
        if replay is not None:
            for message in replay:
                await self._deliver(callback, message)
        elif topic in self.retained:
            await self._deliver(
                callback,
                _HaMessage(
                    topic,
                    self.retained[topic],
                    qos,
                    self.retained_replay_flag,
                    topic,
                ),
            )

        def unsubscribe() -> None:
            self.events.append(("ha_unsubscribe", topic))
            if topic in self.ha_unsubscribe_errors:
                raise self.ha_unsubscribe_errors[topic]
            self.subscriptions.pop(topic, None)

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
        self.events.append(("ha_publish", topic, payload, qos, retain))
        if payload in (b"", "", None) and topic in self.ha_clear_errors:
            raise self.ha_clear_errors[topic]
        if payload in (b"", "", None) and topic in self.ha_clear_blockers:
            await asyncio.Event().wait()
        if retain:
            if payload in (b"", "", None):
                self.retained.pop(topic, None)
            else:
                assert payload is not None
                self.retained[topic] = payload
        if self.device is not None and topic not in self.drop_ha_to_device:
            overrides = self.ha_to_device_overrides.get(topic)
            if overrides is None:
                await self.device.deliver_from_ha(topic, payload, qos, retain)
            else:
                for message in overrides:
                    await self.device.incoming.put(message)

    async def deliver_from_device(
        self,
        topic: str,
        payload: bytes | str | None,
        qos: int,
        retain: bool,
    ) -> None:
        if retain:
            if payload in (b"", "", None):
                self.retained.pop(topic, None)
            else:
                assert payload is not None
                self.retained[topic] = payload
        if topic in self.drop_device_to_ha:
            return
        callback = self.subscriptions.get(topic)
        if callback is not None and payload is not None:
            overrides = self.device_to_ha_overrides.get(topic)
            if overrides is None:
                await self._deliver(
                    callback,
                    _HaMessage(topic, payload, qos, retain, topic),
                )
            else:
                for message in overrides:
                    await self._deliver(callback, message)

    @staticmethod
    async def _deliver(callback: _HaCallback, message: _HaMessage) -> None:
        result = callback(message)
        if inspect.isawaitable(result):
            await result


class _FakePanelProcess:
    """Repeatable process seam with explicit termination and settlement events."""

    def __init__(
        self,
        result: RunResult,
        events: _EventLog,
        *,
        settled: bool = True,
    ) -> None:
        self._result = result
        self._events = events
        self._settled = asyncio.Event()
        if settled:
            self._settled.set()
        self.terminate_count = 0
        self.wait_count = 0

    @property
    def running(self) -> bool:
        return not self._settled.is_set()

    def terminate(self) -> None:
        if not self.running:
            return
        self.terminate_count += 1
        self._events.append(("panel_process_terminate",))
        self._settled.set()

    async def wait(self) -> RunResult:
        self.wait_count += 1
        self._events.append(("panel_process_wait", self.wait_count))
        await self._settled.wait()
        self._events.append(("panel_process_settled", self.wait_count))
        return self._result


class _HungPanelProcess:
    """A broken transport child which ignores terminate and never settles."""

    @property
    def running(self) -> bool:
        return True

    def terminate(self) -> None:
        pass

    async def wait(self) -> RunResult:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


class _FakeDeviceMessages(AsyncIterator[DeviceMqttMessage]):
    def __init__(self, client: _FakeDeviceClient) -> None:
        self._client = client

    def __aiter__(self) -> _FakeDeviceMessages:
        return self

    async def __anext__(self) -> DeviceMqttMessage:
        return await self._client.incoming.get()

    async def aclose(self) -> None:
        self._client.messages_closed = True
        self._client.events.append(("device_messages_close",))
        if self._client.messages_close_error is not None:
            raise self._client.messages_close_error
        if self._client.messages_close_blocker:
            await asyncio.Event().wait()


@dataclass(slots=True)
class _FakeDeviceClient(DeviceMqttClient):
    seam: _FakeHaMqtt
    events: list[tuple[object, ...]]
    incoming: asyncio.Queue[DeviceMqttMessage] = field(default_factory=asyncio.Queue)
    subscriptions: set[str] = field(default_factory=set)
    disconnect_count: int = 0
    subscribe_errors: dict[str, BaseException] = field(default_factory=dict)
    subscribe_blockers: set[str] = field(default_factory=set)
    publish_errors: dict[str, BaseException] = field(default_factory=dict)
    publish_blockers: set[str] = field(default_factory=set)
    clear_errors: dict[str, BaseException] = field(default_factory=dict)
    clear_blockers: set[str] = field(default_factory=set)
    unsubscribe_errors: dict[str, BaseException] = field(default_factory=dict)
    unsubscribe_blockers: set[str] = field(default_factory=set)
    disconnect_error: BaseException | None = None
    disconnect_blocker: bool = False
    messages_close_error: BaseException | None = None
    messages_close_blocker: bool = False
    messages_closed: bool = False
    device_messages: _FakeDeviceMessages = field(init=False)

    def __post_init__(self) -> None:
        self.device_messages = _FakeDeviceMessages(self)

    @property
    def messages(self) -> AsyncIterator[DeviceMqttMessage]:
        return self.device_messages

    async def subscribe(self, topic: str, qos: int = 0) -> None:
        self.events.append(("device_subscribe", topic, qos))
        self.subscriptions.add(topic)
        if topic in self.subscribe_errors:
            raise self.subscribe_errors[topic]
        if topic in self.subscribe_blockers:
            await asyncio.Event().wait()

    async def publish(
        self,
        topic: str,
        payload: bytes | str | None = None,
        qos: int = 0,
        retain: bool = False,
    ) -> None:
        self.events.append(("device_publish", topic, payload, qos, retain))
        errors = self.clear_errors if payload in (b"", "", None) else self.publish_errors
        if topic in errors:
            raise errors[topic]
        if payload not in (b"", "", None) and topic in self.publish_blockers:
            await asyncio.Event().wait()
        if payload in (b"", "", None) and topic in self.clear_blockers:
            await asyncio.Event().wait()
        await self.seam.deliver_from_device(topic, payload, qos, retain)

    async def unsubscribe(self, topic: str) -> None:
        self.events.append(("device_unsubscribe", topic))
        if topic in self.unsubscribe_errors:
            raise self.unsubscribe_errors[topic]
        if topic in self.unsubscribe_blockers:
            await asyncio.Event().wait()
        self.subscriptions.discard(topic)

    async def disconnect(self) -> None:
        self.disconnect_count += 1
        self.events.append(("device_disconnect",))
        if self.disconnect_error is not None:
            raise self.disconnect_error
        if self.disconnect_blocker:
            await asyncio.Event().wait()

    async def deliver_from_ha(
        self,
        topic: str,
        payload: bytes | str | None,
        qos: int,
        retain: bool,
    ) -> None:
        if topic not in self.subscriptions or payload is None:
            return
        encoded = payload.encode() if isinstance(payload, str) else payload
        await self.incoming.put(
            DeviceMqttMessage(
                topic=topic,
                payload=encoded,
                qos=qos,
                retain=retain,
            )
        )


@dataclass(slots=True)
class _FakeDeviceContext(AbstractAsyncContextManager[DeviceMqttClient]):
    seam: _FakeHaMqtt
    events: list[tuple[object, ...]]
    client: _FakeDeviceClient = field(init=False)
    exit_count: int = 0
    enter_error: BaseException | None = None
    exit_error: BaseException | None = None
    enter_blocker: asyncio.Event | None = None
    exit_blocker: bool = False

    def __post_init__(self) -> None:
        self.client = _FakeDeviceClient(self.seam, self.events)

    async def __aenter__(self) -> DeviceMqttClient:
        self.events.append(("device_enter",))
        if self.enter_error is not None:
            raise self.enter_error
        if self.enter_blocker is not None:
            await self.enter_blocker.wait()
        self.seam.device = self.client
        return self.client

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: Any,
    ) -> None:
        self.exit_count += 1
        self.events.append(("device_exit",))
        if self.exit_error is not None:
            raise self.exit_error
        if self.exit_blocker:
            await asyncio.Event().wait()


@dataclass(slots=True)
class _FakeDeviceFactory:
    seam: _FakeHaMqtt
    events: list[tuple[object, ...]]
    profiles: list[BrokerProfile] = field(default_factory=list)
    client_ids: list[str] = field(default_factory=list)
    contexts: list[_FakeDeviceContext] = field(default_factory=list)
    enter_error: BaseException | None = None
    configure_context: Callable[[_FakeDeviceContext], None] | None = None

    def __call__(
        self,
        profile: BrokerProfile,
        client_id: str,
    ) -> _FakeDeviceContext:
        self.profiles.append(profile)
        self.client_ids.append(client_id)
        self.events.append(("device_construct", client_id))
        context = _FakeDeviceContext(
            self.seam,
            self.events,
            enter_error=self.enter_error,
        )
        if self.configure_context is not None:
            self.configure_context(context)
        self.contexts.append(context)
        return context


class _BlockedSuccessfulRawClient:
    def __init__(self) -> None:
        self.enter_started = asyncio.Event()
        self.release_enter = asyncio.Event()
        self.exit_started = asyncio.Event()
        self.release_exit = asyncio.Event()
        self.enter_count = 0
        self.exit_count = 0

    async def __aenter__(self) -> _BlockedSuccessfulRawClient:
        self.enter_count += 1
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
        self.exit_started.set()
        await self.release_exit.wait()


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_success_uses_ordered_qos_one_round_trips_and_redacted_result(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mqtt_entry = _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    factory = _FakeDeviceFactory(seam, seam.events)
    config_entry_snapshot = tuple(hass.config_entries.async_entries())
    entry_snapshot = (
        dict(mqtt_entry.data),
        dict(mqtt_entry.options),
        mqtt_entry.state,
        mqtt_entry.disabled_by,
        mqtt_entry.source,
    )

    profile = _profile()
    result = await BrokerValidator(
        hass,
        factory,
        timeout_seconds=0.2,
    ).async_validate(profile, setup_id=SETUP_ID)

    topics = SetupTopics.for_id(SETUP_ID)
    assert result.setup_id == SETUP_ID
    assert result.completed_stages == tuple(OperationStage)[1:]
    assert result.elapsed_seconds >= 0
    assert tuple(stage for stage, _ in result.stage_elapsed_seconds) == tuple(OperationStage)[1:]
    assert result.redacted_dict() == {
        "setup_id": str(SETUP_ID),
        "completed_stages": [stage.value for stage in tuple(OperationStage)[1:]],
        "elapsed_seconds": result.elapsed_seconds,
        "stage_elapsed_seconds": {
            stage.value: elapsed for stage, elapsed in result.stage_elapsed_seconds
        },
    }
    assert factory.profiles == [profile]
    assert factory.profiles[0] is profile
    assert factory.client_ids == [f"brilliant-mqtt-setup-{SETUP_ID}"]

    def event_index(kind: str, topic: str) -> int:
        return next(
            index
            for index, event in enumerate(seam.events)
            if event[0] == kind and event[1] == topic
        )

    assert event_index("ha_status_watch", topics.panel_to_ha) < event_index(
        "ha_subscribe", topics.panel_to_ha
    )
    assert event_index("ha_suback", topics.panel_to_ha) < event_index(
        "device_publish", topics.panel_to_ha
    )
    assert event_index("ha_subscribe", topics.panel_to_ha) < event_index(
        "device_publish", topics.panel_to_ha
    )
    assert event_index("device_subscribe", topics.ha_to_panel) < event_index(
        "ha_publish", topics.ha_to_panel
    )
    assert event_index("ha_subscribe", topics.discovery_probe) < event_index(
        "device_publish", topics.discovery_probe
    )
    assert event_index("ha_suback", topics.discovery_probe) < event_index(
        "device_publish", topics.discovery_probe
    )
    assert event_index("device_publish", topics.retained) < event_index(
        "ha_subscribe", topics.retained
    )

    probe_topics = {
        topics.panel_to_ha,
        topics.ha_to_panel,
        topics.discovery_probe,
        topics.retained,
    }
    probe_events = [
        event
        for event in seam.events
        if event[0] in {"device_publish", "ha_publish"}
        and event[1] in probe_topics
        and event[2] not in ("", b"", None)
    ]
    assert probe_events
    assert all(event[3] == 1 for event in probe_events)
    assert (
        "device_publish",
        topics.panel_to_ha,
        probe_events[0][2],
        1,
        False,
    ) in seam.events
    assert any(
        event[0] == "ha_publish" and event[1] == topics.ha_to_panel and event[3:] == (1, False)
        for event in seam.events
    )
    assert any(
        event[0] == "device_publish"
        and event[1] == topics.discovery_probe
        and event[3:] == (1, True)
        for event in seam.events
    )
    assert any(
        event[0] == "device_publish" and event[1] == topics.retained and event[3:] == (1, True)
        for event in seam.events
    )
    assert {event[1] for event in seam.events if event[0] == "ha_subscribe" and event[2] == 1} == {
        topics.panel_to_ha,
        topics.discovery_probe,
        topics.retained,
    }
    assert ("device_subscribe", topics.ha_to_panel, 1) in seam.events
    clear_events = [
        event
        for event in seam.events
        if event[0] in {"device_publish", "ha_publish"} and event[2] == b""
    ]
    assert len(clear_events) == 4
    assert all(event[3:] == (1, True) for event in clear_events)
    assert hass.config_entries.async_entries() == list(config_entry_snapshot)
    assert (
        dict(mqtt_entry.data),
        dict(mqtt_entry.options),
        mqtt_entry.state,
        mqtt_entry.disabled_by,
        mqtt_entry.source,
    ) == entry_snapshot
    context = factory.contexts[0]
    assert context.client.disconnect_count == 1
    assert context.exit_count == 1
    assert not context.client.subscriptions
    assert not seam.subscriptions
    assert not seam.status_callbacks
    assert not seam.retained


@pytest.mark.usefixtures("enable_custom_integrations")
@pytest.mark.parametrize(
    ("case", "expected_stage", "expected_code"),
    [
        (
            "ha_disconnected",
            OperationStage.HA_MQTT_READY,
            "ha_mqtt_unavailable",
        ),
        (
            "ha_wait_unavailable",
            OperationStage.HA_MQTT_READY,
            "ha_mqtt_unavailable",
        ),
        (
            "unsupported_prefix",
            OperationStage.HA_MQTT_READY,
            "unsupported_discovery_prefix",
        ),
        ("fleet_auth", OperationStage.FLEET_AUTH, "fleet_auth_failed"),
        (
            "panel_to_ha",
            OperationStage.PANEL_TO_HA,
            "panel_to_ha_timeout",
        ),
        (
            "ha_to_panel",
            OperationStage.HA_TO_PANEL,
            "ha_to_panel_timeout",
        ),
        (
            "discovery_write",
            OperationStage.DISCOVERY_WRITE,
            "discovery_write_timeout",
        ),
        (
            "retained_message",
            OperationStage.RETAINED_MESSAGE,
            "retained_message_invalid",
        ),
        ("cleanup", OperationStage.CLEANUP, "cleanup_failed"),
    ],
)
async def test_stage_table_maps_fixed_errors_and_still_cleans_up(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
    expected_stage: OperationStage,
    expected_code: str,
) -> None:
    _mqtt_entry(
        hass,
        data_prefix="homeassistant",
        options_prefix="zigbee" if case == "unsupported_prefix" else None,
    )
    topics = SetupTopics.for_id(SETUP_ID)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    factory = _FakeDeviceFactory(seam, seam.events)
    if case == "ha_disconnected":
        seam.connected = False
    elif case == "ha_wait_unavailable":
        seam.wait_result = False
    elif case == "fleet_auth":
        factory.enter_error = RuntimeError("secret unclassified fleet auth failure")
    elif case == "panel_to_ha":
        seam.drop_device_to_ha.add(topics.panel_to_ha)
    elif case == "ha_to_panel":
        seam.drop_ha_to_device.add(topics.ha_to_panel)
    elif case == "discovery_write":

        def configure_discovery_failure(context: _FakeDeviceContext) -> None:
            context.client.publish_blockers.add(topics.discovery_probe)

        factory.configure_context = configure_discovery_failure
    elif case == "retained_message":
        seam.retained_replay_flag = False
    elif case == "cleanup":

        def configure_cleanup_failure(context: _FakeDeviceContext) -> None:
            context.client.clear_errors[topics.discovery_probe] = RuntimeError(
                "secret retained-clear response"
            )

        factory.configure_context = configure_cleanup_failure

    with pytest.raises(OperationError) as raised:
        await BrokerValidator(
            hass,
            factory,
            timeout_seconds=0.02,
        ).async_validate(_profile(), setup_id=SETUP_ID)

    error = raised.value
    assert error.stage is expected_stage
    assert error.code == expected_code
    assert error.__cause__ is None
    assert error.__context__ is None
    if expected_code in {"panel_to_ha_timeout", "ha_to_panel_timeout"}:
        assert "different MQTT brokers" in error.redacted_detail
        assert "ACL" in error.redacted_detail

    if case in {
        "ha_disconnected",
        "ha_wait_unavailable",
        "unsupported_prefix",
    }:
        assert factory.contexts == []
        assert not any(event[0] == "device_construct" for event in seam.events)
        return
    if case == "fleet_auth":
        assert factory.contexts[0].exit_count == 0
        assert factory.contexts[0].client.disconnect_count == 0
        return

    context = factory.contexts[0]
    assert context.client.disconnect_count == 1
    assert context.exit_count == 1
    assert not seam.subscriptions
    if case == "cleanup":
        assert ("ha_publish", topics.discovery_probe, b"", 1, True) in seam.events
        assert ("ha_publish", topics.retained, b"", 1, True) in seam.events
        assert ("ha_unsubscribe", topics.panel_to_ha) in seam.events
        assert ("ha_unsubscribe", topics.discovery_probe) in seam.events
        assert ("ha_unsubscribe", topics.retained) in seam.events
        assert ("device_unsubscribe", topics.ha_to_panel) in seam.events


@pytest.mark.usefixtures("enable_custom_integrations")
@pytest.mark.parametrize(
    "classified_code",
    [
        "broker_tls_verification_failed",
        "broker_authentication_failed",
        "broker_connection_rejected",
        "broker_unavailable",
        "broker_connect_failed",
        "broker_timeout",
    ],
)
async def test_fleet_auth_preserves_classified_broker_error(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
    classified_code: str,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    factory = _FakeDeviceFactory(
        seam,
        seam.events,
        enter_error=OperationError.for_code(
            OperationStage.FLEET_AUTH,
            classified_code,
        ),
    )

    with pytest.raises(OperationError) as raised:
        await BrokerValidator(hass, factory, 0.02).async_validate(
            _profile(),
            setup_id=SETUP_ID,
        )

    error = raised.value
    assert error.stage is OperationStage.FLEET_AUTH
    assert error.code == classified_code
    assert error.__cause__ is None
    assert error.__context__ is None


@pytest.mark.usefixtures("enable_custom_integrations")
@pytest.mark.parametrize(
    "timeout",
    [0.0, -0.1, float("nan"), float("inf"), float("-inf"), True],
)
def test_timeout_must_be_finite_positive(
    hass: HomeAssistant,
    timeout: float,
) -> None:
    with pytest.raises(ValueError, match="^invalid_validation_timeout$"):
        BrokerValidator(
            hass,
            cast(Any, object()),
            timeout_seconds=timeout,
        )


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_generated_id_is_v4_and_factory_receives_same_profile(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    factory = _FakeDeviceFactory(seam, seam.events)
    profile = _profile(BrokerKind.OFFICIAL_MOSQUITTO)

    result = await BrokerValidator(hass, factory, 0.2).async_validate(profile)

    assert result.setup_id.version == 4
    assert factory.profiles == [profile]
    assert factory.profiles[0] is profile
    assert factory.client_ids == [f"brilliant-mqtt-setup-{result.setup_id}"]


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_panel_and_ha_nonces_are_distinct_under_repeated_random_output(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    factory = _FakeDeviceFactory(seam, seam.events)
    monkeypatch.setattr(
        "custom_components.brilliant_mqtt.broker_validation.secrets.token_urlsafe",
        lambda size: "repeated-random-output",
    )

    await BrokerValidator(hass, factory, 0.2).async_validate(
        _profile(),
        setup_id=SETUP_ID,
    )

    topics = SetupTopics.for_id(SETUP_ID)
    panel_payload = next(
        event[2]
        for event in seam.events
        if event[0] == "device_publish"
        and event[1] == topics.panel_to_ha
        and event[2] not in (b"", "", None)
    )
    ha_payload = next(
        event[2]
        for event in seam.events
        if event[0] == "ha_publish"
        and event[1] == topics.ha_to_panel
        and event[2] not in (b"", "", None)
    )
    request = SetupRequest.from_payload(cast(bytes | str, panel_payload))
    result = SetupResult.from_payload(cast(bytes | str, ha_payload))
    assert request.nonce != result.nonce


class _SecretSetupId:
    def __str__(self) -> str:
        raise RuntimeError("SECRET-INVALID-SETUP-ID")


@pytest.mark.usefixtures("enable_custom_integrations")
@pytest.mark.parametrize("bad_id", [uuid1(), _SecretSetupId()])
async def test_invalid_setup_id_fails_before_mqtt_with_fixed_clean_error(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
    bad_id: object,
) -> None:
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    factory = _FakeDeviceFactory(seam, seam.events)

    with pytest.raises(ValueError, match="^invalid_setup_id$") as raised:
        await BrokerValidator(hass, factory, 0.2).async_validate(
            _profile(),
            setup_id=cast(Any, bad_id),
        )

    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    assert seam.events == []
    assert factory.contexts == []


@pytest.mark.usefixtures("enable_custom_integrations")
@pytest.mark.parametrize(
    "invalid_kind",
    [
        "wrong_setup_id",
        "wrong_nonce",
        "malformed_bytes",
        "malformed_json",
        "wrong_topic",
        "bytearray",
    ],
)
async def test_panel_to_ha_rejects_invalid_or_stale_payloads(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
    invalid_kind: str,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    topics = SetupTopics.for_id(SETUP_ID)
    monkeypatch.setattr(
        "custom_components.brilliant_mqtt.broker_validation.secrets.token_urlsafe",
        lambda size: "fixed",
    )
    payload: object
    topic = topics.panel_to_ha
    if invalid_kind == "wrong_setup_id":
        payload = SetupRequest(OTHER_SETUP_ID, "panel-fixed").to_payload()
    elif invalid_kind == "wrong_nonce":
        payload = SetupRequest(SETUP_ID, "stale-panel-nonce").to_payload()
    elif invalid_kind == "malformed_bytes":
        payload = b"\xff"
    elif invalid_kind == "malformed_json":
        payload = b'{"schema_version":1'
    elif invalid_kind == "wrong_topic":
        payload = SetupRequest(SETUP_ID, "panel-fixed").to_payload()
        topic = f"{topics.panel_to_ha}/stale"
    else:
        payload = bytearray(SetupRequest(SETUP_ID, "panel-fixed").to_payload().encode())
    seam.device_to_ha_overrides[topics.panel_to_ha] = [
        _HaMessage(
            topic,
            cast(Any, payload),
            1,
            False,
            topics.panel_to_ha,
        )
    ]

    with pytest.raises(OperationError) as raised:
        await BrokerValidator(
            hass,
            _FakeDeviceFactory(seam, seam.events),
            0.01,
        ).async_validate(_profile(), setup_id=SETUP_ID)

    assert raised.value.stage is OperationStage.PANEL_TO_HA
    assert raised.value.code == "panel_to_ha_timeout"


@pytest.mark.usefixtures("enable_custom_integrations")
@pytest.mark.parametrize(
    "invalid_kind",
    [
        "wrong_setup_id",
        "wrong_nonce",
        "wrong_reply_nonce",
        "malformed_bytes",
        "wrong_topic",
        "bytearray",
    ],
)
async def test_ha_to_panel_rejects_invalid_or_stale_payloads(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
    invalid_kind: str,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    topics = SetupTopics.for_id(SETUP_ID)
    monkeypatch.setattr(
        "custom_components.brilliant_mqtt.broker_validation.secrets.token_urlsafe",
        lambda size: "fixed",
    )
    payload: object
    topic = topics.ha_to_panel
    if invalid_kind == "wrong_setup_id":
        payload = (
            SetupResult(
                OTHER_SETUP_ID,
                "ha-fixed",
                "panel-fixed",
            )
            .to_payload()
            .encode()
        )
    elif invalid_kind == "wrong_nonce":
        payload = (
            SetupResult(
                SETUP_ID,
                "stale-ha-nonce",
                "panel-fixed",
            )
            .to_payload()
            .encode()
        )
    elif invalid_kind == "wrong_reply_nonce":
        payload = (
            SetupResult(
                SETUP_ID,
                "ha-fixed",
                "stale-panel-nonce",
            )
            .to_payload()
            .encode()
        )
    elif invalid_kind == "malformed_bytes":
        payload = b"\xff"
    elif invalid_kind == "wrong_topic":
        payload = (
            SetupResult(
                SETUP_ID,
                "ha-fixed",
                "panel-fixed",
            )
            .to_payload()
            .encode()
        )
        topic = f"{topics.ha_to_panel}/stale"
    else:
        payload = bytearray(
            SetupResult(
                SETUP_ID,
                "ha-fixed",
                "panel-fixed",
            )
            .to_payload()
            .encode()
        )
    seam.ha_to_device_overrides[topics.ha_to_panel] = [
        DeviceMqttMessage(
            topic=topic,
            payload=cast(bytes, payload),
            qos=1,
            retain=False,
        )
    ]

    with pytest.raises(OperationError) as raised:
        await BrokerValidator(
            hass,
            _FakeDeviceFactory(seam, seam.events),
            0.01,
        ).async_validate(_profile(), setup_id=SETUP_ID)

    assert raised.value.stage is OperationStage.HA_TO_PANEL
    assert raised.value.code == "ha_to_panel_timeout"


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_stale_messages_are_ignored_until_exact_messages_arrive(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    topics = SetupTopics.for_id(SETUP_ID)
    monkeypatch.setattr(
        "custom_components.brilliant_mqtt.broker_validation.secrets.token_urlsafe",
        lambda size: "fixed",
    )
    seam.device_to_ha_overrides[topics.panel_to_ha] = [
        _HaMessage(
            topics.panel_to_ha,
            SetupRequest(SETUP_ID, "stale").to_payload(),
            1,
            False,
            topics.panel_to_ha,
        ),
        _HaMessage(
            topics.panel_to_ha,
            SetupRequest(SETUP_ID, "panel-fixed").to_payload(),
            1,
            False,
            topics.panel_to_ha,
        ),
    ]
    seam.ha_to_device_overrides[topics.ha_to_panel] = [
        DeviceMqttMessage(
            topics.ha_to_panel,
            SetupResult(SETUP_ID, "stale", "panel-fixed").to_payload().encode(),
            1,
            False,
        ),
        DeviceMqttMessage(
            topics.ha_to_panel,
            SetupResult(
                SETUP_ID,
                "ha-fixed",
                "panel-fixed",
            )
            .to_payload()
            .encode(),
            1,
            False,
        ),
    ]

    result = await BrokerValidator(
        hass,
        _FakeDeviceFactory(seam, seam.events),
        0.2,
    ).async_validate(_profile(), setup_id=SETUP_ID)

    assert result.completed_stages[-1] is OperationStage.CLEANUP


@pytest.mark.usefixtures("enable_custom_integrations")
@pytest.mark.parametrize(
    ("invalid_kind", "expected_code"),
    [
        ("missing_retained_flag", "retained_message_invalid"),
        ("wrong_payload", "retained_message_timeout"),
        ("wrong_topic", "retained_message_timeout"),
        ("bytearray", "retained_message_timeout"),
    ],
)
async def test_retained_replay_requires_exact_topic_payload_and_flag(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
    invalid_kind: str,
    expected_code: str,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    topics = SetupTopics.for_id(SETUP_ID)
    monkeypatch.setattr(
        "custom_components.brilliant_mqtt.broker_validation.secrets.token_urlsafe",
        lambda size: "fixed",
    )
    topic = topics.retained
    payload: object = "retained-fixed"
    retain = True
    if invalid_kind == "missing_retained_flag":
        retain = False
    elif invalid_kind == "wrong_payload":
        payload = "stale-retained-payload"
    elif invalid_kind == "wrong_topic":
        topic = f"{topics.retained}/stale"
    else:
        payload = bytearray(b"retained-fixed")
    seam.retained_replay_overrides[topics.retained] = [
        _HaMessage(
            topic,
            cast(Any, payload),
            1,
            retain,
            topics.retained,
        )
    ]

    with pytest.raises(OperationError) as raised:
        await BrokerValidator(
            hass,
            _FakeDeviceFactory(seam, seam.events),
            0.01,
        ).async_validate(_profile(), setup_id=SETUP_ID)

    assert raised.value.stage is OperationStage.RETAINED_MESSAGE
    assert raised.value.code == expected_code


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_ha_suback_is_required_before_cross_client_publish(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    topics = SetupTopics.for_id(SETUP_ID)
    seam.suppress_suback.add(topics.panel_to_ha)

    with pytest.raises(OperationError) as raised:
        await BrokerValidator(
            hass,
            _FakeDeviceFactory(seam, seam.events),
            0.01,
        ).async_validate(_profile(), setup_id=SETUP_ID)

    assert raised.value.code == "panel_to_ha_timeout"
    assert not any(
        event[0] == "device_publish" and event[1] == topics.panel_to_ha for event in seam.events
    )
    assert ("ha_unsubscribe", topics.panel_to_ha) in seam.events
    assert ("ha_status_unwatch", topics.panel_to_ha, 1) in seam.events


@pytest.mark.usefixtures("enable_custom_integrations")
@pytest.mark.parametrize(
    ("stage", "expected_code"),
    [
        (OperationStage.HA_MQTT_READY, "ha_mqtt_unavailable"),
        (OperationStage.FLEET_AUTH, "broker_timeout"),
    ],
)
async def test_early_stage_deadlines_are_strict_and_stable(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
    stage: OperationStage,
    expected_code: str,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    factory = _FakeDeviceFactory(seam, seam.events)
    if stage is OperationStage.HA_MQTT_READY:
        seam.wait_blocker = asyncio.Event()
    else:

        def configure(context: _FakeDeviceContext) -> None:
            context.enter_blocker = asyncio.Event()

        factory.configure_context = configure

    with pytest.raises(OperationError) as raised:
        await BrokerValidator(hass, factory, 0.01).async_validate(
            _profile(),
            setup_id=SETUP_ID,
        )

    assert raised.value.stage is stage
    assert raised.value.code == expected_code
    if stage is OperationStage.FLEET_AUTH:
        assert factory.contexts[0].client.disconnect_count == 0
        assert factory.contexts[0].exit_count == 0


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_subscribe_side_effect_before_ack_is_explicitly_unsubscribed(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    topics = SetupTopics.for_id(SETUP_ID)

    def configure(context: _FakeDeviceContext) -> None:
        context.client.subscribe_blockers.add(topics.ha_to_panel)
        context.client.disconnect_error = RuntimeError("SECRET-DISCONNECT-AFTER-SUBSCRIBE")

    factory = _FakeDeviceFactory(
        seam,
        seam.events,
        configure_context=configure,
    )
    baseline_tasks = {task for task in asyncio.all_tasks() if task is not asyncio.current_task()}

    with pytest.raises(OperationError) as raised:
        await BrokerValidator(hass, factory, 0.01).async_validate(
            _profile(),
            setup_id=SETUP_ID,
        )
    await asyncio.sleep(0)

    error = raised.value
    assert error.stage is OperationStage.HA_TO_PANEL
    assert error.code == "ha_to_panel_timeout"
    assert error.cleanup_error is not None
    assert error.cleanup_error.stage is OperationStage.CLEANUP
    assert error.cleanup_error.code == "cleanup_failed"
    context = factory.contexts[0]
    assert seam.events.count(("device_unsubscribe", topics.ha_to_panel)) == 1
    assert context.client.subscriptions == set()
    assert context.client.disconnect_count == 1
    assert context.exit_count == 1
    assert {
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task() and task not in baseline_tasks and not task.done()
    } == set()


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_immediate_subscribe_failure_keeps_primary_and_cleans_intent_once(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    topics = SetupTopics.for_id(SETUP_ID)

    def configure(context: _FakeDeviceContext) -> None:
        context.client.subscribe_errors[topics.ha_to_panel] = RuntimeError(
            "SECRET-IMMEDIATE-SUBSCRIBE-FAILURE"
        )

    factory = _FakeDeviceFactory(
        seam,
        seam.events,
        configure_context=configure,
    )

    with pytest.raises(OperationError) as raised:
        await BrokerValidator(hass, factory, 0.02).async_validate(
            _profile(),
            setup_id=SETUP_ID,
        )

    error = raised.value
    assert error.stage is OperationStage.HA_TO_PANEL
    assert error.code == "ha_to_panel_timeout"
    assert error.cleanup_error is None
    context = factory.contexts[0]
    assert seam.events.count(("device_unsubscribe", topics.ha_to_panel)) == 1
    assert context.client.subscriptions == set()
    assert context.client.disconnect_count == 1
    assert context.exit_count == 1


@pytest.mark.usefixtures("enable_custom_integrations")
@pytest.mark.parametrize("invalid_kind", ["wrong_topic", "wrong_payload"])
async def test_discovery_probe_requires_exact_topic_and_payload(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
    invalid_kind: str,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    topics = SetupTopics.for_id(SETUP_ID)
    monkeypatch.setattr(
        "custom_components.brilliant_mqtt.broker_validation.secrets.token_urlsafe",
        lambda size: "fixed",
    )
    seam.device_to_ha_overrides[topics.discovery_probe] = [
        _HaMessage(
            (
                f"{topics.discovery_probe}/stale"
                if invalid_kind == "wrong_topic"
                else topics.discovery_probe
            ),
            ("stale-discovery-payload" if invalid_kind == "wrong_payload" else "discovery-fixed"),
            1,
            True,
            topics.discovery_probe,
        )
    ]

    with pytest.raises(OperationError) as raised:
        await BrokerValidator(
            hass,
            _FakeDeviceFactory(seam, seam.events),
            0.01,
        ).async_validate(_profile(), setup_id=SETUP_ID)

    assert raised.value.stage is OperationStage.DISCOVERY_WRITE
    assert raised.value.code == "discovery_write_timeout"


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_observed_discovery_authorization_rejection_remains_definitive(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    topics = SetupTopics.for_id(SETUP_ID)

    def configure(context: _FakeDeviceContext) -> None:
        context.client.publish_errors[topics.discovery_probe] = OperationError.for_code(
            OperationStage.DISCOVERY_WRITE,
            "discovery_write_denied",
        )

    with pytest.raises(OperationError) as raised:
        await BrokerValidator(
            hass,
            _FakeDeviceFactory(seam, seam.events, configure_context=configure),
            0.02,
        ).async_validate(_profile(), setup_id=SETUP_ID)

    assert raised.value.stage is OperationStage.DISCOVERY_WRITE
    assert raised.value.code == "discovery_write_denied"
    assert raised.value.retryable is False
    assert _direct_exception_chain(raised.value) == [raised.value]


@pytest.mark.usefixtures("enable_custom_integrations")
@pytest.mark.parametrize(
    "entry_case",
    ["missing", "two_loaded", "disabled", "ignored", "not_loaded"],
)
async def test_ha_ready_requires_exactly_one_enabled_nonignored_loaded_entry(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
    entry_case: str,
) -> None:
    if entry_case == "two_loaded":
        _mqtt_entry(hass)
        _mqtt_entry(hass)
    elif entry_case != "missing":
        entry = MockConfigEntry(
            domain="mqtt",
            data={},
            disabled_by=(ConfigEntryDisabler.USER if entry_case == "disabled" else None),
            source="ignore" if entry_case == "ignored" else "user",
            state=(
                ConfigEntryState.NOT_LOADED
                if entry_case == "not_loaded"
                else ConfigEntryState.LOADED
            ),
        )
        entry.add_to_hass(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    factory = _FakeDeviceFactory(seam, seam.events)

    with pytest.raises(OperationError) as raised:
        await BrokerValidator(hass, factory, 0.02).async_validate(
            _profile(),
            setup_id=SETUP_ID,
        )

    assert raised.value.stage is OperationStage.HA_MQTT_READY
    assert raised.value.code == "ha_mqtt_unavailable"
    assert factory.contexts == []


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_default_discovery_prefix_is_homeassistant(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entry = MockConfigEntry(
        domain="mqtt",
        data={},
        options={},
        state=ConfigEntryState.LOADED,
    )
    entry.add_to_hass(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)

    result = await BrokerValidator(
        hass,
        _FakeDeviceFactory(seam, seam.events),
        0.2,
    ).async_validate(_profile(), setup_id=SETUP_ID)

    assert result.completed_stages[-1] is OperationStage.CLEANUP


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_cleanup_attempts_every_action_after_multiple_failures(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    topics = SetupTopics.for_id(SETUP_ID)
    secret_errors = {
        "device_discovery": RuntimeError("SECRET-DEVICE-DISCOVERY-CLEAR"),
        "device_retained": RuntimeError("SECRET-DEVICE-RETAINED-CLEAR"),
        "ha_discovery": RuntimeError("SECRET-HA-DISCOVERY-CLEAR"),
        "ha_retained": RuntimeError("SECRET-HA-RETAINED-CLEAR"),
        "ha_panel_unsub": RuntimeError("SECRET-HA-PANEL-UNSUB"),
        "ha_discovery_unsub": RuntimeError("SECRET-HA-DISCOVERY-UNSUB"),
        "ha_retained_unsub": RuntimeError("SECRET-HA-RETAINED-UNSUB"),
        "device_unsub": RuntimeError("SECRET-DEVICE-UNSUB"),
        "disconnect": RuntimeError("SECRET-DEVICE-DISCONNECT"),
        "exit": RuntimeError("SECRET-DEVICE-EXIT"),
    }
    seam.ha_clear_errors = {
        topics.discovery_probe: secret_errors["ha_discovery"],
        topics.retained: secret_errors["ha_retained"],
    }
    seam.ha_unsubscribe_errors = {
        topics.panel_to_ha: secret_errors["ha_panel_unsub"],
        topics.discovery_probe: secret_errors["ha_discovery_unsub"],
        topics.retained: secret_errors["ha_retained_unsub"],
    }

    def configure(context: _FakeDeviceContext) -> None:
        context.client.clear_errors = {
            topics.discovery_probe: secret_errors["device_discovery"],
            topics.retained: secret_errors["device_retained"],
        }
        context.client.unsubscribe_errors[topics.ha_to_panel] = secret_errors["device_unsub"]
        context.client.disconnect_error = secret_errors["disconnect"]
        context.exit_error = secret_errors["exit"]

    factory = _FakeDeviceFactory(
        seam,
        seam.events,
        configure_context=configure,
    )

    with pytest.raises(OperationError) as raised:
        await BrokerValidator(hass, factory, 0.02).async_validate(
            _profile(),
            setup_id=SETUP_ID,
        )

    assert raised.value.stage is OperationStage.CLEANUP
    assert raised.value.code == "cleanup_failed"
    expected_events = [
        ("device_publish", topics.discovery_probe, b"", 1, True),
        ("device_publish", topics.retained, b"", 1, True),
        ("ha_publish", topics.discovery_probe, b"", 1, True),
        ("ha_publish", topics.retained, b"", 1, True),
        ("ha_unsubscribe", topics.panel_to_ha),
        ("ha_unsubscribe", topics.discovery_probe),
        ("ha_unsubscribe", topics.retained),
        ("device_unsubscribe", topics.ha_to_panel),
        ("device_disconnect",),
        ("device_exit",),
    ]
    for expected in expected_events:
        assert seam.events.count(expected) == 1
    assert factory.contexts[0].client.disconnect_count == 1
    assert factory.contexts[0].exit_count == 1


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_cleanup_closes_device_messages_before_unsubscribe_and_disconnect(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    factory = _FakeDeviceFactory(seam, seam.events)
    baseline_tasks = {task for task in asyncio.all_tasks() if task is not asyncio.current_task()}

    await BrokerValidator(hass, factory, 0.02).async_validate(
        _profile(),
        setup_id=SETUP_ID,
    )
    await asyncio.sleep(0)

    topics = SetupTopics.for_id(SETUP_ID)
    context = factory.contexts[0]
    close_index = seam.events.index(("device_messages_close",))
    later_actions = (
        ("device_publish", topics.discovery_probe, b"", 1, True),
        ("device_publish", topics.retained, b"", 1, True),
        ("ha_publish", topics.discovery_probe, b"", 1, True),
        ("ha_publish", topics.retained, b"", 1, True),
        ("ha_unsubscribe", topics.panel_to_ha),
        ("ha_unsubscribe", topics.discovery_probe),
        ("ha_unsubscribe", topics.retained),
        ("device_unsubscribe", topics.ha_to_panel),
        ("device_disconnect",),
        ("device_exit",),
    )
    assert context.client.messages_closed
    assert all(close_index < seam.events.index(action) for action in later_actions)
    assert {
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task() and task not in baseline_tasks and not task.done()
    } == set()


@pytest.mark.usefixtures("enable_custom_integrations")
@pytest.mark.parametrize("blocked", [False, True])
async def test_device_messages_close_failure_is_bounded_and_cleanup_continues(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
    blocked: bool,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    topics = SetupTopics.for_id(SETUP_ID)

    def configure(context: _FakeDeviceContext) -> None:
        if blocked:
            context.client.messages_close_blocker = True
        else:
            context.client.messages_close_error = RuntimeError(
                "secret device messages close failure"
            )

    factory = _FakeDeviceFactory(
        seam,
        seam.events,
        configure_context=configure,
    )
    baseline_tasks = {task for task in asyncio.all_tasks() if task is not asyncio.current_task()}

    with pytest.raises(OperationError) as raised:
        await BrokerValidator(hass, factory, 0.005).async_validate(
            _profile(),
            setup_id=SETUP_ID,
        )
    await asyncio.sleep(0)

    assert raised.value.stage is OperationStage.CLEANUP
    assert raised.value.code == "cleanup_failed"
    assert ("device_messages_close",) in seam.events
    assert ("ha_unsubscribe", topics.panel_to_ha) in seam.events
    assert ("device_unsubscribe", topics.ha_to_panel) in seam.events
    assert ("device_disconnect",) in seam.events
    assert ("device_exit",) in seam.events
    assert {
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task() and task not in baseline_tasks and not task.done()
    } == set()


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_primary_error_wins_and_carries_fixed_cleanup_error(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    topics = SetupTopics.for_id(SETUP_ID)
    seam.drop_device_to_ha.add(topics.panel_to_ha)

    def configure(context: _FakeDeviceContext) -> None:
        context.client.disconnect_error = RuntimeError("SECRET-CLOSE")

    factory = _FakeDeviceFactory(
        seam,
        seam.events,
        configure_context=configure,
    )

    with pytest.raises(OperationError) as raised:
        await BrokerValidator(hass, factory, 0.01).async_validate(
            _profile(),
            setup_id=SETUP_ID,
        )

    error = raised.value
    assert error.stage is OperationStage.PANEL_TO_HA
    assert error.code == "panel_to_ha_timeout"
    assert error.cleanup_error is not None
    assert error.cleanup_error.stage is OperationStage.CLEANUP
    assert error.cleanup_error.code == "cleanup_failed"
    assert factory.contexts[0].client.disconnect_count == 1
    assert factory.contexts[0].exit_count == 1


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_each_cleanup_action_is_bounded_and_later_actions_still_run(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    topics = SetupTopics.for_id(SETUP_ID)
    seam.ha_clear_blockers = {topics.discovery_probe, topics.retained}

    def configure(context: _FakeDeviceContext) -> None:
        context.client.clear_blockers = {
            topics.discovery_probe,
            topics.retained,
        }
        context.client.unsubscribe_blockers.add(topics.ha_to_panel)
        context.client.disconnect_blocker = True
        context.exit_blocker = True

    factory = _FakeDeviceFactory(
        seam,
        seam.events,
        configure_context=configure,
    )
    started = monotonic()

    with pytest.raises(OperationError) as raised:
        await BrokerValidator(hass, factory, 0.005).async_validate(
            _profile(),
            setup_id=SETUP_ID,
        )
    wall_elapsed = monotonic() - started

    assert raised.value.code == "cleanup_failed"
    # Seven intentionally hung asynchronous actions each receive an independent
    # five-millisecond bound; the complete cleanup remains finitely bounded.
    assert wall_elapsed < 0.25
    assert ("device_publish", topics.discovery_probe, b"", 1, True) in seam.events
    assert ("device_publish", topics.retained, b"", 1, True) in seam.events
    assert ("ha_publish", topics.discovery_probe, b"", 1, True) in seam.events
    assert ("ha_publish", topics.retained, b"", 1, True) in seam.events
    assert ("device_unsubscribe", topics.ha_to_panel) in seam.events
    assert ("device_disconnect",) in seam.events
    assert ("device_exit",) in seam.events


@pytest.mark.usefixtures("enable_custom_integrations")
@pytest.mark.parametrize("stage", list(OperationStage)[1:])
async def test_cancellation_during_every_stage_cleans_once_without_leaked_tasks(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
    stage: OperationStage,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    topics = SetupTopics.for_id(SETUP_ID)
    factory = _FakeDeviceFactory(seam, seam.events)
    marker: tuple[object, ...]
    if stage is OperationStage.HA_MQTT_READY:
        seam.wait_blocker = asyncio.Event()
        marker = ("ha_wait",)
    elif stage is OperationStage.FLEET_AUTH:

        def block_entry(context: _FakeDeviceContext) -> None:
            context.enter_blocker = asyncio.Event()

        factory.configure_context = block_entry
        marker = ("device_enter",)
    elif stage is OperationStage.PANEL_TO_HA:
        seam.drop_device_to_ha.add(topics.panel_to_ha)
        marker = ("device_publish", topics.panel_to_ha)
    elif stage is OperationStage.HA_TO_PANEL:
        seam.drop_ha_to_device.add(topics.ha_to_panel)
        marker = ("ha_publish", topics.ha_to_panel)
    elif stage is OperationStage.DISCOVERY_WRITE:
        seam.drop_device_to_ha.add(topics.discovery_probe)
        marker = ("device_publish", topics.discovery_probe)
    elif stage is OperationStage.RETAINED_MESSAGE:
        seam.retained_replay_overrides[topics.retained] = [
            _HaMessage(
                topics.retained,
                "stale-retained-payload",
                1,
                True,
                topics.retained,
            )
        ]
        marker = ("ha_subscribe", topics.retained, 1)
    else:

        def block_cleanup(context: _FakeDeviceContext) -> None:
            context.client.clear_blockers.add(topics.discovery_probe)

        factory.configure_context = block_cleanup
        marker = ("device_publish", topics.discovery_probe, b"", 1, True)

    baseline_tasks = {task for task in asyncio.all_tasks() if task is not asyncio.current_task()}
    task = asyncio.create_task(
        BrokerValidator(hass, factory, 0.5).async_validate(
            _profile(),
            setup_id=SETUP_ID,
        ),
        name=f"task6-cancel-{stage.value}",
    )
    if len(marker) == 2:
        await seam.events.wait_for(
            lambda event: len(event) > 1 and event[0] == marker[0] and event[1] == marker[1]
        )
    else:
        await _wait_for_event(seam.events, marker)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0)

    new_pending_tasks = {
        pending
        for pending in asyncio.all_tasks()
        if pending is not asyncio.current_task()
        and pending not in baseline_tasks
        and not pending.done()
    }
    assert new_pending_tasks == set()
    if stage is OperationStage.HA_MQTT_READY:
        assert factory.contexts == []
    elif stage is OperationStage.FLEET_AUTH:
        assert factory.contexts[0].client.disconnect_count == 0
        assert factory.contexts[0].exit_count == 0
    else:
        assert factory.contexts[0].client.disconnect_count == 1
        assert factory.contexts[0].exit_count == 1
        assert not seam.subscriptions


@pytest.mark.usefixtures("enable_custom_integrations")
@pytest.mark.parametrize(
    "control",
    [
        KeyboardInterrupt(),
        SystemExit(17),
        GeneratorExit(),
        BaseExceptionGroup("control", [KeyboardInterrupt()]),
    ],
)
async def test_process_control_base_exceptions_are_preserved(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
    control: BaseException,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    factory = _FakeDeviceFactory(
        seam,
        seam.events,
        enter_error=control,
    )

    with pytest.raises(type(control)) as raised:
        await BrokerValidator(hass, factory, 0.2).async_validate(
            _profile(),
            setup_id=SETUP_ID,
        )

    assert raised.value is control
    assert factory.contexts[0].client.disconnect_count == 0
    assert factory.contexts[0].exit_count == 0


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_fleet_auth_cancellation_settles_task5_entry_and_exit_without_orphan(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    raw_client = _BlockedSuccessfulRawClient()
    profile = _profile()

    def task5_factory(
        exact_profile: BrokerProfile,
        client_id: str,
    ) -> AbstractAsyncContextManager[DeviceMqttClient]:
        assert exact_profile is profile
        return exact_profile.device_client(client_id)

    with patch(
        "custom_components.brilliant_mqtt.broker.aiomqtt.Client",
        return_value=raw_client,
    ):
        task = asyncio.create_task(
            BrokerValidator(hass, task5_factory, 0.5).async_validate(
                profile,
                setup_id=SETUP_ID,
            )
        )
        await raw_client.enter_started.wait()
        task.cancel("task6-fleet-auth-cancel")
        await asyncio.sleep(0)
        assert not task.done()

        raw_client.release_enter.set()
        await raw_client.exit_started.wait()
        assert not task.done()

        raw_client.release_exit.set()
        with pytest.raises(asyncio.CancelledError) as raised:
            await task

    assert raised.value.args == ("task6-fleet-auth-cancel",)
    assert raw_client.enter_count == 1
    assert raw_client.exit_count == 1
    assert all(
        candidate.done()
        for candidate in asyncio.all_tasks()
        if candidate.get_name() in {"brilliant-mqtt-device-enter", "brilliant-mqtt-device-exit"}
    )


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_primary_and_cleanup_errors_expose_only_allowlisted_diagnostics(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    topics = SetupTopics.for_id(SETUP_ID)
    raw_secrets = {
        "SECRET-HOST.example.test",
        "SECRET-USERNAME",
        "SECRET-PASSWORD",
        "SECRET-CA-MATERIAL",
        "SECRET-NONCE",
        "SECRET-RAW-DISCOVERY-ERROR",
        "SECRET-RAW-CLEANUP-ERROR",
        "SECRET-ENVIRONMENT-VALUE",
    }
    profile = BrokerProfile(
        kind=BrokerKind.EXISTING_BROKER,
        host="SECRET-HOST.example.test",
        port=8883,
        tls_enabled=True,
        _username_value="SECRET-USERNAME",
        _password_value="SECRET-PASSWORD",
        _ca_pem_value=(
            "-----BEGIN CERTIFICATE-----\nSECRET-CA-MATERIAL\n-----END CERTIFICATE-----"
        ),
    )
    monkeypatch.setattr(
        "custom_components.brilliant_mqtt.broker_validation.secrets.token_urlsafe",
        lambda size: "SECRET-NONCE",
    )

    def configure(context: _FakeDeviceContext) -> None:
        context.client.publish_errors[topics.discovery_probe] = RuntimeError(
            "SECRET-RAW-DISCOVERY-ERROR SECRET-ENVIRONMENT-VALUE"
        )
        context.client.clear_errors[topics.discovery_probe] = RuntimeError(
            "SECRET-RAW-CLEANUP-ERROR SECRET-PASSWORD"
        )

    with pytest.raises(OperationError) as raised:
        await BrokerValidator(
            hass,
            _FakeDeviceFactory(
                seam,
                seam.events,
                configure_context=configure,
            ),
            0.02,
        ).async_validate(profile, setup_id=SETUP_ID)

    error = raised.value
    assert error.code == "operation_failed"
    assert error.cleanup_error is not None
    assert error.cleanup_error.code == "cleanup_failed"
    assert _direct_exception_chain(error) == [error]
    assert _direct_exception_chain(error.cleanup_error) == [error.cleanup_error]
    surfaces = "\n".join(
        (
            str(error),
            repr(error),
            repr(error.redacted_dict()),
            "".join(traceback_module.format_exception(error)),
            repr(_direct_exception_chain(error)),
            repr(error.cleanup_error),
            repr(error.cleanup_error.redacted_dict()),
        )
    )
    for secret in raw_secrets:
        assert secret not in surfaces


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_success_result_never_contains_profile_or_probe_secrets(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    profile = BrokerProfile(
        kind=BrokerKind.OFFICIAL_MOSQUITTO,
        host="result-secret-host.example.test",
        port=1883,
        tls_enabled=False,
        _username_value="result-secret-username",
        _password_value="result-secret-password",
        _ca_pem_value=None,
    )
    monkeypatch.setattr(
        "custom_components.brilliant_mqtt.broker_validation.secrets.token_urlsafe",
        lambda size: "result-secret-nonce",
    )

    result = await BrokerValidator(
        hass,
        _FakeDeviceFactory(seam, seam.events),
        0.2,
    ).async_validate(profile, setup_id=SETUP_ID)

    surface = f"{result!r}\n{result.redacted_dict()!r}"
    for secret in (
        "result-secret-host.example.test",
        "result-secret-username",
        "result-secret-password",
        "result-secret-nonce",
    ):
        assert secret not in surface


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_broker_kinds_follow_byte_for_byte_identical_validation_path(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mqtt_entry(hass)
    monkeypatch.setattr(
        "custom_components.brilliant_mqtt.broker_validation.secrets.token_urlsafe",
        lambda size: "kind-neutral",
    )
    event_runs: list[list[tuple[object, ...]]] = []

    for kind in (
        BrokerKind.OFFICIAL_MOSQUITTO,
        BrokerKind.EXISTING_BROKER,
    ):
        seam = _FakeHaMqtt()
        seam.install(monkeypatch)
        profile = _profile(kind)
        factory = _FakeDeviceFactory(seam, seam.events)

        await BrokerValidator(hass, factory, 0.2).async_validate(
            profile,
            setup_id=SETUP_ID,
        )

        assert factory.profiles == [profile]
        event_runs.append(seam.events)

    assert event_runs[0] == event_runs[1]


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_options_win_without_any_config_entry_mutation_api(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entry = _mqtt_entry(
        hass,
        data_prefix="zigbee",
        options_prefix="homeassistant",
    )
    before = (
        tuple(hass.config_entries.async_entries()),
        dict(entry.data),
        dict(entry.options),
        entry.state,
    )
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    update = Mock(side_effect=AssertionError("validator must not update entries"))
    reload_entry = AsyncMock(side_effect=AssertionError("validator must not reload entries"))
    create_flow = AsyncMock(side_effect=AssertionError("validator must not create entries"))

    with (
        patch.object(hass.config_entries, "async_update_entry", update),
        patch.object(hass.config_entries, "async_reload", reload_entry),
        patch.object(hass.config_entries.flow, "async_init", create_flow),
    ):
        result = await BrokerValidator(
            hass,
            _FakeDeviceFactory(seam, seam.events),
            0.2,
        ).async_validate(_profile(), setup_id=SETUP_ID)

    assert result.completed_stages[-1] is OperationStage.CLEANUP
    assert (
        tuple(hass.config_entries.async_entries()),
        dict(entry.data),
        dict(entry.options),
        entry.state,
    ) == before
    update.assert_not_called()
    reload_entry.assert_not_awaited()
    create_flow.assert_not_awaited()


def _successful_panel_report(setup_id: UUID) -> PreflightReport:
    return PreflightReport(
        setup_id=setup_id,
        success=True,
        completed_stages=tuple(PreflightStage),
        stage_elapsed_ms={stage: 1 for stage in PreflightStage},
        last_stage=PreflightStage.CLEANUP,
    )


def _failed_panel_report(
    setup_id: UUID,
    stage: PreflightStage,
    code: str,
    detail: str,
) -> PreflightReport:
    work_stages = tuple(PreflightStage)[:-1]
    if stage is PreflightStage.CLEANUP:
        completed = work_stages
    else:
        completed = work_stages[: work_stages.index(stage)] + (PreflightStage.CLEANUP,)
    elapsed_stages = set(completed) | {stage, PreflightStage.CLEANUP}
    return PreflightReport(
        setup_id=setup_id,
        success=False,
        completed_stages=completed,
        stage_elapsed_ms={elapsed_stage: 1 for elapsed_stage in elapsed_stages},
        failed_stage=stage,
        error_code=code,
        detail=detail,
    )


async def _deliver_panel_request(
    seam: _FakeHaMqtt,
    request: PreflightRequest,
) -> None:
    topics = SetupTopics.for_id(request.setup_id)
    await seam.deliver_from_device(
        topics.panel_to_ha,
        SetupRequest(request.setup_id, request.panel_nonce).to_payload(),
        1,
        False,
    )


async def _deliver_discovery_probe(
    seam: _FakeHaMqtt,
    request: PreflightRequest,
) -> None:
    await seam.deliver_from_device(
        SetupTopics.for_id(request.setup_id).discovery_probe,
        request.panel_nonce,
        1,
        False,
    )


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_panel_preflight_subacks_before_launch_and_exact_round_trip(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    launched_requests: list[str] = []

    async def launcher(raw_request: str) -> _FakePanelProcess:
        launched_requests.append(raw_request)
        seam.events.append(("panel_launcher",))
        request = PreflightRequest.from_json(raw_request)
        await _deliver_panel_request(seam, request)
        await _deliver_discovery_probe(seam, request)
        return _FakePanelProcess(
            RunResult(0, _successful_panel_report(request.setup_id).to_json() + "\n", ""),
            seam.events,
        )

    result = await BrokerValidator(
        hass,
        cast(Any, object()),
        timeout_seconds=0.1,
    ).async_validate_panel(_profile(), launcher, setup_id=SETUP_ID)

    topics = SetupTopics.for_id(SETUP_ID)
    request = PreflightRequest.from_json(launched_requests[0])
    launch_index = seam.events.index(("panel_launcher",))
    assert seam.events.index(("ha_suback", topics.panel_to_ha, 1)) < launch_index
    assert seam.events.index(("ha_suback", topics.discovery_probe, 1)) < launch_index
    assert len(request.panel_nonce) <= 256
    assert len(request.ha_nonce) <= 256
    assert request.panel_nonce != request.ha_nonce
    assert (
        "ha_publish",
        topics.ha_to_panel,
        SetupResult(SETUP_ID, request.ha_nonce, request.panel_nonce).to_payload(),
        1,
        False,
    ) in seam.events
    assert result.setup_id == SETUP_ID
    assert result.completed_stages == tuple(OperationStage)[1:]
    assert ("ha_unsubscribe", topics.panel_to_ha) in seam.events
    assert ("ha_unsubscribe", topics.discovery_probe) in seam.events
    assert ("ha_publish", topics.discovery_probe, b"", 1, True) in seam.events
    assert ("ha_publish", topics.retained, b"", 1, True) in seam.events
    assert not seam.subscriptions
    assert not seam.retained


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_panel_preflight_never_launches_before_both_subacks(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    topics = SetupTopics.for_id(SETUP_ID)
    seam.suppress_suback.add(topics.discovery_probe)
    launches = 0

    async def launcher(raw_request: str) -> _FakePanelProcess:
        nonlocal launches
        launches += 1
        raise AssertionError(raw_request)

    with pytest.raises(OperationError) as raised:
        await BrokerValidator(
            hass,
            cast(Any, object()),
            timeout_seconds=0.01,
        ).async_validate_panel(_profile(), launcher, setup_id=SETUP_ID)

    assert launches == 0
    assert raised.value.stage is OperationStage.DISCOVERY_WRITE
    assert raised.value.code == "discovery_write_timeout"
    assert ("ha_unsubscribe", topics.panel_to_ha) in seam.events
    assert ("ha_unsubscribe", topics.discovery_probe) in seam.events
    assert ("ha_publish", topics.discovery_probe, b"", 1, True) in seam.events
    assert ("ha_publish", topics.retained, b"", 1, True) in seam.events


@pytest.mark.usefixtures("enable_custom_integrations")
@pytest.mark.parametrize(
    ("invalid_observation", "expected_stage", "expected_code"),
    [
        (
            "panel_request",
            OperationStage.PANEL_TO_HA,
            "panel_to_ha_timeout",
        ),
        (
            "discovery",
            OperationStage.DISCOVERY_WRITE,
            "discovery_write_timeout",
        ),
        (
            "retained_discovery",
            OperationStage.DISCOVERY_WRITE,
            "discovery_write_timeout",
        ),
    ],
)
async def test_panel_preflight_stale_observations_never_satisfy_success(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
    invalid_observation: str,
    expected_stage: OperationStage,
    expected_code: str,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)

    async def launcher(raw_request: str) -> _FakePanelProcess:
        request = PreflightRequest.from_json(raw_request)
        topics = SetupTopics.for_id(request.setup_id)
        if invalid_observation == "panel_request":
            await seam.deliver_from_device(
                topics.panel_to_ha,
                SetupRequest(OTHER_SETUP_ID, request.panel_nonce).to_payload(),
                1,
                False,
            )
        else:
            await _deliver_panel_request(seam, request)
            await seam.deliver_from_device(
                topics.discovery_probe,
                (
                    request.panel_nonce
                    if invalid_observation == "retained_discovery"
                    else f"stale-{request.panel_nonce}"
                ),
                1,
                True,
            )
        return _FakePanelProcess(
            RunResult(0, _successful_panel_report(request.setup_id).to_json() + "\n", ""),
            seam.events,
        )

    with pytest.raises(OperationError) as raised:
        await BrokerValidator(
            hass,
            cast(Any, object()),
            timeout_seconds=0.05,
        ).async_validate_panel(_profile(), launcher, setup_id=SETUP_ID)

    assert raised.value.stage is expected_stage
    assert raised.value.code == expected_code
    assert _direct_exception_chain(raised.value) == [raised.value]


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_panel_preflight_waits_for_live_discovery_dispatch_after_process_exit(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    delivery_completed = asyncio.Event()

    async def launcher(raw_request: str) -> _FakePanelProcess:
        request = PreflightRequest.from_json(raw_request)
        await _deliver_panel_request(seam, request)

        async def deliver_after_stdout() -> None:
            await asyncio.sleep(0.01)
            await _deliver_discovery_probe(seam, request)
            delivery_completed.set()

        asyncio.create_task(
            deliver_after_stdout(),
            name="test-delayed-live-discovery",
        )
        return _FakePanelProcess(
            RunResult(0, _successful_panel_report(request.setup_id).to_json() + "\n", ""),
            seam.events,
        )

    result = await BrokerValidator(
        hass,
        cast(Any, object()),
        timeout_seconds=0.05,
    ).async_validate_panel(_profile(), launcher, setup_id=SETUP_ID)

    assert delivery_completed.is_set()
    assert result.completed_stages == tuple(OperationStage)[1:]


@pytest.mark.usefixtures("enable_custom_integrations")
@pytest.mark.parametrize(
    "invalid_output",
    [
        "extra_stdout",
        "oversized",
        "extra_key",
        "wrong_setup_id",
        "wrong_order",
        "missing_cleanup",
        "nonzero_success",
    ],
)
async def test_panel_preflight_rejects_adversarial_process_reports(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
    invalid_output: str,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)

    async def launcher(raw_request: str) -> _FakePanelProcess:
        request = PreflightRequest.from_json(raw_request)
        await _deliver_panel_request(seam, request)
        await _deliver_discovery_probe(seam, request)
        report_json = _successful_panel_report(request.setup_id).to_json()
        stderr = ""
        exit_status = 0
        if invalid_output == "extra_stdout":
            report_json += "\nraw-extra-output-secret"
        elif invalid_output == "oversized":
            report_json = "x" * (MAX_PREFLIGHT_REPORT_BYTES + 1)
        elif invalid_output == "extra_key":
            value = json.loads(report_json)
            value["raw-extra-secret"] = True
            report_json = json.dumps(value, sort_keys=True, separators=(",", ":"))
        elif invalid_output == "wrong_setup_id":
            report_json = _successful_panel_report(OTHER_SETUP_ID).to_json()
        elif invalid_output in {"wrong_order", "missing_cleanup"}:
            value = json.loads(report_json)
            completed = cast(list[str], value["completed_stages"])
            if invalid_output == "wrong_order":
                completed[1], completed[2] = completed[2], completed[1]
            else:
                completed.pop()
            report_json = json.dumps(value, sort_keys=True, separators=(",", ":"))
        elif invalid_output == "nonzero_success":
            exit_status = 1
        return _FakePanelProcess(
            RunResult(exit_status, report_json + "\n", stderr),
            seam.events,
        )

    with pytest.raises(OperationError) as raised:
        await BrokerValidator(
            hass,
            cast(Any, object()),
            timeout_seconds=0.05,
        ).async_validate_panel(_profile(), launcher, setup_id=SETUP_ID)

    error = raised.value
    assert error.stage is OperationStage.CLEANUP
    assert error.code == "operation_failed"
    assert _direct_exception_chain(error) == [error]
    surface = f"{error!r}\n{error.redacted_dict()!r}\n{traceback_module.format_exception(error)!r}"
    assert "raw-stderr-secret" not in surface
    assert "raw-extra-output-secret" not in surface
    assert "raw-extra-secret" not in surface


def test_valid_panel_success_report_survives_untrusted_stderr() -> None:
    report = _successful_panel_report(SETUP_ID)

    parsed, error = broker_validation._parsed_panel_report(
        RunResult(0, report.to_json() + "\n", "raw-stderr-secret"),
        SETUP_ID,
    )

    assert parsed == report
    assert error is None


def test_valid_panel_failure_report_survives_untrusted_stderr() -> None:
    report = _failed_panel_report(
        SETUP_ID,
        PreflightStage.FLEET_AUTH,
        "mqtt_connect",
        "MQTT connection failed",
    )

    parsed, error = broker_validation._parsed_panel_report(
        RunResult(1, report.to_json() + "\n", "raw-stderr-secret"),
        SETUP_ID,
    )

    assert parsed is None
    assert error is not None
    assert error.stage is OperationStage.FLEET_AUTH
    assert error.code == "broker_connect_failed"
    surface = f"{error!r}\n{error.redacted_dict()!r}"
    assert "raw-stderr-secret" not in surface


@pytest.mark.usefixtures("enable_custom_integrations")
@pytest.mark.parametrize(
    ("stage", "report_code", "detail", "expected_stage", "expected_code"),
    [
        (
            PreflightStage.FLEET_AUTH,
            "settings_invalid",
            "Panel configuration invalid",
            OperationStage.FLEET_AUTH,
            "panel_settings_invalid",
        ),
        (
            PreflightStage.FLEET_AUTH,
            "mqtt_connect",
            "MQTT connection failed",
            OperationStage.FLEET_AUTH,
            "broker_connect_failed",
        ),
        (
            PreflightStage.DISCOVERY_WRITE,
            "mqtt_publish",
            "MQTT publish failed",
            OperationStage.DISCOVERY_WRITE,
            "operation_failed",
        ),
        (
            PreflightStage.DISCOVERY_WRITE,
            "mqtt_authorization",
            "MQTT authorization rejected",
            OperationStage.DISCOVERY_WRITE,
            "discovery_write_denied",
        ),
        (
            PreflightStage.DISCOVERY_WRITE,
            "mqtt_timeout",
            "MQTT stage timed out",
            OperationStage.DISCOVERY_WRITE,
            "discovery_write_timeout",
        ),
        (
            PreflightStage.RETAINED_MESSAGE,
            "retained_flag_missing",
            "Retained replay flag was missing",
            OperationStage.RETAINED_MESSAGE,
            "retained_message_invalid",
        ),
        (
            PreflightStage.RETAINED_MESSAGE,
            "mqtt_timeout",
            "MQTT stage timed out",
            OperationStage.RETAINED_MESSAGE,
            "retained_message_timeout",
        ),
    ],
)
async def test_panel_preflight_maps_only_explicit_evidence(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
    stage: PreflightStage,
    report_code: str,
    detail: str,
    expected_stage: OperationStage,
    expected_code: str,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)

    async def launcher(raw_request: str) -> _FakePanelProcess:
        request = PreflightRequest.from_json(raw_request)
        if stage is not PreflightStage.FLEET_AUTH:
            await _deliver_panel_request(seam, request)
        if stage is PreflightStage.RETAINED_MESSAGE:
            await _deliver_discovery_probe(seam, request)
        report = _failed_panel_report(request.setup_id, stage, report_code, detail)
        return _FakePanelProcess(
            RunResult(1, report.to_json() + "\n", ""),
            seam.events,
        )

    with pytest.raises(OperationError) as raised:
        await BrokerValidator(
            hass,
            cast(Any, object()),
            timeout_seconds=0.05,
        ).async_validate_panel(_profile(), launcher, setup_id=SETUP_ID)

    assert raised.value.stage is expected_stage
    assert raised.value.code == expected_code
    assert _direct_exception_chain(raised.value) == [raised.value]


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_panel_preflight_report_preserves_primary_and_attaches_missing_cleanup(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)

    async def launcher(raw_request: str) -> _FakePanelProcess:
        request = PreflightRequest.from_json(raw_request)
        await _deliver_panel_request(seam, request)
        report = PreflightReport(
            setup_id=request.setup_id,
            success=False,
            completed_stages=(
                PreflightStage.FLEET_AUTH,
                PreflightStage.PANEL_TO_HA,
                PreflightStage.HA_TO_PANEL,
            ),
            stage_elapsed_ms={
                PreflightStage.FLEET_AUTH: 1,
                PreflightStage.PANEL_TO_HA: 1,
                PreflightStage.HA_TO_PANEL: 1,
                PreflightStage.DISCOVERY_WRITE: 1,
                PreflightStage.CLEANUP: 1,
            },
            failed_stage=PreflightStage.DISCOVERY_WRITE,
            error_code="mqtt_timeout",
            detail="MQTT stage timed out",
        )
        return _FakePanelProcess(
            RunResult(1, report.to_json() + "\n", ""),
            seam.events,
        )

    with pytest.raises(OperationError) as raised:
        await BrokerValidator(
            hass,
            cast(Any, object()),
            timeout_seconds=0.05,
        ).async_validate_panel(_profile(), launcher, setup_id=SETUP_ID)

    error = raised.value
    assert error.stage is OperationStage.DISCOVERY_WRITE
    assert error.code == "discovery_write_timeout"
    assert error.cleanup_error is not None
    assert error.cleanup_error.stage is OperationStage.CLEANUP
    assert error.cleanup_error.code == "cleanup_failed"


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_panel_preflight_outer_timeout_terminates_once_settles_then_cleans(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    topics = SetupTopics.for_id(SETUP_ID)
    process: _FakePanelProcess | None = None

    async def launcher(raw_request: str) -> _FakePanelProcess:
        nonlocal process
        request = PreflightRequest.from_json(raw_request)
        process = _FakePanelProcess(
            RunResult(0, _successful_panel_report(request.setup_id).to_json() + "\n", ""),
            seam.events,
            settled=False,
        )
        return process

    with pytest.raises(OperationError) as raised:
        await BrokerValidator(
            hass,
            cast(Any, object()),
            timeout_seconds=0.01,
        ).async_validate_panel(_profile(), launcher, setup_id=SETUP_ID)

    assert process is not None
    assert process.terminate_count == 1
    assert process.running is False
    settled_index = max(
        index for index, event in enumerate(seam.events) if event[0] == "panel_process_settled"
    )
    cleanup_index = min(
        index
        for index, event in enumerate(seam.events)
        if event
        in {
            ("ha_unsubscribe", topics.panel_to_ha),
            ("ha_unsubscribe", topics.discovery_probe),
            ("ha_publish", topics.discovery_probe, b"", 1, True),
            ("ha_publish", topics.retained, b"", 1, True),
        }
    )
    assert settled_index < cleanup_index
    assert raised.value.stage is OperationStage.PANEL_TO_HA
    assert raised.value.code == "panel_to_ha_timeout"


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_panel_preflight_settlement_has_a_hard_transport_bound(
    hass: HomeAssistant,
) -> None:
    validator = BrokerValidator(
        hass,
        cast(Any, object()),
        timeout_seconds=0.01,
    )

    failed, control = await asyncio.wait_for(
        validator._async_settle_panel_process(_HungPanelProcess()),
        timeout=0.2,
    )

    assert failed is True
    assert control is None


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_panel_preflight_settlement_preserves_same_turn_caller_cancel(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    validator = BrokerValidator(
        hass,
        cast(Any, object()),
        timeout_seconds=10,
    )
    loop = asyncio.get_running_loop()
    settlement_task: asyncio.Task[tuple[bool, BaseException | None]] | None = None

    def immediate_deadline(
        _delay: float,
        callback: Callable[..., object],
        *args: object,
        context: contextvars.Context | None = None,
    ) -> asyncio.Handle:
        deadline = loop.call_soon(callback, *args, context=context)
        assert settlement_task is not None
        loop.call_soon(settlement_task.cancel, "caller-cancelled")
        return deadline

    monkeypatch.setattr(loop, "call_later", immediate_deadline)
    settlement_task = asyncio.create_task(
        validator._async_settle_panel_process(_HungPanelProcess())
    )

    failed, control = await settlement_task

    assert failed is True
    assert isinstance(control, asyncio.CancelledError)
    assert control.args == ("caller-cancelled",)


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_panel_preflight_cancellation_wins_after_settlement_and_all_cleanup(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    topics = SetupTopics.for_id(SETUP_ID)
    launched = asyncio.Event()
    process: _FakePanelProcess | None = None
    for topic in (topics.panel_to_ha, topics.discovery_probe):
        seam.ha_unsubscribe_errors[topic] = RuntimeError("raw-unsubscribe-secret")
    for topic in (topics.discovery_probe, topics.retained):
        seam.ha_clear_errors[topic] = RuntimeError("raw-clear-secret")

    async def launcher(raw_request: str) -> _FakePanelProcess:
        nonlocal process
        request = PreflightRequest.from_json(raw_request)
        process = _FakePanelProcess(
            RunResult(0, _successful_panel_report(request.setup_id).to_json() + "\n", ""),
            seam.events,
            settled=False,
        )
        launched.set()
        return process

    task = asyncio.create_task(
        BrokerValidator(
            hass,
            cast(Any, object()),
            timeout_seconds=0.05,
        ).async_validate_panel(_profile(), launcher, setup_id=SETUP_ID)
    )
    await asyncio.wait_for(launched.wait(), timeout=0.2)
    task.cancel("caller-cancelled")

    with pytest.raises(asyncio.CancelledError) as raised:
        await task

    assert raised.value.args == ("caller-cancelled",)
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None
    assert process is not None
    assert process.terminate_count == 1
    assert process.running is False
    assert ("ha_unsubscribe", topics.panel_to_ha) in seam.events
    assert ("ha_unsubscribe", topics.discovery_probe) in seam.events
    assert ("ha_publish", topics.discovery_probe, b"", 1, True) in seam.events
    assert ("ha_publish", topics.retained, b"", 1, True) in seam.events


@pytest.mark.usefixtures("enable_custom_integrations")
async def test_panel_preflight_primary_and_cleanup_failures_are_fully_redacted(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _mqtt_entry(hass)
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    topics = SetupTopics.for_id(SETUP_ID)
    seam.ha_clear_errors[topics.discovery_probe] = RuntimeError("raw-cleanup-secret")

    async def launcher(raw_request: str) -> _FakePanelProcess:
        return _FakePanelProcess(
            RunResult(1, '{"raw-report-secret":true}\n', "raw-stderr-secret"),
            seam.events,
        )

    with pytest.raises(OperationError) as raised:
        await BrokerValidator(
            hass,
            cast(Any, object()),
            timeout_seconds=0.05,
        ).async_validate_panel(_profile(), launcher, setup_id=SETUP_ID)

    error = raised.value
    assert error.code == "operation_failed"
    assert error.cleanup_error is not None
    assert error.cleanup_error.code == "cleanup_failed"
    assert _direct_exception_chain(error) == [error]
    assert _direct_exception_chain(error.cleanup_error) == [error.cleanup_error]
    surface = f"{error!r}\n{error.redacted_dict()!r}\n{traceback_module.format_exception(error)!r}"
    for secret in ("raw-report-secret", "raw-stderr-secret", "raw-cleanup-secret"):
        assert secret not in surface
