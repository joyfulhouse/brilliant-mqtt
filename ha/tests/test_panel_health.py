"""Fresh post-activation MQTT health evidence for fleet provisioning."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from time import monotonic
from typing import Any

import pytest
from custom_components.brilliant_mqtt.panel_health import (
    MAX_HEALTH_PAYLOAD_BYTES,
    PanelHealthError,
    PanelHealthObserver,
)
from homeassistant.components import mqtt
from homeassistant.components.mqtt.models import ReceiveMessage
from homeassistant.core import HomeAssistant

PANEL = "office"
VERSION = "0.7.0"
AVAILABILITY = "brilliant/office/availability"
METADATA = "brilliant/office/bridge"
STATE_FILTER = "brilliant/office/+/state"
STATE = "brilliant/office/load-1/state"
DISCOVERY_FILTER = "homeassistant/+/+/config"
DISCOVERY = "homeassistant/light/brilliant_office_load-1/config"

type MessageCallback = Callable[[ReceiveMessage], Any]


@dataclass(slots=True)
class _FakeHaMqtt:
    events: list[tuple[object, ...]] = field(default_factory=list)
    callbacks: dict[str, MessageCallback] = field(default_factory=dict)
    status_callbacks: dict[str, Callable[[], None]] = field(default_factory=dict)
    missing_ack: set[str] = field(default_factory=set)
    subscribe_errors: dict[str, BaseException] = field(default_factory=dict)
    unsubscribe_errors: set[str] = field(default_factory=set)

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(mqtt, "async_on_subscribe_done", self.on_subscribe_done)
        monkeypatch.setattr(mqtt, "async_subscribe", self.async_subscribe)

    def on_subscribe_done(
        self,
        hass: HomeAssistant,
        topic: str,
        qos: int,
        callback: Callable[[], None],
    ) -> Callable[[], None]:
        del hass
        self.events.append(("status_watch", topic, qos))
        self.status_callbacks[topic] = callback

        def remove() -> None:
            self.events.append(("status_unwatch", topic))
            self.status_callbacks.pop(topic, None)

        return remove

    async def async_subscribe(
        self,
        hass: HomeAssistant,
        topic: str,
        callback: MessageCallback,
        qos: int = 0,
        encoding: str | None = "utf-8",
    ) -> Callable[[], None]:
        del hass, encoding
        self.events.append(("subscribe", topic, qos))
        if error := self.subscribe_errors.get(topic):
            raise error
        self.callbacks[topic] = callback
        if topic not in self.missing_ack:
            self.status_callbacks[topic]()

        def unsubscribe() -> None:
            self.events.append(("unsubscribe", topic))
            self.callbacks.pop(topic, None)
            if topic in self.unsubscribe_errors:
                raise RuntimeError("SECRET unsubscribe detail")

        return unsubscribe

    def fire(
        self,
        subscribed_topic: str,
        topic: str,
        payload: object,
        *,
        retain: bool = False,
        timestamp: float | None = None,
    ) -> None:
        callback = self.callbacks[subscribed_topic]
        callback(
            ReceiveMessage(
                topic=topic,
                payload=payload,  # type: ignore[arg-type]
                qos=1,
                retain=retain,
                subscribed_topic=subscribed_topic,
                timestamp=monotonic() if timestamp is None else timestamp,
            )
        )


@pytest.fixture
def ha_mqtt(monkeypatch: pytest.MonkeyPatch) -> _FakeHaMqtt:
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    return seam


def _discovery(*, identifier: str = "brilliant_panel_office") -> str:
    return (
        '{"availability":[{"topic":"brilliant/office/availability"}],'
        f'"device":{{"identifiers":["{identifier}"]}},'
        '"state_topic":"brilliant/office/load-1/state",'
        '"unique_id":"brilliant_office_load-1"}'
    )


def _fire_online(seam: _FakeHaMqtt) -> None:
    seam.fire(AVAILABILITY, AVAILABILITY, "online")


def _fire_complete_health(seam: _FakeHaMqtt) -> None:
    _fire_online(seam)
    seam.fire(METADATA, METADATA, f'{{"agent_version":"{VERSION}"}}')
    seam.fire(DISCOVERY_FILTER, DISCOVERY, _discovery())
    seam.fire(STATE_FILTER, STATE, '{"state":"OFF"}')


def _exception_graph(root: BaseException) -> list[BaseException]:
    found: list[BaseException] = []
    pending = [root]
    seen: set[int] = set()
    while pending:
        error = pending.pop()
        if id(error) in seen:
            continue
        seen.add(id(error))
        found.append(error)
        if error.__context__ is not None:
            pending.append(error.__context__)
        if error.__cause__ is not None:
            pending.append(error.__cause__)
        pending.extend(argument for argument in error.args if isinstance(argument, BaseException))
    return found


async def _subscribed_observer(
    hass: HomeAssistant,
    *,
    subscription_timeout: float = 0.05,
) -> PanelHealthObserver:
    observer = PanelHealthObserver(
        hass,
        PANEL,
        subscription_timeout=subscription_timeout,
    )
    await observer.async_subscribe()
    return observer


async def test_subscribes_and_confirms_every_topic_before_activation(
    hass: HomeAssistant,
    ha_mqtt: _FakeHaMqtt,
) -> None:
    observer = await _subscribed_observer(hass)

    assert [event for event in ha_mqtt.events if event[0] == "subscribe"] == [
        ("subscribe", AVAILABILITY, 1),
        ("subscribe", METADATA, 1),
        ("subscribe", STATE_FILTER, 1),
        ("subscribe", DISCOVERY_FILTER, 1),
    ]
    assert not ha_mqtt.status_callbacks

    observer.mark_activation_started()
    await observer.async_close()
    assert not ha_mqtt.callbacks


async def test_success_requires_one_coherent_fresh_post_online_session(
    hass: HomeAssistant,
    ha_mqtt: _FakeHaMqtt,
) -> None:
    observer = await _subscribed_observer(hass)

    # Broker replays and live messages received before activation cannot seed a gate.
    for retained in (True, False):
        ha_mqtt.fire(AVAILABILITY, AVAILABILITY, "online", retain=retained)
        ha_mqtt.fire(METADATA, METADATA, f'{{"agent_version":"{VERSION}"}}', retain=retained)
        ha_mqtt.fire(DISCOVERY_FILTER, DISCOVERY, _discovery(), retain=retained)
        ha_mqtt.fire(STATE_FILTER, STATE, '{"state":"OFF"}', retain=retained)

    observer.mark_activation_started()

    # Post-activation metadata/state/discovery still do not count until fresh online.
    ha_mqtt.fire(METADATA, METADATA, f'{{"agent_version":"{VERSION}"}}')
    ha_mqtt.fire(DISCOVERY_FILTER, DISCOVERY, _discovery())
    ha_mqtt.fire(STATE_FILTER, STATE, '{"state":"OFF"}')
    _fire_complete_health(ha_mqtt)

    evidence = await observer.async_wait(VERSION, timeout=0.05)

    assert evidence.panel == PANEL
    assert evidence.agent_version == VERSION
    assert evidence.state_topic == STATE
    assert evidence.discovery_topic == DISCOVERY
    assert evidence.device_identifier == "brilliant_panel_office"
    assert not ha_mqtt.callbacks
    assert [event for event in ha_mqtt.events if event[0] == "unsubscribe"] == [
        ("unsubscribe", DISCOVERY_FILTER),
        ("unsubscribe", STATE_FILTER),
        ("unsubscribe", METADATA),
        ("unsubscribe", AVAILABILITY),
    ]


async def test_retained_and_pre_activation_or_old_timestamp_messages_time_out(
    hass: HomeAssistant,
    ha_mqtt: _FakeHaMqtt,
) -> None:
    observer = await _subscribed_observer(hass)
    pre_activation_timestamp = monotonic() - 10
    observer.mark_activation_started()

    for retain, timestamp in (
        (True, None),
        (False, pre_activation_timestamp),
    ):
        ha_mqtt.fire(
            AVAILABILITY,
            AVAILABILITY,
            "online",
            retain=retain,
            timestamp=timestamp,
        )
        ha_mqtt.fire(
            METADATA,
            METADATA,
            f'{{"agent_version":"{VERSION}"}}',
            retain=retain,
            timestamp=timestamp,
        )
        ha_mqtt.fire(
            DISCOVERY_FILTER,
            DISCOVERY,
            _discovery(),
            retain=retain,
            timestamp=timestamp,
        )
        ha_mqtt.fire(
            STATE_FILTER,
            STATE,
            '{"state":"OFF"}',
            retain=retain,
            timestamp=timestamp,
        )

    with pytest.raises(PanelHealthError) as raised:
        await observer.async_wait(VERSION, timeout=0.01)

    assert raised.value.code == "panel_health_timeout"
    assert raised.value.retryable is True
    assert _exception_graph(raised.value) == [raised.value]
    assert not ha_mqtt.callbacks


async def test_offline_after_online_clears_partial_session_evidence(
    hass: HomeAssistant,
    ha_mqtt: _FakeHaMqtt,
) -> None:
    observer = await _subscribed_observer(hass)
    observer.mark_activation_started()
    _fire_online(ha_mqtt)
    ha_mqtt.fire(METADATA, METADATA, f'{{"agent_version":"{VERSION}"}}')
    ha_mqtt.fire(AVAILABILITY, AVAILABILITY, "offline")
    ha_mqtt.fire(DISCOVERY_FILTER, DISCOVERY, _discovery())
    ha_mqtt.fire(STATE_FILTER, STATE, '{"state":"OFF"}')

    with pytest.raises(PanelHealthError) as raised:
        await observer.async_wait(VERSION, timeout=0.01)

    assert raised.value.code == "panel_health_timeout"


async def test_wrong_version_fails_closed_with_redacted_typed_error(
    hass: HomeAssistant,
    ha_mqtt: _FakeHaMqtt,
) -> None:
    observer = await _subscribed_observer(hass)
    observer.mark_activation_started()
    _fire_online(ha_mqtt)
    ha_mqtt.fire(
        METADATA,
        METADATA,
        '{"agent_version":"SECRET-wrong-version"}',
    )

    with pytest.raises(PanelHealthError) as raised:
        await observer.async_wait(VERSION, timeout=0.05)

    assert raised.value.code == "panel_health_version_mismatch"
    assert "SECRET" not in str(raised.value)
    assert "SECRET" not in repr(raised.value)
    assert not ha_mqtt.callbacks


async def test_wrong_slug_topic_cannot_satisfy_health(
    hass: HomeAssistant,
    ha_mqtt: _FakeHaMqtt,
) -> None:
    observer = await _subscribed_observer(hass)
    observer.mark_activation_started()
    _fire_online(ha_mqtt)
    ha_mqtt.fire(METADATA, METADATA, f'{{"agent_version":"{VERSION}"}}')
    ha_mqtt.fire(
        STATE_FILTER,
        "brilliant/kitchen/load-1/state",
        '{"state":"OFF"}',
    )
    ha_mqtt.fire(
        DISCOVERY_FILTER,
        "homeassistant/light/brilliant_kitchen_load-1/config",
        _discovery(identifier="brilliant_panel_kitchen"),
    )

    with pytest.raises(PanelHealthError) as raised:
        await observer.async_wait(VERSION, timeout=0.01)

    assert raised.value.code == "panel_health_timeout"


async def test_wrong_discovery_device_identity_cannot_satisfy_health(
    hass: HomeAssistant,
    ha_mqtt: _FakeHaMqtt,
) -> None:
    observer = await _subscribed_observer(hass)
    observer.mark_activation_started()
    _fire_online(ha_mqtt)
    ha_mqtt.fire(
        DISCOVERY_FILTER,
        DISCOVERY,
        _discovery(identifier="brilliant_panel_kitchen"),
    )

    with pytest.raises(PanelHealthError) as raised:
        await observer.async_wait(VERSION, timeout=0.01)

    assert raised.value.code == "panel_health_timeout"
    assert not ha_mqtt.callbacks


@pytest.mark.parametrize(
    ("subscribed_topic", "topic", "payload"),
    [
        (METADATA, METADATA, "{"),
        (METADATA, METADATA, '{"agent_version":"0.7.0","agent_version":"0.8.0"}'),
        (METADATA, METADATA, "\ud800"),
        (STATE_FILTER, STATE, "[]"),
        (STATE_FILTER, STATE, '{"value":' + "9" * 5000 + "}"),
        (STATE_FILTER, STATE, '{"nested":' + "[" * 1000 + "]" * 1000 + "}"),
        (STATE_FILTER, STATE, "x" * (MAX_HEALTH_PAYLOAD_BYTES + 1)),
    ],
)
async def test_fresh_targeted_malformed_or_oversized_payload_fails_redacted(
    hass: HomeAssistant,
    ha_mqtt: _FakeHaMqtt,
    subscribed_topic: str,
    topic: str,
    payload: str,
) -> None:
    observer = await _subscribed_observer(hass)
    observer.mark_activation_started()
    _fire_online(ha_mqtt)
    ha_mqtt.fire(subscribed_topic, topic, payload)

    with pytest.raises(PanelHealthError) as raised:
        await observer.async_wait(VERSION, timeout=0.05)

    assert raised.value.code == "panel_health_payload_invalid"
    assert "x" * 20 not in str(raised.value)
    assert not ha_mqtt.callbacks


@pytest.mark.parametrize(
    "payload",
    [
        '{"device":',
        "\ud800",
        "x" * (MAX_HEALTH_PAYLOAD_BYTES + 1),
    ],
)
async def test_malformed_global_discovery_is_rejected_without_cross_panel_failure(
    hass: HomeAssistant,
    ha_mqtt: _FakeHaMqtt,
    payload: str,
) -> None:
    observer = await _subscribed_observer(hass)
    observer.mark_activation_started()
    _fire_online(ha_mqtt)
    ha_mqtt.fire(DISCOVERY_FILTER, DISCOVERY, payload)

    with pytest.raises(PanelHealthError) as raised:
        await observer.async_wait(VERSION, timeout=0.01)

    assert raised.value.code == "panel_health_timeout"
    assert not ha_mqtt.callbacks


async def test_prefix_colliding_panel_discovery_is_ignored(
    hass: HomeAssistant,
    ha_mqtt: _FakeHaMqtt,
) -> None:
    observer = await _subscribed_observer(hass)
    observer.mark_activation_started()
    _fire_online(ha_mqtt)
    ha_mqtt.fire(
        DISCOVERY_FILTER,
        "homeassistant/light/brilliant_office_bath_load-1/config",
        _discovery(identifier="brilliant_panel_office_bath").replace(
            "brilliant_office_load-1",
            "brilliant_office_bath_load-1",
        ),
    )

    with pytest.raises(PanelHealthError) as raised:
        await observer.async_wait(VERSION, timeout=0.01)

    assert raised.value.code == "panel_health_timeout"


async def test_discovery_and_state_may_prove_distinct_entities_on_the_same_panel(
    hass: HomeAssistant,
    ha_mqtt: _FakeHaMqtt,
) -> None:
    observer = await _subscribed_observer(hass)
    observer.mark_activation_started()
    _fire_online(ha_mqtt)
    ha_mqtt.fire(METADATA, METADATA, f'{{"agent_version":"{VERSION}"}}')
    ha_mqtt.fire(DISCOVERY_FILTER, DISCOVERY, _discovery())
    ha_mqtt.fire(STATE_FILTER, "brilliant/office/load-2/state", '{"state":"OFF"}')

    evidence = await observer.async_wait(VERSION, timeout=0.05)

    assert evidence.state_topic == "brilliant/office/load-2/state"
    assert evidence.discovery_topic == DISCOVERY


async def test_partial_subscription_timeout_unsubscribes_every_completed_topic(
    hass: HomeAssistant,
    ha_mqtt: _FakeHaMqtt,
) -> None:
    ha_mqtt.missing_ack.add(METADATA)
    observer = PanelHealthObserver(hass, PANEL, subscription_timeout=0.01)

    with pytest.raises(PanelHealthError) as raised:
        await observer.async_subscribe()

    assert raised.value.code == "panel_health_subscription_timeout"
    assert _exception_graph(raised.value) == [raised.value]
    assert not ha_mqtt.callbacks
    assert not ha_mqtt.status_callbacks
    assert ("unsubscribe", AVAILABILITY) in ha_mqtt.events
    assert ("unsubscribe", METADATA) in ha_mqtt.events


async def test_subscription_failure_is_redacted_and_cleans_partial_state(
    hass: HomeAssistant,
    ha_mqtt: _FakeHaMqtt,
) -> None:
    ha_mqtt.subscribe_errors[STATE_FILTER] = RuntimeError("SECRET broker detail")
    observer = PanelHealthObserver(hass, PANEL, subscription_timeout=0.05)

    with pytest.raises(PanelHealthError) as raised:
        await observer.async_subscribe()

    assert raised.value.code == "panel_health_subscription_failed"
    exception_graph = _exception_graph(raised.value)
    assert exception_graph == [raised.value]
    assert all("SECRET" not in str(error) for error in exception_graph)
    assert not ha_mqtt.callbacks
    assert not ha_mqtt.status_callbacks


async def test_wait_cancellation_closes_every_subscription(
    hass: HomeAssistant,
    ha_mqtt: _FakeHaMqtt,
) -> None:
    observer = await _subscribed_observer(hass)
    observer.mark_activation_started()
    wait = asyncio.create_task(observer.async_wait(VERSION, timeout=60))
    await asyncio.sleep(0)

    wait.cancel()
    with pytest.raises(asyncio.CancelledError):
        await wait

    assert not ha_mqtt.callbacks
    assert len([event for event in ha_mqtt.events if event[0] == "unsubscribe"]) == 4


async def test_explicit_close_attempts_all_unsubscribes_and_reports_redacted_error(
    hass: HomeAssistant,
    ha_mqtt: _FakeHaMqtt,
) -> None:
    observer = await _subscribed_observer(hass)
    ha_mqtt.unsubscribe_errors.update({AVAILABILITY, STATE_FILTER})

    with pytest.raises(PanelHealthError) as raised:
        await observer.async_close()

    assert raised.value.code == "panel_health_cleanup_failed"
    exception_graph = _exception_graph(raised.value)
    assert exception_graph == [raised.value]
    assert all("SECRET" not in str(error) for error in exception_graph)
    assert not ha_mqtt.callbacks
    assert len([event for event in ha_mqtt.events if event[0] == "unsubscribe"]) == 4
    await observer.async_close()


@pytest.mark.parametrize("method", ["mark", "wait"])
async def test_activation_and_wait_require_confirmed_subscriptions(
    hass: HomeAssistant,
    method: str,
) -> None:
    observer = PanelHealthObserver(hass, PANEL)

    with pytest.raises(PanelHealthError) as raised:
        if method == "mark":
            observer.mark_activation_started()
        else:
            await observer.async_wait(VERSION, timeout=0.01)

    assert raised.value.code == "panel_health_not_subscribed"
