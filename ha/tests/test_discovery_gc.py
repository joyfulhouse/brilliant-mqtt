"""Retained discovery-config GC deletes only stale pre-ledger managed topics."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from time import monotonic
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.components import mqtt
from homeassistant.components.mqtt import ReceiveMessage
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from paho.mqtt.client import topic_matches_sub

from custom_components.brilliant_mqtt import discovery_gc
from custom_components.brilliant_mqtt.discovery_gc import (
    DISCOVERY_CONFIG_FILTER,
    async_purge_stale_discovery_configs,
    is_stale_owned_discovery_topic,
)
from custom_components.brilliant_mqtt.fleet_manager import FleetManager
from custom_components.brilliant_mqtt.manager import PanelManager

PANELS = frozenset({"office", "kitchen"})
STALE_OFFICE = "homeassistant/light/brilliant_office_HA Backyard Lamp 1/config"
STALE_KITCHEN = "homeassistant/sensor/brilliant_kitchen_bad id/config"
LEGAL_OFFICE = "homeassistant/light/brilliant_office_HA_Backyard_Lamp_1/config"
STALE_UNMANAGED = "homeassistant/light/brilliant_porch_HA Lamp 2/config"
FOREIGN = "homeassistant/light/other vendor thing/config"

# Real (broker-like) retained replays arrive asynchronously after SUBACK, so
# collection only works while the GC actually sleeps its bounded window.
SMALL_WINDOW = 0.05


@dataclass(slots=True)
class _FakeRetainedBroker:
    """Retained-topic store standing in for HA's mqtt subscribe/publish seam.

    Retained and live replays are delivered asynchronously (``call_soon``) and
    stop at unsubscribe, so a GC that skips its collection window sees nothing.
    """

    retained: dict[str, str] = field(default_factory=dict)
    live_topics: list[str] = field(default_factory=list)
    publish_errors: set[str] = field(default_factory=set)
    published: list[tuple[str, str, int, bool]] = field(default_factory=list)
    subscribed: list[tuple[str, int]] = field(default_factory=list)
    unsubscribed: list[str] = field(default_factory=list)

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(mqtt, "async_subscribe", self.async_subscribe)
        monkeypatch.setattr(mqtt, "async_publish", self.async_publish)

    async def async_subscribe(
        self,
        hass: HomeAssistant,
        topic: str,
        callback: Callable[[ReceiveMessage], Any],
        qos: int = 0,
        encoding: str | None = "utf-8",
    ) -> Callable[[], None]:
        del hass, encoding
        self.subscribed.append((topic, qos))
        active = True

        def deliver(message: ReceiveMessage) -> None:
            if active:
                callback(message)

        loop = asyncio.get_running_loop()
        for retained_topic in sorted(self.retained):
            if topic_matches_sub(topic, retained_topic):
                loop.call_soon(deliver, self._message(topic, retained_topic, retain=True))
        for live_topic in self.live_topics:
            if topic_matches_sub(topic, live_topic):
                loop.call_soon(deliver, self._message(topic, live_topic, retain=False))

        def unsubscribe() -> None:
            nonlocal active
            active = False
            self.unsubscribed.append(topic)

        return unsubscribe

    async def async_publish(
        self,
        hass: HomeAssistant,
        topic: str,
        payload: str,
        qos: int = 0,
        retain: bool = False,
    ) -> None:
        del hass
        self.published.append((topic, payload, qos, retain))
        if topic in self.publish_errors:
            raise RuntimeError("publish rejected")
        if not retain:
            return
        if payload:
            self.retained[topic] = payload
        else:
            self.retained.pop(topic, None)

    def _message(self, subscribed_topic: str, topic: str, *, retain: bool) -> ReceiveMessage:
        return ReceiveMessage(
            topic=topic,
            payload=self.retained.get(topic, "{}"),
            qos=1,
            retain=retain,
            subscribed_topic=subscribed_topic,
            timestamp=monotonic(),
        )


@pytest.fixture
def broker(monkeypatch: pytest.MonkeyPatch) -> _FakeRetainedBroker:
    seam = _FakeRetainedBroker(
        retained={
            STALE_OFFICE: "{}",
            STALE_KITCHEN: "{}",
            LEGAL_OFFICE: "{}",
            STALE_UNMANAGED: "{}",
            FOREIGN: "{}",
        }
    )
    seam.install(monkeypatch)
    return seam


@pytest.mark.parametrize(
    ("topic", "stale"),
    (
        (STALE_OFFICE, True),
        (STALE_KITCHEN, True),
        ("homeassistant/light/brilliant_office_ /config", True),
        (LEGAL_OFFICE, False),
        (STALE_UNMANAGED, False),
        (FOREIGN, False),
        ("homeassistant/light/brilliant_officers lamp/config", False),
        ("homeassistant/light/node/brilliant_office_HA Lamp/config", False),
        ("homeassistant2/light/brilliant_office_HA Lamp/config", False),
        ("homeassistant/light/brilliant_office_HA Lamp/state", False),
        ("homeassistant/bad component/brilliant_office_HA Lamp/config", False),
        ("homeassistant/light/brilliant_office_a+b/config", False),
        ("homeassistant/light/brilliant_office_a#b/config", False),
        ("homeassistant/light/brilliant_office_a\x00b/config", False),
    ),
)
def test_matcher_owns_only_illegal_managed_configs(topic: str, stale: bool) -> None:
    """Ownership requires a managed prefix AND an id the sanitizer cannot emit."""
    assert is_stale_owned_discovery_topic(topic, PANELS) is stale


async def test_deletes_exactly_the_illegal_managed_retained_configs(
    hass: HomeAssistant,
    broker: _FakeRetainedBroker,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Only retained managed-panel configs with illegal object ids are deleted."""
    broker.live_topics.append("homeassistant/light/brilliant_office_live bad/config")

    deleted = await async_purge_stale_discovery_configs(
        hass, PANELS, collection_window=SMALL_WINDOW
    )

    assert deleted == 2
    assert broker.subscribed == [(DISCOVERY_CONFIG_FILTER, 1)]
    assert broker.unsubscribed == [DISCOVERY_CONFIG_FILTER]
    assert broker.published == [
        (STALE_OFFICE, "", 1, True),
        (STALE_KITCHEN, "", 1, True),
    ]
    assert set(broker.retained) == {LEGAL_OFFICE, STALE_UNMANAGED, FOREIGN}
    assert "Published 2 retained discovery-config deletions" in caplog.text


async def test_second_run_deletes_nothing_new(
    hass: HomeAssistant,
    broker: _FakeRetainedBroker,
) -> None:
    """After one pass the stale topics are gone, so a rerun is a no-op."""
    first = await async_purge_stale_discovery_configs(hass, PANELS, collection_window=SMALL_WINDOW)
    second = await async_purge_stale_discovery_configs(hass, PANELS, collection_window=SMALL_WINDOW)

    assert first == 2
    assert second == 0
    assert len(broker.published) == 2


async def test_cap_truncation_warns_and_still_makes_progress(
    hass: HomeAssistant,
    broker: _FakeRetainedBroker,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Hitting the cap deletes what was collected and never claims completion."""
    monkeypatch.setattr(discovery_gc, "_MAX_STALE_TOPICS", 1)

    deleted = await async_purge_stale_discovery_configs(
        hass, PANELS, collection_window=SMALL_WINDOW
    )

    assert deleted == 1
    assert broker.published == [(STALE_OFFICE, "", 1, True)]
    assert STALE_KITCHEN in broker.retained
    assert "truncated at 1 topics" in caplog.text


async def test_publish_failure_is_contained_per_topic(
    hass: HomeAssistant,
    broker: _FakeRetainedBroker,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """One rejected deletion never aborts the rest of the batch."""
    broker.publish_errors.add(STALE_OFFICE)

    deleted = await async_purge_stale_discovery_configs(
        hass, PANELS, collection_window=SMALL_WINDOW
    )

    assert deleted == 1
    assert [entry[0] for entry in broker.published] == [STALE_OFFICE, STALE_KITCHEN]
    assert STALE_OFFICE in broker.retained
    assert STALE_KITCHEN not in broker.retained
    assert "Published 1 of 2 retained discovery-config deletions; 1 failed" in caplog.text


async def test_invalid_panel_slugs_are_ignored_fail_closed(
    hass: HomeAssistant,
    broker: _FakeRetainedBroker,
) -> None:
    """A slug outside the panel-slug grammar can never anchor a deletion."""
    broker.retained["homeassistant/light/brilliant_Bad Slug_x y/config"] = "{}"

    deleted = await async_purge_stale_discovery_configs(
        hass, frozenset({"Bad Slug", ""}), collection_window=SMALL_WINDOW
    )

    assert deleted == 0
    assert broker.published == []
    assert broker.subscribed == []


async def test_mqtt_failure_is_contained(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A broken MQTT seam is attempted, swallowed, and publishes nothing."""
    attempts: list[str] = []

    async def failing_subscribe(*args: Any, **kwargs: Any) -> Callable[[], None]:
        del kwargs
        attempts.append(args[1])
        raise HomeAssistantError("mqtt_not_setup_cannot_subscribe")

    publish = AsyncMock()
    monkeypatch.setattr(mqtt, "async_subscribe", failing_subscribe)
    monkeypatch.setattr(mqtt, "async_publish", publish)

    deleted = await async_purge_stale_discovery_configs(hass, PANELS, collection_window=0)

    assert deleted == 0
    assert attempts == [DISCOVERY_CONFIG_FILTER]
    publish.assert_not_awaited()
    assert "mqtt_not_setup_cannot_subscribe" in caplog.text


def _patched_fleet_status(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "custom_components.brilliant_mqtt.fleet_manager.mqtt.is_connected",
        lambda hass: True,
    )
    monkeypatch.setattr(
        "custom_components.brilliant_mqtt.fleet_manager.mqtt.async_subscribe_connection_status",
        lambda hass, callback: lambda: None,
    )


async def _noop_manager(manager: PanelManager) -> None:
    del manager


async def test_fleet_setup_runs_gc_once_with_managed_panels(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Setup schedules one GC pass scoped to exactly the managed panel slugs."""
    from tests.test_fleet_manager import _fleet_entry, _panel

    _patched_fleet_status(monkeypatch)
    entry = _fleet_entry(
        _panel("office", "SHA256:office", subentry_id="panel-office"),
        _panel("kitchen", "SHA256:kitchen", subentry_id="panel-kitchen"),
    )
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)
    purge = AsyncMock(return_value=0)

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.async_purge_stale_discovery_configs",
            purge,
        ),
        patch.object(PanelManager, "async_setup", _noop_manager),
        patch.object(PanelManager, "async_shutdown", _noop_manager),
    ):
        await fleet.async_setup()
        await hass.async_block_till_done(wait_background_tasks=True)
        await fleet.async_shutdown()

    purge.assert_awaited_once_with(hass, PANELS)


async def test_fleet_setup_survives_gc_mqtt_failure(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The real GC hits an unconfigured MQTT stack and setup still completes."""
    from tests.test_fleet_manager import _fleet_entry, _panel

    _patched_fleet_status(monkeypatch)
    attempts: list[str] = []
    real_subscribe = mqtt.async_subscribe

    async def tracking_subscribe(*args: Any, **kwargs: Any) -> Callable[[], None]:
        attempts.append("subscribe")
        return await real_subscribe(*args, **kwargs)

    monkeypatch.setattr(mqtt, "async_subscribe", tracking_subscribe)
    entry = _fleet_entry(_panel("office", "SHA256:office", subentry_id="panel-office"))
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)

    with (
        patch.object(PanelManager, "async_setup", _noop_manager),
        patch.object(PanelManager, "async_shutdown", _noop_manager),
    ):
        await fleet.async_setup()
        await hass.async_block_till_done(wait_background_tasks=True)
        assert set(fleet.panels) == {"panel-office"}
        await fleet.async_shutdown()

    assert attempts == ["subscribe"]
