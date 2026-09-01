"""Retained MQTT discovery-config GC for stale pre-ledger panel topics.

July-2026 pilot-era agents (pre-0.6.0 retained ledger, pre-0.7.0 sanitizer)
published retained discovery configs whose object ids were display labels
("brilliant_office_HA Backyard Lamp 1"). Home Assistant rejects such ids, so
no entity ever existed under them, yet the retained configs replay on every
restart and nothing else deletes them: the agent ledger prunes only topics it
recorded. This module is the designated backstop: one bounded pass per fleet
setup deletes every retained config that is prefix-owned by a managed panel
(the agent's ledger ownership rule, restated) AND has an object id the
sanitizer can never emit, so an illegal id can never be current.
"""

from __future__ import annotations

import asyncio
import logging
import re

from homeassistant.components import mqtt
from homeassistant.components.mqtt.models import ReceiveMessage
from homeassistant.core import HomeAssistant, callback

_LOGGER = logging.getLogger(__name__)

DISCOVERY_CONFIG_FILTER = "homeassistant/+/+/config"

_COLLECTION_WINDOW_SECONDS = 5.0
_MAX_STALE_TOPICS = 1024

# The agent sanitizer maps every character outside this class to "_", so a
# current discovery object id is always fully legal.
_LEGAL_SEGMENT = re.compile(r"^[a-zA-Z0-9_-]+$")
_PANEL_SLUG = re.compile(r"^[a-z0-9][a-z0-9_-]*$")


def is_stale_owned_discovery_topic(topic: str, panels: frozenset[str]) -> bool:
    """True only for a managed-panel config topic with an illegal object id."""
    if {"+", "#", "\x00"}.intersection(topic):
        return False
    parts = topic.split("/")
    if (
        len(parts) != 4
        or parts[0] != "homeassistant"
        or _LEGAL_SEGMENT.fullmatch(parts[1]) is None
        or parts[3] != "config"
    ):
        return False
    object_id = parts[2]
    if _LEGAL_SEGMENT.fullmatch(object_id) is not None:
        return False
    return any(object_id.startswith(f"brilliant_{panel}_") for panel in panels)


async def async_purge_stale_discovery_configs(
    hass: HomeAssistant,
    panels: frozenset[str],
    *,
    collection_window: float | None = None,
) -> int:
    """Delete stale retained configs, containing every failure; return the count."""
    try:
        return await _async_purge(hass, panels, collection_window)
    except asyncio.CancelledError:
        raise
    except Exception as error:
        _LOGGER.warning(
            "Retained discovery-config cleanup failed (%s)",
            type(error).__name__,
        )
        return 0


async def _async_purge(
    hass: HomeAssistant,
    panels: frozenset[str],
    collection_window: float | None,
) -> int:
    slugs = frozenset(panel for panel in panels if _PANEL_SLUG.fullmatch(panel))
    if not slugs:
        return 0
    stale: set[str] = set()

    @callback
    def collect(message: ReceiveMessage) -> None:
        if (
            message.subscribed_topic == DISCOVERY_CONFIG_FILTER
            and message.retain is True
            and len(stale) < _MAX_STALE_TOPICS
            and is_stale_owned_discovery_topic(message.topic, slugs)
        ):
            stale.add(message.topic)

    unsubscribe = await mqtt.async_subscribe(hass, DISCOVERY_CONFIG_FILTER, collect)
    try:
        await asyncio.sleep(
            _COLLECTION_WINDOW_SECONDS if collection_window is None else collection_window
        )
    finally:
        unsubscribe()
    for topic in sorted(stale):
        await mqtt.async_publish(hass, topic, "", qos=1, retain=True)
    if stale:
        _LOGGER.info(
            "Deleted %d stale pre-ledger retained discovery configs",
            len(stale),
        )
    return len(stale)
