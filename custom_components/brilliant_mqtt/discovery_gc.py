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

Deleting a retained topic requires the broker principal behind Home
Assistant's MQTT session to hold write access on ``homeassistant/#`` (in the
reference deployment, the full-access ``ha`` user — the panels' write-only
``brilliant`` user is the agent's principal, not this one). A restrictive ACL
can drop the empty retained publish without any error, so this module reports
deletions as published attempts, never as confirmed removals.
"""

from __future__ import annotations

import asyncio
import logging
import re

from homeassistant.components import mqtt
from homeassistant.components.mqtt import ReceiveMessage
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
    collection_window: float = _COLLECTION_WINDOW_SECONDS,
) -> int:
    """Publish deletions for stale retained configs, containing every failure.

    Returns how many deletion publishes were handed to the broker session,
    which is an attempt count, not a confirmed removal count.
    """
    try:
        slugs = frozenset(panel for panel in panels if _PANEL_SLUG.fullmatch(panel))
        if not slugs:
            return 0
        stale: set[str] = set()
        truncated = False

        @callback
        def collect(message: ReceiveMessage) -> None:
            nonlocal truncated
            if message.retain is not True or not is_stale_owned_discovery_topic(
                message.topic, slugs
            ):
                return
            if len(stale) >= _MAX_STALE_TOPICS and message.topic not in stale:
                truncated = True
                return
            stale.add(message.topic)

        unsubscribe = await mqtt.async_subscribe(hass, DISCOVERY_CONFIG_FILTER, collect, qos=1)
        try:
            # One bounded window per fleet setup: this is a restart-time
            # backstop, and a retained replay missed here is collected again
            # by the next setup pass, so late arrivals self-heal.
            await asyncio.sleep(collection_window)
        finally:
            unsubscribe()
        if truncated:
            _LOGGER.warning(
                "Retained discovery-config collection truncated at %d topics; "
                "each pass deletes what it collected, so the next setup pass "
                "continues the cleanup",
                _MAX_STALE_TOPICS,
            )
        failed = 0
        first_failure: tuple[str, Exception] | None = None
        for topic in sorted(stale):
            try:
                await mqtt.async_publish(hass, topic, "", qos=1, retain=True)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                failed += 1
                if first_failure is None:
                    first_failure = (topic, error)
                _LOGGER.debug("Deletion publish failed for %s", topic)
        if first_failure is not None:
            _LOGGER.warning(
                "Published %d of %d retained discovery-config deletions; %d failed "
                "(first: %s: %s: %s)",
                len(stale) - failed,
                len(stale),
                failed,
                first_failure[0],
                type(first_failure[1]).__name__,
                first_failure[1],
            )
        elif stale:
            _LOGGER.info(
                "Published %d retained discovery-config deletions",
                len(stale),
            )
        return len(stale) - failed
    except asyncio.CancelledError:
        raise
    except Exception as error:
        _LOGGER.warning(
            "Retained discovery-config cleanup failed (%s: %s)",
            type(error).__name__,
            error,
        )
        return 0
