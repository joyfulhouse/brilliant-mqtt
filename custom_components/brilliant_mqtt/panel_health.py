"""Fresh, coherent MQTT health evidence for one activated fleet panel."""

from __future__ import annotations

import asyncio
import json
import math
import re
from collections.abc import Callable
from dataclasses import dataclass
from time import monotonic
from typing import Any

from homeassistant.components import mqtt
from homeassistant.components.mqtt.models import ReceiveMessage
from homeassistant.core import CALLBACK_TYPE, HomeAssistant, callback

from .const import AVAILABILITY_OFFLINE, AVAILABILITY_ONLINE, MESH_PANEL
from .const import availability_topic as panel_availability_topic
from .const import meta_topic as panel_meta_topic

MAX_HEALTH_PAYLOAD_BYTES = 16 * 1024

_DEFAULT_SUBSCRIPTION_TIMEOUT = 10.0
_MAX_JSON_DEPTH = 64
_MAX_JSON_NODES = 4096
_PANEL_SLUG = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
_AGENT_VERSION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,127}$")

_ERROR_RETRYABLE = {
    "panel_health_invalid_request": False,
    "panel_health_not_subscribed": False,
    "panel_health_already_subscribed": False,
    "panel_health_activation_not_started": False,
    "panel_health_activation_already_started": False,
    "panel_health_already_waiting": False,
    "panel_health_subscription_failed": True,
    "panel_health_subscription_timeout": True,
    "panel_health_payload_invalid": False,
    "panel_health_version_mismatch": False,
    "panel_health_timeout": True,
    "panel_health_closed": True,
    "panel_health_cleanup_failed": True,
}


@dataclass(frozen=True, slots=True)
class PanelHealthError(Exception):
    """Allowlisted health failure which never retains MQTT or broker details."""

    code: str

    def __post_init__(self) -> None:
        if self.code not in _ERROR_RETRYABLE:
            raise ValueError("invalid_panel_health_error")
        Exception.__init__(self, self.code)
        object.__setattr__(self, "__suppress_context__", True)

    @property
    def retryable(self) -> bool:
        """Whether another attempt can reasonably succeed unchanged."""
        return _ERROR_RETRYABLE[self.code]

    def redacted_dict(self) -> dict[str, str | bool]:
        """Return the complete safe diagnostic representation."""
        return {"code": self.code, "retryable": self.retryable}


@dataclass(frozen=True, slots=True)
class PanelHealthEvidence:
    """Bounded proof that the activated panel completed a normal reconcile."""

    panel: str
    agent_version: str
    state_topic: str
    discovery_topic: str
    device_identifier: str

    def redacted_dict(self) -> dict[str, str]:
        """Return the non-payload evidence safe for diagnostics."""
        return {
            "panel": self.panel,
            "agent_version": self.agent_version,
            "state_topic": self.state_topic,
            "discovery_topic": self.discovery_topic,
            "device_identifier": self.device_identifier,
        }


class PanelHealthObserver:
    """Collect one post-activation MQTT session without accepting retained history.

    ``async_subscribe`` must complete before the provisioner mutates the active
    service. The provisioner then calls ``mark_activation_started`` immediately
    before activation. Only messages received after that boundary and after a
    fresh ``online`` publication belong to the candidate session.
    """

    __slots__ = (
        "_activated_at",
        "_closed",
        "_discovery_topics",
        "_error",
        "_expected_version",
        "_hass",
        "_meta_version",
        "_online_seen",
        "_panel",
        "_state_topics",
        "_subscribed",
        "_subscribing",
        "_subscription_timeout",
        "_unsubscribes",
        "_updated",
        "_waiting",
    )

    def __init__(
        self,
        hass: HomeAssistant,
        panel: str,
        *,
        subscription_timeout: float = _DEFAULT_SUBSCRIPTION_TIMEOUT,
    ) -> None:
        if (
            not isinstance(panel, str)
            or panel == MESH_PANEL
            or _PANEL_SLUG.fullmatch(panel) is None
            or not _valid_timeout(subscription_timeout)
        ):
            raise PanelHealthError("panel_health_invalid_request")
        self._hass = hass
        self._panel = panel
        self._subscription_timeout = float(subscription_timeout)
        self._unsubscribes: list[CALLBACK_TYPE] = []
        self._updated = asyncio.Event()
        self._subscribed = False
        self._subscribing = False
        self._closed = False
        self._waiting = False
        self._activated_at: float | None = None
        self._online_seen = False
        self._expected_version: str | None = None
        self._meta_version: str | None = None
        self._state_topics: set[str] = set()
        self._discovery_topics: set[str] = set()
        self._error: PanelHealthError | None = None

    @property
    def panel(self) -> str:
        """Return the immutable panel namespace."""
        return self._panel

    async def async_subscribe(self) -> None:
        """Install and broker-confirm all subscriptions before activation."""
        if self._subscribed or self._subscribing:
            raise PanelHealthError("panel_health_already_subscribed")
        if self._closed:
            raise PanelHealthError("panel_health_closed")

        subscriptions: tuple[tuple[str, Callable[[ReceiveMessage], None]], ...] = (
            (panel_availability_topic(self._panel), self._on_availability),
            (panel_meta_topic(self._panel), self._on_metadata),
            (self._state_filter, self._on_state),
            (self._discovery_filter, self._on_discovery),
        )
        self._subscribing = True
        failure_code: str | None = None
        try:
            for topic, message_callback in subscriptions:
                await self._async_subscribe_one(topic, message_callback)
        except asyncio.CancelledError:
            self._close_suppressing_errors()
            raise
        except TimeoutError:
            failure_code = "panel_health_subscription_timeout"
        except Exception:
            failure_code = "panel_health_subscription_failed"
        finally:
            self._subscribing = False
        if failure_code is not None:
            self._close_suppressing_errors()
            raise PanelHealthError(failure_code)
        self._subscribed = True

    @callback
    def mark_activation_started(self) -> None:
        """Set the one-shot boundary after subscriptions and before activation."""
        if not self._subscribed or self._closed:
            raise PanelHealthError("panel_health_not_subscribed")
        if self._activated_at is not None:
            raise PanelHealthError("panel_health_activation_already_started")
        self._activated_at = monotonic()
        self._clear_session()
        self._updated.set()

    async def async_wait(
        self,
        expected_version: str,
        timeout: float = 90.0,  # noqa: ASYNC109 - public interface names this bound
    ) -> PanelHealthEvidence:
        """Wait for a coherent fresh reconcile and always release subscriptions."""
        if not self._subscribed or self._closed:
            raise PanelHealthError("panel_health_not_subscribed")
        if self._activated_at is None:
            self._close_suppressing_errors()
            raise PanelHealthError("panel_health_activation_not_started")
        if self._waiting:
            raise PanelHealthError("panel_health_already_waiting")
        if not _valid_agent_version(expected_version) or not _valid_timeout(timeout):
            self._close_suppressing_errors()
            raise PanelHealthError("panel_health_invalid_request")

        self._waiting = True
        self._expected_version = expected_version
        if self._meta_version is not None and self._meta_version != expected_version:
            self._set_error("panel_health_version_mismatch")

        try:
            timed_out = False
            try:
                async with asyncio.timeout(float(timeout)):
                    evidence = await self._async_wait_for_evidence()
            except TimeoutError:
                timed_out = True
            except asyncio.CancelledError:
                self._close_suppressing_errors()
                raise
            except PanelHealthError:
                self._close_suppressing_errors()
                raise
            if timed_out:
                self._close_suppressing_errors()
                raise PanelHealthError("panel_health_timeout")
            await self.async_close()
            return evidence
        finally:
            self._waiting = False

    async def async_close(self) -> None:
        """Idempotently release every subscription, attempting all handles."""
        if self._close_suppressing_errors():
            raise PanelHealthError("panel_health_cleanup_failed")

    @property
    def _state_filter(self) -> str:
        return f"brilliant/{self._panel}/+/state"

    @property
    def _discovery_filter(self) -> str:
        return "homeassistant/+/+/config"

    @property
    def _device_identifier(self) -> str:
        return f"brilliant_panel_{self._panel}"

    async def _async_subscribe_one(
        self,
        topic: str,
        message_callback: Callable[[ReceiveMessage], None],
    ) -> None:
        subscribed = asyncio.Event()

        @callback
        def on_subscribe_done() -> None:
            subscribed.set()

        remove_status = mqtt.async_on_subscribe_done(
            self._hass,
            topic,
            1,
            on_subscribe_done,
        )
        try:
            unsubscribe = await mqtt.async_subscribe(
                self._hass,
                topic,
                message_callback,
                qos=1,
            )
            self._unsubscribes.append(unsubscribe)
            async with asyncio.timeout(self._subscription_timeout):
                await subscribed.wait()
        finally:
            remove_status()

    async def _async_wait_for_evidence(self) -> PanelHealthEvidence:
        while True:
            if self._error is not None:
                raise self._error
            if evidence := self._evidence():
                return evidence
            if self._closed:
                raise PanelHealthError("panel_health_closed")
            self._updated.clear()
            await self._updated.wait()

    def _evidence(self) -> PanelHealthEvidence | None:
        if not self._online_seen or self._meta_version is None:
            return None
        if self._expected_version is None or self._meta_version != self._expected_version:
            return None
        if not self._state_topics or not self._discovery_topics:
            return None
        return PanelHealthEvidence(
            panel=self._panel,
            agent_version=self._meta_version,
            state_topic=min(self._state_topics),
            discovery_topic=min(self._discovery_topics),
            device_identifier=self._device_identifier,
        )

    @callback
    def _on_availability(self, message: ReceiveMessage) -> None:
        expected_topic = panel_availability_topic(self._panel)
        if (
            message.topic != expected_topic
            or message.subscribed_topic != expected_topic
            or not self._is_fresh(message)
        ):
            return
        try:
            payload = _decode_payload(message.payload)
        except (TypeError, ValueError, UnicodeError):
            self._set_error("panel_health_payload_invalid")
            return
        if payload == AVAILABILITY_ONLINE:
            self._clear_session()
            self._online_seen = True
            self._updated.set()
        elif payload == AVAILABILITY_OFFLINE:
            self._clear_session()
            self._updated.set()
        else:
            self._set_error("panel_health_payload_invalid")

    @callback
    def _on_metadata(self, message: ReceiveMessage) -> None:
        expected_topic = panel_meta_topic(self._panel)
        if (
            message.topic != expected_topic
            or message.subscribed_topic != expected_topic
            or not self._eligible_after_online(message)
        ):
            return
        try:
            payload = _decode_json_object(message.payload)
            agent_version = payload["agent_version"]
            if not _valid_agent_version(agent_version):
                raise ValueError
        except (KeyError, TypeError, ValueError, UnicodeError):
            self._set_error("panel_health_payload_invalid")
            return

        assert isinstance(agent_version, str)
        self._meta_version = agent_version
        if self._expected_version is not None and agent_version != self._expected_version:
            self._set_error("panel_health_version_mismatch")
            return
        self._updated.set()

    @callback
    def _on_state(self, message: ReceiveMessage) -> None:
        if (
            message.subscribed_topic != self._state_filter
            or _state_topic_peripheral(message.topic, self._panel) is None
            or not self._eligible_after_online(message)
        ):
            return
        try:
            _decode_json_object(message.payload)
        except (TypeError, ValueError, UnicodeError):
            self._set_error("panel_health_payload_invalid")
            return
        self._state_topics.add(message.topic)
        self._updated.set()

    @callback
    def _on_discovery(self, message: ReceiveMessage) -> None:
        unique_id = _target_discovery_unique_id(message.topic, self._panel)
        if (
            message.subscribed_topic != self._discovery_filter
            or unique_id is None
            or not self._eligible_after_online(message)
        ):
            return
        try:
            payload = _decode_json_object(message.payload)
        except (TypeError, ValueError, UnicodeError):
            # Discovery is a broker-global wildcard. A malformed publication
            # cannot safely be attributed to this panel, so it is rejected as
            # evidence without failing an otherwise healthy activation.
            return

        device = payload.get("device")
        identifiers = device.get("identifiers") if isinstance(device, dict) else None
        if identifiers != [self._device_identifier]:
            # Slugs may share prefixes (for example ``office`` and
            # ``office_bath``), so the topic's unique-id prefix alone cannot
            # attribute this global discovery publication to the candidate.
            return

        state_topic = payload.get("state_topic")
        availability = payload.get("availability")
        if (
            payload.get("unique_id") != unique_id
            or not isinstance(state_topic, str)
            or _state_topic_peripheral(state_topic, self._panel) is None
            or not isinstance(availability, list)
            or not any(
                isinstance(item, dict)
                and item.get("topic") == panel_availability_topic(self._panel)
                for item in availability
            )
        ):
            self._set_error("panel_health_payload_invalid")
            return

        self._discovery_topics.add(message.topic)
        self._updated.set()

    def _is_fresh(self, message: ReceiveMessage) -> bool:
        activated_at = self._activated_at
        timestamp = message.timestamp
        return (
            not self._closed
            and activated_at is not None
            and message.retain is False
            and type(timestamp) is float
            and math.isfinite(timestamp)
            and timestamp >= activated_at
        )

    def _eligible_after_online(self, message: ReceiveMessage) -> bool:
        return self._online_seen and self._is_fresh(message)

    def _clear_session(self) -> None:
        self._online_seen = False
        self._meta_version = None
        self._state_topics.clear()
        self._discovery_topics.clear()
        self._error = None

    def _set_error(self, code: str) -> None:
        if self._error is None:
            self._error = PanelHealthError(code)
        self._updated.set()

    def _close_suppressing_errors(self) -> bool:
        self._closed = True
        self._subscribed = False
        self._updated.set()
        failed = False
        while self._unsubscribes:
            unsubscribe = self._unsubscribes.pop()
            try:
                unsubscribe()
            except Exception:
                failed = True
        return failed


def _valid_timeout(value: float) -> bool:
    if type(value) not in (int, float):
        return False
    try:
        return math.isfinite(value) and value > 0
    except OverflowError:
        return False


def _valid_agent_version(value: object) -> bool:
    return isinstance(value, str) and _AGENT_VERSION.fullmatch(value) is not None


def _decode_payload(payload: object) -> str:
    if isinstance(payload, bytes):
        if len(payload) > MAX_HEALTH_PAYLOAD_BYTES:
            raise ValueError
        return payload.decode("utf-8", errors="strict")
    if not isinstance(payload, str):
        raise TypeError
    if len(payload) > MAX_HEALTH_PAYLOAD_BYTES:
        raise ValueError
    encoded = payload.encode("utf-8", errors="strict")
    if len(encoded) > MAX_HEALTH_PAYLOAD_BYTES:
        raise ValueError
    return payload


def _decode_json_object(payload: object) -> dict[str, Any]:
    try:
        value = json.loads(
            _decode_payload(payload),
            object_pairs_hook=_object_without_duplicate_keys,
            parse_constant=_reject_json_constant,
        )
    except (RecursionError, TypeError, ValueError, UnicodeError):
        raise ValueError from None
    if not isinstance(value, dict):
        raise ValueError
    _validate_json_structure(value)
    return value


def _object_without_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError
        value[key] = item
    return value


def _reject_json_constant(value: str) -> None:
    del value
    raise ValueError


def _validate_json_structure(value: object) -> None:
    pending: list[tuple[object, int]] = [(value, 0)]
    nodes = 0
    while pending:
        item, depth = pending.pop()
        nodes += 1
        if depth > _MAX_JSON_DEPTH or nodes > _MAX_JSON_NODES:
            raise ValueError
        if isinstance(item, dict):
            for key, child in item.items():
                key.encode("utf-8", errors="strict")
                pending.append((child, depth + 1))
        elif isinstance(item, list):
            pending.extend((child, depth + 1) for child in item)
        elif isinstance(item, str):
            item.encode("utf-8", errors="strict")
        elif type(item) is float and not math.isfinite(item):
            raise ValueError
        elif item is not None and type(item) not in (bool, int, float):
            raise ValueError


def _state_topic_peripheral(topic: object, panel: str) -> str | None:
    if not isinstance(topic, str):
        return None
    parts = topic.split("/")
    if (
        len(parts) != 4
        or parts[0] != "brilliant"
        or parts[1] != panel
        or parts[3] != "state"
        or not _valid_peripheral_id(parts[2])
    ):
        return None
    return parts[2]


def _valid_peripheral_id(value: str) -> bool:
    return (
        1 <= len(value) <= 256
        and all(0x21 <= ord(character) <= 0x7E for character in value)
        and not {"/", "+", "#"}.intersection(value)
    )


def _target_discovery_unique_id(topic: object, panel: str) -> str | None:
    if not isinstance(topic, str):
        return None
    parts = topic.split("/")
    if (
        len(parts) != 4
        or parts[0] != "homeassistant"
        or not parts[1]
        or parts[3] != "config"
        or not parts[2].startswith(f"brilliant_{panel}_")
    ):
        return None
    return parts[2]
