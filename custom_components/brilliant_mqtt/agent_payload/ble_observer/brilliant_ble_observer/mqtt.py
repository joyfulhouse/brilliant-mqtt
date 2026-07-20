"""Bounded MQTT publication for normalized BLE advertisements."""

from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict, deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Protocol

import aiomqtt

from .config import Settings
from .model import AdvertisementEnvelope, matches_allowlist

logger = logging.getLogger(__name__)

DEFAULT_QUEUE_SIZE = 256
DEFAULT_RATE_STATE_SIZE = 256
DEFAULT_INITIAL_BACKOFF = 1.0
DEFAULT_MAX_BACKOFF = 30.0


def advertisement_topic(panel: str) -> str:
    """Return the non-retained observation topic for one physical panel."""
    return f"brilliant/ble/v1/{panel}/advertisement"


def health_topic(panel: str) -> str:
    """Return the retained observer-health topic for one physical panel."""
    return f"brilliant/ble/v1/{panel}/status"


@dataclass(frozen=True)
class MqttConnectionConfig:
    """Connection material passed to a transport without exposing its secret."""

    hostname: str
    port: int
    username: str
    password: str = field(repr=False)
    identifier: str
    will_topic: str
    will_payload: str
    will_qos: int
    will_retain: bool

    @classmethod
    def from_settings(cls, settings: Settings) -> MqttConnectionConfig:
        """Build the dedicated BLE observer connection and retained LWT."""
        return cls(
            hostname=settings.mqtt_host,
            port=settings.mqtt_port,
            username=settings.mqtt_username,
            password=settings.mqtt_password,
            identifier=f"brilliant-ble-{settings.panel}",
            will_topic=health_topic(settings.panel),
            will_payload="offline",
            will_qos=0,
            will_retain=True,
        )


@dataclass(frozen=True)
class PublisherStats:
    """Payload-free counters for diagnostics."""

    queued: int
    published: int
    dropped_oldest: int
    rate_limited: int
    publish_failures: int
    reconnects: int


class MqttTransport(Protocol):
    """Small outbound-only transport used by the publisher."""

    async def connect(self) -> None:
        """Open the broker connection."""

    async def publish(self, topic: str, payload: str, *, qos: int, retain: bool) -> None:
        """Publish one MQTT message."""

    async def disconnect(self) -> None:
        """Close the broker connection."""


class AioMqttTransport:
    """Concrete outbound-only aiomqtt transport."""

    def __init__(self, config: MqttConnectionConfig) -> None:
        will = aiomqtt.Will(
            topic=config.will_topic,
            payload=config.will_payload,
            qos=config.will_qos,
            retain=config.will_retain,
        )
        self._client = aiomqtt.Client(
            hostname=config.hostname,
            port=config.port,
            username=config.username,
            password=config.password,
            identifier=config.identifier,
            will=will,
        )

    async def connect(self) -> None:
        await self._client.__aenter__()

    async def publish(self, topic: str, payload: str, *, qos: int, retain: bool) -> None:
        await self._client.publish(topic, payload=payload, qos=qos, retain=retain)

    async def disconnect(self) -> None:
        await self._client.__aexit__(None, None, None)


TransportFactory = Callable[[MqttConnectionConfig], MqttTransport]
Sleep = Callable[[float], Awaitable[None]]
Monotonic = Callable[[], float]


class AdvertisementPublisher:
    """Rate-limit, queue, and publish observations without retaining payloads."""

    def __init__(
        self,
        settings: Settings,
        *,
        transport_factory: TransportFactory = AioMqttTransport,
        queue_size: int = DEFAULT_QUEUE_SIZE,
        rate_state_size: int = DEFAULT_RATE_STATE_SIZE,
        monotonic: Monotonic = time.monotonic,
        sleep: Sleep = asyncio.sleep,
        initial_backoff: float = DEFAULT_INITIAL_BACKOFF,
        max_backoff: float = DEFAULT_MAX_BACKOFF,
    ) -> None:
        if queue_size < 1:
            raise ValueError("queue_size must be at least 1")
        if rate_state_size < 1:
            raise ValueError("rate_state_size must be at least 1")
        if initial_backoff <= 0 or max_backoff < initial_backoff:
            raise ValueError("backoff bounds are invalid")
        self._settings = settings
        self._transport_factory = transport_factory
        self._queue_size = queue_size
        self._rate_state_size = rate_state_size
        self._monotonic = monotonic
        self._sleep = sleep
        self._initial_backoff = initial_backoff
        self._max_backoff = max_backoff
        self._pending: deque[AdvertisementEnvelope] = deque()
        self._pending_event = asyncio.Event()
        self._last_emitted: OrderedDict[tuple[str, tuple[object, ...]], float] = OrderedDict()
        self._published = 0
        self._dropped_oldest = 0
        self._rate_limited = 0
        self._publish_failures = 0
        self._reconnects = 0

    @property
    def stats(self) -> PublisherStats:
        """Return a payload-free snapshot of bounded-publisher counters."""
        return PublisherStats(
            queued=len(self._pending),
            published=self._published,
            dropped_oldest=self._dropped_oldest,
            rate_limited=self._rate_limited,
            publish_failures=self._publish_failures,
            reconnects=self._reconnects,
        )

    def enqueue(self, advertisement: AdvertisementEnvelope) -> bool:
        """Queue one allowed observation, or reject it under its source rate limit."""
        key = (advertisement.adapter_address, self._identity_key(advertisement))
        now = self._monotonic()
        last_emitted = self._last_emitted.get(key)
        minimum_interval = 1.0 / self._settings.max_events_per_second
        if last_emitted is not None and now - last_emitted < minimum_interval:
            self._rate_limited += 1
            return False

        self._last_emitted[key] = now
        self._last_emitted.move_to_end(key)
        while len(self._last_emitted) > self._rate_state_size:
            self._last_emitted.popitem(last=False)

        if len(self._pending) == self._queue_size:
            self._pending.popleft()
            self._dropped_oldest += 1
        self._pending.append(advertisement)
        self._pending_event.set()
        return True

    def _identity_key(self, advertisement: AdvertisementEnvelope) -> tuple[object, ...]:
        for entry in self._settings.allowlist:
            if entry.address is not None:
                continue
            if not matches_allowlist(
                address=advertisement.address,
                manufacturer_data=advertisement.manufacturer_data,
                allowlist=(entry,),
            ):
                continue
            return (
                "ibeacon",
                entry.ibeacon_uuid,
                entry.ibeacon_major,
                entry.ibeacon_minor,
            )
        for entry in self._settings.allowlist:
            if entry.address == advertisement.address:
                return ("address", entry.address)
        return ("address", advertisement.address)

    async def run(self) -> None:
        """Publish forever, rebuilding the connection with bounded backoff."""
        config = MqttConnectionConfig.from_settings(self._settings)
        backoff = self._initial_backoff
        while True:
            transport = self._transport_factory(config)
            connected = False
            try:
                await transport.connect()
                connected = True
                await transport.publish(config.will_topic, "online", qos=0, retain=True)
                while True:
                    advertisement = await self._next_pending()
                    try:
                        await transport.publish(
                            advertisement_topic(self._settings.panel),
                            advertisement.to_json(),
                            qos=0,
                            retain=False,
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        self._publish_failures += 1
                        raise
                    self._published += 1
                    backoff = self._initial_backoff
            except asyncio.CancelledError:
                await self._close_transport(transport, connected=connected)
                raise
            except Exception as error:
                self._reconnects += 1
                logger.warning(
                    "MQTT observer session failed (%s); reconnect=%d queued=%d",
                    type(error).__name__,
                    self._reconnects,
                    len(self._pending),
                )
                await self._close_transport(transport, connected=connected)
                await self._sleep(backoff)
                backoff = min(self._max_backoff, backoff * 2)

    async def _next_pending(self) -> AdvertisementEnvelope:
        while not self._pending:
            self._pending_event.clear()
            await self._pending_event.wait()
        return self._pending.popleft()

    async def _close_transport(
        self,
        transport: MqttTransport,
        *,
        connected: bool,
    ) -> None:
        if connected:
            try:
                await transport.publish(
                    health_topic(self._settings.panel), "offline", qos=0, retain=True
                )
            except Exception:
                logger.warning("MQTT observer clean offline publication failed")
        try:
            await transport.disconnect()
        except Exception:
            logger.warning("MQTT observer transport cleanup failed")
