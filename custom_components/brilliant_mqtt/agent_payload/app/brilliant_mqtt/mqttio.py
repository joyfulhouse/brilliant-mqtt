"""Real MQTT adapter (aiomqtt → MqttClient Protocol).

Wraps ``aiomqtt.Client`` (v2 API: async-context-manager client,
``client.messages`` async iterator, ``aiomqtt.Will`` for the LWT). This is the
only module importing aiomqtt; it is validated against the real broker in the
pilot, not unit-tested with mocked internals.

Reconnect/backoff is intentionally NOT handled here — the runner
(``__main__.run``) owns retries by reconstructing the adapter.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

import aiomqtt

from brilliant_mqtt.config import Settings
from brilliant_mqtt.discovery import availability_topic

logger = logging.getLogger(__name__)


class AioMqttAdapter:
    """Concrete :class:`~brilliant_mqtt.protocols.MqttClient` over aiomqtt.

    Construction builds the client (with the LWT) but performs no I/O.
    :meth:`connect` opens the connection and starts the receive loop;
    :meth:`disconnect` cleans up. The runner uses this concrete class so the
    connect/disconnect lifecycle (beyond the Protocol) is available.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        identifier: str | None = None,
        publish_availability: bool = True,
    ) -> None:
        self._settings = settings
        # Multiple consumers (panel bridge + mesh publisher) each register a
        # command callback on this one shared connection — fan out to all.
        self._command_cbs: list[Callable[[str, str], Awaitable[None]]] = []
        self._message_cbs: list[Callable[[str, str, bool], Awaitable[None]]] = []
        self._reader_task: asyncio.Task[None] | None = None
        self._avail_topic = availability_topic(settings.panel)
        # A distinct broker ClientID is REQUIRED for any second connection on the
        # same panel: two clients sharing an id force the broker to disconnect the
        # incumbent (MQTT-3.1.4-2), thrashing the connection. Availability
        # ownership belongs to the main bridge only — a secondary consumer (e.g.
        # the HA mirror using this purely for leader election) must not publish or
        # will the panel's availability topic, or it would flip the panel offline
        # in HA while the bridge is healthy.
        self._identifier = identifier or f"brilliant-mqtt-{settings.panel}"
        self._publish_availability = publish_availability

        # Last-Will-and-Testament: the broker publishes this retained "offline"
        # if we drop without a clean disconnect, so HA marks the panel offline.
        will = (
            aiomqtt.Will(topic=self._avail_topic, payload="offline", qos=0, retain=True)
            if publish_availability
            else None
        )
        self._client = aiomqtt.Client(
            hostname=settings.mqtt_host,
            port=settings.mqtt_port,
            username=settings.mqtt_username,
            password=settings.mqtt_password,
            identifier=self._identifier,
            will=will,
        )

    async def connect(self) -> None:
        """Open the broker connection and start the message reader task."""
        await self._client.__aenter__()
        self._reader_task = asyncio.create_task(self._read_loop())
        logger.info(
            "connected to MQTT broker %s:%s",
            self._settings.mqtt_host,
            self._settings.mqtt_port,
        )

    async def _read_loop(self) -> None:
        """Dispatch inbound messages to every registered command callback.

        Guarded so a single malformed message (bad UTF-8) or one failing
        callback cannot kill the loop OR starve the other callbacks — one bad
        command must not silence all future commands, and one consumer's bug
        must not break the others sharing this connection.
        """
        async for message in self._client.messages:
            command_cbs = list(self._command_cbs)
            message_cbs = list(self._message_cbs)
            if not command_cbs and not message_cbs:
                # No consumer registered yet — drop (reconcile re-subscribes).
                continue
            try:
                topic = str(message.topic)
                payload = _decode_payload(message.payload)
            except Exception:
                # Broad by design: keep the reader alive across any single
                # message's decode failure.
                logger.exception("failed decoding MQTT message; continuing")
                continue
            logger.debug("mqtt message on %s (%d bytes)", topic, len(payload))
            for command_cb in command_cbs:
                try:
                    await command_cb(topic, payload)
                except Exception:
                    # Broad by design — see the docstring.
                    logger.exception("command callback failed; continuing")
            for message_cb in message_cbs:
                try:
                    await message_cb(topic, payload, bool(message.retain))
                except Exception:
                    logger.exception("message callback failed; continuing")

    async def disconnect(self) -> None:
        """Best-effort clean shutdown: publish a clean offline LWT, then close.

        Publishing "offline" retained here (rather than relying on the broker's
        LWT) gives a deterministic offline marker on an orderly stop (plan M7
        Step 3 — "clean LWT on exit"). Skipped when this adapter does not own the
        panel's availability topic (a secondary election-only consumer).
        """
        if self._publish_availability:
            try:
                await self._client.publish(self._avail_topic, payload="offline", retain=True)
            except aiomqtt.MqttError as exc:
                # Ordinary when the link is already down (every runner reconnect
                # cycle hits this): one quiet line, no traceback — the broker-side
                # LWT publishes the retained "offline" for us. MqttCodeError is a
                # subclass of MqttError, so this catch covers both.
                logger.warning("clean offline publish failed (%s); broker-side LWT covers it", exc)
            except Exception:
                # Anything non-MQTT here is genuinely unexpected — keep the
                # traceback, and keep disconnect() best-effort (never raise).
                logger.exception("failed publishing clean offline availability")

        if self._reader_task is not None:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("reader task raised during cancellation")
            self._reader_task = None

        try:
            await self._client.__aexit__(None, None, None)
        except Exception:
            logger.exception("failed closing MQTT client")

    # -- MqttClient Protocol -------------------------------------------------

    async def publish(self, topic: str, payload: str, retain: bool = False) -> None:
        await self._client.publish(topic, payload=payload, retain=retain)

    def on_command(self, cb: Callable[[str, str], Awaitable[None]]) -> None:
        self._command_cbs.append(cb)

    def on_message(self, cb: Callable[[str, str, bool], Awaitable[None]]) -> None:
        self._message_cbs.append(cb)

    async def subscribe(self, topic: str) -> None:
        await self._client.subscribe(topic)

    async def unsubscribe(self, topic: str) -> None:
        # Like subscribe/publish, delegates straight to aiomqtt — which raises
        # its own MqttCodeError when used before connect().
        await self._client.unsubscribe(topic)


def _decode_payload(payload: object) -> str:
    """Decode an aiomqtt payload to text (UTF-8 for bytes; str passthrough)."""
    if isinstance(payload, (bytes, bytearray)):
        return bytes(payload).decode("utf-8")
    return str(payload)
