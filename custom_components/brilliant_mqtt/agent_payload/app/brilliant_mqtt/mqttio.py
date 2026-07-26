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
import ssl
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import aiomqtt

from brilliant_mqtt.config import Settings
from brilliant_mqtt.discovery import availability_topic

logger = logging.getLogger(__name__)

_DISCONNECT_ERROR = "MQTT disconnect failed"


@dataclass(frozen=True, slots=True)
class MqttPayloadDecodeError:
    """Metadata-only signal for an inbound payload that is not valid UTF-8."""

    topic: str
    retained: bool


def build_tls_context(settings: Settings) -> ssl.SSLContext | None:
    """Build a strict server-authenticated TLS context when TLS is enabled."""
    if not settings.mqtt_tls_enabled:
        return None

    context = ssl.create_default_context(cafile=settings.mqtt_tls_ca_file)
    context.check_hostname = True
    context.verify_mode = ssl.CERT_REQUIRED
    return context


class AioMqttAdapter:
    """Concrete :class:`~brilliant_mqtt.protocols.MqttClient` over aiomqtt.

    Construction builds the client (with the LWT) but performs no I/O.
    :meth:`connect` opens the connection and starts the receive loop;
    :meth:`disconnect` cleans up. Disconnect remains best-effort by default;
    temporary preflight clients opt into checked, redacted shutdown. The runner
    uses this concrete class so the connect/disconnect lifecycle (beyond the
    Protocol) is available.
    """

    _checked_disconnect = False
    _redacted_logging = False

    def __init__(
        self,
        settings: Settings,
        *,
        identifier: str | None = None,
        publish_availability: bool = True,
        checked_disconnect: bool = False,
        redacted_logging: bool = False,
    ) -> None:
        self._settings = settings
        # Multiple consumers (panel bridge + mesh publisher) each register a
        # command callback on this one shared connection — fan out to all.
        self._command_cbs: list[Callable[[str, str], Awaitable[None]]] = []
        self._message_cbs: list[Callable[[str, str, bool], Awaitable[None]]] = []
        self._payload_decode_error_cbs: list[
            Callable[[MqttPayloadDecodeError], Awaitable[None]]
        ] = []
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
        self._checked_disconnect = checked_disconnect
        self._redacted_logging = redacted_logging
        self._entered = False
        self._closed = False

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
            tls_context=build_tls_context(settings),
        )

    async def connect(self) -> None:
        """Open the broker connection and start the message reader task."""
        entry_task = asyncio.create_task(
            self._attempt_raw_enter(),
            name="brilliant-mqtt-enter",
        )
        entry_error, cancellation = await _settle_lifecycle_task(entry_task)
        if entry_error is None:
            self._entered = True
        if cancellation is not None:
            raise cancellation from None
        if entry_error is not None:
            raise entry_error

        self._reader_task = asyncio.create_task(self._read_loop())
        if self._redacted_logging:
            logger.info("connected temporary MQTT client")
        else:
            logger.info(
                "connected to MQTT broker %s:%s",
                self._settings.mqtt_host,
                self._settings.mqtt_port,
            )

    async def _attempt_raw_enter(self) -> BaseException | None:
        try:
            await self._client.__aenter__()
        except BaseException as error:
            return error
        return None

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
            payload_decode_error_cbs = list(self._payload_decode_error_cbs)
            if not command_cbs and not message_cbs and not payload_decode_error_cbs:
                # No consumer registered yet — drop (reconcile re-subscribes).
                continue
            try:
                topic = str(message.topic)
            except Exception:
                if self._redacted_logging:
                    logger.warning("failed decoding temporary MQTT message; continuing")
                else:
                    logger.exception("failed decoding MQTT message; continuing")
                continue
            try:
                payload = _decode_payload(message.payload)
            except UnicodeDecodeError:
                if self._redacted_logging:
                    logger.warning("failed decoding temporary MQTT message; continuing")
                else:
                    logger.exception("failed decoding MQTT message; continuing")
                decode_error = MqttPayloadDecodeError(
                    topic=topic,
                    retained=bool(message.retain),
                )
                for payload_decode_error_cb in payload_decode_error_cbs:
                    try:
                        await payload_decode_error_cb(decode_error)
                    except Exception:
                        if self._redacted_logging:
                            logger.warning(
                                "temporary MQTT payload decode callback failed; continuing"
                            )
                        else:
                            logger.exception("payload decode callback failed; continuing")
                continue
            except Exception:
                # Broad by design: keep the reader alive across any single
                # message's decode failure.
                if self._redacted_logging:
                    logger.warning("failed decoding temporary MQTT message; continuing")
                else:
                    logger.exception("failed decoding MQTT message; continuing")
                continue
            if self._redacted_logging:
                logger.debug("temporary MQTT message received (%d bytes)", len(payload))
            else:
                logger.debug("mqtt message on %s (%d bytes)", topic, len(payload))
            for command_cb in command_cbs:
                try:
                    await command_cb(topic, payload)
                except Exception:
                    # Broad by design — see the docstring.
                    if self._redacted_logging:
                        logger.warning("temporary MQTT command callback failed; continuing")
                    else:
                        logger.exception("command callback failed; continuing")
            for message_cb in message_cbs:
                try:
                    await message_cb(topic, payload, bool(message.retain))
                except Exception:
                    if self._redacted_logging:
                        logger.warning("temporary MQTT message callback failed; continuing")
                    else:
                        logger.exception("message callback failed; continuing")

    async def disconnect(self) -> None:
        """Publish a clean offline LWT, stop the reader, and close the client.

        Publishing "offline" retained here (rather than relying on the broker's
        LWT) gives a deterministic offline marker on an orderly stop (plan M7
        Step 3 — "clean LWT on exit"). Skipped when this adapter does not own the
        panel's availability topic (a secondary election-only consumer). The
        default remains best-effort; checked mode finishes every close step and
        then raises one generic error if any step failed.
        """
        if not self._entered or self._closed:
            return

        failed = False
        if self._publish_availability:
            try:
                await self._client.publish(self._avail_topic, payload="offline", retain=True)
            except aiomqtt.MqttError as exc:
                # Ordinary when the link is already down (every runner reconnect
                # cycle hits this): one quiet line, no traceback — the broker-side
                # LWT publishes the retained "offline" for us. MqttCodeError is a
                # subclass of MqttError, so this catch covers both.
                if self._checked_disconnect:
                    failed = True
                    logger.warning("clean offline publish failed; broker-side LWT covers it")
                else:
                    logger.warning(
                        "clean offline publish failed (%s); broker-side LWT covers it", exc
                    )
            except Exception:
                # Anything non-MQTT here is genuinely unexpected. Resident mode
                # keeps the traceback and remains best-effort; checked mode
                # records the failure without exposing its exception text.
                if self._checked_disconnect:
                    failed = True
                    logger.error("failed publishing clean offline availability")
                else:
                    logger.exception("failed publishing clean offline availability")

        if self._checked_disconnect:

            async def close_checked() -> bool:
                self._closed = True
                try:
                    await self._client.__aexit__(None, None, None)
                except asyncio.CancelledError:
                    return False
                except Exception:
                    return False
                finally:
                    self._entered = False
                return True

            if self._reader_task is not None:
                reader_task = self._reader_task
                reader_task.cancel()

                async def wait_reader_checked() -> bool:
                    try:
                        await reader_task
                    except asyncio.CancelledError:
                        return True
                    except Exception:
                        return False
                    return True

                try:
                    # aiomqtt's iterator observes the disconnected state driven
                    # by __aexit__, so start close before draining the reader.
                    closed, reader_stopped = await asyncio.gather(
                        close_checked(),
                        wait_reader_checked(),
                    )
                finally:
                    self._reader_task = None
                if not reader_stopped:
                    failed = True
                    logger.error("reader task failed during cancellation")
            else:
                closed = (await asyncio.gather(close_checked()))[0]
            if not closed:
                failed = True
                logger.error("failed closing MQTT client")
        else:
            if self._reader_task is not None:
                reader_task = self._reader_task
                reader_task.cancel()
                try:
                    await reader_task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    logger.exception("reader task raised during cancellation")
                finally:
                    self._reader_task = None
            try:
                self._closed = True
                await self._client.__aexit__(None, None, None)
            except Exception:
                logger.exception("failed closing MQTT client")
            finally:
                self._entered = False

        if failed:
            raise RuntimeError(_DISCONNECT_ERROR) from None

    # -- MqttClient Protocol -------------------------------------------------

    async def publish(self, topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        if qos not in (0, 1, 2):
            raise ValueError("qos must be between 0 and 2")
        await self._client.publish(topic, payload=payload, retain=retain, qos=qos)

    def on_command(self, cb: Callable[[str, str], Awaitable[None]]) -> None:
        self._command_cbs.append(cb)

    def on_message(self, cb: Callable[[str, str, bool], Awaitable[None]]) -> None:
        self._message_cbs.append(cb)

    def on_payload_decode_error(
        self,
        cb: Callable[[MqttPayloadDecodeError], Awaitable[None]],
    ) -> None:
        """Register metadata-only handling for inbound invalid UTF-8."""
        self._payload_decode_error_cbs.append(cb)

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


async def _settle_lifecycle_task(
    task: asyncio.Task[BaseException | None],
) -> tuple[BaseException | None, asyncio.CancelledError | None]:
    """Wait for raw MQTT entry without cancelling its executor-backed work."""
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
