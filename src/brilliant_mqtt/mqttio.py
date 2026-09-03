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
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import aiomqtt

from brilliant_mqtt.config import Settings
from brilliant_mqtt.discovery import availability_topic
from brilliant_mqtt.mapping import AUX_SPECS
from brilliant_mqtt.protocols import CommandSubscribeError

logger = logging.getLogger(__name__)

_DISCONNECT_ERROR = "MQTT disconnect failed"
_LIFECYCLE_ERROR = "MQTT adapter cannot be reused"
_TOPIC_QUEUE_MAXSIZE = 8
_SHUTDOWN_DRAIN_DEADLINE_S = 5.0
# Post-cancel settlement bound: workers SHOULD exit promptly on cancel, but a
# callback that swallows CancelledError must not wedge disconnect (finding 4).
_SHUTDOWN_WORKER_SETTLE_S = 1.0
_NUMBER_AUX_VARS = frozenset(
    spec.var for specs in AUX_SPECS.values() for spec in specs if spec.component == "number"
)


@dataclass(frozen=True, slots=True)
class MqttPayloadDecodeError:
    """Metadata-only signal for an inbound payload that is not valid UTF-8."""

    topic: str
    retained: bool


@dataclass(frozen=True, slots=True)
class _InboundMessage:
    topic: str
    payload: str
    retained: bool
    command_cbs: tuple[Callable[[str, str], Awaitable[None]], ...]
    message_cbs: tuple[Callable[[str, str, bool], Awaitable[None]], ...]


class _LaneQueue:
    """Bounded FIFO with filtered latest-wins replacement by exact topic."""

    def __init__(self, maxsize: int) -> None:
        self._maxsize = maxsize
        self._pending: deque[_InboundMessage] = deque()
        self._condition = asyncio.Condition()
        self._unfinished_tasks = 0
        self._finished = asyncio.Event()
        self._finished.set()

    @property
    def unfinished_tasks(self) -> int:
        return self._unfinished_tasks

    async def put(self, message: _InboundMessage, *, latest_wins: bool) -> None:
        async with self._condition:
            if latest_wins:
                for index, pending in enumerate(self._pending):
                    if pending.topic == message.topic:
                        self._pending[index] = message
                        return
            await self._condition.wait_for(lambda: len(self._pending) < self._maxsize)
            self._pending.append(message)
            self._unfinished_tasks += 1
            self._finished.clear()
            self._condition.notify()

    async def get(self) -> _InboundMessage:
        async with self._condition:
            await self._condition.wait_for(lambda: bool(self._pending))
            message = self._pending.popleft()
            self._condition.notify_all()
            return message

    def task_done(self) -> None:
        if self._unfinished_tasks <= 0:
            raise ValueError("task_done() called too many times")
        self._unfinished_tasks -= 1
        if self._unfinished_tasks == 0:
            self._finished.set()

    async def join(self) -> None:
        await self._finished.wait()


class _TopicDispatcher:
    """Serialize peripheral commands while retaining cross-lane concurrency."""

    def __init__(
        self,
        handler: Callable[[_InboundMessage], Awaitable[None]],
    ) -> None:
        self._handler = handler
        self._queues: dict[str, _LaneQueue] = {}
        self._workers: dict[str, asyncio.Task[None]] = {}
        self._closing = False
        self._shutdown_task: asyncio.Task[None] | None = None
        # Cancelled workers that outlived the settle bound — held so the
        # abandoned tasks are not garbage-collected mid-flight.
        self._abandoned: set[asyncio.Task[None]] = set()

    async def dispatch(self, message: _InboundMessage, *, latest_wins: bool) -> None:
        """Queue one message, replacing only safely superseded pending work."""
        if self._closing:
            return
        lane = _command_lane_key(message.topic)
        queue = self._queues.get(lane)
        if queue is None:
            queue = _LaneQueue(maxsize=_TOPIC_QUEUE_MAXSIZE)
            self._queues[lane] = queue
            self._workers[lane] = asyncio.create_task(
                self._run_worker(queue),
                name="brilliant-mqtt-command-lane-worker",
            )
        await queue.put(message, latest_wins=latest_wins)

    async def shutdown(self) -> None:
        """Drain accepted messages, then cancel and forget the idle workers."""
        self._closing = True
        if self._shutdown_task is None:
            self._shutdown_task = asyncio.create_task(
                self._drain_and_cancel(),
                name="brilliant-mqtt-topic-shutdown",
            )
        cancellation: asyncio.CancelledError | None = None
        while True:
            try:
                await asyncio.shield(self._shutdown_task)
            except asyncio.CancelledError as error:
                if self._shutdown_task.cancelled():
                    raise
                if cancellation is None:
                    cancellation = error
                continue
            break
        if cancellation is not None:
            raise cancellation from None

    async def _drain_and_cancel(self) -> None:
        try:
            if self._queues:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*(queue.join() for queue in self._queues.values())),
                        timeout=_SHUTDOWN_DRAIN_DEADLINE_S,
                    )
                except asyncio.TimeoutError:
                    undrained = sum(queue.unfinished_tasks for queue in self._queues.values())
                    logger.warning(
                        "MQTT dispatcher shutdown deadline expired; "
                        "%d undrained commands were abandoned",
                        undrained,
                    )
        finally:
            workers = list(self._workers.values())
            for worker in workers:
                worker.cancel()
            if workers:
                # Cancellation is cooperative: a callback that swallows or
                # delays CancelledError must not hold session teardown open
                # forever. asyncio.wait (unlike wait_for) imposes the bound
                # WITHOUT needing the stragglers to be cancellable — wait_for
                # would await the uncancellable gather and wedge anyway.
                _done, pending = await asyncio.wait(workers, timeout=_SHUTDOWN_WORKER_SETTLE_S)
                if pending:
                    logger.warning(
                        "MQTT dispatcher workers did not settle after cancel; abandoning"
                    )
                    # Keep strong references until the stragglers finish.
                    for task in pending:
                        self._abandoned.add(task)
                        task.add_done_callback(self._abandoned.discard)
            self._workers.clear()
            self._queues.clear()

    async def _run_worker(self, queue: _LaneQueue) -> None:
        while True:
            message = await queue.get()
            try:
                await self._handler(message)
            finally:
                queue.task_done()


def _is_latest_wins_topic(topic: str) -> bool:
    """Whether queued payloads on *topic* may safely supersede each other."""
    parts = topic.split("/")
    if len(parts) != 4 or parts[0] != "brilliant" or parts[1] == "ha-control":
        return False
    command = parts[3]
    if command == "set":
        return True
    if not command.startswith("set_"):
        return False
    return command.removeprefix("set_") in _NUMBER_AUX_VARS


def _command_lane_key(topic: str) -> str:
    """Group primary and auxiliary commands by their target peripheral."""
    parts = topic.split("/")
    if len(parts) != 4 or parts[0] != "brilliant":
        return topic
    command = parts[3]
    if command != "set" and not command.startswith("set_"):
        return topic
    return "/".join(parts[:3])


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
        self._topic_dispatcher = _TopicDispatcher(self._dispatch_inbound)
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
        self._connect_started = False
        self._entered = False
        self._closing = False
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
        """Open this one-shot adapter and start its message reader task."""
        if self._connect_started or self._closing or self._closed:
            raise RuntimeError(_LIFECYCLE_ERROR)
        self._connect_started = True
        entry_task = asyncio.create_task(
            self._attempt_raw_enter(),
            name="brilliant-mqtt-enter",
        )
        entry_error, cancellation = await _settle_lifecycle_task(entry_task)
        if entry_error is None:
            self._entered = True
        else:
            self._closed = True
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
        callback cannot kill the loop OR starve the other callbacks. Valid
        messages are handed to per-peripheral workers: one slow bus command
        cannot block another peripheral, while every command for one peripheral
        retains strict ordering.
        """
        dispatcher = self._get_topic_dispatcher()
        try:
            async for message in self._client.messages:
                command_cbs = tuple(self._command_cbs)
                message_cbs = tuple(self._message_cbs)
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
                # Accepted lossless-FIFO trade-off: a saturated lane
                # backpressures this one broker reader, delaying every later
                # topic so bounded lossless pending commands are never dropped.
                await dispatcher.dispatch(
                    _InboundMessage(
                        topic=topic,
                        payload=payload,
                        retained=bool(message.retain),
                        command_cbs=command_cbs,
                        message_cbs=message_cbs,
                    ),
                    latest_wins=_is_latest_wins_topic(topic),
                )
        finally:
            await dispatcher.shutdown()

    def _get_topic_dispatcher(self) -> _TopicDispatcher:
        """Return the dispatcher, lazily covering off-panel object doubles."""
        dispatcher = getattr(self, "_topic_dispatcher", None)
        if dispatcher is None:
            dispatcher = _TopicDispatcher(self._dispatch_inbound)
            self._topic_dispatcher = dispatcher
        return dispatcher

    async def _dispatch_inbound(self, message: _InboundMessage) -> None:
        """Invoke every callback for one message inside its topic worker."""
        for command_cb in message.command_cbs:
            try:
                await command_cb(message.topic, message.payload)
            except Exception:
                if self._redacted_logging:
                    logger.warning("temporary MQTT command callback failed; continuing")
                else:
                    logger.exception("command callback failed; continuing")
        for message_cb in message.message_cbs:
            try:
                await message_cb(message.topic, message.payload, message.retained)
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
        if self._closed:
            return
        if self._closing or (self._connect_started and not self._entered):
            raise RuntimeError(_LIFECYCLE_ERROR)
        if not self._entered:
            return
        self._closing = True

        failed = False
        cancellation: asyncio.CancelledError | None = None
        if self._publish_availability:
            try:
                await self._client.publish(self._avail_topic, payload="offline", retain=True)
            except asyncio.CancelledError as error:
                cancellation = error
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

        reader_settle_task: asyncio.Task[BaseException | None] | None = None
        if self._reader_task is not None:
            self._reader_task.cancel()
            reader_settle_task = asyncio.create_task(
                _attempt_reader_stop(self._reader_task),
                name="brilliant-mqtt-reader-stop",
            )

        # Let every accepted command finish before closing MQTT so a bridge
        # callback can still publish its post-write state echo. Idle workers
        # are cancelled after their queues drain.
        try:
            await self._topic_dispatcher.shutdown()
        except asyncio.CancelledError as error:
            cancellation = cancellation or error

        # The reader was cancelled above but may need the raw context manager
        # to reach its disconnected state before its iterator fully settles.
        exit_task = asyncio.create_task(
            self._attempt_raw_exit(),
            name="brilliant-mqtt-exit",
        )
        exit_error, exit_cancellation = await _settle_lifecycle_task(exit_task)
        cancellation = cancellation or exit_cancellation
        reader_error: BaseException | None = None
        if reader_settle_task is not None:
            reader_error, reader_cancellation = await _settle_lifecycle_task(reader_settle_task)
            cancellation = cancellation or reader_cancellation

        # The adapter becomes terminal only after both owned tasks settle. Raw
        # context-manager instances are one-shot even when exit itself fails.
        self._reader_task = None
        self._entered = False
        self._closing = False
        self._closed = True

        if reader_error is not None:
            if self._checked_disconnect:
                failed = True
                logger.error("reader task failed during cancellation")
            elif isinstance(reader_error, Exception):
                logger.error(
                    "reader task raised during cancellation",
                    exc_info=(
                        type(reader_error),
                        reader_error,
                        reader_error.__traceback__,
                    ),
                )
            else:
                raise reader_error

        if exit_error is not None:
            if self._checked_disconnect:
                failed = True
                logger.error("failed closing MQTT client")
            elif isinstance(exit_error, asyncio.CancelledError):
                if cancellation is None:
                    raise exit_error
            elif isinstance(exit_error, Exception):
                logger.error(
                    "failed closing MQTT client",
                    exc_info=(type(exit_error), exit_error, exit_error.__traceback__),
                )
            else:
                raise exit_error

        if cancellation is not None:
            raise cancellation from None
        if failed:
            raise RuntimeError(_DISCONNECT_ERROR) from None

    async def _attempt_raw_exit(self) -> BaseException | None:
        try:
            await self._client.__aexit__(None, None, None)
        except BaseException as error:
            return error
        return None

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
        try:
            reason_codes = await self._client.subscribe(topic)
        except aiomqtt.MqttError as error:
            raise CommandSubscribeError(f"subscribe failed for {topic}: {error}") from error

        rejected = [
            code
            for code in reason_codes
            if (code >= 0x80 if isinstance(code, int) else code.is_failure)
        ]
        if rejected:
            reasons = ", ".join(str(code) for code in rejected)
            raise CommandSubscribeError(f"subscribe rejected for {topic}: {reasons}")

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


async def _attempt_reader_stop(task: asyncio.Task[None]) -> BaseException | None:
    """Collect one reader's terminal state without exposing it in checked mode."""
    try:
        await task
    except asyncio.CancelledError:
        return None
    except BaseException as error:
        return error
    return None
