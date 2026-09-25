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
import json
import logging
import math
import ssl
import traceback
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from pathlib import Path
from typing import NoReturn

import aiomqtt

from brilliant_mqtt.config import Settings
from brilliant_mqtt.diagnostics import ResponseDiagnostics
from brilliant_mqtt.discovery import availability_topic
from brilliant_mqtt.mapping import AUX_SPECS
from brilliant_mqtt.protocols import CommandSubscribeError
from brilliant_mqtt.write_admission import CommandAdmission, WriteCancelled, command_admission

logger = logging.getLogger(__name__)

_DISCONNECT_ERROR = "MQTT disconnect failed"
_LIFECYCLE_ERROR = "MQTT adapter cannot be reused"
_TOPIC_QUEUE_MAXSIZE = 8
# Transport-level admission bound, upstream of the per-command lanes (#90).
# aiomqtt 2.5.1 defaults its incoming queue to maxsize=0 (unbounded); a stalled
# bus command backpressures the sole reader while paho keeps enqueueing, so
# memory grows until the 96 MiB-capped service (deploy/brilliant-mqtt.service:23)
# is restarted. Sized to eight full command lanes so a whole-home multi-
# peripheral burst — each distinct peripheral is one un-coalescible admission —
# is absorbed without a false overload rebuild, while the byte budget below is
# the true memory guard.
_TRANSPORT_QUEUE_MAXSIZE = _TOPIC_QUEUE_MAXSIZE * 8
# Cumulative encoded topic+payload byte budget for the admission queue. Commands
# are small (absolute-state setters, scene ids), so this ceiling binds only on
# pathological large payloads, capping the backlog at a small, defensible
# fraction (~0.27%) of the service MemoryMax=96M with wide headroom for the rest
# of the agent's working set. On every fresh-admission path this is a HARD cap
# (a put that would exceed it trips before storing). In the latest-wins
# coalesce-replace path (put_nowait) it is a per-admission SOFT bound: a
# replacement is stored before the cumulative total is checked, so the total can
# transiently exceed this by up to one admitted item per distinct latest-wins
# topic before the runner's next tick consumes the overload latch and rebuilds.
# The hard worst case until that rebuild is therefore bounded by
# _TRANSPORT_QUEUE_MAXSIZE * _TRANSPORT_QUEUE_MAX_BYTES (~16 MiB), not this
# single value — still bounded (no unbounded growth), just not a strict ceiling.
_TRANSPORT_QUEUE_MAX_BYTES = 256 * 1024
_SHUTDOWN_DRAIN_DEADLINE_S = 5.0
# Post-cancel settlement bound: workers SHOULD exit promptly on cancel, but a
# callback that swallows CancelledError must not wedge disconnect (finding 4).
_SHUTDOWN_WORKER_SETTLE_S = 1.0
_LANE_RESTART_BASE_DELAY_S = 0.1
_LANE_RESTART_MAX_DELAY_S = 0.5
_LANE_RESTART_MAX_FAILURES = 5
_LANE_REOPEN_COOLDOWN_S = 30.0
_LANE_FAILURE_FRAME_LIMIT = 3
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


@dataclass(slots=True)
class _ClosedLaneState:
    reopen_at: float
    rejected: int = 0


def _worker_failure_metadata(error: BaseException | None) -> tuple[str, str]:
    if error is None:
        return "none", "none"
    frames = traceback.extract_tb(error.__traceback__, limit=-_LANE_FAILURE_FRAME_LIMIT)
    locations = ",".join(
        f"{Path(frame.filename).name}:{frame.lineno}:{frame.name}" for frame in frames
    )
    return type(error).__qualname__, locations or "none"


class _LaneQueue:
    """Bounded FIFO with filtered latest-wins replacement by exact topic."""

    def __init__(self, maxsize: int, *, diagnostics: ResponseDiagnostics | None = None) -> None:
        self._maxsize = maxsize
        self._diagnostics = diagnostics
        self._pending: deque[_InboundMessage] = deque()
        self._condition = asyncio.Condition()
        self._unfinished_tasks = 0
        self._finished = asyncio.Event()
        self._finished.set()
        self._accepting = True

    @property
    def unfinished_tasks(self) -> int:
        return self._unfinished_tasks

    async def put(self, message: _InboundMessage, *, latest_wins: bool) -> bool:
        async with self._condition:
            if not self._accepting:
                return False
            if latest_wins:
                for index in range(len(self._pending) - 1, -1, -1):
                    pending = self._pending[index]
                    if (
                        not _is_latest_wins_topic(pending.topic)
                        or _coalesce_payload(pending.topic, pending.payload, pending.payload)
                        is None
                    ):
                        break
                    if pending.topic == message.topic:
                        if (
                            pending.retained != message.retained
                            or pending.command_cbs != message.command_cbs
                            or pending.message_cbs != message.message_cbs
                        ):
                            break
                        payload = _coalesce_payload(message.topic, pending.payload, message.payload)
                        if payload is None:
                            break
                        self._pending[index] = replace(message, payload=payload)
                        # Transport and lane queues own disjoint pending sets:
                        # moving a command removes it upstream, and a replaced
                        # command never advances, so it can be counted only once.
                        if self._diagnostics is not None:
                            self._diagnostics.note_superseded()
                        return True
            await self._condition.wait_for(
                lambda: not self._accepting or len(self._pending) < self._maxsize
            )
            if not self._accepting:
                return False
            self._pending.append(message)
            self._unfinished_tasks += 1
            self._finished.clear()
            self._condition.notify()
            return True

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

    async def discard_pending(self) -> int:
        async with self._condition:
            self._accepting = False
            discarded = len(self._pending)
            self._pending.clear()
            for _ in range(discarded):
                self.task_done()
            self._condition.notify_all()
            return discarded


class _TopicDispatcher:
    """Serialize peripheral commands while retaining cross-lane concurrency."""

    def __init__(
        self,
        handler: Callable[[_InboundMessage], Awaitable[None]],
        *,
        diagnostics: ResponseDiagnostics | None = None,
        restart_sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._handler = handler
        self._diagnostics = diagnostics
        self._restart_sleep = restart_sleep
        self._queues: dict[str, _LaneQueue] = {}
        self._workers: dict[str, asyncio.Task[None]] = {}
        self._recoveries: dict[str, asyncio.Task[None]] = {}
        self._worker_failures: dict[str, int] = {}
        self._closed_lanes: dict[str, _ClosedLaneState] = {}
        self._active: dict[str, tuple[_InboundMessage, CommandAdmission]] = {}
        self._folded: dict[str, _InboundMessage] = {}
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
        closed = self._closed_lanes.get(lane)
        if (
            closed is not None
            and asyncio.get_running_loop().time() >= closed.reopen_at
            and lane not in self._workers
            and lane not in self._recoveries
        ):
            self._closed_lanes.pop(lane, None)
            self._worker_failures.pop(lane, None)
            queue = _LaneQueue(maxsize=_TOPIC_QUEUE_MAXSIZE, diagnostics=self._diagnostics)
            self._queues[lane] = queue
            self._start_worker(lane, queue)
            logger.info(
                "MQTT command lane reopened after recovery cooldown "
                "(rejected commands while closed: %d)",
                closed.rejected,
            )
        if queue is None:
            queue = _LaneQueue(maxsize=_TOPIC_QUEUE_MAXSIZE, diagnostics=self._diagnostics)
            self._queues[lane] = queue
            self._start_worker(lane, queue)
        active = self._active.get(lane)
        if latest_wins and active is not None and not queue._pending:
            previous, admission = active
            if (
                previous.topic == message.topic
                and previous.command_cbs == message.command_cbs
                and previous.message_cbs == message.message_cbs
                and previous.retained == message.retained
                and admission.try_supersede is not None
            ):
                try:
                    folded = admission.try_supersede(message.payload)
                except Exception:
                    # Preserve the normal callback's error boundary: malformed
                    # input must not escape this optimization into the reader.
                    logger.warning("MQTT command fold hook failed; falling back to queued dispatch")
                    folded = False
                if folded:
                    self._folded[lane] = message
                    return
        accepted = await queue.put(message, latest_wins=latest_wins)
        if accepted:
            return
        closed = self._closed_lanes.get(lane)
        if closed is None:
            return
        closed.rejected += 1
        if closed.rejected == 1:
            logger.warning(
                "MQTT command rejected during lane recovery cooldown; "
                "further rejection logs suppressed"
            )

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
            drainable: list[_LaneQueue] = []
            undrainable = 0
            for lane, queue in self._queues.items():
                worker = self._workers.get(lane)
                if worker is not None and not worker.done():
                    drainable.append(queue)
                    continue
                discarded = await self._close_lane(
                    lane,
                    queue,
                    worker,
                    cooldown=False,
                )
                if discarded is not None:
                    undrainable += discarded
            if undrainable:
                suffix = "" if undrainable == 1 else "s"
                logger.warning(
                    "MQTT dispatcher discarded %d undrainable command%s during teardown",
                    undrainable,
                    suffix,
                )
            if drainable:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*(queue.join() for queue in drainable)),
                        timeout=_SHUTDOWN_DRAIN_DEADLINE_S,
                    )
                except asyncio.TimeoutError:
                    undrained = sum(queue.unfinished_tasks for queue in drainable)
                    logger.warning(
                        "MQTT dispatcher shutdown deadline expired; "
                        "%d undrained commands were abandoned",
                        undrained,
                    )
        finally:
            recoveries = list(self._recoveries.values())
            for recovery in recoveries:
                recovery.cancel()
            if recoveries:
                await asyncio.gather(*recoveries, return_exceptions=True)
            self._recoveries.clear()
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
            self._worker_failures.clear()
            for closed in self._closed_lanes.values():
                if closed.rejected:
                    logger.warning(
                        "MQTT command lane remained closed through teardown "
                        "(rejected commands while closed: %d)",
                        closed.rejected,
                    )
            self._closed_lanes.clear()

    def _start_worker(self, lane: str, queue: _LaneQueue) -> None:
        worker = asyncio.create_task(
            self._run_worker(lane, queue),
            name="brilliant-mqtt-command-lane-worker",
        )
        self._workers[lane] = worker
        worker.add_done_callback(lambda completed: self._worker_done(lane, queue, completed))

    def _worker_done(
        self,
        lane: str,
        queue: _LaneQueue,
        worker: asyncio.Task[None],
    ) -> None:
        error = None if worker.cancelled() else worker.exception()
        if self._workers.get(lane) is not worker:
            if error is not None:
                failure_type, failure_frames = _worker_failure_metadata(error)
                logger.error(
                    "MQTT command lane worker failed during teardown or after replacement "
                    "(type=%s; frames=%s)",
                    failure_type,
                    failure_frames,
                )
            return
        if self._closing:
            if error is not None:
                failure_type, failure_frames = _worker_failure_metadata(error)
                logger.error(
                    "MQTT command lane worker failed during teardown (type=%s; frames=%s)",
                    failure_type,
                    failure_frames,
                )
            closure = asyncio.create_task(
                self._close_completed_worker(lane, queue, worker, cooldown=False),
                name="brilliant-mqtt-command-lane-closure",
            )
            self._recoveries[lane] = closure
            return
        if worker.cancelled():
            closure = asyncio.create_task(
                self._close_completed_worker(lane, queue, worker, cooldown=True),
                name="brilliant-mqtt-command-lane-closure",
            )
            self._recoveries[lane] = closure
            return
        failure_type, failure_frames = _worker_failure_metadata(error)
        failures = self._worker_failures.get(lane, 0) + 1
        self._worker_failures[lane] = failures
        delay = min(
            _LANE_RESTART_BASE_DELAY_S * 2 ** (failures - 1),
            _LANE_RESTART_MAX_DELAY_S,
        )
        logger.error(
            "MQTT command lane worker died unexpectedly; recovery %d/%d in %.3fs "
            "(%d admitted commands pending; type=%s; frames=%s)",
            failures,
            _LANE_RESTART_MAX_FAILURES,
            delay,
            queue.unfinished_tasks,
            failure_type,
            failure_frames,
        )
        recovery = asyncio.create_task(
            self._recover_worker(lane, queue, worker, failures, delay),
            name="brilliant-mqtt-command-lane-recovery",
        )
        self._recoveries[lane] = recovery

    async def _close_lane(
        self,
        lane: str,
        queue: _LaneQueue,
        worker: asyncio.Task[None] | None,
        *,
        cooldown: bool,
    ) -> int | None:
        if self._queues.get(lane) is not queue or self._workers.get(lane) is not worker:
            return None
        self._workers.pop(lane, None)
        if cooldown:
            self._closed_lanes[lane] = _ClosedLaneState(
                reopen_at=asyncio.get_running_loop().time() + _LANE_REOPEN_COOLDOWN_S
            )
        return await queue.discard_pending()

    async def _close_completed_worker(
        self,
        lane: str,
        queue: _LaneQueue,
        worker: asyncio.Task[None],
        *,
        cooldown: bool,
    ) -> None:
        try:
            if cooldown and self._closing:
                return
            discarded = await self._close_lane(lane, queue, worker, cooldown=cooldown)
            if cooldown and discarded is not None:
                logger.error(
                    "MQTT command lane worker cancelled outside teardown; "
                    "%d admitted commands discarded and lane closed for cooldown",
                    discarded,
                )
        finally:
            closure = asyncio.current_task()
            if self._recoveries.get(lane) is closure:
                self._recoveries.pop(lane, None)

    async def _recover_worker(
        self,
        lane: str,
        queue: _LaneQueue,
        dead_worker: asyncio.Task[None],
        failures: int,
        delay: float,
    ) -> None:
        try:
            await self._restart_sleep(delay)
            if self._closing or self._workers.get(lane) is not dead_worker:
                return
            if failures >= _LANE_RESTART_MAX_FAILURES:
                discarded = await self._close_lane(
                    lane,
                    queue,
                    dead_worker,
                    cooldown=True,
                )
                if discarded is None:
                    return
                logger.error(
                    "MQTT command lane recovery exhausted after %d consecutive worker deaths; "
                    "%d admitted commands discarded and future commands rejected during cooldown "
                    "(%.0fs)",
                    failures,
                    discarded,
                    _LANE_REOPEN_COOLDOWN_S,
                )
                return
            self._start_worker(lane, queue)
        finally:
            recovery = asyncio.current_task()
            if self._recoveries.get(lane) is recovery:
                self._recoveries.pop(lane, None)

    async def _run_worker(self, lane: str, queue: _LaneQueue) -> None:
        while True:
            message = await queue.get()
            admission = CommandAdmission()
            token = command_admission.set(admission)
            try:
                while True:
                    self._active[lane] = (message, admission)
                    await self._handler(message)
                    self._worker_failures.pop(lane, None)
                    replacement = self._folded.pop(lane, None)
                    if replacement is None:
                        break
                    message = replacement
            except WriteCancelled:
                # A bus admission can end independently of this worker. Actual
                # Task.cancel() still propagates as ordinary CancelledError.
                logger.debug("MQTT command write cancelled; continuing lane")
                self._worker_failures.pop(lane, None)
            finally:
                # A folded replacement can still belong to the lane before
                # its callback adopts it. Do not orphan it on cancellation.
                admission.ticket.cancel_waiting()
                command_admission.reset(token)
                self._active.pop(lane, None)
                discarded = self._folded.pop(lane, None)
                if discarded is not None:
                    logger.debug("MQTT command lane discarded a folded replacement during cleanup")
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


def _coalesce_payload(topic: str, before: str | bytes, after: str | bytes) -> str | None:
    """Combine absolute setters only when no earlier field or effect is lost."""
    try:
        before = before.decode("utf-8") if isinstance(before, bytes) else before
        after = after.decode("utf-8") if isinstance(after, bytes) else after
    except UnicodeDecodeError:
        return None
    if not topic.endswith("/set"):
        try:
            if not all(math.isfinite(float(payload)) for payload in (before, after)):
                return None
        except (ValueError, OverflowError):
            return None
        return after
    commands: list[dict[str, object]] = []
    for payload in (before, after):
        try:
            value = json.loads(payload)
        except (ValueError, RecursionError):
            return None
        if not isinstance(value, dict) or not value or value.keys() - {"state", "brightness"}:
            return None
        if "state" in value and value["state"] not in ("ON", "OFF"):
            return None
        if "brightness" in value:
            brightness = value["brightness"]
            if isinstance(brightness, bool) or not isinstance(brightness, (int, float)):
                return None
            try:
                if not math.isfinite(brightness):
                    return None
            except OverflowError:
                return None
        commands.append(value)
    # OFF ignores brightness in translation, so merging across it can change
    # the intensity left on the device for its next ON command.
    if any(command.get("state") == "OFF" for command in commands):
        if commands[0] != commands[1]:
            return None
    return json.dumps(commands[0] | commands[1], separators=(",", ":"))


def _command_lane_key(topic: str) -> str:
    """Group primary and auxiliary commands by their target peripheral."""
    parts = topic.split("/")
    if len(parts) != 4 or parts[0] != "brilliant":
        return topic
    command = parts[3]
    if command != "set" and not command.startswith("set_"):
        return topic
    return "/".join(parts[:3])


def _message_bytes(message: aiomqtt.Message) -> int:
    """Encoded topic+payload byte cost of one queued transport message."""
    return len(str(message.topic).encode("utf-8")) + len(message.payload)


class _TransportOverloadLatch:
    """Latch tripped when the bounded transport queue rejects a message.

    aiomqtt's incoming queue drops on ``QueueFull`` with only a 'Discarding
    message' log (``client._on_message``); this latch makes that overload
    OBSERVABLE to the runner, which rebuilds the session — a loud fail, not a
    silent drop (#90). Set on the event-loop thread (aiomqtt runs ``_on_message``
    there) and read+cleared by the session loop, mirroring the single-bool latch
    behind :meth:`bus.RpcBusAdapter.consume_write_timeout`.
    """

    def __init__(self) -> None:
        self._overloaded = False

    def trip(self) -> bool:
        """Latch overload; return True only for the first trip since a consume."""
        first = not self._overloaded
        self._overloaded = True
        return first

    def consume(self) -> bool:
        """Return and clear the overload latch."""
        overloaded = self._overloaded
        self._overloaded = False
        return overloaded


class _BoundedTransportQueue(asyncio.Queue[aiomqtt.Message]):
    """aiomqtt incoming-message queue bounded by count AND payload bytes (#90).

    aiomqtt builds this via its ``queue_type`` hook and, on ``QueueFull``, logs
    'Discarding message' and drops (``client._on_message``). On top of
    ``asyncio.Queue``'s count ``maxsize`` this enforces a cumulative payload-byte
    budget, coalesces latest-wins command topics at admission (mirroring
    :meth:`_LaneQueue.put` one layer earlier, so an idempotent absolute-state
    setter replaces a pending twin instead of consuming budget) and TRIPS an
    observable latch before raising ``QueueFull`` — the drop becomes a loud,
    observable overload the runner sheds-and-rebuilds on, rather than aiomqtt's
    silent discard. ``QueueFull`` is raised (not a bespoke error) because paho
    re-raises anything else escaping the callback.
    """

    _queue: deque[aiomqtt.Message]

    def __init__(
        self,
        maxsize: int = 0,
        *,
        max_bytes: int,
        overload: _TransportOverloadLatch,
        diagnostics: ResponseDiagnostics | None = None,
    ) -> None:
        super().__init__(maxsize)
        self._diagnostics = diagnostics
        self._max_bytes = max_bytes
        self._overload = overload
        self._queued_bytes = 0

    @property
    def queued_bytes(self) -> int:
        return self._queued_bytes

    def put_nowait(self, item: aiomqtt.Message) -> None:
        item_bytes = _message_bytes(item)
        topic = str(item.topic)
        if _is_latest_wins_topic(topic):
            for index in range(len(self._queue) - 1, -1, -1):
                pending = self._queue[index]
                pending_topic = str(pending.topic)
                if _command_lane_key(pending_topic) == _command_lane_key(topic) and (
                    not _is_latest_wins_topic(pending_topic)
                    or _coalesce_payload(pending_topic, pending.payload, pending.payload) is None
                ):
                    break
                if str(pending.topic) == topic:
                    if pending.retain != item.retain or pending.qos != item.qos:
                        break
                    payload = _coalesce_payload(topic, pending.payload, item.payload)
                    if payload is None:
                        break
                    item = aiomqtt.Message(
                        topic=item.topic,
                        payload=payload.encode("utf-8"),
                        qos=item.qos,
                        retain=item.retain,
                        mid=item.mid,
                        properties=item.properties,
                    )
                    item_bytes = _message_bytes(item)
                    if item_bytes > self._max_bytes:
                        # A single latest-wins payload too big to EVER fit the
                        # budget on its own: refuse it and keep the pending twin,
                        # so _queued_bytes stays hard-capped like every other
                        # admission path (replacing first could overshoot by an
                        # arbitrary amount). A message this large can never be
                        # admitted, so keeping the older value is safe — both are
                        # discarded on the ensuing rebuild regardless.
                        self._trip("payload-byte")
                    # The new payload fits the budget on its own. Latest-wins: it
                    # supersedes its pending twin (mirrors _LaneQueue.put) —
                    # replace in place first (count unchanged, so this never
                    # widens the queue), THEN trip if the CUMULATIVE total now
                    # exceeds budget. On that trip the new message is KEPT (a
                    # pre-rebuild drain applies the NEWEST value); what is shed is
                    # the OLD pending value, despite aiomqtt's generic "Discarding
                    # message" log. Storing before the check makes _max_bytes a
                    # per-admission soft bound here — see the
                    # _TRANSPORT_QUEUE_MAX_BYTES comment.
                    self._queued_bytes += item_bytes - _message_bytes(pending)
                    self._queue[index] = item
                    if self._diagnostics is not None:
                        self._diagnostics.note_superseded()
                    if self._queued_bytes > self._max_bytes:
                        self._trip("payload-byte")
                    return
        if self.full():
            self._trip("message-count")
        if self._queued_bytes + item_bytes > self._max_bytes:
            self._trip("payload-byte")
        super().put_nowait(item)
        self._queued_bytes += item_bytes

    def _get(self) -> aiomqtt.Message:
        item = super()._get()
        self._queued_bytes -= _message_bytes(item)
        return item

    def _trip(self, budget: str) -> NoReturn:
        if self._overload.trip():
            logger.error(
                "MQTT transport backlog exceeded its %s budget; "
                "rebuilding session to shed the overload (#90)",
                budget,
            )
        raise asyncio.QueueFull


def _transport_queue_type(
    overload: _TransportOverloadLatch,
    diagnostics: ResponseDiagnostics | None = None,
) -> type[asyncio.Queue[aiomqtt.Message]]:
    """Bind one adapter's overload latch into aiomqtt's ``queue_type`` hook.

    aiomqtt instantiates the returned class as ``queue_type(maxsize=...)`` with
    only the count maxsize, so the byte budget and latch are captured here.
    """

    class _AdapterTransportQueue(_BoundedTransportQueue):
        def __init__(self, maxsize: int = 0) -> None:
            super().__init__(
                maxsize,
                max_bytes=_TRANSPORT_QUEUE_MAX_BYTES,
                overload=overload,
                diagnostics=diagnostics,
            )

    return _AdapterTransportQueue


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
        diagnostics: ResponseDiagnostics | None = None,
    ) -> None:
        self._settings = settings
        self._diagnostics = diagnostics
        # Multiple consumers (panel bridge + mesh publisher) each register a
        # command callback on this one shared connection — fan out to all.
        self._command_cbs: list[Callable[[str, str], Awaitable[None]]] = []
        self._message_cbs: list[Callable[[str, str, bool], Awaitable[None]]] = []
        self._payload_decode_error_cbs: list[
            Callable[[MqttPayloadDecodeError], Awaitable[None]]
        ] = []
        # Tripped by the bounded transport queue (below) when a count/byte bound
        # is exceeded; drained by the runner via consume_transport_overload().
        self._transport_overload = _TransportOverloadLatch()
        self._topic_dispatcher = _TopicDispatcher(self._dispatch_inbound, diagnostics=diagnostics)
        self._reader_task: asyncio.Task[None] | None = None
        self._reader_failure_consumed = False
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
            # Bound the inbound transport backlog upstream of the command lanes
            # (#90): a custom queue enforcing a count AND payload-byte budget,
            # observably tripping on overload instead of aiomqtt's silent drop.
            queue_type=_transport_queue_type(self._transport_overload, diagnostics),
            max_queued_incoming_messages=_TRANSPORT_QUEUE_MAXSIZE,
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
                # A healthy saturated lane backpressures this broker reader to
                # preserve its bounded FIFO. Exhausted recovery closes only the
                # failed lane's queue and wakes this await.
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
            dispatcher = _TopicDispatcher(
                self._dispatch_inbound, diagnostics=getattr(self, "_diagnostics", None)
            )
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

        # A reader failure already surfaced by consume_reader_failure() was
        # logged there; re-reporting the same dead task here as a cancellation
        # failure is misleading (it did not raise during cancellation) and
        # doubles the traceback on every reader-crash rebuild (#89).
        if reader_error is not None and not self._reader_failure_consumed:
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

    def consume_reader_failure(self) -> bool:
        """Return True exactly once if the inbound reader ended without this adapter
        cancelling it."""
        reader_task = self._reader_task
        if reader_task is None or not reader_task.done() or self._reader_failure_consumed:
            return False
        if self._closing or self._closed:
            # This adapter is tearing the reader down itself — an expected stop,
            # not a failure, even when the task ends cancelled.
            return False
        self._reader_failure_consumed = True
        try:
            error: BaseException | None = reader_task.exception()
        except asyncio.CancelledError:
            # Cancelled while this adapter was live (not by our own teardown) —
            # still an unexpected death that must rebuild the session (#89).
            error = None
        if error is not None:
            logger.error(
                "MQTT reader task failed",
                exc_info=(type(error), error, error.__traceback__),
            )
        else:
            # Graceful return or a cancellation not initiated by our teardown —
            # no traceback to surface, but still an unexpected stop worth a line.
            logger.warning("MQTT reader stopped unexpectedly without an exception")
        return True

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

    def consume_transport_overload(self) -> bool:
        """Return and clear the inbound transport-overload latch (#90).

        Mirrors :meth:`bus.RpcBusAdapter.consume_write_timeout`: the runner
        checks this each session tick and rebuilds the session when overload is
        latched, turning aiomqtt's silent 'Discarding message' drop into an
        observable fail + reconnect. The overload SHEDS the excess commands
        (QoS-0 inbound has no broker acknowledgment or retry semantics at all, so
        a shed message is simply gone — never redelivered on reconnect regardless
        of session persistence) and rebuilds — admission control, not a lossless
        promise.
        """
        return self._transport_overload.consume()

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
