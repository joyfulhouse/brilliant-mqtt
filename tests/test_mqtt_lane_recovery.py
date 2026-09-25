"""Command-lane recovery and metadata-only observability regressions."""

from __future__ import annotations

import asyncio
import gc
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from types import SimpleNamespace
from typing import Any, cast

import pytest

from brilliant_mqtt import mqttio
from brilliant_mqtt.mqttio import AioMqttAdapter, _InboundMessage, _LaneQueue, _TopicDispatcher
from brilliant_mqtt.write_admission import WriteCancelled

_PRIVATE_TOPIC = "brilliant/private-panel/private-peripheral/set_screen_on"
_PRIMARY_TOPIC = "brilliant/private-panel/private-peripheral/set"
_BUTTON_TOPIC = "brilliant/private-panel/private-peripheral/set_reset"
_OTHER_TOPIC = "brilliant/private-panel/independent-peripheral/set_screen_on"
_PRIVATE_PAYLOAD = "private-payload-secret"


def _message(
    payload: str,
    *,
    topic: str = _PRIVATE_TOPIC,
    command_cbs: tuple[Callable[[str, str], Awaitable[None]], ...] = (),
) -> _InboundMessage:
    return _InboundMessage(topic, payload, False, command_cbs, ())


async def _turns(count: int = 10) -> None:
    for _ in range(count):
        await asyncio.sleep(0)


def _reader_adapter(
    messages: AsyncIterator[SimpleNamespace], dispatcher: _TopicDispatcher
) -> AioMqttAdapter:
    async def registered_callback(_topic: str, _payload: str) -> None:
        raise AssertionError("the injected dispatcher handler owns this test")

    adapter = object.__new__(AioMqttAdapter)
    adapter._client = cast(Any, SimpleNamespace(messages=messages))
    adapter._command_cbs = [registered_callback]
    adapter._message_cbs = []
    adapter._payload_decode_error_cbs = []
    adapter._redacted_logging = False
    adapter._topic_dispatcher = dispatcher
    return adapter


class _RestartGate:
    def __init__(self) -> None:
        self.delays: list[float] = []
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.cancelled = False

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)
        self.entered.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise


async def test_dead_lane_recovers_saturated_work_without_blocking_independent_lane(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The failed command is loud; every pending admission is handled once."""
    crash = asyncio.Event()
    fatal_started = asyncio.Event()
    restart = _RestartGate()
    seen: list[tuple[str, str]] = []

    async def handler(message: _InboundMessage) -> None:
        seen.append((message.topic, message.payload))
        if message.payload == "fatal":
            fatal_started.set()
            await crash.wait()
            raise RuntimeError(_PRIVATE_PAYLOAD)

    dispatcher = _TopicDispatcher(handler, restart_sleep=restart)
    overflow: asyncio.Task[None] | None = None
    caplog.set_level(logging.DEBUG, logger="brilliant_mqtt.mqttio")
    try:
        await dispatcher.dispatch(_message("fatal"), latest_wins=False)
        await asyncio.wait_for(fatal_started.wait(), timeout=0.2)
        saturating_messages = [
            (_message('{"state":"ON"}', topic=_PRIMARY_TOPIC), True),
            (_message('{"brightness":120}', topic=_PRIMARY_TOPIC), True),
            (_message('{"state":"TOGGLE"}', topic=_PRIMARY_TOPIC), True),
            (_message('{"brightness":80}', topic=_PRIMARY_TOPIC), True),
            *[(_message(f"barrier-{index}", topic=_BUTTON_TOPIC), False) for index in range(5)],
        ]
        for message, latest_wins in saturating_messages:
            await dispatcher.dispatch(message, latest_wins=latest_wins)
        lane_queue = dispatcher._queues[mqttio._command_lane_key(_PRIVATE_TOPIC)]
        assert len(lane_queue._pending) == mqttio._TOPIC_QUEUE_MAXSIZE
        overflow = asyncio.create_task(
            dispatcher.dispatch(_message("overflow", topic=_BUTTON_TOPIC), latest_wins=False)
        )
        await _turns()
        assert not overflow.done()

        crash.set()
        await asyncio.wait_for(restart.entered.wait(), timeout=0.2)

        # Recovery backoff is lane-local: another peripheral still progresses.
        await dispatcher.dispatch(_message("independent", topic=_OTHER_TOPIC), latest_wins=False)
        for _ in range(20):
            if (_OTHER_TOPIC, "independent") in seen:
                break
            await asyncio.sleep(0)
        assert (_OTHER_TOPIC, "independent") in seen
        assert not overflow.done()

        restart.release.set()
        await asyncio.wait_for(overflow, timeout=0.2)
        await asyncio.wait_for(
            asyncio.gather(*(queue.join() for queue in dispatcher._queues.values())),
            timeout=0.2,
        )

        lane_messages = [item for item in seen if item != (_OTHER_TOPIC, "independent")]
        assert lane_messages == [
            (_PRIVATE_TOPIC, "fatal"),
            (_PRIMARY_TOPIC, '{"state":"ON","brightness":120}'),
            (_PRIMARY_TOPIC, '{"state":"TOGGLE"}'),
            (_PRIMARY_TOPIC, '{"brightness":80}'),
            *[(_BUTTON_TOPIC, f"barrier-{index}") for index in range(5)],
            (_BUTTON_TOPIC, "overflow"),
        ]
        assert all(queue.unfinished_tasks == 0 for queue in dispatcher._queues.values())
        assert "MQTT command lane worker died unexpectedly" in caplog.text
        assert _PRIVATE_PAYLOAD not in caplog.text
        assert "private-panel" not in caplog.text
    finally:
        crash.set()
        restart.release.set()
        if overflow is not None and not overflow.done():
            overflow.cancel()
        if overflow is not None:
            await asyncio.gather(overflow, return_exceptions=True)
        await dispatcher.shutdown()


class _AlwaysDeadDispatcher(_TopicDispatcher):
    async def _run_worker(self, lane: str, queue: _LaneQueue) -> None:
        del lane, queue
        raise RuntimeError(_PRIVATE_PAYLOAD)


class _BackoffRecorder:
    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)


class _ExhaustionGate:
    def __init__(self) -> None:
        self.delays: list[float] = []
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)
        self.entered.set()
        await self.release.wait()
        if len(self.delays) > 5:
            await asyncio.Future()


async def test_permanently_dead_lane_uses_capped_backoff_without_spinning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    restart = _BackoffRecorder()
    dispatcher = _AlwaysDeadDispatcher(lambda _message: asyncio.sleep(0), restart_sleep=restart)
    caplog.set_level(logging.ERROR, logger="brilliant_mqtt.mqttio")
    try:
        await dispatcher.dispatch(_message("pending"), latest_wins=False)
        lane = mqttio._command_lane_key(_PRIVATE_TOPIC)
        for _ in range(30):
            if lane not in dispatcher._workers and lane not in dispatcher._recoveries:
                break
            await asyncio.sleep(0)
        expected = [
            mqttio._LANE_RESTART_BASE_DELAY_S,
            min(
                mqttio._LANE_RESTART_BASE_DELAY_S * 2,
                mqttio._LANE_RESTART_MAX_DELAY_S,
            ),
            min(
                mqttio._LANE_RESTART_BASE_DELAY_S * 4,
                mqttio._LANE_RESTART_MAX_DELAY_S,
            ),
            mqttio._LANE_RESTART_MAX_DELAY_S,
            mqttio._LANE_RESTART_MAX_DELAY_S,
        ]
        assert restart.delays == expected
        assert lane not in dispatcher._workers
        assert lane not in dispatcher._recoveries
        assert lane in dispatcher._closed_lanes
        await _turns(20)
        assert restart.delays == expected
        assert caplog.text.count("MQTT command lane worker died unexpectedly") == 5
        assert caplog.text.count("MQTT command lane recovery exhausted") == 1
        assert _PRIVATE_PAYLOAD not in caplog.text
    finally:
        await dispatcher.shutdown()


class _DeadPrivateLaneDispatcher(_TopicDispatcher):
    async def _run_worker(self, lane: str, queue: _LaneQueue) -> None:
        if lane == mqttio._command_lane_key(_PRIVATE_TOPIC):
            raise RuntimeError(_PRIVATE_PAYLOAD)
        await super()._run_worker(lane, queue)


class _FiveDeathsDispatcher(_TopicDispatcher):
    _deaths_remaining = 5

    async def _run_worker(self, lane: str, queue: _LaneQueue) -> None:
        if lane == mqttio._command_lane_key(_PRIVATE_TOPIC) and self._deaths_remaining:
            self._deaths_remaining -= 1
            raise RuntimeError(_PRIVATE_PAYLOAD)
        await super()._run_worker(lane, queue)


async def test_reader_progresses_after_dead_lane_saturates(
    caplog: pytest.LogCaptureFixture,
) -> None:
    restart = _ExhaustionGate()
    finish = asyncio.Event()
    independent_seen = asyncio.Event()
    seen: list[str] = []
    saturating_messages = [
        (_PRIMARY_TOPIC, '{"state":"ON"}'),
        (_PRIMARY_TOPIC, '{"brightness":120}'),
        (_PRIMARY_TOPIC, '{"state":"TOGGLE"}'),
        (_PRIMARY_TOPIC, '{"brightness":80}'),
        *((_BUTTON_TOPIC, f"barrier-{index}") for index in range(5)),
        (_BUTTON_TOPIC, "overflow"),
    ]

    async def handler(message: _InboundMessage) -> None:
        seen.append(message.payload)
        if seen == ["independent-1", "independent-2"]:
            independent_seen.set()

    async def messages() -> AsyncIterator[SimpleNamespace]:
        for topic, payload in saturating_messages:
            yield SimpleNamespace(topic=topic, payload=payload.encode(), retain=False)
        yield SimpleNamespace(topic=_OTHER_TOPIC, payload=b"independent-1", retain=False)
        yield SimpleNamespace(topic=_PRIVATE_TOPIC, payload=b"rejected", retain=False)
        yield SimpleNamespace(topic=_OTHER_TOPIC, payload=b"independent-2", retain=False)
        await finish.wait()

    dispatcher = _DeadPrivateLaneDispatcher(handler, restart_sleep=restart)
    adapter = _reader_adapter(messages(), dispatcher)
    caplog.set_level(logging.ERROR, logger="brilliant_mqtt.mqttio")
    reader = asyncio.create_task(adapter._read_loop())
    try:
        await asyncio.wait_for(restart.entered.wait(), timeout=0.2)
        dead_queue = dispatcher._queues[mqttio._command_lane_key(_PRIVATE_TOPIC)]
        for _ in range(20):
            if len(dead_queue._pending) == mqttio._TOPIC_QUEUE_MAXSIZE:
                break
            await asyncio.sleep(0)
        assert len(dead_queue._pending) == mqttio._TOPIC_QUEUE_MAXSIZE
        assert not independent_seen.is_set()

        restart.release.set()
        await asyncio.wait_for(independent_seen.wait(), timeout=0.2)
        await asyncio.wait_for(
            dispatcher._queues[mqttio._command_lane_key(_OTHER_TOPIC)].join(), timeout=0.2
        )

        assert seen == ["independent-1", "independent-2"]
        assert dead_queue.unfinished_tasks == 0
        assert list(dead_queue._pending) == []
        assert len(restart.delays) == 5
        await _turns(20)
        assert len(restart.delays) == 5
        assert mqttio._command_lane_key(_PRIVATE_TOPIC) not in dispatcher._workers
        assert "MQTT command lane recovery exhausted" in caplog.text
        assert "8 admitted commands discarded" in caplog.text
        assert "future commands rejected during cooldown" in caplog.text
        assert _PRIVATE_PAYLOAD not in caplog.text
        assert "private-panel" not in caplog.text
    finally:
        finish.set()
        reader.cancel()
        await asyncio.gather(reader, return_exceptions=True)


async def test_closed_lane_rejection_logs_once_and_settles(
    caplog: pytest.LogCaptureFixture,
) -> None:
    restart = _ExhaustionGate()
    dispatcher = _AlwaysDeadDispatcher(lambda _message: asyncio.sleep(0), restart_sleep=restart)
    caplog.set_level(logging.WARNING, logger="brilliant_mqtt.mqttio")
    try:
        await dispatcher.dispatch(_message("pending"), latest_wins=False)
        await asyncio.wait_for(restart.entered.wait(), timeout=0.2)
        restart.release.set()
        lane = mqttio._command_lane_key(_PRIVATE_TOPIC)
        for _ in range(20):
            if lane not in dispatcher._workers:
                break
            await asyncio.sleep(0)
        assert lane not in dispatcher._workers
        queue = dispatcher._queues[lane]

        for index in range(3):
            await dispatcher.dispatch(_message(f"rejected-{index}"), latest_wins=False)
        await dispatcher.shutdown()

        assert queue.unfinished_tasks == 0
        assert list(queue._pending) == []
        rejection_logs = [
            record.getMessage()
            for record in caplog.records
            if record.getMessage().startswith("MQTT command rejected during lane recovery cooldown")
        ]
        assert rejection_logs == [
            "MQTT command rejected during lane recovery cooldown; further rejection logs suppressed"
        ]
        assert "MQTT command lane remained closed through teardown" in caplog.text
        assert "rejected commands while closed: 3" in caplog.text
        assert _PRIVATE_PAYLOAD not in caplog.text
        assert "private-panel" not in caplog.text
    finally:
        restart.release.set()
        await dispatcher.shutdown()


async def test_closed_lane_reopens_after_cooldown(
    caplog: pytest.LogCaptureFixture,
) -> None:
    restart = _ExhaustionGate()
    handled = asyncio.Event()
    seen: list[str] = []

    async def handler(message: _InboundMessage) -> None:
        seen.append(message.payload)
        handled.set()

    dispatcher = _FiveDeathsDispatcher(handler, restart_sleep=restart)
    caplog.set_level(logging.INFO, logger="brilliant_mqtt.mqttio")
    try:
        await dispatcher.dispatch(_message("pending"), latest_wins=False)
        await asyncio.wait_for(restart.entered.wait(), timeout=0.2)
        restart.release.set()
        lane = mqttio._command_lane_key(_PRIVATE_TOPIC)
        for _ in range(20):
            if lane not in dispatcher._workers:
                break
            await asyncio.sleep(0)
        assert lane not in dispatcher._workers
        assert lane not in dispatcher._recoveries
        closed_queue = dispatcher._queues[lane]
        closed = dispatcher._closed_lanes[lane]

        closed.reopen_at = asyncio.get_running_loop().time() + 60.0
        await dispatcher.dispatch(_message("too-soon"), latest_wins=False)
        assert not handled.is_set()
        closed.reopen_at = asyncio.get_running_loop().time() - 1.0
        await dispatcher.dispatch(_message("after-cooldown"), latest_wins=False)
        await asyncio.wait_for(handled.wait(), timeout=0.2)
        await asyncio.wait_for(dispatcher._queues[lane].join(), timeout=0.2)

        assert seen == ["after-cooldown"]
        assert dispatcher._queues[lane] is not closed_queue
        assert lane in dispatcher._workers
        assert lane not in dispatcher._recoveries
        assert "MQTT command lane reopened after recovery cooldown" in caplog.text
        assert "rejected commands while closed: 1" in caplog.text

        await dispatcher.shutdown()
        await dispatcher.dispatch(_message("after-shutdown"), latest_wins=False)
        assert seen == ["after-cooldown"]
        assert dispatcher._workers == {}
        assert dispatcher._recoveries == {}
    finally:
        await dispatcher.shutdown()


async def test_shutdown_during_restart_backoff_never_resurrects_lane(
    caplog: pytest.LogCaptureFixture,
) -> None:
    restart = _RestartGate()
    calls = 0

    async def handler(_message: _InboundMessage) -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError(_PRIVATE_PAYLOAD)

    dispatcher = _TopicDispatcher(handler, restart_sleep=restart)
    caplog.set_level(logging.ERROR, logger="brilliant_mqtt.mqttio")
    await dispatcher.dispatch(_message("fatal"), latest_wins=False)
    await asyncio.wait_for(restart.entered.wait(), timeout=0.2)

    await dispatcher.shutdown()
    await _turns()

    assert calls == 1
    assert restart.cancelled
    assert dispatcher._workers == {}
    assert dispatcher._recoveries == {}
    assert _PRIVATE_PAYLOAD not in caplog.text


async def test_shutdown_discards_dead_lane_without_waiting_for_default_deadline(
    caplog: pytest.LogCaptureFixture,
) -> None:
    restart = _RestartGate()

    async def handler(_message: _InboundMessage) -> None:
        raise RuntimeError(_PRIVATE_PAYLOAD)

    dispatcher = _TopicDispatcher(handler, restart_sleep=restart)
    caplog.set_level(logging.WARNING, logger="brilliant_mqtt.mqttio")
    await dispatcher.dispatch(_message("fatal"), latest_wins=False)
    await asyncio.wait_for(restart.entered.wait(), timeout=0.2)
    await dispatcher.dispatch(_message("pending"), latest_wins=False)
    queue = dispatcher._queues[mqttio._command_lane_key(_PRIVATE_TOPIC)]
    assert mqttio._SHUTDOWN_DRAIN_DEADLINE_S == 5.0

    started = asyncio.get_running_loop().time()
    await dispatcher.shutdown()
    elapsed = asyncio.get_running_loop().time() - started

    assert elapsed < 0.5
    assert restart.cancelled
    assert queue.unfinished_tasks == 0
    assert list(queue._pending) == []
    assert "MQTT dispatcher discarded 1 undrainable command during teardown" in caplog.text
    assert _PRIVATE_PAYLOAD not in caplog.text


async def test_reader_reconnect_teardown_during_backoff_never_resurrects_lane() -> None:
    restart = _RestartGate()
    calls = 0

    async def handler(_message: _InboundMessage) -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError(_PRIVATE_PAYLOAD)

    async def reconnect_messages() -> AsyncIterator[SimpleNamespace]:
        yield SimpleNamespace(topic=_PRIVATE_TOPIC, payload=b"fatal", retain=False)
        await restart.entered.wait()

    dispatcher = _TopicDispatcher(handler, restart_sleep=restart)
    adapter = _reader_adapter(reconnect_messages(), dispatcher)

    await asyncio.wait_for(adapter._read_loop(), timeout=0.2)
    await _turns()

    assert calls == 1
    assert restart.cancelled
    assert dispatcher._workers == {}
    assert dispatcher._recoveries == {}


async def test_worker_death_logs_sanitized_type_and_location(
    caplog: pytest.LogCaptureFixture,
) -> None:
    restart = _RestartGate()

    async def failing_handler(_message: _InboundMessage) -> None:
        raise RuntimeError(_PRIVATE_PAYLOAD)

    dispatcher = _TopicDispatcher(failing_handler, restart_sleep=restart)
    caplog.set_level(logging.ERROR, logger="brilliant_mqtt.mqttio")
    try:
        await dispatcher.dispatch(_message("fatal"), latest_wins=False)
        await asyncio.wait_for(restart.entered.wait(), timeout=0.2)

        death_records = [
            record
            for record in caplog.records
            if "MQTT command lane worker died unexpectedly" in record.getMessage()
        ]
        assert len(death_records) == 1
        assert "type=RuntimeError" in death_records[0].getMessage()
        assert "test_mqtt_lane_recovery.py" in death_records[0].getMessage()
        assert "failing_handler" in death_records[0].getMessage()
        assert death_records[0].exc_info is None
        assert _PRIVATE_PAYLOAD not in caplog.text
    finally:
        restart.release.set()
        await dispatcher.shutdown()


async def test_worker_death_during_drain_logs_sanitized_metadata(
    caplog: pytest.LogCaptureFixture,
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def failing_handler(_message: _InboundMessage) -> None:
        started.set()
        await release.wait()
        raise RuntimeError(_PRIVATE_PAYLOAD)

    dispatcher = _TopicDispatcher(failing_handler)
    caplog.set_level(logging.ERROR, logger="brilliant_mqtt.mqttio")
    await dispatcher.dispatch(_message("fatal"), latest_wins=False)
    await asyncio.wait_for(started.wait(), timeout=0.2)
    shutdown = asyncio.create_task(dispatcher.shutdown())
    await _turns()
    assert dispatcher._closing

    release.set()
    await asyncio.wait_for(shutdown, timeout=0.2)

    assert "MQTT command lane worker failed during teardown" in caplog.text
    assert "type=RuntimeError" in caplog.text
    assert "failing_handler" in caplog.text
    assert _PRIVATE_PAYLOAD not in caplog.text
    assert all(record.exc_info is None for record in caplog.records)


async def test_post_shutdown_straggler_exception_is_retrieved_and_sanitized(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(mqttio, "_SHUTDOWN_DRAIN_DEADLINE_S", 0.01)
    monkeypatch.setattr(mqttio, "_SHUTDOWN_WORKER_SETTLE_S", 0.01)
    started = asyncio.Event()
    finish = asyncio.Event()
    loop_errors: list[dict[str, Any]] = []
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()

    def capture_loop_error(
        _loop: asyncio.AbstractEventLoop,
        context: dict[str, Any],
    ) -> None:
        loop_errors.append(context)

    async def failing_handler(_message: _InboundMessage) -> None:
        started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            await finish.wait()
        raise RuntimeError(_PRIVATE_PAYLOAD)

    dispatcher = _TopicDispatcher(failing_handler)
    caplog.set_level(logging.ERROR, logger="brilliant_mqtt.mqttio")
    loop.set_exception_handler(capture_loop_error)
    try:
        await dispatcher.dispatch(_message("fatal"), latest_wins=False)
        await asyncio.wait_for(started.wait(), timeout=0.2)
        await asyncio.wait_for(dispatcher.shutdown(), timeout=0.2)
        assert len(dispatcher._abandoned) == 1

        finish.set()
        for _ in range(20):
            if not dispatcher._abandoned:
                break
            await asyncio.sleep(0)
        gc.collect()
        await _turns()

        assert dispatcher._abandoned == set()
        assert loop_errors == []
        assert "MQTT command lane worker failed during teardown" in caplog.text
        assert "type=RuntimeError" in caplog.text
        assert "failing_handler" in caplog.text
        assert _PRIVATE_PAYLOAD not in caplog.text
        assert all(record.exc_info is None for record in caplog.records)
    finally:
        finish.set()
        loop.set_exception_handler(previous_handler)
        await dispatcher.shutdown()


async def test_cancelled_worker_closes_saturated_lane_and_releases_reader(
    caplog: pytest.LogCaptureFixture,
) -> None:
    restart_delays: list[float] = []
    started = asyncio.Event()
    independent_seen = asyncio.Event()
    finish = asyncio.Event()

    async def restart_sleep(delay: float) -> None:
        restart_delays.append(delay)

    async def handler(message: _InboundMessage) -> None:
        if message.payload == "active":
            started.set()
            await asyncio.Future()
        if message.payload == "independent":
            independent_seen.set()

    async def messages() -> AsyncIterator[SimpleNamespace]:
        yield SimpleNamespace(topic=_PRIVATE_TOPIC, payload=b"active", retain=False)
        for index in range(mqttio._TOPIC_QUEUE_MAXSIZE):
            yield SimpleNamespace(
                topic=_BUTTON_TOPIC,
                payload=f"pending-{index}".encode(),
                retain=False,
            )
        yield SimpleNamespace(topic=_BUTTON_TOPIC, payload=b"overflow", retain=False)
        yield SimpleNamespace(topic=_OTHER_TOPIC, payload=b"independent", retain=False)
        await finish.wait()

    dispatcher = _TopicDispatcher(handler, restart_sleep=restart_sleep)
    adapter = _reader_adapter(messages(), dispatcher)
    caplog.set_level(logging.ERROR, logger="brilliant_mqtt.mqttio")
    reader = asyncio.create_task(adapter._read_loop())
    lane = mqttio._command_lane_key(_PRIVATE_TOPIC)
    queue: _LaneQueue | None = None
    try:
        await asyncio.wait_for(started.wait(), timeout=0.2)
        queue = dispatcher._queues[lane]
        for _ in range(20):
            if len(queue._pending) == mqttio._TOPIC_QUEUE_MAXSIZE:
                break
            await asyncio.sleep(0)
        assert len(queue._pending) == mqttio._TOPIC_QUEUE_MAXSIZE
        worker = dispatcher._workers[lane]

        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
        await asyncio.wait_for(independent_seen.wait(), timeout=0.2)
        await asyncio.wait_for(
            dispatcher._queues[mqttio._command_lane_key(_OTHER_TOPIC)].join(), timeout=0.2
        )

        assert worker.cancelled()
        assert lane not in dispatcher._workers
        assert lane not in dispatcher._recoveries
        assert lane in dispatcher._closed_lanes
        assert queue.unfinished_tasks == 0
        assert list(queue._pending) == []
        assert restart_delays == []
        assert "MQTT command lane worker cancelled outside teardown" in caplog.text
        assert "worker died unexpectedly" not in caplog.text
        assert _PRIVATE_PAYLOAD not in caplog.text
        assert "private-panel" not in caplog.text
    finally:
        finish.set()
        if queue is not None and queue.unfinished_tasks:
            await queue.discard_pending()
        reader.cancel()
        await asyncio.gather(reader, return_exceptions=True)
        await dispatcher.shutdown()


async def test_write_cancelled_continues_on_same_worker(
    caplog: pytest.LogCaptureFixture,
) -> None:
    restart_delays: list[float] = []
    seen: list[str] = []

    async def restart_sleep(delay: float) -> None:
        restart_delays.append(delay)

    async def handler(message: _InboundMessage) -> None:
        seen.append(message.payload)
        if message.payload == "cancelled":
            raise WriteCancelled()

    dispatcher = _TopicDispatcher(handler, restart_sleep=restart_sleep)
    caplog.set_level(logging.DEBUG, logger="brilliant_mqtt.mqttio")
    try:
        await dispatcher.dispatch(_message("cancelled"), latest_wins=False)
        worker = next(iter(dispatcher._workers.values()))
        await dispatcher.dispatch(_message("next"), latest_wins=False)
        queue = next(iter(dispatcher._queues.values()))
        await asyncio.wait_for(queue.join(), timeout=0.2)

        assert seen == ["cancelled", "next"]
        assert next(iter(dispatcher._workers.values())) is worker
        assert not worker.done()
        assert queue.unfinished_tasks == 0
        assert restart_delays == []
        assert "MQTT command write cancelled; continuing lane" in caplog.text
        assert "worker died unexpectedly" not in caplog.text
    finally:
        await dispatcher.shutdown()


async def test_failed_fold_hook_logs_metadata_only_before_queue_fallback(
    caplog: pytest.LogCaptureFixture,
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    seen: list[str] = []

    async def handler(message: _InboundMessage) -> None:
        seen.append(message.payload)
        if message.payload == "first":
            started.set()
            await release.wait()

    dispatcher = _TopicDispatcher(handler)
    try:
        await dispatcher.dispatch(_message("first"), latest_wins=True)
        await asyncio.wait_for(started.wait(), timeout=0.2)
        admission = next(iter(dispatcher._active.values()))[1]

        def failed_fold(_payload: str) -> bool:
            raise RuntimeError(_PRIVATE_PAYLOAD)

        admission.try_supersede = failed_fold
        caplog.set_level(logging.WARNING, logger="brilliant_mqtt.mqttio")
        await dispatcher.dispatch(_message(_PRIVATE_PAYLOAD), latest_wins=True)
        release.set()
        await asyncio.wait_for(next(iter(dispatcher._queues.values())).join(), timeout=0.2)

        assert seen == ["first", _PRIVATE_PAYLOAD]
        assert "MQTT command fold hook failed; falling back to queued dispatch" in caplog.text
        assert _PRIVATE_PAYLOAD not in caplog.text
        assert "private-panel" not in caplog.text
    finally:
        release.set()
        await dispatcher.shutdown()


async def test_fold_discard_cleanup_logs_metadata_only(
    caplog: pytest.LogCaptureFixture,
) -> None:
    started = asyncio.Event()

    async def handler(_message: _InboundMessage) -> None:
        started.set()
        await asyncio.Future()

    dispatcher = _TopicDispatcher(handler)
    await dispatcher.dispatch(_message("first"), latest_wins=True)
    await asyncio.wait_for(started.wait(), timeout=0.2)
    admission = next(iter(dispatcher._active.values()))[1]
    admission.try_supersede = lambda _payload: True
    await dispatcher.dispatch(_message(_PRIVATE_PAYLOAD), latest_wins=True)
    assert dispatcher._folded
    worker = next(iter(dispatcher._workers.values()))

    caplog.set_level(logging.DEBUG, logger="brilliant_mqtt.mqttio")
    worker.cancel()
    await asyncio.gather(worker, return_exceptions=True)

    assert dispatcher._folded == {}
    assert "MQTT command lane discarded a folded replacement during cleanup" in caplog.text
    assert _PRIVATE_PAYLOAD not in caplog.text
    assert "private-panel" not in caplog.text
    await dispatcher.shutdown()
