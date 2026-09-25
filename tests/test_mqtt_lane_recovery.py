"""Command-lane recovery and metadata-only observability regressions."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

import pytest

from brilliant_mqtt import mqttio
from brilliant_mqtt.mqttio import _InboundMessage, _LaneQueue, _TopicDispatcher

_PRIVATE_TOPIC = "brilliant/private-panel/private-peripheral/set_screen_on"
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
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The failed command is loud; every pending admission is handled once."""
    monkeypatch.setattr(mqttio, "_SHUTDOWN_DRAIN_DEADLINE_S", 0.01)
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
        pending_payloads = [str(index) for index in range(mqttio._TOPIC_QUEUE_MAXSIZE)]
        for payload in pending_payloads:
            await dispatcher.dispatch(_message(payload), latest_wins=False)
        overflow = asyncio.create_task(dispatcher.dispatch(_message("overflow"), latest_wins=False))
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

        lane_payloads = [payload for topic, payload in seen if topic == _PRIVATE_TOPIC]
        assert lane_payloads == ["fatal", *pending_payloads, "overflow"]
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
    def __init__(self, stop_after: int) -> None:
        self._stop_after = stop_after
        self.delays: list[float] = []
        self.stopped = asyncio.Event()

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)
        if len(self.delays) >= self._stop_after:
            self.stopped.set()
            await asyncio.Future()


async def test_permanently_dead_lane_uses_capped_backoff_without_spinning(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(mqttio, "_SHUTDOWN_DRAIN_DEADLINE_S", 0.01)
    restart = _BackoffRecorder(stop_after=5)
    dispatcher = _AlwaysDeadDispatcher(lambda _message: asyncio.sleep(0), restart_sleep=restart)
    caplog.set_level(logging.ERROR, logger="brilliant_mqtt.mqttio")
    try:
        await dispatcher.dispatch(_message("pending"), latest_wins=False)
        await asyncio.wait_for(restart.stopped.wait(), timeout=0.2)
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
        observed = len(restart.delays)
        await _turns(20)
        assert len(restart.delays) == observed
        assert caplog.text.count("MQTT command lane worker died unexpectedly") == 5
        assert _PRIVATE_PAYLOAD not in caplog.text
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


async def test_genuine_worker_cancellation_is_not_restarted_or_logged_as_death(
    caplog: pytest.LogCaptureFixture,
) -> None:
    restart_delays: list[float] = []
    started = asyncio.Event()

    async def restart_sleep(delay: float) -> None:
        restart_delays.append(delay)

    async def handler(_message: _InboundMessage) -> None:
        started.set()
        await asyncio.Future()

    dispatcher = _TopicDispatcher(handler, restart_sleep=restart_sleep)
    caplog.set_level(logging.ERROR, logger="brilliant_mqtt.mqttio")
    await dispatcher.dispatch(_message("cancel"), latest_wins=False)
    await asyncio.wait_for(started.wait(), timeout=0.2)
    worker = next(iter(dispatcher._workers.values()))

    worker.cancel()
    await asyncio.gather(worker, return_exceptions=True)
    await _turns()

    assert worker.cancelled()
    assert next(iter(dispatcher._workers.values())) is worker
    assert restart_delays == []
    assert "worker died unexpectedly" not in caplog.text
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
