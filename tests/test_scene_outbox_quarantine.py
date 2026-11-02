"""#113 wedge reproduction: a broker that closes the session on one outbox frame.

The fake broker below models the only reachable wedge (an operator
``max_packet_size`` that one frame exceeds): every QoS-1 publish of the poison
frame makes the broker drop the connection. Each SceneBridge instance is one
MQTT session; the same state path across instances models agent restarts.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from pathlib import Path

import pytest

from brilliant_mqtt import outbox_quarantine as oq
from brilliant_mqtt import scene_bridge as scene_bridge_module
from brilliant_mqtt.diagnostics import ResponseDiagnostics
from brilliant_mqtt.ha_control_protocol import (
    scene_command_topic,
    scene_event_topic,
    scene_result_topic,
    transport_status_topic,
)
from brilliant_mqtt.scene_bridge import SceneBridge
from tests.fakes import FakeBus, FakeClockMs, FakeMqtt
from tests.test_scene_bridge import (
    _NOW_MS,
    _PANEL,
    _command,
    _execution,
    _mode_catalog,
    _payload,
    _published,
    _scene_catalog,
    _wait_for_bus_commands,
)

_POISON_SCENE = "poison"
_COMMAND_ID = "22222222-2222-4222-8222-222222222222"
_MANIFEST_TOPIC = "brilliant/office/owned-topics"


def _is_poison(topic: str, payload: str) -> bool:
    return topic == scene_event_topic(_PANEL) and f'"scene_id":"{_POISON_SCENE}"' in payload


class _SessionMqtt(FakeMqtt):
    """One broker session. ``mode`` decides what happens to the poison frame."""

    def __init__(self, mode: str) -> None:
        super().__init__()
        self.mode = mode
        self.poison_offers = 0
        self.release_hold = asyncio.Event()

    async def publish(self, topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        if self.connection_lost:
            raise ConnectionError("session closed")
        if qos >= 1 and _is_poison(topic, payload):
            self.poison_offers += 1
            if self.mode == "drop":
                # Mosquitto max_packet_size: the broker closes the connection.
                self.drop_connection()
                raise ConnectionError("broker closed the connection")
            if self.mode == "hold":
                await self.release_hold.wait()
        await super().publish(topic, payload, retain, qos)


class _Harness:
    def __init__(self, tmp_path: Path) -> None:
        self.path = tmp_path / "private" / "scene-watermarks.json"
        self.sidecar = oq.sidecar_path(self.path)
        self.clock = FakeClockMs(_NOW_MS)
        self.diagnostics = ResponseDiagnostics()

    async def session(
        self,
        mode: str = "drop",
        *,
        healthy: bool = True,
        execution_scene: str | None = None,
    ) -> tuple[SceneBridge, FakeBus, _SessionMqtt]:
        bus = FakeBus(
            [_execution()],
            scoped_devices=[
                _scene_catalog("all_off", _POISON_SCENE, "later"),
                _mode_catalog("away"),
            ],
        )
        mqtt = _SessionMqtt(mode)
        if healthy:
            # The retained-ownership manifest every session publishes at QoS 1
            # before the scene bridge starts: existing per-session traffic.
            await mqtt.publish(_MANIFEST_TOPIC, "{}", retain=True, qos=1)
        bridge = SceneBridge(
            bus,
            mqtt,
            _PANEL,
            self.path,
            self.clock,
            session_signals=mqtt,
            diagnostics=self.diagnostics,
        )
        await bridge.async_start()
        if execution_scene is not None:
            await bus.emit(_execution(execution_scene, self.clock.now_ms))
        return bridge, bus, mqtt

    def quarantine(self) -> oq.QuarantineState:
        return oq.load_sidecar(self.sidecar)

    def suspect(self) -> oq.SuspectRecord | None:
        for key, value in self.quarantine().suspects:
            if _POISON_SCENE in key:
                return value
        return None


async def _until(condition: Callable[[], bool], what: str) -> None:
    for _ in range(5000):
        if condition():
            return
        await asyncio.sleep(0.001)
    pytest.fail(f"timed out waiting for {what}")


def _failures(harness: _Harness) -> int:
    suspect = harness.suspect()
    return 0 if suspect is None else len(suspect.failures)


async def _offered(mqtt: _SessionMqtt) -> None:
    await _until(lambda: mqtt.poison_offers >= 1, "poison offer")


async def _failures_reach(harness: _Harness, count: int) -> None:
    await _until(lambda: _failures(harness) == count, f"{count} attributed failure(s)")


def _last_status(mqtt: FakeMqtt, transport: str) -> dict[str, object]:
    return _payload(_published(mqtt, transport_status_topic(transport, _PANEL))[-1])


async def _quarantine_poison(harness: _Harness) -> None:
    """Three attributable drops in three sessions spanning one minute."""
    for index in range(3):
        bridge, _, mqtt = await harness.session(
            execution_scene=_POISON_SCENE if index == 0 else None
        )
        await _offered(mqtt)
        await _failures_reach(harness, index + 1)
        await bridge.async_shutdown()
        if index < 2:
            await harness.clock.advance_ms(30_000)


async def test_poison_frame_is_quarantined_after_three_sessions_spanning_a_minute(
    tmp_path: Path,
) -> None:
    harness = _Harness(tmp_path)
    for index in range(3):
        bridge, _, mqtt = await harness.session(
            execution_scene=_POISON_SCENE if index == 0 else None
        )
        await _offered(mqtt)
        await _failures_reach(harness, index + 1)
        suspect = harness.suspect()
        assert suspect is not None
        if index < 2:
            # Fewer than three sessions: still retried, never skipped.
            assert suspect.quarantined_at_ms is None
        await bridge.async_shutdown()
        if index < 2:
            await harness.clock.advance_ms(30_000)

    suspect = harness.suspect()
    assert suspect is not None
    assert suspect.quarantined_at_ms == _NOW_MS + 60_000
    assert len({failure.session for failure in suspect.failures}) == 3
    # The outbox state file itself is unchanged in shape (no new record fields).
    stored = json.loads(harness.path.read_text())
    assert set(stored) == {
        "version",
        "watermarks",
        "mode_watermarks",
        "events",
        "results",
        "pending",
    }
    (event,) = stored["events"].values()
    assert set(event) == {"topic", "payload", "delivered", "created_at_ms"}
    assert event["delivered"] is False


async def test_three_sessions_inside_a_minute_are_not_enough(tmp_path: Path) -> None:
    harness = _Harness(tmp_path)
    for index in range(3):
        bridge, _, mqtt = await harness.session(
            execution_scene=_POISON_SCENE if index == 0 else None
        )
        await _offered(mqtt)
        await _failures_reach(harness, index + 1)
        await bridge.async_shutdown()
        await harness.clock.advance_ms(10_000)
    suspect = harness.suspect()
    assert suspect is not None and suspect.quarantined_at_ms is None

    bridge, _, mqtt = await harness.session()
    await _until(lambda: mqtt.poison_offers >= 1, "fourth offer")
    await _until(lambda: _failures(harness) == 4, "fourth failure")
    await bridge.async_shutdown()
    # Still under a minute in total (0, 10 s, 20 s, 30 s): keep retrying.
    suspect = harness.suspect()
    assert suspect is not None and suspect.quarantined_at_ms is None


async def test_quarantined_record_halts_outbox_without_churn_and_survives_restart(
    tmp_path: Path,
) -> None:
    harness = _Harness(tmp_path)
    await _quarantine_poison(harness)
    await harness.clock.advance_ms(30_000)

    bridge, bus, mqtt = await harness.session()
    await asyncio.sleep(0.05)
    # Never offered again, so the broker never drops this session.
    assert mqtt.poison_offers == 0
    assert mqtt.connection_lost is False
    for transport in ("scene", "mode"):
        status = _last_status(mqtt, transport)
        assert status["available"] is False
        assert status["reason"] == "outbox_quarantine"

    # Later events and results are held behind it (O1), never overtaking it.
    command = _command(_COMMAND_ID, "scene", "later", issued_at_ms=harness.clock.now_ms)
    await mqtt.inject(scene_command_topic(_PANEL), command)
    await _wait_for_bus_commands(bus, 1)
    await bus.emit(_execution("later", _NOW_MS + 90_000))
    await asyncio.sleep(0.05)
    assert _published(mqtt, scene_event_topic(_PANEL)) == []
    assert _published(mqtt, scene_result_topic(_COMMAND_ID)) == []
    stored = json.loads(harness.path.read_text())
    assert len(stored["events"]) == 2
    assert stored["results"][f"scene:{_COMMAND_ID}"]["delivered"] is False
    # Diagnostics carry the details, never the payload contents.
    outbox = harness.diagnostics.meta()["outbox"]
    assert isinstance(outbox, dict)
    assert outbox["state"] == "quarantined"
    assert outbox["kind"] == "event"
    assert isinstance(outbox["key"], str) and _POISON_SCENE in outbox["key"]
    assert outbox["failure_count"] == 3
    assert outbox["quarantined_at_ms"] == _NOW_MS + 60_000
    poison_payload = next(
        event["payload"] for event in stored["events"].values() if _POISON_SCENE in event["payload"]
    )
    assert outbox["payload_bytes"] == len(poison_payload.encode())
    assert outbox["payload_sha256"] == oq.payload_sha256(poison_payload)
    assert poison_payload not in json.dumps(harness.diagnostics.meta())
    await bridge.async_shutdown()

    restarted, _, restarted_mqtt = await harness.session()
    await asyncio.sleep(0.05)
    assert restarted_mqtt.poison_offers == 0
    assert _published(restarted_mqtt, scene_event_topic(_PANEL)) == []
    assert _last_status(restarted_mqtt, "scene")["reason"] == "outbox_quarantine"
    await restarted.async_shutdown()


async def test_never_healthy_session_counts_nothing_and_reports_outbox_blocked(
    tmp_path: Path,
) -> None:
    harness = _Harness(tmp_path)
    bridge, _, mqtt = await harness.session(healthy=False, execution_scene=_POISON_SCENE)
    await _until(lambda: harness.quarantine().blocked is not None, "blocked marker")
    await bridge.async_shutdown()
    assert harness.quarantine().suspects == ()
    assert mqtt.poison_offers == 1

    # The next session reports the blocked outbox until a delivery succeeds.
    bridge, _, mqtt = await harness.session("hold", healthy=False)
    await _until(lambda: mqtt.poison_offers == 1, "held offer")
    status = _last_status(mqtt, "scene")
    assert status["available"] is False
    assert status["reason"] == "outbox_blocked"
    outbox = harness.diagnostics.meta()["outbox"]
    assert isinstance(outbox, dict) and outbox["state"] == "blocked"

    mqtt.release_hold.set()
    await _until(lambda: bool(_published(mqtt, scene_event_topic(_PANEL))), "delivery")
    await _until(lambda: harness.quarantine().blocked is None, "blocked cleared")
    await _until(lambda: _last_status(mqtt, "scene")["available"] is True, "status cleared")
    assert "outbox" not in harness.diagnostics.meta()
    await bridge.async_shutdown()


async def test_drop_long_after_sending_is_not_attributed(tmp_path: Path) -> None:
    harness = _Harness(tmp_path)
    bridge, _, mqtt = await harness.session("hold", execution_scene=_POISON_SCENE)
    await _until(lambda: mqtt.poison_offers == 1, "held offer")
    await harness.clock.advance_ms(oq.ATTRIBUTION_WINDOW_MS + 1)
    mqtt.drop_connection()
    await asyncio.sleep(0.05)
    await bridge.async_shutdown()
    assert harness.quarantine() == oq.QuarantineState()
    assert not harness.sidecar.exists()


async def test_successful_delivery_resets_the_counter(tmp_path: Path) -> None:
    harness = _Harness(tmp_path)
    for index in range(2):
        bridge, _, mqtt = await harness.session(
            execution_scene=_POISON_SCENE if index == 0 else None
        )
        await _failures_reach(harness, index + 1)
        await bridge.async_shutdown()
        await harness.clock.advance_ms(30_000)

    bridge, _, mqtt = await harness.session("accept")
    await _until(lambda: bool(_published(mqtt, scene_event_topic(_PANEL))), "delivery")
    await _until(lambda: harness.suspect() is None, "counter reset")
    await bridge.async_shutdown()
    assert harness.quarantine() == oq.QuarantineState()


async def test_capacity_while_halted_keeps_full_behaviour_and_never_evicts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _Harness(tmp_path)
    await _quarantine_poison(harness)
    monkeypatch.setattr(scene_bridge_module, "_EVENT_OUTBOX_LIMIT", 2)

    bridge, bus, mqtt = await harness.session()
    await bus.emit(_execution("later", _NOW_MS + 70_000))
    # The outbox is now full (quarantined + one held event): the next event is
    # refused exactly as before #113, and the quarantined record stays.
    await bus.emit(_execution("all_off", _NOW_MS + 80_000))
    command = _command(_COMMAND_ID, "scene", "all_off", issued_at_ms=harness.clock.now_ms)
    await mqtt.inject(scene_command_topic(_PANEL), command)
    await asyncio.sleep(0.05)

    stored = json.loads(harness.path.read_text())
    assert len(stored["events"]) == 2
    assert any(_POISON_SCENE in key for key in stored["events"])
    assert "all_off" not in stored["watermarks"][_PANEL]
    assert bus.commands == []
    assert _published(mqtt, scene_event_topic(_PANEL)) == []
    # The quarantine is the root cause, so it stays the reported reason.
    assert _last_status(mqtt, "scene")["reason"] == "outbox_quarantine"
    await bridge.async_shutdown()


async def test_existing_state_without_sidecar_loads_and_delivers_unchanged(
    tmp_path: Path,
) -> None:
    harness = _Harness(tmp_path)
    bridge, _, mqtt = await harness.session("hold", execution_scene=_POISON_SCENE)
    await _until(lambda: mqtt.poison_offers == 1, "held offer")
    await bridge.async_shutdown()
    before = harness.path.read_bytes()
    assert not harness.sidecar.exists()

    bridge, _, mqtt = await harness.session("accept")
    await _until(lambda: bool(_published(mqtt, scene_event_topic(_PANEL))), "delivery")

    def delivered() -> bool:
        stored = json.loads(harness.path.read_text())
        return all(event["delivered"] for event in stored["events"].values())

    await _until(delivered, "delivered state")
    await bridge.async_shutdown()
    assert json.loads(before)["events"] != {}
    assert not harness.sidecar.exists()


async def test_corrupt_sidecar_fails_safe_and_retries_with_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    harness = _Harness(tmp_path)
    await _quarantine_poison(harness)
    harness.sidecar.write_text("{corrupt")
    caplog.set_level(logging.WARNING, logger=oq.__name__)

    bridge, _, mqtt = await harness.session("accept")
    await _until(lambda: bool(_published(mqtt, scene_event_topic(_PANEL))), "retried delivery")
    await bridge.async_shutdown()
    assert any("quarantine sidecar" in record.getMessage() for record in caplog.records)


async def test_failed_sidecar_write_never_quarantines(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    harness = _Harness(tmp_path)

    def failing_write(path: Path, state: oq.QuarantineState) -> None:
        del path, state
        raise OSError("read-only filesystem")

    monkeypatch.setattr(scene_bridge_module, "write_sidecar", failing_write)
    for index in range(4):
        bridge, _, mqtt = await harness.session(
            execution_scene=_POISON_SCENE if index == 0 else None
        )
        await _offered(mqtt)
        await asyncio.sleep(0.02)
        await bridge.async_shutdown()
        await harness.clock.advance_ms(30_000)
    # Nothing was persisted, so nothing is ever skipped: the frame is retried.
    assert not harness.sidecar.exists()
    bridge, _, mqtt = await harness.session()
    await _until(lambda: mqtt.poison_offers >= 1, "still offered")
    await bridge.async_shutdown()


async def test_bridge_without_session_signals_never_attributes(tmp_path: Path) -> None:
    harness = _Harness(tmp_path)
    bus = FakeBus(
        [_execution()],
        scoped_devices=[_scene_catalog(_POISON_SCENE), _mode_catalog("away")],
    )
    mqtt = _SessionMqtt("drop")
    await mqtt.publish(_MANIFEST_TOPIC, "{}", retain=True, qos=1)
    bridge = SceneBridge(bus, mqtt, _PANEL, harness.path, harness.clock)
    await bridge.async_start()
    await bus.emit(_execution(_POISON_SCENE, _NOW_MS))
    await _offered(mqtt)
    await asyncio.sleep(0.02)
    await bridge.async_shutdown()
    assert not harness.sidecar.exists()


async def _failure_recorded_now(harness: _Harness) -> None:
    def recorded() -> bool:
        suspect = harness.suspect()
        return suspect is not None and suspect.failures[-1].at_ms == harness.clock.now_ms

    await _until(recorded, "failure at the current time")


async def test_wedge_at_five_second_rebuild_cadence_is_quarantined(tmp_path: Path) -> None:
    # The real wedge rebuilds the session roughly every 5 s. With a bounded
    # failure history the record must still quarantine once a minute elapses.
    harness = _Harness(tmp_path)
    index = 0
    while True:
        bridge, _, mqtt = await harness.session(
            execution_scene=_POISON_SCENE if index == 0 else None
        )
        await _offered(mqtt)
        await _failure_recorded_now(harness)
        await bridge.async_shutdown()
        suspect = harness.suspect()
        assert suspect is not None
        if index * 5_000 >= oq.QUARANTINE_SPAN_MS:
            assert suspect.quarantined_at_ms == _NOW_MS + index * 5_000
            break
        assert suspect.quarantined_at_ms is None
        index += 1
        await harness.clock.advance_ms(5_000)

    bridge, _, mqtt = await harness.session()
    await asyncio.sleep(0.05)
    assert mqtt.poison_offers == 0
    assert _last_status(mqtt, "scene")["reason"] == "outbox_quarantine"
    await bridge.async_shutdown()
