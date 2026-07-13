"""Safe bidirectional scene/mode bridge tests."""

from __future__ import annotations

import asyncio
import base64
import json
import stat
import struct
from pathlib import Path
from typing import cast
from uuid import UUID

import pytest

from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.ha_control_protocol import (
    MAPPING_VERSION,
    SCHEMA_VERSION,
    encode_json,
    mode_catalog_topic,
    mode_command_topic,
    mode_event_topic,
    mode_result_topic,
    scene_catalog_topic,
    scene_command_topic,
    scene_event_topic,
    scene_result_topic,
    transport_status_topic,
)
from brilliant_mqtt.model import BrilliantDevice, DeviceKind, Variable
from brilliant_mqtt.scene_bridge import SceneBridge
from tests.fakes import FakeBus, FakeClockMs, FakeMqtt

_PANEL = "office"
_DEVICE_ID = "panel-device-id"
_SCENE_PREFIX = "execution_state:scene_execution_handler:scene:"
_NOW_MS = 1_700_000_010_000


def _field_string(field_id: int, value: str) -> bytes:
    encoded = value.encode()
    return b"\x0b" + struct.pack(">hI", field_id, len(encoded)) + encoded


def _field_i64(field_id: int, value: int) -> bytes:
    return b"\x0a" + struct.pack(">hq", field_id, value)


def _blob(*fields: bytes) -> str:
    return base64.b64encode(b"".join((*fields, b"\x00"))).decode()


def _device(
    peripheral_id: str,
    variables: dict[str, Variable],
    *,
    device_id: str = _DEVICE_ID,
) -> BrilliantDevice:
    return BrilliantDevice(
        device_id=device_id,
        peripheral_id=peripheral_id,
        name=peripheral_id,
        kind=DeviceKind.UNKNOWN,
        variables=variables,
    )


def _execution(
    scene_id: str | None = None,
    executed_at_ms: int = 0,
    *,
    mode_id: str | None = None,
    mode_at_ms: int | None = None,
    malformed_scene: bool = False,
) -> BrilliantDevice:
    variables: dict[str, Variable] = {}
    if scene_id is not None:
        name = f"{_SCENE_PREFIX}{scene_id}"
        value = "not-base64" if malformed_scene else _blob(_field_i64(1, executed_at_ms))
        variables[name] = Variable(name, value, timestamp_ms=executed_at_ms + 99)
    if mode_id is not None:
        variables["manual_mode_id"] = Variable("manual_mode_id", mode_id, timestamp_ms=mode_at_ms)
    return _device("execution_peripheral", variables)


def _scene_catalog(*scene_ids: str) -> BrilliantDevice:
    variables = {
        f"scene:{scene_id}": Variable(
            f"scene:{scene_id}",
            _blob(
                _field_string(1, scene_id),
                _field_string(2, scene_id.replace("_", " ").title()),
                _field_string(3, f"icon:{scene_id}"),
            ),
        )
        for scene_id in scene_ids
    }
    return _device("scene_configuration", variables, device_id="configuration_virtual_device")


def _mode_catalog(*mode_ids: str) -> BrilliantDevice:
    variables = {
        f"mode:{mode_id}": Variable(
            f"mode:{mode_id}",
            _blob(_field_string(1, mode_id), _field_string(2, mode_id.title())),
        )
        for mode_id in mode_ids
    }
    return _device("mode_configuration", variables, device_id="configuration_virtual_device")


def _command(command_id: str, kind: str, value: str, *, issued_at_ms: int = _NOW_MS) -> str:
    return encode_json(
        {
            "schema_version": SCHEMA_VERSION,
            "mapping_version": MAPPING_VERSION,
            "command_id": command_id,
            "panel": _PANEL,
            f"{kind}_id": value,
            "issued_at_ms": issued_at_ms,
        }
    )


def _published(mqtt: FakeMqtt, topic: str) -> list[tuple[str, str, bool]]:
    return [item for item in mqtt.published if item[0] == topic]


def _payload(item: tuple[str, str, bool]) -> dict[str, object]:
    return cast(dict[str, object], json.loads(item[1]))


async def _started(
    tmp_path: Path,
    *,
    execution: BrilliantDevice | None = None,
    clock: FakeClockMs | None = None,
    scene_ids: tuple[str, ...] = ("all_off",),
    mode_ids: tuple[str, ...] = ("away",),
) -> tuple[SceneBridge, FakeBus, FakeMqtt, FakeClockMs, Path]:
    execution = execution or _execution()
    bus = FakeBus(
        [execution], scoped_devices=[_scene_catalog(*scene_ids), _mode_catalog(*mode_ids)]
    )
    mqtt = FakeMqtt()
    clock = clock or FakeClockMs(_NOW_MS)
    path = tmp_path / "private" / "scene-watermarks.json"
    bridge = SceneBridge(bus, mqtt, _PANEL, path, clock)
    await bridge.async_start()
    return bridge, bus, mqtt, clock, path


async def test_start_seeds_history_persists_privately_and_publishes_scoped_catalogs(
    tmp_path: Path,
) -> None:
    bridge, bus, mqtt, _, path = await _started(
        tmp_path, execution=_execution("all_off", 1_700_000_000_300)
    )

    assert _published(mqtt, scene_event_topic(_PANEL)) == []
    assert path.exists()
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    persisted = json.loads(path.read_text())
    assert persisted["watermarks"]["office"]["all_off"]["executed_at_ms"] == (1_700_000_000_300)
    assert set(persisted["watermarks"]["office"]["all_off"]) == {
        "executed_at_ms",
        "payload_sha256",
    }
    assert bus.scoped_reads == [
        ("configuration_virtual_device", "scene_configuration"),
        ("configuration_virtual_device", "mode_configuration"),
    ]
    scene_payload = _payload(_published(mqtt, scene_catalog_topic(_PANEL))[-1])
    assert scene_payload["scenes"] == [
        {"display_name": "All Off", "icon": "icon:all_off", "scene_id": "all_off"}
    ]
    assert _published(mqtt, scene_catalog_topic(_PANEL))[-1][2] is True
    assert _published(mqtt, mode_catalog_topic(_PANEL))[-1][2] is True
    assert mqtt.subscriptions == [scene_command_topic(_PANEL), mode_command_topic(_PANEL)]

    await bridge.async_shutdown()


async def test_later_scene_event_publishes_once_and_reconnect_restart_replay_is_suppressed(
    tmp_path: Path,
) -> None:
    bridge, bus, mqtt, clock, path = await _started(tmp_path, execution=_execution("all_off", 100))
    later = _execution("all_off", 200)

    await bus.emit(later)
    await bus.emit(later)
    bus.set_devices([later])
    await bus.fire_reconnect()

    events = _published(mqtt, scene_event_topic(_PANEL))
    assert len(events) == 1
    assert events[0][2] is False
    assert _payload(events[0]) == {
        "deduplication_key": f"{_PANEL}:all_off:200",
        "executed_at_ms": 200,
        "mapping_version": MAPPING_VERSION,
        "panel": _PANEL,
        "scene_id": "all_off",
        "schema_version": SCHEMA_VERSION,
    }
    assert bus.scoped_reads.count(("configuration_virtual_device", "scene_configuration")) == 2

    await bridge.async_shutdown()
    restarted_bus = FakeBus(
        [later], scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")]
    )
    restarted_mqtt = FakeMqtt()
    restarted = SceneBridge(restarted_bus, restarted_mqtt, _PANEL, path, clock)
    await restarted.async_start()

    assert _published(restarted_mqtt, scene_event_topic(_PANEL)) == []
    await restarted.async_shutdown()


async def test_watermark_update_preserves_other_panel_records(tmp_path: Path) -> None:
    path = tmp_path / "scene-watermarks.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "events": {},
                "results": {},
                "pending": {},
                "mode_watermarks": {},
                "watermarks": {
                    "kitchen": {
                        "dinner": {
                            "executed_at_ms": 42,
                            "payload_sha256": "a" * 64,
                        }
                    }
                },
            }
        )
    )
    bus = FakeBus(
        [_execution("all_off", 100)],
        scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
    )
    bridge = SceneBridge(bus, FakeMqtt(), _PANEL, path, FakeClockMs(_NOW_MS))

    await bridge.async_start()
    await bridge.async_shutdown()

    persisted = json.loads(path.read_text())
    assert persisted["watermarks"]["kitchen"]["dinner"] == {
        "executed_at_ms": 42,
        "payload_sha256": "a" * 64,
    }


async def test_valid_commands_write_only_execution_variables_and_wait_for_confirmation(
    tmp_path: Path,
) -> None:
    bridge, bus, mqtt, _, _ = await _started(tmp_path)
    scene_id = "22222222-2222-4222-8222-222222222222"
    mode_id = "33333333-3333-4333-8333-333333333333"

    await mqtt.inject(scene_command_topic(_PANEL), _command(scene_id, "scene", "all_off"))
    await mqtt.inject(mode_command_topic(_PANEL), _command(mode_id, "mode", "away"))

    assert bus.commands == [
        (_DEVICE_ID, "execution_peripheral", [VarSet("last_executed_scene_id", "all_off")]),
        (_DEVICE_ID, "execution_peripheral", [VarSet("manual_mode_id", "away")]),
    ]
    assert _published(mqtt, scene_result_topic(scene_id)) == []
    assert _published(mqtt, mode_result_topic(mode_id)) == []

    await bridge.async_shutdown()


async def test_hung_write_does_not_block_timeout_or_shutdown(tmp_path: Path) -> None:
    class HangingBus(FakeBus):
        def __init__(self) -> None:
            super().__init__(
                [_execution()],
                scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
            )
            self.write_started = asyncio.Event()
            self.write_cancelled = asyncio.Event()

        async def set_variables(
            self, device_id: str, peripheral_id: str, sets: list[VarSet]
        ) -> None:
            await super().set_variables(device_id, peripheral_id, sets)
            self.write_started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                self.write_cancelled.set()
                raise

    bus = HangingBus()
    mqtt = FakeMqtt()
    clock = FakeClockMs(_NOW_MS)
    bridge = SceneBridge(bus, mqtt, _PANEL, tmp_path / "watermarks.json", clock)
    await bridge.async_start()
    command_id = "22222222-2222-4222-8222-222222222222"

    await asyncio.wait_for(
        mqtt.inject(scene_command_topic(_PANEL), _command(command_id, "scene", "all_off")),
        timeout=0.1,
    )
    await asyncio.wait_for(bus.write_started.wait(), timeout=0.1)
    await clock.advance_ms(15_000)

    result = _payload(_published(mqtt, scene_result_topic(command_id))[-1])
    assert result["error"] == "timeout"
    await asyncio.wait_for(bridge.async_shutdown(), timeout=0.1)
    assert bus.write_cancelled.is_set()


async def test_matching_execution_publishes_event_before_accepted_result_and_caches_replay(
    tmp_path: Path,
) -> None:
    bridge, bus, mqtt, _, _ = await _started(tmp_path)
    command_id = "22222222-2222-4222-8222-222222222222"
    command = _command(command_id, "scene", "all_off")
    await mqtt.inject(scene_command_topic(_PANEL), command)

    await bus.emit(_execution("all_off", 1234))

    event_index = next(
        i for i, item in enumerate(mqtt.published) if item[0] == scene_event_topic(_PANEL)
    )
    result_index = next(
        i for i, item in enumerate(mqtt.published) if item[0] == scene_result_topic(command_id)
    )
    assert event_index < result_index
    result = _published(mqtt, scene_result_topic(command_id))[-1]
    assert _payload(result) == {
        "accepted": True,
        "command_id": command_id,
        "mapping_version": MAPPING_VERSION,
        "panel": _PANEL,
        "scene_id": "all_off",
        "schema_version": SCHEMA_VERSION,
        "timestamp_ms": _NOW_MS,
    }
    await mqtt.inject(scene_command_topic(_PANEL), command)

    assert len(bus.commands) == 1
    assert len(_published(mqtt, scene_result_topic(command_id))) == 2
    assert _published(mqtt, scene_result_topic(command_id))[-1][1] == result[1]
    await bridge.async_shutdown()


async def test_completed_command_does_not_replay_after_original_command_ttl(tmp_path: Path) -> None:
    bridge, bus, mqtt, clock, _ = await _started(tmp_path)
    command_id = "22222222-2222-4222-8222-222222222222"
    command = _command(command_id, "scene", "all_off")
    await mqtt.inject(scene_command_topic(_PANEL), command)
    await bus.emit(_execution("all_off", 1_234))
    first = _published(mqtt, scene_result_topic(command_id))[-1]

    await clock.advance_ms(20_000)
    await mqtt.inject(scene_command_topic(_PANEL), command)

    assert len(bus.commands) == 1
    assert len(_published(mqtt, scene_result_topic(command_id))) == 1
    assert _published(mqtt, scene_result_topic(command_id))[-1][1] == first[1]
    await bridge.async_shutdown()


async def test_matching_mode_execution_confirms_only_mode_pending_request(tmp_path: Path) -> None:
    bridge, bus, mqtt, _, _ = await _started(tmp_path)
    scene_command_id = "22222222-2222-4222-8222-222222222222"
    mode_command_id = "33333333-3333-4333-8333-333333333333"
    await mqtt.inject(scene_command_topic(_PANEL), _command(scene_command_id, "scene", "all_off"))
    await mqtt.inject(mode_command_topic(_PANEL), _command(mode_command_id, "mode", "away"))

    await bus.emit(_execution(mode_id="away", mode_at_ms=222))

    assert len(_published(mqtt, mode_event_topic(_PANEL))) == 1
    assert len(_published(mqtt, mode_result_topic(mode_command_id))) == 1
    assert _published(mqtt, scene_result_topic(scene_command_id)) == []
    await bridge.async_shutdown()


async def test_timeout_is_exactly_fifteen_seconds_and_shutdown_cancels_remaining_tasks(
    tmp_path: Path,
) -> None:
    bridge, bus, mqtt, clock, _ = await _started(tmp_path)
    command_id = "22222222-2222-4222-8222-222222222222"
    await mqtt.inject(scene_command_topic(_PANEL), _command(command_id, "scene", "all_off"))

    await clock.advance_ms(14_999)
    assert _published(mqtt, scene_result_topic(command_id)) == []
    await clock.advance_ms(1)
    timeout = _payload(_published(mqtt, scene_result_topic(command_id))[-1])
    assert timeout["accepted"] is False
    assert timeout["error"] == "timeout"

    second_id = "44444444-4444-4444-8444-444444444444"
    await mqtt.inject(scene_command_topic(_PANEL), _command(second_id, "scene", "all_off"))
    await bridge.async_shutdown()
    await clock.advance_ms(20_000)
    assert _published(mqtt, scene_result_topic(second_id)) == []
    assert mqtt.unsubscriptions == [scene_command_topic(_PANEL), mode_command_topic(_PANEL)]


@pytest.mark.parametrize("case", ["expired", "unknown", "retained", "duplicate_pending"])
async def test_unsafe_or_duplicate_scene_commands_never_write(tmp_path: Path, case: str) -> None:
    bridge, bus, mqtt, _, _ = await _started(tmp_path)
    command_id = "22222222-2222-4222-8222-222222222222"
    issued_at_ms = _NOW_MS - 15_001 if case == "expired" else _NOW_MS
    scene_id = "unknown" if case == "unknown" else "all_off"
    command = _command(command_id, "scene", scene_id, issued_at_ms=issued_at_ms)

    await mqtt.inject(scene_command_topic(_PANEL), command, retained=case == "retained")
    if case == "duplicate_pending":
        await mqtt.inject(scene_command_topic(_PANEL), command)

    assert len(bus.commands) == (1 if case == "duplicate_pending" else 0)
    await bridge.async_shutdown()


async def test_wrong_topic_panel_and_malformed_payload_never_write(tmp_path: Path) -> None:
    bridge, bus, mqtt, _, _ = await _started(tmp_path)
    command_id = "22222222-2222-4222-8222-222222222222"
    await mqtt.inject("unrelated", _command(command_id, "scene", "all_off"))
    mismatched = json.loads(_command(command_id, "scene", "all_off"))
    mismatched["panel"] = "kitchen"
    await mqtt.inject(scene_command_topic(_PANEL), encode_json(mismatched))
    await mqtt.inject(scene_command_topic(_PANEL), "not-json")

    assert bus.commands == []
    await bridge.async_shutdown()


async def test_write_failure_is_sanitized_and_cached(tmp_path: Path) -> None:
    class FailingBus(FakeBus):
        async def set_variables(
            self, device_id: str, peripheral_id: str, sets: list[VarSet]
        ) -> None:
            await super().set_variables(device_id, peripheral_id, sets)
            raise RuntimeError("token=secret\nunsafe")

    execution = _execution()
    bus = FailingBus([execution], scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")])
    mqtt = FakeMqtt()
    clock = FakeClockMs(_NOW_MS)
    bridge = SceneBridge(bus, mqtt, _PANEL, tmp_path / "watermarks.json", clock)
    await bridge.async_start()
    command_id = "22222222-2222-4222-8222-222222222222"
    command = _command(command_id, "scene", "all_off")

    await mqtt.inject(scene_command_topic(_PANEL), command)
    first = _published(mqtt, scene_result_topic(command_id))[-1]
    await mqtt.inject(scene_command_topic(_PANEL), command)

    assert _payload(first)["error"] == "write_failed"
    assert "secret" not in first[1]
    assert len(bus.commands) == 1
    assert _published(mqtt, scene_result_topic(command_id))[-1][1] == first[1]
    await bridge.async_shutdown()


async def test_malformed_execution_degrades_status_without_breaking_fanout(tmp_path: Path) -> None:
    bridge, bus, mqtt, _, _ = await _started(tmp_path)
    reached: list[str] = []

    async def other_consumer(device: BrilliantDevice) -> None:
        reached.append(device.peripheral_id)

    bus.on_change(other_consumer)
    await bus.emit(_execution("all_off", malformed_scene=True))

    assert reached == ["execution_peripheral"]
    status = _payload(_published(mqtt, transport_status_topic("scene", _PANEL))[-1])
    assert status["available"] is False
    assert status["reason"] == "malformed_data"
    await bus.emit(_execution("all_off", 500))
    assert len(_published(mqtt, scene_event_topic(_PANEL))) == 1
    await bridge.async_shutdown()


async def test_mixed_initial_execution_seeds_valid_history_while_degrading_status(
    tmp_path: Path,
) -> None:
    valid = _execution("all_off", 500)
    malformed = _execution("broken", malformed_scene=True)
    mixed = _device("execution_peripheral", {**valid.variables, **malformed.variables})
    bridge, bus, mqtt, _, path = await _started(tmp_path, execution=mixed)

    assert _published(mqtt, scene_event_topic(_PANEL)) == []
    assert json.loads(path.read_text())["watermarks"][_PANEL]["all_off"]["executed_at_ms"] == 500
    status = _payload(_published(mqtt, transport_status_topic("scene", _PANEL))[-1])
    assert status["available"] is False

    await bus.emit(valid)
    assert _published(mqtt, scene_event_topic(_PANEL)) == []
    await bridge.async_shutdown()


async def test_reconnect_malformed_execution_stays_degraded_after_valid_catalog(
    tmp_path: Path,
) -> None:
    bridge, bus, mqtt, _, _ = await _started(tmp_path)
    bus.set_devices([_execution("all_off", malformed_scene=True)])

    await bus.fire_reconnect()

    status = _payload(_published(mqtt, transport_status_topic("scene", _PANEL))[-1])
    assert status["available"] is False
    assert status["reason"] == "malformed_data"
    await bridge.async_shutdown()


async def test_missing_execution_is_unavailable_and_reconnect_clears_stale_route(
    tmp_path: Path,
) -> None:
    bridge, bus, mqtt, _, _ = await _started(tmp_path)
    bus.set_devices([])

    await bus.fire_reconnect()

    status = _payload(_published(mqtt, transport_status_topic("scene", _PANEL))[-1])
    assert status["available"] is False
    assert status["reason"] == "execution_unavailable"
    command_id = "22222222-2222-4222-8222-222222222222"
    await mqtt.inject(scene_command_topic(_PANEL), _command(command_id, "scene", "all_off"))
    assert bus.commands == []
    assert _payload(_published(mqtt, scene_result_topic(command_id))[-1])["error"] == (
        "execution_unavailable"
    )
    await bridge.async_shutdown()


async def test_failed_terminal_result_publish_retries_until_delivered(tmp_path: Path) -> None:
    command_id = "22222222-2222-4222-8222-222222222222"

    class OneResultFailsMqtt(FakeMqtt):
        def __init__(self) -> None:
            super().__init__()
            self.failed = False

        async def publish(self, topic: str, payload: str, retain: bool = False) -> None:
            if topic == scene_result_topic(command_id) and not self.failed:
                self.failed = True
                raise RuntimeError("broker unavailable")
            await super().publish(topic, payload, retain)

    bus = FakeBus(
        [_execution()],
        scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
    )
    mqtt = OneResultFailsMqtt()
    clock = FakeClockMs(_NOW_MS)
    bridge = SceneBridge(bus, mqtt, _PANEL, tmp_path / "watermarks.json", clock)
    await bridge.async_start()
    await mqtt.inject(scene_command_topic(_PANEL), _command(command_id, "scene", "all_off"))
    await bus.emit(_execution("all_off", 500))
    assert _published(mqtt, scene_result_topic(command_id)) == []

    await clock.advance_ms(1_000)

    results = _published(mqtt, scene_result_topic(command_id))
    assert len(results) == 1
    assert _payload(results[0])["accepted"] is True
    await bridge.async_shutdown()


async def test_failed_scene_catalog_read_does_not_prevent_scoped_mode_read(tmp_path: Path) -> None:
    class OneReadFailsBus(FakeBus):
        async def get_peripheral(
            self, device_id: str, peripheral_id: str
        ) -> BrilliantDevice | None:
            if peripheral_id == "scene_configuration":
                self.scoped_reads.append((device_id, peripheral_id))
                raise RuntimeError("scene read failed")
            return await super().get_peripheral(device_id, peripheral_id)

    bus = OneReadFailsBus([_execution()], scoped_devices=[_mode_catalog("away")])
    mqtt = FakeMqtt()
    bridge = SceneBridge(bus, mqtt, _PANEL, tmp_path / "watermarks.json", FakeClockMs(_NOW_MS))

    await bridge.async_start()

    assert bus.scoped_reads == [
        ("configuration_virtual_device", "scene_configuration"),
        ("configuration_virtual_device", "mode_configuration"),
    ]
    assert len(_published(mqtt, mode_catalog_topic(_PANEL))) == 1
    await bridge.async_shutdown()


async def test_reconnect_runs_existing_forward_reconcile_and_scene_reconcile(
    tmp_path: Path,
) -> None:
    bus = FakeBus([_execution()], scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")])
    forward_reconciles: list[str] = []

    async def existing_panel_and_mesh_reconcile() -> None:
        forward_reconciles.append("panel-and-mesh")

    bus.on_reconnect(existing_panel_and_mesh_reconcile)
    bridge = SceneBridge(
        bus, FakeMqtt(), _PANEL, tmp_path / "watermarks.json", FakeClockMs(_NOW_MS)
    )
    await bridge.async_start()

    await bus.fire_reconnect()

    assert forward_reconciles == ["panel-and-mesh"]
    assert bus.scoped_reads.count(("configuration_virtual_device", "scene_configuration")) == 2
    await bridge.async_shutdown()


async def test_start_and_shutdown_are_idempotent_and_reject_callbacks_after_stop(
    tmp_path: Path,
) -> None:
    bridge, bus, mqtt, _, _ = await _started(tmp_path)
    await bridge.async_start()
    assert mqtt.subscriptions == [scene_command_topic(_PANEL), mode_command_topic(_PANEL)]

    await bridge.async_shutdown()
    await bridge.async_shutdown()
    before = list(mqtt.published)
    await bus.emit(_execution("all_off", 999))
    command_id = str(UUID("22222222-2222-4222-8222-222222222222"))
    await mqtt.inject(scene_command_topic(_PANEL), _command(command_id, "scene", "all_off"))

    assert mqtt.published == before
    assert mqtt.unsubscriptions == [scene_command_topic(_PANEL), mode_command_topic(_PANEL)]


async def test_undelivered_accepted_result_survives_process_restart(tmp_path: Path) -> None:
    command_id = "22222222-2222-4222-8222-222222222222"

    class ResultOfflineMqtt(FakeMqtt):
        async def publish(self, topic: str, payload: str, retain: bool = False) -> None:
            if topic == scene_result_topic(command_id):
                raise RuntimeError("offline")
            await super().publish(topic, payload, retain)

    path = tmp_path / "state.json"
    bus = FakeBus([_execution()], scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")])
    mqtt = ResultOfflineMqtt()
    clock = FakeClockMs(_NOW_MS)
    bridge = SceneBridge(bus, mqtt, _PANEL, path, clock)
    await bridge.async_start()
    command = _command(command_id, "scene", "all_off")
    await mqtt.inject(scene_command_topic(_PANEL), command)
    await bus.emit(_execution("all_off", 500))
    await asyncio.sleep(0)
    await bridge.async_shutdown()

    stored = json.loads(path.read_text())
    outcome = stored["results"][f"scene:{command_id}"]
    assert outcome["delivered"] is False
    assert outcome["fingerprint"]
    assert outcome["payload"]
    assert outcome["expires_at_ms"] > _NOW_MS

    restarted_bus = FakeBus(
        [_execution("all_off", 500)],
        scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
    )
    restarted_mqtt = FakeMqtt()
    restarted = SceneBridge(restarted_bus, restarted_mqtt, _PANEL, path, clock)
    await restarted.async_start()
    await asyncio.sleep(0)

    results = _published(restarted_mqtt, scene_result_topic(command_id))
    assert len(results) == 1
    assert results[0][1] == outcome["payload"]
    await restarted.async_shutdown()


async def test_event_outbox_survives_restart_and_gates_accepted_result(tmp_path: Path) -> None:
    command_id = "22222222-2222-4222-8222-222222222222"

    class FirstEventOfflineMqtt(FakeMqtt):
        def __init__(self) -> None:
            super().__init__()
            self.fail_events = True

        async def publish(self, topic: str, payload: str, retain: bool = False) -> None:
            if topic == scene_event_topic(_PANEL) and self.fail_events:
                raise RuntimeError("offline")
            await super().publish(topic, payload, retain)

    path = tmp_path / "state.json"
    bus = FakeBus([_execution()], scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")])
    mqtt = FirstEventOfflineMqtt()
    clock = FakeClockMs(_NOW_MS)
    bridge = SceneBridge(bus, mqtt, _PANEL, path, clock)
    await bridge.async_start()
    await mqtt.inject(scene_command_topic(_PANEL), _command(command_id, "scene", "all_off"))
    await bus.emit(_execution("all_off", 500))
    await asyncio.sleep(0)

    assert _published(mqtt, scene_result_topic(command_id)) == []
    stored = json.loads(path.read_text())
    assert any(not event["delivered"] for event in stored["events"].values())
    await bridge.async_shutdown()

    restarted_bus = FakeBus(
        [_execution("all_off", 500)],
        scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
    )
    restarted_mqtt = FakeMqtt()
    restarted = SceneBridge(restarted_bus, restarted_mqtt, _PANEL, path, clock)
    await restarted.async_start()
    await asyncio.sleep(0)
    event_index = next(
        index
        for index, item in enumerate(restarted_mqtt.published)
        if item[0] == scene_event_topic(_PANEL)
    )
    result_index = next(
        index
        for index, item in enumerate(restarted_mqtt.published)
        if item[0] == scene_result_topic(command_id)
    )
    assert event_index < result_index
    await restarted.async_shutdown()


async def test_corrupt_state_seeds_baseline_without_history_and_normalizes_permissions(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.json"
    path.write_text("{corrupt")
    path.chmod(0o644)
    bus = FakeBus(
        [_execution("all_off", 500)],
        scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
    )
    mqtt = FakeMqtt()
    bridge = SceneBridge(bus, mqtt, _PANEL, path, FakeClockMs(_NOW_MS))

    await bridge.async_start()

    assert _published(mqtt, scene_event_topic(_PANEL)) == []
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    stored = json.loads(path.read_text())
    assert stored["version"] == 1
    assert stored["watermarks"][_PANEL]["all_off"]["executed_at_ms"] == 500
    await bridge.async_shutdown()


async def test_corrupt_state_without_snapshot_suppresses_first_observed_record(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "watermarks": {
                    _PANEL: {
                        "all_off": {
                            "executed_at_ms": "bad",
                            "payload_sha256": "not-a-hash",
                        }
                    }
                },
                "events": {},
                "results": {},
                "pending": {},
                "mode_watermarks": {},
            }
        )
    )
    bus = FakeBus([], scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")])
    mqtt = FakeMqtt()
    bridge = SceneBridge(bus, mqtt, _PANEL, path, FakeClockMs(_NOW_MS))
    await bridge.async_start()

    await bus.emit(_execution("all_off", 500))
    await asyncio.sleep(0)
    assert _published(mqtt, scene_event_topic(_PANEL)) == []
    await bus.emit(_execution("all_off", 600))
    await asyncio.sleep(0)
    assert len(_published(mqtt, scene_event_topic(_PANEL))) == 1
    await bridge.async_shutdown()


async def test_persistence_failure_refuses_commands_and_degrades_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail_replace(_source: str, _destination: Path) -> None:
        raise OSError("disk full")

    monkeypatch.setattr("brilliant_mqtt.scene_bridge.os.replace", fail_replace)
    bridge, bus, mqtt, _, _ = await _started(tmp_path, execution=_execution("all_off", 500))
    command_id = "22222222-2222-4222-8222-222222222222"

    await mqtt.inject(scene_command_topic(_PANEL), _command(command_id, "scene", "all_off"))

    assert bus.commands == []
    status = _payload(_published(mqtt, transport_status_topic("scene", _PANEL))[-1])
    assert status["available"] is False
    assert status["reason"] == "state_untrusted"
    await bridge.async_shutdown()


async def test_completed_id_reuse_validates_context_and_fingerprint_before_replay(
    tmp_path: Path,
) -> None:
    bridge, bus, mqtt, clock, _ = await _started(tmp_path, scene_ids=("all_off", "all_on"))
    command_id = "22222222-2222-4222-8222-222222222222"
    original = _command(command_id, "scene", "all_off")
    await mqtt.inject(scene_command_topic(_PANEL), original)
    await bus.emit(_execution("all_off", 500))
    await asyncio.sleep(0)
    original_results = len(_published(mqtt, scene_result_topic(command_id)))

    await mqtt.inject(
        scene_command_topic(_PANEL),
        _command(command_id, "scene", "all_on"),
    )
    await mqtt.inject(scene_command_topic(_PANEL), original, retained=True)
    await clock.advance_ms(15_001)
    await mqtt.inject(scene_command_topic(_PANEL), original)

    assert len(bus.commands) == 1
    assert len(_published(mqtt, scene_result_topic(command_id))) == original_results
    await bridge.async_shutdown()


async def test_hung_reconcile_and_publish_callbacks_do_not_block_shutdown(tmp_path: Path) -> None:
    class HangingBus(FakeBus):
        def __init__(self) -> None:
            super().__init__(
                [_execution()],
                scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
            )
            self.hang_reads = False
            self.read_started = asyncio.Event()

        async def get_all(self) -> list[BrilliantDevice]:
            if self.hang_reads:
                self.read_started.set()
                await asyncio.Future()
            return await super().get_all()

    class HangingEventMqtt(FakeMqtt):
        def __init__(self) -> None:
            super().__init__()
            self.hang_events = False
            self.publish_started = asyncio.Event()

        async def publish(self, topic: str, payload: str, retain: bool = False) -> None:
            if topic == scene_event_topic(_PANEL) and self.hang_events:
                self.publish_started.set()
                await asyncio.Future()
            await super().publish(topic, payload, retain)

    bus = HangingBus()
    mqtt = HangingEventMqtt()
    bridge = SceneBridge(bus, mqtt, _PANEL, tmp_path / "state.json", FakeClockMs(_NOW_MS))
    await bridge.async_start()
    bus.hang_reads = True
    mqtt.hang_events = True

    await asyncio.wait_for(bus.fire_reconnect(), timeout=0.1)
    await asyncio.wait_for(bus.read_started.wait(), timeout=0.1)
    await asyncio.wait_for(bus.emit(_execution("all_off", 500)), timeout=0.1)
    await asyncio.sleep(0)

    await asyncio.wait_for(bridge.async_shutdown(), timeout=0.1)
    before = list(mqtt.published)
    await asyncio.sleep(0)
    assert mqtt.published == before


async def test_hung_event_publish_is_cancelled_before_shutdown_returns(tmp_path: Path) -> None:
    class HangingEventMqtt(FakeMqtt):
        def __init__(self) -> None:
            super().__init__()
            self.publish_started = asyncio.Event()
            self.publish_cancelled = asyncio.Event()

        async def publish(self, topic: str, payload: str, retain: bool = False) -> None:
            if topic == scene_event_topic(_PANEL):
                self.publish_started.set()
                try:
                    await asyncio.Future()
                except asyncio.CancelledError:
                    self.publish_cancelled.set()
                    raise
            await super().publish(topic, payload, retain)

    bus = FakeBus([_execution()], scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")])
    mqtt = HangingEventMqtt()
    bridge = SceneBridge(bus, mqtt, _PANEL, tmp_path / "state.json", FakeClockMs(_NOW_MS))
    await bridge.async_start()

    await asyncio.wait_for(bus.emit(_execution("all_off", 500)), timeout=0.1)
    await asyncio.wait_for(mqtt.publish_started.wait(), timeout=0.1)
    await asyncio.wait_for(bridge.async_shutdown(), timeout=0.1)

    assert mqtt.publish_cancelled.is_set()
    assert _published(mqtt, scene_event_topic(_PANEL)) == []


async def test_valid_existing_state_permissions_are_normalized_on_load(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "watermarks": {},
                "mode_watermarks": {},
                "events": {},
                "results": {},
                "pending": {},
            }
        )
    )
    path.chmod(0o644)
    bridge = SceneBridge(
        FakeBus([], scoped_devices=[_scene_catalog(), _mode_catalog()]),
        FakeMqtt(),
        _PANEL,
        path,
        FakeClockMs(_NOW_MS),
    )

    await bridge.async_start()

    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    await bridge.async_shutdown()


async def test_unreadable_state_requires_and_seeds_a_fresh_baseline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "state.json"
    path.write_text("unreadable")
    original_read_text = Path.read_text

    def fail_target_read(
        target: Path, encoding: str | None = None, errors: str | None = None
    ) -> str:
        if target == path:
            raise PermissionError("denied")
        return original_read_text(target, encoding, errors)

    monkeypatch.setattr(Path, "read_text", fail_target_read)
    bus = FakeBus(
        [_execution("all_off", 500)],
        scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
    )
    mqtt = FakeMqtt()
    bridge = SceneBridge(bus, mqtt, _PANEL, path, FakeClockMs(_NOW_MS))

    await bridge.async_start()

    assert _published(mqtt, scene_event_topic(_PANEL)) == []
    monkeypatch.setattr(Path, "read_text", original_read_text)
    assert json.loads(path.read_text())["watermarks"][_PANEL]["all_off"]["executed_at_ms"] == 500
    await bridge.async_shutdown()


async def test_result_capacity_never_discards_undelivered_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("brilliant_mqtt.scene_bridge._RESULT_CACHE_LIMIT", 1)
    first_id = "22222222-2222-4222-8222-222222222222"
    second_id = "44444444-4444-4444-8444-444444444444"

    class ResultOfflineMqtt(FakeMqtt):
        async def publish(self, topic: str, payload: str, retain: bool = False) -> None:
            if "/scene/result/" in topic:
                raise RuntimeError("offline")
            await super().publish(topic, payload, retain)

    bus = FakeBus([_execution()], scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")])
    mqtt = ResultOfflineMqtt()
    bridge = SceneBridge(bus, mqtt, _PANEL, tmp_path / "state.json", FakeClockMs(_NOW_MS))
    await bridge.async_start()
    await mqtt.inject(scene_command_topic(_PANEL), _command(first_id, "scene", "all_off"))
    await bus.emit(_execution("all_off", 500))
    await mqtt.inject(scene_command_topic(_PANEL), _command(second_id, "scene", "all_off"))

    assert len(bus.commands) == 1
    stored = json.loads((tmp_path / "state.json").read_text())
    assert list(stored["results"]) == [f"scene:{first_id}"]
    status = _payload(_published(mqtt, transport_status_topic("scene", _PANEL))[-1])
    assert status["reason"] == "state_capacity"
    await bridge.async_shutdown()


async def test_event_capacity_never_discards_undelivered_event(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("brilliant_mqtt.scene_bridge._EVENT_OUTBOX_LIMIT", 1)

    class EventOfflineMqtt(FakeMqtt):
        async def publish(self, topic: str, payload: str, retain: bool = False) -> None:
            if topic == scene_event_topic(_PANEL):
                raise RuntimeError("offline")
            await super().publish(topic, payload, retain)

    path = tmp_path / "state.json"
    bus = FakeBus(
        [_execution()],
        scoped_devices=[_scene_catalog("all_off", "all_on"), _mode_catalog("away")],
    )
    mqtt = EventOfflineMqtt()
    bridge = SceneBridge(bus, mqtt, _PANEL, path, FakeClockMs(_NOW_MS))
    await bridge.async_start()
    await bus.emit(_execution("all_off", 500))
    await bus.emit(_execution("all_on", 600))

    stored = json.loads(path.read_text())
    assert len(stored["events"]) == 1
    assert "all_on" not in stored["watermarks"][_PANEL]
    status = _payload(_published(mqtt, transport_status_topic("scene", _PANEL))[-1])
    assert status["reason"] == "state_capacity"
    await bridge.async_shutdown()


async def test_inflight_command_is_durable_before_write_and_never_rewrites_after_restart(
    tmp_path: Path,
) -> None:
    command_id = "22222222-2222-4222-8222-222222222222"
    command = _command(command_id, "scene", "all_off")
    path = tmp_path / "state.json"
    first_bus = FakeBus(
        [_execution()], scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")]
    )
    first_mqtt = FakeMqtt()
    first = SceneBridge(first_bus, first_mqtt, _PANEL, path, FakeClockMs(_NOW_MS))
    await first.async_start()

    await first_mqtt.inject(scene_command_topic(_PANEL), command)
    assert len(first_bus.commands) == 1
    await first.async_shutdown()
    assert f"scene:{command_id}" in json.loads(path.read_text())["pending"]

    second_bus = FakeBus(
        [_execution()], scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")]
    )
    second_mqtt = FakeMqtt()
    second = SceneBridge(second_bus, second_mqtt, _PANEL, path, FakeClockMs(_NOW_MS))
    await second.async_start()
    await second_mqtt.inject(scene_command_topic(_PANEL), command)

    assert second_bus.commands == []
    await second_bus.emit(_execution("all_off", 500))
    assert _payload(_published(second_mqtt, scene_result_topic(command_id))[-1])["accepted"] is True
    await second.async_shutdown()


async def test_partial_persisted_result_payload_marks_entire_state_untrusted(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.json"
    command_id = "22222222-2222-4222-8222-222222222222"
    state = {
        "version": 1,
        "watermarks": {},
        "mode_watermarks": {},
        "events": {},
        "pending": {},
        "results": {
            f"scene:{command_id}": {
                "kind": "scene",
                "command_id": command_id,
                "fingerprint": "a" * 64,
                "topic": scene_result_topic(command_id),
                "payload": json.dumps({"command_id": command_id}),
                "delivered": False,
                "expires_at_ms": _NOW_MS + 1_000,
                "event_key": None,
            }
        },
    }
    path.write_text(json.dumps(state))
    mqtt = FakeMqtt()
    bridge = SceneBridge(
        FakeBus([], scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")]),
        mqtt,
        _PANEL,
        path,
        FakeClockMs(_NOW_MS),
    )

    await bridge.async_start()

    assert _published(mqtt, scene_result_topic(command_id)) == []
    status = _payload(_published(mqtt, transport_status_topic("scene", _PANEL))[-1])
    assert status["reason"] == "state_untrusted"
    await bridge.async_shutdown()


async def test_mode_watermark_survives_restart_without_initial_execution_snapshot(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.json"
    first = SceneBridge(
        FakeBus(
            [_execution(mode_id="away", mode_at_ms=500)],
            scoped_devices=[_scene_catalog(), _mode_catalog("away")],
        ),
        FakeMqtt(),
        _PANEL,
        path,
        FakeClockMs(_NOW_MS),
    )
    await first.async_start()
    await first.async_shutdown()

    second_bus = FakeBus([], scoped_devices=[_scene_catalog(), _mode_catalog("away")])
    second_mqtt = FakeMqtt()
    second = SceneBridge(second_bus, second_mqtt, _PANEL, path, FakeClockMs(_NOW_MS))
    await second.async_start()
    await second_bus.emit(_execution(mode_id="away", mode_at_ms=500))

    assert _published(second_mqtt, mode_event_topic(_PANEL)) == []
    await second.async_shutdown()


async def test_shutdown_cancels_inflight_start_and_unsubscribes_started_topics(
    tmp_path: Path,
) -> None:
    class PausedStartBus(FakeBus):
        def __init__(self) -> None:
            super().__init__(
                [_execution()],
                scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
            )
            self.read_started = asyncio.Event()

        async def get_all(self) -> list[BrilliantDevice]:
            self.read_started.set()
            await asyncio.Future()
            return []

    bus = PausedStartBus()
    mqtt = FakeMqtt()
    bridge = SceneBridge(bus, mqtt, _PANEL, tmp_path / "state.json", FakeClockMs(_NOW_MS))
    start_task = asyncio.create_task(bridge.async_start())
    await asyncio.wait_for(bus.read_started.wait(), timeout=0.1)

    await asyncio.wait_for(bridge.async_shutdown(), timeout=0.1)
    await asyncio.gather(start_task, return_exceptions=True)

    assert mqtt.subscriptions == []
    assert mqtt.unsubscriptions == [scene_command_topic(_PANEL), mode_command_topic(_PANEL)]


async def test_shutdown_abandons_write_that_delays_cancellation(tmp_path: Path) -> None:
    class DelayedCancellationBus(FakeBus):
        def __init__(self) -> None:
            super().__init__(
                [_execution()],
                scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
            )
            self.started = asyncio.Event()
            self.release = asyncio.Event()

        async def set_variables(
            self, device_id: str, peripheral_id: str, sets: list[VarSet]
        ) -> None:
            await super().set_variables(device_id, peripheral_id, sets)
            self.started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                await self.release.wait()

    bus = DelayedCancellationBus()
    mqtt = FakeMqtt()
    bridge = SceneBridge(bus, mqtt, _PANEL, tmp_path / "state.json", FakeClockMs(_NOW_MS))
    await bridge.async_start()
    await mqtt.inject(
        scene_command_topic(_PANEL),
        _command("22222222-2222-4222-8222-222222222222", "scene", "all_off"),
    )
    await asyncio.wait_for(bus.started.wait(), timeout=0.1)

    await asyncio.wait_for(bridge.async_shutdown(), timeout=0.1)
    bus.release.set()
    await asyncio.sleep(0)

    assert _published(mqtt, scene_result_topic("22222222-2222-4222-8222-222222222222")) == []
