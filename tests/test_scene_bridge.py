"""Safe bidirectional scene/mode bridge tests."""

from __future__ import annotations

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
    assert persisted["office"]["all_off"]["executed_at_ms"] == 1_700_000_000_300
    assert set(persisted["office"]["all_off"]) == {"executed_at_ms", "payload_sha256"}
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
                "kitchen": {
                    "dinner": {
                        "executed_at_ms": 42,
                        "payload_sha256": "a" * 64,
                    }
                }
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
    assert persisted["kitchen"]["dinner"] == {
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
    assert _published(mqtt, scene_result_topic(command_id))[-1][1] == result[1]
    await bridge.async_shutdown()


async def test_completed_command_replays_after_original_command_ttl(tmp_path: Path) -> None:
    bridge, bus, mqtt, clock, _ = await _started(tmp_path)
    command_id = "22222222-2222-4222-8222-222222222222"
    command = _command(command_id, "scene", "all_off")
    await mqtt.inject(scene_command_topic(_PANEL), command)
    await bus.emit(_execution("all_off", 1_234))
    first = _published(mqtt, scene_result_topic(command_id))[-1]

    await clock.advance_ms(20_000)
    await mqtt.inject(scene_command_topic(_PANEL), command)

    assert len(bus.commands) == 1
    assert len(_published(mqtt, scene_result_topic(command_id))) == 2
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
