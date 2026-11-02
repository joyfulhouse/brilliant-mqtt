"""On-panel operator recovery CLI for a quarantined outbox record (#113)."""

from __future__ import annotations

import asyncio
import io
import json
import os
from dataclasses import replace
from pathlib import Path

import pytest

from brilliant_mqtt import outbox_admin
from brilliant_mqtt import outbox_quarantine as oq
from brilliant_mqtt.ha_control_protocol import (
    scene_command_topic,
    scene_event_topic,
    scene_result_topic,
)
from brilliant_mqtt.heartbeat import PhaseRecord, current_boot_id, process_generation
from brilliant_mqtt.scene_state import load_state
from tests.test_scene_bridge import _PANEL, _command, _execution, _published, _wait_for_bus_commands
from tests.test_scene_outbox_quarantine import (
    _COMMAND_ID,
    _POISON_SCENE,
    _failures_reach,
    _Harness,
    _last_status,
    _offered,
    _until,
)

_RESULT_KEY = oq.result_record_key("scene", _COMMAND_ID)


async def _quarantined_with_dependent(tmp_path: Path) -> _Harness:
    """A quarantined poison event plus the accepted command result it gates."""
    harness = _Harness(tmp_path)
    bridge, bus, mqtt = await harness.session()
    command = _command(_COMMAND_ID, "scene", _POISON_SCENE, issued_at_ms=harness.clock.now_ms)
    await mqtt.inject(scene_command_topic(_PANEL), command)
    await _wait_for_bus_commands(bus, 1)
    await bus.emit(_execution(_POISON_SCENE, harness.clock.now_ms))
    await _offered(mqtt)
    await _failures_reach(harness, 1)
    await bridge.async_shutdown()
    for count in (2, 3):
        await harness.clock.advance_ms(30_000)
        bridge, _, mqtt = await harness.session()
        await _offered(mqtt)
        await _failures_reach(harness, count)
        await bridge.async_shutdown()
    suspect = harness.suspect()
    assert suspect is not None and suspect.quarantined_at_ms is not None
    return harness


def _event_key(harness: _Harness) -> tuple[str, str]:
    stored = json.loads(harness.path.read_text())
    (key, event) = next(iter(stored["events"].items()))
    return oq.event_record_key(key), oq.payload_sha256(event["payload"])


def _result_sha(harness: _Harness) -> str:
    stored = json.loads(harness.path.read_text())
    return oq.payload_sha256(stored["results"][f"scene:{_COMMAND_ID}"]["payload"])


def _env(harness: _Harness, tmp_path: Path) -> dict[str, str]:
    return {
        "BRILLIANT_PANEL": _PANEL,
        "MQTT_HOST": "broker.invalid",
        "MQTT_USERNAME": "bridge",
        "MQTT_PASSWORD": "unused",
        "SCENE_WATERMARK_FILE": str(harness.path),
        "BUS_PHASE_FILE": str(tmp_path / "run" / "bus-phase"),
    }


def _run(harness: _Harness, tmp_path: Path, *argv: str) -> tuple[int, str]:
    out = io.StringIO()
    status = outbox_admin.main(list(argv), env=_env(harness, tmp_path), out=out)
    return status, out.getvalue()


def _audit(harness: _Harness) -> list[dict[str, object]]:
    path = outbox_admin.audit_log_path(harness.path)
    return [json.loads(line) for line in path.read_text().splitlines()]


async def test_list_shows_key_kind_size_hash_and_status_without_payloads(
    tmp_path: Path,
) -> None:
    harness = await _quarantined_with_dependent(tmp_path)
    event_key, event_sha = _event_key(harness)

    status, output = _run(harness, tmp_path, "list")

    assert status == 0
    lines = output.splitlines()
    assert lines[0] == "key\tkind\tbytes\tsha256\tstatus\tfailures"
    rows = {line.split("\t")[0]: line.split("\t") for line in lines[1:]}
    stored = json.loads(harness.path.read_text())
    event_payload = next(iter(stored["events"].values()))["payload"]
    assert rows[event_key] == [
        event_key,
        "event",
        str(len(event_payload.encode())),
        event_sha,
        "quarantined",
        "3",
    ]
    assert rows[_RESULT_KEY][1] == "result"
    assert rows[_RESULT_KEY][4] == "pending"
    assert event_payload not in output
    assert _audit(harness)[-1]["action"] == "list"


async def test_hash_mismatch_is_refused_without_change(tmp_path: Path) -> None:
    harness = await _quarantined_with_dependent(tmp_path)
    event_key, _ = _event_key(harness)
    state_before = harness.path.read_bytes()
    sidecar_before = harness.sidecar.read_bytes()

    for action in ("release", "discard"):
        status, _ = _run(harness, tmp_path, action, event_key, "0" * 64)
        assert status == 1

    assert harness.path.read_bytes() == state_before
    assert harness.sidecar.read_bytes() == sidecar_before
    assert not outbox_admin.audit_log_path(harness.path).exists()


async def test_running_agent_is_refused_and_stale_marker_is_not(tmp_path: Path) -> None:
    harness = await _quarantined_with_dependent(tmp_path)
    event_key, event_sha = _event_key(harness)
    marker = tmp_path / "run" / "bus-phase"
    marker.parent.mkdir()
    boot_id = current_boot_id()
    generation = process_generation(os.getpid())
    assert boot_id is not None and generation is not None
    live = PhaseRecord("pre_bus", os.getpid(), generation, boot_id, None, None, None)
    marker.write_text(live.encode())
    sidecar_before = harness.sidecar.read_bytes()

    for action in ("release", "discard"):
        status, _ = _run(harness, tmp_path, action, event_key, event_sha)
        assert status == 1
    assert harness.sidecar.read_bytes() == sidecar_before
    reason = outbox_admin.running_agent(str(marker))
    assert reason is not None and f"pid {os.getpid()}" in reason
    assert outbox_admin.running_agent("") is not None

    # A marker from an earlier boot proves nothing is running now.
    marker.write_text(replace(live, boot_id="old-boot").encode())
    assert outbox_admin.running_agent(str(marker)) is None
    marker.write_text("garbage")
    assert outbox_admin.running_agent(str(marker)) is not None
    marker.unlink()
    assert outbox_admin.running_agent(str(marker)) is None


async def test_release_clears_quarantine_and_the_outbox_resumes_in_order(
    tmp_path: Path,
) -> None:
    harness = await _quarantined_with_dependent(tmp_path)
    event_key, event_sha = _event_key(harness)

    status, output = _run(harness, tmp_path, "release", event_key, event_sha)

    assert status == 0
    assert output == f"released {event_key}\n"
    assert harness.quarantine() == oq.QuarantineState()
    entry = _audit(harness)[-1]
    assert entry["action"] == "release"
    assert entry["key"] == event_key
    assert entry["payload_sha256"] == event_sha
    assert entry["failure_count"] == 3
    # Released records are retried: the event, then its gated result.
    bridge, _, mqtt = await harness.session("accept")
    await _until(lambda: bool(_published(mqtt, scene_result_topic(_COMMAND_ID))), "result")
    topics = [topic for topic, _, _ in mqtt.published]
    assert topics.index(scene_event_topic(_PANEL)) < topics.index(scene_result_topic(_COMMAND_ID))
    await bridge.async_shutdown()


async def test_discard_event_removes_dependent_results_and_is_audited(tmp_path: Path) -> None:
    harness = await _quarantined_with_dependent(tmp_path)
    event_key, event_sha = _event_key(harness)

    status, output = _run(harness, tmp_path, "discard", event_key, event_sha)

    assert status == 0
    assert output == f"discarded {event_key} and 1 dependent result(s)\n"
    loaded = load_state(harness.path)
    assert loaded.trusted
    assert loaded.state.events == ()
    assert loaded.state.results == ()
    # The watermark stays, so the discarded activation is never re-emitted.
    assert loaded.state.watermarks != ()
    assert harness.quarantine() == oq.QuarantineState()
    entries = _audit(harness)
    assert [entry["action"] for entry in entries] == ["discard", "discard_done"]
    assert entries[0]["dependents"] == [_RESULT_KEY]
    assert entries[0]["payload_sha256"] == event_sha

    bridge, _, mqtt = await harness.session("accept")
    await asyncio.sleep(0.05)
    assert _published(mqtt, scene_event_topic(_PANEL)) == []
    assert _last_status(mqtt, "scene")["available"] is True
    await bridge.async_shutdown()


async def test_discard_is_refused_when_the_audit_write_fails(tmp_path: Path) -> None:
    harness = await _quarantined_with_dependent(tmp_path)
    event_key, event_sha = _event_key(harness)
    outbox_admin.audit_log_path(harness.path).mkdir()
    state_before = harness.path.read_bytes()

    status, _ = _run(harness, tmp_path, "discard", event_key, event_sha)

    assert status == 1
    assert harness.path.read_bytes() == state_before
    assert harness.suspect() is not None


async def test_only_quarantined_records_are_discarded(tmp_path: Path) -> None:
    harness = await _quarantined_with_dependent(tmp_path)

    status, _ = _run(harness, tmp_path, "discard", _RESULT_KEY, _result_sha(harness))
    assert status == 1
    status, _ = _run(harness, tmp_path, "release", _RESULT_KEY, _result_sha(harness))
    assert status == 1
    status, _ = _run(harness, tmp_path, "release", "bogus", "0" * 64)
    assert status == 1


def test_audit_log_is_append_only_and_size_capped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(outbox_admin, "AUDIT_LOG_LIMIT_BYTES", 200)
    path = tmp_path / "audit.log"
    for index in range(10):
        outbox_admin.append_audit(path, {"action": "list", "index": index})
    rotated = path.with_name("audit.log.1")
    assert path.stat().st_size <= 200
    assert rotated.stat().st_size <= 200
    lines = [json.loads(line) for line in rotated.read_text().splitlines()]
    lines += [json.loads(line) for line in path.read_text().splitlines()]
    indexes = [line["index"] for line in lines]
    assert indexes == sorted(indexes) and indexes[-1] == 9
    assert oct(path.stat().st_mode & 0o777) == "0o600"


def test_incomplete_environment_is_an_error(tmp_path: Path) -> None:
    assert outbox_admin.main(["list"], env={}, out=io.StringIO()) == 1
