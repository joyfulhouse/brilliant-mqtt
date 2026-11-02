"""On-panel operator recovery CLI for a quarantined outbox record (#113)."""

from __future__ import annotations

import asyncio
import fcntl
import io
import json
import os
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import cast

import pytest

from brilliant_mqtt import outbox_admin
from brilliant_mqtt import outbox_quarantine as oq
from brilliant_mqtt.ha_control_protocol import (
    scene_command_topic,
    scene_event_topic,
    scene_result_topic,
)
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


def _stopped(unit: str) -> str:
    assert unit == "brilliant-mqtt"
    return "inactive\n"


def _proc(tmp_path: Path, *processes: tuple[int, bytes]) -> Path:
    root = tmp_path / "proc"
    root.mkdir(exist_ok=True)
    for pid, cmdline in processes:
        (root / str(pid)).mkdir()
        (root / str(pid) / "cmdline").write_bytes(cmdline)
    return root


def _run(
    harness: _Harness,
    tmp_path: Path,
    *argv: str,
    systemctl: Callable[[str], str] = _stopped,
    proc_root: Path | None = None,
) -> tuple[int, str]:
    out = io.StringIO()
    status = outbox_admin.main(
        list(argv),
        env=_env(harness, tmp_path),
        out=out,
        systemctl=systemctl,
        proc_root=_proc(tmp_path) if proc_root is None else proc_root,
    )
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
    # list is read-only and is not audited.
    assert not outbox_admin.audit_log_path(harness.path).exists()


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


_AGENT_CMDLINE = b"/data/switch-embedded/env/bin/python3\x00-m\x00brilliant_mqtt\x00"


async def test_empty_or_missing_marker_with_agent_stopped_is_allowed(tmp_path: Path) -> None:
    harness = await _quarantined_with_dependent(tmp_path)
    event_key, event_sha = _event_key(harness)
    marker = tmp_path / "run" / "bus-phase"
    marker.parent.mkdir()
    marker.write_text("")
    proc_root = _proc(
        tmp_path,
        # Unrelated processes and the admin tool itself are not the agent.
        (400, b"/usr/bin/python3\x00-m\x00brilliant_mqtt.outbox_admin\x00list\x00"),
        (401, b"/usr/sbin/sshd\x00-D\x00"),
    )
    assert outbox_admin.running_agent(str(marker), systemctl=_stopped, proc_root=proc_root) is None
    marker.unlink()
    assert outbox_admin.running_agent(str(marker), systemctl=_stopped, proc_root=proc_root) is None
    marker.write_text("")

    status, _ = _run(harness, tmp_path, "release", event_key, event_sha, proc_root=proc_root)

    assert status == 0
    assert harness.quarantine() == oq.QuarantineState()


def test_failed_unit_counts_as_stopped(tmp_path: Path) -> None:
    proc_root = _proc(tmp_path)
    assert (
        outbox_admin.running_agent("", systemctl=lambda unit: "failed\n", proc_root=proc_root)
        is None
    )


async def test_live_agent_process_is_refused_even_without_a_marker(tmp_path: Path) -> None:
    harness = await _quarantined_with_dependent(tmp_path)
    event_key, event_sha = _event_key(harness)
    proc_root = _proc(tmp_path, (321, _AGENT_CMDLINE))
    sidecar_before = harness.sidecar.read_bytes()

    for action in ("release", "discard"):
        status, _ = _run(harness, tmp_path, action, event_key, event_sha, proc_root=proc_root)
        assert status == 1
    assert harness.sidecar.read_bytes() == sidecar_before
    reason = outbox_admin.running_agent("", systemctl=_stopped, proc_root=proc_root)
    assert reason is not None and "pid 321" in reason
    # The tool's own pid is never counted.
    own = outbox_admin.running_agent("", systemctl=_stopped, proc_root=proc_root, own_pid=321)
    assert own is None


@pytest.mark.parametrize("unit_state", ["active\n", "activating\n", "deactivating\n", ""])
async def test_systemd_not_stopped_is_refused(tmp_path: Path, unit_state: str) -> None:
    harness = await _quarantined_with_dependent(tmp_path)
    event_key, event_sha = _event_key(harness)

    status, _ = _run(
        harness, tmp_path, "discard", event_key, event_sha, systemctl=lambda unit: unit_state
    )

    assert status == 1
    assert harness.suspect() is not None


async def test_systemctl_error_is_refused(tmp_path: Path) -> None:
    harness = await _quarantined_with_dependent(tmp_path)
    event_key, event_sha = _event_key(harness)

    def broken(unit: str) -> str:
        raise FileNotFoundError("systemctl")

    def hung(unit: str) -> str:
        raise subprocess.TimeoutExpired(["systemctl"], 10)

    for systemctl in (broken, hung):
        status, _ = _run(harness, tmp_path, "release", event_key, event_sha, systemctl=systemctl)
        assert status == 1
    assert harness.suspect() is not None


def test_held_phase_lease_adds_a_refusal(tmp_path: Path) -> None:
    marker = tmp_path / "bus-phase"
    marker.write_text("anything")
    proc_root = _proc(tmp_path)
    with open(marker) as holder:
        fcntl.flock(holder.fileno(), fcntl.LOCK_EX)
        reason = outbox_admin.running_agent(str(marker), systemctl=_stopped, proc_root=proc_root)
        assert reason is not None and "lease" in reason
    assert outbox_admin.running_agent(str(marker), systemctl=_stopped, proc_root=proc_root) is None


async def test_release_clears_quarantine_and_the_outbox_resumes_in_order(
    tmp_path: Path,
) -> None:
    harness = await _quarantined_with_dependent(tmp_path)
    event_key, event_sha = _event_key(harness)

    status, output = _run(harness, tmp_path, "release", event_key, event_sha)

    assert status == 0
    assert output == f"released {event_key}\n"
    assert harness.quarantine() == oq.QuarantineState()
    entries = _audit(harness)
    assert [item["action"] for item in entries] == ["release", "release_done"]
    entry = entries[0]
    assert entry["key"] == event_key
    assert entry["payload_sha256"] == event_sha
    assert entry["failure_count"] == 3
    assert entries[1]["outcome"] == "ok"
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
    for index in range(40):
        outbox_admin.append_audit(path, {"action": "release", "index": index})
    generations = [path.with_name(f"audit.log.{n}") for n in (3, 2, 1)] + [path]
    assert not path.with_name("audit.log.4").exists()
    lines: list[dict[str, object]] = []
    for generation in generations:
        assert generation.stat().st_size <= 200
        lines += [json.loads(line) for line in generation.read_text().splitlines()]
    indexes = [cast(int, line["index"]) for line in lines]
    # Three rotated generations plus the live log, contiguous and in order.
    assert indexes == list(range(indexes[0], 40))
    assert indexes[0] > 0
    assert oct(path.stat().st_mode & 0o777) == "0o600"


def test_audit_line_is_one_write_and_a_short_write_refuses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "audit.log"
    writes: list[int] = []
    real_write = os.write

    def counting_write(descriptor: int, data: bytes) -> int:
        writes.append(len(data))
        return real_write(descriptor, data)

    monkeypatch.setattr(outbox_admin, "_os_write", counting_write)
    outbox_admin.append_audit(path, {"action": "release", "key": "k"})
    assert len(writes) == 1 and writes[0] == len(path.read_bytes())

    def short_write(descriptor: int, data: bytes) -> int:
        return real_write(descriptor, data[:3])

    monkeypatch.setattr(outbox_admin, "_os_write", short_write)
    with pytest.raises(OSError):
        outbox_admin.append_audit(path, {"action": "discard", "key": "k"})


async def test_release_is_refused_when_the_audit_write_fails(tmp_path: Path) -> None:
    harness = await _quarantined_with_dependent(tmp_path)
    event_key, event_sha = _event_key(harness)
    outbox_admin.audit_log_path(harness.path).mkdir()
    sidecar_before = harness.sidecar.read_bytes()

    status, _ = _run(harness, tmp_path, "release", event_key, event_sha)

    assert status == 1
    assert harness.sidecar.read_bytes() == sidecar_before


async def test_release_sidecar_write_failure_is_reported_cleanly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    harness = await _quarantined_with_dependent(tmp_path)
    event_key, event_sha = _event_key(harness)

    def failing_write(path: Path, state: oq.QuarantineState) -> None:
        raise OSError("read-only filesystem")

    monkeypatch.setattr(outbox_admin, "write_sidecar", failing_write)

    status, _ = _run(harness, tmp_path, "release", event_key, event_sha)

    assert status == 1
    assert "Traceback" not in capsys.readouterr().err
    entries = _audit(harness)
    assert [item["action"] for item in entries] == ["release", "release_done"]
    assert entries[1]["outcome"] == "failed"
    assert harness.suspect() is not None


def test_incomplete_environment_is_an_error(tmp_path: Path) -> None:
    assert outbox_admin.main(["list"], env={}, out=io.StringIO()) == 1
