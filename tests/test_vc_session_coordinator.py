from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from pathlib import Path
from threading import Event

import pytest

from tools.brilliant_vc.gates import Evidence, GateLedger, GateName, GateStatus
from tools.brilliant_vc.session_approval import SessionApproval
from tools.brilliant_vc.session_coordinator import (
    CoordinatorPaths,
    EmperorIdentity,
    SessionCoordinatorError,
    coordinate_session,
    main,
    validate_emperor_process,
)
from tools.brilliant_vc.single_light_pilot import (
    CleanupReport,
    PeripheralRecord,
    PilotConfig,
    PilotRunError,
    TopologySnapshot,
)

APPROVED_AT_S = 1_800_000_000
RUN_ID = "office-vc-session-01"
STABLE_ID = "11111111-2222-4333-8444-555555555555"
VC_ID = "a" * 32
OFFICE_ID = "c" * 32
ROOM_ID = "backyard-room"


@dataclass
class FakeClock:
    now_s: float

    def wall_time_s(self) -> int:
        return int(self.now_s)

    async def sleep(self, seconds: float) -> None:
        self.now_s += seconds
        await asyncio.sleep(0)


class FakeLease:
    def __init__(self) -> None:
        self.released = False

    def release(self) -> None:
        self.released = True


def _approval() -> SessionApproval:
    return SessionApproval(
        run_id=RUN_ID,
        sha256="d" * 64,
        approved_at_s=APPROVED_AT_S,
        deadline_s=APPROVED_AT_S + 2520,
        bootstrap_timeout_s=600,
        pilot_runtime_s=1800,
        runtime_credential_bundle_sha256="a" * 64,
        vc2_gate_ledger_sha256="b" * 64,
        mqtt_password_sha256=None,
        stable_id=STABLE_ID,
        display_name="HA VC Pilot Light",
        room_id=ROOM_ID,
        office_device_id=OFFICE_ID,
        mqtt_host="mqtt.lan",
        mqtt_port=1883,
        mqtt_username=None,
    )


def _config() -> PilotConfig:
    return PilotConfig(
        stable_id=STABLE_ID,
        display_name="HA VC Pilot Light",
        room_id=ROOM_ID,
        vc_device_id=VC_ID,
        office_device_id=OFFICE_ID,
        vc_socket="/run/brilliant-vc/server_socket",
        runtime_s=1800,
    )


def _topology(*, extra_room: str | None = None) -> TopologySnapshot:
    rooms = {ROOM_ID}
    if extra_room is not None:
        rooms.add(extra_room)
    return TopologySnapshot(
        owner_device_id=VC_ID,
        device_type=6,
        peripherals=(
            PeripheralRecord(VC_ID, "device_config_peripheral", "configuration", 19),
            PeripheralRecord(VC_ID, "art_config_peripheral", "configuration", 16),
            PeripheralRecord(
                VC_ID,
                "motion_detection_config_peripheral",
                "configuration",
                20,
            ),
            PeripheralRecord(VC_ID, "alarm_config_peripheral", "configuration", 48),
        ),
        room_ids=frozenset(rooms),
    )


def _ledger() -> GateLedger:
    ledger = GateLedger.new(run_id=RUN_ID)
    for gate in (GateName.VC0, GateName.VC1, GateName.VC2):
        ledger.record(
            gate,
            GateStatus.PASS,
            f"{gate.value} passed",
            [Evidence(kind="result", value=f"{gate.value.lower()}.json")],
        )
    return ledger


def _paths(tmp_path: Path) -> CoordinatorPaths:
    output = tmp_path / "output"
    control = tmp_path / "control"
    runtime = tmp_path / "runtime"
    for path in (output, control, runtime):
        path.mkdir(mode=0o700)
    executable = tmp_path / "uwsgi"
    executable.write_bytes(b"synthetic-uwsgi")
    executable.chmod(0o755)
    return CoordinatorPaths(
        output_root=output,
        control_root=control,
        emperor_pid_path=runtime / "emperor.pid",
        expected_uwsgi_path=executable,
        emperor_log_path=tmp_path / "emperor.log",
    )


def _identity() -> EmperorIdentity:
    return EmperorIdentity(pid=321, start_ticks=4242, comm="uwsgi")


def _write_monitor_evidence(path: Path, reason: str | None) -> None:
    path.write_text(
        json.dumps(
            {
                "timestamp_s": APPROVED_AT_S,
                "pid": 321,
                "cpu_percent": None,
                "rss_bytes": 1024,
                "load_average": [0.1, 0.2, 0.3],
                "peer_count": None,
                "peer_add_timeouts": 0,
                "cloud_disconnects": 0,
                "reconnect_events": 0,
                "mqtt_round_trip_ms": None,
                "physical_lag": False,
                "abort_reason": reason,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    path.chmod(0o600)


def _replace_monitor_pid(path: Path, pid: int) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["pid"] = pid
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")


def _write_proc_tree(root: Path, *, executable: Path, uid: int, gid: int) -> None:
    pid = 321
    process = root / str(pid)
    process.mkdir(parents=True)
    fields = ["S"] + ["0"] * 19
    fields[11] = "7"
    fields[12] = "3"
    fields[19] = "4242"
    (process / "stat").write_text(f"{pid} (uwsgi) {' '.join(fields)}\n", encoding="ascii")
    (process / "statm").write_text("1000 25 0 0 0 0 0\n", encoding="ascii")
    (process / "comm").write_text("uwsgi\n", encoding="ascii")
    (process / "status").write_text(
        f"Name:\tuwsgi\nUid:\t{uid}\t{uid}\t{uid}\t{uid}\nGid:\t{gid}\t{gid}\t{gid}\t{gid}\n",
        encoding="ascii",
    )
    (process / "cgroup").write_text(
        "0::/system.slice/brilliant-vc-session.service\n", encoding="ascii"
    )
    (process / "exe").symlink_to(executable)
    (root / "stat").write_text("cpu  100 1 2 3 4 0 0 0 0 0\n", encoding="ascii")


def test_emperor_process_is_bound_to_private_pidfile_executable_identity_and_cgroup(
    tmp_path: Path,
) -> None:
    runtime = tmp_path / "runtime"
    runtime.mkdir(mode=0o700)
    executable = tmp_path / "uwsgi"
    executable.write_bytes(b"synthetic-uwsgi")
    executable.chmod(0o755)
    pid_file = runtime / "emperor.pid"
    pid_file.write_text("321\n", encoding="ascii")
    pid_file.chmod(0o600)
    os.chown(pid_file, os.getuid(), os.getgid())
    _write_proc_tree(tmp_path / "proc", executable=executable, uid=os.getuid(), gid=os.getgid())

    identity = validate_emperor_process(
        pid_file=pid_file,
        expected_executable=executable,
        runtime_uid=os.getuid(),
        runtime_gid=os.getgid(),
        proc_root=tmp_path / "proc",
        allowed_pid_roots=(runtime,),
    )

    assert identity == _identity()

    status = tmp_path / "proc/321/status"
    status.write_text(
        f"Name:\tuwsgi\nUid:\t0\t{os.getuid()}\t{os.getuid()}\t{os.getuid()}\n"
        f"Gid:\t{os.getgid()}\t{os.getgid()}\t{os.getgid()}\t{os.getgid()}\n",
        encoding="ascii",
    )
    with pytest.raises(SessionCoordinatorError, match="UID"):
        validate_emperor_process(
            pid_file=pid_file,
            expected_executable=executable,
            runtime_uid=os.getuid(),
            runtime_gid=os.getgid(),
            proc_root=tmp_path / "proc",
            allowed_pid_roots=(runtime,),
        )


def test_emperor_process_accepts_only_the_matching_unified_path_on_hybrid_cgroups(
    tmp_path: Path,
) -> None:
    runtime = tmp_path / "runtime"
    runtime.mkdir(mode=0o700)
    executable = tmp_path / "uwsgi"
    executable.write_bytes(b"synthetic-uwsgi")
    executable.chmod(0o755)
    pid_file = runtime / "emperor.pid"
    pid_file.write_text("321\n", encoding="ascii")
    pid_file.chmod(0o600)
    os.chown(pid_file, os.getuid(), os.getgid())
    _write_proc_tree(tmp_path / "proc", executable=executable, uid=os.getuid(), gid=os.getgid())
    cgroup = tmp_path / "proc/321/cgroup"
    cgroup.write_text(
        "8:cpu,cpuacct:/system.slice/brilliant-vc-session.service\n"
        "7:pids:/system.slice/brilliant-vc-session.service\n"
        "6:blkio:/\n"
        "3:memory:/system.slice/brilliant-vc-session.service\n"
        "0::/system.slice/brilliant-vc-session.service\n",
        encoding="ascii",
    )

    assert (
        validate_emperor_process(
            pid_file=pid_file,
            expected_executable=executable,
            runtime_uid=os.getuid(),
            runtime_gid=os.getgid(),
            proc_root=tmp_path / "proc",
            allowed_pid_roots=(runtime,),
        )
        == _identity()
    )

    cgroup.write_text(
        cgroup.read_text(encoding="ascii").replace(
            "0::/system.slice/brilliant-vc-session.service",
            "0::/system.slice/different.service",
        ),
        encoding="ascii",
    )
    with pytest.raises(SessionCoordinatorError, match="cgroup"):
        validate_emperor_process(
            pid_file=pid_file,
            expected_executable=executable,
            runtime_uid=os.getuid(),
            runtime_gid=os.getgid(),
            proc_root=tmp_path / "proc",
            allowed_pid_roots=(runtime,),
        )


def test_coordinator_cli_requires_explicit_apply_before_account_or_bus_access(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        "tools.brilliant_vc.session_coordinator._runtime_account",
        lambda: (_ for _ in ()).throw(AssertionError("account lookup must not run")),
    )

    assert main([]) == 0

    output = capsys.readouterr().out
    assert "DRY RUN" in output
    assert '"started": false' in output


async def test_stable_topology_records_vc3_vc4_then_runs_one_cleanup_proven_pilot(
    tmp_path: Path,
) -> None:
    paths = _paths(tmp_path)
    approval = _approval()
    clock = FakeClock(APPROVED_AT_S + 50)
    observations = [_topology(), _topology()]
    lease = FakeLease()
    pilot_calls = 0

    async def probe(_config: PilotConfig) -> TopologySnapshot:
        return observations.pop(0)

    async def pilot(**_kwargs: object) -> CleanupReport:
        nonlocal pilot_calls
        pilot_calls += 1
        return CleanupReport(already_clean=False, absent_first=True, absent_second=True)

    async def monitor(*, output_jsonl: Path, stop: Event, **_kwargs: object) -> str | None:
        _write_monitor_evidence(output_jsonl, None)
        await asyncio.to_thread(stop.wait)
        return None

    result = await coordinate_session(
        approval=approval,
        config=_config(),
        vc2_ledger=_ledger(),
        vc2_ledger_sha256=approval.vc2_gate_ledger_sha256,
        mqtt_password=None,
        paths=paths,
        runtime_uid=os.getuid(),
        revalidate_approval=lambda: approval,
        validate_process=_identity,
        topology_probe=probe,
        pilot_runner=pilot,
        monitor_runner=monitor,
        lease_factory=lambda: lease,
        wall_time_s=clock.wall_time_s,
        sleep=clock.sleep,
    )

    assert result.succeeded is True
    assert result.cleanup_proven is True
    assert result.monitor_abort_reason is None
    assert pilot_calls == 1
    assert lease.released is True
    assert observations == []
    persisted = GateLedger.load(paths.output_root / "gate-ledger.json")
    assert persisted.status(GateName.VC3) is GateStatus.PASS
    assert persisted.status(GateName.VC4) is GateStatus.PASS
    assert persisted.status(GateName.VC5) is GateStatus.NOT_RUN
    terminal = json.loads((paths.output_root / "session-result.json").read_text())
    topology = (paths.output_root / "topology.json").read_text()
    assert terminal["succeeded"] is True
    assert VC_ID not in topology and OFFICE_ID not in topology
    assert all(path.stat().st_mode & 0o777 == 0o600 for path in paths.output_root.iterdir())


async def test_topology_instability_exhausts_bootstrap_without_starting_light(
    tmp_path: Path,
) -> None:
    paths = _paths(tmp_path)
    approval = _approval()
    clock = FakeClock(approval.deadline_s - approval.pilot_runtime_s - 65)
    calls = 0

    async def probe(_config: PilotConfig) -> TopologySnapshot:
        nonlocal calls
        calls += 1
        return _topology(extra_room=f"room-{calls}")

    async def pilot(**_kwargs: object) -> CleanupReport:
        raise AssertionError("unstable topology must never start the light")

    result = await coordinate_session(
        approval=approval,
        config=_config(),
        vc2_ledger=_ledger(),
        vc2_ledger_sha256=approval.vc2_gate_ledger_sha256,
        mqtt_password=None,
        paths=paths,
        runtime_uid=os.getuid(),
        revalidate_approval=lambda: approval,
        validate_process=_identity,
        topology_probe=probe,
        pilot_runner=pilot,
        monitor_runner=lambda **_kwargs: asyncio.sleep(0, result=None),
        lease_factory=FakeLease,
        wall_time_s=clock.wall_time_s,
        sleep=clock.sleep,
    )

    assert result.succeeded is False
    assert result.failure_class == "topology_not_stable"
    assert result.cleanup_proven is False
    assert not (paths.output_root / "gate-ledger.json").exists()
    assert (paths.output_root / "session-result.json").exists()


async def test_monitor_abort_requests_cleanup_and_fails_session_without_killing_emperor(
    tmp_path: Path,
) -> None:
    paths = _paths(tmp_path)
    approval = _approval()
    clock = FakeClock(APPROVED_AT_S + 50)
    lease = FakeLease()
    pilot_saw_stop = False

    async def probe(_config: PilotConfig) -> TopologySnapshot:
        return _topology()

    async def monitor(*, output_jsonl: Path, **_kwargs: object) -> str | None:
        _write_monitor_evidence(output_jsonl, "rss_bytes")
        return "rss_bytes"

    async def pilot(*, stop: asyncio.Event, **_kwargs: object) -> CleanupReport:
        nonlocal pilot_saw_stop
        await stop.wait()
        pilot_saw_stop = True
        return CleanupReport(already_clean=False, absent_first=True, absent_second=True)

    result = await coordinate_session(
        approval=approval,
        config=_config(),
        vc2_ledger=_ledger(),
        vc2_ledger_sha256=approval.vc2_gate_ledger_sha256,
        mqtt_password=None,
        paths=paths,
        runtime_uid=os.getuid(),
        revalidate_approval=lambda: approval,
        validate_process=_identity,
        topology_probe=probe,
        pilot_runner=pilot,
        monitor_runner=monitor,
        lease_factory=lambda: lease,
        wall_time_s=clock.wall_time_s,
        sleep=clock.sleep,
    )

    assert result.succeeded is False
    assert result.failure_class == "monitor_abort"
    assert result.monitor_abort_reason == "rss_bytes"
    assert result.cleanup_proven is True
    assert pilot_saw_stop is True
    assert lease.released is True


async def test_pilot_failure_preserves_proven_cleanup_in_terminal_result(
    tmp_path: Path,
) -> None:
    paths = _paths(tmp_path)
    approval = _approval()
    clock = FakeClock(APPROVED_AT_S + 50)

    async def probe(_config: PilotConfig) -> TopologySnapshot:
        return _topology()

    async def monitor(*, output_jsonl: Path, stop: Event, **_kwargs: object) -> str | None:
        _write_monitor_evidence(output_jsonl, None)
        await asyncio.to_thread(stop.wait)
        return None

    async def pilot(**_kwargs: object) -> CleanupReport:
        cleanup = CleanupReport(already_clean=False, absent_first=True, absent_second=True)
        raise PilotRunError("mqtt_authority_lost", cleanup_report=cleanup)

    result = await coordinate_session(
        approval=approval,
        config=_config(),
        vc2_ledger=_ledger(),
        vc2_ledger_sha256=approval.vc2_gate_ledger_sha256,
        mqtt_password=None,
        paths=paths,
        runtime_uid=os.getuid(),
        revalidate_approval=lambda: approval,
        validate_process=_identity,
        topology_probe=probe,
        pilot_runner=pilot,
        monitor_runner=monitor,
        lease_factory=FakeLease,
        wall_time_s=clock.wall_time_s,
        sleep=clock.sleep,
    )

    assert result.succeeded is False
    assert result.failure_class == "pilot_failure"
    assert result.cleanup_proven is True


async def test_monitor_evidence_with_a_different_pid_fails_closed(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    approval = _approval()
    clock = FakeClock(APPROVED_AT_S + 50)

    async def probe(_config: PilotConfig) -> TopologySnapshot:
        return _topology()

    async def monitor(*, output_jsonl: Path, stop: Event, **_kwargs: object) -> str | None:
        _write_monitor_evidence(output_jsonl, None)
        _replace_monitor_pid(output_jsonl, 999)
        await asyncio.to_thread(stop.wait)
        return None

    async def pilot(**_kwargs: object) -> CleanupReport:
        return CleanupReport(already_clean=False, absent_first=True, absent_second=True)

    result = await coordinate_session(
        approval=approval,
        config=_config(),
        vc2_ledger=_ledger(),
        vc2_ledger_sha256=approval.vc2_gate_ledger_sha256,
        mqtt_password=None,
        paths=paths,
        runtime_uid=os.getuid(),
        revalidate_approval=lambda: approval,
        validate_process=_identity,
        topology_probe=probe,
        pilot_runner=pilot,
        monitor_runner=monitor,
        lease_factory=FakeLease,
        wall_time_s=clock.wall_time_s,
        sleep=clock.sleep,
    )

    assert result.succeeded is False
    assert result.failure_class == "monitor_evidence_invalid"
    assert result.cleanup_proven is True


async def test_active_absolute_deadline_requests_cleanup_before_service_failure(
    tmp_path: Path,
) -> None:
    paths = _paths(tmp_path)
    approval = _approval()
    clock = FakeClock(APPROVED_AT_S + 50)
    active = False

    def wall_time_s() -> int:
        return approval.deadline_s if active else clock.wall_time_s()

    async def probe(_config: PilotConfig) -> TopologySnapshot:
        return _topology()

    async def monitor(*, output_jsonl: Path, stop: Event, **_kwargs: object) -> str | None:
        _write_monitor_evidence(output_jsonl, None)
        await asyncio.to_thread(stop.wait)
        return None

    async def pilot(*, stop: asyncio.Event, **_kwargs: object) -> CleanupReport:
        nonlocal active
        active = True
        await stop.wait()
        return CleanupReport(already_clean=False, absent_first=True, absent_second=True)

    result = await coordinate_session(
        approval=approval,
        config=_config(),
        vc2_ledger=_ledger(),
        vc2_ledger_sha256=approval.vc2_gate_ledger_sha256,
        mqtt_password=None,
        paths=paths,
        runtime_uid=os.getuid(),
        revalidate_approval=lambda: approval,
        validate_process=_identity,
        topology_probe=probe,
        pilot_runner=pilot,
        monitor_runner=monitor,
        lease_factory=FakeLease,
        wall_time_s=wall_time_s,
        sleep=clock.sleep,
    )

    assert result.succeeded is False
    assert result.failure_class == "approval_deadline_elapsed"
    assert result.cleanup_proven is True
