"""Coordinate one approved non-root VC bootstrap and one-light lifecycle.

This module never provisions an identity, writes a slider binding, synthesizes
a panel gesture, or addresses the physical Control bus.  Its only live write
adapter is the existing one-light host, reached after a consumed approval,
stable isolated topology, and exact Emperor identity have all been proved.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import os
import stat
import sys
import threading
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from tools.brilliant_vc.gates import Evidence, GateLedger, GateName, GateStatus
from tools.brilliant_vc.monitor import read_proc_snapshot, run_bounded_monitor
from tools.brilliant_vc.runtime_prepare import (
    RuntimePrepareError,
    _read_file,
    _runtime_account,
    _validate_runtime_credentials,
)
from tools.brilliant_vc.session_approval import (
    SessionApproval,
    SessionApprovalError,
    validate_session_approval,
)
from tools.brilliant_vc.session_prepare import (
    CoordinatedSessionPrepareError,
    SessionPreparePaths,
    _default_session_paths,
    validate_coordinated_session_inputs,
)
from tools.brilliant_vc.single_light_pilot import (
    CleanupReport,
    PilotConfig,
    PilotGuardError,
    PilotLease,
    TopologySnapshot,
    canonical_topology_bytes,
    probe_live_topology,
    run_live_pilot,
    validate_topology,
)

_INTERNAL_RESERVE_S = 60
_STABLE_OBSERVATION_INTERVAL_S = 10.0
_TOPOLOGY_RETRY_INTERVAL_S = 2.0
_TOPOLOGY_PROBE_TIMEOUT_S = 35.0
_MAX_MONITOR_BYTES = 8 * 1024 * 1024
_DEFAULT_PID_ROOTS = (Path("/run/brilliant-vc"), Path("/var/run/brilliant-vc"))
_EXPECTED_CGROUP = "/system.slice/brilliant-vc-session.service"


class SessionCoordinatorError(ValueError):
    """Fail-closed coordinator error with a redaction-safe classification."""

    def __init__(self, message: str, *, code: str = "coordinator_guard") -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class CoordinatorPaths:
    """Fixed writable evidence and exact Emperor identity paths."""

    output_root: Path
    control_root: Path
    emperor_pid_path: Path
    expected_uwsgi_path: Path
    emperor_log_path: Path


@dataclass(frozen=True, slots=True)
class EmperorIdentity:
    """Exact process identity retained across readiness and monitoring."""

    pid: int
    start_ticks: int
    comm: str


@dataclass(frozen=True, slots=True)
class LoadedCoordinatorSession:
    """Fully bound fixed-path input retained only in coordinator memory."""

    approval: SessionApproval
    config: PilotConfig
    vc2_ledger: GateLedger
    vc2_ledger_sha256: str
    mqtt_password: str | None
    runtime_uid: int
    runtime_gid: int


@dataclass(frozen=True, slots=True)
class SessionCoordinatorResult:
    """Secret-free terminal result for one non-retryable session."""

    succeeded: bool
    cleanup_proven: bool
    failure_class: str | None
    monitor_abort_reason: str | None
    approval_run_id: str
    approval_sha256: str
    runtime_credential_bundle_sha256: str
    vc2_gate_ledger_sha256: str
    topology_sha256: str | None
    vc_device_id_redacted: str
    office_device_id_redacted: str
    emperor_pid: int | None
    emperor_start_ticks: int | None
    vc3_status: str
    vc4_status: str
    vc5_status: str
    absent_first: bool
    absent_second: bool

    def to_public_dict(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "succeeded": self.succeeded,
            "cleanup_proven": self.cleanup_proven,
            "failure_class": self.failure_class,
            "monitor_abort_reason": self.monitor_abort_reason,
            "approval_run_id": self.approval_run_id,
            "approval_sha256": self.approval_sha256,
            "runtime_credential_bundle_sha256": self.runtime_credential_bundle_sha256,
            "vc2_gate_ledger_sha256": self.vc2_gate_ledger_sha256,
            "topology_sha256": self.topology_sha256,
            "vc_device_id_redacted": self.vc_device_id_redacted,
            "office_device_id_redacted": self.office_device_id_redacted,
            "emperor_pid": self.emperor_pid,
            "emperor_start_ticks": self.emperor_start_ticks,
            "vc3_status": self.vc3_status,
            "vc4_status": self.vc4_status,
            "vc5_status": self.vc5_status,
            "absent_first": self.absent_first,
            "absent_second": self.absent_second,
        }


class SessionLease(Protocol):
    def release(self) -> None: ...


class TopologyProbe(Protocol):
    async def __call__(self, config: PilotConfig, /) -> TopologySnapshot: ...


class PilotRunner(Protocol):
    async def __call__(
        self,
        *,
        config: PilotConfig,
        topology: TopologySnapshot,
        mqtt_host: str,
        mqtt_port: int,
        mqtt_username: str | None,
        mqtt_password: str | None,
        stop: asyncio.Event,
        install_signal_handlers: bool,
    ) -> CleanupReport: ...


class MonitorRunner(Protocol):
    async def __call__(
        self,
        *,
        identity: EmperorIdentity,
        duration_s: float,
        output_jsonl: Path,
        journal_file: Path | None,
        stop: threading.Event,
    ) -> str | None: ...


def validate_emperor_process(
    *,
    pid_file: Path,
    expected_executable: Path,
    runtime_uid: int,
    runtime_gid: int,
    proc_root: Path = Path("/proc"),
    allowed_pid_roots: Sequence[Path] = _DEFAULT_PID_ROOTS,
    expected_cgroup: str = _EXPECTED_CGROUP,
) -> EmperorIdentity:
    """Bind the service PID file to one unchanged non-root uWSGI Emperor."""

    if any(
        isinstance(value, bool) or not isinstance(value, int) or value < 0
        for value in (runtime_uid, runtime_gid)
    ):
        raise SessionCoordinatorError("runtime identity is invalid")
    resolved_parent = pid_file.parent.resolve(strict=False)
    if resolved_parent not in {path.resolve(strict=False) for path in allowed_pid_roots}:
        raise SessionCoordinatorError("Emperor PID file is outside the allowed runtime roots")
    if pid_file.name != "emperor.pid":
        raise SessionCoordinatorError("Emperor PID file name is not canonical")
    if not pid_file.exists() and not pid_file.is_symlink():
        raise SessionCoordinatorError(
            "Emperor PID file is not ready",
            code="emperor_not_ready",
        )
    try:
        raw_pid = _read_file(
            pid_file,
            description="Emperor PID file",
            uid=runtime_uid,
            gid=runtime_gid,
            mode=0o600,
            maximum_bytes=64,
        )
    except RuntimePrepareError as error:
        raise SessionCoordinatorError(str(error)) from None
    try:
        try:
            encoded_pid = raw_pid.decode("ascii")
        except UnicodeDecodeError:
            raise SessionCoordinatorError("Emperor PID file is not ASCII") from None
        stripped = encoded_pid.rstrip("\n")
        if not stripped or not stripped.isascii() or not stripped.isdecimal():
            raise SessionCoordinatorError("Emperor PID file is invalid")
        if stripped != str(int(stripped)) or encoded_pid not in {stripped, f"{stripped}\n"}:
            raise SessionCoordinatorError("Emperor PID file is not canonical")
        pid = int(stripped)
        if pid <= 1:
            raise SessionCoordinatorError("Emperor PID is invalid")
    finally:
        _wipe(raw_pid)

    process_root = proc_root / str(pid)
    try:
        before = read_proc_snapshot(proc_root, pid)
        expected = expected_executable.resolve(strict=True)
        observed = (process_root / "exe").resolve(strict=True)
        if observed != expected:
            raise SessionCoordinatorError("Emperor executable does not match pinned uWSGI")
        expected_stat = expected.stat()
        observed_stat = (process_root / "exe").stat()
        if (expected_stat.st_dev, expected_stat.st_ino) != (
            observed_stat.st_dev,
            observed_stat.st_ino,
        ):
            raise SessionCoordinatorError("Emperor executable identity changed")
        uid_values = _status_identity_values(process_root / "status", "Uid")
        gid_values = _status_identity_values(process_root / "status", "Gid")
        if uid_values != (runtime_uid,) * 4:
            raise SessionCoordinatorError("Emperor UID set does not match brilliant-vc")
        if gid_values != (runtime_gid,) * 4:
            raise SessionCoordinatorError("Emperor GID set does not match brilliant-vc")
        cgroup = _read_bounded_text(process_root / "cgroup", maximum_bytes=16 * 1024)
        cgroup_paths = {line.split("::", 1)[1] for line in cgroup.splitlines() if "::" in line}
        if cgroup_paths != {expected_cgroup}:
            raise SessionCoordinatorError("Emperor cgroup does not match the session unit")
        after = read_proc_snapshot(proc_root, pid)
    except SessionCoordinatorError:
        raise
    except FileNotFoundError:
        raise SessionCoordinatorError(
            "Emperor process is not ready",
            code="emperor_not_ready",
        ) from None
    except (OSError, RuntimeError, ValueError):
        raise SessionCoordinatorError("Emperor process identity could not be read") from None
    if before != after:
        raise SessionCoordinatorError("Emperor process changed during validation")
    if before.comm != "uwsgi":
        raise SessionCoordinatorError("Emperor process name does not match uWSGI")
    return EmperorIdentity(pid=pid, start_ticks=before.start_ticks, comm=before.comm)


def load_coordinator_session(
    session_paths: SessionPreparePaths,
    *,
    now_s: int,
    runtime_uid: int,
    runtime_gid: int,
    credential_uid: int = 0,
    allowed_input_roots: Sequence[Path] = (Path("/data/brilliant-vc-session-input"),),
    allowed_output_roots: Sequence[Path] = (Path("/data/brilliant-vc-session"),),
    allowed_control_roots: Sequence[Path] = (Path("/run/brilliant-vc-session"),),
    allowed_approval_marker_paths: Sequence[Path] = (
        Path("/run/brilliant-vc-session-approval/session-approval-consumed.json"),
    ),
) -> LoadedCoordinatorSession:
    """Revalidate every fixed input after Emperor start and before bus access."""

    try:
        inputs = validate_coordinated_session_inputs(
            session_paths,
            runtime_uid=runtime_uid,
            runtime_gid=runtime_gid,
            credential_uid=credential_uid,
            allowed_input_roots=allowed_input_roots,
            allowed_output_roots=allowed_output_roots,
            allowed_control_roots=allowed_control_roots,
        )
        if (
            session_paths.approval_source_path.exists()
            or session_paths.approval_source_path.is_symlink()
        ):
            raise SessionCoordinatorError("unconsumed coordinated-session approval still exists")
        approval = validate_session_approval(
            session_paths.approval_marker_path,
            now_s=now_s,
            credential_uid=credential_uid,
            runtime_gid=runtime_gid,
            allowed_paths=allowed_approval_marker_paths,
            phase="active",
        )
        vc_device_id, credential_digest = _validate_runtime_credentials(
            session_paths.launcher,
            now_s=now_s,
            credential_uid=credential_uid,
            runtime_gid=runtime_gid,
        )
    except (CoordinatedSessionPrepareError, SessionApprovalError, RuntimePrepareError) as error:
        raise SessionCoordinatorError(str(error)) from None
    if inputs.vc2_ledger_run_id != approval.run_id:
        raise SessionCoordinatorError("approval run ID does not match the VC2 ledger")
    if inputs.vc2_gate_ledger_sha256 != approval.vc2_gate_ledger_sha256:
        raise SessionCoordinatorError("approval does not bind the VC2 ledger")
    if inputs.mqtt_password_sha256 != approval.mqtt_password_sha256:
        raise SessionCoordinatorError("approval does not bind the MQTT password")
    if credential_digest != approval.runtime_credential_bundle_sha256:
        raise SessionCoordinatorError("approval does not bind the runtime credentials")
    password = _load_mqtt_password(
        session_paths,
        credential_uid=credential_uid,
        runtime_gid=runtime_gid,
        expected_sha256=approval.mqtt_password_sha256,
    )
    try:
        config = PilotConfig(
            stable_id=approval.stable_id,
            display_name=approval.display_name,
            room_id=approval.room_id,
            vc_device_id=vc_device_id,
            office_device_id=approval.office_device_id,
            vc_socket=str(session_paths.launcher.socket_path),
            runtime_s=approval.pilot_runtime_s,
        )
    except PilotGuardError as error:
        raise SessionCoordinatorError(str(error)) from None
    return LoadedCoordinatorSession(
        approval=approval,
        config=config,
        vc2_ledger=inputs.vc2_ledger,
        vc2_ledger_sha256=inputs.vc2_gate_ledger_sha256,
        mqtt_password=password,
        runtime_uid=runtime_uid,
        runtime_gid=runtime_gid,
    )


async def coordinate_session(
    *,
    approval: SessionApproval,
    config: PilotConfig,
    vc2_ledger: GateLedger,
    vc2_ledger_sha256: str,
    mqtt_password: str | None,
    paths: CoordinatorPaths,
    runtime_uid: int,
    revalidate_approval: Callable[[], SessionApproval],
    validate_process: Callable[[], EmperorIdentity],
    topology_probe: TopologyProbe = probe_live_topology,
    pilot_runner: PilotRunner = run_live_pilot,
    monitor_runner: MonitorRunner | None = None,
    lease_factory: Callable[[], SessionLease] | None = None,
    wall_time_s: Callable[[], int] = lambda: int(time.time()),
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> SessionCoordinatorResult:
    """Run the exact stable-topology, monitor, host, and cleanup sequence."""

    _validate_empty_session_roots(paths, runtime_uid=runtime_uid)
    terminal_path = paths.output_root / "session-result.json"
    topology_digest: str | None = None
    emperor: EmperorIdentity | None = None
    cleanup: CleanupReport | None = None
    monitor_abort: str | None = None
    vc3 = GateStatus.NOT_RUN
    vc4 = GateStatus.NOT_RUN
    failure_class: str | None = None

    try:
        _validate_bound_session(
            approval=approval,
            config=config,
            ledger=vc2_ledger,
            vc2_ledger_sha256=vc2_ledger_sha256,
            mqtt_password=mqtt_password,
        )
        _require_same_approval(approval, revalidate_approval())
        emperor = await _wait_for_emperor(
            approval=approval,
            revalidate_approval=revalidate_approval,
            validate_process=validate_process,
            wall_time_s=wall_time_s,
            sleep=sleep,
        )
        topology = await _wait_for_stable_topology(
            approval=approval,
            config=config,
            emperor=emperor,
            revalidate_approval=revalidate_approval,
            validate_process=validate_process,
            topology_probe=topology_probe,
            wall_time_s=wall_time_s,
            sleep=sleep,
        )
        normalized_topology = canonical_topology_bytes(topology)
        topology_digest = hashlib.sha256(normalized_topology).hexdigest()
        _write_private_json(
            paths.output_root / "topology.json",
            _sanitized_topology_payload(topology, config=config, sha256=topology_digest),
        )
        _record_readiness_gates(
            vc2_ledger,
            emperor=emperor,
            topology_sha256=topology_digest,
        )
        _save_private_ledger(vc2_ledger, paths.output_root / "gate-ledger.json")
        vc3 = GateStatus.PASS
        vc4 = GateStatus.PASS

        _require_same_approval(approval, revalidate_approval())
        _require_same_process(emperor, validate_process())
        if wall_time_s() + approval.pilot_runtime_s + _INTERNAL_RESERVE_S > approval.deadline_s:
            raise SessionCoordinatorError(
                "the full pilot and cleanup reserve no longer fit",
                code="pilot_budget_exhausted",
            )

        monitor = _default_monitor_runner if monitor_runner is None else monitor_runner
        lease = (
            PilotLease.acquire(
                paths.control_root,
                required_uid=runtime_uid,
                allowed_roots=(paths.control_root,),
            )
            if lease_factory is None
            else lease_factory()
        )
        try:
            cleanup, monitor_abort, run_failure = await _run_pilot_and_monitor(
                approval=approval,
                config=config,
                topology=topology,
                mqtt_password=mqtt_password,
                paths=paths,
                emperor=emperor,
                pilot_runner=pilot_runner,
                monitor_runner=monitor,
                revalidate_approval=revalidate_approval,
                validate_process=validate_process,
                wall_time_s=wall_time_s,
            )
        finally:
            lease.release()
        _validate_monitor_output(
            paths.output_root / "monitor.jsonl",
            runtime_uid=runtime_uid,
            identity=emperor,
            expected_abort_reason=monitor_abort,
        )
        if run_failure is not None:
            failure_class = run_failure
        elif monitor_abort is not None:
            failure_class = "monitor_abort"
    except SessionCoordinatorError as error:
        failure_class = error.code
    except PilotGuardError:
        failure_class = "pilot_guard"
    except Exception:
        failure_class = "unexpected_error"

    cleanup_proven = bool(cleanup is not None and cleanup.absent_first and cleanup.absent_second)
    result = SessionCoordinatorResult(
        succeeded=failure_class is None and cleanup_proven,
        cleanup_proven=cleanup_proven,
        failure_class=(
            "cleanup_not_proven" if failure_class is None and not cleanup_proven else failure_class
        ),
        monitor_abort_reason=monitor_abort,
        approval_run_id=approval.run_id,
        approval_sha256=approval.sha256,
        runtime_credential_bundle_sha256=approval.runtime_credential_bundle_sha256,
        vc2_gate_ledger_sha256=vc2_ledger_sha256,
        topology_sha256=topology_digest,
        vc_device_id_redacted=_redact_device_id(config.vc_device_id),
        office_device_id_redacted=_redact_device_id(config.office_device_id),
        emperor_pid=None if emperor is None else emperor.pid,
        emperor_start_ticks=None if emperor is None else emperor.start_ticks,
        vc3_status=vc3.value,
        vc4_status=vc4.value,
        vc5_status=GateStatus.NOT_RUN.value,
        absent_first=False if cleanup is None else cleanup.absent_first,
        absent_second=False if cleanup is None else cleanup.absent_second,
    )
    _write_private_json(terminal_path, result.to_public_dict())
    return result


async def _wait_for_emperor(
    *,
    approval: SessionApproval,
    revalidate_approval: Callable[[], SessionApproval],
    validate_process: Callable[[], EmperorIdentity],
    wall_time_s: Callable[[], int],
    sleep: Callable[[float], Awaitable[None]],
) -> EmperorIdentity:
    deadline_s = approval.deadline_s - approval.pilot_runtime_s - _INTERNAL_RESERVE_S
    while wall_time_s() < deadline_s:
        _require_same_approval(approval, revalidate_approval())
        try:
            return validate_process()
        except SessionCoordinatorError as error:
            if error.code != "emperor_not_ready":
                raise
        await _bounded_sleep(
            0.25,
            deadline_s=deadline_s,
            wall_time_s=wall_time_s,
            sleep=sleep,
        )
    raise SessionCoordinatorError(
        "the approved Emperor did not become ready",
        code="emperor_not_ready",
    )


async def _wait_for_stable_topology(
    *,
    approval: SessionApproval,
    config: PilotConfig,
    emperor: EmperorIdentity,
    revalidate_approval: Callable[[], SessionApproval],
    validate_process: Callable[[], EmperorIdentity],
    topology_probe: TopologyProbe,
    wall_time_s: Callable[[], int],
    sleep: Callable[[float], Awaitable[None]],
) -> TopologySnapshot:
    deadline_s = approval.deadline_s - approval.pilot_runtime_s - _INTERNAL_RESERVE_S
    previous_bytes: bytes | None = None
    while wall_time_s() < deadline_s:
        _require_same_approval(approval, revalidate_approval())
        _require_same_process(emperor, validate_process())
        remaining = deadline_s - wall_time_s()
        try:
            observed = await asyncio.wait_for(
                topology_probe(config),
                timeout=min(_TOPOLOGY_PROBE_TIMEOUT_S, float(remaining)),
            )
            validate_topology(config, observed)
            normalized = canonical_topology_bytes(observed)
        except (PilotGuardError, TimeoutError, ConnectionError, OSError):
            await _bounded_sleep(
                _TOPOLOGY_RETRY_INTERVAL_S,
                deadline_s=deadline_s,
                wall_time_s=wall_time_s,
                sleep=sleep,
            )
            continue
        if previous_bytes is not None and normalized == previous_bytes:
            return observed
        previous_bytes = normalized
        await _bounded_sleep(
            _STABLE_OBSERVATION_INTERVAL_S,
            deadline_s=deadline_s,
            wall_time_s=wall_time_s,
            sleep=sleep,
        )
    raise SessionCoordinatorError(
        "two stable topology observations were not obtained",
        code="topology_not_stable",
    )


async def _bounded_sleep(
    duration_s: float,
    *,
    deadline_s: int,
    wall_time_s: Callable[[], int],
    sleep: Callable[[float], Awaitable[None]],
) -> None:
    remaining = deadline_s - wall_time_s()
    if remaining <= 0:
        return
    await sleep(min(duration_s, float(remaining)))


async def _run_pilot_and_monitor(
    *,
    approval: SessionApproval,
    config: PilotConfig,
    topology: TopologySnapshot,
    mqtt_password: str | None,
    paths: CoordinatorPaths,
    emperor: EmperorIdentity,
    pilot_runner: PilotRunner,
    monitor_runner: MonitorRunner,
    revalidate_approval: Callable[[], SessionApproval],
    validate_process: Callable[[], EmperorIdentity],
    wall_time_s: Callable[[], int],
) -> tuple[CleanupReport | None, str | None, str | None]:
    pilot_stop = asyncio.Event()
    monitor_stop = threading.Event()
    pilot_task = asyncio.create_task(
        pilot_runner(
            config=config,
            topology=topology,
            mqtt_host=approval.mqtt_host,
            mqtt_port=approval.mqtt_port,
            mqtt_username=approval.mqtt_username,
            mqtt_password=mqtt_password,
            stop=pilot_stop,
            install_signal_handlers=True,
        )
    )
    monitor_task = asyncio.create_task(
        monitor_runner(
            identity=emperor,
            duration_s=float(approval.pilot_runtime_s),
            output_jsonl=paths.output_root / "monitor.jsonl",
            journal_file=(paths.emperor_log_path if paths.emperor_log_path.exists() else None),
            stop=monitor_stop,
        )
    )
    guard_task = asyncio.create_task(
        _watch_active_session(
            approval=approval,
            emperor=emperor,
            revalidate_approval=revalidate_approval,
            validate_process=validate_process,
            wall_time_s=wall_time_s,
        )
    )
    cleanup: CleanupReport | None = None
    monitor_abort: str | None = None
    failure: str | None = None
    try:
        done, _ = await asyncio.wait(
            {pilot_task, monitor_task, guard_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if guard_task in done:
            try:
                failure = guard_task.result()
            except Exception:
                failure = "active_guard_failure"
            pilot_stop.set()
            try:
                cleanup = await pilot_task
            except Exception as error:
                cleanup = _cleanup_from_error(error)
                if failure is None:
                    failure = "pilot_failure"
            monitor_stop.set()
            try:
                monitor_abort = await monitor_task
            except Exception:
                if failure is None:
                    failure = "monitor_failure"
        elif monitor_task in done:
            try:
                monitor_abort = monitor_task.result()
            except Exception:
                failure = "monitor_failure"
            pilot_stop.set()
            try:
                cleanup = await pilot_task
            except Exception as error:
                cleanup = _cleanup_from_error(error)
                failure = "pilot_failure" if failure is None else failure
        else:
            try:
                cleanup = pilot_task.result()
            except Exception as error:
                cleanup = _cleanup_from_error(error)
                failure = "pilot_failure"
            monitor_stop.set()
            try:
                monitor_abort = await monitor_task
            except Exception:
                failure = "monitor_failure" if failure is None else failure
    finally:
        pilot_stop.set()
        monitor_stop.set()
        for task in (pilot_task, monitor_task, guard_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(pilot_task, monitor_task, guard_task, return_exceptions=True)
    return cleanup, monitor_abort, failure


async def _watch_active_session(
    *,
    approval: SessionApproval,
    emperor: EmperorIdentity,
    revalidate_approval: Callable[[], SessionApproval],
    validate_process: Callable[[], EmperorIdentity],
    wall_time_s: Callable[[], int],
) -> str:
    while True:
        if wall_time_s() >= approval.deadline_s:
            return "approval_deadline_elapsed"
        try:
            _require_same_approval(approval, revalidate_approval())
            _require_same_process(emperor, validate_process())
        except SessionCoordinatorError as error:
            return error.code
        await asyncio.sleep(1.0)


async def _default_monitor_runner(
    *,
    identity: EmperorIdentity,
    duration_s: float,
    output_jsonl: Path,
    journal_file: Path | None,
    stop: threading.Event,
) -> str | None:
    return await asyncio.to_thread(
        run_bounded_monitor,
        pid=identity.pid,
        duration_s=duration_s,
        interval_s=5.0,
        output_jsonl=output_jsonl,
        journal_file=journal_file,
        terminate_on_abort=False,
        stop_requested=stop.is_set,
        exclusive_output=True,
        expected_start_ticks=identity.start_ticks,
        expected_comm=identity.comm,
    )


def _cleanup_from_error(error: Exception) -> CleanupReport | None:
    report = getattr(error, "cleanup_report", None)
    return report if isinstance(report, CleanupReport) else None


def _validate_bound_session(
    *,
    approval: SessionApproval,
    config: PilotConfig,
    ledger: GateLedger,
    vc2_ledger_sha256: str,
    mqtt_password: str | None,
) -> None:
    if ledger.run_id != approval.run_id or config.runtime_s != approval.pilot_runtime_s:
        raise SessionCoordinatorError("session run identity or duration is not approval-bound")
    if vc2_ledger_sha256 != approval.vc2_gate_ledger_sha256:
        raise SessionCoordinatorError("VC2 ledger digest is not approval-bound")
    expected = (
        approval.stable_id,
        approval.display_name,
        approval.room_id,
        approval.office_device_id,
    )
    actual = (config.stable_id, config.display_name, config.room_id, config.office_device_id)
    if actual != expected:
        raise SessionCoordinatorError("one-light configuration is not approval-bound")
    if (mqtt_password is None) != (approval.mqtt_password_sha256 is None):
        raise SessionCoordinatorError("MQTT password presence is not approval-bound")
    for gate in (GateName.VC0, GateName.VC1, GateName.VC2):
        if ledger.status(gate) is not GateStatus.PASS:
            raise SessionCoordinatorError(f"{gate.value} must pass before coordination")
    for gate in (GateName.VC3, GateName.VC4, GateName.VC5):
        if ledger.status(gate) is not GateStatus.NOT_RUN:
            raise SessionCoordinatorError(f"{gate.value} must remain not run")


def _require_same_approval(expected: SessionApproval, observed: SessionApproval) -> None:
    if observed != expected:
        raise SessionCoordinatorError(
            "the consumed approval changed during the session",
            code="approval_changed",
        )


def _require_same_process(expected: EmperorIdentity, observed: EmperorIdentity) -> None:
    if observed != expected:
        raise SessionCoordinatorError(
            "the Emperor identity changed during the session",
            code="emperor_changed",
        )


def _record_readiness_gates(
    ledger: GateLedger,
    *,
    emperor: EmperorIdentity,
    topology_sha256: str,
) -> None:
    ledger.record(
        GateName.VC3,
        GateStatus.PASS,
        "Non-root isolated uWSGI Emperor identity remained stable",
        (
            Evidence(kind="emperor_pid", value=emperor.pid),
            Evidence(kind="emperor_start_ticks", value=emperor.start_ticks),
            Evidence(kind="isolated_socket", value=True),
        ),
    )
    ledger.record(
        GateName.VC4,
        GateStatus.PASS,
        "Two scoped topology observations matched ten seconds apart",
        (
            Evidence(kind="stable_observations", value=2),
            Evidence(kind="observation_interval_s", value=10),
            Evidence(kind="report", value="topology.json", sha256=topology_sha256),
        ),
    )


def _sanitized_topology_payload(
    topology: TopologySnapshot,
    *,
    config: PilotConfig,
    sha256: str,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "owner_device_id_redacted": _redact_device_id(topology.owner_device_id),
        "device_type": topology.device_type,
        "requested_room_present": config.room_id in topology.room_ids,
        "room_count": len(topology.room_ids),
        "peripherals": [
            {
                "peripheral_id": item.peripheral_id,
                "role": item.role,
                "peripheral_type": item.peripheral_type,
            }
            for item in sorted(
                topology.peripherals,
                key=lambda item: (item.peripheral_id, item.role, item.peripheral_type),
            )
        ],
        "normalized_topology_sha256": sha256,
        "stable_observations": 2,
        "observation_interval_s": 10,
    }


def _validate_empty_session_roots(paths: CoordinatorPaths, *, runtime_uid: int) -> None:
    for path, description in (
        (paths.output_root, "session output root"),
        (paths.control_root, "session control root"),
    ):
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            raise SessionCoordinatorError(f"{description} does not exist") from None
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise SessionCoordinatorError(f"{description} must be a real directory")
        if metadata.st_uid != runtime_uid or stat.S_IMODE(metadata.st_mode) != 0o700:
            raise SessionCoordinatorError(f"{description} must be service-owned mode 0700")
        if any(path.iterdir()):
            raise SessionCoordinatorError(f"{description} must be empty")
    if paths.output_root.resolve(strict=True) == paths.control_root.resolve(strict=True):
        raise SessionCoordinatorError("session output and control roots must be disjoint")


def _validate_monitor_output(
    path: Path,
    *,
    runtime_uid: int,
    identity: EmperorIdentity,
    expected_abort_reason: str | None,
) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        raise SessionCoordinatorError(
            "monitor evidence was not created",
            code="monitor_evidence_invalid",
        ) from None
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != runtime_uid
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or not 0 < metadata.st_size <= _MAX_MONITOR_BYTES
    ):
        raise SessionCoordinatorError(
            "monitor evidence is not a bounded private regular file",
            code="monitor_evidence_invalid",
        )
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        raise SessionCoordinatorError(
            "monitor evidence could not be opened safely",
            code="monitor_evidence_invalid",
        ) from None
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino):
            raise SessionCoordinatorError(
                "monitor evidence changed during open",
                code="monitor_evidence_invalid",
            )
        raw = bytearray()
        while True:
            chunk = os.read(descriptor, min(8192, _MAX_MONITOR_BYTES + 1 - len(raw)))
            if not chunk:
                break
            raw.extend(chunk)
            if len(raw) > _MAX_MONITOR_BYTES:
                raise SessionCoordinatorError(
                    "monitor evidence exceeds its bound",
                    code="monitor_evidence_invalid",
                )
        after = os.fstat(descriptor)
        if (
            after.st_size != opened.st_size
            or after.st_mtime_ns != opened.st_mtime_ns
            or after.st_ctime_ns != opened.st_ctime_ns
        ):
            raise SessionCoordinatorError(
                "monitor evidence changed while reading",
                code="monitor_evidence_invalid",
            )
    finally:
        os.close(descriptor)
    try:
        _validate_monitor_jsonl(
            bytes(raw),
            identity=identity,
            expected_abort_reason=expected_abort_reason,
        )
    finally:
        _wipe(raw)


def _validate_monitor_jsonl(
    raw: bytes,
    *,
    identity: EmperorIdentity,
    expected_abort_reason: str | None,
) -> None:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise SessionCoordinatorError(
            "monitor evidence is not UTF-8",
            code="monitor_evidence_invalid",
        ) from None
    lines = text.splitlines()
    if not text.endswith("\n") or not 1 <= len(lines) <= 1000:
        raise SessionCoordinatorError(
            "monitor evidence line count is invalid",
            code="monitor_evidence_invalid",
        )
    fields = {
        "timestamp_s",
        "pid",
        "cpu_percent",
        "rss_bytes",
        "load_average",
        "peer_count",
        "peer_add_timeouts",
        "cloud_disconnects",
        "reconnect_events",
        "mqtt_round_trip_ms",
        "physical_lag",
        "abort_reason",
    }
    allowed_abort_reasons = {
        "rss_bytes",
        "peer_add_timeouts",
        "cloud_disconnects",
        "reconnect_events",
        "physical_lag",
        "sustained_cpu_percent",
    }
    observed_abort: str | None = None
    for index, line in enumerate(lines):
        try:
            parsed = json.loads(line, object_pairs_hook=_unique_monitor_object)
        except (json.JSONDecodeError, SessionCoordinatorError):
            raise SessionCoordinatorError(
                "monitor evidence JSON is invalid",
                code="monitor_evidence_invalid",
            ) from None
        if not isinstance(parsed, dict) or set(parsed) != fields:
            raise SessionCoordinatorError(
                "monitor evidence fields are invalid",
                code="monitor_evidence_invalid",
            )
        sample = parsed
        if not _plain_nonnegative_int(sample["timestamp_s"]):
            raise _monitor_value_error()
        if type(sample["pid"]) is not int or sample["pid"] != identity.pid:
            raise _monitor_value_error()
        if not _optional_nonnegative_number(sample["cpu_percent"]):
            raise _monitor_value_error()
        if not _plain_nonnegative_int(sample["rss_bytes"]):
            raise _monitor_value_error()
        load = sample["load_average"]
        if (
            not isinstance(load, list)
            or len(load) != 3
            or any(not _nonnegative_number(value) for value in load)
        ):
            raise _monitor_value_error()
        if not (sample["peer_count"] is None or _plain_nonnegative_int(sample["peer_count"])):
            raise _monitor_value_error()
        for name in ("peer_add_timeouts", "cloud_disconnects", "reconnect_events"):
            if not _plain_nonnegative_int(sample[name]):
                raise _monitor_value_error()
        if not _optional_nonnegative_number(sample["mqtt_round_trip_ms"]):
            raise _monitor_value_error()
        if type(sample["physical_lag"]) is not bool:
            raise _monitor_value_error()
        abort_reason = sample["abort_reason"]
        if abort_reason is not None and abort_reason not in allowed_abort_reasons:
            raise _monitor_value_error()
        if abort_reason is not None:
            if index != len(lines) - 1 or observed_abort is not None:
                raise _monitor_value_error()
            observed_abort = abort_reason
    if observed_abort != expected_abort_reason:
        raise SessionCoordinatorError(
            "monitor evidence does not match its terminal result",
            code="monitor_evidence_invalid",
        )


def _unique_monitor_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise SessionCoordinatorError("monitor evidence has a duplicate field")
        result[key] = value
    return result


def _plain_nonnegative_int(value: object) -> bool:
    return type(value) is int and value >= 0


def _nonnegative_number(value: object) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(value)
        and value >= 0
    )


def _optional_nonnegative_number(value: object) -> bool:
    return value is None or _nonnegative_number(value)


def _monitor_value_error() -> SessionCoordinatorError:
    return SessionCoordinatorError(
        "monitor evidence value is invalid",
        code="monitor_evidence_invalid",
    )


def _save_private_ledger(ledger: GateLedger, path: Path) -> None:
    if path.exists() or path.is_symlink():
        raise SessionCoordinatorError("gate ledger output already exists")
    ledger.save(path)
    os.chmod(path, 0o600, follow_symlinks=False)
    _fsync_file(path)
    _fsync_directory(path.parent)


def _write_private_json(path: Path, payload: dict[str, object]) -> None:
    serialized = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
    if path.exists() or path.is_symlink():
        raise SessionCoordinatorError(f"{path.name} already exists")
    temporary = path.parent / f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp"
    descriptor: int | None = None
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
            0o600,
        )
        offset = 0
        while offset < len(serialized):
            written = os.write(descriptor, serialized[offset:])
            if written <= 0:
                raise OSError("short private evidence write")
            offset += written
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = None
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _fsync_file(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _status_identity_values(path: Path, name: str) -> tuple[int, int, int, int]:
    content = _read_bounded_text(path, maximum_bytes=64 * 1024)
    matches = [line for line in content.splitlines() if line.startswith(f"{name}:")]
    if len(matches) != 1:
        raise SessionCoordinatorError(f"Emperor {name} status is invalid")
    fields = matches[0].split()[1:]
    if len(fields) != 4:
        raise SessionCoordinatorError(f"Emperor {name} status is invalid")
    try:
        values = tuple(int(value) for value in fields)
    except ValueError:
        raise SessionCoordinatorError(f"Emperor {name} status is invalid") from None
    if len(values) != 4:
        raise SessionCoordinatorError(f"Emperor {name} status is invalid")
    return values[0], values[1], values[2], values[3]


def _read_bounded_text(path: Path, *, maximum_bytes: int) -> str:
    with path.open("rb") as handle:
        data = handle.read(maximum_bytes + 1)
    if len(data) > maximum_bytes:
        raise SessionCoordinatorError("bounded proc metadata is too large")
    try:
        return data.decode("ascii")
    except UnicodeDecodeError:
        raise SessionCoordinatorError("proc metadata is not ASCII") from None


def _load_mqtt_password(
    paths: SessionPreparePaths,
    *,
    credential_uid: int,
    runtime_gid: int,
    expected_sha256: str | None,
) -> str | None:
    if expected_sha256 is None:
        return None
    try:
        raw = _read_file(
            paths.mqtt_password_path,
            description="MQTT password",
            uid=credential_uid,
            gid=runtime_gid,
            mode=0o640,
            maximum_bytes=4 * 1024,
        )
    except RuntimePrepareError as error:
        raise SessionCoordinatorError(str(error)) from None
    try:
        if hashlib.sha256(raw).hexdigest() != expected_sha256:
            raise SessionCoordinatorError("MQTT password changed after input validation")
        try:
            password = raw.decode("utf-8").rstrip("\r\n")
        except UnicodeDecodeError:
            raise SessionCoordinatorError("MQTT password is not valid UTF-8") from None
        if not password or "\x00" in password:
            raise SessionCoordinatorError("MQTT password is invalid")
        return password
    finally:
        _wipe(raw)


def _default_coordinator_paths(session: SessionPreparePaths) -> CoordinatorPaths:
    return CoordinatorPaths(
        output_root=session.output_root,
        control_root=session.control_root,
        emperor_pid_path=session.launcher.runtime_dir / "emperor.pid",
        expected_uwsgi_path=Path("/data/switch-embedded/env/bin/uwsgi"),
        emperor_log_path=session.launcher.log_dir / "emperor.log",
    )


async def _run_loaded_session(
    loaded: LoadedCoordinatorSession,
    *,
    session_paths: SessionPreparePaths,
    paths: CoordinatorPaths,
) -> SessionCoordinatorResult:
    def revalidate_approval() -> SessionApproval:
        try:
            return validate_session_approval(
                session_paths.approval_marker_path,
                now_s=int(time.time()),
                credential_uid=0,
                runtime_gid=loaded.runtime_gid,
                allowed_paths=(session_paths.approval_marker_path,),
                phase="active",
            )
        except SessionApprovalError as error:
            raise SessionCoordinatorError(str(error), code="approval_invalid") from None

    def validate_process() -> EmperorIdentity:
        return validate_emperor_process(
            pid_file=paths.emperor_pid_path,
            expected_executable=paths.expected_uwsgi_path,
            runtime_uid=loaded.runtime_uid,
            runtime_gid=loaded.runtime_gid,
        )

    return await coordinate_session(
        approval=loaded.approval,
        config=loaded.config,
        vc2_ledger=loaded.vc2_ledger,
        vc2_ledger_sha256=loaded.vc2_ledger_sha256,
        mqtt_password=loaded.mqtt_password,
        paths=paths,
        runtime_uid=loaded.runtime_uid,
        revalidate_approval=revalidate_approval,
        validate_process=validate_process,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="run the consumed, bounded coordinated session",
    )
    args = parser.parse_args(argv)
    if not args.apply:
        print(
            json.dumps(
                {
                    "dry_run": True,
                    "started": False,
                    "blocked_reason": "explicit_apply_and_consumed_approval_required",
                },
                sort_keys=True,
            )
        )
        print("DRY RUN — no bus opened, monitor started, or light hosted")
        return 0
    try:
        runtime_uid, runtime_gid = _runtime_account()
    except RuntimePrepareError as error:
        raise SessionCoordinatorError(str(error)) from None
    session_paths = _default_session_paths()
    loaded = load_coordinator_session(
        session_paths,
        now_s=int(time.time()),
        runtime_uid=runtime_uid,
        runtime_gid=runtime_gid,
        credential_uid=0,
    )
    result = asyncio.run(
        _run_loaded_session(
            loaded,
            session_paths=session_paths,
            paths=_default_coordinator_paths(session_paths),
        )
    )
    print(json.dumps(result.to_public_dict(), sort_keys=True))
    return 0 if result.succeeded else 2


def _redact_device_id(value: str) -> str:
    return f"{value[:4]}…{value[-4:]}"


def _wipe(value: bytearray) -> None:
    for index in range(len(value)):
        value[index] = 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SessionCoordinatorError as error:
        print(f"VC coordinated session blocked: {error}", file=sys.stderr)
        sys.exit(2)
