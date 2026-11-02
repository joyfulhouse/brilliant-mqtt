"""Operator recovery for a quarantined scene/mode outbox record (#113).

Run ON THE PANEL with the agent stopped, using the agent's own environment so
the paths match the bridge configuration::

    systemctl stop brilliant-mqtt
    set -a; . /etc/brilliant-mqtt.env; set +a
    python3 -m brilliant_mqtt.outbox_admin list
    python3 -m brilliant_mqtt.outbox_admin release <key> <sha256>
    python3 -m brilliant_mqtt.outbox_admin discard <key> <sha256>
    systemctl start brilliant-mqtt

``release`` clears the quarantine so the record is retried. ``discard``
removes the record from the outbox (an event takes its dependent command
results with it). Both require the exact payload SHA-256 printed by ``list``,
refuse while the agent is running, and are recorded in an append-only,
size-capped audit log next to the state file. A discard is refused unless its
audit entry is durably written first. Payload contents are never printed or
logged.
"""

from __future__ import annotations

import argparse
import fcntl
import os
import subprocess
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from pathlib import Path
from typing import TextIO

from brilliant_mqtt.config import Settings
from brilliant_mqtt.ha_control_protocol import encode_json
from brilliant_mqtt.outbox_quarantine import (
    QuarantineState,
    clear_record,
    event_record_key,
    load_sidecar,
    parse_record_key,
    payload_sha256,
    result_record_key,
    retain_live,
    sidecar_path,
    write_sidecar,
)
from brilliant_mqtt.scene_state import SceneState, atomic_write_state, load_state

AUDIT_LOG_LIMIT_BYTES = 256 * 1024
AGENT_UNIT = "brilliant-mqtt"


class AdminRefusal(Exception):
    """A request the tool refuses; the message is safe to print."""


def audit_log_path(state_path: Path) -> Path:
    """Return the append-only audit log that sits next to the outbox state file."""
    return state_path.with_name(f"{state_path.name}.outbox-audit.log")


def systemctl_is_active(unit: str) -> str:
    """Return ``systemctl is-active <unit>`` output; raise when it cannot run."""
    completed = subprocess.run(
        ["systemctl", "is-active", unit],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    return completed.stdout


def _runs_agent(argv: Sequence[str]) -> bool:
    """True when *argv* runs the agent module itself (``-m brilliant_mqtt``)."""
    for index, argument in enumerate(argv):
        if argument == "-mbrilliant_mqtt":
            return True
        if argument == "-m" and index + 1 < len(argv):
            return argv[index + 1] == "brilliant_mqtt"
    return False


def _agent_pids(proc_root: Path, own_pid: int) -> list[int]:
    pids: list[int] = []
    try:
        entries = list(proc_root.iterdir())
    except OSError:
        return pids
    for entry in entries:
        if not entry.name.isdecimal() or int(entry.name) == own_pid:
            continue
        try:
            raw = (entry / "cmdline").read_bytes()
        except OSError:
            continue  # exited meanwhile or unreadable: not evidence of the agent
        argv = [part.decode("utf-8", "replace") for part in raw.split(b"\x00") if part]
        if _runs_agent(argv):
            pids.append(int(entry.name))
    return sorted(pids)


def _phase_lease_held(phase_file: str) -> bool:
    """True only if a live writer holds the agent's exclusive phase lease."""
    if not phase_file:
        return False
    try:
        descriptor = os.open(phase_file, os.O_RDONLY)
    except OSError:
        return False
    try:
        fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
    except BlockingIOError:
        return True
    except OSError:
        return False
    else:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        return False
    finally:
        os.close(descriptor)


def running_agent(
    phase_file: str,
    *,
    systemctl: Callable[[str], str] = systemctl_is_active,
    proc_root: Path = Path("/proc"),
    own_pid: int | None = None,
) -> str | None:
    """Return why the agent may be running (refuse), or None when it is stopped.

    Read-only evidence only. The agent counts as stopped only when systemd
    reports the unit ``inactive`` or ``failed`` AND no live process runs
    ``-m brilliant_mqtt``. The bus-phase marker can only add a reason to
    refuse (its exclusive lease is held); a missing, empty or unrecognised
    marker never proves anything either way.
    """
    try:
        unit_state = systemctl(AGENT_UNIT).strip()
    except (OSError, subprocess.SubprocessError) as error:
        return f"cannot query systemd for {AGENT_UNIT} ({type(error).__name__})"
    if unit_state not in ("inactive", "failed"):
        shown = unit_state or "unknown"
        return f"systemd reports {AGENT_UNIT} as {shown!r}; run systemctl stop {AGENT_UNIT}"
    pids = _agent_pids(proc_root, os.getpid() if own_pid is None else own_pid)
    if pids:
        return f"an agent process is still running (pid {pids[0]}); stop it first"
    if _phase_lease_held(phase_file):
        return f"the agent phase lease on {phase_file} is held; stop the agent first"
    return None


def _undelivered(state: SceneState) -> dict[str, tuple[str, str]]:
    """Map each undelivered record key to (record type, payload)."""
    records: dict[str, tuple[str, str]] = {}
    for key, event in state.events:
        if not event.delivered:
            records[event_record_key(key)] = ("event", event.payload)
    for (kind, command_id), result in state.results:
        if not result.delivered:
            records[result_record_key(kind, command_id)] = ("result", result.payload)
    return records


def _live(records: Mapping[str, tuple[str, str]]) -> dict[str, str]:
    return {key: payload_sha256(payload) for key, (_, payload) in records.items()}


def _status(record: str, sha256: str, quarantine: QuarantineState) -> tuple[str, int]:
    suspect = quarantine.get(record)
    if suspect is not None and suspect.sha256 == sha256:
        state = "quarantined" if suspect.quarantined_at_ms is not None else "suspect"
        return state, len(suspect.failures)
    blocked = quarantine.blocked
    if blocked is not None and blocked.record == record and blocked.sha256 == sha256:
        return "blocked", 0
    return "pending", 0


def append_audit(path: Path, entry: Mapping[str, object]) -> None:
    """Durably append one JSON line; rotate once to ``.1`` at the size cap."""
    line = (encode_json(entry) + "\n").encode("utf-8")
    try:
        size = path.stat().st_size
    except FileNotFoundError:
        size = 0
    if size and size + len(line) > AUDIT_LOG_LIMIT_BYTES:
        os.replace(path, path.with_name(f"{path.name}.1"))
    descriptor = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
    try:
        written = 0
        while written < len(line):
            written += os.write(descriptor, line[written:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class _Admin:
    def __init__(
        self,
        settings: Settings,
        out: TextIO,
        clock: Callable[[], float],
        systemctl: Callable[[str], str],
        proc_root: Path,
    ) -> None:
        self._systemctl = systemctl
        self._proc_root = proc_root
        self._state_path = Path(settings.scene_watermark_file)
        self._sidecar = sidecar_path(self._state_path)
        self._audit = audit_log_path(self._state_path)
        self._phase_file = settings.bus_phase_file
        self._out = out
        self._clock = clock

    def _load(self) -> tuple[SceneState, QuarantineState]:
        loaded = load_state(self._state_path)
        if not loaded.trusted:
            raise AdminRefusal(f"outbox state {self._state_path} is unreadable or invalid")
        return loaded.state, load_sidecar(self._sidecar)

    def _entry(self, action: str, **fields: object) -> dict[str, object]:
        now = self._clock()
        return {
            "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
            "at_ms": int(now * 1_000),
            "action": action,
            **fields,
        }

    def _audit_or_refuse(self, entry: Mapping[str, object]) -> None:
        try:
            append_audit(self._audit, entry)
        except OSError as error:
            raise AdminRefusal(
                f"cannot write the audit log {self._audit} ({type(error).__name__}); refusing"
            ) from None

    def _audit_best_effort(self, entry: Mapping[str, object]) -> None:
        try:
            append_audit(self._audit, entry)
        except OSError as error:
            print(f"warning: audit log write failed ({type(error).__name__})", file=sys.stderr)

    def _target(self, record: str, sha256: str) -> tuple[SceneState, QuarantineState, str, str]:
        reason = running_agent(
            self._phase_file, systemctl=self._systemctl, proc_root=self._proc_root
        )
        if reason is not None:
            raise AdminRefusal(reason)
        try:
            parse_record_key(record)
        except ValueError:
            raise AdminRefusal(f"{record!r} is not an outbox record key") from None
        state, quarantine = self._load()
        records = _undelivered(state)
        if record not in records:
            raise AdminRefusal(f"{record} is not an undelivered outbox record")
        record_type, payload = records[record]
        if payload_sha256(payload) != sha256:
            raise AdminRefusal(f"sha256 does not match the current payload of {record}")
        return state, quarantine, record_type, payload

    def list_records(self) -> None:
        state, quarantine = self._load()
        records = _undelivered(state)
        print("key\tkind\tbytes\tsha256\tstatus\tfailures", file=self._out)
        for record, (record_type, payload) in sorted(records.items()):
            sha256 = payload_sha256(payload)
            status, failures = _status(record, sha256, quarantine)
            print(
                f"{record}\t{record_type}\t{len(payload.encode('utf-8'))}\t{sha256}\t"
                f"{status}\t{failures}",
                file=self._out,
            )
        self._audit_best_effort(self._entry("list", records=len(records)))

    def release(self, record: str, sha256: str) -> None:
        state, quarantine, record_type, payload = self._target(record, sha256)
        status, failures = _status(record, sha256, quarantine)
        if status == "pending":
            raise AdminRefusal(f"{record} has no quarantine evidence to release")
        self._audit_or_refuse(
            self._entry(
                "release",
                key=record,
                kind=record_type,
                payload_bytes=len(payload.encode("utf-8")),
                payload_sha256=sha256,
                status=status,
                failure_count=failures,
            )
        )
        released = retain_live(clear_record(quarantine, record), _live(_undelivered(state)))
        write_sidecar(self._sidecar, released)
        print(f"released {record}", file=self._out)

    def discard(self, record: str, sha256: str) -> None:
        state, quarantine, record_type, payload = self._target(record, sha256)
        status, failures = _status(record, sha256, quarantine)
        if status != "quarantined":
            raise AdminRefusal(f"{record} is not quarantined (only quarantined records)")
        _, native = parse_record_key(record)
        events = dict(state.events)
        results = dict(state.results)
        dependents: list[str] = []
        if isinstance(native, str):
            events.pop(native)
            for result_key, result in list(results.items()):
                if result.event_key != native:
                    continue
                if result.delivered:
                    # Cannot occur in a valid file; keep it valid regardless.
                    results[result_key] = replace(result, event_key=None)
                else:
                    results.pop(result_key)
                    dependents.append(result_record_key(*result_key))
        else:
            results.pop(native)
        self._audit_or_refuse(
            self._entry(
                "discard",
                key=record,
                kind=record_type,
                payload_bytes=len(payload.encode("utf-8")),
                payload_sha256=sha256,
                failure_count=failures,
                dependents=dependents,
            )
        )
        discarded = replace(
            state,
            events=tuple(sorted(events.items())),
            results=tuple((key, results[key]) for key, _ in state.results if key in results),
        )
        atomic_write_state(self._state_path, discarded)
        cleaned = clear_record(quarantine, record)
        for dependent in dependents:
            cleaned = clear_record(cleaned, dependent)
        try:
            write_sidecar(self._sidecar, retain_live(cleaned, _live(_undelivered(discarded))))
        except OSError:
            # The record is gone; stale evidence for a missing record is ignored.
            print("warning: quarantine sidecar not updated", file=sys.stderr)
        self._audit_best_effort(self._entry("discard_done", key=record, dependents=dependents))
        suffix = f" and {len(dependents)} dependent result(s)" if dependents else ""
        print(f"discarded {record}{suffix}", file=self._out)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python3 -m brilliant_mqtt.outbox_admin",
        description="Inspect and recover a quarantined scene outbox record (agent stopped).",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="list undelivered outbox records")
    for name, help_text in (
        ("release", "clear a quarantine so the record is retried"),
        ("discard", "remove a quarantined record (an event takes its results)"),
    ):
        command = commands.add_parser(name, help=help_text)
        command.add_argument("key")
        command.add_argument("sha256")
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    env: Mapping[str, str] | None = None,
    out: TextIO | None = None,
    clock: Callable[[], float] = time.time,
    systemctl: Callable[[str], str] = systemctl_is_active,
    proc_root: Path = Path("/proc"),
) -> int:
    """Run one admin command; return the process exit status."""
    arguments = _parser().parse_args(argv)
    stream = sys.stdout if out is None else out
    try:
        settings = Settings.from_env(env)
    except (KeyError, ValueError) as error:
        print(f"error: agent environment incomplete ({error})", file=sys.stderr)
        return 1
    admin = _Admin(settings, stream, clock, systemctl, proc_root)
    try:
        if arguments.command == "list":
            admin.list_records()
        elif arguments.command == "release":
            admin.release(arguments.key, arguments.sha256)
        else:
            admin.discard(arguments.key, arguments.sha256)
    except AdminRefusal as refusal:
        print(f"refused: {refusal}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
