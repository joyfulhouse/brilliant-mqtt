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
import os
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from pathlib import Path
from typing import TextIO

from brilliant_mqtt.config import Settings
from brilliant_mqtt.ha_control_protocol import encode_json
from brilliant_mqtt.heartbeat import (
    PhaseRecord,
    current_boot_id,
    process_generation,
    process_is_absent,
)
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


class AdminRefusal(Exception):
    """A request the tool refuses; the message is safe to print."""


def audit_log_path(state_path: Path) -> Path:
    """Return the append-only audit log that sits next to the outbox state file."""
    return state_path.with_name(f"{state_path.name}.outbox-audit.log")


def running_agent(phase_file: str) -> str | None:
    """Return why the agent may be running (refuse), or None when it is stopped.

    Uses the agent's existing bus-phase marker: every session stamps it with
    the agent's pid, process start tick and boot id. A marker from an earlier
    boot, a dead pid, or a reused pid proves the agent is not running.
    """
    if not phase_file:
        return "BUS_PHASE_FILE is disabled, so a running agent cannot be ruled out"
    try:
        text = Path(phase_file).read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except (OSError, UnicodeError):
        return f"cannot read the agent phase marker {phase_file}"
    record = PhaseRecord.parse(text)
    if record is None:
        return f"the agent phase marker {phase_file} is not recognised"
    if record.boot_id != current_boot_id():
        return None
    if process_is_absent(record.pid):
        return None
    generation = process_generation(record.pid)
    if generation is not None and generation != record.process_generation:
        return None
    return f"the agent (pid {record.pid}) is running; stop it first (systemctl stop)"


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
    def __init__(self, settings: Settings, out: TextIO, clock: Callable[[], float]) -> None:
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
        reason = running_agent(self._phase_file)
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
) -> int:
    """Run one admin command; return the process exit status."""
    arguments = _parser().parse_args(argv)
    stream = sys.stdout if out is None else out
    try:
        settings = Settings.from_env(env)
    except (KeyError, ValueError) as error:
        print(f"error: agent environment incomplete ({error})", file=sys.stderr)
        return 1
    admin = _Admin(settings, stream, clock)
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
