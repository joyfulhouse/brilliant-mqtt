"""Safety and lifecycle tests for the legacy HA-mirror cleanup command."""

from __future__ import annotations

import asyncio
import builtins
import importlib
import json
import os
import stat
import subprocess
import sys
import textwrap
from collections.abc import Callable, Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

import brilliant_mqtt.cleanup_legacy_mirror as cleanup_module
from brilliant_mqtt.cleanup_legacy_mirror import (
    ALLOWED_ID_PREFIXES,
    ALLOWED_NAME_PREFIXES,
    CleanupReport,
    NativeCleanupClient,
    OwnDeviceSnapshot,
    async_main,
    build_report,
    canonical_report_json,
    is_candidate,
    main,
    run_cleanup,
    select_candidates,
    validate_report_path,
    write_report_atomic,
)
from brilliant_mqtt.model import BrilliantDevice, DeviceKind, Variable


def _device(
    peripheral_id: str,
    name: str,
    *,
    peripheral_type: int = 27,
    variables: dict[str, Variable] | None = None,
) -> BrilliantDevice:
    return BrilliantDevice(
        device_id="own-device",
        peripheral_id=peripheral_id,
        name=name,
        kind=DeviceKind.LIGHT,
        peripheral_type=peripheral_type,
        variables=variables or {},
    )


@pytest.mark.parametrize(
    ("peripheral_id", "name"),
    [
        ("ha_kitchen", "HA Kitchen"),
        ("ha-pilot-office", "HA_PILOT_OFFICE"),
        ("zzz_mirror_lock", "ZZZ Mirror Lock"),
        ("ha_cross_prefix", "ZZZ Mirror Cross Prefix"),
    ],
)
def test_candidate_requires_any_allowlisted_prefix_on_both_dimensions(
    peripheral_id: str, name: str
) -> None:
    assert is_candidate(_device(peripheral_id, name))


@pytest.mark.parametrize(
    ("peripheral_id", "name"),
    [
        ("gangbox_peripheral_0", "SHADE HA Sconce"),
        ("ordinary_load", "HA Kitchen"),
        ("ha_kitchen", "Kitchen"),
        ("HA_kitchen", "HA Kitchen"),
        ("ha_kitchen", "ha Kitchen"),
        ("ha-pilot-office", "Ha_PILOT_OFFICE"),
        ("zzz_mirror_lock", "zzz Mirror Lock"),
        ("", "HA Empty ID"),
        ("ha_empty_name", ""),
        ("execution_peripheral_ha_pilot", "HA Execution"),
        ("configuration_virtual_device", "HA Configuration"),
        ("ble_mesh_ha_pilot", "HA Mesh"),
    ],
)
def test_candidate_near_misses_fail_closed(peripheral_id: str, name: str) -> None:
    assert not is_candidate(_device(peripheral_id, name))


@pytest.mark.parametrize(("peripheral_id", "name"), [(None, "HA X"), ("ha_x", None)])
def test_candidate_malformed_values_fail_closed(peripheral_id: object, name: object) -> None:
    malformed = _device("placeholder", "placeholder")
    malformed.peripheral_id = cast(Any, peripheral_id)
    malformed.name = cast(Any, name)

    assert not is_candidate(malformed)


def test_candidate_prefixes_are_exactly_the_approved_case_sensitive_sets() -> None:
    assert ALLOWED_ID_PREFIXES == ("ha_", "ha-pilot-", "zzz_mirror_")
    assert ALLOWED_NAME_PREFIXES == ("HA ", "HA_PILOT_", "ZZZ Mirror ")


def test_select_candidates_preserves_input_order() -> None:
    first = _device("ha_first", "HA First")
    ordinary = _device("gangbox_peripheral_0", "Lights")
    second = _device("zzz_mirror_second", "ZZZ Mirror Second")

    assert select_candidates([first, ordinary, second]) == (first, second)


def test_select_candidates_excludes_every_occurrence_of_a_duplicate_id() -> None:
    duplicate_a = _device("ha_duplicate", "HA Duplicate A")
    keep = _device("ha_keep", "HA Keep")
    duplicate_b = _device("ha_duplicate", "HA Duplicate B")

    assert select_candidates([duplicate_a, keep, duplicate_b]) == (keep,)


def test_report_is_canonical_and_contains_only_the_approved_fields() -> None:
    secret = "TOP-SECRET-variable-value"
    candidate = _device(
        "ha_kitchen",
        "HA Kitchen",
        peripheral_type=45,
        variables={"token": Variable("token", secret)},
    )
    report = build_report(
        timestamp_ms=1_700_000_000_123,
        owning_device_id="own-device",
        candidates=(candidate,),
        deleted_ids=("ha_old",),
        remaining_ids=("ha_kitchen",),
        success=False,
    )

    payload = canonical_report_json(report)

    assert payload == (
        '{"candidates":[{"id":"ha_kitchen","name":"HA Kitchen","type":45}],'
        '"deleted_ids":["ha_old"],"owning_device_id":"own-device",'
        '"remaining_ids":["ha_kitchen"],"success":false,'
        '"timestamp_ms":1700000000123}'
    )
    assert set(json.loads(payload)) == {
        "timestamp_ms",
        "owning_device_id",
        "candidates",
        "deleted_ids",
        "remaining_ids",
        "success",
    }
    assert secret not in payload
    assert "variables" not in payload
    assert "token" not in payload
    assert "Path(" not in payload


class FakeCleanupClient:
    def __init__(
        self,
        snapshots: list[OwnDeviceSnapshot | BaseException],
        *,
        delete_failure_id: str | None = None,
        snapshot_gate: asyncio.Event | None = None,
        close_gate: asyncio.Event | None = None,
    ) -> None:
        self.snapshots = list(snapshots)
        self.delete_failure_id = delete_failure_id
        self.snapshot_gate = snapshot_gate
        self.close_gate = close_gate
        self.start_calls = 0
        self.snapshot_calls = 0
        self.delete_calls: list[tuple[str, str, int]] = []
        self.close_calls = 0
        self.events: list[str] = []

    async def start(self) -> None:
        self.start_calls += 1
        self.events.append("start")

    async def snapshot_own_device(self) -> OwnDeviceSnapshot:
        self.snapshot_calls += 1
        self.events.append(f"snapshot:{self.snapshot_calls}")
        if self.snapshot_gate is not None:
            await self.snapshot_gate.wait()
        snapshot = self.snapshots.pop(0)
        if isinstance(snapshot, BaseException):
            raise snapshot
        return snapshot

    async def delete_peripheral(
        self, device_id: str, peripheral_id: str, deletion_time_ms: int
    ) -> None:
        self.delete_calls.append((device_id, peripheral_id, deletion_time_ms))
        self.events.append(f"delete:{peripheral_id}")
        if peripheral_id == self.delete_failure_id:
            raise RuntimeError("secret=/credentials/panel-password")

    async def close(self) -> None:
        self.close_calls += 1
        self.events.append("close")
        if self.close_gate is not None:
            await self.close_gate.wait()


class CancellationSuppressingCleanupClient(FakeCleanupClient):
    def __init__(self, snapshots: list[OwnDeviceSnapshot | BaseException]) -> None:
        super().__init__(snapshots)
        self.close_started = asyncio.Event()
        self.close_release = asyncio.Event()
        self.close_finished = asyncio.Event()

    async def close(self) -> None:
        self.close_calls += 1
        self.events.append("close")
        self.close_started.set()
        while not self.close_release.is_set():
            try:
                await self.close_release.wait()
            except asyncio.CancelledError:
                continue
        self.close_finished.set()


def _snapshot(*devices: BrilliantDevice) -> OwnDeviceSnapshot:
    return OwnDeviceSnapshot("own-device", tuple(devices))


def _clock(*values: int) -> Callable[[], int]:
    pending: Iterator[int] = iter(values)
    return lambda: next(pending)


async def test_dry_run_reads_once_prints_one_report_and_has_no_side_effects(
    capsys: pytest.CaptureFixture[str],
) -> None:
    candidate = _device("ha_kitchen", "HA Kitchen")
    client = FakeCleanupClient([_snapshot(candidate)])
    writes: list[tuple[Path, str]] = []
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    code = await async_main(
        [],
        client_factory=lambda: client,
        now_ms=lambda: 1234,
        sleep=fake_sleep,
        report_writer=lambda path, payload: writes.append((path, payload)),
    )

    report = json.loads(capsys.readouterr().out)
    assert code == 0
    assert report["success"] is True
    assert report["remaining_ids"] == ["ha_kitchen"]
    assert client.start_calls == 1
    assert client.snapshot_calls == 1
    assert client.delete_calls == []
    assert client.close_calls == 1
    assert sleeps == []
    assert writes == []


def test_validate_report_path_accepts_a_regular_file_in_safe_tree(tmp_path: Path) -> None:
    safe_root = tmp_path / "data" / "brilliant-mqtt" / "cleanup"
    safe_parent = safe_root / "daily"
    safe_parent.mkdir(parents=True)

    result = validate_report_path(safe_parent / "cleanup.json", safe_root=safe_root)

    assert result == safe_parent / "cleanup.json"


@pytest.mark.parametrize("kind", ["relative", "traversal", "outside", "absent_parent"])
def test_validate_report_path_rejects_unsafe_locations(tmp_path: Path, kind: str) -> None:
    safe_root = tmp_path / "cleanup"
    safe_root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    paths = {
        "relative": Path("cleanup.json"),
        "traversal": safe_root / "subdir" / ".." / "cleanup.json",
        "outside": outside / "cleanup.json",
        "absent_parent": safe_root / "absent" / "cleanup.json",
    }

    with pytest.raises(ValueError, match="unsafe report path"):
        validate_report_path(paths[kind], safe_root=safe_root)


def test_validate_report_path_rejects_the_safe_root_directory_as_a_filename(
    tmp_path: Path,
) -> None:
    safe_root = tmp_path / "cleanup"
    safe_root.mkdir()

    with pytest.raises(ValueError, match="unsafe report path"):
        validate_report_path(safe_root, safe_root=safe_root)


def test_validate_report_path_rejects_parent_symlink_escape(tmp_path: Path) -> None:
    safe_root = tmp_path / "cleanup"
    outside = tmp_path / "outside"
    safe_root.mkdir()
    outside.mkdir()
    (safe_root / "escaped").symlink_to(outside, target_is_directory=True)

    with pytest.raises(ValueError, match="unsafe report path"):
        validate_report_path(safe_root / "escaped" / "cleanup.json", safe_root=safe_root)


def test_validate_report_path_rejects_target_symlinks(tmp_path: Path) -> None:
    safe_root = tmp_path / "cleanup"
    outside = tmp_path / "outside"
    safe_root.mkdir()
    outside.mkdir()
    target = safe_root / "cleanup.json"
    target.symlink_to(outside / "captured.json")

    with pytest.raises(ValueError, match="unsafe report path"):
        validate_report_path(target, safe_root=safe_root)


def test_validate_report_path_rejects_unwritable_parent(tmp_path: Path) -> None:
    safe_root = tmp_path / "cleanup"
    safe_root.mkdir(mode=0o500)
    try:
        with pytest.raises(ValueError, match="unsafe report path"):
            validate_report_path(safe_root / "cleanup.json", safe_root=safe_root)
    finally:
        safe_root.chmod(0o700)


async def test_apply_guards_reject_before_constructing_a_client(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    safe_root = tmp_path / "cleanup"
    safe_root.mkdir()
    factory_calls = 0

    def factory() -> FakeCleanupClient:
        nonlocal factory_calls
        factory_calls += 1
        return FakeCleanupClient([])

    no_root = await async_main(
        ["--apply", "--snapshot", str(safe_root / "report.json")],
        client_factory=factory,
        effective_uid=lambda: 1000,
        safe_root=safe_root,
        now_ms=lambda: 1,
    )
    no_snapshot = await async_main(
        ["--apply"],
        client_factory=factory,
        effective_uid=lambda: 0,
        safe_root=safe_root,
        now_ms=lambda: 2,
    )
    outside = await async_main(
        ["--apply", "--snapshot", str(tmp_path / "outside.json")],
        client_factory=factory,
        effective_uid=lambda: 0,
        safe_root=safe_root,
        now_ms=lambda: 3,
    )

    output = capsys.readouterr().out.strip().splitlines()
    assert (no_root, no_snapshot, outside) == (1, 1, 1)
    assert factory_calls == 0
    assert [json.loads(line)["success"] for line in output] == [False, False, False]
    assert all(set(json.loads(line)) == CleanupReport.public_fields() for line in output)


async def test_apply_rejects_snapshot_argument_with_trailing_separator_before_client(
    capsys: pytest.CaptureFixture[str],
) -> None:
    factory_calls = 0
    validator_calls = 0

    def factory() -> FakeCleanupClient:
        nonlocal factory_calls
        factory_calls += 1
        return FakeCleanupClient([])

    def validator(path: Path, safe_root: Path) -> Path:
        nonlocal validator_calls
        del safe_root
        validator_calls += 1
        return path

    code = await async_main(
        ["--apply", "--snapshot", "/data/brilliant-mqtt/cleanup/"],
        client_factory=factory,
        effective_uid=lambda: 0,
        path_validator=validator,
        now_ms=lambda: 1,
    )

    assert code == 1
    assert factory_calls == 0
    assert validator_calls == 0
    assert json.loads(capsys.readouterr().out)["success"] is False


def test_atomic_report_write_is_compact_restrictive_and_replaces(tmp_path: Path) -> None:
    path = tmp_path / "cleanup.json"
    path.write_text("old secret", encoding="utf-8")
    path.chmod(0o644)
    payload = '{"success":true}'

    write_report_atomic(path, payload)

    assert path.read_text(encoding="utf-8") == payload + "\n"
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert list(tmp_path.iterdir()) == [path]


async def test_required_initial_report_write_failure_prevents_every_delete() -> None:
    candidate = _device("ha_kitchen", "HA Kitchen")
    client = FakeCleanupClient([_snapshot(candidate)])

    def broken_writer(path: Path, payload: str) -> None:
        del path, payload
        raise OSError("secret path=/credentials")

    with pytest.raises(OSError, match="secret path"):
        await run_cleanup(
            client,
            apply=True,
            report_path=Path("/validated/report.json"),
            now_ms=lambda: 1,
            sleep=asyncio.sleep,
            report_writer=broken_writer,
        )

    assert client.snapshot_calls == 1
    assert client.delete_calls == []


async def test_apply_deletes_serially_with_fresh_timestamps_and_between_only_sleep() -> None:
    first = _device("ha_first", "HA First")
    second = _device("zzz_mirror_second", "ZZZ Mirror Second")
    ordinary = _device("gangbox_peripheral_0", "Lights")
    client = FakeCleanupClient([_snapshot(first, ordinary, second), _snapshot(ordinary)])
    path = Path("/validated/report.json")
    writes: list[tuple[Path, str]] = []

    async def fake_sleep(seconds: float) -> None:
        client.events.append(f"sleep:{seconds}")

    def record_write(output: Path, payload: str) -> None:
        client.events.append("write")
        writes.append((output, payload))

    code, report = await run_cleanup(
        client,
        apply=True,
        report_path=path,
        now_ms=_clock(1000, 1001, 1002, 1003),
        sleep=fake_sleep,
        report_writer=record_write,
    )

    assert code == 0
    assert report.success is True
    assert report.deleted_ids == ("ha_first", "zzz_mirror_second")
    assert report.remaining_ids == ()
    assert client.delete_calls == [
        ("own-device", "ha_first", 1001),
        ("own-device", "zzz_mirror_second", 1002),
    ]
    assert client.events == [
        "snapshot:1",
        "write",
        "delete:ha_first",
        "sleep:1.0",
        "delete:zzz_mirror_second",
        "snapshot:2",
        "write",
    ]
    assert len(writes) == 2
    assert json.loads(writes[0][1])["success"] is False
    assert json.loads(writes[1][1])["success"] is False
    assert json.loads(writes[1][1])["deleted_ids"] == ["ha_first", "zzz_mirror_second"]


async def test_apply_does_not_sleep_after_a_single_delete() -> None:
    candidate = _device("ha_only", "HA Only")
    client = FakeCleanupClient([_snapshot(candidate), _snapshot()])
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    code, _ = await run_cleanup(
        client,
        apply=True,
        report_path=Path("/validated/report.json"),
        now_ms=_clock(1, 2, 3),
        sleep=fake_sleep,
        report_writer=lambda path, payload: None,
    )

    assert code == 0
    assert sleeps == []


async def test_apply_fails_when_an_original_candidate_remains_after_verification() -> None:
    candidate = _device("ha_stuck", "HA Stuck")
    client = FakeCleanupClient([_snapshot(candidate), _snapshot(candidate)])

    code, report = await run_cleanup(
        client,
        apply=True,
        report_path=Path("/validated/report.json"),
        now_ms=_clock(1, 2, 3),
        sleep=asyncio.sleep,
        report_writer=lambda path, payload: None,
    )

    assert code == 1
    assert report.deleted_ids == ()
    assert report.remaining_ids == ("ha_stuck",)
    assert report.success is False


async def test_delete_failure_stops_destruction_but_still_verifies_and_reports() -> None:
    first = _device("ha_first", "HA First")
    failed = _device("ha_failed", "HA Failed")
    untouched = _device("ha_untouched", "HA Untouched")
    client = FakeCleanupClient(
        [_snapshot(first, failed, untouched), _snapshot(failed, untouched)],
        delete_failure_id="ha_failed",
    )
    sleeps: list[float] = []
    writes: list[str] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    code, report = await run_cleanup(
        client,
        apply=True,
        report_path=Path("/validated/report.json"),
        now_ms=_clock(1, 2, 3, 4),
        sleep=fake_sleep,
        report_writer=lambda path, payload: writes.append(payload),
    )

    assert code == 1
    assert [call[1] for call in client.delete_calls] == ["ha_first", "ha_failed"]
    assert sleeps == [1.0]
    assert client.snapshot_calls == 2
    assert report.deleted_ids == ("ha_first",)
    assert report.remaining_ids == ("ha_failed", "ha_untouched")
    assert report.success is False
    assert len(writes) == 2


async def test_apply_with_no_candidates_is_idempotent_without_deletes_or_sleeps() -> None:
    ordinary = _device("gangbox_peripheral_0", "Lights")
    client = FakeCleanupClient([_snapshot(ordinary)])
    sleeps: list[float] = []
    writes: list[str] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    code, report = await run_cleanup(
        client,
        apply=True,
        report_path=Path("/validated/report.json"),
        now_ms=lambda: 1,
        sleep=fake_sleep,
        report_writer=lambda path, payload: writes.append(payload),
    )

    assert code == 0
    assert report.success is True
    assert report.candidates == ()
    assert client.snapshot_calls == 1
    assert client.delete_calls == []
    assert sleeps == []
    assert len(writes) == 1


async def test_runtime_failure_emits_only_a_sanitized_report_and_closes(
    capsys: pytest.CaptureFixture[str],
) -> None:
    candidate = _device("ha_failed", "HA Failed")
    client = FakeCleanupClient(
        [_snapshot(candidate), RuntimeError("secret=/credentials/token")],
        delete_failure_id="ha_failed",
    )
    writes: list[str] = []

    code = await async_main(
        ["--apply", "--snapshot", "/safe/cleanup/report.json"],
        client_factory=lambda: client,
        effective_uid=lambda: 0,
        safe_root=Path("/safe/cleanup"),
        path_validator=lambda path, safe_root: path,
        now_ms=_clock(1, 2, 3),
        report_writer=lambda path, payload: writes.append(payload),
    )

    output = capsys.readouterr()
    payload = json.loads(output.out)
    assert code == 1
    assert payload["success"] is False
    assert payload["remaining_ids"] == ["ha_failed"]
    assert set(payload) == CleanupReport.public_fields()
    assert "secret" not in output.out
    assert "credentials" not in output.out
    assert output.err == ""
    assert client.close_calls == 1


async def test_client_closes_after_start_or_snapshot_failure(
    capsys: pytest.CaptureFixture[str],
) -> None:
    client = FakeCleanupClient([RuntimeError("secret")])

    code = await async_main([], client_factory=lambda: client, now_ms=lambda: 1)

    assert code == 1
    assert client.close_calls == 1
    assert json.loads(capsys.readouterr().out)["success"] is False


async def test_client_closes_before_cancellation_propagates() -> None:
    snapshot_gate = asyncio.Event()
    client = FakeCleanupClient([_snapshot()], snapshot_gate=snapshot_gate)
    task = asyncio.create_task(async_main([], client_factory=lambda: client, now_ms=lambda: 1))
    await asyncio.sleep(0)

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert client.close_calls == 1


async def test_client_close_is_bounded_and_close_timeout_fails_the_command(
    capsys: pytest.CaptureFixture[str],
) -> None:
    close_gate = asyncio.Event()
    client = FakeCleanupClient([_snapshot()], close_gate=close_gate)
    writes: list[str] = []

    code = await async_main(
        ["--apply", "--snapshot", "/safe/cleanup/report.json"],
        client_factory=lambda: client,
        effective_uid=lambda: 0,
        safe_root=Path("/safe/cleanup"),
        path_validator=lambda path, safe_root: path,
        now_ms=lambda: 1,
        report_writer=lambda path, payload: writes.append(payload),
        close_timeout_s=0.001,
    )

    assert code == 1
    assert client.close_calls == 1
    assert json.loads(capsys.readouterr().out)["success"] is False
    assert json.loads(writes[-1])["success"] is False


async def test_cancellation_suppressing_client_close_has_a_hard_wall_clock_bound(
    capsys: pytest.CaptureFixture[str],
) -> None:
    client = CancellationSuppressingCleanupClient([_snapshot()])
    operation = asyncio.create_task(
        async_main([], client_factory=lambda: client, now_ms=lambda: 1, close_timeout_s=0.001)
    )

    done, _pending = await asyncio.wait({operation}, timeout=0.2)
    try:
        assert operation in done
        assert operation.result() == 1
        assert json.loads(capsys.readouterr().out)["success"] is False
        assert client.close_calls == 1
        assert not client.close_finished.is_set()
    finally:
        client.close_release.set()
        await asyncio.wait_for(client.close_finished.wait(), timeout=0.5)


async def test_cancellation_delivered_during_client_close_propagates() -> None:
    client = CancellationSuppressingCleanupClient([_snapshot()])
    operation = asyncio.create_task(
        async_main([], client_factory=lambda: client, now_ms=lambda: 1, close_timeout_s=1.0)
    )
    await asyncio.wait_for(client.close_started.wait(), timeout=0.5)

    operation.cancel()

    done, _pending = await asyncio.wait({operation}, timeout=0.2)
    try:
        assert operation in done
        with pytest.raises(asyncio.CancelledError):
            operation.result()
        assert not client.close_finished.is_set()
    finally:
        client.close_release.set()
        await asyncio.wait_for(client.close_finished.wait(), timeout=0.5)


async def test_apply_keeps_durable_failure_until_close_and_final_success_write(
    capsys: pytest.CaptureFixture[str],
) -> None:
    client = FakeCleanupClient([_snapshot()])
    accepted: list[str] = []

    def reject_success(path: Path, payload: str) -> None:
        del path
        if json.loads(payload)["success"]:
            raise OSError("final success write rejected")
        accepted.append(payload)

    code = await async_main(
        ["--apply", "--snapshot", "/safe/cleanup/report.json"],
        client_factory=lambda: client,
        effective_uid=lambda: 0,
        safe_root=Path("/safe/cleanup"),
        path_validator=lambda path, safe_root: path,
        now_ms=lambda: 1,
        report_writer=reject_success,
    )

    output = json.loads(capsys.readouterr().out)
    assert code == 1
    assert output["success"] is False
    assert client.close_calls == 1
    assert len(accepted) == 1
    assert json.loads(accepted[0])["success"] is False


async def test_apply_persists_success_only_after_client_close(
    capsys: pytest.CaptureFixture[str],
) -> None:
    client = FakeCleanupClient([_snapshot()])
    events: list[str] = []

    def record_write(path: Path, payload: str) -> None:
        del path
        events.append(f"write:{json.loads(payload)['success']}")

    original_close = client.close

    async def record_close() -> None:
        events.append("close")
        await original_close()

    client.close = record_close  # type: ignore[method-assign]
    code = await async_main(
        ["--apply", "--snapshot", "/safe/cleanup/report.json"],
        client_factory=lambda: client,
        effective_uid=lambda: 0,
        safe_root=Path("/safe/cleanup"),
        path_validator=lambda path, safe_root: path,
        now_ms=lambda: 1,
        report_writer=record_write,
    )

    assert code == 0
    assert json.loads(capsys.readouterr().out)["success"] is True
    assert events == ["write:False", "close", "write:True"]


async def test_second_report_failure_preserves_exact_verified_stdout(
    capsys: pytest.CaptureFixture[str],
) -> None:
    candidate = _device("ha_deleted", "HA Deleted")
    client = FakeCleanupClient([_snapshot(candidate), _snapshot()])
    durable: list[str] = []

    def fail_second_write(path: Path, payload: str) -> None:
        del path
        if durable:
            raise OSError("second report write failed")
        durable.append(payload)

    code = await async_main(
        ["--apply", "--snapshot", "/safe/cleanup/report.json"],
        client_factory=lambda: client,
        effective_uid=lambda: 0,
        safe_root=Path("/safe/cleanup"),
        path_validator=lambda path, safe_root: path,
        now_ms=_clock(1, 2, 3, 4),
        report_writer=fail_second_write,
    )

    stdout_report = json.loads(capsys.readouterr().out)
    durable_report = json.loads(durable[0])
    assert code == 1
    assert stdout_report["success"] is False
    assert stdout_report["candidates"] == [{"id": "ha_deleted", "name": "HA Deleted", "type": 27}]
    assert stdout_report["deleted_ids"] == ["ha_deleted"]
    assert stdout_report["remaining_ids"] == []
    assert durable_report["success"] is False
    assert durable_report["deleted_ids"] == []
    assert durable_report["remaining_ids"] == ["ha_deleted"]


def _run_synchronous_main_script(body: str) -> subprocess.CompletedProcess[str]:
    source_root = Path(__file__).parents[1] / "src"
    environment = dict(os.environ)
    old_pythonpath = environment.get("PYTHONPATH")
    environment["PYTHONPATH"] = (
        str(source_root) if not old_pythonpath else f"{source_root}{os.pathsep}{old_pythonpath}"
    )
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(body)],
        check=False,
        capture_output=True,
        text=True,
        timeout=1.0,
        env=environment,
    )


def test_synchronous_main_returns_after_cancellation_suppressing_client_close() -> None:
    completed = _run_synchronous_main_script(
        """
        import asyncio
        import time
        import brilliant_mqtt.cleanup_legacy_mirror as cleanup
        from brilliant_mqtt.cleanup_legacy_mirror import OwnDeviceSnapshot

        class Client:
            async def start(self):
                return None

            async def snapshot_own_device(self):
                return OwnDeviceSnapshot("own-device", ())

            async def delete_peripheral(self, device_id, peripheral_id, deletion_time_ms):
                raise AssertionError("dry run must not delete")

            async def close(self):
                while True:
                    try:
                        await asyncio.Event().wait()
                    except asyncio.CancelledError:
                        continue

        started = time.monotonic()
        result = cleanup.main([], client_factory=Client, close_timeout_s=0.001)
        print(f"AFTER:{result}:{time.monotonic() - started:.3f}")
        """
    )

    assert completed.returncode == 0
    assert completed.stderr == ""
    marker = completed.stdout.strip().splitlines()[-1]
    _, result, elapsed = marker.split(":")
    assert result == "1"
    assert float(elapsed) < 0.2


def test_synchronous_main_returns_after_native_child_shutdown_suppresses_cancel() -> None:
    completed = _run_synchronous_main_script(
        """
        import asyncio
        import time
        import brilliant_mqtt.cleanup_legacy_mirror as cleanup
        from brilliant_mqtt.cleanup_legacy_mirror import NativeCleanupClient, OwnDeviceSnapshot

        cleanup._NATIVE_CLOSE_STEP_TIMEOUT_S = 0.001

        class SuppressingShutdown:
            async def shutdown(self):
                while True:
                    try:
                        await asyncio.Event().wait()
                    except asyncio.CancelledError:
                        continue

        class NormalShutdown:
            async def shutdown(self):
                return None

        class Client(NativeCleanupClient):
            async def start(self):
                self._observer = SuppressingShutdown()
                self._processor = NormalShutdown()

            async def snapshot_own_device(self):
                return OwnDeviceSnapshot("own-device", ())

        started = time.monotonic()
        result = cleanup.main([], client_factory=Client)
        print(f"AFTER:{result}:{time.monotonic() - started:.3f}")
        """
    )

    assert completed.returncode == 0
    assert completed.stderr == ""
    marker = completed.stdout.strip().splitlines()[-1]
    _, result, elapsed = marker.split(":")
    assert result == "1"
    assert float(elapsed) < 0.2


def test_help_and_argument_errors_do_not_construct_a_live_client() -> None:
    calls = 0

    def factory() -> FakeCleanupClient:
        nonlocal calls
        calls += 1
        return FakeCleanupClient([])

    with pytest.raises(SystemExit) as help_exit:
        main(["--help"], client_factory=factory)
    with pytest.raises(SystemExit) as error_exit:
        main(["--unknown"], client_factory=factory)

    assert help_exit.value.code == 0
    assert error_exit.value.code != 0
    assert calls == 0


def test_import_and_help_do_not_import_panel_packages(monkeypatch: pytest.MonkeyPatch) -> None:
    real_import = builtins.__import__
    forbidden = ("lib", "thrift_types", "peripherals")

    def guarded_import(
        name: str,
        globals: dict[str, object] | None = None,
        locals: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> Any:
        if name in forbidden or name.startswith(tuple(f"{item}." for item in forbidden)):
            raise AssertionError(f"deferred panel import violated: {name}")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    module = importlib.import_module("brilliant_mqtt.cleanup_legacy_mirror")
    importlib.reload(module)

    with pytest.raises(SystemExit) as help_exit:
        module.main(["--help"])
    assert help_exit.value.code == 0


async def test_native_snapshot_is_scoped_to_the_owning_device() -> None:
    raw_peripheral = SimpleNamespace(
        name="HA Native",
        peripheral_type=27,
        variables={},
    )

    class Observer:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def get_owning_device_id(self) -> str:
            return "own-device"

        async def get_device(self, device_id: str) -> object:
            self.calls.append(device_id)
            return SimpleNamespace(peripherals={"ha_native": raw_peripheral})

        async def get_all(self) -> None:
            raise AssertionError("cleanup must not read the whole home graph")

    observer = Observer()
    client = NativeCleanupClient()
    client._observer = observer

    snapshot = await client.snapshot_own_device()

    assert observer.calls == ["own-device"]
    assert snapshot.owning_device_id == "own-device"
    assert [item.peripheral_id for item in snapshot.peripherals] == ["ha_native"]


@pytest.mark.parametrize("awaitable", [False, True])
async def test_native_delete_calls_the_processors_message_bus_client_directly(
    awaitable: bool,
) -> None:
    calls: list[tuple[str, str, int]] = []

    class MessageBusClient:
        def delete_peripheral(
            self, device_id: str, peripheral_id: str, deletion_time_ms: int
        ) -> object:
            calls.append((device_id, peripheral_id, deletion_time_ms))
            if awaitable:

                async def done() -> None:
                    return None

                return done()
            return None

    client = NativeCleanupClient()
    client._processor = SimpleNamespace(client=MessageBusClient())

    await client.delete_peripheral("own-device", "ha_native", 1234)

    assert calls == [("own-device", "ha_native", 1234)]


class _CancellationSuppressingShutdown:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.finished = asyncio.Event()
        self.calls = 0

    async def shutdown(self) -> None:
        self.calls += 1
        self.started.set()
        while not self.release.is_set():
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                continue
        self.finished.set()


class _RecordingShutdown:
    def __init__(self) -> None:
        self.calls = 0

    async def shutdown(self) -> None:
        self.calls += 1


async def test_native_shutdown_step_has_a_hard_wall_clock_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observer = _CancellationSuppressingShutdown()
    processor = _RecordingShutdown()
    client = NativeCleanupClient()
    client._observer = observer
    client._processor = processor
    monkeypatch.setattr(cleanup_module, "_NATIVE_CLOSE_STEP_TIMEOUT_S", 0.001)
    operation = asyncio.create_task(client.close())

    done, _pending = await asyncio.wait({operation}, timeout=0.2)
    try:
        assert operation in done
        with pytest.raises(RuntimeError, match="native cleanup client close failed"):
            operation.result()
        assert processor.calls == 1
        assert not observer.finished.is_set()
    finally:
        observer.release.set()
        await asyncio.wait_for(observer.finished.wait(), timeout=0.5)
        if not operation.done():
            await asyncio.wait({operation}, timeout=0.5)
        try:
            operation.result()
        except BaseException:
            pass


async def test_cancellation_during_native_shutdown_propagates_and_attempts_processor() -> None:
    observer = _CancellationSuppressingShutdown()
    processor = _RecordingShutdown()
    client = NativeCleanupClient()
    client._observer = observer
    client._processor = processor
    operation = asyncio.create_task(client.close())
    await asyncio.wait_for(observer.started.wait(), timeout=0.5)

    operation.cancel()

    done, _pending = await asyncio.wait({operation}, timeout=0.2)
    try:
        assert operation in done
        with pytest.raises(asyncio.CancelledError):
            operation.result()
        assert processor.calls == 1
        assert not observer.finished.is_set()
    finally:
        observer.release.set()
        await asyncio.wait_for(observer.finished.wait(), timeout=0.5)
        if not operation.done():
            await asyncio.wait({operation}, timeout=0.5)
        try:
            operation.result()
        except BaseException:
            pass
