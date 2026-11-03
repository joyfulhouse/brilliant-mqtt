"""A verified rollback baseline that no Home Assistant record owns is retired.

Real panel recipes on the disposable RehearsalShell, both layouts: a failure
after ``capture_baseline`` and before the journal exists leaves no archive and no
release pin behind, an owned baseline is never touched, and a lost session or a
failed removal is reported as ``baseline_capture_interrupted`` without reconnecting.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Coroutine
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.helpers import json as json_util

from custom_components.brilliant_mqtt import panel_ops, panel_provisioner
from custom_components.brilliant_mqtt.panel_provisioner import (
    PanelProvisioner,
    PanelProvisioningError,
)
from custom_components.brilliant_mqtt.provisioning_journal import (
    ProvisioningJournal,
    ProvisioningRecord,
    RetainedRollback,
    Store,
)
from custom_components.brilliant_mqtt.shell import PanelProcess, PanelShell
from tests.canary_rehearsal import RehearsalProcess, RehearsalShell
from tests.test_panel_provisioner import _PUBLIC_KEY, _fleet, _Harness, _request
from tests.test_provisioning_journal import _record

_LAYOUTS = pytest.mark.parametrize("release", [False, True], ids=["legacy", "release_link"])


@pytest.fixture(autouse=True)
def durable_store(hass: HomeAssistant, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Undo the HA harness's memory-only writer so the real journal persists."""
    hass.config.config_dir = str(tmp_path / "ha-config")

    async def write(store: Store, data: dict[str, object]) -> None:
        def persist() -> None:
            Path(store.path).parent.mkdir(parents=True, exist_ok=True)
            json_util.save_json(store.path, data, private=True, atomic_writes=True)

        await hass.async_add_executor_job(persist)

    async def remove(store: Store) -> None:
        await hass.async_add_executor_job(Path(store.path).unlink, True)

    monkeypatch.setattr(Store, "_async_write_data", write)
    monkeypatch.setattr(Store, "async_remove", remove)


def rollback_entries(shell: RehearsalShell) -> list[Path]:
    parent = shell.panel / ".rollback"
    return sorted(parent.iterdir()) if parent.exists() else []


def release_pins(shell: RehearsalShell) -> list[Path]:
    return list(shell.panel.glob("releases/*/.rollback-retained"))


def assert_no_baseline(shell: RehearsalShell) -> None:
    assert rollback_entries(shell) == []
    assert release_pins(shell) == []


def assert_baseline_left(shell: RehearsalShell, *, release: bool) -> None:
    assert rollback_entries(shell) != []
    assert (release_pins(shell) != []) is release


async def panel(tmp_path: Path, *, release: bool) -> RehearsalShell:
    shell = RehearsalShell(tmp_path / "panel")
    shell.install(release=release)
    shell._pinned = _PUBLIC_KEY
    await shell.connect()
    return shell


def provisioner_for(harness: _Harness, shell: RehearsalShell) -> PanelProvisioner:
    provisioner = harness.provisioner()
    provisioner._operations = panel_ops

    def shell_factory(_host: str, _password: str, _key: str) -> PanelShell:
        harness.events.append(("shell_factory",))
        return shell

    provisioner._shell_factory = shell_factory
    return provisioner


def bridge_running(shell: RehearsalShell) -> bool:
    import json

    states: dict[str, Any] = json.loads(shell.state.read_text())
    return bool(states["brilliant-mqtt"][1])


@asynccontextmanager
async def _no_admission(
    shell: PanelShell, *_args: object, **_kwargs: object
) -> AsyncIterator[panel_ops.ReleaseAdmission]:
    """Admission needs a reviewed payload; these tests fail before any write."""
    yield panel_ops.ReleaseAdmission(shell, {}, {}, {"bridge": True}, "c" * 32)


async def run_update(provisioner: PanelProvisioner, shell: RehearsalShell) -> None:
    record = _record()
    async with provisioner._lock:
        async with provisioner.async_managed_update(
            shell,
            record.panel_request,
            record.fleet_profile,
            record.staged_version,
            subentry_id="fixture-owner",
        ):
            pytest.fail("the update must fail before its journal exists")


async def run_install(provisioner: PanelProvisioner, harness: _Harness) -> None:
    await provisioner.async_install(_request(), _fleet(), harness.progress)


def fail_action(
    monkeypatch: pytest.MonkeyPatch,
    shell: RehearsalShell,
    action: str,
    replacement: str = "exit 1",
) -> list[str]:
    """Replace one baseline action's remote command; returns the replaced commands."""
    start = shell.start
    replaced: list[str] = []

    async def patched(command: str) -> PanelProcess:
        if "BRILLIANT_BASELINE_ACTION" in command and f"'{action}'" in command:
            replaced.append(command)
            return await start(replacement)
        return await start(command)

    monkeypatch.setattr(shell, "start", patched)
    return replaced


def block_create(
    monkeypatch: pytest.MonkeyPatch, harness: _Harness
) -> tuple[asyncio.Event, asyncio.Event]:
    """Hold the journal creation (nothing durable yet) until released."""
    entered = asyncio.Event()
    release = asyncio.Event()

    async def blocked(record: ProvisioningRecord) -> ProvisioningRecord:
        entered.set()
        await release.wait()
        raise RuntimeError("journal create abandoned")

    monkeypatch.setattr(harness.journal, "async_create", blocked)
    return entered, release


# --- every failure in the unjournaled region removes the baseline ------------


_FAILURES = ("verify", "stored_snapshot", "staged_release", "journal", "cancel")


def _inject(
    failure: str,
    monkeypatch: pytest.MonkeyPatch,
    harness: _Harness,
    provisioner: PanelProvisioner,
    shell: RehearsalShell,
) -> tuple[asyncio.Event, asyncio.Event] | None:
    if failure == "verify":
        fail_action(monkeypatch, shell, "verify")
    elif failure == "stored_snapshot":

        def invalid_snapshot(_snapshot: panel_ops.PanelSnapshot) -> Any:
            raise ValueError("invalid_stored_snapshot")

        provisioner._snapshot_to_stored = invalid_snapshot
    elif failure == "staged_release":

        def invalid_staged(*_args: object) -> panel_ops.StagedRelease:
            raise panel_ops.PanelOpError("invalid_staged_release")

        provisioner._staged_release_factory = invalid_staged
    elif failure == "journal":
        harness.journal.fail_create = True
    else:
        return block_create(monkeypatch, harness)
    return None


@_LAYOUTS
@pytest.mark.parametrize("route", ["install", "update"])
@pytest.mark.parametrize("failure", _FAILURES)
async def test_failure_before_the_journal_leaves_no_archive_and_no_pin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    release: bool,
    route: str,
    failure: str,
) -> None:
    shell = await panel(tmp_path, release=release)
    harness = _Harness()
    provisioner = provisioner_for(harness, shell)
    monkeypatch.setattr(panel_ops, "release_transaction", _no_admission)
    blocked = _inject(failure, monkeypatch, harness, provisioner, shell)
    run: Callable[[], Coroutine[Any, Any, None]] = (
        (lambda: run_install(provisioner, harness))
        if route == "install"
        else (lambda: run_update(provisioner, shell))
    )
    task = asyncio.create_task(run())
    if blocked is not None:
        entered, unblock = blocked
        await entered.wait()
        task.cancel()
        await asyncio.sleep(0)
        unblock.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        with pytest.raises(Exception) as raised:
            await task
        assert "baseline_capture_interrupted" not in str(raised.value)

    assert_no_baseline(shell)
    assert not await panel_ops.baseline_retained(shell)
    assert bridge_running(shell)
    assert not shell.transport_lost()


# --- an owned or possibly owned baseline is never finalized -------------------


@_LAYOUTS
async def test_baseline_owned_by_an_armed_retained_record_is_never_finalized(
    hass: HomeAssistant,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    release: bool,
) -> None:
    shell = await panel(tmp_path, release=release)
    harness = _Harness()
    provisioner = provisioner_for(harness, shell)
    journal = ProvisioningJournal(hass)
    provisioner._journal = journal
    finalizes = fail_action(monkeypatch, shell, "finalize", "exit 0")

    async def failing_save(_journal: ProvisioningJournal, _record: ProvisioningRecord) -> None:
        raise OSError("journal save failed")

    monkeypatch.setattr(ProvisioningJournal, "_async_save", failing_save)

    with pytest.raises(OSError, match="journal save failed"):
        await run_update(provisioner, shell)

    retained: tuple[RetainedRollback, ...] = await journal.async_retained_records()
    assert len(retained) == 1 and retained[0].state == "armed"
    assert finalizes == []
    assert_baseline_left(shell, release=release)


@_LAYOUTS
async def test_unreadable_journal_counts_as_owned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, release: bool
) -> None:
    shell = await panel(tmp_path, release=release)
    harness = _Harness()
    provisioner = provisioner_for(harness, shell)
    harness.journal.fail_create = True
    finalizes = fail_action(monkeypatch, shell, "finalize", "exit 0")

    async def unreadable(_transaction_id: UUID) -> RetainedRollback | None:
        raise RuntimeError("journal_load_failed")

    monkeypatch.setattr(harness.journal, "async_retained", unreadable)

    with pytest.raises(PanelProvisioningError, match="^baseline_capture_interrupted$"):
        await run_update(provisioner, shell)

    assert finalizes == []
    assert_baseline_left(shell, release=release)


# --- failed removal, lost session, hung removal, cancellation -----------------


@_LAYOUTS
async def test_failed_removal_is_interrupted_without_reconnecting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, release: bool
) -> None:
    shell = await panel(tmp_path, release=release)
    harness = _Harness()
    provisioner = provisioner_for(harness, shell)
    harness.journal.fail_create = True
    finalizes = fail_action(monkeypatch, shell, "finalize")

    with pytest.raises(PanelProvisioningError, match="^baseline_capture_interrupted$"):
        await run_update(provisioner, shell)

    assert len(finalizes) == 1
    assert ("shell_factory",) not in harness.events
    assert shell.connect_count == 1
    assert_baseline_left(shell, release=release)
    assert bridge_running(shell)


@_LAYOUTS
async def test_lost_session_is_interrupted_without_reconnecting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, release: bool
) -> None:
    shell = await panel(tmp_path, release=release)
    harness = _Harness()
    provisioner = provisioner_for(harness, shell)
    finalizes = fail_action(monkeypatch, shell, "finalize", "exit 0")
    original_start = shell.start

    async def drop_after_verify(command: str) -> PanelProcess:
        process = await original_start(command)
        if "BRILLIANT_BASELINE_ACTION" in command and "'verify'" in command:
            await process.wait()
            raise shell._drop()
        return process

    monkeypatch.setattr(shell, "start", drop_after_verify)

    with pytest.raises(PanelProvisioningError, match="^baseline_capture_interrupted$"):
        await run_update(provisioner, shell)

    assert shell.transport_lost()
    assert finalizes == []
    assert ("shell_factory",) not in harness.events
    assert shell.connect_count == 1
    assert_baseline_left(shell, release=release)
    assert bridge_running(shell)


class _CountingProcess(RehearsalProcess):
    terminations = 0

    def terminate(self) -> None:
        type(self).terminations += 1
        super().terminate()


@_LAYOUTS
async def test_hung_removal_is_bounded_terminated_once_and_fences_the_shell(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, release: bool
) -> None:
    shell = await panel(tmp_path, release=release)
    harness = _Harness()
    provisioner = provisioner_for(harness, shell)
    harness.journal.fail_create = True
    monkeypatch.setattr(panel_provisioner, "CLEANUP_FINALIZE_SECONDS", 0.2)
    monkeypatch.setattr(_CountingProcess, "terminations", 0)
    original_start = shell._do_start

    async def counted(command: str) -> RehearsalProcess:
        if "BRILLIANT_BASELINE_ACTION" in command and "'finalize'" in command:
            hung = await original_start("sleep 30")
            return _CountingProcess(hung.process, hung.panel)
        return await original_start(command)

    monkeypatch.setattr(shell, "_do_start", counted)

    with pytest.raises(PanelProvisioningError, match="^baseline_capture_interrupted$"):
        await run_update(provisioner, shell)

    assert _CountingProcess.terminations == 1
    assert shell.connected is False, "a bounded removal must fence the shell"
    assert ("shell_factory",) not in harness.events
    assert_baseline_left(shell, release=release)
    assert bridge_running(shell)


@_LAYOUTS
async def test_cancel_during_removal_then_removal_failure_still_cancels(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, release: bool
) -> None:
    shell = await panel(tmp_path, release=release)
    harness = _Harness()
    provisioner = provisioner_for(harness, shell)
    harness.journal.fail_create = True
    marker = tmp_path / "release-finalize"
    started = asyncio.Event()
    original_start = shell.start

    async def blocked_failure(command: str) -> PanelProcess:
        if "BRILLIANT_BASELINE_ACTION" in command and "'finalize'" in command:
            started.set()
            return await original_start(f"while [ ! -e {marker} ]; do sleep 0.01; done; exit 1")
        return await original_start(command)

    monkeypatch.setattr(shell, "start", blocked_failure)
    task = asyncio.create_task(run_update(provisioner, shell))
    await started.wait()
    task.cancel()
    await asyncio.sleep(0.05)
    assert not task.done(), "a cancellation must wait for the settled removal"
    marker.touch()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert_baseline_left(shell, release=release)
    assert bridge_running(shell)


async def test_finalize_result_passes_the_original_error_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    shell = await panel(tmp_path, release=True)
    harness = _Harness()
    provisioner = provisioner_for(harness, shell)
    fail_action(monkeypatch, shell, "verify")

    with pytest.raises(panel_ops.PanelOpError, match="^baseline_verify_failed$"):
        await run_update(provisioner, shell)

    assert_no_baseline(shell)
