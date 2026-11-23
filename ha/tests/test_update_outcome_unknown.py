"""An unsettled production swap must never race a fresh recovery writer."""

from __future__ import annotations

import asyncio
import shlex
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import asyncssh
import pytest
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError

from custom_components.brilliant_mqtt import panel_ops
from custom_components.brilliant_mqtt.panel_health import PanelHealthError, PanelHealthObserver
from custom_components.brilliant_mqtt.panel_provisioner import PanelProvisioningError
from custom_components.brilliant_mqtt.provisioning_journal import (
    ProvisioningJournal,
    ProvisioningOperation,
    ProvisioningPhase,
    ProvisioningRecord,
    StoredJournalError,
)
from custom_components.brilliant_mqtt.shell import PanelProcess, RunResult
from custom_components.brilliant_mqtt.update_intent import IntentStage, update_intent_store
from tests.fakes import FakeShell
from tests.test_canary_rollback import _identity_for, _routed_provisioner, _stuck_ownerless_install
from tests.test_panel_health import _FakeHaMqtt
from tests.test_panel_provisioner import _FakeObserver, _health
from tests.test_provisioning_journal import _record
from tests.test_update_connect_retry import _drift_payload
from tests.test_update_intent import _intent
from tests.test_update_resume import _rig
from tests.update_harness import Shells, Spies, update_manager

pytestmark = pytest.mark.usefixtures("durable_journal_store")


@pytest.mark.parametrize("failure", ["transport", "channel", "timeout", "cancel"])
async def test_inflight_production_swap_blocks_immediate_and_restart_recovery(
    hass: HomeAssistant, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    rig = _rig(hass, tmp_path, monkeypatch)
    record = _record()
    release = tmp_path / "release-command"
    started = tmp_path / "command-started"
    process: PanelProcess | None = None
    original_run = rig.panel._do_run

    async def lose_result(command: str) -> RunResult:
        nonlocal process
        if command != panel_ops._swap_command():
            return await original_run(command)
        # Execute the actual production command in sh, paused before its moves.
        process = await rig.panel._do_start(
            f"touch {shlex.quote(str(started))}; "
            f"while [ ! -e {shlex.quote(str(release))} ]; do sleep 0.01; done; " + command
        )

        def wait_for_start() -> None:
            import time

            until = time.monotonic() + 5
            while not started.exists():
                if time.monotonic() > until:
                    raise TimeoutError("production command did not start")
                time.sleep(0.01)

        await asyncio.to_thread(wait_for_start)
        if failure == "transport":
            rig.panel.lost = True
            raise asyncssh.ConnectionLost("lost result")
        if failure == "channel":
            raise asyncssh.ChannelOpenError(1, "channel result lost")
        if failure == "timeout":
            raise TimeoutError
        raise asyncio.CancelledError

    monkeypatch.setattr(rig.panel, "_do_run", lose_result)
    try:
        with pytest.raises((PanelProvisioningError, asyncio.CancelledError)):
            async with rig.provisioner._lock:
                async with rig.provisioner.async_managed_update(
                    rig.panel,
                    record.panel_request,
                    record.fleet_profile,
                    record.staged_version,
                    subentry_id="fixture-owner",
                ):
                    guarded = await rig.journal.async_load()
                    assert guarded is not None and guarded.last_error is not None
                    assert guarded.last_error.code == "mutation_outcome_unknown"
                    persisted = await asyncio.to_thread(Path(rig.journal._store.path).read_text)
                    assert "mutation_outcome_unknown" in persisted
                    staging = Path(rig.panel.translate(panel_ops._STAGING_DIR))
                    for directory in ("app", "vendor"):
                        (staging / directory).mkdir(parents=True)
                    (staging / "vendor/late.py").write_text("candidate = True\n")
                    await panel_ops._checked(rig.panel, panel_ops._swap_command())
        assert process is not None and process.running
        assert rig.fresh == [], "an unsettled mutation cannot race fresh rollback"
        saved = await rig.journal.async_load()
        assert saved is not None and saved.last_error is not None
        assert saved.last_error.code == "mutation_outcome_unknown"
        retained = await rig.journal.async_retained(saved.transaction_id)
        assert retained is not None and retained.state == "armed"
        assert (rig.code / "vendor/original.py").exists()

        # New service and journal objects represent background recovery after restart.
        restarted = _rig(hass, tmp_path / "restart", monkeypatch)
        restarted.provisioner._journal = ProvisioningJournal(hass)
        for recover in (
            restarted.provisioner.async_recover(),
            restarted.provisioner.async_rollback(saved.transaction_id),
        ):
            with pytest.raises(PanelProvisioningError, match="mutation_outcome_unknown"):
                await recover
        assert restarted.fresh == []
        assert await rig.journal.async_load() == saved

        release.touch()
        assert (await asyncio.wait_for(process.wait(), 5)).exit_status == 0
        assert (rig.code / "vendor/late.py").exists()
        assert not (rig.code / "vendor/original.py").exists()
        # Even observed local completion cannot silently remove an operator block.
        with pytest.raises(PanelProvisioningError, match="mutation_outcome_unknown"):
            await rig.provisioner.async_recover()
        assert await rig.journal.async_load() == saved
    finally:
        release.touch()
        if process is not None:
            await asyncio.wait_for(process.wait(), 5)


@pytest.mark.parametrize("kind", ["digest", "ordinal", "selection"])
async def test_repair_retry_binds_persisted_target_across_journal_read(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
    payload_dir: Path,
    shells: Shells,
    spies: Spies,
    kind: str,
) -> None:
    manager = update_manager(hass)
    original = await manager.async_update_target()
    assert original is not None
    manager.interrupted_updates.surfaced = _intent(
        target_version=original.version,
        target_ordinal=original.ordinal,
        target_digest=original.digest,
        target_set_digest=original.set_digest,
    )

    async def journal_read() -> bool:
        if kind == "selection":
            monkeypatch.setattr(
                manager, "_identity_components", lambda: ("bridge", "wifi_watchdog")
            )
        else:
            _drift_payload(payload_dir, kind)
        return False

    monkeypatch.setattr(manager, "async_journal_owns_panel", journal_read)
    try:
        assert await manager.interrupted_updates.async_retry() is None
        await hass.async_block_till_done()
        assert await manager.async_update_target() != original
        assert spies.baselines == 0
        assert all(not shell.uploads for shell in shells.created)
        assert manager.update_last_error is not None
        assert manager.update_last_error["code"] == "release_candidate_changed"
    finally:
        await manager.async_shutdown()


async def test_shutdown_during_successful_initial_connect_prevents_deploy(
    hass: HomeAssistant,
    payload_dir: Path,
    shells: Shells,
    spies: Spies,
) -> None:
    gate = asyncio.Event()
    shell = FakeShell(connect_gate=gate)
    shells.scripted.append(shell)
    manager = update_manager(hass)
    run = asyncio.create_task(manager.async_update_agent())
    await shell.connect_entered.wait()
    await manager.async_shutdown()
    gate.set()
    try:
        with pytest.raises(HomeAssistantError):
            await run
        assert spies.baselines == 0 and not shell.uploads
        assert not shell.connected
        assert await update_intent_store(hass).async_get(manager.management_id) is not None
    finally:
        await manager.async_shutdown()


async def test_cancel_admission_intent_closes_shell(
    hass: HomeAssistant,
    payload_dir: Path,
    shells: Shells,
    spies: Spies,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = update_manager(hass)
    started = asyncio.Event()
    original = manager.interrupted_updates.async_begin_window

    async def save(
        stage: IntentStage, attempts: int, max_attempts: int, deadline: datetime
    ) -> bool:
        if stage != "connect":
            started.set()
            await asyncio.Event().wait()
        return await original(stage, attempts, max_attempts, deadline)

    monkeypatch.setattr(manager.interrupted_updates, "async_begin_window", save)
    run = asyncio.create_task(manager.async_update_agent())
    await started.wait()
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run
    assert not shells.created[0].connected
    assert spies.baselines == 0
    await manager.async_shutdown()


async def test_health_failure_is_typed_after_compensation(
    hass: HomeAssistant,
    payload_dir: Path,
    shells: Shells,
    spies: Spies,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = update_manager(hass)

    @asynccontextmanager
    async def canary(*args: object) -> AsyncIterator[UUID]:
        yield uuid4()
        raise PanelHealthError("panel_health_timeout")

    monkeypatch.setattr(manager, "_async_canary_update", canary)
    try:
        with pytest.raises(HomeAssistantError):
            await manager.async_update_agent()
        assert manager.update_last_error is not None
        assert manager.update_last_error["code"] == "health_failed"
    finally:
        await manager.async_shutdown()


@pytest.mark.parametrize("fault", ["protocol", "audit"])
async def test_admission_failure_never_readmits(
    hass: HomeAssistant,
    payload_dir: Path,
    shells: Shells,
    spies: Spies,
    fault: str,
) -> None:
    class ProtocolFault(FakeShell):
        async def _do_run(self, command: str) -> RunResult:
            if "BRILLIANT_RELEASE_IDENTITY_READ" in command:
                self.lost = True
                raise asyncssh.ProtocolError("protocol fault")
            return await super()._do_run(command)

    from tests.test_release_identity import _legacy_bridge_shell

    first = ProtocolFault() if fault == "protocol" else _legacy_bridge_shell(payload_dir)
    if fault == "audit":
        first.drop_after = "BRILLIANT_RELEASE_IDENTITY_WRITE"
    shells.scripted.extend([first, FakeShell()])
    manager = update_manager(hass)
    try:
        with pytest.raises(HomeAssistantError):
            await manager.async_update_agent()
        assert shells.connects == [1]
        assert spies.baselines == 0
        saved = await update_intent_store(hass).async_get(manager.management_id)
        assert saved is not None and saved.stage == "outcome_unknown"
        await manager.interrupted_updates.async_restore()
        assert await manager.interrupted_updates.async_retry() == "mutation_outcome_unknown"
        with pytest.raises(HomeAssistantError, match="mutation_outcome_unknown"):
            await manager.interrupted_updates.async_dismiss()
        with pytest.raises(HomeAssistantError):
            await manager.async_update_agent()
        assert shells.connects == [1]
    finally:
        await manager.async_shutdown()


async def test_connect_attempt_is_persisted_before_dispatch(
    hass: HomeAssistant,
    payload_dir: Path,
    shells: Shells,
) -> None:
    first = FakeShell(connect_gate=asyncio.Event())
    shells.scripted.append(first)
    manager = update_manager(hass)
    run = asyncio.create_task(manager.async_update_agent())
    await first.connect_entered.wait()
    try:
        saved = await update_intent_store(hass).async_get(manager.management_id)
        assert saved is not None and saved.attempts == 1
    finally:
        run.cancel()
        with pytest.raises(asyncio.CancelledError):
            await run
        await manager.async_shutdown()


async def test_shutdown_cancels_update_suspended_in_unit_read(
    hass: HomeAssistant,
    payload_dir: Path,
    shells: Shells,
    spies: Spies,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = update_manager(hass)
    entered = asyncio.Event()
    release = asyncio.Event()
    original = manager._unit_contents

    async def unit() -> str:
        entered.set()
        await release.wait()
        return await original()

    monkeypatch.setattr(manager, "_unit_contents", unit)
    update = asyncio.create_task(manager.async_update_agent())
    await entered.wait()
    await manager.async_shutdown()
    release.set()
    with pytest.raises(HomeAssistantError):
        await update
    assert not shells.created[0].uploads
    assert not shells.created[0].connected
    assert not manager._ssh_lock.locked()


@pytest.mark.parametrize("trigger", ["auto", "manual"])
@pytest.mark.parametrize("source", ["intent", "journal"])
async def test_unknown_intent_blocks_repair_on_restarted_manager(
    hass: HomeAssistant,
    payload_dir: Path,
    shells: Shells,
    spies: Spies,
    monkeypatch: pytest.MonkeyPatch,
    trigger: str,
    source: str,
) -> None:
    manager = update_manager(hass)
    if source == "intent":
        await update_intent_store(hass).async_set(
            manager.management_id, _intent(stage="outcome_unknown")
        )
    else:
        journal = ProvisioningJournal(hass)
        record = replace(_record(), operation=ProvisioningOperation.UPDATE)
        await journal.async_create(record)
        await journal.async_transition(
            record.transaction_id,
            ProvisioningPhase.ACTIVATION_PENDING,
            last_error=StoredJournalError("recovery", "mutation_outcome_unknown"),
        )
    monkeypatch.setattr(manager, "_auto_repair_still_warranted", lambda: True)
    try:
        await manager.async_repair(trigger=trigger)
        assert shells.created == []
        assert spies.baselines == 0
        assert manager.problem_reason is not None
        assert "mutation_outcome_unknown" in manager.problem_reason
    finally:
        await manager.async_shutdown()


async def test_first_failure_reload_cannot_adopt_changed_same_transaction(
    hass: HomeAssistant,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rig = _rig(hass, tmp_path, monkeypatch)
    record = _record()
    replacement = None
    with pytest.raises(PanelProvisioningError, match="provisioning_recovery_required"):
        async with rig.provisioner._lock:
            async with rig.provisioner.async_managed_update(
                rig.panel,
                record.panel_request,
                record.fleet_profile,
                record.staged_version,
                subentry_id="fixture-owner",
            ):
                active = await rig.journal.async_load()
                assert active is not None
                replacement = replace(active, setup_id=uuid4())
                # Model an exact same-UUID record replacement before the first error read.
                async with rig.journal._coordinator.lock:
                    await rig.journal._async_save(replacement)
                rig.panel.lost = True
                raise asyncssh.ConnectionLost("lost result")
    assert rig.fresh == []
    assert await rig.journal.async_load() == replacement


@pytest.mark.parametrize("operation", ["refresh", "uninstall", "component", "voice"])
async def test_unknown_intent_blocks_other_management_writers(
    hass: HomeAssistant,
    payload_dir: Path,
    shells: Shells,
    operation: str,
) -> None:
    manager = update_manager(hass)
    await update_intent_store(hass).async_set(
        manager.management_id, _intent(stage="outcome_unknown")
    )
    try:
        if operation == "refresh":
            await manager._refresh_staged_copies()
        elif operation == "uninstall":
            with pytest.raises(HomeAssistantError):
                await manager.async_uninstall()
        elif operation == "component":
            with pytest.raises(panel_ops.PanelOpError, match="mutation_outcome_unknown"):
                await manager.async_install_component("wifi_watchdog")
        else:
            with pytest.raises(HomeAssistantError):
                async with manager._voice_ssh_session():
                    pytest.fail("unknown update must block voice writes")
        assert shells.created == []
    finally:
        await manager.async_shutdown()


async def test_real_health_timeout_after_acknowledged_mutation_compensates(
    hass: HomeAssistant,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rig = _rig(hass, tmp_path, monkeypatch)
    mqtt = _FakeHaMqtt()
    mqtt.install(monkeypatch)
    observers = iter((PanelHealthObserver(hass, "office"), _FakeObserver([], _health())))
    rig.provisioner._health_observer_factory = lambda _slug: next(observers)
    rig.provisioner._health_timeout = 0.01
    record = _record()
    with pytest.raises(PanelHealthError) as raised:
        async with rig.provisioner._lock:
            async with rig.provisioner.async_managed_update(
                rig.panel,
                record.panel_request,
                record.fleet_profile,
                record.staged_version,
                subentry_id="fixture-owner",
            ):
                (rig.code / "vendor/original.py").write_text("candidate = True\n")
                await rig.panel.run("true")
    assert raised.value.code == "panel_health_timeout"
    assert rig.fresh == []
    assert await rig.journal.async_load() is None
    retained = await rig.journal.async_retained_records()
    assert len(retained) == 1 and retained[0].state == "restored"
    assert (rig.code / "vendor/original.py").read_text() == "original = True\n"
    assert not mqtt.callbacks and not mqtt.status_callbacks


@pytest.mark.parametrize("failure", ["channel", "timeout", "cancel"])
@pytest.mark.parametrize("point", ["restore", "final_cleanup"])
async def test_inflight_restore_keeps_durable_guard_and_blocks_restart(
    hass: HomeAssistant, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str, point: str
) -> None:
    rig = _rig(hass, tmp_path, monkeypatch)
    record = _record()
    observers = iter((_FakeObserver([], _health(), fail_wait=True), _FakeObserver([], _health())))
    rig.provisioner._health_observer_factory = lambda _slug: next(observers)
    original_start = rig.panel._do_start
    release = tmp_path / "release-restore"
    process: PanelProcess | None = None
    guarded_before_dispatch = False
    dispatched = ""

    async def lose_restore_result(command: str) -> PanelProcess:
        nonlocal process, guarded_before_dispatch, dispatched
        current = await rig.journal.async_load()
        if current is None or current.phase is not ProvisioningPhase.ROLLBACK_PENDING:
            return await original_start(command)
        selected = (
            "BRILLIANT_BASELINE_ACTION" in command and "'restore')" in command
            if point == "restore"
            else command.startswith("rm -rf -- ") and ".rollback-retained" in command
        )
        if not selected:
            return await original_start(command)
        persisted = await asyncio.to_thread(Path(rig.journal._store.path).read_text)
        guarded_before_dispatch = (
            current is not None
            and current.last_error is not None
            and current.last_error.code == "mutation_outcome_unknown"
            and "mutation_outcome_unknown" in persisted
        )
        # Losing start's acknowledgement does not terminate the actual restore.
        dispatched = f"while [ ! -e {shlex.quote(str(release))} ]; do sleep 0.01; done; " + command
        process = await original_start(dispatched)
        if failure == "channel":
            raise asyncssh.ChannelOpenError(1, "restore result lost")
        if failure == "timeout":
            raise TimeoutError
        raise asyncio.CancelledError

    monkeypatch.setattr(rig.panel, "_do_start", lose_restore_result)
    try:
        with pytest.raises((PanelProvisioningError, asyncio.CancelledError)):
            async with rig.provisioner._lock:
                async with rig.provisioner.async_managed_update(
                    rig.panel,
                    record.panel_request,
                    record.fleet_profile,
                    record.staged_version,
                    subentry_id="fixture-owner",
                ):
                    (rig.code / "vendor/original.py").write_text("candidate = True\n")
                    await rig.panel.run("true")
        assert process is not None and process.running
        assert guarded_before_dispatch, "restore must be durably guarded before dispatch"
        assert rig.panel.commands[-1] == dispatched, "no cleanup may race an unsettled restore"
        saved = await rig.journal.async_load()
        assert saved is not None and saved.last_error is not None
        assert saved.last_error.code == "mutation_outcome_unknown"
        assert saved.phase is ProvisioningPhase.ROLLBACK_PENDING
        restarted = _rig(hass, tmp_path / "restart", monkeypatch)
        restarted.provisioner._journal = ProvisioningJournal(hass)
        for recover in (
            restarted.provisioner.async_recover(),
            restarted.provisioner.async_rollback(saved.transaction_id),
        ):
            with pytest.raises(PanelProvisioningError, match="mutation_outcome_unknown"):
                await recover
        assert restarted.fresh == [] and rig.fresh == []
        assert await rig.journal.async_load() == saved
        assert (rig.code / "vendor/original.py").read_text() == (
            "candidate = True\n" if point == "restore" else "original = True\n"
        )
        release.touch()
        assert (await asyncio.wait_for(process.wait(), 5)).exit_status == 0
        assert (rig.code / "vendor/original.py").read_text() == "original = True\n"
        assert await rig.journal.async_load() == saved
    finally:
        release.touch()
        if process is not None:
            await asyncio.wait_for(process.wait(), 5)


async def test_unknown_restore_blocks_ownerless_verified_cleanup(
    hass: HomeAssistant, tmp_path: Path
) -> None:
    shell, record = await _stuck_ownerless_install(hass, tmp_path)
    request = record.panel_request
    calls: list[tuple[str, str]] = []
    provisioner = _routed_provisioner(hass, {(request.host, request.root_password): shell}, calls)
    identity = _identity_for(record)
    provisioner._identity_fetcher = AsyncMock(return_value=identity)
    journal = ProvisioningJournal(hass)
    await journal.async_retained_state(record.transaction_id, "restore_requested")
    await journal.async_begin_restore(record.transaction_id, request.root_password)
    current = await journal.async_load()
    assert current is not None
    guarded = await journal.async_start_restore_mutations(current)
    await journal.async_retained_state(record.transaction_id, "restored")
    with pytest.raises(PanelProvisioningError, match="mutation_outcome_unknown"):
        await provisioner.async_recover_ownerless_journal(
            guarded, request.host, request.root_password, identity
        )
    assert calls == []
    assert await journal.async_load() == guarded


@pytest.mark.parametrize("save", ["acknowledgement", "phase"])
@pytest.mark.parametrize("read_fails", [False, True])
async def test_cancel_after_durable_save_keeps_cancellation_and_record(
    hass: HomeAssistant,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    save: str,
    read_fails: bool,
) -> None:
    rig = _rig(hass, tmp_path, monkeypatch)
    rig.provisioner._journal = rig.journal
    entered = asyncio.Event()
    release = asyncio.Event()
    load = rig.journal.async_load
    finish = rig.journal.async_finish_update_mutations
    transition = rig.journal.async_transition
    saved: ProvisioningRecord | None = None

    async def acknowledge(
        _self: ProvisioningJournal, expected: ProvisioningRecord
    ) -> ProvisioningRecord:
        nonlocal saved
        result = await finish(expected)
        if save == "acknowledgement":
            saved = result
            entered.set()
            await release.wait()
        return result

    async def phase_change(
        _self: ProvisioningJournal,
        transaction: UUID,
        phase: ProvisioningPhase,
        *,
        last_error: StoredJournalError | None = None,
    ) -> ProvisioningRecord:
        nonlocal saved
        result = await transition(transaction, phase, last_error=last_error)
        if save == "phase" and phase is ProvisioningPhase.ACTIVATED:
            saved = result
            entered.set()
            await release.wait()
        return result

    async def reread(_self: ProvisioningJournal) -> ProvisioningRecord | None:
        if read_fails and entered.is_set():
            raise ValueError("SECRET storage detail")
        return await load()

    monkeypatch.setattr(ProvisioningJournal, "async_finish_update_mutations", acknowledge)
    monkeypatch.setattr(ProvisioningJournal, "async_transition", phase_change)
    monkeypatch.setattr(ProvisioningJournal, "async_load", reread)

    async def update() -> None:
        record = _record()
        async with rig.provisioner._lock:
            async with rig.provisioner.async_managed_update(
                rig.panel,
                record.panel_request,
                record.fleet_profile,
                record.staged_version,
                subentry_id="fixture-owner",
            ):
                await rig.panel.run("true")

    task = asyncio.create_task(update())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError) as raised:
        await task
    assert raised.value.__context__ is None
    assert saved is not None and saved.last_error is None
    assert await load() == saved
    assert rig.fresh == []
    assert not rig.provisioner._lock.locked()
