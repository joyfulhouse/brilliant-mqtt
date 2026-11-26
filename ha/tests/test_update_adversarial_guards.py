"""Production regressions for interrupted update admission and writer exclusion."""

from __future__ import annotations

import asyncio
import errno
import shlex
import threading
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import json as json_util

from custom_components.brilliant_mqtt import fleet_manager, panel_ops
from custom_components.brilliant_mqtt import manager as manager_module
from custom_components.brilliant_mqtt.const import PANEL_WIFI_WATCHDOG_DIR
from custom_components.brilliant_mqtt.entry_data import LegacyPanelStore
from custom_components.brilliant_mqtt.panel_provisioner import PanelProvisioningError
from custom_components.brilliant_mqtt.shell import PanelProcess, RunResult
from custom_components.brilliant_mqtt.update_intent import (
    IntentStage,
    InterruptedUpdates,
    UpdateIntentStore,
    UpdateTarget,
    async_assert_no_unknown_update,
    async_forget_update_intent,
    update_intent_store,
)
from tests.fakes import FakeShell
from tests.test_provisioning_journal import _record
from tests.test_release_identity import _legacy_bridge_shell
from tests.test_update_connect_retry import _drift_payload
from tests.test_update_intent import _TARGET, _intent, _Panel
from tests.test_update_resume import _rig
from tests.update_harness import Clock, Shells, Spies, update_manager

pytestmark = pytest.mark.usefixtures("durable_journal_store")


@pytest.mark.parametrize("missing_at", ["retry", "fallback_inspection", "fallback_restart"])
async def test_acknowledged_watchdog_failure_then_missing_result_stops_fallback(
    hass: HomeAssistant,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    payload_dir: Path,
    missing_at: str,
) -> None:
    rig = _rig(hass, tmp_path, monkeypatch)
    manager = update_manager(hass)
    record = _record()
    monkeypatch.setattr(manager_module, "_WATCHDOG_RELAY_RETRY_DELAY_S", 0)
    inspections = restarts = 0

    async def inspect(_shell: object) -> SimpleNamespace:
        nonlocal inspections
        inspections += 1
        if missing_at == "fallback_inspection" and inspections == 3:
            rig.panel.mark_mutation_unknown()
            raise TimeoutError("inspection result missing")
        return SimpleNamespace(payload_present=inspections > 1, version=record.staged_version)

    async def restart(_shell: object) -> None:
        nonlocal restarts
        restarts += 1
        if restarts == (2 if missing_at == "retry" else 3):
            rig.panel.mark_mutation_unknown()
            raise TimeoutError("restart result missing")
        raise panel_ops.PanelOpError("restart exited 1")

    spec = replace(
        manager_module._WIFI_WATCHDOG_RELAY,
        inspect=inspect,
        deploy=AsyncMock(),
        ensure_unit=AsyncMock(),
        enable=AsyncMock(),
        restart=restart,
    )
    immediate_error: str | None = None
    try:
        with pytest.raises(PanelProvisioningError, match="mutation_outcome_unknown"):
            async with rig.provisioner._lock:
                async with rig.provisioner.async_managed_update(
                    rig.panel,
                    record.panel_request,
                    record.fleet_profile,
                    record.staged_version,
                    subentry_id="fixture-owner",
                ):
                    try:
                        await manager._relay_watchdog(rig.panel, spec, record.staged_version)
                    except panel_ops.PanelOpError as error:
                        immediate_error = str(error)
                        raise
        assert immediate_error == "mutation_outcome_unknown"
        assert restarts == (3 if missing_at == "fallback_restart" else 2)
        assert inspections == (2 if missing_at == "retry" else 3)
        saved = await rig.journal.async_load()
        assert saved is not None and saved.last_error is not None
        assert saved.last_error.code == "mutation_outcome_unknown"
        assert rig.fresh == []
    finally:
        await manager.async_shutdown()


@pytest.mark.parametrize("operation", ["recover", "rollback", "finalize", "install"])
async def test_real_provisioner_factory_wires_global_unknown_guard(
    hass: HomeAssistant, operation: str
) -> None:
    await update_intent_store(hass).async_set("old-owner", _intent(stage="outcome_unknown"))
    provisioner = fleet_manager._build_panel_provisioner(hass, AsyncMock())
    with pytest.raises(PanelProvisioningError, match="mutation_outcome_unknown"):
        if operation == "recover":
            await provisioner.async_recover()
        elif operation == "rollback":
            await provisioner.async_rollback(_record().transaction_id)
        elif operation == "finalize":
            await provisioner.async_finalize(_record().transaction_id)
        else:
            async with provisioner._async_install_lock():
                pytest.fail("an unknown intent must block install before inspection")


async def test_live_handoff_requires_exact_row_current_task_and_no_other_unknown(
    hass: HomeAssistant,
) -> None:
    panel = _Panel(hass)
    store = update_intent_store(hass)
    interrupted = InterruptedUpdates(panel, store)
    interrupted.bind_target(_TARGET)
    await interrupted.async_begin_window("outcome_unknown", 1, 5, datetime.now(UTC))
    await interrupted.async_assert_live_handoff()
    with pytest.raises(panel_ops.PanelOpError, match="mutation_outcome_unknown"):
        await async_assert_no_unknown_update(hass)
    with pytest.raises(panel_ops.PanelOpError, match="provisioning_recovery_required"):
        await asyncio.create_task(interrupted.async_assert_live_handoff())
    await store.async_set("other", _intent(stage="outcome_unknown"))
    with pytest.raises(panel_ops.PanelOpError, match="mutation_outcome_unknown"):
        await interrupted.async_assert_live_handoff()
    await store._store.async_save(
        {"intents": {panel.management_id: _intent(stage="outcome_unknown", attempts=2).as_dict()}}
    )
    with pytest.raises(panel_ops.PanelOpError, match="provisioning_recovery_required"):
        await interrupted.async_assert_live_handoff()


async def test_manager_live_handoff_reaches_real_managed_capture(
    hass: HomeAssistant, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(hass, tmp_path, monkeypatch)
    record = _record()
    manager = manager_module.PanelManager(
        hass,
        LegacyPanelStore(hass, rig.owner),
        fleet_manager.legacy_fleet_config(rig.owner),
        rig.provisioner._lock,
    )
    manager._interrupted.bind_target(_TARGET)
    await manager._interrupted.async_begin_window("outcome_unknown", 1, 5, datetime.now(UTC))
    monkeypatch.setattr(fleet_manager, "_get_recovery_provisioner", lambda _hass: rig.provisioner)
    try:
        async with rig.provisioner._lock:
            async with manager._async_canary_update(
                rig.panel, record.staged_version
            ) as transaction:
                await manager._async_end_update_window()
                await rig.panel.run("true")
        assert await rig.journal.async_load() is None
        retained = await rig.journal.async_retained(transaction)
        assert retained is not None and retained.state == "soak"
        assert rig.fresh == []
    finally:
        await manager.async_shutdown()


async def test_watchdog_lost_swap_is_last_dispatch(
    hass: HomeAssistant, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, payload_dir: Path
) -> None:
    rig = _rig(hass, tmp_path, monkeypatch)
    manager = update_manager(hass)
    record = _record()
    monkeypatch.setattr(manager_module, "_WATCHDOG_RELAY_RETRY_DELAY_S", 0)
    gate = tmp_path / "release-old-wifi"
    live = Path(rig.panel.translate(PANEL_WIFI_WATCHDOG_DIR))
    staging = Path(rig.panel.translate(panel_ops._WATCHDOG_STAGING_DIR))
    command = panel_ops._watchdog_swap_command()
    process: PanelProcess | None = None
    swaps = 0
    original = rig.panel._do_run

    async def lose_result(value: str) -> RunResult:
        nonlocal process, swaps
        if value == command:
            swaps += 1
            if swaps == 1:
                process = await rig.panel._do_start(
                    f"while [ ! -e {shlex.quote(str(gate))} ]; do sleep .01; done; " + value
                )
                raise TimeoutError("result lost while transport remains alive")
        return await original(value)

    async def deploy(_shell: object, *_args: object) -> None:
        await asyncio.to_thread(staging.mkdir, exist_ok=True, parents=True)
        (staging / "selected").write_text("new")
        await panel_ops._checked(rig.panel, command)

    ensure_unit, enable, restart = AsyncMock(), AsyncMock(), AsyncMock()
    spec = replace(
        manager_module._WIFI_WATCHDOG_RELAY,
        inspect=AsyncMock(return_value=SimpleNamespace(payload_present=False, version=None)),
        deploy=deploy,
        ensure_unit=ensure_unit,
        enable=enable,
        restart=restart,
    )
    monkeypatch.setattr(rig.panel, "_do_run", lose_result)
    try:
        with pytest.raises(PanelProvisioningError, match="mutation_outcome_unknown"):
            async with rig.provisioner._lock:
                async with rig.provisioner.async_managed_update(
                    rig.panel,
                    record.panel_request,
                    record.fleet_profile,
                    record.staged_version,
                    subentry_id="fixture-owner",
                ):
                    await manager._relay_watchdog(rig.panel, spec, record.staged_version)
        assert process is not None and process.running
        assert swaps == 1
        assert command in rig.panel.commands[-1]
        assert not await asyncio.to_thread(live.exists)
        assert not ensure_unit.await_count
        assert not enable.await_count
        assert not restart.await_count
        gate.touch()
        assert (await asyncio.wait_for(process.wait(), 5)).exit_status == 0
        assert (live / "selected").read_text() == "new"
        saved = await rig.journal.async_load()
        assert saved is not None and saved.last_error is not None
        assert saved.last_error.code == "mutation_outcome_unknown"
    finally:
        gate.touch()
        if process is not None:
            await asyncio.wait_for(process.wait(), 5)
        await manager.async_shutdown()


@pytest.mark.parametrize("operation", ["set", "clear"])
@pytest.mark.parametrize("independent_store", [False, True])
async def test_cancelled_intent_writer_settles_before_other_panel_save(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    independent_store: bool,
) -> None:
    store = update_intent_store(hass)
    await store.async_set("a", _intent(attempts=0))
    path = Path(store._store.path)
    gate, entered = threading.Event(), threading.Event()

    def write(blob: dict[str, object]) -> None:
        data = blob["data"]
        assert isinstance(data, dict)
        if "b" not in data["intents"]:
            entered.set()
            assert gate.wait(10)
        json_util.save_json(str(path), blob, private=True, atomic_writes=True)

    async def persist(blob: dict[str, object]) -> None:
        await hass.async_add_executor_job(write, blob)

    monkeypatch.setattr(store._store, "_async_write_data", persist)
    pending = asyncio.create_task(
        store.async_set("a", _intent(attempts=1)) if operation == "set" else store.async_clear("a")
    )
    other: asyncio.Task[None] | None = None
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        pending.cancel()
        writer = UpdateIntentStore(hass) if independent_store else store
        other = asyncio.create_task(
            writer.async_set("b", _intent(panel="other", stage="outcome_unknown"))
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert not pending.done(), "cancellation must retain the lock until the worker settles"
        assert not other.done()
        gate.set()
        with pytest.raises(asyncio.CancelledError):
            await pending
        await other
        fresh = UpdateIntentStore(hass)
        assert await fresh.async_get("a") == (_intent(attempts=1) if operation == "set" else None)
        assert await fresh.async_get("b") == _intent(panel="other", stage="outcome_unknown")
        assert await store.async_get("b") == await fresh.async_get("b")
    finally:
        gate.set()
        await asyncio.gather(
            pending, *([other] if other is not None else []), return_exceptions=True
        )


@pytest.mark.parametrize("malformation", ["row", "mapping", "extra", "deadline"])
async def test_invalid_inner_intent_blocks_writers_and_preserves_bytes(
    hass: HomeAssistant, malformation: str
) -> None:
    manager = update_manager(hass)
    store = update_intent_store(hass)
    row = _intent(stage="outcome_unknown").as_dict()
    if malformation == "deadline":
        row.pop("deadline")
    payload: dict[str, object] = {"intents": {manager.management_id: row}}
    if malformation == "row":
        payload = {"intents": {manager.management_id: "invalid"}}
    elif malformation == "mapping":
        payload = {"intents": []}
    elif malformation == "extra":
        payload["extra"] = True
    await store._store.async_save(payload)
    before = await asyncio.to_thread(Path(store._store.path).read_bytes)
    try:
        with pytest.raises(panel_ops.PanelOpError, match="provisioning_recovery_required"):
            await manager._async_assert_update_settled()
        with pytest.raises(HomeAssistantError, match="provisioning_recovery_required"):
            await store.async_set("other", _intent(panel="other"))
        with pytest.raises(HomeAssistantError, match="provisioning_recovery_required"):
            await store.async_clear(manager.management_id)
        assert await asyncio.to_thread(Path(store._store.path).read_bytes) == before
    finally:
        await manager.async_shutdown()


async def test_unknown_guard_survives_new_management_identity(
    hass: HomeAssistant, payload_dir: Path, spies: Spies, shells: Shells
) -> None:
    old = update_manager(hass)
    store = update_intent_store(hass)
    await store.async_set(old.management_id, _intent(stage="outcome_unknown"))
    await old.async_shutdown()
    await async_forget_update_intent(hass, old.management_id)
    assert isinstance(old.store, LegacyPanelStore)
    hass.config_entries._entries.pop(old.store.entry.entry_id)
    new = update_manager(hass)
    try:
        assert old.store.data == new.store.data and old.management_id != new.management_id
        with pytest.raises(HomeAssistantError):
            await new.async_update_agent()
        assert spies.baselines == 0 and shells.created == []
        assert await store.async_get(old.management_id) == _intent(stage="outcome_unknown")
    finally:
        await new.async_shutdown()


async def test_production_named_rollback_observes_global_intent_guard(
    hass: HomeAssistant, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(hass, tmp_path, monkeypatch)
    record = _record()
    manager = manager_module.PanelManager(
        hass,
        LegacyPanelStore(hass, rig.owner),
        fleet_manager.legacy_fleet_config(rig.owner),
        rig.provisioner._lock,
    )
    async with rig.provisioner._lock:
        async with rig.provisioner.async_managed_update(
            rig.panel,
            record.panel_request,
            record.fleet_profile,
            record.staged_version,
            subentry_id="fixture-owner",
        ) as transaction:
            await rig.panel.run("true")
    await update_intent_store(hass).async_set(
        "different-management-id", _intent(stage="outcome_unknown")
    )
    monkeypatch.setattr(fleet_manager, "_get_recovery_provisioner", lambda _hass: rig.provisioner)
    try:
        with pytest.raises(HomeAssistantError, match="mutation_outcome_unknown"):
            await manager.async_canary_operation("rollback", str(transaction))
        assert rig.fresh == []
        retained = await rig.journal.async_retained(transaction)
        assert retained is not None and retained.state == "soak"
    finally:
        await manager.async_shutdown()


@pytest.mark.parametrize("pause", ["save", "settlement_read"])
async def test_retry_deadline_rechecked_after_last_persistence_await(
    hass: HomeAssistant,
    payload_dir: Path,
    clock: Clock,
    shells: Shells,
    spies: Spies,
    monkeypatch: pytest.MonkeyPatch,
    pause: str,
) -> None:
    manager = update_manager(hass)
    shells.scripted.append(FakeShell(connect_error=OSError(errno.EHOSTUNREACH, "unreachable")))
    write, settled = manager._async_write_update_window, manager._async_assert_update_settled

    async def save(stage: IntentStage, budget: manager_module._UpdateRetryBudget) -> None:
        await write(stage, budget)
        if pause == "save" and stage == "connect" and budget.attempts == 2:
            clock.now += 130

    async def read() -> None:
        await settled()
        if (
            pause == "settlement_read"
            and manager._update_budget is not None
            and manager._update_budget.attempts == 2
        ):
            clock.now += 130

    monkeypatch.setattr(manager, "_async_write_update_window", save)
    monkeypatch.setattr(manager, "_async_assert_update_settled", read)
    try:
        with pytest.raises(HomeAssistantError):
            await manager.async_update_agent()
        assert len(shells.created) == 1 and spies.baselines == 0
    finally:
        await manager.async_shutdown()


@pytest.mark.parametrize("drift", ["digest", "ordinal", "selection"])
async def test_exact_target_refused_before_any_admission_audit(
    hass: HomeAssistant,
    payload_dir: Path,
    shells: Shells,
    spies: Spies,
    monkeypatch: pytest.MonkeyPatch,
    drift: str,
) -> None:
    manager = update_manager(hass)
    shell = _legacy_bridge_shell(payload_dir)
    shells.scripted.append(shell)
    original = manager.async_update_target

    async def change() -> UpdateTarget | None:
        target = await original()
        if drift == "selection":
            monkeypatch.setattr(
                manager, "_identity_components", lambda: ("bridge", "wifi_watchdog")
            )
        elif drift == "ordinal":
            (payload_dir / "RELEASE_ORDINAL").write_text("3\n")
        else:
            _drift_payload(payload_dir, drift)
        return target

    monkeypatch.setattr(manager, "async_update_target", change)
    try:
        with pytest.raises(HomeAssistantError):
            await manager.async_update_agent()
        assert spies.baselines == 0
        assert shell.identity_uploads == []
        assert not any("BRILLIANT_RELEASE_IDENTITY_WRITE" in command for command in shell.commands)
        assert manager.update_last_error is not None
        assert manager.update_last_error["code"] == "release_candidate_changed"
    finally:
        await manager.async_shutdown()
