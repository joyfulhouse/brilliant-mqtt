"""Only missing remote acknowledgements require external mutation settlement."""

from __future__ import annotations

import asyncio
import shlex
from pathlib import Path
from unittest.mock import AsyncMock

import asyncssh
import pytest
from homeassistant.core import HomeAssistant

from custom_components.brilliant_mqtt import panel_ops
from custom_components.brilliant_mqtt.panel_provisioner import PanelProvisioningError
from custom_components.brilliant_mqtt.provisioning_journal import (
    ProvisioningJournal,
    StoredJournalError,
)
from custom_components.brilliant_mqtt.shell import PanelProcess
from tests.test_canary_rollback import (
    _identity_for,
    _routed_provisioner,
    _stuck_ownerless_install,
)
from tests.test_panel_provisioner import _FakeObserver, _fleet, _health, _request
from tests.test_provisioning_journal import _record
from tests.test_update_resume import _rig

pytestmark = pytest.mark.usefixtures("durable_journal_store")


async def test_acknowledged_nonzero_update_restores_original_bytes(
    hass: HomeAssistant, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(hass, tmp_path, monkeypatch)
    record = _record()
    original = rig.code / "vendor/original.py"
    before = original.read_bytes()
    with pytest.raises(panel_ops.PanelOpError, match="exited 23"):
        async with rig.provisioner._lock:
            async with rig.provisioner.async_managed_update(
                rig.panel,
                record.panel_request,
                record.fleet_profile,
                record.staged_version,
                subentry_id="fixture-owner",
            ) as transaction:
                await panel_ops._checked(
                    rig.panel, "printf changed > /var/brilliant-mqtt/vendor/original.py"
                )
                await panel_ops._checked(rig.panel, "exit 23")
    assert not rig.panel.transport_lost() and not rig.panel.mutation_outcome_unknown()
    assert original.read_bytes() == before
    assert await rig.journal.async_load() is None
    retained = await rig.journal.async_retained(transaction)
    assert retained is not None and retained.state == "restored"
    assert retained.recovery_deployment_id is not None
    assert rig.fresh == [], "acknowledged failure compensates using the existing session"


async def test_subscription_refusal_before_body_does_not_strand_unknown(
    hass: HomeAssistant, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(hass, tmp_path, monkeypatch)
    record = _record()
    observers: list[_FakeObserver] = []

    def observer(_slug: str) -> _FakeObserver:
        current = _FakeObserver([], _health(), fail_subscribe=not observers)
        observers.append(current)
        return current

    rig.provisioner._health_observer_factory = observer
    with pytest.raises(RuntimeError, match="health subscribe failure"):
        async with rig.provisioner._lock:
            async with rig.provisioner.async_managed_update(
                rig.panel,
                record.panel_request,
                record.fleet_profile,
                record.staged_version,
                subentry_id="fixture-owner",
            ):
                pytest.fail("a rejected subscription cannot enter the mutation body")
    assert await rig.journal.async_load() is None
    retained = await rig.journal.async_retained_records()
    assert len(retained) == 1 and retained[0].state == "restored"
    assert not rig.panel.mutation_outcome_unknown()


@pytest.mark.parametrize("failure", ["timeout", "channel", "cancel"])
async def test_ownerless_restored_cleanup_missing_result_blocks_second_recovery(
    hass: HomeAssistant, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    shell, record = await _stuck_ownerless_install(hass, tmp_path)
    request = record.panel_request
    journal = ProvisioningJournal(hass)
    provisioner = _routed_provisioner(hass, {(request.host, request.root_password): shell}, [])
    identity = _identity_for(record)
    provisioner._identity_fetcher = AsyncMock(return_value=identity)
    await journal.async_retained_state(record.transaction_id, "restore_requested")
    await journal.async_begin_restore(record.transaction_id, request.root_password)
    await journal.async_retained_state(record.transaction_id, "restored", evidence="1" * 32)
    current = await journal.async_load()
    assert current is not None and current.last_error is None
    original_start = shell._do_start
    release = tmp_path / "release-cleanup"
    process: PanelProcess | None = None
    starts = 0
    guarded_at_dispatch = False

    async def lose_cleanup_result(command: str) -> PanelProcess:
        nonlocal process, starts, guarded_at_dispatch
        if not command.startswith("rm -rf -- "):
            return await original_start(command)
        starts += 1
        guarded = await journal.async_load()
        guarded_at_dispatch = guarded is not None and guarded.last_error == StoredJournalError(
            "recovery", "mutation_outcome_unknown"
        )
        process = await original_start(
            f"while [ ! -e {shlex.quote(str(release))} ]; do sleep 0.01; done; " + command
        )
        if failure == "cancel":
            raise asyncio.CancelledError
        if failure == "channel":
            raise asyncssh.ChannelOpenError(1, "lost cleanup acknowledgement")
        raise TimeoutError("lost cleanup acknowledgement")

    monkeypatch.setattr(shell, "_do_start", lose_cleanup_result)
    try:
        with pytest.raises((PanelProvisioningError, asyncio.CancelledError)) as raised:
            await provisioner.async_recover_ownerless_journal(
                current, request.host, request.root_password, identity
            )
        if failure == "cancel":
            assert isinstance(raised.value, asyncio.CancelledError)
        else:
            assert isinstance(raised.value, PanelProvisioningError)
            assert raised.value.code == "mutation_outcome_unknown"
        assert process is not None and process.running
        saved = await journal.async_load()
        assert saved is not None
        assert guarded_at_dispatch, "cleanup needs a durable guard before remote dispatch"
        assert saved.last_error == StoredJournalError("recovery", "mutation_outcome_unknown")
        with pytest.raises(PanelProvisioningError, match="mutation_outcome_unknown"):
            await provisioner.async_recover_ownerless_journal(
                saved, request.host, request.root_password, identity
            )
        assert starts == 1 and process.running
        assert await journal.async_load() == saved
        retained = await journal.async_retained(record.transaction_id)
        assert retained is not None and retained.state == "restored"
        assert not shell.connected
    finally:
        release.touch()
        if process is not None:
            await asyncio.wait_for(process.wait(), 5)


async def test_ownerless_restored_cleanup_retires_guard_after_positive_ack(
    hass: HomeAssistant, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    shell, record = await _stuck_ownerless_install(hass, tmp_path)
    request = record.panel_request
    journal = ProvisioningJournal(hass)
    provisioner = _routed_provisioner(hass, {(request.host, request.root_password): shell}, [])
    identity = _identity_for(record)
    provisioner._identity_fetcher = AsyncMock(return_value=identity)
    await journal.async_retained_state(record.transaction_id, "restore_requested")
    await journal.async_begin_restore(record.transaction_id, request.root_password)
    await journal.async_retained_state(record.transaction_id, "restored", evidence="1" * 32)
    current = await journal.async_load()
    assert current is not None
    original_start = shell._do_start
    guarded_at_dispatch = False

    async def inspect_cleanup(command: str) -> PanelProcess:
        nonlocal guarded_at_dispatch
        if command.startswith("rm -rf -- "):
            guarded = await journal.async_load()
            guarded_at_dispatch = guarded is not None and guarded.last_error == StoredJournalError(
                "recovery", "mutation_outcome_unknown"
            )
        return await original_start(command)

    monkeypatch.setattr(shell, "_do_start", inspect_cleanup)
    await provisioner.async_recover_ownerless_journal(
        current, request.host, request.root_password, identity
    )
    assert guarded_at_dispatch
    assert await journal.async_load() is None
    retained = await journal.async_retained(record.transaction_id)
    assert retained is not None and retained.state == "restored"


@pytest.mark.parametrize(
    "entry",
    [
        "install",
        "recover",
        "recover_expected",
        "rollback",
        "finalize",
        "ownerless_recover",
        "ownerless_finalize",
        "ownerless_discard",
    ],
)
@pytest.mark.parametrize("code", ["mutation_outcome_unknown", "provisioning_recovery_required"])
async def test_mutating_entry_checks_global_intent_guard_under_operation_lock(
    hass: HomeAssistant, tmp_path: Path, entry: str, code: str
) -> None:
    shell, record = await _stuck_ownerless_install(hass, tmp_path)
    request = record.panel_request
    calls: list[tuple[str, str]] = []
    provisioner = _routed_provisioner(hass, {(request.host, request.root_password): shell}, calls)
    journal = ProvisioningJournal(hass)
    retained = await journal.async_retained(record.transaction_id)
    assert retained is not None
    commands = list(shell.commands)
    checked = False

    async def refuse() -> None:
        nonlocal checked
        assert provisioner._lock.locked()
        checked = True
        raise panel_ops.PanelOpError(code)

    provisioner._mutation_guard = refuse
    identity = _identity_for(record)
    with pytest.raises(PanelProvisioningError, match=code):
        if entry == "install":
            await provisioner.async_install(_request(), _fleet(), AsyncMock())
        elif entry == "recover":
            await provisioner.async_recover()
        elif entry == "recover_expected":
            await provisioner.async_recover(expected_record=record)
        elif entry == "rollback":
            await provisioner.async_rollback(record.transaction_id)
        elif entry == "finalize":
            await provisioner.async_finalize(record.transaction_id)
        elif entry == "ownerless_recover":
            await provisioner.async_recover_ownerless_journal(
                record, request.host, request.root_password, identity
            )
        elif entry == "ownerless_finalize":
            await provisioner.async_finalize_ownerless_restored(
                retained, request.host, request.root_password, identity
            )
        else:
            await provisioner.async_discard_unactivated_ownerless_baseline(
                retained, request.host, request.root_password, identity
            )
    assert checked
    assert not provisioner._lock.locked()
    assert calls == [] and shell.commands == commands
    assert await journal.async_load() == record
    assert await journal.async_retained(record.transaction_id) == retained
