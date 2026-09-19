"""Phase B regression-first complete baseline and durable recovery contract."""

from __future__ import annotations

import asyncio
import json
import shutil
import time
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.helpers import json as json_util

from custom_components.brilliant_mqtt import panel_ops
from custom_components.brilliant_mqtt.panel_health import PanelHealthError, PanelHealthObserver
from custom_components.brilliant_mqtt.panel_provisioner import (
    PanelProvisioningError,
    stored_snapshot_from_panel,
)
from custom_components.brilliant_mqtt.provisioning_journal import (
    ProvisioningJournal,
    ProvisioningOperation,
    ProvisioningPhase,
    Store,
)
from custom_components.brilliant_mqtt.shell import PanelShell
from tests.canary_rehearsal import RehearsalShell
from tests.test_panel_health import (
    AVAILABILITY,
    DISCOVERY,
    DISCOVERY_FILTER,
    METADATA,
    STATE,
    STATE_FILTER,
    _discovery,
    _FakeHaMqtt,
)
from tests.test_panel_provisioner import _fleet, _Harness, _request
from tests.test_provisioning_journal import _record


@pytest.fixture(autouse=True)
def durable_store(hass: HomeAssistant, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Undo the HA harness's memory-only writer for this filesystem rehearsal."""
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


@pytest.mark.parametrize("release", [False, True], ids=["legacy", "release_link"])
async def test_complete_baseline_restores_real_code_ca_and_ota_copies(
    tmp_path: Path,
    release: bool,
) -> None:
    shell = RehearsalShell(tmp_path)
    code = shell.install(release=release)
    snapshot = await panel_ops.snapshot_panel(shell)
    baseline = await panel_ops.capture_baseline(shell, snapshot)
    assert baseline.baseline is not None
    assert baseline.baseline.identities["bridge"].release_ordinal is None
    assert "fixture-secret" not in repr(baseline)
    artifacts = list((shell.panel / ".rollback").glob("*/complete.json"))
    assert len(artifacts) == 1
    assert artifacts[0].stat().st_mode & 0o777 == 0o600
    if release:
        assert (code / ".rollback-retained").exists()
        manifest = json.loads(artifacts[0].read_bytes())
        assert manifest["release_target"] == str(code)
        assert not any("/releases/" in item for item in manifest["files"])
    else:
        (code / "vendor/original.py").write_text("candidate = True\n")
    (shell.panel / "tls/mqtt-ca.pem").write_bytes(b"candidate CA")
    (shell.panel / "system/brilliant-mqtt.env").write_bytes(b"candidate config")
    transaction = uuid4()
    staged = panel_ops.StagedRelease(
        "0.10.3",
        transaction,
        f"/var/brilliant-mqtt/releases/0.10.3--{transaction.hex}",
        ("bridge",),
    )
    started = time.monotonic()
    await panel_ops.rollback_snapshot(shell, baseline, staged)
    elapsed = time.monotonic() - started
    assert (code / "vendor/original.py").read_text() == "original = True\n"
    assert (shell.panel / "tls/mqtt-ca.pem").read_bytes() == b"fixture CA\n"
    assert (
        shell.panel / "system/brilliant-mqtt.env"
    ).read_bytes() == snapshot.environment_file.content
    assert elapsed < 300
    print(f"recovery_filesystem_seconds={elapsed:.6f}; layout={snapshot.layout.value}")


@pytest.mark.parametrize(
    "problem", ["missing", "watchdog_missing", "unsupported", "oversize", "entries", "reserve"]
)
async def test_capture_fails_closed_before_mutation(tmp_path: Path, problem: str) -> None:
    shell = RehearsalShell(tmp_path)
    code = shell.install()
    if problem == "watchdog_missing":
        (shell.units / "brilliant-wifi-watchdog.service").write_text("missing prior watchdog\n")
    snapshot = await panel_ops.snapshot_panel(shell)
    limits: dict[str, int] = {}
    if problem == "missing":
        shutil.rmtree(code / "app")
    elif problem == "unsupported":
        (code / "app/brilliant_mqtt/__main__.py").write_text("old_build = True\n")
    elif problem == "oversize":
        limits["maximum_bytes"] = 1
    elif problem == "entries":
        limits["maximum_entries"] = 1
    elif problem == "reserve":
        limits["free_reserve"] = 2**60
    with pytest.raises(panel_ops.PanelOpError, match="baseline_"):
        await panel_ops.capture_baseline(shell, snapshot, **limits)
    assert not list((shell.panel / ".rollback").glob("*/complete.json"))
    assert (shell.panel / "tls/mqtt-ca.pem").read_bytes() == b"fixture CA\n"


async def test_missing_pinned_release_refuses_restore_and_cleanup(tmp_path: Path) -> None:
    shell = RehearsalShell(tmp_path)
    code = shell.install(release=True)
    snapshot = await panel_ops.capture_baseline(shell, await panel_ops.snapshot_panel(shell))
    transaction = uuid4()
    staged = panel_ops.StagedRelease(
        "0.10.3",
        transaction,
        f"/var/brilliant-mqtt/releases/0.10.3--{transaction.hex}",
        ("bridge",),
    )
    shutil.rmtree(code)
    with pytest.raises(panel_ops.PanelOpError, match="baseline_"):
        await panel_ops.rollback_snapshot(shell, snapshot, staged)
    assert snapshot.baseline is not None
    assert list((shell.panel / ".rollback").glob("*/complete.json"))


async def test_legacy_baseline_archives_ca_referenced_in_unpinned_release(tmp_path: Path) -> None:
    shell = RehearsalShell(tmp_path)
    shell.install()
    ca = shell.panel / "releases/0.10.1--aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa/mqtt-ca.pem"
    ca.parent.mkdir(parents=True)
    ca.write_bytes(b"prior referenced CA")
    env = shell.root / "etc/brilliant-mqtt.env"
    env.write_text(env.read_text() + "MQTT_TLS_CA_FILE=" + str(ca) + "\n")
    snapshot = await panel_ops.capture_baseline(shell, await panel_ops.snapshot_panel(shell))
    ca.unlink()
    transaction = uuid4()
    staged = panel_ops.StagedRelease(
        "0.10.3",
        transaction,
        f"/var/brilliant-mqtt/releases/0.10.3--{transaction.hex}",
        ("bridge",),
    )
    await panel_ops.rollback_snapshot(shell, snapshot, staged)
    assert ca.read_bytes() == b"prior referenced CA"


async def test_named_baseline_survives_commit_without_root_password(
    hass: HomeAssistant,
    tmp_path: Path,
) -> None:
    shell = RehearsalShell(tmp_path)
    shell.install()
    snapshot = await panel_ops.capture_baseline(shell, await panel_ops.snapshot_panel(shell))
    record = replace(
        _record(),
        operation=ProvisioningOperation.UPGRADE,
        panel_request=replace(_record().panel_request, selected_components=("bridge",)),
        prior_snapshot=stored_snapshot_from_panel(snapshot),
    )
    journal = ProvisioningJournal(hass)
    await journal.async_create(record)
    retained = await journal.async_retained(record.transaction_id)
    assert retained is not None and retained.state == "armed"
    for phase in (
        ProvisioningPhase.ACTIVATION_PENDING,
        ProvisioningPhase.ACTIVATED,
        ProvisioningPhase.VERIFYING,
        ProvisioningPhase.PENDING_CONFIG_COMMIT,
    ):
        await journal.async_transition(record.transaction_id, phase)
    await journal.async_complete_commit(record.transaction_id, subentry_id="fixture-owner")
    restarted = ProvisioningJournal(hass)
    assert await restarted.async_load() is None
    retained = await restarted.async_retained(record.transaction_id)
    assert retained is not None and retained.state == "soak"
    encoded = json.dumps(retained._to_storage())
    assert record.panel_request.root_password not in encoded
    assert "root_password" not in encoded
    assert "fixture-secret" not in repr(retained)
    retained_path = Path(journal._retained_storage().path)
    assert snapshot.baseline is not None
    artifact = shell.panel / ".rollback" / snapshot.baseline.identifier
    assert (await asyncio.to_thread(retained_path.stat)).st_mode & 0o777 == 0o600
    assert (artifact / "complete.json").stat().st_mode & 0o777 == 0o600
    assert (artifact / "baseline.tar").stat().st_mode & 0o777 == 0o600
    assert artifact.stat().st_mode & 0o777 == 0o700
    assert artifact.parent.stat().st_mode & 0o777 == 0o700
    provisioner = _Harness().provisioner()
    provisioner._journal = journal
    provisioner._operations = panel_ops
    shell._pinned = record.panel_request.public_key
    provisioner._shell_factory = lambda _host, _password, _key: shell

    async def credential(_request: object) -> str:
        return record.panel_request.root_password

    provisioner._credential_resolver = credential
    await provisioner.async_finalize(record.transaction_id)
    assert await journal.async_retained(record.transaction_id) is None
    assert not artifact.exists()
    assert json.loads(await asyncio.to_thread(retained_path.read_bytes))["data"] == {"records": {}}


async def test_capture_retry_reconciles_pin_after_fsync_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    shell = RehearsalShell(tmp_path)
    release = shell.install(release=True)
    snapshot = await panel_ops.snapshot_panel(shell)
    original_source = panel_ops._identity_source

    async def interrupted_source() -> str:
        return (await original_source()).replace(
            'with archive.open("rb") as stream:',
            'os._exit(79)\n        with archive.open("rb") as stream:',
            1,
        )

    with monkeypatch.context() as fault:
        fault.setattr(panel_ops, "_identity_source", interrupted_source)
        with pytest.raises(panel_ops.PanelOpError):
            await panel_ops.capture_baseline(shell, snapshot)
    marker = release / ".rollback-retained"
    stranded = marker.read_text()
    assert not await panel_ops.baseline_retained(shell)
    with pytest.raises(panel_ops.PanelOpError, match="baseline_incomplete"):
        await panel_ops.verify_baseline(shell, snapshot)
    captured = await panel_ops.capture_baseline(shell, snapshot)
    assert captured.baseline is not None
    assert marker.read_text() == captured.baseline.identifier != stranded
    assert not (shell.panel / ".rollback" / ("." + stranded + ".tmp")).exists()
    await panel_ops.verify_baseline(shell, captured)
    with pytest.raises(panel_ops.PanelOpError):
        await panel_ops.capture_baseline(shell, snapshot)


async def test_capture_retry_after_pin_creation_before_identifier_flush(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    shell = RehearsalShell(tmp_path)
    release = shell.install(release=True)
    snapshot = await panel_ops.snapshot_panel(shell)
    original_source = panel_ops._identity_source

    async def interrupted_source() -> str:
        source = await original_source()
        assert source.count("stream.write(identifier)") == 1
        return source.replace("stream.write(identifier)", "os._exit(79)")

    with monkeypatch.context() as fault:
        fault.setattr(panel_ops, "_identity_source", interrupted_source)
        with pytest.raises(panel_ops.PanelOpError):
            await panel_ops.capture_baseline(shell, snapshot)
    marker = release / ".rollback-retained"
    if marker.exists():
        assert marker.read_bytes() == b""
    assert not await panel_ops.baseline_retained(shell)
    with pytest.raises(panel_ops.PanelOpError, match="baseline_incomplete"):
        await panel_ops.verify_baseline(shell, snapshot)
    captured = await panel_ops.capture_baseline(shell, snapshot)
    assert captured.baseline is not None
    assert marker.read_text() == captured.baseline.identifier
    await panel_ops.verify_baseline(shell, captured)


async def test_standalone_activation_rejects_untrusted_higher_ordinal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from custom_components.brilliant_mqtt.release_identity import ReleaseIdentity
    from custom_components.brilliant_mqtt.shell import RunResult
    from tests.fakes import FakeShell

    shell = FakeShell()
    transaction = uuid4()
    staged = panel_ops.StagedRelease(
        "0.10.2",
        transaction,
        f"/var/brilliant-mqtt/releases/0.10.2--{transaction.hex}",
        ("bridge",),
    )
    trusted = ReleaseIdentity("0.10.2", 2, "a" * 64, None, "candidate")
    incumbent = replace(trusted, release_ordinal=1, digest="b" * 64, layout="legacy_fixed")
    forged = replace(trusted, release_ordinal=99, digest="c" * 64)
    local = AsyncMock(return_value={"bridge": trusted})
    monkeypatch.setattr(panel_ops, "candidate_identities", local)
    monkeypatch.setattr(
        panel_ops, "_read_release_identities", AsyncMock(return_value={"bridge": incumbent})
    )
    monkeypatch.setattr(
        panel_ops, "_read_staged_identities", AsyncMock(return_value={"bridge": forged})
    )
    monkeypatch.setattr(panel_ops, "_complete_identity", AsyncMock())
    mutate = AsyncMock(return_value=RunResult(0, "", ""))
    monkeypatch.setattr(panel_ops, "_provisioning_run", mutate)
    with pytest.raises(panel_ops.PanelOpError, match="release_candidate_changed"):
        await panel_ops.activate_staged(shell, staged, on_services_stopped=lambda: None)
    local.assert_awaited_once()
    mutate.assert_not_awaited()


@pytest.mark.parametrize("route", ["fleet", "provisioner"])
@pytest.mark.parametrize("crash_point", ["intent", "operation"])
@pytest.mark.parametrize(
    "operation", [ProvisioningOperation.INSTALL, ProvisioningOperation.UPGRADE]
)
async def test_onboarding_compensation_recovers_without_created_owner(
    hass: HomeAssistant,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    route: str,
    crash_point: str,
    operation: ProvisioningOperation,
) -> None:
    from unittest.mock import Mock

    from custom_components.brilliant_mqtt import fleet_manager
    from tests.test_fleet_manager import _provisioning_entry
    from tests.test_panel_provisioner import _FakeObserver, _health

    shell = RehearsalShell(tmp_path)
    code = shell.install()
    snapshot = await panel_ops.capture_baseline(shell, await panel_ops.snapshot_panel(shell))
    record = replace(
        _record(),
        operation=operation,
        panel_request=replace(_record().panel_request, selected_components=("bridge",)),
        prior_snapshot=stored_snapshot_from_panel(snapshot),
    )
    journal = ProvisioningJournal(hass)
    await journal.async_create(record)
    record = await journal.async_transition(
        record.transaction_id, ProvisioningPhase.ACTIVATION_PENDING
    )
    (code / "vendor/original.py").write_text("candidate = True\n")
    shell._pinned = record.panel_request.public_key
    provisioner = _Harness().provisioner()
    provisioner._journal = journal
    provisioner._operations = panel_ops
    provisioner._shell_factory = lambda _host, _password, _key: shell
    provisioner._health_observer_factory = lambda _slug: _FakeObserver([], _health())
    provisioner._credential_resolver = AsyncMock(return_value=None)
    staged = panel_ops.StagedRelease(
        record.staged_version,
        record.transaction_id,
        f"/var/brilliant-mqtt/releases/{record.staged_version}--{record.transaction_id.hex}",
        ("bridge",),
    )
    begin = ProvisioningJournal.async_begin_restore

    async def crash(self: ProvisioningJournal, transaction_id: object, password: str) -> None:
        assert transaction_id == record.transaction_id
        if crash_point == "operation":
            await begin(self, record.transaction_id, password)
        raise RuntimeError("disposable process loss")

    with monkeypatch.context() as fault:
        fault.setattr(ProvisioningJournal, "async_begin_restore", crash)
        with pytest.raises(RuntimeError, match="disposable process loss"):
            await provisioner._async_recovery_rollback(
                record,
                shell,
                snapshot,
                staged,
                original_code="health_failed",
            )
    journal = ProvisioningJournal(hass)
    retained = await journal.async_retained(record.transaction_id)
    assert retained is not None and retained.state == "restore_requested"
    assert "root_password" not in json.dumps(retained._to_storage())
    if route == "fleet":
        entry = _provisioning_entry(scene_owner="")
        entry.add_to_hass(hass)
        manager = fleet_manager.FleetManager(hass, entry)
        schedule = Mock()
        monkeypatch.setattr(manager, "_async_schedule_background_recovery", schedule)
        await manager._async_prepare_provisioning()
        schedule.assert_called_once()
    provisioner._journal = journal
    await provisioner.async_recover()
    assert await journal.async_load() is None
    retained = await journal.async_retained(record.transaction_id)
    assert retained is not None and retained.state == "restored"
    assert (code / "vendor/original.py").read_text() == "original = True\n"


@pytest.mark.parametrize("route", ["fleet", "provisioner"])
async def test_named_rollback_recovery_rejects_foreign_owner(
    hass: HomeAssistant,
    tmp_path: Path,
    route: str,
) -> None:
    from functools import partial

    from custom_components.brilliant_mqtt import fleet_manager
    from custom_components.brilliant_mqtt.entry_data import EntryDataError
    from tests.test_fleet_manager import _panel, _provisioning_entry

    shell = RehearsalShell(tmp_path)
    shell.install()
    snapshot = await panel_ops.capture_baseline(shell, await panel_ops.snapshot_panel(shell))
    record = replace(_record(), prior_snapshot=stored_snapshot_from_panel(snapshot))
    journal = ProvisioningJournal(hass)
    await journal.async_arm(record)
    await journal.async_retained_state(record.transaction_id, "soak")
    await journal.async_request_restore(record.transaction_id)
    await journal.async_begin_restore(record.transaction_id, record.panel_request.root_password)
    entry = _provisioning_entry(
        _panel("office", "SHA256:kitchen", subentry_id="foreign", host=record.panel_request.host),
        scene_owner="foreign",
    )
    entry.add_to_hass(hass)
    before = await journal.async_load()
    if route == "fleet":
        manager = fleet_manager.FleetManager(hass, entry)
        with pytest.raises(EntryDataError, match="rollback_credentials_unavailable"):
            await manager._async_prepare_provisioning()
    else:
        provisioner = _Harness().provisioner()
        provisioner._journal = journal
        provisioner._credential_resolver = partial(fleet_manager._async_rollback_credential, hass)
        with pytest.raises(PanelProvisioningError, match="rollback_credentials_unavailable"):
            await provisioner.async_recover()
    assert await journal.async_load() == before


@pytest.mark.parametrize("policy", ["blocked", "unchanged", "override"])
async def test_migration_inherits_shared_identity_admission(
    hass: HomeAssistant,
    tmp_path: Path,
    policy: str,
) -> None:
    from collections.abc import Mapping
    from uuid import UUID

    from custom_components.brilliant_mqtt.entry_data import FleetConfig
    from custom_components.brilliant_mqtt.panel_provisioner import (
        PanelInstallRequest,
        PanelReleaseBundle,
    )

    shell = RehearsalShell(tmp_path / "panel")
    code = shell.install()
    payload = panel_ops._identity_payload_dir()
    if policy == "unchanged":
        for tree in ("app", "vendor"):
            shutil.rmtree(code / tree)
            shutil.copytree(payload / tree, code / tree)
    harness = _Harness()
    provisioner = harness.provisioner()
    shell._pinned = harness.shell.pinned_host_key()
    provisioner._shell_factory = lambda _host, _password, _key: shell
    provisioner._operations = panel_ops
    journal = ProvisioningJournal(hass)
    provisioner._journal = journal
    request = replace(_request(), selected_components=("bridge",))

    async def release(
        requested: PanelInstallRequest,
        fleet: FleetConfig,
        transaction: UUID,
        setup: UUID,
    ) -> PanelReleaseBundle:
        bundle = await harness.release(requested, fleet, transaction, setup)
        return replace(bundle, local_payload_dir=str(payload), version="0.10.2")

    provisioner._release_provider = release
    before = await panel_ops.snapshot_panel(shell)
    if policy == "override":
        override: Mapping[str, object] | None = None
        with pytest.raises(panel_ops.ReleaseIdentityBlocked) as blocked:
            async with panel_ops.release_transaction(shell, str(payload), panel=request.slug):
                pytest.fail("unknown incumbent ordinal must block")
        override = blocked.value.override
        await provisioner.async_install(
            request,
            _fleet(),
            harness.progress,
            release_override=override,
        )
        installed = await panel_ops._read_release_identities(shell)
        expected = (await panel_ops.candidate_identities(str(payload)))["bridge"]
        assert installed["bridge"] is not None
        assert installed["bridge"].digest == expected.digest
        assert installed["bridge"].release_ordinal == expected.release_ordinal
        assert installed["bridge"].deployment_id == override["transaction"]
        assert installed["bridge"].layout == "release_link"
        assert list((shell.panel / ".release-identities").glob("override-*.json"))
    else:
        with pytest.raises(
            PanelProvisioningError,
            match="release_identity_blocked" if policy == "blocked" else "release_unchanged",
        ):
            await provisioner.async_install(request, _fleet(), harness.progress)
        assert await panel_ops.snapshot_panel(shell) == before
        assert not (shell.panel / "releases").exists()
        assert not await panel_ops.baseline_retained(shell)
        assert await journal.async_load() is None


@pytest.mark.parametrize("boundary", ["stage", "activate"])
async def test_staged_mutation_boundaries_cannot_bypass_identity(
    tmp_path: Path,
    boundary: str,
) -> None:
    shell = RehearsalShell(tmp_path)
    shell.install()
    payload = panel_ops._identity_payload_dir()
    transaction = uuid4()
    staged = panel_ops.StagedRelease(
        "0.10.2",
        transaction,
        f"/var/brilliant-mqtt/releases/0.10.2--{transaction.hex}",
        ("bridge",),
    )
    if boundary == "activate":
        target = Path(shell.translate(staged.release_target))
        target.parent.mkdir(parents=True)
        await shell.put_dir(str(payload), staged.release_target)
    before = await panel_ops.snapshot_panel(shell)
    with pytest.raises(panel_ops.ReleaseIdentityBlocked):
        if boundary == "stage":
            await panel_ops.stage_release(
                shell,
                str(payload),
                "0.10.2",
                "MQTT_PASSWORD=fixture\n",
                ("bridge",),
                transaction,
            )
        else:
            await panel_ops.activate_staged(shell, staged, on_services_stopped=lambda: None)
    assert await panel_ops.snapshot_panel(shell) == before


@pytest.mark.parametrize("restore_fails", [False, True])
async def test_commit_save_failure_after_soak_has_replayable_compensation(
    hass: HomeAssistant, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, restore_fails: bool
) -> None:
    from custom_components.brilliant_mqtt.provisioning_journal import (
        ProvisioningJournalError,
        ProvisioningRecord,
    )
    from tests.test_panel_provisioner import _FakeObserver, _health

    shell = RehearsalShell(tmp_path)
    shell.install()
    journal = ProvisioningJournal(hass)
    provisioner = _Harness().provisioner()
    provisioner._journal = journal
    provisioner._operations = panel_ops
    provisioner._health_observer_factory = lambda _slug: _FakeObserver([], _health())
    original_save = ProvisioningJournal._async_save
    original_restore = panel_ops.rollback_snapshot
    intents: list[str] = []

    async def failed_commit(self: ProvisioningJournal, record: ProvisioningRecord) -> None:
        if record.phase is ProvisioningPhase.COMMITTED:
            retained = await self._async_retained_load()
            assert retained[str(record.transaction_id)].state == "soak"
            raise ProvisioningJournalError("journal_save_failed")
        await original_save(self, record)

    async def restore(
        active: PanelShell, snapshot: panel_ops.PanelSnapshot, staged: panel_ops.StagedRelease
    ) -> None:
        retained = await journal.async_retained(staged.transaction_id)
        current = await journal.async_load()
        assert retained is not None and retained.state == "restore_requested"
        assert current is not None and current.operation is ProvisioningOperation.ROLLBACK
        intents.append(retained.state)
        if restore_fails:
            raise panel_ops.PanelOpError("rollback_restore_failed")
        await original_restore(active, snapshot, staged)

    monkeypatch.setattr(ProvisioningJournal, "_async_save", failed_commit)
    monkeypatch.setattr(panel_ops, "rollback_snapshot", restore)
    record = _record()
    with pytest.raises((PanelProvisioningError, ProvisioningJournalError)):
        async with provisioner._lock:
            async with provisioner.async_managed_update(
                shell,
                replace(record.panel_request, selected_components=("bridge",)),
                record.fleet_profile,
                record.staged_version,
                subentry_id="fixture-owner",
            ) as transaction:
                (shell.panel / "vendor/original.py").write_text("candidate = True\n")
    assert intents == ["restore_requested"]
    retained = await journal.async_retained(transaction)
    assert retained is not None
    assert retained.state == ("rollback_failed" if restore_fails else "restored")
    current = await journal.async_load()
    if restore_fails:
        assert current is not None and current.operation is ProvisioningOperation.ROLLBACK
        assert current.last_error is not None and current.last_error.code == "rollback_failed"
    else:
        assert current is None


@pytest.mark.parametrize("completion", ["commit", "restore"])
async def test_missing_retained_record_cannot_authorize_completion(
    hass: HomeAssistant, tmp_path: Path, completion: str
) -> None:
    from custom_components.brilliant_mqtt.provisioning_journal import ProvisioningJournalError

    shell = RehearsalShell(tmp_path)
    shell.install()
    snapshot = await panel_ops.capture_baseline(shell, await panel_ops.snapshot_panel(shell))
    record = replace(_record(), prior_snapshot=stored_snapshot_from_panel(snapshot))
    journal = ProvisioningJournal(hass)
    await journal.async_create(record)
    for phase in (
        ProvisioningPhase.ACTIVATION_PENDING,
        ProvisioningPhase.ACTIVATED,
        ProvisioningPhase.VERIFYING,
        ProvisioningPhase.PENDING_CONFIG_COMMIT,
    ):
        await journal.async_transition(record.transaction_id, phase)
    await journal._retained_storage().async_remove()
    with pytest.raises(ProvisioningJournalError):
        if completion == "commit":
            await journal.async_complete_commit(record.transaction_id, subentry_id="fixture")
        else:
            await journal.async_retained_state(record.transaction_id, "restored", evidence="c" * 32)
    pending = await journal.async_load()
    assert pending is not None and pending.phase is ProvisioningPhase.PENDING_CONFIG_COMMIT


async def test_capture_tuned_limits_remain_valid_during_restore(tmp_path: Path) -> None:
    shell = RehearsalShell(tmp_path)
    code = shell.install(release=True)
    for index in range(4100):
        (code / "vendor" / f"entry_{index}").touch()
    snapshot = await panel_ops.capture_baseline(
        shell, await panel_ops.snapshot_panel(shell), maximum_entries=5000
    )
    transaction = uuid4()
    staged = panel_ops.StagedRelease(
        "0.10.3",
        transaction,
        f"/var/brilliant-mqtt/releases/0.10.3--{transaction.hex}",
        ("bridge",),
    )
    await panel_ops.rollback_snapshot(shell, snapshot, staged)
    assert (code / "vendor/entry_4099").exists()


async def test_mixed_selectors_pin_watchdog_release_and_preserve_current(tmp_path: Path) -> None:
    shell = RehearsalShell(tmp_path)
    shell.install()
    prior = shell.panel / "releases/0.10.2--aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"
    watchdog = prior / "wifi_watchdog/brilliant_wifi_watchdog"
    watchdog.mkdir(parents=True)
    (watchdog / "run.py").write_text("prior_watchdog = True\n")
    (prior / "VERSION").write_text("0.10.2\n")
    (shell.panel / "current").symlink_to(prior)
    (shell.units / "brilliant-wifi-watchdog.service").write_text(
        shell.translate(
            "[Service]\nExecStart=/var/brilliant-mqtt/current/wifi_watchdog/brilliant_wifi_watchdog/run.py\n"
        )
    )
    snapshot = await panel_ops.capture_baseline(shell, await panel_ops.snapshot_panel(shell))
    assert (prior / ".rollback-retained").exists()
    transaction = uuid4()
    staged = panel_ops.StagedRelease(
        "0.10.3",
        transaction,
        f"/var/brilliant-mqtt/releases/0.10.3--{transaction.hex}",
        ("bridge",),
    )
    await panel_ops.rollback_snapshot(shell, snapshot, staged)
    assert (shell.panel / "current").resolve() == prior
    assert (watchdog / "run.py").read_text() == "prior_watchdog = True\n"


@pytest.mark.parametrize("interrupted_staging", [False, True])
async def test_rollback_intent_creates_explicit_operation_and_preserves_baseline(
    hass: HomeAssistant,
    tmp_path: Path,
    interrupted_staging: bool,
) -> None:
    shell = RehearsalShell(tmp_path)
    shell.install()
    snapshot = await panel_ops.capture_baseline(shell, await panel_ops.snapshot_panel(shell))
    record = replace(
        _record(),
        operation=ProvisioningOperation.UPGRADE,
        panel_request=replace(_record().panel_request, selected_components=("bridge",)),
        prior_snapshot=stored_snapshot_from_panel(snapshot),
    )
    journal = ProvisioningJournal(hass)
    if interrupted_staging:
        await journal.async_create(record)
    else:
        await journal.async_arm(record)
    await journal.async_request_restore(record.transaction_id)
    retained = await journal.async_retained(record.transaction_id)
    assert retained is not None and retained.state == "restore_requested"
    await journal.async_begin_restore(record.transaction_id, record.panel_request.root_password)
    restored_journal = await ProvisioningJournal(hass).async_load()
    assert restored_journal is not None
    assert restored_journal.operation is (
        ProvisioningOperation.ONBOARDING_ROLLBACK
        if interrupted_staging
        else ProvisioningOperation.ROLLBACK
    )
    assert restored_journal.phase is ProvisioningPhase.ROLLBACK_PENDING
    assert restored_journal.prior_snapshot == record.prior_snapshot


async def test_provisioner_rejects_incomplete_capture_before_stage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness()

    async def incomplete(
        shell: PanelShell, snapshot: panel_ops.PanelSnapshot
    ) -> panel_ops.PanelSnapshot:
        return replace(snapshot, baseline=None)

    monkeypatch.setattr(harness.operations, "capture_baseline", incomplete, raising=False)
    with pytest.raises(PanelProvisioningError, match="snapshot_failed"):
        await harness.provisioner().async_install(_request(), _fleet(), harness.progress)
    assert not any(event[0] == "stage" for event in harness.events)


@pytest.mark.parametrize("reconnect", [True, False], ids=["fresh_session", "reconnect_failure"])
async def test_restore_requires_fresh_session_before_durable_success(
    hass: HomeAssistant,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reconnect: bool,
) -> None:
    shell = RehearsalShell(tmp_path)
    code = shell.install()
    baseline = await panel_ops.capture_baseline(shell, await panel_ops.snapshot_panel(shell))
    record = replace(
        _record(),
        operation=ProvisioningOperation.UPGRADE,
        panel_request=replace(_record().panel_request, selected_components=("bridge",)),
        prior_snapshot=stored_snapshot_from_panel(baseline),
    )
    journal = ProvisioningJournal(hass)
    await journal.async_arm(record)
    await journal.async_request_restore(record.transaction_id)
    await journal.async_begin_restore(record.transaction_id, record.panel_request.root_password)
    (code / "vendor/original.py").write_text("candidate = True\n")
    seam = _FakeHaMqtt()
    seam.install(monkeypatch)
    observer = PanelHealthObserver(hass, "office")
    harness = _Harness()
    provisioner = harness.provisioner()
    provisioner._journal = journal
    provisioner._operations = panel_ops
    provisioner._health_observer_factory = lambda _slug: observer
    restarted = asyncio.Event()
    release = asyncio.Event()
    captured_ids: list[str] = []
    original_restart = getattr(panel_ops, "restart_restored", None)

    def publish(identifier: str, *, retain: bool = False) -> None:
        seam.fire(AVAILABILITY, AVAILABILITY, "online", retain=retain)
        seam.fire(
            METADATA,
            METADATA,
            json.dumps({"agent_version": "0.10.2", "deployment_id": identifier}),
            retain=retain,
        )
        seam.fire(STATE_FILTER, STATE, '{"on":true}', retain=retain)
        seam.fire(DISCOVERY_FILTER, DISCOVERY, _discovery(), retain=retain)

    async def restart(
        active_shell: PanelShell,
        snapshot: panel_ops.PanelSnapshot,
        deployment_id: str,
        *,
        on_service_stopped: Callable[[], None],
    ) -> None:
        assert original_restart is not None
        await original_restart(
            active_shell, snapshot, deployment_id, on_service_stopped=on_service_stopped
        )
        captured_ids.append(deployment_id)
        publish("b" * 32)
        publish(deployment_id, retain=True)
        assert observer._evidence() is None
        restarted.set()
        await release.wait()
        if reconnect:
            publish(deployment_id)
        else:
            raise PanelHealthError("panel_health_timeout")

    monkeypatch.setattr(panel_ops, "restart_restored", restart, raising=False)
    staged = panel_ops.StagedRelease(
        record.staged_version,
        record.transaction_id,
        f"/var/brilliant-mqtt/releases/{record.staged_version}--{record.transaction_id.hex}",
        ("bridge",),
    )
    current = await journal.async_load()
    assert current is not None
    started = time.monotonic()
    task = asyncio.create_task(
        provisioner._async_recovery_rollback(
            current, shell, baseline, staged, original_code="operator_requested"
        )
    )
    boundary = asyncio.create_task(restarted.wait())
    await asyncio.wait({task, boundary}, return_when=asyncio.FIRST_COMPLETED)
    if task.done():
        boundary.cancel()
        await asyncio.gather(boundary, return_exceptions=True)
        await task
    assert restarted.is_set(), "rollback was declared successful without fresh MQTT verification"
    assert await journal.async_load() is not None
    assert captured_ids[0] != "b" * 32
    release.set()
    if reconnect:
        await task
        assert await journal.async_load() is None
        retained = await journal.async_retained(record.transaction_id)
        assert retained is not None and retained.state == "restored"
        assert retained.recovery_deployment_id == captured_ids[0]
        print(f"recovery_seconds={time.monotonic() - started:.6f}; mqtt=simulated")
    else:
        with pytest.raises(PanelProvisioningError, match="rollback_health_failed"):
            await task
        assert await journal.async_load() is not None
        retained = await journal.async_retained(record.transaction_id)
        assert retained is not None and retained.state == "rollback_failed"
    env = (shell.root / "etc/brilliant-mqtt.env").read_text()
    assert "fixture-secret" in env and captured_ids[0] in env
    assert (code / "vendor/original.py").read_text() == "original = True\n"


@pytest.mark.parametrize("operation", ["rollback", "finalize"])
@pytest.mark.allow_lingering_timers
async def test_targeted_canary_operator_services_require_one_panel(
    hass: HomeAssistant,
    mqtt_mock: object,
    operation: str,
) -> None:
    from homeassistant.exceptions import HomeAssistantError
    from homeassistant.helpers import entity_registry as er

    from custom_components.brilliant_mqtt.const import DOMAIN
    from tests.test_services import _setup_fleet_entry

    entry = await _setup_fleet_entry(hass)
    office = entry.runtime_data.panels["panel-office"]
    kitchen = entry.runtime_data.panels["panel-kitchen"]
    office.async_canary_operation = AsyncMock()
    kitchen.async_canary_operation = AsyncMock()
    name = str(uuid4())
    entity = er.async_get(hass).async_get_entity_id(
        "binary_sensor", DOMAIN, "SHA256:office_bridge_health"
    )
    assert entity is not None
    try:
        with pytest.raises(HomeAssistantError, match="exactly one"):
            await hass.services.async_call(
                DOMAIN, "canary_" + operation, {"name": name}, blocking=True
            )
        await hass.services.async_call(
            DOMAIN, "canary_" + operation, {"name": name, "entity_id": entity}, blocking=True
        )
        office.async_canary_operation.assert_awaited_once_with(operation, name)
        kitchen.async_canary_operation.assert_not_awaited()
    finally:
        await hass.config_entries.async_unload(entry.entry_id)


async def test_retained_baseline_defers_legacy_retirement(
    hass: HomeAssistant,
    tmp_path: Path,
) -> None:
    from tests.test_manager import _fleet_panel_manager

    shell = RehearsalShell(tmp_path)
    shell.install()
    await panel_ops.capture_baseline(shell, await panel_ops.snapshot_panel(shell))
    _, manager = _fleet_panel_manager(hass)
    shell.commands.clear()
    assert await manager._async_retire_legacy_ha_mirror_on_shell(shell) is None
    assert not any("brilliant-ha-mirror" in command for command in shell.commands)


async def test_repair_reports_actionable_failure_codes(hass: HomeAssistant) -> None:
    from homeassistant.helpers import issue_registry as ir

    from custom_components.brilliant_mqtt.const import DOMAIN
    from custom_components.brilliant_mqtt.fleet_manager import _ProvisioningRepairReporter

    transaction = uuid4()
    await _ProvisioningRepairReporter(hass).async_report_rollback_failure(
        transaction, original_code="health_failed", rollback_code="rollback_health_failed"
    )
    issues = [item for (domain, _), item in ir.async_get(hass).issues.items() if domain == DOMAIN]
    assert len(issues) == 1
    assert issues[0].translation_placeholders is not None
    assert "rollback_health_failed" in issues[0].translation_placeholders["reason"]


async def test_manual_update_arms_before_mutation_and_retains_soak(
    hass: HomeAssistant,
    tmp_path: Path,
) -> None:
    from tests.test_panel_provisioner import _FakeObserver, _health

    shell = RehearsalShell(tmp_path)
    shell.install()
    journal = ProvisioningJournal(hass)
    harness = _Harness()
    provisioner = harness.provisioner()
    provisioner._journal = journal
    provisioner._operations = panel_ops

    class Observer(_FakeObserver):
        def mark_activation_started(
            self, expected_version: str, expected_deployment_id: str
        ) -> None:
            self.evidence = replace(
                _health(), agent_version=expected_version, deployment_id=expected_deployment_id
            )

    provisioner._health_observer_factory = lambda _slug: Observer([], _health())
    record = _record()
    async with provisioner._lock:
        async with provisioner.async_managed_update(
            shell,
            record.panel_request,
            record.fleet_profile,
            record.staged_version,
            subentry_id="fixture-owner",
        ) as transaction:
            current = await journal.async_load()
            assert current is not None and current.phase is ProvisioningPhase.ACTIVATION_PENDING
            assert current.prior_snapshot.baseline is not None
            retained = await journal.async_retained(transaction)
            assert retained is not None and retained.state == "armed"
            (shell.panel / "vendor/original.py").write_text("candidate = True\n")
    assert await journal.async_load() is None
    retained = await journal.async_retained(transaction)
    assert retained is not None and retained.state == "soak"


@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.parametrize("extra_component", [False, True])
@pytest.mark.parametrize(
    "phase",
    [
        ProvisioningPhase.STAGED,
        ProvisioningPhase.ACTIVATION_PENDING,
        ProvisioningPhase.ACTIVATED,
        ProvisioningPhase.VERIFYING,
    ],
)
async def test_owned_managed_update_schedules_restart_recovery(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
    legacy: bool,
    phase: ProvisioningPhase,
    extra_component: bool,
) -> None:
    from unittest.mock import Mock

    from custom_components.brilliant_mqtt import fleet_manager
    from custom_components.brilliant_mqtt.const import (
        CONF_COMPONENTS,
        CONF_HOST,
        CONF_PANEL,
        CONF_ROOT_PASSWORD,
        CONF_SSH_HOST_KEY,
    )
    from tests.test_fleet_manager import (
        _OFFICE_FINGERPRINT,
        _legacy_entry,
        _panel,
        _pending_record,
        _provisioning_entry,
    )

    pending = _pending_record()
    entry = _provisioning_entry(
        _panel(
            "office", "SHA256:office", subentry_id="panel-office", management_id=_OFFICE_FINGERPRINT
        ),
        scene_owner="panel-office",
    )
    if legacy:
        entry = _legacy_entry()
        entry.add_to_hass(hass)
        hass.config_entries.async_update_entry(
            entry,
            data={
                **entry.data,
                CONF_HOST: pending.panel_request.host,
                CONF_PANEL: pending.panel_request.slug,
                CONF_ROOT_PASSWORD: pending.panel_request.root_password,
                CONF_SSH_HOST_KEY: pending.panel_request.public_key,
            },
        )
        pending = replace(
            pending,
            panel_request=replace(
                pending.panel_request,
                selected_components=("bridge",),
            ),
        )
    else:
        entry.add_to_hass(hass)
    if extra_component:
        if legacy:
            hass.config_entries.async_update_entry(
                entry,
                data={
                    **entry.data,
                    CONF_COMPONENTS: {"bridge": True, "hue_ca": True},
                },
            )
        else:
            owner = entry.subentries["panel-office"]
            hass.config_entries.async_update_subentry(
                entry,
                owner,
                data={
                    **owner.data,
                    CONF_COMPONENTS: {**owner.data[CONF_COMPONENTS], "hue_ca": True},
                },
            )
    harness = _Harness()
    provisioner = harness.provisioner()
    async with provisioner._lock:
        async with provisioner.async_managed_update(
            harness.shell,
            pending.panel_request,
            pending.fleet_profile,
            pending.staged_version,
            subentry_id="fixture-owner",
        ):
            recorded = await harness.journal.async_load()
    assert recorded is not None
    recorded = replace(recorded, phase=phase)
    journal = Mock(
        async_retained_records=AsyncMock(return_value=()),
        async_load=AsyncMock(return_value=recorded),
    )
    monkeypatch.setattr(fleet_manager, "ProvisioningJournal", lambda _hass: journal)
    manager = fleet_manager.FleetManager(hass, entry)
    schedule = Mock()
    monkeypatch.setattr(manager, "_async_schedule_background_recovery", schedule)
    await manager._async_prepare_provisioning()
    schedule.assert_called_once_with(recorded)
    lookup = await fleet_manager._async_transaction_lookup(hass, recorded.transaction_id)
    assert lookup.state.value == "matched"


@pytest.mark.parametrize("phase", [ProvisioningPhase.ACTIVATED, ProvisioningPhase.VERIFYING])
async def test_managed_update_restart_restores_without_a_staged_release(
    phase: ProvisioningPhase,
) -> None:
    harness = _Harness()
    provisioner = harness.provisioner()
    record = _record()
    request = replace(
        record.panel_request, selected_components=("bridge", "bus_watchdog", "wifi_watchdog")
    )
    async with provisioner._lock:
        async with provisioner.async_managed_update(
            harness.shell,
            request,
            record.fleet_profile,
            record.staged_version,
            subentry_id="fixture-owner",
        ):
            captured = await harness.journal.async_load()
    assert captured is not None
    harness.journal.record = replace(captured, phase=phase)
    provisioner._shell_factory = lambda _host, _password, _key: harness.shell
    harness.shell._pinned = request.public_key
    await provisioner.async_recover()
    assert harness.journal.record is None
    assert harness.operations.rolled_back_snapshot is not None


async def test_snapshot_uses_actual_unit_selection_with_surviving_old_current(
    tmp_path: Path,
) -> None:
    shell = RehearsalShell(tmp_path)
    release = shell.install(release=True)
    shutil.copytree(release / "app", shell.panel / "app")
    shutil.copytree(release / "vendor", shell.panel / "vendor")
    unit = shell.units / "brilliant-mqtt.service"
    unit.write_text(unit.read_text().replace("/current/", "/"))
    snapshot = await panel_ops.snapshot_panel(shell)
    assert snapshot.layout is panel_ops.PanelLayout.LEGACY_FIXED
    complete = await panel_ops.capture_baseline(shell, snapshot)
    assert complete.baseline is not None
    assert complete.baseline.identities["bridge"].layout == "legacy_fixed"
    assert not (release / ".rollback-retained").exists()


async def test_reaper_cannot_delete_an_inactive_pinned_baseline(tmp_path: Path) -> None:
    from uuid import UUID

    shell = RehearsalShell(tmp_path)
    release = shell.install(release=True)
    await panel_ops.capture_baseline(shell, await panel_ops.snapshot_panel(shell))
    (shell.panel / "current").unlink()
    pinned = panel_ops.StagedRelease(
        "0.10.2",
        UUID("aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"),
        "/var/brilliant-mqtt/releases/0.10.2--aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa",
        ("bridge",),
    )
    with pytest.raises(panel_ops.PanelOpError, match="staged_cleanup_failed"):
        await panel_ops.cleanup_staged(shell, pinned)
    assert (release / "vendor/original.py").read_text() == "original = True\n"


@pytest.mark.parametrize("crash_point", ["intent", "restore", "evidence", "finalize"])
async def test_crash_recovery_replays_real_restore_and_finalize(
    hass: HomeAssistant,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_point: str,
) -> None:
    from tests.test_panel_provisioner import _FakeObserver, _health

    shell = RehearsalShell(tmp_path)
    code = shell.install()
    baseline = await panel_ops.capture_baseline(shell, await panel_ops.snapshot_panel(shell))
    record = replace(
        _record(),
        operation=ProvisioningOperation.UPGRADE,
        panel_request=replace(_record().panel_request, selected_components=("bridge",)),
        prior_snapshot=stored_snapshot_from_panel(baseline),
    )
    shell._pinned = record.panel_request.public_key
    journal = ProvisioningJournal(hass)
    await journal.async_arm(record)
    await journal.async_retained_state(record.transaction_id, "soak")
    (code / "vendor/original.py").write_text("candidate = True\n")
    harness = _Harness()
    provisioner = harness.provisioner()
    provisioner._journal = journal
    provisioner._operations = panel_ops
    provisioner._shell_factory = lambda _host, _password, _key: shell
    provisioner._health_observer_factory = lambda _slug: _FakeObserver([], _health())

    async def credential(_request: object) -> str:
        return record.panel_request.root_password

    provisioner._credential_resolver = credential

    async def crash(*args: object, **kwargs: object) -> None:
        raise RuntimeError("simulated process loss")

    with monkeypatch.context() as fault:
        if crash_point == "intent":
            fault.setattr(ProvisioningJournal, "async_begin_restore", crash)
        elif crash_point == "restore":
            original_source = panel_ops._identity_source

            async def interrupted_source() -> str:
                return (await original_source()).replace(
                    "os.replace(temporary, path)", "os._exit(76)  # disposable crash injection"
                )

            fault.setattr(panel_ops, "_identity_source", interrupted_source)
        elif crash_point == "evidence":
            fault.setattr(ProvisioningJournal, "async_complete_rollback", crash)
        else:
            await provisioner.async_rollback(record.transaction_id)
            fault.setattr(ProvisioningJournal, "async_remove_retained", crash)
        with pytest.raises((PanelProvisioningError, RuntimeError)):
            if crash_point == "finalize":
                await provisioner.async_finalize(record.transaction_id)
            else:
                await provisioner.async_rollback(record.transaction_id)
    # A new journal view and the existing runner settle durable intent after restart.
    provisioner._journal = ProvisioningJournal(hass)
    await provisioner.async_recover()
    assert await provisioner._journal.async_load() is None
    retained = await provisioner._journal.async_retained(record.transaction_id)
    if crash_point == "finalize":
        assert retained is None
        assert not list((shell.panel / ".rollback").glob("*/complete.json"))
    else:
        assert retained is not None and retained.state == "restored"
    assert (code / "vendor/original.py").read_text() == "original = True\n"


async def test_recovery_cancellation_settles_remote_child_before_return(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.fakes import FakePanelProcess, FakeShell

    process = FakePanelProcess(settled=False)
    shell = FakeShell(processes={"restore-fixture": process})
    await shell.connect()
    close = AsyncMock(wraps=shell.close)
    monkeypatch.setattr(shell, "close", close)
    task = asyncio.create_task(
        panel_ops._provisioning_run(shell, "restore-fixture", "rollback_restore_failed")
    )
    await asyncio.sleep(0)
    assert not task.done(), "restore must own a settleable remote process"
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert process.terminate_count == 1
    assert not process.running
    close.assert_awaited_once()


async def test_finalize_resumes_after_partial_artifact_deletion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    shell = RehearsalShell(tmp_path)
    code = shell.install(release=True)
    snapshot = await panel_ops.capture_baseline(shell, await panel_ops.snapshot_panel(shell))
    original_source = panel_ops._identity_source

    async def interrupted_source() -> str:
        return (await original_source()).replace(
            "shutil.rmtree(directory)",
            "(directory / 'complete.json').unlink(); os._exit(77)",
        )

    with monkeypatch.context() as fault:
        fault.setattr(panel_ops, "_identity_source", interrupted_source)
        with pytest.raises(panel_ops.PanelOpError):
            await panel_ops.finalize_baseline(shell, snapshot)
    await panel_ops.finalize_baseline(shell, snapshot)
    assert not (code / ".rollback-retained").exists()
    assert list((shell.panel / ".rollback").iterdir()) == []


async def test_capture_crash_before_publication_never_authorizes_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    shell = RehearsalShell(tmp_path)
    code = shell.install()
    snapshot = await panel_ops.snapshot_panel(shell)
    original_source = panel_ops._identity_source

    async def interrupted_source() -> str:
        return (await original_source()).replace(
            "os.rename(temporary, destination)", "os._exit(78)"
        )

    with monkeypatch.context() as fault:
        fault.setattr(panel_ops, "_identity_source", interrupted_source)
        with pytest.raises(panel_ops.PanelOpError):
            await panel_ops.capture_baseline(shell, snapshot)
    assert not await panel_ops.baseline_retained(shell)
    with pytest.raises(panel_ops.PanelOpError, match="baseline_incomplete"):
        await panel_ops.verify_baseline(shell, snapshot)
    assert (code / "vendor/original.py").read_text() == "original = True\n"


async def test_uninstall_cannot_destroy_retained_recovery_artifacts(tmp_path: Path) -> None:
    shell = RehearsalShell(tmp_path)
    code = shell.install()
    await panel_ops.capture_baseline(shell, await panel_ops.snapshot_panel(shell))
    with pytest.raises(panel_ops.PanelOpError, match="baseline_retained_finalize_first"):
        await panel_ops.uninstall(shell)
    assert (code / "vendor/original.py").read_text() == "original = True\n"


async def test_operator_diagnostics_exposes_name_and_state_without_private_data(
    hass: HomeAssistant,
    tmp_path: Path,
) -> None:
    from custom_components.brilliant_mqtt.diagnostics import _async_provisioning_diagnostics

    shell = RehearsalShell(tmp_path)
    shell.install()
    snapshot = await panel_ops.capture_baseline(shell, await panel_ops.snapshot_panel(shell))
    record = replace(_record(), prior_snapshot=stored_snapshot_from_panel(snapshot))
    await ProvisioningJournal(hass).async_arm(record)
    output = await _async_provisioning_diagnostics(hass)
    assert output["retained"] == [
        {
            "name": str(record.transaction_id),
            "panel": "office",
            "state": "armed",
            "recovery_deployment_id": None,
        }
    ]
    encoded = json.dumps(output)
    assert record.panel_request.root_password not in encoded
    assert "environment_file" not in encoded and "public_key" not in encoded


async def test_overall_recovery_deadline_covers_connection_and_reconciliation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from custom_components.brilliant_mqtt import panel_provisioner

    entered, release = asyncio.Event(), asyncio.Event()
    provisioner = _Harness().provisioner()
    deadlines: list[asyncio.Timeout] = []
    timeout = asyncio.timeout

    def controlled_timeout(seconds: float | None) -> asyncio.Timeout:
        if seconds == panel_provisioner.RECOVERY_TIMEOUT_SECONDS:
            deadline = timeout(None)
            deadlines.append(deadline)
            return deadline
        return timeout(seconds)

    async def stalled(_progress: object) -> None:
        entered.set()
        await release.wait()

    monkeypatch.setattr(provisioner, "_async_recover_current", stalled)
    monkeypatch.setattr(asyncio, "timeout", controlled_timeout)
    task = asyncio.create_task(provisioner.async_recover())
    await entered.wait()
    try:
        assert deadlines, "the 300s recovery deadline must include connect and retained intent"
        deadlines[0].reschedule(asyncio.get_running_loop().time())
        with pytest.raises(PanelProvisioningError, match="rollback_deadline_exceeded"):
            await task
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_other_transaction_is_rejected_before_persisting_restore_intent(
    hass: HomeAssistant,
    tmp_path: Path,
) -> None:
    from custom_components.brilliant_mqtt.provisioning_journal import ProvisioningJournalError

    shell = RehearsalShell(tmp_path)
    shell.install()
    snapshot = await panel_ops.capture_baseline(shell, await panel_ops.snapshot_panel(shell))
    record = replace(_record(), prior_snapshot=stored_snapshot_from_panel(snapshot))
    journal = ProvisioningJournal(hass)
    await journal.async_arm(record)
    await journal.async_retained_state(record.transaction_id, "soak")
    other = replace(_record(), transaction_id=uuid4())
    await journal.async_create(other)
    provisioner = _Harness().provisioner()
    provisioner._journal = journal

    async def credential(_request: object) -> str:
        return record.panel_request.root_password

    provisioner._credential_resolver = credential
    with pytest.raises((PanelProvisioningError, ProvisioningJournalError)):
        await provisioner.async_rollback(record.transaction_id)
    retained = await journal.async_retained(record.transaction_id)
    assert retained is not None and retained.state == "soak"
    assert await journal.async_load() == other


@pytest.mark.parametrize("selected_watchdog", [False, True])
async def test_manager_update_cannot_reach_ca_without_durable_complete_baseline(
    hass: HomeAssistant,
    tmp_path: Path,
    payload_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    selected_watchdog: bool,
) -> None:
    from custom_components.brilliant_mqtt import fleet_manager
    from custom_components.brilliant_mqtt.const import CONF_COMPONENTS, CONF_SSH_HOST_KEY
    from tests.test_manager import _fleet_panel_manager
    from tests.test_panel_provisioner import _FakeObserver, _health

    shell = RehearsalShell(tmp_path / "panel")
    shell.install()
    if selected_watchdog:
        package = shell.panel / "wifi_watchdog/brilliant_wifi_watchdog"
        package.mkdir(parents=True)
        (package / "run.py").write_text("old_watchdog = True\n")
        (shell.panel / "wifi_watchdog/VERSION").write_text("0.10.2\n")
        (shell.units / "brilliant-wifi-watchdog.service").write_text("old watchdog unit\n")
        shell.state.write_text(
            json.dumps({"brilliant-mqtt": [True, True], "brilliant-wifi-watchdog": [True, True]})
        )
    shell._pinned = _record().panel_request.public_key
    _, manager = _fleet_panel_manager(
        hass,
        panel_overrides={
            CONF_SSH_HOST_KEY: shell._pinned,
            CONF_COMPONENTS: {"bridge": True, "wifi_watchdog": selected_watchdog},
        },
    )
    await shell.connect()
    with pytest.raises(panel_ops.ReleaseIdentityBlocked) as blocked:
        async with panel_ops.release_transaction(
            shell,
            str(payload_dir),
            panel="office",
            components=("wifi_watchdog",) if selected_watchdog else (),
        ):
            pytest.fail("legacy ordinal must require explicit approval")
    journal = ProvisioningJournal(hass)
    provisioner = _Harness().provisioner(manager._ssh_lock)
    provisioner._journal = journal
    provisioner._operations = panel_ops
    provisioner._health_observer_factory = lambda _slug: _FakeObserver([], _health())
    monkeypatch.setattr(fleet_manager, "_get_recovery_provisioner", lambda _hass: provisioner)
    monkeypatch.setattr(manager, "_connect_for_repair", AsyncMock(return_value=shell))
    original_ca = manager._async_stage_broker_ca
    reached = asyncio.Event()

    async def stage_ca(active_shell: PanelShell) -> str:
        record = await journal.async_load()
        assert record is not None and record.phase is ProvisioningPhase.ACTIVATION_PENDING
        assert record.prior_snapshot.baseline is not None
        retained = await journal.async_retained(record.transaction_id)
        assert retained is not None and retained.state == "armed"
        reached.set()
        return await original_ca(active_shell)

    monkeypatch.setattr(manager, "_async_stage_broker_ca", stage_ca)
    try:
        await manager.async_update_agent(release_override=blocked.value.override)
        assert reached.is_set()
        assert await journal.async_load() is None
        retained_records = await journal.async_retained_records()
        assert len(retained_records) == 1 and retained_records[0].state == "soak"
        if selected_watchdog:
            assert (shell.panel / "wifi_watchdog/brilliant_wifi_watchdog/run.py").read_bytes() == (
                payload_dir / "wifi_watchdog/brilliant_wifi_watchdog/run.py"
            ).read_bytes()
    finally:
        await manager.async_shutdown()
