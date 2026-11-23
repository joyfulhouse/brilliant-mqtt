"""Managed-update rollback over a fresh pinned session after a lost one (#219)."""

from __future__ import annotations

import asyncio
import errno
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncssh
import pytest
from homeassistant.core import HomeAssistant
from homeassistant.helpers import issue_registry as ir
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.brilliant_mqtt import fleet_manager, panel_ops, panel_provisioner
from custom_components.brilliant_mqtt.const import (
    CONF_COMPONENTS,
    CONF_HOST,
    CONF_MQTT_HOST,
    CONF_MQTT_PORT,
    CONF_MQTT_TLS_ENABLED,
    CONF_PANEL,
    CONF_ROOT_PASSWORD,
    CONF_SSH_HOST_KEY,
    DOMAIN,
)
from custom_components.brilliant_mqtt.fleet_manager import _ProvisioningRepairReporter
from custom_components.brilliant_mqtt.panel_provisioner import (
    PanelProvisioner,
    PanelProvisioningError,
)
from custom_components.brilliant_mqtt.provisioning_journal import (
    ProvisioningJournal,
    ProvisioningOperation,
    ProvisioningPhase,
    ProvisioningRecord,
)
from custom_components.brilliant_mqtt.shell import PanelShell
from tests.canary_rehearsal import RehearsalShell
from tests.fakes import FakeShell
from tests.test_init import ENTRY_DATA
from tests.test_panel_provisioner import _FakeObserver, _health
from tests.test_provisioning_journal import _record
from tests.update_harness import fresh_view

pytestmark = pytest.mark.usefixtures("durable_journal_store")

_UNREACHABLE = OSError(errno.EHOSTUNREACH, "No route to host")
_OTHER_KEY = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIBsF8pQ8C3EWxF7bGrXWvqJXV1fbNHo6p5FC0gIjJbqv"
_ACTIVATE = "BRILLIANT_TEST_ACTIVATE_CANDIDATE"


class _ClosingShell(FakeShell):
    """A reconnect shell that records whether it was closed."""

    def __init__(
        self,
        *,
        connect_error: Exception | None = None,
        pinned: str | None = None,
        reached_auth: bool = False,
        connect_gate: asyncio.Event | None = None,
    ) -> None:
        super().__init__(
            connect_error=connect_error,
            pinned=pinned if pinned is not None else _record().panel_request.public_key,
            reached_auth=reached_auth,
            connect_gate=connect_gate,
        )
        self.closed = False

    async def close(self) -> None:
        self.closed = True
        await super().close()


@dataclass
class _Rig:
    """One managed update on a rehearsal panel whose reconnects are scripted."""

    provisioner: PanelProvisioner
    journal: ProvisioningJournal
    panel: RehearsalShell
    code: Path
    owner: MockConfigEntry
    scripted: list[PanelShell] = field(default_factory=list)
    factory_keys: list[str] = field(default_factory=list)
    fresh: list[PanelShell] = field(default_factory=list)

    def factory(self, host: str, password: str, public_key: str) -> PanelShell:
        request = _record().panel_request
        assert (host, password) == (request.host, request.root_password)
        self.factory_keys.append(public_key)
        shell = self.scripted.pop(0) if self.scripted else fresh_view(self.panel)
        self.fresh.append(shell)
        return shell

    async def update_and_lose(
        self,
        *,
        lose: bool = True,
        stopping: Callable[[], bool] = lambda: False,
    ) -> BaseException:
        """Start the transaction, activate on the panel, then lose the session."""
        record = _record()
        with pytest.raises(BaseException) as raised:
            async with self.provisioner._lock:
                async with self.provisioner.async_managed_update(
                    self.panel,
                    record.panel_request,
                    record.fleet_profile,
                    record.staged_version,
                    subentry_id="fixture-owner",
                    stopping=stopping,
                ):
                    (self.code / "vendor/original.py").write_text("candidate = True\n")
                    if lose:
                        self.panel.drop_after = _ACTIVATE
                    await self.panel.run(f"echo {_ACTIVATE}")
                    raise _LiveFailure("live failure")
        return raised.value


class _LiveFailure(panel_ops.PanelOpError):
    """A deployment failure on a live session."""


def _rig(hass: HomeAssistant, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> _Rig:
    panel = RehearsalShell(tmp_path / "panel")
    code = panel.install()
    panel._pinned = _record().panel_request.public_key
    journal = ProvisioningJournal(hass)
    record = _record()
    request = record.panel_request
    owner = MockConfigEntry(
        domain=DOMAIN,
        unique_id=request.slug,
        data={
            **ENTRY_DATA,
            CONF_HOST: request.host,
            CONF_PANEL: request.slug,
            CONF_ROOT_PASSWORD: request.root_password,
            CONF_SSH_HOST_KEY: request.public_key,
            CONF_COMPONENTS: dict.fromkeys(request.selected_components, True),
            CONF_MQTT_HOST: record.fleet_profile.host,
            CONF_MQTT_PORT: record.fleet_profile.port,
            CONF_MQTT_TLS_ENABLED: record.fleet_profile.tls_enabled,
        },
    )
    owner.add_to_hass(hass)
    provisioner = fleet_manager._build_panel_provisioner(hass, AsyncMock())
    rig = _Rig(provisioner, journal, panel, code, owner)
    provisioner._health_observer_factory = lambda _slug: _FakeObserver([], _health())
    provisioner._repair_reporter = _ProvisioningRepairReporter(hass)
    provisioner._shell_factory = rig.factory

    def never(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("activation must never be repeated")

    monkeypatch.setattr(panel_ops, "activate_staged", never, raising=False)
    monkeypatch.setattr(panel_ops, "restart_candidate", never, raising=False)
    return rig


async def _assert_intent_kept(rig: _Rig, hass: HomeAssistant, code: str) -> None:
    record = await rig.journal.async_load()
    assert record is not None
    assert record.phase is ProvisioningPhase.ROLLBACK_PENDING
    assert record.last_error is not None
    assert (record.last_error.stage, record.last_error.code) == ("recovery", code)
    retained = await rig.journal.async_retained(record.transaction_id)
    assert retained is not None and retained.state == "restore_requested"
    assert (rig.code / "vendor/original.py").read_text() == "candidate = True\n"
    issues = [item for (domain, _), item in ir.async_get(hass).issues.items() if domain == DOMAIN]
    assert len(issues) == 1
    placeholders = issues[0].translation_placeholders
    assert placeholders is not None
    assert "connection_lost" in placeholders["reason"]
    assert code in placeholders["reason"]


async def test_lost_session_restores_over_a_fresh_pinned_session(
    hass: HomeAssistant, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(hass, tmp_path, monkeypatch)

    raised = await rig.update_and_lose()

    assert isinstance(raised, asyncssh.ConnectionLost)
    assert rig.factory_keys == [_record().panel_request.public_key]
    assert [shell.connect_count for shell in rig.fresh if isinstance(shell, FakeShell)] == [1]
    assert await rig.journal.async_load() is None
    retained = await rig.journal.async_retained_records()
    assert len(retained) == 1 and retained[0].state == "restored"
    assert (rig.code / "vendor/original.py").read_text() == "original = True\n"
    # The activation ran exactly once (before the loss) and never on the new session.
    assert sum(_ACTIVATE in command for command in rig.panel.commands) == 1
    assert all(
        _ACTIVATE not in command
        for shell in rig.fresh
        if isinstance(shell, FakeShell)
        for command in shell.commands
    )
    assert not ir.async_get(hass).issues


async def test_live_session_failure_restores_over_that_same_session(
    hass: HomeAssistant, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(hass, tmp_path, monkeypatch)

    raised = await rig.update_and_lose(lose=False)

    assert isinstance(raised, _LiveFailure)
    assert rig.factory_keys == []
    assert await rig.journal.async_load() is None
    assert (rig.code / "vendor/original.py").read_text() == "original = True\n"


async def test_transient_reconnect_failures_back_off(
    hass: HomeAssistant,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reconnect_sleeps: list[float],
) -> None:
    rig = _rig(hass, tmp_path, monkeypatch)
    rig.scripted.extend(
        [_ClosingShell(connect_error=_UNREACHABLE), _ClosingShell(connect_error=TimeoutError())]
    )

    raised = await rig.update_and_lose()

    assert isinstance(raised, asyncssh.ConnectionLost)
    assert reconnect_sleeps == [5.0, 10.0]
    assert len(rig.fresh) == 3
    assert all(isinstance(shell, _ClosingShell) and shell.closed for shell in rig.fresh[:2])
    assert await rig.journal.async_load() is None
    assert (rig.code / "vendor/original.py").read_text() == "original = True\n"


async def test_unreachable_panel_keeps_the_intent_with_a_typed_error(
    hass: HomeAssistant,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reconnect_sleeps: list[float],
) -> None:
    rig = _rig(hass, tmp_path, monkeypatch)
    rig.scripted.extend(_ClosingShell(connect_error=_UNREACHABLE) for _ in range(7))

    raised = await rig.update_and_lose()

    assert isinstance(raised, PanelProvisioningError) and raised.code == "rollback_failed"
    assert len(rig.fresh) == 5
    assert reconnect_sleeps == [5.0, 10.0, 20.0, 30.0]
    await _assert_intent_kept(rig, hass, "rollback_failed")


@pytest.mark.parametrize(
    ("shell", "code"),
    [
        (
            lambda: _ClosingShell(connect_error=asyncssh.PermissionDenied("bad")),
            "panel_authentication_failed",
        ),
        (
            lambda: _ClosingShell(connect_error=asyncssh.HostKeyNotVerifiable("changed")),
            "panel_identity_mismatch",
        ),
        (
            lambda: _ClosingShell(connect_error=TimeoutError(), reached_auth=True),
            "recovery_connection_failed",
        ),
    ],
    ids=["auth", "identity", "post_auth"],
)
async def test_permanent_reconnect_failures_are_not_retried(
    hass: HomeAssistant,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reconnect_sleeps: list[float],
    shell: Callable[[], _ClosingShell],
    code: str,
) -> None:
    rig = _rig(hass, tmp_path, monkeypatch)
    rig.scripted.extend([shell(), _ClosingShell()])

    raised = await rig.update_and_lose()

    assert isinstance(raised, PanelProvisioningError) and raised.code == code
    assert len(rig.fresh) == 1
    assert reconnect_sleeps == []
    await _assert_intent_kept(rig, hass, code)


async def test_mismatched_pin_is_refused_and_its_shell_closed(
    hass: HomeAssistant, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(hass, tmp_path, monkeypatch)
    mismatched = _ClosingShell(pinned=_OTHER_KEY)
    rig.scripted.append(mismatched)

    raised = await rig.update_and_lose()

    assert isinstance(raised, PanelProvisioningError)
    assert raised.code == "panel_identity_mismatch"
    assert mismatched.connect_count == 0
    assert mismatched.closed is True
    await _assert_intent_kept(rig, hass, "panel_identity_mismatch")


async def test_reconnect_deadline_is_a_connection_failure_not_a_rollback_deadline(
    hass: HomeAssistant, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(hass, tmp_path, monkeypatch)
    monkeypatch.setattr(panel_provisioner, "RECONNECT_TIMEOUT_SECONDS", 0.05)
    hanging = _ClosingShell(connect_gate=asyncio.Event())
    rig.scripted.append(hanging)

    raised = await rig.update_and_lose()

    assert isinstance(raised, PanelProvisioningError)
    assert raised.code == "recovery_connection_failed"
    assert hanging.closed is True
    await _assert_intent_kept(rig, hass, "recovery_connection_failed")


async def test_stop_interrupts_the_reconnect_and_says_so(
    hass: HomeAssistant, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(hass, tmp_path, monkeypatch)

    raised = await rig.update_and_lose(stopping=lambda: True)

    assert isinstance(raised, PanelProvisioningError)
    assert raised.code == "recovery_interrupted"
    assert rig.fresh == []
    await _assert_intent_kept(rig, hass, "recovery_interrupted")
    issue = next(iter(ir.async_get(hass).issues.values()))
    assert issue.translation_placeholders is not None
    reason = issue.translation_placeholders["reason"]
    assert "Home Assistant was stopping" in reason
    assert "unreachable" not in reason


async def test_caller_cancellation_stays_a_cancellation_and_keeps_the_intent(
    hass: HomeAssistant, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A cancelled update is not turned into a PanelProvisioningError (#219)."""
    rig = _rig(hass, tmp_path, monkeypatch)
    gate = asyncio.Event()
    reconnect = _ClosingShell(connect_error=asyncssh.PermissionDenied("bad"), connect_gate=gate)
    rig.scripted.append(reconnect)
    update = asyncio.create_task(rig.update_and_lose())
    await reconnect.connect_entered.wait()

    update.cancel()
    gate.set()
    # update_and_lose captures whatever the managed update raised.
    raised = await update

    assert isinstance(raised, asyncio.CancelledError)
    await _assert_intent_kept(rig, hass, "panel_authentication_failed")


async def test_stop_during_reconnect_backoff_ends_it_promptly(
    hass: HomeAssistant,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reconnect_sleeps: list[float],
) -> None:
    rig = _rig(hass, tmp_path, monkeypatch)
    rig.scripted.extend(_ClosingShell(connect_error=_UNREACHABLE) for _ in range(3))
    stopped = False

    def stopping() -> bool:
        return stopped

    def first_failure(_delay: float) -> None:
        nonlocal stopped
        stopped = True

    original = panel_provisioner._reconnect_backoff

    async def stop_then_wait(delay: float, check: Callable[[], bool]) -> bool:
        first_failure(delay)
        return await original(delay, check)

    monkeypatch.setattr(panel_provisioner, "_reconnect_backoff", stop_then_wait)

    raised = await rig.update_and_lose(stopping=stopping)

    assert isinstance(raised, PanelProvisioningError)
    assert raised.code == "recovery_interrupted"
    assert len(rig.fresh) == 1
    await _assert_intent_kept(rig, hass, "recovery_interrupted")


async def test_backoff_polls_for_a_stop_instead_of_sleeping_it_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(panel_provisioner, "_STOP_POLL_SECONDS", 0.01)
    loop = asyncio.get_running_loop()
    started = loop.time()
    stop_at = started + 0.05

    completed = await panel_provisioner._sleep_unless_stopping(30.0, lambda: loop.time() >= stop_at)

    assert completed is False
    assert loop.time() - started < 1.0
    assert await panel_provisioner._sleep_unless_stopping(0.02, lambda: False) is True
    assert await panel_provisioner._sleep_unless_stopping(30.0, lambda: True) is False


@pytest.mark.parametrize("when", ["backoff", "connect"])
@pytest.mark.parametrize("revoke", ["owner", "password", "key", "transaction", "record"])
async def test_fresh_rollback_rechecks_production_authority_after_awaits(
    hass: HomeAssistant,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    when: str,
    revoke: str,
) -> None:
    """Stored credentials cannot outlive current ownership or the exact journal."""
    rig = _rig(hass, tmp_path, monkeypatch)
    replaced = None

    async def revoke_authority() -> None:
        nonlocal replaced
        current = await rig.journal.async_load()
        assert current is not None
        if revoke in {"transaction", "record"}:
            replaced = (
                replace(current, transaction_id=uuid4(), setup_id=uuid4())
                if revoke == "transaction"
                else replace(current, staged_version="99.0.0")
            )
            await rig.journal._async_save(replaced)
        else:
            key, value = {
                "owner": (CONF_PANEL, "replacement-owner"),
                "password": (CONF_ROOT_PASSWORD, "revoked-password"),
                "key": (CONF_SSH_HOST_KEY, _OTHER_KEY),
            }[revoke]
            hass.config_entries.async_update_entry(rig.owner, data={**rig.owner.data, key: value})

    reconnect = fresh_view(rig.panel)
    if when == "backoff":
        rig.scripted.extend([_ClosingShell(connect_error=_UNREACHABLE), reconnect])

        async def backoff(_delay: float, _stopping: Callable[[], bool]) -> bool:
            await revoke_authority()
            return True

        monkeypatch.setattr(panel_provisioner, "_reconnect_backoff", backoff)
    else:
        connect = reconnect.connect

        async def connected_then_revoked() -> None:
            await connect()
            await revoke_authority()

        monkeypatch.setattr(reconnect, "connect", connected_then_revoked)
        rig.scripted.append(reconnect)

    raised = await rig.update_and_lose()

    assert isinstance(raised, PanelProvisioningError)
    assert raised.code in {"provisioning_recovery_required", "rollback_credentials_unavailable"}
    assert len(rig.fresh) == 1
    assert reconnect.commands == []
    assert reconnect.connected is False
    current = await rig.journal.async_load()
    assert current is not None
    if replaced is not None:
        assert current == replaced
    retained = await rig.journal.async_retained_records()
    assert len(retained) == 1 and retained[0].state == "restore_requested"
    assert (rig.code / "vendor/original.py").read_text() == "candidate = True\n"
    assert sum(_ACTIVATE in command for command in rig.panel.commands) == 1


async def test_lost_update_without_central_authorizer_preserves_journal_and_baseline(
    hass: HomeAssistant, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(hass, tmp_path, monkeypatch)
    rig.provisioner._recovery_authorizer = None

    raised = await rig.update_and_lose()

    assert isinstance(raised, PanelProvisioningError)
    assert raised.code == "provisioning_recovery_required"
    assert rig.fresh == []
    current = await rig.journal.async_load()
    assert current is not None and current.operation is ProvisioningOperation.UPDATE
    retained = await rig.journal.async_retained_records()
    assert len(retained) == 1 and retained[0].state == "armed"
    assert (rig.code / "vendor/original.py").read_text() == "candidate = True\n"


async def test_fresh_rollback_authorizes_update_then_current_rollback_operation(
    hass: HomeAssistant, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rig = _rig(hass, tmp_path, monkeypatch)
    authorize = rig.provisioner._recovery_authorizer
    assert authorize is not None
    operations: list[ProvisioningOperation] = []

    async def checked(record: ProvisioningRecord) -> None:
        assert await rig.journal.async_load() == record
        operations.append(record.operation)
        await authorize(record)

    monkeypatch.setattr(rig.provisioner, "_recovery_authorizer", checked)

    raised = await rig.update_and_lose()

    assert isinstance(raised, asyncssh.ConnectionLost)
    assert operations[0] is ProvisioningOperation.UPDATE
    assert operations.count(ProvisioningOperation.ROLLBACK) >= 2
    assert await rig.journal.async_load() is None


@pytest.mark.parametrize("boundary", ["factory", "restore"])
@pytest.mark.parametrize("revoke", ["owner", "password", "key"])
async def test_owner_revoked_during_final_journal_read_cannot_connect_or_restore(
    hass: HomeAssistant,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
    revoke: str,
) -> None:
    rig = _rig(hass, tmp_path, monkeypatch)
    authorize = rig.provisioner._recovery_authorizer
    assert authorize is not None
    load = ProvisioningJournal.async_load
    authorized = False
    revoked = False

    async def checked(record: ProvisioningRecord) -> None:
        nonlocal authorized
        await authorize(record)
        authorized = record.operation is ProvisioningOperation.ROLLBACK

    async def read_then_revoke(journal: ProvisioningJournal) -> ProvisioningRecord | None:
        nonlocal authorized, revoked
        current = await load(journal)
        if journal is not rig.provisioner._journal:
            return current
        connected = any(isinstance(shell, FakeShell) and shell.connected for shell in rig.fresh)
        here = "restore" if connected else "factory"
        if authorized and not revoked and boundary == here:
            key, value = {
                "owner": (CONF_PANEL, "replacement-owner"),
                "password": (CONF_ROOT_PASSWORD, "revoked-password"),
                "key": (CONF_SSH_HOST_KEY, _OTHER_KEY),
            }[revoke]
            hass.config_entries.async_update_entry(rig.owner, data={**rig.owner.data, key: value})
            revoked = True
        authorized = False
        return current

    monkeypatch.setattr(rig.provisioner, "_recovery_authorizer", checked)
    monkeypatch.setattr(ProvisioningJournal, "async_load", read_then_revoke)

    raised = await rig.update_and_lose()

    assert revoked
    assert isinstance(raised, PanelProvisioningError)
    assert raised.code == "rollback_credentials_unavailable"
    assert len(rig.fresh) == (0 if boundary == "factory" else 1)
    assert all(isinstance(shell, FakeShell) and not shell.commands for shell in rig.fresh)
    current = await rig.journal.async_load()
    assert current is not None and current.phase is ProvisioningPhase.ROLLBACK_PENDING
    retained = await rig.journal.async_retained(current.transaction_id)
    assert retained is not None and retained.state == "restore_requested"
    assert (rig.code / "vendor/original.py").read_text() == "candidate = True\n"
