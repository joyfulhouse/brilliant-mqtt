"""Managed updates fail closed when remote mutation results are unknown (#219)."""

from __future__ import annotations

import errno
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.helpers import issue_registry as ir
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.brilliant_mqtt import fleet_manager, panel_ops
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
)
from custom_components.brilliant_mqtt.shell import PanelShell
from tests.canary_rehearsal import RehearsalShell
from tests.test_init import ENTRY_DATA
from tests.test_panel_provisioner import _FakeObserver, _health
from tests.test_provisioning_journal import _record
from tests.update_harness import fresh_view

pytestmark = pytest.mark.usefixtures("durable_journal_store")

_UNREACHABLE = OSError(errno.EHOSTUNREACH, "No route to host")
_OTHER_KEY = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIBsF8pQ8C3EWxF7bGrXWvqJXV1fbNHo6p5FC0gIjJbqv"
_ACTIVATE = "BRILLIANT_TEST_ACTIVATE_CANDIDATE"


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


@pytest.mark.parametrize("lose", [False, True])
async def test_body_failure_keeps_unknown_intent_without_any_rollback(
    hass: HomeAssistant, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, lose: bool
) -> None:
    rig = _rig(hass, tmp_path, monkeypatch)
    raised = await rig.update_and_lose(lose=lose)
    assert isinstance(raised, PanelProvisioningError)
    assert raised.code == "mutation_outcome_unknown"
    assert rig.fresh == [] and rig.factory_keys == []
    record = await rig.journal.async_load()
    assert record is not None and record.operation is ProvisioningOperation.UPDATE
    assert record.phase is ProvisioningPhase.ACTIVATION_PENDING
    assert record.last_error is not None and record.last_error.code == "mutation_outcome_unknown"
    retained = await rig.journal.async_retained(record.transaction_id)
    assert retained is not None and retained.state == "armed"
    assert (rig.code / "vendor/original.py").read_text() == "candidate = True\n"
    issues = [issue for (domain, _), issue in ir.async_get(hass).issues.items() if domain == DOMAIN]
    assert len(issues) == 1
    assert "mutation_outcome_unknown" in str(issues[0].translation_placeholders)


@pytest.mark.parametrize("revocation", ["owner", "password", "key", "authorizer"])
async def test_unknown_update_never_consumes_stored_credentials(
    hass: HomeAssistant, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, revocation: str
) -> None:
    rig = _rig(hass, tmp_path, monkeypatch)
    raised = await rig.update_and_lose()
    assert isinstance(raised, PanelProvisioningError)
    if revocation == "owner":
        hass.config_entries._entries.pop(rig.owner.entry_id)
    elif revocation in {"password", "key"}:
        key = CONF_ROOT_PASSWORD if revocation == "password" else CONF_SSH_HOST_KEY
        hass.config_entries.async_update_entry(rig.owner, data={**rig.owner.data, key: "revoked"})
    else:
        rig.provisioner._recovery_authorizer = None
    record = await rig.journal.async_load()
    with pytest.raises(PanelProvisioningError, match="mutation_outcome_unknown"):
        await rig.provisioner.async_recover()
    assert rig.fresh == []
    assert await rig.journal.async_load() == record
