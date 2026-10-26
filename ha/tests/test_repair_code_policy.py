"""Repair and component enablement never change selected core code (#220).

A retained rollback baseline (on-panel marker or HA retained record in any state)
freezes a panel's bridge and watchdog code: repair may restore units, config and
service state for the incumbent, and refuses with a typed error when code would
be needed. Without a baseline, repair only bootstraps missing code; replacing
installed code with a newer release is left to the canaried update path.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.core import HomeAssistant

from custom_components.brilliant_mqtt import panel_ops
from custom_components.brilliant_mqtt.const import (
    COMPONENT_BRIDGE,
    COMPONENT_BUS_WATCHDOG,
    COMPONENT_WIFI_WATCHDOG,
    CONF_COMPONENTS,
    PANEL_BUS_WATCHDOG_UNIT_FILE,
    PANEL_CURRENT_LINK,
    PANEL_ENV_FILE,
    PANEL_UNIT_FILE,
    PANEL_WIFI_WATCHDOG_UNIT_FILE,
)
from custom_components.brilliant_mqtt.manager import PanelManager
from custom_components.brilliant_mqtt.provisioning_journal import (
    ProvisioningJournal,
    RetainedRollback,
)
from custom_components.brilliant_mqtt.release_identity import RollbackBaseline
from custom_components.brilliant_mqtt.shell import RunResult
from tests.fakes import FakeShell
from tests.test_manager import _fleet_panel_manager
from tests.test_panel_ops import _encoded_file
from tests.test_provisioning_journal import _record

_TRIGGERS = ("auto", "button", "service")
_RETAINED = ("panel_marker", "soak", "restored")
_AGENT_INSTALLED = RunResult(
    0, "unit=0\nenv=1\nenabled=0\nactive=0\nsunit=1\nsenv=1\npayload=1\n0.1.9\n", ""
)
_AGENT_CODELESS = RunResult(
    0, "unit=0\nenv=0\nenabled=0\nactive=0\nsunit=0\nsenv=0\npayload=0\n", ""
)
_WATCHDOGS = {
    COMPONENT_WIFI_WATCHDOG: (
        "brilliant-wifi-watchdog",
        PANEL_WIFI_WATCHDOG_UNIT_FILE,
        panel_ops._WATCHDOG_STAGED_UNIT,
        panel_ops.WIFI_WATCHDOG_INSPECT_COMMAND,
    ),
    COMPONENT_BUS_WATCHDOG: (
        "brilliant-bus-watchdog",
        PANEL_BUS_WATCHDOG_UNIT_FILE,
        panel_ops._BUS_WATCHDOG_STAGED_UNIT,
        panel_ops.BUS_WATCHDOG_INSPECT_COMMAND,
    ),
}


def _older_identity(digest: str = "a" * 64) -> dict[str, object]:
    return {
        "version": "0.1.9",
        "release_ordinal": 9,
        "digest": digest,
        "deployment_id": "incumbent-deployment",
        "layout": "release_link",
    }


def _probe(shell: FakeShell, path: str, content: bytes | None) -> None:
    command = panel_ops._file_probe_command(path, panel_ops.MAX_SNAPSHOT_FILE_BYTES)
    shell.responses[command] = _encoded_file(content, None if content is None else 0o644)


def _installed_shell(payload_dir: Path, *, watchdog: str | None = None) -> FakeShell:
    """An installed panel on an OLDER release than the loaded bundle, /etc wiped."""
    (payload_dir / "RELEASE_ORDINAL").write_text("10\n")
    shell = FakeShell(responses={panel_ops.INSPECT_COMMAND: _AGENT_INSTALLED})
    shell.release_identities["bridge"] = _older_identity()
    for path in (PANEL_UNIT_FILE, panel_ops._STAGED_UNIT, PANEL_ENV_FILE, panel_ops._STAGED_ENV):
        _probe(shell, path, None)
    _probe(shell, f"{PANEL_CURRENT_LINK}/brilliant-mqtt-release.service", b"incumbent unit\n")
    if watchdog is not None:
        service, live, staged, inspect = _WATCHDOGS[watchdog]
        shell.release_identities[watchdog] = _older_identity("b" * 64)
        shell.responses[inspect] = RunResult(
            0, "unit=0\nenabled=0\nactive=0\npayload=1\n0.1.9\n", ""
        )
        _probe(shell, live, None)
        _probe(shell, staged, None)
        _probe(shell, f"{PANEL_CURRENT_LINK}/{service}-release.service", b"incumbent wd unit\n")
    return shell


def _retain(monkeypatch: pytest.MonkeyPatch, shell: FakeShell, retained: str) -> None:
    """Retain a rollback baseline for panel ``office`` from one authority."""
    if retained == "none":
        return
    if retained == "panel_marker":
        shell.baseline_retained = True
        return
    record = _record()
    record = replace(
        record,
        prior_snapshot=replace(
            record.prior_snapshot, baseline=RollbackBaseline("c" * 32, "d" * 64, {})
        ),
    )
    held = RetainedRollback(record, retained, "a" * 32 if retained == "restored" else None)
    monkeypatch.setattr(
        ProvisioningJournal, "async_retained_records", AsyncMock(return_value=(held,))
    )


def _code_writes(shell: FakeShell) -> list[str]:
    swaps = {
        panel_ops._swap_command(),
        panel_ops._watchdog_swap_command(),
        panel_ops._bus_watchdog_swap_command(),
    }
    uploads = [
        path for path, _data, _mode in shell.uploads if path.endswith((".tar.gz", "VERSION"))
    ]
    return [command for command in shell.commands if command in swaps] + uploads


async def _repair(manager: PanelManager, shell: FakeShell, trigger: str) -> AsyncMock:
    manager.availability = "offline"
    stage = AsyncMock(return_value="MQTT_TLS_ENABLED=0\n")
    with (
        patch.object(manager, "_connect_for_repair", AsyncMock(return_value=shell)),
        patch.object(manager, "_async_stage_broker_ca", stage),
        patch.object(manager, "_auto_repair_still_warranted", return_value=True),
    ):
        await shell.connect()
        await manager.async_repair(trigger=trigger)
    return stage


@pytest.mark.parametrize("trigger", _TRIGGERS)
@pytest.mark.parametrize("retained", ("none", *_RETAINED))
async def test_repair_restores_incumbent_config_without_changing_code(
    hass: HomeAssistant,
    payload_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    retained: str,
    trigger: str,
) -> None:
    """A newer loaded bundle never reaches an installed panel through repair."""
    shell = _installed_shell(payload_dir)
    before = dict(shell.release_identities["bridge"] or {})
    _entry, manager = _fleet_panel_manager(hass)
    _retain(monkeypatch, shell, retained)

    await _repair(manager, shell, trigger)

    assert manager.problem_reason is None
    assert _code_writes(shell) == []
    after = shell.release_identities["bridge"]
    assert after is not None
    assert {key: after[key] for key in ("version", "release_ordinal", "digest", "layout")} == {
        key: before[key] for key in ("version", "release_ordinal", "digest", "layout")
    }
    # Non-code repair still runs: the incumbent's own unit, regenerated env, enable.
    units = [data for path, data, _mode in shell.uploads if path == PANEL_UNIT_FILE]
    assert units == [b"incumbent unit\n"]
    assert any(path == PANEL_ENV_FILE for path, _data, _mode in shell.uploads)
    assert "systemctl enable --now brilliant-mqtt" in shell.commands
    await manager.async_shutdown()


@pytest.mark.parametrize("trigger", _TRIGGERS)
@pytest.mark.parametrize("retained", _RETAINED)
async def test_codeless_repair_refuses_while_baseline_retained(
    hass: HomeAssistant,
    payload_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    retained: str,
    trigger: str,
) -> None:
    shell = FakeShell(responses={panel_ops.INSPECT_COMMAND: _AGENT_CODELESS})
    _entry, manager = _fleet_panel_manager(hass)
    _retain(monkeypatch, shell, retained)

    stage = await _repair(manager, shell, trigger)

    assert manager.problem_reason is not None
    assert "baseline_retained_finalize_first" in manager.problem_reason
    assert "finalize" in manager.problem_reason.partition("baseline_retained_finalize_first")[2]
    assert not stage.await_count
    assert not shell.uploads
    assert not any(command.startswith("systemctl") for command in shell.commands)
    await manager.async_shutdown()


@pytest.mark.parametrize("trigger", _TRIGGERS)
async def test_codeless_repair_still_bootstraps_without_baseline(
    hass: HomeAssistant, payload_dir: Path, trigger: str
) -> None:
    del payload_dir
    shell = FakeShell(responses={panel_ops.INSPECT_COMMAND: _AGENT_CODELESS})
    _entry, manager = _fleet_panel_manager(hass)

    await _repair(manager, shell, trigger)

    assert manager.problem_reason is None
    assert panel_ops._swap_command() in shell.commands
    assert "systemctl enable --now brilliant-mqtt" in shell.commands
    await manager.async_shutdown()


@pytest.mark.parametrize("watchdog", (COMPONENT_WIFI_WATCHDOG, COMPONENT_BUS_WATCHDOG))
@pytest.mark.parametrize("retained", ("none", *_RETAINED))
async def test_repair_relays_watchdog_without_changing_its_code(
    hass: HomeAssistant,
    payload_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    retained: str,
    watchdog: str,
) -> None:
    service, live, _staged, _inspect = _WATCHDOGS[watchdog]
    shell = _installed_shell(payload_dir, watchdog=watchdog)
    before = dict(shell.release_identities[watchdog] or {})
    _entry, manager = _fleet_panel_manager(
        hass, panel_overrides={CONF_COMPONENTS: {COMPONENT_BRIDGE: True, watchdog: True}}
    )
    _retain(monkeypatch, shell, retained)

    await _repair(manager, shell, "auto")

    assert manager.problem_reason is None
    assert _code_writes(shell) == []
    after = shell.release_identities[watchdog]
    assert after is not None
    assert after["digest"] == before["digest"]
    units = [data for path, data, _mode in shell.uploads if path == live]
    assert units == [b"incumbent wd unit\n"]
    assert f"systemctl enable --now {service}" in shell.commands
    assert f"systemctl restart {service}" not in shell.commands
    await manager.async_shutdown()


@pytest.mark.parametrize("watchdog", (COMPONENT_WIFI_WATCHDOG, COMPONENT_BUS_WATCHDOG))
@pytest.mark.parametrize("installed", ("absent", "older"))
@pytest.mark.parametrize("retained", _RETAINED)
async def test_watchdog_enablement_refuses_code_while_baseline_retained(
    hass: HomeAssistant,
    payload_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    retained: str,
    installed: str,
    watchdog: str,
) -> None:
    shell = _installed_shell(payload_dir, watchdog=watchdog if installed == "older" else None)
    _entry, manager = _fleet_panel_manager(hass)
    _retain(monkeypatch, shell, retained)

    with (
        patch.object(manager, "_connect_for_repair", AsyncMock(return_value=shell)),
        pytest.raises(panel_ops.PanelOpError, match="baseline_retained_finalize_first"),
    ):
        await shell.connect()
        await manager.async_install_component(watchdog)

    assert _code_writes(shell) == []
    assert not shell.uploads
    assert not any(command.startswith("systemctl") for command in shell.commands)
    await manager.async_shutdown()


@pytest.mark.parametrize("watchdog", (COMPONENT_WIFI_WATCHDOG, COMPONENT_BUS_WATCHDOG))
async def test_watchdog_enablement_never_upgrades_installed_code(
    hass: HomeAssistant, payload_dir: Path, watchdog: str
) -> None:
    shell = _installed_shell(payload_dir, watchdog=watchdog)
    _entry, manager = _fleet_panel_manager(hass)

    with (
        patch.object(manager, "_connect_for_repair", AsyncMock(return_value=shell)),
        pytest.raises(panel_ops.PanelOpError, match="release_update_required"),
    ):
        await shell.connect()
        await manager.async_install_component(watchdog)

    assert _code_writes(shell) == []
    await manager.async_shutdown()


@pytest.mark.parametrize("watchdog", (COMPONENT_WIFI_WATCHDOG, COMPONENT_BUS_WATCHDOG))
async def test_watchdog_enablement_still_bootstraps_absent_code(
    hass: HomeAssistant, payload_dir: Path, watchdog: str
) -> None:
    service, _live, _staged, _inspect = _WATCHDOGS[watchdog]
    shell = FakeShell()
    _entry, manager = _fleet_panel_manager(hass)

    with patch.object(manager, "_connect_for_repair", AsyncMock(return_value=shell)):
        await shell.connect()
        await manager.async_install_component(watchdog)

    assert _code_writes(shell)
    assert f"systemctl enable --now {service}" in shell.commands
    await manager.async_shutdown()


def test_code_policy_refusals_are_operator_safe_details() -> None:
    from custom_components.brilliant_mqtt.manager import _safe_failure_summary

    for code in ("baseline_retained_finalize_first", "release_update_required"):
        summary = _safe_failure_summary("panel repair step failed", panel_ops.PanelOpError(code))
        assert f"({code}: " in summary


@pytest.mark.parametrize("watchdog", (COMPONENT_WIFI_WATCHDOG, COMPONENT_BUS_WATCHDOG))
async def test_post_ota_refresh_never_upgrades_installed_watchdog(
    hass: HomeAssistant, payload_dir: Path, watchdog: str
) -> None:
    """The automatic post-OTA refresh is no rollout path for a newer watchdog either."""
    service, live, _staged, _inspect = _WATCHDOGS[watchdog]
    shell = _installed_shell(payload_dir, watchdog=watchdog)
    candidate = (await panel_ops.candidate_identities(str(payload_dir)))["bridge"]
    shell.release_identities["bridge"] = {
        **candidate.as_dict(),
        "deployment_id": "incumbent-deployment",
        "layout": "release_link",
    }
    before = dict(shell.release_identities[watchdog] or {})
    _entry, manager = _fleet_panel_manager(
        hass, panel_overrides={CONF_COMPONENTS: {COMPONENT_BRIDGE: True, watchdog: True}}
    )
    stage = AsyncMock(return_value="MQTT_TLS_ENABLED=0\n")
    with (
        patch.object(manager, "_shell", return_value=shell),
        patch.object(manager, "_async_stage_broker_ca", stage),
    ):
        await manager._refresh_staged_copies()

    assert _code_writes(shell) == []
    after = shell.release_identities[watchdog]
    assert after is not None
    assert after["digest"] == before["digest"]
    assert [data for path, data, _mode in shell.uploads if path == live] == [b"incumbent wd unit\n"]
    assert f"systemctl enable --now {service}" in shell.commands
    await manager.async_shutdown()
