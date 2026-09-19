"""Release admission regressions: no devices, network, or clock-dependent controls."""

from __future__ import annotations

import asyncio
import json
import shutil
import sys
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError

from custom_components.brilliant_mqtt import components, panel_ops
from custom_components.brilliant_mqtt.const import COMPONENT_BRIDGE
from custom_components.brilliant_mqtt.shell import RunResult
from tests.fakes import FakeShell
from tests.test_components import _bridge_data
from tests.test_manager import _fleet_panel_manager


def _identity(digest: str = "a" * 64, ordinal: int | None = 9) -> dict[str, object]:
    return {
        "version": "0.2.0",
        "release_ordinal": ordinal,
        "digest": digest,
        "deployment_id": "incumbent-deployment",
        "layout": "release_link",
    }


class IdentityShell(FakeShell):
    """A deterministic identity transport; ordinary writes remain inspectable."""

    def __init__(self, installed: dict[str, object] | None) -> None:
        super().__init__()
        self.release_identities["bridge"] = installed
        self.records: list[dict[str, Any]] = []
        self.consumed: set[str] = set()

    async def run(self, command: str) -> RunResult:
        if "BRILLIANT_RELEASE_IDENTITY_READ" in command:
            self.commands.append(command)
            return RunResult(0, json.dumps(self.release_identities), "")
        if "BRILLIANT_RELEASE_IDENTITY_WRITE" in command and "'override-" in command:
            name = command.split("'override-", 1)[1].split("'", 1)[0]
            if name in self.consumed:
                return RunResult(1, "", "")
            self.consumed.add(name)
        return await super().run(command)


@pytest.mark.parametrize("caller", ["update", "repair", "auto", "component", "registry", "refresh"])
async def test_every_caller_refuses_before_ca_or_config_writes(
    hass: HomeAssistant, payload_dir: Path, caller: str
) -> None:
    shell = IdentityShell(_identity())
    _entry, manager = _fleet_panel_manager(hass)
    manager.availability = "offline"
    stage = AsyncMock(return_value="MQTT_TLS_ENABLED=0\n")
    with (
        patch.object(manager, "_connect_for_repair", AsyncMock(return_value=shell)),
        patch.object(manager, "_shell", return_value=shell),
        patch.object(manager, "_async_stage_broker_ca", stage),
        patch.object(manager, "_auto_repair_still_warranted", return_value=True),
    ):
        await shell.connect()
        if caller == "update":
            with pytest.raises(HomeAssistantError) as raised:
                await manager.async_update_agent()
            assert raised.value.translation_placeholders
            assert "release_identity_blocked" in raised.value.translation_placeholders["error"]
        elif caller in {"repair", "auto"}:
            await manager.async_repair(trigger="auto" if caller == "auto" else "service")
            assert manager.problem_reason and "release_identity_blocked" in manager.problem_reason
        elif caller == "component":
            with pytest.raises(panel_ops.PanelOpError, match="release_identity_blocked"):
                await manager.async_install_component(COMPONENT_BRIDGE)
        elif caller == "registry":
            with pytest.raises(panel_ops.PanelOpError, match="release_identity_blocked"):
                await components._bridge_install(hass, shell, _bridge_data(tls_enabled=False))
        else:
            await manager._refresh_staged_copies()
        assert not stage.await_count
        assert not shell.uploads
        assert not any("systemctl restart" in command for command in shell.commands)
    await manager.async_shutdown()


@pytest.mark.parametrize("writer", ["deploy", "config", "wifi", "bus", "wifi_unit", "bus_unit"])
async def test_shared_writers_cannot_bypass_admission(
    payload_dir: Path, writer: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    shell = IdentityShell(_identity())
    await shell.connect()
    monkeypatch.setattr(panel_ops, "_identity_payload_dir", lambda: payload_dir, raising=False)
    with pytest.raises(panel_ops.PanelOpError, match="release_identity_blocked"):
        if writer == "deploy":
            await panel_ops.deploy_payload(shell, str(payload_dir), "0.2.0")
        elif writer == "config":
            await panel_ops.ensure_configs(shell, "legacy unit", "ENV")
        elif writer == "wifi":
            await panel_ops.deploy_wifi_watchdog(shell, str(payload_dir / "wifi_watchdog"), "0.2.0")
        elif writer == "bus":
            await panel_ops.deploy_bus_watchdog(shell, str(payload_dir / "bus_watchdog"), "0.2.0")
        elif writer == "wifi_unit":
            await panel_ops.ensure_wifi_watchdog_unit(shell, "legacy unit")
        else:
            await panel_ops.ensure_bus_watchdog_unit(shell, "legacy unit")
    assert not shell.uploads


async def test_same_bytes_update_preserves_selection_and_all_payload_writes(
    hass: HomeAssistant, payload_dir: Path
) -> None:
    candidate = await panel_ops.candidate_identities(str(payload_dir))
    installed = {**candidate["bridge"].as_dict(), "layout": "release_link"}
    shell = IdentityShell(installed)
    _entry, manager = _fleet_panel_manager(hass)
    with patch.object(manager, "_connect_for_repair", AsyncMock(return_value=shell)):
        await shell.connect()
        await manager.async_update_agent()
    assert not shell.uploads
    assert not any("systemctl" in command or "rm -rf" in command for command in shell.commands)
    await manager.async_shutdown()


@pytest.mark.parametrize(
    "incumbent,candidate,allowed",
    [(9, 8, False), (9, 9, False), (9, 10, True), (None, 10, False), (9, None, False)],
)
async def test_policy_orders_only_known_ordinals(
    payload_dir: Path, incumbent: int | None, candidate: int | None, allowed: bool
) -> None:
    from custom_components.brilliant_mqtt.release_identity import ReleaseIdentity, admit_identity

    old = ReleaseIdentity.from_dict(_identity(ordinal=incumbent))
    new = ReleaseIdentity.from_dict(_identity(digest="0" * 64, ordinal=candidate))
    if allowed:
        assert admit_identity(old, new) is True
    else:
        with pytest.raises(ValueError, match="release_identity_blocked"):
            admit_identity(old, new)


async def test_override_is_bound_consumed_recorded_and_not_inherited(payload_dir: Path) -> None:
    shell = IdentityShell(_identity())
    await shell.connect()
    with pytest.raises(panel_ops.ReleaseIdentityBlocked) as blocked:
        async with panel_ops.release_transaction(shell, str(payload_dir), panel="office"):
            pytest.fail("blocked transaction entered")
    override = blocked.value.override
    for key, bad in (
        ("panel", "other"),
        ("transaction", "invalid"),
        ("incumbent", {}),
        ("candidate", {}),
    ):
        with pytest.raises(panel_ops.PanelOpError):
            async with panel_ops.release_transaction(
                shell, str(payload_dir), panel="office", override={**override, key: bad}
            ):
                pytest.fail("mismatched override entered")
    async with panel_ops.release_transaction(
        shell, str(payload_dir), panel="office", override=override
    ) as admission:
        assert not admission.noop
    audit = [
        json.loads(data)
        for path, data, mode in shell.identity_uploads
        if "override" in path and mode == 0o600
    ]
    assert audit == [override]
    with pytest.raises(panel_ops.ReleaseIdentityBlocked):
        async with panel_ops.release_transaction(shell, str(payload_dir), panel="office"):
            pytest.fail("automatic operation inherited override")
    with pytest.raises(panel_ops.PanelOpError, match="override"):
        async with panel_ops.release_transaction(
            shell, str(payload_dir), panel="office", override=override
        ):
            pytest.fail("override replay entered")


async def test_bootstrap_hashes_live_tree_and_enrolls_unknown_ordinal(tmp_path: Path) -> None:
    from custom_components.brilliant_mqtt.agent_payload import bundle_manifest

    root = tmp_path / "panel"
    (root / "app").mkdir(parents=True)
    (root / "vendor").mkdir()
    (root / "app/main.py").write_text("actual incumbent bytes")
    (root / "VERSION").write_text("0.2.0")
    identities = bundle_manifest.installed_identities(root)
    record = identities["bridge"]
    assert record is not None
    assert record["release_ordinal"] is None
    first_digest = record["digest"]
    bundle_manifest.private_record(root, "bridge", {**record, "release_ordinal": 12})
    unchanged = bundle_manifest.installed_identities(root)["bridge"]
    assert unchanged is not None and unchanged["release_ordinal"] == 12
    (root / "app/main.py").write_text("changed incumbent bytes")
    changed = bundle_manifest.installed_identities(root)["bridge"]
    assert changed is not None and changed["digest"] != first_digest
    assert changed["release_ordinal"] is None
    assert (root / ".release-identities/bridge.json").stat().st_mode & 0o777 == 0o600


async def test_presence_probe_follows_release_link(tmp_path: Path) -> None:
    import asyncio

    root = tmp_path / "panel"
    release = root / "releases/one"
    (release / "app/brilliant_mqtt").mkdir(parents=True)
    (release / "app/brilliant_mqtt/__main__.py").write_text("pass")
    (release / "vendor").mkdir()
    (root / "current").symlink_to(release)
    command = panel_ops.INSPECT_COMMAND.replace("/var/brilliant-mqtt", str(root))
    command = command.replace("systemctl", "false")
    process = await asyncio.create_subprocess_exec(
        "sh", "-c", command, stdout=asyncio.subprocess.PIPE
    )
    stdout, _ = await process.communicate()
    assert b"payload=1" in stdout


async def test_override_executes_update_and_auto_repair_cannot_reuse_it(
    hass: HomeAssistant, payload_dir: Path
) -> None:
    shell = IdentityShell(_identity())
    _entry, manager = _fleet_panel_manager(hass)
    with patch.object(manager, "_connect_for_repair", AsyncMock(return_value=shell)):
        await shell.connect()
        with pytest.raises(panel_ops.ReleaseIdentityBlocked) as blocked:
            async with panel_ops.release_transaction(shell, str(payload_dir), panel="office"):
                pytest.fail("unapproved downgrade")
        await manager.async_update_agent(release_override=blocked.value.override)
        assert panel_ops._swap_command() in shell.commands
        assert shell.release_identities["bridge"] is not None
        assert shell.release_identities["bridge"]["release_ordinal"] == 1
        assert shell.identity_uploads[0][0].startswith("/var/brilliant-mqtt/.release-override-")
        shell.release_identities["bridge"] = _identity(digest="b" * 64)
        shell.uploads.clear()
        manager.availability = "offline"
        with patch.object(manager, "_auto_repair_still_warranted", return_value=True):
            await shell.connect()
            await manager.async_repair(trigger="auto")
        assert manager.problem_reason and "release_identity_blocked" in manager.problem_reason
        assert not shell.uploads
    await manager.async_shutdown()


async def test_shared_swap_revalidates_incumbent_after_upload(payload_dir: Path) -> None:
    class RacingShell(IdentityShell):
        async def put_bytes(self, data: bytes, remote_path: str, mode: int) -> None:
            await super().put_bytes(data, remote_path, mode)
            if remote_path.endswith(".tar.gz"):
                self.release_identities["bridge"] = _identity(ordinal=1)

    shell = RacingShell(None)
    await shell.connect()
    with pytest.raises(panel_ops.PanelOpError, match="release_identity_changed"):
        await panel_ops.deploy_payload(shell, str(payload_dir), "0.2.0")
    assert panel_ops._swap_command() not in shell.commands


async def test_known_newer_ordinal_allows_same_version_update_without_override(
    hass: HomeAssistant, payload_dir: Path
) -> None:
    (payload_dir / "RELEASE_ORDINAL").write_text("10\n")
    shell = IdentityShell(_identity(ordinal=9))
    _entry, manager = _fleet_panel_manager(hass)
    with patch.object(manager, "_connect_for_repair", AsyncMock(return_value=shell)):
        await shell.connect()
        await manager.async_update_agent()
    identity = shell.release_identities["bridge"]
    assert identity is not None and identity["release_ordinal"] == 10
    assert identity["version"] == "0.2.0"
    assert not any("override" in path for path, _, _ in shell.identity_uploads)
    await manager.async_shutdown()


async def test_selected_watchdog_blocks_whole_update_before_ca(
    hass: HomeAssistant, payload_dir: Path
) -> None:
    from custom_components.brilliant_mqtt.const import CONF_COMPONENTS

    shell = IdentityShell(None)
    shell.release_identities["bus_watchdog"] = _identity()
    _entry, manager = _fleet_panel_manager(
        hass, panel_overrides={CONF_COMPONENTS: {"bridge": True, "bus_watchdog": True}}
    )
    stage = AsyncMock()
    with (
        patch.object(manager, "_connect_for_repair", AsyncMock(return_value=shell)),
        patch.object(manager, "_async_stage_broker_ca", stage),
    ):
        await shell.connect()
        with pytest.raises(HomeAssistantError):
            await manager.async_update_agent()
    assert not stage.await_count and not shell.uploads
    assert manager.problem_reason and "release_identity_blocked" in manager.problem_reason
    await manager.async_shutdown()


def test_identity_and_transaction_repr_are_redacted() -> None:
    from custom_components.brilliant_mqtt.release_identity import ReleaseIdentity

    identity = ReleaseIdentity.from_dict(_identity())
    assert repr(identity) == "ReleaseIdentity(<redacted>)"
    error = panel_ops.ReleaseIdentityBlocked({"panel": "private-panel"})
    assert "private-panel" not in repr(error)


async def test_same_code_config_repair_restores_release_selector_not_legacy(
    payload_dir: Path,
) -> None:
    from tests.test_panel_ops import _encoded_file

    candidate = (await panel_ops.candidate_identities(str(payload_dir)))["bridge"]
    shell = IdentityShell({**candidate.as_dict(), "layout": "release_link"})
    source = "/var/brilliant-mqtt/current/brilliant-mqtt-release.service"
    selected = b"Environment=PYTHONPATH=/var/brilliant-mqtt/current/app\n"
    paths = (
        "/etc/systemd/system/brilliant-mqtt.service",
        "/var/brilliant-mqtt/system/brilliant-mqtt.service",
        "/etc/brilliant-mqtt.env",
        "/var/brilliant-mqtt/system/brilliant-mqtt.env",
    )
    for path in paths:
        shell.responses[panel_ops._file_probe_command(path, panel_ops.MAX_SNAPSHOT_FILE_BYTES)] = (
            _encoded_file(None, None)
        )
    shell.responses[panel_ops._file_probe_command(source, panel_ops.MAX_SNAPSHOT_FILE_BYTES)] = (
        _encoded_file(selected, 0o644)
    )
    await shell.connect()
    async with panel_ops.release_transaction(shell, str(payload_dir), panel="office"):
        await panel_ops.ensure_configs(shell, "legacy code selector", "MQTT_TLS_ENABLED=0\n")
    units = [data for path, data, _mode in shell.uploads if path.endswith(".service")]
    assert units == [selected, selected]
    assert not any("rm -rf" in command for command in shell.commands)


async def test_real_identity_probe_and_override_swap_in_disposable_filesystem(
    payload_dir: Path, tmp_path: Path
) -> None:
    """Execute production hashing/publication/swap commands under a path-mapped fake."""
    panel = tmp_path / "device"
    units = tmp_path / "units"
    units.mkdir()
    root = Path(__file__).parents[2]
    (payload_dir / "brilliant-mqtt.service").write_bytes(
        (root / "deploy/brilliant-mqtt.service").read_bytes()
    )
    release = panel / "releases/incumbent"
    (release / "app").mkdir(parents=True)
    (release / "vendor").mkdir()
    (release / "app/old.py").write_text("incumbent = True\n")
    (release / "VERSION").write_text("0.2.0")
    (panel / "current").symlink_to(release)
    old_unit = (
        (root / "deploy/brilliant-mqtt-release.service")
        .read_bytes()
        .replace(b"/var/brilliant-mqtt", str(panel).encode())
    )
    (units / "brilliant-mqtt.service").write_bytes(old_unit)

    class FilesystemShell(FakeShell):
        def translate(self, value: str) -> str:
            return (
                value.replace("/var/brilliant-mqtt", str(panel))
                .replace("/etc/systemd/system", str(units))
                .replace("/etc/brilliant-mqtt.env", str(tmp_path / "environment"))
                .replace(panel_ops._PANEL_PYTHON, sys.executable)
                .replace("systemctl daemon-reload", "true")
            )

        async def run(self, command: str) -> RunResult:
            process = await asyncio.create_subprocess_exec(
                "sh",
                "-c",
                self.translate(command),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()
            assert process.returncode is not None
            return RunResult(process.returncode, stdout.decode(), stderr.decode())

        async def put_bytes(self, data: bytes, remote_path: str, mode: int) -> None:
            target = Path(self.translate(remote_path))
            if remote_path.endswith(".service"):
                data = self.translate(data.decode()).encode()
            await asyncio.to_thread(target.write_bytes, data)
            await asyncio.to_thread(target.chmod, mode)

    # The payload must be separate from the disposable panel tree before archiving.
    candidate = tmp_path.parent / (tmp_path.name + "-candidate")
    shutil.copytree(payload_dir, candidate, ignore=shutil.ignore_patterns("device", "units"))
    shell = FilesystemShell()
    await shell.connect()
    with pytest.raises(panel_ops.ReleaseIdentityBlocked) as blocked:
        async with panel_ops.release_transaction(shell, str(candidate), panel="office"):
            pytest.fail("bootstrap ordering must stay unknown")
    record = json.loads((panel / ".release-identities/bridge.json").read_bytes())
    assert record["release_ordinal"] is None
    assert (units / "brilliant-mqtt.service").read_bytes() == old_unit
    async with panel_ops.release_transaction(
        shell, str(candidate), panel="office", override=blocked.value.override
    ):
        await panel_ops.deploy_payload(shell, str(candidate), "0.2.0")
        await panel_ops.ensure_configs(
            shell, (candidate / "brilliant-mqtt.service").read_text(), "MQTT_TLS_ENABLED=0\n"
        )
    observed = await panel_ops._read_release_identities(shell)
    identity = observed["bridge"]
    assert identity is not None and identity.release_ordinal == 1
    assert identity.layout == "legacy_fixed"
    assert (
        identity.digest == (await panel_ops.candidate_identities(str(candidate)))["bridge"].digest
    )
    assert (release / "app/old.py").read_text() == "incumbent = True\n"
    audit = list((panel / ".release-identities").glob("override-*.json"))
    assert len(audit) == 1 and audit[0].stat().st_mode & 0o777 == 0o600
    # An OTA can remove /etc while the retained predecessor's current link remains.
    (units / "brilliant-mqtt.service").unlink()
    after_ota = (await panel_ops._read_release_identities(shell))["bridge"]
    assert after_ota == identity


@pytest.mark.allow_lingering_timers
@pytest.mark.parametrize("single_target", [True, False])
async def test_redeploy_override_service_requires_one_target_and_forwards_binding(
    hass: HomeAssistant, mqtt_mock: object, single_target: bool
) -> None:
    from homeassistant.helpers import entity_registry as er

    from custom_components.brilliant_mqtt.const import DOMAIN
    from tests.test_services import _setup_fleet_entry

    entry = await _setup_fleet_entry(hass)
    office = entry.runtime_data.panels["panel-office"]
    kitchen = entry.runtime_data.panels["panel-kitchen"]
    office.async_update_agent = AsyncMock()
    kitchen.async_update_agent = AsyncMock()
    override: dict[str, object] = {"panel": "office", "transaction": "f" * 32}
    data: dict[str, object] = {"release_override": override}
    if single_target:
        entity = er.async_get(hass).async_get_entity_id(
            "binary_sensor", DOMAIN, "SHA256:office_bridge_health"
        )
        assert entity is not None
        data["entity_id"] = entity
        await hass.services.async_call(DOMAIN, "redeploy", data, blocking=True)
        office.async_update_agent.assert_awaited_once_with(release_override=override)
    else:
        with pytest.raises(HomeAssistantError, match="exactly one"):
            await hass.services.async_call(DOMAIN, "redeploy", data, blocking=True)
        office.async_update_agent.assert_not_awaited()
    kitchen.async_update_agent.assert_not_awaited()
    assert await hass.config_entries.async_unload(entry.entry_id)
