"""Regression coverage for payload staging event-loop safety."""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import patch

from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.brilliant_mqtt.const import CONF_HOST, CONF_PANEL, DOMAIN
from custom_components.brilliant_mqtt.entry_data import LegacyPanelStore
from custom_components.brilliant_mqtt.fleet_manager import legacy_fleet_config
from custom_components.brilliant_mqtt.manager import PanelManager
from tests.fakes import FakeShell
from tests.test_init import ENTRY_DATA


class _NoRecursiveUploadShell(FakeShell):
    async def put_dir(self, local_dir: str, remote_dir: str) -> None:
        del local_dir, remote_dir
        raise AssertionError("deploy_payload reached recursive SFTP upload")


async def test_concurrent_agent_updates_serialize_on_existing_fleet_lock(
    hass: HomeAssistant, payload_dir: Path
) -> None:
    """Both updates finish, while only one panel enters SSH at a time."""
    del payload_dir
    first_gate = asyncio.Event()
    shells = [
        _NoRecursiveUploadShell(connect_gate=first_gate),
        _NoRecursiveUploadShell(),
    ]
    entries = [
        MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA),
        MockConfigEntry(
            domain=DOMAIN,
            unique_id="kitchen",
            data={**ENTRY_DATA, CONF_PANEL: "kitchen", CONF_HOST: "192.168.1.11"},
        ),
    ]
    for entry in entries:
        entry.add_to_hass(hass)
    fleet_lock = asyncio.Lock()
    managers = [
        PanelManager(
            hass,
            LegacyPanelStore(hass, entry),
            legacy_fleet_config(entry),
            fleet_lock,
        )
        for entry in entries
    ]

    with patch(
        "custom_components.brilliant_mqtt.manager.LegacyAsyncsshShell",
        side_effect=shells,
    ):
        first_update = asyncio.create_task(managers[0].async_update_agent())
        await shells[0].connect_entered.wait()
        second_update = asyncio.create_task(managers[1].async_update_agent())
        await asyncio.sleep(0)
        assert not shells[1].connect_entered.is_set()

        first_gate.set()
        await asyncio.gather(first_update, second_update)

    assert all(shell.connect_count == 1 for shell in shells)
    assert all(not shell.dir_uploads for shell in shells)
    await asyncio.gather(*(manager.async_shutdown() for manager in managers))
