"""Shared fixtures for the brilliant_mqtt integration tests."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterator, Mapping
from dataclasses import dataclass, field
from dataclasses import replace as _dc_replace
from pathlib import Path
from typing import Any
from unittest.mock import patch

import asyncssh
import pytest
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.brilliant_mqtt import components as _components
from custom_components.brilliant_mqtt import config_flow as _config_flow
from custom_components.brilliant_mqtt.config_flow import _PanelProbe
from custom_components.brilliant_mqtt.const import (
    COMPONENT_BRIDGE,
    COMPONENT_BUS_WATCHDOG,
    COMPONENT_VOICE,
    COMPONENT_WIFI_WATCHDOG,
    CONF_COMPONENTS,
    CONF_HOST,
    CONF_MESH_PRIORITY,
    CONF_MQTT_HOST,
    CONF_MQTT_PASSWORD,
    CONF_MQTT_PORT,
    CONF_MQTT_USERNAME,
    CONF_PANEL,
    CONF_ROOT_PASSWORD,
    CONF_VOICE_HA_HOST,
    CONF_VOICE_WAKE_WORD,
    DATA_SSH_HOST_KEY,
    DOMAIN,
)
from custom_components.brilliant_mqtt.manager import PanelManager
from custom_components.brilliant_mqtt.shell import PanelShell
from tests.fakes import FakeShell
from tests.test_init import ENTRY_DATA

_PROBE_PATH = "custom_components.brilliant_mqtt.config_flow._probe_panel"

# The key an unpinned re-pin connect captures (mirrors the rotated server key).
REPIN_NEW_KEY = "ssh-ed25519 NEWKEY"


@dataclass
class RepinShells:
    """Factory standing in for manager.AsyncsshShell during host-key-rotation tests.

    Keyed on the THIRD constructor arg (the pinned host key):

    - a non-``None`` pin → a FakeShell whose ``connect()`` raises
      ``asyncssh.HostKeyNotVerifiable`` (the rotated panel rejects the stored pin
      before auth).
    - a ``None`` pin → the UNPINNED re-pin connect; succeeds and reports
      ``REPIN_NEW_KEY``, UNLESS ``unpinned_connect_error`` is set (then it raises,
      modelling a panel that is also unreachable on the second attempt).

    Tests assert on ``pinned_shell`` / ``unpinned_shell``: if ``unpinned_shell`` is
    still ``None`` the OFF path never constructed an unpinned shell, i.e. the root
    password was never offered to the new-key host.
    """

    unpinned_connect_error: Exception | None = None
    pinned_shell: FakeShell | None = None
    unpinned_shell: FakeShell | None = None
    pins_seen: list[str | None] = field(default_factory=list)

    def __call__(self, host: str, password: str, pinned_host_key: str | None) -> FakeShell:
        self.pins_seen.append(pinned_host_key)
        if pinned_host_key is not None:
            self.pinned_shell = FakeShell(
                connect_error=asyncssh.HostKeyNotVerifiable("changed"), pinned=pinned_host_key
            )
            return self.pinned_shell
        self.unpinned_shell = FakeShell(
            connect_error=self.unpinned_connect_error, pinned=REPIN_NEW_KEY
        )
        return self.unpinned_shell


@pytest.fixture
def repin_shells() -> Iterator[RepinShells]:
    """Patch manager.AsyncsshShell with a pin-keyed factory (see RepinShells)."""
    factory = RepinShells()
    with patch("custom_components.brilliant_mqtt.manager.AsyncsshShell", side_effect=factory):
        yield factory


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(enable_custom_integrations: None) -> None:
    """Let the HA test harness load custom_components/ from this directory."""


@pytest.fixture
def payload_dir(tmp_path: Path) -> Iterator[Path]:
    """A minimal built agent payload, patched in as the bundled one."""
    (tmp_path / "app" / "brilliant_mqtt").mkdir(parents=True)
    (tmp_path / "vendor").mkdir()
    (tmp_path / "VERSION").write_text("0.2.0")
    (tmp_path / "brilliant-mqtt.service").write_text("[Unit]\nDescription=test unit\n")
    (tmp_path / "brilliant-wifi-watchdog.service").write_text(
        "[Unit]\nDescription=test wifi watchdog unit\n"
    )
    (tmp_path / "wifi_watchdog" / "brilliant_wifi_watchdog").mkdir(parents=True)
    (tmp_path / "wifi_watchdog" / "brilliant_wifi_watchdog" / "run.py").write_text("# stub\n")
    (tmp_path / "brilliant-bus-watchdog.service").write_text(
        "[Unit]\nDescription=test bus watchdog unit\n"
    )
    (tmp_path / "bus_watchdog" / "brilliant_bus_watchdog").mkdir(parents=True)
    (tmp_path / "bus_watchdog" / "brilliant_bus_watchdog" / "run.py").write_text("# stub\n")
    with patch("custom_components.brilliant_mqtt.manager._payload_dir", return_value=tmp_path):
        yield tmp_path


@pytest.fixture
def fake_shell() -> Iterator[FakeShell]:
    """Route every manager SSH op through one inspectable FakeShell.

    Its inspect probe reports a fully-installed panel (agent code + unit + env all
    present), so a repair takes the light path — rewrite config + enable, WITHOUT
    re-uploading the payload. Tests exercising the code-absent install path script
    their own shell with a ``payload=0`` inspect.
    """
    from custom_components.brilliant_mqtt import panel_ops
    from custom_components.brilliant_mqtt.shell import RunResult

    installed = RunResult(
        0, "unit=1\nenv=1\nenabled=1\nactive=1\nsunit=1\nsenv=1\npayload=1\n0.2.0\n", ""
    )
    shell = FakeShell(responses={panel_ops.INSPECT_COMMAND: installed})
    with patch("custom_components.brilliant_mqtt.manager.AsyncsshShell", return_value=shell):
        yield shell


@pytest.fixture
def expected_lingering_timers(request: pytest.FixtureRequest) -> bool:
    """Tolerate lingering timers ONLY for tests marked `allow_lingering_timers`
    (the core mqtt integration starts its own recurring timer via mqtt_mock).
    Every other test keeps the harness's strict guard, so a leaked manager timer
    fails loudly instead of hiding here."""
    return request.node.get_closest_marker("allow_lingering_timers") is not None


@pytest.fixture
def manager_with_fake_panel(hass: HomeAssistant) -> Iterator[PanelManager]:
    """PanelManager wired to a FakeShell with no pre-existing component selection.

    Used by the generic component install/remove tests (Task 4).  Entry data is
    plain ENTRY_DATA (no CONF_COMPONENTS key) so tests can verify the key is
    created on first use.  The voice-payload fetch (inside components._voice_install)
    is patched so no network call is made.
    """
    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)
    shell = FakeShell()
    with (
        patch("custom_components.brilliant_mqtt.manager.AsyncsshShell", return_value=shell),
        patch(
            "custom_components.brilliant_mqtt.components.async_fetch_voice_payload",
            return_value="/tmp/fake-voice.tar.gz",
        ),
    ):
        yield PanelManager(hass, entry, asyncio.Lock())


@pytest.fixture
def not_installed_panel() -> Iterator[None]:
    """Patch the step-1 probe to return a fresh panel (agent not installed, key pinned)."""
    probe = _PanelProbe(host_key="ssh-ed25519 PINNED", config=None)
    with patch(_PROBE_PATH, return_value=probe):
        yield


class _PatchInstallsResult:
    """Tracks which component IDs had install() or remove() called."""

    def __init__(self, called_ids: set[str], removed_ids: set[str]) -> None:
        self._called = called_ids
        self._removed = removed_ids

    def called(self, cid: str) -> bool:
        return cid in self._called

    def removed(self, cid: str) -> bool:
        return cid in self._removed


@pytest.fixture
def patch_installs() -> Iterator[_PatchInstallsResult]:
    """Replace REGISTRY install/remove callables with no-ops; track which IDs were invoked.

    Also patches config_flow.AsyncsshShell so _panel_session does not attempt
    real SSH — the mocked install/remove functions do not use the shell.
    """
    called_ids: set[str] = set()
    removed_ids: set[str] = set()

    def _make_install(
        cid: str,
    ) -> Callable[[HomeAssistant, PanelShell, Mapping[str, Any]], Awaitable[None]]:
        async def install(hass: HomeAssistant, shell: PanelShell, data: Mapping[str, Any]) -> None:
            called_ids.add(cid)

        return install

    def _make_remove(cid: str) -> Callable[[PanelShell], Awaitable[None]]:
        async def remove(shell: PanelShell) -> None:
            removed_ids.add(cid)

        return remove

    new_registry = {
        COMPONENT_BRIDGE: _dc_replace(
            _components.REGISTRY[COMPONENT_BRIDGE],
            install=_make_install(COMPONENT_BRIDGE),
            remove=_make_remove(COMPONENT_BRIDGE),
        ),
        COMPONENT_VOICE: _dc_replace(
            _components.REGISTRY[COMPONENT_VOICE],
            install=_make_install(COMPONENT_VOICE),
            remove=_make_remove(COMPONENT_VOICE),
        ),
        COMPONENT_WIFI_WATCHDOG: _dc_replace(
            _components.REGISTRY[COMPONENT_WIFI_WATCHDOG],
            install=_make_install(COMPONENT_WIFI_WATCHDOG),
            remove=_make_remove(COMPONENT_WIFI_WATCHDOG),
        ),
        COMPONENT_BUS_WATCHDOG: _dc_replace(
            _components.REGISTRY[COMPONENT_BUS_WATCHDOG],
            install=_make_install(COMPONENT_BUS_WATCHDOG),
            remove=_make_remove(COMPONENT_BUS_WATCHDOG),
        ),
    }

    install_shell = FakeShell()
    with (
        patch.dict(_components.REGISTRY, new_registry),
        patch.object(_config_flow, "AsyncsshShell", return_value=install_shell),
    ):
        yield _PatchInstallsResult(called_ids, removed_ids)


@pytest.fixture
def installed_voice_entry(hass: HomeAssistant) -> MockConfigEntry:
    """A config entry that already has the voice component installed."""
    data: dict[str, Any] = {
        CONF_PANEL: "office",
        CONF_HOST: "192.168.1.10",
        CONF_ROOT_PASSWORD: "panelpass",
        CONF_MQTT_HOST: "192.168.1.250",
        CONF_MQTT_PORT: 1883,
        CONF_MQTT_USERNAME: "brilliant",
        CONF_MQTT_PASSWORD: "mqttpass",
        CONF_MESH_PRIORITY: 0,
        DATA_SSH_HOST_KEY: "ssh-ed25519 STORED",
        CONF_COMPONENTS: {COMPONENT_BRIDGE: True, COMPONENT_VOICE: True},
        CONF_VOICE_WAKE_WORD: "okay_nabu",
        CONF_VOICE_HA_HOST: "",
    }
    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=data, version=2)
    entry.add_to_hass(hass)
    return entry
