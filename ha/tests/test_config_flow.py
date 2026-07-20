"""Config flow: detection-first onboarding, adopt-installed, broadened reconfigure."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import asyncssh
import pytest
import voluptuous as vol
import voluptuous_serialize
from homeassistant.const import CONF_NAME
from homeassistant.core import HomeAssistant
from homeassistant.helpers import config_validation as cv
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.brilliant_mqtt import async_migrate_entry, components, config_flow, panel_ops
from custom_components.brilliant_mqtt.config_flow import _PanelProbe, _slugify, _WrongPanelError
from custom_components.brilliant_mqtt.const import (
    BLE_OBSERVER_SERVICE_NAME,
    COMPONENT_BLE_OBSERVER,
    COMPONENT_BRIDGE,
    COMPONENT_BUS_WATCHDOG,
    COMPONENT_HA_MIRROR,
    COMPONENT_HUE_CA,
    COMPONENT_VOICE,
    COMPONENT_WIFI_WATCHDOG,
    CONF_BLE_OBSERVER_ALLOWLIST_JSON,
    CONF_BLE_SCANNER_ENABLED,
    CONF_COMPONENTS,
    CONF_HA_CONTROL_DOMAINS,
    CONF_HA_CONTROL_ENABLED,
    CONF_HA_CONTROL_LABEL,
    CONF_HA_MIRROR_LABEL,
    CONF_HA_MIRROR_LEADER_PRIORITY,
    CONF_HA_MIRROR_TOKEN,
    CONF_HA_MIRROR_WS_URL,
    CONF_HOST,
    CONF_HUE_CA_CERT,
    CONF_MAX_MIRRORED_ENTITIES,
    CONF_MESH_PRIORITY,
    CONF_MQTT_HOST,
    CONF_MQTT_PASSWORD,
    CONF_MQTT_PORT,
    CONF_MQTT_USERNAME,
    CONF_PANEL,
    CONF_ROOM_OVERRIDES,
    CONF_ROOT_PASSWORD,
    CONF_SCENE_ACTIONS,
    CONF_SCENE_PANEL,
    CONF_VOICE_ENABLED,
    CONF_VOICE_HA_HOST,
    CONF_VOICE_WAKE_WORD,
    DATA_SSH_HOST_KEY,
    DEFAULT_HA_CONTROL_DOMAINS,
    DEFAULT_HA_CONTROL_ENABLED,
    DEFAULT_HA_CONTROL_LABEL,
    DEFAULT_MAX_MIRRORED_ENTITIES,
    DOMAIN,
    OPT_AUTO_REPAIR,
    OPT_OFFLINE_GRACE_MINUTES,
    OPT_REPAIR_COOLDOWN_MINUTES,
    OPT_TRUST_HOST_KEY_CHANGES,
    PANEL_BLE_OBSERVER_UNIT_FILE,
    PANEL_ENV_FILE,
    PANEL_HUE_CA_CERT_FILE,
)
from custom_components.brilliant_mqtt.shell import PanelShell, RunResult
from custom_components.brilliant_mqtt.voice_payload import VoicePayloadError
from tests.fakes import FakeShell, SequencedResponseShell

PROBE = "custom_components.brilliant_mqtt.config_flow._probe_panel"
APPLY = "custom_components.brilliant_mqtt.config_flow._apply_config"

CONNECT_INPUT = {CONF_HOST: "192.168.1.10", CONF_ROOT_PASSWORD: "panelpass"}
MQTT_INPUT = {
    CONF_MQTT_HOST: "192.168.1.250",
    CONF_MQTT_PORT: 1883,
    CONF_MQTT_USERNAME: "brilliant",
    CONF_MQTT_PASSWORD: "mqttpass",
}
SCRIPT_INPUT = {
    CONF_NAME: "Office Bath",
    CONF_MESH_PRIORITY: 1,
    COMPONENT_VOICE: False,
    COMPONENT_BLE_OBSERVER: False,
    CONF_BLE_SCANNER_ENABLED: False,
    CONF_BLE_OBSERVER_ALLOWLIST_JSON: "[]",
    CONF_VOICE_WAKE_WORD: "okay_nabu",
    CONF_VOICE_HA_HOST: "",
    CONF_HA_CONTROL_ENABLED: DEFAULT_HA_CONTROL_ENABLED,
    CONF_HA_CONTROL_LABEL: DEFAULT_HA_CONTROL_LABEL,
    CONF_ROOM_OVERRIDES: "{}",
    CONF_HA_CONTROL_DOMAINS: list(DEFAULT_HA_CONTROL_DOMAINS),
    CONF_MAX_MIRRORED_ENTITIES: DEFAULT_MAX_MIRRORED_ENTITIES,
    CONF_SCENE_PANEL: "office-bath",
    CONF_SCENE_ACTIONS: "{}",
}

FETCH_VOICE = "custom_components.brilliant_mqtt.components.async_fetch_voice_payload"

RECONFIG_INPUT = {
    CONF_HOST: "192.168.1.10",
    CONF_ROOT_PASSWORD: "newpass",
    CONF_MQTT_HOST: "192.168.1.250",
    CONF_MQTT_PORT: 1883,
    CONF_MQTT_USERNAME: "brilliant",
    CONF_MQTT_PASSWORD: "newbroker",
    CONF_MESH_PRIORITY: 5,
    COMPONENT_VOICE: False,
    COMPONENT_WIFI_WATCHDOG: False,
    COMPONENT_BLE_OBSERVER: False,
    CONF_BLE_SCANNER_ENABLED: False,
    CONF_BLE_OBSERVER_ALLOWLIST_JSON: "[]",
    CONF_VOICE_WAKE_WORD: "okay_nabu",
    CONF_VOICE_HA_HOST: "",
    CONF_HA_CONTROL_ENABLED: DEFAULT_HA_CONTROL_ENABLED,
    CONF_HA_CONTROL_LABEL: DEFAULT_HA_CONTROL_LABEL,
    CONF_ROOM_OVERRIDES: "{}",
    CONF_HA_CONTROL_DOMAINS: list(DEFAULT_HA_CONTROL_DOMAINS),
    CONF_MAX_MIRRORED_ENTITIES: DEFAULT_MAX_MIRRORED_ENTITIES,
    CONF_SCENE_PANEL: "office",
    CONF_SCENE_ACTIONS: "{}",
}


def _not_installed(key: str = "ssh-ed25519 PINNED") -> _PanelProbe:
    return _PanelProbe(host_key=key, config=None)


def _installed(env: dict[str, str], key: str = "ssh-ed25519 PINNED") -> _PanelProbe:
    return _PanelProbe(host_key=key, config=env)


def _env(panel: str = "office", **over: Any) -> dict[str, str]:
    fields: dict[str, Any] = {
        "panel": panel,
        "mesh_priority": 3,
        "mqtt_host": "192.168.1.250",
        "mqtt_port": 8883,
        "mqtt_username": "brilliant",
        "mqtt_password": "frombroker",
    }
    fields.update(over)
    return panel_ops.parse_env(panel_ops.render_env(**fields))


def _full_entry(hass: HomeAssistant, **over: Any) -> MockConfigEntry:
    data: dict[str, Any] = {
        CONF_PANEL: "office",
        CONF_HOST: "192.168.1.10",
        CONF_ROOT_PASSWORD: "oldpass",
        CONF_MQTT_HOST: "old.broker",
        CONF_MQTT_PORT: 1883,
        CONF_MQTT_USERNAME: "brilliant",
        CONF_MQTT_PASSWORD: "oldbroker",
        CONF_MESH_PRIORITY: 0,
        DATA_SSH_HOST_KEY: "ssh-ed25519 STORED",
    }
    data.update(over)
    entry = MockConfigEntry(domain=DOMAIN, unique_id=data[CONF_PANEL], data=data, version=2)
    entry.add_to_hass(hass)
    return entry


def _suggested_values(result: Any) -> dict[str, Any]:
    """The form's per-field suggested values (what add_suggested_values_to_schema set)."""
    schema = result["data_schema"]
    assert schema is not None
    out: dict[str, Any] = {}
    for marker in schema.schema:
        desc = getattr(marker, "description", None)
        if isinstance(desc, dict) and "suggested_value" in desc:
            out[str(marker)] = desc["suggested_value"]
    return out


# --- slugify ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "slug"),
    [
        ("Office Bath", "office-bath"),
        ("  Office  ", "office"),
        ("office_front", "office_front"),
        ("Panel 2", "panel-2"),
        ("Garage (Left)", "garage-left"),
        ("!!!", ""),
    ],
)
def test_slugify(name: str, slug: str) -> None:
    assert _slugify(name) == slug


# --- onboarding: not installed (three steps) -------------------------------


async def test_not_installed_walks_three_steps(hass: HomeAssistant, payload_dir: Path) -> None:
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    assert result["type"] == "form" and result["step_id"] == "user"

    with patch(PROBE, return_value=_not_installed()):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], CONNECT_INPUT)
    assert result["type"] == "form" and result["step_id"] == "broker"

    result = await hass.config_entries.flow.async_configure(result["flow_id"], MQTT_INPUT)
    assert result["type"] == "form" and result["step_id"] == "script"

    # Step 3 now INSTALLS the agent over SSH before the entry is created.
    install_shell = FakeShell()
    with patch.object(config_flow, "AsyncsshShell", return_value=install_shell):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], SCRIPT_INPUT)
    assert result["type"] == "create_entry"
    assert result["title"] == "Brilliant office-bath"
    data = result["data"]
    assert data[CONF_PANEL] == "office-bath"  # slugified from "Office Bath"
    assert data[CONF_HOST] == "192.168.1.10"
    assert data[CONF_ROOT_PASSWORD] == "panelpass"
    assert data[DATA_SSH_HOST_KEY] == "ssh-ed25519 PINNED"
    assert data[CONF_MQTT_HOST] == "192.168.1.250"
    assert data[CONF_MQTT_PASSWORD] == "mqttpass"
    assert data[CONF_MESH_PRIORITY] == 1

    # The agent was actually installed: payload uploaded, unit/env written, service enabled.
    assert install_shell.dir_uploads  # deploy_payload pushed app/+vendor/
    assert "systemctl enable --now brilliant-mqtt" in install_shell.commands
    env_blob = next(d for (p, d, _m) in install_shell.uploads if p == "/etc/brilliant-mqtt.env")
    assert b'BRILLIANT_PANEL="office-bath"' in env_blob  # the slug the operator named
    assert b'MQTT_HOST="192.168.1.250"' in env_blob  # the broker entered in step 2


async def test_not_installed_install_failure_shows_error(
    hass: HomeAssistant, payload_dir: Path
) -> None:
    """A failed SSH install keeps the script step open with cannot_install and creates
    no entry, so the operator can fix the panel and retry."""
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(PROBE, return_value=_not_installed()):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], CONNECT_INPUT)
    result = await hass.config_entries.flow.async_configure(result["flow_id"], MQTT_INPUT)

    # enable --now exits non-zero → PanelOpError out of the install.
    failing = FakeShell(
        responses={"systemctl enable --now brilliant-mqtt": RunResult(1, "", "boom")}
    )
    with patch.object(config_flow, "AsyncsshShell", return_value=failing):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], SCRIPT_INPUT)
    assert result["type"] == "form" and result["step_id"] == "script"
    assert result["errors"] == {"base": "cannot_install"}
    assert not hass.config_entries.async_entries(DOMAIN)  # nothing was created


async def test_not_installed_install_aborts_on_unreadable_bundle(
    hass: HomeAssistant, tmp_path: Path
) -> None:
    """A missing/corrupt bundled payload (VERSION/unit unreadable) surfaces as
    cannot_install rather than crashing the flow — the reads are inside the try."""
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(PROBE, return_value=_not_installed()):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], CONNECT_INPUT)
    result = await hass.config_entries.flow.async_configure(result["flow_id"], MQTT_INPUT)

    empty = tmp_path / "empty"  # no brilliant-mqtt.service / VERSION → read_text raises
    empty.mkdir()
    with (
        patch("custom_components.brilliant_mqtt.manager._payload_dir", return_value=empty),
        patch.object(config_flow, "AsyncsshShell", return_value=FakeShell()),
    ):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], SCRIPT_INPUT)
    assert result["type"] == "form" and result["errors"] == {"base": "cannot_install"}
    assert not hass.config_entries.async_entries(DOMAIN)


async def test_step1_only_requires_host_and_password(hass: HomeAssistant) -> None:
    """The first form asks for exactly host + root password — nothing else."""
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    schema = result["data_schema"]
    assert schema is not None
    assert {str(marker) for marker in schema.schema} == {CONF_HOST, CONF_ROOT_PASSWORD}


async def test_step1_cannot_connect(hass: HomeAssistant) -> None:
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(PROBE, side_effect=OSError("nope")):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], CONNECT_INPUT)
    assert result["type"] == "form" and result["errors"] == {"base": "cannot_connect"}


async def test_step1_rejects_control_char_before_probing(hass: HomeAssistant) -> None:
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(PROBE) as probe:
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {**CONNECT_INPUT, CONF_ROOT_PASSWORD: "bad\npass"}
        )
    assert result["errors"] == {CONF_ROOT_PASSWORD: "invalid_value"}
    probe.assert_not_called()


async def test_step1_rejects_control_char_in_host_not_strips_it(hass: HomeAssistant) -> None:
    """A control char on the host edge is rejected, not silently stripped to a valid value."""
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(PROBE) as probe:
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {**CONNECT_INPUT, CONF_HOST: "192.168.1.10\n"}
        )
    assert result["errors"] == {CONF_HOST: "invalid_value"}
    probe.assert_not_called()


async def test_mqtt_step_rejects_control_char(hass: HomeAssistant) -> None:
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(PROBE, return_value=_not_installed()):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], CONNECT_INPUT)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {**MQTT_INPUT, CONF_MQTT_PASSWORD: "bad\npass"}
    )
    assert result["type"] == "form" and result["step_id"] == "broker"
    assert result["errors"] == {CONF_MQTT_PASSWORD: "invalid_value"}


async def test_broker_redisplay_preserves_typed_values(hass: HomeAssistant) -> None:
    """An error on the broker step re-shows what the operator typed, not the prior prefill."""
    _full_entry(hass, **{CONF_MQTT_HOST: "192.168.1.250", CONF_MQTT_PASSWORD: "shared"})
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(PROBE, return_value=_not_installed()):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], CONNECT_INPUT)
    typed = {**MQTT_INPUT, CONF_MQTT_HOST: "10.9.9.9", CONF_MQTT_PASSWORD: "bad\npass"}
    result = await hass.config_entries.flow.async_configure(result["flow_id"], typed)
    assert result["step_id"] == "broker"
    assert result["errors"] == {CONF_MQTT_PASSWORD: "invalid_value"}
    suggested = _suggested_values(result)
    assert suggested[CONF_MQTT_HOST] == "10.9.9.9"  # typed value, not the prior "192.168.1.250"
    assert suggested[CONF_MQTT_USERNAME] == "brilliant"


async def test_mqtt_step_prefills_from_prior_panel(hass: HomeAssistant) -> None:
    _full_entry(hass, **{CONF_MQTT_HOST: "192.168.1.250", CONF_MQTT_PASSWORD: "shared"})
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(PROBE, return_value=_not_installed()):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], CONNECT_INPUT)
    assert result["step_id"] == "broker"
    schema = result["data_schema"]
    assert schema is not None
    defaults = {
        str(marker): marker.default()
        for marker in schema.schema
        if marker.default is not vol.UNDEFINED
    }
    assert defaults[CONF_MQTT_HOST] == "192.168.1.250"
    assert defaults[CONF_MQTT_PASSWORD] == "shared"


@pytest.mark.parametrize(
    ("name", "error"),
    [("!!!", "invalid_name"), ("mesh", "reserved_panel")],
)
async def test_script_step_rejects_bad_name(hass: HomeAssistant, name: str, error: str) -> None:
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(PROBE, return_value=_not_installed()):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], CONNECT_INPUT)
    result = await hass.config_entries.flow.async_configure(result["flow_id"], MQTT_INPUT)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {**SCRIPT_INPUT, CONF_NAME: name}
    )
    assert result["type"] == "form" and result["errors"] == {CONF_NAME: error}


async def test_not_installed_duplicate_name_aborts(hass: HomeAssistant) -> None:
    MockConfigEntry(
        domain=DOMAIN, unique_id="office-bath", data={CONF_PANEL: "office-bath"}
    ).add_to_hass(hass)
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(PROBE, return_value=_not_installed()):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], CONNECT_INPUT)
    result = await hass.config_entries.flow.async_configure(result["flow_id"], MQTT_INPUT)
    result = await hass.config_entries.flow.async_configure(result["flow_id"], SCRIPT_INPUT)
    assert result["type"] == "abort" and result["reason"] == "already_configured"


# --- onboarding: voice opt-in ----------------------------------------------


async def test_voice_disabled_no_voice_install(hass: HomeAssistant, payload_dir: Path) -> None:
    """Finishing onboarding with voice_enabled=False stores the flag and skips voice install."""
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(PROBE, return_value=_not_installed()):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], CONNECT_INPUT)
    result = await hass.config_entries.flow.async_configure(result["flow_id"], MQTT_INPUT)

    install_shell = FakeShell()
    with (
        patch.object(config_flow, "AsyncsshShell", return_value=install_shell),
        patch(FETCH_VOICE) as mock_fetch,
    ):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], SCRIPT_INPUT)

    assert result["type"] == "create_entry"
    data = result["data"]
    assert data[CONF_COMPONENTS][COMPONENT_VOICE] is False
    assert data[CONF_VOICE_WAKE_WORD] == "okay_nabu"
    assert data[CONF_VOICE_HA_HOST] == ""
    # Voice install was NOT triggered.
    mock_fetch.assert_not_called()
    assert not install_shell.file_uploads  # no voice tarball uploaded
    assert not any("brilliant-voice" in cmd for cmd in install_shell.commands)


async def test_voice_enabled_installs_satellite(hass: HomeAssistant, payload_dir: Path) -> None:
    """Enabling voice installs the satellite and stores all three voice keys in entry data."""
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(PROBE, return_value=_not_installed()):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], CONNECT_INPUT)
    result = await hass.config_entries.flow.async_configure(result["flow_id"], MQTT_INPUT)

    install_shell = FakeShell()
    voice_input = {
        **SCRIPT_INPUT,
        COMPONENT_VOICE: True,
        CONF_VOICE_WAKE_WORD: "hey_jarvis",
        CONF_VOICE_HA_HOST: "192.168.1.10",
    }
    fake_tarball = "/tmp/brilliant-voice-payload-0.1.0.tar.gz"
    with (
        patch.object(config_flow, "AsyncsshShell", return_value=install_shell),
        patch(FETCH_VOICE, return_value=fake_tarball),
    ):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], voice_input)

    assert result["type"] == "create_entry"
    data = result["data"]
    assert data[CONF_COMPONENTS][COMPONENT_VOICE] is True
    assert data[CONF_VOICE_WAKE_WORD] == "hey_jarvis"
    assert data[CONF_VOICE_HA_HOST] == "192.168.1.10"
    # Voice tarball was uploaded via put_file.
    assert install_shell.file_uploads, "expected voice tarball to be uploaded"
    voice_upload_paths = [remote for (_local, remote, _mode) in install_shell.file_uploads]
    assert any("brilliant-voice" in p for p in voice_upload_paths)
    # Voice service was enabled.
    assert "systemctl enable --now brilliant-voice" in install_shell.commands


async def test_voice_install_failure_shows_error_no_entry(
    hass: HomeAssistant, payload_dir: Path
) -> None:
    """A VoicePayloadError after the agent succeeds shows cannot_install_voice; no entry created."""
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(PROBE, return_value=_not_installed()):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], CONNECT_INPUT)
    result = await hass.config_entries.flow.async_configure(result["flow_id"], MQTT_INPUT)

    install_shell = FakeShell()
    voice_input = {**SCRIPT_INPUT, COMPONENT_VOICE: True, CONF_VOICE_WAKE_WORD: "okay_nabu"}
    with (
        patch.object(config_flow, "AsyncsshShell", return_value=install_shell),
        patch(FETCH_VOICE, side_effect=VoicePayloadError("download failed")),
    ):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], voice_input)

    assert result["type"] == "form" and result["step_id"] == "script"
    assert result["errors"] == {"base": "cannot_install_voice"}
    assert not hass.config_entries.async_entries(DOMAIN)  # nothing was created


async def test_agent_install_failure_still_cannot_install(
    hass: HomeAssistant, payload_dir: Path
) -> None:
    """An agent SSH failure still reports cannot_install (not cannot_install_voice)."""
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(PROBE, return_value=_not_installed()):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], CONNECT_INPUT)
    result = await hass.config_entries.flow.async_configure(result["flow_id"], MQTT_INPUT)

    failing = FakeShell(
        responses={"systemctl enable --now brilliant-mqtt": RunResult(1, "", "boom")}
    )
    with (
        patch.object(config_flow, "AsyncsshShell", return_value=failing),
        patch(FETCH_VOICE) as mock_fetch,
    ):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {**SCRIPT_INPUT, COMPONENT_VOICE: True}
        )
    assert result["type"] == "form" and result["step_id"] == "script"
    assert result["errors"] == {"base": "cannot_install"}
    # Voice fetch never reached when agent install fails.
    mock_fetch.assert_not_called()
    assert not hass.config_entries.async_entries(DOMAIN)


async def test_voice_component_ssh_failure_shows_voice_error_no_entry(
    hass: HomeAssistant, payload_dir: Path
) -> None:
    """An SSH/OSError during voice component install (after bridge succeeds) shows
    cannot_install_voice and creates no entry."""
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(PROBE, return_value=_not_installed()):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], CONNECT_INPUT)
    result = await hass.config_entries.flow.async_configure(result["flow_id"], MQTT_INPUT)

    voice_input = {**SCRIPT_INPUT, COMPONENT_VOICE: True, CONF_VOICE_WAKE_WORD: "okay_nabu"}
    with (
        patch.object(config_flow, "AsyncsshShell", return_value=FakeShell()),
        patch(FETCH_VOICE, return_value="/tmp/fake-voice.tar.gz"),
        patch.object(panel_ops, "deploy_voice_payload", side_effect=OSError("ssh fail")),
    ):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], voice_input)

    assert result["type"] == "form" and result["step_id"] == "script"
    assert result["errors"] == {"base": "cannot_install_voice"}
    assert not hass.config_entries.async_entries(DOMAIN)


async def test_script_failure_before_ble_quarantines_desired_observer(
    hass: HomeAssistant,
    not_installed_panel: None,
    patch_installs: Any,
) -> None:
    """Bridge env may be BLE-on before voice fails, so onboarding must remove that state."""
    del patch_installs
    result = await _drive_flow_to_script(hass)
    shell = FakeShell(
        responses={
            panel_ops.BLE_OBSERVER_ACTIVE_COMMAND: RunResult(3, "inactive\n", ""),
        }
    )

    async def fail_voice(*_args: object, **_kwargs: object) -> None:
        raise VoicePayloadError("download failed")

    voice = replace(components.REGISTRY[COMPONENT_VOICE], install=fail_voice)
    with (
        patch.object(config_flow, "AsyncsshShell", return_value=shell),
        patch.dict(components.REGISTRY, {COMPONENT_VOICE: voice}),
    ):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {
                **SCRIPT_INPUT,
                COMPONENT_VOICE: True,
                COMPONENT_BLE_OBSERVER: True,
                CONF_BLE_OBSERVER_ALLOWLIST_JSON: ('[{"address":"AA:BB:CC:DD:EE:FF"}]'),
            },
        )

    assert result["type"] == "form" and result["errors"] == {"base": "cannot_install_voice"}
    assert not hass.config_entries.async_entries(DOMAIN)
    assert f"systemctl disable --now {BLE_OBSERVER_SERVICE_NAME}" in shell.commands
    assert any(PANEL_BLE_OBSERVER_UNIT_FILE in command for command in shell.commands)


async def test_script_partial_ble_enable_failure_is_quarantined_before_no_entry(
    hass: HomeAssistant,
    not_installed_panel: None,
    patch_installs: Any,
) -> None:
    """A BLE install that mutates systemd and then raises cannot outlive failed setup."""
    del patch_installs
    result = await _drive_flow_to_script(hass)
    shell = FakeShell(
        responses={
            panel_ops.BLE_OBSERVER_ACTIVE_COMMAND: RunResult(3, "inactive\n", ""),
        }
    )
    enable = f"systemctl enable --now {BLE_OBSERVER_SERVICE_NAME}"

    async def partially_enable(
        _hass: HomeAssistant,
        target: PanelShell,
        _data: Mapping[str, Any],
    ) -> None:
        await target.run(enable)
        raise panel_ops.PanelOpError("enable reported failure after mutation")

    observer = replace(components.REGISTRY[COMPONENT_BLE_OBSERVER], install=partially_enable)
    with (
        patch.object(config_flow, "AsyncsshShell", return_value=shell),
        patch.dict(components.REGISTRY, {COMPONENT_BLE_OBSERVER: observer}),
    ):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {
                **SCRIPT_INPUT,
                COMPONENT_BLE_OBSERVER: True,
                CONF_BLE_OBSERVER_ALLOWLIST_JSON: ('[{"address":"AA:BB:CC:DD:EE:FF"}]'),
            },
        )

    assert result["type"] == "form" and result["errors"] == {"base": "cannot_install"}
    assert not hass.config_entries.async_entries(DOMAIN)
    disable = f"systemctl disable --now {BLE_OBSERVER_SERVICE_NAME}"
    assert shell.commands.index(enable) < shell.commands.index(disable)
    assert any(PANEL_BLE_OBSERVER_UNIT_FILE in command for command in shell.commands)


async def test_script_default_off_proves_stale_active_observer_stopped_before_entry(
    hass: HomeAssistant,
    not_installed_panel: None,
    patch_installs: Any,
) -> None:
    """Fresh default-off setup reconciles an older active observer before persisting."""
    del patch_installs
    result = await _drive_flow_to_script(hass)
    shell = SequencedResponseShell(
        {
            panel_ops.BLE_OBSERVER_ACTIVE_COMMAND: [
                RunResult(0, "active\n", ""),
                RunResult(3, "inactive\n", ""),
            ]
        }
    )

    with patch.object(config_flow, "AsyncsshShell", return_value=shell):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], SCRIPT_INPUT)

    assert result["type"] == "create_entry"
    assert result["data"][CONF_COMPONENTS][COMPONENT_BLE_OBSERVER] is False
    disable = f"systemctl disable --now {BLE_OBSERVER_SERVICE_NAME}"
    stop = f"systemctl stop {BLE_OBSERVER_SERVICE_NAME}"
    assert shell.commands.index(disable) < shell.commands.index(stop)
    assert any(PANEL_BLE_OBSERVER_UNIT_FILE in command for command in shell.commands)


async def test_script_default_off_never_creates_entry_without_inactive_proof(
    hass: HomeAssistant,
    not_installed_panel: None,
    patch_installs: Any,
) -> None:
    """An observer that remains active makes onboarding fail, with no unowned entry gap."""
    del patch_installs
    result = await _drive_flow_to_script(hass)
    shell = FakeShell(
        responses={
            panel_ops.BLE_OBSERVER_ACTIVE_COMMAND: RunResult(0, "active\n", ""),
        }
    )

    with patch.object(config_flow, "AsyncsshShell", return_value=shell):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], SCRIPT_INPUT)

    assert result["type"] == "form" and result["errors"] == {"base": "cannot_install"}
    assert not hass.config_entries.async_entries(DOMAIN)
    assert shell.commands.count(f"systemctl stop {BLE_OBSERVER_SERVICE_NAME}") == 2
    assert not any(PANEL_BLE_OBSERVER_UNIT_FILE in command for command in shell.commands)


async def test_hue_ca_enabled_persists_cert_through_initial_onboarding(
    hass: HomeAssistant, payload_dir: Path
) -> None:
    """Checking hue_ca + typing a CA PEM during FRESH onboarding installs the hook and
    persists CONF_HUE_CA_CERT in entry_data (regression: async_step_script's entry_data
    allowlist previously omitted it, so the CA was silently dropped and _hue_ca_install
    raised PanelOpError on an empty CA -> cannot_install, blocking onboarding)."""
    (payload_dir / "brilliant-hue-ca.service").write_text("[Unit]\nDescription=test hue-ca unit\n")
    (payload_dir / "brilliant-hue-ca.timer").write_text("[Unit]\nDescription=test hue-ca timer\n")

    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(PROBE, return_value=_not_installed()):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], CONNECT_INPUT)
    result = await hass.config_entries.flow.async_configure(result["flow_id"], MQTT_INPUT)

    ca_pem = "-----BEGIN CERTIFICATE-----\nFAKECA\n-----END CERTIFICATE-----\n"
    hue_ca_input = {**SCRIPT_INPUT, COMPONENT_HUE_CA: True, CONF_HUE_CA_CERT: ca_pem}
    install_shell = FakeShell()
    with (
        patch.object(config_flow, "AsyncsshShell", return_value=install_shell),
        patch(FETCH_VOICE) as mock_fetch,
    ):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], hue_ca_input)

    assert result["type"] == "create_entry"
    data = result["data"]
    assert data[CONF_COMPONENTS][COMPONENT_HUE_CA] is True
    assert data[CONF_HUE_CA_CERT] == ca_pem
    mock_fetch.assert_not_called()  # voice stayed disabled
    # The CA actually reached the panel — proves _hue_ca_install saw the real value
    # (not the empty string it saw before the fix, which raised PanelOpError).
    # _hue_ca_install strips the PEM before writing it, hence .strip() here too.
    assert (PANEL_HUE_CA_CERT_FILE, ca_pem.strip().encode(), 0o644) in install_shell.uploads


async def test_script_step_rejects_control_char_in_voice_ha_host(
    hass: HomeAssistant, payload_dir: Path
) -> None:
    """Bug D: a control char in voice_ha_host re-shows the form with invalid_value and
    creates NO entry — instead of crashing the flow when render_voice_env → _env_quote
    raises ValueError (which the voice except does not catch)."""
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(PROBE, return_value=_not_installed()):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], CONNECT_INPUT)
    result = await hass.config_entries.flow.async_configure(result["flow_id"], MQTT_INPUT)

    bad_input = {**SCRIPT_INPUT, COMPONENT_VOICE: True, CONF_VOICE_HA_HOST: "10.0.0.5\n"}
    # No SSH/fetch should be reached: the control char is rejected before install.
    with (
        patch.object(config_flow, "AsyncsshShell", return_value=FakeShell()) as mock_shell,
        patch(FETCH_VOICE) as mock_fetch,
    ):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], bad_input)

    assert result["type"] == "form" and result["step_id"] == "script"
    assert result["errors"] == {CONF_VOICE_HA_HOST: "invalid_value"}
    mock_shell.assert_not_called()
    mock_fetch.assert_not_called()
    assert not hass.config_entries.async_entries(DOMAIN)


# --- onboarding: already installed (adopt) ---------------------------------


async def test_installed_adopts_from_panel(hass: HomeAssistant) -> None:
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(PROBE, return_value=_installed(_env(panel="office", scene_bridge_enabled=True))):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], CONNECT_INPUT)
    assert result["type"] == "create_entry"
    assert result["title"] == "Brilliant office"
    data = result["data"]
    assert data[CONF_PANEL] == "office"
    assert data[CONF_HOST] == "192.168.1.10"  # from step 1
    assert data[CONF_ROOT_PASSWORD] == "panelpass"  # from step 1
    assert data[DATA_SSH_HOST_KEY] == "ssh-ed25519 PINNED"
    # broker + mesh adopted FROM the panel, not asked
    assert data[CONF_MQTT_HOST] == "192.168.1.250"
    assert data[CONF_MQTT_PORT] == 8883
    assert data[CONF_MQTT_PASSWORD] == "frombroker"
    assert data[CONF_MESH_PRIORITY] == 3
    assert data[CONF_HA_CONTROL_ENABLED] is True
    assert data[CONF_COMPONENTS][COMPONENT_BLE_OBSERVER] is False
    assert data[CONF_BLE_SCANNER_ENABLED] is False
    assert data[CONF_BLE_OBSERVER_ALLOWLIST_JSON] == "[]"


async def test_installed_duplicate_aborts(hass: HomeAssistant) -> None:
    MockConfigEntry(domain=DOMAIN, unique_id="office", data={CONF_PANEL: "office"}).add_to_hass(
        hass
    )
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(PROBE, return_value=_installed(_env(panel="office"))):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], CONNECT_INPUT)
    assert result["type"] == "abort" and result["reason"] == "already_configured"


async def test_installed_unreadable_config_shows_error(hass: HomeAssistant) -> None:
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    # env present but missing the required keys → can't adopt safely.
    with patch(PROBE, return_value=_installed({"LOG_LEVEL": "INFO"})):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], CONNECT_INPUT)
    assert result["type"] == "form" and result["step_id"] == "user"
    assert result["errors"] == {"base": "cannot_read_config"}


@pytest.mark.parametrize(
    "bad_panel",
    ["mesh", "Office Bath", "office/bath", "", "-office", "office-", "_", "--"],
)
async def test_installed_rejects_unsafe_adopted_slug(hass: HomeAssistant, bad_panel: str) -> None:
    """A hand-deployed BRILLIANT_PANEL that isn't the canonical slug form must not adopt.

    Includes the non-canonical cases _slugify can never produce (leading/trailing or
    doubled separators) so the adopt gate stays in lockstep with the typed-name path.
    """
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(PROBE, return_value=_installed(_env(panel=bad_panel))):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], CONNECT_INPUT)
    assert result["type"] == "form" and result["errors"] == {"base": "cannot_read_config"}


async def test_installed_rejects_out_of_range_port(hass: HomeAssistant) -> None:
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(PROBE, return_value=_installed(_env(panel="office", mqtt_port=0))):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], CONNECT_INPUT)
    assert result["type"] == "form" and result["errors"] == {"base": "cannot_read_config"}


async def test_installed_adopts_with_default_port_and_mesh(hass: HomeAssistant) -> None:
    """MQTT_PORT/MESH_PRIORITY are optional in the agent env; a hand-deployed file that
    omits them must still adopt, defaulting to the agent's own 1883 / 0."""
    minimal = panel_ops.parse_env(
        'BRILLIANT_PANEL="office"\nMQTT_HOST="h"\nMQTT_USERNAME="u"\nMQTT_PASSWORD="p"\n'
    )
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(PROBE, return_value=_installed(minimal)):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], CONNECT_INPUT)
    assert result["type"] == "create_entry"
    assert result["data"][CONF_MQTT_PORT] == 1883
    assert result["data"][CONF_MESH_PRIORITY] == 0


def _inspect(unit: bool, env: bool, version: str = "9.9.9") -> RunResult:
    flags = f"unit={int(unit)}\nenv={int(env)}\nenabled=0\nactive=0\nsunit=0\nsenv=0\n"
    return RunResult(0, flags + (f"{version}\n" if version else ""), "")


async def test_probe_panel_adopts_only_when_unit_and_env_present(hass: HomeAssistant) -> None:
    """A lone env file with no systemd unit is NOT mistaken for a running agent."""
    env_text = panel_ops.render_env(
        panel="office",
        mesh_priority=0,
        mqtt_host="h",
        mqtt_port=1883,
        mqtt_username="u",
        mqtt_password="p",
    )
    cat_resp = {f"cat {PANEL_ENV_FILE}": RunResult(0, env_text, "")}

    # env present but unit absent → not adopted (config is None → fresh setup path).
    shell = FakeShell(responses={panel_ops.INSPECT_COMMAND: _inspect(False, True), **cat_resp})
    with patch.object(config_flow, "AsyncsshShell", return_value=shell):
        probe = await config_flow._probe_panel(hass, "10.0.0.10", "pw")
    assert probe.config is None

    # unit AND env present → adopted (config parsed from the live env).
    shell = FakeShell(responses={panel_ops.INSPECT_COMMAND: _inspect(True, True), **cat_resp})
    with patch.object(config_flow, "AsyncsshShell", return_value=shell):
        probe = await config_flow._probe_panel(hass, "10.0.0.10", "pw")
    assert probe.config is not None and probe.config[panel_ops.ENV_PANEL] == "office"


async def test_apply_config_refuses_to_clobber_a_different_panel(hass: HomeAssistant) -> None:
    """Pushing to a host that already runs ANOTHER panel's agent must raise, not write."""
    other = panel_ops.render_env(
        panel="garage",
        mesh_priority=0,
        mqtt_host="h",
        mqtt_port=1883,
        mqtt_username="u",
        mqtt_password="p",
    )
    shell = FakeShell(
        responses={
            panel_ops.INSPECT_COMMAND: _inspect(True, True),
            f"cat {PANEL_ENV_FILE}": RunResult(0, other, ""),
        }
    )
    with patch.object(config_flow, "AsyncsshShell", return_value=shell):
        with pytest.raises(_WrongPanelError):
            await config_flow._apply_config(
                hass, "10.0.0.20", "pw", pinned_key=None, env_content="X", expected_panel="office"
            )
    assert shell.uploads == []  # nothing written to the wrong panel


async def test_apply_config_pushes_when_panel_matches(hass: HomeAssistant) -> None:
    same = panel_ops.render_env(
        panel="office",
        mesh_priority=0,
        mqtt_host="h",
        mqtt_port=1883,
        mqtt_username="u",
        mqtt_password="p",
    )
    shell = FakeShell(
        responses={
            panel_ops.INSPECT_COMMAND: _inspect(True, True),
            f"cat {PANEL_ENV_FILE}": RunResult(0, same, ""),
        }
    )
    with patch.object(config_flow, "AsyncsshShell", return_value=shell):
        key = await config_flow._apply_config(
            hass,
            "10.0.0.10",
            "pw",
            pinned_key="ssh-ed25519 FAKEKEY",
            env_content="NEWENV",
            expected_panel="office",
        )
    assert key == "ssh-ed25519 FAKEKEY"
    assert any(data == b"NEWENV" for (_path, data, _mode) in shell.uploads)
    assert "systemctl restart brilliant-mqtt" in shell.commands


async def test_apply_config_failure_quarantines_changed_observer_env(hass: HomeAssistant) -> None:
    """A partially failing bridge restart cannot leave a newly enabled BLE env runnable."""
    same = panel_ops.render_env(
        panel="office",
        mesh_priority=0,
        mqtt_host="old.broker",
        mqtt_port=1883,
        mqtt_username="brilliant",
        mqtt_password="oldbroker",
    )
    desired = panel_ops.render_env(
        panel="office",
        mesh_priority=0,
        mqtt_host="new.broker",
        mqtt_port=1883,
        mqtt_username="brilliant",
        mqtt_password="newbroker",
        ble_observer_enabled=True,
        ble_observer_allowlist_json='[{"address":"AA:BB:CC:DD:EE:FF"}]',
    )
    shell = FakeShell(
        responses={
            panel_ops.INSPECT_COMMAND: _inspect(True, True),
            f"cat {PANEL_ENV_FILE}": RunResult(0, same, ""),
            "systemctl restart brilliant-mqtt": RunResult(1, "", "restart failed"),
            panel_ops.BLE_OBSERVER_ACTIVE_COMMAND: RunResult(3, "inactive\n", ""),
        }
    )

    with patch.object(config_flow, "AsyncsshShell", return_value=shell):
        with pytest.raises(panel_ops.PanelOpError, match="restart failed"):
            await config_flow._apply_config(
                hass,
                "10.0.0.10",
                "pw",
                pinned_key="ssh-ed25519 FAKEKEY",
                env_content=desired,
                expected_panel="office",
                fail_closed_ble=True,
            )

    live_env_writes = [data for path, data, _mode in shell.uploads if path == PANEL_ENV_FILE]
    assert live_env_writes[0] == desired.encode()
    assert panel_ops.parse_env(live_env_writes[-1].decode())["BLE_OBSERVER_ENABLED"] == "0"
    assert f"systemctl disable --now {BLE_OBSERVER_SERVICE_NAME}" in shell.commands
    assert any(PANEL_BLE_OBSERVER_UNIT_FILE in command for command in shell.commands)


# --- reconfigure -----------------------------------------------------------


async def test_reconfigure_schema_is_http_serializable(hass: HomeAssistant) -> None:
    """Every field must survive the serializer used by HA's config-flow REST API."""
    entry = _full_entry(hass)
    result = await entry.start_reconfigure_flow(hass)

    assert result["type"] == "form" and result["step_id"] == "reconfigure"
    schema = result["data_schema"]
    assert schema is not None
    serialized = cast(
        list[dict[str, Any]],
        voluptuous_serialize.convert(schema, custom_serializer=cv.custom_serializer),
    )

    assert {field["name"] for field in serialized} >= {
        CONF_HA_CONTROL_DOMAINS,
        CONF_MAX_MIRRORED_ENTITIES,
    }


async def test_reconfigure_same_host_applies_and_pushes(hass: HomeAssistant) -> None:
    entry = _full_entry(hass)
    result = await entry.start_reconfigure_flow(hass)
    assert result["type"] == "form"

    with patch(APPLY, return_value="ssh-ed25519 STORED") as apply:
        result = await hass.config_entries.flow.async_configure(result["flow_id"], RECONFIG_INPUT)

    assert result["type"] == "abort" and result["reason"] == "reconfigure_successful"
    # Same host → STORED pin used (verify-before-auth), never None.
    assert apply.call_args.kwargs["pinned_key"] == "ssh-ed25519 STORED"
    # The immutable slug is handed to the push as the clobber-guard identity.
    assert apply.call_args.kwargs["expected_panel"] == "office"
    # The pushed env carries the NEW broker/mesh but the immutable slug.
    env = apply.call_args.kwargs["env_content"]
    assert 'BRILLIANT_PANEL="office"' in env
    assert "MESH_PRIORITY=5" in env
    assert 'MQTT_PASSWORD="newbroker"' in env
    assert "SCENE_BRIDGE_ENABLED=0" in env
    # The entry is updated; slug preserved.
    assert entry.data[CONF_ROOT_PASSWORD] == "newpass"
    assert entry.data[CONF_MQTT_PASSWORD] == "newbroker"
    assert entry.data[CONF_MESH_PRIORITY] == 5
    assert entry.data[CONF_PANEL] == "office"


async def test_reconfigure_different_host_does_fresh_tofu(hass: HomeAssistant) -> None:
    entry = _full_entry(hass)
    result = await entry.start_reconfigure_flow(hass)
    with patch(APPLY, return_value="ssh-ed25519 NEWHOST") as apply:
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {**RECONFIG_INPUT, CONF_HOST: "192.168.1.99"}
        )
    assert result["type"] == "abort" and result["reason"] == "reconfigure_successful"
    assert apply.call_args.kwargs["pinned_key"] is None  # fresh TOFU for the new host
    assert entry.data[CONF_HOST] == "192.168.1.99"
    assert entry.data[DATA_SSH_HOST_KEY] == "ssh-ed25519 NEWHOST"


async def test_reconfigure_key_mismatch_keeps_pin_and_data(hass: HomeAssistant) -> None:
    entry = _full_entry(hass)
    result = await entry.start_reconfigure_flow(hass)
    with patch(APPLY, side_effect=asyncssh.HostKeyNotVerifiable("changed")):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], RECONFIG_INPUT)
    assert result["type"] == "form" and result["errors"] == {"base": "host_key_changed"}
    assert entry.data[CONF_ROOT_PASSWORD] == "oldpass"
    assert entry.data[CONF_MQTT_PASSWORD] == "oldbroker"
    assert entry.data[DATA_SSH_HOST_KEY] == "ssh-ed25519 STORED"


async def test_reconfigure_same_host_missing_pin_fails_closed(hass: HomeAssistant) -> None:
    entry = _full_entry(hass)
    entry_without_pin = dict(entry.data)
    del entry_without_pin[DATA_SSH_HOST_KEY]
    hass.config_entries.async_update_entry(entry, data=entry_without_pin)

    result = await entry.start_reconfigure_flow(hass)
    with patch(APPLY, return_value="ssh-ed25519 NEW") as apply:
        result = await hass.config_entries.flow.async_configure(result["flow_id"], RECONFIG_INPUT)
    assert result["type"] == "form" and result["errors"] == {"base": "host_key_changed"}
    apply.assert_not_called()  # no connect, pinned or unpinned → password not sent
    assert entry.data[CONF_ROOT_PASSWORD] == "oldpass"


async def test_reconfigure_rejects_control_char(hass: HomeAssistant) -> None:
    entry = _full_entry(hass)
    result = await entry.start_reconfigure_flow(hass)
    with patch(APPLY) as apply:
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {**RECONFIG_INPUT, CONF_MQTT_PASSWORD: "bad\npass"}
        )
    assert result["errors"] == {CONF_MQTT_PASSWORD: "invalid_value"}
    apply.assert_not_called()


async def test_reconfigure_push_failure_shows_cannot_apply(hass: HomeAssistant) -> None:
    entry = _full_entry(hass)
    result = await entry.start_reconfigure_flow(hass)
    with patch(APPLY, side_effect=panel_ops.PanelOpError("restart failed")):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], RECONFIG_INPUT)
    assert result["type"] == "form" and result["errors"] == {"base": "cannot_apply"}
    assert entry.data[CONF_MESH_PRIORITY] == 0  # nothing written


async def test_reconfigure_wrong_panel_surfaces_error(hass: HomeAssistant) -> None:
    """A host running a different panel's agent surfaces wrong_panel; entry untouched."""
    entry = _full_entry(hass)
    result = await entry.start_reconfigure_flow(hass)
    with patch(APPLY, side_effect=_WrongPanelError("garage")):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {**RECONFIG_INPUT, CONF_HOST: "192.168.1.99"}
        )
    assert result["type"] == "form" and result["errors"] == {"base": "wrong_panel"}
    assert entry.data[CONF_HOST] == "192.168.1.10"  # nothing written
    assert entry.data[CONF_MQTT_PASSWORD] == "oldbroker"


async def test_reconfigure_redisplay_preserves_edits(hass: HomeAssistant) -> None:
    """A transient failure must not wipe the operator's six edited fields to old config."""
    entry = _full_entry(hass)
    result = await entry.start_reconfigure_flow(hass)
    with patch(APPLY, side_effect=OSError("down")):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], RECONFIG_INPUT)
    assert result["type"] == "form" and result["errors"] == {"base": "cannot_connect"}
    suggested = _suggested_values(result)
    assert suggested[CONF_MQTT_PASSWORD] == "newbroker"  # the edit, not the old "oldbroker"
    assert suggested[CONF_MESH_PRIORITY] == 5


async def test_reconfigure_strips_host_whitespace_and_keeps_pin(hass: HomeAssistant) -> None:
    """A trailing space on an unchanged host must not downgrade the same-host pin to TOFU."""
    entry = _full_entry(hass)
    result = await entry.start_reconfigure_flow(hass)
    with patch(APPLY, return_value="ssh-ed25519 STORED") as apply:
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {**RECONFIG_INPUT, CONF_HOST: "  192.168.1.10  "}
        )
    assert result["type"] == "abort" and result["reason"] == "reconfigure_successful"
    # Stripped → recognized as the SAME host → STORED pin used, not a fresh TOFU.
    assert apply.call_args.kwargs["pinned_key"] == "ssh-ed25519 STORED"
    assert entry.data[CONF_HOST] == "192.168.1.10"  # stored clean


# --- component-driven install (Task 6) ------------------------------------


async def _drive_flow_to_script(hass: HomeAssistant) -> Any:
    """Drive connect → broker steps and return the script-step result."""
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    result = await hass.config_entries.flow.async_configure(result["flow_id"], CONNECT_INPUT)
    assert result["step_id"] == "broker"
    result = await hass.config_entries.flow.async_configure(result["flow_id"], MQTT_INPUT)
    assert result["step_id"] == "script"
    return result


@pytest.mark.asyncio
async def test_install_step_persists_components(
    hass: HomeAssistant,
    not_installed_panel: None,
    patch_installs: Any,
) -> None:
    """Enabling voice alongside bridge installs both and persists the components dict."""
    result = await _drive_flow_to_script(hass)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"],
        {
            CONF_NAME: "Office",
            CONF_MESH_PRIORITY: 0,
            COMPONENT_VOICE: True,
            CONF_VOICE_WAKE_WORD: "okay_nabu",
            CONF_VOICE_HA_HOST: "",
        },
    )
    assert result["type"] == "create_entry"
    comps = result["data"][CONF_COMPONENTS]
    assert comps[COMPONENT_BRIDGE] is True
    assert comps[COMPONENT_VOICE] is True
    assert comps[COMPONENT_BLE_OBSERVER] is False
    assert result["data"][CONF_BLE_SCANNER_ENABLED] is False
    assert result["data"][CONF_BLE_OBSERVER_ALLOWLIST_JSON] == "[]"
    assert patch_installs.called(COMPONENT_BRIDGE)
    assert patch_installs.called(COMPONENT_VOICE)


async def test_deprecated_ha_mirror_fields_are_hidden_from_new_install(
    hass: HomeAssistant, not_installed_panel: None
) -> None:
    result = await _drive_flow_to_script(hass)
    schema = result["data_schema"]
    assert schema is not None
    fields = {str(marker) for marker in schema.schema}
    assert COMPONENT_HA_MIRROR not in fields
    assert CONF_HA_MIRROR_WS_URL not in fields
    assert CONF_HA_MIRROR_TOKEN not in fields
    assert CONF_HA_MIRROR_LEADER_PRIORITY not in fields
    assert CONF_HA_MIRROR_LABEL not in fields


async def test_new_install_exposes_two_independent_default_off_ble_switches(
    hass: HomeAssistant,
    not_installed_panel: None,
) -> None:
    result = await _drive_flow_to_script(hass)
    schema = result["data_schema"]
    assert schema is not None
    fields = {str(marker) for marker in schema.schema}
    assert {
        COMPONENT_BLE_OBSERVER,
        CONF_BLE_SCANNER_ENABLED,
        CONF_BLE_OBSERVER_ALLOWLIST_JSON,
    } <= fields
    assert _schema_default(result, COMPONENT_BLE_OBSERVER) is False
    assert _schema_default(result, CONF_BLE_SCANNER_ENABLED) is False
    assert _schema_default(result, CONF_BLE_OBSERVER_ALLOWLIST_JSON) == "[]"


# --- options ---------------------------------------------------------------


async def test_options_flow_saves_behavior_knobs(hass: HomeAssistant) -> None:
    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data={CONF_PANEL: "office"})
    entry.add_to_hass(hass)

    result = await hass.config_entries.options.async_init(entry.entry_id)
    assert result["type"] == "form" and result["step_id"] == "init"

    result = await hass.config_entries.options.async_configure(
        result["flow_id"],
        {
            OPT_AUTO_REPAIR: False,
            OPT_OFFLINE_GRACE_MINUTES: 5,
            OPT_REPAIR_COOLDOWN_MINUTES: 30,
            OPT_TRUST_HOST_KEY_CHANGES: True,
        },
    )
    assert result["type"] == "create_entry"
    assert entry.options == {
        OPT_AUTO_REPAIR: False,
        OPT_OFFLINE_GRACE_MINUTES: 5,
        OPT_REPAIR_COOLDOWN_MINUTES: 30,
        OPT_TRUST_HOST_KEY_CHANGES: True,
    }


# --- Task 7: Reconfigure — editable component checkboxes (install/remove diff) ------


async def start_reconfigure(hass: HomeAssistant, entry: MockConfigEntry) -> Any:
    """Start the reconfigure flow for *entry* and return the initial form result."""
    return await entry.start_reconfigure_flow(hass)


def reconfigure_input(
    entry: MockConfigEntry,
    *,
    voice: bool | None = None,
    wifi_watchdog: bool | None = None,
    bus_watchdog: bool | None = None,
    ble_observer: bool | None = None,
) -> dict[str, Any]:
    """Build a full reconfigure user_input dict from entry data.

    *voice* overrides the COMPONENT_VOICE checkbox; None keeps the stored value.
    *wifi_watchdog* overrides the COMPONENT_WIFI_WATCHDOG checkbox; None keeps the stored value.
    """
    data = entry.data
    comps: dict[str, bool] = dict(data.get(CONF_COMPONENTS) or {})
    current_voice = bool(comps.get(COMPONENT_VOICE, False))
    current_wd = bool(comps.get(COMPONENT_WIFI_WATCHDOG, False))
    current_bus_wd = bool(comps.get(COMPONENT_BUS_WATCHDOG, False))
    current_ble_observer = bool(comps.get(COMPONENT_BLE_OBSERVER, False))
    return {
        CONF_HOST: data[CONF_HOST],
        CONF_ROOT_PASSWORD: data[CONF_ROOT_PASSWORD],
        CONF_MQTT_HOST: data[CONF_MQTT_HOST],
        CONF_MQTT_PORT: data[CONF_MQTT_PORT],
        CONF_MQTT_USERNAME: data[CONF_MQTT_USERNAME],
        CONF_MQTT_PASSWORD: data[CONF_MQTT_PASSWORD],
        CONF_MESH_PRIORITY: data.get(CONF_MESH_PRIORITY, 0),
        COMPONENT_VOICE: voice if voice is not None else current_voice,
        COMPONENT_WIFI_WATCHDOG: wifi_watchdog if wifi_watchdog is not None else current_wd,
        COMPONENT_BUS_WATCHDOG: (bus_watchdog if bus_watchdog is not None else current_bus_wd),
        COMPONENT_BLE_OBSERVER: (
            ble_observer if ble_observer is not None else current_ble_observer
        ),
        CONF_BLE_SCANNER_ENABLED: data.get(CONF_BLE_SCANNER_ENABLED, False),
        CONF_BLE_OBSERVER_ALLOWLIST_JSON: data.get(CONF_BLE_OBSERVER_ALLOWLIST_JSON, "[]"),
        CONF_VOICE_WAKE_WORD: data.get(CONF_VOICE_WAKE_WORD, "okay_nabu"),
        CONF_VOICE_HA_HOST: data.get(CONF_VOICE_HA_HOST, ""),
        CONF_HA_CONTROL_ENABLED: data.get(CONF_HA_CONTROL_ENABLED, DEFAULT_HA_CONTROL_ENABLED),
        CONF_HA_CONTROL_LABEL: data.get(CONF_HA_CONTROL_LABEL, DEFAULT_HA_CONTROL_LABEL),
        CONF_ROOM_OVERRIDES: json.dumps(data.get(CONF_ROOM_OVERRIDES, {}), sort_keys=True),
        CONF_HA_CONTROL_DOMAINS: list(
            data.get(CONF_HA_CONTROL_DOMAINS, DEFAULT_HA_CONTROL_DOMAINS)
        ),
        CONF_MAX_MIRRORED_ENTITIES: data.get(
            CONF_MAX_MIRRORED_ENTITIES, DEFAULT_MAX_MIRRORED_ENTITIES
        ),
        CONF_SCENE_PANEL: data.get(CONF_SCENE_PANEL, data[CONF_PANEL]),
        CONF_SCENE_ACTIONS: json.dumps(data.get(CONF_SCENE_ACTIONS, {}), sort_keys=True),
    }


@pytest.mark.asyncio
async def test_reconfigure_uncheck_voice_removes(
    hass: HomeAssistant,
    installed_voice_entry: MockConfigEntry,
    patch_installs: Any,
) -> None:
    """Unchecking voice in reconfigure removes the component and persists the change."""
    result = await start_reconfigure(hass, installed_voice_entry)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], reconfigure_input(installed_voice_entry, voice=False)
    )
    assert result["type"] == "abort" and result["reason"] == "reconfigure_successful"
    assert installed_voice_entry.data[CONF_COMPONENTS][COMPONENT_VOICE] is False
    assert patch_installs.removed(COMPONENT_VOICE)


@pytest.mark.asyncio
async def test_reconfigure_check_voice_installs(
    hass: HomeAssistant,
    patch_installs: Any,
) -> None:
    """Checking voice in reconfigure installs the component and persists the change."""
    entry = _full_entry(hass, **{CONF_COMPONENTS: {COMPONENT_BRIDGE: True, COMPONENT_VOICE: False}})
    result = await start_reconfigure(hass, entry)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], reconfigure_input(entry, voice=True)
    )
    assert result["type"] == "abort" and result["reason"] == "reconfigure_successful"
    assert entry.data[CONF_COMPONENTS][COMPONENT_VOICE] is True
    assert patch_installs.called(COMPONENT_VOICE)


@pytest.mark.asyncio
async def test_reconfigure_no_change_skips_install_remove(
    hass: HomeAssistant,
    patch_installs: Any,
) -> None:
    """When the component selection is unchanged, neither install nor remove fires."""
    entry = _full_entry(hass, **{CONF_COMPONENTS: {COMPONENT_BRIDGE: True, COMPONENT_VOICE: False}})
    result = await start_reconfigure(hass, entry)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], reconfigure_input(entry, voice=False)
    )
    assert result["type"] == "abort" and result["reason"] == "reconfigure_successful"
    assert not patch_installs.called(COMPONENT_VOICE)
    assert not patch_installs.removed(COMPONENT_VOICE)


async def test_reconfigure_enables_observer_and_scanner_with_canonical_allowlist(
    hass: HomeAssistant,
    patch_installs: Any,
) -> None:
    entry = _full_entry(
        hass,
        **{
            CONF_COMPONENTS: {
                COMPONENT_BRIDGE: True,
                COMPONENT_BLE_OBSERVER: False,
            },
            CONF_BLE_SCANNER_ENABLED: False,
            CONF_BLE_OBSERVER_ALLOWLIST_JSON: "[]",
        },
    )
    hass.config_entries.async_update_entry(
        entry,
        version=config_flow.BrilliantMqttConfigFlow.VERSION,
    )
    result = await start_reconfigure(hass, entry)
    raw_allowlist = json.dumps(
        [
            {"address": "aa-bb-cc-dd-ee-ff"},
            {
                "ibeacon_uuid": "FDA50693-A4E2-4FB1-AFCF-C6EB07647825",
                "ibeacon_major": 1,
                "ibeacon_minor": 2,
            },
        ]
    )
    submitted = {
        **reconfigure_input(entry, ble_observer=True),
        CONF_BLE_SCANNER_ENABLED: True,
        CONF_BLE_OBSERVER_ALLOWLIST_JSON: raw_allowlist,
    }

    with patch(APPLY, return_value="ssh-ed25519 STORED") as apply:
        result = await hass.config_entries.flow.async_configure(result["flow_id"], submitted)

    assert result["type"] == "abort" and result["reason"] == "reconfigure_successful"
    canonical = (
        '[{"address":"AA:BB:CC:DD:EE:FF"},'
        '{"ibeacon_major":1,"ibeacon_minor":2,'
        '"ibeacon_uuid":"fda50693-a4e2-4fb1-afcf-c6eb07647825"}]'
    )
    assert entry.data[CONF_COMPONENTS][COMPONENT_BLE_OBSERVER] is True
    assert entry.data[CONF_BLE_SCANNER_ENABLED] is True
    assert entry.data[CONF_BLE_OBSERVER_ALLOWLIST_JSON] == canonical
    assert patch_installs.called(COMPONENT_BLE_OBSERVER)
    pushed = panel_ops.parse_env(apply.call_args.kwargs["env_content"])
    assert pushed["BLE_OBSERVER_ENABLED"] == "1"
    assert pushed["BLE_OBSERVER_ALLOWLIST_JSON"] == canonical


async def test_reconfigure_component_failure_before_ble_quarantines_desired_observer(
    hass: HomeAssistant,
) -> None:
    """Desired BLE env is fail-closed when an earlier optional install aborts the diff."""
    entry = _full_entry(
        hass,
        **{
            CONF_COMPONENTS: {
                COMPONENT_BRIDGE: True,
                COMPONENT_VOICE: False,
                COMPONENT_BLE_OBSERVER: False,
            },
            CONF_BLE_OBSERVER_ALLOWLIST_JSON: "[]",
        },
    )
    before = dict(entry.data)
    shell = FakeShell(
        responses={
            panel_ops.BLE_OBSERVER_ACTIVE_COMMAND: RunResult(3, "inactive\n", ""),
        }
    )

    async def fail_voice_install(*_args: object, **_kwargs: object) -> None:
        raise panel_ops.PanelOpError("voice unit failed")

    voice = replace(components.REGISTRY[COMPONENT_VOICE], install=fail_voice_install)
    result = await start_reconfigure(hass, entry)
    submitted = {
        **reconfigure_input(entry, voice=True, ble_observer=True),
        CONF_BLE_OBSERVER_ALLOWLIST_JSON: '[{"address":"AA:BB:CC:DD:EE:FF"}]',
    }
    with (
        patch(APPLY, return_value="ssh-ed25519 STORED") as apply,
        patch.object(config_flow, "AsyncsshShell", return_value=shell),
        patch.dict(components.REGISTRY, {COMPONENT_VOICE: voice}),
    ):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], submitted)

    assert result["type"] == "form" and result["errors"] == {"base": "cannot_apply"}
    assert entry.data == before
    pushed = panel_ops.parse_env(apply.call_args.kwargs["env_content"])
    assert pushed["BLE_OBSERVER_ENABLED"] == "1"
    assert f"systemctl disable --now {BLE_OBSERVER_SERVICE_NAME}" in shell.commands
    assert panel_ops.BLE_OBSERVER_ACTIVE_COMMAND in shell.commands
    assert any(PANEL_BLE_OBSERVER_UNIT_FILE in command for command in shell.commands)
    assert f"systemctl enable --now {BLE_OBSERVER_SERVICE_NAME}" not in shell.commands


@pytest.mark.parametrize(
    ("changed_key", "new_value", "expected_env_key", "expected_env_value"),
    [
        (CONF_HOST, "192.168.1.11", panel_ops.ENV_PANEL, "office"),
        (CONF_MQTT_HOST, "new.broker", panel_ops.ENV_MQTT_HOST, "new.broker"),
        (CONF_MQTT_PORT, 8883, panel_ops.ENV_MQTT_PORT, "8883"),
        (CONF_MQTT_USERNAME, "new-user", panel_ops.ENV_MQTT_USERNAME, "new-user"),
        (CONF_MQTT_PASSWORD, "new-password", panel_ops.ENV_MQTT_PASSWORD, "new-password"),
        (
            CONF_BLE_OBSERVER_ALLOWLIST_JSON,
            '[{"address":"11:22:33:44:55:66"}]',
            panel_ops.ENV_BLE_OBSERVER_ALLOWLIST_JSON,
            '[{"address":"11:22:33:44:55:66"}]',
        ),
    ],
    ids=["panel-host", "mqtt-host", "mqtt-port", "mqtt-username", "mqtt-password", "allowlist"],
)
async def test_reconfigure_selected_observer_restarts_after_runtime_env_write(
    hass: HomeAssistant,
    payload_dir: Path,
    changed_key: str,
    new_value: Any,
    expected_env_key: str,
    expected_env_value: str,
) -> None:
    old_allowlist = '[{"address":"AA:BB:CC:DD:EE:FF"}]'
    entry = _full_entry(
        hass,
        **{
            CONF_COMPONENTS: {
                COMPONENT_BRIDGE: True,
                COMPONENT_BLE_OBSERVER: True,
            },
            CONF_BLE_SCANNER_ENABLED: False,
            CONF_BLE_OBSERVER_ALLOWLIST_JSON: old_allowlist,
        },
    )
    hass.config_entries.async_update_entry(
        entry,
        version=config_flow.BrilliantMqttConfigFlow.VERSION,
    )
    old_env = panel_ops.render_env(
        panel="office",
        mesh_priority=0,
        mqtt_host="old.broker",
        mqtt_port=1883,
        mqtt_username="brilliant",
        mqtt_password="oldbroker",
        ble_observer_enabled=True,
        ble_observer_allowlist_json=old_allowlist,
    )
    shell = FakeShell(
        responses={
            panel_ops.INSPECT_COMMAND: RunResult(
                0,
                "unit=1\nenv=1\nenabled=1\nactive=1\nsunit=1\nsenv=1\npayload=1\n0.5.7\n",
                "",
            ),
            f"cat {PANEL_ENV_FILE}": RunResult(0, old_env, ""),
            panel_ops.BLE_OBSERVER_INSPECT_COMMAND: RunResult(
                0,
                "unit=1\nenabled=1\nactive=1\nsunit=1\npayload=1\n",
                "",
            ),
        }
    )
    refresh_observer = components.refresh_ble_observer

    async def assert_env_then_refresh(
        target_hass: HomeAssistant,
        target: PanelShell,
        data: Mapping[str, Any],
    ) -> None:
        assert target_hass is hass
        fake_target = cast(FakeShell, target)
        live_env = next(
            chunk for path, chunk, _mode in fake_target.uploads if path == PANEL_ENV_FILE
        )
        assert panel_ops.parse_env(live_env.decode())[expected_env_key] == expected_env_value
        await refresh_observer(target_hass, target, data)

    result = await start_reconfigure(hass, entry)
    with (
        patch.object(config_flow, "AsyncsshShell", return_value=shell),
        patch.object(
            components,
            "refresh_ble_observer",
            side_effect=assert_env_then_refresh,
        ) as activate,
    ):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {
                **reconfigure_input(entry),
                changed_key: new_value,
            },
        )

    assert result["type"] == "abort" and result["reason"] == "reconfigure_successful"
    assert activate.await_count == 1
    assert activate.call_args.args[0:2] == (hass, shell)
    bridge_restart = shell.commands.index("systemctl restart brilliant-mqtt")
    observer_enable = shell.commands.index("systemctl enable brilliant-ble-observer")
    observer_restart = shell.commands.index("systemctl restart brilliant-ble-observer")
    assert bridge_restart < observer_enable < observer_restart
    assert shell.commands.count("systemctl restart brilliant-mqtt") == 1
    assert not any(
        "bluetoothctl" in command or "bluetoothd" in command for command in shell.commands
    )
    assert entry.data[changed_key] == new_value


async def test_selected_observer_refresh_retry_reinstalls_after_failed_proof_quarantine(
    hass: HomeAssistant,
    payload_dir: Path,
) -> None:
    """A failed refresh removes unsafe state; the identical retry rebuilds it fully."""
    old_allowlist = '[{"address":"AA:BB:CC:DD:EE:FF"}]'
    new_allowlist = '[{"address":"11:22:33:44:55:66"}]'
    previous: dict[str, Any] = {
        CONF_HOST: "192.168.1.10",
        CONF_ROOT_PASSWORD: "panelpass",
        CONF_MQTT_HOST: "broker",
        CONF_MQTT_PORT: 1883,
        CONF_MQTT_USERNAME: "user",
        CONF_MQTT_PASSWORD: "password",
        CONF_COMPONENTS: {
            COMPONENT_BRIDGE: True,
            COMPONENT_BLE_OBSERVER: True,
        },
        CONF_BLE_OBSERVER_ALLOWLIST_JSON: old_allowlist,
    }
    updated = {**previous, CONF_BLE_OBSERVER_ALLOWLIST_JSON: new_allowlist}
    existing = panel_ops.render_env(
        panel="office",
        mesh_priority=0,
        mqtt_host="broker",
        mqtt_port=1883,
        mqtt_username="user",
        mqtt_password="password",
        ble_observer_enabled=True,
        ble_observer_allowlist_json=new_allowlist,
    )
    unhealthy = FakeShell(
        responses={
            f"cat {PANEL_ENV_FILE}": RunResult(0, existing, ""),
            panel_ops.BLE_OBSERVER_INSPECT_COMMAND: RunResult(
                0,
                "unit=1\nenabled=1\nactive=0\nsunit=1\npayload=1\n",
                "",
            ),
            panel_ops.BLE_OBSERVER_ACTIVE_COMMAND: RunResult(3, "inactive\n", ""),
        }
    )
    healthy = FakeShell(
        responses={
            f"cat {PANEL_ENV_FILE}": RunResult(0, existing, ""),
            panel_ops.BLE_OBSERVER_INSPECT_COMMAND: RunResult(
                0,
                "unit=1\nenabled=1\nactive=1\nsunit=1\npayload=1\n",
                "",
            ),
        }
    )

    with patch.object(
        config_flow,
        "AsyncsshShell",
        side_effect=[unhealthy, healthy],
    ):
        with pytest.raises(panel_ops.PanelOpError):
            await config_flow._restart_changed_ble_observer(
                hass,
                previous,
                updated,
                host_key="ssh-ed25519 STORED",
            )
        await config_flow._restart_changed_ble_observer(
            hass,
            previous,
            updated,
            host_key="ssh-ed25519 STORED",
        )

    assert any(PANEL_BLE_OBSERVER_UNIT_FILE in command for command in unhealthy.commands)
    assert healthy.dir_uploads == [
        (str(payload_dir / "ble_observer"), "/var/brilliant-mqtt/ble_observer.staging")
    ]
    assert any(path == PANEL_BLE_OBSERVER_UNIT_FILE for path, _data, _mode in healthy.uploads)
    assert f"systemctl restart {BLE_OBSERVER_SERVICE_NAME}" in healthy.commands


async def test_reconfigure_unchanged_selected_observer_is_not_restarted(
    hass: HomeAssistant,
) -> None:
    entry = _full_entry(
        hass,
        **{
            CONF_COMPONENTS: {
                COMPONENT_BRIDGE: True,
                COMPONENT_BLE_OBSERVER: True,
            },
            CONF_BLE_OBSERVER_ALLOWLIST_JSON: '[{"address":"AA:BB:CC:DD:EE:FF"}]',
        },
    )
    result = await start_reconfigure(hass, entry)

    with (
        patch(APPLY, return_value="ssh-ed25519 STORED"),
        patch.object(components, "refresh_ble_observer") as activate,
    ):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            reconfigure_input(entry),
        )

    assert result["type"] == "abort" and result["reason"] == "reconfigure_successful"
    activate.assert_not_awaited()


async def test_reconfigure_never_restarts_unselected_observer(hass: HomeAssistant) -> None:
    entry = _full_entry(
        hass,
        **{
            CONF_COMPONENTS: {
                COMPONENT_BRIDGE: True,
                COMPONENT_BLE_OBSERVER: False,
            },
            CONF_BLE_OBSERVER_ALLOWLIST_JSON: "[]",
        },
    )
    result = await start_reconfigure(hass, entry)

    with (
        patch(APPLY, return_value="ssh-ed25519 STORED"),
        patch.object(components, "refresh_ble_observer") as activate,
    ):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            reconfigure_input(entry, ble_observer=False),
        )

    assert result["type"] == "abort" and result["reason"] == "reconfigure_successful"
    activate.assert_not_awaited()


async def test_reconfigure_observer_removal_never_restarts_it(
    hass: HomeAssistant,
    patch_installs: Any,
) -> None:
    entry = _full_entry(
        hass,
        **{
            CONF_COMPONENTS: {
                COMPONENT_BRIDGE: True,
                COMPONENT_BLE_OBSERVER: True,
            },
            CONF_BLE_OBSERVER_ALLOWLIST_JSON: '[{"address":"AA:BB:CC:DD:EE:FF"}]',
        },
    )
    result = await start_reconfigure(hass, entry)

    with (
        patch(APPLY, return_value="ssh-ed25519 STORED"),
        patch.object(components, "refresh_ble_observer") as activate,
    ):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            reconfigure_input(entry, ble_observer=False),
        )

    assert result["type"] == "abort" and result["reason"] == "reconfigure_successful"
    assert patch_installs.removed(COMPONENT_BLE_OBSERVER)
    activate.assert_not_awaited()


def _oversized_ble_allowlist() -> str:
    return json.dumps([{"address": f"AA:BB:CC:DD:EE:{index:02X}"} for index in range(65)])


@pytest.mark.parametrize(
    "allowlist",
    [
        "{}",
        '[{"address":"not-a-mac"}]',
        '[{"address":"AA:BB:CC:DD:EE:FF","extra":true}]',
        '[{"ibeacon_uuid":"fda50693-a4e2-4fb1-afcf-c6eb07647825","ibeacon_major":1}]',
        '[{"ibeacon_uuid":"fda50693-a4e2-4fb1-afcf-c6eb07647825",'
        '"ibeacon_major":true,"ibeacon_minor":2}]',
        '[{"address":"AA:BB:CC:DD:EE:FF"},{"address":"aa-bb-cc-dd-ee-ff"}]',
        '[{"address":"AA:BB:CC:DD:EE:FF\\u0000"}]',
        "[\x00]",
        "[" + (" " * (65 * 1024)) + "]",
        _oversized_ble_allowlist(),
    ],
    ids=[
        "not-array",
        "bad-address",
        "unknown-key",
        "partial-ibeacon",
        "boolean-major",
        "normalized-duplicate",
        "escaped-control",
        "raw-control",
        "oversized-text",
        "too-many-identities",
    ],
)
async def test_reconfigure_rejects_unbounded_or_invalid_ble_identity_data(
    hass: HomeAssistant,
    allowlist: str,
) -> None:
    entry = _full_entry(hass)
    before = dict(entry.data)
    result = await start_reconfigure(hass, entry)
    submitted = {
        **reconfigure_input(entry, ble_observer=True),
        CONF_BLE_OBSERVER_ALLOWLIST_JSON: allowlist,
    }

    with patch(APPLY) as apply:
        result = await hass.config_entries.flow.async_configure(result["flow_id"], submitted)

    assert result["type"] == "form" and result["step_id"] == "reconfigure"
    assert result["errors"] == {CONF_BLE_OBSERVER_ALLOWLIST_JSON: "invalid_value"}
    apply.assert_not_called()
    assert entry.data == before


async def test_reconfigure_does_not_redisplay_unsafe_ble_allowlist(
    hass: HomeAssistant,
    caplog: pytest.LogCaptureFixture,
) -> None:
    entry = _full_entry(hass)
    result = await start_reconfigure(hass, entry)
    unsafe = '[{"address":"SENTINEL\x00"}]'

    with patch(APPLY) as apply:
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {
                **reconfigure_input(entry, ble_observer=True),
                CONF_BLE_OBSERVER_ALLOWLIST_JSON: unsafe,
            },
        )

    assert result["errors"] == {CONF_BLE_OBSERVER_ALLOWLIST_JSON: "invalid_value"}
    assert _suggested_values(result)[CONF_BLE_OBSERVER_ALLOWLIST_JSON] == "[]"
    assert "SENTINEL" not in repr(_suggested_values(result))
    assert "SENTINEL" not in caplog.text
    apply.assert_not_called()


@pytest.mark.asyncio
async def test_reconfigure_rejects_control_char_in_voice_ha_host(
    hass: HomeAssistant,
) -> None:
    """A control char in voice_ha_host surfaces invalid_value; no SSH attempted."""
    entry = _full_entry(hass)
    result = await start_reconfigure(hass, entry)
    bad_input = {**reconfigure_input(entry), CONF_VOICE_HA_HOST: "10.0.0.5\n"}
    with patch(APPLY) as apply:
        result = await hass.config_entries.flow.async_configure(result["flow_id"], bad_input)
    assert result["type"] == "form"
    assert result["errors"] == {CONF_VOICE_HA_HOST: "invalid_value"}
    apply.assert_not_called()


async def test_reconfigure_hides_legacy_mirror_and_preserves_credentials_until_retired(
    hass: HomeAssistant,
) -> None:
    entry = _full_entry(
        hass,
        **{
            CONF_COMPONENTS: {
                COMPONENT_BRIDGE: True,
                COMPONENT_VOICE: True,
                COMPONENT_WIFI_WATCHDOG: True,
                COMPONENT_BUS_WATCHDOG: True,
                COMPONENT_HA_MIRROR: True,
            },
            CONF_HA_MIRROR_WS_URL: "ws://old-ha:8123/api/websocket",
            CONF_HA_MIRROR_TOKEN: "old-secret",
            CONF_HA_MIRROR_LEADER_PRIORITY: 2,
            CONF_HA_MIRROR_LABEL: "old-label",
        },
    )
    result = await start_reconfigure(hass, entry)
    schema = result["data_schema"]
    assert schema is not None
    fields = {str(marker) for marker in schema.schema}
    assert COMPONENT_HA_MIRROR not in fields
    assert CONF_HA_MIRROR_WS_URL not in fields
    assert CONF_HA_MIRROR_TOKEN not in fields
    assert CONF_HA_MIRROR_LEADER_PRIORITY not in fields
    assert CONF_HA_MIRROR_LABEL not in fields

    updated = reconfigure_input(entry)
    with patch(APPLY, return_value="ssh-ed25519 STORED"):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], updated)
    assert result["type"] == "abort" and result["reason"] == "reconfigure_successful"
    assert entry.data[CONF_HA_MIRROR_WS_URL] == "ws://old-ha:8123/api/websocket"
    assert entry.data[CONF_HA_MIRROR_TOKEN] == "old-secret"
    assert entry.data[CONF_HA_MIRROR_LEADER_PRIORITY] == 2
    assert entry.data[CONF_HA_MIRROR_LABEL] == "old-label"
    assert entry.data[CONF_COMPONENTS][COMPONENT_HA_MIRROR] is False


@pytest.mark.asyncio
async def test_reconfigure_migrated_entry_watchdog_default_not_preselected(
    hass: HomeAssistant,
    patch_installs: Any,
) -> None:
    """Fix #2: an existing panel WITHOUT the wifi_watchdog key must show the checkbox
    UNCHECKED on reconfigure, so a no-change Save does NOT install the watchdog.

    Before the fix, _components_schema_fields used ``c.default_enabled`` (True for
    wifi_watchdog) as the fallback for any key absent from CONF_COMPONENTS, making
    the reconfigure form render the checkbox pre-checked on all 14 migrated panels.
    A no-change Save would then drive was=False → now=True → install called.

    After the fix (new_install=False on async_step_reconfigure), the fallback for
    an absent key is False, so the box is unchecked and a no-change Save is a no-op.

    This test FAILS against current code (schema default is True, not False) and
    PASSES after the fix (schema default is False).
    """
    import voluptuous as vol

    # Migrated entry: no wifi_watchdog key at all in CONF_COMPONENTS.
    entry = _full_entry(
        hass,
        **{CONF_COMPONENTS: {COMPONENT_BRIDGE: True, COMPONENT_VOICE: False}},
    )
    result = await entry.start_reconfigure_flow(hass)
    assert result["type"] == "form" and result["step_id"] == "reconfigure"

    # The schema default for wifi_watchdog must be False for an existing entry that
    # does not have the key (was True before the fix → pre-checked → auto-install bug).
    schema = result["data_schema"]
    assert schema is not None
    wd_default: bool | None = None
    for marker in schema.schema:
        if str(marker) == COMPONENT_WIFI_WATCHDOG:
            raw = marker.default
            wd_default = raw() if raw is not vol.UNDEFINED and callable(raw) else raw
            break
    assert wd_default is False, (
        f"wifi_watchdog reconfigure default must be False for a migrated entry "
        f"(got {wd_default!r}; before the fix it was True)"
    )

    # A no-change submit (wifi_watchdog stays False) must not call install or remove.
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"],
        reconfigure_input(entry),  # wifi_watchdog=False since not in entry.data
    )
    assert result["type"] == "abort" and result["reason"] == "reconfigure_successful"
    assert not patch_installs.called(COMPONENT_WIFI_WATCHDOG)
    assert not patch_installs.removed(COMPONENT_WIFI_WATCHDOG)


# --- Task 9: safe HA control configuration + migration --------------------


GLOBAL_KEYS = (
    CONF_HA_CONTROL_ENABLED,
    CONF_HA_CONTROL_LABEL,
    CONF_ROOM_OVERRIDES,
    CONF_HA_CONTROL_DOMAINS,
    CONF_MAX_MIRRORED_ENTITIES,
    CONF_SCENE_PANEL,
    CONF_SCENE_ACTIONS,
)


def _schema_default(result: Any, key: str) -> Any:
    schema = result["data_schema"]
    assert schema is not None
    marker = next(marker for marker in schema.schema if str(marker) == key)
    default = marker.default
    return default() if default is not vol.UNDEFINED and callable(default) else default


async def test_new_install_control_defaults_are_explicit_and_persist_decoded(
    hass: HomeAssistant,
    not_installed_panel: None,
    patch_installs: Any,
) -> None:
    result = await _drive_flow_to_script(hass)
    assert _schema_default(result, CONF_HA_CONTROL_ENABLED) is False
    assert _schema_default(result, CONF_HA_CONTROL_LABEL) == "brilliant"
    assert _schema_default(result, CONF_ROOM_OVERRIDES) == "{}"
    assert _schema_default(result, CONF_HA_CONTROL_DOMAINS) == ["light", "switch"]
    assert _schema_default(result, CONF_MAX_MIRRORED_ENTITIES) == 50
    # The panel slug is derived from the name submitted on this same step, so the
    # untouched form uses a safe blank sentinel and persists the current panel.
    assert _schema_default(result, CONF_SCENE_PANEL) == ""
    assert _schema_default(result, CONF_SCENE_ACTIONS) == "{}"

    actions = {
        "office-bath:all_off": {
            "domain": "scene",
            "service": "turn_on",
            "target": {"entity_id": ["scene.downstairs_off"]},
            "data": {},
        }
    }
    submitted = {
        **SCRIPT_INPUT,
        CONF_HA_CONTROL_ENABLED: True,
        CONF_HA_CONTROL_LABEL: "  ha-visible  ",
        CONF_ROOM_OVERRIDES: '{"Office":"Office Bath"}',
        CONF_HA_CONTROL_DOMAINS: ["switch", "light"],
        CONF_MAX_MIRRORED_ENTITIES: 12,
        CONF_SCENE_ACTIONS: json.dumps(actions),
    }
    result = await hass.config_entries.flow.async_configure(result["flow_id"], submitted)
    assert result["type"] == "create_entry"
    data = result["data"]
    assert data[CONF_HA_CONTROL_LABEL] == "ha-visible"
    assert data[CONF_ROOM_OVERRIDES] == {"Office": "Office Bath"}
    assert data[CONF_HA_CONTROL_DOMAINS] == ["light", "switch"]
    assert data[CONF_SCENE_ACTIONS] == actions
    assert all(not isinstance(data[key], str) for key in (CONF_ROOM_OVERRIDES, CONF_SCENE_ACTIONS))


@pytest.mark.parametrize(
    ("changed", "field"),
    [
        ({CONF_HA_CONTROL_LABEL: "   "}, CONF_HA_CONTROL_LABEL),
        ({CONF_ROOM_OVERRIDES: "{not-json"}, CONF_ROOM_OVERRIDES),
        ({CONF_ROOM_OVERRIDES: "[]"}, CONF_ROOM_OVERRIDES),
        ({CONF_ROOM_OVERRIDES: '{"Office":7}'}, CONF_ROOM_OVERRIDES),
        ({CONF_HA_CONTROL_DOMAINS: ["light", "light"]}, CONF_HA_CONTROL_DOMAINS),
        ({CONF_HA_CONTROL_DOMAINS: ["light", "climate"]}, CONF_HA_CONTROL_DOMAINS),
        ({CONF_MAX_MIRRORED_ENTITIES: True}, CONF_MAX_MIRRORED_ENTITIES),
        ({CONF_MAX_MIRRORED_ENTITIES: 0}, CONF_MAX_MIRRORED_ENTITIES),
        ({CONF_MAX_MIRRORED_ENTITIES: 201}, CONF_MAX_MIRRORED_ENTITIES),
        ({CONF_SCENE_PANEL: "backyard"}, CONF_SCENE_PANEL),
        ({CONF_SCENE_ACTIONS: "{not-json"}, CONF_SCENE_ACTIONS),
        ({CONF_SCENE_ACTIONS: "[]"}, CONF_SCENE_ACTIONS),
        (
            {
                CONF_SCENE_ACTIONS: json.dumps(
                    {
                        "backyard:all_off": {
                            "domain": "scene",
                            "service": "turn_on",
                            "target": {},
                            "data": {},
                        }
                    }
                )
            },
            CONF_SCENE_ACTIONS,
        ),
        (
            {
                CONF_SCENE_ACTIONS: json.dumps(
                    {
                        "office-bath:all_off": {
                            "domain": "scene",
                            "service": "Turn On",
                            "target": {"secret": "never"},
                            "data": {},
                        }
                    }
                )
            },
            CONF_SCENE_ACTIONS,
        ),
    ],
)
async def test_control_validation_fails_closed_and_preserves_safe_text(
    hass: HomeAssistant,
    not_installed_panel: None,
    changed: dict[str, Any],
    field: str,
) -> None:
    result = await _drive_flow_to_script(hass)
    submitted = {**SCRIPT_INPUT, **changed}
    result = await hass.config_entries.flow.async_configure(result["flow_id"], submitted)
    assert result["type"] == "form" and result["step_id"] == "script"
    assert result["errors"] == {field: "invalid_value"}
    if field in (CONF_ROOM_OVERRIDES, CONF_SCENE_ACTIONS):
        assert _suggested_values(result)[field] == submitted[field]
    assert not hass.config_entries.async_entries(DOMAIN)


def _unsafe_json_text(field: str, kind: str) -> tuple[str, str]:
    sentinel = f"unsafe-{field}-{kind}"
    if kind == "oversized":
        return sentinel, '{"' + sentinel + '":"' + ("x" * (70 * 1024)) + '"}'
    return sentinel, f'{{"{sentinel}":"bad\x00value"}}'


@pytest.mark.parametrize("field", [CONF_ROOM_OVERRIDES, CONF_SCENE_ACTIONS])
@pytest.mark.parametrize("kind", ["oversized", "control_char"])
async def test_script_unsafe_json_is_not_redisplayed_or_leaked(
    hass: HomeAssistant,
    not_installed_panel: None,
    caplog: pytest.LogCaptureFixture,
    field: str,
    kind: str,
) -> None:
    sentinel, unsafe = _unsafe_json_text(field, kind)
    result = await _drive_flow_to_script(hass)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {**SCRIPT_INPUT, field: unsafe}
    )

    assert result["type"] == "form" and result["step_id"] == "script"
    assert result["errors"] == {field: "invalid_value"}
    suggested = _suggested_values(result)
    assert suggested[field] == "{}"
    assert sentinel not in repr(suggested)
    assert sentinel not in caplog.text
    assert not hass.config_entries.async_entries(DOMAIN)


@pytest.mark.parametrize("field", [CONF_ROOM_OVERRIDES, CONF_SCENE_ACTIONS])
@pytest.mark.parametrize("kind", ["oversized", "control_char"])
async def test_reconfigure_unsafe_json_is_not_redisplayed_persisted_or_leaked(
    hass: HomeAssistant,
    caplog: pytest.LogCaptureFixture,
    field: str,
    kind: str,
) -> None:
    sentinel, unsafe = _unsafe_json_text(field, kind)
    entry = _full_entry(hass)
    before = dict(entry.data)
    result = await entry.start_reconfigure_flow(hass)
    with (
        patch(APPLY) as apply,
        patch.object(hass.config_entries, "async_update_entry") as update_entry,
    ):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {**reconfigure_input(entry), field: unsafe}
        )

    assert result["type"] == "form" and result["step_id"] == "reconfigure"
    assert result["errors"] == {field: "invalid_value"}
    suggested = _suggested_values(result)
    assert suggested[field] == "{}"
    assert sentinel not in repr(suggested)
    assert sentinel not in caplog.text
    apply.assert_not_called()
    update_entry.assert_not_called()
    assert entry.data == before


_INVALID_DOMAIN_INPUTS = [
    [["light"]],
    {"light": True},
    ["light", 7],
    [{"domain": "light"}],
    ["light", "light"],
]


@pytest.mark.parametrize("domains", _INVALID_DOMAIN_INPUTS)
async def test_script_domain_validation_never_crashes_or_applies(
    hass: HomeAssistant,
    not_installed_panel: None,
    patch_installs: Any,
    domains: object,
) -> None:
    result = await _drive_flow_to_script(hass)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"],
        {**SCRIPT_INPUT, CONF_HA_CONTROL_DOMAINS: domains},
    )

    assert result["type"] == "form" and result["step_id"] == "script"
    assert result["errors"] == {CONF_HA_CONTROL_DOMAINS: "invalid_value"}
    assert not hass.config_entries.async_entries(DOMAIN)
    for component_id in (
        COMPONENT_BRIDGE,
        COMPONENT_VOICE,
        COMPONENT_WIFI_WATCHDOG,
        COMPONENT_BUS_WATCHDOG,
    ):
        assert not patch_installs.called(component_id)


@pytest.mark.parametrize("domains", _INVALID_DOMAIN_INPUTS)
async def test_reconfigure_domain_validation_never_applies_or_updates(
    hass: HomeAssistant, domains: object
) -> None:
    entry = _full_entry(hass)
    before = dict(entry.data)
    result = await entry.start_reconfigure_flow(hass)
    with (
        patch(APPLY) as apply,
        patch.object(hass.config_entries, "async_update_entry") as update_entry,
    ):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {**reconfigure_input(entry), CONF_HA_CONTROL_DOMAINS: domains},
        )

    assert result["type"] == "form" and result["step_id"] == "reconfigure"
    assert result["errors"] == {CONF_HA_CONTROL_DOMAINS: "invalid_value"}
    apply.assert_not_called()
    update_entry.assert_not_called()
    assert entry.data == before


async def test_new_panel_inherits_existing_fleet_global_values(
    hass: HomeAssistant,
    not_installed_panel: None,
    patch_installs: Any,
) -> None:
    inherited = {
        CONF_HA_CONTROL_ENABLED: True,
        CONF_HA_CONTROL_LABEL: "whole_home",
        CONF_ROOM_OVERRIDES: {"Office": "Office Bath"},
        CONF_HA_CONTROL_DOMAINS: ["light", "cover"],
        CONF_MAX_MIRRORED_ENTITIES: 33,
        CONF_SCENE_PANEL: "office",
        CONF_SCENE_ACTIONS: {},
    }
    _full_entry(hass, **inherited)
    result = await _drive_flow_to_script(hass)
    assert _schema_default(result, CONF_HA_CONTROL_ENABLED) is True
    assert _schema_default(result, CONF_HA_CONTROL_LABEL) == "whole_home"
    assert _schema_default(result, CONF_ROOM_OVERRIDES) == '{"Office":"Office Bath"}'
    assert _schema_default(result, CONF_HA_CONTROL_DOMAINS) == ["light", "cover"]
    assert _schema_default(result, CONF_SCENE_PANEL) == "office"

    submitted = {
        **SCRIPT_INPUT,
        CONF_NAME: "Backyard",
        CONF_HA_CONTROL_ENABLED: True,
        CONF_HA_CONTROL_LABEL: "whole_home",
        CONF_ROOM_OVERRIDES: '{"Office":"Office Bath"}',
        CONF_HA_CONTROL_DOMAINS: ["light", "cover"],
        CONF_MAX_MIRRORED_ENTITIES: 33,
        CONF_SCENE_PANEL: "office",
        CONF_SCENE_ACTIONS: "{}",
    }
    result = await hass.config_entries.flow.async_configure(result["flow_id"], submitted)
    assert result["type"] == "create_entry"
    assert {key: result["data"][key] for key in GLOBAL_KEYS} == inherited


async def test_adopted_panel_inherits_fleet_globals_over_stale_panel_toggle(
    hass: HomeAssistant,
) -> None:
    inherited = {
        CONF_HA_CONTROL_ENABLED: True,
        CONF_HA_CONTROL_LABEL: "whole_home",
        CONF_ROOM_OVERRIDES: {"Office": "Office Bath"},
        CONF_HA_CONTROL_DOMAINS: ["light", "cover"],
        CONF_MAX_MIRRORED_ENTITIES: 33,
        CONF_SCENE_PANEL: "office",
        CONF_SCENE_ACTIONS: {},
    }
    _full_entry(hass, **inherited)
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(
        PROBE,
        return_value=_installed(_env(panel="backyard", scene_bridge_enabled=False)),
    ):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], CONNECT_INPUT)

    assert result["type"] == "create_entry"
    assert {key: result["data"][key] for key in GLOBAL_KEYS} == inherited


async def test_new_panel_global_save_propagates_to_existing_fleet_entries(
    hass: HomeAssistant,
    not_installed_panel: None,
    patch_installs: Any,
) -> None:
    office = _full_entry(hass, **{"unrelated": "preserved"})
    before = dict(office.data)
    desired = {
        CONF_HA_CONTROL_ENABLED: True,
        CONF_HA_CONTROL_LABEL: "whole_home",
        CONF_ROOM_OVERRIDES: {"Office": "Office Bath"},
        CONF_HA_CONTROL_DOMAINS: ["light", "lock"],
        CONF_MAX_MIRRORED_ENTITIES: 33,
        CONF_SCENE_PANEL: "office",
        CONF_SCENE_ACTIONS: {},
    }
    result = await _drive_flow_to_script(hass)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"],
        {
            **SCRIPT_INPUT,
            CONF_NAME: "Backyard",
            CONF_HA_CONTROL_ENABLED: True,
            CONF_HA_CONTROL_LABEL: "whole_home",
            CONF_ROOM_OVERRIDES: '{"Office":"Office Bath"}',
            CONF_HA_CONTROL_DOMAINS: ["light", "lock"],
            CONF_MAX_MIRRORED_ENTITIES: 33,
            CONF_SCENE_PANEL: "office",
            CONF_SCENE_ACTIONS: "{}",
        },
    )

    assert result["type"] == "create_entry"
    assert {key: office.data[key] for key in GLOBAL_KEYS} == desired
    for key, value in before.items():
        assert office.data[key] == value


async def test_reconfigure_propagates_identical_globals_without_touching_panel_data(
    hass: HomeAssistant,
) -> None:
    office = _full_entry(hass, **{CONF_COMPONENTS: {COMPONENT_BRIDGE: True}})
    backyard = _full_entry(
        hass,
        **{
            CONF_PANEL: "backyard",
            CONF_HOST: "192.168.1.11",
            CONF_ROOT_PASSWORD: "backyard-root",
            CONF_MQTT_PASSWORD: "backyard-mqtt",
            DATA_SSH_HOST_KEY: "ssh-ed25519 BACKYARD",
            "unrelated": "keep-me",
        },
    )
    before = dict(backyard.data)
    actions = {
        "backyard:movie": {
            "domain": "script",
            "service": "turn_on",
            "target": {"entity_id": ["script.movie"]},
            "data": {"variables": {"safe": True}},
        }
    }
    desired = {
        CONF_HA_CONTROL_ENABLED: True,
        CONF_HA_CONTROL_LABEL: "controlled",
        CONF_ROOM_OVERRIDES: {"Office": "Office Bath"},
        CONF_HA_CONTROL_DOMAINS: ["light", "lock"],
        CONF_MAX_MIRRORED_ENTITIES: 99,
        CONF_SCENE_PANEL: "backyard",
        CONF_SCENE_ACTIONS: actions,
    }
    result = await office.start_reconfigure_flow(hass)
    submitted = {
        **reconfigure_input(office),
        **desired,
        CONF_ROOM_OVERRIDES: json.dumps(desired[CONF_ROOM_OVERRIDES]),
        CONF_SCENE_ACTIONS: json.dumps(actions),
    }
    with patch(APPLY, return_value="ssh-ed25519 STORED") as apply:
        result = await hass.config_entries.flow.async_configure(result["flow_id"], submitted)
    assert result["type"] == "abort" and result["reason"] == "reconfigure_successful"
    for entry in (office, backyard):
        assert {key: entry.data[key] for key in GLOBAL_KEYS} == desired
    assert "SCENE_BRIDGE_ENABLED=1" in apply.call_args.kwargs["env_content"]
    for key in (
        CONF_HOST,
        CONF_ROOT_PASSWORD,
        CONF_MQTT_PASSWORD,
        DATA_SSH_HOST_KEY,
        "unrelated",
    ):
        if key in before:
            assert backyard.data[key] == before[key]


@pytest.mark.parametrize("version", [1, 2])
async def test_migration_adds_safe_defaults_and_preserves_legacy_secrets(
    hass: HomeAssistant, version: int
) -> None:
    data = {
        CONF_PANEL: "office",
        CONF_COMPONENTS: {
            COMPONENT_BRIDGE: True,
            COMPONENT_VOICE: True,
            COMPONENT_HA_MIRROR: True,
        },
        CONF_HA_MIRROR_LABEL: "legacy_label",
        CONF_HA_MIRROR_WS_URL: "ws://ha/api/websocket",
        CONF_HA_MIRROR_TOKEN: "legacy-secret",
        CONF_HA_MIRROR_LEADER_PRIORITY: 7,
        CONF_VOICE_ENABLED: True,
        "unrelated": "preserved",
    }
    entry = MockConfigEntry(domain=DOMAIN, version=version, data=data)
    entry.add_to_hass(hass)
    assert await async_migrate_entry(hass, entry) is True
    assert entry.version == config_flow.BrilliantMqttConfigFlow.VERSION
    assert entry.data[CONF_COMPONENTS][COMPONENT_HA_MIRROR] is False
    assert entry.data[CONF_COMPONENTS][COMPONENT_VOICE] is True
    assert entry.data[CONF_COMPONENTS][COMPONENT_BLE_OBSERVER] is False
    assert entry.data[CONF_BLE_SCANNER_ENABLED] is False
    assert entry.data[CONF_BLE_OBSERVER_ALLOWLIST_JSON] == "[]"
    assert entry.data[CONF_HA_CONTROL_LABEL] == "legacy_label"
    assert entry.data[CONF_HA_CONTROL_ENABLED] is False
    assert entry.data[CONF_HA_CONTROL_DOMAINS] == ["light", "switch"]
    assert entry.data[CONF_ROOM_OVERRIDES] == {}
    assert entry.data[CONF_SCENE_PANEL] == "office"
    assert entry.data[CONF_SCENE_ACTIONS] == {}
    assert entry.data[CONF_HA_MIRROR_TOKEN] == "legacy-secret"
    assert entry.data[CONF_HA_MIRROR_WS_URL] == "ws://ha/api/websocket"
    assert entry.data[CONF_HA_MIRROR_LEADER_PRIORITY] == 7
    assert entry.data["unrelated"] == "preserved"

    migrated = dict(entry.data)
    assert await async_migrate_entry(hass, entry) is True
    assert entry.data == migrated


async def test_migration_does_not_overwrite_new_label_and_rejects_future_version(
    hass: HomeAssistant,
) -> None:
    current = MockConfigEntry(
        domain=DOMAIN,
        version=2,
        data={
            CONF_PANEL: "office",
            CONF_HA_CONTROL_LABEL: "new-label",
            CONF_HA_MIRROR_LABEL: "legacy-label",
        },
    )
    current.add_to_hass(hass)
    assert await async_migrate_entry(hass, current) is True
    assert current.data[CONF_HA_CONTROL_LABEL] == "new-label"

    future = MockConfigEntry(
        domain=DOMAIN,
        version=config_flow.BrilliantMqttConfigFlow.VERSION + 1,
        data={CONF_PANEL: "future"},
    )
    future.add_to_hass(hass)
    assert await async_migrate_entry(hass, future) is False
    assert future.version == config_flow.BrilliantMqttConfigFlow.VERSION + 1


async def test_v3_migration_forces_both_ble_kill_switches_off(
    hass: HomeAssistant,
) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        version=3,
        data={
            CONF_PANEL: "office",
            CONF_COMPONENTS: {
                COMPONENT_BRIDGE: True,
                COMPONENT_BLE_OBSERVER: True,
            },
            CONF_BLE_SCANNER_ENABLED: True,
            CONF_BLE_OBSERVER_ALLOWLIST_JSON: '[{"address":"AA:BB:CC:DD:EE:FF"}]',
        },
    )
    entry.add_to_hass(hass)

    assert await async_migrate_entry(hass, entry) is True

    assert entry.version == config_flow.BrilliantMqttConfigFlow.VERSION
    assert entry.data[CONF_COMPONENTS][COMPONENT_BLE_OBSERVER] is False
    assert entry.data[CONF_BLE_SCANNER_ENABLED] is False
    assert entry.data[CONF_BLE_OBSERVER_ALLOWLIST_JSON] == "[]"
