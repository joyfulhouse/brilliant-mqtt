"""Config flow: detection-first onboarding, adopt-installed, broadened reconfigure."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import asyncssh
import pytest
import voluptuous as vol
from homeassistant.const import CONF_NAME
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.brilliant_mqtt import panel_ops
from custom_components.brilliant_mqtt.config_flow import _PanelProbe, _slugify
from custom_components.brilliant_mqtt.const import (
    CONF_HOST,
    CONF_MESH_PRIORITY,
    CONF_MQTT_HOST,
    CONF_MQTT_PASSWORD,
    CONF_MQTT_PORT,
    CONF_MQTT_USERNAME,
    CONF_PANEL,
    CONF_ROOT_PASSWORD,
    DATA_SSH_HOST_KEY,
    DOMAIN,
    OPT_AUTO_REPAIR,
    OPT_OFFLINE_GRACE_MINUTES,
    OPT_REPAIR_COOLDOWN_MINUTES,
    OPT_TRUST_HOST_KEY_CHANGES,
)

PROBE = "custom_components.brilliant_mqtt.config_flow._probe_panel"
APPLY = "custom_components.brilliant_mqtt.config_flow._apply_config"

CONNECT_INPUT = {CONF_HOST: "10.100.0.10", CONF_ROOT_PASSWORD: "panelpass"}
MQTT_INPUT = {
    CONF_MQTT_HOST: "172.16.1.205",
    CONF_MQTT_PORT: 1883,
    CONF_MQTT_USERNAME: "brilliant",
    CONF_MQTT_PASSWORD: "mqttpass",
}
SCRIPT_INPUT = {CONF_NAME: "Office Bath", CONF_MESH_PRIORITY: 1}

RECONFIG_INPUT = {
    CONF_HOST: "10.100.0.10",
    CONF_ROOT_PASSWORD: "newpass",
    CONF_MQTT_HOST: "172.16.1.205",
    CONF_MQTT_PORT: 1883,
    CONF_MQTT_USERNAME: "brilliant",
    CONF_MQTT_PASSWORD: "newbroker",
    CONF_MESH_PRIORITY: 5,
}


def _not_installed(key: str = "ssh-ed25519 PINNED") -> _PanelProbe:
    return _PanelProbe(host_key=key, config=None)


def _installed(env: dict[str, str], key: str = "ssh-ed25519 PINNED") -> _PanelProbe:
    return _PanelProbe(host_key=key, config=env)


def _env(panel: str = "office", **over: Any) -> dict[str, str]:
    fields: dict[str, Any] = {
        "panel": panel,
        "mesh_priority": 3,
        "mqtt_host": "172.16.1.205",
        "mqtt_port": 8883,
        "mqtt_username": "brilliant",
        "mqtt_password": "frombroker",
    }
    fields.update(over)
    return panel_ops.parse_env(panel_ops.render_env(**fields))


def _full_entry(hass: HomeAssistant, **over: Any) -> MockConfigEntry:
    data: dict[str, Any] = {
        CONF_PANEL: "office",
        CONF_HOST: "10.100.0.10",
        CONF_ROOT_PASSWORD: "oldpass",
        CONF_MQTT_HOST: "old.broker",
        CONF_MQTT_PORT: 1883,
        CONF_MQTT_USERNAME: "brilliant",
        CONF_MQTT_PASSWORD: "oldbroker",
        CONF_MESH_PRIORITY: 0,
        DATA_SSH_HOST_KEY: "ssh-ed25519 STORED",
    }
    data.update(over)
    entry = MockConfigEntry(domain=DOMAIN, unique_id=data[CONF_PANEL], data=data)
    entry.add_to_hass(hass)
    return entry


# --- slugify ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "slug"),
    [
        ("Office Bath", "office-bath"),
        ("  Office  ", "office"),
        ("office_front", "office_front"),
        ("ADU Main", "adu-main"),
        ("Garage (Left)", "garage-left"),
        ("!!!", ""),
    ],
)
def test_slugify(name: str, slug: str) -> None:
    assert _slugify(name) == slug


# --- onboarding: not installed (three steps) -------------------------------


async def test_not_installed_walks_three_steps(hass: HomeAssistant) -> None:
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    assert result["type"] == "form" and result["step_id"] == "user"

    with patch(PROBE, return_value=_not_installed()):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], CONNECT_INPUT)
    assert result["type"] == "form" and result["step_id"] == "broker"

    result = await hass.config_entries.flow.async_configure(result["flow_id"], MQTT_INPUT)
    assert result["type"] == "form" and result["step_id"] == "script"

    result = await hass.config_entries.flow.async_configure(result["flow_id"], SCRIPT_INPUT)
    assert result["type"] == "create_entry"
    assert result["title"] == "Brilliant office-bath"
    data = result["data"]
    assert data[CONF_PANEL] == "office-bath"  # slugified from "Office Bath"
    assert data[CONF_HOST] == "10.100.0.10"
    assert data[CONF_ROOT_PASSWORD] == "panelpass"
    assert data[DATA_SSH_HOST_KEY] == "ssh-ed25519 PINNED"
    assert data[CONF_MQTT_HOST] == "172.16.1.205"
    assert data[CONF_MQTT_PASSWORD] == "mqttpass"
    assert data[CONF_MESH_PRIORITY] == 1


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


async def test_mqtt_step_rejects_control_char(hass: HomeAssistant) -> None:
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(PROBE, return_value=_not_installed()):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], CONNECT_INPUT)
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {**MQTT_INPUT, CONF_MQTT_PASSWORD: "bad\npass"}
    )
    assert result["type"] == "form" and result["step_id"] == "broker"
    assert result["errors"] == {CONF_MQTT_PASSWORD: "invalid_value"}


async def test_mqtt_step_prefills_from_prior_panel(hass: HomeAssistant) -> None:
    _full_entry(hass, **{CONF_MQTT_HOST: "172.16.1.205", CONF_MQTT_PASSWORD: "shared"})
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
    assert defaults[CONF_MQTT_HOST] == "172.16.1.205"
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


# --- onboarding: already installed (adopt) ---------------------------------


async def test_installed_adopts_from_panel(hass: HomeAssistant) -> None:
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(PROBE, return_value=_installed(_env(panel="office"))):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], CONNECT_INPUT)
    assert result["type"] == "create_entry"
    assert result["title"] == "Brilliant office"
    data = result["data"]
    assert data[CONF_PANEL] == "office"
    assert data[CONF_HOST] == "10.100.0.10"  # from step 1
    assert data[CONF_ROOT_PASSWORD] == "panelpass"  # from step 1
    assert data[DATA_SSH_HOST_KEY] == "ssh-ed25519 PINNED"
    # broker + mesh adopted FROM the panel, not asked
    assert data[CONF_MQTT_HOST] == "172.16.1.205"
    assert data[CONF_MQTT_PORT] == 8883
    assert data[CONF_MQTT_PASSWORD] == "frombroker"
    assert data[CONF_MESH_PRIORITY] == 3


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


# --- reconfigure -----------------------------------------------------------


async def test_reconfigure_same_host_applies_and_pushes(hass: HomeAssistant) -> None:
    entry = _full_entry(hass)
    result = await entry.start_reconfigure_flow(hass)
    assert result["type"] == "form"

    with patch(APPLY, return_value="ssh-ed25519 STORED") as apply:
        result = await hass.config_entries.flow.async_configure(result["flow_id"], RECONFIG_INPUT)

    assert result["type"] == "abort" and result["reason"] == "reconfigure_successful"
    # Same host → STORED pin used (verify-before-auth), never None.
    assert apply.call_args.kwargs["pinned_key"] == "ssh-ed25519 STORED"
    # The pushed env carries the NEW broker/mesh but the immutable slug.
    env = apply.call_args.kwargs["env_content"]
    assert 'BRILLIANT_PANEL="office"' in env
    assert "MESH_PRIORITY=5" in env
    assert 'MQTT_PASSWORD="newbroker"' in env
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
            result["flow_id"], {**RECONFIG_INPUT, CONF_HOST: "10.100.0.99"}
        )
    assert result["type"] == "abort" and result["reason"] == "reconfigure_successful"
    assert apply.call_args.kwargs["pinned_key"] is None  # fresh TOFU for the new host
    assert entry.data[CONF_HOST] == "10.100.0.99"
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
