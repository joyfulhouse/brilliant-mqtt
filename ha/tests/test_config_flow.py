"""Config flow: per-panel entries, TOFU pinning, reserved slug, dedupe, validation."""

from __future__ import annotations

from unittest.mock import patch

import asyncssh
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry

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

USER_INPUT = {
    CONF_HOST: "10.100.0.10",
    CONF_ROOT_PASSWORD: "panelpass",
    CONF_PANEL: "Office",
    CONF_MESH_PRIORITY: 1,
    CONF_MQTT_HOST: "172.16.1.205",
    CONF_MQTT_PORT: 1883,
    CONF_MQTT_USERNAME: "brilliant",
    CONF_MQTT_PASSWORD: "mqttpass",
}

VALIDATE = "custom_components.brilliant_mqtt.config_flow._validate_ssh"


async def test_user_flow_creates_entry_with_pinned_key(hass: HomeAssistant) -> None:
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    assert result["type"] == "form"

    with patch(VALIDATE, return_value="ssh-ed25519 PINNED"):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], USER_INPUT)

    assert result["type"] == "create_entry"
    assert result["title"] == "Brilliant office"
    assert result["data"][CONF_PANEL] == "office"  # slug normalized to lowercase
    assert result["data"][DATA_SSH_HOST_KEY] == "ssh-ed25519 PINNED"
    assert result["data"][CONF_ROOT_PASSWORD] == "panelpass"


async def test_mesh_slug_is_rejected(hass: HomeAssistant) -> None:
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(VALIDATE, return_value="ssh-ed25519 PINNED"):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {**USER_INPUT, CONF_PANEL: "mesh"}
        )
    assert result["type"] == "form"
    assert result["errors"] == {CONF_PANEL: "reserved_panel"}


async def test_invalid_panel_slug_is_rejected(hass: HomeAssistant) -> None:
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(VALIDATE, return_value="ssh-ed25519 PINNED"):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {**USER_INPUT, CONF_PANEL: "Office Front!"}
        )
    assert result["type"] == "form"
    assert result["errors"] == {CONF_PANEL: "invalid_panel"}


async def test_control_character_in_field_is_rejected(hass: HomeAssistant) -> None:
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(VALIDATE, return_value="ssh-ed25519 PINNED"):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"], {**USER_INPUT, CONF_MQTT_PASSWORD: "bad\npass"}
        )
    assert result["type"] == "form"
    assert result["errors"] == {CONF_MQTT_PASSWORD: "invalid_value"}


async def test_duplicate_panel_aborts(hass: HomeAssistant) -> None:
    MockConfigEntry(domain=DOMAIN, unique_id="office", data={CONF_PANEL: "office"}).add_to_hass(
        hass
    )
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(VALIDATE, return_value="ssh-ed25519 PINNED"):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], USER_INPUT)
    assert result["type"] == "abort"
    assert result["reason"] == "already_configured"


async def test_ssh_failure_shows_cannot_connect(hass: HomeAssistant) -> None:
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": "user"})
    with patch(VALIDATE, side_effect=OSError("nope")):
        result = await hass.config_entries.flow.async_configure(result["flow_id"], USER_INPUT)
    assert result["type"] == "form"
    assert result["errors"] == {"base": "cannot_connect"}


async def test_reconfigure_rejects_control_character(hass: HomeAssistant) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="office",
        data={CONF_PANEL: "office", CONF_HOST: "10.100.0.10", CONF_ROOT_PASSWORD: "panelpass"},
    )
    entry.add_to_hass(hass)
    result = await entry.start_reconfigure_flow(hass)
    assert result["type"] == "form"

    with patch(VALIDATE, return_value="ssh-ed25519 PINNED"):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {CONF_HOST: "10.100.0.10", CONF_ROOT_PASSWORD: "bad\npass"},
        )
    assert result["type"] == "form"
    assert result["errors"] == {CONF_ROOT_PASSWORD: "invalid_value"}


async def test_reconfigure_same_host_verifies_against_stored_pin(hass: HomeAssistant) -> None:
    """Same host → _validate_ssh is called WITH the stored pin so the password is
    verified against the known-good key before being offered (no fresh TOFU)."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="office",
        data={
            CONF_PANEL: "office",
            CONF_HOST: "10.100.0.10",
            CONF_ROOT_PASSWORD: "oldpass",
            DATA_SSH_HOST_KEY: "ssh-ed25519 STORED",
        },
    )
    entry.add_to_hass(hass)
    result = await entry.start_reconfigure_flow(hass)

    with patch(VALIDATE, return_value="ssh-ed25519 STORED") as validate:
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {CONF_HOST: "10.100.0.10", CONF_ROOT_PASSWORD: "newpass"},
        )

    assert result["type"] == "abort"
    assert result["reason"] == "reconfigure_successful"
    # The same-host path must pass the STORED pin (verify-before-auth), never None.
    assert validate.call_args.kwargs["pinned_key"] == "ssh-ed25519 STORED"
    assert entry.data[CONF_ROOT_PASSWORD] == "newpass"
    assert entry.data[DATA_SSH_HOST_KEY] == "ssh-ed25519 STORED"


async def test_reconfigure_same_host_key_mismatch_shows_error_and_keeps_pin(
    hass: HomeAssistant,
) -> None:
    """Same host where the pinned connect fails host-key verification → form with
    host_key_changed and the stored pin is UNCHANGED (no silent re-pin / TOFU bypass)."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="office",
        data={
            CONF_PANEL: "office",
            CONF_HOST: "10.100.0.10",
            CONF_ROOT_PASSWORD: "oldpass",
            DATA_SSH_HOST_KEY: "ssh-ed25519 STORED",
        },
    )
    entry.add_to_hass(hass)
    result = await entry.start_reconfigure_flow(hass)

    with patch(VALIDATE, side_effect=asyncssh.HostKeyNotVerifiable("changed")):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {CONF_HOST: "10.100.0.10", CONF_ROOT_PASSWORD: "newpass"},
        )

    assert result["type"] == "form"
    assert result["errors"] == {"base": "host_key_changed"}
    # No silent re-pin, and the rotated password was NOT written to the entry.
    assert entry.data[DATA_SSH_HOST_KEY] == "ssh-ed25519 STORED"
    assert entry.data[CONF_ROOT_PASSWORD] == "oldpass"


async def test_reconfigure_same_host_missing_pin_fails_closed(hass: HomeAssistant) -> None:
    """Defense-in-depth: same host with NO stored pin must NOT fall back to an unpinned
    connect (which would re-open the bypass). It fails closed with host_key_changed and
    never offers the password — _validate_ssh is not called at all."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="office",
        data={
            CONF_PANEL: "office",
            CONF_HOST: "10.100.0.10",
            CONF_ROOT_PASSWORD: "oldpass",
            # No DATA_SSH_HOST_KEY — models a future schema where the pin is absent.
        },
    )
    entry.add_to_hass(hass)
    result = await entry.start_reconfigure_flow(hass)

    with patch(VALIDATE, return_value="ssh-ed25519 NEW") as validate:
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {CONF_HOST: "10.100.0.10", CONF_ROOT_PASSWORD: "newpass"},
        )

    assert result["type"] == "form"
    assert result["errors"] == {"base": "host_key_changed"}
    validate.assert_not_called()  # no connect, pinned or unpinned → password not sent
    assert entry.data[CONF_ROOT_PASSWORD] == "oldpass"  # nothing written


async def test_reconfigure_different_host_does_fresh_tofu(hass: HomeAssistant) -> None:
    """A changed host is a new endpoint → _validate_ssh is called with pinned_key=None
    (fresh TOFU) and the entry re-pins to the new host's key."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="office",
        data={
            CONF_PANEL: "office",
            CONF_HOST: "10.100.0.10",
            CONF_ROOT_PASSWORD: "oldpass",
            DATA_SSH_HOST_KEY: "ssh-ed25519 STORED",
        },
    )
    entry.add_to_hass(hass)
    result = await entry.start_reconfigure_flow(hass)

    with patch(VALIDATE, return_value="ssh-ed25519 NEWHOST") as validate:
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {CONF_HOST: "10.100.0.99", CONF_ROOT_PASSWORD: "oldpass"},
        )

    assert result["type"] == "abort"
    assert result["reason"] == "reconfigure_successful"
    assert validate.call_args.kwargs["pinned_key"] is None  # fresh TOFU for the new host
    assert entry.data[CONF_HOST] == "10.100.0.99"
    assert entry.data[DATA_SSH_HOST_KEY] == "ssh-ed25519 NEWHOST"


async def test_options_flow_saves_behavior_knobs(hass: HomeAssistant) -> None:
    """The options flow is registered and persists the manager's behavior knobs."""
    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data={CONF_PANEL: "office"})
    entry.add_to_hass(hass)

    result = await hass.config_entries.options.async_init(entry.entry_id)
    assert result["type"] == "form"
    assert result["step_id"] == "init"

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
