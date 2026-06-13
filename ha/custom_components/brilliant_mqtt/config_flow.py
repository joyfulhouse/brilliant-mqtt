"""Config flow: one entry per panel — each panel stores ITS OWN root password."""

from __future__ import annotations

import re
from typing import Any

import asyncssh
import voluptuous as vol
from homeassistant.config_entries import ConfigEntry, ConfigFlow, ConfigFlowResult, OptionsFlow
from homeassistant.core import HomeAssistant, callback

from .const import (
    CONF_HOST,
    CONF_MESH_PRIORITY,
    CONF_MQTT_HOST,
    CONF_MQTT_PASSWORD,
    CONF_MQTT_PORT,
    CONF_MQTT_USERNAME,
    CONF_PANEL,
    CONF_ROOT_PASSWORD,
    DATA_SSH_HOST_KEY,
    DEFAULT_AUTO_REPAIR,
    DEFAULT_OFFLINE_GRACE_MINUTES,
    DEFAULT_REPAIR_COOLDOWN_MINUTES,
    DOMAIN,
    MESH_PANEL,
    OPT_AUTO_REPAIR,
    OPT_OFFLINE_GRACE_MINUTES,
    OPT_REPAIR_COOLDOWN_MINUTES,
)
from .shell import AsyncsshShell

# Entry-data keys whose values pre-fill the NEXT add-panel form. Only the broker
# creds are genuinely fleet-shared; the root password is deliberately excluded —
# the operator runs per-controller root passwords, so reusing one by accident is
# both the likeliest mistake and the costliest. Host/slug are always blank too.
_PREFILL_KEYS = (
    CONF_MQTT_HOST,
    CONF_MQTT_PORT,
    CONF_MQTT_USERNAME,
    CONF_MQTT_PASSWORD,
)

# Slug grammar: the panel name doubles as the MQTT topic segment and unique_id.
_PANEL_SLUG = re.compile(r"[a-z0-9_-]+")

# Free-text fields that flow into the on-panel env file / SSH; a control char here
# corrupts the env file (panel_ops `_env_quote` rejects it as a hard backstop), so
# reject at the boundary for a friendly per-field message.
_NO_CONTROL_CHARS = (
    CONF_HOST,
    CONF_ROOT_PASSWORD,
    CONF_MQTT_HOST,
    CONF_MQTT_USERNAME,
    CONF_MQTT_PASSWORD,
)


def _has_control_char(value: str) -> bool:
    return any(ord(c) < 32 for c in value)


async def _validate_ssh(
    hass: HomeAssistant, host: str, password: str, pinned_key: str | None = None
) -> str:
    """One SSH connect; returns the captured/verified host public key. Raises on failure.

    With `pinned_key=None` this is a trust-on-first-use connect (new endpoint). With
    `pinned_key` set, asyncssh verifies the server host key BEFORE auth, so the root
    password is never offered to a host whose key no longer matches the pin (a mismatch
    raises asyncssh.HostKeyNotVerifiable instead of authenticating).
    """
    shell = AsyncsshShell(host, password, pinned_key)
    try:
        await shell.connect()
    finally:
        await shell.close()
    key = shell.pinned_host_key()
    if key is None:
        raise OSError("no host key captured")
    return key


class BrilliantMqttConfigFlow(ConfigFlow, domain=DOMAIN):
    """Add one Brilliant panel per entry."""

    VERSION = 1

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: ConfigEntry) -> OptionsFlow:
        return BrilliantMqttOptionsFlow()

    def _schema(self) -> vol.Schema:
        defaults: dict[str, Any] = {}
        if entries := self._async_current_entries():
            data = entries[-1].data
            defaults = {k: data[k] for k in _PREFILL_KEYS if k in data}
        return vol.Schema(
            {
                vol.Required(CONF_HOST): str,
                vol.Required(
                    CONF_ROOT_PASSWORD,
                    default=defaults.get(CONF_ROOT_PASSWORD, vol.UNDEFINED),
                ): str,
                vol.Required(CONF_PANEL): str,
                vol.Required(CONF_MESH_PRIORITY, default=0): vol.All(
                    vol.Coerce(int), vol.Range(min=0, max=99)
                ),
                vol.Required(
                    CONF_MQTT_HOST, default=defaults.get(CONF_MQTT_HOST, vol.UNDEFINED)
                ): str,
                vol.Required(CONF_MQTT_PORT, default=defaults.get(CONF_MQTT_PORT, 1883)): vol.All(
                    vol.Coerce(int), vol.Range(min=1, max=65535)
                ),
                vol.Required(
                    CONF_MQTT_USERNAME,
                    default=defaults.get(CONF_MQTT_USERNAME, vol.UNDEFINED),
                ): str,
                vol.Required(
                    CONF_MQTT_PASSWORD,
                    default=defaults.get(CONF_MQTT_PASSWORD, vol.UNDEFINED),
                ): str,
            }
        )

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            panel = user_input[CONF_PANEL].strip().lower()
            if panel == MESH_PANEL:
                errors[CONF_PANEL] = "reserved_panel"
            elif not _PANEL_SLUG.fullmatch(panel):
                errors[CONF_PANEL] = "invalid_panel"
            for key in _NO_CONTROL_CHARS:
                if _has_control_char(user_input[key]):
                    errors[key] = "invalid_value"
            if not errors:
                await self.async_set_unique_id(panel)
                self._abort_if_unique_id_configured()
                try:
                    host_key = await _validate_ssh(
                        self.hass, user_input[CONF_HOST], user_input[CONF_ROOT_PASSWORD]
                    )
                except (OSError, asyncssh.Error):
                    errors["base"] = "cannot_connect"
                else:
                    return self.async_create_entry(
                        title=f"Brilliant {panel}",
                        data={
                            **user_input,
                            CONF_PANEL: panel,
                            DATA_SSH_HOST_KEY: host_key,
                        },
                    )
        return self.async_show_form(step_id="user", data_schema=self._schema(), errors=errors)

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Rotate host/root password for one panel (slug is immutable)."""
        entry = self._get_reconfigure_entry()
        errors: dict[str, str] = {}
        if user_input is not None:
            for key in (CONF_HOST, CONF_ROOT_PASSWORD):
                if _has_control_char(user_input[key]):
                    errors[key] = "invalid_value"
            if not errors:
                # Same host → verify the rotated password against the STORED pin
                # (key checked before auth, so the password is never offered to a
                # changed/impostor host). Different host → a new endpoint/hardware,
                # so fresh TOFU like adding a panel.
                host_unchanged = user_input[CONF_HOST] == entry.data[CONF_HOST]
                pinned_key = entry.data.get(DATA_SSH_HOST_KEY) if host_unchanged else None
                try:
                    host_key = await _validate_ssh(
                        self.hass,
                        user_input[CONF_HOST],
                        user_input[CONF_ROOT_PASSWORD],
                        pinned_key=pinned_key,
                    )
                except asyncssh.HostKeyNotVerifiable:
                    # Same known-good host but its key no longer matches the pin: a
                    # reflash — or a MITM. Surface it; never silently re-pin (that is
                    # the TOFU bypass). The stored pin and password are left untouched.
                    errors["base"] = "host_key_changed"
                except (OSError, asyncssh.Error):
                    errors["base"] = "cannot_connect"
                else:
                    return self.async_update_reload_and_abort(
                        entry,
                        data={**entry.data, **user_input, DATA_SSH_HOST_KEY: host_key},
                    )
        schema = vol.Schema(
            {
                vol.Required(CONF_HOST, default=entry.data[CONF_HOST]): str,
                vol.Required(CONF_ROOT_PASSWORD): str,
            }
        )
        return self.async_show_form(step_id="reconfigure", data_schema=schema, errors=errors)


class BrilliantMqttOptionsFlow(OptionsFlow):
    """Per-panel behavior knobs; read live by the manager (no reload needed)."""

    async def async_step_init(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        if user_input is not None:
            return self.async_create_entry(data=user_input)
        opts = self.config_entry.options
        schema = vol.Schema(
            {
                vol.Required(
                    OPT_AUTO_REPAIR, default=opts.get(OPT_AUTO_REPAIR, DEFAULT_AUTO_REPAIR)
                ): bool,
                vol.Required(
                    OPT_OFFLINE_GRACE_MINUTES,
                    default=opts.get(OPT_OFFLINE_GRACE_MINUTES, DEFAULT_OFFLINE_GRACE_MINUTES),
                ): vol.All(vol.Coerce(int), vol.Range(min=2, max=120)),
                vol.Required(
                    OPT_REPAIR_COOLDOWN_MINUTES,
                    default=opts.get(OPT_REPAIR_COOLDOWN_MINUTES, DEFAULT_REPAIR_COOLDOWN_MINUTES),
                ): vol.All(vol.Coerce(int), vol.Range(min=5, max=1440)),
            }
        )
        return self.async_show_form(step_id="init", data_schema=schema)
