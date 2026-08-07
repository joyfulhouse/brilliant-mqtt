"""Legacy single-entry panel reconfigure steps (pre-subentry panels)."""

from __future__ import annotations

import copy
from typing import TYPE_CHECKING, Any

import asyncssh
import voluptuous as vol
from homeassistant.config_entries import ConfigFlow, ConfigFlowResult
from homeassistant.helpers.selector import (
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)

from .. import panel_ops
from ..components import REGISTRY, optional
from ..const import (
    COMPONENT_BRIDGE,
    COMPONENT_HA_MIRROR,
    CONF_COMPONENTS,
    CONF_ENTRY_KIND,
    CONF_HA_CONTROL_ENABLED,
    CONF_HOST,
    CONF_MESH_PRIORITY,
    CONF_MQTT_HOST,
    CONF_MQTT_PASSWORD,
    CONF_MQTT_PORT,
    CONF_MQTT_USERNAME,
    CONF_PANEL,
    CONF_ROOT_PASSWORD,
    CONF_VOICE_HA_HOST,
    DATA_SSH_HOST_KEY,
    DOMAIN,
    ENTRY_KIND_FLEET,
)
from ..voice_payload import VoicePayloadError
from . import gateway
from .schemas import (
    _GLOBAL_KEYS,
    _NO_CONTROL_CHARS,
    _components_schema_fields,
    _control_schema_fields,
    _has_control_char,
    _mqtt_schema_fields,
    _safe_control_redisplay_values,
    _validated_control_input,
    control_char_errors,
)

if TYPE_CHECKING:
    _Base = ConfigFlow
else:
    _Base = object


class LegacyPanelReconfigureFlow(_Base):
    """Reconfigure steps for one legacy (single-config-entry) panel.

    Mixed into :class:`~.fleet.BrilliantMqttConfigFlow`; typed against
    ``ConfigFlow`` only when type checking so the runtime class keeps a single
    Home Assistant flow registration.
    """

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Edit host/password/broker/mesh/components for one panel and push it.

        The panel slug (CONF_PANEL) is immutable after onboarding.
        """
        entry = self._get_reconfigure_entry()
        if entry.data.get(CONF_ENTRY_KIND) == ENTRY_KIND_FLEET:
            return self.async_abort(reason="reconfigure_not_supported")
        errors: dict[str, str] = {}
        if user_input is not None:
            # Reject control chars on the RAW input first, THEN strip benign surrounding
            # whitespace — otherwise a stray trailing space would read as a "different"
            # host and downgrade the same-host pinned check to a fresh TOFU.
            errors = control_char_errors(user_input, _NO_CONTROL_CHARS)
            # voice_ha_host is now on this form; validate it for control chars too
            # (a control char there crashes render_voice_env → _env_quote).
            ha_host_val = str(user_input.get(CONF_VOICE_HA_HOST, ""))
            if _has_control_char(ha_host_val):
                errors[CONF_VOICE_HA_HOST] = "invalid_value"
            panels = frozenset(
                str(candidate.data[CONF_PANEL])
                for candidate in self.hass.config_entries.async_entries(DOMAIN)
                if isinstance(candidate.data.get(CONF_PANEL), str)
            )
            control_errors, control_values = _validated_control_input(
                user_input, panels=panels, default_panel=str(entry.data[CONF_PANEL])
            )
            errors.update(control_errors)
            if not errors:
                user_input = {**user_input, CONF_HOST: user_input[CONF_HOST].strip()}
                # Same host → verify the rotated password against the STORED pin (key
                # checked before auth, so the password is never offered to a changed/
                # impostor host). Different host → new endpoint/hardware → fresh TOFU.
                host_unchanged = user_input[CONF_HOST] == entry.data[CONF_HOST]
                pinned_key = entry.data.get(DATA_SSH_HOST_KEY) if host_unchanged else None
                if host_unchanged and pinned_key is None:
                    # Defense-in-depth: same host but no stored pin (not reachable today
                    # — every entry-write pins). Fail closed: an unpinned connect here
                    # would re-offer the root password to an unverified host.
                    errors["base"] = "host_key_changed"
                else:
                    env = panel_ops.render_env(
                        panel=entry.data[CONF_PANEL],
                        mesh_priority=user_input[CONF_MESH_PRIORITY],
                        mqtt_host=user_input[CONF_MQTT_HOST],
                        mqtt_port=user_input[CONF_MQTT_PORT],
                        mqtt_username=user_input[CONF_MQTT_USERNAME],
                        mqtt_password=user_input[CONF_MQTT_PASSWORD],
                        scene_bridge_enabled=control_values[CONF_HA_CONTROL_ENABLED],
                    )
                    try:
                        host_key = await gateway._apply_config(
                            self.hass,
                            user_input[CONF_HOST],
                            user_input[CONF_ROOT_PASSWORD],
                            pinned_key=pinned_key,
                            env_content=env,
                            expected_panel=entry.data[CONF_PANEL],
                        )
                    except gateway._WrongPanelError:
                        # The host runs a DIFFERENT panel's agent (likely a mistyped
                        # address): refuse rather than overwrite + restart that panel.
                        errors["base"] = "wrong_panel"
                    except asyncssh.HostKeyNotVerifiable:
                        # Same known-good host but its key no longer matches the pin: a
                        # reflash — or a MITM. Surface it; never silently re-pin. The
                        # stored pin and entry data are left untouched.
                        errors["base"] = "host_key_changed"
                    except (OSError, asyncssh.Error):
                        errors["base"] = "cannot_connect"
                    except panel_ops.PanelOpError:
                        # Connected fine, but writing the env / restarting failed.
                        errors["base"] = "cannot_apply"
                    else:
                        # Env push succeeded — now diff and apply component changes.
                        current: dict[str, Any] = dict(entry.data.get(CONF_COMPONENTS) or {})
                        desired = {
                            c.id: bool(user_input.get(c.id, current.get(c.id, False)))
                            for c in optional()
                        }
                        desired[COMPONENT_BRIDGE] = True
                        desired[COMPONENT_HA_MIRROR] = False
                        # Strip optional-component checkbox ids (e.g. "voice") from
                        # user_input before merging: those belong only in CONF_COMPONENTS,
                        # not as top-level stray keys in entry data.
                        _opt_ids = {c.id for c in optional()}
                        clean_input = {
                            k: v
                            for k, v in user_input.items()
                            if k not in _opt_ids and k not in _GLOBAL_KEYS
                        }
                        new_data: dict[str, Any] = {
                            **entry.data,
                            **clean_input,
                            DATA_SSH_HOST_KEY: host_key,
                            CONF_COMPONENTS: desired,
                            **control_values,
                        }
                        try:
                            for c in optional():
                                was: bool = bool(current.get(c.id, False))
                                now: bool = desired[c.id]
                                if now and not was:
                                    async with gateway._panel_session(
                                        self.hass,
                                        user_input[CONF_HOST],
                                        user_input[CONF_ROOT_PASSWORD],
                                        host_key,
                                    ) as shell:
                                        await REGISTRY[c.id].install(self.hass, shell, new_data)
                                elif was and not now:
                                    async with gateway._panel_session(
                                        self.hass,
                                        user_input[CONF_HOST],
                                        user_input[CONF_ROOT_PASSWORD],
                                        host_key,
                                    ) as shell:
                                        await REGISTRY[c.id].remove(shell)
                        except VoicePayloadError:
                            errors["base"] = "cannot_install_voice"
                        except (OSError, asyncssh.Error, panel_ops.PanelOpError):
                            errors["base"] = "cannot_apply"
                        else:
                            for fleet_entry in self.hass.config_entries.async_entries(DOMAIN):
                                if fleet_entry.entry_id == entry.entry_id:
                                    continue
                                self.hass.config_entries.async_update_entry(
                                    fleet_entry,
                                    data={**fleet_entry.data, **copy.deepcopy(control_values)},
                                )
                            return self.async_update_reload_and_abort(entry, data=new_data)
        data = entry.data
        schema = vol.Schema(
            {
                vol.Required(CONF_HOST, default=data[CONF_HOST]): str,
                vol.Required(CONF_ROOT_PASSWORD): TextSelector(
                    TextSelectorConfig(type=TextSelectorType.PASSWORD)
                ),
                **_mqtt_schema_fields(data),
                vol.Required(CONF_MESH_PRIORITY, default=data.get(CONF_MESH_PRIORITY, 0)): vol.All(
                    vol.Coerce(int), vol.Range(min=0, max=99)
                ),
                **_components_schema_fields(data, new_install=False),
                **_control_schema_fields(data, panel_default=str(data[CONF_PANEL])),
            }
        )
        # Keep the operator's just-made edits across an error redisplay (a transient
        # cannot_connect / wrong_panel shouldn't wipe all six fields back to the old config).
        if user_input is not None:
            schema = self.add_suggested_values_to_schema(
                schema, _safe_control_redisplay_values(user_input)
            )
        return self.async_show_form(step_id="reconfigure", data_schema=schema, errors=errors)
