"""Menu-driven reconfigure for one legacy (single-config-entry) panel.

Instead of one wall of inputs, reconfigure opens a menu of focused sections
(connection, MQTT broker, components, HA control, advanced). Every section
prefills the stored values, and secret fields left blank silently re-use the
stored credential — a blank never overwrites, and a stored secret is never
redisplayed.
"""

from __future__ import annotations

import copy
from typing import TYPE_CHECKING, Any

import asyncssh
import voluptuous as vol
from homeassistant.config_entries import ConfigEntry, ConfigFlow, ConfigFlowResult

from .. import panel_ops
from ..components import REGISTRY, optional
from ..const import (
    COMPONENT_BRIDGE,
    COMPONENT_HA_MIRROR,
    CONF_COMPONENTS,
    CONF_ENTRY_KIND,
    CONF_HA_CONTROL_ENABLED,
    CONF_HA_CONTROL_LABEL,
    CONF_HOST,
    CONF_HOT_POLL_SECONDS,
    CONF_MESH_PRIORITY,
    CONF_MQTT_HOST,
    CONF_MQTT_PASSWORD,
    CONF_MQTT_PORT,
    CONF_MQTT_USERNAME,
    CONF_PANEL,
    CONF_RESYNC_SECONDS,
    CONF_ROOT_PASSWORD,
    CONF_VOICE_HA_HOST,
    DATA_SSH_HOST_KEY,
    DOMAIN,
    ENTRY_KIND_FLEET,
)
from ..voice_payload import VoicePayloadError
from . import gateway
from .schemas import (
    _PASSWORD_SELECTOR,
    _components_schema_fields,
    _control_schema_fields,
    _has_c0_control_char,
    _mqtt_schema_fields,
    _panel_cadence_schema_fields,
    _safe_control_redisplay_values,
    _validated_bounded_integer,
    _validated_control_input,
    control_char_errors,
)

if TYPE_CHECKING:
    _Base = ConfigFlow
else:
    _Base = object


def _resolved_secret(typed: object, stored: object) -> str:
    """Return the secret to apply: a blank submission keeps the stored one.

    "Blank" means empty after stripping. A password of nothing but whitespace is
    never a credential the operator meant to set, and storing it would push an
    unusable secret to the panel and lock Home Assistant out of it. Anything else
    is used verbatim and never stripped — leading or trailing whitespace can be
    a legitimate part of a real password.
    """
    candidate = str(typed or "")
    return candidate if candidate.strip() else str(stored)


RECONFIGURE_MENU_OPTIONS = (
    "reconfigure_connection",
    "reconfigure_mqtt",
    "reconfigure_components",
    "reconfigure_ha_control",
    "reconfigure_advanced",
)


class LegacyPanelReconfigureFlow(_Base):
    """Reconfigure steps for one legacy (single-config-entry) panel.

    Mixed into :class:`~.fleet.BrilliantMqttConfigFlow`; typed against
    ``ConfigFlow`` only when type checking so the runtime class keeps a single
    Home Assistant flow registration. The panel slug (CONF_PANEL) stays
    immutable after onboarding.
    """

    def _legacy_reconfigure_entry(self) -> ConfigEntry[Any] | None:
        entry = self._get_reconfigure_entry()
        if entry.data.get(CONF_ENTRY_KIND) == ENTRY_KIND_FLEET:
            return None
        return entry

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Offer one focused section per concern instead of a wall of inputs."""
        del user_input
        if self._legacy_reconfigure_entry() is None:
            return self.async_abort(reason="reconfigure_not_supported")
        return self.async_show_menu(
            step_id="reconfigure",
            menu_options=RECONFIGURE_MENU_OPTIONS,
        )

    async def _async_apply_legacy_section(
        self,
        entry: ConfigEntry[Any],
        updates: dict[str, Any],
        *,
        control_values: dict[str, Any] | None = None,
        desired_components: dict[str, bool] | None = None,
        remove_keys: frozenset[str] = frozenset(),
    ) -> tuple[dict[str, str], ConfigFlowResult | None]:
        """Push the merged config to the panel, then persist one section's edits.

        ``updates``/``control_values`` arrive with blank secrets already resolved
        back to the stored values, so the merge below is the exact config the
        panel should run. Returns ``(errors, result)``: transient failures come
        back as form errors and leave the entry untouched.
        """
        data = entry.data
        merged: dict[str, Any] = {**data, **updates, **(control_values or {})}
        for key in remove_keys:
            merged.pop(key, None)
        host = str(merged[CONF_HOST])
        password = str(merged[CONF_ROOT_PASSWORD])
        panel = str(data[CONF_PANEL])
        # Same host → verify against the STORED pin (key checked before auth, so
        # the password is never offered to a changed/impostor host). Different
        # host → new endpoint/hardware → fresh TOFU.
        host_unchanged = host == data[CONF_HOST]
        pinned_key = data.get(DATA_SSH_HOST_KEY) if host_unchanged else None
        if host_unchanged and pinned_key is None:
            # Defense-in-depth: same host but no stored pin (not reachable today
            # — every entry-write pins). Fail closed: an unpinned connect here
            # would re-offer the root password to an unverified host.
            return {"base": "host_key_changed"}, None
        env = panel_ops.render_env(
            panel=panel,
            mesh_priority=int(merged.get(CONF_MESH_PRIORITY, 0)),
            mqtt_host=str(merged[CONF_MQTT_HOST]),
            mqtt_port=int(merged[CONF_MQTT_PORT]),
            mqtt_username=str(merged[CONF_MQTT_USERNAME]),
            mqtt_password=str(merged[CONF_MQTT_PASSWORD]),
            scene_bridge_enabled=bool(merged.get(CONF_HA_CONTROL_ENABLED, False)),
            hot_poll_seconds=merged.get(CONF_HOT_POLL_SECONDS),
            resync_seconds=merged.get(CONF_RESYNC_SECONDS),
        )
        try:
            host_key = await gateway._apply_config(
                self.hass,
                host,
                password,
                pinned_key=pinned_key,
                env_content=env,
                expected_panel=panel,
            )
        except gateway._WrongPanelError:
            # The host runs a DIFFERENT panel's agent (likely a mistyped
            # address): refuse rather than overwrite + restart that panel.
            return {"base": "wrong_panel"}, None
        except asyncssh.HostKeyNotVerifiable:
            # Same known-good host but its key no longer matches the pin: a
            # reflash — or a MITM. Surface it; never silently re-pin. The
            # stored pin and entry data are left untouched.
            return {"base": "host_key_changed"}, None
        except (OSError, asyncssh.Error):
            return {"base": "cannot_connect"}, None
        except panel_ops.PanelOpError:
            # Connected fine, but writing the env / restarting failed.
            return {"base": "cannot_apply"}, None

        new_data: dict[str, Any] = {**data, **updates, DATA_SSH_HOST_KEY: host_key}
        for key in remove_keys:
            new_data.pop(key, None)
        if desired_components is not None:
            current: dict[str, Any] = dict(data.get(CONF_COMPONENTS) or {})
            new_data[CONF_COMPONENTS] = desired_components
            try:
                for component in optional():
                    was = bool(current.get(component.id, False))
                    now = desired_components[component.id]
                    if now == was:
                        continue
                    async with gateway._panel_session(
                        self.hass,
                        host,
                        password,
                        host_key,
                    ) as shell:
                        if now:
                            await REGISTRY[component.id].install(self.hass, shell, new_data)
                        else:
                            await REGISTRY[component.id].remove(shell)
            except VoicePayloadError:
                return {"base": "cannot_install_voice"}, None
            except (OSError, asyncssh.Error, panel_ops.PanelOpError):
                return {"base": "cannot_apply"}, None
        if control_values is not None:
            new_data.update(control_values)
            for fleet_entry in self.hass.config_entries.async_entries(DOMAIN):
                if fleet_entry.entry_id == entry.entry_id:
                    continue
                self.hass.config_entries.async_update_entry(
                    fleet_entry,
                    data={**fleet_entry.data, **copy.deepcopy(control_values)},
                )
        return {}, self.async_update_reload_and_abort(entry, data=new_data)

    def _legacy_section_form(
        self,
        step_id: str,
        fields: dict[Any, Any],
        errors: dict[str, str],
        user_input: dict[str, Any] | None,
    ) -> ConfigFlowResult:
        """One prefilled section form; error redisplays keep non-secret edits."""
        schema = vol.Schema(fields)
        if user_input is not None:
            # Keep the operator's just-made edits across an error redisplay (a
            # transient cannot_connect shouldn't wipe the fields back to the old
            # config) — but never echo credentials or unsafe JSON text.
            schema = self.add_suggested_values_to_schema(
                schema, _safe_control_redisplay_values(user_input)
            )
        return self.async_show_form(step_id=step_id, data_schema=schema, errors=errors)

    async def async_step_reconfigure_connection(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Change the panel address and/or rotate its root password."""
        entry = self._legacy_reconfigure_entry()
        if entry is None:
            return self.async_abort(reason="reconfigure_not_supported")
        errors: dict[str, str] = {}
        if user_input is not None:
            # Reject control chars on the RAW input first, THEN strip benign
            # surrounding whitespace — otherwise a stray trailing space would read
            # as a "different" host and downgrade the pinned check to a fresh TOFU.
            errors = control_char_errors(
                user_input,
                (CONF_HOST, CONF_ROOT_PASSWORD),
                detector=_has_c0_control_char,
            )
            if not errors:
                errors, result = await self._async_apply_legacy_section(
                    entry,
                    {
                        CONF_HOST: str(user_input[CONF_HOST]).strip(),
                        CONF_ROOT_PASSWORD: _resolved_secret(
                            user_input.get(CONF_ROOT_PASSWORD),
                            entry.data[CONF_ROOT_PASSWORD],
                        ),
                    },
                )
                if result is not None:
                    return result
        return self._legacy_section_form(
            "reconfigure_connection",
            {
                vol.Required(CONF_HOST, default=entry.data[CONF_HOST]): str,
                vol.Optional(CONF_ROOT_PASSWORD, default=""): _PASSWORD_SELECTOR,
            },
            errors,
            user_input,
        )

    async def async_step_reconfigure_mqtt(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Point the panel at a different MQTT broker or rotate its credential."""
        entry = self._legacy_reconfigure_entry()
        if entry is None:
            return self.async_abort(reason="reconfigure_not_supported")
        errors: dict[str, str] = {}
        if user_input is not None:
            errors = control_char_errors(
                user_input,
                (CONF_MQTT_HOST, CONF_MQTT_USERNAME, CONF_MQTT_PASSWORD),
                detector=_has_c0_control_char,
            )
            if not errors:
                errors, result = await self._async_apply_legacy_section(
                    entry,
                    {
                        CONF_MQTT_HOST: user_input[CONF_MQTT_HOST],
                        CONF_MQTT_PORT: user_input[CONF_MQTT_PORT],
                        CONF_MQTT_USERNAME: user_input[CONF_MQTT_USERNAME],
                        CONF_MQTT_PASSWORD: _resolved_secret(
                            user_input.get(CONF_MQTT_PASSWORD),
                            entry.data[CONF_MQTT_PASSWORD],
                        ),
                    },
                )
                if result is not None:
                    return result
        return self._legacy_section_form(
            "reconfigure_mqtt",
            _mqtt_schema_fields(entry.data),
            errors,
            user_input,
        )

    async def async_step_reconfigure_components(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Install/remove optional components using the stored SSH credential."""
        entry = self._legacy_reconfigure_entry()
        if entry is None:
            return self.async_abort(reason="reconfigure_not_supported")
        errors: dict[str, str] = {}
        if user_input is not None:
            # A control char in voice_ha_host crashes render_voice_env → _env_quote.
            if _has_c0_control_char(str(user_input.get(CONF_VOICE_HA_HOST, ""))):
                errors[CONF_VOICE_HA_HOST] = "invalid_value"
            if not errors:
                current: dict[str, Any] = dict(entry.data.get(CONF_COMPONENTS) or {})
                desired = {
                    component.id: bool(
                        user_input.get(component.id, current.get(component.id, False))
                    )
                    for component in optional()
                }
                desired[COMPONENT_BRIDGE] = True
                desired[COMPONENT_HA_MIRROR] = False
                # Strip optional-component checkbox ids (e.g. "voice") from the
                # entry-data updates: those belong only in CONF_COMPONENTS, not
                # as top-level stray keys in entry data.
                optional_ids = {component.id for component in optional()}
                errors, result = await self._async_apply_legacy_section(
                    entry,
                    {key: value for key, value in user_input.items() if key not in optional_ids},
                    desired_components=desired,
                )
                if result is not None:
                    return result
        return self._legacy_section_form(
            "reconfigure_components",
            _components_schema_fields(entry.data),
            errors,
            user_input,
        )

    async def async_step_reconfigure_ha_control(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Edit the HA-control globals; they fan out to every panel entry."""
        entry = self._legacy_reconfigure_entry()
        if entry is None:
            return self.async_abort(reason="reconfigure_not_supported")
        errors: dict[str, str] = {}
        if user_input is not None:
            # Gate the RAW label as well as the stripped one below: str.strip()
            # discards every C0 character Python calls whitespace — U+0009-U+000D
            # (tab, newline, vertical tab, form feed, carriage return) and
            # U+001C-U+001F (the file/group/record/unit separators) — so a label
            # ending in any of them would reach _validated_control_input already
            # trimmed and slip past its check. Load-bearing, not redundant.
            errors = control_char_errors(
                user_input,
                (CONF_HA_CONTROL_LABEL,),
                detector=_has_c0_control_char,
            )
            panels = frozenset(
                str(candidate.data[CONF_PANEL])
                for candidate in self.hass.config_entries.async_entries(DOMAIN)
                if isinstance(candidate.data.get(CONF_PANEL), str)
            )
            control_errors, control_values = _validated_control_input(
                user_input,
                panels=panels,
                default_panel=str(entry.data[CONF_PANEL]),
            )
            errors.update(control_errors)
            if not errors:
                errors, result = await self._async_apply_legacy_section(
                    entry,
                    {},
                    control_values=control_values,
                )
                if result is not None:
                    return result
        return self._legacy_section_form(
            "reconfigure_ha_control",
            _control_schema_fields(entry.data, panel_default=str(entry.data[CONF_PANEL])),
            errors,
            user_input,
        )

    async def async_step_reconfigure_advanced(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Edit the mesh leader-election priority and push it to the panel."""
        entry = self._legacy_reconfigure_entry()
        if entry is None:
            return self.async_abort(reason="reconfigure_not_supported")
        errors: dict[str, str] = {}
        if user_input is not None:
            updates = {CONF_MESH_PRIORITY: int(user_input[CONF_MESH_PRIORITY])}
            for key, minimum, maximum in (
                (CONF_HOT_POLL_SECONDS, 0, 60),
                (CONF_RESYNC_SECONDS, 60, 86400),
            ):
                if key not in user_input:
                    continue
                value = _validated_bounded_integer(
                    user_input[key],
                    minimum=minimum,
                    maximum=maximum,
                )
                if value is None:
                    # Cadence-specific key: the shared invalid_value copy talks
                    # about control characters — misleading for a range error.
                    errors[key] = "invalid_cadence"
                else:
                    updates[key] = value
            if not errors:
                errors, result = await self._async_apply_legacy_section(
                    entry,
                    updates,
                    remove_keys=frozenset(
                        key
                        for key in (CONF_HOT_POLL_SECONDS, CONF_RESYNC_SECONDS)
                        if key not in user_input
                    ),
                )
                if result is not None:
                    return result
        return self._legacy_section_form(
            "reconfigure_advanced",
            {
                vol.Required(
                    CONF_MESH_PRIORITY,
                    default=entry.data.get(CONF_MESH_PRIORITY, 0),
                ): vol.All(vol.Coerce(int), vol.Range(min=0, max=99)),
                **_panel_cadence_schema_fields(entry.data),
            },
            errors,
            user_input,
        )
