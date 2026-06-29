"""Config flow: one entry per panel — each panel stores ITS OWN root password.

Onboarding is detection-first: step 1 connects (the only required inputs) and, if the
agent is already installed, ADOPTS the panel by reading its config back — no further
questions, no changes to the panel. A not-yet-installed panel continues to the MQTT
broker (pre-filled from a prior panel) and the panel-settings step. Reconfigure edits
every mutable setting and pushes the change to the panel; the slug is immutable.
"""

from __future__ import annotations

import re
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

import asyncssh
import voluptuous as vol
from homeassistant.config_entries import ConfigEntry, ConfigFlow, ConfigFlowResult, OptionsFlow
from homeassistant.const import CONF_NAME
from homeassistant.core import HomeAssistant, callback

from . import _fleet_lock, panel_ops
from .components import REGISTRY, optional
from .const import (
    COMPONENT_BRIDGE,
    COMPONENT_VOICE,
    CONF_COMPONENTS,
    CONF_HOST,
    CONF_MESH_PRIORITY,
    CONF_MQTT_HOST,
    CONF_MQTT_PASSWORD,
    CONF_MQTT_PORT,
    CONF_MQTT_USERNAME,
    CONF_PANEL,
    CONF_ROOT_PASSWORD,
    CONF_VOICE_ENABLED,
    CONF_VOICE_HA_HOST,
    CONF_VOICE_WAKE_WORD,
    DATA_SSH_HOST_KEY,
    DEFAULT_AUTO_REPAIR,
    DEFAULT_OFFLINE_GRACE_MINUTES,
    DEFAULT_REPAIR_COOLDOWN_MINUTES,
    DEFAULT_TRUST_HOST_KEY_CHANGES,
    DEFAULT_VOICE_WAKE_WORD,
    DOMAIN,
    MESH_PANEL,
    OPT_AUTO_REPAIR,
    OPT_OFFLINE_GRACE_MINUTES,
    OPT_REPAIR_COOLDOWN_MINUTES,
    OPT_TRUST_HOST_KEY_CHANGES,
    VOICE_WAKE_WORDS,
)
from .shell import AsyncsshShell, PanelShell
from .voice_payload import VoicePayloadError

# Entry-data keys whose values pre-fill the NEXT add-panel MQTT step. Only the broker
# creds are genuinely fleet-shared; the root password is deliberately excluded — the
# operator runs per-controller root passwords, so reusing one by accident is both the
# likeliest mistake and the costliest. Host/name are always blank too.
_PREFILL_KEYS = (
    CONF_MQTT_HOST,
    CONF_MQTT_PORT,
    CONF_MQTT_USERNAME,
    CONF_MQTT_PASSWORD,
)

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

_SLUG_SEPARATORS = re.compile(r"[\s.]+")
_SLUG_INVALID = re.compile(r"[^a-z0-9_-]+")
_SLUG_DASH_RUNS = re.compile(r"-{2,}")


class _WrongPanelError(Exception):
    """A reconfigure connected to a host already running a DIFFERENT panel's agent.

    Guards against a mistyped host (e.g. another controller's IP in a multi-panel
    fleet): pushing this entry's env there would overwrite that panel's identity and
    restart it. Carries the foreign panel slug found on the box.
    """


def _has_control_char(value: str) -> bool:
    return any(ord(c) < 32 for c in value)


def _control_char_errors(user_input: dict[str, Any], keys: tuple[str, ...]) -> dict[str, str]:
    """Per-field ``invalid_value`` errors for any *keys* whose value has a control char."""
    return {key: "invalid_value" for key in keys if _has_control_char(user_input[key])}


def _mqtt_schema_fields(source: Mapping[str, Any]) -> dict[Any, Any]:
    """The four broker fields shared by the add-broker and reconfigure steps.

    Defaults come from *source* (prior-entry prefill, or the entry being reconfigured);
    the three string fields fall back to blank, the port to 1883.
    """
    return {
        vol.Required(CONF_MQTT_HOST, default=source.get(CONF_MQTT_HOST, vol.UNDEFINED)): str,
        vol.Required(CONF_MQTT_PORT, default=source.get(CONF_MQTT_PORT, 1883)): vol.All(
            vol.Coerce(int), vol.Range(min=1, max=65535)
        ),
        vol.Required(
            CONF_MQTT_USERNAME, default=source.get(CONF_MQTT_USERNAME, vol.UNDEFINED)
        ): str,
        vol.Required(
            CONF_MQTT_PASSWORD, default=source.get(CONF_MQTT_PASSWORD, vol.UNDEFINED)
        ): str,
    }


def _components_schema_fields(source: Mapping[str, Any]) -> dict[Any, Any]:
    """One checkbox per OPTIONAL component (bridge is implicit/locked), plus voice sub-fields."""
    chosen: Mapping[str, Any] = source.get(CONF_COMPONENTS, {})
    fields: dict[Any, Any] = {}
    for c in optional():
        fields[vol.Required(c.id, default=chosen.get(c.id, c.default_enabled))] = bool
    # Voice sub-config (meaningful only when voice is checked; validated leniently).
    fields[
        vol.Required(
            CONF_VOICE_WAKE_WORD,
            default=source.get(CONF_VOICE_WAKE_WORD, DEFAULT_VOICE_WAKE_WORD),
        )
    ] = vol.In(list(VOICE_WAKE_WORDS))
    fields[vol.Optional(CONF_VOICE_HA_HOST, default=source.get(CONF_VOICE_HA_HOST, ""))] = str
    return fields


def _slugify(name: str) -> str:
    """Free-form panel name → the slug stored as CONF_PANEL / MQTT topic id.

    "Office Bath" → "office-bath". Lowercases, turns whitespace/dots into hyphens,
    drops anything outside ``[a-z0-9_-]``, collapses repeats, trims. May return ""
    (the caller rejects that as invalid_name). HA humanizes the slug back for display
    (entity.py: "office-bath" → "Office Bath"), so the original name need not be stored.
    """
    slug = _SLUG_SEPARATORS.sub("-", name.strip().lower())
    slug = _SLUG_INVALID.sub("", slug)
    return _SLUG_DASH_RUNS.sub("-", slug).strip("-_")


def _adopt_data(env: dict[str, str]) -> dict[str, Any] | None:
    """Map an installed agent's parsed env file to entry data; None if unusable.

    The slug is trusted from the device but still gated to the SAME canonical form
    the typed path produces: a hand-deployed env with BRILLIANT_PANEL="mesh", empty,
    or any non-canonical value (spaces, uppercase, leading/trailing or doubled
    separators like "-office") must NOT become a config entry — it would collide with
    the reserved pseudo-panel or break the MQTT topic contract. Same for an
    out-of-range port. All of these surface as cannot_read_config.
    """
    try:
        panel = env[panel_ops.ENV_PANEL]
        # Require the adopted slug to be exactly what _slugify would produce, so the
        # adopt and typed-name paths can never disagree on what a valid slug is.
        if not panel or panel == MESH_PANEL or _slugify(panel) != panel:
            return None
        # MQTT_PORT and MESH_PRIORITY are OPTIONAL in the agent's env contract
        # (config.py defaults them to 1883 / 0), so a valid hand-deployed env may omit
        # them — mirror those defaults rather than refusing to adopt. The broker host +
        # credentials ARE required by the agent, so a missing one (KeyError) correctly
        # blocks adoption (a half-configured panel isn't safe to adopt).
        port = int(env.get(panel_ops.ENV_MQTT_PORT, "1883"))
        if not 1 <= port <= 65535:
            raise ValueError("mqtt port out of range")
        return {
            CONF_PANEL: panel,
            CONF_MESH_PRIORITY: int(env.get(panel_ops.ENV_MESH_PRIORITY, "0")),
            CONF_MQTT_HOST: env[panel_ops.ENV_MQTT_HOST],
            CONF_MQTT_PORT: port,
            CONF_MQTT_USERNAME: env[panel_ops.ENV_MQTT_USERNAME],
            CONF_MQTT_PASSWORD: env[panel_ops.ENV_MQTT_PASSWORD],
        }
    except (KeyError, ValueError):
        return None


@dataclass(frozen=True)
class _PanelProbe:
    """What one onboarding probe of a panel found."""

    host_key: str
    config: dict[str, str] | None  # parsed env file when the agent is already installed


@asynccontextmanager
async def _panel_session(
    hass: HomeAssistant, host: str, password: str, pinned_key: str | None
) -> AsyncIterator[PanelShell]:
    """One serialized SSH session (fleet lock held), connected and always closed.

    With `pinned_key` set the server key is verified BEFORE auth (a rotated/impostor
    host never receives the root password); `pinned_key=None` is trust-on-first-use.
    """
    async with _fleet_lock(hass):
        shell = AsyncsshShell(host, password, pinned_key)
        try:
            await shell.connect()
            yield shell
        finally:
            await shell.close()


async def _probe_panel(hass: HomeAssistant, host: str, password: str) -> _PanelProbe:
    """Connect (TOFU), capture the host key, and read the agent's config if installed.

    "Installed" requires BOTH the systemd unit AND the env file — the unit is what
    actually runs the agent, so a lone leftover env file (no unit) is NOT mistaken
    for a running agent; it falls through to the normal not-installed setup path.
    """
    async with _panel_session(hass, host, password, None) as shell:
        key = shell.pinned_host_key()
        if key is None:
            raise OSError("no host key captured")
        state = await panel_ops.inspect_panel(shell)
        installed = state.unit_present and state.env_present
        config = await panel_ops.read_env(shell) if installed else None
        return _PanelProbe(host_key=key, config=config)


async def _apply_config(
    hass: HomeAssistant,
    host: str,
    password: str,
    *,
    pinned_key: str | None,
    env_content: str,
    expected_panel: str,
) -> str:
    """Verify/capture the host key; if the agent is installed, push env + restart.

    Returns the (pinned/verified) host key. A not-yet-installed panel skips the push —
    the entry update still lands and the next deploy renders the new values.

    Before overwriting, it refuses to clobber a DIFFERENT panel: if the box already
    runs an agent whose env names another slug than *expected_panel* (e.g. the host
    was mistyped to another controller in the fleet), it raises _WrongPanelError
    instead of stamping this entry's identity onto that panel and restarting it.
    """
    async with _panel_session(hass, host, password, pinned_key) as shell:
        key = shell.pinned_host_key()
        if key is None:
            raise OSError("no host key captured")
        state = await panel_ops.inspect_panel(shell)
        if state.unit_present:
            if state.env_present:
                found = (await panel_ops.read_env(shell)).get(panel_ops.ENV_PANEL)
                if found and found != expected_panel:
                    raise _WrongPanelError(found)
            await panel_ops.write_env(shell, env_content)
            await panel_ops.restart(shell)
        return key


class BrilliantMqttConfigFlow(ConfigFlow, domain=DOMAIN):
    """Add one Brilliant panel per entry (detection-first; adopts installed agents)."""

    VERSION = 2

    def __init__(self) -> None:
        # Carried across the not-installed onboarding steps (user → mqtt → script).
        self._connect: dict[str, Any] = {}
        self._mqtt: dict[str, Any] = {}

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: ConfigEntry) -> OptionsFlow:
        return BrilliantMqttOptionsFlow()

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Step 1 — connect (IP + root password, the only required inputs).

        Adopts the panel outright if the agent is already installed; otherwise
        continues to the MQTT broker step.
        """
        errors: dict[str, str] = {}
        if user_input is not None:
            # Reject control chars on the RAW host first (a leading/trailing \n/\r/\t must
            # fail, not be silently stripped), THEN drop benign surrounding whitespace so a
            # stray space can't store a dirty value or, on a later reconfigure, read as a
            # "different" host and silently re-TOFU.
            errors = _control_char_errors(user_input, (CONF_HOST, CONF_ROOT_PASSWORD))
            if not errors:
                user_input = {**user_input, CONF_HOST: user_input[CONF_HOST].strip()}
                try:
                    probe = await _probe_panel(
                        self.hass, user_input[CONF_HOST], user_input[CONF_ROOT_PASSWORD]
                    )
                except (OSError, asyncssh.Error):
                    errors["base"] = "cannot_connect"
                else:
                    self._connect = {
                        CONF_HOST: user_input[CONF_HOST],
                        CONF_ROOT_PASSWORD: user_input[CONF_ROOT_PASSWORD],
                        DATA_SSH_HOST_KEY: probe.host_key,
                    }
                    if probe.config is None:
                        return await self.async_step_broker()
                    adopted = _adopt_data(probe.config)
                    if adopted is None:
                        errors["base"] = "cannot_read_config"
                    else:
                        await self.async_set_unique_id(adopted[CONF_PANEL])
                        self._abort_if_unique_id_configured()
                        return self.async_create_entry(
                            title=f"Brilliant {adopted[CONF_PANEL]}",
                            data={**self._connect, **adopted},
                        )
        schema = vol.Schema(
            {
                vol.Required(
                    CONF_HOST, default=(user_input or {}).get(CONF_HOST, vol.UNDEFINED)
                ): str,
                vol.Required(CONF_ROOT_PASSWORD): str,
            }
        )
        return self.async_show_form(step_id="user", data_schema=schema, errors=errors)

    async def async_step_broker(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Step 2 — MQTT broker the on-panel agent connects to (pre-filled from a prior panel)."""
        errors: dict[str, str] = {}
        if user_input is not None:
            errors = _control_char_errors(
                user_input, (CONF_MQTT_HOST, CONF_MQTT_USERNAME, CONF_MQTT_PASSWORD)
            )
            if not errors:
                self._mqtt = dict(user_input)
                return await self.async_step_script()
        defaults: dict[str, Any] = {}
        if entries := self._async_current_entries():
            prior = entries[-1].data
            defaults = {k: prior[k] for k in _PREFILL_KEYS if k in prior}
        schema = vol.Schema(_mqtt_schema_fields(defaults))
        # On an error redisplay, show what the operator just typed (not the prior-panel
        # prefill); the prefill is only the first-time default.
        if user_input is not None:
            schema = self.add_suggested_values_to_schema(schema, user_input)
        return self.async_show_form(step_id="broker", data_schema=schema, errors=errors)

    async def async_step_script(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        """Step 3 — name + mesh + voice opt-in, then INSTALL the agent (not-yet-installed only).

        Installing here — push the payload, write the unit/env, enable the service — is
        what makes "add the integration" actually set the panel up. The host key was
        pinned in step 1. An agent install failure keeps this form open with cannot_install;
        a voice install failure (after the agent succeeded) uses cannot_install_voice — both
        leave the entry uncreated so the operator can fix and retry. The agent install is
        idempotent so a retry after a voice failure only re-runs the voice step.
        """
        errors: dict[str, str] = {}
        if user_input is not None:
            slug = _slugify(user_input[CONF_NAME])
            if slug == MESH_PANEL:
                errors[CONF_NAME] = "reserved_panel"
            elif not slug:
                errors[CONF_NAME] = "invalid_name"
            # A control char in the HA host flows into render_voice_env → _env_quote, which
            # raises ValueError (NOT caught by the voice except below) → the flow would
            # crash. Reject at the boundary for a friendly per-field message instead.
            errors.update(_control_char_errors(user_input, (CONF_VOICE_HA_HOST,)))
            if not errors:
                await self.async_set_unique_id(slug)
                self._abort_if_unique_id_configured()
                components: dict[str, bool] = {COMPONENT_BRIDGE: True}
                for c in optional():
                    components[c.id] = bool(user_input.get(c.id, False))
                entry_data: dict[str, Any] = {
                    **self._connect,
                    **self._mqtt,
                    CONF_PANEL: slug,
                    CONF_MESH_PRIORITY: user_input[CONF_MESH_PRIORITY],
                    CONF_COMPONENTS: components,
                    # Transitional backward-compat key so existing readers are unbroken.
                    CONF_VOICE_ENABLED: components.get(COMPONENT_VOICE, False),
                    CONF_VOICE_WAKE_WORD: user_input[CONF_VOICE_WAKE_WORD],
                    CONF_VOICE_HA_HOST: user_input[CONF_VOICE_HA_HOST],
                }
                current_cid: str | None = None
                try:
                    for cid, selected in components.items():
                        if not selected:
                            continue
                        current_cid = cid
                        async with _panel_session(
                            self.hass,
                            self._connect[CONF_HOST],
                            self._connect[CONF_ROOT_PASSWORD],
                            self._connect[DATA_SSH_HOST_KEY],
                        ) as shell:
                            await REGISTRY[cid].install(self.hass, shell, entry_data)
                except VoicePayloadError:
                    errors["base"] = "cannot_install_voice"
                except (OSError, asyncssh.Error, panel_ops.PanelOpError):
                    errors["base"] = (
                        "cannot_install_voice"
                        if current_cid == COMPONENT_VOICE
                        else "cannot_install"
                    )
                else:
                    return self.async_create_entry(title=f"Brilliant {slug}", data=entry_data)
        schema = vol.Schema(
            {
                vol.Required(CONF_NAME): str,
                vol.Required(CONF_MESH_PRIORITY, default=0): vol.All(
                    vol.Coerce(int), vol.Range(min=0, max=99)
                ),
                **_components_schema_fields({}),
            }
        )
        # Preserve name + mesh + voice fields across an error redisplay.
        if user_input is not None:
            schema = self.add_suggested_values_to_schema(schema, user_input)
        return self.async_show_form(step_id="script", data_schema=schema, errors=errors)

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Edit host/password/broker/mesh for one panel and push it (slug is immutable)."""
        entry = self._get_reconfigure_entry()
        errors: dict[str, str] = {}
        if user_input is not None:
            # Reject control chars on the RAW input first, THEN strip benign surrounding
            # whitespace — otherwise a stray trailing space would read as a "different"
            # host and downgrade the same-host pinned check to a fresh TOFU.
            errors = _control_char_errors(user_input, _NO_CONTROL_CHARS)
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
                    )
                    try:
                        host_key = await _apply_config(
                            self.hass,
                            user_input[CONF_HOST],
                            user_input[CONF_ROOT_PASSWORD],
                            pinned_key=pinned_key,
                            env_content=env,
                            expected_panel=entry.data[CONF_PANEL],
                        )
                    except _WrongPanelError:
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
                        return self.async_update_reload_and_abort(
                            entry,
                            data={**entry.data, **user_input, DATA_SSH_HOST_KEY: host_key},
                        )
        data = entry.data
        schema = vol.Schema(
            {
                vol.Required(CONF_HOST, default=data[CONF_HOST]): str,
                vol.Required(CONF_ROOT_PASSWORD): str,
                **_mqtt_schema_fields(data),
                vol.Required(CONF_MESH_PRIORITY, default=data.get(CONF_MESH_PRIORITY, 0)): vol.All(
                    vol.Coerce(int), vol.Range(min=0, max=99)
                ),
            }
        )
        # Keep the operator's just-made edits across an error redisplay (a transient
        # cannot_connect / wrong_panel shouldn't wipe all six fields back to the old config).
        if user_input is not None:
            schema = self.add_suggested_values_to_schema(schema, user_input)
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
                vol.Required(
                    OPT_TRUST_HOST_KEY_CHANGES,
                    default=opts.get(OPT_TRUST_HOST_KEY_CHANGES, DEFAULT_TRUST_HOST_KEY_CHANGES),
                ): bool,
            }
        )
        return self.async_show_form(step_id="init", data_schema=schema)
