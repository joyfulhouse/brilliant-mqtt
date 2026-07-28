"""Fleet-first MQTT validation and crash-safe Brilliant panel onboarding."""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import math
import re
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Protocol, cast

import asyncssh
import voluptuous as vol
from homeassistant.config_entries import (
    ConfigEntry,
    ConfigFlow,
    ConfigFlowResult,
    ConfigSubentry,
    ConfigSubentryFlow,
    FlowType,
    OptionsFlow,
    SubentryFlowResult,
    UnknownEntry,
)
from homeassistant.const import CONF_NAME
from homeassistant.core import HomeAssistant, callback
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.selector import (
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)

from . import _fleet_lock, panel_ops
from .broker import BrokerKind, BrokerProfile
from .broker_validation import BrokerValidator
from .components import REGISTRY, optional
from .const import (
    COMPONENT_BRIDGE,
    COMPONENT_BUS_WATCHDOG,
    COMPONENT_HA_MIRROR,
    COMPONENT_WIFI_WATCHDOG,
    CONF_COMPONENTS,
    CONF_ENTRY_KIND,
    CONF_FEATURE_OVERRIDES,
    CONF_HA_CONTROL_DOMAINS,
    CONF_HA_CONTROL_ENABLED,
    CONF_HA_CONTROL_LABEL,
    CONF_HOST,
    CONF_HUE_CA_CERT,
    CONF_IDENTITY_FINGERPRINT,
    CONF_MANAGEMENT_ID,
    CONF_MAX_MIRRORED_ENTITIES,
    CONF_MESH_PRIORITY,
    CONF_MQTT_HOST,
    CONF_MQTT_PASSWORD,
    CONF_MQTT_PORT,
    CONF_MQTT_USERNAME,
    CONF_NEXT_MESH_PRIORITY,
    CONF_PANEL,
    CONF_PROVISIONING_TRANSACTION_ID,
    CONF_ROOM_OVERRIDES,
    CONF_ROOT_PASSWORD,
    CONF_SCENE_ACTIONS,
    CONF_SCENE_PANEL,
    CONF_SCHEMA_VERSION,
    CONF_SSH_HOST_KEY,
    CONF_SSH_USERNAME,
    CONF_VOICE_HA_HOST,
    CONF_VOICE_WAKE_WORD,
    CONFIG_ENTRY_VERSION,
    DATA_SSH_HOST_KEY,
    DEFAULT_AUTO_REPAIR,
    DEFAULT_HA_CONTROL_DOMAINS,
    DEFAULT_HA_CONTROL_ENABLED,
    DEFAULT_HA_CONTROL_LABEL,
    DEFAULT_MAX_MIRRORED_ENTITIES,
    DEFAULT_OFFLINE_GRACE_MINUTES,
    DEFAULT_REPAIR_COOLDOWN_MINUTES,
    DEFAULT_TRUST_HOST_KEY_CHANGES,
    DEFAULT_VOICE_WAKE_WORD,
    DOMAIN,
    ENTRY_KIND_FLEET,
    FLEET_SCENE_OWNER_UNASSIGNED,
    FLEET_UNIQUE_ID,
    HA_CONTROL_DOMAINS,
    OPT_AUTO_REPAIR,
    OPT_OFFLINE_GRACE_MINUTES,
    OPT_REPAIR_COOLDOWN_MINUTES,
    OPT_TRUST_HOST_KEY_CHANGES,
    SUBENTRY_TYPE_PANEL,
    VOICE_WAKE_WORDS,
)
from .entry_data import EntryDataError, FleetConfig
from .errors import OperationError
from .flow_schemas import (
    BROKER_MENU_OPTIONS,
    FlowInputError,
    allocate_mesh_priority,
    allocate_panel_slug,
    broker_schema,
    normalize_broker_input,
    normalize_panel_connect_input,
    normalize_panel_name,
    panel_confirm_schema,
    panel_connect_schema,
)
from .panel_inspection import (
    PanelCompatibilityError,
    PanelFacts,
    async_inspect_panel,
)
from .panel_provisioner import (
    PanelInstallRequest,
    PanelProvisioner,
    PanelProvisioningError,
    ProvisionedPanel,
    ProvisioningProgress,
    ProvisioningProgressStage,
)
from .shell import (
    AsyncsshShell as FleetAsyncsshShell,
)
from .shell import (
    HostIdentity,
    PanelIdentityError,
    PanelShell,
    _LegacyAsyncsshShell,
    async_fetch_host_identity,
)
from .voice_payload import VoicePayloadError

AsyncsshShell = _LegacyAsyncsshShell
_LOGGER = logging.getLogger(__name__)
_FLEET_STORAGE_ISSUE_REASON = (
    "Home Assistant could not confirm the Brilliant MQTT fleet in durable storage. "
    "Retry Add panel after storage is available."
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
    CONF_HA_CONTROL_LABEL,
)


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
            CONF_MQTT_PASSWORD,
        ): TextSelector(TextSelectorConfig(type=TextSelectorType.PASSWORD)),
    }


def _components_schema_fields(
    source: Mapping[str, Any], *, new_install: bool = True
) -> dict[Any, Any]:
    """One checkbox per OPTIONAL component (bridge is implicit/locked), plus voice sub-fields.

    *new_install* controls the fallback default for keys absent from the entry's
    CONF_COMPONENTS dict:

    - ``True`` (new installs / script step): fall back to ``c.default_enabled`` so
      default-on components (e.g. wifi_watchdog) render pre-checked on first setup.
    - ``False`` (existing panels / reconfigure step): fall back to ``False`` so a
      panel that was onboarded before the component existed does NOT accidentally get
      it installed on a no-change reconfigure Save.
    """
    chosen: Mapping[str, Any] = source.get(CONF_COMPONENTS, {})
    fields: dict[Any, Any] = {}
    for c in optional():
        default = chosen.get(c.id, c.default_enabled if new_install else False)
        fields[vol.Required(c.id, default=default)] = bool
    # Voice sub-config (meaningful only when voice is checked; validated leniently).
    fields[
        vol.Required(
            CONF_VOICE_WAKE_WORD,
            default=source.get(CONF_VOICE_WAKE_WORD, DEFAULT_VOICE_WAKE_WORD),
        )
    ] = vol.In(list(VOICE_WAKE_WORDS))
    fields[vol.Optional(CONF_VOICE_HA_HOST, default=source.get(CONF_VOICE_HA_HOST, ""))] = str
    # Hue CA-recovery sub-config (meaningful only when hue_ca is checked).
    fields[vol.Optional(CONF_HUE_CA_CERT, default=source.get(CONF_HUE_CA_CERT, ""))] = TextSelector(
        TextSelectorConfig(multiline=True)
    )
    return fields


_GLOBAL_KEYS = (
    CONF_HA_CONTROL_ENABLED,
    CONF_HA_CONTROL_LABEL,
    CONF_ROOM_OVERRIDES,
    CONF_HA_CONTROL_DOMAINS,
    CONF_MAX_MIRRORED_ENTITIES,
    CONF_SCENE_PANEL,
    CONF_SCENE_ACTIONS,
)
_MAX_JSON_TEXT = 64 * 1024
_MAX_JSON_NODES = 2_048
_MAX_JSON_DEPTH = 12
_MAX_STRING_LENGTH = 4_096
_MAX_ROOM_OVERRIDES = 200
_MAX_SCENE_ACTIONS = 1_024
_SERVICE_PATTERN = re.compile(r"[a-z0-9_]+")
_TARGET_KEYS = frozenset({"entity_id", "device_id", "area_id"})


class _RawMultiSelect(cv.multi_select):
    """Expose a serializable multi-select while deferring trust-boundary validation."""

    def __call__(self, selected: Any) -> Any:
        return selected


class _RawInteger(vol.Coerce):
    """Expose an integer field without coercing booleans or strings before validation."""

    def __init__(self) -> None:
        super().__init__(int)

    def __call__(self, value: Any) -> Any:
        return value


class _RawRange(vol.Range):
    """Expose numeric bounds while leaving the raw value for strict validation."""

    def __call__(self, value: Any) -> Any:
        return value


def _canonical_json(value: Mapping[str, object]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _safe_control_redisplay_values(user_input: Mapping[str, Any]) -> dict[str, Any]:
    """Keep ordinary edits, but never echo credentials or unsafe JSON text."""
    values = dict(user_input)
    values.pop(CONF_ROOT_PASSWORD, None)
    values.pop(CONF_MQTT_PASSWORD, None)
    for key in (CONF_ROOM_OVERRIDES, CONF_SCENE_ACTIONS):
        raw = values.get(key)
        if not isinstance(raw, str) or len(raw) > _MAX_JSON_TEXT or _has_control_char(raw):
            values[key] = "{}"
    return values


def _control_schema_fields(source: Mapping[str, Any], *, panel_default: str) -> dict[Any, Any]:
    overrides = source.get(CONF_ROOM_OVERRIDES, {})
    actions = source.get(CONF_SCENE_ACTIONS, {})
    return {
        vol.Required(
            CONF_HA_CONTROL_ENABLED,
            default=source.get(CONF_HA_CONTROL_ENABLED, DEFAULT_HA_CONTROL_ENABLED),
        ): bool,
        vol.Required(
            CONF_HA_CONTROL_LABEL,
            default=source.get(CONF_HA_CONTROL_LABEL, DEFAULT_HA_CONTROL_LABEL),
        ): str,
        vol.Required(
            CONF_ROOM_OVERRIDES,
            default=_canonical_json(overrides) if isinstance(overrides, Mapping) else "{}",
        ): str,
        vol.Required(
            CONF_HA_CONTROL_DOMAINS,
            default=list(source.get(CONF_HA_CONTROL_DOMAINS, DEFAULT_HA_CONTROL_DOMAINS)),
        ): _RawMultiSelect({domain: domain for domain in HA_CONTROL_DOMAINS}),
        vol.Required(
            CONF_MAX_MIRRORED_ENTITIES,
            default=source.get(CONF_MAX_MIRRORED_ENTITIES, DEFAULT_MAX_MIRRORED_ENTITIES),
        ): vol.All(_RawInteger(), _RawRange(min=1, max=200)),
        vol.Required(
            CONF_SCENE_PANEL,
            default=source.get(CONF_SCENE_PANEL, panel_default),
        ): str,
        vol.Required(
            CONF_SCENE_ACTIONS,
            default=_canonical_json(actions) if isinstance(actions, Mapping) else "{}",
        ): str,
    }


def _validate_json_value(value: object, *, depth: int, remaining: list[int]) -> None:
    remaining[0] -= 1
    if remaining[0] < 0 or depth > _MAX_JSON_DEPTH:
        raise ValueError
    if value is None or type(value) in (bool, int):
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError
        return
    if isinstance(value, str):
        if len(value) > _MAX_STRING_LENGTH or _has_control_char(value):
            raise ValueError
        return
    if isinstance(value, list):
        for item in value:
            _validate_json_value(item, depth=depth + 1, remaining=remaining)
        return
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        for key, item in value.items():
            if len(key) > _MAX_STRING_LENGTH or _has_control_char(key):
                raise ValueError
            _validate_json_value(item, depth=depth + 1, remaining=remaining)
        return
    raise ValueError


def _decode_json_object(raw: object) -> dict[str, object]:
    if not isinstance(raw, str) or len(raw) > _MAX_JSON_TEXT:
        raise ValueError
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, TypeError) as error:
        raise ValueError from error
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValueError
    _validate_json_value(value, depth=0, remaining=[_MAX_JSON_NODES])
    return value


def _decode_room_overrides(raw: object) -> dict[str, str]:
    value = _decode_json_object(raw)
    if len(value) > _MAX_ROOM_OVERRIDES:
        raise ValueError
    decoded: dict[str, str] = {}
    for key, room in value.items():
        if (
            not key.strip()
            or len(key) > 256
            or not isinstance(room, str)
            or not room.strip()
            or len(room) > 256
        ):
            raise ValueError
        decoded[key.strip()] = room.strip()
    return decoded


def _decode_scene_actions(raw: object, panels: frozenset[str]) -> dict[str, object]:
    value = _decode_json_object(raw)
    if len(value) > _MAX_SCENE_ACTIONS:
        raise ValueError
    decoded: dict[str, object] = {}
    for key, raw_action in value.items():
        if key.count(":") != 1:
            raise ValueError
        panel, scene_id = key.split(":")
        if panel not in panels or not scene_id or len(scene_id) > _MAX_STRING_LENGTH:
            raise ValueError
        if not isinstance(raw_action, dict) or set(raw_action) != {
            "domain",
            "service",
            "target",
            "data",
        }:
            raise ValueError
        domain = raw_action["domain"]
        service = raw_action["service"]
        target = raw_action["target"]
        data = raw_action["data"]
        if (
            not isinstance(domain, str)
            or _SERVICE_PATTERN.fullmatch(domain) is None
            or not isinstance(service, str)
            or _SERVICE_PATTERN.fullmatch(service) is None
            or not isinstance(target, dict)
            or not set(target).issubset(_TARGET_KEYS)
            or not isinstance(data, dict)
        ):
            raise ValueError
        decoded[key] = copy.deepcopy(raw_action)
    return decoded


def _validated_control_input(
    user_input: Mapping[str, Any], *, panels: frozenset[str], default_panel: str
) -> tuple[dict[str, str], dict[str, Any]]:
    errors: dict[str, str] = {}
    values: dict[str, Any] = {}
    label = str(user_input.get(CONF_HA_CONTROL_LABEL, "")).strip()
    if not label or len(label) > 256 or _has_control_char(label):
        errors[CONF_HA_CONTROL_LABEL] = "invalid_value"
    else:
        values[CONF_HA_CONTROL_LABEL] = label
    try:
        values[CONF_ROOM_OVERRIDES] = _decode_room_overrides(user_input.get(CONF_ROOM_OVERRIDES))
    except ValueError:
        errors[CONF_ROOM_OVERRIDES] = "invalid_value"

    raw_domains = user_input.get(CONF_HA_CONTROL_DOMAINS)
    if (
        not isinstance(raw_domains, list)
        or any(
            not isinstance(domain, str) or domain not in HA_CONTROL_DOMAINS
            for domain in raw_domains
        )
        or len(raw_domains) != len(set(raw_domains))
    ):
        errors[CONF_HA_CONTROL_DOMAINS] = "invalid_value"
    else:
        values[CONF_HA_CONTROL_DOMAINS] = [
            domain for domain in HA_CONTROL_DOMAINS if domain in raw_domains
        ]

    maximum = user_input.get(CONF_MAX_MIRRORED_ENTITIES)
    if type(maximum) is not int or not 1 <= maximum <= 200:
        errors[CONF_MAX_MIRRORED_ENTITIES] = "invalid_value"
    else:
        values[CONF_MAX_MIRRORED_ENTITIES] = maximum

    scene_panel = str(user_input.get(CONF_SCENE_PANEL, "")).strip() or default_panel
    if scene_panel not in panels:
        errors[CONF_SCENE_PANEL] = "invalid_value"
    else:
        values[CONF_SCENE_PANEL] = scene_panel
    try:
        values[CONF_SCENE_ACTIONS] = _decode_scene_actions(
            user_input.get(CONF_SCENE_ACTIONS), panels
        )
    except ValueError:
        errors[CONF_SCENE_ACTIONS] = "invalid_value"

    enabled = user_input.get(CONF_HA_CONTROL_ENABLED)
    if type(enabled) is not bool:
        errors[CONF_HA_CONTROL_ENABLED] = "invalid_value"
    else:
        values[CONF_HA_CONTROL_ENABLED] = enabled
    return errors, values


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


type _OnboardingResult = ConfigFlowResult | SubentryFlowResult

_CORE_COMPONENTS = (
    COMPONENT_BRIDGE,
    COMPONENT_WIFI_WATCHDOG,
    COMPONENT_BUS_WATCHDOG,
)
_PROVISIONING_STAGE_ORDER = tuple(ProvisioningProgressStage)


@dataclass(frozen=True, slots=True)
class _FlowFailure:
    """Allowlisted UI failure state which cannot retain a raw exception."""

    code: str
    documentation_slug: str
    stage: str

    def placeholders(self) -> dict[str, str]:
        return {
            "documentation_slug": self.documentation_slug,
            "stage": self.stage,
        }


@dataclass(frozen=True, slots=True)
class _DuplicatePanel:
    subentry_id: str
    title: str


@dataclass(frozen=True, slots=True)
class _OnboardingAbort:
    reason: str
    description_placeholders: Mapping[str, str] | None = None


class _FlowSurface(Protocol):
    """Common Home Assistant flow methods used by panel onboarding."""

    hass: HomeAssistant
    context: Mapping[str, Any]

    def async_show_form(
        self,
        *,
        step_id: str | None = None,
        data_schema: vol.Schema | None = None,
        errors: dict[str, str] | None = None,
        description_placeholders: Mapping[str, str] | None = None,
    ) -> _OnboardingResult: ...

    def async_show_progress(
        self,
        *,
        step_id: str | None = None,
        progress_action: str,
        description_placeholders: Mapping[str, str] | None = None,
        progress_task: asyncio.Task[Any] | None = None,
    ) -> _OnboardingResult: ...

    def async_show_progress_done(
        self,
        *,
        next_step_id: str,
    ) -> _OnboardingResult: ...

    def async_abort(
        self,
        *,
        reason: str,
        description_placeholders: Mapping[str, str] | None = None,
    ) -> _OnboardingResult: ...

    def async_update_progress(self, progress: float) -> None: ...


def _broker_validator(hass: HomeAssistant) -> BrokerValidator:
    """Build the behavioral validator without exporting broker secrets."""
    return BrokerValidator(
        hass,
        lambda profile, client_id: profile.device_client(client_id),
    )


def _get_panel_provisioner(
    hass: HomeAssistant,
    *,
    expected_identity: HostIdentity,
) -> PanelProvisioner:
    """Resolve the production provisioner lazily to avoid package import cycles."""
    from . import get_panel_provisioner

    return get_panel_provisioner(
        hass,
        expected_identity=expected_identity,
    )


async def _async_wait_config_entry_persisted(
    hass: HomeAssistant,
    entry: ConfigEntry[Any],
    *,
    subentry_id: str | None = None,
) -> None:
    """Resolve the durability gate lazily to avoid package import cycles."""
    from .fleet_manager import async_wait_config_entry_persisted

    await async_wait_config_entry_persisted(
        hass,
        entry,
        subentry_id=subentry_id,
    )


async def _async_verify_staged_progress(
    hass: HomeAssistant,
    update: ProvisioningProgress,
) -> None:
    """Accept a flow ownership marker only after its exact STAGED journal exists."""
    from .provisioning_journal import ProvisioningJournal, ProvisioningPhase

    record = await ProvisioningJournal(hass).async_load()
    if (
        record is None
        or record.phase is not ProvisioningPhase.STAGED
        or record.transaction_id != update.transaction_id
        or record.setup_id != update.setup_id
    ):
        raise ValueError("invalid_provisioning_progress")


def _fleet_storage_issue_id(entry_id: str) -> str:
    return f"fleet_storage_{entry_id}"


@callback
def _async_create_fleet_storage_issue(
    hass: HomeAssistant,
    entry_id: str,
) -> None:
    ir.async_create_issue(
        hass,
        DOMAIN,
        _fleet_storage_issue_id(entry_id),
        is_fixable=False,
        severity=ir.IssueSeverity.ERROR,
        translation_key="needs_attention",
        translation_placeholders={
            "panel": "Brilliant MQTT fleet",
            "reason": _FLEET_STORAGE_ISSUE_REASON,
        },
        learn_more_url=(
            "https://github.com/joyfulhouse/brilliant-mqtt/blob/main/docs/ha-integration.md"
        ),
    )


def _operation_failure(error: OperationError) -> _FlowFailure:
    return _FlowFailure(
        code=error.code,
        documentation_slug=error.documentation_slug,
        stage=error.stage.value,
    )


def _panel_failure(code: str, *, stage: str = "panel") -> _FlowFailure:
    return _FlowFailure(
        code=code,
        documentation_slug=f"panel-{code.replace('_', '-')}",
        stage=stage,
    )


def _provisioning_failure(error: PanelProvisioningError) -> _FlowFailure:
    if error.detail is not None:
        return _FlowFailure(
            code=error.detail.code,
            documentation_slug=error.detail.documentation_slug,
            stage=error.detail.stage.value,
        )
    return _panel_failure(error.code, stage="provisioning")


def _panel_subentries(entry: ConfigEntry[Any] | None) -> tuple[ConfigSubentry, ...]:
    if entry is None:
        return ()
    return tuple(
        subentry
        for subentry in entry.subentries.values()
        if subentry.subentry_type == SUBENTRY_TYPE_PANEL
    )


def _is_exact_fleet_parent(entry: ConfigEntry[Any]) -> bool:
    """Accept Add panel only for the singleton versioned fleet entry."""
    return (
        entry.domain == DOMAIN
        and entry.unique_id == FLEET_UNIQUE_ID
        and entry.version == CONFIG_ENTRY_VERSION
        and entry.data.get(CONF_ENTRY_KIND) == ENTRY_KIND_FLEET
        and entry.data.get(CONF_SCHEMA_VERSION) == CONFIG_ENTRY_VERSION
    )


def _duplicate_panel(
    entry: ConfigEntry[Any] | None,
    fingerprint: str,
) -> _DuplicatePanel | None:
    for subentry in _panel_subentries(entry):
        if subentry.unique_id == fingerprint or (
            subentry.data.get(CONF_IDENTITY_FINGERPRINT) == fingerprint
        ):
            return _DuplicatePanel(
                subentry_id=subentry.subentry_id,
                title=subentry.title,
            )
    return None


def _suggested_panel_name(facts: PanelFacts) -> str:
    suggested = facts.hostname.replace("-", " ").replace("_", " ").strip().title()
    return suggested or "Brilliant Panel"


def _facts_placeholders(facts: PanelFacts) -> dict[str, str]:
    return {
        "fingerprint": facts.fingerprint,
        "hostname": facts.hostname,
        "model": facts.model,
        "architecture": facts.architecture,
        "firmware": facts.firmware,
        "python_version": facts.python_version,
        "init_system": facts.init_system,
        "available_bytes": str(facts.available_bytes),
        "available_memory_bytes": str(facts.available_memory_bytes),
        "installed_agent_version": facts.installed_agent_version or "not_installed",
        "active_services": ", ".join(facts.active_services) or "none",
        "conflicting_services": ", ".join(facts.conflicting_services) or "none",
    }


async def _async_close_inspection_shell(shell: PanelShell) -> None:
    """Drain SSH close under caller cancellation, then preserve cancellation."""
    close_task = asyncio.create_task(
        shell.close(),
        name="brilliant-mqtt-inspection-shell-close",
    )
    try:
        await asyncio.shield(close_task)
    except asyncio.CancelledError as cancellation:
        while not close_task.done():
            try:
                await asyncio.shield(close_task)
            except asyncio.CancelledError:
                continue
        try:
            close_task.result()
        except BaseException:
            pass
        raise cancellation


async def _async_inspect_candidate(
    hass: HomeAssistant,
    host: str,
    root_password: str,
    identity: HostIdentity,
) -> PanelFacts:
    """Authenticate only to the previously fetched identity and inspect read-only."""
    async with _fleet_lock(hass):
        shell = FleetAsyncsshShell(host, root_password, identity.public_key)
        primary: BaseException | None = None
        try:
            await shell.connect()
            return await async_inspect_panel(shell, identity)
        except BaseException as error:
            primary = error
            raise
        finally:
            try:
                await _async_close_inspection_shell(shell)
            except asyncio.CancelledError:
                raise
            except BaseException:
                if primary is None:
                    raise PanelCompatibilityError("inspection_failed") from None


def _provisioned_matches_request(
    provisioned: ProvisionedPanel,
    request: PanelInstallRequest,
    identity: HostIdentity,
) -> bool:
    """Defend the storage handoff against a mismatched provisioner result."""
    data = provisioned.panel_data
    expected_components = {
        component: component in request.selected_components for component in _CORE_COMPONENTS
    }
    return (
        provisioned.identity == identity
        and provisioned.facts.fingerprint == identity.fingerprint
        and data.get(CONF_IDENTITY_FINGERPRINT) == identity.fingerprint
        and data.get(CONF_SSH_HOST_KEY) == identity.public_key
        and data.get(CONF_HOST) == request.host
        and data.get(CONF_SSH_USERNAME) == request.ssh_username
        and data.get(CONF_ROOT_PASSWORD) == request.root_password
        and data.get(CONF_NAME) == request.display_name
        and data.get(CONF_PANEL) == request.slug
        and data.get(CONF_MANAGEMENT_ID) == identity.fingerprint
        and data.get(CONF_COMPONENTS) == expected_components
        and data.get(CONF_FEATURE_OVERRIDES) == dict(request.feature_overrides)
        and data.get(CONF_MESH_PRIORITY) == request.mesh_priority
        and data.get(CONF_PROVISIONING_TRANSACTION_ID) == str(provisioned.transaction_id)
    )


class _PanelOnboardingMixin:
    """Shared panel connect/confirm/progress state for initial and Add panel."""

    _panel_host: str | None
    _panel_root_password: str | None
    _panel_ssh_username: str
    _panel_identity: HostIdentity | None
    _panel_facts: PanelFacts | None
    _panel_name: str | None
    _panel_slug: str | None
    _panel_priority: int | None
    _panel_failure: _FlowFailure | None
    _panel_source: dict[str, object]
    _provision_task: asyncio.Task[ProvisionedPanel] | None
    _provisioned: ProvisionedPanel | None
    _provisioner: PanelProvisioner | None
    _install_request: PanelInstallRequest | None
    _provision_fleet: FleetConfig | None
    _recovery_task: asyncio.Task[bool] | None
    _onboarding_committed: bool

    def _init_panel_onboarding(self) -> None:
        self._panel_host = None
        self._panel_root_password = None
        self._panel_ssh_username = "root"
        self._panel_identity = None
        self._panel_facts = None
        self._panel_name = None
        self._panel_slug = None
        self._panel_priority = None
        self._panel_failure = None
        self._panel_source = {}
        self._provision_task = None
        self._provisioned = None
        self._provisioner = None
        self._install_request = None
        self._provision_fleet = None
        self._recovery_task = None
        self._onboarding_committed = False

    def _onboarding_parent_entry(self) -> ConfigEntry[Any] | None:
        raise NotImplementedError

    def _onboarding_fleet(self) -> FleetConfig:
        raise NotImplementedError

    def _onboarding_state_abort(
        self,
        *,
        before_create: bool,
    ) -> _OnboardingAbort | None:
        raise NotImplementedError

    def _finish_panel_onboarding(
        self,
        provisioned: ProvisionedPanel,
    ) -> _OnboardingResult:
        raise NotImplementedError

    def _abort_onboarding(self, abort: _OnboardingAbort) -> _OnboardingResult:
        flow = cast(_FlowSurface, self)
        cast(dict[str, Any], flow.context).pop(
            CONF_PROVISIONING_TRANSACTION_ID,
            None,
        )
        return flow.async_abort(
            reason=abort.reason,
            description_placeholders=abort.description_placeholders,
        )

    async def _async_run_recovery(self, provisioner: PanelProvisioner) -> bool:
        """Convert background recovery errors to one redacted completion state."""
        try:
            await provisioner.async_recover()
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOGGER.error("Panel onboarding recovery failed; manual recovery may be required")
            return False
        return True

    def _ensure_recovery_task(self) -> asyncio.Task[bool] | None:
        if self._recovery_task is not None:
            return self._recovery_task
        provisioner = self._provisioner
        if provisioner is None:
            return None
        flow = cast(_FlowSurface, self)
        self._recovery_task = flow.hass.async_create_task(
            self._async_run_recovery(provisioner),
            "brilliant-mqtt-panel-recovery",
        )
        return self._recovery_task

    async def _async_wait_for_recovery(self) -> bool:
        """Drain one shared recovery task and preserve caller cancellation."""
        task = self._ensure_recovery_task()
        if task is None:
            return False
        cancellation: asyncio.CancelledError | None = None
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError as error:
                if cancellation is None:
                    cancellation = error
        if cancellation is not None:
            try:
                task.result()
            except BaseException:
                pass
            raise cancellation
        return task.result()

    async def _async_compensate_onboarding_abort(
        self,
        abort: _OnboardingAbort,
    ) -> _OnboardingResult:
        """Rollback an installed-but-uncommitted panel before aborting its flow."""
        flow = cast(_FlowSurface, self)
        cast(dict[str, Any], flow.context).pop(
            CONF_PROVISIONING_TRANSACTION_ID,
            None,
        )
        if await self._async_wait_for_recovery():
            return flow.async_abort(
                reason=abort.reason,
                description_placeholders=abort.description_placeholders,
            )
        return flow.async_abort(
            reason="recovery_failed",
            description_placeholders={
                "documentation_slug": "panel-recovery-failed",
                "stage": "recovery",
            },
        )

    def _remove_panel_onboarding(self) -> None:
        """Schedule settlement when HA removes a marked flow before commit."""
        context = cast(dict[str, Any], cast(_FlowSurface, self).context)
        if (
            context.pop(CONF_PROVISIONING_TRANSACTION_ID, None) is not None
            and not self._onboarding_committed
        ):
            self._ensure_recovery_task()

    async def _async_accept_panel_connection(
        self,
        user_input: Mapping[str, object],
    ) -> tuple[dict[str, str], _FlowFailure | None, _DuplicatePanel | None]:
        try:
            normalized = normalize_panel_connect_input(user_input)
        except FlowInputError as error:
            return dict(error.errors), None, None

        host = normalized[CONF_HOST]
        root_password = normalized[CONF_ROOT_PASSWORD]
        self._panel_source = {CONF_HOST: host}
        self._panel_host = host
        self._panel_root_password = root_password
        self._panel_ssh_username = normalized[CONF_SSH_USERNAME]
        try:
            identity = await async_fetch_host_identity(host)
            duplicate = _duplicate_panel(
                self._onboarding_parent_entry(),
                identity.fingerprint,
            )
            if duplicate is not None:
                return {}, None, duplicate
            facts = await _async_inspect_candidate(
                cast(_FlowSurface, self).hass,
                host,
                root_password,
                identity,
            )
        except asyncio.CancelledError:
            raise
        except asyncssh.HostKeyNotVerifiable:
            return {}, _panel_failure("host_key_changed", stage="panel_ssh"), None
        except asyncssh.PermissionDenied:
            return (
                {},
                _panel_failure(
                    "panel_authentication_failed",
                    stage="panel_ssh",
                ),
                None,
            )
        except PanelIdentityError as error:
            return {}, _panel_failure(error.code, stage="panel_identity"), None
        except PanelCompatibilityError as error:
            return {}, _panel_failure(error.code, stage="panel_inspection"), None
        except (OSError, asyncssh.Error):
            return {}, _panel_failure("cannot_connect", stage="panel_ssh"), None
        except Exception:
            return {}, _panel_failure("inspection_failed", stage="panel_inspection"), None

        self._panel_identity = identity
        self._panel_facts = facts
        self._panel_failure = None
        return {}, None, None

    async def async_step_panel_connect(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> _OnboardingResult:
        flow = cast(_FlowSurface, self)
        errors: dict[str, str] = {}
        failure = self._panel_failure
        if user_input is not None:
            errors, failure, duplicate = await self._async_accept_panel_connection(user_input)
            if duplicate is not None:
                return flow.async_abort(
                    reason="already_configured",
                    description_placeholders={
                        "subentry_id": duplicate.subentry_id,
                        "panel_name": duplicate.title,
                    },
                )
            if not errors and failure is None:
                return await self.async_step_panel_confirm()
            self._panel_failure = failure

        return flow.async_show_form(
            step_id="panel_connect",
            data_schema=panel_connect_schema(self._panel_source),
            errors=errors or ({"base": failure.code} if failure is not None else {}),
            description_placeholders=(failure.placeholders() if failure is not None else None),
        )

    async def async_step_panel_confirm(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> _OnboardingResult:
        flow = cast(_FlowSurface, self)
        identity = self._panel_identity
        facts = self._panel_facts
        if identity is None or facts is None:
            return flow.async_abort(reason="invalid_flow_state")

        errors: dict[str, str] = {}
        if user_input is not None:
            if abort := self._onboarding_state_abort(before_create=False):
                return self._abort_onboarding(abort)
            try:
                name = normalize_panel_name(user_input)
            except FlowInputError as error:
                errors = dict(error.errors)
            else:
                self._panel_name = name
                parent = self._onboarding_parent_entry()
                if parent is None:
                    return flow.async_abort(reason="invalid_parent")
                try:
                    await _async_wait_config_entry_persisted(flow.hass, parent)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self._panel_failure = _panel_failure(
                        "config_entry_storage_unavailable",
                        stage="storage",
                    )
                else:
                    ir.async_delete_issue(
                        flow.hass,
                        DOMAIN,
                        _fleet_storage_issue_id(parent.entry_id),
                    )
                    if abort := self._onboarding_state_abort(before_create=False):
                        return self._abort_onboarding(abort)
                    try:
                        fleet = self._onboarding_fleet()
                        subentries = _panel_subentries(parent)
                        slug = allocate_panel_slug(
                            name,
                            (
                                str(subentry.data[CONF_PANEL])
                                for subentry in subentries
                                if isinstance(subentry.data.get(CONF_PANEL), str)
                            ),
                        )
                        priority = allocate_mesh_priority(
                            (
                                int(subentry.data[CONF_MESH_PRIORITY])
                                for subentry in subentries
                                if type(subentry.data.get(CONF_MESH_PRIORITY)) is int
                            ),
                        )
                    except FlowInputError as error:
                        errors = dict(error.errors)
                    else:
                        assert self._panel_host is not None
                        assert self._panel_root_password is not None
                        self._panel_slug = slug
                        self._panel_priority = priority
                        self._provision_fleet = fleet
                        self._recovery_task = None
                        self._install_request = PanelInstallRequest(
                            host=self._panel_host,
                            ssh_username=self._panel_ssh_username,
                            root_password=self._panel_root_password,
                            display_name=name,
                            slug=slug,
                            mesh_priority=priority,
                            selected_components=_CORE_COMPONENTS,
                            feature_overrides=MappingProxyType({}),
                        )
                        self._provisioner = _get_panel_provisioner(
                            flow.hass,
                            expected_identity=identity,
                        )
                        self._provisioned = None
                        self._panel_failure = None
                        self._provision_task = flow.hass.async_create_task(
                            self._async_provision(),
                            "brilliant-mqtt-panel-provision",
                        )
                        return await self.async_step_panel_provision()

        failure = self._panel_failure
        suggested_name = self._panel_name or _suggested_panel_name(facts)
        return flow.async_show_form(
            step_id="panel_confirm",
            data_schema=panel_confirm_schema(suggested_name),
            errors=errors or ({"base": failure.code} if failure is not None else {}),
            description_placeholders={
                **_facts_placeholders(facts),
                **(failure.placeholders() if failure is not None else {}),
            },
        )

    async def _async_report_provisioning_progress(
        self,
        update: ProvisioningProgress,
    ) -> None:
        if not isinstance(update, ProvisioningProgress):
            raise ValueError("invalid_provisioning_progress")
        if update.stage is ProvisioningProgressStage.STAGING:
            if update.transaction_id is None:
                raise ValueError("invalid_provisioning_progress")
            await _async_verify_staged_progress(
                cast(_FlowSurface, self).hass,
                update,
            )
            context = cast(dict[str, Any], cast(_FlowSurface, self).context)
            transaction = str(update.transaction_id)
            existing = context.get(CONF_PROVISIONING_TRANSACTION_ID)
            if existing not in {None, transaction}:
                raise ValueError("invalid_provisioning_progress")
            context[CONF_PROVISIONING_TRANSACTION_ID] = transaction
        index = _PROVISIONING_STAGE_ORDER.index(update.stage)
        cast(_FlowSurface, self).async_update_progress(
            index / max(1, len(_PROVISIONING_STAGE_ORDER) - 1)
        )

    async def _async_provision(self) -> ProvisionedPanel:
        provisioner = self._provisioner
        request = self._install_request
        identity = self._panel_identity
        fleet = self._provision_fleet
        if provisioner is None or request is None or identity is None or fleet is None:
            raise PanelProvisioningError("invalid_provisioning_dependency")
        provisioned = await provisioner.async_install(
            request,
            fleet,
            self._async_report_provisioning_progress,
        )
        context = cast(dict[str, Any], cast(_FlowSurface, self).context)
        context[CONF_PROVISIONING_TRANSACTION_ID] = str(provisioned.transaction_id)
        try:
            if not _provisioned_matches_request(provisioned, request, identity):
                raise PanelProvisioningError("invalid_provisioning_dependency")
            await provisioner.async_mark_pending_config_commit(provisioned.transaction_id)
        except asyncio.CancelledError:
            context.pop(CONF_PROVISIONING_TRANSACTION_ID, None)
            await self._async_wait_for_recovery()
            raise
        except Exception:
            context.pop(CONF_PROVISIONING_TRANSACTION_ID, None)
            if not await self._async_wait_for_recovery():
                raise PanelProvisioningError("recovery_failed") from None
            raise
        return provisioned

    async def async_step_panel_provision(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> _OnboardingResult:
        del user_input
        flow = cast(_FlowSurface, self)
        task = self._provision_task
        if task is None:
            return flow.async_abort(reason="invalid_flow_state")
        if not task.done():
            return flow.async_show_progress(
                step_id="panel_provision",
                progress_action="panel_provision",
                progress_task=task,
            )

        try:
            self._provisioned = task.result()
        except asyncio.CancelledError:
            raise
        except PanelProvisioningError as error:
            self._panel_failure = _provisioning_failure(error)
            cast(dict[str, Any], flow.context).pop(
                CONF_PROVISIONING_TRANSACTION_ID,
                None,
            )
        except Exception:
            self._panel_failure = _panel_failure(
                "provisioning_failed",
                stage="provisioning",
            )
            cast(dict[str, Any], flow.context).pop(
                CONF_PROVISIONING_TRANSACTION_ID,
                None,
            )
        finally:
            self._provision_task = None

        if self._provisioned is None:
            return flow.async_show_progress_done(next_step_id="panel_confirm")
        return flow.async_show_progress_done(next_step_id="panel_create")

    async def async_step_panel_create(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> _OnboardingResult:
        del user_input
        if self._provisioned is None:
            return cast(_FlowSurface, self).async_abort(reason="invalid_flow_state")
        if abort := self._onboarding_state_abort(before_create=True):
            return await self._async_compensate_onboarding_abort(abort)
        result = self._finish_panel_onboarding(self._provisioned)
        if result["type"] is FlowResultType.CREATE_ENTRY:
            self._onboarding_committed = True
        return result


class BrilliantMqttConfigFlow(
    ConfigFlow,
    domain=DOMAIN,
):
    """Validate MQTT and durably create the singleton fleet before panel work."""

    VERSION = CONFIG_ENTRY_VERSION

    def __init__(self) -> None:
        self._broker_kind: BrokerKind | None = None
        self._broker_values: dict[str, object] = {}
        self._broker_profile: BrokerProfile | None = None
        self._broker_failure: _FlowFailure | None = None
        self._broker_task: asyncio.Task[object] | None = None

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: ConfigEntry[Any]) -> OptionsFlow:
        del config_entry
        return BrilliantMqttOptionsFlow()

    @classmethod
    @callback
    def async_get_supported_subentry_types(
        cls,
        config_entry: ConfigEntry[Any],
    ) -> dict[str, type[ConfigSubentryFlow]]:
        del cls
        if _is_exact_fleet_parent(config_entry):
            return {SUBENTRY_TYPE_PANEL: PanelSubentryFlow}
        return {}

    def _fleet_creation_abort(self) -> _OnboardingAbort | None:
        """Recheck singleton and legacy races at the final entry boundary."""
        if not self._broker_values or self._broker_profile is None:
            return _OnboardingAbort(reason="invalid_flow_state")
        entries = self.hass.config_entries.async_entries(DOMAIN)
        fleet = next(
            (entry for entry in entries if entry.data.get(CONF_ENTRY_KIND) == ENTRY_KIND_FLEET),
            None,
        )
        if fleet is not None:
            return _OnboardingAbort(
                reason="already_configured",
                description_placeholders={"entry_id": fleet.entry_id},
            )
        if entries:
            return _OnboardingAbort(reason="legacy_migration_required")
        return None

    def _empty_fleet_data(self) -> dict[str, object]:
        """Build the only valid zero-panel bootstrap state."""
        return {
            CONF_ENTRY_KIND: ENTRY_KIND_FLEET,
            **self._broker_values,
            CONF_NEXT_MESH_PRIORITY: 1,
            CONF_HA_CONTROL_ENABLED: DEFAULT_HA_CONTROL_ENABLED,
            CONF_HA_CONTROL_LABEL: DEFAULT_HA_CONTROL_LABEL,
            CONF_ROOM_OVERRIDES: {},
            CONF_HA_CONTROL_DOMAINS: list(DEFAULT_HA_CONTROL_DOMAINS),
            CONF_MAX_MIRRORED_ENTITIES: DEFAULT_MAX_MIRRORED_ENTITIES,
            CONF_SCENE_PANEL: FLEET_SCENE_OWNER_UNASSIGNED,
            CONF_SCENE_ACTIONS: {},
            CONF_SCHEMA_VERSION: CONFIG_ENTRY_VERSION,
        }

    def _is_exact_created_empty_fleet(self, entry: object) -> bool:
        """Revalidate the inert owner produced by this exact flow instance."""
        return (
            isinstance(entry, ConfigEntry)
            and entry.domain == DOMAIN
            and entry.unique_id == FLEET_UNIQUE_ID
            and entry.version == CONFIG_ENTRY_VERSION
            and entry.data.get(CONF_SCHEMA_VERSION) == CONFIG_ENTRY_VERSION
            and entry.data.get(CONF_ENTRY_KIND) == ENTRY_KIND_FLEET
            and not entry.subentries
            and entry.data == self._empty_fleet_data()
        )

    async def async_on_create_entry(
        self,
        result: ConfigFlowResult,
    ) -> ConfigFlowResult:
        """Prove the empty owner on disk before exposing first-panel onboarding."""
        entry = result["result"]
        if not self._is_exact_created_empty_fleet(entry):
            _LOGGER.error(
                "Created fleet ownership no longer matches the validated empty state; "
                "first-panel onboarding was not started"
            )
            if isinstance(entry, ConfigEntry):
                _async_create_fleet_storage_issue(self.hass, entry.entry_id)
            return result
        try:
            await _async_wait_config_entry_persisted(self.hass, entry)
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOGGER.error(
                "Fleet storage could not be confirmed; first-panel onboarding was not started"
            )
            _async_create_fleet_storage_issue(self.hass, entry.entry_id)
            return result
        ir.async_delete_issue(
            self.hass,
            DOMAIN,
            _fleet_storage_issue_id(entry.entry_id),
        )
        next_result = await self.hass.config_entries.subentries.async_init(
            (entry.entry_id, SUBENTRY_TYPE_PANEL),
            context={"source": "user"},
        )
        if next_result["type"] is not FlowResultType.ABORT:
            result["next_flow"] = (
                FlowType.CONFIG_SUBENTRIES_FLOW,
                next_result["flow_id"],
            )
        return result

    async def async_step_user(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Choose the one broker guidance path for the singleton fleet."""
        del user_input
        await self.async_set_unique_id(FLEET_UNIQUE_ID)
        entries = self._async_current_entries()
        fleet = next(
            (entry for entry in entries if entry.data.get(CONF_ENTRY_KIND) == ENTRY_KIND_FLEET),
            None,
        )
        if fleet is not None:
            return self.async_abort(
                reason="already_configured",
                description_placeholders={"entry_id": fleet.entry_id},
            )
        if entries:
            return self.async_abort(reason="legacy_migration_required")
        return self.async_show_menu(
            step_id="user",
            menu_options=BROKER_MENU_OPTIONS,
        )

    async def async_step_official_mosquitto(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        self._broker_kind = BrokerKind.OFFICIAL_MOSQUITTO
        return await self.async_step_broker()

    async def async_step_existing_broker(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        self._broker_kind = BrokerKind.EXISTING_BROKER
        return await self.async_step_broker()

    async def async_step_broker(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Validate one normalized profile in a non-submit-capable progress task."""
        kind = self._broker_kind
        if kind is None:
            return self.async_abort(reason="invalid_flow_state")
        errors: dict[str, str] = {}
        if user_input is not None:
            try:
                values = normalize_broker_input(kind, user_input)
                profile = BrokerProfile.from_mapping(values)
            except FlowInputError as error:
                errors = dict(error.errors)
            except OperationError as error:
                self._broker_failure = _operation_failure(error)
            else:
                self._broker_values = values
                self._broker_profile = profile
                self._broker_failure = None
                self._broker_task = self.hass.async_create_task(
                    _broker_validator(self.hass).async_validate(profile),
                    "brilliant-mqtt-broker-validation",
                )
                return await self.async_step_broker_validation()

        failure = self._broker_failure
        local_ip = self.hass.config.api.local_ip if self.hass.config.api is not None else None
        return self.async_show_form(
            step_id="broker",
            data_schema=broker_schema(
                kind,
                self._broker_values,
                default_host=local_ip,
            ),
            errors=errors or ({"base": failure.code} if failure is not None else {}),
            description_placeholders=(failure.placeholders() if failure is not None else None),
        )

    async def async_step_broker_validation(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        task = self._broker_task
        if task is None:
            return self.async_abort(reason="invalid_flow_state")
        if not task.done():
            return self.async_show_progress(
                step_id="broker_validation",
                progress_action="broker_validation",
                progress_task=task,
            )
        try:
            task.result()
        except asyncio.CancelledError:
            raise
        except OperationError as error:
            self._broker_failure = _operation_failure(error)
        except Exception:
            self._broker_failure = _panel_failure(
                "broker_validation_failed",
                stage="broker_validation",
            )
        finally:
            self._broker_task = None
        if self._broker_failure is not None:
            return self.async_show_progress_done(next_step_id="broker")
        return self.async_show_progress_done(next_step_id="fleet_create")

    async def async_step_fleet_create(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Create the inert singleton owner before starting any panel flow."""
        del user_input
        if abort := self._fleet_creation_abort():
            return self.async_abort(
                reason=abort.reason,
                description_placeholders=abort.description_placeholders,
            )
        return self.async_create_entry(
            title="Brilliant MQTT",
            data=self._empty_fleet_data(),
        )

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
            errors = _control_char_errors(user_input, _NO_CONTROL_CHARS)
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
                                    async with _panel_session(
                                        self.hass,
                                        user_input[CONF_HOST],
                                        user_input[CONF_ROOT_PASSWORD],
                                        host_key,
                                    ) as shell:
                                        await REGISTRY[c.id].install(self.hass, shell, new_data)
                                elif was and not now:
                                    async with _panel_session(
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


class PanelSubentryFlow(ConfigSubentryFlow, _PanelOnboardingMixin):
    """Add one panel while inheriting the exact parent fleet configuration."""

    def __init__(self) -> None:
        self._init_panel_onboarding()

    @callback
    def async_remove(self) -> None:
        """Settle an uncommitted panel when Home Assistant removes this flow."""
        self._remove_panel_onboarding()
        super().async_remove()

    def _onboarding_parent_entry(self) -> ConfigEntry[Any] | None:
        try:
            entry = self._get_entry()
        except UnknownEntry:
            return None
        return entry if _is_exact_fleet_parent(entry) else None

    def _onboarding_fleet(self) -> FleetConfig:
        entry = self._onboarding_parent_entry()
        if entry is None:
            raise PanelProvisioningError("invalid_provisioning_dependency")
        return FleetConfig.from_entry(entry)

    def _onboarding_state_abort(
        self,
        *,
        before_create: bool,
    ) -> _OnboardingAbort | None:
        entry = self._onboarding_parent_entry()
        if entry is None:
            return _OnboardingAbort(reason="invalid_parent")
        try:
            current_fleet = FleetConfig.from_entry(entry)
        except EntryDataError:
            return _OnboardingAbort(reason="invalid_parent")

        if before_create:
            if self._provision_fleet is None:
                return _OnboardingAbort(reason="invalid_flow_state")
            if current_fleet != self._provision_fleet:
                return _OnboardingAbort(reason="parent_changed")

        identity = self._panel_identity
        if identity is not None and (duplicate := _duplicate_panel(entry, identity.fingerprint)):
            return _OnboardingAbort(
                reason="already_configured",
                description_placeholders={
                    "subentry_id": duplicate.subentry_id,
                    "panel_name": duplicate.title,
                },
            )

        if not before_create:
            return None
        request = self._install_request
        if request is None:
            return _OnboardingAbort(reason="invalid_flow_state")
        for subentry in _panel_subentries(entry):
            if subentry.data.get(CONF_PANEL) == request.slug:
                return _OnboardingAbort(
                    reason="panel_slug_conflict",
                    description_placeholders={
                        "panel": request.slug,
                        "subentry_id": subentry.subentry_id,
                    },
                )
            if subentry.data.get(CONF_MESH_PRIORITY) == request.mesh_priority:
                return _OnboardingAbort(
                    reason="mesh_priority_conflict",
                    description_placeholders={
                        "mesh_priority": str(request.mesh_priority),
                        "subentry_id": subentry.subentry_id,
                    },
                )
        return None

    def _finish_panel_onboarding(
        self,
        provisioned: ProvisionedPanel,
    ) -> SubentryFlowResult:
        return self.async_create_entry(
            title=str(provisioned.panel_data[CONF_NAME]),
            data=provisioned.panel_data.as_dict(),
            unique_id=provisioned.identity.fingerprint,
        )

    async def async_step_user(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> SubentryFlowResult:
        """Collect only panel address/password for an exact fleet parent."""
        if self._onboarding_parent_entry() is None:
            return self.async_abort(reason="invalid_parent")
        return cast(
            SubentryFlowResult,
            await self.async_step_panel_connect(user_input),
        )


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
