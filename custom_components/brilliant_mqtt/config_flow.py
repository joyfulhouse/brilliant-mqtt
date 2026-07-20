"""Config flow: one entry per panel — each panel stores ITS OWN root password.

Onboarding is detection-first: step 1 connects (the only required inputs) and, if the
agent is already installed, ADOPTS the panel by reading its config back — no further
questions, no changes to the panel. A not-yet-installed panel continues to the MQTT
broker (pre-filled from a prior panel) and the panel-settings step. Reconfigure edits
every mutable setting and pushes the change to the panel; the slug is immutable.
"""

from __future__ import annotations

import copy
import json
import math
import re
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncssh
import voluptuous as vol
from homeassistant.config_entries import ConfigEntry, ConfigFlow, ConfigFlowResult, OptionsFlow
from homeassistant.const import CONF_NAME
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.selector import TextSelector, TextSelectorConfig

from . import _fleet_lock, panel_ops
from . import components as component_ops
from .async_cleanup import shielded_cleanup_after_failure
from .components import REGISTRY, optional
from .const import (
    COMPONENT_BLE_OBSERVER,
    COMPONENT_BRIDGE,
    COMPONENT_HA_MIRROR,
    COMPONENT_VOICE,
    CONF_BLE_OBSERVER_ALLOWLIST_JSON,
    CONF_BLE_SCANNER_ENABLED,
    CONF_COMPONENTS,
    CONF_HA_CONTROL_DOMAINS,
    CONF_HA_CONTROL_ENABLED,
    CONF_HA_CONTROL_LABEL,
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
    CONF_VOICE_HA_HOST,
    CONF_VOICE_WAKE_WORD,
    CONFIG_ENTRY_VERSION,
    DATA_SSH_HOST_KEY,
    DEFAULT_AUTO_REPAIR,
    DEFAULT_BLE_OBSERVER_ALLOWLIST_JSON,
    DEFAULT_BLE_SCANNER_ENABLED,
    DEFAULT_HA_CONTROL_DOMAINS,
    DEFAULT_HA_CONTROL_ENABLED,
    DEFAULT_HA_CONTROL_LABEL,
    DEFAULT_MAX_MIRRORED_ENTITIES,
    DEFAULT_OFFLINE_GRACE_MINUTES,
    DEFAULT_REPAIR_COOLDOWN_MINUTES,
    DEFAULT_TRUST_HOST_KEY_CHANGES,
    DEFAULT_VOICE_WAKE_WORD,
    DOMAIN,
    HA_CONTROL_DOMAINS,
    MESH_PANEL,
    OPT_AUTO_REPAIR,
    OPT_OFFLINE_GRACE_MINUTES,
    OPT_REPAIR_COOLDOWN_MINUTES,
    OPT_TRUST_HOST_KEY_CHANGES,
    VOICE_WAKE_WORDS,
    ble_observer_allowlist_json,
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
    CONF_HA_CONTROL_LABEL,
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
    fields[
        vol.Required(
            CONF_BLE_SCANNER_ENABLED,
            default=source.get(CONF_BLE_SCANNER_ENABLED, DEFAULT_BLE_SCANNER_ENABLED),
        )
    ] = bool
    fields[
        vol.Required(
            CONF_BLE_OBSERVER_ALLOWLIST_JSON,
            default=source.get(
                CONF_BLE_OBSERVER_ALLOWLIST_JSON,
                DEFAULT_BLE_OBSERVER_ALLOWLIST_JSON,
            ),
        )
    ] = TextSelector(TextSelectorConfig(multiline=False))
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
_PANEL_PATTERN = re.compile(r"[a-z0-9][a-z0-9_-]{0,62}")
_TARGET_KEYS = frozenset({"entity_id", "device_id", "area_id"})
_MAX_BLE_ALLOWLIST_ENTRIES = 64
_BLE_ADDRESS_PATTERN = re.compile(r"(?:[0-9A-Fa-f]{2}[:-]){5}[0-9A-Fa-f]{2}")
_BLE_ADDRESS_KEYS = frozenset({"address"})
_BLE_IBEACON_KEYS = frozenset({"ibeacon_uuid", "ibeacon_major", "ibeacon_minor"})
_BLE_OBSERVER_RUNTIME_KEYS = (
    CONF_HOST,
    CONF_MQTT_HOST,
    CONF_MQTT_PORT,
    CONF_MQTT_USERNAME,
    CONF_MQTT_PASSWORD,
    CONF_BLE_OBSERVER_ALLOWLIST_JSON,
)
_MAIN_AGENT_RUNTIME_KEYS = (
    CONF_MQTT_HOST,
    CONF_MQTT_PORT,
    CONF_MQTT_USERNAME,
    CONF_MQTT_PASSWORD,
    CONF_MESH_PRIORITY,
    CONF_HA_CONTROL_ENABLED,
)
_PANEL_ENDPOINT_KEYS = (CONF_HOST, CONF_ROOT_PASSWORD)


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
    """Keep ordinary edits, but never echo unsafe JSON text back into a form."""
    values = dict(user_input)
    for key in (CONF_ROOM_OVERRIDES, CONF_SCENE_ACTIONS):
        raw = values.get(key)
        if not isinstance(raw, str) or len(raw) > _MAX_JSON_TEXT or _has_control_char(raw):
            values[key] = "{}"
    raw_allowlist = values.get(CONF_BLE_OBSERVER_ALLOWLIST_JSON)
    if (
        not isinstance(raw_allowlist, str)
        or len(raw_allowlist) > _MAX_JSON_TEXT
        or _has_control_char(raw_allowlist)
    ):
        values[CONF_BLE_OBSERVER_ALLOWLIST_JSON] = DEFAULT_BLE_OBSERVER_ALLOWLIST_JSON
    return values


def _global_defaults(panel: str) -> dict[str, Any]:
    return {
        CONF_HA_CONTROL_ENABLED: DEFAULT_HA_CONTROL_ENABLED,
        CONF_HA_CONTROL_LABEL: DEFAULT_HA_CONTROL_LABEL,
        CONF_ROOM_OVERRIDES: {},
        CONF_HA_CONTROL_DOMAINS: list(DEFAULT_HA_CONTROL_DOMAINS),
        CONF_MAX_MIRRORED_ENTITIES: DEFAULT_MAX_MIRRORED_ENTITIES,
        CONF_SCENE_PANEL: panel,
        CONF_SCENE_ACTIONS: {},
    }


def _inherited_globals(entries: list[ConfigEntry], panel: str) -> dict[str, Any]:
    if not entries:
        return _global_defaults(panel)
    source = min(
        entries,
        key=lambda entry: (str(entry.data.get(CONF_PANEL, "")), entry.entry_id),
    ).data
    defaults = _global_defaults(panel)
    return {
        key: copy.deepcopy(source[key] if key in source else defaults[key]) for key in _GLOBAL_KEYS
    }


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


def _reject_duplicate_json_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    decoded: dict[str, object] = {}
    for key, value in pairs:
        if key in decoded:
            raise ValueError
        decoded[key] = value
    return decoded


def _normalize_ble_ibeacon(
    item: Mapping[str, object],
) -> tuple[tuple[object, ...], dict[str, object]]:
    raw_uuid = item["ibeacon_uuid"]
    major = item["ibeacon_major"]
    minor = item["ibeacon_minor"]
    if (
        not isinstance(raw_uuid, str)
        or type(major) is not int
        or type(minor) is not int
        or not 0 <= major <= 0xFFFF
        or not 0 <= minor <= 0xFFFF
    ):
        raise ValueError
    try:
        ibeacon_uuid = str(UUID(raw_uuid.strip()))
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError from error
    return (
        ("ibeacon", ibeacon_uuid, major, minor),
        {
            "ibeacon_uuid": ibeacon_uuid,
            "ibeacon_major": major,
            "ibeacon_minor": minor,
        },
    )


def _normalize_ble_allowlist_entry(
    item: object,
) -> tuple[tuple[object, ...], dict[str, object]]:
    if not isinstance(item, dict) or not all(isinstance(key, str) for key in item):
        raise ValueError
    keys = frozenset(item)
    if keys == _BLE_ADDRESS_KEYS:
        address = item["address"]
        if not isinstance(address, str):
            raise ValueError
        address = address.strip().replace("-", ":").upper()
        if _BLE_ADDRESS_PATTERN.fullmatch(address) is None:
            raise ValueError
        return ("address", address), {"address": address}
    if keys == _BLE_IBEACON_KEYS:
        return _normalize_ble_ibeacon(item)
    raise ValueError


def _decode_ble_allowlist(raw: object) -> str:
    """Validate and canonicalize the observer's bounded identity allowlist JSON.

    This intentionally mirrors ``brilliant_ble_observer.model.parse_allowlist``:
    the HACS integration is a separately shipped Python 3.14 package and cannot
    import the on-panel Python 3.10 package at runtime. Keep both sides narrow and
    covered by the same address/iBeacon contract cases instead of coupling their
    deployment artifacts.
    """
    if not isinstance(raw, str) or len(raw) > _MAX_JSON_TEXT or _has_control_char(raw):
        raise ValueError
    try:
        value = json.loads(raw, object_pairs_hook=_reject_duplicate_json_keys)
    except (json.JSONDecodeError, TypeError, ValueError) as error:
        raise ValueError from error
    if not isinstance(value, list) or len(value) > _MAX_BLE_ALLOWLIST_ENTRIES:
        raise ValueError

    normalized: list[dict[str, object]] = []
    identities: set[tuple[object, ...]] = set()
    for item in value:
        identity, entry = _normalize_ble_allowlist_entry(item)
        if identity in identities:
            raise ValueError
        identities.add(identity)
        normalized.append(entry)
    return json.dumps(normalized, separators=(",", ":"), sort_keys=True)


def _validated_ble_input(
    user_input: Mapping[str, Any],
) -> tuple[dict[str, str], dict[str, Any]]:
    """Validate independent observer/scanner switches and canonical identity data."""
    errors: dict[str, str] = {}
    values: dict[str, Any] = {}
    for key in (COMPONENT_BLE_OBSERVER, CONF_BLE_SCANNER_ENABLED):
        value = user_input.get(key)
        if type(value) is not bool:
            errors[key] = "invalid_value"
        elif key == CONF_BLE_SCANNER_ENABLED:
            values[key] = value
    try:
        values[CONF_BLE_OBSERVER_ALLOWLIST_JSON] = _decode_ble_allowlist(
            user_input.get(CONF_BLE_OBSERVER_ALLOWLIST_JSON)
        )
    except ValueError:
        errors[CONF_BLE_OBSERVER_ALLOWLIST_JSON] = "invalid_value"
    return errors, values


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
        scene_bridge = env.get(panel_ops.ENV_SCENE_BRIDGE_ENABLED, "0")
        if scene_bridge not in ("0", "1"):
            raise ValueError("invalid scene bridge toggle")
        return {
            CONF_PANEL: panel,
            CONF_MESH_PRIORITY: int(env.get(panel_ops.ENV_MESH_PRIORITY, "0")),
            CONF_MQTT_HOST: env[panel_ops.ENV_MQTT_HOST],
            CONF_MQTT_PORT: port,
            CONF_MQTT_USERNAME: env[panel_ops.ENV_MQTT_USERNAME],
            CONF_MQTT_PASSWORD: env[panel_ops.ENV_MQTT_PASSWORD],
            CONF_HA_CONTROL_ENABLED: scene_bridge == "1",
            CONF_COMPONENTS: {
                COMPONENT_BRIDGE: True,
                COMPONENT_BLE_OBSERVER: False,
            },
            CONF_BLE_SCANNER_ENABLED: DEFAULT_BLE_SCANNER_ENABLED,
            CONF_BLE_OBSERVER_ALLOWLIST_JSON: DEFAULT_BLE_OBSERVER_ALLOWLIST_JSON,
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


def _validate_config_apply_request(
    env_content: str | None,
    *,
    restart_bridge: bool,
    ble_observer_config: tuple[bool, str] | None,
) -> None:
    """Reject contradictory panel config mutations before opening SSH."""
    if restart_bridge and env_content is None:
        raise ValueError("bridge restart requires a full environment")
    if env_content is not None and ble_observer_config is not None:
        raise ValueError("full and observer-only environment writes are mutually exclusive")


async def _apply_requested_panel_config(
    shell: PanelShell,
    *,
    env_content: str | None,
    restart_bridge: bool,
    ble_observer_config: tuple[bool, str] | None,
) -> None:
    """Apply a pre-validated full, observer-only, or verification-only request."""
    if env_content is not None:
        await panel_ops.write_env(shell, env_content)
    if ble_observer_config is not None:
        enabled, allowlist_json = ble_observer_config
        await panel_ops.configure_ble_observer_env(
            shell,
            enabled=enabled,
            allowlist_json=allowlist_json,
        )
    if restart_bridge:
        await panel_ops.restart(shell)


async def _apply_config(
    hass: HomeAssistant,
    host: str,
    password: str,
    *,
    pinned_key: str | None,
    env_content: str | None,
    expected_panel: str,
    restart_bridge: bool = True,
    ble_observer_config: tuple[bool, str] | None = None,
    fail_closed_ble: bool = False,
) -> str:
    """Verify the panel and apply exactly one requested shared-env mutation.

    A full ``env_content`` write may restart the bridge.  ``ble_observer_config``
    instead patches only observer-owned keys and never restarts the bridge.  With both
    values ``None`` this is a verification-only connection.  A not-yet-installed
    panel skips every write and returns the captured/verified host key.

    Before overwriting, it refuses to clobber a DIFFERENT panel: if the box already
    runs an agent whose env names another slug than *expected_panel* (e.g. the host
    was mistyped to another controller in the fleet), it raises _WrongPanelError
    instead of stamping this entry's identity onto that panel and restarting it.
    """
    _validate_config_apply_request(
        env_content,
        restart_bridge=restart_bridge,
        ble_observer_config=ble_observer_config,
    )

    key = pinned_key
    panel_may_have_changed = False
    try:
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
                panel_may_have_changed = env_content is not None or ble_observer_config is not None
                await _apply_requested_panel_config(
                    shell,
                    env_content=env_content,
                    restart_bridge=restart_bridge,
                    ble_observer_config=ble_observer_config,
                )
    except BaseException as error:
        if fail_closed_ble and panel_may_have_changed and key is not None:
            await shielded_cleanup_after_failure(
                error,
                _quarantine_ble_at(
                    hass,
                    host=host,
                    password=password,
                    host_key=key,
                ),
            )
        raise
    if key is None:
        raise OSError("no host key captured")
    return key


def _ble_observer_runtime_values(data: Mapping[str, Any]) -> tuple[object, ...]:
    """Values whose change requires an already-running observer to reload its env."""
    return tuple(
        ble_observer_allowlist_json(data)
        if key == CONF_BLE_OBSERVER_ALLOWLIST_JSON
        else data.get(key)
        for key in _BLE_OBSERVER_RUNTIME_KEYS
    )


def _ble_observer_config_changed(previous: Mapping[str, Any], updated: Mapping[str, Any]) -> bool:
    """Whether an unsuccessful apply could leave newly desired observer state behind."""
    previous_components = previous.get(CONF_COMPONENTS) or {}
    updated_components = updated.get(CONF_COMPONENTS) or {}
    was_selected = bool(previous_components.get(COMPONENT_BLE_OBSERVER, False))
    selected = bool(updated_components.get(COMPONENT_BLE_OBSERVER, False))
    if was_selected != selected:
        return True
    if ble_observer_allowlist_json(previous) != ble_observer_allowlist_json(updated):
        return True
    return (was_selected or selected) and (
        _ble_observer_runtime_values(previous) != _ble_observer_runtime_values(updated)
    )


def _main_agent_config_changed(previous: Mapping[str, Any], updated: Mapping[str, Any]) -> bool:
    """Whether the bridge must reload values from its shared environment."""
    defaults: Mapping[str, Any] = {
        CONF_MESH_PRIORITY: 0,
        CONF_HA_CONTROL_ENABLED: DEFAULT_HA_CONTROL_ENABLED,
    }
    return any(
        previous.get(key, defaults.get(key)) != updated.get(key, defaults.get(key))
        for key in _MAIN_AGENT_RUNTIME_KEYS
    )


def _panel_reconfigure_required(
    previous: Mapping[str, Any],
    updated: Mapping[str, Any],
    *,
    current_components: Mapping[str, Any],
    desired_components: Mapping[str, Any],
) -> bool:
    """Whether a reconfigure changes any panel-side state or SSH endpoint."""
    endpoint_changed = any(previous.get(key) != updated.get(key) for key in _PANEL_ENDPOINT_KEYS)
    selection_changed = any(
        bool(current_components.get(component.id, False))
        != bool(desired_components.get(component.id, False))
        for component in optional()
    )
    return (
        endpoint_changed
        or selection_changed
        or _main_agent_config_changed(previous, updated)
        or _ble_observer_config_changed(previous, updated)
    )


async def _quarantine_failed_ble_transition(
    hass: HomeAssistant,
    previous: Mapping[str, Any],
    updated: Mapping[str, Any],
    *,
    host_key: str,
) -> bool:
    """Fail-close a changed observer after a later component operation failed."""
    if not _ble_observer_config_changed(previous, updated):
        return True
    return await _quarantine_ble_at(
        hass,
        host=str(updated[CONF_HOST]),
        password=str(updated[CONF_ROOT_PASSWORD]),
        host_key=host_key,
    )


async def _quarantine_ble_at(
    hass: HomeAssistant,
    *,
    host: str,
    password: str,
    host_key: str,
) -> bool:
    """Try one pinned, bounded observer quarantine and report whether it was proven."""
    try:
        async with _panel_session(
            hass,
            host,
            password,
            host_key,
        ) as shell:
            await panel_ops.quarantine_ble_observer(shell)
    except (OSError, asyncssh.Error, panel_ops.PanelOpError):
        return False
    return True


async def _restart_changed_ble_observer(
    hass: HomeAssistant,
    previous: Mapping[str, Any],
    updated: Mapping[str, Any],
    *,
    host_key: str,
) -> None:
    """Reload a still-selected observer only when its panel-side inputs changed."""
    previous_components = previous.get(CONF_COMPONENTS) or {}
    updated_components = updated.get(CONF_COMPONENTS) or {}
    was_selected = bool(previous_components.get(COMPONENT_BLE_OBSERVER, False))
    remains_selected = bool(updated_components.get(COMPONENT_BLE_OBSERVER, False))
    if (
        not was_selected
        or not remains_selected
        or _ble_observer_runtime_values(previous) == _ble_observer_runtime_values(updated)
    ):
        return
    async with _panel_session(
        hass,
        str(updated[CONF_HOST]),
        str(updated[CONF_ROOT_PASSWORD]),
        host_key,
    ) as shell:
        await component_ops.refresh_ble_observer(hass, shell, updated)


async def _apply_component_selection_change(
    hass: HomeAssistant,
    component_id: str,
    *,
    was_selected: bool,
    selected: bool,
    data: Mapping[str, Any],
    host_key: str,
) -> None:
    """Apply one optional-component transition in one pinned panel session."""
    if was_selected == selected:
        return
    async with _panel_session(
        hass,
        str(data[CONF_HOST]),
        str(data[CONF_ROOT_PASSWORD]),
        host_key,
    ) as shell:
        if selected:
            await REGISTRY[component_id].install(hass, shell, data)
        else:
            await REGISTRY[component_id].remove(shell)


class BrilliantMqttConfigFlow(ConfigFlow, domain=DOMAIN):
    """Add one Brilliant panel per entry (detection-first; adopts installed agents)."""

    VERSION = CONFIG_ENTRY_VERSION

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
                        entries = self._async_current_entries()
                        inherited = _inherited_globals(entries, adopted[CONF_PANEL])
                        if not entries:
                            # With no fleet owner, preserve the installed bridge's
                            # explicit scene toggle. Once a fleet exists, its seven
                            # canonical globals are authoritative for every new panel.
                            inherited[CONF_HA_CONTROL_ENABLED] = adopted[CONF_HA_CONTROL_ENABLED]
                        return self.async_create_entry(
                            title=f"Brilliant {adopted[CONF_PANEL]}",
                            data={
                                **self._connect,
                                **adopted,
                                **inherited,
                            },
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
            # A control char in the HA host flows into render_voice_env → _env_quote.
            errors.update(
                _control_char_errors(
                    user_input,
                    (CONF_VOICE_HA_HOST,),
                )
            )
            control_values: dict[str, Any] = {}
            ble_values: dict[str, Any] = {}
            if slug and slug != MESH_PANEL:
                panels = frozenset(
                    {
                        slug,
                        *(
                            str(entry.data[CONF_PANEL])
                            for entry in self._async_current_entries()
                            if isinstance(entry.data.get(CONF_PANEL), str)
                        ),
                    }
                )
                control_errors, control_values = _validated_control_input(
                    user_input, panels=panels, default_panel=slug
                )
                errors.update(control_errors)
            ble_errors, ble_values = _validated_ble_input(user_input)
            errors.update(ble_errors)
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
                    CONF_VOICE_WAKE_WORD: user_input[CONF_VOICE_WAKE_WORD],
                    CONF_VOICE_HA_HOST: user_input[CONF_VOICE_HA_HOST],
                    CONF_HUE_CA_CERT: user_input.get(CONF_HUE_CA_CERT, ""),
                    **ble_values,
                    **control_values,
                }
                current_cid: str | None = None
                panel_may_have_changed = False
                try:
                    for cid, selected in components.items():
                        if not selected:
                            continue
                        current_cid = cid
                        if cid == COMPONENT_BRIDGE:
                            # Bridge install writes the shared env containing the
                            # desired BLE switch before later optional components run.
                            panel_may_have_changed = True
                        async with _panel_session(
                            self.hass,
                            self._connect[CONF_HOST],
                            self._connect[CONF_ROOT_PASSWORD],
                            self._connect[DATA_SSH_HOST_KEY],
                        ) as shell:
                            await REGISTRY[cid].install(self.hass, shell, entry_data)
                    if not components.get(COMPONENT_BLE_OBSERVER, False):
                        current_cid = None
                        if not await _quarantine_ble_at(
                            self.hass,
                            host=str(self._connect[CONF_HOST]),
                            password=str(self._connect[CONF_ROOT_PASSWORD]),
                            host_key=str(self._connect[DATA_SSH_HOST_KEY]),
                        ):
                            raise panel_ops.PanelOpError(
                                "default-off BLE observer state could not be proven"
                            )
                except BaseException as error:
                    if panel_may_have_changed:
                        cleanup_succeeded = await shielded_cleanup_after_failure(
                            error,
                            _quarantine_ble_at(
                                self.hass,
                                host=str(self._connect[CONF_HOST]),
                                password=str(self._connect[CONF_ROOT_PASSWORD]),
                                host_key=str(self._connect[DATA_SSH_HOST_KEY]),
                            ),
                        )
                    else:
                        if not isinstance(error, Exception):
                            raise
                        cleanup_succeeded = True
                    if not isinstance(
                        error,
                        (VoicePayloadError, OSError, asyncssh.Error, panel_ops.PanelOpError),
                    ):
                        raise
                    voice_failure = isinstance(error, VoicePayloadError) or (
                        current_cid == COMPONENT_VOICE
                    )
                    errors["base"] = (
                        "cannot_install_voice"
                        if voice_failure and cleanup_succeeded
                        else "cannot_install"
                    )
                else:
                    for fleet_entry in self.hass.config_entries.async_entries(DOMAIN):
                        self.hass.config_entries.async_update_entry(
                            fleet_entry,
                            data={**fleet_entry.data, **copy.deepcopy(control_values)},
                        )
                    return self.async_create_entry(title=f"Brilliant {slug}", data=entry_data)
        inherited = _inherited_globals(self._async_current_entries(), "")
        schema = vol.Schema(
            {
                vol.Required(CONF_NAME): str,
                vol.Required(CONF_MESH_PRIORITY, default=0): vol.All(
                    vol.Coerce(int), vol.Range(min=0, max=99)
                ),
                **_components_schema_fields({}),
                **_control_schema_fields(inherited, panel_default=""),
            }
        )
        # Preserve name + mesh + voice fields across an error redisplay.
        if user_input is not None:
            schema = self.add_suggested_values_to_schema(
                schema, _safe_control_redisplay_values(user_input)
            )
        return self.async_show_form(step_id="script", data_schema=schema, errors=errors)

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Edit host/password/broker/mesh/components for one panel and push it.

        The panel slug (CONF_PANEL) is immutable after onboarding.
        """
        entry = self._get_reconfigure_entry()
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
            ble_errors, ble_values = _validated_ble_input(user_input)
            errors.update(ble_errors)
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
                    current: dict[str, Any] = dict(entry.data.get(CONF_COMPONENTS) or {})
                    desired = {
                        component.id: bool(
                            user_input.get(component.id, current.get(component.id, False))
                        )
                        for component in optional()
                    }
                    desired[COMPONENT_BRIDGE] = True
                    desired[COMPONENT_HA_MIRROR] = False
                    # Checkbox ids live only inside CONF_COMPONENTS; global control
                    # values are canonicalized separately by _validated_control_input.
                    optional_ids = {component.id for component in optional()}
                    clean_input = {
                        key: value
                        for key, value in user_input.items()
                        if key not in optional_ids and key not in _GLOBAL_KEYS
                    }
                    candidate_data: dict[str, Any] = {
                        **entry.data,
                        **clean_input,
                        CONF_COMPONENTS: desired,
                        **ble_values,
                        **control_values,
                    }
                    ble_config_changed = _ble_observer_config_changed(entry.data, candidate_data)
                    main_agent_changed = _main_agent_config_changed(entry.data, candidate_data)
                    panel_reconfigure_required = _panel_reconfigure_required(
                        entry.data,
                        candidate_data,
                        current_components=current,
                        desired_components=desired,
                    )
                    try:
                        if panel_reconfigure_required:
                            env = (
                                panel_ops.render_env(
                                    panel=entry.data[CONF_PANEL],
                                    mesh_priority=user_input[CONF_MESH_PRIORITY],
                                    mqtt_host=user_input[CONF_MQTT_HOST],
                                    mqtt_port=user_input[CONF_MQTT_PORT],
                                    mqtt_username=user_input[CONF_MQTT_USERNAME],
                                    mqtt_password=user_input[CONF_MQTT_PASSWORD],
                                    scene_bridge_enabled=control_values[CONF_HA_CONTROL_ENABLED],
                                    ble_observer_enabled=(
                                        user_input[COMPONENT_BLE_OBSERVER] is True
                                    ),
                                    ble_observer_allowlist_json=ble_values[
                                        CONF_BLE_OBSERVER_ALLOWLIST_JSON
                                    ],
                                )
                                if main_agent_changed
                                else None
                            )
                            observer_config = (
                                (
                                    user_input[COMPONENT_BLE_OBSERVER] is True,
                                    ble_values[CONF_BLE_OBSERVER_ALLOWLIST_JSON],
                                )
                                if ble_config_changed and not main_agent_changed
                                else None
                            )
                            host_key = await _apply_config(
                                self.hass,
                                user_input[CONF_HOST],
                                user_input[CONF_ROOT_PASSWORD],
                                pinned_key=pinned_key,
                                env_content=env,
                                expected_panel=entry.data[CONF_PANEL],
                                restart_bridge=main_agent_changed,
                                ble_observer_config=observer_config,
                                fail_closed_ble=ble_config_changed,
                            )
                        elif isinstance(pinned_key, str):
                            # Scanner/control-only changes are HA-local. Retain the
                            # already-verified pin without opening an SSH connection.
                            host_key = pinned_key
                        else:
                            raise asyncssh.HostKeyNotVerifiable("missing stored host key")
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
                        new_data: dict[str, Any] = {
                            **candidate_data,
                            DATA_SSH_HOST_KEY: host_key,
                        }
                        try:
                            for c in optional():
                                was: bool = bool(current.get(c.id, False))
                                now: bool = desired[c.id]
                                await _apply_component_selection_change(
                                    self.hass,
                                    c.id,
                                    was_selected=was,
                                    selected=now,
                                    data=new_data,
                                    host_key=host_key,
                                )
                            await _restart_changed_ble_observer(
                                self.hass,
                                entry.data,
                                new_data,
                                host_key=host_key,
                            )
                        except BaseException as error:
                            cleanup_succeeded = await shielded_cleanup_after_failure(
                                error,
                                _quarantine_failed_ble_transition(
                                    self.hass,
                                    entry.data,
                                    new_data,
                                    host_key=host_key,
                                ),
                            )
                            if isinstance(error, VoicePayloadError):
                                errors["base"] = (
                                    "cannot_install_voice" if cleanup_succeeded else "cannot_apply"
                                )
                            elif isinstance(
                                error,
                                (OSError, asyncssh.Error, panel_ops.PanelOpError),
                            ):
                                errors["base"] = "cannot_apply"
                            else:
                                raise
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
                vol.Required(CONF_ROOT_PASSWORD): str,
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
