"""Small, secret-safe schemas and allocators for fleet onboarding flows."""

from __future__ import annotations

import copy
import json
import math
import re
import unicodedata
from collections.abc import Iterable, Mapping
from types import MappingProxyType
from typing import Any

import voluptuous as vol
from homeassistant.const import CONF_NAME
from homeassistant.data_entry_flow import section
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.selector import (
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)

from ..broker import BrokerKind
from ..components import optional
from ..const import (
    CONF_BROKER_KIND,
    CONF_COMPONENTS,
    CONF_HA_CONTROL_DOMAINS,
    CONF_HA_CONTROL_ENABLED,
    CONF_HA_CONTROL_LABEL,
    CONF_HOST,
    CONF_HUE_CA_CERT,
    CONF_MAX_MIRRORED_ENTITIES,
    CONF_MQTT_HOST,
    CONF_MQTT_PASSWORD,
    CONF_MQTT_PORT,
    CONF_MQTT_TLS_CA,
    CONF_MQTT_TLS_ENABLED,
    CONF_MQTT_USERNAME,
    CONF_ROOM_OVERRIDES,
    CONF_ROOT_PASSWORD,
    CONF_SCENE_ACTIONS,
    CONF_SCENE_PANEL,
    CONF_SSH_USERNAME,
    CONF_VOICE_HA_HOST,
    CONF_VOICE_WAKE_WORD,
    DEFAULT_AUTO_REPAIR,
    DEFAULT_HA_CONTROL_DOMAINS,
    DEFAULT_HA_CONTROL_ENABLED,
    DEFAULT_HA_CONTROL_LABEL,
    DEFAULT_MAX_MIRRORED_ENTITIES,
    DEFAULT_OFFLINE_GRACE_MINUTES,
    DEFAULT_REPAIR_COOLDOWN_MINUTES,
    DEFAULT_VOICE_WAKE_WORD,
    HA_CONTROL_DOMAINS,
    MESH_PANEL,
    OPT_AUTO_REPAIR,
    OPT_OFFLINE_GRACE_MINUTES,
    OPT_REPAIR_COOLDOWN_MINUTES,
    VOICE_WAKE_WORDS,
)
from ..panel_ops import MAX_MQTT_CA_BYTES

ADVANCED_SECTION = "advanced"
DEFAULT_SSH_USERNAME = "root"
SECRET_UNCHANGED = "**BRILLIANT_MQTT_SECRET_UNCHANGED**"
BROKER_MENU_OPTIONS = (
    BrokerKind.OFFICIAL_MOSQUITTO.value,
    BrokerKind.EXISTING_BROKER.value,
)

_MAX_BROKER_HOST_BYTES = 253
_MAX_MQTT_USERNAME_BYTES = 4 * 1024
_MAX_PASSWORD_BYTES = 16 * 1024
_MAX_PANEL_NAME_BYTES = 256
_MAX_PANEL_SLUG_LENGTH = 64
_MAX_MESH_PRIORITY = 99
_MAX_CONTROL_LABEL_BYTES = 256
_MAX_JSON_TEXT = 64 * 1024
_MAX_JSON_NODES = 2_048
_MAX_JSON_DEPTH = 12
_MAX_JSON_STRING_LENGTH = 4_096
_MAX_ROOM_OVERRIDES = 200
_MAX_SCENE_ACTIONS = 1_024
_MAX_MIRRORED_ENTITIES = 200
_MAX_VOICE_HA_HOST_BYTES = 4 * 1024
_MIN_OFFLINE_GRACE_MINUTES = 2
_MAX_OFFLINE_GRACE_MINUTES = 120
_MIN_REPAIR_COOLDOWN_MINUTES = 5
_MAX_REPAIR_COOLDOWN_MINUTES = 1_440
_PANEL_HOST_FORBIDDEN = frozenset(",*?![]|#@\\")
_SLUG_NON_ALPHANUMERIC = re.compile(r"[^a-z0-9]+")
_SERVICE_PATTERN = re.compile(r"[a-z0-9_]+")
_TARGET_KEYS = frozenset({"entity_id", "device_id", "area_id"})
_CERTIFICATE_BEGIN = "-----BEGIN CERTIFICATE-----"
_CERTIFICATE_END = "-----END CERTIFICATE-----"

_PASSWORD_SELECTOR = TextSelector(TextSelectorConfig(type=TextSelectorType.PASSWORD))
_PUBLIC_CA_SELECTOR = TextSelector(TextSelectorConfig(multiline=True))


class FlowInputError(ValueError):
    """One redacted set of field errors suitable for a config-flow response."""

    __slots__ = ("errors",)

    errors: Mapping[str, str]

    def __init__(self, errors: Mapping[str, str]) -> None:
        self.errors = MappingProxyType(dict(errors))
        super().__init__("invalid_flow_input")


class _StrictInteger(vol.Coerce):
    """Serializer-compatible integer coercion which rejects booleans/floats."""

    def __init__(self) -> None:
        super().__init__(int)

    def __call__(self, value: object) -> int:
        if type(value) is int:
            return value
        if not isinstance(value, str) or _has_control_char(value):
            raise vol.Invalid("expected integer")
        normalized = value.strip()
        if not normalized.isascii() or not normalized.isdecimal():
            raise vol.Invalid("expected integer")
        try:
            return int(normalized)
        except ValueError:
            raise vol.Invalid("expected integer") from None


def _has_control_char(value: str) -> bool:
    return any(unicodedata.category(character) == "Cc" for character in value)


def _encoded_length(value: str) -> int | None:
    try:
        return len(value.encode("utf-8"))
    except UnicodeEncodeError:
        return None


def _valid_text(
    value: object,
    *,
    maximum_bytes: int,
    whitespace_only_is_empty: bool = True,
) -> bool:
    if not isinstance(value, str) or not value or _has_control_char(value):
        return False
    if whitespace_only_is_empty and not value.strip():
        return False
    encoded_length = _encoded_length(value)
    return encoded_length is not None and encoded_length <= maximum_bytes


def _source_values(source: Mapping[str, object] | None) -> Mapping[str, object]:
    return source if source is not None else {}


def _advanced_values(source: Mapping[str, object] | None) -> Mapping[str, object]:
    values = _source_values(source)
    nested = values.get(ADVANCED_SECTION)
    return nested if isinstance(nested, Mapping) else values


def _is_public_certificate_chain(value: str) -> bool:
    encoded_length = _encoded_length(value)
    if (
        encoded_length is None
        or encoded_length > MAX_MQTT_CA_BYTES
        or "PRIVATE KEY" in value.upper()
        or any(
            unicodedata.category(character) == "Cc" and character not in "\r\n"
            for character in value
        )
    ):
        return False

    lines = value.strip(" \r\n").splitlines()
    if not lines:
        return False
    expecting_begin = True
    body_lines = 0
    certificate_count = 0
    for line in lines:
        if expecting_begin:
            if not line.strip():
                continue
            if line != _CERTIFICATE_BEGIN:
                return False
            expecting_begin = False
            body_lines = 0
            continue
        if line == _CERTIFICATE_END:
            if body_lines == 0:
                return False
            expecting_begin = True
            certificate_count += 1
            continue
        if not line or line.isspace() or line == _CERTIFICATE_BEGIN or line.startswith("-----END "):
            return False
        body_lines += 1
    return expecting_begin and certificate_count > 0


def _advanced_defaults(
    source: Mapping[str, object] | None,
) -> dict[str, object]:
    values = _advanced_values(source)
    raw_tls_enabled = values.get(CONF_MQTT_TLS_ENABLED, False)
    tls_enabled = raw_tls_enabled if type(raw_tls_enabled) is bool else False
    defaults: dict[str, object] = {CONF_MQTT_TLS_ENABLED: tls_enabled}
    raw_ca = values.get(CONF_MQTT_TLS_CA)
    if isinstance(raw_ca, str) and _is_public_certificate_chain(raw_ca):
        defaults[CONF_MQTT_TLS_CA] = raw_ca
    return defaults


def _marker_with_default(
    key: str,
    default: object,
    *,
    required: bool = True,
) -> vol.Marker:
    marker: type[vol.Required] | type[vol.Optional]
    marker = vol.Required if required else vol.Optional
    if default is vol.UNDEFINED:
        return marker(key)
    return marker(key, default=default)


def _source_default(source: Mapping[str, object], key: str) -> object:
    value = source.get(key, vol.UNDEFINED)
    return value if isinstance(value, str) and value else vol.UNDEFINED


def control_char_errors(
    values: Mapping[str, object],
    keys: Iterable[str],
) -> dict[str, str]:
    """Return field errors without ever including the submitted values."""
    return {
        key: "invalid_value"
        for key in keys
        if isinstance((value := values.get(key)), str) and _has_control_char(value)
    }


def broker_advanced_schema(
    source: Mapping[str, object] | None = None,
) -> vol.Schema:
    """Build the TLS/custom-public-CA fields shared by both broker choices."""
    defaults = _advanced_defaults(source)
    ca_default = defaults.get(CONF_MQTT_TLS_CA, vol.UNDEFINED)
    return vol.Schema(
        {
            vol.Required(
                CONF_MQTT_TLS_ENABLED,
                default=defaults[CONF_MQTT_TLS_ENABLED],
            ): bool,
            _marker_with_default(
                CONF_MQTT_TLS_CA,
                ca_default,
                required=False,
            ): _PUBLIC_CA_SELECTOR,
        }
    )


def broker_schema(
    kind: BrokerKind | str,
    source: Mapping[str, object] | None = None,
    *,
    default_host: str | None = None,
    reconfigure: bool = False,
) -> vol.Schema:
    """Build one equal broker form with a collapsed shared Advanced section."""
    broker_kind = BrokerKind(kind)
    values = _source_values(source)

    host_default = _source_default(values, CONF_MQTT_HOST)
    if (
        host_default is vol.UNDEFINED
        and broker_kind is BrokerKind.OFFICIAL_MOSQUITTO
        and isinstance(default_host, str)
        and default_host
    ):
        host_default = default_host

    raw_port = values.get(CONF_MQTT_PORT)
    port_default = raw_port if type(raw_port) is int and 1 <= raw_port <= 65535 else 1883
    username_default = _source_default(values, CONF_MQTT_USERNAME)
    password_marker = vol.Required(CONF_MQTT_PASSWORD)
    if reconfigure:
        password_marker.description = {"suggested_value": SECRET_UNCHANGED}

    advanced_defaults = _advanced_defaults(values)
    return vol.Schema(
        {
            _marker_with_default(CONF_MQTT_HOST, host_default): str,
            vol.Required(CONF_MQTT_PORT, default=port_default): vol.All(
                _StrictInteger(),
                vol.Range(min=1, max=65535),
            ),
            _marker_with_default(CONF_MQTT_USERNAME, username_default): str,
            password_marker: _PASSWORD_SELECTOR,
            vol.Optional(
                ADVANCED_SECTION,
                default=advanced_defaults,
            ): section(
                broker_advanced_schema(values),
                {"collapsed": True},
            ),
        }
    )


def _normalized_broker_kind(kind: BrokerKind | str) -> BrokerKind:
    try:
        return BrokerKind(kind)
    except (TypeError, ValueError):
        raise FlowInputError({CONF_BROKER_KIND: "invalid_value"}) from None


def _validated_port(value: object) -> int | None:
    try:
        port = _StrictInteger()(value)
    except vol.Invalid:
        return None
    return port if 1 <= port <= 65535 else None


def normalize_broker_input(
    kind: BrokerKind | str,
    user_input: Mapping[str, object],
    *,
    stored_password: str | None = None,
) -> dict[str, object]:
    """Validate and flatten a broker form without returning the secret sentinel."""
    broker_kind = _normalized_broker_kind(kind)
    errors = control_char_errors(
        user_input,
        (CONF_MQTT_HOST, CONF_MQTT_USERNAME, CONF_MQTT_PASSWORD),
    )

    raw_host = user_input.get(CONF_MQTT_HOST)
    if not _valid_text(raw_host, maximum_bytes=_MAX_BROKER_HOST_BYTES):
        errors[CONF_MQTT_HOST] = "invalid_value"
    raw_username = user_input.get(CONF_MQTT_USERNAME)
    if not _valid_text(raw_username, maximum_bytes=_MAX_MQTT_USERNAME_BYTES):
        errors[CONF_MQTT_USERNAME] = "invalid_value"

    raw_password = user_input.get(CONF_MQTT_PASSWORD)
    resolved_password: str | None = None
    if raw_password == SECRET_UNCHANGED:
        if (
            not _valid_text(stored_password, maximum_bytes=_MAX_PASSWORD_BYTES)
            or stored_password == SECRET_UNCHANGED
        ):
            errors[CONF_MQTT_PASSWORD] = "invalid_value"
        else:
            resolved_password = stored_password
    elif not _valid_text(raw_password, maximum_bytes=_MAX_PASSWORD_BYTES):
        errors[CONF_MQTT_PASSWORD] = "invalid_value"
    else:
        assert isinstance(raw_password, str)
        resolved_password = raw_password

    port = _validated_port(user_input.get(CONF_MQTT_PORT))
    if port is None:
        errors[CONF_MQTT_PORT] = "invalid_value"

    raw_advanced = user_input.get(ADVANCED_SECTION, {})
    if not isinstance(raw_advanced, Mapping):
        errors[ADVANCED_SECTION] = "invalid_value"
        advanced: Mapping[str, object] = {}
    else:
        advanced = raw_advanced
    tls_enabled = advanced.get(CONF_MQTT_TLS_ENABLED, False)
    if type(tls_enabled) is not bool:
        errors[CONF_MQTT_TLS_ENABLED] = "invalid_value"
        tls_enabled = False

    raw_ca = advanced.get(CONF_MQTT_TLS_CA)
    ca: str | None = None
    if raw_ca is not None:
        if not isinstance(raw_ca, str):
            errors[CONF_MQTT_TLS_CA] = "invalid_value"
        elif raw_ca.strip():
            if not tls_enabled or not _is_public_certificate_chain(raw_ca):
                errors[CONF_MQTT_TLS_CA] = "invalid_value"
            else:
                ca = raw_ca

    if errors:
        raise FlowInputError(errors)
    assert isinstance(raw_host, str)
    assert isinstance(raw_username, str)
    assert resolved_password is not None
    assert port is not None
    assert isinstance(tls_enabled, bool)

    normalized: dict[str, object] = {
        CONF_BROKER_KIND: broker_kind.value,
        CONF_MQTT_HOST: raw_host.strip().lower(),
        CONF_MQTT_PORT: port,
        CONF_MQTT_USERNAME: raw_username,
        CONF_MQTT_PASSWORD: resolved_password,
        CONF_MQTT_TLS_ENABLED: tls_enabled,
    }
    if ca is not None:
        normalized[CONF_MQTT_TLS_CA] = ca
    return normalized


def broker_form_source(user_input: Mapping[str, object]) -> dict[str, object]:
    """Retain valid broker form defaults without retaining its password."""
    source: dict[str, object] = {}
    raw_host = user_input.get(CONF_MQTT_HOST)
    if _valid_text(raw_host, maximum_bytes=_MAX_BROKER_HOST_BYTES):
        assert isinstance(raw_host, str)
        source[CONF_MQTT_HOST] = raw_host
    port = _validated_port(user_input.get(CONF_MQTT_PORT))
    if port is not None:
        source[CONF_MQTT_PORT] = port
    raw_username = user_input.get(CONF_MQTT_USERNAME)
    if _valid_text(raw_username, maximum_bytes=_MAX_MQTT_USERNAME_BYTES):
        assert isinstance(raw_username, str)
        source[CONF_MQTT_USERNAME] = raw_username

    raw_advanced = user_input.get(ADVANCED_SECTION)
    if isinstance(raw_advanced, Mapping):
        advanced: dict[str, object] = {}
        tls_enabled = raw_advanced.get(CONF_MQTT_TLS_ENABLED)
        if type(tls_enabled) is bool:
            advanced[CONF_MQTT_TLS_ENABLED] = tls_enabled
        raw_ca = raw_advanced.get(CONF_MQTT_TLS_CA)
        if isinstance(raw_ca, str) and _is_public_certificate_chain(raw_ca):
            advanced[CONF_MQTT_TLS_CA] = raw_ca
        if advanced:
            source[ADVANCED_SECTION] = advanced
    return source


def panel_connect_schema(
    source: Mapping[str, object] | None = None,
) -> vol.Schema:
    """Build the standard panel connection form; SSH username stays fixed to root."""
    values = _source_values(source)
    return vol.Schema(
        {
            _marker_with_default(CONF_HOST, _source_default(values, CONF_HOST)): str,
            vol.Required(CONF_ROOT_PASSWORD): _PASSWORD_SELECTOR,
        }
    )


def _valid_panel_host(value: object) -> bool:
    if not _valid_text(value, maximum_bytes=_MAX_BROKER_HOST_BYTES):
        return False
    assert isinstance(value, str)
    normalized = value.strip()
    return (
        normalized.isascii()
        and all(character.isprintable() and not character.isspace() for character in normalized)
        and not _PANEL_HOST_FORBIDDEN.intersection(normalized)
    )


def normalize_panel_connect_input(
    user_input: Mapping[str, object],
) -> dict[str, str]:
    """Validate connection input and add the only supported fleet SSH username."""
    errors = control_char_errors(user_input, (CONF_HOST, CONF_ROOT_PASSWORD))
    raw_host = user_input.get(CONF_HOST)
    if not _valid_panel_host(raw_host):
        errors[CONF_HOST] = "invalid_value"
    raw_password = user_input.get(CONF_ROOT_PASSWORD)
    if not _valid_text(
        raw_password,
        maximum_bytes=_MAX_PASSWORD_BYTES,
        whitespace_only_is_empty=False,
    ):
        errors[CONF_ROOT_PASSWORD] = "invalid_value"
    if errors:
        raise FlowInputError(errors)
    assert isinstance(raw_host, str)
    assert isinstance(raw_password, str)
    return {
        CONF_HOST: raw_host.strip(),
        CONF_SSH_USERNAME: DEFAULT_SSH_USERNAME,
        CONF_ROOT_PASSWORD: raw_password,
    }


def panel_connect_form_source(user_input: Mapping[str, object]) -> dict[str, object]:
    """Retain a valid panel host without retaining the root password."""
    raw_host = user_input.get(CONF_HOST)
    if not _valid_panel_host(raw_host):
        return {}
    assert isinstance(raw_host, str)
    return {CONF_HOST: raw_host.strip()}


def panel_confirm_schema(suggested_name: str) -> vol.Schema:
    """Build the confirmation form whose sole editable field is the panel name."""
    return vol.Schema({vol.Required(CONF_NAME, default=suggested_name): str})


def normalize_panel_name(user_input: Mapping[str, object]) -> str:
    """Validate one confirmed display name, checking controls before trimming."""
    raw_name = user_input.get(CONF_NAME)
    if not _valid_text(raw_name, maximum_bytes=_MAX_PANEL_NAME_BYTES):
        raise FlowInputError({CONF_NAME: "invalid_value"})
    assert isinstance(raw_name, str)
    return raw_name.strip()


def _unexpected_key_errors(
    values: Mapping[str, object],
    allowed: frozenset[str],
) -> dict[str, str]:
    return {} if set(values).issubset(allowed) else {"base": "invalid_value"}


def _validated_bounded_integer(
    value: object,
    *,
    minimum: int,
    maximum: int,
) -> int | None:
    try:
        normalized = _StrictInteger()(value)
    except vol.Invalid:
        return None
    return normalized if minimum <= normalized <= maximum else None


def _canonical_json(value: Mapping[str, object]) -> str:
    return json.dumps(
        dict(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _validate_json_value(
    value: object,
    *,
    depth: int,
    remaining: list[int],
) -> None:
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
        if len(value) > _MAX_JSON_STRING_LENGTH or _has_control_char(value):
            raise ValueError
        return
    if isinstance(value, list):
        for item in value:
            _validate_json_value(
                item,
                depth=depth + 1,
                remaining=remaining,
            )
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if (
                not isinstance(key, str)
                or len(key) > _MAX_JSON_STRING_LENGTH
                or _has_control_char(key)
            ):
                raise ValueError
            _validate_json_value(
                item,
                depth=depth + 1,
                remaining=remaining,
            )
        return
    raise ValueError


def _decode_json_object(raw: object) -> dict[str, object]:
    if not isinstance(raw, str) or len(raw) > _MAX_JSON_TEXT:
        raise ValueError
    try:
        loaded: object = json.loads(raw)
    except (RecursionError, TypeError, ValueError) as error:
        raise ValueError from error
    if not isinstance(loaded, dict):
        raise ValueError
    decoded: dict[str, object] = {}
    for key, item in loaded.items():
        if not isinstance(key, str):
            raise ValueError
        decoded[key] = item
    _validate_json_value(
        decoded,
        depth=0,
        remaining=[_MAX_JSON_NODES],
    )
    return decoded


def _decode_room_overrides(raw: object) -> dict[str, str]:
    loaded = _decode_json_object(raw)
    if len(loaded) > _MAX_ROOM_OVERRIDES:
        raise ValueError
    decoded: dict[str, str] = {}
    for raw_area, raw_room in loaded.items():
        if (
            not raw_area.strip()
            or len(raw_area) > 256
            or not isinstance(raw_room, str)
            or not raw_room.strip()
            or len(raw_room) > 256
        ):
            raise ValueError
        decoded[raw_area.strip()] = raw_room.strip()
    return decoded


def _decode_scene_actions(
    raw: object,
    panel_slugs: frozenset[str],
) -> dict[str, object]:
    loaded = _decode_json_object(raw)
    if len(loaded) > _MAX_SCENE_ACTIONS:
        raise ValueError
    decoded: dict[str, object] = {}
    for key, raw_action in loaded.items():
        if key.count(":") != 1:
            raise ValueError
        panel_slug, scene_id = key.split(":")
        if panel_slug not in panel_slugs or not scene_id or len(scene_id) > _MAX_JSON_STRING_LENGTH:
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


def _canonical_mapping_default(value: object) -> str:
    if not isinstance(value, Mapping):
        return "{}"
    try:
        raw = _canonical_json(value)
        _decode_json_object(raw)
    except (RecursionError, TypeError, ValueError):
        return "{}"
    return raw


def _canonical_room_overrides_default(value: object) -> str:
    raw = _canonical_mapping_default(value)
    try:
        _decode_room_overrides(raw)
    except ValueError:
        return "{}"
    return raw


def _source_bool_default(
    source: Mapping[str, object],
    key: str,
    fallback: bool,
) -> bool:
    value = source.get(key)
    return value if type(value) is bool else fallback


def _source_integer_default(
    source: Mapping[str, object],
    key: str,
    *,
    fallback: int,
    minimum: int,
    maximum: int,
) -> int:
    value = source.get(key)
    return value if type(value) is int and minimum <= value <= maximum else fallback


def _control_domains_default(source: Mapping[str, object]) -> list[str]:
    raw_domains = source.get(CONF_HA_CONTROL_DOMAINS)
    if (
        not isinstance(raw_domains, (list, tuple))
        or any(
            not isinstance(domain, str) or domain not in HA_CONTROL_DOMAINS
            for domain in raw_domains
        )
        or len(raw_domains) != len(set(raw_domains))
    ):
        return list(DEFAULT_HA_CONTROL_DOMAINS)
    return [domain for domain in HA_CONTROL_DOMAINS if domain in raw_domains]


def fleet_control_schema(
    source: Mapping[str, object] | None = None,
) -> vol.Schema:
    """Build the five Home Assistant-control globals, separate from scenes."""
    values = _source_values(source)
    raw_label = values.get(CONF_HA_CONTROL_LABEL)
    label_default = (
        raw_label.strip()
        if isinstance(raw_label, str)
        and _valid_text(raw_label, maximum_bytes=_MAX_CONTROL_LABEL_BYTES)
        else DEFAULT_HA_CONTROL_LABEL
    )
    return vol.Schema(
        {
            vol.Required(
                CONF_HA_CONTROL_ENABLED,
                default=_source_bool_default(
                    values,
                    CONF_HA_CONTROL_ENABLED,
                    DEFAULT_HA_CONTROL_ENABLED,
                ),
            ): bool,
            vol.Required(
                CONF_HA_CONTROL_LABEL,
                default=label_default,
            ): str,
            vol.Required(
                CONF_ROOM_OVERRIDES,
                default=_canonical_room_overrides_default(
                    values.get(CONF_ROOM_OVERRIDES),
                ),
            ): str,
            vol.Required(
                CONF_HA_CONTROL_DOMAINS,
                default=_control_domains_default(values),
            ): cv.multi_select({domain: domain for domain in HA_CONTROL_DOMAINS}),
            vol.Required(
                CONF_MAX_MIRRORED_ENTITIES,
                default=_source_integer_default(
                    values,
                    CONF_MAX_MIRRORED_ENTITIES,
                    fallback=DEFAULT_MAX_MIRRORED_ENTITIES,
                    minimum=1,
                    maximum=_MAX_MIRRORED_ENTITIES,
                ),
            ): vol.All(
                _StrictInteger(),
                vol.Range(min=1, max=_MAX_MIRRORED_ENTITIES),
            ),
        }
    )


def normalize_fleet_control_input(
    user_input: Mapping[str, object],
) -> dict[str, object]:
    """Validate and normalize the five Home Assistant-control globals."""
    allowed = frozenset(
        {
            CONF_HA_CONTROL_ENABLED,
            CONF_HA_CONTROL_LABEL,
            CONF_ROOM_OVERRIDES,
            CONF_HA_CONTROL_DOMAINS,
            CONF_MAX_MIRRORED_ENTITIES,
        }
    )
    errors = _unexpected_key_errors(user_input, allowed)
    normalized: dict[str, object] = {}

    enabled = user_input.get(CONF_HA_CONTROL_ENABLED)
    if type(enabled) is not bool:
        errors[CONF_HA_CONTROL_ENABLED] = "invalid_value"
    else:
        normalized[CONF_HA_CONTROL_ENABLED] = enabled

    raw_label = user_input.get(CONF_HA_CONTROL_LABEL)
    if not _valid_text(raw_label, maximum_bytes=_MAX_CONTROL_LABEL_BYTES):
        errors[CONF_HA_CONTROL_LABEL] = "invalid_value"
    else:
        assert isinstance(raw_label, str)
        normalized[CONF_HA_CONTROL_LABEL] = raw_label.strip()

    try:
        normalized[CONF_ROOM_OVERRIDES] = _decode_room_overrides(
            user_input.get(CONF_ROOM_OVERRIDES),
        )
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
        normalized[CONF_HA_CONTROL_DOMAINS] = [
            domain for domain in HA_CONTROL_DOMAINS if domain in raw_domains
        ]

    maximum = _validated_bounded_integer(
        user_input.get(CONF_MAX_MIRRORED_ENTITIES),
        minimum=1,
        maximum=_MAX_MIRRORED_ENTITIES,
    )
    if maximum is None:
        errors[CONF_MAX_MIRRORED_ENTITIES] = "invalid_value"
    else:
        normalized[CONF_MAX_MIRRORED_ENTITIES] = maximum

    if errors:
        raise FlowInputError(errors)
    return normalized


def _choice_tuple(values: Iterable[str]) -> tuple[str, ...]:
    choices: list[str] = []
    seen: set[str] = set()
    for value in values:
        if (
            value in seen
            or not _valid_text(value, maximum_bytes=_MAX_JSON_STRING_LENGTH)
            or value != value.strip()
        ):
            continue
        seen.add(value)
        choices.append(value)
    return tuple(choices)


def fleet_scenes_schema(
    source: Mapping[str, object] | None = None,
    *,
    panel_subentry_ids: Iterable[str],
) -> vol.Schema:
    """Build scene globals whose owner is one current same-fleet subentry."""
    values = _source_values(source)
    subentry_ids = _choice_tuple(panel_subentry_ids)
    raw_owner = values.get(CONF_SCENE_PANEL)
    owner_default = (
        raw_owner if isinstance(raw_owner, str) and raw_owner in subentry_ids else vol.UNDEFINED
    )
    return vol.Schema(
        {
            _marker_with_default(
                CONF_SCENE_PANEL,
                owner_default,
            ): vol.In(subentry_ids),
            vol.Required(
                CONF_SCENE_ACTIONS,
                default=_canonical_mapping_default(
                    values.get(CONF_SCENE_ACTIONS),
                ),
            ): str,
        }
    )


def normalize_fleet_scenes_input(
    user_input: Mapping[str, object],
    *,
    panel_subentry_ids: Iterable[str],
    panel_slugs: Iterable[str],
) -> dict[str, object]:
    """Validate scene ownership by subentry ID and action keys by panel slug."""
    errors = _unexpected_key_errors(
        user_input,
        frozenset({CONF_SCENE_PANEL, CONF_SCENE_ACTIONS}),
    )
    normalized: dict[str, object] = {}
    valid_subentry_ids = frozenset(_choice_tuple(panel_subentry_ids))
    valid_panel_slugs = frozenset(_choice_tuple(panel_slugs))

    raw_owner = user_input.get(CONF_SCENE_PANEL)
    owner = raw_owner.strip() if isinstance(raw_owner, str) else None
    if (
        not _valid_text(raw_owner, maximum_bytes=_MAX_JSON_STRING_LENGTH)
        or owner not in valid_subentry_ids
    ):
        errors[CONF_SCENE_PANEL] = "invalid_value"
    else:
        assert owner is not None
        normalized[CONF_SCENE_PANEL] = owner

    try:
        normalized[CONF_SCENE_ACTIONS] = _decode_scene_actions(
            user_input.get(CONF_SCENE_ACTIONS),
            valid_panel_slugs,
        )
    except ValueError:
        errors[CONF_SCENE_ACTIONS] = "invalid_value"

    if errors:
        raise FlowInputError(errors)
    return normalized


def fleet_defaults_schema(
    source: Mapping[str, object] | None = None,
) -> vol.Schema:
    """Build the fleet repair timing defaults without legacy auto-repin."""
    values = _source_values(source)
    return vol.Schema(
        {
            vol.Required(
                OPT_AUTO_REPAIR,
                default=_source_bool_default(
                    values,
                    OPT_AUTO_REPAIR,
                    DEFAULT_AUTO_REPAIR,
                ),
            ): bool,
            vol.Required(
                OPT_OFFLINE_GRACE_MINUTES,
                default=_source_integer_default(
                    values,
                    OPT_OFFLINE_GRACE_MINUTES,
                    fallback=DEFAULT_OFFLINE_GRACE_MINUTES,
                    minimum=_MIN_OFFLINE_GRACE_MINUTES,
                    maximum=_MAX_OFFLINE_GRACE_MINUTES,
                ),
            ): vol.All(
                _StrictInteger(),
                vol.Range(
                    min=_MIN_OFFLINE_GRACE_MINUTES,
                    max=_MAX_OFFLINE_GRACE_MINUTES,
                ),
            ),
            vol.Required(
                OPT_REPAIR_COOLDOWN_MINUTES,
                default=_source_integer_default(
                    values,
                    OPT_REPAIR_COOLDOWN_MINUTES,
                    fallback=DEFAULT_REPAIR_COOLDOWN_MINUTES,
                    minimum=_MIN_REPAIR_COOLDOWN_MINUTES,
                    maximum=_MAX_REPAIR_COOLDOWN_MINUTES,
                ),
            ): vol.All(
                _StrictInteger(),
                vol.Range(
                    min=_MIN_REPAIR_COOLDOWN_MINUTES,
                    max=_MAX_REPAIR_COOLDOWN_MINUTES,
                ),
            ),
        }
    )


def normalize_fleet_defaults_input(
    user_input: Mapping[str, object],
) -> dict[str, object]:
    """Validate fleet repair timing defaults with strict numeric coercion."""
    errors = _unexpected_key_errors(
        user_input,
        frozenset(
            {
                OPT_AUTO_REPAIR,
                OPT_OFFLINE_GRACE_MINUTES,
                OPT_REPAIR_COOLDOWN_MINUTES,
            }
        ),
    )
    normalized: dict[str, object] = {}

    auto_repair = user_input.get(OPT_AUTO_REPAIR)
    if type(auto_repair) is not bool:
        errors[OPT_AUTO_REPAIR] = "invalid_value"
    else:
        normalized[OPT_AUTO_REPAIR] = auto_repair

    offline_grace = _validated_bounded_integer(
        user_input.get(OPT_OFFLINE_GRACE_MINUTES),
        minimum=_MIN_OFFLINE_GRACE_MINUTES,
        maximum=_MAX_OFFLINE_GRACE_MINUTES,
    )
    if offline_grace is None:
        errors[OPT_OFFLINE_GRACE_MINUTES] = "invalid_value"
    else:
        normalized[OPT_OFFLINE_GRACE_MINUTES] = offline_grace

    repair_cooldown = _validated_bounded_integer(
        user_input.get(OPT_REPAIR_COOLDOWN_MINUTES),
        minimum=_MIN_REPAIR_COOLDOWN_MINUTES,
        maximum=_MAX_REPAIR_COOLDOWN_MINUTES,
    )
    if repair_cooldown is None:
        errors[OPT_REPAIR_COOLDOWN_MINUTES] = "invalid_value"
    else:
        normalized[OPT_REPAIR_COOLDOWN_MINUTES] = repair_cooldown

    if errors:
        raise FlowInputError(errors)
    return normalized


def panel_rename_schema(
    source: Mapping[str, object] | None = None,
) -> vol.Schema:
    """Build the display-name-only panel form."""
    values = _source_values(source)
    raw_name = values.get(CONF_NAME)
    default = (
        raw_name.strip()
        if isinstance(raw_name, str) and _valid_text(raw_name, maximum_bytes=_MAX_PANEL_NAME_BYTES)
        else vol.UNDEFINED
    )
    return vol.Schema(
        {
            _marker_with_default(
                CONF_NAME,
                default,
            ): str,
        }
    )


def normalize_panel_rename_input(
    user_input: Mapping[str, object],
) -> dict[str, str]:
    """Validate a panel rename without accepting immutable panel identity."""
    errors = _unexpected_key_errors(user_input, frozenset({CONF_NAME}))
    try:
        name = normalize_panel_name(user_input)
    except FlowInputError as error:
        errors.update(error.errors)
        name = ""
    if errors:
        raise FlowInputError(errors)
    return {CONF_NAME: name}


def panel_address_schema(
    source: Mapping[str, object] | None = None,
) -> vol.Schema:
    """Build the address-only form used before candidate identity inspection."""
    values = _source_values(source)
    raw_host = values.get(CONF_HOST)
    default = (
        raw_host.strip()
        if isinstance(raw_host, str) and _valid_panel_host(raw_host)
        else vol.UNDEFINED
    )
    return vol.Schema(
        {
            _marker_with_default(
                CONF_HOST,
                default,
            ): str,
        }
    )


def normalize_panel_address_input(
    user_input: Mapping[str, object],
) -> dict[str, str]:
    """Validate a panel endpoint without accepting credentials or identity."""
    errors = _unexpected_key_errors(user_input, frozenset({CONF_HOST}))
    raw_host = user_input.get(CONF_HOST)
    if not _valid_panel_host(raw_host):
        errors[CONF_HOST] = "invalid_value"
    if errors:
        raise FlowInputError(errors)
    assert isinstance(raw_host, str)
    return {CONF_HOST: raw_host.strip()}


def panel_ssh_credentials_schema() -> vol.Schema:
    """Build a replacement-root-password form with no stored-secret suggestion."""
    return vol.Schema(
        {
            vol.Required(CONF_ROOT_PASSWORD): _PASSWORD_SELECTOR,
        }
    )


def _validated_panel_password(
    user_input: Mapping[str, object],
    errors: dict[str, str],
) -> str | None:
    raw_password = user_input.get(CONF_ROOT_PASSWORD)
    if not _valid_text(
        raw_password,
        maximum_bytes=_MAX_PASSWORD_BYTES,
        whitespace_only_is_empty=False,
    ):
        errors[CONF_ROOT_PASSWORD] = "invalid_value"
        return None
    assert isinstance(raw_password, str)
    return raw_password


def normalize_panel_ssh_credentials_input(
    user_input: Mapping[str, object],
) -> dict[str, str]:
    """Validate one replacement SSH credential without redisplaying it."""
    errors = _unexpected_key_errors(
        user_input,
        frozenset({CONF_ROOT_PASSWORD}),
    )
    password = _validated_panel_password(user_input, errors)
    if errors:
        raise FlowInputError(errors)
    assert password is not None
    return {CONF_ROOT_PASSWORD: password}


def panel_rebind_schema(
    source: Mapping[str, object] | None = None,
) -> vol.Schema:
    """Build explicit replacement-panel input; candidate identity stays read-only."""
    values = _source_values(source)
    raw_host = values.get(CONF_HOST)
    host_default = (
        raw_host.strip()
        if isinstance(raw_host, str) and _valid_panel_host(raw_host)
        else vol.UNDEFINED
    )
    return vol.Schema(
        {
            _marker_with_default(
                CONF_HOST,
                host_default,
            ): str,
            vol.Required(CONF_ROOT_PASSWORD): _PASSWORD_SELECTOR,
        }
    )


def normalize_panel_rebind_input(
    user_input: Mapping[str, object],
) -> dict[str, str]:
    """Validate explicit rebind input without trusting a user-supplied fingerprint."""
    errors = _unexpected_key_errors(
        user_input,
        frozenset({CONF_HOST, CONF_ROOT_PASSWORD}),
    )
    raw_host = user_input.get(CONF_HOST)
    if not _valid_panel_host(raw_host):
        errors[CONF_HOST] = "invalid_value"
    password = _validated_panel_password(user_input, errors)
    if errors:
        raise FlowInputError(errors)
    assert isinstance(raw_host, str)
    assert password is not None
    return {
        CONF_HOST: raw_host.strip(),
        CONF_ROOT_PASSWORD: password,
    }


def _valid_optional_text(value: object, *, maximum_bytes: int) -> bool:
    if not isinstance(value, str) or _has_control_char(value):
        return False
    encoded_length = _encoded_length(value)
    return encoded_length is not None and encoded_length <= maximum_bytes


def panel_feature_overrides_schema(
    source: Mapping[str, object] | None = None,
) -> vol.Schema:
    """Build only the typed, supported per-panel feature override values."""
    values = _source_values(source)
    raw_wake_word = values.get(CONF_VOICE_WAKE_WORD)
    wake_word_default = (
        raw_wake_word
        if isinstance(raw_wake_word, str) and raw_wake_word in VOICE_WAKE_WORDS
        else DEFAULT_VOICE_WAKE_WORD
    )
    raw_ha_host = values.get(CONF_VOICE_HA_HOST)
    ha_host_default = (
        raw_ha_host.strip()
        if isinstance(raw_ha_host, str)
        and _valid_optional_text(
            raw_ha_host,
            maximum_bytes=_MAX_VOICE_HA_HOST_BYTES,
        )
        else ""
    )
    raw_hue_ca = values.get(CONF_HUE_CA_CERT)
    hue_ca_default = (
        raw_hue_ca
        if isinstance(raw_hue_ca, str)
        and (not raw_hue_ca.strip() or _is_public_certificate_chain(raw_hue_ca))
        else ""
    )
    return vol.Schema(
        {
            vol.Required(
                CONF_VOICE_WAKE_WORD,
                default=wake_word_default,
            ): vol.In(VOICE_WAKE_WORDS),
            vol.Optional(
                CONF_VOICE_HA_HOST,
                default=ha_host_default,
            ): str,
            vol.Optional(
                CONF_HUE_CA_CERT,
                default=hue_ca_default,
            ): _PUBLIC_CA_SELECTOR,
        }
    )


def normalize_panel_feature_overrides_input(
    user_input: Mapping[str, object],
) -> dict[str, object]:
    """Validate the allowlisted per-panel feature overrides."""
    errors = _unexpected_key_errors(
        user_input,
        frozenset(
            {
                CONF_VOICE_WAKE_WORD,
                CONF_VOICE_HA_HOST,
                CONF_HUE_CA_CERT,
            }
        ),
    )
    normalized: dict[str, object] = {}

    wake_word = user_input.get(CONF_VOICE_WAKE_WORD)
    if not isinstance(wake_word, str) or wake_word not in VOICE_WAKE_WORDS:
        errors[CONF_VOICE_WAKE_WORD] = "invalid_value"
    else:
        normalized[CONF_VOICE_WAKE_WORD] = wake_word

    raw_ha_host = user_input.get(CONF_VOICE_HA_HOST, "")
    if not _valid_optional_text(
        raw_ha_host,
        maximum_bytes=_MAX_VOICE_HA_HOST_BYTES,
    ):
        errors[CONF_VOICE_HA_HOST] = "invalid_value"
    else:
        assert isinstance(raw_ha_host, str)
        normalized[CONF_VOICE_HA_HOST] = raw_ha_host.strip()

    raw_hue_ca = user_input.get(CONF_HUE_CA_CERT, "")
    if not isinstance(raw_hue_ca, str):
        errors[CONF_HUE_CA_CERT] = "invalid_value"
    elif raw_hue_ca.strip():
        if not _is_public_certificate_chain(raw_hue_ca):
            errors[CONF_HUE_CA_CERT] = "invalid_value"
        else:
            normalized[CONF_HUE_CA_CERT] = raw_hue_ca
    else:
        normalized[CONF_HUE_CA_CERT] = ""

    if errors:
        raise FlowInputError(errors)
    return normalized


def allocate_panel_slug(
    name: str,
    existing_slugs: Iterable[str],
) -> str:
    """Allocate one stable bounded slug without changing any existing identity."""
    if not isinstance(name, str) or _has_control_char(name):
        raise FlowInputError({CONF_NAME: "invalid_name"})
    base = _SLUG_NON_ALPHANUMERIC.sub("-", name.lower()).strip("-")
    base = base[:_MAX_PANEL_SLUG_LENGTH].rstrip("-")
    if not base:
        raise FlowInputError({CONF_NAME: "invalid_name"})
    if base == MESH_PANEL:
        raise FlowInputError({CONF_NAME: "reserved_panel"})

    used = set(existing_slugs)
    if base not in used:
        return base
    suffix_number = 2
    while True:
        suffix = f"-{suffix_number}"
        prefix = base[: _MAX_PANEL_SLUG_LENGTH - len(suffix)].rstrip("-")
        if not prefix:
            raise FlowInputError({CONF_NAME: "invalid_name"})
        candidate = f"{prefix}{suffix}"
        if candidate not in used:
            return candidate
        suffix_number += 1


def allocate_mesh_priority(existing_priorities: Iterable[int]) -> int:
    """Return the smallest positive priority without renumbering existing panels."""
    used = {priority for priority in existing_priorities if type(priority) is int and priority > 0}
    for candidate in range(1, _MAX_MESH_PRIORITY + 1):
        if candidate not in used:
            return candidate
    raise FlowInputError({"base": "mesh_priority_exhausted"})


def _mqtt_schema_fields(
    source: Mapping[str, Any], *, secret_optional: bool = False
) -> dict[Any, Any]:
    """The four broker fields shared by the add-broker and reconfigure steps.

    Defaults come from *source* (prior-entry prefill, or the entry being reconfigured);
    the three string fields fall back to blank, the port to 1883. With
    ``secret_optional`` (reconfigure mode) the password field is optional and
    defaults to blank — a blank submission re-uses the stored password.
    """
    password_marker: vol.Marker = (
        vol.Optional(CONF_MQTT_PASSWORD, default="")
        if secret_optional
        else vol.Required(CONF_MQTT_PASSWORD)
    )
    return {
        vol.Required(CONF_MQTT_HOST, default=source.get(CONF_MQTT_HOST, vol.UNDEFINED)): str,
        vol.Required(CONF_MQTT_PORT, default=source.get(CONF_MQTT_PORT, 1883)): vol.All(
            vol.Coerce(int), vol.Range(min=1, max=65535)
        ),
        vol.Required(
            CONF_MQTT_USERNAME, default=source.get(CONF_MQTT_USERNAME, vol.UNDEFINED)
        ): str,
        password_marker: TextSelector(TextSelectorConfig(type=TextSelectorType.PASSWORD)),
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
