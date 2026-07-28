"""Small, secret-safe schemas and allocators for fleet onboarding flows."""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable, Mapping
from types import MappingProxyType

import voluptuous as vol
from homeassistant.const import CONF_NAME
from homeassistant.data_entry_flow import section
from homeassistant.helpers.selector import (
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)

from .broker import BrokerKind
from .const import (
    CONF_BROKER_KIND,
    CONF_HOST,
    CONF_MQTT_HOST,
    CONF_MQTT_PASSWORD,
    CONF_MQTT_PORT,
    CONF_MQTT_TLS_CA,
    CONF_MQTT_TLS_ENABLED,
    CONF_MQTT_USERNAME,
    CONF_ROOT_PASSWORD,
    CONF_SSH_USERNAME,
    MESH_PANEL,
)
from .panel_ops import MAX_MQTT_CA_BYTES

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
_PANEL_HOST_FORBIDDEN = frozenset(",*?![]|#@\\")
_SLUG_NON_ALPHANUMERIC = re.compile(r"[^a-z0-9]+")
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
