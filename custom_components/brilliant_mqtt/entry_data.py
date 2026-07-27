"""Typed ownership boundaries for fleet entries and panel compatibility stores."""

from __future__ import annotations

import asyncio
import math
import re
from collections.abc import Coroutine, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Protocol, runtime_checkable

from homeassistant.config_entries import ConfigEntry, ConfigSubentry
from homeassistant.const import CONF_NAME
from homeassistant.core import HomeAssistant

from .broker import BrokerProfile
from .const import (
    CONF_BROKER_KIND,
    CONF_COMPONENTS,
    CONF_ENTRY_KIND,
    CONF_FEATURE_OVERRIDES,
    CONF_HA_CONTROL_DOMAINS,
    CONF_HA_CONTROL_ENABLED,
    CONF_HA_CONTROL_LABEL,
    CONF_HOST,
    CONF_IDENTITY_FINGERPRINT,
    CONF_MANAGEMENT_ID,
    CONF_MAX_MIRRORED_ENTITIES,
    CONF_MESH_PRIORITY,
    CONF_MQTT_HOST,
    CONF_MQTT_PASSWORD,
    CONF_MQTT_PORT,
    CONF_MQTT_TLS_CA,
    CONF_MQTT_TLS_ENABLED,
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
    CONFIG_ENTRY_VERSION,
    ENTRY_KIND_FLEET,
    MESH_PANEL,
    SUBENTRY_TYPE_PANEL,
)

type JsonScalar = None | bool | int | float | str
type JsonValue = JsonScalar | list["JsonValue"] | Mapping[str, "JsonValue"]

_NEW_PANEL_SLUG = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
_LEGACY_PANEL_SLUG = re.compile(r"^[a-z0-9][a-z0-9_-]*$")

_FLEET_KEYS = frozenset(
    {
        CONF_ENTRY_KIND,
        CONF_BROKER_KIND,
        CONF_MQTT_HOST,
        CONF_MQTT_PORT,
        CONF_MQTT_USERNAME,
        CONF_MQTT_PASSWORD,
        CONF_MQTT_TLS_ENABLED,
        CONF_MQTT_TLS_CA,
        CONF_NEXT_MESH_PRIORITY,
        CONF_HA_CONTROL_ENABLED,
        CONF_HA_CONTROL_LABEL,
        CONF_ROOM_OVERRIDES,
        CONF_HA_CONTROL_DOMAINS,
        CONF_MAX_MIRRORED_ENTITIES,
        CONF_SCENE_PANEL,
        CONF_SCENE_ACTIONS,
        CONF_SCHEMA_VERSION,
    }
)
_FLEET_REQUIRED_KEYS = _FLEET_KEYS - {CONF_MQTT_TLS_CA}
_PANEL_KEYS = frozenset(
    {
        CONF_IDENTITY_FINGERPRINT,
        CONF_SSH_HOST_KEY,
        CONF_HOST,
        CONF_SSH_USERNAME,
        CONF_ROOT_PASSWORD,
        CONF_NAME,
        CONF_PANEL,
        CONF_MANAGEMENT_ID,
        CONF_COMPONENTS,
        CONF_FEATURE_OVERRIDES,
        CONF_MESH_PRIORITY,
        CONF_PROVISIONING_TRANSACTION_ID,
    }
)
_PANEL_REQUIRED_KEYS = _PANEL_KEYS - {CONF_PROVISIONING_TRANSACTION_ID}


class EntryDataError(ValueError):
    """Persisted fleet or panel data violates its ownership/type contract."""


def _invalid(code: str) -> EntryDataError:
    """Build a redacted error which never includes persisted values."""
    return EntryDataError(code)


def _required_string(data: Mapping[str, Any], key: str) -> str:
    value = data[key]
    if not isinstance(value, str) or not value:
        raise _invalid("invalid_entry_data")
    return value


def _required_positive_int(data: Mapping[str, Any], key: str) -> int:
    value = data[key]
    if type(value) is not int or value < 1:
        raise _invalid("invalid_entry_data")
    return value


def _required_nonnegative_int(data: Mapping[str, Any], key: str) -> int:
    value = data[key]
    if type(value) is not int or value < 0:
        raise _invalid("invalid_entry_data")
    return value


def _copy_json(value: Any, *, depth: int = 0) -> JsonValue:
    """Validate and detach one persisted JSON-compatible value."""
    if depth > 64:
        raise _invalid("invalid_entry_data")
    if value is None or isinstance(value, (str, bool)):
        return value
    if type(value) is int:
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise _invalid("invalid_entry_data")
        return value
    if isinstance(value, list):
        return [_copy_json(item, depth=depth + 1) for item in value]
    if isinstance(value, Mapping):
        copied: dict[str, JsonValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise _invalid("invalid_entry_data")
            copied[key] = _copy_json(item, depth=depth + 1)
        return copied
    raise _invalid("invalid_entry_data")


def _json_mapping(value: Any) -> MappingProxyType[str, JsonValue]:
    copied = _copy_json(value)
    if not isinstance(copied, Mapping):
        raise _invalid("invalid_entry_data")
    return MappingProxyType(dict(copied))


def _components_mapping(value: Any) -> MappingProxyType[str, bool]:
    if not isinstance(value, Mapping):
        raise _invalid("invalid_entry_data")
    components: dict[str, bool] = {}
    for key, enabled in value.items():
        if not isinstance(key, str) or not key or type(enabled) is not bool:
            raise _invalid("invalid_entry_data")
        components[key] = enabled
    return MappingProxyType(components)


@dataclass(frozen=True, slots=True)
class FleetConfig:
    """Immutable fleet-owned settings parsed from one version-4 config entry."""

    entry_kind: str
    broker: BrokerProfile
    next_mesh_priority: int
    ha_control_enabled: bool
    ha_control_label: str
    room_overrides: MappingProxyType[str, JsonValue]
    ha_control_domains: tuple[str, ...]
    max_mirrored_entities: int
    scene_panel: str
    scene_actions: MappingProxyType[str, JsonValue]
    schema_version: int

    @classmethod
    def from_entry(cls, entry: ConfigEntry[Any]) -> FleetConfig:
        """Parse a fleet entry without accepting panel-owned or unknown data."""
        data = entry.data
        keys = frozenset(data)
        if (
            entry.version != CONFIG_ENTRY_VERSION
            or not _FLEET_REQUIRED_KEYS.issubset(keys)
            or not keys.issubset(_FLEET_KEYS)
        ):
            raise _invalid("invalid_fleet_entry_data")
        if data[CONF_ENTRY_KIND] != ENTRY_KIND_FLEET:
            raise _invalid("invalid_fleet_entry_data")
        if (
            type(data[CONF_SCHEMA_VERSION]) is not int
            or data[CONF_SCHEMA_VERSION] != CONFIG_ENTRY_VERSION
        ):
            raise _invalid("invalid_fleet_entry_data")
        if type(data[CONF_HA_CONTROL_ENABLED]) is not bool:
            raise _invalid("invalid_fleet_entry_data")

        domains = data[CONF_HA_CONTROL_DOMAINS]
        if not isinstance(domains, (list, tuple)) or any(
            not isinstance(domain, str) or not domain for domain in domains
        ):
            raise _invalid("invalid_fleet_entry_data")

        try:
            broker = BrokerProfile.from_mapping(data)
            return cls(
                entry_kind=ENTRY_KIND_FLEET,
                broker=broker,
                next_mesh_priority=_required_positive_int(data, CONF_NEXT_MESH_PRIORITY),
                ha_control_enabled=data[CONF_HA_CONTROL_ENABLED],
                ha_control_label=_required_string(data, CONF_HA_CONTROL_LABEL),
                room_overrides=_json_mapping(data[CONF_ROOM_OVERRIDES]),
                ha_control_domains=tuple(domains),
                max_mirrored_entities=_required_positive_int(data, CONF_MAX_MIRRORED_ENTITIES),
                scene_panel=_required_string(data, CONF_SCENE_PANEL),
                scene_actions=_json_mapping(data[CONF_SCENE_ACTIONS]),
                schema_version=CONFIG_ENTRY_VERSION,
            )
        except EntryDataError:
            raise
        except Exception:
            raise _invalid("invalid_fleet_entry_data") from None


@dataclass(frozen=True, slots=True)
class PanelConfig:
    """Immutable panel-owned settings parsed from one panel subentry."""

    identity_fingerprint: str
    ssh_host_key: str
    host: str
    ssh_username: str
    root_password: str = field(repr=False)
    name: str
    panel: str
    management_id: str
    components: MappingProxyType[str, bool]
    feature_overrides: MappingProxyType[str, JsonValue] = field(repr=False)
    mesh_priority: int
    provisioning_transaction_id: str | None

    @classmethod
    def from_subentry(cls, subentry: ConfigSubentry) -> PanelConfig:
        """Parse a new-fleet panel without accepting fleet-owned or unknown data."""
        data = subentry.data
        keys = frozenset(data)
        if (
            subentry.subentry_type != SUBENTRY_TYPE_PANEL
            or not _PANEL_REQUIRED_KEYS.issubset(keys)
            or not keys.issubset(_PANEL_KEYS)
        ):
            raise _invalid("invalid_panel_entry_data")

        panel = _required_string(data, CONF_PANEL)
        if panel == MESH_PANEL or _NEW_PANEL_SLUG.fullmatch(panel) is None:
            raise _invalid("invalid_panel_entry_data")
        identity_fingerprint = _required_string(data, CONF_IDENTITY_FINGERPRINT)
        if subentry.unique_id != identity_fingerprint:
            raise _invalid("invalid_panel_entry_data")

        transaction_id: str | None = None
        if CONF_PROVISIONING_TRANSACTION_ID in data:
            raw_transaction_id = data[CONF_PROVISIONING_TRANSACTION_ID]
            if not isinstance(raw_transaction_id, str) or not raw_transaction_id:
                raise _invalid("invalid_panel_entry_data")
            transaction_id = raw_transaction_id

        try:
            return cls(
                identity_fingerprint=identity_fingerprint,
                ssh_host_key=_required_string(data, CONF_SSH_HOST_KEY),
                host=_required_string(data, CONF_HOST),
                ssh_username=_required_string(data, CONF_SSH_USERNAME),
                root_password=_required_string(data, CONF_ROOT_PASSWORD),
                name=_required_string(data, CONF_NAME),
                panel=panel,
                management_id=_required_string(data, CONF_MANAGEMENT_ID),
                components=_components_mapping(data[CONF_COMPONENTS]),
                feature_overrides=_json_mapping(data[CONF_FEATURE_OVERRIDES]),
                mesh_priority=_required_nonnegative_int(data, CONF_MESH_PRIORITY),
                provisioning_transaction_id=transaction_id,
            )
        except EntryDataError:
            raise
        except Exception:
            raise _invalid("invalid_panel_entry_data") from None


@runtime_checkable
class PanelConfigStore(Protocol):
    """Storage seam shared by fleet subentries and version-3 panel entries."""

    @property
    def panel_id(self) -> str: ...

    @property
    def subentry_id(self) -> str | None: ...

    @property
    def management_id(self) -> str: ...

    @property
    def data(self) -> Mapping[str, Any]: ...

    @property
    def options(self) -> Mapping[str, Any]: ...

    def update_data(self, new_data: Mapping[str, Any]) -> None: ...

    def update_options(self, new_options: Mapping[str, Any]) -> None: ...

    def async_create_background_task[R](
        self,
        hass: HomeAssistant,
        target: Coroutine[Any, Any, R],
        name: str,
        eager_start: bool = True,
    ) -> asyncio.Task[R]: ...


class FleetPanelStore:
    """Adapt one panel subentry to the manager's storage contract."""

    __slots__ = ("entry", "hass", "subentry")

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry[Any],
        subentry: ConfigSubentry,
    ) -> None:
        PanelConfig.from_subentry(subentry)
        self.hass = hass
        self.entry = entry
        self.subentry = subentry

    @property
    def panel_id(self) -> str:
        """Return the fleet runtime key."""
        return self.subentry.subentry_id

    @property
    def subentry_id(self) -> str:
        """Return the Home Assistant subentry association."""
        return self.subentry.subentry_id

    @property
    def management_id(self) -> str:
        """Return the stable management-entity identity."""
        return str(self.subentry.data[CONF_MANAGEMENT_ID])

    @property
    def data(self) -> Mapping[str, Any]:
        """Expose current panel data without copying fleet globals into it."""
        return self.subentry.data

    @property
    def options(self) -> Mapping[str, Any]:
        """Expose panel options from the panel-owned feature override mapping."""
        return _json_mapping(self.subentry.data[CONF_FEATURE_OVERRIDES])

    def update_data(self, new_data: Mapping[str, Any]) -> None:
        """Validate and atomically update this subentry, preserving stable IDs."""
        if (
            new_data.get(CONF_PANEL) != self.subentry.data[CONF_PANEL]
            or new_data.get(CONF_MANAGEMENT_ID) != self.subentry.data[CONF_MANAGEMENT_ID]
            or new_data.get(CONF_IDENTITY_FINGERPRINT)
            != self.subentry.data[CONF_IDENTITY_FINGERPRINT]
            or new_data.get(CONF_SSH_HOST_KEY) != self.subentry.data[CONF_SSH_HOST_KEY]
        ):
            raise _invalid("immutable_panel_identity")
        plain_data = dict(new_data)
        components = new_data.get(CONF_COMPONENTS)
        if not isinstance(components, Mapping):
            raise _invalid("invalid_panel_entry_data")
        plain_data[CONF_COMPONENTS] = dict(components)
        plain_data[CONF_FEATURE_OVERRIDES] = _copy_json(new_data.get(CONF_FEATURE_OVERRIDES))
        candidate = ConfigSubentry(
            data=MappingProxyType(plain_data),
            subentry_id=self.subentry.subentry_id,
            subentry_type=self.subentry.subentry_type,
            title=str(plain_data.get(CONF_NAME, "")),
            unique_id=self.subentry.unique_id,
        )
        PanelConfig.from_subentry(candidate)
        self.hass.config_entries.async_update_subentry(
            self.entry,
            self.subentry,
            data=plain_data,
            title=str(plain_data[CONF_NAME]),
        )

    def update_options(self, new_options: Mapping[str, Any]) -> None:
        """Persist panel options in the subentry's feature override field."""
        options = _copy_json(new_options)
        if not isinstance(options, Mapping):
            raise _invalid("invalid_panel_entry_data")
        self.update_data(
            {
                **self.subentry.data,
                CONF_FEATURE_OVERRIDES: dict(options),
            }
        )

    def async_create_background_task[R](
        self,
        hass: HomeAssistant,
        target: Coroutine[Any, Any, R],
        name: str,
        eager_start: bool = True,
    ) -> asyncio.Task[R]:
        """Tie panel background work to the parent fleet entry lifecycle."""
        return self.entry.async_create_background_task(hass, target, name, eager_start)


class LegacyPanelStore:
    """Expose one version-3 panel entry without routing it through new allocation."""

    __slots__ = ("entry", "hass")

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry[Any]) -> None:
        panel = entry.data.get(CONF_PANEL)
        if not isinstance(panel, str) or _LEGACY_PANEL_SLUG.fullmatch(panel) is None:
            raise _invalid("invalid_legacy_panel_entry_data")
        self.hass = hass
        self.entry = entry

    @property
    def panel_id(self) -> str:
        """Return the legacy runtime key."""
        return self.entry.entry_id

    @property
    def subentry_id(self) -> None:
        """Legacy entries have no Home Assistant subentry association."""
        return None

    @property
    def management_id(self) -> str:
        """Preserve legacy management entity unique IDs."""
        return self.entry.entry_id

    @property
    def data(self) -> Mapping[str, Any]:
        """Expose the complete legacy data shape unchanged."""
        return self.entry.data

    @property
    def options(self) -> Mapping[str, Any]:
        """Expose the complete legacy options shape unchanged."""
        return self.entry.options

    def update_data(self, new_data: Mapping[str, Any]) -> None:
        """Delegate legacy data updates to the owning ConfigEntry."""
        self.hass.config_entries.async_update_entry(self.entry, data=new_data)

    def update_options(self, new_options: Mapping[str, Any]) -> None:
        """Delegate legacy option updates to the owning ConfigEntry."""
        self.hass.config_entries.async_update_entry(self.entry, options=new_options)

    def async_create_background_task[R](
        self,
        hass: HomeAssistant,
        target: Coroutine[Any, Any, R],
        name: str,
        eager_start: bool = True,
    ) -> asyncio.Task[R]:
        """Delegate legacy background work to the owning ConfigEntry."""
        return self.entry.async_create_background_task(hass, target, name, eager_start)
