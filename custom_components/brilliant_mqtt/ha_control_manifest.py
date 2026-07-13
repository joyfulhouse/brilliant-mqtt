"""Build the Brilliant control manifest from Home Assistant-owned state."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

import voluptuous as vol
from homeassistant.components.cover import (
    ATTR_CURRENT_POSITION,
    ATTR_CURRENT_TILT_POSITION,
    CoverEntityFeature,
)
from homeassistant.components.light import (
    ATTR_SUPPORTED_COLOR_MODES,
    valid_supported_color_modes,
)
from homeassistant.components.light.const import (
    COLOR_MODES_BRIGHTNESS,
    VALID_COLOR_MODES,
    ColorMode,
)
from homeassistant.const import (
    ATTR_DEVICE_CLASS,
    ATTR_FRIENDLY_NAME,
    ATTR_SUPPORTED_FEATURES,
    STATE_UNAVAILABLE,
    STATE_UNKNOWN,
)
from homeassistant.core import HomeAssistant, State
from homeassistant.helpers import area_registry as ar
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import label_registry as lr

from .ha_control_protocol import MAPPING_VERSION, SCHEMA_VERSION, stable_id

SUPPORTED_DOMAINS = frozenset({"light", "switch", "lock", "cover"})
_ATTR_BRIGHTNESS = "brightness"


@dataclass(frozen=True, slots=True)
class ControlSettings:
    """Settings controlling which HA registry entries enter the manifest."""

    label_name: str
    room_overrides: Mapping[str, str]
    enabled_domains: frozenset[str]
    maximum_entities: int

    def __post_init__(self) -> None:
        """Detach and freeze caller-owned mappings."""
        object.__setattr__(self, "room_overrides", MappingProxyType(dict(self.room_overrides)))


@dataclass(frozen=True, slots=True)
class ManifestEntity:
    """An HA entity reduced to the Brilliant control contract."""

    stable_id: str
    entity_id: str
    domain: str
    device_class: str | None
    friendly_name: str
    ha_area: str | None
    brilliant_room: str | None
    commands: tuple[str, ...]
    capabilities: Mapping[str, bool]

    def __post_init__(self) -> None:
        """Freeze capability flags along with the entity record."""
        object.__setattr__(self, "capabilities", MappingProxyType(dict(self.capabilities)))

    def as_payload(self) -> dict[str, object]:
        """Return this entity as JSON-native values."""
        return {
            "stable_id": self.stable_id,
            "entity_id": self.entity_id,
            "domain": self.domain,
            "device_class": self.device_class,
            "friendly_name": self.friendly_name,
            "ha_area": self.ha_area,
            "brilliant_room": self.brilliant_room,
            "commands": list(self.commands),
            "capabilities": dict(self.capabilities),
        }


@dataclass(frozen=True, slots=True)
class ManifestSnapshot:
    """A complete immutable manifest built at one point in time."""

    schema_version: int
    mapping_version: int
    revision: int
    generated_at_ms: int
    entities: tuple[ManifestEntity, ...]
    unsupported_domains: tuple[str, ...]

    def as_payload(self) -> dict[str, object]:
        """Return the complete manifest as JSON-native values."""
        return {
            "schema_version": self.schema_version,
            "mapping_version": self.mapping_version,
            "revision": self.revision,
            "generated_at_ms": self.generated_at_ms,
            "entities": [entity.as_payload() for entity in self.entities],
            "unsupported_domains": list(self.unsupported_domains),
        }


def build_manifest(
    hass: HomeAssistant,
    settings: ControlSettings,
    revision: int,
    generated_at_ms: int,
) -> ManifestSnapshot:
    """Build a deterministic manifest solely from Home Assistant registries/state."""
    entity_registry = er.async_get(hass)
    device_registry = dr.async_get(hass)
    area_registry = ar.async_get(hass)
    label = lr.async_get(hass).async_get_label_by_name(settings.label_name)

    selected = (
        sorted(
            (
                entry
                for entry in entity_registry.entities.values()
                if label is not None and label.label_id in entry.labels
            ),
            key=lambda entry: entry.entity_id,
        )
        if label is not None
        else []
    )
    unsupported_domains = tuple(
        sorted({entry.domain for entry in selected if entry.domain not in SUPPORTED_DOMAINS})
    )
    eligible = (
        entry
        for entry in selected
        if entry.domain in SUPPORTED_DOMAINS and entry.domain in settings.enabled_domains
    )
    overrides = {
        area_name.casefold().strip(): brilliant_room
        for area_name, brilliant_room in settings.room_overrides.items()
    }
    entities = tuple(
        _manifest_entity(hass, entry, entity_registry, device_registry, area_registry, overrides)
        for entry in list(eligible)[: max(0, settings.maximum_entities)]
    )
    return ManifestSnapshot(
        schema_version=SCHEMA_VERSION,
        mapping_version=MAPPING_VERSION,
        revision=revision,
        generated_at_ms=generated_at_ms,
        entities=entities,
        unsupported_domains=unsupported_domains,
    )


def build_state_payload(
    state: State | None,
    entity: ManifestEntity,
    sequence: int,
    generated_at_ms: int,
) -> dict[str, object]:
    """Build an allowlisted, versioned state update for one manifest entity."""
    return {
        "schema_version": SCHEMA_VERSION,
        "mapping_version": MAPPING_VERSION,
        "stable_id": entity.stable_id,
        "entity_id": entity.entity_id,
        "sequence": sequence,
        "generated_at_ms": generated_at_ms,
        "available": state is not None
        and state.state not in {STATE_UNAVAILABLE, STATE_UNKNOWN},
        "state": state.state if state is not None else STATE_UNAVAILABLE,
        "attributes": _supported_attributes(state),
    }


def _manifest_entity(
    hass: HomeAssistant,
    entry: er.RegistryEntry,
    entities: er.EntityRegistry,
    devices: dr.DeviceRegistry,
    areas: ar.AreaRegistry,
    overrides: Mapping[str, str],
) -> ManifestEntity:
    state = hass.states.get(entry.entity_id)
    area_name = _area_name(entry, entities, devices, areas)
    commands, capabilities = _commands_and_capabilities(entry, state)
    return ManifestEntity(
        stable_id=stable_id(entry.entity_id),
        entity_id=entry.entity_id,
        domain=entry.domain,
        device_class=_device_class(entry, state),
        friendly_name=_friendly_name(entry, state),
        ha_area=area_name,
        brilliant_room=(
            overrides.get(area_name.casefold().strip(), area_name)
            if area_name is not None
            else None
        ),
        commands=commands,
        capabilities=capabilities,
    )


def _area_name(
    entity: er.RegistryEntry,
    entities: er.EntityRegistry,
    devices: dr.DeviceRegistry,
    areas: ar.AreaRegistry,
) -> str | None:
    del entities  # Kept in the signature to make registry ownership explicit.
    area_id = entity.area_id
    if area_id is None and entity.device_id is not None:
        device = devices.async_get(entity.device_id)
        area_id = device.area_id if device is not None else None
    area = areas.async_get_area(area_id) if area_id is not None else None
    return area.name if area is not None else None


def _friendly_name(entry: er.RegistryEntry, state: State | None) -> str:
    if state is not None and isinstance(
        friendly_name := state.attributes.get(ATTR_FRIENDLY_NAME), str
    ):
        return friendly_name
    return entry.name or entry.original_name or entry.entity_id


def _device_class(entry: er.RegistryEntry, state: State | None) -> str | None:
    if state is not None and isinstance(
        device_class := state.attributes.get(ATTR_DEVICE_CLASS), str
    ):
        return device_class
    return entry.device_class or entry.original_device_class


def _supported_features(entry: er.RegistryEntry, state: State | None) -> int:
    if state is not None and ATTR_SUPPORTED_FEATURES in state.attributes:
        return _normalize_feature_mask(state.attributes[ATTR_SUPPORTED_FEATURES]) or 0
    return _normalize_feature_mask(entry.supported_features) or 0


def _normalize_feature_mask(value: object) -> int | None:
    """Normalize a valid non-negative integer feature mask."""
    if not isinstance(value, int) or isinstance(value, bool):
        return None
    normalized = int(value)
    return normalized if normalized >= 0 else None


def _commands_and_capabilities(
    entry: er.RegistryEntry, state: State | None
) -> tuple[tuple[str, ...], Mapping[str, bool]]:
    attributes: Mapping[str, Any] = state.attributes if state is not None else {}
    supported_features = _supported_features(entry, state)
    if entry.domain == "light":
        brightness = _supports_brightness(entry, attributes)
        commands = ("turn_on", "turn_off") + (("set_brightness",) if brightness else ())
        return commands, {"brightness": brightness}
    if entry.domain == "switch":
        return ("turn_on", "turn_off"), {}
    if entry.domain == "lock":
        return ("lock", "unlock"), {"lock": True}

    cover_features = CoverEntityFeature(supported_features)
    position = bool(cover_features & CoverEntityFeature.SET_POSITION)
    tilt = bool(cover_features & CoverEntityFeature.SET_TILT_POSITION)
    commands = (
        (("open",) if cover_features & CoverEntityFeature.OPEN else ())
        + (("close",) if cover_features & CoverEntityFeature.CLOSE else ())
        + (("set_position",) if position else ())
        + (("set_tilt",) if tilt else ())
    )
    return commands, {"position": position, "tilt": tilt}


def _supports_brightness(entry: er.RegistryEntry, attributes: Mapping[str, Any]) -> bool:
    color_modes = attributes.get(ATTR_SUPPORTED_COLOR_MODES)
    if color_modes is None and entry.capabilities is not None:
        color_modes = entry.capabilities.get(ATTR_SUPPORTED_COLOR_MODES)
    if not isinstance(color_modes, (list, set, tuple, frozenset)) or not all(
        isinstance(mode, str) and mode in VALID_COLOR_MODES for mode in color_modes
    ):
        return False
    normalized_modes = {ColorMode(mode) for mode in color_modes}
    try:
        valid_supported_color_modes(normalized_modes)
    except vol.Error:
        return False
    return not COLOR_MODES_BRIGHTNESS.isdisjoint(normalized_modes)


def _supported_attributes(state: State | None) -> dict[str, object]:
    if state is None:
        return {}
    attributes: dict[str, object] = {}
    for name, maximum in (
        (_ATTR_BRIGHTNESS, 255),
        (ATTR_CURRENT_POSITION, 100),
        (ATTR_CURRENT_TILT_POSITION, 100),
    ):
        value = state.attributes.get(name)
        if type(value) is int and 0 <= value <= maximum:
            attributes[name] = value
    supported_features = _normalize_feature_mask(state.attributes.get(ATTR_SUPPORTED_FEATURES))
    if supported_features is not None:
        attributes[ATTR_SUPPORTED_FEATURES] = supported_features
    device_class = state.attributes.get(ATTR_DEVICE_CLASS)
    if isinstance(device_class, str):
        attributes[ATTR_DEVICE_CLASS] = device_class
    return attributes
