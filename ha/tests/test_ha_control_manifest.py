"""Tests for the Home Assistant-owned Brilliant control manifest."""

from __future__ import annotations

import json
from collections.abc import Mapping, MutableMapping
from typing import cast

import pytest
from homeassistant.components.cover import CoverEntityFeature
from homeassistant.components.light import ATTR_SUPPORTED_COLOR_MODES
from homeassistant.components.lock import LockEntityFeature
from homeassistant.const import (
    ATTR_DEVICE_CLASS,
    ATTR_FRIENDLY_NAME,
    ATTR_SUPPORTED_FEATURES,
    STATE_UNAVAILABLE,
    STATE_UNKNOWN,
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers import area_registry as ar
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import label_registry as lr
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.brilliant_mqtt.ha_control_manifest import (
    ControlSettings,
    build_manifest,
    build_state_payload,
)
from custom_components.brilliant_mqtt.ha_control_protocol import (
    MAPPING_VERSION,
    SCHEMA_VERSION,
    stable_id,
)

GENERATED_AT_MS = 1_700_000_000_000


def settings(
    *,
    label_name: str = "brilliant",
    room_overrides: Mapping[str, str] | None = None,
    enabled_domains: frozenset[str] = frozenset({"light", "switch", "lock", "cover"}),
    maximum_entities: int = 100,
) -> ControlSettings:
    """Return control settings suitable for focused manifest tests."""
    return ControlSettings(
        label_name=label_name,
        room_overrides=room_overrides or {},
        enabled_domains=enabled_domains,
        maximum_entities=maximum_entities,
    )


def _label(hass: HomeAssistant, name: str = "brilliant") -> str:
    return lr.async_get(hass).async_create(name).label_id


def _entity(
    hass: HomeAssistant,
    entity_id: str,
    *,
    label_id: str | None = None,
    device_id: str | None = None,
    area_id: str | None = None,
    disabled: bool = False,
    device_class: str | None = None,
    supported_features: int = 0,
) -> er.RegistryEntry:
    registry = er.async_get(hass)
    domain, object_id = entity_id.split(".", maxsplit=1)
    entry = registry.async_get_or_create(
        domain,
        "test",
        object_id,
        device_id=device_id,
        original_device_class=device_class,
        original_name=object_id.replace("_", " ").title(),
        supported_features=supported_features,
        suggested_object_id=object_id,
    )
    return registry.async_update_entity(
        entry.entity_id,
        area_id=area_id,
        disabled_by=er.RegistryEntryDisabler.USER if disabled else None,
        labels={label_id} if label_id is not None else set(),
    )


def _device(hass: HomeAssistant, *, area_id: str | None = None) -> dr.DeviceEntry:
    config_entry = MockConfigEntry(domain="test")
    config_entry.add_to_hass(hass)
    registry = dr.async_get(hass)
    device = registry.async_get_or_create(
        config_entry_id=config_entry.entry_id,
        identifiers={("test", config_entry.entry_id)},
        name="Test device",
    )
    updated = registry.async_update_device(device.id, area_id=area_id)
    assert updated is not None
    return updated


def _set_registry_supported_features(
    hass: HomeAssistant, entity_id: str, value: object
) -> None:
    er.async_get(hass).async_update_entity(
        entity_id,
        supported_features=cast(int, value),
    )


async def test_entity_area_precedes_device_area(hass: HomeAssistant) -> None:
    label_id = _label(hass)
    areas = ar.async_get(hass)
    office = areas.async_get_or_create("Office")
    backyard = areas.async_get_or_create("Backyard")
    device = _device(hass, area_id=backyard.id)
    entry = _entity(
        hass,
        "light.desk",
        label_id=label_id,
        device_id=device.id,
        area_id=office.id,
    )
    hass.states.async_set(entry.entity_id, "on", {ATTR_FRIENDLY_NAME: "Desk"})

    snapshot = build_manifest(hass, settings(label_name="brilliant"), 7, GENERATED_AT_MS)

    assert snapshot.entities[0].ha_area == "Office"
    assert snapshot.entities[0].brilliant_room == "Office"


async def test_unmatched_override_is_case_insensitive(hass: HomeAssistant) -> None:
    label_id = _label(hass)
    backyard = ar.async_get(hass).async_get_or_create(" Back Yard ")
    entry = _entity(hass, "switch.patio", label_id=label_id, area_id=backyard.id)
    hass.states.async_set(entry.entity_id, "off")

    snapshot = build_manifest(
        hass,
        settings(label_name="brilliant", room_overrides={"back yard": "Backyard"}),
        1,
        GENERATED_AT_MS,
    )

    assert snapshot.entities[0].brilliant_room == "Backyard"


async def test_only_entity_labels_select(hass: HomeAssistant) -> None:
    label_id = _label(hass)
    selected = _entity(hass, "switch.entity_label", label_id=label_id)
    device = _device(hass)
    dr.async_get(hass).async_update_device(device.id, labels={label_id})
    _entity(hass, "switch.device_label", device_id=device.id)

    snapshot = build_manifest(hass, settings(), 1, GENERATED_AT_MS)

    assert [entity.entity_id for entity in snapshot.entities] == [selected.entity_id]


async def test_disabled_unavailable_unknown_and_missing_states_remain(
    hass: HomeAssistant,
) -> None:
    label_id = _label(hass)
    disabled = _entity(hass, "switch.disabled", label_id=label_id, disabled=True)
    unavailable = _entity(hass, "switch.unavailable", label_id=label_id)
    unknown = _entity(hass, "switch.unknown", label_id=label_id)
    missing = _entity(hass, "switch.missing", label_id=label_id)
    hass.states.async_set(unavailable.entity_id, STATE_UNAVAILABLE)
    hass.states.async_set(unknown.entity_id, STATE_UNKNOWN)

    snapshot = build_manifest(hass, settings(), 1, GENERATED_AT_MS)
    payloads = {
        entity.entity_id: build_state_payload(
            hass.states.get(entity.entity_id), entity, 4, GENERATED_AT_MS
        )
        for entity in snapshot.entities
    }

    assert set(payloads) == {
        disabled.entity_id,
        unavailable.entity_id,
        unknown.entity_id,
        missing.entity_id,
    }
    assert payloads[disabled.entity_id]["available"] is False
    assert payloads[unavailable.entity_id]["available"] is False
    assert payloads[unknown.entity_id]["available"] is False
    assert payloads[missing.entity_id]["available"] is False
    assert payloads[missing.entity_id]["state"] == STATE_UNAVAILABLE


async def test_unsupported_domains_are_reported_but_excluded(hass: HomeAssistant) -> None:
    label_id = _label(hass)
    _entity(hass, "sensor.temperature", label_id=label_id)
    light = _entity(hass, "light.lamp", label_id=label_id)

    snapshot = build_manifest(
        hass,
        settings(enabled_domains=frozenset({"light", "sensor"})),
        1,
        GENERATED_AT_MS,
    )

    assert [entity.entity_id for entity in snapshot.entities] == [light.entity_id]
    assert snapshot.unsupported_domains == ("sensor",)


async def test_maximum_entities_truncates_by_entity_id(hass: HomeAssistant) -> None:
    label_id = _label(hass)
    for entity_id in ("switch.zulu", "switch.alpha", "light.middle"):
        _entity(hass, entity_id, label_id=label_id)

    snapshot = build_manifest(hass, settings(maximum_entities=2), 1, GENERATED_AT_MS)

    assert [entity.entity_id for entity in snapshot.entities] == [
        "light.middle",
        "switch.alpha",
    ]


async def test_commands_and_capabilities_follow_live_support(hass: HomeAssistant) -> None:
    label_id = _label(hass)
    light = _entity(hass, "light.dimmer", label_id=label_id)
    cover = _entity(
        hass,
        "cover.shade",
        label_id=label_id,
        supported_features=int(
            CoverEntityFeature.OPEN
            | CoverEntityFeature.CLOSE
            | CoverEntityFeature.SET_POSITION
            | CoverEntityFeature.SET_TILT_POSITION
        ),
    )
    lock = _entity(
        hass,
        "lock.front_door",
        label_id=label_id,
        supported_features=int(LockEntityFeature.OPEN),
    )
    hass.states.async_set(
        light.entity_id,
        "on",
        {
            "brightness": 128,
            ATTR_SUPPORTED_COLOR_MODES: ["brightness"],
            ATTR_SUPPORTED_FEATURES: 0,
        },
    )
    hass.states.async_set(
        cover.entity_id,
        "open",
        {
            "current_position": 42,
            "current_tilt_position": 25,
            ATTR_SUPPORTED_FEATURES: cover.supported_features,
        },
    )
    hass.states.async_set(
        lock.entity_id,
        "locked",
        {ATTR_SUPPORTED_FEATURES: lock.supported_features},
    )

    snapshot = build_manifest(hass, settings(), 1, GENERATED_AT_MS)
    entities = {entity.entity_id: entity for entity in snapshot.entities}

    assert entities[light.entity_id].commands == ("turn_on", "turn_off", "set_brightness")
    assert entities[light.entity_id].capabilities == {"brightness": True}
    assert entities[cover.entity_id].commands == (
        "open",
        "close",
        "set_position",
        "set_tilt",
    )
    assert entities[cover.entity_id].capabilities == {"position": True, "tilt": True}
    assert entities[lock.entity_id].commands == ("lock", "unlock")
    assert entities[lock.entity_id].capabilities == {"lock": True}


async def test_negative_live_feature_mask_advertises_no_cover_commands(
    hass: HomeAssistant,
) -> None:
    label_id = _label(hass)
    entry = _entity(
        hass,
        "cover.shade",
        label_id=label_id,
        supported_features=int(CoverEntityFeature.OPEN | CoverEntityFeature.CLOSE),
    )
    hass.states.async_set(entry.entity_id, "open", {ATTR_SUPPORTED_FEATURES: -1})

    entity = build_manifest(hass, settings(), 1, GENERATED_AT_MS).entities[0]

    assert entity.commands == ()
    assert entity.capabilities == {"position": False, "tilt": False}


async def test_negative_registry_feature_mask_advertises_no_cover_commands(
    hass: HomeAssistant,
) -> None:
    label_id = _label(hass)
    _entity(hass, "cover.shade", label_id=label_id, supported_features=-1)

    entity = build_manifest(hass, settings(), 1, GENERATED_AT_MS).entities[0]

    assert entity.commands == ()
    assert entity.capabilities == {"position": False, "tilt": False}


@pytest.mark.parametrize(
    "invalid_mask",
    [True, "3", [1, 2], {"open": True}],
    ids=["bool", "string", "list", "mapping"],
)
async def test_malformed_live_feature_mask_does_not_fall_back_to_registry(
    hass: HomeAssistant,
    invalid_mask: object,
) -> None:
    label_id = _label(hass)
    entry = _entity(
        hass,
        "cover.shade",
        label_id=label_id,
        supported_features=int(CoverEntityFeature.OPEN | CoverEntityFeature.CLOSE),
    )
    hass.states.async_set(entry.entity_id, "open", {ATTR_SUPPORTED_FEATURES: invalid_mask})

    entity = build_manifest(hass, settings(), 1, GENERATED_AT_MS).entities[0]

    assert entity.commands == ()
    assert entity.capabilities == {"position": False, "tilt": False}


@pytest.mark.parametrize(
    "invalid_mask",
    [True, "3", [1, 2], {"open": True}],
    ids=["bool", "string", "list", "mapping"],
)
async def test_malformed_registry_feature_mask_advertises_no_cover_commands(
    hass: HomeAssistant,
    invalid_mask: object,
) -> None:
    label_id = _label(hass)
    entry = _entity(hass, "cover.shade", label_id=label_id)
    _set_registry_supported_features(hass, entry.entity_id, invalid_mask)

    entity = build_manifest(hass, settings(), 1, GENERATED_AT_MS).entities[0]

    assert entity.commands == ()
    assert entity.capabilities == {"position": False, "tilt": False}


@pytest.mark.parametrize(
    ("attributes", "supports_brightness"),
    [
        ({ATTR_SUPPORTED_COLOR_MODES: ["brightness"]}, True),
        ({ATTR_SUPPORTED_COLOR_MODES: ["hs"]}, True),
        ({ATTR_SUPPORTED_COLOR_MODES: ["onoff"]}, False),
        ({ATTR_SUPPORTED_COLOR_MODES: ["unknown"]}, False),
        ({ATTR_SUPPORTED_COLOR_MODES: ["invalid"]}, False),
        ({ATTR_SUPPORTED_COLOR_MODES: "brightness"}, False),
        ({ATTR_SUPPORTED_COLOR_MODES: {"brightness": True}}, False),
        ({ATTR_SUPPORTED_COLOR_MODES: [1, None]}, False),
        ({"brightness": 128}, False),
    ],
)
async def test_light_brightness_capability_fails_closed(
    hass: HomeAssistant,
    attributes: dict[str, object],
    supports_brightness: bool,
) -> None:
    label_id = _label(hass)
    entry = _entity(hass, "light.test", label_id=label_id)
    hass.states.async_set(entry.entity_id, "on", attributes)

    entity = build_manifest(hass, settings(), 1, GENERATED_AT_MS).entities[0]

    assert entity.capabilities == {"brightness": supports_brightness}
    assert ("set_brightness" in entity.commands) is supports_brightness


async def test_state_payload_is_versioned_and_attribute_allowlisted(
    hass: HomeAssistant,
) -> None:
    label_id = _label(hass)
    entry = _entity(hass, "cover.garage", label_id=label_id, device_class="garage")
    hass.states.async_set(
        entry.entity_id,
        "open",
        {
            "brightness": 255,
            "current_position": 0,
            "current_tilt_position": 100,
            ATTR_DEVICE_CLASS: "garage",
            ATTR_SUPPORTED_FEATURES: 0,
            "access_token": "must-not-leak",
        },
    )
    entity = build_manifest(hass, settings(), 9, GENERATED_AT_MS).entities[0]

    payload = build_state_payload(hass.states.get(entry.entity_id), entity, 12, GENERATED_AT_MS)

    assert payload == {
        "schema_version": SCHEMA_VERSION,
        "mapping_version": MAPPING_VERSION,
        "stable_id": stable_id(entry.entity_id),
        "entity_id": entry.entity_id,
        "sequence": 12,
        "generated_at_ms": GENERATED_AT_MS,
        "available": True,
        "state": "open",
        "attributes": {
            "brightness": 255,
            "current_position": 0,
            "current_tilt_position": 100,
            "device_class": "garage",
            "supported_features": 0,
        },
    }


async def test_state_payload_normalizes_intflag_supported_features(
    hass: HomeAssistant,
) -> None:
    label_id = _label(hass)
    entry = _entity(hass, "cover.shade", label_id=label_id)
    feature_mask = CoverEntityFeature.OPEN | CoverEntityFeature.CLOSE
    hass.states.async_set(entry.entity_id, "open", {ATTR_SUPPORTED_FEATURES: feature_mask})
    entity = build_manifest(hass, settings(), 1, GENERATED_AT_MS).entities[0]

    assert entity.commands == ("open", "close")
    assert entity.capabilities == {"position": False, "tilt": False}

    payload = build_state_payload(hass.states.get(entry.entity_id), entity, 1, GENERATED_AT_MS)

    attributes = cast(dict[str, object], payload["attributes"])
    assert attributes == {"supported_features": 3}
    assert type(attributes["supported_features"]) is int


@pytest.mark.parametrize(
    ("attribute", "invalid_value"),
    [
        ("brightness", True),
        ("brightness", -1),
        ("brightness", 256),
        ("brightness", {}),
        ("brightness", []),
        ("current_position", True),
        ("current_position", -1),
        ("current_position", 101),
        ("current_position", {}),
        ("current_position", []),
        ("current_tilt_position", True),
        ("current_tilt_position", -1),
        ("current_tilt_position", 101),
        ("current_tilt_position", {}),
        ("current_tilt_position", []),
        ("supported_features", True),
        ("supported_features", -1),
        ("supported_features", {}),
        ("supported_features", []),
        ("device_class", True),
        ("device_class", {}),
        ("device_class", []),
    ],
)
async def test_state_payload_omits_invalid_allowlisted_values(
    hass: HomeAssistant,
    attribute: str,
    invalid_value: object,
) -> None:
    label_id = _label(hass)
    entry = _entity(hass, "switch.test", label_id=label_id)
    hass.states.async_set(entry.entity_id, "on", {attribute: invalid_value})
    entity = build_manifest(hass, settings(), 1, GENERATED_AT_MS).entities[0]

    payload = build_state_payload(hass.states.get(entry.entity_id), entity, 1, GENERATED_AT_MS)

    assert payload["attributes"] == {}
    json.dumps(payload)


async def test_snapshot_contains_complete_versioned_metadata(hass: HomeAssistant) -> None:
    label_id = _label(hass)
    entry = _entity(hass, "switch.coffee", label_id=label_id)
    hass.states.async_set(entry.entity_id, "on", {ATTR_FRIENDLY_NAME: "Coffee Maker"})

    snapshot = build_manifest(hass, settings(), 7, GENERATED_AT_MS)

    assert snapshot.schema_version == SCHEMA_VERSION
    assert snapshot.mapping_version == MAPPING_VERSION
    assert snapshot.revision == 7
    assert snapshot.generated_at_ms == GENERATED_AT_MS
    assert snapshot.entities[0].stable_id == stable_id(entry.entity_id)
    assert snapshot.entities[0].friendly_name == "Coffee Maker"


async def test_manifest_mapping_fields_are_immutable(hass: HomeAssistant) -> None:
    label_id = _label(hass)
    entry = _entity(hass, "switch.coffee", label_id=label_id)
    control_settings = settings(room_overrides={"Office": "Office"})
    hass.states.async_set(entry.entity_id, "on")
    entity = build_manifest(hass, control_settings, 1, GENERATED_AT_MS).entities[0]

    with pytest.raises(TypeError):
        cast(MutableMapping[str, str], control_settings.room_overrides)["Office"] = "Kitchen"
    with pytest.raises(TypeError):
        cast(MutableMapping[str, bool], entity.capabilities)["lock"] = False
