"""Typed fleet/panel config ownership and compatibility adapters."""

from __future__ import annotations

import asyncio
import json
from dataclasses import FrozenInstanceError
from types import MappingProxyType
from typing import Any

import pytest
from homeassistant.config_entries import ConfigSubentry
from homeassistant.const import CONF_NAME
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.brilliant_mqtt.broker import BrokerKind
from custom_components.brilliant_mqtt.const import (
    COMPONENT_BRIDGE,
    COMPONENT_BUS_WATCHDOG,
    COMPONENT_WIFI_WATCHDOG,
    CONF_BROKER_KIND,
    CONF_COMPONENTS,
    CONF_ENTRY_KIND,
    CONF_FEATURE_OVERRIDES,
    CONF_HA_CONTROL_DOMAINS,
    CONF_HA_CONTROL_ENABLED,
    CONF_HA_CONTROL_LABEL,
    CONF_HOST,
    CONF_HOT_POLL_SECONDS,
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
    CONF_RESYNC_SECONDS,
    CONF_ROOM_OVERRIDES,
    CONF_ROOT_PASSWORD,
    CONF_SCENE_ACTIONS,
    CONF_SCENE_PANEL,
    CONF_SCHEMA_VERSION,
    CONF_SSH_HOST_KEY,
    CONF_SSH_USERNAME,
    CONFIG_ENTRY_VERSION,
    DOMAIN,
    ENTRY_KIND_FLEET,
    OPT_AUTO_REPAIR,
    OPT_OFFLINE_GRACE_MINUTES,
    OPT_REPAIR_COOLDOWN_MINUTES,
    SUBENTRY_TYPE_PANEL,
)
from custom_components.brilliant_mqtt.entry_data import (
    EntryDataError,
    FleetConfig,
    FleetPanelStore,
    LegacyPanelStore,
    PanelConfig,
    PanelConfigStore,
)


def _fleet_data(**overrides: object) -> dict[str, Any]:
    data: dict[str, Any] = {
        CONF_ENTRY_KIND: ENTRY_KIND_FLEET,
        CONF_BROKER_KIND: BrokerKind.EXISTING_BROKER.value,
        CONF_MQTT_HOST: "MQTT.EXAMPLE.COM",
        CONF_MQTT_PORT: 1883,
        CONF_MQTT_USERNAME: "brilliant",
        CONF_MQTT_PASSWORD: "fleet-secret",
        CONF_MQTT_TLS_ENABLED: False,
        CONF_MQTT_TLS_CA: None,
        CONF_NEXT_MESH_PRIORITY: 3,
        CONF_HA_CONTROL_ENABLED: False,
        CONF_HA_CONTROL_LABEL: "brilliant",
        CONF_ROOM_OVERRIDES: {"office": "Office"},
        CONF_HA_CONTROL_DOMAINS: ["light", "switch"],
        CONF_MAX_MIRRORED_ENTITIES: 50,
        CONF_SCENE_PANEL: "panel-office",
        CONF_SCENE_ACTIONS: {"movie": {"service": "scene.turn_on"}},
        CONF_SCHEMA_VERSION: CONFIG_ENTRY_VERSION,
    }
    data.update(overrides)
    return data


def _panel_data(
    slug: str = "office",
    fingerprint: str = "SHA256:office",
    **overrides: object,
) -> dict[str, Any]:
    data: dict[str, Any] = {
        CONF_IDENTITY_FINGERPRINT: fingerprint,
        CONF_SSH_HOST_KEY: f"ssh-ed25519 AAAA-{slug}",
        CONF_HOST: f"{slug}.iot.example.com",
        CONF_SSH_USERNAME: "root",
        CONF_ROOT_PASSWORD: f"{slug}-secret",
        CONF_NAME: slug.replace("-", " ").title(),
        CONF_PANEL: slug,
        CONF_MANAGEMENT_ID: fingerprint,
        CONF_COMPONENTS: {
            COMPONENT_BRIDGE: True,
            COMPONENT_WIFI_WATCHDOG: True,
            COMPONENT_BUS_WATCHDOG: True,
        },
        CONF_FEATURE_OVERRIDES: {
            "auto_repair": True,
            "nested": {"labels": ["primary", 1, None]},
        },
        CONF_MESH_PRIORITY: 1,
        CONF_PROVISIONING_TRANSACTION_ID: f"tx-{slug}",
    }
    data.update(overrides)
    return data


def _subentry(
    slug: str = "office",
    fingerprint: str = "SHA256:office",
    **overrides: object,
) -> ConfigSubentry:
    data = _panel_data(slug, fingerprint, **overrides)
    return ConfigSubentry(
        data=MappingProxyType(data),
        subentry_id=f"panel-{slug}",
        subentry_type=SUBENTRY_TYPE_PANEL,
        title=str(data[CONF_NAME]),
        unique_id=fingerprint,
    )


_INVALID_RESILIENCE_OPTIONS: tuple[tuple[str, object], ...] = (
    (OPT_AUTO_REPAIR, 1),
    (OPT_AUTO_REPAIR, "true"),
    (OPT_OFFLINE_GRACE_MINUTES, True),
    (OPT_OFFLINE_GRACE_MINUTES, 1),
    (OPT_OFFLINE_GRACE_MINUTES, 121),
    (OPT_REPAIR_COOLDOWN_MINUTES, False),
    (OPT_REPAIR_COOLDOWN_MINUTES, 4),
    (OPT_REPAIR_COOLDOWN_MINUTES, 1441),
)


def test_typed_views_accept_exact_fleet_and_panel_ownership() -> None:
    """Fleet and panel views expose only their deliberately separate owners."""
    office = _subentry()
    kitchen = _subentry("kitchen", "SHA256:kitchen", mesh_priority=2)
    entry = MockConfigEntry(
        domain=DOMAIN,
        version=CONFIG_ENTRY_VERSION,
        data=_fleet_data(),
        subentries_data=[office.as_dict(), kitchen.as_dict()],
    )

    fleet = FleetConfig.from_entry(entry)
    panels = tuple(PanelConfig.from_subentry(item) for item in entry.subentries.values())

    assert fleet.broker.kind is BrokerKind.EXISTING_BROKER
    assert fleet.broker.host == "mqtt.example.com"
    assert fleet.next_mesh_priority == 3
    assert fleet.ha_control_domains == ("light", "switch")
    assert [panel.panel for panel in panels] == ["office", "kitchen"]
    assert isinstance(panels[0].components, MappingProxyType)
    assert isinstance(panels[0].feature_overrides, MappingProxyType)
    assert "office-secret" not in repr(panels[0])
    with pytest.raises(TypeError):
        panels[0].components[COMPONENT_BRIDGE] = False  # type: ignore[index]
    with pytest.raises(TypeError):
        panels[0].feature_overrides["new"] = True  # type: ignore[index]
    with pytest.raises(FrozenInstanceError):
        panels[0].host = "changed.example.com"  # type: ignore[misc]


def test_typed_views_accept_absent_optional_ca_without_exposing_overrides() -> None:
    """Optional CA storage may be absent and override contents stay out of repr."""
    fleet_data = _fleet_data()
    del fleet_data[CONF_MQTT_TLS_CA]
    fleet = FleetConfig.from_entry(
        MockConfigEntry(
            domain=DOMAIN,
            version=CONFIG_ENTRY_VERSION,
            data=fleet_data,
        )
    )
    panel = PanelConfig.from_subentry(
        _subentry(
            feature_overrides={
                "hue_ca_cert": "sensitive-public-trust-material",
            }
        )
    )

    assert fleet.broker.has_custom_ca is False
    assert "sensitive-public-trust-material" not in repr(panel)


@pytest.mark.parametrize(
    ("options", "expected"),
    [
        ({}, (True, 10, 60)),
        (
            {
                OPT_AUTO_REPAIR: False,
                OPT_OFFLINE_GRACE_MINUTES: 2,
                OPT_REPAIR_COOLDOWN_MINUTES: 5,
            },
            (False, 2, 5),
        ),
        (
            {
                OPT_AUTO_REPAIR: True,
                OPT_OFFLINE_GRACE_MINUTES: 120,
                OPT_REPAIR_COOLDOWN_MINUTES: 1440,
            },
            (True, 120, 1440),
        ),
    ],
)
def test_fleet_resilience_defaults_are_parsed_with_bounded_values(
    options: dict[str, object],
    expected: tuple[bool, int, int],
) -> None:
    """An exact fleet owns validated resilience defaults, with constants last."""
    fleet = FleetConfig.from_entry(
        MockConfigEntry(
            domain=DOMAIN,
            version=CONFIG_ENTRY_VERSION,
            data=_fleet_data(),
            options=options,
        )
    )

    assert (
        fleet.auto_repair,
        fleet.offline_grace_minutes,
        fleet.repair_cooldown_minutes,
    ) == expected


@pytest.mark.parametrize(("option", "value"), _INVALID_RESILIENCE_OPTIONS)
def test_fleet_resilience_defaults_reject_invalid_types_and_ranges(
    option: str,
    value: object,
) -> None:
    """Malformed fleet defaults fail at the persisted-entry boundary."""
    with pytest.raises(EntryDataError, match="invalid_fleet_entry_data"):
        FleetConfig.from_entry(
            MockConfigEntry(
                domain=DOMAIN,
                version=CONFIG_ENTRY_VERSION,
                data=_fleet_data(),
                options={option: value},
            )
        )


def test_fleet_view_rejects_legacy_config_entry_version() -> None:
    """A legacy entry cannot be mistaken for a version-4 fleet."""
    with pytest.raises(EntryDataError):
        FleetConfig.from_entry(
            MockConfigEntry(
                domain=DOMAIN,
                version=3,
                data=_fleet_data(),
            )
        )


@pytest.mark.parametrize(
    ("factory", "bad_data"),
    [
        ("fleet", _fleet_data(host="panel.example.com")),
        ("fleet", {key: value for key, value in _fleet_data().items() if key != CONF_MQTT_HOST}),
        ("panel", _panel_data(mqtt_host="mqtt.example.com")),
        ("panel", _panel_data(ha_control_enabled=False)),
        (
            "panel",
            {
                key: value
                for key, value in _panel_data().items()
                if key != CONF_IDENTITY_FINGERPRINT
            },
        ),
    ],
)
def test_typed_views_reject_cross_owner_unknown_and_missing_keys(
    factory: str, bad_data: dict[str, Any]
) -> None:
    """Broker/global keys cannot leak to panels and SSH fields cannot leak to fleets."""
    if factory == "fleet":
        entry = MockConfigEntry(
            domain=DOMAIN,
            version=CONFIG_ENTRY_VERSION,
            data=bad_data,
        )
        with pytest.raises(EntryDataError):
            FleetConfig.from_entry(entry)
        return

    subentry = ConfigSubentry(
        data=MappingProxyType(bad_data),
        subentry_type=SUBENTRY_TYPE_PANEL,
        title="Office",
        unique_id="SHA256:office",
    )
    with pytest.raises(EntryDataError):
        PanelConfig.from_subentry(subentry)


@pytest.mark.parametrize("slug", ["", "mesh", "Office", "-office", "o" * 65])
def test_new_panel_slugs_fail_closed(slug: str) -> None:
    """New fleet panel slugs use the bounded canonical allocation contract."""
    with pytest.raises(EntryDataError):
        PanelConfig.from_subentry(_subentry(slug))


def test_established_panel_may_omit_provisioning_transaction() -> None:
    """The temporary journal link is absent after runtime verification."""
    data = _panel_data()
    del data[CONF_PROVISIONING_TRANSACTION_ID]
    panel = PanelConfig.from_subentry(
        ConfigSubentry(
            data=MappingProxyType(data),
            subentry_type=SUBENTRY_TYPE_PANEL,
            title="Office",
            unique_id="SHA256:office",
        )
    )
    assert panel.provisioning_transaction_id is None


def test_provisioning_transaction_cannot_be_persisted_as_null() -> None:
    """The temporary journal link is either a real ID or absent, never a null marker."""
    with pytest.raises(EntryDataError):
        PanelConfig.from_subentry(_subentry(provisioning_transaction_id=None))


def test_panel_mesh_priority_allows_nonparticipation() -> None:
    """Priority zero remains the explicit opt-out from mesh leadership."""
    assert PanelConfig.from_subentry(_subentry(mesh_priority=0)).mesh_priority == 0


@pytest.mark.parametrize("mesh_priority", [-1, True])
def test_panel_mesh_priority_rejects_invalid_values(mesh_priority: object) -> None:
    """Negative priorities and booleans are not valid mesh allocation values."""
    with pytest.raises(EntryDataError):
        PanelConfig.from_subentry(_subentry(mesh_priority=mesh_priority))


def test_panel_agent_cadences_are_optional_bounded_integers() -> None:
    defaults = PanelConfig.from_subentry(_subentry())
    tuned = PanelConfig.from_subentry(_subentry(hot_poll_seconds=0, resync_seconds=86400))

    assert defaults.hot_poll_seconds is None
    assert defaults.resync_seconds is None
    assert tuned.hot_poll_seconds == 0
    assert tuned.resync_seconds == 86400


@pytest.mark.parametrize(
    ("key", "value"),
    (
        (CONF_HOT_POLL_SECONDS, -1),
        (CONF_HOT_POLL_SECONDS, 61),
        (CONF_HOT_POLL_SECONDS, True),
        (CONF_RESYNC_SECONDS, 59),
        (CONF_RESYNC_SECONDS, 86401),
        (CONF_RESYNC_SECONDS, False),
    ),
)
def test_panel_agent_cadences_reject_invalid_values(
    key: str,
    value: object,
) -> None:
    subentry = (
        _subentry(hot_poll_seconds=value)
        if key == CONF_HOT_POLL_SECONDS
        else _subentry(resync_seconds=value)
    )
    with pytest.raises(EntryDataError):
        PanelConfig.from_subentry(subentry)


@pytest.mark.parametrize(("option", "value"), _INVALID_RESILIENCE_OPTIONS)
def test_panel_resilience_overrides_reject_invalid_types_and_ranges(
    option: str,
    value: object,
) -> None:
    """A malformed explicit override cannot reach timer or repair arithmetic."""
    with pytest.raises(EntryDataError, match="invalid_panel_entry_data"):
        PanelConfig.from_subentry(_subentry(feature_overrides={option: value}))


def test_panel_identity_must_match_subentry_unique_id() -> None:
    """A malformed subentry cannot split its stored identity from HA uniqueness."""
    with pytest.raises(EntryDataError):
        PanelConfig.from_subentry(
            ConfigSubentry(
                data=MappingProxyType(_panel_data()),
                subentry_type=SUBENTRY_TYPE_PANEL,
                title="Office",
                unique_id="SHA256:different",
            )
        )


async def test_fleet_store_preserves_identity_and_updates_parent_subentry(
    hass: HomeAssistant,
) -> None:
    """Fleet stores reject identity drift and mutate only their owned subentry."""
    candidate = _subentry()
    entry = MockConfigEntry(
        domain=DOMAIN,
        version=CONFIG_ENTRY_VERSION,
        data=_fleet_data(),
        subentries_data=[candidate.as_dict()],
    )
    entry.add_to_hass(hass)
    stored_subentry = entry.subentries[candidate.subentry_id]
    store = FleetPanelStore(hass, entry, stored_subentry)

    assert isinstance(store, PanelConfigStore)
    assert store.panel_id == candidate.subentry_id
    assert store.subentry_id == candidate.subentry_id
    assert store.management_id == "SHA256:office"
    assert store.data is stored_subentry.data
    assert store.options == stored_subentry.data[CONF_FEATURE_OVERRIDES]
    assert isinstance(store.options, MappingProxyType)

    for immutable_change in (
        {**store.data, CONF_PANEL: "renamed"},
        {**store.data, CONF_MANAGEMENT_ID: "replacement-id"},
        {**store.data, CONF_IDENTITY_FINGERPRINT: "SHA256:replacement"},
        {**store.data, CONF_SSH_HOST_KEY: "ssh-ed25519 AAAA-replacement"},
    ):
        with pytest.raises(EntryDataError):
            store.update_data(immutable_change)
    assert stored_subentry.data[CONF_PANEL] == "office"
    assert stored_subentry.data[CONF_MANAGEMENT_ID] == "SHA256:office"

    typed = PanelConfig.from_subentry(stored_subentry)
    store.update_data(
        {
            **store.data,
            CONF_NAME: "Office Hall",
            CONF_COMPONENTS: typed.components,
            CONF_FEATURE_OVERRIDES: typed.feature_overrides,
        }
    )
    assert stored_subentry.title == "Office Hall"
    assert stored_subentry.data[CONF_NAME] == "Office Hall"
    assert not isinstance(stored_subentry.data[CONF_COMPONENTS], MappingProxyType)
    assert not isinstance(stored_subentry.data[CONF_FEATURE_OVERRIDES], MappingProxyType)
    json.dumps(dict(stored_subentry.data))

    store.update_options({"auto_repair": False})
    assert store.options == {"auto_repair": False}
    assert stored_subentry.data[CONF_FEATURE_OVERRIDES] == {"auto_repair": False}

    with pytest.raises(EntryDataError):
        store.update_data({**store.data, CONF_MQTT_HOST: "mqtt.invalid.example"})
    with pytest.raises(EntryDataError):
        store.update_options({"invalid": {"not", "json"}})
    assert CONF_MQTT_HOST not in stored_subentry.data
    assert store.options == {"auto_repair": False}

    release = asyncio.Event()

    async def background_work() -> str:
        await release.wait()
        return "done"

    task = store.async_create_background_task(
        hass,
        background_work(),
        "fleet-panel-background",
    )
    assert task in entry._background_tasks
    release.set()
    assert await task == "done"


@pytest.mark.parametrize("slug", ["", "Legacy", "-legacy"])
async def test_legacy_store_rejects_noncanonical_slug(
    hass: HomeAssistant,
    slug: str,
) -> None:
    """Legacy compatibility is unbounded but still requires canonical slug bytes."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        version=3,
        data={CONF_PANEL: slug},
    )
    entry.add_to_hass(hass)
    with pytest.raises(EntryDataError):
        LegacyPanelStore(hass, entry)


@pytest.mark.parametrize(
    ("key", "value"),
    (
        (CONF_HOT_POLL_SECONDS, 5.9),
        (CONF_RESYNC_SECONDS, 0),
    ),
    ids=("float", "out-of-range"),
)
async def test_legacy_store_rejects_tampered_agent_cadences(
    hass: HomeAssistant,
    key: str,
    value: object,
) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        version=3,
        data={
            CONF_PANEL: "legacy",
            key: value,
        },
    )
    entry.add_to_hass(hass)

    with pytest.raises(EntryDataError, match="invalid_legacy_panel_entry_data"):
        LegacyPanelStore(hass, entry)


async def test_legacy_store_round_trips_valid_agent_cadences(
    hass: HomeAssistant,
) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        version=3,
        data={
            CONF_PANEL: "legacy",
            CONF_HOT_POLL_SECONDS: 5,
            CONF_RESYNC_SECONDS: 900,
        },
    )
    entry.add_to_hass(hass)

    store = LegacyPanelStore(hass, entry)

    assert store.data[CONF_HOT_POLL_SECONDS] == 5
    assert store.data[CONF_RESYNC_SECONDS] == 900


async def test_legacy_store_loads_absent_agent_cadences_as_none(
    hass: HomeAssistant,
) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        version=3,
        data={CONF_PANEL: "legacy"},
    )
    entry.add_to_hass(hass)

    store = LegacyPanelStore(hass, entry)

    assert store.data.get(CONF_HOT_POLL_SECONDS) is None
    assert store.data.get(CONF_RESYNC_SECONDS) is None


async def test_legacy_store_exposes_version_three_shape_unchanged(
    hass: HomeAssistant,
) -> None:
    """Legacy compatibility never normalizes an existing canonical long slug."""
    long_slug = "legacy-" + ("a" * 80)
    legacy_data = {
        CONF_PANEL: long_slug,
        CONF_HOST: "legacy.iot.example.com",
        CONF_ROOT_PASSWORD: "legacy-secret",
        CONF_MQTT_HOST: "mqtt.example.com",
        CONF_MQTT_PORT: 1883,
        CONF_MQTT_USERNAME: "brilliant",
        CONF_MQTT_PASSWORD: "fleet-secret",
        CONF_COMPONENTS: {COMPONENT_BRIDGE: True},
        "legacy_extension": {"preserve": ["exactly"]},
    }
    legacy_options = {"auto_repair": True, "legacy_option": "unchanged"}
    entry = MockConfigEntry(
        domain=DOMAIN,
        version=3,
        data=legacy_data,
        options=legacy_options,
    )
    entry.add_to_hass(hass)
    store = LegacyPanelStore(hass, entry)

    assert isinstance(store, PanelConfigStore)
    assert store.panel_id == entry.entry_id
    assert store.subentry_id is None
    assert store.management_id == entry.entry_id
    assert store.data is entry.data
    assert store.options is entry.options
    assert store.data[CONF_PANEL] == long_slug
    assert dict(store.data) == legacy_data
    assert dict(store.options) == legacy_options

    store.update_data({**store.data, CONF_NAME: "Legacy Panel"})
    store.update_options({**store.options, "auto_repair": False})
    assert entry.data[CONF_NAME] == "Legacy Panel"
    assert entry.options["auto_repair"] is False

    release = asyncio.Event()

    async def background_work() -> str:
        await release.wait()
        return "done"

    task = store.async_create_background_task(
        hass,
        background_work(),
        "legacy-panel-background",
    )
    assert task in entry._background_tasks
    release.set()
    assert await task == "done"
