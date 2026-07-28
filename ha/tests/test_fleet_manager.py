"""FleetManager owns every panel runtime below one Brilliant config entry."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from types import MappingProxyType
from typing import Any
from unittest.mock import Mock, patch

import pytest
from homeassistant.config_entries import ConfigSubentry
from homeassistant.const import CONF_NAME
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers import issue_registry as ir
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
    ENTRY_KIND_LEGACY_PENDING_CONSOLIDATION,
    SUBENTRY_TYPE_PANEL,
)
from custom_components.brilliant_mqtt.entry_data import (
    EntryDataError,
    FleetPanelStore,
    LegacyPanelStore,
)
from custom_components.brilliant_mqtt.fleet_manager import FleetManager
from custom_components.brilliant_mqtt.manager import PanelManager

_OFFICE_PUBLIC_KEY = (
    "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIKIykuTed7zNwJwn20eCelcKcHKJ9c/pGFfvulRWazuC"
)
_OFFICE_FINGERPRINT = "SHA256:JfCon51dCgE/yWGkyroh3Ne+ONLMm6QmHMQnEoPSLx0"
_OTHER_PUBLIC_KEY = (
    "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIG/koBYdTnHujqIpcXlQkQqzGBoZJ6Y4rm22iGIdAu4B"
)
_OTHER_FINGERPRINT = "SHA256:8mIRtm2GlHfcML0pUZInHQk3nT+hlkTq4k2FGR/Y0KM"
_THIRD_PUBLIC_KEY = (
    "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFLOqEG+HLFFAkvglS6WB0dqE/xuFTDmEIFTwEKMj6xI"
)
_THIRD_FINGERPRINT = "SHA256:HvSbMqGcnEU1+Bvnip8Qw0LRDo5dFR0SBrUmf8Haxzs"


@pytest.fixture(autouse=True)
def _mqtt_connection_status(monkeypatch: pytest.MonkeyPatch) -> None:
    """FleetManager's production caller has already proven MQTT is loaded."""
    monkeypatch.setattr(
        "custom_components.brilliant_mqtt.fleet_manager.mqtt.is_connected",
        lambda hass: True,
    )
    monkeypatch.setattr(
        "custom_components.brilliant_mqtt.fleet_manager.mqtt.async_subscribe_connection_status",
        lambda hass, callback: Mock(),
    )


def _fleet_data() -> dict[str, Any]:
    return {
        CONF_ENTRY_KIND: ENTRY_KIND_FLEET,
        CONF_BROKER_KIND: BrokerKind.EXISTING_BROKER.value,
        CONF_MQTT_HOST: "mqtt.example.com",
        CONF_MQTT_PORT: 1883,
        CONF_MQTT_USERNAME: "brilliant",
        CONF_MQTT_PASSWORD: "fleet-secret",
        CONF_MQTT_TLS_ENABLED: False,
        CONF_MQTT_TLS_CA: None,
        CONF_NEXT_MESH_PRIORITY: 3,
        CONF_HA_CONTROL_ENABLED: False,
        CONF_HA_CONTROL_LABEL: "brilliant",
        CONF_ROOM_OVERRIDES: {},
        CONF_HA_CONTROL_DOMAINS: ["light", "switch"],
        CONF_MAX_MIRRORED_ENTITIES: 50,
        CONF_SCENE_PANEL: "panel-office",
        CONF_SCENE_ACTIONS: {},
        CONF_SCHEMA_VERSION: CONFIG_ENTRY_VERSION,
    }


def _panel(
    slug: str,
    fingerprint: str,
    *,
    subentry_id: str,
    host: str | None = None,
    mesh_priority: int = 1,
) -> ConfigSubentry:
    if fingerprint == "SHA256:office":
        public_key, identity_fingerprint = _OFFICE_PUBLIC_KEY, _OFFICE_FINGERPRINT
    elif fingerprint == "SHA256:bedroom":
        public_key, identity_fingerprint = _THIRD_PUBLIC_KEY, _THIRD_FINGERPRINT
    else:
        public_key, identity_fingerprint = _OTHER_PUBLIC_KEY, _OTHER_FINGERPRINT
    return ConfigSubentry(
        data=MappingProxyType(
            {
                CONF_IDENTITY_FINGERPRINT: identity_fingerprint,
                CONF_SSH_HOST_KEY: public_key,
                CONF_HOST: host or f"{slug}.example.com",
                CONF_SSH_USERNAME: "root",
                CONF_ROOT_PASSWORD: f"{slug}-secret",
                CONF_NAME: slug.title(),
                CONF_PANEL: slug,
                CONF_MANAGEMENT_ID: fingerprint,
                CONF_COMPONENTS: {
                    COMPONENT_BRIDGE: True,
                    COMPONENT_WIFI_WATCHDOG: True,
                    COMPONENT_BUS_WATCHDOG: True,
                },
                CONF_FEATURE_OVERRIDES: {"auto_repair": True},
                CONF_MESH_PRIORITY: mesh_priority,
            }
        ),
        subentry_id=subentry_id,
        subentry_type=SUBENTRY_TYPE_PANEL,
        title=slug.title(),
        unique_id=identity_fingerprint,
    )


def _fleet_entry(*panels: ConfigSubentry) -> MockConfigEntry:
    return MockConfigEntry(
        domain=DOMAIN,
        version=CONFIG_ENTRY_VERSION,
        data=_fleet_data(),
        subentries_data=[panel.as_dict() for panel in panels],
    )


def _legacy_entry() -> MockConfigEntry:
    return MockConfigEntry(
        domain=DOMAIN,
        version=CONFIG_ENTRY_VERSION,
        data={
            CONF_ENTRY_KIND: ENTRY_KIND_LEGACY_PENDING_CONSOLIDATION,
            CONF_HOST: "legacy.example.com",
            CONF_ROOT_PASSWORD: "legacy-secret",
            CONF_SSH_HOST_KEY: "ssh-ed25519 AAAA-legacy",
            CONF_PANEL: "legacy",
            CONF_MESH_PRIORITY: 1,
            CONF_MQTT_HOST: "mqtt.example.com",
            CONF_MQTT_PORT: 1883,
            CONF_MQTT_USERNAME: "brilliant",
            CONF_MQTT_PASSWORD: "fleet-secret",
            CONF_COMPONENTS: {COMPONENT_BRIDGE: True},
            CONF_HA_CONTROL_ENABLED: False,
            CONF_HA_CONTROL_LABEL: "brilliant",
            CONF_ROOM_OVERRIDES: {},
            CONF_HA_CONTROL_DOMAINS: ["light", "switch"],
            CONF_MAX_MIRRORED_ENTITIES: 50,
            CONF_SCENE_PANEL: "legacy",
            CONF_SCENE_ACTIONS: {},
        },
    )


async def _noop_setup(manager: PanelManager) -> None:
    del manager


async def _noop_shutdown(manager: PanelManager) -> None:
    del manager


async def test_fleet_builds_every_store_before_start_and_shares_one_ssh_lock(
    hass: HomeAssistant,
) -> None:
    """A malformed later panel cannot leave an earlier panel half-started."""
    office = _panel("office", "SHA256:office", subentry_id="panel-office")
    candidate = _panel("kitchen", "SHA256:kitchen", subentry_id="panel-kitchen")
    malformed = ConfigSubentry(
        data=MappingProxyType({**candidate.data, CONF_PANEL: "Kitchen"}),
        subentry_id=candidate.subentry_id,
        subentry_type=candidate.subentry_type,
        title=candidate.title,
        unique_id=candidate.unique_id,
    )
    entry = _fleet_entry(office, malformed)
    manager = FleetManager(hass, entry)
    starts: list[str] = []

    async def record_start(panel: PanelManager) -> None:
        starts.append(panel.panel)

    with patch.object(PanelManager, "async_setup", record_start):
        with pytest.raises(EntryDataError):
            await manager.async_setup()

    assert starts == []
    assert manager.panels == {}

    entry = _fleet_entry(
        office,
        _panel("kitchen", "SHA256:kitchen", subentry_id="panel-kitchen"),
    )
    manager = FleetManager(hass, entry)
    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await manager.async_setup()
        assert set(manager.panels) == {"panel-office", "panel-kitchen"}
        assert all(isinstance(panel.store, FleetPanelStore) for panel in manager.panels.values())
        locks = {id(panel._ssh_lock) for panel in manager.panels.values()}
        assert len(locks) == 1
        with pytest.raises(TypeError):
            manager.panels["third"] = manager.panels["panel-office"]  # type: ignore[index]
        await manager.async_shutdown()


@pytest.mark.parametrize(
    ("second_slug", "second_fingerprint", "error_code"),
    [
        ("office", "SHA256:other", "duplicate_panel_slug"),
        ("kitchen", "SHA256:office", "duplicate_panel_fingerprint"),
    ],
)
async def test_duplicate_identity_fails_before_any_manager_starts(
    hass: HomeAssistant,
    second_slug: str,
    second_fingerprint: str,
    error_code: str,
) -> None:
    entry = _fleet_entry(
        _panel("office", "SHA256:office", subentry_id="panel-office"),
        _panel(second_slug, second_fingerprint, subentry_id="panel-second"),
    )
    starts: list[str] = []

    async def record_start(manager: PanelManager) -> None:
        starts.append(manager.panel)

    with patch.object(PanelManager, "async_setup", record_start):
        with pytest.raises(EntryDataError, match=error_code):
            await FleetManager(hass, entry).async_setup()

    assert starts == []


async def test_duplicate_management_id_fails_before_manager_start(
    hass: HomeAssistant,
) -> None:
    office = _panel("office", "SHA256:office", subentry_id="panel-office")
    kitchen = _panel("kitchen", "SHA256:kitchen", subentry_id="panel-kitchen")
    colliding = ConfigSubentry(
        data=MappingProxyType(
            {**kitchen.data, CONF_MANAGEMENT_ID: office.data[CONF_MANAGEMENT_ID]}
        ),
        subentry_id=kitchen.subentry_id,
        subentry_type=kitchen.subentry_type,
        title=kitchen.title,
        unique_id=kitchen.unique_id,
    )
    starts: list[str] = []

    async def record_start(manager: PanelManager) -> None:
        starts.append(manager.panel)

    with patch.object(PanelManager, "async_setup", record_start):
        with pytest.raises(EntryDataError, match="duplicate_panel_management_id"):
            await FleetManager(hass, _fleet_entry(office, colliding)).async_setup()

    assert starts == []


async def test_management_id_may_differ_from_rebound_identity_fingerprint(
    hass: HomeAssistant,
) -> None:
    """Explicit rebind preserves management identity while replacing SSH identity."""
    panel = _panel("office", "SHA256:replacement", subentry_id="panel-office")
    rebound = ConfigSubentry(
        data=MappingProxyType({**panel.data, CONF_MANAGEMENT_ID: "SHA256:original-management-id"}),
        subentry_id=panel.subentry_id,
        subentry_type=panel.subentry_type,
        title=panel.title,
        unique_id=panel.unique_id,
    )
    fleet = FleetManager(hass, _fleet_entry(rebound))

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        assert fleet.panels["panel-office"].management_id == "SHA256:original-management-id"
        await fleet.async_shutdown()


@pytest.mark.parametrize(
    ("data_override", "unique_id", "error_code"),
    [
        (
            {CONF_IDENTITY_FINGERPRINT: "SHA256:forged"},
            "SHA256:forged",
            "invalid_panel_identity",
        ),
        (
            {CONF_SSH_USERNAME: "admin"},
            _OFFICE_FINGERPRINT,
            "invalid_panel_ssh_username",
        ),
    ],
)
async def test_panel_identity_and_root_principal_are_validated_before_start(
    hass: HomeAssistant,
    data_override: dict[str, str],
    unique_id: str,
    error_code: str,
) -> None:
    """Persisted identity must match its key and the implemented root-only shell."""
    panel = _panel("office", "SHA256:office", subentry_id="panel-office")
    malformed = ConfigSubentry(
        data=MappingProxyType({**panel.data, **data_override}),
        subentry_id=panel.subentry_id,
        subentry_type=panel.subentry_type,
        title=panel.title,
        unique_id=unique_id,
    )
    starts: list[str] = []

    async def record_start(manager: PanelManager) -> None:
        starts.append(manager.panel)

    with patch.object(PanelManager, "async_setup", record_start):
        with pytest.raises(EntryDataError, match=error_code):
            await FleetManager(hass, _fleet_entry(malformed)).async_setup()

    assert starts == []


@pytest.mark.parametrize(
    ("entry_kind", "version", "error_code"),
    [
        (None, CONFIG_ENTRY_VERSION, "invalid_entry_kind"),
        ("unknown", CONFIG_ENTRY_VERSION, "invalid_entry_kind"),
        (
            ENTRY_KIND_LEGACY_PENDING_CONSOLIDATION,
            CONFIG_ENTRY_VERSION - 1,
            "invalid_entry_version",
        ),
        (
            ENTRY_KIND_LEGACY_PENDING_CONSOLIDATION,
            CONFIG_ENTRY_VERSION + 1,
            "invalid_entry_version",
        ),
    ],
)
async def test_entry_envelope_rejects_unknown_kind_or_wrong_version_before_start(
    hass: HomeAssistant,
    entry_kind: str | None,
    version: int,
    error_code: str,
) -> None:
    """Only exact version-4 fleet and migrated-legacy envelopes reach a manager."""
    legacy = _legacy_entry()
    data = dict(legacy.data)
    if entry_kind is None:
        data.pop(CONF_ENTRY_KIND)
    else:
        data[CONF_ENTRY_KIND] = entry_kind
    entry = MockConfigEntry(domain=DOMAIN, version=version, data=data)
    starts: list[str] = []

    async def record_start(manager: PanelManager) -> None:
        starts.append(manager.panel)

    with patch.object(PanelManager, "async_setup", record_start):
        with pytest.raises(EntryDataError, match=error_code):
            await FleetManager(hass, entry).async_setup()

    assert starts == []


async def test_fleet_schema_version_is_rejected_before_manager_start(
    hass: HomeAssistant,
) -> None:
    """The persisted fleet schema marker must exactly match entry version 4."""
    data = _fleet_data()
    data[CONF_SCHEMA_VERSION] = CONFIG_ENTRY_VERSION - 1
    entry = MockConfigEntry(
        domain=DOMAIN,
        version=CONFIG_ENTRY_VERSION,
        data=data,
        subentries_data=[
            _panel(
                "office",
                "SHA256:office",
                subentry_id="panel-office",
            ).as_dict()
        ],
    )
    starts: list[str] = []

    async def record_start(manager: PanelManager) -> None:
        starts.append(manager.panel)

    with patch.object(PanelManager, "async_setup", record_start):
        with pytest.raises(EntryDataError, match="invalid_fleet_entry_data"):
            await FleetManager(hass, entry).async_setup()

    assert starts == []


async def test_nonempty_fleet_rejects_scene_owner_outside_its_subentries(
    hass: HomeAssistant,
) -> None:
    """Runtime fails closed if a persisted scene owner is not in this fleet."""
    data = _fleet_data()
    data[CONF_SCENE_PANEL] = "panel-missing"
    entry = MockConfigEntry(
        domain=DOMAIN,
        version=CONFIG_ENTRY_VERSION,
        data=data,
        subentries_data=[
            _panel(
                "office",
                "SHA256:office",
                subentry_id="panel-office",
            ).as_dict()
        ],
    )

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        pytest.raises(EntryDataError, match="invalid_scene_panel"),
    ):
        await FleetManager(hass, entry).async_setup()


async def test_empty_fleet_allows_inert_scene_owner_until_first_panel(
    hass: HomeAssistant,
) -> None:
    """Removing the final panel may leave an inert owner for a later atomic update."""
    fleet = FleetManager(hass, _fleet_entry())

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        assert fleet.panels == {}
        await fleet.async_shutdown()


@pytest.mark.parametrize("registration", ["update_listener", "mqtt_status"])
async def test_setup_registration_failure_drains_started_managers_and_listeners(
    hass: HomeAssistant,
    registration: str,
) -> None:
    """Late owner registration is part of the same all-or-nothing setup."""
    entry = _fleet_entry(_panel("office", "SHA256:office", subentry_id="panel-office"))
    fleet = FleetManager(hass, entry)
    stopped: list[str] = []
    update_unsub = Mock()

    async def shutdown(manager: PanelManager) -> None:
        stopped.append(manager.panel)

    update_registration = (
        Mock(side_effect=RuntimeError("update registration failed"))
        if registration == "update_listener"
        else Mock(return_value=update_unsub)
    )
    mqtt_registration = (
        Mock(side_effect=RuntimeError("mqtt registration failed"))
        if registration == "mqtt_status"
        else Mock(return_value=Mock())
    )

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", shutdown),
        patch.object(entry, "add_update_listener", update_registration),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.mqtt.async_subscribe_connection_status",
            mqtt_registration,
        ),
        pytest.raises(RuntimeError, match="registration failed"),
    ):
        await fleet.async_setup()

    assert stopped == ["office"]
    assert fleet.panels == {}
    with pytest.raises(RuntimeError, match="fleet_not_initialized"):
        _ = fleet.fleet
    if registration == "mqtt_status":
        update_unsub.assert_called_once_with()


async def test_one_panel_setup_failure_is_degraded_without_blocking_sibling(
    hass: HomeAssistant,
) -> None:
    entry = _fleet_entry(
        _panel("office", "SHA256:office", subentry_id="panel-office"),
        _panel("kitchen", "SHA256:kitchen", subentry_id="panel-kitchen"),
    )
    starts: list[str] = []

    async def setup(manager: PanelManager) -> None:
        starts.append(manager.panel)
        if manager.panel == "office":
            raise OSError("office unavailable")

    fleet = FleetManager(hass, entry)
    with (
        patch.object(PanelManager, "async_setup", setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        assert starts == ["office", "kitchen"]
        assert fleet.panels["panel-office"].problem is True
        assert "runtime setup failed" in str(fleet.panels["panel-office"].problem_reason)
        assert "office unavailable" not in str(fleet.panels["panel-office"].problem_reason)
        assert fleet.panels["panel-kitchen"].problem is False
        assert (
            ir.async_get(hass).async_get_issue(DOMAIN, f"runtime_setup_failed_{entry.entry_id}")
            is None
        )
        await fleet.async_shutdown()


async def test_zero_successful_panel_setups_raise_retry_and_keep_actionable_issue(
    hass: HomeAssistant,
) -> None:
    """A single dead legacy panel cannot leave HA falsely loaded and green."""
    entry = _legacy_entry()
    fleet = FleetManager(hass, entry)
    stopped: list[str] = []

    async def fail_setup(manager: PanelManager) -> None:
        del manager
        raise OSError("legacy-secret unreachable")

    async def shutdown(manager: PanelManager) -> None:
        stopped.append(manager.panel)

    with (
        patch.object(PanelManager, "async_setup", fail_setup),
        patch.object(PanelManager, "async_shutdown", shutdown),
        pytest.raises(ConfigEntryNotReady, match="No panel runtime could start"),
    ):
        await fleet.async_setup()

    issue_id = f"runtime_setup_failed_{entry.entry_id}"
    issue = ir.async_get(hass).async_get_issue(DOMAIN, issue_id)
    assert issue is not None
    assert issue.translation_key == "needs_attention"
    assert issue.translation_placeholders is not None
    assert "retry" in issue.translation_placeholders["reason"].lower()
    assert "legacy-secret" not in repr(issue)
    assert stopped == ["legacy"]
    assert fleet.panels == {}

    retry = FleetManager(hass, entry)
    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await retry.async_setup()
        assert ir.async_get(hass).async_get_issue(DOMAIN, issue_id) is None
        await retry.async_shutdown()


async def test_shutdown_drains_every_panel_and_clears_fleet_issue_despite_failure(
    hass: HomeAssistant,
) -> None:
    entry = _fleet_entry(
        _panel("office", "SHA256:office", subentry_id="panel-office"),
        _panel("kitchen", "SHA256:kitchen", subentry_id="panel-kitchen"),
    )
    fleet = FleetManager(hass, entry)
    stopped: list[str] = []

    async def shutdown(manager: PanelManager) -> None:
        stopped.append(manager.panel)
        if manager.panel == "office":
            raise RuntimeError("shutdown failed")

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", shutdown),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ir.async_delete_issue"
        ) as delete_issue,
    ):
        await fleet.async_setup()
        with pytest.raises(RuntimeError, match="shutdown failed"):
            await fleet.async_shutdown()

    assert sorted(stopped) == ["kitchen", "office"]
    assert fleet.panels == {}
    delete_issue.assert_called_with(hass, DOMAIN, fleet.broker_issue_id)


async def test_shutdown_drains_all_owners_when_unsubscribe_raises(
    hass: HomeAssistant,
) -> None:
    """A synchronous owner failure cannot skip later unsubscription or panel drains."""
    entry = _fleet_entry(
        _panel("office", "SHA256:office", subentry_id="panel-office"),
        _panel("kitchen", "SHA256:kitchen", subentry_id="panel-kitchen"),
    )
    fleet = FleetManager(hass, entry)
    stopped: list[str] = []
    update_unsub = Mock(side_effect=RuntimeError("update unsubscribe failed"))
    mqtt_unsub = Mock()

    async def shutdown(manager: PanelManager) -> None:
        stopped.append(manager.panel)

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", shutdown),
        patch.object(entry, "add_update_listener", return_value=update_unsub),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.mqtt.async_subscribe_connection_status",
            return_value=mqtt_unsub,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ir.async_delete_issue"
        ) as delete_issue,
    ):
        await fleet.async_setup()
        with pytest.raises(RuntimeError, match="update unsubscribe failed"):
            await fleet.async_shutdown()

    update_unsub.assert_called_once_with()
    mqtt_unsub.assert_called_once_with()
    delete_issue.assert_called_with(hass, DOMAIN, fleet.broker_issue_id)
    assert sorted(stopped) == ["kitchen", "office"]
    assert fleet.panels == {}


async def test_legacy_entry_gets_one_compatibility_manager_and_synthetic_fleet(
    hass: HomeAssistant,
) -> None:
    entry = _legacy_entry()
    fleet = FleetManager(hass, entry)

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        assert set(fleet.panels) == {entry.entry_id}
        panel = fleet.panels[entry.entry_id]
        assert isinstance(panel.store, LegacyPanelStore)
        assert panel.store.subentry_id is None
        assert panel.management_id == entry.entry_id
        assert panel.fleet.broker.host == "mqtt.example.com"
        with pytest.raises(EntryDataError, match="legacy_subentries_unsupported"):
            await fleet.async_panel_added("not-supported")
        await fleet.async_shutdown()


async def test_legacy_runtimes_share_domain_ssh_lock_with_onboarding(
    hass: HomeAssistant,
) -> None:
    """Compatibility entries cannot bypass the installation-wide SSH mutex."""
    first = FleetManager(hass, _legacy_entry())
    second = FleetManager(hass, _legacy_entry())

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await first.async_setup()
        await second.async_setup()
        first_panel = next(iter(first.panels.values()))
        second_panel = next(iter(second.panels.values()))
        assert first_panel._ssh_lock is second_panel._ssh_lock
        assert first_panel._ssh_lock is hass.data[DOMAIN]["ssh_lock"]
        await first.async_shutdown()
        await second.async_shutdown()


async def test_live_subentry_add_update_remove_reconciles_without_restarting_siblings(
    hass: HomeAssistant,
) -> None:
    office = _panel("office", "SHA256:office", subentry_id="panel-office")
    entry = _fleet_entry(office)
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)
    starts: list[str] = []
    stops: list[str] = []

    async def setup(manager: PanelManager) -> None:
        starts.append(manager.panel)

    async def shutdown(manager: PanelManager) -> None:
        stops.append(manager.panel)

    with (
        patch.object(PanelManager, "async_setup", setup),
        patch.object(PanelManager, "async_shutdown", shutdown),
    ):
        await fleet.async_setup()
        office_manager = fleet.panels["panel-office"]

        kitchen = _panel("kitchen", "SHA256:kitchen", subentry_id="panel-kitchen")
        assert hass.config_entries.async_add_subentry(entry, kitchen)
        await hass.async_block_till_done()
        assert set(fleet.panels) == {"panel-office", "panel-kitchen"}
        assert starts == ["office", "kitchen"]

        stored_office = entry.subentries["panel-office"]
        assert hass.config_entries.async_update_subentry(
            entry,
            stored_office,
            data={**stored_office.data, CONF_HOST: "office-new.example.com"},
        )
        await hass.async_block_till_done()
        assert fleet.panels["panel-office"] is office_manager
        assert office_manager.store.data[CONF_HOST] == "office-new.example.com"
        assert starts == ["office", "kitchen"]
        assert stops == []

        assert hass.config_entries.async_remove_subentry(entry, "panel-kitchen")
        await hass.async_block_till_done()
        assert set(fleet.panels) == {"panel-office"}
        assert stops == ["kitchen"]

        await fleet.async_shutdown()
        assert stops == ["kitchen", "office"]


async def test_queued_update_cannot_rebuild_panels_after_shutdown(
    hass: HomeAssistant,
) -> None:
    """A callback queued before shutdown rechecks the latch under the lifecycle lock."""
    entry = _fleet_entry(_panel("office", "SHA256:office", subentry_id="panel-office"))
    fleet = FleetManager(hass, entry)
    starts: list[str] = []

    async def setup(manager: PanelManager) -> None:
        starts.append(manager.panel)

    with (
        patch.object(PanelManager, "async_setup", setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        await fleet._lifecycle_lock.acquire()
        shutdown = asyncio.create_task(fleet._async_shutdown_owned())
        await asyncio.sleep(0)
        update = asyncio.create_task(fleet._async_entry_updated(hass, entry))
        await asyncio.sleep(0)
        fleet._lifecycle_lock.release()
        await shutdown
        await update

    assert starts == ["office"]
    assert fleet.panels == {}


async def test_cancelled_live_add_drains_partially_started_manager(
    hass: HomeAssistant,
) -> None:
    """Cancellation during the second MQTT subscription releases the new manager."""
    office = _panel("office", "SHA256:office", subentry_id="panel-office")
    entry = _fleet_entry(office)
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)

    with patch.object(PanelManager, "async_setup", _noop_setup):
        await fleet.async_setup()
    assert fleet._update_unsub is not None
    fleet._update_unsub()
    fleet._update_unsub = None

    kitchen = _panel("kitchen", "SHA256:kitchen", subentry_id="panel-kitchen")
    assert hass.config_entries.async_add_subentry(entry, kitchen)
    second_subscribe = asyncio.Event()
    availability_unsub = Mock()
    stopped: list[str] = []
    original_shutdown = PanelManager.async_shutdown

    async def subscribe(
        hass_arg: HomeAssistant,
        topic: str,
        message_callback: Callable[..., None],
    ) -> Callable[[], None]:
        del message_callback
        assert hass_arg is hass
        if topic == "brilliant/kitchen/availability":
            return availability_unsub
        second_subscribe.set()
        await asyncio.Future()
        raise AssertionError("unreachable")

    async def shutdown(manager: PanelManager) -> None:
        stopped.append(manager.panel)
        await original_shutdown(manager)

    with (
        patch(
            "custom_components.brilliant_mqtt.manager.mqtt.async_subscribe",
            side_effect=subscribe,
        ),
        patch.object(PanelManager, "async_shutdown", shutdown),
    ):
        addition = asyncio.create_task(fleet.async_panel_added("panel-kitchen"))
        await second_subscribe.wait()
        addition.cancel()
        with pytest.raises(asyncio.CancelledError):
            await addition

        assert stopped == ["kitchen"]
        assert "panel-kitchen" not in fleet.panels
        availability_unsub.assert_called_once_with()
        await fleet.async_shutdown()


async def test_cancelled_staged_live_add_preserves_committed_snapshot_and_drains_all(
    hass: HomeAssistant,
) -> None:
    """A cancelled later addition cannot publish or leak any staged manager."""
    office = _panel("office", "SHA256:office", subentry_id="panel-office")
    entry = _fleet_entry(office)
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)

    with patch.object(PanelManager, "async_setup", _noop_setup):
        await fleet.async_setup()
    assert fleet._update_unsub is not None
    fleet._update_unsub()
    fleet._update_unsub = None

    office_manager = fleet.panels["panel-office"]
    original_fleet = fleet.fleet
    original_store = office_manager.store
    original_panels = dict(fleet.panels)
    original_configs = fleet._panel_configs
    original_config_values = dict(original_configs)

    hass.config_entries.async_update_entry(
        entry,
        data={**entry.data, CONF_HA_CONTROL_LABEL: "new-label"},
    )
    stored_office = entry.subentries["panel-office"]
    assert hass.config_entries.async_update_subentry(
        entry,
        stored_office,
        data={**stored_office.data, CONF_HOST: "office-new.example.com"},
    )
    assert hass.config_entries.async_add_subentry(
        entry,
        _panel("kitchen", "SHA256:kitchen", subentry_id="panel-kitchen"),
    )
    assert hass.config_entries.async_add_subentry(
        entry,
        _panel("bedroom", "SHA256:bedroom", subentry_id="panel-bedroom"),
    )

    bedroom_setup = asyncio.Event()
    starts: list[str] = []
    stopped: list[str] = []

    async def setup(manager: PanelManager) -> None:
        starts.append(manager.panel)
        if manager.panel == "bedroom":
            bedroom_setup.set()
            await asyncio.Future()

    async def shutdown(manager: PanelManager) -> None:
        stopped.append(manager.panel)
        if manager.panel == "kitchen":
            raise RuntimeError("staged cleanup failed")

    with (
        patch.object(PanelManager, "async_setup", setup),
        patch.object(PanelManager, "async_shutdown", shutdown),
    ):
        addition = asyncio.create_task(fleet.async_panel_added("panel-bedroom"))
        await bedroom_setup.wait()
        addition.cancel()
        with pytest.raises(asyncio.CancelledError):
            await addition

        assert starts == ["kitchen", "bedroom"]
        assert sorted(stopped) == ["bedroom", "kitchen"]
        assert dict(fleet.panels) == original_panels
        assert fleet.fleet is original_fleet
        assert fleet._panel_configs is original_configs
        assert fleet._panel_configs == original_config_values
        assert office_manager.store is original_store
        assert office_manager.fleet is original_fleet

        await fleet.async_shutdown()


async def test_removal_commits_snapshot_before_cancellation_settled_cleanup(
    hass: HomeAssistant,
) -> None:
    """Removed-panel cleanup cannot expose or strand a hybrid fleet snapshot."""
    office = _panel("office", "SHA256:office", subentry_id="panel-office")
    kitchen = _panel("kitchen", "SHA256:kitchen", subentry_id="panel-kitchen")
    entry = _fleet_entry(office, kitchen)
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)

    with patch.object(PanelManager, "async_setup", _noop_setup):
        await fleet.async_setup()
    assert fleet._update_unsub is not None
    fleet._update_unsub()
    fleet._update_unsub = None

    office_manager = fleet.panels["panel-office"]
    original_store = office_manager.store
    hass.config_entries.async_update_entry(
        entry,
        data={**entry.data, CONF_HA_CONTROL_LABEL: "new-label"},
    )
    stored_office = entry.subentries["panel-office"]
    assert hass.config_entries.async_update_subentry(
        entry,
        stored_office,
        data={**stored_office.data, CONF_HOST: "office-new.example.com"},
    )
    assert hass.config_entries.async_remove_subentry(entry, "panel-kitchen")

    cleanup_entered = asyncio.Event()
    cleanup_release = asyncio.Event()
    stopped: list[str] = []

    async def shutdown(manager: PanelManager) -> None:
        stopped.append(manager.panel)
        if manager.panel == "kitchen":
            cleanup_entered.set()
            await cleanup_release.wait()

    with patch.object(PanelManager, "async_shutdown", shutdown):
        removal = asyncio.create_task(fleet.async_panel_removed("panel-kitchen"))
        await cleanup_entered.wait()

        assert set(fleet.panels) == {"panel-office"}
        assert fleet.fleet.ha_control_label == "new-label"
        assert set(fleet._panel_configs) == {"panel-office"}
        assert fleet._panel_configs["panel-office"].host == "office-new.example.com"
        assert office_manager.store is not original_store
        assert office_manager.fleet is fleet.fleet

        removal.cancel()
        await asyncio.sleep(0)
        assert not removal.done()
        cleanup_release.set()
        with pytest.raises(asyncio.CancelledError):
            await removal

        assert stopped == ["kitchen"]
        await fleet.async_shutdown()


async def test_cancelled_removal_finishes_cleanup_after_atomic_snapshot_swap(
    hass: HomeAssistant,
) -> None:
    """Once staging succeeds, cancellation exposes the new snapshot and drains removal."""
    entry = _fleet_entry(
        _panel("office", "SHA256:office", subentry_id="panel-office"),
        _panel("garage", "SHA256:garage", subentry_id="panel-garage"),
    )
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)

    with patch.object(PanelManager, "async_setup", _noop_setup):
        await fleet.async_setup()
    assert fleet._update_unsub is not None
    fleet._update_unsub()
    fleet._update_unsub = None

    garage = fleet.panels["panel-garage"]
    hass.config_entries.async_update_entry(
        entry,
        data={**entry.data, CONF_HA_CONTROL_LABEL: "new-label"},
    )
    assert hass.config_entries.async_remove_subentry(entry, "panel-garage")
    assert hass.config_entries.async_add_subentry(
        entry,
        _panel("kitchen", "SHA256:kitchen", subentry_id="panel-kitchen"),
    )

    removal_started = asyncio.Event()
    release_removal = asyncio.Event()
    stopped: list[str] = []

    async def shutdown(manager: PanelManager) -> None:
        if manager is garage:
            removal_started.set()
            await release_removal.wait()
        stopped.append(manager.panel)

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", shutdown),
    ):
        reconcile = asyncio.create_task(fleet.async_panel_added("panel-kitchen"))
        await removal_started.wait()

        assert set(fleet.panels) == {"panel-office", "panel-kitchen"}
        assert fleet.fleet.ha_control_label == "new-label"

        reconcile.cancel()
        await asyncio.sleep(0)
        assert not reconcile.done()
        release_removal.set()
        with pytest.raises(asyncio.CancelledError):
            await reconcile

        assert stopped == ["garage"]
        assert set(fleet.panels) == {"panel-office", "panel-kitchen"}
        assert fleet._panel_configs.keys() == fleet.panels.keys()
        await fleet.async_shutdown()


async def test_manager_originated_subentry_write_updates_in_place_without_loop(
    hass: HomeAssistant,
) -> None:
    entry = _fleet_entry(_panel("office", "SHA256:office", subentry_id="panel-office"))
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)
    starts = 0

    async def setup(manager: PanelManager) -> None:
        nonlocal starts
        del manager
        starts += 1

    with (
        patch.object(PanelManager, "async_setup", setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        panel = fleet.panels["panel-office"]
        with patch.object(panel, "_notify") as notify:
            panel.store.update_options({"auto_repair": False})
            await hass.async_block_till_done()
            notify.assert_called_once_with()

        assert fleet.panels["panel-office"] is panel
        assert panel.store.options == {"auto_repair": False}
        assert starts == 1
        await fleet.async_shutdown()


async def test_mqtt_connection_status_owns_one_redacted_fleet_issue(
    hass: HomeAssistant,
) -> None:
    entry = _fleet_entry(
        _panel("office", "SHA256:office", subentry_id="panel-office"),
        _panel("kitchen", "SHA256:kitchen", subentry_id="panel-kitchen"),
    )
    fleet = FleetManager(hass, entry)
    callbacks: list[Callable[[bool], None]] = []
    unsubscribe = Mock()

    def subscribe(hass_arg: HomeAssistant, callback: Callable[[bool], None]) -> Callable[[], None]:
        assert hass_arg is hass
        callbacks.append(callback)
        return unsubscribe

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.mqtt.async_subscribe_connection_status",
            side_effect=subscribe,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.mqtt.is_connected",
            return_value=False,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ir.async_create_issue"
        ) as create_issue,
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ir.async_delete_issue"
        ) as delete_issue,
    ):
        await fleet.async_setup()
        assert fleet.broker_available is False
        assert len(callbacks) == 1
        assert create_issue.call_count == 1
        assert "fleet-secret" not in repr(create_issue.call_args)

        callbacks[0](False)
        assert create_issue.call_count == 2
        assert {call.args[2] for call in create_issue.call_args_list} == {fleet.broker_issue_id}

        callbacks[0](True)
        assert fleet.broker_available is True
        delete_issue.assert_called_with(hass, DOMAIN, fleet.broker_issue_id)

        await fleet.async_shutdown()

    unsubscribe.assert_called_once_with()
