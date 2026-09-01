"""FleetManager owns every panel runtime below one Brilliant config entry."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from copy import deepcopy
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any, cast
from unittest.mock import AsyncMock, Mock, patch
from uuid import UUID

import pytest
from homeassistant.config_entries import ConfigSubentry
from homeassistant.const import CONF_NAME
from homeassistant.core import Event, HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers import issue_registry as ir
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.brilliant_mqtt.broker import BrokerKind
from custom_components.brilliant_mqtt.components import REGISTRY
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
    ENTRY_KIND_LEGACY_PENDING_CONSOLIDATION,
    EVENT_PANEL_REBOUND,
    EVENT_TYPE,
    FLEET_UNIQUE_ID,
    SUBENTRY_TYPE_PANEL,
)
from custom_components.brilliant_mqtt.entry_data import (
    EntryDataError,
    FleetPanelStore,
    LegacyPanelStore,
    PanelConfig,
)
from custom_components.brilliant_mqtt.fleet_manager import (
    _CONFIG_ENTRY_PERSISTENCE_POLL_SECONDS,
    ConfigEntryPersistenceError,
    FleetManager,
    _async_duplicate_fingerprint,
    _async_transaction_lookup,
    _canonical_entry_storage,
    async_recover_removed_entry,
    async_wait_config_entry_persisted,
    get_panel_provisioner,
    legacy_fleet_config,
    pending_scene_owner,
)
from custom_components.brilliant_mqtt.manager import PanelManager
from custom_components.brilliant_mqtt.panel_provisioner import TransactionLookupState
from custom_components.brilliant_mqtt.provisioning_journal import (
    ProvisioningOperation,
    ProvisioningPhase,
    ProvisioningRecord,
    StoredFileSnapshot,
    StoredFleetProfile,
    StoredPanelLayout,
    StoredPanelRequest,
    StoredPanelSnapshot,
    StoredServiceSnapshot,
)
from custom_components.brilliant_mqtt.shell import HostIdentity, PanelIdentityError

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
_TRANSACTION_ID = UUID("12345678-1234-4abc-8def-1234567890ab")
_OTHER_TRANSACTION_ID = UUID("87654321-4321-4cba-8fed-ba0987654321")
_SETUP_ID = UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")


def test_persistence_poll_interval_avoids_busy_storage_reads() -> None:
    """The durability gate should stay responsive without polling disk at 20 Hz."""
    assert _CONFIG_ENTRY_PERSISTENCE_POLL_SECONDS >= 0.25


def _panel_owned_issue_ids(management_id: str) -> set[str]:
    """Return every issue ID owned by one panel runtime."""
    return {
        f"needs_attention_{management_id}",
        f"voice_missing_{management_id}",
        f"ha_mirror_retired_{management_id}",
        *(
            f"component_state_unverified_{management_id}_{component.id}"
            for component in REGISTRY.values()
            if not component.deprecated
        ),
    }


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

    async def verify_confirmed_identity(
        host: str,
        expected: HostIdentity,
        port: int = 22,
    ) -> HostIdentity:
        del host, port
        return expected

    monkeypatch.setattr(
        "custom_components.brilliant_mqtt.fleet_manager.async_verify_host_identity",
        verify_confirmed_identity,
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
    management_id: str | None = None,
    mesh_priority: int | None = None,
    transaction_id: UUID | None = None,
) -> ConfigSubentry:
    if fingerprint == "SHA256:office":
        public_key, identity_fingerprint = _OFFICE_PUBLIC_KEY, _OFFICE_FINGERPRINT
    elif fingerprint == "SHA256:bedroom":
        public_key, identity_fingerprint = _THIRD_PUBLIC_KEY, _THIRD_FINGERPRINT
    else:
        public_key, identity_fingerprint = _OTHER_PUBLIC_KEY, _OTHER_FINGERPRINT
    if mesh_priority is None:
        mesh_priority = {
            "office": 1,
            "kitchen": 2,
            "bedroom": 3,
            "garage": 2,
        }.get(slug, 1)
    data: dict[str, Any] = {
        CONF_IDENTITY_FINGERPRINT: identity_fingerprint,
        CONF_SSH_HOST_KEY: public_key,
        CONF_HOST: host or f"{slug}.example.com",
        CONF_SSH_USERNAME: "root",
        CONF_ROOT_PASSWORD: f"{slug}-secret",
        CONF_NAME: slug.title(),
        CONF_PANEL: slug,
        CONF_MANAGEMENT_ID: management_id or fingerprint,
        CONF_COMPONENTS: {
            COMPONENT_BRIDGE: True,
            COMPONENT_WIFI_WATCHDOG: True,
            COMPONENT_BUS_WATCHDOG: True,
        },
        CONF_FEATURE_OVERRIDES: {"auto_repair": True},
        CONF_MESH_PRIORITY: mesh_priority,
    }
    if transaction_id is not None:
        data[CONF_PROVISIONING_TRANSACTION_ID] = str(transaction_id)
    return ConfigSubentry(
        data=MappingProxyType(data),
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


def _empty_fleet_entry() -> MockConfigEntry:
    return MockConfigEntry(
        domain=DOMAIN,
        unique_id=FLEET_UNIQUE_ID,
        version=CONFIG_ENTRY_VERSION,
        data={
            **_fleet_data(),
            CONF_NEXT_MESH_PRIORITY: 1,
            CONF_SCENE_PANEL: "__unassigned__",
        },
    )


def _pending_record(
    *,
    phase: ProvisioningPhase = ProvisioningPhase.PENDING_CONFIG_COMMIT,
) -> ProvisioningRecord:
    absent_file = StoredFileSnapshot(content=None, mode=None)
    absent_service = StoredServiceSnapshot(
        unit_file=absent_file,
        enabled=False,
        active=False,
    )
    return ProvisioningRecord(
        transaction_id=_TRANSACTION_ID,
        operation=ProvisioningOperation.INSTALL,
        phase=phase,
        setup_id=_SETUP_ID,
        panel_request=StoredPanelRequest(
            host="office.example.com",
            ssh_username="root",
            root_password="office-secret",
            public_key=_OFFICE_PUBLIC_KEY,
            fingerprint=_OFFICE_FINGERPRINT,
            slug="office",
            selected_components=(
                COMPONENT_BRIDGE,
                COMPONENT_BUS_WATCHDOG,
                COMPONENT_WIFI_WATCHDOG,
            ),
        ),
        fleet_profile=StoredFleetProfile(
            kind=BrokerKind.EXISTING_BROKER,
            host="mqtt.example.com",
            port=1883,
            tls_enabled=False,
        ),
        staged_version="0.6.0",
        prior_snapshot=StoredPanelSnapshot(
            layout=StoredPanelLayout.ABSENT,
            active_release_target=None,
            environment_file=absent_file,
            version_file=absent_file,
            bridge_service=absent_service,
            wifi_watchdog_service=absent_service,
            bus_watchdog_service=absent_service,
            selected_components=(),
        ),
        started_at=datetime(2026, 7, 28, 12, 0, tzinfo=UTC),
        last_error=None,
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


@pytest.mark.parametrize(
    ("key", "value"),
    (
        (CONF_HOT_POLL_SECONDS, 5.9),
        (CONF_RESYNC_SECONDS, 0),
    ),
    ids=("float", "out-of-range"),
)
def test_legacy_fleet_config_rejects_tampered_agent_cadences(
    key: str,
    value: object,
) -> None:
    legacy = _legacy_entry()
    malformed = MockConfigEntry(
        domain=DOMAIN,
        version=legacy.version,
        data={
            **legacy.data,
            key: value,
        },
    )

    with pytest.raises(EntryDataError, match="invalid_legacy_fleet_data"):
        legacy_fleet_config(malformed)


@pytest.mark.parametrize(
    "cadences",
    (
        {},
        {
            CONF_HOT_POLL_SECONDS: 5,
            CONF_RESYNC_SECONDS: 900,
        },
    ),
    ids=("absent", "configured"),
)
def test_legacy_fleet_config_accepts_compatible_agent_cadences(
    cadences: dict[str, int],
) -> None:
    legacy = _legacy_entry()
    compatible = MockConfigEntry(
        domain=DOMAIN,
        version=legacy.version,
        data={
            **legacy.data,
            **cadences,
        },
    )

    legacy_fleet_config(compatible)


async def _noop_setup(manager: PanelManager) -> None:
    del manager


async def _noop_shutdown(manager: PanelManager) -> None:
    del manager


def _create_provisioning_repair(hass: HomeAssistant) -> str:
    """Seed the stable transaction repair to verify successful cleanup."""
    issue_id = f"provisioning_rollback_{_TRANSACTION_ID.hex}"
    ir.async_create_issue(
        hass,
        DOMAIN,
        issue_id,
        is_fixable=False,
        severity=ir.IssueSeverity.ERROR,
        translation_key="needs_attention",
        translation_placeholders={
            "panel": "Brilliant MQTT provisioning",
            "reason": "prior recovery failed",
        },
    )
    return issue_id


def test_pending_scene_owner_is_canonical_and_transaction_scoped() -> None:
    """A different or malformed transaction cannot alias an initial scene owner."""
    assert (
        pending_scene_owner(_TRANSACTION_ID)
        == "pending-provisioning:12345678-1234-4abc-8def-1234567890ab"
    )
    with pytest.raises(ValueError, match="invalid_provisioning_transaction_id"):
        pending_scene_owner(UUID("12345678-1234-3abc-8def-1234567890ab"))


def _provisioning_entry(
    *panels: ConfigSubentry,
    scene_owner: str,
    next_mesh_priority: int = 3,
) -> MockConfigEntry:
    return MockConfigEntry(
        domain=DOMAIN,
        unique_id=FLEET_UNIQUE_ID,
        version=CONFIG_ENTRY_VERSION,
        data={
            **_fleet_data(),
            CONF_SCENE_PANEL: scene_owner,
            CONF_NEXT_MESH_PRIORITY: next_mesh_priority,
        },
        subentries_data=[panel.as_dict() for panel in panels],
    )


def _pending_owner_entry() -> MockConfigEntry:
    return _provisioning_entry(
        _panel(
            "office",
            "SHA256:office",
            subentry_id="panel-office",
            management_id=_OFFICE_FINGERPRINT,
            transaction_id=_TRANSACTION_ID,
        ),
        scene_owner=pending_scene_owner(_TRANSACTION_ID),
    )


def _disk_config_entries_envelope(entry: MockConfigEntry) -> dict[str, object]:
    """Build the fresh on-disk envelope consumed by the strict proof helper."""
    return {
        "version": 1,
        "minor_version": 1,
        "key": "core.config_entries",
        "data": {"entries": [deepcopy(dict(_canonical_entry_storage(entry)))]},
    }


async def test_initial_setup_normalizes_owner_then_commits_exact_runtime_handoff(
    hass: HomeAssistant,
) -> None:
    """The HA-assigned subentry ID replaces the transaction placeholder before parse."""
    entry = _provisioning_entry(
        _panel(
            "office",
            "SHA256:office",
            subentry_id="panel-office",
            management_id=_OFFICE_FINGERPRINT,
            transaction_id=_TRANSACTION_ID,
        ),
        scene_owner=pending_scene_owner(_TRANSACTION_ID),
    )
    entry.add_to_hass(hass)
    journal = Mock(
        async_load=AsyncMock(return_value=_pending_record()),
        async_complete_commit=AsyncMock(),
    )
    fleet = FleetManager(hass, entry)
    persisted = AsyncMock()
    issue_id = _create_provisioning_repair(hass)

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
            persisted,
        ),
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()

    assert entry.data[CONF_SCENE_PANEL] == "panel-office"
    stored = entry.subentries["panel-office"]
    assert CONF_PROVISIONING_TRANSACTION_ID not in stored.data
    assert set(stored.data) == set(
        _panel(
            "office",
            "SHA256:office",
            subentry_id="comparison",
        ).data
    )
    journal.async_complete_commit.assert_awaited_once_with(
        _TRANSACTION_ID,
        subentry_id="panel-office",
    )
    assert persisted.await_count == 2
    assert ir.async_get(hass).async_get_issue(DOMAIN, issue_id) is None

    with patch.object(PanelManager, "async_shutdown", _noop_shutdown):
        await fleet.async_shutdown()


async def test_cancel_during_pending_handoff_commit_clears_resolved_repair(
    hass: HomeAssistant,
) -> None:
    """Cancellation is re-raised only after terminal commit and issue cleanup."""
    entry = _pending_owner_entry()
    entry.add_to_hass(hass)
    commit_started = asyncio.Event()
    allow_commit = asyncio.Event()
    commit_finished = asyncio.Event()

    async def complete_commit(
        transaction_id: UUID,
        *,
        subentry_id: str,
    ) -> None:
        assert transaction_id == _TRANSACTION_ID
        assert subentry_id == "panel-office"
        commit_started.set()
        try:
            await allow_commit.wait()
        except asyncio.CancelledError:
            await allow_commit.wait()
            commit_finished.set()
            raise
        commit_finished.set()

    journal = Mock(
        async_load=AsyncMock(return_value=_pending_record()),
        async_complete_commit=AsyncMock(side_effect=complete_commit),
    )
    issue_id = _create_provisioning_repair(hass)

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
            new=AsyncMock(),
        ),
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        setup = asyncio.create_task(FleetManager(hass, entry).async_setup())
        await commit_started.wait()
        setup.cancel()
        await asyncio.sleep(0)
        assert not setup.done()
        allow_commit.set()
        with pytest.raises(asyncio.CancelledError):
            await setup

    assert commit_finished.is_set()
    journal.async_complete_commit.assert_awaited_once_with(
        _TRANSACTION_ID,
        subentry_id="panel-office",
    )
    assert ir.async_get(hass).async_get_issue(DOMAIN, issue_id) is None


async def test_first_handoff_normalizes_reserved_empty_owner_to_exact_subentry(
    hass: HomeAssistant,
) -> None:
    """The first durable subentry atomically takes scene ownership from the sentinel."""
    entry = _provisioning_entry(
        _panel(
            "office",
            "SHA256:office",
            subentry_id="panel-office",
            management_id=_OFFICE_FINGERPRINT,
            transaction_id=_TRANSACTION_ID,
        ),
        scene_owner="__unassigned__",
        next_mesh_priority=1,
    )
    entry.add_to_hass(hass)
    journal = Mock(
        async_load=AsyncMock(return_value=_pending_record()),
        async_complete_commit=AsyncMock(),
    )
    fleet = FleetManager(hass, entry)
    persisted = AsyncMock()

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
            persisted,
        ),
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()

    assert entry.data[CONF_SCENE_PANEL] == "panel-office"
    assert entry.data[CONF_NEXT_MESH_PRIORITY] == 2
    journal.async_complete_commit.assert_awaited_once_with(
        _TRANSACTION_ID,
        subentry_id="panel-office",
    )
    assert persisted.await_count == 2

    with patch.object(PanelManager, "async_shutdown", _noop_shutdown):
        await fleet.async_shutdown()


async def test_committed_handoff_clears_before_runtime_failure(
    hass: HomeAssistant,
) -> None:
    """A durable COMMITTED record never retains its root secret for runtime retry."""
    entry = _provisioning_entry(
        _panel(
            "office",
            "SHA256:office",
            subentry_id="panel-office",
            management_id=_OFFICE_FINGERPRINT,
        ),
        scene_owner="panel-office",
        next_mesh_priority=2,
    )
    entry.add_to_hass(hass)
    journal = Mock(
        async_load=AsyncMock(return_value=_pending_record(phase=ProvisioningPhase.COMMITTED)),
        async_clear_committed=AsyncMock(),
        async_complete_commit=AsyncMock(),
    )

    async def fail_runtime(manager: PanelManager) -> None:
        del manager
        assert journal.async_clear_committed.await_count == 1
        raise OSError("mqtt runtime unavailable")

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch.object(PanelManager, "async_setup", fail_runtime),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
        pytest.raises(ConfigEntryNotReady, match="No panel runtime"),
    ):
        await FleetManager(hass, entry).async_setup()

    journal.async_clear_committed.assert_awaited_once_with(_TRANSACTION_ID)
    journal.async_complete_commit.assert_not_awaited()


async def test_cancel_during_startup_committed_clear_clears_resolved_repair(
    hass: HomeAssistant,
) -> None:
    """A terminal startup clear and issue deletion drain before cancellation escapes."""
    entry = _provisioning_entry(
        _panel(
            "office",
            "SHA256:office",
            subentry_id="panel-office",
            management_id=_OFFICE_FINGERPRINT,
        ),
        scene_owner="panel-office",
        next_mesh_priority=2,
    )
    entry.add_to_hass(hass)
    clear_started = asyncio.Event()
    allow_clear = asyncio.Event()
    clear_finished = asyncio.Event()

    async def clear_committed(transaction_id: UUID) -> None:
        assert transaction_id == _TRANSACTION_ID
        clear_started.set()
        try:
            await allow_clear.wait()
        except asyncio.CancelledError:
            await allow_clear.wait()
            clear_finished.set()
            raise
        clear_finished.set()

    journal = Mock(
        async_load=AsyncMock(return_value=_pending_record(phase=ProvisioningPhase.COMMITTED)),
        async_clear_committed=AsyncMock(side_effect=clear_committed),
    )
    issue_id = _create_provisioning_repair(hass)
    setup_panel = AsyncMock()

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch.object(PanelManager, "async_setup", setup_panel),
    ):
        setup = asyncio.create_task(FleetManager(hass, entry).async_setup())
        await clear_started.wait()
        setup.cancel()
        await asyncio.sleep(0)
        assert not setup.done()
        allow_clear.set()
        with pytest.raises(asyncio.CancelledError):
            await setup

    assert clear_finished.is_set()
    journal.async_clear_committed.assert_awaited_once_with(_TRANSACTION_ID)
    setup_panel.assert_not_awaited()
    assert ir.async_get(hass).async_get_issue(DOMAIN, issue_id) is None


@pytest.mark.parametrize("corruption", ("marker_present", "wrong_scene_owner"))
async def test_committed_handoff_corruption_fails_closed_with_repair(
    hass: HomeAssistant,
    corruption: str,
) -> None:
    """COMMITTED is trusted only after the exact final config shape is present."""
    office = _panel(
        "office",
        "SHA256:office",
        subentry_id="panel-office",
        management_id=_OFFICE_FINGERPRINT,
        transaction_id=(_TRANSACTION_ID if corruption == "marker_present" else None),
    )
    panels = (
        office,
        *(
            (_panel("kitchen", "SHA256:kitchen", subentry_id="panel-kitchen"),)
            if corruption == "wrong_scene_owner"
            else ()
        ),
    )
    entry = _provisioning_entry(
        *panels,
        scene_owner=("panel-kitchen" if corruption == "wrong_scene_owner" else "panel-office"),
    )
    entry.add_to_hass(hass)
    journal = Mock(
        async_load=AsyncMock(return_value=_pending_record(phase=ProvisioningPhase.COMMITTED)),
        async_clear_committed=AsyncMock(),
    )
    setup = AsyncMock()

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch.object(PanelManager, "async_setup", setup),
        pytest.raises(EntryDataError, match="provisioning_ownership_mismatch"),
    ):
        await FleetManager(hass, entry).async_setup()

    journal.async_clear_committed.assert_not_awaited()
    setup.assert_not_awaited()
    issue_id = f"provisioning_rollback_{_TRANSACTION_ID.hex}"
    assert ir.async_get(hass).async_get_issue(DOMAIN, issue_id) is not None


async def test_restart_after_marker_removal_commits_only_exact_owned_runtime(
    hass: HomeAssistant,
) -> None:
    """A crash between marker removal and journal clear never rolls back ownership."""
    entry = _provisioning_entry(
        _panel(
            "office",
            "SHA256:office",
            subentry_id="panel-office",
            management_id=_OFFICE_FINGERPRINT,
        ),
        scene_owner="panel-office",
        next_mesh_priority=2,
    )
    entry.add_to_hass(hass)
    journal = Mock(
        async_load=AsyncMock(return_value=_pending_record()),
        async_complete_commit=AsyncMock(),
    )
    recover = AsyncMock()
    fleet = FleetManager(hass, entry)
    persisted = AsyncMock()

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager._get_recovery_provisioner",
            return_value=Mock(async_recover=recover),
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
            persisted,
        ),
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()

    recover.assert_not_awaited()
    journal.async_complete_commit.assert_awaited_once_with(
        _TRANSACTION_ID,
        subentry_id="panel-office",
    )
    assert CONF_PROVISIONING_TRANSACTION_ID not in entry.subentries["panel-office"].data
    persisted.assert_awaited_once_with(
        hass,
        entry,
        subentry_id="panel-office",
    )

    with patch.object(PanelManager, "async_shutdown", _noop_shutdown):
        await fleet.async_shutdown()


async def test_restart_after_scene_normalization_finishes_remaining_marker(
    hass: HomeAssistant,
) -> None:
    """The reachable first write boundary resumes without rewriting scene ownership."""
    entry = _provisioning_entry(
        _panel(
            "office",
            "SHA256:office",
            subentry_id="panel-office",
            management_id=_OFFICE_FINGERPRINT,
            transaction_id=_TRANSACTION_ID,
        ),
        scene_owner="panel-office",
        next_mesh_priority=2,
    )
    entry.add_to_hass(hass)
    journal = Mock(
        async_load=AsyncMock(return_value=_pending_record()),
        async_complete_commit=AsyncMock(),
    )
    fleet = FleetManager(hass, entry)
    persisted = AsyncMock()

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
            persisted,
        ),
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()

    assert entry.data[CONF_SCENE_PANEL] == "panel-office"
    assert CONF_PROVISIONING_TRANSACTION_ID not in entry.subentries["panel-office"].data
    journal.async_complete_commit.assert_awaited_once_with(
        _TRANSACTION_ID,
        subentry_id="panel-office",
    )
    persisted.assert_awaited_once_with(
        hass,
        entry,
        subentry_id="panel-office",
    )

    with patch.object(PanelManager, "async_shutdown", _noop_shutdown):
        await fleet.async_shutdown()


async def test_markerless_restart_force_saves_before_journal_commit(
    hass: HomeAssistant,
) -> None:
    """A failed HA storage flush leaves the safety journal authoritative."""
    entry = _provisioning_entry(
        _panel(
            "office",
            "SHA256:office",
            subentry_id="panel-office",
            management_id=_OFFICE_FINGERPRINT,
        ),
        scene_owner="panel-office",
        next_mesh_priority=2,
    )
    entry.add_to_hass(hass)
    journal = Mock(
        async_load=AsyncMock(return_value=_pending_record()),
        async_complete_commit=AsyncMock(),
    )

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
            side_effect=OSError("storage unavailable"),
        ) as save,
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
        pytest.raises(ConfigEntryNotReady, match="ownership storage"),
    ):
        await FleetManager(hass, entry).async_setup()

    save.assert_awaited_once_with(
        hass,
        entry,
        subentry_id="panel-office",
    )
    journal.async_complete_commit.assert_not_awaited()


async def test_strict_persistence_accepts_exact_fresh_disk_fragment(
    hass: HomeAssistant,
) -> None:
    """Only the canonical live entry found in a fresh disk read proves durability."""
    entry = _provisioning_entry(
        _panel(
            "office",
            "SHA256:office",
            subentry_id="panel-office",
            management_id=_OFFICE_FINGERPRINT,
        ),
        scene_owner="panel-office",
        next_mesh_priority=2,
    )
    entry.add_to_hass(hass)
    stored = _disk_config_entries_envelope(entry)
    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.json_util.load_json",
            return_value=stored,
        ) as load_json,
    ):
        await async_wait_config_entry_persisted(
            hass,
            entry,
            subentry_id="panel-office",
        )

    load_json.assert_called_once_with(hass.config.path(".storage/core.config_entries"))


async def test_strict_persistence_retries_stale_disk_until_exact_fragment(
    hass: HomeAssistant,
) -> None:
    """Pending Store memory and stale disk cannot substitute for an exact fresh read."""
    entry = _provisioning_entry(
        _panel(
            "office",
            "SHA256:office",
            subentry_id="panel-office",
            management_id=_OFFICE_FINGERPRINT,
        ),
        scene_owner="panel-office",
        next_mesh_priority=2,
    )
    entry.add_to_hass(hass)
    exact = _disk_config_entries_envelope(entry)
    stale = deepcopy(exact)
    entries = cast(dict[str, Any], stale["data"])["entries"]
    cast(dict[str, Any], entries[0])["subentries"][0]["unique_id"] = "SHA256:stale"

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.json_util.load_json",
            side_effect=[stale, {"malformed": True}, exact],
        ) as load_json,
        patch(
            "custom_components.brilliant_mqtt.fleet_manager._CONFIG_ENTRY_PERSISTENCE_POLL_SECONDS",
            0,
        ),
    ):
        await async_wait_config_entry_persisted(
            hass,
            entry,
            subentry_id="panel-office",
        )

    assert load_json.call_count == 3


async def test_strict_persistence_retries_nonownership_metadata_drift(
    hass: HomeAssistant,
) -> None:
    """A title/timestamp race retries while the frozen ownership fields stay exact."""
    entry = _provisioning_entry(
        _panel(
            "office",
            "SHA256:office",
            subentry_id="panel-office",
            management_id=_OFFICE_FINGERPRINT,
        ),
        scene_owner="panel-office",
        next_mesh_priority=2,
    )
    entry.add_to_hass(hass)
    before = _disk_config_entries_envelope(entry)
    reads = 0

    async def fresh_read(
        load_json: Callable[[str], object],
        path: str,
    ) -> object:
        del load_json, path
        nonlocal reads
        reads += 1
        if reads == 1:
            hass.config_entries.async_update_entry(entry, title="Renamed fleet")
            return before
        return _disk_config_entries_envelope(entry)

    with patch.object(
        hass,
        "async_add_executor_job",
        side_effect=fresh_read,
    ):
        await async_wait_config_entry_persisted(
            hass,
            entry,
            subentry_id="panel-office",
        )

    assert reads == 2


async def test_strict_persistence_rejects_ownership_drift_during_disk_read(
    hass: HomeAssistant,
) -> None:
    """Live ownership drift invalidates an otherwise exact stale disk fragment."""
    entry = _provisioning_entry(
        _panel(
            "office",
            "SHA256:office",
            subentry_id="panel-office",
            management_id=_OFFICE_FINGERPRINT,
        ),
        scene_owner="panel-office",
        next_mesh_priority=2,
    )
    entry.add_to_hass(hass)
    before = _disk_config_entries_envelope(entry)

    async def drifting_read(
        load_json: Callable[[str], object],
        path: str,
    ) -> object:
        del load_json, path
        hass.config_entries.async_update_entry(
            entry,
            data={**entry.data, CONF_HA_CONTROL_LABEL: "changed-owner-data"},
        )
        return before

    with (
        patch.object(
            hass,
            "async_add_executor_job",
            side_effect=drifting_read,
        ),
        pytest.raises(
            ConfigEntryPersistenceError,
            match="config_entry_storage_unavailable",
        ),
    ):
        await async_wait_config_entry_persisted(
            hass,
            entry,
            subentry_id="panel-office",
        )


async def test_strict_persistence_disk_failure_is_bounded_and_redacted(
    hass: HomeAssistant,
) -> None:
    """Disk failures collapse to one fixed error without leaking exception text."""
    entry = _empty_fleet_entry()
    entry.add_to_hass(hass)
    secret = "never-log-this-storage-secret"

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.json_util.load_json",
            side_effect=OSError(secret),
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager._CONFIG_ENTRY_PERSISTENCE_TIMEOUT_SECONDS",
            0,
        ),
        pytest.raises(ConfigEntryPersistenceError) as captured,
    ):
        await async_wait_config_entry_persisted(hass, entry)

    assert str(captured.value) == "config_entry_storage_unavailable"
    assert secret not in repr(captured.value)


async def test_strict_persistence_propagates_cancellation(
    hass: HomeAssistant,
) -> None:
    """Cancellation interrupts a disk proof without translating its identity."""
    entry = _empty_fleet_entry()
    entry.add_to_hass(hass)
    started = asyncio.Event()
    blocked = asyncio.Event()

    async def blocked_read(
        load_json: Callable[[str], object],
        path: str,
    ) -> object:
        del load_json, path
        started.set()
        await blocked.wait()
        return _disk_config_entries_envelope(entry)

    with patch.object(
        hass,
        "async_add_executor_job",
        side_effect=blocked_read,
    ):
        proof = asyncio.create_task(async_wait_config_entry_persisted(hass, entry))
        await started.wait()
        proof.cancel()
        with pytest.raises(asyncio.CancelledError):
            await proof


async def test_cancelled_ownership_flush_settles_without_clearing_journal(
    hass: HomeAssistant,
) -> None:
    """Caller cancellation is preserved only after the storage task has settled."""
    entry = _provisioning_entry(
        _panel(
            "office",
            "SHA256:office",
            subentry_id="panel-office",
            management_id=_OFFICE_FINGERPRINT,
        ),
        scene_owner="panel-office",
        next_mesh_priority=2,
    )
    entry.add_to_hass(hass)
    journal = Mock(
        async_load=AsyncMock(return_value=_pending_record()),
        async_complete_commit=AsyncMock(),
    )
    save_started = asyncio.Event()
    allow_save = asyncio.Event()

    async def blocked_save(
        hass_arg: HomeAssistant,
        entry_arg: MockConfigEntry,
        *,
        subentry_id: str | None = None,
    ) -> None:
        assert hass_arg is hass
        assert entry_arg is entry
        assert subentry_id == "panel-office"
        save_started.set()
        await allow_save.wait()

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
            side_effect=blocked_save,
        ),
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        setup = asyncio.create_task(FleetManager(hass, entry).async_setup())
        await save_started.wait()
        setup.cancel()
        await asyncio.sleep(0)
        assert not setup.done()
        allow_save.set()
        with pytest.raises(asyncio.CancelledError):
            await setup

    journal.async_complete_commit.assert_not_awaited()


@pytest.mark.parametrize(
    "panels",
    [
        (
            _panel(
                "office",
                "SHA256:office",
                subentry_id="panel-office",
                management_id=_OFFICE_FINGERPRINT,
                transaction_id=_OTHER_TRANSACTION_ID,
            ),
        ),
        (
            _panel(
                "office",
                "SHA256:office",
                subentry_id="panel-office",
                management_id=_OFFICE_FINGERPRINT,
                transaction_id=_TRANSACTION_ID,
            ),
            _panel(
                "kitchen",
                "SHA256:kitchen",
                subentry_id="panel-kitchen",
                transaction_id=_TRANSACTION_ID,
            ),
        ),
        (
            _panel(
                "kitchen",
                "SHA256:kitchen",
                subentry_id="panel-kitchen",
                transaction_id=_TRANSACTION_ID,
            ),
        ),
        (
            _panel(
                "office",
                "SHA256:office",
                subentry_id="panel-office",
                management_id=_OFFICE_FINGERPRINT,
            ),
        ),
    ],
    ids=[
        "different_transaction",
        "multiple_markers",
        "wrong_identity",
        "marker_removed_before_scene_normalization",
    ],
)
async def test_pending_handoff_mismatch_fails_closed_without_clearing_journal(
    hass: HomeAssistant,
    panels: tuple[ConfigSubentry, ...],
) -> None:
    entry = _provisioning_entry(
        *panels,
        scene_owner=pending_scene_owner(_TRANSACTION_ID),
    )
    entry.add_to_hass(hass)
    journal = Mock(
        async_load=AsyncMock(return_value=_pending_record()),
        async_complete_commit=AsyncMock(),
    )

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch.object(PanelManager, "async_setup", _noop_setup),
        pytest.raises(EntryDataError, match="provisioning_ownership_mismatch"),
    ):
        await FleetManager(hass, entry).async_setup()

    journal.async_complete_commit.assert_not_awaited()
    assert entry.data[CONF_SCENE_PANEL] == pending_scene_owner(_TRANSACTION_ID)
    issue_id = f"provisioning_rollback_{_TRANSACTION_ID.hex}"
    assert ir.async_get(hass).async_get_issue(DOMAIN, issue_id) is not None


async def test_pending_handoff_rejects_different_journaled_broker(
    hass: HomeAssistant,
) -> None:
    """Panel identity cannot claim a journal created for a different MQTT fleet."""
    entry = _provisioning_entry(
        _panel(
            "office",
            "SHA256:office",
            subentry_id="panel-office",
            management_id=_OFFICE_FINGERPRINT,
            transaction_id=_TRANSACTION_ID,
        ),
        scene_owner=pending_scene_owner(_TRANSACTION_ID),
    )
    entry.add_to_hass(hass)
    hass.config_entries.async_update_entry(
        entry,
        data={**entry.data, CONF_MQTT_HOST: "different-broker.example.com"},
    )
    journal = Mock(
        async_load=AsyncMock(return_value=_pending_record()),
        async_complete_commit=AsyncMock(),
    )

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
        pytest.raises(EntryDataError, match="provisioning_ownership_mismatch"),
    ):
        await FleetManager(hass, entry).async_setup()

    journal.async_complete_commit.assert_not_awaited()
    assert entry.data[CONF_SCENE_PANEL] == pending_scene_owner(_TRANSACTION_ID)
    issue_id = f"provisioning_rollback_{_TRANSACTION_ID.hex}"
    assert ir.async_get(hass).async_get_issue(DOMAIN, issue_id) is not None


async def test_pending_handoff_accepts_guidance_only_broker_kind_correction(
    hass: HomeAssistant,
) -> None:
    """Broker kind guidance cannot invalidate an unchanged journal transport."""
    entry = _provisioning_entry(
        _panel(
            "office",
            "SHA256:office",
            subentry_id="panel-office",
            management_id=_OFFICE_FINGERPRINT,
            transaction_id=_TRANSACTION_ID,
        ),
        scene_owner=pending_scene_owner(_TRANSACTION_ID),
    )
    entry.add_to_hass(hass)
    hass.config_entries.async_update_entry(
        entry,
        data={
            **entry.data,
            CONF_BROKER_KIND: BrokerKind.OFFICIAL_MOSQUITTO.value,
        },
    )
    journal = Mock(
        async_load=AsyncMock(return_value=_pending_record()),
        async_complete_commit=AsyncMock(),
    )
    persisted = AsyncMock()
    fleet = FleetManager(hass, entry)

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
            persisted,
        ),
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()

    journal.async_complete_commit.assert_awaited_once_with(
        _TRANSACTION_ID,
        subentry_id="panel-office",
    )
    assert entry.data[CONF_BROKER_KIND] == BrokerKind.OFFICIAL_MOSQUITTO.value
    assert entry.data[CONF_SCENE_PANEL] == "panel-office"
    assert persisted.await_count == 2

    with patch.object(PanelManager, "async_shutdown", _noop_shutdown):
        await fleet.async_shutdown()


async def test_startup_without_stored_owner_invokes_recorded_recovery_before_setup(
    hass: HomeAssistant,
) -> None:
    """An absent config-flow result rolls back instead of claiming another panel."""
    entry = _provisioning_entry(
        _panel("kitchen", "SHA256:kitchen", subentry_id="panel-kitchen"),
        scene_owner="panel-kitchen",
        next_mesh_priority=2,
    )
    entry.add_to_hass(hass)
    journal = Mock(
        async_load=AsyncMock(side_effect=[_pending_record(), None]),
        async_complete_commit=AsyncMock(),
    )
    recover = AsyncMock()
    fleet = FleetManager(hass, entry)

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager._get_recovery_provisioner",
            return_value=Mock(async_recover=recover),
        ),
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()

    recover.assert_awaited_once_with()
    journal.async_complete_commit.assert_not_awaited()
    assert set(fleet.panels) == {"panel-kitchen"}

    with patch.object(PanelManager, "async_shutdown", _noop_shutdown):
        await fleet.async_shutdown()


async def test_unreachable_stale_recovery_does_not_hold_healthy_runtime_offline(
    hass: HomeAssistant,
) -> None:
    """Network rollback is an entry-owned background repair, not a setup dependency."""
    entry = _provisioning_entry(
        _panel("kitchen", "SHA256:kitchen", subentry_id="panel-kitchen"),
        scene_owner="panel-kitchen",
        next_mesh_priority=2,
    )
    entry.add_to_hass(hass)
    journal = Mock(
        async_load=AsyncMock(return_value=_pending_record(phase=ProvisioningPhase.ACTIVATED))
    )
    recover_started = asyncio.Event()
    release_recovery = asyncio.Event()

    async def blocked_recovery() -> None:
        recover_started.set()
        await release_recovery.wait()
        raise OSError("unreachable stale panel")

    fleet = FleetManager(hass, entry)
    persisted = AsyncMock()
    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
            persisted,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager._get_recovery_provisioner",
            return_value=Mock(async_recover=blocked_recovery),
        ),
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await asyncio.wait_for(fleet.async_setup(), timeout=1)
        await asyncio.wait_for(recover_started.wait(), timeout=1)
        assert set(fleet.panels) == {"panel-kitchen"}
        release_recovery.set()
        assert fleet._recovery_task is not None  # noqa: SLF001
        await fleet._recovery_task  # noqa: SLF001

    issue_id = f"provisioning_rollback_{_TRANSACTION_ID.hex}"
    assert ir.async_get(hass).async_get_issue(DOMAIN, issue_id) is not None
    with patch.object(PanelManager, "async_shutdown", _noop_shutdown):
        await fleet.async_shutdown()


async def test_finished_background_recovery_is_retried_after_entry_update(
    hass: HomeAssistant,
) -> None:
    """A transient recovery failure cannot suppress the same journal forever."""
    entry = _provisioning_entry(
        _panel("kitchen", "SHA256:kitchen", subentry_id="panel-kitchen"),
        scene_owner="panel-kitchen",
        next_mesh_priority=2,
    )
    entry.add_to_hass(hass)
    state: dict[str, ProvisioningRecord | None] = {
        "record": _pending_record(phase=ProvisioningPhase.ACTIVATED)
    }

    async def load_record() -> ProvisioningRecord | None:
        return state["record"]

    journal = Mock(async_load=AsyncMock(side_effect=load_record))
    attempts = 0

    async def recover() -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("panel temporarily unreachable")
        state["record"] = None

    fleet = FleetManager(hass, entry)
    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager._get_recovery_provisioner",
            return_value=Mock(async_recover=recover),
        ),
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
        patch.object(
            hass.config_entries,
            "async_schedule_reload",
        ) as schedule_reload,
    ):
        await fleet.async_setup()
        first = fleet._recovery_task  # noqa: SLF001
        assert first is not None
        await first

        await fleet._async_reconcile()  # noqa: SLF001
        second = fleet._recovery_task  # noqa: SLF001
        assert second is not None
        assert second is not first
        await second

    assert attempts == 2
    schedule_reload.assert_called_once_with(entry.entry_id)
    with patch.object(PanelManager, "async_shutdown", _noop_shutdown):
        await fleet.async_shutdown()


async def test_stale_allocator_hint_does_not_wait_for_storage_before_healthy_runtime(
    hass: HomeAssistant,
) -> None:
    """A recomputable priority repair uses HA's save queue, not the ownership barrier."""
    entry = _provisioning_entry(
        _panel("office", "SHA256:office", subentry_id="panel-office"),
        scene_owner="panel-office",
        next_mesh_priority=99,
    )
    entry.add_to_hass(hass)
    journal = Mock(async_load=AsyncMock(return_value=None))
    persisted = AsyncMock(side_effect=OSError("storage is slow"))
    fleet = FleetManager(hass, entry)

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
            persisted,
        ),
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()

    assert entry.data[CONF_NEXT_MESH_PRIORITY] == 2
    persisted.assert_not_awaited()
    assert set(fleet.panels) == {"panel-office"}
    with patch.object(PanelManager, "async_shutdown", _noop_shutdown):
        await fleet.async_shutdown()


@pytest.mark.parametrize(
    "phase",
    (
        ProvisioningPhase.PENDING_CONFIG_COMMIT,
        ProvisioningPhase.COMMITTED,
    ),
)
@pytest.mark.parametrize("competitor_kind", ("fleet", "legacy"))
async def test_startup_preserves_owned_journal_when_any_domain_competitor_survives(
    hass: HomeAssistant,
    phase: ProvisioningPhase,
    competitor_kind: str,
) -> None:
    """No candidate handoff is consumed while singleton ownership is ambiguous."""
    entry = (
        _provisioning_entry(
            _panel(
                "office",
                "SHA256:office",
                subentry_id="panel-office",
                management_id=_OFFICE_FINGERPRINT,
            ),
            scene_owner="panel-office",
            next_mesh_priority=2,
        )
        if phase is ProvisioningPhase.COMMITTED
        else _pending_owner_entry()
    )
    competitor = _empty_fleet_entry() if competitor_kind == "fleet" else _legacy_entry()
    entry.add_to_hass(hass)
    competitor.add_to_hass(hass)
    journal = Mock(
        async_load=AsyncMock(return_value=_pending_record(phase=phase)),
        async_clear_committed=AsyncMock(),
        async_complete_commit=AsyncMock(),
    )
    persisted = AsyncMock()
    setup = AsyncMock()

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
            persisted,
        ),
        patch.object(PanelManager, "async_setup", setup),
        pytest.raises(EntryDataError, match="provisioning_ownership_mismatch"),
    ):
        await FleetManager(hass, entry).async_setup()

    journal.async_clear_committed.assert_not_awaited()
    journal.async_complete_commit.assert_not_awaited()
    persisted.assert_not_awaited()
    setup.assert_not_awaited()
    issue_id = f"provisioning_rollback_{_TRANSACTION_ID.hex}"
    assert ir.async_get(hass).async_get_issue(DOMAIN, issue_id) is not None


async def test_empty_fleet_with_legacy_competitor_reports_without_orphan_recovery(
    hass: HomeAssistant,
) -> None:
    """A legacy entry makes a pre-subentry journal unsafe to consume."""
    entry = _empty_fleet_entry()
    legacy = _legacy_entry()
    entry.add_to_hass(hass)
    legacy.add_to_hass(hass)
    journal = Mock(
        async_load=AsyncMock(return_value=_pending_record(phase=ProvisioningPhase.STAGED))
    )
    recover = AsyncMock()
    fleet = FleetManager(hass, entry)

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager._get_recovery_provisioner",
            return_value=Mock(async_recover=recover),
        ),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
        pytest.raises(EntryDataError, match="provisioning_ownership_mismatch"),
    ):
        await fleet.async_setup()

    recover.assert_not_awaited()
    assert fleet._recovery_task is None  # noqa: SLF001
    issue_id = f"provisioning_rollback_{_TRANSACTION_ID.hex}"
    assert ir.async_get(hass).async_get_issue(DOMAIN, issue_id) is not None


@pytest.mark.parametrize(
    "phase",
    (
        ProvisioningPhase.STAGED,
        ProvisioningPhase.ACTIVATED,
        ProvisioningPhase.PENDING_CONFIG_COMMIT,
    ),
)
async def test_hard_restart_empty_fleet_schedules_orphan_recovery(
    hass: HomeAssistant,
    phase: ProvisioningPhase,
) -> None:
    """A durable empty fleet anchors its pre-subentry journal across hard restart."""
    entry = _empty_fleet_entry()
    entry.add_to_hass(hass)
    journal = Mock(async_load=AsyncMock(return_value=_pending_record(phase=phase)))
    recover = AsyncMock()
    fleet = FleetManager(hass, entry)

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager._get_recovery_provisioner",
            return_value=Mock(async_recover=recover),
        ),
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        assert fleet._recovery_task is not None  # noqa: SLF001
        await fleet._recovery_task  # noqa: SLF001

    recover.assert_awaited_once_with()
    assert fleet.panels == {}
    assert fleet.fleet.scene_panel == "__unassigned__"
    with patch.object(PanelManager, "async_shutdown", _noop_shutdown):
        await fleet.async_shutdown()


async def test_live_subentry_flow_prevents_reload_from_rolling_back_staged_panel(
    hass: HomeAssistant,
) -> None:
    """The STAGING transaction marker keeps a concurrent fleet reload read-only."""
    entry = _empty_fleet_entry()
    entry.add_to_hass(hass)
    journal = Mock(
        async_load=AsyncMock(return_value=_pending_record(phase=ProvisioningPhase.STAGED))
    )
    recover = AsyncMock()
    active = [
        {
            "context": {
                CONF_PROVISIONING_TRANSACTION_ID: str(_TRANSACTION_ID),
            }
        }
    ]
    fleet = FleetManager(hass, entry)

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager._get_recovery_provisioner",
            return_value=Mock(async_recover=recover),
        ),
        patch.object(
            hass.config_entries.flow,
            "async_progress",
            return_value=[],
        ),
        patch.object(
            hass.config_entries.subentries,
            "async_progress",
            return_value=active,
        ),
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()

    recover.assert_not_awaited()
    with patch.object(PanelManager, "async_shutdown", _noop_shutdown):
        await fleet.async_shutdown()


async def test_active_flow_context_prevents_pre_storage_rollback(
    hass: HomeAssistant,
) -> None:
    """An unrelated fleet update cannot roll back a flow awaiting HA persistence."""
    journal = Mock(async_load=AsyncMock(return_value=_pending_record()))
    active = [
        {
            "context": {
                CONF_PROVISIONING_TRANSACTION_ID: str(_TRANSACTION_ID),
            }
        }
    ]

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch.object(
            hass.config_entries.flow,
            "async_progress",
            return_value=active,
        ),
        patch.object(
            hass.config_entries.subentries,
            "async_progress",
            return_value=[],
        ),
    ):
        lookup = await _async_transaction_lookup(hass, _TRANSACTION_ID)

    assert lookup.state is TransactionLookupState.FLOW_PENDING
    assert lookup.subentry_id is None


async def test_actual_ha_remove_recovers_exact_removed_sole_owner(
    hass: HomeAssistant,
) -> None:
    """Registry deletion turns the removed exact owner into rollback eligibility."""
    entry = _pending_owner_entry()
    entry.add_to_hass(hass)
    journal = Mock(async_load=AsyncMock(return_value=_pending_record()))
    recover = AsyncMock()
    issue_id = _create_provisioning_repair(hass)

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager._get_recovery_provisioner",
            return_value=Mock(async_recover=recover),
        ) as get_recovery,
    ):
        await hass.config_entries.async_remove(entry.entry_id)

    assert hass.config_entries.async_get_entry(entry.entry_id) is None
    get_recovery.assert_called_once_with(hass)
    recover.assert_awaited_once_with()
    assert ir.async_get(hass).async_get_issue(DOMAIN, issue_id) is None


@pytest.mark.parametrize(
    "phase",
    (
        ProvisioningPhase.STAGED,
        ProvisioningPhase.ACTIVATED,
        ProvisioningPhase.PENDING_CONFIG_COMMIT,
    ),
)
async def test_actual_ha_remove_recovers_exact_empty_fleet_before_subentry(
    hass: HomeAssistant,
    phase: ProvisioningPhase,
) -> None:
    """The removed durable bootstrap owner settles every pre-subentry crash phase."""
    entry = _empty_fleet_entry()
    entry.add_to_hass(hass)
    journal = Mock(async_load=AsyncMock(return_value=_pending_record(phase=phase)))
    recover = AsyncMock()

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager._get_recovery_provisioner",
            return_value=Mock(async_recover=recover),
        ),
        patch(
            "custom_components.brilliant_mqtt.async_setup_entry",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "custom_components.brilliant_mqtt.async_unload_entry",
            new=AsyncMock(return_value=True),
        ),
    ):
        await hass.config_entries.async_remove(entry.entry_id)

    assert hass.config_entries.async_get_entry(entry.entry_id) is None
    recover.assert_awaited_once_with()


@pytest.mark.parametrize(
    "phase",
    (
        ProvisioningPhase.STAGED,
        ProvisioningPhase.ACTIVATION_PENDING,
        ProvisioningPhase.ACTIVATED,
        ProvisioningPhase.VERIFYING,
        ProvisioningPhase.PENDING_CONFIG_COMMIT,
        ProvisioningPhase.ROLLBACK_PENDING,
        ProvisioningPhase.ROLLED_BACK,
    ),
)
async def test_actual_ha_remove_recovers_pre_subentry_panel_from_nonempty_singleton_fleet(
    hass: HomeAssistant,
    phase: ProvisioningPhase,
) -> None:
    """An exact sole fleet owns pre-subentry rollback despite existing panels."""
    entry = _provisioning_entry(
        _panel(
            "bedroom",
            "SHA256:bedroom",
            subentry_id="panel-bedroom",
        ),
        scene_owner="panel-bedroom",
        next_mesh_priority=2,
    )
    entry.add_to_hass(hass)
    journal = Mock(async_load=AsyncMock(return_value=_pending_record(phase=phase)))
    recover = AsyncMock()
    issue_id = _create_provisioning_repair(hass)

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager._get_recovery_provisioner",
            return_value=Mock(async_recover=recover),
        ),
        patch(
            "custom_components.brilliant_mqtt.async_setup_entry",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "custom_components.brilliant_mqtt.async_unload_entry",
            new=AsyncMock(return_value=True),
        ),
    ):
        await hass.config_entries.async_remove(entry.entry_id)

    assert hass.config_entries.async_get_entry(entry.entry_id) is None
    recover.assert_awaited_once_with()
    assert ir.async_get(hass).async_get_issue(DOMAIN, issue_id) is None


async def test_actual_ha_remove_preserves_unowned_pre_subentry_committed_phase(
    hass: HomeAssistant,
) -> None:
    """COMMITTED requires an exact journal-owned subentry even in a sole fleet."""
    entry = _provisioning_entry(
        _panel(
            "bedroom",
            "SHA256:bedroom",
            subentry_id="panel-bedroom",
        ),
        scene_owner="panel-bedroom",
        next_mesh_priority=2,
    )
    entry.add_to_hass(hass)
    journal = Mock(
        async_load=AsyncMock(return_value=_pending_record(phase=ProvisioningPhase.COMMITTED)),
        async_clear_committed=AsyncMock(),
    )
    recover = AsyncMock()

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager._get_recovery_provisioner",
            return_value=Mock(async_recover=recover),
        ),
        patch(
            "custom_components.brilliant_mqtt.async_setup_entry",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "custom_components.brilliant_mqtt.async_unload_entry",
            new=AsyncMock(return_value=True),
        ),
    ):
        await hass.config_entries.async_remove(entry.entry_id)

    recover.assert_not_awaited()
    journal.async_clear_committed.assert_not_awaited()
    issue_id = f"provisioning_rollback_{_TRANSACTION_ID.hex}"
    assert ir.async_get(hass).async_get_issue(DOMAIN, issue_id) is not None


async def test_actual_ha_remove_clears_committed_exact_owner_without_panel_network(
    hass: HomeAssistant,
) -> None:
    """COMMITTED proves storage ownership, so removal only purges the secret journal."""
    entry = _pending_owner_entry()
    entry.add_to_hass(hass)
    journal = Mock(
        async_load=AsyncMock(return_value=_pending_record(phase=ProvisioningPhase.COMMITTED)),
        async_clear_committed=AsyncMock(),
    )
    get_recovery = Mock()
    active = [
        {
            "flow_id": "owned-panel-flow",
            "handler": (entry.entry_id, SUBENTRY_TYPE_PANEL),
            "context": {
                CONF_PROVISIONING_TRANSACTION_ID: str(_TRANSACTION_ID),
            },
        }
    ]
    abort = Mock()
    issue_id = _create_provisioning_repair(hass)

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager._get_recovery_provisioner",
            get_recovery,
        ),
        patch.object(
            hass.config_entries.subentries,
            "async_progress",
            return_value=active,
        ),
        patch.object(
            hass.config_entries.subentries,
            "async_abort",
            abort,
        ),
        patch(
            "custom_components.brilliant_mqtt.async_setup_entry",
            new=AsyncMock(return_value=True),
        ),
        patch(
            "custom_components.brilliant_mqtt.async_unload_entry",
            new=AsyncMock(return_value=True),
        ),
    ):
        await hass.config_entries.async_remove(entry.entry_id)

    journal.async_clear_committed.assert_awaited_once_with(_TRANSACTION_ID)
    get_recovery.assert_not_called()
    abort.assert_called_once_with("owned-panel-flow")
    assert ir.async_get(hass).async_get_issue(DOMAIN, issue_id) is None


async def test_removed_entry_wrong_handoff_phase_preserves_journal_and_reports_repair(
    hass: HomeAssistant,
) -> None:
    """Removal stays fail-closed but never leaves the retained root secret silent."""
    entry = _pending_owner_entry()
    journal = Mock(
        async_load=AsyncMock(return_value=_pending_record(phase=ProvisioningPhase.VERIFYING))
    )

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager._get_recovery_provisioner",
        ) as get_recovery,
    ):
        await async_recover_removed_entry(hass, entry)

    get_recovery.assert_not_called()
    issue_id = f"provisioning_rollback_{_TRANSACTION_ID.hex}"
    issue = ir.async_get(hass).async_get_issue(DOMAIN, issue_id)
    assert issue is not None
    assert issue.translation_key == "needs_attention"


async def test_removed_entry_wrong_broker_preserves_journal_and_reports_repair(
    hass: HomeAssistant,
) -> None:
    """A broker-envelope mismatch cannot silently retain a root secret."""
    entry = _pending_owner_entry()
    entry.add_to_hass(hass)
    hass.config_entries.async_update_entry(
        entry,
        data={**entry.data, CONF_MQTT_HOST: "different-broker.example.com"},
    )
    journal = Mock(async_load=AsyncMock(return_value=_pending_record()))

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager._get_recovery_provisioner",
        ) as get_recovery,
    ):
        await hass.config_entries.async_remove(entry.entry_id)

    get_recovery.assert_not_called()
    issue_id = f"provisioning_rollback_{_TRANSACTION_ID.hex}"
    issue = ir.async_get(hass).async_get_issue(DOMAIN, issue_id)
    assert issue is not None
    assert issue.translation_key == "needs_attention"


async def test_actual_ha_remove_aborts_owned_flow_and_settles_pending_journal(
    hass: HomeAssistant,
) -> None:
    """Removal cancels its exact flow before settling the durable transaction."""
    entry = _pending_owner_entry()
    entry.add_to_hass(hass)
    journal = Mock(async_load=AsyncMock(return_value=_pending_record()))
    active = [
        {
            "flow_id": "owned-panel-flow",
            "handler": (entry.entry_id, SUBENTRY_TYPE_PANEL),
            "context": {
                CONF_PROVISIONING_TRANSACTION_ID: str(_TRANSACTION_ID),
            },
        }
    ]
    abort = Mock()
    recover = AsyncMock()

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch.object(
            hass.config_entries.subentries,
            "async_progress",
            return_value=active,
        ),
        patch.object(
            hass.config_entries.subentries,
            "async_abort",
            abort,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager._get_recovery_provisioner",
            return_value=Mock(async_recover=recover),
        ),
    ):
        await hass.config_entries.async_remove(entry.entry_id)

    abort.assert_called_once_with("owned-panel-flow")
    recover.assert_awaited_once_with()


async def test_actual_ha_remove_does_not_abort_unrelated_active_flow(
    hass: HomeAssistant,
) -> None:
    """A transaction marker alone cannot authorize aborting another owner's flow."""
    entry = _pending_owner_entry()
    entry.add_to_hass(hass)
    journal = Mock(async_load=AsyncMock(return_value=_pending_record()))
    unrelated = [
        {
            "flow_id": "other-panel-flow",
            "handler": ("another-entry", SUBENTRY_TYPE_PANEL),
            "context": {
                CONF_PROVISIONING_TRANSACTION_ID: str(_TRANSACTION_ID),
            },
        },
        {
            "flow_id": "other-kind-flow",
            "handler": (entry.entry_id, "another-kind"),
            "context": {
                CONF_PROVISIONING_TRANSACTION_ID: str(_TRANSACTION_ID),
            },
        },
    ]
    abort = Mock()
    recover = AsyncMock()

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch.object(
            hass.config_entries.subentries,
            "async_progress",
            return_value=unrelated,
        ),
        patch.object(
            hass.config_entries.subentries,
            "async_abort",
            abort,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager._get_recovery_provisioner",
            return_value=Mock(async_recover=recover),
        ),
    ):
        await hass.config_entries.async_remove(entry.entry_id)

    abort.assert_not_called()
    recover.assert_awaited_once_with()


async def test_removed_empty_fleet_with_legacy_competitor_preserves_journal(
    hass: HomeAssistant,
) -> None:
    """A surviving legacy entry prevents ownership claims before any rollback."""
    removed = _empty_fleet_entry()
    legacy = _legacy_entry()
    removed.add_to_hass(hass)
    legacy.add_to_hass(hass)
    journal = Mock(
        async_load=AsyncMock(return_value=_pending_record(phase=ProvisioningPhase.STAGED)),
        async_clear_committed=AsyncMock(),
    )
    abort = Mock()
    recover = AsyncMock()

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch.object(
            hass.config_entries.subentries,
            "async_abort",
            abort,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager._get_recovery_provisioner",
            return_value=Mock(async_recover=recover),
        ),
    ):
        await hass.config_entries.async_remove(removed.entry_id)

    assert hass.config_entries.async_get_entry(legacy.entry_id) is legacy
    abort.assert_not_called()
    recover.assert_not_awaited()
    journal.async_clear_committed.assert_not_awaited()
    issue_id = f"provisioning_rollback_{_TRANSACTION_ID.hex}"
    assert ir.async_get(hass).async_get_issue(DOMAIN, issue_id) is not None


async def test_removed_committed_owner_with_legacy_competitor_preserves_journal(
    hass: HomeAssistant,
) -> None:
    """COMMITTED is clear-only only after global singleton ownership is proven."""
    removed = _pending_owner_entry()
    legacy = _legacy_entry()
    removed.add_to_hass(hass)
    legacy.add_to_hass(hass)
    journal = Mock(
        async_load=AsyncMock(return_value=_pending_record(phase=ProvisioningPhase.COMMITTED)),
        async_clear_committed=AsyncMock(),
    )
    abort = Mock()
    get_recovery = Mock()

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch.object(
            hass.config_entries.subentries,
            "async_abort",
            abort,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager._get_recovery_provisioner",
            get_recovery,
        ),
    ):
        await hass.config_entries.async_remove(removed.entry_id)

    assert hass.config_entries.async_get_entry(legacy.entry_id) is legacy
    abort.assert_not_called()
    journal.async_clear_committed.assert_not_awaited()
    get_recovery.assert_not_called()
    issue_id = f"provisioning_rollback_{_TRANSACTION_ID.hex}"
    assert ir.async_get(hass).async_get_issue(DOMAIN, issue_id) is not None


async def test_actual_ha_remove_preserves_another_exact_persisted_owner(
    hass: HomeAssistant,
) -> None:
    """A surviving exact owner keeps the pending handoff out of rollback."""
    removed = _pending_owner_entry()
    survivor = _pending_owner_entry()
    removed.add_to_hass(hass)
    survivor.add_to_hass(hass)
    journal = Mock(async_load=AsyncMock(return_value=_pending_record()))

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager._get_recovery_provisioner",
        ) as get_recovery,
    ):
        await hass.config_entries.async_remove(removed.entry_id)

    assert hass.config_entries.async_get_entry(survivor.entry_id) is survivor
    get_recovery.assert_not_called()


async def test_actual_ha_remove_drains_recovery_before_propagating_cancellation(
    hass: HomeAssistant,
) -> None:
    entry = _pending_owner_entry()
    entry.add_to_hass(hass)
    journal = Mock(async_load=AsyncMock(return_value=_pending_record()))
    entered = asyncio.Event()
    release = asyncio.Event()
    finished = asyncio.Event()

    async def recover() -> None:
        entered.set()
        await release.wait()
        finished.set()

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager._get_recovery_provisioner",
            return_value=Mock(async_recover=recover),
        ),
    ):
        removal = hass.async_create_task(
            hass.config_entries.async_remove(entry.entry_id),
            "test-remove-pending-owner",
        )
        await asyncio.wait_for(entered.wait(), timeout=1)
        assert hass.config_entries.async_get_entry(entry.entry_id) is None
        removal.cancel()
        await asyncio.sleep(0)
        assert not removal.done()
        assert not finished.is_set()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await removal

    assert finished.is_set()


async def test_actual_ha_remove_recovery_failure_keeps_one_redacted_issue(
    hass: HomeAssistant,
    caplog: pytest.LogCaptureFixture,
) -> None:
    entry = _pending_owner_entry()
    entry.add_to_hass(hass)
    journal = Mock(
        async_load=AsyncMock(return_value=_pending_record()),
        async_complete_rollback=AsyncMock(),
    )
    registry = ir.async_get(hass)
    ir.async_create_issue(
        hass,
        DOMAIN,
        f"needs_attention_{_OFFICE_FINGERPRINT}",
        is_fixable=False,
        severity=ir.IssueSeverity.WARNING,
        translation_key="needs_attention",
        translation_placeholders={
            "panel": "Office",
            "reason": "unrelated cleanup",
        },
    )

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager._get_recovery_provisioner",
            return_value=Mock(async_recover=AsyncMock(side_effect=OSError("office-secret"))),
        ),
    ):
        await hass.config_entries.async_remove(entry.entry_id)

    assert (
        registry.async_get_issue(
            DOMAIN,
            f"needs_attention_{_OFFICE_FINGERPRINT}",
        )
        is None
    )
    issue = registry.async_get_issue(
        DOMAIN,
        f"provisioning_rollback_{_TRANSACTION_ID.hex}",
    )
    assert issue is not None
    assert issue.translation_key == "needs_attention"
    assert issue.translation_placeholders == {
        "panel": "Brilliant MQTT provisioning",
        "reason": (
            "Automatic provisioning rollback did not finish. Retry panel onboarding or "
            "follow the provisioning recovery instructions."
        ),
    }
    assert "office-secret" not in repr(issue)
    assert "office-secret" not in caplog.text
    journal.async_complete_rollback.assert_not_awaited()


async def test_provisioner_factory_reverifies_confirmed_pin_and_rechecks_duplicates(
    hass: HomeAssistant,
) -> None:
    """Every flow shares one lock and performs late identity/duplicate checks."""
    expected = HostIdentity(_OFFICE_PUBLIC_KEY, _OFFICE_FINGERPRINT)
    verified = AsyncMock(return_value=expected)

    with patch(
        "custom_components.brilliant_mqtt.fleet_manager.async_verify_host_identity",
        verified,
    ):
        first = get_panel_provisioner(hass, expected_identity=expected)
        second = get_panel_provisioner(hass, expected_identity=expected)
        assert first._lock is second._lock
        assert first._lock is hass.data[DOMAIN]["ssh_lock"]
        assert await first._identity_fetcher("office.example.com") == expected

    verified.assert_awaited_once_with(
        "office.example.com",
        expected=expected,
    )

    entry = _provisioning_entry(
        _panel(
            "office",
            "SHA256:office",
            subentry_id="panel-office",
            management_id=_OFFICE_FINGERPRINT,
        ),
        scene_owner="panel-office",
        next_mesh_priority=2,
    )
    entry.add_to_hass(hass)
    assert await first._duplicate_fingerprint(_OFFICE_FINGERPRINT) is True
    assert await first._duplicate_fingerprint(_THIRD_FINGERPRINT) is False

    with pytest.raises(ValueError, match="expected_panel_identity_required"):
        get_panel_provisioner(hass, expected_identity=cast(Any, object()))


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


async def test_provisioning_duplicate_guard_reserves_rebound_management_identity(
    hass: HomeAssistant,
) -> None:
    """Adding the old physical panel after A→X rebind cannot reuse stable identity A."""
    rebound = _panel(
        "office",
        "SHA256:bedroom",
        subentry_id="panel-office",
        management_id=_OFFICE_FINGERPRINT,
    )
    entry = _fleet_entry(rebound)
    entry.add_to_hass(hass)

    assert rebound.data[CONF_IDENTITY_FINGERPRINT] == _THIRD_FINGERPRINT
    assert rebound.data[CONF_MANAGEMENT_ID] == _OFFICE_FINGERPRINT
    assert await _async_duplicate_fingerprint(hass, _OFFICE_FINGERPRINT) is True


async def test_duplicate_positive_mesh_priority_fails_before_manager_start(
    hass: HomeAssistant,
) -> None:
    """Only one panel may publish each positive mesh leadership priority."""
    entry = _fleet_entry(
        _panel(
            "office",
            "SHA256:office",
            subentry_id="panel-office",
            mesh_priority=1,
        ),
        _panel(
            "kitchen",
            "SHA256:kitchen",
            subentry_id="panel-kitchen",
            mesh_priority=1,
        ),
    )
    starts: list[str] = []

    async def record_start(manager: PanelManager) -> None:
        starts.append(manager.panel)

    with (
        patch.object(PanelManager, "async_setup", record_start),
        pytest.raises(EntryDataError, match="duplicate_panel_mesh_priority"),
    ):
        await FleetManager(hass, entry).async_setup()

    assert starts == []


async def test_zero_mesh_priority_may_repeat_for_nonpublishing_panels(
    hass: HomeAssistant,
) -> None:
    """Priority zero opts out of leadership and therefore is not an ownership key."""
    entry = _fleet_entry(
        _panel(
            "office",
            "SHA256:office",
            subentry_id="panel-office",
            mesh_priority=0,
        ),
        _panel(
            "kitchen",
            "SHA256:kitchen",
            subentry_id="panel-kitchen",
            mesh_priority=0,
        ),
    )
    fleet = FleetManager(hass, entry)

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        assert set(fleet.panels) == {"panel-office", "panel-kitchen"}
        await fleet.async_shutdown()


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


async def test_rebind_durably_adopts_exact_identity_and_audits_after_proof(
    hass: HomeAssistant,
) -> None:
    """A deliberate rebind preserves logical identity and advances the live baseline."""
    office = _panel(
        "office",
        "SHA256:office",
        subentry_id="panel-office",
        management_id="stable-office-management",
    )
    entry = _fleet_entry(office)
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)
    order: list[str] = []
    immutable_checks: list[tuple[PanelConfig, PanelConfig]] = []
    events: list[Event[dict[str, Any]]] = []

    def capture_event(event: Event[dict[str, Any]]) -> None:
        events.append(event)

    hass.bus.async_listen(EVENT_TYPE, capture_event)
    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        manager = fleet.panels["panel-office"]
        original_store = manager.store
        expected = PanelConfig.from_subentry(entry.subentries["panel-office"])
        original_parent_data = deepcopy(dict(entry.data))
        original_subentry = entry.subentries["panel-office"]
        original_panel_data = deepcopy(dict(original_subentry.data))
        original_title = original_subentry.title
        candidate = HostIdentity(
            public_key=_OTHER_PUBLIC_KEY,
            fingerprint=_OTHER_FINGERPRINT,
        )
        new_password = "new-rebind-root-secret"

        async def prove_persisted(
            hass_arg: HomeAssistant,
            entry_arg: MockConfigEntry,
            *,
            subentry_id: str | None = None,
        ) -> None:
            assert hass_arg is hass
            assert entry_arg is entry
            assert subentry_id == "panel-office"
            assert fleet._lifecycle_lock.locked()
            stored = entry.subentries["panel-office"]
            assert stored.unique_id == candidate.fingerprint
            assert stored.data[CONF_SSH_HOST_KEY] == candidate.public_key
            assert manager.store is original_store
            assert fleet._panel_configs["panel-office"] == expected
            order.append("persisted")

        original_fire = manager._fire

        def record_audit(
            event_type: str,
            data: dict[str, Any] | None = None,
        ) -> None:
            order.append("audit")
            original_fire(event_type, data)

        original_assert = fleet._assert_immutable_panel

        def record_immutable_check(
            previous: PanelConfig,
            current: PanelConfig,
        ) -> None:
            immutable_checks.append((previous, current))
            original_assert(previous, current)

        with (
            patch(
                "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
                new=AsyncMock(side_effect=prove_persisted),
            ) as persisted,
            patch.object(manager, "_fire", side_effect=record_audit) as audit,
            patch.object(
                fleet,
                "_assert_immutable_panel",
                side_effect=record_immutable_check,
            ),
        ):
            await fleet.async_rebind_panel(
                "panel-office",
                expected,
                host="replacement.example.com",
                root_password=new_password,
                candidate=candidate,
            )
            await hass.async_block_till_done()

        stored = entry.subentries["panel-office"]
        rebound = PanelConfig.from_subentry(stored)
        assert stored is original_subentry
        assert stored.subentry_id == "panel-office"
        assert stored.title == original_title
        assert stored.unique_id == candidate.fingerprint
        assert set(stored.data) == set(original_panel_data)
        assert {
            key for key, old_value in original_panel_data.items() if stored.data[key] != old_value
        } == {
            CONF_HOST,
            CONF_ROOT_PASSWORD,
            CONF_IDENTITY_FINGERPRINT,
            CONF_SSH_HOST_KEY,
        }
        assert rebound.host == "replacement.example.com"
        assert rebound.root_password == new_password
        assert rebound.identity_fingerprint == candidate.fingerprint
        assert rebound.ssh_host_key == candidate.public_key
        assert rebound.name == expected.name
        assert rebound.panel == expected.panel
        assert rebound.management_id == expected.management_id
        assert rebound.components == expected.components
        assert rebound.feature_overrides == expected.feature_overrides
        assert rebound.mesh_priority == expected.mesh_priority
        assert entry.data == original_parent_data
        assert fleet.panels["panel-office"] is manager
        assert manager.store is not original_store
        assert manager.store.subentry_id == "panel-office"
        assert manager.store.data == stored.data
        assert fleet._panel_configs["panel-office"] == rebound
        assert order == ["persisted", "audit"]
        persisted.assert_awaited_once_with(
            hass,
            entry,
            subentry_id="panel-office",
        )
        audit.assert_called_once_with(
            EVENT_PANEL_REBOUND,
            {
                "old_fingerprint": expected.identity_fingerprint,
                "new_fingerprint": candidate.fingerprint,
            },
        )
        assert immutable_checks
        assert all(
            previous.identity_fingerprint == candidate.fingerprint
            and current.identity_fingerprint == candidate.fingerprint
            for previous, current in immutable_checks
        )
        assert len(events) == 1
        assert events[0].data == {
            "type": EVENT_PANEL_REBOUND,
            "panel": expected.panel,
            "entry_id": expected.management_id,
            "old_fingerprint": expected.identity_fingerprint,
            "new_fingerprint": candidate.fingerprint,
        }
        serialized_audit = repr(events[0].data)
        assert new_password not in serialized_audit
        assert candidate.public_key not in serialized_audit
        assert expected.root_password not in serialized_audit
        assert expected.ssh_host_key not in serialized_audit

        assert fleet._update_unsub is not None
        fleet._update_unsub()
        fleet._update_unsub = None
        assert hass.config_entries.async_update_subentry(
            entry,
            stored,
            data={
                **stored.data,
                CONF_IDENTITY_FINGERPRINT: _THIRD_FINGERPRINT,
                CONF_SSH_HOST_KEY: _THIRD_PUBLIC_KEY,
            },
            unique_id=_THIRD_FINGERPRINT,
        )
        with pytest.raises(EntryDataError, match="immutable_panel_identity"):
            await fleet._async_reconcile()

        await fleet.async_shutdown()


async def test_concurrent_rebinds_serialize_and_stale_snapshot_fails(
    hass: HomeAssistant,
) -> None:
    """Only the first contender may durably replace one expected identity snapshot."""
    entry = _fleet_entry(_panel("office", "SHA256:office", subentry_id="panel-office"))
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)
    proof_started = asyncio.Event()
    allow_proof = asyncio.Event()
    proofs = 0

    async def prove_persisted(
        hass_arg: HomeAssistant,
        entry_arg: MockConfigEntry,
        *,
        subentry_id: str | None = None,
    ) -> None:
        del hass_arg, entry_arg
        nonlocal proofs
        assert subentry_id == "panel-office"
        proofs += 1
        proof_started.set()
        await allow_proof.wait()

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        assert fleet._update_unsub is not None
        fleet._update_unsub()
        fleet._update_unsub = None
        expected = PanelConfig.from_subentry(entry.subentries["panel-office"])
        first_candidate = HostIdentity(_OTHER_PUBLIC_KEY, _OTHER_FINGERPRINT)
        second_candidate = HostIdentity(_THIRD_PUBLIC_KEY, _THIRD_FINGERPRINT)

        with patch(
            "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
            new=AsyncMock(side_effect=prove_persisted),
        ):
            first = asyncio.create_task(
                fleet.async_rebind_panel(
                    "panel-office",
                    expected,
                    host="first.example.com",
                    root_password="first-secret",
                    candidate=first_candidate,
                )
            )
            await proof_started.wait()
            second = asyncio.create_task(
                fleet.async_rebind_panel(
                    "panel-office",
                    expected,
                    host="second.example.com",
                    root_password="second-secret",
                    candidate=second_candidate,
                )
            )
            await asyncio.sleep(0)
            assert not second.done()

            allow_proof.set()
            await first
            with pytest.raises(EntryDataError, match="panel_snapshot_changed"):
                await second

        assert proofs == 1
        assert fleet._panel_configs["panel-office"].identity_fingerprint == _OTHER_FINGERPRINT
        assert fleet.panels["panel-office"].store.data[CONF_HOST] == "first.example.com"
        await fleet.async_shutdown()


async def test_rebind_waits_for_shared_ssh_operation_before_any_write_or_audit(
    hass: HomeAssistant,
) -> None:
    """An old-panel SSH operation must finish before identity ownership can move."""
    entry = _fleet_entry(_panel("office", "SHA256:office", subentry_id="panel-office"))
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)
    candidate = HostIdentity(_OTHER_PUBLIC_KEY, _OTHER_FINGERPRINT)
    verified = AsyncMock(return_value=candidate)
    persisted = AsyncMock()

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        expected = PanelConfig.from_subentry(entry.subentries["panel-office"])
        manager = fleet.panels["panel-office"]
        await fleet._ssh_lock.acquire()
        try:
            with (
                patch(
                    "custom_components.brilliant_mqtt.fleet_manager.async_verify_host_identity",
                    verified,
                ),
                patch(
                    "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
                    persisted,
                ),
                patch.object(manager, "_fire") as audit,
            ):
                rebind = asyncio.create_task(
                    fleet.async_rebind_panel(
                        "panel-office",
                        expected,
                        host="replacement.example.com",
                        root_password="replacement-secret",
                        candidate=candidate,
                    )
                )
                await asyncio.sleep(0)

                assert not rebind.done()
                verified.assert_not_awaited()
                persisted.assert_not_awaited()
                audit.assert_not_called()

                fleet._ssh_lock.release()
                await rebind

                verified.assert_awaited_once_with(
                    "replacement.example.com",
                    candidate,
                )
                persisted.assert_awaited_once_with(
                    hass,
                    entry,
                    subentry_id="panel-office",
                )
                audit.assert_called_once()
        finally:
            if fleet._ssh_lock.locked():
                fleet._ssh_lock.release()
        await fleet.async_shutdown()


async def test_rebind_cancelled_while_waiting_for_shared_ssh_lock_is_write_free(
    hass: HomeAssistant,
) -> None:
    """Cancellation while queued releases lifecycle ownership without side effects."""
    entry = _fleet_entry(_panel("office", "SHA256:office", subentry_id="panel-office"))
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)
    candidate = HostIdentity(_OTHER_PUBLIC_KEY, _OTHER_FINGERPRINT)
    verified = AsyncMock(return_value=candidate)
    persisted = AsyncMock()

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        expected = PanelConfig.from_subentry(entry.subentries["panel-office"])
        manager = fleet.panels["panel-office"]
        original_data = deepcopy(dict(entry.subentries["panel-office"].data))
        await fleet._ssh_lock.acquire()
        try:
            with (
                patch(
                    "custom_components.brilliant_mqtt.fleet_manager.async_verify_host_identity",
                    verified,
                ),
                patch(
                    "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
                    persisted,
                ),
                patch.object(manager, "_fire") as audit,
            ):
                rebind = asyncio.create_task(
                    fleet.async_rebind_panel(
                        "panel-office",
                        expected,
                        host="cancelled.example.com",
                        root_password="cancelled-secret",
                        candidate=candidate,
                    )
                )
                await asyncio.sleep(0)

                rebind.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await rebind

                assert not fleet._lifecycle_lock.locked()
                assert fleet._ssh_lock.locked()
                verified.assert_not_awaited()
                persisted.assert_not_awaited()
                audit.assert_not_called()
                assert entry.subentries["panel-office"].data == original_data
                assert fleet._panel_configs["panel-office"] == expected
                assert manager.store.data == original_data
        finally:
            fleet._ssh_lock.release()
        await fleet.async_shutdown()


async def test_rebind_rechecks_the_panel_snapshot_after_waiting_for_ssh(
    hass: HomeAssistant,
) -> None:
    """A queued rebind cannot commit against the pre-wait panel snapshot."""
    entry = _fleet_entry(_panel("office", "SHA256:office", subentry_id="panel-office"))
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)
    candidate = HostIdentity(_OTHER_PUBLIC_KEY, _OTHER_FINGERPRINT)
    verified = AsyncMock(return_value=candidate)
    persisted = AsyncMock()

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        assert fleet._update_unsub is not None
        fleet._update_unsub()
        fleet._update_unsub = None
        expected = PanelConfig.from_subentry(entry.subentries["panel-office"])
        manager = fleet.panels["panel-office"]
        await fleet._ssh_lock.acquire()
        try:
            with (
                patch(
                    "custom_components.brilliant_mqtt.fleet_manager.async_verify_host_identity",
                    verified,
                ),
                patch(
                    "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
                    persisted,
                ),
                patch.object(manager, "_fire") as audit,
            ):
                rebind = asyncio.create_task(
                    fleet.async_rebind_panel(
                        "panel-office",
                        expected,
                        host="replacement.example.com",
                        root_password="replacement-secret",
                        candidate=candidate,
                    )
                )
                await asyncio.sleep(0)

                subentry = entry.subentries["panel-office"]
                assert hass.config_entries.async_update_subentry(
                    entry,
                    subentry,
                    data={**subentry.data, CONF_NAME: "Renamed while queued"},
                    title="Renamed while queued",
                )
                fleet._ssh_lock.release()

                with pytest.raises(EntryDataError, match="panel_snapshot_changed"):
                    await rebind

                verified.assert_not_awaited()
                persisted.assert_not_awaited()
                audit.assert_not_called()
                assert entry.subentries["panel-office"].data[CONF_NAME] == "Renamed while queued"
                assert entry.subentries["panel-office"].data[CONF_HOST] == expected.host
                assert (
                    entry.subentries["panel-office"].data[CONF_IDENTITY_FINGERPRINT]
                    == expected.identity_fingerprint
                )
                assert fleet._panel_configs["panel-office"] == expected
                assert manager.store.data[CONF_NAME] == "Renamed while queued"
        finally:
            if fleet._ssh_lock.locked():
                fleet._ssh_lock.release()
        await fleet.async_shutdown()


async def test_rebind_rechecks_duplicate_ownership_after_waiting_for_ssh(
    hass: HomeAssistant,
) -> None:
    """A candidate claimed while queued cannot be persisted into this panel."""
    entry = _fleet_entry(_panel("office", "SHA256:office", subentry_id="panel-office"))
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)
    candidate = HostIdentity(_OTHER_PUBLIC_KEY, _OTHER_FINGERPRINT)
    verified = AsyncMock(return_value=candidate)
    persisted = AsyncMock()

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        expected = PanelConfig.from_subentry(entry.subentries["panel-office"])
        manager = fleet.panels["panel-office"]
        await fleet._ssh_lock.acquire()
        try:
            with (
                patch(
                    "custom_components.brilliant_mqtt.fleet_manager.async_verify_host_identity",
                    verified,
                ),
                patch(
                    "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
                    persisted,
                ),
                patch.object(manager, "_fire") as audit,
            ):
                rebind = asyncio.create_task(
                    fleet.async_rebind_panel(
                        "panel-office",
                        expected,
                        host="replacement.example.com",
                        root_password="replacement-secret",
                        candidate=candidate,
                    )
                )
                await asyncio.sleep(0)

                duplicate_entry = _fleet_entry(
                    _panel(
                        "kitchen",
                        "SHA256:kitchen",
                        subentry_id="panel-kitchen",
                    )
                )
                duplicate_entry.add_to_hass(hass)
                fleet._ssh_lock.release()

                with pytest.raises(
                    EntryDataError,
                    match="duplicate_panel_fingerprint",
                ):
                    await rebind

                verified.assert_not_awaited()
                persisted.assert_not_awaited()
                audit.assert_not_called()
                assert PanelConfig.from_subentry(entry.subentries["panel-office"]) == expected
                assert fleet._panel_configs["panel-office"] == expected
                assert manager.store.data[CONF_IDENTITY_FINGERPRINT] == (
                    expected.identity_fingerprint
                )
        finally:
            if fleet._ssh_lock.locked():
                fleet._ssh_lock.release()
        await fleet.async_shutdown()


async def test_rebind_rechecks_snapshot_after_late_identity_network_wait(
    hass: HomeAssistant,
) -> None:
    """A config change during the key exchange cannot bypass optimistic ownership."""
    entry = _fleet_entry(_panel("office", "SHA256:office", subentry_id="panel-office"))
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)
    candidate = HostIdentity(_OTHER_PUBLIC_KEY, _OTHER_FINGERPRINT)
    verification_started = asyncio.Event()
    allow_verification = asyncio.Event()
    persisted = AsyncMock()

    async def verify_identity(host: str, expected: HostIdentity) -> HostIdentity:
        assert host == "replacement.example.com"
        assert expected == candidate
        assert fleet._lifecycle_lock.locked()
        assert fleet._ssh_lock.locked()
        verification_started.set()
        await allow_verification.wait()
        return candidate

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        assert fleet._update_unsub is not None
        fleet._update_unsub()
        fleet._update_unsub = None
        expected = PanelConfig.from_subentry(entry.subentries["panel-office"])
        manager = fleet.panels["panel-office"]

        with (
            patch(
                "custom_components.brilliant_mqtt.fleet_manager.async_verify_host_identity",
                new=AsyncMock(side_effect=verify_identity),
            ),
            patch(
                "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
                persisted,
            ),
            patch.object(manager, "_fire") as audit,
        ):
            rebind = asyncio.create_task(
                fleet.async_rebind_panel(
                    "panel-office",
                    expected,
                    host="replacement.example.com",
                    root_password="replacement-secret",
                    candidate=candidate,
                )
            )
            await verification_started.wait()
            subentry = entry.subentries["panel-office"]
            assert hass.config_entries.async_update_subentry(
                entry,
                subentry,
                data={**subentry.data, CONF_NAME: "Changed during verification"},
                title="Changed during verification",
            )
            allow_verification.set()

            with pytest.raises(EntryDataError, match="panel_snapshot_changed"):
                await rebind

        persisted.assert_not_awaited()
        audit.assert_not_called()
        assert entry.subentries["panel-office"].data[CONF_HOST] == expected.host
        assert entry.subentries["panel-office"].data[CONF_IDENTITY_FINGERPRINT] == (
            expected.identity_fingerprint
        )
        assert fleet._panel_configs["panel-office"] == expected
        await fleet.async_shutdown()


async def test_rebind_rechecks_duplicate_after_late_identity_network_wait(
    hass: HomeAssistant,
) -> None:
    """A fingerprint claimed during key exchange cannot cross the final write gate."""
    entry = _fleet_entry(_panel("office", "SHA256:office", subentry_id="panel-office"))
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)
    candidate = HostIdentity(_OTHER_PUBLIC_KEY, _OTHER_FINGERPRINT)
    verification_started = asyncio.Event()
    allow_verification = asyncio.Event()
    persisted = AsyncMock()

    async def verify_identity(host: str, expected: HostIdentity) -> HostIdentity:
        assert host == "replacement.example.com"
        assert expected == candidate
        verification_started.set()
        await allow_verification.wait()
        return candidate

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        expected = PanelConfig.from_subentry(entry.subentries["panel-office"])
        manager = fleet.panels["panel-office"]

        with (
            patch(
                "custom_components.brilliant_mqtt.fleet_manager.async_verify_host_identity",
                new=AsyncMock(side_effect=verify_identity),
            ),
            patch(
                "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
                persisted,
            ),
            patch.object(manager, "_fire") as audit,
        ):
            rebind = asyncio.create_task(
                fleet.async_rebind_panel(
                    "panel-office",
                    expected,
                    host="replacement.example.com",
                    root_password="replacement-secret",
                    candidate=candidate,
                )
            )
            await verification_started.wait()
            duplicate_entry = _fleet_entry(
                _panel(
                    "kitchen",
                    "SHA256:kitchen",
                    subentry_id="panel-kitchen",
                )
            )
            duplicate_entry.add_to_hass(hass)
            allow_verification.set()

            with pytest.raises(
                EntryDataError,
                match="duplicate_panel_fingerprint",
            ):
                await rebind

        persisted.assert_not_awaited()
        audit.assert_not_called()
        assert PanelConfig.from_subentry(entry.subentries["panel-office"]) == expected
        assert fleet._panel_configs["panel-office"] == expected
        await fleet.async_shutdown()


async def test_rebind_rechecks_global_provisioning_journal_after_identity_wait(
    hass: HomeAssistant,
) -> None:
    """A transaction appearing during key exchange blocks the identity write."""
    entry = _fleet_entry(_panel("office", "SHA256:office", subentry_id="panel-office"))
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)
    candidate = HostIdentity(_OTHER_PUBLIC_KEY, _OTHER_FINGERPRINT)
    journal = Mock(async_load=AsyncMock(side_effect=[None, _pending_record()]))
    persisted = AsyncMock()

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        expected = PanelConfig.from_subentry(entry.subentries["panel-office"])
        manager = fleet.panels["panel-office"]
        original_data = deepcopy(dict(entry.subentries["panel-office"].data))

        with (
            patch(
                "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
                return_value=journal,
            ),
            patch(
                "custom_components.brilliant_mqtt.fleet_manager.async_verify_host_identity",
                new=AsyncMock(return_value=candidate),
            ) as verified,
            patch(
                "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
                persisted,
            ),
            patch.object(manager, "_fire") as audit,
            pytest.raises(
                EntryDataError,
                match="panel_rebind_blocked_by_panel_onboarding",
            ),
        ):
            await fleet.async_rebind_panel(
                "panel-office",
                expected,
                host="replacement.example.com",
                root_password="replacement-secret",
                candidate=candidate,
            )

        assert journal.async_load.await_count == 2
        verified.assert_awaited_once_with("replacement.example.com", candidate)
        persisted.assert_not_awaited()
        audit.assert_not_called()
        assert entry.subentries["panel-office"].data == original_data
        assert fleet._panel_configs["panel-office"] == expected
        assert manager.store.data == original_data
        await fleet.async_shutdown()


async def test_rebind_active_journal_blocks_before_identity_network(
    hass: HomeAssistant,
) -> None:
    """An existing recovery owner prevents even the unauthenticated key exchange."""
    entry = _fleet_entry(_panel("office", "SHA256:office", subentry_id="panel-office"))
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)
    candidate = HostIdentity(_OTHER_PUBLIC_KEY, _OTHER_FINGERPRINT)
    journal = Mock(async_load=AsyncMock(return_value=_pending_record()))
    verified = AsyncMock(return_value=candidate)
    persisted = AsyncMock()

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        expected = PanelConfig.from_subentry(entry.subentries["panel-office"])
        manager = fleet.panels["panel-office"]
        original_data = deepcopy(dict(entry.subentries["panel-office"].data))

        with (
            patch(
                "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
                return_value=journal,
            ),
            patch(
                "custom_components.brilliant_mqtt.fleet_manager.async_verify_host_identity",
                verified,
            ),
            patch(
                "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
                persisted,
            ),
            patch.object(manager, "_fire") as audit,
            pytest.raises(
                EntryDataError,
                match="panel_rebind_blocked_by_panel_onboarding",
            ),
        ):
            await fleet.async_rebind_panel(
                "panel-office",
                expected,
                host="replacement.example.com",
                root_password="replacement-secret",
                candidate=candidate,
            )

        journal.async_load.assert_awaited_once_with()
        verified.assert_not_awaited()
        persisted.assert_not_awaited()
        audit.assert_not_called()
        assert entry.subentries["panel-office"].data == original_data
        assert fleet._panel_configs["panel-office"] == expected
        assert manager.store.data == original_data
        await fleet.async_shutdown()


async def test_rebind_unreadable_journal_blocks_before_identity_network(
    hass: HomeAssistant,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Unreadable recovery ownership has a distinct secret-safe failure."""
    entry = _fleet_entry(_panel("office", "SHA256:office", subentry_id="panel-office"))
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)
    candidate = HostIdentity(_OTHER_PUBLIC_KEY, _OTHER_FINGERPRINT)
    private_failure = "journal-backend-private-detail"
    journal = Mock(async_load=AsyncMock(side_effect=OSError(private_failure)))
    verified = AsyncMock(return_value=candidate)
    persisted = AsyncMock()

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        expected = PanelConfig.from_subentry(entry.subentries["panel-office"])
        manager = fleet.panels["panel-office"]
        original_data = deepcopy(dict(entry.subentries["panel-office"].data))

        with (
            patch(
                "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
                return_value=journal,
            ),
            patch(
                "custom_components.brilliant_mqtt.fleet_manager.async_verify_host_identity",
                verified,
            ),
            patch(
                "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
                persisted,
            ),
            patch.object(manager, "_fire") as audit,
            pytest.raises(
                EntryDataError,
                match="panel_rebind_blocked_by_panel_onboarding",
            ) as captured,
        ):
            await fleet.async_rebind_panel(
                "panel-office",
                expected,
                host="replacement.example.com",
                root_password="replacement-secret",
                candidate=candidate,
            )

        journal.async_load.assert_awaited_once_with()
        verified.assert_not_awaited()
        persisted.assert_not_awaited()
        audit.assert_not_called()
        assert entry.subentries["panel-office"].data == original_data
        assert fleet._panel_configs["panel-office"] == expected
        assert manager.store.data == original_data
        assert private_failure not in f"{captured.value!r}\n{caplog.text}"
        await fleet.async_shutdown()


@pytest.mark.parametrize(
    ("identity_failure", "error_code"),
    [
        (
            PanelIdentityError("host_key_changed"),
            "panel_rebind_identity_changed",
        ),
        (
            PanelIdentityError("host_unreachable"),
            "panel_rebind_identity_unreachable",
        ),
        (
            OSError("transport carried replacement-root-secret"),
            "panel_rebind_identity_unreachable",
        ),
    ],
)
async def test_rebind_identity_recheck_fails_closed_before_mutation(
    hass: HomeAssistant,
    identity_failure: BaseException,
    error_code: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Late identity drift or loss cannot cross the durable ownership boundary."""
    entry = _fleet_entry(_panel("office", "SHA256:office", subentry_id="panel-office"))
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)
    persisted = AsyncMock()
    replacement_password = "replacement-root-secret"
    candidate = HostIdentity(_OTHER_PUBLIC_KEY, _OTHER_FINGERPRINT)

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        expected = PanelConfig.from_subentry(entry.subentries["panel-office"])
        manager = fleet.panels["panel-office"]
        original_data = deepcopy(dict(entry.subentries["panel-office"].data))
        original_unique_id = entry.subentries["panel-office"].unique_id

        with (
            patch(
                "custom_components.brilliant_mqtt.fleet_manager.async_verify_host_identity",
                new=AsyncMock(side_effect=identity_failure),
            ) as verified,
            patch(
                "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
                persisted,
            ),
            patch.object(manager, "_fire") as audit,
            pytest.raises(EntryDataError, match=error_code) as captured,
        ):
            await fleet.async_rebind_panel(
                "panel-office",
                expected,
                host="replacement.example.com",
                root_password=replacement_password,
                candidate=candidate,
            )

        verified.assert_awaited_once_with("replacement.example.com", candidate)
        persisted.assert_not_awaited()
        audit.assert_not_called()
        stored = entry.subentries["panel-office"]
        assert stored.data == original_data
        assert stored.unique_id == original_unique_id
        assert fleet._panel_configs["panel-office"] == expected
        assert manager.store.data == original_data
        redacted = f"{captured.value!r}\n{caplog.text}"
        assert replacement_password not in redacted
        assert candidate.public_key not in redacted
        assert str(identity_failure) not in redacted
        await fleet.async_shutdown()


async def test_rebind_rejects_fingerprint_owned_by_another_panel(
    hass: HomeAssistant,
) -> None:
    """A rebind cannot merge two physical identities into one fleet panel."""
    entry = _fleet_entry(
        _panel("office", "SHA256:office", subentry_id="panel-office"),
        _panel(
            "kitchen",
            "SHA256:kitchen",
            subentry_id="panel-kitchen",
            mesh_priority=2,
        ),
    )
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)
    persisted = AsyncMock()

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        expected = PanelConfig.from_subentry(entry.subentries["panel-office"])
        office_manager = fleet.panels["panel-office"]
        with (
            patch(
                "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
                persisted,
            ),
            patch.object(office_manager, "_fire") as audit,
            pytest.raises(EntryDataError, match="duplicate_panel_fingerprint"),
        ):
            await fleet.async_rebind_panel(
                "panel-office",
                expected,
                host="kitchen-replacement.example.com",
                root_password="duplicate-secret",
                candidate=HostIdentity(_OTHER_PUBLIC_KEY, _OTHER_FINGERPRINT),
            )

        assert PanelConfig.from_subentry(entry.subentries["panel-office"]) == expected
        assert fleet._panel_configs["panel-office"] == expected
        persisted.assert_not_awaited()
        audit.assert_not_called()
        await fleet.async_shutdown()


async def test_rebind_storage_failure_durably_rolls_back_without_audit(
    hass: HomeAssistant,
) -> None:
    """An unproven write restores the exact old snapshot and reports a fixed error."""
    entry = _fleet_entry(_panel("office", "SHA256:office", subentry_id="panel-office"))
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)
    storage_secret = "storage-error-with-private-context"
    new_password = "uncommitted-new-root-secret"
    proof_calls = 0

    async def prove_persisted(
        hass_arg: HomeAssistant,
        entry_arg: MockConfigEntry,
        *,
        subentry_id: str | None = None,
    ) -> None:
        del hass_arg, entry_arg
        nonlocal proof_calls
        assert subentry_id == "panel-office"
        proof_calls += 1
        if proof_calls == 1:
            raise OSError(storage_secret)
        assert PanelConfig.from_subentry(entry.subentries["panel-office"]) == expected

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        expected = PanelConfig.from_subentry(entry.subentries["panel-office"])
        manager = fleet.panels["panel-office"]
        candidate = HostIdentity(_OTHER_PUBLIC_KEY, _OTHER_FINGERPRINT)
        with (
            patch(
                "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
                new=AsyncMock(side_effect=prove_persisted),
            ),
            patch.object(manager, "_fire") as audit,
            pytest.raises(ConfigEntryPersistenceError) as captured,
        ):
            await fleet.async_rebind_panel(
                "panel-office",
                expected,
                host="uncommitted.example.com",
                root_password=new_password,
                candidate=candidate,
            )
        await hass.async_block_till_done()

        assert proof_calls == 2
        assert PanelConfig.from_subentry(entry.subentries["panel-office"]) == expected
        assert fleet._panel_configs["panel-office"] == expected
        assert manager.store.data[CONF_IDENTITY_FINGERPRINT] == expected.identity_fingerprint
        audit.assert_not_called()
        assert str(captured.value) == "config_entry_storage_unavailable"
        serialized_error = repr(captured.value)
        assert storage_secret not in serialized_error
        assert new_password not in serialized_error
        assert candidate.public_key not in serialized_error
        await fleet.async_shutdown()


async def test_cancelled_rebind_settles_durable_rollback_before_reraising(
    hass: HomeAssistant,
) -> None:
    """Cancellation cannot expose an unproven identity or emit a success audit."""
    entry = _fleet_entry(_panel("office", "SHA256:office", subentry_id="panel-office"))
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)
    first_proof_started = asyncio.Event()
    rollback_proof_started = asyncio.Event()
    allow_rollback_proof = asyncio.Event()
    proof_calls = 0

    async def prove_persisted(
        hass_arg: HomeAssistant,
        entry_arg: MockConfigEntry,
        *,
        subentry_id: str | None = None,
    ) -> None:
        del hass_arg, entry_arg
        nonlocal proof_calls
        assert subentry_id == "panel-office"
        proof_calls += 1
        if proof_calls == 1:
            first_proof_started.set()
            await asyncio.Future()
        rollback_proof_started.set()
        await allow_rollback_proof.wait()

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        expected = PanelConfig.from_subentry(entry.subentries["panel-office"])
        manager = fleet.panels["panel-office"]
        candidate = HostIdentity(_OTHER_PUBLIC_KEY, _OTHER_FINGERPRINT)
        with (
            patch(
                "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
                new=AsyncMock(side_effect=prove_persisted),
            ),
            patch.object(manager, "_fire") as audit,
        ):
            rebind = asyncio.create_task(
                fleet.async_rebind_panel(
                    "panel-office",
                    expected,
                    host="cancelled.example.com",
                    root_password="cancelled-root-secret",
                    candidate=candidate,
                )
            )
            await first_proof_started.wait()
            rebind.cancel()
            await rollback_proof_started.wait()

            assert not rebind.done()
            assert PanelConfig.from_subentry(entry.subentries["panel-office"]) == expected
            assert fleet._panel_configs["panel-office"] == expected
            assert manager.store.data[CONF_IDENTITY_FINGERPRINT] == expected.identity_fingerprint
            audit.assert_not_called()

            allow_rollback_proof.set()
            with pytest.raises(asyncio.CancelledError):
                await rebind
        await hass.async_block_till_done()

        assert proof_calls == 2
        assert PanelConfig.from_subentry(entry.subentries["panel-office"]) == expected
        audit.assert_not_called()
        await fleet.async_shutdown()


async def test_rebind_double_storage_failure_keeps_one_redacted_repair_until_proven(
    hass: HomeAssistant,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Unknown rollback durability stays visible until an exact later disk proof."""
    entry = _fleet_entry(_panel("office", "SHA256:office", subentry_id="panel-office"))
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)
    candidate = HostIdentity(_OTHER_PUBLIC_KEY, _OTHER_FINGERPRINT)
    replacement_password = "unproven-replacement-root-secret"
    storage_secret = "storage-backend-private-detail"
    proof = AsyncMock(side_effect=OSError(storage_secret))

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        expected = PanelConfig.from_subentry(entry.subentries["panel-office"])
        manager = fleet.panels["panel-office"]
        original_data = deepcopy(dict(entry.subentries["panel-office"].data))
        original_title = entry.subentries["panel-office"].title
        original_unique_id = entry.subentries["panel-office"].unique_id

        with (
            patch(
                "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
                proof,
            ),
            patch.object(manager, "_fire") as audit,
            pytest.raises(ConfigEntryPersistenceError) as captured,
        ):
            await fleet.async_rebind_panel(
                "panel-office",
                expected,
                host="replacement.example.com",
                root_password=replacement_password,
                candidate=candidate,
            )

        assert proof.await_count == 2
        audit.assert_not_called()
        issue = ir.async_get(hass).async_get_issue(
            DOMAIN,
            f"fleet_storage_{entry.entry_id}",
        )
        assert issue is not None
        assert issue.translation_key == "needs_attention"
        assert issue.translation_placeholders == {
            "panel": "Brilliant MQTT fleet",
            "reason": (
                "Home Assistant could not prove whether the previous panel identity was "
                "restored in durable storage. Do not operate or retry either panel until "
                "storage is healthy and the saved panel identity has been inspected or "
                "reloaded. If the previous identity is restored, run Replace physical "
                "panel again."
            ),
        }
        serialized = f"{captured.value!r}\n{issue!r}\n{caplog.text}"
        assert storage_secret not in serialized
        assert replacement_password not in serialized
        assert candidate.public_key not in serialized
        assert expected.root_password not in serialized
        assert expected.ssh_host_key not in serialized

        stored = entry.subentries["panel-office"]
        assert stored.data == original_data
        assert stored.title == original_title
        assert stored.unique_id == original_unique_id
        assert fleet._panel_configs["panel-office"] == expected
        assert manager.store.data == original_data

        with patch(
            "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
            new=AsyncMock(),
        ) as later_proof:
            await fleet._async_restore_rebind_snapshot(
                "panel-office",
                data=original_data,
                title=original_title,
                unique_id=original_unique_id,
            )

        later_proof.assert_awaited_once_with(
            hass,
            entry,
            subentry_id="panel-office",
        )
        assert (
            ir.async_get(hass).async_get_issue(
                DOMAIN,
                f"fleet_storage_{entry.entry_id}",
            )
            is None
        )
        audit.assert_not_called()
        await fleet.async_shutdown()


async def test_rebind_is_unavailable_for_legacy_runtime(
    hass: HomeAssistant,
) -> None:
    """Compatibility managers cannot mutate identity through the fleet-only API."""
    entry = _legacy_entry()
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)
    expected = PanelConfig.from_subentry(
        _panel("office", "SHA256:office", subentry_id="panel-office")
    )
    persisted = AsyncMock()

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        with (
            patch(
                "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
                persisted,
            ),
            pytest.raises(EntryDataError, match="panel_rebind_unavailable"),
        ):
            await fleet.async_rebind_panel(
                entry.entry_id,
                expected,
                host="replacement.example.com",
                root_password="replacement-secret",
                candidate=HostIdentity(_OTHER_PUBLIC_KEY, _OTHER_FINGERPRINT),
            )

        persisted.assert_not_awaited()
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
    """Only the fixed bootstrap sentinel is valid while no panel owns scenes."""
    fleet = FleetManager(hass, _empty_fleet_entry())

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        assert fleet.panels == {}
        await fleet.async_shutdown()


@pytest.mark.parametrize(
    "scene_owner",
    ("panel-office", "pending", ""),
)
async def test_empty_fleet_rejects_every_non_reserved_scene_owner(
    hass: HomeAssistant,
    scene_owner: str,
) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id=FLEET_UNIQUE_ID,
        version=CONFIG_ENTRY_VERSION,
        data={
            **_fleet_data(),
            CONF_NEXT_MESH_PRIORITY: 1,
            CONF_SCENE_PANEL: scene_owner,
        },
    )

    with pytest.raises(EntryDataError, match="invalid_(scene_panel|entry_data)"):
        await FleetManager(hass, entry).async_setup()


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


async def test_successful_fleet_control_change_reloads_after_lifecycle_unlock(
    hass: HomeAssistant,
) -> None:
    """A validated control snapshot reaches the existing plane without lock inversion."""
    entry = _fleet_entry(_panel("office", "SHA256:office", subentry_id="panel-office"))
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)

    async def reload_settings() -> None:
        assert not fleet._lifecycle_lock.locked()

    control_plane = Mock(
        async_reload_settings=AsyncMock(side_effect=reload_settings),
    )
    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        assert fleet._update_unsub is not None
        fleet._update_unsub()
        fleet._update_unsub = None
        hass.config_entries.async_update_entry(
            entry,
            data={
                **entry.data,
                CONF_HA_CONTROL_LABEL: "new-label",
                CONF_SCENE_ACTIONS: {"movie": {"service": "scene.turn_on"}},
            },
        )

        with patch(
            "custom_components.brilliant_mqtt.ha_control.get_control_plane",
            return_value=control_plane,
        ) as get_control_plane:
            await fleet._async_reconcile()

        assert fleet.fleet.ha_control_label == "new-label"
        assert fleet.fleet.scene_actions == {"movie": {"service": "scene.turn_on"}}
        get_control_plane.assert_called_once_with(hass)
        control_plane.async_reload_settings.assert_awaited_once_with()
        await fleet.async_shutdown()


async def test_failed_control_reload_stays_pending_for_identical_reconcile_retry(
    hass: HomeAssistant,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A committed baseline cannot consume a failed control-plane publication."""
    entry = _fleet_entry(_panel("office", "SHA256:office", subentry_id="panel-office"))
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)
    private_failure = "control-plane-private-runtime-detail"
    attempts = 0

    async def reload_settings() -> None:
        nonlocal attempts
        assert not fleet._lifecycle_lock.locked()
        attempts += 1
        if attempts == 1:
            raise RuntimeError(private_failure)

    control_plane = Mock(
        async_reload_settings=AsyncMock(side_effect=reload_settings),
    )
    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        assert fleet._update_unsub is not None
        fleet._update_unsub()
        fleet._update_unsub = None
        hass.config_entries.async_update_entry(
            entry,
            data={**entry.data, CONF_HA_CONTROL_LABEL: "retry-label"},
        )

        with (
            patch(
                "custom_components.brilliant_mqtt.ha_control.get_control_plane",
                return_value=control_plane,
            ),
            patch.object(hass.config_entries, "async_schedule_reload") as schedule_reload,
        ):
            with pytest.raises(
                ConfigEntryNotReady,
                match="Control-plane settings reload is temporarily unavailable",
            ) as captured:
                await fleet._async_reconcile()

            assert fleet.fleet.ha_control_label == "retry-label"
            assert fleet._pending_control_plane_reload is not None
            schedule_reload.assert_called_once_with(entry.entry_id)

            await fleet._async_reconcile()

        assert attempts == 2
        assert fleet._pending_control_plane_reload is None
        serialized = f"{captured.value!r}\n{caplog.text}"
        assert private_failure not in serialized
        await fleet.async_shutdown()


async def test_cancelled_control_reload_keeps_pending_and_schedules_recovery(
    hass: HomeAssistant,
) -> None:
    """Cancellation after baseline commit retains retry ownership and propagates."""
    entry = _fleet_entry(_panel("office", "SHA256:office", subentry_id="panel-office"))
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)
    first_reload_started = asyncio.Event()
    attempts = 0

    async def reload_settings() -> None:
        nonlocal attempts
        assert not fleet._lifecycle_lock.locked()
        attempts += 1
        if attempts == 1:
            first_reload_started.set()
            await asyncio.Future()

    control_plane = Mock(
        async_reload_settings=AsyncMock(side_effect=reload_settings),
    )
    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        assert fleet._update_unsub is not None
        fleet._update_unsub()
        fleet._update_unsub = None
        hass.config_entries.async_update_entry(
            entry,
            data={**entry.data, CONF_HA_CONTROL_LABEL: "cancelled-reload-label"},
        )

        with (
            patch(
                "custom_components.brilliant_mqtt.ha_control.get_control_plane",
                return_value=control_plane,
            ),
            patch.object(hass.config_entries, "async_schedule_reload") as schedule_reload,
        ):
            reconcile = asyncio.create_task(fleet._async_reconcile())
            await first_reload_started.wait()
            assert fleet.fleet.ha_control_label == "cancelled-reload-label"
            assert fleet._pending_control_plane_reload is not None

            reconcile.cancel()
            with pytest.raises(asyncio.CancelledError):
                await reconcile

            assert fleet._pending_control_plane_reload is not None
            schedule_reload.assert_called_once_with(entry.entry_id)

            await fleet._async_reconcile()

        assert attempts == 2
        assert fleet._pending_control_plane_reload is None
        await fleet.async_shutdown()


async def test_cancelled_control_reload_clear_schedules_pending_recovery(
    hass: HomeAssistant,
) -> None:
    """Cancellation while reacquiring lifecycle cannot orphan a successful attempt."""
    entry = _fleet_entry(_panel("office", "SHA256:office", subentry_id="panel-office"))
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)
    reload_started = asyncio.Event()
    allow_reload_return = asyncio.Event()
    reload_returning = asyncio.Event()

    async def reload_settings() -> None:
        assert not fleet._lifecycle_lock.locked()
        reload_started.set()
        await allow_reload_return.wait()
        reload_returning.set()

    control_plane = Mock(
        async_reload_settings=AsyncMock(side_effect=reload_settings),
    )
    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        assert fleet._update_unsub is not None
        fleet._update_unsub()
        fleet._update_unsub = None
        hass.config_entries.async_update_entry(
            entry,
            data={**entry.data, CONF_HA_CONTROL_LABEL: "cancelled-clear-label"},
        )

        with (
            patch(
                "custom_components.brilliant_mqtt.ha_control.get_control_plane",
                return_value=control_plane,
            ),
            patch.object(hass.config_entries, "async_schedule_reload") as schedule_reload,
        ):
            reconcile = asyncio.create_task(fleet._async_reconcile())
            await reload_started.wait()
            await fleet._lifecycle_lock.acquire()
            try:
                allow_reload_return.set()
                await reload_returning.wait()
                await asyncio.sleep(0)

                reconcile.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await reconcile

                assert fleet._pending_control_plane_reload is not None
                schedule_reload.assert_called_once_with(entry.entry_id)
            finally:
                fleet._lifecycle_lock.release()

            await fleet._async_reconcile()

        assert fleet._pending_control_plane_reload is None
        await fleet.async_shutdown()


async def test_cancellation_during_post_commit_cleanup_schedules_control_recovery(
    hass: HomeAssistant,
) -> None:
    """Any cancellation after the new baseline exists retains publication ownership."""
    entry = _fleet_entry(
        _panel("office", "SHA256:office", subentry_id="panel-office"),
        _panel("kitchen", "SHA256:kitchen", subentry_id="panel-kitchen"),
    )
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)
    kitchen_shutdown_started = asyncio.Event()
    allow_kitchen_shutdown = asyncio.Event()

    async def shutdown(manager: PanelManager) -> None:
        if manager.panel == "kitchen":
            kitchen_shutdown_started.set()
            await allow_kitchen_shutdown.wait()

    control_plane = Mock(async_reload_settings=AsyncMock())
    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", shutdown),
    ):
        await fleet.async_setup()
        assert fleet._update_unsub is not None
        fleet._update_unsub()
        fleet._update_unsub = None
        assert hass.config_entries.async_remove_subentry(entry, "panel-kitchen")
        hass.config_entries.async_update_entry(
            entry,
            data={**entry.data, CONF_HA_CONTROL_LABEL: "cleanup-cancel-label"},
        )

        with (
            patch(
                "custom_components.brilliant_mqtt.ha_control.get_control_plane",
                return_value=control_plane,
            ),
            patch.object(hass.config_entries, "async_schedule_reload") as schedule_reload,
        ):
            reconcile = asyncio.create_task(fleet._async_reconcile())
            await kitchen_shutdown_started.wait()
            assert fleet.fleet.ha_control_label == "cleanup-cancel-label"
            assert fleet._pending_control_plane_reload is not None

            reconcile.cancel()
            await asyncio.sleep(0)
            assert not reconcile.done()
            allow_kitchen_shutdown.set()
            with pytest.raises(asyncio.CancelledError):
                await reconcile

            schedule_reload.assert_called_once_with(entry.entry_id)
            control_plane.async_reload_settings.assert_not_awaited()
            assert fleet._pending_control_plane_reload is not None

            await fleet._async_reconcile()

        control_plane.async_reload_settings.assert_awaited_once_with()
        assert fleet._pending_control_plane_reload is None
        await fleet.async_shutdown()


async def test_failed_post_commit_cleanup_schedules_identical_control_retry(
    hass: HomeAssistant,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A generic cleanup failure cannot orphan committed publication ownership."""
    entry = _fleet_entry(
        _panel("office", "SHA256:office", subentry_id="panel-office"),
        _panel("kitchen", "SHA256:kitchen", subentry_id="panel-kitchen"),
    )
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)
    private_failure = "removed-runtime-private-shutdown-detail"
    kitchen_shutdown_attempts = 0

    async def shutdown(manager: PanelManager) -> None:
        nonlocal kitchen_shutdown_attempts
        if manager.panel != "kitchen":
            return
        kitchen_shutdown_attempts += 1
        raise RuntimeError(private_failure)

    control_plane = Mock(async_reload_settings=AsyncMock())
    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", shutdown),
    ):
        await fleet.async_setup()
        kitchen_manager = fleet.panels["panel-kitchen"]
        kitchen_issue_ids = _panel_owned_issue_ids(kitchen_manager.management_id)
        office_issue_id = f"needs_attention_{fleet.panels['panel-office'].management_id}"
        registry = ir.async_get(hass)
        for issue_id in kitchen_issue_ids | {office_issue_id}:
            ir.async_create_issue(
                hass,
                DOMAIN,
                issue_id,
                is_fixable=False,
                severity=ir.IssueSeverity.WARNING,
                translation_key="needs_attention",
                translation_placeholders={
                    "panel": "test",
                    "reason": "test",
                },
            )
        assert fleet._update_unsub is not None
        fleet._update_unsub()
        fleet._update_unsub = None
        assert hass.config_entries.async_remove_subentry(entry, "panel-kitchen")
        hass.config_entries.async_update_entry(
            entry,
            data={**entry.data, CONF_HA_CONTROL_LABEL: "cleanup-retry-label"},
        )

        with (
            patch(
                "custom_components.brilliant_mqtt.ha_control.get_control_plane",
                return_value=control_plane,
            ),
            patch.object(hass.config_entries, "async_schedule_reload") as schedule_reload,
        ):
            with pytest.raises(
                ConfigEntryNotReady,
                match="Fleet post-update cleanup is temporarily unavailable",
            ) as captured:
                await fleet._async_reconcile()

            assert fleet.fleet.ha_control_label == "cleanup-retry-label"
            assert set(fleet.panels) == {"panel-office"}
            assert fleet._pending_control_plane_reload is not None
            assert kitchen_shutdown_attempts == 1
            schedule_reload.assert_called_once_with(entry.entry_id)
            control_plane.async_reload_settings.assert_not_awaited()
            assert all(
                registry.async_get_issue(DOMAIN, issue_id) is None for issue_id in kitchen_issue_ids
            )
            assert registry.async_get_issue(DOMAIN, office_issue_id) is not None

            await fleet._async_reconcile()

        control_plane.async_reload_settings.assert_awaited_once_with()
        assert fleet._pending_control_plane_reload is None
        assert captured.value.__cause__ is None
        assert captured.value.__suppress_context__
        assert private_failure not in f"{captured.value!r}\n{caplog.text}"
        await fleet.async_shutdown()


async def test_older_control_reload_success_cannot_clear_newer_snapshot(
    hass: HomeAssistant,
) -> None:
    """Generation ownership prevents an older completion from consuming newer work."""
    entry = _fleet_entry(_panel("office", "SHA256:office", subentry_id="panel-office"))
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)
    first_reload_started = asyncio.Event()
    second_reload_started = asyncio.Event()
    finish_first_reload = asyncio.Event()
    finish_second_reload = asyncio.Event()
    attempts = 0

    async def reload_settings() -> None:
        nonlocal attempts
        assert not fleet._lifecycle_lock.locked()
        attempts += 1
        if attempts == 1:
            first_reload_started.set()
            await finish_first_reload.wait()
            return
        if attempts == 2:
            second_reload_started.set()
            await finish_second_reload.wait()
            return
        raise AssertionError("unexpected control-plane reload")

    control_plane = Mock(
        async_reload_settings=AsyncMock(side_effect=reload_settings),
    )
    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        assert fleet._update_unsub is not None
        fleet._update_unsub()
        fleet._update_unsub = None

        with patch(
            "custom_components.brilliant_mqtt.ha_control.get_control_plane",
            return_value=control_plane,
        ):
            hass.config_entries.async_update_entry(
                entry,
                data={**entry.data, CONF_HA_CONTROL_LABEL: "generation-one"},
            )
            first = asyncio.create_task(fleet._async_reconcile())
            await first_reload_started.wait()
            first_pending = fleet._pending_control_plane_reload
            assert first_pending is not None

            hass.config_entries.async_update_entry(
                entry,
                data={**entry.data, CONF_HA_CONTROL_LABEL: "generation-two"},
            )
            second = asyncio.create_task(fleet._async_reconcile())
            await second_reload_started.wait()
            second_pending = fleet._pending_control_plane_reload
            assert second_pending is not None
            assert second_pending != first_pending
            assert fleet.fleet.ha_control_label == "generation-two"

            finish_first_reload.set()
            await first
            assert fleet._pending_control_plane_reload == second_pending

            finish_second_reload.set()
            await second

        assert attempts == 2
        assert fleet._pending_control_plane_reload is None
        await fleet.async_shutdown()


async def test_successful_fleet_topology_change_reloads_control_plane(
    hass: HomeAssistant,
) -> None:
    """Scene routing is rebuilt when a validated fleet gains a panel slug."""
    entry = _fleet_entry(_panel("office", "SHA256:office", subentry_id="panel-office"))
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)
    control_plane = Mock(async_reload_settings=AsyncMock())

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        assert fleet._update_unsub is not None
        fleet._update_unsub()
        fleet._update_unsub = None
        assert hass.config_entries.async_add_subentry(
            entry,
            _panel("kitchen", "SHA256:kitchen", subentry_id="panel-kitchen"),
        )

        with patch(
            "custom_components.brilliant_mqtt.ha_control.get_control_plane",
            return_value=control_plane,
        ):
            await fleet.async_panel_added("panel-kitchen")

        control_plane.async_reload_settings.assert_awaited_once_with()
        await fleet.async_shutdown()


async def test_rejected_fleet_snapshot_does_not_reload_control_plane(
    hass: HomeAssistant,
) -> None:
    """An immutable-identity violation cannot publish unrelated control changes."""
    entry = _fleet_entry(_panel("office", "SHA256:office", subentry_id="panel-office"))
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)
    control_plane = Mock(async_reload_settings=AsyncMock())

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        assert fleet._update_unsub is not None
        fleet._update_unsub()
        fleet._update_unsub = None
        hass.config_entries.async_update_entry(
            entry,
            data={**entry.data, CONF_HA_CONTROL_LABEL: "rejected-label"},
        )
        stored_office = entry.subentries["panel-office"]
        assert hass.config_entries.async_update_subentry(
            entry,
            stored_office,
            data={**stored_office.data, CONF_PANEL: "rebound-office"},
        )

        with (
            patch(
                "custom_components.brilliant_mqtt.ha_control.get_control_plane",
                return_value=control_plane,
            ) as get_control_plane,
            pytest.raises(EntryDataError, match="immutable_panel_identity"),
        ):
            await fleet._async_reconcile()

        assert fleet.fleet.ha_control_label == "brilliant"
        get_control_plane.assert_not_called()
        control_plane.async_reload_settings.assert_not_awaited()
        await fleet.async_shutdown()


async def test_update_listener_schedules_reload_when_live_reconcile_rejects(
    hass: HomeAssistant,
) -> None:
    """An identity re-pin (host + SSH host key) applies via a scheduled reload.

    The config flow no longer reloads on completion (HA 2026.12 forbids a
    reloading flow method next to an update listener), so a snapshot the live
    reconcile refuses must not be dropped — the listener schedules the reload.
    """
    entry = _fleet_entry(_panel("office", "SHA256:office", subentry_id="panel-office"))
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
        patch.object(hass.config_entries, "async_schedule_reload") as schedule_reload,
    ):
        await fleet.async_setup()
        assert fleet._update_unsub is not None
        stored_office = entry.subentries["panel-office"]
        assert hass.config_entries.async_update_subentry(
            entry,
            stored_office,
            data={
                **stored_office.data,
                CONF_HOST: "office-moved.example.com",
                CONF_SSH_HOST_KEY: _OTHER_PUBLIC_KEY,
                CONF_IDENTITY_FINGERPRINT: _OTHER_FINGERPRINT,
            },
            unique_id=_OTHER_FINGERPRINT,
        )
        await hass.async_block_till_done()

        schedule_reload.assert_called_once_with(entry.entry_id)
        await fleet.async_shutdown()


async def test_panel_host_only_change_does_not_reload_control_plane(
    hass: HomeAssistant,
) -> None:
    """Panel connection changes stay outside HA-control and scene authority."""
    entry = _fleet_entry(_panel("office", "SHA256:office", subentry_id="panel-office"))
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)
    control_plane = Mock(async_reload_settings=AsyncMock())

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        assert fleet._update_unsub is not None
        fleet._update_unsub()
        fleet._update_unsub = None
        stored_office = entry.subentries["panel-office"]
        assert hass.config_entries.async_update_subentry(
            entry,
            stored_office,
            data={**stored_office.data, CONF_HOST: "office-new.example.com"},
        )

        with patch(
            "custom_components.brilliant_mqtt.ha_control.get_control_plane",
            return_value=control_plane,
        ) as get_control_plane:
            await fleet.async_panel_updated("panel-office")

        assert fleet._panel_configs["panel-office"].host == "office-new.example.com"
        get_control_plane.assert_not_called()
        control_plane.async_reload_settings.assert_not_awaited()
        await fleet.async_shutdown()


async def test_legacy_reconcile_preserves_existing_control_plane_behavior(
    hass: HomeAssistant,
) -> None:
    """Compatibility entries keep their full-entry reload path unchanged."""
    entry = _legacy_entry()
    entry.add_to_hass(hass)
    fleet = FleetManager(hass, entry)
    control_plane = Mock(async_reload_settings=AsyncMock())

    with (
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
    ):
        await fleet.async_setup()
        assert fleet._update_unsub is not None
        fleet._update_unsub()
        fleet._update_unsub = None
        hass.config_entries.async_update_entry(
            entry,
            data={**entry.data, CONF_HA_CONTROL_LABEL: "legacy-new-label"},
        )

        with patch(
            "custom_components.brilliant_mqtt.ha_control.get_control_plane",
            return_value=control_plane,
        ) as get_control_plane:
            await fleet._async_reconcile()

        assert fleet.fleet.ha_control_label == "legacy-new-label"
        assert fleet._pending_control_plane_reload is None
        get_control_plane.assert_not_called()
        control_plane.async_reload_settings.assert_not_awaited()
        await fleet.async_shutdown()


async def test_failed_pending_panel_start_is_not_published_and_schedules_retry(
    hass: HomeAssistant,
) -> None:
    """Eager listeners coalesce a failed live handoff into one reload retry."""
    entry = _provisioning_entry(
        _panel(
            "kitchen",
            "SHA256:kitchen",
            subentry_id="panel-kitchen",
            mesh_priority=1,
        ),
        scene_owner="panel-kitchen",
        next_mesh_priority=2,
    )
    entry.add_to_hass(hass)
    current_record: ProvisioningRecord | None = None

    async def load_record() -> ProvisioningRecord | None:
        return current_record

    journal = Mock(
        async_load=AsyncMock(side_effect=load_record),
        async_complete_commit=AsyncMock(),
    )
    starts: list[str] = []
    stops: list[str] = []

    async def setup(manager: PanelManager) -> None:
        starts.append(manager.panel)
        if manager.panel == "office":
            raise OSError("new panel unavailable")

    async def shutdown(manager: PanelManager) -> None:
        stops.append(manager.panel)

    fleet = FleetManager(hass, entry)
    persisted = AsyncMock()
    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
            persisted,
        ),
        patch.object(PanelManager, "async_setup", setup),
        patch.object(PanelManager, "async_shutdown", shutdown),
        patch.object(hass.config_entries, "async_schedule_reload") as schedule_reload,
    ):
        await fleet.async_setup()
        assert fleet._update_unsub is not None

        current_record = _pending_record()
        candidate = _panel(
            "office",
            "SHA256:office",
            subentry_id="panel-office",
            management_id=_OFFICE_FINGERPRINT,
            mesh_priority=2,
            transaction_id=_TRANSACTION_ID,
        )
        assert hass.config_entries.async_add_subentry(entry, candidate)
        await hass.async_block_till_done()

        schedule_reload.assert_called_once_with(entry.entry_id)
        assert set(fleet.panels) == {"panel-kitchen"}
        assert starts.count("office") >= 2
        assert stops
        assert set(stops) == {"office"}
        assert entry.subentries["panel-office"].data[CONF_PROVISIONING_TRANSACTION_ID] == str(
            _TRANSACTION_ID
        )
        journal.async_complete_commit.assert_not_awaited()
        assert persisted.await_count >= 1

        await fleet.async_shutdown()


async def test_successful_live_handoff_advances_priority_and_reloads_parent_once(
    hass: HomeAssistant,
) -> None:
    """Entities materialize only after the exact journal commit has succeeded."""
    entry = _provisioning_entry(
        _panel(
            "kitchen",
            "SHA256:kitchen",
            subentry_id="panel-kitchen",
            mesh_priority=1,
        ),
        scene_owner="panel-kitchen",
        next_mesh_priority=2,
    )
    entry.add_to_hass(hass)
    current_record: ProvisioningRecord | None = None

    async def load_record() -> ProvisioningRecord | None:
        return current_record

    async def complete_commit(
        transaction_id: UUID,
        *,
        subentry_id: str,
    ) -> None:
        nonlocal current_record
        assert transaction_id == _TRANSACTION_ID
        assert subentry_id == "panel-office"
        current_record = None

    journal = Mock(
        async_load=AsyncMock(side_effect=load_record),
        async_complete_commit=AsyncMock(side_effect=complete_commit),
    )
    fleet = FleetManager(hass, entry)
    persisted = AsyncMock()

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
            persisted,
        ),
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
        patch.object(hass.config_entries, "async_schedule_reload") as schedule_reload,
    ):
        await fleet.async_setup()
        assert fleet._update_unsub is not None
        fleet._update_unsub()
        fleet._update_unsub = None

        current_record = _pending_record()
        assert hass.config_entries.async_add_subentry(
            entry,
            _panel(
                "office",
                "SHA256:office",
                subentry_id="panel-office",
                management_id=_OFFICE_FINGERPRINT,
                mesh_priority=2,
                transaction_id=_TRANSACTION_ID,
            ),
        )
        await fleet.async_panel_added("panel-office")

        schedule_reload.assert_called_once_with(entry.entry_id)
        assert set(fleet.panels) == {"panel-kitchen", "panel-office"}
        assert entry.data[CONF_NEXT_MESH_PRIORITY] == 3
        assert CONF_PROVISIONING_TRANSACTION_ID not in entry.subentries["panel-office"].data
        journal.async_complete_commit.assert_awaited_once()
        assert persisted.await_count == 2

        await fleet.async_shutdown()


async def test_live_commit_failure_schedules_one_reload_with_markerless_owner(
    hass: HomeAssistant,
) -> None:
    """A failed journal write schedules recovery without rolling back ownership."""
    entry = _provisioning_entry(
        _panel(
            "kitchen",
            "SHA256:kitchen",
            subentry_id="panel-kitchen",
            mesh_priority=1,
        ),
        scene_owner="panel-kitchen",
        next_mesh_priority=2,
    )
    entry.add_to_hass(hass)
    current_record: ProvisioningRecord | None = None
    commit_attempt = 0

    async def load_record() -> ProvisioningRecord | None:
        return current_record

    async def complete_commit(
        transaction_id: UUID,
        *,
        subentry_id: str,
    ) -> None:
        nonlocal commit_attempt, current_record
        assert transaction_id == _TRANSACTION_ID
        assert subentry_id == "panel-office"
        commit_attempt += 1
        if commit_attempt == 1:
            raise OSError("journal unavailable")
        current_record = None

    journal = Mock(
        async_load=AsyncMock(side_effect=load_record),
        async_complete_commit=AsyncMock(side_effect=complete_commit),
    )
    fleet = FleetManager(hass, entry)
    persisted = AsyncMock()

    with (
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.ProvisioningJournal",
            return_value=journal,
        ),
        patch(
            "custom_components.brilliant_mqtt.fleet_manager.async_wait_config_entry_persisted",
            persisted,
        ),
        patch.object(PanelManager, "async_setup", _noop_setup),
        patch.object(PanelManager, "async_shutdown", _noop_shutdown),
        patch.object(hass.config_entries, "async_schedule_reload") as schedule_reload,
    ):
        await fleet.async_setup()
        assert fleet._update_unsub is not None
        fleet._update_unsub()
        fleet._update_unsub = None

        current_record = _pending_record()
        assert hass.config_entries.async_add_subentry(
            entry,
            _panel(
                "office",
                "SHA256:office",
                subentry_id="panel-office",
                management_id=_OFFICE_FINGERPRINT,
                mesh_priority=2,
                transaction_id=_TRANSACTION_ID,
            ),
        )
        with pytest.raises(ConfigEntryNotReady, match="journal commit"):
            await fleet.async_panel_added("panel-office")

        schedule_reload.assert_called_once_with(entry.entry_id)
        assert "panel-office" in fleet.panels
        assert CONF_PROVISIONING_TRANSACTION_ID not in entry.subentries["panel-office"].data
        assert current_record is not None
        assert commit_attempt == 1
        assert persisted.await_count == 2

        await fleet.async_shutdown()


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
        patch("custom_components.brilliant_mqtt.ha_control.get_control_plane") as get_control_plane,
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
        get_control_plane.assert_not_called()

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

    with (
        patch.object(PanelManager, "async_shutdown", shutdown),
        patch.object(hass.config_entries, "async_schedule_reload") as schedule_reload,
    ):
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
        schedule_reload.assert_called_once_with(entry.entry_id)
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
        patch.object(hass.config_entries, "async_schedule_reload") as schedule_reload,
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
        schedule_reload.assert_called_once_with(entry.entry_id)
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
