"""Thin, durable orchestration for staged fleet-panel provisioning."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any
from uuid import UUID

import pytest
from homeassistant.core import HomeAssistant

from custom_components.brilliant_mqtt import panel_ops, panel_provisioner
from custom_components.brilliant_mqtt.broker import BrokerKind, BrokerProfile
from custom_components.brilliant_mqtt.broker_validation import BrokerValidationResult
from custom_components.brilliant_mqtt.const import (
    COMPONENT_BRIDGE,
    COMPONENT_BUS_WATCHDOG,
    COMPONENT_WIFI_WATCHDOG,
    CONF_COMPONENTS,
    CONF_FEATURE_OVERRIDES,
    CONF_HOST,
    CONF_IDENTITY_FINGERPRINT,
    CONF_MANAGEMENT_ID,
    CONF_MESH_PRIORITY,
    CONF_PANEL,
    CONF_PROVISIONING_TRANSACTION_ID,
    CONF_ROOT_PASSWORD,
    CONF_SSH_HOST_KEY,
    CONF_SSH_USERNAME,
    ENTRY_KIND_FLEET,
)
from custom_components.brilliant_mqtt.entry_data import FleetConfig
from custom_components.brilliant_mqtt.errors import OperationError, OperationStage
from custom_components.brilliant_mqtt.panel_health import PanelHealthEvidence
from custom_components.brilliant_mqtt.panel_inspection import (
    PanelCompatibilityError,
    PanelFacts,
)
from custom_components.brilliant_mqtt.panel_provisioner import (
    CanonicalPanelData,
    PanelInstallRequest,
    PanelPreflightLauncher,
    PanelProvisioner,
    PanelProvisioningError,
    PanelReleaseBundle,
    ProvisioningProgress,
    StagedRelease,
    TransactionLookup,
    TransactionLookupState,
    _settle,
    panel_release_provider,
    panel_snapshot_from_stored,
    staged_release_from_journal,
    stored_snapshot_from_panel,
)
from custom_components.brilliant_mqtt.provisioning_journal import (
    ProvisioningOperation,
    ProvisioningPhase,
    ProvisioningRecord,
    StoredFileSnapshot,
    StoredFleetProfile,
    StoredJournalError,
    StoredPanelLayout,
    StoredPanelRequest,
    StoredPanelSnapshot,
    StoredServiceSnapshot,
)
from custom_components.brilliant_mqtt.setup_protocol import PreflightRequest
from custom_components.brilliant_mqtt.shell import HostIdentity, PanelShell
from tests.fakes import FakePanelProcess, FakeShell

_PUBLIC_KEY = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIKIykuTed7zNwJwn20eCelcKcHKJ9c/pGFfvulRWazuC"
_FINGERPRINT = "SHA256:JfCon51dCgE/yWGkyroh3Ne+ONLMm6QmHMQnEoPSLx0"
_TRANSACTION_ID = UUID("12345678-1234-4abc-8def-1234567890ab")
_SETUP_ID = UUID("87654321-4321-4cba-8fed-ba0987654321")
_STARTED_AT = datetime(2026, 7, 27, 18, 30, tzinfo=UTC)
_VERSION = "0.7.0"
_CA_BYTES = b"-----BEGIN CERTIFICATE-----\nPUBLIC-CA\n-----END CERTIFICATE-----"
_RELEASE_CA_PATH = f"/var/brilliant-mqtt/releases/{_VERSION}--{_TRANSACTION_ID.hex}/mqtt-ca.pem"


def _staged_release(
    version: str,
    transaction_id: UUID,
    selected_components: tuple[str, ...],
) -> StagedRelease:
    return StagedRelease(
        version=version,
        transaction_id=transaction_id,
        release_target=(f"/var/brilliant-mqtt/releases/{version}--{transaction_id.hex}"),
        selected_components=selected_components,
    )


def _request(
    *,
    ssh_username: str = "root",
    mesh_priority: int = 2,
    slug: str = "office",
    selected_components: tuple[str, ...] = (
        COMPONENT_BUS_WATCHDOG,
        COMPONENT_BRIDGE,
    ),
    feature_overrides: Any = None,
) -> PanelInstallRequest:
    return PanelInstallRequest(
        host="office.iot.example",
        ssh_username=ssh_username,
        root_password="SECRET-root-password",
        display_name="Office",
        slug=slug,
        mesh_priority=mesh_priority,
        selected_components=selected_components,
        feature_overrides={} if feature_overrides is None else feature_overrides,
    )


def test_install_request_is_strict_detached_and_redacted() -> None:
    overrides: dict[str, Any] = {
        "nested": [{"enabled": True}],
        "threshold": 2.5,
    }

    request = _request(feature_overrides=overrides)
    overrides["nested"][0]["enabled"] = False

    assert request.selected_components == (
        COMPONENT_BRIDGE,
        COMPONENT_BUS_WATCHDOG,
    )
    assert isinstance(request.feature_overrides, MappingProxyType)
    assert request.feature_overrides["nested"] == [{"enabled": True}]
    assert repr(request) == "PanelInstallRequest(<redacted>)"
    assert "SECRET" not in repr(request)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("ssh_username", "admin"),
        ("selected_components", (COMPONENT_WIFI_WATCHDOG,)),
        ("selected_components", (COMPONENT_BRIDGE, COMPONENT_BRIDGE)),
        ("selected_components", (COMPONENT_BRIDGE, "voice")),
        ("feature_overrides", {"invalid": float("nan")}),
        ("feature_overrides", {"invalid": object()}),
        ("mesh_priority", 100),
        ("slug", "mesh"),
    ],
)
def test_install_request_rejects_invalid_or_non_json_safe_data(
    field: str,
    value: object,
) -> None:
    kwargs = {field: value}

    with pytest.raises(PanelProvisioningError) as raised:
        _request(**kwargs)  # type: ignore[arg-type]

    assert raised.value.code == "invalid_panel_install_request"
    assert repr(raised.value) == ("PanelProvisioningError(code='invalid_panel_install_request')")
    assert "SECRET" not in repr(raised.value)


def test_provisioning_error_rejects_unallowlisted_cleanup_code() -> None:
    with pytest.raises(ValueError, match="^invalid_panel_provisioning_error_code$") as raised:
        PanelProvisioningError(
            "stage_failed",
            cleanup_code="SECRET-cleanup-code",
        )

    assert "SECRET" not in str(raised.value)
    assert "SECRET" not in repr(raised.value)


def test_canonical_panel_data_detaches_input_and_returned_nested_values() -> None:
    source: dict[str, Any] = {
        CONF_COMPONENTS: {COMPONENT_BRIDGE: True},
        CONF_FEATURE_OVERRIDES: {"nested": [{"enabled": True}]},
    }
    panel_data = CanonicalPanelData(MappingProxyType(source))
    source[CONF_COMPONENTS][COMPONENT_BRIDGE] = False

    components = panel_data[CONF_COMPONENTS]
    overrides = panel_data[CONF_FEATURE_OVERRIDES]
    assert isinstance(components, dict)
    assert isinstance(overrides, dict)
    components[COMPONENT_BRIDGE] = False
    overrides["nested"][0]["enabled"] = False

    assert panel_data[CONF_COMPONENTS] == {COMPONENT_BRIDGE: True}
    assert panel_data[CONF_FEATURE_OVERRIDES] == {"nested": [{"enabled": True}]}
    assert panel_data.as_dict()[CONF_COMPONENTS] == {COMPONENT_BRIDGE: True}


def test_release_bundle_rejects_oversized_custom_ca() -> None:
    oversized_ca = b"A" * (256 * 1024 + 1)

    with pytest.raises(ValueError, match="invalid_panel_release_bundle"):
        PanelReleaseBundle(
            local_payload_dir="/trusted/payload",
            version=_VERSION,
            transaction_id=_TRANSACTION_ID,
            environment=f"MQTT_TLS_CA_FILE={_RELEASE_CA_PATH}\n",
            mqtt_ca=oversized_ca,
            mqtt_ca_path=_RELEASE_CA_PATH,
        )


def test_release_bundle_rejects_environment_larger_than_panel_stage_limit() -> None:
    with pytest.raises(ValueError, match="invalid_panel_release_bundle"):
        PanelReleaseBundle(
            local_payload_dir="/trusted/payload",
            version=_VERSION,
            transaction_id=_TRANSACTION_ID,
            environment="A" * (16 * 1024 + 1),
        )


def test_release_bundle_requires_exact_ca_environment_binding() -> None:
    with pytest.raises(ValueError, match="invalid_panel_release_bundle"):
        PanelReleaseBundle(
            local_payload_dir="/trusted/payload",
            version=_VERSION,
            transaction_id=_TRANSACTION_ID,
            environment=f'UNRELATED="{_RELEASE_CA_PATH}"\n',
            mqtt_ca=_CA_BYTES,
            mqtt_ca_path=_RELEASE_CA_PATH,
        )


def test_release_bundle_rejects_ca_reference_without_public_material() -> None:
    with pytest.raises(ValueError, match="invalid_panel_release_bundle"):
        PanelReleaseBundle(
            local_payload_dir="/trusted/payload",
            version=_VERSION,
            transaction_id=_TRANSACTION_ID,
            environment=f"MQTT_TLS_CA_FILE={_RELEASE_CA_PATH}\n",
        )


def test_release_bundle_rejects_global_or_other_transaction_ca_path() -> None:
    for path in (
        "/var/brilliant-mqtt/tls/mqtt-ca-deadbeefdeadbeef.pem",
        f"/var/brilliant-mqtt/releases/{_VERSION}--{_SETUP_ID.hex}/mqtt-ca.pem",
    ):
        with pytest.raises(ValueError, match="invalid_panel_release_bundle"):
            PanelReleaseBundle(
                local_payload_dir="/trusted/payload",
                version=_VERSION,
                transaction_id=_TRANSACTION_ID,
                environment=f"MQTT_TLS_CA_FILE={path}\n",
                mqtt_ca=_CA_BYTES,
                mqtt_ca_path=path,
            )


def test_release_bundle_ca_binding_uses_only_literal_lf_records() -> None:
    path = f"/var/brilliant-mqtt/releases/{_VERSION}--{_TRANSACTION_ID.hex}/mqtt-ca.pem"

    bundle = PanelReleaseBundle(
        local_payload_dir="/trusted/payload",
        version=_VERSION,
        transaction_id=_TRANSACTION_ID,
        environment=(
            f"BRILLIANT_DEPLOYMENT_ID={_TRANSACTION_ID.hex}\n"
            'MQTT_PASSWORD="before\u2028MQTT_TLS_CA_FILE=not-an-assignment"\n'
            f"MQTT_TLS_CA_FILE={path}\n"
        ),
        mqtt_ca=_CA_BYTES,
        mqtt_ca_path=path,
    )

    assert bundle.mqtt_ca_path == path


async def test_concrete_release_provider_uses_bundled_payload_and_broker_seam(
    hass: HomeAssistant,
) -> None:
    provider = panel_release_provider(hass)

    bundle = await provider(
        _request(),
        _fleet(custom_ca=True),
        _TRANSACTION_ID,
        _SETUP_ID,
    )

    assert bundle.local_payload_dir.endswith("/custom_components/brilliant_mqtt/agent_payload")
    assert bundle.version == "0.6.0"
    assert bundle.transaction_id == _TRANSACTION_ID
    assert bundle.mqtt_ca == _CA_BYTES
    assert bundle.mqtt_ca_path == (
        f"/var/brilliant-mqtt/releases/0.6.0--{_TRANSACTION_ID.hex}/mqtt-ca.pem"
    )
    parsed = panel_ops.parse_env(bundle.environment)
    assert parsed["BRILLIANT_PANEL"] == "office"
    assert parsed["BRILLIANT_DEPLOYMENT_ID"] == _TRANSACTION_ID.hex
    assert parsed["MESH_PRIORITY"] == "2"
    assert parsed["MQTT_USERNAME"] == "SECRET-mqtt-user"
    assert parsed["MQTT_PASSWORD"] == "SECRET-mqtt-password"
    assert parsed["MQTT_TLS_CA_FILE"] == bundle.mqtt_ca_path
    assert repr(bundle) == "PanelReleaseBundle(<redacted>)"


def test_release_bundle_requires_exact_deployment_binding() -> None:
    for environment in (
        "MQTT_PASSWORD=secret\n",
        f"BRILLIANT_DEPLOYMENT_ID={_SETUP_ID.hex}\n",
        (
            f"BRILLIANT_DEPLOYMENT_ID={_TRANSACTION_ID.hex}\n"
            f"BRILLIANT_DEPLOYMENT_ID={_TRANSACTION_ID.hex}\n"
        ),
    ):
        with pytest.raises(ValueError, match="invalid_panel_release_bundle"):
            PanelReleaseBundle(
                local_payload_dir="/trusted/payload",
                version=_VERSION,
                transaction_id=_TRANSACTION_ID,
                environment=environment,
            )


def _fleet(*, custom_ca: bool = False) -> FleetConfig:
    return FleetConfig(
        entry_kind=ENTRY_KIND_FLEET,
        broker=BrokerProfile(
            kind=BrokerKind.EXISTING_BROKER,
            host="mqtt.iot.example",
            port=8883,
            tls_enabled=True,
            _username_value="SECRET-mqtt-user",
            _password_value="SECRET-mqtt-password",
            _ca_pem_value=(
                "-----BEGIN CERTIFICATE-----\nPUBLIC-CA\n-----END CERTIFICATE-----"
                if custom_ca
                else None
            ),
        ),
        next_mesh_priority=3,
        ha_control_enabled=False,
        ha_control_label="brilliant",
        room_overrides=MappingProxyType({}),
        ha_control_domains=("light", "switch"),
        max_mirrored_entities=50,
        scene_panel="none",
        scene_actions=MappingProxyType({}),
        schema_version=4,
    )


def _absent_snapshot() -> StoredPanelSnapshot:
    missing = StoredFileSnapshot(content=None, mode=None)
    missing_service = StoredServiceSnapshot(
        unit_file=missing,
        enabled=False,
        active=False,
    )
    return StoredPanelSnapshot(
        layout=StoredPanelLayout.ABSENT,
        active_release_target=None,
        environment_file=missing,
        version_file=missing,
        bridge_service=missing_service,
        wifi_watchdog_service=missing_service,
        bus_watchdog_service=missing_service,
        selected_components=(),
    )


def _release_link_snapshot() -> StoredPanelSnapshot:
    return StoredPanelSnapshot(
        layout=StoredPanelLayout.RELEASE_LINK,
        active_release_target=(f"/var/brilliant-mqtt/releases/0.6.0--{_TRANSACTION_ID.hex}"),
        environment_file=StoredFileSnapshot(
            content=b"MQTT_PASSWORD=SECRET-old\n",
            mode=0o600,
        ),
        version_file=StoredFileSnapshot(content=b"0.6.0\n", mode=0o644),
        bridge_service=StoredServiceSnapshot(
            unit_file=StoredFileSnapshot(content=b"bridge-unit\n", mode=0o640),
            enabled=True,
            active=True,
        ),
        wifi_watchdog_service=StoredServiceSnapshot(
            unit_file=StoredFileSnapshot(content=b"wifi-unit\n", mode=0o644),
            enabled=True,
            active=False,
        ),
        bus_watchdog_service=StoredServiceSnapshot(
            unit_file=StoredFileSnapshot(content=b"bus-unit\n", mode=0o600),
            enabled=False,
            active=True,
        ),
        selected_components=(
            COMPONENT_BRIDGE,
            COMPONENT_WIFI_WATCHDOG,
            COMPONENT_BUS_WATCHDOG,
        ),
    )


def _facts() -> PanelFacts:
    return PanelFacts(
        fingerprint=_FINGERPRINT,
        hostname="office-panel",
        model="BHA120US-WH2",
        architecture="armv7l",
        firmware="2026.07",
        python_version="3.8.2",
        init_system="systemd",
        available_bytes=128 * 1024 * 1024,
        available_memory_bytes=64 * 1024 * 1024,
        installed_agent_version=None,
        active_services=(),
        conflicting_services=(),
    )


def _health() -> PanelHealthEvidence:
    return PanelHealthEvidence(
        panel="office",
        agent_version=_VERSION,
        deployment_id=_TRANSACTION_ID.hex,
        state_topic="brilliant/office/load-1/state",
        discovery_topic="homeassistant/light/brilliant_office_load-1/config",
        device_identifier="brilliant_panel_office",
    )


def _record(phase: ProvisioningPhase) -> ProvisioningRecord:
    return ProvisioningRecord(
        transaction_id=_TRANSACTION_ID,
        operation=ProvisioningOperation.INSTALL,
        phase=phase,
        setup_id=_SETUP_ID,
        panel_request=StoredPanelRequest(
            host="office.iot.example",
            ssh_username="root",
            root_password="SECRET-root-password",
            public_key=_PUBLIC_KEY,
            fingerprint=_FINGERPRINT,
            slug="office",
            selected_components=(
                COMPONENT_BRIDGE,
                COMPONENT_BUS_WATCHDOG,
            ),
        ),
        fleet_profile=StoredFleetProfile(
            kind=BrokerKind.EXISTING_BROKER,
            host="mqtt.iot.example",
            port=8883,
            tls_enabled=True,
        ),
        staged_version=_VERSION,
        prior_snapshot=_absent_snapshot(),
        started_at=_STARTED_AT,
        last_error=None,
    )


class _EventShell(FakeShell):
    def __init__(
        self,
        events: list[tuple[object, ...]],
        *,
        pinned: str | None = _PUBLIC_KEY,
    ) -> None:
        super().__init__(pinned=pinned)
        self._events = events
        self.block_close: asyncio.Event | None = None
        self.close_started = asyncio.Event()
        self.fail_close_once = False
        self.close_count = 0

    async def connect(self) -> None:
        self._events.append(("shell_connect",))
        await super().connect()

    async def close(self) -> None:
        self._events.append(("shell_close",))
        self.close_count += 1
        first_close = self.close_count == 1
        if first_close:
            self.close_started.set()
            if self.block_close is not None:
                await self.block_close.wait()
        await super().close()
        if first_close and self.fail_close_once:
            raise RuntimeError("SECRET shell close failure")


@dataclass
class _FakeJournal:
    events: list[tuple[object, ...]]
    record: ProvisioningRecord | None = None
    fail_create: bool = False
    fail_record_error: bool = False
    block_after_create: bool = False
    create_committed: asyncio.Event = field(default_factory=asyncio.Event)
    allow_create: asyncio.Event = field(default_factory=asyncio.Event)
    block_after_transition: ProvisioningPhase | None = None
    transition_committed: asyncio.Event = field(default_factory=asyncio.Event)
    allow_transition: asyncio.Event = field(default_factory=asyncio.Event)
    block_pending_commit: asyncio.Event | None = None
    block_complete_commit: asyncio.Event | None = None
    pending_commit_started: asyncio.Event = field(default_factory=asyncio.Event)
    complete_commit_started: asyncio.Event = field(default_factory=asyncio.Event)
    created_record: ProvisioningRecord | None = None

    async def async_load(self) -> ProvisioningRecord | None:
        self.events.append(("journal_load",))
        return self.record

    async def async_create(self, record: ProvisioningRecord) -> ProvisioningRecord:
        self.events.append(("journal_create", record.phase.value))
        if self.fail_create:
            raise RuntimeError("SECRET journal create failure")
        assert self.record is None
        self.created_record = record
        self.record = record
        if self.block_after_create:
            self.create_committed.set()
            await self.allow_create.wait()
        return record

    async def async_transition(
        self,
        transaction_id: UUID,
        phase: ProvisioningPhase,
        *,
        last_error: StoredJournalError | None = None,
    ) -> ProvisioningRecord:
        self.events.append(("journal_transition", phase.value))
        if phase is ProvisioningPhase.PENDING_CONFIG_COMMIT:
            self.pending_commit_started.set()
            if self.block_pending_commit is not None:
                await self.block_pending_commit.wait()
        assert self.record is not None
        assert self.record.transaction_id == transaction_id
        self.record = replace(self.record, phase=phase, last_error=last_error)
        if phase is self.block_after_transition:
            self.transition_committed.set()
            await self.allow_transition.wait()
        return self.record

    async def async_record_error(
        self,
        transaction_id: UUID,
        last_error: StoredJournalError,
    ) -> ProvisioningRecord:
        self.events.append(("journal_error", last_error.stage, last_error.code))
        if self.fail_record_error:
            raise RuntimeError("SECRET journal error write failure")
        assert self.record is not None
        assert self.record.transaction_id == transaction_id
        self.record = replace(self.record, last_error=last_error)
        return self.record

    async def async_complete_commit(
        self,
        transaction_id: UUID,
        *,
        subentry_id: str,
    ) -> ProvisioningRecord:
        self.events.append(("journal_commit", subentry_id))
        self.complete_commit_started.set()
        if self.block_complete_commit is not None:
            await self.block_complete_commit.wait()
        assert self.record is not None
        assert self.record.transaction_id == transaction_id
        committed = replace(self.record, phase=ProvisioningPhase.COMMITTED)
        self.record = None
        return committed

    async def async_clear_committed(
        self,
        transaction_id: UUID,
    ) -> ProvisioningRecord:
        self.events.append(("journal_clear_committed",))
        assert self.record is not None
        assert self.record.transaction_id == transaction_id
        assert self.record.phase is ProvisioningPhase.COMMITTED
        committed = self.record
        self.record = None
        return committed

    async def async_complete_rollback(
        self,
        transaction_id: UUID,
        *,
        verified: bool,
    ) -> ProvisioningRecord:
        self.events.append(("journal_rollback_complete", verified))
        assert self.record is not None
        assert self.record.transaction_id == transaction_id
        rolled_back = replace(self.record, phase=ProvisioningPhase.ROLLED_BACK)
        self.record = None
        return rolled_back


@dataclass
class _FakeOperations:
    events: list[tuple[object, ...]]
    snapshot: panel_ops.PanelSnapshot
    fail_at: set[str] = field(default_factory=set)
    block_activation: asyncio.Event | None = None
    block_rollback: asyncio.Event | None = None
    activation_started: asyncio.Event = field(default_factory=asyncio.Event)
    rollback_started: asyncio.Event = field(default_factory=asyncio.Event)
    rolled_back_snapshot: panel_ops.PanelSnapshot | None = None

    async def snapshot_panel(self, shell: PanelShell) -> panel_ops.PanelSnapshot:
        del shell
        self.events.append(("snapshot",))
        return self.snapshot

    async def stage_release(
        self,
        shell: PanelShell,
        local_payload_dir: str,
        version: str,
        environment: str,
        selected_components: tuple[str, ...],
        transaction_id: UUID,
        mqtt_ca: bytes | None = None,
    ) -> StagedRelease:
        del shell
        self.events.append(
            (
                "stage",
                local_payload_dir,
                version,
                selected_components,
                transaction_id,
                mqtt_ca is not None,
            )
        )
        assert "SECRET" in environment
        assert mqtt_ca is None or mqtt_ca == _CA_BYTES
        if "stage" in self.fail_at:
            raise RuntimeError("SECRET stage failure")
        return _staged_release(
            version,
            transaction_id,
            selected_components,
        )

    async def activate_staged(
        self,
        shell: PanelShell,
        staged: StagedRelease,
        *,
        on_services_stopped: Callable[[], None],
    ) -> None:
        del shell
        self.events.append(("activation_stopped", staged.transaction_id))
        on_services_stopped()
        self.events.append(("activate", staged.transaction_id))
        self.activation_started.set()
        if self.block_activation is not None:
            await self.block_activation.wait()
        if "activate" in self.fail_at:
            raise RuntimeError("SECRET activation failure")

    async def restart_candidate(
        self,
        shell: PanelShell,
        staged: StagedRelease,
        *,
        on_service_stopped: Callable[[], None],
    ) -> None:
        del shell
        self.events.append(("restart_stopped", staged.transaction_id))
        on_service_stopped()
        self.events.append(("restart", staged.transaction_id))

    async def rollback_snapshot(
        self,
        shell: PanelShell,
        snapshot: panel_ops.PanelSnapshot,
        staged: StagedRelease,
    ) -> None:
        del shell
        self.rolled_back_snapshot = snapshot
        self.events.append(("rollback", snapshot.layout.value, staged.transaction_id))
        self.rollback_started.set()
        if self.block_rollback is not None:
            await self.block_rollback.wait()
        if "rollback" in self.fail_at:
            raise RuntimeError("SECRET rollback failure")

    async def cleanup_staged(
        self,
        shell: PanelShell,
        staged: StagedRelease,
    ) -> None:
        del shell
        self.events.append(("cleanup_staged", staged.transaction_id))
        if "cleanup" in self.fail_at:
            raise RuntimeError("SECRET cleanup failure")


@dataclass
class _FakeObserver:
    events: list[tuple[object, ...]]
    evidence: PanelHealthEvidence
    fail_subscribe: bool = False
    fail_wait: bool = False
    fail_close: bool = False
    block_close: asyncio.Event | None = None
    close_started: asyncio.Event = field(default_factory=asyncio.Event)

    async def async_subscribe(self) -> None:
        self.events.append(("health_subscribe",))
        if self.fail_subscribe:
            raise RuntimeError("SECRET health subscribe failure")

    def mark_activation_started(
        self,
        expected_version: str,
        expected_deployment_id: str,
    ) -> None:
        self.events.append(
            (
                "health_boundary",
                expected_version,
                expected_deployment_id,
            )
        )

    async def async_wait(
        self,
        expected_version: str,
        timeout: float = 90.0,  # noqa: ASYNC109 - mirrors observer API
    ) -> PanelHealthEvidence:
        self.events.append(("health_wait", expected_version, timeout))
        if self.fail_wait:
            raise RuntimeError("SECRET health payload")
        return self.evidence

    async def async_close(self) -> None:
        self.events.append(("health_close",))
        self.close_started.set()
        if self.block_close is not None:
            await self.block_close.wait()
        if self.fail_close:
            raise RuntimeError("SECRET observer close failure")


class _FakeRepairReporter:
    def __init__(self, events: list[tuple[object, ...]]) -> None:
        self.events = events

    async def async_report_rollback_failure(
        self,
        transaction_id: UUID,
        *,
        original_code: str,
        rollback_code: str,
    ) -> None:
        self.events.append(("repair", transaction_id, original_code, rollback_code))


class _Harness:
    def __init__(self) -> None:
        self.events: list[tuple[object, ...]] = []
        self.journal = _FakeJournal(self.events)
        self.operations = _FakeOperations(
            self.events,
            panel_snapshot_from_stored(_absent_snapshot()),
        )
        self.shell = _EventShell(self.events)
        self.observer = _FakeObserver(self.events, _health())
        self.lookup = TransactionLookup(TransactionLookupState.FLOW_PENDING)
        self.ids = iter((_TRANSACTION_ID, _SETUP_ID))
        self.duplicate_found = False
        self.inspection_error: PanelCompatibilityError | None = None
        self.preflight_error: BaseException | None = None
        self.omit_custom_ca = False

    async def fetch_identity(self, host: str) -> HostIdentity:
        self.events.append(("identity", host))
        return HostIdentity(_PUBLIC_KEY, _FINGERPRINT)

    def shell_factory(
        self,
        host: str,
        root_password: str,
        pinned_host_key: str,
    ) -> PanelShell:
        assert host == "office.iot.example"
        assert root_password == "SECRET-root-password"
        assert pinned_host_key == _PUBLIC_KEY
        self.events.append(("shell_factory",))
        return self.shell

    async def inspect(
        self,
        shell: PanelShell,
        identity: HostIdentity,
    ) -> PanelFacts:
        del shell
        assert identity.fingerprint == _FINGERPRINT
        self.events.append(("inspect",))
        if self.inspection_error is not None:
            raise self.inspection_error
        return _facts()

    async def duplicate(self, fingerprint: str) -> bool:
        self.events.append(("duplicate", fingerprint))
        return self.duplicate_found

    async def release(
        self,
        request: PanelInstallRequest,
        fleet: FleetConfig,
        transaction_id: UUID,
        setup_id: UUID,
    ) -> PanelReleaseBundle:
        del request
        assert fleet.broker.host == "mqtt.iot.example"
        self.events.append(("release", transaction_id, setup_id))
        mqtt_ca = None if not fleet.broker.has_custom_ca or self.omit_custom_ca else _CA_BYTES
        mqtt_ca_path = (
            None
            if mqtt_ca is None
            else (f"/var/brilliant-mqtt/releases/{_VERSION}--{transaction_id.hex}/mqtt-ca.pem")
        )
        return PanelReleaseBundle(
            local_payload_dir="/trusted/payload",
            version=_VERSION,
            transaction_id=transaction_id,
            environment=(
                f"BRILLIANT_DEPLOYMENT_ID={transaction_id.hex}\n"
                "MQTT_PASSWORD=SECRET-mqtt-password\n"
                + (f"MQTT_TLS_CA_FILE={mqtt_ca_path}\n" if mqtt_ca_path is not None else "")
            ),
            mqtt_ca=mqtt_ca,
            mqtt_ca_path=mqtt_ca_path,
        )

    def launcher(
        self,
        shell: PanelShell,
        staged: StagedRelease,
    ) -> PanelPreflightLauncher:
        del shell
        self.events.append(("launcher", staged.transaction_id))

        async def launch(raw_request: str) -> FakePanelProcess:
            assert raw_request
            return FakePanelProcess()

        return launch

    async def async_validate_panel(
        self,
        profile: BrokerProfile,
        launcher: PanelPreflightLauncher,
        setup_id: UUID | None = None,
    ) -> BrokerValidationResult:
        assert callable(launcher)
        assert setup_id is not None
        assert profile.host == "mqtt.iot.example"
        self.events.append(("preflight", setup_id))
        if self.preflight_error is not None:
            raise self.preflight_error
        return BrokerValidationResult(
            setup_id=setup_id,
            completed_stages=tuple(OperationStage),
            elapsed_seconds=1.0,
            stage_elapsed_seconds=(),
        )

    def observer_factory(self, slug: str) -> _FakeObserver:
        assert slug == "office"
        self.events.append(("health_factory", slug))
        return self.observer

    async def transaction_lookup(self, transaction_id: UUID) -> TransactionLookup:
        self.events.append(("transaction_lookup", transaction_id))
        return self.lookup

    async def progress(self, update: ProvisioningProgress) -> None:
        self.events.append(("progress", update.stage.value))

    @staticmethod
    def snapshot_to_stored(snapshot: panel_ops.PanelSnapshot) -> StoredPanelSnapshot:
        return stored_snapshot_from_panel(snapshot)

    @staticmethod
    def snapshot_from_stored(snapshot: StoredPanelSnapshot) -> panel_ops.PanelSnapshot:
        return panel_snapshot_from_stored(snapshot)

    def id_factory(self) -> UUID:
        return next(self.ids)

    def provisioner(self) -> PanelProvisioner:
        return PanelProvisioner(
            operation_lock=asyncio.Lock(),
            journal=self.journal,
            identity_fetcher=self.fetch_identity,
            shell_factory=self.shell_factory,
            inspector=self.inspect,
            duplicate_fingerprint=self.duplicate,
            operations=self.operations,
            snapshot_to_stored=self.snapshot_to_stored,
            snapshot_from_stored=self.snapshot_from_stored,
            staged_release_factory=staged_release_from_journal,
            release_provider=self.release,
            preflight_launcher_factory=self.launcher,
            broker_validator=self,
            health_observer_factory=self.observer_factory,
            transaction_lookup=self.transaction_lookup,
            repair_reporter=_FakeRepairReporter(self.events),
            id_factory=self.id_factory,
            clock=lambda: _STARTED_AT,
            health_timeout=45.0,
        )


async def test_install_has_exact_durable_order_and_returns_canonical_panel_data() -> None:
    harness = _Harness()

    result = await harness.provisioner().async_install(
        _request(feature_overrides={"scene": {"enabled": True}}),
        _fleet(),
        harness.progress,
    )

    assert result.identity == HostIdentity(_PUBLIC_KEY, _FINGERPRINT)
    assert result.facts == _facts()
    assert result.version == _VERSION
    assert result.health == _health()
    assert result.transaction_id == _TRANSACTION_ID
    assert result.setup_id == _SETUP_ID
    assert result.panel_data[CONF_IDENTITY_FINGERPRINT] == _FINGERPRINT
    assert result.panel_data[CONF_SSH_HOST_KEY] == _PUBLIC_KEY
    assert result.panel_data[CONF_HOST] == "office.iot.example"
    assert result.panel_data[CONF_SSH_USERNAME] == "root"
    assert result.panel_data[CONF_ROOT_PASSWORD] == "SECRET-root-password"
    assert result.panel_data[CONF_PANEL] == "office"
    assert result.panel_data[CONF_MANAGEMENT_ID] == _FINGERPRINT
    assert result.panel_data[CONF_COMPONENTS] == {
        COMPONENT_BRIDGE: True,
        COMPONENT_WIFI_WATCHDOG: False,
        COMPONENT_BUS_WATCHDOG: True,
    }
    assert result.panel_data[CONF_FEATURE_OVERRIDES] == {"scene": {"enabled": True}}
    assert result.panel_data[CONF_MESH_PRIORITY] == 2
    assert result.panel_data[CONF_PROVISIONING_TRANSACTION_ID] == str(_TRANSACTION_ID)
    assert repr(result.panel_data) == "CanonicalPanelData(<redacted>)"
    assert "SECRET" not in repr(result.panel_data)
    assert repr(result) == (
        "ProvisionedPanel(transaction_id='12345678-1234-4abc-8def-1234567890ab', version='0.7.0')"
    )
    assert "SECRET" not in repr(result)
    assert harness.journal.record is not None
    assert harness.journal.record.phase is ProvisioningPhase.VERIFYING
    assert harness.journal.record.operation is ProvisioningOperation.INSTALL

    significant = [
        event
        for event in harness.events
        if event[0]
        in {
            "journal_load",
            "identity",
            "shell_connect",
            "inspect",
            "duplicate",
            "snapshot",
            "release",
            "stage",
            "preflight",
            "journal_create",
            "journal_transition",
            "health_subscribe",
            "activation_stopped",
            "health_boundary",
            "activate",
            "health_wait",
            "shell_close",
        }
    ]
    assert significant == [
        ("journal_load",),
        ("identity", "office.iot.example"),
        ("shell_connect",),
        ("inspect",),
        ("duplicate", _FINGERPRINT),
        ("snapshot",),
        ("release", _TRANSACTION_ID, _SETUP_ID),
        ("journal_create", ProvisioningPhase.STAGED.value),
        (
            "stage",
            "/trusted/payload",
            _VERSION,
            (COMPONENT_BRIDGE, COMPONENT_BUS_WATCHDOG),
            _TRANSACTION_ID,
            False,
        ),
        ("preflight", _SETUP_ID),
        ("journal_transition", ProvisioningPhase.ACTIVATION_PENDING.value),
        ("health_subscribe",),
        ("activation_stopped", _TRANSACTION_ID),
        ("health_boundary", _VERSION, _TRANSACTION_ID.hex),
        ("activate", _TRANSACTION_ID),
        ("journal_transition", ProvisioningPhase.ACTIVATED.value),
        ("health_wait", _VERSION, 45.0),
        ("journal_transition", ProvisioningPhase.VERIFYING.value),
        ("shell_close",),
    ]


def _exception_graph(root: BaseException) -> list[BaseException]:
    found: list[BaseException] = []
    pending = [root]
    seen: set[int] = set()
    while pending:
        error = pending.pop()
        if id(error) in seen:
            continue
        seen.add(id(error))
        found.append(error)
        if error.__context__ is not None:
            pending.append(error.__context__)
        if error.__cause__ is not None:
            pending.append(error.__cause__)
        pending.extend(argument for argument in error.args if isinstance(argument, BaseException))
    return found


async def test_unsupported_toolchain_stops_before_snapshot_journal_or_panel_write() -> None:
    harness = _Harness()
    compatibility_error = PanelCompatibilityError(
        "unsupported_panel_toolchain",
        capability="mv_no_target_directory",
    )
    harness.inspection_error = compatibility_error

    with pytest.raises(PanelProvisioningError) as raised:
        await harness.provisioner().async_install(
            _request(),
            _fleet(),
            harness.progress,
        )

    assert compatibility_error.capability == "mv_no_target_directory"
    assert str(compatibility_error) == "unsupported_panel_toolchain"
    assert raised.value.code == "unsupported_panel_toolchain"
    assert raised.value.capability == "mv_no_target_directory"
    assert "mv_no_target_directory" in repr(raised.value)
    forbidden = {
        "snapshot",
        "release",
        "journal_create",
        "journal_transition",
        "journal_error",
        "stage",
        "preflight",
        "health_subscribe",
        "activation_stopped",
        "activate",
        "rollback",
        "cleanup_staged",
    }
    assert not any(event[0] in forbidden for event in harness.events)
    assert harness.events[-1] == ("shell_close",)
    assert _exception_graph(raised.value) == [raised.value]
    assert "SECRET" not in repr(raised.value)
    assert "SECRET" not in repr(compatibility_error)


def test_provisioning_error_rejects_capability_on_unrelated_code() -> None:
    with pytest.raises(ValueError, match="^invalid_panel_provisioning_error_code$"):
        PanelProvisioningError(
            "inspection_failed",
            capability="SECRET-untrusted-tool-output",
        )


async def test_duplicate_identity_is_rejected_before_snapshot_or_any_write() -> None:
    harness = _Harness()
    harness.duplicate_found = True

    with pytest.raises(PanelProvisioningError) as raised:
        await harness.provisioner().async_install(
            _request(),
            _fleet(),
            harness.progress,
        )

    assert raised.value.code == "duplicate_panel"
    assert not any(event[0] in {"snapshot", "stage", "journal_create"} for event in harness.events)
    assert harness.events[-1] == ("shell_close",)
    assert _exception_graph(raised.value) == [raised.value]


async def test_install_rejects_unpinned_shell_before_password_authentication() -> None:
    harness = _Harness()
    harness.shell = _EventShell(harness.events, pinned=None)

    with pytest.raises(PanelProvisioningError) as raised:
        await harness.provisioner().async_install(
            _request(),
            _fleet(),
            harness.progress,
        )

    assert raised.value.code == "panel_authentication_failed"
    assert not any(event[0] == "shell_connect" for event in harness.events)
    assert not any(event[0] in {"snapshot", "stage"} for event in harness.events)
    assert _exception_graph(raised.value) == [raised.value]


async def test_pre_activation_failure_cleans_only_inactive_candidate() -> None:
    harness = _Harness()
    harness.preflight_error = RuntimeError("SECRET profile host password and environment")

    with pytest.raises(PanelProvisioningError) as raised:
        await harness.provisioner().async_install(
            _request(),
            _fleet(),
            harness.progress,
        )

    assert raised.value.code == "preflight_failed"
    assert (
        "cleanup_staged",
        _TRANSACTION_ID,
    ) in harness.events
    assert not any(event[0] == "rollback" for event in harness.events)
    assert any(event[0] == "journal_create" for event in harness.events)
    assert harness.journal.record is None
    assert "SECRET" not in repr(raised.value)
    assert _exception_graph(raised.value) == [raised.value]


async def test_custom_ca_stage_failure_cleans_transaction_owned_release() -> None:
    harness = _Harness()
    harness.operations.fail_at.add("stage")

    with pytest.raises(PanelProvisioningError) as raised:
        await harness.provisioner().async_install(
            _request(),
            _fleet(custom_ca=True),
            harness.progress,
        )

    assert raised.value.code == "stage_failed"
    stage_event = next(event for event in harness.events if event[0] == "stage")
    assert stage_event[-1] is True
    assert ("cleanup_staged", _TRANSACTION_ID) in harness.events
    assert "/var/brilliant-mqtt/tls/" not in repr(harness.events)
    assert harness.journal.record is None
    assert _exception_graph(raised.value) == [raised.value]


async def test_stage_and_cleanup_double_failure_remains_durable() -> None:
    harness = _Harness()
    harness.operations.fail_at.update({"stage", "cleanup"})

    with pytest.raises(PanelProvisioningError) as raised:
        await harness.provisioner().async_install(
            _request(),
            _fleet(),
            harness.progress,
        )

    assert raised.value.code == "stage_failed"
    assert raised.value.cleanup_code == "cleanup_failed"
    assert harness.journal.record is not None
    assert harness.journal.record.phase is ProvisioningPhase.STAGED
    assert harness.journal.record.last_error == StoredJournalError(
        stage="stage",
        code="stage_failed",
    )
    assert ("cleanup_staged", _TRANSACTION_ID) in harness.events
    assert harness.events.index(("journal_error", "stage", "stage_failed")) < (
        harness.events.index(("cleanup_staged", _TRANSACTION_ID))
    )
    assert (
        "repair",
        _TRANSACTION_ID,
        "stage_failed",
        "cleanup_failed",
    ) in harness.events
    assert not any(event[0] in {"activate", "rollback"} for event in harness.events)
    assert _exception_graph(raised.value) == [raised.value]


async def test_staged_diagnostic_write_failure_cannot_skip_candidate_cleanup() -> None:
    harness = _Harness()
    harness.journal.fail_record_error = True
    harness.operations.fail_at.update({"stage", "cleanup"})

    with pytest.raises(PanelProvisioningError) as raised:
        await harness.provisioner().async_install(
            _request(),
            _fleet(),
            harness.progress,
        )

    assert raised.value.code == "stage_failed"
    assert raised.value.cleanup_code == "cleanup_failed"
    assert harness.journal.record is not None
    assert harness.journal.record.phase is ProvisioningPhase.STAGED
    assert harness.journal.record.last_error is None
    assert ("cleanup_staged", _TRANSACTION_ID) in harness.events
    assert harness.events.index(("journal_error", "stage", "stage_failed")) < (
        harness.events.index(("cleanup_staged", _TRANSACTION_ID))
    )
    assert (
        "repair",
        _TRANSACTION_ID,
        "stage_failed",
        "cleanup_failed",
    ) in harness.events
    assert not any(event[0] in {"activate", "rollback"} for event in harness.events)
    assert "SECRET" not in repr(raised.value)
    assert _exception_graph(raised.value) == [raised.value]


async def test_staged_diagnostic_write_failure_is_retried_after_safe_cleanup() -> None:
    harness = _Harness()
    harness.journal.fail_record_error = True
    harness.operations.fail_at.add("stage")

    with pytest.raises(PanelProvisioningError) as raised:
        await harness.provisioner().async_install(
            _request(),
            _fleet(),
            harness.progress,
        )

    assert raised.value.code == "stage_failed"
    assert raised.value.cleanup_code is None
    assert harness.journal.record is None
    assert ("cleanup_staged", _TRANSACTION_ID) in harness.events
    assert not any(event[0] in {"repair", "rollback"} for event in harness.events)
    assert "SECRET" not in repr(raised.value)


async def test_journal_create_failure_prevents_all_panel_staging_writes() -> None:
    harness = _Harness()
    harness.journal.fail_create = True

    with pytest.raises(PanelProvisioningError) as raised:
        await harness.provisioner().async_install(
            _request(),
            _fleet(custom_ca=True),
            harness.progress,
        )

    assert raised.value.code == "journal_failed"
    assert not any(event[0] in {"stage", "cleanup_staged"} for event in harness.events)
    assert harness.journal.record is None
    assert _exception_graph(raised.value) == [raised.value]


async def test_preflight_cleanup_failure_stays_durable_and_is_surfaced() -> None:
    harness = _Harness()
    failure = OperationError.for_code(
        OperationStage.PANEL_TO_HA,
        "panel_to_ha_timeout",
    )
    harness.preflight_error = failure
    harness.operations.fail_at.add("cleanup")

    with pytest.raises(PanelProvisioningError) as raised:
        await harness.provisioner().async_install(
            _request(),
            _fleet(),
            harness.progress,
        )

    assert raised.value.code == "preflight_failed"
    assert raised.value.cleanup_code == "cleanup_failed"
    assert raised.value.detail is not None
    assert raised.value.detail.code == failure.code
    assert raised.value.detail.documentation_slug == failure.documentation_slug
    assert harness.journal.record is not None
    assert harness.journal.record.phase is ProvisioningPhase.STAGED
    assert harness.journal.record.last_error == StoredJournalError(
        stage="preflight",
        code="preflight_failed",
    )
    assert ("cleanup_staged", _TRANSACTION_ID) in harness.events
    assert harness.events.index(
        ("journal_error", "preflight", "preflight_failed")
    ) < harness.events.index(("cleanup_staged", _TRANSACTION_ID))
    assert (
        "repair",
        _TRANSACTION_ID,
        "preflight_failed",
        "cleanup_failed",
    ) in harness.events
    assert not any(event[0] == "rollback" for event in harness.events)
    assert "SECRET" not in repr(raised.value)
    assert _exception_graph(raised.value) == [raised.value]


@pytest.mark.parametrize(
    ("failure", "retryable"),
    [
        (
            OperationError.for_code(
                OperationStage.PANEL_TO_HA,
                "panel_to_ha_timeout",
            ),
            True,
        ),
        (
            OperationError.for_code(
                OperationStage.DISCOVERY_WRITE,
                "discovery_write_denied",
            ),
            False,
        ),
    ],
)
async def test_preflight_preserves_safe_actionable_mqtt_failure_metadata(
    failure: OperationError,
    retryable: bool,
) -> None:
    harness = _Harness()
    harness.preflight_error = failure

    with pytest.raises(PanelProvisioningError) as raised:
        await harness.provisioner().async_install(
            _request(),
            _fleet(),
            harness.progress,
        )

    assert raised.value.code == "preflight_failed"
    detail = raised.value.detail
    assert detail is not None
    assert detail.stage is failure.stage
    assert detail.code == failure.code
    assert detail.retryable is retryable
    assert detail.summary_key == failure.summary_key
    assert detail.documentation_slug == failure.documentation_slug
    assert detail.redacted_detail == failure.redacted_detail
    assert not isinstance(detail, BaseException)
    assert _exception_graph(raised.value) == [raised.value]


async def test_activation_failure_durably_rolls_back_and_clears_journal() -> None:
    harness = _Harness()
    harness.operations.fail_at.add("activate")

    with pytest.raises(PanelProvisioningError) as raised:
        await harness.provisioner().async_install(
            _request(),
            _fleet(),
            harness.progress,
        )

    assert raised.value.code == "activation_failed"
    rollback_pending = (
        "journal_transition",
        ProvisioningPhase.ROLLBACK_PENDING.value,
    )
    assert rollback_pending in harness.events
    assert (
        "rollback",
        StoredPanelLayout.ABSENT.value,
        _TRANSACTION_ID,
    ) in harness.events
    assert ("journal_rollback_complete", True) in harness.events
    assert harness.events.index(rollback_pending) < next(
        index for index, event in enumerate(harness.events) if event[0] == "rollback"
    )
    assert harness.journal.record is None
    assert _exception_graph(raised.value) == [raised.value]


async def test_health_subscription_failure_after_boundary_uses_exact_rollback() -> None:
    harness = _Harness()
    harness.observer.fail_subscribe = True

    with pytest.raises(PanelProvisioningError) as raised:
        await harness.provisioner().async_install(
            _request(),
            _fleet(),
            harness.progress,
        )

    assert raised.value.code == "health_subscribe_failed"
    assert any(event[0] == "rollback" for event in harness.events)
    assert harness.journal.record is None
    assert _exception_graph(raised.value) == [raised.value]


async def test_install_cancellation_waits_for_exact_rollback_and_preserves_cancel() -> None:
    harness = _Harness()
    harness.operations.block_activation = asyncio.Event()
    harness.operations.block_rollback = asyncio.Event()
    task = asyncio.create_task(
        harness.provisioner().async_install(
            _request(),
            _fleet(),
            harness.progress,
        )
    )
    await harness.operations.activation_started.wait()

    task.cancel()
    await harness.operations.rollback_started.wait()
    await asyncio.sleep(0)

    assert task.done() is False
    assert harness.journal.record is not None
    assert harness.journal.record.phase is ProvisioningPhase.ROLLBACK_PENDING
    task.cancel()
    await asyncio.sleep(0)
    assert task.done() is False
    harness.operations.block_rollback.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert harness.journal.record is None
    assert harness.events[-1] == ("shell_close",)


async def test_cancel_after_staged_journal_commit_settles_then_cleans_candidate() -> None:
    harness = _Harness()
    harness.journal.block_after_create = True
    task = asyncio.create_task(
        harness.provisioner().async_install(
            _request(),
            _fleet(),
            harness.progress,
        )
    )
    await harness.journal.create_committed.wait()

    task.cancel()
    await asyncio.sleep(0)

    assert not task.done()
    assert harness.journal.record is not None
    assert harness.journal.record.phase is ProvisioningPhase.STAGED
    harness.journal.allow_create.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert not any(event[0] == "stage" for event in harness.events)
    assert ("cleanup_staged", _TRANSACTION_ID) in harness.events
    assert harness.journal.record is None
    assert harness.events[-1] == ("shell_close",)


@pytest.mark.parametrize(
    ("phase", "expected_compensation"),
    [
        (ProvisioningPhase.ACTIVATION_PENDING, "cleanup_staged"),
        (ProvisioningPhase.ACTIVATED, "rollback"),
        (ProvisioningPhase.VERIFYING, "rollback"),
    ],
)
async def test_cancel_after_durable_phase_write_uses_safe_predecessor_compensation(
    phase: ProvisioningPhase,
    expected_compensation: str,
) -> None:
    harness = _Harness()
    harness.journal.block_after_transition = phase
    task = asyncio.create_task(
        harness.provisioner().async_install(
            _request(),
            _fleet(),
            harness.progress,
        )
    )
    await harness.journal.transition_committed.wait()

    task.cancel()
    await asyncio.sleep(0)
    harness.journal.allow_transition.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert any(event[0] == expected_compensation for event in harness.events)
    assert harness.journal.record is None
    assert harness.events[-1] == ("shell_close",)


async def test_settle_preserves_same_turn_cancellation_race() -> None:
    for _ in range(20):
        loop = asyncio.get_running_loop()
        operation: asyncio.Future[None] = loop.create_future()
        task = asyncio.create_task(_settle(operation))
        await asyncio.sleep(0)
        loop.call_soon(operation.set_result, None)
        loop.call_soon(task.cancel)

        outcome = await task

        assert outcome.cancellation is not None
        assert outcome.error is None


async def test_settle_classifies_dependency_cancellation_as_child_failure() -> None:
    async def internally_cancelled() -> None:
        raise asyncio.CancelledError("inner-cleanup")

    outcome = await _settle(internally_cancelled())

    assert isinstance(outcome.error, asyncio.CancelledError)
    assert outcome.error.args == ("inner-cleanup",)
    assert outcome.cancellation is None
    owner = asyncio.current_task()
    assert owner is not None
    assert owner.cancelling() == 0


async def test_settle_preserves_latest_repeated_caller_cancellation() -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def operation() -> None:
        started.set()
        await release.wait()

    task = asyncio.create_task(_settle(operation()))
    await started.wait()
    task.cancel("first-control")
    await asyncio.sleep(0)
    task.cancel("newest-control")
    await asyncio.sleep(0)
    release.set()

    outcome = await task

    assert outcome.error is None
    assert outcome.cancellation is not None
    assert outcome.cancellation.args == ("newest-control",)


async def test_cancel_during_observer_close_rolls_back_despite_repeated_cancel() -> None:
    harness = _Harness()
    harness.observer.block_close = asyncio.Event()
    harness.operations.block_rollback = asyncio.Event()
    task = asyncio.create_task(
        harness.provisioner().async_install(
            _request(),
            _fleet(),
            harness.progress,
        )
    )
    await harness.observer.close_started.wait()

    task.cancel()
    task.cancel()
    harness.observer.block_close.set()
    await asyncio.wait_for(harness.operations.rollback_started.wait(), timeout=1.0)

    assert task.done() is False
    assert harness.journal.record is not None
    assert harness.journal.record.phase is ProvisioningPhase.ROLLBACK_PENDING
    task.cancel()
    await asyncio.sleep(0)
    assert task.done() is False
    harness.operations.block_rollback.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert harness.journal.record is None
    assert harness.events[-1] == ("shell_close",)


async def test_observer_close_failure_rolls_back_without_leaking_context() -> None:
    harness = _Harness()
    harness.observer.fail_close = True

    with pytest.raises(PanelProvisioningError) as raised:
        await harness.provisioner().async_install(
            _request(),
            _fleet(),
            harness.progress,
        )

    assert raised.value.code == "cleanup_failed"
    assert any(event[0] == "rollback" for event in harness.events)
    assert harness.journal.record is None
    assert _exception_graph(raised.value) == [raised.value]


async def test_synchronous_observer_close_failure_is_redacted_and_rolls_back() -> None:
    harness = _Harness()

    def fail_close() -> None:
        raise RuntimeError("SECRET synchronous close failure")

    harness.observer.async_close = fail_close  # type: ignore[method-assign,assignment]

    with pytest.raises(PanelProvisioningError) as raised:
        await harness.provisioner().async_install(
            _request(),
            _fleet(),
            harness.progress,
        )

    assert raised.value.code == "cleanup_failed"
    assert any(event[0] == "rollback" for event in harness.events)
    assert harness.journal.record is None
    assert harness.events[-1] == ("shell_close",)
    assert _exception_graph(raised.value) == [raised.value]


async def test_rollback_progress_cancellation_cannot_skip_exact_rollback() -> None:
    harness = _Harness()
    harness.operations.fail_at.add("activate")

    async def cancelling_progress(update: ProvisioningProgress) -> None:
        harness.events.append(("progress", update.stage.value))
        if update.stage.value == "rolling_back":
            raise asyncio.CancelledError("SECRET progress cancellation")

    with pytest.raises(PanelProvisioningError) as raised:
        await harness.provisioner().async_install(
            _request(),
            _fleet(),
            cancelling_progress,
        )

    assert raised.value.code == "activation_failed"
    assert any(event[0] == "rollback" for event in harness.events)
    assert harness.journal.record is None
    assert _exception_graph(raised.value) == [raised.value]


async def test_hung_rollback_progress_is_bounded_after_durable_intent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _Harness()
    harness.operations.fail_at.add("activate")
    progress_started = asyncio.Event()
    release_progress = asyncio.Event()
    phase_at_progress: ProvisioningPhase | None = None
    monkeypatch.setattr(
        panel_provisioner,
        "_PROGRESS_TIMEOUT_SECONDS",
        0.01,
        raising=False,
    )

    async def hanging_progress(update: ProvisioningProgress) -> None:
        nonlocal phase_at_progress
        harness.events.append(("progress", update.stage.value))
        if update.stage.value == "rolling_back":
            phase_at_progress = (
                None if harness.journal.record is None else harness.journal.record.phase
            )
            progress_started.set()
            await release_progress.wait()

    task = asyncio.create_task(
        harness.provisioner().async_install(
            _request(),
            _fleet(),
            hanging_progress,
        )
    )
    raised: PanelProvisioningError | None = None
    try:
        await asyncio.wait_for(progress_started.wait(), timeout=1.0)
        await asyncio.sleep(0.05)
        completed_without_external_release = task.done()
    finally:
        release_progress.set()
        try:
            await task
        except PanelProvisioningError as error:
            raised = error

    assert completed_without_external_release
    assert raised is not None
    assert raised.code == "activation_failed"
    assert phase_at_progress is ProvisioningPhase.ROLLBACK_PENDING
    assert any(event[0] == "rollback" for event in harness.events)
    assert harness.journal.record is None
    assert _exception_graph(raised) == [raised]


async def test_cancel_during_final_shell_close_reconnects_and_rolls_back() -> None:
    harness = _Harness()
    harness.shell.block_close = asyncio.Event()
    harness.operations.block_rollback = asyncio.Event()
    task = asyncio.create_task(
        harness.provisioner().async_install(
            _request(),
            _fleet(),
            harness.progress,
        )
    )
    await harness.shell.close_started.wait()

    task.cancel()
    task.cancel()
    harness.shell.block_close.set()
    await asyncio.wait_for(harness.operations.rollback_started.wait(), timeout=1.0)

    assert task.done() is False
    assert harness.journal.record is not None
    assert harness.journal.record.phase is ProvisioningPhase.ROLLBACK_PENDING
    task.cancel()
    harness.operations.block_rollback.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert harness.journal.record is None
    assert harness.shell.close_count == 2
    assert harness.events[-1] == ("shell_close",)


async def test_final_shell_close_failure_does_not_rollback_verified_panel() -> None:
    harness = _Harness()
    harness.shell.fail_close_once = True

    result = await harness.provisioner().async_install(
        _request(),
        _fleet(),
        harness.progress,
    )

    assert result.health == _health()
    assert not any(event[0] == "rollback" for event in harness.events)
    assert harness.journal.record is not None
    assert harness.journal.record.phase is ProvisioningPhase.VERIFYING
    assert harness.shell.close_count == 1
    assert sum(event[0] == "shell_factory" for event in harness.events) == 1
    assert not any(event[0] in {"cleanup_staged", "repair"} for event in harness.events)


async def test_rollback_failure_stays_pending_and_reports_only_fixed_codes() -> None:
    harness = _Harness()
    harness.operations.fail_at.update({"activate", "rollback"})

    with pytest.raises(PanelProvisioningError) as raised:
        await harness.provisioner().async_install(
            _request(),
            _fleet(),
            harness.progress,
        )

    assert raised.value.code == "activation_failed"
    assert harness.journal.record is not None
    assert harness.journal.record.phase is ProvisioningPhase.ROLLBACK_PENDING
    assert (
        "repair",
        _TRANSACTION_ID,
        "activation_failed",
        "rollback_failed",
    ) in harness.events
    assert "SECRET" not in repr(raised.value)
    assert _exception_graph(raised.value) == [raised.value]


def test_panel_snapshot_adapters_round_trip_concrete_panel_ops_dto() -> None:
    stored = _release_link_snapshot()

    operational = panel_snapshot_from_stored(stored)

    assert stored_snapshot_from_panel(operational) == stored
    assert repr(operational) == "PanelSnapshot(<redacted>)"


def test_panel_snapshot_adapters_round_trip_bridge_less_legacy_residue() -> None:
    absent_file = StoredFileSnapshot(content=None, mode=None)
    absent_service = StoredServiceSnapshot(
        unit_file=absent_file,
        enabled=False,
        active=False,
    )
    stored = StoredPanelSnapshot(
        layout=StoredPanelLayout.LEGACY_FIXED,
        active_release_target=None,
        environment_file=absent_file,
        version_file=absent_file,
        bridge_service=absent_service,
        wifi_watchdog_service=StoredServiceSnapshot(
            unit_file=StoredFileSnapshot(
                content=b"orphaned-wifi-unit",
                mode=0o644,
            ),
            enabled=True,
            active=False,
        ),
        bus_watchdog_service=absent_service,
        selected_components=(COMPONENT_WIFI_WATCHDOG,),
    )

    operational = panel_snapshot_from_stored(stored)

    assert stored_snapshot_from_panel(operational) == stored
    assert operational.selected_components == (COMPONENT_WIFI_WATCHDOG,)


async def test_upgrade_failure_rolls_back_exact_distinct_snapshot() -> None:
    harness = _Harness()
    expected = panel_snapshot_from_stored(_release_link_snapshot())
    harness.operations.snapshot = expected
    harness.operations.fail_at.add("activate")

    with pytest.raises(PanelProvisioningError) as raised:
        await harness.provisioner().async_install(
            _request(
                selected_components=(
                    COMPONENT_BRIDGE,
                    COMPONENT_WIFI_WATCHDOG,
                    COMPONENT_BUS_WATCHDOG,
                )
            ),
            _fleet(),
            harness.progress,
        )

    assert raised.value.code == "activation_failed"
    assert harness.journal.created_record is not None
    assert harness.journal.created_record.operation is ProvisioningOperation.UPGRADE
    assert harness.operations.rolled_back_snapshot == expected
    assert harness.journal.record is None


async def test_post_activation_rollback_prunes_candidate_before_journal_clear() -> None:
    harness = _Harness()
    harness.operations.fail_at.add("activate")

    with pytest.raises(PanelProvisioningError) as raised:
        await harness.provisioner().async_install(
            _request(),
            _fleet(),
            harness.progress,
        )

    rollback = next(index for index, event in enumerate(harness.events) if event[0] == "rollback")
    cleanup = next(
        index for index, event in enumerate(harness.events) if event[0] == "cleanup_staged"
    )
    journal_clear = next(
        index
        for index, event in enumerate(harness.events)
        if event[0] == "journal_rollback_complete"
    )
    assert rollback < cleanup < journal_clear
    assert raised.value.code == "activation_failed"
    assert harness.journal.record is None


async def test_post_rollback_candidate_cleanup_failure_stays_durable() -> None:
    harness = _Harness()
    harness.operations.fail_at.update({"activate", "cleanup"})

    with pytest.raises(PanelProvisioningError) as raised:
        await harness.provisioner().async_install(
            _request(),
            _fleet(),
            harness.progress,
        )

    assert raised.value.code == "activation_failed"
    assert harness.journal.record is not None
    assert harness.journal.record.phase is ProvisioningPhase.ROLLBACK_PENDING
    assert (
        "repair",
        _TRANSACTION_ID,
        "activation_failed",
        "rollback_failed",
    ) in harness.events
    assert _exception_graph(raised.value) == [raised.value]


async def test_concrete_panel_preflight_launcher_matches_orchestration_seam() -> None:
    shell = FakeShell()
    await shell.connect()
    launcher: PanelPreflightLauncher = panel_ops.panel_preflight_launcher(
        shell,
        _staged_release(
            _VERSION,
            _TRANSACTION_ID,
            (COMPONENT_BRIDGE, COMPONENT_BUS_WATCHDOG),
        ),
    )
    request = PreflightRequest(
        setup_id=_SETUP_ID,
        panel_nonce="panel-nonce",
        ha_nonce="ha-nonce",
        timeout_seconds=10.0,
    )

    process = await launcher(request.to_json())

    assert process is shell.started_processes[0]
    assert isinstance(process, FakePanelProcess)


async def test_custom_tls_ca_is_owned_by_release_before_preflight() -> None:
    harness = _Harness()

    await harness.provisioner().async_install(
        _request(),
        _fleet(custom_ca=True),
        harness.progress,
    )

    journal = next(
        index for index, event in enumerate(harness.events) if event[0] == "journal_create"
    )
    stage = next(index for index, event in enumerate(harness.events) if event[0] == "stage")
    preflight = next(index for index, event in enumerate(harness.events) if event[0] == "preflight")
    assert journal < stage < preflight
    assert harness.events[stage][-1] is True
    assert not any(event[0] == "stage_ca" for event in harness.events)


async def test_custom_tls_bundle_cannot_silently_omit_public_ca() -> None:
    harness = _Harness()
    harness.omit_custom_ca = True

    with pytest.raises(PanelProvisioningError) as raised:
        await harness.provisioner().async_install(
            _request(),
            _fleet(custom_ca=True),
            harness.progress,
        )

    assert raised.value.code == "release_prepare_failed"
    assert not any(event[0] == "stage" for event in harness.events)
    assert _exception_graph(raised.value) == [raised.value]


@pytest.mark.parametrize(
    ("phase", "expected_operation"),
    [
        (ProvisioningPhase.STAGED, "cleanup_staged"),
        (ProvisioningPhase.ACTIVATION_PENDING, "rollback"),
        (ProvisioningPhase.ROLLBACK_PENDING, "rollback"),
    ],
)
async def test_recovery_cleans_or_rolls_back_interrupted_mutation_phases(
    phase: ProvisioningPhase,
    expected_operation: str,
) -> None:
    harness = _Harness()
    harness.journal.record = _record(phase)

    await harness.provisioner().async_recover(harness.progress)

    assert any(event[0] == expected_operation for event in harness.events)
    assert harness.journal.record is None
    assert harness.events[-1] == ("shell_close",)
    if phase is ProvisioningPhase.STAGED:
        assert not any(event[0] == "rollback" for event in harness.events)


async def test_recovery_rejects_unpinned_shell_before_password_authentication() -> None:
    harness = _Harness()
    harness.journal.record = _record(ProvisioningPhase.ACTIVATION_PENDING)
    harness.shell = _EventShell(harness.events, pinned=None)

    with pytest.raises(PanelProvisioningError) as raised:
        await harness.provisioner().async_recover(harness.progress)

    assert raised.value.code == "recovery_failed"
    assert not any(event[0] == "shell_connect" for event in harness.events)
    assert not any(event[0] == "rollback" for event in harness.events)
    assert harness.journal.record is not None
    assert _exception_graph(raised.value) == [raised.value]


@pytest.mark.parametrize(
    "phase",
    [ProvisioningPhase.ACTIVATED, ProvisioningPhase.VERIFYING],
)
async def test_recovery_subscribes_before_restart_and_leaves_flow_pending(
    phase: ProvisioningPhase,
) -> None:
    harness = _Harness()
    harness.journal.record = _record(phase)

    await harness.provisioner().async_recover(harness.progress)

    positions = {
        name: next(index for index, event in enumerate(harness.events) if event[0] == name)
        for name in (
            "health_subscribe",
            "restart_stopped",
            "health_boundary",
            "restart",
            "health_wait",
        )
    }
    assert (
        positions["health_subscribe"]
        < positions["restart_stopped"]
        < positions["health_boundary"]
        < positions["restart"]
        < positions["health_wait"]
    )
    assert harness.journal.record is not None
    assert harness.journal.record.phase is ProvisioningPhase.PENDING_CONFIG_COMMIT
    assert harness.events[-1] == ("shell_close",)


async def test_recovery_verified_candidate_commits_only_exact_transaction_match() -> None:
    harness = _Harness()
    harness.journal.record = _record(ProvisioningPhase.ACTIVATED)
    harness.lookup = TransactionLookup(
        TransactionLookupState.MATCHED,
        subentry_id="panel-subentry-id",
    )

    await harness.provisioner().async_recover(harness.progress)

    assert ("journal_commit", "panel-subentry-id") in harness.events
    assert harness.journal.record is None


async def test_recovery_verified_candidate_rolls_back_authoritative_abort() -> None:
    harness = _Harness()
    harness.journal.record = _record(ProvisioningPhase.VERIFYING)
    harness.lookup = TransactionLookup(TransactionLookupState.FLOW_ABORTED_OR_ABSENT)

    await harness.provisioner().async_recover(harness.progress)

    health_index = next(
        index for index, event in enumerate(harness.events) if event[0] == "health_wait"
    )
    rollback_index = next(
        index for index, event in enumerate(harness.events) if event[0] == "rollback"
    )
    assert health_index < rollback_index
    assert harness.journal.record is None


async def test_recovery_health_failure_rolls_back_instead_of_leaving_candidate() -> None:
    harness = _Harness()
    harness.journal.record = _record(ProvisioningPhase.ACTIVATED)
    harness.observer.fail_wait = True

    await harness.provisioner().async_recover(harness.progress)

    assert any(event[0] == "rollback" for event in harness.events)
    assert harness.journal.record is None
    assert harness.events[-1] == ("shell_close",)


@pytest.mark.parametrize(
    ("lookup", "expected_phase", "operation"),
    [
        (
            TransactionLookup(
                TransactionLookupState.MATCHED,
                subentry_id="panel-subentry-id",
            ),
            None,
            "journal_commit",
        ),
        (
            TransactionLookup(TransactionLookupState.FLOW_PENDING),
            ProvisioningPhase.PENDING_CONFIG_COMMIT,
            None,
        ),
        (
            TransactionLookup(TransactionLookupState.FLOW_ABORTED_OR_ABSENT),
            None,
            "rollback",
        ),
    ],
)
async def test_pending_commit_recovery_uses_authoritative_tri_state_lookup(
    lookup: TransactionLookup,
    expected_phase: ProvisioningPhase | None,
    operation: str | None,
) -> None:
    harness = _Harness()
    harness.journal.record = _record(ProvisioningPhase.PENDING_CONFIG_COMMIT)
    harness.lookup = lookup

    await harness.provisioner().async_recover(harness.progress)

    if expected_phase is None:
        assert harness.journal.record is None
    else:
        assert harness.journal.record is not None
        assert harness.journal.record.phase is expected_phase
    if operation is not None:
        assert any(event[0] == operation for event in harness.events)
    if lookup.state is TransactionLookupState.FLOW_PENDING:
        assert not any(event[0] == "rollback" for event in harness.events)


@pytest.mark.parametrize(
    ("phase", "terminal_event"),
    [
        (ProvisioningPhase.COMMITTED, "journal_clear_committed"),
        (ProvisioningPhase.ROLLED_BACK, "journal_rollback_complete"),
    ],
)
async def test_terminal_recovery_retries_journal_cleanup(
    phase: ProvisioningPhase,
    terminal_event: str,
) -> None:
    harness = _Harness()
    harness.journal.record = _record(phase)
    harness.lookup = TransactionLookup(
        TransactionLookupState.MATCHED,
        subentry_id="panel-subentry-id",
    )

    await harness.provisioner().async_recover(harness.progress)

    assert any(event[0] == terminal_event for event in harness.events)
    assert harness.journal.record is None
    assert not any(event[0] == "shell_connect" for event in harness.events)


async def test_committed_recovery_needs_no_removed_transaction_marker() -> None:
    harness = _Harness()
    harness.journal.record = _record(ProvisioningPhase.COMMITTED)
    harness.lookup = TransactionLookup(TransactionLookupState.FLOW_ABORTED_OR_ABSENT)

    await harness.provisioner().async_recover(harness.progress)

    assert ("journal_clear_committed",) in harness.events
    assert harness.journal.record is None
    assert not any(event[0] == "transaction_lookup" for event in harness.events)
    assert not any(event[0] == "shell_connect" for event in harness.events)


async def test_pending_and_commit_hooks_preserve_exact_transaction_identity() -> None:
    harness = _Harness()
    harness.journal.record = _record(ProvisioningPhase.VERIFYING)
    provisioner = harness.provisioner()

    await provisioner.async_mark_pending_config_commit(_TRANSACTION_ID)
    assert harness.journal.record is not None
    assert harness.journal.record.phase is ProvisioningPhase.PENDING_CONFIG_COMMIT

    await provisioner.async_complete_config_commit(
        _TRANSACTION_ID,
        subentry_id="panel-subentry-id",
    )
    assert harness.journal.record is None
    assert ("journal_commit", "panel-subentry-id") in harness.events


async def test_pending_commit_hook_settles_write_under_repeated_cancel() -> None:
    harness = _Harness()
    harness.journal.record = _record(ProvisioningPhase.VERIFYING)
    harness.journal.block_pending_commit = asyncio.Event()
    task = asyncio.create_task(
        harness.provisioner().async_mark_pending_config_commit(_TRANSACTION_ID)
    )
    await harness.journal.pending_commit_started.wait()

    task.cancel()
    task.cancel()
    await asyncio.sleep(0)
    assert task.done() is False
    harness.journal.block_pending_commit.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert harness.journal.record is not None
    assert harness.journal.record.phase is ProvisioningPhase.PENDING_CONFIG_COMMIT


async def test_complete_commit_hook_settles_write_under_repeated_cancel() -> None:
    harness = _Harness()
    harness.journal.record = _record(ProvisioningPhase.PENDING_CONFIG_COMMIT)
    harness.journal.block_complete_commit = asyncio.Event()
    task = asyncio.create_task(
        harness.provisioner().async_complete_config_commit(
            _TRANSACTION_ID,
            subentry_id="panel-subentry-id",
        )
    )
    await harness.journal.complete_commit_started.wait()

    task.cancel()
    task.cancel()
    await asyncio.sleep(0)
    assert task.done() is False
    harness.journal.block_complete_commit.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert harness.journal.record is None
    assert ("journal_commit", "panel-subentry-id") in harness.events


async def test_install_never_starts_second_transaction_while_recovery_is_pending() -> None:
    harness = _Harness()
    harness.journal.record = _record(ProvisioningPhase.PENDING_CONFIG_COMMIT)

    with pytest.raises(PanelProvisioningError) as raised:
        await harness.provisioner().async_install(
            _request(),
            _fleet(),
            harness.progress,
        )

    assert raised.value.code == "transaction_in_progress"
    assert not any(event[0] == "identity" for event in harness.events)
