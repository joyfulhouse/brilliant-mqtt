"""Fleet-first config flow and Add panel subentry flow contracts."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import replace
from types import MappingProxyType
from typing import Any, cast
from unittest.mock import AsyncMock, patch
from uuid import UUID

import pytest
import voluptuous as vol
from homeassistant.components.http import ApiConfig
from homeassistant.config_entries import SOURCE_IGNORE, ConfigSubentry
from homeassistant.const import CONF_NAME
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.selector import TextSelector, TextSelectorType
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.brilliant_mqtt import _fleet_lock, config_flow
from custom_components.brilliant_mqtt.broker import BrokerKind, BrokerProfile
from custom_components.brilliant_mqtt.const import (
    COMPONENT_BRIDGE,
    COMPONENT_BUS_WATCHDOG,
    COMPONENT_HUE_CA,
    COMPONENT_VOICE,
    COMPONENT_WIFI_WATCHDOG,
    CONF_BROKER_KIND,
    CONF_COMPONENTS,
    CONF_ENTRY_KIND,
    CONF_FEATURE_OVERRIDES,
    CONF_HA_CONTROL_DOMAINS,
    CONF_HA_CONTROL_ENABLED,
    CONF_HA_CONTROL_LABEL,
    CONF_HOST,
    CONF_HUE_CA_CERT,
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
    CONF_VOICE_HA_HOST,
    CONF_VOICE_WAKE_WORD,
    CONFIG_ENTRY_VERSION,
    DEFAULT_HA_CONTROL_DOMAINS,
    DEFAULT_HA_CONTROL_ENABLED,
    DEFAULT_HA_CONTROL_LABEL,
    DEFAULT_MAX_MIRRORED_ENTITIES,
    DEFAULT_VOICE_WAKE_WORD,
    DOMAIN,
    ENTRY_KIND_FLEET,
    ENTRY_KIND_LEGACY_PENDING_CONSOLIDATION,
    FLEET_SCENE_OWNER_UNASSIGNED,
    FLEET_UNIQUE_ID,
    OPT_AUTO_REPAIR,
    OPT_OFFLINE_GRACE_MINUTES,
    OPT_REPAIR_COOLDOWN_MINUTES,
    SUBENTRY_TYPE_PANEL,
)
from custom_components.brilliant_mqtt.entry_data import (
    EntryDataError,
    FleetConfig,
    PanelConfig,
)
from custom_components.brilliant_mqtt.errors import OperationError, OperationStage
from custom_components.brilliant_mqtt.fleet_manager import FleetManager
from custom_components.brilliant_mqtt.flow import gateway as flow_gateway
from custom_components.brilliant_mqtt.flow import support as flow_support
from custom_components.brilliant_mqtt.flow.schemas import (
    ADVANCED_SECTION,
    BROKER_MENU_OPTIONS,
    SECRET_UNCHANGED,
)
from custom_components.brilliant_mqtt.panel_health import PanelHealthEvidence
from custom_components.brilliant_mqtt.panel_inspection import (
    PanelCompatibilityError,
    PanelFacts,
)
from custom_components.brilliant_mqtt.panel_provisioner import (
    CanonicalPanelData,
    PanelInstallRequest,
    PanelProvisioningError,
    ProvisionedPanel,
    ProvisioningProgress,
)
from custom_components.brilliant_mqtt.provisioning_journal import ProvisioningJournal
from custom_components.brilliant_mqtt.shell import HostIdentity, PanelIdentityError

_PUBLIC_KEY = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIKIykuTed7zNwJwn20eCelcKcHKJ9c/pGFfvulRWazuC"
_FINGERPRINT = "SHA256:JfCon51dCgE/yWGkyroh3Ne+ONLMm6QmHMQnEoPSLx0"
_OTHER_PUBLIC_KEY = (
    "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIG/koBYdTnHujqIpcXlQkQqzGBoZJ6Y4rm22iGIdAu4B"
)
_OTHER_FINGERPRINT = "SHA256:8mIRtm2GlHfcML0pUZInHQk3nT+hlkTq4k2FGR/Y0KM"
_THIRD_PUBLIC_KEY = (
    "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFLOqEG+HLFFAkvglS6WB0dqE/xuFTDmEIFTwEKMj6xI"
)
_THIRD_FINGERPRINT = "SHA256:HvSbMqGcnEU1+Bvnip8Qw0LRDo5dFR0SBrUmf8Haxzs"
_TRANSACTION_ID = UUID("12345678-1234-4abc-8def-1234567890ab")
_SETUP_ID = UUID("87654321-4321-4cba-8fed-ba0987654321")
_BROKER_PASSWORD = "SECRET-mqtt-password"
_ROOT_PASSWORD = "SECRET-root-password"
_CA_PEM = "-----BEGIN CERTIFICATE-----\nPUBLIC-CA\n-----END CERTIFICATE-----"

_BROKER_INPUT: dict[str, object] = {
    CONF_MQTT_HOST: "mqtt.iot.example",
    CONF_MQTT_PORT: 1883,
    CONF_MQTT_USERNAME: "brilliant-fleet",
    CONF_MQTT_PASSWORD: _BROKER_PASSWORD,
    ADVANCED_SECTION: {CONF_MQTT_TLS_ENABLED: False},
}
_TLS_BROKER_INPUT: dict[str, object] = {
    **_BROKER_INPUT,
    CONF_MQTT_PORT: 8883,
    ADVANCED_SECTION: {
        CONF_MQTT_TLS_ENABLED: True,
        CONF_MQTT_TLS_CA: _CA_PEM,
    },
}
_PANEL_INPUT = {
    CONF_HOST: "office.iot.example",
    CONF_ROOT_PASSWORD: _ROOT_PASSWORD,
}


@pytest.fixture(autouse=True)
def _prove_config_entry_persistence(monkeypatch: pytest.MonkeyPatch) -> None:
    """HA's test Store is in-memory; focused fleet tests cover real disk polling."""
    monkeypatch.setattr(
        flow_gateway,
        "_async_wait_config_entry_persisted",
        AsyncMock(),
    )


def _identity(*, other: bool = False) -> HostIdentity:
    return HostIdentity(
        _OTHER_PUBLIC_KEY if other else _PUBLIC_KEY,
        _OTHER_FINGERPRINT if other else _FINGERPRINT,
    )


def _facts(identity: HostIdentity | None = None) -> PanelFacts:
    candidate = identity or _identity()
    return PanelFacts(
        fingerprint=candidate.fingerprint,
        hostname="office-panel",
        model="Brilliant Control Development Board",
        architecture="armv7l",
        firmware="v26.07.15.1",
        python_version="3.10.9",
        init_system="systemd 250",
        available_bytes=1_000_000_000,
        available_memory_bytes=128_000_000,
        installed_agent_version=None,
        active_services=(),
        conflicting_services=(),
    )


def _panel_data(
    request: PanelInstallRequest,
    identity: HostIdentity,
) -> CanonicalPanelData:
    return CanonicalPanelData(
        MappingProxyType(
            {
                CONF_IDENTITY_FINGERPRINT: identity.fingerprint,
                CONF_SSH_HOST_KEY: identity.public_key,
                CONF_HOST: request.host,
                CONF_SSH_USERNAME: request.ssh_username,
                CONF_ROOT_PASSWORD: request.root_password,
                CONF_NAME: request.display_name,
                CONF_PANEL: request.slug,
                CONF_MANAGEMENT_ID: identity.fingerprint,
                CONF_COMPONENTS: {
                    COMPONENT_BRIDGE: True,
                    COMPONENT_WIFI_WATCHDOG: True,
                    COMPONENT_BUS_WATCHDOG: True,
                },
                CONF_FEATURE_OVERRIDES: {},
                CONF_MESH_PRIORITY: request.mesh_priority,
                CONF_PROVISIONING_TRANSACTION_ID: str(_TRANSACTION_ID),
            }
        )
    )


def _provisioned(
    request: PanelInstallRequest,
    identity: HostIdentity,
) -> ProvisionedPanel:
    return ProvisionedPanel(
        identity=identity,
        facts=_facts(identity),
        version="0.7.0",
        health=PanelHealthEvidence(
            panel=request.slug,
            agent_version="0.7.0",
            deployment_id=_TRANSACTION_ID.hex,
            state_topic=f"brilliant/{request.slug}/peripheral/state",
            discovery_topic=f"homeassistant/light/{request.slug}/config",
            device_identifier=f"brilliant_panel_{request.slug}",
        ),
        transaction_id=_TRANSACTION_ID,
        setup_id=_SETUP_ID,
        panel_data=_panel_data(request, identity),
    )


class _FakeValidator:
    def __init__(
        self,
        *,
        error: OperationError | None = None,
        gate: asyncio.Event | None = None,
    ) -> None:
        self.error = error
        self.gate = gate
        self.calls: list[BrokerProfile] = []

    async def async_validate(self, profile: BrokerProfile) -> object:
        self.calls.append(profile)
        if self.gate is not None:
            await self.gate.wait()
        if self.error is not None:
            raise self.error
        return object()


class _FakeProvisioner:
    def __init__(
        self,
        identity: HostIdentity,
        *,
        error: PanelProvisioningError | None = None,
        gate: asyncio.Event | None = None,
        mark_error: Exception | None = None,
        mark_gate: asyncio.Event | None = None,
        recover_error: Exception | None = None,
        recover_gate: asyncio.Event | None = None,
        panel_data_overrides: Mapping[str, object] | None = None,
    ) -> None:
        self.identity = identity
        self.error = error
        self.gate = gate
        self.mark_error = mark_error
        self.mark_gate = mark_gate
        self.recover_error = recover_error
        self.recover_gate = recover_gate
        self.panel_data_overrides = panel_data_overrides
        self.recover_probe: Callable[[], bool] | None = None
        self.recover_calls = 0
        self.recover_probe_results: list[bool] = []
        self.mark_started = asyncio.Event()
        self.recover_started = asyncio.Event()
        self.install_calls: list[tuple[PanelInstallRequest, FleetConfig]] = []
        self.marked_transactions: list[UUID] = []

    async def async_install(
        self,
        request: PanelInstallRequest,
        fleet: FleetConfig,
        progress: Callable[[ProvisioningProgress], Awaitable[None]],
    ) -> ProvisionedPanel:
        del progress
        self.install_calls.append((request, fleet))
        if self.gate is not None:
            await self.gate.wait()
        if self.error is not None:
            raise self.error
        provisioned = _provisioned(request, self.identity)
        if self.panel_data_overrides is None:
            return provisioned
        panel_data = {
            **provisioned.panel_data.as_dict(),
            **self.panel_data_overrides,
        }
        return replace(
            provisioned,
            panel_data=CanonicalPanelData(MappingProxyType(panel_data)),
        )

    async def async_mark_pending_config_commit(self, transaction_id: UUID) -> None:
        self.marked_transactions.append(transaction_id)
        self.mark_started.set()
        if self.mark_gate is not None:
            await self.mark_gate.wait()
        if self.mark_error is not None:
            raise self.mark_error

    async def async_recover(self) -> None:
        self.recover_calls += 1
        self.recover_started.set()
        if self.recover_probe is not None:
            self.recover_probe_results.append(self.recover_probe())
        if self.recover_gate is not None:
            await self.recover_gate.wait()
        if self.recover_error is not None:
            raise self.recover_error


def _schema_keys(result: Mapping[str, Any]) -> set[str]:
    schema = result["data_schema"]
    assert isinstance(schema, vol.Schema)
    return {str(marker) for marker in schema.schema}


def _schema_defaults(result: Mapping[str, Any]) -> dict[str, object]:
    schema = result["data_schema"]
    assert isinstance(schema, vol.Schema)
    return {
        str(marker): marker.default()
        for marker in schema.schema
        if marker.default is not vol.UNDEFINED
    }


def _schema_validator(result: Mapping[str, Any], key: str) -> object:
    schema = result["data_schema"]
    assert isinstance(schema, vol.Schema)
    return next(validator for marker, validator in schema.schema.items() if str(marker) == key)


def _flow_transaction_is_cleared(flow: Any) -> bool:
    return CONF_PROVISIONING_TRANSACTION_ID not in cast(
        Mapping[str, Any],
        flow.context,
    )


@pytest.mark.parametrize(
    ("code", "documentation_slug"),
    (
        ("invalid_broker_profile", "mqtt-broker-profile"),
        ("broker_validation_failed", "mqtt-broker-validation-failed"),
    ),
)
def test_broker_fallback_failure_links_to_mqtt_guide(
    code: str,
    documentation_slug: str,
) -> None:
    placeholders = flow_support._panel_failure(
        code,
        stage="broker_validation",
    ).placeholders()

    assert placeholders == {
        "documentation_slug": documentation_slug,
        "documentation_url": (
            "https://github.com/joyfulhouse/brilliant-mqtt/blob/main/docs/"
            f"install/mqtt-broker.md#{documentation_slug}"
        ),
        "stage": "broker_validation",
    }


def _fleet_data(
    *,
    kind: BrokerKind = BrokerKind.EXISTING_BROKER,
    scene_panel: str = "panel-office",
    next_mesh_priority: int = 2,
) -> dict[str, object]:
    return {
        CONF_ENTRY_KIND: ENTRY_KIND_FLEET,
        CONF_BROKER_KIND: kind.value,
        CONF_MQTT_HOST: "mqtt.iot.example",
        CONF_MQTT_PORT: 1883,
        CONF_MQTT_USERNAME: "brilliant-fleet",
        CONF_MQTT_PASSWORD: _BROKER_PASSWORD,
        CONF_MQTT_TLS_ENABLED: False,
        CONF_NEXT_MESH_PRIORITY: next_mesh_priority,
        CONF_HA_CONTROL_ENABLED: DEFAULT_HA_CONTROL_ENABLED,
        CONF_HA_CONTROL_LABEL: DEFAULT_HA_CONTROL_LABEL,
        CONF_ROOM_OVERRIDES: {},
        CONF_HA_CONTROL_DOMAINS: list(DEFAULT_HA_CONTROL_DOMAINS),
        CONF_MAX_MIRRORED_ENTITIES: DEFAULT_MAX_MIRRORED_ENTITIES,
        CONF_SCENE_PANEL: scene_panel,
        CONF_SCENE_ACTIONS: {},
        CONF_SCHEMA_VERSION: CONFIG_ENTRY_VERSION,
    }


def _subentry(
    *,
    slug: str = "office",
    fingerprint: str = _FINGERPRINT,
    public_key: str = _PUBLIC_KEY,
    priority: int = 1,
    subentry_id: str = "panel-office",
    management_id: str | None = None,
) -> ConfigSubentry:
    return ConfigSubentry(
        data=MappingProxyType(
            {
                CONF_IDENTITY_FINGERPRINT: fingerprint,
                CONF_SSH_HOST_KEY: public_key,
                CONF_HOST: f"{slug}.iot.example",
                CONF_SSH_USERNAME: "root",
                CONF_ROOT_PASSWORD: f"{slug}-root-password",
                CONF_NAME: slug.title(),
                CONF_PANEL: slug,
                CONF_MANAGEMENT_ID: management_id or fingerprint,
                CONF_COMPONENTS: {
                    COMPONENT_BRIDGE: True,
                    COMPONENT_WIFI_WATCHDOG: True,
                    COMPONENT_BUS_WATCHDOG: True,
                },
                CONF_FEATURE_OVERRIDES: {},
                CONF_MESH_PRIORITY: priority,
            }
        ),
        subentry_type=SUBENTRY_TYPE_PANEL,
        title=slug.title(),
        unique_id=fingerprint,
        subentry_id=subentry_id,
    )


def _fleet_entry(
    hass: HomeAssistant,
    *panels: ConfigSubentry,
    kind: BrokerKind = BrokerKind.EXISTING_BROKER,
) -> MockConfigEntry:
    scene_panel = panels[0].subentry_id if panels else FLEET_SCENE_OWNER_UNASSIGNED
    entry = MockConfigEntry(
        domain=DOMAIN,
        title="Brilliant MQTT",
        unique_id=FLEET_UNIQUE_ID,
        version=CONFIG_ENTRY_VERSION,
        data=_fleet_data(
            kind=kind,
            scene_panel=scene_panel,
            next_mesh_priority=len(panels) + 1,
        ),
        subentries_data=[panel.as_dict() for panel in panels],
    )
    entry.add_to_hass(hass)
    return entry


def _legacy_entry(hass: HomeAssistant) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        title="Legacy Brilliant panel",
        version=CONFIG_ENTRY_VERSION,
        data={
            **_fleet_data(),
            CONF_ENTRY_KIND: ENTRY_KIND_LEGACY_PENDING_CONSOLIDATION,
            CONF_HOST: "office.iot.example",
            CONF_ROOT_PASSWORD: _ROOT_PASSWORD,
            CONF_SSH_HOST_KEY: _PUBLIC_KEY,
            CONF_PANEL: "office",
            CONF_MESH_PRIORITY: 1,
            CONF_SCENE_PANEL: "office",
            CONF_COMPONENTS: {
                COMPONENT_BRIDGE: True,
                COMPONENT_WIFI_WATCHDOG: True,
                COMPONENT_BUS_WATCHDOG: True,
            },
        },
    )
    entry.add_to_hass(hass)
    return entry


async def _start_broker_form(
    hass: HomeAssistant,
    kind: BrokerKind,
) -> dict[str, Any]:
    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": "user"},
    )
    assert result["type"] is FlowResultType.MENU
    return cast(
        dict[str, Any],
        await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {"next_step_id": kind.value},
        ),
    )


async def _start_fleet_broker_options(
    hass: HomeAssistant,
    entry: MockConfigEntry,
    kind: BrokerKind = BrokerKind.EXISTING_BROKER,
) -> dict[str, Any]:
    result = await hass.config_entries.options.async_init(entry.entry_id)
    result = await hass.config_entries.options.async_configure(
        result["flow_id"],
        {"next_step_id": "broker"},
    )
    assert result["type"] is FlowResultType.MENU
    assert tuple(cast(Iterable[str], result["menu_options"])) == BROKER_MENU_OPTIONS
    return cast(
        dict[str, Any],
        await hass.config_entries.options.async_configure(
            result["flow_id"],
            {"next_step_id": kind.value},
        ),
    )


async def _prepare_fleet_broker_commit(
    hass: HomeAssistant,
    entry: MockConfigEntry,
    *,
    kind: BrokerKind = BrokerKind.EXISTING_BROKER,
    broker_input: dict[str, object] | None = None,
) -> str:
    """Leave one validated fleet broker options flow at its commit boundary."""
    form = await _start_fleet_broker_options(hass, entry, kind)
    validator = _FakeValidator(gate=asyncio.Event())
    submitted = broker_input or {
        **_BROKER_INPUT,
        CONF_MQTT_HOST: "replacement-broker.iot.example",
        CONF_MQTT_PASSWORD: SECRET_UNCHANGED,
    }
    with patch.object(flow_gateway, "_broker_validator", return_value=validator):
        progress = await hass.config_entries.options.async_configure(
            form["flow_id"],
            submitted,
        )
    assert progress["type"] is FlowResultType.SHOW_PROGRESS
    assert validator.gate is not None
    validator.gate.set()
    await _wait_progress_done(hass.config_entries.options, form["flow_id"])
    return cast(str, form["flow_id"])


async def _start_fleet_options_step(
    hass: HomeAssistant,
    entry: MockConfigEntry,
    step_id: str,
) -> dict[str, Any]:
    result = await hass.config_entries.options.async_init(entry.entry_id)
    assert result["type"] is FlowResultType.MENU
    return cast(
        dict[str, Any],
        await hass.config_entries.options.async_configure(
            result["flow_id"],
            {"next_step_id": step_id},
        ),
    )


async def _start_panel_reconfigure_step(
    hass: HomeAssistant,
    entry: MockConfigEntry,
    panel: ConfigSubentry,
    step_id: str,
) -> dict[str, Any]:
    result = await hass.config_entries.subentries.async_init(
        (entry.entry_id, SUBENTRY_TYPE_PANEL),
        context={
            "source": "reconfigure",
            "subentry_id": panel.subentry_id,
        },
    )
    assert result["type"] is FlowResultType.MENU
    return cast(
        dict[str, Any],
        await hass.config_entries.subentries.async_configure(
            result["flow_id"],
            {"next_step_id": step_id},
        ),
    )


async def _submit_broker_create(
    hass: HomeAssistant,
    result: Mapping[str, Any],
    validator: _FakeValidator,
    *,
    broker_input: dict[str, object] | None = None,
) -> dict[str, Any]:
    if validator.gate is None:
        validator.gate = asyncio.Event()
    with patch.object(flow_gateway, "_broker_validator", return_value=validator):
        progress = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            broker_input or _BROKER_INPUT,
        )
    assert progress["type"] is FlowResultType.SHOW_PROGRESS
    validator.gate.set()
    return await _drain_progress(hass.config_entries.flow, result["flow_id"])


async def _submit_broker(
    hass: HomeAssistant,
    result: Mapping[str, Any],
    validator: _FakeValidator,
    *,
    broker_input: dict[str, object] | None = None,
) -> dict[str, Any]:
    """Create the durable fleet and follow HA's chained first-panel subentry flow."""
    created = await _submit_broker_create(
        hass,
        result,
        validator,
        broker_input=broker_input,
    )
    if created["type"] is not FlowResultType.CREATE_ENTRY:
        return created
    _flow_type, flow_id = created["next_flow"]
    return cast(
        dict[str, Any],
        hass.config_entries.subentries.async_get(flow_id),
    )


async def _drain_progress(manager: Any, flow_id: str) -> dict[str, Any]:
    """Finish one HA progress callback without draining unrelated background work."""
    await _wait_progress_done(manager, flow_id)
    return cast(dict[str, Any], await manager.async_configure(flow_id))


async def _wait_progress_done(manager: Any, flow_id: str) -> None:
    """Wait for HA's one-shot progress callback, leaving its next step unconsumed."""
    flow = manager._progress[flow_id]
    task = flow.async_get_progress_task()
    assert task is not None
    done, _pending = await asyncio.wait({task}, timeout=1)
    assert task in done

    async with asyncio.timeout(1):
        while True:
            flow = manager._progress.get(flow_id)
            assert flow is not None
            current = flow.cur_step
            if current is not None and current["type"] is FlowResultType.SHOW_PROGRESS_DONE:
                break
            await asyncio.sleep(0)


async def _start_initial_confirm(
    hass: HomeAssistant,
    *,
    identity: HostIdentity | None = None,
) -> dict[str, Any]:
    candidate = identity or _identity()
    broker = await _start_broker_form(hass, BrokerKind.EXISTING_BROKER)
    panel_connect = await _submit_broker(hass, broker, _FakeValidator())
    assert panel_connect["step_id"] == "panel_connect"
    with (
        patch.object(
            flow_gateway,
            "async_fetch_host_identity",
            return_value=candidate,
        ),
        patch.object(
            flow_gateway,
            "_async_inspect_candidate",
            return_value=_facts(candidate),
        ),
    ):
        return cast(
            dict[str, Any],
            await hass.config_entries.subentries.async_configure(
                panel_connect["flow_id"],
                _PANEL_INPUT,
            ),
        )


async def _start_subentry_confirm(
    hass: HomeAssistant,
    entry: MockConfigEntry,
    *,
    identity: HostIdentity | None = None,
) -> dict[str, Any]:
    candidate = identity or _identity(other=True)
    result = await hass.config_entries.subentries.async_init(
        (entry.entry_id, SUBENTRY_TYPE_PANEL),
        context={"source": "user"},
    )
    with (
        patch.object(
            flow_gateway,
            "async_fetch_host_identity",
            return_value=candidate,
        ),
        patch.object(
            flow_gateway,
            "_async_inspect_candidate",
            return_value=_facts(candidate),
        ),
    ):
        return cast(
            dict[str, Any],
            await hass.config_entries.subentries.async_configure(
                result["flow_id"],
                _PANEL_INPUT,
            ),
        )


async def test_user_menu_offers_recommended_and_existing_brokers_equally(
    hass: HomeAssistant,
) -> None:
    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": "user"},
    )

    assert result["type"] is FlowResultType.MENU
    assert result["step_id"] == "user"
    assert tuple(cast(Iterable[str], result["menu_options"])) == BROKER_MENU_OPTIONS


async def test_official_broker_prefills_editable_local_host_and_port(
    hass: HomeAssistant,
) -> None:
    initial = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": "user"},
    )
    assert initial["type"] is FlowResultType.MENU
    hass.config.api = ApiConfig(
        local_ip="192.0.2.10",
        host="0.0.0.0",
        port=8123,
        use_ssl=False,
    )
    result = cast(
        dict[str, Any],
        await hass.config_entries.flow.async_configure(
            initial["flow_id"],
            {"next_step_id": BrokerKind.OFFICIAL_MOSQUITTO.value},
        ),
    )

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "broker"
    assert _schema_keys(result) == {
        CONF_HA_CONTROL_ENABLED,
        CONF_MQTT_HOST,
        CONF_MQTT_PORT,
        CONF_MQTT_USERNAME,
        CONF_MQTT_PASSWORD,
        ADVANCED_SECTION,
    }
    defaults = _schema_defaults(result)
    assert defaults[CONF_HA_CONTROL_ENABLED] is False
    assert defaults[CONF_MQTT_HOST] == "192.0.2.10"
    assert defaults[CONF_MQTT_PORT] == 1883
    validated = result["data_schema"](
        {
            **_BROKER_INPUT,
            CONF_MQTT_HOST: "broker-for-panels.example",
            CONF_MQTT_PORT: 2883,
        }
    )
    assert validated[CONF_MQTT_HOST] == "broker-for-panels.example"
    assert validated[CONF_MQTT_PORT] == 2883


async def test_existing_broker_has_normal_fields_and_shared_advanced_tls(
    hass: HomeAssistant,
) -> None:
    result = await _start_broker_form(hass, BrokerKind.EXISTING_BROKER)

    assert _schema_keys(result) == {
        CONF_HA_CONTROL_ENABLED,
        CONF_MQTT_HOST,
        CONF_MQTT_PORT,
        CONF_MQTT_USERNAME,
        CONF_MQTT_PASSWORD,
        ADVANCED_SECTION,
    }
    validated = result["data_schema"](_TLS_BROKER_INPUT)
    assert validated[ADVANCED_SECTION] == {
        CONF_MQTT_TLS_ENABLED: True,
        CONF_MQTT_TLS_CA: _CA_PEM,
    }
    assert result["description_placeholders"]["documentation_url"].endswith(
        "/docs/install/mqtt-broker.md#mqtt-validation"
    )


@pytest.mark.parametrize(
    "kind",
    (BrokerKind.OFFICIAL_MOSQUITTO, BrokerKind.EXISTING_BROKER),
)
async def test_both_broker_choices_use_the_same_normalized_validator(
    hass: HomeAssistant,
    kind: BrokerKind,
) -> None:
    broker = await _start_broker_form(hass, kind)
    validator = _FakeValidator()

    panel_connect = await _submit_broker(
        hass,
        broker,
        validator,
        broker_input=_TLS_BROKER_INPUT,
    )

    assert panel_connect["step_id"] == "panel_connect"
    assert len(validator.calls) == 1
    profile = validator.calls[0]
    assert profile.kind is kind
    assert profile.host == "mqtt.iot.example"
    assert profile.port == 8883
    assert profile.tls_enabled is True
    assert profile.has_custom_ca is True


@pytest.mark.parametrize(
    ("code", "stage", "docs_slug"),
    (
        (
            "ha_mqtt_unavailable",
            OperationStage.HA_MQTT_READY,
            "mqtt-validation",
        ),
        (
            "unsupported_discovery_prefix",
            OperationStage.HA_MQTT_READY,
            "mqtt-discovery-prefix",
        ),
    ),
)
async def test_broker_readiness_failure_preserves_only_nonsecret_input(
    hass: HomeAssistant,
    code: str,
    stage: OperationStage,
    docs_slug: str,
) -> None:
    broker = await _start_broker_form(hass, BrokerKind.EXISTING_BROKER)
    validator = _FakeValidator(error=OperationError.for_code(stage, code))

    result = await _submit_broker(
        hass,
        broker,
        validator,
        broker_input={
            **_BROKER_INPUT,
            CONF_HA_CONTROL_ENABLED: True,
        },
    )

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "broker"
    assert result["errors"] == {"base": code}
    assert result["description_placeholders"]["documentation_slug"] == docs_slug
    assert result["description_placeholders"]["documentation_url"].endswith(
        f"/docs/install/mqtt-broker.md#{docs_slug}"
    )
    defaults = _schema_defaults(result)
    assert defaults[CONF_MQTT_HOST] == "mqtt.iot.example"
    assert defaults[CONF_MQTT_PORT] == 1883
    assert defaults[CONF_MQTT_USERNAME] == "brilliant-fleet"
    assert defaults[CONF_HA_CONTROL_ENABLED] is True
    assert _BROKER_PASSWORD not in repr(result)
    assert not hass.config_entries.async_entries(DOMAIN)


async def test_broker_field_error_preserves_only_valid_nonsecret_input(
    hass: HomeAssistant,
) -> None:
    broker = await _start_broker_form(hass, BrokerKind.EXISTING_BROKER)
    submitted_password = "SECRET-invalid-form-broker-password\n"

    result = await hass.config_entries.flow.async_configure(
        broker["flow_id"],
        {
            **_TLS_BROKER_INPUT,
            CONF_MQTT_PASSWORD: submitted_password,
            CONF_HA_CONTROL_ENABLED: True,
        },
    )

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "broker"
    assert result["errors"] == {CONF_MQTT_PASSWORD: "invalid_value"}
    defaults = _schema_defaults(result)
    assert defaults[CONF_MQTT_HOST] == "mqtt.iot.example"
    assert defaults[CONF_MQTT_PORT] == 8883
    assert defaults[CONF_MQTT_USERNAME] == "brilliant-fleet"
    assert defaults[CONF_HA_CONTROL_ENABLED] is True
    assert defaults[ADVANCED_SECTION] == {
        CONF_MQTT_TLS_ENABLED: True,
        CONF_MQTT_TLS_CA: _CA_PEM,
    }
    assert submitted_password not in repr(result)
    assert CONF_MQTT_PASSWORD not in defaults
    assert not hass.config_entries.async_entries(DOMAIN)


async def test_broker_field_error_clears_stale_validation_failure_help(
    hass: HomeAssistant,
) -> None:
    broker = await _start_broker_form(hass, BrokerKind.EXISTING_BROKER)
    failure = await _submit_broker(
        hass,
        broker,
        _FakeValidator(
            error=OperationError.for_code(
                OperationStage.HA_MQTT_READY,
                "unsupported_discovery_prefix",
            )
        ),
    )
    assert failure["errors"] == {"base": "unsupported_discovery_prefix"}

    result = await hass.config_entries.flow.async_configure(
        failure["flow_id"],
        {
            **_BROKER_INPUT,
            CONF_MQTT_PASSWORD: "SECRET-invalid-retry-password\n",
        },
    )

    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {CONF_MQTT_PASSWORD: "invalid_value"}
    assert result["description_placeholders"] == {
        "documentation_url": (
            "https://github.com/joyfulhouse/brilliant-mqtt/blob/main/"
            "docs/install/mqtt-broker.md#mqtt-validation"
        )
    }


async def test_broker_progress_task_cannot_be_double_submitted(
    hass: HomeAssistant,
) -> None:
    broker = await _start_broker_form(hass, BrokerKind.EXISTING_BROKER)
    gate = asyncio.Event()
    validator = _FakeValidator(gate=gate)
    with patch.object(flow_gateway, "_broker_validator", return_value=validator):
        first = await hass.config_entries.flow.async_configure(
            broker["flow_id"],
            _BROKER_INPUT,
        )
        second = await hass.config_entries.flow.async_configure(
            broker["flow_id"],
            _BROKER_INPUT,
        )

    assert first["type"] is FlowResultType.SHOW_PROGRESS
    assert second["type"] is FlowResultType.SHOW_PROGRESS
    assert len(validator.calls) == 1
    gate.set()
    result = await _drain_progress(hass.config_entries.flow, broker["flow_id"])
    assert result["type"] is FlowResultType.CREATE_ENTRY
    _flow_type, flow_id = result["next_flow"]
    assert hass.config_entries.subentries.async_get(flow_id)["step_id"] == "panel_connect"


async def test_broker_validation_creates_durable_empty_fleet_and_chains_first_panel(
    hass: HomeAssistant,
) -> None:
    """The fleet owner exists before the first panel flow can touch a controller."""
    broker = await _start_broker_form(hass, BrokerKind.EXISTING_BROKER)
    identity_fetch = AsyncMock()
    persistence_calls: list[str] = []

    async def setup_entry(_hass: HomeAssistant, entry: Any) -> bool:
        entry.runtime_data = AsyncMock()
        return True

    async def prove_persistence(
        _hass: HomeAssistant,
        entry: Any,
        *,
        subentry_id: str | None = None,
    ) -> None:
        assert subentry_id is None
        assert entry.subentries == {}
        persistence_calls.append(entry.entry_id)

    with (
        patch(
            "custom_components.brilliant_mqtt.async_setup_entry",
            new=AsyncMock(side_effect=setup_entry),
        ),
        patch.object(
            flow_gateway,
            "_async_wait_config_entry_persisted",
            side_effect=prove_persistence,
        ),
        patch.object(
            flow_gateway,
            "async_fetch_host_identity",
            identity_fetch,
        ),
    ):
        result = await _submit_broker_create(hass, broker, _FakeValidator())

    assert result["type"] is FlowResultType.CREATE_ENTRY
    entry = result["result"]
    assert entry.unique_id == FLEET_UNIQUE_ID
    assert entry.data[CONF_ENTRY_KIND] == ENTRY_KIND_FLEET
    assert entry.data[CONF_NEXT_MESH_PRIORITY] == 1
    assert entry.data[CONF_HA_CONTROL_ENABLED] is False
    assert entry.data[CONF_SCENE_PANEL] == "__unassigned__"
    assert entry.subentries == {}
    assert persistence_calls == [entry.entry_id]
    assert result["next_flow"][0].value == "config_subentries_flow"
    subentry_flow = hass.config_entries.subentries.async_get(result["next_flow"][1])
    assert subentry_flow["step_id"] == "panel_connect"
    identity_fetch.assert_not_awaited()


async def test_initial_broker_form_enables_ha_control_before_first_panel(
    hass: HomeAssistant,
) -> None:
    """The broker page is the only pre-panel decision point for HA control."""
    broker = await _start_broker_form(hass, BrokerKind.EXISTING_BROKER)

    created = await _submit_broker_create(
        hass,
        broker,
        _FakeValidator(),
        broker_input={
            **_BROKER_INPUT,
            CONF_HA_CONTROL_ENABLED: True,
        },
    )

    assert created["type"] is FlowResultType.CREATE_ENTRY
    entry = created["result"]
    assert entry.data[CONF_HA_CONTROL_ENABLED] is True
    _flow_type, flow_id = created["next_flow"]
    assert hass.config_entries.subentries.async_get(flow_id)["step_id"] == "panel_connect"


async def test_created_fleet_mutation_blocks_persistence_and_first_panel_chain(
    hass: HomeAssistant,
) -> None:
    """A create/callback race cannot turn a mutated owner into onboarding authority."""
    broker = await _start_broker_form(hass, BrokerKind.EXISTING_BROKER)
    original = config_flow.BrilliantMqttConfigFlow.async_on_create_entry
    persisted = AsyncMock()
    start_subentry = AsyncMock(
        return_value={
            "type": FlowResultType.ABORT,
            "reason": "test-blocked",
        }
    )

    async def mutate_before_callback(
        flow: config_flow.BrilliantMqttConfigFlow,
        result: Any,
    ) -> Any:
        entry = result["result"]
        assert hass.config_entries.async_update_entry(
            entry,
            data={**entry.data, CONF_NEXT_MESH_PRIORITY: 2},
        )
        return await original(flow, result)

    with (
        patch.object(
            config_flow.BrilliantMqttConfigFlow,
            "async_on_create_entry",
            new=mutate_before_callback,
        ),
        patch.object(
            flow_gateway,
            "_async_wait_config_entry_persisted",
            persisted,
        ),
        patch.object(
            hass.config_entries.subentries,
            "async_init",
            start_subentry,
        ),
    ):
        result = await _submit_broker_create(hass, broker, _FakeValidator())

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert "next_flow" not in result
    persisted.assert_not_awaited()
    start_subentry.assert_not_awaited()
    entry = result["result"]
    issue = ir.async_get(hass).async_get_issue(
        DOMAIN,
        f"fleet_storage_{entry.entry_id}",
    )
    assert issue is not None
    assert issue.translation_placeholders == {
        "panel": "Brilliant MQTT fleet",
        "reason": (
            "Home Assistant could not confirm the Brilliant MQTT fleet in durable storage. "
            "Retry Add panel after storage is available."
        ),
    }


async def test_unproven_fleet_storage_keeps_empty_entry_and_one_redacted_repair(
    hass: HomeAssistant,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A failed durability proof never starts panel work or exposes raw disk errors."""
    broker = await _start_broker_form(hass, BrokerKind.EXISTING_BROKER)
    secret = "SECRET-storage-backend-detail"
    start_subentry = AsyncMock()

    with (
        patch.object(
            flow_gateway,
            "_async_wait_config_entry_persisted",
            side_effect=OSError(secret),
        ),
        patch.object(
            hass.config_entries.subentries,
            "async_init",
            start_subentry,
        ),
    ):
        result = await _submit_broker_create(hass, broker, _FakeValidator())

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert "next_flow" not in result
    entry = result["result"]
    assert entry.subentries == {}
    start_subentry.assert_not_awaited()
    issue = ir.async_get(hass).async_get_issue(
        DOMAIN,
        f"fleet_storage_{entry.entry_id}",
    )
    assert issue is not None
    assert issue.translation_key == "needs_attention"
    assert issue.translation_placeholders == {
        "panel": "Brilliant MQTT fleet",
        "reason": (
            "Home Assistant could not confirm the Brilliant MQTT fleet in durable storage. "
            "Retry Add panel after storage is available."
        ),
    }
    assert secret not in repr(issue)
    assert secret not in caplog.text

    confirm = await _start_subentry_confirm(hass, entry)
    provisioner = _FakeProvisioner(_identity(), gate=asyncio.Event())
    with patch.object(
        flow_gateway,
        "_get_panel_provisioner",
        return_value=provisioner,
    ):
        progress = await hass.config_entries.subentries.async_configure(
            confirm["flow_id"],
            {CONF_NAME: "Office"},
        )
        assert progress["type"] is FlowResultType.SHOW_PROGRESS
        assert (
            ir.async_get(hass).async_get_issue(
                DOMAIN,
                f"fleet_storage_{entry.entry_id}",
            )
            is None
        )
        hass.config_entries.subentries.async_abort(confirm["flow_id"])
        await hass.async_block_till_done()


async def test_panel_confirm_storage_failure_is_fixed_redacted_and_write_free(
    hass: HomeAssistant,
) -> None:
    """Exact parent persistence is required before provisioner or panel writes."""
    entry = _fleet_entry(hass)
    confirm = await _start_subentry_confirm(hass, entry)
    get_provisioner = AsyncMock()
    secret = "SECRET-storage-read-error"

    with (
        patch.object(
            flow_gateway,
            "_async_wait_config_entry_persisted",
            side_effect=OSError(secret),
        ),
        patch.object(
            flow_gateway,
            "_get_panel_provisioner",
            get_provisioner,
        ),
    ):
        result = await hass.config_entries.subentries.async_configure(
            confirm["flow_id"],
            {CONF_NAME: "Office"},
        )

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "panel_confirm"
    assert result["errors"] == {"base": "config_entry_storage_unavailable"}
    placeholders = result["description_placeholders"]
    assert placeholders is not None
    assert placeholders["stage"] == "storage"
    assert secret not in repr(result)
    assert entry.subentries == {}
    get_provisioner.assert_not_called()


async def test_panel_connect_fetches_identity_before_password_authentication(
    hass: HomeAssistant,
) -> None:
    broker = await _start_broker_form(hass, BrokerKind.EXISTING_BROKER)
    panel_connect = await _submit_broker(hass, broker, _FakeValidator())
    events: list[str] = []
    shell: Any = AsyncMock()
    shell.pinned_host_key.return_value = _PUBLIC_KEY

    async def fetch(host: str, port: int = 22) -> HostIdentity:
        del host, port
        events.append("identity")
        return _identity()

    def shell_factory(host: str, password: str, pinned_key: str) -> Any:
        assert host == "office.iot.example"
        assert password == _ROOT_PASSWORD
        assert pinned_key == _PUBLIC_KEY
        events.append("authenticated_shell")
        return shell

    async def inspect(candidate_shell: Any, identity: HostIdentity) -> PanelFacts:
        assert candidate_shell is shell
        assert identity == _identity()
        events.append("inspection")
        return _facts(identity)

    with (
        patch.object(flow_gateway, "async_fetch_host_identity", side_effect=fetch),
        patch.object(flow_gateway, "FleetAsyncsshShell", side_effect=shell_factory),
        patch.object(flow_gateway, "async_inspect_panel", side_effect=inspect),
    ):
        result = await hass.config_entries.subentries.async_configure(
            panel_connect["flow_id"],
            _PANEL_INPUT,
        )

    assert events == ["identity", "authenticated_shell", "inspection"]
    shell.connect.assert_awaited_once_with()
    shell.close.assert_awaited_once_with()
    assert result["step_id"] == "panel_confirm"


async def test_panel_inspection_preserves_cancellation_while_closing(
    hass: HomeAssistant,
) -> None:
    shell: Any = AsyncMock()
    close_started = asyncio.Event()
    release_close = asyncio.Event()
    close_finished = asyncio.Event()

    async def close() -> None:
        close_started.set()
        await release_close.wait()
        close_finished.set()

    shell.close.side_effect = close
    with (
        patch.object(flow_gateway, "FleetAsyncsshShell", return_value=shell),
        patch.object(
            flow_gateway,
            "async_inspect_panel",
            side_effect=OSError("inspection failed"),
        ),
    ):
        task = asyncio.create_task(
            flow_gateway._async_inspect_candidate(
                hass,
                "office.iot.example",
                _ROOT_PASSWORD,
                _identity(),
            )
        )
        await asyncio.wait_for(close_started.wait(), timeout=1)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        assert not close_finished.is_set()
        release_close.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert close_finished.is_set()


async def test_panel_confirm_shows_allowlisted_facts_and_only_name_is_editable(
    hass: HomeAssistant,
) -> None:
    result = await _start_initial_confirm(hass)

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "panel_confirm"
    assert _schema_keys(result) == {CONF_NAME}
    assert _schema_defaults(result)[CONF_NAME] == "Office Panel"
    assert result["description_placeholders"] == {
        "fingerprint": _FINGERPRINT,
        "hostname": "office-panel",
        "model": "Brilliant Control Development Board",
        "architecture": "armv7l",
        "firmware": "v26.07.15.1",
        "python_version": "3.10.9",
        "init_system": "systemd 250",
        "available_bytes": "1000000000",
        "available_memory_bytes": "128000000",
        "installed_agent_version": "not_installed",
        "active_services": "none",
        "conflicting_services": "none",
        "documentation_url": (
            "https://github.com/joyfulhouse/brilliant-mqtt/blob/main/"
            "docs/ha-integration.md#panel-onboarding-errors"
        ),
    }
    assert _ROOT_PASSWORD not in repr(result)
    assert _BROKER_PASSWORD not in repr(result)


@pytest.mark.parametrize(
    "panel_override",
    (
        {
            CONF_COMPONENTS: {
                COMPONENT_BRIDGE: True,
                COMPONENT_WIFI_WATCHDOG: False,
                COMPONENT_BUS_WATCHDOG: True,
            }
        },
        {CONF_FEATURE_OVERRIDES: {"unexpected": True}},
    ),
    ids=("components", "feature-overrides"),
)
def test_canonical_handoff_rejects_component_or_override_mismatch(
    panel_override: dict[str, object],
) -> None:
    identity = _identity()
    request = PanelInstallRequest(
        host="office.iot.example",
        ssh_username="root",
        root_password=_ROOT_PASSWORD,
        display_name="Office",
        slug="office",
        mesh_priority=1,
        selected_components=(
            COMPONENT_BRIDGE,
            COMPONENT_WIFI_WATCHDOG,
            COMPONENT_BUS_WATCHDOG,
        ),
        feature_overrides=MappingProxyType({}),
    )
    provisioned = _provisioned(request, identity)
    assert flow_support._provisioned_matches_request(
        provisioned,
        request,
        identity,
    )
    mismatched_data = {**provisioned.panel_data.as_dict(), **panel_override}
    mismatched = replace(
        provisioned,
        panel_data=CanonicalPanelData(MappingProxyType(mismatched_data)),
    )

    assert not flow_support._provisioned_matches_request(
        mismatched,
        request,
        identity,
    )


async def test_panel_connect_error_is_redacted_and_creates_nothing(
    hass: HomeAssistant,
) -> None:
    broker = await _start_broker_form(hass, BrokerKind.EXISTING_BROKER)
    panel_connect = await _submit_broker(hass, broker, _FakeValidator())

    with patch.object(
        flow_gateway,
        "async_fetch_host_identity",
        side_effect=OSError(_ROOT_PASSWORD),
    ):
        result = await hass.config_entries.subentries.async_configure(
            panel_connect["flow_id"],
            _PANEL_INPUT,
        )

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "panel_connect"
    assert result["errors"] == {"base": "cannot_connect"}
    assert _ROOT_PASSWORD not in repr(result)
    assert _BROKER_PASSWORD not in repr(result)
    entries = hass.config_entries.async_entries(DOMAIN)
    assert len(entries) == 1
    assert entries[0].subentries == {}


async def test_panel_field_error_preserves_only_valid_host(
    hass: HomeAssistant,
) -> None:
    broker = await _start_broker_form(hass, BrokerKind.EXISTING_BROKER)
    panel_connect = await _submit_broker(hass, broker, _FakeValidator())
    submitted_password = "SECRET-invalid-panel-password\n"

    result = await hass.config_entries.subentries.async_configure(
        panel_connect["flow_id"],
        {
            CONF_HOST: "office.iot.example",
            CONF_ROOT_PASSWORD: submitted_password,
        },
    )

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "panel_connect"
    assert result["errors"] == {CONF_ROOT_PASSWORD: "invalid_value"}
    assert _schema_defaults(result) == {CONF_HOST: "office.iot.example"}
    assert submitted_password not in repr(result)
    assert CONF_ROOT_PASSWORD not in _schema_defaults(result)
    entries = hass.config_entries.async_entries(DOMAIN)
    assert len(entries) == 1
    assert entries[0].subentries == {}


async def test_panel_identity_error_uses_translated_stable_code(
    hass: HomeAssistant,
) -> None:
    """Identity-first connection failures retain their actionable code."""
    broker = await _start_broker_form(hass, BrokerKind.EXISTING_BROKER)
    panel_connect = await _submit_broker(hass, broker, _FakeValidator())

    with patch.object(
        flow_gateway,
        "async_fetch_host_identity",
        side_effect=PanelIdentityError("host_unreachable"),
    ):
        result = await hass.config_entries.subentries.async_configure(
            panel_connect["flow_id"],
            _PANEL_INPUT,
        )

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "panel_connect"
    assert result["errors"] == {"base": "host_unreachable"}
    placeholders = result["description_placeholders"]
    assert placeholders is not None
    assert placeholders["documentation_slug"] == "panel-host-unreachable"
    assert placeholders["documentation_url"].endswith(
        "/docs/ha-integration.md#panel-onboarding-errors"
    )
    assert _ROOT_PASSWORD not in repr(result)


async def test_panel_compatibility_error_uses_stable_code_without_secret(
    hass: HomeAssistant,
) -> None:
    broker = await _start_broker_form(hass, BrokerKind.EXISTING_BROKER)
    panel_connect = await _submit_broker(hass, broker, _FakeValidator())

    with (
        patch.object(
            flow_gateway,
            "async_fetch_host_identity",
            return_value=_identity(),
        ),
        patch.object(
            flow_gateway,
            "_async_inspect_candidate",
            side_effect=PanelCompatibilityError("insufficient_memory"),
        ),
    ):
        result = await hass.config_entries.subentries.async_configure(
            panel_connect["flow_id"],
            _PANEL_INPUT,
        )

    assert result["errors"] == {"base": "insufficient_memory"}
    placeholders = result["description_placeholders"]
    assert placeholders is not None
    assert placeholders["documentation_slug"] == "panel-insufficient-memory"
    assert _ROOT_PASSWORD not in repr(result)


async def test_first_panel_progress_cannot_double_submit_and_creates_subentry(
    hass: HomeAssistant,
) -> None:
    confirm = await _start_initial_confirm(hass)
    gate = asyncio.Event()
    mark_gate = asyncio.Event()
    provisioner = _FakeProvisioner(
        _identity(),
        gate=gate,
        mark_gate=mark_gate,
    )
    captured: list[dict[str, Any]] = []
    original_finish = hass.config_entries.subentries.async_finish_flow

    async def capture_finish(
        flow: Any,
        result: Any,
    ) -> Any:
        captured.append(dict(result))
        return await original_finish(flow, result)

    with (
        patch.object(
            flow_gateway,
            "_get_panel_provisioner",
            return_value=provisioner,
        ) as get_provisioner,
        patch.object(
            hass.config_entries.subentries,
            "async_finish_flow",
            side_effect=capture_finish,
        ),
    ):
        first = await hass.config_entries.subentries.async_configure(
            confirm["flow_id"],
            {CONF_NAME: "Office"},
        )
        second = await hass.config_entries.subentries.async_configure(
            confirm["flow_id"],
            {CONF_NAME: "Ignored duplicate submission"},
        )
        assert first["type"] is FlowResultType.SHOW_PROGRESS
        assert second["type"] is FlowResultType.SHOW_PROGRESS
        assert len(provisioner.install_calls) == 1

        gate.set()
        await asyncio.wait_for(provisioner.mark_started.wait(), timeout=1)
        active_flow = hass.config_entries.subentries._progress[confirm["flow_id"]]
        context = cast(Mapping[str, Any], active_flow.context)
        assert context[CONF_PROVISIONING_TRANSACTION_ID] == str(_TRANSACTION_ID)
        mark_gate.set()
        await _drain_progress(hass.config_entries.subentries, confirm["flow_id"])
        await hass.async_block_till_done()

    get_provisioner.assert_called_once_with(
        hass,
        expected_identity=_identity(),
    )
    assert provisioner.marked_transactions == [_TRANSACTION_ID]
    assert provisioner.recover_calls == 0
    assert provisioner.recover_probe_results == []
    create_result = next(
        result for result in captured if result["type"] is FlowResultType.CREATE_ENTRY
    )
    assert create_result["title"] == "Office"
    assert create_result["unique_id"] == _FINGERPRINT
    assert create_result["data"][CONF_PROVISIONING_TRANSACTION_ID] == str(_TRANSACTION_ID)
    entries = hass.config_entries.async_entries(DOMAIN)
    assert len(entries) == 1
    assert entries[0].data[CONF_NEXT_MESH_PRIORITY] == 1
    assert entries[0].data[CONF_SCENE_PANEL] == "__unassigned__"
    assert len(entries[0].subentries) == 1


async def test_first_panel_provision_failure_returns_to_confirm_without_secret_leak(
    hass: HomeAssistant,
) -> None:
    confirm = await _start_initial_confirm(hass)
    provisioner = _FakeProvisioner(
        _identity(),
        error=PanelProvisioningError("stage_failed"),
        gate=asyncio.Event(),
    )

    with patch.object(
        flow_gateway,
        "_get_panel_provisioner",
        return_value=provisioner,
    ):
        result = cast(
            dict[str, Any],
            await hass.config_entries.subentries.async_configure(
                confirm["flow_id"],
                {CONF_NAME: "Office"},
            ),
        )
        assert result["type"] is FlowResultType.SHOW_PROGRESS
        assert provisioner.gate is not None
        provisioner.gate.set()
        result = await _drain_progress(
            hass.config_entries.subentries,
            confirm["flow_id"],
        )

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "panel_confirm"
    assert result["errors"] == {"base": "stage_failed"}
    assert result["description_placeholders"]["documentation_slug"] == ("panel-stage-failed")
    assert _ROOT_PASSWORD not in repr(result)
    assert _BROKER_PASSWORD not in repr(result)
    entries = hass.config_entries.async_entries(DOMAIN)
    assert len(entries) == 1
    assert entries[0].subentries == {}


async def test_first_panel_mark_pending_failure_clears_flow_transaction(
    hass: HomeAssistant,
) -> None:
    confirm = await _start_initial_confirm(hass)
    provisioner = _FakeProvisioner(
        _identity(),
        gate=asyncio.Event(),
        mark_error=OSError(_ROOT_PASSWORD),
    )

    with patch.object(
        flow_gateway,
        "_get_panel_provisioner",
        return_value=provisioner,
    ):
        progress = await hass.config_entries.subentries.async_configure(
            confirm["flow_id"],
            {CONF_NAME: "Office"},
        )
        assert progress["type"] is FlowResultType.SHOW_PROGRESS
        active_flow = hass.config_entries.subentries._progress[confirm["flow_id"]]
        provisioner.recover_probe = lambda: _flow_transaction_is_cleared(active_flow)
        assert provisioner.gate is not None
        provisioner.gate.set()
        result = await _drain_progress(
            hass.config_entries.subentries,
            confirm["flow_id"],
        )

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "panel_confirm"
    assert result["errors"] == {"base": "provisioning_failed"}
    assert CONF_PROVISIONING_TRANSACTION_ID not in active_flow.context
    assert provisioner.recover_calls == 1
    assert provisioner.recover_probe_results == [True]
    assert _ROOT_PASSWORD not in repr(result)
    entries = hass.config_entries.async_entries(DOMAIN)
    assert len(entries) == 1
    assert entries[0].subentries == {}


async def test_mark_pending_cancellation_drains_recovery_then_propagates(
    hass: HomeAssistant,
) -> None:
    confirm = await _start_initial_confirm(hass)
    recover_gate = asyncio.Event()
    provisioner = _FakeProvisioner(
        _identity(),
        gate=asyncio.Event(),
        mark_gate=asyncio.Event(),
        recover_gate=recover_gate,
    )

    with patch.object(
        flow_gateway,
        "_get_panel_provisioner",
        return_value=provisioner,
    ):
        progress = await hass.config_entries.subentries.async_configure(
            confirm["flow_id"],
            {CONF_NAME: "Office"},
        )
        assert progress["type"] is FlowResultType.SHOW_PROGRESS
        active_flow = hass.config_entries.subentries._progress[confirm["flow_id"]]
        task = active_flow.async_get_progress_task()
        assert task is not None
        provisioner.recover_probe = lambda: _flow_transaction_is_cleared(active_flow)
        assert provisioner.gate is not None
        provisioner.gate.set()
        await asyncio.wait_for(provisioner.mark_started.wait(), timeout=1)

        task.cancel()
        await asyncio.wait_for(provisioner.recover_started.wait(), timeout=1)
        assert not task.done()
        hass.config_entries.subentries.async_abort(confirm["flow_id"])
        recover_gate.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert provisioner.recover_calls == 1
    assert provisioner.recover_probe_results == [True]
    entries = hass.config_entries.async_entries(DOMAIN)
    assert len(entries) == 1
    assert entries[0].subentries == {}


async def test_mismatched_provisioner_result_is_recovered_before_retry(
    hass: HomeAssistant,
) -> None:
    confirm = await _start_initial_confirm(hass)
    provisioner = _FakeProvisioner(
        _identity(),
        gate=asyncio.Event(),
        panel_data_overrides={
            CONF_COMPONENTS: {
                COMPONENT_BRIDGE: True,
                COMPONENT_WIFI_WATCHDOG: False,
                COMPONENT_BUS_WATCHDOG: True,
            }
        },
    )

    with patch.object(
        flow_gateway,
        "_get_panel_provisioner",
        return_value=provisioner,
    ):
        progress = await hass.config_entries.subentries.async_configure(
            confirm["flow_id"],
            {CONF_NAME: "Office"},
        )
        assert progress["type"] is FlowResultType.SHOW_PROGRESS
        active_flow = hass.config_entries.subentries._progress[confirm["flow_id"]]
        provisioner.recover_probe = lambda: _flow_transaction_is_cleared(active_flow)
        assert provisioner.gate is not None
        provisioner.gate.set()
        result = await _drain_progress(
            hass.config_entries.subentries,
            confirm["flow_id"],
        )

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "panel_confirm"
    assert result["errors"] == {"base": "invalid_provisioning_dependency"}
    assert provisioner.marked_transactions == []
    assert provisioner.recover_calls == 1
    assert provisioner.recover_probe_results == [True]
    entries = hass.config_entries.async_entries(DOMAIN)
    assert len(entries) == 1
    assert entries[0].subentries == {}


async def test_existing_fleet_aborts_new_initial_flow(hass: HomeAssistant) -> None:
    _fleet_entry(hass, _subentry())

    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": "user"},
    )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "already_configured"


async def test_ignored_fleet_entry_still_allows_manual_onboarding(
    hass: HomeAssistant,
) -> None:
    """Core's _abort_if_unique_id_configured deliberately lets a manual user
    flow proceed past an ignored entry — ignoring a fleet must not brick
    onboarding forever."""
    MockConfigEntry(
        domain=DOMAIN,
        unique_id=FLEET_UNIQUE_ID,
        source=SOURCE_IGNORE,
    ).add_to_hass(hass)

    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": "user"},
    )

    assert result["type"] is FlowResultType.MENU


async def test_legacy_entry_aborts_competing_fleet_creation(
    hass: HomeAssistant,
) -> None:
    MockConfigEntry(
        domain=DOMAIN,
        version=CONFIG_ENTRY_VERSION,
        data={CONF_ENTRY_KIND: ENTRY_KIND_LEGACY_PENDING_CONSOLIDATION},
    ).add_to_hass(hass)

    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": "user"},
    )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "legacy_migration_required"


def test_subentry_type_is_supported_only_by_exact_fleet_parent(
    hass: HomeAssistant,
) -> None:
    fleet = MockConfigEntry(
        domain=DOMAIN,
        unique_id=FLEET_UNIQUE_ID,
        version=CONFIG_ENTRY_VERSION,
        data=_fleet_data(),
    )
    wrong_identity = MockConfigEntry(
        domain=DOMAIN,
        unique_id="not-the-fleet",
        version=CONFIG_ENTRY_VERSION,
        data=_fleet_data(),
    )
    legacy = MockConfigEntry(
        domain=DOMAIN,
        version=CONFIG_ENTRY_VERSION,
        data={CONF_ENTRY_KIND: ENTRY_KIND_LEGACY_PENDING_CONSOLIDATION},
    )

    assert config_flow.BrilliantMqttConfigFlow.async_get_supported_subentry_types(fleet) == {
        SUBENTRY_TYPE_PANEL: config_flow.PanelSubentryFlow
    }
    assert config_flow.BrilliantMqttConfigFlow.async_get_supported_subentry_types(legacy) == {}
    assert (
        config_flow.BrilliantMqttConfigFlow.async_get_supported_subentry_types(wrong_identity) == {}
    )


async def test_add_panel_inherits_fleet_and_creates_panel_only_subentry(
    hass: HomeAssistant,
) -> None:
    entry = _fleet_entry(hass, _subentry())
    candidate = _identity(other=True)
    confirm = await _start_subentry_confirm(hass, entry, identity=candidate)
    mark_gate = asyncio.Event()
    provisioner = _FakeProvisioner(
        candidate,
        gate=asyncio.Event(),
        mark_gate=mark_gate,
    )

    with patch.object(
        flow_gateway,
        "_get_panel_provisioner",
        return_value=provisioner,
    ) as get_provisioner:
        progress = await hass.config_entries.subentries.async_configure(
            confirm["flow_id"],
            {CONF_NAME: "Kitchen"},
        )
        assert progress["type"] is FlowResultType.SHOW_PROGRESS
        assert provisioner.gate is not None
        provisioner.gate.set()
        await asyncio.wait_for(provisioner.mark_started.wait(), timeout=1)
        active_flow = hass.config_entries.subentries._progress[confirm["flow_id"]]
        context = cast(Mapping[str, Any], active_flow.context)
        assert context[CONF_PROVISIONING_TRANSACTION_ID] == str(_TRANSACTION_ID)
        mark_gate.set()
        await _drain_progress(
            hass.config_entries.subentries,
            confirm["flow_id"],
        )
        await hass.async_block_till_done()

    get_provisioner.assert_called_once_with(
        hass,
        expected_identity=candidate,
    )
    assert len(provisioner.install_calls) == 1
    request, fleet = provisioner.install_calls[0]
    assert request.slug == "kitchen"
    assert request.mesh_priority == 2
    assert request.ssh_username == "root"
    assert request.selected_components == (
        COMPONENT_BRIDGE,
        COMPONENT_WIFI_WATCHDOG,
        COMPONENT_BUS_WATCHDOG,
    )
    assert fleet.broker.kind is BrokerKind.EXISTING_BROKER
    assert provisioner.marked_transactions == [_TRANSACTION_ID]
    assert provisioner.recover_calls == 0
    assert provisioner.recover_probe_results == []

    created = [
        panel for panel in entry.subentries.values() if panel.unique_id == _OTHER_FINGERPRINT
    ]
    assert len(created) == 1
    panel = created[0]
    assert panel.title == "Kitchen"
    assert set(panel.data) == {
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
    assert not {
        CONF_BROKER_KIND,
        CONF_MQTT_HOST,
        CONF_MQTT_PORT,
        CONF_MQTT_USERNAME,
        CONF_MQTT_PASSWORD,
        CONF_MQTT_TLS_ENABLED,
        CONF_MQTT_TLS_CA,
        CONF_HA_CONTROL_ENABLED,
        CONF_SCENE_PANEL,
    }.intersection(panel.data)


async def test_add_panel_allocates_first_available_slug_and_priority(
    hass: HomeAssistant,
) -> None:
    office = _subentry()
    office_2 = _subentry(
        slug="office-2",
        fingerprint="SHA256:persisted-office-2",
        public_key=_OTHER_PUBLIC_KEY,
        priority=3,
        subentry_id="panel-office-2",
    )
    entry = _fleet_entry(hass, office, office_2)
    third_identity = _identity(other=True)
    confirm = await _start_subentry_confirm(hass, entry, identity=third_identity)
    provisioner = _FakeProvisioner(third_identity, gate=asyncio.Event())

    with patch.object(
        flow_gateway,
        "_get_panel_provisioner",
        return_value=provisioner,
    ):
        await hass.config_entries.subentries.async_configure(
            confirm["flow_id"],
            {CONF_NAME: "Office"},
        )
        assert provisioner.gate is not None
        provisioner.gate.set()
        await _drain_progress(
            hass.config_entries.subentries,
            confirm["flow_id"],
        )

    request, _fleet = provisioner.install_calls[0]
    assert request.slug == "office-3"
    assert request.mesh_priority == 2


async def test_exhausted_mesh_priorities_redisplay_without_starting_provision(
    hass: HomeAssistant,
) -> None:
    """Allocation failures stay on confirm and never cross the panel-write boundary."""
    panels = tuple(
        _subentry(
            slug=f"panel-{priority}",
            fingerprint=f"SHA256:persisted-{priority}",
            priority=priority,
            subentry_id=f"panel-{priority}",
        )
        for priority in range(1, 100)
    )
    entry = _fleet_entry(hass, *panels)
    confirm = await _start_subentry_confirm(hass, entry)

    with patch.object(flow_gateway, "_get_panel_provisioner") as get_provisioner:
        result = await hass.config_entries.subentries.async_configure(
            confirm["flow_id"],
            {CONF_NAME: "New panel"},
        )

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "panel_confirm"
    assert result["errors"] == {"base": "mesh_priority_exhausted"}
    get_provisioner.assert_not_called()
    assert len(entry.subentries) == 99


async def test_subentry_progress_abandonment_invokes_removal_recovery(
    hass: HomeAssistant,
) -> None:
    entry = _fleet_entry(hass, _subentry())
    candidate = _identity(other=True)
    confirm = await _start_subentry_confirm(hass, entry, identity=candidate)
    provisioner = _FakeProvisioner(candidate, gate=asyncio.Event())

    with patch.object(
        flow_gateway,
        "_get_panel_provisioner",
        return_value=provisioner,
    ):
        progress = await hass.config_entries.subentries.async_configure(
            confirm["flow_id"],
            {CONF_NAME: "Kitchen"},
        )
        assert progress["type"] is FlowResultType.SHOW_PROGRESS
        active_flow = hass.config_entries.subentries._progress[confirm["flow_id"]]
        provisioner.recover_probe = lambda: _flow_transaction_is_cleared(active_flow)
        assert provisioner.gate is not None
        provisioner.gate.set()
        await _wait_progress_done(
            hass.config_entries.subentries,
            confirm["flow_id"],
        )
        assert not _flow_transaction_is_cleared(active_flow)

        hass.config_entries.subentries.async_abort(confirm["flow_id"])
        await asyncio.wait_for(provisioner.recover_started.wait(), timeout=1)

    assert _flow_transaction_is_cleared(active_flow)
    assert provisioner.recover_calls == 1
    assert provisioner.recover_probe_results == [True]
    assert len(entry.subentries) == 1


@pytest.mark.parametrize(
    "fleet_update",
    (
        {CONF_MQTT_HOST: "replacement-broker.iot.example"},
        {CONF_MQTT_PASSWORD: "replacement-password"},
        {
            CONF_MQTT_PORT: 8883,
            CONF_MQTT_TLS_ENABLED: True,
        },
    ),
    ids=("endpoint", "credentials", "tls"),
)
async def test_add_panel_aborts_if_fleet_changes_during_install(
    hass: HomeAssistant,
    fleet_update: dict[str, object],
) -> None:
    entry = _fleet_entry(hass, _subentry())
    expected_fleet = FleetConfig.from_entry(entry)
    candidate = _identity(other=True)
    confirm = await _start_subentry_confirm(hass, entry, identity=candidate)
    provisioner = _FakeProvisioner(candidate, gate=asyncio.Event())

    with patch.object(
        flow_gateway,
        "_get_panel_provisioner",
        return_value=provisioner,
    ):
        progress = await hass.config_entries.subentries.async_configure(
            confirm["flow_id"],
            {CONF_NAME: "Kitchen"},
        )
        assert progress["type"] is FlowResultType.SHOW_PROGRESS
        active_flow = hass.config_entries.subentries._progress[confirm["flow_id"]]
        provisioner.recover_probe = lambda: _flow_transaction_is_cleared(active_flow)
        request, provisioned_fleet = provisioner.install_calls[0]
        assert request.slug == "kitchen"
        assert provisioned_fleet == expected_fleet

        hass.config_entries.async_update_entry(
            entry,
            data={**entry.data, **fleet_update},
        )
        assert provisioner.gate is not None
        provisioner.gate.set()
        result = await _drain_progress(
            hass.config_entries.subentries,
            confirm["flow_id"],
        )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "parent_changed"
    assert CONF_PROVISIONING_TRANSACTION_ID not in active_flow.context
    assert provisioner.recover_calls == 1
    assert provisioner.recover_probe_results == [True]
    assert len(entry.subentries) == 1


async def test_add_panel_aborts_if_parent_is_removed_during_install(
    hass: HomeAssistant,
) -> None:
    entry = _fleet_entry(hass, _subentry())
    candidate = _identity(other=True)
    confirm = await _start_subentry_confirm(hass, entry, identity=candidate)
    provisioner = _FakeProvisioner(candidate, gate=asyncio.Event())

    with patch.object(
        flow_gateway,
        "_get_panel_provisioner",
        return_value=provisioner,
    ):
        progress = await hass.config_entries.subentries.async_configure(
            confirm["flow_id"],
            {CONF_NAME: "Kitchen"},
        )
        assert progress["type"] is FlowResultType.SHOW_PROGRESS
        active_flow = hass.config_entries.subentries._progress[confirm["flow_id"]]
        provisioner.recover_probe = lambda: _flow_transaction_is_cleared(active_flow)
        await hass.config_entries.async_remove(entry.entry_id)
        assert provisioner.gate is not None
        provisioner.gate.set()
        result = await _drain_progress(
            hass.config_entries.subentries,
            confirm["flow_id"],
        )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "invalid_parent"
    assert CONF_PROVISIONING_TRANSACTION_ID not in active_flow.context
    assert provisioner.recover_calls == 1
    assert provisioner.recover_probe_results == [True]
    assert not hass.config_entries.async_entries(DOMAIN)


async def test_post_install_abort_surfaces_recovery_failure(
    hass: HomeAssistant,
    caplog: pytest.LogCaptureFixture,
) -> None:
    entry = _fleet_entry(hass, _subentry())
    candidate = _identity(other=True)
    confirm = await _start_subentry_confirm(hass, entry, identity=candidate)
    provisioner = _FakeProvisioner(
        candidate,
        gate=asyncio.Event(),
        recover_error=OSError(_ROOT_PASSWORD),
    )

    with patch.object(
        flow_gateway,
        "_get_panel_provisioner",
        return_value=provisioner,
    ):
        progress = await hass.config_entries.subentries.async_configure(
            confirm["flow_id"],
            {CONF_NAME: "Kitchen"},
        )
        assert progress["type"] is FlowResultType.SHOW_PROGRESS
        active_flow = hass.config_entries.subentries._progress[confirm["flow_id"]]
        provisioner.recover_probe = lambda: _flow_transaction_is_cleared(active_flow)
        hass.config_entries.async_update_entry(
            entry,
            data={**entry.data, CONF_MQTT_HOST: "replacement-broker.iot.example"},
        )
        assert provisioner.gate is not None
        provisioner.gate.set()
        result = await _drain_progress(
            hass.config_entries.subentries,
            confirm["flow_id"],
        )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "recovery_failed"
    assert result["description_placeholders"] == {
        "documentation_slug": "panel-recovery-failed",
        "stage": "recovery",
    }
    assert provisioner.recover_calls == 1
    assert provisioner.recover_probe_results == [True]
    assert len(entry.subentries) == 1
    assert _ROOT_PASSWORD not in caplog.text


@pytest.mark.parametrize(
    ("conflict", "reason"),
    (
        ("fingerprint", "already_configured"),
        ("slug", "panel_slug_conflict"),
        ("priority", "mesh_priority_conflict"),
    ),
)
async def test_add_panel_rechecks_storage_conflicts_after_install(
    hass: HomeAssistant,
    conflict: str,
    reason: str,
) -> None:
    entry = _fleet_entry(hass, _subentry())
    candidate = _identity(other=True)
    confirm = await _start_subentry_confirm(hass, entry, identity=candidate)
    provisioner = _FakeProvisioner(candidate, gate=asyncio.Event())

    with patch.object(
        flow_gateway,
        "_get_panel_provisioner",
        return_value=provisioner,
    ):
        progress = await hass.config_entries.subentries.async_configure(
            confirm["flow_id"],
            {CONF_NAME: "Kitchen"},
        )
        assert progress["type"] is FlowResultType.SHOW_PROGRESS
        active_flow = hass.config_entries.subentries._progress[confirm["flow_id"]]
        provisioner.recover_probe = lambda: _flow_transaction_is_cleared(active_flow)
        raced = _subentry(
            slug="kitchen" if conflict == "slug" else f"race-{conflict}",
            fingerprint=(
                candidate.fingerprint if conflict == "fingerprint" else f"SHA256:race-{conflict}"
            ),
            public_key=_OTHER_PUBLIC_KEY,
            priority=2 if conflict == "priority" else 3,
            subentry_id=f"panel-race-{conflict}",
        )
        hass.config_entries.async_add_subentry(entry, raced)
        assert provisioner.gate is not None
        provisioner.gate.set()
        result = await _drain_progress(
            hass.config_entries.subentries,
            confirm["flow_id"],
        )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == reason
    assert CONF_PROVISIONING_TRANSACTION_ID not in active_flow.context
    assert provisioner.recover_calls == 1
    assert provisioner.recover_probe_results == [True]
    assert raced.subentry_id in entry.subentries
    assert len(entry.subentries) == 2


async def test_duplicate_fingerprint_aborts_before_password_authentication(
    hass: HomeAssistant,
) -> None:
    office = _subentry()
    entry = _fleet_entry(hass, office)
    result = await hass.config_entries.subentries.async_init(
        (entry.entry_id, SUBENTRY_TYPE_PANEL),
        context={"source": "user"},
    )

    with (
        patch.object(
            flow_gateway,
            "async_fetch_host_identity",
            return_value=_identity(),
        ),
        patch.object(flow_gateway, "FleetAsyncsshShell") as shell_constructor,
    ):
        result = await hass.config_entries.subentries.async_configure(
            result["flow_id"],
            _PANEL_INPUT,
        )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "already_configured"
    assert result["description_placeholders"] == {
        "subentry_id": office.subentry_id,
        "panel_name": office.title,
    }
    shell_constructor.assert_not_called()


async def test_add_panel_reserves_rebound_panel_management_identity_before_authentication(
    hass: HomeAssistant,
) -> None:
    """A rebind cannot free the old physical identity for a second logical panel."""
    rebound = _subentry(
        fingerprint=_OTHER_FINGERPRINT,
        public_key=_OTHER_PUBLIC_KEY,
        management_id=_FINGERPRINT,
    )
    entry = _fleet_entry(hass, rebound)
    result = await hass.config_entries.subentries.async_init(
        (entry.entry_id, SUBENTRY_TYPE_PANEL),
        context={"source": "user"},
    )
    inspect = AsyncMock()

    with (
        patch.object(
            flow_gateway,
            "async_fetch_host_identity",
            return_value=_identity(),
        ),
        patch.object(flow_gateway, "_async_inspect_candidate", inspect),
    ):
        result = await hass.config_entries.subentries.async_configure(
            result["flow_id"],
            _PANEL_INPUT,
        )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "already_configured"
    assert result["description_placeholders"] == {
        "subentry_id": rebound.subentry_id,
        "panel_name": rebound.title,
    }
    inspect.assert_not_awaited()


async def test_subentry_provisioning_error_keeps_parent_and_secrets_redacted(
    hass: HomeAssistant,
) -> None:
    entry = _fleet_entry(hass, _subentry())
    candidate = _identity(other=True)
    confirm = await _start_subentry_confirm(hass, entry, identity=candidate)
    provisioner = _FakeProvisioner(
        candidate,
        error=PanelProvisioningError("preflight_failed"),
        gate=asyncio.Event(),
    )

    with patch.object(
        flow_gateway,
        "_get_panel_provisioner",
        return_value=provisioner,
    ):
        result = cast(
            dict[str, Any],
            await hass.config_entries.subentries.async_configure(
                confirm["flow_id"],
                {CONF_NAME: "Kitchen"},
            ),
        )
        assert result["type"] is FlowResultType.SHOW_PROGRESS
        assert provisioner.gate is not None
        provisioner.gate.set()
        result = await _drain_progress(
            hass.config_entries.subentries,
            confirm["flow_id"],
        )

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "panel_confirm"
    assert result["errors"] == {"base": "preflight_failed"}
    assert len(entry.subentries) == 1
    assert _ROOT_PASSWORD not in repr(result)
    assert _BROKER_PASSWORD not in repr(result)


async def test_fleet_options_menu_is_owner_scoped_and_uses_native_add_panel_action(
    hass: HomeAssistant,
) -> None:
    panel = _subentry()
    entry = _fleet_entry(hass, panel)

    result = await hass.config_entries.options.async_init(entry.entry_id)

    assert result["type"] is FlowResultType.MENU
    assert result["step_id"] == "init"
    assert tuple(cast(Iterable[str], result["menu_options"])) == (
        "broker",
        "ha_control",
        "scenes",
        "fleet_defaults",
        "advanced",
    )
    assert "add_panel" not in result["menu_options"]
    assert "trust_host_key_changes" not in repr(result)
    assert config_flow.BrilliantMqttConfigFlow.async_get_supported_subentry_types(entry) == {
        SUBENTRY_TYPE_PANEL: config_flow.PanelSubentryFlow
    }


async def test_fleet_broker_options_masks_stored_password(
    hass: HomeAssistant,
) -> None:
    entry = _fleet_entry(hass, _subentry())

    result = await _start_fleet_broker_options(hass, entry)

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "broker_profile"
    password_marker = next(
        marker for marker in result["data_schema"].schema if str(marker) == CONF_MQTT_PASSWORD
    )
    assert password_marker.description == {"suggested_value": SECRET_UNCHANGED}
    assert _BROKER_PASSWORD not in repr(result)


async def test_fleet_broker_options_field_error_preserves_only_nonsecret_input(
    hass: HomeAssistant,
) -> None:
    entry = _fleet_entry(hass)
    form = await _start_fleet_broker_options(hass, entry)
    submitted_password = "SECRET-invalid-options-password\n"

    result = await hass.config_entries.options.async_configure(
        form["flow_id"],
        {
            **_TLS_BROKER_INPUT,
            CONF_MQTT_HOST: "replacement-broker.iot.example",
            CONF_MQTT_PASSWORD: submitted_password,
        },
    )

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "broker_profile"
    assert result["errors"] == {CONF_MQTT_PASSWORD: "invalid_value"}
    defaults = _schema_defaults(result)
    assert defaults[CONF_MQTT_HOST] == "replacement-broker.iot.example"
    assert defaults[CONF_MQTT_PORT] == 8883
    assert defaults[CONF_MQTT_USERNAME] == "brilliant-fleet"
    assert defaults[ADVANCED_SECTION] == {
        CONF_MQTT_TLS_ENABLED: True,
        CONF_MQTT_TLS_CA: _CA_PEM,
    }
    assert submitted_password not in repr(result)
    assert _BROKER_PASSWORD not in repr(result)
    assert CONF_MQTT_PASSWORD not in defaults
    assert entry.data[CONF_MQTT_HOST] == "mqtt.iot.example"


async def test_fleet_broker_options_field_error_clears_stale_validation_help(
    hass: HomeAssistant,
) -> None:
    entry = _fleet_entry(hass)
    form = await _start_fleet_broker_options(hass, entry)
    validator = _FakeValidator(
        error=OperationError.for_code(
            OperationStage.HA_MQTT_READY,
            "unsupported_discovery_prefix",
        ),
        gate=asyncio.Event(),
    )
    with patch.object(flow_gateway, "_broker_validator", return_value=validator):
        progress = await hass.config_entries.options.async_configure(
            form["flow_id"],
            {
                **_BROKER_INPUT,
                CONF_MQTT_PASSWORD: SECRET_UNCHANGED,
            },
        )
    assert progress["type"] is FlowResultType.SHOW_PROGRESS
    assert validator.gate is not None
    validator.gate.set()
    failure = await _drain_progress(
        hass.config_entries.options,
        form["flow_id"],
    )
    assert failure["errors"] == {"base": "unsupported_discovery_prefix"}

    result = await hass.config_entries.options.async_configure(
        failure["flow_id"],
        {
            **_BROKER_INPUT,
            CONF_MQTT_PASSWORD: "SECRET-invalid-options-retry-password\n",
        },
    )

    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {CONF_MQTT_PASSWORD: "invalid_value"}
    assert result["description_placeholders"] == {
        "documentation_url": (
            "https://github.com/joyfulhouse/brilliant-mqtt/blob/main/"
            "docs/install/mqtt-broker.md#mqtt-validation"
        )
    }
    assert entry.data[CONF_MQTT_PASSWORD] == _BROKER_PASSWORD


async def test_empty_fleet_broker_options_validates_then_updates_profile(
    hass: HomeAssistant,
) -> None:
    entry = _fleet_entry(hass)
    original_globals = {
        key: entry.data[key]
        for key in (
            CONF_HA_CONTROL_ENABLED,
            CONF_HA_CONTROL_LABEL,
            CONF_ROOM_OVERRIDES,
            CONF_HA_CONTROL_DOMAINS,
            CONF_MAX_MIRRORED_ENTITIES,
            CONF_SCENE_PANEL,
            CONF_SCENE_ACTIONS,
        )
    }
    form = await _start_fleet_broker_options(hass, entry)
    validator = _FakeValidator(gate=asyncio.Event())
    submitted = {
        **_BROKER_INPUT,
        CONF_MQTT_HOST: "replacement-broker.iot.example",
        CONF_MQTT_PASSWORD: "SECRET-replacement-broker-password",
    }

    with patch.object(flow_gateway, "_broker_validator", return_value=validator):
        progress = await hass.config_entries.options.async_configure(
            form["flow_id"],
            submitted,
        )
    assert progress["type"] is FlowResultType.SHOW_PROGRESS
    assert validator.gate is not None
    validator.gate.set()
    result = await _drain_progress(
        hass.config_entries.options,
        form["flow_id"],
    )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "reconfigure_successful"
    assert entry.data[CONF_MQTT_HOST] == "replacement-broker.iot.example"
    assert entry.data[CONF_MQTT_PASSWORD] == "SECRET-replacement-broker-password"
    assert {key: entry.data[key] for key in original_globals} == original_globals
    assert len(validator.calls) == 1


async def test_populated_fleet_identical_broker_profile_is_validation_only(
    hass: HomeAssistant,
) -> None:
    entry = _fleet_entry(hass, _subentry())
    original_data = dict(entry.data)
    form = await _start_fleet_broker_options(hass, entry)
    validator = _FakeValidator(gate=asyncio.Event())
    submitted = {
        CONF_MQTT_HOST: entry.data[CONF_MQTT_HOST],
        CONF_MQTT_PORT: entry.data[CONF_MQTT_PORT],
        CONF_MQTT_USERNAME: entry.data[CONF_MQTT_USERNAME],
        CONF_MQTT_PASSWORD: SECRET_UNCHANGED,
        ADVANCED_SECTION: {
            CONF_MQTT_TLS_ENABLED: entry.data[CONF_MQTT_TLS_ENABLED],
        },
    }

    with patch.object(flow_gateway, "_broker_validator", return_value=validator):
        progress = await hass.config_entries.options.async_configure(
            form["flow_id"],
            submitted,
        )
    assert progress["type"] is FlowResultType.SHOW_PROGRESS
    assert validator.gate is not None
    validator.gate.set()
    result = await _drain_progress(
        hass.config_entries.options,
        form["flow_id"],
    )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "reconfigure_successful"
    assert entry.data == original_data
    assert len(validator.calls) == 1
    assert _BROKER_PASSWORD not in repr(result)


async def test_populated_fleet_broker_change_requires_guided_flow_without_validation(
    hass: HomeAssistant,
) -> None:
    entry = _fleet_entry(hass, _subentry())
    original_data = dict(entry.data)
    form = await _start_fleet_broker_options(hass, entry)
    validator = _FakeValidator()

    with patch.object(flow_gateway, "_broker_validator", return_value=validator):
        result = await hass.config_entries.options.async_configure(
            form["flow_id"],
            {
                **_BROKER_INPUT,
                CONF_MQTT_HOST: "changed-broker.iot.example",
                CONF_MQTT_PASSWORD: SECRET_UNCHANGED,
            },
        )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "broker_change_requires_guided_flow"
    assert entry.data == original_data
    assert validator.calls == []
    assert _BROKER_PASSWORD not in repr(result)


async def test_fleet_broker_validation_failure_is_actionable_and_redacted(
    hass: HomeAssistant,
) -> None:
    entry = _fleet_entry(hass)
    original_data = dict(entry.data)
    form = await _start_fleet_broker_options(hass, entry)
    validator = _FakeValidator(
        error=OperationError.for_code(
            OperationStage.HA_MQTT_READY,
            "ha_mqtt_unavailable",
        ),
        gate=asyncio.Event(),
    )

    with patch.object(flow_gateway, "_broker_validator", return_value=validator):
        progress = await hass.config_entries.options.async_configure(
            form["flow_id"],
            {
                **_BROKER_INPUT,
                CONF_MQTT_HOST: "replacement-broker.iot.example",
                CONF_MQTT_PASSWORD: "SECRET-replacement-broker-password",
            },
        )
    assert progress["type"] is FlowResultType.SHOW_PROGRESS
    assert validator.gate is not None
    validator.gate.set()
    result = await _drain_progress(
        hass.config_entries.options,
        form["flow_id"],
    )

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "broker_profile"
    assert result["errors"] == {"base": "ha_mqtt_unavailable"}
    assert result["description_placeholders"]["documentation_slug"] == "mqtt-validation"
    assert entry.data == original_data
    assert "SECRET-replacement-broker-password" not in repr(result)
    assert _BROKER_PASSWORD not in repr(result)


async def test_empty_fleet_broker_update_fails_closed_if_panel_appears_during_validation(
    hass: HomeAssistant,
) -> None:
    entry = _fleet_entry(hass)
    original_broker = FleetConfig.from_entry(entry).broker
    form = await _start_fleet_broker_options(hass, entry)
    validator = _FakeValidator(gate=asyncio.Event())

    with patch.object(flow_gateway, "_broker_validator", return_value=validator):
        progress = await hass.config_entries.options.async_configure(
            form["flow_id"],
            {
                **_BROKER_INPUT,
                CONF_MQTT_HOST: "replacement-broker.iot.example",
                CONF_MQTT_PASSWORD: SECRET_UNCHANGED,
            },
        )
    assert progress["type"] is FlowResultType.SHOW_PROGRESS
    raced = _subentry()
    hass.config_entries.async_update_entry(
        entry,
        data={
            **entry.data,
            CONF_NEXT_MESH_PRIORITY: 2,
            CONF_SCENE_PANEL: raced.subentry_id,
        },
    )
    assert hass.config_entries.async_add_subentry(entry, raced)
    assert validator.gate is not None
    validator.gate.set()
    result = await _drain_progress(
        hass.config_entries.options,
        form["flow_id"],
    )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "broker_change_requires_guided_flow"
    assert FleetConfig.from_entry(entry).broker == original_broker
    assert len(entry.subentries) == 1


async def test_empty_fleet_broker_update_does_not_overwrite_concurrent_profile_change(
    hass: HomeAssistant,
) -> None:
    entry = _fleet_entry(hass)
    form = await _start_fleet_broker_options(hass, entry)
    validator = _FakeValidator(gate=asyncio.Event())

    with patch.object(flow_gateway, "_broker_validator", return_value=validator):
        progress = await hass.config_entries.options.async_configure(
            form["flow_id"],
            {
                **_BROKER_INPUT,
                CONF_MQTT_HOST: "validated-broker.iot.example",
                CONF_MQTT_PASSWORD: SECRET_UNCHANGED,
            },
        )
    assert progress["type"] is FlowResultType.SHOW_PROGRESS
    concurrently_updated = {
        **entry.data,
        CONF_MQTT_HOST: "concurrent-broker.iot.example",
    }
    hass.config_entries.async_update_entry(entry, data=concurrently_updated)
    assert validator.gate is not None
    validator.gate.set()
    result = await _drain_progress(
        hass.config_entries.options,
        form["flow_id"],
    )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "parent_changed"
    assert entry.data == concurrently_updated


async def test_empty_fleet_broker_update_rejects_active_pre_journal_add_flow(
    hass: HomeAssistant,
) -> None:
    entry = _fleet_entry(hass)
    original_data = dict(entry.data)
    options_flow_id = await _prepare_fleet_broker_commit(hass, entry)
    add_flow = await hass.config_entries.subentries.async_init(
        (entry.entry_id, SUBENTRY_TYPE_PANEL),
        context={"source": "user"},
    )

    assert add_flow["type"] is FlowResultType.FORM
    assert add_flow["step_id"] == "panel_connect"
    with patch.object(
        ProvisioningJournal,
        "async_load",
        AsyncMock(return_value=None),
    ) as load_journal:
        result = await hass.config_entries.options.async_configure(options_flow_id)

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "broker_change_blocked_by_panel_onboarding"
    assert entry.data == original_data
    load_journal.assert_awaited_once()


async def test_empty_fleet_broker_update_rejects_journal_without_subentry(
    hass: HomeAssistant,
) -> None:
    entry = _fleet_entry(hass)
    original_data = dict(entry.data)
    options_flow_id = await _prepare_fleet_broker_commit(hass, entry)

    with patch.object(
        ProvisioningJournal,
        "async_load",
        AsyncMock(return_value=object()),
    ):
        result = await hass.config_entries.options.async_configure(options_flow_id)

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "broker_change_blocked_by_panel_onboarding"
    assert entry.data == original_data
    assert not entry.subentries


async def test_empty_fleet_broker_update_fails_closed_when_journal_is_unreadable(
    hass: HomeAssistant,
) -> None:
    entry = _fleet_entry(hass)
    original_data = dict(entry.data)
    options_flow_id = await _prepare_fleet_broker_commit(hass, entry)

    with patch.object(
        ProvisioningJournal,
        "async_load",
        AsyncMock(side_effect=RuntimeError("journal storage unavailable")),
    ):
        result = await hass.config_entries.options.async_configure(options_flow_id)

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "broker_change_blocked_by_panel_onboarding"
    assert entry.data == original_data


async def test_empty_fleet_broker_update_rechecks_add_flow_after_journal_read(
    hass: HomeAssistant,
) -> None:
    entry = _fleet_entry(hass)
    original_data = dict(entry.data)
    options_flow_id = await _prepare_fleet_broker_commit(hass, entry)
    journal_read_started = asyncio.Event()
    finish_journal_read = asyncio.Event()

    async def _gated_journal_read(_journal: ProvisioningJournal) -> None:
        journal_read_started.set()
        await finish_journal_read.wait()

    with patch.object(
        ProvisioningJournal,
        "async_load",
        _gated_journal_read,
    ):
        commit_task = asyncio.create_task(
            hass.config_entries.options.async_configure(options_flow_id),
        )
        try:
            await asyncio.wait_for(journal_read_started.wait(), timeout=1)
            add_flow = await hass.config_entries.subentries.async_init(
                (entry.entry_id, SUBENTRY_TYPE_PANEL),
                context={"source": "user"},
            )
            assert add_flow["type"] is FlowResultType.FORM
        finally:
            finish_journal_read.set()
            result = await asyncio.wait_for(commit_task, timeout=1)

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "broker_change_blocked_by_panel_onboarding"
    assert entry.data == original_data


async def test_empty_fleet_broker_update_rechecks_snapshot_after_journal_read(
    hass: HomeAssistant,
) -> None:
    entry = _fleet_entry(hass)
    options_flow_id = await _prepare_fleet_broker_commit(hass, entry)
    journal_read_started = asyncio.Event()
    finish_journal_read = asyncio.Event()

    async def _gated_journal_read(_journal: ProvisioningJournal) -> None:
        journal_read_started.set()
        await finish_journal_read.wait()

    with patch.object(
        ProvisioningJournal,
        "async_load",
        _gated_journal_read,
    ):
        commit_task = asyncio.create_task(
            hass.config_entries.options.async_configure(options_flow_id),
        )
        try:
            await asyncio.wait_for(journal_read_started.wait(), timeout=1)
            concurrently_updated = {
                **entry.data,
                CONF_MQTT_HOST: "concurrent-broker.iot.example",
            }
            hass.config_entries.async_update_entry(entry, data=concurrently_updated)
        finally:
            finish_journal_read.set()
            result = await asyncio.wait_for(commit_task, timeout=1)

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "parent_changed"
    assert entry.data == concurrently_updated


async def test_empty_fleet_broker_update_waits_for_real_provisioner_operation_lock(
    hass: HomeAssistant,
) -> None:
    entry = _fleet_entry(hass)
    original_data = dict(entry.data)
    options_flow_id = await _prepare_fleet_broker_commit(hass, entry)
    provisioner = flow_gateway._get_panel_provisioner(
        hass,
        expected_identity=_identity(),
    )
    provisioner_holds_lock = asyncio.Event()
    release_provisioner = asyncio.Event()

    async def _hold_recovery(_update: ProvisioningProgress) -> None:
        provisioner_holds_lock.set()
        await release_provisioner.wait()

    recovery_task = asyncio.create_task(provisioner.async_recover(_hold_recovery))
    await asyncio.wait_for(provisioner_holds_lock.wait(), timeout=1)
    commit_task = asyncio.create_task(
        hass.config_entries.options.async_configure(options_flow_id),
    )
    try:
        await asyncio.sleep(0)
        assert cast(asyncio.Lock, hass.data[DOMAIN]["ssh_lock"]).locked()
        assert not commit_task.done()
        assert entry.data == original_data
    finally:
        release_provisioner.set()
        await asyncio.wait_for(recovery_task, timeout=1)
        result = await asyncio.wait_for(commit_task, timeout=1)

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "reconfigure_successful"
    assert entry.data[CONF_MQTT_HOST] == "replacement-broker.iot.example"


async def test_fleet_broker_failure_retry_keeps_new_pending_password(
    hass: HomeAssistant,
) -> None:
    entry = _fleet_entry(hass)
    form = await _start_fleet_broker_options(hass, entry)
    replacement_password = "SECRET-pending-replacement-password"
    failed = _FakeValidator(
        error=OperationError.for_code(
            OperationStage.HA_MQTT_READY,
            "ha_mqtt_unavailable",
        ),
        gate=asyncio.Event(),
    )
    submitted = {
        **_BROKER_INPUT,
        CONF_MQTT_HOST: "replacement-broker.iot.example",
        CONF_MQTT_PASSWORD: replacement_password,
    }

    with patch.object(flow_gateway, "_broker_validator", return_value=failed):
        progress = await hass.config_entries.options.async_configure(
            form["flow_id"],
            submitted,
        )
    assert progress["type"] is FlowResultType.SHOW_PROGRESS
    assert failed.gate is not None
    failed.gate.set()
    retry = await _drain_progress(
        hass.config_entries.options,
        form["flow_id"],
    )
    assert retry["type"] is FlowResultType.FORM
    assert retry["errors"] == {"base": "ha_mqtt_unavailable"}
    assert replacement_password not in repr(retry)

    succeeded = _FakeValidator(gate=asyncio.Event())
    with patch.object(flow_gateway, "_broker_validator", return_value=succeeded):
        progress = await hass.config_entries.options.async_configure(
            retry["flow_id"],
            {
                **submitted,
                CONF_MQTT_PASSWORD: SECRET_UNCHANGED,
            },
        )
    assert progress["type"] is FlowResultType.SHOW_PROGRESS
    assert succeeded.gate is not None
    succeeded.gate.set()
    result = await _drain_progress(
        hass.config_entries.options,
        retry["flow_id"],
    )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "reconfigure_successful"
    assert entry.data[CONF_MQTT_PASSWORD] == replacement_password
    assert len(succeeded.calls) == 1


async def test_populated_fleet_can_correct_guidance_only_broker_kind(
    hass: HomeAssistant,
) -> None:
    entry = _fleet_entry(hass, _subentry())
    original = dict(entry.data)
    form = await _start_fleet_broker_options(
        hass,
        entry,
        BrokerKind.OFFICIAL_MOSQUITTO,
    )
    validator = _FakeValidator(gate=asyncio.Event())

    with patch.object(flow_gateway, "_broker_validator", return_value=validator):
        progress = await hass.config_entries.options.async_configure(
            form["flow_id"],
            {
                CONF_MQTT_HOST: entry.data[CONF_MQTT_HOST],
                CONF_MQTT_PORT: entry.data[CONF_MQTT_PORT],
                CONF_MQTT_USERNAME: entry.data[CONF_MQTT_USERNAME],
                CONF_MQTT_PASSWORD: SECRET_UNCHANGED,
                ADVANCED_SECTION: {
                    CONF_MQTT_TLS_ENABLED: entry.data[CONF_MQTT_TLS_ENABLED],
                },
            },
        )
    assert progress["type"] is FlowResultType.SHOW_PROGRESS
    assert validator.gate is not None
    validator.gate.set()
    await _wait_progress_done(hass.config_entries.options, form["flow_id"])
    add_flow = await hass.config_entries.subentries.async_init(
        (entry.entry_id, SUBENTRY_TYPE_PANEL),
        context={"source": "user"},
    )
    assert add_flow["type"] is FlowResultType.FORM
    with patch.object(
        ProvisioningJournal,
        "async_load",
        AsyncMock(side_effect=AssertionError("kind-only changes must not read the journal")),
    ) as load_journal:
        result = await hass.config_entries.options.async_configure(form["flow_id"])

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "reconfigure_successful"
    assert entry.data == {
        **original,
        CONF_BROKER_KIND: BrokerKind.OFFICIAL_MOSQUITTO.value,
    }
    load_journal.assert_not_awaited()


async def test_fleet_broker_commit_rejects_ownership_envelope_drift(
    hass: HomeAssistant,
) -> None:
    entry = _fleet_entry(hass)
    original_data = dict(entry.data)
    form = await _start_fleet_broker_options(hass, entry)
    validator = _FakeValidator(gate=asyncio.Event())

    with patch.object(flow_gateway, "_broker_validator", return_value=validator):
        progress = await hass.config_entries.options.async_configure(
            form["flow_id"],
            {
                **_BROKER_INPUT,
                CONF_MQTT_HOST: "validated-broker.iot.example",
                CONF_MQTT_PASSWORD: SECRET_UNCHANGED,
            },
        )
    assert progress["type"] is FlowResultType.SHOW_PROGRESS
    hass.config_entries.async_update_entry(
        entry,
        unique_id="not-the-fleet-owner",
    )
    assert validator.gate is not None
    validator.gate.set()
    result = await _drain_progress(
        hass.config_entries.options,
        form["flow_id"],
    )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "invalid_parent"
    assert entry.data == original_data


async def test_fleet_broker_commit_handles_parent_removal_during_validation(
    hass: HomeAssistant,
) -> None:
    entry = _fleet_entry(hass)
    form = await _start_fleet_broker_options(hass, entry)
    validator = _FakeValidator(gate=asyncio.Event())

    with patch.object(flow_gateway, "_broker_validator", return_value=validator):
        progress = await hass.config_entries.options.async_configure(
            form["flow_id"],
            {
                **_BROKER_INPUT,
                CONF_MQTT_HOST: "validated-broker.iot.example",
                CONF_MQTT_PASSWORD: SECRET_UNCHANGED,
            },
        )
    assert progress["type"] is FlowResultType.SHOW_PROGRESS
    await hass.config_entries.async_remove(entry.entry_id)
    assert validator.gate is not None
    validator.gate.set()
    result = await _drain_progress(
        hass.config_entries.options,
        form["flow_id"],
    )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "invalid_parent"
    assert not hass.config_entries.async_entries(DOMAIN)


async def test_fleet_control_updates_only_parent_globals_once(
    hass: HomeAssistant,
) -> None:
    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    original_panel = panel.as_dict()
    form = await _start_fleet_options_step(hass, entry, "ha_control")
    submitted = {
        CONF_HA_CONTROL_ENABLED: entry.data[CONF_HA_CONTROL_ENABLED],
        CONF_HA_CONTROL_LABEL: "Downstairs Brilliant",
        CONF_ROOM_OVERRIDES: json.dumps({"area-office": "Office"}),
        CONF_HA_CONTROL_DOMAINS: ["switch", "light"],
        CONF_MAX_MIRRORED_ENTITIES: 75,
    }
    original_update = hass.config_entries.async_update_entry

    with patch.object(
        hass.config_entries,
        "async_update_entry",
        wraps=original_update,
    ) as update_entry:
        result = await hass.config_entries.options.async_configure(
            form["flow_id"],
            submitted,
        )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "reconfigure_successful"
    assert update_entry.call_count == 1
    assert entry.data[CONF_HA_CONTROL_ENABLED] is False
    assert entry.data[CONF_HA_CONTROL_LABEL] == "Downstairs Brilliant"
    assert entry.data[CONF_ROOM_OVERRIDES] == {"area-office": "Office"}
    assert entry.data[CONF_HA_CONTROL_DOMAINS] == ["light", "switch"]
    assert entry.data[CONF_MAX_MIRRORED_ENTITIES] == 75
    assert entry.subentries[panel.subentry_id].as_dict() == original_panel


async def test_installed_fleet_control_enablement_requires_agent_rollout(
    hass: HomeAssistant,
) -> None:
    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    original_data = dict(entry.data)
    form = await _start_fleet_options_step(hass, entry, "ha_control")

    result = await hass.config_entries.options.async_configure(
        form["flow_id"],
        {
            CONF_HA_CONTROL_ENABLED: True,
            CONF_HA_CONTROL_LABEL: entry.data[CONF_HA_CONTROL_LABEL],
            CONF_ROOM_OVERRIDES: "{}",
            CONF_HA_CONTROL_DOMAINS: list(entry.data[CONF_HA_CONTROL_DOMAINS]),
            CONF_MAX_MIRRORED_ENTITIES: entry.data[CONF_MAX_MIRRORED_ENTITIES],
        },
    )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "ha_control_change_requires_agent_rollout"
    assert entry.data == original_data


async def test_fleet_scenes_store_subentry_owner_and_slug_actions_only_on_parent(
    hass: HomeAssistant,
) -> None:
    office = _subentry()
    kitchen = _subentry(
        slug="kitchen",
        fingerprint=_OTHER_FINGERPRINT,
        public_key=_OTHER_PUBLIC_KEY,
        priority=2,
        subentry_id="panel-kitchen",
    )
    entry = _fleet_entry(hass, office, kitchen)
    original_subentries = {
        panel_id: panel.as_dict() for panel_id, panel in entry.subentries.items()
    }
    form = await _start_fleet_options_step(hass, entry, "scenes")
    actions = {
        "kitchen:movie": {
            "domain": "light",
            "service": "turn_on",
            "target": {"entity_id": "light.kitchen"},
            "data": {"brightness_pct": 60},
        }
    }
    original_update = hass.config_entries.async_update_entry

    with patch.object(
        hass.config_entries,
        "async_update_entry",
        wraps=original_update,
    ) as update_entry:
        result = await hass.config_entries.options.async_configure(
            form["flow_id"],
            {
                CONF_SCENE_PANEL: kitchen.subentry_id,
                CONF_SCENE_ACTIONS: json.dumps(actions),
            },
        )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "reconfigure_successful"
    assert update_entry.call_count == 1
    assert entry.data[CONF_SCENE_PANEL] == kitchen.subentry_id
    assert entry.data[CONF_SCENE_ACTIONS] == actions
    assert {
        panel_id: panel.as_dict() for panel_id, panel in entry.subentries.items()
    } == original_subentries


async def test_fleet_scene_owner_rechecks_current_same_fleet_subentries(
    hass: HomeAssistant,
) -> None:
    office = _subentry()
    entry = _fleet_entry(hass, office)
    original_data = dict(entry.data)
    form = await _start_fleet_options_step(hass, entry, "scenes")
    hass.config_entries.async_remove_subentry(entry, office.subentry_id)

    result = await hass.config_entries.options.async_configure(
        form["flow_id"],
        {
            CONF_SCENE_PANEL: office.subentry_id,
            CONF_SCENE_ACTIONS: "{}",
        },
    )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "parent_changed"
    assert entry.data == original_data


async def test_fleet_scenes_fail_closed_if_parent_is_removed(
    hass: HomeAssistant,
) -> None:
    office = _subentry()
    entry = _fleet_entry(hass, office)
    form = await _start_fleet_options_step(hass, entry, "scenes")
    await hass.config_entries.async_remove(entry.entry_id)

    result = await hass.config_entries.options.async_configure(
        form["flow_id"],
        {
            CONF_SCENE_PANEL: office.subentry_id,
            CONF_SCENE_ACTIONS: "{}",
        },
    )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "invalid_parent"


async def test_fleet_defaults_are_owned_once_in_parent_options(
    hass: HomeAssistant,
) -> None:
    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    original_panel = panel.as_dict()
    form = await _start_fleet_options_step(hass, entry, "fleet_defaults")

    result = await hass.config_entries.options.async_configure(
        form["flow_id"],
        {
            OPT_AUTO_REPAIR: False,
            OPT_OFFLINE_GRACE_MINUTES: 20,
            OPT_REPAIR_COOLDOWN_MINUTES: 180,
        },
    )

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["data"] == {
        OPT_AUTO_REPAIR: False,
        OPT_OFFLINE_GRACE_MINUTES: 20,
        OPT_REPAIR_COOLDOWN_MINUTES: 180,
    }
    assert entry.subentries[panel.subentry_id].as_dict() == original_panel


async def test_fleet_advanced_mesh_screen_defers_unsafe_live_reordering(
    hass: HomeAssistant,
) -> None:
    entry = _fleet_entry(hass, _subentry())
    advanced = await _start_fleet_options_step(hass, entry, "advanced")

    assert advanced["type"] is FlowResultType.MENU
    assert tuple(cast(Iterable[str], advanced["menu_options"])) == ("mesh_priorities",)
    result = await hass.config_entries.options.async_configure(
        advanced["flow_id"],
        {"next_step_id": "mesh_priorities"},
    )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "mesh_priority_change_requires_agent_rollout"


async def test_legacy_options_flow_remains_available_in_compatibility_mode(
    hass: HomeAssistant,
) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        version=CONFIG_ENTRY_VERSION,
        data={CONF_ENTRY_KIND: ENTRY_KIND_LEGACY_PENDING_CONSOLIDATION},
    )
    entry.add_to_hass(hass)

    result = await hass.config_entries.options.async_init(entry.entry_id)

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "init"


async def test_panel_reconfigure_menu_is_subentry_scoped(
    hass: HomeAssistant,
) -> None:
    panel = _subentry()
    entry = _fleet_entry(hass, panel)

    result = await hass.config_entries.subentries.async_init(
        (entry.entry_id, SUBENTRY_TYPE_PANEL),
        context={
            "source": "reconfigure",
            "subentry_id": panel.subentry_id,
        },
    )

    assert result["type"] is FlowResultType.MENU
    assert result["step_id"] == "reconfigure"
    assert tuple(cast(Iterable[str], result["menu_options"])) == (
        "rename",
        "address",
        "repair_credentials",
        "components",
        "overrides",
        "rebind",
    )
    assert _ROOT_PASSWORD not in repr(result)
    assert _PUBLIC_KEY not in repr(result)


async def test_panel_rename_preserves_all_runtime_identity(
    hass: HomeAssistant,
) -> None:
    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    stored = entry.subentries[panel.subentry_id]
    original = dict(stored.data)
    form = await _start_panel_reconfigure_step(
        hass,
        entry,
        stored,
        "rename",
    )

    result = await hass.config_entries.subentries.async_configure(
        form["flow_id"],
        {CONF_NAME: "Office Hall"},
    )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "reconfigure_successful"
    assert stored.title == "Office Hall"
    assert stored.data == {**original, CONF_NAME: "Office Hall"}
    for key in (
        CONF_PANEL,
        CONF_IDENTITY_FINGERPRINT,
        CONF_SSH_HOST_KEY,
        CONF_MANAGEMENT_ID,
        CONF_MESH_PRIORITY,
    ):
        assert stored.data[key] == original[key]
    assert stored.unique_id == panel.unique_id


async def test_panel_address_rejects_changed_identity_before_password_authentication(
    hass: HomeAssistant,
) -> None:
    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    stored = entry.subentries[panel.subentry_id]
    original = dict(stored.data)
    form = await _start_panel_reconfigure_step(
        hass,
        entry,
        stored,
        "address",
    )
    inspect = AsyncMock()

    with (
        patch.object(
            flow_gateway,
            "async_fetch_host_identity",
            AsyncMock(return_value=_identity(other=True)),
        ) as fetch_identity,
        patch.object(flow_gateway, "_async_inspect_candidate", inspect),
    ):
        result = await hass.config_entries.subentries.async_configure(
            form["flow_id"],
            {CONF_HOST: "replacement-office.iot.example"},
        )

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "address"
    assert result["errors"] == {"base": "panel_identity_mismatch"}
    fetch_identity.assert_awaited_once_with("replacement-office.iot.example")
    inspect.assert_not_awaited()
    assert stored.data == original
    assert stored.data[CONF_ROOT_PASSWORD] not in repr(result)


async def test_panel_address_authenticates_only_after_existing_identity_matches(
    hass: HomeAssistant,
) -> None:
    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    stored = entry.subentries[panel.subentry_id]
    original = dict(stored.data)
    identity = _identity()
    form = await _start_panel_reconfigure_step(
        hass,
        entry,
        stored,
        "address",
    )
    inspect = AsyncMock(return_value=_facts(identity))

    with (
        patch.object(
            flow_gateway,
            "async_fetch_host_identity",
            AsyncMock(return_value=identity),
        ),
        patch.object(flow_gateway, "_async_inspect_candidate", inspect),
    ):
        result = await hass.config_entries.subentries.async_configure(
            form["flow_id"],
            {CONF_HOST: "replacement-office.iot.example"},
        )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "reconfigure_successful"
    inspect.assert_awaited_once_with(
        hass,
        "replacement-office.iot.example",
        original[CONF_ROOT_PASSWORD],
        identity,
    )
    assert stored.data == {
        **original,
        CONF_HOST: "replacement-office.iot.example",
    }
    assert stored.unique_id == panel.unique_id


async def test_panel_credential_repair_rechecks_identity_and_never_redisplays_secret(
    hass: HomeAssistant,
) -> None:
    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    stored = entry.subentries[panel.subentry_id]
    original = dict(stored.data)
    identity = _identity()
    form = await _start_panel_reconfigure_step(
        hass,
        entry,
        stored,
        "repair_credentials",
    )
    replacement_password = "SECRET-replacement-root-password"
    inspect = AsyncMock(return_value=_facts(identity))

    with (
        patch.object(
            flow_gateway,
            "async_fetch_host_identity",
            AsyncMock(return_value=identity),
        ),
        patch.object(flow_gateway, "_async_inspect_candidate", inspect),
    ):
        result = await hass.config_entries.subentries.async_configure(
            form["flow_id"],
            {CONF_ROOT_PASSWORD: replacement_password},
        )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "reconfigure_successful"
    inspect.assert_awaited_once_with(
        hass,
        original[CONF_HOST],
        replacement_password,
        identity,
    )
    assert stored.data == {
        **original,
        CONF_ROOT_PASSWORD: replacement_password,
    }
    assert replacement_password not in repr(result)
    assert original[CONF_ROOT_PASSWORD] not in repr(form)


async def test_panel_feature_overrides_are_allowlisted_and_preserve_resilience(
    hass: HomeAssistant,
) -> None:
    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    stored = entry.subentries[panel.subentry_id]
    hass.config_entries.async_update_subentry(
        entry,
        stored,
        data={
            **stored.data,
            CONF_FEATURE_OVERRIDES: {
                OPT_AUTO_REPAIR: False,
                OPT_OFFLINE_GRACE_MINUTES: 30,
            },
        },
    )
    form = await _start_panel_reconfigure_step(
        hass,
        entry,
        stored,
        "overrides",
    )
    certificate = "-----BEGIN CERTIFICATE-----\nPUBLIC-CA\n-----END CERTIFICATE-----"

    result = await hass.config_entries.subentries.async_configure(
        form["flow_id"],
        {
            CONF_VOICE_WAKE_WORD: "hey_jarvis",
            CONF_VOICE_HA_HOST: "ha.internal",
            CONF_HUE_CA_CERT: certificate,
        },
    )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "reconfigure_successful"
    assert stored.data[CONF_FEATURE_OVERRIDES] == {
        OPT_AUTO_REPAIR: False,
        OPT_OFFLINE_GRACE_MINUTES: 30,
        CONF_VOICE_WAKE_WORD: "hey_jarvis",
        CONF_VOICE_HA_HOST: "ha.internal",
        CONF_HUE_CA_CERT: certificate,
    }


@pytest.mark.parametrize(
    "submitted",
    (
        {
            CONF_VOICE_WAKE_WORD: "hey_jarvis",
            CONF_VOICE_HA_HOST: "",
            CONF_HUE_CA_CERT: "",
        },
        {
            CONF_VOICE_WAKE_WORD: DEFAULT_VOICE_WAKE_WORD,
            CONF_VOICE_HA_HOST: "ha.internal",
            CONF_HUE_CA_CERT: "",
        },
    ),
    ids=("wake-word", "ha-host"),
)
async def test_active_voice_blocks_override_changes_until_guided_rollout(
    hass: HomeAssistant,
    submitted: dict[str, object],
) -> None:
    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    stored = entry.subentries[panel.subentry_id]
    hass.config_entries.async_update_subentry(
        entry,
        stored,
        data={
            **stored.data,
            CONF_COMPONENTS: {
                **stored.data[CONF_COMPONENTS],
                COMPONENT_VOICE: True,
            },
            CONF_FEATURE_OVERRIDES: {
                CONF_VOICE_WAKE_WORD: DEFAULT_VOICE_WAKE_WORD,
                CONF_VOICE_HA_HOST: "",
                CONF_HUE_CA_CERT: "",
            },
        },
    )
    original = dict(stored.data)
    form = await _start_panel_reconfigure_step(
        hass,
        entry,
        stored,
        "overrides",
    )

    result = await hass.config_entries.subentries.async_configure(
        form["flow_id"],
        submitted,
    )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "feature_override_change_requires_agent_rollout"
    assert stored.data == original


async def test_active_hue_recovery_blocks_ca_change_until_guided_rollout(
    hass: HomeAssistant,
) -> None:
    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    stored = entry.subentries[panel.subentry_id]
    hass.config_entries.async_update_subentry(
        entry,
        stored,
        data={
            **stored.data,
            CONF_COMPONENTS: {
                **stored.data[CONF_COMPONENTS],
                COMPONENT_HUE_CA: True,
            },
            CONF_FEATURE_OVERRIDES: {
                CONF_VOICE_WAKE_WORD: DEFAULT_VOICE_WAKE_WORD,
                CONF_VOICE_HA_HOST: "",
                CONF_HUE_CA_CERT: "",
            },
        },
    )
    original = dict(stored.data)
    form = await _start_panel_reconfigure_step(
        hass,
        entry,
        stored,
        "overrides",
    )

    result = await hass.config_entries.subentries.async_configure(
        form["flow_id"],
        {
            CONF_VOICE_WAKE_WORD: DEFAULT_VOICE_WAKE_WORD,
            CONF_VOICE_HA_HOST: "",
            CONF_HUE_CA_CERT: _CA_PEM,
        },
    )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "feature_override_change_requires_agent_rollout"
    assert stored.data == original


async def test_active_feature_override_noop_is_allowed_without_mutation(
    hass: HomeAssistant,
) -> None:
    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    stored = entry.subentries[panel.subentry_id]
    overrides = {
        CONF_VOICE_WAKE_WORD: "hey_jarvis",
        CONF_VOICE_HA_HOST: "ha.internal",
        CONF_HUE_CA_CERT: _CA_PEM,
    }
    hass.config_entries.async_update_subentry(
        entry,
        stored,
        data={
            **stored.data,
            CONF_COMPONENTS: {
                **stored.data[CONF_COMPONENTS],
                COMPONENT_VOICE: True,
                COMPONENT_HUE_CA: True,
            },
            CONF_FEATURE_OVERRIDES: overrides,
        },
    )
    original = dict(stored.data)
    form = await _start_panel_reconfigure_step(
        hass,
        entry,
        stored,
        "overrides",
    )

    result = await hass.config_entries.subentries.async_configure(
        form["flow_id"],
        overrides,
    )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "reconfigure_successful"
    assert stored.data == original


async def test_panel_override_commit_waits_for_fleet_lock_and_rejects_snapshot_race(
    hass: HomeAssistant,
) -> None:
    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    stored = entry.subentries[panel.subentry_id]
    original = dict(stored.data)
    form = await _start_panel_reconfigure_step(
        hass,
        entry,
        stored,
        "overrides",
    )
    operation_lock = _fleet_lock(hass)
    await operation_lock.acquire()
    commit_task = asyncio.create_task(
        hass.config_entries.subentries.async_configure(
            form["flow_id"],
            {
                CONF_VOICE_WAKE_WORD: "hey_jarvis",
                CONF_VOICE_HA_HOST: "",
                CONF_HUE_CA_CERT: "",
            },
        )
    )
    try:
        await asyncio.sleep(0)
        assert not commit_task.done()
        raced = {
            **original,
            CONF_COMPONENTS: {
                **original[CONF_COMPONENTS],
                COMPONENT_VOICE: True,
            },
        }
        hass.config_entries.async_update_subentry(
            entry,
            stored,
            data=raced,
        )
    finally:
        operation_lock.release()
    result = await asyncio.wait_for(commit_task, timeout=1)

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "parent_changed"
    assert stored.data == raced


async def test_panel_components_routes_to_existing_config_entities(
    hass: HomeAssistant,
) -> None:
    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    result = await _start_panel_reconfigure_step(
        hass,
        entry,
        entry.subentries[panel.subentry_id],
        "components",
    )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "manage_components_with_panel_entities"


async def test_rebind_field_error_preserves_only_valid_replacement_host(
    hass: HomeAssistant,
) -> None:
    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    stored = entry.subentries[panel.subentry_id]
    form = await _start_panel_reconfigure_step(
        hass,
        entry,
        stored,
        "rebind",
    )
    submitted_password = "SECRET-invalid-rebind-password\n"

    result = await hass.config_entries.subentries.async_configure(
        form["flow_id"],
        {
            CONF_HOST: "replacement-panel.iot.example",
            CONF_ROOT_PASSWORD: submitted_password,
        },
    )

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "rebind"
    assert result["errors"] == {CONF_ROOT_PASSWORD: "invalid_value"}
    assert _schema_defaults(result) == {
        CONF_HOST: "replacement-panel.iot.example",
    }
    assert submitted_password not in repr(result)
    assert CONF_ROOT_PASSWORD not in _schema_defaults(result)
    assert stored.data[CONF_HOST] == "office.iot.example"


async def test_explicit_rebind_verifies_new_identity_then_requires_confirmation(
    hass: HomeAssistant,
) -> None:
    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    stored = entry.subentries[panel.subentry_id]
    expected = PanelConfig.from_subentry(stored)
    runtime = FleetManager(hass, entry)
    entry.runtime_data = runtime
    candidate = _identity(other=True)
    replacement_password = "SECRET-rebind-root-password"
    rebind_panel = AsyncMock()
    form = await _start_panel_reconfigure_step(
        hass,
        entry,
        stored,
        "rebind",
    )
    fetch_identity = AsyncMock(side_effect=(candidate, candidate))
    inspect = AsyncMock(return_value=_facts(candidate))

    with (
        patch.object(flow_gateway, "async_fetch_host_identity", fetch_identity),
        patch.object(flow_gateway, "_async_inspect_candidate", inspect),
        patch.object(runtime, "async_rebind_panel", rebind_panel),
    ):
        confirm = await hass.config_entries.subentries.async_configure(
            form["flow_id"],
            {
                CONF_HOST: "replacement-panel.iot.example",
                CONF_ROOT_PASSWORD: replacement_password,
            },
        )
        assert confirm["type"] is FlowResultType.FORM
        assert confirm["step_id"] == "rebind_confirm"
        placeholders = cast(
            Mapping[str, str],
            confirm["description_placeholders"],
        )
        assert placeholders["old_fingerprint"] == _FINGERPRINT
        assert placeholders["new_fingerprint"] == _OTHER_FINGERPRINT
        assert replacement_password not in repr(confirm)
        assert candidate.public_key not in repr(confirm)
        rebind_panel.assert_not_awaited()

        result = await hass.config_entries.subentries.async_configure(
            confirm["flow_id"],
            {"confirm": True},
        )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "reconfigure_successful"
    assert [awaited.args for awaited in fetch_identity.await_args_list] == [
        ("replacement-panel.iot.example",),
        ("replacement-panel.iot.example",),
    ]
    inspect.assert_awaited_once_with(
        hass,
        "replacement-panel.iot.example",
        replacement_password,
        candidate,
    )
    rebind_panel.assert_awaited_once_with(
        panel.subentry_id,
        expected,
        host="replacement-panel.iot.example",
        root_password=replacement_password,
        candidate=candidate,
    )


async def test_explicit_rebind_rejects_current_or_duplicate_identity_before_auth(
    hass: HomeAssistant,
) -> None:
    office = _subentry()
    kitchen = _subentry(
        slug="kitchen",
        fingerprint=_OTHER_FINGERPRINT,
        public_key=_OTHER_PUBLIC_KEY,
        priority=2,
        subentry_id="panel-kitchen",
    )
    entry = _fleet_entry(hass, office, kitchen)

    for identity, expected_type, expected_code in (
        (_identity(), FlowResultType.FORM, "rebind_identity_unchanged"),
        (_identity(other=True), FlowResultType.ABORT, "already_configured"),
    ):
        form = await _start_panel_reconfigure_step(
            hass,
            entry,
            entry.subentries[office.subentry_id],
            "rebind",
        )
        inspect = AsyncMock()
        with (
            patch.object(
                flow_gateway,
                "async_fetch_host_identity",
                AsyncMock(return_value=identity),
            ),
            patch.object(flow_gateway, "_async_inspect_candidate", inspect),
        ):
            result = await hass.config_entries.subentries.async_configure(
                form["flow_id"],
                {
                    CONF_HOST: "replacement-panel.iot.example",
                    CONF_ROOT_PASSWORD: "SECRET-unused-rebind-password",
                },
            )

        assert result["type"] is expected_type
        if expected_type is FlowResultType.FORM:
            assert result["errors"] == {"base": expected_code}
        else:
            assert result["reason"] == expected_code
            assert result["description_placeholders"] == {
                "subentry_id": kitchen.subentry_id,
                "panel_name": kitchen.title,
            }
        inspect.assert_not_awaited()
        assert "SECRET-unused-rebind-password" not in repr(result)


async def test_explicit_rebind_allows_target_reserved_management_identity(
    hass: HomeAssistant,
) -> None:
    """A logical panel may return to the physical identity it continues to own."""
    rebound = _subentry(
        fingerprint=_OTHER_FINGERPRINT,
        public_key=_OTHER_PUBLIC_KEY,
        management_id=_FINGERPRINT,
    )
    entry = _fleet_entry(hass, rebound)
    stored = entry.subentries[rebound.subentry_id]
    original = dict(stored.data)
    candidate = _identity()
    form = await _start_panel_reconfigure_step(
        hass,
        entry,
        stored,
        "rebind",
    )

    with (
        patch.object(
            flow_gateway,
            "async_fetch_host_identity",
            AsyncMock(return_value=candidate),
        ),
        patch.object(
            flow_gateway,
            "_async_inspect_candidate",
            AsyncMock(return_value=_facts(candidate)),
        ),
    ):
        result = await hass.config_entries.subentries.async_configure(
            form["flow_id"],
            {
                CONF_HOST: "restored-office.iot.example",
                CONF_ROOT_PASSWORD: "SECRET-restored-root-password",
            },
        )

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "rebind_confirm"
    placeholders = cast(Mapping[str, str], result["description_placeholders"])
    assert placeholders["old_fingerprint"] == _OTHER_FINGERPRINT
    assert placeholders["new_fingerprint"] == _FINGERPRINT
    assert stored.data == original
    assert "SECRET-restored-root-password" not in repr(result)
    assert candidate.public_key not in repr(result)


async def test_explicit_rebind_rejects_another_panels_reserved_management_identity(
    hass: HomeAssistant,
) -> None:
    """Target exclusion must not release an identity reserved by another owner."""
    office = _subentry(
        fingerprint=_OTHER_FINGERPRINT,
        public_key=_OTHER_PUBLIC_KEY,
        management_id=_OTHER_FINGERPRINT,
    )
    kitchen = _subentry(
        slug="kitchen",
        fingerprint=_THIRD_FINGERPRINT,
        public_key=_THIRD_PUBLIC_KEY,
        priority=2,
        subentry_id="panel-kitchen",
        management_id=_FINGERPRINT,
    )
    entry = _fleet_entry(hass, office, kitchen)
    stored = entry.subentries[office.subentry_id]
    form = await _start_panel_reconfigure_step(
        hass,
        entry,
        stored,
        "rebind",
    )

    with (
        patch.object(
            flow_gateway,
            "async_fetch_host_identity",
            AsyncMock(return_value=_identity()),
        ),
        patch.object(flow_gateway, "_async_inspect_candidate", AsyncMock()),
    ):
        result = await hass.config_entries.subentries.async_configure(
            form["flow_id"],
            {
                CONF_HOST: "reserved-kitchen-identity.iot.example",
                CONF_ROOT_PASSWORD: "SECRET-unused-rebind-password",
            },
        )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "already_configured"
    assert result["description_placeholders"] == {
        "subentry_id": kitchen.subentry_id,
        "panel_name": kitchen.title,
    }
    assert "SECRET-unused-rebind-password" not in repr(result)


async def test_explicit_rebind_requires_positive_confirmation_without_mutation(
    hass: HomeAssistant,
) -> None:
    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    stored = entry.subentries[panel.subentry_id]
    runtime = FleetManager(hass, entry)
    entry.runtime_data = runtime
    candidate = _identity(other=True)
    rebind_panel = AsyncMock()
    form = await _start_panel_reconfigure_step(
        hass,
        entry,
        stored,
        "rebind",
    )

    with (
        patch.object(
            flow_gateway,
            "async_fetch_host_identity",
            AsyncMock(return_value=candidate),
        ),
        patch.object(
            flow_gateway,
            "_async_inspect_candidate",
            AsyncMock(return_value=_facts(candidate)),
        ),
        patch.object(runtime, "async_rebind_panel", rebind_panel),
    ):
        confirm = await hass.config_entries.subentries.async_configure(
            form["flow_id"],
            {
                CONF_HOST: "replacement-panel.iot.example",
                CONF_ROOT_PASSWORD: "SECRET-rebind-root-password",
            },
        )
        result = await hass.config_entries.subentries.async_configure(
            confirm["flow_id"],
            {"confirm": False},
        )

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "rebind_confirm"
    assert result["errors"] == {"confirm": "confirmation_required"}
    rebind_panel.assert_not_awaited()
    assert stored.data[CONF_IDENTITY_FINGERPRINT] == _FINGERPRINT


async def test_explicit_rebind_rechecks_identity_at_confirmation(
    hass: HomeAssistant,
) -> None:
    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    stored = entry.subentries[panel.subentry_id]
    runtime = FleetManager(hass, entry)
    entry.runtime_data = runtime
    candidate = _identity(other=True)
    rebind_panel = AsyncMock()
    fetch_identity = AsyncMock(side_effect=(candidate, _identity()))
    form = await _start_panel_reconfigure_step(
        hass,
        entry,
        stored,
        "rebind",
    )

    with (
        patch.object(flow_gateway, "async_fetch_host_identity", fetch_identity),
        patch.object(
            flow_gateway,
            "_async_inspect_candidate",
            AsyncMock(return_value=_facts(candidate)),
        ),
        patch.object(runtime, "async_rebind_panel", rebind_panel),
    ):
        confirm = await hass.config_entries.subentries.async_configure(
            form["flow_id"],
            {
                CONF_HOST: "replacement-panel.iot.example",
                CONF_ROOT_PASSWORD: "SECRET-rebind-root-password",
            },
        )
        result = await hass.config_entries.subentries.async_configure(
            confirm["flow_id"],
            {"confirm": True},
        )

    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "rebind_confirm"
    assert result["errors"] == {"base": "rebind_identity_changed"}
    rebind_panel.assert_not_awaited()
    assert stored.data[CONF_IDENTITY_FINGERPRINT] == _FINGERPRINT
    assert "SECRET-rebind-root-password" not in repr(result)


@pytest.mark.parametrize(
    ("manager_code", "reason"),
    (
        ("panel_rebind_identity_changed", "rebind_identity_changed"),
        ("panel_rebind_identity_unreachable", "cannot_connect"),
        (
            "panel_rebind_blocked_by_panel_onboarding",
            "rebind_blocked_by_panel_onboarding",
        ),
    ),
)
async def test_explicit_rebind_maps_locked_manager_failures(
    hass: HomeAssistant,
    manager_code: str,
    reason: str,
) -> None:
    panel = _subentry()
    entry = _fleet_entry(hass, panel)
    stored = entry.subentries[panel.subentry_id]
    runtime = FleetManager(hass, entry)
    entry.runtime_data = runtime
    candidate = _identity(other=True)
    form = await _start_panel_reconfigure_step(
        hass,
        entry,
        stored,
        "rebind",
    )
    rebind_panel = AsyncMock(side_effect=EntryDataError(manager_code))

    with (
        patch.object(
            flow_gateway,
            "async_fetch_host_identity",
            AsyncMock(side_effect=(candidate, candidate)),
        ),
        patch.object(
            flow_gateway,
            "_async_inspect_candidate",
            AsyncMock(return_value=_facts(candidate)),
        ),
        patch.object(runtime, "async_rebind_panel", rebind_panel),
    ):
        confirm = await hass.config_entries.subentries.async_configure(
            form["flow_id"],
            {
                CONF_HOST: "replacement-panel.iot.example",
                CONF_ROOT_PASSWORD: "SECRET-rebind-root-password",
            },
        )
        result = await hass.config_entries.subentries.async_configure(
            confirm["flow_id"],
            {"confirm": True},
        )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == reason
    assert stored.data[CONF_IDENTITY_FINGERPRINT] == _FINGERPRINT
    assert "SECRET-rebind-root-password" not in repr(result)


async def test_fleet_reconfigure_aborts_without_indexing_panel_secrets(
    hass: HomeAssistant,
) -> None:
    """Task 5 keeps fleet reconfigure outside the legacy panel-only boundary."""
    entry = _fleet_entry(hass)

    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={
            "source": "reconfigure",
            "entry_id": entry.entry_id,
        },
    )

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "reconfigure_not_supported"
    assert _BROKER_PASSWORD not in repr(result)


async def test_legacy_reconfigure_passwords_are_masked_and_never_redisplayed(
    hass: HomeAssistant,
) -> None:
    """Stored and just-submitted credentials never become schema defaults."""
    entry = _legacy_entry(hass)
    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={
            "source": "reconfigure",
            "entry_id": entry.entry_id,
        },
    )

    assert result["type"] is FlowResultType.FORM
    for key in (CONF_ROOT_PASSWORD, CONF_MQTT_PASSWORD):
        selector = _schema_validator(result, key)
        assert isinstance(selector, TextSelector)
        assert selector.config["type"] == TextSelectorType.PASSWORD
    assert _ROOT_PASSWORD not in repr(result)
    assert _BROKER_PASSWORD not in repr(result)

    submitted = {
        **_schema_defaults(result),
        CONF_ROOT_PASSWORD: "SECRET-new-root-password",
        CONF_MQTT_PASSWORD: "SECRET-new-mqtt-password",
    }
    with patch.object(
        flow_gateway,
        "_apply_config",
        side_effect=OSError("transient connection failure"),
    ):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            submitted,
        )

    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": "cannot_connect"}
    assert "SECRET-new-root-password" not in repr(result)
    assert "SECRET-new-mqtt-password" not in repr(result)
    assert _ROOT_PASSWORD not in repr(result)
    assert _BROKER_PASSWORD not in repr(result)
    assert CONF_ROOT_PASSWORD not in _schema_defaults(result)
    assert CONF_MQTT_PASSWORD not in _schema_defaults(result)
