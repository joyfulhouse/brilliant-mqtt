"""Shared failure states, placeholders, and fleet-entry invariants for flows."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

import voluptuous as vol
from homeassistant.config_entries import (
    ConfigEntry,
    ConfigFlowResult,
    ConfigSubentry,
    SubentryFlowResult,
)
from homeassistant.const import CONF_NAME
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import issue_registry as ir

from ..const import (
    COMPONENT_BRIDGE,
    COMPONENT_BUS_WATCHDOG,
    COMPONENT_WIFI_WATCHDOG,
    CONF_COMPONENTS,
    CONF_ENTRY_KIND,
    CONF_FEATURE_OVERRIDES,
    CONF_HOST,
    CONF_IDENTITY_FINGERPRINT,
    CONF_MANAGEMENT_ID,
    CONF_MESH_PRIORITY,
    CONF_PANEL,
    CONF_PROVISIONING_TRANSACTION_ID,
    CONF_ROOT_PASSWORD,
    CONF_SCHEMA_VERSION,
    CONF_SSH_HOST_KEY,
    CONF_SSH_USERNAME,
    CONFIG_ENTRY_VERSION,
    DOMAIN,
    ENTRY_KIND_FLEET,
    FLEET_UNIQUE_ID,
    SUBENTRY_TYPE_PANEL,
)
from ..errors import OperationError
from ..panel_inspection import PanelFacts
from ..panel_provisioner import (
    PanelInstallRequest,
    PanelProvisioningError,
    ProvisionedPanel,
    ProvisioningProgressStage,
)
from ..shell import HostIdentity

_FLEET_STORAGE_ISSUE_REASON = (
    "Home Assistant could not confirm the Brilliant MQTT fleet in durable storage. "
    "Retry Add panel after storage is available."
)

type _OnboardingResult = ConfigFlowResult | SubentryFlowResult

_CORE_COMPONENTS = (
    COMPONENT_BRIDGE,
    COMPONENT_WIFI_WATCHDOG,
    COMPONENT_BUS_WATCHDOG,
)
_PROVISIONING_STAGE_ORDER = tuple(ProvisioningProgressStage)
_DOCUMENTATION_ROOT = "https://github.com/joyfulhouse/brilliant-mqtt/blob/main/docs"
_MQTT_DOCUMENTATION_URL = f"{_DOCUMENTATION_ROOT}/install/mqtt-broker.md"
_PANEL_ONBOARDING_DOCUMENTATION_URL = (
    f"{_DOCUMENTATION_ROOT}/ha-integration.md#panel-onboarding-errors"
)
_DOCUMENTATION_SLUG = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")
_FAILURE_DOCUMENTATION_SLUGS = {
    "invalid_broker_profile": "mqtt-broker-profile",
    "broker_validation_failed": "mqtt-broker-validation-failed",
}


def _documentation_url(slug: str) -> str:
    if _DOCUMENTATION_SLUG.fullmatch(slug) and slug.startswith("mqtt-"):
        return f"{_MQTT_DOCUMENTATION_URL}#{slug}"
    return _PANEL_ONBOARDING_DOCUMENTATION_URL


@dataclass(frozen=True, slots=True)
class _FlowFailure:
    """Allowlisted UI failure state which cannot retain a raw exception."""

    code: str
    documentation_slug: str
    stage: str

    def placeholders(self) -> dict[str, str]:
        return {
            "documentation_slug": self.documentation_slug,
            "documentation_url": _documentation_url(self.documentation_slug),
            "stage": self.stage,
        }


@dataclass(frozen=True, slots=True)
class _DuplicatePanel:
    subentry_id: str
    title: str


@dataclass(frozen=True, slots=True)
class _OnboardingAbort:
    reason: str
    description_placeholders: Mapping[str, str] | None = None


class _FlowSurface(Protocol):
    """Common Home Assistant flow methods used by panel onboarding."""

    hass: HomeAssistant
    context: Mapping[str, Any]

    def async_show_form(
        self,
        *,
        step_id: str | None = None,
        data_schema: vol.Schema | None = None,
        errors: dict[str, str] | None = None,
        description_placeholders: Mapping[str, str] | None = None,
    ) -> _OnboardingResult: ...

    def async_show_progress(
        self,
        *,
        step_id: str | None = None,
        progress_action: str,
        description_placeholders: Mapping[str, str] | None = None,
        progress_task: asyncio.Task[Any] | None = None,
    ) -> _OnboardingResult: ...

    def async_show_progress_done(
        self,
        *,
        next_step_id: str,
    ) -> _OnboardingResult: ...

    def async_abort(
        self,
        *,
        reason: str,
        description_placeholders: Mapping[str, str] | None = None,
    ) -> _OnboardingResult: ...

    def async_update_progress(self, progress: float) -> None: ...


@callback
def _same_entry_panel_add_flow_active(
    hass: HomeAssistant,
    entry_id: str,
) -> bool:
    """Return whether an Add panel flow can still start provisioning for this fleet."""
    for progress in hass.config_entries.subentries.async_progress(
        include_uninitialized=True,
    ):
        if not isinstance(progress, Mapping) or progress.get("handler") != (
            entry_id,
            SUBENTRY_TYPE_PANEL,
        ):
            continue
        context = progress.get("context")
        if not isinstance(context, Mapping) or context.get("source") != "reconfigure":
            return True
    return False


def _fleet_storage_issue_id(entry_id: str) -> str:
    return f"fleet_storage_{entry_id}"


@callback
def _async_create_fleet_storage_issue(
    hass: HomeAssistant,
    entry_id: str,
) -> None:
    ir.async_create_issue(
        hass,
        DOMAIN,
        _fleet_storage_issue_id(entry_id),
        is_fixable=False,
        severity=ir.IssueSeverity.ERROR,
        translation_key="needs_attention",
        translation_placeholders={
            "panel": "Brilliant MQTT fleet",
            "reason": _FLEET_STORAGE_ISSUE_REASON,
        },
        learn_more_url=(
            "https://github.com/joyfulhouse/brilliant-mqtt/blob/main/docs/ha-integration.md"
        ),
    )


def _operation_failure(error: OperationError) -> _FlowFailure:
    return _FlowFailure(
        code=error.code,
        documentation_slug=error.documentation_slug,
        stage=error.stage.value,
    )


def _panel_failure(code: str, *, stage: str = "panel") -> _FlowFailure:
    return _FlowFailure(
        code=code,
        documentation_slug=_FAILURE_DOCUMENTATION_SLUGS.get(
            code,
            f"panel-{code.replace('_', '-')}",
        ),
        stage=stage,
    )


def _provisioning_failure(error: PanelProvisioningError) -> _FlowFailure:
    if error.detail is not None:
        return _FlowFailure(
            code=error.detail.code,
            documentation_slug=error.detail.documentation_slug,
            stage=error.detail.stage.value,
        )
    return _panel_failure(error.code, stage="provisioning")


def _panel_subentries(entry: ConfigEntry[Any] | None) -> tuple[ConfigSubentry, ...]:
    if entry is None:
        return ()
    return tuple(
        subentry
        for subentry in entry.subentries.values()
        if subentry.subentry_type == SUBENTRY_TYPE_PANEL
    )


def _is_exact_fleet_parent(entry: ConfigEntry[Any]) -> bool:
    """Accept Add panel only for the singleton versioned fleet entry."""
    return (
        entry.domain == DOMAIN
        and entry.unique_id == FLEET_UNIQUE_ID
        and entry.version == CONFIG_ENTRY_VERSION
        and entry.data.get(CONF_ENTRY_KIND) == ENTRY_KIND_FLEET
        and entry.data.get(CONF_SCHEMA_VERSION) == CONFIG_ENTRY_VERSION
    )


def _duplicate_panel(
    entry: ConfigEntry[Any] | None,
    fingerprint: str,
    *,
    exclude_subentry_id: str | None = None,
) -> _DuplicatePanel | None:
    for subentry in _panel_subentries(entry):
        if subentry.subentry_id == exclude_subentry_id:
            continue
        if (
            subentry.unique_id == fingerprint
            or subentry.data.get(CONF_IDENTITY_FINGERPRINT) == fingerprint
            or subentry.data.get(CONF_MANAGEMENT_ID) == fingerprint
        ):
            return _DuplicatePanel(
                subentry_id=subentry.subentry_id,
                title=subentry.title,
            )
    return None


def _suggested_panel_name(facts: PanelFacts) -> str:
    suggested = facts.hostname.replace("-", " ").replace("_", " ").strip().title()
    return suggested or "Brilliant Panel"


def _facts_placeholders(facts: PanelFacts) -> dict[str, str]:
    return {
        "fingerprint": facts.fingerprint,
        "hostname": facts.hostname,
        "model": facts.model,
        "architecture": facts.architecture,
        "firmware": facts.firmware,
        "python_version": facts.python_version,
        "init_system": facts.init_system,
        "available_bytes": str(facts.available_bytes),
        "available_memory_bytes": str(facts.available_memory_bytes),
        "installed_agent_version": facts.installed_agent_version or "not_installed",
        "active_services": ", ".join(facts.active_services) or "none",
        "conflicting_services": ", ".join(facts.conflicting_services) or "none",
    }


def _provisioned_matches_request(
    provisioned: ProvisionedPanel,
    request: PanelInstallRequest,
    identity: HostIdentity,
) -> bool:
    """Defend the storage handoff against a mismatched provisioner result."""
    data = provisioned.panel_data
    expected_components = {
        component: component in request.selected_components for component in _CORE_COMPONENTS
    }
    return (
        provisioned.identity == identity
        and provisioned.facts.fingerprint == identity.fingerprint
        and data.get(CONF_IDENTITY_FINGERPRINT) == identity.fingerprint
        and data.get(CONF_SSH_HOST_KEY) == identity.public_key
        and data.get(CONF_HOST) == request.host
        and data.get(CONF_SSH_USERNAME) == request.ssh_username
        and data.get(CONF_ROOT_PASSWORD) == request.root_password
        and data.get(CONF_NAME) == request.display_name
        and data.get(CONF_PANEL) == request.slug
        and data.get(CONF_MANAGEMENT_ID) == identity.fingerprint
        and data.get(CONF_COMPONENTS) == expected_components
        and data.get(CONF_FEATURE_OVERRIDES) == dict(request.feature_overrides)
        and data.get(CONF_MESH_PRIORITY) == request.mesh_priority
        and data.get(CONF_PROVISIONING_TRANSACTION_ID) == str(provisioned.transaction_id)
    )
