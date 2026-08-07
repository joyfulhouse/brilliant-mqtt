"""Add panel and day-two panel actions as a fleet subentry flow."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, cast

import asyncssh
import voluptuous as vol
from homeassistant.config_entries import (
    ConfigEntry,
    ConfigSubentry,
    ConfigSubentryFlow,
    SubentryFlowResult,
    UnknownEntry,
    UnknownSubEntry,
)
from homeassistant.const import CONF_NAME
from homeassistant.core import callback

from .. import _fleet_lock
from ..const import (
    COMPONENT_HUE_CA,
    COMPONENT_VOICE,
    CONF_FEATURE_OVERRIDES,
    CONF_HOST,
    CONF_HUE_CA_CERT,
    CONF_MESH_PRIORITY,
    CONF_PANEL,
    CONF_ROOT_PASSWORD,
    CONF_VOICE_HA_HOST,
    CONF_VOICE_WAKE_WORD,
    DEFAULT_VOICE_WAKE_WORD,
    SUBENTRY_TYPE_PANEL,
)
from ..entry_data import EntryDataError, FleetConfig, PanelConfig
from ..panel_inspection import PanelCompatibilityError, PanelFacts
from ..panel_provisioner import PanelProvisioningError, ProvisionedPanel
from ..shell import HostIdentity, PanelIdentityError
from . import gateway
from .onboarding import _PanelOnboardingMixin
from .schemas import (
    FlowInputError,
    normalize_panel_address_input,
    normalize_panel_feature_overrides_input,
    normalize_panel_rebind_input,
    normalize_panel_rename_input,
    normalize_panel_ssh_credentials_input,
    panel_address_schema,
    panel_connect_form_source,
    panel_feature_overrides_schema,
    panel_rebind_schema,
    panel_rename_schema,
    panel_ssh_credentials_schema,
)
from .support import (
    _duplicate_panel,
    _facts_placeholders,
    _FlowFailure,
    _is_exact_fleet_parent,
    _OnboardingAbort,
    _panel_failure,
    _panel_subentries,
)

_LOGGER = logging.getLogger(__name__)


class PanelSubentryFlow(ConfigSubentryFlow, _PanelOnboardingMixin):
    """Add one panel while inheriting the exact parent fleet configuration."""

    def __init__(self) -> None:
        self._init_panel_onboarding()
        self._reconfigure_expected: PanelConfig | None = None
        self._reconfigure_host: str | None = None
        self._rebind_identity: HostIdentity | None = None
        self._rebind_facts: PanelFacts | None = None
        self._rebind_password: str | None = None

    @callback
    def async_remove(self) -> None:
        """Settle an uncommitted panel when Home Assistant removes this flow."""
        self._remove_panel_onboarding()
        super().async_remove()

    def _onboarding_parent_entry(self) -> ConfigEntry[Any] | None:
        try:
            entry = self._get_entry()
        except UnknownEntry:
            return None
        return entry if _is_exact_fleet_parent(entry) else None

    def _onboarding_fleet(self) -> FleetConfig:
        entry = self._onboarding_parent_entry()
        if entry is None:
            raise PanelProvisioningError("invalid_provisioning_dependency")
        return FleetConfig.from_entry(entry)

    def _onboarding_state_abort(
        self,
        *,
        before_create: bool,
    ) -> _OnboardingAbort | None:
        entry = self._onboarding_parent_entry()
        if entry is None:
            return _OnboardingAbort(reason="invalid_parent")
        try:
            current_fleet = FleetConfig.from_entry(entry)
        except EntryDataError:
            return _OnboardingAbort(reason="invalid_parent")

        if before_create:
            if self._provision_fleet is None:
                return _OnboardingAbort(reason="invalid_flow_state")
            if current_fleet != self._provision_fleet:
                return _OnboardingAbort(reason="parent_changed")

        identity = self._panel_identity
        if identity is not None and (duplicate := _duplicate_panel(entry, identity.fingerprint)):
            return _OnboardingAbort(
                reason="already_configured",
                description_placeholders={
                    "subentry_id": duplicate.subentry_id,
                    "panel_name": duplicate.title,
                },
            )

        if not before_create:
            return None
        request = self._install_request
        if request is None:
            return _OnboardingAbort(reason="invalid_flow_state")
        for subentry in _panel_subentries(entry):
            if subentry.data.get(CONF_PANEL) == request.slug:
                return _OnboardingAbort(
                    reason="panel_slug_conflict",
                    description_placeholders={
                        "panel": request.slug,
                        "subentry_id": subentry.subentry_id,
                    },
                )
            if subentry.data.get(CONF_MESH_PRIORITY) == request.mesh_priority:
                return _OnboardingAbort(
                    reason="mesh_priority_conflict",
                    description_placeholders={
                        "mesh_priority": str(request.mesh_priority),
                        "subentry_id": subentry.subentry_id,
                    },
                )
        return None

    def _finish_panel_onboarding(
        self,
        provisioned: ProvisionedPanel,
    ) -> SubentryFlowResult:
        return self.async_create_entry(
            title=str(provisioned.panel_data[CONF_NAME]),
            data=provisioned.panel_data.as_dict(),
            unique_id=provisioned.identity.fingerprint,
        )

    async def async_step_user(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> SubentryFlowResult:
        """Collect only panel address/password for an exact fleet parent."""
        if self._onboarding_parent_entry() is None:
            return self.async_abort(reason="invalid_parent")
        return cast(
            SubentryFlowResult,
            await self.async_step_panel_connect(user_input),
        )

    async def async_step_reconfigure(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> SubentryFlowResult:
        """Expose focused day-two actions for one exact panel subentry."""
        del user_input
        if self._reconfigure_target() is None:
            return self.async_abort(reason="invalid_parent")
        return self.async_show_menu(
            step_id="reconfigure",
            menu_options=(
                "rename",
                "address",
                "repair_credentials",
                "components",
                "overrides",
                "rebind",
            ),
        )

    def _reconfigure_target(
        self,
    ) -> tuple[ConfigEntry[Any], ConfigSubentry, PanelConfig] | None:
        """Return one exact, validated same-fleet reconfigure target."""
        entry = self._onboarding_parent_entry()
        if entry is None:
            return None
        try:
            subentry = self._get_reconfigure_subentry()
        except (UnknownEntry, UnknownSubEntry):
            return None
        if subentry.subentry_type != SUBENTRY_TYPE_PANEL:
            return None
        try:
            panel = PanelConfig.from_subentry(subentry)
        except EntryDataError:
            return None
        return entry, subentry, panel

    def _capture_or_compare_reconfigure(
        self,
        current: PanelConfig,
    ) -> bool:
        """Capture the form snapshot once and reject concurrent panel changes."""
        if self._reconfigure_expected is None:
            self._reconfigure_expected = current
            return True
        return self._reconfigure_expected == current

    async def _async_verify_existing_panel(
        self,
        current: PanelConfig,
        *,
        host: str,
        root_password: str,
    ) -> _FlowFailure | None:
        """Prove the pinned identity before offering a password to the candidate."""
        try:
            identity = await gateway.async_fetch_host_identity(host)
        except asyncio.CancelledError:
            raise
        except PanelIdentityError as error:
            return _panel_failure(error.code, stage="panel_identity")
        except (OSError, asyncssh.Error):
            return _panel_failure("cannot_connect", stage="panel_identity")
        except Exception:
            return _panel_failure("inspection_failed", stage="panel_identity")

        if (
            identity.fingerprint != current.identity_fingerprint
            or identity.public_key != current.ssh_host_key
        ):
            return _panel_failure(
                "panel_identity_mismatch",
                stage="panel_identity",
            )
        try:
            await gateway._async_inspect_candidate(
                self.hass,
                host,
                root_password,
                identity,
            )
        except asyncio.CancelledError:
            raise
        except asyncssh.HostKeyNotVerifiable:
            return _panel_failure("host_key_changed", stage="panel_ssh")
        except asyncssh.PermissionDenied:
            return _panel_failure(
                "panel_authentication_failed",
                stage="panel_ssh",
            )
        except PanelIdentityError as error:
            return _panel_failure(error.code, stage="panel_identity")
        except PanelCompatibilityError as error:
            return _panel_failure(error.code, stage="panel_inspection")
        except (OSError, asyncssh.Error):
            return _panel_failure("cannot_connect", stage="panel_ssh")
        except Exception:
            return _panel_failure("inspection_failed", stage="panel_inspection")
        return None

    async def async_step_rename(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> SubentryFlowResult:
        """Change only the human-facing panel title and name."""
        target = self._reconfigure_target()
        if target is None:
            return self.async_abort(reason="invalid_parent")
        entry, subentry, current = target
        if not self._capture_or_compare_reconfigure(current):
            return self.async_abort(reason="parent_changed")
        errors: dict[str, str] = {}
        if user_input is not None:
            try:
                normalized = normalize_panel_rename_input(user_input)
            except FlowInputError as error:
                errors = dict(error.errors)
            else:
                target = self._reconfigure_target()
                if target is None:
                    return self.async_abort(reason="invalid_parent")
                entry, subentry, current = target
                if not self._capture_or_compare_reconfigure(current):
                    return self.async_abort(reason="parent_changed")
                name = normalized[CONF_NAME]
                return self.async_update_and_abort(
                    entry,
                    subentry,
                    title=name,
                    data={**subentry.data, CONF_NAME: name},
                )
        return self.async_show_form(
            step_id="rename",
            data_schema=panel_rename_schema(subentry.data),
            errors=errors,
        )

    async def async_step_address(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> SubentryFlowResult:
        """Move a panel address only after its existing identity is proven."""
        target = self._reconfigure_target()
        if target is None:
            return self.async_abort(reason="invalid_parent")
        entry, subentry, current = target
        if not self._capture_or_compare_reconfigure(current):
            return self.async_abort(reason="parent_changed")
        errors: dict[str, str] = {}
        failure: _FlowFailure | None = None
        if user_input is not None:
            try:
                normalized = normalize_panel_address_input(user_input)
            except FlowInputError as error:
                errors = dict(error.errors)
            else:
                host = normalized[CONF_HOST]
                self._reconfigure_host = host
                failure = await self._async_verify_existing_panel(
                    current,
                    host=host,
                    root_password=current.root_password,
                )
                if failure is None:
                    target = self._reconfigure_target()
                    if target is None:
                        return self.async_abort(reason="invalid_parent")
                    entry, subentry, latest = target
                    if not self._capture_or_compare_reconfigure(latest):
                        return self.async_abort(reason="parent_changed")
                    return self.async_update_and_abort(
                        entry,
                        subentry,
                        data={**subentry.data, CONF_HOST: host},
                    )
        source = {CONF_HOST: self._reconfigure_host or current.host}
        return self.async_show_form(
            step_id="address",
            data_schema=panel_address_schema(source),
            errors=errors or ({"base": failure.code} if failure is not None else {}),
            description_placeholders=(failure.placeholders() if failure is not None else None),
        )

    async def async_step_repair_credentials(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> SubentryFlowResult:
        """Replace the root credential only after re-proving pinned identity."""
        target = self._reconfigure_target()
        if target is None:
            return self.async_abort(reason="invalid_parent")
        entry, subentry, current = target
        if not self._capture_or_compare_reconfigure(current):
            return self.async_abort(reason="parent_changed")
        errors: dict[str, str] = {}
        failure: _FlowFailure | None = None
        if user_input is not None:
            try:
                normalized = normalize_panel_ssh_credentials_input(user_input)
            except FlowInputError as error:
                errors = dict(error.errors)
            else:
                password = normalized[CONF_ROOT_PASSWORD]
                failure = await self._async_verify_existing_panel(
                    current,
                    host=current.host,
                    root_password=password,
                )
                if failure is None:
                    target = self._reconfigure_target()
                    if target is None:
                        return self.async_abort(reason="invalid_parent")
                    entry, subentry, latest = target
                    if not self._capture_or_compare_reconfigure(latest):
                        return self.async_abort(reason="parent_changed")
                    return self.async_update_and_abort(
                        entry,
                        subentry,
                        data={
                            **subentry.data,
                            CONF_ROOT_PASSWORD: password,
                        },
                    )
        return self.async_show_form(
            step_id="repair_credentials",
            data_schema=panel_ssh_credentials_schema(),
            errors=errors or ({"base": failure.code} if failure is not None else {}),
            description_placeholders=(failure.placeholders() if failure is not None else None),
        )

    async def async_step_overrides(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> SubentryFlowResult:
        """Update only allowlisted panel feature values."""
        target = self._reconfigure_target()
        if target is None:
            return self.async_abort(reason="invalid_parent")
        entry, subentry, current = target
        if not self._capture_or_compare_reconfigure(current):
            return self.async_abort(reason="parent_changed")
        errors: dict[str, str] = {}
        if user_input is not None:
            try:
                normalized = normalize_panel_feature_overrides_input(user_input)
            except FlowInputError as error:
                errors = dict(error.errors)
            else:
                async with _fleet_lock(self.hass):
                    target = self._reconfigure_target()
                    if target is None:
                        return self.async_abort(reason="invalid_parent")
                    entry, subentry, latest = target
                    if not self._capture_or_compare_reconfigure(latest):
                        return self.async_abort(reason="parent_changed")

                    current_wake_word = latest.feature_overrides.get(
                        CONF_VOICE_WAKE_WORD,
                        DEFAULT_VOICE_WAKE_WORD,
                    )
                    current_ha_host = latest.feature_overrides.get(
                        CONF_VOICE_HA_HOST,
                        "",
                    )
                    current_hue_ca = latest.feature_overrides.get(
                        CONF_HUE_CA_CERT,
                        "",
                    )
                    voice_changed = (
                        normalized[CONF_VOICE_WAKE_WORD] != current_wake_word
                        or normalized[CONF_VOICE_HA_HOST] != current_ha_host
                    )
                    hue_ca_changed = normalized[CONF_HUE_CA_CERT] != current_hue_ca
                    if (latest.components.get(COMPONENT_VOICE) is True and voice_changed) or (
                        latest.components.get(COMPONENT_HUE_CA) is True and hue_ca_changed
                    ):
                        return self.async_abort(
                            reason="feature_override_change_requires_agent_rollout",
                        )

                    overrides = {
                        **latest.feature_overrides,
                        **normalized,
                    }
                    if overrides == latest.feature_overrides:
                        return self.async_abort(reason="reconfigure_successful")
                    return self.async_update_and_abort(
                        entry,
                        subentry,
                        data={
                            **subentry.data,
                            CONF_FEATURE_OVERRIDES: overrides,
                        },
                    )
        return self.async_show_form(
            step_id="overrides",
            data_schema=panel_feature_overrides_schema(
                current.feature_overrides,
            ),
            errors=errors,
        )

    async def async_step_components(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> SubentryFlowResult:
        """Route component mutation to the existing observable config entities."""
        del user_input
        if self._reconfigure_target() is None:
            return self.async_abort(reason="invalid_parent")
        return self.async_abort(reason="manage_components_with_panel_entities")

    async def async_step_rebind(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> SubentryFlowResult:
        """Inspect a replacement identity without granting user identity authority."""
        target = self._reconfigure_target()
        if target is None:
            return self.async_abort(reason="invalid_parent")
        entry, subentry, current = target
        if not self._capture_or_compare_reconfigure(current):
            return self.async_abort(reason="parent_changed")

        errors: dict[str, str] = {}
        failure: _FlowFailure | None = None
        if user_input is not None:
            self._rebind_identity = None
            self._rebind_facts = None
            self._rebind_password = None
            preserved_host = panel_connect_form_source(user_input).get(CONF_HOST)
            self._reconfigure_host = preserved_host if isinstance(preserved_host, str) else None
            try:
                normalized = normalize_panel_rebind_input(user_input)
            except FlowInputError as error:
                errors = dict(error.errors)
            else:
                host = normalized[CONF_HOST]
                password = normalized[CONF_ROOT_PASSWORD]
                self._reconfigure_host = host
                try:
                    identity = await gateway.async_fetch_host_identity(host)
                except asyncio.CancelledError:
                    raise
                except PanelIdentityError as error:
                    failure = _panel_failure(error.code, stage="panel_identity")
                except (OSError, asyncssh.Error):
                    failure = _panel_failure("cannot_connect", stage="panel_identity")
                except Exception:
                    failure = _panel_failure(
                        "inspection_failed",
                        stage="panel_identity",
                    )
                else:
                    if (
                        identity.fingerprint == current.identity_fingerprint
                        and identity.public_key == current.ssh_host_key
                    ):
                        failure = _panel_failure(
                            "rebind_identity_unchanged",
                            stage="panel_identity",
                        )
                    elif duplicate := _duplicate_panel(
                        entry,
                        identity.fingerprint,
                        exclude_subentry_id=subentry.subentry_id,
                    ):
                        return self.async_abort(
                            reason="already_configured",
                            description_placeholders={
                                "subentry_id": duplicate.subentry_id,
                                "panel_name": duplicate.title,
                            },
                        )
                    else:
                        try:
                            facts = await gateway._async_inspect_candidate(
                                self.hass,
                                host,
                                password,
                                identity,
                            )
                        except asyncio.CancelledError:
                            raise
                        except asyncssh.HostKeyNotVerifiable:
                            failure = _panel_failure(
                                "host_key_changed",
                                stage="panel_ssh",
                            )
                        except asyncssh.PermissionDenied:
                            failure = _panel_failure(
                                "panel_authentication_failed",
                                stage="panel_ssh",
                            )
                        except PanelIdentityError as error:
                            failure = _panel_failure(
                                error.code,
                                stage="panel_identity",
                            )
                        except PanelCompatibilityError as error:
                            failure = _panel_failure(
                                error.code,
                                stage="panel_inspection",
                            )
                        except (OSError, asyncssh.Error):
                            failure = _panel_failure(
                                "cannot_connect",
                                stage="panel_ssh",
                            )
                        except Exception:
                            failure = _panel_failure(
                                "inspection_failed",
                                stage="panel_inspection",
                            )
                        else:
                            target = self._reconfigure_target()
                            if target is None:
                                return self.async_abort(reason="invalid_parent")
                            _entry, _subentry, latest = target
                            if not self._capture_or_compare_reconfigure(latest):
                                return self.async_abort(reason="parent_changed")
                            self._rebind_identity = identity
                            self._rebind_facts = facts
                            self._rebind_password = password
                            return await self.async_step_rebind_confirm()

        source = {CONF_HOST: self._reconfigure_host or current.host}
        return self.async_show_form(
            step_id="rebind",
            data_schema=panel_rebind_schema(source),
            errors=errors or ({"base": failure.code} if failure is not None else {}),
            description_placeholders=(failure.placeholders() if failure is not None else None),
        )

    async def async_step_rebind_confirm(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> SubentryFlowResult:
        """Commit only an explicitly confirmed, rechecked replacement identity."""
        identity = self._rebind_identity
        facts = self._rebind_facts
        password = self._rebind_password
        host = self._reconfigure_host
        expected = self._reconfigure_expected
        if (
            identity is None
            or facts is None
            or password is None
            or host is None
            or expected is None
        ):
            return self.async_abort(reason="invalid_flow_state")

        errors: dict[str, str] = {}
        if user_input is not None:
            if set(user_input) != {"confirm"} or user_input.get("confirm") is not True:
                errors["confirm"] = "confirmation_required"
            else:
                target = self._reconfigure_target()
                if target is None:
                    return self.async_abort(reason="invalid_parent")
                entry, subentry, current = target
                if not self._capture_or_compare_reconfigure(current):
                    return self.async_abort(reason="parent_changed")
                try:
                    live_identity = await gateway.async_fetch_host_identity(host)
                except asyncio.CancelledError:
                    raise
                except (PanelIdentityError, OSError, asyncssh.Error):
                    errors["base"] = "cannot_connect"
                except Exception:
                    errors["base"] = "inspection_failed"
                else:
                    if live_identity != identity:
                        errors["base"] = "rebind_identity_changed"
                    elif duplicate := _duplicate_panel(
                        entry,
                        identity.fingerprint,
                        exclude_subentry_id=subentry.subentry_id,
                    ):
                        return self.async_abort(
                            reason="already_configured",
                            description_placeholders={
                                "subentry_id": duplicate.subentry_id,
                                "panel_name": duplicate.title,
                            },
                        )
                    if not errors:
                        from ..fleet_manager import (
                            ConfigEntryPersistenceError,
                            FleetManager,
                        )

                        runtime = entry.runtime_data
                        if not isinstance(runtime, FleetManager):
                            return self.async_abort(reason="runtime_unavailable")
                        try:
                            await runtime.async_rebind_panel(
                                subentry.subentry_id,
                                expected,
                                host=host,
                                root_password=password,
                                candidate=identity,
                            )
                        except asyncio.CancelledError:
                            raise
                        except ConfigEntryPersistenceError:
                            return self.async_abort(
                                reason="config_entry_storage_unavailable",
                            )
                        except EntryDataError as error:
                            code = str(error)
                            if code == "panel_snapshot_changed":
                                return self.async_abort(reason="parent_changed")
                            if code == "duplicate_panel_fingerprint":
                                duplicate = _duplicate_panel(
                                    entry,
                                    identity.fingerprint,
                                    exclude_subentry_id=subentry.subentry_id,
                                )
                                if duplicate is not None:
                                    return self.async_abort(
                                        reason="already_configured",
                                        description_placeholders={
                                            "subentry_id": duplicate.subentry_id,
                                            "panel_name": duplicate.title,
                                        },
                                    )
                                return self.async_abort(reason="parent_changed")
                            if code == "panel_rebind_unavailable":
                                return self.async_abort(reason="runtime_unavailable")
                            if code == "panel_rebind_blocked_by_panel_onboarding":
                                return self.async_abort(
                                    reason="rebind_blocked_by_panel_onboarding",
                                )
                            if code == "panel_rebind_identity_changed":
                                return self.async_abort(
                                    reason="rebind_identity_changed",
                                )
                            if code == "panel_rebind_identity_unreachable":
                                return self.async_abort(reason="cannot_connect")
                            return self.async_abort(reason="rebind_failed")
                        except Exception as error:
                            _LOGGER.error(
                                "Explicit panel rebind failed (%s)",
                                type(error).__name__,
                            )
                            return self.async_abort(reason="rebind_failed")
                        finally:
                            self._rebind_password = None
                        return self.async_abort(reason="reconfigure_successful")

        return self.async_show_form(
            step_id="rebind_confirm",
            data_schema=vol.Schema(
                {
                    vol.Required("confirm", default=False): bool,
                }
            ),
            errors=errors,
            description_placeholders={
                **_facts_placeholders(facts),
                "old_fingerprint": expected.identity_fingerprint,
                "new_fingerprint": identity.fingerprint,
            },
        )
