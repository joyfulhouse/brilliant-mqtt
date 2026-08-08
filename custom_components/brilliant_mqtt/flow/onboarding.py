"""Shared panel connect/confirm/provision steps for initial and Add panel."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, cast

import asyncssh
from homeassistant.config_entries import ConfigEntry
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.helpers import issue_registry as ir

from ..const import (
    CONF_HOST,
    CONF_MESH_PRIORITY,
    CONF_PANEL,
    CONF_PROVISIONING_TRANSACTION_ID,
    CONF_ROOT_PASSWORD,
    CONF_SSH_USERNAME,
    DOMAIN,
)
from ..entry_data import FleetConfig
from ..panel_inspection import PanelCompatibilityError, PanelFacts
from ..panel_provisioner import (
    PanelInstallRequest,
    PanelProvisioner,
    PanelProvisioningError,
    ProvisionedPanel,
    ProvisioningProgress,
    ProvisioningProgressStage,
)
from ..shell import HostIdentity, PanelIdentityError
from . import gateway
from .schemas import (
    FlowInputError,
    allocate_mesh_priority,
    allocate_panel_slug,
    normalize_panel_connect_input,
    normalize_panel_name,
    panel_confirm_schema,
    panel_connect_form_source,
    panel_connect_schema,
)
from .support import (
    _CORE_COMPONENTS,
    _PANEL_ONBOARDING_DOCUMENTATION_URL,
    _PROVISIONING_STAGE_ORDER,
    _duplicate_panel,
    _DuplicatePanel,
    _facts_placeholders,
    _fleet_storage_issue_id,
    _FlowFailure,
    _FlowSurface,
    _OnboardingAbort,
    _OnboardingResult,
    _panel_failure,
    _panel_subentries,
    _provisioned_matches_request,
    _provisioning_failure,
    _suggested_panel_name,
)

_LOGGER = logging.getLogger(__name__)


class _PanelOnboardingMixin:
    """Shared panel connect/confirm/progress state for initial and Add panel."""

    _panel_host: str | None
    _panel_root_password: str | None
    _panel_ssh_username: str
    _panel_identity: HostIdentity | None
    _panel_facts: PanelFacts | None
    _panel_name: str | None
    _panel_slug: str | None
    _panel_priority: int | None
    _panel_failure: _FlowFailure | None
    _panel_source: dict[str, object]
    _provision_task: asyncio.Task[ProvisionedPanel] | None
    _provisioned: ProvisionedPanel | None
    _provisioner: PanelProvisioner | None
    _install_request: PanelInstallRequest | None
    _provision_fleet: FleetConfig | None
    _recovery_task: asyncio.Task[bool] | None
    _onboarding_committed: bool

    def _init_panel_onboarding(self) -> None:
        self._panel_host = None
        self._panel_root_password = None
        self._panel_ssh_username = "root"
        self._panel_identity = None
        self._panel_facts = None
        self._panel_name = None
        self._panel_slug = None
        self._panel_priority = None
        self._panel_failure = None
        self._panel_source = {}
        self._provision_task = None
        self._provisioned = None
        self._provisioner = None
        self._install_request = None
        self._provision_fleet = None
        self._recovery_task = None
        self._onboarding_committed = False

    def _onboarding_parent_entry(self) -> ConfigEntry[Any] | None:
        raise NotImplementedError

    def _onboarding_fleet(self) -> FleetConfig:
        raise NotImplementedError

    def _onboarding_state_abort(
        self,
        *,
        before_create: bool,
    ) -> _OnboardingAbort | None:
        raise NotImplementedError

    def _finish_panel_onboarding(
        self,
        provisioned: ProvisionedPanel,
    ) -> _OnboardingResult:
        raise NotImplementedError

    def _abort_onboarding(self, abort: _OnboardingAbort) -> _OnboardingResult:
        flow = cast(_FlowSurface, self)
        cast(dict[str, Any], flow.context).pop(
            CONF_PROVISIONING_TRANSACTION_ID,
            None,
        )
        return flow.async_abort(
            reason=abort.reason,
            description_placeholders=abort.description_placeholders,
        )

    async def _async_run_recovery(self, provisioner: PanelProvisioner) -> bool:
        """Convert background recovery errors to one redacted completion state."""
        try:
            await provisioner.async_recover()
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOGGER.error("Panel onboarding recovery failed; manual recovery may be required")
            return False
        return True

    def _ensure_recovery_task(self) -> asyncio.Task[bool] | None:
        if self._recovery_task is not None:
            return self._recovery_task
        provisioner = self._provisioner
        if provisioner is None:
            return None
        flow = cast(_FlowSurface, self)
        self._recovery_task = flow.hass.async_create_task(
            self._async_run_recovery(provisioner),
            "brilliant-mqtt-panel-recovery",
        )
        return self._recovery_task

    async def _async_wait_for_recovery(self) -> bool:
        """Drain one shared recovery task and preserve caller cancellation."""
        task = self._ensure_recovery_task()
        if task is None:
            return False
        cancellation: asyncio.CancelledError | None = None
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError as error:
                if cancellation is None:
                    cancellation = error
        if cancellation is not None:
            try:
                task.result()
            except BaseException:
                pass
            raise cancellation
        return task.result()

    async def _async_compensate_onboarding_abort(
        self,
        abort: _OnboardingAbort,
    ) -> _OnboardingResult:
        """Rollback an installed-but-uncommitted panel before aborting its flow."""
        flow = cast(_FlowSurface, self)
        cast(dict[str, Any], flow.context).pop(
            CONF_PROVISIONING_TRANSACTION_ID,
            None,
        )
        if await self._async_wait_for_recovery():
            return flow.async_abort(
                reason=abort.reason,
                description_placeholders=abort.description_placeholders,
            )
        return flow.async_abort(
            reason="recovery_failed",
            description_placeholders={
                "documentation_slug": "panel-recovery-failed",
                "stage": "recovery",
            },
        )

    def _remove_panel_onboarding(self) -> None:
        """Schedule settlement when HA removes a marked flow before commit."""
        context = cast(dict[str, Any], cast(_FlowSurface, self).context)
        if (
            context.pop(CONF_PROVISIONING_TRANSACTION_ID, None) is not None
            and not self._onboarding_committed
        ):
            self._ensure_recovery_task()

    async def _async_accept_panel_connection(
        self,
        user_input: Mapping[str, object],
    ) -> tuple[dict[str, str], _FlowFailure | None, _DuplicatePanel | None]:
        self._panel_source = panel_connect_form_source(user_input)
        try:
            normalized = normalize_panel_connect_input(user_input)
        except FlowInputError as error:
            return dict(error.errors), None, None

        host = normalized[CONF_HOST]
        root_password = normalized[CONF_ROOT_PASSWORD]
        self._panel_host = host
        self._panel_root_password = root_password
        self._panel_ssh_username = normalized[CONF_SSH_USERNAME]
        try:
            identity = await gateway.async_fetch_host_identity(host)
            duplicate = _duplicate_panel(
                self._onboarding_parent_entry(),
                identity.fingerprint,
            )
            if duplicate is not None:
                return {}, None, duplicate
            facts = await gateway._async_inspect_candidate(
                cast(_FlowSurface, self).hass,
                host,
                root_password,
                identity,
            )
        except asyncio.CancelledError:
            raise
        except asyncssh.HostKeyNotVerifiable:
            return {}, _panel_failure("host_key_changed", stage="panel_ssh"), None
        except asyncssh.PermissionDenied:
            return (
                {},
                _panel_failure(
                    "panel_authentication_failed",
                    stage="panel_ssh",
                ),
                None,
            )
        except PanelIdentityError as error:
            return {}, _panel_failure(error.code, stage="panel_identity"), None
        except PanelCompatibilityError as error:
            return {}, _panel_failure(error.code, stage="panel_inspection"), None
        except (OSError, asyncssh.Error):
            return {}, _panel_failure("cannot_connect", stage="panel_ssh"), None
        except Exception:
            return {}, _panel_failure("inspection_failed", stage="panel_inspection"), None

        self._panel_identity = identity
        self._panel_facts = facts
        self._panel_failure = None
        return {}, None, None

    async def async_step_panel_connect(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> _OnboardingResult:
        flow = cast(_FlowSurface, self)
        errors: dict[str, str] = {}
        failure = self._panel_failure
        if user_input is not None:
            errors, failure, duplicate = await self._async_accept_panel_connection(user_input)
            if duplicate is not None:
                return flow.async_abort(
                    reason="already_configured",
                    description_placeholders={
                        "subentry_id": duplicate.subentry_id,
                        "panel_name": duplicate.title,
                    },
                )
            if not errors and failure is None:
                return await self.async_step_panel_confirm()
            self._panel_failure = failure

        return flow.async_show_form(
            step_id="panel_connect",
            data_schema=panel_connect_schema(self._panel_source),
            errors=errors or ({"base": failure.code} if failure is not None else {}),
            description_placeholders=(
                failure.placeholders()
                if failure is not None
                else {"documentation_url": _PANEL_ONBOARDING_DOCUMENTATION_URL}
            ),
        )

    async def async_step_panel_confirm(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> _OnboardingResult:
        flow = cast(_FlowSurface, self)
        identity = self._panel_identity
        facts = self._panel_facts
        if identity is None or facts is None:
            return flow.async_abort(reason="invalid_flow_state")

        errors: dict[str, str] = {}
        if user_input is not None:
            if abort := self._onboarding_state_abort(before_create=False):
                return self._abort_onboarding(abort)
            try:
                name = normalize_panel_name(user_input)
            except FlowInputError as error:
                errors = dict(error.errors)
            else:
                self._panel_name = name
                parent = self._onboarding_parent_entry()
                if parent is None:
                    return flow.async_abort(reason="invalid_parent")
                try:
                    await gateway._async_wait_config_entry_persisted(flow.hass, parent)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self._panel_failure = _panel_failure(
                        "config_entry_storage_unavailable",
                        stage="storage",
                    )
                else:
                    ir.async_delete_issue(
                        flow.hass,
                        DOMAIN,
                        _fleet_storage_issue_id(parent.entry_id),
                    )
                    if abort := self._onboarding_state_abort(before_create=False):
                        return self._abort_onboarding(abort)
                    try:
                        fleet = self._onboarding_fleet()
                        subentries = _panel_subentries(parent)
                        slug = allocate_panel_slug(
                            name,
                            (
                                str(subentry.data[CONF_PANEL])
                                for subentry in subentries
                                if isinstance(subentry.data.get(CONF_PANEL), str)
                            ),
                        )
                        priority = allocate_mesh_priority(
                            (
                                int(subentry.data[CONF_MESH_PRIORITY])
                                for subentry in subentries
                                if type(subentry.data.get(CONF_MESH_PRIORITY)) is int
                            ),
                        )
                    except FlowInputError as error:
                        errors = dict(error.errors)
                    else:
                        assert self._panel_host is not None
                        assert self._panel_root_password is not None
                        self._panel_slug = slug
                        self._panel_priority = priority
                        self._provision_fleet = fleet
                        self._recovery_task = None
                        self._install_request = PanelInstallRequest(
                            host=self._panel_host,
                            ssh_username=self._panel_ssh_username,
                            root_password=self._panel_root_password,
                            display_name=name,
                            slug=slug,
                            mesh_priority=priority,
                            selected_components=_CORE_COMPONENTS,
                            feature_overrides=MappingProxyType({}),
                        )
                        self._provisioner = gateway._get_panel_provisioner(
                            flow.hass,
                            expected_identity=identity,
                        )
                        self._provisioned = None
                        self._panel_failure = None
                        self._provision_task = flow.hass.async_create_task(
                            self._async_provision(),
                            "brilliant-mqtt-panel-provision",
                        )
                        return await self.async_step_panel_provision()

        failure = self._panel_failure
        suggested_name = self._panel_name or _suggested_panel_name(facts)
        return flow.async_show_form(
            step_id="panel_confirm",
            data_schema=panel_confirm_schema(suggested_name),
            errors=errors or ({"base": failure.code} if failure is not None else {}),
            description_placeholders={
                **_facts_placeholders(facts),
                **(
                    failure.placeholders()
                    if failure is not None
                    else {"documentation_url": _PANEL_ONBOARDING_DOCUMENTATION_URL}
                ),
            },
        )

    async def _async_report_provisioning_progress(
        self,
        update: ProvisioningProgress,
    ) -> None:
        if not isinstance(update, ProvisioningProgress):
            raise ValueError("invalid_provisioning_progress")
        if update.stage is ProvisioningProgressStage.STAGING:
            if update.transaction_id is None:
                raise ValueError("invalid_provisioning_progress")
            await gateway._async_verify_staged_progress(
                cast(_FlowSurface, self).hass,
                update,
            )
            context = cast(dict[str, Any], cast(_FlowSurface, self).context)
            transaction = str(update.transaction_id)
            existing = context.get(CONF_PROVISIONING_TRANSACTION_ID)
            if existing not in {None, transaction}:
                raise ValueError("invalid_provisioning_progress")
            context[CONF_PROVISIONING_TRANSACTION_ID] = transaction
        index = _PROVISIONING_STAGE_ORDER.index(update.stage)
        cast(_FlowSurface, self).async_update_progress(
            index / max(1, len(_PROVISIONING_STAGE_ORDER) - 1)
        )

    async def _async_provision(self) -> ProvisionedPanel:
        provisioner = self._provisioner
        request = self._install_request
        identity = self._panel_identity
        fleet = self._provision_fleet
        if provisioner is None or request is None or identity is None or fleet is None:
            raise PanelProvisioningError("invalid_provisioning_dependency")
        provisioned = await provisioner.async_install(
            request,
            fleet,
            self._async_report_provisioning_progress,
        )
        context = cast(dict[str, Any], cast(_FlowSurface, self).context)
        context[CONF_PROVISIONING_TRANSACTION_ID] = str(provisioned.transaction_id)
        try:
            if not _provisioned_matches_request(provisioned, request, identity):
                raise PanelProvisioningError("invalid_provisioning_dependency")
            await provisioner.async_mark_pending_config_commit(provisioned.transaction_id)
        except asyncio.CancelledError:
            context.pop(CONF_PROVISIONING_TRANSACTION_ID, None)
            await self._async_wait_for_recovery()
            raise
        except Exception:
            context.pop(CONF_PROVISIONING_TRANSACTION_ID, None)
            if not await self._async_wait_for_recovery():
                raise PanelProvisioningError("recovery_failed") from None
            raise
        return provisioned

    async def async_step_panel_provision(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> _OnboardingResult:
        del user_input
        flow = cast(_FlowSurface, self)
        task = self._provision_task
        if task is None:
            return flow.async_abort(reason="invalid_flow_state")
        if not task.done():
            return flow.async_show_progress(
                step_id="panel_provision",
                progress_action="panel_provision",
                progress_task=task,
            )

        try:
            self._provisioned = task.result()
        except asyncio.CancelledError:
            raise
        except PanelProvisioningError as error:
            self._panel_failure = _provisioning_failure(error)
            cast(dict[str, Any], flow.context).pop(
                CONF_PROVISIONING_TRANSACTION_ID,
                None,
            )
        except Exception:
            self._panel_failure = _panel_failure(
                "provisioning_failed",
                stage="provisioning",
            )
            cast(dict[str, Any], flow.context).pop(
                CONF_PROVISIONING_TRANSACTION_ID,
                None,
            )
        finally:
            self._provision_task = None

        if self._provisioned is None:
            return flow.async_show_progress_done(next_step_id="panel_confirm")
        return flow.async_show_progress_done(next_step_id="panel_create")

    async def async_step_panel_create(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> _OnboardingResult:
        del user_input
        if self._provisioned is None:
            return cast(_FlowSurface, self).async_abort(reason="invalid_flow_state")
        if abort := self._onboarding_state_abort(before_create=True):
            return await self._async_compensate_onboarding_abort(abort)
        result = self._finish_panel_onboarding(self._provisioned)
        if result["type"] is FlowResultType.CREATE_ENTRY:
            self._onboarding_committed = True
        return result
