"""Top-level config flow: broker validation and durable fleet creation."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import voluptuous as vol
from homeassistant.config_entries import (
    ConfigEntry,
    ConfigFlow,
    ConfigFlowResult,
    ConfigSubentryFlow,
    FlowType,
    OptionsFlow,
)
from homeassistant.core import callback
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.helpers import issue_registry as ir

from ..broker import BrokerKind, BrokerProfile
from ..const import (
    CONF_ENTRY_KIND,
    CONF_HA_CONTROL_DOMAINS,
    CONF_HA_CONTROL_ENABLED,
    CONF_HA_CONTROL_LABEL,
    CONF_MAX_MIRRORED_ENTITIES,
    CONF_NEXT_MESH_PRIORITY,
    CONF_ROOM_OVERRIDES,
    CONF_SCENE_ACTIONS,
    CONF_SCENE_PANEL,
    CONF_SCHEMA_VERSION,
    CONFIG_ENTRY_VERSION,
    DEFAULT_HA_CONTROL_DOMAINS,
    DEFAULT_HA_CONTROL_ENABLED,
    DEFAULT_HA_CONTROL_LABEL,
    DEFAULT_MAX_MIRRORED_ENTITIES,
    DOMAIN,
    ENTRY_KIND_FLEET,
    FLEET_SCENE_OWNER_UNASSIGNED,
    FLEET_UNIQUE_ID,
    SUBENTRY_TYPE_PANEL,
)
from ..errors import OperationError
from . import gateway
from .options import BrilliantMqttFleetOptionsFlow, BrilliantMqttOptionsFlow
from .reconfigure import LegacyPanelReconfigureFlow
from .schemas import (
    BROKER_MENU_OPTIONS,
    FlowInputError,
    broker_form_source,
    broker_schema,
    normalize_broker_input,
)
from .subentry import PanelSubentryFlow
from .support import (
    _MQTT_DOCUMENTATION_URL,
    _async_create_fleet_storage_issue,
    _fleet_storage_issue_id,
    _FlowFailure,
    _is_exact_fleet_parent,
    _OnboardingAbort,
    _operation_failure,
    _panel_failure,
)

_LOGGER = logging.getLogger(__name__)


class BrilliantMqttConfigFlow(
    LegacyPanelReconfigureFlow,
    ConfigFlow,
    domain=DOMAIN,
):
    """Validate MQTT and durably create the singleton fleet before panel work."""

    VERSION = CONFIG_ENTRY_VERSION

    def __init__(self) -> None:
        self._broker_kind: BrokerKind | None = None
        self._broker_values: dict[str, object] = {}
        self._broker_source: dict[str, object] = {}
        self._ha_control_enabled = DEFAULT_HA_CONTROL_ENABLED
        self._broker_profile: BrokerProfile | None = None
        self._broker_failure: _FlowFailure | None = None
        self._broker_task: asyncio.Task[object] | None = None

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: ConfigEntry[Any]) -> OptionsFlow:
        if config_entry.data.get(CONF_ENTRY_KIND) == ENTRY_KIND_FLEET:
            return BrilliantMqttFleetOptionsFlow()
        return BrilliantMqttOptionsFlow()

    @classmethod
    @callback
    def async_get_supported_subentry_types(
        cls,
        config_entry: ConfigEntry[Any],
    ) -> dict[str, type[ConfigSubentryFlow]]:
        del cls
        if _is_exact_fleet_parent(config_entry):
            return {SUBENTRY_TYPE_PANEL: PanelSubentryFlow}
        return {}

    def _fleet_creation_abort(self) -> _OnboardingAbort | None:
        """Recheck singleton and legacy races at the final entry boundary."""
        if not self._broker_values or self._broker_profile is None:
            return _OnboardingAbort(reason="invalid_flow_state")
        entries = self.hass.config_entries.async_entries(DOMAIN)
        fleet = next(
            (entry for entry in entries if entry.data.get(CONF_ENTRY_KIND) == ENTRY_KIND_FLEET),
            None,
        )
        if fleet is not None:
            return _OnboardingAbort(
                reason="already_configured",
                description_placeholders={"entry_id": fleet.entry_id},
            )
        if entries:
            return _OnboardingAbort(reason="legacy_migration_required")
        return None

    def _empty_fleet_data(self) -> dict[str, object]:
        """Build the only valid zero-panel bootstrap state."""
        return {
            CONF_ENTRY_KIND: ENTRY_KIND_FLEET,
            **self._broker_values,
            CONF_NEXT_MESH_PRIORITY: 1,
            CONF_HA_CONTROL_ENABLED: self._ha_control_enabled,
            CONF_HA_CONTROL_LABEL: DEFAULT_HA_CONTROL_LABEL,
            CONF_ROOM_OVERRIDES: {},
            CONF_HA_CONTROL_DOMAINS: list(DEFAULT_HA_CONTROL_DOMAINS),
            CONF_MAX_MIRRORED_ENTITIES: DEFAULT_MAX_MIRRORED_ENTITIES,
            CONF_SCENE_PANEL: FLEET_SCENE_OWNER_UNASSIGNED,
            CONF_SCENE_ACTIONS: {},
            CONF_SCHEMA_VERSION: CONFIG_ENTRY_VERSION,
        }

    def _is_exact_created_empty_fleet(self, entry: object) -> bool:
        """Revalidate the inert owner produced by this exact flow instance."""
        return (
            isinstance(entry, ConfigEntry)
            and entry.domain == DOMAIN
            and entry.unique_id == FLEET_UNIQUE_ID
            and entry.version == CONFIG_ENTRY_VERSION
            and entry.data.get(CONF_SCHEMA_VERSION) == CONFIG_ENTRY_VERSION
            and entry.data.get(CONF_ENTRY_KIND) == ENTRY_KIND_FLEET
            and not entry.subentries
            and entry.data == self._empty_fleet_data()
        )

    async def async_on_create_entry(
        self,
        result: ConfigFlowResult,
    ) -> ConfigFlowResult:
        """Prove the empty owner on disk before exposing first-panel onboarding."""
        entry = result["result"]
        if not self._is_exact_created_empty_fleet(entry):
            _LOGGER.error(
                "Created fleet ownership no longer matches the validated empty state; "
                "first-panel onboarding was not started"
            )
            if isinstance(entry, ConfigEntry):
                _async_create_fleet_storage_issue(self.hass, entry.entry_id)
            return result
        try:
            await gateway._async_wait_config_entry_persisted(self.hass, entry)
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOGGER.error(
                "Fleet storage could not be confirmed; first-panel onboarding was not started"
            )
            _async_create_fleet_storage_issue(self.hass, entry.entry_id)
            return result
        ir.async_delete_issue(
            self.hass,
            DOMAIN,
            _fleet_storage_issue_id(entry.entry_id),
        )
        next_result = await self.hass.config_entries.subentries.async_init(
            (entry.entry_id, SUBENTRY_TYPE_PANEL),
            context={"source": "user"},
        )
        if next_result["type"] is not FlowResultType.ABORT:
            result["next_flow"] = (
                FlowType.CONFIG_SUBENTRIES_FLOW,
                next_result["flow_id"],
            )
        return result

    async def async_step_user(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Choose the one broker guidance path for the singleton fleet."""
        del user_input
        await self.async_set_unique_id(FLEET_UNIQUE_ID)
        entries = self._async_current_entries()
        fleet = next(
            (entry for entry in entries if entry.data.get(CONF_ENTRY_KIND) == ENTRY_KIND_FLEET),
            None,
        )
        if fleet is not None:
            return self.async_abort(
                reason="already_configured",
                description_placeholders={"entry_id": fleet.entry_id},
            )
        if entries:
            return self.async_abort(reason="legacy_migration_required")
        # Canonical backstop behind the richer aborts above. For an ignored
        # fleet entry this deliberately falls through (core lets a manual user
        # flow re-configure an ignored integration), so ignoring a fleet does
        # not permanently brick onboarding.
        self._abort_if_unique_id_configured()
        return self.async_show_menu(
            step_id="user",
            menu_options=BROKER_MENU_OPTIONS,
        )

    async def async_step_official_mosquitto(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        self._broker_kind = BrokerKind.OFFICIAL_MOSQUITTO
        return await self.async_step_broker()

    async def async_step_existing_broker(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        self._broker_kind = BrokerKind.EXISTING_BROKER
        return await self.async_step_broker()

    async def async_step_broker(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Validate one normalized profile in a non-submit-capable progress task."""
        kind = self._broker_kind
        if kind is None:
            return self.async_abort(reason="invalid_flow_state")
        errors: dict[str, str] = {}
        if user_input is not None:
            self._broker_source = broker_form_source(user_input)
            profile: BrokerProfile | None = None
            ha_control_enabled = user_input.get(CONF_HA_CONTROL_ENABLED)
            if type(ha_control_enabled) is not bool:
                errors[CONF_HA_CONTROL_ENABLED] = "invalid_value"
            else:
                self._ha_control_enabled = ha_control_enabled
            try:
                values = normalize_broker_input(kind, user_input)
                profile = BrokerProfile.from_mapping(values)
            except FlowInputError as error:
                errors.update(error.errors)
            except OperationError as error:
                self._broker_failure = _operation_failure(error)
            if profile is not None and not errors:
                self._broker_values = values
                self._broker_profile = profile
                self._broker_failure = None
                self._broker_task = self.hass.async_create_task(
                    gateway._broker_validator(self.hass).async_validate(profile),
                    "brilliant-mqtt-broker-validation",
                )
                return await self.async_step_broker_validation()
            if errors:
                self._broker_failure = None

        failure = self._broker_failure
        local_ip = self.hass.config.api.local_ip if self.hass.config.api is not None else None
        schema = broker_schema(
            kind,
            self._broker_source,
            default_host=local_ip,
        ).extend(
            {
                vol.Required(
                    CONF_HA_CONTROL_ENABLED,
                    default=self._ha_control_enabled,
                ): bool,
            }
        )
        return self.async_show_form(
            step_id="broker",
            data_schema=schema,
            errors=errors or ({"base": failure.code} if failure is not None else {}),
            description_placeholders=(
                failure.placeholders()
                if failure is not None
                else {"documentation_url": f"{_MQTT_DOCUMENTATION_URL}#mqtt-validation"}
            ),
        )

    async def async_step_broker_validation(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        task = self._broker_task
        if task is None:
            return self.async_abort(reason="invalid_flow_state")
        if not task.done():
            return self.async_show_progress(
                step_id="broker_validation",
                progress_action="broker_validation",
                progress_task=task,
            )
        try:
            task.result()
        except asyncio.CancelledError:
            raise
        except OperationError as error:
            self._broker_failure = _operation_failure(error)
        except Exception:
            self._broker_failure = _panel_failure(
                "broker_validation_failed",
                stage="broker_validation",
            )
        finally:
            self._broker_task = None
        if self._broker_failure is not None:
            return self.async_show_progress_done(next_step_id="broker")
        return self.async_show_progress_done(next_step_id="fleet_create")

    async def async_step_fleet_create(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Create the inert singleton owner before starting any panel flow."""
        del user_input
        if abort := self._fleet_creation_abort():
            return self.async_abort(
                reason=abort.reason,
                description_placeholders=abort.description_placeholders,
            )
        return self.async_create_entry(
            title="Brilliant MQTT",
            data=self._empty_fleet_data(),
        )
