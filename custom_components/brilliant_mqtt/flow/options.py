"""Fleet-level and per-panel options flows."""

from __future__ import annotations

import asyncio
import copy
from collections.abc import Mapping
from typing import Any, cast

import voluptuous as vol
from homeassistant.config_entries import (
    ConfigEntry,
    ConfigFlowResult,
    OptionsFlow,
    UnknownEntry,
)
from homeassistant.core import callback

from .. import _fleet_lock
from ..broker import BrokerKind, BrokerProfile
from ..const import (
    CONF_BROKER_KIND,
    CONF_HA_CONTROL_DOMAINS,
    CONF_HA_CONTROL_ENABLED,
    CONF_HA_CONTROL_LABEL,
    CONF_MAX_MIRRORED_ENTITIES,
    CONF_MQTT_HOST,
    CONF_MQTT_PASSWORD,
    CONF_MQTT_PORT,
    CONF_MQTT_TLS_CA,
    CONF_MQTT_TLS_ENABLED,
    CONF_MQTT_USERNAME,
    CONF_ROOM_OVERRIDES,
    CONF_SCENE_ACTIONS,
    CONF_SCENE_PANEL,
    DEFAULT_AUTO_REPAIR,
    DEFAULT_OFFLINE_GRACE_MINUTES,
    DEFAULT_REPAIR_COOLDOWN_MINUTES,
    DEFAULT_TRUST_HOST_KEY_CHANGES,
    OPT_AUTO_REPAIR,
    OPT_OFFLINE_GRACE_MINUTES,
    OPT_REPAIR_COOLDOWN_MINUTES,
    OPT_TRUST_HOST_KEY_CHANGES,
)
from ..entry_data import EntryDataError, FleetConfig, PanelConfig
from ..errors import OperationError
from . import gateway
from .schemas import (
    BROKER_MENU_OPTIONS,
    SECRET_UNCHANGED,
    FlowInputError,
    broker_form_source,
    broker_schema,
    fleet_control_schema,
    fleet_defaults_schema,
    fleet_scenes_schema,
    normalize_broker_input,
    normalize_fleet_control_input,
    normalize_fleet_defaults_input,
    normalize_fleet_scenes_input,
)
from .support import (
    _MQTT_DOCUMENTATION_URL,
    _FlowFailure,
    _is_exact_fleet_parent,
    _operation_failure,
    _panel_failure,
    _panel_subentries,
    _same_entry_panel_add_flow_active,
)


class BrilliantMqttFleetOptionsFlow(OptionsFlow):
    """Focused settings owned once by the singleton fleet entry."""

    def __init__(self) -> None:
        self._broker_kind: BrokerKind | None = None
        self._broker_values: dict[str, object] = {}
        self._broker_source: dict[str, object] = {}
        self._broker_profile: BrokerProfile | None = None
        self._broker_expected_profile: BrokerProfile | None = None
        self._broker_failure: _FlowFailure | None = None
        self._broker_task: asyncio.Task[object] | None = None
        self._control_expected: dict[str, object] | None = None
        self._scenes_expected: tuple[object, ...] | None = None
        self._defaults_expected: dict[str, Any] | None = None

    def _exact_registered_entry(self) -> ConfigEntry[Any] | None:
        """Resolve the still-registered singleton owner for every commit boundary."""
        try:
            entry = self.config_entry
        except UnknownEntry:
            return None
        if self.hass.config_entries.async_get_entry(
            entry.entry_id
        ) is not entry or not _is_exact_fleet_parent(entry):
            return None
        return entry

    @staticmethod
    def _broker_transport_matches(
        values: Mapping[str, object],
        current: BrokerProfile,
    ) -> bool:
        """Compare runtime broker material while ignoring guidance-only kind."""
        try:
            comparable = BrokerProfile.from_mapping(
                {
                    **values,
                    CONF_BROKER_KIND: current.kind.value,
                }
            )
        except OperationError:
            return False
        return comparable == current

    async def async_step_init(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        if self._exact_registered_entry() is None:
            return self.async_abort(reason="invalid_parent")
        return self.async_show_menu(
            step_id="init",
            menu_options=(
                "broker",
                "ha_control",
                "scenes",
                "fleet_defaults",
                "advanced",
            ),
        )

    async def async_step_broker(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Choose broker guidance without preferring away external brokers."""
        del user_input
        return self.async_show_menu(
            step_id="broker",
            menu_options=BROKER_MENU_OPTIONS,
        )

    async def async_step_official_mosquitto(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        self._broker_kind = BrokerKind.OFFICIAL_MOSQUITTO
        return await self.async_step_broker_profile()

    async def async_step_existing_broker(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        del user_input
        self._broker_kind = BrokerKind.EXISTING_BROKER
        return await self.async_step_broker_profile()

    async def async_step_broker_profile(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Normalize a masked profile before any validation or mutation."""
        kind = self._broker_kind
        entry = self._exact_registered_entry()
        if kind is None or entry is None:
            return self.async_abort(reason="invalid_parent")

        errors: dict[str, str] = {}
        if user_input is not None:
            self._broker_source = broker_form_source(user_input)
            try:
                current = FleetConfig.from_entry(entry).broker
                pending_password = self._broker_values.get(CONF_MQTT_PASSWORD)
                retained_password = (
                    pending_password
                    if isinstance(pending_password, str) and pending_password != SECRET_UNCHANGED
                    else str(entry.data[CONF_MQTT_PASSWORD])
                )
                values = normalize_broker_input(
                    kind,
                    user_input,
                    stored_password=retained_password,
                )
                candidate = BrokerProfile.from_mapping(values)
            except FlowInputError as error:
                errors = dict(error.errors)
            except (EntryDataError, OperationError):
                self._broker_failure = _panel_failure(
                    "invalid_broker_profile",
                    stage="broker_validation",
                )
            else:
                if entry.subentries and not self._broker_transport_matches(
                    values,
                    current,
                ):
                    return self.async_abort(
                        reason="broker_change_requires_guided_flow",
                    )
                self._broker_values = values
                self._broker_profile = candidate
                self._broker_expected_profile = current
                self._broker_failure = None
                self._broker_task = self.hass.async_create_task(
                    gateway._broker_validator(self.hass).async_validate(candidate),
                    "brilliant-mqtt-options-broker-validation",
                )
                return await self.async_step_broker_validation()
            if errors:
                self._broker_failure = None

        source = self._broker_source or dict(entry.data)
        failure = self._broker_failure
        return self.async_show_form(
            step_id="broker_profile",
            data_schema=broker_schema(
                kind,
                source,
                reconfigure=True,
            ),
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
        """Validate before committing one empty-fleet profile update."""
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
            return self.async_show_progress_done(next_step_id="broker_profile")
        return self.async_show_progress_done(next_step_id="broker_commit")

    async def async_step_broker_commit(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Commit runtime broker material only outside panel provisioning."""
        del user_input
        entry = self._exact_registered_entry()
        if entry is None:
            return self.async_abort(reason="invalid_parent")
        candidate = self._broker_profile
        expected = self._broker_expected_profile
        if candidate is None or expected is None:
            return self.async_abort(reason="invalid_flow_state")
        try:
            current = FleetConfig.from_entry(entry).broker
        except EntryDataError:
            return self.async_abort(reason="invalid_parent")
        if current != expected:
            return self.async_abort(reason="parent_changed")

        runtime_change = not self._broker_transport_matches(
            self._broker_values,
            current,
        )
        if runtime_change:
            async with _fleet_lock(self.hass):
                entry = self._exact_registered_entry()
                if entry is None:
                    return self.async_abort(reason="invalid_parent")
                try:
                    current = FleetConfig.from_entry(entry).broker
                except EntryDataError:
                    return self.async_abort(reason="invalid_parent")
                if current != expected:
                    return self.async_abort(reason="parent_changed")
                if entry.subentries:
                    return self.async_abort(
                        reason="broker_change_requires_guided_flow",
                    )

                journal_clear = await gateway._async_provisioning_journal_clear(self.hass)

                # The journal read yields to HA. Re-resolve every storage invariant
                # and the active-flow registry afterward, then update synchronously
                # while the shared provisioning lock is still held.
                entry = self._exact_registered_entry()
                if entry is None:
                    return self.async_abort(reason="invalid_parent")
                try:
                    current = FleetConfig.from_entry(entry).broker
                except EntryDataError:
                    return self.async_abort(reason="invalid_parent")
                if current != expected:
                    return self.async_abort(reason="parent_changed")
                if entry.subentries:
                    return self.async_abort(
                        reason="broker_change_requires_guided_flow",
                    )
                if not journal_clear or _same_entry_panel_add_flow_active(
                    self.hass,
                    entry.entry_id,
                ):
                    return self.async_abort(
                        reason="broker_change_blocked_by_panel_onboarding",
                    )
                self._update_broker_entry(entry)
        elif candidate != current:
            # BrokerKind is persisted guidance only and has no runtime transport
            # effect, so it cannot orphan an active or recoverable installation.
            self._update_broker_entry(entry)
        return self.async_abort(reason="reconfigure_successful")

    @callback
    def _update_broker_entry(self, entry: ConfigEntry[Any]) -> None:
        """Synchronously replace only the canonical broker profile fields."""
        updated = dict(entry.data)
        for key in (
            CONF_BROKER_KIND,
            CONF_MQTT_HOST,
            CONF_MQTT_PORT,
            CONF_MQTT_USERNAME,
            CONF_MQTT_PASSWORD,
            CONF_MQTT_TLS_ENABLED,
            CONF_MQTT_TLS_CA,
        ):
            updated.pop(key, None)
        updated.update(self._broker_values)
        self.hass.config_entries.async_update_entry(
            entry,
            data=updated,
        )

    def _fleet_config(self) -> FleetConfig | None:
        """Return the exact current fleet snapshot or fail closed."""
        entry = self._exact_registered_entry()
        if entry is None:
            return None
        try:
            return FleetConfig.from_entry(entry)
        except EntryDataError:
            return None

    def _control_snapshot(self) -> dict[str, object]:
        return {
            key: copy.deepcopy(self.config_entry.data.get(key))
            for key in (
                CONF_HA_CONTROL_ENABLED,
                CONF_HA_CONTROL_LABEL,
                CONF_ROOM_OVERRIDES,
                CONF_HA_CONTROL_DOMAINS,
                CONF_MAX_MIRRORED_ENTITIES,
            )
        }

    @staticmethod
    def _panel_topology(
        entry: ConfigEntry[Any],
    ) -> tuple[tuple[str, PanelConfig], ...] | None:
        panels: list[tuple[str, PanelConfig]] = []
        try:
            for subentry in _panel_subentries(entry):
                panels.append(
                    (
                        subentry.subentry_id,
                        PanelConfig.from_subentry(subentry),
                    )
                )
        except EntryDataError:
            return None
        return tuple(sorted(panels, key=lambda item: item[0]))

    def _scenes_snapshot(
        self,
        entry: ConfigEntry[Any],
    ) -> tuple[object, ...] | None:
        topology = self._panel_topology(entry)
        if topology is None:
            return None
        return (
            entry.data.get(CONF_SCENE_PANEL),
            copy.deepcopy(entry.data.get(CONF_SCENE_ACTIONS)),
            topology,
        )

    async def async_step_ha_control(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Update the five live HA-control globals on the fleet owner."""
        fleet = self._fleet_config()
        if fleet is None:
            return self.async_abort(reason="invalid_parent")
        current = self._control_snapshot()
        if self._control_expected is None:
            self._control_expected = current
        elif current != self._control_expected:
            return self.async_abort(reason="parent_changed")

        errors: dict[str, str] = {}
        if user_input is not None:
            try:
                normalized = normalize_fleet_control_input(user_input)
            except FlowInputError as error:
                errors = dict(error.errors)
            else:
                if (
                    self.config_entry.subentries
                    and normalized[CONF_HA_CONTROL_ENABLED] != fleet.ha_control_enabled
                ):
                    return self.async_abort(
                        reason="ha_control_change_requires_agent_rollout",
                    )
                latest = self._control_snapshot()
                if latest != self._control_expected:
                    return self.async_abort(reason="parent_changed")
                self.hass.config_entries.async_update_entry(
                    self.config_entry,
                    data={
                        **self.config_entry.data,
                        **normalized,
                    },
                )
                return self.async_abort(reason="reconfigure_successful")
        return self.async_show_form(
            step_id="ha_control",
            data_schema=fleet_control_schema(self.config_entry.data),
            errors=errors,
        )

    async def async_step_scenes(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Update scene ownership by subentry ID and actions by wire slug."""
        entry = self._exact_registered_entry()
        if entry is None:
            return self.async_abort(reason="invalid_parent")
        snapshot = self._scenes_snapshot(entry)
        if snapshot is None:
            return self.async_abort(reason="invalid_parent")
        if self._scenes_expected is None:
            if self._fleet_config() is None:
                return self.async_abort(reason="invalid_parent")
            self._scenes_expected = snapshot
        elif snapshot != self._scenes_expected:
            return self.async_abort(reason="parent_changed")

        topology = cast(tuple[tuple[str, PanelConfig], ...], snapshot[2])
        if not topology:
            return self.async_abort(reason="no_panels_configured")
        panel_ids = tuple(panel_id for panel_id, _panel in topology)
        panel_slugs = tuple(panel.panel for _panel_id, panel in topology)
        errors: dict[str, str] = {}
        if user_input is not None:
            try:
                normalized = normalize_fleet_scenes_input(
                    user_input,
                    panel_subentry_ids=panel_ids,
                    panel_slugs=panel_slugs,
                )
            except FlowInputError as error:
                errors = dict(error.errors)
            else:
                if self._scenes_snapshot(entry) != self._scenes_expected:
                    return self.async_abort(reason="parent_changed")
                self.hass.config_entries.async_update_entry(
                    entry,
                    data={
                        **entry.data,
                        **normalized,
                    },
                )
                return self.async_abort(reason="reconfigure_successful")
        return self.async_show_form(
            step_id="scenes",
            data_schema=fleet_scenes_schema(
                entry.data,
                panel_subentry_ids=panel_ids,
            ),
            errors=errors,
        )

    async def async_step_fleet_defaults(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Persist the three fleet-wide resilience defaults in entry options."""
        if self._fleet_config() is None:
            return self.async_abort(reason="invalid_parent")
        current = dict(self.config_entry.options)
        if self._defaults_expected is None:
            self._defaults_expected = current
        elif current != self._defaults_expected:
            return self.async_abort(reason="parent_changed")
        errors: dict[str, str] = {}
        if user_input is not None:
            try:
                normalized = normalize_fleet_defaults_input(user_input)
            except FlowInputError as error:
                errors = dict(error.errors)
            else:
                if dict(self.config_entry.options) != self._defaults_expected:
                    return self.async_abort(reason="parent_changed")
                return self.async_create_entry(
                    title="",
                    data=normalized,
                )
        return self.async_show_form(
            step_id="fleet_defaults",
            data_schema=fleet_defaults_schema(self.config_entry.options),
            errors=errors,
        )

    async def async_step_advanced(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Expose advanced fleet operations without unsafe direct mutation."""
        del user_input
        if self._fleet_config() is None:
            return self.async_abort(reason="invalid_parent")
        return self.async_show_menu(
            step_id="advanced",
            menu_options=("mesh_priorities",),
        )

    async def async_step_mesh_priorities(
        self,
        user_input: dict[str, Any] | None = None,
    ) -> ConfigFlowResult:
        """Defer mesh env mutation until it has a journaled agent rollout."""
        del user_input
        if self._fleet_config() is None:
            return self.async_abort(reason="invalid_parent")
        return self.async_abort(
            reason="mesh_priority_change_requires_agent_rollout",
        )


class BrilliantMqttOptionsFlow(OptionsFlow):
    """Per-panel behavior knobs; read live by the manager (no reload needed)."""

    async def async_step_init(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        if user_input is not None:
            return self.async_create_entry(data=user_input)
        opts = self.config_entry.options
        schema = vol.Schema(
            {
                vol.Required(
                    OPT_AUTO_REPAIR, default=opts.get(OPT_AUTO_REPAIR, DEFAULT_AUTO_REPAIR)
                ): bool,
                vol.Required(
                    OPT_OFFLINE_GRACE_MINUTES,
                    default=opts.get(OPT_OFFLINE_GRACE_MINUTES, DEFAULT_OFFLINE_GRACE_MINUTES),
                ): vol.All(vol.Coerce(int), vol.Range(min=2, max=120)),
                vol.Required(
                    OPT_REPAIR_COOLDOWN_MINUTES,
                    default=opts.get(OPT_REPAIR_COOLDOWN_MINUTES, DEFAULT_REPAIR_COOLDOWN_MINUTES),
                ): vol.All(vol.Coerce(int), vol.Range(min=5, max=1440)),
                vol.Required(
                    OPT_TRUST_HOST_KEY_CHANGES,
                    default=opts.get(OPT_TRUST_HOST_KEY_CHANGES, DEFAULT_TRUST_HOST_KEY_CHANGES),
                ): bool,
            }
        )
        return self.async_show_form(step_id="init", data_schema=schema)
