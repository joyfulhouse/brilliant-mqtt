"""One fleet runtime owning panel managers and shared lifecycle resources."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine, Iterable, Mapping
from dataclasses import dataclass
from functools import partial
from types import MappingProxyType
from typing import Any, cast
from uuid import RFC_4122, UUID

from homeassistant.components import mqtt
from homeassistant.config_entries import ConfigEntry, ConfigSubentry
from homeassistant.core import CALLBACK_TYPE, HomeAssistant, callback
from homeassistant.data_entry_flow import UnknownFlow
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.json import json_bytes
from homeassistant.util import json as json_util

from . import panel_ops
from .broker import BrokerKind, BrokerProfile
from .broker_validation import BrokerValidator
from .const import (
    CONF_BROKER_KIND,
    CONF_COMPONENTS,
    CONF_ENTRY_KIND,
    CONF_HA_CONTROL_DOMAINS,
    CONF_HA_CONTROL_ENABLED,
    CONF_HA_CONTROL_LABEL,
    CONF_HOST,
    CONF_IDENTITY_FINGERPRINT,
    CONF_MANAGEMENT_ID,
    CONF_MAX_MIRRORED_ENTITIES,
    CONF_MESH_PRIORITY,
    CONF_MQTT_TLS_CA,
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
    CONFIG_ENTRY_VERSION,
    DEFAULT_HA_CONTROL_DOMAINS,
    DEFAULT_HA_CONTROL_ENABLED,
    DEFAULT_HA_CONTROL_LABEL,
    DEFAULT_MAX_MIRRORED_ENTITIES,
    DOMAIN,
    ENTRY_KIND_FLEET,
    ENTRY_KIND_LEGACY_PENDING_CONSOLIDATION,
    EVENT_PANEL_REBOUND,
    FLEET_SCENE_OWNER_UNASSIGNED,
    FLEET_UNIQUE_ID,
    SUBENTRY_TYPE_PANEL,
)
from .entry_data import (
    EntryDataError,
    FleetConfig,
    FleetPanelStore,
    LegacyPanelStore,
    PanelConfig,
    PanelConfigStore,
    _panel_agent_cadences,
)
from .manager import PanelManager, async_delete_panel_issues
from .panel_health import PanelHealthObserver
from .panel_inspection import async_inspect_panel
from .panel_provisioner import (
    PanelProvisioner,
    TransactionLookup,
    TransactionLookupState,
    panel_release_provider,
    panel_snapshot_from_stored,
    staged_release_from_journal,
    stored_snapshot_from_panel,
)
from .provisioning_journal import (
    ProvisioningJournal,
    ProvisioningPhase,
    ProvisioningRecord,
)
from .shell import (
    AsyncsshShell,
    HostIdentity,
    PanelIdentityError,
    async_verify_host_identity,
)

_LOGGER = logging.getLogger(__name__)

_BROKER_ISSUE_REASON = "Home Assistant's MQTT broker connection is unavailable"
_RUNTIME_SETUP_ISSUE_REASON = (
    "No panel runtime could start. Home Assistant will retry automatically; "
    "verify MQTT connectivity and review the integration logs."
)
_REBIND_STORAGE_ISSUE_REASON = (
    "Home Assistant could not prove whether the previous panel identity was restored "
    "in durable storage. Do not operate or retry either panel until storage is healthy "
    "and the saved panel identity has been inspected or reloaded. If the previous "
    "identity is restored, run Replace physical panel again."
)
_PENDING_SCENE_OWNER_PREFIX = "pending-provisioning:"
_PROVISIONING_REPAIR_REASON = (
    "Automatic provisioning rollback did not finish. Retry panel onboarding or "
    "follow the provisioning recovery instructions."
)
_CONFIG_ENTRIES_STORAGE_FILE = ".storage/core.config_entries"
_CONFIG_ENTRY_PERSISTENCE_TIMEOUT_SECONDS = 5.0
_CONFIG_ENTRY_PERSISTENCE_POLL_SECONDS = 0.25


def pending_scene_owner(transaction_id: UUID) -> str:
    """Return the exact temporary scene owner used before HA assigns a subentry ID."""
    if (
        not isinstance(transaction_id, UUID)
        or transaction_id.version != 4
        or transaction_id.variant != RFC_4122
    ):
        raise ValueError("invalid_provisioning_transaction_id")
    return f"{_PENDING_SCENE_OWNER_PREFIX}{transaction_id}"


def _provisioning_repair_issue_id(transaction_id: UUID) -> str:
    return f"provisioning_rollback_{transaction_id.hex}"


def _fleet_storage_issue_id(entry_id: str) -> str:
    return f"fleet_storage_{entry_id}"


@callback
def _async_create_fleet_storage_issue(
    hass: HomeAssistant,
    entry_id: str,
) -> None:
    """Keep unknown config-entry ownership visible without exposing storage errors."""
    ir.async_create_issue(
        hass,
        DOMAIN,
        _fleet_storage_issue_id(entry_id),
        is_fixable=False,
        severity=ir.IssueSeverity.ERROR,
        translation_key="needs_attention",
        translation_placeholders={
            "panel": "Brilliant MQTT fleet",
            "reason": _REBIND_STORAGE_ISSUE_REASON,
        },
        learn_more_url=(
            "https://github.com/joyfulhouse/brilliant-mqtt/blob/main/docs/ha-integration.md"
        ),
    )


@dataclass(frozen=True, slots=True)
class _PendingHandoff:
    """Exact stored ownership awaiting runtime proof and journal completion."""

    record: ProvisioningRecord
    subentry_id: str


@dataclass(frozen=True, slots=True)
class _PendingControlPlaneReload:
    """One generation-bound target awaiting successful control-plane publication."""

    generation: int
    target: tuple[object, ...]


def _control_plane_snapshot(
    fleet: FleetConfig,
    panels: Mapping[str, PanelConfig],
) -> tuple[object, ...]:
    """Return only entry data consumed by HA-control and scene routing."""
    return (
        fleet.ha_control_enabled,
        fleet.ha_control_label,
        fleet.room_overrides,
        fleet.ha_control_domains,
        fleet.max_mirrored_entities,
        fleet.scene_panel,
        fleet.scene_actions,
        tuple(sorted((panel_id, config.panel) for panel_id, config in panels.items())),
    )


class _ProvisioningRepairReporter:
    """Create one fixed, redacted repair when automatic rollback cannot settle."""

    __slots__ = ("_hass",)

    def __init__(self, hass: HomeAssistant) -> None:
        self._hass = hass

    async def async_report_rollback_failure(
        self,
        transaction_id: UUID,
        *,
        original_code: str,
        rollback_code: str,
    ) -> None:
        del original_code, rollback_code
        ir.async_create_issue(
            self._hass,
            DOMAIN,
            _provisioning_repair_issue_id(transaction_id),
            is_fixable=False,
            severity=ir.IssueSeverity.ERROR,
            translation_key="needs_attention",
            translation_placeholders={
                "panel": "Brilliant MQTT provisioning",
                "reason": _PROVISIONING_REPAIR_REASON,
            },
            learn_more_url=(
                "https://github.com/joyfulhouse/brilliant-mqtt/blob/main/docs/ha-integration.md"
            ),
        )

    async def async_clear_rollback_failure(self, transaction_id: UUID) -> None:
        """Delete the stable repair after the exact transaction is resolved."""
        ir.async_delete_issue(
            self._hass,
            DOMAIN,
            _provisioning_repair_issue_id(transaction_id),
        )


async def _async_report_ownership_unproven(
    hass: HomeAssistant,
    record: ProvisioningRecord,
    *,
    original_code: str,
) -> None:
    """Preserve one journal and surface its fixed, redacted ownership repair."""
    await _ProvisioningRepairReporter(hass).async_report_rollback_failure(
        record.transaction_id,
        original_code=original_code,
        rollback_code="ownership_unproven",
    )


def _enabled_components(data: Mapping[str, Any]) -> frozenset[str] | None:
    raw = data.get(CONF_COMPONENTS)
    if not isinstance(raw, Mapping) or any(
        not isinstance(component, str) or type(enabled) is not bool
        for component, enabled in raw.items()
    ):
        return None
    return frozenset(component for component, enabled in raw.items() if enabled)


def _matches_record_identity(
    subentry: ConfigSubentry,
    record: ProvisioningRecord,
) -> bool:
    """Match every journaled panel owner field without exposing credential values."""
    request = record.panel_request
    data = subentry.data
    return (
        subentry.unique_id == request.fingerprint
        and data.get(CONF_IDENTITY_FINGERPRINT) == request.fingerprint
        and data.get(CONF_SSH_HOST_KEY) == request.public_key
        and data.get(CONF_HOST) == request.host
        and data.get(CONF_SSH_USERNAME) == request.ssh_username
        and data.get(CONF_ROOT_PASSWORD) == request.root_password
        and data.get(CONF_PANEL) == request.slug
        and data.get(CONF_MANAGEMENT_ID) == request.fingerprint
        and _enabled_components(data) == frozenset(request.selected_components)
    )


def _matches_record_fleet(
    entry: ConfigEntry[Any],
    record: ProvisioningRecord,
) -> bool:
    """Bind a stored panel owner to the journal's exact MQTT transport."""
    try:
        broker = BrokerProfile.from_mapping(entry.data)
    except ValueError:
        return False
    expected = record.fleet_profile
    return (
        broker.host == expected.host
        and broker.port == expected.port
        and broker.tls_enabled is expected.tls_enabled
    )


def _is_exact_fleet_entry(entry: ConfigEntry[Any]) -> bool:
    """Recognize only the versioned singleton fleet envelope."""
    return (
        entry.domain == DOMAIN
        and entry.unique_id == FLEET_UNIQUE_ID
        and entry.version == CONFIG_ENTRY_VERSION
        and entry.data.get(CONF_ENTRY_KIND) == ENTRY_KIND_FLEET
        and entry.data.get(CONF_SCHEMA_VERSION) == CONFIG_ENTRY_VERSION
    )


def _is_exact_empty_fleet_owner(
    entry: ConfigEntry[Any],
    record: ProvisioningRecord,
) -> bool:
    """Bind a pre-subentry journal to the durable zero-panel bootstrap owner."""
    if not _is_exact_fleet_entry(entry) or entry.subentries:
        return False
    try:
        fleet = FleetConfig.from_entry(entry)
    except EntryDataError:
        return False
    return (
        fleet.scene_panel == FLEET_SCENE_OWNER_UNASSIGNED
        and fleet.next_mesh_priority == 1
        and _matches_record_fleet(entry, record)
    )


def _is_exact_fleet_envelope_owner(
    entry: ConfigEntry[Any],
    record: ProvisioningRecord,
) -> bool:
    """Bind a pre-subentry journal to one complete fleet and matching broker."""
    if not _is_exact_fleet_entry(entry):
        return False
    try:
        FleetConfig.from_entry(entry)
    except EntryDataError:
        return False
    return _matches_record_fleet(entry, record)


def _has_other_domain_entry(
    hass: HomeAssistant,
    entry: ConfigEntry[Any],
) -> bool:
    """Fail closed when any Brilliant fleet or legacy entry survives."""
    return any(
        candidate.entry_id != entry.entry_id
        for candidate in hass.config_entries.async_entries(DOMAIN)
    )


def _owned_subentry(
    entry: ConfigEntry[Any],
    record: ProvisioningRecord,
) -> ConfigSubentry | None:
    """Return one exact owner, distinguish absence, and reject all ambiguity."""
    transaction = str(record.transaction_id)
    marked: list[ConfigSubentry] = []
    identity_matches: list[ConfigSubentry] = []
    any_marker = False
    for subentry in entry.subentries.values():
        marker = subentry.data.get(CONF_PROVISIONING_TRANSACTION_ID)
        if marker is not None:
            any_marker = True
            if marker == transaction:
                marked.append(subentry)
        if _matches_record_identity(subentry, record):
            identity_matches.append(subentry)

    if len(marked) > 1 or len(identity_matches) > 1:
        raise EntryDataError("provisioning_ownership_mismatch")
    if marked:
        candidate = marked[0]
        if (
            not identity_matches
            or identity_matches[0] is not candidate
            or not _matches_record_fleet(entry, record)
        ):
            raise EntryDataError("provisioning_ownership_mismatch")
        if any(
            subentry.data.get(CONF_PROVISIONING_TRANSACTION_ID) not in {None, transaction}
            for subentry in entry.subentries.values()
        ):
            raise EntryDataError("provisioning_ownership_mismatch")
        return candidate
    if any_marker:
        raise EntryDataError("provisioning_ownership_mismatch")
    if identity_matches:
        # Narrow crash window: the marker was durably removed, but the journal
        # commit did not settle. The parent must already have a real scene owner,
        # proving the initial transaction placeholder was normalized first.
        scene_owner = entry.data.get(CONF_SCENE_PANEL)
        if scene_owner not in entry.subentries or not _matches_record_fleet(entry, record):
            raise EntryDataError("provisioning_ownership_mismatch")
        return identity_matches[0]
    return None


class ConfigEntryPersistenceError(RuntimeError):
    """One fixed, redacted failure for an unproven config-entry disk boundary."""

    def __init__(self) -> None:
        super().__init__("config_entry_storage_unavailable")

    def __repr__(self) -> str:
        return "ConfigEntryPersistenceError('config_entry_storage_unavailable')"


def _canonical_entry_storage(entry: ConfigEntry[Any]) -> Mapping[str, Any]:
    """Return the JSON-canonical fragment HA writes to core.config_entries."""
    return json_util.json_loads_object(json_bytes(entry.as_storage_fragment))


def _entry_ownership_guard(
    entry: ConfigEntry[Any],
    *,
    subentry_id: str | None,
) -> tuple[object, ...]:
    """Freeze the fields whose drift invalidates a provisioning ownership proof."""
    canonical = _canonical_entry_storage(entry)
    subentries = canonical.get("subentries")
    if not isinstance(subentries, list):
        raise ConfigEntryPersistenceError
    if subentry_id is not None:
        targets = [
            item
            for item in subentries
            if isinstance(item, Mapping) and item.get("subentry_id") == subentry_id
        ]
        if len(targets) != 1:
            raise ConfigEntryPersistenceError
    return (
        canonical.get("entry_id"),
        canonical.get("domain"),
        canonical.get("unique_id"),
        canonical.get("version"),
        canonical.get("minor_version"),
        canonical.get("data"),
        subentries,
    )


def _stored_entry_matches_exact(
    stored: object,
    expected: Mapping[str, Any],
) -> bool:
    """Require exactly one byte-canonical target fragment in the fresh disk envelope."""
    if not isinstance(stored, Mapping):
        return False
    data = stored.get("data")
    if not isinstance(data, Mapping):
        return False
    raw_entries = data.get("entries")
    if not isinstance(raw_entries, list):
        return False
    matches = [
        item
        for item in raw_entries
        if isinstance(item, Mapping) and item.get("entry_id") == expected.get("entry_id")
    ]
    if len(matches) != 1:
        return False
    return dict(matches[0]) == dict(expected)


async def async_wait_config_entry_persisted(
    hass: HomeAssistant,
    entry: ConfigEntry[Any],
    *,
    subentry_id: str | None = None,
) -> None:
    """Wait until HA's scheduled save is proven in the actual storage file."""
    if hass.config_entries.async_get_entry(entry.entry_id) is not entry:
        raise ConfigEntryPersistenceError
    ownership_guard = _entry_ownership_guard(entry, subentry_id=subentry_id)
    path = hass.config.path(_CONFIG_ENTRIES_STORAGE_FILE)
    deadline = hass.loop.time() + _CONFIG_ENTRY_PERSISTENCE_TIMEOUT_SECONDS
    while True:
        if (
            hass.config_entries.async_get_entry(entry.entry_id) is not entry
            or _entry_ownership_guard(entry, subentry_id=subentry_id) != ownership_guard
        ):
            raise ConfigEntryPersistenceError
        live_before = _canonical_entry_storage(entry)
        try:
            stored = await hass.async_add_executor_job(json_util.load_json, path)
        except Exception:
            stored = None
        if (
            hass.config_entries.async_get_entry(entry.entry_id) is not entry
            or _entry_ownership_guard(entry, subentry_id=subentry_id) != ownership_guard
        ):
            raise ConfigEntryPersistenceError
        live_after = _canonical_entry_storage(entry)
        if live_before == live_after and _stored_entry_matches_exact(stored, live_after):
            return
        remaining = deadline - hass.loop.time()
        if remaining <= 0:
            raise ConfigEntryPersistenceError
        await asyncio.sleep(min(_CONFIG_ENTRY_PERSISTENCE_POLL_SECONDS, remaining))


async def _async_duplicate_fingerprint(
    hass: HomeAssistant,
    fingerprint: str,
    *,
    exclude_entry_id: str | None = None,
    exclude_subentry_id: str | None = None,
) -> bool:
    """Recheck every persisted fleet and legacy pin immediately before writes."""
    for entry in hass.config_entries.async_entries(DOMAIN):
        for subentry in entry.subentries.values():
            if entry.entry_id == exclude_entry_id and subentry.subentry_id == exclude_subentry_id:
                continue
            if (
                subentry.data.get(CONF_IDENTITY_FINGERPRINT) == fingerprint
                or subentry.data.get(CONF_MANAGEMENT_ID) == fingerprint
            ):
                return True
        public_key = entry.data.get(CONF_SSH_HOST_KEY)
        if not isinstance(public_key, str):
            continue
        try:
            if HostIdentity(public_key, fingerprint).fingerprint == fingerprint:
                return True
        except PanelIdentityError:
            continue
    return False


async def _async_transaction_lookup(
    hass: HomeAssistant,
    transaction_id: UUID,
) -> TransactionLookup:
    """Keep persisted exact ownership pending until FleetManager proves runtime."""
    record = await ProvisioningJournal(hass).async_load()
    if record is None or record.transaction_id != transaction_id:
        return TransactionLookup(TransactionLookupState.FLOW_ABORTED_OR_ABSENT)
    matches: list[ConfigSubentry] = []
    for entry in hass.config_entries.async_entries(DOMAIN):
        if entry.data.get(CONF_ENTRY_KIND) != ENTRY_KIND_FLEET:
            continue
        candidate = _owned_subentry(entry, record)
        if candidate is not None:
            matches.append(candidate)
    if len(matches) > 1:
        raise EntryDataError("provisioning_ownership_mismatch")
    if matches:
        # MATCHED would let PanelProvisioner clear the journal before runtime
        # ownership. FleetManager's eager entry listener is the sole commit owner.
        return TransactionLookup(TransactionLookupState.FLOW_PENDING)
    transaction = str(transaction_id)
    for manager in (
        hass.config_entries.flow,
        hass.config_entries.subentries,
    ):
        for progress in manager.async_progress(include_uninitialized=True):
            if not isinstance(progress, Mapping):
                continue
            context = progress.get("context")
            if (
                isinstance(context, Mapping)
                and context.get(CONF_PROVISIONING_TRANSACTION_ID) == transaction
            ):
                return TransactionLookup(TransactionLookupState.FLOW_PENDING)
    return TransactionLookup(TransactionLookupState.FLOW_ABORTED_OR_ABSENT)


def _build_panel_provisioner(
    hass: HomeAssistant,
    identity_fetcher: Any,
) -> PanelProvisioner:
    """Wire one provisioner to HA-wide locks and production adapters."""
    validator = BrokerValidator(
        hass,
        lambda profile, client_id: profile.device_client(client_id),
    )
    return PanelProvisioner(
        operation_lock=_shared_ssh_lock(hass),
        journal=ProvisioningJournal(hass),
        identity_fetcher=identity_fetcher,
        shell_factory=AsyncsshShell,
        inspector=async_inspect_panel,
        duplicate_fingerprint=partial(_async_duplicate_fingerprint, hass),
        operations=cast(Any, panel_ops),
        snapshot_to_stored=stored_snapshot_from_panel,
        snapshot_from_stored=panel_snapshot_from_stored,
        staged_release_factory=staged_release_from_journal,
        release_provider=panel_release_provider(hass),
        preflight_launcher_factory=panel_ops.panel_preflight_launcher,
        broker_validator=validator,
        health_observer_factory=partial(PanelHealthObserver, hass),
        transaction_lookup=partial(_async_transaction_lookup, hass),
        repair_reporter=_ProvisioningRepairReporter(hass),
    )


def get_panel_provisioner(
    hass: HomeAssistant,
    *,
    expected_identity: HostIdentity,
) -> PanelProvisioner:
    """Build fresh-install orchestration bound to the flow-confirmed SSH pin."""
    if not isinstance(expected_identity, HostIdentity):
        raise ValueError("expected_panel_identity_required")
    return _build_panel_provisioner(
        hass,
        partial(async_verify_host_identity, expected=expected_identity),
    )


def _get_recovery_provisioner(hass: HomeAssistant) -> PanelProvisioner:
    """Build recovery-only orchestration; fresh installation stays unavailable."""

    async def recovery_only_identity_fetcher(host: str) -> HostIdentity:
        del host
        raise RuntimeError("recovery_only_provisioner")

    return _build_panel_provisioner(hass, recovery_only_identity_fetcher)


@callback
def _async_abort_owned_panel_flows(
    hass: HomeAssistant,
    entry: ConfigEntry[Any],
    transaction_id: UUID,
) -> None:
    """Abort only panel subentry flows exactly owned by the removed transaction."""
    transaction = str(transaction_id)
    manager = hass.config_entries.subentries
    flow_ids: list[str] = []
    for progress in manager.async_progress(include_uninitialized=True):
        if not isinstance(progress, Mapping) or progress.get("handler") != (
            entry.entry_id,
            SUBENTRY_TYPE_PANEL,
        ):
            continue
        context = progress.get("context")
        flow_id = progress.get("flow_id")
        if (
            isinstance(context, Mapping)
            and context.get(CONF_PROVISIONING_TRANSACTION_ID) == transaction
            and isinstance(flow_id, str)
            and flow_id
        ):
            flow_ids.append(flow_id)
    for flow_id in flow_ids:
        try:
            manager.async_abort(flow_id)
        except UnknownFlow:
            # Finishing between the progress snapshot and abort is equivalent to
            # already having removed this flow from the ownership lookup.
            continue


async def _async_recover_removed_entry(
    hass: HomeAssistant,
    entry: ConfigEntry[Any],
) -> None:
    """Settle only a journal exactly anchored to the removed fleet owner."""
    record: ProvisioningRecord | None = None
    try:
        journal = ProvisioningJournal(hass)
        record = await journal.async_load()
        if record is None or not _is_exact_fleet_entry(entry):
            return
        if _has_other_domain_entry(hass, entry):
            await _async_report_ownership_unproven(
                hass,
                record,
                original_code="entry_removed",
            )
            return
        try:
            subentry_owner = _owned_subentry(entry, record)
        except EntryDataError:
            await _async_report_ownership_unproven(
                hass,
                record,
                original_code="entry_removed",
            )
            return
        empty_owner = _is_exact_empty_fleet_owner(entry, record)
        pre_subentry_fleet_owner = record.phase in {
            ProvisioningPhase.STAGED,
            ProvisioningPhase.ACTIVATION_PENDING,
            ProvisioningPhase.ACTIVATED,
            ProvisioningPhase.VERIFYING,
            ProvisioningPhase.PENDING_CONFIG_COMMIT,
            ProvisioningPhase.ROLLBACK_PENDING,
            ProvisioningPhase.ROLLED_BACK,
        } and _is_exact_fleet_envelope_owner(entry, record)
        if record.phase is ProvisioningPhase.COMMITTED:
            if subentry_owner is None:
                await _async_report_ownership_unproven(
                    hass,
                    record,
                    original_code="entry_removed",
                )
                return
            _async_abort_owned_panel_flows(hass, entry, record.transaction_id)
            try:
                await journal.async_clear_committed(record.transaction_id)
            except Exception:
                if await journal.async_load() is not None:
                    raise
            ir.async_delete_issue(
                hass,
                DOMAIN,
                _provisioning_repair_issue_id(record.transaction_id),
            )
            return
        if subentry_owner is not None:
            if record.phase is not ProvisioningPhase.PENDING_CONFIG_COMMIT:
                await _async_report_ownership_unproven(
                    hass,
                    record,
                    original_code="entry_removed",
                )
                return
        elif not empty_owner and not pre_subentry_fleet_owner:
            await _async_report_ownership_unproven(
                hass,
                record,
                original_code="entry_removed",
            )
            return
        elif record.phase not in {
            ProvisioningPhase.STAGED,
            ProvisioningPhase.ACTIVATION_PENDING,
            ProvisioningPhase.ACTIVATED,
            ProvisioningPhase.VERIFYING,
            ProvisioningPhase.PENDING_CONFIG_COMMIT,
            ProvisioningPhase.ROLLBACK_PENDING,
            ProvisioningPhase.ROLLED_BACK,
        }:
            await _async_report_ownership_unproven(
                hass,
                record,
                original_code="entry_removed",
            )
            return
        _async_abort_owned_panel_flows(hass, entry, record.transaction_id)
        await _get_recovery_provisioner(hass).async_recover()
        ir.async_delete_issue(
            hass,
            DOMAIN,
            _provisioning_repair_issue_id(record.transaction_id),
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        if record is not None:
            await _ProvisioningRepairReporter(hass).async_report_rollback_failure(
                record.transaction_id,
                original_code="entry_removed",
                rollback_code="recovery_failed",
            )


async def async_recover_removed_entry(
    hass: HomeAssistant,
    entry: ConfigEntry[Any],
) -> None:
    """Drain removal recovery under cancellation without exposing raw failures."""
    recovery = asyncio.create_task(
        _async_recover_removed_entry(hass, entry),
        name="brilliant-mqtt-removed-entry-recovery",
    )
    owner = asyncio.current_task()
    observed_cancellations = owner.cancelling() if owner is not None else 0
    cancellation: asyncio.CancelledError | None = None
    while not recovery.done():
        try:
            await asyncio.shield(recovery)
        except asyncio.CancelledError as error:
            current_cancellations = owner.cancelling() if owner is not None else 0
            if current_cancellations > observed_cancellations:
                cancellation = error
                observed_cancellations = current_cancellations
            if recovery.done():
                break
    failure: BaseException | None = None
    try:
        recovery.result()
    except BaseException as error:
        failure = error
    if cancellation is not None:
        raise cancellation
    if failure is not None:
        raise failure


def _shared_ssh_lock(hass: HomeAssistant) -> asyncio.Lock:
    """Reuse the domain lock shared with legacy config-flow SSH operations."""
    domain_data: dict[str, Any] = hass.data.setdefault(DOMAIN, {})
    lock: asyncio.Lock = domain_data.setdefault("ssh_lock", asyncio.Lock())
    return lock


def _legacy_mapping(
    data: Mapping[str, Any],
    key: str,
) -> MappingProxyType[str, Any]:
    """Return one detached legacy global mapping or fail without exposing data."""
    value = data.get(key, {})
    if not isinstance(value, Mapping) or any(not isinstance(item, str) for item in value):
        raise EntryDataError("invalid_legacy_fleet_data")
    return MappingProxyType(dict(value))


def legacy_fleet_config(entry: ConfigEntry[Any]) -> FleetConfig:
    """Normalize one legacy panel entry into the immutable fleet-global contract."""
    data = entry.data
    panel = data.get(CONF_PANEL)
    if not isinstance(panel, str) or not panel:
        raise EntryDataError("invalid_legacy_fleet_data")
    try:
        _panel_agent_cadences(data)
    except EntryDataError:
        raise EntryDataError("invalid_legacy_fleet_data") from None

    broker_data = dict(data)
    broker_data.setdefault(CONF_BROKER_KIND, BrokerKind.EXISTING_BROKER.value)
    broker_data.setdefault(CONF_MQTT_TLS_CA, None)
    try:
        broker = BrokerProfile.from_mapping(broker_data)
    except Exception:
        raise EntryDataError("invalid_legacy_fleet_data") from None

    enabled = data.get(CONF_HA_CONTROL_ENABLED, DEFAULT_HA_CONTROL_ENABLED)
    label = data.get(CONF_HA_CONTROL_LABEL, DEFAULT_HA_CONTROL_LABEL)
    domains = data.get(CONF_HA_CONTROL_DOMAINS, DEFAULT_HA_CONTROL_DOMAINS)
    maximum = data.get(CONF_MAX_MIRRORED_ENTITIES, DEFAULT_MAX_MIRRORED_ENTITIES)
    scene_panel = data.get(CONF_SCENE_PANEL, panel)
    mesh_priority = data.get(CONF_MESH_PRIORITY, 0)
    if (
        type(enabled) is not bool
        or not isinstance(label, str)
        or not label
        or not isinstance(domains, (list, tuple))
        or any(not isinstance(domain, str) or not domain for domain in domains)
        or type(maximum) is not int
        or maximum < 1
        or not isinstance(scene_panel, str)
        or not scene_panel
        or type(mesh_priority) is not int
        or mesh_priority < 0
    ):
        raise EntryDataError("invalid_legacy_fleet_data")

    return FleetConfig(
        entry_kind=ENTRY_KIND_LEGACY_PENDING_CONSOLIDATION,
        broker=broker,
        next_mesh_priority=max(1, mesh_priority + 1),
        ha_control_enabled=enabled,
        ha_control_label=label,
        room_overrides=_legacy_mapping(data, CONF_ROOM_OVERRIDES),
        ha_control_domains=tuple(domains),
        max_mirrored_entities=maximum,
        scene_panel=scene_panel,
        scene_actions=_legacy_mapping(data, CONF_SCENE_ACTIONS),
        schema_version=entry.version,
    )


async def _async_drain(
    operations: Iterable[Coroutine[Any, Any, None]],
) -> list[BaseException]:
    """Settle every independent cleanup and return failures in input order."""
    tasks = [
        asyncio.create_task(operation, name="brilliant-mqtt-panel-shutdown")
        for operation in operations
    ]
    if not tasks:
        return []
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return [result for result in results if isinstance(result, BaseException)]


async def _async_settle_cleanup(operation: Coroutine[Any, Any, None]) -> None:
    """Drain cleanup through caller cancellation, then preserve its primary result."""
    cleanup_task = asyncio.create_task(operation, name="brilliant-mqtt-fleet-cleanup")
    owner = asyncio.current_task()
    baseline = owner.cancelling() if owner is not None else 0
    cancellation: asyncio.CancelledError | None = None
    try:
        await asyncio.shield(cleanup_task)
    except asyncio.CancelledError as error:
        if owner is None or owner.cancelling() <= baseline:
            await cleanup_task
            return
        cancellation = error
        while not cleanup_task.done():
            try:
                await asyncio.shield(cleanup_task)
            except asyncio.CancelledError as repeated:
                cancellation = repeated
                continue
    try:
        cleanup_task.result()
    except BaseException:
        if cancellation is None:
            raise
        _LOGGER.warning("Fleet cleanup also failed while its caller was cancelled")
    if cancellation is not None:
        raise cancellation


class FleetManager:
    """Own all panel managers below one fleet or one legacy compatibility entry."""

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry[Any]) -> None:
        self.hass = hass
        self.entry = entry
        self._ssh_lock = _shared_ssh_lock(hass)
        self._lifecycle_lock = asyncio.Lock()
        self._panels: dict[str, PanelManager] = {}
        self._panels_view: Mapping[str, PanelManager] = MappingProxyType(self._panels)
        self._panel_configs: dict[str, PanelConfig] = {}
        self._runtime_owned_panel_ids: set[str] = set()
        self._pending_handoff: _PendingHandoff | None = None
        self._control_plane_reload_generation = 0
        self._pending_control_plane_reload: _PendingControlPlaneReload | None = None
        self._recovery_task: asyncio.Task[None] | None = None
        self._recovery_transaction_id: UUID | None = None
        self._fleet: FleetConfig | None = None
        self._legacy = False
        self._update_unsub: CALLBACK_TYPE | None = None
        self._mqtt_unsub: CALLBACK_TYPE | None = None
        self._shutting_down = False
        self._reload_scheduled = False
        self.broker_available: bool | None = None

    @property
    def panels(self) -> Mapping[str, PanelManager]:
        """Return a live, read-only view keyed by subentry or legacy entry ID."""
        return self._panels_view

    @property
    def fleet(self) -> FleetConfig:
        """Return the normalized fleet globals after setup has parsed them."""
        if self._fleet is None:
            raise RuntimeError("fleet_not_initialized")
        return self._fleet

    @property
    def broker_issue_id(self) -> str:
        """Return the one stable Home Assistant MQTT outage issue ID."""
        return f"broker_unavailable_{self.entry.entry_id}"

    @property
    def runtime_setup_issue_id(self) -> str:
        """Return the stable issue ID for an entry with no usable panel runtime."""
        return f"runtime_setup_failed_{self.entry.entry_id}"

    def _assert_no_orphaned_handoff(self) -> None:
        """Never load a temporary owner marker without its exact safety journal."""
        if self.entry.data.get(CONF_ENTRY_KIND) != ENTRY_KIND_FLEET:
            return
        if (
            isinstance(self.entry.data.get(CONF_SCENE_PANEL), str)
            and str(self.entry.data[CONF_SCENE_PANEL]).startswith(_PENDING_SCENE_OWNER_PREFIX)
        ) or any(
            CONF_PROVISIONING_TRANSACTION_ID in subentry.data
            for subentry in self.entry.subentries.values()
        ):
            raise EntryDataError("orphaned_provisioning_handoff")

    def _reconciled_next_mesh_priority(self) -> int | None:
        """Return the smallest positive priority unused by persisted panels."""
        used: set[int] = set()
        for subentry in self.entry.subentries.values():
            priority = subentry.data.get(CONF_MESH_PRIORITY)
            if type(priority) is not int or priority < 0:
                return None
            if priority > 0:
                used.add(priority)
        candidate = 1
        while candidate in used:
            candidate += 1
        return candidate

    async def _async_persist_parent_normalization(
        self,
        handoff: _PendingHandoff | None,
    ) -> None:
        """Persist ownership strictly; queue recomputable allocation repairs normally."""
        if self.entry.data.get(CONF_ENTRY_KIND) != ENTRY_KIND_FLEET:
            return
        updated = dict(self.entry.data)
        if handoff is not None:
            current_owner = updated.get(CONF_SCENE_PANEL)
            expected_placeholder = pending_scene_owner(handoff.record.transaction_id)
            first_subentry = (
                len(self.entry.subentries) == 1 and handoff.subentry_id in self.entry.subentries
            )
            if current_owner == expected_placeholder or (
                current_owner == FLEET_SCENE_OWNER_UNASSIGNED and first_subentry
            ):
                updated[CONF_SCENE_PANEL] = handoff.subentry_id
            elif current_owner not in self.entry.subentries:
                raise EntryDataError("provisioning_ownership_mismatch")
        next_priority = self._reconciled_next_mesh_priority()
        if next_priority is not None:
            updated[CONF_NEXT_MESH_PRIORITY] = next_priority
        if updated == self.entry.data:
            return
        if self.hass.config_entries.async_get_entry(self.entry.entry_id) is not self.entry:
            # FleetManager's production lifecycle only receives registered
            # entries. Keep direct unit construction read-only.
            if handoff is not None:
                raise EntryDataError("provisioning_entry_not_persisted")
            return
        self.hass.config_entries.async_update_entry(self.entry, data=updated)
        if handoff is None:
            # The allocator hint is derived from immutable panel priorities and
            # safely repaired again after restart. HA's scheduled Store save is
            # sufficient; slow storage must not hold healthy runtimes offline.
            return
        try:
            await _async_settle_cleanup(async_wait_config_entry_persisted(self.hass, self.entry))
        except asyncio.CancelledError:
            raise
        except Exception:
            raise ConfigEntryNotReady(
                "Fleet ownership storage is temporarily unavailable"
            ) from None

    async def _async_background_recovery(
        self,
        record: ProvisioningRecord,
    ) -> None:
        """Settle one orphan journal without holding healthy panel runtime offline."""
        reporter = _ProvisioningRepairReporter(self.hass)
        try:
            await _get_recovery_provisioner(self.hass).async_recover()
            remaining = await ProvisioningJournal(self.hass).async_load()
        except asyncio.CancelledError:
            raise
        except Exception:
            await reporter.async_report_rollback_failure(
                record.transaction_id,
                original_code="restart_recovery",
                rollback_code="recovery_failed",
            )
            return
        if remaining is not None and remaining.transaction_id == record.transaction_id:
            lookup = await _async_transaction_lookup(self.hass, record.transaction_id)
            if lookup.state is TransactionLookupState.FLOW_PENDING:
                ir.async_delete_issue(
                    self.hass,
                    DOMAIN,
                    _provisioning_repair_issue_id(record.transaction_id),
                )
                return
            await reporter.async_report_rollback_failure(
                record.transaction_id,
                original_code="restart_recovery",
                rollback_code="recovery_incomplete",
            )
            return
        ir.async_delete_issue(
            self.hass,
            DOMAIN,
            _provisioning_repair_issue_id(record.transaction_id),
        )
        self._async_schedule_reload_once()

    def _async_schedule_background_recovery(
        self,
        record: ProvisioningRecord,
    ) -> None:
        """Create at most one recovery task for this runtime generation."""
        if (
            self._recovery_task is not None
            and self._recovery_transaction_id == record.transaction_id
            and not self._recovery_task.done()
        ):
            return
        self._recovery_transaction_id = record.transaction_id
        self._recovery_task = self.entry.async_create_background_task(
            self.hass,
            self._async_background_recovery(record),
            "brilliant-mqtt-provisioning-recovery",
        )

    async def _async_prepare_provisioning(self) -> None:
        """Bind cheap handoffs now and schedule orphan network recovery separately."""
        self._pending_handoff = None
        journal = ProvisioningJournal(self.hass)
        try:
            record = await journal.async_load()
        except asyncio.CancelledError:
            raise
        except Exception:
            raise ConfigEntryNotReady(
                "Provisioning recovery state is temporarily unavailable"
            ) from None

        if record is None:
            self._assert_no_orphaned_handoff()
            await self._async_persist_parent_normalization(None)
            return

        if _has_other_domain_entry(self.hass, self.entry):
            await _async_report_ownership_unproven(
                self.hass,
                record,
                original_code="restart_recovery",
            )
            raise EntryDataError("provisioning_ownership_mismatch")
        try:
            candidate = (
                _owned_subentry(self.entry, record)
                if self.entry.data.get(CONF_ENTRY_KIND) == ENTRY_KIND_FLEET
                else None
            )
        except EntryDataError:
            await _async_report_ownership_unproven(
                self.hass,
                record,
                original_code="restart_recovery",
            )
            raise
        if candidate is not None:
            if record.phase not in {
                ProvisioningPhase.PENDING_CONFIG_COMMIT,
                ProvisioningPhase.COMMITTED,
            }:
                await _async_report_ownership_unproven(
                    self.hass,
                    record,
                    original_code="restart_recovery",
                )
                raise EntryDataError("provisioning_ownership_mismatch")
            if record.phase is ProvisioningPhase.COMMITTED:
                if (
                    candidate.data.get(CONF_PROVISIONING_TRANSACTION_ID) is not None
                    or self.entry.data.get(CONF_SCENE_PANEL) != candidate.subentry_id
                ):
                    await _ProvisioningRepairReporter(self.hass).async_report_rollback_failure(
                        record.transaction_id,
                        original_code="restart_recovery",
                        rollback_code="ownership_unproven",
                    )
                    raise EntryDataError("provisioning_ownership_mismatch")

                async def clear_committed() -> None:
                    await journal.async_clear_committed(record.transaction_id)
                    ir.async_delete_issue(
                        self.hass,
                        DOMAIN,
                        _provisioning_repair_issue_id(record.transaction_id),
                    )

                try:
                    await _async_settle_cleanup(clear_committed())
                except asyncio.CancelledError:
                    raise
                except Exception:
                    await _ProvisioningRepairReporter(self.hass).async_report_rollback_failure(
                        record.transaction_id,
                        original_code="restart_recovery",
                        rollback_code="committed_clear_failed",
                    )
                    raise ConfigEntryNotReady(
                        "Committed provisioning cleanup is temporarily unavailable"
                    ) from None
                await self._async_persist_parent_normalization(None)
                return
            handoff = _PendingHandoff(record, candidate.subentry_id)
            await self._async_persist_parent_normalization(handoff)
            self._pending_handoff = handoff
            return

        if (
            record.phase is ProvisioningPhase.COMMITTED
            and _is_exact_fleet_entry(self.entry)
            and _matches_record_fleet(self.entry, record)
        ):
            await _ProvisioningRepairReporter(self.hass).async_report_rollback_failure(
                record.transaction_id,
                original_code="restart_recovery",
                rollback_code="ownership_unproven",
            )
            raise EntryDataError("provisioning_ownership_mismatch")

        lookup = await _async_transaction_lookup(self.hass, record.transaction_id)
        if lookup.state is not TransactionLookupState.FLOW_PENDING:
            if _is_exact_fleet_entry(self.entry) and _matches_record_fleet(self.entry, record):
                self._async_schedule_background_recovery(record)
            else:
                await _ProvisioningRepairReporter(self.hass).async_report_rollback_failure(
                    record.transaction_id,
                    original_code="restart_recovery",
                    rollback_code="ownership_unproven",
                )

        self._assert_no_orphaned_handoff()
        await self._async_persist_parent_normalization(None)

    async def _async_complete_pending_handoff(self) -> bool:
        """Remove the temporary field, persist it, then commit the exact journal."""
        handoff = self._pending_handoff
        if handoff is None:
            return False
        panel_id = handoff.subentry_id
        manager = self._panels.get(panel_id)
        if (
            manager is None
            or panel_id not in self._runtime_owned_panel_ids
            or manager.store.subentry_id != panel_id
            or manager.panel != handoff.record.panel_request.slug
            or manager.management_id != handoff.record.panel_request.fingerprint
        ):
            raise ConfigEntryNotReady("Provisioned panel runtime ownership is not ready")
        subentry = self.entry.subentries.get(panel_id)
        if subentry is None or not _matches_record_identity(
            subentry,
            handoff.record,
        ):
            raise EntryDataError("provisioning_ownership_mismatch")
        marker = subentry.data.get(CONF_PROVISIONING_TRANSACTION_ID)
        if marker is not None:
            if marker != str(handoff.record.transaction_id):
                raise EntryDataError("provisioning_ownership_mismatch")
            updated = dict(subentry.data)
            del updated[CONF_PROVISIONING_TRANSACTION_ID]
            if not self.hass.config_entries.async_update_subentry(
                self.entry,
                subentry,
                data=updated,
            ):
                raise ConfigEntryNotReady("Provisioned panel ownership marker was not removed")
        try:
            await _async_settle_cleanup(
                async_wait_config_entry_persisted(
                    self.hass,
                    self.entry,
                    subentry_id=panel_id,
                )
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            raise ConfigEntryNotReady(
                "Panel ownership storage is temporarily unavailable"
            ) from None

        async def complete_commit() -> None:
            await ProvisioningJournal(self.hass).async_complete_commit(
                handoff.record.transaction_id,
                subentry_id=panel_id,
            )
            ir.async_delete_issue(
                self.hass,
                DOMAIN,
                _provisioning_repair_issue_id(handoff.record.transaction_id),
            )

        try:
            await _async_settle_cleanup(complete_commit())
        except asyncio.CancelledError:
            raise
        except Exception:
            raise ConfigEntryNotReady("Provisioning journal commit has not completed") from None
        self._pending_handoff = None
        return True

    def _build_stores(
        self,
    ) -> tuple[FleetConfig, dict[str, tuple[PanelConfigStore, PanelConfig | None]], bool]:
        """Parse every owner and reject collisions before constructing a manager."""
        if self.entry.version != CONFIG_ENTRY_VERSION:
            raise EntryDataError("invalid_entry_version")
        entry_kind = self.entry.data.get(CONF_ENTRY_KIND)
        if entry_kind not in {
            ENTRY_KIND_FLEET,
            ENTRY_KIND_LEGACY_PENDING_CONSOLIDATION,
        }:
            raise EntryDataError("invalid_entry_kind")

        if entry_kind == ENTRY_KIND_FLEET:
            fleet = FleetConfig.from_entry(self.entry)
            built: dict[str, tuple[PanelConfigStore, PanelConfig | None]] = {}
            slugs: set[str] = set()
            fingerprints: set[str] = set()
            management_ids: set[str] = set()
            positive_mesh_priorities: set[int] = set()
            for subentry in self.entry.subentries.values():
                config = PanelConfig.from_subentry(subentry)
                if config.ssh_username != "root":
                    raise EntryDataError("invalid_panel_ssh_username")
                try:
                    HostIdentity(
                        public_key=config.ssh_host_key,
                        fingerprint=config.identity_fingerprint,
                    )
                except PanelIdentityError:
                    raise EntryDataError("invalid_panel_identity") from None
                if config.panel in slugs:
                    raise EntryDataError("duplicate_panel_slug")
                if config.identity_fingerprint in fingerprints:
                    raise EntryDataError("duplicate_panel_fingerprint")
                if config.management_id in management_ids:
                    raise EntryDataError("duplicate_panel_management_id")
                if config.mesh_priority > 0 and config.mesh_priority in positive_mesh_priorities:
                    raise EntryDataError("duplicate_panel_mesh_priority")
                slugs.add(config.panel)
                fingerprints.add(config.identity_fingerprint)
                management_ids.add(config.management_id)
                if config.mesh_priority > 0:
                    positive_mesh_priorities.add(config.mesh_priority)
                fleet_store = FleetPanelStore(self.hass, self.entry, subentry)
                built[fleet_store.panel_id] = (fleet_store, config)
            if built and fleet.scene_panel not in built:
                raise EntryDataError("invalid_scene_panel")
            if not built and fleet.scene_panel != FLEET_SCENE_OWNER_UNASSIGNED:
                raise EntryDataError("invalid_scene_panel")
            return fleet, built, False

        if self.entry.subentries:
            raise EntryDataError("legacy_subentries_unsupported")
        legacy_store = LegacyPanelStore(self.hass, self.entry)
        return (
            legacy_fleet_config(self.entry),
            {legacy_store.panel_id: (legacy_store, None)},
            True,
        )

    def _release_shared_owners(self) -> list[BaseException]:
        """Attempt every synchronous owner release and return failures in order."""
        failures: list[BaseException] = []
        for attribute in ("_update_unsub", "_mqtt_unsub"):
            unsubscribe = getattr(self, attribute)
            setattr(self, attribute, None)
            if unsubscribe is None:
                continue
            try:
                unsubscribe()
            except BaseException as error:
                failures.append(error)
        try:
            ir.async_delete_issue(self.hass, DOMAIN, self.broker_issue_id)
        except BaseException as error:
            failures.append(error)
        return failures

    async def _async_cleanup_failed_setup(
        self,
        managers: Iterable[PanelManager],
    ) -> None:
        """Release every partially registered owner without masking setup failure."""
        failures = self._release_shared_owners()
        failures.extend(await _async_drain(manager.async_shutdown() for manager in managers))
        self._panels.clear()
        self._panel_configs.clear()
        self._runtime_owned_panel_ids.clear()
        self._pending_handoff = None
        self._fleet = None
        self._legacy = False
        self.broker_available = None
        for failure in failures:
            _LOGGER.warning(
                "Fleet setup cleanup failed (%s)",
                type(failure).__name__,
            )

    async def _async_cleanup_staged_additions(
        self,
        managers: Iterable[PanelManager],
    ) -> None:
        """Drain every unpublished live addition without masking its failure."""
        failures = await _async_drain(manager.async_shutdown() for manager in managers)
        for failure in failures:
            _LOGGER.warning(
                "Live-add cleanup failed (%s)",
                type(failure).__name__,
            )

    async def _async_shutdown_removed(
        self,
        managers: Iterable[PanelManager],
    ) -> None:
        """Drain removed runtimes, delete their issues, and surface cleanup failure."""
        removed = tuple(managers)
        failures = await _async_drain(manager.async_shutdown() for manager in removed)
        for manager in removed:
            try:
                async_delete_panel_issues(self.hass, manager.management_id)
            except BaseException as error:
                failures.append(error)
        if not failures:
            return
        for failure in failures[1:]:
            _LOGGER.warning(
                "Additional removed-panel cleanup failed (%s)",
                type(failure).__name__,
            )
        raise failures[0]

    @staticmethod
    def _assert_immutable_panel(
        previous: PanelConfig,
        current: PanelConfig,
    ) -> None:
        """Fail closed if a live update bypasses the store's immutable fields."""
        if (
            current.panel != previous.panel
            or current.identity_fingerprint != previous.identity_fingerprint
            or current.management_id != previous.management_id
            or current.ssh_host_key != previous.ssh_host_key
        ):
            raise EntryDataError("immutable_panel_identity")

    async def _async_setup_manager(self, manager: PanelManager) -> bool:
        """Start one manager and report whether it owns a usable runtime."""
        try:
            await manager.async_setup()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            _LOGGER.warning(
                "%s: panel runtime loaded degraded after %s",
                manager.panel,
                type(error).__name__,
            )
            manager.mark_runtime_degraded(f"runtime setup failed ({type(error).__name__})")
            return False
        return True

    @callback
    def _async_create_runtime_setup_issue(self) -> None:
        """Surface a redacted, actionable issue while HA retries the entry."""
        ir.async_create_issue(
            self.hass,
            DOMAIN,
            self.runtime_setup_issue_id,
            is_fixable=False,
            severity=ir.IssueSeverity.ERROR,
            translation_key="needs_attention",
            translation_placeholders={
                "panel": "Brilliant MQTT fleet",
                "reason": _RUNTIME_SETUP_ISSUE_REASON,
            },
            learn_more_url=(
                "https://github.com/joyfulhouse/brilliant-mqtt/blob/main/docs/ha-integration.md"
            ),
        )

    @callback
    def _async_schedule_reload_once(self) -> None:
        """Schedule one structural/recovery reload for this runtime generation."""
        if self._reload_scheduled or self._shutting_down:
            return
        self._reload_scheduled = True
        self.hass.config_entries.async_schedule_reload(self.entry.entry_id)

    @callback
    def _async_broker_status(self, connected: bool) -> None:
        """Maintain one broker issue rather than one issue per panel."""
        if self._shutting_down:
            return
        self.broker_available = connected
        if connected:
            ir.async_delete_issue(self.hass, DOMAIN, self.broker_issue_id)
            return
        ir.async_create_issue(
            self.hass,
            DOMAIN,
            self.broker_issue_id,
            is_fixable=False,
            severity=ir.IssueSeverity.ERROR,
            translation_key="needs_attention",
            translation_placeholders={
                "panel": "Brilliant MQTT fleet",
                "reason": _BROKER_ISSUE_REASON,
            },
            learn_more_url=(
                "https://github.com/joyfulhouse/brilliant-mqtt/blob/main/docs/ha-integration.md"
            ),
        )

    async def async_setup(self) -> None:
        """Build all stores first, then start panel runtimes independently."""
        async with self._lifecycle_lock:
            await self._async_prepare_provisioning()
            fleet, built, legacy = self._build_stores()
            managers = {
                panel_id: PanelManager(self.hass, store, fleet, self._ssh_lock)
                for panel_id, (store, _config) in built.items()
            }
            self._fleet = fleet
            self._legacy = legacy
            self._panel_configs = {
                panel_id: config
                for panel_id, (_store, config) in built.items()
                if config is not None
            }
            self._panels.update(managers)
            try:
                successful_starts = 0
                for panel_id, manager in managers.items():
                    if await self._async_setup_manager(manager):
                        successful_starts += 1
                        self._runtime_owned_panel_ids.add(panel_id)
                if managers and successful_starts == 0:
                    self._async_create_runtime_setup_issue()
                    raise ConfigEntryNotReady("No panel runtime could start")
                ir.async_delete_issue(
                    self.hass,
                    DOMAIN,
                    self.runtime_setup_issue_id,
                )
                await self._async_complete_pending_handoff()
                self._update_unsub = self.entry.add_update_listener(self._async_entry_updated)
                self._mqtt_unsub = mqtt.async_subscribe_connection_status(
                    self.hass, self._async_broker_status
                )
                self._async_broker_status(mqtt.is_connected(self.hass))
            except BaseException:
                await _async_settle_cleanup(
                    self._async_cleanup_failed_setup(tuple(managers.values()))
                )
                raise

    async def _async_entry_updated(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry[Any],
    ) -> None:
        """Reconcile live subentry/global updates without restarting managers."""
        del hass
        if entry is not self.entry or self._shutting_down:
            return
        try:
            reload_required = await self._async_reconcile()
        except ConfigEntryNotReady:
            _LOGGER.warning("Fleet reconciliation deferred; a reload retry is scheduled")
            return
        except EntryDataError:
            # The flow's completion no longer reloads (HA 2026.12 forbids that
            # next to an update listener), so a snapshot the live reconcile
            # refuses — e.g. a host change re-pinning the SSH host key — must
            # apply via a full reload instead of being silently dropped.
            _LOGGER.info(
                "Fleet reconciliation requires a reload to apply this change; scheduling it"
            )
            self._async_schedule_reload_once()
            return
        if reload_required:
            self._async_schedule_reload_once()

    async def _async_reconcile(self) -> bool:
        """Apply one live snapshot and schedule one retry on transient failure."""
        try:
            return await self._async_reconcile_once()
        except ConfigEntryNotReady:
            self._async_schedule_reload_once()
            raise

    async def _async_reconcile_once(self) -> bool:
        """Apply one fully validated entry snapshot under the lifecycle lock."""
        async with self._lifecycle_lock:
            if self._shutting_down:
                return False
            await self._async_prepare_provisioning()
            fleet, built, legacy = self._build_stores()
            if legacy != self._legacy:
                raise EntryDataError("runtime_entry_kind_changed")

            for panel_id, previous in self._panel_configs.items():
                if panel_id in built:
                    current = built[panel_id][1]
                    assert current is not None
                    self._assert_immutable_panel(previous, current)

            staged: dict[str, PanelManager] = {}
            pending_start_failed = False
            try:
                for panel_id, (store, _config) in built.items():
                    if panel_id not in self._panels:
                        staged[panel_id] = PanelManager(
                            self.hass,
                            store,
                            fleet,
                            self._ssh_lock,
                        )
                for panel_id, manager in staged.items():
                    if await self._async_setup_manager(manager):
                        self._runtime_owned_panel_ids.add(panel_id)
                    elif (
                        self._pending_handoff is not None
                        and panel_id == self._pending_handoff.subentry_id
                    ):
                        pending_start_failed = True
                if pending_start_failed:
                    raise ConfigEntryNotReady("Provisioned panel runtime ownership is not ready")
            except BaseException:
                for panel_id in staged:
                    self._runtime_owned_panel_ids.discard(panel_id)
                await _async_settle_cleanup(
                    self._async_cleanup_staged_additions(tuple(staged.values()))
                )
                raise

            removed = tuple(
                (panel_id, manager)
                for panel_id, manager in self._panels.items()
                if panel_id not in built
            )
            panel_configs = {
                panel_id: config
                for panel_id, (_store, config) in built.items()
                if config is not None
            }
            control_plane_target = _control_plane_snapshot(fleet, panel_configs)
            control_plane_changed = (
                not legacy
                and self._fleet is not None
                and _control_plane_snapshot(self._fleet, self._panel_configs)
                != control_plane_target
            )

            self._fleet = fleet
            for panel_id, (store, _config) in built.items():
                if existing := self._panels.get(panel_id):
                    existing.update_config(store, fleet)
            for panel_id, _manager in removed:
                self._panels.pop(panel_id)
                self._runtime_owned_panel_ids.discard(panel_id)
            self._panels.update(staged)

            self._panel_configs = panel_configs
            control_plane_reload_attempt: _PendingControlPlaneReload | None = None
            if not legacy and (
                control_plane_changed or self._pending_control_plane_reload is not None
            ):
                self._control_plane_reload_generation += 1
                control_plane_reload_attempt = _PendingControlPlaneReload(
                    self._control_plane_reload_generation,
                    control_plane_target,
                )
                self._pending_control_plane_reload = control_plane_reload_attempt
            try:
                if removed:
                    await _async_settle_cleanup(
                        self._async_shutdown_removed(
                            tuple(manager for _panel_id, manager in removed)
                        )
                    )
                entry_reload_required = await self._async_complete_pending_handoff()
            except asyncio.CancelledError:
                if control_plane_reload_attempt is not None and not self._shutting_down:
                    self._async_schedule_reload_once()
                raise
            except ConfigEntryNotReady:
                raise
            except Exception:
                raise ConfigEntryNotReady(
                    "Fleet post-update cleanup is temporarily unavailable"
                ) from None

        if control_plane_reload_attempt is not None:
            from .ha_control import get_control_plane

            try:
                await get_control_plane(self.hass).async_reload_settings()
                async with self._lifecycle_lock:
                    if (
                        self._pending_control_plane_reload == control_plane_reload_attempt
                        and self._fleet is not None
                        and _control_plane_snapshot(self._fleet, self._panel_configs)
                        == control_plane_reload_attempt.target
                    ):
                        self._pending_control_plane_reload = None
            except asyncio.CancelledError:
                if not self._shutting_down:
                    self._async_schedule_reload_once()
                raise
            except Exception:
                raise ConfigEntryNotReady(
                    "Control-plane settings reload is temporarily unavailable"
                ) from None
        return entry_reload_required

    async def _async_restore_rebind_snapshot(
        self,
        subentry_id: str,
        *,
        data: Mapping[str, Any],
        title: str,
        unique_id: str | None,
    ) -> None:
        """Restore and prove the exact pre-rebind subentry snapshot."""
        try:
            subentry = self.entry.subentries.get(subentry_id)
            if subentry is None:
                raise ConfigEntryPersistenceError
            self.hass.config_entries.async_update_subentry(
                self.entry,
                subentry,
                data=dict(data),
                title=title,
                unique_id=unique_id,
            )
            if subentry.data != data or subentry.title != title or subentry.unique_id != unique_id:
                raise ConfigEntryPersistenceError
            await async_wait_config_entry_persisted(
                self.hass,
                self.entry,
                subentry_id=subentry_id,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            _async_create_fleet_storage_issue(self.hass, self.entry.entry_id)
            raise ConfigEntryPersistenceError from None
        ir.async_delete_issue(
            self.hass,
            DOMAIN,
            _fleet_storage_issue_id(self.entry.entry_id),
        )

    async def _async_settle_rebind_rollback(
        self,
        subentry_id: str,
        *,
        data: Mapping[str, Any],
        title: str,
        unique_id: str | None,
    ) -> None:
        """Finish an exact rollback despite caller cancellation."""
        try:
            await _async_settle_cleanup(
                self._async_restore_rebind_snapshot(
                    subentry_id,
                    data=data,
                    title=title,
                    unique_id=unique_id,
                )
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOGGER.warning("Panel rebind rollback persistence could not be proven")

    def _rebind_snapshot(
        self,
        subentry_id: str,
        expected: PanelConfig,
    ) -> tuple[FleetConfig, PanelManager, ConfigSubentry]:
        """Return one exact live rebind target while both operation locks are held."""
        if (
            self._shutting_down
            or self._legacy
            or self._fleet is None
            or self.entry.data.get(CONF_ENTRY_KIND) != ENTRY_KIND_FLEET
            or self.hass.config_entries.async_get_entry(self.entry.entry_id) is not self.entry
            or self._pending_handoff is not None
        ):
            raise EntryDataError("panel_rebind_unavailable")

        fleet, built, legacy = self._build_stores()
        manager = self._panels.get(subentry_id)
        built_panel = built.get(subentry_id)
        if legacy or manager is None or built_panel is None:
            raise EntryDataError("panel_rebind_unavailable")
        _store, current = built_panel
        if current is None:
            raise EntryDataError("panel_rebind_unavailable")
        if (
            fleet != self._fleet
            or current != expected
            or self._panel_configs.get(subentry_id) != expected
            or current.provisioning_transaction_id is not None
        ):
            raise EntryDataError("panel_snapshot_changed")

        subentry = self.entry.subentries.get(subentry_id)
        if subentry is None:
            raise EntryDataError("panel_rebind_unavailable")
        if subentry.title != current.name:
            raise EntryDataError("panel_snapshot_changed")
        return fleet, manager, subentry

    async def _async_assert_rebind_journal_clear(self) -> None:
        """Fail closed unless no global panel transaction owns durable recovery."""
        try:
            record = await ProvisioningJournal(self.hass).async_load()
        except asyncio.CancelledError:
            raise
        except Exception:
            raise EntryDataError("panel_rebind_blocked_by_panel_onboarding") from None
        if record is not None:
            raise EntryDataError("panel_rebind_blocked_by_panel_onboarding")

    async def async_rebind_panel(
        self,
        subentry_id: str,
        expected: PanelConfig,
        *,
        host: str,
        root_password: str,
        candidate: HostIdentity,
    ) -> None:
        """Durably replace one explicitly verified panel identity."""
        async with self._lifecycle_lock, self._ssh_lock:
            await self._async_assert_rebind_journal_clear()
            fleet, manager, subentry = self._rebind_snapshot(subentry_id, expected)
            if await _async_duplicate_fingerprint(
                self.hass,
                candidate.fingerprint,
                exclude_entry_id=self.entry.entry_id,
                exclude_subentry_id=subentry_id,
            ):
                raise EntryDataError("duplicate_panel_fingerprint")

            try:
                verified = await async_verify_host_identity(host, candidate)
            except asyncio.CancelledError:
                raise
            except PanelIdentityError as error:
                code = (
                    "panel_rebind_identity_changed"
                    if str(error) == "host_key_changed"
                    else "panel_rebind_identity_unreachable"
                )
                raise EntryDataError(code) from None
            except Exception:
                raise EntryDataError("panel_rebind_identity_unreachable") from None
            if verified != candidate:
                raise EntryDataError("panel_rebind_identity_changed")

            await self._async_assert_rebind_journal_clear()
            fleet, manager, subentry = self._rebind_snapshot(subentry_id, expected)
            if await _async_duplicate_fingerprint(
                self.hass,
                candidate.fingerprint,
                exclude_entry_id=self.entry.entry_id,
                exclude_subentry_id=subentry_id,
            ):
                raise EntryDataError("duplicate_panel_fingerprint")

            original_data = dict(subentry.data)
            original_title = subentry.title
            original_unique_id = subentry.unique_id
            rebound_data = {
                **original_data,
                CONF_HOST: host,
                CONF_ROOT_PASSWORD: root_password,
                CONF_IDENTITY_FINGERPRINT: candidate.fingerprint,
                CONF_SSH_HOST_KEY: candidate.public_key,
            }
            rebound_subentry = ConfigSubentry(
                data=MappingProxyType(rebound_data),
                subentry_id=subentry.subentry_id,
                subentry_type=subentry.subentry_type,
                title=subentry.title,
                unique_id=candidate.fingerprint,
            )
            rebound = PanelConfig.from_subentry(rebound_subentry)

            try:
                changed = self.hass.config_entries.async_update_subentry(
                    self.entry,
                    subentry,
                    data=rebound_data,
                    unique_id=candidate.fingerprint,
                )
            except Exception:
                await self._async_settle_rebind_rollback(
                    subentry_id,
                    data=original_data,
                    title=original_title,
                    unique_id=original_unique_id,
                )
                raise ConfigEntryPersistenceError from None
            if not changed:
                raise EntryDataError("panel_snapshot_changed")

            try:
                await async_wait_config_entry_persisted(
                    self.hass,
                    self.entry,
                    subentry_id=subentry_id,
                )
            except asyncio.CancelledError:
                await self._async_settle_rebind_rollback(
                    subentry_id,
                    data=original_data,
                    title=original_title,
                    unique_id=original_unique_id,
                )
                raise
            except Exception:
                await self._async_settle_rebind_rollback(
                    subentry_id,
                    data=original_data,
                    title=original_title,
                    unique_id=original_unique_id,
                )
                raise ConfigEntryPersistenceError from None

            ir.async_delete_issue(
                self.hass,
                DOMAIN,
                _fleet_storage_issue_id(self.entry.entry_id),
            )
            store = FleetPanelStore(self.hass, self.entry, subentry)
            manager.update_config(store, fleet)
            self._panel_configs[subentry_id] = rebound
            manager._fire(
                EVENT_PANEL_REBOUND,
                {
                    "old_fingerprint": expected.identity_fingerprint,
                    "new_fingerprint": rebound.identity_fingerprint,
                },
            )

    async def async_panel_added(self, subentry_id: str) -> None:
        """Reconcile a newly persisted panel subentry."""
        if self._legacy:
            raise EntryDataError("legacy_subentries_unsupported")
        if await self._async_reconcile():
            self._async_schedule_reload_once()
        if subentry_id not in self._panels:
            raise EntryDataError("panel_subentry_missing")

    async def async_panel_updated(self, subentry_id: str) -> None:
        """Reconcile one persisted panel update in place."""
        if await self._async_reconcile():
            self._async_schedule_reload_once()
        if subentry_id not in self._panels:
            raise EntryDataError("panel_subentry_missing")

    async def async_panel_removed(self, panel_id: str) -> None:
        """Reconcile one subentry removal and drain its manager."""
        if await self._async_reconcile():
            self._async_schedule_reload_once()
        if panel_id in self._panels:
            raise EntryDataError("panel_subentry_still_present")

    async def _async_shutdown_owned(self) -> None:
        """Unregister shared owners and drain every panel despite failures."""
        async with self._lifecycle_lock:
            self._shutting_down = True
            managers = tuple(self._panels.values())
            failures = self._release_shared_owners()
            self._panels.clear()
            self._panel_configs.clear()
            self._runtime_owned_panel_ids.clear()
            self._pending_handoff = None
            failures.extend(await _async_drain(manager.async_shutdown() for manager in managers))
            if failures:
                for secondary in failures[1:]:
                    _LOGGER.warning(
                        "Additional panel shutdown failed (%s)",
                        type(secondary).__name__,
                    )
                raise failures[0]

    async def async_shutdown(self) -> None:
        """Drain all panel runtimes even through failure or caller cancellation."""
        await _async_settle_cleanup(self._async_shutdown_owned())
