"""Operator-triggered, recoverable legacy-to-fleet consolidation.

The coordinator is the only cross-entry writer. Under the domain migration lock
and the shared panel-operation (SSH) lock it fences every Brilliant entry's
setup, quiesces the legacy runtimes, plans with the pure :mod:`.migration`
planner from the exact snapshots it journals, converts the deterministic anchor
(smallest entry ID) into the singleton fleet entry, adds one journaled panel
subentry per legacy entry, retargets the integration-owned entity/device
registry associations, sets up and verifies the fleet runtime, proves config
entries + both registries on disk, persists the ``fleet_verified`` commit
point, and only then removes the sibling entries.

Every journal write and every terminal state is proven on disk (see
:mod:`.migration_persistence`). Before the commit point any failure reverses to
the exact original entries and registry associations; at/after it, recovery
only finishes sibling removal. While a journal is unresolved, the setup fence
keeps mixed fleet/legacy generations from ever starting. Migration never
contacts, reinstalls, restarts, or rewrites a panel.
"""

from __future__ import annotations

import asyncio
import copy
import logging
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from types import MappingProxyType
from typing import Any
from uuid import uuid4

from homeassistant.components import mqtt
from homeassistant.config_entries import ConfigEntry, ConfigEntryState, ConfigSubentry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import issue_registry as ir

from . import fleet_manager
from .const import (
    CONF_ENTRY_KIND,
    CONF_HUE_CA_CERT,
    CONF_MQTT_PASSWORD,
    CONF_MQTT_TLS_CA,
    CONF_MQTT_USERNAME,
    CONF_ROOT_PASSWORD,
    CONFIG_ENTRY_VERSION,
    DOMAIN,
    ENTRY_KIND_FLEET,
    FLEET_UNIQUE_ID,
    SUBENTRY_TYPE_PANEL,
)
from .entry_data import EntryDataError, FleetConfig
from .migration import (
    FLEET_TITLE,
    LegacySnapshot,
    MigrationConflictReport,
    MigrationPlan,
    plan_migration,
    secret_equal,
)
from .migration_journal import (
    COMMITTED_PHASES,
    OUTCOME_MIGRATED,
    OUTCOME_REVERTED,
    DeviceAssociation,
    EntityAssociation,
    EntrySnapshot,
    MigrationJournal,
    MigrationJournalError,
    MigrationPhase,
    MigrationRecord,
    SubentryPlan,
)
from .migration_persistence import (
    PersistenceUnprovenError,
    async_persist_config_entries,
    async_persist_device_registry,
    async_persist_entity_registry,
)
from .provisioning_journal import ProvisioningJournal

_LOGGER = logging.getLogger(__name__)

DATA_MIGRATION_LOCK = "migration_lock"
DATA_MIGRATION_REMOVING = "migration_removing"
DATA_MIGRATION_FENCE = "migration_fence"
ISSUE_MIGRATION_AVAILABLE = "legacy_migration_available"
ISSUE_MIGRATION_RECOVERY_FAILED = "migration_recovery_failed"
ISSUE_MIGRATION_REVERTED_NOT_LOADED = "migration_reverted_not_loaded"
_DOCS_URL = "https://github.com/joyfulhouse/brilliant-mqtt/blob/main/docs/ha-integration.md"
_PANEL_OPERATION_WAIT_SECONDS = 120.0
# Values compared in constant time when proving an entry is unchanged.
_SECRET_DATA_KEYS = frozenset(
    {CONF_ROOT_PASSWORD, CONF_MQTT_PASSWORD, CONF_MQTT_USERNAME, CONF_MQTT_TLS_CA, CONF_HUE_CA_CERT}
)

type Checkpoint = Callable[[str], Awaitable[None]]


class MigrationError(Exception):
    """One stable, value-free migration failure code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class MigrationFence:
    """Keep Brilliant entries from starting while a migration is unresolved.

    ``async_setup_entry`` consults it: while active, only explicitly allowed
    entries (the anchor being verified, or the committed fleet) may set up, so a
    scheduled reload/setup-retry can never start a legacy sibling next to the
    converted fleet. ``quiet`` holds entries whose next setup is a migration
    restoration that must not contact the panel.
    """

    def __init__(self) -> None:
        self.active = False
        self.allowed: set[str] = set()
        self.quiet: set[str] = set()

    def activate(self) -> None:
        self.active = True

    def deactivate(self) -> None:
        self.active = False
        self.allowed.clear()

    def blocks(self, entry_id: str) -> bool:
        return self.active and entry_id not in self.allowed

    def consume_quiet(self, entry_id: str) -> bool:
        if entry_id in self.quiet:
            self.quiet.discard(entry_id)
            return True
        return False


def migration_fence(hass: HomeAssistant) -> MigrationFence:
    """Return the domain-wide setup fence."""
    domain_data: dict[str, Any] = hass.data.setdefault(DOMAIN, {})
    fence = domain_data.get(DATA_MIGRATION_FENCE)
    if not isinstance(fence, MigrationFence):
        fence = MigrationFence()
        domain_data[DATA_MIGRATION_FENCE] = fence
    return fence


def migration_lock(hass: HomeAssistant) -> asyncio.Lock:
    """Return the one domain-wide migration lock."""
    domain_data: dict[str, Any] = hass.data.setdefault(DOMAIN, {})
    lock: asyncio.Lock = domain_data.setdefault(DATA_MIGRATION_LOCK, asyncio.Lock())
    return lock


def migration_removing(hass: HomeAssistant) -> set[str]:
    """Entry IDs the coordinator is removing because their panel now lives in the fleet.

    ``async_remove_entry`` consults this so a migrated sibling's removal never
    deletes issues owned by the panel's unchanged management ID and never runs
    provisioning recovery (which could contact a panel).
    """
    domain_data: dict[str, Any] = hass.data.setdefault(DOMAIN, {})
    removing: set[str] = domain_data.setdefault(DATA_MIGRATION_REMOVING, set())
    return removing


def _is_fleet(entry: ConfigEntry[Any]) -> bool:
    return entry.data.get(CONF_ENTRY_KIND) == ENTRY_KIND_FLEET


@callback
def async_update_migration_issue(hass: HomeAssistant) -> None:
    """Show one informational issue while legacy entries could be consolidated."""
    entries = hass.config_entries.async_entries(DOMAIN)
    legacy = [entry for entry in entries if not _is_fleet(entry)]
    if not legacy or any(_is_fleet(entry) for entry in entries):
        ir.async_delete_issue(hass, DOMAIN, ISSUE_MIGRATION_AVAILABLE)
        return
    ir.async_create_issue(
        hass,
        DOMAIN,
        ISSUE_MIGRATION_AVAILABLE,
        is_fixable=False,
        is_persistent=False,
        severity=ir.IssueSeverity.WARNING,
        translation_key=ISSUE_MIGRATION_AVAILABLE,
        translation_placeholders={"count": str(len(legacy))},
        learn_more_url=_DOCS_URL,
    )


@callback
def async_report_recovery_failure(hass: HomeAssistant, code: str) -> None:
    """Keep one redacted issue while a migration journal is unresolved."""
    ir.async_create_issue(
        hass,
        DOMAIN,
        ISSUE_MIGRATION_RECOVERY_FAILED,
        is_fixable=False,
        is_persistent=False,
        severity=ir.IssueSeverity.ERROR,
        translation_key=ISSUE_MIGRATION_RECOVERY_FAILED,
        translation_placeholders={"code": code},
        learn_more_url=_DOCS_URL,
    )


def _error_code(error: BaseException) -> str:
    if isinstance(error, (MigrationError, MigrationJournalError, PersistenceUnprovenError)):
        return error.code
    if isinstance(error, EntryDataError):
        return str(error)
    return "unexpected_error"


def _blocker(code: str, field: str, entries: Sequence[ConfigEntry[Any]] = ()) -> dict[str, Any]:
    return {
        "code": code,
        "field": field,
        "entries": [{"entry_id": entry.entry_id, "title": entry.title} for entry in entries],
    }


def _text(value: object) -> str | None:
    return None if value is None else str(value)


def _data_matches(live: Mapping[str, Any], snapshot: Mapping[str, Any]) -> bool:
    """Deep-compare entry data, credentials in constant time."""
    if set(live) != set(snapshot):
        return False
    for key, value in snapshot.items():
        if key in _SECRET_DATA_KEYS and (isinstance(value, str) or value is None):
            if not secret_equal(live[key], value):
                return False
        elif live[key] != value:
            return False
    return True


def _conflict_summary(
    result: MigrationPlan | MigrationConflictReport,
    blockers: Sequence[dict[str, Any]],
    candidate_count: int,
    *,
    dry_run: bool,
) -> dict[str, Any]:
    summary: dict[str, Any] = (
        result.summary()
        if isinstance(result, MigrationConflictReport)
        else {"status": "conflicts", "candidate_count": candidate_count, "conflicts": []}
    )
    summary["status"] = "conflicts"
    summary["conflicts"] = [*summary["conflicts"], *blockers]
    summary["dry_run"] = dry_run
    return summary


class MigrationCoordinator:
    """Run or recover one legacy-to-fleet migration."""

    def __init__(self, hass: HomeAssistant, *, checkpoint: Checkpoint | None = None) -> None:
        self.hass = hass
        self._journal = MigrationJournal(hass)
        self._checkpoint = checkpoint
        self._fence = migration_fence(hass)
        # The latest record proven on disk; exception handling keys off it.
        self._durable: MigrationRecord | None = None

    async def _point(self, name: str) -> None:
        """Failure-injection seam between every journal save and mutation."""
        if self._checkpoint is not None:
            await self._checkpoint(name)

    async def _save(self, record: MigrationRecord) -> MigrationRecord:
        await self._point(f"before_save:{record.phase.value}")
        await self._journal.async_save(record)
        self._durable = record
        await self._point(f"after_save:{record.phase.value}")
        return record

    async def _async_remove_journal(self) -> None:
        await self._point("remove_journal")
        await self._journal.async_remove()
        self._durable = None
        self._fence.deactivate()
        ir.async_delete_issue(self.hass, DOMAIN, ISSUE_MIGRATION_RECOVERY_FAILED)

    # ------------------------------------------------------------------ public

    async def async_run(self, *, dry_run: bool) -> dict[str, Any]:
        """Plan (dry run) or execute the migration and return a secret-free outcome."""
        async with migration_lock(self.hass):
            try:
                pending = await self._journal.async_load()
            except MigrationJournalError as error:
                self._fence.activate()
                async_report_recovery_failure(self.hass, error.code)
                return {"status": "recovery_failed", "dry_run": dry_run, "error": error.code}
            if pending is not None:
                if dry_run:
                    return {
                        "status": "recovery_pending",
                        "dry_run": True,
                        "phase": pending.phase.value,
                    }
                return await self._async_recover_locked(pending, live=True)

            entries = self.hass.config_entries.async_entries(DOMAIN)
            if not entries:
                return {"status": "nothing_to_migrate", "dry_run": dry_run}
            fleets = [entry for entry in entries if _is_fleet(entry)]
            if fleets and len(fleets) == len(entries):
                return {
                    "status": "already_migrated",
                    "dry_run": dry_run,
                    "fleet_entry_id": fleets[0].entry_id,
                }

            # Fail fast on an ineligible fleet; the binding plan is recomputed
            # below from the exact quiesced snapshots that get journaled.
            result = plan_migration([LegacySnapshot.from_entry(entry) for entry in entries])
            blockers = await self._async_blockers(entries)
            if isinstance(result, MigrationConflictReport) or blockers:
                return _conflict_summary(result, blockers, len(entries), dry_run=dry_run)
            if dry_run:
                return {**result.summary(), "dry_run": True}
            if not await mqtt.async_wait_for_mqtt_client(self.hass):
                return _conflict_summary(
                    result, [_blocker("mqtt_unavailable", "mqtt")], len(entries), dry_run=False
                )

            ssh_lock = fleet_manager._shared_ssh_lock(self.hass)
            try:
                async with asyncio.timeout(_PANEL_OPERATION_WAIT_SECONDS):
                    await ssh_lock.acquire()
            except TimeoutError:
                return _conflict_summary(
                    result,
                    [_blocker("panel_operation_in_progress", "ssh")],
                    len(entries),
                    dry_run=False,
                )
            try:
                return await self._async_execute()
            finally:
                ssh_lock.release()

    async def async_recover(self) -> dict[str, Any] | None:
        """Startup recovery: settle a journaled migration before entries load.

        The fence is raised first and stays raised unless recovery resolves the
        journal, so a failed recovery can never start mixed generations.
        """
        async with migration_lock(self.hass):
            self._fence.activate()
            try:
                record = await self._journal.async_load()
            except MigrationJournalError as error:
                async_report_recovery_failure(self.hass, error.code)
                return {"status": "recovery_failed", "error": error.code}
            if record is None:
                self._fence.deactivate()
                return None
            return await self._async_recover_locked(record, live=False)

    # --------------------------------------------------------------- planning

    async def _async_blockers(self, entries: Sequence[ConfigEntry[Any]]) -> list[dict[str, Any]]:
        """Runtime preconditions the pure planner cannot see."""
        blockers: list[dict[str, Any]] = []
        provisioning = ProvisioningJournal(self.hass)
        try:
            if await provisioning.async_load() is not None:
                blockers.append(_blocker("provisioning_in_progress", "provisioning"))
            if await provisioning.async_retained_records():
                blockers.append(_blocker("retained_canary_present", "provisioning"))
        except Exception:
            blockers.append(_blocker("provisioning_journal_unavailable", "provisioning"))

        unrecoverable = [entry for entry in entries if not entry.state.recoverable]
        if unrecoverable:
            blockers.append(_blocker("entry_not_recoverable", "state", unrecoverable))

        if self._entry_flows_in_progress({entry.entry_id for entry in entries}):
            blockers.append(_blocker("config_flow_in_progress", "flow"))

        registry = er.async_get(self.hass)
        foreign = [
            entry
            for entry in entries
            if any(
                item.platform != DOMAIN
                for item in er.async_entries_for_config_entry(registry, entry.entry_id)
            )
        ]
        if foreign:
            blockers.append(_blocker("foreign_registry_entity", "entity_registry", foreign))
        return blockers

    def _entry_flows_in_progress(self, entry_ids: set[str]) -> bool:
        """Detect reconfigure/options/subentry flows that could write a candidate."""
        manager = self.hass.config_entries
        for flow in manager.flow.async_progress_by_handler(DOMAIN, include_uninitialized=True):
            if flow["context"].get("entry_id") in entry_ids:
                return True
        for options_flow in manager.options.async_progress(include_uninitialized=True):
            if options_flow["handler"] in entry_ids:
                return True
        for subentry_flow in manager.subentries.async_progress(include_uninitialized=True):
            handler: object = subentry_flow["handler"]
            if isinstance(handler, tuple) and handler and handler[0] in entry_ids:
                return True
        return False

    def _build_record(
        self, plan: MigrationPlan, snapshots: Sequence[LegacySnapshot]
    ) -> MigrationRecord:
        """Journal exactly the snapshots the plan was computed from."""
        by_id = {snapshot.entry_id: snapshot for snapshot in snapshots}
        entries = [
            EntrySnapshot(
                entry_id=entry_id,
                title=by_id[entry_id].title,
                unique_id=by_id[entry_id].unique_id,
                version=by_id[entry_id].version,
                minor_version=by_id[entry_id].minor_version,
                data=copy.deepcopy(dict(by_id[entry_id].data)),
                options=copy.deepcopy(dict(by_id[entry_id].options)),
            )
            for entry_id in plan.candidate_ids
        ]
        ent_reg = er.async_get(self.hass)
        dev_reg = dr.async_get(self.hass)
        entities: list[EntityAssociation] = []
        devices: dict[str, DeviceAssociation] = {}
        for entry_id in plan.candidate_ids:
            for item in er.async_entries_for_config_entry(ent_reg, entry_id):
                entities.append(
                    EntityAssociation(
                        entity_id=item.entity_id,
                        unique_id=item.unique_id,
                        platform=item.platform,
                        config_entry_id=entry_id,
                        config_subentry_id=item.config_subentry_id,
                    )
                )
            for device in dr.async_entries_for_config_entry(dev_reg, entry_id):
                devices[device.id] = DeviceAssociation(
                    device_id=device.id,
                    associations={
                        owner: tuple(sorted(subentries, key=lambda item: item or ""))
                        for owner, subentries in sorted(device.config_entries_subentries.items())
                    },
                )
        return MigrationRecord(
            transaction_id=str(uuid4()),
            plan_digest=plan.plan_digest,
            phase=MigrationPhase.PLANNED,
            anchor_entry_id=plan.anchor_entry_id,
            candidates=plan.candidate_ids,
            entries=tuple(entries),
            fleet_title=FLEET_TITLE,
            fleet_unique_id=FLEET_UNIQUE_ID,
            fleet_data=dict(plan.fleet_data),
            fleet_options=dict(plan.fleet_options),
            subentries=tuple(
                SubentryPlan(
                    entry_id=panel.entry_id,
                    subentry_id=panel.subentry_id,
                    title=panel.title,
                    unique_id=panel.fingerprint,
                    data=dict(panel.data),
                )
                for panel in plan.panels
            ),
            entities=tuple(sorted(entities, key=lambda item: item.entity_id)),
            devices=tuple(devices[device_id] for device_id in sorted(devices)),
        )

    # ---------------------------------------------------------------- forward

    def _snapshot_record(self, record: MigrationRecord) -> None:
        """Fail closed if any candidate changed since it was journaled."""
        for original in record.entries:
            entry = self.hass.config_entries.async_get_entry(original.entry_id)
            if (
                entry is None
                or entry.title != original.title
                or entry.unique_id != original.unique_id
                or entry.version != original.version
                or entry.minor_version != original.minor_version
                or entry.subentries
                or not _data_matches(entry.data, original.data)
                or not _data_matches(entry.options, original.options)
            ):
                raise MigrationError("entry_changed_during_migration")

    async def _async_quiesce(self, entry_ids: Iterable[str]) -> None:
        """Unload every listed runtime (also cancels a fenced setup retry)."""
        for entry_id in entry_ids:
            await self._point(f"unload:{entry_id}")
            entry = self.hass.config_entries.async_get_entry(entry_id)
            if entry is None:
                raise MigrationError("candidate_missing")
            if entry.state is ConfigEntryState.NOT_LOADED:
                continue
            if not entry.state.recoverable or not (
                await self.hass.config_entries.async_unload(entry_id)
            ):
                raise MigrationError("unload_failed")

    async def _async_execute(self) -> dict[str, Any]:
        """Fence, quiesce, re-plan from the journaled snapshots, then migrate."""
        self._fence.activate()
        self._durable = None
        candidate_ids = [entry.entry_id for entry in self.hass.config_entries.async_entries(DOMAIN)]
        try:
            await self._async_quiesce(candidate_ids)
        except Exception as error:
            return await self._async_reload(candidate_ids, error=_error_code(error))

        entries = self.hass.config_entries.async_entries(DOMAIN)
        snapshots = [LegacySnapshot.from_entry(entry) for entry in entries]
        result = plan_migration(snapshots)
        blockers = await self._async_blockers(entries)
        if isinstance(result, MigrationConflictReport) or blockers:
            await self._async_reload(candidate_ids, error=None)
            return _conflict_summary(result, blockers, len(entries), dry_run=False)

        record = self._build_record(result, snapshots)
        try:
            await self._async_forward(record)
            durable = self._durable
            assert durable is not None
            return await self._async_complete_migration(durable)
        except Exception as error:
            return await self._async_handle_failure(record, error)

    async def _async_handle_failure(
        self, record: MigrationRecord, error: Exception
    ) -> dict[str, Any]:
        """Settle a failure from the latest durable phase, never a stale local one."""
        code = _error_code(error)
        durable = self._durable
        if durable is None:
            # The planned record never reached disk, so nothing was mutated.
            _LOGGER.warning("Legacy fleet migration did not start (%s)", code)
            return await self._async_reload(record.candidates, error=code)
        if durable.phase in COMMITTED_PHASES or durable.outcome == OUTCOME_MIGRATED:
            # Past the commit point: never reverse; finish sibling removal.
            _LOGGER.warning("Legacy fleet migration interrupted after commit (%s)", code)
            try:
                return await self._async_complete_migration(durable)
            except Exception as finish_error:
                finish_code = _error_code(finish_error)
                async_report_recovery_failure(self.hass, finish_code)
                return {"status": "incomplete", "dry_run": False, "error": finish_code}
        _LOGGER.warning("Legacy fleet migration failed before commit (%s); reversing", code)
        try:
            await self._async_reverse(durable, code)
        except Exception as reverse_error:
            reverse_code = _error_code(reverse_error)
            async_report_recovery_failure(self.hass, reverse_code)
            return {
                "status": "recovery_failed",
                "dry_run": False,
                "error": code,
                "recovery_error": reverse_code,
            }
        return await self._async_reload(record.candidates, error=code)

    async def _async_reload(self, entry_ids: Iterable[str], *, error: str | None) -> dict[str, Any]:
        """Lift the fence and quietly restart the originals; report what loaded."""
        ids = list(entry_ids)
        self._fence.deactivate()
        not_loaded: list[str] = []
        for entry_id in ids:
            entry = self.hass.config_entries.async_get_entry(entry_id)
            if entry is None:
                not_loaded.append(entry_id)
                continue
            if entry.state is not ConfigEntryState.LOADED:
                try:
                    if entry.state is not ConfigEntryState.NOT_LOADED:
                        await self.hass.config_entries.async_unload(entry_id)
                    self._fence.quiet.add(entry_id)
                    await self.hass.config_entries.async_setup(entry_id)
                except Exception as setup_error:
                    _LOGGER.warning(
                        "Restored legacy entry did not set up (%s)", type(setup_error).__name__
                    )
                finally:
                    self._fence.quiet.discard(entry_id)
            if entry.state is not ConfigEntryState.LOADED:
                not_loaded.append(entry_id)
        if error is None:
            return {"status": "not_started", "dry_run": False}
        if not_loaded:
            ir.async_create_issue(
                self.hass,
                DOMAIN,
                ISSUE_MIGRATION_REVERTED_NOT_LOADED,
                is_fixable=False,
                is_persistent=False,
                severity=ir.IssueSeverity.WARNING,
                translation_key=ISSUE_MIGRATION_REVERTED_NOT_LOADED,
                translation_placeholders={"count": str(len(not_loaded))},
                learn_more_url=_DOCS_URL,
            )
            return {
                "status": "reverted_config_only",
                "dry_run": False,
                "error": error,
                "not_loaded_entry_ids": not_loaded,
            }
        ir.async_delete_issue(self.hass, DOMAIN, ISSUE_MIGRATION_REVERTED_NOT_LOADED)
        return {"status": "reverted", "dry_run": False, "error": error}

    def _migrated_outcome(self, record: MigrationRecord) -> dict[str, Any]:
        return {
            "status": "migrated",
            "dry_run": False,
            "fleet_entry_id": record.anchor_entry_id,
            "plan_digest": record.plan_digest,
            "panels": [
                {
                    "entry_id": plan.entry_id,
                    "subentry_id": plan.subentry_id,
                    "title": plan.title,
                    "panel": plan.data.get("panel"),
                }
                for plan in record.subentries
            ],
            "removed_entry_ids": list(record.siblings),
        }

    async def _async_forward(self, record: MigrationRecord) -> None:
        """Advance planned -> fleet_verified; every step is idempotent."""
        record = await self._save(record)
        await self._async_quiesce(record.candidates)
        record = await self._save(record.advanced(MigrationPhase.QUIESCED))

        await self._point("convert_anchor")
        self._snapshot_record(record)
        self._apply_conversion(record)
        record = await self._save(record.advanced(MigrationPhase.ANCHOR_CONVERTED))

        await self._point("retarget_registries")
        self._retarget_registries(record, strict=True)
        record = await self._save(record.advanced(MigrationPhase.REGISTRIES_RETARGETED))

        await self._point("setup_fleet")
        anchor = self._anchor(record)
        await self._async_quiesce([anchor.entry_id])
        self._fence.allowed.add(anchor.entry_id)
        try:
            set_up = await self.hass.config_entries.async_setup(anchor.entry_id)
        finally:
            self._fence.allowed.discard(anchor.entry_id)
        if not set_up:
            raise MigrationError("fleet_setup_failed")
        await self._point("verify_fleet")
        self._verify_fleet(record)
        await self._async_prove(record)
        await self._save(record.advanced(MigrationPhase.FLEET_VERIFIED))
        # The committed fleet may now reload; legacy siblings stay fenced.
        self._fence.allowed.add(anchor.entry_id)

    async def _async_complete_migration(self, record: MigrationRecord) -> dict[str, Any]:
        """Remove siblings after the commit point, prove it on disk, then finish."""
        self._fence.allowed.add(record.anchor_entry_id)
        if record.phase is MigrationPhase.FLEET_VERIFIED:
            record = await self._save(record.advanced(MigrationPhase.SIBLINGS_REMOVING))
        removing = migration_removing(self.hass)
        for entry_id in record.siblings:
            if self.hass.config_entries.async_get_entry(entry_id) is not None:
                # Idempotent: a registry write that did not reach disk before a
                # crash must never let HA's removal cleanup delete an entity. Past
                # the commit point a vanished entity is no reason to stop.
                self._retarget_registries(record, strict=False)
                await self._point(f"remove:{entry_id}")
                removing.add(entry_id)
                try:
                    await self.hass.config_entries.async_remove(entry_id)
                finally:
                    removing.discard(entry_id)
            if record.phase is MigrationPhase.SIBLINGS_REMOVING and entry_id not in record.removed:
                record = await self._save(
                    record.advanced(
                        MigrationPhase.SIBLINGS_REMOVING,
                        removed=(*record.removed, entry_id),
                    )
                )
        anchor = self._anchor(record)
        if not _is_fleet(anchor) or set(anchor.subentries) != set(
            record.subentry_by_entry.values()
        ):
            raise MigrationError("fleet_runtime_mismatch")
        await self._point("prove_migrated")
        await self._async_prove(record)
        if record.phase is not MigrationPhase.COMPLETE:
            record = await self._save(
                record.advanced(MigrationPhase.COMPLETE, outcome=OUTCOME_MIGRATED)
            )
        await self._async_remove_journal()
        async_update_migration_issue(self.hass)
        _LOGGER.info(
            "Consolidated %d legacy Brilliant entries into one fleet", len(record.candidates)
        )
        return self._migrated_outcome(record)

    async def _async_prove(self, record: MigrationRecord) -> None:
        """Flush config entries and both registries, then prove disk == memory."""
        await self._point("prove")
        disk_entries = await async_persist_config_entries(self.hass)
        disk_entities = await async_persist_entity_registry(self.hass)
        disk_devices = await async_persist_device_registry(self.hass)
        for entry_id in record.candidates:
            entry = self.hass.config_entries.async_get_entry(entry_id)
            stored = disk_entries.get(entry_id)
            if entry is None:
                if stored is not None:
                    raise PersistenceUnprovenError()
            elif stored is None or dict(stored) != dict(
                fleet_manager._canonical_entry_storage(entry)
            ):
                raise PersistenceUnprovenError()
        ent_reg = er.async_get(self.hass)
        for association in record.entities:
            live = ent_reg.async_get(association.entity_id)
            disk = disk_entities.get(association.entity_id)
            if live is None:
                if disk is not None and disk.get("unique_id") == association.unique_id:
                    raise PersistenceUnprovenError()
                continue
            if disk is None or (
                disk.get("unique_id"),
                disk.get("config_entry_id"),
                disk.get("config_subentry_id"),
                disk.get("disabled_by"),
                disk.get("hidden_by"),
            ) != (
                live.unique_id,
                live.config_entry_id,
                live.config_subentry_id,
                _text(live.disabled_by),
                _text(live.hidden_by),
            ):
                raise PersistenceUnprovenError()
        dev_reg = dr.async_get(self.hass)
        for device_association in record.devices:
            device = dev_reg.async_get(device_association.device_id)
            stored_device = disk_devices.get(device_association.device_id)
            if device is None:
                if stored_device is not None:
                    raise PersistenceUnprovenError()
                continue
            raw = stored_device.get("config_entries_subentries") if stored_device else None
            if not isinstance(raw, Mapping) or {
                str(owner): set(subentries) for owner, subentries in raw.items()
            } != {
                owner: set(subentries)
                for owner, subentries in device.config_entries_subentries.items()
            }:
                raise PersistenceUnprovenError()

    def _anchor(self, record: MigrationRecord) -> ConfigEntry[Any]:
        anchor = self.hass.config_entries.async_get_entry(record.anchor_entry_id)
        if anchor is None:
            raise MigrationError("anchor_missing")
        return anchor

    def _apply_conversion(self, record: MigrationRecord) -> None:
        """Make the anchor the journaled fleet entry and add every planned subentry."""
        anchor = self._anchor(record)
        if not _is_fleet(anchor):
            if anchor.subentries:
                raise MigrationError("anchor_has_subentries")
            self.hass.config_entries.async_update_entry(
                anchor,
                data=dict(record.fleet_data),
                options=dict(record.fleet_options),
                title=record.fleet_title,
                unique_id=record.fleet_unique_id,
                version=CONFIG_ENTRY_VERSION,
            )
        elif anchor.unique_id != record.fleet_unique_id:
            raise MigrationError("anchor_drift")
        for plan in record.subentries:
            existing = anchor.subentries.get(plan.subentry_id)
            if existing is None:
                self.hass.config_entries.async_add_subentry(
                    anchor,
                    ConfigSubentry(
                        data=MappingProxyType(dict(plan.data)),
                        subentry_id=plan.subentry_id,
                        subentry_type=SUBENTRY_TYPE_PANEL,
                        title=plan.title,
                        unique_id=plan.unique_id,
                    ),
                )
            elif existing.unique_id != plan.unique_id or dict(existing.data) != dict(plan.data):
                raise MigrationError("subentry_drift")

    def _retarget_registries(self, record: MigrationRecord, *, strict: bool) -> None:
        """Point integration entities/devices at the anchor + planned subentry."""
        anchor_id = record.anchor_entry_id
        planned = record.subentry_by_entry
        candidates = set(record.candidates)
        ent_reg = er.async_get(self.hass)
        for association in record.entities:
            current = ent_reg.async_get(association.entity_id)
            if current is None or current.unique_id != association.unique_id:
                if strict:
                    raise MigrationError("registry_entity_missing")
                continue
            target = planned[association.config_entry_id]
            if current.config_entry_id != anchor_id or current.config_subentry_id != target:
                ent_reg.async_update_entity(
                    association.entity_id,
                    config_entry_id=anchor_id,
                    config_subentry_id=target,
                )

        dev_reg = dr.async_get(self.hass)
        for device_association in record.devices:
            device_id = device_association.device_id
            owners = [owner for owner in device_association.associations if owner in candidates]
            device = dev_reg.async_get(device_id)
            if device is None:
                if strict:
                    raise MigrationError("registry_device_missing")
                continue
            for owner in owners:
                target = planned[owner]
                current_targets = device.config_entries_subentries.get(anchor_id, set())
                if target not in current_targets:
                    updated = dev_reg.async_update_device(
                        device_id,
                        add_config_entry_id=anchor_id,
                        add_config_subentry_id=target,
                    )
                    if updated is None:
                        raise MigrationError("registry_device_missing")
                    device = updated
            for owner in owners:
                if owner == anchor_id:
                    if None in device.config_entries_subentries.get(anchor_id, set()):
                        updated = dev_reg.async_update_device(
                            device_id,
                            remove_config_entry_id=anchor_id,
                            remove_config_subentry_id=None,
                        )
                        if updated is None:
                            raise MigrationError("registry_device_missing")
                        device = updated
                elif owner in device.config_entries:
                    updated = dev_reg.async_update_device(
                        device_id,
                        remove_config_entry_id=owner,
                    )
                    if updated is None:
                        raise MigrationError("registry_device_missing")
                    device = updated

    def _verify_fleet(self, record: MigrationRecord) -> None:
        """Prove the exact runtime and every registry target before committing."""
        anchor = self._anchor(record)
        if anchor.state is not ConfigEntryState.LOADED:
            raise MigrationError("fleet_setup_failed")
        try:
            FleetConfig.from_entry(anchor)
        except EntryDataError:
            raise MigrationError("fleet_runtime_mismatch") from None
        runtime = getattr(anchor, "runtime_data", None)
        if not isinstance(runtime, fleet_manager.FleetManager):
            raise MigrationError("fleet_runtime_mismatch")
        expected = {plan.subentry_id: plan for plan in record.subentries}
        if set(runtime.panels) != set(expected) or set(anchor.subentries) != set(expected):
            raise MigrationError("fleet_runtime_mismatch")
        if runtime.runtime_owned_panel_ids != frozenset(expected):
            # FleetManager inserts every manager before starting it and tolerates
            # individual start failures; only a started runtime owns its panel.
            raise MigrationError("fleet_runtime_mismatch")
        for subentry_id, manager in runtime.panels.items():
            plan = expected[subentry_id]
            if (
                manager.management_id != plan.entry_id
                or manager.panel != plan.data.get("panel")
                or manager.store.subentry_id != subentry_id
            ):
                raise MigrationError("fleet_runtime_mismatch")

        planned = record.subentry_by_entry
        candidates = set(record.candidates)
        ent_reg = er.async_get(self.hass)
        for association in record.entities:
            current = ent_reg.async_get(association.entity_id)
            if (
                current is None
                or current.unique_id != association.unique_id
                or current.platform != association.platform
                or current.config_entry_id != record.anchor_entry_id
                or current.config_subentry_id != planned[association.config_entry_id]
            ):
                raise MigrationError("registry_mismatch")
        dev_reg = dr.async_get(self.hass)
        for device_association in record.devices:
            device = dev_reg.async_get(device_association.device_id)
            if device is None:
                raise MigrationError("registry_mismatch")
            expected_targets = {
                planned[owner] for owner in device_association.associations if owner in candidates
            }
            links = device.config_entries_subentries
            if not expected_targets <= links.get(record.anchor_entry_id, set()):
                raise MigrationError("registry_mismatch")
            if None in links.get(record.anchor_entry_id, set()) or any(
                owner in links for owner in candidates - {record.anchor_entry_id}
            ):
                raise MigrationError("registry_mismatch")
            for owner, subentries in device_association.associations.items():
                if owner not in candidates and set(links.get(owner, set())) != set(subentries):
                    raise MigrationError("registry_mismatch")

    # ---------------------------------------------------------------- reverse

    async def _async_reverse(self, record: MigrationRecord, error: str) -> None:
        """Restore the exact original entries and registry associations, proven on disk."""
        if record.phase is not MigrationPhase.COMPLETE:
            if record.phase is not MigrationPhase.REVERSING:
                record = record.advanced(MigrationPhase.REVERSING, last_error=error)
            try:
                record = await self._save(record)
            except MigrationJournalError:
                # The durable journal is still in a reversible phase, so recovery
                # would reverse too; restoring memory now is always safe.
                _LOGGER.warning("Migration reversal could not be journaled; restoring anyway")
        await self._async_quiesce(record.candidates)
        await self._point("reverse_registries")
        self._restore_registries(record)
        await self._point("reverse_anchor")
        anchor = self._anchor(record)
        for plan in record.subentries:
            if plan.subentry_id in anchor.subentries:
                self.hass.config_entries.async_remove_subentry(anchor, plan.subentry_id)
        original = record.entry(record.anchor_entry_id)
        self.hass.config_entries.async_update_entry(
            anchor,
            data=copy.deepcopy(dict(original.data)),
            options=copy.deepcopy(dict(original.options)),
            title=original.title,
            unique_id=original.unique_id,
            version=original.version,
            minor_version=original.minor_version,
        )
        self._verify_reversed(record)
        await self._point("prove_reverted")
        await self._async_prove(record)
        if record.phase is not MigrationPhase.COMPLETE:
            await self._save(record.advanced(MigrationPhase.COMPLETE, outcome=OUTCOME_REVERTED))
        # Each restored entry's next setup is quiet: it must not contact a panel.
        self._fence.quiet.update(record.candidates)
        await self._async_remove_journal()

    def _restore_registries(self, record: MigrationRecord) -> None:
        anchor_id = record.anchor_entry_id
        planned_ids = {plan.subentry_id for plan in record.subentries}
        candidates = set(record.candidates)
        ent_reg = er.async_get(self.hass)
        for association in record.entities:
            current = ent_reg.async_get(association.entity_id)
            if current is None:
                continue
            if (
                current.config_entry_id != association.config_entry_id
                or current.config_subentry_id != association.config_subentry_id
            ):
                ent_reg.async_update_entity(
                    association.entity_id,
                    config_entry_id=association.config_entry_id,
                    config_subentry_id=association.config_subentry_id,
                )
        dev_reg = dr.async_get(self.hass)
        for device_association in record.devices:
            device = dev_reg.async_get(device_association.device_id)
            if device is None:
                continue
            for owner, subentries in device_association.associations.items():
                if owner not in candidates:
                    continue
                for subentry_id in subentries:
                    if subentry_id not in device.config_entries_subentries.get(owner, set()):
                        updated = dev_reg.async_update_device(
                            device.id,
                            add_config_entry_id=owner,
                            add_config_subentry_id=subentry_id,
                        )
                        if updated is None:
                            raise MigrationError("registry_device_missing")
                        device = updated
            for subentry_id in sorted(
                planned_ids & set(device.config_entries_subentries.get(anchor_id, set())),
                key=lambda item: item or "",
            ):
                updated = dev_reg.async_update_device(
                    device.id,
                    remove_config_entry_id=anchor_id,
                    remove_config_subentry_id=subentry_id,
                )
                if updated is None:
                    raise MigrationError("registry_device_missing")
                device = updated

    def _verify_reversed(self, record: MigrationRecord) -> None:
        for original in record.entries:
            entry = self.hass.config_entries.async_get_entry(original.entry_id)
            if (
                entry is None
                or entry.title != original.title
                or entry.unique_id != original.unique_id
                or entry.version != original.version
                or dict(entry.data) != dict(original.data)
                or dict(entry.options) != dict(original.options)
                or entry.subentries
            ):
                raise MigrationError("reverse_incomplete")
        ent_reg = er.async_get(self.hass)
        for association in record.entities:
            current = ent_reg.async_get(association.entity_id)
            if (
                current is None
                or current.unique_id != association.unique_id
                or current.config_entry_id != association.config_entry_id
                or current.config_subentry_id != association.config_subentry_id
            ):
                raise MigrationError("reverse_incomplete")
        dev_reg = dr.async_get(self.hass)
        candidates = set(record.candidates)
        for device_association in record.devices:
            device = dev_reg.async_get(device_association.device_id)
            if device is None:
                raise MigrationError("reverse_incomplete")
            restored = {
                owner: set(subentries)
                for owner, subentries in device.config_entries_subentries.items()
                if owner in candidates
            }
            expected = {
                owner: set(subentries)
                for owner, subentries in device_association.associations.items()
                if owner in candidates
            }
            if restored != expected:
                raise MigrationError("reverse_incomplete")

    # --------------------------------------------------------------- recovery

    async def _async_recover_locked(self, record: MigrationRecord, *, live: bool) -> dict[str, Any]:
        self._fence.activate()
        self._durable = record
        try:
            if record.phase in COMMITTED_PHASES or record.outcome == OUTCOME_MIGRATED:
                self._apply_conversion(record)
                self._retarget_registries(record, strict=False)
                outcome = await self._async_complete_migration(record)
                anchor = self._anchor(record)
                if live and anchor.state is not ConfigEntryState.LOADED:
                    await self._async_quiesce([anchor.entry_id])
                    await self.hass.config_entries.async_setup(anchor.entry_id)
                return {**outcome, "recovered": True}
            await self._async_reverse(record, record.last_error or "interrupted")
            if live:
                reloaded = await self._async_reload(
                    record.candidates, error=record.last_error or "interrupted"
                )
                return {**reloaded, "recovered": True}
            return {"status": "reverted", "recovered": True, "error": record.last_error}
        except Exception as error:
            code = _error_code(error)
            _LOGGER.error("Legacy fleet migration recovery failed (%s)", code)
            async_report_recovery_failure(self.hass, code)
            if record.phase in COMMITTED_PHASES or record.outcome == OUTCOME_MIGRATED:
                self._fence.allowed.add(record.anchor_entry_id)
            return {"status": "recovery_failed", "error": code}
