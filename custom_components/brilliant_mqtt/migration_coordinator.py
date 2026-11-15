"""Operator-triggered, recoverable legacy-to-fleet consolidation.

The coordinator is the only cross-entry writer. Under one domain migration lock
it plans with the pure :mod:`.migration` planner, journals the complete
pre-change snapshot, quiesces every legacy runtime, converts the deterministic
anchor (smallest entry ID) into the singleton fleet entry, adds one journaled
panel subentry per legacy entry, retargets the integration-owned entity/device
registry associations, sets up and verifies the fleet runtime, persists the
``fleet_verified`` commit point, and only then removes the sibling entries.

Before the commit point any failure reverses to the exact original entries and
registry associations. At/after it, recovery only finishes sibling removal.
Migration never contacts, reinstalls, restarts, or rewrites a panel.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
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
)
from .migration_journal import (
    COMMITTED_PHASES,
    DeviceAssociation,
    EntityAssociation,
    EntrySnapshot,
    MigrationJournal,
    MigrationJournalError,
    MigrationPhase,
    MigrationRecord,
    SubentryPlan,
)
from .provisioning_journal import ProvisioningJournal

_LOGGER = logging.getLogger(__name__)

DATA_MIGRATION_LOCK = "migration_lock"
DATA_MIGRATION_REMOVING = "migration_removing"
ISSUE_MIGRATION_AVAILABLE = "legacy_migration_available"
ISSUE_MIGRATION_RECOVERY_FAILED = "migration_recovery_failed"
_DOCS_URL = "https://github.com/joyfulhouse/brilliant-mqtt/blob/main/docs/ha-integration.md"
_PANEL_OPERATION_WAIT_SECONDS = 120.0

type Checkpoint = Callable[[str], Awaitable[None]]
type PersistenceWaiter = Callable[[HomeAssistant, ConfigEntry[Any]], Awaitable[None]]


class MigrationError(Exception):
    """One stable, value-free migration failure code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


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
def _async_report_recovery_failure(hass: HomeAssistant, code: str) -> None:
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
    if isinstance(error, (MigrationError, MigrationJournalError)):
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


class MigrationCoordinator:
    """Run or recover one legacy-to-fleet migration."""

    def __init__(
        self,
        hass: HomeAssistant,
        *,
        checkpoint: Checkpoint | None = None,
        persistence_waiter: PersistenceWaiter | None = None,
    ) -> None:
        self.hass = hass
        self._journal = MigrationJournal(hass)
        self._checkpoint = checkpoint
        self._persistence_waiter = persistence_waiter

    async def _point(self, name: str) -> None:
        """Failure-injection seam between every journal save and mutation."""
        if self._checkpoint is not None:
            await self._checkpoint(name)

    async def _save(self, record: MigrationRecord) -> MigrationRecord:
        await self._point(f"before_save:{record.phase.value}")
        await self._journal.async_save(record)
        await self._point(f"after_save:{record.phase.value}")
        return record

    async def _async_wait_persisted(self, entry: ConfigEntry[Any]) -> None:
        waiter = self._persistence_waiter or fleet_manager.async_wait_config_entry_persisted
        await waiter(self.hass, entry)

    # ------------------------------------------------------------------ public

    async def async_run(self, *, dry_run: bool) -> dict[str, Any]:
        """Plan (dry run) or execute the migration and return a secret-free outcome."""
        async with migration_lock(self.hass):
            try:
                pending = await self._journal.async_load()
            except MigrationJournalError as error:
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

            result = plan_migration([LegacySnapshot.from_entry(entry) for entry in entries])
            blockers = await self._async_blockers(entries)
            if isinstance(result, MigrationConflictReport) or blockers:
                summary: dict[str, Any] = (
                    result.summary()
                    if isinstance(result, MigrationConflictReport)
                    else {"status": "conflicts", "candidate_count": len(entries), "conflicts": []}
                )
                summary["status"] = "conflicts"
                summary["conflicts"] = [*summary["conflicts"], *blockers]
                summary["dry_run"] = dry_run
                return summary
            if dry_run:
                return {**result.summary(), "dry_run": True}
            if not await mqtt.async_wait_for_mqtt_client(self.hass):
                return {
                    "status": "conflicts",
                    "dry_run": False,
                    "candidate_count": len(entries),
                    "conflicts": [_blocker("mqtt_unavailable", "mqtt")],
                }
            return await self._async_execute(result)

    async def async_recover(self) -> dict[str, Any] | None:
        """Startup recovery: settle a journaled migration before entries load."""
        async with migration_lock(self.hass):
            try:
                record = await self._journal.async_load()
            except MigrationJournalError as error:
                _async_report_recovery_failure(self.hass, error.code)
                return {"status": "recovery_failed", "error": error.code}
            if record is None:
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

    def _build_record(self, plan: MigrationPlan) -> MigrationRecord:
        entries: list[EntrySnapshot] = []
        for entry_id in plan.candidate_ids:
            entry = self.hass.config_entries.async_get_known_entry(entry_id)
            entries.append(
                EntrySnapshot(
                    entry_id=entry.entry_id,
                    title=entry.title,
                    unique_id=entry.unique_id,
                    version=entry.version,
                    minor_version=entry.minor_version,
                    data=dict(entry.data),
                    options=dict(entry.options),
                )
            )
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

    async def _async_execute(self, plan: MigrationPlan) -> dict[str, Any]:
        # Wait for any in-flight panel SSH operation so no legacy runtime can write
        # its entry after the snapshot. Fleet setup never takes this lock.
        ssh_lock = fleet_manager._shared_ssh_lock(self.hass)
        try:
            async with asyncio.timeout(_PANEL_OPERATION_WAIT_SECONDS):
                await ssh_lock.acquire()
        except TimeoutError:
            return {
                "status": "conflicts",
                "dry_run": False,
                "candidate_count": len(plan.candidate_ids),
                "conflicts": [_blocker("panel_operation_in_progress", "ssh")],
            }
        failure: Exception | None = None
        try:
            record = self._build_record(plan)
            try:
                record = await self._async_forward(await self._save(record))
            except Exception as error:
                failure = error
                _LOGGER.warning(
                    "Legacy fleet migration failed before commit (%s); reversing",
                    _error_code(error),
                )
                try:
                    await self._async_reverse(record, _error_code(error))
                except Exception as reverse_error:
                    code = _error_code(reverse_error)
                    _async_report_recovery_failure(self.hass, code)
                    return {
                        "status": "recovery_failed",
                        "dry_run": False,
                        "error": _error_code(error),
                        "recovery_error": code,
                    }
        finally:
            ssh_lock.release()

        if failure is not None:
            # Legacy setup may itself SSH (HA-mirror retirement audit), so the
            # originals reload only after the panel-operation lock is released.
            await self._async_reload_originals(record)
            return {"status": "reverted", "dry_run": False, "error": _error_code(failure)}
        try:
            await self._async_remove_siblings(record)
        except Exception as error:
            code = _error_code(error)
            _LOGGER.warning("Legacy sibling removal did not finish (%s)", code)
            _async_report_recovery_failure(self.hass, code)
            return {"status": "incomplete", "dry_run": False, "error": code}
        return self._migrated_outcome(record)

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

    async def _async_forward(self, record: MigrationRecord) -> MigrationRecord:
        """Advance planned → fleet_verified; every step is idempotent."""
        for entry_id in record.candidates:
            await self._point(f"unload:{entry_id}")
            entry = self.hass.config_entries.async_get_entry(entry_id)
            if entry is None:
                raise MigrationError("candidate_missing")
            if entry.state is not ConfigEntryState.NOT_LOADED and not (
                await self.hass.config_entries.async_unload(entry_id)
            ):
                raise MigrationError("unload_failed")
        record = await self._save(record.advanced(MigrationPhase.QUIESCED))

        await self._point("convert_anchor")
        self._apply_conversion(record)
        record = await self._save(record.advanced(MigrationPhase.ANCHOR_CONVERTED))

        await self._point("retarget_registries")
        self._retarget_registries(record, strict=True)
        record = await self._save(record.advanced(MigrationPhase.REGISTRIES_RETARGETED))

        await self._point("setup_fleet")
        anchor = self._anchor(record)
        if not await self.hass.config_entries.async_setup(anchor.entry_id):
            raise MigrationError("fleet_setup_failed")
        await self._point("verify_fleet")
        self._verify_fleet(record)
        try:
            await self._async_wait_persisted(anchor)
        except Exception:
            raise MigrationError("config_entry_storage_unavailable") from None
        return await self._save(record.advanced(MigrationPhase.FLEET_VERIFIED))

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

    async def _async_remove_siblings(self, record: MigrationRecord) -> None:
        """Remove each sibling only after the commit point, journaling progress."""
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
            if entry_id not in record.removed:
                record = await self._save(
                    record.advanced(
                        MigrationPhase.SIBLINGS_REMOVING,
                        removed=(*record.removed, entry_id),
                    )
                )
        record = await self._save(record.advanced(MigrationPhase.COMPLETE))
        await self._journal.async_remove()
        ir.async_delete_issue(self.hass, DOMAIN, ISSUE_MIGRATION_RECOVERY_FAILED)
        async_update_migration_issue(self.hass)
        _LOGGER.info(
            "Consolidated %d legacy Brilliant entries into one fleet", len(record.candidates)
        )

    # ---------------------------------------------------------------- reverse

    async def _async_reverse(self, record: MigrationRecord, error: str) -> None:
        """Restore the exact original entries and registry associations."""
        if record.phase is not MigrationPhase.REVERSING:
            record = await self._save(record.advanced(MigrationPhase.REVERSING, last_error=error))
        for entry_id in record.candidates:
            entry = self.hass.config_entries.async_get_entry(entry_id)
            if entry is None:
                raise MigrationError("candidate_missing")
            if entry.state is not ConfigEntryState.NOT_LOADED:
                if not entry.state.recoverable or not (
                    await self.hass.config_entries.async_unload(entry_id)
                ):
                    raise MigrationError("unload_failed")
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
            data=dict(original.data),
            options=dict(original.options),
            title=original.title,
            unique_id=original.unique_id,
            version=original.version,
            minor_version=original.minor_version,
        )
        self._verify_reversed(record)
        await self._save(record.advanced(MigrationPhase.COMPLETE))
        await self._journal.async_remove()
        ir.async_delete_issue(self.hass, DOMAIN, ISSUE_MIGRATION_RECOVERY_FAILED)

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

    async def _async_reload_originals(self, record: MigrationRecord) -> None:
        for entry_id in record.candidates:
            entry = self.hass.config_entries.async_get_entry(entry_id)
            if entry is None or entry.state is not ConfigEntryState.NOT_LOADED:
                continue
            try:
                await self.hass.config_entries.async_setup(entry_id)
            except Exception as error:
                _LOGGER.warning("Restored legacy entry did not set up (%s)", type(error).__name__)

    # --------------------------------------------------------------- recovery

    async def _async_recover_locked(self, record: MigrationRecord, *, live: bool) -> dict[str, Any]:
        try:
            if record.phase is MigrationPhase.COMPLETE:
                await self._journal.async_remove()
                return {"status": "recovered", "phase": record.phase.value}
            if record.phase in COMMITTED_PHASES:
                self._finish_conversion(record)
                await self._async_remove_siblings(record)
                if live:
                    anchor = self._anchor(record)
                    if anchor.state is ConfigEntryState.NOT_LOADED:
                        await self.hass.config_entries.async_setup(anchor.entry_id)
                return {**self._migrated_outcome(record), "recovered": True}
            await self._async_reverse(record, record.last_error or "interrupted")
            if live:
                await self._async_reload_originals(record)
            return {"status": "reverted", "recovered": True, "error": record.last_error}
        except Exception as error:
            code = _error_code(error)
            _LOGGER.error("Legacy fleet migration recovery failed (%s)", code)
            _async_report_recovery_failure(self.hass, code)
            return {"status": "recovery_failed", "error": code}

    def _finish_conversion(self, record: MigrationRecord) -> None:
        """Past the commit point, converge config + registries on the journaled fleet."""
        self._apply_conversion(record)
        self._retarget_registries(record, strict=False)
