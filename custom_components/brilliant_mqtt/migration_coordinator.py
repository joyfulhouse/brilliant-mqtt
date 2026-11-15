"""Operator-triggered, recoverable legacy-to-fleet consolidation.

The coordinator is the only cross-entry writer. Under the domain migration lock
and the shared panel-operation (SSH) lock it fences every Brilliant entry's
setup, quiesces the legacy runtimes, plans with the pure :mod:`.migration`
planner from the exact snapshots it journals, converts the deterministic anchor
(smallest entry ID) into the singleton fleet entry, adds one journaled panel
subentry per legacy entry, retargets the integration-owned entity/device
registry associations, sets up and verifies the fleet runtime, persists the
``fleet_verified`` commit point, and only then removes the sibling entries.

The journal (an atomically written file, every write re-read and compared) is
the single source of truth; nothing relies on Home Assistant's lazily written
config-entry or registry stores. Every startup idempotently re-applies the
journaled target to config entries and registries (forward once committed,
otherwise the original associations) and records ``complete``. A ``complete``
journal is kept, with the fence lowered, until a later startup observes the
loaded state already at the target; only then is it deleted. A crash before
Home Assistant's final write is therefore always repaired from the journal.
While a non-complete journal is unresolved, the setup fence keeps mixed
fleet/legacy generations from ever starting. Migration never contacts,
reinstalls, restarts, or rewrites a panel.
"""

from __future__ import annotations

import asyncio
import copy
import inspect
import logging
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from types import MappingProxyType
from typing import Any, cast
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
    QuietRestorations,
    SubentryPlan,
)
from .provisioning_journal import ProvisioningJournal

_LOGGER = logging.getLogger(__name__)

DATA_MIGRATION_LOCK = "migration_lock"
DATA_MIGRATION_REMOVING = "migration_removing"
DATA_MIGRATION_FENCE = "migration_fence"
DATA_QUIET_LOCK = "migration_quiet_lock"
ISSUE_MIGRATION_AVAILABLE = "legacy_migration_available"
ISSUE_MIGRATION_RECOVERY_FAILED = "migration_recovery_failed"
ISSUE_MIGRATION_REVERTED_NOT_LOADED = "migration_reverted_not_loaded"
ISSUE_QUIET_STATE_UNWRITABLE = "migration_quiet_state_unwritable"
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


class MigrationDriftError(MigrationError):
    """A candidate changed after it was journaled and before anything was converted."""

    def __init__(self) -> None:
        super().__init__("entry_changed_during_migration")


def _committed(record: MigrationRecord | None) -> bool:
    return record is not None and (
        record.phase in COMMITTED_PHASES or record.outcome == OUTCOME_MIGRATED
    )


@callback
def async_clear_reverted_issue(hass: HomeAssistant, starting_entry_id: str) -> None:
    """Drop the restored-but-not-running issue once every Brilliant entry runs.

    Called at the end of a successful ``async_setup_entry``, before HA marks that
    entry loaded, so the entry being started counts as running.
    """
    if all(
        entry.entry_id == starting_entry_id
        or entry.state is ConfigEntryState.LOADED
        or entry.disabled_by is not None
        for entry in hass.config_entries.async_entries(DOMAIN)
    ):
        ir.async_delete_issue(hass, DOMAIN, ISSUE_MIGRATION_REVERTED_NOT_LOADED)


def migration_blocks_entry_writes(hass: HomeAssistant) -> bool:
    """Whether config/options flows must refuse to write Brilliant entries now."""
    domain_data = hass.data.get(DOMAIN)
    fence = domain_data.get(DATA_MIGRATION_FENCE) if isinstance(domain_data, dict) else None
    return isinstance(fence, MigrationFence) and fence.active


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

    def is_quiet(self, entry_id: str) -> bool:
        """Peek; the marker is cleared only after a successful setup."""
        return entry_id in self.quiet


def migration_fence(hass: HomeAssistant) -> MigrationFence:
    """Return the domain-wide setup fence."""
    domain_data: dict[str, Any] = hass.data.setdefault(DOMAIN, {})
    fence = domain_data.get(DATA_MIGRATION_FENCE)
    if not isinstance(fence, MigrationFence):
        fence = MigrationFence()
        domain_data[DATA_MIGRATION_FENCE] = fence
    return fence


def quiet_lock(hass: HomeAssistant) -> asyncio.Lock:
    """Serialize read-modify-write of the durable quiet-restoration set."""
    domain_data: dict[str, Any] = hass.data.setdefault(DOMAIN, {})
    lock: asyncio.Lock = domain_data.setdefault(DATA_QUIET_LOCK, asyncio.Lock())
    return lock


@callback
def async_report_quiet_state_unwritable(hass: HomeAssistant) -> None:
    """Make prolonged quiet-restoration (deferred HA-mirror audits) visible."""
    ir.async_create_issue(
        hass,
        DOMAIN,
        ISSUE_QUIET_STATE_UNWRITABLE,
        is_fixable=False,
        is_persistent=False,
        severity=ir.IssueSeverity.WARNING,
        translation_key=ISSUE_QUIET_STATE_UNWRITABLE,
        learn_more_url=_DOCS_URL,
    )


async def async_load_quiet_obligations(hass: HomeAssistant) -> None:
    """Re-establish durable quiet-restoration markers at startup (fail closed)."""
    outstanding = await QuietRestorations(hass).async_load()
    fence = migration_fence(hass)
    if outstanding is None:
        async_report_quiet_state_unwritable(hass)
        fence.quiet.update(entry.entry_id for entry in hass.config_entries.async_entries(DOMAIN))
        return
    fence.quiet.update(outstanding)


async def async_quiet_setup_succeeded(hass: HomeAssistant, entry_id: str) -> None:
    """Retire one entry's quiet obligation after its quiet setup succeeded."""
    fence = migration_fence(hass)
    if entry_id not in fence.quiet:
        return
    async with quiet_lock(hass):
        store = QuietRestorations(hass)
        current = await store.async_load()
        remaining = (
            current
            if current is not None
            else frozenset(entry.entry_id for entry in hass.config_entries.async_entries(DOMAIN))
        )
        try:
            await store.async_store(
                frozenset(
                    item
                    for item in remaining
                    if item != entry_id and hass.config_entries.async_get_entry(item) is not None
                )
            )
        except MigrationJournalError as error:
            # Kept durable: the next setup is quiet again, never a panel contact.
            _LOGGER.warning("Quiet-restoration obligation not retired (%s)", error.code)
            async_report_quiet_state_unwritable(hass)
            return
    ir.async_delete_issue(hass, DOMAIN, ISSUE_QUIET_STATE_UNWRITABLE)
    fence.quiet.discard(entry_id)


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


# Home Assistant 2026.10 changed the device registry: a device now belongs to
# exactly one config entry and subentry (a shared device was split per entry),
# ``config_entries_subentries`` is a deprecated shim, and devices move with
# ``new_config_entry_id``/``new_config_subentry_id``. Releases up to 2026.9
# share one device across entries and link it with add/remove. Support both.
SINGLE_OWNER_DEVICES = (
    "new_config_entry_id" in inspect.signature(dr.DeviceRegistry.async_update_device).parameters
)


def device_links(device: dr.DeviceEntry) -> dict[str, set[str | None]]:
    """Return a device's config entry -> subentries ownership without deprecated reads."""
    if SINGLE_OWNER_DEVICES and not getattr(device, "is_composite_device", False):
        owner = getattr(device, "config_entry_id", None)
        if not isinstance(owner, str):
            return {}
        subentry = getattr(device, "config_subentry_id", None)
        return {owner: {subentry if isinstance(subentry, str) else None}}
    links = device.config_entries_subentries
    return {owner: set(subentries) for owner, subentries in links.items()}


def _move_device(
    dev_reg: dr.DeviceRegistry, device_id: str, entry_id: str, subentry_id: str | None
) -> None:
    """Move a single-owner device to one config entry and subentry."""
    update: Callable[..., dr.DeviceEntry | None] = cast(Any, dev_reg).async_update_device
    if update(device_id, new_config_entry_id=entry_id, new_config_subentry_id=subentry_id) is None:
        raise MigrationError("registry_device_missing")


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
        # Set when a write's outcome could not be established on either side of
        # the commit point: nothing may reverse or finish until it is re-read.
        self._ambiguous = False

    async def _point(self, name: str) -> None:
        """Failure-injection seam between every journal save and mutation."""
        if self._checkpoint is not None:
            await self._checkpoint(name)

    async def _save(self, record: MigrationRecord) -> MigrationRecord:
        """Persist and prove one record; settle what is on disk if that fails.

        Any failure after an attempted write enters persisted-phase settlement:
        a re-read that finds the attempted record makes the write a success.
        """
        await self._point(f"before_save:{record.phase.value}")
        try:
            await self._journal.async_save(record)
        except Exception as error:
            if not await self._async_settle_unproven(record):
                if isinstance(error, MigrationJournalError):
                    raise
                raise MigrationJournalError("journal_unproven") from None
        self._durable = record
        await self._point(f"after_save:{record.phase.value}")
        return record

    async def _async_settle_unproven(self, attempted: MigrationRecord) -> bool:
        """Establish the persisted phase after a write whose outcome is unknown.

        Returns True only when the attempted record is positively on disk.
        """
        previous = self._durable
        try:
            on_disk = await self._journal.async_load()
        except Exception:
            on_disk = None
            readable = False
        else:
            readable = True
        if readable and on_disk == attempted:
            return True
        if readable and on_disk == previous:
            return False
        # Unknown: either the previous or the attempted record may be on disk.
        # Only a write that straddles the commit point is truly ambiguous.
        if _committed(previous) != _committed(attempted):
            self._ambiguous = True
        elif _committed(attempted):
            self._durable = attempted
        return False

    async def _try_save(self, record: MigrationRecord) -> MigrationRecord:
        """Best-effort progress write; recovery re-applies idempotently anyway."""
        try:
            return await self._save(record)
        except MigrationJournalError as error:
            _LOGGER.warning("Migration progress could not be journaled (%s)", error.code)
            return record

    async def _async_try_remove_journal(self) -> None:
        await self._point("remove_journal")
        try:
            await self._journal.async_remove()
        except MigrationJournalError as error:
            _LOGGER.warning("Migration journal could not be removed (%s)", error.code)
            return
        self._durable = None

    # ------------------------------------------------------------------ public

    async def async_run(self, *, dry_run: bool) -> dict[str, Any]:
        """Plan (dry run) or execute the migration and return a secret-free outcome."""
        async with migration_lock(self.hass):
            try:
                pending = await self._journal.async_load()
            except MigrationJournalError as error:
                if not dry_run:
                    self._fence.activate()
                    async_report_recovery_failure(self.hass, error.code)
                return {"status": "recovery_failed", "dry_run": dry_run, "error": error.code}
            if pending is not None and pending.phase is not MigrationPhase.COMPLETE:
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
        """Startup recovery: settle or confirm a journaled migration before entries load.

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

        # HA 2026.10+ refuses to move a device that has child devices (they would
        # need a cascading move), which a migration must not discover mid-way.
        children_of = getattr(dr, "async_entries_for_parent_device", None)
        if callable(children_of):
            dev_reg = dr.async_get(self.hass)
            parents = [
                entry
                for entry in entries
                if any(
                    children_of(dev_reg, device.id)
                    for device in dr.async_entries_for_config_entry(dev_reg, entry.entry_id)
                )
            ]
            if parents:
                blockers.append(_blocker("device_has_child_devices", "device_registry", parents))

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
                        for owner, subentries in sorted(device_links(device).items())
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
                raise MigrationDriftError

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
        self._ambiguous = False
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
        if self._ambiguous:
            # The commit write may or may not be on disk: keep the journal and the
            # fleet, keep the siblings fenced, and let a re-read decide.
            self._fence.allowed.add(record.anchor_entry_id)
            async_report_recovery_failure(self.hass, "commit_unproven")
            return {"status": "ambiguous", "dry_run": False, "error": code}
        if _committed(durable):
            assert durable is not None
            _LOGGER.warning("Legacy fleet migration interrupted after commit (%s)", code)
            try:
                return await self._async_complete_migration(durable)
            except Exception as finish_error:
                finish_code = _error_code(finish_error)
                self._fence.allowed.add(durable.anchor_entry_id)
                async_report_recovery_failure(self.hass, finish_code)
                return {"status": "incomplete", "dry_run": False, "error": finish_code}
        if durable is None or isinstance(error, MigrationDriftError):
            # Nothing was converted: drop any journal and restart the current,
            # intact legacy set as it is now (an edit made meanwhile is kept).
            _LOGGER.warning("Legacy fleet migration aborted before conversion (%s)", code)
            await self._async_try_remove_journal()
            outcome = await self._async_reload(record.candidates, error=code)
            if isinstance(error, MigrationDriftError) and outcome["status"] == "reverted":
                outcome["status"] = "aborted"
            return outcome
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
        """Lift the fence and quietly restart the originals; report what loaded.

        Each restarted entry keeps its quiet marker until a setup succeeds, so a
        failed attempt's automatic retry is quiet too.
        """
        ids = list(entry_ids)
        self._fence.deactivate()
        not_loaded: list[str] = []
        for entry_id in ids:
            entry = self.hass.config_entries.async_get_entry(entry_id)
            if entry is None:
                continue
            if entry.state is not ConfigEntryState.LOADED:
                self._fence.quiet.add(entry_id)
                try:
                    if entry.state is not ConfigEntryState.NOT_LOADED:
                        await self.hass.config_entries.async_unload(entry_id)
                    await self.hass.config_entries.async_setup(entry_id)
                except Exception as setup_error:
                    _LOGGER.warning(
                        "Restored legacy entry did not set up (%s)", type(setup_error).__name__
                    )
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
        await self._save(record.advanced(MigrationPhase.FLEET_VERIFIED))
        # The committed fleet may now reload; legacy siblings stay fenced.
        self._fence.allowed.add(anchor.entry_id)

    async def _async_remove_remaining_siblings(self, record: MigrationRecord) -> MigrationRecord:
        removing = migration_removing(self.hass)
        for entry_id in record.siblings:
            if self.hass.config_entries.async_get_entry(entry_id) is not None:
                # Idempotent: a registry write that did not reach disk before a
                # crash must never let HA's removal cleanup delete an entity.
                self._retarget_registries(record, strict=False)
                await self._point(f"remove:{entry_id}")
                removing.add(entry_id)
                try:
                    await self.hass.config_entries.async_remove(entry_id)
                finally:
                    removing.discard(entry_id)
            if record.phase is MigrationPhase.SIBLINGS_REMOVING and entry_id not in record.removed:
                record = await self._try_save(
                    record.advanced(
                        MigrationPhase.SIBLINGS_REMOVING,
                        removed=(*record.removed, entry_id),
                    )
                )
        return record

    async def _async_complete_migration(self, record: MigrationRecord) -> dict[str, Any]:
        """Remove siblings after the commit point, then record ``complete``.

        The ``complete`` journal is kept (fence lowered) until a later startup
        confirms the loaded state, because the removals may not be on disk yet.
        """
        self._fence.allowed.add(record.anchor_entry_id)
        if record.phase is MigrationPhase.FLEET_VERIFIED:
            record = await self._try_save(record.advanced(MigrationPhase.SIBLINGS_REMOVING))
        record = await self._async_remove_remaining_siblings(record)
        if not self._migrated_target_reached(record):
            raise MigrationError("fleet_runtime_mismatch")
        await self._point("complete")
        if record.phase is not MigrationPhase.COMPLETE:
            target = (
                record
                if record.phase is MigrationPhase.SIBLINGS_REMOVING
                else record.advanced(MigrationPhase.SIBLINGS_REMOVING)
            )
            await self._try_save(target.advanced(MigrationPhase.COMPLETE, outcome=OUTCOME_MIGRATED))
        self._fence.deactivate()
        ir.async_delete_issue(self.hass, DOMAIN, ISSUE_MIGRATION_RECOVERY_FAILED)
        ir.async_delete_issue(self.hass, DOMAIN, ISSUE_MIGRATION_REVERTED_NOT_LOADED)
        async_update_migration_issue(self.hass)
        _LOGGER.info(
            "Consolidated %d legacy Brilliant entries into one fleet", len(record.candidates)
        )
        return self._migrated_outcome(record)

    def _migrated_target_reached(self, record: MigrationRecord) -> bool:
        """Whether no migration-loss artifact remains for a committed migration."""
        anchor = self.hass.config_entries.async_get_entry(record.anchor_entry_id)
        if anchor is None:
            return True  # The fleet was deliberately removed after completion.
        if not _is_fleet(anchor):
            return False
        if any(self.hass.config_entries.async_get_entry(item) for item in record.siblings):
            return False
        planned = record.subentry_by_entry
        present = set(anchor.subentries)
        ent_reg = er.async_get(self.hass)
        for association in record.entities:
            current = ent_reg.async_get(association.entity_id)
            target = planned[association.config_entry_id]
            if (
                current is not None
                and current.unique_id == association.unique_id
                and target in present
                and (current.config_entry_id, current.config_subentry_id)
                != (record.anchor_entry_id, target)
            ):
                return False
        dev_reg = dr.async_get(self.hass)
        for device_association in record.devices:
            device = dev_reg.async_get(device_association.device_id)
            if device is None:
                continue
            links = device_links(device)
            if None in links.get(record.anchor_entry_id, set()) or any(
                owner in links for owner in record.siblings
            ):
                return False
        return True

    def _anchor(self, record: MigrationRecord) -> ConfigEntry[Any]:
        anchor = self.hass.config_entries.async_get_entry(record.anchor_entry_id)
        if anchor is None:
            raise MigrationError("anchor_missing")
        return anchor

    def _apply_conversion(self, record: MigrationRecord) -> None:
        """Make the anchor the journaled fleet entry with every planned subentry.

        Idempotent: a fleet anchor is left alone. Its subentries were written in the
        same step as the conversion, so a missing one was removed deliberately.
        """
        anchor = self._anchor(record)
        if _is_fleet(anchor):
            if anchor.unique_id != record.fleet_unique_id:
                raise MigrationError("anchor_drift")
            return
        if anchor.subentries:
            raise MigrationError("anchor_has_subentries")
        self.hass.config_entries.async_update_entry(
            anchor,
            data=copy.deepcopy(dict(record.fleet_data)),
            options=copy.deepcopy(dict(record.fleet_options)),
            title=record.fleet_title,
            unique_id=record.fleet_unique_id,
            version=CONFIG_ENTRY_VERSION,
        )
        for plan in record.subentries:
            self.hass.config_entries.async_add_subentry(
                anchor,
                ConfigSubentry(
                    data=MappingProxyType(copy.deepcopy(dict(plan.data))),
                    subentry_id=plan.subentry_id,
                    subentry_type=SUBENTRY_TYPE_PANEL,
                    title=plan.title,
                    unique_id=plan.unique_id,
                ),
            )

    def _retarget_registries(self, record: MigrationRecord, *, strict: bool) -> None:
        """Point integration entities/devices at the anchor + planned subentry."""
        anchor_id = record.anchor_entry_id
        planned = record.subentry_by_entry
        candidates = set(record.candidates)
        present = set(self._anchor(record).subentries)
        ent_reg = er.async_get(self.hass)
        for association in record.entities:
            current = ent_reg.async_get(association.entity_id)
            if current is None or current.unique_id != association.unique_id:
                if strict:
                    raise MigrationError("registry_entity_missing")
                continue
            target = planned[association.config_entry_id]
            if target not in present:
                if strict:
                    raise MigrationError("fleet_runtime_mismatch")
                continue
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
            owners = [owner for owner in owners if planned[owner] in present]
            if SINGLE_OWNER_DEVICES:
                if owners and device_links(device) != {anchor_id: {planned[owners[0]]}}:
                    _move_device(dev_reg, device_id, anchor_id, planned[owners[0]])
                continue
            for owner in owners:
                target = planned[owner]
                current_targets = device_links(device).get(anchor_id, set())
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
                    if None in device_links(device).get(anchor_id, set()):
                        updated = dev_reg.async_update_device(
                            device_id,
                            remove_config_entry_id=anchor_id,
                            remove_config_subentry_id=None,
                        )
                        if updated is None:
                            raise MigrationError("registry_device_missing")
                        device = updated
                elif owner in device_links(device):
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
            links = device_links(device)
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

    def _anchor_converted(self, record: MigrationRecord) -> bool:
        anchor = self.hass.config_entries.async_get_entry(record.anchor_entry_id)
        planned = set(record.subentry_by_entry.values())
        return anchor is not None and (_is_fleet(anchor) or bool(planned & set(anchor.subentries)))

    async def _async_reverse(self, record: MigrationRecord, error: str) -> None:
        """Remove every migration artifact and record ``complete`` (reverted).

        Drift-tolerant: only the anchor is restored from its snapshot, and only
        when it was actually converted; every other candidate is left exactly as
        it is now, so an edit made during the transaction is never undone.
        """
        if record.phase is not MigrationPhase.COMPLETE:
            if record.phase is not MigrationPhase.REVERSING:
                record = record.advanced(MigrationPhase.REVERSING, last_error=error)
            record = await self._try_save(record)
        # Each restored entry's next successful setup must be quiet (no panel
        # contact). The obligation is made durable FIRST, before any entry or
        # registry is touched; if it cannot be, recovery stays unresolved.
        await self._async_require_quiet_obligation(self._restorable(record))
        await self._async_quiesce(
            entry_id
            for entry_id in record.candidates
            if self.hass.config_entries.async_get_entry(entry_id) is not None
        )
        await self._point("reverse_registries")
        self._restore_registries(record)
        await self._point("reverse_anchor")
        if self._anchor_converted(record):
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
        if not self._reverted_target_reached(record):
            raise MigrationError("reverse_incomplete")
        await self._point("complete")
        if record.phase is not MigrationPhase.COMPLETE:
            await self._try_save(record.advanced(MigrationPhase.COMPLETE, outcome=OUTCOME_REVERTED))
        self._fence.deactivate()
        ir.async_delete_issue(self.hass, DOMAIN, ISSUE_MIGRATION_RECOVERY_FAILED)

    def _restore_registries(self, record: MigrationRecord) -> None:
        anchor_id = record.anchor_entry_id
        planned_ids = {plan.subentry_id for plan in record.subentries}
        candidates = set(record.candidates)
        ent_reg = er.async_get(self.hass)
        for association in record.entities:
            current = ent_reg.async_get(association.entity_id)
            if (
                current is None
                or self.hass.config_entries.async_get_entry(association.config_entry_id) is None
            ):
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
            if SINGLE_OWNER_DEVICES:
                originals = [
                    (owner, subentries)
                    for owner, subentries in device_association.associations.items()
                    if owner in candidates
                    and self.hass.config_entries.async_get_entry(owner) is not None
                ]
                if originals and device_links(device) != {originals[0][0]: set(originals[0][1])}:
                    _move_device(
                        dev_reg, device.id, originals[0][0], next(iter(originals[0][1]), None)
                    )
                continue
            for owner, subentries in device_association.associations.items():
                if (
                    owner not in candidates
                    or self.hass.config_entries.async_get_entry(owner) is None
                ):
                    continue
                for subentry_id in subentries:
                    if subentry_id not in device_links(device).get(owner, set()):
                        updated = dev_reg.async_update_device(
                            device.id,
                            add_config_entry_id=owner,
                            add_config_subentry_id=subentry_id,
                        )
                        if updated is None:
                            raise MigrationError("registry_device_missing")
                        device = updated
            for subentry_id in sorted(
                planned_ids & set(device_links(device).get(anchor_id, set())),
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

    def _reverted_target_reached(self, record: MigrationRecord) -> bool:
        """Whether no migration artifact remains for a reversed migration."""
        if self._anchor_converted(record):
            return False
        planned = set(record.subentry_by_entry.values())
        candidates = set(record.candidates)
        ent_reg = er.async_get(self.hass)
        for association in record.entities:
            current = ent_reg.async_get(association.entity_id)
            if current is None or current.unique_id != association.unique_id:
                continue
            if current.config_subentry_id in planned or (
                current.config_entry_id in candidates
                and (current.config_entry_id, current.config_subentry_id)
                != (association.config_entry_id, association.config_subentry_id)
                and self.hass.config_entries.async_get_entry(association.config_entry_id)
                is not None
            ):
                return False
        dev_reg = dr.async_get(self.hass)
        for device_association in record.devices:
            device = dev_reg.async_get(device_association.device_id)
            if device is None:
                continue
            links = device_links(device)
            if planned & set(links.get(record.anchor_entry_id, set())):
                return False
            for owner, subentries in device_association.associations.items():
                if (
                    owner in candidates
                    and self.hass.config_entries.async_get_entry(owner) is not None
                    and not set(subentries) <= set(links.get(owner, set()))
                ):
                    return False
        return True

    def _restorable(self, record: MigrationRecord) -> frozenset[str]:
        return frozenset(
            entry_id
            for entry_id in record.candidates
            if self.hass.config_entries.async_get_entry(entry_id) is not None
        )

    async def _async_require_quiet_obligation(self, entry_ids: frozenset[str]) -> None:
        """Durably record that ``entry_ids`` must set up quietly, or fail closed."""
        async with quiet_lock(self.hass):
            store = QuietRestorations(self.hass)
            current = await store.async_load()
            try:
                await store.async_store((current or frozenset()) | entry_ids)
            except MigrationJournalError:
                async_report_quiet_state_unwritable(self.hass)
                raise MigrationError("quiet_marker_unproven") from None
        ir.async_delete_issue(self.hass, DOMAIN, ISSUE_QUIET_STATE_UNWRITABLE)
        self._fence.quiet.update(entry_ids)

    # --------------------------------------------------------------- recovery

    async def _async_recover_locked(self, record: MigrationRecord, *, live: bool) -> dict[str, Any]:
        """Re-apply the journaled target; confirm a ``complete`` journal on startup."""
        self._fence.activate()
        self._durable = record
        self._ambiguous = False
        try:
            if record.phase is MigrationPhase.COMPLETE:
                migrated = record.outcome == OUTCOME_MIGRATED
                reached = (
                    self._migrated_target_reached(record)
                    if migrated
                    else self._reverted_target_reached(record)
                )
                if reached:
                    # The state Home Assistant just loaded from disk is the
                    # target, so the terminal state is durable: forget it. A
                    # reversal's restored entries must first be durably quiet
                    # again (a prior obligation may already have been consumed by
                    # a setup whose stores never reached disk).
                    if not migrated:
                        await self._async_require_quiet_obligation(self._restorable(record))
                    await self._async_try_remove_journal()
                    self._fence.deactivate()
                    return {"status": "confirmed", "outcome": record.outcome}
                if migrated:
                    self._apply_conversion(record)
                    self._retarget_registries(record, strict=False)
                    await self._async_remove_remaining_siblings(record)
                    if not self._migrated_target_reached(record):
                        raise MigrationError("fleet_runtime_mismatch")
                    self._fence.deactivate()
                else:
                    await self._async_reverse(record, record.last_error or "interrupted")
                return {"status": "reapplied", "outcome": record.outcome}
            if _committed(record):
                self._apply_conversion(record)
                self._retarget_registries(record, strict=False)
                outcome = await self._async_complete_migration(record)
                anchor = self.hass.config_entries.async_get_entry(record.anchor_entry_id)
                if live and anchor is not None and anchor.state is not ConfigEntryState.LOADED:
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
            if _committed(record):
                self._fence.allowed.add(record.anchor_entry_id)
            return {"status": "recovery_failed", "error": code}
