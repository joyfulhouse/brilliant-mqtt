"""Durable record of a pre-mutation agent update window (#219).

An agent update keeps a small record while it is in its retryable window (the
connect and release-admission stages, before any baseline capture). If Home
Assistant stops inside that window the record survives and, on the next setup,
the interrupted update is SURFACED as a fixable Repair issue. It is never
resumed automatically: the operator chooses Retry (a fresh update with a fresh
retry budget) or Dismiss. Once baseline capture begins the provisioning journal
owns recovery, so the record is cleared before that stage.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Protocol

import asyncssh
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.storage import Store
from homeassistant.util import dt as dt_util

from .const import DOMAIN
from .panel_ops import PanelOpError, candidate_identities
from .provisioning_journal import ProvisioningJournal, StoredPanelRequest

_LOGGER = logging.getLogger(__name__)

UPDATE_INTENT_STORAGE_VERSION = 1
UPDATE_INTENT_STORAGE_KEY = f"{DOMAIN}.update_intents"
_STORE_DATA_KEY = f"{DOMAIN}_update_intent_store"
_INTENT_KEYS = frozenset(
    {
        "panel",
        "target_version",
        "target_ordinal",
        "target_digest",
        "target_set_digest",
        "stage",
        "attempts",
        "max_attempts",
        "started_at",
        "deadline",
    }
)
_SLUG = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
_VERSION = re.compile(r"^[0-9A-Za-z][0-9A-Za-z._+-]{0,127}$")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_MAX_ATTEMPTS_BOUND = 100

type IntentStage = Literal["connect", "admission"]


@dataclass(frozen=True, slots=True)
class UpdateTarget:
    """The exact release an update would deploy: the bridge plus every component."""

    version: str
    ordinal: int | None
    digest: str
    set_digest: str


async def compute_update_target(
    payload_dir: str, components: tuple[str, ...]
) -> UpdateTarget | None:
    """Identify the bundled candidate; None when the payload cannot be read.

    ``set_digest`` is SHA-256 over the identity of EVERY component the update would
    deploy, so a change to any selected component is a different target.
    """
    try:
        candidate = await candidate_identities(payload_dir)
    except PanelOpError:
        return None
    selected = tuple(dict.fromkeys(("bridge", *components)))
    if any(component not in candidate for component in selected):
        return None
    wire = "".join(
        f"{component}\t{identity.version}\t{identity.release_ordinal}\t{identity.digest}\n"
        for component in sorted(selected)
        for identity in (candidate[component],)
    )
    bridge = candidate["bridge"]
    return UpdateTarget(
        version=bridge.version,
        ordinal=bridge.release_ordinal,
        digest=bridge.digest,
        set_digest=hashlib.sha256(wire.encode()).hexdigest(),
    )


def _aware(value: object) -> datetime | None:
    if not isinstance(value, str) or len(value) > 64:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None and parsed.utcoffset() is not None else None


def _count(value: object, *, minimum: int) -> bool:
    return type(value) is int and minimum <= value <= _MAX_ATTEMPTS_BOUND


@dataclass(frozen=True, slots=True)
class UpdateIntent:
    """One panel's in-window update, strictly validated; invalid reads as absent."""

    panel: str
    target_version: str
    target_ordinal: int | None
    target_digest: str
    target_set_digest: str
    stage: IntentStage
    attempts: int
    max_attempts: int
    started_at: datetime
    deadline: datetime

    def __post_init__(self) -> None:
        if (
            not isinstance(self.panel, str)
            or _SLUG.fullmatch(self.panel) is None
            or not isinstance(self.target_version, str)
            or _VERSION.fullmatch(self.target_version) is None
            or (
                self.target_ordinal is not None
                and (type(self.target_ordinal) is not int or not 0 < self.target_ordinal < 10**16)
            )
            or not isinstance(self.target_digest, str)
            or _DIGEST.fullmatch(self.target_digest) is None
            or not isinstance(self.target_set_digest, str)
            or _DIGEST.fullmatch(self.target_set_digest) is None
            or self.stage not in ("connect", "admission")
            or not _count(self.attempts, minimum=0)
            or not _count(self.max_attempts, minimum=1)
            or not isinstance(self.started_at, datetime)
            or self.started_at.tzinfo is None
            or not isinstance(self.deadline, datetime)
            or self.deadline.tzinfo is None
        ):
            raise ValueError("invalid_update_intent")

    def matches(self, target: UpdateTarget | None) -> bool:
        """Whether *target* is exactly the release this intent was for."""
        return (
            target is not None
            and target.version == self.target_version
            and target.ordinal == self.target_ordinal
            and target.digest == self.target_digest
            and target.set_digest == self.target_set_digest
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "panel": self.panel,
            "target_version": self.target_version,
            "target_ordinal": self.target_ordinal,
            "target_digest": self.target_digest,
            "target_set_digest": self.target_set_digest,
            "stage": self.stage,
            "attempts": self.attempts,
            "max_attempts": self.max_attempts,
            "started_at": self.started_at.isoformat(),
            "deadline": self.deadline.isoformat(),
        }

    @classmethod
    def from_dict(cls, raw: object) -> UpdateIntent | None:
        """Parse one stored record; anything not exactly valid is absent."""
        if not isinstance(raw, Mapping) or frozenset(raw) != _INTENT_KEYS:
            return None
        started_at = _aware(raw["started_at"])
        deadline = _aware(raw["deadline"])
        stage = raw["stage"]
        if started_at is None or deadline is None or stage not in ("connect", "admission"):
            return None
        try:
            return cls(
                panel=raw["panel"],
                target_version=raw["target_version"],
                target_ordinal=raw["target_ordinal"],
                target_digest=raw["target_digest"],
                target_set_digest=raw["target_set_digest"],
                stage="connect" if stage == "connect" else "admission",
                attempts=raw["attempts"],
                max_attempts=raw["max_attempts"],
                started_at=started_at,
                deadline=deadline,
            )
        except ValueError:
            return None


class UpdateIntentStore:
    """HA-wide store of in-window update records, keyed by panel management ID.

    Every mutation builds a NEW mapping, awaits ``Store.async_save`` and only then
    replaces the cache, so a save that raises leaves the previous record in place.
    Honest limit: HA's ``Store`` swallows write I/O errors itself
    (``helpers/storage.py`` ``_async_handle_write_data`` logs and returns), so a
    disk failure is NOT observable here; only exceptions raised before the write
    (for example while serializing) reach the caller.
    """

    def __init__(self, hass: HomeAssistant) -> None:
        self._store: Store[dict[str, Any]] = Store(
            hass,
            UPDATE_INTENT_STORAGE_VERSION,
            UPDATE_INTENT_STORAGE_KEY,
            private=True,
            atomic_writes=True,
        )
        self._cache: dict[str, dict[str, object]] | None = None
        self._lock = asyncio.Lock()

    async def _async_records(self) -> dict[str, dict[str, object]]:
        if self._cache is None:
            try:
                raw = await self._store.async_load()
            except HomeAssistantError:
                raw = None  # unreadable storage: no record survives
            records = raw.get("intents") if isinstance(raw, dict) else None
            self._cache = (
                {
                    key: dict(value)
                    for key, value in records.items()
                    if isinstance(key, str) and isinstance(value, dict)
                }
                if isinstance(records, dict)
                else {}
            )
        return self._cache

    async def async_get(self, key: str) -> UpdateIntent | None:
        async with self._lock:
            return UpdateIntent.from_dict((await self._async_records()).get(key))

    async def async_set(self, key: str, intent: UpdateIntent) -> None:
        async with self._lock:
            records = await self._async_records()
            updated = {**records, key: intent.as_dict()}
            await self._store.async_save({"intents": updated})
            self._cache = updated

    async def async_clear(self, key: str) -> None:
        async with self._lock:
            records = await self._async_records()
            if key not in records:
                return
            updated = {name: value for name, value in records.items() if name != key}
            await self._store.async_save({"intents": updated})
            self._cache = updated


def update_intent_store(hass: HomeAssistant) -> UpdateIntentStore:
    """Return the one HA-wide update intent store."""
    store = hass.data.get(_STORE_DATA_KEY)
    if not isinstance(store, UpdateIntentStore):
        store = UpdateIntentStore(hass)
        hass.data[_STORE_DATA_KEY] = store
    return store


async def async_forget_update_intent(hass: HomeAssistant, management_id: str) -> None:
    """Forget a removed panel's window record; never blocks removal."""
    try:
        await update_intent_store(hass).async_clear(management_id)
    except Exception as error:
        _LOGGER.warning("Update intent cleanup failed (%s)", type(error).__name__)


def pinned_fingerprint(key: object) -> str | None:
    """SHA-256 fingerprint of a pinned public key; None when missing or unparseable."""
    if not isinstance(key, str) or not key:
        return None
    try:
        return str(asyncssh.import_public_key(key).get_fingerprint("sha256"))
    except (asyncssh.KeyImportError, ValueError):
        return None


def request_concerns_panel(
    request: StoredPanelRequest, *, slug: str, host: str, pinned_key: object
) -> bool:
    """Whether a journaled panel request may be about this panel.

    Matches on the pinned key, its fingerprint, the slug or the host. Fails closed
    (True) when this panel's pinned key is missing or unparseable, because then
    identity cannot be ruled out.
    """
    fingerprint = pinned_fingerprint(pinned_key)
    if fingerprint is None:
        return True
    return (
        request.public_key == pinned_key
        or request.fingerprint == fingerprint
        or request.slug == slug
        or request.host == host
    )


async def async_journal_concerns_panel(
    hass: HomeAssistant, *, slug: str, host: str, pinned_key: object
) -> bool:
    """Whether the active journal or any retained record may concern this panel.

    An unreadable journal counts as concerning the panel.
    """
    journal = ProvisioningJournal(hass)
    try:
        record = await journal.async_load()
        retained = await journal.async_retained_records()
    except Exception:
        return True
    requests = [item.record.panel_request for item in retained]
    if record is not None:
        requests.append(record.panel_request)
    return any(
        request_concerns_panel(request, slug=slug, host=host, pinned_key=pinned_key)
        for request in requests
    )


class UpdatePanel(Protocol):
    """The public surface of one panel runtime that InterruptedUpdates needs."""

    @property
    def hass(self) -> HomeAssistant: ...

    @property
    def panel(self) -> str: ...

    @property
    def management_id(self) -> str: ...

    @property
    def interrupted_issue_id(self) -> str: ...

    @property
    def update_running(self) -> bool: ...

    async def async_update_target(self) -> UpdateTarget | None: ...

    async def async_journal_owns_panel(self) -> bool: ...

    def notify_update_state(self) -> None: ...

    async def async_update_agent(self) -> None: ...


class InterruptedUpdates:
    """Own one panel's update window record and its surfaced Repair issue."""

    def __init__(self, panel: UpdatePanel, store: UpdateIntentStore) -> None:
        self._panel = panel
        self._store = store
        self._window_started: datetime | None = None
        self.surfaced: UpdateIntent | None = None

    async def async_begin_window(
        self,
        stage: IntentStage,
        attempts: int,
        max_attempts: int,
        deadline: datetime,
    ) -> bool:
        """Persist the window record; only then drop a surfaced interrupted update.

        Returns False, overwriting nothing, when no target can be computed.
        """
        target = await self._panel.async_update_target()
        if target is None:
            return False
        if self._window_started is None:
            self._window_started = dt_util.utcnow()
        intent = UpdateIntent(
            panel=self._panel.panel,
            target_version=target.version,
            target_ordinal=target.ordinal,
            target_digest=target.digest,
            target_set_digest=target.set_digest,
            stage=stage,
            attempts=attempts,
            max_attempts=max_attempts,
            started_at=self._window_started,
            deadline=deadline,
        )
        await self._store.async_set(self._panel.management_id, intent)
        self._drop_surfaced()
        return True

    async def async_end_window(self) -> None:
        """Clear the window record (baseline stage reached or a terminal outcome)."""
        self._window_started = None
        await self._store.async_clear(self._panel.management_id)
        self._drop_surfaced()

    def _drop_surfaced(self) -> None:
        if self.surfaced is None:
            return
        self.surfaced = None
        ir.async_delete_issue(self._panel.hass, DOMAIN, self._panel.interrupted_issue_id)
        self._panel.notify_update_state()

    async def async_restore(self) -> None:
        """Surface a record left by an HA stop. Never connects; never fails setup."""
        try:
            intent = await self._store.async_get(self._panel.management_id)
            if intent is None:
                return
            if await self._panel.async_journal_owns_panel():
                # The provisioning journal owns recovery of this panel: it wins.
                await self._store.async_clear(self._panel.management_id)
                return
            self.surfaced = intent
            ir.async_create_issue(
                self._panel.hass,
                DOMAIN,
                self._panel.interrupted_issue_id,
                is_fixable=True,
                is_persistent=False,
                severity=ir.IssueSeverity.WARNING,
                translation_key="interrupted_update",
                translation_placeholders={
                    "panel": self._panel.panel,
                    "target_version": intent.target_version,
                    "stage": intent.stage,
                    "attempts": str(intent.attempts),
                },
                data={"management_id": self._panel.management_id},
            )
            self._panel.notify_update_state()
        except Exception as error:
            _LOGGER.warning(
                "%s: interrupted update could not be restored (%s)",
                self._panel.panel,
                type(error).__name__,
            )

    def diagnostics(self) -> dict[str, object] | None:
        """The surfaced interrupted update, allowlisted.

        ``expired`` is evaluated when read, so on the entity it only refreshes when
        the state is otherwise written.
        """
        intent = self.surfaced
        if intent is None:
            return None
        return {
            "attempts": intent.attempts,
            "deadline": intent.deadline.isoformat(),
            "expired": dt_util.utcnow() > intent.deadline,
            "max_attempts": intent.max_attempts,
            "stage": intent.stage,
            "started_at": intent.started_at.isoformat(),
            "target_version": intent.target_version,
        }

    async def async_retry(self) -> str | None:
        """Start a fresh update for the surfaced intent; return an abort reason."""
        if self._panel.update_running:
            return "already_in_progress"
        intent = self.surfaced
        if intent is None:
            return "interrupted_update_unavailable"
        if not intent.matches(await self._panel.async_update_target()):
            return "interrupted_update_target_changed"
        if await self._panel.async_journal_owns_panel():
            await self._store.async_clear(self._panel.management_id)
            self._drop_surfaced()
            return "interrupted_update_recovery_owned"
        # No await between this check and the eager task start: the update sets its
        # running flag synchronously before its first await.
        if self._panel.update_running:
            return "already_in_progress"
        self._panel.hass.async_create_task(
            self._async_run_retry(), f"{DOMAIN}-retry-{self._panel.panel}", eager_start=True
        )
        return None

    async def _async_run_retry(self) -> None:
        try:
            await self._panel.async_update_agent()
        except HomeAssistantError:
            pass  # already escalated and recorded by the update itself

    async def async_dismiss(self) -> None:
        """Forget the interrupted update. Raises when the record could not be cleared."""
        await self._store.async_clear(self._panel.management_id)
        self._drop_surfaced()
