"""Journal-first ownership of verified panel connection changes."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import replace
from typing import Any
from uuid import uuid4

import asyncssh
from homeassistant.config_entries import ConfigEntry, ConfigSubentry
from homeassistant.core import HomeAssistant
from homeassistant.helpers import issue_registry as ir

from .connection_state import ConnectionIntent, PanelConnection, PanelOwner
from .const import (
    CONF_HOST,
    CONF_IDENTITY_FINGERPRINT,
    CONF_PANEL,
    CONF_ROOT_PASSWORD,
    CONF_SSH_HOST_KEY,
    CONF_SSH_USERNAME,
    DOMAIN,
)
from .provisioning_journal import ProvisioningJournal, ProvisioningJournalError, StoredPanelRequest

CONNECTION_COMMIT_ID = "connection_commit_id"


class ConnectionOwnershipError(ValueError):
    """A retryable, redacted connection ownership refusal."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def connection(data: Mapping[str, Any]) -> PanelConnection:
    """Read one complete connection, never mixing config with journal fields."""
    return PanelConnection(
        str(data[CONF_HOST]),
        str(data.get(CONF_SSH_USERNAME, "root")),
        str(data[CONF_ROOT_PASSWORD]),
        str(data[CONF_SSH_HOST_KEY]),
    )


def _issue_id(owner: PanelOwner) -> str:
    return f"address_change_incomplete_{owner.entry_id}_{owner.subentry_id or 'legacy'}"


async def async_reconcile_connection_issues(hass: HomeAssistant) -> None:
    """Reflect durable evidence even when its original config owner was removed."""
    intents = await ProvisioningJournal(hass).async_connection_intents()
    wanted = {_issue_id(item.owner): item for item in intents}
    registry = ir.async_get(hass)
    for domain, issue_id in tuple(registry.issues):
        if (
            domain == DOMAIN
            and issue_id.startswith("address_change_incomplete_")
            and issue_id not in wanted
        ):
            issue = registry.async_get_issue(domain, issue_id)
            if issue is not None and issue.data and issue.data.get("notification_pending"):
                continue
            ir.async_delete_issue(hass, DOMAIN, issue_id)
    for issue_id, intent in wanted.items():
        ir.async_create_issue(
            hass,
            DOMAIN,
            issue_id,
            is_fixable=False,
            severity=ir.IssueSeverity.ERROR,
            translation_key="address_change_incomplete",
            translation_placeholders={
                "panel": intent.owner.slug,
                "action": "Change network address" if intent.owner.subentry_id else "Connection",
            },
        )


class PanelConnectionOwner:
    """Bind fresh verification and durable publication to one original HA owner."""

    def __init__(
        self, hass: HomeAssistant, entry: ConfigEntry[Any], subentry: ConfigSubentry | None = None
    ) -> None:
        self.hass = hass
        self.entry = entry
        self.subentry = subentry
        data = entry.data if subentry is None else subentry.data
        self.owner = PanelOwner(
            entry.entry_id,
            None if subentry is None else subentry.subentry_id,
            str(data[CONF_PANEL]),
            str(data[CONF_SSH_HOST_KEY]),
        )
        self.prior = connection(data)
        self.journal = ProvisioningJournal(hass)

    def live_data(self) -> Mapping[str, Any]:
        if self.hass.config_entries.async_get_entry(self.owner.entry_id) is not self.entry:
            raise ConnectionOwnershipError("parent_changed")
        if self.owner.subentry_id is None:
            data = self.entry.data
        else:
            current = self.entry.subentries.get(self.owner.subentry_id)
            if current is None:
                raise ConnectionOwnershipError("parent_changed")
            data = current.data
        if (
            data.get(CONF_PANEL) != self.owner.slug
            or data.get(CONF_SSH_HOST_KEY) != self.owner.public_key
        ):
            raise ConnectionOwnershipError("baseline_owner_mismatch")
        return data

    def assert_current(self, expected: PanelConnection | None = None) -> None:
        if connection(self.live_data()) != (expected or self.prior):
            raise ConnectionOwnershipError("parent_changed")

    def assert_unique(self) -> None:
        """Check keys as well as optional fingerprints, including key-only legacy."""
        try:
            fingerprint = asyncssh.import_public_key(self.owner.public_key).get_fingerprint(
                "sha256"
            )
        except (ValueError, asyncssh.KeyImportError):
            fingerprint = None
        for entry in self.hass.config_entries.async_entries(DOMAIN):
            owners = [
                (None, entry.data),
                *((item.subentry_id, item.data) for item in entry.subentries.values()),
            ]
            for subentry_id, data in owners:
                if (entry.entry_id, subentry_id) == (self.owner.entry_id, self.owner.subentry_id):
                    continue
                sibling_key = data.get(CONF_SSH_HOST_KEY)
                sibling_fingerprint = None
                if isinstance(sibling_key, str):
                    try:
                        sibling_fingerprint = asyncssh.import_public_key(
                            sibling_key.strip()
                        ).get_fingerprint("sha256")
                    except (ValueError, asyncssh.KeyImportError):
                        pass
                if (
                    data.get(CONF_SSH_HOST_KEY) == self.owner.public_key
                    or fingerprint is not None
                    and (
                        data.get(CONF_IDENTITY_FINGERPRINT) == fingerprint
                        or sibling_fingerprint == fingerprint
                    )
                ):
                    raise ConnectionOwnershipError("duplicate_panel")

    async def async_commit(self, *, host: str, password: str) -> None:
        """Publish only this request's verified tuple while its operation lock is held."""
        from .fleet_manager import async_wait_config_entry_persisted

        proposed = replace(self.prior, host=host, password=password)
        self.assert_current()
        self.assert_unique()
        try:
            active = await self.journal.async_load()
            retained = await self.journal.async_retained_records()
        except ProvisioningJournalError:
            raise ConnectionOwnershipError("address_change_incomplete") from None
        self.assert_current()
        transactions = tuple(
            sorted(
                {
                    str(record.transaction_id)
                    for record in ([active] if active is not None else [])
                    + [row.record for row in retained]
                    if record.panel_request.slug == self.owner.slug
                    and record.panel_request.public_key == self.owner.public_key
                }
            )
        )
        for record in ([active] if active is not None else []) + [row.record for row in retained]:
            request = record.panel_request
            if request.slug == self.owner.slug and request.public_key != self.owner.public_key:
                raise ConnectionOwnershipError("baseline_owner_mismatch")
            if request.public_key == self.owner.public_key and (
                request.slug != self.owner.slug or request.bound_owner not in {None, self.owner}
            ):
                raise ConnectionOwnershipError("baseline_owner_mismatch")
        for row in retained:
            if (
                row.recovery_record is not None
                and row.record.panel_request.public_key == self.owner.public_key
                and row.recovery_record.panel_request.bound_owner not in {None, self.owner}
            ):
                raise ConnectionOwnershipError("baseline_owner_mismatch")
        issue = ir.async_get(self.hass).async_get_issue(DOMAIN, _issue_id(self.owner))
        commit_id = self.live_data().get(CONNECTION_COMMIT_ID)
        if (
            proposed == self.prior
            and issue is not None
            and issue.data
            and issue.data.get("notification_pending")
            and issue.data.get("commit_id") == commit_id
            and isinstance(commit_id, str)
        ):
            pending = await self.journal.async_connection_intents()
            requests = [
                record.panel_request
                for record in ([active] if active is not None else [])
                + [row.record for row in retained]
                if record.panel_request.public_key == self.owner.public_key
            ]
            if (
                not any(item.owner == self.owner for item in pending)
                and all(
                    request.host == proposed.host and request.ssh_username == proposed.username
                    for request in requests
                )
                and (
                    active is None
                    or active.panel_request.public_key != self.owner.public_key
                    or active.panel_request.root_password == proposed.password
                )
            ):
                try:
                    await async_wait_config_entry_persisted(
                        self.hass, self.entry, subentry_id=self.owner.subentry_id
                    )
                    self.assert_current(proposed)
                    if self.live_data().get(CONNECTION_COMMIT_ID) != commit_id:
                        raise ConnectionOwnershipError("parent_changed")
                except Exception:
                    raise ConnectionOwnershipError("address_change_incomplete") from None
                self._enqueue_runtime()
                return
        intent = ConnectionIntent(
            str(uuid4()),
            self.owner,
            self.prior,
            proposed,
            transactions,
            str(active.transaction_id)
            if active is not None and active.panel_request.public_key == self.owner.public_key
            else None,
            tuple(
                sorted(
                    str(row.record.transaction_id)
                    for row in retained
                    if row.record.panel_request.public_key == self.owner.public_key
                )
            ),
        )
        committed = False
        removal_started = False
        issue_reconciliation_failed = False
        cancellation: asyncio.CancelledError | None = None

        def assert_commit() -> None:
            self.assert_current(proposed)
            if self.live_data().get(CONNECTION_COMMIT_ID) != intent.commit_id:
                raise ConnectionOwnershipError("parent_changed")

        async def validate_commit() -> None:
            assert_commit()
            await async_wait_config_entry_persisted(
                self.hass, self.entry, subentry_id=self.owner.subentry_id
            )
            assert_commit()

        try:
            await self.journal.async_begin_connection_change(intent)
            await self.journal.async_retarget_connection(intent)
            self.assert_current()
            data = {
                **self.live_data(),
                CONF_HOST: proposed.host,
                CONF_ROOT_PASSWORD: proposed.password,
                CONF_SSH_HOST_KEY: proposed.public_key,
                CONNECTION_COMMIT_ID: intent.commit_id,
            }
            if self.subentry is None:
                self.hass.config_entries.async_update_entry(self.entry, data=data)
            else:
                assert self.owner.subentry_id is not None
                current = self.entry.subentries[self.owner.subentry_id]
                self.hass.config_entries.async_update_subentry(self.entry, current, data=data)
            await validate_commit()
            removal_started = True
            await self.journal.async_finish_connection_change(
                intent, validate_owner=validate_commit
            )
            committed = True
        except asyncio.CancelledError as error:
            # Intent removal may have reached disk before cancellation surfaced.
            pending = await self.journal.async_connection_intents()
            committed = removal_started and not any(
                item.commit_id == intent.commit_id for item in pending
            )
            if committed:
                try:
                    assert_commit()
                except ConnectionOwnershipError:
                    committed = False
                if not committed:
                    await self.journal.async_begin_connection_change(intent)
            cancellation = error
        except ConnectionOwnershipError:
            raise
        except Exception:
            raise ConnectionOwnershipError("address_change_incomplete") from None
        finally:
            if committed:
                self._enqueue_runtime()
            try:
                await async_reconcile_connection_issues(self.hass)
            except Exception:
                issue_reconciliation_failed = True
        if cancellation is not None:
            raise cancellation
        if issue_reconciliation_failed:
            raise ConnectionOwnershipError("address_change_incomplete")

    def _enqueue_runtime(self) -> None:
        """Enqueue only; recovery takes the same operation lock after we release it."""
        from .fleet_manager import FleetManager

        runtime = getattr(self.entry, "runtime_data", None)
        if self.hass.is_stopping or runtime is None:
            ir.async_delete_issue(self.hass, DOMAIN, _issue_id(self.owner))
            return
        try:
            if isinstance(runtime, FleetManager):
                runtime.async_schedule_connection_reconcile()
            else:
                self.hass.config_entries.async_schedule_reload(self.entry.entry_id)
            ir.async_delete_issue(self.hass, DOMAIN, _issue_id(self.owner))
        except Exception:
            ir.async_create_issue(
                self.hass,
                DOMAIN,
                _issue_id(self.owner),
                is_fixable=False,
                severity=ir.IssueSeverity.ERROR,
                translation_key="address_change_incomplete",
                data={
                    "notification_pending": True,
                    "commit_id": self.live_data().get(CONNECTION_COMMIT_ID),
                },
                translation_placeholders={
                    "panel": self.owner.slug,
                    "action": "Change network address" if self.owner.subentry_id else "Connection",
                },
            )
            raise ConnectionOwnershipError("address_change_incomplete") from None


async def async_resolve_panel_owner(hass: HomeAssistant, request: StoredPanelRequest) -> PanelOwner:
    """Resolve an exact current owner, respecting a prior named operation binding."""
    bound = request.bound_owner
    matches: list[PanelOwner] = []
    for entry in hass.config_entries.async_entries(DOMAIN):
        candidates = (
            [(subentry.subentry_id, subentry.data) for subentry in entry.subentries.values()]
            if entry.subentries
            else [(None, entry.data)]
        )
        for subentry_id, data in candidates:
            owner = PanelOwner(
                entry.entry_id,
                subentry_id,
                str(data.get(CONF_PANEL, "unknown")),
                str(data.get(CONF_SSH_HOST_KEY, "unknown")),
            )
            if bound is not None and bound != owner:
                continue
            if (
                data.get(CONF_PANEL) == request.slug
                and data.get(CONF_SSH_HOST_KEY) == request.public_key
                and data.get(CONF_HOST) == request.host
            ):
                matches.append(owner)
    if len(matches) != 1:
        raise ConnectionOwnershipError("baseline_owner_mismatch")
    await ProvisioningJournal(hass).async_assert_connection_available(
        owner=matches[0], request=request
    )
    return matches[0]
