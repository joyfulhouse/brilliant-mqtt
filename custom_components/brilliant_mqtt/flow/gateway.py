"""Runtime boundary used by the config flows: SSH sessions, broker validation.

Every callable here touches a live panel or broker (or resolves the production
helpers that do). Tests patch these names on THIS module, so flow modules must
call them as ``gateway.<name>(...)`` rather than importing them directly.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .. import _fleet_lock, panel_ops
from ..broker_validation import BrokerValidator
from ..panel_inspection import (
    PanelCompatibilityError,
    PanelFacts,
    async_inspect_panel,
)
from ..panel_provisioner import PanelProvisioner, ProvisioningProgress
from ..shell import (
    AsyncsshShell as FleetAsyncsshShell,
)
from ..shell import (
    HostIdentity,
    PanelShell,
    _LegacyAsyncsshShell,
    async_fetch_host_identity,
)

AsyncsshShell = _LegacyAsyncsshShell

__all__ = [
    "AsyncsshShell",
    "FleetAsyncsshShell",
    "async_fetch_host_identity",
    "async_inspect_panel",
]


class _WrongPanelError(Exception):
    """A reconfigure connected to a host already running a DIFFERENT panel's agent.

    Guards against a mistyped host (e.g. another controller's IP in a multi-panel
    fleet): pushing this entry's env there would overwrite that panel's identity and
    restart it. Carries the foreign panel slug found on the box.
    """


@asynccontextmanager
async def _panel_session(
    hass: HomeAssistant, host: str, password: str, pinned_key: str | None
) -> AsyncIterator[PanelShell]:
    """One serialized SSH session (fleet lock held), connected and always closed.

    With `pinned_key` set the server key is verified BEFORE auth (a rotated/impostor
    host never receives the root password); `pinned_key=None` is trust-on-first-use.
    """
    async with _fleet_lock(hass):
        shell = AsyncsshShell(host, password, pinned_key)
        try:
            await shell.connect()
            yield shell
        finally:
            await shell.close()


async def _apply_config(
    hass: HomeAssistant,
    host: str,
    password: str,
    *,
    pinned_key: str | None,
    env_content: str,
    expected_panel: str,
) -> str:
    """Verify/capture the host key; if the agent is installed, push env + restart.

    Returns the (pinned/verified) host key. A not-yet-installed panel skips the push —
    the entry update still lands and the next deploy renders the new values.

    Before overwriting, it refuses to clobber a DIFFERENT panel: if the box already
    runs an agent whose env names another slug than *expected_panel* (e.g. the host
    was mistyped to another controller in the fleet), it raises _WrongPanelError
    instead of stamping this entry's identity onto that panel and restarting it.
    """
    async with _panel_session(hass, host, password, pinned_key) as shell:
        key = shell.pinned_host_key()
        if key is None:
            raise OSError("no host key captured")
        state = await panel_ops.inspect_panel(shell)
        if state.unit_present:
            if state.env_present:
                found = (await panel_ops.read_env(shell)).get(panel_ops.ENV_PANEL)
                if found and found != expected_panel:
                    raise _WrongPanelError(found)
            await panel_ops.write_env(shell, env_content)
            await panel_ops.restart(shell)
        return key


def _broker_validator(hass: HomeAssistant) -> BrokerValidator:
    """Build the behavioral validator without exporting broker secrets."""
    return BrokerValidator(
        hass,
        lambda profile, client_id: profile.device_client(client_id),
    )


def _get_panel_provisioner(
    hass: HomeAssistant,
    *,
    expected_identity: HostIdentity,
) -> PanelProvisioner:
    """Resolve the production provisioner lazily to avoid package import cycles."""
    from .. import get_panel_provisioner

    return get_panel_provisioner(
        hass,
        expected_identity=expected_identity,
    )


async def _async_wait_config_entry_persisted(
    hass: HomeAssistant,
    entry: ConfigEntry[Any],
    *,
    subentry_id: str | None = None,
) -> None:
    """Resolve the durability gate lazily to avoid package import cycles."""
    from ..fleet_manager import async_wait_config_entry_persisted

    await async_wait_config_entry_persisted(
        hass,
        entry,
        subentry_id=subentry_id,
    )


async def _async_verify_staged_progress(
    hass: HomeAssistant,
    update: ProvisioningProgress,
) -> None:
    """Accept a flow ownership marker only after its exact STAGED journal exists."""
    from ..provisioning_journal import ProvisioningJournal, ProvisioningPhase

    record = await ProvisioningJournal(hass).async_load()
    if (
        record is None
        or record.phase is not ProvisioningPhase.STAGED
        or record.transaction_id != update.transaction_id
        or record.setup_id != update.setup_id
    ):
        raise ValueError("invalid_provisioning_progress")


async def _async_provisioning_journal_clear(hass: HomeAssistant) -> bool:
    """Fail closed unless durable provisioning state is readable and empty."""
    from ..provisioning_journal import ProvisioningJournal

    try:
        return await ProvisioningJournal(hass).async_load() is None
    except asyncio.CancelledError:
        raise
    except Exception:
        return False


async def _async_close_inspection_shell(shell: PanelShell) -> None:
    """Drain SSH close under caller cancellation, then preserve cancellation."""
    close_task = asyncio.create_task(
        shell.close(),
        name="brilliant-mqtt-inspection-shell-close",
    )
    try:
        await asyncio.shield(close_task)
    except asyncio.CancelledError as cancellation:
        while not close_task.done():
            try:
                await asyncio.shield(close_task)
            except asyncio.CancelledError:
                continue
        try:
            close_task.result()
        except BaseException:
            pass
        raise cancellation


async def _async_inspect_candidate(
    hass: HomeAssistant,
    host: str,
    root_password: str,
    identity: HostIdentity,
) -> PanelFacts:
    """Authenticate only to the previously fetched identity and inspect read-only."""
    async with _fleet_lock(hass):
        shell = FleetAsyncsshShell(host, root_password, identity.public_key)
        primary: BaseException | None = None
        try:
            await shell.connect()
            return await async_inspect_panel(shell, identity)
        except BaseException as error:
            primary = error
            raise
        finally:
            try:
                await _async_close_inspection_shell(shell)
            except asyncio.CancelledError:
                raise
            except BaseException:
                if primary is None:
                    raise PanelCompatibilityError("inspection_failed") from None
