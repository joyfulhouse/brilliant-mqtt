"""PanelManager — per-entry runtime: MQTT watchers and the OTA state machine.

State machine (one panel): the availability LWT and the retained bridge meta drive
everything. offline → (grace timer) → auto-repair (restore unit/env, enable) →
(recovery timer) → online ? repair_succeeded : escalate. A repair cooldown stops a
flapping panel from being repaired in a tight loop; auto-repair can be turned off
per panel, in which case an outage only notifies. A firmware change on the meta topic
fires panel_updated and re-stages the OTA-proof config copies.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import asyncssh
from homeassistant.components import mqtt, persistent_notification
from homeassistant.components.mqtt.models import ReceiveMessage
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import CALLBACK_TYPE, HomeAssistant, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.dispatcher import async_dispatcher_send
from homeassistant.helpers.event import async_call_later

from . import panel_ops
from .const import (
    AVAILABILITY_OFFLINE,
    AVAILABILITY_ONLINE,
    CONF_HOST,
    CONF_MESH_PRIORITY,
    CONF_MQTT_HOST,
    CONF_MQTT_PASSWORD,
    CONF_MQTT_PORT,
    CONF_MQTT_USERNAME,
    CONF_PANEL,
    CONF_ROOT_PASSWORD,
    DATA_LAST_FIRMWARE,
    DATA_SSH_HOST_KEY,
    DEFAULT_AUTO_REPAIR,
    DEFAULT_OFFLINE_GRACE_MINUTES,
    DEFAULT_REPAIR_COOLDOWN_MINUTES,
    EVENT_AGENT_UPDATED,
    EVENT_NEEDS_ATTENTION,
    EVENT_PANEL_UPDATED,
    EVENT_REPAIR_FAILED,
    EVENT_REPAIR_STARTED,
    EVENT_REPAIR_SUCCEEDED,
    EVENT_TYPE,
    OPT_AUTO_REPAIR,
    OPT_OFFLINE_GRACE_MINUTES,
    OPT_REPAIR_COOLDOWN_MINUTES,
    SIGNAL_PANEL_STATE,
    availability_topic,
    meta_topic,
)
from .panel_ops import PanelOpError
from .shell import AsyncsshShell, PanelShell

_LOGGER = logging.getLogger(__name__)

_RECOVERY_SECONDS = 60.0
_UNREACHABLE_RECHECK_SECONDS = 300.0


def _payload_dir() -> Path:
    """The bundled agent payload (built by scripts/build_payload.sh / release CI)."""
    return Path(__file__).parent / "agent_payload"


class PanelManager:
    """Owns one panel's state. Entities read it; the state machine mutates it."""

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry, ssh_lock: asyncio.Lock) -> None:
        self.hass = hass
        self.entry = entry
        self.panel: str = entry.data[CONF_PANEL]
        self._ssh_lock = ssh_lock  # fleet-wide: ONE panel SSH op at a time
        self.availability: str | None = None  # None until the retained LWT arrives
        self.meta: dict[str, Any] | None = None
        self.problem = False
        self.problem_reason: str | None = None
        self._unsubs: list[Any] = []
        self._grace_cancel: CALLBACK_TYPE | None = None
        self._recovery_cancel: CALLBACK_TYPE | None = None
        self._last_repair_mono: float | None = None
        self._repairing = False
        # Set true by async_shutdown. A repair already awaiting inside the ssh_lock
        # resumes AFTER shutdown's one-shot cancel; this flag stops it re-arming a
        # timer that would then fire on a torn-down entry and SSH a removed panel.
        self._shutting_down = False

    @property
    def signal(self) -> str:
        """Dispatcher signal entities subscribe to for state refreshes."""
        return f"{SIGNAL_PANEL_STATE}_{self.entry.entry_id}"

    def _shell(self) -> PanelShell:
        return AsyncsshShell(
            self.entry.data[CONF_HOST],
            self.entry.data[CONF_ROOT_PASSWORD],
            self.entry.data.get(DATA_SSH_HOST_KEY),
        )

    async def async_setup(self) -> None:
        self._unsubs.append(
            await mqtt.async_subscribe(
                self.hass, availability_topic(self.panel), self._on_availability
            )
        )
        self._unsubs.append(
            await mqtt.async_subscribe(self.hass, meta_topic(self.panel), self._on_meta)
        )

    async def async_shutdown(self) -> None:
        # Latch shutdown FIRST: a repair wedged in the ssh_lock checks this flag
        # before (re-)arming a timer, so it cannot schedule one past this cancel.
        self._shutting_down = True
        # Cancel any in-flight timers BEFORE dropping the subscriptions so an entry
        # unload (or reload) never leaves a grace/recovery callback dangling.
        self._cancel("_grace_cancel")
        self._cancel("_recovery_cancel")
        for unsub in self._unsubs:
            unsub()
        self._unsubs.clear()

    @callback
    def _notify(self) -> None:
        async_dispatcher_send(self.hass, self.signal)

    def _opt(self, key: str, default: Any) -> Any:
        return self.entry.options.get(key, default)

    def _fire(self, event_type: str, data: dict[str, Any] | None = None) -> None:
        self.hass.bus.async_fire(
            EVENT_TYPE,
            {"type": event_type, "panel": self.panel, "entry_id": self.entry.entry_id}
            | (data or {}),
        )

    @callback
    def _set_problem(self, problem: bool, reason: str | None) -> None:
        self.problem = problem
        self.problem_reason = reason
        self._notify()

    def _cancel(self, attr: str) -> None:
        cancel: CALLBACK_TYPE | None = getattr(self, attr)
        if cancel is not None:
            cancel()
            setattr(self, attr, None)

    async def _on_availability(self, msg: ReceiveMessage) -> None:
        if self._shutting_down:
            return  # defense-in-depth: never arm a timer on a torn-down entry
        payload = str(msg.payload)
        self.availability = payload
        if payload == AVAILABILITY_ONLINE:
            self._cancel("_grace_cancel")
            if self._recovery_cancel is not None:
                self._cancel("_recovery_cancel")
                self._fire(EVENT_REPAIR_SUCCEEDED)
            self._set_problem(False, None)
        elif (
            payload == AVAILABILITY_OFFLINE
            and self._grace_cancel is None
            and self._recovery_cancel is None
            and not self._repairing
        ):
            grace_s = self._opt(OPT_OFFLINE_GRACE_MINUTES, DEFAULT_OFFLINE_GRACE_MINUTES) * 60
            self._grace_cancel = async_call_later(self.hass, grace_s, self._grace_expired)
        self._notify()

    async def _grace_expired(self, _now: datetime) -> None:
        self._grace_cancel = None
        if self._shutting_down:
            return
        if self.availability != AVAILABILITY_OFFLINE:
            return
        if not self._opt(OPT_AUTO_REPAIR, DEFAULT_AUTO_REPAIR):
            self._escalate("bridge offline past grace period (auto-repair is off)")
            return
        cooldown_s = self._opt(OPT_REPAIR_COOLDOWN_MINUTES, DEFAULT_REPAIR_COOLDOWN_MINUTES) * 60
        if (
            self._last_repair_mono is not None
            and time.monotonic() - self._last_repair_mono < cooldown_s
        ):
            self._escalate("bridge offline again within the repair cooldown")
            return
        await self.async_repair(trigger="auto")

    def _escalate(self, reason: str) -> None:
        self._fire(EVENT_NEEDS_ATTENTION, {"reason": reason})
        persistent_notification.async_create(
            self.hass,
            f"Panel `{self.panel}`: {reason}. See docs/ha-integration.md.",
            title="Brilliant MQTT needs attention",
            notification_id=f"{EVENT_TYPE}_{self.panel}",
        )
        self._set_problem(True, reason)

    async def _config_contents(self) -> tuple[str, str]:
        """(unit, env): unit from the bundled payload, env re-rendered from entry data.

        Always regenerated from known-good sources — never read back from the
        panel — so a repair also heals config drift.
        """
        unit = await self.hass.async_add_executor_job(
            (_payload_dir() / "brilliant-mqtt.service").read_text
        )
        data = self.entry.data
        env = panel_ops.render_env(
            panel=self.panel,
            mesh_priority=data[CONF_MESH_PRIORITY],
            mqtt_host=data[CONF_MQTT_HOST],
            mqtt_port=data[CONF_MQTT_PORT],
            mqtt_username=data[CONF_MQTT_USERNAME],
            mqtt_password=data[CONF_MQTT_PASSWORD],
        )
        return unit, env

    async def async_repair(self, trigger: str = "manual") -> None:
        """Restore unit/env + enable; recovery is confirmed by the availability LWT."""
        if self._repairing:
            return
        self._repairing = True
        # A grace timer may be pending (armed by _on_availability on offline). Cancel it
        # so it can't later fire _grace_expired → within-cooldown → a spurious
        # needs_attention during the recovery window this repair opens.
        self._cancel("_grace_cancel")
        self._fire(EVENT_REPAIR_STARTED, {"trigger": trigger})
        try:
            async with self._ssh_lock:
                shell = self._shell()
                try:
                    await shell.connect()
                except (OSError, asyncssh.Error, PanelOpError) as err:
                    self._fire(EVENT_REPAIR_FAILED, {"reason": "unreachable"})
                    self._set_problem(True, f"panel unreachable: {err}")
                    # Record the cooldown so the recheck does not re-offer the root
                    # password to a flapping host every few minutes forever.
                    self._last_repair_mono = time.monotonic()
                    if self._shutting_down:
                        return  # entry torn down mid-repair: do not re-arm a timer
                    self._grace_cancel = async_call_later(
                        self.hass, _UNREACHABLE_RECHECK_SECONDS, self._grace_expired
                    )
                    return
                try:
                    await panel_ops.inspect_panel(shell)  # logged context (journal on fail)
                    unit, env = await self._config_contents()
                    await panel_ops.ensure_configs(shell, unit, env)
                    await panel_ops.enable_now(shell)
                except (OSError, asyncssh.Error, PanelOpError) as err:
                    # A checked step (mkdir/daemon-reload/systemctl) exited non-zero.
                    # The panel is half-broken; surface it loudly instead of letting
                    # the exception escape (silent + entry shows GREEN) or falling
                    # through to the success path (no recovery timer would ever fire).
                    self._fire(
                        EVENT_REPAIR_FAILED,
                        {"reason": "repair_step_failed", "error": str(err)},
                    )
                    self._escalate(f"repair step failed: {err}")
                    self._last_repair_mono = time.monotonic()  # gate any retry
                    return
                finally:
                    await shell.close()
            self._last_repair_mono = time.monotonic()
            if self._shutting_down:
                return  # entry torn down mid-repair: do not re-arm a timer
            self._recovery_cancel = async_call_later(
                self.hass, _RECOVERY_SECONDS, self._recovery_timeout
            )
        finally:
            self._repairing = False

    async def async_update_agent(self) -> None:
        """Push the bundled agent payload, refresh configs, restart, verify via LWT.

        Takes the SAME _repairing mutex as async_repair (C1): a concurrent repair (or
        a second update) early-returns, and — critically — while this holds it the
        restart-induced `offline` LWT cannot arm a grace timer (facet b), so one panel
        never runs grace + recovery at once. The recovery timer is armed only on
        success and only when not shutting down (no await before the schedule so
        async_shutdown's cancel can't be raced), and any prior _recovery_cancel handle
        is cancelled first so an update inside a repair's recovery window can't orphan
        a timer that would outlive shutdown (facet a).

        Unlike async_repair (timers/button → swallow + escalate), this runs only from
        the update.install service call, so on failure it escalates AND re-raises as
        HomeAssistantError (I4) — otherwise HA reports the install "done" while the
        agent is broken.
        """
        if self._repairing:
            raise HomeAssistantError("a repair or update is already in progress")
        self._repairing = True
        # Cancel any pending grace timer (as async_repair does) so an outage that armed
        # it can't fire a spurious needs_attention during this update's recovery window.
        self._cancel("_grace_cancel")
        try:
            version = (
                await self.hass.async_add_executor_job((_payload_dir() / "VERSION").read_text)
            ).strip()
            async with self._ssh_lock:
                shell = self._shell()
                try:
                    await shell.connect()
                    await panel_ops.deploy_payload(shell, str(_payload_dir()), version)
                    unit, env = await self._config_contents()
                    await panel_ops.ensure_configs(shell, unit, env)
                    await panel_ops.restart(shell)
                except (OSError, asyncssh.Error, PanelOpError) as err:
                    self._escalate(f"agent update failed: {err}")
                    raise HomeAssistantError(f"agent update failed: {err}") from err
                finally:
                    await shell.close()
            self._fire(EVENT_AGENT_UPDATED, {"version": version})
            if self._shutting_down:
                return  # entry torn down mid-update: do not re-arm a timer
            # Cancel any prior recovery handle (e.g. a repair's, if this update lands in
            # its window) BEFORE re-arming, so the old TimerHandle can't be orphaned.
            self._cancel("_recovery_cancel")
            self._recovery_cancel = async_call_later(
                self.hass, _RECOVERY_SECONDS, self._recovery_timeout
            )
        finally:
            self._repairing = False

    async def async_uninstall(self) -> None:
        """Remove the agent from the panel (explicit service — never on entry removal).

        Service-call context (like async_update_agent): on failure escalate AND
        re-raise as HomeAssistantError so the operator sees it, rather than letting a
        half-removed panel report success. Schedules no timer, so _shutting_down needs
        no special handling here.
        """
        async with self._ssh_lock:
            shell = self._shell()
            try:
                await shell.connect()
                await panel_ops.uninstall(shell)
            except (OSError, asyncssh.Error, PanelOpError) as err:
                self._escalate(f"agent uninstall failed: {err}")
                raise HomeAssistantError(f"agent uninstall failed: {err}") from err
            finally:
                await shell.close()
        persistent_notification.async_create(
            self.hass,
            f"Agent removed from panel `{self.panel}`. Delete the config entry to stop "
            "managing it; retained MQTT discovery topics are cleaned per "
            "docs/reference/deployment.md (Rollback).",
            title="Brilliant MQTT",
            notification_id=f"{EVENT_TYPE}_{self.panel}",
        )

    async def _recovery_timeout(self, _now: datetime) -> None:
        self._recovery_cancel = None
        if self._shutting_down:
            return
        if self.availability == AVAILABILITY_ONLINE:
            return
        journal = ""
        try:
            async with self._ssh_lock:
                shell = self._shell()
                await shell.connect()
                try:
                    journal = await panel_ops.collect_journal(shell, 50)
                finally:
                    await shell.close()
        except (OSError, asyncssh.Error, PanelOpError):
            _LOGGER.warning("%s: could not collect journal after failed repair", self.panel)
        self._fire(EVENT_REPAIR_FAILED, {"reason": "still_offline", "journal": journal})
        self._escalate(
            "repair ran but the bridge did not come back — probable bus-lib API drift "
            "after the firmware update; the agent needs a code fix"
        )

    async def _on_meta(self, msg: ReceiveMessage) -> None:
        if self._shutting_down:
            return  # defense-in-depth: don't spawn a staged-copy task on a dead entry
        try:
            meta = json.loads(str(msg.payload))
        except ValueError:
            _LOGGER.warning("%s: unparseable bridge meta payload: %r", self.panel, msg.payload)
            return
        if not isinstance(meta, dict):
            _LOGGER.warning("%s: bridge meta is not a JSON object: %r", self.panel, msg.payload)
            return
        self.meta = meta
        firmware = meta.get("panel_firmware")
        previous = self.entry.data.get(DATA_LAST_FIRMWARE)
        if firmware and firmware != previous:
            self.hass.config_entries.async_update_entry(
                self.entry, data={**self.entry.data, DATA_LAST_FIRMWARE: firmware}
            )
            if previous is not None:
                self._fire(
                    EVENT_PANEL_UPDATED,
                    {"old_firmware": previous, "new_firmware": firmware},
                )
                self.entry.async_create_background_task(
                    self.hass, self._refresh_staged_copies(), name=f"{self.panel}-staged"
                )
        self._notify()

    async def _refresh_staged_copies(self) -> None:
        """Post-OTA hygiene when the bridge survived: re-write /etc + staged copies."""
        try:
            async with self._ssh_lock:
                shell = self._shell()
                await shell.connect()
                try:
                    unit, env = await self._config_contents()
                    await panel_ops.ensure_configs(shell, unit, env)
                finally:
                    await shell.close()
        except (OSError, asyncssh.Error, PanelOpError):
            _LOGGER.warning("%s: staged-copy refresh failed; will retry next reconcile", self.panel)
