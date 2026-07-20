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
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import asyncssh
from homeassistant.components import mqtt, persistent_notification
from homeassistant.components.mqtt.models import ReceiveMessage
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import CALLBACK_TYPE, HomeAssistant, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.dispatcher import async_dispatcher_send
from homeassistant.helpers.event import async_call_later

from . import panel_ops
from .ble_scanner import BrilliantBleScannerBridge
from .const import (
    AVAILABILITY_OFFLINE,
    AVAILABILITY_ONLINE,
    COMPONENT_BUS_WATCHDOG,
    COMPONENT_HA_MIRROR,
    COMPONENT_HUE_CA,
    COMPONENT_VOICE,
    COMPONENT_WIFI_WATCHDOG,
    CONF_BLE_SCANNER_ENABLED,
    CONF_COMPONENTS,
    CONF_HA_CONTROL_ENABLED,
    CONF_HA_MIRROR_LEADER_PRIORITY,
    CONF_HA_MIRROR_TOKEN,
    CONF_HA_MIRROR_WS_URL,
    CONF_HOST,
    CONF_HUE_CA_CERT,
    CONF_MESH_PRIORITY,
    CONF_MQTT_HOST,
    CONF_MQTT_PASSWORD,
    CONF_MQTT_PORT,
    CONF_MQTT_USERNAME,
    CONF_PANEL,
    CONF_ROOT_PASSWORD,
    CONF_VOICE_HA_HOST,
    CONF_VOICE_WAKE_WORD,
    DATA_HA_MIRROR_RETIRE_VERIFIED,
    DATA_LAST_FIRMWARE,
    DATA_SSH_HOST_KEY,
    DEFAULT_AUTO_REPAIR,
    DEFAULT_BLE_SCANNER_ENABLED,
    DEFAULT_HA_CONTROL_ENABLED,
    DEFAULT_OFFLINE_GRACE_MINUTES,
    DEFAULT_REBOOT_JOURNAL_LINES,
    DEFAULT_REPAIR_COOLDOWN_MINUTES,
    DEFAULT_TRUST_HOST_KEY_CHANGES,
    DEFAULT_VOICE_WAKE_WORD,
    DIAGNOSTICS_RETENTION,
    DIAGNOSTICS_SUBDIR,
    DOMAIN,
    EVENT_AGENT_UPDATED,
    EVENT_HOST_KEY_REPINNED,
    EVENT_NEEDS_ATTENTION,
    EVENT_PANEL_UPDATED,
    EVENT_REPAIR_FAILED,
    EVENT_REPAIR_STARTED,
    EVENT_REPAIR_SUCCEEDED,
    EVENT_TYPE,
    OPT_AUTO_REPAIR,
    OPT_OFFLINE_GRACE_MINUTES,
    OPT_REPAIR_COOLDOWN_MINUTES,
    OPT_TRUST_HOST_KEY_CHANGES,
    SIGNAL_PANEL_STATE,
    VOICE_PAYLOAD_VERSION,
    availability_topic,
    meta_topic,
    panel_device_name,
)
from .panel_ops import PanelOpError
from .shell import AsyncsshShell, PanelShell
from .voice_payload import VoicePayloadError, async_fetch_voice_payload

_LOGGER = logging.getLogger(__name__)

_RECOVERY_SECONDS = 60.0
_UNREACHABLE_RECHECK_SECONDS = 300.0
_LEGACY_RETIRE_TIMEOUT_SECONDS = 30.0
_SHELL_CLOSE_TIMEOUT_SECONDS = 5.0


class _HostKeyChanged(Exception):
    """Pinned SSH host key no longer matches and auto-re-pin is disabled."""


def _payload_dir() -> Path:
    """The bundled agent payload (built by scripts/build_payload.sh / release CI)."""
    return Path(__file__).parent / "agent_payload"


def _write_diagnostics_bundle(directory: str, content: str) -> str:
    """Write *content* to a timestamped .log in *directory*, keeping only the newest N.

    Runs in the executor (blocking file I/O off the event loop). Names are UTC
    yyyymmdd-HHMMSS stamps so they sort chronologically; a same-second burst gets a
    `_N` suffix (which sorts AFTER the bare stamp) so a write never clobbers a sibling.
    Retention prunes by that filename sort, keeping the newest DIAGNOSTICS_RETENTION.
    """
    dir_path = Path(directory)
    dir_path.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    target = dir_path / f"{stamp}.log"
    collision = 1
    while target.exists():
        target = dir_path / f"{stamp}_{collision}.log"
        collision += 1
    target.write_text(content, encoding="utf-8")
    for stale in sorted(dir_path.glob("*.log"))[:-DIAGNOSTICS_RETENTION]:
        stale.unlink(missing_ok=True)
    return str(target)


class _PayloadState(Protocol):
    @property
    def payload_present(self) -> bool: ...


@dataclass(frozen=True)
class _WatchdogRelaySpec:
    service_filename: str
    payload_subdir: str
    inspect: Callable[[PanelShell], Awaitable[_PayloadState]]
    deploy: Callable[[PanelShell, str], Awaitable[None]]
    ensure_unit: Callable[[PanelShell, str], Awaitable[None]]
    enable: Callable[[PanelShell], Awaitable[None]]


_WIFI_WATCHDOG_RELAY = _WatchdogRelaySpec(
    service_filename="brilliant-wifi-watchdog.service",
    payload_subdir="wifi_watchdog",
    inspect=panel_ops.inspect_wifi_watchdog,
    deploy=panel_ops.deploy_wifi_watchdog,
    ensure_unit=panel_ops.ensure_wifi_watchdog_unit,
    enable=panel_ops.enable_wifi_watchdog,
)
_BUS_WATCHDOG_RELAY = _WatchdogRelaySpec(
    service_filename="brilliant-bus-watchdog.service",
    payload_subdir="bus_watchdog",
    inspect=panel_ops.inspect_bus_watchdog,
    deploy=panel_ops.deploy_bus_watchdog,
    ensure_unit=panel_ops.ensure_bus_watchdog_unit,
    enable=panel_ops.enable_bus_watchdog,
)


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
        self.legacy_mirror_problem: str | None = None
        self.ble_scanner_bridge: BrilliantBleScannerBridge | None = None
        self._unsubs: list[Any] = []
        self._grace_cancel: CALLBACK_TYPE | None = None
        self._recovery_cancel: CALLBACK_TYPE | None = None
        self._last_repair_mono: float | None = None
        self._repairing = False
        self._abandoned_close_tasks: set[asyncio.Task[None]] = set()
        # Set true by async_shutdown. A repair already awaiting inside the ssh_lock
        # resumes AFTER shutdown's one-shot cancel; this flag stops it re-arming a
        # timer that would then fire on a torn-down entry and SSH a removed panel.
        self._shutting_down = False

    @property
    def signal(self) -> str:
        """Dispatcher signal entities subscribe to for state refreshes."""
        return f"{SIGNAL_PANEL_STATE}_{self.entry.entry_id}"

    @property
    def _issue_id(self) -> str:
        """Stable issue-registry id for this panel's 'needs attention' repair issue."""
        return f"needs_attention_{self.entry.entry_id}"

    @property
    def _voice_issue_id(self) -> str:
        """Issue-registry id for 'voice enabled but satellite not running'."""
        return f"voice_missing_{self.entry.entry_id}"

    @property
    def _ha_mirror_issue_id(self) -> str:
        return f"ha_mirror_retired_{self.entry.entry_id}"

    def _shell(self) -> PanelShell:
        return AsyncsshShell(
            self.entry.data[CONF_HOST],
            self.entry.data[CONF_ROOT_PASSWORD],
            self.entry.data.get(DATA_SSH_HOST_KEY),
        )

    def _shell_unpinned(self) -> PanelShell:
        # Mirrors _shell() but with NO pinned key: this connect WILL offer the root
        # password to whatever host answers. Used only after an explicit opt-in to
        # re-pin a rotated host key (see _connect_for_repair).
        return AsyncsshShell(self.entry.data[CONF_HOST], self.entry.data[CONF_ROOT_PASSWORD], None)

    def _retain_abandoned_close(self, task: asyncio.Task[None]) -> None:
        """Strongly retain and eventually consume a cancellation-resistant close."""
        self._abandoned_close_tasks.add(task)

        def _consume(done: asyncio.Task[None]) -> None:
            self._abandoned_close_tasks.discard(done)
            try:
                done.result()
            except BaseException:
                pass

        task.add_done_callback(_consume)

    async def _async_close_shell(self, shell: PanelShell) -> bool:
        """Close once within a hard bound; never await a stuck task after cancel."""
        close_task = asyncio.create_task(shell.close(), name=f"{self.panel}-bounded-shell-close")
        try:
            done, _pending = await asyncio.wait({close_task}, timeout=_SHELL_CLOSE_TIMEOUT_SECONDS)
        except asyncio.CancelledError:
            close_task.cancel()
            self._retain_abandoned_close(close_task)
            raise
        if close_task not in done:
            close_task.cancel()
            self._retain_abandoned_close(close_task)
            return False
        try:
            close_task.result()
        except BaseException:
            return False
        return True

    async def _async_close_shell_or_raise(self, shell: PanelShell) -> None:
        """Fail closed when a management session cannot be proven closed."""
        if not await self._async_close_shell(shell):
            raise OSError("panel SSH session could not be closed") from None

    async def _connect_for_repair(self) -> PanelShell:
        """Connect for a management op, honoring the host-key trust policy.

        Pinned connect (verify-before-auth) normally. On a rotated host key
        (HostKeyNotVerifiable):
          - auto-re-pin OFF (default): raise _HostKeyChanged — the password is NEVER
            offered to the new-key host.
          - auto-re-pin ON: one fresh UNPINNED connect (which DOES offer the password to
            the host presenting the new key — the opt-in tradeoff), persist the new key
            to the entry, fire EVENT_HOST_KEY_REPINNED, return the connected shell.
        Any other connect failure propagates unchanged to the caller's handler.
        """
        shell = self._shell()
        try:
            await shell.connect()
        except asyncssh.HostKeyNotVerifiable:
            # Caught BEFORE any generic asyncssh.Error (this is the only except here,
            # so every other connect failure propagates to the caller's handler).
            await self._async_close_shell_or_raise(shell)
            if not self._opt(OPT_TRUST_HOST_KEY_CHANGES, DEFAULT_TRUST_HOST_KEY_CHANGES):
                raise _HostKeyChanged from None
            repinned = self._shell_unpinned()
            try:
                await repinned.connect()  # unpinned: captures the new key (offers the password)
            except asyncio.CancelledError:
                await self._async_close_shell(repinned)
                raise
            except BaseException:
                await self._async_close_shell_or_raise(repinned)
                raise
            new_key = repinned.pinned_host_key()
            if new_key is None:
                await self._async_close_shell_or_raise(repinned)
                # A fresh failure (the unpinned connect succeeded but exposed no key),
                # not caused by the host-key mismatch — chain to None, and it reaches
                # the caller's (OSError, asyncssh.Error) handler as "unreachable".
                raise OSError("no host key captured on re-pin") from None
            self.hass.config_entries.async_update_entry(
                self.entry, data={**self.entry.data, DATA_SSH_HOST_KEY: new_key}
            )
            self._fire(EVENT_HOST_KEY_REPINNED, {"new_host_key": new_key})
            return repinned
        except asyncio.CancelledError:
            await self._async_close_shell(shell)
            raise
        except BaseException:
            await self._async_close_shell_or_raise(shell)
            raise
        return shell

    async def async_setup(self) -> None:
        self._unsubs.append(
            await mqtt.async_subscribe(
                self.hass, availability_topic(self.panel), self._on_availability
            )
        )
        self._unsubs.append(
            await mqtt.async_subscribe(self.hass, meta_topic(self.panel), self._on_meta)
        )
        if self.entry.data.get(CONF_BLE_SCANNER_ENABLED, DEFAULT_BLE_SCANNER_ENABLED) is True:
            panel_device = dr.async_get(self.hass).async_get_or_create(
                config_entry_id=self.entry.entry_id,
                identifiers={
                    (DOMAIN, self.panel),
                    ("mqtt", f"brilliant_panel_{self.panel}"),
                },
                name=panel_device_name(self.panel),
                manufacturer="Brilliant",
            )
            bridge = BrilliantBleScannerBridge(
                self.hass,
                self.entry,
                device_id=panel_device.id,
            )
            self.ble_scanner_bridge = bridge
            await bridge.async_setup()
            self.entry.async_on_unload(bridge.async_shutdown)
        if self._legacy_retirement_evidence(include_verified_history=True):
            await self.async_retire_legacy_ha_mirror(force_history_audit=True)

    async def async_shutdown(self) -> None:
        # Latch shutdown FIRST: a repair wedged in the ssh_lock checks this flag
        # before (re-)arming a timer, so it cannot schedule one past this cancel.
        self._shutting_down = True
        # Cancel any in-flight timers BEFORE dropping the subscriptions so an entry
        # unload (or reload) never leaves a grace/recovery callback dangling.
        self._cancel("_grace_cancel")
        self._cancel("_recovery_cancel")
        if self.ble_scanner_bridge is not None:
            self.ble_scanner_bridge.async_shutdown()
            self.ble_scanner_bridge = None
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
        if not problem:
            # The panel recovered (or the problem otherwise cleared): drop any open
            # "needs attention" repair issue so it doesn't linger after recovery.
            ir.async_delete_issue(self.hass, DOMAIN, self._issue_id)
        self._notify()

    def _legacy_retirement_evidence(self, *, include_verified_history: bool = False) -> bool:
        components = self.entry.data.get(CONF_COMPONENTS, {})
        component_evidence = (
            isinstance(components, Mapping) and components.get(COMPONENT_HA_MIRROR) is True
        )
        config_evidence = any(
            key in self.entry.data
            for key in (
                CONF_HA_MIRROR_WS_URL,
                CONF_HA_MIRROR_TOKEN,
                CONF_HA_MIRROR_LEADER_PRIORITY,
            )
        )
        return (
            component_evidence
            or config_evidence
            or (
                include_verified_history
                and self.entry.data.get(DATA_HA_MIRROR_RETIRE_VERIFIED) is True
            )
        )

    def _create_ha_mirror_retirement_issue(self) -> None:
        ir.async_create_issue(
            self.hass,
            DOMAIN,
            self._ha_mirror_issue_id,
            is_fixable=False,
            severity=ir.IssueSeverity.WARNING,
            translation_key="ha_mirror_retired",
            translation_placeholders={
                "panel": self.panel,
                "reason": "Physical-Control hosting was disabled for responsiveness safety",
            },
            learn_more_url="https://github.com/joyfulhouse/brilliant-mqtt/blob/main/docs/ha-integration.md",
        )

    @staticmethod
    def _ha_mirror_absent(state: panel_ops.HaMirrorState) -> bool:
        return not any(
            (
                state.unit_present,
                state.env_present,
                state.enabled,
                state.active,
                state.staged_env_present,
                state.payload_present,
            )
        )

    def _persist_verified_ha_mirror_retirement(self) -> None:
        data = dict(self.entry.data)
        components = dict(data.get(CONF_COMPONENTS) or {})
        components[COMPONENT_HA_MIRROR] = False
        data[CONF_COMPONENTS] = components
        data[DATA_HA_MIRROR_RETIRE_VERIFIED] = True
        if data.get(CONF_HA_CONTROL_ENABLED, DEFAULT_HA_CONTROL_ENABLED) is True:
            for key in (
                CONF_HA_MIRROR_WS_URL,
                CONF_HA_MIRROR_TOKEN,
                CONF_HA_MIRROR_LEADER_PRIORITY,
            ):
                data.pop(key, None)
        self.hass.config_entries.async_update_entry(self.entry, data=data)

    def _finalize_verified_ha_mirror_retirement(self) -> None:
        """Persist proof only after the owning shell has closed successfully."""
        self._persist_verified_ha_mirror_retirement()
        self.legacy_mirror_problem = None
        ir.async_delete_issue(self.hass, DOMAIN, self._ha_mirror_issue_id)

    def _complete_ha_mirror_retirement_after_close(
        self, result: bool | None, close_ok: bool
    ) -> None:
        """Finalize verified absence only after the transport has really closed."""
        if result is not True:
            return
        if close_ok:
            self._finalize_verified_ha_mirror_retirement()
            return
        self.legacy_mirror_problem = "Legacy HA mirror retirement could not be verified"
        self._create_ha_mirror_retirement_issue()

    async def _async_retire_legacy_ha_mirror_on_shell(self, shell: PanelShell) -> bool | None:
        """Uninstall and prove absence; the shell owner finalizes after close."""
        initial = await panel_ops.inspect_ha_mirror(shell)
        evidence = self._legacy_retirement_evidence() or not self._ha_mirror_absent(initial)
        if not evidence:
            return None
        self._create_ha_mirror_retirement_issue()
        await panel_ops.uninstall_ha_mirror(shell)
        verified = await panel_ops.inspect_ha_mirror(shell)
        if not self._ha_mirror_absent(verified):
            self.legacy_mirror_problem = "Legacy HA mirror retirement could not be verified"
            return False
        return True

    async def async_retire_legacy_ha_mirror(self, *, force_history_audit: bool = False) -> bool:
        """Best-effort bounded retirement; panel outages never block entry setup."""
        if not self._legacy_retirement_evidence(include_verified_history=force_history_audit):
            return True

        self._create_ha_mirror_retirement_issue()
        shell: PanelShell | None = None
        result: bool | None = False
        failure = False
        cancellation: asyncio.CancelledError | None = None
        try:
            async with asyncio.timeout(_LEGACY_RETIRE_TIMEOUT_SECONDS):
                async with self._ssh_lock:
                    try:
                        if self._shutting_down:
                            return False
                        shell = await self._connect_for_repair()
                        result = await self._async_retire_legacy_ha_mirror_on_shell(shell)
                    except asyncio.CancelledError as error:
                        cancellation = error
                    except (_HostKeyChanged, OSError, asyncssh.Error, PanelOpError):
                        failure = True
                    close_ok = True
                    if shell is not None:
                        close_ok = await self._async_close_shell(shell)
                    if cancellation is not None:
                        self.legacy_mirror_problem = "Legacy HA mirror retirement was interrupted"
                        raise cancellation
                    if failure or not close_ok:
                        self.legacy_mirror_problem = (
                            "Legacy HA mirror retirement could not be verified"
                        )
                        _LOGGER.warning(
                            "%s: legacy HA mirror retirement could not be verified",
                            self.panel,
                        )
                        return False
                    if result is True or (force_history_audit and result is None):
                        self._finalize_verified_ha_mirror_retirement()
                    return result is not False
        except asyncio.CancelledError:
            self.legacy_mirror_problem = "Legacy HA mirror retirement was interrupted"
            raise
        except (TimeoutError, _HostKeyChanged, OSError, asyncssh.Error, PanelOpError):
            self.legacy_mirror_problem = "Legacy HA mirror retirement could not be verified"
            _LOGGER.warning("%s: legacy HA mirror retirement could not be verified", self.panel)
            return False

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
            _LOGGER.info(
                "%s: bridge is unavailable (LWT offline); starting %d-minute grace period",
                self.panel,
                grace_s // 60,
            )
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
        # Surface as a repair issue (was a persistent_notification). _set_problem(True)
        # below records the same problem/reason the binary_sensor reads; _set_problem(
        # False) on recovery deletes this issue.
        ir.async_create_issue(
            self.hass,
            DOMAIN,
            self._issue_id,
            is_fixable=False,
            severity=ir.IssueSeverity.ERROR,
            translation_key="needs_attention",
            translation_placeholders={"panel": self.panel, "reason": reason},
            learn_more_url="https://github.com/joyfulhouse/brilliant-mqtt/blob/main/docs/ha-integration.md",
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
            scene_bridge_enabled=data.get(CONF_HA_CONTROL_ENABLED, DEFAULT_HA_CONTROL_ENABLED)
            is True,
        )
        return unit, env

    async def _payload_version(self) -> str:
        """The bundled agent payload's version string (read off-thread; blocking IO)."""
        return (
            await self.hass.async_add_executor_job((_payload_dir() / "VERSION").read_text)
        ).strip()

    def _voice_env(self, wake_word: str | None = None) -> str:
        """Render the voice env. *wake_word* overrides the persisted value so a push can
        use a NOT-YET-persisted word (async_set_voice_wake_word persists only on success).
        """
        data = self.entry.data
        return panel_ops.render_voice_env(
            panel=self.panel,
            name=panel_device_name(self.panel),
            api_port=6053,  # LVA ESPHome API; not exposed per-panel this phase
            wake_word=wake_word
            if wake_word is not None
            else data.get(CONF_VOICE_WAKE_WORD, DEFAULT_VOICE_WAKE_WORD),
            ha_host=data.get(CONF_VOICE_HA_HOST, ""),
            enable_aec=False,  # AEC ships OFF (barge-in tuning is a follow-up)
        )

    async def _deploy_voice(self, shell: PanelShell, tarball: str) -> None:
        """Install/enable the voice satellite on a connected shell (idempotent).

        Deploys the payload only when absent (an OTA wipes /etc but not /var, so the
        common case just restores the unit/env from the surviving payload).
        """
        vstate = await panel_ops.inspect_voice(shell)
        if not vstate.payload_present:
            await panel_ops.deploy_voice_payload(shell, tarball, VOICE_PAYLOAD_VERSION)
        await panel_ops.ensure_voice_config(shell, self._voice_env())
        await panel_ops.enable_voice(shell)

    async def _relay_watchdog(
        self,
        shell: PanelShell,
        spec: _WatchdogRelaySpec,
    ) -> Exception | None:
        """Restore one selected watchdog without blocking the bridge operation."""
        try:
            payload_dir = _payload_dir()
            unit = await self.hass.async_add_executor_job(
                (payload_dir / spec.service_filename).read_text
            )
            state = await spec.inspect(shell)
            if not state.payload_present:
                await spec.deploy(shell, str(payload_dir / spec.payload_subdir))
            await spec.ensure_unit(shell, unit)
            await spec.enable(shell)
        except (OSError, asyncssh.Error, PanelOpError) as err:
            return err
        return None

    async def _relay_hue_ca(self, shell: PanelShell) -> Exception | None:
        """Restore the selected hue-ca recovery hook without blocking the bridge op.

        Bespoke sibling of _relay_watchdog: the hook needs TWO unit files (a oneshot
        .service the .timer activates) plus the operator's CA PEM, so it can't reuse
        _WatchdogRelaySpec verbatim. An OTA wipes /etc/systemd/system/ (dropping the
        units) AND /data (the pinned Hue CA bundle the hook re-appends) — but the code
        + the CA PEM this hook already wrote to PANEL_HUE_CA_CERT_FILE both live under
        /var and normally survive. So the common case is a pure re-lay + re-enable;
        deploy_hue_ca (which re-writes the code AND the CA) only runs when the /var
        payload itself was also lost. Mirrors deploy_hue_ca's own contract (CA is
        written only after a successful code swap) by never redeploying with an empty
        CA — matching _hue_ca_install's guard — and skips the whole relay (not just
        the redeploy) when the code is gone and no CA is configured, since enabling
        the timer then would just fail every run.
        """
        try:
            payload_dir = _payload_dir()
            service = await self.hass.async_add_executor_job(
                (payload_dir / "brilliant-hue-ca.service").read_text
            )
            timer = await self.hass.async_add_executor_job(
                (payload_dir / "brilliant-hue-ca.timer").read_text
            )
            state = await panel_ops.inspect_hue_ca(shell)
            if not state.payload_present:
                ca_pem = str(self.entry.data.get(CONF_HUE_CA_CERT, "")).strip()
                if not ca_pem:
                    _LOGGER.warning(
                        "%s: hue-ca code is missing and no CA certificate is "
                        "configured; skipping the hue-ca relay",
                        self.panel,
                    )
                    return None
                await panel_ops.deploy_hue_ca(shell, str(payload_dir / "hue_ca"), ca_pem)
            await panel_ops.ensure_hue_ca_units(shell, service, timer)
            await panel_ops.enable_hue_ca(shell)
        except (OSError, asyncssh.Error, PanelOpError) as err:
            return err
        return None

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
            # Pre-fetch the voice tarball BEFORE taking the SSH lock (the download is slow
            # and lock-free), but INSIDE this try so its finally always resets _repairing.
            # async_fetch_voice_payload raises ONLY VoicePayloadError, but keep this defensive:
            # any escape here would otherwise wedge _repairing=True forever (it is called from
            # timers/the Repair button).
            voice_tarball: str | None = None
            if self.entry.data.get(CONF_COMPONENTS, {}).get(COMPONENT_VOICE, False):
                try:
                    voice_tarball = await async_fetch_voice_payload(self.hass)
                except VoicePayloadError as fetch_err:
                    _LOGGER.warning(
                        "%s: could not fetch voice payload for repair: %s",
                        self.panel,
                        fetch_err,
                    )
            async with self._ssh_lock:
                try:
                    shell = await self._connect_for_repair()
                except _HostKeyChanged:
                    # A rotated host key with auto-re-pin OFF. Distinct from "unreachable":
                    # the password was NOT offered to the new-key host, and a recheck would
                    # just hit the same mismatch — so escalate for operator action and
                    # arm NO timer. _escalate already sets problem.
                    self._fire(EVENT_REPAIR_FAILED, {"reason": "host_key_changed"})
                    self._escalate(
                        "panel SSH host key changed (likely a firmware reflash) — open "
                        "Reconfigure to re-pin, or enable 'Trust host-key changes' in "
                        "options for hands-off repair"
                    )
                    self._last_repair_mono = time.monotonic()
                    return  # needs operator action; a recheck would just hit the same mismatch
                except (OSError, asyncssh.Error, PanelOpError) as connect_err:
                    self._fire(EVENT_REPAIR_FAILED, {"reason": "unreachable"})
                    self._set_problem(True, f"panel unreachable: {connect_err}")
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
                    retirement_result: bool | None = None
                    state = await panel_ops.inspect_panel(shell)
                    unit, env = await self._config_contents()
                    # Bootstrap a code-less panel (never installed, or its /var code was
                    # lost): lay the agent payload down BEFORE enabling the unit, so the
                    # Repair button / auto-repair can install from scratch rather than
                    # enable a unit whose ExecStart points at code that isn't there. An
                    # already-installed panel (the common OTA-wiped-/etc case) keeps the
                    # light path — rewrite config + enable, no re-upload.
                    if not state.payload_present:
                        await panel_ops.deploy_payload(
                            shell, str(_payload_dir()), await self._payload_version()
                        )
                    await panel_ops.ensure_configs(shell, unit, env)
                    await panel_ops.enable_now(shell)
                    if voice_tarball is not None:
                        try:
                            await self._deploy_voice(shell, voice_tarball)
                        except (OSError, asyncssh.Error, PanelOpError) as voice_err:
                            _LOGGER.warning("%s: voice repair failed: %s", self.panel, voice_err)
                            ir.async_create_issue(
                                self.hass,
                                DOMAIN,
                                self._voice_issue_id,
                                is_fixable=False,
                                severity=ir.IssueSeverity.WARNING,
                                translation_key="voice_missing",
                                translation_placeholders={"panel": self.panel},
                                learn_more_url="https://github.com/joyfulhouse/brilliant-mqtt/blob/main/docs/ha-integration.md",
                            )
                        else:
                            ir.async_delete_issue(self.hass, DOMAIN, self._voice_issue_id)
                    # Wi-Fi watchdog re-lay: re-write unit to /etc if selected.
                    # OTA wipes /etc/systemd/system/ so the unit disappears after a
                    # firmware update even though the code survives in /var.  Lay it
                    # back down (and redeploy the code if /var was also wiped) so the
                    # watchdog keeps running across OTAs.  Failure is logged and
                    # swallowed — a watchdog outage must not block the bridge repair.
                    from .components import selected_ids  # lazy: components imports manager

                    selected = selected_ids(self.entry.data)
                    if COMPONENT_WIFI_WATCHDOG in selected:
                        if err := await self._relay_watchdog(shell, _WIFI_WATCHDOG_RELAY):
                            _LOGGER.warning("%s: watchdog repair failed: %s", self.panel, err)
                    # Bus watchdog re-lay: re-write unit to /etc if selected.
                    # OTA wipes /etc/systemd/system/ so the unit disappears after a
                    # firmware update even though the code survives in /var.  Lay it
                    # back down (and redeploy the code if /var was also wiped) so the
                    # watchdog keeps running across OTAs.  Failure is logged and
                    # swallowed — a watchdog outage must not block the bridge repair.
                    if COMPONENT_BUS_WATCHDOG in selected:
                        if err := await self._relay_watchdog(shell, _BUS_WATCHDOG_RELAY):
                            _LOGGER.warning("%s: bus watchdog repair failed: %s", self.panel, err)
                    # Hue CA recovery hook re-lay: re-write its units to /etc if selected.
                    # OTA wipes /etc/systemd/system/ (and /data — what the hook itself
                    # recovers), so the timer disappears after a firmware update even
                    # though the code + previously-written CA survive in /var. Lay it
                    # back down (and redeploy code+CA if /var was also wiped) so the
                    # hook keeps re-appending the CA across OTAs. Failure is logged and
                    # swallowed — a hue-ca outage must not block the bridge repair.
                    if COMPONENT_HUE_CA in selected:
                        if err := await self._relay_hue_ca(shell):
                            _LOGGER.warning("%s: hue-ca repair failed: %s", self.panel, err)
                    try:
                        retirement_result = await self._async_retire_legacy_ha_mirror_on_shell(
                            shell
                        )
                    except (OSError, asyncssh.Error, PanelOpError):
                        self.legacy_mirror_problem = (
                            "Legacy HA mirror retirement could not be verified"
                        )
                except (OSError, asyncssh.Error, PanelOpError) as repair_err:
                    # A checked step (mkdir/daemon-reload/systemctl) exited non-zero.
                    # The panel is half-broken; surface it loudly instead of letting
                    # the exception escape (silent + entry shows GREEN) or falling
                    # through to the success path (no recovery timer would ever fire).
                    self._fire(
                        EVENT_REPAIR_FAILED,
                        {"reason": "repair_step_failed", "error": str(repair_err)},
                    )
                    self._escalate(f"repair step failed: {repair_err}")
                    self._last_repair_mono = time.monotonic()  # gate any retry
                    return
                finally:
                    close_ok = await self._async_close_shell(shell)
                self._complete_ha_mirror_retirement_after_close(retirement_result, close_ok)
            self._last_repair_mono = time.monotonic()
            if self._shutting_down:
                return  # entry torn down mid-repair: do not re-arm a timer
            self._recovery_cancel = async_call_later(
                self.hass, _RECOVERY_SECONDS, self._recovery_timeout
            )
        finally:
            self._repairing = False

    async def async_update_agent(self, progress: Callable[[int], None] | None = None) -> None:
        """Push the bundled agent payload, refresh configs, restart, verify via LWT.

        *progress*, when given, is called with a 0-100 percentage at each deploy stage
        so the update entity can render a real progress bar (the service path passes
        None).

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
            raise HomeAssistantError(
                translation_domain=DOMAIN, translation_key="already_in_progress"
            )
        self._repairing = True
        # Cancel any pending grace timer (as async_repair does) so an outage that armed
        # it can't fire a spurious needs_attention during this update's recovery window.
        self._cancel("_grace_cancel")

        def _p(pct: int) -> None:
            if progress is not None:
                progress(pct)

        try:
            _p(10)
            version = await self._payload_version()
            async with self._ssh_lock:
                try:
                    shell = await self._connect_for_repair()
                except _HostKeyChanged as err:
                    # A rotated host key with auto-re-pin OFF: never offer the password to
                    # the new-key host. Service-call context → escalate AND raise so HA
                    # reports the install as failed (not a false success).
                    self._escalate(
                        "panel SSH host key changed — Reconfigure to re-pin, or enable "
                        "'Trust host-key changes' in options"
                    )
                    raise HomeAssistantError(
                        translation_domain=DOMAIN, translation_key="host_key_changed"
                    ) from err
                except (OSError, asyncssh.Error) as err:
                    self._escalate(f"agent update failed: {err}")
                    raise HomeAssistantError(
                        translation_domain=DOMAIN,
                        translation_key="update_failed",
                        translation_placeholders={"error": str(err)},
                    ) from err
                _p(25)
                try:
                    retirement_result: bool | None = None
                    _p(40)
                    await panel_ops.deploy_payload(shell, str(_payload_dir()), version)
                    _p(80)
                    unit, env = await self._config_contents()
                    await panel_ops.ensure_configs(shell, unit, env)
                    _p(90)
                    await panel_ops.restart(shell)
                    try:
                        retirement_result = await self._async_retire_legacy_ha_mirror_on_shell(
                            shell
                        )
                    except (OSError, asyncssh.Error, PanelOpError):
                        self.legacy_mirror_problem = (
                            "Legacy HA mirror retirement could not be verified"
                        )
                    _p(95)
                except (OSError, asyncssh.Error, PanelOpError) as err:
                    self._escalate(f"agent update failed: {err}")
                    raise HomeAssistantError(
                        translation_domain=DOMAIN,
                        translation_key="update_failed",
                        translation_placeholders={"error": str(err)},
                    ) from err
                finally:
                    close_ok = await self._async_close_shell(shell)
                self._complete_ha_mirror_retirement_after_close(retirement_result, close_ok)
            _p(100)
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
            try:
                shell = await self._connect_for_repair()
            except _HostKeyChanged as err:
                # A rotated host key with auto-re-pin OFF: never offer the password to
                # the new-key host. Service-call context → escalate AND raise so HA
                # reports the uninstall as failed (not a false success).
                self._escalate(
                    "panel SSH host key changed — Reconfigure to re-pin, or enable "
                    "'Trust host-key changes' in options"
                )
                raise HomeAssistantError(
                    translation_domain=DOMAIN, translation_key="host_key_changed"
                ) from err
            except (OSError, asyncssh.Error) as err:
                self._escalate(f"agent uninstall failed: {err}")
                raise HomeAssistantError(
                    translation_domain=DOMAIN,
                    translation_key="uninstall_failed",
                    translation_placeholders={"error": str(err)},
                ) from err
            try:
                await panel_ops.uninstall(shell)
            except (OSError, asyncssh.Error, PanelOpError) as err:
                self._escalate(f"agent uninstall failed: {err}")
                raise HomeAssistantError(
                    translation_domain=DOMAIN,
                    translation_key="uninstall_failed",
                    translation_placeholders={"error": str(err)},
                ) from err
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

    async def async_reboot(
        self,
        *,
        collect_diagnostics: bool = True,
        journal_lines: int = DEFAULT_REBOOT_JOURNAL_LINES,
    ) -> None:
        """Capture a pre-reboot diagnostics bundle (if asked), then reboot the panel.

        The panel's journald is volatile (/run tmpfs — only the current boot survives a
        reboot), so the wedge evidence MUST be pulled over SSH BEFORE the reboot that
        would erase it: capture always precedes the reboot command. Capture is
        best-effort — a capture/persist failure is logged but never blocks the reboot the
        operator asked for (the wedge must still clear). The reboot is issued LAST and its
        inevitable mid-command SSH disconnect is treated as success (``panel_ops.reboot``).

        Service-call/button context (like ``async_uninstall``): a connect failure raises
        HomeAssistantError so the caller — the operator's scheduled 4 AM automation — sees
        it. A connect failure does NOT escalate: a nightly reboot hitting a briefly-offline
        panel should not raise a persistent repair issue, and the availability state
        machine already owns real outages.
        """
        async with self._ssh_lock:
            try:
                shell = await self._connect_for_repair()
            except _HostKeyChanged as err:
                raise HomeAssistantError(
                    translation_domain=DOMAIN, translation_key="host_key_changed"
                ) from err
            except (OSError, asyncssh.Error) as err:
                raise HomeAssistantError(
                    translation_domain=DOMAIN,
                    translation_key="reboot_failed",
                    translation_placeholders={"error": str(err)},
                ) from err
            diagnostics_path: str | None = None
            try:
                if collect_diagnostics:
                    try:
                        bundle = await panel_ops.collect_diagnostics(shell, journal_lines)
                        diagnostics_path = await self._persist_diagnostics(bundle)
                    except (OSError, asyncssh.Error) as diag_err:
                        # Best-effort: a diagnostics failure must never block the reboot
                        # (collect_diagnostics is itself per-probe tolerant, so this is
                        # almost always the executor file write, e.g. a full disk).
                        _LOGGER.warning(
                            "%s: pre-reboot diagnostics capture failed: %s",
                            self.panel,
                            diag_err,
                        )
                # Reboot LAST: the connection drops as the panel goes down; panel_ops.reboot
                # treats that disconnect (or a timeout with no exit status) as success.
                await panel_ops.reboot(shell)
            finally:
                await self._async_close_shell(shell)
        _LOGGER.info(
            "%s: reboot issued (diagnostics: %s)",
            self.panel,
            diagnostics_path or ("skipped" if not collect_diagnostics else "capture failed"),
        )

    async def _persist_diagnostics(self, bundle: str) -> str:
        """Write *bundle* under <config>/brilliant_mqtt/diagnostics/<panel>/, return its path.

        The blocking file write + retention prune run in the executor (never on the event
        loop). The panel slug is filesystem-safe (lowercase alnum + hyphens), so it is used
        directly as the per-panel subdirectory.
        """
        directory = self.hass.config.path(DOMAIN, DIAGNOSTICS_SUBDIR, self.panel)
        return await self.hass.async_add_executor_job(_write_diagnostics_bundle, directory, bundle)

    @asynccontextmanager
    async def _voice_ssh_session(self) -> AsyncIterator[PanelShell]:
        """One voice SSH op: fleet lock → connect → yield → always close.

        Maps failures to the SAME HomeAssistantError keys both voice methods used —
        connect host-key rotation → host_key_changed; connect or op (OSError/asyncssh/
        PanelOpError) → voice_failed — so callers just `async with` and run their ops.
        """
        async with self._ssh_lock:
            try:
                shell = await self._connect_for_repair()
            except _HostKeyChanged as err:
                raise HomeAssistantError(
                    translation_domain=DOMAIN, translation_key="host_key_changed"
                ) from err
            except (OSError, asyncssh.Error) as err:
                raise HomeAssistantError(
                    translation_domain=DOMAIN,
                    translation_key="voice_failed",
                    translation_placeholders={"error": str(err)},
                ) from err
            try:
                yield shell
            except (OSError, asyncssh.Error, PanelOpError) as err:
                raise HomeAssistantError(
                    translation_domain=DOMAIN,
                    translation_key="voice_failed",
                    translation_placeholders={"error": str(err)},
                ) from err
            finally:
                await shell.close()

    async def _set_component_flag(self, component_id: str, enabled: bool) -> None:
        """Persist a component's selected state into entry data."""
        components = dict(self.entry.data.get(CONF_COMPONENTS, {}))
        components[component_id] = enabled
        self.hass.config_entries.async_update_entry(
            self.entry, data={**self.entry.data, CONF_COMPONENTS: components}
        )

    async def async_install_component(self, component_id: str) -> None:
        """SSH-install a component, then record it as selected.

        ``components.py`` imports ``manager`` at module top level, so REGISTRY is
        imported lazily inside this method to avoid a circular import.
        """
        from .components import REGISTRY  # lazy: components imports manager

        component = REGISTRY[component_id]
        if component.deprecated:
            raise PanelOpError(f"{component.label} is deprecated and cannot be installed")
        async with self._ssh_lock:
            shell = await self._connect_for_repair()
            try:
                await component.install(self.hass, shell, self.entry.data)
            finally:
                await shell.close()
        await self._set_component_flag(component_id, True)
        self._notify()

    async def async_remove_component(self, component_id: str) -> None:
        """SSH-remove a component, then clear its selection.

        ``components.py`` imports ``manager`` at module top level, so REGISTRY is
        imported lazily inside this method to avoid a circular import.
        """
        from .components import REGISTRY  # lazy: components imports manager

        component = REGISTRY[component_id]
        if component.deprecated:
            if not await self.async_retire_legacy_ha_mirror(force_history_audit=True):
                raise PanelOpError("Legacy HA mirror retirement could not be verified")
            self._notify()
            return
        async with self._ssh_lock:
            shell = await self._connect_for_repair()
            try:
                await component.remove(shell)
            finally:
                await shell.close()
        await self._set_component_flag(component_id, False)
        self._notify()

    async def async_set_voice_enabled(self, enabled: bool) -> None:
        """Enable (deploy+start) or disable (uninstall) the voice satellite on the panel.

        Delegates the SSH operation to the generic component methods (which update
        ``CONF_COMPONENTS`` and fire a notify), then clears the
        ``voice_missing_<entry_id>`` repair issue on success.

        Errors from the SSH layer or the voice-payload fetch are mapped to the same
        ``HomeAssistantError`` keys the voice switch has always expected, so the switch
        still surfaces failures correctly.
        """
        try:
            if enabled:
                await self.async_install_component(COMPONENT_VOICE)
            else:
                await self.async_remove_component(COMPONENT_VOICE)
        except _HostKeyChanged as err:
            raise HomeAssistantError(
                translation_domain=DOMAIN, translation_key="host_key_changed"
            ) from err
        except (VoicePayloadError, OSError, asyncssh.Error, PanelOpError) as err:
            raise HomeAssistantError(
                translation_domain=DOMAIN,
                translation_key="voice_failed",
                translation_placeholders={"error": str(err)},
            ) from err
        # Clear any stale "voice enabled but not running" issue: after a disable there is
        # nothing to run, and after a successful enable the satellite IS running — either
        # way the issue must not linger.
        ir.async_delete_issue(self.hass, DOMAIN, self._voice_issue_id)

    async def async_set_voice_wake_word(self, wake_word: str) -> None:
        """Push the wake word + restart the satellite if enabled, THEN persist it.

        Persisting only AFTER a successful push keeps the select and the running panel in
        agreement: a failed push raises before the entry is updated, so the select still
        shows the OLD word the panel is actually using. When voice is disabled there is no
        panel to push to, so just persist.
        """
        if self.entry.data.get(CONF_COMPONENTS, {}).get(COMPONENT_VOICE, False):
            async with self._voice_ssh_session() as shell:
                await panel_ops.ensure_voice_config(shell, self._voice_env(wake_word=wake_word))
                await panel_ops.restart_voice(shell)
        self.hass.config_entries.async_update_entry(
            self.entry, data={**self.entry.data, CONF_VOICE_WAKE_WORD: wake_word}
        )
        self._notify()

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
                    retirement_result: bool | None = None
                    unit, env = await self._config_contents()
                    await panel_ops.ensure_configs(shell, unit, env)
                    # Wi-Fi watchdog: also re-lay its unit when selected — OTA wipes /etc
                    # and the watchdog unit disappears even though the code in /var survives.
                    from .components import selected_ids  # lazy: components imports manager

                    selected = selected_ids(self.entry.data)
                    if COMPONENT_WIFI_WATCHDOG in selected:
                        if await self._relay_watchdog(shell, _WIFI_WATCHDOG_RELAY):
                            _LOGGER.warning(
                                "%s: watchdog refresh failed; will retry next reconcile",
                                self.panel,
                            )
                    # Bus watchdog: also re-lay its unit when selected — OTA wipes /etc
                    # and the watchdog unit disappears even though the code in /var survives.
                    if COMPONENT_BUS_WATCHDOG in selected:
                        if await self._relay_watchdog(shell, _BUS_WATCHDOG_RELAY):
                            _LOGGER.warning(
                                "%s: bus watchdog refresh failed; will retry next reconcile",
                                self.panel,
                            )
                    # Hue CA recovery hook: also re-lay its units when selected — OTA
                    # wipes /etc and the timer disappears even though the code +
                    # previously-written CA in /var survive.
                    if COMPONENT_HUE_CA in selected:
                        if await self._relay_hue_ca(shell):
                            _LOGGER.warning(
                                "%s: hue-ca refresh failed; will retry next reconcile",
                                self.panel,
                            )
                    try:
                        retirement_result = await self._async_retire_legacy_ha_mirror_on_shell(
                            shell
                        )
                    except (OSError, asyncssh.Error, PanelOpError):
                        self.legacy_mirror_problem = (
                            "Legacy HA mirror retirement could not be verified"
                        )
                finally:
                    close_ok = await self._async_close_shell(shell)
                self._complete_ha_mirror_retirement_after_close(retirement_result, close_ok)
        except (OSError, asyncssh.Error, PanelOpError):
            _LOGGER.warning("%s: staged-copy refresh failed; will retry next reconcile", self.panel)
