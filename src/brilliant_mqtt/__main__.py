"""Entrypoint: wire the real adapters and run the supervised bridge loop.

``python -m brilliant_mqtt`` on the panel. The process is also under systemd
``Restart=always`` — the in-process supervisor loop here is belt-and-braces so a
transient bus/broker drop does not require a full process restart.
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path

from brilliant_mqtt.bridge import Bridge
from brilliant_mqtt.bus import RpcBusAdapter
from brilliant_mqtt.config import Settings
from brilliant_mqtt.desired_state import DesiredState
from brilliant_mqtt.mesh_leader import MeshLeader
from brilliant_mqtt.model import BrilliantDevice
from brilliant_mqtt.mqttio import AioMqttAdapter
from brilliant_mqtt.protocols import BusClient

log = logging.getLogger(__name__)

# Backoff before reconnecting after a failed/ended session.
_BACKOFF_S = 5
# Loop tick when the hot poll is disabled (stale checks still need a cadence).
_IDLE_TICK_S = 30.0

# The virtual bus device carrying whole-home mesh loads. Published under the
# reserved "mesh" pseudo-panel by the elected leader, never the panel bridge.
_MESH_DEVICE_ID = "ble_mesh"


class BusStaleError(RuntimeError):
    """No bus push for longer than BUS_STALE_SECONDS — session presumed dead."""


class BusReconnectStormError(RuntimeError):
    """Bus reconnected past the rate threshold — session torn down to break the
    self-reinforcing storm (incident 2026-06-13, adu-main)."""


def _is_reconnect_storm(bus: BusClient, settings: Settings) -> bool:
    """True when bus reconnects in the window meet/exceed the threshold.

    A reconnect storm — the lib auto-reconnecting many times/sec when the panel
    bus server drops the peer under load — is invisible to the stale watchdog
    because every reconnect also resets the push clock. It is detected by RATE
    instead, and the proven recovery is a full session rebuild after backoff
    (what a manual ``systemctl restart`` did on adu-main). Threshold <= 0
    disables the breaker.
    """
    if settings.reconnect_storm_threshold <= 0:
        return False
    count = bus.recent_reconnects(settings.reconnect_storm_window_seconds)
    return count >= settings.reconnect_storm_threshold


def _make_desired(settings: Settings, name: str) -> DesiredState | None:
    """A loaded DesiredState for one bridge scope, or None when disabled."""
    if not settings.motion_reconcile_enabled:
        return None
    ds = DesiredState(Path(settings.motion_desired_state_dir) / f"{name}.json")
    ds.load()
    return ds


def _is_panel_device(device: BrilliantDevice) -> bool:
    """Panel-bridge scope: everything EXCEPT the virtual mesh device.

    The mesh device belongs to the elected fleet-wide leader under the "mesh"
    pseudo-panel; letting it leak into the panel namespace would duplicate
    every mesh entity on every participating panel.
    """
    return device.device_id != _MESH_DEVICE_ID


async def _run_session(settings: Settings) -> None:
    """Run ONE bridge session: construct the adapters, serve forever, tear down.

    A module-level function (not a body inlined in :func:`run`'s while-loop)
    so the callbacks defined here close over stable function locals instead of
    loop variables (ruff B023). It owns the session's adapters end to end: the
    ``finally`` teardown runs on any exit, including cancellation.
    """
    participating = settings.mesh_priority >= 1
    mqtt = AioMqttAdapter(settings)
    bus = RpcBusAdapter(extra_device_ids=(_MESH_DEVICE_ID,) if participating else ())
    try:
        # Bridges register their bus/mqtt callbacks in __init__, BEFORE any I/O
        # starts — so no early change/command event is missed.
        panel_bridge = Bridge(
            bus,
            mqtt,
            settings.panel,
            include=_is_panel_device,
            desired=_make_desired(settings, f"{settings.panel}-faceplate"),
            reconcile_min_interval_s=settings.motion_reconcile_min_interval_s,
            reconcile_max_writes_per_tick=settings.motion_reconcile_max_writes_per_tick,
            reconcile_min_write_spacing_s=settings.motion_reconcile_min_write_spacing_s,
        )

        if participating:

            def _mesh_in_scope(device: BrilliantDevice) -> bool:
                # Leadership gates pushes AND polls: a non-leader (or fresh
                # ex-leader, whose _on_change stays registered after
                # withdraw()) must publish nothing on the mesh namespace.
                # `leader` is late-bound on purpose — callbacks first fire
                # after bus.start(), by which time it is assigned below.
                return device.device_id == _MESH_DEVICE_ID and leader.is_leader

            mesh_bridge = Bridge(
                bus,
                mqtt,
                "mesh",
                include=_mesh_in_scope,
                desired=_make_desired(settings, "mesh"),
                reconcile_min_interval_s=settings.motion_reconcile_min_interval_s,
                reconcile_max_writes_per_tick=settings.motion_reconcile_max_writes_per_tick,
                reconcile_min_write_spacing_s=settings.motion_reconcile_min_write_spacing_s,
            )
            leader = MeshLeader(
                mqtt,
                settings.panel,
                settings.mesh_priority,
                settings.mesh_heartbeat_seconds,
                on_acquire=mesh_bridge.reconcile,
                on_lose=mesh_bridge.withdraw,
            )

        async def _on_bus_reconnect() -> None:
            # After a bus reconnect, pushes (and the observer's get_all mirror)
            # may have missed changes — re-reconcile to republish the truth.
            await panel_bridge.reconcile()
            if participating and leader.is_leader:
                await mesh_bridge.reconcile()

        bus.on_reconnect(_on_bus_reconnect)

        await mqtt.connect()
        if participating:
            # Join the election before bus data flows; the FIRST mesh
            # reconcile is acquisition's job (on_acquire), not startup's.
            await leader.start()
        await bus.start()
        await panel_bridge.reconcile()

        tick = settings.hot_poll_seconds if settings.hot_poll_seconds > 0 else _IDLE_TICK_S
        next_resync = time.monotonic() + settings.resync_seconds
        while True:
            await asyncio.sleep(tick)

            # Stale-stream watchdog: a silently dead notification stream
            # freezes pushes AND get_all (pilot finding 2026-06-12) — only
            # a full session rebuild restores a trustworthy view.
            if settings.bus_stale_seconds > 0:
                age = bus.seconds_since_last_push()
                if age is not None and age > settings.bus_stale_seconds:
                    raise BusStaleError(
                        f"no bus push for {age:.0f}s (threshold {settings.bus_stale_seconds:.0f}s)"
                    )

            # Reconnect-storm breaker: a saturated panel bus server drops our
            # peer repeatedly; the lib's aggressive auto-reconnect + our
            # re-reconcile feed the loop (incident 2026-06-13). Tear the session
            # down so the supervisor backoff gives the bus server a breather.
            if _is_reconnect_storm(bus, settings):
                raise BusReconnectStormError(
                    f"bus reconnected >={settings.reconnect_storm_threshold} times in "
                    f"{settings.reconnect_storm_window_seconds:.0f}s — rebuilding session"
                )

            if participating:
                await leader.tick()

            # Hot poll: bounds state staleness at the poll cadence; the
            # bridge's diff cache keeps unchanged payloads off MQTT.
            if settings.hot_poll_seconds > 0:
                await panel_bridge.poll_once()
                if participating and leader.is_leader:
                    await mesh_bridge.poll_once()

            # Periodic level-triggered resync: republishes retained discovery
            # + state, covering any push notifications that were missed.
            if time.monotonic() >= next_resync:
                await panel_bridge.reconcile()
                if participating and leader.is_leader:
                    await mesh_bridge.reconcile()
                next_resync = time.monotonic() + settings.resync_seconds
    finally:
        # Best-effort teardown; both adapters tolerate a never-fully-started state.
        try:
            await bus.shutdown()
        except Exception:
            log.exception("bus shutdown failed during cleanup")
        try:
            await mqtt.disconnect()
        except Exception:
            log.exception("mqtt disconnect failed during cleanup")


async def run(settings: Settings) -> None:
    """Supervise the bridge forever: (re)connect, reconcile, periodically resync."""
    while True:
        try:
            await _run_session(settings)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("bridge session failed; will reconnect after backoff")
        await asyncio.sleep(_BACKOFF_S)


def main() -> None:
    """Read settings from the environment, configure logging, and run."""
    settings = Settings.from_env()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(run(settings))


if __name__ == "__main__":
    main()
