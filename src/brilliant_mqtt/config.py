"""Environment-based configuration for the Brilliant MQTT bridge.

All settings are read from environment variables at startup. Required
variables raise KeyError when absent; optional variables fall back to
their dataclass defaults.

No panel imports, no MQTT library imports: pure stdlib only.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

# Recognized spellings for boolean env vars. Anything else raises ValueError at
# startup — an operator writing "false" must never silently get "enabled".
_ENV_TRUTHY = frozenset({"1", "true", "on", "yes"})
_ENV_FALSY = frozenset({"0", "false", "off", "no"})


def _env_bool(env: Mapping[str, str], key: str, default: str) -> bool:
    raw = env.get(key, default).strip().lower()
    if raw in _ENV_TRUTHY:
        return True
    if raw in _ENV_FALSY:
        return False
    raise ValueError(
        f"{key} must be one of {sorted(_ENV_TRUTHY)} / {sorted(_ENV_FALSY)}, got {raw!r}"
    )


@dataclass(frozen=True)
class Settings:
    """Immutable bridge configuration sourced from environment variables."""

    panel: str
    mqtt_host: str
    mqtt_username: str
    mqtt_password: str
    mqtt_port: int = 1883
    resync_seconds: int = 300
    log_level: str = "INFO"
    # Cadence of the scoped get-device poll that bounds state staleness even
    # when the bus push stream silently dies (0 disables the poll).
    hot_poll_seconds: float = 2.0
    # Rebuild the whole bus session when no push arrived for this long —
    # last-resort recovery for a half-dead stream (0 disables the watchdog).
    bus_stale_seconds: float = 900.0
    # Mesh (ble_mesh) publishing: every panel sees the same mesh devices, so
    # exactly one fleet-wide leader publishes them. Priority ranks panels in
    # the leader election (0 = this panel never participates); the heartbeat
    # is the leader's liveness cadence.
    mesh_priority: int = 0
    mesh_heartbeat_seconds: float = 10.0
    # Reconnect-storm circuit breaker: when the bus auto-reconnects this many
    # times within the window, the session is torn down and rebuilt (after the
    # supervisor backoff) instead of churning. This catches a failure mode the
    # stale watchdog cannot — a reconnect storm keeps resetting the push clock,
    # so the session never looks "stale" (incident 2026-06-13, adu-main). 0
    # disables the breaker.
    reconnect_storm_threshold: int = 20
    reconnect_storm_window_seconds: float = 60.0
    # Motion desired-state reconciliation: re-assert the operator's
    # last-commanded motion vars (RECONCILED_VARS — the enable flags plus
    # thresholds) when the firmware reverts them; runtime enables reset to
    # defaults within minutes while NVM thresholds persist. Falsy spellings
    # ("0"/"false"/"off"/"no") disable.
    motion_reconcile_enabled: bool = True
    # Per-(peripheral, var) floor between re-assert attempts. Also what rotates
    # the single write slot across peripherals over time (fairness) — keep > 0.
    motion_reconcile_min_interval_s: float = 60.0
    # Cap on reconciler bus writes per poll tick. NOTE: only effective when
    # MOTION_RECONCILE_MIN_WRITE_SPACING_S is 0 — with spacing > 0 the spacing
    # guard already limits every tick to ONE write, so spacing (not this cap)
    # is the lever that tunes the catch-up ramp. Must be >= 1.
    motion_reconcile_max_writes_per_tick: int = 4
    # Minimum seconds between consecutive reconciler writes across ticks —
    # bus-global (shared by both bridges), independent of the poll cadence.
    # 0 disables spacing (the per-tick cap then governs).
    motion_reconcile_min_write_spacing_s: float = 0.5
    # Directory for the per-bridge desired-state JSON files. Must live under
    # the OTA-persistent /var so desired state survives firmware updates.
    motion_desired_state_dir: str = "/var/brilliant-mqtt/state"

    @classmethod
    def from_env(cls) -> Settings:
        """Construct Settings from environment variables.

        Required: BRILLIANT_PANEL, MQTT_HOST, MQTT_USERNAME, MQTT_PASSWORD.
        Optional: MQTT_PORT (default 1883), RESYNC_SECONDS (default 300),
                  LOG_LEVEL (default "INFO"), HOT_POLL_SECONDS (default 2.0),
                  BUS_STALE_SECONDS (default 900), MESH_PRIORITY (default 0:
                  never participate in mesh publishing),
                  MESH_HEARTBEAT_SECONDS (default 10.0),
                  RECONNECT_STORM_THRESHOLD (default 20: reconnects within the
                  window that trip a session rebuild; 0 disables),
                  RECONNECT_STORM_WINDOW_SECONDS (default 60),
                  MOTION_RECONCILE_ENABLED (default "1"; "0"/"false"/"off"/"no"
                  disable, "1"/"true"/"on"/"yes" enable, anything else raises),
                  MOTION_RECONCILE_MIN_INTERVAL_S (default 60.0, must be >= 0),
                  MOTION_RECONCILE_MAX_WRITES_PER_TICK (default 4, must be >= 1;
                  only effective when the write spacing is 0),
                  MOTION_RECONCILE_MIN_WRITE_SPACING_S (default 0.5, must be
                  >= 0; 0 disables spacing — this is the catch-up ramp lever),
                  MOTION_DESIRED_STATE_DIR (default "/var/brilliant-mqtt/state").

        Raises KeyError when a required variable is absent and ValueError when
        a value fails validation — both crash startup loudly under systemd.
        """
        env = os.environ

        # Required — intentionally use direct __getitem__ so KeyError propagates.
        panel = env["BRILLIANT_PANEL"]
        mqtt_host = env["MQTT_HOST"]
        mqtt_username = env["MQTT_USERNAME"]
        mqtt_password = env["MQTT_PASSWORD"]

        # Optional with typed defaults.
        mqtt_port = int(env.get("MQTT_PORT", "1883"))
        resync_seconds = int(env.get("RESYNC_SECONDS", "300"))
        log_level = env.get("LOG_LEVEL", "INFO")
        hot_poll_seconds = float(env.get("HOT_POLL_SECONDS", "2.0"))
        bus_stale_seconds = float(env.get("BUS_STALE_SECONDS", "900"))
        mesh_priority = int(env.get("MESH_PRIORITY", "0"))
        mesh_heartbeat_seconds = float(env.get("MESH_HEARTBEAT_SECONDS", "10.0"))
        reconnect_storm_threshold = int(env.get("RECONNECT_STORM_THRESHOLD", "20"))
        reconnect_storm_window_seconds = float(env.get("RECONNECT_STORM_WINDOW_SECONDS", "60"))
        motion_reconcile_enabled = _env_bool(env, "MOTION_RECONCILE_ENABLED", "1")
        motion_reconcile_min_interval_s = float(env.get("MOTION_RECONCILE_MIN_INTERVAL_S", "60"))
        if motion_reconcile_min_interval_s < 0:
            raise ValueError("MOTION_RECONCILE_MIN_INTERVAL_S must be >= 0")
        motion_reconcile_max_writes_per_tick = int(
            env.get("MOTION_RECONCILE_MAX_WRITES_PER_TICK", "4")
        )
        if motion_reconcile_max_writes_per_tick < 1:
            # 0 would silently disable ALL enforcement while commands still
            # record; MOTION_RECONCILE_ENABLED is the one way to turn it off.
            raise ValueError("MOTION_RECONCILE_MAX_WRITES_PER_TICK must be >= 1")
        motion_reconcile_min_write_spacing_s = float(
            env.get("MOTION_RECONCILE_MIN_WRITE_SPACING_S", "0.5")
        )
        if motion_reconcile_min_write_spacing_s < 0:
            raise ValueError("MOTION_RECONCILE_MIN_WRITE_SPACING_S must be >= 0 (0 disables)")
        motion_desired_state_dir = env.get("MOTION_DESIRED_STATE_DIR", "/var/brilliant-mqtt/state")

        return cls(
            panel=panel,
            mqtt_host=mqtt_host,
            mqtt_username=mqtt_username,
            mqtt_password=mqtt_password,
            mqtt_port=mqtt_port,
            resync_seconds=resync_seconds,
            log_level=log_level,
            hot_poll_seconds=hot_poll_seconds,
            bus_stale_seconds=bus_stale_seconds,
            mesh_priority=mesh_priority,
            mesh_heartbeat_seconds=mesh_heartbeat_seconds,
            reconnect_storm_threshold=reconnect_storm_threshold,
            reconnect_storm_window_seconds=reconnect_storm_window_seconds,
            motion_reconcile_enabled=motion_reconcile_enabled,
            motion_reconcile_min_interval_s=motion_reconcile_min_interval_s,
            motion_reconcile_max_writes_per_tick=motion_reconcile_max_writes_per_tick,
            motion_reconcile_min_write_spacing_s=motion_reconcile_min_write_spacing_s,
            motion_desired_state_dir=motion_desired_state_dir,
        )
