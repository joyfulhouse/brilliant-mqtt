"""Environment-based configuration for the Brilliant MQTT bridge.

All settings are read from environment variables at startup. Required
variables raise KeyError when absent; optional variables fall back to
their dataclass defaults.

No panel imports, no MQTT library imports: pure stdlib only.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


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

    @classmethod
    def from_env(cls) -> Settings:
        """Construct Settings from environment variables.

        Required: BRILLIANT_PANEL, MQTT_HOST, MQTT_USERNAME, MQTT_PASSWORD.
        Optional: MQTT_PORT (default 1883), RESYNC_SECONDS (default 300),
                  LOG_LEVEL (default "INFO"), HOT_POLL_SECONDS (default 2.0),
                  BUS_STALE_SECONDS (default 900), MESH_PRIORITY (default 0:
                  never participate in mesh publishing),
                  MESH_HEARTBEAT_SECONDS (default 10.0).

        Raises KeyError when a required variable is absent.
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
        )
