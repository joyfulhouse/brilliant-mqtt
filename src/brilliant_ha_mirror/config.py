"""Environment-based configuration for the HA-to-Brilliant mirror.

All settings are read from an environment mapping at startup. Required
variables raise KeyError when absent; optional variables fall back to their
dataclass defaults. Pure stdlib only — no panel imports, no network libraries.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Settings:
    """Immutable mirror configuration sourced from environment variables."""

    panel: str
    ha_ws_url: str
    ha_token: str
    mirror_label: str = "brilliant"
    # Leader election across panels (all panels see the same HA): priority ranks
    # panels; the heartbeat is the leader's liveness cadence.
    leader_priority: int = 0
    leader_heartbeat_seconds: float = 10.0
    # HA area name -> Brilliant room id, parsed from a JSON object string.
    room_overrides: Mapping[str, str] = field(default_factory=dict)
    log_level: str = "INFO"

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Settings:
        """Construct Settings from an environment mapping.

        Required: PANEL, HA_WS_URL, HA_TOKEN.
        Optional: MIRROR_LABEL (default "brilliant"), LEADER_PRIORITY (int,
                  default 0), LEADER_HEARTBEAT_SECONDS (float, default 10.0),
                  ROOM_OVERRIDES (JSON object string, default {}), LOG_LEVEL
                  (default "INFO").

        Raises KeyError when a required variable is absent and ValueError when a
        value fails to parse — both crash startup loudly under systemd.
        """
        # Required — direct __getitem__ so KeyError propagates.
        panel = env["PANEL"]
        ha_ws_url = env["HA_WS_URL"]
        ha_token = env["HA_TOKEN"]

        mirror_label = env.get("MIRROR_LABEL", "brilliant")
        leader_priority = int(env.get("LEADER_PRIORITY", "0"))
        leader_heartbeat_seconds = float(env.get("LEADER_HEARTBEAT_SECONDS", "10.0"))
        room_overrides: Mapping[str, str] = dict(json.loads(env.get("ROOM_OVERRIDES", "{}")))
        log_level = env.get("LOG_LEVEL", "INFO")

        return cls(
            panel=panel,
            ha_ws_url=ha_ws_url,
            ha_token=ha_token,
            mirror_label=mirror_label,
            leader_priority=leader_priority,
            leader_heartbeat_seconds=leader_heartbeat_seconds,
            room_overrides=room_overrides,
            log_level=log_level,
        )
