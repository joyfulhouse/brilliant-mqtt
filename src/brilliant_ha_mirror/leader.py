"""HA mirror leader election reusing the mesh election over a distinct topic."""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable

from brilliant_mqtt.mesh_leader import MeshLeader
from brilliant_mqtt.protocols import MqttClient

HA_MIRROR_LEADER_TOPIC = "brilliant/ha-mirror/leader"


class MirrorLeader(MeshLeader):
    """Reuse the mesh leader election protocol over the HA mirror's own topic."""

    def __init__(
        self,
        mqtt: MqttClient,
        panel: str,
        priority: int,
        heartbeat_seconds: float,
        on_acquire: Callable[[], Awaitable[None]],
        on_lose: Callable[[], Awaitable[None]],
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        super().__init__(
            mqtt,
            panel,
            priority,
            heartbeat_seconds,
            on_acquire,
            on_lose,
            clock,
            claim_topic=HA_MIRROR_LEADER_TOPIC,
        )
