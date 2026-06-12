"""Protocol definitions for the Brilliant MQTT bridge adapters.

These are the typing seam between the bridge orchestrator and the real
adapters (bus client, MQTT client). The bridge and all tests import only
these Protocols — never the concrete adapter implementations.

No panel imports, no MQTT library imports: pure stdlib + project types.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Protocol

from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.model import BrilliantDevice


class BusClient(Protocol):
    """Adapter for the Brilliant panel's internal message bus."""

    async def start(self) -> None:
        """Connect to the bus and begin receiving updates."""
        ...

    async def get_all(self) -> list[BrilliantDevice]:
        """Return all peripherals already scoped to this panel."""
        ...

    def on_change(self, cb: Callable[[BrilliantDevice], Awaitable[None]]) -> None:
        """Register a callback invoked when any peripheral changes."""
        ...

    def on_reconnect(self, cb: Callable[[], Awaitable[None]]) -> None:
        """Register a callback invoked after the bus session reconnects.

        Pushes (and the data behind get_all) may have been lost while the
        connection was down — the callback should trigger a full reconcile.
        """
        ...

    def seconds_since_last_push(self) -> float | None:
        """Seconds since the last inbound push notification (None: none yet).

        Lets the run loop detect a silently dead notification stream and
        rebuild the session.
        """
        ...

    async def set_variables(self, device_id: str, peripheral_id: str, sets: list[VarSet]) -> None:
        """Write one or more variable values to the given peripheral.

        *device_id* routes the write to the bus device that OWNS the
        peripheral: panel loads use the panel's own CONTROL device id; mesh
        loads (plug-in switches/dimmers) use the virtual "ble_mesh" device.
        """
        ...

    async def shutdown(self) -> None:
        """Disconnect from the bus cleanly."""
        ...


class MqttClient(Protocol):
    """Adapter for the central MQTT broker."""

    async def publish(self, topic: str, payload: str, retain: bool = False) -> None:
        """Publish *payload* to *topic*, optionally with the retain flag."""
        ...

    def on_command(self, cb: Callable[[str, str], Awaitable[None]]) -> None:
        """Register a callback invoked for every inbound MQTT message."""
        ...

    async def subscribe(self, topic: str) -> None:
        """Subscribe to *topic* so that inbound messages reach on_command."""
        ...

    async def unsubscribe(self, topic: str) -> None:
        """Stop receiving messages for *topic*.

        Needed by the mesh leader's step-down: a panel that loses the mesh
        leader election must stop consuming the mesh command topics.
        """
        ...
