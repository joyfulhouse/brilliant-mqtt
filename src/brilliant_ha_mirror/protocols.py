"""Protocol definitions for the Home Assistant mirror adapters."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import Protocol

from brilliant_ha_mirror.mapping import HaEntity, PeripheralSpec, ServiceCall


class HaClient(Protocol):
    """Adapter for Home Assistant entity state and services."""

    async def start(self) -> None:
        """Connect to Home Assistant and begin receiving state changes."""
        ...

    def is_running(self) -> bool:
        """True while the connection is alive and delivering state changes.

        The supervisor polls this to detect a silently dropped connection and
        rebuild the session instead of leaving a leader that no longer reflects
        Home Assistant state.
        """
        ...

    async def get_entities(self, label: str) -> list[HaEntity]:
        """Return the Home Assistant entities assigned to *label*."""
        ...

    def on_state_change(self, cb: Callable[[HaEntity], Awaitable[None]]) -> None:
        """Register a callback invoked when a mirrored entity changes."""
        ...

    async def call_service(self, call: ServiceCall) -> None:
        """Invoke the Home Assistant service described by *call*."""
        ...

    async def shutdown(self) -> None:
        """Disconnect from Home Assistant cleanly."""
        ...


class PeripheralHostClient(Protocol):
    """Adapter for peripherals hosted on a Brilliant panel."""

    async def start(self) -> None:
        """Connect to the panel and prepare to host peripherals."""
        ...

    async def register(
        self,
        name: str,
        spec: PeripheralSpec,
        on_command: Callable[[str, str], Awaitable[None]],
    ) -> None:
        """Host *name* and call *on_command* when the panel sets a command variable."""
        ...

    async def update_variables(self, name: str, values: Mapping[str, str]) -> None:
        """Update the hosted peripheral *name* with the supplied variable values."""
        ...

    async def get_rooms(self) -> Mapping[str, str]:
        """Return the Brilliant room catalog as opaque id to display name."""
        ...

    async def set_room_assignment(self, name: str, room_ids: list[str]) -> None:
        """Replace the hosted peripheral's Brilliant room assignment."""
        ...

    async def delete(self, name: str) -> None:
        """Delete the hosted peripheral identified by *name*."""
        ...

    async def shutdown(self) -> None:
        """Disconnect from the panel cleanly."""
        ...
