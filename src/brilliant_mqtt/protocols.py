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
from brilliant_mqtt.write_admission import AdmissionTicket, WriteClass, WriteResult


class CommandSubscribeError(RuntimeError):
    """A command-topic SUBSCRIBE failed at the MQTT adapter boundary."""


class BusClient(Protocol):
    """Adapter for the Brilliant panel's internal message bus."""

    async def start(self) -> None:
        """Connect to the bus and begin receiving updates."""
        ...

    async def get_all(self) -> list[BrilliantDevice]:
        """Return all peripherals already scoped to this panel.

        Wired primary loads must carry capture_provenance stamped when the
        adapter copies each native read result, before delivery to the bridge.

        Raises built-in ``TimeoutError`` or ``asyncio.TimeoutError`` when a
        scoped panel RPC misses its request deadline (the panel runs Python
        3.10, where those are distinct classes).
        """
        ...

    async def get_peripheral(self, device_id: str, peripheral_id: str) -> BrilliantDevice | None:
        """Return one peripheral via a scoped on-demand read, if present.

        Stamp wired primary capture_provenance when copying the read result.
        """
        ...

    def on_change(
        self,
        cb: Callable[[BrilliantDevice], Awaitable[None]],
        *,
        coalesce_pushes: bool = True,
        want_device: Callable[[str], bool] | None = None,
    ) -> None:
        """Register a callback invoked when any peripheral changes.

        Coalescing consumers receive the newest pending device snapshot;
        lossless consumers receive every pushed snapshot in arrival order.
        Wired primary pushes must carry capture_provenance stamped when the
        adapter copies the notification, before callback delivery.

        *want_device* is a live scope predicate keyed by the raw push's bus
        device id (``None`` = wants every device, the default). The adapter
        evaluates it BEFORE normalizing anything, so a push no registered
        consumer wants costs zero normalization/allocation (issue #98). It may
        over-admit (a false positive just wastes one normalization) but MUST
        never be narrower than this callback's own downstream include check, or
        real state would be silently dropped.
        """
        ...

    def on_reconnect(self, cb: Callable[[], Awaitable[None]]) -> None:
        """Add a callback invoked after the bus session reconnects.

        Pushes (and the data behind get_all) may have been lost while the
        connection was down — each callback should trigger its own reconcile.
        """
        ...

    def seconds_since_last_push(self) -> float | None:
        """Seconds since the last inbound push notification (None: none yet).

        Lets the run loop detect a silently dead notification stream and
        rebuild the session.
        """
        ...

    def recent_reconnects(self, window_s: float) -> int:
        """Number of bus reconnects within the last *window_s* seconds.

        Lets the run loop detect a reconnect STORM (the lib auto-reconnecting
        many times/sec when the panel bus server drops the peer under load) —
        invisible to seconds_since_last_push because each reconnect resets the
        push clock — and rebuild the session instead of amplifying the storm.
        """
        ...

    def consume_write_timeout(self) -> bool:
        """Return and clear whether an outbound bus write timed out."""
        ...

    def try_supersede(self, ticket: AdmissionTicket, new_payload: list[VarSet]) -> bool:
        """Replace an unissued latest-wins admission only with a covering payload."""
        ...

    async def set_variables(
        self,
        device_id: str,
        peripheral_id: str,
        sets: list[VarSet],
        *,
        write_class: WriteClass = WriteClass.INTERACTIVE_FIFO,
        ticket: AdmissionTicket | None = None,
    ) -> WriteResult:
        """Write one or more variable values to the given peripheral.

        *device_id* routes the write to the bus device that OWNS the
        peripheral: panel loads use the panel's own CONTROL device id; mesh
        loads (plug-in switches/dimmers) use the virtual "ble_mesh" device.

        Returns a SMALL normalized receipt describing the transport's
        acknowledgement, or the explicit non-error Superseded outcome. A ticket
        lets the command lane adopt the replacement without admitting it twice.
        For an issued wired write, call ticket.mark_issued with the current
        capture provenance after admission and the per-device lock, immediately
        before sending the native RPC. Omitting this disables the capture fence.
        """
        ...

    async def shutdown(self) -> None:
        """Disconnect from the bus cleanly."""
        ...


class MqttClient(Protocol):
    """Adapter for the central MQTT broker."""

    def consume_reader_failure(self) -> bool:
        """Return True exactly once if the inbound reader ended without this adapter
        cancelling it."""
        ...

    async def publish(self, topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        """Publish *payload* to *topic* with the requested retain flag and QoS."""
        ...

    def on_command(self, cb: Callable[[str, str], Awaitable[None]]) -> None:
        """Register a callback invoked for every inbound MQTT message."""
        ...

    def on_message(self, cb: Callable[[str, str, bool], Awaitable[None]]) -> None:
        """Register a callback receiving topic, payload, and MQTT retain context."""
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
