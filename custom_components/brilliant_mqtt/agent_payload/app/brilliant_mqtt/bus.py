"""Real Brilliant message-bus adapter (RPCObserver → BusClient Protocol).

This is the ONLY module permitted to import the panel's closed-source Cython
libraries (``lib.message_bus_api``, ``lib.protocol``, ``thrift_types``). Those
imports are DEFERRED — performed inside methods, never at module level — so that
``import brilliant_mqtt.bus`` succeeds on any machine without the panel libs and
the full unit suite runs off-panel. ``normalize_peripheral`` below is pure (no
panel imports) and is the only part of this module that is unit-tested.

Connection / call shapes follow ``docs/reference/poc-findings.md`` (verified
live on the pilot panel): §2 connection recipe, §3 signatures, §4 schema, §5 scoping,
§7 command call, §8 notifications.
"""

from __future__ import annotations

import asyncio
import logging
import math
import secrets
import time
from collections.abc import Awaitable, Callable
from typing import Any

from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.model import BrilliantDevice, Variable, kind_for_peripheral_type

logger = logging.getLogger(__name__)

# Path to the panel's message-bus unix socket (poc-findings §2).
_SOCKET_PATH = "/var/run/brilliant/server_socket"
# How long to wait for the processor handshake before giving up (poc-findings §2:
# connected in <1 s in practice; 10 s is a generous ceiling).
_CONNECT_TIMEOUT_S = 10.0
_CONNECT_POLL_S = 0.25


def _session_client_name(base: str) -> str:
    """Return a per-session bus client name: *base* plus a short random suffix.

    The bus registers our peer under ``<owning_device_id>.<name>``. With a
    constant name that key is fully deterministic, so a registration left
    half-bound by a connect that timed out mid-handshake becomes a permanent
    ghost: every later attempt (the lib's own retries, the supervisor's session
    rebuilds, even a fresh process after a reboot) reuses the identical name and
    is rejected by the server with ``NameInUseError``, locking the bridge out of
    the bus until the panel's ``message_bus`` is restarted — and it re-forms on
    the next load-induced timeout. A fresh suffix per session means a stale
    ghost can never block a new session; the bridge self-recovers on its normal
    reconnect (adu-bath incident, 2026-07-05).
    """
    return f"{base}-{secrets.token_hex(4)}"


def normalize_peripheral(device_id: str, peripheral_id: str, raw: Any) -> BrilliantDevice:
    """Translate a raw bus Peripheral into a normalized :class:`BrilliantDevice`.

    PURE function (no panel imports) so it is unit-testable off-panel. ``raw`` is
    duck-typed: it must expose ``name`` (str), ``peripheral_type`` (int), and
    ``variables`` (mapping name → object with ``.value`` and
    ``.externally_settable``).

    Mapping rules (poc-findings §4/§6):
    - ``kind`` from :func:`kind_for_peripheral_type`.
    - ``name`` from the ``display_name`` variable's value when present and
      non-empty, else the raw peripheral ``name``, else ``peripheral_id``.
    - ``variables`` is a dict of
      ``Variable(name, str(value), bool(settable), timestamp_ms)``,
      skipping any entry whose value is ``None`` (complex/absent blob values —
      poc-findings §4 notes those are base64 thrift blobs to ignore); ``bytes``
      values are utf-8-decoded (errors="replace"), never ``str()``-repr'd.
    """
    peripheral_type = int(raw.peripheral_type)
    kind = kind_for_peripheral_type(peripheral_type)

    variables: dict[str, Variable] = {}
    for var_name, raw_var in dict(raw.variables).items():
        value = raw_var.value
        if value is None:
            # Skip None-valued entries (complex blobs / absent) — §4.
            continue
        if isinstance(value, (bytes, bytearray)):
            # Decode to text instead of str()-ing the repr ("b'Lights'");
            # errors="replace" so a bad byte can never raise here.
            value = bytes(value).decode("utf-8", errors="replace")
        raw_timestamp = getattr(raw_var, "timestamp", None)
        if isinstance(raw_timestamp, bool):
            timestamp_ms = None
        elif isinstance(raw_timestamp, int):
            timestamp_ms = raw_timestamp
        elif isinstance(raw_timestamp, float) and math.isfinite(raw_timestamp):
            timestamp_ms = int(raw_timestamp)
        else:
            timestamp_ms = None
        variables[var_name] = Variable(
            name=var_name,
            value=str(value),
            externally_settable=bool(raw_var.externally_settable),
            timestamp_ms=timestamp_ms,
        )

    name = _resolve_name(variables, raw, peripheral_id)

    return BrilliantDevice(
        device_id=device_id,
        peripheral_id=peripheral_id,
        name=name,
        kind=kind,
        peripheral_type=peripheral_type,
        variables=variables,
    )


def _resolve_name(variables: dict[str, Variable], raw: Any, peripheral_id: str) -> str:
    """Pick the human entity name: display_name → raw name → peripheral_id."""
    display = variables.get("display_name")
    if display is not None and display.value:
        return display.value
    raw_name = getattr(raw, "name", None)
    if raw_name:
        return str(raw_name)
    return peripheral_id


def _make_observer_class(base: Any) -> Any:
    """Build the ``_BridgeObserver`` subclass against the lazily-imported base.

    Defined as a factory because the real ``RPCObserver`` base class is only
    importable on-panel and only inside :meth:`RpcBusAdapter.start`. ``base`` is
    that class at runtime (typed ``Any`` — closed-source Cython, no stubs; the
    pyproject ``[[tool.mypy.overrides]]`` block relaxes ``disallow_subclassing_any``
    for this one module). The methods below match the override surface
    (poc-findings §3).
    """

    class _BridgeObserver(base):
        """RPCObserver that forwards bridged devices' updates to a dispatch fn."""

        def __init__(
            self,
            loop: asyncio.AbstractEventLoop,
            dispatch: Callable[[Any], None],
            mark_push: Callable[[], None],
        ) -> None:
            super().__init__(loop)
            self._loop = loop
            self._dispatch = dispatch
            self._mark_push = mark_push
            self._device_ids: frozenset[str] = frozenset()

        def bind_device_ids(self, device_ids: frozenset[str]) -> None:
            """Restrict dispatch to *device_ids* (empty set = no filtering).

            The set is {own CONTROL device} ∪ configured extras (e.g.
            "ble_mesh") — the bus pushes the WHOLE home graph, and everything
            outside the bridged devices is noise.
            """
            self._device_ids = device_ids

        async def handle_notification(self, notification: Any) -> None:
            """Push handler (poc-findings §8). Must NEVER raise — the bus loop
            would otherwise crash. MUST be a coroutine: the lib's inbound
            dispatcher (``thrift_inspect.handle_method``) awaits the handler's
            return value — a sync override produces ``await None`` TypeErrors on
            every push (pilot finding, 2026-06-12). Runs on the observer's own
            loop, so dispatch can schedule tasks directly.
            """
            try:
                # Liveness first, before any filtering: ANY inbound push proves
                # the notification stream is alive (stale-stream watchdog).
                self._mark_push()
                updated = getattr(notification, "updated_device", None)
                if updated is None:
                    return
                if self._device_ids and updated.id not in self._device_ids:
                    # Not a device we bridge (own panel or configured extra).
                    return
                self._dispatch(updated)
            except Exception:
                # Broad by design: a raise here would kill the bus receive loop.
                logger.exception("handle_notification failed; ignoring notification")

    return _BridgeObserver


class RpcBusAdapter:
    """Concrete :class:`~brilliant_mqtt.protocols.BusClient` over ``RPCObserver``.

    All panel-library access is deferred to :meth:`start`. Methods that need a
    live connection raise :class:`RuntimeError` when called before ``start()``.
    """

    def __init__(
        self,
        my_name: str = "brilliant_mqtt",
        extra_device_ids: tuple[str, ...] = (),
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        # A UNIQUE name per session (see _session_client_name): the bus peer key
        # is <owning_device_id>.<my_name>, so a constant name lets a half-bound
        # ghost registration lock the bridge out forever with NameInUseError.
        self._my_name = _session_client_name(my_name)
        # Injectable monotonic clock (tests drive it deterministically); backs
        # both the push-liveness clock and the reconnect-rate window.
        self._clock = clock
        # Extra bus device ids to subscribe/fetch beyond the panel's own
        # CONTROL device — e.g. the virtual "ble_mesh" device carrying the
        # home's plug-in mesh switches/dimmers (Milestone 11).
        self._extra_device_ids = extra_device_ids
        # Populated by start(); typed Any because the panel libs have no stubs.
        self._obs: Any = None
        self._proc: Any = None
        self._own_device_id: str | None = None
        # Multiple consumers (panel bridge + mesh publisher) may each register
        # a change callback; every change fans out to all of them.
        self._change_cbs: list[Callable[[BrilliantDevice], Awaitable[None]]] = []
        self._reconnect_cbs: list[Callable[[], Awaitable[None]]] = []
        # Re-issues this session's subscription; bound as a closure in start()
        # so the reconnect path never needs panel imports of its own.
        self._resubscribe: Callable[[], Awaitable[None]] | None = None
        # monotonic timestamp of the last inbound push (None: none yet).
        self._last_push: float | None = None
        # Monotonic timestamps of recent processor reconnects (newest last);
        # recent_reconnects() prunes anything outside the queried window so the
        # list stays bounded. Feeds the run loop's reconnect-storm breaker.
        self._reconnect_times: list[float] = []
        # Retain fired callback tasks so they are not garbage-collected mid-flight
        # (asyncio holds only weak references to tasks). Done-callback discards.
        self._pending_tasks: set[asyncio.Task[None]] = set()

    async def start(self) -> None:
        """Connect to the bus following the poc-findings §2 recipe."""
        # Deferred imports — see the module docstring. Never hoist these.
        import lib.protocol.message_bus_peer_service as mbps
        from lib.message_bus_api.observer_interface import RPCObserver
        from lib.protocol.processor import SinglePeerProcessor
        from thrift_types.message_bus.ttypes import SubscriptionRequest

        loop = asyncio.get_running_loop()

        observer_cls = _make_observer_class(RPCObserver)
        obs = observer_cls(loop, self._dispatch_raw_device, self._note_push)
        proc = SinglePeerProcessor(
            socket_path=_SOCKET_PATH,
            my_name=self._my_name,
            handler=mbps.PeripheralServer(obs),
            client_class=mbps.MessageBusClient,
            loop=loop,
        )
        await proc.start()

        # Poll until the handshake completes (poc-findings §2). Fail fast on timeout.
        waited = 0.0
        while not proc.is_connected():
            if waited >= _CONNECT_TIMEOUT_S:
                raise TimeoutError(f"message bus did not connect within {_CONNECT_TIMEOUT_S:.0f}s")
            await asyncio.sleep(_CONNECT_POLL_S)
            waited += _CONNECT_POLL_S

        # Observer must start AFTER the processor is connected (poc-findings §2:
        # otherwise the observer's first client call hits a NoneType client).
        await obs.start(proc, None)

        own_device_id = obs.get_owning_device_id()
        obs.bind_device_ids(frozenset({own_device_id, *self._extra_device_ids}))

        async def resubscribe() -> None:
            # Re-issue EVERY subscription (own + extras): the closure runs at
            # connect time AND after each processor reconnect, where the bus
            # forgets all of this session's subscriptions.
            await obs.subscribe(SubscriptionRequest(device_id=own_device_id))
            for extra in self._extra_device_ids:
                await obs.subscribe(SubscriptionRequest(device_id=extra))

        await resubscribe()
        self._resubscribe = resubscribe

        # The pilot showed the notification stream can die and recover with the
        # underlying connection (2026-06-12: pushes silently lost for minutes,
        # the observer's get_all mirror frozen, then both self-healed). Hook the
        # processor's reconnect signal so the bridge can re-reconcile the gap.
        proc.add_reconnect_callback(self._on_proc_reconnect)

        # Start the stale-stream clock at connect time so a quiet-but-healthy
        # session reads as "old push", not "no push ever".
        self._note_push()

        # Only assign instance state once everything succeeded.
        self._proc = proc
        self._obs = obs
        self._own_device_id = own_device_id
        logger.info("bus connected; owning device id=%s", own_device_id)

    def _require_started(self) -> tuple[Any, str]:
        """Return ``(observer, owning_device_id)`` or raise if not started.

        Returning the id as a guaranteed ``str`` (not ``str | None``) lets the
        callers pass it to :func:`normalize_peripheral` without a cast.
        """
        if self._obs is None or self._own_device_id is None:
            raise RuntimeError("RpcBusAdapter.start() must be called before use")
        return self._obs, self._own_device_id

    def _dispatch_raw_device(self, raw_device: Any) -> None:
        """Normalize each peripheral of a changed device and fire ALL callbacks.

        Pushing every peripheral on a device update (rather than diffing) is
        acceptable: the bridge republishes retained state idempotently, and the
        periodic resync also covers any gaps (poc-findings §8 / design §5).

        The device id comes from the RAW device itself (the bus device the
        peripherals actually live on — "ble_mesh" for mesh pushes, the own
        32-hex id otherwise), so each normalized BrilliantDevice carries its
        true owner and writes can be routed back. A missing/falsy raw id falls
        back to our own device id (the pre-M11 behaviour).
        """
        cbs = list(self._change_cbs)
        if not cbs:
            return
        raw_id = getattr(raw_device, "id", None)
        device_id = str(raw_id) if raw_id else self._own_device_id
        if device_id is None:
            return
        # Same defensive access as get_all(): a peripheral-less housekeeping
        # notification is routine, not worth a logger.exception from the
        # handler's broad catch.
        peripherals = getattr(raw_device, "peripherals", None)
        if not peripherals:
            return
        for peripheral_id, raw_peripheral in dict(peripherals).items():
            device = normalize_peripheral(device_id, peripheral_id, raw_peripheral)
            for cb in cbs:
                self._spawn(cb(device))

    def _spawn(self, coro: Awaitable[None]) -> None:
        """Schedule *coro* on the running loop, retaining a strong reference."""
        task = asyncio.ensure_future(coro)
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)

    def _note_push(self) -> None:
        """Record that an inbound push arrived (stale-stream watchdog clock)."""
        self._last_push = self._clock()

    def seconds_since_last_push(self) -> float | None:
        """Seconds since the last inbound push; None before the first one."""
        if self._last_push is None:
            return None
        return self._clock() - self._last_push

    def _note_reconnect(self) -> None:
        """Record that the processor reconnected (reconnect-rate clock)."""
        self._reconnect_times.append(self._clock())

    def recent_reconnects(self, window_s: float) -> int:
        """Count processor reconnects within the last *window_s* seconds.

        Prunes older timestamps as a side effect so the buffer stays bounded
        even through a sustained storm (the run loop queries every tick).
        """
        cutoff = self._clock() - window_s
        self._reconnect_times = [t for t in self._reconnect_times if t >= cutoff]
        return len(self._reconnect_times)

    def on_reconnect(self, cb: Callable[[], Awaitable[None]]) -> None:
        """Add a callback fired after the bus session reconnects."""
        self._reconnect_cbs.append(cb)

    def _on_proc_reconnect(self, *args: Any, **kwargs: Any) -> None:
        """Processor reconnect signal (sync, lib-invoked) → async fan-out.

        Accepts any args defensively: the closed lib does not document the
        callback signature. A reconnect proves the stream is alive again, so
        the stale clock resets here — otherwise the watchdog could tear down a
        session that just recovered.
        """
        logger.warning("bus processor reconnected; re-subscribing and re-reconciling")
        self._note_push()
        self._note_reconnect()
        self._spawn(self._after_reconnect())

    async def _after_reconnect(self) -> None:
        """Re-subscribe (belt-and-braces) then notify the bridge to reconcile."""
        if self._resubscribe is not None:
            try:
                await self._resubscribe()
            except Exception:
                logger.exception("re-subscribe after reconnect failed")
        for cb in list(self._reconnect_cbs):
            try:
                await cb()
            except Exception:
                logger.exception("reconnect callback failed")

    async def get_all(self) -> list[BrilliantDevice]:
        """Return the normalized peripherals of every bridged bus device.

        Fetches the panel's own CONTROL device plus each configured extra
        (e.g. "ble_mesh"), each via the SCOPED ``obs.get_device(device_id)``
        rather than ``obs.get_all()`` (whole home graph, poc-findings §5) —
        same data at a fraction of the work, which matters now that the hot
        poll calls this every couple of seconds. Each peripheral is normalized
        with the device id it actually lives on so writes can be routed back.

        A device that comes back missing/peripheral-less is warned about and
        skipped — an absent extra (e.g. a home with no mesh devices) must
        never fail the whole snapshot.
        """
        obs, own_id = self._require_started()
        devices: list[BrilliantDevice] = []
        for device_id in (own_id, *self._extra_device_ids):
            raw_device = await obs.get_device(device_id)
            if raw_device is None or getattr(raw_device, "peripherals", None) is None:
                label = "own device" if device_id == own_id else "extra device"
                logger.warning("%s id=%s not returned by get_device()", label, device_id)
                continue
            devices.extend(
                normalize_peripheral(device_id, peripheral_id, raw_peripheral)
                for peripheral_id, raw_peripheral in dict(raw_device.peripherals).items()
            )
        return devices

    async def get_peripheral(self, device_id: str, peripheral_id: str) -> BrilliantDevice | None:
        """Return one normalized peripheral via an on-demand scoped read."""
        obs, _ = self._require_started()
        raw = await obs.get_peripheral(device_id, peripheral_id)
        if raw is None:
            return None
        return normalize_peripheral(device_id, peripheral_id, raw)

    def on_change(self, cb: Callable[[BrilliantDevice], Awaitable[None]]) -> None:
        """Register a change callback fired by :meth:`_dispatch_raw_device`.

        May be called more than once: the panel bridge and the mesh publisher
        each consume the same bus stream, so changes fan out to ALL callbacks.
        """
        self._change_cbs.append(cb)

    async def set_variables(self, device_id: str, peripheral_id: str, sets: list[VarSet]) -> None:
        """Write variables to *peripheral_id* on *device_id* (poc-findings §7).

        The write must target the bus device that OWNS the peripheral — the
        panel's own CONTROL id for local loads, "ble_mesh" for mesh loads —
        so the caller passes the device id from its snapshot.
        """
        obs, _ = self._require_started()
        response = await obs.request_set_variables_in_peripheral(
            peripheral_id,
            {s.name: s.value for s in sets},
            device_id=device_id,
        )
        logger.debug("set_variables(%s/%s) response: %s", device_id, peripheral_id, response)

    async def shutdown(self) -> None:
        """Best-effort teardown; tolerant of a never-started adapter."""
        if self._obs is not None:
            try:
                await self._obs.shutdown()
            except Exception:
                # Best-effort cleanup — log and continue; never raise from shutdown.
                logger.exception("observer shutdown failed")
        if self._proc is not None:
            try:
                await self._proc.shutdown()
            except Exception:
                # Best-effort cleanup — log and continue; never raise from shutdown.
                logger.exception("processor shutdown failed")
