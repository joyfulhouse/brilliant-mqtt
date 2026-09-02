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
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
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
# Last-resort RPC backstops. Healthy panel calls complete in tens of
# milliseconds; these only prevent a wedged bus request from holding its
# caller forever. ``asyncio.wait_for`` preserves the TimeoutError surface that
# hot-poll callers already handle on the panel's Python 3.10 runtime.
_READ_DEADLINE_S = 5.0
# Caller deadline for a write, measured AFTER the per-device write lock is
# acquired. Reaching it detaches the caller (TimeoutError) but never cancels
# the closed-source RPC — it runs on, still holding its device lock (#72).
_WRITE_DEADLINE_S = 5.0
# A write still unresolved this long after acquiring its device lock is a
# wedged transport: the write-timeout latch trips and the coordinator rebuilds
# the whole session on its next tick. Fixed by design — deliberately not
# configurable (#72).
_WRITE_HARD_CAP_S = 15.0
# How long shutdown() waits for cancelled write tasks to settle before closing
# the observer anyway. A closed-source coroutine may delay or swallow
# cancellation — exactly the wedged-write case teardown exists to recover
# from — so the wait is bounded. 2 s sits well inside the coordinator's
# 5 s rebuild backoff (__main__._BACKOFF_S) and leaves room for the observer
# and processor shutdowns that follow.
_WRITE_SETTLE_TIMEOUT_S = 2.0
# Upper bound on the normalized set-variables receipt (issue #46): the RPC
# response type is closed-source and undocumented, so only a bounded repr
# string ever crosses the adapter boundary.
_RECEIPT_MAX_CHARS = 160


def _normalize_receipt(response: object) -> str:
    """Collapse a set-variables RPC response into a small, safe log string.

    Passive ack instrumentation for silently-lost mesh writes (issue #46):
    the receipt is fleet-scale evidence of what the transport ack actually
    encodes on failed deliveries. It gates nothing, and the closed-source
    response object itself never leaves this boundary.
    """
    try:
        text = repr(response)
    except Exception:
        text = "<unreprable response>"
    if len(text) > _RECEIPT_MAX_CHARS:
        text = text[:_RECEIPT_MAX_CHARS] + "..."
    return text


@dataclass
class _WriteRecord:
    """Bookkeeping for one ``set_variables`` RPC (issue #72).

    ``queued_at``/``started_at`` split the lock queue wait from the RPC time in
    the logs; ``detached`` flips when the caller gave up at its deadline so the
    write task knows to log its own late outcome (nobody else will).
    """

    label: str
    queued_at: float
    started_at: float | None = None
    detached: bool = False


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


def load_rpc_observer_class() -> Any:
    """Deferred panel-only import of ``RPCObserver``.

    This module is the single sanctioned ``lib.message_bus_api`` import site
    (CLAUDE.md). Other agent modules that need the class at the panel runtime
    boundary must call this instead of importing the panel library directly,
    so the import surface stays auditable and every module keeps importing
    off-panel.
    """
    from lib.message_bus_api.observer_interface import RPCObserver

    return RPCObserver


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
        self._change_cbs: list[tuple[Callable[[BrilliantDevice], Awaitable[None]], bool]] = []
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
        # A write unresolved past _WRITE_HARD_CAP_S is a wedged transport.
        # The session tick consumes this latch and rebuilds the whole session.
        self._write_timed_out = False
        # One write lock per bus device id (own CONTROL id, "ble_mesh"): same-
        # device writes serialize so a newer command can never actuate before
        # an older, still-running one; reads never take it (#72).
        self._write_locks: dict[str, asyncio.Lock] = {}
        # Strong refs to every write task (queued, running, or detached past
        # its caller deadline). shutdown() settles whatever is left.
        self._write_tasks: set[asyncio.Task[str]] = set()
        # Set synchronously at the top of shutdown(): a lane callback that
        # reaches set_variables() afterwards is rejected instead of creating
        # a task nobody settles (the bus closes before MQTT disconnects).
        self._shutting_down = False
        # Retain fired callback tasks so they are not garbage-collected mid-flight
        # (asyncio holds only weak references to tasks). Done-callback discards.
        self._pending_tasks: set[asyncio.Task[None]] = set()
        # Coalescing callbacks keep one newest snapshot per raw device. Lossless
        # callbacks use one callback-wide FIFO so distinct scene/mode executions
        # retain their arrival order even when several raw devices are involved.
        self._pending_pushes: dict[
            tuple[str | None, Callable[[BrilliantDevice], Awaitable[None]]],
            deque[list[BrilliantDevice]],
        ] = {}
        self._push_tasks: dict[
            tuple[str | None, Callable[[BrilliantDevice], Awaitable[None]]],
            asyncio.Task[None],
        ] = {}

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
        """Normalize a full changed device and dispatch by callback policy.

        Every peripheral is normalized on every push because the bus delta
        metadata is not trusted. A coalescing callback replaces its pending
        snapshot for this raw device with the newest one; a lossless callback
        drains every full snapshot in arrival order after any in-flight call.

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
        devices = [
            normalize_peripheral(device_id, peripheral_id, raw_peripheral)
            for peripheral_id, raw_peripheral in dict(peripherals).items()
        ]
        for cb, coalesce_pushes in cbs:
            key = (device_id if coalesce_pushes else None, cb)
            pending = self._pending_pushes.setdefault(key, deque())
            if coalesce_pushes:
                pending.clear()
            pending.append(devices)
            if key in self._push_tasks:
                continue
            task = asyncio.ensure_future(self._drain_pushes(key, cb))
            self._push_tasks[key] = task
            self._track_task(task)

    async def _drain_pushes(
        self,
        key: tuple[str | None, Callable[[BrilliantDevice], Awaitable[None]]],
        cb: Callable[[BrilliantDevice], Awaitable[None]],
    ) -> None:
        """Deliver queued whole-device snapshots serially for one callback."""
        try:
            while True:
                pending = self._pending_pushes.get(key)
                if not pending:
                    self._pending_pushes.pop(key, None)
                    return
                devices = pending.popleft()
                for device in devices:
                    try:
                        await cb(device)
                    except Exception:
                        logger.exception("bus change callback failed; continuing")
        finally:
            self._push_tasks.pop(key, None)

    def _spawn(self, coro: Awaitable[None]) -> None:
        """Schedule *coro* on the running loop, retaining a strong reference."""
        task = asyncio.ensure_future(coro)
        self._track_task(task)

    def _track_task(self, task: asyncio.Task[None]) -> None:
        """Retain *task* until completion."""
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

    async def get_all(self, *, include_extras: bool = True) -> list[BrilliantDevice]:
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

        The panel RPC layer's ``TimeoutError`` deliberately propagates so each
        caller can apply its own policy (best-effort hot poll versus fatal
        startup/reconcile).

        ``include_extras=False`` restricts a standby hot poll to the panel's
        own device. Full reads remain the default for reconcile callers.
        """
        obs, own_id = self._require_started()
        devices: list[BrilliantDevice] = []
        device_ids = (own_id, *self._extra_device_ids) if include_extras else (own_id,)
        for device_id in device_ids:
            raw_device = await asyncio.wait_for(
                obs.get_device(device_id),
                timeout=_READ_DEADLINE_S,
            )
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

    def on_change(
        self,
        cb: Callable[[BrilliantDevice], Awaitable[None]],
        *,
        coalesce_pushes: bool = True,
    ) -> None:
        """Register a change callback fired by :meth:`_dispatch_raw_device`.

        May be called more than once: the panel bridge and the mesh publisher
        each consume the same bus stream, so changes fan out to ALL callbacks.
        """
        self._change_cbs.append((cb, coalesce_pushes))

    def consume_write_timeout(self) -> bool:
        """Return and clear the outbound-write timeout latch."""
        timed_out = self._write_timed_out
        self._write_timed_out = False
        return timed_out

    async def set_variables(self, device_id: str, peripheral_id: str, sets: list[VarSet]) -> str:
        """Write variables to *peripheral_id* on *device_id* (poc-findings §7).

        The write must target the bus device that OWNS the peripheral — the
        panel's own CONTROL id for local loads, "ble_mesh" for mesh loads —
        so the caller passes the device id from its snapshot.

        Writes to the same device serialize on a per-device lock; the caller
        deadline starts only once the lock is held (queue wait is the previous
        write's time, not this RPC's). At the deadline the caller gets
        ``asyncio.TimeoutError`` while the RPC keeps running detached — it is
        never cancelled, keeps its device lock until it settles, and logs its
        own late outcome. Only the fixed hard cap latches a session rebuild.

        Returns the normalized transport-ack receipt (see
        :func:`_normalize_receipt`); the response object stays here.
        """
        obs, _ = self._require_started()
        if self._shutting_down:
            raise RuntimeError("bus adapter shutting down; write rejected")
        record = _WriteRecord(label=f"{device_id}/{peripheral_id}", queued_at=self._clock())
        acquired: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        task = asyncio.ensure_future(
            self._run_write(
                obs,
                device_id,
                peripheral_id,
                {s.name: s.value for s in sets},
                record,
                acquired,
            )
        )
        task.set_name(record.label)
        self._write_tasks.add(task)
        task.add_done_callback(self._settle_write)
        # Wake on lock acquisition OR on the task settling first (a task
        # cancelled before its first step never runs _run_write at all).
        waiters: list[asyncio.Future[Any]] = [acquired, task]
        await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
        try:
            return await asyncio.wait_for(asyncio.shield(task), timeout=_WRITE_DEADLINE_S)
        except asyncio.TimeoutError:
            record.detached = True
            logger.warning(
                "set_variables(%s) unresolved after %.0fs; detaching from the caller "
                "(RPC keeps running, device lock held; queue wait %.3fs)",
                record.label,
                _WRITE_DEADLINE_S,
                self._queue_wait(record),
            )
            raise

    async def _run_write(
        self,
        obs: Any,
        device_id: str,
        peripheral_id: str,
        values: dict[str, str],
        record: _WriteRecord,
        acquired: asyncio.Future[None],
    ) -> str:
        """The write task: hold the device lock for the RPC's whole lifetime."""
        lock = self._write_locks.setdefault(device_id, asyncio.Lock())
        async with lock:
            record.started_at = self._clock()
            if not acquired.done():
                acquired.set_result(None)
            logger.debug(
                "set_variables(%s) acquired device lock after %.3fs queue wait",
                record.label,
                self._queue_wait(record),
            )
            cap = asyncio.get_running_loop().call_later(_WRITE_HARD_CAP_S, self._cap_write, record)
            try:
                response = await obs.request_set_variables_in_peripheral(
                    peripheral_id,
                    values,
                    device_id=device_id,
                )
            except asyncio.CancelledError:
                logger.info("set_variables(%s) cancelled at session teardown", record.label)
                raise
            except Exception:
                if record.detached:
                    logger.warning(
                        "detached set_variables(%s) failed after %.1fs (queue wait %.3fs)",
                        record.label,
                        self._rpc_elapsed(record),
                        self._queue_wait(record),
                        exc_info=True,
                    )
                raise
            finally:
                cap.cancel()
        receipt = _normalize_receipt(response)
        if record.detached:
            logger.warning(
                "detached set_variables(%s) completed after %.1fs (queue wait %.3fs); receipt: %s",
                record.label,
                self._rpc_elapsed(record),
                self._queue_wait(record),
                receipt,
            )
        else:
            logger.debug(
                "set_variables(%s) receipt: %s (rpc %.3fs, queue wait %.3fs)",
                record.label,
                receipt,
                self._rpc_elapsed(record),
                self._queue_wait(record),
            )
        return receipt

    def _cap_write(self, record: _WriteRecord) -> None:
        """Hard-cap timer: the RPC is still unresolved — latch a session rebuild."""
        logger.error(
            "set_variables(%s) still unresolved %.0fs after acquiring its device lock; "
            "latching session rebuild",
            record.label,
            _WRITE_HARD_CAP_S,
        )
        self._write_timed_out = True

    def _settle_write(self, task: asyncio.Task[str]) -> None:
        """Done-callback: drop the strong ref and mark any exception retrieved.

        A detached caller never awaits its task, so without this asyncio would
        log "Task exception was never retrieved" at garbage collection.
        """
        self._write_tasks.discard(task)
        if not task.cancelled():
            task.exception()

    def _queue_wait(self, record: _WriteRecord) -> float:
        started = record.started_at if record.started_at is not None else self._clock()
        return started - record.queued_at

    def _rpc_elapsed(self, record: _WriteRecord) -> float:
        started = record.started_at if record.started_at is not None else record.queued_at
        return self._clock() - started

    async def _settle_writes(self) -> None:
        """Session teardown: cancel every outstanding write task, wait a bounded time.

        Stragglers that outlive _WRITE_SETTLE_TIMEOUT_S are logged and left to
        finish on their own: they stay strongly referenced in _write_tasks and
        their done-callback (_settle_write) still consumes any exception.
        """
        outstanding = [task for task in self._write_tasks if not task.done()]
        if not outstanding:
            return
        logger.warning(
            "cancelling %d outstanding bus write(s) at session teardown", len(outstanding)
        )
        for task in outstanding:
            task.cancel()
        _, pending = await asyncio.wait(outstanding, timeout=_WRITE_SETTLE_TIMEOUT_S)
        if pending:
            logger.error(
                "%d bus write(s) did not settle within %.0fs of cancellation; closing the "
                "observer anyway: %s",
                len(pending),
                _WRITE_SETTLE_TIMEOUT_S,
                sorted(task.get_name() for task in pending),
            )

    async def shutdown(self) -> None:
        """Best-effort teardown; tolerant of a never-started adapter."""
        self._shutting_down = True
        await self._settle_writes()
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
