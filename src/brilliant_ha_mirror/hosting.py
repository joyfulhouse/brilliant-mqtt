"""Real Brilliant peripheral-host adapter (``PeripheralHostClient``).

This is the ONLY module in :mod:`brilliant_ha_mirror` that touches the on-panel
firmware framework (``lib.*`` / ``peripherals.*`` / ``thrift_types.*``). Those
imports are DEFERRED — performed inside methods, never at module level — so ``import
brilliant_ha_mirror.hosting`` succeeds on any machine without the panel libs
(matching :mod:`brilliant_mqtt.bus`). Everything else runs off panel behind the
:class:`~brilliant_ha_mirror.protocols.PeripheralHostClient` Protocol with fakes.

Design (Tier 1): one peripheral host per mirrored entity, all driven from the
mirror's single event loop. ``PeripheralHost.start()`` returns after
registration (it does not block), so N hosts coexist in one loop. This reuses
the proven single-peripheral hosting path; hosting several peripheral hosts on
the panel's own device is the firmware's normal pattern (faceplate, gangbox).

Verified on-panel facts encoded here (see the HA-mirror research docs):

* ``_my_variables`` is a METHOD (a ``@property`` there raises
  ``TypeError: 'dict' object is not callable``).
* ``VariableSpec`` represents thrift BOOL as ``int`` (0/1); command values cross
  the bus as strings.
* The bus registry keys peripherals by their NAME (the ``name`` property), which
  is also the argument to ``delete_peripheral``.
* Own-device peripherals persist and must be deleted explicitly;
  ``delete_peripheral`` needs an explicit ``deletion_time_ms`` or it logs an
  error and propagates slowly across panels.
"""

from __future__ import annotations

import asyncio
import logging
import re
import secrets
import time
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from brilliant_ha_mirror.mapping import INT_VARIABLES, PeripheralSpec

logger = logging.getLogger(__name__)

_ROOM_OBSERVER_CONNECT_TIMEOUT_SECONDS = 10.0
_ROOM_OBSERVER_CONNECT_POLL_SECONDS = 0.25

RoomObserverFactory = Callable[[], Awaitable[tuple[Any, Any]]]

# Live peripheral instances by name, so the adapter can push variable updates via
# each instance's set_value(). The host instantiates the peripheral class, so the
# instance registers itself here in __init__. This is a MODULE global keyed by
# display name; it is safe because the design runs exactly ONE elected leader
# host at a time and the supervisor tears the old host down (delete() pops
# _INSTANCES) before building a new one, and peripheral names are unique (the
# orchestrator disambiguates collisions). It would need per-host keying only if
# two hosts ever coexisted.
_INSTANCES: dict[str, Any] = {}


async def _await_if_coroutine(result: Any) -> None:
    """Await a borrowed firmware method's result when it is a coroutine.

    The panel's ``set_value``-family methods are Cython methods whose
    sync/async-ness is not reliably introspectable, so callers probe before
    awaiting. Kept in one place so the reason lives in a single docstring.
    """
    if hasattr(result, "__await__"):
        await result


def _slug(name: str) -> str:
    """A stable peripheral_id slug derived from the display name."""
    return "ha_mirror_" + re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def _var_type(var: str) -> type:
    # INT_VARIABLES (defined in mapping.py alongside the variable vocabulary) is
    # the single source of truth for which bus variables are integer-typed.
    return int if var in INT_VARIABLES else str


def _typed_value(var: str, raw: str) -> Any:
    return int(raw) if var in INT_VARIABLES else raw


def _container_entries(container: Any) -> list[tuple[str | None, Any]]:
    """Normalize thrift maps and immutable thrift lists into keyed entries."""
    if container is None:
        return []
    items = getattr(container, "items", None)
    if callable(items):
        return [(key if isinstance(key, str) else None, value) for key, value in items()]
    try:
        return [(None, value) for value in container]
    except TypeError:
        return []


def _entry_name(key: str | None, value: Any) -> str | None:
    if key is not None:
        return key
    for field in ("name", "id"):
        candidate = getattr(value, field, None)
        if isinstance(candidate, str):
            return candidate
    return None


def _find_rooms_value(snapshot: Any) -> str | None:
    """Find home_configuration.rooms across firmware container variants."""
    devices = getattr(snapshot, "devices", snapshot)
    for _, device in _container_entries(devices):
        peripherals = getattr(device, "peripherals", None)
        for peripheral_key, peripheral in _container_entries(peripherals):
            if _entry_name(peripheral_key, peripheral) != "home_configuration":
                continue
            variables = getattr(peripheral, "variables", None)
            for variable_key, variable in _container_entries(variables):
                if _entry_name(variable_key, variable) != "rooms":
                    continue
                value = getattr(variable, "value", None)
                return value if isinstance(value, str) else None
    return None


def _decode_rooms(value: str) -> dict[str, str]:
    """Deserialize a firmware Rooms value into an opaque id/name catalog."""
    from lib.serialization import deserialize
    from thrift_types.configuration.ttypes import Rooms

    decoded = deserialize(Rooms, value)
    catalog: dict[str, str] = {}
    for key, room in _container_entries(getattr(decoded, "rooms", None)):
        room_id = key if key is not None else getattr(room, "id", None)
        room_name = getattr(room, "name", None)
        if isinstance(room_id, str) and isinstance(room_name, str):
            catalog[room_id] = room_name
    return catalog


def _room_assignment_value(room_ids: list[str]) -> Any:
    """Build the firmware struct expected by the in-process value setter."""
    from thrift_types.configuration.ttypes import RoomAssignment

    return RoomAssignment(room_ids=list(room_ids))


def _make_peripheral_class(
    display_name: str,
    spec: PeripheralSpec,
    on_command: Callable[[str, str], Awaitable[None]],
) -> Any:
    """Build a Peripheral subclass for one mirrored entity (deferred firmware)."""
    from peripherals.lib.peripheral_service.peripheral import Peripheral, VariableSpec

    def _push_factory(var_name: str) -> Callable[[Any], Awaitable[None]]:
        async def _push(value: Any) -> None:
            await on_command(var_name, str(value))

        return _push

    def build_variables() -> dict[str, Any]:
        variables: dict[str, Any] = {}
        for var, raw in spec.variables.items():
            settable = var in spec.command_vars
            variables[var] = VariableSpec(
                _var_type(var),
                settable,
                default_value=_typed_value(var, raw),
                push_func=_push_factory(var) if settable else None,
            )
        return variables

    class MirrorPeripheral(Peripheral):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            _INSTANCES[display_name] = self

        @property
        def name(self) -> str:
            return display_name

        @property
        def peripheral_type(self) -> int:
            return spec.peripheral_type

        def _my_variables(self) -> dict[str, Any]:
            return build_variables()

    return MirrorPeripheral


class RpcPeripheralHost:
    """Hosts mirrored HA entities as native Brilliant peripherals on this panel.

    Satisfies :class:`~brilliant_ha_mirror.protocols.PeripheralHostClient`.
    """

    def __init__(
        self,
        loop: Any,
        socket_path: str = "/var/run/brilliant/server_socket",
        *,
        room_observer_factory: RoomObserverFactory | None = None,
    ) -> None:
        self._loop = loop
        self._socket_path = socket_path
        self._hosts: dict[str, Any] = {}
        self._room_observer_factory = room_observer_factory
        self._room_observer: Any = None
        self._room_processor: Any = None
        self._room_catalog: dict[str, str] = {}

    async def start(self) -> None:
        """No global connection; each peripheral host connects on register."""
        return None

    def _point_socket_flag_at_panel(self) -> None:
        """Set the firmware bus-socket gflag to the configured socket.

        A plain ``python -m brilliant_ha_mirror`` process never parses the
        ``run_startable`` CLI flags, so ``message_bus_server_socket_path`` would
        default to ``/tmp/server_socket``. This runs from :meth:`register` after
        the host module (which defines the flag) has been imported, so every
        peripheral host connects to the real bus.
        """
        import gflags

        flags = gflags.FLAGS
        try:
            _ = flags.message_bus_server_socket_path
        except gflags.UnparsedFlagAccessError:
            flags([""])
        flags.message_bus_server_socket_path = self._socket_path

    async def register(
        self,
        name: str,
        spec: PeripheralSpec,
        on_command: Callable[[str, str], Awaitable[None]],
    ) -> None:
        if name in self._hosts:
            return
        from lib.startables.startable import HostedStartableSpec
        from peripherals.lib.peripheral_service.peripheral_host import (
            PeripheralConfig,
            PeripheralHost,
        )

        # The peripheral_host import above defines the bus-socket gflag; point it
        # at the real panel socket before building the host.
        self._point_socket_flag_at_panel()
        peripheral_class = _make_peripheral_class(name, spec, on_command)
        startable_id = _slug(name)
        config = PeripheralConfig(startable_id, peripheral_class)
        host = PeripheralHost(
            loop=self._loop,
            startable_id=startable_id,
            startables_to_host=[HostedStartableSpec(startable_id, config, {})],
            parallel_registration_limit=1,
            raise_errors_for_lost_user_configured_data=False,
            message_bus_address_override=None,
        )
        try:
            await host.start()
        except BaseException:
            # The peripheral registered itself in _INSTANCES during __init__
            # (inside host.start()); if start() fails after that, drop the stale
            # entry so a later update_variables cannot write into a dead
            # peripheral. _hosts was not yet set, so nothing else references it.
            _INSTANCES.pop(name, None)
            raise
        self._hosts[name] = host

    async def update_variables(self, name: str, values: Mapping[str, str]) -> None:
        instance = _INSTANCES.get(name)
        if instance is None:
            return
        from peripherals.lib.peripheral_service.peripheral import Peripheral

        # Reflect HA state into the reported value via the framework's internal
        # updater. The public set_value() routes through the external-set handler
        # (it fires the command push_func and rejects unconfirmed values), which
        # would create a HA->panel->HA feedback loop; _set_value_internal updates
        # the reported value + notifies subscribers WITHOUT invoking push_func
        # (verified on panel). Accessed via __dict__ (like the borrowed delete).
        update = Peripheral.__dict__["_set_value_internal"]
        for var, raw in values.items():
            await _await_if_coroutine(update(instance, var, _typed_value(var, raw), notify=True))

    async def _open_room_observer(self) -> tuple[Any, Any]:
        """Connect a dedicated read-only observer using the proven bus recipe."""
        import lib.protocol.message_bus_peer_service as mbps
        from lib.message_bus_api.observer_interface import RPCObserver
        from lib.protocol.processor import SinglePeerProcessor

        observer = RPCObserver(self._loop)
        processor = SinglePeerProcessor(
            socket_path=self._socket_path,
            my_name=f"brilliant_ha_mirror_rooms-{secrets.token_hex(4)}",
            handler=mbps.PeripheralServer(observer),
            client_class=mbps.MessageBusClient,
            loop=self._loop,
        )
        try:
            await processor.start()
            waited = 0.0
            while not processor.is_connected():
                if waited >= _ROOM_OBSERVER_CONNECT_TIMEOUT_SECONDS:
                    raise TimeoutError(
                        "room observer did not connect within "
                        f"{_ROOM_OBSERVER_CONNECT_TIMEOUT_SECONDS:.0f}s"
                    )
                await asyncio.sleep(_ROOM_OBSERVER_CONNECT_POLL_SECONDS)
                waited += _ROOM_OBSERVER_CONNECT_POLL_SECONDS
            await observer.start(processor, None)
        except BaseException:
            for component, label in (
                (observer, "room observer"),
                (processor, "room observer processor"),
            ):
                try:
                    await component.shutdown()
                except Exception:
                    logger.exception("%s shutdown after startup failure failed", label)
            raise
        return observer, processor

    async def _get_room_observer(self) -> Any:
        if self._room_observer is None:
            factory = self._room_observer_factory or self._open_room_observer
            observer, processor = await factory()
            self._room_observer = observer
            self._room_processor = processor
        return self._room_observer

    async def _close_room_observer(self) -> None:
        observer = self._room_observer
        processor = self._room_processor
        self._room_observer = None
        self._room_processor = None
        for component, label in (
            (observer, "room observer"),
            (processor, "room observer processor"),
        ):
            if component is None:
                continue
            try:
                await component.shutdown()
            except Exception:
                logger.exception("%s shutdown failed", label)

    async def get_rooms(self) -> Mapping[str, str]:
        """Read and decode the virtual home's Brilliant room catalog."""
        try:
            observer = await self._get_room_observer()
            snapshot = await observer.get_all()
            value = _find_rooms_value(snapshot)
            if value is None:
                logger.warning(
                    "home_configuration.rooms was not found on the message bus; "
                    "keeping the last room catalog"
                )
                return dict(self._room_catalog)
            self._room_catalog = _decode_rooms(value)
            return dict(self._room_catalog)
        except Exception as exc:
            logger.warning(
                "failed to read Brilliant room catalog; keeping the last catalog: %s",
                exc,
                exc_info=True,
            )
            await self._close_room_observer()
            return dict(self._room_catalog)

    async def set_room_assignment(self, name: str, room_ids: list[str]) -> None:
        """Reflect a RoomAssignment struct into a hosted peripheral."""
        instance = _INSTANCES.get(name)
        if instance is None:
            return
        from peripherals.lib.peripheral_service.peripheral import Peripheral

        update = Peripheral.__dict__["_set_value_internal"]
        value = _room_assignment_value(room_ids)
        await _await_if_coroutine(update(instance, "room_assignment", value, notify=True))

    async def delete(self, name: str) -> None:
        host = self._hosts.pop(name, None)
        if host is None:
            return
        from peripherals.lib.peripheral_service.conditional_peripheral_host import (
            ConditionalPeripheralHost,
        )

        delete_impl = ConditionalPeripheralHost.__dict__["delete_peripheral"]
        try:
            await _await_if_coroutine(delete_impl(host, name, int(time.time() * 1000)))
        finally:
            # Always drop the module-global instance entry and best-effort shut
            # the host down, even if delete_peripheral raised, so a failed delete
            # cannot leak a stale _INSTANCES entry across the session rebuild.
            _INSTANCES.pop(name, None)
            await host.shutdown()

    async def shutdown(self) -> None:
        await self._close_room_observer()
        for name in list(self._hosts):
            await self.delete(name)
