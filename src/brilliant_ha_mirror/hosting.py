"""Real Brilliant peripheral-host adapter (``PeripheralHostClient``).

This is the ONLY module in :mod:`brilliant_ha_mirror` that touches the on-panel
firmware framework (``lib.*`` / ``peripherals.*``). Those imports are DEFERRED —
performed inside methods, never at module level — so ``import
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

import re
import time
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from brilliant_ha_mirror.mapping import PeripheralSpec

# Tier-1 interface variables that are integer-typed on the bus (thrift BOOL and
# I32 alike are represented as int); every other mirrored variable is text.
_INT_VARS = frozenset({"on", "dimmable", "intensity", "locked", "position"})

# Live peripheral instances by name, so the adapter can push variable updates via
# each instance's set_value(). The host instantiates the peripheral class, so the
# instance registers itself here in __init__.
_INSTANCES: dict[str, Any] = {}


def _slug(name: str) -> str:
    """A stable peripheral_id slug derived from the display name."""
    return "ha_mirror_" + re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def _var_type(var: str) -> type:
    return int if var in _INT_VARS else str


def _typed_value(var: str, raw: str) -> Any:
    return int(raw) if var in _INT_VARS else raw


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

    def __init__(self, loop: Any, socket_path: str = "/var/run/brilliant/server_socket") -> None:
        self._loop = loop
        self._socket_path = socket_path
        self._hosts: dict[str, Any] = {}

    async def start(self) -> None:
        """No global connection; each peripheral host connects on register."""
        return None

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
        await host.start()
        self._hosts[name] = host

    async def update_variables(self, name: str, values: Mapping[str, str]) -> None:
        instance = _INSTANCES.get(name)
        if instance is None:
            return
        for var, raw in values.items():
            instance.set_value(var, _typed_value(var, raw), notify=True)

    async def delete(self, name: str) -> None:
        host = self._hosts.pop(name, None)
        if host is None:
            return
        from peripherals.lib.peripheral_service.conditional_peripheral_host import (
            ConditionalPeripheralHost,
        )

        delete_impl = ConditionalPeripheralHost.__dict__["delete_peripheral"]
        result = delete_impl(host, name, int(time.time() * 1000))
        if hasattr(result, "__await__"):
            await result
        _INSTANCES.pop(name, None)
        await host.shutdown()

    async def shutdown(self) -> None:
        for name in list(self._hosts):
            await self.delete(name)
