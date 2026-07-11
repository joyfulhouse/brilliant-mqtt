"""Leader-gated, supervised entrypoint for the Home Assistant mirror."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from collections.abc import Awaitable, Callable
from typing import Protocol

from brilliant_ha_mirror.config import Settings
from brilliant_ha_mirror.mirror import Mirror
from brilliant_ha_mirror.protocols import HaClient, PeripheralHostClient
from brilliant_mqtt.protocols import MqttClient

log = logging.getLogger(__name__)

_BACKOFF_SECONDS = 5.0


class Leader(Protocol):
    """Election operations used by the supervised loop."""

    async def start(self) -> None:
        """Join the leader election."""
        ...

    async def tick(self) -> None:
        """Advance the leader election state machine."""
        ...


class ManagedMqtt(MqttClient, Protocol):
    """An MqttClient with an explicit connect/disconnect lifecycle."""

    async def connect(self) -> None:
        """Open the broker connection and start receiving."""
        ...

    async def disconnect(self) -> None:
        """Close the broker connection (best-effort)."""
        ...


HaFactory = Callable[[], HaClient]
HostFactory = Callable[[], PeripheralHostClient]
MqttFactory = Callable[[], ManagedMqtt]
TransitionCallback = Callable[[], Awaitable[None]]
LeaderFactory = Callable[[MqttClient, TransitionCallback, TransitionCallback], Leader]
Clock = Callable[[], float]
Sleep = Callable[[float], Awaitable[None]]


def _always_continue() -> bool:
    return True


async def _safe_disconnect(mqtt: ManagedMqtt) -> None:
    """Disconnect a session's MQTT adapter, never raising from cleanup."""
    try:
        await mqtt.disconnect()
    except Exception:
        log.exception("MQTT disconnect failed during session cleanup")


async def run(
    settings: Settings,
    *,
    ha_factory: HaFactory,
    host_factory: HostFactory,
    mqtt_factory: MqttFactory,
    leader_factory: LeaderFactory,
    clock: Clock,
    sleep: Sleep,
    should_continue: Callable[[], bool] = _always_continue,
) -> None:
    """Supervise leader election and rebuild every adapter after failures.

    Each supervised session builds a FRESH MQTT connection, leader, HA client,
    and peripheral host, and disconnects the MQTT connection on session end.
    Rebuilding the MQTT adapter per session (rather than sharing one for the
    process lifetime) is deliberate: MeshLeader.start() appends a callback to the
    adapter with no unregister path, so a shared adapter would leak one stale
    election callback on every reconnect. The injected clock, sleep, factories,
    and predicate keep this loop fully deterministic and runnable without panel
    firmware in unit tests.
    """
    while should_continue():
        current_mirror: Mirror | None = None
        current_ha: HaClient | None = None
        current_host: PeripheralHostClient | None = None
        callback_error: Exception | None = None

        async def stop_current() -> Exception | None:
            """Drop the current mirror and adapters, returning the first error."""
            nonlocal current_mirror, current_ha, current_host
            mirror = current_mirror
            ha = current_ha
            host = current_host
            current_mirror = None
            current_ha = None
            current_host = None

            first_error: Exception | None = None
            operations: list[tuple[str, TransitionCallback]] = []
            if mirror is not None:
                operations.append(("mirror stop", mirror.stop))
            if ha is not None:
                operations.append(("Home Assistant shutdown", ha.shutdown))
            if host is not None:
                operations.append(("peripheral host shutdown", host.shutdown))

            for operation_name, operation in operations:
                try:
                    await operation()
                except Exception as exc:
                    if first_error is None:
                        first_error = exc
                    log.exception("%s failed during cleanup", operation_name)
            return first_error

        async def on_acquire() -> None:
            nonlocal current_mirror, current_ha, current_host, callback_error
            cleanup_error = await stop_current()
            if cleanup_error is not None:
                callback_error = cleanup_error
                return

            current_ha = ha_factory()
            current_host = host_factory()
            current_mirror = Mirror(current_ha, current_host, settings)
            try:
                await current_ha.start()
                await current_host.start()
                await current_mirror.start()
            except Exception as exc:
                callback_error = exc

        async def on_lose() -> None:
            nonlocal callback_error
            cleanup_error = await stop_current()
            if cleanup_error is not None:
                callback_error = cleanup_error

        failed = False
        mqtt = mqtt_factory()
        try:
            await mqtt.connect()
            leader = leader_factory(mqtt, on_acquire, on_lose)
            await leader.start()
            while should_continue():
                await leader.tick()
                if callback_error is not None:
                    error = callback_error
                    callback_error = None
                    raise error
                # Surface a silently dropped HA connection: when we are leader,
                # the Home Assistant reader lives in a detached task whose death
                # (e.g. HA restarted and closed the socket) is invisible to the
                # leader/MQTT path. Treat it as a session failure so the finally
                # block tears down and the loop rebuilds + re-reconciles.
                if current_ha is not None and not current_ha.is_running():
                    raise RuntimeError("Home Assistant connection lost")
                if should_continue():
                    await sleep(settings.leader_heartbeat_seconds)
        except asyncio.CancelledError:
            raise
        except Exception:
            failed = True
            log.exception(
                "HA mirror session failed at monotonic %.3f; will restart after backoff",
                clock(),
            )
        finally:
            await stop_current()
            await _safe_disconnect(mqtt)

        if failed and should_continue():
            await sleep(_BACKOFF_SECONDS)


def main() -> None:
    """Read environment settings, connect MQTT, and run the mirror forever."""
    from brilliant_ha_mirror.ha_client import WsHaClient
    from brilliant_ha_mirror.hosting import RpcPeripheralHost
    from brilliant_ha_mirror.leader import MirrorLeader
    from brilliant_mqtt.config import Settings as MqttSettings
    from brilliant_mqtt.mqttio import AioMqttAdapter

    settings = Settings.from_env(os.environ)
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    mqtt_settings = MqttSettings(
        panel=settings.panel,
        mqtt_host=os.environ["MQTT_HOST"],
        mqtt_username=os.environ["MQTT_USERNAME"],
        mqtt_password=os.environ["MQTT_PASSWORD"],
        mqtt_port=int(os.environ.get("MQTT_PORT", "1883")),
        log_level=settings.log_level,
    )

    async def run_connected() -> None:
        loop = asyncio.get_running_loop()

        def ha_factory() -> HaClient:
            return WsHaClient(settings.ha_ws_url, settings.ha_token)

        def host_factory() -> PeripheralHostClient:
            return RpcPeripheralHost(loop)

        def mqtt_factory() -> ManagedMqtt:
            # A DISTINCT ClientID and NO availability ownership: this connection
            # is only for leader election and must not collide with the main
            # brilliant-mqtt bridge's ClientID or availability topic on this panel
            # (that would thrash both connections and flip the panel offline).
            return AioMqttAdapter(
                mqtt_settings,
                identifier=f"brilliant-ha-mirror-{settings.panel}",
                publish_availability=False,
            )

        def leader_factory(
            mqtt_client: MqttClient,
            on_acquire: TransitionCallback,
            on_lose: TransitionCallback,
        ) -> Leader:
            return MirrorLeader(
                mqtt_client,
                settings.panel,
                settings.leader_priority,
                settings.leader_heartbeat_seconds,
                on_acquire,
                on_lose,
                clock=time.monotonic,
            )

        await run(
            settings,
            ha_factory=ha_factory,
            host_factory=host_factory,
            mqtt_factory=mqtt_factory,
            leader_factory=leader_factory,
            clock=time.monotonic,
            sleep=asyncio.sleep,
        )

    asyncio.run(run_connected())


if __name__ == "__main__":
    main()
