"""Home Assistant WebSocket adapter and pure JSON parsing helpers."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping
from typing import Protocol

import aiohttp

from brilliant_ha_mirror.mapping import HaEntity, ServiceCall
from brilliant_ha_mirror.protocols import HaClient

logger = logging.getLogger(__name__)

# Bound on how long a single WebSocket command waits for its result, so a panel
# command driving call_service can never hang the peripheral push path.
_COMMAND_TIMEOUT_SECONDS = 10.0


class _WsTransport(Protocol):
    async def send(self, message: dict[str, object]) -> None: ...

    async def receive(self) -> dict[str, object]: ...

    async def close(self) -> None: ...


TransportFactory = Callable[[str], Awaitable[_WsTransport]]


class _AiohttpTransport:
    """Thin aiohttp seam used by :class:`WsHaClient`."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        socket: aiohttp.ClientWebSocketResponse[bool],
    ) -> None:
        self._session = session
        self._socket = socket

    async def send(self, message: dict[str, object]) -> None:
        await self._socket.send_json(message)

    async def receive(self) -> dict[str, object]:
        return _object_dict(await self._socket.receive_json(), "WebSocket message")

    async def close(self) -> None:
        await self._socket.close()
        await self._session.close()


async def _open_aiohttp_transport(url: str) -> _WsTransport:
    session = aiohttp.ClientSession()
    try:
        socket = await session.ws_connect(url)
    except BaseException:
        await session.close()
        raise
    return _AiohttpTransport(session, socket)


def _object_dict(value: object, field: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValueError(f"{field} must be an object")
    return {key: item for key, item in value.items() if isinstance(key, str)}


def _object_list(value: object, field: str) -> list[dict[str, object]]:
    if not isinstance(value, list):
        raise ValueError(f"{field} must be a list")
    return [_object_dict(item, f"{field} item") for item in value]


def _string_field(value: Mapping[str, object], field: str) -> str:
    item = value.get(field)
    if not isinstance(item, str):
        raise ValueError(f"{field} must be a string")
    return item


def entity_from_state(state: dict[str, object], area: str | None) -> HaEntity:
    """Convert a Home Assistant state object into a mirror entity."""
    return HaEntity(
        entity_id=_string_field(state, "entity_id"),
        state=_string_field(state, "state"),
        attributes=_object_dict(state.get("attributes"), "attributes"),
        area=area,
    )


def labeled_entity_ids(
    entity_registry: list[dict[str, object]],
    label_registry: list[dict[str, object]],
    label_name: str,
) -> set[str]:
    """Resolve a label name and return entity ids assigned to that label."""
    label_ids = {
        label_id
        for label in label_registry
        if label.get("name") == label_name
        for label_id in [label.get("label_id")]
        if isinstance(label_id, str)
    }
    if not label_ids:
        return set()

    result: set[str] = set()
    for entity in entity_registry:
        entity_id = entity.get("entity_id")
        labels = entity.get("labels")
        if not isinstance(entity_id, str) or not isinstance(labels, list):
            continue
        if any(label_id in label_ids for label_id in labels if isinstance(label_id, str)):
            result.add(entity_id)
    return result


def area_by_entity(
    entity_registry: list[dict[str, object]],
    area_registry: list[dict[str, object]],
) -> dict[str, str | None]:
    """Map each entity registry id to its assigned Home Assistant area name."""
    names_by_id = {
        area_id: name
        for area in area_registry
        for area_id, name in [(area.get("area_id"), area.get("name"))]
        if isinstance(area_id, str) and isinstance(name, str)
    }
    result: dict[str, str | None] = {}
    for entity in entity_registry:
        entity_id = entity.get("entity_id")
        if not isinstance(entity_id, str):
            continue
        area_id = entity.get("area_id")
        result[entity_id] = names_by_id.get(area_id) if isinstance(area_id, str) else None
    return result


def event_to_entity(
    event: dict[str, object],
    area_by_entity: Mapping[str, str | None],
) -> HaEntity | None:
    """Convert a state_changed event into an entity, or ignore a removed state."""
    event_data = _object_dict(event.get("event"), "event")
    if event_data.get("event_type") != "state_changed":
        return None
    data = _object_dict(event_data.get("data"), "event.data")
    new_state = data.get("new_state")
    if new_state is None:
        return None
    state = _object_dict(new_state, "event.data.new_state")
    entity_id = _string_field(state, "entity_id")
    return entity_from_state(state, area_by_entity.get(entity_id))


def service_command(cmd_id: int, call: ServiceCall) -> dict[str, object]:
    """Build a Home Assistant call_service command from a mirror service call."""
    entity_id = call.data.get("entity_id")
    if not isinstance(entity_id, str):
        raise ValueError("service call entity_id must be a string")
    service_data = {key: value for key, value in call.data.items() if key != "entity_id"}
    return {
        "id": cmd_id,
        "type": "call_service",
        "domain": call.domain,
        "service": call.service,
        "service_data": service_data,
        "target": {"entity_id": entity_id},
    }


def _command_error(message: Mapping[str, object]) -> RuntimeError:
    error = message.get("error")
    if isinstance(error, dict):
        detail = error.get("message")
        code = error.get("code")
        if isinstance(detail, str) and isinstance(code, str):
            return RuntimeError(f"Home Assistant command failed ({code}): {detail}")
        if isinstance(detail, str):
            return RuntimeError(f"Home Assistant command failed: {detail}")
    return RuntimeError(f"Home Assistant command failed: {error}")


class WsHaClient(HaClient):
    """Home Assistant client backed by its authenticated WebSocket API."""

    def __init__(
        self,
        ws_url: str,
        token: str,
        transport_factory: TransportFactory | None = None,
    ) -> None:
        self._ws_url = ws_url
        self._token = token
        self._transport_factory = transport_factory or _open_aiohttp_transport
        self._transport: _WsTransport | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._next_id = 1
        self._pending: dict[int, asyncio.Future[object]] = {}
        self._on_state_change: Callable[[HaEntity], Awaitable[None]] | None = None
        self._areas_by_entity: dict[str, str | None] = {}

    async def start(self) -> None:
        """Connect, authenticate, subscribe, and start receiving messages."""
        if self._transport is not None:
            raise RuntimeError("Home Assistant client is already started")
        self._transport = await self._transport_factory(self._ws_url)
        try:
            auth_required = await self._transport.receive()
            if auth_required.get("type") != "auth_required":
                raise RuntimeError("Home Assistant did not request authentication")
            await self._transport.send({"type": "auth", "access_token": self._token})
            auth_result = await self._transport.receive()
            auth_type = auth_result.get("type")
            if auth_type == "auth_invalid":
                message = auth_result.get("message")
                detail = message if isinstance(message, str) else "authentication rejected"
                raise RuntimeError(f"Home Assistant authentication failed: {detail}")
            if auth_type != "auth_ok":
                raise RuntimeError("Home Assistant returned an unexpected authentication response")

            self._reader_task = asyncio.create_task(self._read_loop())
            await self._new_command({"type": "subscribe_events", "event_type": "state_changed"})
        except BaseException:
            await self.shutdown()
            raise

    async def get_entities(self, label: str) -> list[HaEntity]:
        """Fetch entities assigned to a label and attach their area names."""
        states = _object_list(
            await self._new_command({"type": "get_states"}),
            "get_states result",
        )
        entities = _object_list(
            await self._new_command({"type": "config/entity_registry/list"}),
            "entity registry result",
        )
        areas = _object_list(
            await self._new_command({"type": "config/area_registry/list"}),
            "area registry result",
        )
        labels = _object_list(
            await self._new_command({"type": "config/label_registry/list"}),
            "label registry result",
        )

        labeled_ids = labeled_entity_ids(entities, labels, label)
        self._areas_by_entity = area_by_entity(entities, areas)
        result: list[HaEntity] = []
        for state in states:
            # Skip (and log) a single malformed state rather than aborting the
            # whole reconcile — one bad entity must not wedge the supervisor into
            # a backoff/rebuild loop that never converges.
            try:
                entity_id = _string_field(state, "entity_id")
                if entity_id not in labeled_ids:
                    continue
                result.append(entity_from_state(state, self._areas_by_entity.get(entity_id)))
            except ValueError:
                logger.warning("skipping malformed Home Assistant state: %r", state)
        return result

    def on_state_change(self, cb: Callable[[HaEntity], Awaitable[None]]) -> None:
        """Register the callback for parsed state_changed events."""
        self._on_state_change = cb

    async def call_service(self, call: ServiceCall) -> None:
        """Invoke a Home Assistant service and wait for its result."""
        cmd_id = self._reserve_id()
        await self._send_command(cmd_id, service_command(cmd_id, call))

    async def shutdown(self) -> None:
        """Stop receiving messages and close the WebSocket transport."""
        reader_task = self._reader_task
        self._reader_task = None
        if reader_task is not None:
            reader_task.cancel()
            try:
                await reader_task
            except asyncio.CancelledError:
                pass

        self._fail_pending(RuntimeError("Home Assistant client shut down"))
        transport = self._transport
        self._transport = None
        if transport is not None:
            await transport.close()

    def _reserve_id(self) -> int:
        cmd_id = self._next_id
        self._next_id += 1
        return cmd_id

    async def _new_command(self, command: dict[str, object]) -> object:
        cmd_id = self._reserve_id()
        return await self._send_command(cmd_id, {"id": cmd_id, **command})

    async def _send_command(
        self,
        cmd_id: int,
        command: dict[str, object],
    ) -> object:
        transport = self._transport
        reader_task = self._reader_task
        if transport is None or reader_task is None:
            raise RuntimeError("Home Assistant client is not started")
        if reader_task.done():
            raise RuntimeError("Home Assistant WebSocket reader is not running")

        future = asyncio.get_running_loop().create_future()
        self._pending[cmd_id] = future
        try:
            await transport.send(command)
            # Bound the wait: this runs from a panel command's push_func, which
            # the firmware awaits. Without a timeout, a dropped/never-answered
            # result id (reader alive but silent) would hang the command path
            # forever. _fail_pending only fires when the reader dies.
            return await asyncio.wait_for(future, _COMMAND_TIMEOUT_SECONDS)
        finally:
            self._pending.pop(cmd_id, None)

    async def _read_loop(self) -> None:
        transport = self._transport
        if transport is None:
            raise RuntimeError("Home Assistant client is not started")
        try:
            while True:
                message = await transport.receive()
                if message.get("type") == "result":
                    self._route_result(message)
                elif message.get("type") == "event":
                    await self._route_event(message)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Home Assistant WebSocket reader failed")
            self._fail_pending(RuntimeError(f"Home Assistant WebSocket reader failed: {exc}"))

    def _route_result(self, message: Mapping[str, object]) -> None:
        cmd_id = message.get("id")
        if not isinstance(cmd_id, int):
            return
        future = self._pending.get(cmd_id)
        if future is None or future.done():
            return
        if message.get("success") is not True:
            future.set_exception(_command_error(message))
            return
        future.set_result(message.get("result"))

    async def _route_event(self, message: dict[str, object]) -> None:
        try:
            entity = event_to_entity(message, self._areas_by_entity)
        except ValueError:
            logger.exception("Ignoring malformed Home Assistant state_changed event")
            return
        callback = self._on_state_change
        if entity is None or callback is None:
            return
        try:
            await callback(entity)
        except Exception:
            logger.exception("Home Assistant state-change callback failed")

    def _fail_pending(self, error: RuntimeError) -> None:
        for future in self._pending.values():
            if not future.done():
                future.set_exception(error)
