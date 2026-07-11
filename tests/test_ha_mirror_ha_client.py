import asyncio

import pytest

from brilliant_ha_mirror.ha_client import (
    WsHaClient,
    area_by_entity,
    entity_from_state,
    event_to_entity,
    labeled_entity_ids,
    service_command,
)
from brilliant_ha_mirror.mapping import HaEntity, ServiceCall


class FakeTransport:
    def __init__(self, *, auth_ok: bool = True, fail_service: bool = False) -> None:
        self.incoming: asyncio.Queue[dict[str, object]] = asyncio.Queue()
        self.incoming.put_nowait({"type": "auth_required", "ha_version": "2026.7.0"})
        self.sent: list[dict[str, object]] = []
        self.closed = False
        self._auth_ok = auth_ok
        self._fail_service = fail_service

    async def send(self, message: dict[str, object]) -> None:
        self.sent.append(message)
        message_type = message.get("type")
        if message_type == "auth":
            if self._auth_ok:
                await self.incoming.put({"type": "auth_ok", "ha_version": "2026.7.0"})
            else:
                await self.incoming.put(
                    {
                        "type": "auth_invalid",
                        "message": "Invalid access token or password",
                    }
                )
            return

        cmd_id = message["id"]
        if message_type == "subscribe_events":
            result: object = None
        elif message_type == "get_states":
            result = [
                {
                    "entity_id": "light.kitchen",
                    "state": "on",
                    "attributes": {
                        "friendly_name": "Kitchen Light",
                        "brightness": 128,
                    },
                    "last_changed": "2026-07-10T12:00:00+00:00",
                    "last_updated": "2026-07-10T12:00:00+00:00",
                    "context": {"id": "context-1", "parent_id": None, "user_id": None},
                },
                {
                    "entity_id": "switch.unlabeled",
                    "state": "off",
                    "attributes": {"friendly_name": "Unlabeled Switch"},
                    "last_changed": "2026-07-10T12:00:00+00:00",
                    "last_updated": "2026-07-10T12:00:00+00:00",
                    "context": {"id": "context-2", "parent_id": None, "user_id": None},
                },
            ]
        elif message_type == "config/entity_registry/list":
            result = [
                {
                    "entity_id": "light.kitchen",
                    "area_id": "area-kitchen",
                    "labels": ["label-brilliant"],
                    "disabled_by": None,
                    "hidden_by": None,
                },
                {
                    "entity_id": "switch.unlabeled",
                    "area_id": None,
                    "labels": [],
                    "disabled_by": None,
                    "hidden_by": None,
                },
            ]
        elif message_type == "config/area_registry/list":
            result = [
                {
                    "area_id": "area-kitchen",
                    "name": "Kitchen",
                    "aliases": [],
                    "floor_id": None,
                    "labels": [],
                    "picture": None,
                }
            ]
        elif message_type == "config/label_registry/list":
            result = [
                {
                    "label_id": "label-brilliant",
                    "name": "brilliant",
                    "color": None,
                    "description": None,
                    "icon": None,
                }
            ]
        elif message_type == "call_service":
            if self._fail_service:
                await self.incoming.put(
                    {
                        "id": cmd_id,
                        "type": "result",
                        "success": False,
                        "error": {"code": "unauthorized", "message": "not allowed"},
                    }
                )
                return
            result = {"context": {"id": "context-3", "parent_id": None, "user_id": None}}
        else:
            raise AssertionError(f"Unexpected command: {message}")
        await self.incoming.put({"id": cmd_id, "type": "result", "success": True, "result": result})

    async def receive(self) -> dict[str, object]:
        return await self.incoming.get()

    async def close(self) -> None:
        self.closed = True


def test_entity_from_state_maps_fields_and_area() -> None:
    state: dict[str, object] = {
        "entity_id": "light.kitchen",
        "state": "on",
        "attributes": {"friendly_name": "Kitchen", "brightness": 128},
    }

    assert entity_from_state(state, "Kitchen") == HaEntity(
        entity_id="light.kitchen",
        state="on",
        attributes={"friendly_name": "Kitchen", "brightness": 128},
        area="Kitchen",
    )


def test_labeled_entity_ids_resolves_name_and_excludes_unlabeled() -> None:
    entity_registry: list[dict[str, object]] = [
        {
            "entity_id": "light.kitchen",
            "area_id": "kitchen",
            "labels": ["label-brilliant"],
        },
        {"entity_id": "switch.unlabeled", "area_id": None, "labels": []},
    ]
    label_registry: list[dict[str, object]] = [
        {"label_id": "label-brilliant", "name": "brilliant"},
        {"label_id": "label-other", "name": "other"},
    ]

    assert labeled_entity_ids(entity_registry, label_registry, "brilliant") == {"light.kitchen"}


def test_area_by_entity_maps_names_and_preserves_unassigned() -> None:
    entity_registry: list[dict[str, object]] = [
        {
            "entity_id": "light.kitchen",
            "area_id": "area-kitchen",
            "labels": ["label-brilliant"],
        },
        {"entity_id": "lock.front_door", "area_id": None, "labels": []},
    ]
    area_registry: list[dict[str, object]] = [
        {"area_id": "area-kitchen", "name": "Kitchen"},
    ]

    assert area_by_entity(entity_registry, area_registry) == {
        "light.kitchen": "Kitchen",
        "lock.front_door": None,
    }


def test_event_to_entity_builds_entity() -> None:
    event: dict[str, object] = {
        "id": 1,
        "type": "event",
        "event": {
            "event_type": "state_changed",
            "data": {
                "entity_id": "light.kitchen",
                "new_state": {
                    "entity_id": "light.kitchen",
                    "state": "off",
                    "attributes": {"friendly_name": "Kitchen"},
                },
                "old_state": None,
            },
        },
    }

    assert event_to_entity(event, {"light.kitchen": "Kitchen"}) == HaEntity(
        entity_id="light.kitchen",
        state="off",
        attributes={"friendly_name": "Kitchen"},
        area="Kitchen",
    )


def test_event_to_entity_returns_none_when_new_state_is_null() -> None:
    event: dict[str, object] = {
        "id": 1,
        "type": "event",
        "event": {
            "event_type": "state_changed",
            "data": {
                "entity_id": "light.kitchen",
                "new_state": None,
                "old_state": {
                    "entity_id": "light.kitchen",
                    "state": "off",
                    "attributes": {},
                },
            },
        },
    }

    assert event_to_entity(event, {"light.kitchen": "Kitchen"}) is None


def test_service_command_targets_light_and_keeps_brightness_in_service_data() -> None:
    call = ServiceCall(
        domain="light",
        service="turn_on",
        data={"entity_id": "light.kitchen", "brightness": 180},
    )

    assert service_command(7, call) == {
        "id": 7,
        "type": "call_service",
        "domain": "light",
        "service": "turn_on",
        "service_data": {"brightness": 180},
        "target": {"entity_id": "light.kitchen"},
    }


def test_service_command_targets_lock_without_extra_service_data() -> None:
    call = ServiceCall(
        domain="lock",
        service="unlock",
        data={"entity_id": "lock.front_door"},
    )

    assert service_command(8, call) == {
        "id": 8,
        "type": "call_service",
        "domain": "lock",
        "service": "unlock",
        "service_data": {},
        "target": {"entity_id": "lock.front_door"},
    }


async def test_client_authenticates_lists_entities_routes_events_and_calls_service() -> None:
    transport = FakeTransport()
    opened_urls: list[str] = []

    async def open_transport(url: str) -> FakeTransport:
        opened_urls.append(url)
        return transport

    client = WsHaClient(
        "ws://ha.local:8123/api/websocket",
        "long-lived-token",
        transport_factory=open_transport,
    )
    changed: list[HaEntity] = []
    event_received = asyncio.Event()

    async def on_state_change(entity: HaEntity) -> None:
        changed.append(entity)
        event_received.set()

    client.on_state_change(on_state_change)
    await client.start()
    entities = await client.get_entities("brilliant")

    assert opened_urls == ["ws://ha.local:8123/api/websocket"]
    assert transport.sent[:2] == [
        {"type": "auth", "access_token": "long-lived-token"},
        {"id": 1, "type": "subscribe_events", "event_type": "state_changed"},
    ]
    assert entities == [
        HaEntity(
            entity_id="light.kitchen",
            state="on",
            attributes={"friendly_name": "Kitchen Light", "brightness": 128},
            area="Kitchen",
        )
    ]

    await transport.incoming.put(
        {
            "id": 1,
            "type": "event",
            "event": {
                "event_type": "state_changed",
                "data": {
                    "entity_id": "light.kitchen",
                    "new_state": {
                        "entity_id": "light.kitchen",
                        "state": "off",
                        "attributes": {"friendly_name": "Kitchen Light"},
                    },
                    "old_state": {
                        "entity_id": "light.kitchen",
                        "state": "on",
                        "attributes": {
                            "friendly_name": "Kitchen Light",
                            "brightness": 128,
                        },
                    },
                },
                "origin": "LOCAL",
                "time_fired": "2026-07-10T12:01:00+00:00",
                "context": {"id": "context-4", "parent_id": None, "user_id": None},
            },
        }
    )
    await asyncio.wait_for(event_received.wait(), timeout=1.0)
    assert changed == [
        HaEntity(
            entity_id="light.kitchen",
            state="off",
            attributes={"friendly_name": "Kitchen Light"},
            area="Kitchen",
        )
    ]

    await client.call_service(
        ServiceCall(
            domain="light",
            service="turn_on",
            data={"entity_id": "light.kitchen", "brightness": 200},
        )
    )
    assert transport.sent[-1] == {
        "id": 6,
        "type": "call_service",
        "domain": "light",
        "service": "turn_on",
        "service_data": {"brightness": 200},
        "target": {"entity_id": "light.kitchen"},
    }

    await client.shutdown()
    assert transport.closed


async def test_client_rejects_invalid_auth_and_closes_transport() -> None:
    transport = FakeTransport(auth_ok=False)

    async def open_transport(url: str) -> FakeTransport:
        return transport

    client = WsHaClient("ws://ha.local/api/websocket", "bad", open_transport)

    with pytest.raises(RuntimeError, match="Invalid access token or password"):
        await client.start()

    assert transport.closed


async def test_client_raises_home_assistant_command_error() -> None:
    transport = FakeTransport(fail_service=True)

    async def open_transport(url: str) -> FakeTransport:
        return transport

    client = WsHaClient("ws://ha.local/api/websocket", "token", open_transport)
    await client.start()

    with pytest.raises(RuntimeError, match="not allowed"):
        await client.call_service(ServiceCall("lock", "unlock", {"entity_id": "lock.front_door"}))

    await client.shutdown()
