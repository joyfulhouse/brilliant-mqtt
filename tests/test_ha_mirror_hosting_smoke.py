"""Off-panel guards for the firmware-facing hosting adapter.

hosting.py can only run on the panel (it uses the closed-source framework), so
its behaviour is verified on-panel. Off panel we assert two invariants: the
module imports cleanly (firmware imports are deferred), and NO other module in
the package imports the firmware libraries.
"""

from __future__ import annotations

import asyncio
import importlib.util
import pathlib
import re
import sys
from types import ModuleType
from typing import Any

import pytest


class _Struct:
    def __init__(self, **fields: object) -> None:
        for name, value in fields.items():
            setattr(self, name, value)


class _FakeRoomObserver:
    def __init__(self, snapshot: object) -> None:
        self.snapshot = snapshot
        self.get_all_calls = 0
        self.shutdown_calls = 0

    async def get_all(self) -> object:
        self.get_all_calls += 1
        return self.snapshot

    async def shutdown(self) -> None:
        self.shutdown_calls += 1


class _FakeRoomProcessor:
    def __init__(self) -> None:
        self.shutdown_calls = 0

    async def shutdown(self) -> None:
        self.shutdown_calls += 1


def _firmware_available() -> bool:
    # find_spec on a submodule imports the parent package, which raises
    # ModuleNotFoundError off panel where `lib` does not exist at all.
    try:
        return importlib.util.find_spec("lib.startables") is not None
    except ModuleNotFoundError:
        return False


_HAVE_FW = _firmware_available()


def test_hosting_module_imports_off_panel() -> None:
    # Deferred firmware imports mean the module itself imports anywhere.
    import brilliant_ha_mirror.hosting as hosting

    assert hasattr(hosting, "RpcPeripheralHost")
    assert hosting._slug("HA Kitchen Light") == "ha_mirror_ha_kitchen_light"


def test_find_rooms_value_handles_dict_like_firmware_containers() -> None:
    from brilliant_ha_mirror.hosting import _find_rooms_value

    snapshot = _Struct(
        devices={
            "virtual-home": _Struct(
                peripherals={
                    "home_configuration": _Struct(
                        variables={"rooms": _Struct(value="encoded-rooms")}
                    )
                }
            )
        }
    )

    assert _find_rooms_value(snapshot) == "encoded-rooms"


def test_find_rooms_value_handles_immutable_list_firmware_containers() -> None:
    from brilliant_ha_mirror.hosting import _find_rooms_value

    snapshot = _Struct(
        devices=(
            _Struct(
                peripherals=(
                    _Struct(
                        name="home_configuration",
                        variables=(_Struct(name="rooms", value="encoded-list-rooms"),),
                    ),
                )
            ),
        )
    )

    assert _find_rooms_value(snapshot) == "encoded-list-rooms"


@pytest.mark.parametrize(
    ("snapshot", "encoded"),
    [
        (
            _Struct(
                devices={
                    "virtual-home": _Struct(
                        peripherals={
                            "home_configuration": _Struct(
                                variables={"rooms": _Struct(value="encoded-dict-rooms")}
                            )
                        }
                    )
                }
            ),
            "encoded-dict-rooms",
        ),
        (
            (
                _Struct(
                    peripherals=(
                        _Struct(
                            name="home_configuration",
                            variables=(_Struct(name="rooms", value="encoded-list-rooms"),),
                        ),
                    )
                ),
            ),
            "encoded-list-rooms",
        ),
    ],
)
async def test_get_rooms_reads_persistent_observer_for_firmware_container_variants(
    monkeypatch: pytest.MonkeyPatch,
    snapshot: object,
    encoded: str,
) -> None:
    import brilliant_ha_mirror.hosting as hosting

    observer = _FakeRoomObserver(snapshot)
    processor = _FakeRoomProcessor()
    factory_calls = 0

    async def open_observer() -> tuple[_FakeRoomObserver, _FakeRoomProcessor]:
        nonlocal factory_calls
        factory_calls += 1
        return observer, processor

    monkeypatch.setattr(hosting, "_decode_rooms", lambda value: {"room-id": value})
    host = hosting.RpcPeripheralHost(
        asyncio.get_running_loop(),
        room_observer_factory=open_observer,
    )

    assert await host.get_rooms() == {"room-id": encoded}
    assert await host.get_rooms() == {"room-id": encoded}
    assert factory_calls == 1
    assert observer.get_all_calls == 2

    await host.shutdown()
    assert observer.shutdown_calls == 1
    assert processor.shutdown_calls == 1


async def test_get_rooms_tolerates_observer_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    import brilliant_ha_mirror.hosting as hosting

    class FailingObserver(_FakeRoomObserver):
        async def get_all(self) -> object:
            raise RuntimeError("catalog unavailable")

    observer = FailingObserver(object())
    processor = _FakeRoomProcessor()

    async def open_observer() -> tuple[_FakeRoomObserver, _FakeRoomProcessor]:
        return observer, processor

    host = hosting.RpcPeripheralHost(
        asyncio.get_running_loop(),
        room_observer_factory=open_observer,
    )

    with caplog.at_level("WARNING"):
        assert await host.get_rooms() == {}

    assert "catalog unavailable" in caplog.text


async def test_get_rooms_tolerates_absent_catalog(
    caplog: pytest.LogCaptureFixture,
) -> None:
    import brilliant_ha_mirror.hosting as hosting

    observer = _FakeRoomObserver(_Struct(devices={}))
    processor = _FakeRoomProcessor()

    async def open_observer() -> tuple[_FakeRoomObserver, _FakeRoomProcessor]:
        return observer, processor

    host = hosting.RpcPeripheralHost(
        asyncio.get_running_loop(),
        room_observer_factory=open_observer,
    )

    with caplog.at_level("WARNING"):
        assert await host.get_rooms() == {}

    assert "home_configuration.rooms" in caplog.text


async def test_set_room_assignment_passes_struct_to_firmware(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import brilliant_ha_mirror.hosting as hosting

    class FakeRoomAssignment:
        def __init__(self, room_ids: list[str]) -> None:
            self.room_ids = room_ids

    writes: list[tuple[object, str, Any, bool]] = []

    class FakePeripheral:
        def _set_value_internal(
            self,
            variable_name: str,
            value: Any,
            *,
            notify: bool,
        ) -> None:
            writes.append((self, variable_name, value, notify))

    serialize_calls: list[Any] = []

    def fake_serialize(value: Any) -> str:
        serialize_calls.append(value)
        return "serialized-room-assignment"

    modules = {
        "lib": ModuleType("lib"),
        "lib.serialization": ModuleType("lib.serialization"),
        "peripherals": ModuleType("peripherals"),
        "peripherals.lib": ModuleType("peripherals.lib"),
        "peripherals.lib.peripheral_service": ModuleType("peripherals.lib.peripheral_service"),
        "peripherals.lib.peripheral_service.peripheral": ModuleType(
            "peripherals.lib.peripheral_service.peripheral"
        ),
        "thrift_types": ModuleType("thrift_types"),
        "thrift_types.configuration": ModuleType("thrift_types.configuration"),
        "thrift_types.configuration.ttypes": ModuleType("thrift_types.configuration.ttypes"),
    }
    modules["lib.serialization"].__dict__["serialize"] = fake_serialize
    modules["peripherals.lib.peripheral_service.peripheral"].__dict__["Peripheral"] = FakePeripheral
    modules["thrift_types.configuration.ttypes"].__dict__["RoomAssignment"] = FakeRoomAssignment
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)

    instance = object()
    monkeypatch.setitem(hosting._INSTANCES, "HA Kitchen", instance)
    host = hosting.RpcPeripheralHost(asyncio.get_running_loop())

    await host.set_room_assignment("HA Kitchen", ["room-kitchen"])

    assert len(writes) == 1
    written_instance, variable_name, value, notify = writes[0]
    assert written_instance is instance
    assert variable_name == "room_assignment"
    assert isinstance(value, FakeRoomAssignment)
    assert value.room_ids == ["room-kitchen"]
    assert notify is True
    assert serialize_calls == []


@pytest.mark.skipif(not _HAVE_FW, reason="firmware framework only exists on the panel")
def test_rpc_host_constructs_on_panel() -> None:
    from brilliant_ha_mirror.hosting import RpcPeripheralHost

    assert RpcPeripheralHost is not None


def test_hosting_is_the_only_firmware_importer() -> None:
    package_root = pathlib.Path(__file__).resolve().parents[1] / "src" / "brilliant_ha_mirror"
    firmware_import = re.compile(
        r"^\s*(from|import)\s+(lib|peripherals|thrift_types)\b", re.MULTILINE
    )
    offenders = []
    for path in package_root.glob("*.py"):
        if path.name == "hosting.py":
            continue
        if firmware_import.search(path.read_text()):
            offenders.append(path.name)
    assert offenders == [], f"non-hosting modules import firmware: {offenders}"
