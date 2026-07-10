from __future__ import annotations

import inspect
from collections.abc import Callable
from types import ModuleType
from typing import cast

MODULE_NAMES = (
    "peripherals.bootstrap.device_provisioning_client",
    "peripherals.bootstrap.bootstrap_peripheral",
    "lib.protocol.processor",
    "lib.protocol.message_bus_peer_service",
    "thrift_types.bootstrap.ttypes",
    "thrift_types.discovery.ttypes",
    "thrift_types.message_bus.ttypes",
)


def _signature(value: object) -> str:
    try:
        return str(inspect.signature(cast(Callable[..., object], value)))
    except (TypeError, ValueError):
        return "<hidden>"


def _type_detail(detail: object) -> dict[str, object] | None:
    if detail is None:
        return None
    if isinstance(detail, tuple) and detail and inspect.isclass(detail[0]):
        target = detail[0]
        return {"kind": "struct", "type_name": f"{target.__module__}.{target.__name__}"}
    if isinstance(detail, tuple) and len(detail) == 2 and isinstance(detail[0], int):
        return {
            "kind": "collection",
            "element_type": detail[0],
            "element_detail": _type_detail(detail[1]),
        }
    if isinstance(detail, tuple) and len(detail) == 4:
        return {
            "kind": "map",
            "key_type": detail[0],
            "key_detail": _type_detail(detail[1]),
            "value_type": detail[2],
            "value_detail": _type_detail(detail[3]),
        }
    return {"kind": "opaque-shape", "arity": len(detail) if isinstance(detail, tuple) else 1}


def _describe_class(cls: type[object]) -> dict[str, object]:
    required = set(getattr(cls, "thrift_required_fields", ()))
    fields = [
        {
            "field_id": int(entry[0]),
            "thrift_type": int(entry[1]),
            "field_name": str(entry[2]),
            "required": str(entry[2]) in required,
            "type_detail": _type_detail(entry[3]),
        }
        for entry in (getattr(cls, "thrift_spec", ()) or ())
        if entry is not None
    ]
    methods = {
        name: _signature(member)
        for name, member in inspect.getmembers(cls)
        if not name.startswith("_") and callable(member)
    }
    return {"signature": _signature(cls), "fields": fields, "methods": methods}


def collect_structure(
    importer: Callable[[str], ModuleType], module_names: tuple[str, ...] = MODULE_NAMES
) -> dict[str, object]:
    modules: dict[str, object] = {}
    for module_name in module_names:
        try:
            module = importer(module_name)
        except (ImportError, OSError) as error:
            modules[module_name] = {"import_error": type(error).__name__}
            continue
        classes = {
            name: _describe_class(value)
            for name, value in inspect.getmembers(module, inspect.isclass)
            if not name.startswith("_") and value.__module__ == module.__name__
        }
        callables = {
            name: _signature(value)
            for name, value in inspect.getmembers(module)
            if not name.startswith("_")
            and callable(value)
            and not inspect.isclass(value)
            and getattr(value, "__module__", module.__name__) == module.__name__
        }
        modules[module_name] = {"classes": classes, "callables": callables}
    return {"format": 1, "modules": modules}
