from types import ModuleType
from typing import Any, cast

from brilliant_protocol_lab.oracle import collect_structure


def test_collects_nested_struct_reference_without_instantiation() -> None:
    module = ModuleType("fake_thrift")

    class Bootstrap:
        __module__ = "fake_thrift"
        thrift_required_fields = ["home_id"]
        thrift_spec = (None, (1, 11, "home_id", None, None))

    class JoinResult:
        __module__ = "fake_thrift"
        thrift_required_fields = ["success"]
        thrift_spec = (None, (0, 12, "success", (Bootstrap, Bootstrap.thrift_spec), None))

    dynamic_module = cast(Any, module)
    dynamic_module.Bootstrap = Bootstrap
    dynamic_module.JoinResult = JoinResult
    result = collect_structure(lambda _: module, ("fake_thrift",))
    join = cast(Any, result)["modules"]["fake_thrift"]["classes"]["JoinResult"]
    assert join["fields"][0]["type_detail"] == {
        "kind": "struct",
        "type_name": "fake_thrift.Bootstrap",
    }
