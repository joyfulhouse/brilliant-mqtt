"""Offline contract tests for the first, non-executing plugin slice."""

import hashlib
import json
from copy import deepcopy
from dataclasses import replace
from typing import Any
from unittest.mock import patch

import pytest

from custom_components.brilliant_mqtt.plugin_manifest import (
    PluginValidationError,
    Replacement,
    UnitTopology,
    ValidationPolicy,
    parse_manifest,
)


def manifest_document() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "id": "demo",
        "version": "1.2.3",
        "publisher": "maintainer",
        "artifact": {
            "url": "https://example.invalid/demo.tar.gz",
            "sha256": hashlib.sha256(b"artifact").hexdigest(),
            "size_bytes": 8,
            "unpacked_bytes_max": 100,
            "files_max": 10,
        },
        "compatibility": {
            "architectures": ["armv7l"],
            "python": "3.10",
            "firmware_profiles": ["fake_firmware"],
            "manager_api": 1,
            "bridge_plugin_api": 1,
        },
        "runtime": {
            "adapter": "fake_adapter",
            "entrypoint": "bin/demo",
            "resources_profile": "fake_resources",
        },
        "capabilities": {
            "network_egress": [],
            "network_listeners": [],
            "microphone": False,
            "camera": False,
            "display_takeover": False,
            "personal_media": False,
            "provider_accounts": [],
            "ha": {"layout": "none", "reads": [], "actions": []},
            "bus": {"reads": ["own_panel_summary"], "writes": []},
            "hardware": [],
            "secrets": [],
            "retention": {"media_disk_bytes": 0, "media_lifetime_seconds": 0},
        },
        "replaces": [],
        "conflicts": [],
        "ownership": {"surfaces": [], "topics": ["plugins/demo"]},
        "health_profile": "fake_health",
    }


def policy(**kwargs: Any) -> ValidationPolicy:
    values: dict[str, Any] = {
        "adapters": frozenset({"fake_adapter"}),
        "resources_profiles": frozenset({"fake_resources"}),
        "firmware_profiles": frozenset({"fake_firmware"}),
        "health_profiles": frozenset({"fake_health"}),
        "origins": frozenset({"https://example.invalid:443"}),
        "qualified_firmware_profile": "fake_firmware",
    }
    return ValidationPolicy(**(values | kwargs))


def encoded(document: dict[str, Any]) -> bytes:
    return json.dumps(document).encode()


def test_valid_manifest_is_pure_and_pins_raw_bytes() -> None:
    raw = encoded(manifest_document())
    manifest = parse_manifest(raw, policy())
    assert manifest.id == "demo"
    assert manifest.manifest_sha256 == hashlib.sha256(raw).hexdigest()
    assert manifest.artifact.size_bytes == 8
    assert manifest.replaces == ()


@pytest.mark.parametrize(
    "raw",
    [
        b'{"schema_version":1,"schema_version":1}',
        b'{"nested":{"duplicate":1,"duplicate":2}}',
        b'{"x":NaN}',
        b'{"x":Infinity}',
        b'"' + b"x" * 65536 + b'"',
        b"[" * 1000 + b"0" + b"]" * 1000,
        b"\xff",
        b"[]",
    ],
)
def test_malformed_json_refused(raw: bytes) -> None:
    with pytest.raises(PluginValidationError):
        parse_manifest(raw, policy())


@pytest.mark.parametrize(
    "path",
    [
        (),
        ("artifact",),
        ("compatibility",),
        ("runtime",),
        ("capabilities",),
        ("capabilities", "ha"),
        ("capabilities", "bus"),
        ("capabilities", "retention"),
        ("ownership",),
    ],
)
def test_unknown_fields_refused_at_every_level(path: tuple[str, ...]) -> None:
    document = manifest_document()
    node = document
    for key in path:
        node = node[key]
    node["unexpected"] = True
    with pytest.raises(PluginValidationError):
        parse_manifest(encoded(document), policy())


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("schema_version",), True),
        (("schema_version",), 2),
        (("version",), "latest"),
        (("version",), "1.2"),
        (("id",), "../demo"),
        (("artifact", "size_bytes"), True),
        (("artifact", "sha256"), "A" * 64),
        (("artifact", "size_bytes"), 2**40),
        (("artifact", "url"), "http://example.invalid/demo"),
        (("artifact", "url"), "https://example.invalid:0/demo"),
        (("artifact", "url"), "https://other.invalid/demo"),
        (("artifact", "url"), "https://name:pass@example.invalid/demo"),
        (("runtime", "entrypoint"), "../demo"),
        (("runtime", "entrypoint"), "."),
        (("runtime", "entrypoint"), "/bin/demo"),
        (("runtime", "adapter"), "unknown"),
        (("compatibility", "firmware_profiles"), ["unknown"]),
        (("capabilities", "microphone"), True),
        (("capabilities", "camera"), True),
        (("capabilities", "bus", "writes"), ["raw_thrift"]),
        (("capabilities", "ha", "reads"), ["all_entities"]),
        (("capabilities", "retention", "media_disk_bytes"), 1),
        (("ownership", "topics"), ["brilliant/core"]),
    ],
)
def test_invalid_values_refused(path: tuple[str, ...], value: object) -> None:
    document = manifest_document()
    node = document
    for key in path[:-1]:
        node = node[key]
    node[path[-1]] = value
    with pytest.raises(PluginValidationError):
        parse_manifest(encoded(document), policy())


def replacement(identity: str = "optional.service", **kwargs: Any) -> dict[str, Any]:
    return {
        "target": "optional",
        "kind": "systemd",
        "identity": identity,
        "firmware_adapter": "fake_stock",
        **kwargs,
    }


def test_replacements_empty_by_default_with_complete_topology() -> None:
    document = manifest_document()
    document["replaces"] = [replacement()]
    with pytest.raises(PluginValidationError):
        parse_manifest(encoded(document), policy(topology=(UnitTopology("optional.service"),)))


def test_allowlisted_replacement_with_unknown_topology_fails_closed() -> None:
    document = manifest_document()
    document["replaces"] = [replacement()]
    allowed = Replacement("optional", "systemd", "optional.service", "fake_stock")
    with pytest.raises(PluginValidationError):
        parse_manifest(encoded(document), policy(replacement_allowlist=frozenset({allowed})))


@pytest.mark.parametrize(
    "identity",
    [
        "message_bus.service",
        "message_bus@instance.service",
        "brilliant-mqtt.service",
        "brilliant-bus-watchdog.service",
        "brilliant-wifi-watchdog.service",
    ],
)
def test_protected_units_cannot_be_allowlisted(identity: str) -> None:
    document = manifest_document()
    document["replaces"] = [replacement(identity)]
    allowed = Replacement("optional", "systemd", identity, "fake_stock")
    with pytest.raises(PluginValidationError):
        parse_manifest(
            encoded(document),
            policy(replacement_allowlist=frozenset({allowed}), topology=(UnitTopology(identity),)),
        )


@pytest.mark.parametrize(
    "nodes",
    [
        (UnitTopology("message_bus.service", aliases=("optional.service",)),),
        (
            UnitTopology("optional.service", dependencies=("middle.service",)),
            UnitTopology("middle.service", dependencies=("message_bus.service",)),
            UnitTopology("message_bus.service"),
        ),
        (UnitTopology("optional.service", dependencies=("unknown.service",)),),
        (
            UnitTopology("message_bus.service", dependencies=("optional.service",)),
            UnitTopology("optional.service"),
        ),
        (UnitTopology("optional.service", dependencies=("optional.service",)),),
        (
            UnitTopology("optional.service", aliases=("same.service",)),
            UnitTopology("other.service", aliases=("same.service",)),
        ),
        (
            UnitTopology(
                "optional.service",
                paths=("/etc/systemd/system/message_bus.service.d/display.conf",),
            ),
        ),
        (UnitTopology("optional.service", paths=("/var/brilliant-mqtt/app",)),),
        (UnitTopology("optional.service", paths=("/var",)),),
    ],
)
def test_alias_dependency_and_path_protection(nodes: tuple[UnitTopology, ...]) -> None:
    document = manifest_document()
    document["replaces"] = [replacement()]
    allowed = Replacement("optional", "systemd", "optional.service", "fake_stock")
    with pytest.raises(PluginValidationError):
        parse_manifest(
            encoded(document), policy(replacement_allowlist=frozenset({allowed}), topology=nodes)
        )


def test_qualified_fake_replacement_only_and_no_manifest_mutation() -> None:
    document = manifest_document()
    document["replaces"] = [replacement()]
    before = deepcopy(document)
    allowed = Replacement("optional", "systemd", "optional.service", "fake_stock")
    result = parse_manifest(
        encoded(document),
        policy(
            replacement_allowlist=frozenset({allowed}), topology=(UnitTopology("optional.service"),)
        ),
    )
    assert result.replaces == (allowed,)
    assert document == before


@pytest.mark.parametrize("relation", ["wants", "requires"])
def test_protected_dependency_directories_are_not_replaceable(relation: str) -> None:
    document = manifest_document()
    document["replaces"] = [replacement()]
    allowed = Replacement("optional", "systemd", "optional.service", "fake_stock")
    with pytest.raises(PluginValidationError):
        parse_manifest(
            encoded(document),
            policy(
                replacement_allowlist=frozenset({allowed}),
                topology=(
                    UnitTopology(
                        "optional.service",
                        paths=(
                            f"/etc/systemd/system/message_bus.service.{relation}/optional.service",
                        ),
                    ),
                ),
            ),
        )


@pytest.mark.parametrize("target", ["message_bus", "bridge", "bus_watchdog", "wifi_watchdog"])
def test_protected_symbolic_targets_cannot_hide_behind_optional_identity(target: str) -> None:
    document = manifest_document()
    item = replacement()
    item["target"] = target
    document["replaces"] = [item]
    allowed = Replacement(target, "systemd", "optional.service", "fake_stock")
    with pytest.raises(PluginValidationError):
        parse_manifest(
            encoded(document),
            policy(
                replacement_allowlist=frozenset({allowed}),
                topology=(UnitTopology("optional.service"),),
            ),
        )


def test_bridge_runtime_heartbeat_path_is_protected() -> None:
    document = manifest_document()
    document["replaces"] = [replacement()]
    allowed = Replacement("optional", "systemd", "optional.service", "fake_stock")
    with pytest.raises(PluginValidationError):
        parse_manifest(
            encoded(document),
            policy(
                replacement_allowlist=frozenset({allowed}),
                topology=(
                    UnitTopology("optional.service", paths=("/run/brilliant-mqtt/bus-heartbeat",)),
                ),
            ),
        )


def test_entrypoint_is_a_file_path_without_command_arguments() -> None:
    document = manifest_document()
    document["runtime"]["entrypoint"] = "bin/demo --extra"
    with pytest.raises(PluginValidationError):
        parse_manifest(encoded(document), policy())


def test_dependency_of_unit_with_protected_path_is_protected() -> None:
    document = manifest_document()
    document["replaces"] = [replacement()]
    allowed = Replacement("optional", "systemd", "optional.service", "fake_stock")
    with pytest.raises(PluginValidationError):
        parse_manifest(
            encoded(document),
            policy(
                replacement_allowlist=frozenset({allowed}),
                topology=(
                    UnitTopology(
                        "generic.service",
                        dependencies=("optional.service",),
                        paths=("/run/brilliant-mqtt/bus-phase",),
                    ),
                    UnitTopology("optional.service"),
                ),
            ),
        )


@pytest.mark.parametrize(
    "path",
    [
        "//var/brilliant-mqtt/app",
        "//run/brilliant-mqtt/bus-heartbeat",
        "//var/run/brilliant-mqtt/bus-phase",
        "/etc/systemd/system",
        "/run/systemd/system",
        "/usr/lib/systemd/system",
        "/lib/systemd/system",
        "/run/systemd/generator",
        "/run/systemd/generator.early",
        "/run/systemd/generator.late",
        "/run/systemd/transient",
        "/etc/systemd/system.control",
    ],
)
def test_unit_search_directories_and_double_slash_paths_are_protected(path: str) -> None:
    document = manifest_document()
    document["replaces"] = [replacement()]
    allowed = Replacement("optional", "systemd", "optional.service", "fake_stock")
    with pytest.raises(PluginValidationError):
        parse_manifest(
            encoded(document),
            policy(
                replacement_allowlist=frozenset({allowed}),
                topology=(UnitTopology("optional.service", paths=(path,)),),
            ),
        )


@pytest.mark.parametrize("path", ["//var/run/brilliant-mqtt/bus-phase", "/lib/systemd/system"])
def test_reverse_dependency_of_unit_search_directory_owner_is_protected(path: str) -> None:
    document = manifest_document()
    document["replaces"] = [replacement()]
    allowed = Replacement("optional", "systemd", "optional.service", "fake_stock")
    with pytest.raises(PluginValidationError):
        parse_manifest(
            encoded(document),
            policy(
                replacement_allowlist=frozenset({allowed}),
                topology=(
                    UnitTopology(
                        "generic.service", dependencies=("optional.service",), paths=(path,)
                    ),
                    UnitTopology("optional.service"),
                ),
            ),
        )


def test_custom_profile_core_paths_are_protected() -> None:
    document = manifest_document()
    document["replaces"] = [replacement()]
    allowed = Replacement("optional", "systemd", "optional.service", "fake_stock")
    with pytest.raises(PluginValidationError):
        parse_manifest(
            encoded(document),
            policy(
                replacement_allowlist=frozenset({allowed}),
                topology=(
                    UnitTopology("message_bus.service", paths=("/var/custom-bus/state",)),
                    UnitTopology("optional.service", paths=("/var/custom-bus",)),
                ),
            ),
        )


def test_optional_unit_child_paths_are_not_all_denied() -> None:
    document = manifest_document()
    document["replaces"] = [replacement()]
    allowed = Replacement("optional", "systemd", "optional.service", "fake_stock")
    result = parse_manifest(
        encoded(document),
        policy(
            replacement_allowlist=frozenset({allowed}),
            topology=(
                UnitTopology("optional.service", paths=("/etc/systemd/system/optional.service",)),
            ),
        ),
    )
    assert result.replaces == (allowed,)


@pytest.mark.parametrize("chained", [False, True])
def test_overlapping_core_path_owners_protect_their_prerequisites(chained: bool) -> None:
    document = manifest_document()
    document["replaces"] = [replacement()]
    allowed = Replacement("optional", "systemd", "optional.service", "fake_stock")
    nodes = [
        UnitTopology("message_bus.service", paths=("/var/custom-bus/state",)),
        UnitTopology(
            "generic.service", dependencies=("optional.service",), paths=("/var/custom-bus",)
        ),
        UnitTopology("optional.service"),
    ]
    if chained:
        nodes[1] = UnitTopology("generic.service", paths=("/var/custom-bus", "/var/second-core"))
        nodes.append(
            UnitTopology(
                "second.service",
                dependencies=("optional.service",),
                paths=("/var/second-core/runtime",),
            )
        )
    with pytest.raises(PluginValidationError):
        parse_manifest(
            encoded(document),
            policy(replacement_allowlist=frozenset({allowed}), topology=tuple(nodes)),
        )


@pytest.mark.parametrize("path", ["//var/custom-core", "/var/custom/../core", "/var//custom-core"])
def test_unresolved_profile_paths_fail_admission_even_when_not_directly_replaced(path: str) -> None:
    document = manifest_document()
    document["replaces"] = [replacement()]
    allowed = Replacement("optional", "systemd", "optional.service", "fake_stock")
    with pytest.raises(PluginValidationError):
        parse_manifest(
            encoded(document),
            policy(
                replacement_allowlist=frozenset({allowed}),
                topology=(
                    UnitTopology("message_bus.service", paths=(path,)),
                    UnitTopology("optional.service"),
                ),
            ),
        )


@pytest.mark.parametrize(
    "path",
    [
        "/etc/systemd/system/service.d/limits.conf",
        "/run/systemd/system/service.d/limits.conf",
        "/usr/lib/systemd/system/timer.d/limits.conf",
        "/etc/systemd/system/brilliant-.service.d/limits.conf",
        "/etc/systemd/system/brilliant-bus-.service.d/limits.conf",
        "/etc/systemd/system/brilliant-wifi-.service.d/limits.conf",
    ],
)
@pytest.mark.parametrize("prerequisite", [False, True])
def test_shared_core_dropins_also_protect_prerequisites(path: str, prerequisite: bool) -> None:
    document = manifest_document()
    document["replaces"] = [replacement()]
    allowed = Replacement("optional", "systemd", "optional.service", "fake_stock")
    nodes: tuple[UnitTopology, ...] = (UnitTopology("optional.service", paths=(path,)),)
    if prerequisite:
        nodes = (
            UnitTopology("optional.service"),
            UnitTopology("generic.service", paths=(path,), dependencies=("optional.service",)),
        )
    with pytest.raises(PluginValidationError):
        parse_manifest(
            encoded(document), policy(replacement_allowlist=frozenset({allowed}), topology=nodes)
        )


def test_qualified_optional_unit_dropin_is_not_a_shared_core_dropin() -> None:
    document = manifest_document()
    document["replaces"] = [replacement()]
    allowed = Replacement("optional", "systemd", "optional.service", "fake_stock")
    result = parse_manifest(
        encoded(document),
        policy(
            replacement_allowlist=frozenset({allowed}),
            topology=(
                UnitTopology(
                    "optional.service",
                    paths=("/etc/systemd/system/optional.service.d/limits.conf",),
                ),
            ),
        ),
    )
    assert result.replaces == (allowed,)


@pytest.mark.parametrize("key", ["schema_version", "size_bytes"])
def test_duplicate_key_in_otherwise_valid_manifest_is_refused(key: str) -> None:
    raw = encoded(manifest_document())
    needle = f'"{key}": '.encode()
    value = b"1" if key == "schema_version" else b"8"
    raw = raw.replace(needle + value, needle + value + b", " + needle + value, 1)
    with pytest.raises(PluginValidationError):
        parse_manifest(raw, policy())


@pytest.mark.parametrize(
    "config_name",
    [
        "bus_alias.service.d",
        "bus_alias.service.wants",
        "bus_alias.service.requires",
        "bus_alias.service.upholds",
        "bus_alias@other.service.d",
        "bus_alias@.service.d",
        "message_bus.service.upholds",
        "network-.service.d",
        "network-link.service.d",
        "bus-proxy@.service.d",
        "bus-.service.d",
    ],
)
@pytest.mark.parametrize("prerequisite", [False, True])
def test_dynamic_core_configuration_namespaces_protect_owners_and_prerequisites(
    config_name: str,
    prerequisite: bool,
) -> None:
    document = manifest_document()
    document["replaces"] = [replacement()]
    allowed = Replacement("optional", "systemd", "optional.service", "fake_stock")
    config_path = f"/etc/systemd/system/{config_name}/override.conf"
    nodes = [
        UnitTopology(
            "message_bus.service",
            aliases=("bus_alias.service", "bus-proxy@panel.service"),
            dependencies=("network-link.service",),
        ),
        UnitTopology("network-link.service"),
        UnitTopology("optional.service", paths=(config_path,)),
    ]
    if prerequisite:
        nodes[2] = UnitTopology("optional.service")
        nodes.append(
            UnitTopology(
                "generic.service", dependencies=("optional.service",), paths=(config_path,)
            )
        )
    with pytest.raises(PluginValidationError):
        parse_manifest(
            encoded(document),
            policy(replacement_allowlist=frozenset({allowed}), topology=tuple(nodes)),
        )


def test_replacements_without_a_firmware_qualification_pin_are_refused() -> None:
    document = manifest_document()
    document["replaces"] = [replacement()]
    base = policy()
    unqualified = ValidationPolicy(
        adapters=base.adapters,
        resources_profiles=base.resources_profiles,
        firmware_profiles=frozenset({"fake_firmware", "other_firmware"}),
        health_profiles=base.health_profiles,
        origins=base.origins,
        replacement_allowlist=frozenset(
            {Replacement("optional", "systemd", "optional.service", "fake_stock")}
        ),
        topology=(UnitTopology("optional.service"),),
    )
    with pytest.raises(PluginValidationError):
        parse_manifest(encoded(document), unqualified)


@pytest.mark.parametrize("origin", ["https://[2001:db8::1]", "https://[2001:db8::1]:443"])
def test_reviewed_ipv6_origins_match_implicit_and_explicit_https_ports(origin: str) -> None:
    document = manifest_document()
    document["artifact"]["url"] = origin + "/demo.tar.gz"
    ipv6_policy = replace(policy(), origins=frozenset({"https://[2001:db8::1]:443"}))
    assert parse_manifest(encoded(document), ipv6_policy).id == "demo"


def test_valid_manifest_accepts_exact_document_limit_and_refuses_one_more_before_hashing() -> None:
    raw = encoded(manifest_document())
    padded = raw + b" " * (65536 - len(raw))
    assert parse_manifest(padded, policy()).id == "demo"
    with patch("hashlib.sha256", side_effect=AssertionError("oversized input was hashed")):
        with pytest.raises(PluginValidationError):
            parse_manifest(padded + b" ", policy())


def test_ipv6_origin_with_a_different_port_is_not_reviewed() -> None:
    document = manifest_document()
    document["artifact"]["url"] = "https://[2001:db8::1]:444/demo.tar.gz"
    ipv6_policy = replace(policy(), origins=frozenset({"https://[2001:db8::1]:443"}))
    with pytest.raises(PluginValidationError):
        parse_manifest(encoded(document), ipv6_policy)


@pytest.mark.parametrize("profiles", [["other_firmware"], ["fake_firmware", "other_firmware"]])
def test_replacement_cannot_borrow_another_firmware_qualification(profiles: list[str]) -> None:
    document = manifest_document()
    document["replaces"] = [replacement()]
    document["compatibility"]["firmware_profiles"] = profiles
    allowed = Replacement("optional", "systemd", "optional.service", "fake_stock")
    qualified = policy(
        firmware_profiles=frozenset({"fake_firmware", "other_firmware"}),
        replacement_allowlist=frozenset({allowed}),
        topology=(UnitTopology("optional.service"),),
    )
    with pytest.raises(PluginValidationError):
        parse_manifest(encoded(document), qualified)


def test_exact_qualified_firmware_and_unrelated_optional_dropin_are_accepted() -> None:
    document = manifest_document()
    document["replaces"] = [replacement()]
    allowed = Replacement("optional", "systemd", "optional.service", "fake_stock")
    qualified = policy(
        firmware_profiles=frozenset({"fake_firmware", "other_firmware"}),
        replacement_allowlist=frozenset({allowed}),
        topology=(
            UnitTopology(
                "message_bus.service",
                aliases=("bus_alias.service",),
                dependencies=("ordinary.service",),
            ),
            UnitTopology("ordinary.service"),
            UnitTopology(
                "optional.service", paths=("/etc/systemd/system/optional.service.d/override.conf",)
            ),
        ),
    )
    assert parse_manifest(encoded(document), qualified).firmware_profiles == ("fake_firmware",)


@pytest.mark.parametrize("profiles", [(), ("other_firmware",), ("fake_firmware", "other_firmware")])
def test_pure_guard_independently_binds_manifest_to_qualified_firmware(
    profiles: tuple[str, ...],
) -> None:
    from custom_components.brilliant_mqtt.plugin_manifest import guard_protected_core

    document = manifest_document()
    document["replaces"] = [replacement()]
    allowed = Replacement("optional", "systemd", "optional.service", "fake_stock")
    qualified = policy(
        firmware_profiles=frozenset({"fake_firmware", "other_firmware"}),
        replacement_allowlist=frozenset({allowed}),
        topology=(UnitTopology("optional.service"),),
    )
    manifest = parse_manifest(encoded(document), qualified)
    with pytest.raises(PluginValidationError):
        guard_protected_core(replace(manifest, firmware_profiles=profiles), qualified)


def test_nonreplacement_manifest_can_declare_multiple_recognized_firmware_profiles() -> None:
    document = manifest_document()
    document["compatibility"]["firmware_profiles"] = ["fake_firmware", "other_firmware"]
    qualified = policy(firmware_profiles=frozenset({"fake_firmware", "other_firmware"}))
    assert parse_manifest(encoded(document), qualified).replaces == ()


@pytest.mark.parametrize("target", ["bus_alias", "ordinary"])
def test_dynamic_protected_unit_names_also_protect_symbolic_targets(target: str) -> None:
    document = manifest_document()
    document["replaces"] = [{**replacement(), "target": target}]
    allowed = Replacement(target, "systemd", "optional.service", "fake_stock")
    qualified = policy(
        replacement_allowlist=frozenset({allowed}),
        topology=(
            UnitTopology(
                "message_bus.service",
                aliases=("bus_alias.service",),
                dependencies=("ordinary.service",),
            ),
            UnitTopology("ordinary.service"),
            UnitTopology("optional.service"),
        ),
    )
    with pytest.raises(PluginValidationError):
        parse_manifest(encoded(document), qualified)


@pytest.mark.parametrize("prerequisite", [False, True])
def test_protected_template_alias_instances_and_their_prerequisites_are_protected(
    prerequisite: bool,
) -> None:
    identity = "optional.service" if prerequisite else "bus@instance.service"
    document = manifest_document()
    document["replaces"] = [replacement(identity)]
    allowed = Replacement("optional", "systemd", identity, "fake_stock")
    nodes = [
        UnitTopology("message_bus@.service", aliases=("bus@.service",)),
        UnitTopology("bus@instance.service"),
    ]
    if prerequisite:
        nodes[1] = UnitTopology("bus@instance.service", dependencies=("optional.service",))
        nodes.append(UnitTopology("optional.service"))
    with pytest.raises(PluginValidationError):
        parse_manifest(
            encoded(document),
            policy(replacement_allowlist=frozenset({allowed}), topology=tuple(nodes)),
        )


@pytest.mark.parametrize("stem", ["gangbox_peripherals", "faceplate_peripheral"])
@pytest.mark.parametrize("use", ["identity", "symbolic_target", "prerequisite"])
def test_verified_physical_core_process_names_cannot_be_allowlisted(stem: str, use: str) -> None:
    identity = f"{stem}.service" if use == "identity" else "optional.service"
    target = stem if use == "symbolic_target" else "optional"
    document = manifest_document()
    document["replaces"] = [{**replacement(identity), "target": target}]
    allowed = Replacement(target, "systemd", identity, "fake_stock")
    nodes = [UnitTopology(identity)]
    if use == "prerequisite":
        nodes.append(UnitTopology(f"{stem}.service", dependencies=("optional.service",)))
    with pytest.raises(PluginValidationError):
        parse_manifest(
            encoded(document),
            policy(replacement_allowlist=frozenset({allowed}), topology=tuple(nodes)),
        )


@pytest.mark.parametrize("prerequisite", [False, True])
def test_vendor_ui_binary_mutation_scope_and_its_prerequisites_are_protected(
    prerequisite: bool,
) -> None:
    document = manifest_document()
    document["replaces"] = [replacement()]
    allowed = Replacement("optional", "systemd", "optional.service", "fake_stock")
    nodes = [UnitTopology("optional.service", paths=("/data/switch-ui/switch-ui",))]
    if prerequisite:
        nodes = [
            UnitTopology("optional.service"),
            UnitTopology(
                "generic.service",
                paths=("/data/switch-ui/switch-ui",),
                dependencies=("optional.service",),
            ),
        ]
    with pytest.raises(PluginValidationError):
        parse_manifest(
            encoded(document),
            policy(replacement_allowlist=frozenset({allowed}), topology=tuple(nodes)),
        )


def test_artifact_url_with_empty_fragment_delimiter_is_refused() -> None:
    document = manifest_document()
    document["artifact"]["url"] += "#"
    with pytest.raises(PluginValidationError):
        parse_manifest(encoded(document), policy())


def test_reviewed_origin_with_empty_query_delimiter_is_refused() -> None:
    document = manifest_document()
    malformed_origin = replace(policy(), origins=frozenset({"https://example.invalid:443?"}))
    with pytest.raises(PluginValidationError):
        parse_manifest(encoded(document), malformed_origin)


def test_unrelated_template_instance_remains_eligible() -> None:
    identity = "plugin_alias@instance.service"
    document = manifest_document()
    document["replaces"] = [replacement(identity)]
    allowed = Replacement("optional", "systemd", identity, "fake_stock")
    qualified = policy(
        replacement_allowlist=frozenset({allowed}),
        topology=(
            UnitTopology("message_bus@.service", aliases=("bus@.service",)),
            UnitTopology("plugin@.service", aliases=("plugin_alias@.service",)),
            UnitTopology(identity),
        ),
    )
    assert parse_manifest(encoded(document), qualified).replaces == (allowed,)


# Art/voice names below are fake test wrappers; firmware runs vassals, not these units.
@pytest.mark.parametrize("identity", ["switch_ui_app.service", "art.service", "voice.service"])
def test_fake_optional_unit_control_does_not_imply_vendor_binary_mutation(identity: str) -> None:
    document = manifest_document()
    document["replaces"] = [replacement(identity)]
    allowed = Replacement("optional", "systemd", identity, "fake_stock")
    qualified = policy(
        replacement_allowlist=frozenset({allowed}),
        topology=(
            UnitTopology(
                identity,
                paths=(
                    f"/etc/systemd/system/{identity}",
                    f"/etc/systemd/system/{identity}.d/reviewed.conf",
                ),
            ),
        ),
    )
    assert parse_manifest(encoded(document), qualified).replaces == (allowed,)


@pytest.mark.parametrize("path", ["demo.tar.gz?release=1", "demo%23release.tar.gz"])
def test_artifact_query_or_encoded_fragment_character_is_not_a_fragment(path: str) -> None:
    document = manifest_document()
    document["artifact"]["url"] = "https://example.invalid/" + path
    assert parse_manifest(encoded(document), policy()).id == "demo"
