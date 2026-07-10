from __future__ import annotations

import argparse
import dataclasses
import json
from collections.abc import Mapping, Sequence
from pathlib import Path

from brilliant_protocol_lab.profile import Evidence, ProtocolProfile
from brilliant_protocol_lab.redaction import sanitize

INIT_SERVICE = "_init-brilliant._tcp.local."
REQUIRED_METHODS = (
    "search_for_available_homes",
    "knock_on_home",
    "request_provisioning_with_code",
    "join_home",
)
ALLOWED_REMOVAL_VALUES = ("app-remove-then-local-delete", "unknown")


def _collect_classes(structure: object) -> dict[str, object] | None:
    if not isinstance(structure, Mapping):
        return None
    modules = structure.get("modules")
    if not isinstance(modules, Mapping):
        return None
    classes: dict[str, object] = {}
    for module in modules.values():
        if not isinstance(module, Mapping):
            continue
        module_classes = module.get("classes")
        if not isinstance(module_classes, Mapping):
            continue
        for name, detail in module_classes.items():
            classes[str(name)] = detail
    return classes


def _method_names(classes: Mapping[str, object]) -> set[str]:
    names: set[str] = set()
    for detail in classes.values():
        if not isinstance(detail, Mapping):
            continue
        methods = detail.get("methods")
        if isinstance(methods, Mapping):
            names.update(str(name) for name in methods)
    return names


def _structural_evidence(structure: object, source: str) -> tuple[Evidence, Evidence]:
    classes = _collect_classes(structure)
    if classes is None:
        missing = Evidence.unknown(f"missing or invalid --structure oracle file: {source}")
        return missing, missing

    method_names = _method_names(classes)
    missing_methods = tuple(name for name in REQUIRED_METHODS if name not in method_names)
    if missing_methods:
        provisioning_methods = Evidence.unknown(
            f"structure missing required method(s) {', '.join(missing_methods)} in {source}"
        )
    else:
        provisioning_methods = Evidence.known(REQUIRED_METHODS, f"structure:{source}")

    if not classes:
        thrift_type_graph = Evidence.unknown(f"structure has no classes recorded in {source}")
    else:
        graph = {
            name: {"fields": detail.get("fields", []) if isinstance(detail, Mapping) else []}
            for name, detail in classes.items()
        }
        thrift_type_graph = Evidence.known(graph, f"structure:{source}")

    return provisioning_methods, thrift_type_graph


def _init_service_evidence(mdns: object, source: str) -> Evidence:
    if not isinstance(mdns, Sequence) or isinstance(mdns, (str, bytes, bytearray)):
        return Evidence.unknown(f"missing or invalid --mdns oracle file: {source}")
    for entry in mdns:
        if not isinstance(entry, Mapping):
            continue
        if entry.get("service_type") != INIT_SERVICE:
            continue
        instance = entry.get("instance")
        if isinstance(instance, str) and instance:
            return Evidence.known(instance, f"mdns:{source}")
    return Evidence.unknown(f"no {INIT_SERVICE} record observed in {source}")


def _transport_evidence(capture: object, source: str) -> tuple[Evidence, Evidence, Evidence]:
    if not isinstance(capture, Mapping):
        missing = Evidence.unknown(f"missing or invalid --capture oracle file: {source}")
        return missing, missing, missing

    framing = capture.get("framing")
    protocol = capture.get("protocol")
    tls = capture.get("tls")

    framing_evidence = (
        Evidence.known(framing, f"capture:{source}")
        if isinstance(framing, str) and framing != "unknown"
        else Evidence.unknown(f"capture classifier reported unknown framing in {source}")
    )
    protocol_evidence = (
        Evidence.known(protocol, f"capture:{source}")
        if isinstance(protocol, str) and protocol != "unknown"
        else Evidence.unknown(f"capture classifier reported unknown protocol in {source}")
    )
    tls_evidence = (
        Evidence.known(tls, f"capture:{source}")
        if isinstance(tls, bool)
        else Evidence.unknown(f"capture classifier reported unknown tls mode in {source}")
    )
    return framing_evidence, protocol_evidence, tls_evidence


def _commitment_evidence(commitment: object, source: str) -> tuple[Evidence, Evidence]:
    if not isinstance(commitment, Mapping):
        missing = Evidence.unknown(f"missing or invalid --commitment oracle file: {source}")
        return missing, missing

    primitive = commitment.get("commitment")
    attestation = commitment.get("hardware_attestation")

    commitment_evidence = (
        Evidence.known(primitive, f"commitment:{source}")
        if isinstance(primitive, str)
        else Evidence.unknown(f"no matched standard commitment primitive recorded in {source}")
    )
    attestation_evidence = (
        Evidence.known(attestation, f"commitment:{source}")
        if isinstance(attestation, bool)
        else Evidence.unknown(f"hardware attestation status not resolved in {source}")
    )
    return commitment_evidence, attestation_evidence


def _removal_evidence(removal: object, source: str) -> Evidence:
    if not isinstance(removal, Mapping):
        return Evidence.unknown(f"missing or invalid --removal observation file: {source}")

    value = removal.get("removal_path")
    if value == "app-remove-then-local-delete":
        return Evidence.known(value, f"removal:{source}")
    if value == "unknown":
        return Evidence.unknown(f"removal path manually recorded as unknown in {source}")
    return Evidence.unknown(
        f"removal path value not in allowed set {ALLOWED_REMOVAL_VALUES} in {source}"
    )


def compile_profile(
    *,
    structure: object,
    structure_source: str,
    mdns: object,
    mdns_source: str,
    capture: object,
    capture_source: str,
    commitment: object,
    commitment_source: str,
    removal: object,
    removal_source: str,
) -> ProtocolProfile:
    """Compile sanitized oracle JSON into a ProtocolProfile.

    Every fact is derived only from the evidence present in the supplied
    payloads. Any absent, malformed, or conflicting fact is recorded as
    ``Evidence.unknown`` naming the exact missing source/key -- this
    function never substitutes a default transport or crypto value.
    """
    provisioning_methods, thrift_type_graph = _structural_evidence(structure, structure_source)
    init_service = _init_service_evidence(mdns, mdns_source)
    framing, protocol, tls = _transport_evidence(capture, capture_source)
    commitment_evidence, attestation_evidence = _commitment_evidence(commitment, commitment_source)
    removal_path = _removal_evidence(removal, removal_source)

    return ProtocolProfile(
        init_service=init_service,
        provisioning_methods=provisioning_methods,
        thrift_type_graph=thrift_type_graph,
        framing=framing,
        protocol=protocol,
        tls=tls,
        commitment=commitment_evidence,
        hardware_attestation=attestation_evidence,
        removal_path=removal_path,
    )


def _load_json(path: Path) -> object:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Compile sanitized oracle JSON into a ProtocolProfile."
    )
    parser.add_argument("--structure", type=Path, required=True)
    parser.add_argument("--mdns", type=Path, required=True)
    parser.add_argument("--capture", type=Path, required=True)
    parser.add_argument("--commitment", type=Path, required=True)
    parser.add_argument("--removal", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args(argv)

    if arguments.output.parent != Path("/private/tmp"):
        raise SystemExit("--output must be directly under /private/tmp")

    profile = compile_profile(
        structure=_load_json(arguments.structure),
        structure_source=str(arguments.structure),
        mdns=_load_json(arguments.mdns),
        mdns_source=str(arguments.mdns),
        capture=_load_json(arguments.capture),
        capture_source=str(arguments.capture),
        commitment=_load_json(arguments.commitment),
        commitment_source=str(arguments.commitment),
        removal=_load_json(arguments.removal),
        removal_source=str(arguments.removal),
    )

    payload = sanitize(dataclasses.asdict(profile))
    arguments.output.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    blockers = profile.blockers()
    if blockers:
        print("blockers:")
        for blocker in blockers:
            print(f"- {blocker}")
    else:
        print("blockers: none")

    return 0 if profile.ready_for_pairing() else 1


if __name__ == "__main__":
    raise SystemExit(main())
