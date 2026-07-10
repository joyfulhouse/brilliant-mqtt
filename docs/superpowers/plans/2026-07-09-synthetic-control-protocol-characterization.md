# Synthetic Brilliant CONTROL Protocol Characterization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce a clean-room, value-free protocol profile that proves whether an off-panel software `CONTROL` can use Brilliant's LAN discovery and peer-provisioning path without proprietary runtime dependencies or live panel/account writes.

**Architecture:** A standalone Python 3.13 protocol lab inventories an operator-owned firmware bundle outside the repository, extracts only structural interface metadata, decodes Apache Thrift messages generically, and observes Brilliant mDNS records read-only. A profile compiler marks every required pairing fact as known or unknown; only a fully known, fixture-tested profile permits writing the separate enrollment/Home Assistant App plan.

**Tech Stack:** Python 3.13, asyncio, Apache Thrift binary protocol, `zeroconf`, `ifaddr`, pytest/pytest-asyncio, Ruff, mypy strict, optional private ARM/QEMU or read-only on-panel Python oracle.

## Global Constraints

- Source design: `docs/superpowers/specs/2026-07-09-ha-virtual-brilliant-control-design.md`.
- This plan performs no panel message-bus writes, peripheral registration, pairing request, account mutation, device enrollment, service restart, or panel filesystem write.
- Office Panel (`10.100.0.10`) is the only live read-only observation target.
- Proprietary firmware, generated vendor code, packet captures, certificates, private keys, tokens, device IDs, home IDs, and raw oracle output remain outside the repository.
- Committed artifacts contain independently authored MIT-licensed code, synthetic fixtures, hashed identifiers, and value-free interoperability facts only.
- Never infer an unknown field ID, nested type, framing mode, TLS mode, commitment primitive, authentication requirement, or teardown method.
- Imports in the private oracle are structural only: no class instantiation, async method call, socket open, credential read, or filesystem mutation.
- A protocol fact is `known` only when supported by structural metadata, a synthetic known-answer vector, or a read-only LAN observation.
- If hardware attestation or an opaque proprietary cryptographic primitive is required, the result is `STOP-CLEAN-ROOM`; proprietary runtime code is not moved into production.
- The existing root package remains Python `>=3.10,<3.11`; `protocol_lab` is an isolated Python `>=3.13,<3.14` subproject.
- No Home Assistant App packaging, identity persistence, enrollment state machine, peer graph, peripheral provider, or media work is included here.
- Full gate before each commit:

```bash
uv run --project protocol_lab ruff check protocol_lab
uv run --project protocol_lab ruff format --check protocol_lab
uv run --project protocol_lab mypy --strict protocol_lab/src protocol_lab/tests
uv run --project protocol_lab pytest protocol_lab/tests -q
```

## File Structure

| Path | Responsibility |
|---|---|
| `protocol_lab/pyproject.toml` | Isolated Python 3.13 development project. |
| `protocol_lab/src/brilliant_protocol_lab/redaction.py` | Stable identifier hashing and recursive secret redaction. |
| `protocol_lab/src/brilliant_protocol_lab/manifest.py` | Hash-only inventory of a private firmware root outside Git. |
| `protocol_lab/src/brilliant_protocol_lab/oracle.py` | Structural module/class/signature/Thrift type-graph extractor. |
| `protocol_lab/tools/reference_probe.py` | Standalone read-only probe run by the private firmware interpreter. |
| `protocol_lab/src/brilliant_protocol_lab/thrift_wire.py` | Vendor-neutral recursive Thrift binary message reader/writer. |
| `protocol_lab/src/brilliant_protocol_lab/mdns.py` | Read-only `_init-brilliant`/`_brilliant` browser and safe normalization. |
| `protocol_lab/tools/browse_brilliant.py` | CLI that prints sanitized mDNS observations. |
| `protocol_lab/src/brilliant_protocol_lab/profile.py` | Known/unknown evidence model and pairing-readiness gate. |
| `protocol_lab/tools/compile_profile.py` | Combine sanitized structural and LAN observations into a profile. |
| `protocol_lab/src/brilliant_protocol_lab/capture.py` | Classify synthetic loopback captures without saving secrets. |
| `protocol_lab/tools/record_loopback.py` | One-shot loopback-only byte recorder for a private native client. |
| `protocol_lab/tools/classify_capture.py` | Convert private bytes into value-free transport facts. |
| `protocol_lab/tools/invoke_commitment.py` | Invoke one structurally identified pure callable with fixed synthetic inputs. |
| `protocol_lab/tools/classify_commitment.py` | Match synthetic vectors against a finite standard-primitive set. |
| `protocol_lab/tests/fixtures/` | Synthetic Thrift, mDNS, and capture fixtures only. |
| `docs/superpowers/research/2026-07-09-synthetic-control-protocol-gate.md` | Sanitized `GO-PAIRING-PLAN` or `STOP-CLEAN-ROOM` evidence report. |

---

### Task 1: Scaffold the isolated protocol lab

**Files:**
- Modify: `.gitignore`
- Create: `protocol_lab/pyproject.toml`
- Create: `protocol_lab/src/brilliant_protocol_lab/__init__.py`
- Create: `protocol_lab/tests/test_package.py`

**Interfaces:**
- Consumes: no runtime code.
- Produces: importable `brilliant_protocol_lab` package at version `0.1.0`; isolated quality commands.

- [ ] **Step 1: Write the failing package test**

```python
from brilliant_protocol_lab import __version__


def test_version() -> None:
    assert __version__ == "0.1.0"
```

- [ ] **Step 2: Run the test and verify the project is absent**

Run: `uv run --project protocol_lab pytest protocol_lab/tests/test_package.py -q`

Expected: FAIL because `protocol_lab/pyproject.toml` or `brilliant_protocol_lab` does not exist.

- [ ] **Step 3: Add project metadata, package initialization, and defense-in-depth ignores**

Append to `.gitignore`:

```gitignore
# Private reverse-engineering inputs/outputs — never commit.
protocol_lab/private/
*.pcap
*.pcapng
*.p12
*.pkcs12
*.oracle.local.json
*.profile.local.json
```

`protocol_lab/pyproject.toml`:

```toml
[project]
name = "brilliant-protocol-lab"
version = "0.1.0"
description = "Clean-room structural characterization tools for Brilliant LAN protocols"
license = "MIT"
requires-python = ">=3.13,<3.14"
dependencies = [
  "ifaddr>=0.2,<1",
  "thrift>=0.22,<1",
  "zeroconf>=0.147,<1",
]

[dependency-groups]
dev = [
  "mypy>=1.16,<2",
  "pytest>=8.4,<9",
  "pytest-asyncio>=1,<2",
  "ruff>=0.12,<1",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/brilliant_protocol_lab"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]

[tool.ruff]
target-version = "py313"
line-length = 100

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "ASYNC"]

[tool.mypy]
python_version = "3.13"
strict = true
```

`protocol_lab/src/brilliant_protocol_lab/__init__.py`:

```python
"""Clean-room Brilliant protocol characterization tools."""

__version__ = "0.1.0"
```

- [ ] **Step 4: Lock dependencies and run the full lab gate**

Run:

```bash
uv lock --project protocol_lab
uv run --project protocol_lab ruff check protocol_lab
uv run --project protocol_lab ruff format --check protocol_lab
uv run --project protocol_lab mypy --strict protocol_lab/src protocol_lab/tests
uv run --project protocol_lab pytest protocol_lab/tests -q
```

Expected: all commands exit 0; pytest reports `1 passed`.

- [ ] **Step 5: Commit the project boundary**

```bash
git add .gitignore protocol_lab/pyproject.toml protocol_lab/uv.lock protocol_lab/src protocol_lab/tests
git commit -m "chore(protocol-lab): scaffold clean-room analysis project"
```

### Task 2: Enforce private-root and redaction boundaries

**Files:**
- Create: `protocol_lab/src/brilliant_protocol_lab/redaction.py`
- Create: `protocol_lab/src/brilliant_protocol_lab/manifest.py`
- Create: `protocol_lab/tests/test_redaction.py`
- Create: `protocol_lab/tests/test_manifest.py`

**Interfaces:**
- Consumes: arbitrary nested observation data and an operator-selected firmware root.
- Produces: `sanitize(value: object, key: str = "") -> object`, `safe_id(value: str) -> str`, and `build_manifest(private_root: Path, repository_root: Path) -> tuple[ManifestEntry, ...]`.

- [ ] **Step 1: Write failing boundary tests**

```python
import json
from pathlib import Path

import pytest

from brilliant_protocol_lab.manifest import build_manifest
from brilliant_protocol_lab.redaction import sanitize


def test_recursive_redaction_removes_secret_values_and_hashes_ids() -> None:
    result = sanitize(
        {
            "access_token": "header.payload.signature",
            "device_id": "0123456789abcdef0123456789abcdef",
            "method": "join_home",
            "blob": b"private bytes",
        }
    )
    assert isinstance(result, dict)
    encoded = json.dumps(result)
    assert "header.payload.signature" not in encoded
    assert "0123456789abcdef0123456789abcdef" not in encoded
    assert "private bytes" not in encoded
    assert result["method"] == "join_home"


def test_manifest_refuses_a_root_inside_repository(tmp_path: Path) -> None:
    private = tmp_path / "protocol_lab" / "private"
    private.mkdir(parents=True)
    with pytest.raises(ValueError, match="outside the repository"):
        build_manifest(private, tmp_path)


def test_manifest_contains_hashes_not_contents(tmp_path: Path) -> None:
    repository = tmp_path / "repo"
    private = tmp_path / "firmware"
    repository.mkdir()
    private.mkdir()
    (private / "module.so").write_bytes(b"vendor-secret-payload")
    entries = build_manifest(private, repository)
    assert entries[0].relative_path == "module.so"
    assert entries[0].size == len(b"vendor-secret-payload")
    assert "vendor-secret-payload" not in repr(entries)
```

- [ ] **Step 2: Run tests and verify modules are absent**

Run: `uv run --project protocol_lab pytest protocol_lab/tests/test_redaction.py protocol_lab/tests/test_manifest.py -q`

Expected: FAIL during collection for missing modules.

- [ ] **Step 3: Implement recursive redaction**

```python
from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence

_SECRET_KEYS = ("password", "token", "secret", "private", "credential", "certificate")
_HEX_ID = re.compile(r"^[0-9a-fA-F]{32}$")
_PEM = re.compile(r"-----BEGIN [A-Z0-9 ]+-----")


def safe_id(value: str) -> str:
    digest = hashlib.sha256(value.lower().encode()).hexdigest()[:12]
    return f"id:{digest}"


def sanitize(value: object, key: str = "") -> object:
    if isinstance(value, Mapping):
        return {str(child_key): sanitize(child, str(child_key)) for child_key, child in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [sanitize(child, key) for child in value]
    if isinstance(value, (bytes, bytearray)):
        return f"<redacted-bytes:{len(value)}>"
    if isinstance(value, str):
        if any(fragment in key.lower() for fragment in _SECRET_KEYS) or _PEM.search(value):
            return f"<redacted:{len(value)}>"
        if _HEX_ID.fullmatch(value):
            return safe_id(value)
    return value
```

- [ ] **Step 4: Implement hash-only private inventory**

```python
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ManifestEntry:
    relative_path: str
    size: int
    sha256: str


def build_manifest(private_root: Path, repository_root: Path) -> tuple[ManifestEntry, ...]:
    private = private_root.resolve(strict=True)
    repository = repository_root.resolve(strict=True)
    if private == repository or repository in private.parents:
        raise ValueError("private firmware root must remain outside the repository")
    entries: list[ManifestEntry] = []
    for path in sorted(candidate for candidate in private.rglob("*") if candidate.is_file()):
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
        entries.append(
            ManifestEntry(
                relative_path=path.relative_to(private).as_posix(),
                size=path.stat().st_size,
                sha256=digest.hexdigest(),
            )
        )
    return tuple(entries)
```

- [ ] **Step 5: Run the full gate and commit**

Run the full lab gate. Expected: `3 passed`; all quality commands exit 0.

```bash
git add protocol_lab/src/brilliant_protocol_lab/redaction.py protocol_lab/src/brilliant_protocol_lab/manifest.py protocol_lab/tests/test_redaction.py protocol_lab/tests/test_manifest.py
git commit -m "feat(protocol-lab): enforce private firmware boundary"
```

### Task 3: Extract a recursive, value-free Thrift type graph

**Files:**
- Create: `protocol_lab/src/brilliant_protocol_lab/oracle.py`
- Create: `protocol_lab/tools/reference_probe.py`
- Create: `protocol_lab/tests/test_oracle.py`

**Interfaces:**
- Consumes: module imports from a private firmware interpreter.
- Produces: `collect_structure(importer, module_names) -> dict[str, object]` containing import status, public signatures, method names, field IDs/types/required flags, and nested type references—never instance values.

- [ ] **Step 1: Write a failing nested-type test**

```python
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

    module.Bootstrap = Bootstrap
    module.JoinResult = JoinResult
    result = collect_structure(lambda _: module, ("fake_thrift",))
    join = cast(Any, result)["modules"]["fake_thrift"]["classes"]["JoinResult"]
    assert join["fields"][0]["type_detail"] == {
        "kind": "struct",
        "type_name": "fake_thrift.Bootstrap",
    }
```

- [ ] **Step 2: Run the test and verify oracle code is absent**

Run: `uv run --project protocol_lab pytest protocol_lab/tests/test_oracle.py -q`

Expected: FAIL during collection for missing `oracle.py`.

- [ ] **Step 3: Implement nested Thrift type description**

```python
from __future__ import annotations

import inspect
from collections.abc import Callable
from types import ModuleType

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
        return str(inspect.signature(value))
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
```

- [ ] **Step 4: Implement class/module collection and standalone JSON output**

Append to `oracle.py`:

```python
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
```

`reference_probe.py`:

```python
from __future__ import annotations

import importlib
import json
import re

from brilliant_protocol_lab.oracle import MODULE_NAMES, collect_structure
from brilliant_protocol_lab.redaction import sanitize

result = sanitize(collect_structure(importlib.import_module, MODULE_NAMES))
encoded = json.dumps(result, sort_keys=True, separators=(",", ":"))
if "-----BEGIN" in encoded:
    raise SystemExit("oracle output contains PEM-shaped material")
if re.search(r"[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\.", encoded):
    raise SystemExit("oracle output contains JWT-shaped material")
if re.search(r"(?<!id:)[0-9a-f]{32}", encoded):
    raise SystemExit("oracle output contains an unredacted identifier")
print(encoded)
```

Run privately:

```bash
export BRILLIANT_REFERENCE_PYTHON=/private/tmp/brilliant-reference/data/switch-embedded/env/bin/python3
PYTHONPATH=protocol_lab/src \
  "$BRILLIANT_REFERENCE_PYTHON" protocol_lab/tools/reference_probe.py \
  > /private/tmp/brilliant-structure.oracle.local.json
```

Expected: one JSON object, exit 0, no network connection, and no Git status entry.

- [ ] **Step 5: Run the full gate and commit source only**

Run the full lab gate and `git status --short`. Expected: oracle test passes; private output remains outside Git.

```bash
git add protocol_lab/src/brilliant_protocol_lab/oracle.py protocol_lab/tools/reference_probe.py protocol_lab/tests/test_oracle.py
git commit -m "feat(protocol-lab): extract value-free Thrift type graph"
```

### Task 4: Implement a generic Apache Thrift binary codec

**Files:**
- Create: `protocol_lab/src/brilliant_protocol_lab/thrift_wire.py`
- Create: `protocol_lab/tests/test_thrift_wire.py`

**Interfaces:**
- Consumes: unframed Apache Thrift strict-binary message bytes.
- Produces: `WireField`, `WireMessage`, `decode_message(payload)`, and `encode_message(message)` without importing vendor types.

- [ ] **Step 1: Write the failing recursive round-trip test**

```python
from brilliant_protocol_lab.thrift_wire import WireField, WireMessage, decode_message, encode_message


def test_recursive_binary_round_trip() -> None:
    message = WireMessage(
        name="join_home",
        message_type=1,
        sequence_id=7,
        fields=(
            WireField(1, 11, b"synthetic-client"),
            WireField(2, 12, (WireField(1, 2, True), WireField(2, 8, 42))),
            WireField(3, 15, (WireField(0, 8, 10), WireField(1, 8, 20))),
        ),
    )
    assert decode_message(encode_message(message)) == message
```

- [ ] **Step 2: Run the test and verify the codec is absent**

Run: `uv run --project protocol_lab pytest protocol_lab/tests/test_thrift_wire.py -q`

Expected: FAIL during collection for missing `thrift_wire.py`.

- [ ] **Step 3: Implement recursive reading**

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from thrift.Thrift import TType
from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport.TTransport import TMemoryBuffer


@dataclass(frozen=True)
class WireField:
    field_id: int
    thrift_type: int
    value: Any


@dataclass(frozen=True)
class WireMessage:
    name: str
    message_type: int
    sequence_id: int
    fields: tuple[WireField, ...]


def _read_struct(protocol: TBinaryProtocol) -> tuple[WireField, ...]:
    protocol.readStructBegin()
    fields: list[WireField] = []
    while True:
        _, thrift_type, field_id = protocol.readFieldBegin()
        if thrift_type == TType.STOP:
            break
        fields.append(WireField(field_id, thrift_type, _read_value(protocol, thrift_type)))
        protocol.readFieldEnd()
    protocol.readStructEnd()
    return tuple(fields)


def _read_value(protocol: TBinaryProtocol, thrift_type: int) -> Any:
    scalar = {
        TType.BOOL: protocol.readBool,
        TType.BYTE: protocol.readByte,
        TType.I16: protocol.readI16,
        TType.I32: protocol.readI32,
        TType.I64: protocol.readI64,
        TType.DOUBLE: protocol.readDouble,
        TType.STRING: protocol.readBinary,
    }
    if thrift_type in scalar:
        return scalar[thrift_type]()
    if thrift_type == TType.STRUCT:
        return _read_struct(protocol)
    if thrift_type in (TType.LIST, TType.SET):
        element_type, size = (
            protocol.readListBegin() if thrift_type == TType.LIST else protocol.readSetBegin()
        )
        result = tuple(
            WireField(index, element_type, _read_value(protocol, element_type))
            for index in range(size)
        )
        protocol.readListEnd() if thrift_type == TType.LIST else protocol.readSetEnd()
        return result
    if thrift_type == TType.MAP:
        key_type, value_type, size = protocol.readMapBegin()
        result = tuple(
            (
                WireField(index, key_type, _read_value(protocol, key_type)),
                WireField(index, value_type, _read_value(protocol, value_type)),
            )
            for index in range(size)
        )
        protocol.readMapEnd()
        return result
    raise ValueError(f"unsupported thrift type {thrift_type}")


def decode_message(payload: bytes) -> WireMessage:
    protocol = TBinaryProtocol(TMemoryBuffer(payload))
    name, message_type, sequence_id = protocol.readMessageBegin()
    fields = _read_struct(protocol)
    protocol.readMessageEnd()
    return WireMessage(name, message_type, sequence_id, fields)
```

- [ ] **Step 4: Implement symmetric writing**

```python
def _write_struct(protocol: TBinaryProtocol, fields: tuple[WireField, ...]) -> None:
    protocol.writeStructBegin("anonymous")
    for field in fields:
        protocol.writeFieldBegin("", field.thrift_type, field.field_id)
        _write_value(protocol, field.thrift_type, field.value)
        protocol.writeFieldEnd()
    protocol.writeFieldStop()
    protocol.writeStructEnd()


def _write_value(protocol: TBinaryProtocol, thrift_type: int, value: Any) -> None:
    scalar = {
        TType.BOOL: protocol.writeBool,
        TType.BYTE: protocol.writeByte,
        TType.I16: protocol.writeI16,
        TType.I32: protocol.writeI32,
        TType.I64: protocol.writeI64,
        TType.DOUBLE: protocol.writeDouble,
    }
    if thrift_type in scalar:
        scalar[thrift_type](value)
        return
    if thrift_type == TType.STRING:
        protocol.writeBinary(value if isinstance(value, bytes) else str(value).encode())
        return
    if thrift_type == TType.STRUCT:
        _write_struct(protocol, tuple(value))
        return
    if thrift_type in (TType.LIST, TType.SET):
        elements = tuple(value)
        if not elements:
            raise ValueError("empty collection requires schema-supplied element type")
        element_type = elements[0].thrift_type
        if any(element.thrift_type != element_type for element in elements):
            raise ValueError("heterogeneous Thrift collection")
        if thrift_type == TType.LIST:
            protocol.writeListBegin(element_type, len(elements))
        else:
            protocol.writeSetBegin(element_type, len(elements))
        for element in elements:
            _write_value(protocol, element_type, element.value)
        protocol.writeListEnd() if thrift_type == TType.LIST else protocol.writeSetEnd()
        return
    if thrift_type == TType.MAP:
        pairs = tuple(value)
        if not pairs:
            raise ValueError("empty map requires schema-supplied key/value types")
        key_type, value_type = pairs[0][0].thrift_type, pairs[0][1].thrift_type
        if any(
            key.thrift_type != key_type or item.thrift_type != value_type
            for key, item in pairs
        ):
            raise ValueError("heterogeneous Thrift map")
        protocol.writeMapBegin(key_type, value_type, len(pairs))
        for key, item in pairs:
            _write_value(protocol, key_type, key.value)
            _write_value(protocol, value_type, item.value)
        protocol.writeMapEnd()
        return
    raise ValueError(f"unsupported thrift type {thrift_type}")


def encode_message(message: WireMessage) -> bytes:
    transport = TMemoryBuffer()
    protocol = TBinaryProtocol(transport)
    protocol.writeMessageBegin(message.name, message.message_type, message.sequence_id)
    _write_struct(protocol, message.fields)
    protocol.writeMessageEnd()
    return transport.getvalue()
```

Add these focused rejection tests:

```python
import pytest
from thrift.Thrift import TType


def test_empty_collection_without_schema_type_is_rejected() -> None:
    message = WireMessage("empty", 1, 1, (WireField(1, TType.LIST, ()),))
    with pytest.raises(ValueError, match="element type"):
        encode_message(message)


def test_heterogeneous_collection_is_rejected() -> None:
    message = WireMessage(
        "mixed",
        1,
        1,
        (
            WireField(
                1,
                TType.LIST,
                (WireField(0, TType.I32, 1), WireField(1, TType.STRING, b"two")),
            ),
        ),
    )
    with pytest.raises(ValueError, match="heterogeneous"):
        encode_message(message)
```

- [ ] **Step 5: Run the full gate and commit**

Run the full lab gate. Expected: codec tests pass; Ruff and mypy exit 0.

```bash
git add protocol_lab/src/brilliant_protocol_lab/thrift_wire.py protocol_lab/tests/test_thrift_wire.py
git commit -m "feat(protocol-lab): add vendor-neutral Thrift codec"
```

### Task 5: Observe Brilliant mDNS services read-only

**Files:**
- Create: `protocol_lab/src/brilliant_protocol_lab/mdns.py`
- Create: `protocol_lab/tools/browse_brilliant.py`
- Create: `protocol_lab/tests/test_mdns.py`

**Interfaces:**
- Consumes: mDNS records on one named network interface.
- Produces: `ServiceObservation`, `normalize_service(service_type, instance, addresses, port, properties) -> ServiceObservation`, and `browse_read_only(interface_name: str, timeout_s: float) -> tuple[ServiceObservation, ...]` for `_init-brilliant._tcp.local.` and `_brilliant._tcp.local.`.

- [ ] **Step 1: Write failing normalization tests**

```python
from brilliant_protocol_lab.mdns import INIT_SERVICE, normalize_service


def test_normalizes_known_txt_keys_and_hashes_ids() -> None:
    observation = normalize_service(
        service_type=INIT_SERVICE,
        instance="office._init-brilliant._tcp.local.",
        addresses=("10.100.0.10",),
        port=5555,
        properties={
            b"device_id": b"0123456789abcdef0123456789abcdef",
            b"provisioning_port": b"5556",
            b"unknown": b"discard-me",
        },
    )
    assert observation.addresses == ("10.100.0.10",)
    assert observation.properties["device_id"].startswith("id:")
    assert observation.properties["provisioning_port"] == 5556
    assert "unknown" not in observation.properties
```

- [ ] **Step 2: Run the test and verify mDNS code is absent**

Run: `uv run --project protocol_lab pytest protocol_lab/tests/test_mdns.py -q`

Expected: FAIL during collection for missing `mdns.py`.

- [ ] **Step 3: Implement safe normalization**

```python
from __future__ import annotations

from dataclasses import dataclass

from brilliant_protocol_lab.redaction import safe_id

INIT_SERVICE = "_init-brilliant._tcp.local."
HOME_SERVICE = "_brilliant._tcp.local."


@dataclass(frozen=True)
class ServiceObservation:
    service_type: str
    instance: str
    addresses: tuple[str, ...]
    port: int
    properties: dict[str, str | int]


def normalize_service(
    service_type: str,
    instance: str,
    addresses: tuple[str, ...],
    port: int,
    properties: dict[bytes, bytes | None],
) -> ServiceObservation:
    safe: dict[str, str | int] = {}
    device_id = properties.get(b"device_id")
    home_id = properties.get(b"home_id")
    provisioning_port = properties.get(b"provisioning_port")
    if device_id:
        safe["device_id"] = safe_id(device_id.decode("ascii"))
    if home_id:
        safe["home_id"] = safe_id(home_id.decode("ascii"))
    if provisioning_port:
        safe["provisioning_port"] = int(provisioning_port)
    return ServiceObservation(service_type, instance, addresses, port, safe)
```

- [ ] **Step 4: Implement the bounded read-only browser and CLI**

Append to `mdns.py`:

```python
import asyncio

import ifaddr
from zeroconf import ServiceStateChange
from zeroconf.asyncio import AsyncServiceBrowser, AsyncServiceInfo, AsyncZeroconf


def _interface_ipv4(interface_name: str) -> str:
    for adapter in ifaddr.get_adapters():
        if interface_name not in (adapter.name, adapter.nice_name):
            continue
        for address in adapter.ips:
            if isinstance(address.ip, str):
                return address.ip
    raise ValueError(f"interface {interface_name!r} has no IPv4 address")


async def browse_read_only(
    interface_name: str, timeout_s: float
) -> tuple[ServiceObservation, ...]:
    if timeout_s <= 0:
        raise ValueError("timeout_s must be greater than zero")
    zeroconf = AsyncZeroconf(interfaces=[_interface_ipv4(interface_name)])
    observations: dict[tuple[str, str], ServiceObservation] = {}
    pending: set[asyncio.Task[None]] = set()

    async def resolve(service_type: str, name: str) -> None:
        info = AsyncServiceInfo(service_type, name)
        found = await info.async_request(zeroconf.zeroconf, int(timeout_s * 1000))
        if not found:
            return
        observations[(service_type, name)] = normalize_service(
            service_type=service_type,
            instance=name,
            addresses=tuple(info.parsed_scoped_addresses()),
            port=info.port,
            properties=dict(info.properties),
        )

    def on_change(
        _zeroconf: object,
        service_type: str,
        name: str,
        state: ServiceStateChange,
    ) -> None:
        if state not in (ServiceStateChange.Added, ServiceStateChange.Updated):
            return
        task = asyncio.create_task(resolve(service_type, name))
        pending.add(task)
        task.add_done_callback(pending.discard)

    browsers = [
        AsyncServiceBrowser(zeroconf.zeroconf, service_type, handlers=[on_change])
        for service_type in (INIT_SERVICE, HOME_SERVICE)
    ]
    try:
        await asyncio.sleep(timeout_s)
        if pending:
            await asyncio.gather(*tuple(pending))
        return tuple(
            sorted(observations.values(), key=lambda item: (item.service_type, item.instance))
        )
    finally:
        for browser in browsers:
            await browser.async_cancel()
        await zeroconf.async_close()
```

`browse_brilliant.py`:

```python
from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import asdict

from brilliant_protocol_lab.mdns import browse_read_only

parser = argparse.ArgumentParser()
parser.add_argument("--interface", required=True)
parser.add_argument("--timeout", type=float, default=15.0)
arguments = parser.parse_args()
observations = asyncio.run(browse_read_only(arguments.interface, arguments.timeout))
print(json.dumps([asdict(item) for item in observations], sort_keys=True))
```

Do not register a service or send a unicast provisioning request.

Run:

```bash
uv run --project protocol_lab python protocol_lab/tools/browse_brilliant.py \
  --interface eth0 --timeout 15 \
  > /private/tmp/brilliant-mdns.oracle.local.json
```

Expected on a LAN-reachable host: zero or more sanitized records; raw device/home IDs never appear. Zero records is an evidence result, not a reason to advertise or modify network policy during this task.

- [ ] **Step 5: Run the full gate and commit**

Run the full lab gate. Expected: normalization and fake-backend tests pass.

```bash
git add protocol_lab/src/brilliant_protocol_lab/mdns.py protocol_lab/tools/browse_brilliant.py protocol_lab/tests/test_mdns.py
git commit -m "feat(protocol-lab): observe Brilliant mDNS safely"
```

### Task 6: Compile evidence without converting unknowns into assumptions

**Files:**
- Create: `protocol_lab/src/brilliant_protocol_lab/profile.py`
- Create: `protocol_lab/tools/compile_profile.py`
- Create: `protocol_lab/tests/test_profile.py`

**Interfaces:**
- Consumes: sanitized structural and mDNS observation JSON plus explicit synthetic-conformance facts.
- Produces: `Evidence`, `ProtocolProfile`, `ProtocolProfile.ready_for_pairing() -> bool`, and a local JSON profile with a reason for every unknown.

- [ ] **Step 1: Write failing readiness tests**

```python
from dataclasses import replace

from brilliant_protocol_lab.profile import Evidence, ProtocolProfile


def _known(value: object) -> Evidence:
    return Evidence.known(value, "synthetic-test")


def _complete_profile() -> ProtocolProfile:
    return ProtocolProfile(
        init_service=_known("_init-brilliant._tcp.local."),
        provisioning_methods=_known(
            (
                "search_for_available_homes",
                "knock_on_home",
                "request_provisioning_with_code",
                "join_home",
            )
        ),
        thrift_type_graph=_known({"join_home_args": {"fields": []}}),
        framing=_known("framed"),
        protocol=_known("binary"),
        tls=_known(False),
        commitment=_known("hmac-sha256"),
        hardware_attestation=_known(False),
        removal_path=_known("app-remove-then-local-delete"),
    )


def test_unknown_transport_blocks_pairing_plan() -> None:
    profile = _complete_profile()
    profile = replace(profile, framing=Evidence.unknown("no loopback capture"))
    assert profile.ready_for_pairing() is False
    assert profile.blockers() == ("framing: no loopback capture",)


def test_complete_standard_profile_allows_pairing_plan() -> None:
    profile = _complete_profile()
    assert profile.ready_for_pairing() is True
    assert profile.blockers() == ()
```

- [ ] **Step 2: Run tests and verify profile code is absent**

Run: `uv run --project protocol_lab pytest protocol_lab/tests/test_profile.py -q`

Expected: FAIL during collection for missing `profile.py`.

- [ ] **Step 3: Implement immutable evidence and readiness rules**

```python
from __future__ import annotations

from dataclasses import dataclass, fields
from enum import StrEnum
from typing import cast


class EvidenceStatus(StrEnum):
    KNOWN = "known"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class Evidence:
    status: EvidenceStatus
    value: object | None
    source: str

    @classmethod
    def known(cls, value: object, source: str) -> Evidence:
        return cls(EvidenceStatus.KNOWN, value, source)

    @classmethod
    def unknown(cls, reason: str) -> Evidence:
        return cls(EvidenceStatus.UNKNOWN, None, reason)


@dataclass(frozen=True)
class ProtocolProfile:
    init_service: Evidence
    provisioning_methods: Evidence
    thrift_type_graph: Evidence
    framing: Evidence
    protocol: Evidence
    tls: Evidence
    commitment: Evidence
    hardware_attestation: Evidence
    removal_path: Evidence

    def blockers(self) -> tuple[str, ...]:
        blocked: list[str] = []
        for field in fields(self):
            evidence = cast(Evidence, getattr(self, field.name))
            if evidence.status is EvidenceStatus.UNKNOWN:
                blocked.append(f"{field.name}: {evidence.source}")
        if (
            self.hardware_attestation.status is EvidenceStatus.KNOWN
            and self.hardware_attestation.value is not False
        ):
            blocked.append("hardware_attestation: proprietary attestation required")
        allowed_commitments = {"hmac-sha256", "sha256-client-server", "sha256-server-client"}
        if (
            self.commitment.status is EvidenceStatus.KNOWN
            and self.commitment.value not in allowed_commitments
        ):
            blocked.append("commitment: no matched standard primitive")
        return tuple(blocked)

    def ready_for_pairing(self) -> bool:
        return not self.blockers()
```

- [ ] **Step 4: Implement the compiler with explicit evidence sources**

`compile_profile.py` accepts:

```text
--structure /private/tmp/brilliant-structure.oracle.local.json
--mdns /private/tmp/brilliant-mdns.oracle.local.json
--capture /private/tmp/brilliant-loopback-capture.oracle.local.json
--commitment /private/tmp/brilliant-commitment.oracle.local.json
--removal /private/tmp/brilliant-removal.oracle.local.json
--output /private/tmp/brilliant-control.profile.local.json
```

It derives only these facts automatically:

- `init_service` from an observed `_init-brilliant._tcp.local.` record;
- the four required method names and recursive type graph from structural metadata;
- framing/protocol/TLS from Task 7's capture classifier;
- commitment and hardware-attestation status from Task 7's synthetic conformance record;
- removal path from a manually recorded, read-only app/UI observation whose value is one of `app-remove-then-local-delete` or `unknown`.

For every absent or conflicting fact, write `Evidence.unknown` with the exact missing source/key. Serialize with `dataclasses.asdict`, pass through `sanitize`, write only under `/private/tmp`, and print the blocker list. Never choose a default transport or crypto value.

- [ ] **Step 5: Run the full gate and commit the compiler**

Run the full lab gate. Expected: readiness tests pass and the compiler unit test proves missing input remains unknown.

```bash
git add protocol_lab/src/brilliant_protocol_lab/profile.py protocol_lab/tools/compile_profile.py protocol_lab/tests/test_profile.py
git commit -m "feat(protocol-lab): gate pairing plan on explicit evidence"
```

### Task 7: Classify private loopback evidence and publish the go/no-go report

**Files:**
- Create: `protocol_lab/src/brilliant_protocol_lab/capture.py`
- Create: `protocol_lab/tools/record_loopback.py`
- Create: `protocol_lab/tools/classify_capture.py`
- Create: `protocol_lab/tools/invoke_commitment.py`
- Create: `protocol_lab/tools/classify_commitment.py`
- Create: `protocol_lab/tests/test_capture.py`
- Create: `docs/superpowers/research/2026-07-09-synthetic-control-protocol-gate.md`

**Interfaces:**
- Consumes: the first bytes sent by a private native provisioning client to a loopback recording socket and synthetic commitment vectors.
- Produces: `TransportClassification`, a sanitized profile, and exactly one report outcome: `GO-PAIRING-PLAN` or `STOP-CLEAN-ROOM`.

- [ ] **Step 1: Write failing transport-classification tests**

```python
from brilliant_protocol_lab.capture import classify_transport


def test_classifies_framed_strict_binary() -> None:
    message = bytes.fromhex("80010001") + b"synthetic"
    frame = len(message).to_bytes(4, "big") + message
    result = classify_transport(frame)
    assert result.framing == "framed"
    assert result.protocol == "binary"
    assert result.tls is False


def test_classifies_tls_client_hello_without_guessing_inner_protocol() -> None:
    result = classify_transport(bytes.fromhex("1603030020") + b"x" * 32)
    assert result.tls is True
    assert result.framing == "unknown"
    assert result.protocol == "unknown"


def test_unknown_bytes_remain_unknown() -> None:
    result = classify_transport(b"not-a-protocol")
    assert (result.framing, result.protocol, result.tls) == (
        "unknown",
        "unknown",
        "unknown",
    )
```

- [ ] **Step 2: Run tests and verify capture code is absent**

Run: `uv run --project protocol_lab pytest protocol_lab/tests/test_capture.py -q`

Expected: FAIL during collection for missing `capture.py`.

- [ ] **Step 3: Implement conservative prefix classification**

```python
from __future__ import annotations

from dataclasses import dataclass

_STRICT_BINARY_VERSION = 0x80010000
_VERSION_MASK = 0xFFFF0000


@dataclass(frozen=True)
class TransportClassification:
    framing: str
    protocol: str
    tls: bool | str


def _is_binary_header(data: bytes) -> bool:
    return len(data) >= 4 and (int.from_bytes(data[:4], "big") & _VERSION_MASK) == _STRICT_BINARY_VERSION


def classify_transport(data: bytes) -> TransportClassification:
    if len(data) >= 3 and data[0] == 0x16 and data[1] == 0x03:
        return TransportClassification("unknown", "unknown", True)
    if len(data) >= 8:
        frame_size = int.from_bytes(data[:4], "big")
        if frame_size == len(data) - 4 and _is_binary_header(data[4:]):
            return TransportClassification("framed", "binary", False)
    if _is_binary_header(data):
        return TransportClassification("unframed", "binary", False)
    if data[:1] == b"\x82":
        return TransportClassification("unknown", "compact", False)
    return TransportClassification("unknown", "unknown", "unknown")
```

`record_loopback.py` binds only `127.0.0.1`, accepts one connection, records at most 4 MiB for at most five seconds, and exits:

```python
from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

MAX_CAPTURE = 4 * 1024 * 1024


async def record(port: int, output: Path) -> None:
    if output.parent != Path("/private/tmp"):
        raise ValueError("capture must be directly under /private/tmp")
    completed = asyncio.Event()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            data = await asyncio.wait_for(reader.read(MAX_CAPTURE + 1), timeout=5)
            if len(data) > MAX_CAPTURE:
                raise ValueError("capture exceeded 4 MiB")
            output.write_bytes(data)
        finally:
            writer.close()
            await writer.wait_closed()
            completed.set()

    server = await asyncio.start_server(handle, "127.0.0.1", port)
    async with server:
        await asyncio.wait_for(completed.wait(), timeout=30)


parser = argparse.ArgumentParser()
parser.add_argument("--port", type=int, required=True)
parser.add_argument("--output", type=Path, required=True)
arguments = parser.parse_args()
asyncio.run(record(arguments.port, arguments.output))
```

`classify_capture.py` reads one path, calls `classify_transport`, and prints `json.dumps(dataclasses.asdict(result), sort_keys=True)`. It never prints or copies captured bytes.

- [ ] **Step 4: Run private synthetic conformance without contacting Brilliant**

Run the operator-owned ARM runtime in QEMU or on a disconnected analysis host. Point the native provisioning client at a loopback recording server using only synthetic IDs, secrets, and a generated test certificate. The server accepts one connection, writes the raw first client frame to `/private/tmp/brilliant-loopback.bin`, and closes; it never forwards traffic. Run `classify_capture.py` to emit sanitized classification JSON.

For the commitment function, `invoke_commitment.py` calls the structurally identified pure callable with three fixed 32-byte synthetic pairs and writes only synthetic input/output hex under `/private/tmp`:

```python
from __future__ import annotations

import argparse
import asyncio
import importlib
import inspect
import json
from collections.abc import Callable
from pathlib import Path

PAIRS = tuple((bytes([left]) * 32, bytes([right]) * 32) for left, right in ((0, 17), (34, 51), (85, 170)))


def resolve(path: str) -> Callable[[bytes, bytes], object]:
    module_name, separator, qualname = path.partition(":")
    if not separator:
        raise ValueError("callable path must be module:qualified.name")
    value: object = importlib.import_module(module_name)
    for part in qualname.split("."):
        value = getattr(value, part)
    if not callable(value):
        raise TypeError(f"{path} is not callable")
    return value


async def invoke(function: Callable[[bytes, bytes], object]) -> list[dict[str, str]]:
    vectors: list[dict[str, str]] = []
    for client, server in PAIRS:
        result = function(client, server)
        if inspect.isawaitable(result):
            result = await result
        if not isinstance(result, bytes) or len(result) != 32:
            raise TypeError("commitment callable must return exactly 32 bytes")
        vectors.append({"client": client.hex(), "server": server.hex(), "output": result.hex()})
    return vectors


parser = argparse.ArgumentParser()
parser.add_argument("callable_path")
parser.add_argument("output", type=Path)
arguments = parser.parse_args()
if arguments.output.parent != Path("/private/tmp"):
    raise SystemExit("output must be directly under /private/tmp")
arguments.output.write_text(
    json.dumps(asyncio.run(invoke(resolve(arguments.callable_path))), indent=2) + "\n",
    encoding="utf-8",
)
```

`classify_commitment.py` compares them with exactly HMAC-SHA256(client, server), SHA256(client || server), and SHA256(server || client):

```python
from __future__ import annotations

import argparse
import hashlib
import hmac
import json
from pathlib import Path


def candidates(client: bytes, server: bytes) -> dict[str, bytes]:
    return {
        "hmac-sha256": hmac.new(client, server, hashlib.sha256).digest(),
        "sha256-client-server": hashlib.sha256(client + server).digest(),
        "sha256-server-client": hashlib.sha256(server + client).digest(),
    }


def classify(vectors: list[dict[str, str]]) -> dict[str, object]:
    matching = set(candidates(b"\0" * 32, b"\1" * 32))
    for vector in vectors:
        client = bytes.fromhex(vector["client"])
        server = bytes.fromhex(vector["server"])
        output = bytes.fromhex(vector["output"])
        if len(client) != 32 or len(server) != 32 or len(output) != 32:
            raise ValueError("every synthetic vector field must be 32 bytes")
        matching.intersection_update(
            name for name, candidate in candidates(client, server).items() if candidate == output
        )
    primitive = next(iter(matching)) if len(matching) == 1 else None
    return {
        "commitment": primitive,
        "hardware_attestation": False if primitive is not None else None,
        "vector_count": len(vectors),
    }


parser = argparse.ArgumentParser()
parser.add_argument("input", type=Path)
arguments = parser.parse_args()
raw = json.loads(arguments.input.read_text(encoding="utf-8"))
if not isinstance(raw, list):
    raise SystemExit("vector input must be a list")
print(json.dumps(classify(raw), sort_keys=True))
```

Record a primitive only if exactly one candidate matches all three vectors. Change `hardware_attestation` back to unknown if structural metadata reveals a device-unique key, certificate, secure element, or hardware input outside the two synthetic byte strings.

If the native client cannot be constructed without real device credentials, if it opens a non-loopback socket, or if it requires a device-unique attestation input, stop the conformance run immediately and preserve that fact as an unknown/blocker. Do not substitute a real panel/account call.

- [ ] **Step 5: Compile, verify, and publish the result**

Run:

```bash
uv run --project protocol_lab python protocol_lab/tools/classify_capture.py \
  /private/tmp/brilliant-loopback.bin \
  > /private/tmp/brilliant-loopback-capture.oracle.local.json
PYTHONPATH=protocol_lab/src \
  "$BRILLIANT_REFERENCE_PYTHON" protocol_lab/tools/invoke_commitment.py \
  "$BRILLIANT_COMMITMENT_CALLABLE" \
  /private/tmp/brilliant-commitment-vectors.oracle.local.json
uv run --project protocol_lab python protocol_lab/tools/classify_commitment.py \
  /private/tmp/brilliant-commitment-vectors.oracle.local.json \
  > /private/tmp/brilliant-commitment.oracle.local.json
uv run --project protocol_lab python protocol_lab/tools/compile_profile.py \
  --structure /private/tmp/brilliant-structure.oracle.local.json \
  --mdns /private/tmp/brilliant-mdns.oracle.local.json \
  --capture /private/tmp/brilliant-loopback-capture.oracle.local.json \
  --commitment /private/tmp/brilliant-commitment.oracle.local.json \
  --removal /private/tmp/brilliant-removal.oracle.local.json \
  --output /private/tmp/brilliant-control.profile.local.json
```

The report contains a table for discovery service/TXT keys, method set, recursive type graph, framing, protocol, TLS, commitment, hardware attestation, and removal path. Each row states `known` or `unknown`, evidence source, and no raw values. Its final line is:

- `GO-PAIRING-PLAN` only when `ProtocolProfile.ready_for_pairing()` is true; or
- `STOP-CLEAN-ROOM` with the exact blocker list.

Run the full lab gate, scan the report for PEM/JWT/32-hex material, then commit only source, synthetic fixtures, and the sanitized report:

```bash
rg -n '-----BEGIN|[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\.|[0-9a-f]{32}' \
  docs/superpowers/research/2026-07-09-synthetic-control-protocol-gate.md
git add protocol_lab/src/brilliant_protocol_lab/capture.py protocol_lab/tools/record_loopback.py protocol_lab/tools/classify_capture.py protocol_lab/tools/invoke_commitment.py protocol_lab/tools/classify_commitment.py protocol_lab/tests/test_capture.py docs/superpowers/research/2026-07-09-synthetic-control-protocol-gate.md
git commit -m "docs(protocol-lab): record synthetic CONTROL protocol gate"
```

Expected: the scan has no matches. A `GO-PAIRING-PLAN` report authorizes writing—not executing—the next plan for guarded enrollment, identity persistence, App packaging, restart, and teardown. It does not authorize a live pairing action. A `STOP-CLEAN-ROOM` report ends this architecture unless new read-only evidence resolves every blocker.
