# Home Assistant Control Plane and Scene Bridge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the unsafe physical-Control HA mirror with one HA-owned, MQTT-only control plane and a safe bidirectional Brilliant scene/mode bridge that creates no hosted peripherals.

**Architecture:** Home Assistant owns entity selection, registry/area resolution, state publication, command validation, service calls, and scene/mode automation dispatch. Every panel agent keeps its existing single bus peer and MQTT connection, observes its own `execution_peripheral`, performs scoped configuration reads for scene/mode catalogs, and writes only `execution_peripheral.last_executed_scene_id` or `execution_peripheral.manual_mode_id`; the old one-host-per-entity mirror stays stopped and is retired after a hardware validation gate.

**Tech Stack:** Python 3.10 panel agent, Python 3.14 / Home Assistant Core 2026.6.2 custom integration, asyncio, HA MQTT integration, JSON over MQTT, Brilliant Thrift TBinaryProtocol decoding, pytest, pytest-homeassistant-custom-component, ruff, mypy strict, uv.

## Global Constraints

- Never create a `PeripheralHost` with `virtual_device_id=None` for mirrored HA entities.
- Never bid on or overwrite ownership of `brilliant_virtual_device`, `configuration_virtual_device`, or `ble_mesh`.
- Never run one framework host per mirrored entity.
- Never exfiltrate panel private keys, PKCS#12 material, Brilliant passwords, MFA codes, bootstrap tokens, account JWTs, or Home Assistant tokens into the repository, MQTT payloads, diagnostics, or logs.
- Keep the existing forward MQTT bridge independent and available throughout the migration.
- `src/` is the source of truth; the bundled agent payload must be generated from it and byte-for-byte parity-tested for non-vendored files.
- Binary dumps, `/var` collections, credentials, generated Ghidra projects, and pilot logs stay under gitignored `artifacts/` paths.
- Agent code remains compatible with Python 3.10; HA integration code targets the pinned Python 3.14 / HA Core 2026.6.2 environment.
- Commands are non-retained, expire after 15 seconds, and are idempotent by command ID; HA state is authoritative.
- Scene execution is confirmed from a new `execution_state:scene_execution_handler:scene:<scene_id>` record, never from the write response alone.
- Execute in an isolated worktree based on commit `dffb67a`; do not absorb the uncommitted Tier-1 experiments currently present in the primary worktree.

---

## File map

### Shared protocol and fixtures

- Create `tests/fixtures/ha_control_v1_vectors.json`: non-secret golden payloads used by both Python projects.
- Create `src/brilliant_mqtt/ha_control_protocol.py`: panel-side topic helpers and strict JSON parsing/encoding.
- Create `custom_components/brilliant_mqtt/ha_control_protocol.py`: HA-side copy of the wire contract; no import from the Python 3.10 package.
- Create `tests/test_ha_control_protocol.py` and `ha/tests/test_ha_control_protocol.py`: enforce identical wire behavior.

### Home Assistant ownership

- Create `custom_components/brilliant_mqtt/ha_control_manifest.py`: label selection, registry-area precedence, capability reduction, stable IDs, state payloads.
- Create `custom_components/brilliant_mqtt/ha_control.py`: singleton lifecycle, retained manifest/state publication, command execution, registry/state listeners, debouncing, status, Repairs.
- Create `custom_components/brilliant_mqtt/scene_control.py`: scene/mode catalog and event subscriptions, HA event/action dispatch, `run_scene`/`set_mode` request confirmation, panel availability.
- Modify `custom_components/brilliant_mqtt/__init__.py`, `const.py`, `config_flow.py`, `manager.py`, `components.py`, `panel_ops.py`, `diagnostics.py`, `select.py`, `button.py`, `services.yaml`, `strings.json`, and `translations/en.json`: lifecycle, configuration, scene entities, migration, legacy retirement.

### Panel transport

- Modify `src/brilliant_mqtt/model.py`, `bus.py`, and `protocols.py`: preserve variable timestamps and expose one scoped peripheral read.
- Create `src/brilliant_mqtt/thrift_binary.py`: bounded generic TBinaryProtocol decoder extracted from the validated research utility.
- Create `src/brilliant_mqtt/scene_codec.py`: typed scene/mode definitions and execution records.
- Create `src/brilliant_mqtt/scene_bridge.py`: deduplication, catalog publishing, command routing, confirmation, status.
- Modify `src/brilliant_mqtt/config.py` and `src/brilliant_mqtt/__main__.py`: enable the bridge on the existing bus/MQTT session.

### Retirement, packaging, and documentation

- Create `src/brilliant_mqtt/cleanup_legacy_mirror.py`: dry-run-first, allowlisted, idempotent legacy peripheral cleanup.
- Create `tests/test_cleanup_legacy_mirror.py` and `tests/test_payload_parity.py`.
- Modify `scripts/build_payload.sh`, `.github/workflows/ci.yml`, `.github/workflows/release.yml`, `.gitignore`, `docs/ha-mirror.md`, and `docs/brilliant-panel/home-assistant-integration.md`.

---

### Task 1: Freeze the versioned MQTT contract

**Files:**
- Create: `tests/fixtures/ha_control_v1_vectors.json`
- Create: `src/brilliant_mqtt/ha_control_protocol.py`
- Create: `custom_components/brilliant_mqtt/ha_control_protocol.py`
- Create: `tests/test_ha_control_protocol.py`
- Create: `ha/tests/test_ha_control_protocol.py`

**Interfaces:**
- Produces: `stable_id(entity_id: str) -> str`, all topic helpers, `decode_command(payload: str, *, now_ms: int) -> EntityCommand`, `decode_scene_command(payload: str, *, now_ms: int) -> SceneCommand`, `decode_mode_command(payload: str, *, now_ms: int) -> ModeCommand`, and canonical `encode_json(value: Mapping[str, object]) -> str`.
- Wire constants: `SCHEMA_VERSION = 1`, `MAPPING_VERSION = 1`, namespace UUID `ddd06dfa-168a-5a0b-b8b3-4c5f742b0354`, command TTL `15_000` ms.

- [ ] **Step 1: Add golden vectors and failing tests in both projects**

Use this first vector verbatim; add corresponding vectors for entity command, result, scene/mode catalogs, scene/mode events, scene/mode commands, scene/mode results, and transport status with fixed timestamps and sorted JSON keys:

```json
{
  "stable_ids": {
    "light.office_lamp": "d353e38a-793e-5b6f-813b-17a1c38aba96"
  },
  "topics": {
    "manifest": "brilliant/ha-control/v1/manifest",
    "state": "brilliant/ha-control/v1/state/d353e38a-793e-5b6f-813b-17a1c38aba96",
    "command": "brilliant/ha-control/v1/command/d353e38a-793e-5b6f-813b-17a1c38aba96",
    "result": "brilliant/ha-control/v1/result/11111111-1111-4111-8111-111111111111",
    "scene_catalog": "brilliant/ha-control/v1/scene/catalog/office",
    "scene_event": "brilliant/ha-control/v1/scene/event/office",
    "scene_command": "brilliant/ha-control/v1/scene/command/office",
    "scene_result": "brilliant/ha-control/v1/scene/result/11111111-1111-4111-8111-111111111111",
    "scene_status": "brilliant/ha-control/v1/status/scene/office",
    "mode_catalog": "brilliant/ha-control/v1/mode/catalog/office",
    "mode_event": "brilliant/ha-control/v1/mode/event/office",
    "mode_command": "brilliant/ha-control/v1/mode/command/office",
    "mode_result": "brilliant/ha-control/v1/mode/result/11111111-1111-4111-8111-111111111111"
  }
}
```

Each test loads the same fixture path and asserts stable IDs, topic strings, rejection of retained/expired/malformed commands, and canonical JSON. The HA path is `Path(__file__).parents[2] / "tests/fixtures/ha_control_v1_vectors.json"`.

- [ ] **Step 2: Run the two targeted suites and verify they fail**

Run: `uv run pytest tests/test_ha_control_protocol.py -q`

Expected: FAIL during import because `brilliant_mqtt.ha_control_protocol` does not exist.

Run: `uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_ha_control_protocol.py -q`

Expected: FAIL during import because the HA protocol module does not exist.

- [ ] **Step 3: Implement the protocol in both runtimes**

Both files expose the same public surface. Use dataclasses with exact fields and reject unknown schema/mapping versions, missing IDs, mismatched topic stable IDs, timestamps more than 5 seconds in the future, and commands older than 15 seconds.

```python
SCHEMA_VERSION = 1
MAPPING_VERSION = 1
COMMAND_TTL_MS = 15_000
_STABLE_NAMESPACE = UUID("ddd06dfa-168a-5a0b-b8b3-4c5f742b0354")

def stable_id(entity_id: str) -> str:
    return str(uuid5(_STABLE_NAMESPACE, entity_id))

def encode_json(value: Mapping[str, object]) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)

@dataclass(frozen=True)
class EntityCommand:
    command_id: str
    stable_id: str
    kind: str
    value: object
    observed_sequence: int
    issued_at_ms: int

@dataclass(frozen=True)
class SceneCommand:
    command_id: str
    panel: str
    scene_id: str
    issued_at_ms: int

@dataclass(frozen=True)
class ModeCommand:
    command_id: str
    panel: str
    mode_id: str
    issued_at_ms: int
```

Topic helpers must percent-free validate slugs/UUIDs with `re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,62}", panel)` and `UUID(value)` before interpolation. Scene topics are the locked extension to the namespace table in the design spec.

- [ ] **Step 4: Run both protocol suites**

Expected: both commands PASS, including exact canonical payload comparisons.

- [ ] **Step 5: Commit**

```bash
git add tests/fixtures/ha_control_v1_vectors.json tests/test_ha_control_protocol.py ha/tests/test_ha_control_protocol.py src/brilliant_mqtt/ha_control_protocol.py custom_components/brilliant_mqtt/ha_control_protocol.py
git commit -m "feat: define HA control MQTT v1 contract"
```

### Task 2: Build manifests and state exclusively inside Home Assistant

**Files:**
- Create: `custom_components/brilliant_mqtt/ha_control_manifest.py`
- Create: `ha/tests/test_ha_control_manifest.py`

**Interfaces:**
- Consumes: `stable_id`, `SCHEMA_VERSION`, and `MAPPING_VERSION` from Task 1.
- Produces: `ControlSettings`, `ManifestEntity`, `ManifestSnapshot`, `build_manifest(hass, settings, revision, generated_at_ms)`, and `build_state_payload(state, entity, sequence, generated_at_ms)`.

- [ ] **Step 1: Write failing registry/capability tests**

Cover these exact cases with real HA test registries:

```python
async def test_entity_area_precedes_device_area(hass: HomeAssistant) -> None:
    # label the entity, set entity area to Office and device area to Backyard
    snapshot = build_manifest(hass, settings(label_name="brilliant"), 7, 1_700_000_000_000)
    assert snapshot.entities[0].ha_area == "Office"
    assert snapshot.entities[0].brilliant_room == "Office"

async def test_unmatched_override_is_case_insensitive(hass: HomeAssistant) -> None:
    snapshot = build_manifest(
        hass,
        settings(label_name="brilliant", room_overrides={"back yard": "Backyard"}),
        1,
        1_700_000_000_000,
    )
    assert snapshot.entities[0].brilliant_room == "Backyard"
```

Also assert: entity labels select; device labels do not implicitly select; disabled/unavailable/missing entities remain in manifest with availability state; unsupported domains are reported but excluded; max count truncates deterministically by entity ID; light brightness, cover position/tilt, and lock commands are reduced from current state/support flags.

- [ ] **Step 2: Run and verify failure**

Run: `uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_ha_control_manifest.py -q`

Expected: FAIL because the manifest module is absent.

- [ ] **Step 3: Implement immutable manifest types and registry precedence**

```python
SUPPORTED_DOMAINS = frozenset({"light", "switch", "lock", "cover"})

@dataclass(frozen=True, slots=True)
class ControlSettings:
    label_name: str
    room_overrides: Mapping[str, str]
    enabled_domains: frozenset[str]
    maximum_entities: int

@dataclass(frozen=True, slots=True)
class ManifestEntity:
    stable_id: str
    entity_id: str
    domain: str
    device_class: str | None
    friendly_name: str
    ha_area: str | None
    brilliant_room: str | None
    commands: tuple[str, ...]
    capabilities: Mapping[str, bool]

def _area_name(entity: er.RegistryEntry, entities: er.EntityRegistry,
               devices: dr.DeviceRegistry, areas: ar.AreaRegistry) -> str | None:
    area_id = entity.area_id
    if area_id is None and entity.device_id is not None:
        device = devices.async_get(entity.device_id)
        area_id = device.area_id if device is not None else None
    area = areas.async_get_area(area_id) if area_id is not None else None
    return area.name if area is not None else None
```

`build_manifest` resolves `label_registry.async_get_label_by_name`, selects registry entries whose `labels` contains its ID, sorts by `entity_id`, derives commands from domain and supported features, and emits a complete JSON-ready snapshot. Normalize override keys with `casefold().strip()`.

- [ ] **Step 4: Implement normalized state payloads**

Keep only `brightness`, `current_position`, `current_tilt_position`, `device_class`, and `supported_features`; do not forward arbitrary attributes.

```python
def build_state_payload(state: State | None, entity: ManifestEntity,
                        sequence: int, generated_at_ms: int) -> dict[str, object]:
    return {
        "schema_version": SCHEMA_VERSION,
        "mapping_version": MAPPING_VERSION,
        "stable_id": entity.stable_id,
        "entity_id": entity.entity_id,
        "sequence": sequence,
        "generated_at_ms": generated_at_ms,
        "available": state is not None and state.state not in {STATE_UNAVAILABLE, STATE_UNKNOWN},
        "state": state.state if state is not None else STATE_UNAVAILABLE,
        "attributes": _supported_attributes(state),
    }
```

- [ ] **Step 5: Run manifest tests and the HA type/lint gate**

Run: `uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_ha_control_manifest.py -q`

Expected: PASS.

Run: `uv run --project ha ruff check --config ha/pyproject.toml custom_components/brilliant_mqtt/ha_control_manifest.py ha/tests/test_ha_control_manifest.py && uv run --project ha mypy --strict --config-file ha/pyproject.toml custom_components/brilliant_mqtt/ha_control_manifest.py ha/tests/test_ha_control_manifest.py`

Expected: both exit 0.

- [ ] **Step 6: Commit**

```bash
git add custom_components/brilliant_mqtt/ha_control_manifest.py ha/tests/test_ha_control_manifest.py
git commit -m "feat: build HA-owned control manifests"
```

### Task 3: Publish the singleton HA control plane and execute commands

**Files:**
- Create: `custom_components/brilliant_mqtt/ha_control.py`
- Create: `ha/tests/test_ha_control.py`
- Modify: `custom_components/brilliant_mqtt/__init__.py`
- Modify: `custom_components/brilliant_mqtt/const.py`
- Modify: `ha/tests/test_init.py`

**Interfaces:**
- Consumes: Task 1 protocol and Task 2 manifest builder.
- Produces: `HaControlPlane.async_attach(entry)`, `async_detach(entry_id)`, `async_reload_settings()`, `async_start()`, `async_stop()`, and `get_control_plane(hass)`.
- Singleton key: `hass.data[DOMAIN][DATA_CONTROL_PLANE]`; owner is the enabled loaded entry with lexicographically smallest panel slug.

- [ ] **Step 1: Write failing lifecycle/publication tests**

Use two config entries and the MQTT mock. Assert exactly one subscription to `brilliant/ha-control/v1/command/+`, one retained manifest publication, and one retained state publication per selected entity. Assert the singleton survives unloading one entry and stops/unsubscribes when the last entry unloads. Fire entity/device/area/label registry update events and verify one debounced manifest rebuild after 500 ms.

- [ ] **Step 2: Write failing command tests**

For each vocabulary item, send a valid MQTT command and assert the exact HA service call:

```python
COMMAND_CASES = (
    ("turn_on", None, "light", "turn_on", {}),
    ("set_brightness", 128, "light", "turn_on", {"brightness": 128}),
    ("turn_off", None, "switch", "turn_off", {}),
    ("lock", None, "lock", "lock", {}),
    ("unlock", None, "lock", "unlock", {}),
    ("open", None, "cover", "open_cover", {}),
    ("close", None, "cover", "close_cover", {}),
    ("set_position", 42, "cover", "set_cover_position", {"position": 42}),
    ("set_tilt", 25, "cover", "set_cover_tilt_position", {"tilt_position": 25}),
)
```

Assert expired commands, stable-ID mismatches, commands absent from the current manifest, duplicate IDs, and invalid ranges never call a service and publish an error result. Duplicate IDs replay the same result without another service call. Result cache holds 1,024 entries for 10 minutes.

- [ ] **Step 3: Run and verify failure**

Run: `uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_ha_control.py ha/tests/test_init.py -q`

Expected: FAIL because the coordinator and constants are absent.

- [ ] **Step 4: Implement the coordinator lifecycle**

Register listeners for `EVENT_STATE_CHANGED`, `EVENT_ENTITY_REGISTRY_UPDATED`, `EVENT_DEVICE_REGISTRY_UPDATED`, `EVENT_AREA_REGISTRY_UPDATED`, and `EVENT_LABEL_REGISTRY_UPDATED`. State changes for selected entity IDs publish immediately; registry changes schedule one 500 ms rebuild with `async_call_later`. Increment revision only when the canonical manifest body changes.

```python
class HaControlPlane:
    def __init__(self, hass: HomeAssistant) -> None:
        self.hass = hass
        self._entries: dict[str, BrilliantMqttConfigEntry] = {}
        self._manifest: ManifestSnapshot | None = None
        self._state_sequences: defaultdict[str, int] = defaultdict(int)
        self._unsubscribers: list[Callable[[], None]] = []
        self._started = False

    async def async_attach(self, entry: BrilliantMqttConfigEntry) -> None:
        self._entries[entry.entry_id] = entry
        if not self._started:
            await self.async_start()
        else:
            await self.async_reload_settings()
```

In `async_setup_entry`, attach after MQTT setup succeeds and before forwarding platforms. In unload, detach after platforms unload but before clearing `runtime_data`.

- [ ] **Step 5: Implement command validation and HA service routing**

Use a closed dispatch table, validate integer ranges (`brightness` 0–255; cover values 0–100), call `hass.services.async_call(domain, service, service_data, blocking=True)`, and publish a non-retained result containing `accepted`, `error`, `state_sequence`, and elapsed milliseconds. Never include service exception tracebacks in MQTT; log exception class and a sanitized message.

- [ ] **Step 6: Run lifecycle, command, and full HA tests**

Run: `uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_ha_control.py ha/tests/test_init.py -q`

Expected: PASS.

Run: `uv run --project ha pytest -c ha/pyproject.toml ha/tests -q`

Expected: PASS with no new lingering-task warnings.

- [ ] **Step 7: Commit**

```bash
git add custom_components/brilliant_mqtt/ha_control.py custom_components/brilliant_mqtt/__init__.py custom_components/brilliant_mqtt/const.py ha/tests/test_ha_control.py ha/tests/test_init.py
git commit -m "feat: publish the HA MQTT control plane"
```

### Task 4: Preserve bus timestamps and add one scoped configuration read

**Files:**
- Modify: `src/brilliant_mqtt/model.py`
- Modify: `src/brilliant_mqtt/bus.py`
- Modify: `src/brilliant_mqtt/protocols.py`
- Modify: `tests/fakes.py`
- Modify: `tests/test_bus_normalize.py`
- Modify: `tests/test_bus_adapter.py`
- Modify: `tests/test_fakes.py`

**Interfaces:**
- Changes `Variable` to `Variable(name: str, value: str, externally_settable: bool = False, timestamp_ms: int | None = None)`.
- Adds `BusClient.get_peripheral(device_id: str, peripheral_id: str) -> BrilliantDevice | None`.

- [ ] **Step 1: Write failing normalization and scoped-read tests**

Assert `normalize_peripheral` converts integer timestamps, tolerates missing/invalid timestamps as `None`, and `RpcBusAdapter.get_peripheral("configuration_virtual_device", "scene_configuration")` calls only `obs.get_peripheral`, not `get_all` or `get_device`.

- [ ] **Step 2: Run and verify failure**

Run: `uv run pytest tests/test_bus_normalize.py tests/test_bus_adapter.py tests/test_fakes.py -q`

Expected: FAIL on missing `timestamp_ms` and `get_peripheral`.

- [ ] **Step 3: Implement the model and adapter changes**

```python
raw_timestamp = getattr(raw_var, "timestamp", None)
timestamp_ms = int(raw_timestamp) if isinstance(raw_timestamp, (int, float)) else None
variables[var_name] = Variable(
    name=var_name,
    value=str(value),
    externally_settable=bool(raw_var.externally_settable),
    timestamp_ms=timestamp_ms,
)

async def get_peripheral(self, device_id: str, peripheral_id: str) -> BrilliantDevice | None:
    obs, _ = self._require_started()
    raw = await obs.get_peripheral(device_id, peripheral_id)
    if raw is None:
        return None
    return normalize_peripheral(device_id, peripheral_id, raw)
```

The new read is on-demand only. Do not add `configuration_virtual_device` to `_extra_device_ids`, subscriptions, or the hot `get_all()` loop.

- [ ] **Step 4: Run the targeted and full agent suites**

Expected: `uv run pytest tests/test_bus_normalize.py tests/test_bus_adapter.py tests/test_fakes.py -q` PASS, then `uv run pytest -q` PASS.

- [ ] **Step 5: Commit**

```bash
git add src/brilliant_mqtt/model.py src/brilliant_mqtt/bus.py src/brilliant_mqtt/protocols.py tests/fakes.py tests/test_bus_normalize.py tests/test_bus_adapter.py tests/test_fakes.py
git commit -m "feat: expose scoped scene configuration reads"
```

### Task 5: Decode scene catalogs and execution records off-panel

**Files:**
- Create: `src/brilliant_mqtt/thrift_binary.py`
- Create: `src/brilliant_mqtt/scene_codec.py`
- Create: `tests/test_thrift_binary.py`
- Create: `tests/test_scene_codec.py`
- Create: `tests/fixtures/scene_all_off.json`
- Create: `tests/fixtures/scene_execution_all_off.json`

**Interfaces:**
- Produces: `decode_struct_base64(value: str, *, max_bytes=262_144, max_depth=16, max_items=10_000) -> dict[int, object]`.
- Produces: `SceneDefinition(scene_id, display_name, icon)`, `SceneExecution(scene_id, executed_at_ms, payload_sha256)`, `ModeDefinition(mode_id, display_name)`, `ModeExecution(mode_id, executed_at_ms)`, `decode_scene_catalog(device)`, `decode_mode_catalog(device)`, and `decode_scene_execution(device)`.

- [ ] **Step 1: Extract redacted fixtures and write failing tests**

Copy only the two already committed `all_off` base64 values from `docs/claude/research/2026-07-06-mirror-poc/out/baseline.json`; store the expected decoded values, not any device credentials. Assert field 1/2/3 decode to `all_off`, `All Lights Off`, and its qrc icon. Assert the execution record returns timestamp `1683501714715` and scene ID parsed from the variable name.

Also test invalid base64, truncated values, negative collection sizes, depth/item/byte limits, non-scene variables, mismatch between variable timestamp and embedded execution timestamp, synthetic `mode:<id>` definitions, and timestamped `manual_mode_id` changes. The embedded field-1 scene timestamp is authoritative; the bus timestamp is diagnostic. An empty `manual_mode_id` is not an execution event.

- [ ] **Step 2: Run and verify failure**

Run: `uv run pytest tests/test_thrift_binary.py tests/test_scene_codec.py -q`

Expected: FAIL because both modules are absent.

- [ ] **Step 3: Implement a bounded TBinaryProtocol decoder**

Port the validated primitive readers from `decode_scenes.py`, but use a cursor object that checks every read and decrements item/depth budgets before allocation. Public errors are `ThriftDecodeError`; never log raw blobs.

```python
class ThriftDecodeError(ValueError):
    pass

def decode_struct_base64(value: str, *, max_bytes: int = 262_144,
                         max_depth: int = 16, max_items: int = 10_000) -> dict[int, object]:
    try:
        raw = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ThriftDecodeError("invalid base64 thrift value") from exc
    if len(raw) > max_bytes:
        raise ThriftDecodeError("thrift value exceeds byte limit")
    cursor = _Cursor(raw, max_depth=max_depth, max_items=max_items)
    result = cursor.read_struct(depth=0)
    if cursor.position != len(raw):
        raise ThriftDecodeError("trailing bytes after thrift struct")
    return result
```

- [ ] **Step 4: Implement typed scene reduction**

Only expose IDs, display names, icons, execution timestamp, and SHA-256; do not publish the action/device list from execution blobs. Decode optional `mode:<id>` values from `mode_configuration`; when no modes are configured, publish an empty mode catalog rather than inventing defaults. Treat a non-empty `execution_peripheral.manual_mode_id` update as a mode execution keyed by its bus variable timestamp.

```python
_SCENE_PREFIX = "execution_state:scene_execution_handler:scene:"

def decode_scene_execution(device: BrilliantDevice) -> tuple[SceneExecution, ...]:
    records: list[SceneExecution] = []
    if device.peripheral_id != "execution_peripheral":
        return ()
    for name, variable in device.variables.items():
        if not name.startswith(_SCENE_PREFIX):
            continue
        decoded = decode_struct_base64(variable.value)
        executed_at_ms = decoded.get(1)
        if not isinstance(executed_at_ms, int):
            raise SceneCodecError("scene execution is missing its timestamp")
        records.append(SceneExecution(
            scene_id=name.removeprefix(_SCENE_PREFIX),
            executed_at_ms=executed_at_ms,
            payload_sha256=hashlib.sha256(variable.value.encode()).hexdigest(),
        ))
    return tuple(sorted(records, key=lambda item: (item.executed_at_ms, item.scene_id)))
```

- [ ] **Step 5: Run codec tests and quality checks**

Run: `uv run pytest tests/test_thrift_binary.py tests/test_scene_codec.py -q`

Expected: PASS.

Run: `uv run ruff check src/brilliant_mqtt/thrift_binary.py src/brilliant_mqtt/scene_codec.py tests/test_thrift_binary.py tests/test_scene_codec.py && uv run mypy --strict src/brilliant_mqtt/thrift_binary.py src/brilliant_mqtt/scene_codec.py tests/test_thrift_binary.py tests/test_scene_codec.py`

Expected: exit 0.

- [ ] **Step 6: Commit**

```bash
git add src/brilliant_mqtt/thrift_binary.py src/brilliant_mqtt/scene_codec.py tests/test_thrift_binary.py tests/test_scene_codec.py tests/fixtures/scene_all_off.json tests/fixtures/scene_execution_all_off.json
git commit -m "feat: decode Brilliant scene records safely"
```

### Task 6: Implement the panel scene bridge on the existing session

**Files:**
- Create: `src/brilliant_mqtt/scene_bridge.py`
- Create: `tests/test_scene_bridge.py`
- Modify: `tests/fakes.py`

**Interfaces:**
- Consumes: `BusClient`, `MqttClient`, Task 1 scene contract, Task 5 codecs.
- Produces: `SceneBridge(bus, mqtt, panel, watermark_path, clock_ms)`, `async_start()`, `async_reconcile()`, `async_shutdown()`.

- [ ] **Step 1: Write failing event, replay, catalog, and command tests**

Test all of the following with `FakeBus`/`FakeMqtt`:

- initial retained records seed the watermark and do not fire as new events;
- a later embedded timestamp publishes one non-retained scene event;
- identical replay after reconnect/process restart is suppressed from the persisted watermark file;
- scene and mode catalogs are read with two scoped calls to `get_peripheral("configuration_virtual_device", "scene_configuration")` and `get_peripheral("configuration_virtual_device", "mode_configuration")` at start and reconnect, then published retained;
- a valid scene command writes only `last_executed_scene_id` on the own device's `execution_peripheral`; a valid mode command writes only `manual_mode_id` there;
- a matching later execution record publishes accepted result; no record within 15 seconds publishes timeout;
- expired/duplicate/unknown-scene commands never write;
- malformed blobs publish degraded status without killing the forward bridge callback fan-out.

- [ ] **Step 2: Run and verify failure**

Run: `uv run pytest tests/test_scene_bridge.py -q`

Expected: FAIL because `SceneBridge` does not exist.

- [ ] **Step 3: Implement persistent deduplication and catalog publication**

Persist only the newest timestamp and hash per `(panel, scene_id)` using atomic temp-file replace and mode `0o600`.

```python
@dataclass(frozen=True, slots=True)
class Watermark:
    executed_at_ms: int
    payload_sha256: str

def _is_new(previous: Watermark | None, current: SceneExecution) -> bool:
    return previous is None or (current.executed_at_ms, current.payload_sha256) > (
        previous.executed_at_ms, previous.payload_sha256
    )
```

`async_start()` registers the bus callback before I/O, subscribes to its exact panel scene and mode command topics, reconciles the current execution peripheral without emitting history, reads both catalogs once, and publishes retained status. It never subscribes to the configuration device.

- [ ] **Step 4: Implement command write and confirmation**

Track scene and mode pending requests separately; after a write, schedule a 15-second timeout task. On a new matching event, publish the event first, then the accepted result, cancel timeout, and cache the result for idempotent replay.

```python
await self._bus.set_variables(
    execution.device_id,
    "execution_peripheral",
    [VarSet(name="last_executed_scene_id", value=command.scene_id)],
)

await self._bus.set_variables(
    execution.device_id,
    "execution_peripheral",
    [VarSet(name="manual_mode_id", value=command.mode_id)],
)
```

The command write response is not success. If `set_variables` raises, publish an immediate sanitized error result. On shutdown, cancel timeouts, unsubscribe the command topic, and flush the watermark.

- [ ] **Step 5: Run bridge tests and the full agent suite**

Run: `uv run pytest tests/test_scene_bridge.py -q`

Expected: PASS.

Run: `uv run pytest -q`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/brilliant_mqtt/scene_bridge.py tests/test_scene_bridge.py tests/fakes.py
git commit -m "feat: bridge Brilliant scenes over MQTT"
```

### Task 7: Wire the scene bridge without adding a bus peer

**Files:**
- Modify: `src/brilliant_mqtt/config.py`
- Modify: `src/brilliant_mqtt/__main__.py`
- Modify: `tests/test_config.py`
- Modify: `tests/test_main.py`

**Interfaces:**
- Adds settings: `scene_bridge_enabled: bool` from `SCENE_BRIDGE_ENABLED` (default false) and `scene_watermark_file: str` from `SCENE_WATERMARK_FILE` (default `/data/brilliant-mqtt/scene-watermarks.json`).

- [ ] **Step 1: Write failing settings and session-wiring tests**

Assert false/true parsing, default path, and that enabled sessions construct `SceneBridge` with the exact existing `bus` and `mqtt` objects. Assert call order: callbacks constructed → MQTT connect → bus start → `SceneBridge.async_start`; teardown order: scene bridge shutdown → bus shutdown → MQTT disconnect. Assert disabled mode never subscribes to scene topics.

- [ ] **Step 2: Run and verify failure**

Run: `uv run pytest tests/test_config.py tests/test_main.py -q`

Expected: FAIL on missing settings/wiring.

- [ ] **Step 3: Implement settings and session composition**

```python
scene_bridge = (
    SceneBridge(
        bus,
        mqtt,
        settings.panel,
        Path(settings.scene_watermark_file),
    )
    if settings.scene_bridge_enabled
    else None
)
```

The bridge is an additional consumer of the existing adapters, just like `Bridge` and `MeshLeader`; do not instantiate `RpcBusAdapter`, `AioMqttAdapter`, `RPCObserver`, or `PeripheralHost` inside it.

- [ ] **Step 4: Run targeted and complete agent quality gates**

Run: `uv run pytest tests/test_config.py tests/test_main.py -q`

Expected: PASS.

Run: `uv run ruff check && uv run ruff format --check && uv run mypy --strict src tests && uv run pytest`

Expected: every command exits 0.

- [ ] **Step 5: Commit**

```bash
git add src/brilliant_mqtt/config.py src/brilliant_mqtt/__main__.py tests/test_config.py tests/test_main.py
git commit -m "feat: enable scene transport on the shared panel session"
```

### Task 8: Add HA scene events, actions, service confirmation, and scene entities

**Files:**
- Create: `custom_components/brilliant_mqtt/scene_control.py`
- Create: `ha/tests/test_scene_control.py`
- Modify: `custom_components/brilliant_mqtt/ha_control.py`
- Modify: `custom_components/brilliant_mqtt/select.py`
- Modify: `custom_components/brilliant_mqtt/button.py`
- Modify: `custom_components/brilliant_mqtt/services.yaml`
- Modify: `custom_components/brilliant_mqtt/strings.json`
- Modify: `custom_components/brilliant_mqtt/translations/en.json`
- Modify: `ha/tests/test_entities.py`
- Modify: `ha/tests/test_services.py`

**Interfaces:**
- Produces HA events `brilliant_mqtt_scene` and `brilliant_mqtt_mode` with panel, activation ID, timestamp, and deduplication key.
- Registers services `brilliant_mqtt.run_scene(panel: str | None, scene_id: str)` and `brilliant_mqtt.set_mode(panel: str | None, mode_id: str)`, each waiting up to 16 seconds for result.
- Produces per-panel `SceneSelect` and `RunSelectedSceneButton`; select options are display names and internally map to scene IDs.

- [ ] **Step 1: Write failing scene runtime tests**

Assert wildcard subscriptions to scene/mode catalog/event/result/status, retained catalog replacement, stale/out-of-order event suppression, both HA event types, configured action dispatch, selected-panel defaulting, explicit panel override, offline/unknown activation rejection, accepted confirmation, timeout, and unload cleanup. Action mappings use this closed JSON shape:

```json
{
  "office:all_off": {
    "domain": "scene",
    "service": "turn_on",
    "target": {"entity_id": ["scene.downstairs_off"]},
    "data": {}
  }
}
```

Reject service names not matching `^[a-z0-9_]+$`, target keys other than `entity_id`, `device_id`, and `area_id`, and mapping keys without exactly one colon.

- [ ] **Step 2: Write failing entity/service tests**

After a catalog message with `all_off` and `all_on`, assert the select options are `All Lights Off`, `All Lights On`; selecting an option updates only the local selection, and pressing the scene button calls `brilliant_mqtt.run_scene` with the selected ID. The service schema rejects missing scene ID and unknown panel.

- [ ] **Step 3: Run and verify failure**

Run: `uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_scene_control.py ha/tests/test_entities.py ha/tests/test_services.py -q`

Expected: FAIL because scene control and entities are absent.

- [ ] **Step 4: Implement scene runtime and service confirmation**

```python
async def async_run_scene(self, panel: str, scene_id: str) -> None:
    catalog = self._catalogs.get(panel)
    if catalog is None or scene_id not in catalog.by_id:
        raise HomeAssistantError(f"Scene {scene_id} is not available on panel {panel}")
    command_id = str(uuid4())
    future = self.hass.loop.create_future()
    self._pending[command_id] = future
    command = SceneCommand(
        command_id=command_id,
        panel=panel,
        scene_id=scene_id,
        issued_at_ms=int(time.time() * 1000),
    )
    async_publish(
        self.hass,
        scene_command_topic(panel),
        encode_scene_command(command),
        retain=False,
    )
    try:
        result = await asyncio.wait_for(future, timeout=16)
    finally:
        self._pending.pop(command_id, None)
    if not result.accepted:
        raise HomeAssistantError(result.error or "Brilliant scene execution failed")
```

Implement `async_set_mode` symmetrically using the mode catalog/topic/result and `manual_mode_id` confirmation. MQTT callbacks must parse defensively, log no raw payload on failure, and keep running. Fire the HA event before the optional action so automation observers always see it. Use `hass.services.async_call` with `blocking=False` for mapped actions to avoid deadlocking the MQTT callback.

- [ ] **Step 5: Implement scene select/button entities and translations**

Attach them to the existing MQTT panel device. Set scene entities unavailable unless scene status is online and the catalog is non-empty. Add translation keys `scene`, `run_selected_scene`, and service field descriptions; do not label native tiles as available.

- [ ] **Step 6: Run scene and full HA gates**

Run: `uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_scene_control.py ha/tests/test_entities.py ha/tests/test_services.py -q`

Expected: PASS.

Run: `uv run --project ha ruff check --config ha/pyproject.toml custom_components/brilliant_mqtt ha/tests && uv run --project ha ruff format --check --config ha/pyproject.toml custom_components/brilliant_mqtt ha/tests && uv run --project ha mypy --strict --config-file ha/pyproject.toml custom_components/brilliant_mqtt ha/tests && uv run --project ha pytest -c ha/pyproject.toml ha/tests`

Expected: every command exits 0.

- [ ] **Step 7: Commit**

```bash
git add custom_components/brilliant_mqtt/scene_control.py custom_components/brilliant_mqtt/ha_control.py custom_components/brilliant_mqtt/select.py custom_components/brilliant_mqtt/button.py custom_components/brilliant_mqtt/services.yaml custom_components/brilliant_mqtt/strings.json custom_components/brilliant_mqtt/translations/en.json ha/tests/test_scene_control.py ha/tests/test_entities.py ha/tests/test_services.py
git commit -m "feat: expose confirmed Brilliant scenes in Home Assistant"
```

### Task 9: Add the configuration vertical slice and retire Tier 1 safely

**Files:**
- Modify: `custom_components/brilliant_mqtt/const.py`
- Modify: `custom_components/brilliant_mqtt/config_flow.py`
- Modify: `custom_components/brilliant_mqtt/components.py`
- Modify: `custom_components/brilliant_mqtt/manager.py`
- Modify: `custom_components/brilliant_mqtt/panel_ops.py`
- Modify: `custom_components/brilliant_mqtt/diagnostics.py`
- Modify: `custom_components/brilliant_mqtt/strings.json`
- Modify: `custom_components/brilliant_mqtt/translations/en.json`
- Modify: `ha/tests/test_config_flow.py`
- Modify: `ha/tests/test_components.py`
- Modify: `ha/tests/test_manager.py`
- Modify: `ha/tests/test_panel_ops.py`
- Modify: `ha/tests/test_diagnostics.py`
- Modify: `ha/tests/test_repairs.py`

**Interfaces:**
- Adds `CONF_HA_CONTROL_ENABLED`, `CONF_HA_CONTROL_LABEL`, `CONF_ROOM_OVERRIDES`, `CONF_HA_CONTROL_DOMAINS`, `CONF_MAX_MIRRORED_ENTITIES`, `CONF_SCENE_PANEL`, and `CONF_SCENE_ACTIONS`.
- Config-entry migration copies old `CONF_HA_MIRROR_LABEL`, disables `COMPONENT_HA_MIRROR`, removes URL/token/leader fields only after the control plane is enabled, and preserves unrelated component choices.

- [ ] **Step 1: Write failing config/migration tests**

Use defaults: control disabled, label `brilliant`, domains `light,switch`, maximum 50, empty overrides/actions, selected panel equal to current panel. Validate maximum 1–200; domains subset of `light/switch/lock/cover`; overrides and actions are JSON objects; panel is one of loaded entries. Reconfigure must propagate global control fields to every Brilliant entry while leaving per-panel SSH/MQTT fields unchanged.

Test migration from the current entry version with HA mirror enabled: mirror becomes false, label is copied, old token remains only while control plane is disabled; after enabling control plane and successful manager apply, token/URL/leader keys are removed.

- [ ] **Step 2: Write failing manager/env/repair/diagnostic tests**

Assert `SCENE_BRIDGE_ENABLED=1` is rendered on every panel when globally enabled; disabled renders `0`. Assert the old mirror unit is stopped/disabled and its env file removed when legacy mirror is selected in old data. Assert a Repair issue `ha_mirror_retired_<entry_id>` explains physical-Control hosting was disabled for responsiveness safety. Assert diagnostics expose label, override count, manifest revision/entity count, scene catalog revision/last event, and blocked native status—but redact mappings' service data and all old tokens.

- [ ] **Step 3: Run and verify failure**

Run: `uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_config_flow.py ha/tests/test_components.py ha/tests/test_manager.py ha/tests/test_panel_ops.py ha/tests/test_diagnostics.py ha/tests/test_repairs.py -q`

Expected: FAIL on missing fields and retirement behavior.

- [ ] **Step 4: Implement validated fields and all-entry propagation**

Store overrides/actions as decoded dictionaries in entry data, not raw JSON strings. The form accepts JSON text, validates it, then persists canonical mappings. When a global setting changes, loop over `hass.config_entries.async_entries(DOMAIN)` and `async_update_entry` each with the same seven global keys.

`panel_ops.render_env` gains only `scene_bridge_enabled`; HA registry/label/room/action data never goes to the panel. This enforces MQTT-only panels.

- [ ] **Step 5: Hide and disable the legacy component while keeping uninstall support**

Add `deprecated: bool = False` to `Component`; set true for `COMPONENT_HA_MIRROR`; `optional()` excludes deprecated rows. `selected_ids()` excludes it even when old data says true, so reconciliation invokes `uninstall_ha_mirror`. Keep the registry row until Task 12 so old installs can be removed idempotently.

Create the Repair before removal and delete it after `inspect_ha_mirror` proves service inactive and env/token files absent. A removal failure keeps the Repair open and never starts the legacy service.

- [ ] **Step 6: Run targeted and full HA gates**

Expected: targeted command from Step 3 PASS, then all four HA quality commands from Task 8 PASS.

- [ ] **Step 7: Commit**

```bash
git add custom_components/brilliant_mqtt/const.py custom_components/brilliant_mqtt/config_flow.py custom_components/brilliant_mqtt/components.py custom_components/brilliant_mqtt/manager.py custom_components/brilliant_mqtt/panel_ops.py custom_components/brilliant_mqtt/diagnostics.py custom_components/brilliant_mqtt/strings.json custom_components/brilliant_mqtt/translations/en.json ha/tests/test_config_flow.py ha/tests/test_components.py ha/tests/test_manager.py ha/tests/test_panel_ops.py ha/tests/test_diagnostics.py ha/tests/test_repairs.py
git commit -m "feat: migrate HA mirror settings to the safe control plane"
```

### Task 10: Add dry-run-first legacy peripheral cleanup

**Files:**
- Create: `src/brilliant_mqtt/cleanup_legacy_mirror.py`
- Create: `tests/test_cleanup_legacy_mirror.py`

**Interfaces:**
- CLI: `python -m brilliant_mqtt.cleanup_legacy_mirror [--apply] [--snapshot PATH]`.
- Candidate requires both an allowlisted ID prefix (`ha_`, `ha-pilot-`, `zzz_mirror_`) and an allowlisted display-name prefix (`HA `, `HA_PILOT_`, `ZZZ Mirror `); no match on either side means no deletion.

- [ ] **Step 1: Write failing candidate, dry-run, apply, and verification tests**

Include real loads whose name contains “HA” but whose ID is not allowlisted and assert they are never candidates. Dry run prints a canonical JSON report and performs no writes. Apply deletes candidates serially, waits one second between deletes, reads a second scoped own-device snapshot, exits 0 only if every candidate is absent, and produces the same success report on a second run.

- [ ] **Step 2: Run and verify failure**

Run: `uv run pytest tests/test_cleanup_legacy_mirror.py -q`

Expected: FAIL because the module is absent.

- [ ] **Step 3: Implement the deferred panel-only cleanup client**

Use deferred Brilliant imports and `MessageBusClient.delete_peripheral(device_id, peripheral_id, deletion_time_ms)`. Require root for `--apply`, reject `--apply` without a writable `--snapshot` report path under `/data/brilliant-mqtt/cleanup/`, and write no variable values or blobs to the report.

```python
def is_candidate(device: BrilliantDevice) -> bool:
    return device.peripheral_id.startswith(ALLOWED_ID_PREFIXES) and device.name.startswith(
        ALLOWED_NAME_PREFIXES
    )
```

The report contains only timestamp, owning device ID, candidate IDs/names/types, deleted IDs, remaining IDs, and success.

- [ ] **Step 4: Run cleanup and full agent gates**

Expected: targeted tests PASS; all four agent quality commands from Task 7 PASS.

- [ ] **Step 5: Commit**

```bash
git add src/brilliant_mqtt/cleanup_legacy_mirror.py tests/test_cleanup_legacy_mirror.py
git commit -m "feat: add verified legacy mirror cleanup"
```

### Task 11: Eliminate source/payload drift in tests and release CI

**Files:**
- Create: `tests/test_payload_parity.py`
- Modify: `scripts/build_payload.sh`
- Modify: `.github/workflows/ci.yml`
- Modify: `.github/workflows/release.yml`
- Modify: `.gitignore`

**Interfaces:**
- Parity compares every file under `src/brilliant_mqtt` to `custom_components/brilliant_mqtt/agent_payload/app/brilliant_mqtt`, excluding only `__pycache__` and `*.pyc`.

- [ ] **Step 1: Write the failing parity test**

Build maps of relative path → SHA-256 for source and payload, then assert the maps are equal. The failure message prints missing, extra, and changed relative paths. Do not auto-build inside pytest; drift must fail visibly.

- [ ] **Step 2: Run and verify failure against the currently stale payload**

Run: `uv run pytest tests/test_payload_parity.py -q`

Expected: FAIL and list the new control/scene modules as missing from the payload.

- [ ] **Step 3: Update build and CI behavior**

Keep the existing deterministic copy for the agent. While legacy uninstall support remains, continue packaging the mirror service, but add a comment that Task 12 removes it after live validation. In release CI, run:

```yaml
- name: Build panel payload
  run: scripts/build_payload.sh
- name: Verify generated payload is committed
  run: git diff --exit-code -- custom_components/brilliant_mqtt/agent_payload
```

Add/retain ignore rules for `artifacts/`, `*.gpr`, `*.rep`, `*.p12`, `*.pfx`, `*.pem`, `*.key`, `*.token`, `pilot-logs/`, and `var-collections/` without unignoring existing tracked sanitized analysis.

- [ ] **Step 4: Build payload and run parity/full gates**

Run: `scripts/build_payload.sh`

Expected: `payload built: custom_components/brilliant_mqtt/agent_payload` (the script prints the absolute repository path) and no errors.

Run: `uv run pytest tests/test_payload_parity.py -q && uv run ruff check && uv run ruff format --check && uv run mypy --strict src tests && uv run pytest`

Expected: all exit 0.

Run the four HA quality commands from Task 8; expected all exit 0.

- [ ] **Step 5: Commit generated payload and CI changes**

```bash
git add tests/test_payload_parity.py scripts/build_payload.sh .github/workflows/ci.yml .github/workflows/release.yml .gitignore custom_components/brilliant_mqtt/agent_payload
git commit -m "build: enforce panel payload source parity"
```

### Task 12: Document, validate on Office, then remove legacy runtime code

**Files:**
- Modify: `docs/ha-mirror.md`
- Create: `docs/brilliant-panel/home-assistant-integration.md`
- Create: `docs/brilliant-panel/runbooks/scene-bridge-pilot.md`
- Modify after hardware pass: `src/brilliant_ha_mirror/**`, `tests/test_ha_mirror_*.py`, `deploy/brilliant-ha-mirror.service`, `scripts/build_payload.sh`, `custom_components/brilliant_mqtt/components.py`, `custom_components/brilliant_mqtt/panel_ops.py`, and related HA tests.

**Interfaces:**
- Hardware acceptance: Office scene event → configured HA action; HA `run_scene` → Brilliant execution confirmation; no extra bus peer/host; physical controls remain responsive.

- [ ] **Step 1: Write the documentation before deployment**

Document the ownership model, exact topics and payload fields, label/area precedence, room overrides, command vocabulary, scene catalog semantics, replay deduplication, service confirmation, diagnostics, safety invariants, cleanup dry-run/apply sequence, rollback, and the fact that this does not create native HA tiles. Link the approved design and reverse-engineering findings.

- [ ] **Step 2: Commit documentation and deploy only the safe feature**

```bash
git add docs/ha-mirror.md docs/brilliant-panel/home-assistant-integration.md docs/brilliant-panel/runbooks/scene-bridge-pilot.md
git commit -m "docs: add HA control and scene bridge runbook"
```

Build the committed payload, install it through the integration, enable the safe scene bridge, and leave native tiles disabled/blocked.

- [ ] **Step 3: Run the Office hardware gate**

Record a redacted baseline and post-test report under ignored `artifacts/brilliant-panel/pilots/scene-bridge-<timestamp>/`. Verify:

1. `systemctl is-active brilliant-ha-mirror` is inactive and `brilliant-mqtt` is active.
2. Bus peer count does not increase relative to the forward-bridge baseline.
3. Tapping a known scene produces exactly one MQTT event and one HA event/action; if the home has a configured mode, changing it produces one mode event.
4. Reconnect/restart does not replay old scene or mode events.
5. `brilliant_mqtt.run_scene` returns only after a matching execution record; `brilliant_mqtt.set_mode` is hardware-tested when a real mode exists and otherwise remains covered by off-panel tests with an explicit “no configured modes” diagnostic.
6. HA, MQTT, agent, and panel restarts recover.
7. Ten consecutive physical light interactions remain subjectively immediate; no peer-add timeout, cloud-peer drop, or reconnect storm appears.
8. Disable/rollback removes scene subscriptions without deleting any Brilliant device/peripheral.

Any failure leaves the old mirror stopped and blocks the removal step; it does not restart Tier 1.

- [ ] **Step 4: After the hardware gate passes, delete legacy runtime packaging**

Remove the `brilliant_ha_mirror` source/tests/service and mirror payload subtree. Keep only the deprecated config migration and cleanup command for one release. Replace the deprecated registry row with migration-time direct `panel_ops.uninstall_ha_mirror`; never expose an install path.

- [ ] **Step 5: Rebuild and run every gate after removal**

Run: `scripts/build_payload.sh`

Run: `uv run ruff check && uv run ruff format --check && uv run mypy --strict src tests && uv run pytest`

Run: `uv run --project ha ruff check --config ha/pyproject.toml custom_components/brilliant_mqtt ha/tests && uv run --project ha ruff format --check --config ha/pyproject.toml custom_components/brilliant_mqtt ha/tests && uv run --project ha mypy --strict --config-file ha/pyproject.toml custom_components/brilliant_mqtt ha/tests && uv run --project ha pytest -c ha/pyproject.toml ha/tests`

Expected: all commands exit 0; `rg -n "enable_ha_mirror|brilliant-ha-mirror.service|src/brilliant_ha_mirror" src custom_components deploy scripts tests ha/tests` finds no install/start path.

- [ ] **Step 6: Commit the validated retirement**

```bash
git add -A src/brilliant_ha_mirror tests deploy/brilliant-ha-mirror.service custom_components/brilliant_mqtt scripts/build_payload.sh ha/tests docs/brilliant-panel
git commit -m "refactor: retire unsafe physical-control HA hosting"
```

---

## Completion evidence

Before opening a PR, attach or summarize:

- complete agent and HA quality-gate output;
- payload parity and clean post-build diff;
- protocol golden-vector parity across both runtimes;
- redacted Office scene event and confirmed HA→panel execution timing;
- before/after bus peer count and absence of reconnect/cloud-peer regressions;
- proof the panel has no HA token and the legacy service is inactive/removed;
- cleanup dry-run output, and apply/second-snapshot proof only if legacy candidates existed.
