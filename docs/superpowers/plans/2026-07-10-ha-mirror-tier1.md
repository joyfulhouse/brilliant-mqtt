# HA Mirror (Tier 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `brilliant_ha_mirror` — a per-panel service that reflects HA-labeled Home Assistant entities (light/switch/lock/cover/garage) as native, controllable Brilliant panel peripherals, active only on an elected leader.

**Architecture:** Separate package + systemd unit. HA state/commands over the HA WebSocket API; peripherals hosted on the leader panel's own bus device via the firmware `PeripheralHost`; leader chosen by a priority election reusing the `mesh_leader` pattern. Real adapters (WS lib, framework host) are isolated behind Protocols; all orchestration/mapping/election logic is pure and unit-tested off-panel with fakes.

**Tech Stack:** Python 3.10 (panel runtime), `uv`, `pytest`, `mypy --strict`, `ruff`. HA WebSocket via `aiohttp` (already a transitive dep; confirm in Task 3). Firmware `lib.startables`/`peripherals.lib.peripheral_service` reached ONLY from `hosting.py`.

## Global Constraints

- **Python 3.10 only** for the agent/`src` (panel interpreter is 3.10.9); `requires-python = ">=3.10,<3.11"`. No 3.11+ syntax.
- **uv for everything.** Gate: `uv run ruff check --fix && uv run ruff format && uv run mypy --strict src tests && uv run pytest` — all green before every commit.
- **TDD.** Failing test → minimal impl → green → commit. Small, frequent commits.
- **Never disable linters** (`# noqa`, `# type: ignore`). Fix the root cause.
- **Never import firmware `lib.*` / `peripherals.*` outside `src/brilliant_ha_mirror/hosting.py`.** Never import the WS library outside `src/brilliant_ha_mirror/ha_client.py`. Everything else is behind Protocols with fakes and MUST run off-panel.
- **No secrets in git.** HA token + URL come from the environment at deploy time. No device ids, tokens, certs, or panel hostnames in committed files.
- **Verified bus/peripheral facts (from the 2026-07-10 spike):** own-device hosting = `PeripheralConfig(peripheral_id, MirrorClass)` with `virtual_device_id=None`; `_my_variables` is a METHOD (not a property); `VariableSpec` uses `int` for thrift BOOL (0/1); the registry keys peripherals by NAME; set VALUES are strings; own-device peripherals persist across reboot and require explicit delete via `ConditionalPeripheralHost.__dict__["delete_peripheral"](host, name)`.
- **Panel safety:** on-panel spikes run on the designated pilot first (see `CREDENTIALS.local.md`); every on-panel host launch is bounded (`timeout`) and every registered test peripheral is deleted before finishing; monitor `/proc/loadavg` and abort if load exceeds ~3.5.

---

## Task 1: V1 spike — home-wide visibility (GATE, on-panel, no feature code)

**Purpose:** Prove that a peripheral hosted on one panel's own bus device renders on OTHER panels (room-assigned). If it only renders locally, the leader-election model is wrong and the whole plan must be revised — so this runs first.

**Files:**
- Create: `docs/superpowers/research/2026-07-10-ha-mirror-v1-visibility.md` (result record; value-free)

- [ ] **Step 1: Host a room-assigned test LIGHT on the pilot's own device**

On the pilot only, reuse the proven recipe: a `Peripheral` subclass (type 27) with `_my_variables` returning `on`/`dimmable`/`intensity` (all `int`) plus a `room_assignment` set to a real room id, `PeripheralConfig(peripheral_id, MirrorLight)` (no `virtual_device_id`), launched via `python -m lib.startables.run_startable --message_bus_server_socket_path=/var/run/brilliant/server_socket <module>` under `timeout 60`. Resolve a real room id first (an observer `get_device` on the panel's own device → read an existing peripheral's `room_assignment`).

- [ ] **Step 2: Observe from a second panel**

From a DIFFERENT panel (office-bath pilot), run a read-only observer that lists the home's devices/peripherals and check whether "HA Test Light" appears, and in which room. Record present/absent + room.

- [ ] **Step 3: Delete the test peripheral and record the verdict**

Delete via `ConditionalPeripheralHost.__dict__["delete_peripheral"](host, "HA Test Light")` (re-host + borrowed-method pattern), confirm it is gone on both panels. Write the verdict to the research doc:
- **GO** if it renders home-wide → the plan proceeds unchanged.
- **NO-GO** if local-only → STOP; open a design revision (candidate: host on a device all panels subscribe to). Do not start feature tasks.

- [ ] **Step 4: Commit the research doc**

```bash
git add docs/superpowers/research/2026-07-10-ha-mirror-v1-visibility.md
git commit -m "docs: HA mirror V1 home-wide visibility spike result"
```

---

## Task 2: V3 spike — per-type control-routing proofs (on-panel, no feature code)

**Purpose:** The LIGHT path is proven. Before writing `mapping.py`, confirm each remaining Tier-1 type registers and routes a panel command to a `push_func`, and capture each type's EXACT required variables + string encodings (these become the mapping table's ground truth).

**Files:**
- Create: `docs/superpowers/research/2026-07-10-ha-mirror-v3-per-type.md` (per-type var tables + verdicts; value-free)

- [ ] **Step 1: For each type, extract required vars**

For GENERIC_ON_OFF (45), LOCK (1), SHADE (53), GARAGE_DOOR (74): read `thrift_types/peripheral_interfaces/{generic_on_off,lock,shade,garage_door}_interface/ttypes.py`, list required fields + thrift types (BOOL→int, I32→int, STRING→str). Record per-type var tables.

- [ ] **Step 2: Register + drive each type (bounded)**

For each type, host a minimal peripheral (own device, `timeout`-bounded) with the required vars, one `externally_settable=True` command var + `push_func`; drive it with a string-valued `request_set_variables_in_peripheral`; confirm the `push_func` fires (log line). Delete each via the borrowed `delete_peripheral` before the next.

- [ ] **Step 3: Record verdicts + encodings, commit**

Record GO/NO-GO per type and the exact command var + value encoding. Commit:

```bash
git add docs/superpowers/research/2026-07-10-ha-mirror-v3-per-type.md
git commit -m "docs: HA mirror V3 per-type control-routing proofs"
```

Gate: any NO-GO type is dropped from Tier 1 (note it); the rest proceed to `mapping.py` with the recorded var tables.

---

## Task 3: Package skeleton + config

**Files:**
- Create: `src/brilliant_ha_mirror/__init__.py`, `src/brilliant_ha_mirror/config.py`
- Create: `tests/test_ha_mirror_config.py`
- Modify: `pyproject.toml` (confirm `aiohttp` present for Task 9; add if missing with a pinned latest stable)

**Interfaces:**
- Produces: `Settings` (frozen dataclass) with fields `panel: str`, `ha_ws_url: str`, `ha_token: str`, `mirror_label: str = "brilliant"`, `leader_priority: int = 0`, `leader_heartbeat_seconds: float = 10.0`, `room_overrides: Mapping[str, str]` (HA area → Brilliant room id), `log_level: str = "INFO"`; classmethod `from_env(env: Mapping[str, str]) -> Settings`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_ha_mirror_config.py
import pytest
from brilliant_ha_mirror.config import Settings

def test_from_env_reads_required_and_defaults():
    env = {
        "PANEL": "office", "HA_WS_URL": "ws://ha.local:8123/api/websocket",
        "HA_TOKEN": "tok", "MIRROR_LABEL": "brilliant", "LEADER_PRIORITY": "5",
    }
    s = Settings.from_env(env)
    assert s.panel == "office"
    assert s.ha_ws_url.endswith("/api/websocket")
    assert s.mirror_label == "brilliant"
    assert s.leader_priority == 5
    assert s.room_overrides == {}

def test_room_overrides_parsed_from_json():
    env = {"PANEL": "p", "HA_WS_URL": "ws://x", "HA_TOKEN": "t",
           "ROOM_OVERRIDES": '{"Back Yard": "room-123"}'}
    s = Settings.from_env(env)
    assert s.room_overrides == {"Back Yard": "room-123"}

def test_missing_required_raises():
    with pytest.raises(KeyError):
        Settings.from_env({"PANEL": "p"})
```

- [ ] **Step 2: Run to verify it fails** — `uv run pytest tests/test_ha_mirror_config.py -v` → FAIL (module missing).

- [ ] **Step 3: Implement `config.py`**

Follow `brilliant_mqtt/config.py`: frozen dataclass, `from_env` reads `os.environ`-style mapping, required via `env[...]` (KeyError), optional via `env.get`, `ROOM_OVERRIDES` parsed with `json.loads` (default `{}`), `LEADER_PRIORITY` via `int(...)`. `__init__.py` sets `__version__ = "0.1.0"`.

- [ ] **Step 4: Run to verify it passes** — `uv run pytest tests/test_ha_mirror_config.py -v` → PASS.

- [ ] **Step 5: Run full gate + commit**

```bash
uv run ruff check --fix && uv run ruff format && uv run mypy --strict src tests && uv run pytest
git add src/brilliant_ha_mirror/__init__.py src/brilliant_ha_mirror/config.py tests/test_ha_mirror_config.py pyproject.toml
git commit -m "feat(ha-mirror): package skeleton + env config"
```

---

## Task 4: Mapping table (pure translation)

**Files:**
- Create: `src/brilliant_ha_mirror/mapping.py`
- Create: `tests/test_ha_mirror_mapping.py`

**Interfaces:**
- Consumes: the per-type var tables recorded in Task 2.
- Produces:
  - `@dataclass(frozen=True) class PeripheralSpec: peripheral_type: int; variables: dict[str, str]` (initial var values, all strings) `; command_vars: frozenset[str]` (externally-settable var names).
  - `@dataclass(frozen=True) class ServiceCall: domain: str; service: str; data: dict[str, object]`.
  - `SUPPORTED_DOMAINS: frozenset[str]` = `{"light","switch","lock","cover"}`.
  - `def spec_for(entity: HaEntity) -> PeripheralSpec | None` — maps an HA entity (domain + attributes, incl. `device_class` for garage covers) to the Brilliant peripheral spec, or `None` if unsupported.
  - `def state_to_variables(entity: HaEntity) -> dict[str, str]` — HA state → variable string values.
  - `def command_to_service(entity_id: str, var: str, value: str) -> ServiceCall` — a panel variable set → the HA service call.
  - `HaEntity` = `@dataclass(frozen=True): entity_id: str; state: str; attributes: Mapping[str, object]; area: str | None` (defined here; reused by `ha_client`/`mirror`).

- [ ] **Step 1: Write failing tests (one per domain, both directions)**

```python
# tests/test_ha_mirror_mapping.py
from brilliant_ha_mirror.mapping import (
    HaEntity, spec_for, state_to_variables, command_to_service, ServiceCall,
)

def _e(eid, state, **attrs):
    return HaEntity(entity_id=eid, state=state, attributes=attrs, area="Kitchen")

def test_light_spec_and_state():
    e = _e("light.k", "on", brightness=128)
    spec = spec_for(e)
    assert spec.peripheral_type == 27
    assert "on" in spec.command_vars and "intensity" in spec.command_vars
    v = state_to_variables(e)
    assert v["on"] == "1"
    assert int(v["intensity"]) > 0

def test_light_command_on_off():
    assert command_to_service("light.k", "on", "0") == ServiceCall("light", "turn_off", {"entity_id": "light.k"})
    c = command_to_service("light.k", "on", "1")
    assert c.domain == "light" and c.service == "turn_on"

def test_switch_maps_to_generic_on_off():
    assert spec_for(_e("switch.s", "off")).peripheral_type == 45
    assert command_to_service("switch.s", "on", "1") == ServiceCall("switch", "turn_on", {"entity_id": "switch.s"})

def test_lock_maps_and_commands():
    assert spec_for(_e("lock.l", "locked")).peripheral_type == 1
    assert state_to_variables(_e("lock.l", "locked"))["locked"] == "1"
    assert command_to_service("lock.l", "locked", "0") == ServiceCall("lock", "unlock", {"entity_id": "lock.l"})

def test_cover_position_maps_to_shade():
    e = _e("cover.blind", "open", current_position=40)
    assert spec_for(e).peripheral_type == 53
    assert state_to_variables(e)["position"] == "40"
    assert command_to_service("cover.blind", "position", "70") == ServiceCall(
        "cover", "set_cover_position", {"entity_id": "cover.blind", "position": 70})

def test_garage_cover_maps_to_garage_door():
    e = _e("cover.garage", "closed", device_class="garage")
    assert spec_for(e).peripheral_type == 74
    assert command_to_service("cover.garage", "event", "open") == ServiceCall(
        "cover", "open_cover", {"entity_id": "cover.garage"})

def test_unsupported_domain_returns_none():
    assert spec_for(_e("climate.t", "heat")) is None
```

- [ ] **Step 2: Run to verify fail** — `uv run pytest tests/test_ha_mirror_mapping.py -v` → FAIL.

- [ ] **Step 3: Implement `mapping.py`**

Pure functions, no I/O. Use the Task-2 var tables for required vars/encodings. Light brightness 0–255 → Brilliant `intensity` (scale to the range recorded in Task 2; store as string). Garage detected via `attributes["device_class"] == "garage"`. All variable values are strings.

- [ ] **Step 4: Run to verify pass** — `uv run pytest tests/test_ha_mirror_mapping.py -v` → PASS.

- [ ] **Step 5: Gate + commit**

```bash
uv run ruff check --fix && uv run ruff format && uv run mypy --strict src tests && uv run pytest
git add src/brilliant_ha_mirror/mapping.py tests/test_ha_mirror_mapping.py
git commit -m "feat(ha-mirror): Tier-1 HA<->Brilliant mapping table"
```

---

## Task 5: Protocols + fakes

**Files:**
- Create: `src/brilliant_ha_mirror/protocols.py`
- Modify: `tests/fakes.py` (add `FakeHaClient`, `FakePeripheralHost`)
- Create: `tests/test_ha_mirror_fakes.py`

**Interfaces:**
- Produces:
  - `class HaClient(Protocol)`: `async def start()`, `async def get_entities(label: str) -> list[HaEntity]`, `def on_state_change(cb: Callable[[HaEntity], Awaitable[None]])`, `async def call_service(call: ServiceCall) -> None`, `async def shutdown()`.
  - `class PeripheralHostClient(Protocol)`: `async def start()`, `async def register(name: str, spec: PeripheralSpec, on_command: Callable[[str, str], Awaitable[None]]) -> None`, `async def update_variables(name: str, values: Mapping[str, str]) -> None`, `async def delete(name: str) -> None`, `async def shutdown()`. (`on_command(var, value)` fires when the panel sets a command var.)
  - `FakeHaClient` (scriptable entities + `emit_state`/records `call_service`) and `FakePeripheralHost` (records registrations/updates/deletes + `fire_command(name, var, value)`).

- [ ] **Step 1: Write failing test for the fakes' contract**

```python
# tests/test_ha_mirror_fakes.py
import pytest
from brilliant_ha_mirror.mapping import PeripheralSpec, ServiceCall, HaEntity
from tests.fakes import FakeHaClient, FakePeripheralHost

@pytest.mark.asyncio
async def test_fake_host_records_and_fires_command():
    host = FakePeripheralHost(); seen = []
    await host.register("HA L", PeripheralSpec(27, {"on": "0"}, frozenset({"on"})),
                        lambda var, val: seen.append((var, val)) or _noop())
    await host.update_variables("HA L", {"on": "1"})
    assert host.variables["HA L"]["on"] == "1"
    await host.fire_command("HA L", "on", "0")
    assert seen == [("on", "0")]

async def _noop(): ...

@pytest.mark.asyncio
async def test_fake_ha_client_serves_entities_and_records_calls():
    ha = FakeHaClient(entities=[HaEntity("light.k", "on", {}, "Kitchen")])
    assert (await ha.get_entities("brilliant"))[0].entity_id == "light.k"
    await ha.call_service(ServiceCall("light", "turn_off", {"entity_id": "light.k"}))
    assert ha.calls[-1].service == "turn_off"
```

- [ ] **Step 2: Run to verify fail** → FAIL.
- [ ] **Step 3: Implement `protocols.py` + the two fakes in `tests/fakes.py`.**
- [ ] **Step 4: Run to verify pass** → PASS.
- [ ] **Step 5: Gate + commit**

```bash
git add src/brilliant_ha_mirror/protocols.py tests/fakes.py tests/test_ha_mirror_fakes.py
git commit -m "feat(ha-mirror): HaClient + PeripheralHostClient protocols and fakes"
```

---

## Task 6: Mirror orchestrator

**Files:**
- Create: `src/brilliant_ha_mirror/mirror.py`
- Create: `tests/test_ha_mirror_orchestrator.py`

**Interfaces:**
- Consumes: `HaClient`, `PeripheralHostClient`, `mapping.*`, `Settings`.
- Produces: `class Mirror` with `__init__(self, ha: HaClient, host: PeripheralHostClient, settings: Settings, room_id_for_area: Callable[[str | None], str | None])`; `async def reconcile()` (register/update/delete to match labeled entities); `async def start()` (initial reconcile + wire `on_state_change` → `update_variables`, and each peripheral's `on_command` → `command_to_service` → `ha.call_service`); `async def stop()` (delete all hosted peripherals). Peripheral **name** = a stable derived label (e.g. `HA <friendly_name>`); track name↔entity_id both ways.

- [ ] **Step 1: Write failing tests (reconcile add/remove; state→var; command→service)**

```python
# tests/test_ha_mirror_orchestrator.py
import pytest
from brilliant_ha_mirror.config import Settings
from brilliant_ha_mirror.mapping import HaEntity
from brilliant_ha_mirror.mirror import Mirror
from tests.fakes import FakeHaClient, FakePeripheralHost

def _settings(): return Settings(panel="p", ha_ws_url="ws://x", ha_token="t")

@pytest.mark.asyncio
async def test_start_registers_supported_entities():
    ha = FakeHaClient(entities=[HaEntity("light.k", "on", {"brightness": 200}, "Kitchen"),
                                HaEntity("climate.t", "heat", {}, "Kitchen")])
    host = FakePeripheralHost()
    m = Mirror(ha, host, _settings(), room_id_for_area=lambda a: "room-k")
    await m.start()
    assert len(host.registered) == 1  # climate unsupported, skipped
    assert host.registered_types == [27]

@pytest.mark.asyncio
async def test_state_change_updates_variable():
    ha = FakeHaClient(entities=[HaEntity("switch.s", "off", {}, "Kitchen")])
    host = FakePeripheralHost()
    m = Mirror(ha, host, _settings(), room_id_for_area=lambda a: "room-k")
    await m.start()
    await ha.emit_state(HaEntity("switch.s", "on", {}, "Kitchen"))
    name = host.registered[0]
    assert host.variables[name]["on"] == "1"

@pytest.mark.asyncio
async def test_panel_command_calls_ha_service():
    ha = FakeHaClient(entities=[HaEntity("switch.s", "off", {}, "Kitchen")])
    host = FakePeripheralHost()
    m = Mirror(ha, host, _settings(), room_id_for_area=lambda a: "room-k")
    await m.start()
    name = host.registered[0]
    await host.fire_command(name, "on", "1")
    assert ha.calls[-1].service == "turn_on"
    assert ha.calls[-1].data["entity_id"] == "switch.s"

@pytest.mark.asyncio
async def test_reconcile_deletes_unlabeled_entity():
    ha = FakeHaClient(entities=[HaEntity("switch.s", "off", {}, "Kitchen")])
    host = FakePeripheralHost()
    m = Mirror(ha, host, _settings(), room_id_for_area=lambda a: "room-k")
    await m.start()
    ha.entities = []          # label removed
    await m.reconcile()
    assert host.deleted == host.registered  # the one peripheral was deleted
```

- [ ] **Step 2: Run to verify fail** → FAIL.
- [ ] **Step 3: Implement `mirror.py`** — reconcile diff (register new supported, update existing, delete gone), state-change handler, command handler; inject `room_assignment` from `room_id_for_area(entity.area)` into the spec's variables when present.
- [ ] **Step 4: Run to verify pass** → PASS.
- [ ] **Step 5: Gate + commit**

```bash
git add src/brilliant_ha_mirror/mirror.py tests/test_ha_mirror_orchestrator.py
git commit -m "feat(ha-mirror): reconciling orchestrator (state<->command)"
```

---

## Task 7: Leader election

**Files:**
- Create: `src/brilliant_ha_mirror/leader.py`
- Create: `tests/test_ha_mirror_leader.py`

**Interfaces:**
- Produces: `class MirrorLeader` adapting `brilliant_mqtt.mesh_leader.MeshLeader`'s priority/claim/heartbeat state machine over a retained MQTT claim topic (own topic, e.g. `brilliant/ha-mirror/leader`), with `is_leader() -> bool`, `async def start()`, `async def tick()`. `priority < 1` → never leads. Reuse the algorithm; do not fork behavior. Prefer importing/parametrizing `MeshLeader` if its topic is injectable; otherwise a thin subclass with the mirror topic.

- [ ] **Step 1: Inspect `mesh_leader.py`** to decide reuse-by-composition vs subclass; confirm the claim topic is injectable.
- [ ] **Step 2: Write failing tests** mirroring `tests/test_mesh_leader.py` (lower priority yields; stale incumbent is preempted; `priority<1` never leads).
- [ ] **Step 3: Run to verify fail** → FAIL.
- [ ] **Step 4: Implement `leader.py`** (compose or subclass `MeshLeader`).
- [ ] **Step 5: Run to verify pass** → PASS.
- [ ] **Step 6: Gate + commit**

```bash
git add src/brilliant_ha_mirror/leader.py tests/test_ha_mirror_leader.py
git commit -m "feat(ha-mirror): priority leader election (reuse mesh_leader)"
```

---

## Task 8: Real peripheral-host adapter (`hosting.py`)

**Files:**
- Create: `src/brilliant_ha_mirror/hosting.py`
- Create: `tests/test_ha_mirror_hosting_smoke.py` (import-guarded; real verification is on-panel)

**Interfaces:**
- Produces: `class RpcPeripheralHost(PeripheralHostClient)` — the ONLY module importing `lib.*`/`peripherals.*`. Wraps the framework `PeripheralHost`: builds a `Peripheral` subclass per registration whose `_my_variables()` (a METHOD) returns `VariableSpec`s (`int` for BOOL vars, `str` for text; command vars `externally_settable=True` with a `push_func` bound to the registration's `on_command`); connects on the bus socket `/var/run/brilliant/server_socket`; `update_variables` pushes values; `delete` uses `ConditionalPeripheralHost.__dict__["delete_peripheral"](host, name)`.

- [ ] **Step 1: Write an import-guarded smoke test**

```python
# tests/test_ha_mirror_hosting_smoke.py
import importlib.util, pytest
_HAVE_FW = importlib.util.find_spec("lib.startables") is not None

@pytest.mark.skipif(not _HAVE_FW, reason="firmware only on panel")
def test_hosting_imports_on_panel():
    from brilliant_ha_mirror.hosting import RpcPeripheralHost
    assert RpcPeripheralHost is not None

def test_module_is_the_only_firmware_importer():
    import pathlib, re
    root = pathlib.Path("src/brilliant_ha_mirror")
    for p in root.glob("*.py"):
        if p.name == "hosting.py":
            continue
        text = p.read_text()
        assert not re.search(r"^\s*(from|import)\s+(lib|peripherals)\b", text, re.M), p
```

- [ ] **Step 2: Run to verify** — off-panel: firmware test skips, guard test runs → PASS after impl.
- [ ] **Step 3: Implement `hosting.py`** using the verified recipe (see Global Constraints). Keep it thin; no orchestration logic.
- [ ] **Step 4: On-panel smoke (pilot, bounded):** register one entity via `RpcPeripheralHost`, `update_variables`, fire a set → `on_command`, `delete`; confirm gone. Record in the V3 research doc.
- [ ] **Step 5: Gate + commit**

```bash
git add src/brilliant_ha_mirror/hosting.py tests/test_ha_mirror_hosting_smoke.py
git commit -m "feat(ha-mirror): framework peripheral-host adapter"
```

---

## Task 9: Real HA WebSocket adapter (`ha_client.py`)

**Files:**
- Create: `src/brilliant_ha_mirror/ha_client.py`
- Create: `tests/test_ha_mirror_ha_client.py` (protocol-shape + parsing tests against a fake WS transport; no network)

**Interfaces:**
- Produces: `class WsHaClient(HaClient)` — the ONLY module importing the WS lib (`aiohttp`). Implements HA WS handshake (`auth_required`→`auth`→`auth_ok`), `get_entities(label)` (fetch states + entity/area/label registries, filter by label, attach area), `subscribe_events("state_changed")` → parse into `HaEntity` → `on_state_change`, `call_service`. Parsing helpers (`_entity_from_state`, `_service_command`) are pure and unit-tested; the socket is injected for tests.

- [ ] **Step 1: Confirm `aiohttp`** is a dependency (Task 3); if added, pin latest stable (check https://pypi.org/project/aiohttp/).
- [ ] **Step 2: Write failing parsing tests** — feed recorded HA WS JSON frames (auth flow, a `get_states` result, a `state_changed` event) through the pure parsers; assert `HaEntity` fields + label filtering + area attach.
- [ ] **Step 3: Run to verify fail** → FAIL.
- [ ] **Step 4: Implement `ha_client.py`** with the socket injected behind a tiny transport seam so parsing tests run without network.
- [ ] **Step 5: Run to verify pass** → PASS.
- [ ] **Step 6: On-panel/off-box live check** against the operator HA (URL+token from env): connect, list labeled entities, observe one `state_changed`. Record counts only (no entity data committed).
- [ ] **Step 7: Gate + commit**

```bash
git add src/brilliant_ha_mirror/ha_client.py tests/test_ha_mirror_ha_client.py pyproject.toml
git commit -m "feat(ha-mirror): HA WebSocket adapter"
```

---

## Task 10: Entrypoint + supervised loop

**Files:**
- Create: `src/brilliant_ha_mirror/__main__.py`
- Create: `tests/test_ha_mirror_main.py`

**Interfaces:**
- Consumes: everything above.
- Produces: `async def run(settings, ha_factory, host_factory, mqtt_factory, leader_factory, *, clock, sleep) -> None` — testable supervised loop: join election; while leader, `Mirror.start()` + tick the leader + pump; on leader loss `Mirror.stop()` (delete all hosted peripherals); reconnect/backoff on WS or bus failure; resource-safe. `__main__` wires the real adapters + `Settings.from_env(os.environ)` and runs `run(...)`.

- [ ] **Step 1: Write failing test** — with all fakes + a fake leader: when leader, entities are hosted; when leadership is lost, `Mirror.stop()` deletes them; a raised WS error triggers backoff+resume (assert via injected `sleep`).
- [ ] **Step 2: Run to verify fail** → FAIL.
- [ ] **Step 3: Implement `__main__.py`** mirroring `brilliant_mqtt/__main__.py`'s supervisor structure.
- [ ] **Step 4: Run to verify pass** → PASS.
- [ ] **Step 5: Gate + commit**

```bash
git add src/brilliant_ha_mirror/__main__.py tests/test_ha_mirror_main.py
git commit -m "feat(ha-mirror): supervised entrypoint with leader gating"
```

---

## Task 11: Systemd unit + integration config delivery

**Files:**
- Create: `deploy/brilliant-ha-mirror.service`
- Create: `custom_components/brilliant_mqtt/agent_payload/brilliant-ha-mirror.service`
- Modify: the integration's installer + a config-flow/switch entry to enable the reverse mirror and write env (HA URL/token, label, leader priority, room overrides)
- Create/Modify: integration tests under `ha/tests/` for the new config plumbing

**Interfaces:**
- Consumes: `Settings` env var names from Task 3.

- [ ] **Step 1: Author the systemd unit** modeled on `deploy/brilliant-voice.service`: `ExecStart=/var/.../python -m brilliant_ha_mirror`, `Restart=always`, `MemoryMax`/`CPUQuota`/`Nice` caps, `EnvironmentFile=` for secrets. Copy into `agent_payload/`.
- [ ] **Step 2: Wire integration config delivery** — write the env file, install+enable the unit, expose a switch/Repair "Enable HA→panel mirror". Follow the existing voice/watchdog install path.
- [ ] **Step 3: Run BOTH gates** — agent gate and the integration gate (`uv run --project ha ...` per CLAUDE.md).
- [ ] **Step 4: Commit**

```bash
git add deploy/brilliant-ha-mirror.service custom_components/brilliant_mqtt/agent_payload/brilliant-ha-mirror.service custom_components/brilliant_mqtt ha/tests
git commit -m "feat(ha-mirror): systemd unit + integration config delivery"
```

---

## Self-Review (completed against the spec)

- **Spec coverage:** WS API → Tasks 9/6; leader election → Task 7/10; label selection + area→room → Tasks 6/9/3; separate package+unit → Tasks 3/11; mapping table (all 5 domains) → Task 4; Protocol isolation (WS in `ha_client`, framework in `hosting`) → Tasks 5/8/9 + the importer-guard test; cleanup/`delete_peripheral` → Tasks 6/8/10; failure handling/backoff → Task 10; V1/V2/V3/V4 → Tasks 1/2 (V1, V3), Task 7+10 (V4 leader handoff via stop→delete), V2 (room id) resolved in Task 1 and consumed in Task 6. Testing → each task's TDD steps + on-panel checks.
- **Placeholder scan:** none — every code step shows real signatures/tests; on-panel spikes give explicit procedures + gates.
- **Type consistency:** `HaEntity`, `PeripheralSpec`, `ServiceCall` defined in Task 4 and reused verbatim in Tasks 5/6/9; `HaClient`/`PeripheralHostClient` defined in Task 5 and consumed in 6/8/9/10.

## Notes / risks folded in

- **V1 is a hard gate:** if home-wide visibility fails, stop after Task 1 and revise the design (do not build Tasks 3–11 on a false assumption).
- **V2 (room id resolution):** Task 1 must record the concrete `room_assignment` id format; `room_id_for_area` in Task 6 depends on it. If room ids are not resolvable read-only, add a small resolver task before Task 6.
- **Panel safety:** every on-panel step is `timeout`-bounded, deletes its test peripherals, and monitors load; pilot first.
