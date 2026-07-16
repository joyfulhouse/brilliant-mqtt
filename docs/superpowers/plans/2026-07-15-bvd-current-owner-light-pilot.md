# BVD Current-Owner Single-Light Pilot Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a one-light, current-owner-only BVD pilot for
`light.backyard_light_group` that can receive native slider commands, use the
existing HA MQTT control plane, and prove local deletion without ever mutating
BVD ownership.

**Architecture:** One off-panel-testable tool contains pure topology,
controller, and lifecycle state machines behind read-only bus, native-host,
MQTT, process-guard, and probe Protocols. Deferred live adapters use the
physical panel bus and a framework host targeted explicitly at BVD. Apply mode
fails before mutation unless Office is already the fresh natural BVD owner and
its unmodified stock BVD vassal is serving all built-ins.

**Tech Stack:** Python 3.10, asyncio, Brilliant deferred Cython APIs, aiomqtt,
MQTT v1 JSON contract, pytest, ruff, mypy strict, uv.

## Global Constraints

- Never host on a physical Control, `ble_mesh`, or a new/named virtual device.
- Never write, refresh, clear, or restore the BVD owner variable.
- Exactly one type-27 pilot peripheral for `light.backyard_light_group`.
- No HA token on-panel; HA remains authoritative through MQTT.
- Every `tools.brilliant_bvd` module uses
  `brilliant_mqtt.bus.load_rpc_observer_class()` and never imports
  `lib.message_bus_api` directly. The existing read-only VC binding tool is
  staged unchanged for operator evidence collection.
- All Brilliant imports are deferred and the module imports off-panel.
- Apply runtime is at most 120 seconds with a 180-second external cleanup
  reserve covering bounded teardown and proof operations.
- Delete with `deletion_time_ms` and prove absence from two independent scoped
  reads 30 seconds apart.
- Root gate: `uv run ruff check && uv run ruff format --check && uv run mypy
  --strict src tests tools && uv run pytest`.

---

### Task 1: Pure safety model and topology guard

**Files:**
- Create: `tools/brilliant_bvd/__init__.py`
- Create: `tools/brilliant_bvd/single_light_pilot.py`
- Create: `tests/test_bvd_single_light.py`

**Interfaces:**
- Produces: `PilotConfig`, `PeripheralFact`, `BvdTopology`,
  `validate_preflight(config, topology, now_ms) -> None`, and
  `peripheral_id_for_entity(entity_id) -> str`.
- Consumes: no I/O; pure values only.

- [x] **Step 1: Write failing tests** for exact Office/entity binding, duration
  bounds, stable ID, fresh owner age, owner/relay equality, local stock-vassal
  readiness, exact six built-ins, ONLINE statuses, configuration links, and
  pre-existing pilot rejection. Assert the read-only `BvdBus` Protocol has no
  owner-write method and the source contains no forwarded owner RPC call.
- [x] **Step 2: Run red:** `uv run pytest tests/test_bvd_single_light.py -q`;
  expect import failure.
- [x] **Step 3: Implement minimal dataclasses, constants, Protocols, and pure
  validators**. Reject unknown/missing built-ins and do not add an ownership
  mutation interface.
- [x] **Step 4: Run green:** `uv run pytest tests/test_bvd_single_light.py -q`.

### Task 2: Native LIGHT schema and HA-authoritative controller

**Files:**
- Modify: `tools/brilliant_bvd/single_light_pilot.py`
- Create: `tools/brilliant_bvd/live.py`
- Modify: `tests/test_bvd_single_light.py`
- Create: `tests/test_bvd_live.py`

**Interfaces:**
- Produces: `VariableDefinition`, `build_light_variables`,
  `brightness_to_intensity`, `intensity_to_brightness`, decoded state/result
  types, and `PilotController`.
- Consumes: `Publisher` and `StateSink` Protocols.

- [x] **Step 1: Write failing tests** for the exact ten-variable typed schema,
  BVD configuration link, room assignment, scaling boundaries, strict state
  envelope, on/off/intensity command envelopes, current observed sequence,
  non-retained publication, unavailable fencing, epoch/sequence rejection,
  duplicate suppression, rejected-result restore-and-abort, canonical result
  payload compatibility, and identical retained replay restoring full native
  state.
- [x] **Step 2: Run red:** `uv run pytest tests/test_bvd_single_light.py -q`;
  expect missing symbols/behavior.
- [x] **Step 3: Implement the minimal controller** using v1 topic constructors
  and canonical JSON. Require retained HA state for initial/post-fence
  admission; then treat only the validated HA state stream as authority.
  Results are diagnostics only.
- [x] **Step 4: Run green:** `uv run pytest tests/test_bvd_single_light.py -q`.

### Task 3: Partial-start-safe lifecycle and rollback proof

**Files:**
- Modify: `tools/brilliant_bvd/single_light_pilot.py`
- Modify: `tests/test_bvd_single_light.py`

**Interfaces:**
- Produces: `VirtualLightHost`, `ScopedPeripheralProbe`, `PilotLifecycle`, and
  `CleanupReport`.
- Consumes: immutable BVD/peripheral IDs and a timestamp clock.

- [x] **Step 1: Write failing async tests** for start-once behavior,
  partial-register exception/cancellation/timeout, explicit deletion timestamp,
  two separately created absence probes, the 30-second interval, residual
  presence, delete failure followed by a real retry, read failure followed by a
  real retry, idempotent proven cleanup, and host shutdown ordering.
- [x] **Step 2: Run red:** `uv run pytest tests/test_bvd_single_light.py -q`.
- [x] **Step 3: Implement lifecycle** without deriving proof identity from the
  mutable host object. Never mark cleanup complete until both reads succeed.
- [x] **Step 4: Run green:** `uv run pytest tests/test_bvd_single_light.py -q`.

### Task 4: Deferred live bus, framework-host, MQTT, and CLI adapters

**Files:**
- Modify: `tools/brilliant_bvd/single_light_pilot.py`
- Modify: `tests/test_bvd_single_light.py`
- Modify: `pyproject.toml` only if the closed-source subclass requires the
  existing `disallow_subclassing_any=false` override.

**Interfaces:**
- Produces: `NativeBvdBus`, `LiveVirtualLightHost`, `LivePublisher`,
  `run_live_pilot`, and `main`.
- Consumes: `load_rpc_observer_class`, `PeripheralHost`, aiomqtt, and the pure
  controller/lifecycle.

- [x] **Step 1: Write failing tests** using injected fake firmware bindings and
  fake transports. Assert processor-before-observer start, unique peer name,
  exact scoped reads, exact `PeripheralConfig(...,
  virtual_device_id="brilliant_virtual_device")`, one HostedStartableSpec,
  internal state update, timestamped direct delete, signal/timeout/monitor
  abort cleanup, dry-run no mutation, and apply refusal when natural ownership
  is absent.
- [x] **Step 2: Run red:** `uv run pytest tests/test_bvd_single_light.py -q`.
- [x] **Step 3: Implement thin deferred adapters and CLI**. Keep firmware
  imports inside method bodies. Connect MQTT and obtain retained authority
  before exposing READY for operator assignment.
- [x] **Step 4: Run green and focused static checks:** `uv run pytest
  tests/test_bvd_single_light.py -q && uv run ruff check
  tools/brilliant_bvd tests/test_bvd_single_light.py && uv run mypy --strict
  tools/brilliant_bvd tests/test_bvd_single_light.py`.

### Task 5: Live-test and rollback runbook

**Files:**
- Create: `docs/brilliant-panel/runbooks/bvd-current-owner-light-pilot.md`
- Modify: `docs/superpowers/specs/2026-07-15-bvd-current-owner-light-pilot-design.md`
  only for implementation-discovered corrections.

**Interfaces:**
- Produces: exact orchestrator commands and operator checkpoints.
- Consumes: the implemented CLI and existing read-only slider binding tool.

- [x] **Step 1: Document staging and preflight** with exact root gate, Office
  identity, current-owner wait/refusal, stock process/service baselines, MQTT
  retained state check, slider baseline, and a staged cleanup command.
- [x] **Step 2: Document apply and observation**: start bounded process, wait for
  READY, operator assigns one slider, perform on/off and brightness gestures,
  capture command/result/state/native convergence, and reject snap-back.
- [x] **Step 3: Document hard aborts and rollback**: signal the pilot first,
  restore slider only through UI, prove byte-identical binding, prove peripheral
  absence twice, verify unchanged BVD owner/built-ins and stock services, and
  handle the failed-cleanup/current-owner-changed contingency without retries
  or lease writes.
- [x] **Step 4: Scan the design/runbook for placeholders and contradictions.**

### Task 6: Verification and independent review

**Files:** all changed files.

- [x] **Step 1: Run focused tests and red/green regression evidence.**
- [ ] **Step 2: Run the full root gate:** `uv run ruff check && uv run ruff
  format --check && uv run mypy --strict src tests tools && uv run pytest`.
- [ ] **Step 3: Request an independent code/safety review** against the user
  requirements and both design documents. Fix every critical/important issue.
- [ ] **Step 4: Re-run the full root gate and inspect `git diff --check`,
  `git status --short`, and the final diff before handoff.**
