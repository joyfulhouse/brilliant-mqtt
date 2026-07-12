# Home Assistant Control Plane and Brilliant Virtual Control — Design

- **Date:** 2026-07-12
- **Status:** Approved direction; pending specification review
- **Scope:** Replace the unsafe Tier-1 reverse mirror with a safe local control
  plane, ship a scene/mode bridge, and retry native tiles behind a gated
  Virtual Control feasibility track.
- **Supersedes:** `2026-07-10-ha-mirror-tier1-design.md`
- **Related evidence:**
  - `docs/claude/research/2026-07-06-mirror-poc/FINDINGS.md`
  - `docs/claude/research/2026-07-06-mirror-poc/REPORT.md`

## Executive decision

The current HA mirror must not host peripherals on a physical Brilliant
Control. That creates a second manager for the Control, adds one message-bus
peer per entity, can starve real loads, and has not produced reliable native UI
tiles. Room assignment and additional metadata cannot repair the ownership and
routing model.

The replacement has two tracks sharing one HA/MQTT control plane:

1. **Safe baseline:** a local Brilliant scene/mode ↔ Home Assistant bridge. It
   provides dependable panel-to-HA and HA-to-panel actions without hosting new
   peripherals or co-managing physical devices.
2. **Native-tile research track:** retry Brilliant's distinct
   `DeviceType.VIRTUAL_CONTROL` mechanism. Treat it as blocked until an official
   app-generated bootstrap token is available and live testing proves that
   runtime control remains local when WAN access is removed. Only after every
   gate passes may Virtual Control become the native-peripheral transport.

This design does **not** treat Virtual Control as already feasible. A July 9
live attempt authenticated the account and completed MFA, but
`/provisioning/virtual-control-self-bootstrap` rejected the account JWT. The
endpoint needs a provisioning-scoped token normally minted by the Brilliant
app. No Virtual Control was created. Static and live evidence also classify it
as a cloud-relayed device, so its latency and offline behavior remain suspect.

## Terminology and prior failures

These mechanisms are different and must not be conflated:

| Mechanism | What it is | Proven result | Decision |
|---|---|---|---|
| Physical Control hosting | Additional `PeripheralHost` managing a real panel device | Commands can reach `push_func`, but real lights became unresponsive and assigned test lights did not reliably render | Rejected |
| Invented third-party virtual device | A new name such as `homeassistant` or `shelly` | No cloud-seeded config/owner record, so registration and lease ownership fail | Rejected |
| `brilliant_virtual_device` | Existing shared Brilliant software device | Raw records can be registered by its current owner, but command ownership remains with its manager; taking its lease risks breaking built-in solar/weather/group services | Diagnostic only; never take its lease |
| Raw injected peripheral | A persisted bus record with no owned host | Can render with the right home graph and metadata, but UI commands route to the device manager and revert | Diagnostic only |
| Virtual Control | Brilliant device type 6, a non-physical Control with its own identity | Correct ownership paradigm, but app-mediated provisioning and runtime locality are unproven | Gated research track |

## Evidence incorporated from the 2026-07-12 validation

The Office pilot established several independent defects in Tier 1:

- The panel ran byte-for-byte copies of the stale bundled payload rather than
  the room-aware `src/` files.
- All five selected HA lights inherited `Backyard` or `Balcony` from their HA
  devices. The deployed client read only entity-level areas, so every mirror
  appeared unassigned.
- The UI binary excludes empty room assignments from normal room models, but a
  correctly assigned own-Control pilot still did not render. Room assignment is
  necessary for placement, not sufficient for admission.
- Inside the firmware framework, `room_assignment` is typed as
  `RoomAssignment`. Passing a serialized string fails with
  `TypeError: Expected type RoomAssignment but got str`; the framework performs
  serialization itself.
- `PeripheralHost.get_all()` for a room lookup consumed its CPU allowance and
  did not complete promptly. A scoped read of
  `configuration_virtual_device/home_configuration` completed and returned the
  exact room catalog.
- Fetching HA's device registry on-panel stalled long enough to hit resource and
  responsiveness concerns. HA already owns those registries and should resolve
  areas itself.
- The five-entity service opened five framework hosts and contributed to local
  bus peer timeouts. A single shared host is required for any future native
  transport.
- The configured BVD comparison was present on its owning panel but absent from
  Office's bus snapshot. Office's home-graph/cloud peer was disconnected, so
  the missing UI tile was a propagation failure and not valid evidence about
  metadata admission.

All transient BVD pilots were deleted. The unsafe `brilliant-ha-mirror` service
was left stopped on Office after validation.

## Goals

1. Make Home Assistant the authoritative entity, area, state, and command hub.
2. Provide useful local panel control even if native HA device tiles remain
   blocked.
3. Preserve physical Brilliant load responsiveness and the existing forward
   MQTT bridge.
4. Keep normal state and command traffic on LAN/MQTT whenever the selected
   transport supports it.
5. Prove rather than assume Virtual Control provisioning, rendering, routing,
   latency, and offline behavior.
6. Make every hardware experiment bounded, observable, and cleanly reversible.

## Non-goals

- Taking over `brilliant_virtual_device`, `configuration_virtual_device`,
  `ble_mesh`, or a physical Control lease.
- Blindly guessing private production GraphQL mutations to mint a provisioning
  token.
- Claiming cloud independence for a Virtual Control before a WAN-disconnect
  test proves it.
- Cameras, doorbells, WebRTC, and media in the initial native-peripheral tier.
- Automatic failover of a Virtual Control identity between panels in the first
  release.

## Safety invariants

The implementation and runbooks must enforce these rules:

- Never create a `PeripheralHost` with `virtual_device_id=None` for mirrored HA
  entities.
- Never bid on or overwrite ownership of `brilliant_virtual_device`,
  `configuration_virtual_device`, or `ble_mesh`.
- Never run one framework host per mirrored entity.
- Never provision an account-visible device without a fresh operator approval
  immediately before the write.
- Never exfiltrate panel private keys, PKCS#12 material, Brilliant passwords,
  MFA codes, bootstrap tokens, or account JWTs into the repository or logs.
- Abort a pilot if physical controls become sluggish, the cloud peer drops, the
  local bus begins rejecting peers, CPU/RSS exceed the gate, or cleanup cannot
  be proven.
- Every pilot has a hard runtime limit and an idempotent cleanup path verified
  from a second bus snapshot.

## Shared Home Assistant/MQTT control plane

The custom integration, not a wall panel, resolves HA registries and invokes HA
services. The panel-side process becomes a constrained transport adapter.

```text
HA entity/device/area registries
              │
              ▼
custom_components/brilliant_mqtt
  manifest + state publisher
  command executor
              │ MQTT
              ▼
panel transport adapter
  scene bridge (baseline)
  Virtual Control host (only after gates pass)
              │
              ▼
Brilliant message bus / UI
```

### Selection and registry ownership

- Continue using an HA label as the entity selection mechanism.
- Resolve entity registry area first and device registry area second inside HA.
- Resolve friendly name, device class, supported features, and the minimal
  attribute subset required by mappings inside HA.
- Subscribe to HA registry and state changes; do not poll full registries from
  the panel.
- Keep explicit case-insensitive HA-area → Brilliant-room overrides for names
  that cannot match automatically.

### MQTT namespace

Use a versioned namespace separate from the existing forward bridge:

| Topic | Retained | Payload purpose |
|---|---:|---|
| `brilliant/ha-control/v1/manifest` | Yes | Complete selected-entity catalog and monotonically increasing revision |
| `brilliant/ha-control/v1/state/<stable_id>` | Yes | Authoritative entity state and supported attribute subset |
| `brilliant/ha-control/v1/command/<stable_id>` | No | Panel-originated command with command ID and requested value |
| `brilliant/ha-control/v1/result/<command_id>` | No | HA service-call acceptance/error and resulting state sequence |
| `brilliant/ha-control/v1/status/<transport>` | Yes | Availability, manifest revision, resource use, and circuit-breaker reason |

`stable_id` is a deterministic UUIDv5 of the HA entity ID. Entity IDs and
friendly names remain payload fields rather than topic path components.

### Manifest contract

The retained JSON manifest contains:

- `schema_version`, `revision`, and generation timestamp;
- entity ID and stable ID;
- domain, device class, friendly name, and HA area name;
- supported command vocabulary;
- normalized capabilities such as dimming, position, tilt, and lock support;
- optional explicit Brilliant room override;
- a mapping-version field so incompatible agents fail closed.

Manifest changes are debounced and published atomically. State changes use
per-entity topics so normal updates do not republish the whole manifest.

### Commands and confirmation

- Panel commands contain a unique command ID, stable entity ID, command kind,
  value, and last observed state sequence.
- The HA integration validates the command against the current manifest before
  calling a service.
- HA state remains authoritative. A panel may show a short optimistic change,
  but it must reconcile to the subsequent HA state event or display failure.
- Duplicate command IDs are idempotently ignored.
- Commands expire quickly and are never retained.

## Track A: local scene and mode bridge

This is the shippable baseline because it requires no new peripheral owner.

### Panel → HA

- Observe each panel's `execution_peripheral`.
- Detect timestamped dynamic variables named
  `execution_state:scene_execution_handler:scene:<scene_id>`.
- Decode the execution payload and publish a non-retained scene event with
  panel ID, scene ID, execution timestamp, and deduplication key.
- The HA integration exposes the event to automations and optional configured
  HA actions.
- Deduplicate retained/replayed execution variables across reconnects.

### HA → panel

- Expose an HA service such as `brilliant_mqtt.run_scene`.
- Route the request through the existing agent connection to a selected online
  panel's `execution_peripheral.last_executed_scene_id` handler.
- Confirm execution from the resulting dynamic execution-state variable rather
  than assuming the set request succeeded.

### Scene catalog

- Read scene and mode definitions through scoped configuration-device reads.
- Publish IDs and display names for HA selectors and diagnostics.
- Treat the catalog as cached home configuration; executing a known cached
  scene remains local, while creating/editing Brilliant scenes is outside scope.

## Track B: Virtual Control feasibility gates

Virtual Control is promoted to a production transport only if every gate passes.

### VC0 — security and prior-state audit

- Inventory and remove or deliberately retain any prior root-only account token
  under `/tmp/mirror_poc/`; never copy it into the repo.
- Confirm no Virtual Control was created by the July 9 attempt.
- Record the exact firmware release and API behavior before retrying.

### VC1 — obtain an official bootstrap token

- Use only an official Brilliant app/device-add workflow or a directly observed
  supported request made by that workflow.
- Do not blind-guess GraphQL mutation names against production.
- If the official workflow cannot create or authorize a Virtual Control, mark
  the track blocked and stop. The scene bridge remains the product path.

### VC2 — provision one disposable Virtual Control

Requires a new operator approval immediately before execution.

- Use the provisioning-scoped token with
  `/provisioning/virtual-control-self-bootstrap`.
- Keep the returned device identity and PKCS#12 material on the designated
  panel with root-only permissions.
- Confirm the new device is visible in the Brilliant account and home graph.
- Prove the official removal/rollback path before hosting HA entities.

### VC3 — determine runtime topology

- Establish a baseline for command latency and state propagation with WAN up.
- Remove WAN access while retaining LAN, MQTT, HA, and panel-to-panel access.
- Test tile visibility, panel → HA commands, HA → panel state, process restart,
  and cross-panel propagation.
- If control or propagation needs the Brilliant cloud relay, report it plainly
  and do not market the transport as local. The operator decides whether the
  result still improves on SmartThings.

### VC4 — resource and isolation gate

- Run the Virtual Control identity on one explicitly selected pilot panel.
- Verify that it does not co-manage the panel's physical device ID.
- Measure message-bus peer count, CPU, RSS, load average, cloud-peer stability,
  UI frame responsiveness, and physical light latency for at least 24 hours.
- Abort on sustained agent CPU above 15%, RSS above 100 MiB, new peer-add
  timeouts, cloud disconnects, or operator-observed physical-control lag.

### VC5 — single native light

- Host one complete native light on the Virtual Control.
- Verify room placement and UI rendering on two panels.
- Verify panel → MQTT → HA command routing and HA → MQTT → tile state.
- Verify behavior across agent restart, HA restart, MQTT restart, panel reboot,
  and temporary network loss.
- Remove the light and prove no persistent phantom remains.

Only after VC5 passes may implementation expand to multiple entities.

## Native transport design after VC gates pass

### One shared host

- Create one `PeripheralHost` containing every mirror
  `HostedStartableSpec` rather than one host per entity.
- Batch manifest changes and apply them through one host reload/reconcile path.
- Limit registration concurrency to one and rate-limit churn.
- Use a stable internal peripheral name derived from `stable_id`; expose the HA
  friendly name through `display_name` so HA renames do not delete/recreate the
  bus identity.

### Room catalog

- Subscribe only to
  `configuration_virtual_device/home_configuration.rooms`.
- Decode and cache the room ID/name map once, then update it from scoped
  notifications.
- Use a typed `RoomAssignment(room_ids=[...])` as the framework value.
- Populate room assignment before initial registration, preserving the
  framework's timestamp-zero user-configuration expectations.
- Leave unmatched areas unhosted by default and surface a Repair/diagnostic;
  an explicit option may allow an `Unassigned` fallback.

### Complete type schemas

Do not use the Tier-1 minimal variable dictionaries. Build each peripheral from
the firmware thrift interface plus live exemplars and configuration linkage for
the provisioned Virtual Control.

Initial order:

1. LIGHT (27): `on`, `intensity`, `dimmable`, `max_intensity_value`, dim bounds,
   display name, room assignment, mode transitions, and required configuration
   linkage. Scale HA 0–255 to Brilliant 0–1000.
2. GENERIC_ON_OFF (45): on/off switch after a native exemplar comparison.
3. LOCK (1): lock state and lock/unlock commands with explicit security opt-in.
4. SHADE (53): position first; tilt only when both sides support it.
5. GARAGE_DOOR (74): open/close with confirmation and safety warning.

Every type receives its own live admission, rendering, command, and state test.

### Lifecycle

- The Virtual Control identity is tied to one designated host in the first
  release; systemd restarts it on that host.
- No automatic cross-panel identity failover until certificate/identity locking
  and split-brain behavior are understood.
- On normal agent restart, retain stable peripherals if the framework supports
  clean reattachment; on removal/unlabel, perform explicit timestamped delete.
- Handle SIGTERM and SIGINT so a bounded pilot can reconcile or delete before
  exit.
- A circuit breaker stops native hosting while leaving the scene bridge and
  forward MQTT bridge intact.

## Integration configuration and UX

The HA integration exposes:

- enable/disable for the safe scene bridge;
- selected scene-execution panel and scene/action mappings;
- HA mirror label;
- area/room override editor;
- native-tile experimental status and gate results;
- designated Virtual Control host only after provisioning succeeds;
- maximum mirrored entity count and per-domain opt-ins;
- Repairs for unmatched rooms, unsupported entities, stale transport,
  incompatible schema, and circuit-breaker activation.

The UI must never present native tiles as available merely because the feature
code is installed. It becomes selectable only after the Virtual Control gates
are recorded as passed.

The direct panel HA WebSocket URL/token fields are deprecated after migration;
panels consume MQTT only.

## Packaging and deployment

- `src/` remains the source of truth.
- Add a deterministic test that compares the packaged
  `custom_components/brilliant_mqtt/agent_payload` Python tree with `src/` for
  every non-vendored file.
- Release CI must run `scripts/build_payload.sh` and fail on a dirty diff.
- Add `ROOM_OVERRIDES` and any remaining runtime settings to config-entry,
  manager, env rendering, reconfigure, diagnostics, translations, and tests as
  one vertical slice; do not support settings only in the standalone unit.
- Keep binary dumps, `/var` collections, credentials, generated Ghidra projects,
  and pilot logs under gitignored artifacts.

## Migration and retirement of Tier 1

1. Keep `brilliant-ha-mirror.service` stopped by default.
2. Add an integration Repair explaining that physical-Control hosting was
   disabled for safety.
3. Provide an idempotent cleanup command for persistent `HA ` / pilot
   peripherals, verified from a second snapshot.
4. Migrate label and room-override settings to the HA-side manifest publisher.
5. Remove panel HA tokens after the MQTT control plane is active.
6. Remove the old leader election and one-host-per-entity implementation only
   after the scene bridge migration is validated.

## Observability and circuit breakers

Publish diagnostics without secrets:

- manifest revision and entity count;
- scene catalog revision and last execution event;
- MQTT connected state and command/result latency;
- bus connected state, peer count, registration latency, and reconnect count;
- process CPU/RSS and panel load average;
- Brilliant cloud-peer state as an observation, not a claimed dependency;
- hosted peripheral count, unmatched rooms, and last cleanup result;
- Virtual Control gate status and explicit blocked reason.

The native transport circuit breaker opens on resource threshold violation,
peer-add timeout, reconnect storm, repeated registration failure, or physical
control health alarm. It stops only the experimental transport and requires an
operator reset after the underlying condition clears.

## Testing strategy

Implementation follows test-driven development.

### Off-panel tests

- Manifest schema, stable IDs, registry-area precedence, capability reduction,
  and debouncing.
- MQTT state, command, result, expiry, idempotency, and reconnect behavior.
- Scene execution decoding and replay deduplication.
- Scene service routing and confirmation.
- Room matching, typed assignments, and scoped catalog updates.
- Shared-host reconciliation, stable rename behavior, deletion, and circuit
  breakers using firmware fakes.
- Per-domain state/command mapping and intensity conversion.
- Config-flow, reconfigure, diagnostics redaction, translations, migration, and
  payload-parity tests.

### On-panel gates

- Scene bridge on Office with no extra framework host.
- Virtual Control VC0–VC5 only in order and only after explicit approvals.
- Two-panel UI validation for every native type.
- WAN-disconnect test separated from ordinary Wi-Fi loss.
- Twenty-four-hour load and physical-control soak before entity count grows.
- OTA/reinstall and cleanup verification.

## Rollback

- Scene bridge rollback disables its MQTT subscriptions and removes no native
  devices.
- Native transport rollback stops the Virtual Control host, deletes test
  peripherals, verifies the home graph, and uses the official app removal path
  for the disposable Virtual Control.
- If the app cannot remove the Virtual Control cleanly, provisioning fails its
  precondition and must not proceed.
- The forward `brilliant-mqtt` bridge remains independent throughout.

## Success criteria

The baseline release succeeds when:

- Brilliant scene executions reliably trigger configured HA actions locally;
- HA can run Brilliant scenes and observe confirmation;
- physical controls and the forward bridge show no regression;
- panels hold no HA API token;
- packaged payload and source cannot drift.

The native-tile track succeeds only when:

- an officially provisioned Virtual Control is removable and isolated;
- a complete light renders on at least two panels;
- bidirectional control survives component restarts;
- WAN-disconnect measurements establish the real dependency and acceptable
  latency;
- a 24-hour soak produces no physical lag, peer failures, or cloud disconnects;
- cleanup leaves no phantom peripherals.

Failure of the Virtual Control track does not block or roll back the scene
bridge and HA/MQTT control plane.
