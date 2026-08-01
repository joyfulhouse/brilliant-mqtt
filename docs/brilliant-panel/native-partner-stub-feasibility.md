# Native partner-stub feasibility

- **Date:** 2026-07-20
- **Firmware:** `v26.06.03.1`
- **Status:** research complete enough for an offline/disposable-home prototype;
  production mutation is **NO-GO** until the validation gates in this document
  pass.

## Decision

Use Brilliant's shipped partner-stub mechanism as the primary research path for
native non-light Home Assistant entities:

1. prove one `SHADE` on the unused Hunter Douglas partner host;
2. only after that lifecycle is clean, prove one non-common-area `LOCK` on the
   dormant RemoteLock host;
3. keep security systems, garage doors, cameras, and doorbells out of the first
   pilots.

This is a distinct path from both rejected physical-Control hosting and the
provisioning-blocked Virtual Control. The partner configuration is already
naturally owned, and the stock process already knows how to host the native
peripheral type. Hunter Douglas already has its virtual device; the
dynamic-gated RemoteLock process would register its predeclared virtual-device
identity when the first valid entry appears. The proposed HA bridge would
supply commands and state without inventing an owner or taking a shared lease.

PowerView Gen 3 emulation remains the lower-risk fallback for covers. It uses
the panel's ordinary local Hunter Douglas integration and has a cleaner
availability/state protocol, but it does not help locks. Stub-first research is
therefore the best way to test one reusable architecture across covers and
locks; PowerView should replace only the cover portion if the generic stub data
plane cannot be made authoritative and fail-safe.

No panel variable, partner configuration, peripheral, scene, lock, shade,
camera, or security system was changed during this investigation. The live
evidence below came from scoped read-only message-bus inspection.

## Evidence labels

| Label | Meaning in this document |
|---|---|
| **Live/read-only** | Observed in the running Office home graph without writing state |
| **Firmware** | Present in generated Thrift types, process configuration, shipped tools, or installed modules |
| **UI** | Present in the native Qt UI binary/resources |
| **Inference** | Supported by multiple artifacts but still needs a controlled mutation or traffic trace |

## Why this path is different

The deprecated mirror hosted arbitrary peripherals on a physical Brilliant
Control. That co-managed a real hardware owner, degraded load responsiveness,
and could leave persistent phantom records. A new Virtual Control would provide
a clean owner, but its official provisioning token, home assignment, local
runtime, and cleanup remain unproven.

Partner stubs use the ownership model that Brilliant already ships:

```text
configuration_virtual_device
  partner_configuration.process_config:<stable-id>
                 │ serialized PeripheralInfo(stubbed=True)
                 ▼
stock ConditionalPeripheralHost on the naturally selected panel
                 │ selects partner stub_module
                 ▼
stock partner virtual-device identity owns native SHADE / LOCK / ... peripheral
                 │
                 ├── native Brilliant room/tile/slider UI
                 └── scoped HA bridge observes commands and reflects HA state
```

There are two ownership layers and neither may be manufactured by the bridge:

1. `PeripheralInfo.owner` selects the logical partner host, such as
   `hunter_douglas` or `remotelock`.
2. The partner configuration's `owner` and the virtual device's
   `remote_bridge.relay_device` select the physical panel running that host.

The pilot may add or remove exactly one deterministic `process_config:*`
variable only while the existing configuration owner is stable. If the target
virtual device already exists, its relay must agree; a dynamic-gated device
must prove that agreement immediately after creation and before accepting any
command. The pilot must never write either ownership field.

## Shipped firmware mechanism

The stock `tools.test` command exposes `add_stubs [integration_id]`. Its helper:

1. formats a dynamic variable name for the partner configuration type;
2. serializes a normal `configuration.PeripheralInfo`;
3. sets `stubbed=True`, the partner owner, native peripheral type,
   third-party identifier, and configuration linkage; and
4. writes the variable through the ordinary message-bus
   `set_variables_request` call.

Relevant captured-firmware locations:

- `tools/test.py:120-134` builds a Hunter Douglas `SHADE` stub;
- `tools/test.py:423-455` builds RemoteLock lock variants;
- `tools/test.py:603-621` constructs the dynamic variable and
  `PeripheralInfo(stubbed=True)`;
- `tools/test.py:623-645` lists the shipped partner stub families;
- `tools/test.py:862-874` performs the normal bus write;
- `thrift_types/configuration/ttypes.py:7293-7496` defines `stubbed` as an
  ordinary serialized `PeripheralInfo` field.

The compiled
`peripherals/lib/peripheral_service/conditional_peripheral_host` documents that
configuration dynamic variables represent hosted peripherals. Its code paths
partition by owner, choose `stub_module` when `stubbed` is set, reload changed
peripherals, stop removed peripherals, and delete represented peripherals. The
compiled `virtual_peripheral_host` separately gates registration and status
updates on configuration owner matching `remote_bridge.relay_device`.

This is not proof that a specific stub renders or round-trips correctly in this
home. It is proof that the configuration and lifecycle mechanism is an intended
part of the shipped host framework rather than a raw unowned-record trick.

## Read-only Office inventory

The 2026-07-20 scoped probe inspected the configuration virtual device and only
the relevant partner virtual devices. It printed no credentials, room names, or
raw home/device identifiers.

| Partner surface | Live/read-only observation | Consequence |
|---|---|---|
| Configuration virtual device | Online virtual device with 38 configuration peripherals | Partner process configuration is present in the ordinary home graph |
| Hunter Douglas | Configuration online with zero dynamic `process_config:*` entries; partner virtual device exists with only `remote_bridge`; owner and relay agree | Best first stub pilot: active natural host, no represented shades to collide with |
| Somfy | Configuration and remote-bridge-only virtual device present | Local cover alternative, but its gateway/WebSocket/auth flow is more complex and adds no lock path |
| RemoteLock | Configuration present with no dynamic device entries; partner virtual device absent | Best later lock candidate: the process is deliberately gated on dynamic variables and should start only for the pilot entry |
| Schlage | Six represented native locks, including online real devices | Never use as an initial stub ring; any config change can reconcile the real lock population |
| Ring | 23 dynamic configuration entries and an active virtual device containing cameras, doorbells, and security systems | Never use as an initial stub ring; media/security traffic and real devices are already active |
| SmartThings | No active represented HA target population suitable for a safe pilot | Its adapter is cloud/token dependent and does not improve on the dedicated candidates |
| August and Genie | Configuration records exist, but no active partner virtual device | Access-token gates block a clean local pilot |

The inventory is a point-in-time observation, not a lease. Every future gate
must repeat stable reads from at least two panels immediately before any
mutation.

## Candidate ranking

### 1. Hunter Douglas `SHADE`: first pilot

**Verdict:** best low-risk admission and lifecycle probe.

Firmware evidence:

- the process has no access-token requirement;
- the stock helper creates one native `SHADE` owned by `hunter_douglas`;
- `hunter_douglas_shade_peripheral_stub` overrides `start` and `_set_position`
  and marks the represented peripheral `ONLINE`;
- the native shade interface supports position, continuous movement,
  capabilities, secondary/tilt positions, supported positions, and stop;
- the slider UI's permitted target set explicitly includes `SHADE` (53).

The first pilot must expose one primary-position-only cover. Tilt, secondary
rails, favorites, groups, scenes, and physical-slider binding remain out of
scope until touchscreen command/state behavior and cleanup pass.

### 2. RemoteLock `LOCK`: second pilot

**Verdict:** best lock candidate after the shade lifecycle is proven.

Firmware evidence:

- `remotelock_peripherals` requires dynamic configuration variables rather than
  an access token;
- the real lock setter reaches a `_make_lock_request` seam;
- the stub overrides that request seam, offline handling, and startup;
- the host reports that it does not poll RemoteLock when no non-stubbed
  peripherals exist.

Do not run the broad stock `add_stubs remotelock` helper on a live home. It
creates a normal lock, a common-area lock, and an offline lock that share a
third-party identifier. A future pilot must hand-build exactly one deterministic
`LOCK` with `configuration_variables={"is_common_area": "0"}` and a unique
peripheral/config suffix.

The first lock pilot is state-only plus **lock action only**. Unlock remains
disabled until authentication, authorization, stale-state fencing, failure
recovery, and cleanup have separately passed. A panel that merely displays a
lock as `ONLINE` is not enough.

### 3. Security systems: mechanically present, operationally no-go

Ring and Honeywell TC2 ship security-system stubs. The Ring stub overrides mode
requests, polling, offline handling, and startup. The inherited interface still
includes capabilities, encrypted codes/PIN handling, transitions, sensor data,
and special disarm semantics.

The active Ring ring already represents real cameras and security systems. A
configuration change can trigger a whole-host reconcile. No security stub
should be added there. If this domain is revisited, the first experiment is an
offline harness followed by an arm-only disposable-home pilot; disarm is a
separate security design.

### 4. Cameras and doorbells: stub admission does not solve media

The panel can render native `CAMERA` and `DOORBELL` types and can pull an RTSP
URL. Both types define externally settable dynamic
`live_view_session:<session-id>` requests, and `RemoteMediaSession` has a direct
`rtsp_url` field.

The stock camera stubs do not synthesize an arbitrary local stream. They inherit
vendor-specific Ring, Nest, SmartThings, or Honeywell behavior. When a user
opens a camera tile, the owning peripheral must answer the live-view request,
maintain `remote_sessions`, and tear the session down. A separate bus observer
cannot authoritatively answer on behalf of that owner.

Therefore:

- a stub may be useful later for UI admission research;
- go2rtc can provide H.264/AAC RTSP only after ownership/signaling is solved;
- a working HA camera tile still needs a proper custom owner or a safely
  isolated vendor-host replacement;
- cameras remain a separate media/privacy project, not an extension of the
  shade/lock data plane.

### 5. Garage doors and other partner types

The stock helper and schemas include Genie `GARAGE_DOOR`, SmartThings switches,
locks and shades, and other partner types. Genie and most access adapters are
token/cloud gated in this home. They are not better first candidates than
Hunter Douglas and RemoteLock. Existing Brilliant scenes remain the bounded
fallback for triggering garage, arm, or lock-only HA actions without a stateful
native tile.

## The unresolved HA data-plane problem

The stock stub supplies an owner and native UI callback, but it does not make HA
authoritative by itself.

The intended bridge would:

1. subscribe only to the one materialized partner peripheral;
2. translate UI-originated `position` or `locked` writes into one HA service
   call with sequence and in-flight-command fencing;
3. wait for authoritative HA state;
4. reflect that confirmed state to the native peripheral; and
5. restore or fence the provisional native value on rejection, timeout, HA
   disconnect, bus reconnect, or stale subscription.

Two issues are not yet resolved:

1. **Feedback origin.** Writing HA state back through the same externally
   settable variable may invoke the stub setter again and can be
   indistinguishable from a UI command unless request-origin metadata or an
   owner-internal update path is proven. Equality/revision suppression may make
   it safe, but that must be demonstrated rather than assumed.
2. **Availability authority.** Stock stubs force `ONLINE`; that status does not
   represent HA, MQTT, or the underlying entity. The observer bridge may not be
   authorized to change owner-managed status. A live-looking tile during HA
   failure is unacceptable for locks and misleading for covers.

These are the decisive production blockers. If they cannot be solved cleanly,
use PowerView emulation for covers and keep locks behind scenes or a future
proper virtual owner.

## PowerView fallback for covers

A local service can make selected HA covers look like a PowerView Gen 3 gateway:

```text
native Brilliant SHADE
  ↕ mDNS + HTTP /home/* API + event stream
local PowerView-compatible service
  ↕ HA WebSocket and cover services
Home Assistant cover
```

The captured firmware contains discovery, registration, enumeration, room,
scene, position, stop, and event paths. No TLS credential, pairing secret, or
gateway attestation was found. A prototype requires a stable dedicated LAN IP,
HTTP port 80, stable home/room/shade IDs, multicast reachability, and explicit
selection of **Add Device → Hunter Douglas** on Brilliant.

This path is cover-specific but has important operational advantages:

- the stock adapter treats the local gateway as its upstream state authority;
- it can report shade offline when HA is unavailable;
- HA-to-panel feedback is a normal gateway event, not another externally
  settable bus command;
- removal can use Brilliant's ordinary partner-integration workflow rather than
  direct configuration-variable deletion, if the first admission test confirms
  that workflow removes every represented record.

Current external references:

- [Brilliant's compatibility table](https://support.brilliant.tech/hc/en-us/articles/360015720891-Smart-Home-Products-That-Work-With-Brilliant)
  classifies Hunter Douglas and Somfy shades as local integrations while listed
  locks and cameras require Internet access;
- [Home Assistant's PowerView integration](https://www.home-assistant.io/integrations/hunterdouglas_powerview/)
  remains a local integration with Gen 3 position feedback;
- openHAB carries a captured
  [PowerView Gen 3 OpenAPI document](https://github.com/openhab/openhab-addons/blob/09b3792946ecb54484824620f9c0b7957f746677/bundles/org.openhab.binding.hdpowerview/src/test/resources/org/openhab/binding/hdpowerview/internal/gen3/openapi.yaml)
  covering the same `/home/*` surface found in the Brilliant binaries.

## Non-negotiable safety constraints

- Never host another test peripheral on a physical Brilliant Control.
- Never write a partner configuration `owner` or virtual-device
  `relay_device`.
- Never take or refresh the Brilliant virtual device lease for this path.
- Never call `set_config_peripheral`; it replaces the entire shared
  configuration peripheral.
- Never run a broad `add_stubs` helper against the production home.
- Never mutate active Schlage, Ring, or other real-device rings for an initial
  pilot.
- Use one deterministic, collision-checked process-config suffix, peripheral
  name, and third-party ID.
- Baseline the complete candidate config and target device twice from two
  panels before mutation and compare again afterward.
- Remove the exact declarative `process_config:*` source and let the natural
  host reconcile; do not merely delete the materialized peripheral, because the
  config can reconstruct it.
- Require a real deletion timestamp, two independent absence reads, peer
  convergence, unchanged owner/relay state, and no stock-host restart before
  declaring cleanup.
- Never clear the shared object store or configuration peripheral as recovery.
- Keep unlock, disarm, garage-open, camera/microphone activation, and physical
  slider binding out of the first pilot.

## Ordered validation gates

### Gate 0: offline ARM harness

Run captured firmware with all network access denied and fake
clients/coordinators. Prove:

- `stubbed=True` selects only the expected stub module;
- starting and driving one shade/lock produces zero HTTP, LAN, or partner API
  calls;
- UI-equivalent writes produce understood state deltas;
- HA feedback does not loop or duplicate commands;
- add/update/remove leaves unrelated represented peripherals untouched;
- owner/relay disagreement stops or rejects the host as expected;
- exact config removal produces timestamped peripheral deletion.

### Gate 1: production read-only baseline

From two panels, twice, record sanitized facts only:

- candidate config status, owner value/timestamp, dynamic names/types/count,
  and stubbed count;
- target virtual-device type/status, remote-bridge relay/timestamp, and every
  peripheral type/status/config linkage;
- natural owner process PID/start identity and resource baseline;
- absence of all proposed IDs and suffixes.

Hunter Douglas must remain remote-bridge-only. RemoteLock must have no existing
represented devices. Owner observations must be stable.

### Gate 2: disposable or cloned-home admission

Prove one shade through:

- single-entry creation and native room/tile rendering;
- cross-panel convergence;
- UI → HA command, exactly once;
- HA → UI confirmed state without echo or stale snap-back;
- HA/MQTT unavailable behavior;
- bus, HA, bridge, host, and panel restarts;
- owner handoff or explicit evidence that the process cannot hand off during
  the test;
- exact source removal and zero residue.

### Gate 3: bounded production shade pilot

Only after explicit operator approval, repeat Gate 2 with one noncritical cover,
without tilt or slider binding. Abort on owner drift, host restart, bus
reconnect, stale observer, HA authority loss, resource regression, or unrelated
graph change.

### Gate 4: lock research

Only after the shade path passes and its cleanup is independently reviewed:

- repeat the offline and disposable-home tests for one RemoteLock `LOCK`;
- expose state first, then lock-only command;
- test denial, timeout, restart, and HA-offline behavior;
- keep unlock disabled until a separate security review approves it.

## Current verdict by domain

| Domain | Firmware mechanism | Live-home suitability now | Next step |
|---|---|---|---|
| Cover/shade | Stock Hunter Douglas stub; local PowerView adapter fallback | Stub mutation no-go; bounded offline prototype go | Prove stub lifecycle/feedback first; use PowerView if those gates fail |
| Lock | Stock RemoteLock stub and dormant dynamic-gated host | Production no-go | Offline harness after shade results |
| Security system | Ring/TC2 stubs and native UI | No-go; active ring and credential semantics | Separate arm-only security design later |
| Camera/doorbell | Native tile, RTSP/WebRTC sessions, vendor stubs | Blocked by owner/session response | Read-only native session trace, then proper-owner design |
| Garage door | Native type and cloud partner helpers | Token/cloud blocked | Use scene actions pending a proper owner |
| Generic switch/light | Existing diyHue path covers HA `light`/`switch` | Proven for lighting-shaped entities | Keep diyHue; do not move proven lights to stubs |

## Documentation consequences

The earlier blanket conclusion that all native non-light types must wait for a
Virtual Control is too broad. The corrected boundary is:

- physical-Control hosting remains rejected;
- generic arbitrary native hosting still needs a proper Virtual Control;
- selected types with a shipped, otherwise-unused partner host and stub module
  have a distinct gated research path;
- covers also have a local protocol-emulation fallback;
- cameras remain a separate ownership/media problem even when a stub can admit
  a tile.

Related records:

- [Home Assistant support matrix](home-assistant-support-matrix.md)
- [Peripheral and control surfaces](peripheral-surfaces.md)
- [Cloud and local boundaries](cloud-boundaries.md)
- [HA entity to physical-slider feasibility](slider-bridge-feasibility.md)
- [diyHue local control path](diyhue-bridge.md)
- [Deprecated HA mirror and cleanup](../ha-mirror.md)
