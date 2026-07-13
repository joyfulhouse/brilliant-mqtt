# Native Home Assistant light and physical-slider validation status

## Decision

As of 2026-07-13, a Home Assistant-shaped native `LIGHT` is **renderable,
room-addressable, and admitted by the Office slider settings UI**, but an
end-to-end Home Assistant-to-slider bridge is **not yet confirmed**.

The exact status is:

> `legacy-picker-proven; virtual-control-live-blocked`

The positive UI result came from stale peripherals owned by Office's physical
Control. It proves the ordinary light schema can reach the native room and
slider-selection surfaces. It does not prove the required Virtual Control
ownership, online command routing, state feedback, restart behavior, or local
operation without Brilliant's cloud.

Do not bind a physical slider to the existing offline HA records. They have no
active owner and cannot be used for a meaningful control test.

## Safety and scope record

- Designated panel: Office.
- Firmware under test: `v26.06.03.1`.
- The operator prohibited the agent from triggering devices or scenes.
- No HA light, physical load, slider, or scene was triggered during this
  investigation.
- The operator navigated the native UI and confirmed that the HA controls were
  offered as slider targets, but did not save a binding or operate a slider.
- No Virtual Control was created, provisioned, started, or removed.
- No legacy peripheral was deleted.
- Panel-side inspection was read-only: service state, file existence, process
  presence, and scoped message-bus reads.
- Raw firmware, `/var`, object-store data, Ghidra projects, credentials, and
  private evidence remain under the gitignored `artifacts/brilliant-panel/`
  tree or root-only panel paths.

## Live evidence

### Panel and service state

| Observation | Result | Meaning |
|---|---|---|
| `brilliant-mqtt.service` | Active; deployed agent `0.5.6` | The supported forward bridge remained healthy during inspection. |
| `brilliant-ha-mirror.service` | Inactive/absent | The old physical-Control host was not serving the visible HA records. |
| Mirror environment/payload/process | Absent | No hidden mirror process could be refreshing state or accepting commands. |
| Virtual Control (`DeviceType 6`) | Not observed | The visible records are not evidence of a provisioned Virtual Control. |

Repository `0.5.7` contains the corrected legacy-cleanup allowlist, but it has
not been deployed to Office and no cleanup apply has been authorized.

### Persisted legacy peripherals

A scoped read of Office's own device found five persistent `LIGHT` (27)
peripherals from the earlier room-assignment pilot:

| Peripheral | Assigned room | UI confirmation |
|---|---|---|
| `HA Backyard Lamp 1` | Backyard | Rendered offline; offered as a slider target |
| `HA Backyard Lamp 2` | Backyard | Rendered offline; offered as a slider target |
| `HA Backyard Lamp 3` | Backyard | Rendered offline; offered as a slider target |
| `HA Balcony Lamp 1` | Balcony | Bus assignment confirmed; picker not separately checked |
| `HA Balcony Lamp 2` | Balcony | Bus assignment confirmed; picker not separately checked |

All five are attached to Office's ordinary physical `CONTROL` (`DeviceType 1`),
not a Virtual Control. The records retain the ordinary light variables used by
the UI, including `on`, `intensity`, `dimmable`, display metadata, and a valid
serialized room assignment. Their state was stale/off and the UI displayed
`offline` because the owning host process no longer exists.

This is the live persistence failure already documented for own-Control hosted
peripherals: the graph record can survive host exit and reboot. Tile existence
is therefore not proof of a live manager.

### Native UI and binary evidence

| Surface | Evidence | Conclusion |
|---|---|---|
| Backyard room screen | Operator saw the three `HA Backyard Lamp` tiles, each offline. | Native light rendering and room placement are confirmed. |
| Slider settings | The same three controls were offered as assignable targets; the operator did not bind one. | Ordinary `LIGHT` selector admission is confirmed. |
| `SwitchSliderSettingsScreen` | Decompiled eligibility resolves the target peripheral and checks the slider-capable `PeripheralType` set. | A host `DeviceType 6` is not itself rejected. |
| `HomePeripheralSelector` | Receives the permitted type collection and composes the picker models. | The picker uses typed home-graph data rather than HA entity IDs or MQTT topics. |
| Generic peripheral action path | Returns early for null or offline peripheral data. | Binding one of the stale targets would not prove routing and may leave a dead reference. |
| `CapTouchSliderConfig` | Stores slider index plus target `device_id` and `peripheral_id`. | A production binding must point to the Virtual Control and its owned light. |

## What is confirmed

1. The native UI accepts an ordinary `LIGHT` schema with valid display and room
   metadata.
2. A room assignment places those lights in the expected native room.
3. The Office slider settings UI offers those lights as assignable targets.
4. The slider eligibility path is based on target peripheral capability/type;
   static analysis found no host-`DeviceType` rejection for a Virtual Control.
5. Offline state and tile visibility are separate: a record can render and be
   selectable while lacking an active owner.
6. Physical-Control hosting is unsuitable despite the positive UI behavior: it
   co-manages a real panel, previously harmed responsiveness, and leaves
   persistent phantoms.

## What is not confirmed

1. Creation of an official Brilliant Virtual Control.
2. A Virtual-Control-owned configuration peripheral suitable for the hosted
   light's `configuration_peripheral_id`.
3. An online VC-owned HA light rendering on Office and a second panel.
4. A saved physical-slider binding to a VC-owned light.
5. Slider tap/dim gestures producing exactly one HA command.
6. HA on/off/brightness state returning to the native tile without command
   echo, oscillation, or stale snap-back.
7. Recovery across HA, MQTT, VC process, and panel restarts.
8. Resource safety during a sustained co-hosted VC process.
9. WAN-independent behavior after initial provisioning.
10. Supported restoration and removal without a stale tile, owner, or slider
    reference.

## Implemented validation components

| Component | Purpose | Live status |
|---|---|---|
| `tools.brilliant_vc.gates` | Ordered, sanitized VC0-VC5 evidence ledger | Off-panel tested |
| `tools.brilliant_vc.audit` | Prior-state and credential-path metadata audit | Read-only pieces exercised; official-app comparison incomplete |
| `tools.brilliant_vc.token_check` | Claims-only validation of the exact self-bootstrap permission | Off-panel tested; no official token available |
| `tools.brilliant_vc.provision_panel` | Single guarded official self-bootstrap call and private identity persistence | Off-panel tested; never applied |
| `tools.brilliant_vc.monitor` | Bounded process/resource/physical-lag abort monitor | Off-panel tested; no VC process exists |
| `tools.brilliant_vc.single_light_pilot` | One VC-owned HA-backed `LIGHT`, retained-state fencing, exactly-once command contract, and cleanup | Off-panel tested; live preconditions unavailable |
| `tools.brilliant_vc.slider_binding` | Scoped own-Control read, strict `slider_config` decoding, private baseline, and exact restoration verdict | Off-panel tested; no baseline captured and no binding written |
| `tools.brilliant_vc.e2e_acceptance` | Offline correlation of operator gesture, MQTT command/result/state, and two-panel convergence | Off-panel tested; no gestures performed or transcript collected |
| `tools.brilliant_vc.launcher_preflight` | Pinned-firmware, identity, permission, and isolated-path checks with no start primitive | Off-panel tested; deliberately reports the identity-consumer contract blocked |
| `brilliant_mqtt.cleanup_legacy_mirror` | Dry-run-first, own-device-only stale-record cleanup | Repository `0.5.7`; not deployed or applied |

The three repository-safe validation helpers are implemented. They do not make
the live experiment feasible by themselves: no official VC identity exists,
the shipped identity-to-standalone-message-bus consumer is still unresolved,
and no online VC light, physical binding, gesture, or E2E transcript exists.
See the [native slider E2E runbook](runbooks/native-slider-e2e.md) for their
scope and usage.

## Remaining steps to confirm end to end

The steps are ordered. Stop on the first failed or blocked gate; do not skip
ahead by hand-writing a `slider_config` or borrowing Office's identity.

| Step | Required action | Pass condition | Current state |
|---:|---|---|---|
| 1 | Finish VC0 prior-state audit in the official app and scoped bus view. | App and bus agree that no unexplained VC exists; sensitive paths are accounted for. | **Partial**: bus side observed; app inventory still required. |
| 2 | Obtain a token only from the official app's supported workflow. | Claims allow the exact `/provisioning/virtual-control-self-bootstrap` path and time bounds are valid. | **Blocked**: the account JWT returned HTTP 401 and is not sufficient. |
| 3 | Obtain fresh approval for one account-visible provisioning write. | Approval names the home, Office, one disposable VC, private storage, and mandatory official removal. | **Pending step 2**. |
| 4 | Run the guarded provisioner once. | HTTP 200; target home matches; exactly one DeviceType-6 identity appears; identity files are root-only. | **Not run**. |
| 5 | Confirm the official app exposes a supported removal path, without submitting it. | Correct VC/home/account target is shown at final confirmation. | **Not run**. |
| 6 | Run the isolated-launcher preflight and then, only after its blocker is resolved and separately approved, a bounded VC process under the monitor. | Dedicated identity, socket, state, certificate, and config paths; a proven official identity consumer; no physical-Control path reuse; resource/peer/latency limits pass. | **Preflight implemented; live start blocked** because the official identity-bundle consumer is unresolved. |
| 7 | Inventory only the VC-owned graph and configuration peripheral. | Owner is the provisioned DeviceType-6 ID; exactly one suitable VC-owned configuration link exists. | **Not run**. |
| 8 | Dry-run, then separately approve and start the one-light pilot. | One stable VC-owned `LIGHT`; valid Backyard room; retained HA authority received; tile shows online on two panels. | **Implementation exists; live blocked**. |
| 9 | Check the online VC light in the native slider picker. | The VC-owned light is offered without writing `slider_config` manually. | **Not run**; legacy picker result does not substitute. |
| 10 | Snapshot one named Office slider's complete original binding and behavior. | Private canonical snapshot is mode `0600`; a read-only verifier can prove exact restoration later. | **Helper implemented; no slider chosen or snapshot captured**. |
| 11 | Obtain fresh approval for that named slider, then have the operator bind it through the native UI. | Saved binding resolves to the disposable VC/light; all other slider/load configs are byte-identical. | **Not authorized**. |
| 12 | Obtain separate permission for physical gestures; the operator performs one tap and one dim while passive evidence is collected. | Each gesture creates exactly one HA command and HA feedback updates both panels without oscillation; the offline analyzer passes. | **Analyzer implemented; gestures currently prohibited** and no transcript exists. |
| 13 | Exercise HA, broker, VC, and panel restart/reconnect cases. | No replay, duplication, stale snap-back, peer regression, or physical lag. | **Not run**. |
| 14 | Test WAN-up and WAN-denied operation after provisioning. | Locality/cloud dependency is classified from observed behavior. | **Not run**. |
| 15 | Restore the original slider binding in the native UI and verify it exactly. | Snapshot comparison passes and normal wired/default behavior is restored. | **Not run**. |
| 16 | Remove the hosted light and VC through supported paths. | Two later scoped snapshots show no tile, owner, or slider reference. | **Not run**. |
| 17 | Separately decide whether to deploy `0.5.7` and delete the five legacy phantoms. | Fresh dry run and explicit cleanup approval; no object-store clearing. | **Not authorized**. |

## Immediate next action

Do not bind the offline legacy lights. Finish the official-app portion of VC0,
obtain a supported provisioning-scoped token for VC1, and close the
identity-to-standalone-VC launcher contract. The first live mutation must
remain the single guarded VC provisioning call after fresh approval; the first
physical-slider mutation must remain the operator's native-UI binding after an
online VC light and private original-binding snapshot exist.

See also:

- [Slider feasibility and binary evidence](slider-bridge-feasibility.md)
- [Virtual Control gate runbook](runbooks/virtual-control-gates.md)
- [Native slider E2E capture and restoration runbook](runbooks/native-slider-e2e.md)
- [HA mirror retirement and legacy cleanup](../ha-mirror.md)
- [Home Assistant support matrix](home-assistant-support-matrix.md)
