# Home Assistant support matrix

## Scope language

| Status | Meaning |
|---|---|
| **Implemented + live** | Code exists and the essential bus behavior was exercised on a panel |
| **Implemented + test** | Code and off-panel tests exist; the exact current-build end-to-end behavior still needs a live checklist |
| **Partial** | Useful subset exists but important semantics remain native-only |
| **Research** | Firmware/UI surface is understood enough for a bounded probe |
| **Defer** | Technically present but poor fit, duplicate, unsafe, or privacy-heavy |

## Forward bridge: panel to HA

| Capability | Current status | Evidence / remaining work |
|---|---|---|
| Wired light on/off | **Implemented + live** | Plain string writes and physical response verified |
| Wired dimming | **Implemented + live** | Instance intensity denominator handled |
| Wired switch/outlet | **Implemented + live** | GENERIC_ON_OFF/OUTLET mapping |
| Always-on circuit monitoring | **Implemented + live** | Power/temp/fault only; not falsely switchable |
| Per-load watts | **Implemented + live** | Wired and mesh; `-1` sentinel suppressed |
| Load temperature/fault | **Implemented + live** | Fault is inverse of `is_safe` |
| Mesh lights/switches | **Implemented + live** | Home-wide `ble_mesh`, published once by leader election |
| Mesh motion | **Implemented + live** | Derived from motion score; desired-state reconciliation |
| Mesh motion score/thresholds/reporting | **Implemented + live** | Advanced entities disabled by default |
| Faceplate motion/lux/LED | **Implemented + live** | Lux requires firmware enable switch |
| Faceplate motion tuning | **Implemented + live** | Advanced detection modes/thresholds; bounds partly assumed |
| Screen on/brightness | **Implemented + live** | Hardware peripheral; UI/sysfs max discrepancy handled through bus range |
| Mic mute and volume | **Implemented + live** | Output and alert volume; ducking opt-in |
| CPU temperature/firmware | **Implemented + live** | Diagnostic entities |
| Camera/privacy state | **Implemented + live** | Correctly read-only; writes are permission-rejected |
| UI “In Use” | **Implemented + live** | Touchscreen activity, not occupancy |
| Child lock/night mode/identify | **Implemented + live** | UI peripheral scalar controls |
| Wi-Fi/Internet/NTP status | **Implemented + live** | Diagnostic subset |
| Firmware auto-update | **Implemented + test** | Writable hardware variable; opt-in due security trade-off |
| Vendor remote assistance | **Implemented + test** | Writable hardware variable; disabled by default/security-sensitive |
| Screen wake/sleep on motion | **Implemented + test** | Type 20 scalar mappings; run current-build live toggle/restore checklist |
| Screensaver/time-date/widgets | **Implemented + test** | Type 16 simple booleans; complex art/widget objects omitted |
| Touch sliders enable | **Implemented + test** | Inverted `disable_cap_touch_sliders`; validate UI and physical slider restore |
| Intercom broadcasts receive | **Implemented + test** | Scalar preference only, not an intercom implementation |
| Slider double-tap timeout | **Implemented + test** | Numeric mapping; verify native bounds |
| Load calibration and electrical modes | **Research** | Writable but safety-critical; expert service only |
| Mesh status-light brightness | **Research** | Live settable variable found on switch config |
| Scene list/activation | **Research** | Catalog decoded; trigger write unverified |
| Modes/groups/rooms | **Research** | Schema/UI understood; semantic HA mapping not designed |
| Music/media player | **Research** | Rich MUSIC schema and UI; no forward mapping |
| Notification/announce | **Research** | UI rich, notification peripheral has no readable vars |
| Camera/intercom | **Defer to subsystem** | Media/signaling/privacy/resource work, not core MQTT bridge |
| Partner virtual devices | **Defer** | Prefer native HA integrations; avoid duplicates/cloud indirection |
| Mesh DFU, bootstrap, reset, beta firmware | **Defer/guarded admin** | High-risk operational commands |

### 2026-07-11 deployment spot check

The pilot was inspected read-only during this analysis:

- native message bus, native UI, `brilliant-mqtt`, bus watchdog, and voice satellite were active;
- the deployed bridge reported version `0.5.6`;
- recent reconciles reported 9 mapped panel peripherals producing 50 entities and 32 command topics;
- mesh reconciles reported 20 load peripherals producing 112 entities and 72 command topics;
- recent logs showed both desired-state motion-score reconciliation and ordinary mesh load commands flowing;
- no bridge error appeared in the inspected tail;
- a read-only retained-topic check found 17 panel state topics and 20 mesh state topics, with both availability topics and panel bridge metadata present;
- retained payloads included the Tier-1 fields for wake/sleep-on-motion, screensaver and lock widgets, touch sliders, intercom receive, double-tap timeout, update governance, remote assistance, and the previously proven load/motion/diagnostic fields;
- a read-only HA state check found 127 pilot/mesh entities across light, switch, sensor, binary-sensor, number, button, select, and update domains; none were unavailable (five optional readings were `unknown`);
- the community Wi-Fi watchdog was inactive on this pilot;
- the HA mirror was inactive and its payload directory absent after the bounded development run; the prior unit log showed a clean stop.

This spot check validates current service health, discovery-state publication, retained telemetry, and ongoing command flow. It is not a substitute for the per-variable write/restore checklist.

## HA to panel: safe control versus native tiles

The earlier reverse-mirror implementation is deprecated. Its physical-Control
hosting mechanism is not a supported panel transport even where an individual
callback or delete was observed. See the
[retirement guide](../ha-mirror.md) for the failure model.

| Surface | Status | Evidence / gap |
|---|---|---|
| HA label/area/device manifest and retained state | **Implemented + off-panel test** | HA owns registry resolution and MQTT publication; no current panel transport consumes the generic entity manifest. |
| Brilliant scene catalog/event → HA | **Implemented + off-panel test** | Scoped catalog codec, durable replay suppression, HA event and constrained configured action; Office hardware gate pending. |
| HA `run_scene` → Brilliant confirmation | **Implemented + off-panel test** | Existing catalog IDs only; completion requires a matching execution record; Office hardware gate pending. |
| Brilliant mode catalog/event and HA `set_mode` | **Implemented + off-panel test** | Same constrained transport; live test requires a real configured mode. |
| Native HA light/switch/lock/cover tiles | **Blocked research** | Physical-Control hosting rejected. Virtual Control must pass provisioning, ownership, rendering, routing, WAN-isolation, resource, and cleanup gates. |
| Room overrides | **Implemented manifest metadata** | Entity area precedes device area and explicit overrides are case-insensitive; metadata does not render a native tile. |

## What the current integration gets right

1. **One source of truth for scalar auxiliary entities.** `AUX_SPECS` drives discovery, state rendering, and command translation together.
2. **Strict ownership boundaries.** Per-panel bridge instances publish only their owning Control; mesh uses a separate elected publisher.
3. **Permission-aware writes.** The bus rejects non-settable variables; mappings should never override that contract.
4. **Resilience against the frozen observer mirror.** Reconnect, hot polling, stale-stream rebuild, watchdog recovery, retained MQTT state, and LWT address observed failures.
5. **HA lifecycle management.** The companion integration installs, repairs, updates, and monitors panel-side components without requiring users to hand-edit systemd files.
6. **Safe reverse semantics.** Existing Brilliant scenes/modes cross the shared
   local MQTT/bus sessions without hosting a peripheral or storing an HA
   credential on the panel.

## Important gaps

### P0: validate what is already shipped

Run the [validation runbook](validation-runbook.md) for every **Implemented + test** row on the current firmware. In particular:

- screen wake/sleep timeout;
- art/screensaver and each lock widget;
- touch-slider enable/restore and double-tap timeout;
- intercom broadcast receive preference;
- firmware auto-update and remote-assistance read/restore without starting an update/tunnel;
- Office scene event/action and confirmed `run_scene`;
- reconnect/restart replay suppression with no peer or physical-control
  regression.

### P1: scenes as an interoperability bridge

The safe scene/mode bridge and HA surfaces are implemented off-panel. Complete
the [Office scene-bridge pilot](runbooks/scene-bridge-pilot.md): validate one
benign existing scene event/action, confirmed HA-to-panel execution, restart
recovery, no replay, unchanged peer count, and physical responsiveness. Do not
create arbitrary scene blobs or hosted HA scene peripherals.

### P1: physical-control bindings to HA

Use existing Brilliant scene bindings to trigger the HA-side configured action
map. Directly hosting HA scene/group targets on a physical Control remains
rejected. Any native binding to a future HA peripheral must wait for the
distinct Virtual Control feasibility gates.

### P2: media player and announce

Separate low-risk audio playback from full intercom:

1. local HA `media_player`/`notify` output through the existing speaker/GStreamer path;
2. volume and playback-state synchronization;
3. chime/announcement queue and ducking;
4. optional native MUSIC peripheral hosting;
5. only later, microphone/intercom/video.

Keep wake-word capture, announcements, and native Alexa independently controllable.

### P2: configuration and diagnostics

Useful additions with bounded risk:

- mesh status LED brightness;
- Wi-Fi signal/channel diagnostics via OS read-only probes;
- faceplate/gangbox firmware and hardware revision sensors;
- rootfs/update state diagnostics;
- bridge/watchdog health breadcrumbs;
- explicit “HomeKit service healthy” diagnostic for fallback monitoring.

### P3: advanced native types

Do not add another native type through physical-Control hosting. Type-specific
adapters become eligible only after the separate Virtual Control track passes
all feasibility gates. Camera/doorbell/intercom remains a separate media and
privacy architecture regardless of transport.

## Features to avoid by default

- raw electrical calibration and break-circuit controls;
- reset-all-settings, bootstrap pivot, or HomeKit reset;
- mesh DFU and firmware beta toggles;
- raw device/home/account identifiers;
- partner cloud virtual devices duplicated into HA;
- camera or microphone activation without explicit privacy design;
- using `switch_ui.active` as room occupancy;
- high-frequency full-graph polling;
- writing any variable merely because `externally_settable=True`.

## Community-facing product shape

A coherent local product has three layers:

1. **Brilliant → HA:** physical loads, sensors, panel controls, health, and mesh.
2. **Bidirectional semantics:** existing Brilliant scene/mode events trigger HA
   actions, and HA requests catalog-allowlisted execution with confirmation.
3. **Gated native research:** only an officially provisioned, local-enough
   Virtual Control may eventually render selected HA entities; physical-Control
   hosting is not a fallback.

That combination makes HA the central hub while preserving the panel's existing
scene/room UX without risking its physical load manager.
