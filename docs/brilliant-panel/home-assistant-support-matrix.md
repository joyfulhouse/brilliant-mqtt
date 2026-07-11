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

## Reverse mirror: HA to native panel UI

| HA entity | Panel type | Status | Evidence / gap |
|---|---:|---|---|
| Light | LIGHT 27 | **Implemented + live mechanism** | Hosted light control callback proven; home-wide visibility proven |
| Switch | GENERIC_ON_OFF 45 | **Implemented + test** | Same scalar command mechanism; complete end-to-end panel UI smoke still useful |
| Lock | LOCK 1 | **Implemented + live mechanism** | Hosted lock callback proven |
| Positional cover | SHADE 53 | **Implemented + test** | Interface extracted; no live native shade instance; hosted smoke needed |
| Garage cover | GARAGE_DOOR 74 | **Implemented + test** | Event vocabulary inferred/implemented; live UI reflection explicitly unverified |
| Home-wide visibility | own Control device | **Implemented + live** | Hosted peripheral observed on a second panel and cleanly deleted |
| Leader election | MQTT priority | **Implemented + test** | Reuses mesh election pattern; live failover/no-phantom fleet drill still recommended |
| Explicit delete/tombstone | peripheral name + timestamp | **Implemented + live facts** | Persistence and timestamp requirement discovered live; adapter passes timestamp |
| Label selection | HA label registry | **Implemented + test** | Supported labeled entities only |
| HA state → panel | internal variable update | **Implemented + live fact** | Uses internal update to avoid callback loop |
| Panel command → HA | `push_func` → service | **Implemented + live mechanism** | Light/lock callback proven; service adapter covered off-panel |
| HA area → Brilliant room | room assignment struct | **Partial** | HA area names collected, but room IDs/struct are not applied in Tier 1 |
| Climate | THERMOSTAT 4 | **Research** | Required interface known; defer until room/type hosting is hardened |
| Media player | MUSIC 3 | **Research** | Rich schema and native screens; service translation is substantial |
| Alarm panel | SECURITY_SYSTEM 89 | **Research** | Must preserve code/PIN and state-transition semantics |
| Camera/doorbell | CAMERA 59 / DOORBELL 2 | **Defer to subsystem** | Needs streaming sessions in addition to variables |
| Valve/leak | WATER_SHUTOFF_VALVE 95 | **Research** | Useful community type after mirror framework validation |
| Weather/solar/energy | 79/97/101 | **Research** | Good display candidates; decide HA authority and update cadence |

## What the current integration gets right

1. **One source of truth for scalar auxiliary entities.** `AUX_SPECS` drives discovery, state rendering, and command translation together.
2. **Strict ownership boundaries.** Per-panel bridge instances publish only their owning Control; mesh uses a separate elected publisher.
3. **Permission-aware writes.** The bus rejects non-settable variables; mappings should never override that contract.
4. **Resilience against the frozen observer mirror.** Reconnect, hot polling, stale-stream rebuild, watchdog recovery, retained MQTT state, and LWT address observed failures.
5. **HA lifecycle management.** The companion integration installs, repairs, updates, and monitors panel-side components without requiring users to hand-edit systemd files.
6. **Native reverse hosting.** HA mirror devices use Brilliant's own peripheral model instead of a foreign panel web app.

## Important gaps

### P0: validate what is already shipped

Run the [validation runbook](validation-runbook.md) for every **Implemented + test** row on the current firmware. In particular:

- screen wake/sleep timeout;
- art/screensaver and each lock widget;
- touch-slider enable/restore and double-tap timeout;
- intercom broadcast receive preference;
- firmware auto-update and remote-assistance read/restore without starting an update/tunnel;
- HA mirror switch, shade, and garage rendering/control;
- leader handoff with no persistent phantom.

### P1: room-aware HA mirror

This is the highest-leverage UX improvement. Resolve Brilliant room ID enumeration and struct-valued `room_assignment`, then map HA areas to native rooms with explicit overrides. It makes mirrored devices appear where users expect across Rooms, shortcuts, and type screens.

Acceptance criteria:

- exact and case-normalized room match with operator override;
- unassigned fallback rather than wrong-room assignment;
- rename/move reconciliation;
- explicit deletion of stale assignments/peripherals;
- visibility confirmed from two physical panels.

### P1: scenes as an interoperability bridge

Validate a benign existing scene trigger, then expose Brilliant scenes as HA scene/button entities. In the reverse direction, consider hosting HA scenes or scripts as a native Brilliant scene-compatible target. This is the cleanest route for cap-touch double tap and screen gestures to launch HA automations without inventing raw input events.

Do not begin by exposing dynamic execution-handler variables. Use the catalog and one validated scene command.

### P1: physical-control bindings to HA

After scene hosting, validate these workflows:

- bind panel slider double tap to an HA-hosted scene/script;
- bind a screen gesture to a mirrored HA group/scene;
- bind a mesh-switch double tap when firmware marks it supported;
- verify action survives panel reboot and leader handoff;
- ensure deletion removes or invalidates the binding cleanly.

This addresses the community's “use the panel as a central local controller” need more naturally than publishing noisy low-level gestures.

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

Once the mirror host, room assignment, and lifecycle are proven fleet-wide, add HA-to-panel adapters in this order:

1. climate sensor and thermostat;
2. water leak/valve;
3. weather/energy/solar display;
4. media player;
5. security system with proper code handling;
6. camera/doorbell/intercom as a separate architecture.

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
2. **HA → Brilliant:** selected HA entities rendered natively, room-aware, with clean failover.
3. **Automation bindings:** scenes/scripts/groups that native sliders, gestures, modes, and shortcuts can execute.

That combination makes HA the central hub while retaining the panel's strongest UX instead of reducing it to a dimmer with a screen.
