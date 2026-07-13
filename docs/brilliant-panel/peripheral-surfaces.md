# Peripheral and control surfaces

## Type system

The firmware's generated Thrift types define 106 `PeripheralType` values, numbered 0–105, and 96 named peripheral-interface packages. Three layers must be kept separate:

1. **Type:** a global numeric category such as LIGHT=27, SHADE=53, or SECURITY_SYSTEM=89.
2. **Interface:** the typed field schema a conforming peripheral may serialize.
3. **Instance:** a live peripheral whose variables may be a subset/superset, may include dynamic names, and independently marks each variable externally settable or read-only.

The message bus stores every live `Variable.value` as a string even when the interface schema says BOOL, I32, DOUBLE, STRUCT, or LIST. Complex values are binary Thrift encoded and commonly base64-encoded in snapshots. Do not parse them as JSON or guess delimiters.

## Live pilot Control

The same-build 2026-07-06 snapshot contains 25 peripherals on the pilot's own Control device:

| Type | Peripheral | Function | HA status |
|---:|---|---|---|
| 5 | `faceplate_peripheral` | PIR/light/screen motion, lux, LED, thresholds | Core readings and tuning implemented |
| 7 | `discovery_peripheral` | Local service and partner discovery | Not exposed; infrastructure |
| 9 | `remote_bridge` | Panel-to-panel distributed-device bridge | Not exposed |
| 10 | `object_store_peripheral` | Local object-store endpoint | Not exposed |
| 12 | `switch_ui` | UI activity, lock/night flags, intercom state, UI flows | Activity, child lock, night mode, identify implemented |
| 13 | `voice_peripheral` | Native Alexa state/audio configuration | Mic mute overlaps hardware; native voice kept separate |
| 15 | `art_peripheral` | Available art libraries | Native-only |
| 16 | `art_config_peripheral` | Screensaver, widgets, art configuration | Simple switches implemented; complex blobs native-only |
| 19 | `device_config_peripheral` | Name, room, gestures, sliders, intercom receive, orientation | Sliders/intercom/double-tap timeout partly implemented |
| 20 | `motion_detection_config_peripheral` | Screen wake/sleep behavior | Implemented |
| 22 | `hardware_peripheral` | Screen/audio/OTA/privacy/CPU/rootfs/governance | High-value safe subset implemented |
| 27 | `gangbox_peripheral_0` | Wired dimmable light | On/off, intensity, power, temp, fault implemented |
| 29 | `wifi_peripheral` | Wi-Fi/Ethernet state and requests | Connectivity status implemented |
| 30 | `analytics_peripheral` | Analytics endpoint configuration | Not exposed |
| 33 | `execution_peripheral` | Scene/state/mode execution and handlers | Scene/mode bridge implemented off-panel; Office hardware acceptance pending |
| 37 | `bootstrap` | Authentication/home join/pivot | Provisioning-only |
| 42 | `gangbox_config_peripheral` | Gang count and process configuration | Diagnostic/configuration infrastructure |
| 43 | `faceplate_uart_status_peripheral` | Faceplate firmware status | Not exposed |
| 44 | `gangbox_uart_status_peripheral_0` | Gang firmware, revision, reboot/beta flag | Not exposed; operationally risky |
| 46 | `gangbox_peripheral_1` | Wired always-on circuit | Power, temp, fault implemented; correctly not switchable |
| 48 | `alarm_config_peripheral` | Alarm execution/configuration | Not exposed |
| 60 | `remote_media_peripheral` | Active remote media sessions | Not exposed |
| 62 | `ble_peripheral` | Mesh proxy/topology/messages/DFU | Bridge uses mesh device state, not low-level controls |
| 66 | `homekit_peripheral` | HAP pairing, fixtures, reset/restart | Managed natively; fallback path |
| 75 | `notification_peripheral` | Notification sink/controller | Zero readable variables; protocol unknown |

### Writable does not mean suitable

Many infrastructure variables are externally settable because the native UI or service stack must operate them. Examples include process configuration, mesh update triggers, bootstrap pivots, HomeKit reset, UART reboot, and execution handler state. The HA integration should expose only a reviewed semantic subset.

## Wired loads

### LIGHT (27)

Required schema fields are `on`, `dimmable`, and `intensity`. Optional fields include:

- `max_intensity_value`, `minimum_dim_level`, `maximum_dim_level`, and `dimming_edge`;
- `low_wattage`, `multi_way`, `on_off_inverted`, and dim-smoothing configuration;
- `display_name`, `room_assignment`, and ULID;
- `temperature`, `is_safe`, voltage references, current sensing, and power notification configuration;
- diagnostic/break-circuit fields used by compatibility and factory flows.

Live values use an intensity scale whose denominator is `max_intensity_value` (1000 on the pilot wired dimmer). HA brightness 0–255 must be scaled, rounded, and clamped. `minimum_dim_level` is calibration, not the current minimum brightness request.

### GENERIC_ON_OFF (45) and OUTLET (40)

These use `on` for control and may carry display, room, power, temperature, and fault variables in a live instance. They map naturally to HA switches unless a specific instance presents dimming fields.

### ALWAYS_ON (46)

An always-on wired gang has no `on` variable. It remains useful for live watts, temperature, and fault state. Publishing it as a controllable switch would misrepresent the electrical configuration.

### Calibration and safety variables

The bridge currently omits writable calibration and electrical-mode variables such as minimum/maximum dim, `dimmable`, `low_wattage`, `multi_way`, inversion, compatibility check, and break-circuit/dimming controls. This is intentional. A future expert-only service should:

- read and store the complete baseline;
- validate model/gang type;
- use documented bounds from the native UI or interface;
- require explicit confirmation;
- restore on failure;
- never appear as default dashboard numbers/switches.

## Faceplate motion and presence

The faceplate interface defines:

- `movement_detected` and `pir_motion_score`;
- `lux`;
- hottest/coldest internal temperature;
- PIR high/low thresholds;
- enable flags for PIR scoring, lux, screen-motion detection, and light-motion detection;
- faceplate LED state.

The current bridge exposes these with advanced tuning disabled by default. `switch_ui.active` means the panel is actively in use, not room occupancy. Use motion, UI activity, and load changes as separate signals in HA rather than collapsing them into one presence sensor.

## BLE mesh

The `ble_mesh` virtual device has 40 peripherals:

- 11 LIGHT;
- 1 GENERIC_ON_OFF;
- 8 ALWAYS_ON;
- 20 SWITCH_CONFIGURATION.

The 20 load peripherals share motion scoring and power-related variables in addition to normal load control. The bridge publishes them once through a priority-elected mesh leader to avoid 14 duplicates.

### Mesh load controls

Mesh LIGHT instances use `on`, `intensity`, `dimmable`, display/room information, power, compatibility flags, motion score, thresholds, and update state. Their intensity denominator can differ from wired gangs; use the instance's own scale.

Mesh motion required special handling. The firmware's `movement_detected` latch did not behave as a useful live sensor in testing. The bridge derives motion from `motion_score`, gates it on `enable_motion_score`, and reconciles the desired reporting state.

### SWITCH_CONFIGURATION

Configuration fields include:

- `display_name`, `room_assignment`, firmware/hardware identity;
- `slider_config` and optional light binding;
- `status_light_max_brightness`;
- `double_tap_enabled` and scene association in dynamic configuration;
- cap-touch tuning, connectivity, update, and reboot fields.

No normal button-press/event variable was found. A physical action invokes its configured binding and the resulting target state changes. `status_light_max_brightness` is a reasonable future expert number; reboot/DFU/cap-touch internals are not.

## Hardware, display, and governance

The hardware interface includes screen brightness/on state and schedule, output/alert volume, mic mute, CPU temperature, boot/release data, update enablement, tracked release stage, rootfs status, camera/privacy status, SoC type, cap-touch count, remote assistance, reset/reboot timers, software-integrity challenge, and HomeKit-token regeneration.

Current safe mappings include:

- screen on and brightness;
- mic mute and output/alert volume;
- speaker ducking and low-temperature mode as opt-in configuration;
- CPU temperature and firmware tag diagnostics;
- firmware auto-update and remote assistance, disabled by default;
- camera and privacy status as read-only diagnostics.

The live bus enforces `externally_settable`. `camera_on` and `privacy_toggle` are read-only and cannot be promoted to switches through the normal set path.

Root SSH, reverse SSH, reset-all-settings, immediate update, release channel, rootfs operations, integrity challenge, and HomeKit token regeneration should remain native or guarded administrative services.

## UI, art, and lock screen

`switch_ui` carries UI activity, child lock, night/feature flags, home-screen configuration, request/reset/identify triggers, and intercom state/target/SDP/parameters. The current integration maps stable scalar controls but not dynamic home-screen layout or media session fields.

`art_config_peripheral` exposes simple booleans alongside serialized configuration:

- screensaver/art `on`;
- time/date;
- weather, music, device-status, climate, security, and solar widgets;
- art rotation and library selections;
- global/local art behavior;
- display name and room assignment.

Simple widget switches are mapped. Art libraries, rotation policies, climate/security widget objects, and home-screen layout require blob decoding and lifecycle semantics.

## Rooms, scenes, groups, modes, and execution

These cross-cutting concepts live primarily under `configuration_virtual_device` and the Control's execution peripheral.

### Scenes

Scene configuration is a binary Thrift object containing at least:

- scene ID;
- display name;
- icon resource;
- ordered per-device actions of device ID, peripheral ID, variable, and value.

The pilot home has only `all_off` and `all_on`. The execution peripheral has
`last_executed_scene_id`, validity triggers, and dynamic execution-state
variables. The bridge now implements catalog-allowlisted writes and requires a
matching dynamic execution record before reporting success; the Office hardware
gate remains pending.

### Groups

Device-group configuration supports group membership plus toggle and intensity actions. The UI can create/edit groups, count offline/unsupported devices, and display aggregate power. HA groups map conceptually, but no supported transport currently renders generic HA entities in native room/type browsing.

### Modes

Mode configuration exposes active mode and default eco behavior. The execution
peripheral includes `manual_mode_id`. The bridge catalogs existing modes,
publishes timestamped mode events, and accepts confirmed `set_mode` requests;
hardware validation requires a real configured mode. It does not conflate modes
with HA scenes or invent defaults for an empty catalog.

### State/config execution

Dynamic variables named like `execution_state:<handler>:<target>` reveal handlers for lights, scenes, art/screen, auto-update, state configs, and mesh OTA. Never wildcard-write execution-state variables; several invoke disruptive operations.

## Access, climate, environment, and media schemas

The firmware can model the following even though this installation has no native instances for most of them:

| Type | Principal interface fields | Recommended HA shape |
|---|---|---|
| LOCK (1) | `locked` | `lock` |
| SHADE (53) | continuous, position, secondary/tilt position, capabilities | `cover` |
| GARAGE_DOOR (74) | display/room, event | garage `cover` |
| THERMOSTAT (4) | ambient/target/ranges, HVAC/fan mode, capabilities | `climate` |
| CLIMATE_SENSOR (80) | temperature, humidity, alarms, battery | sensors/binary sensors |
| MUSIC (3) | playback, track, seek, volume, speakers, presets, shuffle/repeat | `media_player` |
| CAMERA (59) | display/room, sessions, streaming configuration | camera/media subsystem |
| DOORBELL (2) | camera fields plus notification configuration | camera/event/chime |
| SECURITY_SYSTEM (89) | system mode, capabilities, sensors | `alarm_control_panel` |
| WATER_SHUTOFF_VALVE (95) | valve and leak status | valve/switch + leak sensor |
| WEATHER (79) | temperature, status, sunrise/sunset, sky cover | weather/sensors |
| SOLAR (97) | configuration, estimates, savings/reset | energy sensors/button |
| HOME_ENERGY_SYSTEM (101) | display/room, solar generation today | energy sensor |

The former Tier-1 mirror implemented peripheral schemas for lights, switches,
locks, positional covers, and garage covers, but its physical-Control hosting
mechanism is deprecated and unsafe. Those schemas are research evidence, not a
supported native-tile surface. The safe baseline is documented in the
[HA integration guide](home-assistant-integration.md); native types remain gated
behind Virtual Control feasibility.

## Media, camera, and intercom

Media uses more than bus variables:

- GStreamer playback and capture;
- RTSP server components;
- WebRTC/SDP and ICE handling;
- `remote_sessions` and streaming configuration objects;
- intercom target/state/parameters;
- camera/live-view and remote-access gates;
- speaker/ring volume and chime assets.

The bus is the signaling/state plane; actual audio/video uses media transports. A future HA camera/intercom bridge must trace both planes, enforce privacy, bound CPU/memory, and avoid exposing bedrooms or microphones by default.

## Notifications

The `notification_peripheral` has no readable variables on the pilot, while the UI contains a large notification controller and many notification views. Likely designs include write-only/dynamic variables, subscription events, or direct service calls. The next safe step is to observe bus traffic while invoking the native Identify action and receiving a benign system alert, then correlate the notification controller's Thrift methods and dynamic peripheral changes.

## Partner virtual devices

The full graph includes partner virtual devices for systems such as Hue, LIFX, TP-Link, SmartThings, Ring, Schlage, Ecobee, Nest, Sonos, Somfy, Wemo, Hunter Douglas, and Bluesound. They demonstrate the hosting architecture but should not be blindly republished to HA:

- native HA integrations generally provide richer, maintained support;
- many Brilliant adapters remain cloud dependent;
- republishing causes duplicates and loses native integration diagnostics;
- credentials and partner-specific setup remain in Brilliant state.

Use the graph for interoperability and gated Virtual Control research, while
keeping the forward MQTT bridge scoped to Brilliant-owned hardware and
explicitly selected home-wide Brilliant mesh devices. Do not retry native type
validation by hosting on a physical Control.
