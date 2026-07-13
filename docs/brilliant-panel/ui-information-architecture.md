# UI/UX information architecture

## Implementation evidence

The native UI is a single stripped ARM ELF that embeds the application, QML, fonts, sounds, JavaScript, images, generated Thrift clients, and much of its resource catalog. Static analysis recovered:

- 225 registered QML types;
- 148 registered `*Screen` types;
- 426 PNG assets;
- readable QML for many wrapper/views;
- controller and model names for navigation, rooms, devices, groups, scenes, shortcuts, media, security, mesh, and settings;
- direct message-bus/state-manager bindings.

Screen presence is not the same as availability. Some screens are factory-only, developer-only, demo-only, partner-specific, subscription-gated, feature-flagged, or require a matching live peripheral.

## Interaction grammar

The UI is a portrait, touch-first stack application built around a small number of repeated patterns:

| Pattern | Behavior |
|---|---|
| `StackManager` / `StackScreenWrapper` | Push/pop screen navigation with transition state and timing |
| `ScreenHeader` | Stable title and back affordance; some onboarding screens suppress Back |
| `BaseColumnLayout` | Consistent vertical rhythm and screen margins |
| `MenuRow` / `SectionHeader` | Settings and list hierarchy |
| `ActionButton` | Primary, secondary, and tertiary action hierarchy |
| `ScreenPopupLoader` | Confirmation, keyboard, passcode, warning, and result overlays |
| `SecurityCodeEntryPopup` | Configuration/privacy gate with contextual audit labels |
| `ThrottledSlider` / `ThrottledUpDown` | Coalesced continuous control to avoid bus-write floods |
| `ConfirmationOptions` | Paired accept/cancel decision pattern |
| `NotificationView` | System alerts layered independently of the main stack |

The visual system is a black/dark canvas with white outline glyphs, Avenir typography, orange for primary Brilliant actions and selected configuration, blue for selection/linking states, green for active/success states, and yellow/red for warning/fault states. The embedded asset set contains explicit pressed/tapped variants, so feedback is visual rather than relying on platform chrome.

## Primary navigation

The home screen is both a launcher and a dashboard. `HomeScreenShortcutController` manages a four-quadrant shortcut layout plus an overflow menu. Each shortcut is a `BaseHomeMenuOptionData` with icon, pressed icon, category, badge, pinned state, error state, and optional sublabel.

The exact home-function vocabulary present in the binary is:

| Function | Native intent | HA relevance |
|---|---|---|
| Lights | Browse/control light peripherals | Implemented for panel and mesh loads |
| Music | Now-playing, groups, presets, volume | Missing as an HA-facing panel capability |
| Climate | Thermostats and readings | Supported by firmware; no native instance in this panel fleet |
| Intercom | Call/broadcast/live video | Separate media project; privacy-sensitive |
| Scenes | Activate/edit/create scenes | Safe catalog/event/confirmed-command bridge implemented off-panel; Office gate pending |
| Modes | Home-wide operating modes | Useful semantic bridge to HA alarm/presence/input-select state |
| Rooms | Room-first device navigation | HA area/room metadata is modeled, but does not create a native HA tile |
| Devices | Type/device browse and settings | Existing HA entity device grouping should align |
| Alexa | Native voice status/mute | HA voice satellite is intentionally separate |
| Shades | Position/tilt controls | Native schema exists; generic HA native tiles are blocked |
| Alarms | Alarm configuration/execution | Distinct from security system alarm state |
| Cameras | Live video/device view | Media subsystem, not core MQTT bridge |
| Access | Locks/access panels/garages | Native schemas exist; physical-Control HA hosting is rejected |
| Doorbell | Doorbell feeds, chime, paired lock/security | Deferred media/security tier |
| Solar | Questionnaire, estimates, savings | Firmware schema/UI present; data source partly cloud-derived |
| Add to Home | Device/group provisioning | Keep native; HA should not emulate mesh provisioning casually |
| HomeKit | Pair/manage fixtures and Controls | Fallback integration path |
| Security | Arm/disarm, sensors, cameras | Deferred; must preserve PIN semantics |
| Energy | Energy-system and load information | Per-load watts implemented; richer system data is missing |

Shortcut targets can be a home function, scene, room, mode, or single device. Supported single-device pin types include light, on/off device, music, camera, climate, access, security, shade, and always-on outlet. Shortcut errors explicitly distinguish offline devices, deleted targets, invalid scenes, video disabled/busy, unsupported actions, and high-wattage restrictions.

### HA design consequence

If native HA entities become feasible, they should use an officially owned
Virtual Control identity and complete room/type metadata rather than a parallel
web silo. Physical-Control hosting and raw injection are rejected. The supported
baseline intentionally uses HA scene surfaces and existing Brilliant scenes;
see the [HA integration guide](home-assistant-integration.md).

## Information hierarchy

```text
Lock / art screen
├── time and date
├── weather, music, device-status, climate, security, solar widgets
└── unlock / touch → Home

Home
├── four pinned shortcuts
├── overflow home functions
├── room/device navigation
├── alert and notification overlays
└── Settings
    ├── Lights / physical gang settings
    ├── Display and lock-screen widgets
    ├── Gestures and sliders
    ├── Audio
    ├── Motion
    ├── Connectivity and OTA
    ├── Privacy and configuration lock
    ├── Home, users, rooms, and groups
    ├── Partner integrations and HomeKit
    └── Advanced / diagnostics / developer / factory
```

## Rooms, devices, and groups

Rooms are first-class configuration records. `RoomsScreen` presents a grid, can filter to rooms containing devices, and can create a room behind the configuration passcode. The home-layout controller maps peripherals and device groups to room IDs.

Device browsing is type-aware rather than service-aware. The UI has explicit display data and icons for:

- lights and on/off devices;
- speakers/music;
- locks and access panels;
- thermostats and climate sensors;
- Brilliant displays;
- shades;
- garages;
- security systems;
- energy systems;
- always-on outlets.

Device groups have group toggle and group intensity actions, power summaries, offline/unsupported counts, and editable membership. For Home Assistant, areas map naturally to rooms and HA groups/scenes can map to group/scene semantics, but IDs and deletion behavior must be reconciled deliberately.

## Scenes and modes

The scene editor is capable of more than light toggles. Static controller properties show support for:

- on/off and intensity;
- secondary levels such as thermostat ranges or shade tilt;
- colors;
- playlists, presets, favorites, repeat/shuffle, and art libraries;
- multi-device selection and device groups;
- validity checking when referenced devices disappear.

The current home snapshot has only the auto-generated “All Lights Off” and “All
Lights On” scenes. The integration decodes this catalog and implements
catalog-allowlisted `last_executed_scene_id` requests that wait for a matching
execution record. It remains off-panel-tested until the Office hardware gate.

Modes are separate from scenes. The UI tracks configured modes, active and
manual mode IDs, scene membership, invalid scenes, geolocation-triggered scenes,
and a Brilliant subscription flag. The bridge preserves that distinction with
separate mode catalog/event/command topics and `set_mode`; it does not map a
mode silently to an HA scene.

## Sliders, gestures, and motion

The panel distinguishes three input systems:

1. **Cap-touch sliders:** each physical slider has a configuration binding. It may target a local/default gang or a selected home peripheral. Options include tap-to-toggle, slider index, calibration/reset, and double-tap scene assignment.
2. **Screen gestures:** single-finger and two-finger gesture configurations can select device subsets/actions. The UI contains gesture sampling and improvement flows with tap, flick, slide, direction, and feedback modes.
3. **Motion:** faceplate PIR/light/screen detection generates `movement_detected`; a separate motion-configuration peripheral controls whether motion wakes the display and whether/when it turns off again.

Mesh switch configuration contains `slider_config`, `double_tap_enabled`, and a scene binding, but no ordinary “last pressed” variable. That matches live observation: hardware input is consumed locally to execute its binding, while downstream load state changes appear on the bus. Exposing raw gestures as HA events will require one of:

- mapping an existing Brilliant scene execution to a configured HA action;
- decoding and participating in execution/configuration objects;
- instrumenting a lower-level input or message path outside the normal variable graph.

The shipped UI does not accept every home peripheral indiscriminately. Ghidra
analysis of `SwitchSliderSettingsScreen` shows that its constructor loads the
firmware's `Slider Gesture` peripheral-type capability set. The
`supportsSliderConfiguredPeripheral` getter resolves the configured
`device_id`/`peripheral_id` and tests the target type against that set; it does
not test the target's hosting `DeviceType`. `sliderCapabilitiesText` then has
explicit behavior for `LIGHT`, `OUTLET`, `GENERIC_ON_OFF`, `SHADE`, and a
special `MUSIC` path. The persisted `CapTouchSliderConfig` names the target
`device_id` and `peripheral_id`.

Consequently, DeviceType 6 (`VIRTUAL_CONTROL`) is not itself an eligibility
blocker for a correctly owned `LIGHT`. Its admission to the native selector is
still **not live-validated** because the Office home currently has no Virtual
Control or hosted HA light. Tile visibility alone does not prove
physical-slider assignability; the [slider feasibility analysis](slider-bridge-feasibility.md)
and Virtual Control gates require native selection, operator-performed physical
operation after separate approval, restart persistence, exact binding
restoration, and stale-reference cleanup.

The first option is the supported baseline because it adds no peripheral owner.
Hosting an HA-backed target remains blocked behind Virtual Control gates.

## Settings hierarchy

### Device and load settings

Relevant screens include `LightSettingsScreen`, `DeviceSettingsScreen`, `SliderSettingsScreen`, `SwitchSliderSettingsScreen`, `SwitchMotionSettingsScreen`, `MotionControlSettingsScreen`, `ConfigureLightScreen`, dimming-range calibration, compatibility checks, load warnings, and cap-touch tuning.

The UI supports naming, gang selection, dimming range, load compatibility, low-wattage/multi-way characteristics, slider reset, motion devices, tap-to-toggle, status light, and double-tap scene selection. Many of these are writable bus variables but intentionally absent from the default HA surface because changing electrical/load characteristics can be unsafe.

### Display and art

`DisplaySettingsScreen`, `ArtLibrarySettingsScreen`, `ArtPhotoSettingsScreen`, lock-screen widget controllers, and time selection cover:

- screen brightness and schedule/range;
- screen always-on/off behavior;
- art rotation and libraries;
- time/date display;
- weather, music, device status, climate, security, and solar lock widgets;
- night mode and motion wake/sleep behavior.

The integration currently exposes screen on/brightness, night mode, screensaver, several lock widgets, and wake/sleep-on-motion. Complex art libraries and widget configuration blobs remain native-only.

### Audio and voice

`AudioSettingsScreen`, speaker/microphone tests, Alexa setup/management, render templates, notification badges, music controls, and GStreamer player types cover system volume, alert volume, mic mute, Alexa state, chimes, media, and voice capture.

The HA voice satellite reuses the hardware locally but does not attempt to emulate the Alexa UI. A future HA media player/notify feature should reuse the proven audio pipeline while keeping it isolated from wake-word capture.

### Connectivity and updates

`ConnectivitySettingsScreen` manages current/backup Wi-Fi, Ethernet/PoE, forget/reassociate, ping checks, release tags, update availability, auto-update, update-now, and tracked release stages for Control and mesh firmware.

The current integration exposes connectivity status and firmware auto-update but not network reconfiguration, immediate update, beta channel, or mesh DFU. The latter are deliberate footguns and should remain behind guarded services or native UI.

### Privacy and governance

Embedded QML confirms that Privacy Settings is passcode-gated and links to:

- Remote Video Access;
- Configuration Lock;
- Change Home Passcode.

Device settings and device configuration modifications have their own passcode prompts. Advanced Settings can enable root login, vendor reverse SSH, reset all settings, and open third-party notices. These are governance operations, not casual dashboard switches.

`privacy_toggle` and `camera_on` are read-only status mirrors on the bus. The write permission is enforced; attempts to set a non-settable variable fail. HA should show these as diagnostic binary sensors, which the current mapping does.

## Alerts and notifications

The binary contains views and routing for:

- bad Wi-Fi and UART/load faults;
- new-device discovery;
- mandatory updates;
- Alexa logout/state;
- scene activation;
- building entry and doorbell;
- security alarms/intrusion;
- water leak and shutoff valve state;
- climate high/low temperature or humidity;
- deauthorized partner integrations;
- remote media and identify feedback.

The pilot's `notification_peripheral` has zero readable variables, so it is likely a command/event sink or uses dynamic state elsewhere. Treating it as a conventional MQTT sensor will not work without tracing its request format or UI notification controller.

## Media and intercom

Intercom has separate selection, call, broadcast, disabled, and active-call screens. The runtime exposes camera enablement, remote camera enablement, receive dimensions, volume/ring control, WebRTC/SDP, and demo audio/video assets. Doorbells add notification/chime/pairing with locks and security systems. Cameras add live view and remote-video access control.

This is a coherent future subsystem with its own privacy, signaling, transcoding, and resource requirements. It should not be folded into the lightweight MQTT state bridge.

## Onboarding, integrations, diagnostics

The UI also includes:

- startup login/pairing/home selection and automatic configuration;
- Wi-Fi/Ethernet/country/home location/device naming;
- mobile configuration QR and email verification;
- partner add/auth/relink/reset flows and mobile-only warnings;
- HomeKit pairing and per-fixture setup;
- mesh diagnostics, firmware tracking, and device updates;
- camera, microphone, speaker, PIR, ambient-light, thermal, UART, link-rate, captouch, and bootstrap tests;
- developer feature flags, release-stage selection, demos, and factory reset.

These screens explain why a static screen count overstates end-user IA. The supported community surface should focus on stable device semantics and leave provisioning, factory tests, partner OAuth, and destructive firmware controls to the native UI.
