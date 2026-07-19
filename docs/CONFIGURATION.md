# Configuration

Full configuration reference for brilliant-mqtt.

> **Status:** implemented and verified live on a pilot panel — this contract
> is enforced by the test suite (`config.py`).

## Runtime configuration (environment)

The agent is configured entirely from the environment, loaded by systemd from
`/etc/brilliant-mqtt.env` on the panel (written by hand or rendered by your
configuration management; keep credentials out of git).

### Core variables

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `BRILLIANT_PANEL` | yes | — | Stable panel slug (e.g. `office`); namespaces all MQTT topics and entity IDs. `mesh` is reserved — do not use. |
| `MQTT_HOST` | yes | — | Central broker hostname or IP (e.g. `192.0.2.10`) |
| `MQTT_PORT` | no | `1883` | Broker TCP port |
| `MQTT_USERNAME` | yes | — | Broker user (the dedicated `brilliant` user) |
| `MQTT_PASSWORD` | yes | — | Broker password — store in your secret store, never in git |
| `LOG_LEVEL` | no | `INFO` | Python log level: `DEBUG` turns on verbose tracing; `WARNING` quiets normal traffic |

### Polling and watchdog timers

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `RESYNC_SECONDS` | no | `300` (5 min) | Period of the full discovery + state re-sync |
| `HOT_POLL_SECONDS` | no | `2.0` seconds | Cadence of the scoped state poll that bounds staleness when bus push notifications die silently. Only changed payloads are published. `0` disables the hot poll. |
| `BUS_STALE_SECONDS` | no | `900` (15 min) | Rebuild the bus session when no push has arrived for this many seconds (half-dead stream watchdog). `0` disables. |

### Reconnect-storm breaker

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `RECONNECT_STORM_THRESHOLD` | no | `20` reconnects | Rebuild the bus session when the bus reconnects at least this many times within the window. Breaks a storm that the stale watchdog cannot detect (each reconnect resets its push clock). `0` disables. |
| `RECONNECT_STORM_WINDOW_SECONDS` | no | `60` seconds | Sliding window for `RECONNECT_STORM_THRESHOLD` |

### BLE mesh

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `MESH_PRIORITY` | no | `0` (never publish) | Participate in BLE-mesh leader election with this priority. Lower number wins; ties broken by panel name. `0` means this panel never publishes the mesh namespace. Set to `1` or higher on every panel that should be a standby. |
| `MESH_HEARTBEAT_SECONDS` | no | `10` seconds | How often the elected leader heartbeats its retained claim. A claim older than 3× this triggers a standby takeover. |

### Scene and mode bridge

Off by default. The HA integration renders `SCENE_BRIDGE_ENABLED=1` during its
normal reconfigure/redeploy path when **HA control** is enabled; you only set
these by hand for manual deploys. The bridge's behavior, MQTT contract, and HA
surfaces are documented in
[the HA control plane and scene bridge guide](brilliant-panel/home-assistant-integration.md).

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `SCENE_BRIDGE_ENABLED` | no | `0` | Enable the per-panel Brilliant scene/mode bridge on the agent's existing bus and MQTT sessions. Boolean spellings match `MOTION_RECONCILE_ENABLED`. |
| `SCENE_WATERMARK_FILE` | no | `/data/brilliant-mqtt/scene-watermarks.json` | Durable scene/mode execution watermarks, pending intents, and delivery state — used to suppress retained/reconnect replay. |

### Motion desired-state reconciler

The firmware reverts the motion **enable** flags to defaults within minutes
(thresholds persist in NVM; runtime enables reset). The reconciler records the
last value you commanded for the motion vars and re-asserts any that drift.

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `MOTION_RECONCILE_ENABLED` | no | `1` | `0`/`false`/`off`/`no` disable re-assertion (commands still pass through); any other unrecognized value fails startup. |
| `MOTION_RECONCILE_MIN_INTERVAL_S` | no | `60` seconds | Floor between re-assert attempts per (peripheral, variable). Also rotates the write slot across peripherals — keep above `0`. |
| `MOTION_RECONCILE_MIN_WRITE_SPACING_S` | no | `0.5` seconds | Minimum gap between consecutive reconciler bus writes, shared across the panel and mesh bridges. **This is the lever that tunes the catch-up ramp** after a mass drift (e.g. a mesh-leader restart): with spacing above `0`, each poll tick writes at most one peripheral. `0` disables spacing. |
| `MOTION_RECONCILE_MAX_WRITES_PER_TICK` | no | `4` | Cap on reconciler writes in a single poll tick. Only takes effect when the write spacing is `0` (see above). Must be at least `1`. |
| `MOTION_DESIRED_STATE_DIR` | no | `/var/brilliant-mqtt/state` | Where the per-bridge desired-state JSON files live. Keep it under `/var` so the state survives firmware OTA updates. |

### Score-derived motion

The firmware `movement_detected` latch on mesh loads never fires (verified
live: scores of 255 with a threshold of 45 never tripped it), so the bridge
derives the **Motion** sensor from the score stream instead: motion turns on
when `motion_score` ≥ the device's **Motion High Threshold** and stays on
until no qualifying spike has been seen for the hold window. Tune per room
with the Motion High Threshold number entity (persists on the device);
where **Motion Score Reporting** is off the sensor simply stays `off`.

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `MOTION_DERIVED_ENABLED` | no | `1` | `0`/`false`/`off`/`no` restore the raw firmware latch value (not recommended — it never fires); any other unrecognized value fails startup. |
| `MOTION_DERIVED_HOLD_S` | no | `60` seconds | How long motion stays `on` after the last score spike at or above the threshold. `0` pulses only on the spike itself. Must be ≥ 0. |

### Wi-Fi watchdog

An optional standalone daemon (`brilliant_wifi_watchdog`) that recovers a
panel's Wi-Fi when the gateway goes unreachable — escalating from a soft
`connman` reconnect, to a `connman`/`wpa_supplicant` service restart, to a
GPIO/SDIO Wi-Fi chip reset + reboot (guarded against reboot-looping).
Installed separately from the bridge, via the HA integration's **Wi-Fi
watchdog** switch (see [ha-integration.md → Entities](ha-integration.md#entities))
or manually from
[deploy/brilliant-wifi-watchdog.service](../deploy/brilliant-wifi-watchdog.service).
It reads the same `/etc/brilliant-mqtt.env` as the bridge — there is no
separate env file.

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `WIFI_WATCHDOG_INTERVAL` | no | `30` seconds | Probe loop cadence — how often the gateway is pinged. |
| `WIFI_WATCHDOG_GATEWAY` | no | _(auto)_ | Gateway IP to probe. Blank auto-detects the default gateway from the routing table every cycle. |
| `WIFI_WATCHDOG_SOFT_AFTER` | no | `90` seconds | Gateway unreachable this long triggers a soft `connman` reconnect (rung 1). |
| `WIFI_WATCHDOG_RESTART_AFTER` | no | `180` seconds | Escalates to restarting the `connman` + `wpa_supplicant` services (rung 2). |
| `WIFI_WATCHDOG_REBOOT_AFTER` | no | `360` seconds | Escalates to a GPIO/SDIO Wi-Fi chip reset + `systemctl reboot` (rung 3), subject to the reboot guard below. |
| `WIFI_WATCHDOG_REBOOT_COOLDOWN` | no | `3600` seconds (1 h) | Minimum gap between guard-permitted reboots. |
| `WIFI_WATCHDOG_REBOOT_CAP` | no | `3` reboots | Maximum reboots allowed inside the rolling window; the guard blocks further ones (logs only) past this. |
| `WIFI_WATCHDOG_REBOOT_WINDOW` | no | `21600` seconds (6 h) | Rolling window the reboot cap is measured over. |
| `WIFI_WATCHDOG_LOG` | no | `/var/brilliant-mqtt/wifi-watchdog.log` | Rotating log file (3 × 512 KB backups). |
| `WIFI_WATCHDOG_STATE` | no | `/var/brilliant-mqtt/wifi-watchdog.state` | Where reboot timestamps persist, so the cooldown/cap survive a watchdog restart. |

`MQTT_HOST` / `MQTT_PORT` (already set for the bridge) are reused to log
broker reachability alongside the gateway probe — informational only; broker
state never drives the escalation ladder.

### Hue CA recovery

An optional standalone oneshot (`brilliant_hue_ca`) for panels bridged to a
local [diyHue](https://diyhue.org) bridge: it re-appends your diyHue CA's
*public* certificate to the panel's pinned Hue trust bundle and restarts the
local Hue coordinator whenever it has to append — recovering from a firmware
OTA, which wipes `/data` (and the bundle with it) but not `/var`. This is what
keeps the panel's native Hue client trusting your diyHue bridge so a wall
slider can keep controlling an HA-backed bulb with no SmartThings/cloud
round-trip. **Off by default.** Installed via the HA integration's component
checklist (onboarding, or **Reconfigure** on an existing panel), or — like
the watchdogs below — toggled live afterward via the **Hue CA recovery**
switch (see [ha-integration.md → Entities](ha-integration.md#entities)).

Enabling it requires pasting the **diyHue CA certificate (PEM)** field: your
diyHue bridge's CA *public* certificate, PEM-encoded. The integration refuses
to install the component if this field is empty.

A `brilliant-hue-ca.timer` drives the oneshot: it fires once ~2 minutes after
boot, then every ~15 minutes thereafter — frequent enough to reliably catch a
post-OTA cert wipe without waiting on a manual restart.

> **Rollout note:** enable this on **every** bridged panel, not just the one
> currently hosting the Brilliant Hue integration. The Hue integration host can
> move to any panel (leader election, manual reassignment, panel replacement),
> and a panel without this hook would strand the integration — silently
> failing Hue-bridge TLS — if it became the host.

It reads the same `/etc/brilliant-mqtt.env` as the bridge (optionally — a
missing file is not an error) — there is no separate env file.

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `HUE_CA_CERT_PATH` | no | `/var/brilliant-hue-ca/injected-ca.pem` | Where the operator's injected CA PEM lives on the panel (written by the HA integration when the component is installed). |
| `HUE_CA_BUNDLE_PATH` | no | the pinned `.../lib/certs/hue-bridge-ca-certs.pem` under the panel's Python 3.10 site-packages | The panel's pinned Hue CA trust bundle to append into. If this path doesn't exist, the hook falls back to a glob under `HUE_CA_SITE_PACKAGES`. |
| `HUE_CA_SITE_PACKAGES` | no | the panel's Python 3.10 `site-packages` root | Glob-fallback root searched for `hue-bridge-ca-certs.pem` when `HUE_CA_BUNDLE_PATH` doesn't exist (e.g. after a firmware version bump moves the path). |
| `HUE_CA_VASSAL_INI` | no | `/var/run/brilliant/processes/hue_bridge_peripherals.ini` | The Hue coordinator's uWSGI vassal control file. Its presence means this panel currently hosts Hue; touching it (`os.utime`) triggers the emperor to reload that vassal — the "restart" this hook performs after appending. |
| `HUE_CA_LOG` | no | `/var/log/brilliant-hue-ca.log` | Rotating log file (256 KB × 2 backups) for each oneshot run. |

Each run is idempotent: the hook compares certificates by SHA-256 fingerprint
of the DER encoding, so it only appends (and only restarts the coordinator)
when your CA is actually missing from the bundle — a no-op run on every other
timer tick.

### Bus-health watchdog

An optional standalone daemon (`brilliant_bus_watchdog`) that reboots the
panel when the bridge has been unable to hold a Brilliant message-bus session
for **30 minutes** (default) — a known wedge mode where the on-panel
`message_bus` vassal accepts the socket but hangs mid-handshake, and only a
reboot clears it (a bridge restart does not). Gated so it fires only for that
specific failure: the bridge's systemd unit must still be **active** (never
reboots a deliberately stopped or uninstalled bridge), and the network
gateway must be **reachable** (a plain network outage is the
[Wi-Fi watchdog](#wi-fi-watchdog)'s job — this watchdog defers while the
gateway is down, so the two never race each other). Guarded against
reboot-looping with its own cooldown/cap, tracked in its own state file,
independent of the Wi-Fi watchdog's guard. Installed separately from the
bridge, via the HA integration's **Bus watchdog** switch (see
[ha-integration.md → Entities](ha-integration.md#entities)) or manually from
[deploy/brilliant-bus-watchdog.service](../deploy/brilliant-bus-watchdog.service).
It reads the same `/etc/brilliant-mqtt.env` as the bridge — there is no
separate env file.

The bridge stamps a heartbeat file after every successful bus read
(`BUS_HEARTBEAT_FILE`, below) — a tmpfs path by default, so there is no flash
wear. During a wedge, the bus read never completes, so the heartbeat naturally
stops updating with no special wedge-detection needed in the bridge itself;
the watchdog just measures how stale that one file is.

| Variable | Required | Default | Meaning |
|---|---|---|---|
| `BUS_WATCHDOG_INTERVAL` | no | `60` seconds | Poll loop cadence. |
| `BUS_WATCHDOG_STALE_AFTER` | no | `1800` seconds (30 min) | Heartbeat age that triggers a reboot, once the gating conditions above also hold. |
| `BUS_WATCHDOG_GATEWAY` | no | _(auto)_ | Gateway IP to probe for the network-up gate. Blank auto-detects the default gateway from the routing table every cycle. |
| `BUS_HEARTBEAT_FILE` | no | `/run/brilliant-mqtt/bus-heartbeat` | **Shared with the bridge** (same variable name on both sides) — tmpfs path the bridge stamps and the watchdog reads. An empty value on the bridge disables emission; leave it set for the watchdog to have anything to check. |
| `BUS_WATCHDOG_STATE` | no | `/var/brilliant-mqtt/bus-watchdog.state` | Where reboot timestamps persist (its own file — deliberately separate from the Wi-Fi watchdog's ledger, so the two guards never race on the same file). |
| `BUS_WATCHDOG_LOG` | no | `/var/brilliant-mqtt/bus-watchdog.log` | Rotating log file (3 × 512 KB backups). |
| `BUS_WATCHDOG_REBOOT_COOLDOWN` | no | `3600` seconds (1 h) | Minimum gap between guard-permitted reboots. |
| `BUS_WATCHDOG_REBOOT_CAP` | no | `3` reboots | Maximum reboots allowed inside the rolling window; the guard blocks further ones (logs only) past this. |
| `BUS_WATCHDOG_REBOOT_WINDOW` | no | `21600` seconds (6 h) | Rolling window the reboot cap is measured over. |
| `BRIDGE_SERVICE` | no | `brilliant-mqtt` | systemd unit checked with `is-active` before a reboot — must be active for the watchdog to fire. |

Example `/etc/brilliant-mqtt.env`:

```
BRILLIANT_PANEL=office
MQTT_HOST=192.0.2.10
MQTT_PORT=1883
MQTT_USERNAME=brilliant
MQTT_PASSWORD=<your broker password>
LOG_LEVEL=INFO
```

---

## Voice configuration

> These vars are for **manual or advanced setups only.** When you enable voice
> via the HACS integration the HA integration writes `/etc/brilliant-voice.env`
> for you — you do not need to touch these directly.

The voice satellite is opt-in, per panel. Enable it in the HA integration during
onboarding (or later via the **Voice satellite** switch on the panel's device
page). The satellite advertises via zeroconf so HA auto-discovers it as an
ESPHome `assist_satellite` entity.

| Variable | Default | Meaning |
|---|---|---|
| `BRILLIANT_PANEL` | _(required)_ | Same panel slug as the bridge — shared with the main agent |
| `VOICE_NAME` | `Brilliant <panel>` | Display name for the satellite in HA (e.g. `Brilliant Office`) |
| `VOICE_API_PORT` | `6053` | ESPHome native API port that HA connects to |
| `VOICE_WAKE_WORD` | `okay_nabu` | Active wake word: `okay_nabu`, `hey_jarvis`, or `hey_mycroft` |
| `VOICE_HA_HOST` | _(empty)_ | `hostname=ip` mapping (e.g. `homeassistant.example=10.0.0.5`) — only needed when the panel cannot resolve your HA URL's hostname (e.g. IoT VLAN). Leave blank to rely on the panel's own DNS. |
| `VOICE_ENABLE_AEC` | `off` | `on` to enable acoustic echo cancellation (for barge-in). Ships off — not needed for normal use because the mic is muted during TTS. |
| `VOICE_MIC_DEVICE` | `default` | ALSA capture device. `default` uses the panel's own tuned DSP chain (recommended; far-field wake detection verified at 0.996 confidence). |
| `VOICE_SND_DEVICE` | `plug:dmix_48000` | ALSA playback device. `plug:dmix_48000` mixes with the panel's other audio. |
| `VOICE_AEC_MIC_DEVICE` | `plug:dsnoop_48000` | ALSA capture device for the AEC daemon (raw 2-mic tap, pre-DSP). Only used when `VOICE_ENABLE_AEC=on`. |
| `VOICE_AEC_DELAY_MS` | `0` ms | Loudspeaker-to-mic delay hint for the AEC algorithm |
| `VOICE_AEC_TYPE` | `1` | AEC algorithm: `0`=DSP_WIDGETS, `1`=SPEEX, `2`=WEBRTC |
| `LOG_LEVEL` | `INFO` | Shared with the bridge. Set `DEBUG` for voice tracing. |

See [docs/voice.md](voice.md) for the full end-to-end voice setup guide.

---

## MQTT topic scheme

| Purpose | Topic | Notes |
|---|---|---|
| Discovery | `homeassistant/<component>/brilliant_<panel>_<peripheral>[_<var>]/config` | retained |
| State | `brilliant/<panel>/<peripheral>/state` | retained JSON, shared by all of a peripheral's entities |
| Load command | `brilliant/<panel>/<peripheral>/set` | JSON (HA light `schema: json`; switch payloads) |
| Aux command | `brilliant/<panel>/<peripheral>/set_<variable>` | plain payloads: `ON`/`OFF` (switches), numeric (numbers), `PRESS` (buttons) |
| Availability | `brilliant/<panel>/availability` | `online`/`offline`, LWT, retained |
| Mesh namespace | `brilliant/mesh/...` (same shapes as above) | published by the **elected leader panel only** — publisher-agnostic, so failover causes zero HA churn |
| Mesh leadership | `brilliant/mesh/leader` | retained claim `{"panel", "priority"}` + heartbeat |
| HA control plane & scene bridge | `brilliant/ha-control/v1/...` | versioned JSON contract: HA-owned entity manifest/state/command/result, plus per-panel scene/mode catalogs, events, commands, results, and status. Exact topics and payload fields: [MQTT version 1 contract](brilliant-panel/home-assistant-integration.md#mqtt-version-1-contract) |

---

## BLE mesh loads (elected publisher)

Brilliant's plug-in switches and mesh dimmers live on the bus's virtual
`ble_mesh` device, visible identically from every panel. Exactly **one** panel
publishes them — under the reserved `mesh` pseudo-panel / "Brilliant BLE Mesh"
HA device — elected by `MESH_PRIORITY`.

### How leader election works

- The leader heartbeats a retained claim every `MESH_HEARTBEAT_SECONDS`;
  standbys are completely silent on the mesh namespace.
- If the leader goes quiet for 3× the heartbeat, the best-priority standby
  takes over: it republishes discovery/state and takes the command subscriptions.
- When a higher-priority panel returns, it preempts the current leader and
  control moves back automatically.

### Known gaps

- Commands sent during a takeover window (≤ ~30 s) are lost.
- The leadership claim has no LWT — a whole-fleet outage leaves the claim stale
  until a panel returns.

### Power sensors and motion

- Mesh loads whose `power` reports `-1` (uncalibrated) get no power sensor; the
  entity appears automatically once real wattage shows up.
- Each mesh load also exposes a **`Motion` binary_sensor** (device_class:
  motion, enabled by default). Additional disabled-by-default entities:
  `Motion Score` (diagnostic sensor), `Motion Score Reporting` (config
  switch), and `Motion High Threshold` / `Motion Low Threshold` (config
  numbers, 0–255 — the score is 8-bit). Panel loads are unaffected — these entities
  only appear when the backing bus variables are present.

> For mesh motion to reflect real presence you must enable the device's
> **Motion Score Reporting** switch first. See
> [Troubleshooting → Mesh-load motion](TROUBLESHOOTING.md#mesh-load-motion-always-reads-off-or-read-a-stuck-on)
> for the full tuning steps.

---

## Entities per panel

Each panel exposes several dozen entities (many disabled by default) across
these categories:

- **Loads** (per gang): `light` (dimmer) or `switch`, with brightness where
  the hardware supports it.
- **Power & electrical**: per-gang `Power` (W) — including always-on circuits —
  plus `Temperature` and a `Fault` problem sensor (diagnostic).
- **Panel controls** (config category): `Microphone Mute`, `Screen`,
  `Screen Brightness` (0–10), `Volume` / `Alert Volume` (0–100),
  `Child Lock`, `Night Mode`, `Faceplate LED`, `Illuminance Sensor` enable,
  and an `Identify` button. Disabled-by-default governance/audio extras: see
  [Governance & audio switches](#governance--audio-switches).
- **Screen, screensaver & sliders**: wake/sleep-on-motion, screensaver and
  lock-screen widgets, touch-slider and intercom controls — see
  [Screen, screensaver & slider controls](#screen-screensaver--slider-controls).
- **Presence & privacy**: `In Use` (someone at the panel, occupancy),
  `Motion` + `Illuminance` (faceplate), `Camera` active and `Privacy Mode`
  (read-only, diagnostic). Advanced motion-source tuning is disabled by
  default — see
  [Faceplate motion-detection controls](#faceplate-motion-detection-controls).

  `In Use` reflects touchscreen interaction, not room presence.
- **Voice** (when enabled): `Voice satellite` switch and `Wake word` select.
- **Diagnostics**: `CPU Temperature`, `Wi-Fi` / `Internet` connectivity,
  `NTP Sync`, `PIR Score`, `Internal Temperature` (some disabled by default);
  panel firmware version on the HA device page (`sw_version`).

All entities are grouped under one HA device (`brilliant_panel_<panel>`), with
availability tied to the panel's LWT topic.

### Screen, screensaver & slider controls

| Entity | Component | Bus var | Enabled by default |
|---|---|---|---|
| Wake Screen on Motion | switch | `trigger_screen` | Yes |
| Sleep Screen After Motion Stops | switch | `trigger_screen_off` | Yes |
| Screen Off Timeout | number (30–3600 s) | `trigger_screen_off_timeout_sec` | Yes |
| Screensaver | switch | `on` (payload key `screensaver_on`) | Yes |
| Show Time & Date | switch | `display_time_date` | Yes |
| Weather Widget | switch | `weather_widget_on_lock` | No |
| Music Widget | switch | `music_widget_on_lock` | No |
| Device Status Widget | switch | `device_status_on_lock` | No |
| Solar Savings Widget | switch | `solar_savings_on_lock` | No |
| Touch Sliders | switch (inverted) | `disable_cap_touch_sliders` (payload key `touch_sliders_enabled`) | Yes |
| Intercom Broadcasts | switch | `receive_intercom_broadcasts` | Yes |
| Slider Double-Tap Timeout | number (100–1000 ms) | `slider_double_tap_timeout_ms` | No |

`Touch Sliders` is inverted: the bus variable is a *disable* flag, but the
switch (and its payload key) reads as "sliders usable" — ON means the touch
sliders work.

### Faceplate motion-detection controls

Shipped in 0.2.3, disabled by default (advanced tuning). `movement_detected`
is driven by whichever detection switch below is on; the PIR thresholds only
take effect once **PIR Score Reporting** is on.

| Entity | Component | Bus var | Enabled by default |
|---|---|---|---|
| Screen Motion Detection | switch | `enable_screen_motion_detection` | No |
| PIR Score Reporting | switch | `enable_pir_motion_score` | No |
| Light Motion Detection | switch | `enable_light_motion_detection` | No |
| PIR Motion High Threshold | number (0–100) | `pir_motion_detection_high_threshold` | No |
| PIR Motion Low Threshold | number (0–100) | `pir_motion_detection_low_threshold` | No |

### Governance & audio switches

Disabled by default — opt in per panel.

| Entity | Component | Bus var | Enabled by default |
|---|---|---|---|
| Speaker Ducking | switch | `duck_speaker` | No |
| Low Temperature Mode | switch | `low_temp_mode` | No |
| Firmware Auto-Update | switch | `software_update_enabled` | No |
| Remote Assistance | switch | `remote_assistance_enabled` | No |

`Firmware Auto-Update` off stops the panel receiving vendor security patches;
`Remote Assistance` on opens Brilliant's reverse-SSH support tunnel. Both are
opt-in governance surfaces, not fleet-wide defaults.

---

## Broker user and ACL

The bridge uses a dedicated `brilliant` broker user scoped to its own topics:

```
# /etc/mosquitto/acl
user brilliant
topic readwrite brilliant/#
topic write homeassistant/#
```

- `brilliant/#` — read/write for state, commands, and availability. This also
  covers the `brilliant/ha-control/v1/#` control-plane and scene-bridge topics;
  no extra ACL entry is needed for the panel agent.
- `homeassistant/#` — write for MQTT Discovery configs

If you use the scene bridge, Home Assistant's own MQTT user must additionally
be able to read **and publish** under `brilliant/ha-control/v1/#` (the HA
integration publishes the manifest, state, and commands there). A full-access
HA broker user already satisfies this.

**Mosquitto ACL denials are silent.** A publish to a denied topic is dropped
with no error to the client. If entities never appear or commands are silently
swallowed, check the ACL first — it is the most common cause.

For provisioning steps (password file, `mosquitto.conf` references, and the
HA Mosquitto add-on path) see [INSTALL.md](../INSTALL.md#step-2--set-up-the-mqtt-broker).

---

## Home Assistant

No HA-side configuration is required: entities appear via MQTT Discovery as
native MQTT entities (light/switch/sensor per device capability), and recover
automatically across HA restarts from retained discovery + state.
