# Home Assistant companion integration

> **Which Brilliant?** This integration is for **Brilliant Smart Home Control** — the in-wall touchscreen control panels (1–4 switch and plug-in models) made by **Brilliant NextGen, Inc.** ([brilliant.tech](https://www.brilliant.tech), San Mateo, CA). It is **not** affiliated with the Australian **"Brilliant Smart"** lighting brand (smart plugs/bulbs/cameras) or any other "Brilliant" product. It replaces the panel's HomeKit-Controller path with a local MQTT / Home Assistant bridge.

A small Home Assistant **custom integration** that manages the lifecycle of the
on-panel `brilliant-mqtt` agent across your fleet — deploy, OTA-survival repair,
version updates, and removal — all from the HA UI. It lives in this repo under
[`custom_components/brilliant_mqtt/`](../custom_components/brilliant_mqtt) (at the
repo root for HACS compliance; its py3.14 tooling and tests are in `ha/`).

## What it manages (and what it does not)

The integration manages **the agent, not the devices**. Your lights, switches,
sensors, and panel controls continue to arrive as **native MQTT-Discovery
entities published by the agent itself** — exactly as they do without the
integration. The integration adds nothing to that data path and is not in the
loop for state or commands.

What it adds is **fleet lifecycle**: it talks to each panel over SSH (root,
password-only) to install/update/repair the agent, and it watches each panel's
MQTT availability (LWT) and retained bridge-meta topic to drive an automatic
post-firmware-OTA repair. If you remove the integration, the agents keep running
and the device entities are unaffected.

One config entry = one panel. Each entry stores **its own** root password (the
operator runs per-controller root passwords). The integration attaches its
management entities to the **same HA device** the agent already publishes, so
they appear on the existing per-panel device page.

## Install

### Via HACS (custom repository)

1. In HACS → **Integrations** → ⋮ → **Custom repositories**, add
   `joyfulhouse/brilliant-mqtt` with category **Integration**.
2. Install **Brilliant MQTT Panel Manager**, then restart Home Assistant.
3. Add it under **Settings → Devices & Services → Add Integration → Brilliant
   MQTT** (one add per panel — see the onboarding flow below).

HACS installs the release zip, whose contents extract straight into
`config/custom_components/brilliant_mqtt/`. The zip bundles the agent payload
(the agent package, vendored py3.10 MQTT wheels, the systemd unit, and a
`VERSION` file) used for deploy/update/repair — you do not stage anything by
hand.

### Manual (release zip)

Download `brilliant_mqtt.zip` from the
[latest release](https://github.com/joyfulhouse/brilliant-mqtt/releases) and
extract it into `config/custom_components/brilliant_mqtt/` so that
`manifest.json` sits at the root of that folder. Restart Home Assistant and add
the integration as above.

## Onboarding a panel

Adding the integration walks a **detection-first** flow. The path taken depends
on whether the agent is already installed on the panel:

| Step | New panel (no agent) | Already has agent |
|---|---|---|
| **1. Connect** | Enter Host + Root password. Integration SSHes in, pins the host key (TOFU), detects no agent. | Enter Host + Root password. Integration SSHes in, detects the running agent. |
| **2. MQTT broker** | Enter broker host / port / user / password. Pre-filled from the most recently added panel (broker is fleet-shared); root password is never pre-filled. | _Skipped_ — broker is read back from the live env file. |
| **3. Panel settings** | Set Panel Name (e.g. "Office Bath" → slug `office-bath`) and Mesh priority (`MESH_PRIORITY`: 0 = never lead; 1 = primary; 2/3 = standbys). Optionally enable **Voice satellite** (see [Voice satellite](#voice-satellite)). On submit: agent is installed over SSH, then entry is created. | _Skipped_ — name + mesh priority + broker are adopted verbatim from the running agent. Panel is left untouched. |
| **Result** | Agent installed; panel entities fill in after first MQTT publish. | Panel adopted; entry created immediately. |

**Install failure:** if the SSH install fails at step 3, the step stays open
with `cannot_install` and **no entry is created** — fix the panel and retry.

**Adopting a hand-deployed panel:** onboarding reads `BRILLIANT_PANEL` from the
live env file and adopts it verbatim. Set it to a lowercase slug
(`^[a-z0-9_-]+$`) before adding — a non-slug or the reserved value `mesh` is
refused (`cannot_read_config`). See [INSTALL.md](../INSTALL.md) for manual
deploy steps.

**Slug is immutable** after creation (rename = remove + re-add).

**Reconfigure** lets you change host, root password, broker, or mesh priority
later — it re-validates over SSH and pushes the change to the panel. If the host
is unchanged, the new password is verified against the **stored** host key (key
checked before auth), so rotating a password can't silently accept a swapped
key. If the host changes, a fresh TOFU connect re-pins to the new host.

Behavior knobs are under **Configure** (Options).

## Entities

Each panel's device gains six management entities (three diagnostic, three control):

| Entity | What it is |
|---|---|
| `update.brilliant_<panel>_bridge` | Agent **update** entity. Installed version comes from the panel's retained bridge-meta (`agent_version`); latest from the bundled payload's `VERSION`. Installing pushes the bundled payload and restarts the agent. |
| `binary_sensor.brilliant_<panel>_bridge_health` | Bridge **health** (device class `problem`). `on` = needs attention (offline past grace with auto-repair off, a repair step failed, or a repair ran but the bridge stayed offline). Attributes: `reason`, `availability`. |
| `button.brilliant_<panel>_repair_bridge` | **Manual repair** — restores the unit/env and starts the agent (installs agent code first if missing), bypassing the auto-repair cooldown. |
| `switch.brilliant_<panel>_voice_satellite` | **Voice satellite** — enable installs and starts the satellite; disable uninstalls it. |
| `select.brilliant_<panel>_wake_word` | **Wake word** — choose `okay_nabu` (default), `hey_jarvis`, or `hey_mycroft`; changing it restarts the satellite. |
| `switch.brilliant_<panel>_wi_fi_watchdog` | **Wi-Fi watchdog** — enable installs and starts the on-panel Wi-Fi watchdog daemon (auto-recovers lost Wi-Fi: reconnect → restart networking → reboot as a last resort, see [CONFIGURATION.md → Wi-Fi watchdog](CONFIGURATION.md#wi-fi-watchdog)); disable uninstalls it. |
| `switch.brilliant_<panel>_bus_watchdog` | **Bus watchdog** — enable installs and starts the on-panel bus-health watchdog daemon (reboots the panel if the Brilliant message bus stays wedged 30+ minutes, gated on the bridge being active and the network being up, see [CONFIGURATION.md → Bus-health watchdog](CONFIGURATION.md#bus-health-watchdog)); disable uninstalls it. |
| `select.brilliant_<panel>_scene` | **Scene** — the panel's Brilliant scenes, populated from its accepted MQTT catalog. Changing it only updates the HA-local selection; it publishes no command. |
| `button.brilliant_<panel>_run_selected_scene` | **Run selected scene** — runs the selected scene with blocking execution confirmation. Available only while the scene transport, catalog, and a selection exist. |

The two scene entities are part of the HA control plane and scene bridge —
canonical semantics, MQTT contract, and safety model live in
[the scene bridge guide](brilliant-panel/home-assistant-integration.md).

Entity ids follow the panel's HA device name (`Brilliant <panel>`).

> **Two version numbers, on purpose.** The **HACS package version** (this
> integration, e.g. `0.2.0`) and the **on-panel agent version** are independent.
> The Bridge **update** entity tracks only the *agent* (the version the panel
> reports vs. the version this integration bundles), so it can read e.g. `0.1.0`
> with "no update available" while HACS shows the integration at `0.2.0` — that
> is expected, not a fault.

## Services

All three target devices (`target: device → integration: brilliant_mqtt`) and
fan out across every targeted panel:

| Service | What it does |
|---|---|
| `brilliant_mqtt.repair` | Restore unit/env from known-good sources and start the agent (same as the repair button). Failures are escalated per panel, not raised. |
| `brilliant_mqtt.redeploy` | Force-push the bundled agent payload and restart — the fleet-wide equivalent of the update entity's install. |
| `brilliant_mqtt.uninstall` | Stop, disable, and **remove** the agent from the panel (explicit only — never on entry removal). |

`redeploy` and `uninstall` attempt **every** targeted panel and then raise one
aggregated error naming any that failed (so a single bad panel never silently
skips the rest of a fleet wave).

Two further services run existing Brilliant scenes and modes:

| Service | What it does |
|---|---|
| `brilliant_mqtt.run_scene` | Run a scene on a panel and wait (up to 16 s) for confirmed execution — not just publication. `scene_id` must exist in the panel's current catalog. |
| `brilliant_mqtt.set_mode` | Set a mode on a panel with the same confirmation semantics. |

Both take an optional `panel` (defaulting to the configured scene panel) and
reject unknown fields and unknown IDs. Full semantics, error conditions, and
YAML examples: [scene bridge guide → Services](brilliant-panel/home-assistant-integration.md#services).

## Events

The integration fires `brilliant_mqtt_event` on the HA event bus. Every event
carries `panel`, `entry_id`, and a `type`; the table lists the per-type extras.
(Scene and mode executions fire the separate `brilliant_mqtt_scene` and
`brilliant_mqtt_mode` events — see
[scene bridge guide → Events](brilliant-panel/home-assistant-integration.md#events).)

| `type` | Meaning | Extra data |
|---|---|---|
| `panel_updated` | Panel **firmware** changed (seen on the bridge-meta topic). | `old_firmware`, `new_firmware` |
| `repair_started` | A repair began. | `trigger` (`auto` / `button` / `service`) |
| `repair_succeeded` | The bridge came back online after a repair. | — |
| `repair_failed` | A repair could not complete or the bridge stayed offline. | `reason` (`unreachable` / `host_key_changed` / `repair_step_failed` / `still_offline`), plus `error` or `journal` |
| `needs_attention` | The panel needs a human (escalation). | `reason` |
| `agent_updated` | The agent was updated to a new version. | `version` |
| `host_key_repinned` | A panel's SSH host key changed and was **auto-trusted** during repair/update (only when **Trust host-key changes** is on). | `new_host_key` |

Example — notify on anything that needs a human, and on repair outcomes:

```yaml
automation:
  - alias: "Brilliant bridge needs attention"
    trigger:
      - platform: event
        event_type: brilliant_mqtt_event
        event_data:
          type: needs_attention
    action:
      - service: notify.mobile_app
        data:
          title: "Brilliant panel {{ trigger.event.data.panel }}"
          message: "Needs attention: {{ trigger.event.data.reason }}"

  - alias: "Brilliant repair outcome"
    trigger:
      - platform: event
        event_type: brilliant_mqtt_event
        event_data:
          type: repair_succeeded
      - platform: event
        event_type: brilliant_mqtt_event
        event_data:
          type: repair_failed
    action:
      - service: notify.mobile_app
        data:
          title: "Brilliant {{ trigger.event.data.panel }}"
          message: "Repair {{ trigger.event.data.type.split('_')[1] }}"
```

## Options

Per-panel behavior knobs (under **Configure**); read live — no reload needed.

| Option | Default | What it does |
|---|---|---|
| **Auto-repair** (`auto_repair`) | `true` | On: outage past the grace period triggers automatic repair. Off: outage only notifies. |
| **Offline grace minutes** (`offline_grace_minutes`) | `10` | How long a panel may stay `offline` before repair/escalation kicks in. |
| **Repair cooldown minutes** (`repair_cooldown_minutes`) | `60` | Minimum gap between automatic repairs, preventing tight-loop repairs on a flapping panel. The manual repair button bypasses this. |
| **Trust host-key changes** (`trust_host_key_changes`) | `false` | **Off (default):** a changed SSH host key surfaces as `repair_failed: host_key_changed` with guidance to Reconfigure — the root password is never offered to the new-key host. **On:** repair/update auto-re-pins a changed key on the same-host panel so a key-rotating OTA recovers hands-off; fires an auditable `host_key_repinned` event. Only enable on a trusted/isolated network (e.g. a firewalled IoT VLAN). |

Seven further **HA control** settings (enable flag, entity label, room
overrides, domains, entity cap, default scene panel, scene actions) configure
the HA control plane and scene bridge. They are fleet-global values copied to
each entry; their validation rules and defaults are canonical in the
[scene bridge guide → Configuration](brilliant-panel/home-assistant-integration.md#configuration).

## Voice satellite

A Brilliant panel can act as a **Home Assistant ESPHome voice satellite**
(on-panel wake word + mic + speaker). STT, the conversation agent, and TTS all
run in your existing HA Assist pipeline — the panel is backend-agnostic.

**Quick start:**
1. During onboarding (step 3), toggle **Enable voice satellite**, choose a wake
   word, and optionally set a HA host override. The integration installs the
   satellite alongside the bridge agent.
2. Alternatively, flip the **Voice satellite** switch on the device page at any
   time (or use the **Wake word** select to change the wake word).
3. HA auto-discovers the satellite over zeroconf — accept the ESPHome device
   discovery, then assign an Assist pipeline under **Settings → Voice
   assistants**. The resulting device is managed by HA's built-in **ESPHome**
   integration; brilliant_mqtt remains MQTT-only and is not involved in the
   voice data path.

**Key facts:**
- Wake words bundled: `okay_nabu` (default), `hey_jarvis`, `hey_mycroft`.
- The satellite payload (~57 MB) is downloaded from the matching GitHub release
  asset and installed under `/var/brilliant-voice/` (OTA-persistent). It is
  cached after the first panel, so fleet installs are fast.
- If voice is enabled and the satellite goes missing (e.g. after a filesystem
  wipe), a `voice_missing` repair issue is raised — press **Repair** or the
  repair button to redeploy.
- `VOICE_HA_HOST`: only needed when the panel can't resolve your HA URL's
  hostname (e.g. a segmented IoT VLAN). Format: `hostname=ip`. Blank = use the
  panel's DNS.
- AEC (echo cancellation) ships **off** — the mic is closed during TTS, so
  normal use has no echo; AEC is only for barge-in.
- Resource-capped (`Nice=5`, `MemoryMax=300M`, `CPUQuota=100%`,
  `OOMScoreAdjust=500`) so wake inference can't starve the touchscreen UI.
  Coexists with the panel's built-in Alexa via ALSA mic sharing.

For the full guide (requirements, troubleshooting, advanced config) see
[docs/voice.md](voice.md).

## The OTA repair state machine

**Summary:** going offline arms a grace timer → if still offline when it
expires and auto-repair is on (cooldown elapsed) → SSH in, (re)install missing
agent code, rewrite unit + env from known-good sources, `enable --now` the
service → wait for LWT to flip back online → `repair_succeeded` or
`repair_failed` + `needs_attention`.

**Key behaviors:**
- Config is always **regenerated** from the stored entry, never read back from
  the panel — so a repair also heals config drift.
- A firmware change on the bridge-meta topic fires `panel_updated` and
  re-stages the config copies under `/var`.

**Caveat — repair can't fix bus-lib drift.** Repair restores configuration and
the unit; it does not change the agent code. If a firmware OTA changed the
on-panel message-bus API such that the agent can no longer communicate, the
service will start but the bridge won't come back online. The recovery timer
fires `repair_failed` (`reason: still_offline`, with a captured journal) and
`needs_attention`, because the agent itself needs a code fix — deploy a new
release via the update entity or `redeploy`.

## Security model

**Key points:**
- Root password is stored in the HA config-entry store (same exposure class as
  `secrets.yaml`). Protect the HA host accordingly. It is per-panel, never
  shared, and redacted from diagnostics.
- TOFU host-key pinning: the **first** connect trusts whatever host answers
  and pins its key (trust-on-first-use — like the first `ssh` to a new
  machine, the password is sent to an unverified host that one time; add
  panels from a trusted network). Every **later** connect verifies the pinned
  key **before** authenticating, so the root password is never offered to a
  changed or impostor host afterwards.
- Single auth attempt per connect (`client_keys=None`,
  `preferred_auth=("password",)`, keyboard-interactive disabled) so a wrong
  password can't burn through a lockout threshold.
- The integration only writes paths it owns: `/var/brilliant-mqtt/**`
  (including the Wi-Fi watchdog code, and `/var/brilliant-mqtt/bus_watchdog/**`
  for the bus watchdog, when enabled), `/var/brilliant-voice/**` (when voice is
  enabled), `/etc/brilliant-mqtt.env` (mode `0600`), `/etc/brilliant-voice.env`
  (mode `0600`), and the systemd units
  `/etc/systemd/system/brilliant-mqtt.service` /
  `brilliant-voice.service` / `brilliant-wifi-watchdog.service` /
  `brilliant-bus-watchdog.service`. The running bridge itself (not the
  integration) also stamps a liveness heartbeat to the tmpfs path
  `/run/brilliant-mqtt/bus-heartbeat`, cleared on every reboot.

**OTA host-key rotation** is handled in two modes (see the **Trust host-key
changes** option above): the default surfaces a changed key as a detectable
failure (re-pin by removing and re-adding the panel); the opt-in mode
auto-re-pins on the same-host panel and fires an auditable event.

See also [ARCHITECTURE.md](ARCHITECTURE.md) and
[reference/deployment.md](reference/deployment.md).
