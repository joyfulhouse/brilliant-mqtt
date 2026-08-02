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

One config entry represents the Brilliant MQTT **fleet**. It stores the single
broker profile and installation-global Home Assistant control settings. Each
panel is a Home Assistant config subentry with its own pinned SSH identity,
address, root password, stable MQTT slug, and feature overrides. Management
entities still attach to the **same HA device** the agent publishes, so they
appear on the existing per-panel device page.

## Install

### Via HACS (custom repository)

1. In HACS → **Integrations** → ⋮ → **Custom repositories**, add
   `joyfulhouse/brilliant-mqtt` with category **Integration**.
2. Install **Brilliant MQTT Fleet Manager**, then restart Home Assistant.
3. Add it under **Settings → Devices & Services → Add Integration → Brilliant
   MQTT**. Add the integration once; later panels use its native **Add panel**
   action, shown as **Add Brilliant panel**.

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

## MQTT prerequisite

MQTT remains the lightweight transport between Home Assistant and every panel,
so configure one broker before adding Brilliant MQTT. Choose either supported
path:

- **Home Assistant Mosquitto add-on (Recommended):** install and start the
  official `core_mosquitto` add-on first.
- **Existing MQTT broker:** use a compatible local, remote, or hosted broker.
  This path is at the same setup level and never requires `core_mosquitto`.

For either path:

- connect Home Assistant's MQTT integration to the broker first;
- create a dedicated, non-owner Brilliant username/password instead of reading
  or reusing Home Assistant's hidden generated MQTT credential;
- enter a LAN hostname/IP and TCP port the panels can resolve and reach (an
  internal app hostname may work only inside Home Assistant); and
- keep Home Assistant's effective MQTT Discovery prefix fixed at
  `homeassistant` for current agent compatibility.

The two choices normalize to the same broker profile and run the same
behavioral validator. It checks Home Assistant MQTT readiness, the dedicated
Brilliant credential, panel-client → Home Assistant and Home Assistant →
panel-client traffic, Discovery writes, retained delivery, and cleanup.
Panel provisioning later repeats the relevant checks from a staged,
non-active agent before activation.

Failures are stage-specific and secret-safe. Correct the reported
prerequisite, follow the linked cause/check/fix/retry guidance, and retry:
[Home Assistant MQTT unavailable](install/mqtt-broker.md#ha_mqtt_unavailable),
[authentication](install/mqtt-broker.md#fleet_auth_failed),
[panel → Home Assistant](install/mqtt-broker.md#panel_to_ha_timeout),
[Home Assistant → panel](install/mqtt-broker.md#ha_to_panel_timeout),
[Discovery ACL](install/mqtt-broker.md#discovery_write_denied),
[retained messages](install/mqtt-broker.md#retained_message_invalid), or
[cleanup](install/mqtt-broker.md#cleanup_failed).

The integration never installs or starts `core_mosquitto`, creates accounts,
edits ACLs, or modifies broker configuration. It supports plaintext TCP,
strict system/public-CA TLS, and strict custom-CA TLS; TLS verification never
falls back to plaintext. The
[MQTT broker prerequisite](install/mqtt-broker.md) has the two-principal ACL
table and transport details.

## Onboarding a panel

Initial setup is fleet-first:

1. Choose **Home Assistant Mosquitto add-on (Recommended)** or **Existing MQTT
   broker**, then enter the dedicated Brilliant broker profile. The same form
   has one optional **Enable Home Assistant control and scenes** checkbox. It
   defaults off and must be chosen here if the first panel should run the scene
   bridge; changing that flag after panels exist requires a guided agent rollout.
2. Home Assistant runs the identical behavioral validation described above.
   A failure creates no Brilliant entry or panel side effect.
3. After validation succeeds, Home Assistant creates and durably verifies an
   empty Brilliant MQTT fleet. Only then does it chain into the first **Add
   Brilliant panel** flow.
4. Enter only the panel address and root password. Home Assistant obtains the
   SSH host key before authentication, pins that exact candidate for the
   authenticated inspection, and shows the detected fingerprint and panel
   facts before provisioning.
5. Give the panel a friendly name. Home Assistant allocates its immutable MQTT
   slug and the next unused positive mesh priority automatically.
6. Provisioning snapshots the current state, stages the candidate release,
   validates MQTT from the panel path, activates it, and waits for fresh
   availability, metadata, state, and Discovery. A failed candidate is rolled
   back.

If first-panel setup fails or is cancelled, the validated empty fleet remains
available for retry; broker settings do not need to be entered again. To add
another panel, open the existing Brilliant MQTT fleet and use its native **Add
panel** action, shown as **Add Brilliant panel**. Later additions use the same
panel flow and never repeat broker settings, optional-feature choices, raw JSON,
or mesh priority.

The MQTT slug and mesh priority remain stable after creation. Renaming changes
only the display name; it does not rename topics, devices, entity unique IDs,
or the stored management identity. Optional components and overrides are
configured after the base panel is healthy.

The staged software checks do not claim qualification across every Brilliant
firmware and network combination. Pilot and soak one panel before expanding
the fleet.

### Panel onboarding errors

The **Open troubleshooting** link on the panel connection and confirmation
screens returns here. Leave the failed flow open while correcting the reported
condition; the form keeps only a stable error code and never retains raw SSH,
broker, command, or credential text.

- `cannot_connect`, `host_unreachable`, or `panel_authentication_failed`:
  confirm root SSH is enabled, the address is reachable from Home Assistant,
  and the root password is current. Retry the same form.
- `host_key_missing`, `host_key_malformed`, `host_key_unsupported`, or
  `host_key_fingerprint_invalid`: stop and verify the panel firmware and SSH
  service. Do not bypass host-key verification.
- `host_key_changed` or `panel_identity_mismatch`: stop and verify the physical
  panel and address. Use **Replace physical panel** only for an intentional
  replacement whose old and new fingerprints you can review.
- `firmware_unknown`, `unsupported_architecture`, `unsupported_python`,
  `unsupported_panel_toolchain`, `insufficient_memory`, or
  `insufficient_storage`: correct the reported compatibility prerequisite
  before retrying; the integration has not activated a candidate.
- Broker, TLS, authentication, ACL, retained-message, and MQTT timeout codes
  link directly to their error-specific section in the
  [MQTT prerequisite guide](install/mqtt-broker.md).
- `stage_failed`, `preflight_failed`, `activation_failed`, or `health_failed`:
  the candidate did not become the committed release. Correct the stable cause
  and retry only after the flow reports that rollback completed.
- `rollback_failed`, `recovery_failed`, `journal_failed`, or
  `config_entry_storage_unavailable`: stop panel mutations, repair Home
  Assistant storage if indicated, and follow the persistent Brilliant MQTT
  repair issue before retrying. Do not delete the fleet entry to bypass the
  recovery journal.

<a id="inspection_failed"></a>
#### `inspection_failed`

- **Cause:** A panel identity or compatibility inspection failed unexpectedly,
  so the integration discarded the raw exception and stopped before changing
  the panel.
- **Check:** Confirm the panel remains reachable over root SSH, then inspect
  Home Assistant's Brilliant MQTT logs for the inspection stage and verify the
  panel firmware and free resources.
- **Fix:** Restore SSH/panel responsiveness or the reported compatibility
  prerequisite. If the stable code repeats on an otherwise healthy panel,
  collect secret-redacted Home Assistant diagnostics and report the failure.
- **Retry:** Reopen the same panel action after the panel is healthy. No
  candidate release was activated by this failed inspection.

<a id="provisioning_failed"></a>
#### `provisioning_failed`

- **Cause:** Provisioning hit an unexpected failure after its transactional
  safety path began; the integration attempted recovery and exposed no raw
  command, exception, or credential text.
- **Check:** Confirm there is no active Brilliant MQTT recovery issue, verify
  the prior panel release is healthy, and inspect the stable provisioning stage
  recorded in Home Assistant.
- **Fix:** Complete any reported recovery first, then correct the panel,
  storage, or network dependency identified for that stage. Do not delete the
  fleet entry or manually remove the recovery journal.
- **Retry:** Start **Add Brilliant panel** again only after recovery is clear
  and the prior release is healthy.

<a id="rebind_identity_unchanged"></a>
#### `rebind_identity_unchanged`

- **Cause:** **Replace physical panel** reached the same pinned SSH identity
  already stored for this panel, so it is not a physical replacement.
- **Check:** Compare the displayed address with the intended panel and confirm
  whether only its address or root password changed.
- **Fix:** Use **Change address** for a network-address change or **Repair SSH
  credentials** for a password change. Do not force an identity rebind.
- **Retry:** Close the replacement flow and start the matching focused panel
  action. No identity, credential, or panel setting was changed.

## Fleet and panel configuration

Open **Configure** on the fleet entry for focused, single-owner settings:

- broker and fleet credential;
- Home Assistant control;
- scenes and the default scene panel;
- fleet repair defaults; and
- mesh priorities under **Advanced**.

An empty fleet may accept a validated broker-profile change, and an unchanged
profile may be revalidated. Once panels exist, a changed broker endpoint, TLS
profile, or credential is deliberately deferred to a guided multi-panel
operation. The integration refuses the edit instead of leaving a mixed-broker
fleet. Broker edits also fail closed while an Add panel flow, provisioning
journal, or recovery transaction is active; see
[`broker_change_blocked_by_panel_onboarding`](install/mqtt-broker.md#broker_change_blocked_by_panel_onboarding).

Each panel subentry has focused day-two actions:

- **Rename** changes its display name only.
- **Change address** and **Repair SSH credentials** first fetch the candidate
  host identity and require it to match the stored fingerprint before a
  password is sent.
- **Components** points to the panel device's observable component switches and
  selectors. Set inactive Voice or Hue CA values in **Panel overrides**, then
  enable the component from its device control.
- An active Voice wake word changes through the **Wake word** selector. To
  change an active Voice host override or Hue CA certificate, disable that
  component, edit the override, and enable it again; the flow refuses a silent
  stored/running mismatch.
- **Rebind replaced/reflashed panel** is the only path that may accept a
  different SSH identity. It displays the old and new fingerprints for
  deliberate confirmation while preserving the panel's MQTT slug and
  management identity.

Normal repair, update, and address changes never auto-repin a new host key. An
unexpected mismatch fails closed; verify the address and network before using
the explicit rebind action.

<a id="rebind_blocked_by_panel_onboarding"></a>
### Rebind blocked by panel onboarding or recovery

- **Cause:** Another panel onboarding/recovery transaction owns the durable
  provisioning journal, or Home Assistant cannot prove that journal is
  readable and empty.
- **Check:** Finish or cancel the visible **Add Brilliant panel** flow, then
  inspect Brilliant MQTT repair issues and Home Assistant storage health.
- **Fix:** Let the existing transaction commit or complete its verified
  rollback. Repair unreadable Home Assistant storage before replacing a panel.
- **Retry:** Start **Rebind replaced/reflashed panel** again only after no panel
  onboarding/recovery issue remains. The blocked attempt changed no identity,
  credential, or panel setting.

## Entities

Each panel's device gains nine management entities (three diagnostic, six
control), plus two more scene entities when the fleet's **Enable Home
Assistant control** option is on (production default: off):

| Entity | What it is |
|---|---|
| `update.brilliant_<panel>_bridge` | Agent **update** entity. Installed version comes from the panel's retained bridge-meta (`agent_version`); latest from the bundled payload's `VERSION`. Installing pushes the bundled payload and restarts the agent. |
| `binary_sensor.brilliant_<panel>_bridge_health` | Bridge **health** (device class `problem`). `on` = needs attention (offline past grace with auto-repair off, a repair step failed, or a repair ran but the bridge stayed offline). Attributes: `reason`, `availability`. |
| `button.brilliant_<panel>_repair_bridge` | **Manual repair** — restores the unit/env and starts the agent (installs agent code first if missing), bypassing the auto-repair cooldown. |
| `button.brilliant_<panel>_reboot_panel` | **Reboot panel** (device class `restart`) — captures a secret-safe diagnostics summary over SSH, then reboots the panel. Typed service-journal categories and probe metrics are saved before volatile logs disappear; raw log text is never persisted. |
| `switch.brilliant_<panel>_voice_satellite` | **Voice satellite** — enable installs and starts the satellite; disable uninstalls it. |
| `select.brilliant_<panel>_wake_word` | **Wake word** — choose `okay_nabu` (default), `hey_jarvis`, or `hey_mycroft`; changing it restarts the satellite. |
| `switch.brilliant_<panel>_wi_fi_watchdog` | **Wi-Fi watchdog** — enable installs and starts the on-panel Wi-Fi watchdog daemon (auto-recovers lost Wi-Fi: reconnect → restart networking → reboot as a last resort, see [CONFIGURATION.md → Wi-Fi watchdog](CONFIGURATION.md#wi-fi-watchdog)); disable uninstalls it. |
| `switch.brilliant_<panel>_bus_watchdog` | **Bus watchdog** — enable installs and starts the on-panel bus-health watchdog daemon (reboots the panel if the Brilliant message bus stays wedged 30+ minutes, gated on the bridge being active and the network being up, see [CONFIGURATION.md → Bus-health watchdog](CONFIGURATION.md#bus-health-watchdog)); disable uninstalls it. |
| `switch.brilliant_<panel>_hue_ca_recovery` | **Hue CA recovery** — enable installs and starts the on-panel diyHue CA recovery oneshot+timer (re-appends your diyHue bridge's CA to the panel's pinned Hue trust bundle after every OTA, see [CONFIGURATION.md → Hue CA recovery](CONFIGURATION.md#hue-ca-recovery)); requires the diyHue CA certificate to be set through **Panel overrides** first — enabling with none configured fails closed. Disable uninstalls it. |
| `select.brilliant_<panel>_scene` | **Scene** — the panel's Brilliant scenes, populated from its accepted MQTT catalog. Changing it only updates the HA-local selection; it publishes no command. Created only when the fleet's **Enable Home Assistant control** option is on. |
| `button.brilliant_<panel>_run_selected_scene` | **Run selected scene** — runs the selected scene with blocking execution confirmation. Available only while the scene transport, catalog, and a selection exist. Created only when the fleet's **Enable Home Assistant control** option is on. |

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
| `repair_failed` | A repair could not complete or the bridge stayed offline. | `reason` (`unreachable` / `host_key_changed` / `repair_step_failed` / `still_offline`) |
| `needs_attention` | The panel needs a human (escalation). | `reason` |
| `agent_updated` | The agent was updated to a new version. | `version` |
| `panel_rebound` | An explicit replacement-panel identity rebind was durably committed. | `old_fingerprint`, `new_fingerprint` |
| `host_key_repinned` | Legacy panel entry only: its compatibility option auto-trusted a changed key. New fleet panels never auto-repin and require explicit rebind. | `new_host_key` |

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

## Fleet repair defaults

These defaults are owned once by the fleet and read live:

| Option | Default | What it does |
|---|---|---|
| **Auto-repair** (`auto_repair`) | `true` | On: outage past the grace period triggers automatic repair. Off: outage only notifies. |
| **Offline grace minutes** (`offline_grace_minutes`) | `10` | How long a panel may stay `offline` before repair/escalation kicks in. |
| **Repair cooldown minutes** (`repair_cooldown_minutes`) | `60` | Minimum gap between automatic repairs, preventing tight-loop repairs on a flapping panel. The manual repair button bypasses this. |

Seven **HA control and scene** settings (enable flag, entity label, room
overrides, domains, entity cap, default scene panel, scene actions) are also
stored once on the fleet rather than copied into panel subentries. Their
validation rules and defaults are canonical in the
[scene bridge guide → Configuration](brilliant-panel/home-assistant-integration.md#configuration).
Choose the enable flag during initial broker validation. Once a panel is
installed, that flag remains fail-closed until the guided agent rollout ships;
the other fleet control fields update live.

## Voice satellite

A Brilliant panel can act as a **Home Assistant ESPHome voice satellite**
(on-panel wake word + mic + speaker). STT, the conversation agent, and TTS all
run in your existing HA Assist pipeline — the panel is backend-agnostic.

**Quick start:**
1. After the base panel is healthy, open **Panel overrides** and set the initial
   wake word plus a Home Assistant host override only if needed.
2. Open **Components** to reach the panel's device controls, then turn on the
   **Voice satellite** switch. Use the **Wake word** selector for later active
   wake-word changes. To change the active host override, turn Voice off, edit
   the override, and turn it on again.
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
- Config is always **regenerated** from the stored fleet and panel subentry,
  never read back from the panel — so a repair also heals config drift.
- A firmware change on the bridge-meta topic fires `panel_updated` and
  re-stages the config copies under `/var`.

**Caveat — repair can't fix bus-lib drift.** Repair restores configuration and
the unit; it does not change the agent code. If a firmware OTA changed the
on-panel message-bus API such that the agent can no longer communicate, the
service will start but the bridge won't come back online. The recovery timer
transiently probes the service journal (raw output is discarded), then fires
`repair_failed` (`reason: still_offline`) and `needs_attention`, because the
agent itself needs a code fix — deploy a new release via the update entity or
`redeploy`.

<a id="retained-ledger"></a>
**Retained-ledger fault.** If the agent cannot read or durably persist
`/var/brilliant-mqtt/state/owned-topics.json`, it remains fail-closed and
publishes retained bridge metadata with `"degraded":"retained_ledger"` at QoS
1. Home Assistant creates one needs-attention repair issue and suppresses the
generic offline auto-repair loop so it cannot replace the useful storage
diagnosis. Check panel storage capacity, the state-directory permissions, and
the agent journal. Repair the storage or ledger deliberately; do not discard
the ledger casually because it is the ownership inventory for safe retained
topic cleanup. After restart, ordinary bridge metadata clears the issue.

## Security model

**Key points:**
- Each root password is stored only in that panel's HA config subentry (same
  exposure class as `secrets.yaml`). Protect the HA host accordingly. It is
  never shared across panels and is redacted from diagnostics.
- On first add, Home Assistant performs an unauthenticated SSH key exchange,
  derives the fingerprint, and pins that exact candidate before authenticating.
  This prevents a key swap between discovery and password use, but the first
  address is still trust-on-first-use; add panels from a trusted network and
  review the displayed fingerprint. Every later connection verifies the
  stored key **before** sending a password.
- Single auth attempt per connect (`client_keys=None`,
  `preferred_auth=("password",)`, keyboard-interactive disabled) so a wrong
  password can't burn through a lockout threshold.
- The integration only writes paths it owns: `/var/brilliant-mqtt/**`
  (including the Wi-Fi watchdog code, and `/var/brilliant-mqtt/bus_watchdog/**`
  for the bus watchdog, when enabled, content-addressed public MQTT CA files
  under `/var/brilliant-mqtt/tls/`, and the retained-topic ledger under
  `/var/brilliant-mqtt/state/`), `/var/brilliant-voice/**` (when voice is
  enabled), `/etc/brilliant-mqtt.env` (mode `0600`; its staged restore copy is
  also `0600`), `/etc/brilliant-voice.env`
  (mode `0600`), and the systemd units
  `/etc/systemd/system/brilliant-mqtt.service` /
  `brilliant-voice.service` / `brilliant-wifi-watchdog.service` /
  `brilliant-bus-watchdog.service`. The running bridge itself (not the
  integration) also stamps a liveness heartbeat to the tmpfs path
  `/run/brilliant-mqtt/bus-heartbeat`, cleared on every reboot.

**Host-key changes fail closed.** Normal repair, update, address, and credential
flows never auto-repin. If a panel was deliberately replaced or reflashed,
verify the new fingerprint and use its explicit **rebind** action; an
unexpected change should be investigated as a wrong address or possible
impostor.

See also [ARCHITECTURE.md](ARCHITECTURE.md) and
[reference/deployment.md](reference/deployment.md).
