# Home Assistant companion integration

A small Home Assistant **custom integration** that manages the lifecycle of the
on-panel `brilliant-mqtt` agent across your fleet — deploy, OTA-survival repair,
version updates, and removal — all from the HA UI. It lives in this repo under
[`ha/custom_components/brilliant_mqtt/`](../ha/custom_components/brilliant_mqtt).

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
   MQTT** (one add per panel — see the add-panel form below).

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

## The add-panel form

Adding the integration opens a per-panel form:

| Field | Notes |
|---|---|
| **Host** | Panel hostname or IP for SSH. |
| **Root password** | The panel's **per-controller** root password. Deliberately **not** pre-filled from a previous panel — reusing one by accident is the costliest mistake. |
| **Panel** | The panel slug — doubles as the MQTT topic segment and the entry's unique id. Must match `^[a-z0-9_-]+$`; lower-cased on submit. `mesh` is **reserved** (the whole-home pseudo-panel has no host behind it) and is rejected. |
| **Mesh priority** | `MESH_PRIORITY` for BLE-mesh leader election (0 = never lead; 1 = primary; 2/3 = standbys). Written into the panel's env file. |
| **MQTT host / port / username / password** | Broker credentials the agent uses; written into the panel's env file over SSH. The broker fields **are** pre-filled from the previous panel (fleet-shared). |

On submit the integration makes **one** SSH connection to validate the host and
**pin its host key** (trust-on-first-use). The root password is never offered to
an unpinned/impostor host on later connects.

To rotate a panel's host or root password later, use **Reconfigure** on the
entry (the slug is immutable); this re-validates over SSH. If the **host is
unchanged**, it verifies the new password against the **stored** host key (key
checked before auth — a mismatch is rejected with `host_key_changed`, never a
silent re-pin), so rotating a password can't be used to accept a swapped key. If
the **host changes**, it does a fresh trust-on-first-use connect and **re-pins**
to the new host's key, so pointing the entry at different hardware is safe.
Behavior knobs are under **Configure** (Options).

## Entities

Each panel's device gains three management entities (all diagnostic):

| Entity | What it is |
|---|---|
| `update.brilliant_<panel>_bridge` | Agent **update** entity. Installed version comes from the panel's retained bridge-meta (`agent_version`); latest comes from the bundled payload's `VERSION`. Installing pushes the bundled payload and restarts the agent. Also the **first-deploy** path to a bare panel. |
| `binary_sensor.brilliant_<panel>_bridge_health` | Bridge **health** (device class `problem`). `on` = needs attention (offline past grace with auto-repair off, a repair step failed, or a repair ran but the bridge stayed offline). Attributes: `reason`, `availability`. |
| `button.brilliant_<panel>_repair_bridge` | **Manual repair** — restores the unit/env and starts the agent, bypassing the auto-repair cooldown. |

Entity ids are derived from the panel's HA device name (`Brilliant <panel>`), so
in practice they read `update.brilliant_<panel>_bridge`,
`binary_sensor.brilliant_<panel>_bridge_health`, and
`button.brilliant_<panel>_repair_bridge`.

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

## Events

The integration fires `brilliant_mqtt_event` on the HA event bus. Every event
carries `panel`, `entry_id`, and a `type`; the table lists the per-type extras:

| `type` | Meaning | Extra data |
|---|---|---|
| `panel_updated` | Panel **firmware** changed (seen on the bridge-meta topic). | `old_firmware`, `new_firmware` |
| `repair_started` | A repair began. | `trigger` (`auto` / `button` / `service`) |
| `repair_succeeded` | The bridge came back online after a repair. | — |
| `repair_failed` | A repair could not complete or the bridge stayed offline. | `reason` (`unreachable` / `repair_step_failed` / `still_offline`), plus `error` or `journal` |
| `needs_attention` | The panel needs a human (escalation). | `reason` |
| `agent_updated` | The agent was updated to a new version. | `version` |

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

Per-panel behavior knobs (under **Configure**); the manager reads them live, so
no reload is needed:

| Option | Default | What it does |
|---|---|---|
| **Auto-repair** (`auto_repair`) | `true` | When on, an outage past the grace period triggers an automatic repair. When off, an outage only notifies. |
| **Offline grace minutes** (`offline_grace_minutes`) | `10` | How long a panel may stay `offline` before repair/escalation kicks in. |
| **Repair cooldown minutes** (`repair_cooldown_minutes`) | `60` | Minimum gap between automatic repairs, so a flapping panel is not repaired in a tight loop. (The manual repair button bypasses this.) |

## The OTA repair state machine

The availability LWT and the retained bridge-meta drive everything for one
panel. Going `offline` arms a **grace timer**; if the panel is still offline when
it expires and auto-repair is on (and the repair cooldown has elapsed), the
integration SSHes in, **rewrites the unit + env from known-good sources** (always
regenerated, never read back — so a repair also heals config drift), re-stages
the OTA-proof copies under `/var`, and `enable --now`s the service; a **recovery
timer** then waits for the availability LWT to flip back to `online`
(→ `repair_succeeded`) or escalates (→ `repair_failed` + `needs_attention`). A
firmware change on the bridge-meta topic fires `panel_updated` and re-stages the
config copies.

**Caveat — repair can't fix bus-lib drift.** Repair restores *configuration and
the unit*; it does not change the agent's code. If a firmware OTA changed the
on-panel message-bus API such that the agent can no longer talk to the bus, the
service will start but the bridge will not come back online — the recovery timer
fires `repair_failed` (`reason: still_offline`, with a captured journal) and
`needs_attention`, because the agent itself needs a code fix (a new release,
deployed via the update entity / `redeploy`), not a repair.

## Security model

- **Per-panel root password** is stored in the panel's HA **config entry**
  (HA's config-entry store) — the **same exposure class as `secrets.yaml`**: it
  is readable by anyone who can read HA's config/storage, so protect the HA host
  accordingly. It is deliberately not shared between panels and is redacted from
  diagnostics.
- **TOFU host-key pinning.** The first successful connect captures and pins the
  panel's SSH host key; every later connect verifies it **before** authenticating,
  so the root password is never offered to an impostor host.
  - **OTA host-key rotation caveat.** Because `async_repair` (and Reconfigure on the
    same host) connects using that pin, a firmware OTA that regenerates the panel's
    SSH host key would make repair connects fail host-key verification — surfacing as
    `repair_failed: unreachable` exactly when a repair is needed. Operators should
    verify on the pilot whether the OSTree OTA rotates `/etc/ssh` host keys; if it
    does, re-pin via remove + re-add (a future enhancement may add re-pin-on-mismatch).
- **Single auth attempt.** SSH is password-only with `client_keys=None`,
  `preferred_auth=("password",)`, and keyboard-interactive disabled — exactly one
  credentialed attempt per connect, so a wrong password can't burn through a
  lockout threshold.
- The integration only ever writes the paths it owns on the panel:
  `/var/brilliant-mqtt/**`, `/etc/brilliant-mqtt.env` (mode `0600`), and
  `/etc/systemd/system/brilliant-mqtt.service`.

> **Adopting a hand-deployed panel:** the integration lowercases the panel slug
> and writes a matching lowercase `BRILLIANT_PANEL` into the env file, so any
> panel it deploys is self-consistent. If you *manually* deployed the agent first
> (per [INSTALL.md](../INSTALL.md)), make sure that `BRILLIANT_PANEL` is already
> lowercase `^[a-z0-9_-]+$` — the agent publishes its MQTT topics verbatim from
> that value, so a capitalized slug won't match the integration's lowercase
> subscriptions until the first repair/redeploy rewrites the env and converges
> them.

See also [ARCHITECTURE.md](ARCHITECTURE.md) and
[reference/deployment.md](reference/deployment.md).
