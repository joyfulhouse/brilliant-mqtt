# Installing brilliant-mqtt

The bridge is an **on-panel agent**: it runs on each Brilliant Control panel
under the panel's own Python 3.10, because the message bus is only reachable
via a local unix socket.

> **Pilot one panel first.** Let it soak before rolling out to the rest of
> your fleet.

## Prerequisites

Before you start, confirm you have each of these:

| Requirement | Ready? | Guide |
|---|---|---|
| A Brilliant Control panel with **root SSH enabled** | Brilliant's official opt-in; off by default | [docs/install/root-ssh.md](docs/install/root-ssh.md) |
| An **MQTT broker** reachable from the panels and Home Assistant | Standalone Mosquitto (recommended) or the HA Mosquitto add-on | [docs/install/mqtt-broker.md](docs/install/mqtt-broker.md) |
| **Home Assistant** connected to that broker | *Settings → Devices & Services → Add Integration → MQTT* | — |

If you already have a broker, skip to [broker user and ACL](docs/CONFIGURATION.md#broker-user-and-acl) to add the dedicated `brilliant` user, then go straight to [Deploy](#step-3--deploy-the-agent-to-a-panel).

## Step 1 — Enable root SSH on the panel

See **[docs/install/root-ssh.md](docs/install/root-ssh.md)** for the full
steps. This is Brilliant's official opt-in feature — no jailbreak needed.

## Step 2 — Set up the MQTT broker

See **[docs/install/mqtt-broker.md](docs/install/mqtt-broker.md)** for
standalone Mosquitto or the Home Assistant Mosquitto add-on, plus the
dedicated bridge user and ACL.

## Step 3 — Deploy the agent to a panel

Choose one path. **HACS is recommended for most users.**

| Option | Choose this if… | Guide |
|---|---|---|
| **HACS companion integration** *(recommended)* | You run Home Assistant and want guided onboarding, auto-repair after firmware OTA, one-click updates, and the optional voice satellite | [docs/ha-integration.md](docs/ha-integration.md) |
| **Manual deploy** | You don't run Home Assistant, or prefer shell / Ansible | [deploy/README.md](deploy/README.md) |

### HACS (recommended)

Install the integration as a custom HACS repository
(`joyfulhouse/brilliant-mqtt`, category Integration) or via the release zip,
then add one config entry per panel (root password, mesh priority, broker
credentials). The integration deploys the agent over SSH, keeps it updated,
and repairs it automatically after panel firmware OTAs.

[![Add via HACS](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=joyfulhouse&repository=brilliant-mqtt&category=integration)

### Manual deploy

The full manual steps — vendoring wheels, copying the app, writing the env
file, foreground smoke-run, enabling the unit — are in
[`deploy/README.md`](deploy/README.md).

For a fleet, automate the same layout with configuration management (Ansible,
etc.): render `/var/brilliant-mqtt/`, `/etc/brilliant-mqtt.env` (credentials
from your secret store — **never from git**), and the systemd unit. If you
publish BLE mesh loads, give exactly one panel `MESH_PRIORITY=1` and one or
two standbys higher numbers — see [docs/CONFIGURATION.md](docs/CONFIGURATION.md).

### Where it lands on the panel

| Path | Purpose |
|---|---|
| `/var/brilliant-mqtt/app/` | the `brilliant_mqtt` package |
| `/var/brilliant-mqtt/vendor/` | vendored pure-Python deps (aiomqtt, paho-mqtt) |
| `/etc/brilliant-mqtt.env` | panel slug + MQTT credentials |
| `brilliant-mqtt.service` | systemd unit, resource-capped, `Restart=always` |

The app and vendored deps live under `/var` (the persistent partition) so they
survive firmware OTA updates. The interpreter is the panel's bundled
`/data/switch-embedded/env/bin/python3` (Python 3.10) — the only interpreter
that can import the on-box message-bus client library.

## Verify the installation

Check `systemctl status brilliant-mqtt` on the panel — it should be `active (running)`.

Then confirm the integration is working end-to-end:

1. `systemctl status brilliant-mqtt` active?
   - Yes → check that entities appear in HA.
   - No → see [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md).
2. Entities appear in HA?
   - Yes → toggle a load at the panel and confirm HA updates.
   - No → check the broker ACL (see [CONFIGURATION.md#broker-user-and-acl](docs/CONFIGURATION.md#broker-user-and-acl)), then see [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md).

Specific checks:
1. Entities for the panel's loads appear in Home Assistant automatically.
2. Toggling a load at the panel updates the HA entity (telemetry).
3. Toggling the HA entity drives the physical load (command).
4. Killing the agent marks the panel `offline` in HA (LWT); systemd restarts it and it recovers.
5. Restarting Home Assistant brings entities straight back (retained discovery + state).

For anything else, see [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md).

## Add voice (optional)

Turn a panel into a local wake-word voice satellite. Enable the **Voice
satellite** toggle during HACS onboarding, or from the panel's device page
after install. See **[docs/voice.md](docs/voice.md)**.

## Updating

**Via HACS integration:** use the integration's update button in the HA UI.

**Manually:** re-copy `src/brilliant_mqtt` to `/var/brilliant-mqtt/app/` and
restart the service. After any panel **firmware OTA**, re-validate the bus API
before trusting the fleet — see
[docs/reference/deployment.md](docs/reference/deployment.md).

## Uninstall / rollback

```bash
systemctl disable --now brilliant-mqtt
```

If the panels are paired with HomeKit, that path is untouched and remains a
fallback. Discovery topics are retained — to fully remove an entity from HA,
publish an empty retained payload to its
`homeassistant/<component>/<unique_id>/config` topic.
