# Installing brilliant-mqtt

> **Status:** the manual install path below has been exercised end-to-end on
> a live pilot panel — discovery, telemetry, commands, and LWT recovery all
> verified against a production Home Assistant.

The bridge is an **on-panel agent**: it must run on each Brilliant Control
panel, under the panel's own Python, because the message bus is only reachable
via a local unix socket.

## Requirements

- A Brilliant Control panel with **root SSH enabled** — see
  [Enable root SSH on the panel](#enable-root-ssh-on-the-panel) below; it is
  off by default and must be enabled per device.
- An **MQTT broker** reachable from both the panels and Home Assistant, with a
  dedicated bridge user — see [Set up the MQTT broker](#set-up-the-mqtt-broker)
  below if you don't have one yet.
- **Home Assistant** with the MQTT integration connected to that broker
  (entities arrive via MQTT Discovery; no other HA-side configuration is
  needed).

## Enable root SSH on the panel

Brilliant ships an official, supported way to get root access — no jailbreak
needed, but it is opt-in per device:

1. On the panel (or in the Brilliant app for that panel), open **Settings →
   Advanced Settings → Root SSH Login**.
2. Enabling it requires **verifying your identity by email**; complete the
   verification and the panel activates SSH with **per-device root
   credentials** (password authentication only — there is no authorized-keys
   mechanism).
3. Record the credentials somewhere safe and **out of git** (this repo's
   convention is a gitignored `CREDENTIALS.local.md`). The password can contain
   shell-hostile characters — connect with `sshpass` and password-only auth:

   ```bash
   SSHPASS='<root password>' sshpass -e ssh \
     -o PreferredAuthentications=password -o PubkeyAuthentication=no \
     -o NumberOfPasswordPrompts=1 root@<panel-ip>
   ```

Read Brilliant's caveats in the official
[RootSSH support article](https://support.brilliant.tech/hc/en-us/articles/23152790775195-RootSSH)
before enabling — in short: it is intended for knowledgeable command-line
users; changes you make can break updates or functionality; once enabled, the
device is **permanently flagged as possibly manipulated**; don't enable it on
a device you plan to transfer; and Brilliant will never ask you to enable it
or to share the credentials.

These are production in-wall touchscreens: treat SSH as read-only except for
the deliberate install steps below, pilot ONE panel first, and note that the
bridge itself only calls the same message-bus APIs Brilliant's own HomeKit
peripheral uses, under a resource-capped systemd unit.

## Set up the MQTT broker

The bridge publishes retained discovery/state and subscribes to command topics,
so it needs a broker that both the panels and Home Assistant can reach, plus a
dedicated user.

### Option A — standalone broker (recommended, especially for many panels)

Any Mosquitto (or compatible) broker works. Create a dedicated user and scope
it with an ACL:

```bash
mosquitto_passwd -b /etc/mosquitto/passwd brilliant '<password>'
```

```
# /etc/mosquitto/acl
user brilliant
topic readwrite brilliant/#
topic write homeassistant/#
```

Reference the password and ACL files from `mosquitto.conf`
(`password_file` / `acl_file`), then restart the broker.

> **Mosquitto ACL denials are silent.** If entities or commands never appear,
> check the ACL first — see
> [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md).

Connect Home Assistant to the same broker via the MQTT integration
(*Settings → Devices & Services → Add Integration → MQTT*).

### Option B — no broker yet: Home Assistant's Mosquitto add-on

If you run Home Assistant OS or Supervised and don't have a standalone broker,
use the official **Mosquitto broker** add-on:

1. Install and start the add-on (*Settings → Add-ons → Add-on Store →
   Mosquitto broker*). Home Assistant discovers and configures its own MQTT
   integration automatically.
2. Create a dedicated Home Assistant user for the bridge (*Settings → People →
   Users*, e.g. `brilliant`) — the add-on accepts Home Assistant users as MQTT
   credentials.
3. Point the bridge at Home Assistant's address:
   `MQTT_HOST=<home-assistant-ip>`, `MQTT_PORT=1883`, and that user's
   credentials.

> Home Assistant **Container/Core** installs don't support add-ons — run any
> Mosquitto instance (Option A) and connect both Home Assistant and the bridge
> to it.

## Where it lands on the panel

| Path | Purpose |
|---|---|
| `/var/brilliant-mqtt/app/` | the `brilliant_mqtt` package |
| `/var/brilliant-mqtt/vendor/` | vendored pure-python deps (aiomqtt, paho-mqtt) |
| `/etc/brilliant-mqtt.env` | panel slug + MQTT credentials |
| `brilliant-mqtt.service` | systemd unit, resource-capped, `Restart=always` |

The app and vendored deps live under `/var` (the persistent partition) so they
survive firmware OTA updates; configuration is rendered to
`/etc/brilliant-mqtt.env`, and the systemd unit is re-installed after OTA if
`/etc` does not survive it (see
[docs/reference/deployment.md](docs/reference/deployment.md)). The interpreter
is the panel's bundled `/data/switch-embedded/env/bin/python3` (Python 3.10),
which is the only interpreter that can import the on-box message-bus client.

## Install on a panel

The full manual steps (vendoring wheels, copying the app, env file, foreground
smoke-run, enabling the unit) are in [`deploy/README.md`](deploy/README.md).
Pilot **one** panel first and let it soak before touching the rest.

For a fleet, automate the same layout with your configuration management of
choice (Ansible, etc.): render `/var/brilliant-mqtt/`, `/etc/brilliant-mqtt.env`
(credentials from your secret store — never from git), and the systemd unit,
then enable the service. If you publish the BLE mesh loads, give exactly one
panel `MESH_PRIORITY=1` and one or two standbys higher numbers — see
[docs/CONFIGURATION.md](docs/CONFIGURATION.md).

## Verify the installation

1. Entities for the panel's loads appear in Home Assistant automatically.
2. Toggling a load at the panel updates the HA entity (telemetry).
3. Toggling the HA entity drives the physical load (command).
4. Killing the agent marks the panel `offline` in HA (LWT); systemd restarts it
   and it recovers.
5. Restarting Home Assistant brings the entities straight back (retained
   discovery + state).

## Updating

Re-copy `src/brilliant_mqtt` to `/var/brilliant-mqtt/app/` and restart the
service (or re-run your configuration management). After any panel **firmware
OTA**, re-validate the bus API before trusting the fleet — see
[docs/reference/deployment.md](docs/reference/deployment.md).

## Uninstall / rollback

```bash
systemctl disable --now brilliant-mqtt
```

If the panels are paired with HomeKit, that path is untouched and remains a
fallback. Discovery topics are retained — to fully remove an entity from HA,
publish an empty retained payload to its
`homeassistant/<component>/<unique_id>/config` topic.
