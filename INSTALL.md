# Installing brilliant-mqtt

> **Status:** the manual install path below has been exercised end-to-end on a
> live pilot panel — discovery, telemetry, commands, and LWT recovery all
> verified against a production Home Assistant.

The bridge is an **on-panel agent**: it runs on each Brilliant Control panel,
under the panel's own Python, because the message bus is only reachable via a
local unix socket.

Installing has three parts. **Pilot one panel first** and let it soak before
touching the rest.

1. **[Enable root SSH on the panel](docs/install/root-ssh.md)** — Brilliant's
   official opt-in; off by default, enabled per device.
2. **[Set up the MQTT broker](docs/install/mqtt-broker.md)** — a broker both the
   panels and Home Assistant can reach, with a dedicated bridge user. Skip if you
   already have one (just add the user + ACL).
3. **[Deploy the agent to a panel](#deploy-the-agent-to-a-panel)** — via the HACS
   companion integration (recommended) or a manual deploy.

## Requirements

- A Brilliant Control panel with **root SSH enabled** —
  [guide](docs/install/root-ssh.md).
- An **MQTT broker** reachable from the panels and Home Assistant, with a
  dedicated bridge user — [guide](docs/install/mqtt-broker.md).
- **Home Assistant** with the MQTT integration connected to that broker. Entities
  arrive via MQTT Discovery; no other HA-side configuration is needed.

## Deploy the agent to a panel

### Option A — HACS companion integration (recommended)

Install the **Home Assistant companion integration** and deploy/update/repair
every panel from the HA UI — no per-panel shell work, and a panel that loses its
agent after a firmware OTA is repaired automatically. Install it via HACS (custom
repository `joyfulhouse/brilliant-mqtt`, category Integration) or the release zip,
then add one panel per config entry (per-panel root password, mesh priority,
broker credentials).

See **[docs/ha-integration.md](docs/ha-integration.md)** for the full guide.

### Option B — manual deploy

The full manual steps (vendoring wheels, copying the app, env file, foreground
smoke-run, enabling the unit) are in [`deploy/README.md`](deploy/README.md).

For a fleet, automate the same layout with your configuration management of
choice (Ansible, etc.): render `/var/brilliant-mqtt/`, `/etc/brilliant-mqtt.env`
(credentials from your secret store — never from git), and the systemd unit, then
enable the service. If you publish the BLE mesh loads, give exactly one panel
`MESH_PRIORITY=1` and one or two standbys higher numbers — see
[docs/CONFIGURATION.md](docs/CONFIGURATION.md).

### Where it lands on the panel

| Path | Purpose |
|---|---|
| `/var/brilliant-mqtt/app/` | the `brilliant_mqtt` package |
| `/var/brilliant-mqtt/vendor/` | vendored pure-python deps (aiomqtt, paho-mqtt) |
| `/etc/brilliant-mqtt.env` | panel slug + MQTT credentials |
| `brilliant-mqtt.service` | systemd unit, resource-capped, `Restart=always` |

The app and vendored deps live under `/var` (the persistent partition) so they
survive firmware OTA updates; the systemd unit is re-installed after OTA if
`/etc` does not survive it (see
[docs/reference/deployment.md](docs/reference/deployment.md)). The interpreter is
the panel's bundled `/data/switch-embedded/env/bin/python3` (Python 3.10), the
only interpreter that can import the on-box message-bus client.

## Verify the installation

1. Entities for the panel's loads appear in Home Assistant automatically.
2. Toggling a load at the panel updates the HA entity (telemetry).
3. Toggling the HA entity drives the physical load (command).
4. Killing the agent marks the panel `offline` in HA (LWT); systemd restarts it
   and it recovers.
5. Restarting Home Assistant brings the entities straight back (retained
   discovery + state).

If something is off, start with [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md).

## Updating

Re-copy `src/brilliant_mqtt` to `/var/brilliant-mqtt/app/` and restart the service
(or re-run your configuration management / the HACS integration's update). After
any panel **firmware OTA**, re-validate the bus API before trusting the fleet —
see [docs/reference/deployment.md](docs/reference/deployment.md).

## Uninstall / rollback

```bash
systemctl disable --now brilliant-mqtt
```

If the panels are paired with HomeKit, that path is untouched and remains a
fallback. Discovery topics are retained — to fully remove an entity from HA,
publish an empty retained payload to its
`homeassistant/<component>/<unique_id>/config` topic.
