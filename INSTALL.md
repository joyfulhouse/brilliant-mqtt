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
| An **MQTT broker** reachable from the panels and Home Assistant | Official Home Assistant Mosquitto app/add-on (`core_mosquitto`) recommended; an existing local, remote, or hosted broker is equally supported | [docs/install/mqtt-broker.md](docs/install/mqtt-broker.md) |
| **Home Assistant** connected to that broker | *Settings → Devices & Services → Add Integration → MQTT* | — |

If you already have a broker, keep it. Follow the
[existing-broker prerequisites](docs/install/mqtt-broker.md#existing-broker)
to connect Home Assistant and configure the two broker principals. After
confirming root SSH is enabled, go straight to
[Deploy](#step-3--deploy-the-agent-to-a-panel).

## Step 1 — Enable root SSH on the panel

See **[docs/install/root-ssh.md](docs/install/root-ssh.md)** for the full
steps. This is Brilliant's official opt-in feature — no jailbreak needed.

## Step 2 — Set up the MQTT broker

See **[docs/install/mqtt-broker.md](docs/install/mqtt-broker.md)**. The
recommended shortcut is the official Home Assistant Mosquitto Broker
app/add-on (`core_mosquitto`), installed and started before onboarding. It is
not required: an existing local, remote, or hosted broker is a first-class
path.

Whichever path you choose:

- configure Home Assistant's MQTT integration first;
- create a dedicated, non-owner Brilliant username/password;
- use a LAN hostname/IP and TCP port every panel can resolve and reach; and
- keep Home Assistant's MQTT Discovery prefix set to `homeassistant`.

In HACS onboarding, both broker choices run the same behavioral validation
before setup continues: Home Assistant MQTT readiness, Brilliant credential
authentication, both message directions, Discovery writes, retained delivery,
and cleanup. Adding a panel then preflights the staged agent against that same
broker before activation. A failure identifies the failed stage and points to
the matching
[MQTT validation error](docs/install/mqtt-broker.md#validation-errors);
correct the prerequisite and retry. Manual deployments must verify the same
broker behaviors before enabling the service.

The integration validates broker behavior but never installs a broker, reads
Home Assistant's hidden MQTT credential, or creates/modifies broker users,
ACLs, or configuration. Plaintext TCP, strict system/public-CA TLS, and strict
custom-CA TLS are supported; TLS never falls back to plaintext. See
[transport security](docs/install/mqtt-broker.md#transport-security).

## Step 3 — Deploy the agent to a panel

Choose one path. **HACS is recommended for most users.**

| Option | Choose this if… | Guide |
|---|---|---|
| **HACS companion integration** *(recommended)* | You run Home Assistant and want guided onboarding, auto-repair after firmware OTA, one-click updates, and the optional voice satellite | [docs/ha-integration.md](docs/ha-integration.md) |
| **Manual deploy** | You don't run Home Assistant, or prefer shell / Ansible | [deploy/README.md](deploy/README.md) |

### HACS (recommended)

Install the integration as a custom HACS repository
(`joyfulhouse/brilliant-mqtt`, category Integration) or via the release zip,
then add Brilliant MQTT once. One fleet entry owns the broker profile and
shared settings; each panel is a Home Assistant panel subentry with its own SSH
identity, address, and root password. The integration deploys the agent over
SSH, keeps it updated, and repairs it automatically after panel firmware OTAs.

> **Use HACS or a release zip for a supported installation.** The on-panel
> agent payload (`agent_payload/`) is committed and shipped with the
> integration. CI rebuilds it and verifies both byte parity and the exact
> tracked/untracked file set before publishing. A raw `git clone` therefore
> contains the payload, but it is development source rather than a qualified
> user-install artifact; contributors must run the payload build and parity
> gates before loading it. The broker connection also requires a **username
> and password** — anonymous brokers are not supported (see
> [broker setup](docs/CONFIGURATION.md#broker-user-and-acl)).

[![Add via HACS](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=joyfulhouse&repository=brilliant-mqtt&category=integration)

### Fleet-first onboarding

1. Add **Brilliant MQTT** and choose either **Home Assistant Mosquitto
   add-on (Recommended)** or **Existing MQTT broker**. The second path is
   equally supported and does not require `core_mosquitto`.
2. Enter the dedicated Brilliant broker credential and an address reachable
   from every panel. Optionally enable **Home Assistant control and scenes** on
   this same form; it defaults off and must be selected before the first panel
   if you want the scene bridge immediately. Both broker choices run the same
   behavioral validation. Broker failure creates no Brilliant fleet.
3. After validation, Home Assistant creates and verifies a durable empty fleet,
   then automatically opens the first **Add Brilliant panel** flow. If panel
   setup fails or is cancelled, the valid empty fleet remains so you can retry
   without repeating broker setup.
4. Enter only the panel address and root password, review the detected panel,
   and give it a friendly name. Home Assistant assigns a stable MQTT slug and
   the next unused positive mesh priority automatically.
5. Provisioning snapshots the current panel, stages and preflights the agent,
   activates it, and waits for fresh health. A failed candidate is rolled back
   instead of being left partly active.

For another panel, open the existing Brilliant MQTT fleet and use its native
**Add panel** action, shown as **Add Brilliant panel**. Broker settings, raw
JSON, optional features, and mesh priority are not repeated during panel
onboarding. Configure inactive feature values through **Panel overrides**, then
use the panel device's component switches and selectors. Active Voice/Hue
configuration is never changed silently: use the Wake word selector, or disable
the affected component before editing its other override and enable it again.

Address changes and SSH credential repairs remain bound to the saved SSH
identity. A replacement or reflashed panel with a different identity requires
the separate, explicit **rebind** action. Broker-profile changes on a populated
fleet are intentionally deferred to a guided multi-panel operation so setup
cannot create a mixed-broker fleet. An active Add panel or recovery transaction
also blocks an otherwise eligible empty-fleet broker edit. See
[Home Assistant day-two configuration](docs/ha-integration.md#fleet-and-panel-configuration).

The staged checks are software safety gates, not a claim that every panel
firmware/network combination has completed hardware qualification. Keep the
one-panel pilot and soak period before a fleet rollout.

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
| `/etc/brilliant-mqtt.env` | panel slug + MQTT credentials/TLS path (mode `0600`) |
| `/var/brilliant-mqtt/tls/` | immutable, content-addressed public MQTT CA files when custom-CA TLS is used |
| `/var/brilliant-mqtt/state/owned-topics.json` | retained-topic ownership ledger used for safe cleanup |
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

Turn a panel into a local wake-word voice satellite after its base agent is
healthy. Use the panel's focused optional-feature controls or its **Voice
satellite** switch. See **[docs/voice.md](docs/voice.md)**.

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
