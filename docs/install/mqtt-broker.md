# MQTT broker setup

The bridge publishes retained discovery/state and subscribes to command topics,
so it needs an MQTT broker that **both the panels and Home Assistant can reach**,
plus a dedicated user. If you already run a broker, just add the user and ACL
from [Option A](#option-a--standalone-broker-recommended) and skip the rest.

## Option A — standalone broker (recommended, especially for many panels)

Any Mosquitto (or compatible) broker works. Create a dedicated user and scope it
with an ACL:

```bash
mosquitto_passwd -b /etc/mosquitto/passwd brilliant '<password>'
```

```
# /etc/mosquitto/acl
user brilliant
topic readwrite brilliant/#
topic write homeassistant/#
```

Reference the password and ACL files from `mosquitto.conf` (`password_file` /
`acl_file`), then restart the broker.

> **Mosquitto ACL denials are silent.** If entities or commands never appear,
> check the ACL first — see [Troubleshooting](../TROUBLESHOOTING.md).

Connect Home Assistant to the same broker via the MQTT integration
(*Settings → Devices & Services → Add Integration → MQTT*).

## Option B — no broker yet: Home Assistant's Mosquitto add-on

If you run Home Assistant OS or Supervised and don't have a standalone broker,
use the official **Mosquitto broker** add-on:

1. Install and start the add-on (*Settings → Add-ons → Add-on Store →
   Mosquitto broker*). Home Assistant discovers and configures its own MQTT
   integration automatically.
2. Create a dedicated Home Assistant user for the bridge (*Settings → People →
   Users*, e.g. `brilliant`) — the add-on accepts Home Assistant users as MQTT
   credentials.
3. Point the bridge at Home Assistant's address: `MQTT_HOST=<home-assistant-ip>`,
   `MQTT_PORT=1883`, and that user's credentials (set in the panel's
   `/etc/brilliant-mqtt.env`).

> Home Assistant **Container/Core** installs don't support add-ons — run any
> Mosquitto instance ([Option A](#option-a--standalone-broker-recommended)) and
> connect both Home Assistant and the bridge to it.

## Why the dedicated user and ACL

The bridge only ever needs `brilliant/#` (read+write, its own state/command
namespace) and `homeassistant/#` (write, for MQTT Discovery configs). Scoping the
user this way keeps a panel agent from touching unrelated topics. The full topic
scheme and ACL rationale are in
[Configuration → Broker user and ACL](../CONFIGURATION.md#broker-user-and-acl).

---

Back to the [install overview](../../INSTALL.md).
