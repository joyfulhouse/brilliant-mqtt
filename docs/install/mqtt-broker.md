# MQTT broker setup

The bridge publishes retained discovery/state and subscribes to command topics,
so it needs an MQTT broker that **both the panels and Home Assistant can reach**,
plus a dedicated user.

If you already run a broker, just add the `brilliant` user and ACL —
see [Configuration → Broker user and ACL](../CONFIGURATION.md#broker-user-and-acl)
— then return to [INSTALL.md](../../INSTALL.md).

> **Mosquitto ACL denials are silent.** If entities or commands never appear,
> check the ACL first — see [Troubleshooting](../TROUBLESHOOTING.md).

## Option A — standalone broker (recommended)

Any Mosquitto (or compatible) broker works. Create a dedicated user, then add
the ACL and restart the broker:

```bash
mosquitto_passwd -b /etc/mosquitto/passwd brilliant '<password>'
```

Reference the password and ACL files from `mosquitto.conf` (`password_file` /
`acl_file`). For the full ACL snippet and rationale, see
[Configuration → Broker user and ACL](../CONFIGURATION.md#broker-user-and-acl).

Connect Home Assistant to the same broker via the MQTT integration
(*Settings → Devices & Services → Add Integration → MQTT*).

### Verify

```bash
mosquitto_pub -h <broker> -u brilliant -P '<password>' -t test/x -m hi
```

If it exits without error, the user and connection are working.

## Option B — Home Assistant's Mosquitto add-on

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

---

Back to the [install overview](../../INSTALL.md).
