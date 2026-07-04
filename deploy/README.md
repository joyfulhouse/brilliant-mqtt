# deploy/

Reference deployment assets for the on-panel bridge. These files are the
canonical reference for any automation you build around the install (Ansible
or similar). See `../docs/reference/deployment.md` for the full operational
guide and `../INSTALL.md` for prerequisites including MQTT broker setup.

## Contents

- `brilliant-mqtt.service` — the systemd unit for the bridge (runs under the
  panel's Python 3.10 with `/var`-based app + vendored deps; resource-capped).
- `brilliant-bus-watchdog.service` — optional bus-health watchdog: reboots the
  panel if the Brilliant message bus wedges (heartbeat stale 30 min+, gated on
  the bridge being active and the gateway reachable). See
  [../docs/CONFIGURATION.md](../docs/CONFIGURATION.md#bus-health-watchdog).
- `brilliant-wifi-watchdog.service` — optional Wi-Fi watchdog: recovers a panel
  that drops off Wi-Fi (connman re-enable → restart → GPIO reset/reboot). See
  [../docs/CONFIGURATION.md](../docs/CONFIGURATION.md#wi-fi-watchdog).
- `brilliant-voice.service` — optional on-panel voice satellite. See
  [../docs/voice.md](../docs/voice.md).

The HA integration installs and enables these per panel automatically. To wire
one up **manually**: drop the unit in `/etc/systemd/system/`, then
`systemctl enable --now <unit>`. The watchdog units read their settings from the
same `/etc/brilliant-mqtt.env`.

## Manual deploy (pilot one panel first)

> Keep SSH and MQTT credentials in a local **gitignored** file (this repo's
> convention: `../CREDENTIALS.local.md`). Pilot on **one** panel and let it
> soak; treat the rest as hands-off until you trust it.

1. **Vendor MQTT deps** (the panel ships no MQTT client):
   ```bash
   # (uv has no `pip download`; run pip's downloader under uv)
   uv run --with pip python -m pip download aiomqtt paho-mqtt \
     --python-version 3.10 --only-binary=:all: -d /tmp/wheels
   # unzip each wheel and copy the package dirs (aiomqtt/, paho/) into
   # /var/brilliant-mqtt/vendor/ on the panel (tar-over-ssh works well)
   ```
2. **Copy the app**: `scp -r src/brilliant_mqtt root@<panel-ip>:/var/brilliant-mqtt/app/`
3. **Write `/etc/brilliant-mqtt.env`** on the panel (mode 0600):
   ```
   BRILLIANT_PANEL=office
   MQTT_HOST=<broker-ip>
   MQTT_PORT=1883
   MQTT_USERNAME=brilliant
   MQTT_PASSWORD=<your broker password>
   LOG_LEVEL=INFO
   ```
   To publish the whole-home BLE mesh loads from this panel, also set
   `MESH_PRIORITY=1` (see `../docs/CONFIGURATION.md`).
4. **Smoke-run in the foreground** (watch logs):
   ```bash
   PYTHONPATH=/var/brilliant-mqtt/app:/var/brilliant-mqtt/vendor \
     /data/switch-embedded/env/bin/python3 -m brilliant_mqtt
   ```
5. Verify in Home Assistant (entities appear, telemetry + command + LWT). Then
   install the unit and `systemctl enable --now brilliant-mqtt`.

## OTA re-validation

After any panel firmware update, re-run a read-only bus smoke test on one
panel (connect, `get_all()`, subscribe) to confirm the closed-source bus API
is unchanged before updating the rest of your panels — see
`../docs/reference/deployment.md`.
