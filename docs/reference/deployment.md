# Deployment Reference

How the bridge runs on a panel, survives OTA, and gets its credentials. The
authoritative steps are in the plan (Milestones 7–9); this is the operational
quick reference.

## Where it runs

- **Interpreter:** `/data/switch-embedded/env/bin/python3` (the panel's bundled
  Python 3.10.9 — the only interpreter that can `import lib.message_bus_api`).
  This path is stable across OTA.
- **Our code + vendored deps:** `/var/brilliant-mqtt/` (the persistent rw
  partition — survives OTA, unlike `/data`).
  - `/var/brilliant-mqtt/app/brilliant_mqtt/…` — our package
  - `/var/brilliant-mqtt/vendor/…` — vendored pure-python deps (aiomqtt, paho-mqtt)
  - `/var/brilliant-mqtt/tls/mqtt-ca-<hash>.pem` — immutable public custom CAs
    when strict custom-CA MQTT TLS is used
  - `/var/brilliant-mqtt/state/owned-topics.json` — retained-topic ownership
    ledger
- **Config:** `/etc/brilliant-mqtt.env` (panel slug + MQTT credentials/TLS
  path, mode `0600`), with its OTA restore copy under
  `/var/brilliant-mqtt/system/` also mode `0600`.
- **Service:** systemd `brilliant-mqtt.service`, `Restart=always`, resource-capped
  (`MemoryMax`, `CPUQuota`, `Nice`) so a bug can't degrade the panel UI.

## Vendoring the MQTT client

The panel has no pip into `/data` (OSTree-immutable), so MQTT deps are vendored
to `/var`. Confirm in the PoC whether `aiomqtt`/`paho` already exist in the panel
site-packages; if not:

```bash
# On the dev machine: download pure-python wheels for py3.10 and unpack into vendor/
uv pip download aiomqtt paho-mqtt --python-version 3.10 --only-binary=:all: -d /tmp/wheels
# unzip each wheel's top-level package dir into /var/brilliant-mqtt/vendor on the panel
# (aiomqtt + paho/ are both pure-python; no compiled extensions)
```

`PYTHONPATH=/var/brilliant-mqtt/app:/var/brilliant-mqtt/vendor` is set in the
systemd unit so the venv python finds both our app and the vendored deps (in
addition to the panel's own site-packages it already exposes).

## MQTT credentials (no secrets in git)

- The recommended shortcut is Home Assistant's official Mosquitto Broker
  app/add-on (`core_mosquitto`), but it is never required or managed by this
  integration. An existing local, remote, or hosted Mosquitto-compatible
  broker is equally supported.
- Configure Home Assistant's MQTT integration first, then use a broker
  hostname/IP and TCP port that every panel can resolve and reach. Do not
  assume the app's internal hostname is reachable from the panel VLAN.
- Use a dedicated, non-owner `brilliant` principal. Keep its password in your
  secret store and inject it into `/etc/brilliant-mqtt.env` at deploy time.
  Never extract or reuse Home Assistant's hidden generated MQTT credential.
- **ACL:** the panel principal needs `brilliant/#` (read/write) and
  `homeassistant/#` (write). The Home Assistant principal also needs
  `brilliant/#` (read/write), `homeassistant/#` (read), narrow validation
  cleanup write access to `homeassistant/brilliant_mqtt_setup/+/probe` (or
  broader existing write access), and its normal birth/will/status permissions.
  Mosquitto ACL **deny is silent**—get either direction wrong and state or
  commands can vanish with no client error.
- Keep Home Assistant's MQTT Discovery prefix fixed at `homeassistant`.
- After restarting the broker, check that your OTHER MQTT clients reconnected —
  some (e.g. certain container deployments) need a restart after a broker roll.

The [MQTT broker prerequisite guide](../install/mqtt-broker.md) has the exact
principal table, official source links, and stable remediation anchors for the
packaged validator. The current one-panel flow does not call it. The
forthcoming Plan 2 fleet flow will prove authentication, both message
directions, discovery write access, and retained-message behavior without
modifying broker users, ACLs, or configuration.

## MQTT TLS

The panel agent supports plaintext TCP (`MQTT_TLS_ENABLED=0`), strict TLS with
its system/public CA store (`MQTT_TLS_ENABLED=1` with no CA file), and strict
TLS with a custom public CA for manual deployment. The integration package
contains a future-flow helper that uploads exact CA bytes as mode `0644` to a
content-addressed path below `/var/brilliant-mqtt/tls/`; the mode-`0600`
environment stores only that path. Old CA files are not rewritten or deleted
during staging, so an older environment snapshot can still reference its prior
trust material.

The current one-panel integration UI, adoption, reconfigure, repair, and update
paths are not TLS-aware. They can discard manual TLS settings and regenerate
the environment with `MQTT_TLS_ENABLED=0`; do not place a manually
TLS-configured panel under that lifecycle management until Plan 2 wires the
packaged seam throughout the fleet flow.

TLS verifies both hostname and certificate chain and never falls back to
plaintext. Anonymous access, insecure certificate bypass, mutual TLS, and MQTT
over WebSockets are unsupported by the panel transport.

## OTA survival

- App + unit live in `/var` (persistent). The interpreter path is stable.
- `/data/switch-embedded` (and thus the Cython libs the bridge imports) is
  replaced on every OTA — if the libs' API drifts, the bridge can break silently.
- If you can gate/mirror firmware updates, do — **after any firmware bump:**
  re-run a read-only bus smoke test (connect, `get_all()`, subscribe) on one
  panel to confirm the bus API is unchanged, then let the rest update.
- If a unit in `/etc/systemd/system` does NOT survive OTA on your firmware,
  re-install + re-enable it as a post-OTA step. The companion HA integration
  (see `docs/ha-integration.md`) automates exactly this: it watches the panel's
  availability LWT + `brilliant/<panel>/bridge` meta topic, and restores the
  unit/env from the copies it stages under `/var/brilliant-mqtt/system/`.

## Roll-out order

1. Pilot ONE panel. Soak ≥1 day.
2. Verify in HA: entities present, telemetry reflects manual panel changes,
   commands drive loads, LWT `offline` on agent kill, recovery on restart,
   entities return after an HA restart (retained discovery/state).
3. Roll out to the remaining panels (your configuration management); if
   publishing the BLE mesh loads, give exactly one panel `MESH_PRIORITY=1`
   and one or two standbys higher numbers.
4. Repoint HA automations to the MQTT entities; if the panels are HomeKit-
   paired, keep that pairing as a fallback.

## Rollback

- Stop + disable `brilliant-mqtt.service` on the affected panel(s); HomeKit (kept
  paired) remains the control path.
- Discovery topics are retained — to fully remove an entity from HA, publish an
  empty retained payload to its `homeassistant/<component>/<unique_id>/config`.
- When decommissioning a panel entirely, also clear its retained
  `brilliant/<panel>/bridge` meta topic the same way.
