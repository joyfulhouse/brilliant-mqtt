# Deployment Reference

How the bridge runs on a panel, survives OTA, and gets its credentials. The
authoritative release gates are in
[Task 8 of the fleet-onboarding plan](../superpowers/plans/2026-07-21-fleet-onboarding.md#task-8-run-full-gates-and-one-panel-onboarding-canaries);
this is the operational quick reference.

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
packaged validator. Fleet onboarding proves authentication, both message
directions, Discovery write access, and retained-message behavior before it
creates the fleet. Each staged panel repeats the relevant broker checks before
activation. Validation never modifies broker users, ACLs, or configuration.

## MQTT TLS

The panel agent and fleet integration support plaintext TCP
(`MQTT_TLS_ENABLED=0`), strict TLS with the panel's system/public CA store
(`MQTT_TLS_ENABLED=1` with no CA file), and strict TLS with a custom public CA.
For custom trust, lifecycle operations upload the exact CA bytes as mode `0644`
to a content-addressed path below `/var/brilliant-mqtt/tls/`; the mode-`0600`
environment stores only that path. Old CA files are not rewritten or deleted
during staging, so rollback can still reference its prior trust material.

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

> **Office safety stop:** Office is reserved for the existing-external-broker,
> software-only canary. Keep its broker endpoint, TLS profile, and credentials
> unchanged. Do not send a light, switch, scene, mode, or other physical-load
> command unless the user separately approves one exact safe Office circuit.
> Run the outstanding official `core_mosquitto` qualification on a different
> disposable Home Assistant instance and pilot panel.

## Roll-out order

1. Pilot ONE panel. Soak ≥1 day.
2. Verify in HA: entities present, telemetry reflects manual panel changes,
   LWT `offline` on agent kill, recovery on restart, and entities return after
   an HA restart (retained discovery/state). Exercise load commands only on a
   separately approved safe circuit; this is explicitly not authorized for
   Office by the software canary.
3. Roll out to the remaining panels (your configuration management); if
   publishing the BLE mesh loads, give exactly one panel `MESH_PRIORITY=1`
   and one or two standbys higher numbers.
4. Repoint HA automations to the MQTT entities; if the panels are HomeKit-
   paired, keep that pairing as a fallback.

## Office exact-bundle parity gate

Before any Office update or redeploy, prove byte parity against the actual
integration loaded by Home Assistant—not a nearby checkout or a matching
version label.

The deterministic helper
[`scripts/brilliant-panel/bundle_manifest.py`](../../scripts/brilliant-panel/bundle_manifest.py)
emits exactly `<logical-path><TAB><sha256>`, rejects symlinks and special
files, rejects ambiguous filenames, and detects files, directories, or the
active-release selector changing while hashing. It excludes runtime Python
caches in every layout and excludes only the generated top-level
`brilliant-mqtt.env` and `mqtt-ca.pem` from an active panel release. Run these
commands from the qualified repository root. `ha-qualified` and
`office-qualified` must be preconfigured SSH aliases with pinned host keys;
never add an insecure host-key option:

```bash
set -euo pipefail
parity_evidence=artifacts/brilliant-panel/pilots/office-bundle-parity
manifest_helper=scripts/brilliant-panel/bundle_manifest.py
mkdir -p "$parity_evidence"

scripts/build_payload.sh
git diff --exit-code -- custom_components/brilliant_mqtt/agent_payload
test -z "$(git ls-files --others --exclude-standard -- custom_components/brilliant_mqtt/agent_payload)"
test -z "$(git ls-files --others --ignored --exclude-standard -- custom_components/brilliant_mqtt/agent_payload)"

uv run python "$manifest_helper" integration \
  custom_components/brilliant_mqtt \
  > "$parity_evidence/repository-integration.sha256"
uv run python "$manifest_helper" payload-release \
  custom_components/brilliant_mqtt/agent_payload \
  > "$parity_evidence/repository-payload.sha256"

ssh ha-qualified \
  'python3 - integration /config/custom_components/brilliant_mqtt' \
  < "$manifest_helper" \
  > "$parity_evidence/home-assistant-integration.sha256"
ssh ha-qualified \
  'python3 - payload-release /config/custom_components/brilliant_mqtt/agent_payload' \
  < "$manifest_helper" \
  > "$parity_evidence/home-assistant-payload.sha256"

ssh office-qualified \
  '/data/switch-embedded/env/bin/python3 - panel-release /var/brilliant-mqtt --unit /etc/systemd/system/brilliant-mqtt.service --wifi-unit /etc/systemd/system/brilliant-wifi-watchdog.service --bus-unit /etc/systemd/system/brilliant-bus-watchdog.service' \
  < "$manifest_helper" \
  > "$parity_evidence/office-active-payload.sha256"

diff -u \
  "$parity_evidence/repository-integration.sha256" \
  "$parity_evidence/home-assistant-integration.sha256" \
  > "$parity_evidence/integration.diff" || {
    echo "STOP: Home Assistant integration bundle differs" >&2
    exit 1
  }
diff -u \
  "$parity_evidence/repository-payload.sha256" \
  "$parity_evidence/home-assistant-payload.sha256" \
  > "$parity_evidence/home-assistant-payload.diff" || {
    echo "STOP: Home Assistant payload differs" >&2
    exit 1
  }
diff -u \
  "$parity_evidence/repository-payload.sha256" \
  "$parity_evidence/office-active-payload.sha256" \
  > "$parity_evidence/office-payload.diff" || {
    echo "STOP: Office active payload differs" >&2
    exit 1
  }
```

The `payload-release` layout hashes the complete static payload and adds
normalized `installed/` aliases for `VERSION` and all three release service
templates. The `panel-release` layout requires
`/var/brilliant-mqtt/current` to remain one stable symlink to one direct,
non-symlink child of `/var/brilliant-mqtt/releases`; there is deliberately no
legacy-layout fallback for this exact-release gate. It hashes the complete
active static release and maps `/var/brilliant-mqtt/VERSION` plus the installed
bridge, Wi-Fi-watchdog, and bus-watchdog units to those same normalized aliases.
A missing `current` selector or required installed unit, any command failure,
any rejected or changing filesystem entry, or any non-empty diff is a stop
condition.

Store only the path/hash manifests and their path/hash-only diff below
gitignored `artifacts/brilliant-panel/pilots/office-bundle-parity/`. Do not
capture file contents, environment files, CA material, credentials, logs,
host details, or command transcripts. The private provisioning journal is not
canary evidence and must never be copied into this directory.

Equality of `VERSION` values is necessary but is not proof of bundle parity.
Keep Office on its existing external broker throughout this gate. A passing
manifest comparison authorizes only the previously approved software canary;
it does not authorize physical load actuation.

Before the integration mutates Office, require its journaled snapshot to cover
the prior active-release selector, exact environment,
`VERSION`, bridge/Wi-Fi-watchdog/bus-watchdog units and their modes, enabled and
active service states, and selected components. The referenced immutable prior
release must still contain its complete static tree before mutation begins.
The integration must durably commit and re-read that private journal snapshot
before it changes the active selector or installed files. Record only the
transaction identifier and proven journal phase in sanitized canary notes;
never export journal contents, environment bytes, or CA material. An external
Home Assistant backup remains a sensible operator precaution, but is not an
additional canary gate because the onboarding transaction cannot create and
verify one itself.

Rollback means: stop the candidate, restore the saved files with their saved
modes, restore the prior immutable release if absent, atomically repoint
`current` (or restore the legacy fixed layout), run `systemctl daemon-reload`,
restore the recorded enablement/active states, and require fresh MQTT health
from the prior version. If the integration's automatic rollback or startup
recovery cannot complete that sequence, stop all further panel mutations and
surface the persistent repair issue; do not improvise a partial redeploy.

## Office 0.6.0 foundation canary

The 2026-07-27 Office pilot used the existing-broker path (not the official
Mosquitto add-on) and candidate commit
`40e1c398e1c067e63ea8ecdd17ec4cec6bc67f2b`, with normalized app/vendor
manifest
`6b1a95021d2b902dddfad8b4d629d43b5498476ed30228244f6c9d9acd1e20ec`.
The host-key-pinned deployment guard retained an exact rollback snapshot and
observed `0.5.7 → offline → 0.6.0 → online`, fresh retained-topic ownership,
one bus connection, one MQTT connection, and no degraded metadata.

Both the before and after resource windows contained 31 samples at 60-second
cadence. The fail-closed analyzer passed every invariant and threshold:

| Metric | 0.5.7 baseline | 0.6.0 candidate | Delta / limit |
| --- | ---: | ---: | ---: |
| Agent RSS p95 | 25,944 KiB | 26,544 KiB | +600 KiB / ≤5,120 KiB |
| Agent CPU | 9.0506% | 9.5169% | +0.4663 points / ≤2.0 |
| Message-bus CPU | 0.0216% | 0.0222% | +0.0006 points / ≤0.5 |
| Pre-existing message-bus peer failures | 1,279 | 1,146 | ≤1,412 |
| Bridge failures | 1 | 0 | no increase |
| Bridge timeout markers | 2 | 0 | no increase |

The agent, stock message bus, and panel UI kept stable PIDs for the complete
post window. The agent remained a single process with zero systemd restarts,
zero MQTT/bus reconnects, exactly one established MQTT connection, and a
healthy socket shape. `MemoryMax=96M` and `CPUQuota=20%` remained pinned by the
candidate unit.

Home Assistant independently reported the installed agent as 0.6.0,
availability online, clear bridge health, and no update in progress. Its
`latest_version` remained 0.5.7 because the live Home Assistant integration
bundle still contains the older payload. Do not run that integration's
update/redeploy action against this pilot until its bundled payload is
refreshed to 0.6.0; doing so could downgrade the panel.
This is the dated live-instance state captured by that canary, not the version
of the current repository payload.

The 30-minute technical/resource gate is complete. Keep Office as the sole
pilot for the ≥1-day soak. The final physical-control regression check still
requires an explicitly approved safe Office circuit; no load was actuated by
the automated canary.

This dated Office foundation canary does **not** qualify the fleet-onboarding
official-Mosquitto path. The Task 8 `core_mosquitto` canary remains outstanding
and must use a separate disposable Home Assistant instance and non-Office
pilot.

## Office read-only onboarding preflight

The 2026-07-27 Task 2 canary fetched Office's SSH identity without a username
or password, required the previously approved Ed25519 fingerprint during the
credentialed connection, and then ran the single bounded read-only inspection
command. The final candidate reported:

| Fact | Office result |
| --- | --- |
| Hostname / model | `b2qt-brilliant-imx6` / `Brilliant Control Development Board` |
| Architecture / firmware | `armv7l` / `v26.07.15.1` |
| Python / init | `3.10.9` / `systemd 250` |
| Available `/var` / memory | 4,742,729,728 bytes / 254,840,832 bytes |
| Installed agent | `0.6.0` |
| Active owned services | `brilliant-mqtt`, `brilliant-voice`, `brilliant-bus-watchdog` |
| Conflicting retired services | none |

The model is retained as a bounded descriptive fact rather than an invented
hardware allowlist; architecture, Python, firmware provenance/shape, service
manager, disk, and memory remain compatibility gates. The probe wrote no
files, changed no services, contacted no MQTT broker, and actuated no circuit.

## Rollback

- For a journaled integration rollout, use its automatic rollback/startup
  recovery contract described above. A rollback is complete only after the
  prior files, release selector/layout, component service states, and fresh
  prior-version MQTT health are all verified.
- If recovery reports `rollback_failed` or `recovery_failed`, stop further
  mutations and follow the persistent repair issue using the approved encrypted
  snapshot. Merely stopping the candidate is not an exact restoration.
- Discovery topics are retained — to fully remove an entity from HA, publish an
  empty retained payload to its `homeassistant/<component>/<unique_id>/config`.
- When decommissioning a panel entirely, also clear its retained
  `brilliant/<panel>/bridge` meta topic the same way.
