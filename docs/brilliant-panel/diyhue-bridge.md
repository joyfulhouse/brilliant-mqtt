# diyHue as a local Brilliant ↔ Home Assistant control path

**Status:** proven end-to-end on production, 2026-07-18.
**What it gives you:** a Brilliant panel's own **native Hue integration** controls
Home-Assistant-managed lights (Tuya, WiZ, anything HA drives) — a wall slider →
real bulb, **fully local, no SmartThings, no cloud.**

This is the path that finally works after the SmartThings-virtual-dimmer,
Virtual-Control, and physical-Control-mirror approaches each hit a wall (see
[slider-bridge-feasibility.md](slider-bridge-feasibility.md),
[virtual-control-runtime-contract.md](virtual-control-runtime-contract.md)).
The durability automation for it ships as the **Hue CA recovery** integration
component — see [CONFIGURATION.md → Hue CA recovery](../CONFIGURATION.md#hue-ca-recovery).

---

## The idea in one picture

```
 Wall slider ─▶ Brilliant panel's native Hue client (per-home, leader-elected)
                          │  HTTPS Hue API v1, TLS pinned
                          ▼
        diyHue  (Hue-bridge emulator, in k3s on the IoT VLAN, 10.100.0.97)
                          │  Home Assistant WebSocket
                          ▼
                 Home Assistant ─▶ Tuya / WiZ / any HA-driven bulb
```

The panel already speaks Hue. diyHue makes Home Assistant *look like* a Hue
bridge. Point one at the other and the panel controls HA lights natively.

## Why it works (the key facts)

| Fact | Why it matters |
|---|---|
| **The panel is a Hue API v1 *polling* client** (`/api/<user>/lights\|groups\|config`, PUT `/state`). No `clip/v2`, no SSE, no `hue-application-key` in its binaries. | diyHue's weakest area (its rickety SSE eventstream) is irrelevant. diyHue's most complete surface — v1 REST — is exactly what the panel uses. |
| The Hue integration is **home-wide, hosted by one elected panel** (the "leader"), not per-panel. | Enumerated Hue lights become home-wide peripherals any panel can bind a slider to. But the leader can **move** — see [Leadership moves](#leadership-moves). |
| The panel **pins** its Hue trust to a bundled CA file, not the OS store, and does **real chain validation**. | A stock diyHue self-signed cert is rejected. The fix targets that one file — see [Trust](#trust-the-cert-wall). |
| Home-wide config (the paired-bridge credential) lives in **`/var`** (OTA-persistent, home-synced). | Only the CA (in `/data`) is lost on a firmware OTA. Recovery is CA-only. |

## Three walls, and how each is handled

Prior attempts (including HA's `emulated_hue` and stock diyHue) failed at one of
these. All three are handled:

1. **Discovery** — Brilliant switched Hue discovery to **mDNS** (`_hue._tcp.local.`);
   `emulated_hue` (UPnP only) is never seen. diyHue *does* advertise mDNS, but we
   don't rely on it: we **inject the paired credential directly** (below), which
   also skips the link-button pairing.
2. **Pairing** — the paired-bridge state is a writable **type-25 config
   peripheral** (`hue_bridge_configuration`) on the home's
   `configuration_virtual_device`. We write a `HueBridgeCredential`
   (`ip_address`, `username`, `bridge_name`) keyed by bridge id — the panel then
   treats diyHue as an already-paired bridge.
3. **Trust (the cert wall)** — see next section.

### Trust (the cert wall)

The panel builds its Hue HTTP client with
`ssl.create_default_context(cafile=…/lib/certs/hue-bridge-ca-certs.pem)` — a
**pinned** two-anchor bundle (Philips `root-bridge` + Signify `Hue Root CA 01`),
`verify_mode=CERT_REQUIRED`, connecting by IP with `check_hostname` off. A
stock diyHue self-signed leaf fails chain validation (this is the "SSL-Error"
behind diyHue issues #322/#339 and every prior attempt).

**Fix — with root on the panel, append your own CA to that specific file:**

- Generate an operator CA; sign an **EC P-256** leaf (diyHue's server ciphers
  are ECDSA-only) with `CN=<bridgeid>`, SAN = the diyHue IP.
- Drop that leaf into diyHue's replaceable `cert.pem`.
- **Append the CA public cert to the panel's pinned bundle** (matched by
  **DER SHA-256 fingerprint**, never CN). The panel's own SSL context then
  validates diyHue and returns `200`.

> The OS trust store is irrelevant — `update-ca-certificates` does nothing here.
> Only the pinned bundle file matters.

## Deployment (this fleet)

| Piece | Where |
|---|---|
| **diyHue** | k3s app `diyhue` (GitOps: `k3s-infra/k8s/diyhue/`), image `diyhue/core:latest`, HA-WebSocket backend, our-CA leaf seeded as `cert.pem` (SealedSecret). |
| **Reachability** | On the **IoT VLAN (10.100.0.0/20)** so the Brilliant panels reach it same-subnet with **no inter-VLAN firewall rule**. `server1` carries a netplan VLAN-52 leg (`enp1s0.52`); MetalLB **L2-only** pool announces the VIP **`10.100.0.97`** on it (`k3s-infra/k8s/metallb/` `iot-pool`/`iot-l2`). |
| **Panel trust** | Operator CA appended to **every** bridged panel's `hue-bridge-ca-certs.pem` (leadership moves — all panels need it). |
| **Credential** | One `HueBridgeCredential` (diyHue IP + a diyHue API username) injected into the home-wide `hue_bridge_configuration`. |
| **Durability** | The **Hue CA recovery** agent component re-appends the CA + restarts the Hue coordinator after every OTA — see below. |

## Exposing HA lights to the panel

diyHue only **state-syncs** (and therefore lets the panel control) HA entities it
is told to include. Two independent things must both be true:

1. **The light exists in diyHue** — either auto-imported (tagged, see #2) or
   added via `lights.discover.addNewLight(model, name, "homeassistant_ws",
   {"entity_id": "light.x"})`.
2. **The HA entity carries the include tag** — `diyhue: include` in HA's
   `configuration.yaml` under `homeassistant: → customize:`. **Without the tag a
   light populates onto the panel but stays `reachable=False` / panel `status=0`
   and the panel drops its commands.** (`homeAssistantIncludeByDefault: true`
   would import *all* ~400 HA lights — don't.)

```yaml
# HA configuration.yaml
homeassistant:
  customize:
    light.backyard_lamp_1:
      diyhue: include
```

Apply with the `homeassistant.reload_core_config` service (no restart), then a
diyHue pod restart forces a full state sync (it does **not** duplicate
already-served lights). See the [runbook](#operational-runbook).

## Leadership moves

The Brilliant Hue integration is hosted by one **elected** panel. It re-elects
on disruption (e.g. a diyHue pod restart) — observed moving `.30 → .15 → .22`
across a session. Consequences:

- The **CA must be on every bridged panel**, not just the current leader.
- After a leader move, the new leader's coordinator must (re)connect to diyHue;
  it will, because it already trusts diyHue (CA is fleet-wide).
- To find the current leader: read the `owner` field of the type-25
  `hue_bridge_configuration` peripheral (a device id) and map it to a panel via
  `/var/device_variables/device_id`. **Panel DNS names ≠ Brilliant display
  names** — map explicitly.

## Durability across OTA — the Hue CA recovery component

A firmware OTA wipes **both** `/data` (the CA bundle) and `/etc/systemd/system/`
(units). The agent's code, staged units, and injected CA survive in `/var`. The
**Hue CA recovery** component (`brilliant_hue_ca`) closes the loop:

- A systemd **timer** oneshot (`OnBootSec` ≈ 2 min after boot + every ≈ 15 min):
  locate the pinned bundle → if our CA (by DER fingerprint) is missing, re-append
  it → restart the local Hue coordinator **only if one is running here**
  (leader-agnostic; steady state is a no-op).
- The integration **re-lays the units + re-enables the timer** after an OTA in
  both panel-repair paths — otherwise the hook would be inert after the very
  update it targets.
- Off by default; enable on **all** bridged panels with the operator CA PEM.
  Full setup: [CONFIGURATION.md → Hue CA recovery](../CONFIGURATION.md#hue-ca-recovery).

## What's proven vs. what's operational

- **Proven end-to-end:** panel → diyHue (TLS via our CA) → HA → real bulb, both
  shed and backyard lamps; on/off exact.
- **Soft spots:** HA→panel *reflection* is poll-bounded (diyHue 10 s state poll +
  the panel's own poll) — display lag, not command lag. Brightness is approximate
  (intensity 0–1000 ↔ HA 0–255, localtuya calibration slop); on/off is exact.

## Operational runbook

**Add an HA light to the panel**
1. Tag the entity in HA `configuration.yaml` (`diyhue: include`), `ha core check`,
   then `homeassistant.reload_core_config`; verify the attr via `/api/states/<e>`.
2. Add it to diyHue (`addNewLight … "homeassistant_ws" {"entity_id": …}` in the
   pod) **or** rely on auto-import for tagged entities.
3. Restart the diyHue pod (deterministic full state sync; no duplicates).
4. Restart the **current leader's** Hue coordinator: `touch
   /var/run/brilliant/processes/hue_bridge_peripherals.ini`.
5. Verify: the light shows `status=1` on the `hue_bridge` device and a bus set of
   its `on`/`intensity` drives the HA entity.

**Find the current leader** — read the `owner` field of the type-25
`hue_bridge_configuration`; map the device id to a panel IP via each panel's
`/var/device_variables/device_id`.

**Teardown** — `kubectl delete ns diyhue`; revert `k3s-infra` metallb `iot-pool`/
`iot-l2` + the server1 netplan leg; restore each panel's
`hue-bridge-ca-certs.pem.orig`; remove the diyHue credential from the type-25
config.

## Deep dive

The full research record — firmware evidence, the /tmp TLS proofs, the
credential-injection thrift encoding, the leader-move incident, and the k8s/VLAN
buildout — is the operator-local report
`docs/claude/2026-07-18-diyhue-local-control-findings.md`.
