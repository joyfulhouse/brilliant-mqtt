# diyHue CA-Recovery Hook — Design

**Date:** 2026-07-18
**Status:** Approved for planning
**Repo:** brilliant-mqtt (agent = Python 3.10; integration = Python 3.14 under `ha/`)

## Problem

The Brilliant panels' native Hue client is the local rendezvous for
Home-Assistant-backed lights (panel Hue client → diyHue → HA → Tuya bulb, no
SmartThings/cloud). For a panel to trust the diyHue bridge, our own CA is
appended to the panel's **pinned** Hue CA bundle at
`…/env/lib/python3.10/site-packages/lib/certs/hue-bridge-ca-certs.pem` (under
`/data/switch-embedded`). That file is **wiped on every firmware OTA** (`/data`
is replaced), which silently breaks the integration until the CA is re-appended
and the Hue coordinator restarted.

Two live-verified facts shape the design:

1. **The injected bridge credential persists across OTA.** The home-wide Hue
   config peripheral is backed by `/var/brilliant/object_store` (OTA-persistent,
   home-synced). Only the CA (in `/data`) is lost. → The hook is **CA-only**; it
   never re-injects the credential.
2. **Hue-integration hosting is home-wide and the leader moves.** The Hue
   coordinator runs on whichever panel is the elected host, and leadership can
   move between panels with no reboot (observed 2026-07-17: host moved
   `10.100.0.30` → `10.100.0.15` during an unrelated restart; the new leader
   lacked the CA and the integration broke). → The CA must be present on **every
   bridged panel**, and the hook must be **leader-agnostic**.

## Goal (narrow — YAGNI)

Guarantee CA durability only: ensure our CA is in the panel's Hue bundle and, if
the hook had to (re)append it, restart the **local** Hue coordinator so it
rebuilds its TLS context. Nothing more — no bridge-reachability probing, no
credential re-injection, no leader election.

## Architecture

Two independently-testable parts, mirroring the existing watchdog components:

1. **On-panel oneshot** — `src/brilliant_hue_ca/`, a small package whose
   `main()` runs one idempotent reconcile pass and exits, driven by a systemd
   **timer** (there is no state to hold between runs, so a persistent daemon
   would be wasteful).
2. **Integration component** — a new optional `hue-ca` entry in the companion
   integration's component registry that SSH-deploys the oneshot payload, the
   operator's CA cert, and the systemd units, per-panel, **off by default**.

## Part 1 — On-panel oneshot (`src/brilliant_hue_ca/`)

### Modules

- `reconcile.py` — the pure core. `reconcile(ctx) -> Outcome` where `ctx`
  carries the collaborators (filesystem + coordinator-control) behind Protocols.
  Steps:
  1. **Locate the bundle.** Try the configured path; if absent, glob for
     `hue-bridge-ca-certs.pem` under the site-packages tree (the venv path can
     shift across firmware). If not found → return `Outcome(bundle_found=False)`
     (non-fatal; the timer retries).
  2. **Ensure the CA.** Compute the provided CA's identity (SHA-256 fingerprint
     of its DER, derived from the CA PEM at runtime — **not** a hardcoded
     subject/CN, since the operator's CA is their own). If no cert in the bundle
     matches that fingerprint → append the CA PEM to the bundle →
     `appended=True`. If already present → `appended=False` (no-op).
  3. **Restart iff needed.** If `appended` **and** the coordinator control
     reports a local Hue coordinator is running (its vassal control file
     `…/processes/hue_bridge_peripherals.ini` exists) → request a restart (touch
     that file). If the vassal file is absent, this panel is not the current
     host → skip (normal, not an error).
  Returns `Outcome(bundle_found, appended, coordinator_restarted, bundle_path)`.

- `fs.py` — the filesystem boundary Protocol + a real implementation:
  `read_text(path)`, `append_text(path, text)`, `glob(root, name)`,
  `exists(path)`. Pure enough to fake in tests.

- `coordinator.py` — the coordinator-control boundary Protocol + real impl:
  `is_running() -> bool` (vassal control file exists) and `restart()` (touch the
  control file). Real impl uses the configured vassal-ini path.

- `config.py` — `Config` dataclass + `load_config(environ)` (same idiom as the
  watchdogs): `ca_cert_path` (`HUE_CA_CERT_PATH`, default
  `/var/brilliant-hue-ca/injected-ca.pem`), `bundle_path`
  (`HUE_CA_BUNDLE_PATH`, default the known path), `site_packages_root`
  (`HUE_CA_SITE_PACKAGES`, default for the glob fallback), `vassal_ini_path`
  (`HUE_CA_VASSAL_INI`, default `…/processes/hue_bridge_peripherals.ini`),
  `log_path`.

- `run.py` — thin `main()`: load config, build real collaborators, call
  `reconcile`, log the `Outcome`, exit 0 on success / non-zero on an operation
  error (append failed, CA PEM unreadable). Matches the watchdogs' "thin main,
  logic in functions" shape.

### systemd

- `deploy/brilliant-hue-ca.service` — `Type=oneshot`, runs the panel's
  Python 3.10 against the `/var`-based package, resource-capped
  (`MemoryMax`/`CPUQuota`/`Nice`), `[Install]` **omitted** (the timer owns
  activation — the service is never enabled directly).
- `deploy/brilliant-hue-ca.timer` — `OnBootSec≈2min` (covers the post-OTA
  reboot) + `OnUnitActiveSec≈15min` (closes the boot race where the coordinator
  starts before the oneshot, and heals any drift), `Persistent=true`.

### Idempotence & safety

- Steady state (CA present) is a pure no-op — no restart, so no runaway restart
  loop is possible: a restart only ever follows an actual append, and an append
  only happens when the CA is genuinely missing.
- The restart touches only the Hue vassal; it never restarts unrelated
  peripherals or the panel.

## Part 2 — Integration component (`custom_components/brilliant_mqtt/`)

Follow the existing watchdog component pattern exactly.

- **`const.py`:** add `COMPONENT_HUE_CA = "hue_ca"` and a config key for the CA
  PEM (`CONF_HUE_CA_CERT`).
- **`components.py`:** add a `REGISTRY` entry
  `Component(id=COMPONENT_HUE_CA, label="Hue CA recovery", locked=False,
  default_enabled=False, present=_hue_ca_present, install=_hue_ca_install,
  remove=panel_ops.uninstall_hue_ca)`. It is returned by `optional()`, so the
  existing select/switch entity machinery exposes it as a deploy-or-not choice
  automatically.
- **CA cert input:** the operator supplies their diyHue CA **public** cert PEM
  via `CONF_HUE_CA_CERT` (a config-flow / options field). The repo hardcodes no
  CA. `_hue_ca_install` writes it to the panel at `ca_cert_path` and refuses to
  install if the field is empty (`PanelOpError`).
- **`panel_ops.py`:** add `deploy_hue_ca` (push the `hue_ca` payload subdir +
  write the CA PEM), `ensure_hue_ca_units` (install BOTH the `.service` and
  `.timer`), `enable_hue_ca` (enable + start the **timer**),
  `inspect_hue_ca` → `payload_present`, and `uninstall_hue_ca` (disable/stop the
  timer, remove units + payload + CA PEM). Reuse the `_install_watchdog` helper
  where it fits; extend it (or add a timer-aware sibling) so `enable` targets the
  timer rather than a service.
- **Payload:** the deploy payload gains a `hue_ca/` subdir carrying the
  `brilliant_hue_ca` package (same vendoring approach as the watchdogs).

## Data flow

- **Deploy (integration → panel, SSH):** write `brilliant_hue_ca` to `/var` →
  write operator CA PEM to `/var/brilliant-hue-ca/injected-ca.pem` → install
  `.service` + `.timer` → enable+start the timer.
- **Runtime (panel):** timer fires (≈2min after boot, then every ≈15min) →
  oneshot `reconcile()` → CA ensured in the bundle; local coordinator restarted
  iff the CA was just (re)appended and this panel currently hosts Hue.

## Error handling

| Condition | Behaviour |
|---|---|
| Bundle file not found (path shifted) | glob fallback; if still missing, log + non-fatal exit; timer retries |
| CA PEM unreadable/empty at runtime | log + non-zero exit (systemd records failure); no bundle change |
| Append fails (permissions) | log + non-zero exit; no restart |
| Vassal ini absent (not the host) | skip restart — normal, not an error |
| CA already present | pure no-op, exit 0 |

## Testing

- **Agent (pytest, py3.10, `tests/`):** unit-test `reconcile()` with fake `fs`
  and fake `coordinator`:
  - CA absent + coordinator running → appended **and** restarted.
  - CA absent + coordinator absent → appended, **not** restarted.
  - CA already present → no-op (not appended, not restarted).
  - Bundle at default path missing but found via glob → operates on the globbed
    path.
  - Bundle not found anywhere → `bundle_found=False`, no throw.
  - CA-present matching is by fingerprint (a different cert with the same CN is
    NOT treated as present; the real CA with a re-wrapped PEM IS).
  - `load_config` env parsing (defaults + overrides).
  The suite must run off-panel (no panel imports; boundaries are faked) per the
  repo's non-negotiables.
- **Integration (pytest, py3.14, `ha/tests/`):** the `hue-ca` component wiring —
  `present`/`install`/`remove` call the expected `panel_ops` functions
  (mocked `PanelShell`); install refuses when the CA field is empty; the
  component appears in `optional()`; the select/switch entity reflects it.

## Rollout note

Enable `hue-ca` on **all** bridged panels, not a subset — the Hue host can move
to any panel, and a panel without the hook would strand the integration if it
became leader. (Documented in CONFIGURATION.md alongside the other components.)

## Non-goals

- No bridge-reachability / TLS-health probing (that was the rejected "broader"
  scope).
- No credential re-injection (the credential is OTA-persistent).
- No leader election or leader pinning.
- No changes to the diyHue deployment or the panel's Hue coordinator code.
