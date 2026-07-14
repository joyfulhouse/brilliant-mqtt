# Virtual Control research toolkit (`tools/brilliant_vc`)

`tools/brilliant_vc/` is a set of **repository-safe feasibility probes** for
the blocked Virtual Control research track — the investigation into whether
Home Assistant entities could ever appear as native Brilliant tiles through an
officially provisioned Virtual Control. It is **research tooling, not a
product feature**: nothing in it is installed, enabled, or started by
repository automation, and no gate in it is a substitute for the operator
approvals it enforces.

Where it fits:

- The ordered feasibility gates and per-gate operator procedure are in the
  [Virtual Control gates runbook](runbooks/virtual-control-gates.md).
- The recovered stock vassal lifecycle and privilege model are in the
  [Virtual Control runtime contract](virtual-control-runtime-contract.md).
- The implemented one-shot coordinated session is in the
  [coordinated VC session design](coordinated-session-design.md).
- The physical-slider acceptance procedure is in the
  [native slider E2E runbook](runbooks/native-slider-e2e.md).

## Safety model

Every tool is written to fail closed and to be individually incapable of the
dangerous step it prepares:

| Principle | How the toolkit implements it |
|---|---|
| Fail-closed gates | An ordered, secret-free evidence ledger (`gates.py`) records each gate's outcome; later stages validate the ledger before doing anything. |
| One-shot operator approvals | Live actions require a root-issued approval marker that a pinned, immutable OS mover atomically renames (consumes) before any repository code runs. Validators (`start_approval.py`, `session_approval.py`) can only read an already-consumed marker; they have no write, rename, subprocess, socket, or process-start capability. |
| Pinned, verified runtime | Staged app/vendor trees are validated file-by-file against committed SHA-256 manifests (`staged_runtime.py`, `deploy/*.sha256`); every unlisted file or directory is rejected. Firmware surfaces are hash-pinned before use. |
| No-start validation | Preflight and preparation modules (`launcher_preflight.py`, `runtime_prepare.py`, `session_prepare.py`, `vassal_manifest.py`) deliberately contain no process creation, socket connection, or executable command builder; `start_permitted` in the data-only manifest is always false. |
| Least privilege | Provisioned credentials are handed off from root-only material to a dedicated non-root runtime user with a minimal copied set (`runtime_handoff.py`); reference units run as that user with root exempted only for the pinned approval mover. |
| No secrets in evidence | Ledgers, monitors, and acceptance analyzers are redaction-safe: they record aggregates, comparisons, and counts — never credentials, tokens, private bindings, or panel identifiers. |

## Module inventory

| Module | Purpose | Live capability |
|---|---|---|
| `gates.py` | Ordered, secret-free evidence ledger for the feasibility gates | File writes to the ledger only |
| `audit.py` | VC0 prior-state and credential-permission audit | `lstat` only; never opens or hashes credential files |
| `token_check.py` | Claims-only inspection of an official bootstrap token | Read-only |
| `provision_panel.py` | One-shot, approval-gated Virtual Control provisioner | Calls the official provisioning endpoint after all preconditions pass; fails closed on any validation error |
| `identity_materializer.py` | Converts the provisioned PKCS#12 identity to the PEM pair the runtime expects, in a private isolated directory | No network, bus, launch, or control capability |
| `runtime_handoff.py` | Copies the minimal credential set into a non-root runtime directory | File copies only |
| `launcher_preflight.py` | No-start preflight of the pinned uWSGI runtime surface, identity, and filesystem topology | Read-only; reports the next unresolved contract |
| `runtime_prepare.py` | Validates pins/credentials/approval, then invokes only the stock `run.pre_exec` step | The single captured firmware call; no process start |
| `vassal_manifest.py` | Data-only candidate manifest of the pinned process topology | None; `start_permitted` always false |
| `monitor.py` | Bounded, redaction-safe monitor of a disposable VC process | Allowlisted `/proc` reads; can signal the monitored process |
| `staged_runtime.py` | Validates the root-owned session app and MQTT vendor tree against exact manifests | Read-only |
| `start_approval.py` / `session_approval.py` | Validate one consumed approval marker (bootstrap / coordinated session) | Read-only |
| `session_prepare.py` | Validates ledger, credentials, empty session roots, and approval, then delegates to `runtime_prepare` | As `runtime_prepare` |
| `session_coordinator.py` | Coordinates one approved non-root bootstrap plus one hosted HA-backed light | One-light host adapter only, after approval/topology/identity proofs |
| `single_light_pilot.py` | Fail-closed one-light feasibility pilot; HA stays authoritative through the MQTT control-plane contract | Deferred firmware/MQTT adapters; never accepts an HA token, never writes a slider binding |
| `slider_binding.py` | Private, read-only snapshots of physical-slider bindings | Read-only; snapshots are never committed |
| `e2e_acceptance.py` | Offline analysis of one-light gesture trials from a private transcript | None; never connects to MQTT, HA, or a panel |

## Reference deploy units

`deploy/brilliant-vc-pilot.service` (bounded bootstrap) and
`deploy/brilliant-vc-session.service` (coordinated one-light session) are
**reference-only**. They are not packaged, installed, enabled, or started by
repository automation. Both:

1. require a fresh approval marker to exist (`ConditionPathExists`);
2. consume it atomically with the pinned OS mover — the only root-exempted
   step;
3. run non-root validators (`staged_runtime`, `runtime_prepare` /
   `session_prepare`) that verify manifests, credentials, and topology before
   anything else executes; and
4. run as the dedicated `brilliant-vc` user with a hardened sandbox.

The committed `deploy/brilliant-vc-*-app-manifest.sha256` files are the exact
staged-tree pins those validators enforce.

## What this toolkit is not

- It does **not** make native HA tiles available — that outcome remains
  blocked behind unresolved live gates (see the
  [status page](native-ha-slider-validation-status.md)).
- It is **not** part of the `brilliant-mqtt` agent, the HA integration, or any
  install path; the supported HA surface is the
  [scene bridge and control plane](home-assistant-integration.md).
- It never bypasses Brilliant's official provisioning: identities come only
  from the official endpoint, under an operator's own account, after an
  explicit approval.
