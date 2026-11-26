# Component store and stock-service replacement plugins

Status: proposed design for [#283](https://github.com/joyfulhouse/brilliant-mqtt/issues/283).
This document specifies future work. It implements no installer, grants no panel
deployment permission, and does not qualify [Atmosphere #280](https://github.com/joyfulhouse/brilliant-mqtt/pull/280)
or [HA Kiosk #237/#239](https://github.com/joyfulhouse/brilliant-mqtt/issues/239).
The operator asked for this design first on 2026-11-26. Decisions requiring an
operator are collected at the end; the safety requirements below are mandatory.

## 1. Scope and evidence

A component store is a reviewed catalog plus per-panel lifecycle management in
the existing HA integration. Plugins run outside the bridge process. Replacing
a corresponding stock Brilliant function is a first-class capability: its
contract includes disabling that exact stock implementation, exclusive ownership,
and verified restoration on disable, failure, uninstall and OTA. Merely adding
another microphone listener or external TV renderer does not prove replacement.

Grounding references, in order of authority:

- [PoC findings](../reference/poc-findings.md): the local bus, observer mirror
  staleness, Python 3.10.9, `/var` persistence and uncertain `/etc` OTA survival.
- [Architecture](../ARCHITECTURE.md) and [deployment reference](../reference/deployment.md):
  the single bus adapter, shared sessions, journaled exact rollback and resource
  canary. The dated Office canary is evidence for that bridge release only.
- [Panel software architecture](../brilliant-panel/software-architecture.md):
  i.MX6 DualLite, two cores, roughly 1 GB RAM, no swap, near-saturated workloads;
  `message_bus.service` is an Emperor supervisor, not an optional feature unit.
  `art` and `voice` are child vassals. `switch_ui_app.service` is separate and
  its watchdog/failure policy can reboot the panel.
- [Peripheral surfaces](../brilliant-panel/peripheral-surfaces.md): screen,
  privacy, art and voice boundaries; writable infrastructure is not a public API.
- [Existing component registry](../../custom_components/brilliant_mqtt/components.py),
  [panel operations](../../custom_components/brilliant_mqtt/panel_ops.py),
  [provisioning journal](../../custom_components/brilliant_mqtt/provisioning_journal.py)
  and [provisioner](../../custom_components/brilliant_mqtt/panel_provisioner.py):
  reuse their selection, pinned SSH identity, admission, durable baseline,
  recovery and redacted-error seams. Their present snapshot schema is for core
  services; it does not already snapshot arbitrary stock replacements.
- [Kiosk declaration](https://github.com/joyfulhouse/brilliant-mqtt/issues/283#issuecomment-7109372560)
  and [newer Phase 1 evidence](https://github.com/joyfulhouse/brilliant-mqtt/issues/239#issuecomment-7109373023):
  upstream reports non-admin layout/API checks and atomic secret-file fixes;
  TTS/media and actual OTA remain unqualified. These are contributor reports,
  not local verification. Its current bus display drop-in/restart conflicts
  with this proposal's protected-core rule.
- [Atmosphere review](https://github.com/joyfulhouse/brilliant-mqtt/pull/280#issuecomment-7115324280):
  reproducible privacy, activation, registration, network and resource findings
  remain acceptance requirements for a plugin revision.

The existing architecture's prohibition on UI injection/preload remains. A
separately approved replacement may stop the standalone UI unit through this
contract; editing or injecting its binary is not permitted. This design does
not silently authorize exceptions to existing panel-safety runbooks.

## 2. Owners and protected core

There is one lifecycle owner per panel: the HA brilliant-mqtt manager, backed by
a small first-party local supervisor/recovery helper. The helper is new work,
not a capability of today's bridge. HA stages via the existing `PanelShell`
SSH/SFTP boundary with pinned host identity; normal runtime and crash recovery
must not depend on HA, MQTT, DNS or an SSH connection being available.

A separate first-party rescue service uses systemd's independent monotonic
timer/watchdog to monitor the helper's renewable transaction and ownership
leases. It lives outside the helper/plugin cgroups and does not share their
worker loop. Before stock is stopped, it must acknowledge the durable intent,
baseline and absolute recovery deadline. Proposed leases expire in five seconds
with two-second renewals; renewals cannot extend a phase's absolute deadline.
On helper exit, OOM, hang or lease
expiry, systemd stops the helper and its workers; rescue verifies their complete
quiescence before acquiring exclusive recovery authority. It then fences/kills
the plugin cgroup and restores stock using the journal, without HA or reboot.
No concurrent recovery writers are permitted. If it cannot establish writer
quiescence, it fences plugin device access and signals a local failure rather
than racing another writer. Rescue has bounded attempts and an independent
local failure indicator plus persistent repair evidence if restoration fails;
it never escalates to a panel reboot. Qualify this path on each target/profile,
including termination/hang of both the helper and its candidate. A target that
cannot guarantee this recovery trigger is ineligible.
Rescue uses its own qualified executable/runtime in the persistent recovery
root, independent of the helper's firmware Python and imports. It has a
systemd-owned trigger and bounded restart/start-limit policy. Test helper
interpreter loss, repeated helper startup failure and rescue failure itself;
do not install if the profile lacks a surviving local recovery path. A failed
rescue leaves plugin access fenced, records a persistent/local failure and
requires the named on-site recovery owner; it must never loop or reboot.

Only the first-party helper may change stock service state. Plugin processes
cannot run `systemctl`, edit units/drop-ins, invoke privileged installers, reboot,
write firmware, use the root SSH identity, or alter the helper/journal. Root
installation code is part of the reviewed trusted computing base; sandboxing a
runtime cannot make arbitrary root install hooks safe. No manifest-supplied
shell hooks or unit text are executed. A catalog selects compiled-in, reviewed
adapter IDs and validated data; it cannot introduce new root behavior.

Protected by an allowlist of permitted replacement targets and a second,
non-overridable deny policy:

- `message_bus.service`, its core bus vassal, Emperor, zygote/fork-server,
  bootstrap/configuration/object-store/monitor processes and any transitive
  dependency needed for bus, wired-load, faceplate, mesh, networking or recovery.
- The bridge, its bus/MQTT credentials, heartbeat/phase files, recovery helper,
  and selected core watchdogs. Plugins cannot change watchdog reboot policy.
- Unit aliases, dependency edges, templates, generated configs and filesystem
  paths resolving to those protected processes. Stopping a parent to remove
  an `art` or `voice` child is explicitly forbidden.

No plugin may mask, disable, replace, add a drop-in to, restart or namespace
remap the protected core. New adapters must resolve the full dependency graph,
canonical unit identities and external supervisors against a firmware profile;
unknown edges or aliases fail admission. Even a first-party catalog signature
cannot override this policy. Only a separate core design/change can extend it.

For stock vassals, a trusted firmware profile must establish an independently
supported stop, persistent disable and exact restore operation without restarting
the supervisor or impairing required dependencies. Deleting a generated INI,
killing a PID, or setting a guessed process flag is insufficient: bootstrap may
respawn it. No such qualified adapter is established here; art/voice replacement
stays unavailable until one exists. There is no invented `art.service`,
`voice.service`, or stock Cast unit.

## 3. Versioned manifest and catalog

Contract version 1 is strict JSON, bounded to 64 KiB, with duplicate keys,
unknown keys, invalid types and unknown schema versions rejected. The YAML
below illustrates the same data model; placeholders are intentionally not an
installable release. Catalog records authenticate the entire manifest as well
as artifact bytes. The bundle's manifest must match the authenticated record.

```yaml
schema_version: 1
id: ha_kiosk                         # [a-z][a-z0-9_]{0,63}, immutable
version: 1.6.0                      # exact release, never latest/nightly alias
publisher: reviewed_kiosk
artifact:
  url: https://releases.example.invalid/ha-kiosk-1.6.0.tar.gz
  sha256: <64-lowercase-hex-from-reviewed-catalog>
  size_bytes: <exact-reviewed-size>
  unpacked_bytes_max: <reviewed-limit>
  files_max: <reviewed-limit>
compatibility:
  architectures: [armv7l]
  python: '3.10'
  firmware_profiles: [<exact-qualified-profile-id>]
  manager_api: 1
  bridge_plugin_api: 1
runtime:
  adapter: kiosk_v1
  entrypoint: bin/kiosk
  resources_profile: display_pilot_v1
capabilities:
  network_egress: []                 # HA access is mediated below
  network_listeners: []
  microphone: false
  camera: false
  display_takeover: true
  personal_media: false
  provider_accounts: []
  ha: {layout: configured_dashboard, reads: [configured_entities], actions: []}
  bus: {reads: [own_panel_summary], writes: [own_wired_on_intensity]}
  hardware: [display, backlight, touchscreen]
  secrets: []                        # trusted HA broker retains its token
  retention: {media_disk_bytes: 0, media_lifetime_seconds: 0}
replaces:
  - target: stock_touchscreen
    kind: systemd
    identity: switch_ui_app.service
    firmware_adapter: stock_ui_v1
conflicts: [other_display_owner]
ownership:
  surfaces: [screen_power, screen_brightness, screen_idle, screensaver, gestures]
  topics: [plugins/ha_kiosk]
health_profile: kiosk_frames_and_local_control_v1
```

Additional catalog fields are manifest digest, publisher/key identity, review
revision, signature envelope when required, license/source, support owner,
firmware evidence links, revocations and minimum safe version. These records
are data shipped with a reviewed integration release. Origin aliases resolve
to consented exact scheme/host/port pairs; they are not arbitrary host globs.
The manager validates the manifest against its own hard maxima and profile
allowlists. An entrypoint is a regular file within the verified release; no
arbitrary command arguments, paths, capabilities or service names are accepted.

Each replacement names its stock function, actual unit/vassal identity,
firmware adapter, required dependencies, exclusive devices and entity/command
surfaces. The catalog's firmware profile pins the expected stock topology,
configuration fingerprints, external supervisor behavior and restoration
algorithm. It must explicitly cover initial states such as disabled or masked.
Unsupported states fail preflight instead of being normalized to enabled.

Capabilities cover raw and derived data. Microphone approval includes capture
mode and named STT destinations; camera includes frame/clip handling;
personal media includes album URLs, captions and history. Inbound listeners
specify bind interface, port, protocol and authentication separately from egress.
Subfeatures cannot use an undeclared capability. Expanded permissions, target
changes, endpoint changes or higher budgets require new per-panel consent.

## 4. Trust, packaging and secrets

Recommended first release: a first-party reviewed catalog only. It can contain
an externally authored plugin after source, adapter and packaging review; that
does not enable arbitrary third-party catalogs. The root of trust is the
reviewed integration release and its pinned manifest/artifact SHA-256 and exact
size, not a checksum or `release.json` downloaded beside an untrusted tarball.
Verify on every download, cache hit and panel staging, before extraction or
execution. Never execute a downloader-supplied installer to verify itself.

Downloads use verified HTTPS, finite time/byte limits and an origin allowlist.
Reject unexpected redirects; credentialed requests never forward credentials
to another origin or downgrade transport. Extraction has independent byte,
file-count, depth and filename limits; reject absolute/parent paths, links,
devices, FIFOs, duplicate members and unexpected content. Verify the complete
file inventory and modes; keep current/previous verified releases immutable to
the plugin UID. Free space must cover the expanded candidate, retained previous
release and recovery reserve before any stock mutation.

Before opening third-party catalogs, require signed metadata binding publisher,
plugin ID, manifest digest, version, artifact digest/size and expiry to an
operator-trusted key. Require monotonic catalog revisions and persisted highest
accepted revisions to prevent rollback/freeze; invalid clock/expiry evidence
blocks every activation path. Key rotation is signed by an existing trusted key
plus reviewed trust-root update; compromise recovery uses an out-of-band
integration trust-root update. A signature establishes provenance, not safety;
all adapters, capabilities and firmware targets still require review.

Revocations identify exact digests/keys and a safe action. Reject revoked
candidates and previous releases during rollback. On receipt of a verified
runtime revocation, stop the affected plugin and restore stock; retain repair
evidence. Offline panels retain the last verified revocation set and show its
age; there is no claim of instant offline revocation. Do not introduce a
network-fetched remote-shell emergency switch. No automatic plugin updates.

One common start-admission check governs install, enable, update, rollback,
automatic recovery and boot reactivation. It revalidates artifact and manifest
identity, current persisted revocations, minimum safe version, required metadata
freshness/clock evidence, firmware/API compatibility, consent and ownership.
Persist monotonic catalog/revocation/minimum-version decisions in the protected
recovery root, independently of release rollback, for offline use. A verified
non-revoked artifact below the current minimum safe version is still ineligible.
Expired/missing trust evidence or an unreliable required clock forbids any new
plugin activation, including automatic recovery to an old plugin: restore stock
instead. An already running plugin follows the last verified runtime policy
until a revocation or local safety event, but metadata expiry never permits a
restart. Do not weaken this check because HA or the catalog is unreachable.

Use operator-created least-privilege HA tokens initially. No automatic user
creation or admin token for the pilot; admin-only capture upload is unavailable.
Non-admin is not equivalent to narrowly scoped: HA may permit broad household
state/event reads and media browsing. The trusted HA-access broker holds the
token; it is never delivered to the plugin. Its separate scoped IPC exposes
only consented layout fields, entity projections and semantic actions. It
filters snapshots/events and rejects arbitrary REST paths, WebSocket frames,
service calls, dashboard references and media requests. The plugin has no
direct authenticated HA HTTP/WSS path, including through media URLs, redirects
or another local proxy. A dashboard cannot expand its own capability grant.
Even same-origin requests require operation/entity authorization. With the
example manifest, personal-media browsing and unrelated entity reads are denied.
If required UI behavior cannot work within this boundary, list the missing
capabilities for explicit consent or make the release ineligible; never label a
raw broad token as scoped. The broker is a separate capped trusted service,
included in helper-side resource accounting and implementation review.
Never share bridge MQTT credentials with a plugin. Prefer brokered publication;
if a later reviewed adapter needs direct MQTT, it requires a separate scoped
principal and ACLs that cannot write core state/discovery/commands.

Secrets remain outside artifacts, manifests, status, diagnostics and retained
MQTT. Private persistent directories are mode 0700, files 0600, atomically
created with exclusive temporary files, fsync and rename. Store root-owned
persistent copies; pass only the particular secret to a plugin UID through a
qualified credential mechanism or private runtime copy. Delete runtime copies
on stop. Never grant directory access to other plugins or expose root-only
files by broadening permissions. Rotation replaces copies atomically, fences
old sessions, and requires operator revocation of the old provider token.
Uninstall removes every live, staged and recovery copy of that plugin's secret
after stock restore. A retained previous release contains code, not old tokens.

## 5. Durable per-panel lifecycle

Extend the existing component seam; do not replace it with a second installer.
The HA journal and panel-side journal share a transaction UUID, panel identity,
owner ID, firmware/profile and boot identities, authenticated release identities
and phase.
Extend the current journal schema with explicit versioned plugin/stock records;
old records cannot be interpreted as a complete replacement baseline.

Serialize all core/plugin/update/repair/OTA operations under the existing HA
admission and a panel-local exclusive lock. The local helper executes detached
bounded transactions and exposes bounded status polling; losing SSH does not
cancel safety recovery or permit a second writer. Acquire locks in one fixed
order; helpers resume the recorded operation rather than nesting another
lifecycle transaction. A manager ownership marker prevents the kiosk's
standalone integration from updating/removing a brilliant-mqtt-owned panel.
An ownership transfer is an explicit offline transaction; no force takeover.

Proposed persistent layout (all new plugin state is below `/var`):

```text
/var/brilliant-mqtt/plugins/<id>/releases/<version>-<digest>/
/var/brilliant-mqtt/plugins/<id>/current
/var/brilliant-mqtt/plugins/<id>/config/       # root-private persistent config
/var/brilliant-mqtt/plugins/<id>/data/         # bounded, plugin-writable data
/var/lib/brilliant-plugin-recovery/journal/   # private durable transactions
/var/lib/brilliant-plugin-recovery/baselines/ # stock and prior-plugin snapshots
/var/lib/brilliant-plugin-recovery/system/    # rescue/helper/unit templates
```

Immutable releases are root-owned and not writable by plugins. Temporary IPC,
leases and runtime secret copies live under a private `/run` subtree. The
trusted helper and its interpreter must remain executable independently of
plugin artifacts; qualify their availability on each supported OTA profile.
Recovery state and rescue executables deliberately live outside the existing
`/var/brilliant-mqtt` recursive removal root. Core uninstall must first drain
and verify stock handback for every active, staged or failed plugin, then settle
pending restoration before stopping the bridge or deleting any plugin assets.
A failed/interrupted handback blocks destructive cleanup. Unresolved topic debt
or provider revocation survives in protected recovery state and is shown as
such; it cannot be discarded by HA entry removal. Removing an entry either
completes handback first or leaves the independent rescue owner and a durable
repair/re-adoption record. It cannot abandon a stock suppression.

Installer admission also requires a compatible core removal/downgrade guard.
Older managers that do not understand replacement records cannot manage this
panel: retain independent rescue state, reject unsupported owner/protocol
versions before mutation, and require stock handback before changing to an old
manager or core. A legacy removal of `/var/brilliant-mqtt` cannot delete rescue;
loss of the bridge/plugin tree expires the local lease and triggers stock
restore. Test this using the actual legacy removal shape, not just the new API.
Rescue removal is the final separate cleanup step after all stock restoration
and recovery obligations are verified, never an incidental core `rm -rf`.

### Snapshot before mutation

Inventory canonical units, aliases, fragment/drop-in bytes and metadata
(owner/group/mode and relevant xattrs), exact symlink targets and absence,
enablement links (enabled/disabled/static/indirect/masked, runtime vs persistent),
active state, and adapter-owned configuration. For vassals also capture the
process-manager's persistent enable policy and generated-config provenance.
Record watchdog/restart/failure/start-limit policy, external supervisors,
dependencies and device owners. Empty or ambiguous probes are errors; a
`systemctl is-enabled` string alone is insufficient.

Capture the pre-transaction plugin selector/release, configuration, resources,
consent, entity routes and stock ownership as well. Retain the original stock
baseline separately across plugin upgrades; taking a new snapshot of an
already-disabled stock service must not overwrite it. Recovery records may
contain private stock data and never enter public diagnostics or canary notes.
Durably commit, fsync and re-read both baseline content and digest before
changing any selector, route, unit, stock configuration or service state.

Preflight requires stable active/inactive states, no pending jobs, no stock
failure loop, available disk, working recovery hook and compatible firmware.
Within the captured boot, originally inactive stock remains inactive after
restoration and originally masked stock remains masked. Do not clear an
operator mask just to activate a plugin.
Concurrent out-of-band changes invalidate the baseline and stop admission.

### Handoff and commit

1. `preflight -> staged`: validate consent, trust, compatibility, ownership and
   budgets; stage verified code and a resource-capped unit. Probe candidate
   config/self-test in a device-free, no-actuation mode while stock still runs.
2. `baseline_committed -> handoff`: persist intent and suppress commands on the
   surfaces being transferred. Stop the exact allowlisted stock target through
   its reviewed adapter, then disable its independent respawn sources. Apply a
   mask only when that firmware adapter proves it is needed and safe. Verify
   the stock process is gone and exclusive hardware is released. No physical
   display or microphone health probe may compete with the stock owner.
3. `candidate_starting`: start the candidate under its new ownership generation.
   Hardware leases are issued only after stock quiescence. The local helper
   checks cgroup identity, readiness, privacy, resource and core-health evidence
   within 90 seconds; a separate profile can demand stricter limits.
4. `probation`: require at least 60 seconds of fresh health, including display
   frame progress/local-control readiness for kiosk and no core regressions.
   HA connectivity is reported separately; a network outage cannot cause a
   reboot/retry loop. Each profile defines local degraded behavior and the
   minimum readiness needed to keep the replacement.
5. `active`: commit the ownership generation, routes and desired release only
   after health passes. Retain stock baseline and previous verified plugin
   until explicit qualification/finalize. Persist each intent before its side
   effect and completion afterward, so every crash point is replayable.

All phases have finite deadlines; maximum handoff plus probation is 180 seconds
for the proposed pilot. Failure kills/drains the entire candidate cgroup and
revokes its leases before any stock restart. Never enable both owners to make
a health check pass. The helper is independent of the candidate and monitors
it continuously after commit; missing leases, OOM, exit, stale health or a core
regression triggers restoration, without needing HA. Candidate units use no
automatic restart in the pilot; one failed attempt leads to recovery, not a loop.

### Recovery and removal semantics

| Operation | Result required before reporting success |
|---|---|
| Disable | Stop/drain plugin, revoke routes/leases, restore exact stock state; retain verified code, consent and private config for a later explicit enable. |
| Enable | New preflight and journaled handoff against the original stock baseline; fresh privacy/session evidence and consent if anything changed. |
| Update | Retain prior code/config and original stock baseline; stage pinned candidate, fence old work, hand off once, check health, commit. |
| Rollback | Restore the prior verified, non-revoked plugin and compatible configuration/consent, or exact stock if prior plugin cannot pass; report which result occurred. |
| Failed activation / crash / interrupted transaction | Local bounded recovery to prior qualified plugin if compatible and safe, otherwise stock; never blindly rerun installation. |
| Uninstall | Restore stock and core routes first, delete secrets and owned retained topics, then remove releases/units/config. Retain only consented non-sensitive preferences and sanitized recovery result. |
| Purge | Uninstall plus remove retained preferences/media/cache; delete private baseline only after verified stock restoration and no pending recovery. |

Same-firmware, same-boot stock restoration means exact saved file/link/absence state and
metadata, original enablement/masks and original active/inactive state. Stop
the plugin and prevent further side effects first; remove only manager-owned
overrides, restore saved config, daemon-reload if required, restore enablement
and masks, then start only targets originally active. Validate expected stock
health plus independent bus/bridge/load-control health, using a qualified
profile. A saved PID is evidence of identity, not a restorable value. Expected
restarted UI/vassal PIDs may change; protected core PIDs must remain stable
during same-boot plugin operations. Restore entity routes from the saved map.

Exact replay of runtime enablement/masks and temporary active state applies
only to the recorded boot ID. Persistent operator policy is stored separately
from these boot-local facts. After an ordinary reboot on the same firmware,
retire old leases and apply persistent policy plus the firmware's normal boot
semantics: a runtime-only mask expires, and an enabled service temporarily
stopped in the previous boot may start normally. Do not resurrect runtime links
or classify expected boot changes as drift. A plugin stays stopped until a
fresh boot baseline, consent/session checks and handoff grant new ownership.
Unknown boot identity blocks handoff; never infer it from firmware version.

An originally masked/inactive service is a valid restored state and does not
require a running health signal. Service restoration never replays old privacy
or consent values over a newer denial. Persist current denies separately from
the rollback snapshot and enforce them for stock as well as plugin capture.
If the adapter cannot restore a stock service without reopening denied capture,
keep that optional function stopped/fenced, report `recovery_required`, and
preserve the exact baseline for resolution; do not call that exact restoration.
Qualify denial followed by plugin failure/disable/OTA, not only plugin teardown.
Conflicting live operator changes, missing
baseline or failed restore produce `recovery_required`, persistent HA Repair,
and no further automatic mutation. Preserve private journals/artifacts; do not
claim rollback because the candidate stopped. Topic cleanup or provider token
revocation may need a separate retry/reminder after successful local stock
restore. Offline MQTT cleanup is durable, plugin-scoped debt; block reinstall
until stale commands/topics are reconciled or fenced by a fresh generation.

## 6. Firmware OTA and core updates

`/data/switch-embedded` and `/data/switch-ui` are replaced by firmware OTA;
`/var` persists, and `/etc` units/drop-ins may disappear. Old firmware binaries
or vendor unit fragments must never be copied over new firmware. Keep two
identities: the immutable original baseline for its firmware epoch and a
separate post-OTA inventory for the new epoch. Exact restore applies within
one epoch; across epochs the requirement is the new firmware's authentic stock
implementation with supported operator policy reapplied, not old vendor bytes.

Before a managed OTA, revoke plugin leases, stop it and restore stock while
the old firmware is still present. Mark the transaction `ota_suspended` and
disable plugin auto-start. Never gate a core boot on plugin health. After any
firmware identity change (including an unexpected OTA), the local boot gate
refuses plugin startup until it inventories the new deployment, checks stock
topology and interpreter/API compatibility, and reconciles any surviving masks
or overrides owned by the manager. Authenticate and inventory fresh vendor
stock files without starting optional functions. Map the operator's saved
persistent policy through a qualified adapter before any affected activation;
otherwise retain the evidence and require repair/decision.
Never enable an old masked target name on an unknown topology.

An OTA profile must prove that manager-created suppression cannot strand stock
off when `/etc` or manager units disappear. Prefer boot-scoped suppression with
no persistent mask/disable when independently safe. If persistent suppression
is required, admission requires an independently qualified early recovery hook
that actually runs after this OTA and reconciles it before stock needs to start.
Copying a unit into `/var` alone is not that proof. If hook persistence/order,
safe suppression expiry or vendor stock recovery cannot be guaranteed, that
replacement is ineligible for installation. Default to stock, plugin stopped.
HA's existing availability/firmware reapply path is a second recovery channel,
not the only means of getting a working screen back when HA is unreachable.

Authenticating/inventorying vendor files does not authorize starting them.
Persistent operator denies (including microphone/camera masks) must be mapped
and enforced before the corresponding new stock process can acquire devices;
there can be no transient default-enabled capture. The pre-install firmware
profile must prove an independently preserved deny and boot ordering across
lost `/etc`, changed target identities and HA unavailable, without blocking
protected core boot or guessing unknown unit names. Otherwise refuse that
initial state/profile before installing a replacement. An unmappable deny
leaves the affected optional function fenced and repair required; it never
becomes permission to enable vendor defaults.

Unknown firmware means no plugin reactivation. A known new firmware profile
must authenticate vendor provenance, map changed targets/dependencies, pass a
fresh read-only bus/API smoke check and capture a new stock baseline before an
explicit enable. Keep old baselines for audit/recovery on a genuine firmware
rollback, never as instructions to overwrite the new deployment. Actual OTA
qualification must include lost `/etc`, surviving `/etc` and HA unavailable;
deleting files to simulate OTA does not establish real OTA behavior.

Core updates declare the plugin API versions they retain. Under the shared
lock, check all installed plugins before core mutation. Incompatible plugins
must be disabled/restored before upgrade, with the outcome shown in HA; if
restore fails, block upgrade. Core/plugin rollback restores a coherent version
set and route semantics, not independently selected files. Every rollback,
stock handback, re-enable, manager recovery and boot creates a fresh,
never-reused authority epoch; never restore an old epoch with code/config or
route snapshots. Use an independently persisted monotonic counter plus a fresh
unpredictable nonce, outside release/config rollback. Loss/corruption of that
authority state denies commands and requires re-admission. Do not
remove a shared API while a compatible previous-release rollback depends on it.

## 7. Protected bus and entity ownership

Keep `lib.message_bus_api` imports only in `src/brilliant_mqtt/bus.py`. No new
standalone-plugin exception is granted. Introduce a narrow, versioned local
IPC API served by the bridge boundary; there is no second plugin observer or
full-home poller. IPC parsing/admission must stay bounded so plugin traffic
cannot starve hot polls or load commands. Heavy indexing, STT, Cast, networking
and provider work remain in plugin cgroups.

Use a private Unix socket, peer UID credentials and a root-owned mapping from
unique plugin UID to consented permissions. Runtime tokens alone are not an
identity boundary. Hide the real bus socket, bridge credentials and other
plugins' sockets/files with qualified mount/device/process isolation. The
runtime has no root or capabilities. Network policy also prevents bypass via
an alternate exposed control endpoint; all these restrictions require negative
tests on each supported kernel/systemd profile before listing a plugin.

Initial API offers scoped own-panel snapshots/events and an allowlist of
semantic requests. It rejects arbitrary device IDs, variable names, raw Thrift,
process configuration, firmware/reboot/calibration and foreign-panel writes.
Use the existing validated command path for separately approved local controls;
runtime permission does not authorize hardware qualification commands.
No native peripheral hosting in v1; adding it requires a separate reviewed
ownership/deletion contract. In particular, plugin registration must not revive
the rejected physical-Control co-management path.

The kiosk's `own_wired_on_intensity` grant is a semantic request mediated by the
bridge, not a transfer of load ownership. The trusted profile resolves it only
to this panel's qualified LIGHT/GENERIC_ON_OFF wired peripherals and validated
on/intensity operations (intensity only for dimmable LIGHT). It cannot select
foreign panels, mesh, scenes, calibration or raw variables. This local IPC path
must work when HA is unavailable and pass local-control readiness before
handoff completes; HA API access cannot substitute. Test denied scopes offline,
and qualify actuation only on the separately authorized safe circuit.

Proposed v1 hard limits per plugin: 16 KiB messages, 64 queued events, two
in-flight requests, five semantic requests/second (burst ten), with lower
profile-specific bus-write limits. Core traffic has priority. Overflow drops
the plugin stream and requires a bounded fresh scoped snapshot; no unbounded
queue or silent continued use of stale data. A new bus session invalidates all
old capabilities/readiness until a fresh baseline. Health is not inferred from
`get_all()` alone because it can read a frozen observer mirror.

The bridge owns discovery and publication for plugins by default, restricting
them to `brilliant/<panel>/plugins/<id>/...` and an assigned discovery ledger.
It never accepts a plugin-selected core topic, unique ID or wildcard. Commands
have generation, ID and expiry; retained/replayed commands cannot activate a
plugin. State distinguishes requested versus observed/confirmed behavior.

The manager holds a generation-numbered ownership map for screen power,
brightness, motion wake/sleep, idle timeout, screensaver/night and gestures.
During handoff all affected writes are blocked. Withdraw native retained
discovery and command routes together; plugin discovery is admitted only after
the manager commits ownership. Preserve stable identifiers for semantically
identical surfaces, or explicitly retire incompatible ones; never duplicate
wired loads, power, motion or diagnostics. A stale `kiosk_state=active` MQTT
message is not authority. Only the local helper's current lease/generation can
establish ownership. On restore, revoke plugin commands first, reconcile
retained state, then republish native discovery and resume native routes.

Privacy denial, shutter closure, mute, missing/stale evidence, IPC loss and
generation changes revoke relevant capabilities immediately. Fence capture,
queued work, executor results, subprocesses, history and derived data with one
authorization epoch; recheck at every provider send, redirect, actual display
writer and publication. Regrant starts a new epoch, never revives old data.
Stop capture and clear sensitive retained state on denial. A plugin that
cannot demonstrate these checks is not eligible for microphone/camera access.

Epoch checks are also enforced outside the plugin. Only a trusted, dedicated
capture-worker cgroup may open raw microphone/camera devices; the general
plugin receives bounded streams through the consent broker, never device fds.
On denial/mute/shutter, stale evidence or IPC loss, the independent trusted
supervisor closes capture streams, kills/drains capture workers and the
sensitive-mode plugin cgroup, verifies descriptor/process teardown, and closes
provider/egress sessions at the trusted proxy. Proposed deadline is one second
from observed denial; qualify the observation-to-stop bound on hardware too.
Do not rely on device-cgroup changes to revoke already-open descriptors or on
cooperative cancellation of blocked reads/sends. Regrant waits for proven
teardown and starts a fresh process/epoch; no old descriptor/session is reused.
Sensitive data is confined to disposable memory and epoch-scoped storage;
delete queued/raw/derived/history data within that deadline, or fence all access
and keep the capability denied with repair required until deletion is verified.
The privacy profile must cover malicious/blocked workers that ignore epochs;
if external cessation cannot be enforced, microphone/camera stay unavailable.

## 8. Runtime limits and net resource qualification

Every plugin has its own unprivileged UID, systemd unit and cgroup, separate
from the bridge, with `MemoryMax`, `CPUQuota`, `Nice`, `TasksMax`, finite file
descriptors, bounded temporary/data storage and `KillMode=control-group`.
Use `NoNewPrivileges`, empty capability sets, restricted devices, read-only
system/release trees and explicit writable paths. Reject unsupported sandbox
directives or unenforced cgroup limits; merely rendering a unit is not proof.
`CPUQuota=10%` means ten percent of one CPU, not ten percent of a two-core panel.
`Nice` is additional scheduling preference, not a resource guarantee.

Recommended provisional pilot ceilings (not measurements or exemptions):

| Budget | Manager services combined | One replacement plugin | Aggregate plugins |
|---|---:|---:|---:|
| MemoryMax | 64 MiB | 96 MiB | 128 MiB parent slice |
| CPUQuota | 5% | 10% | 15% parent slice |
| Nice | 10 | 10 | children inherit policy |
| TasksMax | 32 | 32 | 48 |
| Plugin-writable persistent data | journal/baseline reserve separately bounded | 64 MiB | 128 MiB |

Only one replacement is enabled during the pilot. Budgets include child
processes/workers and must also account for system services doing work on a
plugin's behalf. Immutable release sizes are bounded separately by catalog
limits and staging admission. Keep at least 256 MiB free `/var` after staging
and a 128 MiB available-memory floor; lower profile-specific headroom is never
inferred from stock savings. These proposed numbers require operator choice
and actual workload qualification. If kiosk/STT cannot fit, it stays unavailable
until a revised measured budget is reviewed. Do not raise caps automatically.

The manager total reserves independent ceilings for rescue (16 MiB, 1% CPU,
8 tasks), helper (24 MiB, 2%, 12 tasks) and HA broker (24 MiB, 2%, 12 tasks).
They never share the plugin slice or a helper/broker OOM failure domain with
rescue. Verify rescue can meet its deadline under maximum allowed plugin load;
otherwise reject the profile or review revised measured budgets.

Network grants name exact origins/protocols and listeners. Hostname-based
HTTPS/WSS grants require a trusted application proxy that enforces exact
scheme/host/port, TLS identity, DNS/rebinding checks, redirects, approved API
operations and per-epoch credential/session handling. Direct plugin egress is
default-deny; namespace/firewall rules permit only the proxy path. A namespace
alone cannot distinguish virtual hosts on the same IP/port or enforce HTTP
redirect/origin policy. Separately reviewed raw-IP protocol grants require a
specific transport adapter, identity policy and explicit consent; they cannot
be used to bypass HTTPS policy. Reject private or unexpected destinations
unless explicitly consented for local HA/STT/receiver use. Credentials bind to
exact origin. Proxy work is capped and included in manager resource accounting;
additional proxy/capture workers require an explicit sub-budget within the
manager total or a reviewed budget change before listing. Authenticated
receiver identity is required before personal media leaves for Cast; disabling
TLS verification is not authentication. No open Internet grant by default.

Resource acceptance uses paired, same-panel, same-firmware workload windows:

1. Stock plus unchanged bridge baseline; record target process tree/cgroup,
   panel memory/CPU/temperature, core bus traffic, response latency and startup.
2. Candidate after verified target quiescence; include helper, IPC overhead,
   all plugin children, decoder/indexer/network workers and downstream core
   cost. Measure idle, active, peak, startup and privacy-denied workloads.
3. For CPU time and comparable memory populations, report
    `net cost = plugin + all manager services + added core work - stock cost actually removed`.
   Do not subtract all Emperor children or double-count shared pages. Use
   process-tree PSS where possible and cgroup peak memory for hard containment;
   compare whole-panel memory to catch shared-page/accounting effects. Preserve
   peaks as well as means/p95. Report bus request/notification deltas separately.
4. Recommended acceptance: non-positive steady net CPU and memory, no new
   thermal/low-memory condition, no core restart/reconnect/error increase, no
   bus-traffic increase above the predeclared noise tolerance, and no p95
   wired-control response increase greater than 10% or 50 ms, whichever is
   stricter. Predeclare measurement accuracy/sample size; inadequate resolution
   or headroom is inconclusive. Never fabricate zero latency from an RPC ACK.

Use at least matched 30-minute windows and a one-day single-panel soak; record
absolute headroom and cgroup enforcement, not only favorable deltas. Stock
services already inactive earn no savings. For shared vassals, count only
independently measured removed work. Any net-positive exception needs a
separate operator decision with measured total headroom. Qualification of
physical responsiveness requires a separately authorized safe circuit and
offline HA test; this document authorizes no actuation.

## 9. HA registry, consent and migration

Extend `Component`/`REGISTRY` with a plugin descriptor and lifecycle adapter;
retain locked bridge behavior and existing core watchdog semantics. Voice and
Hue CA are currently default-off components, while watchdogs default on. Hue
CA is a trusted maintenance component, not a claimed stock replacement. New
feature plugins are default-off; desired selection is distinct from observed
installed/running state. A checked box is not evidence that activation worked.

The per-panel UI shows publisher/trust/review state, exact available/installed/
previous release identities, compatibility, stock function/target replaced,
capabilities/destinations/data retention, measured versus proposed budgets,
owner, health, firmware epoch, pending cleanup and recovery progress. Selection
opens a consent step with defaults denied for microphone, camera, remote media
and display takeover. Show loss of stock Alexa/art/UI explicitly. Refuse
incompatible firmware, conflicting exclusive owners, missing recovery evidence
and expanded permissions pending consent. Updates are explicit and pinned.

Actions are Install, Enable, Disable and restore stock, Update, Roll back,
Uninstall, and Purge retained data. Distinguish stock restored from prior
plugin restored. Uninstall confirms what local data is deleted and supplies
provider-token revocation steps. No success notification until local restoration
and critical cleanup are verified; outstanding broker cleanup appears as a
separate repair. Diagnostics expose bounded codes, versions/digests, counters
and phases; never raw env/journal/provider URLs/audio/photos/tokens.

Migrate existing voice incrementally:

1. Wrap its current `present/install/remove` path with authenticated version/
   digest/size validation including cached payloads, private staging and the
   common journal. Do not treat payload presence as verified ownership.
2. Inventory existing installs read-only and report `legacy_unmanaged` when
   there is no original stock baseline. Do not invent an enabled-Alexa baseline
   or silently stop it. No automatic reinstall/update/adoption on HA restart.
3. Offer explicit adoption only after compatibility, stock policy and privacy
   consent, fresh baseline capture and supported independent voice disable/
   restore. Until qualified, retain the legacy component path for existing
   users without claiming it meets the replacement contract; block migration
   and new replacement installs rather than weakening the bus protection.
4. Move adopted voice to its own capped service and exclusive microphone lease;
   preserve user config where safe, rotate secret delivery and keep rollback
   evidence. New voice must not keep Alexa concurrently active. Remove the
   legacy path only after offline and live migration/restore qualification.

## 10. Motivating plugins and current blockers

| Plugin | Intended replacement | Required before any activation |
|---|---|---|
| HA Kiosk (#237/#239) | Standalone `switch_ui_app.service`; screen/backlight/touch ownership only. Wired loads/power/motion remain core-owned. | Exact original states, watchdog/external-supervisor proof, one installer owner, command/discovery handoff, offline local control, resource qualification and real OTA. |
| Atmosphere art (#280) | Candidate `art` vassal and actual panel art/screensaver function. | Contributor must demonstrate equivalent replaced function and an independently safe firmware adapter; external-TV casting alone establishes neither. |
| Atmosphere voice (#280) | Candidate `voice` vassal / native Alexa pipeline. | Independent safe adapter, exclusive mic, fresh privacy evidence and no concurrent Alexa. Separate permission/config from art; Cast is an additional capability, not a stock service. |

Kiosk's current `message_bus.service.d/ha-kiosk-display.conf` uses bind mounts
to redirect hardware backlight writes and requires a bus restart. That is
ineligible under section 2 even though it does not mask the bus. It needs a
qualified semantic display-ownership mechanism that leaves the bus supervisor
and required hardware control intact, or a separately reviewed core change
before a plugin adapter can use it. A wrapper must not simply execute today's
root `ha-kiosk-setup install/reapply` and inherit that behavior. Use its proposed
versioned CLI/status/owner contract only after splitting out prohibited actions
and independently reviewing its root changes. Upstream implementation claims
remain evidence to verify against the exact pinned release.

The upstream installer reports original-state and watchdog gaps in its #283
declaration. Its later #239 note advances non-admin API and atomic-secret
evidence; do not repeat those as wholly unimplemented. TTS/media still needs
qualification; admin-only upload stays off. A real firmware OTA and survival of
the recovery hook are still required. Kiosk-only MQTT cannot reuse the bridge's
principal; brokered entities or a separate constrained identity are needed.

Atmosphere should separate art, conversational voice and optional Cast grants
in the manifest and runtime, so a scenic art mode needs no mic/backend key.
Provider/indexer/STT/Cast code runs outside the bridge and cannot block native
input acknowledgements. Retain activation/privacy generations through actual
worker lifetime; bound caches/backoff; confirm display state; authenticate Cast;
validate assets/redirects and credential origin. All HIGH/MED cases in the
linked adversarial review need regression coverage. No native peripheral
hosting path is accepted by v1. These constraints leave #280 on hold pending
its own rework/review; accepting this design does not accept its current diff.

## 11. Implementation phases and acceptance evidence

1. Agree this design and record the operator decisions below. Maintainers own
   the schema, helper and firmware profiles; contributors supply packaging,
   exact replacement declarations and evidence. No panel changes in this phase.
2. Implement the strict catalog/manifest validator, isolated staging, versioned
   journal extension, local helper and independent rescue, core-removal guard,
   scoped HA broker, UID/IPC boundary and HA consent behind a default-off
   feature flag. Use fakes for all stock and SSH operations.
3. Qualify one firmware adapter and local boot recovery before offering a
   replacement. Kiosk is the recommended first candidate only after resolving
   its protected-bus conflict; there is no unconditional approved pilot today.
4. Run an explicitly scheduled single-panel pilot, stock handback, failure
   drills, one-day soak and actual OTA with HA unavailable. Then qualify core
   upgrade/rollback and voice adoption. Keep firmware profiles narrow.
5. Expand the reviewed catalog only after evidence and support ownership;
   third-party catalogs/signing require a separate reviewed rollout. Reconsider
   Atmosphere after its replacement mapping and review findings are resolved.

Offline acceptance must cover every journal intent/completion crash point,
power/SSH/HA loss, lock contention, ownership conflict, masked/disabled/static/
missing stock targets, ambiguous probes, external respawn/watchdog behavior,
full disk, corrupt/missing baselines, changing topology and rollback failure.
Include tampered cached/bundled artifacts, hostile archives, signature/key
revocation, expanded capabilities, secret rotation/deletion, sandbox escape
attempts, resource enforcement, privacy revoke/regrant during blocked workers,
late display writes, replayed commands, stale leases and retained cleanup.
Prove protected units/paths never mutate even through aliases/dependencies.
Include helper exit/OOM/hang during handoff and after commit with HA offline;
independent rescue must meet the same-boot recovery deadline without reboot.
Core uninstall, HA entry deletion and legacy removal with active/staged/failed
plugins must preserve rescue state until handback. Test an ordinary reboot with
a runtime-only mask and an enabled-but-temporarily-stopped target. OTA tests
must cover a persistent operator deny, newly enabled vendor default, lost
`/etc` and changed target identity, asserting no transient device acquisition.
Delay a never-delivered unexpired command through A -> B -> rollback to A:
it must fail while a new-epoch command succeeds. With the example HA grant,
layout rendering must work while unrelated state, personal-media browsing and
unapproved services are denied over both REST and WebSocket.
Test privacy denial during blocked capture/send, including workers ignoring
cancellation; prove external termination, session closure and data deletion
before regrant. Cover minimum-safe-version increases without a digest
revocation, expired trust metadata and bad clock evidence for every activation
path. Network tests reject same-IP alternate Host/SNI, direct-egress bypass,
DNS rebinding and cross-origin redirects. Kiosk's own-wired semantic requests
must pass with HA unavailable while mesh/foreign/calibration requests fail.

Live evidence must identify exact firmware/profile/release digests, original
states, successful stock handback on failure and uninstall, measured net/peak
resources, core health/physical responsiveness, device exclusivity, absence of
reboot loops and actual OTA recovery. Publish sanitized verdicts only; private
recovery contents remain private. A simulated pass is not hardware evidence.
#283 stays open until implementation, offline coverage and a qualified pilot
meet its completion criteria; #237/#239 retain their specific checklists.

## 12. Operator decisions

Record choices on #283 before implementation/pilot; no response means no new
permission. These recommended defaults make subsequent PRs reviewable.

| Decision | Recommended default | Consequence / evidence needed |
|---|---|---|
| Catalog and trust owner | Reviewed integration catalog only; maintainer owns pins, revocations and incident response. | External authors permitted after review; no user-added catalogs or root hooks. Third-party keys/signing deferred until policy is implemented. |
| Allowed stock targets | Only independently qualified `switch_ui_app.service` initially; art/voice unavailable until independent vassal adapters exist. | Protected core never replaceable. Kiosk bus modification must be resolved first. Approve each firmware profile, not a wildcard target list. |
| Atmosphere scope | Separate art and voice permissions; optional Cast only as part of a proven replacement. | Contributor identifies the actual replaced function and net savings; #280 remains on hold. |
| Pilot budgets | Section 8 ceilings, non-positive steady net CPU/memory, no core regression; one plugin. | Measure before accepting a release. A larger cap or net-positive exception requires explicit measured justification. |
| Pilot owner/panel | Operator names an on-site recovery owner, maintainer and one non-Office pilot panel before scheduling. | No panel is selected or contacted by this design. Owner must be able to restore locally if recovery fails. |
| UI safety and offline behavior | Bus untouched; local control and stock fallback work without HA. | Approve a specific safe test circuit separately; qualify UI watchdog and display ownership first. |
| Consent and retention | Per-panel explicit grants; mic/camera/remote media off; transient media only. | Any cache retention or destination expansion requires a visible grant and bounded deletion policy. |
| HA access authority | Broker-held non-admin token; plugin sees only consented entities, layout fields and actions. | Upstream kiosk must support this mediated mode. Broad direct token access is ineligible under the example grants. |
| Updates and OTA | Manual pinned plugin updates; unknown firmware leaves plugin stopped and authentic stock restored where safely supported. | Recovery-hook proof is an installation gate; no auto-reenable after OTA. |
| Legacy voice | Explicit adoption only; no automatic stock-state assumption or new replacement install without a qualified adapter. | Choose migration timing after helper/bus boundary/voice restore tests. Existing legacy use is not qualification. |

Merging this documentation accepts a design for implementation review. It does
not deploy a plugin, select a pilot, close the linked issues or authorize merging
the motivating feature PRs.
