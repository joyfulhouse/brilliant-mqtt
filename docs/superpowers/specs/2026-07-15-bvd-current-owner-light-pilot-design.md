# BVD Current-Owner Single-Light Pilot — Design

- **Date:** 2026-07-15
- **Status:** implemented conditional spike; live apply is **NO-GO** until the
  cross-owner cleanup, stock-service canary, approved external health/cloud/MQTT
  observer, pinned-firmware, and retired-mirror gates below are operational
- **Target panel:** Office, `10.100.0.10`, device
  `017ff60733f100038e04fa0fbab29096`
- **Target HA entity:** `light.backyard_light_group`
- **Evidence baseline:** firmware `v26.06.03.1`, OSTree
  `2174d3882504c03bf9c7b3f78f0cad4a5ae039f7a32f0bb9c5eca02dc0370b0b`,
  embedded stack `2332ced103d755d48d2302b592f95f8e7b6c66f5`, acquired from
  the designated pilot as recorded in the
  [evidence index](../../brilliant-panel/README.md) and
  [integrity record](../../brilliant-panel/acquisition.md#integrity-record)

## Decision

Build a one-light pilot on `brilliant_virtual_device` (BVD), but do **not**
take, refresh, clear, or hand back its lease. Apply mode is considered only
while Office is already the naturally elected BVD owner. Owner age at most 30
seconds is an admission heuristic, not proof that ownership will remain stable:
the observed election is a refreshed, health-scored race
([LeaseManager correction](../../claude/research/2026-07-06-mirror-poc/FINDINGS.md#correction-2026-07-06-subagent-lease-path-read-only--the-wall-was-a-method-mismatch)).

This is narrower than the initially proposed forced takeover. The forwarded
owner write is mechanically plausible, but cloud acceptance has never been
confirmed live, and the captured `LeaseManager` surface has no evidenced
release operation. Writing the recorded prior owner back would be another
undocumented ownership mutation, not a proven release. The earlier findings
already classify a BVD takeover as hazardous because ownership carries the
home-wide software host
([True slider mirroring](../../claude/research/2026-07-06-mirror-poc/FINDINGS.md#true-slider-mirroring--only-via-brilliant_virtual_devices-lease-hazardous)).

The pilot therefore contains no owner-write capability. If Office is not the
current BVD owner, it exits before MQTT connection or native host
start/registration. A separate exact-target cleanup-only path can run on
whichever panel is the current BVD owner, but that path is not proof that every
possible owner is reachable. Live apply remains NO-GO unless the orchestrator
stages and preflights cleanup on all candidate owners. This preserves the
cloud-acceptance question as an explicit live unknown rather than crossing an
irreversible gate.

## Why BVD, and why only in this mode

The other candidate devices are closed by live evidence:

- A raw peripheral record can render and be slider-assignable, but commands
  route to the managing host and the slider reverts
  ([FINDINGS.md](../../claude/research/2026-07-06-mirror-poc/FINDINGS.md#control-path-investigation-render-works-raw-injection-control-does-not)).
- Hosting on Office's physical Control made its real lights extremely
  unresponsive
  ([FINDINGS.md](../../claude/research/2026-07-06-mirror-poc/FINDINGS.md#live-framework-host-test-2026-07-06-later--mechanically-works-own-device-hosting-degrades-real-loads)).
- A new named virtual device has no cloud-seeded configuration/owner record,
  so there is nothing to own
  ([FINDINGS.md](../../claude/research/2026-07-06-mirror-poc/FINDINGS.md#corrected-verdict-split)).
- The Virtual Control self-bootstrap path is app-token gated and was rejected
  before device creation
  ([FINDINGS.md](../../claude/research/2026-07-06-mirror-poc/FINDINGS.md#virtual_control-live-provisioning-attempt--auth-proven-provisioning-wall-2026-07-09)).

BVD is the only existing non-Control device whose owner can legitimately host
an ordinary peripheral. Its observed peripherals are software-only, and raw
registration from its owner propagated home-wide
([FINDINGS.md](../../claude/research/2026-07-06-mirror-poc/FINDINGS.md#experiments-ab-post-no-go-operator-approved-panel-owned-registration-works)).

Three approaches were considered:

1. **Natural-current-owner attachment (chosen).** No ownership transition and
   no release problem; availability is nondeterministic because Office must
   already own BVD.
2. **Forced forwarded bid.** Deterministic scheduling, but cloud acceptance,
   stock-host handoff, and release are unproven; rejected as non-reversible.
3. **Render-only raw record.** Reversible visibility test, but it cannot satisfy
   control and is known to revert; rejected for this use case.

## Home-wide BVD hosting obligation

BVD ownership covers one stock process with five configured software
startables: `device_groups_configuration`, `request_dispatcher`,
`solar_peripheral`, `thirdparty_discovery_peripheral`, and
`weather_peripheral`. `remote_bridge` is the sixth observed BVD peripheral
([baseline snapshot](../../claude/research/2026-07-06-mirror-poc/out/baseline.json)).
`request_dispatcher` participates in device-group control, so these services
must not be treated as cosmetic.

The pilot does not replace or proxy them. It relies on the unmodified stock
`brilliant_virtual_device_peripherals` vassal that is already serving them on
the naturally elected owner. Before registering the light it requires:

- BVD configuration owner and BVD `remote_bridge.relay_device` both equal the
  exact Office device ID;
- an owner-variable age of at most 30 seconds, used only immediately before
  registration;
- BVD DeviceType 3;
- the stock BVD vassal PID/start identity to be present locally;
- exactly the five non-empty `process_config:*` entries named by the stock
  process configuration;
- exactly the six known built-ins, all ONLINE;
- all five configured peripherals to point to
  `brilliant_virtual_device_configuration`; and
- the pilot peripheral to be absent.

There are three distinct point-in-time invariants. PRE is the exact six stock
peripherals and no pilot; it is read once during initial admission and again
immediately before the first mutation. ACTIVE is the same six plus exactly the
stable pilot LIGHT; the host must first produce a new bus notification, then a
bounded snapshot of the subscribed observer mirror must validate ACTIVE before
READY. Each further ACTIVE mirror snapshot is scheduled five seconds after the
previous one completes (an observation gap of at most eight seconds with its
three-second bound) and is admissible only while bus notifications remain less
than 30 seconds old. POST is a newly opened peer's initial snapshot after
deletion, host/guard shutdown, and both independent absence reads; it must show
the exact six and no pilot before `STOPPED_CLEAN`. The operator then repeats a
separate dry-run POST check.

ACTIVE and POST also require the baseline stock-vassal PID/start identity and a
non-regressing owner timestamp; they do not reuse the 30-second admission age
as a lease guarantee. Owner/relay change, stock-host restart, missing/offline
built-in, stale notification stream, bus reconnect, or probe failure fences
commands and starts teardown. Ownership drift also makes POST fail, so it
cannot be reported as pristine merely because exact-target absence succeeded.
The firmware process configuration assigns the BVD process elevated priority
specifically because `request_dispatcher` handles responsive device-group intensity
([process_configs.py](../../../artifacts/brilliant-panel/v26.06.03.1/extracted/data/switch-embedded/env/lib/python3.10/site-packages/configs/process_configs.py#L581)).
Because there is no owner change initiated by the pilot, the stock home-wide
hosting obligation remains where Brilliant's own election placed it; a
functional stock-service canary is still required because ONLINE records alone
cannot exclude routing contention. `--stock-canary-approved` is only a CLI
assertion of that prior operator decision; the operator and a separately
approved observer perform and judge the canary.
`--external-observer-approved` separately attests that this required
non-writing observer is armed and recording; the pilot does not implement it.

## Components and boundaries

`tools/brilliant_bvd/single_light_pilot.py` contains the off-panel safety,
controller, and lifecycle core. `tools/brilliant_bvd/live.py` contains deferred
panel-only adapters and the CLI.

- `PilotConfig` hard-codes the Office device and HA entity boundaries and
  accepts only the room-assignment reference, display label, and a 60–120
  second active duration. The live bus verifies that the room reference exists
  on both sides of the final PRE snapshot and rejects a bus notification during
  that admission sandwich.
- `BvdTopology` and pure validators normalize only the owner/config/BVD facts
  needed by the guard. `snapshot()` reads only the RPCObserver's subscribed
  mirror and brackets the BVD device read with two configuration reads to reject
  an owner/configuration change during collection. Subscription notification
  age and reconnect aborts bound mirror liveness; the code does not claim that
  these reads are direct RPCs to an independently authoritative source.
- `BvdBus` is a read-only Protocol. The live adapter uses
  `brilliant_mqtt.bus.load_rpc_observer_class()` and exposes no owner-variable
  write method. It shares one persistent observer for preflight/monitoring and
  opens independent scoped observers for cleanup proof; after those close, the
  runner opens a separate full read-only observer for POST topology.
- `--owner-status` opens a read-only peer and emits the local panel ID, a
  twice-read stable configuration owner, and equality boolean. It deliberately
  does not parse the BVD device/peripheral topology, so recovery discovery still
  works when that topology is the damaged surface. The runbook runs it
  sequentially across an exhaustive approved fleet list and requires one
  reachable owner; the output is a scoped observation, not a lease lock.
- `VirtualLightHost` is a Protocol. The live adapter borrows the proven
  `PeripheralHost`, `VariableSpec`, internal state-update, and timestamped
  deletion mechanics from
  [`brilliant_ha_mirror.hosting`](../../../src/brilliant_ha_mirror/hosting.py),
  while fixing the target to BVD and keeping stable peripheral identity
  separate from `display_name`. Each process also uses a unique host startable
  ID so a prior framework record cannot alias the fixed peripheral identity.
- The MQTT adapter requires the initial manifest entry and available state to
  arrive as retained replay before registration, then watches manifest, state,
  command result, and disconnect behavior. HA accepts only manifest-listed commands
  ([ha_control.py](../../../custom_components/brilliant_mqtt/ha_control.py#L531)).
- `PilotController` owns authority, sequence fencing, command de-duplication,
  result rejection, and state reflection. State unavailability clears authority
  immediately; the live supervisor treats that loss as an abort rather than
  leaving a read-only tile alive. A rejected result or 15-second confirmation
  timeout first re-applies the last cached authoritative HA values internally,
  then aborts into bounded deletion.
- `PilotLifecycle` owns partial-start-safe deletion and two independent absence
  observations. Its immutable BVD/peripheral identity survives host shutdown;
  the live runner follows it with the separate POST topology peer. Cleanup-only
  first reads the exact target and treats an already-absent target as
  idempotently clean before running its two independent absence probes.

The active supervisor rechecks stop, reconnect, MQTT-reader completion, command
callback failure, HA authority, and subscribed-notification age after every
awaited live operation and on a 250 ms tick. Topology reads remain separately
bounded and run every five seconds. This closes the interval between a failed
registration/update and the next periodic topology sample without adding a bus
peer.

The live CLI deliberately does not implement cloud-peer, process-resource,
physical-load, stock-canary, or human-visible latency observation. Live apply
therefore remains NO-GO until the orchestrator supplies a separately reviewed,
executable, non-writing observer and named human observers. The three apply
flags are attestations, not automation. The staged release is a read-only directory
created from `git archive <reviewed-commit>` bytes, includes the committed
aiomqtt/Paho vendor tree used by the pilot, and is never overlaid on the
installed agent. Stage verification asserts those imports resolve inside that
immutable release rather than the mutable installed-agent vendor directory.
The root-only release, recovery command, and private run evidence are staged
under persistent `/var`; `/data` belongs to the selected OSTree deployment and
can be replaced by OTA
([persistence map](../../brilliant-panel/var-persistence.md#why-var-matters)).
Only the process lock is kept in volatile `/run` and recovery securely
recreates it when absent.

The firmware observer contract is processor-start, connection wait, then
observer-start; every method is asynchronous except the cached owning-device
getter ([poc-findings.md](../../reference/poc-findings.md#2-connection-recipe-the-buspy-adapter-contract)).
No `tools.brilliant_bvd` module imports `lib.message_bus_api` directly; it uses
`brilliant_mqtt.bus.load_rpc_observer_class()`.

## Native light and data flow

The peripheral is type 27 with stable ID
`ha_bvd_<stable-uuid-without-dashes>`. It declares the exact single-light
schema used by the VC pilot: writable `on`/`intensity`, typed display/room
metadata, a 0..1000 intensity scale, dim limits, transition metadata, and the
BVD configuration link
([single-light pattern](../../../tools/brilliant_vc/single_light_pilot.py)).

Data flow:

1. HA's initial retained `state/<stable_id>` replay is validated into a buffer
   before host construction. Its `on`/`intensity` values seed registration and
   are re-applied internally after host start; each native reflection has a
   three-second bound and never uses the externally-settable path.
2. A physical slider push invokes the light's `push_func`.
3. `on` maps to `turn_on`/`turn_off`; intensity 0..1000 maps round-half-up to HA
   brightness 0..255.
4. The controller allows one in-flight command per observed HA sequence and
   coalesces a slider burst to the latest value; otherwise later burst values
   would be rejected as stale by HA's sequence gate
   ([ha_control.py](../../../custom_components/brilliant_mqtt/ha_control.py#L535)).
5. The pilot publishes a non-retained v1 command carrying the currently
   observed HA sequence.
6. The native framework may expose the requested value when `push_func` returns
   after its bounded MQTT publish; that provisional framework behavior is not
   treated as HA authority. HA validates the sequence and performs the service
   call. A subsequent confirmed HA state publication overwrites the native
   variables with authoritative values. A rejected result or confirmation
   timeout restores the cached HA values before aborting. State unavailability
   or an epoch/sequence conflict also aborts. The result topic is diagnostic,
   never authority. Whether this round trip remains visibly settled without
   snap-back on the physical slider is a live unknown and pass criterion.

The MQTT contract and sequence enforcement are implemented in
[`ha_control_protocol.py`](../../../src/brilliant_mqtt/ha_control_protocol.py)
and the HA control plane commits a sequence only after retained state publish
([HA control plane](../../../custom_components/brilliant_mqtt/ha_control.py)).

At the controller boundary, an identical retained replay after an explicit
transport fence re-applies the complete native state before reopening commands.
The bounded live runner does not reconnect in place: MQTT stream loss enters
cleanup. The replay rule still prevents a future transport adapter from leaving
an HA-on light displayed off after a fence.

## Bounded lifecycle and rollback

- Apply requires the pinned firmware/evidence build, root, a fixed-path
  single-process mode-0600 lock under a root-owned mode-0700 directory, exact
  Office bus identity, fresh natural BVD ownership, healthy stock BVD services,
  initial retained manifest/state authority, an immutable committed cleanup
  release on every possible owner, an approved reversible stock canary, and an
  approved executable external observer. The retired
  `brilliant-ha-mirror` unit/process must be inactive or absent both before and
  after the run, per its [retirement contract](../../ha-mirror.md#inspect-service-retirement).
- The mutation timer starts before `host.start()` and is at most 120 seconds.
  Native state reflections and active snapshots have three-second bounds.
  Lifecycle deletion, shutdown, probe construction/read/close, and fencing are
  individually bounded; the two proof reads have one fixed 30-second interval.
  A fresh POST peer then uses the bounded native bus operations. The worst
  successful sequential timeout budget is about 160 seconds: 60 for
  fence/lifecycle/proofs including the fixed interval, then 50 to open and
  subscribe the POST peer, 30 for its double-owner/device reads, and 20 to shut
  down its two components. The runbook uses a 180-second operational reserve;
  the extra 20 seconds is scheduling margin, not an aggregate timeout enforced
  by the code. None of this turns the approximately five-minute refreshed
  election into an ownership guarantee.
- `SIGHUP`, `SIGINT`, `SIGTERM`, active timeout, MQTT/HA authority loss, bus reconnect,
  stale BVD notifications, BVD invariant failure, or command-path exception all
  enter the same cancellation-shielded cleanup.
- Cleanup fences commands, calls native `delete_peripheral` with explicit wall
  clock milliseconds, shuts down the extra host and persistent guard, then
  performs two independently opened-and-closed scoped absence reads 30 seconds
  apart. A new POST peer must then validate baseline owner/relay, stock-host
  continuity, five configs, and exactly six ONLINE stock peripherals before a
  clean result. The separate `--cleanup-only` command uses the exact fixed
  BVD/pilot IDs and refuses to run unless the local panel is the current BVD
  owner; it proves exact-target absence but does not replace the full POST
  topology acceptance gate.
- No owner variable is written on startup or exit. There is therefore no pilot
  lease to release and no guessed hand-back.
- Slider binding is changed only by the operator in the native UI. The existing
  read-only slider-binding tool captures and later byte-compares the baseline.
- Only after full POST, slider, retired-mirror, external-observer, stock-canary,
  and physical acceptance does the runbook verify no pilot process/lock holder
  and remove the exact immutable release, private run directory, and lock file
  from every candidate. The on-panel MQTT password is unlinked immediately
  after READY, or after a pre-READY exit, and its dedicated broker credential
  is revoked off-panel after teardown. A failed run retains recovery code and
  evidence, never the credential file.

If ownership changes before deletion, immediate pristine cleanup is no longer
provable from Office. Registrations persist after the creating process exits
([Control path investigation](../../claude/research/2026-07-06-mirror-poc/FINDINGS.md#control-path-investigation-render-works-raw-injection-control-does-not)).
That is a hard failed outcome: leave the old mirror inactive, do not retry
registration, identify the current BVD owner read-only, and run only the staged
exact-target cleanup on that owner before any further experiment.

If the in-process cleanup is still alive after the runbook's 180-second reserve,
the external observer does not escalate automatically. The orchestrator first
verifies the recorded PID, apply command, and immutable-release environment,
then may stop that pilot process once and immediately repeat fleet owner
discovery plus owner-local cleanup. That fallback is a failed run, never a
successful POST, even if later absence is proven.

## Residual live unknowns

- Whether the cloud would accept a forced Office BVD bid remains untested and
  is deliberately not exercised.
- Whether a generic framework host can coexist with the stock BVD host and
  register this LIGHT.
- Whether the native slider routes to this host's `push_func`.
- Whether confirmed HA state prevents snap-back under a real gesture.
- Whether the room-assignment reference shape is accepted for this BVD light.
- Whether BVD ownership remains stable for the bounded window despite a fresh
  starting timestamp.
- Whether the direct cleanup RPC succeeds after an ownership transition; this
  is why owner-local cleanup staging is mandatory rather than relying on an
  Office-originated delete.

## Failure and abort criteria

The pilot itself aborts on bus reconnect, a 30-second-stale BVD notification
stream, owner/relay or exact-topology drift, stock-host identity change, native
callback failure, MQTT reader/manifest/state authority failure,
malformed/conflicting protocol data, or missing HA state confirmation for 15
seconds.

The separately approved executable observer must send one `SIGTERM` on
`message_bus` PID/start change, stock-vassal PID/start change, cloud-peer leaving
CONNECTED or its disconnect counter rising, peer timeout/rejection, UI restart,
pilot RSS above 100 MiB, or pilot CPU above 15% for five samples. Those resource
thresholds come from the existing monitor
([monitor.py](../../../tools/brilliant_vc/monitor.py)), but that monitor is not
used unchanged because its ten-second SIGKILL policy is shorter than cleanup.
The observer must also timestamp MQTT command/result/state messages; it may
claim command-to-result at most one second and command-to-confirmed-state at
most 1.5 seconds only from measured timestamps.

The operator sends `SIGTERM` on any real-load lag, stock BVD/device-group canary
failure, wrong slider target, snap-back, wrong HA action, or visibly missing
convergence. Numeric gesture-to-command or state-to-native latency is not
accepted from a person's stopwatch; it requires an approved instrumented
observer. If any required surface cannot be observed, apply is NO-GO rather
than “pass by missing data.” Failure to delete, prove absence twice, validate
fresh POST topology, restore the slider baseline, or keep the retired mirror
inactive is a failed, non-pristine outcome.

## Test strategy

Off-panel fakes cover:

- exact target/identity/duration guards and proof that no owner-write surface
  exists;
- read-only owner-status output, natural-owner freshness, and exact
  PRE/ACTIVE/POST topology validation;
- typed light schema and stable identity;
- on/off and round-half-up brightness command mapping;
- initial-retained-authority admission, bounded HA-authoritative echo,
  identical retained replay, authority-loss abort, stale epoch rejection,
  canonical result payloads with optional diagnostics, and rejected-result
  cached-state restoration plus abort;
- partial registration, timeout/cancellation, timestamped delete, independent
  two-read absence proof, fresh POST probing, and retry after delete/read
  failure; and
- every cleanup/abort ordering path.

The full root gate is `uv run ruff check && uv run ruff format --check && uv run
mypy --strict src tests tools && uv run pytest`.
