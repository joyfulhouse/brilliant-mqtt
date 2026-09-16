# Proposal to diyHue: measure before adding event-driven HA state reflection

Refs #153. Addressed to the maintainers of the owning upstream project,
[github.com/diyhue/diyHue](https://github.com/diyhue/diyHue).

For a native Brilliant Hue client, displayed lag has two distinct waiting terms:
**diyHue state reflection and firmware-owned panel polling**. Event-driven
reflection can remove only the first. The panel's own polling cadence is
undocumented, so the achievable display benefit is currently unknown. The
recommendation is **measure first**, with tightening the existing diyHue poll as
the cheap comparator; an event-driven redesign is not the immediate action.

This is a documentation-only proposal for upstream consideration. It adds no
bridge runtime to brilliant-mqtt, no dependency, and no benchmark code. An upstream
implementation remains a separate qualification. Merging this documentation
claims neither a runtime fix, infrastructure recovery, nor closure of issue #153.
No synchronized physical slider qualification or latency measurement was performed,
and no physical slider latency fix is claimed.

## Evidence and uncertainty

The citations below refer to the brilliant-mqtt repository, not to diyHue source.
They establish the observed integration context and a local design precedent;
they do not establish an upstream implementation owner below the project level.

| Evidence | What it supports | What remains unknown |
|---|---|---|
| [docs/brilliant-panel/diyhue-bridge.md:162](diyhue-bridge.md#L162) | The integration record reports a diyHue **10 s state poll**. | This is not a documented minimum or a guarantee for every upstream version/configuration. Confirm the actual deployment before choosing a comparator. |
| [docs/brilliant-panel/diyhue-bridge.md:36](diyhue-bridge.md#L36) | The native panel is a Hue API v1 polling client. | Its polling interval, scheduling, and idle/active behavior are undocumented here. Changing diyHue reflection does not change that firmware-owned cadence. |
| [docs/brilliant-panel/diyhue-bridge.md:97](diyhue-bridge.md#L97) | A missing inclusion tag leaves the light unreachable in the documented integration. | This establishes **missing-tag unreachability only**. It does not establish a translation of HA `unavailable` into Hue `reachable`; that is a design decision proposed below. |
| [docs/reference/poc-findings.md:238](../reference/poc-findings.md#L238) | A **Brilliant bus** notification stream stopped delivering while its notification-fed mirror became stale. | This is a transferable caution about silent divergence, **not verified evidence of a diyHue or HA WebSocket failure**. It does not establish an upstream failure rate or timing. |

Only the diyHue 10 s state poll is repo-corroborated timing in this reflection
path. There is no repository-backed claim here about an activity-dependent wait,
an idle synchronization period, or immediate command forwarding. Those behaviors
remain unmeasured and require their own traces. A claim of worthwhile display
gain is not yet falsifiable without a measured panel-poll term, a baseline, and
a declared acceptance criterion.

### Live local design precedent, retired deployment

[src/brilliant_ha_mirror/ha_client.py:226](../../src/brilliant_ha_mirror/ha_client.py#L226)
contains authenticated HA event subscription, and
[src/brilliant_ha_mirror/ha_client.py:275](../../src/brilliant_ha_mirror/ha_client.py#L275)
contains registry/state reads. This is **live, mypy-covered source code**, not dead
or retired code: the package remains in the wheel
([pyproject.toml:74](../../pyproject.toml#L74)) and under the strict `src` mypy gate
([pyproject.toml:44](../../pyproject.toml#L44)); the narrow framework exception
names the hosting adapter, not the HA client
([pyproject.toml:59](../../pyproject.toml#L59)).

Retirement applies to the old **deployment** path: the guide forbids the old
manual deployment/direct-HA/physical-host instructions
([docs/ha-mirror.md:222](../ha-mirror.md#L222)), and the architecture documents
cleanup of those experiments
([docs/ARCHITECTURE.md:56](../ARCHITECTURE.md#L56)). This precedent is not a
recommendation to deploy that path or copy it into diyHue. It also does not already
satisfy the proposed contract: for example, its event parser ignores a removed
state ([src/brilliant_ha_mirror/ha_client.py:162](../../src/brilliant_ha_mirror/ha_client.py#L162)),
whereas this proposal requires an explicit tombstone.

## Conditional latency model and the cheaper comparator

Let `D` be the diyHue reflection polling period and `P` the firmware-owned native
panel polling period. **`P` is an explicit free parameter to be measured**, not
a repository fact. Assuming **independent uniform phases and negligible service
time**, mean displayed delay is `D/2 + P/2`. Transport, processing, and rendering
overheads are neglected in this model and must be measured in real qualification.
Ideal events set the modeled upstream waiting term to zero; they do not assert
zero real event-delivery cost.

Every delay cell is a **CONDITIONAL PREDICTION**, not a measured display gain.
The illustrative `P` values below are scenarios, not panel timing observations.

| Panel period, free parameter | 10 s polling | 2 s polling | Ideal events |
|---|---|---|---|
| P = 2 s | 6 s (CONDITIONAL PREDICTION) | 2 s (CONDITIONAL PREDICTION) | 1 s (CONDITIONAL PREDICTION) |
| P = 10 s | 10 s (CONDITIONAL PREDICTION) | 6 s (CONDITIONAL PREDICTION) | 5 s (CONDITIONAL PREDICTION) |
| P = 60 s | 35 s (CONDITIONAL PREDICTION) | 31 s (CONDITIONAL PREDICTION) | 30 s (CONDITIONAL PREDICTION) |

**At P = 60 s, the entire event-driven redesign moves the conditional mean from
35 s to 30 s.** It does not remove the firmware's remaining wait. Under these same
assumptions, tightening diyHue polling from 10 s to 2 s removes about **80% of the
mean upstream polling wait** (5 s to 1 s); ideal events save roughly **one further
second**. These are analytical consequences, not measured improvements or claims
that the proposed poll period is supported or safe for a particular deployment.

Measure `P` and both latency paths below before deciding whether that incremental
benefit justifies the event path's ordering, authentication, and recovery costs.
Compare the current poll with a separately approved shorter poll before proposing
a default change. A sweep of this formula would not discover `P` or add a distinct
falsifiable prediction, so this proposal deliberately supplies the table rather
than a synthetic benchmark implementation.

### Recorded dissent

The second independent review seat argued for a hard-capped, stdlib-only Python
3.10 deterministic sweep under tests: **fewer than 300 lines, no I/O, and no
reimplementation**, because the panel-poll term is unknown. This remains a live
minority view. The current ruling is documentation only; the reversal condition
is **measured traces exposing phase-locking, bursts, or tail behavior that a mean
calculation cannot represent**. Such evidence would justify upstream replay tests
later, rather than treating the analytical mean as sufficient for those behaviors.

## Proposed upstream contract, conditional on measured benefit

The smallest reviewable upstream change would be an additive, configuration-gated
HA state-subscription input into the **existing served Hue cache**, **off by
default**. Preserve the existing v1 REST surface and client compatibility. Keep
one served-cache write path: events and polls submit candidates to the same
per-entity writer, rather than becoming competing cache owners. This proposal
names **github.com/diyhue/diyHue as the owning project only**; this repository does
not establish a responsible upstream module, file, or class.

### Ordering and concurrency

Use an ordered mailbox/reducer per entity, coalescing a burst to its newest state
and preserving a **single-writer-per-entity invariant**. Value, availability,
membership, and deletion changes must participate in that ordering; a tombstone
cannot be discarded just because it carries no ordinary state value. Bound
pending work and treat overflow or ambiguous ordering as a resynchronization
condition, not permission to publish an arbitrary partial result.

Each session/reconcile has an epoch, and each entity update has a generation.
Record the epoch and relevant generation when an authoritative poll starts. A
result may commit only if those guards still permit it: a **stale in-flight poll
must never overwrite a newer event**, resurrect a tombstone, or restore an old
mapping. Fence results and callbacks from an earlier session even if cancellation
arrives late. Do not treat local receipt order across connections as authoritative
source order. The implementation must demonstrate a baseline/event cutover that
cannot replay older values; if ordering cannot be established, remain in resync
and use the shared bounded reconciliation path.

Polling repairs divergence; an event stream alone does not. Silent divergence
would make this change worse than the current polling design, even if the common
case updates faster.

### Full reconciliation on every reconnect

Every reconnect, including one following credential rotation or a reader restart,
must establish a **full authoritative baseline before resuming event mode**.
Never resume mid-stream under an assumption that HA retained missed events. Cover
all of inventory, inclusion, mappings, capabilities, values, availability, and
deletions; reconciling only values of already-known lights is insufficient.

Use jittered backoff and single-flight collapse of concurrent reconnect requests.
Reconnect, periodic, and watchdog reconciliation share in-flight reads and a retry
budget so a burst of clients or errors cannot multiply HA work. Upstream must
choose and record finite read/concurrency/retry and reconnect-storm bounds before
qualification; their safe values are unmeasured here. Exhausting a budget keeps
the system in bounded recovery rather than opening another independent loop.

Serve last-known cache state during resync through the unchanged v1 surface,
without claiming it is fresh or treating it as physical confirmation. A cache
miss must **never trigger an individual backing read**. Missing inventory is
handled by the shared full reconcile, not a per-client or per-entity read storm.
Buffer/coalesce new-epoch events during the baseline and apply them only through
the guarded cutover. A partial/failed baseline must not promote event mode or
interpret an incomplete inventory response as authoritative deletions.

### Unavailable is not removed

These are proposed upstream semantics, not a mapping verified in this repository:

| Authoritative input | Proposed served-cache behavior |
|---|---|
| HA entity becomes `unavailable` | Retain the Hue object and last-known value, but explicitly mark it unreachable. Do not synthesize `off` or present an unroutable target as available. |
| Entity is removed, including `new_state=null` | Commit an explicit tombstone/deletion through the ordered writer. Fence stale values and stop accepting commands to the removed mapping; do not leave a zombie light accepting unroutable commands. |
| Inclusion or mapping is removed by a complete authoritative reconcile | Remove the served membership/mapping explicitly, with the same tombstone and stale-result guards. Absence from a partial read is insufficient. |
| Entity becomes available again or is authoritatively re-created | Restore only the current authoritative value, availability, and mapping under the current epoch/generation. Do not replay a pre-removal cached command. |

Neither unavailability nor removal means `off`. Preserve v1 compatibility while
testing these transitions explicitly. The missing-tag evidence cited earlier
does not decide HA-unavailability translation; maintainers must review and
qualify this proposed behavior.

### Bounded polling remains permanent

Keep a slow **full-sync poll as a permanent floor**, even when events appear
healthy. Authoritative polling/reconciliation is the **only divergence repair**;
the event path accelerates reflection but cannot establish completeness. This
floor is not a transitional mechanism to remove after rollout.

On a dead-stream watchdog condition, tighten the bounded poll within the shared
HA read/retry budget and remain in polling/recovery mode. Resume event mode only
after a successful full reconcile. The watchdog must distinguish transport or
reader failure from ordinary inactivity: **a quiet stream proves nothing**, and
socket liveness alone does not prove complete state delivery. Periodic comparison
with authoritative state remains necessary even when no watchdog trips. Poll and
watchdog periods are upstream tuning decisions to be measured against HA load,
not invented defaults in this proposal.

### Authentication is a separate boundary

Authenticate the outbound HA subscription separately from inbound Hue access.
Do not reuse Hue client credentials as HA authorization or expose HA credentials
in served Hue objects, requests, logs, traces, or error payloads. Store HA access
material in the operator's supported secret store or restricted configuration,
with access limited to the upstream service. Preserve the existing inbound Hue
authentication contract and v1 client behavior.

Credential rotation must replace the outbound session, fence the old session's
callbacks/results with a new epoch, authenticate using the replacement material,
and complete the full baseline before enabling events. An authentication failure
must not bypass verification or erase the cache; use bounded retry/backoff and
the documented operator recovery path. Invalid credentials can prevent polling
as well, so flag-off does not by itself repair HA authorization.

Replay and ordering are handled by **baseline-on-reconnect plus epoch/generation
guards**, not by assuming event replay across sessions. Qualify rotation,
revocation, reconnect overlap, and stale callbacks independently from inbound Hue
authentication. No credentials or production endpoints belong in public evidence.

## Two separate benchmarks to define and run upstream

**Neither benchmark was run here.** They answer different questions and require
separate records. Use an approved synchronized time reference and report timestamp
uncertainty; record deployment/configuration, inventory, actual polling periods,
sample coverage, dropped/missing samples, and a bounded observation/recurrence
window agreed before execution. Keep raw identifiers and captures private.

| Benchmark | Start and end | What it establishes |
|---|---|---|
| **A: HA event -> diyHue cached-Hue-state reflection** | Start at the authoritative HA event timestamp for the selected change; also record arrival at diyHue to separate delivery from local processing. End when the matching mapped value and availability are committed to the cache actually served by v1 REST, not when a callback merely receives the event. | Upstream reflection latency, including event delivery and reducer/cache work. It does not establish panel display or physical actuation latency. |
| **B: HA change -> native panel display** | Start at the authoritative HA state-change timestamp. End at independently observed matching native panel display; record first matching and settled display separately, including any later regression. Correlate the cache commit and actual panel v1 reads when available. | **Only B is user-perceived display latency.** It includes the firmware-owned polling floor and real transport/rendering effects. A cache-only improvement cannot stand in for it. |

Measure `P` from authorized observations of the actual panel's repeated native v1
reads, across idle and active conditions, and correlate those reads with display
observations. Do not infer `P` from diyHue's poll setting, an HA event interval, or
a stale displayed value. If reads, display timing, or synchronization cannot be
observed, record that benchmark or term as **INCONCLUSIVE**, not a zero delay.

Compare the current measured configuration, a supported/approved shorter poll,
and only then an event candidate, **one change at a time** with matched workloads,
compatibility checks, exact baseline/rollback, and a declared recurrence window.
For the proposed 2 s comparator, HA load and client compatibility must be measured
before accepting it; that number is a hypothesis from the table, not a safe rate
assertion. Report distributions and failures rather than only a mean, and exercise
bursts, event/poll overlap, reconnection, inclusion changes, unavailability,
removal, and credential rotation in a separately approved environment.

Command dispatch, actual physical actuation, reflected/displayed state, and
mesh-confirmed state remain different evidence. **Never infer command latency from
a state polling interval or treat an acknowledgment as proof of actuation.** In
brilliant-mqtt, ordinary MQTT reflection is optimistic
([src/brilliant_mqtt/bridge.py:888](../../src/brilliant_mqtt/bridge.py#L888)); a mesh
RPC can be accepted without actuation
([src/brilliant_mqtt/bridge.py:687](../../src/brilliant_mqtt/bridge.py#L687)), and
mesh confirmation requires a fresh observation in a background policy
([src/brilliant_mqtt/bridge.py:758](../../src/brilliant_mqtt/bridge.py#L758)). Those
facts do not make the proposed Hue cache a physical confirmation source. Any
synchronized gesture-to-actuation/display trial needs separate approval; the
[response runbook](runbooks/wifi-response-qualification.md#phase-4-separately-approved-physical-slider-qualification)
describes that boundary.

## Qualification, abort criteria, and rollback

Before testing, upstream maintainers and the operator must record the permitted
HA load, read/retry/reconnect-storm bounds, client compatibility requirements,
display-benefit criterion, observation duration, and recurrence window. All remain
unmeasured here; without them, benefit/safety qualification is **INCONCLUSIVE**.

| Phase | Required evidence before proceeding | Rollback |
|---|---|---|
| Measure the existing deployment | Both benchmark definitions are executable; current `D`, observed `P`, workload, client behavior, and clock uncertainty are recorded. | Event flag stays off; no event candidate is deployed by this documentation. |
| Test the cheap poll comparator | Isolated shorter-poll result for A and B, HA load, client compatibility, and covered recurrence window. | Event flag remains off; restore the exact previous poll settings. |
| Qualify an additive upstream candidate | Prove reducer ordering, stale-poll fencing, full reconnect baseline, permanent repair polling, availability/deletion behavior, and separate authentication handling before opt-in. | **Flag-off at this phase**, fence/drain stale event work, and retain the existing polling/cache path. |
| Separately approved opt-in | A improves and B meets the predeclared display-benefit criterion without HA load/client regressions, divergence, or reconnect-bound breaches across the declared recurrence window. | **Flag-off at this phase**, fence the event epoch, restore baseline polling settings, and reconcile/verify the served state. |

Abort the event candidate on **event/poll divergence, reconnect-storm bound breach,
HA load regression, or absent display benefit**. Compare event and poll values at
a common guarded generation so an expected in-flight transition is not mislabeled
as divergence; unexplained disagreement is an abort, not a warning to ignore.
Flag-off is available at **every phase**. Because the candidate is additive and
removes no baseline polling path, software reversal is small, but rollback is
complete only when stale writers are fenced and authoritative served state and
client behavior are verified. Preserve failed-run evidence privately.

Issues #149/#150 own queued write policy, #151 owns requested/pending versus
confirmed state, and #152 owns bounded metrics in brilliant-mqtt. This proposal
rewrites none of those streams and defines no metrics surface for them. Before
merging the combined work, validate the documentation and shared semantics against
the sibling branches for all four issues. That check does not replace upstream
implementation qualification or the remaining operator measurements.
