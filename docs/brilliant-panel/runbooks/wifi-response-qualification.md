# Wi-Fi and native response qualification

Refs #148. Use this runbook when a panel is online but a control responds slowly.
Attribute the delay to an observed path before changing infrastructure. This
workstream did **not** measure a synchronized physical slider gesture, and claims
**no slider latency fix**. Documentation delivery or merge is neither an
infrastructure recovery nor runtime qualification.

Start with the [validation runbook](../validation-runbook.md). Follow
[broadcast, ARP, and established MQTT qualification](broadcast-arp-vs-mqtt-diagnostics.md)
for the broadcast/neighbor-resolution branch; do not substitute MQTT availability
for that procedure. Its cited connection semantics apply here: healthy MQTT does
not imply healthy multicast or fresh ARP, and failed ICMP to a broker VIP does not
prove failed MQTT. A VIP commonly does not answer ICMP while its TCP service works.

Every sampling interval, experiment limit, and recurrence window below is a
chosen procedure budget, not a measured latency or universal RF threshold.
Baseline values and acceptable response limits are **unmeasured** until the
operator records the measurements below. Do not infer command latency from a
state-reflection delay, a polling interval, or MQTT availability.

## Scope, prerequisites, and hard stops

- Name the suspect, a working comparison device on the same segment, the affected
  control, and the intended physical load in private inventory. Record whether
  other controls on the same panel and owning bus device are affected, and whether
  the symptom is delayed actuation, delayed displayed state, or both.
- Use existing operator access, installed read-only diagnostics, and native UI
  inspection. Do not deploy an observer, increase logging, open extra bus peers,
  issue load commands, or alter bindings as part of an unapproved baseline.
- Confirm physical access and a known rollback path before any later approved
  intervention. No firmware update, mesh DFU, calibration, or concurrent tuning
  may overlap the experiment. Snapshot the exact relevant configuration privately.
- Stop on new physical-control lag, unexpected actuation, bus/UI restart, a
  disconnect storm, stranded client, lost management access, or uncertain restore.
  Stop on missing evidence required to make the next change safely.
- Keep raw radio identities, addresses, SSIDs, BSSIDs, MACs, native IDs, credentials,
  and packet/log contents private. Public records use synthetic role labels and
  measured durations/counts only. Do not paste environment files or binding blobs.

## Phase 1: agent and binding gates before RF attribution

First complete the agent-side gate in the
[broadcast/ARP runbook, Phase 1](broadcast-arp-vs-mqtt-diagnostics.md#phase-1-eliminate-agent-side-explanations-first).
It supplies the exact log strings and settings for hot diff-poll, stale-stream
rebuild, and **bus** reconnect-storm detection. Preserve that gate's `PASS`, `FAIL`,
or `INCONCLUSIVE`; a missing log is not a zero counter. Do not confuse bus
reconnect events with MQTT reconnects. The v0.10.2 / #146 changes concern HA-side
watchdog deployment, not resident Wi-Fi/RF detection
([CHANGELOG.md:10](../../../CHANGELOG.md#L10),
[custom_components/brilliant_mqtt/manager.py:1028](../../../custom_components/brilliant_mqtt/manager.py#L1028)).

### Native target binding gate

If **only one control is affected**, that pattern points first at a stale or
incorrect native target binding, not at RF. It is a prioritization clue, not proof;
a target-specific downstream failure remains possible.

Within a 5-minute read-only inspection window, compare the native UI's selected
target with the private intended-target inventory and an already approved fresh
native snapshot. Verify the current owning bus device, peripheral, availability,
and group membership resolve to the intended load. Check for stale, offline,
duplicated, or reassigned targets. Ownership matters: writes target the bus device
that owns the peripheral
([src/brilliant_mqtt/bus.py:979](../../../src/brilliant_mqtt/bus.py#L979)).
Compare an unaffected control's mapping without operating it. Record any inability
to obtain a fresh snapshot; a possibly frozen observer mirror cannot establish
current ownership by itself
([src/brilliant_mqtt/__main__.py:340](../../../src/brilliant_mqtt/__main__.py#L340)).

| Verdict | Binding gate |
|---|---|
| PASS | Native selection and a fresh approved snapshot agree with the intended current owner/target, and no stale mapping explains the symptom. Continue measurement. |
| FAIL | The control resolves to the wrong, stale, offline, duplicated, or unintended grouped target. Stop RF tuning and route a separately approved native-UI correction with exact baseline/restore evidence. |
| INCONCLUSIVE | Inventory, native selection, fresh ownership, or group resolution cannot be established within the window. Do not repair by guessing or writing raw binding configuration. |

### Separate queue wait, RPC acceptance, and observation

Writes serialize **per owning bus device**; reads remain independent. Queue wait
ends when the write acquires that device's lock; RPC latency starts there. The
normal DEBUG log formats are:

```text
set_variables(%s) acquired device lock after %.3fs queue wait
set_variables(%s) receipt: %s (rpc %.3fs, queue wait %.3fs)
```

Source: [src/brilliant_mqtt/bus.py:1047](../../../src/brilliant_mqtt/bus.py#L1047)
and [src/brilliant_mqtt/bus.py:1084](../../../src/brilliant_mqtt/bus.py#L1084).
Use timestamped, already available logs; redact target identities. When DEBUG
evidence is absent, record timing as unmeasured. Any temporary logging change
requires its own bounded operator approval and restoration of the original level.

The 5 s caller deadline begins after lock acquisition and **detaches** the RPC;
the RPC keeps running with its device lock held and its late outcome handled.
The write is unresolved, not lost. The 15 s hard cap latches a session rebuild
([src/brilliant_mqtt/bus.py:44](../../../src/brilliant_mqtt/bus.py#L44),
[src/brilliant_mqtt/bus.py:979](../../../src/brilliant_mqtt/bus.py#L979)).
Record late completion/failure separately; never send a replacement command simply
because the caller timed out. The detached completion log format is
`detached set_variables(%s) completed after %.1fs (queue wait %.3fs); receipt: %s`
([src/brilliant_mqtt/bus.py:1076](../../../src/brilliant_mqtt/bus.py#L1076)).

The fixed **80 s mesh confirmation wait is expected policy, not a symptom**.
It outlasts the measured false-ack/revert tail, runs in a background task off the
command lane, does not delay command dispatch, and is superseded by a newer
command ([src/brilliant_mqtt/bridge.py:49](../../../src/brilliant_mqtt/bridge.py#L49),
[src/brilliant_mqtt/bridge.py:699](../../../src/brilliant_mqtt/bridge.py#L699),
[src/brilliant_mqtt/bridge.py:758](../../../src/brilliant_mqtt/bridge.py#L758)).
Do not count that interval as Wi-Fi latency or shorten it as RF tuning.

## Phase 2: independent, timestamped baseline measurements

Declare a 10-minute baseline session, with the individual intervals below starting
at recorded offsets within it. Use matched observations from the suspect and
working comparison device under comparable association, traffic, and load. Record
AP/channel policy and direction privately. Record clock offset/uncertainty before
correlating sources; do not report cross-host elapsed time finer than that
uncertainty. A controller's sample age is part of the evidence.

| Measurement | Its observation interval | Method and limit of inference |
|---|---|---|
| Signal and link rates | Sample every 10 s for 5 minutes. | Read the existing AP/client diagnostic surface. Record signal units, transmit and receive rates separately, association/roaming changes, and sample age. Rates are link selections, not delivered application throughput. A station-wide average cannot isolate this panel. |
| Gateway TCP latency | One connect-only attempt every 30 s for 5 minutes, each with a 2 s timeout. | From the panel's own network path, measure TCP connect time to an approved, known listening gateway service. Use installed approved tooling, a monotonic timer, and no application request. Confirm that the same listener works for the comparison device. If there is no approved listener, mark this measurement `INCONCLUSIVE`; do not scan ports or substitute ICMP. |
| Broker TCP latency | One connect-only attempt every 30 s for 5 minutes, each with a 2 s timeout, offset from gateway probes so attempts do not overlap on a device. | Measure the actual configured broker TCP service from the same source path. Distinguish TCP connect from TLS/authentication and MQTT-session health. Label these diagnostic connections so they are not counted as reconnects of the established agent client. A refusal or timeout needs listener/policy evidence before it can be attributed to RF. |
| Retransmission/failure counters | Two timestamped reads exactly 60 s apart, within the baseline; record actual elapsed time. | Use the same AP/client or TCP socket counter, direction, units, and reset generation at both reads. Record radio retries, radio failures, and TCP retransmissions separately. A global host counter cannot identify which session lost data. Apply the delta rules below. |
| MQTT reconnect events | Observe continuously for the full 10 minutes. | Use existing broker records for the established agent client and timestamped agent logs. Count completed MQTT reconnects separately from bus reconnects, startup, diagnostic TCP connections, and HA consumer activity. The exact agent connect log format is `connected to MQTT broker %s:%s` ([src/brilliant_mqtt/mqttio.py:500](../../../src/brilliant_mqtt/mqttio.py#L500)); correlate it with session identity/reason privately. |
| Native peer-RPC timing | Observe existing, authorized traffic for 5 minutes, recording each write's queue wait and RPC duration separately. | Correlate by owning-device role, target role, and event order. Keep caller detachment and late outcome in the same record. No writes or missing DEBUG logs means unmeasured timing, not a fast path. RPC duration is native-path time, not an RF measurement. |

A laptop on the same SSID is not the panel's source path for a TCP latency
measurement. If safe installed tooling or access to that path is absent, record
`INCONCLUSIVE` rather than copying a laptop's result into the panel row. Keep
TCP timeouts as censored failures; do not average them as successful 2 s samples.
Do not capture application payloads or send MQTT commands for these measurements.

### Counter delta rule

For each cumulative counter record `(t0, C0)` and `(t1, C1)` from the **same**
counter instance, then calculate `delta = C1 - C0` over the recorded interval
`t1 - t0`. A single cumulative read is meaningless for current packet loss:
it includes unspecified earlier traffic over an unknown duration and has no
current denominator. A large lifetime counter alone says nothing about this
incident's rate.

If a reset, roam, wrap, counter identity change, negative delta, or missing read
occurs, the interval is `INCONCLUSIVE`. Do not silently clamp or stitch it.
Report the delta as its actual unit, such as retry attempts over the interval.
Only compute a ratio when the platform documents a matching denominator and both
timestamped denominator reads cover the same direction, population, and interval.
Even then label the ratio by its counter semantics: radio retries or TCP
retransmissions are not automatically end-to-end packet-loss percentages. With no
matching denominator, current packet-loss percentage remains unmeasured.

### Measurement verdicts and attribution

Before examining candidate-change results, record the baseline distribution,
sample completeness, timeout/event counts, and an operator-selected tolerance
for each measurement. These thresholds are local acceptance criteria, not a
repository claim about expected performance. Missing a predeclared tolerance
precludes claiming an improvement passed its target.

| Verdict | Apply independently to every measurement row |
|---|---|
| PASS | Complete comparable samples fall within the declared baseline/tolerance, with no correlated fault for that measurement. This does not clear another path. |
| FAIL | Valid samples show a reproducible regression or a fault against that row's declared criterion during the symptom. Record the measured path; do not infer its root cause from one counter. |
| INCONCLUSIVE | Missing source-path access, unsupported/stale counters, absent listener, inadequate timestamp correlation, insufficient samples, no native writes, reset counters, or uncontrolled comparison. Preserve these as gaps. |

| Candidate cause | Evidence needed to support attribution | What does not establish it |
|---|---|---|
| RF performance | Symptom-aligned signal/rate changes plus correctly scoped retry/failure deltas and latency degradation on the panel path, compared with the working control. A separately approved isolated RF change must reproduce the improvement before claiming recovery. | Low signal alone, a cumulative failure count, a successful association, MQTT `online`, or a slow native RPC by itself. |
| Broadcast delivery | The separate [broadcast/ARP runbook](broadcast-arp-vs-mqtt-diagnostics.md) demonstrates the relevant inbound delivery failure with its working same-segment control. Keep its ARP and established-session verdicts distinct. | Healthy unicast TCP, failed broker-VIP ICMP, or missing discovery application output without an eligible inbound capture. |
| Stale/incorrect native target binding | A single affected control's native mapping disagrees with the intended current target/owner, or resolves to a stale/offline target. Retain the Phase 1 binding evidence. | A shared network explanation inferred solely because the affected control is wireless. A healthy mapping also does not rule out downstream target failure. |
| Agent-side scheduling/queueing | The measured wait before acquiring the owning-device write lock increases while RPC time remains comparable, or independent dispatch timestamps show a scheduling delay. Review hot-poll/stale-stream/bus-storm evidence first. | Combining queue wait and RPC duration into one network latency, claiming reads use the write lock, or counting the expected mesh confirmation wait as dispatch delay. |

If queue wait is small but RPC duration is large, investigate native peer/target
processing before asserting RF. If gateway measurements remain comparable while
only broker TCP regresses, investigate the broker/service/routed path. These are
next investigations, not proven causes. Mixed evidence may support multiple
contributors; an exclusive cause remains `INCONCLUSIVE` without isolation.

## Phase 3: controlled infrastructure experiments

Proceed only with a separately approved operator action outside this repository,
after baseline and binding/agent evidence justify that action. Select **one change
at a time** from the table. Record exact before/after settings and all affected
clients privately. Check compatibility and recovery access **for every change**;
association alone is insufficient if normal client functions stop working.

| Experiment | Hold constant and record | Compatibility and rollback gate |
|---|---|---|
| AP placement | Move only the selected AP to an approved temporary position; preserve channel, width, power, rate policy, and comparable traffic. Record the position privately. | Check coverage and normal functions of the panel, comparison, and other affected clients. Restore the exact original placement/cabling if coverage or management access regresses. |
| Channel utilization | First observe existing utilization for the baseline window. Then approve either a channel change or removal of one known competing traffic source; do not combine them. Keep placement, power, width, and rate policy fixed. | Check channel support and regulatory constraints for every affected client. Restore the prior channel or traffic-source state; uncontrolled background-load changes make causal attribution `INCONCLUSIVE`. |
| Transmit power | Change only the selected radio's power setting; keep placement, channel, width, and rate policy fixed. Record both link directions. | Verify association, roaming, normal functions, and bidirectional response for affected clients. More AP power does not establish a healthy client return path. Restore the prior setting on any regression. |
| Minimum data rates | Change only the approved minimum/basic-rate policy after reviewing the entire affected client inventory; retain the exact prior set. | **No universal minimum rate is prescribed. Raising minimum rates can strand legacy clients.** Verify support before changing and association plus normal operation afterward for every affected client. Unknown compatibility is a hard stop. Restore the exact prior rates promptly on any loss; do not wait for recurrence monitoring. |

Use this bounded sequence for each approved experiment:

1. Record the Phase 2 baseline, declared per-measurement tolerances, client
   compatibility results, original settings, change approval, and rollback owner.
   If an affected client cannot be checked, stop with `INCONCLUSIVE` before changing.
2. Apply the single approved change. Allow a chosen maximum 5-minute stabilization
   window while checking client/management continuity. Instability at the end, or
   any hard stop sooner, is `FAIL` and triggers the approved rollback.
3. Repeat the complete 10-minute measurement session with the same individual
   intervals, sources, working control, and comparable workload. Keep actuation
   tests separately approved; do not create traffic/load bursts for sample count.
4. Watch for recurrence using existing passive telemetry for a declared 24 hours,
   including the original symptom trigger. Repeat the bounded baseline measurements
   at the end. This is an observation budget, not a measured recurrence period;
   if the trigger or a suspected longer cycle is absent, record `INCONCLUSIVE` and
   seek a separately bounded follow-up window before claiming durable recovery.
5. Restore the original baseline before testing a different variable unless the
   operator explicitly accepts the measured candidate as the new baseline. Record
   that decision and a fresh baseline; never attribute a combined sequence to one
   setting. Close each experiment's rollback/recurrence record first.

| Verdict | Experiment gate |
|---|---|
| PASS | The targeted measured path meets its predeclared improvement criterion, comparison and all compatibility checks pass, and the covered recurrence window includes the original trigger without recurrence. Limit the claim to those conditions. |
| FAIL | The candidate fails its criterion, the fault recurs, or any client/control regresses. Roll back and verify restoration. |
| INCONCLUSIVE | Uncontrolled changes, incomplete client checks, missing measurements, or absent trigger/recurrence coverage prevent attribution. Do not label an immediate improvement a recovery. |

## Phase 4: separately approved physical slider qualification

**This workstream did not measure a synchronized slider gesture. No slider latency
fix is claimed.** A later physical test needs a separate approval naming the slider,
safe load, intended binding, observers, tolerances, and restore procedure. The
[native slider E2E runbook](native-slider-e2e.md) supplies its existing transcript
and restoration gates for its specific Virtual Control scope; it is not blanket
approval to provision a Virtual Control or operate any native load here.

For an approved native-path test, use a chosen 5-minute maximum trial window with
one deliberate gesture and no competing commands. Correlate the gesture against
**both actual physical actuation and displayed state** using an approved synchronized
observer or common visible time reference. Record timestamp uncertainty. Capture
gesture start/end and define which is the timing origin before the trial; preserve
initial and settled actuation separately from first and settled displayed state.
Observe the requested command, RPC receipt, ordinary MQTT reflection, and any mesh
confirmation independently where those surfaces are available.

| Evidence | Meaning and acceptance limit |
|---|---|
| RPC receipt | Transport acceptance only; it does not prove physical actuation ([src/brilliant_mqtt/bus.py:993](../../../src/brilliant_mqtt/bus.py#L993), [src/brilliant_mqtt/bridge.py:687](../../../src/brilliant_mqtt/bridge.py#L687)). |
| Ordinary MQTT reflection / displayed state | Ordinary reflection is optimistic ([src/brilliant_mqtt/bridge.py:888](../../../src/brilliant_mqtt/bridge.py#L888)). A matching UI or reflected value is not independent physical evidence. Record its own gesture-to-display interval. |
| Mesh-confirmed state | Requires a fresh observation under the background confirmation policy ([src/brilliant_mqtt/bridge.py:758](../../../src/brilliant_mqtt/bridge.py#L758)). An early matching mirror is insufficient. Record confirmation separately from observed physical actuation. |
| Actual actuation | The approved physical observer records what the load did and when. Measure gesture-to-actuation directly; do not infer it from any of the other rows or from a polling cadence. |

`PASS` requires complete correlated gesture, actuation, and display evidence within
the separately declared tolerances, correct target behavior, and verified restore.
`FAIL` means observed wrong/missing actuation, display regression, or exceeded
tolerance during a complete valid trial. `INCONCLUSIVE` means missing synchronization,
physical/display observation, ambiguous command correlation, a superseding command,
or an incomplete confirmation window. Restore the load and any approved native-UI
binding change immediately after the trial; never write raw binding blobs.

## Rollback and sibling-stream boundaries

On an experiment failure, stop measurements and restore the exact pre-change
placement/settings through the approved infrastructure procedure. Verify all
affected clients, native controls, management access, and the original measured
paths. If restoration cannot be verified, stop further experiments and use the
named operator's physical recovery plan. Remove temporary collection processes
and restore any separately approved logging level. Preserve failed-run evidence
privately; do not describe a documentation merge as executing this recovery.

Issues **#149/#150 own queued write policy**, **#151 owns requested/pending versus
confirmed state**, and **#152 owns bounded metrics**. Queue timing and pending
state are inputs to qualification, not policies this runbook rewrites.
Existing diagnostics provide logs, fixed fleet counts, bounded categories, and
internal counters, not a general metrics endpoint or a defined cardinality budget
([custom_components/brilliant_mqtt/diagnostics.py:257](../../../custom_components/brilliant_mqtt/diagnostics.py#L257)).

There is no promised exported RF, TCP-latency, packet-loss, or scheduling metric
here. Use approved platform observations where available. If additional agent
evidence is needed, #152 owns the narrow hook to expose existing queue-wait/RPC
durations or timestamp dispatch relative to command receipt for a bounded sample.
This runbook specifies no metric endpoint, label surface, or cardinality design.
Missing hooks remain unmeasured; an absent scheduling metric is not proof of no
scheduling delay.

## Evidence record

```text
operator / approval / private inventory and evidence references:
version / firmware / effective non-secret settings / clocks and uncertainty:
symptom: actuation / displayed state / both
affected-control pattern / intended target / native binding verdict:
agent gate / bus reconnect events / queue wait / RPC duration / late outcome:
baseline UTC start/end / comparison role / workload / declared tolerances:
signal/rates: source, sample interval, count, age, values, verdict
gateway TCP: source path, listener validation, interval, times/timeouts, verdict
broker TCP: source path, listener validation, interval, times/timeouts, verdict
counters: scope/direction/units/generation, t0/C0, t1/C1, delta, verdict
counter denominator: matching reads or unavailable; no inferred loss percentage
MQTT reconnects: observation start/end, events/reasons, verdict
native timing: observation start/end, queue/RPC pairs, missing samples, verdict
broadcast/ARP branch: private record reference and separate path verdicts
attribution: RF / broadcast / native binding / scheduling / unresolved mixture
one approved change / baseline / client compatibility / rollback owner:
post-change matched measurements / actual observation duration:
recurrence window / original trigger coverage / result:
physical slider trial: not performed / separate approval reference
gesture / actuation / displayed state / receipt / reflection / confirmation times:
restored load, binding, infrastructure and logging / verification:
overall result: PASS / FAIL / INCONCLUSIVE
claim scope / unmeasured paths / remaining evidence needed:
```
