# Mesh write settlement with one owner per peripheral

Status: proposed; design review precedes implementation. Refs #225.

This design replaces the distributed mesh confirmation machinery proposed in
PR #232. That PR remains held. The implementation will be a separate PR from
current main, after both prescribed reviewers have reviewed this document.
This document changes no runtime behavior.

## Problem and preserved behavior

Native mesh acceptance is transport evidence, never proof of physical
actuation. A native request can outlive its five-second caller deadline while
retaining the adapter's device write lock. Main can interpret the pre-write
mirror as a contradiction, then miss late acceptance entirely. PR #232 added
settlement handling but left state ownership distributed across native return,
caller resumption, observations, reconnect, reconcile, timers and MQTT awaits.
Eleven confirmed findings over three rounds demonstrate those gaps.

Keep the existing wire vocabulary: `idle`, `pending`, `unconfirmed`,
`contradicted`, `failed`, `superseded`; pending requests hold primary state at
JSON null, with observed auxiliary values. Keep the 80-second confirmation
window and existing maximum matching-observation age. Matching observations do
not confirm early. Deadlines use monotonic time internally; their fixed UTC
wire representation is computed once per window. Brightness-only commands
track only their actual normalized targets.

Keep admission classes, latest-command replacement, the shared device write
lock, five-second caller deadline, 15-second hard cap, watchdog, scoped shared
polling, election, and standby scoping. No new native reads, retries, sockets,
poll loops, or additional mesh-leader polling are permitted. Native peer churn
and physical responsiveness remain separate live-validation work under #225.

## One serialized owner

Each mesh LIGHT/SWITCH peripheral has one `MeshSlot`, owned exclusively by a
`MeshSettlement` reducer. Its public `dispatch(event)` is synchronous, runs on
the adapter's asyncio loop, and performs no awaits, I/O, task cancellation
joins, or user callbacks. It reduces one event completely before another can
run. This is the actor's serialization point; it does not need an unbounded
async event queue or a task per observation. Effects are descriptions of work
for bridge-owned workers. Worker results return as events.

The bridge must not mutate slot fields, keep a second mesh pending record,
overlay caller-local observations, or infer outcomes from a completed waiter.
The only source for a mesh state document is `render(slot)`. `_devices` may
retain ordinary discovery metadata; mesh state rendering cannot read it as a
second authority. Derivations such as mesh motion remain observations and are
applied once before reduction without modifying the captured input.

Proposed boundaries:

| File | Responsibility |
| --- | --- |
| `mesh_settlement.py` (new) | Typed events, slot, pure synchronous transition logic, renderer and effect tokens. |
| `bus.py` | Loop-confined provenance allocation and immediate synchronous ingress for normalized captures, generation changes and native outcomes. Sole panel-library boundary. |
| `write_admission.py` | Ticket outcome replay and callbacks; identity remains separate from command replacement identity. |
| `model.py`, `protocols.py` | Immutable provenance and narrow synchronous ingress contracts; no panel types. |
| `bridge.py` | Slot lifecycle, command translation, timers, bounded publication worker and independent discovery/subscription repair. |
| `tests/test_mesh_settlement*.py` | Reducer permutations, production-adapter schedules, transport and boundedness contracts. |

Existing wired feedback, camera and privacy provenance consumers preserve
their contracts. New fields have explicit defaults where compatibility needs
them; provenance-less fakes are updated to emit typed captures. A production
mesh observation without valid provenance is rejected as evidence and cannot
silently become a fresh confirmation.

## Event envelope and ingress ordering

Every event carries `source`, `generation`, `sequence`, `captured_at` (monotonic),
and peripheral identity. Native events also carry the immutable command ID and
ticket identity. Effect events carry their immutable ownership token. Source
identifies both origin kind (read, push, native, caller, lifecycle, timer, MQTT)
and the adapter incarnation. Incarnation distinguishes replaced adapter objects;
generation distinguishes reconnects within one adapter. Neither wall time nor
native variable timestamps establish ordering.

The adapter allocates sequences from one monotonically increasing counter for
captures, native issue and native return boundaries within its incarnation.
Bridge-local events receive sequence numbers from the same loop-confined
allocator. Sequence is provenance, not a generic reject-all watermark: each
event class validates its own ticket/effect token, and only observations use
the slot's observation watermark. A delayed native failure must not be dropped
because a newer observation was already delivered.

Full reads reserve generation/sequence and monotonic read-start time before
awaiting the native read. On completion, immutable normalized captures retain
that boundary; they also carry a monotonic completion time for diagnostics.
Freshness conservatively uses read-start time. Thus a read begun before
acceptance or reconnect cannot turn into post-acceptance evidence just because
it completed later. Pushes freeze values and assign provenance synchronously
when the raw notification enters the adapter, before deferred callback drains.
Partial captures state which fields were actually present or removed; copied
mirror fields are not new matching evidence.

Add narrow synchronous sinks alongside the existing async bus callbacks. At
capture, deliver the immutable mesh event to the reducer before returning the
read batch or starting async fan-out. This includes reconcile and shared hot
poll reads. Async mesh callbacks may request publication/metadata repair but
must not reduce the same observation again. The sink registration has a scope
predicate so standby panels do not start normalizing mesh traffic unnecessarily.
An initial/acquisition reconcile installs the sink before its first read.

Native return calls `ticket.mark_settled(outcome)` synchronously immediately
after the awaited RPC completes, before releasing adapter ownership, waking
the caller, logging with a new await, or scheduling a done callback. Outcomes
include acceptance receipt and return provenance, rejection, native timeout,
or teardown cancellation. The callback dispatches directly to the owner;
registering after settlement synchronously replays the stored outcome exactly
once for that registration. Caller timeout is a separate event and cannot
erase a known outcome. Replaying an already reduced outcome is idempotent.

Reconnect first increments adapter generation and invokes synchronous lifecycle
sinks in the same loop turn, before spawning resubscription or async reconcile.
Timers also compare their token with the adapter's current incarnation and
generation, so a broken sink cannot make old evidence valid. Cross-thread
library hooks must marshal the whole operation onto the loop; callbacks from
an obsolete adapter session are rejected before bumping anything.

If a synchronous sink unexpectedly raises, catch and report it at the adapter
boundary and revoke that mesh bridge's active publication/confirmation lease.
Do not continue with apparently valid old evidence. Existing session recovery
can rebuild the bridge; this adds no native retry or polling.

## State, identities and invariants

A slot contains: active lifecycle lease, current incarnation/generation,
observed immutable device plus observation watermark and per-target evidence,
current command ID/ticket/targets, native outcome, acceptance fence, fixed
monotonic and wire deadlines, matching evidence, wire status, and revision.
It also contains publication debt and at most one in-flight effect identity.
No list of old commands, captures, outcomes or payloads accumulates.

Command ID, bus generation and publication revision are different counters.
Replacement increments command ID synchronously before yielding or issuing
the new write. Acquisition/withdrawal changes the lifecycle lease. Every state
change that affects rendering increments revision and replaces publication
debt with the latest immutable render. Timers carry lease, command ID,
generation and window ID; MQTT effects additionally carry revision and client
session. Equality checks use the complete token, never status equality alone.

Invariants:

1. Only the reducer changes mesh observations, evidence, request or feedback.
2. Only current-generation evidence strictly after the acceptance boundary can
   confirm an accepted request. Unknown acceptance never confirms.
3. An observation older than the accepted observation watermark cannot replace
   state, clear terminal feedback, or refresh evidence, even after retirement.
4. Matching freshness uses capture time; complete missing-target captures clear
   matching eligibility. A newer partial target contradiction also clears it.
5. A timer cannot extend its fixed deadline or publish directly. It dispatches
   expiry and rendering uses the resulting current slot.
6. MQTT completion changes only publication debt for its exact token. It cannot
   restore an old slot, settle a request, or clear a newer publication debt.
7. Withdrawal revokes ingress/effect ownership before awaiting cleanup. An
   ex-leader cannot enqueue a new shared state or terminal-clear publication.

## State machine

Internal phases are `QUIET`, `ISSUING`, `UNRESOLVED`, `ACCEPTED`, `TERMINAL`,
and `INACTIVE`. Wire status is a projection, so `TERMINAL` retains its specific
reason. Admission waiting is represented by `ISSUING` without an issue fence;
it adds no requested phase to the wire contract and creates no native retry.

| Event and guard | Transition and effects |
| --- | --- |
| Acquire, new lease | `QUIET`, clear all old request/evidence/deadlines; wire `idle`; require discovery/routes and current-state debt. Old command callbacks remain invalid. |
| Validated newest command, active lease | Increment command ID, retire old window, enter `ISSUING`; wire `superseded` if replacing tracked feedback, otherwise `idle`; no new await or feedback publish before normal native admission. |
| Native issue for current ticket | Record issue fence and actual targets; remain `ISSUING`. Admission waiting/supersession does not fabricate a native issue. |
| Caller timeout while native unresolved | `UNRESOLVED`; wire `pending`, primary unknown; arm original 80-second deadline from timeout, fixed once. Observations update auxiliary/current observed state but cannot settle this request. |
| Current native acceptance, before or after caller timeout | `ACCEPTED`; wire `pending`, primary unknown; record native-return fence/receipt; clear pre-acceptance matching evidence; arm a fresh 80-second window from actual return capture time, never callback delivery time. Replace any unresolved timer. |
| Caller timeout after known acceptance/rejection | Ignore; native outcome already owns the phase and receipt. |
| Current native rejection | `TERMINAL/failed`, clear request/deadlines/evidence, retire timer, render latest observation. Works after caller detaches and after unresolved expiry. |
| Native timeout or non-teardown cancellation without acceptance | Keep existing unresolved deadline if present; otherwise arm one original 80-second unknown-outcome window at outcome capture time. It cannot confirm without acceptance; expiry yields `unconfirmed`. |
| Teardown cancellation | Withdrawal semantics; never a failure publish from the old lease. |
| Eligible matching observation in `ACCEPTED`, captured strictly before deadline | Store latest observed fields and target evidence. Remain pending until expiry; no optimistic echo. Each target needs post-fence evidence, all current and fresh. |
| Eligible target contradiction in `ACCEPTED` | `TERMINAL/contradicted`, revoke matching evidence/timer and render latest native state, including when delivered at/after deadline. |
| Complete observation missing any required target in `ACCEPTED` | Clear matching eligibility, primary unknown where absent; remain pending until expiry. Missing is not an implicit OFF or a match. |
| Partial observation omitting a target in `ACCEPTED` | Preserve that target's evidence and its original age; absence does not refresh it. Explicit removal clears it. |
| Pre-acceptance or old-generation observation | Cannot affect request evidence; old-generation or out-of-order observations cannot replace current observed state. Current-generation pre-fence observations may update ordinary observed/auxiliary fields while primary remains held unknown. |
| Current expiry in `UNRESOLVED` | `TERMINAL/unconfirmed`, primary unknown, clear request/deadlines; retain ticket identity so a later current acceptance can start its fresh window or rejection can publish failed. |
| Current expiry in `ACCEPTED` | `QUIET/idle` with observed primary only if every requested target has fresh, post-fence, same-generation matching evidence captured strictly before deadline, and no newer contrary/missing evidence. Otherwise `TERMINAL/unconfirmed`, primary unknown. Clear window in either case. Never manufacture target values for an echo. |
| Current observation in `QUIET` or `TERMINAL` | Update observed state and return to idle after rendering the terminal transition; newer genuine native observations resume ordinary state. An unresolved expired ticket remains outcome-eligible until superseded, generation change or withdrawal. |
| Generation invalidation, any active phase | Immediately clear request, receipt, all confirmation evidence and window; revoke old command outcomes and effects; enter `TERMINAL/unconfirmed` with primary unknown. Fresh new-generation captures can resume ordinary idle state. Never reissue the write. |
| Replacement, superseded waiter, duplicate outcome, old timer or old effect | Only a validated new command replaces ownership. Other stale IDs/tokens are no-ops; they cannot arm a window or publish. |
| Withdraw/shutdown | `INACTIVE`, revoke lease/tickets/deadlines/debt synchronously; cancel and join bounded workers separately; publish no shared terminal clear/offline message. |

Ingress is synchronous at capture, so any contradictory capture already made
before expiry is reduced before the timer can run, even if async poll/push
delivery is blocked. At the exact deadline, if expiry runs first, a later
contradiction becomes the next observed state; if capture runs first, expiry
sees the contradiction. A capture strictly at/after the deadline cannot add
matching evidence or prolong the window. A delayed pre-deadline capture that
some nonproduction test explicitly delivers later is ordered by provenance;
it cannot undo a newer observation or revive a retired window.

## Publication ownership and broker limits

Each peripheral has one serialized publication lane: one in-flight publish
and one replaceable latest desired render. State, feedback, forced reconcile
and expiry all use this lane. No reconcile or change callback calls MQTT for
mesh state directly. A wakeup event is level-triggered; an event storm cannot
create tasks or queued payloads proportional to input volume.

The worker reads current debt and validates lease, adapter generation, client
session and revision immediately before the MQTT call, with no intervening
await. It renders from the current slot, never from the local device captured
before discovery/availability/subscription awaits. On return it dispatches an
ack event with the original token; an obsolete ack cannot clear newer debt or
update a second payload cache. On failure, keep only latest debt. Retry on the
existing change/reconcile/session-recovery triggers; do not add a tight retry
loop. Supersession changes desired debt without starting another publisher.

A single lane prevents an old pending publish from completing *after* a newer
terminal publish to the same topic: the newer publish waits its turn. Internal
expiry still occurs at the fixed monotonic deadline while MQTT is blocked.
When the blocked call returns, its ack is stale, and the worker sends the
latest terminal/current document, skipping all intermediate revisions.

Tokens cannot retract a PUBLISH already handed to a broker. An old in-flight
document can temporarily reach the broker before the latest one; wire deadline
validation and normal MQTT recovery bound what can be claimed, not a false
atomic delivery guarantee. Withdrawal cancels/joins the worker with the
existing bounded MQTT/session teardown; if transport cannot settle safely, the
old client session must be closed before starting a new lease on that client.
No canceled old worker may be left running beside a new lane. Election cannot
provide a distributed broker fence against an ex-leader's packet already in
flight; improving that protocol is outside this change.

## Routing repair is independent

Reconcile first inventories included peripherals and performs idempotent
discovery/command-route repair. Freshness rejection may discard only the
observation's evidence/state contribution, never skip discovery or subscription
installation. After each routing await, recheck lifecycle lease and current
scope. If still active, complete repair and request current owned publication.
If withdrawn, stop installation and clean any partial route registration.

Capture ingress happens before availability/discovery publishes, so a newer
push during a routing await already updates the slot. Reconcile never resumes
with a frozen old state document. Acquisition and reconnect follow this same
path; discovery failure retains ordinary repair debt for a later existing
reconcile. It does not undo a slot transition or hide a command subscription.

## Bounded work and panel safety

Per active peripheral retain one observed snapshot, a bounded target map
(validated native command fields), one ticket/outcome, one matching-evidence
map, one timer, one publisher and one latest payload. Synchronous ingress has
no backlog. Replacement cancels the previous timer; callback registries remove
finished ticket listeners and old lifecycle sinks. Removal drops slots and
workers after revocation/join; tombstones are bounded by active work rather
than every ID ever seen. Sequence counters consume constant object count.

Cost is O(fields in the admitted capture/command) per event; reconnect is
O(current slots) once per reconnect. Use existing normalized/frozen captures
rather than clone the whole home graph. No task per capture, no busy wakeups,
no extra clock reads on uninterested paths, and no mesh read on standbys.
Preserve `MemoryMax=96M`, `CPUQuota=20%`, and `Nice` from the deployment unit.
The recorded deployment acceptance budget is agent CPU delta <=2 percentage
points and bus CPU delta <=0.5; off-device tests cannot establish that live
budget. Resource and physical responsiveness validation remains outstanding.

## Exclusion of the eleven confirmed #232 findings

References are the three review comments on PR #232, ending in
`7115011259`, `7115376563` and `7115722723` respectively.

| Finding | Construction that excludes it | Required production-adapter schedule |
| --- | --- | --- |
| R1.1 delayed pre-acceptance capture cancels fresh window | Read-start provenance and native-return acceptance fence; only post-fence evidence settles. | Capture old ON, accept OFF, release delayed shared batch; remain pending/unknown. |
| R1.2 fresh contradiction lost before deferred settlement | Native return synchronously dispatches acceptance; no deferred ownership installation. | OFF return, ON push before waiter resumes; contradicted/ON. |
| R1.3 blocked MQTT delays expiry | Timer dispatches to reducer independently of publisher; one fixed deadline. | Block pending publication, advance past expiry without releasing MQTT; internal unconfirmed, then latest terminal delivered. |
| R2.1 old match survives reconnect while new source ignored | Synchronous generation invalidation clears match before resubscription; new generation admitted. | OFF match, reconnect, new ON; never confirm old OFF. |
| R2.2 deadline contradiction overwritten by older match | Capture reduces synchronously before async drains; contrary/missing evidence invalidates match; no target echo. | ON at deadline before/after timer; latest retained state ON, no later old OFF. |
| R2.3 delivery time refreshes old match | Immutable monotonic read-start/push-capture time and observation watermark. | Match captured t=1, delayed batch t=79, expiry t=80; unconfirmed. |
| R2.4 caller deadline loses known receipt/fence | Native outcome is stored/reduced before future completion; timeout cannot replace it. | Ticket settles before result future; caller timeout, old ON delivery; pending with correct acceptance fence. |
| R3.1 reconcile publishes stale local snapshot | Reconcile only wakes current-state publication lane after routing awaits. | OFF reconcile blocks in discovery, ON contradiction, release; final retained ON. |
| R3.2 reconnect invalidation waits for observations | Generation bump and sink dispatch are synchronous, timer checks live generation. | OFF match t=75, reconnect/resubscribe blocks through t=80; unknown, no idle/OFF. |
| R3.3 stale reconcile skips discovery/subscription | Routing repair never branches on capture freshness. | Acquire, availability blocks, newer push, release; discovery and set route installed. |
| R3.4 timely return loses push before waiter arm | Owner arms at native return; waiter has no arming responsibility. | Successful OFF, ON push before command waiter resumes; contradicted. |

## Implementation and validation sequence

1. Obtain both design reviews on the exact draft head and resolve design
   findings before runtime code. Record reviewer sessions and verdicts in S9's
   persistent ledger and notify the coordinator.
2. Create `fix/225-mesh-settlement-v2` fresh from current main. Port every
   regression from #232, including all three rounds' committed tests and
   external runtime repros, before implementation. Keep a manifest of original
   path/test ID/head and any seam adaptation. Port assertions, not the obsolete
   distributed pending architecture. Retain the old scenarios and substantive
   expectations; document deliberate safety-contract changes for review.
3. Replay the ported tests on unchanged main. Record semantic failures and
   baseline passes; import/API failures alone do not count as reproductions.
   Use production `RpcBusAdapter` + `Bridge` behind gated native and MQTT fakes.
   Preserve all existing tests. Add reducer permutations and partial/missing
   targets, expiry ties, reconnect before every await, native result before
   waiter, stale acks, replacement, acquisition/withdrawal, slot cleanup and
   event-storm bounds. Test real callback capture ingress, not only direct
   reducer calls.
4. Implement typed reducer/events, adapter synchronous ingress/outcome replay,
   bridge lifecycle/timers/publication lane and independent routing repair in
   small TDD steps. Remove the previous mesh mutator paths rather than layering
   a second owner on top. Update the public feedback/architecture references to
   describe actual late acceptance and conservative unknown-outcome behavior.
5. Run Python 3.10 root Ruff/format/strict mypy and focused tests for every
   changed source/test file, including existing admission, scheduler, wired,
   privacy and camera consumers affected by provenance. Rebuild mirrored agent
   payload with `scripts/build_payload.sh` and verify a repeat build has no diff;
   run payload parity and HA static/focused checks for the changed payload.
   Use uv and umask 022; full suites run in GitHub CI under gate rule v2.
6. Push the separate implementation PR, linking this reviewed design and
   marking it as replacing #232 with `Refs #225`. Report every implementation
   push to the coordinator. Both prescribed reviewers inspect the exact final
   head containing current origin/main, with no local full suites. Reproduce
   findings, comment confirmed HIGH/MED findings, fix/re-review for at most
   three implementation rounds, then report BLOCKED if still unresolved.
7. MERGE-READY requires both CLEAN, static/focused/payload gates green, full
   GitHub CI green, current main incorporated, no draft, and closing-keyword
   audit of body and commits. Report to the coordinator; never merge. Only the
   coordinator merges and closes #232. Keep #225 open for live validation.
