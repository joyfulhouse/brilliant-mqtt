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
window and existing 10-second maximum matching-observation age. Matching
observations do not confirm early. Deadlines use monotonic time internally; their fixed UTC
wire representation is computed once per window. Brightness-only commands
track only their actual normalized targets.

Keep admission classes, latest-command replacement, the shared device write
lock, five-second caller deadline, 15-second hard cap, watchdog, scoped shared
polling, election, and standby scoping. No new native reads, retries, sockets,
poll loops, or additional mesh-leader polling are permitted. Native peer churn
and physical responsiveness remain separate live-validation work under #225.

## One serialized owner

Each mesh peripheral with published state or an auxiliary route has one
`MeshSlot`, owned exclusively by a `MeshSettlement` reducer. Only LIGHT/SWITCH
primaries enter primary confirmation; other mesh loads retain their auxiliary
and derived-state behavior through the same owner. Its public `dispatch(event)`
is synchronous, runs on
the adapter's asyncio loop, and performs no awaits, I/O, task cancellation
joins, or user callbacks. It reduces one event completely before another can
run. This is the actor's serialization point; it does not need an unbounded
async event queue or a task per observation. Effects are descriptions of work
for bridge-owned workers. Worker results return as events.

The bridge must not mutate slot fields, keep a second mesh pending record,
overlay caller-local observations, or infer outcomes from a completed waiter.
The only source for a mesh state document is `render(slot)`. `_devices` may
retain ordinary discovery metadata; mesh state rendering cannot read it as a
second authority. Compatibility readers may receive a materialized view of
the owner's current native fields plus auxiliary projections, including their
timestamp-less values; that cache is never a source for reducing mesh state.
Admission of lifecycle, binding and per-field provenance
precedes every stateful derivation. Mesh motion history belongs to the slot;
only an admitted score field can update its last-hot time, using capture time.
Derived motion is rendered from accepted merged fields and hold history.
Reconnect, removal and withdrawal clear that history. A stale high-score read
between current low-score captures cannot turn the next render on.

Proposed boundaries:

| File | Responsibility |
| --- | --- |
| `mesh_settlement.py` (new) | Typed events, slot, pure synchronous transition logic, renderer and effect tokens. |
| `bus.py` | Loop-confined provenance allocation and immediate synchronous ingress for normalized captures, generation changes and native outcomes. Sole panel-library boundary. |
| `write_admission.py` | Ticket outcome replay and callbacks; identity remains separate from command replacement identity. |
| `mqttio.py` | Capture opaque owner references at local command ingress; retain them through queues/coalescing and reject cross-owner folds. |
| `model.py`, `protocols.py` | Immutable provenance and narrow synchronous ingress contracts; no panel types. |
| `bridge.py` | Slot lifecycle, command translation, timers, bounded publication worker and independent discovery/subscription repair. |
| `__main__.py` | Consume a latched mesh-fatal condition in the existing coordinator tick and rebuild the session with existing teardown/backoff. |
| `tests/test_mesh_settlement*.py` | Reducer permutations, production-adapter schedules, transport and boundedness contracts. |

Existing wired feedback, camera and privacy provenance consumers preserve
their contracts. New fields have explicit defaults where compatibility needs
them; provenance-less fakes are updated to emit typed captures. A production
mesh observation without valid provenance is rejected as evidence and cannot
silently become a fresh confirmation.

## Event envelope and ingress ordering

Use one owner object as the lifetime identity. Acquisition, retyping/removal or
source-generation replacement permanently revokes the old object; a current
binding gets a new one. The same reference follows a queued MQTT command,
admission ticket and effects. Historical references to a lease/binding token
mean this object, not additional stamp classes or independent epoch counters.
Its birth sequence and unknown-state barrier use the event envelope's existing
sequence. Keep one current-command ID alongside the existing ticket identity
for folds; use the originating timeout/return event sequence for a window,
plus one render revision for debt.
Do not build another general lease/permit framework.

Every event carries `source`, `generation`, `sequence`, `captured_at` (monotonic),
and peripheral identity. Native events also carry the immutable command ID and
ticket identity. Effect events carry their immutable ownership token. Source
identifies both origin kind (read, push, native, caller, lifecycle, timer, MQTT)
and the adapter incarnation. Origin kind is informational, not a separate
ordering domain: reads and pushes from one incarnation are comparable.
Incarnation distinguishes replaced adapter objects; generation distinguishes
reconnects within one adapter. Neither wall time nor native variable timestamps
establish ordering.

The adapter allocates sequences from one monotonically increasing counter for
captures, native issue and native return boundaries within its incarnation.
Bridge-local events receive sequence numbers from the same loop-confined
allocator. Sequence is provenance, not a generic reject-all watermark: each
event class validates its own ticket/effect token. Observations use per-field
watermarks, including versioned explicit removal and complete-read absence.
A delayed native failure must not be dropped because a newer observation was
already delivered.

Full reads reserve generation/sequence and monotonic read-start time before
awaiting the native read. On completion, immutable normalized captures retain
that boundary; they also carry a monotonic completion time for diagnostics.
Freshness conservatively uses read-start time. Thus a read begun before
acceptance or reconnect cannot turn into post-acceptance evidence just because
it completed later. Pushes freeze values and assign provenance synchronously
when the raw notification enters the adapter, before deferred callback drains.
Partial captures state which fields were actually reported or removed; copied
mirror fields are not new matching evidence. The observer must pass a frozen,
sanitized evidence mask from `modified_peripherals`/`modified_variables` and
explicit deletions alongside `updated_device`. Raw scalar values still come
from the frozen mirror, not unvalidated delta values. Missing, inconsistent or
unusable masks are evidence-neutral: they may supply display metadata and
auxiliary values, but cannot refresh a target, clear an unknown hold or prove
target absence. Do not infer a repeated target report from presence in the
whole mirror or from value equality/difference. Complete scoped reads remain
evidence for all returned fields and absence at their reserved boundary.

Merge fields independently. If complete read R(20) overlaps power-only push
P(21), R's `on` and P's `power` can both apply. If P also reports `on`, R's older
`on` cannot replace it. A newer disjoint field cannot advance another field's
watermark or erase its only contradiction. Missing-target invalidation uses
the target's watermark, not a peripheral-wide maximum sequence.

Add narrow synchronous sinks alongside the existing async bus callbacks. At
capture, deliver the frozen mesh event to the reducer before returning the
read batch or starting async fan-out. This includes reconcile and shared hot
poll reads. Scope before normalization; synchronously extract only eligible
primary/target and derivation fields from the existing primitive snapshots.
Defer whole-device `Variable` construction, rendering and encoding until the
publication worker needs the latest snapshot. A power-only or other-load
motion notification must not normalize/render all 40 mesh peripherals or
refresh unrelated target evidence. Async callbacks may request publication or
metadata repair but must not reduce the same capture twice. Standby sink scope
remains closed. Acquisition installs its sink before its first existing read.

Every acquisition and binding replacement creates a new owner with its event
sequence as a birth floor. Sink delivery checks source/generation and this
floor before slot creation, motion
history, inventory/routing contribution or field merging. Thus a read started
in lease A cannot finish into lease B on the same bus generation, even before
B's first read finishes. Metadata freshness and inventory deletion have their
own watermarks; new push metadata, not an older reconcile-local descriptor,
determines the current binding. The admitted binding-change event may seed its
new slot atomically from its own validated source snapshot; its floor excludes
all other work started before the change. It cannot seed request-confirmation
evidence from a retired command. Freshness rejection of field evidence still
does not suppress repair of a currently admitted binding.

Topology has a separate per-device creation floor. A reliable explicit removal,
complete inventory removal or binding change advances it; no older-started
read may create an absent slot or resurrect a removed binding. Existing slots
still merge unshadowed fields per field, so this floor cannot discard their
only contradiction. Explicit deletion events are delivered even when the raw
device has no remaining peripherals; partial or unreliable masks do not imply
inventory absence. This permits dropping per-ID tombstones after cleanup
without retaining every peripheral name ever seen. Reappearance requires a
newer admitted inventory/explicit creation event; stale async state callbacks
cannot create slots or routes.

Successful full-device reads also emit an immutable `Inventory` event with
read-start provenance, successful/complete scope and returned
peripheral IDs, including an empty inventory. It is delivered synchronously
even when no per-peripheral capture exists. A successful scoped
`get_peripheral() -> None` emits `PeripheralAbsent` at its read-start boundary.
A failed, timed-out, partial or out-of-scope read emits no absence inference.

Read result shapes have explicit semantics. A well-formed device with an empty
peripheral mapping is a successful complete empty inventory. Successful
`get_device("ble_mesh") -> None` is ambiguous parent disappearance: emit
`ScopeUnknown` at read start, invalidate unshadowed pre-boundary primary
validity/matching evidence, and retain bindings/routes for existing repair.
An incomplete object such as `peripherals=None` takes the same evidence-neutral
topology path with a bounded diagnostic, never an authoritative empty inventory.
Transport exception/timeout has no inventory/absence contribution. ScopeUnknown
does not replace newer explicit target evidence; it cannot cause a target echo
from older evidence. Test all four shapes through actual `get_all()`.

For each owned peripheral missing from a complete inventory, reduce versioned
absence for its fields and primary-confirmation evidence, subject to each
field's watermark. A newer presence report can keep the slot alive but cannot
prevent an older absence from clearing still-older unshadowed target evidence:
absence(20) then power-only presence(21) cannot retain an `on` match from 19.
A newer explicit `on` report, however, shadows the old `on` absence. Existence
metadata separately determines whether the binding itself is removed.

For an actually removed binding, retire its request/timer, revoke old effects
and routes, and render one bounded absence obligation: primary unknown, with
unconfirmed feedback if a request was being tracked. This active-leader
absence effect has its own token; it cannot echo the old target. After its
attempt succeeds and bounded route/config cleanup completes, drop the slot
and workers. Transport failure retains only this bounded cleanup debt until
existing recovery triggers; a noncooperative operation latches fatal recovery.
Reappearance replaces the absence token with a fresh binding; withdrawal
revokes it without any ex-leader state clear. The creation floor prevents old
reads from resurrecting the dropped slot. No additional read is introduced.

Native return calls `ticket.mark_settled(outcome)` synchronously immediately
after the awaited RPC completes, before releasing adapter ownership, waking
the caller, logging with a new await, or scheduling a done callback. Outcomes
include acceptance receipt and return provenance, rejection, native timeout,
or teardown cancellation. The callback dispatches directly to the owner;
registering after settlement synchronously replays the stored outcome exactly
once for that registration. Caller timeout is a separate event and cannot
erase a known outcome. Replaying an already reduced outcome is idempotent.

Reconnect fencing starts at entry to the registered processor hook. A small
lock-protected adapter session/source-generation cell validates the hook's
bound session and increments the live generation immediately, including on a
foreign thread. Shutdown/new-session replacement updates that same cell under the
lock; an old hook cannot invalidate its successor. This generation fence is the
reconnect linearization point. Same-loop hooks invalidate/reduce inline;
foreign-thread hooks marshal only the reducer/generation fan-out onto the loop.
No reducer or MQTT operation runs on the foreign thread. The live generation
counter doubles as the invalidating epoch: do not defer its increment until
the marshalled callback. The loop applies that already-published generation
and retires evidence before resubscription or async reconcile begins; repeated
queued hooks cannot move it backward. Capture/expiry/publication authorization
read the live counter rather than trusting only a reducer-local generation.
A timer already ready on the loop cannot authorize old confirmation after hook
entry while loop fan-out waits.
Test the actual registered callback, not only `_on_proc_reconnect` directly.

If a synchronous sink unexpectedly raises, catch and report it at the adapter
boundary, latch a mesh-fatal condition, and revoke publication, confirmation and
command admission immediately. The existing coordinator tick consumes the
latch and performs normal bounded session teardown/rebuild with its backoff,
even when bus, broker and leadership otherwise remain healthy. Do not wait for
another election, stale-stream timeout or observation to trigger recovery.
This adds no native retry or polling; test a one-shot sink failure followed by
recovery with unchanged leadership.

### Folded command admission

The MQTT reader captures the current owner reference for each mesh command
callback before its first deferred transport/lane queue. The owner already
contains its source incarnation/generation and binding; no separate ingress
permit is minted. A currently unowned mesh topic is rejected, not saved for
later route reuse. Preserve the reference through bounded queues and field
coalescing; different owners cannot merge or fold even at the same topic.
At dequeue, the bridge checks that original owner is still current/live before
translation and at admission. It cannot replace it with today's route owner.
Thus old intent A queued across same-topic remove/recreate or withdraw/acquire
cannot become a valid write under B. Already-authorized native work remains
subject to the existing nonpreemption/lock rule. Other command consumers keep
their existing behavior; the MQTT layer treats owner references as opaque.

A successful latest-command fold is the ownership-transfer boundary, not the
later re-entry of the MQTT lane into the bridge. The synchronous fold hook
validates the current lease/binding and translates the newest payload. If
`bus.try_supersede` succeeds, it dispatches replacement ownership, actual merged
targets and ticket/listener association in that same no-await call stack,
before a released device lock can issue the replacement. Failed folds transfer
nothing. Re-handling adopts this command ID/window; it cannot begin another
window or discard a contradiction that arrived before waiter resumption.

Retyping revokes the owner, so queued old intent is rejected rather than
retranslated against its successor. On a `TicketAdoptionError` while the same
owner remains live, revoke the old association/listener and tombstone its
waiting admission before installing a fresh ticket/command ID for current
targets. An already-issued stale write
retains its native lock and may finish; it cannot settle the fresh command.
Cancellation before issue tombstones admission without starting an unknown
window or publishing native failure. Withdrawal cancels waiting tickets before
joining cleanup. Test the production MQTT lane folding B into waiting A, B
issuing/returning and a real contradiction before B's re-handler resumes.

### Native issue authorization and detached callers

Every mesh primary, auxiliary and maintenance ticket retains that same owner
reference. Under the existing device lock, immediately before native issue,
require it to remain current/live and its generation to equal the adapter's.
A mesh issue-sink exception revokes the owner and prevents the RPC; logging then
proceeding is forbidden. Preserve other consumers' existing best-effort issue
notification behavior. This is an owner check, not a second permit object.

Prepare the loop-owned issue event without an await; then check the owner
and mark native work authorized under the same session/generation lock
used by the processor hook. No user callback runs while holding that thread
lock. A hook that wins first denies/tombstones the waiting admission before any
native call. Authorization that wins first is the native-issue linearization
point: it is now issued work, retains the device lock and cannot be retracted
by a later hook, even if transport bytes have not yet left. Call the RPC with
no intervening await or fallible mesh sink after this commit. If preparation
succeeds but the generation changes before commit, reduce pre-issue revocation
instead of sending. This closes the foreign-hook/lock-acquisition gap without
holding a thread lock across a native await.

An issued command worker cancellation is deliberately fail-closed: abandon its
intent, clear targets/deadline, and mark the load unconfirmed/null immediately.
The native RPC and device lock continue; keep only its completion listener.
Pre-return mirror values cannot release this unknown hold. Real native return
sets the existing unknown-state barrier to its return sequence; the next
trusted primary capture beyond it can resume ordinary state. Do not rearm or
confirm this abandoned command, and do not fabricate a native cancellation or
failure. A newer explicit command may replace it normally. This intentional
policy for an abandoned caller avoids another independently owned five-second
timer. It is a deliberate conservative trade-off: the user sees `unknown`
state until the next trusted primary capture after real native completion,
even if that native request ultimately succeeded. Ordinary caller timeout retains its existing five-second deadline and
late-acceptance full-window behavior; it is not abandonment. Test cancellation
at t=1, contrary capture t=6 and native success t=7: stay unconfirmed/unknown
through return, then recover from a genuinely later existing read/push.

## State, identities and invariants

### Auxiliary and maintenance projections

Interactive auxiliary and desired-state maintenance writes have typed
`AuxIssue`, `AuxAccepted`, `AuxRejected` and pre-/post-issue cancellation events
with lease, binding, ticket, actual field targets and issue/return provenance.
Their ownership is per auxiliary field, separate from the primary command ID.
Issue/acceptance is reduced at the same adapter boundaries as primary writes;
the caller's later `_echo_state` path only requests publication and cannot
mutate `_devices` or apply another projection.

On current successful return, install a timestamp-less optimistic auxiliary
projection only for actual validated auxiliary fields. A later eligible native
capture of a projected field replaces that field's projection, even if its
value repeats; a disjoint field does not. A field already newer than the return
fence cannot be overwritten by replay. Failure or cancellation creates no
projection. Superseded tickets, retyping, removal and withdrawal revoke the
relevant associations; at most one current projection/association per supported
auxiliary field is retained. Successful maintenance echoes use the same owner
and keep their existing desired-state retry/throttle policy.

Auxiliary projections are never native observations: they do not refresh
primary matching evidence, clear a primary unknown hold, change its request or
deadline, or create motion score history. Primary render still applies its
pending mask over the combined native-plus-auxiliary view. Preserve
`test_mesh_aux_write_still_echoes_optimistically`,
`test_aux_echo_during_pending_keeps_state_null`,
`test_mesh_aux_echo_is_unchanged` and maintenance echo regressions. Add a real
native auxiliary contradiction captured after return but before caller echo;
the caller cannot republish its older projection. No additional poll is needed.

### Owned state

A slot contains: active lifecycle lease, current incarnation/generation,
observed immutable fields plus per-field watermarks and per-target evidence,
current command ID/ticket/targets, native outcome, acceptance fence, fixed
monotonic and wire deadlines, matching evidence, wire status, and revision.
It also contains bounded publication debt and one in-flight effect identity.
No list of old commands, captures, outcomes or payloads accumulates.

Command ID, bus generation and publication revision are different counters.
Replacement increments command ID synchronously before yielding or issuing
the new write, including at a successful fold. Acquisition/withdrawal changes
a never-reused lifecycle identity; slot deletion/recreation cannot reset it.
Binding changes and terminal retirement also reserve capture floors. Every state
change that affects rendering increments revision and replaces ordinary
publication debt with the latest immutable render; terminal obligations follow
the separate bounded rules below. Timers carry lease, command ID,
generation and window ID; MQTT effects additionally carry revision and client
session. Equality checks use the complete token, never status equality alone.

Invariants:

1. Only the reducer changes mesh observations, evidence, request or feedback.
2. Only current-generation evidence strictly after the native-return boundary
   can confirm a settled request. An unresolved native outcome never confirms.
   Success uses its acceptance fence; a settled inconclusive outcome uses its
   return fence, preserving the existing observation-based timeout contract.
3. An observation field older than that field's accepted watermark cannot
   replace state or evidence. Unknown primary holds release only on explicit
   current-generation primary evidence beyond the retirement/acquisition floor.
4. Matching freshness uses capture time; complete missing-target captures clear
   matching eligibility. A newer partial target contradiction also clears it.
5. A timer cannot extend its fixed deadline or publish directly. It dispatches
   expiry and rendering uses the resulting current slot.
6. MQTT completion changes only publication debt for its exact token. It cannot
   restore an old slot, settle a request, or clear a newer publication debt.
7. Withdrawal revokes ingress/effect ownership before awaiting cleanup. An
   ex-leader cannot enqueue a new shared state or terminal-clear publication.

## State machine

Internal phases are `QUIET`, `ISSUING`, `UNRESOLVED`, `INCONCLUSIVE`, `ACCEPTED`,
`TERMINAL`, and `INACTIVE`. Wire status is a projection, so `TERMINAL` retains
its specific reason. Admission waiting is represented by `ISSUING` without an
issue fence; it adds no requested phase to the wire contract and creates no native retry.

| Event and guard | Transition and effects |
| --- | --- |
| Acquire, new lease | `QUIET`, clear all old observations, auxiliary projections, motion history, request/evidence/deadlines; wire `idle`, primary unknown until admitted current primary evidence. Reserve acquisition floor; require discovery/routes and current-state debt. Old command callbacks remain invalid. |
| Validated newest command, active lease | Increment command ID, retire old window, enter `ISSUING`; wire `superseded` if replacing tracked feedback, otherwise `idle`; no new await or feedback publish before normal native admission. |
| Native issue for current ticket | Record issue fence and actual targets; remain `ISSUING`. Admission waiting/supersession does not fabricate a native issue. |
| Caller timeout while native unresolved | `UNRESOLVED`; wire `pending`, primary unknown; arm original 80-second deadline from timeout, fixed once. Observations update auxiliary/current observed state but cannot settle this request. |
| Current non-abandoned native acceptance, before or after caller timeout | `ACCEPTED`; wire `pending`, primary unknown; record native-return fence/receipt; clear pre-acceptance matching evidence; arm a fresh 80-second window from actual return capture time, never callback delivery time. Replace any unresolved timer and all prior-window terminal/ordinary obligations, even for the same command ID; synchronously revoke its in-flight pending or terminal child. |
| Caller timeout after known acceptance/rejection | Ignore; native outcome already owns the phase and receipt. |
| Current non-abandoned native rejection | `TERMINAL/failed`, clear request/deadlines/evidence, retire timer, render latest observation. Works after ordinary caller timeout and after unresolved expiry. |
| Native timeout or non-teardown cancellation actually ends a non-abandoned native operation, without acceptance | Enter `INCONCLUSIVE`; keep existing unresolved deadline if present; otherwise arm the original 80-second window at outcome capture time. Record its native-return fence; clear pre-return evidence without rearming the deadline. Post-fence target contradiction yields `contradicted`. Fresh post-fence matches may confirm only at expiry, preserving #232's settled native-timeout observation contract; no acceptance receipt or physical-success claim is invented. |
| Cancellation/rejection of a never-issued waiting admission | Tombstone its ticket, revoke the association and return to quiet/superseded as appropriate; no native failure or unknown-outcome window. |
| Command worker canceled after issue, without withdrawal | Abandon the intent and publish unconfirmed/null; keep the native lock/completion listener. No new timeout handle, native-return fence or rearmed request. Real completion then a later trusted primary capture can release the hold. Ordinary five-second caller timeout is a different event. |
| Real native completion for abandoned intent | Set its unknown-state barrier to the return sequence only if that abandoned hold is still current. Keep unconfirmed/null until later trusted primary evidence; never rearm, confirm or replace a newer command. |
| Teardown cancellation | Withdrawal semantics; never a failure publish from the old lease. |
| Eligible matching observation in `ACCEPTED` or `INCONCLUSIVE`, captured strictly before deadline | Store latest observed fields and target evidence. Remain pending until expiry; no optimistic echo. Each target needs post-fence evidence, all current and fresh. |
| Eligible target contradiction in `ACCEPTED` or `INCONCLUSIVE` | `TERMINAL/contradicted`, revoke matching evidence/timer and render latest native state, including when delivered at/after deadline. |
| Complete observation missing any required target in `ACCEPTED` or `INCONCLUSIVE` | Clear matching eligibility, primary unknown where absent; remain pending until expiry. Missing is not an implicit OFF or a match. |
| Partial observation omitting a target in `ACCEPTED` or `INCONCLUSIVE` | Preserve that target's evidence and its original age; absence does not refresh it. Explicit removal clears it. |
| Pre-acceptance or old-generation observation | Cannot affect accepted-request evidence; old-generation or per-field out-of-order fields cannot replace current observed state. Current-generation pre-fence fields may update ordinary observed/auxiliary fields while primary remains held unknown. |
| Current expiry in `UNRESOLVED` | `TERMINAL/unconfirmed`, primary unknown, clear request/deadlines; revoke the old pending child. Retain unresolved ticket identity so a later current acceptance can start its fresh window or rejection can publish failed. |
| Current expiry in `ACCEPTED` or `INCONCLUSIVE` | `QUIET/idle` with observed primary only if every requested target has fresh, post-return-fence, same-generation matching evidence captured strictly before deadline, and no newer contrary/missing evidence. Otherwise `TERMINAL/unconfirmed`, primary unknown. Clear window and revoke its pending child in either case, including successful idle expiry. Never manufacture target values for an echo. A settled inconclusive ticket cannot acquire a second native outcome. |
| Current observation in `QUIET` or `TERMINAL` | Merge eligible fields independently. Releasing an unknown hold requires explicit primary/required-target evidence captured after retirement, in the active epoch/generation/lease. Auxiliary-only captures cannot unmask cached `on`. Terminal publication debt precedes later idle debt. An unresolved expired ticket remains outcome-eligible until superseded, generation/binding change or withdrawal. |
| Generation invalidation, any active phase | Immediately clear request, receipt, all confirmation evidence, primary validity, motion history and window; revoke old command outcomes and effects; reserve retirement floor; enter `TERMINAL/unconfirmed`, primary unknown. Fresh explicit new-generation primary evidence can resume ordinary idle state. Never reissue the write. |
| Peripheral removal or retyping/rebinding | Revoke old binding, ticket and effects, reserve new binding floor and update desired routing membership. Removal drops the old slot after cleanup; retyping starts from admitted current metadata. Delayed old reads cannot recreate it. |
| Successful complete inventory omits peripheral, or scoped lookup returns None | Reduce per-field absence at the read boundary; clear unshadowed target match/primary validity even without a device capture. If binding is truly absent, retire request/timer and old effects, publish only its owned unknown absence obligation, then remove after bounded routing/config cleanup. Failed/partial/out-of-scope reads are not absence. |
| Successful missing parent or incomplete parent object | `ScopeUnknown` invalidates unshadowed pre-boundary primary/matching eligibility; retain topology/routes, no destructive cleanup. A newer explicit field wins; transport failure has no absence contribution. |
| Auxiliary/maintenance issue/outcome | Update only current per-field association/projection as specified above; never transition the primary request, evidence or deadline. Caller echo has no state-changing transition. |
| Replacement, superseded waiter, duplicate outcome, old timer or old effect | Only a validated new command replaces ownership. Other stale IDs/tokens are no-ops; they cannot arm a window or publish. |
| Publication accepted/failed | Exact ordinary revision acknowledgment clears only its debt; exact current terminal-obligation acknowledgment releases its latch, leaving newer ordinary debt. Failure retains bounded debt for existing retry triggers. Neither result changes native outcome, evidence or deadline. |
| Routing/discovery completion | Commit only current desired binding/membership, or retain current repair debt; never mutate write evidence or cancel current ownership because an older repair completed. |
| Wrong incarnation/owner, pre-birth capture or stale hook | Ignore before slot creation, derivation, inventory contribution or effect admission. A valid field from an otherwise admitted capture still uses its own watermark. |
| Synchronous ingress failure or noncooperative effect past teardown bound | Revoke write/state-effect admission, retire confirmation ownership and latch session-fatal recovery. Coordinator rebuilds; healthy bus/MQTT/election cannot suppress recovery. |
| Withdraw/shutdown | `INACTIVE`, revoke lease/tickets/deadlines/debt synchronously; cancel and join bounded workers separately; publish no shared terminal clear/offline message. |

The original caller-timeout monotonic deadline remains available as bounded
outcome metadata after an unresolved window expires, even though its live timer
and wire deadline are cleared. A later inconclusive outcome cannot restart or
extend that expired window; it stays unconfirmed. Only actual late native
acceptance starts a new full window. Non-finite wall-clock input produces no
usable wire deadline and never changes monotonic expiry or proves completion.
At expiry reduction, matching age is measured against the actual monotonic
handling time, not a nominal deadline or delivery timestamp. Age exactly ten
seconds remains eligible; older evidence does not. A delayed timer cannot
publish a match that was fresh at t=80 but is stale when it finally runs.

Ingress is synchronous at capture, so any contradictory capture already made
before expiry is reduced before the timer can run, even if async poll/push
delivery is blocked. At the exact deadline, if expiry runs first, a later
contradiction becomes the next observed state; if capture runs first, expiry
sees the contradiction. A capture strictly at/after the deadline cannot add
matching evidence or prolong the window. A delayed pre-deadline capture that
some nonproduction test explicitly delivers later is ordered by provenance;
it cannot undo a newer field or revive a retired window. Expiry reserves a
retirement sequence and monotonic boundary: a read started at t=79 and returned
at t=81 cannot clear an unconfirmed/null hold established at t=80, even if its
sequence exceeds all prior *observations*. Its eligible auxiliary fields may
merge; a later genuine primary observation beyond that floor releases the hold.

## Publication ownership and broker limits

`PublishAccepted` means the existing `MqttClient.publish` call returned under
its unchanged QoS contract. It does not establish subscriber receipt or
physical actuation. This design does not change publish QoS or add a second
broker connection; effect acknowledgments are local ownership events.

Each peripheral has one serialized publication lane: one in-flight publish,
one current-command terminal obligation and one replaceable latest ordinary
render. State, feedback, forced reconcile and expiry all use this lane. No
reconcile or change callback calls MQTT for mesh state directly. A wakeup event
is level-triggered; an event storm cannot create tasks or queued payloads
proportional to input volume.

The worker reads current debt and validates lease, binding, live invalidation
epoch, adapter generation, client session and revision before the MQTT call,
with no intervening await. It renders from the current slot, never from the
local device captured before discovery/availability/subscription awaits. On return it dispatches an
ack event with the original token; an obsolete ack cannot clear newer debt or
update a second payload cache. On failure retain only the current bounded
obligations. Retry on existing change/reconcile/session-recovery triggers; do
not add a tight retry loop.

Every window replacement or end revokes the previous window's in-flight effect:
new acceptance (including same-command late acceptance), every expiry
(including successful `QUIET/idle`), supersession, contradiction, abandonment,
generation/binding invalidation and withdrawal. Replace old-window ordinary and
terminal debt with the transition's current debt and request cancellation of
the obsolete publish child synchronously, before dispatch returns. This rule
does not cancel a current terminal obligation for an ordinary field update.
The single lane worker joins that child before any new publish. A fake transport blocked before
handoff must observe cancellation before its release gate opens, and must never
send the old requested value afterward. This preserves both the cancellation
and wire assertions in #232's
`test_adapter_rearm_publish_stays_cancellable[supersede]` and the separate
withdrawal case. Revocation does not await this join before native admission.
There is one worker and at most one publish child per peripheral, regardless
of replacement rate. Reuse the existing one-second worker-settle bound
(`_SHUTDOWN_WORKER_SETTLE_S`) for this join, using a bounded wait even if the
child suppresses cancellation. Failure to settle latches session-fatal recovery
through existing bounded teardown; do not start a competing lane on that client.

A single lane prevents an old pending publish from completing *after* a newer
terminal publish to the same topic: the newer publish waits for completion or
joined cancellation. Internal expiry still occurs at the fixed monotonic
deadline while MQTT is blocked. Every expiry revokes stale pending publication
without waiting for the broker, whether its result is unconfirmed or idle.
An obsolete ack releases only its own in-flight bookkeeping, then the worker
sends current terminal/current debt. A late acceptance similarly cannot leave
the lane blocked behind the previous unresolved window's earlier wire deadline.

Terminal status for the current command/window is latched until an attempt
with that exact obligation is accepted by MQTT. Ordinary subsequent observations
may update fields but cannot replace this terminal status with idle before that
attempt; latest ordinary state follows it. A newer terminal for the same command
replaces the older terminal reason, and a new command, removal or withdrawal
revokes the previous command's obligation. A new window of the same command,
including late acceptance after unresolved expiry, also revokes the old
window's obligation and cancels/joins its stale pre-handoff child. Window ID is
part of every obligation/ack token. Ordinary observations in an unchanged
terminal window preserve the latch. This bounds memory and does not
promise delivery of every transient reason or feedback from superseded
commands. Tests block the lane, contradict and then observe, and require a
current terminal attempt before eventual ordinary idle state.

A terminal obligation has an identity separate from ordinary render revision.
Auxiliary/ordinary field updates do not cancel its in-flight attempt or prevent
its acknowledgment forever. An ack for the same current obligation can retire
that diagnostic obligation even if newer ordinary fields exist, but cannot
clear those newer fields' publication debt. A replaced terminal obligation,
command or lease makes the old obligation ack ineligible. Ordinary publication
debt still clears only for its exact render revision. This separation prevents
an auxiliary stream from starving terminal delivery or losing newer state.
Render a terminal obligation with its own reason/mask plus eligible current
auxiliary fields: `unconfirmed` always has null primary. It cannot borrow an
idle projection's known primary while waiting for its attempt. A stale prior-
window terminal ack cannot release the new pending window's debt or deadline.

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

Reconcile requests idempotent discovery/command-route repair for current
admitted inventory/bindings. Rejection of stale field evidence never skips
repair for a current binding. A pre-acquisition or stale inventory batch cannot
create a binding or install a peripheral already removed/retyped by newer
metadata. After each await, use the current binding/descriptors and scope,
then request current owned publication.

Routing membership is level-triggered desired state, serialized per MQTT
client/topic with one in-flight SUBSCRIBE/UNSUBSCRIBE and current desired
membership. Old-lease cleanup cannot unconditionally unsubscribe a topic or
delete a route another lease now wants. If A's SUBACK returns after B wants
the same topic, retain the subscription and install B's current route. If no
current owner wants it, serialize unsubscribe before any later subscribe.
Acknowledgments update only their own bookkeeping; the route map and accepted
membership cache are committed for current desired bindings. New route types
are retranslated from current descriptors. Discovery/config repair likewise
serializes each topic so an old completion schedules current repair rather
than leaving a stale descriptor last on the broker. Withdrawal sets desired
membership to absent synchronously; reacquisition can replace that intent
without racing an unconditional compensating cleanup. Test delayed old SUBACK
across reacquisition and retyping, then command delivery without another resync.

Capture ingress happens before availability/discovery publishes, so a newer
push during a routing await already updates the slot. Reconcile never resumes
with a frozen old state document. Acquisition and reconnect follow this same
path; discovery failure retains ordinary repair debt for a later existing
reconcile. It does not undo a slot transition or hide a command subscription.

## Bounded work and panel safety

Per active peripheral retain one observed snapshot, a bounded target map
(validated native command fields), one ticket/outcome, one matching-evidence
map, one timer, one publisher with one publish child, one terminal obligation
and one latest ordinary payload, plus one projection/association per supported
auxiliary field. Routing retains one desired membership and
one in-flight operation per active topic. Synchronous ingress has
no backlog. Replacement cancels the previous timer; callback registries remove
finished ticket listeners and old lifecycle sinks. Removal drops slots and
workers after revocation/join; tombstones are bounded by active work rather
than every ID ever seen. Sequence counters consume constant object count.
Active slots plus retired cleanup records share a fixed internal ceiling of
256 per mesh bridge, including pending topic operations derived from those
records. Check before allocation, and preflight complete inventory capacity
before field/slot creation. Exceeding the ceiling closes mesh state/command
admission with one disabled flag;
capacity alone must not tear down the shared healthy bus/MQTT sessions or
interrupt the wired bridge. Keep only counts, mode and bounded admitted/cleanup
records, never another slot/worker for overflow. Log flag changes with bounded
diagnostics, not on every notification.
Revoke existing mesh owners/tickets and confirmation windows synchronously;
cancel/join their old effects and use only the bounded admitted records to
publish unconfirmed/null and finish route cleanup. Capacity cannot leave an
old command eligible to issue or an old match eligible to confirm.

The same stable oversized topology stays disabled across ordinary ticks and
elections. Capacity never requests a shared session rebuild; an unrelated
rebuild simply preflights capacity again. Re-enable only when an existing
successful complete inventory plus retained cleanup fits the ceiling,
creating current owners from that admitted inventory. The inventory control
path remains enabled while mesh state/command admission is disabled; it only
counts/preflights before allocating owners. Otherwise keep the flag
disabled. No persistent circuit, retry counter or new polling is needed.
Noncooperative transport/actual sink failures remain separate fatal recovery
cases. Test a stable 257-binding inventory over many ticks/resyncs: no repeated
rebuild/acquisition attempts, no mesh writes, wired commands/publishes continue.
Also test recovery from a newer fitting inventory.

The documented 40-peripheral mesh is a sample, not a protocol limit. This guard
does not promise lossless arbitrary inventory churn behind an unavailable
broker. Test blocked-broker churn through the ceiling and repeated successful
create/write/remove cycles in one generation.

Adapter/compatibility metadata follows the same lifetime accounting. Mesh uses
its slot field watermarks and issue/return sequences, so mesh writes/captures
must not allocate or scan `_field_eras` at all. Skip this unused bookkeeping
for mesh writes/captures; leave wired/privacy era behavior unchanged. The mesh acquisition,
binding and topology floors provide ABA protection for delayed reads after
mesh-era retirement. On completed removal, drop materialized `_devices` views,
payload/field caches, command-generation associations and unused topic-repair
entries as well as reducer slots. Keep lifecycle/binding identities globally
unique instead of retaining a counter for every old peripheral ID. Tests count
adapter keys, scan work and compatibility maps through successful churn; a
small live-slot count alone is insufficient evidence of bounded work.

Cost is O(fields in the admitted capture/command) per event; reconnect is
O(current slots) once per reconnect. Use existing normalized/frozen captures
rather than clone the whole home graph. No task per capture, no busy wakeups,
no extra clock reads on uninterested paths, and no standby mesh hot-poll read.
Preserve baseline reconcile scoping: a standby panel's existing full reconcile
may include extras; this change does not add or remove those reads. Count native
reads by leader/standby and startup/hot-poll/resync/reconnect triggers against
unchanged main. With a 40-peripheral mesh and blocked MQTT, count synchronous
field work, normalizations, renders, publications and live tasks as well as
asserting object bounds. Preserve existing coalescing for unrelated consumers.
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
| R2.3 delivery time refreshes old match | Immutable monotonic read-start/push-capture time and per-field watermarks. | Match captured t=1, delayed batch t=79, expiry t=80; unconfirmed. |
| R2.4 caller deadline loses known receipt/fence | Native outcome is stored/reduced before future completion; timeout cannot replace it. | Ticket settles before result future; caller timeout, old ON delivery; pending with correct acceptance fence. |
| R3.1 reconcile publishes stale local snapshot | Reconcile only wakes current-state publication lane after routing awaits. | OFF reconcile blocks in discovery, ON contradiction, release; final retained ON. |
| R3.2 reconnect invalidation waits for observations | Generation bump and sink dispatch are synchronous, timer checks live generation. | OFF match t=75, reconnect/resubscribe blocks through t=80; unknown, no idle/OFF. |
| R3.3 stale reconcile skips discovery/subscription | Routing repair never branches on capture freshness. | Acquire, availability blocks, newer push, release; discovery and set route installed. |
| R3.4 timely return loses push before waiter arm | Owner arms at native return; waiter has no arming responsibility. | Successful OFF, ON push before command waiter resumes; contradicted. |

## Implementation and validation sequence

Design-review counterexamples are additional required regressions, beyond the
historical #232 port:

| Schedule | Required result |
| --- | --- |
| Gated complete read overlaps disjoint power push, then overlapping `on` push in a separate case | Read's unshadowed contradiction applies; newer overlapping target wins; auxiliary fields do not regress. |
| Actual registered reconnect hook with already-ready expiry, on-loop and foreign-thread cases; old-session hook after replacement | No old confirmation after hook fence; stale hook cannot invalidate successor. |
| Read begun in lease A finishes after withdraw/acquire B, before B's initial read | No stale state, motion, slot recreation or removed route from A's batch. |
| Observer notifications with identical mirror but target-only, power-only, deletion and unreliable delta masks | Only explicit reliable target reports refresh evidence; missing-mask fallback cannot confirm. |
| Real MQTT lane folds B into waiting A; B returns and ON contradicts before re-handler | One issue on unchanged binding, actual B targets own the window, B is contradicted. Fresh-ticket rebound ignores A/B's abandoned association. |
| t=79 read completes after t=80 unconfirmed retirement; current auxiliary-only delta follows reconnect | Primary remains unknown until explicit current primary evidence beyond retirement. |
| Stale high motion score arrives between two admitted low scores | No stale last-hot refresh or ON derivation. |
| Old SUBACK/discovery completion straddles reacquisition or retyping | Final current route/config and successful command without another reconcile; old cleanup cannot unsubscribe it. |
| Pending transport gate superseded before release; separate withdrawal case | Old pre-handoff publication is canceled and never sends old request after release; current request stays owned. |
| Blocked publisher, terminal contradiction, then ordinary observation | Terminal obligation gets a current MQTT attempt before ordinary idle debt; storage stays bounded. |
| One-shot sink failure, healthy bus/MQTT, unchanged election | Immediate write/publication revocation, coordinator fatal-latch consumption, bounded session recovery. |
| Never-issued cancellation, waiting withdrawal, 40-peripheral notification storm and blocked MQTT | No spurious unknown/failure window, waiting RPC tombstoned, baseline read counts, bounded tasks and normalization/render work. |
| Successful full inventory omits a matched peripheral, or scoped lookup returns None, before expiry | No old idle/target echo; versioned absence clears unshadowed match/primary validity, removes absent binding after owned unknown publication and bounded cleanup. Failed/partial reads do not remove it. |
| Auxiliary/maintenance return, newer native auxiliary contradiction, delayed caller echo | Current native value wins; optimistic echo still works without intervening native evidence and preserves primary pending/unknown and evidence age. |
| Old queued mesh intent crosses same-topic owner replacement, followed by current intent | Zero native calls for old intent, exactly one for current intent; queue coalescing/folds never cross owner references. |
| Foreign reconnect hook advances generation before waiting mesh task wins device lock; separate issue-sink exception | Positive issue authorization fails, no native observer call, waiting admission tombstoned; already-authorized RPC retains its lock. Wired best-effort issue-callback behavior stays unchanged. |
| Issued command worker canceled at t=1, RPC unresolved through t=6, native success t=7 | Intent is abandoned/unconfirmed immediately; no pre-return contradiction or false native cancellation; keep unknown through return, then resume only on a later trusted primary capture. No new independent deadline timer. |
| Successful scoped parent lookup returns None, empty mapping, incomplete object and exception in separate runs | Explicit ScopeUnknown versus authoritative inventory versus no absence contribution; older negative evidence cannot erase newer target capture. |
| Unresolved expiry terminal blocked before handoff, then same-command acceptance rearms and old ack arrives | Cancel/join only old window effect, publish fresh pending deadline, no old pre-handoff unconfirmed send or stale debt clearing. |
| Blocked unresolved pending publish followed by late acceptance | Cancel/join the prior pending child and publish the new full-window deadline; stale ack cannot clear current debt. A noncooperative child reaches bounded fatal recovery; already handed-off packets cannot be retracted. |
| Blocked pending publish followed by successful `QUIET/idle` expiry | Expiry revokes/joins the pending child even without a terminal failure; eventually publish current idle state, or bounded fatal recovery for a noncooperative child. |
| Many successful create/write/remove cycles without reconnect | Adapter era keys, per-capture scan work, materialized views and route/payload maps remain bounded; wired/privacy provenance passes unchanged. |
| Stable 257-binding inventory over repeated ticks/resyncs, then newer fitting inventory | Mesh stays safely disabled with bounded attempts and records while wired service continues; existing inventory event permits bounded recovery, without added polling. |

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
