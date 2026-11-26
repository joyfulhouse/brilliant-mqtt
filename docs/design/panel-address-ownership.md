# Panel address ownership

Status: proposed for issue #259. Supersedes the approach in closed PR #260;
credit @samsonusmc for the original fix, identity checks, and regression cases.

## Decision and owner

Use **journal first, config second, with one reconciling reader**. A new
HA-side `PanelConnectionOwner` owns an existing panel's connection tuple:
config host/login/pin, journal host/login/pin, retained-baseline host/pin.
Config is the committed source; a pending intent makes that tuple unavailable
to recovery. Connections are never assembled from mixed stores.

The owner key is `(entry_id, subentry_id)` (legacy: entry ID with no subentry),
plus unchanged slug/key. A replacement subentry is a different owner even with
matching address/pin. Derive missing legacy fingerprints from the key; preserve
unjournaled legacy key types and slugs. Address moves cannot change slug/pin.

Change network address, Repair SSH credentials, and legacy Connection call this
service after current-request verification. Candidate connections check the
stored key before login/writes; no address-move TOFU. Unjournaled legacy endpoint
replacement requires an explicit choice: discover the key without login, confirm
its fingerprint, then connect pinned to it. Recheck no active/retained/pending
record under the lock before login/writes; transfer no baseline or old intent.
Canaries never adopt: report typed `baseline_owner_mismatch` or
`address_change_incomplete`. Re-running the verified form repairs stranded
baselines, including same-address submissions.

## Durable commit protocol

Take the provisioner's operation lock before the journal lock. Verification
captures owner IDs and prior connection; re-read them under the lock and reject
drift. Merge into fresh config. Every legacy section writer also rechecks its
connection after the last await, rejects drift, and patches only its section's
fields into fresh config. No stale full-data saves; no lock waits for a reload.

1. Read this slug/key's active/retained records and current owner. Only this
   request's freshly verified target/login may replace an incomplete move;
   refuse a different bound owner/pin. Preserve unrelated records.
2. Atomically save and read back an address-change intent in the provisioning
   envelope before changing any member of the tuple. It binds the original
   commit ID, owner IDs, slug/key, prior/proposed connection and transaction IDs.
   Multiple intents and the optional active record share one envelope writer;
   all completion/removal paths preserve intents. Retained rows omit logins.
3. Write/read back matching active/scoped recovery records and retained hosts.
   Preserve phases, snapshots, transaction IDs and named-operation owner IDs.
4. Recheck the original live owner/prior connection; merge connection fields
   and this request's new `connection_commit_id` into fresh config using HA's
   update API. The new ID schedules a save even on a same-value retry after a
   consumed failed save. Wait for bounded proof in real `core.config_entries`
   of the exact owner, resulting connection and commit ID, checking live
   ownership before/after each read. Equality in RAM is insufficient.
5. Re-read/validate all affected records and the exact live/durable config.
   Only then atomically remove this intent and read back the envelope. This
   removal is the commit point. Report success only after it is proven. Request
   runtime reconciliation explicitly, even for a same-value retry. Proven
   removal enqueues this idempotent notification and reconciles repair issues
   before propagating cancellation; neither waits for recovery under the lock.
   If the runtime is absent or HA is stopping, normal startup consumes committed
   state. Other enqueue failures report a retryable error and retain the repair;
   a fresh form retries notification even with no remaining pending intent.
   The committed tuple is never undone for a notification failure.

Errors, timeouts, drift and cancellation after step 2 retain intent and fail the
request. Shielded writes settle/read back before unlocking. Never compensate
with an older tuple. Reconcile issues against durable intent state.

## Reload, recovery, and isolation

`PanelConnectionOwner` supplies the single connection-resolution/admission
reader used by startup, background recovery, named finalize/rollback, onboarding
compensation, and removal recovery (including INSTALL/UPGRADE). It reads pending
state before returning connection data, under the operation lock at dispatch.
It never applies, retries, or retires an intent during passive loading.

| Interruption | Reload behavior |
| --- | --- |
| Before durable intent | Unchanged tuple; ordinary recovery. |
| After intent, before/between writes (including delayed config save) | Preserve evidence; refuse affected recovery. |
| All stores match, intent remains | Refuse until fresh verification finishes commit. |
| Proven intent removal | Committed config and normal checks. |
| Later edit, removal/replacement or re-pin | Preserve evidence; never replay, overwrite or transfer. |

All incomplete states yield one typed `address_change_incomplete` repair per
original owner: **re-run Change network address** (legacy: **Connection**).
If the saved login fails, Change network address routes to the current-login
form while retaining the entered host; only successful fresh verification can
complete the commit. Missing owners leave an actionable diagnostic with the
same code and preserved evidence; recreating an owner never inherits it.

Fleet setup defers only records whose owner cannot currently be resolved,
including a named intent bound to a removed owner. It still constructs healthy
sibling runtimes. Background recovery skips deferred retained finalizations;
explicit rollback processes its named transaction, not unrelated finalizations.

Named rollback uses a transaction-scoped journal view backed by optional
`recovery_record` in its retained row. Phase/error/evidence/completion writes
touch only that row; login is omitted on disk and resolved from the bound live
owner before dispatch. Cold reload resumes kitchen's scoped rollback without
replacing office's active record. An existing same-transaction active rollback
continues in place with a bound retained marker; never duplicate it. Finalize
uses its own retained marker, without an unrelated-active-slot veto. All execute
under the operation lock; new provisioning still requires the single free slot.

Named rollback/finalize persist the original owner IDs with their intent and
check them on continuation and cold recovery. A queued request cannot borrow
a replacement owner's login. An explicit operation re-reads the named record
under the operation lock after an intervening verified change: it executes the
fresh authorized record or returns a retryable typed error, never silent success
from the background worker's stale-record comparison.

Old named ROLLBACK/restore-requested/finalizing markers lack owner IDs:
defer them, preserve evidence, report the existing recovery repair, start
siblings. Never infer past authority from lookalike config. A new explicit
named request binds its selected current owner under the operation lock;
an address mismatch first requires the verified form. Passive old soak rows
remain usable. Existing independently proven onboarding/ownerless authority
keeps its separate recovery path, subject to the pending-connection gate.
Pending connection commits always need fresh form verification.

## Scope and verification

No automatic replay, fleet/journal-backed pin changes, slug changes, baseline
deletion, new panel process, polling, extra provisioning slot or private HA
forced-save API. Use existing bounded disk proof. Read old record-only envelopes
and retained rows without scoped recovery; unbound markers need explicit
authorization. No unmerged #260 format migration. Invalid storage retains
existing refusal rules.

TDD reuses S2/S8 repros: delayed saves/cold loads, later edits, combined repair,
replacement owners, removal, sibling startup/rollback with an occupied slot,
same-value retry plus final-removal cancellation, legacy formats/key types/slugs.
Exercise production readers/setup behind offline fakes. Local static/focused
gates and CI full suites are required; live panel validation remains outstanding.
