# Panel address ownership

Status: proposed for issue #259. Supersedes the approach in closed PR #260;
credit @samsonusmc for the original fix, identity checks, and regression cases.

## Decision and owner

Use **journal first, config second, with one reconciling reader**. A new
HA-side `PanelConnectionOwner` service is the only writer for an existing
panel's connection tuple: config host/login/pin, active journal host/login/pin,
and retained-baseline host/pin. Config remains the committed connection source;
an outstanding journal intent makes that panel's tuple unavailable to recovery.
There is no effective connection assembled from mixed stores.

The owner key is `(entry_id, subentry_id)` (legacy: entry ID with no subentry),
plus unchanged slug and public key. A replacement subentry is a different owner
even if its address and pin match. A fingerprint is derived from the key when
an older legacy entry lacks one. Address changes cannot change slug or pin.
Unjournaled legacy entries retain their existing accepted key types and slugs.

Fleet Change network address, Repair SSH credentials, and legacy Connection
all call this service after current-request identity and login verification.
Canary actions do not adopt addresses. They report typed `baseline_owner_mismatch`
or `address_change_incomplete`; an existing stranded baseline is repaired by
re-running the verified form, including a same-address submission.

## Durable commit protocol

The service uses the provisioner's existing operation lock before the journal
lock. Verification captures the exact owner IDs and prior connection tuple;
after acquiring the lock it re-reads that owner and rejects changed connection
fields. Names and unrelated settings are merged from fresh config, never from
the pre-verification mapping. No lock is held while waiting for a reload.

1. Read the exact active and retained records for this slug/key and the current
   owner. An existing incomplete move is evidence, never authority: only this
   request's freshly verified target/login may replace it. Refuse a different
   bound owner or pin. Preserve any unrelated panel's records.
2. Atomically save and read back an address-change intent in the provisioning
   envelope before changing any member of the tuple. It binds the original
   owner IDs, slug/key, prior and proposed connection fields, and affected
   transaction IDs. Multiple panel intents coexist; an optional active record
   and all intents share one envelope writer. Every journal completion/removal
   path preserves outstanding intents. Retained rows never acquire login data.
3. Write/read back the matching active journal with the verified connection,
   then the matching retained rows with the verified host. Preserve phases,
   snapshots, transaction IDs, and named-operation owner bindings.
4. Recheck the original live owner and prior connection, merge only connection
   fields into its latest config, and call HA's config update API. This merely
   schedules a save. Wait for bounded proof in real `core.config_entries` that
   the exact owner and resulting connection are stored, checking live ownership
   before and after each read. A consumed failed save is explicitly rescheduled
   by a fresh verified retry; equality in RAM is insufficient.
5. Re-read/validate all affected records and the exact live/durable config.
   Only then atomically remove this intent and read back the envelope. This
   removal is the commit point. Report success only after it is proven. Request
   runtime reconciliation explicitly, even for a same-value retry, so deferred
   recovery resumes without needing another config edit.

Any error, timeout, owner drift or cancellation after step 2 retains the intent
and fails the request. Shielded writes settle and are read back before releasing
locks; cancellation never implies completion. No compensation writes an older
tuple back. If intent removal completed despite cancellation, issue reporting
reconciles against the durable intent set, avoiding a stale resolved repair.

## Reload, recovery, and isolation

`PanelConnectionOwner` supplies the single connection-resolution/admission
reader used by startup, background recovery, named finalize/rollback, onboarding
compensation, and removal recovery (including INSTALL/UPGRADE). It reads pending
state before returning connection data, under the operation lock at dispatch.
It never applies, retries, or retires an intent during passive loading.

| Interruption | Reload behavior |
| --- | --- |
| Before intent is durable | Tuple is unchanged; ordinary existing recovery applies. |
| After intent, before journal/retained/config writes | Preserve evidence; refuse affected recovery. |
| Between any writes, including HA's delayed config save | Same refusal even if RAM happens to match the target. |
| All stores match, before intent removal | Still refuse; a fresh verified form must finish the commit. |
| Intent removal is durably proven | Use committed config and normal record checks. |
| Later saved address/login, owner removal/replacement or re-pin | Never replay or overwrite it; retain the old evidence and refuse transfer to the new owner. |

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
An affected active transaction may continue occupying the existing single
provisioning slot; it cannot stop unrelated named rollback/finalize or startup.

Named rollback/finalize persist the original owner IDs with their intent and
check them on continuation and cold recovery. A queued request cannot borrow
a replacement owner's login. An explicit operation re-reads the named record
under the operation lock after an intervening verified change: it executes the
fresh authorized record or returns a retryable typed error, never silent success
from the background worker's stale-record comparison.

## Scope and verification

No automatic replay, pin rotation, slug changes, baseline deletion, new panel
process, recurring polling, or change to the single provisioning slot. No
private HA forced-save API; the existing bounded disk-proof approach is used.
Old record-only envelopes remain readable; no migration from the unmerged #260
formats is required. Structurally invalid storage keeps existing refusal rules.

TDD reuses applicable S2/S8 repros: real delayed config saves and cold loads at
every boundary, later edits, combined login/address repair, replacement owners
before/after dispatch, removal recovery, two-panel startup and named rollback,
same-value repair notification, cancellation at final removal, and legacy
key-only/ECDSA/long-slug compatibility. Tests exercise production readers and
fleet setup behind offline fakes. Both projects' static checks and focused tests
run locally; CI provides the full suites. Live panel validation remains required.
