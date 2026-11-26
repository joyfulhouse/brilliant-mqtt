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
stored key before login/writes; no address-move TOFU. Under the operation lock,
refuse a candidate key/identity owned by another fleet subentry or legacy entry,
including key-only entries and supported legacy key types, before login/writes.
Different-key legacy endpoint replacement is unsupported in v1: preserve config
and evidence/typed repair; return `host_key_changed`. Remove guidance suggesting
Connection can accept a new key. Same-key legacy changes remain supported.
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
   Fleet, legacy and migration schemas accept an optional canonical UUID commit
   ID as owner metadata, never panel settings. Absence gives no proof for a new
   commit. Migration copies no commit ID into its destination entries.
5. Re-read/validate all affected records and the exact live/durable config.
   Only then atomically remove this intent and read back the envelope. This
   removal is the commit point. Before success or cancellation propagation,
   enqueue idempotent runtime reconciliation and reconcile issues, even on a
   same-value retry. Never wait for recovery under the lock. Absent runtimes or
   stopping HA use normal startup; other enqueue failures return a retryable
   error and retain the repair. A fresh form retries notification without an
   intent. Never undo the committed tuple for notification failure.

Errors, timeouts, drift and cancellation after step 2 retain intent and fail the
request. Shielded writes settle/read back before unlocking. Never compensate
with an older tuple. Reconcile issues against durable intent state.

## Reload, recovery, and isolation

The owner's single admission reader gates startup, background/named recovery,
onboarding compensation and removal (including INSTALL/UPGRADE). Under the
operation lock, check pending state before dispatching connection data.
Passive loading never applies, retries or retires intents.

| Interruption | Reload behavior |
| --- | --- |
| Before durable intent | Unchanged tuple; ordinary recovery. |
| After intent, before/between writes (including delayed config save) | Preserve evidence; refuse affected recovery. |
| All stores match, intent remains | Refuse until fresh verification finishes commit. |
| Proven intent removal | Committed config and normal checks. |
| Later edit, removal/replacement or re-pin | Preserve evidence; never replay, overwrite or transfer. |

Incomplete moves yield one `address_change_incomplete` repair per original
owner: **re-run Change network address** (legacy: **Connection**). Failed saved
login routes to the current-login form, retaining the entered host. Only fresh
verification completes commit. Missing owners retain the diagnostic/evidence;
recreating an owner never inherits it.

Fleet setup defers only records whose owner cannot currently be resolved,
including a named intent bound to a removed owner. It still constructs healthy
sibling runtimes. Background recovery skips deferred retained finalizations;
explicit rollback processes its named transaction, not unrelated finalizations.

Before any marker or contact, inspect the exact retained row under the operation
lock. An existing scoped `recovery_record` makes finalize refuse, including
`rollback_failed` and `restored` before cleanup. Rollback may only resume that
exact record or refuse; never reset or duplicate it. Only proven completion and
durable scoped-record removal allow normal finalize admission. Otherwise:

| Active slot | Named rollback | Named finalize |
| --- | --- | --- |
| Empty or different panel identity | Retained scoped record | Own retained marker |
| Same transaction, ROLLBACK | Continue active record in place | Refuse |
| Same panel identity, any other active record | Refuse | Refuse |

Refusal is typed `provisioning_recovery_required`, with no marker write or new
authority. Different transaction IDs alone never prove independent panels.
In v1, the existing active path alone settles that panel's updates/installs/
upgrades; no competing scoped rollback or handoff can be added.
A scoped journal view stores optional `recovery_record` in its retained row;
phase/error/evidence/completion writes touch only that row. Omit login on disk;
resolve it from the bound live owner before dispatch. Cold reload resumes
kitchen without replacing office's active record. All execution uses the
operation lock; new provisioning still requires the single free slot.

Named intents persist original owner IDs; check them on continuation/cold
recovery. Never borrow replacement-owner login. Explicit requests re-read under
the operation lock after intervening changes: execute the fresh authorized
record or return a typed retryable error, never background-style silent success.

Defer old unbound named ROLLBACK/restore-requested/finalizing markers with the
existing recovery repair; start siblings. Never infer past authority from
lookalike config. A new explicit named request binds the selected current owner
under lock; address mismatch requires the verified form first. Old soak rows
remain usable. Independently proven onboarding/ownerless authority keeps its
separate path, subject to the pending gate.

Migration preflight and locked execution both refuse a participating panel
with pending address intent, including pending-only envelopes. Check owner IDs
and slug/key associations; preserve its original entry and Connection repair.
Migration becomes eligible again after verified repair. No intent is migrated.

## Scope and verification

No automatic replay, pin/slug changes, baseline deletion, new panel process,
polling, extra provisioning slot or private HA forced-save API. Read old
record-only envelopes/retained rows; no unmerged #260 format migration.
Invalid storage retains existing refusal rules.

TDD reuses S2/S8 crash/concurrency repros against production readers/setup and
offline fakes. Include legacy compatibility, same-value cancellation/wakeup,
occupied-slot sibling rollback, same-transaction refusal/single authority,
in-place rollback, scoped-finalize refusal through cleanup, migration refusal
and marker schemas. Local static/focused
gates and CI full suites are required. Live validation remains outstanding.
