# Issue #143: sampled lease revocation and remaining ambiguity

This follow-up reduces the read-only-filesystem recovered-but-unrecorded
writer-death residual. It does not completely resolve #143 or authorize closing
it. No production fault injection, bus commands, reboot, or deployment was used
to validate this change.

## Independent evidence

A failed marker write is not evidence that the bus recovered. The bridge now
retains an existing exclusive marker lease when an unsuccessful local-bus
attempt cannot update its marker. A successful bus read or a transition to
`pre_bus` still releases that lease if replacement, in-place rewrite, and
invalidation cannot record the transition. A successful read also releases a
retained failed-attempt lease when marker retries are rate limited. Attribution
remains best effort and never prevents the bridge from operating.

The watchdog keeps one in-memory observer for its phase path. It revokes stale
attribution only after these ordered observations:

1. A valid bus record has a contended lease and its exact boot, PID, and process
   start tick are confirmed live.
2. The same record on the same device/inode later has no exclusive lease.
3. After reading that unleased inode, the same process generation is confirmed
   live again. Death between the lock sample and the process sample cannot prove
   an intentional live release. The Linux `PF_EXITING` flag and zombie/dead
   kernel states are excluded from live generation evidence: their locks can
   already be released while `/proc` still exposes the old start tick.

The reader holds its shared lock through an unleased record read. A copied
record on a replacement inode does not prove release of the original inode's
lease. The observer's sampled revocation survives a later disappearance of the
same writer generation. Missing or malformed intermediate samples do not erase
it. A changed valid record with a lease and the same generation observed live
can rearm that generation; this is explicit fresh marker evidence, rather than
an inference from a failed write. A changed record first seen after death does
not rearm a revoked generation.

Sampling runs even if systemd start-time metadata is unavailable. The existing
start-time requirement still prevents reboot decisions in that situation.
Gateway, service activity, stale-heartbeat, failure-age, retention, and reboot
journal/admission gates remain in force.

## Completion limits

Revocation is deliberately limited to one observed writer generation and one
watchdog lifetime. These cases remain unresolved:

- Recovery and lease release followed by death entirely between watchdog
  samples, or before any live leased sample, provide no independent evidence.
- A watchdog restart loses in-memory revocation. If the stale attempt is still
  within retention, a newly started observer can accept it as native-crash
  evidence.
- A different writer generation can inherit old failure history under the
  existing bounded inheritance rules when it can write a new marker. This
  patch does not redesign that cross-generation protocol.
- Read or identity failures, inode replacement, and an unobserved fresh marker
  can prevent revocation or rearming. The watchdog does not fabricate proof
  from those gaps.

In the first two cases, the final observable state is exactly the same for
(a) a failed local-bus attempt ending in a native crash and (b) a recovered bus
whose success could not be recorded before death: identical marker bytes,
no lease, the same boot ID, absent writer PID, and the same service metadata.
Rejecting every such state would remove existing native-crash-loop recovery
(#133). Fully separating those histories requires a separate recovery/revocation
signal with independently guaranteed delivery or retention (for example, a
watchdog-owned channel acknowledged before the writer proceeds), and a policy
for signal loss and watchdog restarts. Another file on the same read-only
filesystem cannot provide that guarantee.

## Regression evidence

`tests/test_bus_wd_health.py` includes synchronized child processes using real
Linux `/proc` identity and `flock`. Both failed-attempt and recovered variants
retain identical old marker bytes under simulated read-only writes. Only the
recovered variant releases its lease while alive; the existing observer keeps
it revoked after process exit, while a new observer demonstrates the unresolved
restart ambiguity. Other regressions cover replacement-inode and first-unleased
samples, fresh leased rearming, and the unsampled native-crash ambiguity.
`tests/test_heartbeat.py` covers retention of genuine failed-attempt leases and
release during a rate-limited successful read.
