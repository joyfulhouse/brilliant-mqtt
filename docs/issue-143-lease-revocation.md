# Issue #143: lease revocation and remaining ambiguity

Issue #143 is the recovered-but-unrecorded writer-death residual. A bridge
recovers its local bus, cannot record that success in the phase marker, and
then dies. Its stale failed-attempt record then looks like native-crash
evidence (#133). The writer now revokes that record through its lease
descriptor, and the watchdog rejects dead-writer evidence on read-only marker
storage. Together these close #143 except in the cases listed under
"Completion limits". No production fault injection, bus commands, reboot, or
deployment was used to validate this change.

## Where the marker lives

The marker defaults to `/run/brilliant-mqtt/bus-phase` on tmpfs, and no shipped
unit makes `/run` read-only. The read-only gate below therefore matters only
when `BUS_PHASE_FILE` points at persistent storage or `/run` itself is
read-only. A fallback marker on volatile storage would add nothing: the marker
already lives on tmpfs, and the residual there is failed writes, not a
read-only mount.

## Independent evidence

A failed marker write is not evidence that the bus recovered. The bridge
retains an existing exclusive marker lease when an unsuccessful local-bus
attempt cannot update its marker. A successful bus read or a transition to
`pre_bus` still releases that lease if replacement, in-place rewrite, and
invalidation cannot record the transition. Before releasing it, the writer
revokes the stale record through the lease descriptor (see below). A
successful read also releases and revokes a retained failed-attempt lease when
marker retries are rate limited. Attribution remains best effort and never
prevents the bridge from operating.

The watchdog keeps one in-memory observer for its phase path. It revokes stale
attribution only after these ordered observations:

1. A valid bus record has a contended lease and its exact boot, PID, and
   process start tick are confirmed live.
2. The same record on the same device/inode later has no exclusive lease.
3. After reading that unleased inode, the same process generation is confirmed
   live again. Death between the lock sample and the process sample cannot
   prove an intentional live release. The Linux `PF_EXITING` flag and
   zombie/dead kernel states are excluded from live generation evidence: their
   locks can already be released while `/proc` still exposes the old start
   tick.

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

## Read-only marker storage gate

A writer on read-only marker storage cannot record a recovered bus, so a dead
writer's stale attempt there is not crash-loop evidence. `bus_failure_age`
checks `os.statvfs` for `ST_RDONLY` on the marker path. Read-only storage, or a
`statvfs` failure (writability unknown), rejects dead-writer evidence. Unknown
is deliberately treated as read-only, which fails safe toward no reboot.

The check is the last operand of the dead-writer rejection chain. In that
position it only adds rejections, and `statvfs` runs only after the marker was
opened and parsed. A live writer that still holds its lease is still accepted
on read-only storage: its lease is independent evidence that the attempt is
current.

## Revocation through the lease descriptor

A writer leaving bus attribution (a successful read, or a `pre_bus` broker-only
retry) first tries every path-based write: atomic replacement, in-place
rewrite, and unlink. Only if all of them fail does it call `ftruncate(fd, 0)` on
the leased descriptor, while still holding the exclusive `flock`, and then
release the lease. The watchdog therefore never sees the stale attempt
unleased: it sees either the leased record or an empty marker, which parses as
no evidence. This holds after writer death and across watchdog restarts, so it
does not depend on sampling.

Revocation runs on these paths:

- The rate-limited recovery return. A successful read within the retry interval
  of a failed write truncates without attempting a write in that call.
- The write-failure path after the atomic replacement and in-place rewrite
  fail and the unlink also fails.
- The outer `OSError`/`UnicodeError` path for an unexpected error mid-update,
  again only if the unlink fails.
- The `BlockingIOError` path.

A failed bus attempt never truncates, so genuine #133 crash-loop evidence is
unchanged whenever marker writes succeed, and still retained when they fail.
If the kernel refuses the truncate, the writer logs one warning and keeps the
earlier best-effort behaviour: it releases the lease with the stale bytes in
place.

The lease stream is opened unbuffered and binary (`io.FileIO`). With no
userspace buffer, no stale bytes can be flushed into the inode after a truncate
or by a later close. A buffered stream was shown to rewrite the stale attempt.
Rewrites encode the record before any I/O, so an unencodable record changes
nothing. They loop on short writes and truncate at the current position. A
short write followed by an error can leave a retained failed-attempt marker
with a mixed new-prefix/old-tail record. Such a record almost never parses,
and the leave-attribution paths truncate it afterwards.

On a full tmpfs, an in-place rewrite and an unlink usually still succeed, so
revocation is rarely needed there. The cases that actually reach truncation
are an LSM denial, an I/O error with a persistent `BUS_PHASE_FILE`, the
rate-limited recovery, and an unexpected error mid-update. An immutable or
append-only inode refuses the truncate; that example applies only to
persistent storage, since tmpfs supports the immutable flag only from Linux
6.0.

## Completion limits

The remaining cases are:

- The kernel refuses `ftruncate` on an open writable descriptor, for example
  an append-only or immutable inode on persistent storage, or an LSM policy.
- The writer dies in the few syscalls between a successful read and the
  truncate.
- Sampling gaps. If no revocation was needed or possible, the watchdog's
  sampled-release evidence is limited to one observed writer generation and
  one watchdog lifetime. A different writer generation can inherit old failure
  history under the existing bounded inheritance rules. Read or identity
  failures, inode replacement, and an unobserved fresh marker can prevent
  sampled revocation or rearming.

In these cases the final observable state can be exactly the same for (a) a
failed local-bus attempt ending in a native crash and (b) a recovered bus whose
success could not be recorded before death: identical marker bytes, no lease,
the same boot ID, absent writer PID, and the same service metadata. Rejecting
every such state would remove existing native-crash-loop recovery (#133).
Closing these needs a separate recovery/revocation channel with independently
guaranteed delivery or retention (for example, a watchdog-owned channel
acknowledged before the writer proceeds), and a policy for signal loss and
watchdog restarts.

## Regression evidence

`tests/test_bus_wd_health.py` includes synchronized child processes using real
Linux `/proc` identity and `flock`. The read-only writer test is parametrized
on an inode that accepts `ftruncate` versus one that refuses it. With a
writable inode, the recovered variant empties its marker, so both the existing
observer and a newly started one reject it after exit. With a refused
truncate, the recovered variant keeps identical old marker bytes: the existing
observer keeps it revoked after process exit, while a new observer
demonstrates the remaining restart ambiguity. The failed-attempt variant keeps
its evidence in both. Other watchdog tests cover the read-only and unknown
storage gate, a live leased writer on read-only storage, replacement-inode and
first-unleased samples, fresh leased rearming, and the unsampled native-crash
ambiguity.

`tests/test_heartbeat.py` covers unrecordable recovery and `pre_bus`
transitions (not dead-writer evidence), retained failed-attempt evidence, a
refused truncate, revocation after a partly failed rewrite, the unbuffered
lease stream, unencodable records, the lease being held during the truncate,
swallowed close errors, revocation after an unexpected error, and revoking an
already-closed lease.
