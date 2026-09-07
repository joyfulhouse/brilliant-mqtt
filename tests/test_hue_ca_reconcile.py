import pytest

from brilliant_hue_ca.reconcile import (
    Outcome,
    cert_fingerprint,
    reconcile,
    split_pem_certs,
)
from brilliant_hue_ca.state import PendingReload, load_pending, save_pending

# Two distinct self-signed EC P-256 certs generated once and pasted as
# fixtures (openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256
# -nodes -days 3650 -subj "/CN=..." -keyout /dev/null). CA_A and CA_B have
# DIFFERENT keys (different fingerprints). CA_A_REWRAPPED is CA_A re-emitted
# with different line wrapping (same DER, same fingerprint).
CA_A = """-----BEGIN CERTIFICATE-----
MIIBfTCCASOgAwIBAgIUdPxf3XpyWhlomBsnOw4v6PnRbEwwCgYIKoZIzj0EAwIw
FDESMBAGA1UEAwwJY2EtYS10ZXN0MB4XDTI2MDcxODIzMDQyMFoXDTM2MDcxNTIz
MDQyMFowFDESMBAGA1UEAwwJY2EtYS10ZXN0MFkwEwYHKoZIzj0CAQYIKoZIzj0D
AQcDQgAELbHkjdm57Utb7nuP+u68qOg+5DtLm3J3BkkLthx4TSYFkD02O8STczCH
/eykkJrKVd90Zn4NlnnwPHh1TqXBKaNTMFEwHQYDVR0OBBYEFHI1jyb/yVM80rJa
pCrwjLltX/JzMB8GA1UdIwQYMBaAFHI1jyb/yVM80rJapCrwjLltX/JzMA8GA1Ud
EwEB/wQFMAMBAf8wCgYIKoZIzj0EAwIDSAAwRQIhALuIYO82yKVgMuFSWB70ALJE
UZ0KQhgbgLS5gw+Rh6xeAiBu0CzhNXZ6QO4blinurR+/lGd5m1qRG/RuKanWrWOo
Jw==
-----END CERTIFICATE-----
"""
CA_B = """-----BEGIN CERTIFICATE-----
MIIBfDCCASOgAwIBAgIUElr8OuROFZwugyiOBzTg6jV8Wu8wCgYIKoZIzj0EAwIw
FDESMBAGA1UEAwwJY2EtYi10ZXN0MB4XDTI2MDcxODIzMDQyMFoXDTM2MDcxNTIz
MDQyMFowFDESMBAGA1UEAwwJY2EtYi10ZXN0MFkwEwYHKoZIzj0CAQYIKoZIzj0D
AQcDQgAEfIJgNxbrZgNjCQ3hQopnI5XVvWr5vXpnGFzzHoboL4dE/f/HCg8YnV/j
9lJQNz+tZEePTiJfd5SDtaoNzCQqDaNTMFEwHQYDVR0OBBYEFF7I8YP4O4EZ0kMt
/9ASz1EJH1+cMB8GA1UdIwQYMBaAFF7I8YP4O4EZ0kMt/9ASz1EJH1+cMA8GA1Ud
EwEB/wQFMAMBAf8wCgYIKoZIzj0EAwIDRwAwRAIgL3AIG3UEz4Y0KZ+btz3jYxj0
bM2ExCyfZYQNPLxUzpQCICb/xkNczWKigCiELjj4vY9PWRBiumg7pGNKqFgbDiu4
-----END CERTIFICATE-----
"""
CA_A_REWRAPPED = (
    "-----BEGIN CERTIFICATE-----\n"
    + "".join(CA_A.split("-----")[2].split())
    + "\n-----END CERTIFICATE-----\n"
)

STATE = "/state.json"
INTERVAL = 300.0


class FakeFS:
    def __init__(self, files: dict[str, str], globs: dict[tuple[str, str], str | None]) -> None:
        self.files = dict(files)
        self.globs = dict(globs)
        self.appended: list[tuple[str, str]] = []

    def exists(self, path: str) -> bool:
        return path in self.files

    def read_text(self, path: str) -> str:
        return self.files[path]

    def append_text(self, path: str, text: str) -> None:
        self.files[path] = self.files.get(path, "") + text
        self.appended.append((path, text))

    def write_text(self, path: str, text: str) -> None:
        self.files[path] = text  # overwrite semantics (state file)

    def glob(self, root: str, name: str) -> str | None:
        return self.globs.get((root, name))


class FakeCoord:
    def __init__(self, running: bool, fail_first: int = 0) -> None:
        self._running = running
        self._fail_remaining = fail_first
        self.restarted = False  # kept for existing tests: last restart succeeded
        self.attempts = 0  # total restart() calls (including ones that raise)
        self.successes = 0  # restart() calls that returned without raising

    def is_running(self) -> bool:
        return self._running

    def restart(self) -> None:
        self.attempts += 1
        if self._fail_remaining > 0:
            self._fail_remaining -= 1
            raise OSError("simulated transient vassal-touch failure")
        self.restarted = True
        self.successes += 1


class KilledDuringRestart:
    """Models the oneshot being hard-killed *during* the restart call: restart
    raises something reconcile does NOT catch (only OSError is caught), so it
    propagates like a real crash. The pending marker must already have been
    persisted to disk *before* restart was invoked."""

    def __init__(self) -> None:
        self.attempts = 0

    def is_running(self) -> bool:
        return True

    def restart(self) -> None:
        self.attempts += 1
        raise KeyboardInterrupt


class RaisingFS(FakeFS):
    """FakeFS whose write_text raises a preloaded exception on its next call,
    then behaves normally — models a crash (KeyboardInterrupt) or a read-only /
    full /var (OSError) hitting the marker write at a chosen moment. Unlike the
    base FakeFS's atomic dict-assign, this can fail mid-decision."""

    def __init__(self, files: dict[str, str], globs: dict[tuple[str, str], str | None]) -> None:
        super().__init__(files, globs)
        self.raise_next: BaseException | None = None
        self.write_calls = 0

    def write_text(self, path: str, text: str) -> None:
        self.write_calls += 1
        if self.raise_next is not None:
            exc = self.raise_next
            self.raise_next = None
            raise exc
        super().write_text(path, text)


class ReadFailFS(FakeFS):
    """FakeFS whose read_text raises OSError for one chosen path — models EIO /
    EACCES on an *existing* state file (exists() still True)."""

    def __init__(
        self,
        files: dict[str, str],
        globs: dict[tuple[str, str], str | None],
        *,
        fail_read: str,
    ) -> None:
        super().__init__(files, globs)
        self._fail_read = fail_read

    def read_text(self, path: str) -> str:
        if path == self._fail_read:
            raise OSError(5, "EIO")
        return super().read_text(path)


class UnwritableFS(FakeFS):
    """FakeFS whose write_text always raises once `raising` is set — models a
    persistently read-only/full state dir (EROFS/ENOSPC on /var)."""

    def __init__(self, files: dict[str, str], globs: dict[tuple[str, str], str | None]) -> None:
        super().__init__(files, globs)
        self.raising = False

    def write_text(self, path: str, text: str) -> None:
        if self.raising:
            raise OSError(30, "EROFS")
        super().write_text(path, text)


def test_fingerprint_matches_across_rewrapped_pem() -> None:
    assert cert_fingerprint(CA_A) == cert_fingerprint(CA_A_REWRAPPED)
    assert cert_fingerprint(CA_A) != cert_fingerprint(CA_B)


def test_split_pem_certs_counts_blocks() -> None:
    assert len(split_pem_certs(CA_A + CA_B)) == 2
    assert split_pem_certs("no certs here") == []


def test_ca_absent_and_host_running_appends_and_restarts() -> None:
    fs = FakeFS({"/b": CA_B}, {})
    coord = FakeCoord(running=True)
    out = reconcile(
        fs, coord, bundle_path="/b", site_packages_root="/sp", ca_pem=CA_A, state_path=STATE
    )
    assert out == Outcome(
        bundle_found=True, appended=True, coordinator_restarted=True, bundle_path="/b"
    )
    assert fs.appended and CA_A.strip() in fs.files["/b"]
    assert coord.restarted is True


def test_ca_absent_and_not_host_appends_without_restart() -> None:
    fs = FakeFS({"/b": CA_B}, {})
    coord = FakeCoord(running=False)
    out = reconcile(
        fs, coord, bundle_path="/b", site_packages_root="/sp", ca_pem=CA_A, state_path=STATE
    )
    assert out.appended is True
    assert out.coordinator_restarted is False
    assert coord.restarted is False


def test_ca_present_is_noop() -> None:
    fs = FakeFS({"/b": CA_B + CA_A}, {})
    coord = FakeCoord(running=True)
    out = reconcile(
        fs, coord, bundle_path="/b", site_packages_root="/sp", ca_pem=CA_A, state_path=STATE
    )
    assert out == Outcome(
        bundle_found=True, appended=False, coordinator_restarted=False, bundle_path="/b"
    )
    assert fs.appended == []
    assert coord.restarted is False


def test_ca_present_even_when_rewrapped_is_noop() -> None:
    fs = FakeFS({"/b": CA_A_REWRAPPED}, {})
    coord = FakeCoord(running=True)
    out = reconcile(
        fs, coord, bundle_path="/b", site_packages_root="/sp", ca_pem=CA_A, state_path=STATE
    )
    assert out.appended is False


def test_bundle_missing_at_path_uses_glob() -> None:
    fs = FakeFS(
        {"/sp/lib/certs/hue-bridge-ca-certs.pem": CA_B},
        {("/sp", "hue-bridge-ca-certs.pem"): "/sp/lib/certs/hue-bridge-ca-certs.pem"},
    )
    coord = FakeCoord(running=False)
    out = reconcile(
        fs, coord, bundle_path="/missing", site_packages_root="/sp", ca_pem=CA_A, state_path=STATE
    )
    assert out.bundle_found is True
    assert out.bundle_path == "/sp/lib/certs/hue-bridge-ca-certs.pem"
    assert out.appended is True


def test_bundle_not_found_anywhere() -> None:
    fs = FakeFS({}, {})
    coord = FakeCoord(running=True)
    out = reconcile(
        fs, coord, bundle_path="/missing", site_packages_root="/sp", ca_pem=CA_A, state_path=STATE
    )
    assert out == Outcome(
        bundle_found=False, appended=False, coordinator_restarted=False, bundle_path=None
    )
    assert coord.restarted is False


def test_unparseable_block_in_bundle_is_skipped_not_fatal() -> None:
    fs = FakeFS(
        {"/b": "-----BEGIN CERTIFICATE-----\ngarbage\n-----END CERTIFICATE-----\n" + CA_B},
        {},
    )
    coord = FakeCoord(running=False)
    out = reconcile(
        fs, coord, bundle_path="/b", site_packages_root="/sp", ca_pem=CA_A, state_path=STATE
    )
    assert out.appended is True  # CA_A not present -> appended; garbage block skipped


# --- issue #96: retry the coordinator reload after a partial recovery ---------


def test_restart_oserror_then_next_run_completes_retry() -> None:
    # The literal repro: first restart raises OSError; running ten subsequent
    # reconciles (each a fresh oneshot, `now` advanced past the interval) must
    # eventually land exactly one successful restart and then stop retrying.
    fs = FakeFS({"/b": CA_B}, {})
    coord = FakeCoord(running=True, fail_first=1)

    out = reconcile(
        fs,
        coord,
        bundle_path="/b",
        site_packages_root="/sp",
        ca_pem=CA_A,
        state_path=STATE,
        min_retry_interval_s=INTERVAL,
        now=1000.0,
    )
    assert out.appended is True
    assert out.coordinator_restarted is False  # first restart did NOT succeed
    assert out.reload_pending is True
    assert coord.attempts == 1 and coord.successes == 0
    assert CA_A.strip() in fs.files["/b"]

    now = 1000.0
    for _ in range(10):
        now += INTERVAL + 1
        out = reconcile(
            fs,
            coord,
            bundle_path="/b",
            site_packages_root="/sp",
            ca_pem=CA_A,
            state_path=STATE,
            min_retry_interval_s=INTERVAL,
            now=now,
        )
    assert coord.successes == 1  # completed exactly once...
    assert coord.attempts == 2  # ...and stopped retrying after it succeeded
    assert out.reload_pending is False
    assert load_pending(fs, STATE) is None
    assert len(fs.appended) == 1  # cert appended once, never duplicated


def test_interruption_after_append_is_retried_by_fresh_run() -> None:
    # Append + persist-marker happen, then the process is killed mid-restart
    # (uncaught). A brand-new process must still complete the reload, driven by
    # the on-disk marker (nothing survives in memory across oneshot runs).
    fs = FakeFS({"/b": CA_B}, {})
    dying = KilledDuringRestart()
    with pytest.raises(KeyboardInterrupt):
        reconcile(
            fs,
            dying,
            bundle_path="/b",
            site_packages_root="/sp",
            ca_pem=CA_A,
            state_path=STATE,
            min_retry_interval_s=INTERVAL,
            now=1000.0,
        )
    assert dying.attempts == 1
    assert CA_A.strip() in fs.files["/b"]  # cert was appended before the kill
    assert load_pending(fs, STATE) is not None  # marker durable on disk

    fresh = FakeCoord(running=True)  # a new oneshot, nothing in memory
    out = reconcile(
        fs,
        fresh,
        bundle_path="/b",
        site_packages_root="/sp",
        ca_pem=CA_A,
        state_path=STATE,
        min_retry_interval_s=INTERVAL,
        now=2000.0,
    )
    assert out.appended is False  # no duplicate append
    assert fresh.successes == 1  # reload completed on the fresh run
    assert out.reload_pending is False
    assert load_pending(fs, STATE) is None


def test_retry_is_paced_between_attempts() -> None:
    fs = FakeFS({"/b": CA_B}, {})
    coord = FakeCoord(running=True, fail_first=99)  # restart always fails
    reconcile(
        fs,
        coord,
        bundle_path="/b",
        site_packages_root="/sp",
        ca_pem=CA_A,
        state_path=STATE,
        min_retry_interval_s=INTERVAL,
        now=1000.0,
    )
    assert coord.attempts == 1

    out = reconcile(  # too soon after the last attempt: must NOT re-attempt
        fs,
        coord,
        bundle_path="/b",
        site_packages_root="/sp",
        ca_pem=CA_A,
        state_path=STATE,
        min_retry_interval_s=INTERVAL,
        now=1010.0,
    )
    assert coord.attempts == 1
    assert out.reload_pending is True  # still owed, just paced

    reconcile(  # interval elapsed: attempt again
        fs,
        coord,
        bundle_path="/b",
        site_packages_root="/sp",
        ca_pem=CA_A,
        state_path=STATE,
        min_retry_interval_s=INTERVAL,
        now=1000.0 + INTERVAL + 1,
    )
    assert coord.attempts == 2


def test_ca_present_with_no_pending_state_is_noop() -> None:
    fs = FakeFS({"/b": CA_B + CA_A}, {})
    coord = FakeCoord(running=True)
    out = reconcile(
        fs,
        coord,
        bundle_path="/b",
        site_packages_root="/sp",
        ca_pem=CA_A,
        state_path=STATE,
        min_retry_interval_s=INTERVAL,
        now=1000.0,
    )
    assert out == Outcome(
        bundle_found=True, appended=False, coordinator_restarted=False, bundle_path="/b"
    )
    assert out.reload_pending is False
    assert coord.attempts == 0
    assert fs.appended == []
    assert load_pending(fs, STATE) is None  # no spurious marker created


def test_non_host_appends_without_restart_and_creates_no_pending() -> None:
    fs = FakeFS({"/b": CA_B}, {})
    coord = FakeCoord(running=False)
    out = reconcile(
        fs,
        coord,
        bundle_path="/b",
        site_packages_root="/sp",
        ca_pem=CA_A,
        state_path=STATE,
        min_retry_interval_s=INTERVAL,
        now=1000.0,
    )
    assert out.appended is True
    assert out.coordinator_restarted is False
    assert out.reload_pending is False
    assert coord.attempts == 0
    assert load_pending(fs, STATE) is None  # non-host: nothing owed, no marker


def test_pending_cleared_after_success_then_noop() -> None:
    fs = FakeFS({"/b": CA_B}, {})
    coord = FakeCoord(running=True, fail_first=1)
    reconcile(
        fs,
        coord,
        bundle_path="/b",
        site_packages_root="/sp",
        ca_pem=CA_A,
        state_path=STATE,
        min_retry_interval_s=INTERVAL,
        now=1000.0,
    )
    reconcile(  # retry succeeds, clears the marker
        fs,
        coord,
        bundle_path="/b",
        site_packages_root="/sp",
        ca_pem=CA_A,
        state_path=STATE,
        min_retry_interval_s=INTERVAL,
        now=2000.0,
    )
    assert coord.successes == 1
    assert load_pending(fs, STATE) is None

    out = reconcile(  # clean no-op afterwards
        fs,
        coord,
        bundle_path="/b",
        site_packages_root="/sp",
        ca_pem=CA_A,
        state_path=STATE,
        min_retry_interval_s=INTERVAL,
        now=3000.0,
    )
    assert out.reload_pending is False
    assert coord.attempts == 2  # unchanged: no run-3 attempt


def test_stale_marker_for_other_generation_does_not_fire() -> None:
    # Bundle-replacement case: the bundle contains only CA_A, and the on-disk
    # marker is for CA_B which is NO LONGER in the bundle (its append was wiped,
    # e.g. by a firmware bump). That reload is genuinely moot -> no restart, and
    # the stale marker is cleared back to the sentinel.
    fs = FakeFS({"/b": CA_A}, {})
    coord = FakeCoord(running=True)
    save_pending(fs, STATE, PendingReload("/b", cert_fingerprint(CA_B), last_attempt_at=0.0))
    out = reconcile(
        fs,
        coord,
        bundle_path="/b",
        site_packages_root="/sp",
        ca_pem=CA_A,
        state_path=STATE,
        min_retry_interval_s=INTERVAL,
        now=10_000.0,
    )
    assert out.appended is False
    assert out.reload_pending is False
    assert coord.attempts == 0
    assert load_pending(fs, STATE) is None  # stale (replaced-away) marker cleared


# --- tribunal round 1: marker ordering, state-write survival, clock, staleness --


def test_interruption_in_append_marker_window_is_retried_by_fresh_run() -> None:
    # Cert absent, coordinator running. The pre-append marker write is
    # interrupted (hard kill). Because the marker is written BEFORE the append,
    # the cert is never appended, so a fresh run simply re-appends and reloads.
    fs = RaisingFS({"/b": CA_B}, {})
    coord = FakeCoord(running=True)
    fs.raise_next = KeyboardInterrupt()
    with pytest.raises(KeyboardInterrupt):
        reconcile(
            fs,
            coord,
            bundle_path="/b",
            site_packages_root="/sp",
            ca_pem=CA_A,
            state_path=STATE,
            min_retry_interval_s=INTERVAL,
            now=1000.0,
        )
    assert fs.appended == []  # died before the append (marker write comes first)
    assert coord.attempts == 0
    assert load_pending(fs, STATE) is None

    out = reconcile(  # fresh oneshot: cert still absent -> normal append + reload
        fs,
        coord,
        bundle_path="/b",
        site_packages_root="/sp",
        ca_pem=CA_A,
        state_path=STATE,
        min_retry_interval_s=INTERVAL,
        now=1001.0,
    )
    assert out.appended is True
    assert coord.attempts == 1 and coord.successes == 1
    assert load_pending(fs, STATE) is None  # cleared after the reload confirmed


def test_state_write_failure_on_first_append_does_not_skip_restart() -> None:
    # First append (cert absent, coordinator running): the pre-append marker write
    # hits a read-only/full /var (OSError). The reload is definitely owed and
    # there's no prior marker to loop on, so the restart MUST still be attempted
    # (round 1 finding #2 stands for the first-append path).
    fs = RaisingFS({"/b": CA_B}, {})
    coord = FakeCoord(running=True)
    fs.raise_next = OSError("read-only /var")  # fails the pre-append marker save
    out = reconcile(  # must NOT raise: the state-write OSError is swallowed
        fs,
        coord,
        bundle_path="/b",
        site_packages_root="/sp",
        ca_pem=CA_A,
        state_path=STATE,
        min_retry_interval_s=INTERVAL,
        now=1000.0,
    )
    assert out.appended is True
    assert coord.attempts == 1 and coord.successes == 1  # restart still attempted


@pytest.mark.parametrize("torn", ["", "garbage-not-json", '{"bundle_path": "/b"}'])
def test_cert_present_torn_marker_requests_reload(torn: str) -> None:
    # Cert already present but the marker is torn/garbled (not the clean "{}"
    # sentinel). A reload may be owed, so it must be requested, not a silent
    # no-op.
    fs = FakeFS({"/b": CA_B + CA_A, STATE: torn}, {})
    coord = FakeCoord(running=True)
    out = reconcile(
        fs,
        coord,
        bundle_path="/b",
        site_packages_root="/sp",
        ca_pem=CA_A,
        state_path=STATE,
        min_retry_interval_s=INTERVAL,
        now=5000.0,
    )
    assert coord.attempts == 1  # reload requested despite the unreadable marker
    assert out.coordinator_restarted is True
    assert load_pending(fs, STATE) is None  # cleared after the successful reload


def test_cert_present_cleared_sentinel_is_noop() -> None:
    # The clean "{}" sentinel means nothing is owed -> no restart storm.
    fs = FakeFS({"/b": CA_B + CA_A, STATE: "{}"}, {})
    coord = FakeCoord(running=True)
    out = reconcile(
        fs,
        coord,
        bundle_path="/b",
        site_packages_root="/sp",
        ca_pem=CA_A,
        state_path=STATE,
        min_retry_interval_s=INTERVAL,
        now=5000.0,
    )
    assert coord.attempts == 0
    assert out.reload_pending is False


def test_future_clock_marker_is_not_skipped() -> None:
    # Marker stamped in the future (panels have no reliable RTC; NTP can step the
    # clock back after boot). Negative elapsed must count as "interval up", not
    # pace the owed reload away forever.
    fs = FakeFS({"/b": CA_B + CA_A}, {})
    save_pending(fs, STATE, PendingReload("/b", cert_fingerprint(CA_A), last_attempt_at=5000.0))
    coord = FakeCoord(running=True)
    out = reconcile(
        fs,
        coord,
        bundle_path="/b",
        site_packages_root="/sp",
        ca_pem=CA_A,
        state_path=STATE,
        min_retry_interval_s=INTERVAL,
        now=1000.0,  # BEFORE the marker's stamp
    )
    assert coord.attempts == 1  # not skipped
    assert out.coordinator_restarted is True


def test_matching_marker_but_coordinator_stopped_clears_marker() -> None:
    # A matching marker but the coordinator isn't running (not the Hue host):
    # nothing is owed, so the stale marker is dropped (it must not fire a restart
    # if the vassal later returns).
    fs = FakeFS({"/b": CA_B + CA_A}, {})
    save_pending(fs, STATE, PendingReload("/b", cert_fingerprint(CA_A), last_attempt_at=0.0))
    coord = FakeCoord(running=False)
    out = reconcile(
        fs,
        coord,
        bundle_path="/b",
        site_packages_root="/sp",
        ca_pem=CA_A,
        state_path=STATE,
        min_retry_interval_s=INTERVAL,
        now=10_000.0,
    )
    assert coord.attempts == 0
    assert out.reload_pending is False
    assert load_pending(fs, STATE) is None  # stale marker cleared


# --- tribunal round 2: read crash, restart storm, torn non-host, orphaned path --


def test_unreadable_state_file_does_not_crash_and_requests_reload() -> None:
    # finding #1: an OSError reading an EXISTING marker (EIO/EACCES) must not
    # crash the oneshot; it is treated as torn -> reload requested.
    fs = ReadFailFS({"/b": CA_B + CA_A, STATE: "x"}, {}, fail_read=STATE)
    coord = FakeCoord(running=True)
    out = reconcile(  # must NOT raise
        fs,
        coord,
        bundle_path="/b",
        site_packages_root="/sp",
        ca_pem=CA_A,
        state_path=STATE,
        min_retry_interval_s=INTERVAL,
        now=5000.0,
    )
    assert coord.attempts == 1
    assert out.coordinator_restarted is True


@pytest.mark.parametrize("seed_torn", [None, "", "garbage-not-json"])
def test_no_restart_storm_when_state_dir_unwritable(seed_torn: str | None) -> None:
    # finding #2: a persistently unwritable state dir with an existing marker
    # (valid or torn) for the current generation must NOT restart on every tick.
    fs = UnwritableFS({"/b": CA_B + CA_A}, {})
    if seed_torn is None:
        save_pending(fs, STATE, PendingReload("/b", cert_fingerprint(CA_A), last_attempt_at=0.0))
    else:
        fs.files[STATE] = seed_torn  # torn marker seeded directly
    fs.raising = True  # /var now read-only for every write
    coord = FakeCoord(running=True)
    for i in range(4):  # four ticks, well past the interval each time
        reconcile(
            fs,
            coord,
            bundle_path="/b",
            site_packages_root="/sp",
            ca_pem=CA_A,
            state_path=STATE,
            min_retry_interval_s=INTERVAL,
            now=900.0 * (i + 1),
        )
    assert coord.successes <= 1  # no restart storm


def test_torn_marker_on_non_host_is_cleared_not_warned_forever() -> None:
    # finding #3: a torn marker on a non-host (coordinator not running) owes no
    # reload — clear it so it doesn't warn every tick forever.
    fs = FakeFS({"/b": CA_B + CA_A, STATE: "garbage"}, {})
    coord = FakeCoord(running=False)
    out = reconcile(
        fs,
        coord,
        bundle_path="/b",
        site_packages_root="/sp",
        ca_pem=CA_A,
        state_path=STATE,
        min_retry_interval_s=INTERVAL,
        now=5000.0,
    )
    assert out.reload_pending is False
    assert coord.attempts == 0
    assert fs.files[STATE] == "{}"  # torn marker cleared to the sentinel


def test_marker_with_stale_bundle_path_but_matching_fp_is_not_orphaned() -> None:
    # finding #5: a marker whose bundle_path differs from the path resolved this
    # run but whose fingerprint matches the CA in the bundle (operator changed
    # HUE_CA_BUNDLE_PATH / glob flip) must not be a permanent orphan.
    fs = FakeFS({"/b": CA_B + CA_A}, {})
    save_pending(fs, STATE, PendingReload("/old", cert_fingerprint(CA_A), last_attempt_at=0.0))
    coord = FakeCoord(running=True)
    reconcile(
        fs,
        coord,
        bundle_path="/b",
        site_packages_root="/sp",
        ca_pem=CA_A,
        state_path=STATE,
        min_retry_interval_s=INTERVAL,
        now=10_000.0,
    )
    assert coord.attempts == 1  # reload owed for the CA now present
    assert load_pending(fs, STATE) is None  # marker cleared after the reload


# --- tribunal round 3: unrecordable owed reload, retired-CA marker cleanup -----


def test_first_append_unrecordable_reload_is_not_falsely_retryable() -> None:
    # finding #1: first append with an unwritable state dir AND a failed restart
    # -> the reload is owed but NO marker persists, so nothing can drive a retry.
    # reconcile must report marker_persisted=False (so run_once can flag it), not
    # a false "will retry".
    fs = UnwritableFS({"/b": CA_B}, {})
    fs.raising = True  # state dir read-only from the start
    coord = FakeCoord(running=True, fail_first=1)  # restart fails this run
    out = reconcile(
        fs,
        coord,
        bundle_path="/b",
        site_packages_root="/sp",
        ca_pem=CA_A,
        state_path=STATE,
        min_retry_interval_s=INTERVAL,
        now=1000.0,
    )
    assert out.appended is True
    assert out.reload_pending is True
    assert out.marker_persisted is False  # marker could not be recorded
    assert load_pending(fs, STATE) is None  # nothing on disk to drive a retry

    # A fresh run cannot resurrect it (documents the limitation): cert now present
    # but no marker exists, so it's a silent healthy no-op.
    out2 = reconcile(
        fs,
        coord,
        bundle_path="/b",
        site_packages_root="/sp",
        ca_pem=CA_A,
        state_path=STATE,
        min_retry_interval_s=INTERVAL,
        now=2000.0,
    )
    assert out2.reload_pending is False
    assert out2.coordinator_restarted is False


def test_first_append_owed_reload_is_retryable_when_state_dir_writable() -> None:
    # The contrast to the case above: a writable state dir records the marker, so
    # marker_persisted stays True and a fresh run genuinely retries.
    fs = FakeFS({"/b": CA_B}, {})
    coord = FakeCoord(running=True, fail_first=1)
    out = reconcile(
        fs,
        coord,
        bundle_path="/b",
        site_packages_root="/sp",
        ca_pem=CA_A,
        state_path=STATE,
        min_retry_interval_s=INTERVAL,
        now=1000.0,
    )
    assert out.reload_pending is True
    assert out.marker_persisted is True
    assert load_pending(fs, STATE) is not None  # marker recorded -> retryable

    out2 = reconcile(
        fs,
        coord,
        bundle_path="/b",
        site_packages_root="/sp",
        ca_pem=CA_A,
        state_path=STATE,
        min_retry_interval_s=INTERVAL,
        now=2000.0,
    )
    assert out2.coordinator_restarted is True  # fresh run completed the reload


# --- tribunal round 4: a marker is owed while its CA is still in the bundle ----


def test_ca_rotation_under_failing_restarts_still_reloads() -> None:
    # finding (round 4): CA rotation while restarts fail must not lose the owed
    # reload. run1 appends A (restart fails, marker A); run2 appends B (restart
    # fails, marker B); run3 with CA_A and a WORKING coordinator sees marker B —
    # but B is still in the bundle, so a reload IS still owed and must fire.
    fs = FakeFS({"/b": ""}, {})  # empty bundle to start
    reconcile(  # run1: append A, restart fails -> marker A
        fs,
        FakeCoord(running=True, fail_first=1),
        bundle_path="/b",
        site_packages_root="/sp",
        ca_pem=CA_A,
        state_path=STATE,
        min_retry_interval_s=INTERVAL,
        now=1000.0,
    )
    reconcile(  # run2: append B, restart fails -> marker B overwrites marker A
        fs,
        FakeCoord(running=True, fail_first=1),
        bundle_path="/b",
        site_packages_root="/sp",
        ca_pem=CA_B,
        state_path=STATE,
        min_retry_interval_s=INTERVAL,
        now=2000.0,
    )
    coord3 = FakeCoord(running=True)  # fresh oneshot, working coordinator
    reconcile(  # run3: CA_A present, marker B still in bundle -> reload owed
        fs,
        coord3,
        bundle_path="/b",
        site_packages_root="/sp",
        ca_pem=CA_A,
        state_path=STATE,
        min_retry_interval_s=INTERVAL,
        now=3000.0,
    )
    assert coord3.attempts == 1  # the reload finally fired
    assert coord3.successes == 1
    assert CA_A.strip() in fs.files["/b"] and CA_B.strip() in fs.files["/b"]
    assert load_pending(fs, STATE) is None  # cleared after the successful reload


def test_marker_for_earlier_ca_still_in_bundle_is_owed() -> None:
    # One-run variant: bundle contains both A and B, marker is for CA_B, this run
    # carries CA_A. CA_B is still present, so the reload is still owed -> restart.
    fs = FakeFS({"/b": CA_A + CA_B}, {})
    save_pending(fs, STATE, PendingReload("/b", cert_fingerprint(CA_B), last_attempt_at=0.0))
    coord = FakeCoord(running=True)
    reconcile(
        fs,
        coord,
        bundle_path="/b",
        site_packages_root="/sp",
        ca_pem=CA_A,
        state_path=STATE,
        min_retry_interval_s=INTERVAL,
        now=10_000.0,
    )
    assert coord.attempts == 1


def test_marker_ca_replaced_away_is_cleared_as_stale() -> None:
    # The legitimate stale case: the marker's CA (CA_B) is NOT in the bundle (it
    # was replaced away), so its reload is moot -> no restart, marker cleared.
    fs = FakeFS({"/b": CA_A}, {})
    save_pending(fs, STATE, PendingReload("/b", cert_fingerprint(CA_B), last_attempt_at=0.0))
    coord = FakeCoord(running=True)
    out = reconcile(
        fs,
        coord,
        bundle_path="/b",
        site_packages_root="/sp",
        ca_pem=CA_A,
        state_path=STATE,
        min_retry_interval_s=INTERVAL,
        now=10_000.0,
    )
    assert coord.attempts == 0
    assert out.reload_pending is False
    assert load_pending(fs, STATE) is None  # stale marker cleared
