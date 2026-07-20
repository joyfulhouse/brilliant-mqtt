from brilliant_hue_ca.reconcile import (
    Outcome,
    cert_fingerprint,
    reconcile,
    split_pem_certs,
)

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

    def glob(self, root: str, name: str) -> str | None:
        return self.globs.get((root, name))


class FakeCoord:
    def __init__(self, running: bool) -> None:
        self._running = running
        self.restarted = False

    def is_running(self) -> bool:
        return self._running

    def restart(self) -> None:
        self.restarted = True


def test_fingerprint_matches_across_rewrapped_pem() -> None:
    assert cert_fingerprint(CA_A) == cert_fingerprint(CA_A_REWRAPPED)
    assert cert_fingerprint(CA_A) != cert_fingerprint(CA_B)


def test_split_pem_certs_counts_blocks() -> None:
    assert len(split_pem_certs(CA_A + CA_B)) == 2
    assert split_pem_certs("no certs here") == []


def test_ca_absent_and_host_running_appends_and_restarts() -> None:
    fs = FakeFS({"/b": CA_B}, {})
    coord = FakeCoord(running=True)
    out = reconcile(fs, coord, bundle_path="/b", site_packages_root="/sp", ca_pem=CA_A)
    assert out == Outcome(
        bundle_found=True, appended=True, coordinator_restarted=True, bundle_path="/b"
    )
    assert fs.appended and CA_A.strip() in fs.files["/b"]
    assert coord.restarted is True


def test_ca_absent_and_not_host_appends_without_restart() -> None:
    fs = FakeFS({"/b": CA_B}, {})
    coord = FakeCoord(running=False)
    out = reconcile(fs, coord, bundle_path="/b", site_packages_root="/sp", ca_pem=CA_A)
    assert out.appended is True
    assert out.coordinator_restarted is False
    assert coord.restarted is False


def test_ca_present_is_noop() -> None:
    fs = FakeFS({"/b": CA_B + CA_A}, {})
    coord = FakeCoord(running=True)
    out = reconcile(fs, coord, bundle_path="/b", site_packages_root="/sp", ca_pem=CA_A)
    assert out == Outcome(
        bundle_found=True, appended=False, coordinator_restarted=False, bundle_path="/b"
    )
    assert fs.appended == []
    assert coord.restarted is False


def test_ca_present_even_when_rewrapped_is_noop() -> None:
    fs = FakeFS({"/b": CA_A_REWRAPPED}, {})
    coord = FakeCoord(running=True)
    out = reconcile(fs, coord, bundle_path="/b", site_packages_root="/sp", ca_pem=CA_A)
    assert out.appended is False


def test_bundle_missing_at_path_uses_glob() -> None:
    fs = FakeFS(
        {"/sp/lib/certs/hue-bridge-ca-certs.pem": CA_B},
        {("/sp", "hue-bridge-ca-certs.pem"): "/sp/lib/certs/hue-bridge-ca-certs.pem"},
    )
    coord = FakeCoord(running=False)
    out = reconcile(fs, coord, bundle_path="/missing", site_packages_root="/sp", ca_pem=CA_A)
    assert out.bundle_found is True
    assert out.bundle_path == "/sp/lib/certs/hue-bridge-ca-certs.pem"
    assert out.appended is True


def test_bundle_not_found_anywhere() -> None:
    fs = FakeFS({}, {})
    coord = FakeCoord(running=True)
    out = reconcile(fs, coord, bundle_path="/missing", site_packages_root="/sp", ca_pem=CA_A)
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
    out = reconcile(fs, coord, bundle_path="/b", site_packages_root="/sp", ca_pem=CA_A)
    assert out.appended is True  # CA_A not present -> appended; garbage block skipped
