from __future__ import annotations

import pytest

from brilliant_hue_ca.run import run_once
from brilliant_hue_ca.state import load_pending

# PEM_cert_to_DER_cert only base64-decodes the body (no DER/X.509 structure
# validation), so a valid-base64 placeholder is enough here: run_once/reconcile
# only need cert_fingerprint() to succeed, not a cryptographically real cert
# (unlike tests/test_hue_ca_reconcile.py, which does need real x.509 fixtures).
CA = "-----BEGIN CERTIFICATE-----\nWA==\n-----END CERTIFICATE-----\n"


class FakeFS:
    def __init__(self, exists_map: dict[str, bool], files: dict[str, str]) -> None:
        self._exists = exists_map
        self._files = files
        self.appended: list[tuple[str, str]] = []

    def exists(self, path: str) -> bool:
        return self._exists.get(path, False)

    def read_text(self, path: str) -> str:
        return self._files[path]

    def append_text(self, path: str, text: str) -> None:
        self.appended.append((path, text))

    def write_text(self, path: str, text: str) -> None:
        self._files[path] = text
        self._exists[path] = True  # keep exists() consistent with writes

    def glob(self, root: str, name: str) -> str | None:
        return None


class FakeCoord:
    def __init__(self, running: bool = False, fail: bool = False) -> None:
        self._running = running
        self._fail = fail
        self.restarted = False
        self.attempts = 0

    def is_running(self) -> bool:
        return self._running

    def restart(self) -> None:
        self.attempts += 1
        if self._fail:
            raise OSError("simulated transient vassal-touch failure")
        self.restarted = True


def test_run_once_returns_zero_on_success(monkeypatch: pytest.MonkeyPatch) -> None:
    # CA present at the bundle path -> no-op -> exit 0
    fs = FakeFS({"/b": True}, {"/b": CA})
    rc = run_once(
        {"HUE_CA_BUNDLE_PATH": "/b"},
        fs=fs,
        coordinator=FakeCoord(),
        read_ca=lambda _p: CA,
    )
    assert rc == 0


def test_run_once_returns_one_when_ca_unreadable() -> None:
    def boom(_p: str) -> str:
        raise OSError("no ca")

    fs = FakeFS({"/b": True}, {"/b": CA})
    rc = run_once({"HUE_CA_BUNDLE_PATH": "/b"}, fs=fs, coordinator=FakeCoord(), read_ca=boom)
    assert rc == 1


def test_run_once_returns_zero_when_bundle_absent() -> None:
    fs = FakeFS({}, {})
    rc = run_once(
        {"HUE_CA_BUNDLE_PATH": "/missing"},
        fs=fs,
        coordinator=FakeCoord(),
        read_ca=lambda _p: CA,
    )
    # bundle-not-found is non-fatal (timer retries) -> exit 0
    assert rc == 0


def test_run_once_returns_one_when_ca_empty() -> None:
    # read_ca succeeds (file is readable) but returns an empty string -> the
    # PEM is unparseable, so cert_fingerprint() must raise ValueError, which
    # run_once needs to catch and turn into a logged, non-zero exit (not an
    # uncaught traceback).
    fs = FakeFS({"/b": True}, {"/b": CA})
    rc = run_once(
        {"HUE_CA_BUNDLE_PATH": "/b"},
        fs=fs,
        coordinator=FakeCoord(),
        read_ca=lambda _p: "",
    )
    assert rc == 1


def test_run_once_returns_one_when_ca_corrupt() -> None:
    # Readable, non-empty, but not valid base64 in the PEM body -> binascii.Error
    # (a ValueError subclass) from ssl.PEM_cert_to_DER_cert.
    corrupt = "-----BEGIN CERTIFICATE-----\nnot-valid-base64!!!\n-----END CERTIFICATE-----\n"
    fs = FakeFS({"/b": True}, {"/b": CA})
    rc = run_once(
        {"HUE_CA_BUNDLE_PATH": "/b"},
        fs=fs,
        coordinator=FakeCoord(),
        read_ca=lambda _p: corrupt,
    )
    assert rc == 1


def test_run_once_returns_zero_when_restart_fails_and_marks_pending() -> None:
    # Cert absent, coordinator running, but restart raises OSError. A caught,
    # self-healing restart failure must NOT be conflated with the fatal
    # bundle-write path: run_once returns 0 and leaves a durable marker so the
    # next timer run retries.
    fs = FakeFS({"/b": True}, {"/b": ""})  # empty bundle -> CA is absent
    coord = FakeCoord(running=True, fail=True)
    rc = run_once(
        {"HUE_CA_BUNDLE_PATH": "/b", "HUE_CA_STATE_PATH": "/state.json"},
        fs=fs,
        coordinator=coord,
        read_ca=lambda _p: CA,
    )
    assert rc == 0
    assert coord.attempts == 1
    assert fs.appended  # the CA was appended
    assert load_pending(fs, "/state.json") is not None  # reload owed, persisted


def test_run_once_completes_reload_and_clears_pending_when_host_running() -> None:
    # Cert absent, coordinator running, restart succeeds -> reload confirmed,
    # marker cleared, exit 0. Proves state_path threads through run_once.
    fs = FakeFS({"/b": True}, {"/b": ""})
    coord = FakeCoord(running=True, fail=False)
    rc = run_once(
        {"HUE_CA_BUNDLE_PATH": "/b", "HUE_CA_STATE_PATH": "/state.json"},
        fs=fs,
        coordinator=coord,
        read_ca=lambda _p: CA,
    )
    assert rc == 0
    assert coord.restarted is True
    assert load_pending(fs, "/state.json") is None  # cleared on success
