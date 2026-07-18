from __future__ import annotations

import pytest

from brilliant_hue_ca.run import run_once

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

    def glob(self, root: str, name: str) -> str | None:
        return None


class FakeCoord:
    def __init__(self) -> None:
        self.restarted = False

    def is_running(self) -> bool:
        return False

    def restart(self) -> None:
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
