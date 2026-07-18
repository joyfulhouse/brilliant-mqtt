# diyHue CA-Recovery Hook Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the panel's injected diyHue CA survive firmware OTA by having the `/var`-resident agent re-append it to the pinned Hue bundle and restart the local Hue coordinator, deployable per-panel via an optional integration component.

**Architecture:** An on-panel oneshot Python package (`brilliant_hue_ca`) run by a systemd timer performs one idempotent reconcile (locate bundle → ensure our CA present by fingerprint → restart the local Hue coordinator only if it just re-appended). A new optional, off-by-default `hue-ca` integration component SSH-deploys the package, the operator's CA cert, and the systemd units, following the existing watchdog-component pattern.

**Tech Stack:** Python 3.10 (agent, stdlib only — `ssl`, `hashlib`, `os`); Python 3.14 (integration, Home Assistant); systemd oneshot + timer; pytest; ruff; mypy --strict.

## Global Constraints

- **Agent is Python 3.10, stdlib only.** No new runtime deps; fingerprinting uses `ssl.PEM_cert_to_DER_cert` + `hashlib.sha256`. `requires-python = ">=3.10,<3.11"`.
- **Integration is separate (Python 3.14, under `ha/`).** Its code lives at repo root `custom_components/brilliant_mqtt/`; its tests in `ha/tests/`.
- **Never disable linters** (`# noqa`, `# type: ignore`). Fix the root cause.
- **Never import `lib.message_bus_api` outside `src/brilliant_mqtt/bus.py`.** This feature does not touch the message bus at all (it is filesystem + systemd only) — no bus imports anywhere.
- **The agent suite must run off-panel.** All panel side effects sit behind Protocols with fakes in tests.
- **CA is operator-specific — never hardcode a CA in the repo.** The integration supplies the CA PEM via a config field; the agent reads it from a file path.
- **CA-present match is by fingerprint** (SHA-256 of DER), never by subject/CN.
- **Restart only when the CA was actually (re)appended.** Steady state (CA present) is a pure no-op — no restart — so no restart-loop is possible.
- **Agent pre-commit gate:** `uv run ruff check --fix && uv run ruff format && uv run mypy --strict src tests && uv run pytest`
- **Integration pre-commit gate:** `uv run --project ha ruff check --fix --config ha/pyproject.toml custom_components/brilliant_mqtt ha/tests && uv run --project ha ruff format --config ha/pyproject.toml custom_components/brilliant_mqtt ha/tests && uv run --project ha mypy --strict --config-file ha/pyproject.toml custom_components/brilliant_mqtt ha/tests && uv run --project ha pytest -c ha/pyproject.toml ha/tests`

## File Structure

**Agent (`src/brilliant_hue_ca/`, Python 3.10):**
- `__init__.py` — package marker + `__all__`.
- `config.py` — `Config` dataclass + `load_config(environ)`.
- `fs.py` — `FileSystem` Protocol + `RealFileSystem` (read/append/exists/glob).
- `coordinator.py` — `Coordinator` Protocol + `RealCoordinator` (vassal-ini presence + touch-to-restart).
- `reconcile.py` — `Outcome` dataclass, `cert_fingerprint`, `split_pem_certs`, `reconcile`.
- `run.py` — thin `main()`.

**Deploy / packaging:**
- `deploy/brilliant-hue-ca.service` — `Type=oneshot`, no `[Install]`.
- `deploy/brilliant-hue-ca.timer` — `OnBootSec` + `OnUnitActiveSec`.
- `scripts/build_payload.sh` — copy `brilliant_hue_ca` into `agent_payload/hue_ca/` + the two units.
- `pyproject.toml` — add `src/brilliant_hue_ca` to wheel packages.

**Agent tests (`tests/`):**
- `test_hue_ca_config.py`, `test_hue_ca_fs.py`, `test_hue_ca_coordinator.py`, `test_hue_ca_reconcile.py`, `test_hue_ca_run.py`.

**Integration (`custom_components/brilliant_mqtt/`, Python 3.14):**
- `const.py` — `COMPONENT_HUE_CA`, `CONF_HUE_CA_CERT`, panel path/unit constants.
- `panel_ops.py` — `HueCaState`, `inspect_hue_ca`, `deploy_hue_ca`, `ensure_hue_ca_units`, `enable_hue_ca`, `uninstall_hue_ca`.
- `components.py` — `_hue_ca_present`, `_hue_ca_install`, `REGISTRY` entry.
- `config_flow.py` — add `CONF_HUE_CA_CERT` sub-field to `_components_schema_fields`.

**Integration tests (`ha/tests/`):**
- `test_hue_ca_component.py`.

**Docs:**
- `docs/CONFIGURATION.md`, `deploy/README.md`.

---

### Task 1: Agent boundaries + config (`fs.py`, `coordinator.py`, `config.py`)

**Files:**
- Create: `src/brilliant_hue_ca/__init__.py`
- Create: `src/brilliant_hue_ca/fs.py`
- Create: `src/brilliant_hue_ca/coordinator.py`
- Create: `src/brilliant_hue_ca/config.py`
- Test: `tests/test_hue_ca_fs.py`, `tests/test_hue_ca_coordinator.py`, `tests/test_hue_ca_config.py`

**Interfaces:**
- Produces:
  - `FileSystem` Protocol: `exists(path: str) -> bool`, `read_text(path: str) -> str`, `append_text(path: str, text: str) -> None`, `glob(root: str, name: str) -> str | None`. Real impl `RealFileSystem`.
  - `Coordinator` Protocol: `is_running() -> bool`, `restart() -> None`. Real impl `RealCoordinator(vassal_ini_path: str)`.
  - `Config` dataclass (frozen) with fields `ca_cert_path`, `bundle_path`, `site_packages_root`, `vassal_ini_path`, `log_path` (all `str`); `load_config(environ: Mapping[str, str]) -> Config`.

- [ ] **Step 1: Write the failing tests**

`tests/test_hue_ca_config.py`:
```python
from brilliant_hue_ca.config import Config, load_config


def test_load_config_defaults():
    cfg = load_config({})
    assert isinstance(cfg, Config)
    assert cfg.ca_cert_path == "/var/brilliant-hue-ca/injected-ca.pem"
    assert cfg.bundle_path.endswith("lib/certs/hue-bridge-ca-certs.pem")
    assert cfg.site_packages_root.endswith("site-packages")
    assert cfg.vassal_ini_path.endswith("processes/hue_bridge_peripherals.ini")


def test_load_config_overrides():
    cfg = load_config(
        {
            "HUE_CA_CERT_PATH": "/x/ca.pem",
            "HUE_CA_BUNDLE_PATH": "/x/bundle.pem",
            "HUE_CA_SITE_PACKAGES": "/x/sp",
            "HUE_CA_VASSAL_INI": "/x/v.ini",
            "HUE_CA_LOG": "/x/log",
        }
    )
    assert cfg.ca_cert_path == "/x/ca.pem"
    assert cfg.bundle_path == "/x/bundle.pem"
    assert cfg.site_packages_root == "/x/sp"
    assert cfg.vassal_ini_path == "/x/v.ini"
    assert cfg.log_path == "/x/log"
```

`tests/test_hue_ca_fs.py`:
```python
from brilliant_hue_ca.fs import RealFileSystem


def test_real_fs_read_append_exists(tmp_path):
    fs = RealFileSystem()
    p = tmp_path / "bundle.pem"
    p.write_text("A\n")
    assert fs.exists(str(p)) is True
    assert fs.exists(str(tmp_path / "nope")) is False
    assert fs.read_text(str(p)) == "A\n"
    fs.append_text(str(p), "B\n")
    assert fs.read_text(str(p)) == "A\nB\n"


def test_real_fs_glob_finds_nested(tmp_path):
    fs = RealFileSystem()
    nested = tmp_path / "a" / "b" / "certs"
    nested.mkdir(parents=True)
    (nested / "hue-bridge-ca-certs.pem").write_text("X")
    found = fs.glob(str(tmp_path), "hue-bridge-ca-certs.pem")
    assert found == str(nested / "hue-bridge-ca-certs.pem")
    assert fs.glob(str(tmp_path), "absent.pem") is None
```

`tests/test_hue_ca_coordinator.py`:
```python
from brilliant_hue_ca.coordinator import RealCoordinator


def test_coordinator_running_reflects_ini_presence(tmp_path):
    ini = tmp_path / "hue_bridge_peripherals.ini"
    coord = RealCoordinator(str(ini))
    assert coord.is_running() is False
    ini.write_text("")
    assert coord.is_running() is True


def test_coordinator_restart_touches_ini(tmp_path):
    ini = tmp_path / "hue_bridge_peripherals.ini"
    ini.write_text("")
    import os

    os.utime(str(ini), (1000, 1000))
    before = os.stat(str(ini)).st_mtime
    coord = RealCoordinator(str(ini))
    coord.restart()
    after = os.stat(str(ini)).st_mtime
    assert after > before
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_hue_ca_config.py tests/test_hue_ca_fs.py tests/test_hue_ca_coordinator.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'brilliant_hue_ca'`.

- [ ] **Step 3: Write the implementations**

`src/brilliant_hue_ca/__init__.py`:
```python
"""On-panel oneshot that keeps the injected diyHue CA in the pinned Hue bundle
across firmware OTA and restarts the local Hue coordinator when it re-appends."""

from __future__ import annotations

__all__ = ["config", "coordinator", "fs", "reconcile", "run"]
```

`src/brilliant_hue_ca/fs.py`:
```python
"""Filesystem boundary. Real impl is stdlib-only; tests fake the Protocol."""

from __future__ import annotations

import os
from typing import Protocol


class FileSystem(Protocol):
    def exists(self, path: str) -> bool: ...
    def read_text(self, path: str) -> str: ...
    def append_text(self, path: str, text: str) -> None: ...
    def glob(self, root: str, name: str) -> str | None: ...


class RealFileSystem:
    def exists(self, path: str) -> bool:
        return os.path.exists(path)

    def read_text(self, path: str) -> str:
        with open(path, encoding="utf-8") as f:
            return f.read()

    def append_text(self, path: str, text: str) -> None:
        with open(path, "a", encoding="utf-8") as f:
            f.write(text)

    def glob(self, root: str, name: str) -> str | None:
        for dirpath, _dirs, files in os.walk(root):
            if name in files:
                return os.path.join(dirpath, name)
        return None
```

`src/brilliant_hue_ca/coordinator.py`:
```python
"""Hue-coordinator control boundary. The coordinator is a uWSGI emperor vassal;
its presence is signalled by the vassal control file, and touching that file
triggers an emperor reload (restart). This panel hosts Hue only when the file
exists, so is_running() doubles as "am I the current Hue host"."""

from __future__ import annotations

import os
from typing import Protocol


class Coordinator(Protocol):
    def is_running(self) -> bool: ...
    def restart(self) -> None: ...


class RealCoordinator:
    def __init__(self, vassal_ini_path: str) -> None:
        self._ini = vassal_ini_path

    def is_running(self) -> bool:
        return os.path.exists(self._ini)

    def restart(self) -> None:
        os.utime(self._ini, None)
```

`src/brilliant_hue_ca/config.py`:
```python
"""Config from the environment (same idiom as the watchdog daemons)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

_DEFAULT_SP = "/data/switch-embedded/env/lib/python3.10/site-packages"


@dataclass(frozen=True)
class Config:
    ca_cert_path: str
    bundle_path: str
    site_packages_root: str
    vassal_ini_path: str
    log_path: str


def load_config(environ: Mapping[str, str]) -> Config:
    def s(key: str, default: str) -> str:
        return environ.get(key, default)

    return Config(
        ca_cert_path=s("HUE_CA_CERT_PATH", "/var/brilliant-hue-ca/injected-ca.pem"),
        bundle_path=s(
            "HUE_CA_BUNDLE_PATH", f"{_DEFAULT_SP}/lib/certs/hue-bridge-ca-certs.pem"
        ),
        site_packages_root=s("HUE_CA_SITE_PACKAGES", _DEFAULT_SP),
        vassal_ini_path=s(
            "HUE_CA_VASSAL_INI",
            "/var/run/brilliant/processes/hue_bridge_peripherals.ini",
        ),
        log_path=s("HUE_CA_LOG", "/var/log/brilliant-hue-ca.log"),
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_hue_ca_config.py tests/test_hue_ca_fs.py tests/test_hue_ca_coordinator.py -v`
Expected: PASS (7 tests).

- [ ] **Step 5: Run the agent gate + commit**

Run: `uv run ruff check --fix && uv run ruff format && uv run mypy --strict src tests && uv run pytest`
Expected: all green.
```bash
git add src/brilliant_hue_ca/__init__.py src/brilliant_hue_ca/fs.py src/brilliant_hue_ca/coordinator.py src/brilliant_hue_ca/config.py tests/test_hue_ca_config.py tests/test_hue_ca_fs.py tests/test_hue_ca_coordinator.py
git commit -m "feat(hue-ca): agent boundaries + config"
```

---

### Task 2: Reconcile core (`reconcile.py`)

**Files:**
- Create: `src/brilliant_hue_ca/reconcile.py`
- Test: `tests/test_hue_ca_reconcile.py`

**Interfaces:**
- Consumes: `FileSystem`, `Coordinator` Protocols (Task 1).
- Produces:
  - `Outcome` dataclass (frozen): `bundle_found: bool`, `appended: bool`, `coordinator_restarted: bool`, `bundle_path: str | None`.
  - `cert_fingerprint(pem: str) -> str` — SHA-256 hex of the cert's DER.
  - `split_pem_certs(text: str) -> list[str]` — individual `BEGIN/END CERTIFICATE` blocks.
  - `reconcile(fs: FileSystem, coordinator: Coordinator, *, bundle_path: str, site_packages_root: str, ca_pem: str) -> Outcome`.

- [ ] **Step 1: Write the failing tests**

`tests/test_hue_ca_reconcile.py`:
```python
import ssl

from brilliant_hue_ca.reconcile import (
    Outcome,
    cert_fingerprint,
    reconcile,
    split_pem_certs,
)

# Two distinct self-signed EC certs generated once and pasted as fixtures.
# CA_A and CA_B have DIFFERENT keys (different fingerprints). CA_A_REWRAPPED is
# CA_A re-emitted with different line wrapping (same DER, same fingerprint).
CA_A = """-----BEGIN CERTIFICATE-----
MIIBIjCBygIJAK... (full PEM here) ...
-----END CERTIFICATE-----
"""
CA_B = """-----BEGIN CERTIFICATE-----
MIIBIjCBygIJAKdifferentkey... (full PEM here) ...
-----END CERTIFICATE-----
"""
CA_A_REWRAPPED = "-----BEGIN CERTIFICATE-----\n" + "".join(
    CA_A.split("-----")[2].split()
) + "\n-----END CERTIFICATE-----\n"


class FakeFS:
    def __init__(self, files: dict[str, str], globs: dict[tuple[str, str], str | None]):
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
    def __init__(self, running: bool):
        self._running = running
        self.restarted = False

    def is_running(self) -> bool:
        return self._running

    def restart(self) -> None:
        self.restarted = True


def test_fingerprint_matches_across_rewrapped_pem():
    assert cert_fingerprint(CA_A) == cert_fingerprint(CA_A_REWRAPPED)
    assert cert_fingerprint(CA_A) != cert_fingerprint(CA_B)


def test_split_pem_certs_counts_blocks():
    assert len(split_pem_certs(CA_A + CA_B)) == 2
    assert split_pem_certs("no certs here") == []


def test_ca_absent_and_host_running_appends_and_restarts():
    fs = FakeFS({"/b": CA_B}, {})
    coord = FakeCoord(running=True)
    out = reconcile(fs, coord, bundle_path="/b", site_packages_root="/sp", ca_pem=CA_A)
    assert out == Outcome(
        bundle_found=True, appended=True, coordinator_restarted=True, bundle_path="/b"
    )
    assert fs.appended and CA_A.strip() in fs.files["/b"]
    assert coord.restarted is True


def test_ca_absent_and_not_host_appends_without_restart():
    fs = FakeFS({"/b": CA_B}, {})
    coord = FakeCoord(running=False)
    out = reconcile(fs, coord, bundle_path="/b", site_packages_root="/sp", ca_pem=CA_A)
    assert out.appended is True
    assert out.coordinator_restarted is False
    assert coord.restarted is False


def test_ca_present_is_noop():
    fs = FakeFS({"/b": CA_B + CA_A}, {})
    coord = FakeCoord(running=True)
    out = reconcile(fs, coord, bundle_path="/b", site_packages_root="/sp", ca_pem=CA_A)
    assert out == Outcome(
        bundle_found=True, appended=False, coordinator_restarted=False, bundle_path="/b"
    )
    assert fs.appended == []
    assert coord.restarted is False


def test_ca_present_even_when_rewrapped_is_noop():
    fs = FakeFS({"/b": CA_A_REWRAPPED}, {})
    coord = FakeCoord(running=True)
    out = reconcile(fs, coord, bundle_path="/b", site_packages_root="/sp", ca_pem=CA_A)
    assert out.appended is False


def test_bundle_missing_at_path_uses_glob():
    fs = FakeFS(
        {"/sp/lib/certs/hue-bridge-ca-certs.pem": CA_B},
        {("/sp", "hue-bridge-ca-certs.pem"): "/sp/lib/certs/hue-bridge-ca-certs.pem"},
    )
    coord = FakeCoord(running=False)
    out = reconcile(
        fs, coord, bundle_path="/missing", site_packages_root="/sp", ca_pem=CA_A
    )
    assert out.bundle_found is True
    assert out.bundle_path == "/sp/lib/certs/hue-bridge-ca-certs.pem"
    assert out.appended is True


def test_bundle_not_found_anywhere():
    fs = FakeFS({}, {})
    coord = FakeCoord(running=True)
    out = reconcile(fs, coord, bundle_path="/missing", site_packages_root="/sp", ca_pem=CA_A)
    assert out == Outcome(
        bundle_found=False, appended=False, coordinator_restarted=False, bundle_path=None
    )
    assert coord.restarted is False


def test_unparseable_block_in_bundle_is_skipped_not_fatal():
    fs = FakeFS({"/b": "-----BEGIN CERTIFICATE-----\ngarbage\n-----END CERTIFICATE-----\n" + CA_B}, {})
    coord = FakeCoord(running=False)
    out = reconcile(fs, coord, bundle_path="/b", site_packages_root="/sp", ca_pem=CA_A)
    assert out.appended is True  # CA_A not present -> appended; garbage block skipped
```

> Note for the implementer: generate two real self-signed EC certs to fill `CA_A`
> and `CA_B` (they must be parseable by `ssl.PEM_cert_to_DER_cert`). One-liner to
> produce one:
> `openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 2 -subj "/CN=t" -keyout /dev/null 2>/dev/null` (take the cert PEM). Paste each full PEM verbatim.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_hue_ca_reconcile.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'brilliant_hue_ca.reconcile'`.

- [ ] **Step 3: Write the implementation**

`src/brilliant_hue_ca/reconcile.py`:
```python
"""Pure reconcile: ensure our CA is in the pinned Hue bundle (match by DER
fingerprint), and restart the local Hue coordinator only when we just appended.
Stdlib-only so it runs on the panel's Python 3.10 and off-panel in tests."""

from __future__ import annotations

import hashlib
import ssl
from dataclasses import dataclass

from .coordinator import Coordinator
from .fs import FileSystem

_BEGIN = "-----BEGIN CERTIFICATE-----"
_END = "-----END CERTIFICATE-----"


@dataclass(frozen=True)
class Outcome:
    bundle_found: bool
    appended: bool
    coordinator_restarted: bool
    bundle_path: str | None


def cert_fingerprint(pem: str) -> str:
    """SHA-256 hex of the certificate's DER encoding. Raises ssl.SSLError /
    ValueError on an unparseable PEM (callers guard where skipping is wanted)."""
    der = ssl.PEM_cert_to_DER_cert(pem)
    return hashlib.sha256(der).hexdigest()


def split_pem_certs(text: str) -> list[str]:
    certs: list[str] = []
    idx = 0
    while True:
        start = text.find(_BEGIN, idx)
        if start == -1:
            break
        end = text.find(_END, start)
        if end == -1:
            break
        certs.append(text[start : end + len(_END)] + "\n")
        idx = end + len(_END)
    return certs


def _bundle_contains(bundle_text: str, want_fp: str) -> bool:
    for block in split_pem_certs(bundle_text):
        try:
            if cert_fingerprint(block) == want_fp:
                return True
        except (ssl.SSLError, ValueError):
            continue  # skip unparseable block, keep scanning
    return False


def reconcile(
    fs: FileSystem,
    coordinator: Coordinator,
    *,
    bundle_path: str,
    site_packages_root: str,
    ca_pem: str,
) -> Outcome:
    path = bundle_path if fs.exists(bundle_path) else fs.glob(site_packages_root, "hue-bridge-ca-certs.pem")
    if path is None:
        return Outcome(False, False, False, None)

    want_fp = cert_fingerprint(ca_pem)
    if _bundle_contains(fs.read_text(path), want_fp):
        return Outcome(True, False, False, path)

    fs.append_text(path, "\n" + ca_pem if not ca_pem.startswith("\n") else ca_pem)
    if coordinator.is_running():
        coordinator.restart()
        return Outcome(True, True, True, path)
    return Outcome(True, True, False, path)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_hue_ca_reconcile.py -v`
Expected: PASS (all tests).

- [ ] **Step 5: Run the agent gate + commit**

Run: `uv run ruff check --fix && uv run ruff format && uv run mypy --strict src tests && uv run pytest`
```bash
git add src/brilliant_hue_ca/reconcile.py tests/test_hue_ca_reconcile.py
git commit -m "feat(hue-ca): reconcile core (fingerprint match + conditional restart)"
```

---

### Task 3: Thin entrypoint (`run.py`)

**Files:**
- Create: `src/brilliant_hue_ca/run.py`
- Test: `tests/test_hue_ca_run.py`

**Interfaces:**
- Consumes: `load_config` (Task 1), `RealFileSystem`, `RealCoordinator` (Task 1), `reconcile`, `Outcome` (Task 2).
- Produces: `run_once(environ, *, fs, coordinator, read_ca) -> int` (exit code); `main() -> None` wires the real collaborators and calls `sys.exit(run_once(...))`.

- [ ] **Step 1: Write the failing test**

`tests/test_hue_ca_run.py`:
```python
from brilliant_hue_ca.reconcile import Outcome
from brilliant_hue_ca.run import run_once

CA = "-----BEGIN CERTIFICATE-----\nX\n-----END CERTIFICATE-----\n"


class FakeFS:
    def __init__(self, exists_map, files):
        self._exists = exists_map
        self._files = files
        self.appended = []

    def exists(self, path):
        return self._exists.get(path, False)

    def read_text(self, path):
        return self._files[path]

    def append_text(self, path, text):
        self.appended.append((path, text))

    def glob(self, root, name):
        return None


class FakeCoord:
    def __init__(self):
        self.restarted = False

    def is_running(self):
        return False

    def restart(self):
        self.restarted = True


def test_run_once_returns_zero_on_success(monkeypatch):
    # CA present at the bundle path -> no-op -> exit 0
    fs = FakeFS({"/b": True}, {"/b": CA})
    rc = run_once(
        {"HUE_CA_BUNDLE_PATH": "/b"},
        fs=fs,
        coordinator=FakeCoord(),
        read_ca=lambda _p: CA,
    )
    assert rc == 0


def test_run_once_returns_one_when_ca_unreadable():
    def boom(_p):
        raise OSError("no ca")

    fs = FakeFS({"/b": True}, {"/b": CA})
    rc = run_once({"HUE_CA_BUNDLE_PATH": "/b"}, fs=fs, coordinator=FakeCoord(), read_ca=boom)
    assert rc == 1


def test_run_once_returns_zero_when_bundle_absent():
    fs = FakeFS({}, {})
    rc = run_once(
        {"HUE_CA_BUNDLE_PATH": "/missing"},
        fs=fs,
        coordinator=FakeCoord(),
        read_ca=lambda _p: CA,
    )
    # bundle-not-found is non-fatal (timer retries) -> exit 0
    assert rc == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_hue_ca_run.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'brilliant_hue_ca.run'`.

- [ ] **Step 3: Write the implementation**

`src/brilliant_hue_ca/run.py`:
```python
"""Thin oneshot entrypoint: one reconcile pass, then exit. Driven by the
brilliant-hue-ca.timer systemd unit (OnBootSec + periodic)."""

from __future__ import annotations

import logging
import logging.handlers
import os
import sys
from collections.abc import Callable, Mapping

from .config import load_config
from .coordinator import Coordinator, RealCoordinator
from .fs import FileSystem, RealFileSystem
from .reconcile import reconcile

_LOG = logging.getLogger("brilliant_hue_ca")


def run_once(
    environ: Mapping[str, str],
    *,
    fs: FileSystem,
    coordinator: Coordinator,
    read_ca: Callable[[str], str],
) -> int:
    cfg = load_config(environ)
    try:
        ca_pem = read_ca(cfg.ca_cert_path)
    except OSError:
        _LOG.exception("cannot read CA cert at %s", cfg.ca_cert_path)
        return 1
    try:
        outcome = reconcile(
            fs,
            coordinator,
            bundle_path=cfg.bundle_path,
            site_packages_root=cfg.site_packages_root,
            ca_pem=ca_pem,
        )
    except OSError:
        _LOG.exception("reconcile failed writing the bundle")
        return 1
    if not outcome.bundle_found:
        _LOG.warning("Hue CA bundle not found (path=%s, glob root=%s)", cfg.bundle_path, cfg.site_packages_root)
    elif outcome.appended:
        _LOG.info(
            "appended CA to %s; coordinator_restarted=%s",
            outcome.bundle_path,
            outcome.coordinator_restarted,
        )
    else:
        _LOG.debug("CA already present in %s; no-op", outcome.bundle_path)
    return 0


def _read_text(path: str) -> str:
    with open(path, encoding="utf-8") as f:
        return f.read()


def main() -> None:
    cfg = load_config(os.environ)
    handler = logging.handlers.RotatingFileHandler(cfg.log_path, maxBytes=256_000, backupCount=2)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    _LOG.addHandler(handler)
    _LOG.setLevel(logging.INFO)
    sys.exit(
        run_once(
            os.environ,
            fs=RealFileSystem(),
            coordinator=RealCoordinator(cfg.vassal_ini_path),
            read_ca=_read_text,
        )
    )


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_hue_ca_run.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Run the agent gate + commit**

Run: `uv run ruff check --fix && uv run ruff format && uv run mypy --strict src tests && uv run pytest`
```bash
git add src/brilliant_hue_ca/run.py tests/test_hue_ca_run.py
git commit -m "feat(hue-ca): thin oneshot entrypoint"
```

---

### Task 4: systemd units + payload/pyproject wiring

**Files:**
- Create: `deploy/brilliant-hue-ca.service`
- Create: `deploy/brilliant-hue-ca.timer`
- Modify: `scripts/build_payload.sh`
- Modify: `pyproject.toml` (add `src/brilliant_hue_ca` to `[tool.hatch.build.targets.wheel]` packages)
- Test: `tests/test_hue_ca_payload.py`

**Interfaces:**
- Consumes: `scripts/build_payload.sh` existing structure (`DEST=custom_components/brilliant_mqtt/agent_payload`, subdir-per-package pattern).
- Produces: `agent_payload/hue_ca/brilliant_hue_ca/*.py`, `agent_payload/brilliant-hue-ca.service`, `agent_payload/brilliant-hue-ca.timer` after a build.

- [ ] **Step 1: Write the failing test**

`tests/test_hue_ca_payload.py`:
```python
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_build_payload_includes_hue_ca(tmp_path):
    # Runs the real build script; asserts the hue_ca package + units land in the payload.
    subprocess.run(["bash", str(ROOT / "scripts" / "build_payload.sh")], check=True)
    dest = ROOT / "custom_components" / "brilliant_mqtt" / "agent_payload"
    assert (dest / "hue_ca" / "brilliant_hue_ca" / "run.py").is_file()
    assert (dest / "hue_ca" / "brilliant_hue_ca" / "reconcile.py").is_file()
    assert (dest / "brilliant-hue-ca.service").is_file()
    assert (dest / "brilliant-hue-ca.timer").is_file()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_hue_ca_payload.py -v`
Expected: FAIL — the `hue_ca/` subdir and units are not yet produced.

- [ ] **Step 3: Write the units + wire the build**

`deploy/brilliant-hue-ca.service`:
```ini
[Unit]
Description=Brilliant diyHue CA recovery (oneshot)
After=network.target

[Service]
Type=oneshot
EnvironmentFile=-/etc/brilliant-mqtt.env
ExecStart=/usr/bin/python3.10 -m brilliant_hue_ca.run
WorkingDirectory=/var/brilliant-mqtt/hue_ca
Environment=PYTHONPATH=/var/brilliant-mqtt/hue_ca
Nice=15
MemoryMax=48M
CPUQuota=10%
```

`deploy/brilliant-hue-ca.timer`:
```ini
[Unit]
Description=Brilliant diyHue CA recovery timer

[Timer]
OnBootSec=2min
OnUnitActiveSec=15min
Persistent=true

[Install]
WantedBy=timers.target
```

In `scripts/build_payload.sh`, after the bus-watchdog copy block (near the
`cp "$ROOT/src/brilliant_bus_watchdog"/*.py "$BUSWD_DST"/` line), add:
```bash
# hue-ca CA-recovery oneshot
HUECA_DST="$DEST/hue_ca/brilliant_hue_ca"
mkdir -p "$HUECA_DST"
cp "$ROOT/src/brilliant_hue_ca"/*.py "$HUECA_DST"/
```
And in the service-copy block (near the other `cp "$ROOT/deploy/…service"` lines):
```bash
cp "$ROOT/deploy/brilliant-hue-ca.service" "$DEST/brilliant-hue-ca.service"
cp "$ROOT/deploy/brilliant-hue-ca.timer" "$DEST/brilliant-hue-ca.timer"
```

In `pyproject.toml` `[tool.hatch.build.targets.wheel]` `packages`, add the line
`"src/brilliant_hue_ca",` alongside `"src/brilliant_bus_watchdog",`.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_hue_ca_payload.py -v`
Expected: PASS.

- [ ] **Step 5: Run the agent gate + commit**

Run: `uv run ruff check --fix && uv run ruff format && uv run mypy --strict src tests && uv run pytest`
```bash
git add deploy/brilliant-hue-ca.service deploy/brilliant-hue-ca.timer scripts/build_payload.sh pyproject.toml tests/test_hue_ca_payload.py
git commit -m "feat(hue-ca): systemd oneshot+timer units + payload/pyproject wiring"
```

---

### Task 5: Integration const + `panel_ops`

**Files:**
- Modify: `custom_components/brilliant_mqtt/const.py`
- Modify: `custom_components/brilliant_mqtt/panel_ops.py`
- Test: `ha/tests/test_hue_ca_component.py` (panel_ops portion)

**Interfaces:**
- Consumes: `PanelShell` (`shell.run`, `shell.put_dir`, `shell.put_bytes`), `_checked`, `PANEL_VAR_DIR`, the bus-watchdog deploy/ensure/enable/uninstall pattern.
- Produces (in `const.py`): `COMPONENT_HUE_CA = "hue_ca"`, `CONF_HUE_CA_CERT = "hue_ca_cert"`, `PANEL_HUE_CA_DIR = f"{PANEL_VAR_DIR}/hue_ca"`, `PANEL_HUE_CA_CERT_FILE = "/var/brilliant-hue-ca/injected-ca.pem"`, `PANEL_HUE_CA_SERVICE_UNIT_FILE = "/etc/systemd/system/brilliant-hue-ca.service"`, `PANEL_HUE_CA_TIMER_UNIT_FILE = "/etc/systemd/system/brilliant-hue-ca.timer"`, `HUE_CA_TIMER_NAME = "brilliant-hue-ca.timer"`.
- Produces (in `panel_ops.py`): `HueCaState` (`payload_present: bool`), `inspect_hue_ca(shell) -> HueCaState`, `deploy_hue_ca(shell, local_dir, ca_pem) -> None`, `ensure_hue_ca_units(shell, service_content, timer_content) -> None`, `enable_hue_ca(shell) -> None`, `uninstall_hue_ca(shell) -> None`.

- [ ] **Step 1: Write the failing test**

`ha/tests/test_hue_ca_component.py` (panel_ops portion — start the file here; extend it in Task 6):
```python
from unittest.mock import AsyncMock

import pytest

from custom_components.brilliant_mqtt import panel_ops
from custom_components.brilliant_mqtt.const import (
    HUE_CA_TIMER_NAME,
    PANEL_HUE_CA_CERT_FILE,
    PANEL_HUE_CA_DIR,
)


@pytest.mark.asyncio
async def test_deploy_hue_ca_puts_package_and_ca(tmp_path):
    shell = AsyncMock()
    (tmp_path / "brilliant_hue_ca").mkdir()
    await panel_ops.deploy_hue_ca(shell, str(tmp_path), ca_pem="PEMDATA")
    shell.put_dir.assert_awaited()  # staged package upload
    # CA written to the panel path with the exact bytes
    put_bytes_calls = [c.args for c in shell.put_bytes.await_args_list]
    assert any(
        args[1] == PANEL_HUE_CA_CERT_FILE and args[0] == b"PEMDATA" for args in put_bytes_calls
    )


@pytest.mark.asyncio
async def test_enable_hue_ca_enables_timer_not_service():
    shell = AsyncMock()
    shell.run.return_value = (0, "", "")
    await panel_ops.enable_hue_ca(shell)
    cmd = " ".join(str(c.args[0]) for c in shell.run.await_args_list)
    assert HUE_CA_TIMER_NAME in cmd
    assert "enable" in cmd and "--now" in cmd


@pytest.mark.asyncio
async def test_uninstall_hue_ca_removes_units_dir_and_cert():
    shell = AsyncMock()
    shell.run.return_value = (0, "", "")
    await panel_ops.uninstall_hue_ca(shell)
    cmd = " ".join(str(c.args[0]) for c in shell.run.await_args_list)
    assert HUE_CA_TIMER_NAME in cmd and "disable" in cmd
    assert PANEL_HUE_CA_DIR in cmd
    assert PANEL_HUE_CA_CERT_FILE in cmd
```

> Match the assertion style to how the existing `panel_ops` watchdog tests
> inspect `shell` calls in `ha/tests/` — mirror their mock/await-arg patterns
> exactly (adjust `.args`/`.await_args_list` access to the repo's convention).

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_hue_ca_component.py -v`
Expected: FAIL — `panel_ops` has no `deploy_hue_ca` / consts undefined.

- [ ] **Step 3: Add the consts + panel_ops functions**

In `const.py`, alongside the existing `COMPONENT_*` and `PANEL_BUS_WATCHDOG_*`:
```python
COMPONENT_HUE_CA = "hue_ca"
CONF_HUE_CA_CERT = "hue_ca_cert"

PANEL_HUE_CA_DIR = f"{PANEL_VAR_DIR}/hue_ca"
PANEL_HUE_CA_CERT_FILE = "/var/brilliant-hue-ca/injected-ca.pem"
PANEL_HUE_CA_SERVICE_UNIT_FILE = "/etc/systemd/system/brilliant-hue-ca.service"
PANEL_HUE_CA_TIMER_UNIT_FILE = "/etc/systemd/system/brilliant-hue-ca.timer"
HUE_CA_TIMER_NAME = "brilliant-hue-ca.timer"
```

In `panel_ops.py`, model on `deploy_bus_watchdog` / `ensure_bus_watchdog_unit` /
`enable_bus_watchdog` / `uninstall_bus_watchdog` (near them). Add a staging const
`_HUE_CA_STAGING_DIR = f"{PANEL_VAR_DIR}/hue_ca.staging"`, a staged-units dir
matching the bus-watchdog `_BUS_WATCHDOG_STAGED_UNIT` convention, and:
```python
@dataclass(frozen=True)
class HueCaState:
    payload_present: bool


async def inspect_hue_ca(shell: PanelShell) -> HueCaState:
    flags = await _probe_flags(  # reuse the repo's existing flag-probe helper
        shell,
        {"payload": f"test -f {PANEL_HUE_CA_DIR}/brilliant_hue_ca/run.py"},
    )
    return HueCaState(payload_present=flags.get("payload", False))


async def deploy_hue_ca(shell: PanelShell, local_dir: str, ca_pem: str) -> None:
    await shell.run(f"rm -rf {_HUE_CA_STAGING_DIR}")
    await shell.put_dir(local_dir, _HUE_CA_STAGING_DIR)
    await _checked(shell, _hue_ca_swap_command())
    await _checked(shell, f"mkdir -p {os.path.dirname(PANEL_HUE_CA_CERT_FILE)}")
    await shell.put_bytes(ca_pem.encode(), PANEL_HUE_CA_CERT_FILE, 0o644)


def _hue_ca_swap_command() -> str:
    return " && ".join(
        [
            f"mkdir -p {PANEL_VAR_DIR}",
            f"rm -rf {PANEL_HUE_CA_DIR}.bak",
            f"{{ [ -e {PANEL_HUE_CA_DIR} ] && mv {PANEL_HUE_CA_DIR} {PANEL_HUE_CA_DIR}.bak; true; }}",
            f"mv {_HUE_CA_STAGING_DIR} {PANEL_HUE_CA_DIR}",
            f"rm -rf {PANEL_HUE_CA_DIR}.bak",
        ]
    )


async def ensure_hue_ca_units(shell: PanelShell, service_content: str, timer_content: str) -> None:
    await _checked(shell, f"mkdir -p {PANEL_HUE_CA_DIR}")
    await shell.put_bytes(service_content.encode(), PANEL_HUE_CA_SERVICE_UNIT_FILE, 0o644)
    await shell.put_bytes(timer_content.encode(), PANEL_HUE_CA_TIMER_UNIT_FILE, 0o644)
    await _checked(shell, "systemctl daemon-reload")


async def enable_hue_ca(shell: PanelShell) -> None:
    await _checked(shell, f"systemctl enable --now {HUE_CA_TIMER_NAME}")


async def uninstall_hue_ca(shell: PanelShell) -> None:
    await shell.run(f"systemctl disable --now {HUE_CA_TIMER_NAME}")
    await shell.run(
        f"rm -f {PANEL_HUE_CA_SERVICE_UNIT_FILE} {PANEL_HUE_CA_TIMER_UNIT_FILE}"
    )
    await shell.run(f"rm -rf {PANEL_HUE_CA_DIR} {PANEL_HUE_CA_CERT_FILE}")
    await shell.run("systemctl daemon-reload")
```

> Use the repo's real flag-probe helper name (whatever `inspect_bus_watchdog`
> calls) rather than `_probe_flags` if it differs, and match `os` import
> presence. Also add staged OTA-proof unit copies if the bus watchdog does
> (mirror `_BUS_WATCHDOG_STAGED_UNIT`).

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_hue_ca_component.py -v`
Expected: PASS.

- [ ] **Step 5: Run the integration gate + commit**

Run the full integration gate (Global Constraints).
```bash
git add custom_components/brilliant_mqtt/const.py custom_components/brilliant_mqtt/panel_ops.py ha/tests/test_hue_ca_component.py
git commit -m "feat(hue-ca): integration const + panel_ops deploy/enable/uninstall"
```

---

### Task 6: Integration component registry + CA-cert config field

**Files:**
- Modify: `custom_components/brilliant_mqtt/components.py`
- Modify: `custom_components/brilliant_mqtt/config_flow.py`
- Test: `ha/tests/test_hue_ca_component.py` (extend)

**Interfaces:**
- Consumes: `Component`, `REGISTRY`, `optional()` (components.py); `panel_ops.inspect_hue_ca/deploy_hue_ca/ensure_hue_ca_units/enable_hue_ca/uninstall_hue_ca` (Task 5); `_mgr._payload_dir()`; `CONF_HUE_CA_CERT`, `COMPONENT_HUE_CA` (Task 5); `PanelOpError`.
- Produces: a `REGISTRY[COMPONENT_HUE_CA]` entry (returned by `optional()`); a `CONF_HUE_CA_CERT` sub-field in the components options schema.

- [ ] **Step 1: Write the failing tests (extend the file)**

Append to `ha/tests/test_hue_ca_component.py`:
```python
from unittest.mock import patch

from custom_components.brilliant_mqtt import components
from custom_components.brilliant_mqtt.const import COMPONENT_HUE_CA, CONF_HUE_CA_CERT
from custom_components.brilliant_mqtt.panel_ops import PanelOpError


def test_hue_ca_in_optional_components():
    ids = [c.id for c in components.optional()]
    assert COMPONENT_HUE_CA in ids
    comp = components.REGISTRY[COMPONENT_HUE_CA]
    assert comp.locked is False
    assert comp.default_enabled is False


@pytest.mark.asyncio
async def test_hue_ca_install_refuses_empty_ca():
    comp = components.REGISTRY[COMPONENT_HUE_CA]
    with pytest.raises(PanelOpError):
        await comp.install(None, AsyncMock(), {CONF_HUE_CA_CERT: ""})


@pytest.mark.asyncio
async def test_hue_ca_install_deploys_with_ca(tmp_path):
    # Stub the bundled payload dir so install can read the unit files from it.
    (tmp_path / "brilliant-hue-ca.service").write_text("svc")
    (tmp_path / "brilliant-hue-ca.timer").write_text("tmr")
    (tmp_path / "hue_ca").mkdir()
    comp = components.REGISTRY[COMPONENT_HUE_CA]
    hass = AsyncMock()
    hass.async_add_executor_job = AsyncMock(side_effect=lambda fn, *a: fn(*a))
    shell = AsyncMock()
    with (
        patch.object(components._mgr, "_payload_dir", return_value=tmp_path),
        patch("custom_components.brilliant_mqtt.panel_ops.deploy_hue_ca", new=AsyncMock()) as dep,
        patch("custom_components.brilliant_mqtt.panel_ops.ensure_hue_ca_units", new=AsyncMock()),
        patch("custom_components.brilliant_mqtt.panel_ops.enable_hue_ca", new=AsyncMock()),
    ):
        await comp.install(hass, shell, {CONF_HUE_CA_CERT: "PEM"})
    dep.assert_awaited()
```

> Adjust the `install` positional args (`hass`, `shell`, `data`) and the
> service-file reading to match how `_voice_install` / `_wd_install` read units
> from `_payload_dir()`; the test above is a template — align the patch targets
> to the actual call sites you write in Step 3.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_hue_ca_component.py -v`
Expected: FAIL — no `REGISTRY[COMPONENT_HUE_CA]`.

- [ ] **Step 3: Add the component + config field**

In `components.py` (near `_bus_present`/`_bus_install`), add:
```python
async def _hue_ca_present(shell: PanelShell) -> bool:
    return (await panel_ops.inspect_hue_ca(shell)).payload_present


async def _hue_ca_install(hass: HomeAssistant, shell: PanelShell, data: Mapping[str, Any]) -> None:
    ca_pem = str(data.get(CONF_HUE_CA_CERT, "")).strip()
    if not ca_pem:
        raise PanelOpError("Hue CA recovery needs the diyHue CA certificate (PEM)")
    payload_dir = _mgr._payload_dir()
    service = await hass.async_add_executor_job(
        (payload_dir / "brilliant-hue-ca.service").read_text
    )
    timer = await hass.async_add_executor_job(
        (payload_dir / "brilliant-hue-ca.timer").read_text
    )
    await panel_ops.deploy_hue_ca(shell, str(payload_dir / "hue_ca"), ca_pem)
    await panel_ops.ensure_hue_ca_units(shell, service, timer)
    await panel_ops.enable_hue_ca(shell)
```
And add to `REGISTRY`:
```python
    COMPONENT_HUE_CA: Component(
        id=COMPONENT_HUE_CA,
        label="Hue CA recovery",
        locked=False,
        default_enabled=False,
        present=_hue_ca_present,
        install=_hue_ca_install,
        remove=panel_ops.uninstall_hue_ca,
    ),
```
Add the imports `COMPONENT_HUE_CA`, `CONF_HUE_CA_CERT` from `.const` at the top of `components.py` (with the other `COMPONENT_*` imports).

In `config_flow.py` `_components_schema_fields`, after the voice sub-fields
(`fields[vol.Optional(CONF_VOICE_HA_HOST, …)] = str`), add the CA field
(multiline text) and import `CONF_HUE_CA_CERT`:
```python
    from homeassistant.helpers.selector import TextSelector, TextSelectorConfig

    fields[vol.Optional(CONF_HUE_CA_CERT, default=source.get(CONF_HUE_CA_CERT, ""))] = (
        TextSelector(TextSelectorConfig(multiline=True))
    )
```
(Place the import with the other `homeassistant.helpers.selector` imports if the
file already imports from it; otherwise add it at module top.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_hue_ca_component.py -v`
Expected: PASS.

- [ ] **Step 5: Run the integration gate + commit**

Run the full integration gate (Global Constraints).
```bash
git add custom_components/brilliant_mqtt/components.py custom_components/brilliant_mqtt/config_flow.py ha/tests/test_hue_ca_component.py
git commit -m "feat(hue-ca): optional off-by-default integration component + CA-cert field"
```

---

### Task 7: Docs

**Files:**
- Modify: `docs/CONFIGURATION.md`
- Modify: `deploy/README.md`

**Interfaces:** none (documentation only).

- [ ] **Step 1: Document the component in `docs/CONFIGURATION.md`**

Add a "Hue CA recovery" section alongside the other components. Cover: what it
does (re-appends the operator's diyHue CA to the pinned Hue bundle after OTA and
restarts the local Hue coordinator), that it is **off by default**, the
**CA-certificate PEM** field it requires (paste the diyHue CA *public* cert),
its systemd timer cadence (`OnBootSec` + 15 min), and the **rollout note**:
enable it on **all** bridged panels because the Hue integration host can move to
any panel and a panel without the hook would strand the integration if it became
leader. Include the env-var overrides table (`HUE_CA_CERT_PATH`,
`HUE_CA_BUNDLE_PATH`, `HUE_CA_SITE_PACKAGES`, `HUE_CA_VASSAL_INI`, `HUE_CA_LOG`).
Follow the existing scannable table/step style used for the watchdog sections.

- [ ] **Step 2: Add the units to `deploy/README.md`**

Under "Contents", add bullets for `brilliant-hue-ca.service` (the oneshot) and
`brilliant-hue-ca.timer` (its activation timer), one line each, matching the
existing bullet style, and note it is the "Hue CA recovery" component installed
via the integration.

- [ ] **Step 3: Commit**

```bash
git add docs/CONFIGURATION.md deploy/README.md
git commit -m "docs(hue-ca): document the Hue CA recovery component + units"
```

---

## Notes for the executor

- Tasks 1-4 are the **agent** (Python 3.10) — run the **agent gate** after each.
- Tasks 5-7 are the **integration** (Python 3.14) — run the **integration gate**
  after each (both gates in Global Constraints).
- The two subsystems are decoupled: the agent tasks (1-4) and integration tasks
  (5-7) can each be reviewed independently; 5-6 depend only on the *unit
  filenames* and *payload subdir name* (`hue_ca`) that Task 4 establishes, not on
  agent internals.
- After Task 4, a `bash scripts/build_payload.sh` must be run before the
  integration install path is exercised live (the payload must contain the new
  subdir + units). This is not part of the unit tests but is required before any
  real panel deploy.
