# Native Partner Stub Gate 0 Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and run a network-isolated ARM harness that determines whether the shipped Hunter Douglas shade and RemoteLock lock stubs have a safe, loop-free, authoritative lifecycle before any Brilliant panel is mutated.

**Architecture:** Host-side Python validates the pinned firmware corpus, constructs a hardened Docker invocation, and evaluates a typed JSON report. A separate ARM-only probe imports Brilliant's captured Cython modules inside a read-only `linux/arm/v7` container with no network namespace, drives synthetic `PeripheralInfo(stubbed=True)` objects through the stock selection and lifecycle seams, and records calls through fake coordinators and clients. The report distinguishes harness completion from candidate feasibility, so a forced-online stub or indistinguishable HA feedback yields `NO_GO` without turning a research result into a production claim.

**Tech Stack:** Python 3.10, pytest, stdlib `dataclasses`/`subprocess`/`unittest.mock`, captured Brilliant firmware `v26.06.03.1`, Docker `linux/arm/v7`, pinned official Python 3.10 ARM image, uv, ruff, mypy strict.

## Global Constraints

- Gate 0 is entirely off-panel. Do not SSH to a panel, open a panel message-bus socket, call Home Assistant, publish MQTT, or invoke a Brilliant/partner API.
- Do not create or register a device in the Office home, Home Assistant, Brilliant's cloud, Hunter Douglas, or RemoteLock.
- Run the ARM probe with Docker `--network none`, a read-only root filesystem, all Linux capabilities dropped, `no-new-privileges`, bounded PID/memory/CPU limits, and only read-only bind mounts.
- Use synthetic identifiers only. Do not load bootstrap material, device certificates, access tokens, home IDs, room IDs, panel IDs, production peripheral snapshots, or `CREDENTIALS.local.md`.
- Pin firmware exactly to `v26.06.03.1` and reject the corpus when any required module hash differs.
- Never call the stock broad `tools.test add_stubs` helper. Construct one in-memory primary-position-only Hunter Douglas `SHADE` and one in-memory non-common-area RemoteLock `LOCK`.
- Evaluate the shade before the lock. A blocked or failing shade contract prevents the lock probe from being represented as an approved next-stage candidate.
- The lock probe may exercise state and the lock action (`locked=True`) only. It must reject unlock (`locked=False`) before invoking firmware code.
- Keep PowerView, cameras, doorbells, security systems, garage doors, tilt/secondary shade rails, favorites, groups, scenes, and physical-slider binding out of this plan.
- A stock stub that forces `ONLINE` without an upstream-health input fails availability authority. Feedback that enters the same externally settable callback without a provable origin fails loop authority.
- A completed harness may legitimately conclude `NO_GO` or `BLOCKED`. Only a report satisfying every required assertion may conclude `PASS`, and even `PASS` authorizes Gate 1 read-only baseline work only.
- Proprietary firmware and generated evidence remain below gitignored `artifacts/`; only source, tests, schemas, runbooks, and sanitized conclusions may be committed.
- Root project code remains Python `>=3.10,<3.11`; use `uv` for tests and quality gates.

---

## File map

- Create `tools/brilliant_partner_stubs/__init__.py`: package marker and public contract exports.
- Create `tools/brilliant_partner_stubs/contracts.py`: pinned candidate metadata, deterministic synthetic IDs, required firmware paths, and SHA-256 hashes.
- Create `tools/brilliant_partner_stubs/report.py`: typed evidence model, strict JSON parsing, verdict derivation, and concise report summary.
- Create `tools/brilliant_partner_stubs/container_runner.py`: corpus/image preflight, hardened Docker command construction, bounded execution, and atomic evidence write.
- Create `tools/brilliant_partner_stubs/firmware_probe.py`: ARM-only extension loader, network tripwires, stock module-selection/lifecycle probes, and synthetic shade/lock drives.
- Create `tests/test_partner_stub_contracts.py`: exact firmware/candidate contract tests.
- Create `tests/test_partner_stub_report.py`: report validation and `PASS`/`NO_GO`/`BLOCKED` truth-table tests.
- Create `tests/test_partner_stub_container_runner.py`: fail-closed preflight and exact Docker-boundary tests.
- Create `tests/test_partner_stub_firmware_probe.py`: host-import and source-level safety tests for the ARM-only probe.
- Create `docs/brilliant-panel/native-partner-stub-gate0.schema.json`: committed schema for sanitized Gate 0 evidence.
- Create `docs/brilliant-panel/runbooks/native-partner-stub-gate0.md`: image preparation, execution, interpretation, stop conditions, and proof that no device is created.
- Modify `docs/brilliant-panel/native-partner-stub-feasibility.md`: link the harness/runbook and record the observed Gate 0 verdict after execution.
- Modify `docs/brilliant-panel/README.md`: add the Gate 0 runbook to the documentation index.

The package is not included by `scripts/build_payload.sh`, is not installed on a panel, and has no systemd, MQTT, HA, SSH, or deployment entry point.

---

### Task 1: Pin candidate contracts and define verdict semantics

**Files:**
- Create: `tools/brilliant_partner_stubs/__init__.py`
- Create: `tools/brilliant_partner_stubs/contracts.py`
- Create: `tools/brilliant_partner_stubs/report.py`
- Create: `docs/brilliant-panel/native-partner-stub-gate0.schema.json`
- Test: `tests/test_partner_stub_contracts.py`
- Test: `tests/test_partner_stub_report.py`

**Interfaces:**
- Produces: `Candidate`, `CandidateContract`, `CONTRACTS`, `PINNED_FIRMWARE`, `REQUIRED_FIRMWARE_SHA256`.
- Produces: `Verdict`, `CandidateEvidence`, `Gate0Report.from_json(raw: str) -> Gate0Report`, `Gate0Report.to_json() -> str`, and `Gate0Report.summary() -> str`.
- Candidate keys are exactly `shade` and `lock`; report verdicts are exactly `pass`, `no_go`, and `blocked`.

- [ ] **Step 1: Write failing exact-contract tests**

```python
from tools.brilliant_partner_stubs.contracts import (
    CONTRACTS,
    PINNED_FIRMWARE,
    Candidate,
)


def test_candidate_contracts_are_exactly_pinned_to_captured_firmware() -> None:
    assert PINNED_FIRMWARE == "v26.06.03.1"
    assert set(CONTRACTS) == {Candidate.SHADE, Candidate.LOCK}

    shade = CONTRACTS[Candidate.SHADE]
    assert shade.integration_id == "hunter_douglas"
    assert shade.configuration_peripheral_id == "hunter_douglas_configuration"
    assert shade.configuration_peripheral_type == 90
    assert shade.peripheral_type == 53
    assert shade.stock_module.endswith("hunter_douglas_shade_peripheral")
    assert shade.stub_module.endswith("hunter_douglas_shade_peripheral_stub")
    assert shade.setter_name == "_set_position"
    assert shade.state_variable == "position"

    lock = CONTRACTS[Candidate.LOCK]
    assert lock.integration_id == "remotelock"
    assert lock.configuration_peripheral_id == "remotelock_configuration"
    assert lock.configuration_peripheral_type == 91
    assert lock.peripheral_type == 1
    assert lock.configuration_variables == {"is_common_area": "0"}
    assert lock.setter_name == "_make_lock_request"
    assert lock.state_variable == "locked"
```

Add hash assertions for all eleven pinned files listed in Step 3. Assert both synthetic peripheral names begin with `gate0_`, contain no 32-hex identifier, and differ from their third-party IDs.

- [ ] **Step 2: Run the tests and verify the missing-module failure**

Run: `uv run pytest tests/test_partner_stub_contracts.py tests/test_partner_stub_report.py -q`

Expected: FAIL during collection with `ModuleNotFoundError: No module named 'tools.brilliant_partner_stubs'`.

- [ ] **Step 3: Implement the exact candidate and corpus contracts**

```python
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Mapping

PINNED_FIRMWARE = "v26.06.03.1"
SITE_PACKAGES = "env/lib/python3.10/site-packages"


class Candidate(str, Enum):
    SHADE = "shade"
    LOCK = "lock"


@dataclass(frozen=True, slots=True)
class CandidateContract:
    candidate: Candidate
    integration_id: str
    configuration_peripheral_id: str
    configuration_peripheral_type: int
    peripheral_type: int
    peripheral_name: str
    thirdparty_device_id: str
    stock_module: str
    stub_module: str
    stub_class: str
    setter_name: str
    state_variable: str
    configuration_variables: Mapping[str, str]


CONTRACTS = MappingProxyType(
    {
        Candidate.SHADE: CandidateContract(
            candidate=Candidate.SHADE,
            integration_id="hunter_douglas",
            configuration_peripheral_id="hunter_douglas_configuration",
            configuration_peripheral_type=90,
            peripheral_type=53,
            peripheral_name="gate0_hunter_douglas_shade_v1",
            thirdparty_device_id="gate0:shade:v1",
            stock_module=(
                "peripherals.hunter_douglas.hunter_douglas_shade_peripheral"
            ),
            stub_module=(
                "peripherals.hunter_douglas.hunter_douglas_shade_peripheral_stub"
            ),
            stub_class="HunterDouglasShadePeripheralStub",
            setter_name="_set_position",
            state_variable="position",
            configuration_variables=MappingProxyType({}),
        ),
        Candidate.LOCK: CandidateContract(
            candidate=Candidate.LOCK,
            integration_id="remotelock",
            configuration_peripheral_id="remotelock_configuration",
            configuration_peripheral_type=91,
            peripheral_type=1,
            peripheral_name="gate0_remotelock_lock_v1",
            thirdparty_device_id="gate0:lock:v1",
            stock_module="peripherals.remotelock.remotelock_lock_peripheral",
            stub_module="peripherals.remotelock.remotelock_lock_peripheral_stub",
            stub_class="RemoteLockLockPeripheralStub",
            setter_name="_make_lock_request",
            state_variable="locked",
            configuration_variables=MappingProxyType({"is_common_area": "0"}),
        ),
    }
)

REQUIRED_FIRMWARE_SHA256 = MappingProxyType(
    {
        f"{SITE_PACKAGES}/peripherals/lib/peripheral_service/"
        "conditional_peripheral_host.cpython-310-arm-linux-gnueabi.so": (
            "12ab11a7986a71912991f6a98cac95655c531353cf6b3a60d72c38d0d525c482"
        ),
        f"{SITE_PACKAGES}/peripherals/lib/virtual/"
        "virtual_peripheral_host.cpython-310-arm-linux-gnueabi.so": (
            "e3c828f0ece288603ec63a7e59a92d7ffab59a49fdbe6e2e382a4dea1c94b886"
        ),
        f"{SITE_PACKAGES}/peripherals/hunter_douglas/"
        "hunter_douglas_peripheral_host.cpython-310-arm-linux-gnueabi.so": (
            "8e1b44a7cead9849ee5d4395319ec853ef0fc3ee25fff0df862a3b63273e03f0"
        ),
        f"{SITE_PACKAGES}/peripherals/hunter_douglas/"
        "hunter_douglas_shade_peripheral.cpython-310-arm-linux-gnueabi.so": (
            "36081a58bc97dde4edba657e5a47a8ae36905a2e97c8c68c46b5f16676f78341"
        ),
        f"{SITE_PACKAGES}/peripherals/hunter_douglas/"
        "hunter_douglas_shade_peripheral_stub.cpython-310-arm-linux-gnueabi.so": (
            "14836027b18dfc4f4dfc235d034e7c14eb59777a82a3aa8e59eacb742daa2a7b"
        ),
        f"{SITE_PACKAGES}/peripherals/remotelock/"
        "remotelock_peripheral_host.cpython-310-arm-linux-gnueabi.so": (
            "f5457df1075eb295de5f17de2343c1ba8478f9d0497e94b429ee3e2cc0f184c1"
        ),
        f"{SITE_PACKAGES}/peripherals/remotelock/"
        "remotelock_lock_peripheral.cpython-310-arm-linux-gnueabi.so": (
            "31f121f13e3735069bc1bc9d84afca3b74f5a3b419e206a35fd4bf5ffda62e33"
        ),
        f"{SITE_PACKAGES}/peripherals/remotelock/"
        "remotelock_lock_peripheral_stub.cpython-310-arm-linux-gnueabi.so": (
            "7002a26b82dec9878ec3681b3468429a3c27b91becea7a6e7799bbf837f4ef98"
        ),
        f"{SITE_PACKAGES}/lib/tools/"
        "peripheral_interface_helpers.cpython-310-arm-linux-gnueabi.so": (
            "5a8ad086150de040ba3878f975e9897def2b61c366f751e4312ec75a74cb4288"
        ),
        f"{SITE_PACKAGES}/thrift_types/configuration/ttypes.py": (
            "0d6c19be7c96af3285bed7906896df0de3258aa421e975d9741fe2eda627229f"
        ),
        f"{SITE_PACKAGES}/thrift_types/message_bus/ttypes.py": (
            "99c62781c8c3678b8c44faa98108710409b77700a44fb6bd736a6e303686968f"
        ),
    }
)
```

The hash map is an interoperability guard, not redistributed firmware. Paths are relative to the ignored `switch-embedded` snapshot root.

- [ ] **Step 4: Implement strict report parsing and verdict derivation**

```python
class Verdict(str, Enum):
    PASS = "pass"
    NO_GO = "no_go"
    BLOCKED = "blocked"


@dataclass(frozen=True, slots=True)
class CandidateEvidence:
    candidate: Candidate
    probe_completed: bool
    selected_expected_stub: bool
    non_stub_selected_when_stubbed_false: bool
    network_attempts: int
    ui_command_count: int
    ui_state_delta_count: int
    feedback_command_count: int
    feedback_origin_distinguishable: bool
    stock_online_without_upstream_health: bool
    unrelated_entry_unchanged: bool
    owner_relay_mismatch_rejected: bool
    exact_source_removed: bool
    deletion_timestamp_ms: int | None
    blocked_reason: str | None

    @property
    def verdict(self) -> Verdict:
        if self.blocked_reason is not None or not self.probe_completed:
            return Verdict.BLOCKED
        safe = (
            self.selected_expected_stub
            and self.non_stub_selected_when_stubbed_false
            and self.network_attempts == 0
            and self.ui_command_count == 1
            and self.ui_state_delta_count == 1
            and self.feedback_command_count == 0
            and self.feedback_origin_distinguishable
            and not self.stock_online_without_upstream_health
            and self.unrelated_entry_unchanged
            and self.owner_relay_mismatch_rejected
            and self.exact_source_removed
            and self.deletion_timestamp_ms is not None
            and self.deletion_timestamp_ms > 0
        )
        return Verdict.PASS if safe else Verdict.NO_GO


@dataclass(frozen=True, slots=True)
class Gate0Report:
    firmware: str
    image_digest: str
    harness_completed: bool
    network_mode: str
    docker_network_disabled: bool
    non_loopback_route_count: int
    synthetic_identity_only: bool
    physical_devices_available: bool
    candidate_order: tuple[Candidate, ...]
    candidates: tuple[CandidateEvidence, ...]
    unlock_firmware_invocations: int
    lock_next_stage_eligible: bool

    def validate(self) -> None:
        if self.firmware != PINNED_FIRMWARE:
            raise ReportError("firmware is not the pinned Gate 0 release")
        if self.network_mode != "none" or not self.docker_network_disabled:
            raise ReportError("Gate 0 report does not prove a disabled network")
        if self.non_loopback_route_count != 0:
            raise ReportError("Gate 0 report contains a non-loopback route")
        if not self.synthetic_identity_only or self.physical_devices_available:
            raise ReportError("Gate 0 report is not synthetic-only")
        if self.candidate_order != (Candidate.SHADE, Candidate.LOCK):
            raise ReportError("candidate order must be shade then lock")
        if tuple(item.candidate for item in self.candidates) != self.candidate_order:
            raise ReportError("candidate evidence order does not match")
        if self.unlock_firmware_invocations != 0:
            raise ReportError("unlock reached captured firmware")
        shade_passed = self.candidates[0].verdict is Verdict.PASS
        if self.lock_next_stage_eligible != (
            shade_passed and self.candidates[1].verdict is Verdict.PASS
        ):
            raise ReportError("lock next-stage eligibility is inconsistent")
```

`Gate0Report.from_json` constructs these exact fields, rejects unknown/missing keys, rejects negative counts and 32-character hexadecimal identifier-shaped string values recursively, calls `validate()`, derives each candidate verdict locally, and rejects a serialized verdict that disagrees. `to_json()` emits sorted keys and includes each derived verdict; `summary()` emits only firmware/image prefixes, isolation booleans, verdicts, and sanitized failed/blocked assertion names.

The committed JSON Schema uses `additionalProperties: false`, the same enums and required fields, and integer minima of zero. It permits sanitized module paths and reasons but no raw event payloads, logs, environment, IDs, credentials, URLs, or exception tracebacks.

- [ ] **Step 5: Complete the verdict truth-table tests**

Test these exact cases:

- every safe assertion true yields `PASS`;
- `stock_online_without_upstream_health=True` yields `NO_GO`;
- `feedback_command_count=1` or `feedback_origin_distinguishable=False` yields `NO_GO`;
- an unrelated mutation, owner/relay acceptance, missing deletion timestamp, or any network attempt yields `NO_GO`;
- an import/constructor incompatibility with a nonempty sanitized `blocked_reason` yields `BLOCKED`;
- a serialized verdict that says `pass` for a forced-online stub is rejected;
- a report containing a 32-character hexadecimal home/device identifier is rejected.

- [ ] **Step 6: Run focused quality gates**

Run:

```bash
uv run pytest tests/test_partner_stub_contracts.py tests/test_partner_stub_report.py -q
uv run ruff check tools/brilliant_partner_stubs tests/test_partner_stub_contracts.py tests/test_partner_stub_report.py
uv run mypy --strict tools/brilliant_partner_stubs/contracts.py tools/brilliant_partner_stubs/report.py tests/test_partner_stub_contracts.py tests/test_partner_stub_report.py
```

Expected: all commands exit 0.

- [ ] **Step 7: Commit**

```bash
git add tools/brilliant_partner_stubs/__init__.py \
  tools/brilliant_partner_stubs/contracts.py \
  tools/brilliant_partner_stubs/report.py \
  tests/test_partner_stub_contracts.py \
  tests/test_partner_stub_report.py \
  docs/brilliant-panel/native-partner-stub-gate0.schema.json
git commit -m "test: define partner stub Gate 0 contracts"
```

---

### Task 2: Build the fail-closed ARM container runner

**Files:**
- Create: `tools/brilliant_partner_stubs/container_runner.py`
- Test: `tests/test_partner_stub_container_runner.py`

**Interfaces:**
- Produces: `RunnerPaths`, `RunnerError`, `validate_firmware(root: Path) -> None`, `build_docker_command(paths: RunnerPaths) -> tuple[str, ...]`, and `run_gate0(paths: RunnerPaths, *, timeout_s: float = 90.0) -> Gate0Report`.
- CLI: `python -m tools.brilliant_partner_stubs.container_runner --firmware-root PATH --output PATH`.
- Image: `docker.io/arm32v7/python@sha256:b543951a164e971a90609c512e04a73d2cad681aadcd5916dc081d2028cb0848` (`linux/arm/v7`, Python 3.10.18, Debian bullseye-slim).

- [ ] **Step 1: Write failing runner-boundary tests**

```python
def test_docker_command_has_no_network_or_writable_repository(paths: RunnerPaths) -> None:
    command = build_docker_command(paths)
    assert command[:4] == ("docker", "run", "--rm", "--pull")
    assert command[4] == "never"
    assert ("--platform", "linux/arm/v7") == _pair(command, "--platform")
    assert ("--network", "none") == _pair(command, "--network")
    assert "--read-only" in command
    assert ("--cap-drop", "ALL") == _pair(command, "--cap-drop")
    assert ("--security-opt", "no-new-privileges:true") == _pair(
        command, "--security-opt"
    )
    mounts = [command[index + 1] for index, value in enumerate(command) if value == "--mount"]
    assert mounts and all(mount.endswith(",readonly") for mount in mounts)
    assert not any("CREDENTIALS.local.md" in item for item in command)
```

Also assert the runner makes no Docker call when a required file is missing or has the wrong digest; rejects output paths outside `artifacts/brilliant-panel/pilots/partner-stubs/`; rejects an output path that is not ignored by Git; rejects an existing output file; uses `shell=False`; enforces the timeout; refuses an image that is not already local; and never returns a report when container stderr contains a traceback or stdout is not one JSON object.

- [ ] **Step 2: Run the runner test and verify failure**

Run: `uv run pytest tests/test_partner_stub_container_runner.py -q`

Expected: FAIL because `tools.brilliant_partner_stubs.container_runner` does not exist.

- [ ] **Step 3: Implement corpus validation and path confinement**

```python
PINNED_IMAGE = (
    "docker.io/arm32v7/python@"
    "sha256:b543951a164e971a90609c512e04a73d2cad681aadcd5916dc081d2028cb0848"
)
EVIDENCE_ROOT = Path("artifacts/brilliant-panel/pilots/partner-stubs")


@dataclass(frozen=True, slots=True)
class RunnerPaths:
    repo_root: Path
    firmware_root: Path
    output: Path

    def resolved(self) -> "RunnerPaths":
        return RunnerPaths(
            repo_root=self.repo_root.resolve(strict=True),
            firmware_root=self.firmware_root.resolve(strict=True),
            output=self.output.resolve(strict=False),
        )


def validate_firmware(root: Path) -> None:
    for relative, expected in REQUIRED_FIRMWARE_SHA256.items():
        path = root / relative
        if not path.is_file() or path.is_symlink():
            raise RunnerError(f"required pinned firmware file is absent: {relative}")
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if not hmac.compare_digest(actual, expected):
            raise RunnerError(f"pinned firmware hash mismatch: {relative}")
```

Resolve the evidence root under `repo_root`, require `output.parent` to be inside it, run `git check-ignore -q -- <output.parent>` from the repository without a shell, create the directory mode `0700`, and open the eventual output with `O_CREAT | O_EXCL | O_NOFOLLOW` mode `0600`. Do not follow a symlink at any checked path.

- [ ] **Step 4: Implement the exact hardened Docker command**

```python
def build_docker_command(paths: RunnerPaths) -> tuple[str, ...]:
    paths = paths.resolved()
    return (
        "docker",
        "run",
        "--rm",
        "--pull",
        "never",
        "--platform",
        "linux/arm/v7",
        "--network",
        "none",
        "--read-only",
        "--cap-drop",
        "ALL",
        "--security-opt",
        "no-new-privileges:true",
        "--pids-limit",
        "128",
        "--memory",
        "256m",
        "--cpus",
        "1",
        "--tmpfs",
        "/tmp:rw,nosuid,nodev,noexec,size=64m,mode=0700",
        "--mount",
        f"type=bind,src={paths.firmware_root},dst=/firmware,readonly",
        "--mount",
        f"type=bind,src={paths.repo_root},dst=/work,readonly",
        "--workdir",
        "/tmp",
        "--env",
        "PYTHONDONTWRITEBYTECODE=1",
        PINNED_IMAGE,
        "python3.10",
        "-B",
        "/work/tools/brilliant_partner_stubs/firmware_probe.py",
        "--candidate",
        "shade",
        "--candidate",
        "lock",
    )
```

The runner first calls `docker image inspect PINNED_IMAGE`; it never pulls. Execute the command with `subprocess.run(command, text=True, capture_output=True, timeout=90.0, check=False)`. Cap accepted stdout and stderr at 1 MiB each, require exit code 0, require empty stderr, parse stdout with `Gate0Report.from_json`, then atomically write `report.to_json()` plus a newline. Do not include the process environment or Docker diagnostic body in committed/sanitized output.

- [ ] **Step 5: Add exact CLI defaults and dry preflight output**

The CLI requires both paths. The documented firmware root is:

```text
artifacts/brilliant-panel/v26.06.03.1/extracted/data/switch-embedded
```

Add `--preflight-only`; it validates the corpus, ignored evidence path, daemon, and already-local image, prints `Gate 0 preflight PASS; no container started; no device created`, and exits 0. The normal path prints only the report summary and evidence path; it never prints raw JSON.

- [ ] **Step 6: Run focused tests and quality gates**

Run:

```bash
uv run pytest tests/test_partner_stub_container_runner.py -q
uv run ruff check tools/brilliant_partner_stubs/container_runner.py tests/test_partner_stub_container_runner.py
uv run mypy --strict tools/brilliant_partner_stubs/container_runner.py tests/test_partner_stub_container_runner.py
```

Expected: all commands exit 0.

- [ ] **Step 7: Commit**

```bash
git add tools/brilliant_partner_stubs/container_runner.py \
  tests/test_partner_stub_container_runner.py
git commit -m "test: add isolated ARM partner stub runner"
```

---

### Task 3: Probe stock stub selection and direct shade behavior

**Files:**
- Create: `tools/brilliant_partner_stubs/firmware_probe.py`
- Test: `tests/test_partner_stub_firmware_probe.py`

**Interfaces:**
- Consumes: `CONTRACTS`, `CandidateEvidence`, and the exact candidate order emitted by `container_runner.py`.
- Produces: one `Gate0Report` JSON object on stdout; all diagnostic details remain in bounded in-memory event lists.
- ARM entry point: `python3.10 -B /work/tools/brilliant_partner_stubs/firmware_probe.py --candidate shade --candidate lock`.

- [ ] **Step 1: Write host-side import and safety tests**

```python
def test_firmware_probe_imports_without_loading_firmware() -> None:
    module = importlib.import_module("tools.brilliant_partner_stubs.firmware_probe")
    assert module.CAPTURED_EXTENSION_SUFFIX.endswith("arm-linux-gnueabi.so")
    assert not any(name.startswith("peripherals.") for name in sys.modules)


def test_firmware_probe_source_contains_no_live_boundary() -> None:
    source = Path("tools/brilliant_partner_stubs/firmware_probe.py").read_text()
    forbidden = ("sshpass", "SSHPASS", "aiomqtt", "homeassistant", "server_socket")
    assert [term for term in forbidden if term in source] == []
```

Parse the module AST and assert every `peripherals`, `thrift_types`, `lib`, and `aiohttp` import occurs inside `configure_arm_imports`, `install_network_tripwires`, or a function whose name begins with `probe_`; no firmware import may execute on host import.

- [ ] **Step 2: Run the source-safety test and verify failure**

Run: `uv run pytest tests/test_partner_stub_firmware_probe.py -q`

Expected: FAIL because `tools.brilliant_partner_stubs.firmware_probe` does not exist.

- [ ] **Step 3: Add the ARM extension loader and network tripwires**

```python
CAPTURED_EXTENSION_SUFFIX = ".cpython-310-arm-linux-gnueabi.so"


def configure_arm_imports() -> None:
    firmware_site = Path("/firmware/env/lib/python3.10/site-packages")
    if platform.machine() not in {"armv7l", "armv8l"}:
        raise ProbeBlocked("captured firmware probe requires linux/arm/v7")
    if not firmware_site.is_dir():
        raise ProbeBlocked("read-only firmware site-packages mount is absent")
    loader_details = (
        (
            importlib.machinery.ExtensionFileLoader,
            [*importlib.machinery.EXTENSION_SUFFIXES, CAPTURED_EXTENSION_SUFFIX],
        ),
        (importlib.machinery.SourceFileLoader, importlib.machinery.SOURCE_SUFFIXES),
        (importlib.machinery.SourcelessFileLoader, importlib.machinery.BYTECODE_SUFFIXES),
    )
    sys.path_hooks.insert(0, importlib.machinery.FileFinder.path_hook(*loader_details))
    sys.path.insert(0, str(firmware_site))
    sys.path.insert(0, "/work")
    sys.path_importer_cache.clear()
```

`install_network_tripwires(attempts)` replaces `socket.socket` with a subclass whose `connect` and `connect_ex` append only the address family and operation name, then raise `NetworkDenied`; replaces `socket.create_connection`, `asyncio.open_connection`, and `asyncio.start_server` with raising functions; and, after importing `aiohttp`, replaces `ClientSession._request` with an async raising method that records only the HTTP method. It never records a URL, host, address, header, body, or credential. The Docker network namespace is the primary boundary; these hooks provide zero-call evidence.

Read `/proc/net/route` and require no route row other than the header. Record `docker_network_disabled=True`, `non_loopback_route_count=0`, and `physical_devices_available=False`.

- [ ] **Step 4: Construct one synthetic `PeripheralInfo` per contract**

```python
def build_peripheral_info(contract: CandidateContract) -> object:
    from thrift_types.configuration.ttypes import PeripheralInfo

    return PeripheralInfo(
        name=contract.peripheral_name,
        owner=contract.integration_id,
        peripheral_type=contract.peripheral_type,
        thirdparty_device_id=contract.thirdparty_device_id,
        configuration_peripheral_id=contract.configuration_peripheral_id,
        configuration_variables=dict(contract.configuration_variables),
        stubbed=True,
    )
```

Serialize the object through the captured `peripheral_interface_helpers` exactly once in memory to prove the declared configuration type accepts it, deserialize it immediately, and assert every field round-trips. Do not call any message-bus method and do not write the serialized bytes to disk or stdout.

- [ ] **Step 5: Exercise the stock `stubbed=True` selector**

Create a `SelectorHost` subclass of captured `ConditionalPeripheralHost` with an empty initializer and a `get_module_path_for_peripheral_type` override that returns the candidate's stock module. Call the captured `ConditionalPeripheralHost.__dict__["get_module_path_for_peripheral_info"]` unbound with the synthetic info. Normalize a returned string/module/startable config to its module name and require the candidate's exact `stub_module`.

Repeat with a copy whose `stubbed=False` and require the exact `stock_module`. Separately import the stub module and require:

```python
stub_class = getattr(stub_module, contract.stub_class)
stub_config = stub_module.__startable_config__
assert stub_config.stub_class is stub_class
assert stub_config.non_stub_startable_config is stock_module.__startable_config__
```

Any missing field, incompatible signature, subclass prohibition, or unexpected module becomes a sanitized `blocked_reason` such as `conditional selector contract changed`; do not emit a traceback.

- [ ] **Step 6: Drive the Hunter Douglas stub without its real gateway host**

Build a dynamic subclass of `HunterDouglasShadePeripheralStub` whose initializer creates `probe_events`, whose `_update_peripheral_status` records the numeric status, and whose `_record_value` records variable name and normalized scalar value. Invoke the captured `start()` and require one `PeripheralStatus.ONLINE` event. Invoke `_set_position` with `position=37` and `position_type=PositionType.PRIMARY`; accept positional or keyword binding only when `inspect.signature` names the parameters exactly.

Require one `position=37` record, no `secondary_position`/`tilt_position`/`continuous` record, and zero network attempts. Then invoke the same setter under an `ha_feedback` phase with `position=64`. If it emits the same setter/state-delta shape as the UI phase and contains no origin field, record `feedback_command_count=1` and `feedback_origin_distinguishable=False`; do not hide that result with a harness-side equality filter.

Because `start()` reports `ONLINE` without receiving an upstream-health input, record `stock_online_without_upstream_health=True`. This is expected to make the stock shade candidate `NO_GO` unless the captured behavior differs.

- [ ] **Step 7: Run host-side tests and quality gates**

Run:

```bash
uv run pytest tests/test_partner_stub_firmware_probe.py -q
uv run ruff check tools/brilliant_partner_stubs/firmware_probe.py tests/test_partner_stub_firmware_probe.py
uv run mypy --strict tools/brilliant_partner_stubs/firmware_probe.py tests/test_partner_stub_firmware_probe.py
```

Expected: all commands exit 0; no captured ARM module is imported by these host-side tests.

- [ ] **Step 8: Commit**

```bash
git add tools/brilliant_partner_stubs/firmware_probe.py \
  tests/test_partner_stub_firmware_probe.py
git commit -m "test: probe captured partner stub contracts"
```

---

### Task 4: Add lifecycle, ownership, deletion, and lock probes

**Files:**
- Modify: `tools/brilliant_partner_stubs/firmware_probe.py`
- Modify: `tools/brilliant_partner_stubs/report.py`
- Modify: `tests/test_partner_stub_firmware_probe.py`
- Modify: `tests/test_partner_stub_report.py`

**Interfaces:**
- Consumes: the same in-memory `PeripheralInfo` objects from Task 3.
- Produces: `ProbeEvent`, `LifecycleEvidence`, and `lifecycle_from_events(candidate: Candidate, events: Sequence[ProbeEvent]) -> LifecycleEvidence`.
- Produces: lifecycle fields `unrelated_entry_unchanged`, `owner_relay_mismatch_rejected`, `exact_source_removed`, and `deletion_timestamp_ms` for both candidates.
- Lock behavior is limited to `locked=True`; `locked=False` is a local guard failure and never reaches the captured method.

- [ ] **Step 1: Write failing lifecycle/result tests**

Add tests that pass synthetic event streams into the pure result builder and assert:

```python
def test_exact_remove_requires_source_absence_and_timestamp() -> None:
    result = lifecycle_from_events(
        Candidate.SHADE,
        events=(
            event("add", "gate0_hunter_douglas_shade_v1", timestamp_ms=100),
            event("unrelated_digest", "before", value="a" * 64),
            event("update", "gate0_hunter_douglas_shade_v1", timestamp_ms=200),
            event("delete", "gate0_hunter_douglas_shade_v1", timestamp_ms=300),
            event("source_absent", "gate0_hunter_douglas_shade_v1", value=True),
            event("unrelated_digest", "after", value="a" * 64),
        ),
    )
    assert result.exact_source_removed is True
    assert result.deletion_timestamp_ms == 300
    assert result.unrelated_entry_unchanged is True
```

Implement the pure event types in `report.py`:

```python
@dataclass(frozen=True, slots=True)
class ProbeEvent:
    action: str
    subject: str
    timestamp_ms: int | None = None
    value: str | int | bool | None = None


@dataclass(frozen=True, slots=True)
class LifecycleEvidence:
    unrelated_entry_unchanged: bool
    owner_relay_mismatch_rejected: bool
    exact_source_removed: bool
    deletion_timestamp_ms: int | None
```

`lifecycle_from_events` requires exactly one `unrelated_digest` event for `before` and one for `after`; compares their 64-lowercase-hex values with `hmac.compare_digest`; requires one positive target `delete` timestamp and a later `source_absent=True`; and sets the owner/relay field only when both mismatch scenarios contain zero registration and zero status calls. Duplicate, out-of-order, unknown-subject, or unknown-action events raise `ReportError`.

Assert a delete of only the materialized peripheral, a zero timestamp, a remaining declarative source, or changed unrelated digest fails. Assert an owner/relay mismatch passes only when no registration/status call reaches the fake client. Assert the lock guard raises `UnsafeLockAction("unlock is outside Gate 0")` before the stock method call count changes.

- [ ] **Step 2: Run the focused tests and verify failure**

Run:

```bash
uv run pytest tests/test_partner_stub_firmware_probe.py \
  tests/test_partner_stub_report.py -q
```

Expected: FAIL because lifecycle event evaluation and the lock guard are absent.

- [ ] **Step 3: Probe add/update/remove partitioning with stock helpers**

Use captured `lib.tools.peripheral_interface_helpers.format_dynamic_variable_name` and `lib.tools.peripheral_interface_helpers.serialize_peripheral_variable` to build exactly two configuration variables: the target and `process_config:gate0_unrelated_v1`. The unrelated `PeripheralInfo` uses the same candidate owner but `stubbed=False`, a different name/type-safe ID, and is never instantiated.

Invoke captured `ConditionalPeripheralHost._get_partitioned_configuration_variables` on initial, target-updated, and target-removed maps. Normalize the returned partitions without including serialized values in the report. Require:

- initial partition contains both names exactly once;
- update changes only the target's canonical SHA-256 digest;
- removal leaves the unrelated canonical digest unchanged;
- removal is driven by absence of the exact `process_config:<target>` source, not deletion of a materialized peripheral alone.

If the captured partition helper cannot be driven without a real message bus, record `blocked_reason="conditional lifecycle requires unavailable bus state"`; do not replace stock behavior with a simulated host and do not claim lifecycle proof.

- [ ] **Step 4: Probe framework deletion with a fake processor client**

Create an allocated/subclassed `ConditionalPeripheralHost` with a fake `processor.client` exposing async `delete_peripheral` and `set_variables_request` recorders. Call the captured method exactly as established elsewhere in this repository:

```python
delete_impl = ConditionalPeripheralHost.__dict__["delete_peripheral"]
timestamp_ms = time.time_ns() // 1_000_000
result = delete_impl(host, contract.peripheral_name, timestamp_ms)
if inspect.isawaitable(result):
    await result
```

Require exactly one deletion call for the target and none for `gate0_unrelated_v1`; require the recorded timestamp to equal `timestamp_ms` and be positive. The fake client rejects every device ID or peripheral name outside these two synthetic values.

- [ ] **Step 5: Probe owner/relay mismatch rejection**

Subclass captured `VirtualPeripheralHost` with `am_owner=True`, `am_relay_device=False`, an empty initializer, and a fake processor client that records registration/status calls. Invoke `_register_peripheral_with_message_bus` and `_update_peripheral_status` through their captured unbound methods using only the synthetic target. Require both paths to produce zero fake-client calls. Repeat with `am_owner=False`, `am_relay_device=True` and require the same rejection.

The probe may record only the booleans and call counts. It must not emit the captured log text. A constructor/layout incompatibility yields `blocked_reason="owner relay guard contract changed"`.

- [ ] **Step 6: Drive the RemoteLock stub and enforce lock-only scope**

Build the RemoteLock dynamic subclass with the same `_update_peripheral_status` and `_record_value` recorders used for the shade. Require `start()` to report `PeripheralStatus.ONLINE`, then invoke `_make_lock_request(locked=True)` exactly once and require one `locked=True` state delta and zero network attempts.

Implement the guard before dispatch:

```python
def require_safe_lock_action(locked: bool) -> None:
    if not locked:
        raise UnsafeLockAction("unlock is outside Gate 0")
```

Attempt `locked=False` only against this guard and assert the captured method call count remains one. Drive a second `locked=True` call under `ha_feedback`; if its event shape is indistinguishable from the UI call, record one feedback command and no origin distinction. Record forced-online availability exactly as for the shade.

- [ ] **Step 7: Enforce candidate ordering and early stop semantics**

The main coroutine always evaluates shade first. It may evaluate the lock to gather offline evidence after a shade `NO_GO`, but the report includes `lock_next_stage_eligible=False` whenever shade is not `PASS`. If shade is `BLOCKED`, record the lock as `BLOCKED` with `blocked_reason="shade contract blocked prerequisite"` without importing RemoteLock modules.

The process exits 0 when it emits one schema-valid report, including legitimate `NO_GO` results. It exits 2 with a single sanitized JSON error only when the report itself cannot be constructed. The host runner treats any nonzero exit as a harness failure, not a candidate verdict.

- [ ] **Step 8: Run focused tests and commit**

Run:

```bash
uv run pytest tests/test_partner_stub_firmware_probe.py tests/test_partner_stub_report.py -q
uv run ruff check tools/brilliant_partner_stubs tests/test_partner_stub_firmware_probe.py tests/test_partner_stub_report.py
uv run mypy --strict tools/brilliant_partner_stubs tests/test_partner_stub_firmware_probe.py tests/test_partner_stub_report.py
```

Expected: all commands exit 0.

```bash
git add tools/brilliant_partner_stubs/firmware_probe.py \
  tools/brilliant_partner_stubs/report.py \
  tests/test_partner_stub_firmware_probe.py \
  tests/test_partner_stub_report.py
git commit -m "test: complete partner stub Gate 0 lifecycle probes"
```

---

### Task 5: Document and execute the no-device Gate 0 workflow

**Files:**
- Create: `docs/brilliant-panel/runbooks/native-partner-stub-gate0.md`
- Modify: `docs/brilliant-panel/README.md`
- Modify after evidence exists: `docs/brilliant-panel/native-partner-stub-feasibility.md`
- Evidence only, ignored: `artifacts/brilliant-panel/pilots/partner-stubs/<run-id>/gate0.json`

**Interfaces:**
- Consumes: the runner CLI and schema-valid report from Tasks 1–4.
- Produces: an operator-safe runbook and one sanitized, ignored Gate 0 evidence record.
- This task does not expose a production apply mode.

- [ ] **Step 1: Write the runbook before executing the harness**

The runbook opens with:

```markdown
> This workflow creates no Brilliant, Home Assistant, Hunter Douglas, or
> RemoteLock device. It never connects to a panel. Every `PeripheralInfo` is
> synthetic and exists only in a disposable, networkless ARM process.
```

Document the exact firmware root, pinned image digest, preflight command, run command, three verdict meanings, every stop condition, and the rule that `PASS` permits Gate 1 read-only inventory only. State that `NO_GO` is a useful completed result and that no user should manually run `tools.test add_stubs` in response.

- [ ] **Step 2: Prepare the pinned image explicitly outside the test container**

With Docker Desktop running, execute:

```bash
docker pull --platform linux/arm/v7 \
  docker.io/arm32v7/python@sha256:b543951a164e971a90609c512e04a73d2cad681aadcd5916dc081d2028cb0848
docker image inspect \
  docker.io/arm32v7/python@sha256:b543951a164e971a90609c512e04a73d2cad681aadcd5916dc081d2028cb0848 \
  --format '{{.Os}}/{{.Architecture}} {{index .RepoDigests 0}}'
```

Expected: `linux/arm` and a repo digest ending in `b543951a164e971a90609c512e04a73d2cad681aadcd5916dc081d2028cb0848`. This pull is the only networked preparation; the Gate 0 container itself always uses `--pull never --network none`.

- [ ] **Step 3: Run repository tests before ARM execution**

Run:

```bash
uv run pytest \
  tests/test_partner_stub_contracts.py \
  tests/test_partner_stub_report.py \
  tests/test_partner_stub_container_runner.py \
  tests/test_partner_stub_firmware_probe.py -q
```

Expected: all tests pass, with no skipped partner-stub tests.

- [ ] **Step 4: Run no-start preflight**

Choose a UTC run ID such as `20260720T210000Z` and run:

```bash
uv run python -m tools.brilliant_partner_stubs.container_runner \
  --firmware-root artifacts/brilliant-panel/v26.06.03.1/extracted/data/switch-embedded \
  --output artifacts/brilliant-panel/pilots/partner-stubs/20260720T210000Z/gate0.json \
  --preflight-only
```

Expected: `Gate 0 preflight PASS; no container started; no device created`. If Docker Desktop is unavailable, a digest differs, or the image is absent, record the environment as blocked and stop without weakening the check.

- [ ] **Step 5: Execute the bounded ARM probe**

Run the same command without `--preflight-only`:

```bash
uv run python -m tools.brilliant_partner_stubs.container_runner \
  --firmware-root artifacts/brilliant-panel/v26.06.03.1/extracted/data/switch-embedded \
  --output artifacts/brilliant-panel/pilots/partner-stubs/20260720T210000Z/gate0.json
```

Expected harness invariants in the saved report:

- firmware `v26.06.03.1`;
- image digest `b543951a…0848`;
- network mode `none`, zero non-loopback routes, and zero recorded network attempts;
- synthetic identity only and no physical devices;
- candidate order `shade`, then `lock`;
- no unlock invocation;
- one derived verdict per candidate.

Do not predetermine the candidate verdict. With the currently recovered behavior, forced `ONLINE` and indistinguishable feedback are expected to produce `NO_GO`; that is a successful execution of the research gate and a stop before Gate 1 mutation work.

- [ ] **Step 6: Validate ignored evidence and privacy**

Run:

```bash
git check-ignore -v artifacts/brilliant-panel/pilots/partner-stubs/20260720T210000Z/gate0.json
uv run python -m tools.brilliant_partner_stubs.container_runner \
  --summarize artifacts/brilliant-panel/pilots/partner-stubs/20260720T210000Z/gate0.json
rg -n -i 'password|authorization|access_token|home_id|room_id|ssid|BEGIN .*PRIVATE' \
  artifacts/brilliant-panel/pilots/partner-stubs/20260720T210000Z/gate0.json
```

Expected: Git reports the `artifacts/` ignore rule; summary prints only firmware, isolation invariants, and candidate verdict/reasons; the privacy scan returns no matches.

- [ ] **Step 7: Record the observed conclusion without broadening authorization**

Update `native-partner-stub-feasibility.md` with the execution timestamp, firmware/image digests, isolation result, shade verdict, lock verdict, and the exact failed/blocked assertions. Use only sanitized values from `Gate0Report.summary()`.

If either candidate is `NO_GO` or `BLOCKED`, retain the production `NO-GO`, name the remaining blocker, and do not add a production device. If both are `PASS`, state only that Gate 1 read-only baseline is eligible; require a separate plan and explicit approval before any configuration-variable write.

Add the runbook link to `docs/brilliant-panel/README.md` and the feasibility document's Ordered validation gates section.

- [ ] **Step 8: Run the full repository gate**

Run:

```bash
uv run ruff check --fix
uv run ruff format
uv run mypy --strict src tests
uv run pytest
git diff --check
git status --short --ignored
```

Expected: quality commands and tests exit 0; `git diff --check` prints nothing; only the intended source/docs/tests are unignored; the Gate 0 JSON appears only as ignored evidence.

- [ ] **Step 9: Commit source and sanitized documentation, never evidence**

```bash
git add docs/brilliant-panel/runbooks/native-partner-stub-gate0.md \
  docs/brilliant-panel/native-partner-stub-feasibility.md \
  docs/brilliant-panel/README.md
git diff --cached --check
git commit -m "docs(panel): record partner stub Gate 0 verdict"
```

Before committing, run `git diff --cached --name-only` and require that no path begins with `artifacts/`.

---

## Completion boundary

This plan is complete when the reusable harness is tested, the ARM probe either produces one schema-valid sanitized report or records a fail-closed environment blocker, and the documentation records the resulting Gate 0 verdict. Completion does not include a Home Assistant entity, Brilliant tile, partner account, production `process_config:*` variable, panel deployment, or physical/device action.
