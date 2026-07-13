# Brilliant Virtual Control Feasibility Gates Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Determine, with one disposable officially provisioned Virtual Control, whether Brilliant native HA tiles are removable, isolated, operationally safe, and materially useful—including their real WAN dependency—before any production native transport is designed.

**Architecture:** A repository-safe probe package creates redacted, hash-verifiable gate records while all tokens, certificates, bootstrap blobs, `/var` captures, packet captures, and live logs remain root-only on the selected panel or under gitignored artifacts. Gates VC0–VC5 execute strictly in order; provisioning is impossible without a fresh approval file, runtime tests use one supervised Virtual Control identity, and any failed/blocked gate stops expansion while leaving the safe scene bridge as the supported path.

**Tech Stack:** Python 3.10 on-panel Brilliant libraries, stdlib-only off-panel gate tooling, Brilliant official app workflow, root-only filesystem storage, SSH, MQTT, Home Assistant, router/firewall WAN isolation, `/proc` resource sampling, pytest, ruff, mypy strict, uv.

## Global Constraints

- Never provision an account-visible device without a fresh operator approval immediately before the write.
- Use only an official Brilliant app/device-add workflow or a directly observed supported request made by that workflow.
- Do not blind-guess GraphQL mutation names or production API fields.
- Never print, log, commit, upload, or publish panel private keys, PKCS#12 material, Brilliant passwords, MFA codes, bootstrap tokens, account JWTs, refresh tokens, or certificate contents.
- Keep returned Virtual Control identity material under `/data/brilliant-vc/identity/` with directory mode `0700` and file mode `0600`; never copy it into the repository.
- Never bid on or overwrite ownership of `brilliant_virtual_device`, `configuration_virtual_device`, `ble_mesh`, or a physical Control.
- Never run the Virtual Control identity on more than one host and never implement automatic failover in this feasibility track.
- Abort on sustained agent CPU above 15%, RSS above 100 MiB, new peer-add timeouts, Brilliant cloud-peer disconnects, operator-observed physical-control lag, or inability to prove cleanup.
- Every live process has a hard runtime limit, SIGTERM/SIGINT cleanup, and an idempotent second-snapshot verification path.
- Binary dumps, `/var` collections, packet captures, generated Ghidra projects, credentials, and pilot logs stay under gitignored `artifacts/brilliant-panel/pilots/virtual-control/` or root-only panel paths.
- A failed or blocked Virtual Control gate does not block, disable, or roll back the HA/MQTT control plane or scene bridge.
- This plan ends with a feasibility decision. Production native-host implementation receives a new plan only after VC5 passes; no multi-entity, lock, shade, or garage hosting belongs here.

---

## File map

- Create `tools/brilliant_vc/__init__.py`: probe package marker.
- Create `tools/brilliant_vc/gates.py`: ordered gate model, redaction-safe ledger, progression validation.
- Create `tools/brilliant_vc/audit.py`: VC0 local/panel prior-state audit that never reads secret contents.
- Create `tools/brilliant_vc/token_check.py`: offline JWT shape/claim validation and fingerprinting without token output.
- Create `tools/brilliant_vc/provision_panel.py`: on-panel, approval-gated call through Brilliant's shipped provisioning client.
- Create `tools/brilliant_vc/monitor.py`: bounded process, bus-health, resource, cloud-peer, and latency sampler.
- Create `tools/brilliant_vc/single_light_pilot.py`: VC5 one-light framework pilot with a typed room assignment and one shared host.
- Create `tests/test_vc_gates.py`, `tests/test_vc_audit.py`, `tests/test_vc_token_check.py`, `tests/test_vc_provision.py`, `tests/test_vc_monitor.py`, and `tests/test_vc_single_light.py`.
- Create `docs/brilliant-panel/runbooks/virtual-control-gates.md`: operator workflow and stop conditions.
- Create `docs/brilliant-panel/virtual-control-gate-schema.json`: committed JSON Schema for redacted evidence.
- Modify `.gitignore`: keep every sensitive/generated live artifact out of Git.

The tool package is not copied by `scripts/build_payload.sh`, not installed by the HA integration, and not started by systemd. The operator copies only the required script into an ignored pilot directory for a bounded gate.

---

### Task 1: Create the ordered, secret-free gate ledger

**Files:**
- Create: `tools/brilliant_vc/__init__.py`
- Create: `tools/brilliant_vc/gates.py`
- Create: `docs/brilliant-panel/virtual-control-gate-schema.json`
- Create: `tests/test_vc_gates.py`

**Interfaces:**
- Produces: `GateName`, `GateStatus`, `GateRecord`, `GateLedger.load(path: Path) -> GateLedger`, `GateLedger.record(gate: GateName, status: GateStatus, summary: str, evidence: Sequence[Evidence]) -> None`, and `GateLedger.save(path: Path) -> None`.
- Ledger path during live work: ignored `artifacts/brilliant-panel/pilots/virtual-control/<run-id>/gate-ledger.json`.

- [ ] **Step 1: Write failing progression and redaction tests**

```python
def test_cannot_pass_vc2_before_vc1() -> None:
    ledger = GateLedger.new(run_id="20260712-office")
    with pytest.raises(GateProgressionError, match="VC1 must pass before VC2"):
        ledger.record(GateName.VC2, GateStatus.PASS, summary="provisioned", evidence=[])

def test_secret_shaped_evidence_is_rejected() -> None:
    with pytest.raises(UnsafeEvidenceError):
        Evidence(kind="note", value="eyJhbGciOiJIUzI1NiJ9.abcdefgh.signature")
```

Test statuses `not_run`, `pass`, `fail`, and `blocked`; immutable earlier passes; a failed/blocked gate prevents later gates; evidence accepts relative artifact paths, SHA-256 digests, counts, durations, booleans, firmware versions, HTTP status, and redacted identifiers only. Reject PEM markers, JWT shapes, base64 values over 256 characters, fields containing token/password/secret/certificate/private-key names, and absolute sensitive paths.

- [ ] **Step 2: Run and verify failure**

Run: `uv run pytest tests/test_vc_gates.py -q`

Expected: FAIL because `tools.brilliant_vc.gates` does not exist.

- [ ] **Step 3: Implement exact ordered gate types**

```python
class GateName(str, Enum):
    VC0 = "VC0"
    VC1 = "VC1"
    VC2 = "VC2"
    VC3 = "VC3"
    VC4 = "VC4"
    VC5 = "VC5"

class GateStatus(str, Enum):
    NOT_RUN = "not_run"
    PASS = "pass"
    FAIL = "fail"
    BLOCKED = "blocked"

GATE_ORDER = tuple(GateName)

@dataclass(frozen=True, slots=True)
class Evidence:
    kind: str
    value: str | int | float | bool
    sha256: str | None = None

@dataclass(frozen=True, slots=True)
class GateRecord:
    gate: GateName
    status: GateStatus
    recorded_at: str
    summary: str
    evidence: tuple[Evidence, ...]
```

Write ledger JSON atomically with `sort_keys=True`, indent 2, UTC timestamps, and no environment dump. Validate against the committed schema before replace.

- [ ] **Step 4: Run tests and quality checks**

Run: `uv run pytest tests/test_vc_gates.py -q && uv run ruff check tools/brilliant_vc tests/test_vc_gates.py && uv run mypy --strict tools/brilliant_vc/gates.py tests/test_vc_gates.py`

Expected: all exit 0.

- [ ] **Step 5: Commit**

```bash
git add tools/brilliant_vc/__init__.py tools/brilliant_vc/gates.py docs/brilliant-panel/virtual-control-gate-schema.json tests/test_vc_gates.py
git commit -m "test: add ordered Virtual Control gate ledger"
```

### Task 2: Implement VC0 prior-state and security audit

**Files:**
- Create: `tools/brilliant_vc/audit.py`
- Create: `tests/test_vc_audit.py`
- Modify: `.gitignore`

**Interfaces:**
- CLI: `python -m tools.brilliant_vc.audit --panel office --snapshot-json PATH --output PATH`.
- The tool consumes an already-redacted device snapshot and a stat-only JSON inventory from the panel; it never opens credential files.

- [ ] **Step 1: Write failing audit tests**

Assert the audit:

- reports firmware version, bus home ID hash, physical Control count, and DeviceType 6 count;
- marks VC0 failed when any type-6 device cannot be explained as pre-existing;
- inventories `/tmp/mirror_poc/.access` and similar paths by existence, owner UID, mode, size, and mtime only;
- marks world/group-readable credential-shaped files failed;
- rejects input containing file content, JWT shape, PEM marker, or PKCS#12 value;
- records whether the July 9 attempt created no VC without asserting that from absence alone—both app inventory and bus/home-graph evidence are required.

- [ ] **Step 2: Run and verify failure**

Run: `uv run pytest tests/test_vc_audit.py -q`

Expected: FAIL because the audit module is absent.

- [ ] **Step 3: Implement safe stat-only audit input**

The panel inventory command must be generated by the tool and limited to these fields:

```python
SAFE_STAT_FIELDS = ("path", "exists", "uid", "gid", "mode", "size", "mtime_ns")
SENSITIVE_PATHS = (
    "/tmp/mirror_poc/.access",
    "/tmp/mirror_poc/.vc_record.json",
    "/data/brilliant-vc/identity",
)
```

Do not hash file contents: even a digest would create an unnecessary stable identifier for a credential. The operator chooses one of two explicit VC0 actions: delete expired prior tokens on-panel, or retain them root-only with a recorded reason and expiry. Never copy them.

- [ ] **Step 4: Add ignore coverage**

Add:

```gitignore
artifacts/brilliant-panel/pilots/virtual-control/
**/virtual-control-identity/
**/vc-captures/
*.pcap
*.pcapng
*.p12
*.pfx
*.pem
*.key
*.token
```

Do not remove already tracked sanitized reverse-engineering outputs.

- [ ] **Step 5: Run tests, then execute VC0 read-only**

Run: `uv run pytest tests/test_vc_audit.py -q`

Expected: PASS.

On Office, collect stat-only inventory and a redacted bus snapshot. In the official Brilliant app, record device count/type/name without screenshots containing personal data. Confirm no unexplained DeviceType 6 exists and no July 9 `.vc_record.json` exists. Save only sanitized outputs under the ignored run directory, then record VC0 PASS. If an unexplained VC exists, record BLOCKED and stop.

- [ ] **Step 6: Commit code only**

```bash
git add tools/brilliant_vc/audit.py tests/test_vc_audit.py .gitignore
git commit -m "test: add Virtual Control prior-state audit"
```

### Task 3: Implement VC1 offline bootstrap-token verification

**Files:**
- Create: `tools/brilliant_vc/token_check.py`
- Create: `tests/test_vc_token_check.py`
- Create: `docs/brilliant-panel/runbooks/virtual-control-gates.md`

**Interfaces:**
- CLI on the panel: `python -m tools.brilliant_vc.token_check --token-file PATH --report PATH`.
- Output contains issuer/audience hashes, issued/expiry times, allowed-path booleans, token SHA-256 prefix (8 hex characters), and no token bytes.

- [ ] **Step 1: Write failing token checks**

Use synthetic JWTs only. Assert account tokens that allow GraphQL but not `/provisioning/virtual-control-self-bootstrap` fail VC1; expired/future tokens fail; an official-workflow token whose claims allow the exact endpoint passes. The checker must not verify cryptographic authenticity—it labels its result “claims-only”; the shipped Brilliant client/server performs actual verification at VC2.

- [ ] **Step 2: Run and verify failure**

Run: `uv run pytest tests/test_vc_token_check.py -q`

Expected: FAIL because the module is absent.

- [ ] **Step 3: Implement claims-only parsing with no token output**

```python
@dataclass(frozen=True, slots=True)
class TokenReport:
    jwt_shape: bool
    expires_at: int | None
    issued_at: int | None
    allows_self_bootstrap: bool
    fingerprint8: str

def inspect_token(raw: bytes, now_s: int) -> TokenReport:
    text = raw.decode("ascii")
    parts = text.split(".")
    if len(parts) != 3:
        raise TokenCheckError("bootstrap token is not JWT-shaped")
    claims = json.loads(_decode_segment(parts[1]))
    allowed = claims.get("allowed_paths", ())
    return TokenReport(
        jwt_shape=True,
        expires_at=_optional_int(claims.get("exp")),
        issued_at=_optional_int(claims.get("iat")),
        allows_self_bootstrap="/provisioning/virtual-control-self-bootstrap" in allowed,
        fingerprint8=hashlib.sha256(raw).hexdigest()[:8],
    )
```

Open the token file with `O_NOFOLLOW`, require UID 0 and mode with no group/other bits, cap at 64 KiB, and overwrite the in-memory bytearray after parsing where practical.

- [ ] **Step 4: Document the only allowed VC1 acquisition workflow**

The runbook requires the official Brilliant app on a test handset/account session. Navigate its supported “add device/control” path and observe the request made by that workflow using normal OS/app diagnostic facilities or an operator-controlled network capture. Do not enumerate mutation names, fuzz fields, or replay unrelated production calls. Outcomes are exact:

- the app has no Virtual Control/device-add path or produces no provisioning-scoped token: VC1 BLOCKED, stop;
- TLS pinning prevents normal observation: VC1 BLOCKED unless the operator separately authorizes app instrumentation;
- the official flow yields a root-only token and `token_check` confirms the exact self-bootstrap path: record capture timestamp, app version, endpoint/path, token fingerprint8, and VC1 PASS.

The capture itself remains ignored; the committed/run ledger stores only the sanitized facts.

- [ ] **Step 5: Run tests and commit**

Run: `uv run pytest tests/test_vc_token_check.py -q && uv run ruff check tools/brilliant_vc/token_check.py tests/test_vc_token_check.py && uv run mypy --strict tools/brilliant_vc/token_check.py tests/test_vc_token_check.py`

Expected: all exit 0.

```bash
git add tools/brilliant_vc/token_check.py tests/test_vc_token_check.py docs/brilliant-panel/runbooks/virtual-control-gates.md
git commit -m "test: verify official Virtual Control bootstrap tokens"
```

### Task 4: Build a one-shot, approval-gated VC2 provisioning client

**Files:**
- Create: `tools/brilliant_vc/provision_panel.py`
- Create: `tests/test_vc_provision.py`

**Interfaces:**
- Panel CLI dry run: `python provision_panel.py --token-file PATH --property-id ID --expected-home-id ID --identity-dir /data/brilliant-vc/identity`.
- Live CLI adds: `--apply --approval-file /run/brilliant-vc-approval.json`.
- Uses shipped `WebAPIProvisioningClient.get_virtual_control_self_bootstrap(home_property_id, token)` against `https://web-api.brilliant.tech` through the panel's device-cert session.

- [ ] **Step 1: Write failing guard/storage tests with fakes**

Assert no network call unless all conditions hold:

1. `--apply` is present;
2. VC0 and VC1 are PASS in the referenced ledger;
3. approval file is root-owned, mode `0600`, less than 10 minutes old, names this run ID/property ID/pilot panel, and contains `approved: true`;
4. token check passes;
5. identity directory does not exist or is empty;
6. no prior VC record exists;
7. expected home ID/property ID are 32 lowercase hex characters.

Test response status other than 200 writes no identity. Status 200 requires `device_id`, `pkcs12_certificate`, and `bootstrap`; decode `BootstrapParameters.target_home_id` and reject/move to quarantine if it differs. Never include response bodies in exceptions or reports.

- [ ] **Step 2: Run and verify failure**

Run: `uv run pytest tests/test_vc_provision.py -q`

Expected: FAIL because the provisioning module is absent.

- [ ] **Step 3: Implement fail-closed provisioning and storage**

```python
def validate_approval(path: Path, *, run_id: str, property_id: str,
                      panel: str, now_s: int) -> None:
    file_stat = path.lstat()
    if not stat.S_ISREG(file_stat.st_mode) or file_stat.st_uid != 0 or file_stat.st_mode & 0o077:
        raise ProvisioningGuardError("approval file must be root-owned mode 0600")
    data = json.loads(path.read_text())
    if now_s - int(data["approved_at_s"]) > 600:
        raise ProvisioningGuardError("approval is older than 10 minutes")
    expected = {"approved": True, "run_id": run_id, "property_id": property_id, "panel": panel}
    if any(data.get(key) != value for key, value in expected.items()):
        raise ProvisioningGuardError("approval scope does not match this request")
```

Create identity directory with `mkdir(mode=0o700)`; write `device_id`, certificate, bootstrap, and decoded non-secret metadata to separate temp files opened with `O_CREAT|O_EXCL|O_NOFOLLOW`, mode `0600`, `fsync`, then atomic rename. Log only HTTP status, device ID redacted to first/last four characters, presence booleans, target-home match, and durations.

- [ ] **Step 4: Run tests and quality checks**

Run: `uv run pytest tests/test_vc_provision.py -q && uv run ruff check tools/brilliant_vc/provision_panel.py tests/test_vc_provision.py && uv run mypy --strict tools/brilliant_vc/provision_panel.py tests/test_vc_provision.py`

Expected: all exit 0.

- [ ] **Step 5: Commit**

```bash
git add tools/brilliant_vc/provision_panel.py tests/test_vc_provision.py
git commit -m "test: add guarded Virtual Control provisioner"
```

### Task 5: Execute VC2 and prove official rollback before hosting

**Files:**
- Update only ignored live ledger/artifacts; no repository source change.

**Interfaces:**
- Produces one account-visible disposable VC, root-only identity, app/home-graph visibility evidence, and a proven official removal path.

- [ ] **Step 1: Reconfirm preconditions without writing**

Run the provisioner without `--apply`; expected output is a redacted request summary and `DRY RUN — no provisioning request sent`. Confirm the official app is logged in, the account device count is known, the exact removal UI is documented through its final confirmation screen, and Office physical controls/forward bridge/cloud peer are healthy.

- [ ] **Step 2: Obtain a fresh operator approval immediately before the write**

Pause execution and request explicit approval naming: one disposable Virtual Control, the target home/property, Office as identity host, cloud account change, root-only identity storage, and the official removal requirement. After approval, create `/run/brilliant-vc-approval.json` root-owned mode `0600` with current epoch and the exact scoped fields. Do not treat the earlier general approval as this fresh write approval.

- [ ] **Step 3: Provision exactly once**

Run the approved `--apply` command once. Expected success: HTTP 200; target home matches; identity files are mode `0600`; the app device count increases by exactly one; bus/home graph shows exactly one new DeviceType 6 identity. Any retry after ambiguous failure is blocked until the app/home graph proves no device was created.

- [ ] **Step 4: Prove the supported rollback path before hosting any peripheral**

In the official app, navigate the disposable VC's removal flow through the final confirmation screen and directly observe the supported removal request shape without submitting it. Confirm the target identity/account/home match, record the app version and removal endpoint/action name, then cancel at the final confirmation. Do not guess or call a private removal mutation. The actual removal and second-snapshot proof occurs in Task 9 after VC5 (or immediately after any failed gate). If no official removal action is offered or its target cannot be verified, record VC2 FAIL and stop before hosting.

- [ ] **Step 5: Record VC2**

Record only redacted device ID, before/after counts, HTTP status, target-home match, file modes, official removal-action availability, and pilot creation duration. VC2 PASS requires one currently visible pilot VC and a verified official removal flow ready for immediate use; final removal success remains a completion condition for the entire track.

### Task 6: Implement and execute VC3 runtime-topology measurements

**Files:**
- Create: `tools/brilliant_vc/monitor.py`
- Create: `tests/test_vc_monitor.py`
- Modify: `docs/brilliant-panel/runbooks/virtual-control-gates.md`

**Interfaces:**
- CLI: `python -m tools.brilliant_vc.monitor --pid PID --duration-s SECONDS --interval-s 5 --output-jsonl PATH`.
- Samples process CPU/RSS, load average, bus socket peer count, reconnect/peer-timeout/cloud-peer log counters, MQTT round-trip marker latency, and physical-control observation markers.

- [ ] **Step 1: Write failing `/proc` and log-counter tests**

Use fixture proc trees. CPU is delta process ticks / delta total ticks; RSS uses resident pages × page size. Assert secrets in journal lines are redacted/dropped; only allowlisted counters are stored. A threshold violation writes one `abort_reason` and invokes the supplied terminator once.

- [ ] **Step 2: Run and verify failure**

Run: `uv run pytest tests/test_vc_monitor.py -q`

Expected: FAIL because monitor does not exist.

- [ ] **Step 3: Implement bounded monitoring and hard aborts**

```python
THRESHOLDS = Thresholds(
    cpu_percent=15.0,
    rss_bytes=100 * 1024 * 1024,
    peer_add_timeouts=1,
    cloud_disconnects=1,
    reconnect_storms=1,
)
```

Require `--duration-s` between 60 and 90,000. On violation send SIGTERM, wait up to 10 seconds, then SIGKILL only to the exact monitored PID. Never kill `message_bus`, `switch-ui`, or the forward `brilliant-mqtt` process by name.

- [ ] **Step 4: Determine the exact shipped VC launcher read-only**

On Office, run the existing read-only introspection of `bus.message_bus.start_as_virtual_control`, `run_as_main`, `BootstrapParameters`, and accepted flagfile/constructor parameters. Record module, callable signature, firmware build, and SHA-256 of the defining `.so`; record no bootstrap/certificate values. If no supported launcher consumes the official returned identity without replacing the physical panel's message bus, mark VC3 BLOCKED and stop.

- [ ] **Step 5: Start only the VC identity under the bounded supervisor**

Use a dedicated process, device ID, data directory, and bus/client name. It must not claim the physical Control device ID or modify the panel's main message-bus configuration. Start with no hosted peripherals, a 10-minute hard limit, and the monitor attached. Confirm the home graph sees the VC and the physical bus peer/loads remain healthy.

- [ ] **Step 6: Measure WAN-up and WAN-off topology**

With WAN up, measure process start/join, cross-panel visibility, and no-op heartbeat propagation. Then isolate only the pilot VC/panel from public internet at the router while allowing RFC1918 LAN, MQTT, HA, DNS as locally provided, SSH, and panel-to-panel traffic. Prove isolation by failed connection to `web-api.brilliant.tech:443` and successful MQTT/HA/LAN probes. Repeat visibility, restart, and propagation measurements. Restore WAN and verify recovery.

Classify the result exactly:

- all runtime paths work with WAN denied: `local`;
- existing state remains but commands/restart/propagation fail: `cloud-dependent`;
- no reliable join/visibility even with WAN up: `not viable`.

Cloud-dependent is not mislabeled local; the operator may still accept it only if measured latency/reliability improves on SmartThings.

- [ ] **Step 7: Run tests, commit code, and record VC3**

Run: `uv run pytest tests/test_vc_monitor.py -q && uv run ruff check tools/brilliant_vc/monitor.py tests/test_vc_monitor.py && uv run mypy --strict tools/brilliant_vc/monitor.py tests/test_vc_monitor.py`

Expected: all exit 0.

```bash
git add tools/brilliant_vc/monitor.py tests/test_vc_monitor.py docs/brilliant-panel/runbooks/virtual-control-gates.md
git commit -m "test: measure Virtual Control runtime topology"
```

Record VC3 PASS only when topology is conclusively classified and restart/visibility are repeatable. `cloud-dependent` may pass feasibility only with an explicit operator acceptance recorded in the ledger; it can never satisfy a claim of local control.

### Task 7: Execute VC4 isolation and 24-hour resource soak

**Files:**
- Update ignored ledger/artifacts and the runbook only if a discovered operational command needs documentation.

**Interfaces:**
- Produces 24 hours of 5-second samples, 1-minute aggregates, abort state, and physical-control observations for the no-peripheral VC runtime.

- [ ] **Step 1: Establish the comparable baseline**

Collect 30 minutes on Office with the VC stopped: process list, load, forward-agent CPU/RSS, bus peers, cloud-peer state, reconnects, peer-add timeouts, and 20 timestamped physical-light interactions. Do not scrape `/var` broadly; collect only allowlisted counters and retain raw logs in the ignored run directory.

- [ ] **Step 2: Start the same single VC runtime for 24 hours**

Use a systemd transient unit or supervisor with `RuntimeMaxSec=90000`, `Restart=no`, `KillMode=control-group`, and no identity copy. The identity path remains root-only and mounted/readable only by the probe process. Attach the monitor from Task 6.

- [ ] **Step 3: Observe controls throughout the soak**

At hours 0, 1, 6, 12, 18, and 24, observe ten operator-performed physical
light interactions, one disposable-light state round trip only when separately
approved, and one UI navigation check. Do not execute Brilliant scenes. The
agent only observes and records physical interaction. Record operator-observed
lag as a boolean and optional non-sensitive note. Any lag, cloud disconnect,
peer timeout, threshold violation, or forward-bridge regression stops the
runtime immediately.

- [ ] **Step 4: Analyze and record VC4**

VC4 PASS requires: no abort, no new peer-add timeout/cloud disconnect/reconnect storm, peak RSS ≤100 MiB, no sustained (five consecutive samples) CPU >15%, no physical lag, and forward bridge availability throughout. Preserve raw samples ignored; ledger stores min/median/p95/max and SHA-256 of the sample file.

### Task 8: Build and execute the VC5 single-native-light pilot

**Files:**
- Create: `tools/brilliant_vc/single_light_pilot.py`
- Create: `tests/test_vc_single_light.py`
- Modify: `docs/brilliant-panel/runbooks/virtual-control-gates.md`

**Interfaces:**
- CLI on Office: `python -m tools.brilliant_vc.single_light_pilot --vc-identity-dir /data/brilliant-vc/identity --topology-json <root-only-ignored-snapshot> --ledger <ignored-ledger> --run-id <same-ledger-run-id> --stable-id <uuid> --display-name "HA VC Pilot Light" --room-id <catalog-id> --office-device-id <physical-id> --vc-socket /run/brilliant-vc/server_socket --runtime-s 1800`.
- Exactly one `PeripheralHost`, one LIGHT peripheral, one registration at a time, and one MQTT entity command/state route.

- [ ] **Step 1: Write failing schema/guard/lifecycle tests with firmware fakes**

Assert the hosted light contains exact variables/types:

```python
{
    "on": (int, True, 0),
    "intensity": (int, True, 500),
    "dimmable": (int, False, 1),
    "max_intensity_value": (int, False, 1000),
    "minimum_dim_level": (int, True, 100),
    "maximum_dim_level": (int, True, 1000),
    "display_name": (str, True, "HA VC Pilot Light"),
    "room_assignment": (RoomAssignment, True, RoomAssignment(room_ids=[room_id])),
    "mode_transition_settings": (str, True, "{}"),
    "configuration_peripheral_id": (str, False, vc_configuration_id),
}
```

Assert HA brightness 0–255 scales round-half-up to Brilliant 0–1000 and back; stable ID determines peripheral name; display rename does not change it; only the VC device ID is passed as `virtual_device_id`; missing room/config linkage fails before registration; SIGTERM deletes the pilot and verifies absence; second cleanup is successful/no-op.

- [ ] **Step 2: Run and verify failure**

Run: `uv run pytest tests/test_vc_single_light.py -q`

Expected: FAIL because the pilot module is absent.

- [ ] **Step 3: Implement one typed framework peripheral and shared host**

Defer every Brilliant import until live adapter execution. Decode the room
catalog with a scoped `configuration_virtual_device/home_configuration` read
and require the supplied room ID to exist. Discover the provisioned VC's own
configuration peripheral from its home-graph record; never borrow
`brilliant_virtual_device_configuration` or a physical Control configuration.
Require exactly one configuration-type candidate on the VC's own Device record
and compare it with a root-only, ignored topology snapshot. Both dry run and
live mode re-read these facts from the dedicated VC socket before registration;
the complete room and peripheral sets must match. Canonicalize the socket and
reject traversal or symlink escape to the physical Control bus.

Use `PeripheralHost`/`HostedStartableSpec` with `virtual_device_id=<provisioned VC device ID>`. Reject `None`, `brilliant_virtual_device`, `configuration_virtual_device`, `ble_mesh`, or the Office physical ID. The push callbacks publish v1 entity commands to HA; retained v1 state updates drive pull/state variables. No direct HA WebSocket/token is accepted.

Keep one host across bounded MQTT reconnect sessions. Fence commands on
disconnect or HA unavailability, require authoritative retained state after
each resubscribe, and accept HA sequence resets only when `generated_at_ms`
proves a newer publication epoch. Establish the total runtime deadline before
registration and reserve bounded time for deletion and two absence reads.
Acquire one nonblocking root-owned lease in the canonical VC runtime before
apply-mode live preflight and hold it through cleanup so concurrent pilot
processes cannot race the single registration.

- [ ] **Step 4: Run off-panel tests and a preflight dry run**

Run: `uv run pytest tests/test_vc_single_light.py -q`

Expected: PASS.

Run without `--apply`; expected output lists only the redacted VC ID, stable
peripheral ID, display name, boolean room/config validations, runtime, topics,
and `DRY RUN — no host started`. This performs scoped read-only bus preflight
but does not start a host.

- [ ] **Step 5: Run the bounded live light test**

Start monitor first, then the pilot for at most 30 minutes. Validate on Office and a second panel:

1. tile renders in the intended room with correct display name;
2. the tile's on-screen on/off and slider commands reach MQTT/HA exactly once;
3. the Virtual-Control-owned light appears as a selectable target in the
   Office panel's native physical-slider settings UI; do not infer eligibility
   merely from tile rendering or the light's peripheral type;
4. before changing any binding, snapshot the complete selected Office slider
   configuration and its default/wired-gang behavior, then obtain explicit
   operator approval naming that physical slider;
5. bind the pilot light through the native settings UI, never through a guessed
   raw variable write, and verify one physical tap plus one physical dimming
   gesture reaches MQTT/HA exactly once and feeds state back to both panels;
6. verify the bound slider does not operate, stall, or alter its prior wired or
   default gang and that every other Office load remains responsive;
7. HA on/off/brightness updates render on both panels and on the bound physical
   slider's feedback surface, if the firmware exposes one;
8. restart HA, MQTT, pilot process, Office panel, and the second panel one at a
   time, verifying both tile operation and the approved slider binding after
   each restart;
9. temporarily remove network, restore it, and verify reconciliation;
10. repeat WAN-denied behavior from VC3 and record latency;
11. restore the physical slider's exact baseline binding through the native UI
    and verify its original wired/default behavior before ending the pilot;
12. physical Office loads remain responsive and monitor thresholds remain clear.

The physical-slider sub-gate is **BLOCKED**, not passed, if the hosted light is
absent from the native selector. Do not bypass that selector by hand-crafting a
`slider_config`; the selector's eligibility checks and any associated metadata
are part of the behavior being validated.

- [ ] **Step 6: Delete and prove no phantom**

Terminate normally, issue timestamped deletion if the framework did not remove it, and take two scoped VC snapshots at least 30 seconds apart. Confirm the peripheral is absent on both panels and the app/home graph while the VC identity itself remains. Run cleanup a second time and require success/no-op.

Also re-read the complete Office physical-slider configuration twice and prove
that it matches the pre-pilot snapshot exactly and contains no reference to the
Virtual Control device ID or pilot peripheral ID. A stale binding is a cleanup
failure even when the native tile has disappeared.

- [ ] **Step 7: Run quality checks, commit code, and record VC5**

Run: `uv run pytest tests/test_vc_single_light.py -q && uv run ruff check tools/brilliant_vc/single_light_pilot.py tests/test_vc_single_light.py && uv run mypy --strict tools/brilliant_vc/single_light_pilot.py tests/test_vc_single_light.py`

Expected: all exit 0.

```bash
git add tools/brilliant_vc/single_light_pilot.py tests/test_vc_single_light.py docs/brilliant-panel/runbooks/virtual-control-gates.md
git commit -m "test: validate one Virtual Control native light"
```

VC5 PASS requires every rendering/control/restart/network/cleanup check,
including native physical-slider selection, operation, restoration, and no
safety abort. Any persistent tile or slider-binding phantom is FAIL.

### Task 9: Close the feasibility track and remove the disposable identity

**Files:**
- Create after the run: `docs/brilliant-panel/virtual-control-feasibility.md`
- Modify: `docs/brilliant-panel/home-assistant-integration.md`

**Interfaces:**
- Produces one of three decisions: `blocked`, `rejected`, or `eligible_for_native_transport_plan`.

- [ ] **Step 1: Generate a sanitized evidence summary**

Summarize every gate status, firmware/app versions, topology classification, latency distributions, resource aggregates, two-panel rendering, restart matrix, and cleanup proof. Link artifact SHA-256 values without committing raw artifacts or absolute panel paths. State cloud dependency plainly.

- [ ] **Step 2: Apply the decision rule**

- Any BLOCKED gate → decision `blocked`; scene bridge remains supported.
- Any FAIL gate or unacceptable cloud dependency/resource result → `rejected`; remove the pilot VC.
- VC0–VC5 all PASS → `eligible_for_native_transport_plan`; this authorizes planning, not production deployment.

- [ ] **Step 3: Remove the disposable VC through the official app**

Stop the runtime, confirm no hosted peripherals, remove the VC through the already-proven official path, verify account/home graph absence from a second snapshot, then securely delete `/data/brilliant-vc/identity/` and `/run/brilliant-vc-approval.json` on Office. Record counts, modes, and absence only. A failed removal changes the decision to `rejected`.

- [ ] **Step 4: Run repository secret and artifact scans**

Run:

```bash
git status --short
git check-ignore -v artifacts/brilliant-panel/pilots/virtual-control/example/identity.p12
rg -n "BEGIN (RSA |EC |OPENSSH )?PRIVATE KEY|pkcs12_certificate|server_authentication_token|refresh_token|eyJ[A-Za-z0-9_-]+\.eyJ" --glob '!artifacts/**' --glob '!docs/claude/research/**' .
```

Expected: live artifacts are ignored; the secret scan finds no newly introduced value. Field-name mentions in probe code are reviewed and contain no values.

- [ ] **Step 5: Run full repository gates and commit the conclusion**

Run: `uv run ruff check && uv run ruff format --check && uv run mypy --strict src tests tools && uv run pytest`

Run the four HA quality commands from the baseline implementation plan.

Expected: all exit 0.

```bash
git add docs/brilliant-panel/virtual-control-feasibility.md docs/brilliant-panel/home-assistant-integration.md
git commit -m "docs: record Virtual Control feasibility decision"
```

If and only if the decision is `eligible_for_native_transport_plan`, create a new design/implementation plan from the observed VC identity launcher, its own configuration peripheral, verified room linkage, real WAN topology, restart semantics, resource envelope, and VC5 light schema. That later plan may cover the shared multi-entity host and additional domains; this feasibility branch must not grow them opportunistically.

---

## Completion evidence

- VC0–VC5 ordered ledger with sanitized evidence and artifact digests.
- Fresh provisioning approval timestamp less than ten minutes before the single write.
- Official token provenance and exact endpoint allow-path confirmation without token bytes.
- Official removal rehearsal before hosting and final removal after testing.
- WAN-up/WAN-denied topology classification with successful isolation proof.
- 24-hour resource/physical-control soak and all abort counters.
- Two-panel single-light rendering, bidirectional command/state timing, restart/network matrix, and double-snapshot cleanup.
- Secret scan, ignored artifact verification, and root-only identity deletion.
