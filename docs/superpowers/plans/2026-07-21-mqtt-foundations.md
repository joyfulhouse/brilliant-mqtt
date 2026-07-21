# MQTT Validation and Panel-Agent Foundations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (- [ ]) syntax for tracking.

**Goal:** Add the shared MQTT validation, strict TLS, panel preflight, and retained-topic ownership primitives required by fleet onboarding without changing the current one-entry-per-panel setup behavior.

**Architecture:** The Python 3.10 panel agent and Python 3.14 Home Assistant integration implement the same versioned setup-wire contract from a golden vector. The panel runtime and its temporary preflight share one MQTT client factory, including strict server-authenticated TLS. Home Assistant validates a normalized broker profile through both its already-connected MQTT client and a temporary device-principal client. A durable panel-owned retained-topic ledger wraps only retained panel publications; mesh ownership remains unchanged and outside the ledger.

**Tech Stack:** Python 3.10 panel agent, Python 3.14 Home Assistant integration, aiomqtt 2.5.1/Paho MQTT 2.1.0, Home Assistant MQTT APIs, pytest/pytest-homeassistant-custom-component, uv, ruff, mypy strict, Mosquitto 2.0.22 in disposable integration tests.

## Global Constraints

- This is plan 1 of 3. Complete it before [fleet onboarding](2026-07-21-fleet-onboarding.md), then complete [migration and operations](2026-07-21-fleet-migration-operations.md).
- Keep the current config-entry schema, setup screens, and per-panel runtime path unchanged in this plan. New HA services are internal and additive.
- Keep MQTT Discovery fixed at the existing homeassistant prefix. Do not add a configurable discovery prefix.
- The official Mosquitto app is documentation guidance, not a runtime dependency. The validator treats official and external brokers identically after profile normalization.
- Use MQTT 3.1.1 from the panel-compatible aiomqtt/Paho stack. Do not add HTTP, WebSockets, mutual TLS, an insecure TLS toggle, or another resident process.
- TLS always verifies the server certificate chain and hostname. A TLS failure never retries over plaintext.
- Setup IDs are UUIDv4 values generated for one validation attempt. Setup topics are cleaned in a finally block after success, failure, timeout, or cancellation.
- A panel ownership ledger may claim only retained topics owned by that concrete panel slug. It must reject brilliant/mesh and mesh discovery identifiers.
- Keep FakeMqtt.published as the existing three-tuple shape so unrelated tests do not churn; record QoS separately.
- Root source remains Python 3.10. HA source remains Python 3.14. Never import HA code into the panel package.
- Automated tests do not contact a real panel, a production broker, or the Brilliant message bus.
- Run scripts/build_payload.sh after panel package changes and commit the resulting agent payload.

---

## File map

- Create src/brilliant_mqtt/setup_protocol.py: strict setup v1 topic and message contract.
- Create custom_components/brilliant_mqtt/setup_protocol.py: HA-runtime copy of that contract.
- Create tests/fixtures/mqtt_setup_v1_vectors.json: language/runtime-neutral golden vectors.
- Create tests/test_setup_protocol.py and ha/tests/test_setup_protocol.py: exact parity and rejection tests.
- Modify src/brilliant_mqtt/config.py: strict MQTT TLS and retained-ledger environment settings.
- Modify src/brilliant_mqtt/protocols.py, src/brilliant_mqtt/mqttio.py, and tests/fakes.py: QoS-aware MQTT seam and shared strict TLS client construction.
- Create tests/test_mqttio.py: client construction, TLS, and publish acknowledgement tests.
- Create src/brilliant_mqtt/retained_topics.py and tests/test_retained_topics.py: durable bounded ownership ledger.
- Modify src/brilliant_mqtt/bridge.py and src/brilliant_mqtt/__main__.py: route panel-retained publications through the ledger.
- Create src/brilliant_mqtt/preflight.py and tests/test_preflight.py: temporary panel-side validation CLI.
- Create custom_components/brilliant_mqtt/errors.py and ha/tests/test_errors.py: stable typed operation errors.
- Create custom_components/brilliant_mqtt/broker.py and ha/tests/test_broker.py: normalized/redacted fleet broker profile and temporary MQTT client.
- Create custom_components/brilliant_mqtt/broker_validation.py and ha/tests/test_broker_validation.py: end-to-end HA/device validation.
- Modify pyproject.toml, custom_components/brilliant_mqtt/manifest.json, ha/pyproject.toml, and uv.lock: pin the same aiomqtt/Paho device stack in both runtimes.
- Modify custom_components/brilliant_mqtt/panel_ops.py, custom_components/brilliant_mqtt/components.py, ha/tests/test_panel_ops.py, and ha/tests/test_components.py: render TLS and ledger settings into staged panel environments.
- Modify scripts/build_payload.sh and custom_components/brilliant_mqtt/agent_payload/: package the new agent modules and pinned dependencies.
- Create tests/mqtt_broker/mosquitto.conf, tests/mqtt_broker/acl, tests/mqtt_broker/passwords.example, tests/mqtt_broker/tls/: disposable broker fixtures with generated certificates.
- Create scripts/run_mqtt_validation_tests.sh and ha/tests/test_broker_validation_live.py: opt-in plain/TLS/ACL live-broker contract tests.
- Modify ha/pyproject.toml: register the mqtt_live marker.
- Modify INSTALL.md, docs/CONFIGURATION.md, docs/reference/deployment.md, and docs/ha-integration.md: prerequisites, TLS, ACL, and error guidance.

---

### Task 1: Freeze the setup v1 wire contract in both runtimes

**Files:**
- Create: src/brilliant_mqtt/setup_protocol.py
- Create: custom_components/brilliant_mqtt/setup_protocol.py
- Create: tests/fixtures/mqtt_setup_v1_vectors.json
- Create: tests/test_setup_protocol.py
- Create: ha/tests/test_setup_protocol.py

**Interfaces:**
- SetupTopics.for_id(setup_id: UUID) -> SetupTopics
- SetupRequest.from_payload(payload: bytes | str) -> SetupRequest
- SetupResult.from_payload(payload: bytes | str) -> SetupResult
- setup_id is serialized in canonical lowercase hyphenated UUID form.
- For canonical UUID text setup_id, topics are exactly f"brilliant/setup/{setup_id}/panel_to_ha", f"brilliant/setup/{setup_id}/ha_to_panel", f"brilliant/setup/{setup_id}/retained", and f"homeassistant/brilliant_mqtt_setup/{setup_id}/probe".
- Payload objects reject unknown keys, non-string nonces, wrong schema_version, malformed UUIDs, and payloads larger than 16 KiB.

- [ ] **Step 1: Add one golden vector and failing parity tests**

Create tests/fixtures/mqtt_setup_v1_vectors.json with this exact first vector:

~~~json
{
  "schema_version": 1,
  "vectors": [
    {
      "setup_id": "12345678-1234-4abc-8def-1234567890ab",
      "topics": {
        "panel_to_ha": "brilliant/setup/12345678-1234-4abc-8def-1234567890ab/panel_to_ha",
        "ha_to_panel": "brilliant/setup/12345678-1234-4abc-8def-1234567890ab/ha_to_panel",
        "retained": "brilliant/setup/12345678-1234-4abc-8def-1234567890ab/retained",
        "discovery_probe": "homeassistant/brilliant_mqtt_setup/12345678-1234-4abc-8def-1234567890ab/probe"
      },
      "request": {"schema_version": 1, "setup_id": "12345678-1234-4abc-8def-1234567890ab", "nonce": "panel-nonce"},
      "result": {"schema_version": 1, "setup_id": "12345678-1234-4abc-8def-1234567890ab", "nonce": "ha-nonce", "reply_to_nonce": "panel-nonce"}
    }
  ]
}
~~~

In both test modules, load the fixture and assert that SetupTopics and canonical JSON output match every field. Parametrize malformed UUID, schema_version 2, extra key, missing key, empty nonce, and 16,385-byte payload rejection.

- [ ] **Step 2: Run both tests and verify missing-module failures**

Run:

~~~bash
uv run pytest tests/test_setup_protocol.py -q
uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_setup_protocol.py -q
~~~

Expected: each command fails during collection because its setup_protocol module does not exist.

- [ ] **Step 3: Implement the exact immutable contract twice**

Use frozen slot dataclasses, json.loads, json.dumps(value, sort_keys=True, separators=(",", ":")), UUID(str(value)), and an exact-key-set comparison. Define MAX_SETUP_PAYLOAD_BYTES = 16 * 1024 and SCHEMA_VERSION = 1 in both copies. SetupTopics.for_id must reject a non-v4 UUID and construct all four topics without accepting a prefix argument. SetupRequest fields are setup_id: UUID and nonce: str. SetupResult adds reply_to_nonce: str. Both expose the to_payload/from_payload signatures listed under Interfaces and return/accept the exact golden-vector shapes.

Raise ValueError with stable prefixes setup_payload_too_large, invalid_setup_payload, unsupported_setup_schema, and invalid_setup_id so callers can map errors without parsing broker-library text.

- [ ] **Step 4: Prove runtime parity and green tests**

Run both targeted commands from Step 2. Expected: PASS. Then run:

~~~bash
diff -u src/brilliant_mqtt/setup_protocol.py custom_components/brilliant_mqtt/setup_protocol.py
~~~

Expected: no output and exit 0.

- [ ] **Step 5: Commit the wire contract**

~~~bash
git add src/brilliant_mqtt/setup_protocol.py custom_components/brilliant_mqtt/setup_protocol.py tests/fixtures/mqtt_setup_v1_vectors.json tests/test_setup_protocol.py ha/tests/test_setup_protocol.py
git commit -m "feat: define MQTT setup validation protocol"
~~~

---

### Task 2: Add strict TLS and QoS-aware MQTT client construction

**Files:**
- Modify: src/brilliant_mqtt/config.py
- Modify: src/brilliant_mqtt/protocols.py
- Modify: src/brilliant_mqtt/mqttio.py
- Modify: tests/fakes.py
- Modify: tests/test_config.py
- Modify: tests/test_mqtt_context.py
- Create: tests/test_mqttio.py

**Interfaces:**
- Settings adds mqtt_tls_enabled: bool, mqtt_tls_ca_file: str | None, retained_topics_file: str.
- build_tls_context(settings: Settings) -> ssl.SSLContext | None
- AioMqttAdapter.publish(topic, payload, retain=False, qos=0) awaits broker acknowledgement.
- MqttClient.publish has the same signature.

- [ ] **Step 1: Write failing environment and adapter tests**

Add tests proving:

- defaults are plaintext, no CA file, and /var/brilliant-mqtt/state/owned-topics.json;
- MQTT_TLS_ENABLED accepts only the existing strict boolean spellings;
- MQTT_TLS_CA_FILE without MQTT_TLS_ENABLED is rejected;
- TLS with no CA calls ssl.create_default_context() with no cafile;
- TLS with a CA calls ssl.create_default_context(cafile="/tmp/brilliant-mqtt-test-ca.pem") in the isolated unit test;
- check_hostname is true and verify_mode is ssl.CERT_REQUIRED;
- aiomqtt.Client receives tls_context but never tls_insecure;
- publish("probe/topic", "nonce", qos=1) forwards qos=1 and awaits the returned publish call;
- FakeMqtt.published remains list[tuple[str, str, bool]] while published_qos records the matching integer.

- [ ] **Step 2: Run the focused tests and verify attribute/signature failures**

Run:

~~~bash
uv run pytest tests/test_config.py tests/test_mqttio.py tests/test_mqtt_context.py tests/test_fakes.py -q
~~~

Expected: FAIL because Settings lacks TLS/ledger fields and publish lacks qos.

- [ ] **Step 3: Implement strict settings validation**

Parse:

~~~python
mqtt_tls_enabled = _env_bool(env, "MQTT_TLS_ENABLED", "0")
mqtt_tls_ca_file = env.get("MQTT_TLS_CA_FILE") or None
retained_topics_file = env.get(
    "RETAINED_TOPICS_FILE",
    "/var/brilliant-mqtt/state/owned-topics.json",
)
if mqtt_tls_ca_file is not None and not mqtt_tls_enabled:
    raise ValueError("MQTT_TLS_CA_FILE requires MQTT_TLS_ENABLED")
if not retained_topics_file.startswith("/var/brilliant-mqtt/"):
    raise ValueError("RETAINED_TOPICS_FILE must be below /var/brilliant-mqtt/")
~~~

Update the Settings constructor and from_env return in one change. Do not test CA existence in Settings; the shared MQTT factory owns that I/O failure.

- [ ] **Step 4: Implement one client factory and QoS seam**

In mqttio.py, build SSLContext with ssl.create_default_context(cafile=settings.mqtt_tls_ca_file), then explicitly require check_hostname and ssl.CERT_REQUIRED. Pass it as tls_context to aiomqtt.Client. Do not pass tls_insecure. Add qos: int = 0 to the protocol, adapter, and fake; reject qos outside 0..2 before calling aiomqtt.

- [ ] **Step 5: Run focused and compatibility tests**

Run:

~~~bash
uv run pytest tests/test_config.py tests/test_mqttio.py tests/test_mqtt_context.py tests/test_fakes.py tests/test_bridge.py tests/test_scene_bridge.py -q
~~~

Expected: PASS with existing three-tuple publish assertions unchanged.

- [ ] **Step 6: Commit TLS/QoS support**

~~~bash
git add src/brilliant_mqtt/config.py src/brilliant_mqtt/protocols.py src/brilliant_mqtt/mqttio.py tests/fakes.py tests/test_config.py tests/test_mqtt_context.py tests/test_mqttio.py tests/test_fakes.py
git commit -m "feat: add strict MQTT TLS client settings"
~~~

---

### Task 3: Add the durable retained-topic ownership ledger

**Files:**
- Create: src/brilliant_mqtt/retained_topics.py
- Create: tests/test_retained_topics.py
- Modify: src/brilliant_mqtt/bridge.py
- Modify: src/brilliant_mqtt/__main__.py
- Modify: tests/test_bridge.py
- Modify: tests/test_bridge_reconcile.py

**Interfaces:**
- RetainedTopicLedger(panel_slug: str, path: Path)
- async_load() -> None
- async_publish(mqtt: MqttClient, topic: str, payload: str) -> None
- async_clear(mqtt: MqttClient, topic: str) -> None
- async_clear_all(mqtt: MqttClient) -> None
- ownership_topic and topics are read-only properties.
- Manifest limits are 4,096 unique topics and 256 KiB canonical JSON.

- [ ] **Step 1: Write fail-closed ledger tests**

Cover empty startup, valid reload, corrupt JSON, unknown keys, duplicate topics, mismatched slug, 4,097 topics, a 256 KiB overflow, a topic for another slug, brilliant/mesh, mesh discovery device identifiers, and an unrecognized retained shape.

Use these permitted panel topic families:

~~~python
f"brilliant/{slug}/availability"
f"brilliant/{slug}/bridge"
f"brilliant/{slug}/{peripheral}/state"
f"homeassistant/{component}/{unique_id}/config"
~~~

For discovery entries require the caller to supply the concrete topic only; ledger storage does not parse the discovery payload. Integration cleanup performs the stronger payload/device check in plan 3.

Assert publish ordering:

~~~python
assert mqtt.published == [
    ("brilliant/kitchen/ownership", expected_manifest, True),
    ("brilliant/kitchen/light-1/state", '{"on":true}', True),
]
assert mqtt.published_qos == [1, 0]
~~~

Assert that disk write failure or manifest QoS-1 publish failure prevents the target publish. Assert clear publishes an empty retained payload first, then removes the topic from disk and republishes the smaller manifest. Assert clear_all clears ownership last.

- [ ] **Step 2: Run and verify the missing-module failure**

Run: uv run pytest tests/test_retained_topics.py -q

Expected: FAIL during collection for missing brilliant_mqtt.retained_topics.

- [ ] **Step 3: Implement durable validation and atomic persistence**

Use a frozen OwnedTopicsManifest dataclass. Persist by mkdir(parents=True), write canonical UTF-8 JSON to a sibling temporary file opened with mode 0o600, flush, os.fsync, os.replace, then fsync the parent directory. Offload file operations with asyncio.to_thread. Keep a frozenset snapshot and serialize sorted topics.

The first publication of a topic must execute:

1. validate the topic;
2. persist the enlarged set;
3. publish the ownership manifest retained at QoS 1;
4. publish the retained target at QoS 0.

On step 3 failure, retain the conservative on-disk over-inclusion. On target failure, leave the manifest claiming the topic. Never silently recreate an invalid existing ledger; raise RetainedLedgerError and let the runtime publish a degraded diagnostic.

- [ ] **Step 4: Route every panel-retained bridge publication through the ledger**

Add owned_topics: RetainedTopicLedger | None to Bridge. Introduce _async_publish_retained(topic, payload) and replace only retain=True bridge calls with it. Instantiate/load a ledger only for the configured real panel bridge in __main__.py. The mesh Bridge receives None and therefore never claims mesh topics. Non-retained commands and scene messages continue to call MQTT directly.

- [ ] **Step 5: Run focused bridge regression tests**

Run:

~~~bash
uv run pytest tests/test_retained_topics.py tests/test_bridge.py tests/test_bridge_reconcile.py tests/test_bridge_heartbeat.py tests/test_mesh_leader.py -q
~~~

Expected: PASS, including an explicit assertion that a mesh reconcile never publishes brilliant/mesh/ownership.

- [ ] **Step 6: Commit retained ownership**

~~~bash
git add src/brilliant_mqtt/retained_topics.py src/brilliant_mqtt/bridge.py src/brilliant_mqtt/__main__.py tests/test_retained_topics.py tests/test_bridge.py tests/test_bridge_reconcile.py
git commit -m "feat: track panel-owned retained MQTT topics"
~~~

---

### Task 4: Build the temporary panel-side MQTT preflight

**Files:**
- Create: src/brilliant_mqtt/preflight.py
- Create: tests/test_preflight.py

**Interfaces:**
- PreflightStage enum values: fleet_auth, panel_to_ha, ha_to_panel, discovery_write, retained_message, cleanup.
- PreflightRequest.from_json(raw: str) -> PreflightRequest.
- PreflightReport.to_json() -> str.
- async_run_preflight(settings, request, mqtt_factory=AioMqttAdapter) -> PreflightReport.
- python -m brilliant_mqtt.preflight accepts one required --request-json argument containing the exact request object shown in Step 1 and emits exactly one JSON object to stdout.

- [ ] **Step 1: Write failing orchestration and CLI tests**

Use FakeMqtt plus an async message injector. Prove exact nonces and topic order, retained flag enforcement, a 10-second per-stage timeout, cancellation cleanup, empty-retained cleanup for retained and discovery probes, no bus imports, stdout containing only one JSON line, and exit status 1 for a failed stage.

The CLI request keys are exactly:

~~~json
{
  "schema_version": 1,
  "setup_id": "12345678-1234-4abc-8def-1234567890ab",
  "panel_nonce": "panel-nonce",
  "ha_nonce": "ha-nonce",
  "timeout_seconds": 10.0
}
~~~

- [ ] **Step 2: Run and verify the missing-module failure**

Run: uv run pytest tests/test_preflight.py -q

Expected: FAIL during collection for missing brilliant_mqtt.preflight.

- [ ] **Step 3: Implement the state machine**

Connect through AioMqttAdapter(settings), subscribe before each publish, validate SetupRequest/SetupResult through setup_protocol, and use asyncio.timeout(request.timeout_seconds) around each stage. A successful report contains schema_version, setup_id, success true, last_stage cleanup, and one elapsed-milliseconds value per stage. A failed report contains success false, failed_stage, one of the stable codes mqtt_connect, mqtt_publish, mqtt_subscribe, mqtt_timeout, mqtt_payload, retained_flag_missing, or cleanup_failed, plus redacted detail.

Wrap all post-connect work in try/finally. Cleanup must attempt both empty-retained publishes and disconnect even when one cleanup action fails. Do not include Settings, environment values, credentials, or CA contents in report/log text.

- [ ] **Step 4: Run focused tests and an offline CLI smoke test**

Run:

~~~bash
uv run pytest tests/test_preflight.py tests/test_setup_protocol.py tests/test_mqttio.py -q
uv run python -m brilliant_mqtt.preflight --help
~~~

Expected: tests PASS; help exits 0 and lists --request-json.

- [ ] **Step 5: Commit the panel preflight**

~~~bash
git add src/brilliant_mqtt/preflight.py tests/test_preflight.py
git commit -m "feat: add panel-side MQTT preflight"
~~~

---

### Task 5: Define HA broker profiles and typed operation errors

**Files:**
- Create: custom_components/brilliant_mqtt/errors.py
- Create: custom_components/brilliant_mqtt/broker.py
- Create: ha/tests/test_errors.py
- Create: ha/tests/test_broker.py
- Modify: pyproject.toml
- Modify: custom_components/brilliant_mqtt/manifest.json
- Modify: ha/pyproject.toml
- Modify: uv.lock

**Interfaces:**
- BrokerKind values: official_mosquitto, existing_broker.
- BrokerProfile.from_mapping(data: Mapping[str, Any]) -> BrokerProfile.
- BrokerProfile.device_client(client_id: str) -> AbstractAsyncContextManager[DeviceMqttClient].
- OperationStage and OperationError expose stable code, retryable, summary_key, documentation_slug, redacted_detail, and cleanup_error.

- [ ] **Step 1: Pin one device-client stack across both runtimes**

Replace the root aiomqtt range with aiomqtt==2.5.1 and add paho-mqtt==2.1.0 explicitly. Add both exact pins beside asyncssh in manifest requirements and HA dev dependencies. Run uv lock, then assert in root and HA tests that importlib.metadata reports aiomqtt 2.5.1 and paho-mqtt 2.1.0. This keeps the resident agent, staged preflight, and HA temporary device principal on the same tested protocol stack.

- [ ] **Step 2: Write failing normalization, TLS, and redaction tests**

Prove:

- kind changes guidance only; equal connection fields normalize identically;
- host is stripped/lowercased, port is 1..65535, username is nonempty, password is not exposed by repr/asdict/diagnostics;
- plaintext defaults to 1883 and TLS defaults to 8883 only when no explicit port exists;
- a custom CA is loaded directly with ssl.create_default_context(cadata=ca_pem), creates no temporary file, and never appears in client diagnostics;
- TLS enables ssl.CERT_REQUIRED/check_hostname and never uses tls_insecure;
- OperationError text contains the stable code but not supplied username, password, CA body, nonce, or full environment;
- invalid profile data produces invalid_broker_profile rather than a raw Voluptuous or aiomqtt exception.

- [ ] **Step 3: Run and verify missing-module failures**

Run:

~~~bash
uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_errors.py ha/tests/test_broker.py -q
~~~

Expected: FAIL during collection for missing errors and broker modules.

- [ ] **Step 4: Implement normalized immutable models**

Use a frozen slot dataclass whose password field has repr=False. Keep public CA text, not a private key. Provide redacted_dict() returning kind, host, port, tls_enabled, has_custom_ca, and username_configured only. Map socket/gaierror, MqttConnectError reason codes, ssl.SSLCertVerificationError, TimeoutError, and ValueError into stable OperationError instances at the boundary.

Define a DeviceMqttClient Protocol with subscribe, messages, publish, and disconnect methods so validator tests do not import or monkeypatch Paho internals.

- [ ] **Step 5: Run HA focused tests and lock consistency**

Run:

~~~bash
uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_errors.py ha/tests/test_broker.py -q
uv lock --check
~~~

Expected: PASS and lockfile current.

- [ ] **Step 6: Commit broker primitives**

~~~bash
git add pyproject.toml custom_components/brilliant_mqtt/errors.py custom_components/brilliant_mqtt/broker.py custom_components/brilliant_mqtt/manifest.json ha/pyproject.toml ha/tests/test_errors.py ha/tests/test_broker.py uv.lock
git commit -m "feat: add normalized HA broker profiles"
~~~

---

### Task 6: Implement end-to-end BrokerValidator

**Files:**
- Create: custom_components/brilliant_mqtt/broker_validation.py
- Create: ha/tests/test_broker_validation.py

**Interfaces:**
- BrokerValidator(hass, device_client_factory, timeout_seconds=10.0)
- async_validate(profile: BrokerProfile, setup_id: UUID | None = None) -> BrokerValidationResult.
- BrokerValidationResult contains setup_id, completed stages, elapsed time, and redacted diagnostics.

- [ ] **Step 1: Write failing stage-table tests**

Build a fake HA MQTT seam around mqtt.async_wait_for_mqtt_client, mqtt.async_subscribe, and mqtt.async_publish, plus a fake DeviceMqttClient. Parametrize every stage:

| Stage | Injected failure | Stable code |
|---|---|---|
| ha_mqtt_ready | no connected HA client | ha_mqtt_unavailable |
| ha_mqtt_ready | effective prefix zigbee | unsupported_discovery_prefix |
| fleet_auth | CONNACK rejection | fleet_auth_failed |
| panel_to_ha | nonce never reaches HA | panel_to_ha_timeout |
| ha_to_panel | nonce never reaches device | ha_to_panel_timeout |
| discovery_write | explicit authorization failure | discovery_write_denied |
| retained_message | missing retained flag | retained_message_invalid |
| cleanup | retained clear fails | cleanup_failed |

Also prove same-broker ambiguity text for silent cross-direction timeouts, QoS 1 on probes, unique client ID f"brilliant-mqtt-setup-{setup_id}", subscriptions established before publishes, and cleanup after cancellation.

- [ ] **Step 2: Run and verify the missing-module failure**

Run: uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_broker_validation.py -q

Expected: FAIL during collection for missing broker_validation.

- [ ] **Step 3: Implement the ordered validator**

Use one generated UUID and SetupTopics. Read Home Assistant's sole effective MQTT config from its loaded MQTT config entry data merged with options; reject a discovery prefix other than homeassistant before opening the device client. Then execute the approved stage table in order with asyncio.timeout per stage.

Use mqtt.async_subscribe for HA receives and mqtt.async_publish(self.hass, topic, payload, qos=1, retain=retain) for HA sends. Use the device protocol for panel-principal receives/sends. Validate the exact setup_id and nonce at each hop. Retained validation must subscribe after the device retained publish and require both the retained flag and exact nonce.

In finally, independently attempt: device empty-retained cleanup, HA empty-retained cleanup, HA unsubscribe callbacks, device unsubscribe/disconnect. If validation already failed, attach cleanup failure to that OperationError; if validation succeeded but cleanup failed, raise cleanup_failed.

- [ ] **Step 4: Run focused and existing MQTT integration tests**

Run:

~~~bash
uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_broker_validation.py ha/tests/test_init.py ha/tests/test_manager.py -q
~~~

Expected: PASS with no config entry created or modified by BrokerValidator.

- [ ] **Step 5: Commit validation**

~~~bash
git add custom_components/brilliant_mqtt/broker_validation.py ha/tests/test_broker_validation.py
git commit -m "feat: validate broker paths end to end"
~~~

---

### Task 7: Package TLS/preflight support and document broker prerequisites

**Files:**
- Modify: custom_components/brilliant_mqtt/panel_ops.py
- Modify: custom_components/brilliant_mqtt/components.py
- Modify: ha/tests/test_panel_ops.py
- Modify: ha/tests/test_components.py
- Modify: scripts/build_payload.sh
- Modify: custom_components/brilliant_mqtt/agent_payload/
- Modify: INSTALL.md
- Modify: docs/CONFIGURATION.md
- Modify: docs/reference/deployment.md
- Modify: docs/ha-integration.md

- [ ] **Step 1: Write failing environment-rendering tests**

Assert render_env emits:

~~~text
MQTT_TLS_ENABLED=1
MQTT_TLS_CA_FILE=/var/brilliant-mqtt/tls/mqtt-ca-0fa6a631898df0f5.pem
RETAINED_TOPICS_FILE=/var/brilliant-mqtt/state/owned-topics.json
~~~

for the test CA bytes test-ca followed by LF, when TLS/custom CA are enabled. The filename is mqtt-ca- plus the first 16 lowercase hex characters of SHA-256 over the exact CA bytes. Also assert plaintext emits MQTT_TLS_ENABLED=0 and no CA line, shell-safe quoting is preserved, CA PEM is never inline, the immutable CA file is mode 0o644, the environment is 0o600, and the preflight module exists in the built payload.

- [ ] **Step 2: Run and verify focused failures**

Run:

~~~bash
uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_panel_ops.py ha/tests/test_components.py -q
uv run pytest tests/test_payload_parity.py -q
~~~

Expected: FAIL because render_env and payload do not include the new contract.

- [ ] **Step 3: Extend staging without changing activation behavior**

Add constants for /var/brilliant-mqtt/tls and /var/brilliant-mqtt/state/owned-topics.json. Extend render_env with broker TLS fields and an explicit mqtt_tls_ca_file value. Add a stage_mqtt_ca helper that creates the TLS directory and uploads public CA material to the filename produced by f"mqtt-ca-{hashlib.sha256(ca_bytes).hexdigest()[:16]}.pem"; an inactive new file cannot change the running service, while old env snapshots continue to reference their prior file during rollback. Keep current deploy/enable calls untouched; fleet provisioning will use the staging seam in plan 2 and garbage-collect only unreferenced CA files after commit.

Update agent package version to 0.6.0. Ensure scripts/build_payload.sh copies setup_protocol.py, preflight.py, and retained_topics.py through its existing package-copy step and continues to vendor the lock-resolved aiomqtt/Paho versions.

- [ ] **Step 4: Regenerate and verify payload parity**

Run:

~~~bash
scripts/build_payload.sh
uv run pytest tests/test_payload_parity.py tests/test_preflight.py tests/test_retained_topics.py -q
uv run --project ha pytest -c ha/pyproject.toml ha/tests/test_panel_ops.py ha/tests/test_components.py -q
~~~

Expected: PASS.

- [ ] **Step 5: Add task-oriented prerequisite documentation**

Document two equal runtime paths:

1. Home Assistant Mosquitto (Recommended): install/start core_mosquitto, configure HA MQTT, create a dedicated non-owner Brilliant user, and use a panel-reachable host rather than assuming an add-on hostname.
2. Existing broker: configure both principals with the exact ACL table from the approved design.

Document plaintext and strict TLS examples, the fixed homeassistant discovery prefix, why internal HA MQTT credentials are not reused, and stable error-code anchors for every Task 6 code. State that onboarding validates rather than modifies broker users/ACLs.

- [ ] **Step 6: Run documentation/reference checks**

Run:

~~~bash
rg -n "core_mosquitto|existing broker|unsupported_discovery_prefix|brilliant/#|homeassistant/#|MQTT_TLS_ENABLED" INSTALL.md docs/CONFIGURATION.md docs/reference/deployment.md docs/ha-integration.md
rg -n "anonymous|tls_insecure|mutual TLS|WebSocket" INSTALL.md docs/CONFIGURATION.md
~~~

Expected: the first command finds every required concept; the second contains no instruction to enable anonymous access, insecure TLS, mutual TLS, or WebSockets.

- [ ] **Step 7: Commit packaging and documentation**

~~~bash
git add custom_components/brilliant_mqtt/panel_ops.py custom_components/brilliant_mqtt/components.py custom_components/brilliant_mqtt/agent_payload ha/tests/test_panel_ops.py ha/tests/test_components.py scripts/build_payload.sh INSTALL.md docs/CONFIGURATION.md docs/reference/deployment.md docs/ha-integration.md
git commit -m "feat: package MQTT preflight and TLS support"
~~~

---

### Task 8: Add disposable Mosquitto contract tests and run release gates

**Files:**
- Create: tests/mqtt_broker/mosquitto.conf
- Create: tests/mqtt_broker/acl
- Create: tests/mqtt_broker/passwords.example
- Create: tests/mqtt_broker/tls/README.md
- Create: scripts/run_mqtt_validation_tests.sh
- Create: ha/tests/test_broker_validation_live.py
- Modify: ha/pyproject.toml

- [ ] **Step 1: Write a skipped-by-default live test**

Mark tests mqtt_live and skip unless BRILLIANT_MQTT_TEST_BROKER_URL exists. Exercise four cases against disposable listeners: plaintext success, CA-trusted TLS success, bad device password, and discovery-write ACL denial. A fifth case connects the HA seam and device principal to two listeners and asserts the panel_to_ha timeout maps to the broker-mismatch-or-ACL guidance.

- [ ] **Step 2: Add a hardened broker runner**

scripts/run_mqtt_validation_tests.sh must:

- create a temporary directory with mktemp -d and trap cleanup;
- generate a one-run CA/server certificate with SAN DNS:localhost and IP:127.0.0.1;
- generate password files at runtime from fixed non-production test values;
- start pinned eclipse-mosquitto:2.0.22 containers with explicit localhost port mappings;
- wait for readiness with a bounded 30-second loop;
- export only test URLs/credentials;
- run the mqtt_live test;
- remove containers and the temporary directory in the trap.

The committed ACL grants device readwrite brilliant/# and write homeassistant/#, while a denial variant omits homeassistant write. Do not commit generated keys, certificates, or a usable password file.

- [ ] **Step 3: Run the live contract suite**

Run: scripts/run_mqtt_validation_tests.sh

Expected: PASS for success paths and expected typed failures. If Docker is unavailable, record the command as an environment prerequisite and do not replace this evidence with mocks.

- [ ] **Step 4: Run both complete project gates**

Run:

~~~bash
uv run ruff check --fix
uv run ruff format
uv run mypy --strict src tests
uv run pytest
uv run --project ha ruff check --fix --config ha/pyproject.toml custom_components/brilliant_mqtt ha/tests
uv run --project ha ruff format --config ha/pyproject.toml custom_components/brilliant_mqtt ha/tests
uv run --project ha mypy --strict --config-file ha/pyproject.toml custom_components/brilliant_mqtt ha/tests
uv run --project ha pytest -c ha/pyproject.toml ha/tests
git diff --check
~~~

Expected: all commands exit 0.

- [ ] **Step 5: Run a one-panel resource canary before release**

On the designated pilot only, capture 30 idle minutes before and after 0.6.0. Verify one brilliant-mqtt process, one MQTT connection, one Brilliant bus observer, RSS increase no more than 5 MiB, CPU increase no more than 2 percentage points, MemoryMax=96M, CPUQuota=20%, no physical-control regression, and no reconnect storm. Store raw evidence only in gitignored artifacts/ and add the sanitized result to docs/reference/deployment.md.

- [ ] **Step 6: Commit live tests and gate evidence**

~~~bash
git add tests/mqtt_broker scripts/run_mqtt_validation_tests.sh ha/tests/test_broker_validation_live.py ha/pyproject.toml docs/reference/deployment.md
git commit -m "test: exercise MQTT validation against Mosquitto"
~~~

---

## Plan 1 completion criteria

- Both runtimes pass the same setup v1 golden vectors.
- Runtime and preflight use the same strict TLS client construction.
- BrokerValidator proves HA readiness, both message directions, discovery write, and retained delivery with cleanup.
- Every real-panel retained publish is preceded by a durable ownership-manifest acknowledgement; mesh remains unclaimed.
- The payload contains the new modules and stays byte-for-byte in parity with source.
- Official Mosquitto is recommended but not required; external broker ACLs cover both principals.
- Mock, disposable-broker, full root, full HA, and pilot resource gates pass.
- Existing one-entry-per-panel setup and runtime behavior remain compatible, ready for plan 2.
