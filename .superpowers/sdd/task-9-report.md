# Task 9 Implementation Report

## Outcome

Task 9 adds the versioned Home Assistant-owned control-plane configuration slice and
retires the legacy on-panel HA mirror without treating a write as proof. The seven
fleet-global values now have explicit defaults, bounded validation, decoded storage,
new/adopted-panel inheritance, and all-entry propagation. The only new panel-agent
configuration is `SCENE_BRIDGE_ENABLED=0|1`; HA labels, registry selections, mappings,
actions, URLs, tokens, and selected-scene metadata remain HA-only.

The legacy component remains in the component registry with its presence and removal
recipes for later cleanup work, but it is deprecated, excluded from defaults/forms and
selection, absent from switch registration, and guarded against installation at both
the registry recipe and manager boundary.

## TDD evidence

The required six-file RED command was run before production edits:

```text
uv run --project ha pytest -c ha/pyproject.toml \
  ha/tests/test_config_flow.py ha/tests/test_components.py \
  ha/tests/test_manager.py ha/tests/test_panel_ops.py \
  ha/tests/test_diagnostics.py ha/tests/test_repairs.py -q

75 failed, 160 passed
```

The config-flow RED tranche independently produced `45 failed, 32 passed`. Failures
covered global defaults and validation, migration, deprecated component behavior,
panel env boundaries, retirement lifecycle, Repair sequencing, and diagnostic
non-disclosure.

The final scope audit found two additional cases directly required by Task 9 and
captured each as a failing test before its minimal fix:

- an adopted panel's stale `SCENE_BRIDGE_ENABLED=0` incorrectly overrode an already
  enabled fleet setting;
- saving changed global settings while adding a panel did not propagate them to the
  existing fleet entries.

Both focused tests failed for the expected value mismatch, then passed after fleet
globals were made authoritative for adoption and add-panel saves propagated the seven
decoded values.

Final targeted GREEN:

```text
237 passed in 3.30s
```

## Architecture and sequencing

- Config entry version is now 3. Migration preserves future-version rejection and v1
  voice folding, forces `bridge=true` and `ha_mirror=false`, copies the legacy label
  only when the new label is absent, and adds all seven explicit defaults. Legacy URL,
  token, leader priority, and compatibility label are retained by migration.
- New installs accept JSON text only at the form boundary and store detached decoded
  dictionaries. Validation bounds input size, node count, depth, strings, mapping
  count, allowed domains, entity limit, configured panel keys, exact action keys,
  service syntax, and target keys. Invalid JSON is redisplayed from the safe submitted
  form value without being logged or persisted.
- Fleet-global values are copied with one `async_update_entry` per existing entry while
  preserving every per-panel credential, pin, component map, option, and unrelated
  data key. An adopted panel uses its installed scene toggle only when it is the first
  fleet entry; otherwise the existing fleet globals win.
- Reconfigure first validates all input, pushes only the boolean scene-bridge flag to
  the addressed panel, applies non-deprecated optional component changes, propagates
  the seven global values, and reloads the current entry. Old mirror credentials remain
  untouched by this ordinary configuration save.
- Retirement is serialized through the fleet SSH lock and bounded by a timeout. A
  Repair is created before removal, the old unit/env/payload/staged state is removed,
  and a second independent inspection must prove unit, env, enabled, active, staged
  env, and payload are all absent. Failure/cancellation keeps the Repair and secrets,
  closes the shell, and records only a generic sanitized problem. Offline retirement
  does not prevent the integration from loading.
- Repair, agent update, staged-copy refresh, setup evidence handling, and direct legacy
  removal converge on the same retirement core. Verified retirement atomically sets
  the old component false and a verification marker. Old URL/token/leader credentials
  are removed in that same per-panel update only when safe HA control is enabled.
- Diagnostics remove raw room overrides and scene actions entirely, continue redacting
  root/MQTT/legacy HA tokens, and expose only safe counts/settings plus committed
  manifest and current scene runtime metadata. Native tiles are explicitly reported as
  `blocked` and not validated; absent runtime values remain `None`.

## Minimal secondary-file changes

- `custom_components/brilliant_mqtt/__init__.py`: version-3 migration and deletion of
  the legacy-retirement Repair when an entry is removed.
- `custom_components/brilliant_mqtt/ha_control.py`: read-only manifest revision/entity
  count properties used by diagnostics.
- `custom_components/brilliant_mqtt/scene_control.py`: read-only raw transport-status
  accessor used by diagnostics without inferring status from catalog contents.
- `custom_components/brilliant_mqtt/switch.py`: stops registering the deprecated mirror
  switch while retaining its compatibility class through the planned later removal.
- `ha/tests/test_init.py`: updates the two v1 migration expectations to current version
  3 and the forced-false legacy component marker.

No live panel or production Home Assistant state was touched. Task 10 cleanup tooling
and Task 12 physical removal were not implemented.

## Final verification

```text
uv run --project ha ruff check --config ha/pyproject.toml \
  custom_components/brilliant_mqtt ha/tests
All checks passed!

uv run --project ha ruff format --check --config ha/pyproject.toml \
  custom_components/brilliant_mqtt ha/tests
41 files already formatted

uv run --project ha mypy --strict --config-file ha/pyproject.toml \
  custom_components/brilliant_mqtt ha/tests
Success: no issues found in 41 source files

uv run --project ha pytest -c ha/pyproject.toml ha/tests
461 passed in 7.54s

uv run --project ha pytest -c ha/pyproject.toml \
  ha/tests/test_config_flow.py ha/tests/test_components.py \
  ha/tests/test_manager.py ha/tests/test_panel_ops.py \
  ha/tests/test_diagnostics.py ha/tests/test_repairs.py -q
237 passed in 3.30s

git diff --check
clean

jq empty custom_components/brilliant_mqtt/strings.json \
  custom_components/brilliant_mqtt/translations/en.json
valid; strings and English translation are byte-identical
```
