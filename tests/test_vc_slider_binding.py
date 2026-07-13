from __future__ import annotations

import base64
import json
import os
import struct
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools.brilliant_vc.slider_binding import (
    OwnConfigState,
    SliderBindingError,
    build_private_snapshot,
    collect_private_snapshot,
    decode_slider_config,
    dumps_private_snapshot,
    extract_slider_variables,
    loads_private_snapshot,
    read_private_snapshot,
    verify_restoration,
    write_private_snapshot,
)


def _slider_blob(
    index: int,
    device_id: str,
    peripheral_id: str,
    *,
    action: int | None = None,
) -> str:
    payload = bytearray()
    payload.extend(b"\x08" + struct.pack(">h", 1) + struct.pack(">i", index))
    for field_id, value in ((2, device_id), (3, peripheral_id)):
        raw = value.encode()
        payload.extend(b"\x0b" + struct.pack(">h", field_id))
        payload.extend(struct.pack(">i", len(raw)) + raw)
    if action is not None:
        payload.extend(b"\x08" + struct.pack(">h", 6) + struct.pack(">i", action))
    payload.append(0)
    return base64.b64encode(payload).decode()


def _variables() -> dict[str, str]:
    return {
        "device_name": "Office",
        "disable_cap_touch_sliders": "0",
        "slider_double_tap_timeout_ms": "400",
        "slider_config:0": _slider_blob(0, "office-device", "gangbox_peripheral_0"),
        "slider_config:1": _slider_blob(1, "ble_mesh", "mesh-light", action=0),
    }


def test_decode_slider_config_extracts_required_target_and_optional_fields() -> None:
    binding = decode_slider_config(_slider_blob(1, "ble_mesh", "mesh-light", action=0))

    assert binding.slider_index == 1
    assert binding.device_id == "ble_mesh"
    assert binding.peripheral_id == "mesh-light"
    assert binding.action == 0


@pytest.mark.parametrize(
    "encoded",
    [
        "not-base64!",
        base64.b64encode(b"\x00trailing").decode(),
        base64.b64encode(b"\x08\x00\x01\x00").decode(),
    ],
)
def test_decode_slider_config_rejects_malformed_or_trailing_data(encoded: str) -> None:
    with pytest.raises(SliderBindingError):
        decode_slider_config(encoded)


def test_build_private_snapshot_captures_all_slider_bindings_and_guard_values() -> None:
    snapshot = build_private_snapshot(
        owning_device_id="office-device",
        variables=_variables(),
        selected_slider_index=1,
    )

    assert snapshot.owning_device_id == "office-device"
    assert snapshot.selected_slider_index == 1
    assert [record.variable_name for record in snapshot.slider_configs] == [
        "slider_config:0",
        "slider_config:1",
    ]
    assert snapshot.slider_configs[1].binding.peripheral_id == "mesh-light"
    assert snapshot.guard_values == {
        "disable_cap_touch_sliders": "0",
        "slider_double_tap_timeout_ms": "400",
    }
    assert "device_name" not in dumps_private_snapshot(snapshot)


def test_build_private_snapshot_requires_selected_slider_and_matching_wire_index() -> None:
    with pytest.raises(SliderBindingError, match="selected slider"):
        build_private_snapshot(
            owning_device_id="office-device",
            variables=_variables(),
            selected_slider_index=2,
        )

    variables = _variables()
    variables["slider_config:1"] = _slider_blob(2, "ble_mesh", "mesh-light")
    with pytest.raises(SliderBindingError, match="index"):
        build_private_snapshot(
            owning_device_id="office-device",
            variables=variables,
            selected_slider_index=1,
        )


def test_private_snapshot_round_trip_has_a_fixed_schema() -> None:
    snapshot = build_private_snapshot(
        owning_device_id="office-device",
        variables=_variables(),
        selected_slider_index=0,
    )

    serialized = dumps_private_snapshot(snapshot)
    loaded = loads_private_snapshot(serialized)

    assert loaded == snapshot
    payload = json.loads(serialized)
    assert set(payload) == {
        "schema_version",
        "owning_device_id",
        "selected_slider_index",
        "slider_configs",
        "guard_values",
    }

    payload["unexpected"] = "value"
    with pytest.raises(SliderBindingError, match="schema"):
        loads_private_snapshot(json.dumps(payload))


def test_verify_restoration_requires_exact_owner_names_values_and_guards() -> None:
    baseline = build_private_snapshot(
        owning_device_id="office-device",
        variables=_variables(),
        selected_slider_index=1,
    )

    result = verify_restoration(
        baseline,
        current_owning_device_id="office-device",
        current_variables=_variables(),
    )

    assert result.restored is True
    assert result.to_public_dict() == {
        "owner_matches": True,
        "slider_names_match": True,
        "slider_values_match": True,
        "guard_values_match": True,
        "selected_binding_matches": True,
        "slider_count": 2,
        "restored": True,
    }


def test_verify_restoration_fails_closed_on_each_changed_dimension() -> None:
    baseline = build_private_snapshot(
        owning_device_id="office-device",
        variables=_variables(),
        selected_slider_index=1,
    )

    changed = _variables()
    changed["slider_config:1"] = _slider_blob(1, "other-device", "other-light")
    result = verify_restoration(
        baseline,
        current_owning_device_id="office-device",
        current_variables=changed,
    )
    assert result.slider_names_match is True
    assert result.slider_values_match is False
    assert result.selected_binding_matches is False
    assert result.restored is False

    added = _variables()
    added["slider_config:2"] = _slider_blob(2, "device", "light")
    assert (
        verify_restoration(
            baseline,
            current_owning_device_id="office-device",
            current_variables=added,
        ).slider_names_match
        is False
    )

    guards = _variables()
    guards["disable_cap_touch_sliders"] = "1"
    assert (
        verify_restoration(
            baseline,
            current_owning_device_id="office-device",
            current_variables=guards,
        ).guard_values_match
        is False
    )

    assert (
        verify_restoration(
            baseline,
            current_owning_device_id="different-device",
            current_variables=_variables(),
        ).owner_matches
        is False
    )


def test_private_snapshot_writer_is_exclusive_private_and_round_trips(tmp_path: Path) -> None:
    safe_root = tmp_path / "evidence"
    safe_root.mkdir(mode=0o700)
    path = safe_root / "slider-before.json"
    snapshot = build_private_snapshot(
        owning_device_id="office-device",
        variables=_variables(),
        selected_slider_index=0,
    )

    digest = write_private_snapshot(
        path,
        snapshot,
        safe_root=safe_root,
        required_uid=os.getuid(),
    )

    assert len(digest) == 64
    assert path.stat().st_mode & 0o777 == 0o600
    assert read_private_snapshot(path, safe_root=safe_root, required_uid=os.getuid()) == snapshot
    with pytest.raises(SliderBindingError, match="already exists"):
        write_private_snapshot(
            path,
            snapshot,
            safe_root=safe_root,
            required_uid=os.getuid(),
        )


def test_private_snapshot_io_rejects_paths_outside_safe_root_and_symlinks(
    tmp_path: Path,
) -> None:
    safe_root = tmp_path / "evidence"
    safe_root.mkdir(mode=0o700)
    snapshot = build_private_snapshot(
        owning_device_id="office-device",
        variables=_variables(),
        selected_slider_index=0,
    )

    with pytest.raises(SliderBindingError, match="safe root"):
        write_private_snapshot(
            tmp_path / "outside.json",
            snapshot,
            safe_root=safe_root,
            required_uid=os.getuid(),
        )

    target = safe_root / "target.json"
    target.write_text(dumps_private_snapshot(snapshot), encoding="utf-8")
    target.chmod(0o600)
    link = safe_root / "link.json"
    link.symlink_to(target)
    with pytest.raises(SliderBindingError, match="symlink"):
        read_private_snapshot(link, safe_root=safe_root, required_uid=os.getuid())


def test_extract_slider_variables_reads_only_allowlisted_device_config_values() -> None:
    raw_variables = {name: SimpleNamespace(value=value) for name, value in _variables().items()}
    raw_device = SimpleNamespace(
        peripherals={
            "device_config_peripheral": SimpleNamespace(
                peripheral_type=19,
                variables=raw_variables,
            ),
            "gangbox_peripheral_0": SimpleNamespace(
                peripheral_type=1,
                variables={"on": SimpleNamespace(value="1")},
            ),
        }
    )

    result = extract_slider_variables(raw_device)

    assert set(result) == {
        "disable_cap_touch_sliders",
        "slider_double_tap_timeout_ms",
        "slider_config:0",
        "slider_config:1",
    }
    assert "device_name" not in result


@pytest.mark.parametrize(
    "config_peripheral",
    [
        None,
        SimpleNamespace(peripheral_type=18, variables={}),
        SimpleNamespace(
            peripheral_type=19,
            variables={"slider_config:0": SimpleNamespace(value=None)},
        ),
    ],
)
def test_extract_slider_variables_rejects_missing_wrong_or_unreadable_config(
    config_peripheral: object | None,
) -> None:
    peripherals = (
        {} if config_peripheral is None else {"device_config_peripheral": config_peripheral}
    )
    with pytest.raises(SliderBindingError):
        extract_slider_variables(SimpleNamespace(peripherals=peripherals))


async def test_collect_private_snapshot_uses_one_scoped_own_config_read() -> None:
    class FakeReader:
        def __init__(self) -> None:
            self.calls = 0

        async def read_own_config(self) -> OwnConfigState:
            self.calls += 1
            return OwnConfigState("office-device", _variables())

    reader = FakeReader()
    snapshot = await collect_private_snapshot(reader, selected_slider_index=1)

    assert reader.calls == 1
    assert snapshot.owning_device_id == "office-device"
    assert snapshot.selected_slider_index == 1
