from __future__ import annotations

from pathlib import Path

from brilliant_mqtt.desired_state import RECONCILED_VARS, DesiredState


def test_record_then_wanted(tmp_path: Path) -> None:
    ds = DesiredState(tmp_path / "d.json")
    ds.record("pid1", "enable_motion_score", "1")
    assert ds.wanted("pid1") == {"enable_motion_score": "1"}


def test_wanted_unknown_peripheral_is_empty(tmp_path: Path) -> None:
    assert DesiredState(tmp_path / "d.json").wanted("nope") == {}


def test_last_write_wins(tmp_path: Path) -> None:
    ds = DesiredState(tmp_path / "d.json")
    ds.record("pid1", "enable_motion_score", "1")
    ds.record("pid1", "enable_motion_score", "0")
    assert ds.wanted("pid1") == {"enable_motion_score": "0"}


def test_record_persists_across_instances(tmp_path: Path) -> None:
    path = tmp_path / "sub" / "d.json"  # parent dir does not exist yet
    DesiredState(path).record("pid1", "motion_low_threshold", "30")
    fresh = DesiredState(path)
    fresh.load()
    assert fresh.wanted("pid1") == {"motion_low_threshold": "30"}


def test_load_missing_file_is_empty(tmp_path: Path) -> None:
    ds = DesiredState(tmp_path / "absent.json")
    ds.load()
    assert ds.wanted("pid1") == {}


def test_load_corrupt_file_is_empty(tmp_path: Path) -> None:
    path = tmp_path / "d.json"
    path.write_text("{not json")
    ds = DesiredState(path)
    ds.load()
    assert ds.wanted("pid1") == {}


def test_load_non_dict_json_is_empty(tmp_path: Path) -> None:
    path = tmp_path / "d.json"
    path.write_text("[1, 2, 3]")
    ds = DesiredState(path)
    ds.load()
    assert ds.wanted("anything") == {}


def test_reconciled_vars_membership() -> None:
    assert "enable_motion_score" in RECONCILED_VARS
    assert "enable_pir_motion_score" in RECONCILED_VARS
    assert "on" not in RECONCILED_VARS
