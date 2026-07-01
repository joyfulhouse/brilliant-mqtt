from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import pytest

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


def test_load_filters_to_reconciled_vars(tmp_path: Path) -> None:
    """Stale/hand-edited files with non-reconciled vars are silently dropped on load."""
    path = tmp_path / "d.json"
    # File contains one reconciled var and one non-reconciled var under the same peripheral.
    path.write_text(json.dumps({"pid1": {"enable_motion_score": "1", "on": "1"}}))
    ds = DesiredState(path)
    ds.load()
    # Only the reconciled var survives; "on" must not be loaded.
    assert ds.wanted("pid1") == {"enable_motion_score": "1"}


def test_load_drops_peripheral_with_only_non_reconciled_vars(tmp_path: Path) -> None:
    """A peripheral whose every var is non-reconciled is omitted entirely after load."""
    path = tmp_path / "d.json"
    path.write_text(json.dumps({"pid1": {"on": "1", "intensity": "50"}}))
    ds = DesiredState(path)
    ds.load()
    assert ds.wanted("pid1") == {}


def test_record_resilient_to_save_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A disk error in save() must not prevent the in-memory state from updating."""
    ds = DesiredState(tmp_path / "d.json")

    def _bad_save() -> None:
        raise OSError("disk full")

    monkeypatch.setattr(ds, "save", _bad_save)
    ds.record("pid1", "enable_motion_score", "1")  # must not raise
    assert ds.wanted("pid1") == {"enable_motion_score": "1"}


def test_load_normalizes_bool_values(tmp_path: Path) -> None:
    """JSON booleans and string synonyms are normalized to bus strings on load."""
    path = tmp_path / "d.json"
    # JSON true -> "1", string "30" stays "30".
    path.write_text(
        json.dumps({"pidX": {"enable_motion_score": True, "motion_low_threshold": "30"}})
    )
    ds = DesiredState(path)
    ds.load()
    assert ds.wanted("pidX") == {"enable_motion_score": "1", "motion_low_threshold": "30"}


def test_load_normalizes_falsy_and_synonym_values(tmp_path: Path) -> None:
    """JSON false and string synonyms normalize to "0"; truthy synonyms to "1"."""
    path = tmp_path / "d.json"
    path.write_text(
        json.dumps(
            {
                "pidX": {
                    "enable_motion_score": False,
                    "enable_pir_motion_score": "off",
                    "enable_screen_motion_detection": "yes",
                }
            }
        )
    )
    ds = DesiredState(path)
    ds.load()
    assert ds.wanted("pidX") == {
        "enable_motion_score": "0",
        "enable_pir_motion_score": "0",
        "enable_screen_motion_detection": "1",
    }


def test_load_canonicalizes_integral_numeric_values(tmp_path: Path) -> None:
    """Integral numerics (30, 30.0, "030") load as the canonical bus string "30".

    The bus returns integral strings for the reconciled thresholds; a hand-edited
    "30.0" loaded verbatim would never compare equal to the bus value and would
    re-write every min-interval forever.
    """
    path = tmp_path / "d.json"
    path.write_text(
        json.dumps(
            {
                "pidX": {
                    "motion_low_threshold": 30.0,
                    "motion_high_threshold": "030",
                    "pir_motion_detection_low_threshold": 25,
                }
            }
        )
    )
    ds = DesiredState(path)
    ds.load()
    assert ds.wanted("pidX") == {
        "motion_low_threshold": "30",
        "motion_high_threshold": "30",
        "pir_motion_detection_low_threshold": "25",
    }


def test_load_passes_non_integral_and_non_numeric_through(tmp_path: Path) -> None:
    """Values with no canonical integral form pass through unchanged."""
    path = tmp_path / "d.json"
    path.write_text(
        json.dumps({"pidX": {"motion_low_threshold": "2.5", "motion_high_threshold": "weird"}})
    )
    ds = DesiredState(path)
    ds.load()
    assert ds.wanted("pidX") == {"motion_low_threshold": "2.5", "motion_high_threshold": "weird"}


def test_load_skips_non_dict_peripheral_entry_with_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A corrupt per-peripheral entry is dropped with a warning, others survive."""
    path = tmp_path / "d.json"
    path.write_text(
        json.dumps({"pid1": ["not", "a", "dict"], "pid2": {"enable_motion_score": "1"}})
    )
    ds = DesiredState(path)
    with caplog.at_level(logging.WARNING, logger="brilliant_mqtt.desired_state"):
        ds.load()
    assert ds.wanted("pid1") == {}
    assert ds.wanted("pid2") == {"enable_motion_score": "1"}
    assert any("pid1" in r.getMessage() for r in caplog.records)


def test_load_logs_summary_when_file_present(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """load() reports what it restored — the only startup evidence in journald."""
    path = tmp_path / "d.json"
    path.write_text(
        json.dumps({"pidX": {"enable_motion_score": "1", "motion_low_threshold": "30"}})
    )
    ds = DesiredState(path)
    with caplog.at_level(logging.INFO, logger="brilliant_mqtt.desired_state"):
        ds.load()
    assert any(
        "1 peripheral" in r.getMessage() and "2 var" in r.getMessage() for r in caplog.records
    )


def test_load_logs_first_boot_when_file_missing(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """First boot (no state file) is diagnosable — distinct from a vanished file."""
    ds = DesiredState(tmp_path / "absent.json")
    with caplog.at_level(logging.INFO, logger="brilliant_mqtt.desired_state"):
        ds.load()
    assert any("no desired-state file" in r.getMessage() for r in caplog.records)


def test_save_fsyncs_tmp_before_replace_and_dir_after(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Durability contract: fsync the temp file BEFORE the rename and the parent
    directory AFTER — a breaker power-cut must not leave a truncated state file
    (atomicity alone does not give durability on ext4 with delayed allocation)."""
    calls: list[str] = []
    real_fsync = os.fsync
    real_replace = os.replace

    def spy_fsync(fd: int) -> None:
        calls.append("fsync")
        real_fsync(fd)

    def spy_replace(src: str | os.PathLike[str], dst: str | os.PathLike[str]) -> None:
        calls.append("replace")
        real_replace(src, dst)

    monkeypatch.setattr(os, "fsync", spy_fsync)
    monkeypatch.setattr(os, "replace", spy_replace)

    DesiredState(tmp_path / "d.json").record("pid1", "enable_motion_score", "1")

    assert "replace" in calls
    ri = calls.index("replace")
    assert "fsync" in calls[:ri], "temp file must be fsynced before the rename"
    assert "fsync" in calls[ri + 1 :], "parent dir must be fsynced after the rename"


def test_failed_replace_preserves_previous_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the atomic rename fails, the previous on-disk state must survive
    (and the in-memory state must still carry the new value)."""
    path = tmp_path / "d.json"
    ds = DesiredState(path)
    ds.record("pid1", "enable_motion_score", "1")  # persisted

    def bad_replace(src: str | os.PathLike[str], dst: str | os.PathLike[str]) -> None:
        raise OSError("rename blocked")

    monkeypatch.setattr(os, "replace", bad_replace)
    ds.record("pid1", "enable_motion_score", "0")  # save fails; must not raise

    assert ds.wanted("pid1") == {"enable_motion_score": "0"}  # in-memory kept
    fresh = DesiredState(path)
    fresh.load()
    assert fresh.wanted("pid1") == {"enable_motion_score": "1"}  # disk intact


def test_load_ignores_stale_tmp_and_save_replaces_it(tmp_path: Path) -> None:
    """A stale/garbage .tmp beside the state file neither breaks load() nor
    survives the next save() (crash-between-write-and-rename recovery)."""
    path = tmp_path / "d.json"
    DesiredState(path).record("pid1", "enable_motion_score", "1")
    (tmp_path / "d.json.tmp").write_text("{garbage")

    ds = DesiredState(path)
    ds.load()
    assert ds.wanted("pid1") == {"enable_motion_score": "1"}

    ds.record("pid1", "enable_motion_score", "0")
    assert not (tmp_path / "d.json.tmp").exists()
    fresh = DesiredState(path)
    fresh.load()
    assert fresh.wanted("pid1") == {"enable_motion_score": "0"}


def test_record_survives_unwritable_path(tmp_path: Path) -> None:
    """A REAL disk failure (parent path blocked by a file) must not raise and
    must keep the in-memory value — mock-free version of the resilience test."""
    blocker = tmp_path / "blocker"
    blocker.write_text("i am a file, not a directory")
    ds = DesiredState(blocker / "d.json")
    ds.record("pid1", "enable_motion_score", "1")  # must not raise
    assert ds.wanted("pid1") == {"enable_motion_score": "1"}
