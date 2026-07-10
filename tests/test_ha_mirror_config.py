"""Tests for the brilliant_ha_mirror env config (Task 3)."""

from __future__ import annotations

import pytest

from brilliant_ha_mirror.config import Settings


def test_from_env_reads_required_and_defaults() -> None:
    env = {
        "PANEL": "office",
        "HA_WS_URL": "ws://ha.local:8123/api/websocket",
        "HA_TOKEN": "tok",
        "MIRROR_LABEL": "brilliant",
        "LEADER_PRIORITY": "5",
    }
    s = Settings.from_env(env)
    assert s.panel == "office"
    assert s.ha_ws_url.endswith("/api/websocket")
    assert s.mirror_label == "brilliant"
    assert s.leader_priority == 5
    assert s.room_overrides == {}


def test_room_overrides_parsed_from_json() -> None:
    env = {
        "PANEL": "p",
        "HA_WS_URL": "ws://x",
        "HA_TOKEN": "t",
        "ROOM_OVERRIDES": '{"Back Yard": "room-123"}',
    }
    s = Settings.from_env(env)
    assert s.room_overrides == {"Back Yard": "room-123"}


def test_missing_required_raises() -> None:
    with pytest.raises(KeyError):
        Settings.from_env({"PANEL": "p"})


def test_defaults_when_optional_absent() -> None:
    env = {"PANEL": "p", "HA_WS_URL": "ws://x", "HA_TOKEN": "t"}
    s = Settings.from_env(env)
    assert s.mirror_label == "brilliant"
    assert s.leader_priority == 0
    assert s.leader_heartbeat_seconds == 10.0
    assert s.log_level == "INFO"
    assert s.room_overrides == {}


def test_leader_heartbeat_seconds_override_parsed_as_float() -> None:
    env = {
        "PANEL": "p",
        "HA_WS_URL": "ws://x",
        "HA_TOKEN": "t",
        "LEADER_HEARTBEAT_SECONDS": "2.5",
    }
    s = Settings.from_env(env)
    assert s.leader_heartbeat_seconds == 2.5
    assert isinstance(s.leader_heartbeat_seconds, float)


def test_invalid_leader_priority_raises() -> None:
    env = {
        "PANEL": "p",
        "HA_WS_URL": "ws://x",
        "HA_TOKEN": "t",
        "LEADER_PRIORITY": "high",
    }
    with pytest.raises(ValueError):
        Settings.from_env(env)
