"""Tests for Settings / env config (Milestone 6)."""

from __future__ import annotations

import pytest

from brilliant_mqtt.config import Settings


class TestSettings:
    def test_full_env_all_fields(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BRILLIANT_PANEL", "office")
        monkeypatch.setenv("MQTT_HOST", "10.0.0.1")
        monkeypatch.setenv("MQTT_USERNAME", "brilliant")
        monkeypatch.setenv("MQTT_PASSWORD", "s3cr3t")
        monkeypatch.setenv("MQTT_PORT", "8883")
        monkeypatch.setenv("RESYNC_SECONDS", "120")
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")

        s = Settings.from_env()

        assert s.panel == "office"
        assert s.mqtt_host == "10.0.0.1"
        assert s.mqtt_username == "brilliant"
        assert s.mqtt_password == "s3cr3t"
        assert s.mqtt_port == 8883
        assert s.resync_seconds == 120
        assert s.log_level == "DEBUG"

    def test_defaults_applied_when_optional_absent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BRILLIANT_PANEL", "office")
        monkeypatch.setenv("MQTT_HOST", "10.0.0.1")
        monkeypatch.setenv("MQTT_USERNAME", "brilliant")
        monkeypatch.setenv("MQTT_PASSWORD", "s3cr3t")
        monkeypatch.delenv("MQTT_PORT", raising=False)
        monkeypatch.delenv("RESYNC_SECONDS", raising=False)
        monkeypatch.delenv("LOG_LEVEL", raising=False)

        s = Settings.from_env()

        assert s.mqtt_port == 1883
        assert s.resync_seconds == 300
        assert s.log_level == "INFO"

    def test_missing_brilliant_panel_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("BRILLIANT_PANEL", raising=False)
        monkeypatch.setenv("MQTT_HOST", "10.0.0.1")
        monkeypatch.setenv("MQTT_USERNAME", "brilliant")
        monkeypatch.setenv("MQTT_PASSWORD", "s3cr3t")

        with pytest.raises(KeyError):
            Settings.from_env()

    def test_missing_mqtt_host_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BRILLIANT_PANEL", "office")
        monkeypatch.delenv("MQTT_HOST", raising=False)
        monkeypatch.setenv("MQTT_USERNAME", "brilliant")
        monkeypatch.setenv("MQTT_PASSWORD", "s3cr3t")

        with pytest.raises(KeyError):
            Settings.from_env()

    def test_missing_mqtt_username_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BRILLIANT_PANEL", "office")
        monkeypatch.setenv("MQTT_HOST", "10.0.0.1")
        monkeypatch.delenv("MQTT_USERNAME", raising=False)
        monkeypatch.setenv("MQTT_PASSWORD", "s3cr3t")

        with pytest.raises(KeyError):
            Settings.from_env()

    def test_missing_mqtt_password_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BRILLIANT_PANEL", "office")
        monkeypatch.setenv("MQTT_HOST", "10.0.0.1")
        monkeypatch.setenv("MQTT_USERNAME", "brilliant")
        monkeypatch.delenv("MQTT_PASSWORD", raising=False)

        with pytest.raises(KeyError):
            Settings.from_env()

    def test_mqtt_port_parsed_as_int(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BRILLIANT_PANEL", "office")
        monkeypatch.setenv("MQTT_HOST", "10.0.0.1")
        monkeypatch.setenv("MQTT_USERNAME", "brilliant")
        monkeypatch.setenv("MQTT_PASSWORD", "s3cr3t")
        monkeypatch.setenv("MQTT_PORT", "8883")

        s = Settings.from_env()
        assert s.mqtt_port == 8883
        assert isinstance(s.mqtt_port, int)

    def test_realtime_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BRILLIANT_PANEL", "office")
        monkeypatch.setenv("MQTT_HOST", "10.0.0.1")
        monkeypatch.setenv("MQTT_USERNAME", "brilliant")
        monkeypatch.setenv("MQTT_PASSWORD", "s3cr3t")
        monkeypatch.delenv("HOT_POLL_SECONDS", raising=False)
        monkeypatch.delenv("BUS_STALE_SECONDS", raising=False)

        s = Settings.from_env()
        assert s.hot_poll_seconds == 2.0
        assert s.bus_stale_seconds == 900.0

    def test_realtime_overrides_parsed_as_float(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BRILLIANT_PANEL", "office")
        monkeypatch.setenv("MQTT_HOST", "10.0.0.1")
        monkeypatch.setenv("MQTT_USERNAME", "brilliant")
        monkeypatch.setenv("MQTT_PASSWORD", "s3cr3t")
        monkeypatch.setenv("HOT_POLL_SECONDS", "0.5")
        monkeypatch.setenv("BUS_STALE_SECONDS", "120")

        s = Settings.from_env()
        assert s.hot_poll_seconds == 0.5
        assert isinstance(s.hot_poll_seconds, float)
        assert s.bus_stale_seconds == 120.0
        assert isinstance(s.bus_stale_seconds, float)

    def test_realtime_zero_disables(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """ "0" parses cleanly; the disable semantics live in the run loop."""
        monkeypatch.setenv("BRILLIANT_PANEL", "office")
        monkeypatch.setenv("MQTT_HOST", "10.0.0.1")
        monkeypatch.setenv("MQTT_USERNAME", "brilliant")
        monkeypatch.setenv("MQTT_PASSWORD", "s3cr3t")
        monkeypatch.setenv("HOT_POLL_SECONDS", "0")
        monkeypatch.setenv("BUS_STALE_SECONDS", "0")

        s = Settings.from_env()
        assert s.hot_poll_seconds == 0.0
        assert s.bus_stale_seconds == 0.0

    def test_mesh_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Default priority 0 = this panel never participates in mesh publishing."""
        monkeypatch.setenv("BRILLIANT_PANEL", "office")
        monkeypatch.setenv("MQTT_HOST", "10.0.0.1")
        monkeypatch.setenv("MQTT_USERNAME", "brilliant")
        monkeypatch.setenv("MQTT_PASSWORD", "s3cr3t")
        monkeypatch.delenv("MESH_PRIORITY", raising=False)
        monkeypatch.delenv("MESH_HEARTBEAT_SECONDS", raising=False)

        s = Settings.from_env()
        assert s.mesh_priority == 0
        assert s.mesh_heartbeat_seconds == 10.0

    def test_mesh_overrides_parsed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BRILLIANT_PANEL", "office")
        monkeypatch.setenv("MQTT_HOST", "10.0.0.1")
        monkeypatch.setenv("MQTT_USERNAME", "brilliant")
        monkeypatch.setenv("MQTT_PASSWORD", "s3cr3t")
        monkeypatch.setenv("MESH_PRIORITY", "2")
        monkeypatch.setenv("MESH_HEARTBEAT_SECONDS", "5.5")

        s = Settings.from_env()
        assert s.mesh_priority == 2
        assert isinstance(s.mesh_priority, int)
        assert s.mesh_heartbeat_seconds == 5.5
        assert isinstance(s.mesh_heartbeat_seconds, float)

    def test_reconnect_storm_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Default breaker: 20 reconnects within a 60s window trips a rebuild."""
        monkeypatch.setenv("BRILLIANT_PANEL", "office")
        monkeypatch.setenv("MQTT_HOST", "10.0.0.1")
        monkeypatch.setenv("MQTT_USERNAME", "brilliant")
        monkeypatch.setenv("MQTT_PASSWORD", "s3cr3t")
        monkeypatch.delenv("RECONNECT_STORM_THRESHOLD", raising=False)
        monkeypatch.delenv("RECONNECT_STORM_WINDOW_SECONDS", raising=False)

        s = Settings.from_env()
        assert s.reconnect_storm_threshold == 20
        assert s.reconnect_storm_window_seconds == 60.0

    def test_reconnect_storm_overrides_parsed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BRILLIANT_PANEL", "office")
        monkeypatch.setenv("MQTT_HOST", "10.0.0.1")
        monkeypatch.setenv("MQTT_USERNAME", "brilliant")
        monkeypatch.setenv("MQTT_PASSWORD", "s3cr3t")
        monkeypatch.setenv("RECONNECT_STORM_THRESHOLD", "5")
        monkeypatch.setenv("RECONNECT_STORM_WINDOW_SECONDS", "30")

        s = Settings.from_env()
        assert s.reconnect_storm_threshold == 5
        assert isinstance(s.reconnect_storm_threshold, int)
        assert s.reconnect_storm_window_seconds == 30.0
        assert isinstance(s.reconnect_storm_window_seconds, float)

    def test_reconnect_storm_zero_disables(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """ "0" parses cleanly; the disable semantics live in the run loop."""
        monkeypatch.setenv("BRILLIANT_PANEL", "office")
        monkeypatch.setenv("MQTT_HOST", "10.0.0.1")
        monkeypatch.setenv("MQTT_USERNAME", "brilliant")
        monkeypatch.setenv("MQTT_PASSWORD", "s3cr3t")
        monkeypatch.setenv("RECONNECT_STORM_THRESHOLD", "0")

        s = Settings.from_env()
        assert s.reconnect_storm_threshold == 0

    def test_reconcile_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Default motion reconciler settings."""
        monkeypatch.setenv("BRILLIANT_PANEL", "office")
        monkeypatch.setenv("MQTT_HOST", "10.0.0.1")
        monkeypatch.setenv("MQTT_USERNAME", "brilliant")
        monkeypatch.setenv("MQTT_PASSWORD", "s3cr3t")
        monkeypatch.delenv("MOTION_RECONCILE_ENABLED", raising=False)
        monkeypatch.delenv("MOTION_RECONCILE_MIN_INTERVAL_S", raising=False)
        monkeypatch.delenv("MOTION_RECONCILE_MAX_WRITES_PER_TICK", raising=False)
        monkeypatch.delenv("MOTION_RECONCILE_MIN_WRITE_SPACING_S", raising=False)
        monkeypatch.delenv("MOTION_DESIRED_STATE_DIR", raising=False)

        s = Settings.from_env()
        assert s.motion_reconcile_enabled is True
        assert s.motion_reconcile_min_interval_s == 60.0
        assert s.motion_reconcile_max_writes_per_tick == 4
        assert s.motion_reconcile_min_write_spacing_s == 0.5
        assert s.motion_desired_state_dir == "/var/brilliant-mqtt/state"

    def test_reconcile_env_overrides(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Motion reconciler settings parsed from environment."""
        monkeypatch.setenv("BRILLIANT_PANEL", "office")
        monkeypatch.setenv("MQTT_HOST", "10.0.0.1")
        monkeypatch.setenv("MQTT_USERNAME", "brilliant")
        monkeypatch.setenv("MQTT_PASSWORD", "s3cr3t")
        monkeypatch.setenv("MOTION_RECONCILE_ENABLED", "0")
        monkeypatch.setenv("MOTION_RECONCILE_MIN_INTERVAL_S", "15")
        monkeypatch.setenv("MOTION_RECONCILE_MAX_WRITES_PER_TICK", "8")
        monkeypatch.setenv("MOTION_RECONCILE_MIN_WRITE_SPACING_S", "0.25")
        monkeypatch.setenv("MOTION_DESIRED_STATE_DIR", "/tmp/state")

        s = Settings.from_env()
        assert s.motion_reconcile_enabled is False
        assert s.motion_reconcile_min_interval_s == 15.0
        assert s.motion_reconcile_max_writes_per_tick == 8
        assert s.motion_reconcile_min_write_spacing_s == 0.25
        assert isinstance(s.motion_reconcile_min_write_spacing_s, float)
        assert s.motion_desired_state_dir == "/tmp/state"
