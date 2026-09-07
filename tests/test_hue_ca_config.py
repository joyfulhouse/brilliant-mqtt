import pytest

from brilliant_hue_ca.config import Config, load_config


def test_load_config_defaults() -> None:
    cfg = load_config({})
    assert isinstance(cfg, Config)
    assert cfg.ca_cert_path == "/var/brilliant-hue-ca/injected-ca.pem"
    assert cfg.bundle_path.endswith("lib/certs/hue-bridge-ca-certs.pem")
    assert cfg.site_packages_root.endswith("site-packages")
    assert cfg.vassal_ini_path.endswith("processes/hue_bridge_peripherals.ini")
    assert cfg.state_path == "/var/brilliant-hue-ca/pending-reload.json"
    assert cfg.min_retry_interval_s == 300.0


def test_min_retry_interval_invalid_falls_back_to_default() -> None:
    cfg = load_config({"HUE_CA_MIN_RETRY_INTERVAL_S": "not-a-number"})
    assert cfg.min_retry_interval_s == 300.0


@pytest.mark.parametrize("bad", ["inf", "-inf", "nan", "-5", "-0.1"])
def test_min_retry_interval_non_finite_or_negative_falls_back(bad: str) -> None:
    # nan/negative would defeat pacing (retry every tick); inf would pace the
    # reload away forever. All must fall back to the safe default.
    cfg = load_config({"HUE_CA_MIN_RETRY_INTERVAL_S": bad})
    assert cfg.min_retry_interval_s == 300.0


def test_load_config_overrides() -> None:
    cfg = load_config(
        {
            "HUE_CA_CERT_PATH": "/x/ca.pem",
            "HUE_CA_BUNDLE_PATH": "/x/bundle.pem",
            "HUE_CA_SITE_PACKAGES": "/x/sp",
            "HUE_CA_VASSAL_INI": "/x/v.ini",
            "HUE_CA_LOG": "/x/log",
            "HUE_CA_STATE_PATH": "/x/state.json",
            "HUE_CA_MIN_RETRY_INTERVAL_S": "42.5",
        }
    )
    assert cfg.ca_cert_path == "/x/ca.pem"
    assert cfg.bundle_path == "/x/bundle.pem"
    assert cfg.site_packages_root == "/x/sp"
    assert cfg.vassal_ini_path == "/x/v.ini"
    assert cfg.log_path == "/x/log"
    assert cfg.state_path == "/x/state.json"
    assert cfg.min_retry_interval_s == 42.5
