from brilliant_hue_ca.config import Config, load_config


def test_load_config_defaults() -> None:
    cfg = load_config({})
    assert isinstance(cfg, Config)
    assert cfg.ca_cert_path == "/var/brilliant-hue-ca/injected-ca.pem"
    assert cfg.bundle_path.endswith("lib/certs/hue-bridge-ca-certs.pem")
    assert cfg.site_packages_root.endswith("site-packages")
    assert cfg.vassal_ini_path.endswith("processes/hue_bridge_peripherals.ini")


def test_load_config_overrides() -> None:
    cfg = load_config(
        {
            "HUE_CA_CERT_PATH": "/x/ca.pem",
            "HUE_CA_BUNDLE_PATH": "/x/bundle.pem",
            "HUE_CA_SITE_PACKAGES": "/x/sp",
            "HUE_CA_VASSAL_INI": "/x/v.ini",
            "HUE_CA_LOG": "/x/log",
        }
    )
    assert cfg.ca_cert_path == "/x/ca.pem"
    assert cfg.bundle_path == "/x/bundle.pem"
    assert cfg.site_packages_root == "/x/sp"
    assert cfg.vassal_ini_path == "/x/v.ini"
    assert cfg.log_path == "/x/log"
