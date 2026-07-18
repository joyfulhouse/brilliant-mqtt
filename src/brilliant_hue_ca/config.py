"""Config from the environment (same idiom as the watchdog daemons)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

_DEFAULT_SP = "/data/switch-embedded/env/lib/python3.10/site-packages"


@dataclass(frozen=True)
class Config:
    ca_cert_path: str
    bundle_path: str
    site_packages_root: str
    vassal_ini_path: str
    log_path: str


def load_config(environ: Mapping[str, str]) -> Config:
    def s(key: str, default: str) -> str:
        return environ.get(key, default)

    return Config(
        ca_cert_path=s("HUE_CA_CERT_PATH", "/var/brilliant-hue-ca/injected-ca.pem"),
        bundle_path=s("HUE_CA_BUNDLE_PATH", f"{_DEFAULT_SP}/lib/certs/hue-bridge-ca-certs.pem"),
        site_packages_root=s("HUE_CA_SITE_PACKAGES", _DEFAULT_SP),
        vassal_ini_path=s(
            "HUE_CA_VASSAL_INI",
            "/var/run/brilliant/processes/hue_bridge_peripherals.ini",
        ),
        log_path=s("HUE_CA_LOG", "/var/log/brilliant-hue-ca.log"),
    )
