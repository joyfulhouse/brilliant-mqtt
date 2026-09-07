"""Config from the environment (same idiom as the watchdog daemons)."""

from __future__ import annotations

import math
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
    state_path: str
    min_retry_interval_s: float


def load_config(environ: Mapping[str, str]) -> Config:
    def s(key: str, default: str) -> str:
        return environ.get(key, default)

    def f(key: str, default: float) -> float:
        try:
            v = float(environ[key])
        except (KeyError, ValueError):
            return default
        # Reject nan/inf/negatives: nan or a negative defeats pacing (retry every
        # tick), inf paces the reload away forever. Fall back to the safe default.
        if not math.isfinite(v) or v < 0:
            return default
        return v

    return Config(
        ca_cert_path=s("HUE_CA_CERT_PATH", "/var/brilliant-hue-ca/injected-ca.pem"),
        bundle_path=s("HUE_CA_BUNDLE_PATH", f"{_DEFAULT_SP}/lib/certs/hue-bridge-ca-certs.pem"),
        site_packages_root=s("HUE_CA_SITE_PACKAGES", _DEFAULT_SP),
        vassal_ini_path=s(
            "HUE_CA_VASSAL_INI",
            "/var/run/brilliant/processes/hue_bridge_peripherals.ini",
        ),
        log_path=s("HUE_CA_LOG", "/var/log/brilliant-hue-ca.log"),
        state_path=s("HUE_CA_STATE_PATH", "/var/brilliant-hue-ca/pending-reload.json"),
        # 5 min: well under the 15-min timer cadence, so a real restart failure
        # still retries within one or two ticks, but immune to back-to-back
        # restart storms if reconcile is somehow called in quick succession.
        min_retry_interval_s=f("HUE_CA_MIN_RETRY_INTERVAL_S", 300.0),
    )
