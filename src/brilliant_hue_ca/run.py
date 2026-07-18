"""Thin oneshot entrypoint: one reconcile pass, then exit. Driven by the
brilliant-hue-ca.timer systemd unit (OnBootSec + periodic)."""

from __future__ import annotations

import logging
import logging.handlers
import os
import sys
from collections.abc import Callable, Mapping

from .config import load_config
from .coordinator import Coordinator, RealCoordinator
from .fs import FileSystem, RealFileSystem
from .reconcile import cert_fingerprint, reconcile

_LOG = logging.getLogger("brilliant_hue_ca")


def run_once(
    environ: Mapping[str, str],
    *,
    fs: FileSystem,
    coordinator: Coordinator,
    read_ca: Callable[[str], str],
) -> int:
    cfg = load_config(environ)
    try:
        ca_pem = read_ca(cfg.ca_cert_path)
    except OSError:
        _LOG.exception("cannot read CA cert at %s", cfg.ca_cert_path)
        return 1
    try:
        cert_fingerprint(ca_pem)
    except ValueError:
        _LOG.exception(
            "CA cert at %s is empty or unparseable (not a valid PEM certificate)",
            cfg.ca_cert_path,
        )
        return 1
    try:
        outcome = reconcile(
            fs,
            coordinator,
            bundle_path=cfg.bundle_path,
            site_packages_root=cfg.site_packages_root,
            ca_pem=ca_pem,
        )
    except OSError:
        _LOG.exception("reconcile failed writing the bundle")
        return 1
    if not outcome.bundle_found:
        _LOG.warning(
            "Hue CA bundle not found (path=%s, glob root=%s)",
            cfg.bundle_path,
            cfg.site_packages_root,
        )
    elif outcome.appended:
        _LOG.info(
            "appended CA to %s; coordinator_restarted=%s",
            outcome.bundle_path,
            outcome.coordinator_restarted,
        )
    else:
        _LOG.debug("CA already present in %s; no-op", outcome.bundle_path)
    return 0


def _read_text(path: str) -> str:
    with open(path, encoding="utf-8") as f:
        return f.read()


def main() -> None:
    cfg = load_config(os.environ)
    handler = logging.handlers.RotatingFileHandler(cfg.log_path, maxBytes=256_000, backupCount=2)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    _LOG.addHandler(handler)
    _LOG.setLevel(logging.INFO)
    sys.exit(
        run_once(
            os.environ,
            fs=RealFileSystem(),
            coordinator=RealCoordinator(cfg.vassal_ini_path),
            read_ca=_read_text,
        )
    )


if __name__ == "__main__":
    main()
