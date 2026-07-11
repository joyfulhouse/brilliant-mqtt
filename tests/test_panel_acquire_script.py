"""Contract tests for the Brilliant panel corpus acquisition helper."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "brilliant-panel" / "acquire.sh"


def test_dry_run_describes_sensitive_corpus_without_printing_password() -> None:
    env = os.environ | {"SSHPASS": "do-not-print"}

    result = subprocess.run(
        [str(SCRIPT), "--dry-run", "v26.06.03.1", "192.0.2.10"],
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "data/switch-ui" in result.stdout
    assert "data/switch-embedded" in result.stdout
    assert "var" in result.stdout
    assert "artifacts/brilliant-panel/v26.06.03.1/raw/pilot-corpus.tar.zst" in result.stdout
    assert "do-not-print" not in result.stdout
    assert "do-not-print" not in result.stderr


def test_live_mode_requires_sshpass_environment_variable() -> None:
    env = os.environ.copy()
    env.pop("SSHPASS", None)

    result = subprocess.run(
        [str(SCRIPT), "v26.06.03.1", "192.0.2.10"],
        cwd=ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert "SSHPASS must be set" in result.stderr
