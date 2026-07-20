import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_build_payload_includes_hue_ca() -> None:
    # Runs the real build script; asserts the hue_ca package + units land in the payload.
    subprocess.run(["bash", str(ROOT / "scripts" / "build_payload.sh")], check=True)
    dest = ROOT / "custom_components" / "brilliant_mqtt" / "agent_payload"
    assert (dest / "hue_ca" / "brilliant_hue_ca" / "run.py").is_file()
    assert (dest / "hue_ca" / "brilliant_hue_ca" / "reconcile.py").is_file()
    assert (dest / "brilliant-hue-ca.service").is_file()
    assert (dest / "brilliant-hue-ca.timer").is_file()
