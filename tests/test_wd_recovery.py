import inspect
import logging
from pathlib import Path
from typing import Any

import pytest

from brilliant_wifi_watchdog import bounded, recovery


class Rec:
    def __init__(self) -> None:
        self.cmds: list[list[str]] = []
        self.writes: list[tuple[str, str]] = []

    def run(self, argv: list[str]) -> int:
        self.cmds.append(argv)
        return 0

    def write(self, path: str, val: str) -> None:
        self.writes.append((path, val))


def test_soft_reconnect_sequence() -> None:
    r = Rec()
    recovery.soft_reconnect(run=r.run)
    assert ["connmanctl", "enable", "wifi"] in r.cmds
    assert ["connmanctl", "scan", "wifi"] in r.cmds


def test_restart_services_sequence() -> None:
    r = Rec()
    recovery.restart_services(run=r.run)
    assert ["systemctl", "restart", "connman"] in r.cmds
    assert ["systemctl", "restart", "wpa_supplicant"] in r.cmds


def test_gpio_reset_then_reboot() -> None:
    r = Rec()
    result = recovery.gpio_reset_and_reboot(
        run=r.run,
        write=r.write,
        read_alias=lambda: "2194000.mmc",
        reset_supported=lambda: True,
    )
    # unbind the wifi usdhc controller
    assert ("/sys/bus/platform/drivers/sdhci-esdhc-imx/unbind", "2194000.mmc") in r.writes
    # WL_REG_ON (gpio2) + BT_REG_ON (gpio5) exported, driven low, unexported
    assert ("/sys/class/gpio/export", "2") in r.writes
    assert ("/sys/class/gpio/gpio2/direction", "out") in r.writes
    assert ("/sys/class/gpio/gpio2/value", "0") in r.writes
    assert ("/sys/class/gpio/export", "5") in r.writes
    assert ("/sys/class/gpio/gpio5/direction", "out") in r.writes
    assert ("/sys/class/gpio/gpio5/value", "0") in r.writes
    assert ("/sys/class/gpio/unexport", "2") in r.writes
    assert ("/sys/class/gpio/unexport", "5") in r.writes
    # unbind precedes gpio2 export which precedes gpio2 unexport
    ub = r.writes.index(("/sys/bus/platform/drivers/sdhci-esdhc-imx/unbind", "2194000.mmc"))
    e2 = r.writes.index(("/sys/class/gpio/export", "2"))
    u2 = r.writes.index(("/sys/class/gpio/unexport", "2"))
    assert ub < e2 < u2
    # reboot is LAST
    assert r.cmds[-1] == ["systemctl", "reboot"]
    assert result == 0


def test_gpio_reset_skipped_off_imx_but_still_reboots(caplog: pytest.LogCaptureFixture) -> None:
    """Issue #155: the GPIO 2/5 + sdhci-esdhc-imx reset is i.MX6-specific. On any
    other SoC it must write nothing (not even read the alias), log once, and still
    request the reboot through the normal path."""
    r = Rec()
    alias_reads: list[str] = []

    def read_alias() -> str | None:
        alias_reads.append("read")
        return "2194000.mmc"

    with caplog.at_level(logging.WARNING, logger="brilliant_wifi_watchdog"):
        result = recovery.gpio_reset_and_reboot(
            run=r.run,
            write=r.write,
            read_alias=read_alias,
            sleep=lambda _: None,
            reset_supported=lambda: False,
        )
    assert r.writes == []
    assert alias_reads == []
    assert r.cmds == [["systemctl", "reboot"]]
    assert result == 0
    warnings = [rec for rec in caplog.records if rec.levelno == logging.WARNING]
    assert [w.getMessage() for w in warnings] == [
        "i.MX mmc1 controller binding unverified; skipping GPIO/usdhc reset"
    ]
    assert warnings[0].name == "brilliant_wifi_watchdog"


def test_gpio_reset_on_imx_logs_no_warning(caplog: pytest.LogCaptureFixture) -> None:
    r = Rec()
    with caplog.at_level(logging.WARNING, logger="brilliant_wifi_watchdog"):
        recovery.gpio_reset_and_reboot(
            run=r.run,
            write=r.write,
            read_alias=lambda: "2194000.mmc",
            sleep=lambda _: None,
            reset_supported=lambda: True,
        )
    assert r.writes
    assert [rec for rec in caplog.records if rec.levelno >= logging.WARNING] == []


def test_imx_reset_supported_requires_more_than_registered_driver_dir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    driver = tmp_path / "sdhci-esdhc-imx"
    monkeypatch.setattr(recovery, "_IMX_SDHCI_DRIVER_DIR", str(driver))
    monkeypatch.setattr(recovery, "_ALIAS_PATH", str(tmp_path / "missing_alias"))
    assert recovery.imx_wifi_reset_supported() is False
    driver.mkdir()
    assert recovery.imx_wifi_reset_supported() is False


def test_gpio_reset_defaults_to_the_imx_driver_probe() -> None:
    default = inspect.signature(recovery.gpio_reset_and_reboot).parameters["reset_supported"]
    assert default.default is recovery.imx_wifi_reset_supported


@pytest.mark.parametrize(
    "target",
    [
        "missing_alias",
        "empty_alias",
        "unbound",
        "other_controller",
        "dangling",
        "other_driver",
        "bound",
    ],
)
def test_gpio_reset_requires_bound_mmc1_controller(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, target: str
) -> None:
    driver = tmp_path / "bus/platform/drivers/sdhci-esdhc-imx"
    driver.mkdir(parents=True)
    alias = tmp_path / "firmware/devicetree/base/aliases/mmc1"
    alias.parent.mkdir(parents=True)
    if target != "missing_alias":
        alias.write_bytes(
            b"" if target == "empty_alias" else b"/soc/aips-bus@02100000/usdhc@02194000\x00"
        )
    device = tmp_path / "devices/platform/soc/2194000.mmc"
    device.mkdir(parents=True)
    (device / "driver").symlink_to(driver if target != "other_driver" else tmp_path)
    if target in ("bound", "other_driver", "other_controller", "dangling"):
        name = "2198000.mmc" if target == "other_controller" else "2194000.mmc"
        (driver / name).symlink_to(device if target != "dangling" else tmp_path / "missing")
    monkeypatch.setattr(recovery, "_ALIAS_PATH", str(alias))
    monkeypatch.setattr(recovery, "_IMX_SDHCI_DRIVER_DIR", str(driver))
    r = Rec()
    sleeps: list[float] = []
    assert recovery.imx_wifi_reset_supported() is (target == "bound")
    assert recovery.gpio_reset_and_reboot(run=r.run, write=r.write, sleep=sleeps.append) == 0
    assert r.cmds == [["systemctl", "reboot"]]
    if target == "bound":
        assert r.writes[0] == (str(driver / "unbind"), "2194000.mmc")
        assert ("/sys/class/gpio/gpio2/value", "0") in r.writes
        assert ("/sys/class/gpio/gpio5/value", "0") in r.writes
        assert sleeps == [1]
    else:
        assert r.writes == []
        assert sleeps == []


def test_gpio_reset_missing_alias_is_reboot_only_even_with_supported_driver() -> None:
    r = Rec()
    recovery.gpio_reset_and_reboot(
        run=r.run,
        write=r.write,
        read_alias=lambda: None,
        reset_supported=lambda: True,
        sleep=lambda _: None,
    )
    assert r.writes == []
    assert r.cmds == [["systemctl", "reboot"]]


# ---------------------------------------------------------------------------
# The DEFAULT recovery runner must be bounded — a recovery command against a
# stuck network daemon may hang, and it must not wedge the watchdog.
# ---------------------------------------------------------------------------


def test_recovery_default_runner_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[tuple[list[str], float]] = []

    def spy(argv: Any, *, timeout: float, capture: bool = False) -> bounded.Completed:
        seen.append((list(argv), timeout))
        return bounded.Completed(returncode=0, stdout="", timed_out=False)

    monkeypatch.setattr(bounded, "run_bounded", spy)
    recovery.soft_reconnect()
    recovery.restart_services()
    argvs = [a for a, _ in seen]
    assert ["connmanctl", "enable", "wifi"] in argvs
    assert ["systemctl", "restart", "connman"] in argvs
    assert seen  # sanity: the default runner was actually exercised
    assert all(t > 0 for _, t in seen)  # every recovery child got a wall-clock deadline
