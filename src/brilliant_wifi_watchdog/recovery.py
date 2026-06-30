"""Recovery actions. The GPIO/SDIO reset replicates Brilliant's /usr/sbin/wifi_watchdog
`_do_imx6_wifi_reset` exactly, then reboots (the proven way brcmfmac re-inits)."""

from __future__ import annotations

import subprocess
import time
from collections.abc import Callable

WL_REG_ON_GPIO = 2
BT_REG_ON_GPIO = 5
_ALIAS_PATH = "/sys/firmware/devicetree/base/aliases/mmc1"


def _run(argv: list[str]) -> int:
    return subprocess.run(argv, check=False).returncode


def _write(path: str, val: str) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(val + "\n")
    except OSError:
        pass  # best-effort, matches firmware behavior


def _read_alias() -> str | None:
    try:
        with open(_ALIAS_PATH, encoding="utf-8") as f:
            alias = f.read().rstrip("\x00")
    except OSError:
        return None
    if not alias:
        return None
    controller = alias.split("/")[-1]
    return f"{controller.split('@')[-1].lstrip('0')}.mmc"


def soft_reconnect(run: Callable[[list[str]], int] = _run) -> None:
    run(["connmanctl", "enable", "wifi"])
    run(["connmanctl", "scan", "wifi"])


def restart_services(run: Callable[[list[str]], int] = _run) -> None:
    run(["systemctl", "restart", "connman"])
    run(["systemctl", "restart", "wpa_supplicant"])


def _pull_down_gpio(num: int, write: Callable[[str, str], None]) -> None:
    write("/sys/class/gpio/export", str(num))
    write(f"/sys/class/gpio/gpio{num}/direction", "out")
    write(f"/sys/class/gpio/gpio{num}/value", "0")


def gpio_reset_and_reboot(
    run: Callable[[list[str]], int] = _run,
    write: Callable[[str, str], None] = _write,
    read_alias: Callable[[], str | None] = _read_alias,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    controller = read_alias()
    if controller:
        write("/sys/bus/platform/drivers/sdhci-esdhc-imx/unbind", controller)
    _pull_down_gpio(WL_REG_ON_GPIO, write)
    _pull_down_gpio(BT_REG_ON_GPIO, write)
    sleep(1)
    write("/sys/class/gpio/unexport", str(WL_REG_ON_GPIO))
    write("/sys/class/gpio/unexport", str(BT_REG_ON_GPIO))
    run(["systemctl", "reboot"])
