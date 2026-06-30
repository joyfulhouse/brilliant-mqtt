from brilliant_wifi_watchdog import recovery


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
    recovery.gpio_reset_and_reboot(run=r.run, write=r.write, read_alias=lambda: "2194000.mmc")
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
