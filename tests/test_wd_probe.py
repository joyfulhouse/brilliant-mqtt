from brilliant_wifi_watchdog import probe


def test_default_gateway_parses_ip_route() -> None:
    out = "default via 192.168.1.1 dev wlan0 proto dhcp metric 600\n"
    assert probe.default_gateway(run=lambda argv: (0, out)) == "192.168.1.1"


def test_default_gateway_none_when_absent() -> None:
    assert probe.default_gateway(run=lambda argv: (0, "")) is None


def test_ping_true_on_zero_rc() -> None:
    assert probe.ping("192.168.1.1", run=lambda argv: 0) is True
    assert probe.ping("192.168.1.1", run=lambda argv: 1) is False
