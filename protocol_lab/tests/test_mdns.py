from brilliant_protocol_lab.mdns import INIT_SERVICE, normalize_service


def test_normalizes_known_txt_keys_and_hashes_ids() -> None:
    observation = normalize_service(
        service_type=INIT_SERVICE,
        instance="office._init-brilliant._tcp.local.",
        addresses=("10.100.0.10",),
        port=5555,
        properties={
            b"device_id": b"0123456789abcdef0123456789abcdef",
            b"provisioning_port": b"5556",
            b"unknown": b"discard-me",
        },
    )
    assert observation.addresses == ("10.100.0.10",)
    device_id = observation.properties["device_id"]
    assert isinstance(device_id, str)
    assert device_id.startswith("id:")
    assert observation.properties["provisioning_port"] == 5556
    assert "unknown" not in observation.properties
