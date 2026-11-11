"""Unit tests for the Virtual HMI / Detached Slider Controller."""

from __future__ import annotations

import asyncio
import json
import sys
import types
from pathlib import Path
from typing import Any

import pytest

from brilliant_mqtt.bus import HostedVirtualPeripheralAdapter
from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.model import BrilliantDevice, DeviceKind, Variable
from brilliant_mqtt.protocols import (
    VirtualPeripheralHostProtocol,
    VirtualPeripheralSpec,
)
from brilliant_mqtt.retained_topics import RetainedTopicLedger
from brilliant_mqtt.virtual_sliders import (
    SliderMode,
    VirtualSliderController,
    VirtualSliderSpec,
    brightness_to_raw,
    decode_slider_config_target,
    encode_slider_config,
    parse_virtual_slider_specs,
    raw_to_brightness,
)
from brilliant_mqtt.write_admission import AdmissionTicket, WriteClass
from tests.fakes import FakeBus, FakeMqtt
from tools.brilliant_vc.slider_binding import decode_slider_config as vc_decode_slider_config

PANEL_DEV_ID = "019c7b7d-36df-4fe9-a49e-48e05136f5a4"


class FakeVirtualPeripheralHost(VirtualPeripheralHostProtocol):
    """In-memory fake for VirtualPeripheralHostProtocol."""

    def __init__(
        self,
        spec: VirtualPeripheralSpec,
        on_external_set: Any,
    ) -> None:
        self.spec = spec
        self.on_external_set = on_external_set
        self.started = False
        self.stopped = False
        self.variables: dict[str, Any] = dict(spec.initial_variables)

    async def start(self) -> None:
        self.started = True

    async def set_variable(self, name: str, value: Any) -> None:
        self.variables[name] = value

    async def shutdown(self) -> None:
        self.stopped = True

    async def simulate_bus_write(self, var_name: str, value: Any) -> None:
        self.variables[var_name] = value
        await self.on_external_set(var_name, value)


class _ConfigurableFakeBus(FakeBus):
    """FakeBus that reflects set_variables onto in-memory devices when reflect_writes is True."""

    def __init__(self, devices: list[BrilliantDevice] | None = None) -> None:
        super().__init__(devices or [])
        self.reflect_writes: bool = True

    async def set_variables(
        self,
        device_id: str,
        peripheral_id: str,
        sets: list[VarSet],
        *,
        write_class: WriteClass = WriteClass.INTERACTIVE_FIFO,
        ticket: AdmissionTicket | None = None,
    ) -> str:
        res = await super().set_variables(
            device_id, peripheral_id, sets, write_class=write_class, ticket=ticket
        )
        if self.reflect_writes:
            for d in self._devices:
                if d.device_id == device_id and d.peripheral_id == peripheral_id:
                    for vs in sets:
                        d.variables[vs.name] = Variable(vs.name, vs.value)
        return res


class TestSliderConfigThrift:
    def test_encode_decode_roundtrip_panel_load_and_vc_crosscheck(self) -> None:
        b64 = encode_slider_config(
            PANEL_DEV_ID,
            "vs_dimmer_living_left",
            slider_index=2,
            double_tap_scene_id="scene-double-123",
        )
        assert b64 != ""
        decoded = decode_slider_config_target(b64)
        assert decoded == (PANEL_DEV_ID, "vs_dimmer_living_left")

        # Cross-check against tools/brilliant_vc/slider_binding.py TBinaryProtocol decoder
        vc_binding = vc_decode_slider_config(b64)
        assert vc_binding.slider_index == 2
        assert vc_binding.device_id == PANEL_DEV_ID
        assert vc_binding.peripheral_id == "vs_dimmer_living_left"
        assert vc_binding.action == 0

    def test_encode_decode_roundtrip_ble_mesh_load(self) -> None:
        b64 = encode_slider_config("ble_mesh", "018691f172bc00060153fd53c15b8c3d")
        decoded = decode_slider_config_target(b64)
        assert decoded == ("ble_mesh", "018691f172bc00060153fd53c15b8c3d")
        vc_binding = vc_decode_slider_config(b64)
        assert vc_binding.slider_index == 0
        assert vc_binding.device_id == "ble_mesh"
        assert vc_binding.peripheral_id == "018691f172bc00060153fd53c15b8c3d"

    def test_empty_or_malformed_returns_empty_and_none(self) -> None:
        assert encode_slider_config("", "") == ""
        assert decode_slider_config_target("") is None
        assert decode_slider_config_target("not-valid-base64!!!") is None
        # Protobuf-formatted bytes must be rejected by the Thrift decoder
        assert decode_slider_config_target("CgYKAWEKAmI=") is None


class TestVirtualSliderSpec:
    def test_ble_switch_spec_builders(self) -> None:
        spec = VirtualSliderSpec.for_ble_switch(
            slider_id="half_bath",
            name="Half Bath",
            switch_id="018691f172bc00060153fd53c15b8c3d",
            load_peripheral_id="018691f172bc00060153fd53c15b8c3d",
            scene_single_tap="scene-1",
            scene_double_tap="scene-2",
        )
        assert spec.config_device_id == "ble_mesh"
        assert spec.config_peripheral_id == "switch_config:018691f172bc00060153fd53c15b8c3d"
        assert spec.config_var_name == "slider_config"
        assert spec.virtual_peripheral_id == "vs_dimmer_half_bath"

        virt_b64 = spec.virtual_slider_config_b64(PANEL_DEV_ID)
        assert decode_slider_config_target(virt_b64) == (PANEL_DEV_ID, "vs_dimmer_half_bath")

        pass_b64 = spec.passthrough_slider_config_b64()
        assert decode_slider_config_target(pass_b64) == (
            "ble_mesh",
            "018691f172bc00060153fd53c15b8c3d",
        )

    def test_panel_slider_spec_builders(self) -> None:
        spec = VirtualSliderSpec.for_panel_slider(
            slider_id="living_left",
            name="Living Room Left",
            panel_device_id=PANEL_DEV_ID,
            slider_index=0,
            load_peripheral_id="gangbox_peripheral_0",
        )
        assert spec.config_device_id == PANEL_DEV_ID
        assert spec.config_peripheral_id == "faceplate_peripheral"
        assert spec.config_var_name == "slider_config:0"

    def test_panel_slider_invalid_index_raises(self) -> None:
        with pytest.raises(ValueError, match="slider_index"):
            VirtualSliderSpec.for_panel_slider(
                slider_id="bad",
                name="Bad",
                panel_device_id=PANEL_DEV_ID,
                slider_index=4,
            )

    def test_brightness_scaling_roundtrip(self) -> None:
        assert raw_to_brightness(100) == 1
        assert raw_to_brightness(1000) == 255
        assert raw_to_brightness(550) == 128
        assert brightness_to_raw(1) == 100
        assert brightness_to_raw(255) == 1000

    def test_parse_virtual_slider_specs_json(self) -> None:
        raw = json.dumps(
            [
                {
                    "slider_id": "half_bath",
                    "name": "Half Bath",
                    "switch_id": "018691f172bc00060153fd53c15b8c3d",
                    "load_peripheral_id": "018691f172bc00060153fd53c15b8c3d",
                    "scene_single_tap": "s1",
                    "scene_double_tap": "s2",
                    "num_steps": 5,
                },
                {
                    "slider_id": "living_left",
                    "name": "Living Room Left",
                    "slider_index": 0,
                    "load_peripheral_id": "gangbox_peripheral_0",
                },
            ]
        )
        specs = parse_virtual_slider_specs(raw, own_device_id=PANEL_DEV_ID)
        assert len(specs) == 2
        assert specs[0].slider_id == "half_bath"
        assert specs[1].config_device_id == PANEL_DEV_ID
        assert specs[1].config_var_name == "slider_config:0"


class TestHostedVirtualPeripheralAdapterContract:
    async def test_adapter_follows_panel_hosting_contract_and_deletes_on_shutdown(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        deleted_calls: list[tuple[Any, str, int]] = []
        shutdown_called = False
        internal_updates: list[tuple[str, Any, bool]] = []

        class FakeVariableSpec:
            def __init__(
                self,
                vtype: type,
                settable: bool,
                default_value: Any = None,
                push_func: Any = None,
            ) -> None:
                self.vtype = vtype
                self.settable = settable
                self.default_value = default_value
                self.push_func = push_func

        class FakePeripheral:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                pass

            def _set_value_internal(self, name: str, value: Any, notify: bool = True) -> None:
                internal_updates.append((name, value, notify))

        class FakeHostedStartableSpec:
            def __init__(self, sid: str, cfg: Any, extra: dict[str, Any]) -> None:
                self.sid = sid
                self.cfg = cfg
                self.extra = extra

        class FakePeripheralConfig:
            def __init__(self, sid: str, cls: Any) -> None:
                self.sid = sid
                self.cls = cls

        class FakePeripheralHost:
            def __init__(
                self,
                *,
                loop: Any,
                startable_id: str,
                startables_to_host: list[FakeHostedStartableSpec],
                parallel_registration_limit: int,
                raise_errors_for_lost_user_configured_data: bool,
                message_bus_address_override: Any,
            ) -> None:
                self.startable_id = startable_id
                self.startables_to_host = startables_to_host
                self.parallel_registration_limit = parallel_registration_limit

            async def start(self) -> None:
                spec_item = self.startables_to_host[0]
                instance = spec_item.cfg.cls()
                # Verify _my_variables is a callable method, not a property!
                assert callable(instance._my_variables)
                vars_map = instance._my_variables()
                assert "on" in vars_map
                assert "intensity" in vars_map
                assert instance.name == "vs_dimmer_test"
                assert instance.peripheral_type == 1

            async def shutdown(self) -> None:
                nonlocal shutdown_called
                shutdown_called = True

        class FakeConditionalPeripheralHost:
            def delete_peripheral(self, name: str, deletion_time_ms: int) -> None:
                deleted_calls.append((self, name, deletion_time_ms))

        # Register fake firmware modules in sys.modules
        mod_periph = types.ModuleType("peripherals.lib.peripheral_service.peripheral")
        mod_periph.__dict__["Peripheral"] = FakePeripheral
        mod_periph.__dict__["VariableSpec"] = FakeVariableSpec

        mod_startable = types.ModuleType("lib.startables.startable")
        mod_startable.__dict__["HostedStartableSpec"] = FakeHostedStartableSpec

        mod_phost = types.ModuleType("peripherals.lib.peripheral_service.peripheral_host")
        mod_phost.__dict__["PeripheralConfig"] = FakePeripheralConfig
        mod_phost.__dict__["PeripheralHost"] = FakePeripheralHost

        mod_chost = types.ModuleType(
            "peripherals.lib.peripheral_service.conditional_peripheral_host"
        )
        mod_chost.__dict__["ConditionalPeripheralHost"] = FakeConditionalPeripheralHost

        for name, mod in (
            ("lib", types.ModuleType("lib")),
            ("lib.startables", types.ModuleType("lib.startables")),
            ("lib.startables.startable", mod_startable),
            ("peripherals", types.ModuleType("peripherals")),
            ("peripherals.lib", types.ModuleType("peripherals.lib")),
            (
                "peripherals.lib.peripheral_service",
                types.ModuleType("peripherals.lib.peripheral_service"),
            ),
            ("peripherals.lib.peripheral_service.peripheral", mod_periph),
            ("peripherals.lib.peripheral_service.peripheral_host", mod_phost),
            (
                "peripherals.lib.peripheral_service.conditional_peripheral_host",
                mod_chost,
            ),
        ):
            monkeypatch.setitem(sys.modules, name, mod)

        vspec = VirtualPeripheralSpec(
            peripheral_id="vs_dimmer_test",
            display_name="Test Dimmer",
            peripheral_type=1,
            initial_variables={"on": 1, "intensity": 1000},
            settable_variables=frozenset({"on", "intensity"}),
        )

        async def _noop(k: str, v: Any) -> None:
            pass

        adapter = HostedVirtualPeripheralAdapter(vspec, _noop)
        await adapter.start()
        await adapter.set_variable("intensity", 550)
        assert internal_updates == [("intensity", 550, True)]

        await adapter.shutdown()
        assert len(deleted_calls) == 1
        assert deleted_calls[0][1] == "vs_dimmer_test"
        assert shutdown_called is True


class TestVirtualSliderController:
    async def test_startup_backs_up_original_slider_config_and_restores_on_passthrough(
        self, tmp_path: Path
    ) -> None:
        """Even without load_peripheral_id configured, original_slider_config is
        backed up from the bus before VIRTUAL binding and restored on PASSTHROUGH."""
        original_b64 = encode_slider_config(PANEL_DEV_ID, "gangbox_peripheral_0")
        faceplate = BrilliantDevice(
            device_id=PANEL_DEV_ID,
            peripheral_id="faceplate_peripheral",
            name="Faceplate",
            kind=DeviceKind.UNKNOWN,
            variables={"slider_config:0": Variable("slider_config:0", original_b64)},
        )
        bus = _ConfigurableFakeBus([faceplate])
        mqtt = FakeMqtt()
        hosts: dict[str, FakeVirtualPeripheralHost] = {}

        def make_host(vspec: VirtualPeripheralSpec, cb: Any) -> VirtualPeripheralHostProtocol:
            h = FakeVirtualPeripheralHost(vspec, cb)
            hosts[vspec.peripheral_id] = h
            return h

        # Notice load_peripheral_id is omitted (""): passthrough relies on backed-up original!
        spec = VirtualSliderSpec.for_panel_slider(
            slider_id="living_left",
            name="Living Room Left",
            panel_device_id=PANEL_DEV_ID,
            slider_index=0,
            tap_window_s=0.05,
        )
        state_file = tmp_path / "vs_state.json"
        ledger = RetainedTopicLedger("living_room", tmp_path / "owned-topics.json")
        await ledger.async_load()

        ctrl = VirtualSliderController(
            bus=bus,
            mqtt=mqtt,
            panel="living_room",
            own_device_id=PANEL_DEV_ID,
            specs=[spec],
            host_factory=make_host,
            state_path=state_file,
            owned_topics=ledger,
        )
        await ctrl.start()

        # Verify lossless push registration
        assert bus.change_callback_modes == [False]  # coalesce_pushes=False

        # Verify original_slider_config was captured and persisted
        assert ctrl.state_for("living_left").original_slider_config == original_b64
        saved_disk = json.loads(state_file.read_text(encoding="utf-8"))
        assert saved_disk["living_left"]["original_slider_config"] == original_b64

        # Verify 4-part state topic and canonical device block ("model": "Control")
        state_topic = "brilliant/living_room/virtual_slider_living_left/state"
        assert state_topic in ledger.topics
        light_cfg_pub = next(
            p
            for p in mqtt.published
            if p[0] == "homeassistant/light/brilliant_living_room_vs_living_left_light/config"
        )
        light_cfg = json.loads(light_cfg_pub[1])
        assert light_cfg["device"]["model"] == "Control"
        assert light_cfg["state_topic"] == state_topic

        # Switch to passthrough -> restores original_b64!
        bus.commands.clear()
        await ctrl.on_mqtt_command(
            "brilliant/living_room/virtual_slider/living_left/set_mode",
            "passthrough",
        )
        assert ctrl.state_for("living_left").mode is SliderMode.PASSTHROUGH
        assert len(bus.commands) == 1
        assert bus.commands[0] == (
            PANEL_DEV_ID,
            "faceplate_peripheral",
            [VarSet("slider_config:0", original_b64)],
        )
        await ctrl.shutdown()

    async def test_failed_or_superseded_bus_write_does_not_mutate_mode(
        self, tmp_path: Path
    ) -> None:
        original_b64 = encode_slider_config(PANEL_DEV_ID, "gangbox_peripheral_0")
        faceplate = BrilliantDevice(
            device_id=PANEL_DEV_ID,
            peripheral_id="faceplate_peripheral",
            name="Faceplate",
            kind=DeviceKind.UNKNOWN,
            variables={"slider_config:0": Variable("slider_config:0", original_b64)},
        )
        bus = _ConfigurableFakeBus([faceplate])
        mqtt = FakeMqtt()
        spec = VirtualSliderSpec.for_panel_slider(
            slider_id="living_left",
            name="Living Room Left",
            panel_device_id=PANEL_DEV_ID,
            slider_index=0,
        )
        ctrl = VirtualSliderController(
            bus=bus,
            mqtt=mqtt,
            panel="living_room",
            own_device_id=PANEL_DEV_ID,
            specs=[spec],
            host_factory=lambda vspec, cb: FakeVirtualPeripheralHost(vspec, cb),
            state_path=tmp_path / "vs_state.json",
        )
        await ctrl.start()
        assert ctrl.state_for("living_left").mode is SliderMode.VIRTUAL

        # Now make read-back verification fail (bus ignores the write)
        bus.reflect_writes = False
        await ctrl.on_mqtt_command(
            "brilliant/living_room/virtual_slider/living_left/set_mode",
            "passthrough",
        )
        # Mode must remain VIRTUAL because the bus write verification failed!
        assert ctrl.state_for("living_left").mode is SliderMode.VIRTUAL

        # Also test bus RPC exception
        bus.set_variables_error = RuntimeError("rpc timeout")
        await ctrl.on_mqtt_command(
            "brilliant/living_room/virtual_slider/living_left/set_mode",
            "passthrough",
        )
        assert ctrl.state_for("living_left").mode is SliderMode.VIRTUAL
        await ctrl.shutdown()

    async def test_refuses_to_overwrite_slider_config_when_unparseable_and_unbacked_up(
        self, tmp_path: Path
    ) -> None:
        """Fail closed: if original_slider_config is not backed up and the current
        bus value cannot be decoded as valid Thrift CapTouchSliderConfig, do not overwrite."""
        faceplate = BrilliantDevice(
            device_id=PANEL_DEV_ID,
            peripheral_id="faceplate_peripheral",
            name="Faceplate",
            kind=DeviceKind.UNKNOWN,
            variables={"slider_config:0": Variable("slider_config:0", "CgYKAWEKAmI=")},
        )
        bus = _ConfigurableFakeBus([faceplate])
        mqtt = FakeMqtt()
        spec = VirtualSliderSpec.for_panel_slider(
            slider_id="living_left",
            name="Living Room Left",
            panel_device_id=PANEL_DEV_ID,
            slider_index=0,
            load_peripheral_id="gangbox_peripheral_0",
        )
        ctrl = VirtualSliderController(
            bus=bus,
            mqtt=mqtt,
            panel="living_room",
            own_device_id=PANEL_DEV_ID,
            specs=[spec],
            host_factory=lambda vspec, cb: FakeVirtualPeripheralHost(vspec, cb),
            state_path=tmp_path / "vs_state.json",
        )
        await ctrl.start()
        # Must NOT have issued any set_variables call when slider_config:0 is not valid Thrift
        assert bus.commands == []
        assert ctrl.state_for("living_left").original_slider_config is None
        await ctrl.shutdown()

    async def test_mesh_leader_gating_and_orphaned_slider_restore(self, tmp_path: Path) -> None:
        orig_mesh_b64 = encode_slider_config("ble_mesh", "018691f172bc00060153fd53c15b8c3d")
        sw_cfg = BrilliantDevice(
            device_id="ble_mesh",
            peripheral_id="switch_config:018691f172bc00060153fd53c15b8c3d",
            name="Switch Config",
            kind=DeviceKind.UNKNOWN,
            variables={"slider_config": Variable("slider_config", orig_mesh_b64)},
        )
        bus = _ConfigurableFakeBus([sw_cfg])
        mqtt = FakeMqtt()
        is_leader = False

        spec = VirtualSliderSpec.for_ble_switch(
            slider_id="half_bath",
            name="Half Bath",
            switch_id="018691f172bc00060153fd53c15b8c3d",
        )
        state_file = tmp_path / "vs_state.json"
        ctrl = VirtualSliderController(
            bus=bus,
            mqtt=mqtt,
            panel="living_room",
            own_device_id=PANEL_DEV_ID,
            specs=[spec],
            host_factory=lambda vspec, cb: FakeVirtualPeripheralHost(vspec, cb),
            state_path=state_file,
            is_mesh_leader=lambda: is_leader,
        )
        await ctrl.start()
        # Not mesh leader yet -> zero bus writes to ble_mesh!
        assert bus.commands == []

        # Acquire mesh leadership -> binds ble_mesh slider and saves original_slider_config
        is_leader = True
        await ctrl.on_mesh_leadership_acquired()
        assert len(bus.commands) == 1
        assert ctrl.state_for("half_bath").original_slider_config == orig_mesh_b64
        await ctrl.shutdown()

        # Now start a new controller with empty specs (slider removed from config) ->
        # restores orig_mesh_b64 onto the bus!
        bus.commands.clear()
        ctrl2 = VirtualSliderController(
            bus=bus,
            mqtt=mqtt,
            panel="living_room",
            own_device_id=PANEL_DEV_ID,
            specs=[],
            host_factory=lambda vspec, cb: FakeVirtualPeripheralHost(vspec, cb),
            state_path=state_file,
            is_mesh_leader=lambda: True,
        )
        await ctrl2.start()
        assert bus.commands == [
            (
                "ble_mesh",
                "switch_config:018691f172bc00060153fd53c15b8c3d",
                [VarSet("slider_config", orig_mesh_b64)],
            )
        ]
        await ctrl2.shutdown()

    async def test_startup_seeds_scene_timestamp_and_classifies_multi_tap_and_swipes(
        self, tmp_path: Path
    ) -> None:
        exec_dev_initial = BrilliantDevice(
            device_id=PANEL_DEV_ID,
            peripheral_id="execution_peripheral",
            name="Execution",
            kind=DeviceKind.UNKNOWN,
            variables={
                "last_executed_scene_id": Variable(
                    "last_executed_scene_id", "scene-1tap", timestamp_ms=500
                )
            },
        )
        bus = _ConfigurableFakeBus([exec_dev_initial])
        mqtt = FakeMqtt()
        hosts: dict[str, FakeVirtualPeripheralHost] = {}

        def make_host(vspec: VirtualPeripheralSpec, cb: Any) -> VirtualPeripheralHostProtocol:
            h = FakeVirtualPeripheralHost(vspec, cb)
            hosts[vspec.peripheral_id] = h
            return h

        spec = VirtualSliderSpec.for_ble_switch(
            slider_id="half_bath",
            name="Half Bath",
            switch_id="018691f172bc00060153fd53c15b8c3d",
            load_peripheral_id="018691f172bc00060153fd53c15b8c3d",
            scene_single_tap="scene-1tap",
            scene_double_tap="scene-2tap",
            tap_window_s=0.05,
        )
        ctrl = VirtualSliderController(
            bus=bus,
            mqtt=mqtt,
            panel="living_room",
            own_device_id=PANEL_DEV_ID,
            specs=[spec],
            host_factory=make_host,
            state_path=tmp_path / "vs_state.json",
        )
        await ctrl.start()
        mqtt.published.clear()

        # Replaying timestamp_ms=500 or timestamp_ms=None must be ignored!
        await ctrl.on_bus_device_change(exec_dev_initial)
        await ctrl.on_bus_device_change(
            BrilliantDevice(
                device_id=PANEL_DEV_ID,
                peripheral_id="execution_peripheral",
                name="Execution",
                kind=DeviceKind.UNKNOWN,
                variables={
                    "last_executed_scene_id": Variable(
                        "last_executed_scene_id", "scene-1tap", timestamp_ms=None
                    )
                },
            )
        )
        await asyncio.sleep(0.08)
        assert [p for p in mqtt.published if p[0].endswith("/event")] == []

        # Triple tap: double_tap (ts=1000) + single_tap (ts=1150)
        await ctrl.on_bus_device_change(
            BrilliantDevice(
                device_id=PANEL_DEV_ID,
                peripheral_id="execution_peripheral",
                name="Execution",
                kind=DeviceKind.UNKNOWN,
                variables={
                    "last_executed_scene_id": Variable(
                        "last_executed_scene_id", "scene-2tap", timestamp_ms=1000
                    )
                },
            )
        )
        await ctrl.on_bus_device_change(
            BrilliantDevice(
                device_id=PANEL_DEV_ID,
                peripheral_id="execution_peripheral",
                name="Execution",
                kind=DeviceKind.UNKNOWN,
                variables={
                    "last_executed_scene_id": Variable(
                        "last_executed_scene_id", "scene-1tap", timestamp_ms=1150
                    )
                },
            )
        )
        await asyncio.sleep(0.08)

        ev_pubs = [
            p
            for p in mqtt.published
            if p[0] == "brilliant/living_room/virtual_slider/half_bath/event"
        ]
        assert len(ev_pubs) == 1
        assert json.loads(ev_pubs[0][1])["event_type"] == "triple_tap"

        # Simulate swipe down on virtual peripheral
        mqtt.published.clear()
        host = hosts["vs_dimmer_half_bath"]
        await host.simulate_bus_write("intensity", 280)
        ev_pubs = [
            p
            for p in mqtt.published
            if p[0] == "brilliant/living_room/virtual_slider/half_bath/event"
        ]
        assert len(ev_pubs) == 1
        ev_data = json.loads(ev_pubs[0][1])
        assert ev_data["event_type"] == "swipe_down"
        assert ev_data["step"] == 1
        await ctrl.shutdown()
