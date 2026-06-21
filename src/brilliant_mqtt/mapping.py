"""HA entity mapping — converts BrilliantDevice into EntityDescriptor(s).

No panel imports, no MQTT imports: pure Python / stdlib only.

The ``AUX_SPECS`` table below is the single source of truth for the
variable-backed "extended" entities (power monitoring, panel controls,
presence/privacy) added in Milestone 10. Both entity discovery
(:func:`entities_for`) and state payload rendering (:func:`payload_fields`)
derive from it, so a spec is declared exactly once.
"""

from __future__ import annotations

from dataclasses import dataclass

from brilliant_mqtt.model import BrilliantDevice, DeviceKind, Variable


@dataclass(frozen=True)
class AuxSpec:
    """One variable-backed auxiliary entity for a given DeviceKind.

    ``var`` is the bus variable name; ``payload_key`` (defaulting to ``var``) is
    the key used in the shared JSON state payload and read by the entity's
    value_template. ``value_kind`` drives both payload rendering and inbound
    command translation.

    ``skip_values`` lists raw variable values that mean "no reading" (e.g. the
    power sentinel "-1" on uncalibrated mesh loads). A matching value
    suppresses BOTH the entity descriptor and the payload key — symmetric on
    purpose, so the descriptor set and the payload keys never drift apart (an
    entity whose template reads a missing key logs HA template errors; a
    payload key without an entity is dead weight). Because devices are
    re-evaluated at every reconcile/poll, a gated entity appears at the next
    reconcile once the variable reports a real value.
    """

    var: str
    component: str  # "sensor" | "binary_sensor" | "switch" | "number" | "button"
    name: str
    value_kind: str  # "bool" | "int" | "float" | "str"
    payload_key: str | None = None
    device_class: str | None = None
    unit: str | None = None
    state_class: str | None = None
    entity_category: str | None = None  # None | "config" | "diagnostic"
    enabled_by_default: bool = True
    invert: bool = False
    min_value: float | None = None
    max_value: float | None = None
    step: float | None = None
    skip_values: tuple[str, ...] = ()
    # When set, this aux's reading is only meaningful while the named sibling
    # variable is enabled ("1"). Otherwise the bus value is stale (e.g. a frozen
    # ``movement_detected`` latch when motion-scoring is off) and the payload is
    # forced to a concrete False rather than publishing the stale value. Only
    # the entity DESCRIPTOR is unaffected — the sensor still exists, it just
    # reads "off" until the subsystem is enabled. Supported for ``value_kind=
    # "bool"`` only (enforced in ``__post_init__``), since the disabled-state
    # value is a boolean False.
    gate_var: str | None = None

    def __post_init__(self) -> None:
        # Validate the static spec table once at construction (import time)
        # rather than per-render: a gated reading collapses to boolean False, so
        # gate_var is meaningful only on bool specs. A non-bool gated spec would
        # publish a type-wrong False, so reject it loudly at the source.
        if self.gate_var is not None and self.value_kind != "bool":
            raise ValueError(
                f"gate_var is only supported for value_kind='bool', "
                f"not {self.value_kind!r} (spec var={self.var!r})"
            )

    @property
    def key(self) -> str:
        """The payload/template key for this spec (``payload_key`` or ``var``)."""
        return self.payload_key or self.var


# Shared power/temperature/fault specs for the gangbox load kinds. LIGHT, SWITCH,
# and ALWAYS_ON all expose the same three monitoring variables.
_LOAD_AUX: tuple[AuxSpec, ...] = (
    AuxSpec(
        var="power",
        component="sensor",
        name="Power",
        value_kind="float",
        device_class="power",
        unit="W",
        state_class="measurement",
        # "-1" is the bus sentinel for "no reading" (uncalibrated mesh loads);
        # panel loads always report real watts, so this never gates them.
        skip_values=("-1",),
    ),
    AuxSpec(
        var="temperature",
        component="sensor",
        name="Temperature",
        value_kind="float",
        device_class="temperature",
        unit="°C",
        state_class="measurement",
        entity_category="diagnostic",
    ),
    AuxSpec(
        var="is_safe",
        component="binary_sensor",
        name="Fault",
        value_kind="bool",
        payload_key="fault",
        device_class="problem",
        entity_category="diagnostic",
        invert=True,  # problem ON when is_safe is "0"
    ),
)


# Integrated motion-sensing subsystem present on every mesh load peripheral
# (LIGHT/SWITCH/ALWAYS_ON on the virtual ble_mesh device).
# Live-verified on office.iot, 2026-06-13 via get_device("ble_mesh") — all 20
# load peripherals (11 LIGHT, 1 GENERIC_ON_OFF→SWITCH, 8 ALWAYS_ON) carry
# these five variables. Panel loads lack them, so these specs auto-no-op there.
# Note: variable names differ from the faceplate's PIR variables
# (e.g. "motion_score" vs "pir_motion_score") — only "movement_detected" is
# shared. No lux/illuminance variable exists on mesh loads.
_MOTION_AUX: tuple[AuxSpec, ...] = (
    AuxSpec(
        var="movement_detected",
        component="binary_sensor",
        name="Motion",
        value_kind="bool",
        payload_key="motion",
        device_class="motion",
        # movement_detected only tracks live presence while motion-scoring is on;
        # with it off the bus reports a frozen latch (live-verified office.iot
        # 2026-06-14), so gate the published value on enable_motion_score.
        gate_var="enable_motion_score",
    ),
    AuxSpec(
        var="motion_score",
        component="sensor",
        name="Motion Score",
        value_kind="int",
        state_class="measurement",
        entity_category="diagnostic",
        enabled_by_default=False,
    ),
    AuxSpec(
        var="enable_motion_score",
        component="switch",
        name="Motion Score Reporting",
        value_kind="bool",
        entity_category="config",
        enabled_by_default=False,
    ),
    # 0–100 range is ASSUMED (bus did not document valid bounds; observed
    # values 70 high / 20 low on 2026-06-13 live probe). Revisit if firmware
    # documents an explicit range.
    AuxSpec(
        var="motion_high_threshold",
        component="number",
        name="Motion High Threshold",
        value_kind="int",
        entity_category="config",
        min_value=0,
        max_value=100,
        step=1,
        enabled_by_default=False,
    ),
    AuxSpec(
        var="motion_low_threshold",
        component="number",
        name="Motion Low Threshold",
        value_kind="int",
        entity_category="config",
        min_value=0,
        max_value=100,
        step=1,
        enabled_by_default=False,
    ),
)


# The variable-entity table. Keyed by DeviceKind; each entry is the tuple of
# auxiliary entities that kind contributes (in addition to any primary entity).
AUX_SPECS: dict[DeviceKind, tuple[AuxSpec, ...]] = {
    DeviceKind.LIGHT: _LOAD_AUX + _MOTION_AUX,
    DeviceKind.SWITCH: _LOAD_AUX + _MOTION_AUX,
    DeviceKind.ALWAYS_ON: _LOAD_AUX + _MOTION_AUX,
    DeviceKind.BINARY_SENSOR: (
        AuxSpec(
            var="led_on",
            component="switch",
            name="Faceplate LED",
            value_kind="bool",
            entity_category="config",
        ),
        AuxSpec(
            var="enable_lux",
            component="switch",
            name="Illuminance Sensor",
            value_kind="bool",
            entity_category="config",
        ),
        AuxSpec(
            var="pir_motion_score",
            component="sensor",
            name="PIR Score",
            value_kind="int",
            state_class="measurement",
            entity_category="diagnostic",
            enabled_by_default=False,
        ),
        # Faceplate motion-detection subsystem (live-probed; all externally
        # settable). DISABLED-by-default — advanced tuning. movement_detected is
        # driven by whichever detection mode(s) are enabled here; the PIR
        # thresholds only take effect when enable_pir_motion_score is on, and
        # screen-based detection (enable_screen_motion_detection) can register
        # screen/ambient changes as motion independently of the PIR.
        AuxSpec(
            var="enable_screen_motion_detection",
            component="switch",
            name="Screen Motion Detection",
            value_kind="bool",
            entity_category="config",
            enabled_by_default=False,
        ),
        AuxSpec(
            var="enable_pir_motion_score",
            component="switch",
            name="PIR Score Reporting",
            value_kind="bool",
            entity_category="config",
            enabled_by_default=False,
        ),
        AuxSpec(
            var="enable_light_motion_detection",
            component="switch",
            name="Light Motion Detection",
            value_kind="bool",
            entity_category="config",
            enabled_by_default=False,
        ),
        # 0–100 range ASSUMED (same as the mesh thresholds — no firmware bound
        # documented; live-observed 25 high / 14 low on the faceplate).
        AuxSpec(
            var="pir_motion_detection_high_threshold",
            component="number",
            name="PIR Motion High Threshold",
            value_kind="int",
            entity_category="config",
            min_value=0,
            max_value=100,
            step=1,
            enabled_by_default=False,
        ),
        AuxSpec(
            var="pir_motion_detection_low_threshold",
            component="number",
            name="PIR Motion Low Threshold",
            value_kind="int",
            entity_category="config",
            min_value=0,
            max_value=100,
            step=1,
            enabled_by_default=False,
        ),
        AuxSpec(
            var="hottest_internal_temperature",
            component="sensor",
            name="Internal Temperature",
            value_kind="float",
            device_class="temperature",
            unit="°C",
            state_class="measurement",
            entity_category="diagnostic",
            enabled_by_default=False,
        ),
    ),
    DeviceKind.HARDWARE: (
        AuxSpec(
            var="muted",
            component="switch",
            name="Microphone Mute",
            value_kind="bool",
            entity_category="config",
        ),
        AuxSpec(
            var="screen_on",
            component="switch",
            name="Screen",
            value_kind="bool",
            entity_category="config",
        ),
        AuxSpec(
            var="screen_brightness",
            component="number",
            name="Screen Brightness",
            value_kind="int",
            entity_category="config",
            min_value=0,
            max_value=10,
            step=1,
        ),
        AuxSpec(
            var="output_volume",
            component="number",
            name="Volume",
            value_kind="int",
            entity_category="config",
            min_value=0,
            max_value=100,
            step=1,
        ),
        AuxSpec(
            var="alert_volume",
            component="number",
            name="Alert Volume",
            value_kind="int",
            entity_category="config",
            min_value=0,
            max_value=100,
            step=1,
            enabled_by_default=False,
        ),
        AuxSpec(
            var="cpu_temperature",
            component="sensor",
            name="CPU Temperature",
            value_kind="float",
            device_class="temperature",
            unit="°C",
            state_class="measurement",
            entity_category="diagnostic",
        ),
        # Deliberately read-only binary_sensor — bus marks it settable, but write
        # semantics were never verified; revisit if needed.
        AuxSpec(
            var="camera_on",
            component="binary_sensor",
            name="Camera",
            value_kind="bool",
            device_class="running",
            entity_category="diagnostic",
        ),
        # Deliberately read-only binary_sensor — bus marks it settable, but write
        # semantics were never verified; revisit if needed.
        AuxSpec(
            var="privacy_toggle",
            component="binary_sensor",
            name="Privacy Mode",
            value_kind="bool",
            entity_category="diagnostic",
            enabled_by_default=False,
        ),
        # Panel firmware tag (e.g. "v26.05.20.2") — read-only diagnostic string.
        # Machine consumers should prefer the brilliant/<panel>/bridge meta topic;
        # this entity exists for humans and user automations.
        # A blank tag means "unknown" — gated to match _sw_version_from (bridge.py),
        # the other reader of this variable.
        AuxSpec(
            var="current_release_tag",
            component="sensor",
            name="Firmware",
            value_kind="str",
            entity_category="diagnostic",
            skip_values=("",),
        ),
    ),
    DeviceKind.UI: (
        # Deliberately read-only binary_sensor — bus marks it settable, but write
        # semantics were never verified; revisit if needed.
        AuxSpec(
            var="active",
            component="binary_sensor",
            name="In Use",
            value_kind="bool",
            device_class="occupancy",
        ),
        AuxSpec(
            var="child_lock_enabled",
            component="switch",
            name="Child Lock",
            value_kind="bool",
            entity_category="config",
        ),
        AuxSpec(
            var="enable_night_mode",
            component="switch",
            name="Night Mode",
            value_kind="bool",
            entity_category="config",
        ),
        AuxSpec(
            var="request_identify",
            component="button",
            name="Identify",
            value_kind="bool",
            entity_category="config",
        ),
    ),
    DeviceKind.WIFI: (
        AuxSpec(
            var="association_status",
            component="binary_sensor",
            name="Wi-Fi",
            value_kind="bool",
            device_class="connectivity",
            entity_category="diagnostic",
        ),
        AuxSpec(
            var="connectivity_ping_successful",
            component="binary_sensor",
            name="Internet",
            value_kind="bool",
            device_class="connectivity",
            entity_category="diagnostic",
        ),
        AuxSpec(
            var="ntp_synced",
            component="binary_sensor",
            name="NTP Sync",
            value_kind="bool",
            entity_category="diagnostic",
            enabled_by_default=False,
        ),
    ),
}

# Aux components that accept inbound commands (carry a command_var).
_COMMANDABLE_COMPONENTS = {"switch", "number", "button"}

# Gangbox load kinds: a panel can carry several (one per gang), so their aux
# entity names are prefixed with the load name — bare "Power"/"Temperature"/
# "Fault" collide into sensor...power / sensor...power_2 entity ids in HA
# (pilot finding 2026-06-12). Singleton kinds keep the short spec names.
_LOAD_KINDS = {DeviceKind.LIGHT, DeviceKind.SWITCH, DeviceKind.ALWAYS_ON}


@dataclass(frozen=True)
class EntityDescriptor:
    """Describes a single HA entity derived from one BrilliantDevice peripheral."""

    component: str  # "light" | "switch" | "binary_sensor" | "sensor" | "number" | "button"
    unique_id: str
    name: str
    panel: str
    peripheral_id: str
    supports_brightness: bool = False
    device_class: str | None = None
    unit: str | None = None
    # Which key of the shared JSON state payload this entity reads.
    # None for the primary light/switch; "motion" / "lux" / aux key otherwise.
    value_key: str | None = None
    entity_category: str | None = None
    enabled_by_default: bool = True
    state_class: str | None = None
    # The bus variable an aux switch/number/button writes to. None for sensors,
    # binary_sensors, and the primary (JSON-payload) light/switch.
    command_var: str | None = None
    value_kind: str = "bool"
    invert: bool = False
    min_value: float | None = None
    max_value: float | None = None
    step: float | None = None


def _aux_descriptors(device: BrilliantDevice, panel: str) -> list[EntityDescriptor]:
    """Build descriptors for every AUX_SPEC variable present on *device*.

    For multi-gang load kinds (_LOAD_KINDS) the entity name is prefixed with the
    load's display name ("Lights Power"); unique_ids are unaffected.

    A variable whose current value is in the spec's ``skip_values`` ("no
    reading") yields no descriptor — mirrored by :func:`payload_fields` so the
    entity set and the payload keys stay in lockstep.
    """
    specs = AUX_SPECS.get(device.kind, ())
    base_uid = f"brilliant_{panel}_{device.peripheral_id}"
    prefix = f"{device.name} " if device.kind in _LOAD_KINDS else ""
    descriptors: list[EntityDescriptor] = []
    for spec in specs:
        var = device.variables.get(spec.var)
        if var is None or var.value in spec.skip_values:
            continue
        command_var = spec.var if spec.component in _COMMANDABLE_COMPONENTS else None
        descriptors.append(
            EntityDescriptor(
                component=spec.component,
                unique_id=f"{base_uid}_{spec.var}",
                name=f"{prefix}{spec.name}",
                panel=panel,
                peripheral_id=device.peripheral_id,
                device_class=spec.device_class,
                unit=spec.unit,
                value_key=spec.key,
                entity_category=spec.entity_category,
                enabled_by_default=spec.enabled_by_default,
                state_class=spec.state_class,
                command_var=command_var,
                value_kind=spec.value_kind,
                invert=spec.invert,
                min_value=spec.min_value,
                max_value=spec.max_value,
                step=spec.step,
            )
        )
    return descriptors


def entities_for(device: BrilliantDevice, panel: str) -> list[EntityDescriptor]:
    """Return the HA entity descriptors for *device* on *panel*.

    LIGHT / SWITCH yield their primary entity plus any present aux entities.
    BINARY_SENSOR yields the motion binary_sensor + the lux sensor (when present)
    plus any present aux entities. ALWAYS_ON / HARDWARE / UI / WIFI yield ONLY
    their aux entities. UNKNOWN and SENSOR yield [].
    """
    base_uid = f"brilliant_{panel}_{device.peripheral_id}"

    if device.kind is DeviceKind.LIGHT:
        return [
            EntityDescriptor(
                component="light",
                unique_id=base_uid,
                name=device.name,
                panel=panel,
                peripheral_id=device.peripheral_id,
                supports_brightness=device.is_dimmable,
            ),
            *_aux_descriptors(device, panel),
        ]

    if device.kind is DeviceKind.SWITCH:
        return [
            EntityDescriptor(
                component="switch",
                unique_id=base_uid,
                name=device.name,
                panel=panel,
                peripheral_id=device.peripheral_id,
            ),
            *_aux_descriptors(device, panel),
        ]

    if device.kind is DeviceKind.BINARY_SENSOR:
        descriptors: list[EntityDescriptor] = [
            EntityDescriptor(
                component="binary_sensor",
                unique_id=base_uid,
                name="Motion",
                panel=panel,
                peripheral_id=device.peripheral_id,
                device_class="motion",
                value_key="motion",
            )
        ]
        if "lux" in device.variables:
            descriptors.append(
                EntityDescriptor(
                    component="sensor",
                    unique_id=f"{base_uid}_lux",
                    name="Illuminance",
                    panel=panel,
                    peripheral_id=device.peripheral_id,
                    device_class="illuminance",
                    unit="lx",
                    value_key="lux",
                    state_class="measurement",
                )
            )
        descriptors.extend(_aux_descriptors(device, panel))
        return descriptors

    if device.kind in AUX_SPECS:
        # ALWAYS_ON / HARDWARE / UI / WIFI — aux entities only, no primary.
        return _aux_descriptors(device, panel)

    # UNKNOWN or SENSOR — deliberately not mapped to any HA entity.
    return []


def _render_aux(var: Variable, value_kind: str, invert: bool) -> object | None:
    """Render one aux variable to its JSON value per *value_kind*; None to skip."""
    if value_kind == "bool":
        return var.as_bool() != invert  # XOR: flip the boolean when invert is set
    if value_kind == "int":
        return var.as_int()
    if value_kind == "float":
        return var.as_float()
    if value_kind == "str":
        return var.value
    return None


def payload_fields(device: BrilliantDevice) -> dict[str, object]:
    """Return the shared JSON state payload fields for *device*.

    THE single source for state payloads (the bridge serialises the result).

    - LIGHT / SWITCH: {"state": ...} (+ "brightness" when dimmable) + present aux.
    - BINARY_SENSOR: {"motion": bool} (+ "lux" when present) + present aux.
    - ALWAYS_ON / HARDWARE / UI / WIFI: present aux only.
    - UNKNOWN / SENSOR: {}.
    """
    data: dict[str, object] = {}

    if device.kind in (DeviceKind.LIGHT, DeviceKind.SWITCH):
        data["state"] = "ON" if device.is_on else "OFF"
        if device.is_dimmable and device.intensity is not None:
            data["brightness"] = round(device.intensity / device.max_intensity * 255)

    elif device.kind is DeviceKind.BINARY_SENSOR:
        # motion_detected is None when absent — collapse to False so HA always
        # gets a concrete boolean and the value_template never fails.
        data["motion"] = bool(device.motion_detected)
        if device.lux is not None:
            data["lux"] = device.lux

    elif device.kind not in AUX_SPECS:
        # UNKNOWN / SENSOR — nothing to render.
        return {}

    for spec in AUX_SPECS.get(device.kind, ()):
        var = device.variables.get(spec.var)
        # skip_values gate mirrors _aux_descriptors: a gated variable renders
        # no payload key because it also minted no entity to read it.
        if var is None or var.value in spec.skip_values:
            continue
        rendered = _render_aux(var, spec.value_kind, spec.invert)
        if rendered is None:
            continue
        # Gate: a bool reading that is only valid while a sibling variable is
        # enabled collapses to a concrete False when that gate is absent or off
        # (stale subsystem). Only the VALUE is forced — the payload key is still
        # emitted, so descriptor/payload-key lockstep is preserved. (gate_var is
        # validated bool-only in AuxSpec.__post_init__.)
        if spec.gate_var is not None:
            gate = device.variables.get(spec.gate_var)
            if gate is None or not gate.as_bool():
                rendered = False
        data[spec.key] = rendered

    return data
