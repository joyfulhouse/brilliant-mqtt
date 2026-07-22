#!/usr/bin/env bash
# Assemble the agent payload the HA integration bundles (deploy/update/repair source).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEST="$ROOT/custom_components/brilliant_mqtt/agent_payload"
WHEELS="$(mktemp -d)"
DBUS_WHEELS="$(mktemp -d)"
trap 'rm -rf "$WHEELS" "$DBUS_WHEELS"' EXIT

rm -rf "$DEST"
mkdir -p "$DEST/app" "$DEST/vendor"
cp -R "$ROOT/src/brilliant_mqtt" "$DEST/app/brilliant_mqtt"
find "$DEST/app" -name __pycache__ -type d -prune -exec rm -rf {} +
find "$DEST/app" -type f \( -name '*.pyc' -o -name '*.pyo' \) -delete

# Keep the HA mirror compatibility payload only until Task 12 live validation.
rm -rf "$DEST/ha_mirror"
mkdir -p "$DEST/ha_mirror"
cp -R "$ROOT/src/brilliant_ha_mirror" "$DEST/ha_mirror/brilliant_ha_mirror"
find "$DEST/ha_mirror" -name __pycache__ -type d -prune -exec rm -rf {} +
find "$DEST/ha_mirror" -type f \( -name '*.pyc' -o -name '*.pyo' \) -delete

cp "$ROOT/deploy/brilliant-mqtt.service" "$DEST/brilliant-mqtt.service"
cp "$ROOT/deploy/brilliant-ha-mirror.service" "$DEST/brilliant-ha-mirror.service"
cp "$ROOT/deploy/brilliant-wifi-watchdog.service" "$DEST/brilliant-wifi-watchdog.service"
cp "$ROOT/deploy/brilliant-bus-watchdog.service" "$DEST/brilliant-bus-watchdog.service"
cp "$ROOT/deploy/brilliant-hue-ca.service" "$DEST/brilliant-hue-ca.service"
cp "$ROOT/deploy/brilliant-hue-ca.timer" "$DEST/brilliant-hue-ca.timer"
cp "$ROOT/deploy/brilliant-ble-observer.service" "$DEST/brilliant-ble-observer.service"

# Bundle the stdlib Wi-Fi watchdog (no vendored deps) into the integration payload.
WD_SRC="$ROOT/src/brilliant_wifi_watchdog"
WD_DST="$DEST/wifi_watchdog/brilliant_wifi_watchdog"
rm -rf "$(dirname "$WD_DST")"
mkdir -p "$WD_DST"
cp "$WD_SRC"/*.py "$WD_DST"/

# Bundle the stdlib bus-health watchdog (no vendored deps) into the integration payload.
BUSWD_DST="$DEST/bus_watchdog/brilliant_bus_watchdog"
rm -rf "$(dirname "$BUSWD_DST")"
mkdir -p "$BUSWD_DST"
cp "$ROOT/src/brilliant_bus_watchdog"/*.py "$BUSWD_DST"/

# hue-ca CA-recovery oneshot
HUECA_DST="$DEST/hue_ca/brilliant_hue_ca"
mkdir -p "$HUECA_DST"
cp "$ROOT/src/brilliant_hue_ca"/*.py "$HUECA_DST"/

# Bundle the passive observer separately so enabling it never depends on replacing
# the main bridge payload already installed on a panel.
BLE_OBSERVER_DST="$DEST/ble_observer/brilliant_ble_observer"
mkdir -p "$BLE_OBSERVER_DST" "$DEST/ble_observer/vendor"
cp "$ROOT/src/brilliant_ble_observer"/*.py "$BLE_OBSERVER_DST"/

# Vendored pure-python MQTT deps for the panel's py3.10 (the panel has no pip).
# Resolve versions from the frozen environment and exact wheel records directly
# from uv.lock so rebuilding a committed payload cannot float on PyPI.
AIOMQTT_VERSION="$(
  uv run --frozen python -c 'import importlib.metadata as m; print(m.version("aiomqtt"))'
)"
PAHO_MQTT_VERSION="$(
  uv run --frozen python -c 'import importlib.metadata as m; print(m.version("paho-mqtt"))'
)"
TYPING_EXTENSIONS_VERSION="$(
  uv run --frozen python -c 'import importlib.metadata as m; print(m.version("typing-extensions"))'
)"
DBUS_NEXT_VERSION="$(
  uv run --frozen python -c 'import importlib.metadata as m; print(m.version("dbus-next"))'
)"

resolve_locked_wheel() {
  local package_name="$1"
  local package_version="$2"
  uv run --frozen python "$ROOT/scripts/locked_wheel.py" \
    "$ROOT/uv.lock" "$package_name" "$package_version"
}

AIOMQTT_LOCK_RECORD="$(
  resolve_locked_wheel aiomqtt "$AIOMQTT_VERSION"
)"
IFS=$'\t' read -r AIOMQTT_URL AIOMQTT_SHA256 AIOMQTT_FILENAME <<< "$AIOMQTT_LOCK_RECORD"
PAHO_MQTT_LOCK_RECORD="$(
  resolve_locked_wheel paho-mqtt "$PAHO_MQTT_VERSION"
)"
IFS=$'\t' read -r PAHO_MQTT_URL PAHO_MQTT_SHA256 PAHO_MQTT_FILENAME \
  <<< "$PAHO_MQTT_LOCK_RECORD"
TYPING_EXTENSIONS_LOCK_RECORD="$(
  resolve_locked_wheel typing-extensions "$TYPING_EXTENSIONS_VERSION"
)"
IFS=$'\t' read -r TYPING_EXTENSIONS_URL TYPING_EXTENSIONS_SHA256 \
  TYPING_EXTENSIONS_FILENAME <<< "$TYPING_EXTENSIONS_LOCK_RECORD"
DBUS_NEXT_LOCK_RECORD="$(
  resolve_locked_wheel dbus-next "$DBUS_NEXT_VERSION"
)"
IFS=$'\t' read -r DBUS_NEXT_URL DBUS_NEXT_SHA256 DBUS_NEXT_FILENAME \
  <<< "$DBUS_NEXT_LOCK_RECORD"

download_verified_wheel() {
  local wheel_url="$1"
  local wheel_sha256="$2"
  local wheel_filename="$3"
  local wheel_dir="$4"
  uv run --frozen --with pip python -m pip download \
    "$wheel_url" \
    --no-deps --python-version 3.10 --only-binary=:all: -d "$wheel_dir" >/dev/null
  uv run --frozen python "$ROOT/scripts/verify_sha256.py" \
    "$wheel_dir/$wheel_filename" "$wheel_sha256"
}

write_vendor_provenance() {
  local licenses_dir="$1"
  local package_name="$2"
  local package_version="$3"
  local wheel_url="$4"
  local wheel_sha256="$5"
  local license_filename="$6"
  printf '%s\n' \
    "Name: $package_name" \
    "Version: $package_version" \
    'Lock-Source: uv.lock' \
    "Wheel-URL: $wheel_url" \
    "Wheel-SHA256: $wheel_sha256" \
    "License: $license_filename" \
    > "$licenses_dir/${package_name}-PROVENANCE.txt"
}

download_verified_wheel \
  "$AIOMQTT_URL" "$AIOMQTT_SHA256" "$AIOMQTT_FILENAME" "$WHEELS"
download_verified_wheel \
  "$PAHO_MQTT_URL" "$PAHO_MQTT_SHA256" "$PAHO_MQTT_FILENAME" "$WHEELS"
download_verified_wheel \
  "$TYPING_EXTENSIONS_URL" "$TYPING_EXTENSIONS_SHA256" \
  "$TYPING_EXTENSIONS_FILENAME" "$WHEELS"

AIOMQTT_WHEEL="$WHEELS/$AIOMQTT_FILENAME"
PAHO_MQTT_WHEEL="$WHEELS/$PAHO_MQTT_FILENAME"
TYPING_EXTENSIONS_WHEEL="$WHEELS/$TYPING_EXTENSIONS_FILENAME"
VENDOR_LICENSES="$DEST/vendor-licenses"
mkdir -p "$VENDOR_LICENSES"
unzip -p "$AIOMQTT_WHEEL" \
  "aiomqtt-${AIOMQTT_VERSION}.dist-info/licenses/LICENSE" \
  > "$VENDOR_LICENSES/aiomqtt-LICENSE"
unzip -p "$PAHO_MQTT_WHEEL" \
  "paho_mqtt-${PAHO_MQTT_VERSION}.dist-info/licenses/LICENSE.txt" \
  > "$VENDOR_LICENSES/paho-mqtt-LICENSE"
unzip -p "$TYPING_EXTENSIONS_WHEEL" \
  "typing_extensions-${TYPING_EXTENSIONS_VERSION}.dist-info/licenses/LICENSE" \
  > "$VENDOR_LICENSES/typing-extensions-LICENSE"
write_vendor_provenance \
  "$VENDOR_LICENSES" aiomqtt "$AIOMQTT_VERSION" "$AIOMQTT_URL" \
  "$AIOMQTT_SHA256" aiomqtt-LICENSE
write_vendor_provenance \
  "$VENDOR_LICENSES" paho-mqtt "$PAHO_MQTT_VERSION" "$PAHO_MQTT_URL" \
  "$PAHO_MQTT_SHA256" paho-mqtt-LICENSE
write_vendor_provenance \
  "$VENDOR_LICENSES" typing-extensions "$TYPING_EXTENSIONS_VERSION" \
  "$TYPING_EXTENSIONS_URL" "$TYPING_EXTENSIONS_SHA256" typing-extensions-LICENSE

for whl in "$WHEELS"/*.whl; do unzip -qo "$whl" -d "$DEST/vendor"; done
rm -rf "$DEST"/vendor/*.dist-info

# dbus-next is pure Python and private to the observer component. Resolve the exact
# version from uv.lock's frozen environment and download without transitive deps.
download_verified_wheel \
  "$DBUS_NEXT_URL" "$DBUS_NEXT_SHA256" "$DBUS_NEXT_FILENAME" "$DBUS_WHEELS"
DBUS_NEXT_WHEEL="$DBUS_WHEELS/$DBUS_NEXT_FILENAME"

DBUS_LICENSES="$DEST/ble_observer/vendor-licenses"
mkdir -p "$DBUS_LICENSES"
unzip -p "$DBUS_NEXT_WHEEL" "dbus_next-${DBUS_NEXT_VERSION}.dist-info/LICENSE" \
  > "$DBUS_LICENSES/dbus-next-LICENSE"
write_vendor_provenance \
  "$DBUS_LICENSES" dbus-next "$DBUS_NEXT_VERSION" "$DBUS_NEXT_URL" \
  "$DBUS_NEXT_SHA256" dbus-next-LICENSE
unzip -qo "$DBUS_NEXT_WHEEL" -d "$DEST/ble_observer/vendor"

# Generated payloads never retain interpreter/build caches or wheel metadata.
find "$DEST" -name __pycache__ -type d -prune -exec rm -rf {} +
find "$DEST" -type f \( -name '*.pyc' -o -name '*.pyo' \) -delete
find "$DEST" -name '*.dist-info' -type d -prune -exec rm -rf {} +

VERSION="$(uv run --frozen python -c 'import brilliant_mqtt; print(brilliant_mqtt.__version__)')"
printf '%s' "$VERSION" > "$DEST/VERSION"
echo "payload built: $DEST (agent $VERSION)"
