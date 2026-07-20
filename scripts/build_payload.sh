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
# Resolve the installed versions from the frozen project environment, then pin
# the wheel download so rebuilding a committed payload cannot float on PyPI.
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
DBUS_NEXT_REQUIREMENT="dbus-next==$DBUS_NEXT_VERSION"
DBUS_NEXT_URL="https://files.pythonhosted.org/packages/d2/fc/c0a3f4c4eaa5a22fbef91713474666e13d0ea2a69c84532579490a9f2cc8/dbus_next-0.2.3-py3-none-any.whl"
DBUS_NEXT_SHA256="58948f9aff9db08316734c0be2a120f6dc502124d9642f55e90ac82ffb16a18b"
if [[ "$DBUS_NEXT_REQUIREMENT" != "dbus-next==0.2.3" ]]; then
  echo "uv.lock dbus-next version no longer matches build provenance" >&2
  exit 1
fi
uv run --frozen --with pip python -m pip download \
  "aiomqtt==$AIOMQTT_VERSION" "paho-mqtt==$PAHO_MQTT_VERSION" \
  "typing-extensions==$TYPING_EXTENSIONS_VERSION" \
  --no-deps --python-version 3.10 --only-binary=:all: -d "$WHEELS" >/dev/null
for whl in "$WHEELS"/*.whl; do unzip -qo "$whl" -d "$DEST/vendor"; done
rm -rf "$DEST"/vendor/*.dist-info

# dbus-next is pure Python and private to the observer component. Resolve the exact
# version from uv.lock's frozen environment and download without transitive deps.
uv run --frozen --with pip python -m pip download \
  "$DBUS_NEXT_URL" \
  --no-deps --python-version 3.10 --only-binary=:all: -d "$DBUS_WHEELS" >/dev/null
DBUS_NEXT_WHEEL="$DBUS_WHEELS/dbus_next-0.2.3-py3-none-any.whl"
uv run --frozen python "$ROOT/scripts/verify_sha256.py" \
  "$DBUS_NEXT_WHEEL" "$DBUS_NEXT_SHA256"

DBUS_LICENSES="$DEST/ble_observer/vendor-licenses"
mkdir -p "$DBUS_LICENSES"
unzip -p "$DBUS_NEXT_WHEEL" 'dbus_next-0.2.3.dist-info/LICENSE' \
  > "$DBUS_LICENSES/dbus-next-LICENSE"
printf '%s\n' \
  'Name: dbus-next' \
  'Version: 0.2.3' \
  'Lock-Source: uv.lock' \
  "Wheel-URL: $DBUS_NEXT_URL" \
  "Wheel-SHA256: $DBUS_NEXT_SHA256" \
  'License: dbus-next-LICENSE' \
  > "$DBUS_LICENSES/dbus-next-PROVENANCE.txt"
unzip -qo "$DBUS_NEXT_WHEEL" -d "$DEST/ble_observer/vendor"

# Generated payloads never retain interpreter/build caches or wheel metadata.
find "$DEST" -name __pycache__ -type d -prune -exec rm -rf {} +
find "$DEST" -type f \( -name '*.pyc' -o -name '*.pyo' \) -delete
find "$DEST" -name '*.dist-info' -type d -prune -exec rm -rf {} +

VERSION="$(uv run --frozen python -c 'import brilliant_mqtt; print(brilliant_mqtt.__version__)')"
printf '%s' "$VERSION" > "$DEST/VERSION"
echo "payload built: $DEST (agent $VERSION)"
