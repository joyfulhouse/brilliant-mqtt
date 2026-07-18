#!/usr/bin/env bash
# Assemble the agent payload the HA integration bundles (deploy/update/repair source).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEST="$ROOT/custom_components/brilliant_mqtt/agent_payload"
WHEELS="$(mktemp -d)"
trap 'rm -rf "$WHEELS"' EXIT

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
uv run --frozen --with pip python -m pip download \
  "aiomqtt==$AIOMQTT_VERSION" "paho-mqtt==$PAHO_MQTT_VERSION" \
  "typing-extensions==$TYPING_EXTENSIONS_VERSION" \
  --no-deps --python-version 3.10 --only-binary=:all: -d "$WHEELS" >/dev/null
for whl in "$WHEELS"/*.whl; do unzip -qo "$whl" -d "$DEST/vendor"; done
rm -rf "$DEST"/vendor/*.dist-info

VERSION="$(uv run --frozen python -c 'import brilliant_mqtt; print(brilliant_mqtt.__version__)')"
printf '%s' "$VERSION" > "$DEST/VERSION"
echo "payload built: $DEST (agent $VERSION)"
