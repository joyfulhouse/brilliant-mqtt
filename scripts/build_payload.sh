#!/usr/bin/env bash
# Assemble the agent payload the HA integration bundles (deploy/update/repair source).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEST="$ROOT/custom_components/brilliant_mqtt/agent_payload"
WHEELS="$(mktemp -d)"

rm -rf "$DEST"
mkdir -p "$DEST/app" "$DEST/vendor"
cp -R "$ROOT/src/brilliant_mqtt" "$DEST/app/brilliant_mqtt"
find "$DEST/app" -name __pycache__ -type d -prune -exec rm -rf {} +
cp "$ROOT/deploy/brilliant-mqtt.service" "$DEST/brilliant-mqtt.service"
cp "$ROOT/deploy/brilliant-wifi-watchdog.service" "$DEST/brilliant-wifi-watchdog.service"

# Bundle the stdlib Wi-Fi watchdog (no vendored deps) into the integration payload.
WD_SRC="$ROOT/src/brilliant_wifi_watchdog"
WD_DST="$DEST/wifi_watchdog/brilliant_wifi_watchdog"
rm -rf "$(dirname "$WD_DST")"
mkdir -p "$WD_DST"
cp "$WD_SRC"/*.py "$WD_DST"/

# Vendored pure-python MQTT deps for the panel's py3.10 (the panel has no pip).
uv run --with pip python -m pip download aiomqtt paho-mqtt \
  --python-version 3.10 --only-binary=:all: -d "$WHEELS" >/dev/null
for whl in "$WHEELS"/*.whl; do unzip -qo "$whl" -d "$DEST/vendor"; done
rm -rf "$DEST"/vendor/*.dist-info

VERSION="$(uv run python -c 'import brilliant_mqtt; print(brilliant_mqtt.__version__)')"
printf '%s' "$VERSION" > "$DEST/VERSION"
echo "payload built: $DEST (agent $VERSION)"
