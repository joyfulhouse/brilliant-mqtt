#!/usr/bin/env bash
# Assemble the voice payload the HA integration bundles for on-panel deployment.
# Fetch-only; NO compilation (panel is armv7 Cortex-A9; everything is prebuilt).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# ── pinned artifact identifiers ───────────────────────────────────────────────
PY_TAG="20260610"
PY_ASSET="cpython-3.11.15+20260610-armv7-unknown-linux-gnueabihf-install_only_stripped.tar.gz"
PY_URL="https://github.com/astral-sh/python-build-standalone/releases/download/${PY_TAG}/${PY_ASSET}"

LVA_REF="$(cat "${ROOT}/deploy/voice/LVA_REF")"

DEB_OPENBLAS="https://deb.debian.org/debian/pool/main/o/openblas/libopenblas0-pthread_0.3.13+ds-3+deb11u1_armhf.deb"
DEB_OPENBLAS_SHA256="d45763bf67b70baa567247beb73c754d092dd1400b716c662e0ea92e8e0b6371"

DEB_GFORTRAN="https://deb.debian.org/debian/pool/main/g/gcc-10/libgfortran5_10.2.1-6_armhf.deb"
DEB_GFORTRAN_SHA256="5aeff120a11bee91544f409d35a8236bd490e513735cf58f0ceb8362da69712d"

DEB_LIBSTDCXX="https://ports.ubuntu.com/ubuntu-ports/pool/main/g/gcc-12/libstdc++6_12.3.0-1ubuntu1~22.04.3_armhf.deb"
DEB_LIBSTDCXX_SHA256="f9901b20640ebd7f6f75c528d67d2f4e4c58c8ecfb39877a4f48cbc3db96cf2c"

PKGS="aioesphomeapi==45.3.1 netifaces2==0.0.22 numpy>=2,<3 pymicro-wakeword>=2,<3 pyopen-wakeword>=1,<2 zeroconf<1 getmac<1"

# ── staging paths ─────────────────────────────────────────────────────────────
DEST="${ROOT}/custom_components/brilliant_mqtt/voice_payload/build/brilliant-voice"
PAYLOAD_DIR="${ROOT}/custom_components/brilliant_mqtt/voice_payload"

VOICE_VERSION="$(uv run python -c 'import brilliant_voice; print(brilliant_voice.__version__)')"
TARBALL="${PAYLOAD_DIR}/brilliant-voice-payload-${VOICE_VERSION}.tar.gz"

TMP="$(mktemp -d)"
WHEELS="$(mktemp -d)"
trap 'rm -rf "$TMP" "$WHEELS"' EXIT

rm -rf "$DEST"
mkdir -p \
  "$DEST/python" \
  "$DEST/site" \
  "$DEST/libs" \
  "$DEST/lva" \
  "$DEST/app/brilliant_voice" \
  "$DEST/aec"

# ── 1. Python 3.11 stripped interpreter ──────────────────────────────────────
echo "==> [1/6] Fetching Python 3.11 (armv7hf stripped)…"
curl -fsSL "${PY_URL}" | tar -xz -C "$TMP"
# tarball extracts to a python/ dir; place it as $DEST/python
cp -R "$TMP/python/." "$DEST/python/"

# ── 2. LVA py3.11 deps → site/ ───────────────────────────────────────────────
echo "==> [2/6] Downloading armv7 cp311 wheels…"
# shellcheck disable=SC2086
uv run --with pip python -m pip download $PKGS \
  --only-binary=:all: --python-version 3.11 --implementation cp --abi cp311 \
  --platform manylinux2014_armv7l --platform manylinux_2_17_armv7l \
  --platform manylinux_2_31_armv7l --platform linux_armv7l \
  --extra-index-url https://www.piwheels.org/simple -d "$WHEELS" >/dev/null
for whl in "$WHEELS"/*.whl; do unzip -qo "$whl" -d "$DEST/site"; done
rm -rf "$DEST"/site/*.dist-info

# ── 3. Native libs → libs/ ───────────────────────────────────────────────────
echo "==> [3/6] Extracting native libs from .deb packages…"

verify_deb() {
  local path="$1" expected="$2" name
  name="$(basename "$path")"
  local actual
  actual="$(sha256sum "$path" | cut -d' ' -f1)"
  if [ "$actual" != "$expected" ]; then
    echo "ERROR: sha256 mismatch for ${name}" >&2
    echo "  expected: ${expected}" >&2
    echo "  actual:   ${actual}" >&2
    exit 1
  fi
}

extract_deb() {
  local url="$1" expected_sha256="$2"
  local name
  name="$(basename "$url")"
  local deb_dir="$TMP/deb_${name%.deb}"
  mkdir -p "$deb_dir"
  curl -fsSL "$url" -o "$deb_dir/${name}"
  verify_deb "$deb_dir/${name}" "$expected_sha256"
  # cd into the deb's own dir so we don't need GNU-ar's --output flag (BSD/macOS ar lacks it).
  (cd "$deb_dir" && ar x "$name")
  # data archive may be .tar.xz, .tar.gz, or .tar.zst
  local data_tar
  data_tar="$(find "$deb_dir" -maxdepth 1 -name 'data.tar.*' | head -1)"
  tar -xf "$data_tar" -C "$deb_dir"
  # Collect .so files; use a temp list to avoid pipe-to-while masking cp failures.
  local so_list="$TMP/so_list_${name%.deb}.txt"
  find "$deb_dir" -name '*.so*' ! -type d > "$so_list"
  while IFS= read -r so; do
    cp -n "$so" "$DEST/libs/"
  done < "$so_list"
}

extract_deb "${DEB_OPENBLAS}" "${DEB_OPENBLAS_SHA256}"
extract_deb "${DEB_GFORTRAN}" "${DEB_GFORTRAN_SHA256}"
extract_deb "${DEB_LIBSTDCXX}" "${DEB_LIBSTDCXX_SHA256}"

# SONAME symlinks (match what LVA's numpy + tflite wake runtime dlopen via LD_LIBRARY_PATH)
ln -sf libopenblas_armv6p-r0.3.13.so "$DEST/libs/libopenblas.so.0"
test -e "$DEST/libs/libopenblas.so.0"  || { echo "ERROR: dangling symlink libopenblas.so.0" >&2; exit 1; }
ln -sf libgfortran.so.5.0.0           "$DEST/libs/libgfortran.so.5"
test -e "$DEST/libs/libgfortran.so.5"  || { echo "ERROR: dangling symlink libgfortran.so.5" >&2; exit 1; }
ln -sf libstdc++.so.6.0.30            "$DEST/libs/libstdc++.so.6"
test -e "$DEST/libs/libstdc++.so.6"    || { echo "ERROR: dangling symlink libstdc++.so.6" >&2; exit 1; }

# ── 4. LVA fork → lva/ ───────────────────────────────────────────────────────
echo "==> [4/6] Cloning linux-voice-assistant @ ${LVA_REF}…"
git clone --depth 1 https://github.com/OHF-Voice/linux-voice-assistant "$TMP/lva"
git -C "$TMP/lva" fetch --depth 1 origin "${LVA_REF}"
git -C "$TMP/lva" checkout "${LVA_REF}"

# upstream package
cp -R "$TMP/lva/linux_voice_assistant" "$DEST/lva/linux_voice_assistant"

# our overlay replaces/adds 5 files over the upstream package
cp -R "${ROOT}/deploy/voice/lva-overlay/linux_voice_assistant/." \
       "$DEST/lva/linux_voice_assistant/"

# bundled wake models + notification sounds that LVA needs at runtime
[ -d "$TMP/lva/wakewords" ] && cp -R "$TMP/lva/wakewords" "$DEST/lva/wakewords"
[ -d "$TMP/lva/sounds"    ] && cp -R "$TMP/lva/sounds"    "$DEST/lva/sounds"

# ── 5. Our code + metadata ────────────────────────────────────────────────────
echo "==> [5/6] Copying brilliant_voice supervisor + AEC daemon + service unit…"
cp -R "${ROOT}/src/brilliant_voice/." "$DEST/app/brilliant_voice/"
find "$DEST/app" -name __pycache__ -type d -prune -exec rm -rf {} +
cp "${ROOT}/deploy/voice/aec_daemon.py"       "$DEST/aec/aec_daemon.py"
cp "${ROOT}/deploy/brilliant-voice.service"   "$DEST/brilliant-voice.service"
printf '%s' "${VOICE_VERSION}" > "$DEST/VOICE_VERSION"

# ── 6. Tarball ────────────────────────────────────────────────────────────────
echo "==> [6/6] Packing tarball…"
tar czf "${TARBALL}" -C "$(dirname "$DEST")" "$(basename "$DEST")"

SIZE="$(du -h "${TARBALL}" | cut -f1)"
echo "voice payload built: ${TARBALL} (${SIZE}, voice ${VOICE_VERSION})"
