#!/usr/bin/env bash

set -euo pipefail

usage() {
  echo "usage: $0 [--dry-run] <release> <panel-host>" >&2
}

dry_run=false
if [[ ${1:-} == "--dry-run" ]]; then
  dry_run=true
  shift
fi

if [[ $# -ne 2 ]]; then
  usage
  exit 2
fi

release=$1
panel_host=$2
if [[ ! $release =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "release must contain only letters, digits, dots, underscores, or hyphens" >&2
  exit 2
fi

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
destination_rel="artifacts/brilliant-panel/$release/raw/pilot-corpus.tar.zst"
destination="$repo_root/$destination_rel"
partial="$destination.partial"
sources=(
  data/switch-ui
  data/switch-embedded
  var
  etc/systemd/system/message_bus.service
  etc/systemd/system/switch_ui_app.service
  etc/systemd/system/update_manager.service
  etc/default/update_manager
  usr/sbin/update_manager
)

if $dry_run; then
  printf 'Source: root@%s:/\n' "$panel_host"
  printf 'Paths: %s\n' "${sources[*]}"
  printf 'Destination: %s\n' "$destination_rel"
  exit 0
fi

if [[ -z ${SSHPASS:-} ]]; then
  echo "SSHPASS must be set" >&2
  exit 2
fi

for command in git sshpass ssh zstd; do
  if ! command -v "$command" >/dev/null 2>&1; then
    echo "required command not found: $command" >&2
    exit 2
  fi
done

mkdir -p "$(dirname "$destination")"
if ! git -C "$repo_root" check-ignore -q "$destination_rel"; then
  echo "refusing acquisition: $destination_rel is not gitignored" >&2
  exit 2
fi

cleanup() {
  rm -f "$partial"
}
trap cleanup EXIT

set -o pipefail
sshpass -e ssh -n \
  -o PreferredAuthentications=password \
  -o PubkeyAuthentication=no \
  -o StrictHostKeyChecking=no \
  -o ConnectTimeout=15 \
  -o NumberOfPasswordPrompts=1 \
  -o LogLevel=ERROR \
  "root@$panel_host" \
  "tar -C / -cf - ${sources[*]}" \
  | zstd -T0 -6 -f -o "$partial"

mv -f "$partial" "$destination"
trap - EXIT

if command -v sha256sum >/dev/null 2>&1; then
  sha256sum "$destination" >"$destination.sha256"
else
  shasum -a 256 "$destination" >"$destination.sha256"
fi

echo "Acquired $destination_rel"
cat "$destination.sha256"
