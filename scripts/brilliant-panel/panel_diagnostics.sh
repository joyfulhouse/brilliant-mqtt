#!/bin/sh
# Read-only Brilliant panel architecture/runtime diagnostic (issue #155).
#
# Collects the facts needed to judge whether a panel (e.g. the aarch64 i350)
# could run brilliant-mqtt. It does NOT establish support: see
# docs/brilliant-panel/i350-support.md for the qualification gates.
#
# Usage (from a workstation; runs nothing but this script on the panel):
#   ssh -o NumberOfPasswordPrompts=1 root@<panel-ip> 'sh -s' \
#     < scripts/brilliant-panel/panel_diagnostics.sh > i350-diagnostics.txt
#
# Guarantees:
#   * read-only: writes nothing, starts/stops nothing, never connects to the bus
#     (the bus library is imported only, with bytecode writing disabled);
#   * systemctl is used only for is-enabled / is-active of message_bus.service;
#   * no credentials, keys, network identity, Wi-Fi config or certificate
#     contents are read -- the Hue CA paths are presence checks only;
#   * POSIX sh (dash / busybox ash), every child's stdin is /dev/null so the
#     script also works when piped in via `sh -s`;
#   * output: one header line, then unique key=value lines of printable ASCII.
#
# DIAG_ROOT is a root-prefix used only by the test-suite (fake panel tree).

set -u
cd / || exit 1

R=${DIAG_ROOT:-}
PANEL_PYTHON="$R/data/switch-embedded/env/bin/python3"
HUE_SITE_PACKAGES="/data/switch-embedded/env/lib/python3.10/site-packages"
export PYTHONDONTWRITEBYTECODE=1

# Emit the first line of stdin as printable ASCII, capped at 200 characters.
first_line() {
    head -n 1 | tr -cd ' -~' | cut -c 1-200
}

kv() {
    printf '%s=%s\n' "$1" "$2"
}

present() {
    if [ -e "$1" ]; then echo 1; else echo 0; fi
}

or_unknown() {
    if [ -n "$1" ]; then echo "$1"; else echo unknown; fi
}

printf '%s\n' "# brilliant-mqtt panel diagnostics (read-only) v1"

# --- timeout flavour, detected once -----------------------------------------
TIMEOUT_MODE=unbounded
if command -v timeout >/dev/null 2>&1; then
    kv timeout_present 1
    kv timeout_version "$(or_unknown "$(timeout --help </dev/null 2>&1 | first_line)")"
    if timeout 5 true </dev/null >/dev/null 2>&1; then
        TIMEOUT_MODE=plain
    elif timeout -t 5 true </dev/null >/dev/null 2>&1; then
        TIMEOUT_MODE=busybox_t
    fi
else
    kv timeout_present 0
    kv timeout_version missing
fi
kv timeout_mode "$TIMEOUT_MODE"

# bounded SECONDS CMD [ARG...] -- callers redirect stdin from /dev/null.
bounded() {
    b_secs=$1
    shift
    case $TIMEOUT_MODE in
        plain) timeout "$b_secs" "$@" ;;
        busybox_t) timeout -t "$b_secs" "$@" ;;
        *) "$@" ;;
    esac
}

# True when an exit status means a bounded child was killed for time.
timed_out() {
    [ "$TIMEOUT_MODE" != unbounded ] || return 1
    case $1 in
        124 | 137 | 143) return 0 ;;
        *) return 1 ;;
    esac
}

# --- kernel / firmware ---------------------------------------------------------
kv machine "$(or_unknown "$(uname -m </dev/null 2>/dev/null | first_line)")"
kv kernel_release "$(or_unknown "$(uname -r </dev/null 2>/dev/null | first_line)")"

if [ -x "$PANEL_PYTHON" ]; then
    kv panel_python_present 1
    kv panel_python_version \
        "$(or_unknown "$(bounded 5 "$PANEL_PYTHON" --version </dev/null 2>&1 | first_line)")"
else
    kv panel_python_present 0
    kv panel_python_version missing
fi

# read_head FILE -- the first 4096 bytes of FILE (dd where `head -c` is missing).
if head -c 1 </dev/null >/dev/null 2>&1; then
    read_head() { head -c 4096 "$1" </dev/null 2>/dev/null; }
else
    read_head() { dd if="$1" bs=4096 count=1 </dev/null 2>/dev/null; }
fi

if [ -r "$R/etc/release_info.json" ]; then
    tag=$(read_head "$R/etc/release_info.json" | tr '\n' ' ' |
        sed -n 's/.*"release_tag"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | first_line)
    kv release_tag "$(or_unknown "$tag")"
else
    kv release_tag missing
fi

# --- bus library -----------------------------------------------------------------
SP=
for candidate in "$R"/data/switch-embedded/env/lib/python3.*/site-packages; do
    if [ -d "$candidate" ]; then
        SP=$candidate
        break
    fi
done
if [ -n "$SP" ]; then
    kv site_packages_dir "$(printf '%s\n' "${SP#"$R"}" | first_line)"
else
    kv site_packages_dir missing
fi

if [ -x "$PANEL_PYTHON" ]; then
    # -I: ignore PYTHON* env, user site and the cwd; -B: write no bytecode.
    import_err=$(bounded 20 "$PANEL_PYTHON" -I -B -c \
        'from lib.message_bus_api.observer_interface import RPCObserver' \
        </dev/null 2>&1 >/dev/null)
    import_rc=$?
    if [ "$import_rc" -eq 0 ]; then
        kv bus_library_import ok
    elif timed_out "$import_rc"; then
        kv bus_library_import "failed: timed out"
    else
        last=$(printf '%s\n' "$import_err" | tail -n 1 | first_line)
        kv bus_library_import "failed: exit $import_rc: $(or_unknown "$last")"
    fi
else
    kv bus_library_import "skipped: panel python missing"
fi

# Sample one native module: prefer the bus API package itself, else any
# .so under lib/. Report its path relative to site-packages.
so_count=0
elf_sample=none
elf_class=none
elf_machine=none
elf_arch=none
if [ -n "$SP" ] && [ -d "$SP/lib" ]; then
    have_find=0
    if command -v find >/dev/null 2>&1; then have_find=1; fi
    if [ "$have_find" = 1 ]; then
        so_count=$(find "$SP/lib" -type f -name '*.so' </dev/null 2>/dev/null | wc -l |
            tr -cd '0-9')
    else
        so_count=unknown
    fi
    sample=
    for f in "$SP"/lib/message_bus_api/*.so; do
        if [ -f "$f" ]; then
            sample=$f
            break
        fi
    done
    if [ -z "$sample" ] && [ "$have_find" = 1 ]; then
        sample=$(find "$SP/lib" -type f -name '*.so' </dev/null 2>/dev/null | sort | head -n 1)
    fi
    if [ -n "$sample" ]; then
        elf_sample=$(printf '%s\n' "${sample#"$SP"/}" | first_line)
        magic=$(od -An -t x1 -N 4 "$sample" </dev/null 2>/dev/null | tr -cd '0-9a-f')
        if [ "$magic" = 7f454c46 ]; then
            elf_class=$(od -An -t u1 -j 4 -N 1 "$sample" </dev/null 2>/dev/null | tr -cd '0-9')
            elf_machine=$(od -An -t u1 -j 18 -N 2 "$sample" </dev/null 2>/dev/null |
                awk '{ print $1 + 256 * $2 }')
            case "$elf_class/$elf_machine" in
                2/183) elf_arch=aarch64 ;;
                1/40) elf_arch=arm32 ;;
                *) elf_arch=other ;;
            esac
        else
            elf_class=unknown
            elf_machine=unknown
            # Only a magic we actually read proves "not ELF"; no od means unknown.
            if [ -n "$magic" ]; then elf_arch=not_elf; else elf_arch=unknown; fi
        fi
    elif [ "$have_find" = 0 ]; then
        elf_sample=unknown
        elf_class=unknown
        elf_machine=unknown
        elf_arch=unknown
    fi
fi
kv bus_library_so_count "$(or_unknown "$so_count")"
kv bus_library_elf_sample "$(or_unknown "$elf_sample")"
kv bus_library_elf_class "$(or_unknown "$elf_class")"
kv bus_library_elf_machine "$(or_unknown "$elf_machine")"
kv bus_library_elf_arch "$elf_arch"

kv bus_socket_present "$(present "$R/var/run/brilliant/server_socket")"

if command -v systemctl >/dev/null 2>&1; then
    kv message_bus_enabled "$(or_unknown "$(bounded 5 systemctl is-enabled message_bus.service \
        </dev/null 2>/dev/null | first_line)")"
    kv message_bus_active "$(or_unknown "$(bounded 5 systemctl is-active message_bus.service \
        </dev/null 2>/dev/null | first_line)")"
else
    kv message_bus_enabled missing
    kv message_bus_active missing
fi

# --- storage -----------------------------------------------------------------------
for mp in var data; do
    mount_line=
    if [ -r "$R/proc/mounts" ]; then
        mount_line=$(awk -v m="/$mp" '$2 == m { t = $3; o = $4; f = 1 }
            END { if (f) print t " " o }' "$R/proc/mounts" </dev/null 2>/dev/null)
    fi
    if [ -n "$mount_line" ]; then
        kv "${mp}_mount_fstype" "$(printf '%s\n' "${mount_line%% *}" | first_line)"
        kv "${mp}_mount_options" "$(printf '%s\n' "${mount_line#* }" | first_line)"
    else
        kv "${mp}_mount_fstype" none
        kv "${mp}_mount_options" none
    fi
    free=
    if [ -d "$R/$mp" ]; then
        free=$(df -Pk "$R/$mp" </dev/null 2>/dev/null | awk 'NR == 2 { print $4 }' | first_line)
    fi
    kv "${mp}_free_kib" "$(or_unknown "$free")"
done

# --- Wi-Fi hardware (presence only) -------------------------------------------------
kv wlan0_present "$(present "$R/sys/class/net/wlan0")"
for tool in iw connmanctl; do
    if command -v "$tool" >/dev/null 2>&1; then kv "${tool}_present" 1; else kv "${tool}_present" 0; fi
done

drivers=
for d in "$R"/sys/bus/platform/drivers/*; do
    [ -d "$d" ] || continue
    name=${d##*/}
    case $name in
        *mmc* | *sdhci* | *msdc*) drivers="$drivers${drivers:+,}$name" ;;
    esac
done
kv mmc_drivers "$(if [ -n "$drivers" ]; then printf '%s\n' "$drivers" | first_line; else echo none; fi)"
if [ -d "$R/sys/bus/platform/drivers/sdhci-esdhc-imx" ]; then
    kv imx_sdhci_driver_present 1
else
    kv imx_sdhci_driver_present 0
fi

# --- memory / CPU ----------------------------------------------------------------------
mem=
if [ -r "$R/proc/meminfo" ]; then
    mem=$(awk '/^MemTotal:/ { v = $2 } END { if (v != "") print v }' "$R/proc/meminfo" \
        </dev/null 2>/dev/null | first_line)
fi
kv mem_total_kib "$(or_unknown "$mem")"
cpus=
if [ -r "$R/proc/cpuinfo" ]; then
    cpus=$(awk '/^processor[[:space:]]*:/ { n++ } END { print n + 0 }' "$R/proc/cpuinfo" \
        </dev/null 2>/dev/null | first_line)
fi
kv cpu_count "$(or_unknown "$cpus")"

# --- Hue CA paths used by brilliant_hue_ca (presence only, never contents) --------------
kv hue_site_packages_present "$(present "$R$HUE_SITE_PACKAGES")"
kv hue_peripherals_ini_present "$(present "$R/var/run/brilliant/processes/hue_bridge_peripherals.ini")"
kv hue_ca_bundle_present "$(present "$R$HUE_SITE_PACKAGES/lib/certs/hue-bridge-ca-certs.pem")"

exit 0
