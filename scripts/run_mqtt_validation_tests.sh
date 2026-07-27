#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

readonly MOSQUITTO_IMAGE="eclipse-mosquitto:2.0.22"
readonly DEVICE_USERNAME="brilliant_device"
readonly DEVICE_PASSWORD="brilliant-device-live-test-only"
readonly HA_USERNAME="homeassistant"
readonly HA_PASSWORD="homeassistant-live-test-only"

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
readonly script_dir
repo_root="$(cd -- "${script_dir}/.." && pwd -P)"
readonly repo_root
fixture_dir="${repo_root}/tests/mqtt_broker"
readonly fixture_dir

runtime_base="${TMPDIR:-/tmp}"
runtime_base="${runtime_base%/}"
if [[ "${runtime_base}" != /* || ! -d "${runtime_base}" || "${runtime_base}" == "/" ]]; then
    echo "Refusing unsafe temporary-directory base: ${runtime_base}" >&2
    exit 2
fi
readonly runtime_base

suffix="$(openssl rand -hex 8)"
readonly suffix
if [[ ! "${suffix}" =~ ^[0-9a-f]{16}$ ]]; then
    echo "Could not generate a safe container-name suffix" >&2
    exit 2
fi

readonly password_name="brilliant-mqtt-live-${suffix}-passwd"
readonly plain_name="brilliant-mqtt-live-${suffix}-plain"
readonly tls_name="brilliant-mqtt-live-${suffix}-tls"
readonly deny_name="brilliant-mqtt-live-${suffix}-deny"
readonly mismatch_name="brilliant-mqtt-live-${suffix}-mismatch"
created_containers=()
created_container_ids=()
runtime_dir=""
pending_container_name=""
pending_cid_file=""
create_in_progress=0
deferred_signal_status=0

valid_container_name() {
    local exact_name="$1"
    [[ "${exact_name}" =~ ^brilliant-mqtt-live-[0-9a-f]{16}-(passwd|plain|tls|deny|mismatch)$ ]]
}

valid_runtime_dir() {
    [[ -n "${runtime_dir}" &&
        -d "${runtime_dir}" &&
        "${runtime_dir}" == "${runtime_base}"/brilliant-mqtt-live.* ]]
}

container_is_owned() {
    local exact_name="$1"
    local expected_id="$2"
    local actual_id
    if ! valid_container_name "${exact_name}" ||
        [[ ! "${expected_id}" =~ ^[0-9a-f]{64}$ ]]; then
        return 1
    fi
    actual_id="$(
        docker inspect --format '{{.Id}}' "${exact_name}" 2>/dev/null
    )" || return 1
    [[ "${actual_id}" == "${expected_id}" ]]
}

pending_container_id() {
    local container_id
    if ! valid_container_name "${pending_container_name}" ||
        [[ "${pending_cid_file}" != "${runtime_dir}"/brilliant-mqtt-live-*.cid ]] ||
        [[ ! -f "${pending_cid_file}" || -L "${pending_cid_file}" ]]; then
        return 1
    fi
    container_id="$(<"${pending_cid_file}")"
    if ! container_is_owned "${pending_container_name}" "${container_id}"; then
        return 1
    fi
    printf '%s\n' "${container_id}"
}

cleanup() {
    local status=$?
    local index
    local exact_name
    local expected_id
    local removed_pending=""
    trap - EXIT
    trap '' INT TERM HUP
    set +e
    if expected_id="$(pending_container_id)"; then
        docker rm -f -v -- "${pending_container_name}" >/dev/null 2>&1
        removed_pending="${pending_container_name}"
    fi
    for ((index = ${#created_containers[@]} - 1; index >= 0; index--)); do
        exact_name="${created_containers[index]}"
        expected_id="${created_container_ids[index]}"
        if [[ "${exact_name}" != "${removed_pending}" ]] &&
            container_is_owned "${exact_name}" "${expected_id}"; then
            docker rm -f -v -- "${exact_name}" >/dev/null 2>&1
        fi
    done
    if valid_runtime_dir; then
        rm -rf -- "${runtime_dir}"
    fi
    exit "${status}"
}

handle_signal() {
    local signal_status="$1"
    if ((create_in_progress)); then
        if ((deferred_signal_status == 0)); then
            deferred_signal_status="${signal_status}"
        fi
        return
    fi
    exit "${signal_status}"
}

trap cleanup EXIT
trap 'handle_signal 130' INT
trap 'handle_signal 143' TERM
trap 'handle_signal 129' HUP

runtime_dir="$(mktemp -d "${runtime_base}/brilliant-mqtt-live.XXXXXXXX")"
if [[ ! -d "${runtime_dir}" || "${runtime_dir}" != "${runtime_base}"/brilliant-mqtt-live.* ]]; then
    echo "mktemp returned an unexpected path" >&2
    exit 2
fi

create_container() {
    local exact_name="$1"
    local create_status
    local created_id
    shift
    if ! valid_container_name "${exact_name}"; then
        echo "Refusing unsafe container name" >&2
        return 2
    fi
    pending_container_name="${exact_name}"
    pending_cid_file="${runtime_dir}/${exact_name}.cid"
    if [[ -e "${pending_cid_file}" || -L "${pending_cid_file}" ]]; then
        echo "Refusing an existing container ownership record" >&2
        return 2
    fi
    create_in_progress=1
    if (
        trap '' INT TERM HUP
        docker create \
            --cidfile "${pending_cid_file}" \
            --name "${exact_name}" \
            "$@" >/dev/null
    ); then
        create_status=0
    else
        create_status=$?
    fi
    if ((create_status != 0)); then
        create_in_progress=0
        if ((deferred_signal_status != 0)); then
            exit "${deferred_signal_status}"
        fi
        return "${create_status}"
    fi
    if ! created_id="$(pending_container_id)"; then
        create_in_progress=0
        echo "Could not validate created container ownership" >&2
        if ((deferred_signal_status != 0)); then
            exit "${deferred_signal_status}"
        fi
        return 1
    fi
    created_containers+=("${exact_name}")
    created_container_ids+=("${created_id}")
    pending_container_name=""
    pending_cid_file=""
    create_in_progress=0
    if ((deferred_signal_status != 0)); then
        exit "${deferred_signal_status}"
    fi
}

container_exit_code() {
    local exact_name="$1"
    docker inspect --format '{{.State.ExitCode}}' "${exact_name}"
}

published_port() {
    local exact_name="$1"
    local container_port="$2"
    local port
    port="$(
        docker inspect \
            --format "{{(index (index .NetworkSettings.Ports \"${container_port}/tcp\") 0).HostPort}}" \
            "${exact_name}"
    )"
    if [[ ! "${port}" =~ ^[0-9]+$ ]] || ((port < 1 || port > 65535)); then
        echo "Container ${exact_name} has no valid published port" >&2
        return 1
    fi
    printf '%s\n' "${port}"
}

wait_for_broker() {
    local exact_name="$1"
    local container_port="$2"
    local use_tls="$3"
    local deadline=$((SECONDS + 30))
    local attempt
    local -a tls_args=()
    if [[ "${use_tls}" == "yes" ]]; then
        tls_args=(--cafile /mosquitto/config/ca.crt)
    fi
    for ((attempt = 0; attempt < 120 && SECONDS < deadline; attempt++)); do
        if docker exec "${exact_name}" mosquitto_pub \
            -h localhost \
            -p "${container_port}" \
            -u "${DEVICE_USERNAME}" \
            -P "${DEVICE_PASSWORD}" \
            -t "brilliant/live-readiness" \
            -m "ready" \
            -q 1 \
            "${tls_args[@]}" >/dev/null 2>&1; then
            return 0
        fi
        if ((attempt < 119 && SECONDS < deadline)); then
            sleep 0.25
        fi
    done
    echo "Broker ${exact_name} did not become ready within 30 seconds" >&2
    docker logs "${exact_name}" >&2
    return 1
}

start_plain_broker() {
    local exact_name="$1"
    local acl_path="$2"
    create_container \
        "${exact_name}" \
        --user "$(id -u):$(id -g)" \
        --mount "type=bind,source=${runtime_dir}/plain.conf,target=/mosquitto/config/mosquitto.conf,readonly" \
        --mount "type=bind,source=${runtime_dir}/password-output/passwords,target=/mosquitto/config/passwords,readonly" \
        --mount "type=bind,source=${acl_path},target=/mosquitto/config/acl,readonly" \
        --publish "127.0.0.1::1883" \
        "${MOSQUITTO_IMAGE}"
    docker start "${exact_name}" >/dev/null
    wait_for_broker "${exact_name}" 1883 no
}

cp -- "${fixture_dir}/mosquitto.conf" "${runtime_dir}/plain.conf"
cp -- "${fixture_dir}/acl" "${runtime_dir}/acl"
cp -- "${fixture_dir}/acl-deny-discovery" "${runtime_dir}/acl-deny-discovery"
mkdir -- "${runtime_dir}/password-output"

sed 's/^listener 1883$/listener 8883/' \
    "${fixture_dir}/mosquitto.conf" >"${runtime_dir}/tls.conf"
printf '%s\n' \
    'cafile /mosquitto/config/ca.crt' \
    'certfile /mosquitto/config/server.crt' \
    'keyfile /mosquitto/config/server.key' \
    'require_certificate false' \
    'tls_version tlsv1.2' >>"${runtime_dir}/tls.conf"

printf '%s\n' \
    '[server_ext]' \
    'basicConstraints=critical,CA:FALSE' \
    'keyUsage=critical,digitalSignature,keyEncipherment' \
    'extendedKeyUsage=serverAuth' \
    'subjectAltName=DNS:localhost,IP:127.0.0.1' >"${runtime_dir}/server.ext"

openssl req \
    -x509 \
    -newkey rsa:2048 \
    -sha256 \
    -days 1 \
    -nodes \
    -keyout "${runtime_dir}/ca.key" \
    -out "${runtime_dir}/ca.crt" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -subj "/CN=Brilliant MQTT live test CA" >/dev/null 2>&1
openssl req \
    -new \
    -newkey rsa:2048 \
    -sha256 \
    -nodes \
    -keyout "${runtime_dir}/server.key" \
    -out "${runtime_dir}/server.csr" \
    -subj "/CN=localhost" >/dev/null 2>&1
openssl x509 \
    -req \
    -in "${runtime_dir}/server.csr" \
    -CA "${runtime_dir}/ca.crt" \
    -CAkey "${runtime_dir}/ca.key" \
    -CAcreateserial \
    -out "${runtime_dir}/server.crt" \
    -days 1 \
    -sha256 \
    -extfile "${runtime_dir}/server.ext" \
    -extensions server_ext >/dev/null 2>&1

create_container \
    "${password_name}" \
    --user "$(id -u):$(id -g)" \
    --entrypoint /bin/sh \
    --mount "type=bind,source=${runtime_dir}/password-output,target=/work" \
    "${MOSQUITTO_IMAGE}" \
    -ec \
    'mosquitto_passwd -b -c /work/passwords "$1" "$2"
mosquitto_passwd -b /work/passwords "$3" "$4"' \
    _ \
    "${DEVICE_USERNAME}" \
    "${DEVICE_PASSWORD}" \
    "${HA_USERNAME}" \
    "${HA_PASSWORD}"
docker start -a "${password_name}" >/dev/null
if [[ "$(container_exit_code "${password_name}")" != "0" ||
    ! -s "${runtime_dir}/password-output/passwords" ]]; then
    echo "Could not generate the disposable password database" >&2
    exit 1
fi

start_plain_broker "${plain_name}" "${runtime_dir}/acl"
start_plain_broker "${deny_name}" "${runtime_dir}/acl-deny-discovery"
start_plain_broker "${mismatch_name}" "${runtime_dir}/acl"

create_container \
    "${tls_name}" \
    --user "$(id -u):$(id -g)" \
    --mount "type=bind,source=${runtime_dir}/tls.conf,target=/mosquitto/config/mosquitto.conf,readonly" \
    --mount "type=bind,source=${runtime_dir}/password-output/passwords,target=/mosquitto/config/passwords,readonly" \
    --mount "type=bind,source=${runtime_dir}/acl,target=/mosquitto/config/acl,readonly" \
    --mount "type=bind,source=${runtime_dir}/ca.crt,target=/mosquitto/config/ca.crt,readonly" \
    --mount "type=bind,source=${runtime_dir}/server.crt,target=/mosquitto/config/server.crt,readonly" \
    --mount "type=bind,source=${runtime_dir}/server.key,target=/mosquitto/config/server.key,readonly" \
    --publish "127.0.0.1::8883" \
    "${MOSQUITTO_IMAGE}"
docker start "${tls_name}" >/dev/null
wait_for_broker "${tls_name}" 8883 yes

plain_port="$(published_port "${plain_name}" 1883)"
tls_port="$(published_port "${tls_name}" 8883)"
deny_port="$(published_port "${deny_name}" 1883)"
mismatch_port="$(published_port "${mismatch_name}" 1883)"
readonly plain_port tls_port deny_port mismatch_port

export BRILLIANT_MQTT_TEST_BROKER_URL="mqtt://127.0.0.1:${plain_port}"
export BRILLIANT_MQTT_TEST_TLS_BROKER_URL="mqtts://localhost:${tls_port}"
export BRILLIANT_MQTT_TEST_DENY_BROKER_URL="mqtt://127.0.0.1:${deny_port}"
export BRILLIANT_MQTT_TEST_MISMATCH_BROKER_URL="mqtt://127.0.0.1:${mismatch_port}"
export BRILLIANT_MQTT_TEST_CA_FILE="${runtime_dir}/ca.crt"
export BRILLIANT_MQTT_TEST_HA_USERNAME="${HA_USERNAME}"
export BRILLIANT_MQTT_TEST_HA_PASSWORD="${HA_PASSWORD}"
export BRILLIANT_MQTT_TEST_DEVICE_USERNAME="${DEVICE_USERNAME}"
export BRILLIANT_MQTT_TEST_DEVICE_PASSWORD="${DEVICE_PASSWORD}"

cd -- "${repo_root}"
set +e
uv run --project ha pytest -c ha/pyproject.toml \
    ha/tests/test_broker_validation_live.py -m mqtt_live -q
test_status=$?
set -e
exit "${test_status}"
