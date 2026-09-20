#!/usr/bin/env bash
# Proves Kafka mode and startup recovery against the live compose stack.
# Not run in CI - requires kafka/redis containers up.

set -euo pipefail

log() {
    echo "[replay-check] $*"
}

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:19092}"
REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
REDIS_PORT="${REDIS_PORT:-6379}"
ORDERS_TOPIC="${ORDERS_TOPIC:-orders}"
DELTAS_TOPIC="${DELTAS_TOPIC:-deltas}"
RUN_ID="$$-$(date +%s)"
ENGINE_BIN="${ENGINE_BIN:-./build/engine_main}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-matching-engine-kafka}"
REDIS_CONTAINER="${REDIS_CONTAINER:-matching-engine-redis}"
ENGINE_CONTAINER="${ENGINE_CONTAINER:-matching-engine-engine}"
ENGINE_MODE="${ENGINE_MODE:-host}"
ENGINE_SERVICE="${ENGINE_SERVICE:-engine}"
ENGINE_STOP_TIMEOUT_SECONDS=60

# Container mode uses the group id compose assigns the engine, not a
# unique-per-run one, since the script does not control the container's env.
if [[ "${ENGINE_MODE}" == "host" ]]; then
    GROUP_ID="${GROUP_ID:-replay-check-${RUN_ID}}"
elif [[ "${ENGINE_MODE}" == "container" ]]; then
    command -v jq >/dev/null 2>&1 || fail "jq is required for ENGINE_MODE=container"
    GROUP_ID="$(docker compose config --format json | jq -r ".services.${ENGINE_SERVICE}.environment.KAFKA_GROUP_ID")"
    [[ -n "${GROUP_ID}" && "${GROUP_ID}" != "null" ]] || fail "could not determine KAFKA_GROUP_ID for compose service ${ENGINE_SERVICE}"
else
    fail "unknown ENGINE_MODE '${ENGINE_MODE}' (expected 'host' or 'container')"
fi

SYM_A="RPA${RUN_ID}"
SYM_B="RPB${RUN_ID}"

KAFKA_BIN=/opt/kafka/bin
WAIT_TIMEOUT_SECONDS=60

TMP_DIR="$(mktemp -d)"
ENGINE_LOG="${TMP_DIR}/engine.log"
SNAP_A_BEFORE="${TMP_DIR}/snap_a_before"
SNAP_B_BEFORE="${TMP_DIR}/snap_b_before"
SNAP_A_AFTER="${TMP_DIR}/snap_a_after"
SNAP_B_AFTER="${TMP_DIR}/snap_b_after"
DELTAS_OUT="${TMP_DIR}/deltas.ndjson"

ENGINE_PID=""
ENGINE_SERVICE_WAS_RUNNING=""

engine_service_running() {
    [[ "$(docker compose ps --status running --services)" == *"${ENGINE_SERVICE}"* ]]
}

cleanup() {
    if [[ -n "${ENGINE_PID}" ]] && kill -0 "${ENGINE_PID}" 2>/dev/null; then
        kill -TERM "${ENGINE_PID}" 2>/dev/null || true
        wait "${ENGINE_PID}" 2>/dev/null || true
    fi
    if [[ "${ENGINE_SERVICE_WAS_RUNNING}" == "1" ]] && ! engine_service_running; then
        docker compose start "${ENGINE_SERVICE}" >/dev/null 2>&1 || true
    fi
    docker exec "${REDIS_CONTAINER}" redis-cli DEL "book:${SYM_A}" "book:${SYM_B}" >/dev/null 2>&1 || true
    rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

# A running compose engine service consumes the shared orders topic
# regardless of mode, so it must be quiesced even for host-mode runs.
if engine_service_running; then
    ENGINE_SERVICE_WAS_RUNNING="1"
    if [[ "${ENGINE_MODE}" == "host" ]]; then
        log "stopping compose engine service so it does not double-consume orders"
        docker compose stop "${ENGINE_SERVICE}" >/dev/null
    fi
else
    ENGINE_SERVICE_WAS_RUNNING="0"
fi

engine_start_host() {
    KAFKA_BROKERS="${KAFKA_BOOTSTRAP}" \
    ORDERS_TOPIC="${ORDERS_TOPIC}" \
    DELTAS_TOPIC="${DELTAS_TOPIC}" \
    KAFKA_GROUP_ID="${GROUP_ID}" \
    REDIS_HOST="${REDIS_HOST}" \
    REDIS_PORT="${REDIS_PORT}" \
    "${ENGINE_BIN}" >>"${ENGINE_LOG}" 2>&1 &
    ENGINE_PID=$!
}

engine_stop_host() {
    kill -TERM "${ENGINE_PID}"
    if ! wait "${ENGINE_PID}"; then
        fail "engine exited non-zero on SIGTERM. Log:
$(engine_log_capture)"
    fi
    ENGINE_PID=""
}

engine_start_container() {
    docker compose start "${ENGINE_SERVICE}" >/dev/null
}

engine_stop_container() {
    docker compose stop "${ENGINE_SERVICE}" >/dev/null
    local waited=0
    while engine_service_running; do
        if (( waited >= ENGINE_STOP_TIMEOUT_SECONDS )); then
            fail "timed out after ${ENGINE_STOP_TIMEOUT_SECONDS}s waiting for ${ENGINE_SERVICE} to stop"
        fi
        sleep 1
        waited=$((waited + 1))
    done
    local exit_code
    exit_code="$(docker inspect -f '{{.State.ExitCode}}' "${ENGINE_CONTAINER}")"
    if [[ "${exit_code}" != "0" ]]; then
        fail "${ENGINE_SERVICE} exited non-zero (${exit_code}) on SIGTERM. Log:
$(engine_log_capture)"
    fi
    # Rejoining the same fixed group before the broker finishes processing
    # the LeaveGroup can hand out a stale assignment; wait for it to settle.
    local group_waited=0
    while [[ "$(docker exec "${KAFKA_CONTAINER}" "${KAFKA_BIN}/kafka-consumer-groups.sh" \
        --bootstrap-server localhost:9092 --describe --group "${GROUP_ID}" --state 2>/dev/null \
        | awk -v g="${GROUP_ID}" '$1==g {print $(NF-1)}')" != "Empty" ]]; do
        if (( group_waited >= ENGINE_STOP_TIMEOUT_SECONDS )); then
            fail "timed out after ${ENGINE_STOP_TIMEOUT_SECONDS}s waiting for group ${GROUP_ID} to go Empty"
        fi
        sleep 1
        group_waited=$((group_waited + 1))
    done
}

engine_start() {
    if [[ "${ENGINE_MODE}" == "host" ]]; then
        engine_start_host
    else
        engine_start_container
    fi
}

engine_stop() {
    if [[ "${ENGINE_MODE}" == "host" ]]; then
        engine_stop_host
    else
        engine_stop_container
    fi
}

engine_log_capture() {
    if [[ "${ENGINE_MODE}" == "host" ]]; then
        cat "${ENGINE_LOG}"
    else
        docker compose logs "${ENGINE_SERVICE}"
    fi
}

produce_orders() {
    local input="$1"
    docker exec -i "${KAFKA_CONTAINER}" "${KAFKA_BIN}/kafka-console-producer.sh" \
        --bootstrap-server localhost:9092 \
        --topic "${ORDERS_TOPIC}" \
        --property parse.key=true \
        --property key.separator='|' <<<"${input}"
}

wait_for_lag_zero() {
    local waited=0
    log "waiting for consumer group ${GROUP_ID} to reach lag 0 on ${ORDERS_TOPIC}..."
    while true; do
        local describe
        describe="$(docker exec "${KAFKA_CONTAINER}" "${KAFKA_BIN}/kafka-consumer-groups.sh" \
            --bootstrap-server localhost:9092 --describe --group "${GROUP_ID}" 2>/dev/null || true)"
        if [[ -n "${describe}" ]]; then
            local nonzero
            nonzero="$(awk -v topic="${ORDERS_TOPIC}" '$1==topic && $6!="0" {print}' <<<"${describe}")"
            if [[ -z "${nonzero}" ]] && grep -q "${ORDERS_TOPIC}" <<<"${describe}"; then
                log "lag is 0 on all partitions"
                return 0
            fi
        fi
        if (( waited >= WAIT_TIMEOUT_SECONDS )); then
            fail "timed out after ${WAIT_TIMEOUT_SECONDS}s waiting for lag 0 on group ${GROUP_ID}. Last describe:
${describe}"
        fi
        sleep 1
        waited=$((waited + 1))
    done
}

wait_for_redis_key() {
    local key="$1"
    local waited=0
    while [[ "$(docker exec "${REDIS_CONTAINER}" redis-cli EXISTS "${key}")" != "1" ]]; do
        if (( waited >= WAIT_TIMEOUT_SECONDS )); then
            fail "timed out after ${WAIT_TIMEOUT_SECONDS}s waiting for redis key ${key} to exist"
        fi
        sleep 1
        waited=$((waited + 1))
    done
}

save_snapshot() {
    local symbol="$1"
    local out="$2"
    docker exec "${REDIS_CONTAINER}" redis-cli GET "book:${symbol}" >"${out}"
}

if [[ "${ENGINE_MODE}" == "host" ]]; then
    [[ -x "${ENGINE_BIN}" ]] || fail "engine binary not found or not executable at ${ENGINE_BIN}"
    ENGINE_DESC="${ENGINE_BIN}"
else
    docker compose config --services | grep -qx "${ENGINE_SERVICE}" \
        || fail "compose service ${ENGINE_SERVICE} does not exist"
    ENGINE_DESC="compose service ${ENGINE_SERVICE}"
fi

docker inspect -f '{{.State.Running}}' "${KAFKA_CONTAINER}" 2>/dev/null | grep -q true \
    || fail "container ${KAFKA_CONTAINER} is not running"
docker inspect -f '{{.State.Running}}' "${REDIS_CONTAINER}" 2>/dev/null | grep -q true \
    || fail "container ${REDIS_CONTAINER} is not running"

TOPICS="$(docker exec "${KAFKA_CONTAINER}" "${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server localhost:9092 --list)"
grep -qx "${ORDERS_TOPIC}" <<<"${TOPICS}" || fail "topic ${ORDERS_TOPIC} does not exist"
grep -qx "${DELTAS_TOPIC}" <<<"${TOPICS}" || fail "topic ${DELTAS_TOPIC} does not exist"

log "preconditions ok (mode=${ENGINE_MODE}, engine=${ENGINE_DESC}, group=${GROUP_ID}, symbols=${SYM_A},${SYM_B})"

# Resting orders on both sides for each symbol, plus a partial cross, leaving
# resting orders that a later post-restart order can cross.
ORDERS_BATCH_1="$(cat <<EOF
${SYM_A}|{"type":"new","symbol":"${SYM_A}","order_id":"${SYM_A}-b1","side":"buy","price":10000,"qty":10,"ingress_ts_ns":1}
${SYM_A}|{"type":"new","symbol":"${SYM_A}","order_id":"${SYM_A}-a1","side":"sell","price":10010,"qty":10,"ingress_ts_ns":2}
${SYM_A}|{"type":"new","symbol":"${SYM_A}","order_id":"${SYM_A}-b2","side":"buy","price":10005,"qty":5,"ingress_ts_ns":3}
${SYM_A}|{"type":"new","symbol":"${SYM_A}","order_id":"${SYM_A}-a2","side":"sell","price":10005,"qty":2,"ingress_ts_ns":4}
${SYM_B}|{"type":"new","symbol":"${SYM_B}","order_id":"${SYM_B}-b1","side":"buy","price":20000,"qty":8,"ingress_ts_ns":5}
${SYM_B}|{"type":"new","symbol":"${SYM_B}","order_id":"${SYM_B}-a1","side":"sell","price":20010,"qty":8,"ingress_ts_ns":6}
${SYM_B}|{"type":"new","symbol":"${SYM_B}","order_id":"${SYM_B}-b2","side":"buy","price":20005,"qty":3,"ingress_ts_ns":7}
${SYM_B}|{"type":"new","symbol":"${SYM_B}","order_id":"${SYM_B}-a2","side":"sell","price":20005,"qty":1,"ingress_ts_ns":8}
EOF
)"

log "producing first batch of orders for ${SYM_A} and ${SYM_B}"
produce_orders "${ORDERS_BATCH_1}"

log "starting engine (first run)"
engine_start
wait_for_lag_zero
# Lag 0 confirms the orders offset committed, but the redis write happens
# just before that commit - wait for it explicitly to avoid reading early.
wait_for_redis_key "book:${SYM_A}"
wait_for_redis_key "book:${SYM_B}"

save_snapshot "${SYM_A}" "${SNAP_A_BEFORE}"
save_snapshot "${SYM_B}" "${SNAP_B_BEFORE}"
log "saved pre-restart snapshots"

log "stopping engine (first run)"
engine_stop

log "deleting redis snapshot keys so a no-op restart cannot pass"
docker exec "${REDIS_CONTAINER}" redis-cli DEL "book:${SYM_A}" "book:${SYM_B}" >/dev/null

log "starting engine (second run, should replay and rebuild)"
engine_start
wait_for_redis_key "book:${SYM_A}"
wait_for_redis_key "book:${SYM_B}"

save_snapshot "${SYM_A}" "${SNAP_A_AFTER}"
save_snapshot "${SYM_B}" "${SNAP_B_AFTER}"

if ! diff -u "${SNAP_A_BEFORE}" "${SNAP_A_AFTER}"; then
    fail "snapshot for ${SYM_A} changed after replay.
before: $(cat "${SNAP_A_BEFORE}")
after:  $(cat "${SNAP_A_AFTER}")"
fi
if ! diff -u "${SNAP_B_BEFORE}" "${SNAP_B_AFTER}"; then
    fail "snapshot for ${SYM_B} changed after replay.
before: $(cat "${SNAP_B_BEFORE}")
after:  $(cat "${SNAP_B_AFTER}")"
fi
log "post-restart snapshots are byte-identical to pre-restart snapshots"

# Cross the resting ask left on SYM_A (a1, price 10010, qty 10) created before the restart.
ORDERS_BATCH_2="$(cat <<EOF
${SYM_A}|{"type":"new","symbol":"${SYM_A}","order_id":"${SYM_A}-b3","side":"buy","price":10010,"qty":4,"ingress_ts_ns":9}
EOF
)"
log "producing crossing order for ${SYM_A} against pre-restart resting order"
produce_orders "${ORDERS_BATCH_2}"
wait_for_lag_zero

log "consuming deltas topic to check for the resulting trade"
docker exec "${KAFKA_CONTAINER}" "${KAFKA_BIN}/kafka-console-consumer.sh" \
    --bootstrap-server localhost:9092 \
    --topic "${DELTAS_TOPIC}" \
    --from-beginning \
    --timeout-ms 15000 >"${DELTAS_OUT}" 2>/dev/null || true

TRADE_LINE="$(grep "\"type\":\"trade\"" "${DELTAS_OUT}" | grep "\"symbol\":\"${SYM_A}\"" || true)"
if [[ -z "${TRADE_LINE}" ]] \
    || ! grep -q "\"price\":10010" <<<"${TRADE_LINE}" \
    || ! grep -q "\"qty\":4" <<<"${TRADE_LINE}"; then
    fail "no matching trade event found for ${SYM_A} (price 10010, qty 4) in deltas topic. Captured:
$(cat "${DELTAS_OUT}")"
fi
# Replay must be silent: if startup recovery skipped emitting for already-
# committed messages, this pre-restart trade appears exactly once ever, even
# though the engine reprocesses offset 0..N on every startup.
PRE_RESTART_TRADE_COUNT="$(grep "\"type\":\"trade\"" "${DELTAS_OUT}" \
    | grep "\"symbol\":\"${SYM_A}\"" \
    | grep -c "\"price\":10005," || true)"
if [[ "${PRE_RESTART_TRADE_COUNT}" != "1" ]]; then
    fail "expected exactly 1 trade event for ${SYM_A} price 10005 (the pre-restart cross), found ${PRE_RESTART_TRADE_COUNT} - startup recovery is re-emitting replayed messages instead of replaying them silently. Captured:
$(cat "${DELTAS_OUT}")"
fi
log "found expected trade event for ${SYM_A}, and confirmed replay did not duplicate the earlier trade"

log "stopping engine (second run)"
engine_stop

echo "PASS: kafka mode replay-check succeeded (engine_mode=${ENGINE_MODE}) for symbols ${SYM_A}, ${SYM_B}"
