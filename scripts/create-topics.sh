#!/usr/bin/env bash
# Creates the Kafka topics the system needs, idempotently.

set -euo pipefail

: "${KAFKA_BROKER:?KAFKA_BROKER must be set (e.g. kafka:9092)}"
: "${ORDERS_TOPIC:?ORDERS_TOPIC must be set}"
: "${DELTAS_TOPIC:?DELTAS_TOPIC must be set}"
: "${TOPIC_PARTITIONS:?TOPIC_PARTITIONS must be set}"
: "${TOPIC_REPLICATION:?TOPIC_REPLICATION must be set}"

KAFKA_TOPICS_BIN="${KAFKA_TOPICS_BIN:-/opt/kafka/bin/kafka-topics.sh}"
KAFKA_CONFIGS_BIN="${KAFKA_CONFIGS_BIN:-/opt/kafka/bin/kafka-configs.sh}"

# Healthcheck passing doesn't guarantee the listener is reachable from
# this container yet, so retry rather than fail on first attempt.
MAX_ATTEMPTS="${MAX_ATTEMPTS:-30}"
RETRY_INTERVAL_SECONDS="${RETRY_INTERVAL_SECONDS:-2}"

echo "Waiting for Kafka broker at ${KAFKA_BROKER} to accept connections..."
attempt=1
until "${KAFKA_TOPICS_BIN}" --bootstrap-server "${KAFKA_BROKER}" --list >/dev/null 2>&1; do
  if (( attempt >= MAX_ATTEMPTS )); then
    echo "ERROR: Kafka broker at ${KAFKA_BROKER} did not become reachable" \
      "after ${MAX_ATTEMPTS} attempts (${RETRY_INTERVAL_SECONDS}s apart)." >&2
    exit 1
  fi
  echo "  attempt ${attempt}/${MAX_ATTEMPTS}: not yet reachable, retrying in ${RETRY_INTERVAL_SECONDS}s..."
  sleep "${RETRY_INTERVAL_SECONDS}"
  attempt=$((attempt + 1))
done
echo "Broker is reachable."

# Partition count caps engine parallelism (one thread per partition) and
# raising it later remaps key -> partition for symbols already in flight.
create_topic() {
  local topic="$1"
  echo "Creating topic '${topic}' (partitions=${TOPIC_PARTITIONS}, replication=${TOPIC_REPLICATION}) if it doesn't exist..."
  "${KAFKA_TOPICS_BIN}" --create --if-not-exists \
    --bootstrap-server "${KAFKA_BROKER}" \
    --topic "${topic}" \
    --partitions "${TOPIC_PARTITIONS}" \
    --replication-factor "${TOPIC_REPLICATION}"
}

create_topic "${ORDERS_TOPIC}"
create_topic "${DELTAS_TOPIC}"

# --create --config is skipped when the topic already exists, so retention
# must be set separately via --alter to stay idempotent across restarts.
echo "Setting retention.ms=-1 on '${ORDERS_TOPIC}'..."
"${KAFKA_CONFIGS_BIN}" --alter \
  --bootstrap-server "${KAFKA_BROKER}" \
  --entity-type topics --entity-name "${ORDERS_TOPIC}" \
  --add-config retention.ms=-1

echo "Topics ready:"
"${KAFKA_TOPICS_BIN}" --bootstrap-server "${KAFKA_BROKER}" --list
