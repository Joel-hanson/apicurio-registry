#!/usr/bin/env bash
# Shared helpers for kafkasql-snapshot-repro scripts.
# Ensures kafkasql-snapshots is compaction-ready before Registry starts with kafka-store.
#
# Usage: source this file from another script in the same directory, then call:
#   ensure_snapshots_topic_for_kafka_store
#
# Safe to call when the topic does not exist yet (alter is skipped / warned).

SNAPSHOTS_TOPIC="${SNAPSHOTS_TOPIC:-kafkasql-snapshots}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafkasql-repro-kafka}"
KAFKA_BOOTSTRAP_INTERNAL="${KAFKA_BOOTSTRAP_INTERNAL:-localhost:9092}"

ensure_snapshots_topic_for_kafka_store() {
  if ! docker inspect "$KAFKA_CONTAINER" >/dev/null 2>&1; then
    printf '[prep] ERROR: kafka container not running: %s\n' "$KAFKA_CONTAINER" >&2
    return 1
  fi

  printf '[prep] ensuring topic %s has cleanup.policy=compact,delete (required for kafka-store) ...\n' \
    "$SNAPSHOTS_TOPIC" >&2

  # Topic may not exist yet (Registry creates it on first start with kafka-store on).
  if ! docker exec "$KAFKA_CONTAINER" \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_INTERNAL" \
    --topic "$SNAPSHOTS_TOPIC" \
    --describe >/dev/null 2>&1; then
    printf '[prep] topic %s does not exist yet — Registry will create it with compact,delete when kafka-store is enabled\n' \
      "$SNAPSHOTS_TOPIC" >&2
    return 0
  fi

  if ! docker exec "$KAFKA_CONTAINER" \
    /opt/kafka/bin/kafka-configs.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_INTERNAL" \
    --entity-type topics \
    --entity-name "$SNAPSHOTS_TOPIC" \
    --alter \
    --add-config 'cleanup.policy=compact,delete,delete.retention.ms=86400000'; then
    printf '[prep] ERROR: failed to alter %s for kafka-store compaction\n' "$SNAPSHOTS_TOPIC" >&2
    return 1
  fi

  local described
  described="$(docker exec "$KAFKA_CONTAINER" \
    /opt/kafka/bin/kafka-configs.sh \
    --bootstrap-server "$KAFKA_BOOTSTRAP_INTERNAL" \
    --entity-type topics \
    --entity-name "$SNAPSHOTS_TOPIC" \
    --describe 2>/dev/null || true)"
  if ! grep -Eq 'cleanup\.policy=[^[:space:]]*compact' <<<"$described"; then
    printf '[prep] ERROR: after alter, %s still lacks compact in cleanup.policy:\n%s\n' \
      "$SNAPSHOTS_TOPIC" "$described" >&2
    return 1
  fi

  printf '[prep] %s ready for kafka-store (compaction enabled)\n' "$SNAPSHOTS_TOPIC" >&2
  return 0
}
