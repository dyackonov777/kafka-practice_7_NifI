#!/bin/bash
# Создание топиков student-topic и nifi-users-json: 3 партиции, RF=3,
# параметры хранения — из configs/topic-configs.txt
source "$(dirname "$0")/common.sh"

config_args=()
while IFS= read -r config; do
  config_args+=(--config "$config")
done < <(topic_configs)

for topic in "$SOURCE_TOPIC" "$TARGET_TOPIC"; do
  kafka_cli kafka-topics \
    --bootstrap-server "$BOOTSTRAP_INTERNAL" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$TOPIC_PARTITIONS" \
    --replication-factor "$TOPIC_REPLICATION_FACTOR" \
    "${config_args[@]}"
done

kafka_cli kafka-topics --bootstrap-server "$BOOTSTRAP_INTERNAL" --list
