#!/bin/bash
# Применение параметров из configs/topic-configs.txt к существующему топику
# Использование: scripts/configure-topic.sh [topic]   (по умолчанию student-topic)
source "$(dirname "$0")/common.sh"

topic="${1:-$SOURCE_TOPIC}"
configs="$(topic_configs | paste -sd, -)"

kafka_cli kafka-configs \
  --bootstrap-server "$BOOTSTRAP_INTERNAL" \
  --entity-type topics --entity-name "$topic" \
  --alter --add-config "$configs"

kafka_cli kafka-configs \
  --bootstrap-server "$BOOTSTRAP_INTERNAL" \
  --entity-type topics --entity-name "$topic" \
  --describe
