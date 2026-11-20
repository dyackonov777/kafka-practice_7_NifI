#!/bin/bash
# Вывод kafka-topics --describe и конфигурации топика -> evidence/topic-describe.txt
source "$(dirname "$0")/common.sh"

{
  for topic in "$SOURCE_TOPIC" "$TARGET_TOPIC"; do
    echo "### kafka-topics --describe --topic $topic"
    kafka_cli kafka-topics --bootstrap-server "$BOOTSTRAP_INTERNAL" --describe --topic "$topic"
    echo
  done
  echo "### kafka-configs --describe --topic $SOURCE_TOPIC"
  kafka_cli kafka-configs --bootstrap-server "$BOOTSTRAP_INTERNAL" \
    --entity-type topics --entity-name "$SOURCE_TOPIC" --describe
} | tee "$EVIDENCE_DIR/topic-describe.txt"
