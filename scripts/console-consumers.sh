#!/bin/bash
# Чтение топиков консольными консьюмерами -> evidence/
# Использование: scripts/console-consumers.sh [max-messages]   (по умолчанию 10)
source "$(dirname "$0")/common.sh"

max_messages="${1:-10}"

echo "### kafka-avro-console-consumer: $SOURCE_TOPIC (Avro, схема из Schema Registry)"
docker exec schema-registry kafka-avro-console-consumer \
  --bootstrap-server "$BOOTSTRAP_INTERNAL" \
  --topic "$SOURCE_TOPIC" \
  --from-beginning \
  --max-messages "$max_messages" \
  --skip-message-on-error \
  --property schema.registry.url=http://localhost:8081 \
  | tee "$EVIDENCE_DIR/console-$SOURCE_TOPIC.txt"

echo
echo "### kafka-console-consumer: $TARGET_TOPIC (данные, переданные NiFi)"
kafka_cli kafka-console-consumer \
  --bootstrap-server "$BOOTSTRAP_INTERNAL" \
  --topic "$TARGET_TOPIC" \
  --from-beginning \
  --max-messages "$max_messages" \
  --property print.partition=true \
  --property print.offset=true \
  | tee "$EVIDENCE_DIR/console-$TARGET_TOPIC.txt"
