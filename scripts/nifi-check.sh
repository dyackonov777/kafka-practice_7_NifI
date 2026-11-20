#!/bin/bash
# Подтверждение работы NiFi: статус потока, логи, записанные файлы -> evidence/
source "$(dirname "$0")/common.sh"

output_dir="nifi/output"

echo "### Статус потока NiFi"
$PY nifi/setup_flow.py status | tee "$EVIDENCE_DIR/nifi-status.txt"

echo
echo "### Логи NiFi (LogAttribute / Kafka-процессоры)"
docker logs nifi 2>&1 \
  | grep -E "LogAttribute|ConsumeKafkaRecord|PublishKafkaRecord|PutFile|kafka\.topic|\"id\"" \
  | tail -n 80 | tee "$EVIDENCE_DIR/nifi-logs.txt"

echo
echo "### Файлы, записанные PutFile ($output_dir)"
ls -la "$output_dir" | tee "$EVIDENCE_DIR/nifi-output-ls.txt"
latest_file=$(ls -t "$output_dir"/*.json 2>/dev/null | head -n 1 || true)
if [ -n "$latest_file" ]; then
  echo "--- $latest_file"
  tee "$EVIDENCE_DIR/nifi-output-sample.json" < "$latest_file"
fi

echo
echo "### Битые сообщения (parse.failure -> $output_dir/failed)"
ls -la "$output_dir/failed" 2>/dev/null || echo "нет — все сообщения разобраны успешно"
