#!/bin/bash
# Общие настройки для скриптов. Подключается через: source "$(dirname "$0")/common.sh"
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# --- Kafka (адреса внутри сети Docker) ---
KAFKA_CONTAINER="kafka1"
BOOTSTRAP_INTERNAL="kafka1:29092"
SOURCE_TOPIC="student-topic"     # задание 1: продюсер -> консьюмер / NiFi
TARGET_TOPIC="nifi-users-json"   # задание 2: выход NiFi
TOPIC_PARTITIONS=3
TOPIC_REPLICATION_FACTOR=3
TOPIC_CONFIG_FILE="configs/topic-configs.txt"

# --- Schema Registry ---
SR_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
SCHEMA_FILE="schema/user.avsc"
SCHEMA_SUBJECT="${SOURCE_TOPIC}-value"   # TopicNameStrategy

# --- Прочее ---
EVIDENCE_DIR="evidence"
mkdir -p "$EVIDENCE_DIR"

# python3 в Linux/macOS, python в Windows (Git Bash)
if python3 -c "" >/dev/null 2>&1; then PY=python3; else PY=python; fi

# Выполнить команду Kafka CLI внутри контейнера брокера
kafka_cli() {
  docker exec "$KAFKA_CONTAINER" "$@"
}

# Параметры топика из configs/topic-configs.txt (без комментариев и пустых строк)
topic_configs() {
  grep -Ev '^[[:space:]]*(#|$)' "$TOPIC_CONFIG_FILE" | tr -d '\r'
}
