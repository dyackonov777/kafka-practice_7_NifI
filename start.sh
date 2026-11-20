#!/bin/bash
# Подъём всего стенда одной командой:
#   кластер Kafka + Schema Registry + UI + NiFi -> топики -> схема -> поток NiFi
source "$(dirname "$0")/scripts/common.sh"

if [ ! -f .env ]; then
  cp .env.example .env
  echo "[i] Создан .env из .env.example (поменяйте пароль NiFi при необходимости)"
fi

echo "== 1/5 docker compose up (ждём healthy брокеров)"
docker compose up -d --wait

echo "== 2/5 Создание топиков"
scripts/create-topics.sh

echo "== 3/5 Регистрация схемы"
scripts/register-schema.sh

echo "== 4/5 Python-зависимости"
$PY -m pip install -q -r requirements.txt

echo "== 5/5 Поток NiFi (NiFi стартует 1-2 минуты)"
$PY nifi/setup_flow.py setup --recreate

cat <<EOF

Готово.
  Kafka UI:        http://localhost:8080
  Schema Registry: $SR_URL/subjects
  NiFi:            https://localhost:8443/nifi  (логин/пароль из .env)

Дальше:
  $PY app/consumer.py --idle-timeout 20     # терминал 1
  $PY app/producer.py --count 10            # терминал 2
  scripts/describe-topic.sh && scripts/console-consumers.sh && scripts/nifi-check.sh
EOF
