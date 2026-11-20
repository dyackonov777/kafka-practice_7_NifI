#!/bin/bash
# Регистрация Avro-схемы schema/user.avsc в Schema Registry (subject student-topic-value)
# и вывод curl /subjects и /subjects/<subject>/versions -> evidence/
source "$(dirname "$0")/common.sh"

content_type="Content-Type: application/vnd.schemaregistry.v1+json"

# .avsc -> тело запроса {"schema": "..."}.
# Кириллица из "doc" экранируется в \uXXXX (ensure_ascii), а тело передаётся
# через stdin, а не аргументом: в Windows аргументы перекодируются в cp1251,
# и Schema Registry отвечает "Invalid UTF-8 middle byte".
echo "== Регистрация схемы в subject '$SCHEMA_SUBJECT'"
"$PY" -c '
import json, sys
with open(sys.argv[1], encoding="utf-8-sig") as f:
    schema = json.load(f)
sys.stdout.write(json.dumps({"schema": json.dumps(schema, ensure_ascii=True)}, ensure_ascii=True))
' "$SCHEMA_FILE" | curl -sS -X POST "$SR_URL/subjects/$SCHEMA_SUBJECT/versions" \
  -H "$content_type" --data-binary @-
echo

echo "== Режим совместимости BACKWARD"
curl -sS -X PUT "$SR_URL/config/$SCHEMA_SUBJECT" -H "$content_type" \
  -d '{"compatibility": "BACKWARD"}'
echo

echo "== curl $SR_URL/subjects"
curl -sS "$SR_URL/subjects" | tee "$EVIDENCE_DIR/subjects.txt"
echo

echo "== curl -X GET $SR_URL/subjects/$SCHEMA_SUBJECT/versions"
curl -sS -X GET "$SR_URL/subjects/$SCHEMA_SUBJECT/versions" | tee "$EVIDENCE_DIR/subject-versions.txt"
echo

echo "== curl $SR_URL/subjects/$SCHEMA_SUBJECT/versions/latest"
curl -sS "$SR_URL/subjects/$SCHEMA_SUBJECT/versions/latest" | tee "$EVIDENCE_DIR/subject-latest.txt"
echo
