"""Создание и запуск потока NiFi через REST API (задание 2).

Поток (Process Group "kafka-integration"):

    ConsumeKafkaRecord_2_6 (student-topic, Avro + Schema Registry -> JSON)
        │ success
        ├──> UpdateAttribute (filename=users-....json) ──> PutFile (/opt/nifi/output)
        ├──> LogAttribute (payload -> docker logs nifi)
        └──> PublishKafkaRecord_2_6 (nifi-users-json, JSON, acks=all)

    Ошибки: parse.failure -> PutFile (/opt/nifi/output/failed);
            failure у PutFile и PublishKafkaRecord -> LogAttribute (уровень ERROR)

Логин/пароль NiFi берутся из .env (NIFI_USERNAME / NIFI_PASSWORD).

Команды:
    python nifi/setup_flow.py setup [--recreate]   # создать, включить сервисы, запустить
    python nifi/setup_flow.py status                # счётчики по процессорам и очередям
    python nifi/setup_flow.py export                # сохранить nifi/flow-definition.json
"""
import argparse
import json
import logging
import os
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import requests
import urllib3

ROOT_DIR = Path(__file__).resolve().parent.parent
EXPORT_PATH = ROOT_DIR / "nifi" / "flow-definition.json"

NIFI_API = os.getenv("NIFI_API", "https://localhost:8443/nifi-api")
PG_NAME = "kafka-integration"
KAFKA_BOOTSTRAP = "kafka1:29092,kafka2:29092,kafka3:29092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
SOURCE_TOPIC = "student-topic"
TARGET_TOPIC = "nifi-users-json"
OUTPUT_DIR = "/opt/nifi/output"

LOGIN_TIMEOUT_S = 300
STATE_TIMEOUT_S = 60
POLL_INTERVAL_S = 2

# Типы компонентов NiFi
CONFLUENT_SCHEMA_REGISTRY = "org.apache.nifi.confluent.schemaregistry.ConfluentSchemaRegistry"
AVRO_READER = "org.apache.nifi.avro.AvroReader"
JSON_READER = "org.apache.nifi.json.JsonTreeReader"
JSON_WRITER = "org.apache.nifi.json.JsonRecordSetWriter"
CONSUME_KAFKA_RECORD = "org.apache.nifi.processors.kafka.pubsub.ConsumeKafkaRecord_2_6"
PUBLISH_KAFKA_RECORD = "org.apache.nifi.processors.kafka.pubsub.PublishKafkaRecord_2_6"
UPDATE_ATTRIBUTE = "org.apache.nifi.processors.attributes.UpdateAttribute"
PUT_FILE = "org.apache.nifi.processors.standard.PutFile"
LOG_ATTRIBUTE = "org.apache.nifi.processors.standard.LogAttribute"

log = logging.getLogger("nifi-setup")


class NiFiError(RuntimeError):
    pass


def load_dotenv(path: Path) -> None:
    """Минимальный загрузчик .env без внешних зависимостей; переменные окружения важнее."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def wait_until(check: Callable[[], bool], what: str, timeout_s: float = STATE_TIMEOUT_S) -> None:
    deadline = time.monotonic() + timeout_s
    while not check():
        if time.monotonic() > deadline:
            raise NiFiError(f"Timeout waiting for {what}")
        time.sleep(POLL_INTERVAL_S)


class NiFiClient:
    """Тонкая обёртка над NiFi REST API."""

    def __init__(self, api_url: str):
        self.api_url = api_url
        self.session = requests.Session()
        self.session.verify = False  # у NiFi в Docker самоподписанный сертификат
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # --- базовые запросы -----------------------------------------------------
    def login(self, username: str, password: str) -> None:
        """Получает JWT-токен; ждёт, пока NiFi стартует (это занимает 1-2 минуты)."""
        deadline = time.monotonic() + LOGIN_TIMEOUT_S
        while True:
            try:
                r = self.session.post(f"{self.api_url}/access/token", timeout=10,
                                      data={"username": username, "password": password})
                if r.status_code in (200, 201):
                    self.session.headers["Authorization"] = f"Bearer {r.text}"
                    log.info("Logged in to NiFi")
                    return
                reason = f"HTTP {r.status_code}: {r.text[:100]}"
            except requests.RequestException as e:
                reason = type(e).__name__
            if time.monotonic() > deadline:
                raise NiFiError(f"NiFi is not available: {reason}")
            log.info("Waiting for NiFi (%s)", reason)
            time.sleep(10)

    def request(self, method: str, path: str, **kwargs) -> Any:
        r = self.session.request(method, f"{self.api_url}{path}", timeout=30, **kwargs)
        if r.status_code >= 400:
            raise NiFiError(f"{method} {path} -> HTTP {r.status_code}: {r.text}")
        return r.json() if "json" in r.headers.get("Content-Type", "") else r.text

    # --- чтение --------------------------------------------------------------
    def root_group_id(self) -> str:
        return self.request("GET", "/process-groups/root")["id"]

    def group_flow(self, group_id: str) -> dict:
        return self.request("GET", f"/flow/process-groups/{group_id}")["processGroupFlow"]["flow"]

    def find_group(self, name: str) -> dict | None:
        groups = self.group_flow(self.root_group_id())["processGroups"]
        return next((g for g in groups if g["component"]["name"] == name), None)

    def controller_services(self, group_id: str) -> list[dict]:
        return self.request(
            "GET", f"/flow/process-groups/{group_id}/controller-services")["controllerServices"]

    def processors(self, group_id: str) -> list[dict]:
        return self.request("GET", f"/process-groups/{group_id}/processors")["processors"]

    def download_flow_definition(self, group_id: str) -> dict:
        definition = self.request("GET", f"/process-groups/{group_id}/download")
        return json.loads(definition) if isinstance(definition, str) else definition

    # --- создание ------------------------------------------------------------
    def create_group(self, name: str) -> str:
        body = {"revision": {"version": 0},
                "component": {"name": name, "position": {"x": 0, "y": 0}}}
        group_id = self.request(
            "POST", f"/process-groups/{self.root_group_id()}/process-groups", json=body)["id"]
        log.info("Process group '%s' created (%s)", name, group_id)
        return group_id

    def create_service(self, group_id: str, type_: str, name: str, properties: dict) -> str:
        body = {"revision": {"version": 0},
                "component": {"type": type_, "name": name, "properties": properties}}
        service_id = self.request(
            "POST", f"/process-groups/{group_id}/controller-services", json=body)["id"]
        log.info("Controller service '%s' created", name)
        return service_id

    def create_processor(self, group_id: str, type_: str, name: str, position: tuple[int, int],
                         properties: dict, auto_terminate: list[str]) -> str:
        body = {
            "revision": {"version": 0},
            "component": {
                "type": type_,
                "name": name,
                "position": {"x": position[0], "y": position[1]},
                "config": {
                    "properties": properties,
                    "autoTerminatedRelationships": auto_terminate,
                    "schedulingPeriod": "1 sec",
                },
            },
        }
        processor_id = self.request(
            "POST", f"/process-groups/{group_id}/processors", json=body)["id"]
        log.info("Processor '%s' created", name)
        return processor_id

    def connect(self, group_id: str, source_id: str, target_id: str,
                relationships: list[str]) -> None:
        body = {
            "revision": {"version": 0},
            "component": {
                "source": {"id": source_id, "groupId": group_id, "type": "PROCESSOR"},
                "destination": {"id": target_id, "groupId": group_id, "type": "PROCESSOR"},
                "selectedRelationships": relationships,
            },
        }
        self.request("POST", f"/process-groups/{group_id}/connections", json=body)

    # --- управление состоянием -----------------------------------------------
    def set_services_state(self, group_id: str, state: str) -> None:
        self.request("PUT", f"/flow/process-groups/{group_id}/controller-services",
                     json={"id": group_id, "state": state})
        try:
            wait_until(lambda: all(s["component"]["state"] == state
                                   for s in self.controller_services(group_id)),
                       f"controller services to become {state}")
        except NiFiError:
            for s in self.controller_services(group_id):
                c = s["component"]
                log.error("  %s: %s %s", c["name"], c["state"], c.get("validationErrors", ""))
            raise
        log.info("Controller services %s", state)

    def wait_processors_valid(self, group_id: str) -> None:
        try:
            wait_until(lambda: all(p["component"].get("validationStatus") == "VALID"
                                   for p in self.processors(group_id)),
                       "processors to become VALID")
        except NiFiError:
            for p in self.processors(group_id):
                c = p["component"]
                if c.get("validationStatus") != "VALID":
                    log.error("  %s: %s", c["name"], c.get("validationErrors"))
            raise
        log.info("All processors are VALID")

    def set_group_state(self, group_id: str, state: str) -> None:
        self.request("PUT", f"/flow/process-groups/{group_id}",
                     json={"id": group_id, "state": state})
        log.info("Process group %s", state)

    def empty_queues(self, group_id: str) -> None:
        path = f"/process-groups/{group_id}/empty-all-connections-requests"
        request_id = self.request("POST", path)["dropRequest"]["id"]
        wait_until(lambda: self.request("GET", f"{path}/{request_id}")["dropRequest"]["finished"],
                   "queues to be emptied")
        self.request("DELETE", f"{path}/{request_id}")

    def delete_group(self, group_id: str) -> None:
        self.set_group_state(group_id, "STOPPED")
        self.set_services_state(group_id, "DISABLED")
        self.empty_queues(group_id)
        version = self.request("GET", f"/process-groups/{group_id}")["revision"]["version"]
        self.request("DELETE", f"/process-groups/{group_id}", params={"version": version})
        log.info("Process group deleted")


def put_file_properties(directory: str) -> dict:
    return {
        "Directory": directory,
        "Conflict Resolution Strategy": "replace",
        "Create Missing Directories": "true",
    }


def build_flow(nifi: NiFiClient, group_id: str) -> None:
    # --- controller services ---
    schema_registry = nifi.create_service(
        group_id, CONFLUENT_SCHEMA_REGISTRY, "ConfluentSchemaRegistry",
        {"url": SCHEMA_REGISTRY_URL})
    avro_reader = nifi.create_service(
        group_id, AVRO_READER, "AvroReader (Confluent)",
        {"schema-access-strategy": "confluent-encoded", "schema-registry": schema_registry})
    json_reader = nifi.create_service(
        group_id, JSON_READER, "JsonTreeReader", {"schema-access-strategy": "infer-schema"})
    json_writer_files = nifi.create_service(
        group_id, JSON_WRITER, "JsonWriter (files)",
        {"Pretty Print JSON": "true", "output-grouping": "output-array"})
    json_writer_kafka = nifi.create_service(
        group_id, JSON_WRITER, "JsonWriter (kafka)", {"output-grouping": "output-oneline"})

    # --- основной поток ---
    consume = nifi.create_processor(
        group_id, CONSUME_KAFKA_RECORD, f"ConsumeKafkaRecord {SOURCE_TOPIC}", (0, 0),
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "topic": SOURCE_TOPIC,
            "topic_type": "names",
            "group.id": "nifi-consumer-group",
            "auto.offset.reset": "earliest",
            "record-reader": avro_reader,
            "record-writer": json_writer_files,
        },
        auto_terminate=[])
    set_filename = nifi.create_processor(
        group_id, UPDATE_ATTRIBUTE, "UpdateAttribute filename", (-450, 300),
        {"filename": "users-${now():format('yyyyMMdd-HHmmss-SSS')}-${kafka.partition}.json"},
        auto_terminate=[])
    put_file = nifi.create_processor(
        group_id, PUT_FILE, f"PutFile {OUTPUT_DIR}", (-450, 600),
        put_file_properties(OUTPUT_DIR), auto_terminate=["success"])
    log_records = nifi.create_processor(
        group_id, LOG_ATTRIBUTE, "LogAttribute", (0, 300),
        {"Log Level": "info", "Log Payload": "true"}, auto_terminate=["success"])
    publish = nifi.create_processor(
        group_id, PUBLISH_KAFKA_RECORD, f"PublishKafkaRecord {TARGET_TOPIC}", (450, 300),
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "topic": TARGET_TOPIC,
            "record-reader": json_reader,
            "record-writer": json_writer_kafka,
            "acks": "all",
            "use-transactions": "false",
        },
        auto_terminate=["success"])

    # --- обработка ошибок: ничего не отбрасывается молча ---
    put_failed = nifi.create_processor(
        group_id, PUT_FILE, "PutFile parse failures", (450, -300),
        put_file_properties(f"{OUTPUT_DIR}/failed"), auto_terminate=["success", "failure"])
    log_errors = nifi.create_processor(
        group_id, LOG_ATTRIBUTE, "LogAttribute errors", (0, 600),
        {"Log Level": "error", "Log Payload": "true"}, auto_terminate=["success"])

    connections = [
        (consume, set_filename, "success"),
        (set_filename, put_file, "success"),
        (consume, log_records, "success"),
        (consume, publish, "success"),
        (consume, put_failed, "parse.failure"),  # не-Avro / неизвестная схема
        (put_file, log_errors, "failure"),        # не удалось записать файл
        (publish, log_errors, "failure"),         # не удалось отправить в Kafka
    ]
    for source, target, relationship in connections:
        nifi.connect(group_id, source, target, [relationship])
    log.info("Connections created: %d", len(connections))


def require_group(nifi: NiFiClient) -> dict:
    group = nifi.find_group(PG_NAME)
    if group is None:
        raise NiFiError(f"Process group '{PG_NAME}' not found, run 'setup' first")
    return group


def cmd_setup(nifi: NiFiClient, recreate: bool) -> None:
    existing = nifi.find_group(PG_NAME)
    if existing:
        if not recreate:
            raise NiFiError(f"Process group '{PG_NAME}' already exists, use 'setup --recreate'")
        nifi.delete_group(existing["id"])

    group_id = nifi.create_group(PG_NAME)
    build_flow(nifi, group_id)
    nifi.set_services_state(group_id, "ENABLED")
    nifi.wait_processors_valid(group_id)
    nifi.set_group_state(group_id, "RUNNING")
    cmd_export(nifi)
    log.info("Flow is running. UI: https://localhost:8443/nifi")


def cmd_status(nifi: NiFiClient) -> None:
    flow = nifi.group_flow(require_group(nifi)["id"])
    print(f"{'Processor':40} {'State':10} {'In (5 min)':>18} {'Out (5 min)':>18}")
    for p in flow["processors"]:
        snap = p["status"]["aggregateSnapshot"]
        print(f"{p['component']['name']:40} {p['component']['state']:10} "
              f"{snap['input']:>18} {snap['output']:>18}")
    print()
    for c in flow["connections"]:
        snap = c["status"]["aggregateSnapshot"]
        print(f"Queue {snap['sourceName']} -> {snap['destinationName']}: {snap['queued']}")


def cmd_export(nifi: NiFiClient) -> None:
    definition = nifi.download_flow_definition(require_group(nifi)["id"])
    EXPORT_PATH.write_text(json.dumps(definition, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info("Flow definition exported to %s", EXPORT_PATH.relative_to(ROOT_DIR))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manage NiFi Kafka integration flow")
    commands = parser.add_subparsers(dest="command", required=True)
    setup = commands.add_parser("setup", help="создать и запустить поток")
    setup.add_argument("--recreate", action="store_true", help="пересоздать существующий поток")
    commands.add_parser("status", help="счётчики по процессорам и очередям")
    commands.add_parser("export", help="сохранить flow definition в nifi/flow-definition.json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    load_dotenv(ROOT_DIR / ".env")
    username, password = os.getenv("NIFI_USERNAME"), os.getenv("NIFI_PASSWORD")
    if not username or not password:
        log.error("NIFI_USERNAME / NIFI_PASSWORD are not set. Run: cp .env.example .env")
        return 1

    nifi = NiFiClient(NIFI_API)
    try:
        nifi.login(username, password)
        if args.command == "setup":
            cmd_setup(nifi, args.recreate)
        elif args.command == "status":
            cmd_status(nifi)
        elif args.command == "export":
            cmd_export(nifi)
    except NiFiError as e:
        log.error("%s", e)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
