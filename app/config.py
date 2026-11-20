"""Общие настройки Python-клиентов Kafka.

Значения по умолчанию совпадают с docker-compose.yml и переопределяются
переменными окружения KAFKA_BOOTSTRAP, SCHEMA_REGISTRY_URL, KAFKA_TOPIC.
"""
import logging
import os
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = ROOT_DIR / "logs"
SCHEMA_PATH = ROOT_DIR / "schema" / "user.avsc"

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092,localhost:9094,localhost:9095")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC = os.getenv("KAFKA_TOPIC", "student-topic")

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


def setup_logging(name: str) -> None:
    """Логирование в консоль и в файл logs/<name>.log."""
    LOG_DIR.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format=LOG_FORMAT,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(LOG_DIR / f"{name}.log", encoding="utf-8"),
        ],
    )
