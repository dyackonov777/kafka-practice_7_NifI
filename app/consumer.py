"""Avro-консьюмер: читает записи User из топика student-topic.

Схема для десериализации подтягивается из Schema Registry по id из сообщения.
Оффсеты коммитятся вручную после обработки (at-least-once). Битые сообщения
логируются и пропускаются, не останавливая консьюмер.

Запуск:
    python app/consumer.py                    # до Ctrl+C
    python app/consumer.py --idle-timeout 15  # выход после 15 с без сообщений
"""
import argparse
import logging
import sys
import time

from confluent_kafka import Consumer, KafkaException, Message
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from config import BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL, TOPIC, setup_logging

log = logging.getLogger("consumer")

POLL_TIMEOUT_S = 1.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Avro consumer for student-topic")
    parser.add_argument("--group", default="practice-group", help="consumer group id")
    parser.add_argument("--idle-timeout", type=float, default=0,
                        help="завершиться после N секунд без сообщений (0 — работать бесконечно)")
    return parser.parse_args()


def build_consumer(group_id: str) -> Consumer:
    return Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "client.id": "practice-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })


def log_assignment(_consumer: Consumer, partitions: list) -> None:
    log.info("Assigned partitions: %s", [p.partition for p in partitions])


def process_message(msg: Message, deserializer: AvroDeserializer) -> bool:
    """Десериализует и логирует сообщение. Возвращает False для битого сообщения."""
    try:
        user = deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
    except Exception as e:  # битое сообщение (poison pill) не должно ронять консьюмер
        log.error("Skipping bad message partition=%d offset=%d: %s: %s",
                  msg.partition(), msg.offset(), type(e).__name__, e)
        return False

    key = msg.key().decode("utf-8", errors="replace") if msg.key() else None
    log.info("Received partition=%d offset=%d key=%s value=%s",
             msg.partition(), msg.offset(), key, user)
    return True


def commit(consumer: Consumer, msg: Message) -> None:
    try:
        consumer.commit(message=msg, asynchronous=False)
    except KafkaException as e:
        # следующий успешный коммит закроет и этот оффсет
        log.warning("Offset commit failed: %s", e)


def main() -> int:
    args = parse_args()
    setup_logging("consumer")

    deserializer = AvroDeserializer(SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL}))
    consumer = build_consumer(args.group)
    consumer.subscribe([TOPIC], on_assign=log_assignment)
    log.info("Consuming from '%s' as group '%s' (bootstrap=%s)",
             TOPIC, args.group, BOOTSTRAP_SERVERS)

    received = skipped = 0
    last_message_at = time.monotonic()
    try:
        while True:
            msg = consumer.poll(POLL_TIMEOUT_S)
            if msg is None:
                idle_s = time.monotonic() - last_message_at
                if args.idle_timeout and idle_s > args.idle_timeout:
                    log.info("No messages for %.0f s, stopping", args.idle_timeout)
                    break
                continue

            if msg.error():
                if msg.error().fatal():
                    raise KafkaException(msg.error())
                log.warning("Consumer error (retriable): %s", msg.error())
                continue

            last_message_at = time.monotonic()
            if process_message(msg, deserializer):
                received += 1
            else:
                skipped += 1
            # коммитим и пропущенные сообщения, чтобы не зациклиться на битом
            commit(consumer, msg)
    except KeyboardInterrupt:
        log.info("Interrupted by user")
    finally:
        consumer.close()
        log.info("Consumer closed, messages read: %d, skipped (bad): %d", received, skipped)
    return 0


if __name__ == "__main__":
    sys.exit(main())
