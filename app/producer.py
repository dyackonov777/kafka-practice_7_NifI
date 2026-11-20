"""Avro-продюсер: отправляет записи User в топик student-topic.

Схема берётся из Schema Registry (subject student-topic-value). Автоматическая
регистрация отключена — схему заранее регистрирует scripts/register-schema.sh.

Запуск:
    python app/producer.py --count 10
    python app/producer.py --count 5 --bad 2   # + 2 битых сообщения для проверки обработки ошибок
"""
import argparse
import logging
import sys
import time
from dataclasses import dataclass

from confluent_kafka import KafkaError, KafkaException, Message, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    SerializationError,
)

from config import BOOTSTRAP_SERVERS, SCHEMA_PATH, SCHEMA_REGISTRY_URL, TOPIC, setup_logging

log = logging.getLogger("producer")

FLUSH_TIMEOUT_S = 30
BAD_PAYLOAD = b"this is not avro"


@dataclass
class DeliveryStats:
    """Delivery callback, считающий успешные и неуспешные доставки."""
    delivered: int = 0
    failed: int = 0

    def __call__(self, err: KafkaError | None, msg: Message) -> None:
        key = msg.key().decode() if msg.key() else None
        if err is not None:
            self.failed += 1
            log.error("Delivery FAILED key=%s: %s", key, err)
            return
        self.delivered += 1
        log.info("Delivered key=%s -> topic=%s partition=%d offset=%d",
                 key, msg.topic(), msg.partition(), msg.offset())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Avro producer for student-topic")
    parser.add_argument("--count", type=int, default=10, help="число сообщений")
    parser.add_argument("--start-id", type=int, default=1, help="id первого пользователя")
    parser.add_argument("--interval", type=float, default=0.5, help="пауза между сообщениями, с")
    parser.add_argument("--bad", type=int, default=0,
                        help="дополнительно отправить N битых (не-Avro) сообщений")
    return parser.parse_args()


def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "practice-producer",
        "acks": "all",               # подтверждение от всех ISR (min.insync.replicas=2)
        "enable.idempotence": True,  # без дублей при повторных отправках
        "linger.ms": 5,
        "compression.type": "lz4",
    })


def build_serializer() -> AvroSerializer:
    return AvroSerializer(
        schema_registry_client=SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL}),
        schema_str=SCHEMA_PATH.read_text(encoding="utf-8"),
        conf={"auto.register.schemas": False},
    )


def make_user(user_id: int) -> dict:
    return {
        "id": user_id,
        "name": f"user-{user_id}",
        "email": f"user{user_id}@example.com" if user_id % 2 == 0 else None,
        "created_at": int(time.time() * 1000),
    }


def produce(producer: Producer, key: str, value: bytes, on_delivery: DeliveryStats) -> bool:
    """Ставит сообщение в очередь продюсера; при переполнении очереди ждёт и повторяет."""
    while True:
        try:
            producer.produce(TOPIC, key=key.encode(), value=value, on_delivery=on_delivery)
            return True
        except BufferError:
            log.warning("Producer queue is full, waiting...")
            producer.poll(1)
        except KafkaException as e:
            log.error("Produce failed for key=%s: %s", key, e)
            return False


def main() -> int:
    args = parse_args()
    setup_logging("producer")

    producer = build_producer()
    serializer = build_serializer()
    stats = DeliveryStats()
    not_sent = 0
    context = SerializationContext(TOPIC, MessageField.VALUE)

    log.info("Start producing %d messages to '%s' (bootstrap=%s)",
             args.count, TOPIC, BOOTSTRAP_SERVERS)

    for user_id in range(args.start_id, args.start_id + args.count):
        user = make_user(user_id)
        try:
            value = serializer(user, context)
        except SerializationError as e:  # схема не зарегистрирована / данные не по схеме
            log.error("Serialization failed for %s: %s", user, e)
            not_sent += 1
            continue

        if produce(producer, str(user_id), value, stats):
            log.info("Sent: %s", user)
        else:
            not_sent += 1
        producer.poll(0)
        time.sleep(args.interval)

    for n in range(args.bad):
        if produce(producer, f"bad-{n}", BAD_PAYLOAD, stats):
            log.warning("Sent BAD (non-Avro) message key=bad-%d", n)

    in_queue = producer.flush(FLUSH_TIMEOUT_S)
    if in_queue or not_sent or stats.failed:
        log.error("Not delivered: %d still in queue, %d not sent, %d failed delivery",
                  in_queue, not_sent, stats.failed)
        return 1
    log.info("All %d messages delivered successfully", stats.delivered)
    return 0


if __name__ == "__main__":
    sys.exit(main())
