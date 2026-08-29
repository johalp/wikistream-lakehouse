from __future__ import annotations

import logging
import signal
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException
from pydantic import ValidationError

from wikistream.config import Settings
from wikistream.models import WikimediaChange

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger(__name__)
RUNNING = True


def stop(_signum: int, _frame: Any) -> None:
    global RUNNING
    RUNNING = False
    LOGGER.info("Shutdown requested")


def run() -> None:
    settings = Settings.from_env()
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.kafka_consumer_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([settings.kafka_topic])
    LOGGER.info("Consuming %s as group %s", settings.kafka_topic, settings.kafka_consumer_group)

    try:
        while RUNNING:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(message.error())

            try:
                event = WikimediaChange.model_validate_json(message.value())
                LOGGER.info(
                    "partition=%s offset=%s | %s | %s | delta=%s | bot=%s",
                    message.partition(),
                    message.offset(),
                    event.domain,
                    event.title,
                    event.length_delta,
                    event.bot,
                )
                # Commit only after successful processing.
                consumer.commit(message=message, asynchronous=False)
            except ValidationError as exc:
                LOGGER.error("Invalid Kafka payload at offset %s: %s", message.offset(), exc)
    finally:
        consumer.close()


if __name__ == "__main__":
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    run()
