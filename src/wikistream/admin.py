from __future__ import annotations

import logging

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.error import KafkaError, KafkaException

from wikistream.config import Settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger(__name__)


def ensure_topic() -> None:
    settings = Settings.from_env()
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    futures = admin.create_topics(
        [NewTopic(settings.kafka_topic, num_partitions=3, replication_factor=1)]
    )

    future = futures[settings.kafka_topic]
    try:
        future.result()
        LOGGER.info("Created topic %s with 3 partitions", settings.kafka_topic)
    except KafkaException as exc:
        if exc.args and getattr(exc.args[0], "code", lambda: None)() == KafkaError.TOPIC_ALREADY_EXISTS:
            LOGGER.info("Topic %s already exists", settings.kafka_topic)
            return
        raise


if __name__ == "__main__":
    ensure_topic()
