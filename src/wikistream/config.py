from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "wikimedia.recentchange"
    kafka_consumer_group: str = "wikistream-console"
    wikimedia_stream_url: str = "https://stream.wikimedia.org/v2/stream/recentchange"
    wikimedia_user_agent: str = "wikistream-lakehouse/0.1"

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            kafka_bootstrap_servers=os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            cls.kafka_bootstrap_servers,
            ),
            kafka_topic=os.getenv("KAFKA_TOPIC", cls.kafka_topic),
            kafka_consumer_group=os.getenv("KAFKA_CONSUMER_GROUP", cls.kafka_consumer_group),
            wikimedia_stream_url=os.getenv("WIKIMEDIA_STREAM_URL", cls.wikimedia_stream_url),
            wikimedia_user_agent=os.getenv("WIKIMEDIA_USER_AGENT", cls.wikimedia_user_agent),
        )
