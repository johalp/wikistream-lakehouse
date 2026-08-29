
import json

import requests
from confluent_kafka import Producer

EVENT_STEAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"



def main():
    kafka_producer = Producer({
    "bootstrap.servers": "localhost:9092"
    })
    headers = {
        "User-Agent": "wikistream-lakehouse/0.1 (contact: angeljohal5@gmail.com)",
        "Accept": "application/json",
    }


    response = requests.get(
        EVENT_STEAM_URL,
        stream=True,
        timeout = 60,
        headers=headers
    )

    response.raise_for_status()

    print("Connected to wikimedia")

    try:
        for line in response.iter_lines():
            if line:
                event = json.loads(line)

                clean_event = transform_event(event)

                message_key = f"{clean_event['wiki']}:{clean_event['title']}"

                kafka_producer.produce(
                    topic="wikimedia.recentchange",
                    key=message_key.encode("utf-8"),
                    value=json.dumps(clean_event).encode("utf-8")
                )

                kafka_producer.poll(0)

                print("Sent event to Kafka")

    except KeyboardInterrupt:
        print("Stopping producer")

    finally:
        kafka_producer.flush()


def transform_event(event):
    length = event.get("length", {})

    old_length = length.get("old")
    new_length = length.get("new")

    length_diff = None


    if old_length is not None and new_length is not None:
        length_diff = new_length - old_length

    return {
        "event_id": event["meta"]["id"],
        "event_time": event["meta"]["dt"],
        "domain": event["meta"]["domain"],
        "wiki": event["wiki"],
        "change_type": event["type"],
        "namespace": event["namespace"],
        "title": event["title"],
        "user": event["user"],
        "bot": event["bot"],
        "comment": event.get("comment"), #using .get incase 'comment' is missing
        "old_length": old_length,
        "new_length": new_length,
        "length_diff": length_diff,
    }

if __name__ == "__main__":
    main()


