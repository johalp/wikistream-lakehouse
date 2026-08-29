import json

from confluent_kafka import Consumer


KAFKA_TOPIC = "wikimedia.recentchange"

def main():
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "wikistream-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    consumer.subscribe([KAFKA_TOPIC])

    print("Listenng for kafka messages...")
    consumer.commit(message=message, asynchronous=False)

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                print(message.error())
                continue

            raw_message = message.value().decode("utf-8")

            try:
                event = json.loads(raw_message)
            except json.JSONDecodeError:
                print(f"Skipping non-JSON message: {raw_message}")
                continue

    except KeyboardInterrupt:
        print("Stopping consumer")

    finally:
        consumer.close()

if __name__ == "__main__":
    main()
