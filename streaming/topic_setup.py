"""Create Kafka topics with configured partition count and retention.

Usage:
    python -m streaming.topic_setup
"""

from confluent_kafka.admin import AdminClient, NewTopic

from streaming.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_CONFIG


def create_topics():
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    existing = admin.list_topics(timeout=10).topics.keys()
    new_topics = []

    for topic_name, config in TOPIC_CONFIG.items():
        if topic_name in existing:
            print(f"  Topic '{topic_name}' already exists — skipping")
            continue

        new_topics.append(
            NewTopic(
                topic=topic_name,
                num_partitions=config["partitions"],
                replication_factor=1,
                config={"retention.ms": str(config["retention_ms"])},
            )
        )

    if not new_topics:
        print("All topics already exist.")
        return

    futures = admin.create_topics(new_topics)
    for topic_name, future in futures.items():
        try:
            future.result()
            print(f"  Created topic '{topic_name}'")
        except Exception as e:
            print(f"  Failed to create '{topic_name}': {e}")


if __name__ == "__main__":
    print("Creating Kafka topics...")
    create_topics()
    print("Done.")
