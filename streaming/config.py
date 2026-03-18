"""Kafka configuration for the fulfillment streaming layer."""

import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "fulfillment-pipeline")

TOPICS = {
    "orders": "fulfillment.orders.created",
    "deliveries": "fulfillment.deliveries.updated",
    "inventory": "fulfillment.inventory.snapshot",
}

TOPIC_CONFIG = {
    "fulfillment.orders.created": {
        "partitions": 8,
        "retention_ms": 7 * 24 * 60 * 60 * 1000,  # 7 days
    },
    "fulfillment.deliveries.updated": {
        "partitions": 8,
        "retention_ms": 7 * 24 * 60 * 60 * 1000,
    },
    "fulfillment.inventory.snapshot": {
        "partitions": 8,
        "retention_ms": 3 * 24 * 60 * 60 * 1000,  # 3 days
    },
}

PRODUCER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id": "fulfillment-producer",
    "acks": "all",
    "retries": 3,
    "linger.ms": 10,
    "batch.size": 32768,
}

CONSUMER_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": KAFKA_CONSUMER_GROUP,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
    "max.poll.interval.ms": 300000,
}
