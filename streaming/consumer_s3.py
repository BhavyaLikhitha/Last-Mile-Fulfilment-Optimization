"""Kafka consumer that batches events and writes CSVs to S3.

Produces CSV files in the same S3 path format as Lambda, so the existing
Airflow DAG (S3KeySensor → COPY INTO) works unchanged.

Usage:
    python -m streaming.consumer_s3
    python -m streaming.consumer_s3 --timeout 60
"""

import argparse
import io
import json
from collections import defaultdict

import boto3
import pandas as pd
from confluent_kafka import Consumer

from streaming.config import CONSUMER_CONFIG, TOPICS

S3_BUCKET = "last-mile-fulfillment-platform"

TOPIC_TABLE_MAP = {
    TOPICS["orders"]: "fact_orders",
    TOPICS["deliveries"]: "fact_deliveries",
    TOPICS["inventory"]: "fact_inventory_snapshot",
}

TOPIC_DATE_FIELD = {
    TOPICS["orders"]: "order_date",
    TOPICS["deliveries"]: "assigned_time",
    TOPICS["inventory"]: "snapshot_date",
}


def extract_date(topic: str, record: dict) -> str:
    date_field = TOPIC_DATE_FIELD.get(topic)
    if date_field and date_field in record:
        return str(record[date_field])[:10]
    return "unknown"


def flush_to_s3(batches: dict, s3_client):
    """Write accumulated batches to S3 as CSVs."""
    total_written = 0

    for (topic, date), records in batches.items():
        if not records:
            continue

        table = TOPIC_TABLE_MAP.get(topic, topic)
        df = pd.DataFrame(records)

        # Remove event envelope fields before writing CSV
        for col in ["event_type", "event_time"]:
            if col in df.columns:
                df = df.drop(columns=[col])

        csv_buffer = io.BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        s3_key = f"raw/{table}/date={date}/data.csv"
        s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=csv_buffer.getvalue())
        print(f"  Wrote {len(records)} rows → s3://{S3_BUCKET}/{s3_key}")
        total_written += len(records)

    return total_written


def consume_and_write(timeout_seconds: int = 300, batch_size: int = 5000):
    consumer = Consumer(CONSUMER_CONFIG)
    consumer.subscribe(list(TOPIC_TABLE_MAP.keys()))
    s3_client = boto3.client("s3")

    batches = defaultdict(list)
    total_consumed = 0
    empty_polls = 0
    max_empty_polls = timeout_seconds  # 1 poll/sec when no messages

    print(f"Consuming from {list(TOPIC_TABLE_MAP.keys())}...")
    print(f"Will flush after {batch_size} messages or {timeout_seconds}s of inactivity.")

    try:
        while empty_polls < max_empty_polls:
            msg = consumer.poll(1.0)

            if msg is None:
                empty_polls += 1
                continue

            if msg.error():
                print(f"  Consumer error: {msg.error()}")
                continue

            empty_polls = 0
            topic = msg.topic()
            value = json.loads(msg.value().decode("utf-8"))
            date = extract_date(topic, value)

            batches[(topic, date)].append(value)
            total_consumed += 1

            if total_consumed % batch_size == 0:
                written = flush_to_s3(batches, s3_client)
                consumer.commit()
                batches.clear()
                print(f"  Flushed batch: {written} rows total")

        # Final flush
        if any(batches.values()):
            written = flush_to_s3(batches, s3_client)
            consumer.commit()
            print(f"  Final flush: {written} rows")

    finally:
        consumer.close()

    print(f"  Total consumed: {total_consumed}")


def main():
    parser = argparse.ArgumentParser(description="Consume Kafka events and write to S3")
    parser.add_argument("--timeout", type=int, default=60, help="Seconds of inactivity before stopping")
    parser.add_argument("--batch-size", type=int, default=5000, help="Flush to S3 every N messages")
    args = parser.parse_args()

    consume_and_write(args.timeout, args.batch_size)


if __name__ == "__main__":
    main()
