"""Kafka producer that replays S3-landed CSVs as streaming events.

Reads the same CSV files that Lambda uploads to S3 and publishes them
row-by-row as Kafka events with simulated timestamps spread across the day.

Usage:
    python -m streaming.producer --date 2026-03-17
    python -m streaming.producer --date 2026-03-17 --source local
"""

import argparse
import io
import json
import time
from datetime import datetime

import boto3
import pandas as pd
from confluent_kafka import Producer

from streaming.config import PRODUCER_CONFIG, TOPICS
from streaming.schemas import (
    DeliveryUpdatedEvent,
    InventorySnapshotEvent,
    OrderCreatedEvent,
)

S3_BUCKET = "last-mile-fulfillment-platform"

TABLE_TOPIC_MAP = {
    "fact_orders": TOPICS["orders"],
    "fact_deliveries": TOPICS["deliveries"],
    "fact_inventory_snapshot": TOPICS["inventory"],
}

TABLE_KEY_MAP = {
    "fact_orders": "order_id",
    "fact_deliveries": "delivery_id",
    "fact_inventory_snapshot": lambda row: f"{row['warehouse_id']}-{row['product_id']}",
}

TABLE_SCHEMA_MAP = {
    "fact_orders": OrderCreatedEvent,
    "fact_deliveries": DeliveryUpdatedEvent,
    "fact_inventory_snapshot": InventorySnapshotEvent,
}


def delivery_report(err, msg):
    if err:
        print(f"  Delivery failed: {err}")


def read_csv_from_s3(table: str, date: str) -> pd.DataFrame:
    s3 = boto3.client("s3")
    key = f"raw/{table}/date={date}/data.csv"
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return pd.read_csv(io.BytesIO(obj["Body"].read()))
    except s3.exceptions.NoSuchKey:
        print(f"  No S3 file found: s3://{S3_BUCKET}/{key}")
        return pd.DataFrame()


def read_csv_local(table: str, date: str) -> pd.DataFrame:
    path = f"output/raw/{table}/date={date}/data.csv"
    try:
        return pd.read_csv(path)
    except FileNotFoundError:
        print(f"  No local file found: {path}")
        return pd.DataFrame()


def produce_events(date: str, source: str = "s3", delay_ms: float = 5):
    producer = Producer(PRODUCER_CONFIG)
    total_produced = 0

    for table, topic in TABLE_TOPIC_MAP.items():
        if source == "s3":
            df = read_csv_from_s3(table, date)
        else:
            df = read_csv_local(table, date)

        if df.empty:
            continue

        schema_cls = TABLE_SCHEMA_MAP[table]
        key_field = TABLE_KEY_MAP[table]

        print(f"  Producing {len(df)} events to {topic}...")

        for _, row in df.iterrows():
            row_dict = row.to_dict()
            row_dict["event_time"] = datetime.utcnow().isoformat()

            # Extract message key
            if callable(key_field):
                msg_key = key_field(row_dict)
            else:
                msg_key = str(row_dict.get(key_field, ""))

            # Validate with Pydantic (skip fields not in schema)
            try:
                event = schema_cls(**row_dict)
                value = event.model_dump_json()
            except Exception:
                value = json.dumps(row_dict, default=str)

            producer.produce(
                topic=topic,
                key=msg_key,
                value=value,
                callback=delivery_report,
            )
            total_produced += 1

            if delay_ms > 0:
                time.sleep(delay_ms / 1000)

            if total_produced % 1000 == 0:
                producer.flush()

    producer.flush()
    print(f"  Total events produced: {total_produced}")


def main():
    parser = argparse.ArgumentParser(description="Produce fulfillment events to Kafka")
    parser.add_argument("--date", required=True, help="Date to replay (YYYY-MM-DD)")
    parser.add_argument(
        "--source", default="s3", choices=["s3", "local"], help="Read CSVs from S3 or local output/ directory"
    )
    parser.add_argument("--delay-ms", type=float, default=5, help="Delay between messages in ms (simulates real-time)")
    args = parser.parse_args()

    print(f"Producing events for {args.date} from {args.source}...")
    produce_events(args.date, args.source, args.delay_ms)


if __name__ == "__main__":
    main()
