"""Kafka consumer that writes events directly to Snowflake via COPY INTO.

Alternative to consumer_s3.py — batches events in memory, writes a temp CSV,
stages it in Snowflake, and runs COPY INTO.

Usage:
    python -m streaming.consumer_snowflake
    python -m streaming.consumer_snowflake --timeout 60
"""

import argparse
import json
import os
import tempfile
from collections import defaultdict

import pandas as pd
import snowflake.connector
from confluent_kafka import Consumer

from streaming.config import CONSUMER_CONFIG, TOPICS

TOPIC_TABLE_MAP = {
    TOPICS["orders"]: "FACT_ORDERS",
    TOPICS["deliveries"]: "FACT_DELIVERIES",
    TOPICS["inventory"]: "FACT_INVENTORY_SNAPSHOT",
}


def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE", "FULFILLMENT_DB"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "FULFILLMENT_WH"),
        schema="RAW",
    )


def flush_to_snowflake(batches: dict, conn):
    """Write accumulated batches directly to Snowflake."""
    total_written = 0
    cursor = conn.cursor()

    for (topic, _date), records in batches.items():
        if not records:
            continue

        table = TOPIC_TABLE_MAP.get(topic)
        if not table:
            continue

        df = pd.DataFrame(records)
        for col in ["event_type", "event_time"]:
            if col in df.columns:
                df = df.drop(columns=[col])

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            df.to_csv(f, index=False)
            tmp_path = f.name

        try:
            stage_name = f"@%{table}"
            cursor.execute(f"PUT file://{tmp_path} {stage_name} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
            cursor.execute(f"""
                COPY INTO {table}
                FROM {stage_name}
                FILE_FORMAT = (TYPE='CSV' SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"')
                ON_ERROR = 'CONTINUE'
                PURGE = TRUE
            """)
            print(f"  Loaded {len(records)} rows → {table}")
            total_written += len(records)
        finally:
            os.unlink(tmp_path)

    cursor.close()
    return total_written


def consume_and_write(timeout_seconds: int = 60, batch_size: int = 5000):
    consumer = Consumer(CONSUMER_CONFIG)
    consumer.subscribe(list(TOPIC_TABLE_MAP.keys()))
    conn = get_snowflake_connection()

    batches = defaultdict(list)
    total_consumed = 0
    empty_polls = 0

    print("Consuming from Kafka → Snowflake RAW...")

    try:
        while empty_polls < timeout_seconds:
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
            date = value.get("order_date", value.get("snapshot_date", "unknown"))[:10]

            batches[(topic, date)].append(value)
            total_consumed += 1

            if total_consumed % batch_size == 0:
                written = flush_to_snowflake(batches, conn)
                consumer.commit()
                batches.clear()
                print(f"  Flushed batch: {written} rows")

        if any(batches.values()):
            written = flush_to_snowflake(batches, conn)
            consumer.commit()
            print(f"  Final flush: {written} rows")

    finally:
        consumer.close()
        conn.close()

    print(f"  Total consumed: {total_consumed}")


def main():
    parser = argparse.ArgumentParser(description="Consume Kafka events → Snowflake")
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--batch-size", type=int, default=5000)
    args = parser.parse_args()

    consume_and_write(args.timeout, args.batch_size)


if __name__ == "__main__":
    main()
