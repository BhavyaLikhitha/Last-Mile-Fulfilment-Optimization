"""Spark job: build ETA features and write to Snowflake staging table.

Usage:
    spark-submit --master spark://spark-master:7077 \
        --packages net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.5,net.snowflake:snowflake-jdbc:3.18.0 \
        spark/jobs/run_eta_features.py
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from spark.config import create_spark_session, read_snowflake_table, write_snowflake_table
from spark.features.eta_features_spark import build_eta_features_spark, get_feature_columns


def main():
    print("Starting Spark ETA feature engineering...")
    spark = create_spark_session("eta-features")

    try:
        # Read source tables — no year-by-year chunking needed with Spark
        print("  Reading INT_DELIVERY_ENRICHED...")
        deliveries = read_snowflake_table(spark, "INT_DELIVERY_ENRICHED", schema="INTERMEDIATE")
        print(f"  Loaded {deliveries.count():,} rows")

        print("  Reading STG_DATES...")
        dates = read_snowflake_table(spark, "STG_DATES", schema="STAGING")

        # Build features
        print("  Building ETA features...")
        features_df = build_eta_features_spark(deliveries, dates)

        # Select only feature columns + keys for downstream
        key_cols = ["date", "delivery_id", "warehouse_id", "driver_id"]
        output_cols = key_cols + get_feature_columns() + ["actual_delivery_minutes"]
        features_df = features_df.select(*[c for c in output_cols if c in features_df.columns])

        row_count = features_df.count()
        print(f"  Built {row_count:,} feature rows")

        # Write to Snowflake staging table
        print("  Writing to STAGING.SPARK_ETA_FEATURES...")
        write_snowflake_table(features_df, "SPARK_ETA_FEATURES", schema="STAGING")
        print(f"  Done — {row_count:,} rows written")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
