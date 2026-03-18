"""Spark job: build demand features and write to Snowflake staging table.

Usage:
    spark-submit --master spark://spark-master:7077 \
        --packages net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.5,net.snowflake:snowflake-jdbc:3.18.0 \
        spark/jobs/run_demand_features.py
"""

import os
import sys

# Add project root to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from spark.config import create_spark_session, read_snowflake_table, write_snowflake_table
from spark.features.demand_features_spark import build_demand_features_spark, get_feature_columns


def main():
    print("Starting Spark demand feature engineering...")
    spark = create_spark_session("demand-features")

    try:
        # Read source tables from Snowflake
        print("  Reading MART_DAILY_PRODUCT_KPIS...")
        product_kpis = read_snowflake_table(
            spark,
            "MART_DAILY_PRODUCT_KPIS",
            schema="MARTS",
            query="SELECT * FROM FULFILLMENT_DB.MARTS.MART_DAILY_PRODUCT_KPIS WHERE IS_FORECAST = FALSE",
        )
        print(f"  Loaded {product_kpis.count():,} rows")

        print("  Reading STG_DATES...")
        dates = read_snowflake_table(spark, "STG_DATES", schema="STAGING")

        print("  Reading STG_PRODUCTS (current only)...")
        products = read_snowflake_table(
            spark,
            "STG_PRODUCTS",
            schema="STAGING",
            query="SELECT * FROM FULFILLMENT_DB.STAGING.STG_PRODUCTS WHERE IS_CURRENT = TRUE",
        )

        # Build features
        print("  Building demand features...")
        features_df = build_demand_features_spark(product_kpis, dates, products)

        # Select only feature columns + keys for downstream
        output_cols = ["date", "product_id"] + get_feature_columns() + ["total_units_sold"]
        features_df = features_df.select(*[c for c in output_cols if c in features_df.columns])

        row_count = features_df.count()
        print(f"  Built {row_count:,} feature rows")

        # Write to Snowflake staging table
        print("  Writing to STAGING.SPARK_DEMAND_FEATURES...")
        write_snowflake_table(features_df, "SPARK_DEMAND_FEATURES", schema="STAGING")
        print(f"  Done — {row_count:,} rows written")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
