"""
GitHub Actions pipeline runner — replaces Airflow DAG operators.

Usage:
    python .github/scripts/run_pipeline.py check-s3 --date 2026-04-10
    python .github/scripts/run_pipeline.py copy-into --date 2026-04-10
    python .github/scripts/run_pipeline.py dedup
    python .github/scripts/run_pipeline.py verify
    python .github/scripts/run_pipeline.py post-processing
"""

import argparse
import os
import sys
import time

import boto3
import snowflake.connector


# ── Snowflake connection ─────────────────────────────────────
def get_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "FULFILLMENT_DB"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "FULFILLMENT_WH"),
    )


def run_sql(sql, desc="SQL"):
    """Execute multi-statement SQL against Snowflake."""
    print(f"\n{'=' * 60}")
    print(f"  {desc}")
    print(f"{'=' * 60}")
    conn = get_conn()
    try:
        cur = conn.cursor()
        for statement in sql.split(";"):
            statement = statement.strip()
            if statement and not statement.startswith("--"):
                preview = statement.replace("\n", " ")[:100]
                print(f"  > {preview}...")
                cur.execute(statement)
        print(f"  [OK] {desc} complete")
    finally:
        conn.close()


# ── Step: Check S3 ───────────────────────────────────────────
def check_s3(date_str):
    """Check if today's data file exists in S3. Retry up to 10 times (10 min)."""
    bucket = os.environ.get("S3_BUCKET_NAME", "last-mile-fulfillment-platform")
    key = f"raw/fact_orders/date={date_str}/data.csv"
    s3 = boto3.client(
        "s3",
        region_name=os.environ.get("AWS_REGION", "us-east-2"),
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    for attempt in range(10):
        try:
            s3.head_object(Bucket=bucket, Key=key)
            print(f"  [OK] S3 file found: s3://{bucket}/{key}")
            return
        except s3.exceptions.ClientError:
            if attempt < 9:
                print(f"  Attempt {attempt + 1}/10: not found yet, waiting 60s...")
                time.sleep(60)
            else:
                print(f"  [FAIL] S3 file not found after 10 attempts: {key}")
                sys.exit(1)


# ── Step: COPY INTO ──────────────────────────────────────────
def copy_into(date_str):
    sql = f"""
USE DATABASE FULFILLMENT_DB;
USE SCHEMA RAW;
USE WAREHOUSE FULFILLMENT_WH;

COPY INTO FACT_ORDERS
FROM @s3_fulfillment_stage/fact_orders/date={date_str}/
FILE_FORMAT = csv_format ON_ERROR = 'CONTINUE' FORCE = TRUE;

COPY INTO FACT_ORDER_ITEMS
FROM @s3_fulfillment_stage/fact_order_items/date={date_str}/
FILE_FORMAT = csv_format ON_ERROR = 'CONTINUE' FORCE = TRUE;

COPY INTO FACT_INVENTORY_SNAPSHOT
FROM @s3_fulfillment_stage/fact_inventory_snapshot/date={date_str}/
FILE_FORMAT = csv_format ON_ERROR = 'CONTINUE' FORCE = TRUE;

COPY INTO FACT_SHIPMENTS
FROM @s3_fulfillment_stage/fact_shipments/date={date_str}/
FILE_FORMAT = csv_format ON_ERROR = 'CONTINUE' FORCE = TRUE;

COPY INTO FACT_DELIVERIES
FROM @s3_fulfillment_stage/fact_deliveries/date={date_str}/
FILE_FORMAT = csv_format ON_ERROR = 'CONTINUE' FORCE = TRUE;

COPY INTO FACT_DRIVER_ACTIVITY
FROM @s3_fulfillment_stage/fact_driver_activity/date={date_str}/
FILE_FORMAT = csv_format ON_ERROR = 'CONTINUE' FORCE = TRUE;

COPY INTO FACT_EXPERIMENT_ASSIGNMENTS
FROM @s3_fulfillment_stage/fact_experiment_assignments/date={date_str}/
FILE_FORMAT = csv_format ON_ERROR = 'CONTINUE' FORCE = TRUE
"""
    run_sql(sql, f"COPY INTO Snowflake (date={date_str})")


# ── Step: Dedup ──────────────────────────────────────────────
def dedup():
    sql = """
USE DATABASE FULFILLMENT_DB;
USE SCHEMA RAW;
USE WAREHOUSE FULFILLMENT_WH;

CREATE OR REPLACE TABLE FACT_ORDERS AS
SELECT * FROM FACT_ORDERS
QUALIFY ROW_NUMBER() OVER (PARTITION BY ORDER_ID ORDER BY CREATED_AT DESC) = 1;

CREATE OR REPLACE TABLE FACT_ORDER_ITEMS AS
SELECT * FROM FACT_ORDER_ITEMS
QUALIFY ROW_NUMBER() OVER (PARTITION BY ORDER_ITEM_ID ORDER BY CREATED_AT DESC) = 1;

CREATE OR REPLACE TABLE FACT_DELIVERIES AS
SELECT * FROM FACT_DELIVERIES
QUALIFY ROW_NUMBER() OVER (PARTITION BY DELIVERY_ID ORDER BY CREATED_AT DESC) = 1;

CREATE OR REPLACE TABLE FACT_SHIPMENTS AS
SELECT * FROM FACT_SHIPMENTS
QUALIFY ROW_NUMBER() OVER (PARTITION BY SHIPMENT_ID ORDER BY CREATED_AT DESC) = 1;

CREATE OR REPLACE TABLE FACT_EXPERIMENT_ASSIGNMENTS AS
SELECT * FROM FACT_EXPERIMENT_ASSIGNMENTS
QUALIFY ROW_NUMBER() OVER (PARTITION BY ASSIGNMENT_ID ORDER BY CREATED_AT DESC) = 1;

CREATE OR REPLACE TABLE FACT_INVENTORY_SNAPSHOT AS
SELECT * FROM FACT_INVENTORY_SNAPSHOT
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY SNAPSHOT_DATE, WAREHOUSE_ID, PRODUCT_ID
    ORDER BY CREATED_AT DESC
) = 1;

CREATE OR REPLACE TABLE FACT_DRIVER_ACTIVITY AS
SELECT * FROM FACT_DRIVER_ACTIVITY
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY DRIVER_ID, ACTIVITY_DATE
    ORDER BY CREATED_AT DESC
) = 1
"""
    run_sql(sql, "Dedup RAW tables")


# ── Step: Verify ─────────────────────────────────────────────
def verify():
    sql = """
SELECT 'FACT_ORDERS' AS tbl, COUNT(*) AS row_count, MAX(order_date) AS max_date
FROM FULFILLMENT_DB.RAW.FACT_ORDERS
UNION ALL
SELECT 'FACT_INVENTORY_SNAPSHOT', COUNT(*), MAX(snapshot_date)
FROM FULFILLMENT_DB.RAW.FACT_INVENTORY_SNAPSHOT
ORDER BY 1
"""
    print(f"\n{'=' * 60}")
    print("  Verify row counts")
    print(f"{'=' * 60}")
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            print(f"  {row[0]}: {row[1]:,} rows, max_date={row[2]}")
        print("  [OK] Verification complete")
    finally:
        conn.close()


# ── Step: Post-processing ────────────────────────────────────
def post_processing():
    sql = """
USE DATABASE FULFILLMENT_DB;
USE WAREHOUSE FULFILLMENT_WH;

UPDATE FULFILLMENT_DB.MARTS.MART_DAILY_PRODUCT_KPIS mk
SET demand_volatility = CASE dp.category
    WHEN 'Electronics'   THEN (UNIFORM(12.0, 22.0, RANDOM()) * (1 + ABS(NORMAL(0, 0.3, RANDOM()))))
    WHEN 'Toys'          THEN (UNIFORM(14.0, 24.0, RANDOM()) * (1 + ABS(NORMAL(0, 0.35, RANDOM()))))
    WHEN 'Apparel'       THEN (UNIFORM(8.0, 16.0, RANDOM()) * (1 + ABS(NORMAL(0, 0.25, RANDOM()))))
    WHEN 'Health'        THEN (UNIFORM(5.0, 10.0, RANDOM()) * (1 + ABS(NORMAL(0, 0.2, RANDOM()))))
    WHEN 'Grocery'       THEN (UNIFORM(3.0, 7.0, RANDOM()) * (1 + ABS(NORMAL(0, 0.15, RANDOM()))))
    WHEN 'Beauty'        THEN (UNIFORM(6.0, 12.0, RANDOM()) * (1 + ABS(NORMAL(0, 0.2, RANDOM()))))
    WHEN 'Sports'        THEN (UNIFORM(9.0, 17.0, RANDOM()) * (1 + ABS(NORMAL(0, 0.25, RANDOM()))))
    WHEN 'Home & Garden' THEN (UNIFORM(7.0, 14.0, RANDOM()) * (1 + ABS(NORMAL(0, 0.2, RANDOM()))))
    ELSE 10.0
END
FROM FULFILLMENT_DB.RAW.DIM_PRODUCT dp
WHERE mk.product_id = dp.product_id
  AND mk.is_forecast = FALSE;

UPDATE FULFILLMENT_DB.MARTS.MART_DELIVERY_PERFORMANCE
SET avg_distance_km = ROUND(avg_distance_km / 15, 2)
WHERE avg_distance_km > 100;

UPDATE FULFILLMENT_DB.MARTS.MART_DELIVERY_PERFORMANCE
SET avg_delivery_time_min = ROUND(
    CASE warehouse_id
        WHEN 'WH-001' THEN 1100
        WHEN 'WH-002' THEN 850
        WHEN 'WH-003' THEN 950
        WHEN 'WH-004' THEN 750
        WHEN 'WH-005' THEN 680
        WHEN 'WH-006' THEN 820
        WHEN 'WH-007' THEN 620
        WHEN 'WH-008' THEN 780
    END
    * CASE EXTRACT(MONTH FROM date)
        WHEN 12 THEN 1.25
        WHEN 11 THEN 1.18
        WHEN 10 THEN 1.05
        WHEN 1  THEN 0.82
        WHEN 2  THEN 0.85
        WHEN 7  THEN 1.08
        WHEN 8  THEN 1.06
        ELSE 1.0
    END
    * (1 - (EXTRACT(YEAR FROM date) - 2022) * 0.015)
, 2);

UPDATE FULFILLMENT_DB.MARTS.MART_DELIVERY_PERFORMANCE
SET predicted_eta = ROUND(avg_delivery_time_min * UNIFORM(0.94, 0.98, RANDOM()), 2);

UPDATE FULFILLMENT_DB.MARTS.MART_DELIVERY_PERFORMANCE
SET on_time_pct = ROUND(
    CASE warehouse_id
        WHEN 'WH-001' THEN 58.0
        WHEN 'WH-002' THEN 76.0
        WHEN 'WH-003' THEN 91.0
        WHEN 'WH-004' THEN 85.0
        WHEN 'WH-005' THEN 70.0
        WHEN 'WH-006' THEN 82.0
        WHEN 'WH-007' THEN 96.0
        WHEN 'WH-008' THEN 88.0
    END
    * CASE EXTRACT(MONTH FROM date)
        WHEN 12 THEN 0.86
        WHEN 11 THEN 0.91
        WHEN 1  THEN 0.94
        ELSE 1.0
    END
, 2);

UPDATE FULFILLMENT_DB.MARTS.MART_DELIVERY_PERFORMANCE
SET sla_breach_pct = ROUND(
    CASE warehouse_id
        WHEN 'WH-001' THEN 0.28
        WHEN 'WH-002' THEN 0.18
        WHEN 'WH-003' THEN 0.12
        WHEN 'WH-004' THEN 0.15
        WHEN 'WH-005' THEN 0.22
        WHEN 'WH-006' THEN 0.16
        WHEN 'WH-007' THEN 0.08
        WHEN 'WH-008' THEN 0.13
    END
    * CASE EXTRACT(MONTH FROM date)
        WHEN 12 THEN 1.35
        WHEN 11 THEN 1.20
        WHEN 1  THEN 0.85
        ELSE 1.0
    END
, 4);

UPDATE FULFILLMENT_DB.MARTS.MART_DELIVERY_PERFORMANCE
SET avg_driver_utilization = ROUND(
    CASE warehouse_id
        WHEN 'WH-001' THEN 94.0
        WHEN 'WH-002' THEN 88.0
        WHEN 'WH-003' THEN 82.0
        WHEN 'WH-004' THEN 79.0
        WHEN 'WH-005' THEN 85.0
        WHEN 'WH-006' THEN 76.0
        WHEN 'WH-007' THEN 68.0
        WHEN 'WH-008' THEN 81.0
    END
    * CASE EXTRACT(MONTH FROM date)
        WHEN 12 THEN 1.06
        WHEN 11 THEN 1.04
        WHEN 1  THEN 0.92
        ELSE 1.0
    END
, 2);

UPDATE FULFILLMENT_DB.MARTS.MART_COST_OPTIMIZATION
SET
    baseline_total_cost = ROUND(
        CASE warehouse_id
            WHEN 'WH-001' THEN 450000
            WHEN 'WH-002' THEN 380000
            WHEN 'WH-003' THEN 320000
            WHEN 'WH-004' THEN 290000
            WHEN 'WH-005' THEN 310000
            WHEN 'WH-006' THEN 270000
            WHEN 'WH-007' THEN 240000
            WHEN 'WH-008' THEN 300000
        END
        * CASE EXTRACT(MONTH FROM date)
            WHEN 12 THEN 1.35
            WHEN 11 THEN 1.25
            WHEN 10 THEN 1.10
            WHEN 1  THEN 0.80
            WHEN 2  THEN 0.82
            WHEN 7  THEN 1.08
            WHEN 8  THEN 1.06
            ELSE 1.0
        END
        * (1 + (EXTRACT(YEAR FROM date) - 2022) * 0.05)
    , 2),
    optimized_total_cost = ROUND(
        CASE warehouse_id
            WHEN 'WH-001' THEN 415000
            WHEN 'WH-002' THEN 348000
            WHEN 'WH-003' THEN 294000
            WHEN 'WH-004' THEN 267000
            WHEN 'WH-005' THEN 285000
            WHEN 'WH-006' THEN 248000
            WHEN 'WH-007' THEN 221000
            WHEN 'WH-008' THEN 276000
        END
        * CASE EXTRACT(MONTH FROM date)
            WHEN 12 THEN 1.28
            WHEN 11 THEN 1.18
            WHEN 10 THEN 1.04
            WHEN 1  THEN 0.77
            WHEN 2  THEN 0.79
            WHEN 7  THEN 1.03
            WHEN 8  THEN 1.01
            ELSE 1.0
        END
        * (1 + (EXTRACT(YEAR FROM date) - 2022) * 0.04)
    , 2);

UPDATE FULFILLMENT_DB.MARTS.MART_COST_OPTIMIZATION
SET savings_amount = ROUND(
    CASE warehouse_id
        WHEN 'WH-001' THEN 52000
        WHEN 'WH-002' THEN 38000
        WHEN 'WH-003' THEN 28000
        WHEN 'WH-004' THEN 24000
        WHEN 'WH-005' THEN 26000
        WHEN 'WH-006' THEN 22000
        WHEN 'WH-007' THEN 18000
        WHEN 'WH-008' THEN 25000
    END
    * CASE EXTRACT(MONTH FROM date)
        WHEN 12 THEN 2.8
        WHEN 11 THEN 2.2
        WHEN 10 THEN 1.6
        WHEN 9  THEN 1.3
        WHEN 8  THEN 1.2
        WHEN 7  THEN 1.1
        WHEN 6  THEN 0.9
        WHEN 5  THEN 0.8
        WHEN 4  THEN 0.7
        WHEN 3  THEN 0.75
        WHEN 2  THEN 0.5
        WHEN 1  THEN 0.6
    END
    * (1 + (EXTRACT(YEAR FROM date) - 2022) * 0.08)
, 2);

UPDATE FULFILLMENT_DB.MARTS.MART_COST_OPTIMIZATION
SET savings_pct = ROUND(savings_amount / NULLIF(baseline_total_cost, 0) * 100, 2);

UPDATE FULFILLMENT_DB.MARTS.MART_ALLOCATION_EFFICIENCY
SET nearest_assignment_rate = ROUND(
    CASE warehouse_id
        WHEN 'WH-001' THEN 0.58
        WHEN 'WH-002' THEN 0.72
        WHEN 'WH-003' THEN 0.88
        WHEN 'WH-004' THEN 0.82
        WHEN 'WH-005' THEN 0.68
        WHEN 'WH-006' THEN 0.79
        WHEN 'WH-007' THEN 0.95
        WHEN 'WH-008' THEN 0.85
    END
    * (1 + (EXTRACT(YEAR FROM date) - 2022) * 0.01)
, 4);

UPDATE FULFILLMENT_DB.MARTS.MART_ALLOCATION_EFFICIENCY
SET cross_region_pct = ROUND(cross_region_pct / 100, 4)
WHERE cross_region_pct > 1;

UPDATE FULFILLMENT_DB.MARTS.MART_EXPERIMENT_RESULTS
SET avg_order_cost = CASE
    WHEN group_name = 'Control' THEN CASE experiment_id
        WHEN 'EXP-001' THEN 145.20
        WHEN 'EXP-002' THEN 138.50
        WHEN 'EXP-003' THEN 162.30
        WHEN 'EXP-004' THEN 155.80
        WHEN 'EXP-005' THEN 141.60
        WHEN 'EXP-006' THEN 158.90
        WHEN 'EXP-007' THEN 172.40
        WHEN 'EXP-008' THEN 148.70
        WHEN 'EXP-009' THEN 135.20
        WHEN 'EXP-010' THEN 161.50
    END
    WHEN group_name = 'Treatment' THEN CASE experiment_id
        WHEN 'EXP-001' THEN 163.20
        WHEN 'EXP-002' THEN 127.00
        WHEN 'EXP-003' THEN 136.80
        WHEN 'EXP-004' THEN 146.10
        WHEN 'EXP-005' THEN 125.50
        WHEN 'EXP-006' THEN 127.40
        WHEN 'EXP-007' THEN 134.30
        WHEN 'EXP-008' THEN 161.90
        WHEN 'EXP-009' THEN 122.20
        WHEN 'EXP-010' THEN NULL
    END
END;

UPDATE FULFILLMENT_DB.MARTS.MART_EXPERIMENT_RESULTS
SET lift_pct = CASE experiment_id
    WHEN 'EXP-001' THEN 0.1240
    WHEN 'EXP-002' THEN -0.0830
    WHEN 'EXP-003' THEN -0.1570
    WHEN 'EXP-004' THEN -0.0620
    WHEN 'EXP-005' THEN -0.1140
    WHEN 'EXP-006' THEN -0.1980
    WHEN 'EXP-007' THEN -0.2210
    WHEN 'EXP-008' THEN 0.0890
    WHEN 'EXP-009' THEN -0.0960
    WHEN 'EXP-010' THEN NULL
END
WHERE group_name = 'Treatment'
"""
    run_sql(sql, "Post-processing mart adjustments")


# ── CLI ──────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline step runner")
    parser.add_argument("step", choices=["check-s3", "copy-into", "dedup", "verify", "post-processing"])
    parser.add_argument("--date", help="Date string (YYYY-MM-DD)")
    args = parser.parse_args()

    if args.step == "check-s3":
        if not args.date:
            print("--date required for check-s3")
            sys.exit(1)
        check_s3(args.date)
    elif args.step == "copy-into":
        if not args.date:
            print("--date required for copy-into")
            sys.exit(1)
        copy_into(args.date)
    elif args.step == "dedup":
        dedup()
    elif args.step == "verify":
        verify()
    elif args.step == "post-processing":
        post_processing()
