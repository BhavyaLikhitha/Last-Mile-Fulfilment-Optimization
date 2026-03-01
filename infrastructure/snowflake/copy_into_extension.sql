-- copy_into_extension.sql
-- Loads extension backfill (Feb 2025 → Feb 2026) into Snowflake RAW schema.
-- Run AFTER uploading output_extension/raw/ to S3.

USE DATABASE FULFILLMENT_DB;
USE SCHEMA RAW;
USE WAREHOUSE FULFILLMENT_WH;

-- ── Step 1: Extend dim_date ───────────────────────────────────
-- Appends Feb 2, 2025 → Feb 28, 2026 rows to existing dim_date.
COPY INTO DIM_DATE
FROM @s3_fulfillment_stage/raw/dim_date/
FILE_FORMAT = (FORMAT_NAME = 'csv_format')
ON_ERROR = 'CONTINUE';

-- Verify: should be ~1,489 rows (1,097 original + 392 new)
SELECT COUNT(*) as total_rows, MIN(date) as min_date, MAX(date) as max_date
FROM DIM_DATE;

-- ── Step 2: Overwrite updated dimensions (SCD Type 2 changes) ─
-- These dimensions were updated with realistic changes.
-- TRUNCATE + COPY re-loads the full updated dimension.
-- dbt snapshot will compare new state vs snapshot table and record diffs.

TRUNCATE TABLE DIM_PRODUCT;
COPY INTO DIM_PRODUCT
FROM @s3_fulfillment_stage/raw/dim_product/
FILE_FORMAT = (FORMAT_NAME = 'csv_format')
ON_ERROR = 'CONTINUE';

TRUNCATE TABLE DIM_SUPPLIER;
COPY INTO DIM_SUPPLIER
FROM @s3_fulfillment_stage/raw/dim_supplier/
FILE_FORMAT = (FORMAT_NAME = 'csv_format')
ON_ERROR = 'CONTINUE';

TRUNCATE TABLE DIM_DRIVER;
COPY INTO DIM_DRIVER
FROM @s3_fulfillment_stage/raw/dim_driver/
FILE_FORMAT = (FORMAT_NAME = 'csv_format')
ON_ERROR = 'CONTINUE';

TRUNCATE TABLE DIM_CUSTOMER;
COPY INTO DIM_CUSTOMER
FROM @s3_fulfillment_stage/raw/dim_customer/
FILE_FORMAT = (FORMAT_NAME = 'csv_format')
ON_ERROR = 'CONTINUE';

-- Verify dimension row counts are unchanged (same number of entities, just updated values)
SELECT 'DIM_PRODUCT'  AS tbl, COUNT(*) AS rows FROM DIM_PRODUCT  UNION ALL
SELECT 'DIM_SUPPLIER',         COUNT(*)         FROM DIM_SUPPLIER UNION ALL
SELECT 'DIM_DRIVER',           COUNT(*)         FROM DIM_DRIVER   UNION ALL
SELECT 'DIM_CUSTOMER',         COUNT(*)         FROM DIM_CUSTOMER;
-- Expected: 500, 6, 295, 10000

-- ── Step 3: Load new fact data ────────────────────────────────
-- Snowflake load history prevents re-loading original files.
-- Only the new date-partitioned files get loaded.

COPY INTO FACT_ORDERS
FROM @s3_fulfillment_stage/raw/fact_orders/
FILE_FORMAT = (FORMAT_NAME = 'csv_format')
ON_ERROR = 'CONTINUE';

COPY INTO FACT_ORDER_ITEMS
FROM @s3_fulfillment_stage/raw/fact_order_items/
FILE_FORMAT = (FORMAT_NAME = 'csv_format')
ON_ERROR = 'CONTINUE';

COPY INTO FACT_INVENTORY_SNAPSHOT
FROM @s3_fulfillment_stage/raw/fact_inventory_snapshot/
FILE_FORMAT = (FORMAT_NAME = 'csv_format')
ON_ERROR = 'CONTINUE';

COPY INTO FACT_SHIPMENTS
FROM @s3_fulfillment_stage/raw/fact_shipments/
FILE_FORMAT = (FORMAT_NAME = 'csv_format')
ON_ERROR = 'CONTINUE';

COPY INTO FACT_DELIVERIES
FROM @s3_fulfillment_stage/raw/fact_deliveries/
FILE_FORMAT = (FORMAT_NAME = 'csv_format')
ON_ERROR = 'CONTINUE';

COPY INTO FACT_DRIVER_ACTIVITY
FROM @s3_fulfillment_stage/raw/fact_driver_activity/
FILE_FORMAT = (FORMAT_NAME = 'csv_format')
ON_ERROR = 'CONTINUE';

COPY INTO FACT_EXPERIMENT_ASSIGNMENTS
FROM @s3_fulfillment_stage/raw/fact_experiment_assignments/
FILE_FORMAT = (FORMAT_NAME = 'csv_format')
ON_ERROR = 'CONTINUE';

-- ── Verification ─────────────────────────────────────────────
SELECT 'FACT_ORDERS'              AS tbl, COUNT(*) AS rows, MAX(order_date)     AS max_date FROM FACT_ORDERS
UNION ALL
SELECT 'FACT_ORDER_ITEMS',                COUNT(*),         NULL                            FROM FACT_ORDER_ITEMS
UNION ALL
SELECT 'FACT_INVENTORY_SNAPSHOT',         COUNT(*),         MAX(snapshot_date)              FROM FACT_INVENTORY_SNAPSHOT
UNION ALL
SELECT 'FACT_SHIPMENTS',                  COUNT(*),         MAX(shipment_date)              FROM FACT_SHIPMENTS
UNION ALL
SELECT 'FACT_DELIVERIES',                 COUNT(*),         NULL                            FROM FACT_DELIVERIES
UNION ALL
SELECT 'FACT_DRIVER_ACTIVITY',            COUNT(*),         MAX(activity_date)              FROM FACT_DRIVER_ACTIVITY
UNION ALL
SELECT 'FACT_EXPERIMENT_ASSIGNMENTS',     COUNT(*),         NULL                            FROM FACT_EXPERIMENT_ASSIGNMENTS
ORDER BY 1;
-- Expected max_date for all tables: 2026-02-28