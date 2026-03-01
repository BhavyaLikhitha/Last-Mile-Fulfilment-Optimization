-- copy_into_extension.sql
-- Loads extension backfill (Feb 2025 → Feb 2026) into Snowflake RAW schema.
-- Run AFTER uploading output_extension/raw/ to S3.
--
-- NOTE: Stage URL already includes /raw/ prefix, so paths below do NOT repeat it.
-- Stage: s3://last-mile-fulfillment-platform/raw/
-- Path:  @s3_fulfillment_stage/fact_orders/ = s3://.../raw/fact_orders/

USE DATABASE FULFILLMENT_DB;
USE SCHEMA RAW;
USE WAREHOUSE FULFILLMENT_WH;

-- ── Step 1: Extend dim_date ───────────────────────────────────
-- Appends Feb 2, 2025 → Feb 28, 2026 rows to existing dim_date.
COPY INTO dim_date
FROM @s3_fulfillment_stage/dim_date/
FILE_FORMAT = csv_format
FORCE = TRUE
ON_ERROR = 'CONTINUE';

-- Verify: should be ~1,489 rows (1,097 original + 392 new)
SELECT COUNT(*) AS total_date_rows, MIN(date) AS min_date, MAX(date) AS max_date
FROM dim_date;

-- ── Step 2: Reload updated dimensions (SCD Type 2 changes) ────
TRUNCATE TABLE dim_product;
COPY INTO dim_product
FROM @s3_fulfillment_stage/dim_product/
FILE_FORMAT = csv_format
FORCE = TRUE
ON_ERROR = 'CONTINUE';

TRUNCATE TABLE dim_supplier;
COPY INTO dim_supplier
FROM @s3_fulfillment_stage/dim_supplier/
FILE_FORMAT = csv_format
FORCE = TRUE
ON_ERROR = 'CONTINUE';

TRUNCATE TABLE dim_driver;
COPY INTO dim_driver
FROM @s3_fulfillment_stage/dim_driver/
FILE_FORMAT = csv_format
FORCE = TRUE
ON_ERROR = 'CONTINUE';

TRUNCATE TABLE dim_customer;
COPY INTO dim_customer
FROM @s3_fulfillment_stage/dim_customer/
FILE_FORMAT = csv_format
FORCE = TRUE
ON_ERROR = 'CONTINUE';

-- Verify dimension row counts
SELECT 'dim_product'  AS tbl, COUNT(*) AS row_count FROM dim_product  UNION ALL
SELECT 'dim_supplier',         COUNT(*)              FROM dim_supplier UNION ALL
SELECT 'dim_driver',           COUNT(*)              FROM dim_driver   UNION ALL
SELECT 'dim_customer',         COUNT(*)              FROM dim_customer;
-- Expected: 500, 6, 295, 10000

-- ── Step 3: Load new fact data (2025-2026 only) ───────────────
-- PATTERN matches only date=2025... and date=2026... partitions.
-- Prevents reloading 2022-2024 data. FORCE=TRUE bypasses load history.

COPY INTO fact_orders
FROM @s3_fulfillment_stage/fact_orders/
FILE_FORMAT = csv_format
PATTERN = '.*date=202[5-6].*/data\.csv'
FORCE = TRUE
ON_ERROR = 'CONTINUE';

COPY INTO fact_order_items
FROM @s3_fulfillment_stage/fact_order_items/
FILE_FORMAT = csv_format
PATTERN = '.*date=202[5-6].*/data\.csv'
FORCE = TRUE
ON_ERROR = 'CONTINUE';

COPY INTO fact_inventory_snapshot
FROM @s3_fulfillment_stage/fact_inventory_snapshot/
FILE_FORMAT = csv_format
PATTERN = '.*date=202[5-6].*/data\.csv'
FORCE = TRUE
ON_ERROR = 'CONTINUE';

COPY INTO fact_shipments
FROM @s3_fulfillment_stage/fact_shipments/
FILE_FORMAT = csv_format
PATTERN = '.*date=202[5-6].*/data\.csv'
FORCE = TRUE
ON_ERROR = 'CONTINUE';

COPY INTO fact_deliveries
FROM @s3_fulfillment_stage/fact_deliveries/
FILE_FORMAT = csv_format
PATTERN = '.*date=202[5-6].*/data\.csv'
FORCE = TRUE
ON_ERROR = 'CONTINUE';

COPY INTO fact_driver_activity
FROM @s3_fulfillment_stage/fact_driver_activity/
FILE_FORMAT = csv_format
PATTERN = '.*date=202[5-6].*/data\.csv'
FORCE = TRUE
ON_ERROR = 'CONTINUE';

COPY INTO fact_experiment_assignments
FROM @s3_fulfillment_stage/fact_experiment_assignments/
FILE_FORMAT = csv_format
PATTERN = '.*date=202[5-6].*/data\.csv'
FORCE = TRUE
ON_ERROR = 'CONTINUE';

-- ── Final verification ────────────────────────────────────────
SELECT 'fact_orders'               AS tbl, COUNT(*) AS row_count, MAX(order_date)    AS max_date FROM fact_orders
UNION ALL
SELECT 'fact_order_items',                  COUNT(*),              NULL                           FROM fact_order_items
UNION ALL
SELECT 'fact_inventory_snapshot',           COUNT(*),              MAX(snapshot_date)             FROM fact_inventory_snapshot
UNION ALL
SELECT 'fact_shipments',                    COUNT(*),              MAX(shipment_date)             FROM fact_shipments
UNION ALL
SELECT 'fact_deliveries',                   COUNT(*),              NULL                           FROM fact_deliveries
UNION ALL
SELECT 'fact_driver_activity',              COUNT(*),              MAX(activity_date)             FROM fact_driver_activity
UNION ALL
SELECT 'fact_experiment_assignments',       COUNT(*),              NULL                           FROM fact_experiment_assignments
ORDER BY 1;

-- Expected approximate row counts after extension:
--   fact_orders                 : ~7,500,000
--   fact_order_items            : ~18,000,000
--   fact_inventory_snapshot     : ~5,960,000
--   fact_shipments              : ~215,000
--   fact_deliveries             : ~7,200,000
--   fact_driver_activity        : ~375,000
--   fact_experiment_assignments : ~1,530,000
-- Expected max_date: 2026-02-28


-- Full data verification across all 14 RAW tables + 6 MART tables
SELECT 'dim_product'               AS tbl, COUNT(*) AS row_count, NULL::DATE AS min_date, NULL::DATE AS max_date FROM RAW.DIM_PRODUCT
UNION ALL SELECT 'dim_warehouse',          COUNT(*), NULL, NULL                                                    FROM RAW.DIM_WAREHOUSE
UNION ALL SELECT 'dim_supplier',           COUNT(*), NULL, NULL                                                    FROM RAW.DIM_SUPPLIER
UNION ALL SELECT 'dim_driver',             COUNT(*), NULL, NULL                                                    FROM RAW.DIM_DRIVER
UNION ALL SELECT 'dim_customer',           COUNT(*), NULL, NULL                                                    FROM RAW.DIM_CUSTOMER
UNION ALL SELECT 'dim_date',               COUNT(*), MIN(date), MAX(date)                                          FROM RAW.DIM_DATE
UNION ALL SELECT 'dim_experiments',        COUNT(*), NULL, NULL                                                    FROM RAW.DIM_EXPERIMENTS
UNION ALL SELECT 'fact_orders',            COUNT(*), MIN(order_date), MAX(order_date)                              FROM RAW.FACT_ORDERS
UNION ALL SELECT 'fact_order_items',       COUNT(*), NULL, NULL                                                    FROM RAW.FACT_ORDER_ITEMS
UNION ALL SELECT 'fact_inventory_snapshot',COUNT(*), MIN(snapshot_date), MAX(snapshot_date)                        FROM RAW.FACT_INVENTORY_SNAPSHOT
UNION ALL SELECT 'fact_shipments',         COUNT(*), MIN(shipment_date), MAX(shipment_date)                        FROM RAW.FACT_SHIPMENTS
UNION ALL SELECT 'fact_deliveries',        COUNT(*), NULL, NULL                                                    FROM RAW.FACT_DELIVERIES
UNION ALL SELECT 'fact_driver_activity',   COUNT(*), MIN(activity_date), MAX(activity_date)                        FROM RAW.FACT_DRIVER_ACTIVITY
UNION ALL SELECT 'fact_experiment_assignments', COUNT(*), NULL, NULL                                               FROM RAW.FACT_EXPERIMENT_ASSIGNMENTS
UNION ALL SELECT '--- MARTS ---',          0, NULL, NULL
UNION ALL SELECT 'mart_daily_product_kpis',    COUNT(*), MIN(date), MAX(date)                                      FROM MARTS.MART_DAILY_PRODUCT_KPIS
UNION ALL SELECT 'mart_delivery_performance',  COUNT(*), MIN(date), MAX(date)                                      FROM MARTS.MART_DELIVERY_PERFORMANCE
UNION ALL SELECT 'mart_cost_optimization',     COUNT(*), MIN(date), MAX(date)                                      FROM MARTS.MART_COST_OPTIMIZATION
UNION ALL SELECT 'mart_daily_warehouse_kpis',  COUNT(*), MIN(date), MAX(date)                                      FROM MARTS.MART_DAILY_WAREHOUSE_KPIS
UNION ALL SELECT 'mart_allocation_efficiency', COUNT(*), MIN(date), MAX(date)                                      FROM MARTS.MART_ALLOCATION_EFFICIENCY
UNION ALL SELECT 'mart_experiment_results',    COUNT(*), NULL, NULL                                                FROM MARTS.MART_EXPERIMENT_RESULTS
ORDER BY 1;