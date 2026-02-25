USE DATABASE FULFILLMENT_DB;
USE SCHEMA RAW;
USE WAREHOUSE FULFILLMENT_WH;

-- ============================================================
-- DIMENSION TABLES (no date partitioning)
-- ============================================================

COPY INTO dim_product
FROM @s3_fulfillment_stage/dim_product/
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE';

COPY INTO dim_warehouse
FROM @s3_fulfillment_stage/dim_warehouse/
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE';

COPY INTO dim_supplier
FROM @s3_fulfillment_stage/dim_supplier/
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE';

COPY INTO dim_driver
FROM @s3_fulfillment_stage/dim_driver/
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE';

COPY INTO dim_customer
FROM @s3_fulfillment_stage/dim_customer/
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE';

COPY INTO dim_date
FROM @s3_fulfillment_stage/dim_date/
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE';

COPY INTO dim_experiments
FROM @s3_fulfillment_stage/dim_experiments/
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE';

-- ============================================================
-- FACT TABLES (date-partitioned in S3)
-- ============================================================

COPY INTO fact_orders
FROM @s3_fulfillment_stage/fact_orders/
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE'
PATTERN = '.*data\.csv';

COPY INTO fact_order_items
FROM @s3_fulfillment_stage/fact_order_items/
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE'
PATTERN = '.*data\.csv';

COPY INTO fact_inventory_snapshot
FROM @s3_fulfillment_stage/fact_inventory_snapshot/
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE'
PATTERN = '.*data\.csv';

COPY INTO fact_shipments
FROM @s3_fulfillment_stage/fact_shipments/
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE'
PATTERN = '.*data\.csv';

COPY INTO fact_deliveries
FROM @s3_fulfillment_stage/fact_deliveries/
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE'
PATTERN = '.*data\.csv';

COPY INTO fact_driver_activity
FROM @s3_fulfillment_stage/fact_driver_activity/
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE'
PATTERN = '.*data\.csv';

COPY INTO fact_experiment_assignments
FROM @s3_fulfillment_stage/fact_experiment_assignments/
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE'
PATTERN = '.*data\.csv';



-- VERIFICATION QUERIES
SELECT 'dim_product' AS table_name, COUNT(*) AS row_count FROM dim_product
UNION ALL SELECT 'dim_warehouse', COUNT(*) FROM dim_warehouse
UNION ALL SELECT 'dim_supplier', COUNT(*) FROM dim_supplier
UNION ALL SELECT 'dim_driver', COUNT(*) FROM dim_driver
UNION ALL SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL SELECT 'dim_experiments', COUNT(*) FROM dim_experiments
UNION ALL SELECT 'fact_orders', COUNT(*) FROM fact_orders
UNION ALL SELECT 'fact_order_items', COUNT(*) FROM fact_order_items
UNION ALL SELECT 'fact_inventory_snapshot', COUNT(*) FROM fact_inventory_snapshot
UNION ALL SELECT 'fact_shipments', COUNT(*) FROM fact_shipments
UNION ALL SELECT 'fact_deliveries', COUNT(*) FROM fact_deliveries
UNION ALL SELECT 'fact_driver_activity', COUNT(*) FROM fact_driver_activity
UNION ALL SELECT 'fact_experiment_assignments', COUNT(*) FROM fact_experiment_assignments
ORDER BY table_name;