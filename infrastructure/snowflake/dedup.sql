USE DATABASE FULFILLMENT_DB;
USE SCHEMA RAW;
USE WAREHOUSE FULFILLMENT_WH;

-- Deduplicate fact_orders (PK: order_id)
CREATE OR REPLACE TABLE FACT_ORDERS AS
SELECT * FROM FACT_ORDERS
QUALIFY ROW_NUMBER() OVER (PARTITION BY ORDER_ID ORDER BY CREATED_AT DESC) = 1;

-- Deduplicate fact_order_items (PK: order_item_id)
CREATE OR REPLACE TABLE FACT_ORDER_ITEMS AS
SELECT * FROM FACT_ORDER_ITEMS
QUALIFY ROW_NUMBER() OVER (PARTITION BY ORDER_ITEM_ID ORDER BY CREATED_AT DESC) = 1;

-- Deduplicate fact_deliveries (PK: delivery_id)
CREATE OR REPLACE TABLE FACT_DELIVERIES AS
SELECT * FROM FACT_DELIVERIES
QUALIFY ROW_NUMBER() OVER (PARTITION BY DELIVERY_ID ORDER BY CREATED_AT DESC) = 1;

-- Deduplicate fact_shipments (PK: shipment_id)
CREATE OR REPLACE TABLE FACT_SHIPMENTS AS
SELECT * FROM FACT_SHIPMENTS
QUALIFY ROW_NUMBER() OVER (PARTITION BY SHIPMENT_ID ORDER BY CREATED_AT DESC) = 1;

-- Deduplicate fact_experiment_assignments (PK: assignment_id)
CREATE OR REPLACE TABLE FACT_EXPERIMENT_ASSIGNMENTS AS
SELECT * FROM FACT_EXPERIMENT_ASSIGNMENTS
QUALIFY ROW_NUMBER() OVER (PARTITION BY ASSIGNMENT_ID ORDER BY CREATED_AT DESC) = 1;

-- Deduplicate fact_inventory_snapshot (PK: snapshot_date + warehouse_id + product_id)
CREATE OR REPLACE TABLE FACT_INVENTORY_SNAPSHOT AS
SELECT * FROM FACT_INVENTORY_SNAPSHOT
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY SNAPSHOT_DATE, WAREHOUSE_ID, PRODUCT_ID
    ORDER BY CREATED_AT DESC
) = 1;

-- Deduplicate fact_driver_activity (PK: driver_id + activity_date)
CREATE OR REPLACE TABLE FACT_DRIVER_ACTIVITY AS
SELECT * FROM FACT_DRIVER_ACTIVITY
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY DRIVER_ID, ACTIVITY_DATE
    ORDER BY CREATED_AT DESC
) = 1;

-- Verify counts after dedup
SELECT 'fact_orders'               AS tbl, COUNT(*) AS row_count, MAX(order_date)    AS max_date FROM FACT_ORDERS
UNION ALL
SELECT 'fact_order_items',                  COUNT(*),              NULL                           FROM FACT_ORDER_ITEMS
UNION ALL
SELECT 'fact_inventory_snapshot',           COUNT(*),              MAX(snapshot_date)             FROM FACT_INVENTORY_SNAPSHOT
UNION ALL
SELECT 'fact_shipments',                    COUNT(*),              MAX(shipment_date)             FROM FACT_SHIPMENTS
UNION ALL
SELECT 'fact_deliveries',                   COUNT(*),              NULL                           FROM FACT_DELIVERIES
UNION ALL
SELECT 'fact_driver_activity',              COUNT(*),              MAX(activity_date)             FROM FACT_DRIVER_ACTIVITY
UNION ALL
SELECT 'fact_experiment_assignments',       COUNT(*),              NULL                           FROM FACT_EXPERIMENT_ASSIGNMENTS
ORDER BY 1;


SELECT 
    is_forecast,
    COUNT(*) as row_count,
    MIN(date) as min_date,
    MAX(date) as max_date
FROM FULFILLMENT_DB.MARTS.MART_DAILY_PRODUCT_KPIS
GROUP BY 1
ORDER BY 1;