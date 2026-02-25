-- ============================================================
-- FULFILLMENT PLATFORM — SNOWFLAKE DDL
-- Run this entire script once in Snowflake to set up everything
-- ============================================================

-- ============================================================
-- 1. WAREHOUSE, DATABASE, SCHEMAS
-- ============================================================

CREATE WAREHOUSE IF NOT EXISTS FULFILLMENT_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS FULFILLMENT_DB;

USE DATABASE FULFILLMENT_DB;

CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS INTERMEDIATE;
CREATE SCHEMA IF NOT EXISTS MARTS;

USE SCHEMA RAW;

-- ============================================================
-- 2. DIMENSION TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_product (
    product_id          VARCHAR(20)     PRIMARY KEY,
    product_name        VARCHAR(100)    NOT NULL,
    category            VARCHAR(50)     NOT NULL,
    subcategory         VARCHAR(50),
    cost_price          DECIMAL(10,2)   NOT NULL,
    selling_price       DECIMAL(10,2)   NOT NULL,
    weight_kg           DECIMAL(5,2)    NOT NULL,
    lead_time_days      INTEGER         NOT NULL,
    reorder_point       INTEGER         NOT NULL,
    safety_stock        INTEGER         NOT NULL,
    is_perishable       BOOLEAN         DEFAULT FALSE,
    created_at          TIMESTAMP       NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_warehouse (
    warehouse_id            VARCHAR(20)     PRIMARY KEY,
    warehouse_name          VARCHAR(100)    NOT NULL,
    region                  VARCHAR(50)     NOT NULL,
    city                    VARCHAR(50)     NOT NULL,
    state                   VARCHAR(10)     NOT NULL,
    latitude                DECIMAL(9,6)    NOT NULL,
    longitude               DECIMAL(9,6)    NOT NULL,
    capacity_units          INTEGER         NOT NULL,
    operating_cost_per_day  DECIMAL(10,2)   NOT NULL,
    is_active               BOOLEAN         DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_id         VARCHAR(20)     PRIMARY KEY,
    supplier_name       VARCHAR(100)    NOT NULL,
    region              VARCHAR(50)     NOT NULL,
    average_lead_time   INTEGER         NOT NULL,
    lead_time_std_dev   DECIMAL(5,2)    NOT NULL,
    reliability_score   DECIMAL(3,2)    NOT NULL,
    product_categories  VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS dim_driver (
    driver_id               VARCHAR(20)     PRIMARY KEY,
    warehouse_id            VARCHAR(20)     NOT NULL,
    driver_name             VARCHAR(100)    NOT NULL,
    vehicle_type            VARCHAR(20)     NOT NULL,
    max_delivery_capacity   INTEGER         NOT NULL,
    avg_speed_kmh           DECIMAL(5,2)    NOT NULL,
    availability_status     VARCHAR(20)     NOT NULL,
    hire_date               DATE            NOT NULL,

    CONSTRAINT fk_driver_warehouse FOREIGN KEY (warehouse_id) REFERENCES dim_warehouse(warehouse_id)
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id             VARCHAR(20)     PRIMARY KEY,
    region                  VARCHAR(50)     NOT NULL,
    city                    VARCHAR(50)     NOT NULL,
    customer_segment        VARCHAR(20)     NOT NULL,
    order_frequency_score   DECIMAL(3,2),
    acquisition_date        DATE            NOT NULL,
    latitude                DECIMAL(9,6)    NOT NULL,
    longitude               DECIMAL(9,6)    NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_date (
    date                DATE            PRIMARY KEY,
    day_of_week         VARCHAR(10)     NOT NULL,
    day_of_week_num     INTEGER         NOT NULL,
    week_number         INTEGER         NOT NULL,
    month               INTEGER         NOT NULL,
    month_name          VARCHAR(10)     NOT NULL,
    quarter             INTEGER         NOT NULL,
    year                INTEGER         NOT NULL,
    is_holiday          BOOLEAN         DEFAULT FALSE,
    is_weekend          BOOLEAN         NOT NULL,
    season              VARCHAR(10)     NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_experiments (
    experiment_id       VARCHAR(20)     PRIMARY KEY,
    experiment_name     VARCHAR(200)    NOT NULL,
    strategy_name       VARCHAR(100)    NOT NULL,
    experiment_type     VARCHAR(50)     NOT NULL,
    description         VARCHAR(500),
    start_date          DATE            NOT NULL,
    end_date            DATE,
    target_warehouses   VARCHAR(200),
    status              VARCHAR(20)     NOT NULL
);

-- ============================================================
-- 3. FACT TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS fact_orders (
    order_id                VARCHAR(30)     PRIMARY KEY,
    order_date              DATE            NOT NULL,
    order_timestamp         TIMESTAMP       NOT NULL,
    customer_id             VARCHAR(20)     NOT NULL,
    assigned_warehouse_id   VARCHAR(20)     NOT NULL,
    nearest_warehouse_id    VARCHAR(20)     NOT NULL,
    allocation_strategy     VARCHAR(50)     NOT NULL,
    order_priority          VARCHAR(20)     NOT NULL,
    total_items             INTEGER         NOT NULL,
    total_amount            DECIMAL(10,2)   NOT NULL,
    total_fulfillment_cost  DECIMAL(12,2)   NOT NULL,
    order_status            VARCHAR(20)     NOT NULL,
    return_flag             BOOLEAN         NOT NULL DEFAULT FALSE,
    experiment_id           VARCHAR(20),
    experiment_group        VARCHAR(20),
    created_at              TIMESTAMP       NOT NULL,
    updated_at              TIMESTAMP       NOT NULL,
    batch_id                VARCHAR(50)     NOT NULL,

    CONSTRAINT fk_order_date FOREIGN KEY (order_date) REFERENCES dim_date(date),
    CONSTRAINT fk_order_customer FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    CONSTRAINT fk_order_assigned_wh FOREIGN KEY (assigned_warehouse_id) REFERENCES dim_warehouse(warehouse_id),
    CONSTRAINT fk_order_nearest_wh FOREIGN KEY (nearest_warehouse_id) REFERENCES dim_warehouse(warehouse_id),
    CONSTRAINT fk_order_experiment FOREIGN KEY (experiment_id) REFERENCES dim_experiments(experiment_id)
);

CREATE TABLE IF NOT EXISTS fact_order_items (
    order_item_id       VARCHAR(30)     PRIMARY KEY,
    order_id            VARCHAR(30)     NOT NULL,
    product_id          VARCHAR(20)     NOT NULL,
    quantity            INTEGER         NOT NULL,
    unit_price          DECIMAL(10,2)   NOT NULL,
    discount_amount     DECIMAL(10,2)   DEFAULT 0,
    revenue             DECIMAL(10,2)   NOT NULL,
    created_at          TIMESTAMP       NOT NULL,
    updated_at          TIMESTAMP       NOT NULL,
    batch_id            VARCHAR(50)     NOT NULL,

    CONSTRAINT fk_item_order FOREIGN KEY (order_id) REFERENCES fact_orders(order_id),
    CONSTRAINT fk_item_product FOREIGN KEY (product_id) REFERENCES dim_product(product_id)
);

CREATE TABLE IF NOT EXISTS fact_inventory_snapshot (
    snapshot_date               DATE            NOT NULL,
    warehouse_id                VARCHAR(20)     NOT NULL,
    product_id                  VARCHAR(20)     NOT NULL,
    opening_stock               INTEGER         NOT NULL,
    units_sold                  INTEGER         NOT NULL DEFAULT 0,
    units_received              INTEGER         NOT NULL DEFAULT 0,
    units_returned              INTEGER         NOT NULL DEFAULT 0,
    closing_stock               INTEGER         NOT NULL,
    stockout_flag               BOOLEAN         NOT NULL,
    below_safety_stock_flag     BOOLEAN         NOT NULL,
    reorder_triggered_flag      BOOLEAN         NOT NULL,
    units_on_order              INTEGER         NOT NULL DEFAULT 0,
    days_of_supply              DECIMAL(5,2),
    holding_cost                DECIMAL(10,2)   NOT NULL,
    inventory_value             DECIMAL(12,2)   NOT NULL,
    created_at                  TIMESTAMP       NOT NULL,
    updated_at                  TIMESTAMP       NOT NULL,
    batch_id                    VARCHAR(50)     NOT NULL,

    CONSTRAINT pk_inventory_snapshot PRIMARY KEY (snapshot_date, warehouse_id, product_id),
    CONSTRAINT fk_inv_date FOREIGN KEY (snapshot_date) REFERENCES dim_date(date),
    CONSTRAINT fk_inv_warehouse FOREIGN KEY (warehouse_id) REFERENCES dim_warehouse(warehouse_id),
    CONSTRAINT fk_inv_product FOREIGN KEY (product_id) REFERENCES dim_product(product_id)
);

CREATE TABLE IF NOT EXISTS fact_shipments (
    shipment_id             VARCHAR(30)     PRIMARY KEY,
    supplier_id             VARCHAR(20)     NOT NULL,
    warehouse_id            VARCHAR(20)     NOT NULL,
    product_id              VARCHAR(20)     NOT NULL,
    quantity                INTEGER         NOT NULL,
    shipment_cost           DECIMAL(10,2)   NOT NULL,
    shipment_date           DATE            NOT NULL,
    expected_arrival_date   DATE            NOT NULL,
    actual_arrival_date     DATE,
    delay_days              INTEGER,
    delay_flag              BOOLEAN         NOT NULL DEFAULT FALSE,
    reorder_triggered_flag  BOOLEAN         NOT NULL,
    created_at              TIMESTAMP       NOT NULL,
    updated_at              TIMESTAMP       NOT NULL,
    batch_id                VARCHAR(50)     NOT NULL,

    CONSTRAINT fk_ship_supplier FOREIGN KEY (supplier_id) REFERENCES dim_supplier(supplier_id),
    CONSTRAINT fk_ship_warehouse FOREIGN KEY (warehouse_id) REFERENCES dim_warehouse(warehouse_id),
    CONSTRAINT fk_ship_product FOREIGN KEY (product_id) REFERENCES dim_product(product_id)
);

CREATE TABLE IF NOT EXISTS fact_deliveries (
    delivery_id             VARCHAR(30)     PRIMARY KEY,
    order_id                VARCHAR(30)     NOT NULL,
    driver_id               VARCHAR(20)     NOT NULL,
    warehouse_id            VARCHAR(20)     NOT NULL,
    assigned_time           TIMESTAMP       NOT NULL,
    pickup_time             TIMESTAMP,
    delivered_time          TIMESTAMP,
    estimated_eta_minutes   DECIMAL(6,2)    NOT NULL,
    actual_delivery_minutes DECIMAL(6,2),
    distance_km             DECIMAL(8,2)    NOT NULL,
    delivery_cost           DECIMAL(10,2)   NOT NULL,
    delivery_status         VARCHAR(20)     NOT NULL,
    on_time_flag            BOOLEAN,
    sla_minutes             INTEGER         NOT NULL,
    sla_breach_flag         BOOLEAN,
    created_at              TIMESTAMP       NOT NULL,
    updated_at              TIMESTAMP       NOT NULL,
    batch_id                VARCHAR(50)     NOT NULL,

    CONSTRAINT fk_del_order FOREIGN KEY (order_id) REFERENCES fact_orders(order_id),
    CONSTRAINT fk_del_driver FOREIGN KEY (driver_id) REFERENCES dim_driver(driver_id),
    CONSTRAINT fk_del_warehouse FOREIGN KEY (warehouse_id) REFERENCES dim_warehouse(warehouse_id)
);

CREATE TABLE IF NOT EXISTS fact_driver_activity (
    driver_id               VARCHAR(20)     NOT NULL,
    activity_date           DATE            NOT NULL,
    warehouse_id            VARCHAR(20)     NOT NULL,
    deliveries_completed    INTEGER         NOT NULL,
    total_distance_km       DECIMAL(8,2)    NOT NULL,
    total_active_hours      DECIMAL(5,2)    NOT NULL,
    idle_hours              DECIMAL(5,2)    NOT NULL,
    utilization_pct         DECIMAL(5,2)    NOT NULL,
    created_at              TIMESTAMP       NOT NULL,
    updated_at              TIMESTAMP       NOT NULL,
    batch_id                VARCHAR(50)     NOT NULL,

    CONSTRAINT pk_driver_activity PRIMARY KEY (driver_id, activity_date),
    CONSTRAINT fk_da_driver FOREIGN KEY (driver_id) REFERENCES dim_driver(driver_id),
    CONSTRAINT fk_da_date FOREIGN KEY (activity_date) REFERENCES dim_date(date),
    CONSTRAINT fk_da_warehouse FOREIGN KEY (warehouse_id) REFERENCES dim_warehouse(warehouse_id)
);

CREATE TABLE IF NOT EXISTS fact_experiment_assignments (
    assignment_id       VARCHAR(30)     PRIMARY KEY,
    experiment_id       VARCHAR(20)     NOT NULL,
    order_id            VARCHAR(30)     NOT NULL,
    group_name          VARCHAR(20)     NOT NULL,
    assigned_at         TIMESTAMP       NOT NULL,
    warehouse_id        VARCHAR(20)     NOT NULL,
    created_at          TIMESTAMP       NOT NULL,
    updated_at          TIMESTAMP       NOT NULL,
    batch_id            VARCHAR(50)     NOT NULL,

    CONSTRAINT fk_ea_experiment FOREIGN KEY (experiment_id) REFERENCES dim_experiments(experiment_id),
    CONSTRAINT fk_ea_order FOREIGN KEY (order_id) REFERENCES fact_orders(order_id),
    CONSTRAINT fk_ea_warehouse FOREIGN KEY (warehouse_id) REFERENCES dim_warehouse(warehouse_id)
);

-- ============================================================
-- 4. FILE FORMAT FOR S3 INGESTION
-- ============================================================

CREATE FILE FORMAT IF NOT EXISTS csv_format
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    COMPRESSION = 'AUTO';

-- ============================================================
-- 5. EXTERNAL STAGE (S3)
-- ============================================================

CREATE STAGE IF NOT EXISTS s3_fulfillment_stage
    URL = 's3://last-mile-fulfillment-platform/raw/'
    CREDENTIALS = (AWS_KEY_ID = 'REPLACE_WITH_YOUR_KEY' AWS_SECRET_KEY = 'REPLACE_WITH_YOUR_SECRET')
    FILE_FORMAT = csv_format;

-- ============================================================
-- SETUP COMPLETE
-- Tables: 7 Dimensions + 7 Facts = 14 (in RAW schema)
-- Marts (6) will be created by dbt in MARTS schema
-- CHECK constraints handled by simulation code + dbt tests
-- Foreign keys are informational only (not enforced by Snowflake)
-- ============================================================