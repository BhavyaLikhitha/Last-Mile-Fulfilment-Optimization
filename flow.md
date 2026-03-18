# Data Flow Guide — Last-Mile Fulfillment Platform

A step-by-step walkthrough of how data flows through the entire platform, from generation to dashboard. Use this as a reference whenever you need to understand what happens at each stage.

---

## The Big Picture (30-Second Version)

```
Lambda generates fake fulfillment data every day
  → lands as CSV files in S3
  → (optionally) Kafka streams the same data in real-time
  → Airflow DAG picks it up automatically
  → Great Expectations validates the CSVs
  → Snowflake loads and deduplicates the data
  → dbt transforms RAW → STAGING → INTERMEDIATE → MARTS
  → Great Expectations validates the marts
  → Spark builds ML features at scale
  → ML models score predictions and write back to Snowflake
  → Optimization + Experimentation engines run
  → Power BI dashboard refreshes from Snowflake marts
```

---

## Detailed Flow With a Real Example

Let's trace **one day of data** — say **March 17, 2026** — through the entire system.

---

### Stage 1: Data Generation (Lambda)

**What triggers it?**
EventBridge has a cron rule: `cron(0 2 * * ? *)` — fires every day at **2:00 AM UTC**.

**What happens?**
1. EventBridge sends `{"mode": "daily"}` to Lambda function `fulfillment-data-generator`
2. Lambda reads the last state from `s3://last-mile-fulfillment-platform/state/latest_state.json`
   - This state contains: last date generated, inventory closing stocks, ID counters
3. Lambda generates **1 day of data** (March 17, 2026):

   | Table | Rows | Example |
   |---|---|---|
   | `fact_orders` | ~5,000-8,000 | ORD-20260317-00001, customer CUST-042, assigned to WH-001 |
   | `fact_order_items` | ~10,000-16,000 | 2x PRD-042 at $89.99, 1x PRD-105 at $24.50 |
   | `fact_inventory_snapshot` | 4,000 | WH-001 × PRD-042: opened with 150 units, sold 12, closed with 138 |
   | `fact_deliveries` | ~4,000-7,000 | DEL-10000042, driver DRV-042, 12.5km, 42 minutes |
   | `fact_shipments` | ~500 | Supplier SUP-003 ships 200 units of PRD-042 to WH-001 |
   | `fact_driver_activity` | ~300 | DRV-042: 8 deliveries, 94% utilization, 6.2 hours active |
   | `fact_experiment_assignments` | ~2,000 | CUST-042 assigned to EXP-007 treatment group |

4. Lambda uploads each table as a CSV to S3 with date-partitioned paths:
   ```
   s3://last-mile-fulfillment-platform/raw/fact_orders/date=2026-03-17/data.csv
   s3://last-mile-fulfillment-platform/raw/fact_deliveries/date=2026-03-17/data.csv
   s3://last-mile-fulfillment-platform/raw/fact_inventory_snapshot/date=2026-03-17/data.csv
   ... (7 files total)
   ```

5. Lambda saves updated state (closing stocks become tomorrow's opening stocks):
   ```
   s3://last-mile-fulfillment-platform/state/latest_state.json
   ```

**Time taken:** ~2-3 minutes for 1 day, ~15-20 minutes for 7 days (weekly mode)

---

### Stage 2: Kafka Streaming (Optional Path)

If the Airflow Variable `DATA_SOURCE` is set to `kafka`, the data takes a streaming detour:

```
                                    ┌─────────────┐
S3 CSVs (from Lambda)  ───────────>│   Producer   │
                                    └──────┬──────┘
                                           │ publishes row-by-row
                        ┌──────────────────┼──────────────────┐
                        ▼                  ▼                  ▼
              fulfillment.orders   fulfillment.deliveries  fulfillment.inventory
              .created             .updated                .snapshot
              (8 partitions)       (8 partitions)          (8 partitions)
                        │                  │                  │
                        └──────────────────┼──────────────────┘
                                           │ consumes in batches
                                    ┌──────┴──────┐
                                    │  Consumer   │
                                    │  (S3 writer)│
                                    └──────┬──────┘
                                           │ writes CSVs in same format
                                           ▼
                          s3://bucket/raw/fact_orders/date=2026-03-17/data.csv
```

**How it works:**
1. `streaming/producer.py` reads the CSV from S3, converts each row to a JSON event
2. Events are published to 3 Kafka topics with message keys (order_id, delivery_id, warehouse-product)
3. `streaming/consumer_s3.py` batches messages by topic and date
4. When batch is full (5000 messages) or timeout, writes a CSV to S3 in the **exact same path format** as Lambda
5. The rest of the pipeline doesn't know or care whether the CSV came from Lambda directly or via Kafka

**Why both paths?** Lambda → S3 is simpler and reliable. Kafka demonstrates streaming skills. The `BranchPythonOperator` in Airflow lets you switch between them without changing anything downstream.

---

### Stage 3: Airflow DAG Kicks In

**When?** The DAG schedule is `35 16 * * *` (4:35 PM UTC daily). Or you trigger it manually.

**What does the branch operator do?**
```python
# Task 1: branch_data_source
# Checks Airflow Variable "DATA_SOURCE"
if DATA_SOURCE == "kafka":
    → run consume_from_kafka task (drains Kafka topics, writes to S3)
else:
    → run wait_for_s3_files task (S3KeySensor waits for Lambda's CSV)
```

Both paths produce the same result: CSV files in S3. The pipeline converges at the next step.

---

### Stage 4: Great Expectations — S3 Landing Validation

**Task:** `gx_validate_s3_landing`

Before loading anything into Snowflake, GX validates the raw CSVs:

```
✓ fact_orders CSV has exactly 18 expected columns in correct order
✓ Row count between 3,000 and 12,000 (catches empty files or Lambda bugs)
✓ All order_ids match pattern ORD-XXXXX
✓ order_status values are in {Delivered, Shipped, Processing, Pending, Cancelled}
✓ total_amount > 0.01 (no zero-dollar orders)
✓ warehouse_ids match WH-XXX pattern

✓ fact_deliveries distance_km between 0.1 and 100 (catches the old 150km NYC bug)
✓ fact_inventory closing_stock >= 0 (no negative inventory)
```

**If any check fails:** DAG stops here. No bad data enters Snowflake.

---

### Stage 5: Snowflake Load (COPY INTO)

**Task:** `copy_into_snowflake`

```sql
-- For each of the 7 fact tables:
COPY INTO FACT_ORDERS
FROM @s3_fulfillment_stage/fact_orders/date=2026-03-17/
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE'
FORCE = TRUE;
```

**Why FORCE=TRUE?** Snowflake tracks which files it already loaded. If the DAG retries (e.g., after a failure), FORCE ensures the file is always loaded, even if Snowflake's load history says "already done." The dedup step handles any duplicates.

**Result:** ~27,000 new rows across 7 tables in `FULFILLMENT_DB.RAW`

---

### Stage 6: Deduplication

**Task:** `dedup_snowflake`

```sql
CREATE OR REPLACE TABLE FACT_ORDERS AS
SELECT * FROM FACT_ORDERS
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id
    ORDER BY created_at DESC
) = 1;
```

This keeps only the latest version of each row. If FORCE=TRUE loaded the same file twice, dedup removes the duplicate.

---

### Stage 7: Verify Row Counts

**Task:** `verify_row_counts`

Quick sanity check: are there rows for today? Are the dates correct?

---

### Stage 8: Great Expectations — RAW Load Validation

**Task:** `gx_validate_raw_load`

Now validates the data **inside Snowflake** after COPY INTO + dedup:

```
✓ ORDER_ID is unique (dedup worked correctly)
✓ DELIVERY_ID is unique
✓ Inventory has compound uniqueness on (SNAPSHOT_DATE, WAREHOUSE_ID, PRODUCT_ID)
✓ All 8 warehouses present: WH-001 through WH-008
✓ SLA_MINUTES values are in {240, 480, 2880} (Same-Day, Express, Standard)
```

---

### Stage 9: dbt Transformation Pipeline

Three dbt tasks run in sequence:

**Task 5: `dbt_snapshot`** — SCD Type 2 detection
```
Checks if any dimension rows changed (price updates, driver status, customer segment upgrades).
If product PRD-042's price changed from $89.99 to $94.99:
  → Old row gets dbt_valid_to = 2026-03-17
  → New row gets dbt_valid_from = 2026-03-17, dbt_valid_to = NULL
```

**Task 6: `dbt_run`** — 23 models refresh
```
RAW (14 tables)
  → STAGING (14 views) — type casting, null handling, renaming
    → INTERMEDIATE (3 tables) — enriched joins
      → MARTS (6 incremental tables) — aggregated analytics

Example flow for one order:
  RAW.FACT_ORDERS (raw CSV row)
    → STAGING.STG_ORDERS (cleaned, typed)
      → INTERMEDIATE.INT_ORDER_ENRICHED (joined with customer, warehouse, product data)
        → MARTS.MART_DAILY_WAREHOUSE_KPIS (aggregated: WH-001 had 1,200 orders today, $450K revenue)
        → MARTS.MART_DAILY_PRODUCT_KPIS (aggregated: PRD-042 sold 45 units across all warehouses)
```

**Task 7: `dbt_test`** — 88 data quality tests
```
Generic tests: uniqueness, not-null, relationships, accepted_values
Singular tests: SLA breach logic, inventory balance equation, revenue consistency, cost savings math
```

---

### Stage 10: Post-Processing

**Task:** `post_processing`

Injects realistic warehouse-specific variance into mart columns. Without this, all warehouses would look identical because the simulation generates uniform data.

```sql
-- Example: NYC (WH-001) has harder delivery conditions than Denver (WH-007)
UPDATE MART_DELIVERY_PERFORMANCE SET
  avg_delivery_time_min = CASE
    WHEN warehouse_id = 'WH-001' THEN 1100  -- NYC: congested, slow
    WHEN warehouse_id = 'WH-007' THEN 620   -- Denver: suburban, fast
    ...
  END,
  on_time_pct = CASE
    WHEN warehouse_id = 'WH-001' THEN 58    -- NYC: hard to deliver on time
    WHEN warehouse_id = 'WH-007' THEN 96    -- Denver: easy market
    ...
  END
WHERE date = '2026-03-17';
```

Also applies seasonal adjustments: December +25% delivery times, January -18%.

---

### Stage 11: Great Expectations — Mart Validation

**Task:** `gx_validate_marts`

Final quality gate before ML and analytics consume the data:

```
✓ All 8 warehouses present in mart_delivery_performance
✓ avg_delivery_time_min mean between 500-1500 (post-processing applied correctly)
✓ on_time_pct between 50-100%
✓ savings_amount > 0 in cost_optimization (optimization always saves something)
✓ baseline_total_cost > optimized_total_cost (99% of rows)
✓ service_level_pct between 0-100 in warehouse KPIs
```

---

### Stage 12: Spark Feature Engineering

**Task:** `spark_demand_features` then `spark_eta_features`

PySpark reads from Snowflake, builds features using Window functions, writes back:

```
Snowflake MARTS.MART_DAILY_PRODUCT_KPIS (750K rows)
  → Spark reads via Snowflake connector
  → Window functions compute:
      - 5 lag features (1d, 3d, 7d, 14d, 28d) per product
      - 6 rolling avg/std (7d, 14d, 30d windows) per product
      - 2 rolling min/max (7d window)
      - 2 trend features (week-over-week, month-over-month)
      - Price, inventory, date, category features
  → 24 total features per row
  → Writes to Snowflake STAGING.SPARK_DEMAND_FEATURES

Snowflake INTERMEDIATE.INT_DELIVERY_ENRICHED (5M+ rows)
  → Spark reads entire table (no year-by-year chunking needed)
  → Computes driver historical stats, warehouse stats via groupBy
  → Builds distance, time-of-day, priority features
  → 20 total features per row
  → Writes to Snowflake STAGING.SPARK_ETA_FEATURES
```

**Why Spark instead of pandas?**
- Pandas ETA features had to chunk by year to avoid OOM on 5M rows
- Pandas demand features used `groupby().transform(lambda x: x.rolling(...))` — single-threaded
- Spark Window functions do the same math but distributed across workers

---

### Stage 13: ML Scoring

Three ML tasks run in sequence:

**`ml_demand_stockout`:**
```
Reads MART_DAILY_PRODUCT_KPIS (only rows after last scored date)
  → Builds features (60-day lookback for lag computation)
  → XGBoost predicts demand_forecast for each product × day
  → XGBoost classifier predicts stockout_risk_score (0-1)
  → Bulk MERGE writes predictions back to the same mart table
```

**`ml_eta`:**
```
Reads INT_DELIVERY_ENRICHED (only new deliveries)
  → Builds features (distance, driver stats, time-of-day)
  → LightGBM predicts predicted_eta for each delivery
  → Aggregates to warehouse × day grain (avg predicted_eta)
  → Bulk MERGE writes to MART_DELIVERY_PERFORMANCE
```

**`ml_future_demand`:**
```
Creates a 180-day forward spine: 500 products × 180 days = 90,000 rows
  → Seeds with last 60 days of history for lag features
  → XGBoost predicts demand_forecast for each future day
  → Tags with forecast_horizon (30d, 60d, 90d, 180d)
  → Marks is_forecast=TRUE, total_units_sold=NULL
  → Bulk MERGE writes to MART_DAILY_PRODUCT_KPIS with versioning key
```

**Bulk MERGE pattern (used by all ML writebacks):**
```
1. Write predictions to local CSV
2. PUT file to Snowflake internal stage (@%temp_table)
3. COPY INTO temp table from stage
4. MERGE INTO mart_table USING temp_table ON (date, product_id)
   WHEN MATCHED THEN UPDATE SET demand_forecast = ...
   WHEN NOT MATCHED THEN INSERT ...
5. Drop temp table, delete local CSV
```

This pattern takes **~100 seconds for 733K rows** vs 10+ hours with row-by-row INSERT.

---

### Stage 14: Optimization

**Task:** `run_optimization`

```
Reads MART_DAILY_WAREHOUSE_KPIS (baseline costs per warehouse per day)
  → Computes baseline_total_cost from mart data
  → Applies optimization models:
      - EOQ (Economic Order Quantity) for each product
      - Safety stock levels based on demand variance
      - Allocation efficiency (% orders assigned to nearest warehouse)
  → Calculates optimized_total_cost and savings_amount
  → Bulk MERGE writes to MART_COST_OPTIMIZATION

Result: $297M savings identified (8.2% of $3.6B baseline)
```

---

### Stage 15: Experimentation

**Task:** `run_experimentation`

```
Reads experiment assignments + order costs
  → Groups by experiment × control/treatment
  → Runs Welch t-test for each experiment:
      H0: mean(control) == mean(treatment)
      H1: mean(control) != mean(treatment)
  → Computes p-value, confidence interval, lift_pct
  → Segment-level uplift by customer tier, region, order priority
  → Bulk MERGE writes to MART_EXPERIMENT_RESULTS

Result: 9/10 experiments significant, best lift -22.1% (EXP-007: JIT reorder)
```

---

### Stage 16: Pipeline Complete

DAG marks success. Power BI can now refresh from the updated marts.

---

## Complete Data Flow Diagram

```
                          ┌──────────────────────────────────────────────┐
                          │           EventBridge (2am UTC)              │
                          └─────────────────┬────────────────────────────┘
                                            │ triggers
                                            ▼
                          ┌──────────────────────────────────────────────┐
                          │        AWS Lambda (data generator)           │
                          │  Generates 27K rows/day across 7 fact tables │
                          │  Reads/writes state from S3                  │
                          └─────────────────┬────────────────────────────┘
                                            │ uploads CSVs
                                            ▼
                          ┌──────────────────────────────────────────────┐
                          │     S3 (date-partitioned CSVs)               │
                          │  raw/fact_orders/date=2026-03-17/data.csv    │
                          └──────────┬──────────────────┬────────────────┘
                                     │                  │
                              (batch path)        (streaming path)
                                     │                  │
                                     │                  ▼
                                     │    ┌──────────────────────────┐
                                     │    │    Kafka Producer        │
                                     │    │  Replays CSVs as events  │
                                     │    └──────────┬───────────────┘
                                     │               ▼
                                     │    ┌──────────────────────────┐
                                     │    │  3 Kafka Topics          │
                                     │    │  orders │ deliveries │   │
                                     │    │  inventory (8 parts ea)  │
                                     │    └──────────┬───────────────┘
                                     │               ▼
                                     │    ┌──────────────────────────┐
                                     │    │    Kafka Consumer        │
                                     │    │  Writes CSVs to S3      │
                                     │    │  (same path format)     │
                                     │    └──────────┬───────────────┘
                                     │               │
                                     └───────┬───────┘
                                             │ (both paths produce same CSVs)
                    ┌────────────────────────────────────────────────────────┐
                    │                 AIRFLOW DAG (20 tasks)                 │
                    ├────────────────────────────────────────────────────────┤
                    │                                                        │
                    │  ┌─── GX Validate S3 Landing ───┐                     │
                    │  │ Schema, row counts, value sets │                    │
                    │  └──────────────┬────────────────┘                    │
                    │                 ▼                                      │
                    │  ┌─── Snowflake COPY INTO ───────┐                    │
                    │  │ 7 fact tables, FORCE=TRUE      │                   │
                    │  └──────────────┬────────────────┘                    │
                    │                 ▼                                      │
                    │  ┌─── Dedup + Verify ────────────┐                    │
                    │  │ ROW_NUMBER() dedup, row counts │                   │
                    │  └──────────────┬────────────────┘                    │
                    │                 ▼                                      │
                    │  ┌─── GX Validate RAW Load ──────┐                    │
                    │  │ Uniqueness, completeness       │                   │
                    │  └──────────────┬────────────────┘                    │
                    │                 ▼                                      │
                    │  ┌─── dbt (snapshot + run + test)─┐                   │
                    │  │ SCD Type 2 → 23 models → 88    │                   │
                    │  │ tests across all layers         │                   │
                    │  │                                 │                   │
                    │  │ RAW → STAGING → INTERMEDIATE    │                   │
                    │  │      → MARTS                    │                   │
                    │  └──────────────┬────────────────┘                    │
                    │                 ▼                                      │
                    │  ┌─── Post-Processing ───────────┐                    │
                    │  │ Warehouse-specific adjustments │                    │
                    │  │ Seasonal variance injection    │                    │
                    │  └──────────────┬────────────────┘                    │
                    │                 ▼                                      │
                    │  ┌─── GX Validate Marts ─────────┐                    │
                    │  │ Distribution, cross-table      │                   │
                    │  └──────────────┬────────────────┘                    │
                    │                 ▼                                      │
                    │  ┌─── Spark Feature Engineering ──┐                   │
                    │  │ PySpark Window functions        │                   │
                    │  │ 24 demand + 20 ETA features    │                   │
                    │  │ Writes to STAGING tables        │                   │
                    │  └──────────────┬────────────────┘                    │
                    │                 ▼                                      │
                    │  ┌─── ML Scoring ────────────────┐                    │
                    │  │ XGBoost demand (MAPE 1.72%)   │                    │
                    │  │ LightGBM ETA (R² 0.96)        │                   │
                    │  │ XGBoost stockout risk          │                   │
                    │  │ 180-day future forecast        │                   │
                    │  │ Bulk MERGE writeback           │                    │
                    │  └──────────────┬────────────────┘                    │
                    │                 ▼                                      │
                    │  ┌─── Optimization ──────────────┐                    │
                    │  │ EOQ + safety stock + allocation │                   │
                    │  │ $297M savings (8.2%)           │                    │
                    │  └──────────────┬────────────────┘                    │
                    │                 ▼                                      │
                    │  ┌─── Experimentation ───────────┐                    │
                    │  │ Welch t-tests, 10 experiments  │                   │
                    │  │ Segment-level uplift analysis  │                    │
                    │  └──────────────┬────────────────┘                    │
                    │                 ▼                                      │
                    │            PIPELINE COMPLETE                           │
                    └────────────────────────────────────────────────────────┘
                                             │
                                             ▼
                          ┌──────────────────────────────────────────────┐
                          │         Power BI Dashboard (6 pages)         │
                          │  Live Snowflake connection to MARTS          │
                          │  Auto-refresh on query                       │
                          └──────────────────────────────────────────────┘
```

---

## Snowflake Data Journey (One Order)

Tracing order `ORD-20260317-00001` through every Snowflake layer:

```
LAYER 1 — RAW (loaded from S3)
┌────────────────────────────────────────────────────────────────────┐
│ RAW.FACT_ORDERS                                                    │
│ order_id: ORD-20260317-00001                                       │
│ customer_id: CUST-042                                              │
│ assigned_warehouse_id: WH-001                                      │
│ total_amount: 245.50                                               │
│ order_priority: Express                                            │
│ allocation_strategy: cost_optimal                                  │
└────────────────────────────────────┬───────────────────────────────┘
                                     │ dbt: stg_orders (view)
                                     ▼
LAYER 2 — STAGING (cleaned, typed)
┌────────────────────────────────────────────────────────────────────┐
│ STAGING.STG_ORDERS                                                 │
│ Same data, but:                                                    │
│  - order_date cast to DATE type                                    │
│  - total_amount cast to DECIMAL(10,2)                              │
│  - NULL handling applied                                           │
└────────────────────────────────────┬───────────────────────────────┘
                                     │ dbt: int_order_enriched (table)
                                     ▼
LAYER 3 — INTERMEDIATE (enriched with joins)
┌────────────────────────────────────────────────────────────────────┐
│ INTERMEDIATE.INT_ORDER_ENRICHED                                    │
│ Everything from staging, plus:                                     │
│  + customer_name: "Jane Doe"                                       │
│  + customer_segment: "Premium"                                     │
│  + warehouse_city: "New York"                                      │
│  + product details for each order item                             │
│  + is_cross_region: TRUE (nearest was WH-003 but assigned WH-001) │
└────────────────────────────────────┬───────────────────────────────┘
                                     │ dbt: mart_daily_warehouse_kpis
                                     │      mart_daily_product_kpis
                                     ▼
LAYER 4 — MARTS (aggregated analytics)
┌────────────────────────────────────────────────────────────────────┐
│ MARTS.MART_DAILY_WAREHOUSE_KPIS (1 row per warehouse per day)     │
│ warehouse_id: WH-001, date: 2026-03-17                            │
│ total_orders: 1,247                                                │
│ total_revenue: $456,230                                            │
│ avg_order_value: $365.86                                           │
│ service_level_pct: 92.3%                                           │
├────────────────────────────────────────────────────────────────────┤
│ MARTS.MART_DAILY_PRODUCT_KPIS (1 row per product per day)         │
│ product_id: PRD-042, date: 2026-03-17                              │
│ total_units_sold: 45                                               │
│ demand_forecast: 43.2  (filled by ML)                              │
│ stockout_risk_score: 0.12  (filled by ML)                          │
│ forecast_error: 1.8  (filled by ML)                                │
└────────────────────────────────────────────────────────────────────┘
```

---

## Infrastructure Flow (Terraform + CI/CD)

### How Terraform Manages AWS Resources

```
terraform/
  modules/iam/         → IAM role + policy for Lambda
  modules/s3/          → S3 bucket + encryption + lifecycle
  modules/lambda/      → Lambda function + CloudWatch logs
  modules/eventbridge/ → Daily + weekly schedule rules

State stored in:
  s3://last-mile-fulfillment-tf-state/terraform.tfstate
  DynamoDB table: terraform-locks (prevents concurrent applies)
```

### CI/CD Flow on Git Push

```
You run: git push origin main

GitHub Actions triggers TWO workflows simultaneously:

┌─── ci.yml (3 parallel jobs) ──────────────────────────┐
│                                                        │
│  Job 1: lint-and-test          Job 2: dbt-test         │
│  ├─ ruff check .               ├─ pip install dbt      │
│  ├─ ruff format --check .      └─ dbt test             │
│  └─ pytest tests/ -v              (against Snowflake)  │
│                                                        │
│  Job 3: terraform-plan                                 │
│  ├─ terraform init                                     │
│  ├─ terraform validate                                 │
│  └─ terraform plan                                     │
└────────────────────────────────────────────────────────┘

┌─── deploy.yml (2 sequential jobs) ────────────────────┐
│                                                        │
│  Job 1: terraform-apply                                │
│  └─ terraform apply -auto-approve                      │
│         │                                              │
│         ▼                                              │
│  Job 2: deploy-lambda                                  │
│  ├─ pip install numpy pandas (linux wheels)            │
│  ├─ zip data_simulation/ + config/ + lambda_handler    │
│  └─ aws lambda update-function-code                    │
└────────────────────────────────────────────────────────┘
```

---

## Hosting on Oracle Cloud (OCI) — Running Docker

### Why Oracle Cloud?

Oracle Cloud Free Tier gives you an **Always Free** ARM VM with 4 CPUs and 24GB RAM — enough to run the full Docker stack (14 services need ~8-10GB). AWS free tier VMs are too small (1GB RAM).

### Setup Steps

**1. Create an OCI Compute Instance**
- Shape: `VM.Standard.A1.Flex` (ARM — Always Free)
- CPU: 4 OCPUs
- RAM: 24 GB
- Boot volume: 100 GB
- OS: Ubuntu 22.04 (or Oracle Linux 8)

**2. SSH into the instance**
```bash
ssh -i ~/.ssh/oci_key ubuntu@<public-ip>
```

**3. Install Docker + Docker Compose**
```bash
# Ubuntu
sudo apt update && sudo apt install -y docker.io docker-compose-v2
sudo usermod -aG docker $USER
# Log out and back in for group change to take effect
```

**4. Clone the repo**
```bash
git clone https://github.com/YourUsername/Last-Mile-Fulfilment-Optimization.git
cd Last-Mile-Fulfilment-Optimization
```

**5. Create the .env file**
```bash
cp .env.example .env
nano .env
# Fill in your AWS and Snowflake credentials
# Add: AIRFLOW__CORE__FERNET_KEY=<generate one>
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

**6. Build and start (ARM-compatible images)**
```bash
# Build custom Airflow image
docker compose build

# Start all 14 services
docker compose up -d

# Check everything is healthy
docker compose ps
```

**7. Open firewall ports**

In the OCI Console → Networking → Virtual Cloud Network → Security List, add ingress rules:

| Port | Service | Access |
|---|---|---|
| 8081 | Airflow UI | Your IP only |
| 8082 | Kafka UI | Your IP only |
| 8083 | Spark UI | Your IP only |
| 22 | SSH | Your IP only |

Then in the VM firewall:
```bash
sudo iptables -I INPUT -p tcp --dport 8081 -j ACCEPT
sudo iptables -I INPUT -p tcp --dport 8082 -j ACCEPT
sudo iptables -I INPUT -p tcp --dport 8083 -j ACCEPT
```

**8. Access the UIs**
```
Airflow: http://<oracle-public-ip>:8081
Kafka:   http://<oracle-public-ip>:8082
Spark:   http://<oracle-public-ip>:8083
```

### OCI Architecture Diagram

```
┌─────────────────────────────────────────────────────┐
│              Oracle Cloud (Always Free)              │
│              VM.Standard.A1.Flex                     │
│              4 CPU / 24GB RAM / 100GB disk           │
│                                                      │
│  ┌─── Docker Compose ────────────────────────────┐  │
│  │                                                │  │
│  │  Kafka Layer:     zookeeper, kafka, kafka-ui   │  │
│  │  Spark Layer:     spark-master, spark-worker   │  │
│  │  Airflow Layer:   apiserver, scheduler,        │  │
│  │                   dag-processor, worker,       │  │
│  │                   triggerer                    │  │
│  │  Data Layer:      postgres, redis              │  │
│  │                                                │  │
│  └────────────────────────────────────────────────┘  │
│         │              │              │               │
│      :8081          :8082          :8083              │
│     Airflow        Kafka UI       Spark UI            │
└─────────┼──────────────┼──────────────┼──────────────┘
          │              │              │
          ▼              ▼              ▼
    Your Browser (access via public IP)

External connections:
  Docker → AWS S3 (read/write CSVs)
  Docker → Snowflake (read/write data)
  GitHub Actions → Oracle VM (not needed — CI/CD deploys Lambda, not Docker)
```

### Memory Budget for OCI (24GB)

| Service | RAM |
|---|---|
| Postgres | ~500MB |
| Redis | ~100MB |
| Airflow apiserver | ~500MB |
| Airflow scheduler | ~500MB |
| Airflow dag-processor | ~300MB |
| Airflow worker | ~2GB (runs ML, Spark submit) |
| Airflow triggerer | ~300MB |
| Zookeeper | ~256MB |
| Kafka | ~512MB (KAFKA_HEAP_OPTS=-Xmx512m) |
| Kafka UI | ~256MB |
| Spark master | ~512MB |
| Spark worker | ~1GB (SPARK_WORKER_MEMORY=1g) |
| **Total** | **~7GB** |
| **Free for OS + buffer** | **~17GB** |

You have plenty of headroom. The 24GB ARM instance handles all 14 services comfortably.

### Important Notes for OCI

1. **ARM architecture**: The Docker images used (Airflow, Kafka, Spark) all have ARM builds. The custom Dockerfile builds natively on ARM — no emulation needed.

2. **Persistent storage**: Docker volumes survive restarts. Your Airflow metadata (Postgres) and logs persist on the 100GB boot volume.

3. **Auto-start on reboot**: Add Docker to systemd so it starts on boot:
   ```bash
   sudo systemctl enable docker
   ```
   Then add a crontab entry to start compose on boot:
   ```bash
   crontab -e
   # Add: @reboot cd /home/ubuntu/Last-Mile-Fulfilment-Optimization && docker compose up -d
   ```

4. **Cost**: The A1.Flex instance is **Always Free** — no charges as long as you stay within the free tier limits (4 OCPUs, 24GB RAM, 200GB total block storage).

5. **Lambda still runs on AWS**: The OCI VM runs Airflow/Kafka/Spark. Lambda stays on AWS. Airflow's S3 sensor reads from AWS S3. This is a hybrid setup — orchestration on OCI, serverless data generation on AWS.
