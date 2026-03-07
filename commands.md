# Commands Reference — Last-Mile Fulfillment Platform

Complete command reference for every component of the platform.

---

## Environment Setup

```powershell
# Activate virtual environment (Windows)
poetry env activate
& ".venv\Scripts\activate.ps1"

# Install dependencies
pip install -r requirements.txt

# Set Snowflake environment variables (PowerShell — required for dbt)
$env:SNOWFLAKE_ACCOUNT="IKZQFMX-KKC80571"
$env:SNOWFLAKE_USER="Bhavyalikhitha"
$env:SNOWFLAKE_PASSWORD="your_password"
$env:SNOWFLAKE_DATABASE="FULFILLMENT_DB"
$env:SNOWFLAKE_WAREHOUSE="FULFILLMENT_WH"
$env:SNOWFLAKE_SCHEMA_RAW="RAW"
```

---

## Data Simulation & Backfill

```powershell
# Original backfill (Feb 2022 - Feb 2025) — run once locally
python -m data_simulation.backfill

# Extension backfill (Feb 2025 - Feb 2026) — run once locally
python -m data_simulation.backfill_extension

# Upload backfill CSVs to S3
python -m data_simulation.upload_to_s3
```

---

## Snowflake Setup

Run these SQL files in Snowflake worksheet in order:

```
1. infrastructure/snowflake/create_tables.sql     — create all 20 tables + schemas
2. infrastructure/snowflake/copy_into.sql          — load original backfill (2022-2025)
3. infrastructure/snowflake/copy_into_extension.sql — load extension (2025-2026)
4. infrastructure/snowflake/dedup.sql              — deduplicate after load
5. infrastructure/snowflake/data_checks.sql        — verify row counts and dates
```

---

## dbt Transformations

```powershell
cd dbt

# Validate connection
dbt debug --profiles-dir .

# Run SCD Type 2 snapshots (dim_product, dim_supplier, dim_driver, dim_customer)
dbt snapshot --profiles-dir .

# Run all 23 models
dbt run --profiles-dir .

# Run specific model
dbt run --select mart_daily_product_kpis --profiles-dir .

# Full refresh specific model (wipes and rebuilds)
dbt run --select mart_daily_product_kpis --full-refresh --profiles-dir .

# Run all 88 tests
dbt test --profiles-dir .

# Run tests by tag
dbt test --select tag:staging --profiles-dir .
dbt test --select tag:revenue --profiles-dir .
dbt test --select test_type:singular --profiles-dir .
dbt test --select test_type:generic --profiles-dir .

# Full pipeline: snapshot + run + test
dbt snapshot --profiles-dir . && dbt run --profiles-dir . && dbt test --profiles-dir .

# Serve documentation locally
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .

cd ..
```

---

## ML Pipeline

```powershell
# Train all models (XGBoost demand, LightGBM ETA, XGBoost stockout)
python -m ml.training.train_pipeline

# Save best models to ml/saved_models/
python -m ml.training.save_best_models

# Run all predictions + writeback to Snowflake
python -m ml.training.predict_and_writeback

# Run specific phases
python -m ml.training.predict_and_writeback --phase demand
python -m ml.training.predict_and_writeback --phase stockout
python -m ml.training.predict_and_writeback --phase eta
python -m ml.training.predict_and_writeback --phase future_demand

# Run multiple phases
python -m ml.training.predict_and_writeback --phase demand stockout
python -m ml.training.predict_and_writeback --phase demand stockout future_demand eta
```

**Expected output — future_demand:**
```
Historical data ends : 2026-03-05
Forecast window      : 2026-03-06 → 2026-09-01
180 days × 500 products = 90,000 rows
✓ Merged 90,000 future forecast rows (vintage: 2026-03-05)
✓ Completed in 14s
```

---

## Optimization & Experimentation

```powershell
# Run optimization engine (cost model + EOQ + allocation efficiency)
python -m optimization.run_optimization
# Expected: ~$297M savings, 8.21% reduction, 81.65% allocation efficiency

# Run experimentation engine (Welch t-tests + uplift analysis)
python -m experimentation.run_experimentation
# Expected: 10/10 experiments significant, uplift CSVs saved to experimentation/results/
```

**Expected output — optimization:**
```
Total baseline cost : $3,629,183,554
Total optimized cost: $3,331,586,272
Total savings       : $297,597,281 (8.21% reduction)
Avg allocation eff  : 81.65%
```

**Expected output — experimentation:**
```
10/10 experiments significant at alpha=0.05
EXP-007 | order_priority=Same-Day | 17.5% reduction | p=0.0000 (best segment lift)
✓ Merged 20 rows into mart_experiment_results
```

---

## Airflow (Docker)

```powershell
# Build custom Docker image — first time only (5-10 minutes)
docker-compose build

# Start all 7 containers
docker-compose up -d

# Check container status
docker-compose ps

# Get Airflow login password
docker exec last-mile-fulfilment-optimization-airflow-apiserver-1 `
  cat /opt/airflow/simple_auth_manager_passwords.json.generated

# View logs
docker-compose logs -f airflow-worker
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-dag-processor

# Trigger DAG manually via UI
# http://localhost:8081 → fulfillment_pipeline → Trigger DAG

# Stop all containers
docker-compose down

# Full reset (removes volumes + metadata)
docker-compose down -v --remove-orphans

# Install dbt manually in worker (if needed after container restart)
docker exec -u airflow last-mile-fulfilment-optimization-airflow-worker-1 `
  pip install dbt-snowflake --quiet
```

**Daily flow (automatic when Docker is running):**
```
Every day at 2am UTC:
  EventBridge fires → Lambda runs → generates 1 day → uploads to S3

Airflow S3 sensor detects new file → pipeline runs automatically:
  COPY INTO → dedup → dbt snapshot → dbt run → dbt test →
  post_processing → ML scoring → future forecast →
  optimization → experimentation → done
```

**Starting Airflow after a restart:**
```powershell
# 1. Start Docker
docker-compose up -d

# 2. Airflow checks S3 — Lambda already ran at 2am, file is there
# 3. Pipeline runs automatically
# 4. Snowflake updated — data is current
```

---

## Lambda (AWS)

```powershell
# Check Lambda function name
aws lambda list-functions --query "Functions[].FunctionName"

# Package and deploy Lambda with Linux-compatible wheels
pip install numpy pandas boto3 `
  --target lambda_build `
  --platform manylinux2014_x86_64 `
  --only-binary=:all: `
  --python-version 3.12

Copy-Item data_simulation/lambda_handler.py lambda_handler.py
Compress-Archive -Path data_simulation, config, lambda_handler.py `
  -DestinationPath lambda_package.zip -Force
aws lambda update-function-code `
  --function-name fulfillment-data-generator `
  --zip-file fileb://lambda_package.zip
Remove-Item lambda_handler.py

# Quick redeploy (no dependency rebuild — code changes only)
Copy-Item data_simulation/lambda_handler.py lambda_handler.py
Compress-Archive -Path data_simulation, config, lambda_handler.py `
  -DestinationPath lambda_package.zip -Force
aws lambda update-function-code `
  --function-name fulfillment-data-generator `
  --zip-file fileb://lambda_package.zip
Remove-Item lambda_handler.py

# Test Lambda — generate 1 day
'{"mode": "manual", "days": 1}' | Out-File -FilePath payload.json -Encoding utf8
aws lambda invoke `
  --function-name fulfillment-data-generator `
  --payload fileb://payload.json `
  response.json
Get-Content response.json

# Test Lambda — generate 7 days (weekly mode)
'{"mode": "weekly"}' | Out-File -FilePath payload.json -Encoding utf8
aws lambda invoke `
  --function-name fulfillment-data-generator `
  --payload fileb://payload.json `
  response.json
Get-Content response.json

# Deploy using deploy script
python -m infrastructure.aws.deploy_lambda --mode daily
python -m infrastructure.aws.deploy_lambda --mode weekly
```

---

## Snowflake Verification Queries

```sql
-- ── Data Freshness ────────────────────────────────────────────
SELECT MAX(order_date) FROM FULFILLMENT_DB.RAW.FACT_ORDERS;
-- Should advance by 1 day each daily run

SELECT MAX(date) FROM FULFILLMENT_DB.MARTS.MART_DAILY_WAREHOUSE_KPIS;
-- Should match fact_orders max date after dbt runs

SELECT MAX(date) FROM FULFILLMENT_DB.MARTS.MART_DAILY_PRODUCT_KPIS
WHERE is_forecast = FALSE;
-- Should match after ML runs

-- ── Raw Table Dates ───────────────────────────────────────────
SELECT 'fact_orders' as tbl, MAX(order_date) as max_date
FROM FULFILLMENT_DB.RAW.FACT_ORDERS
UNION ALL
SELECT 'fact_inventory_snapshot', MAX(snapshot_date)
FROM FULFILLMENT_DB.RAW.FACT_INVENTORY_SNAPSHOT
UNION ALL
SELECT 'fact_deliveries', MAX(delivered_time::date)
FROM FULFILLMENT_DB.RAW.FACT_DELIVERIES
UNION ALL
SELECT 'fact_shipments', MAX(shipment_date)
FROM FULFILLMENT_DB.RAW.FACT_SHIPMENTS;

-- ── Mart Row Counts ───────────────────────────────────────────
SELECT COUNT(*) FROM FULFILLMENT_DB.MARTS.MART_DAILY_WAREHOUSE_KPIS;
SELECT COUNT(*) FROM FULFILLMENT_DB.MARTS.MART_DAILY_PRODUCT_KPIS;
SELECT COUNT(*) FROM FULFILLMENT_DB.MARTS.MART_DELIVERY_PERFORMANCE;
SELECT COUNT(*) FROM FULFILLMENT_DB.MARTS.MART_COST_OPTIMIZATION;

-- ── ML Predictions ────────────────────────────────────────────
SELECT COUNT(*) FROM FULFILLMENT_DB.MARTS.MART_DAILY_PRODUCT_KPIS
WHERE demand_forecast IS NOT NULL AND is_forecast = FALSE;
-- Should be ~733,000

SELECT COUNT(*) FROM FULFILLMENT_DB.MARTS.MART_DELIVERY_PERFORMANCE
WHERE predicted_eta IS NOT NULL;
-- Should be ~8,776+

-- ── Future Forecast ───────────────────────────────────────────
SELECT
    forecast_horizon,
    COUNT(*) as rows,
    MIN(date) as min_date,
    MAX(date) as max_date
FROM FULFILLMENT_DB.MARTS.MART_DAILY_PRODUCT_KPIS
WHERE is_forecast = TRUE
GROUP BY forecast_horizon
ORDER BY forecast_horizon;
-- Expected: 30→15K rows, 60→15K, 90→15K, 180→45K

-- ── Experiment Results ────────────────────────────────────────
SELECT experiment_id, group_name, lift_pct, p_value, is_significant
FROM FULFILLMENT_DB.MARTS.MART_EXPERIMENT_RESULTS
ORDER BY experiment_id, group_name;
-- Expected: 10 experiments, 20 rows total

-- ── Optimization Results ──────────────────────────────────────
SELECT
    ROUND(SUM(savings_amount), 0) as total_savings,
    ROUND(AVG(savings_pct), 2) as avg_savings_pct,
    ROUND(AVG(allocation_efficiency_pct), 2) as avg_alloc_eff
FROM FULFILLMENT_DB.MARTS.MART_COST_OPTIMIZATION;
-- Expected: ~$297M savings, ~8.21%, ~81.65%

-- ── Recent Orders (spot check) ────────────────────────────────
SELECT order_date, COUNT(*) AS orders
FROM FULFILLMENT_DB.RAW.FACT_ORDERS
WHERE order_date >= DATEADD(day, -7, CURRENT_DATE())
GROUP BY order_date
ORDER BY order_date;
```

---

## Troubleshooting

```powershell
# dbt: command not found
# Use full path inside Docker or set PATH explicitly
/home/airflow/.local/bin/dbt run --profiles-dir .

# Lambda: numpy/pandas import error
# Must use Linux wheels — rebuild with manylinux flag
pip install numpy pandas --target lambda_build `
  --platform manylinux2014_x86_64 --only-binary=:all: --python-version 3.12

# Airflow: tasks skipped immediately (0.12s run)
# start_date is in the future — fix in DAG
# start_date must be BEFORE first expected run date
start_date=datetime(2026, 3, 4)  # never use datetime.now()

# Airflow: invalid auth token between worker and apiserver
# AIRFLOW__CORE__FERNET_KEY is missing or empty
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Add output to .env as AIRFLOW__CORE__FERNET_KEY=...

# Snowflake: duplicate rows after COPY INTO
# Run dedup manually
# infrastructure/snowflake/dedup.sql

# dbt: env var required but not provided
# Set env vars in same PowerShell session before running dbt
$env:SNOWFLAKE_ACCOUNT="IKZQFMX-KKC80571"
# (see Environment Setup section above)

# ML: ModuleNotFoundError for xgboost or lightgbm
# Wrong Python environment — activate venv first
& ".venv\Scripts\activate.ps1"
python -m ml.training.predict_and_writeback --phase demand
```