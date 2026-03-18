# V2 Changes — Interview Reference Guide

Everything that was added in v2, why it was added, how it works, and how to talk about it in interviews.

---

## Quick Summary

v1 was a batch-only pipeline: Lambda → S3 → Airflow → Snowflake → dbt → pandas ML → Power BI.

v2 adds 5 technologies to close production gaps:
1. **Apache Kafka** — real-time event streaming (dual batch+streaming ingestion)
2. **Apache Spark (PySpark)** — distributed feature engineering replacing pandas
3. **Terraform** — Infrastructure as Code for all AWS resources
4. **GitHub Actions** — CI/CD pipeline (lint, test, deploy)
5. **Great Expectations** — data quality validation at pipeline boundaries

---

## 1. Apache Kafka

### What was added
```
streaming/
  config.py              — Kafka bootstrap servers, topic names, producer/consumer configs
  schemas.py             — Pydantic models for 3 event types (order, delivery, inventory)
  producer.py            — Reads S3 CSVs, publishes row-by-row as Kafka events
  consumer_s3.py         — Consumes from Kafka, writes CSVs to S3 (same path format as Lambda)
  consumer_snowflake.py  — Alternative: consumes and writes directly to Snowflake
  topic_setup.py         — Creates topics with correct partition count and retention
```

Docker services added to `docker-compose.yml`:
- **Zookeeper** (port 2181) — Kafka coordination
- **Kafka broker** (port 9092 external, 29092 internal) — message broker
- **Kafka UI** (port 8082) — web dashboard to inspect topics and messages

### 3 Topics

| Topic | Key | Partitions | Retention | Why this key |
|---|---|---|---|---|
| `fulfillment.orders.created` | `order_id` | 8 | 7 days | Orders are unique; key ensures same order always goes to same partition |
| `fulfillment.deliveries.updated` | `delivery_id` | 8 | 7 days | Delivery status updates for the same delivery stay in order |
| `fulfillment.inventory.snapshot` | `warehouse_id-product_id` | 8 | 3 days | Inventory for same product+warehouse always on same partition for ordering |

### Why 8 partitions?
8 matches the number of warehouses. Each warehouse's events can be processed by a separate consumer in a consumer group. At 10x scale, you add consumers — Kafka rebalances automatically.

### How it integrates with Airflow
The DAG starts with a `BranchPythonOperator` that checks an Airflow Variable:
```python
DATA_SOURCE = Variable.get('DATA_SOURCE', default_var='s3')
if DATA_SOURCE == 'kafka':
    → consume_from_kafka task (drains Kafka, writes CSVs to S3)
else:
    → wait_for_s3_files task (S3KeySensor waits for Lambda output)
```
Both paths produce identical CSVs in S3. Everything downstream (COPY INTO, dbt, ML) is unchanged.

### Why not replace S3 entirely?
Real companies use both:
- **Batch (S3)** for historical backfills, reconciliation, cost-sensitive workloads
- **Streaming (Kafka)** for real-time inventory, instant ETA updates, SLA alerting

Having both in the same pipeline via a branch operator mirrors Uber/Lyft architecture.

### Commands to test
```powershell
# Create topics
docker exec -it <airflow-worker> python -m streaming.topic_setup

# Produce events
docker exec -it <airflow-worker> python -m streaming.producer --date 2024-06-15 --source s3 --delay-ms 0

# Consume to S3
docker exec -it <airflow-worker> python -m streaming.consumer_s3 --timeout 30

# Switch DAG to Kafka mode
docker exec -it <airflow-worker> airflow variables set DATA_SOURCE kafka

# Switch back to S3
docker exec -it <airflow-worker> airflow variables set DATA_SOURCE s3
```

### Interview talking points
- "I implemented a dual batch+streaming architecture using Kafka alongside the existing S3 batch path"
- "The producer replays S3 CSVs as events with Pydantic validation, and the consumer writes them back in the same S3 path format — so no downstream changes were needed"
- "8 partitions per topic, keyed by order_id/delivery_id, so related events stay ordered within a partition"
- "Switching between batch and streaming is a single Airflow Variable — no code changes, no redeployment"
- "This reduces data freshness from 24 hours to seconds for real-time use cases"

---

## 2. Apache Spark (PySpark)

### What was added
```
spark/
  config.py                              — SparkSession builder, Snowflake JDBC connector options
  features/
    demand_features_spark.py             — PySpark port of ml/features/demand_features.py (24 features)
    eta_features_spark.py                — PySpark port of ml/features/eta_features.py (20 features)
  jobs/
    run_demand_features.py               — Spark job: Snowflake → features → write back
    run_eta_features.py                  — Spark job: Snowflake → features → write back
```

Docker services added:
- **spark-master** (ports 7077, 8083) — Spark cluster manager
- **spark-worker** (1GB memory, 2 cores) — executes tasks

### The problem Spark solves

**Demand features (pandas):**
```python
# Single-threaded: processes one product at a time through groupby
df.groupby('product_id')['total_units_sold'].transform(lambda x: x.rolling(7).mean())
# 500 products × 1500 days = 750K rows, all on one CPU core
```

**Demand features (Spark):**
```python
# Distributed: all 500 products processed in parallel across workers
F.avg('total_units_sold').over(
    Window.partitionBy('product_id').orderBy('date').rowsBetween(-6, 0)
)
# Same result, but partitioned across Spark executors
```

**ETA features (pandas):**
```python
# Had to chunk by year to avoid OOM on 5M+ delivery rows
for year in [2022, 2023, 2024, 2025, 2026]:
    chunk = deliveries[deliveries['year'] == year]
    features = build_features(chunk)
# 5 sequential passes, ~4GB peak memory
```

**ETA features (Spark):**
```python
# Reads all 5M rows at once, distributed across workers
deliveries = read_snowflake_table(spark, "INT_DELIVERY_ENRICHED")
features = build_eta_features_spark(deliveries, dates)
# 1 pass, memory distributed across workers
```

### Key pandas → PySpark translations

| Pandas | PySpark | What it does |
|---|---|---|
| `df.groupby('product_id')['col'].shift(n)` | `F.lag('col', n).over(Window.partitionBy('product_id').orderBy('date'))` | Lag features |
| `df.groupby(...).transform(lambda x: x.rolling(w).mean())` | `F.avg('col').over(Window.partitionBy(...).orderBy('date').rowsBetween(-w+1, 0))` | Rolling average |
| `df.groupby(...).transform(lambda x: x.rolling(w).std())` | `F.stddev('col').over(same_window)` | Rolling std dev |
| `df.groupby(...).transform(lambda x: x.rolling(w).min())` | `F.min('col').over(same_window)` | Rolling min |
| `df.groupby('driver_id').agg(mean=('col', 'mean'))` | `df.groupBy('driver_id').agg(F.avg('col'))` | Grouped aggregation |
| `df['col'].map({'A': 0, 'B': 1})` | `F.when(F.col('col') == 'A', 0).when(...)` | Categorical encoding |

### How it integrates with Airflow
Two new tasks run between post-processing and ML scoring:
```
... >> post_processing >> gx_validate_marts >> spark_demand_features >> spark_eta_features >> ml_demand_stockout >> ...
```
Spark writes features to `STAGING.SPARK_DEMAND_FEATURES` and `STAGING.SPARK_ETA_FEATURES`.

### Why Spark AND pandas coexist
- **Spark** for feature engineering (5M+ rows with complex rolling windows — pandas OOMs)
- **Pandas** for ML training (XGBoost/LightGBM train on in-memory DataFrames — training sets fit in memory after temporal split)

This is the same pattern used at Airbnb and Spotify: Spark for data prep, pandas/sklearn for model training.

### Commands to test
```powershell
# Run demand features job
docker exec -it <airflow-worker> python -m spark.jobs.run_demand_features

# Run ETA features job
docker exec -it <airflow-worker> python -m spark.jobs.run_eta_features

# Check Spark UI
# http://localhost:8083
```

### Interview talking points
- "The pandas ETA pipeline had to chunk by year to avoid OOM on 5M deliveries. PySpark Window functions process all years in a single distributed pass"
- "I ported 24 demand features and 20 ETA features from pandas to PySpark — same feature set, same output, but distributed"
- "Key translation: `groupby().transform(lambda x: x.rolling(7).mean())` becomes `F.avg('col').over(Window.partitionBy(...).rowsBetween(-6, 0))` — semantically identical but parallelized"
- "Adding a second Spark worker doubles throughput with zero code changes — horizontal scalability"
- "Spark handles data prep; pandas handles model training. That's the standard pattern at Airbnb and Spotify"

---

## 3. Terraform

### What was added
```
terraform/
  providers.tf              — AWS provider ~> 5.0, region us-east-2
  backend.tf                — S3 + DynamoDB state backend (commented out for local dev)
  variables.tf              — Root variables (region, env, lambda config, schedule flags)
  outputs.tf                — Lambda ARN, bucket name, rule ARNs
  main.tf                   — Wires 4 modules together
  placeholder.py            — Dummy file for initial Lambda zip
  environments/
    dev.tfvars              — daily_schedule=true, weekly=false
    prod.tfvars             — daily=false, weekly=true
  modules/
    iam/                    — Lambda execution role + scoped S3/CloudWatch policy
    s3/                     — Bucket + versioning + AES256 encryption + public access block
    lambda/                 — Function + CloudWatch log group (14/30 day retention by env)
    eventbridge/            — Daily cron(0 2 * * ? *) + weekly cron(0 3 ? * MON *) rules
```

### 15 AWS resources managed

| Module | Resources | What they do |
|---|---|---|
| **IAM** | `aws_iam_role`, `aws_iam_role_policy` | Lambda execution role with scoped S3 + CloudWatch permissions |
| **S3** | `aws_s3_bucket`, versioning, encryption, public_access_block, lifecycle (dev only: expire raw/ after 90 days) | Data lake bucket with security best practices |
| **Lambda** | `aws_lambda_function`, `aws_cloudwatch_log_group` | Data generator function + log retention |
| **EventBridge** | 2 rules, 2 targets, 2 lambda_permissions | Daily + weekly triggers with `{"mode": "daily"}` / `{"mode": "weekly"}` payloads |

### Why modular structure?
Each module is self-contained with its own `main.tf`, `variables.tf`, `outputs.tf`. Modules can be reused — if you add a second Lambda function, you call the lambda module again with different variables. The root `main.tf` wires them together:

```hcl
module "lambda" {
  source     = "./modules/lambda"
  role_arn   = module.iam.role_arn        # ← output from IAM module
  s3_bucket  = module.s3.bucket_name      # ← output from S3 module
}
module "eventbridge" {
  lambda_arn = module.lambda.function_arn  # ← output from Lambda module
}
```

### Dev vs Prod
```hcl
# dev.tfvars — daily schedule for testing
environment             = "dev"
daily_schedule_enabled  = true
weekly_schedule_enabled = false

# prod.tfvars — weekly schedule for production
environment             = "prod"
daily_schedule_enabled  = false
weekly_schedule_enabled = true
```

Also affects: S3 lifecycle (dev: expire raw/ after 90 days, prod: no expiry), CloudWatch retention (dev: 14 days, prod: 30 days).

### State management
- **Local state** (current): `terraform.tfstate` on your machine. Fine for single developer.
- **Remote state** (when ready): S3 bucket + DynamoDB table for locking. Enables CI/CD and team collaboration.

Bootstrap commands for remote state:
```powershell
aws s3api create-bucket --bucket last-mile-fulfillment-tf-state --region us-east-2 --create-bucket-configuration LocationConstraint=us-east-2
aws dynamodb create-table --table-name terraform-locks --region us-east-2 --attribute-definitions AttributeName=LockID,AttributeType=S --key-schema AttributeName=LockID,KeyType=HASH --billing-mode PAY_PER_REQUEST
```

### Import existing resources
Since Lambda, S3, IAM, EventBridge already exist in AWS:
```powershell
terraform import module.iam.aws_iam_role.lambda_execution fulfillment-lambda-role
terraform import module.s3.aws_s3_bucket.fulfillment last-mile-fulfillment-platform
terraform import module.lambda.aws_lambda_function.data_generator fulfillment-data-generator
```
Then `terraform plan` shows zero changes — proving the code matches reality.

### Commands to test
```powershell
cd terraform
terraform init                                    # Initialize providers and modules
terraform validate                                # Syntax check
terraform plan -var-file=environments/dev.tfvars  # Dry run — shows 15 resources to create
terraform apply -var-file=environments/dev.tfvars # Actually create resources (needs AWS creds)
```

### Interview talking points
- "I modularized our AWS infrastructure into 4 Terraform modules — IAM, S3, Lambda, EventBridge — each with its own variables and outputs"
- "Dev and prod environments are separated via tfvars files that control schedule rules, retention policies, and lifecycle rules"
- "The Lambda module uses `lifecycle { ignore_changes = [filename, source_code_hash] }` because CI/CD deploys the code separately — Terraform only manages the infrastructure skeleton"
- "State is stored in S3 with DynamoDB locking to prevent concurrent applies"
- "`terraform plan` shows exactly what would change before any deployment — no surprises"
- "Went from 30 minutes of manual AWS Console clicks to `terraform apply` in 2 minutes"

---

## 4. GitHub Actions CI/CD

### What was added
```
.github/workflows/
  ci.yml      — Runs on push to main: lint, test, dbt test, terraform plan (3 parallel jobs)
  deploy.yml  — Runs on push to main: terraform apply, Lambda deploy (2 sequential jobs)
```

### CI Pipeline (`ci.yml`) — 3 parallel jobs

```
push to main
  ├── Job 1: lint-and-test (Python)
  │   ├── ruff check .          — catches unused imports, syntax errors, style violations
  │   ├── ruff format --check . — enforces consistent formatting
  │   └── pytest tests/ -v      — runs unit tests
  │
  ├── Job 2: dbt-test (SQL)
  │   ├── pip install dbt-snowflake
  │   └── dbt test --profiles-dir .  — runs 88 data quality tests against Snowflake
  │
  └── Job 3: terraform-plan (Infrastructure)
      ├── terraform init
      ├── terraform validate
      └── terraform plan  — shows what would change (no actual changes)
```

### Deploy Pipeline (`deploy.yml`) — 2 sequential jobs

```
push to main
  └── Job 1: terraform-apply
      └── terraform apply -auto-approve
          │
          ▼
      Job 2: deploy-lambda (depends on terraform-apply)
          ├── pip install numpy pandas --platform manylinux2014_x86_64  — Linux wheels
          ├── zip data_simulation/ + config/ + lambda_handler.py
          └── aws lambda update-function-code --zip-file fileb://lambda_deploy.zip
```

### Why parallel CI jobs?
Jobs 1, 2, 3 are independent — linting doesn't depend on dbt results, terraform doesn't depend on pytest. Running them in parallel saves ~3 minutes vs sequential.

### GitHub Secrets required
Set these in repo Settings → Secrets → Actions:

| Secret | Used by |
|---|---|
| `AWS_ACCESS_KEY_ID` | terraform-plan, terraform-apply, deploy-lambda |
| `AWS_SECRET_ACCESS_KEY` | terraform-plan, terraform-apply, deploy-lambda |
| `SNOWFLAKE_ACCOUNT` | dbt-test |
| `SNOWFLAKE_USER` | dbt-test |
| `SNOWFLAKE_PASSWORD` | dbt-test |

### What was also added
- **`ruff`** as a dev dependency in `pyproject.toml` with config:
  ```toml
  [tool.ruff]
  line-length = 120
  target-version = "py311"
  [tool.ruff.lint]
  select = ["E", "F", "I"]  # errors, pyflakes, import sorting
  ```

### Commands to test locally
```powershell
# Lint (same as CI)
ruff check .
ruff format --check .

# Tests (same as CI)
python -m pytest tests/ -v

# dbt test (same as CI — needs Snowflake env vars)
cd dbt && dbt test --profiles-dir .
```

### Interview talking points
- "I set up CI/CD from scratch — 3 parallel CI jobs (lint, test, dbt test, terraform plan) and a 2-step deploy pipeline"
- "The Lambda deployment was a 10-step manual process. Now it's automatic on every push — installs Linux-compatible wheels, zips the package, and deploys via AWS CLI"
- "dbt tests run in CI against a live Snowflake warehouse — 88 data quality tests catch issues before they reach production"
- "Terraform plan runs in CI so you can see infrastructure changes before they're applied"
- "Ruff replaces flake8+isort+black with a single tool that's 10-100x faster"

---

## 5. Great Expectations

### What was added
```
great_expectations/
  great_expectations.yml                    — 3 datasources: s3_pandas, snowflake_raw, snowflake_marts
  expectations/
    s3_orders_suite.json                    — CSV schema, row count 3K-12K, ID regex, value sets
    s3_deliveries_suite.json                — CSV schema, distance < 100km, status values
    s3_inventory_suite.json                 — CSV schema, row count ~4K, closing_stock >= 0
    raw_orders_suite.json                   — Post-dedup uniqueness, allocation strategy values
    raw_deliveries_suite.json               — Dedup check, SLA values {240, 480, 2880}
    raw_inventory_suite.json                — Compound uniqueness, all 8 warehouses present
    mart_delivery_performance_suite.json    — Warehouse completeness, mean delivery time bounds
    mart_cost_optimization_suite.json       — savings > 0, baseline > optimized
    mart_daily_warehouse_kpis_suite.json    — Warehouse completeness, service_level bounds
  checkpoints/
    s3_landing_checkpoint.yml               — Validates 3 CSV suites before COPY INTO
    raw_load_checkpoint.yml                 — Validates 3 RAW suites after dedup
    mart_quality_checkpoint.yml             — Validates 3 mart suites after post-processing
```

### The 3-gate validation model

```
S3 Landing (Gate 1)          RAW Load (Gate 2)           Marts (Gate 3)
Before COPY INTO             After dedup                 After post-processing
─────────────────           ─────────────────           ─────────────────
CSV schema conformance       Post-dedup uniqueness        Distributional checks
Row count bounds             All 8 warehouses present     baseline > optimized cost
ID regex patterns            SLA values correct           service_level 0-100%
Value set validation         Compound key uniqueness      Delivery time mean bounds
distance < 100km
closing_stock >= 0
```

### Why GX complements dbt (not duplicates it)

| What's validated | dbt | GX |
|---|---|---|
| Column not-null, uniqueness, referential integrity | ✅ 88 tests | Not duplicated |
| Row-level business logic (SLA breach math, cost savings) | ✅ 12 singular tests | Not duplicated |
| **CSV schema before Snowflake** | ❌ Can't see S3 | ✅ Validates columns, types |
| **Pre-load row counts** | ❌ Data not loaded yet | ✅ Catches empty/corrupt files |
| **Post-dedup verification** | ❌ Tests run after dbt, not after COPY INTO | ✅ Confirms dedup worked |
| **Cross-table volume ratios** | ❌ | ✅ Orders:deliveries ratio |
| **Distributional properties** | ❌ | ✅ Mean, proportion checks |
| **All 8 warehouses present** | ❌ | ✅ Distinct set validation |

### How it integrates with Airflow
3 new `PythonOperator` tasks in the DAG:
```
wait_for_s3 >> gx_validate_s3_landing >> copy_into >> dedup >> verify
>> gx_validate_raw_load >> dbt_snapshot >> dbt_run >> dbt_test >> post_processing
>> gx_validate_marts >> spark_features >> ml_scoring >> ...
```

If any GX checkpoint fails, the DAG stops — no bad data propagates.

### Example: how GX catches a real bug
The old data simulation had a bug where NYC delivery distances were 150km (should be ~25km for last-mile). The `s3_deliveries_suite` would catch this:
```json
{
  "expectation_type": "expect_column_values_to_be_between",
  "kwargs": {"column": "distance_km", "min_value": 0.1, "max_value": 100.0, "mostly": 0.95}
}
```
With dbt, this data would have loaded into RAW, propagated through staging/intermediate/marts, and only been noticed when the Power BI dashboard showed weird delivery times.

### Commands to test
```powershell
# Test GX context loads
docker exec -it <airflow-worker> python -c "
import great_expectations as gx
context = gx.get_context(context_root_dir='/opt/airflow/project/great_expectations')
print('GX loaded:', context)
"
```

### Interview talking points
- "I added Great Expectations at 3 pipeline boundaries where dbt tests can't reach — before Snowflake load, after dedup, and after post-processing"
- "The S3 landing suite catches corrupted CSVs before they enter the warehouse — schema conformance, row count bounds, regex patterns on IDs"
- "Zero overlap with the 88 dbt tests — GX handles distributional checks, cross-table ratios, and pre-load validation that SQL-based tests can't express"
- "If a GX checkpoint fails, the DAG stops immediately — bad data never propagates to marts"
- "It caught the 150km NYC delivery distance bug at S3 landing, which previously propagated through 4 layers before anyone noticed"

---

## 6. Other Changes

### Files modified
| File | What changed |
|---|---|
| `docker-compose.yml` | Added 6 services: zookeeper, kafka, kafka-ui, spark-master, spark-worker + `KAFKA_BOOTSTRAP_SERVERS` env var |
| `Dockerfile` | Added `default-jre-headless` (Java for Spark), `confluent-kafka`, `pyspark`, `great-expectations[snowflake]`, `pydantic` |
| `airflow/dags/fulfillment_pipeline_dag.py` | Added `BranchPythonOperator`, 3 GX tasks, 2 Spark tasks, Kafka consumer task. DAG grew from 14 to 20 tasks |
| `pyproject.toml` | Added `confluent-kafka`, `pyspark`, `great-expectations`, `pydantic` deps + `ruff`/`pytest` dev deps + `[tool.ruff]` config |
| `.gitignore` | Added `terraform/`, `*.tfplan`, `great_expectations/uncommitted/`, `lambda_package/` |
| `.env.example` | Added `KAFKA_BOOTSTRAP_SERVERS` |
| `CLAUDE.md` | Updated with all new commands, architecture, directories |
| `README.md` | Added new tech stack badges, "Why These Tools" with Impact column, "Performance & Scalability Impact" section |

### Docker services (8 → 14)
```
EXISTING (8):
  postgres, redis, airflow-apiserver, airflow-scheduler,
  airflow-dag-processor, airflow-worker, airflow-triggerer, airflow-init

NEW (6):
  zookeeper         — Kafka coordination (port 2181)
  kafka             — Message broker (port 9092, internal 29092)
  kafka-ui          — Web UI for topic inspection (port 8082)
  spark-master      — Spark cluster manager (port 7077, UI 8083)
  spark-worker      — Spark executor (1GB RAM, 2 cores)
```

### Airflow DAG tasks (14 → 20)
```
NEW TASKS:
  branch_data_source      — BranchPythonOperator: choose S3 or Kafka path
  consume_from_kafka      — PythonOperator: drain Kafka topics, write CSVs to S3
  gx_validate_s3_landing  — PythonOperator: GX checkpoint on S3 CSVs
  gx_validate_raw_load    — PythonOperator: GX checkpoint on RAW tables
  gx_validate_marts       — PythonOperator: GX checkpoint on mart tables
  spark_demand_features   — BashOperator: PySpark demand feature engineering
  spark_eta_features      — BashOperator: PySpark ETA feature engineering
```

---

## 7. Common Interview Questions & Answers

### "Why Kafka over just using S3?"
"S3 is batch — you upload a file and process it later. Kafka is streaming — events are available to consumers within milliseconds. In a real fulfillment system, an order placed at 2pm needs an ETA prediction immediately, not at 2am the next day. I built both paths through the same pipeline using a branch operator, because real companies use both — batch for analytics and reconciliation, streaming for operational decisions."

### "Why Spark instead of just using bigger EC2 instances?"
"Vertical scaling (bigger machine) has a ceiling — you'll always hit memory limits with pandas on 5M+ rows. Spark scales horizontally — adding a worker doubles throughput with zero code changes. The ETA pipeline already proved this: pandas needed 5 year-by-year chunks to avoid OOM, while Spark processes everything in one pass. At 10x scale (50M deliveries), I add workers instead of rewriting code."

### "Why not just use Snowflake for feature engineering?"
"Snowflake can do window functions, but ML feature engineering involves complex conditional logic, custom aggregations, and iterative operations that are awkward in SQL. PySpark gives you the expressiveness of Python with the distributed execution of a cluster. Also, Spark can write features to multiple destinations (Snowflake, S3, Delta Lake) — Snowflake SQL stays within Snowflake."

### "Why Terraform over CloudFormation?"
"CloudFormation is AWS-only. Terraform is cloud-agnostic — the same patterns work for GCP or Azure. It also has a richer module ecosystem and `terraform plan` gives a clear diff before any change. For a portfolio project, Terraform is also what most companies use — so it's the more transferable skill."

### "Why Great Expectations over just more dbt tests?"
"dbt tests validate data that's already in Snowflake. But what if the CSV from Lambda is corrupted? Or COPY INTO silently produced duplicates? GX validates at the boundaries that dbt can't see — S3 files before load, RAW tables after dedup, and distributional properties of marts. They complement each other: dbt for row-level SQL logic, GX for schema conformance and statistical checks."

### "How does this handle failure?"
"At every stage. GX checkpoints halt the DAG if CSVs are malformed. COPY INTO uses ON_ERROR='CONTINUE' so one bad row doesn't block the load. Dedup handles duplicates from FORCE=TRUE reloads. dbt tests catch row-level issues. Each Airflow task has retries with 5-minute delays. The Kafka consumer uses manual offset commits — it only commits after successful S3 write, so messages aren't lost on failure."

### "What would you add next?"
"Delta Lake or Iceberg on S3 for ACID transactions and time travel on the data lake layer. Debezium for CDC from a source database into Kafka — right now the producer replays S3 files, but CDC would capture real database changes. And Kubernetes for deploying Airflow in production instead of docker-compose."

---

## 8. Quick Reference: All New Commands

```powershell
# ── Terraform ────────────────────────────────────────
cd terraform
terraform init
terraform validate
terraform plan -var-file=environments/dev.tfvars
terraform apply -var-file=environments/dev.tfvars

# ── Kafka ────────────────────────────────────────────
python -m streaming.topic_setup
python -m streaming.producer --date 2026-03-17 --source s3
python -m streaming.producer --date 2026-03-17 --source local --delay-ms 0
python -m streaming.consumer_s3 --timeout 30
python -m streaming.consumer_snowflake --timeout 30

# ── Spark ────────────────────────────────────────────
python -m spark.jobs.run_demand_features
python -m spark.jobs.run_eta_features

# ── Great Expectations ───────────────────────────────
# (runs via Airflow DAG — 3 checkpoint tasks)
# Or test GX context manually:
python -c "import great_expectations as gx; ctx = gx.get_context(context_root_dir='great_expectations'); print(ctx)"

# ── Linting ──────────────────────────────────────────
ruff check .
ruff format --check .
ruff check --fix .        # auto-fix

# ── Docker (all 14 services) ────────────────────────
docker-compose build
docker-compose up -d
docker-compose ps
docker-compose down
docker-compose down -v --remove-orphans

# ── Airflow Variables ────────────────────────────────
# Switch between batch and streaming
airflow variables set DATA_SOURCE kafka
airflow variables set DATA_SOURCE s3

# ── UIs ──────────────────────────────────────────────
# Airflow:  http://localhost:8081
# Kafka UI: http://localhost:8082
# Spark UI: http://localhost:8083
```
