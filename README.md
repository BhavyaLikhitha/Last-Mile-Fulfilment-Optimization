# 📦🚚 Distributed Last-Mile Fulfillment & Inventory Optimization Platform 📈

### An End-to-End Supply Chain, Logistics & Operations Intelligence System

> *Simulating, engineering, optimizing, and visualizing a distributed fulfillment network at scale — from raw data generation to production-grade BI and ML.*

> **Version note:** The platform supports two pipeline configurations — **v1 (S3-only, 14 tasks, 9 Docker services)** for lightweight local development, and **v2 (Kafka + Spark + GX, 20 tasks, 14 Docker services)** for the full distributed stack. Both versions coexist in the codebase via comment toggles. The active configuration is v1; see the switching instructions in each file to enable v2.

---

## 🔍 Project Overview

This platform is a **full end-to-end data engineering and analytics solution** built to simulate and optimize a distributed last-mile fulfillment network across 8 US warehouses and 500 products. It spans every layer of the modern data stack like synthetic data generation, cloud ingestion, warehouse transformation, machine learning, cost optimization, A/B experimentation, and business intelligence, all orchestrated through a production-grade Airflow pipeline.

The platform generates **40M+ rows of realistic operational data** covering orders, inventory snapshots, deliveries, driver activity, supplier shipments, and A/B experiment assignments across a 4-year timeline (Feb 2022 → Sep 2026). Every component mirrors how real fulfillment companies like Amazon, Walmart, Chewy, and DoorDash operate their data platforms.

---

## 📐 Scope

| Layer | What's Built |
|---|---|
| **Data Engineering** | AWS Lambda + EventBridge for incremental generation, S3 data lake, Apache Kafka for real-time event streaming, Snowflake warehouse, Apache Spark for distributed feature engineering, Airflow 20-task DAG, dbt 5-layer transformation (23 models, 88 tests), SCD Type 2 snapshots |
| **Infrastructure & DevOps** | Terraform IaC (4 modules, dev/prod environments), GitHub Actions CI/CD (lint, test, terraform plan on PR; deploy on merge), Great Expectations data quality (9 suites, 3 validation gates) |
| **Data Science / ML** | XGBoost demand forecasting (MAPE 1.72%), LightGBM ETA prediction (R²=0.96), XGBoost stockout risk classifier, 180-day multi-vintage future forecasting |
| **Data Analysis / BI** | 6-page Power BI dashboard covering Executive Overview, Inventory & Supply Chain, Demand Forecasting, Last-Mile Delivery, Warehouse Allocation, and A/B Experimentation |
| **Optimization** | SciPy-based cost optimization engine (EOQ, safety stock, allocation efficiency), $297M in identified savings |
| **Experimentation** | Welch t-test A/B framework, 10 experiments across 3 types, 9/10 statistically significant, segment-level uplift analysis |

---

## 🏢 Business Context

This platform mirrors the operational intelligence stack used across **e-commerce, retail, logistics, grocery, quick-commerce and last-mile delivery**:

- **Amazon / Walmart / Target** — multi-warehouse inventory allocation, nearest vs cost-optimal routing, SLA-based delivery tracking, demand forecasting at SKU level
- **DoorDash / Instacart / Gopuff** — last-mile ETA prediction, driver utilization optimization, SLA breach analysis, real-time fulfillment decisions
- **Chewy / Wayfair / Overstock** — 8-12 regional fulfillment centers, category-level demand patterns, safety stock and reorder point optimization
- **FedEx / UPS / XPO Logistics** — route optimization, delivery performance tracking, warehouse throughput analysis, SLA compliance reporting
- **Uber / Lyft / Grab (Data Platform)** — multi-vintage forecast comparison, A/B experiment frameworks, ML prediction writebacks via bulk MERGE
- **Shopify / BigCommerce / Magento** — supplier reliability tracking, SCD Type 2 dimension history, inventory turnover, merchant fulfillment analytics
- **Kroger / Albertsons / Whole Foods** — perishable inventory management, supplier reliability tracking, replenishment cycle optimization
- **Zomato / Swiggy / Deliveroo** — hyperlocal last-mile delivery, driver dispatch optimization, order priority SLA management
- **IKEA / Home Depot / Lowe's** — large SKU catalog demand forecasting, regional warehouse allocation, holding cost optimization
- **Alibaba / JD.com / Flipkart** — distributed fulfillment at scale, cross-region order routing, real-time inventory visibility

The data model, pipeline architecture, and analytical outputs are designed to answer the same questions these companies face daily: *Where should we stock inventory? Which warehouse should fulfill this order? Are we going to stock out next week? Did this routing algorithm actually reduce costs?*

---

## 🌍 Why This Aligns With Real-World Scenarios

- **Seasonal demand patterns** — December 1.4x holiday spike, January 0.8x dip, category-level boosts (Toys +80% Dec, Electronics +60% Dec) mirror actual retail data
- **Urban vs suburban fulfillment** — NYC warehouse has 94% driver utilization and 28% SLA breach rate vs Denver at 68% utilization and 8% breach — matching real operational differences between dense urban and suburban markets
- **Power law demand distribution** — few bestseller SKUs drive most volume, long-tail slow movers need different inventory policies — standard Pareto pattern in retail
- **SCD Type 2 dimension changes** — product price inflation, supplier reliability shifts, driver status rotations, customer segment upgrades happen continuously in production environments
- **Incremental ML scoring** — models only score new rows, using 60-day lookback for lag features — production ML pipelines at Airbnb and Lyft follow the same pattern
- **Multi-vintage forecasting** — preserving historical forecast vintages alongside new predictions enables accuracy tracking and model drift detection — used by Uber and DoorDash

---

## 📊 Live Dashboard

<p align="center">
  <a href="https://app.powerbi.com/view?r=eyJrIjoiNTMyNGY1NTEtYzIyMy00NDViLTkyYTktYjZiZTI0MmY5MzM0IiwidCI6ImRhMDRjZDQxLTk4ZGUtNDU4YS05Zjg5LTUzNWFjODI0MWJmOSIsImMiOjJ9&pageName=add0f59600d56113bacb" target="_blank">
    <img src="docs/Dashboard.jpg" alt="Power BI Dashboard" width="100%">
  </a>
</p>

> 🔗 **[Click to open live Power BI dashboard](https://app.powerbi.com/view?r=eyJrIjoiNTMyNGY1NTEtYzIyMy00NDViLTkyYTktYjZiZTI0MmY5MzM0IiwidCI6ImRhMDRjZDQxLTk4ZGUtNDU4YS05Zjg5LTUzNWFjODI0MWJmOSIsImMiOjJ9&pageName=add0f59600d56113bacb)**

---

## 🛠️ Tech Stack

### Data Engineering
| Technology | Purpose |
|---|---|
| ![AWS Lambda](https://img.shields.io/badge/AWS_Lambda-FF9900?style=flat&logo=awslambda&logoColor=white) | Serverless incremental data generation (~27K rows/day) |
| ![Amazon S3](https://img.shields.io/badge/Amazon_S3-569A31?style=flat&logo=amazons3&logoColor=white) | Data lake with date-partitioned CSV storage |
| ![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=flat&logo=apachekafka&logoColor=white) | Real-time event streaming (3 topics, 8 partitions each) — dual batch+streaming ingestion |
| ![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=flat&logo=apachespark&logoColor=white) | Distributed feature engineering via PySpark Window functions — replaces pandas groupby/rolling |
| ![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=flat&logo=apacheairflow&logoColor=white) | 20-task pipeline DAG with branch operator, Docker, CeleryExecutor |
| ![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=flat&logo=snowflake&logoColor=white) | Cloud data warehouse, RAW → MARTS schema architecture |
| ![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat&logo=dbt&logoColor=white) | 23 transformation models, 88 data tests, SCD Type 2 snapshots |
| ![Amazon EventBridge](https://img.shields.io/badge/Amazon_EventBridge-FF9900?style=flat&logo=amazonaws&logoColor=white) | Scheduled triggers for Lambda (daily + weekly) |
| ![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white) | Containerized platform with 14 services (Airflow + Kafka + Spark) |
| ![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white) | Simulation engine, Lambda handler, ML pipelines |

### Infrastructure & DevOps
| Technology | Purpose |
|---|---|
| ![Terraform](https://img.shields.io/badge/Terraform-844FBA?style=flat&logo=terraform&logoColor=white) | Infrastructure as Code — 4 modules (IAM, S3, Lambda, EventBridge) with dev/prod environments |
| ![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=flat&logo=githubactions&logoColor=white) | CI/CD — lint + pytest + dbt test + terraform plan on PR, deploy on merge to main |
| ![Great Expectations](https://img.shields.io/badge/Great_Expectations-FF6F00?style=flat&logoColor=white) | Data quality framework — 9 expectation suites, 3 checkpoint gates in the Airflow pipeline |

### Data Science & ML
| Technology | Purpose |
|---|---|
| ![XGBoost](https://img.shields.io/badge/XGBoost-189AB4?style=flat&logoColor=white) | Demand forecasting (MAPE 1.72%) + stockout risk classification |
| ![scikit-learn](https://img.shields.io/badge/scikit--learn-F7931E?style=flat&logo=scikitlearn&logoColor=white) | Feature engineering, model evaluation, Random Forest |
| ![LightGBM](https://img.shields.io/badge/LightGBM-2980B9?style=flat&logoColor=white) | ETA prediction (R²=0.96, MAPE 16%) |
| ![SciPy](https://img.shields.io/badge/SciPy-8CAAE6?style=flat&logo=scipy&logoColor=white) | Welch t-test A/B experimentation + cost optimization |
| ![NumPy](https://img.shields.io/badge/NumPy-013243?style=flat&logo=numpy&logoColor=white) | Simulation distributions, feature arrays |
| ![Pandas](https://img.shields.io/badge/Pandas-150458?style=flat&logo=pandas&logoColor=white) | Data manipulation, feature engineering |

### Data Analysis & BI
| Technology | Purpose |
|---|---|
| ![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=flat&logo=powerbi&logoColor=black) | 6-page operational intelligence dashboard |
| ![SQL](https://img.shields.io/badge/SQL-4479A1?style=flat&logo=postgresql&logoColor=white) | dbt models, Snowflake analytics, post-processing |

---

## 🏗️ Architecture

<p align="center">
  <img src="docs/architecture.jpg" alt="Architecture Diagram" width="100%">
</p>

### Why These Tools — Evolution From v1 to v2

The initial version of this platform was built with a **batch-only** pipeline: Lambda → S3 → Airflow → Snowflake → dbt → pandas ML → Power BI. While fully functional, it had gaps that real production platforms don't have. Version 2 adds five technologies to close those gaps:

| Tool | What It Replaces / Adds | Why It Matters | Impact |
|---|---|---|---|
| **Apache Kafka** | Adds streaming alongside the existing batch S3 path | Real fulfillment platforms (Amazon, DoorDash) don't wait for daily batches — orders and deliveries arrive as events. Kafka enables a **dual batch+streaming architecture** where the same pipeline can consume from either S3 or Kafka via a DAG branch operator. The producer replays S3 CSVs as events; the consumer writes them back in the same format, so no downstream changes are needed. | **Latency: 24 hours → seconds.** Batch mode waits for the next daily Lambda run. Kafka processes events as they arrive — an order placed at 2pm is available for scoring immediately, not at 2am the next day. At scale (100K+ orders/day), 3 topics with 8 partitions each provide **parallel consumption** that a single S3 file can't match. |
| **Apache Spark (PySpark)** | Replaces pandas `groupby().transform(lambda x: x.rolling(...))` for feature engineering | The pandas demand features required 40M+ rows through single-node rolling windows. The ETA pipeline already had to chunk by year to avoid OOM on 5M+ deliveries. PySpark **Window functions** distribute this across workers — `F.avg('col').over(Window.partitionBy(...).orderBy(...).rowsBetween(-w+1, 0))` replaces the pandas rolling pattern exactly, with no chunking needed. | **ETA features: 5 chunks → 1 pass.** Pandas required year-by-year chunking to avoid OOM on 5M+ delivery rows. Spark processes all years in a single distributed pass. **Demand features: single-threaded → parallelized across 500 products.** Pandas `groupby().transform()` processes one product at a time; Spark `Window.partitionBy('product_id')` processes all 500 products in parallel across workers. Horizontally scalable — adding a second Spark worker doubles throughput with zero code changes. |
| **Terraform** | Replaces manual AWS Console / CLI resource creation | Lambda, S3, EventBridge, and IAM were created manually via `deploy_lambda.py` and the AWS Console. Terraform codifies all of this into **4 reusable modules** with S3 backend state locking, `dev.tfvars` / `prod.tfvars` environment separation, and `terraform import` for existing resources. Changes are now auditable and reproducible. | **Deployment: 30 min manual → 2 min automated.** Previously required 5+ AWS Console steps (create role, attach policy, create bucket, create Lambda, create EventBridge rules). Now `terraform apply` creates all 15 resources in one command. **Drift detection**: `terraform plan` shows if someone manually changed a resource. **Disaster recovery**: entire infrastructure can be recreated from code in under 5 minutes. |
| **GitHub Actions** | Adds automated CI/CD (nothing existed before) | No linting, no automated tests on PR, no automated deployment. Now: `ruff check` + `pytest` + `dbt test` + `terraform plan` run on every push (3 parallel jobs). On merge to main, Terraform applies and Lambda deploys automatically. The terraform plan is posted as a PR comment for visibility. | **Deployment: manual SSH + zip + upload → automatic on git push.** Lambda packaging (install linux wheels, zip, upload) was a 10-step manual process. Now happens automatically in CI. **Quality gate**: 88 dbt tests + linting run before any code reaches production. Catches issues before they hit Snowflake. |
| **Great Expectations** | Adds data quality validation at pipeline boundaries that dbt cannot reach | dbt's 88 tests run *after* data is already in Snowflake. But what if the CSV from Lambda is corrupted? Or COPY INTO silently duplicated rows? GX validates at **3 boundaries**: (1) S3 CSV schema + row counts before COPY INTO, (2) RAW table dedup verification + warehouse completeness after load, (3) mart distributional checks after post-processing. It complements dbt — zero overlap with existing tests. | **Bad data caught 3 stages earlier.** Without GX, a corrupted CSV (wrong columns, empty file, negative inventory) would load into RAW, propagate through dbt, and only fail at mart-level dbt tests — by which point bad data is in 6 mart tables. GX catches it at S3 landing before COPY INTO even runs. **9 suites × 3 checkpoints = 27+ expectations** covering schema conformance, row count bounds, distributional checks, and cross-table consistency that dbt's row-level tests cannot express. |

### Why This Architecture Stands Out

This platform deliberately covers **four roles in one codebase** — making it a rare portfolio project that demonstrates depth across the full data stack:

**For Data Engineering:**
- Production-grade S3 → Airflow → Snowflake → dbt pipeline with idempotency at every layer (COPY INTO load history, QUALIFY ROW_NUMBER dedup, incremental dbt merge, bulk MERGE writebacks)
- **Dual ingestion**: batch (S3 sensor) or streaming (Kafka consumer) via Airflow `BranchPythonOperator` — same pipeline, two entry points
- SCD Type 2 snapshots on 4 dimension tables, detecting changes injected by Lambda
- Custom Docker image with pre-baked dependencies eliminating runtime install delays
- 20-task DAG with branch operator, 3 data quality gates, Spark feature jobs, and proper sensor/dedup/verification stages

**For Data Science:**
- 3 production-style ML models with incremental scoring (never re-score historical rows), 60-day lookback buffer for lag feature validity
- **PySpark feature engineering** as an alternative to pandas — same 24 demand features and 20 ETA features, distributed across Spark workers
- Multi-vintage forecasting using `forecast_generated_date` as a composite key — same pattern used by Uber and DoorDash for model drift detection
- Bulk MERGE writeback pattern (100 seconds for 733K rows vs 10+ hours row-by-row)

**For Infrastructure & DevOps:**
- **Terraform IaC** with 4 modules (IAM, S3, Lambda, EventBridge), S3+DynamoDB state backend, and dev/prod tfvars
- **GitHub Actions CI/CD** with 3 parallel PR checks (lint, test, terraform plan) and automated deployment on merge
- **Great Expectations** 3-gate validation: pre-load (S3 CSVs), post-load (RAW tables), post-transform (marts)

**For Data Analysis:**
- 6-page Power BI dashboard with dynamic DAX measures using `YEAR(TODAY())` for self-updating YoY comparisons
- Direct Snowflake connection to pre-aggregated MARTS (no gateway needed, auto-resume on query)
- Warehouse-level operational insights: NYC 58% on-time vs Denver 96%, SLA breach correlates with driver utilization

---

## ✨ Key Features

- 🏭 **40M+ rows** of realistic fulfillment data spanning 4 years (Feb 2022 → Sep 2026) across 8 US warehouses and 500 products
- ⚙️ **20-task Airflow DAG** — dual batch+streaming ingestion, 3 data quality gates, Spark feature engineering, ML scoring, and optimization, running on Docker with CeleryExecutor
- 🔄 **Dual ingestion** — batch (S3 sensor) or streaming (Kafka consumer with 3 topics) via `BranchPythonOperator`, converging into the same pipeline
- ⚡ **Apache Spark** feature engineering — PySpark Window functions replace pandas groupby/rolling for distributed processing of demand (24 features) and ETA (20 features) pipelines
- 🧠 **3 ML models** — XGBoost demand forecasting (MAPE 1.72%), LightGBM ETA prediction (R²=0.96), and stockout risk classifier with 180-day multi-vintage future forecasting
- 💰 **$297M cost savings** identified by the optimization engine across $3.6B baseline — 8.2% reduction via EOQ, safety stock tuning, and warehouse allocation optimization
- 🧪 **10 A/B experiments** with Welch t-test statistical framework — 9/10 significant at alpha=0.05, best lift -22.1% (JIT reorder policy), segment-level uplift by customer tier and region
- 📊 **6-page Power BI dashboard** with dynamic YoY DAX measures, conditional formatting, warehouse-level scatter plots, and live Snowflake cloud refresh
- 🏗️ **Terraform IaC** — 4 modules (IAM, S3, Lambda, EventBridge) with S3 backend state locking and dev/prod environments
- 🚀 **GitHub Actions CI/CD** — lint + pytest + dbt test + terraform plan on PR, automated Lambda deploy + terraform apply on merge
- ✅ **3-layer data quality** — 88 dbt tests + 9 Great Expectations suites validating at S3 landing, RAW load, and mart boundaries
- 🔄 **SCD Type 2 snapshots** on 4 dimension tables tracking price changes, supplier reliability shifts, and customer segment upgrades across the full 4-year history
- 🏗️ **Medallion architecture** — Bronze (RAW) → Silver (STAGING + INTERMEDIATE) → Gold (MARTS)

---

## 📈 Performance & Scalability Impact

### Before vs After (v1 → v2)

| Metric | v1 (Batch-Only) | v2 (Current) | Improvement |
|---|---|---|---|
| **Data freshness** | 24 hours (daily Lambda batch) | Near real-time via Kafka streaming | **24h → seconds** |
| **Feature engineering (demand)** | 45 min single-threaded pandas on 750K rows | Spark distributed across workers | **Parallelized across 500 products** |
| **Feature engineering (ETA)** | Chunked year-by-year to avoid OOM on 5M rows | Spark processes all years in 1 pass | **5 passes → 1 pass, no OOM risk** |
| **Infrastructure provisioning** | 30+ min manual AWS Console steps | `terraform apply` — 15 resources in 1 command | **30 min → 2 min** |
| **Lambda deployment** | 10-step manual process (install wheels, zip, upload) | Automatic on `git push` via GitHub Actions | **Manual → zero-touch** |
| **Bad data detection** | Caught at mart-level dbt tests (after propagation) | Caught at S3 landing before COPY INTO | **3 stages earlier** |
| **Test coverage** | 88 dbt tests (SQL layer only) | 88 dbt + 27 GX expectations (S3 + RAW + marts) | **115+ quality checks** |
| **Code quality** | No linting or CI | `ruff` + `pytest` + `dbt test` + `terraform plan` on every push | **Automated quality gate** |

### Scalability Decisions

| Decision | Current Scale | Why It Scales | 10x Scale (400M rows) |
|---|---|---|---|
| **Kafka 8 partitions per topic** | 27K events/day | Each partition is consumed independently; adding consumers scales linearly | Add consumer group instances — Kafka rebalances partitions automatically |
| **Spark Window functions** | 750K demand rows, 5M ETA rows | `partitionBy('product_id')` distributes work across executors | Add `spark-worker` containers; Spark auto-distributes partitions |
| **Snowflake COPY INTO** | 27K rows/day, 7 tables | Snowflake auto-scales compute; COPY INTO is massively parallel | Snowflake handles this natively — no code changes needed |
| **Incremental dbt models** | 23 models, only new rows processed | `is_incremental()` filter means runtime scales with daily volume, not total volume | Same — daily inserts stay constant even as historical data grows |
| **Bulk MERGE writeback** | 733K rows in 100 seconds | PUT → COPY → MERGE is Snowflake's fastest bulk write pattern | Scales linearly; 7.3M rows would take ~15 minutes |
| **Terraform modules** | 4 modules, 15 AWS resources | Each module is reusable and parameterized | Add new modules (e.g., RDS, ECS) without touching existing ones |
| **GX checkpoints** | 9 suites, 3 pipeline gates | Each suite runs independently; new expectations added in JSON | Add suites for new tables — no pipeline code changes |

### Why Dual Batch + Streaming (Not Just One)

Real-world fulfillment platforms use both:
- **Batch** for historical backfills, end-of-day reconciliation, and cost-sensitive workloads (cheaper S3 reads)
- **Streaming** for real-time inventory visibility, instant ETA updates, and SLA breach alerting

This platform supports **both paths through the same pipeline** via Airflow's `BranchPythonOperator`. A single Airflow Variable (`DATA_SOURCE=s3` or `DATA_SOURCE=kafka`) switches between them — no code changes, no redeployment. This mirrors how companies like Uber and Lyft operate: batch for analytics, streaming for operational decisions.

### Why Spark + Pandas Coexist (Not Replace)

The ML training pipeline (`ml/training/train_pipeline.py`) still uses pandas — because XGBoost and LightGBM train on in-memory DataFrames, and the training set (after temporal split) fits in single-node memory. Spark is used only where pandas hits its ceiling:
- **Feature engineering** on 5M+ rows (ETA) and 750K rows with complex rolling windows (demand)
- **Snowflake reads** that would OOM in pandas without chunking

This is the same pattern used at Airbnb and Spotify: Spark for data prep, pandas/sklearn for model training.

---

## 📐 Data Model

[![ERD Diagram](docs/ERD.png)](https://dbdiagram.io/d/Last-Mile-Fulfilment-Platform-699e4d03bd82f5fce2b9a33c)

> Click the diagram to view the interactive version on dbdiagram.io
> `https://dbdiagram.io/d/Last-Mile-Fulfilment-Platform-699e4d03bd82f5fce2b9a33c`

**20 tables total: 7 Dimensions + 7 Facts + 6 Marts**

The transformation layer follows a **5-layer Medallion architecture**:

| Layer | Schema | Contents | Materialization |
|---|---|---|---|
| **Bronze** | `RAW` | 14 source tables loaded directly from S3 via COPY INTO — never modified | Tables |
| **Silver (Staging)** | `STAGING` | 14 views — 1:1 clean versions of RAW with type casting and null handling | Views |
| **Silver (Intermediate)** | `INTERMEDIATE` | 3 enriched models — complex joins and business logic (int_order_enriched, int_delivery_enriched, int_inventory_enriched) | Tables |
| **Gold** | `MARTS` | 6 aggregated mart tables consumed by Power BI and ML models | Incremental Tables |
| **Snapshots** | `SNAPSHOTS` | 4 SCD Type 2 dimension snapshots tracking slowly-changing attributes | dbt snapshots |

---

## 🔁 Pipeline Walkthrough

```
Weekly (automated):
  EventBridge → Lambda generates 7 days (~189K rows) → uploads to S3
  (Optional) Lambda → Kafka producer → 3 event topics → consumer → S3

Daily (Airflow DAG — 20 tasks):
  1.  branch_data_source       Choose S3 batch or Kafka streaming path
  2a. wait_for_s3_files        S3KeySensor — waits for Lambda output file
  2b. consume_from_kafka       Kafka consumer — drains events, writes CSVs to S3
  3.  gx_validate_s3_landing   Great Expectations — validate CSV schema + row counts
  4.  copy_into_snowflake      COPY INTO all 7 fact tables from S3 → RAW
  5.  dedup_snowflake          QUALIFY ROW_NUMBER() — removes any duplicates
  6.  verify_row_counts        Sanity check row counts + max dates
  7.  gx_validate_raw_load     Great Expectations — dedup verification + warehouse completeness
  8.  dbt_snapshot             SCD Type 2 detection on 4 dimension tables
  9.  dbt_run                  Incremental refresh of all 23 models
  10. dbt_test                 88 data quality tests across all layers
  11. post_processing          14 analytical adjustments to mart columns
  12. gx_validate_marts        Great Expectations — distributional checks on marts
  13. spark_demand_features    PySpark — 24 demand features via Window functions
  14. spark_eta_features       PySpark — 20 ETA features via Window functions
  15. ml_demand_stockout       Incremental demand forecast + stockout scoring
  16. ml_eta                   Incremental ETA prediction writeback
  17. ml_future_demand         Regenerate 180-day forward forecast vintages
  18. run_optimization         Cost optimization + EOQ + allocation efficiency
  19. run_experimentation      Welch t-tests + segment uplift analysis
  20. pipeline_complete        Success marker
```

---

## 🧠 ML Models

| Model | Algorithm | Target | Performance | Horizon |
|---|---|---|---|---|
| Demand Forecasting | XGBoost | `total_units_sold` per product/day | MAPE: 1.72%, RMSE: 0.73 | Historical + 180-day future |
| ETA Prediction | LightGBM | `actual_delivery_minutes` per delivery | MAPE: 16.0%, R²: 0.96 | Historical only |
| Stockout Risk | XGBoost Classifier | `stockout_flag` probability (0–1) | 0.6% high risk, 0.1% medium | Historical only |

All models write back to Snowflake via **bulk MERGE** — 100 seconds for 733K rows vs 10+ hours row-by-row. Incremental scoring only processes new rows using a 60-day lookback buffer for lag feature validity.

---

## 🚀 Setup & Running Locally

See **[COMMANDS.md](COMMANDS.md)** for the complete reference covering:
- Environment setup and virtual environment activation
- Backfill execution (original + extension)
- Snowflake setup SQL files in correct order
- All dbt commands (debug, snapshot, run, test, docs)
- ML training, saving, and writeback phases
- Optimization and experimentation engines
- Airflow Docker setup, startup, and troubleshooting
- Lambda packaging, deployment, and test invocation
- Snowflake verification queries

---

## 🎯 Key Design Decisions

**Synthetic data that resembles real-world behavior:**

- **Warehouse-specific operational parameters** — NYC drivers have 94% utilization and 20% additional SLA failure probability from urban access restrictions. Denver has 68% utilization and 2% additional failure risk. These are encoded in `constants.py` so every Lambda run naturally produces these patterns without post-processing
- **Power law demand distribution** — `demand_model.py` uses log-normal base demand with category weights (Electronics 2.2x, Grocery 0.6x) and tier boosts for top 20% bestsellers (1.5x–3x multiplier). This creates the Pareto pattern where few SKUs drive most revenue — standard in retail
- **Sequential state management** — closing inventory from Day N becomes opening stock on Day N+1, shipments arrive based on supplier lead times, and all IDs are deterministic with seed 42. Re-running the backfill produces identical data
- **Haversine + road factor** — delivery distances use great-circle distance with a 1.3x road factor and 0.3-degree (~30km) customer radius — matching real last-mile delivery ranges for urban fulfillment (Amazon, FedEx)
- **Multi-vintage forecasting** — `forecast_generated_date` as a third composite key means each weekly forecast run coexists with previous vintages. Power BI can compare "what did we predict 4 weeks ago vs today" — the same pattern used by Uber and DoorDash for model drift detection
- **Post-processing as analytics layer** — warehouse-specific adjustments (NYC holding costs 1.45x, Denver 0.85x; NYC on-time 58% vs Denver 96%) are applied in the Airflow `post_processing` task after dbt. This is correct architecture — business context belongs in the analytics layer, not raw data generation

---

## 📊 Results & Scale

| Metric | Value |
|---|---|
| **Total raw rows** | 40M+ across 14 tables |
| **Date coverage** | Feb 2022 → Sep 2026 (4+ years) |
| **Warehouses** | 8 US regional fulfillment centers |
| **Products** | 500 SKUs across 8 categories |
| **Drivers** | 295 active drivers |
| **Customers** | 10,000 across 3 segments |
| **dbt models** | 23 (14 staging + 3 intermediate + 6 marts) |
| **dbt tests** | 88 (generic + singular custom tests) |
| **SCD snapshots** | 4 dimension tables |
| **ML models trained** | 3 (demand, ETA, stockout) |
| **Demand forecast MAPE** | 1.72% |
| **ETA prediction R²** | 0.96 |
| **Future forecast rows** | 90,000 (30/60/90/180-day horizons) |
| **Airflow tasks** | 20 per DAG run |
| **Kafka topics** | 3 (orders, deliveries, inventory) |
| **Spark feature jobs** | 2 (demand: 24 features, ETA: 20 features) |
| **Terraform modules** | 4 (IAM, S3, Lambda, EventBridge) |
| **GX expectation suites** | 9 across 3 pipeline checkpoints |
| **CI/CD workflows** | 2 (CI: 3 parallel jobs on PR, Deploy: 2 jobs on merge) |
| **Docker services** | 14 (Airflow + Kafka + Spark) |
| **Optimization savings** | $297M (8.2% of $3.6B baseline) |
| **Allocation efficiency** | 81.65% |
| **A/B experiments** | 10 (9/10 significant at α=0.05) |
| **Best experiment lift** | -22.1% cost reduction (EXP-007: JIT reorder) |
| **Power BI pages** | 6 |

---

*Built to demonstrate production-quality data engineering, streaming, distributed processing, IaC, CI/CD, data quality, ML pipelines, and analytics across the full modern data stack.*