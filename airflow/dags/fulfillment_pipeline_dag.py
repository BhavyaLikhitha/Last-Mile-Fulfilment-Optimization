"""
Fulfillment Platform — Main Pipeline DAG
Orchestrates the full weekly data pipeline:

  S3 Sensor (wait for Lambda files)
      ↓
  COPY INTO Snowflake (load new fact data)
      ↓
  dbt snapshot (SCD Type 2)
      ↓
  dbt run (incremental marts)
      ↓
  dbt test (data quality)
      ↓
  ML: demand + stockout (incremental scoring)
      ↓
  ML: ETA (incremental scoring)
      ↓
  ML: future demand (regenerate forecast)
      ↓
  Optimization (cost model + EOQ)
      ↓
  Experimentation (A/B tests + uplift)

Schedule: Weekly on Monday at 6am UTC
  (Lambda runs Sunday night → files land in S3 → DAG picks up Monday morning)

Connections required:
  - snowflake_default: Snowflake connection
  - aws_default: AWS connection for S3 sensor
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import os

# ── Default arguments ─────────────────────────────────────────
default_args = {
    'owner'           : 'fulfillment-platform',
    'depends_on_past' : False,
    'email_on_failure': False,
    'email_on_retry'  : False,
    'retries'         : 2,
    'retry_delay'     : timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# ── Project paths inside Docker container ─────────────────────
PROJECT_DIR = '/opt/airflow/project'
DBT_DIR     = f'{PROJECT_DIR}/dbt'
VENV_PYTHON = f'{PROJECT_DIR}/.venv/bin/python'

# S3 bucket and prefix where Lambda drops new files
S3_BUCKET = os.getenv('S3_BUCKET_NAME', 'last-mile-fulfillment-platform')

# ── Helper: build bash command with project path ──────────────
def project_cmd(cmd: str) -> str:
    """Run a command from the project directory."""
    return f"cd {PROJECT_DIR} && {cmd}"

def python_cmd(module: str, args: str = '') -> str:
    """Run a Python module from the project directory."""
    return project_cmd(f"{VENV_PYTHON} -m {module} {args}")

def dbt_cmd(command: str) -> str:
    """Run a dbt command."""
    return f"cd {DBT_DIR} && dbt {command} --profiles-dir {DBT_DIR}"


# ── Snowflake COPY INTO SQL ───────────────────────────────────
COPY_INTO_SQL = """
USE DATABASE FULFILLMENT_DB;
USE SCHEMA RAW;
USE WAREHOUSE FULFILLMENT_WH;

-- Load new fact data only (load history skips already-loaded files)
COPY INTO FACT_ORDERS
FROM @s3_fulfillment_stage/fact_orders/
FILE_FORMAT = csv_format
PATTERN = '.*data\\.csv'
ON_ERROR = 'CONTINUE';

COPY INTO FACT_ORDER_ITEMS
FROM @s3_fulfillment_stage/fact_order_items/
FILE_FORMAT = csv_format
PATTERN = '.*data\\.csv'
ON_ERROR = 'CONTINUE';

COPY INTO FACT_INVENTORY_SNAPSHOT
FROM @s3_fulfillment_stage/fact_inventory_snapshot/
FILE_FORMAT = csv_format
PATTERN = '.*data\\.csv'
ON_ERROR = 'CONTINUE';

COPY INTO FACT_SHIPMENTS
FROM @s3_fulfillment_stage/fact_shipments/
FILE_FORMAT = csv_format
PATTERN = '.*data\\.csv'
ON_ERROR = 'CONTINUE';

COPY INTO FACT_DELIVERIES
FROM @s3_fulfillment_stage/fact_deliveries/
FILE_FORMAT = csv_format
PATTERN = '.*data\\.csv'
ON_ERROR = 'CONTINUE';

COPY INTO FACT_DRIVER_ACTIVITY
FROM @s3_fulfillment_stage/fact_driver_activity/
FILE_FORMAT = csv_format
PATTERN = '.*data\\.csv'
ON_ERROR = 'CONTINUE';

COPY INTO FACT_EXPERIMENT_ASSIGNMENTS
FROM @s3_fulfillment_stage/fact_experiment_assignments/
FILE_FORMAT = csv_format
PATTERN = '.*data\\.csv'
ON_ERROR = 'CONTINUE';
"""

ROW_COUNT_CHECK_SQL = """
SELECT
    'FACT_ORDERS'   AS tbl, COUNT(*) AS row_count, MAX(order_date) AS max_date
    FROM FULFILLMENT_DB.RAW.FACT_ORDERS
UNION ALL SELECT 'FACT_DELIVERIES', COUNT(*), NULL
    FROM FULFILLMENT_DB.RAW.FACT_DELIVERIES
UNION ALL SELECT 'FACT_INVENTORY_SNAPSHOT', COUNT(*), MAX(snapshot_date)
    FROM FULFILLMENT_DB.RAW.FACT_INVENTORY_SNAPSHOT
ORDER BY 1;
"""


# ── DAG Definition ────────────────────────────────────────────
with DAG(
    dag_id='fulfillment_pipeline',
    default_args=default_args,
    description='Weekly fulfillment platform pipeline: S3 → Snowflake → dbt → ML → Optimization → Experimentation',
    schedule_interval='0 6 * * 1',   # Every Monday at 6am UTC
    start_date=datetime(2026, 3, 2),
    catchup=False,
    max_active_runs=1,               # Only one run at a time
    tags=['fulfillment', 'weekly', 'production'],
) as dag:

    # ── Task 1: S3 Sensor ─────────────────────────────────────
    # Wait for Lambda to drop new files in S3 before proceeding
    # Checks for the Monday date partition in fact_orders
    wait_for_s3_files = S3KeySensor(
        task_id='wait_for_s3_files',
        bucket_name=S3_BUCKET,
        # Check for fact_orders partition for this week
        # Lambda drops files like: raw/fact_orders/date=2026-03-07/data.csv
        bucket_key='raw/fact_orders/date={{ ds }}/data.csv',
        aws_conn_id='aws_default',
        timeout=60 * 60 * 6,         # Wait up to 6 hours for Lambda
        poke_interval=60 * 5,        # Check every 5 minutes
        mode='reschedule',           # Release worker slot while waiting
    )

    # ── Task 2: COPY INTO Snowflake ───────────────────────────
    copy_into_snowflake = SnowflakeOperator(
        task_id='copy_into_snowflake',
        sql=COPY_INTO_SQL,
        snowflake_conn_id='snowflake_default',
    )

    # ── Task 3: Row count verification ───────────────────────
    verify_row_counts = SnowflakeOperator(
        task_id='verify_row_counts',
        sql=ROW_COUNT_CHECK_SQL,
        snowflake_conn_id='snowflake_default',
    )

    # ── Task 4: dbt snapshot (SCD Type 2) ────────────────────
    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot',
        bash_command=dbt_cmd('snapshot'),
    )

    # ── Task 5: dbt run (incremental marts) ──────────────────
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=dbt_cmd('run'),
    )

    # ── Task 6: dbt test (data quality) ──────────────────────
    # Uses warn trigger rule so pipeline continues even if tests warn
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=dbt_cmd('test'),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Task 7: ML — demand + stockout (incremental) ─────────
    ml_demand_stockout = BashOperator(
        task_id='ml_demand_stockout',
        bash_command=python_cmd(
            'ml.training.predict_and_writeback',
            '--phase demand stockout'
        ),
    )

    # ── Task 8: ML — ETA prediction (incremental) ────────────
    ml_eta = BashOperator(
        task_id='ml_eta',
        bash_command=python_cmd(
            'ml.training.predict_and_writeback',
            '--phase eta'
        ),
    )

    # ── Task 9: ML — future demand forecast ──────────────────
    ml_future_demand = BashOperator(
        task_id='ml_future_demand',
        bash_command=python_cmd(
            'ml.training.predict_and_writeback',
            '--phase future_demand'
        ),
    )

    # ── Task 10: Optimization ─────────────────────────────────
    run_optimization = BashOperator(
        task_id='run_optimization',
        bash_command=python_cmd('optimization.run_optimization'),
    )

    # ── Task 11: Experimentation ──────────────────────────────
    run_experimentation = BashOperator(
        task_id='run_experimentation',
        bash_command=python_cmd('experimentation.run_experimentation'),
    )

    # ── Task 12: Pipeline complete notification ───────────────
    pipeline_complete = BashOperator(
        task_id='pipeline_complete',
        bash_command="""
            echo "============================================"
            echo "  FULFILLMENT PIPELINE COMPLETE"
            echo "  Run date: {{ ds }}"
            echo "  Logical date: {{ logical_date }}"
            echo "============================================"
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Dependencies ──────────────────────────────────────────
    (
        wait_for_s3_files
        >> copy_into_snowflake
        >> verify_row_counts
        >> dbt_snapshot
        >> dbt_run
        >> dbt_test
        >> ml_demand_stockout
        >> ml_eta
        >> ml_future_demand
        >> run_optimization
        >> run_experimentation
        >> pipeline_complete
    )