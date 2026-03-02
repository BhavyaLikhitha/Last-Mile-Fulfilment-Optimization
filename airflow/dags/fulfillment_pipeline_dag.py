# """
# Fulfillment Platform — Main Pipeline DAG
# Orchestrates the full weekly data pipeline:

#   S3 Sensor (wait for Lambda files)
#       ↓
#   COPY INTO Snowflake (load new fact data)
#       ↓
#   dbt snapshot (SCD Type 2)
#       ↓
#   dbt run (incremental marts)
#       ↓
#   dbt test (data quality)
#       ↓
#   ML: demand + stockout (incremental scoring)
#       ↓
#   ML: ETA (incremental scoring)
#       ↓
#   ML: future demand (regenerate forecast)
#       ↓
#   Optimization (cost model + EOQ)
#       ↓
#   Experimentation (A/B tests + uplift)

# Schedule: Weekly on Monday at 6am UTC
#   (Lambda runs Sunday night → files land in S3 → DAG picks up Monday morning)

# Connections required:
#   - snowflake_default: Snowflake connection
#   - aws_default: AWS connection for S3 sensor
# """

# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.operators.python import PythonOperator
# from airflow.utils.trigger_rule import TriggerRule
# import os

# # ── Default arguments ─────────────────────────────────────────
# default_args = {
#     'owner'           : 'fulfillment-platform',
#     'depends_on_past' : False,
#     'email_on_failure': False,
#     'email_on_retry'  : False,
#     'retries'         : 2,
#     'retry_delay'     : timedelta(minutes=5),
#     'execution_timeout': timedelta(hours=2),
# }

# # ── Project paths inside Docker container ─────────────────────
# PROJECT_DIR = '/opt/airflow/project'
# DBT_DIR     = f'{PROJECT_DIR}/dbt'
# VENV_PYTHON = f'{PROJECT_DIR}/.venv/bin/python'

# # S3 bucket and prefix where Lambda drops new files
# S3_BUCKET = os.getenv('S3_BUCKET_NAME', 'last-mile-fulfillment-platform')

# # ── Helper: build bash command with project path ──────────────
# def project_cmd(cmd: str) -> str:
#     """Run a command from the project directory."""
#     return f"cd {PROJECT_DIR} && {cmd}"

# def python_cmd(module: str, args: str = '') -> str:
#     """Run a Python module from the project directory."""
#     return project_cmd(f"{VENV_PYTHON} -m {module} {args}")

# def dbt_cmd(command: str) -> str:
#     """Run a dbt command."""
#     return f"cd {DBT_DIR} && dbt {command} --profiles-dir {DBT_DIR}"


# # ── Snowflake COPY INTO SQL ───────────────────────────────────
# COPY_INTO_SQL = """
# USE DATABASE FULFILLMENT_DB;
# USE SCHEMA RAW;
# USE WAREHOUSE FULFILLMENT_WH;

# -- Load new fact data only (load history skips already-loaded files)
# COPY INTO FACT_ORDERS
# FROM @s3_fulfillment_stage/fact_orders/
# FILE_FORMAT = csv_format
# PATTERN = '.*data\\.csv'
# ON_ERROR = 'CONTINUE';

# COPY INTO FACT_ORDER_ITEMS
# FROM @s3_fulfillment_stage/fact_order_items/
# FILE_FORMAT = csv_format
# PATTERN = '.*data\\.csv'
# ON_ERROR = 'CONTINUE';

# COPY INTO FACT_INVENTORY_SNAPSHOT
# FROM @s3_fulfillment_stage/fact_inventory_snapshot/
# FILE_FORMAT = csv_format
# PATTERN = '.*data\\.csv'
# ON_ERROR = 'CONTINUE';

# COPY INTO FACT_SHIPMENTS
# FROM @s3_fulfillment_stage/fact_shipments/
# FILE_FORMAT = csv_format
# PATTERN = '.*data\\.csv'
# ON_ERROR = 'CONTINUE';

# COPY INTO FACT_DELIVERIES
# FROM @s3_fulfillment_stage/fact_deliveries/
# FILE_FORMAT = csv_format
# PATTERN = '.*data\\.csv'
# ON_ERROR = 'CONTINUE';

# COPY INTO FACT_DRIVER_ACTIVITY
# FROM @s3_fulfillment_stage/fact_driver_activity/
# FILE_FORMAT = csv_format
# PATTERN = '.*data\\.csv'
# ON_ERROR = 'CONTINUE';

# COPY INTO FACT_EXPERIMENT_ASSIGNMENTS
# FROM @s3_fulfillment_stage/fact_experiment_assignments/
# FILE_FORMAT = csv_format
# PATTERN = '.*data\\.csv'
# ON_ERROR = 'CONTINUE';
# """

# ROW_COUNT_CHECK_SQL = """
# SELECT
#     'FACT_ORDERS'   AS tbl, COUNT(*) AS row_count, MAX(order_date) AS max_date
#     FROM FULFILLMENT_DB.RAW.FACT_ORDERS
# UNION ALL SELECT 'FACT_DELIVERIES', COUNT(*), NULL
#     FROM FULFILLMENT_DB.RAW.FACT_DELIVERIES
# UNION ALL SELECT 'FACT_INVENTORY_SNAPSHOT', COUNT(*), MAX(snapshot_date)
#     FROM FULFILLMENT_DB.RAW.FACT_INVENTORY_SNAPSHOT
# ORDER BY 1;
# """


# # ── DAG Definition ────────────────────────────────────────────
# with DAG(
#     dag_id='fulfillment_pipeline',
#     default_args=default_args,
#     description='Weekly fulfillment platform pipeline: S3 → Snowflake → dbt → ML → Optimization → Experimentation',
#     schedule_interval='0 6 * * 1',   # Every Monday at 6am UTC
#     start_date=datetime(2026, 3, 2),
#     catchup=False,
#     max_active_runs=1,               # Only one run at a time
#     tags=['fulfillment', 'weekly', 'production'],
# ) as dag:

#     # ── Task 1: S3 Sensor ─────────────────────────────────────
#     # Wait for Lambda to drop new files in S3 before proceeding
#     # Checks for the Monday date partition in fact_orders
#     wait_for_s3_files = S3KeySensor(
#         task_id='wait_for_s3_files',
#         bucket_name=S3_BUCKET,
#         # Check for fact_orders partition for this week
#         # Lambda drops files like: raw/fact_orders/date=2026-03-07/data.csv
#         bucket_key='raw/fact_orders/date={{ ds }}/data.csv',
#         aws_conn_id='aws_default',
#         timeout=60 * 60 * 6,         # Wait up to 6 hours for Lambda
#         poke_interval=60 * 5,        # Check every 5 minutes
#         mode='reschedule',           # Release worker slot while waiting
#     )

#     # ── Task 2: COPY INTO Snowflake ───────────────────────────
#     copy_into_snowflake = SnowflakeOperator(
#         task_id='copy_into_snowflake',
#         sql=COPY_INTO_SQL,
#         snowflake_conn_id='snowflake_default',
#     )

#     # ── Task 3: Row count verification ───────────────────────
#     verify_row_counts = SnowflakeOperator(
#         task_id='verify_row_counts',
#         sql=ROW_COUNT_CHECK_SQL,
#         snowflake_conn_id='snowflake_default',
#     )

#     # ── Task 4: dbt snapshot (SCD Type 2) ────────────────────
#     dbt_snapshot = BashOperator(
#         task_id='dbt_snapshot',
#         bash_command=dbt_cmd('snapshot'),
#     )

#     # ── Task 5: dbt run (incremental marts) ──────────────────
#     dbt_run = BashOperator(
#         task_id='dbt_run',
#         bash_command=dbt_cmd('run'),
#     )

#     # ── Task 6: dbt test (data quality) ──────────────────────
#     # Uses warn trigger rule so pipeline continues even if tests warn
#     dbt_test = BashOperator(
#         task_id='dbt_test',
#         bash_command=dbt_cmd('test'),
#         trigger_rule=TriggerRule.ALL_SUCCESS,
#     )

#     # ── Task 7: ML — demand + stockout (incremental) ─────────
#     ml_demand_stockout = BashOperator(
#         task_id='ml_demand_stockout',
#         bash_command=python_cmd(
#             'ml.training.predict_and_writeback',
#             '--phase demand stockout'
#         ),
#     )

#     # ── Task 8: ML — ETA prediction (incremental) ────────────
#     ml_eta = BashOperator(
#         task_id='ml_eta',
#         bash_command=python_cmd(
#             'ml.training.predict_and_writeback',
#             '--phase eta'
#         ),
#     )

#     # ── Task 9: ML — future demand forecast ──────────────────
#     ml_future_demand = BashOperator(
#         task_id='ml_future_demand',
#         bash_command=python_cmd(
#             'ml.training.predict_and_writeback',
#             '--phase future_demand'
#         ),
#     )

#     # ── Task 10: Optimization ─────────────────────────────────
#     run_optimization = BashOperator(
#         task_id='run_optimization',
#         bash_command=python_cmd('optimization.run_optimization'),
#     )

#     # ── Task 11: Experimentation ──────────────────────────────
#     run_experimentation = BashOperator(
#         task_id='run_experimentation',
#         bash_command=python_cmd('experimentation.run_experimentation'),
#     )

#     # ── Task 12: Pipeline complete notification ───────────────
#     pipeline_complete = BashOperator(
#         task_id='pipeline_complete',
#         bash_command="""
#             echo "============================================"
#             echo "  FULFILLMENT PIPELINE COMPLETE"
#             echo "  Run date: {{ ds }}"
#             echo "  Logical date: {{ logical_date }}"
#             echo "============================================"
#         """,
#         trigger_rule=TriggerRule.ALL_SUCCESS,
#     )

#     # ── Dependencies ──────────────────────────────────────────
#     (
#         wait_for_s3_files
#         >> copy_into_snowflake
#         >> verify_row_counts
#         >> dbt_snapshot
#         >> dbt_run
#         >> dbt_test
#         >> ml_demand_stockout
#         >> ml_eta
#         >> ml_future_demand
#         >> run_optimization
#         >> run_experimentation
#         >> pipeline_complete
#     )


"""
Fulfillment Platform — Main Pipeline DAG
Airflow 3.x compatible

Key design decisions:
- BashOperator tasks use full paths to avoid PATH issues
- dbt runs via full path /home/airflow/.local/bin/dbt
- ML/optimization/experimentation use PythonOperator to avoid subprocess PATH issues
- S3 sensor uses mode='poke' for simpler testing
- start_date before first run date so manual triggers work
"""

from datetime import datetime, timedelta
import os
import sys

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.task.trigger_rule import TriggerRule

# ── Constants ─────────────────────────────────────────────────
PROJECT_DIR = '/opt/airflow/project'
DBT_DIR     = f'{PROJECT_DIR}/dbt'
DBT_BIN     = '/home/airflow/.local/bin/dbt'   # full path avoids PATH issues
PYTHON_BIN  = '/usr/local/bin/python'           # system python in container

S3_BUCKET   = os.getenv('S3_BUCKET_NAME', 'last-mile-fulfillment-platform')

# ── Default arguments ─────────────────────────────────────────
default_args = {
    'owner'            : 'fulfillment-platform',
    'depends_on_past'  : False,
    'email_on_failure' : False,
    'email_on_retry'   : False,
    'retries'          : 1,
    'retry_delay'      : timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# ── Snowflake SQL ─────────────────────────────────────────────
COPY_INTO_SQL = """
USE DATABASE FULFILLMENT_DB;
USE SCHEMA RAW;
USE WAREHOUSE FULFILLMENT_WH;

COPY INTO FACT_ORDERS
FROM @s3_fulfillment_stage/fact_orders/
FILE_FORMAT = csv_format PATTERN = '.*data\\.csv' ON_ERROR = 'CONTINUE';

COPY INTO FACT_ORDER_ITEMS
FROM @s3_fulfillment_stage/fact_order_items/
FILE_FORMAT = csv_format PATTERN = '.*data\\.csv' ON_ERROR = 'CONTINUE';

COPY INTO FACT_INVENTORY_SNAPSHOT
FROM @s3_fulfillment_stage/fact_inventory_snapshot/
FILE_FORMAT = csv_format PATTERN = '.*data\\.csv' ON_ERROR = 'CONTINUE';

COPY INTO FACT_SHIPMENTS
FROM @s3_fulfillment_stage/fact_shipments/
FILE_FORMAT = csv_format PATTERN = '.*data\\.csv' ON_ERROR = 'CONTINUE';

COPY INTO FACT_DELIVERIES
FROM @s3_fulfillment_stage/fact_deliveries/
FILE_FORMAT = csv_format PATTERN = '.*data\\.csv' ON_ERROR = 'CONTINUE';

COPY INTO FACT_DRIVER_ACTIVITY
FROM @s3_fulfillment_stage/fact_driver_activity/
FILE_FORMAT = csv_format PATTERN = '.*data\\.csv' ON_ERROR = 'CONTINUE';

COPY INTO FACT_EXPERIMENT_ASSIGNMENTS
FROM @s3_fulfillment_stage/fact_experiment_assignments/
FILE_FORMAT = csv_format PATTERN = '.*data\\.csv' ON_ERROR = 'CONTINUE';
"""

VERIFY_SQL = """
SELECT 'FACT_ORDERS' AS tbl, COUNT(*) AS row_count, MAX(order_date) AS max_date
FROM FULFILLMENT_DB.RAW.FACT_ORDERS
UNION ALL
SELECT 'FACT_INVENTORY_SNAPSHOT', COUNT(*), MAX(snapshot_date)
FROM FULFILLMENT_DB.RAW.FACT_INVENTORY_SNAPSHOT
ORDER BY 1;
"""

# ── Python callables for ML/Optimization/Experimentation ──────
# Using PythonOperator avoids subprocess PATH issues entirely

def run_ml_demand_stockout():
    sys.path.insert(0, PROJECT_DIR)
    os.chdir(PROJECT_DIR)
    from ml.training.predict_and_writeback import predict_demand, predict_stockout, get_snowflake_connection
    conn = get_snowflake_connection()
    cur  = conn.cursor()
    try:
        predict_demand()
        predict_stockout()
    finally:
        cur.close()
        conn.close()

def run_ml_eta():
    sys.path.insert(0, PROJECT_DIR)
    os.chdir(PROJECT_DIR)
    from ml.training.predict_and_writeback import predict_eta, get_snowflake_connection
    conn = get_snowflake_connection()
    cur  = conn.cursor()
    try:
        predict_eta(conn, cur)
    finally:
        cur.close()
        conn.close()

def run_ml_future_demand():
    sys.path.insert(0, PROJECT_DIR)
    os.chdir(PROJECT_DIR)
    from ml.training.predict_and_writeback import predict_future_demand, get_snowflake_connection
    conn = get_snowflake_connection()
    cur  = conn.cursor()
    try:
        predict_future_demand()
    finally:
        cur.close()
        conn.close()

def run_optimization():
    sys.path.insert(0, PROJECT_DIR)
    os.chdir(PROJECT_DIR)
    from optimization.run_optimization import run_optimization as _run
    _run(mode='full')

def run_experimentation():
    sys.path.insert(0, PROJECT_DIR)
    os.chdir(PROJECT_DIR)
    from experimentation.run_experimentation import run_experimentation as _run
    _run(mode='full')


# ── DAG ───────────────────────────────────────────────────────
with DAG(
    dag_id='fulfillment_pipeline',
    default_args=default_args,
    description='Weekly fulfillment platform pipeline',
    schedule='0 6 * * 1',
    start_date=datetime(2026, 2, 28),
    catchup=False,
    max_active_runs=1,
    tags=['fulfillment', 'weekly', 'production'],
) as dag:

    # Task 1: Wait for Lambda files in S3
    wait_for_s3_files = S3KeySensor(
        task_id='wait_for_s3_files',
        bucket_name=S3_BUCKET,
        bucket_key='raw/fact_orders/date={{ ds }}/data.csv',
        aws_conn_id='aws_default',
        timeout=60 * 60 * 6,
        poke_interval=60 * 5,
        mode='poke',
    )

    # Task 2: COPY INTO Snowflake
    copy_into_snowflake = SQLExecuteQueryOperator(
        task_id='copy_into_snowflake',
        sql=COPY_INTO_SQL,
        conn_id='snowflake_default',
    )

    # Task 3: Verify row counts
    verify_row_counts = SQLExecuteQueryOperator(
        task_id='verify_row_counts',
        sql=VERIFY_SQL,
        conn_id='snowflake_default',
    )

    # Task 4: dbt snapshot — full path to dbt binary
    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot',
        bash_command=f'cd {DBT_DIR} && {DBT_BIN} snapshot --profiles-dir {DBT_DIR}',
        env={
            'PATH'               : f'/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin',
            'SNOWFLAKE_ACCOUNT'  : os.getenv('SNOWFLAKE_ACCOUNT', ''),
            'SNOWFLAKE_USER'     : os.getenv('SNOWFLAKE_USER', ''),
            'SNOWFLAKE_PASSWORD' : os.getenv('SNOWFLAKE_PASSWORD', ''),
            'SNOWFLAKE_DATABASE' : os.getenv('SNOWFLAKE_DATABASE', 'FULFILLMENT_DB'),
            'SNOWFLAKE_WAREHOUSE': os.getenv('SNOWFLAKE_WAREHOUSE', 'FULFILLMENT_WH'),
        },
    )

    # Task 5: dbt run
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {DBT_DIR} && {DBT_BIN} run --profiles-dir {DBT_DIR}',
        env={
            'PATH'               : f'/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin',
            'SNOWFLAKE_ACCOUNT'  : os.getenv('SNOWFLAKE_ACCOUNT', ''),
            'SNOWFLAKE_USER'     : os.getenv('SNOWFLAKE_USER', ''),
            'SNOWFLAKE_PASSWORD' : os.getenv('SNOWFLAKE_PASSWORD', ''),
            'SNOWFLAKE_DATABASE' : os.getenv('SNOWFLAKE_DATABASE', 'FULFILLMENT_DB'),
            'SNOWFLAKE_WAREHOUSE': os.getenv('SNOWFLAKE_WAREHOUSE', 'FULFILLMENT_WH'),
        },
    )

    # Task 6: dbt test
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_DIR} && {DBT_BIN} test --profiles-dir {DBT_DIR}',
        env={
            'PATH'               : f'/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin',
            'SNOWFLAKE_ACCOUNT'  : os.getenv('SNOWFLAKE_ACCOUNT', ''),
            'SNOWFLAKE_USER'     : os.getenv('SNOWFLAKE_USER', ''),
            'SNOWFLAKE_PASSWORD' : os.getenv('SNOWFLAKE_PASSWORD', ''),
            'SNOWFLAKE_DATABASE' : os.getenv('SNOWFLAKE_DATABASE', 'FULFILLMENT_DB'),
            'SNOWFLAKE_WAREHOUSE': os.getenv('SNOWFLAKE_WAREHOUSE', 'FULFILLMENT_WH'),
        },
    )

    # Tasks 7-9: ML — PythonOperator avoids PATH issues
    ml_demand_stockout = PythonOperator(
        task_id='ml_demand_stockout',
        python_callable=run_ml_demand_stockout,
    )

    ml_eta = PythonOperator(
        task_id='ml_eta',
        python_callable=run_ml_eta,
    )

    ml_future_demand = PythonOperator(
        task_id='ml_future_demand',
        python_callable=run_ml_future_demand,
    )

    # Task 10: Optimization
    run_optimization_task = PythonOperator(
        task_id='run_optimization',
        python_callable=run_optimization,
    )

    # Task 11: Experimentation
    run_experimentation_task = PythonOperator(
        task_id='run_experimentation',
        python_callable=run_experimentation,
    )

    # Task 12: Done
    pipeline_complete = BashOperator(
        task_id='pipeline_complete',
        bash_command=f'echo "FULFILLMENT PIPELINE COMPLETE — $(date)"',
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Dependencies
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
        >> run_optimization_task
        >> run_experimentation_task
        >> pipeline_complete
    )