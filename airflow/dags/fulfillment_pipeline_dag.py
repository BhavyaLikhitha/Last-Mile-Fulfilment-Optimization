"""
Fulfillment Platform — Main Pipeline DAG
Airflow 3.x compatible

Key design decisions:
- BashOperator tasks use full paths to avoid PATH issues
- dbt runs via full path /home/airflow/.local/bin/dbt
- ML/optimization/experimentation use PythonOperator to avoid subprocess PATH issues
- S3 sensor uses mode='poke' for simpler testing
- start_date before first run date so manual triggers work
- Dedup step after COPY INTO prevents duplicate rows from re-loading S3 files
"""

from datetime import datetime, timedelta
import os
import sys

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
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

DEDUP_SQL = """
USE DATABASE FULFILLMENT_DB;
USE SCHEMA RAW;
USE WAREHOUSE FULFILLMENT_WH;

CREATE OR REPLACE TABLE FACT_ORDERS AS
SELECT * FROM FACT_ORDERS
QUALIFY ROW_NUMBER() OVER (PARTITION BY ORDER_ID ORDER BY CREATED_AT DESC) = 1;

CREATE OR REPLACE TABLE FACT_ORDER_ITEMS AS
SELECT * FROM FACT_ORDER_ITEMS
QUALIFY ROW_NUMBER() OVER (PARTITION BY ORDER_ITEM_ID ORDER BY CREATED_AT DESC) = 1;

CREATE OR REPLACE TABLE FACT_DELIVERIES AS
SELECT * FROM FACT_DELIVERIES
QUALIFY ROW_NUMBER() OVER (PARTITION BY DELIVERY_ID ORDER BY CREATED_AT DESC) = 1;

CREATE OR REPLACE TABLE FACT_SHIPMENTS AS
SELECT * FROM FACT_SHIPMENTS
QUALIFY ROW_NUMBER() OVER (PARTITION BY SHIPMENT_ID ORDER BY CREATED_AT DESC) = 1;

CREATE OR REPLACE TABLE FACT_EXPERIMENT_ASSIGNMENTS AS
SELECT * FROM FACT_EXPERIMENT_ASSIGNMENTS
QUALIFY ROW_NUMBER() OVER (PARTITION BY ASSIGNMENT_ID ORDER BY CREATED_AT DESC) = 1;

CREATE OR REPLACE TABLE FACT_INVENTORY_SNAPSHOT AS
SELECT * FROM FACT_INVENTORY_SNAPSHOT
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY SNAPSHOT_DATE, WAREHOUSE_ID, PRODUCT_ID
    ORDER BY CREATED_AT DESC
) = 1;

CREATE OR REPLACE TABLE FACT_DRIVER_ACTIVITY AS
SELECT * FROM FACT_DRIVER_ACTIVITY
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY DRIVER_ID, ACTIVITY_DATE
    ORDER BY CREATED_AT DESC
) = 1;
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

# def run_ml_eta():
#     sys.path.insert(0, PROJECT_DIR)
#     os.chdir(PROJECT_DIR)
#     from ml.training.predict_and_writeback import predict_eta, get_snowflake_connection
#     conn = get_snowflake_connection()
#     cur  = conn.cursor()
#     try:
#         predict_eta(conn, cur)
#     finally:
#         cur.close()
#         conn.close()


def run_ml_eta():
    sys.path.insert(0, PROJECT_DIR)
    os.chdir(PROJECT_DIR)
    from ml.training.predict_and_writeback import predict_eta
    predict_eta()

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
    # this is weekly schedule for production, but daily for testing to speed up iterations
    # schedule='0 6 * * 1', 
    # this is daily schedule for testing
    # schedule='0 6 * * *',
    schedule='0 15 * * *',
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

    # Task 3: Deduplicate after COPY INTO
    dedup_snowflake = SQLExecuteQueryOperator(
        task_id='dedup_snowflake',
        sql=DEDUP_SQL,
        conn_id='snowflake_default',
    )

    # Task 4: Verify row counts
    verify_row_counts = SQLExecuteQueryOperator(
        task_id='verify_row_counts',
        sql=VERIFY_SQL,
        conn_id='snowflake_default',
    )

    # Task 5: dbt snapshot — full path to dbt binary
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

    # Task 6: dbt run
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

    # Task 7: dbt test
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

    # Tasks 8-10: ML — PythonOperator avoids PATH issues
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

    # Task 11: Optimization
    run_optimization_task = PythonOperator(
        task_id='run_optimization',
        python_callable=run_optimization,
    )

    # Task 12: Experimentation
    run_experimentation_task = PythonOperator(
        task_id='run_experimentation',
        python_callable=run_experimentation,
    )

    # Task 13: Done
    pipeline_complete = BashOperator(
        task_id='pipeline_complete',
        bash_command=f'echo "FULFILLMENT PIPELINE COMPLETE — $(date)"',
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Dependencies
    (
        wait_for_s3_files
        >> copy_into_snowflake
        >> dedup_snowflake
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