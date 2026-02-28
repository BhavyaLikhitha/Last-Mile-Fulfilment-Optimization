"""
Experimentation Engine — Main Entry Point
Runs A/B tests for all 10 experiments and writes statistical results
back to mart_experiment_results in Snowflake.

Writes to mart_experiment_results:
  - p_value                  : Welch t-test p-value
  - confidence_interval_lower: 95% CI lower bound on mean difference
  - confidence_interval_upper: 95% CI upper bound on mean difference
  - is_significant           : TRUE if p_value < 0.05

Also saves uplift analysis CSVs to experimentation/results/

Usage:
    python -m experimentation.run_experimentation
    python -m experimentation.run_experimentation --mode stats     # t-tests only
    python -m experimentation.run_experimentation --mode uplift    # uplift only
    python -m experimentation.run_experimentation --mode full      # both (default)
"""

import os
import sys
import time
import argparse
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from experimentation.assignment_engine import load_experiment_data, build_all_experiment_data
from experimentation.statistical_tests import run_all_tests, print_summary
from experimentation.uplift_analysis import run_uplift_analysis, print_uplift_highlights


# ── Connection ────────────────────────────────────────────────

def get_snowflake_connection():
    from dotenv import load_dotenv
    import snowflake.connector
    load_dotenv()
    return snowflake.connector.connect(
        account  =os.getenv('SNOWFLAKE_ACCOUNT'),
        user     =os.getenv('SNOWFLAKE_USER'),
        password =os.getenv('SNOWFLAKE_PASSWORD'),
        database =os.getenv('SNOWFLAKE_DATABASE', 'FULFILLMENT_DB'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'FULFILLMENT_WH'),
    )


def fast_query(conn, sql: str) -> pd.DataFrame:
    cur = conn.cursor()
    try:
        cur.execute(sql)
        try:
            import pyarrow as pa
            batches = cur.fetch_arrow_batches()
            table = pa.concat_tables(list(batches))
            df = table.to_pandas()
        except Exception:
            df = pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])
    finally:
        cur.close()
    df.columns = [c.lower() for c in df.columns]
    return df


def bulk_merge(cur, df: pd.DataFrame, temp_table: str, temp_ddl: str,
               merge_sql: str, temp_path: str) -> int:
    os.makedirs(os.path.dirname(temp_path), exist_ok=True)
    df.to_csv(temp_path, index=False)
    abs_path = os.path.abspath(temp_path).replace('\\', '/')
    cur.execute(f"CREATE OR REPLACE TEMPORARY TABLE {temp_table} ({temp_ddl})")
    cur.execute(f"PUT file://{abs_path} @%{temp_table} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
    cur.execute(f"""
        COPY INTO {temp_table} FROM @%{temp_table}
        FILE_FORMAT = (TYPE='CSV' SKIP_HEADER=1
                       FIELD_OPTIONALLY_ENCLOSED_BY='"'
                       EMPTY_FIELD_AS_NULL=TRUE)
    """)
    cur.execute(merge_sql)
    rows = cur.rowcount
    try:
        os.remove(temp_path)
    except OSError:
        pass
    return rows


# ── Main Runs ─────────────────────────────────────────────────

def run_statistical_tests(conn, cur, experiment_data: dict) -> pd.DataFrame:
    """Run t-tests and write p_value, CI, is_significant to mart_experiment_results."""
    print("\n" + "=" * 60)
    print("  STATISTICAL TESTS — WELCH T-TEST")
    print("=" * 60)
    start = time.time()

    print("\n  Running Welch t-tests for all experiments...")
    results_df = run_all_tests(experiment_data)
    print_summary(results_df)

    # Build writeback — one row per experiment per group
    # The mart has Control and Treatment rows — we update both with same p_value/CI/is_significant
    writeback_rows = []
    for _, row in results_df.iterrows():
        for group in ['Control', 'Treatment']:
            writeback_rows.append({
                'experiment_id'            : row['experiment_id'],
                'group_name'               : group,
                'p_value'                  : row['p_value'],
                'confidence_interval_lower': row['ci_lower'],
                'confidence_interval_upper': row['ci_upper'],
                'is_significant'           : row['is_significant'],
            })

    writeback = pd.DataFrame(writeback_rows)

    # Handle None/NaN for Snowflake
    writeback['p_value']                   = writeback['p_value'].astype(float)
    writeback['confidence_interval_lower'] = writeback['confidence_interval_lower'].astype(float)
    writeback['confidence_interval_upper'] = writeback['confidence_interval_upper'].astype(float)
    writeback['is_significant']            = writeback['is_significant'].astype(bool)

    cur.execute("USE SCHEMA MARTS")
    rows_merged = bulk_merge(
        cur=cur,
        df=writeback,
        temp_table="_temp_experiment_stats",
        temp_ddl="""
            experiment_id             VARCHAR(20),
            group_name                VARCHAR(20),
            p_value                   DECIMAL(8,6),
            confidence_interval_lower DECIMAL(8,4),
            confidence_interval_upper DECIMAL(8,4),
            is_significant            BOOLEAN
        """,
        merge_sql="""
            MERGE INTO MART_EXPERIMENT_RESULTS t
            USING _temp_experiment_stats s
            ON t.EXPERIMENT_ID = s.EXPERIMENT_ID
            AND t.GROUP_NAME   = s.GROUP_NAME
            WHEN MATCHED THEN UPDATE SET
                t.P_VALUE                   = s.P_VALUE,
                t.CONFIDENCE_INTERVAL_LOWER = s.CONFIDENCE_INTERVAL_LOWER,
                t.CONFIDENCE_INTERVAL_UPPER = s.CONFIDENCE_INTERVAL_UPPER,
                t.IS_SIGNIFICANT            = s.IS_SIGNIFICANT
        """,
        temp_path='experimentation/results/_temp_experiment_stats.csv'
    )
    conn.commit()

    print(f"\n  ✓ Merged {rows_merged:,} rows into mart_experiment_results")
    print(f"  ✓ Completed in {time.time() - start:.0f}s")
    return results_df


def run_uplift(conn, experiment_data: dict):
    """Run segment-level uplift analysis and save to CSV."""
    print("\n" + "=" * 60)
    print("  UPLIFT ANALYSIS — SEGMENT BREAKDOWN")
    print("=" * 60)
    start = time.time()

    print("\n  Loading warehouse dimension for region mapping...")
    warehouses = fast_query(conn, "SELECT warehouse_id, region FROM RAW.DIM_WAREHOUSE")

    print("  Running uplift analysis...")
    all_uplift = run_uplift_analysis(experiment_data, warehouses)
    print_uplift_highlights(all_uplift)

    print(f"\n  ✓ Uplift CSVs saved to experimentation/results/")
    print(f"  ✓ Completed in {time.time() - start:.0f}s")


# ── Entry Point ───────────────────────────────────────────────

def run_experimentation(mode: str = 'full'):
    print("=" * 60)
    print("  FULFILLMENT PLATFORM — EXPERIMENTATION ENGINE")
    print(f"  Mode: {mode}")
    print("=" * 60)

    total_start = time.time()

    conn = get_snowflake_connection()
    cur  = conn.cursor()

    try:
        # Load all data once — reused for both stats and uplift
        print("\n  Loading experiment data from Snowflake...")
        experiments, assignments, orders, deliveries = load_experiment_data(conn)

        print("\n  Preparing observations with treatment effects...")
        experiment_data = build_all_experiment_data(
            experiments, assignments, orders, deliveries
        )
        print(f"  Ready: {len(experiment_data)} experiments prepared")

        if mode in ('full', 'stats'):
            run_statistical_tests(conn, cur, experiment_data)

        if mode in ('full', 'uplift'):
            run_uplift(conn, experiment_data)

    finally:
        cur.close()
        conn.close()

    print(f"\n{'=' * 60}")
    print(f"  Experimentation complete in {time.time() - total_start:.0f}s")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Experimentation Engine')
    parser.add_argument(
        '--mode',
        choices=['full', 'stats', 'uplift'],
        default='full',
        help='full=all, stats=t-tests+writeback only, uplift=segment analysis only (default: full)'
    )
    args = parser.parse_args()
    run_experimentation(mode=args.mode)