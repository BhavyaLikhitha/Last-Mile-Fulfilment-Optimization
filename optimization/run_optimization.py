"""
Optimization Engine — Main Entry Point
Reads baseline costs from Snowflake marts, runs cost and allocation optimization,
and writes results back to mart_cost_optimization.

Writes to:
  - mart_cost_optimization: baseline_total_cost, optimized_total_cost,
                            savings_amount, savings_pct,
                            holding_cost_baseline, holding_cost_optimized,
                            transport_cost_baseline, transport_cost_optimized,
                            allocation_efficiency_pct

Usage:
    python -m optimization.run_optimization
    python -m optimization.run_optimization --mode cost        # cost model only
    python -m optimization.run_optimization --mode allocation  # allocation only
    python -m optimization.run_optimization --mode full        # both (default)
"""

import os
import sys
import time
import argparse
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from optimization.cost_model import (
    compute_baseline_costs,
    compute_optimized_costs,
    compute_allocation_efficiency
)
from optimization.inventory_optimization import compute_inventory_optimization_summary
from optimization.warehouse_allocation import optimize_warehouse_allocation_greedy


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
    """Fetch via Arrow batches — faster than pd.read_sql for large tables."""
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
    """Generic bulk MERGE pattern via PUT → COPY INTO → MERGE."""
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


# ── Cost Optimization ─────────────────────────────────────────

def run_cost_optimization(conn, cur):
    """
    Pull warehouse KPIs, compute baseline vs optimized costs,
    merge into mart_cost_optimization.
    """
    print("\n" + "=" * 60)
    print("  COST OPTIMIZATION — BASELINE VS OPTIMIZED")
    print("=" * 60)
    start = time.time()

    print("\n  Loading mart_daily_warehouse_kpis...")
    warehouse_kpis = fast_query(conn, "SELECT * FROM MARTS.MART_DAILY_WAREHOUSE_KPIS")
    print(f"  Loaded {len(warehouse_kpis):,} rows")

    # Compute baseline costs from mart data
    print("  Computing baseline costs...")
    df = compute_baseline_costs(warehouse_kpis)

    # Apply optimization reductions
    print("  Applying optimization model...")
    df = compute_optimized_costs(df)

    # Load allocation efficiency from orders
    print("  Computing allocation efficiency...")
    orders = fast_query(conn, """
        SELECT order_id, order_date, assigned_warehouse_id, nearest_warehouse_id
        FROM RAW.FACT_ORDERS
        WHERE order_status != 'Cancelled'
    """)
    alloc_eff = compute_allocation_efficiency(orders)

    # Merge allocation efficiency into cost df
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    alloc_eff['date'] = pd.to_datetime(alloc_eff['date']).dt.strftime('%Y-%m-%d')
    df = df.merge(alloc_eff, on=['date', 'warehouse_id'], how='left')
    df['allocation_efficiency_pct'] = df['allocation_efficiency_pct'].fillna(0).round(2)

    # Build writeback dataframe
    writeback = df[[
        'date', 'warehouse_id',
        'baseline_total_cost', 'optimized_total_cost',
        'savings_amount', 'savings_pct',
        'holding_cost_baseline', 'holding_cost_optimized',
        'transport_cost_baseline', 'transport_cost_optimized',
        'allocation_efficiency_pct'
    ]].copy()

    print(f"\n  Cost optimization summary:")
    print(f"    Total baseline cost : ${writeback['baseline_total_cost'].sum():>15,.2f}")
    print(f"    Total optimized cost: ${writeback['optimized_total_cost'].sum():>15,.2f}")
    print(f"    Total savings       : ${writeback['savings_amount'].sum():>15,.2f}")
    print(f"    Avg savings pct     : {writeback['savings_pct'].mean():>14.2f}%")
    print(f"    Avg allocation eff  : {writeback['allocation_efficiency_pct'].mean():>14.2f}%")

    cur.execute("USE SCHEMA MARTS")
    rows_merged = bulk_merge(
        cur=cur,
        df=writeback,
        temp_table="_temp_cost_optimization",
        temp_ddl="""
            date                      DATE,
            warehouse_id              VARCHAR(20),
            baseline_total_cost       DECIMAL(12,2),
            optimized_total_cost      DECIMAL(12,2),
            savings_amount            DECIMAL(12,2),
            savings_pct               DECIMAL(5,2),
            holding_cost_baseline     DECIMAL(10,2),
            holding_cost_optimized    DECIMAL(10,2),
            transport_cost_baseline   DECIMAL(10,2),
            transport_cost_optimized  DECIMAL(10,2),
            allocation_efficiency_pct DECIMAL(5,2)
        """,
        merge_sql="""
            MERGE INTO MART_COST_OPTIMIZATION t
            USING _temp_cost_optimization s
            ON t.DATE = s.DATE AND t.WAREHOUSE_ID = s.WAREHOUSE_ID
            WHEN MATCHED THEN UPDATE SET
                t.BASELINE_TOTAL_COST       = s.BASELINE_TOTAL_COST,
                t.OPTIMIZED_TOTAL_COST      = s.OPTIMIZED_TOTAL_COST,
                t.SAVINGS_AMOUNT            = s.SAVINGS_AMOUNT,
                t.SAVINGS_PCT               = s.SAVINGS_PCT,
                t.HOLDING_COST_BASELINE     = s.HOLDING_COST_BASELINE,
                t.HOLDING_COST_OPTIMIZED    = s.HOLDING_COST_OPTIMIZED,
                t.TRANSPORT_COST_BASELINE   = s.TRANSPORT_COST_BASELINE,
                t.TRANSPORT_COST_OPTIMIZED  = s.TRANSPORT_COST_OPTIMIZED,
                t.ALLOCATION_EFFICIENCY_PCT = s.ALLOCATION_EFFICIENCY_PCT
            WHEN NOT MATCHED THEN INSERT (
                DATE, WAREHOUSE_ID,
                BASELINE_TOTAL_COST, OPTIMIZED_TOTAL_COST,
                SAVINGS_AMOUNT, SAVINGS_PCT,
                HOLDING_COST_BASELINE, HOLDING_COST_OPTIMIZED,
                TRANSPORT_COST_BASELINE, TRANSPORT_COST_OPTIMIZED,
                ALLOCATION_EFFICIENCY_PCT
            ) VALUES (
                s.DATE, s.WAREHOUSE_ID,
                s.BASELINE_TOTAL_COST, s.OPTIMIZED_TOTAL_COST,
                s.SAVINGS_AMOUNT, s.SAVINGS_PCT,
                s.HOLDING_COST_BASELINE, s.HOLDING_COST_OPTIMIZED,
                s.TRANSPORT_COST_BASELINE, s.TRANSPORT_COST_OPTIMIZED,
                s.ALLOCATION_EFFICIENCY_PCT
            )
        """,
        temp_path='optimization/results/_temp_cost_optimization.csv'
    )
    conn.commit()

    print(f"\n  ✓ Merged {rows_merged:,} rows into mart_cost_optimization")
    print(f"  ✓ Completed in {time.time() - start:.0f}s")


# ── Inventory Optimization Summary ───────────────────────────

def run_inventory_optimization(conn):
    """
    Run EOQ and safety stock optimization across all products.
    Results saved locally as CSV for reference — not written to Snowflake
    since this is a product-level summary, not the warehouse × day mart grain.
    """
    print("\n" + "=" * 60)
    print("  INVENTORY OPTIMIZATION — EOQ & SAFETY STOCK")
    print("=" * 60)
    start = time.time()

    print("\n  Loading data...")
    product_kpis = fast_query(conn, """
        SELECT product_id, total_units_sold, avg_closing_stock,
               total_holding_cost, is_forecast
        FROM MARTS.MART_DAILY_PRODUCT_KPIS
        WHERE is_forecast = FALSE
    """)
    products = fast_query(conn, """
        SELECT product_id, cost_price, lead_time_days, safety_stock, reorder_point
        FROM STAGING.STG_PRODUCTS WHERE IS_CURRENT = TRUE
    """)
    suppliers = fast_query(conn, "SELECT lead_time_std_dev FROM STAGING.STG_SUPPLIERS")

    print(f"  Loaded {len(product_kpis):,} product KPI rows, {len(products):,} products")

    print("  Running EOQ optimization...")
    results = compute_inventory_optimization_summary(product_kpis, products, suppliers)

    os.makedirs('optimization/results', exist_ok=True)
    output_path = 'optimization/results/inventory_optimization_results.csv'
    results.to_csv(output_path, index=False)

    print(f"\n  EOQ Optimization Summary:")
    print(f"    Products optimized: {len(results):,}")
    print(f"    Avg EOQ           : {results['eoq'].mean():.1f} units")
    print(f"    Avg optimal SS    : {results['optimal_safety_stock'].mean():.1f} units")
    print(f"    Avg current SS    : {results['current_safety_stock'].mean():.1f} units")

    print(f"\n  ✓ Results saved to {output_path}")
    print(f"  ✓ Completed in {time.time() - start:.0f}s")
    return results


# ── Main Entry Point ──────────────────────────────────────────

def run_optimization(mode: str = 'full'):
    print("=" * 60)
    print("  FULFILLMENT PLATFORM — OPTIMIZATION ENGINE")
    print(f"  Mode: {mode}")
    print("=" * 60)

    total_start = time.time()
    conn = get_snowflake_connection()
    cur  = conn.cursor()

    try:
        if mode in ('full', 'cost'):
            run_cost_optimization(conn, cur)

        if mode in ('full', 'inventory'):
            run_inventory_optimization(conn)

    finally:
        cur.close()
        conn.close()

    print(f"\n{'=' * 60}")
    print(f"  Optimization complete in {time.time() - total_start:.0f}s")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fulfillment Optimization Engine')
    parser.add_argument(
        '--mode',
        choices=['full', 'cost', 'inventory', 'allocation'],
        default='full',
        help='full=all, cost=cost model only, inventory=EOQ only (default: full)'
    )
    args = parser.parse_args()
    run_optimization(mode=args.mode)