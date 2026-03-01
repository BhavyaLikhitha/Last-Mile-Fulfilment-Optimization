"""
Extension backfill: generates data from Feb 2, 2025 → Feb 28, 2026.
Picks up where the original backfill left off.

Key differences from backfill.py:
  - Skips dimension generation (dims already exist in Snowflake)
  - Starts from EXTENSION_START_DATE, not BACKFILL_START_DATE
  - Initializes fresh inventory state (original state was not saved to disk)
  - Extends dim_date to cover the new date range
  - Injects SCD Type 2 changes into all 4 dimension tables:
      snap_product   : price changes + safety stock adjustments (~50 products)
      snap_supplier  : reliability + lead time changes (all 6 suppliers)
      snap_driver    : status cycles + vehicle upgrades (~30 drivers)
      snap_customer  : segment upgrades (~200 customers)
  - Uses day_counter offset so IDs don't collide with original backfill

Usage:
    cd Last-Mile-Fulfilment-Optimization
    python -m data_simulation.backfill_extension

Output:
    output_extension/raw/dim_date/data.csv           <- new date rows only
    output_extension/raw/dim_product/data.csv        <- updated with SCD changes
    output_extension/raw/dim_supplier/data.csv       <- updated with SCD changes
    output_extension/raw/dim_driver/data.csv         <- updated with SCD changes
    output_extension/raw/dim_customer/data.csv       <- updated with SCD changes
    output_extension/raw/fact_orders/date=.../       <- new fact data
    ...

After running:
    1. Upload output_extension/raw/ to S3
    2. Run copy_into_extension.sql in Snowflake
    3. cd dbt && dbt snapshot   <- picks up SCD Type 2 changes
    4. dbt run                  <- incremental marts
    5. dbt test
    6. Re-run ML, optimization, experimentation pipelines
"""

import os
import sys
import time
import numpy as np
import pandas as pd
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.constants import RANDOM_SEED, BACKFILL_START_DATE
from data_simulation.core.dimensions import (
    generate_dim_product, generate_dim_warehouse, generate_dim_supplier,
    generate_dim_driver, generate_dim_customer, generate_dim_experiments,
)
from data_simulation.core.orders import generate_daily_orders
from data_simulation.core.inventory import (
    initialize_inventory, generate_daily_inventory_snapshot,
)
from data_simulation.core.shipments import generate_daily_shipments
from data_simulation.core.deliveries import generate_daily_deliveries
from data_simulation.core.driver_activity import generate_daily_driver_activity
from data_simulation.core.experiments import generate_daily_experiment_assignments
from data_simulation.state.state_manager import SimulationState

# ── Extension date range ──────────────────────────────────────
EXTENSION_START_DATE   = date(2025, 2, 2)
EXTENSION_END_DATE     = date(2026, 2, 28)
ORIGINAL_BACKFILL_DAYS = (date(2025, 2, 1) - BACKFILL_START_DATE).days + 1  # 1096

OUTPUT_DIR = "output_extension"


def save_csv(df: pd.DataFrame, table_name: str, partition_date: date = None):
    if partition_date:
        path = os.path.join(OUTPUT_DIR, "raw", table_name, f"date={partition_date.isoformat()}")
    else:
        path = os.path.join(OUTPUT_DIR, "raw", table_name)
    os.makedirs(path, exist_ok=True)
    df.to_csv(os.path.join(path, "data.csv"), index=False)


# ─────────────────────────────────────────────────────────────
#  SCD TYPE 2 DIMENSION CHANGES
# ─────────────────────────────────────────────────────────────

def inject_product_changes(products_df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """
    Inject realistic product dimension changes for SCD Type 2 tracking.

    Changes:
      - 25 products: price increase 5-15% (inflation / demand increase)
      - 20 products: safety_stock increased 20-40% (after historical stockouts)
      - 15 products: reorder_point adjusted ±10-20% (operational tuning)

    Total: ~50 unique products affected (some overlap between change types).
    """
    df = products_df.copy()
    n = len(df)

    # Price increases — 25 products, 5-15% increase
    price_idx = rng.choice(n, size=25, replace=False)
    price_multiplier = rng.uniform(1.05, 1.15, size=25)
    df.loc[price_idx, 'cost_price']    = (df.loc[price_idx, 'cost_price'] * price_multiplier).round(2)
    df.loc[price_idx, 'selling_price'] = (df.loc[price_idx, 'selling_price'] * price_multiplier).round(2)

    # Safety stock increases — 20 products that had stockouts, increase 20-40%
    ss_idx = rng.choice(n, size=20, replace=False)
    ss_multiplier = rng.uniform(1.20, 1.40, size=20)
    df.loc[ss_idx, 'safety_stock'] = (df.loc[ss_idx, 'safety_stock'] * ss_multiplier).round(0).astype(int)

    # Reorder point adjustments — 15 products, ±10-20%
    rop_idx = rng.choice(n, size=15, replace=False)
    rop_multiplier = rng.uniform(0.80, 1.20, size=15)
    df.loc[rop_idx, 'reorder_point'] = (df.loc[rop_idx, 'reorder_point'] * rop_multiplier).round(0).astype(int)

    changed = len(set(price_idx) | set(ss_idx) | set(rop_idx))
    print(f"  snap_product : {changed} products changed")
    print(f"    Price increases   : {len(price_idx)} products (+5-15%)")
    print(f"    Safety stock up   : {len(ss_idx)} products (+20-40%)")
    print(f"    Reorder pt tuned  : {len(rop_idx)} products (±10-20%)")

    return df


def inject_supplier_changes(suppliers_df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """
    Inject supplier dimension changes for SCD Type 2 tracking.

    Changes:
      - 3 suppliers: reliability_score degrades slightly (supply chain disruption)
      - 3 suppliers: reliability_score improves (supplier performance program)
      - 4 suppliers: average_lead_time adjusted ±1-2 days
    """
    df = suppliers_df.copy()
    n = len(df)

    # Reliability changes — split 3 degrade, 3 improve
    indices = rng.permutation(n)
    degrade_idx = indices[:3]
    improve_idx = indices[3:6] if n >= 6 else indices[3:]

    # Degrade: -0.02 to -0.05
    df.loc[degrade_idx, 'reliability_score'] = (
        df.loc[degrade_idx, 'reliability_score'] - rng.uniform(0.02, 0.05, size=len(degrade_idx))
    ).clip(0.70, 1.0).round(2)

    # Improve: +0.01 to +0.03
    df.loc[improve_idx, 'reliability_score'] = (
        df.loc[improve_idx, 'reliability_score'] + rng.uniform(0.01, 0.03, size=len(improve_idx))
    ).clip(0.70, 1.0).round(2)

    # Lead time changes — 4 suppliers ±1-2 days
    lt_idx = rng.choice(n, size=min(4, n), replace=False)
    lt_delta = rng.integers(-2, 3, size=len(lt_idx))  # -2 to +2 days
    df.loc[lt_idx, 'average_lead_time'] = (
        df.loc[lt_idx, 'average_lead_time'] + lt_delta
    ).clip(1, 14)

    print(f"  snap_supplier: {n} suppliers updated")
    print(f"    Reliability degraded : {len(degrade_idx)} suppliers")
    print(f"    Reliability improved : {len(improve_idx)} suppliers")
    print(f"    Lead time adjusted   : {len(lt_idx)} suppliers (±1-2 days)")

    return df


def inject_driver_changes(drivers_df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """
    Inject driver dimension changes for SCD Type 2 tracking.

    Changes:
      - 20 drivers: availability_status cycles Active → On Leave → Active
        (represents vacation/sick leave during extension period)
      - 10 drivers: vehicle_type upgrade (Car → Van, Van → Truck)
        (represents fleet upgrades / promotions)
    """
    df = drivers_df.copy()
    n = len(df)

    # Status changes — 20 drivers currently Active go On Leave
    active_mask = df['availability_status'] == 'Active'
    active_idx  = df[active_mask].index.tolist()
    leave_idx   = rng.choice(active_idx, size=min(20, len(active_idx)), replace=False)
    df.loc[leave_idx, 'availability_status'] = 'On Leave'

    # Vehicle upgrades — 10 drivers promoted
    upgrade_map = {'Car': 'Van', 'Van': 'Truck'}
    upgradeable = df[df['vehicle_type'].isin(['Car', 'Van'])].index.tolist()
    upgrade_idx = rng.choice(upgradeable, size=min(10, len(upgradeable)), replace=False)
    df.loc[upgrade_idx, 'vehicle_type'] = df.loc[upgrade_idx, 'vehicle_type'].map(upgrade_map)

    # Capacity update for upgraded drivers
    capacity_map = {'Van': (15, 25), 'Truck': (25, 40)}
    for idx in upgrade_idx:
        new_type = df.loc[idx, 'vehicle_type']
        if new_type in capacity_map:
            lo, hi = capacity_map[new_type]
            df.loc[idx, 'max_delivery_capacity'] = int(rng.integers(lo, hi + 1))

    print(f"  snap_driver  : {len(leave_idx) + len(upgrade_idx)} drivers changed")
    print(f"    Status -> On Leave    : {len(leave_idx)} drivers")
    print(f"    Vehicle upgraded      : {len(upgrade_idx)} drivers (Car->Van or Van->Truck)")

    return df


def inject_customer_changes(customers_df: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    """
    Inject customer segment upgrades for SCD Type 2 tracking.

    Changes:
      - 150 Occasional customers → Regular (increased purchase frequency)
      - 50 Regular customers → Premium (became high-value customers)

    No downgrades — companies track segment upgrades, not downgrades in SCD.
    This simulates customers becoming more engaged over the 13-month extension.
    """
    df = customers_df.copy()

    # Occasional → Regular (150 customers)
    occasional_mask = df['customer_segment'] == 'Occasional'
    occasional_idx  = df[occasional_mask].index.tolist()
    upgrade1_idx    = rng.choice(occasional_idx, size=min(150, len(occasional_idx)), replace=False)
    df.loc[upgrade1_idx, 'customer_segment']       = 'Regular'
    df.loc[upgrade1_idx, 'order_frequency_score']  = rng.uniform(0.30, 0.69, size=len(upgrade1_idx)).round(2)

    # Regular → Premium (50 customers)
    regular_mask = df['customer_segment'] == 'Regular'
    regular_idx  = df[regular_mask].index.tolist()
    upgrade2_idx = rng.choice(regular_idx, size=min(50, len(regular_idx)), replace=False)
    df.loc[upgrade2_idx, 'customer_segment']      = 'Premium'
    df.loc[upgrade2_idx, 'order_frequency_score'] = rng.uniform(0.70, 1.00, size=len(upgrade2_idx)).round(2)

    print(f"  snap_customer: {len(upgrade1_idx) + len(upgrade2_idx)} customers upgraded")
    print(f"    Occasional -> Regular : {len(upgrade1_idx)} customers")
    print(f"    Regular -> Premium    : {len(upgrade2_idx)} customers")

    return df


# ─────────────────────────────────────────────────────────────
#  DIM_DATE EXTENSION
# ─────────────────────────────────────────────────────────────

def generate_extended_dim_date() -> pd.DataFrame:
    """Generate dim_date rows for extension period only (Feb 2, 2025 → Feb 28, 2026)."""
    from config.constants import US_HOLIDAYS

    season_map = {
        12: 'Winter', 1: 'Winter', 2: 'Winter',
        3: 'Spring',  4: 'Spring', 5: 'Spring',
        6: 'Summer',  7: 'Summer', 8: 'Summer',
        9: 'Fall',   10: 'Fall',  11: 'Fall'
    }
    rows = []
    current = EXTENSION_START_DATE
    while current <= EXTENSION_END_DATE:
        rows.append({
            'date'           : current,
            'day_of_week'    : current.strftime('%A'),
            'day_of_week_num': current.isoweekday(),
            'week_number'    : current.isocalendar()[1],
            'month'          : current.month,
            'month_name'     : current.strftime('%B'),
            'quarter'        : (current.month - 1) // 3 + 1,
            'year'           : current.year,
            'is_holiday'     : (current.month, current.day) in US_HOLIDAYS,
            'is_weekend'     : current.isoweekday() >= 6,
            'season'         : season_map[current.month],
        })
        current += timedelta(days=1)
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────

def run_extension_backfill():
    print("=" * 60)
    print("FULFILLMENT PLATFORM — EXTENSION BACKFILL")
    print(f"Period: {EXTENSION_START_DATE} → {EXTENSION_END_DATE}")
    print(f"Original backfill ended: {date(2025, 2, 1)}")
    print("=" * 60)

    start_time = time.time()

    # RNG for fact data (seed+1 so sequences differ from original)
    rng = np.random.default_rng(RANDOM_SEED + 1)
    # Separate RNG for SCD changes (seed+2 for full independence)
    rng_scd = np.random.default_rng(RANDOM_SEED + 2)

    # ── Step 1: Generate extended dim_date ───────────────────
    print("\n[1/4] Generating extended dim_date...")
    dates_df = generate_extended_dim_date()
    save_csv(dates_df, "dim_date")
    print(f"  {len(dates_df)} new date rows ({EXTENSION_START_DATE} → {EXTENSION_END_DATE})")

    # ── Step 2: Load + update dimensions with SCD changes ────
    print("\n[2/4] Loading dimensions and injecting SCD Type 2 changes...")
    rng_dims = np.random.default_rng(RANDOM_SEED)  # same seed as original — identical dims
    products_df   = generate_dim_product(rng_dims)
    warehouses_df = generate_dim_warehouse()
    suppliers_df  = generate_dim_supplier()
    drivers_df    = generate_dim_driver(rng_dims)
    customers_df  = generate_dim_customer(rng_dims)
    experiments_df = generate_dim_experiments()

    # Inject changes — these updated versions go to S3/Snowflake
    # dbt snapshot will compare against previous snapshot and record the diffs
    print("\n  Injecting dimension changes for SCD Type 2 tracking:")
    products_updated  = inject_product_changes(products_df, rng_scd)
    suppliers_updated = inject_supplier_changes(suppliers_df, rng_scd)
    drivers_updated   = inject_driver_changes(drivers_df, rng_scd)
    customers_updated = inject_customer_changes(customers_df, rng_scd)

    # Save updated dimensions — dbt snapshot reads from RAW and detects changes
    save_csv(products_updated,  "dim_product")
    save_csv(suppliers_updated, "dim_supplier")
    save_csv(drivers_updated,   "dim_driver")
    save_csv(customers_updated, "dim_customer")
    print("\n  Updated dimensions saved (dbt snapshot will track SCD Type 2 changes)")
    print("  NOTE: dim_warehouse and dim_experiments are not snapshotted — not saved")

    # ── Step 3: Initialize state for extension ───────────────
    print("\n[3/4] Initializing simulation state for extension period...")
    state = SimulationState()
    state.inventory_state = initialize_inventory(products_updated, rng)
    # Offset counters to avoid ID collisions with original backfill
    state.shipment_counter   = 200_000
    state.delivery_counter   = 5_500_000
    state.assignment_counter = 1_200_000
    state.day_counter        = ORIGINAL_BACKFILL_DAYS
    print(f"  ID offsets: shipments={state.shipment_counter:,}, "
          f"deliveries={state.delivery_counter:,}, "
          f"assignments={state.assignment_counter:,}")

    # ── Step 4: Generate daily fact data ─────────────────────
    total_days = (EXTENSION_END_DATE - EXTENSION_START_DATE).days + 1
    print(f"\n[4/4] Generating {total_days} days of fact data...")

    total_orders = total_items = total_shipments = 0
    total_deliveries = total_driver_rows = total_assignments = total_inventory = 0

    current_date = EXTENSION_START_DATE
    day_num = 0

    while current_date <= EXTENSION_END_DATE:
        day_num += 1
        state.day_counter = ORIGINAL_BACKFILL_DAYS + day_num

        if day_num % 30 == 0 or day_num == 1:
            elapsed = time.time() - start_time
            pct = (day_num / total_days) * 100
            print(f"  Day {day_num}/{total_days} ({pct:.0f}%) — "
                  f"{current_date} [{elapsed:.0f}s elapsed]")

        orders_df, items_df = generate_daily_orders(
            current_date, customers_updated, products_updated,
            experiments_df, rng, state.day_counter
        )

        shipments_df, arriving_df, state.pending_shipments, state.shipment_counter = \
            generate_daily_shipments(
                current_date, products_updated, suppliers_updated,
                state.inventory_state, state.pending_shipments,
                rng, state.shipment_counter
            )

        inventory_df, state.inventory_state = generate_daily_inventory_snapshot(
            current_date, products_updated, orders_df, items_df,
            arriving_df, state.inventory_state, rng
        )

        deliveries_df, state.delivery_counter = generate_daily_deliveries(
            current_date, orders_df, customers_updated, drivers_updated,
            rng, state.delivery_counter
        )

        driver_activity_df = generate_daily_driver_activity(
            current_date, drivers_updated, deliveries_df, rng
        )

        assignments_df, state.assignment_counter = generate_daily_experiment_assignments(
            current_date, orders_df, rng, state.assignment_counter
        )

        save_csv(orders_df,          "fact_orders",                 current_date)
        save_csv(items_df,           "fact_order_items",            current_date)
        save_csv(inventory_df,       "fact_inventory_snapshot",     current_date)
        save_csv(deliveries_df,      "fact_deliveries",             current_date)
        save_csv(driver_activity_df, "fact_driver_activity",        current_date)
        if len(shipments_df) > 0:
            save_csv(shipments_df,   "fact_shipments",              current_date)
        if len(assignments_df) > 0:
            save_csv(assignments_df, "fact_experiment_assignments", current_date)

        total_orders      += len(orders_df)
        total_items       += len(items_df)
        total_shipments   += len(shipments_df)
        total_deliveries  += len(deliveries_df)
        total_driver_rows += len(driver_activity_df)
        total_assignments += len(assignments_df)
        total_inventory   += len(inventory_df)

        current_date += timedelta(days=1)

    # ── Summary ──────────────────────────────────────────────
    elapsed = time.time() - start_time
    total_rows = (total_orders + total_items + total_inventory + total_shipments +
                  total_deliveries + total_driver_rows + total_assignments)

    print("\n" + "=" * 60)
    print("EXTENSION BACKFILL COMPLETE")
    print("=" * 60)
    print(f"  Duration                    : {elapsed:.0f}s ({elapsed/60:.1f} min)")
    print(f"  Days generated              : {day_num}")
    print(f"  fact_orders                 : {total_orders:,}")
    print(f"  fact_order_items            : {total_items:,}")
    print(f"  fact_inventory_snapshot     : {total_inventory:,}")
    print(f"  fact_shipments              : {total_shipments:,}")
    print(f"  fact_deliveries             : {total_deliveries:,}")
    print(f"  fact_driver_activity        : {total_driver_rows:,}")
    print(f"  fact_experiment_assignments : {total_assignments:,}")
    print(f"  TOTAL ROWS                  : {total_rows:,}")
    print(f"\n  SCD Type 2 changes injected:")
    print(f"    dim_product  : ~50 products (prices, safety stock, reorder points)")
    print(f"    dim_supplier : 6 suppliers (reliability scores, lead times)")
    print(f"    dim_driver   : ~30 drivers (status cycles, vehicle upgrades)")
    print(f"    dim_customer : ~200 customers (segment upgrades)")
    print(f"\n  Output: {os.path.abspath(OUTPUT_DIR)}/")
    print("\n  Next steps:")
    print("  1. Upload output_extension/raw/ to s3://last-mile-fulfillment-platform/raw/")
    print("  2. Run copy_into_extension.sql in Snowflake worksheet")
    print("     (loads dim_date extension + all new fact data)")
    print("  3. COPY INTO updated dimensions (overwrite existing):")
    print("     COPY INTO RAW.DIM_PRODUCT FROM @stage/raw/dim_product/ FORCE=TRUE;")
    print("     COPY INTO RAW.DIM_SUPPLIER FROM @stage/raw/dim_supplier/ FORCE=TRUE;")
    print("     COPY INTO RAW.DIM_DRIVER FROM @stage/raw/dim_driver/ FORCE=TRUE;")
    print("     COPY INTO RAW.DIM_CUSTOMER FROM @stage/raw/dim_customer/ FORCE=TRUE;")
    print("  4. cd dbt && dbt snapshot   <- detects SCD Type 2 changes")
    print("  5. dbt run                  <- incremental mart refresh")
    print("  6. dbt test")
    print("  7. python -m ml.training.predict_and_writeback --phase demand stockout")
    print("  8. python -m ml.training.predict_and_writeback --phase eta")
    print("  9. python -m ml.training.predict_and_writeback --phase future_demand")
    print("  10. python -m optimization.run_optimization")
    print("  11. python -m experimentation.run_experimentation")


if __name__ == "__main__":
    run_extension_backfill()