"""
Backfill script: generates 3 years of historical data (Feb 2022 → Feb 2025).
Runs locally. Saves partitioned CSV files to output/ directory.
Upload to S3 separately after generation.

Usage:
    cd Last-Mile-Fulfilment-Optimization
    python -m data_simulation.backfill

Output structure:
    output/raw/dim_product/data.csv
    output/raw/dim_warehouse/data.csv
    output/raw/fact_orders/date=2022-02-01/data.csv
    output/raw/fact_orders/date=2022-02-02/data.csv
    ...
"""

import os
import sys
import time
import numpy as np
import pandas as pd
from datetime import date, timedelta

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.constants import (
    RANDOM_SEED, BACKFILL_START_DATE, BACKFILL_END_DATE,
)
from data_simulation.core.dimensions import (
    generate_dim_product, generate_dim_warehouse, generate_dim_supplier,
    generate_dim_driver, generate_dim_customer, generate_dim_date,
    generate_dim_experiments,
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


OUTPUT_DIR = "output"


def save_csv(df: pd.DataFrame, table_name: str, partition_date: date = None):
    """Save DataFrame as CSV with optional date partitioning."""
    if partition_date:
        path = os.path.join(OUTPUT_DIR, "raw", table_name, f"date={partition_date.isoformat()}")
    else:
        path = os.path.join(OUTPUT_DIR, "raw", table_name)
    
    os.makedirs(path, exist_ok=True)
    filepath = os.path.join(path, "data.csv")
    df.to_csv(filepath, index=False)


def run_backfill():
    """Main backfill function. Generates all data day-by-day."""
    print("=" * 60)
    print("FULFILLMENT PLATFORM — BACKFILL")
    print(f"Period: {BACKFILL_START_DATE} → {BACKFILL_END_DATE}")
    print("=" * 60)
    
    start_time = time.time()
    
    # Initialize RNG with seed for reproducibility
    rng = np.random.default_rng(RANDOM_SEED)
    
    # ── Step 1: Generate Dimension Tables ──
    print("\n[1/3] Generating dimension tables...")
    
    products_df = generate_dim_product(rng)
    warehouses_df = generate_dim_warehouse()
    suppliers_df = generate_dim_supplier()
    drivers_df = generate_dim_driver(rng)
    customers_df = generate_dim_customer(rng)
    dates_df = generate_dim_date()
    experiments_df = generate_dim_experiments()
    
    # Save dimensions (not date-partitioned)
    save_csv(products_df, "dim_product")
    save_csv(warehouses_df, "dim_warehouse")
    save_csv(suppliers_df, "dim_supplier")
    save_csv(drivers_df, "dim_driver")
    save_csv(customers_df, "dim_customer")
    save_csv(dates_df, "dim_date")
    save_csv(experiments_df, "dim_experiments")
    
    print(f"  dim_product:    {len(products_df)} rows")
    print(f"  dim_warehouse:  {len(warehouses_df)} rows")
    print(f"  dim_supplier:   {len(suppliers_df)} rows")
    print(f"  dim_driver:     {len(drivers_df)} rows")
    print(f"  dim_customer:   {len(customers_df)} rows")
    print(f"  dim_date:       {len(dates_df)} rows")
    print(f"  dim_experiments:{len(experiments_df)} rows")
    
    # ── Step 2: Initialize State ──
    print("\n[2/3] Initializing simulation state...")
    state = SimulationState()
    state.inventory_state = initialize_inventory(products_df, rng)
    
    # ── Step 3: Generate Daily Fact Tables ──
    total_days = (BACKFILL_END_DATE - BACKFILL_START_DATE).days + 1
    print(f"\n[3/3] Generating {total_days} days of fact data...")
    
    # Tracking totals
    total_orders = 0
    total_items = 0
    total_shipments = 0
    total_deliveries = 0
    total_driver_rows = 0
    total_assignments = 0
    total_inventory = 0
    
    current_date = BACKFILL_START_DATE
    day_num = 0
    
    while current_date <= BACKFILL_END_DATE:
        day_num += 1
        state.day_counter = day_num
        
        # Progress indicator
        if day_num % 30 == 0 or day_num == 1:
            elapsed = time.time() - start_time
            pct = (day_num / total_days) * 100
            print(f"  Day {day_num}/{total_days} ({pct:.0f}%) - {current_date} [{elapsed:.0f}s elapsed]")
        
        # ── Generate orders ──
        orders_df, items_df = generate_daily_orders(
            current_date, customers_df, products_df, experiments_df, rng, day_num
        )
        
        # ── Generate shipments ──
        shipments_df, arriving_df, state.pending_shipments, state.shipment_counter = \
            generate_daily_shipments(
                current_date, products_df, suppliers_df,
                state.inventory_state, state.pending_shipments,
                rng, state.shipment_counter
            )
        
        # ── Generate inventory snapshot ──
        inventory_df, state.inventory_state = generate_daily_inventory_snapshot(
            current_date, products_df, orders_df, items_df,
            arriving_df, state.inventory_state, rng
        )
        
        # ── Generate deliveries ──
        deliveries_df, state.delivery_counter = generate_daily_deliveries(
            current_date, orders_df, customers_df, drivers_df, rng, state.delivery_counter
        )
        
        # ── Generate driver activity ──
        driver_activity_df = generate_daily_driver_activity(
            current_date, drivers_df, deliveries_df, rng
        )
        
        # ── Generate experiment assignments ──
        assignments_df, state.assignment_counter = generate_daily_experiment_assignments(
            current_date, orders_df, rng, state.assignment_counter
        )
        
        # ── Save daily data ──
        save_csv(orders_df, "fact_orders", current_date)
        save_csv(items_df, "fact_order_items", current_date)
        save_csv(inventory_df, "fact_inventory_snapshot", current_date)
        save_csv(deliveries_df, "fact_deliveries", current_date)
        save_csv(driver_activity_df, "fact_driver_activity", current_date)
        
        if len(shipments_df) > 0:
            save_csv(shipments_df, "fact_shipments", current_date)
        
        if len(assignments_df) > 0:
            save_csv(assignments_df, "fact_experiment_assignments", current_date)
        
        # Track totals
        total_orders += len(orders_df)
        total_items += len(items_df)
        total_shipments += len(shipments_df)
        total_deliveries += len(deliveries_df)
        total_driver_rows += len(driver_activity_df)
        total_assignments += len(assignments_df)
        total_inventory += len(inventory_df)
        
        current_date += timedelta(days=1)
    
    # ── Summary ──
    elapsed = time.time() - start_time
    print("\n" + "=" * 60)
    print("BACKFILL COMPLETE")
    print("=" * 60)
    print(f"  Duration:                    {elapsed:.0f} seconds ({elapsed/60:.1f} min)")
    print(f"  Days generated:              {day_num}")
    print(f"  fact_orders:                 {total_orders:,}")
    print(f"  fact_order_items:            {total_items:,}")
    print(f"  fact_inventory_snapshot:     {total_inventory:,}")
    print(f"  fact_shipments:              {total_shipments:,}")
    print(f"  fact_deliveries:             {total_deliveries:,}")
    print(f"  fact_driver_activity:        {total_driver_rows:,}")
    print(f"  fact_experiment_assignments: {total_assignments:,}")
    print(f"  TOTAL ROWS:                  {total_orders + total_items + total_inventory + total_shipments + total_deliveries + total_driver_rows + total_assignments:,}")
    print(f"\n  Output: {os.path.abspath(OUTPUT_DIR)}/")
    print("  Next step: upload output/raw/ to s3://last-mile-fulfillment-platform/raw/")


if __name__ == "__main__":
    run_backfill()