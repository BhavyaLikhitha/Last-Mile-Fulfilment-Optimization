# data_simulation/lambda_handler.py
"""
Lambda Handler — Incremental Data Generation
Generates N days of fulfillment data starting from the day after
the last date already in S3.

Triggered by:
  - EventBridge daily rule  : generates 1 day
  - EventBridge weekly rule : generates 7 days
  - Manual invocation       : generates N days based on event payload

Event payload examples:
  {}                          -> auto-detect mode (1 day default)
  {"mode": "daily"}           -> generate 1 day
  {"mode": "weekly"}          -> generate 7 days
  {"mode": "manual", "days": 3} -> generate 3 specific days
  {"mode": "backfill", "start_date": "2026-03-01", "end_date": "2026-03-07"}

State management:
  - Reads last_state.json from S3 for inventory continuity
  - Saves updated state back to S3 after each run
  - If no state exists, initializes fresh (same as extension backfill)

SCD Type 2:
  - ~5% probability per run: inject small dimension changes
  - Uploads updated dimensions to S3 so dbt snapshot detects changes
"""

import json
import os
import sys
import time
from datetime import date, timedelta

import boto3
import numpy as np
import pandas as pd

# Add /tmp to path for Lambda layer imports
sys.path.insert(0, "/tmp")

# ── Constants ─────────────────────────────────────────────────
RANDOM_SEED = 42
S3_BUCKET = os.environ.get("S3_BUCKET_NAME", "last-mile-fulfillment-platform")
S3_STATE_KEY = "state/latest_state.json"
S3_RAW_PREFIX = "raw"

# ID offsets — must be higher than any ID generated in backfill + extension
# Original backfill: ~5.6M orders, ~5.3M deliveries, ~1.1M assignments
# Extension backfill: +~2M orders, +~2M deliveries, +~600K assignments
# Lambda starts from these safe offsets
LAMBDA_DELIVERY_OFFSET = 10_000_000
LAMBDA_SHIPMENT_OFFSET = 500_000
LAMBDA_ASSIGNMENT_OFFSET = 3_000_000

ORIGINAL_BACKFILL_DAYS = 1096  # Feb 1 2022 → Feb 1 2025
EXTENSION_BACKFILL_DAYS = 1489  # + 393 extension days

# SCD Type 2 change probability per Lambda run
SCD_CHANGE_PROBABILITY = 0.05  # 5% chance of dimension changes per run


def get_s3_client():
    return boto3.client("s3")


def get_last_date_in_s3() -> date:
    """
    Find the most recent date partition in S3 for fact_orders.
    Returns the last date that has been generated.
    """
    s3 = get_s3_client()
    prefix = f"{S3_RAW_PREFIX}/fact_orders/"

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix, Delimiter="/")

    dates = []
    for page in pages:
        for cp in page.get("CommonPrefixes", []):
            # Extract date from: raw/fact_orders/date=2026-03-01/
            folder = cp["Prefix"].rstrip("/").split("/")[-1]
            if folder.startswith("date="):
                try:
                    d = date.fromisoformat(folder.replace("date=", ""))
                    dates.append(d)
                except ValueError:
                    pass

    if not dates:
        # No data in S3 yet — start from extension end date
        return date(2026, 2, 28)

    return max(dates)


def load_state_from_s3() -> dict:
    """Load simulation state from S3. Returns None if no state exists."""
    s3 = get_s3_client()
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=S3_STATE_KEY)
        return json.loads(response["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        return None
    except Exception as e:
        print(f"Warning: Could not load state from S3: {e}")
        return None


def save_state_to_s3(state_dict: dict):
    """Save simulation state to S3."""
    s3 = get_s3_client()
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=S3_STATE_KEY,
        Body=json.dumps(state_dict, default=str).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"  State saved to s3://{S3_BUCKET}/{S3_STATE_KEY}")


def upload_csv_to_s3(df: pd.DataFrame, table_name: str, partition_date: date):
    """Upload a DataFrame as CSV to S3 with date partitioning."""
    s3 = get_s3_client()
    key = f"{S3_RAW_PREFIX}/{table_name}/date={partition_date.isoformat()}/data.csv"
    csv_buffer = df.to_csv(index=False).encode("utf-8")
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=csv_buffer)


def upload_dimension_to_s3(df: pd.DataFrame, table_name: str):
    """Upload a dimension table CSV to S3 (no date partitioning)."""
    s3 = get_s3_client()
    key = f"{S3_RAW_PREFIX}/{table_name}/data.csv"
    csv_buffer = df.to_csv(index=False).encode("utf-8")
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=csv_buffer)
    print(f"  Uploaded dimension: {table_name}")


def maybe_inject_scd_changes(
    products_df: pd.DataFrame,
    suppliers_df: pd.DataFrame,
    drivers_df: pd.DataFrame,
    customers_df: pd.DataFrame,
    rng: np.random.Generator,
    run_seed: int,
) -> tuple:
    """
    With SCD_CHANGE_PROBABILITY, inject small realistic dimension changes.
    Returns updated DataFrames and a flag indicating if changes were made.

    Changes are small (2-5 records per dimension) to be realistic for
    a weekly operational update — not the bulk changes of the extension backfill.
    """
    rng_scd = np.random.default_rng(run_seed + 999)
    roll = rng_scd.random()

    if roll > SCD_CHANGE_PROBABILITY:
        return products_df, suppliers_df, drivers_df, customers_df, False

    print("  SCD Type 2 changes triggered this run (~5% probability)")
    changed = False

    # Product: 2-3 products get small price adjustments
    n = len(products_df)
    price_idx = rng_scd.choice(n, size=rng_scd.integers(2, 4), replace=False)
    mult = rng_scd.uniform(1.02, 1.08, size=len(price_idx))
    products_df = products_df.copy()
    products_df.loc[price_idx, "cost_price"] = (products_df.loc[price_idx, "cost_price"] * mult).round(2)
    products_df.loc[price_idx, "selling_price"] = (products_df.loc[price_idx, "selling_price"] * mult).round(2)
    print(f"    dim_product: {len(price_idx)} price adjustments")

    # Supplier: 1-2 suppliers get reliability score update
    s_idx = rng_scd.choice(len(suppliers_df), size=rng_scd.integers(1, 3), replace=False)
    delta = rng_scd.uniform(-0.02, 0.02, size=len(s_idx))
    suppliers_df = suppliers_df.copy()
    suppliers_df.loc[s_idx, "reliability_score"] = (
        (suppliers_df.loc[s_idx, "reliability_score"] + delta).clip(0.70, 1.0).round(2)
    )
    print(f"    dim_supplier: {len(s_idx)} reliability updates")

    # Driver: 1-3 drivers change availability status
    active_idx = drivers_df[drivers_df["availability_status"] == "Active"].index.tolist()
    leave_idx = drivers_df[drivers_df["availability_status"] == "On Leave"].index.tolist()
    drivers_df = drivers_df.copy()
    if active_idx:
        go_leave = rng_scd.choice(active_idx, size=min(2, len(active_idx)), replace=False)
        drivers_df.loc[go_leave, "availability_status"] = "On Leave"
    if leave_idx:
        come_back = rng_scd.choice(leave_idx, size=min(2, len(leave_idx)), replace=False)
        drivers_df.loc[come_back, "availability_status"] = "Active"
    print("    dim_driver: status rotations applied")

    # Customer: 5-10 customers upgrade segment
    occ_idx = customers_df[customers_df["customer_segment"] == "Occasional"].index.tolist()
    customers_df = customers_df.copy()
    if occ_idx:
        upgrade_idx = rng_scd.choice(occ_idx, size=min(5, len(occ_idx)), replace=False)
        customers_df.loc[upgrade_idx, "customer_segment"] = "Regular"
        customers_df.loc[upgrade_idx, "order_frequency_score"] = rng_scd.uniform(
            0.30, 0.69, size=len(upgrade_idx)
        ).round(2)
        print(f"    dim_customer: {len(upgrade_idx)} segment upgrades")

    changed = True
    return products_df, suppliers_df, drivers_df, customers_df, changed


def generate_days(start_date: date, end_date: date, state_dict: dict = None) -> dict:
    """
    Generate data for date range [start_date, end_date].
    Returns summary stats.
    """
    from config.constants import RANDOM_SEED
    from data_simulation.core.deliveries import generate_daily_deliveries
    from data_simulation.core.dimensions import (
        generate_dim_customer,
        generate_dim_driver,
        generate_dim_experiments,
        generate_dim_product,
        generate_dim_supplier,
        generate_dim_warehouse,
    )
    from data_simulation.core.driver_activity import generate_daily_driver_activity
    from data_simulation.core.experiments import generate_daily_experiment_assignments
    from data_simulation.core.inventory import generate_daily_inventory_snapshot, initialize_inventory
    from data_simulation.core.orders import generate_daily_orders
    from data_simulation.core.shipments import generate_daily_shipments
    from data_simulation.state.state_manager import SimulationState

    total_days = (end_date - start_date).days + 1
    run_seed = int(start_date.strftime("%Y%m%d"))

    rng = np.random.default_rng(run_seed)
    rng_dims = np.random.default_rng(RANDOM_SEED)

    print("  Loading dimension tables...")
    products_df = generate_dim_product(rng_dims)
    generate_dim_warehouse()
    suppliers_df = generate_dim_supplier()
    drivers_df = generate_dim_driver(rng_dims)
    customers_df = generate_dim_customer(rng_dims)
    experiments_df = generate_dim_experiments()

    products_df, suppliers_df, drivers_df, customers_df, scd_changed = maybe_inject_scd_changes(
        products_df, suppliers_df, drivers_df, customers_df, rng, run_seed
    )

    if scd_changed:
        upload_dimension_to_s3(products_df, "dim_product")
        upload_dimension_to_s3(suppliers_df, "dim_supplier")
        upload_dimension_to_s3(drivers_df, "dim_driver")
        upload_dimension_to_s3(customers_df, "dim_customer")

    state = SimulationState()
    if state_dict:
        print("  Restoring state from S3...")
        state = SimulationState.from_dict(state_dict)
    else:
        print("  Initializing fresh state...")
        state.inventory_state = initialize_inventory(products_df, rng)
        state.shipment_counter = LAMBDA_SHIPMENT_OFFSET
        state.delivery_counter = LAMBDA_DELIVERY_OFFSET
        state.assignment_counter = LAMBDA_ASSIGNMENT_OFFSET
        state.day_counter = EXTENSION_BACKFILL_DAYS

    totals = {k: 0 for k in ["orders", "items", "inventory", "shipments", "deliveries", "drivers", "assignments"]}

    current_date = start_date
    day_num = 0

    while current_date <= end_date:
        day_num += 1
        state.day_counter += 1
        print(f"  Generating {current_date} (day {day_num}/{total_days})...")

        orders_df, items_df = generate_daily_orders(
            current_date, customers_df, products_df, experiments_df, rng, state.day_counter
        )
        shipments_df, arriving_df, state.pending_shipments, state.shipment_counter = generate_daily_shipments(
            current_date,
            products_df,
            suppliers_df,
            state.inventory_state,
            state.pending_shipments,
            rng,
            state.shipment_counter,
        )
        inventory_df, state.inventory_state = generate_daily_inventory_snapshot(
            current_date, products_df, orders_df, items_df, arriving_df, state.inventory_state, rng
        )
        deliveries_df, state.delivery_counter = generate_daily_deliveries(
            current_date, orders_df, customers_df, drivers_df, rng, state.delivery_counter
        )
        driver_activity_df = generate_daily_driver_activity(current_date, drivers_df, deliveries_df, rng)
        assignments_df, state.assignment_counter = generate_daily_experiment_assignments(
            current_date, orders_df, rng, state.assignment_counter
        )

        upload_csv_to_s3(orders_df, "fact_orders", current_date)
        upload_csv_to_s3(items_df, "fact_order_items", current_date)
        upload_csv_to_s3(inventory_df, "fact_inventory_snapshot", current_date)
        upload_csv_to_s3(deliveries_df, "fact_deliveries", current_date)
        upload_csv_to_s3(driver_activity_df, "fact_driver_activity", current_date)
        if len(shipments_df) > 0:
            upload_csv_to_s3(shipments_df, "fact_shipments", current_date)
        if len(assignments_df) > 0:
            upload_csv_to_s3(assignments_df, "fact_experiment_assignments", current_date)

        totals["orders"] += len(orders_df)
        totals["items"] += len(items_df)
        totals["inventory"] += len(inventory_df)
        totals["shipments"] += len(shipments_df)
        totals["deliveries"] += len(deliveries_df)
        totals["drivers"] += len(driver_activity_df)
        totals["assignments"] += len(assignments_df)

        current_date += timedelta(days=1)

    save_state_to_s3(state.to_dict())

    return totals


def lambda_handler(event: dict, context) -> dict:
    """
    Main Lambda entry point.

    Event examples:
      {}                                                    -> daily (1 day)
      {"mode": "daily"}                                     -> 1 day
      {"mode": "weekly"}                                    -> 7 days
      {"mode": "manual", "days": 3}                         -> 3 days from last date
      {"mode": "backfill", "start_date": "2026-03-01",
                           "end_date":   "2026-03-07"}      -> specific range
    """
    print("=" * 60)
    print("  FULFILLMENT PLATFORM — LAMBDA DATA GENERATOR")
    print(f"  Event: {json.dumps(event)}")
    print("=" * 60)

    start_time = time.time()

    # Today's date — never generate beyond this
    today = date.today()

    mode = event.get("mode", "daily")
    last_date = get_last_date_in_s3()
    print(f"  Last date in S3 : {last_date}")
    print(f"  Today           : {today}")

    # ── Determine date range ───────────────────────────────────
    if mode == "backfill" and "start_date" in event and "end_date" in event:
        # Backfill mode: explicit range, no today cap (intentional historical load)
        start_date = date.fromisoformat(event["start_date"])
        end_date = date.fromisoformat(event["end_date"])

    elif mode == "weekly":
        start_date = last_date + timedelta(days=1)
        end_date = min(last_date + timedelta(days=7), today)  # never exceed today

    elif mode == "manual" and "days" in event:
        start_date = last_date + timedelta(days=1)
        end_date = min(last_date + timedelta(days=int(event["days"])), today)  # never exceed today

    else:
        # Default: daily — generate exactly 1 day, never future
        start_date = last_date + timedelta(days=1)
        end_date = min(start_date, today)  # never exceed today

    # ── Guard: already up to date ──────────────────────────────
    if start_date > today:
        print(f"\n  Already up to date (last={last_date}, today={today})")
        print("  Nothing to generate. Exiting cleanly.")
        return {
            "statusCode": 200,
            "message": "already_up_to_date",
            "last_date": str(last_date),
            "today": str(today),
        }

    print(f"  Mode       : {mode}")
    print(f"  Generating : {start_date} → {end_date} ({(end_date - start_date).days + 1} days)")

    # Load existing state
    state_dict = load_state_from_s3()

    # Generate data
    totals = generate_days(start_date, end_date, state_dict)

    elapsed = time.time() - start_time
    total_rows = sum(totals.values())

    print("\n" + "=" * 60)
    print("  GENERATION COMPLETE")
    print("=" * 60)
    print(f"  Duration    : {elapsed:.0f}s")
    print(f"  Date range  : {start_date} → {end_date}")
    print(f"  fact_orders : {totals['orders']:,}")
    print(f"  Total rows  : {total_rows:,}")

    return {
        "statusCode": 200,
        "mode": mode,
        "start_date": str(start_date),
        "end_date": str(end_date),
        "total_rows": total_rows,
        "totals": totals,
        "duration_s": round(elapsed, 1),
    }
