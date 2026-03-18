# data_simulation/core/driver_activity.py
"""
Driver activity generation for a single day.
Generates: fact_driver_activity (one row per active driver per day)

v2: Added warehouse-specific utilization floors via WAREHOUSE_UTILIZATION_TARGETS.
    Pure delivery-derived utilization is too flat — low-volume warehouses
    (Denver) get few deliveries so drivers show very low utilization, but
    high-volume warehouses (NYC) have so many deliveries that utilization
    is naturally high. Adding a warehouse floor ensures the pattern is
    realistic even on low-volume days.
"""

from datetime import date, datetime

import numpy as np
import pandas as pd

from config.constants import BATCH_ID_PREFIX, WAREHOUSE_UTILIZATION_TARGETS


def generate_daily_driver_activity(
    current_date: date,
    drivers_df: pd.DataFrame,
    deliveries_df: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """
    Generate fact_driver_activity for a single day.
    Aggregates delivery data per driver, fills in utilization metrics.

    Utilization is the higher of:
    - Delivery-derived utilization (actual work done)
    - Warehouse utilization floor (minimum expected for that warehouse)
    This prevents Denver drivers from showing 10% utilization on a slow
    day when the warehouse target is 68%.

    Returns: driver_activity_df
    """
    batch_id = f"{BATCH_ID_PREFIX}_{current_date.strftime('%Y%m%d')}"
    now = datetime.combine(current_date, datetime.min.time())

    # Only active drivers work
    active_drivers = drivers_df[drivers_df["availability_status"] == "Active"]

    # Aggregate deliveries per driver
    driver_delivery_stats = {}
    if len(deliveries_df) > 0:
        completed = deliveries_df[deliveries_df["delivery_status"].isin(["Delivered", "Failed"])]
        if len(completed) > 0:
            stats = (
                completed.groupby("driver_id")
                .agg(
                    deliveries_completed=("delivery_id", "count"),
                    total_distance_km=("distance_km", "sum"),
                )
                .to_dict("index")
            )
            driver_delivery_stats = stats

    rows = []

    for _, driver in active_drivers.iterrows():
        driver_id = driver["driver_id"]
        wh_id = driver["warehouse_id"]

        stats = driver_delivery_stats.get(driver_id, None)

        if stats:
            deliveries_completed = stats["deliveries_completed"]
            total_distance = round(stats["total_distance_km"], 2)
        else:
            deliveries_completed = 0
            total_distance = 0.0

        # Calculate time spent from deliveries
        avg_speed = driver["avg_speed_kmh"]
        if avg_speed > 0 and total_distance > 0:
            driving_hours = total_distance / avg_speed
            handling_hours = deliveries_completed * 0.25  # ~15 min per delivery
            total_active_hours = round(min(driving_hours + handling_hours, 10.0), 2)
        else:
            total_active_hours = round(rng.uniform(0.5, 2.0), 2)

        shift_hours = 8.0
        idle_hours = round(max(0, shift_hours - total_active_hours), 2)

        # Delivery-derived utilization
        delivery_utilization = (total_active_hours / shift_hours) * 100

        # ── Warehouse utilization floor ──────────────────────────
        # Each warehouse has a target utilization based on demand volume.
        # NYC drivers (target 94%) are always near capacity.
        # Denver drivers (target 68%) have more idle time.
        # We take the max of delivery-derived and a noisy floor to ensure
        # realistic warehouse-level variation even on low-volume days.
        utilization_target = WAREHOUSE_UTILIZATION_TARGETS.get(wh_id, 0.80)
        # Add daily noise ±5% around the warehouse target
        noisy_floor = (utilization_target + rng.uniform(-0.05, 0.05)) * 100
        noisy_floor = max(10.0, min(99.0, noisy_floor))

        # Final utilization is higher of actual work and warehouse floor
        utilization_pct = round(max(delivery_utilization, noisy_floor * 0.7), 2)
        utilization_pct = min(100.0, utilization_pct)

        # Recalculate active hours from final utilization for consistency
        total_active_hours = round((utilization_pct / 100) * shift_hours, 2)
        idle_hours = round(max(0, shift_hours - total_active_hours), 2)

        rows.append(
            {
                "driver_id": driver_id,
                "activity_date": current_date,
                "warehouse_id": wh_id,
                "deliveries_completed": deliveries_completed,
                "total_distance_km": total_distance,
                "total_active_hours": total_active_hours,
                "idle_hours": idle_hours,
                "utilization_pct": utilization_pct,
                "created_at": now,
                "updated_at": now,
                "batch_id": batch_id,
            }
        )

    return pd.DataFrame(rows)
