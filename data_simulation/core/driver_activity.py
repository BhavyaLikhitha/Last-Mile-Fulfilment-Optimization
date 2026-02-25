# data_simulation/core/driver_activity.py
"""
Driver activity generation for a single day.
Generates: fact_driver_activity (one row per active driver per day)
"""

import numpy as np
import pandas as pd
from datetime import date, datetime

from config.constants import BATCH_ID_PREFIX
from config.warehouse_config import WAREHOUSE_IDS


def generate_daily_driver_activity(
    current_date: date,
    drivers_df: pd.DataFrame,
    deliveries_df: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """
    Generate fact_driver_activity for a single day.
    Aggregates delivery data per driver, fills in utilization metrics.
    
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
            stats = completed.groupby("driver_id").agg(
                deliveries_completed=("delivery_id", "count"),
                total_distance_km=("distance_km", "sum"),
            ).to_dict("index")
            driver_delivery_stats = stats
    
    rows = []
    
    for _, driver in active_drivers.iterrows():
        driver_id = driver["driver_id"]
        wh_id = driver["warehouse_id"]
        
        # Get actual stats or generate reasonable defaults
        stats = driver_delivery_stats.get(driver_id, None)
        
        if stats:
            deliveries_completed = stats["deliveries_completed"]
            total_distance = round(stats["total_distance_km"], 2)
        else:
            # Driver was active but got no deliveries assigned
            deliveries_completed = 0
            total_distance = 0.0
        
        # Calculate time spent
        avg_speed = driver["avg_speed_kmh"]
        if avg_speed > 0 and total_distance > 0:
            driving_hours = total_distance / avg_speed
            # Add handling time: ~15 min per delivery
            handling_hours = deliveries_completed * 0.25
            total_active_hours = round(min(driving_hours + handling_hours, 10.0), 2)
        else:
            total_active_hours = round(rng.uniform(0.5, 2.0), 2)  # Minimum shift time
        
        # Standard shift is 8 hours
        shift_hours = 8.0
        idle_hours = round(max(0, shift_hours - total_active_hours), 2)
        
        # Utilization
        utilization_pct = round((total_active_hours / shift_hours) * 100, 2)
        utilization_pct = min(100.0, utilization_pct)
        
        rows.append({
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
        })
    
    return pd.DataFrame(rows)