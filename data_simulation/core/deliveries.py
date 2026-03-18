# data_simulation/core/deliveries.py
"""
Delivery generation for a single day.
Generates: fact_deliveries (one per order)

v2: Added warehouse-specific operational realism:
    - WAREHOUSE_CONGESTION_FACTORS: controls ETA variability per warehouse
      NYC has high unpredictability (std=0.35); Denver very consistent (std=0.10)
    - WAREHOUSE_SLA_FAILURE_BIAS: additional SLA breach probability beyond distance
      NYC has 20% extra failure risk from access restrictions and parking
    - WAREHOUSE_DISTANCE_FACTORS: already applied via constants (unchanged)
    These changes mean future Lambda data will naturally produce the same
    warehouse-level performance gaps as the post-processing DAG updates.
"""

from datetime import date, datetime, timedelta
from typing import Tuple

import numpy as np
import pandas as pd

from config.constants import (
    BATCH_ID_PREFIX,
    SLA_MINUTES,
    WAREHOUSE_CONGESTION_FACTORS,
    WAREHOUSE_DISTANCE_FACTORS,
    WAREHOUSE_SLA_FAILURE_BIAS,
)
from config.warehouse_config import WAREHOUSE_IDS
from data_simulation.utils.cost import calculate_delivery_cost
from data_simulation.utils.geo import get_delivery_distance


def generate_daily_deliveries(
    current_date: date,
    orders_df: pd.DataFrame,
    customers_df: pd.DataFrame,
    drivers_df: pd.DataFrame,
    rng: np.random.Generator,
    delivery_counter: int,
) -> Tuple[pd.DataFrame, int]:
    """
    Generate fact_deliveries for a single day.
    One delivery per order (excluding cancelled orders).

    Returns: (deliveries_df, updated_counter)
    """
    batch_id = f"{BATCH_ID_PREFIX}_{current_date.strftime('%Y%m%d')}"

    # Filter out cancelled orders
    active_orders = orders_df[orders_df["order_status"] != "Cancelled"]

    if len(active_orders) == 0:
        return pd.DataFrame(), delivery_counter

    # Build customer lookup
    customer_lookup = customers_df.set_index("customer_id")[["latitude", "longitude"]].to_dict("index")

    # Build active drivers per warehouse
    active_drivers = drivers_df[drivers_df["availability_status"] == "Active"]
    drivers_by_wh = {}
    for wh_id in WAREHOUSE_IDS:
        wh_drivers = active_drivers[active_drivers["warehouse_id"] == wh_id]
        if len(wh_drivers) > 0:
            drivers_by_wh[wh_id] = wh_drivers["driver_id"].values
        else:
            drivers_by_wh[wh_id] = np.array(["DRV-0001"])  # fallback

    rows = []

    for _, order in active_orders.iterrows():
        delivery_id = f"DEL-{current_date.strftime('%Y%m%d')}-{delivery_counter:05d}"

        wh_id = order["assigned_warehouse_id"]
        customer_id = order["customer_id"]

        # Get customer location
        cust_info = customer_lookup.get(customer_id, {"latitude": 40.0, "longitude": -74.0})
        cust_lat = cust_info["latitude"]
        cust_lon = cust_info["longitude"]

        # Distance with warehouse-specific road factor
        distance_km = get_delivery_distance(wh_id, cust_lat, cust_lon)
        distance_factor = WAREHOUSE_DISTANCE_FACTORS.get(wh_id, 1.0)

        # Assign driver (round-robin within warehouse)
        available = drivers_by_wh.get(wh_id, np.array(["DRV-0001"]))
        driver_id = rng.choice(available)

        # Get driver speed
        driver_info = drivers_df[drivers_df["driver_id"] == driver_id]
        if len(driver_info) > 0:
            avg_speed = driver_info.iloc[0]["avg_speed_kmh"]
        else:
            avg_speed = 35.0

        # Estimated ETA with warehouse distance factor
        handling_minutes = rng.uniform(5, 20)
        estimated_eta = round((distance_km / avg_speed) * 60 * distance_factor + handling_minutes, 2)

        # ── Warehouse-specific ETA variability ──────────────────
        # NYC (std=0.35) is highly unpredictable due to traffic/parking.
        # Denver (std=0.10) is very consistent in suburban environment.
        # This creates realistic variance in actual_delivery_minutes
        # that directly drives on_time_flag and sla_breach_flag differences
        # across warehouses — no post-processing needed for future data.
        congestion_std = WAREHOUSE_CONGESTION_FACTORS.get(wh_id, 0.20)
        variability = rng.normal(1.0, congestion_std)
        variability = max(0.6, min(2.0, variability))  # clip to reasonable range
        actual_delivery = round(estimated_eta * variability, 2)

        # SLA based on priority
        priority = order["order_priority"]
        sla = SLA_MINUTES[priority]

        # Delivery status
        order_status = order["order_status"]
        if order_status == "Delivered":
            delivery_status = "Delivered"
        elif order_status == "Shipped":
            delivery_status = rng.choice(["In Transit", "Delivered"], p=[0.6, 0.4])
        elif order_status == "Processing":
            delivery_status = "Assigned"
        else:
            delivery_status = "Assigned"

        # Failed deliveries (~4%)
        if delivery_status == "Delivered" and rng.random() < 0.04:
            delivery_status = "Failed"

        # Timestamps
        order_ts = order["order_timestamp"]
        assigned_time = order_ts + timedelta(minutes=int(rng.integers(5, 30)))
        pickup_time = assigned_time + timedelta(minutes=int(rng.integers(10, 45)))

        if delivery_status in ["Delivered", "Failed"]:
            delivered_time = pickup_time + timedelta(minutes=int(actual_delivery))
        else:
            delivered_time = None
            actual_delivery = None

        # ── On-time and SLA breach flags ────────────────────────
        # Two components:
        # 1. Pure distance/time calculation (actual_delivery vs sla)
        # 2. Warehouse-specific SLA failure bias (access restrictions,
        #    parking, local knowledge gaps)
        # NYC has 20% additional failure risk even when delivery time
        # looks fine on paper — reflects real urban delivery challenges.
        on_time_flag = None
        sla_breach_flag = None
        if delivery_status == "Delivered" and actual_delivery is not None:
            time_based_breach = actual_delivery > sla
            # Apply warehouse-specific additional failure probability
            sla_bias = WAREHOUSE_SLA_FAILURE_BIAS.get(wh_id, 0.0)
            bias_breach = rng.random() < sla_bias
            # SLA breach if either time-based or bias-based
            sla_breach_flag = time_based_breach or bias_breach
            on_time_flag = not sla_breach_flag

        # Delivery cost
        delivery_cost = calculate_delivery_cost(distance_km)

        now = datetime.combine(current_date, datetime.min.time())

        rows.append(
            {
                "delivery_id": delivery_id,
                "order_id": order["order_id"],
                "driver_id": driver_id,
                "warehouse_id": wh_id,
                "assigned_time": assigned_time,
                "pickup_time": pickup_time,
                "delivered_time": delivered_time,
                "estimated_eta_minutes": estimated_eta,
                "actual_delivery_minutes": actual_delivery,
                "distance_km": distance_km,
                "delivery_cost": delivery_cost,
                "delivery_status": delivery_status,
                "on_time_flag": on_time_flag,
                "sla_minutes": sla,
                "sla_breach_flag": sla_breach_flag,
                "created_at": now,
                "updated_at": now,
                "batch_id": batch_id,
            }
        )
        delivery_counter += 1

    return pd.DataFrame(rows), delivery_counter
