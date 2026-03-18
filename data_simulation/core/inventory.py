"""
Inventory snapshot generation for a single day.
Generates: fact_inventory_snapshot (500 products x 8 warehouses = 4,000 rows/day)

v2: Added warehouse-specific holding cost multipliers.
    NYC/LA have significantly higher warehouse real estate costs than Denver/Dallas.
    This creates natural holding_cost variation across warehouses in the mart
    without post-processing adjustments.
"""

from datetime import date, datetime
from typing import Dict, Tuple

import numpy as np
import pandas as pd

from config.constants import (
    BATCH_ID_PREFIX,
    HOLDING_COST_RATE,
    INITIAL_STOCK_RANGE,
    REORDER_QUANTITY_RANGE,
    WAREHOUSE_HOLDING_COST_MULTIPLIERS,
)
from config.warehouse_config import WAREHOUSE_IDS


def initialize_inventory(
    products_df: pd.DataFrame,
    rng: np.random.Generator,
) -> Dict[Tuple[str, str], dict]:
    """
    Create initial inventory state for day 1.
    Returns dict keyed by (warehouse_id, product_id) with stock info.
    """
    inventory_state = {}

    for wh_id in WAREHOUSE_IDS:
        for _, product in products_df.iterrows():
            pid = product["product_id"]
            initial_stock = int(rng.integers(*INITIAL_STOCK_RANGE))

            inventory_state[(wh_id, pid)] = {
                "closing_stock": initial_stock,
                "units_on_order": 0,
                "avg_daily_demand": 0.0,
            }

    return inventory_state


def generate_daily_inventory_snapshot(
    current_date: date,
    products_df: pd.DataFrame,
    orders_df: pd.DataFrame,
    order_items_df: pd.DataFrame,
    shipments_arriving: pd.DataFrame,
    inventory_state: Dict[Tuple[str, str], dict],
    rng: np.random.Generator,
) -> Tuple[pd.DataFrame, Dict[Tuple[str, str], dict]]:
    """
    Generate fact_inventory_snapshot for a single day.
    Updates inventory_state in place and returns the snapshot DataFrame.

    Returns: (snapshot_df, updated_inventory_state)
    """
    batch_id = f"{BATCH_ID_PREFIX}_{current_date.strftime('%Y%m%d')}"
    now = datetime.combine(current_date, datetime.min.time())

    # Calculate units sold per warehouse x product
    units_sold_map = {}
    if len(orders_df) > 0 and len(order_items_df) > 0:
        merged = order_items_df.merge(orders_df[["order_id", "assigned_warehouse_id"]], on="order_id", how="left")
        delivered_orders = orders_df[orders_df["order_status"].isin(["Delivered", "Shipped", "Processing"])]["order_id"]
        merged = merged[merged["order_id"].isin(delivered_orders)]

        if len(merged) > 0:
            sold_agg = merged.groupby(["assigned_warehouse_id", "product_id"])["quantity"].sum().to_dict()
            units_sold_map = {(wh, pid): qty for (wh, pid), qty in sold_agg.items()}

    # Calculate units received from shipments arriving today
    units_received_map = {}
    if len(shipments_arriving) > 0:
        received_agg = shipments_arriving.groupby(["warehouse_id", "product_id"])["quantity"].sum().to_dict()
        units_received_map = {(wh, pid): qty for (wh, pid), qty in received_agg.items()}

    # Calculate returns
    units_returned_map = {}
    if len(orders_df) > 0 and len(order_items_df) > 0:
        returned_orders = orders_df[orders_df["return_flag"]]["order_id"]
        if len(returned_orders) > 0:
            returned_items = order_items_df[order_items_df["order_id"].isin(returned_orders)]
            if len(returned_items) > 0:
                returned_merged = returned_items.merge(
                    orders_df[["order_id", "assigned_warehouse_id"]], on="order_id", how="left"
                )
                ret_agg = returned_merged.groupby(["assigned_warehouse_id", "product_id"])["quantity"].sum().to_dict()
                units_returned_map = {(wh, pid): qty for (wh, pid), qty in ret_agg.items()}

    rows = []

    for wh_id in WAREHOUSE_IDS:
        # ── Warehouse-specific holding cost multiplier ───────────
        # NYC/LA have 35-45% higher warehouse costs per sq ft than
        # Denver/Dallas due to real estate prices. This creates natural
        # holding_cost variance across warehouses in mart_daily_product_kpis
        # and mart_daily_warehouse_kpis without post-processing.
        wh_cost_multiplier = WAREHOUSE_HOLDING_COST_MULTIPLIERS.get(wh_id, 1.0)

        for _, product in products_df.iterrows():
            pid = product["product_id"]
            key = (wh_id, pid)

            prev = inventory_state.get(key, {"closing_stock": 100, "units_on_order": 0, "avg_daily_demand": 0})
            opening_stock = prev["closing_stock"]
            units_sold = units_sold_map.get(key, 0)
            units_received = units_received_map.get(key, 0)
            units_returned = units_returned_map.get(key, 0)

            closing_stock = max(0, opening_stock - units_sold + units_received + units_returned)

            stockout_flag = closing_stock == 0
            below_safety = closing_stock < product["safety_stock"]
            reorder_triggered = closing_stock <= product["reorder_point"] and prev["units_on_order"] == 0

            units_on_order = prev["units_on_order"]
            if units_received > 0:
                units_on_order = max(0, units_on_order - units_received)
            if reorder_triggered:
                reorder_qty = int(rng.integers(*REORDER_QUANTITY_RANGE))
                units_on_order += reorder_qty

            alpha = 0.1
            avg_demand = prev["avg_daily_demand"] * (1 - alpha) + units_sold * alpha

            days_of_supply = round(closing_stock / avg_demand, 2) if avg_demand > 0 else 99.99
            days_of_supply = min(99.99, days_of_supply)

            cost_price = product["cost_price"]
            # Apply warehouse-specific holding cost multiplier
            holding_cost = round(closing_stock * cost_price * HOLDING_COST_RATE * wh_cost_multiplier, 2)
            inventory_value = round(closing_stock * cost_price, 2)

            rows.append(
                {
                    "snapshot_date": current_date,
                    "warehouse_id": wh_id,
                    "product_id": pid,
                    "opening_stock": opening_stock,
                    "units_sold": units_sold,
                    "units_received": units_received,
                    "units_returned": units_returned,
                    "closing_stock": closing_stock,
                    "stockout_flag": stockout_flag,
                    "below_safety_stock_flag": below_safety,
                    "reorder_triggered_flag": reorder_triggered,
                    "units_on_order": units_on_order,
                    "days_of_supply": days_of_supply,
                    "holding_cost": holding_cost,
                    "inventory_value": inventory_value,
                    "created_at": now,
                    "updated_at": now,
                    "batch_id": batch_id,
                }
            )

            inventory_state[key] = {
                "closing_stock": closing_stock,
                "units_on_order": units_on_order,
                "avg_daily_demand": avg_demand,
            }

    return pd.DataFrame(rows), inventory_state
