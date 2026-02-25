# data_simulation/core/orders.py
"""
Order generation for a single day.
Generates: fact_orders + fact_order_items
"""

import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta
from typing import Tuple

from config.constants import (
    DAILY_ORDERS, ORDER_PRIORITY_DISTRIBUTION, SLA_MINUTES,
    ORDER_STATUS_DISTRIBUTION, RETURN_RATE, ALLOCATION_STRATEGIES,
    DISCOUNT_PROBABILITY, DISCOUNT_RANGE, BATCH_ID_PREFIX,
)
from config.warehouse_config import WAREHOUSE_IDS
from data_simulation.utils.seasonality import get_daily_order_count
from data_simulation.utils.geo import find_nearest_warehouse, get_delivery_distance
from data_simulation.utils.cost import calculate_fulfillment_cost, calculate_delivery_cost


def generate_daily_orders(
    current_date: date,
    customers_df: pd.DataFrame,
    products_df: pd.DataFrame,
    experiments_df: pd.DataFrame,
    rng: np.random.Generator,
    day_counter: int,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Generate fact_orders and fact_order_items for a single day.
    
    Returns: (orders_df, order_items_df)
    """
    batch_id = f"{BATCH_ID_PREFIX}_{current_date.strftime('%Y%m%d')}"
    now = datetime.combine(current_date, datetime.min.time())
    
    # Calculate daily order count with seasonality
    num_orders = get_daily_order_count(DAILY_ORDERS, current_date, rng)
    
    # Priority distribution
    priorities = list(ORDER_PRIORITY_DISTRIBUTION.keys())
    priority_probs = list(ORDER_PRIORITY_DISTRIBUTION.values())
    
    # Status distribution
    statuses = list(ORDER_STATUS_DISTRIBUTION.keys())
    status_probs = list(ORDER_STATUS_DISTRIBUTION.values())
    
    # Allocation strategies
    strategies = list(ALLOCATION_STRATEGIES.keys())
    strategy_probs = list(ALLOCATION_STRATEGIES.values())
    
    # Active experiments for this date
    active_experiments = experiments_df[
        (experiments_df["start_date"] <= current_date) &
        ((experiments_df["end_date"].isna()) | (experiments_df["end_date"] >= current_date))
    ]
    
    # Customer arrays for fast sampling
    customer_ids = customers_df["customer_id"].values
    customer_lats = customers_df["latitude"].values
    customer_lons = customers_df["longitude"].values
    
    # Product arrays
    product_ids = products_df["product_id"].values
    product_prices = products_df["selling_price"].values
    product_costs = products_df["cost_price"].values
    
    orders_rows = []
    items_rows = []
    item_counter = 1
    
    for i in range(num_orders):
        order_id = f"ORD-{current_date.strftime('%Y%m%d')}-{i+1:05d}"
        
        # Random timestamp throughout the day
        hour = int(rng.choice(range(6, 23), p=_hour_weights()))
        minute = int(rng.integers(0, 60))
        order_timestamp = now.replace(hour=hour, minute=minute)
        
        # Random customer
        cust_idx = int(rng.integers(0, len(customer_ids)))
        customer_id = customer_ids[cust_idx]
        cust_lat = customer_lats[cust_idx]
        cust_lon = customer_lons[cust_idx]
        
        # Nearest warehouse
        nearest_wh = find_nearest_warehouse(cust_lat, cust_lon)
        
        # Allocation strategy determines assigned warehouse
        strategy = rng.choice(strategies, p=strategy_probs)
        if strategy == "nearest":
            assigned_wh = nearest_wh
        elif strategy == "cost_optimal":
            # Simulate: sometimes pick a different warehouse
            if rng.random() < 0.3:
                assigned_wh = rng.choice(WAREHOUSE_IDS)
            else:
                assigned_wh = nearest_wh
        else:  # load_balanced
            assigned_wh = rng.choice(WAREHOUSE_IDS)
        
        # Order priority
        priority = rng.choice(priorities, p=priority_probs)
        
        # Order status
        status = rng.choice(statuses, p=status_probs)
        
        # Return flag (only for delivered orders)
        return_flag = False
        if status == "Delivered" and rng.random() < RETURN_RATE:
            return_flag = True
        
        # Experiment assignment (~40% of orders participate)
        experiment_id = None
        experiment_group = None
        if len(active_experiments) > 0 and rng.random() < 0.40:
            exp = active_experiments.sample(1, random_state=int(rng.integers(0, 100000))).iloc[0]
            # Check if assigned warehouse is in experiment's target warehouses
            target_whs = exp["target_warehouses"].split(",") if pd.notna(exp["target_warehouses"]) else []
            if assigned_wh in target_whs:
                experiment_id = exp["experiment_id"]
                experiment_group = rng.choice(["Control", "Treatment"])
        
        # Generate order items (1-5 items per order)
        num_items = int(rng.choice([1, 1, 1, 2, 2, 2, 3, 3, 4, 5]))
        selected_products = rng.choice(len(product_ids), size=num_items, replace=False)
        
        total_amount = 0.0
        total_items = 0
        
        for prod_idx in selected_products:
            item_id = f"ITM-{current_date.strftime('%Y%m%d')}-{item_counter:06d}"
            quantity = int(rng.choice([1, 1, 1, 2, 2, 3]))
            unit_price = product_prices[prod_idx]
            
            # Discount
            discount = 0.0
            if rng.random() < DISCOUNT_PROBABILITY:
                discount_pct = rng.uniform(*DISCOUNT_RANGE)
                discount = round(unit_price * quantity * discount_pct, 2)
            
            revenue = round(unit_price * quantity - discount, 2)
            
            items_rows.append({
                "order_item_id": item_id,
                "order_id": order_id,
                "product_id": product_ids[prod_idx],
                "quantity": quantity,
                "unit_price": unit_price,
                "discount_amount": discount,
                "revenue": revenue,
                "created_at": order_timestamp,
                "updated_at": order_timestamp,
                "batch_id": batch_id,
            })
            
            total_amount += revenue
            total_items += quantity
            item_counter += 1
        
        # Calculate fulfillment cost
        distance = get_delivery_distance(assigned_wh, cust_lat, cust_lon)
        delivery_cost = calculate_delivery_cost(distance)
        fulfillment_cost = calculate_fulfillment_cost(delivery_cost, total_amount * 0.02)
        
        orders_rows.append({
            "order_id": order_id,
            "order_date": current_date,
            "order_timestamp": order_timestamp,
            "customer_id": customer_id,
            "assigned_warehouse_id": assigned_wh,
            "nearest_warehouse_id": nearest_wh,
            "allocation_strategy": strategy,
            "order_priority": priority,
            "total_items": total_items,
            "total_amount": round(total_amount, 2),
            "total_fulfillment_cost": round(fulfillment_cost, 2),
            "order_status": status,
            "return_flag": return_flag,
            "experiment_id": experiment_id,
            "experiment_group": experiment_group,
            "created_at": order_timestamp,
            "updated_at": order_timestamp,
            "batch_id": batch_id,
        })
    
    return pd.DataFrame(orders_rows), pd.DataFrame(items_rows)


def _hour_weights():
    """Order probability by hour (6am-10pm). Peak at lunch and evening."""
    weights = [
        0.02, 0.03, 0.05, 0.08, 0.10, 0.12,  # 6-11am
        0.11, 0.09, 0.08, 0.07, 0.06, 0.05,   # 12-5pm
        0.04, 0.04, 0.03, 0.02, 0.01,          # 6-10pm
    ]
    total = sum(weights)
    return [w / total for w in weights]