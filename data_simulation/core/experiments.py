# data_simulation/core/experiments.py
"""
Experiment assignment generation for a single day.
Generates: fact_experiment_assignments
"""

import numpy as np
import pandas as pd
from datetime import date, datetime

from config.constants import BATCH_ID_PREFIX


def generate_daily_experiment_assignments(
    current_date: date,
    orders_df: pd.DataFrame,
    rng: np.random.Generator,
    assignment_counter: int,
) -> tuple:
    """
    Generate fact_experiment_assignments for a single day.
    Creates assignment records for orders that have experiment_id set.
    
    Returns: (assignments_df, updated_counter)
    """
    batch_id = f"{BATCH_ID_PREFIX}_{current_date.strftime('%Y%m%d')}"
    now = datetime.combine(current_date, datetime.min.time())
    
    # Filter orders that are part of experiments
    experiment_orders = orders_df[orders_df["experiment_id"].notna()]
    
    if len(experiment_orders) == 0:
        return pd.DataFrame(), assignment_counter
    
    rows = []
    
    for _, order in experiment_orders.iterrows():
        assignment_id = f"ASG-{current_date.strftime('%Y%m%d')}-{assignment_counter:05d}"
        
        rows.append({
            "assignment_id": assignment_id,
            "experiment_id": order["experiment_id"],
            "order_id": order["order_id"],
            "group_name": order["experiment_group"],
            "assigned_at": order["order_timestamp"],
            "warehouse_id": order["assigned_warehouse_id"],
            "created_at": now,
            "updated_at": now,
            "batch_id": batch_id,
        })
        assignment_counter += 1
    
    return pd.DataFrame(rows), assignment_counter