# data_simulation/core/shipments.py

"""
Shipment generation for a single day.
Generates: fact_shipments (supplier → warehouse replenishment)
"""

import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta
from typing import Dict, Tuple, List

from config.constants import (
    SUPPLIER_CONFIGS, BATCH_ID_PREFIX, REORDER_QUANTITY_RANGE,
)
from config.warehouse_config import WAREHOUSE_IDS
from data_simulation.utils.cost import calculate_shipment_cost


def generate_daily_shipments(
    current_date: date,
    products_df: pd.DataFrame,
    suppliers_df: pd.DataFrame,
    inventory_state: Dict[Tuple[str, str], dict],
    pending_shipments: List[dict],
    rng: np.random.Generator,
    shipment_counter: int,
) -> Tuple[pd.DataFrame, List[dict], int]:
    """
    Generate fact_shipments for a single day.
    Creates new shipments for reorder triggers and resolves arriving shipments.
    
    Returns: (shipments_df, updated_pending_shipments, updated_counter)
    """
    batch_id = f"{BATCH_ID_PREFIX}_{current_date.strftime('%Y%m%d')}"
    now = datetime.combine(current_date, datetime.min.time())
    
    # Build supplier lookup by category
    supplier_by_category = {}
    for _, sup in suppliers_df.iterrows():
        cats = sup["product_categories"].split(",")
        for cat in cats:
            cat = cat.strip()
            if cat not in supplier_by_category:
                supplier_by_category[cat] = []
            supplier_by_category[cat].append(sup)
    
    product_lookup = products_df.set_index("product_id").to_dict("index")
    
    new_shipments = []
    
    # Create shipments for products that triggered reorder
    for (wh_id, pid), state in inventory_state.items():
        product = product_lookup.get(pid)
        if not product:
            continue
        
        # Check if reorder was triggered (closing_stock <= reorder_point and no pending orders)
        if state["closing_stock"] <= product["reorder_point"] and state["units_on_order"] > 0:
            # Find a supplier for this product's category
            category = product["category"]
            available_suppliers = supplier_by_category.get(category, [])
            
            if not available_suppliers:
                continue
            
            # Pick a supplier (weighted by reliability)
            supplier = available_suppliers[int(rng.integers(0, len(available_suppliers)))]
            
            # Shipment quantity
            quantity = int(rng.integers(*REORDER_QUANTITY_RANGE))
            
            # Calculate lead time with variability
            base_lead = supplier["average_lead_time"]
            std_dev = supplier["lead_time_std_dev"]
            actual_lead = max(1, int(rng.normal(base_lead, std_dev)))
            
            expected_arrival = current_date + timedelta(days=base_lead)
            actual_arrival = current_date + timedelta(days=actual_lead)
            
            delay_days = max(0, actual_lead - base_lead)
            delay_flag = delay_days > 0
            
            # Is this a reliability miss?
            if rng.random() > supplier["reliability_score"]:
                # Supplier is late — add extra delay
                extra_delay = int(rng.integers(1, 5))
                actual_lead += extra_delay
                actual_arrival = current_date + timedelta(days=actual_lead)
                delay_days = actual_lead - base_lead
                delay_flag = True
            
            shipment_id = f"SHP-{current_date.strftime('%Y%m%d')}-{shipment_counter:05d}"
            shipment_cost = calculate_shipment_cost(quantity)
            
            shipment = {
                "shipment_id": shipment_id,
                "supplier_id": supplier["supplier_id"],
                "warehouse_id": wh_id,
                "product_id": pid,
                "quantity": quantity,
                "shipment_cost": shipment_cost,
                "shipment_date": current_date,
                "expected_arrival_date": expected_arrival,
                "actual_arrival_date": actual_arrival,
                "delay_days": delay_days,
                "delay_flag": delay_flag,
                "reorder_triggered_flag": True,
                "created_at": now,
                "updated_at": now,
                "batch_id": batch_id,
            }
            
            new_shipments.append(shipment)
            pending_shipments.append(shipment)
            shipment_counter += 1
    
    # Find shipments arriving today
    arriving_today = [s for s in pending_shipments if s["actual_arrival_date"] == current_date]
    
    # Remove arrived shipments from pending
    pending_shipments = [s for s in pending_shipments if s["actual_arrival_date"] > current_date]
    
    # Combine new shipments + any that need to be recorded
    all_shipments_today = new_shipments
    
    # Convert arriving shipments to DataFrame for inventory processing
    arriving_df = pd.DataFrame(arriving_today) if arriving_today else pd.DataFrame()
    
    shipments_df = pd.DataFrame(all_shipments_today) if all_shipments_today else pd.DataFrame()
    
    return shipments_df, arriving_df, pending_shipments, shipment_counter