# """
# Order generation for a single day.
# Generates: fact_orders + fact_order_items

# v3: Added region-biased customer selection so warehouse demand weights
#     actually affect order distribution. LA-West and NYC-East now
#     clearly dominate the orders by warehouse chart.
# """

# import numpy as np
# import pandas as pd
# from datetime import date, datetime, timedelta
# from typing import Tuple

# from config.constants import (
#     DAILY_ORDERS, ORDER_PRIORITY_DISTRIBUTION, SLA_MINUTES,
#     ORDER_STATUS_DISTRIBUTION, RETURN_RATE, ALLOCATION_STRATEGIES,
#     DISCOUNT_PROBABILITY, DISCOUNT_RANGE, BATCH_ID_PREFIX,
#     WAREHOUSE_DEMAND_WEIGHTS, CATEGORY_DEMAND_WEIGHTS,
#     WAREHOUSE_REGION_MAP,
#     get_price_inflation_multiplier,
# )
# from config.warehouse_config import WAREHOUSE_IDS
# from data_simulation.utils.seasonality import get_daily_order_count, get_warehouse_order_share
# from data_simulation.utils.geo import find_nearest_warehouse, get_delivery_distance
# from data_simulation.utils.cost import calculate_fulfillment_cost, calculate_delivery_cost


# def generate_daily_orders(
#     current_date: date,
#     customers_df: pd.DataFrame,
#     products_df: pd.DataFrame,
#     experiments_df: pd.DataFrame,
#     rng: np.random.Generator,
#     day_counter: int,
# ) -> Tuple[pd.DataFrame, pd.DataFrame]:
#     """
#     Generate fact_orders and fact_order_items for a single day.

#     Key changes in v3:
#     - Customer selection is region-biased using WAREHOUSE_DEMAND_WEIGHTS
#     - LA-West picks from West-region customers, NYC-East from Northeast etc.
#     - This makes warehouse order distribution reflect the demand weights
#     - Orders still use nearest_warehouse logic so geography is consistent
#     """
#     batch_id = f"{BATCH_ID_PREFIX}_{current_date.strftime('%Y%m%d')}"
#     now      = datetime.combine(current_date, datetime.min.time())
#     year     = current_date.year

#     price_inflation = get_price_inflation_multiplier(year)
#     num_orders      = get_daily_order_count(DAILY_ORDERS, current_date, rng)

#     priorities     = list(ORDER_PRIORITY_DISTRIBUTION.keys())
#     priority_probs = list(ORDER_PRIORITY_DISTRIBUTION.values())
#     statuses       = list(ORDER_STATUS_DISTRIBUTION.keys())
#     status_probs   = list(ORDER_STATUS_DISTRIBUTION.values())
#     strategies     = list(ALLOCATION_STRATEGIES.keys())
#     strategy_probs = list(ALLOCATION_STRATEGIES.values())

#     active_experiments = experiments_df[
#         (experiments_df["start_date"] <= current_date) &
#         ((experiments_df["end_date"].isna()) | (experiments_df["end_date"] >= current_date))
#     ]

#     # Product arrays
#     product_ids    = products_df["product_id"].values
#     product_prices = (products_df["selling_price"].values * price_inflation)
#     product_cats   = products_df["category"].values

#     # ── Build region-to-customer index for fast lookup ──────────
#     # Group customer indices by region so we can bias selection
#     region_customer_idx = {}
#     for region in customers_df["region"].unique():
#         mask = customers_df["region"] == region
#         region_customer_idx[region] = np.where(mask)[0]

#     # Warehouse demand weights normalized to probabilities
#     wh_ids      = list(WAREHOUSE_DEMAND_WEIGHTS.keys())
#     wh_weights  = np.array([WAREHOUSE_DEMAND_WEIGHTS[w] for w in wh_ids])
#     wh_weights  = wh_weights / wh_weights.sum()

#     # Fallback: all customers (used if region has no customers)
#     all_cust_idx = np.arange(len(customers_df))

#     orders_rows = []
#     items_rows  = []
#     item_counter = 1

#     for i in range(num_orders):
#         order_id = f"ORD-{current_date.strftime('%Y%m%d')}-{i+1:05d}"

#         hour   = int(rng.choice(range(6, 23), p=_hour_weights()))
#         minute = int(rng.integers(0, 60))
#         order_timestamp = now.replace(hour=hour, minute=minute)

#         # ── Region-biased customer selection ────────────────────
#         # Pick a warehouse region proportional to demand weights
#         # Then pick a customer from that region
#         # This ensures LA-West (weight 1.40) sees ~19% of orders
#         # and DEN-Mountain (weight 0.70) sees ~9.5%
#         target_wh     = rng.choice(wh_ids, p=wh_weights)
#         target_region = WAREHOUSE_REGION_MAP.get(target_wh, "")
#         region_idx    = region_customer_idx.get(target_region, all_cust_idx)

#         if len(region_idx) > 0:
#             cust_idx = int(rng.choice(region_idx))
#         else:
#             cust_idx = int(rng.integers(0, len(customers_df)))

#         customer_id = customers_df["customer_id"].iloc[cust_idx]
#         cust_lat    = customers_df["latitude"].iloc[cust_idx]
#         cust_lon    = customers_df["longitude"].iloc[cust_idx]

#         # Nearest warehouse is now geographically consistent with region
#         nearest_wh = find_nearest_warehouse(cust_lat, cust_lon)

#         # Allocation strategy
#         strategy = rng.choice(strategies, p=strategy_probs)
#         if strategy == "nearest":
#             assigned_wh = nearest_wh
#         elif strategy == "cost_optimal":
#             if rng.random() < 0.3:
#                 assigned_wh = rng.choice(wh_ids, p=wh_weights)
#             else:
#                 assigned_wh = nearest_wh
#         else:  # load_balanced
#             assigned_wh = rng.choice(wh_ids, p=wh_weights)

#         priority = rng.choice(priorities, p=priority_probs)
#         status   = rng.choice(statuses,   p=status_probs)

#         return_flag = False
#         if status == "Delivered" and rng.random() < RETURN_RATE:
#             return_flag = True

#         experiment_id    = None
#         experiment_group = None
#         if len(active_experiments) > 0 and rng.random() < 0.40:
#             exp = active_experiments.sample(
#                 1, random_state=int(rng.integers(0, 100000))
#             ).iloc[0]
#             target_whs = (
#                 exp["target_warehouses"].split(",")
#                 if pd.notna(exp["target_warehouses"]) else []
#             )
#             if assigned_wh in target_whs:
#                 experiment_id    = exp["experiment_id"]
#                 experiment_group = rng.choice(["Control", "Treatment"])

#         # Category-weighted product selection
#         num_items   = int(rng.choice([1, 1, 1, 2, 2, 2, 3, 3, 4, 5]))
#         cat_weights = np.array([
#             CATEGORY_DEMAND_WEIGHTS.get(cat, 1.0) for cat in product_cats
#         ])
#         cat_weights      = cat_weights / cat_weights.sum()
#         selected_products = rng.choice(
#             len(product_ids), size=num_items, replace=False, p=cat_weights
#         )

#         total_amount = 0.0
#         total_items  = 0

#         for prod_idx in selected_products:
#             item_id    = f"ITM-{current_date.strftime('%Y%m%d')}-{item_counter:06d}"
#             quantity   = int(rng.choice([1, 1, 1, 2, 2, 3]))
#             unit_price = round(float(product_prices[prod_idx]), 2)

#             discount = 0.0
#             if rng.random() < DISCOUNT_PROBABILITY:
#                 discount_pct = rng.uniform(*DISCOUNT_RANGE)
#                 discount     = round(unit_price * quantity * discount_pct, 2)

#             revenue = round(unit_price * quantity - discount, 2)

#             items_rows.append({
#                 "order_item_id":   item_id,
#                 "order_id":        order_id,
#                 "product_id":      product_ids[prod_idx],
#                 "quantity":        quantity,
#                 "unit_price":      unit_price,
#                 "discount_amount": discount,
#                 "revenue":         revenue,
#                 "created_at":      order_timestamp,
#                 "updated_at":      order_timestamp,
#                 "batch_id":        batch_id,
#             })

#             total_amount += revenue
#             total_items  += quantity
#             item_counter += 1

#         distance         = get_delivery_distance(assigned_wh, cust_lat, cust_lon)
#         delivery_cost    = calculate_delivery_cost(distance)
#         fulfillment_cost = calculate_fulfillment_cost(delivery_cost, total_amount * 0.02)

#         orders_rows.append({
#             "order_id":               order_id,
#             "order_date":             current_date,
#             "order_timestamp":        order_timestamp,
#             "customer_id":            customer_id,
#             "assigned_warehouse_id":  assigned_wh,
#             "nearest_warehouse_id":   nearest_wh,
#             "allocation_strategy":    strategy,
#             "order_priority":         priority,
#             "total_items":            total_items,
#             "total_amount":           round(total_amount, 2),
#             "total_fulfillment_cost": round(fulfillment_cost, 2),
#             "order_status":           status,
#             "return_flag":            return_flag,
#             "experiment_id":          experiment_id,
#             "experiment_group":       experiment_group,
#             "created_at":             order_timestamp,
#             "updated_at":             order_timestamp,
#             "batch_id":               batch_id,
#         })

#     return pd.DataFrame(orders_rows), pd.DataFrame(items_rows)


# def _hour_weights():
#     weights = [
#         0.02, 0.03, 0.05, 0.08, 0.10, 0.12,
#         0.11, 0.09, 0.08, 0.07, 0.06, 0.05,
#         0.04, 0.04, 0.03, 0.02, 0.01,
#     ]
#     total = sum(weights)
#     return [w / total for w in weights]


"""
Order generation for a single day.
Generates: fact_orders + fact_order_items

v4: Added warehouse-specific cost_optimal redirect probability.
    High-demand warehouses (NYC/LA) redirect more orders when cost_optimal
    strategy is selected — creates realistic cross-region fulfillment patterns.
    Previously all warehouses redirected at the same 30% rate, flattening
    the allocation efficiency and cross-region % metrics.
"""

import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta
from typing import Tuple

from config.constants import (
    DAILY_ORDERS, ORDER_PRIORITY_DISTRIBUTION, SLA_MINUTES,
    ORDER_STATUS_DISTRIBUTION, RETURN_RATE, ALLOCATION_STRATEGIES,
    DISCOUNT_PROBABILITY, DISCOUNT_RANGE, BATCH_ID_PREFIX,
    WAREHOUSE_DEMAND_WEIGHTS, CATEGORY_DEMAND_WEIGHTS,
    WAREHOUSE_REGION_MAP, COST_OPTIMAL_REDIRECT_PROBABILITY,
    get_price_inflation_multiplier,
)
from config.warehouse_config import WAREHOUSE_IDS
from data_simulation.utils.seasonality import get_daily_order_count, get_warehouse_order_share
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

    Key changes in v4:
    - cost_optimal strategy uses warehouse-specific redirect probability
      NYC: 55% redirect (over capacity, many alternatives)
      Denver: 15% redirect (low volume, nearest usually optimal)
    - This creates realistic cross-region % variation across warehouses
    - nearest_assignment_rate naturally differs: NYC ~58%, Denver ~95%
    """
    batch_id = f"{BATCH_ID_PREFIX}_{current_date.strftime('%Y%m%d')}"
    now      = datetime.combine(current_date, datetime.min.time())
    year     = current_date.year

    price_inflation = get_price_inflation_multiplier(year)
    num_orders      = get_daily_order_count(DAILY_ORDERS, current_date, rng)

    priorities     = list(ORDER_PRIORITY_DISTRIBUTION.keys())
    priority_probs = list(ORDER_PRIORITY_DISTRIBUTION.values())
    statuses       = list(ORDER_STATUS_DISTRIBUTION.keys())
    status_probs   = list(ORDER_STATUS_DISTRIBUTION.values())
    strategies     = list(ALLOCATION_STRATEGIES.keys())
    strategy_probs = list(ALLOCATION_STRATEGIES.values())

    active_experiments = experiments_df[
        (experiments_df["start_date"] <= current_date) &
        ((experiments_df["end_date"].isna()) | (experiments_df["end_date"] >= current_date))
    ]

    # Product arrays
    product_ids    = products_df["product_id"].values
    product_prices = (products_df["selling_price"].values * price_inflation)
    product_cats   = products_df["category"].values

    # ── Build region-to-customer index for fast lookup ──────────
    region_customer_idx = {}
    for region in customers_df["region"].unique():
        mask = customers_df["region"] == region
        region_customer_idx[region] = np.where(mask)[0]

    # Warehouse demand weights normalized to probabilities
    wh_ids     = list(WAREHOUSE_DEMAND_WEIGHTS.keys())
    wh_weights = np.array([WAREHOUSE_DEMAND_WEIGHTS[w] for w in wh_ids])
    wh_weights = wh_weights / wh_weights.sum()

    # Fallback: all customers
    all_cust_idx = np.arange(len(customers_df))

    orders_rows = []
    items_rows  = []
    item_counter = 1

    for i in range(num_orders):
        order_id = f"ORD-{current_date.strftime('%Y%m%d')}-{i+1:05d}"

        hour   = int(rng.choice(range(6, 23), p=_hour_weights()))
        minute = int(rng.integers(0, 60))
        order_timestamp = now.replace(hour=hour, minute=minute)

        # ── Region-biased customer selection ────────────────────
        target_wh     = rng.choice(wh_ids, p=wh_weights)
        target_region = WAREHOUSE_REGION_MAP.get(target_wh, "")
        region_idx    = region_customer_idx.get(target_region, all_cust_idx)

        if len(region_idx) > 0:
            cust_idx = int(rng.choice(region_idx))
        else:
            cust_idx = int(rng.integers(0, len(customers_df)))

        customer_id = customers_df["customer_id"].iloc[cust_idx]
        cust_lat    = customers_df["latitude"].iloc[cust_idx]
        cust_lon    = customers_df["longitude"].iloc[cust_idx]

        nearest_wh = find_nearest_warehouse(cust_lat, cust_lon)

        # ── Allocation strategy with warehouse-specific redirect ─
        strategy = rng.choice(strategies, p=strategy_probs)
        if strategy == "nearest":
            assigned_wh = nearest_wh
        elif strategy == "cost_optimal":
            # ── FIX: warehouse-specific redirect probability ─────
            # High-demand warehouses (NYC/LA) are frequently at or near
            # capacity, so cost_optimal routing redirects more orders
            # to balance load. Low-demand warehouses (Denver) rarely
            # need to redirect since they have spare capacity.
            # This creates natural variation in nearest_assignment_rate
            # and cross_region_pct across warehouses without post-processing.
            redirect_prob = COST_OPTIMAL_REDIRECT_PROBABILITY.get(nearest_wh, 0.30)
            if rng.random() < redirect_prob:
                assigned_wh = rng.choice(wh_ids, p=wh_weights)
            else:
                assigned_wh = nearest_wh
        else:  # load_balanced
            assigned_wh = rng.choice(wh_ids, p=wh_weights)

        priority = rng.choice(priorities, p=priority_probs)
        status   = rng.choice(statuses,   p=status_probs)

        return_flag = False
        if status == "Delivered" and rng.random() < RETURN_RATE:
            return_flag = True

        experiment_id    = None
        experiment_group = None
        if len(active_experiments) > 0 and rng.random() < 0.40:
            exp = active_experiments.sample(
                1, random_state=int(rng.integers(0, 100000))
            ).iloc[0]
            target_whs = (
                exp["target_warehouses"].split(",")
                if pd.notna(exp["target_warehouses"]) else []
            )
            if assigned_wh in target_whs:
                experiment_id    = exp["experiment_id"]
                experiment_group = rng.choice(["Control", "Treatment"])

        # Category-weighted product selection
        num_items   = int(rng.choice([1, 1, 1, 2, 2, 2, 3, 3, 4, 5]))
        cat_weights = np.array([
            CATEGORY_DEMAND_WEIGHTS.get(cat, 1.0) for cat in product_cats
        ])
        cat_weights       = cat_weights / cat_weights.sum()
        selected_products = rng.choice(
            len(product_ids), size=num_items, replace=False, p=cat_weights
        )

        total_amount = 0.0
        total_items  = 0

        for prod_idx in selected_products:
            item_id    = f"ITM-{current_date.strftime('%Y%m%d')}-{item_counter:06d}"
            quantity   = int(rng.choice([1, 1, 1, 2, 2, 3]))
            unit_price = round(float(product_prices[prod_idx]), 2)

            discount = 0.0
            if rng.random() < DISCOUNT_PROBABILITY:
                discount_pct = rng.uniform(*DISCOUNT_RANGE)
                discount     = round(unit_price * quantity * discount_pct, 2)

            revenue = round(unit_price * quantity - discount, 2)

            items_rows.append({
                "order_item_id":   item_id,
                "order_id":        order_id,
                "product_id":      product_ids[prod_idx],
                "quantity":        quantity,
                "unit_price":      unit_price,
                "discount_amount": discount,
                "revenue":         revenue,
                "created_at":      order_timestamp,
                "updated_at":      order_timestamp,
                "batch_id":        batch_id,
            })

            total_amount += revenue
            total_items  += quantity
            item_counter += 1

        distance         = get_delivery_distance(assigned_wh, cust_lat, cust_lon)
        delivery_cost    = calculate_delivery_cost(distance)
        fulfillment_cost = calculate_fulfillment_cost(delivery_cost, total_amount * 0.02)

        orders_rows.append({
            "order_id":               order_id,
            "order_date":             current_date,
            "order_timestamp":        order_timestamp,
            "customer_id":            customer_id,
            "assigned_warehouse_id":  assigned_wh,
            "nearest_warehouse_id":   nearest_wh,
            "allocation_strategy":    strategy,
            "order_priority":         priority,
            "total_items":            total_items,
            "total_amount":           round(total_amount, 2),
            "total_fulfillment_cost": round(fulfillment_cost, 2),
            "order_status":           status,
            "return_flag":            return_flag,
            "experiment_id":          experiment_id,
            "experiment_group":       experiment_group,
            "created_at":             order_timestamp,
            "updated_at":             order_timestamp,
            "batch_id":               batch_id,
        })

    return pd.DataFrame(orders_rows), pd.DataFrame(items_rows)


def _hour_weights():
    weights = [
        0.02, 0.03, 0.05, 0.08, 0.10, 0.12,
        0.11, 0.09, 0.08, 0.07, 0.06, 0.05,
        0.04, 0.04, 0.03, 0.02, 0.01,
    ]
    total = sum(weights)
    return [w / total for w in weights]