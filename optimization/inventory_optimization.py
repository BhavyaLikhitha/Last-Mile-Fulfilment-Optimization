"""
Inventory Optimization Engine
Computes Economic Order Quantity (EOQ) and optimal safety stock levels
per product per warehouse, then estimates the cost impact vs baseline.

Tools: SciPy (minimize) for safety stock optimization
       NumPy for EOQ formula

Output: Written to mart_cost_optimization via run_optimization.py
"""

import numpy as np
import pandas as pd
from scipy.optimize import minimize_scalar


# ── EOQ Parameters ────────────────────────────────────────────
HOLDING_COST_RATE  = 0.001    # 0.1% of unit cost per day
ORDERING_BASE_COST = 25.0     # $ fixed cost per order
STOCKOUT_COST      = 15.0     # $ estimated cost per stockout unit
SERVICE_LEVEL      = 0.95     # 95% service level target → z = 1.645
Z_SCORE            = 1.645    # z-score for 95% service level


def compute_eoq(annual_demand: float, unit_cost: float, ordering_cost: float = ORDERING_BASE_COST) -> float:
    """
    Economic Order Quantity formula.
    EOQ = sqrt(2 × D × S / H)
    where:
      D = annual demand
      S = ordering cost per order
      H = holding cost per unit per year

    Args:
        annual_demand: expected units sold per year
        unit_cost    : product cost price
        ordering_cost: fixed cost per order

    Returns:
        Optimal order quantity (units)
    """
    if annual_demand <= 0 or unit_cost <= 0:
        return 0.0

    holding_cost_per_unit = unit_cost * HOLDING_COST_RATE * 365
    if holding_cost_per_unit <= 0:
        return 0.0

    eoq = np.sqrt(2 * annual_demand * ordering_cost / holding_cost_per_unit)
    return round(eoq, 2)


def compute_optimal_safety_stock(
    avg_daily_demand: float,
    demand_std: float,
    lead_time_days: float,
    lead_time_std: float = 1.0
) -> float:
    """
    Optimal safety stock using demand and lead time variability.
    SS = Z × sqrt(LT × σ_d² + D² × σ_lt²)
    where:
      Z     = service level z-score
      LT    = average lead time
      σ_d   = daily demand std dev
      D     = average daily demand
      σ_lt  = lead time std dev

    Args:
        avg_daily_demand: mean units sold per day
        demand_std      : std dev of daily demand
        lead_time_days  : supplier average lead time
        lead_time_std   : supplier lead time std dev

    Returns:
        Optimal safety stock (units)
    """
    if avg_daily_demand <= 0:
        return 0.0

    variance = (lead_time_days * demand_std ** 2) + (avg_daily_demand ** 2 * lead_time_std ** 2)
    safety_stock = Z_SCORE * np.sqrt(max(variance, 0))
    return round(safety_stock, 2)


def optimize_reorder_point(
    avg_daily_demand: float,
    demand_std: float,
    lead_time_days: float,
    unit_cost: float,
    lead_time_std: float = 1.0
) -> dict:
    """
    Minimize total inventory cost (holding + stockout) to find optimal reorder point.
    Uses SciPy scalar minimization.

    Total cost(ROP) = holding_cost(ROP) + stockout_cost(ROP)

    Args:
        avg_daily_demand: mean units sold per day
        demand_std      : std dev of daily demand
        lead_time_days  : supplier average lead time
        unit_cost       : product cost price
        lead_time_std   : supplier lead time std dev

    Returns:
        dict with optimal_rop, safety_stock, total_cost, holding_cost, stockout_cost
    """
    if avg_daily_demand <= 0 or lead_time_days <= 0:
        return {
            'optimal_rop': 0, 'safety_stock': 0,
            'total_cost': 0, 'holding_cost': 0, 'stockout_cost': 0
        }

    avg_demand_during_lt = avg_daily_demand * lead_time_days
    holding_cost_per_unit_day = unit_cost * HOLDING_COST_RATE

    def total_cost(rop):
        safety_stock = max(rop - avg_demand_during_lt, 0)
        h_cost = safety_stock * holding_cost_per_unit_day * lead_time_days
        # Stockout probability approximated by normal distribution tail
        if demand_std > 0:
            from scipy import stats
            z = (rop - avg_demand_during_lt) / (demand_std * np.sqrt(lead_time_days))
            stockout_prob = 1 - stats.norm.cdf(z)
        else:
            stockout_prob = 0.0
        s_cost = stockout_prob * avg_daily_demand * STOCKOUT_COST
        return h_cost + s_cost

    # Search between 0 and 3× average demand during lead time
    upper_bound = max(avg_demand_during_lt * 3, 1)
    result = minimize_scalar(total_cost, bounds=(0, upper_bound), method='bounded')

    optimal_rop = round(result.x, 2)
    safety_stock = round(max(optimal_rop - avg_demand_during_lt, 0), 2)

    return {
        'optimal_rop'  : optimal_rop,
        'safety_stock' : safety_stock,
        'total_cost'   : round(result.fun, 4),
        'holding_cost' : round(safety_stock * holding_cost_per_unit_day * lead_time_days, 4),
        'stockout_cost': round(result.fun - safety_stock * holding_cost_per_unit_day * lead_time_days, 4)
    }


def compute_inventory_optimization_summary(
    product_kpis: pd.DataFrame,
    products: pd.DataFrame,
    suppliers: pd.DataFrame
) -> pd.DataFrame:
    """
    Compute EOQ and optimal reorder points for all products.
    Returns a summary of potential cost savings from inventory optimization.

    Args:
        product_kpis: mart_daily_product_kpis (aggregated to product level)
        products    : stg_products dimension
        suppliers   : stg_suppliers dimension

    Returns:
        DataFrame with product-level optimization results
    """
    # Aggregate to product level — avg daily demand and volatility
    product_agg = (
        product_kpis[product_kpis['is_forecast'] == False]
        .groupby('product_id')
        .agg(
            avg_daily_demand=('total_units_sold', 'mean'),
            demand_std=('total_units_sold', 'std'),
            avg_closing_stock=('avg_closing_stock', 'mean'),
            total_holding_cost=('total_holding_cost', 'sum'),
        )
        .reset_index()
    )

    # Merge product attributes
    product_agg = product_agg.merge(
        products[['product_id', 'cost_price', 'lead_time_days', 'safety_stock', 'reorder_point']],
        on='product_id', how='left'
    )

    # Merge supplier lead time variability (use average across suppliers)
    avg_lead_time_std = suppliers['lead_time_std_dev'].mean() if len(suppliers) > 0 else 1.0

    results = []
    for _, row in product_agg.iterrows():
        annual_demand = row['avg_daily_demand'] * 365
        demand_std    = row['demand_std'] if not pd.isna(row['demand_std']) else 0

        eoq = compute_eoq(annual_demand, row['cost_price'])

        opt = optimize_reorder_point(
            avg_daily_demand=row['avg_daily_demand'],
            demand_std=demand_std,
            lead_time_days=row['lead_time_days'],
            unit_cost=row['cost_price'],
            lead_time_std=avg_lead_time_std
        )

        results.append({
            'product_id'        : row['product_id'],
            'avg_daily_demand'  : round(row['avg_daily_demand'], 2),
            'eoq'               : eoq,
            'optimal_rop'       : opt['optimal_rop'],
            'optimal_safety_stock': opt['safety_stock'],
            'current_safety_stock': row['safety_stock'],
            'optimized_holding_cost': opt['holding_cost'],
        })

    return pd.DataFrame(results)