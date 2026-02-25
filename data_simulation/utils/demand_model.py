# data_simulation/utils/demand_model.py
"""
Demand model for generating realistic product-level demand.
Each product gets a base daily demand, then seasonality and noise are applied.
"""

import numpy as np
from typing import Dict


def generate_base_demand_map(num_products: int, rng: np.random.Generator) -> Dict[str, float]:
    """
    Generate a base daily demand for each product.
    Uses a log-normal distribution so most products have low demand
    and a few have high demand (realistic Pareto-like pattern).
    
    Returns: dict of product_id -> base_daily_demand
    """
    # Log-normal: mean ~5 units/day, with long tail up to ~50
    base_demands = rng.lognormal(mean=1.5, sigma=0.8, size=num_products)
    
    # Clamp to reasonable range
    base_demands = np.clip(base_demands, 0.5, 50.0)
    
    demand_map = {}
    for i in range(num_products):
        product_id = f"PROD-{i+1:04d}"
        demand_map[product_id] = round(float(base_demands[i]), 2)
    
    return demand_map


def distribute_demand_across_warehouses(
    total_demand: int,
    num_warehouses: int,
    rng: np.random.Generator
) -> list:
    """
    Distribute a product's total daily demand across warehouses.
    Uses Dirichlet distribution for realistic uneven splits.
    """
    if total_demand <= 0:
        return [0] * num_warehouses
    
    # Dirichlet gives a natural uneven split
    proportions = rng.dirichlet(np.ones(num_warehouses) * 2.0)
    
    # Convert to integer counts
    demands = np.round(proportions * total_demand).astype(int)
    
    # Fix rounding: adjust largest warehouse to match total
    diff = total_demand - demands.sum()
    demands[demands.argmax()] += diff
    
    return demands.tolist()