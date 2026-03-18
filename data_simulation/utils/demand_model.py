"""
Demand model for generating realistic product-level demand.
Each product gets a base daily demand, then seasonality and noise are applied.

v2: Added category demand weights so Electronics/Health dominate,
    Grocery/Beauty have lower demand — creates visible category variation.
"""

from typing import Dict

import numpy as np

from config.constants import CATEGORY_DEMAND_WEIGHTS


def generate_base_demand_map(num_products: int, rng: np.random.Generator, products_df=None) -> Dict[str, float]:
    """
    Generate base daily demand for each product.
    Uses log-normal distribution (Pareto-like: few bestsellers, many slow movers).
    Category demand weights applied so Electronics products have higher base demand
    than Grocery products — creates visible category-level variation in charts.

    Returns: dict of product_id -> base_daily_demand
    """
    base_demands = rng.lognormal(mean=1.5, sigma=0.9, size=num_products)
    base_demands = np.clip(base_demands, 0.3, 80.0)

    demand_map = {}

    if products_df is not None:
        # Apply category weight to each product's base demand
        for i, (_, row) in enumerate(products_df.iterrows()):
            product_id = row["product_id"]
            category = row.get("category", "Apparel")
            cat_weight = CATEGORY_DEMAND_WEIGHTS.get(category, 1.0)

            # Additional product-tier variation within category
            # Top 20% of products in a category get a "bestseller" boost
            tier_boost = 1.0
            if base_demands[i] > np.percentile(base_demands, 80):
                tier_boost = rng.uniform(1.5, 3.0)  # Bestseller — 1.5x to 3x boost
            elif base_demands[i] < np.percentile(base_demands, 20):
                tier_boost = rng.uniform(0.2, 0.5)  # Slow mover — 0.2x to 0.5x

            demand_map[product_id] = round(float(base_demands[i]) * cat_weight * tier_boost, 2)
    else:
        # Fallback: no product dataframe provided
        for i in range(num_products):
            product_id = f"PROD-{i + 1:04d}"
            demand_map[product_id] = round(float(base_demands[i]), 2)

    return demand_map


def distribute_demand_across_warehouses(
    total_demand: int, num_warehouses: int, rng: np.random.Generator, warehouse_ids: list = None
) -> list:
    """
    Distribute a product's total daily demand across warehouses.
    Uses warehouse demand weights so LA-West/NYC-East get more orders
    than DEN-Mountain/SEA-NW — creates visible warehouse variation in charts.
    """
    if total_demand <= 0:
        return [0] * num_warehouses

    if warehouse_ids is not None:
        from config.constants import WAREHOUSE_DEMAND_WEIGHTS

        # Use warehouse weights as Dirichlet concentration parameters
        # Higher weight = more demand allocated to that warehouse
        weights = np.array([WAREHOUSE_DEMAND_WEIGHTS.get(wh_id, 1.0) for wh_id in warehouse_ids])
        # Scale weights for Dirichlet (multiply by 3 to reduce variance)
        concentrations = weights * 3.0
    else:
        # Equal distribution fallback
        concentrations = np.ones(num_warehouses) * 2.0

    proportions = rng.dirichlet(concentrations)

    # Convert to integer counts
    demands = np.round(proportions * total_demand).astype(int)

    # Fix rounding: adjust largest warehouse to match total exactly
    diff = total_demand - demands.sum()
    demands[demands.argmax()] += diff

    return demands.tolist()


def get_category_revenue_multiplier(category: str, year: int) -> float:
    """
    Returns a revenue multiplier for a category in a given year.
    Electronics grows faster; Grocery grows slower.
    This creates diverging category revenue lines over 4 years.
    """
    from config.constants import BASE_YEAR

    base_modifier = {
        "Electronics": 1.04,
        "Health": 1.03,
        "Beauty": 1.02,
        "Sports": 1.01,
        "Home & Garden": 1.01,
        "Apparel": 1.00,
        "Toys": 1.00,
        "Grocery": 0.98,
    }
    modifier = base_modifier.get(category, 1.0)
    years_elapsed = max(0, year - BASE_YEAR)
    return modifier**years_elapsed
