# # data_simulation/utils/seasonality.py
# """
# Seasonality and demand pattern utilities.
# Controls how demand varies by month, day-of-week, holidays, and category.
# """

# import numpy as np
# from datetime import date

# # Monthly seasonality multipliers (1.0 = average)
# # Higher in Nov-Dec (holiday shopping), lower in Jan-Feb (post-holiday)
# MONTHLY_SEASONALITY = {
#     1: 0.80,   # January - post-holiday dip
#     2: 0.82,   # February
#     3: 0.90,   # March - spring pickup
#     4: 0.95,   # April
#     5: 1.00,   # May
#     6: 1.05,   # June - summer
#     7: 1.02,   # July
#     8: 0.98,   # August - back to school
#     9: 1.00,   # September
#     10: 1.10,  # October - pre-holiday
#     11: 1.30,  # November - Black Friday
#     12: 1.40,  # December - holiday peak
# }

# # Day-of-week multipliers (Monday=1, Sunday=7)
# DAY_OF_WEEK_MULTIPLIERS = {
#     1: 1.10,  # Monday - high
#     2: 1.05,  # Tuesday
#     3: 1.00,  # Wednesday
#     4: 1.00,  # Thursday
#     5: 1.15,  # Friday - highest
#     6: 0.85,  # Saturday - lower
#     7: 0.75,  # Sunday - lowest
# }

# # Category-specific seasonal boosts (on top of base seasonality)
# CATEGORY_SEASONAL_BOOSTS = {
#     "Electronics": {11: 1.4, 12: 1.6},    # Big holiday boost
#     "Toys": {11: 1.5, 12: 1.8},           # Biggest holiday boost
#     "Apparel": {3: 1.2, 4: 1.2, 9: 1.3},  # Spring + back-to-school
#     "Grocery": {},                          # Relatively stable
#     "Home & Garden": {4: 1.3, 5: 1.4, 6: 1.3},  # Spring/summer
#     "Health": {1: 1.3, 2: 1.2},           # New year resolutions
#     "Sports": {5: 1.2, 6: 1.3, 7: 1.3},   # Summer
#     "Beauty": {2: 1.2, 12: 1.3},          # Valentine's + holidays
# }

# # Holiday demand spike
# HOLIDAY_MULTIPLIER = 1.25


# def get_demand_multiplier(current_date: date, category: str = None) -> float:
#     """
#     Calculate the total demand multiplier for a given date and optional category.
#     Combines monthly, day-of-week, holiday, and category-specific seasonality.

#     Returns a float multiplier (1.0 = normal demand).
#     """
#     month = current_date.month
#     dow = current_date.isoweekday()  # 1=Monday, 7=Sunday

#     # Base monthly seasonality
#     multiplier = MONTHLY_SEASONALITY.get(month, 1.0)

#     # Day-of-week effect
#     multiplier *= DAY_OF_WEEK_MULTIPLIERS.get(dow, 1.0)

#     # Holiday effect
#     is_holiday = (month, current_date.day) in {
#         (1, 1), (7, 4), (12, 25), (11, 24), (11, 25),
#         (11, 26), (11, 27),  # Black Friday weekend
#     }
#     if is_holiday:
#         multiplier *= HOLIDAY_MULTIPLIER

#     # Category-specific seasonal boost
#     if category and category in CATEGORY_SEASONAL_BOOSTS:
#         category_boost = CATEGORY_SEASONAL_BOOSTS[category].get(month, 1.0)
#         multiplier *= category_boost

#     return multiplier


# def get_daily_order_count(base_count: int, current_date: date, rng: np.random.Generator) -> int:
#     """
#     Calculate the number of orders for a given day.
#     Applies seasonality + random noise.
#     """
#     multiplier = get_demand_multiplier(current_date)

#     # Add random noise (±10%)
#     noise = rng.normal(1.0, 0.10)
#     noise = max(0.8, min(1.2, noise))  # Clamp

#     count = int(base_count * multiplier * noise)
#     return max(1, count)


# def get_product_demand(base_demand: float, current_date: date, category: str,
#                        rng: np.random.Generator) -> int:
#     """
#     Calculate demand for a specific product on a given day.
#     """
#     multiplier = get_demand_multiplier(current_date, category)
#     noise = rng.normal(1.0, 0.15)
#     noise = max(0.5, min(1.5, noise))

#     demand = int(base_demand * multiplier * noise)
#     return max(0, demand)

"""
Seasonality and demand pattern utilities.
Controls how demand varies by month, day-of-week, holidays, and category.

v2: Added larger noise bands, year-over-year drift, and shock event application.
"""

from datetime import date

import numpy as np

from config.constants import (
    BASE_YEAR,
    SHOCK_EVENT_PROBABILITY,
    SHOCK_EVENTS,
    get_year_growth_multiplier,
)

# ── Monthly Seasonality ──
MONTHLY_SEASONALITY = {
    1: 0.80,  # January  — post-holiday dip
    2: 0.82,  # February
    3: 0.90,  # March    — spring pickup
    4: 0.95,  # April
    5: 1.00,  # May
    6: 1.05,  # June     — summer
    7: 1.02,  # July
    8: 0.98,  # August   — back to school
    9: 1.00,  # September
    10: 1.10,  # October  — pre-holiday
    11: 1.30,  # November — Black Friday
    12: 1.40,  # December — holiday peak
}

# ── Day-of-Week Multipliers ──
DAY_OF_WEEK_MULTIPLIERS = {
    1: 1.10,  # Monday
    2: 1.05,  # Tuesday
    3: 1.00,  # Wednesday
    4: 1.00,  # Thursday
    5: 1.15,  # Friday  — highest
    6: 0.85,  # Saturday
    7: 0.75,  # Sunday  — lowest
}

# ── Category-Specific Seasonal Boosts ──
CATEGORY_SEASONAL_BOOSTS = {
    "Electronics": {10: 1.3, 11: 1.6, 12: 1.8},  # Strong Q4 spike
    "Toys": {10: 1.4, 11: 1.7, 12: 2.0},  # Biggest holiday boost
    "Apparel": {3: 1.3, 4: 1.2, 9: 1.3},  # Spring + back-to-school
    "Grocery": {11: 1.2, 12: 1.1},  # Mild holiday bump
    "Home & Garden": {4: 1.4, 5: 1.5, 6: 1.3},  # Spring/summer
    "Health": {1: 1.4, 2: 1.2},  # New year resolutions
    "Sports": {5: 1.3, 6: 1.4, 7: 1.3},  # Summer
    "Beauty": {2: 1.3, 11: 1.2, 12: 1.3},  # Valentine's + holidays
}

# ── Year-over-Year Demand Drift ──
# Categories that are growing faster than average
CATEGORY_GROWTH_MODIFIERS = {
    "Electronics": 1.04,  # +4% extra growth per year (tech adoption)
    "Health": 1.03,  # +3% extra (health consciousness trend)
    "Beauty": 1.02,  # +2% extra (e-commerce beauty growth)
    "Grocery": 0.98,  # -2% (grocery e-commerce growing slower)
    "Apparel": 1.00,  # baseline
    "Home & Garden": 1.01,
    "Toys": 1.00,
    "Sports": 1.01,
}

HOLIDAY_MULTIPLIER = 1.25

# ── Noise Configuration ──
# Larger noise creates more realistic day-to-day fluctuation
# Instead of smooth flat lines, charts show realistic jaggedness
DAILY_NOISE_MEAN = 1.0
DAILY_NOISE_STD = 0.18  # ±18% daily noise (was ±10% — much more visible variation)
DAILY_NOISE_MIN = 0.65  # Can drop to 65% on slow days
DAILY_NOISE_MAX = 1.45  # Can spike to 145% on busy days


def get_demand_multiplier(current_date: date, category: str = None, rng: np.random.Generator = None) -> float:
    """
    Calculate total demand multiplier for a given date and category.
    Includes: monthly + day-of-week + holiday + category seasonal + YoY growth + noise.
    """
    month = current_date.month
    dow = current_date.isoweekday()
    year = current_date.year

    # Base monthly seasonality
    multiplier = MONTHLY_SEASONALITY.get(month, 1.0)

    # Day-of-week effect
    multiplier *= DAY_OF_WEEK_MULTIPLIERS.get(dow, 1.0)

    # Holiday effect
    is_holiday = (month, current_date.day) in {
        (1, 1),
        (7, 4),
        (12, 25),
        (11, 24),
        (11, 25),
        (11, 26),
        (11, 27),
    }
    if is_holiday:
        multiplier *= HOLIDAY_MULTIPLIER

    # Category seasonal boost
    if category and category in CATEGORY_SEASONAL_BOOSTS:
        category_boost = CATEGORY_SEASONAL_BOOSTS[category].get(month, 1.0)
        multiplier *= category_boost

    # Year-over-year growth compounded from BASE_YEAR
    yoy_growth = get_year_growth_multiplier(year)
    multiplier *= yoy_growth

    # Category-specific growth drift
    if category and category in CATEGORY_GROWTH_MODIFIERS:
        years_elapsed = max(0, year - BASE_YEAR)
        cat_growth = CATEGORY_GROWTH_MODIFIERS[category] ** years_elapsed
        multiplier *= cat_growth

    # Daily noise — larger range creates visible jaggedness in charts
    if rng is not None:
        noise = rng.normal(DAILY_NOISE_MEAN, DAILY_NOISE_STD)
        noise = max(DAILY_NOISE_MIN, min(DAILY_NOISE_MAX, noise))
        multiplier *= noise

    return multiplier


def get_daily_order_count(base_count: int, current_date: date, rng: np.random.Generator) -> int:
    """
    Calculate number of orders for a given day.
    Applies seasonality + YoY growth + larger noise + occasional shock events.
    """
    multiplier = get_demand_multiplier(current_date, rng=rng)

    # Shock event — random disruption ~3% of days
    shock_multiplier = _get_shock_multiplier(current_date, rng)
    multiplier *= shock_multiplier

    count = int(base_count * multiplier)
    return max(1, count)


def get_product_demand(base_demand: float, current_date: date, category: str, rng: np.random.Generator) -> int:
    """
    Calculate demand for a specific product on a given day.
    Applies category weights + seasonal boosts + YoY growth + noise.
    """
    multiplier = get_demand_multiplier(current_date, category=category, rng=rng)

    # Additional product-level noise (each product varies independently)
    product_noise = rng.normal(1.0, 0.20)
    product_noise = max(0.3, min(2.0, product_noise))
    multiplier *= product_noise

    demand = int(base_demand * multiplier)
    return max(0, demand)


def _get_shock_multiplier(current_date: date, rng: np.random.Generator) -> float:
    """
    Randomly apply a shock event with SHOCK_EVENT_PROBABILITY.
    Returns 1.0 (no shock) most days, or a shock multiplier occasionally.
    This creates visible outliers/spikes in the time series charts.
    """
    if rng.random() > SHOCK_EVENT_PROBABILITY:
        return 1.0

    # Pick a random shock event weighted by probability
    event_probs = [e["probability"] for e in SHOCK_EVENTS]
    total = sum(event_probs)
    normalized = [p / total for p in event_probs]
    idx = int(rng.choice(len(SHOCK_EVENTS), p=normalized))
    event = SHOCK_EVENTS[idx]

    return float(event["multiplier"])


def get_warehouse_order_share(warehouse_id: str, rng: np.random.Generator) -> float:
    """
    Return demand weight for a specific warehouse with small random variation.
    LA-West and NYC-East dominate; DEN-Mountain and SEA-NW are smaller.
    """
    from config.constants import WAREHOUSE_DEMAND_WEIGHTS

    base_weight = WAREHOUSE_DEMAND_WEIGHTS.get(warehouse_id, 1.0)
    # Small per-day variation so warehouse shares aren't exactly the same every day
    noise = rng.normal(1.0, 0.05)
    noise = max(0.9, min(1.1, noise))
    return base_weight * noise
