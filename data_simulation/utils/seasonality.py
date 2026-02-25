# data_simulation/utils/seasonality.py
"""
Seasonality and demand pattern utilities.
Controls how demand varies by month, day-of-week, holidays, and category.
"""

import numpy as np
from datetime import date

# Monthly seasonality multipliers (1.0 = average)
# Higher in Nov-Dec (holiday shopping), lower in Jan-Feb (post-holiday)
MONTHLY_SEASONALITY = {
    1: 0.80,   # January - post-holiday dip
    2: 0.82,   # February
    3: 0.90,   # March - spring pickup
    4: 0.95,   # April
    5: 1.00,   # May
    6: 1.05,   # June - summer
    7: 1.02,   # July
    8: 0.98,   # August - back to school
    9: 1.00,   # September
    10: 1.10,  # October - pre-holiday
    11: 1.30,  # November - Black Friday
    12: 1.40,  # December - holiday peak
}

# Day-of-week multipliers (Monday=1, Sunday=7)
DAY_OF_WEEK_MULTIPLIERS = {
    1: 1.10,  # Monday - high
    2: 1.05,  # Tuesday
    3: 1.00,  # Wednesday
    4: 1.00,  # Thursday
    5: 1.15,  # Friday - highest
    6: 0.85,  # Saturday - lower
    7: 0.75,  # Sunday - lowest
}

# Category-specific seasonal boosts (on top of base seasonality)
CATEGORY_SEASONAL_BOOSTS = {
    "Electronics": {11: 1.4, 12: 1.6},    # Big holiday boost
    "Toys": {11: 1.5, 12: 1.8},           # Biggest holiday boost
    "Apparel": {3: 1.2, 4: 1.2, 9: 1.3},  # Spring + back-to-school
    "Grocery": {},                          # Relatively stable
    "Home & Garden": {4: 1.3, 5: 1.4, 6: 1.3},  # Spring/summer
    "Health": {1: 1.3, 2: 1.2},           # New year resolutions
    "Sports": {5: 1.2, 6: 1.3, 7: 1.3},   # Summer
    "Beauty": {2: 1.2, 12: 1.3},          # Valentine's + holidays
}

# Holiday demand spike
HOLIDAY_MULTIPLIER = 1.25


def get_demand_multiplier(current_date: date, category: str = None) -> float:
    """
    Calculate the total demand multiplier for a given date and optional category.
    Combines monthly, day-of-week, holiday, and category-specific seasonality.
    
    Returns a float multiplier (1.0 = normal demand).
    """
    month = current_date.month
    dow = current_date.isoweekday()  # 1=Monday, 7=Sunday
    
    # Base monthly seasonality
    multiplier = MONTHLY_SEASONALITY.get(month, 1.0)
    
    # Day-of-week effect
    multiplier *= DAY_OF_WEEK_MULTIPLIERS.get(dow, 1.0)
    
    # Holiday effect
    is_holiday = (month, current_date.day) in {
        (1, 1), (7, 4), (12, 25), (11, 24), (11, 25),
        (11, 26), (11, 27),  # Black Friday weekend
    }
    if is_holiday:
        multiplier *= HOLIDAY_MULTIPLIER
    
    # Category-specific seasonal boost
    if category and category in CATEGORY_SEASONAL_BOOSTS:
        category_boost = CATEGORY_SEASONAL_BOOSTS[category].get(month, 1.0)
        multiplier *= category_boost
    
    return multiplier


def get_daily_order_count(base_count: int, current_date: date, rng: np.random.Generator) -> int:
    """
    Calculate the number of orders for a given day.
    Applies seasonality + random noise.
    """
    multiplier = get_demand_multiplier(current_date)
    
    # Add random noise (±10%)
    noise = rng.normal(1.0, 0.10)
    noise = max(0.8, min(1.2, noise))  # Clamp
    
    count = int(base_count * multiplier * noise)
    return max(1, count)


def get_product_demand(base_demand: float, current_date: date, category: str,
                       rng: np.random.Generator) -> int:
    """
    Calculate demand for a specific product on a given day.
    """
    multiplier = get_demand_multiplier(current_date, category)
    noise = rng.normal(1.0, 0.15)
    noise = max(0.5, min(1.5, noise))
    
    demand = int(base_demand * multiplier * noise)
    return max(0, demand)