"""
Dimension table generators.
Generates: dim_product, dim_warehouse, dim_supplier, dim_driver, 
           dim_customer, dim_date, dim_experiments
"""
#core/dimensions.py

import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta

from config.constants import (
    RANDOM_SEED, NUM_PRODUCTS, NUM_CUSTOMERS, NUM_DRIVERS,
    PRODUCT_CATEGORIES, PRODUCTS_PER_CATEGORY,
    CATEGORY_PRICE_RANGES, MARKUP_RANGE, CATEGORY_WEIGHT_RANGES,
    CATEGORY_LEAD_TIME_RANGES, PERISHABLE_CATEGORIES,
    CUSTOMER_SEGMENTS, SEGMENT_FREQUENCY_RANGES,
    VEHICLE_TYPES, DRIVER_STATUS_DISTRIBUTION,
    SUPPLIER_CONFIGS, EXPERIMENT_CONFIGS,
    REORDER_POINT_RANGE, SAFETY_STOCK_RANGE,
    BACKFILL_START_DATE, BACKFILL_END_DATE, US_HOLIDAYS,
)
from config.warehouse_config import (
    WAREHOUSES, WAREHOUSE_IDS, DRIVERS_PER_WAREHOUSE,
)
from data_simulation.utils.geo import generate_customer_location, REGIONAL_CITIES


def generate_dim_product(rng: np.random.Generator) -> pd.DataFrame:
    """Generate dim_product: 500 products across 8 categories."""
    rows = []
    product_counter = 1
    
    for category, count in PRODUCTS_PER_CATEGORY.items():
        subcategories = PRODUCT_CATEGORIES[category]
        price_min, price_max = CATEGORY_PRICE_RANGES[category]
        weight_min, weight_max = CATEGORY_WEIGHT_RANGES[category]
        lead_min, lead_max = CATEGORY_LEAD_TIME_RANGES[category]
        
        for i in range(count):
            product_id = f"PROD-{product_counter:04d}"
            subcategory = rng.choice(subcategories)
            
            cost_price = round(rng.uniform(price_min, price_max), 2)
            markup = rng.uniform(*MARKUP_RANGE)
            selling_price = round(cost_price * markup, 2)
            
            weight_kg = round(rng.uniform(weight_min, weight_max), 2)
            lead_time_days = int(rng.integers(lead_min, lead_max + 1))
            reorder_point = int(rng.integers(*REORDER_POINT_RANGE))
            safety_stock = int(rng.integers(*SAFETY_STOCK_RANGE))
            is_perishable = category in PERISHABLE_CATEGORIES
            
            rows.append({
                "product_id": product_id,
                "product_name": f"{subcategory} {category[0]}-{product_counter:03d}",
                "category": category,
                "subcategory": subcategory,
                "cost_price": cost_price,
                "selling_price": selling_price,
                "weight_kg": weight_kg,
                "lead_time_days": lead_time_days,
                "reorder_point": reorder_point,
                "safety_stock": safety_stock,
                "is_perishable": is_perishable,
                "created_at": datetime(2022, 1, 1),
            })
            product_counter += 1
    
    return pd.DataFrame(rows)


def generate_dim_warehouse() -> pd.DataFrame:
    """Generate dim_warehouse: 8 warehouses."""
    rows = []
    for w in WAREHOUSES:
        rows.append({
            "warehouse_id": w["warehouse_id"],
            "warehouse_name": w["warehouse_name"],
            "region": w["region"],
            "city": w["city"],
            "state": w["state"],
            "latitude": w["latitude"],
            "longitude": w["longitude"],
            "capacity_units": w["capacity_units"],
            "operating_cost_per_day": w["operating_cost_per_day"],
            "is_active": True,
        })
    return pd.DataFrame(rows)


def generate_dim_supplier() -> pd.DataFrame:
    """Generate dim_supplier: 6 suppliers."""
    rows = []
    for i, s in enumerate(SUPPLIER_CONFIGS):
        rows.append({
            "supplier_id": f"SUP-{i+1:03d}",
            "supplier_name": s["name"],
            "region": s["region"],
            "average_lead_time": s["lead_time"],
            "lead_time_std_dev": s["std_dev"],
            "reliability_score": s["reliability"],
            "product_categories": s["categories"],
        })
    return pd.DataFrame(rows)


def generate_dim_driver(rng: np.random.Generator) -> pd.DataFrame:
    """Generate dim_driver: ~300 drivers across 8 warehouses."""
    rows = []
    driver_counter = 1
    
    # Build vehicle type choices and their probabilities
    v_types = list(VEHICLE_TYPES.keys())
    v_probs = [VEHICLE_TYPES[v]["proportion"] for v in v_types]
    
    # Build status choices
    statuses = list(DRIVER_STATUS_DISTRIBUTION.keys())
    status_probs = list(DRIVER_STATUS_DISTRIBUTION.values())
    
    for wh_id in WAREHOUSE_IDS:
        num_drivers = DRIVERS_PER_WAREHOUSE[wh_id]
        
        for _ in range(num_drivers):
            driver_id = f"DRV-{driver_counter:04d}"
            vehicle = rng.choice(v_types, p=v_probs)
            
            cap_min, cap_max = VEHICLE_TYPES[vehicle]["capacity"]
            spd_min, spd_max = VEHICLE_TYPES[vehicle]["speed"]
            
            status = rng.choice(statuses, p=status_probs)
            
            # Hire date: between 2019 and 2023
            days_offset = int(rng.integers(0, 365 * 4))
            hire_date = date(2019, 1, 1) + timedelta(days=days_offset)
            
            first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer",
                          "Michael", "Linda", "David", "Elizabeth", "William", "Barbara",
                          "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah",
                          "Carlos", "Maria", "Wei", "Li", "Ahmed", "Fatima",
                          "Raj", "Priya", "Kenji", "Yuki", "Olga", "Ivan"]
            last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
                         "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez",
                         "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson",
                         "Lee", "Kim", "Chen", "Wang", "Patel", "Singh",
                         "Tanaka", "Sato", "Petrov", "Ivanov", "Park", "Nguyen"]
            
            driver_name = f"{rng.choice(first_names)} {rng.choice(last_names)}"
            
            rows.append({
                "driver_id": driver_id,
                "warehouse_id": wh_id,
                "driver_name": driver_name,
                "vehicle_type": vehicle,
                "max_delivery_capacity": int(rng.integers(cap_min, cap_max + 1)),
                "avg_speed_kmh": round(rng.uniform(spd_min, spd_max), 2),
                "availability_status": status,
                "hire_date": hire_date,
            })
            driver_counter += 1
    
    return pd.DataFrame(rows)


def generate_dim_customer(rng: np.random.Generator) -> pd.DataFrame:
    """Generate dim_customer: ~10,000 customers."""
    rows = []
    
    segments = list(CUSTOMER_SEGMENTS.keys())
    segment_probs = list(CUSTOMER_SEGMENTS.values())
    
    for i in range(NUM_CUSTOMERS):
        customer_id = f"CUST-{i+1:05d}"
        
        # Assign customer to a region (warehouse)
        assigned_wh = rng.choice(WAREHOUSE_IDS)
        region = next(w["region"] for w in WAREHOUSES if w["warehouse_id"] == assigned_wh)
        city = rng.choice(REGIONAL_CITIES[assigned_wh])
        
        # Customer segment
        segment = rng.choice(segments, p=segment_probs)
        freq_min, freq_max = SEGMENT_FREQUENCY_RANGES[segment]
        frequency_score = round(rng.uniform(freq_min, freq_max), 2)
        
        # Acquisition date: between 2020 and 2024
        days_offset = int(rng.integers(0, 365 * 4))
        acquisition_date = date(2020, 1, 1) + timedelta(days=days_offset)
        
        # Location near assigned warehouse
        lat, lon = generate_customer_location(assigned_wh, rng)
        
        rows.append({
            "customer_id": customer_id,
            "region": region,
            "city": city,
            "customer_segment": segment,
            "order_frequency_score": frequency_score,
            "acquisition_date": acquisition_date,
            "latitude": lat,
            "longitude": lon,
        })
    
    return pd.DataFrame(rows)


def generate_dim_date() -> pd.DataFrame:
    """Generate dim_date: one row per day for the backfill period."""
    rows = []
    current = BACKFILL_START_DATE
    end = BACKFILL_END_DATE
    
    while current <= end:
        month = current.month
        
        # Determine season
        if month in (3, 4, 5):
            season = "Spring"
        elif month in (6, 7, 8):
            season = "Summer"
        elif month in (9, 10, 11):
            season = "Fall"
        else:
            season = "Winter"
        
        # Check holiday
        is_holiday = (month, current.day) in US_HOLIDAYS
        
        rows.append({
            "date": current,
            "day_of_week": current.strftime("%A"),
            "day_of_week_num": current.isoweekday(),
            "week_number": current.isocalendar()[1],
            "month": month,
            "month_name": current.strftime("%B"),
            "quarter": (month - 1) // 3 + 1,
            "year": current.year,
            "is_holiday": is_holiday,
            "is_weekend": current.isoweekday() >= 6,
            "season": season,
        })
        current += timedelta(days=1)
    
    return pd.DataFrame(rows)


def generate_dim_experiments() -> pd.DataFrame:
    """Generate dim_experiments: ~10 experiments."""
    rows = []
    
    # Spread experiments across the 3-year period
    experiment_starts = [
        date(2022, 6, 1), date(2022, 9, 1), date(2023, 1, 1),
        date(2023, 4, 1), date(2023, 7, 1), date(2023, 10, 1),
        date(2024, 1, 1), date(2024, 4, 1), date(2024, 7, 1),
        date(2024, 10, 1),
    ]
    
    for i, config in enumerate(EXPERIMENT_CONFIGS):
        start = experiment_starts[i]
        # Most experiments run 90 days, last 2 are still active
        if i < 8:
            end = start + timedelta(days=90)
            status = "Completed"
        else:
            end = None
            status = "Active"
        
        rows.append({
            "experiment_id": f"EXP-{i+1:03d}",
            "experiment_name": config["name"],
            "strategy_name": config["strategy"],
            "experiment_type": config["type"],
            "description": f"Testing {config['strategy']} strategy across target warehouses.",
            "start_date": start,
            "end_date": end,
            "target_warehouses": config["warehouses"],
            "status": status,
        })
    
    return pd.DataFrame(rows)