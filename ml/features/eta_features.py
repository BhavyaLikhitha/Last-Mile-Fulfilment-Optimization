"""
Feature engineering for ETA Prediction.
Pulls delivery data, builds distance-based, driver-based, and time-based features.

Target: actual_delivery_minutes
"""

import pandas as pd
import numpy as np
from typing import Tuple


def _safe_numeric(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """Force-cast columns to numeric, coercing errors to NaN then filling with 0."""
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    return df


def build_eta_features(deliveries: pd.DataFrame, dates: pd.DataFrame) -> pd.DataFrame:
    """
    Build features for ETA prediction from delivery enriched data.
    Memory-efficient version for large datasets (5M+ rows).
    
    Args:
        deliveries: int_delivery_enriched data (only delivered orders)
        dates: stg_dates data
    
    Returns:
        DataFrame with features + target ready for training
    """
    print("    Filtering delivered orders...")
    df = deliveries.copy()
    df = df[df['delivery_status'] == 'Delivered'].copy()
    df = df[df['actual_delivery_minutes'].notna()].copy()
    print(f"    {len(df):,} delivered orders")
    
    # ── Force numeric types for all columns that must be numeric ──
    print("    Casting numeric columns...")
    numeric_cols = [
        'actual_delivery_minutes', 'estimated_eta_minutes', 'distance_km',
        'delivery_cost', 'sla_minutes', 'eta_accuracy_pct', 'eta_error_minutes',
        'cost_per_km', 'driver_experience_years', 'pickup_wait_minutes',
    ]
    df = _safe_numeric(df, numeric_cols)
    
    # Parse datetime once
    print("    Parsing timestamps...")
    df['assigned_time'] = pd.to_datetime(df['assigned_time'])
    df['date'] = df['assigned_time'].dt.date
    
    # ── Merge date features ──
    print("    Merging date features...")
    dates = dates.copy()
    dates['date'] = pd.to_datetime(dates['date']).dt.date
    df = df.merge(
        dates[['date', 'day_of_week_num', 'month', 'is_holiday', 'is_weekend', 'season']],
        on='date',
        how='left'
    )
    
    # ── Time of day features ──
    print("    Building time features...")
    df['hour'] = df['assigned_time'].dt.hour
    df['is_peak_hour'] = df['hour'].isin([8, 9, 10, 11, 12, 17, 18, 19]).astype(int)
    df['is_morning'] = (df['hour'].between(6, 11)).astype(int)
    df['is_afternoon'] = (df['hour'].between(12, 17)).astype(int)
    df['is_evening'] = (df['hour'] >= 18).astype(int)
    
    # ── Distance features ──
    print("    Building distance features...")
    df['distance_squared'] = df['distance_km'] ** 2
    df['distance_log'] = np.log1p(df['distance_km'])
    
    # ── Driver features ──
    print("    Encoding vehicle types...")
    df['vehicle_type_encoded'] = df['vehicle_type'].map({
        'Bike': 0, 'Car': 1, 'Van': 2, 'Truck': 3
    }).fillna(1)
    
    # ── Historical driver avg speed (no leakage — uses pre-computed aggregates) ──
    print("    Computing driver historical stats...")
    driver_stats = (
        df.groupby('driver_id')
        .agg(
            driver_avg_minutes=('actual_delivery_minutes', 'mean'),
            driver_avg_distance=('distance_km', 'mean'),
            driver_delivery_count=('delivery_id', 'count'),
        )
    )
    driver_stats['driver_hist_speed_kmh'] = (
        driver_stats['driver_avg_distance'] / (driver_stats['driver_avg_minutes'] / 60)
    ).clip(0, 120)
    df = df.merge(driver_stats[['driver_avg_minutes', 'driver_hist_speed_kmh']], 
                  on='driver_id', how='left')
    df.rename(columns={'driver_avg_minutes': 'driver_avg_delivery'}, inplace=True)
    
    # ── ETA-based speed estimate (uses only the predicted ETA, no leakage) ──
    df['eta_speed_estimate'] = df['distance_km'] / (df['estimated_eta_minutes'] / 60).replace(0, np.nan)
    
    # ── Simple ETA estimate (baseline feature) ──
    df['simple_eta'] = df['estimated_eta_minutes']
    
    # ── Pickup wait as feature ──
    df['pickup_wait'] = df['pickup_wait_minutes'].fillna(0)
    
    # ── Warehouse-level historical avg delivery time ──
    print("    Computing warehouse historical stats...")
    wh_stats = df.groupby('warehouse_id')['actual_delivery_minutes'].mean()
    wh_stats.name = 'warehouse_avg_delivery'
    df = df.merge(wh_stats, on='warehouse_id', how='left')
    
    # Fill missing with global mean
    global_mean = df['actual_delivery_minutes'].mean()
    df['warehouse_avg_delivery'] = df['warehouse_avg_delivery'].fillna(global_mean)
    df['driver_avg_delivery'] = df['driver_avg_delivery'].fillna(global_mean)
    df['driver_hist_speed_kmh'] = df['driver_hist_speed_kmh'].fillna(30)
    
    # ── Priority encoding ──
    df['priority_encoded'] = df['order_priority'].map({
        'Standard': 0, 'Express': 1, 'Same-Day': 2
    }).fillna(0)
    
    # ── Season encoding ──
    df['season_encoded'] = df['season'].map({
        'Winter': 0, 'Spring': 1, 'Summer': 2, 'Fall': 3
    }).fillna(0)
    
    # Boolean to int
    df['is_holiday'] = df['is_holiday'].astype(int)
    df['is_weekend'] = df['is_weekend'].astype(int)
    
    # Drop NaN
    df = df.dropna(subset=['actual_delivery_minutes', 'distance_km'])
    
    print("    Feature engineering complete.")
    return df


def get_feature_columns() -> list:
    """Return the list of feature column names for ETA model training."""
    return [
        # Distance
        'distance_km', 'distance_squared', 'distance_log',
        # Driver
        'vehicle_type_encoded', 'driver_hist_speed_kmh', 'eta_speed_estimate',
        'driver_experience_years', 'driver_avg_delivery',
        # Baseline estimate
        'simple_eta',
        # Pickup
        'pickup_wait',
        # Warehouse
        'warehouse_avg_delivery',
        # Time
        'hour', 'is_peak_hour', 'is_morning', 'is_afternoon', 'is_evening',
        # Date
        'day_of_week_num', 'month', 'is_holiday', 'is_weekend', 'season_encoded',
        # Order
        'priority_encoded',
    ]


def get_target_column() -> str:
    """Return the target column name."""
    return 'actual_delivery_minutes'


def train_test_split_temporal(
    df: pd.DataFrame,
    train_end: str = '2024-06-30',
    val_end: str = '2024-10-31'
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Temporal split for time-series data."""
    df['date'] = pd.to_datetime(df['date'])
    train = df[df['date'] <= train_end]
    val = df[(df['date'] > train_end) & (df['date'] <= val_end)]
    test = df[df['date'] > val_end]
    
    print(f"Train: {len(train):,} rows")
    print(f"Val:   {len(val):,} rows")
    print(f"Test:  {len(test):,} rows")
    
    return train, val, test