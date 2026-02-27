"""
Feature engineering for Stockout Risk Prediction.
Predicts whether a product at a warehouse will stockout in the next 3 days.

Target: will_stockout_3d (binary: 1 = stockout within 3 days, 0 = no stockout)
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


def build_stockout_features(inventory: pd.DataFrame, dates: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    """
    Build features for stockout risk prediction.
    
    Args:
        inventory: int_inventory_enriched data
        dates: stg_dates data
        products: stg_products data (current only)
    
    Returns:
        DataFrame with features + binary target ready for training
    """
    df = inventory.copy()
    
    # ── Force numeric types for all columns that must be numeric ──
    numeric_cols = [
        'opening_stock', 'units_sold', 'units_received', 'units_returned',
        'closing_stock', 'units_on_order', 'days_of_supply', 'holding_cost',
        'inventory_value', 'net_stock_movement', 'product_capacity_pct',
        'revenue_at_risk', 'safety_stock', 'reorder_point',
        'cost_price', 'selling_price', 'lead_time_days',
    ]
    df = _safe_numeric(df, numeric_cols)
    
    # ── Merge date features ──
    df = df.merge(
        dates[['date', 'day_of_week_num', 'month', 'quarter', 'is_holiday', 'is_weekend', 'season']],
        left_on='snapshot_date', right_on='date',
        how='left'
    )
    
    # Sort for lag calculations
    df = df.sort_values(['warehouse_id', 'product_id', 'snapshot_date']).reset_index(drop=True)
    
    # ── Create Target: will stockout in next 3 days ──
    df['stockout_in_1d'] = df.groupby(['warehouse_id', 'product_id'])['stockout_flag'].shift(-1)
    df['stockout_in_2d'] = df.groupby(['warehouse_id', 'product_id'])['stockout_flag'].shift(-2)
    df['stockout_in_3d'] = df.groupby(['warehouse_id', 'product_id'])['stockout_flag'].shift(-3)
    
    df['will_stockout_3d'] = (
        (df['stockout_in_1d'] == True) |
        (df['stockout_in_2d'] == True) |
        (df['stockout_in_3d'] == True)
    ).astype(int)
    
    # Drop the helper columns
    df = df.drop(columns=['stockout_in_1d', 'stockout_in_2d', 'stockout_in_3d'])
    
    # ── Current State Features ──
    df['stock_to_safety_ratio'] = df['closing_stock'] / df['safety_stock'].replace(0, np.nan)
    df['stock_to_reorder_ratio'] = df['closing_stock'] / df['reorder_point'].replace(0, np.nan)
    
    # ── Lag Features ──
    for lag in [1, 3, 7]:
        df[f'closing_stock_lag_{lag}d'] = (
            df.groupby(['warehouse_id', 'product_id'])['closing_stock'].shift(lag)
        )
        df[f'units_sold_lag_{lag}d'] = (
            df.groupby(['warehouse_id', 'product_id'])['units_sold'].shift(lag)
        )
    
    # ── Rolling Features ──
    for window in [7, 14]:
        df[f'demand_rolling_avg_{window}d'] = (
            df.groupby(['warehouse_id', 'product_id'])['units_sold']
            .transform(lambda x: x.rolling(window, min_periods=1).mean())
        )
        df[f'demand_rolling_std_{window}d'] = (
            df.groupby(['warehouse_id', 'product_id'])['units_sold']
            .transform(lambda x: x.rolling(window, min_periods=1).std())
        )
    
    # ── Stock Depletion Rate ──
    df['stock_depletion_rate'] = df['units_sold'] / df['closing_stock'].replace(0, np.nan)
    df['stock_depletion_rate'] = df['stock_depletion_rate'].clip(0, 10)
    
    # ── Days until stockout estimate ──
    df['est_days_until_stockout'] = df['closing_stock'] / df['demand_rolling_avg_7d'].replace(0, np.nan)
    df['est_days_until_stockout'] = df['est_days_until_stockout'].clip(0, 99)
    
    # ── Replenishment signals ──
    df['has_pending_order'] = (df['units_on_order'] > 0).astype(int)
    df['reorder_triggered_today'] = df['reorder_triggered_flag'].astype(int)
    
    # ── Historical stockout frequency (last 30 days) ──
    df['stockout_count_30d'] = (
        df.groupby(['warehouse_id', 'product_id'])['stockout_flag']
        .transform(lambda x: x.rolling(30, min_periods=1).sum())
    )
    
    # ── Encode categoricals ──
    df['season_encoded'] = df['season'].map({
        'Winter': 0, 'Spring': 1, 'Summer': 2, 'Fall': 3
    }).fillna(0)
    
    df['category_encoded'] = pd.Categorical(df['category']).codes
    
    df['is_holiday'] = df['is_holiday'].astype(int)
    df['is_weekend'] = df['is_weekend'].astype(int)
    
    # ── Cast boolean flags to int ──
    for col in ['stockout_flag', 'below_safety_stock_flag', 'reorder_triggered_flag']:
        if col in df.columns:
            df[col] = df[col].astype(int)
    
    # ── Drop rows with NaN from lags ──
    df = df.dropna(subset=['closing_stock_lag_7d', 'will_stockout_3d'])
    
    return df


def get_feature_columns() -> list:
    """Return the list of feature column names for stockout model."""
    return [
        # Current state
        'closing_stock', 'opening_stock', 'units_sold', 'units_received',
        'days_of_supply', 'holding_cost',
        'stock_to_safety_ratio', 'stock_to_reorder_ratio',
        'below_safety_stock_flag',
        # Lags
        'closing_stock_lag_1d', 'closing_stock_lag_3d', 'closing_stock_lag_7d',
        'units_sold_lag_1d', 'units_sold_lag_3d', 'units_sold_lag_7d',
        # Rolling
        'demand_rolling_avg_7d', 'demand_rolling_avg_14d',
        'demand_rolling_std_7d', 'demand_rolling_std_14d',
        # Depletion
        'stock_depletion_rate', 'est_days_until_stockout',
        # Replenishment
        'has_pending_order', 'units_on_order', 'reorder_triggered_today',
        # Historical
        'stockout_count_30d',
        # Date
        'day_of_week_num', 'month', 'quarter', 'is_holiday', 'is_weekend', 'season_encoded',
        # Product
        'category_encoded',
    ]


def get_target_column() -> str:
    """Return the target column name."""
    return 'will_stockout_3d'


def train_test_split_temporal(
    df: pd.DataFrame,
    train_end: str = '2024-06-30',
    val_end: str = '2024-10-31'
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Temporal split for stockout prediction."""
    df['snapshot_date'] = pd.to_datetime(df['snapshot_date'])
    train = df[df['snapshot_date'] <= train_end]
    val = df[(df['snapshot_date'] > train_end) & (df['snapshot_date'] <= val_end)]
    test = df[df['snapshot_date'] > val_end]
    
    # Print class balance
    for name, split in [('Train', train), ('Val', val), ('Test', test)]:
        pos = split['will_stockout_3d'].sum()
        total = len(split)
        print(f"{name}: {total:,} rows | Stockout: {pos:,} ({pos/total*100:.1f}%)")
    
    return train, val, test