"""
Feature engineering for Demand Forecasting.
Pulls data from Snowflake marts, builds lag features, rolling averages,
seasonality indicators, and category-level aggregates.

Target: total_units_sold (per product per day)
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


def build_demand_features(product_kpis: pd.DataFrame, dates: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    """
    Build features for demand forecasting from mart and staging data.
    
    Args:
        product_kpis: mart_daily_product_kpis data
        dates: stg_dates data
        products: stg_products data (current only)
    
    Returns:
        DataFrame with features + target ready for training
    """
    df = product_kpis.copy()
    
    # Drop columns from product_kpis that also exist in products to avoid
    # duplicate suffixes (_x, _y) after merge. Keep them from products instead.
    overlap_cols = ['category', 'price_tier']
    df = df.drop(columns=[c for c in overlap_cols if c in df.columns], errors='ignore')
    
    # ── Force numeric types for all columns that must be numeric ──
    numeric_cols = [
        'total_units_sold', 'total_revenue', 'stockout_count', 'avg_closing_stock',
        'inventory_turnover', 'avg_days_of_supply', 'total_holding_cost',
        'total_inventory_value', 'demand_forecast', 'forecast_error', 'demand_volatility',
        'cost_price', 'selling_price', 'weight_kg',
    ]
    df = _safe_numeric(df, numeric_cols)
    
    # ── Merge date features ──
    df = df.merge(
        dates[['date', 'day_of_week_num', 'month', 'quarter', 'year',
               'is_holiday', 'is_weekend', 'season']],
        on='date',
        how='left'
    )
    
    # ── Merge product features ──
    df = df.merge(
        products[['product_id', 'category', 'subcategory', 'cost_price',
                  'selling_price', 'weight_kg', 'is_perishable']],
        on='product_id',
        how='left'
    )
    
    # Sort for lag calculations
    df = df.sort_values(['product_id', 'date']).reset_index(drop=True)
    
    # ── Lag Features (previous days' demand) ──
    for lag in [1, 3, 7, 14, 28]:
        df[f'demand_lag_{lag}d'] = df.groupby('product_id')['total_units_sold'].shift(lag)
    
    # ── Rolling Average Features ──
    for window in [7, 14, 30]:
        df[f'demand_rolling_avg_{window}d'] = (
            df.groupby('product_id')['total_units_sold']
            .transform(lambda x: x.rolling(window, min_periods=1).mean())
        )
        df[f'demand_rolling_std_{window}d'] = (
            df.groupby('product_id')['total_units_sold']
            .transform(lambda x: x.rolling(window, min_periods=1).std())
        )
    
    # ── Rolling Min/Max (demand range) ──
    df['demand_rolling_min_7d'] = (
        df.groupby('product_id')['total_units_sold']
        .transform(lambda x: x.rolling(7, min_periods=1).min())
    )
    df['demand_rolling_max_7d'] = (
        df.groupby('product_id')['total_units_sold']
        .transform(lambda x: x.rolling(7, min_periods=1).max())
    )
    
    # ── Trend Features ──
    # Week-over-week change
    df['demand_wow_change'] = df.groupby('product_id')['total_units_sold'].shift(7)
    df['demand_wow_change'] = df['total_units_sold'] - df['demand_wow_change']
    
    # Month-over-month change
    df['demand_mom_change'] = df.groupby('product_id')['total_units_sold'].shift(30)
    df['demand_mom_change'] = df['total_units_sold'] - df['demand_mom_change']
    
    # ── Inventory Features ──
    df['stockout_lag_1d'] = df.groupby('product_id')['stockout_count'].shift(1)
    df['avg_stock_lag_1d'] = df.groupby('product_id')['avg_closing_stock'].shift(1)
    
    # ── Price Features ──
    df['price_ratio'] = df['selling_price'] / df['cost_price'].replace(0, np.nan)
    df['profit_margin'] = df['selling_price'] - df['cost_price']
    
    # ── Encode Categoricals ──
    df['season_encoded'] = df['season'].map({
        'Winter': 0, 'Spring': 1, 'Summer': 2, 'Fall': 3
    })
    
    # Category encoding (label encode)
    df['category_encoded'] = pd.Categorical(df['category']).codes
    
    # Boolean to int
    df['is_holiday'] = df['is_holiday'].astype(int)
    df['is_weekend'] = df['is_weekend'].astype(int)
    df['is_perishable'] = df['is_perishable'].astype(int)
    
    # ── Drop rows with NaN from lag features (first 28 days per product) ──
    df = df.dropna(subset=['demand_lag_28d'])
    
    return df


def get_feature_columns() -> list:
    """Return the list of feature column names for model training."""
    return [
        # Lag features
        'demand_lag_1d', 'demand_lag_3d', 'demand_lag_7d', 'demand_lag_14d', 'demand_lag_28d',
        # Rolling features
        'demand_rolling_avg_7d', 'demand_rolling_avg_14d', 'demand_rolling_avg_30d',
        'demand_rolling_std_7d', 'demand_rolling_std_14d', 'demand_rolling_std_30d',
        'demand_rolling_min_7d', 'demand_rolling_max_7d',
        # Trend
        'demand_wow_change', 'demand_mom_change',
        # Inventory
        'stockout_count', 'stockout_lag_1d', 'avg_closing_stock', 'avg_stock_lag_1d',
        'inventory_turnover',
        # Date
        'day_of_week_num', 'month', 'quarter', 'year',
        'is_holiday', 'is_weekend', 'season_encoded',
        # Product
        'category_encoded', 'cost_price', 'selling_price', 'weight_kg',
        'is_perishable', 'price_ratio', 'profit_margin',
    ]


def get_target_column() -> str:
    """Return the target column name."""
    return 'total_units_sold'


def train_test_split_temporal(
    df: pd.DataFrame,
    train_end: str = '2024-06-30',
    val_end: str = '2024-10-31'
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Split data temporally (not random) for time-series.
    Train: start → train_end
    Validation: train_end → val_end
    Test: val_end → end
    """
    df['date'] = pd.to_datetime(df['date'])
    train = df[df['date'] <= train_end]
    val = df[(df['date'] > train_end) & (df['date'] <= val_end)]
    test = df[df['date'] > val_end]
    
    print(f"Train: {len(train):,} rows ({train['date'].min()} to {train['date'].max()})")
    print(f"Val:   {len(val):,} rows ({val['date'].min()} to {val['date'].max()})")
    print(f"Test:  {len(test):,} rows ({test['date'].min()} to {test['date'].max()})")
    
    return train, val, test