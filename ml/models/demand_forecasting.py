"""
Demand Forecasting Model Comparison.
Trains: SARIMA, Prophet, XGBoost, LightGBM
Compares on: MAPE, RMSE, MAE
Selects best model for production predictions.
"""

import numpy as np
import pandas as pd
from typing import Dict, Tuple
import warnings
warnings.filterwarnings('ignore')

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
import xgboost as xgb
import lightgbm as lgb


def mean_absolute_percentage_error(y_true, y_pred):
    """Calculate MAPE, handling zero values."""
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    mask = y_true != 0
    return np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100


def train_xgboost(X_train, y_train, X_val, y_val) -> Tuple[object, Dict]:
    """Train XGBoost regressor."""
    model = xgb.XGBRegressor(
        n_estimators=500,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=5,
        reg_alpha=0.1,
        reg_lambda=1.0,
        random_state=42,
        n_jobs=-1,
    )
    
    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        verbose=False,
    )
    
    y_pred = model.predict(X_val)
    y_pred = np.maximum(y_pred, 0)  # Demand can't be negative
    
    metrics = {
        'MAPE': round(mean_absolute_percentage_error(y_val, y_pred), 2),
        'RMSE': round(np.sqrt(mean_squared_error(y_val, y_pred)), 2),
        'MAE': round(mean_absolute_error(y_val, y_pred), 2),
    }
    
    return model, metrics


def train_lightgbm(X_train, y_train, X_val, y_val) -> Tuple[object, Dict]:
    """Train LightGBM regressor."""
    model = lgb.LGBMRegressor(
        n_estimators=500,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_samples=20,
        reg_alpha=0.1,
        reg_lambda=1.0,
        random_state=42,
        n_jobs=-1,
        verbose=-1,
    )
    
    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
    )
    
    y_pred = model.predict(X_val)
    y_pred = np.maximum(y_pred, 0)
    
    metrics = {
        'MAPE': round(mean_absolute_percentage_error(y_val, y_pred), 2),
        'RMSE': round(np.sqrt(mean_squared_error(y_val, y_pred)), 2),
        'MAE': round(mean_absolute_error(y_val, y_pred), 2),
    }
    
    return model, metrics


def train_prophet(df_train: pd.DataFrame, df_val: pd.DataFrame, product_id: str = None) -> Tuple[object, Dict]:
    """
    Train Facebook Prophet for a single product or aggregated demand.
    Prophet expects columns: ds (date), y (target).
    """
    try:
        from prophet import Prophet
    except ImportError:
        print("Prophet not installed. Skipping.")
        return None, {'MAPE': None, 'RMSE': None, 'MAE': None}
    
    # Aggregate daily demand if not already
    train_ts = df_train.groupby('date')['total_units_sold'].sum().reset_index()
    train_ts.columns = ['ds', 'y']
    train_ts['ds'] = pd.to_datetime(train_ts['ds'])
    
    val_ts = df_val.groupby('date')['total_units_sold'].sum().reset_index()
    val_ts.columns = ['ds', 'y']
    val_ts['ds'] = pd.to_datetime(val_ts['ds'])
    
    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False,
        changepoint_prior_scale=0.05,
    )
    model.fit(train_ts)
    
    forecast = model.predict(val_ts[['ds']])
    y_pred = forecast['yhat'].values
    y_pred = np.maximum(y_pred, 0)
    y_true = val_ts['y'].values
    
    metrics = {
        'MAPE': round(mean_absolute_percentage_error(y_true, y_pred), 2),
        'RMSE': round(np.sqrt(mean_squared_error(y_true, y_pred)), 2),
        'MAE': round(mean_absolute_error(y_true, y_pred), 2),
    }
    
    return model, metrics


def train_sarima(df_train: pd.DataFrame, df_val: pd.DataFrame) -> Tuple[object, Dict]:
    """
    Train SARIMA model on aggregated daily demand.
    """
    try:
        from statsmodels.tsa.statespace.sarimax import SARIMAX
    except ImportError:
        print("Statsmodels not installed. Skipping SARIMA.")
        return None, {'MAPE': None, 'RMSE': None, 'MAE': None}
    
    # Aggregate daily demand
    train_ts = df_train.groupby('date')['total_units_sold'].sum()
    train_ts.index = pd.to_datetime(train_ts.index)
    train_ts = train_ts.asfreq('D', fill_value=0)
    
    val_ts = df_val.groupby('date')['total_units_sold'].sum()
    val_ts.index = pd.to_datetime(val_ts.index)
    val_ts = val_ts.asfreq('D', fill_value=0)
    
    # SARIMA(1,1,1)(1,1,1,7) - weekly seasonality
    model = SARIMAX(
        train_ts,
        order=(1, 1, 1),
        seasonal_order=(1, 1, 1, 7),
        enforce_stationarity=False,
        enforce_invertibility=False,
    )
    fitted = model.fit(disp=False, maxiter=200)
    
    # Forecast validation period
    forecast = fitted.forecast(steps=len(val_ts))
    y_pred = np.maximum(forecast.values, 0)
    y_true = val_ts.values
    
    metrics = {
        'MAPE': round(mean_absolute_percentage_error(y_true, y_pred), 2),
        'RMSE': round(np.sqrt(mean_squared_error(y_true, y_pred)), 2),
        'MAE': round(mean_absolute_error(y_true, y_pred), 2),
    }
    
    return fitted, metrics


def compare_models(results: Dict[str, Dict]) -> pd.DataFrame:
    """Create a comparison DataFrame from model results."""
    comparison = pd.DataFrame(results).T
    comparison.index.name = 'Model'
    comparison = comparison.sort_values('MAPE')
    
    print("\n" + "=" * 50)
    print("DEMAND FORECASTING — MODEL COMPARISON")
    print("=" * 50)
    print(comparison.to_string())
    print(f"\nBest Model: {comparison.index[0]} (MAPE: {comparison.iloc[0]['MAPE']}%)")
    
    return comparison


def get_feature_importance(model, feature_names: list, model_name: str = 'XGBoost') -> pd.DataFrame:
    """Extract feature importance from tree-based models."""
    if hasattr(model, 'feature_importances_'):
        importance = pd.DataFrame({
            'feature': feature_names,
            'importance': model.feature_importances_,
        }).sort_values('importance', ascending=False)
        
        print(f"\nTop 10 Features ({model_name}):")
        print(importance.head(10).to_string(index=False))
        
        return importance
    return pd.DataFrame()