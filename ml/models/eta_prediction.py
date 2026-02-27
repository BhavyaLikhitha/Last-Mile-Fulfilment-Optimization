"""
ETA Prediction Model Comparison.
Trains: Linear Regression, Random Forest, XGBoost, LightGBM
Compares on: MAPE, RMSE, R²
"""

import numpy as np
import pandas as pd
from typing import Dict, Tuple
import warnings
warnings.filterwarnings('ignore')

from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import xgboost as xgb
import lightgbm as lgb


def mean_absolute_percentage_error(y_true, y_pred):
    """Calculate MAPE, handling zero values."""
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    mask = y_true != 0
    return np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100


def train_linear_regression(X_train, y_train, X_val, y_val) -> Tuple[object, Dict]:
    """Train Linear Regression baseline."""
    model = LinearRegression()
    model.fit(X_train, y_train)
    
    y_pred = model.predict(X_val)
    y_pred = np.maximum(y_pred, 1)  # ETA must be positive
    
    metrics = {
        'MAPE': round(mean_absolute_percentage_error(y_val, y_pred), 2),
        'RMSE': round(np.sqrt(mean_squared_error(y_val, y_pred)), 2),
        'MAE': round(mean_absolute_error(y_val, y_pred), 2),
        'R2': round(r2_score(y_val, y_pred), 4),
    }
    
    return model, metrics


def train_random_forest(X_train, y_train, X_val, y_val) -> Tuple[object, Dict]:
    """Train Random Forest regressor."""
    model = RandomForestRegressor(
        n_estimators=200,
        max_depth=10,
        min_samples_split=10,
        min_samples_leaf=5,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)
    
    y_pred = model.predict(X_val)
    y_pred = np.maximum(y_pred, 1)
    
    metrics = {
        'MAPE': round(mean_absolute_percentage_error(y_val, y_pred), 2),
        'RMSE': round(np.sqrt(mean_squared_error(y_val, y_pred)), 2),
        'MAE': round(mean_absolute_error(y_val, y_pred), 2),
        'R2': round(r2_score(y_val, y_pred), 4),
    }
    
    return model, metrics


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
    y_pred = np.maximum(y_pred, 1)
    
    metrics = {
        'MAPE': round(mean_absolute_percentage_error(y_val, y_pred), 2),
        'RMSE': round(np.sqrt(mean_squared_error(y_val, y_pred)), 2),
        'MAE': round(mean_absolute_error(y_val, y_pred), 2),
        'R2': round(r2_score(y_val, y_pred), 4),
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
    y_pred = np.maximum(y_pred, 1)
    
    metrics = {
        'MAPE': round(mean_absolute_percentage_error(y_val, y_pred), 2),
        'RMSE': round(np.sqrt(mean_squared_error(y_val, y_pred)), 2),
        'MAE': round(mean_absolute_error(y_val, y_pred), 2),
        'R2': round(r2_score(y_val, y_pred), 4),
    }
    
    return model, metrics


def compare_models(results: Dict[str, Dict]) -> pd.DataFrame:
    """Create a comparison DataFrame from model results."""
    comparison = pd.DataFrame(results).T
    comparison.index.name = 'Model'
    comparison = comparison.sort_values('MAPE')
    
    print("\n" + "=" * 50)
    print("ETA PREDICTION — MODEL COMPARISON")
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