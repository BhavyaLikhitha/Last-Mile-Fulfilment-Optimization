"""
Stockout Risk Prediction Model Comparison.
Trains: Logistic Regression, Random Forest, XGBoost, LightGBM
Compares on: AUC-ROC, Precision, Recall, F1-Score
"""

import numpy as np
import pandas as pd
from typing import Dict, Tuple
import warnings
warnings.filterwarnings('ignore')

from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    roc_auc_score, precision_score, recall_score, f1_score,
    confusion_matrix, classification_report
)
from sklearn.preprocessing import StandardScaler
import xgboost as xgb
import lightgbm as lgb


def train_logistic_regression(X_train, y_train, X_val, y_val) -> Tuple[object, Dict]:
    """Train Logistic Regression baseline."""
    # Scale features for logistic regression
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_val_scaled = scaler.transform(X_val)
    
    model = LogisticRegression(
        max_iter=1000,
        class_weight='balanced',  # Handle imbalanced classes
        random_state=42,
    )
    model.fit(X_train_scaled, y_train)
    
    y_pred = model.predict(X_val_scaled)
    y_proba = model.predict_proba(X_val_scaled)[:, 1]
    
    metrics = _calculate_metrics(y_val, y_pred, y_proba)
    
    return (model, scaler), metrics


def train_random_forest(X_train, y_train, X_val, y_val) -> Tuple[object, Dict]:
    """Train Random Forest classifier."""
    model = RandomForestClassifier(
        n_estimators=200,
        max_depth=10,
        min_samples_split=10,
        min_samples_leaf=5,
        class_weight='balanced',
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)
    
    y_pred = model.predict(X_val)
    y_proba = model.predict_proba(X_val)[:, 1]
    
    metrics = _calculate_metrics(y_val, y_pred, y_proba)
    
    return model, metrics


def train_xgboost(X_train, y_train, X_val, y_val) -> Tuple[object, Dict]:
    """Train XGBoost classifier."""
    # Calculate scale_pos_weight for imbalanced classes
    neg_count = (y_train == 0).sum()
    pos_count = (y_train == 1).sum()
    scale_pos_weight = neg_count / max(pos_count, 1)
    
    model = xgb.XGBClassifier(
        n_estimators=500,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=5,
        scale_pos_weight=scale_pos_weight,
        reg_alpha=0.1,
        reg_lambda=1.0,
        random_state=42,
        n_jobs=-1,
        eval_metric='auc',
    )
    
    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        verbose=False,
    )
    
    y_pred = model.predict(X_val)
    y_proba = model.predict_proba(X_val)[:, 1]
    
    metrics = _calculate_metrics(y_val, y_pred, y_proba)
    
    return model, metrics


def train_lightgbm(X_train, y_train, X_val, y_val) -> Tuple[object, Dict]:
    """Train LightGBM classifier."""
    model = lgb.LGBMClassifier(
        n_estimators=500,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_samples=20,
        is_unbalance=True,  # Handle imbalanced classes
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
    y_proba = model.predict_proba(X_val)[:, 1]
    
    metrics = _calculate_metrics(y_val, y_pred, y_proba)
    
    return model, metrics


def _calculate_metrics(y_true, y_pred, y_proba) -> Dict:
    """Calculate classification metrics."""
    return {
        'AUC-ROC': round(roc_auc_score(y_true, y_proba), 4),
        'Precision': round(precision_score(y_true, y_pred, zero_division=0), 4),
        'Recall': round(recall_score(y_true, y_pred, zero_division=0), 4),
        'F1-Score': round(f1_score(y_true, y_pred, zero_division=0), 4),
    }


def compare_models(results: Dict[str, Dict]) -> pd.DataFrame:
    """Create a comparison DataFrame from model results."""
    comparison = pd.DataFrame(results).T
    comparison.index.name = 'Model'
    comparison = comparison.sort_values('AUC-ROC', ascending=False)
    
    print("\n" + "=" * 50)
    print("STOCKOUT RISK — MODEL COMPARISON")
    print("=" * 50)
    print(comparison.to_string())
    print(f"\nBest Model: {comparison.index[0]} (AUC-ROC: {comparison.iloc[0]['AUC-ROC']})")
    
    return comparison


def print_confusion_matrix(y_true, y_pred, model_name: str = 'Best Model'):
    """Print formatted confusion matrix."""
    cm = confusion_matrix(y_true, y_pred)
    print(f"\nConfusion Matrix ({model_name}):")
    print(f"  TN: {cm[0][0]:,}  FP: {cm[0][1]:,}")
    print(f"  FN: {cm[1][0]:,}  TP: {cm[1][1]:,}")
    print(f"\n{classification_report(y_true, y_pred, target_names=['No Stockout', 'Stockout'])}")


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