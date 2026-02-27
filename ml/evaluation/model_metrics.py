"""
Model evaluation utilities.
Provides consistent metric calculation across all models.
"""

import numpy as np
import pandas as pd
from typing import Dict
from sklearn.metrics import (
    mean_absolute_error, mean_squared_error, r2_score,
    roc_auc_score, precision_score, recall_score, f1_score,
    confusion_matrix, classification_report
)


def mean_absolute_percentage_error(y_true, y_pred) -> float:
    """MAPE — handles zero values by masking them."""
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    mask = y_true != 0
    if mask.sum() == 0:
        return 0.0
    return np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100


def regression_metrics(y_true, y_pred) -> Dict:
    """Calculate all regression metrics."""
    return {
        'MAPE': round(mean_absolute_percentage_error(y_true, y_pred), 2),
        'RMSE': round(np.sqrt(mean_squared_error(y_true, y_pred)), 2),
        'MAE': round(mean_absolute_error(y_true, y_pred), 2),
        'R2': round(r2_score(y_true, y_pred), 4),
    }


def classification_metrics(y_true, y_pred, y_proba=None) -> Dict:
    """Calculate all classification metrics."""
    metrics = {
        'Precision': round(precision_score(y_true, y_pred, zero_division=0), 4),
        'Recall': round(recall_score(y_true, y_pred, zero_division=0), 4),
        'F1-Score': round(f1_score(y_true, y_pred, zero_division=0), 4),
    }
    
    if y_proba is not None:
        metrics['AUC-ROC'] = round(roc_auc_score(y_true, y_proba), 4)
    
    return metrics


def save_comparison(results: Dict[str, Dict], task_name: str, output_path: str = 'ml/results/'):
    """Save model comparison to CSV."""
    import os
    os.makedirs(output_path, exist_ok=True)
    
    df = pd.DataFrame(results).T
    df.index.name = 'Model'
    filepath = os.path.join(output_path, f'{task_name}_comparison.csv')
    df.to_csv(filepath)
    print(f"Saved comparison to {filepath}")
    return df


def print_summary(task_name: str, comparison: pd.DataFrame, sort_by: str = 'MAPE'):
    """Print formatted model comparison summary."""
    print(f"\n{'=' * 60}")
    print(f"  {task_name.upper()} — MODEL COMPARISON")
    print(f"{'=' * 60}")
    
    sorted_df = comparison.sort_values(sort_by, ascending=(sort_by != 'AUC-ROC'))
    print(sorted_df.to_string())
    
    best = sorted_df.index[0]
    best_val = sorted_df.iloc[0][sort_by]
    print(f"\n  Best Model: {best}")
    print(f"  Best {sort_by}: {best_val}")
    print(f"{'=' * 60}")