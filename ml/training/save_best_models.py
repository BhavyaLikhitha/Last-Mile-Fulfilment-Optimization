"""
Save best models from training results.
Called after train_pipeline.py completes.

Usage:
    # Usually called from train_pipeline.py automatically
    # Or standalone:
    python -m ml.training.save_best_models
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from ml.training.save_models import save_model
from ml.features.demand_features import get_feature_columns as demand_features
from ml.features.eta_features import get_feature_columns as eta_features
from ml.features.stockout_features import get_feature_columns as stockout_features


def save_best_from_results(all_results: dict):
    """
    Save the best model for each task from training results.
    
    Args:
        all_results: Dict from run_full_pipeline() with keys 'demand', 'eta', 'stockout'
    """
    print("\n" + "=" * 60)
    print("  SAVING BEST MODELS")
    print("=" * 60)
    
    # ── Demand Forecasting ──
    if 'demand' in all_results:
        demand = all_results['demand']
        best_name = demand['comparison'].index[0]
        best_model = demand['models'].get(best_name)
        best_metrics = demand['results'].get(best_name, {})
        
        if best_model and best_name in ['XGBoost', 'LightGBM']:
            save_model(
                model=best_model,
                model_name='demand_best',
                metrics=best_metrics,
                features=demand_features(),
            )
            print(f"  ✓ Demand: Saved {best_name} (MAPE: {best_metrics.get('MAPE')}%)")
    
    # ── ETA Prediction ──
    if 'eta' in all_results:
        eta = all_results['eta']
        best_name = eta['comparison'].index[0]
        best_model = eta['models'].get(best_name)
        best_metrics = eta['results'].get(best_name, {})
        
        if best_model and best_name in ['XGBoost', 'LightGBM', 'Random Forest', 'Linear Regression']:
            save_model(
                model=best_model,
                model_name='eta_best',
                metrics=best_metrics,
                features=eta_features(),
            )
            print(f"  ✓ ETA: Saved {best_name} (MAPE: {best_metrics.get('MAPE')}%)")
    
    # ── Stockout Risk ──
    if 'stockout' in all_results:
        stockout = all_results['stockout']
        best_name = stockout['comparison'].index[0]
        best_model = stockout['models'].get(best_name)
        best_metrics = stockout['results'].get(best_name, {})
        
        if best_model:
            save_model(
                model=best_model,
                model_name='stockout_best',
                metrics=best_metrics,
                features=stockout_features(),
            )
            print(f"  ✓ Stockout: Saved {best_name} (AUC-ROC: {best_metrics.get('AUC-ROC')})")
    
    print(f"\n  Models saved to ml/saved_models/")
    print(f"  Run 'python -m ml.training.predict_and_writeback' to generate predictions")