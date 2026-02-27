"""
Save and load trained models using joblib.
Models are saved to ml/saved_models/ directory.

Usage:
    from ml.training.save_models import save_model, load_model
    
    save_model(xgb_model, 'demand_xgboost')
    model = load_model('demand_xgboost')
"""

import os
import json
import joblib
from datetime import datetime
from typing import Any, Dict


MODELS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'saved_models')


def save_model(model: Any, model_name: str, metrics: Dict = None, features: list = None) -> str:
    """
    Save a trained model to disk with metadata.
    
    Args:
        model: Trained model object
        model_name: Name for the model file (e.g., 'demand_xgboost')
        metrics: Dict of evaluation metrics
        features: List of feature column names used
    
    Returns:
        Path to saved model file
    """
    os.makedirs(MODELS_DIR, exist_ok=True)
    
    # Save model
    model_path = os.path.join(MODELS_DIR, f'{model_name}.joblib')
    joblib.dump(model, model_path)
    
    # Save metadata
    metadata = {
        'model_name': model_name,
        'saved_at': datetime.now().isoformat(),
        'metrics': metrics or {},
        'features': features or [],
    }
    
    meta_path = os.path.join(MODELS_DIR, f'{model_name}_metadata.json')
    with open(meta_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"  Model saved: {model_path}")
    print(f"  Metadata saved: {meta_path}")
    
    return model_path


def load_model(model_name: str) -> Any:
    """
    Load a saved model from disk.
    
    Args:
        model_name: Name of the model (e.g., 'demand_xgboost')
    
    Returns:
        Loaded model object
    """
    model_path = os.path.join(MODELS_DIR, f'{model_name}.joblib')
    
    if not os.path.exists(model_path):
        raise FileNotFoundError(f"Model not found: {model_path}")
    
    model = joblib.load(model_path)
    print(f"  Model loaded: {model_path}")
    
    return model


def load_metadata(model_name: str) -> Dict:
    """Load model metadata."""
    meta_path = os.path.join(MODELS_DIR, f'{model_name}_metadata.json')
    
    if not os.path.exists(meta_path):
        return {}
    
    with open(meta_path, 'r') as f:
        return json.load(f)


def list_saved_models() -> list:
    """List all saved models."""
    if not os.path.exists(MODELS_DIR):
        return []
    
    models = []
    for f in os.listdir(MODELS_DIR):
        if f.endswith('.joblib'):
            name = f.replace('.joblib', '')
            meta = load_metadata(name)
            models.append({
                'name': name,
                'saved_at': meta.get('saved_at', 'Unknown'),
                'metrics': meta.get('metrics', {}),
            })
    
    return models