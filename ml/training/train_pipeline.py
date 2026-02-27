"""
ML Training Pipeline.
Orchestrates: Pull data from Snowflake → Build features → Train models → Compare → Write predictions back.

Usage:
    cd Last-Mile-Fulfilment-Optimization
    python -m ml.training.train_pipeline

    
# ── Run individual phases ──
python -m ml.training.train_pipeline --phase demand
python -m ml.training.train_pipeline --phase eta
python -m ml.training.train_pipeline --phase stockout

# ── Run multiple phases ──
python -m ml.training.train_pipeline --phase demand eta
python -m ml.training.train_pipeline --phase eta stockout
python -m ml.training.train_pipeline --phase demand stockout

# ── Run all phases (default) ──
python -m ml.training.train_pipeline

# ── Force fresh data from Snowflake (wipe cache) ──
python -m ml.training.train_pipeline --full-refresh

# ── Fresh data + specific phase ──
python -m ml.training.train_pipeline --full-refresh --phase eta
"""

import os
import sys
import time
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from ml.features.demand_features import (
    build_demand_features, get_feature_columns as demand_features,
    get_target_column as demand_target, train_test_split_temporal as demand_split
)
from ml.features.eta_features import (
    build_eta_features, get_feature_columns as eta_features,
    get_target_column as eta_target, train_test_split_temporal as eta_split
)
from ml.features.stockout_features import (
    build_stockout_features, get_feature_columns as stockout_features,
    get_target_column as stockout_target, train_test_split_temporal as stockout_split
)

from ml.models import demand_forecasting as demand_models
from ml.models import eta_prediction as eta_models
from ml.models import stockout_risk as stockout_models

from ml.evaluation.model_metrics import save_comparison, print_summary


# def load_data_from_snowflake():
#     """
#     Pull data from Snowflake for ML training.
#     Uses Arrow-based fetch (fetch_pandas_batches) with progress logging
#     so you can see it's not stuck.
#     Returns dict of DataFrames.
#     """
#     from dotenv import load_dotenv
#     import snowflake.connector

#     load_dotenv()

#     conn = snowflake.connector.connect(
#         account=os.getenv('SNOWFLAKE_ACCOUNT'),
#         user=os.getenv('SNOWFLAKE_USER'),
#         password=os.getenv('SNOWFLAKE_PASSWORD'),
#         database=os.getenv('SNOWFLAKE_DATABASE', 'FULFILLMENT_DB'),
#         warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'FULFILLMENT_WH'),
#     )

#     print("Connected to Snowflake. Pulling data...")

#     data = {}

#     queries = {
#         'product_kpis': "SELECT * FROM MARTS.MART_DAILY_PRODUCT_KPIS",
#         'deliveries': "SELECT * FROM INTERMEDIATE.INT_DELIVERY_ENRICHED",
#         'inventory': "SELECT * FROM INTERMEDIATE.INT_INVENTORY_ENRICHED",
#         'dates': "SELECT * FROM STAGING.STG_DATES",
#         'products': "SELECT * FROM STAGING.STG_PRODUCTS WHERE IS_CURRENT = TRUE",
#     }

#     cur = conn.cursor()

#     for name, query in queries.items():
#         print(f"\n  Loading {name}...")
#         start = time.time()

#         # Run query and get row count
#         cur.execute(query)
#         total_rows = cur.rowcount
#         print(f"    Query done — {total_rows:,} rows to fetch")

#         # Fetch in batches with progress
#         batches = []
#         rows_fetched = 0

#         for batch_df in cur.fetch_pandas_batches():
#             batch_size = len(batch_df)
#             rows_fetched += batch_size
#             pct = (rows_fetched / total_rows * 100) if total_rows else 0
#             elapsed = time.time() - start
#             print(
#                 f"    Fetched {rows_fetched:,} / {total_rows:,} rows "
#                 f"({pct:.1f}%) — {elapsed:.1f}s elapsed",
#                 end='\r'
#             )
#             batches.append(batch_df)

#         # Combine all batches
#         data[name] = pd.concat(batches, ignore_index=True) if batches else pd.DataFrame()

#         # Lowercase column names for consistency
#         data[name].columns = [c.lower() for c in data[name].columns]

#         elapsed = time.time() - start
#         print(
#             f"\n    ✓ {name}: {len(data[name]):,} rows loaded in {elapsed:.1f}s"
#         )

#     cur.close()
#     conn.close()

#     total_rows_all = sum(len(df) for df in data.values())
#     print(f"\nAll data loaded — {total_rows_all:,} total rows across {len(data)} tables.\n")

#     return data

def load_data_from_snowflake(cache_dir: str = 'output/cache', use_cache: bool = True, full_refresh: bool = False):
    """
    Pull data from Snowflake for ML training with incremental caching.

    First run:  Pulls everything from Snowflake, caches locally as parquet.
    Next runs:  Pulls only NEW rows (since last cache date), appends to cache.
    
    Args:
        cache_dir:    Directory to store cached parquet files.
        use_cache:    If True, use incremental cache. If False, always pull full data but still cache.
        full_refresh: If True, wipe cache and pull everything fresh.

    Returns:
        Dict of DataFrames.
    """
    import json
    from dotenv import load_dotenv
    import snowflake.connector

    load_dotenv()
    os.makedirs(cache_dir, exist_ok=True)

    # Table configs: name → (query_template, date_column for incremental pulls)
    # Tables with a date column support incremental loads.
    # Small/static tables (dates, products) always do full pulls.
    table_config = {
        'product_kpis': {
            'query': "SELECT * FROM MARTS.MART_DAILY_PRODUCT_KPIS",
            'incremental_col': 'DATE',
        },
        'deliveries': {
            'query': "SELECT * FROM INTERMEDIATE.INT_DELIVERY_ENRICHED",
            'incremental_col': 'ASSIGNED_TIME',
        },
        'inventory': {
            'query': "SELECT * FROM INTERMEDIATE.INT_INVENTORY_ENRICHED",
            'incremental_col': 'SNAPSHOT_DATE',
        },
        'dates': {
            'query': "SELECT * FROM STAGING.STG_DATES",
            'incremental_col': None,  # Always full pull (tiny table)
        },
        'products': {
            'query': "SELECT * FROM STAGING.STG_PRODUCTS WHERE IS_CURRENT = TRUE",
            'incremental_col': None,  # Always full pull (tiny table)
        },
    }

    # Metadata file tracks last pull date per table
    meta_path = os.path.join(cache_dir, '_cache_meta.json')

    # ── Handle full refresh ──
    if full_refresh:
        print("Full refresh requested — clearing cache...")
        for f in os.listdir(cache_dir):
            os.remove(os.path.join(cache_dir, f))
        cache_meta = {}
    else:
        cache_meta = {}
        if os.path.exists(meta_path):
            with open(meta_path, 'r') as f:
                cache_meta = json.load(f)

    # ── Connect to Snowflake ──
    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        database=os.getenv('SNOWFLAKE_DATABASE', 'FULFILLMENT_DB'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'FULFILLMENT_WH'),
    )
    cur = conn.cursor()
    print("Connected to Snowflake.\n")

    data = {}

    for name, config in table_config.items():
        cache_path = os.path.join(cache_dir, f'{name}.parquet')
        inc_col = config['incremental_col']
        has_cache = os.path.exists(cache_path) and use_cache
        last_pulled = cache_meta.get(name, {}).get('last_date')

        # ── Decide: full pull or incremental ──
        if has_cache and inc_col and last_pulled and not full_refresh:
            # INCREMENTAL: only pull new rows
            query = f"{config['query']} WHERE {inc_col} > '{last_pulled}'"
            mode = 'incremental'
        else:
            # FULL: pull everything
            query = config['query']
            mode = 'full'

        print(f"  Loading {name} ({mode})...")
        start = time.time()

        cur.execute(query)
        total_rows = cur.rowcount
        print(f"    Query done — {total_rows:,} rows to fetch")

        if total_rows == 0 and has_cache:
            # No new data — just load from cache
            data[name] = pd.read_parquet(cache_path)
            elapsed = time.time() - start
            print(f"    ✓ No new rows. Loaded {len(data[name]):,} rows from cache in {elapsed:.1f}s")
            continue

        # Fetch in batches with progress
        batches = []
        rows_fetched = 0

        for batch_df in cur.fetch_pandas_batches():
            rows_fetched += len(batch_df)
            pct = (rows_fetched / total_rows * 100) if total_rows else 0
            elapsed = time.time() - start
            print(
                f"    Fetched {rows_fetched:,} / {total_rows:,} rows "
                f"({pct:.1f}%) — {elapsed:.1f}s elapsed",
                end='\r'
            )
            batches.append(batch_df)

        new_data = pd.concat(batches, ignore_index=True) if batches else pd.DataFrame()
        new_data.columns = [c.lower() for c in new_data.columns]

        # ── Merge with existing cache if incremental ──
        if mode == 'incremental' and has_cache:
            cached_data = pd.read_parquet(cache_path)
            data[name] = pd.concat([cached_data, new_data], ignore_index=True)
            print(f"\n    ✓ {name}: +{len(new_data):,} new rows → {len(data[name]):,} total")
        else:
            data[name] = new_data
            print(f"\n    ✓ {name}: {len(data[name]):,} rows loaded")

        # ── Update cache ──
        data[name].to_parquet(cache_path, index=False)

        # Track the max date for next incremental pull
        if inc_col:
            col_lower = inc_col.lower()
            if col_lower in data[name].columns:
                max_date = str(pd.to_datetime(data[name][col_lower]).max())
                cache_meta[name] = {'last_date': max_date, 'rows': len(data[name])}

        elapsed = time.time() - start
        print(f"    Cached to {cache_path} ({elapsed:.1f}s)")

    cur.close()
    conn.close()

    # ── Save metadata ──
    with open(meta_path, 'w') as f:
        json.dump(cache_meta, f, indent=2)

    total_rows_all = sum(len(df) for df in data.values())
    print(f"\nAll data loaded — {total_rows_all:,} total rows across {len(data)} tables.")
    print(f"Cache metadata saved to {meta_path}\n")

    return data

def load_data_from_csv(base_path: str = 'output/raw'):
    """
    Alternative: Load from local CSV files if Snowflake isn't available.
    Useful for testing without Snowflake connection.
    """
    print("Loading data from local CSV files...")
    
    # This is a simplified loader — in production, use Snowflake
    data = {}
    
    # Load product KPIs (you'd need to compute this or load from a pre-built CSV)
    print("  Note: For full pipeline, use load_data_from_snowflake()")
    print("  CSV loading is for testing only.")
    
    return data


def run_demand_forecasting(data: dict) -> dict:
    """Run the demand forecasting pipeline."""
    print("\n" + "=" * 60)
    print("  PHASE 1: DEMAND FORECASTING")
    print("=" * 60)
    
    start = time.time()
    
    # Build features
    print("\nBuilding features...")
    df = build_demand_features(data['product_kpis'], data['dates'], data['products'])
    print(f"  Feature matrix: {df.shape[0]:,} rows x {df.shape[1]} columns")
    
    # Split data
    print("\nSplitting data (temporal)...")
    train, val, test = demand_split(df)
    
    features = demand_features()
    target = demand_target()
    
    X_train = train[features].fillna(0)
    y_train = train[target]
    X_val = val[features].fillna(0)
    y_val = val[target]
    X_test = test[features].fillna(0)
    y_test = test[target]
    
    # Train models
    results = {}
    models = {}
    
    # 1. SARIMA
    print("\nTraining SARIMA...")
    sarima_model, sarima_metrics = demand_models.train_sarima(train, val)
    results['SARIMA'] = sarima_metrics
    models['SARIMA'] = sarima_model
    print(f"  MAPE: {sarima_metrics['MAPE']}")
    
    # 2. Prophet
    print("\nTraining Prophet...")
    prophet_model, prophet_metrics = demand_models.train_prophet(train, val)
    results['Prophet'] = prophet_metrics
    models['Prophet'] = prophet_model
    print(f"  MAPE: {prophet_metrics['MAPE']}")
    
    # 3. XGBoost
    print("\nTraining XGBoost...")
    xgb_model, xgb_metrics = demand_models.train_xgboost(X_train, y_train, X_val, y_val)
    results['XGBoost'] = xgb_metrics
    models['XGBoost'] = xgb_model
    print(f"  MAPE: {xgb_metrics['MAPE']}")
    
    # 4. LightGBM
    print("\nTraining LightGBM...")
    lgb_model, lgb_metrics = demand_models.train_lightgbm(X_train, y_train, X_val, y_val)
    results['LightGBM'] = lgb_metrics
    models['LightGBM'] = lgb_model
    print(f"  MAPE: {lgb_metrics['MAPE']}")
    
    # Compare
    comparison = demand_models.compare_models(results)
    save_comparison(results, 'demand_forecasting')
    
    # Feature importance from best tree model
    best_tree = 'XGBoost' if results['XGBoost']['MAPE'] <= results['LightGBM']['MAPE'] else 'LightGBM'
    demand_models.get_feature_importance(models[best_tree], features, best_tree)
    
    # Test set evaluation with best model
    best_model_name = comparison.index[0]
    if best_model_name in ['XGBoost', 'LightGBM']:
        y_pred_test = models[best_model_name].predict(X_test)
        y_pred_test = np.maximum(y_pred_test, 0)
        test_mape = demand_models.mean_absolute_percentage_error(y_test, y_pred_test)
        print(f"\nTest Set MAPE ({best_model_name}): {test_mape:.2f}%")
    
    elapsed = time.time() - start
    print(f"\nDemand forecasting completed in {elapsed:.0f}s")
    
    return {'results': results, 'models': models, 'comparison': comparison, 'test_data': test}


# def run_eta_prediction(data: dict) -> dict:
#     """Run the ETA prediction pipeline."""
#     print("\n" + "=" * 60)
#     print("  PHASE 2: ETA PREDICTION")
#     print("=" * 60)
    
#     start = time.time()
    
#     # Build features
#     print("\nBuilding features...")
#     df = build_eta_features(data['deliveries'], data['dates'])
#     print(f"  Feature matrix: {df.shape[0]:,} rows x {df.shape[1]} columns")
    
#     # Split data
#     print("\nSplitting data (temporal)...")
#     train, val, test = eta_split(df)
    
#     features = eta_features()
#     target = eta_target()
    
#     # Handle missing features gracefully
#     available_features = [f for f in features if f in train.columns]
    
#     X_train = train[available_features].fillna(0)
#     y_train = train[target]
#     X_val = val[available_features].fillna(0)
#     y_val = val[target]
#     X_test = test[available_features].fillna(0)
#     y_test = test[target]
    
#     results = {}
#     models = {}
    
#     # 1. Linear Regression
#     print("\nTraining Linear Regression...")
#     lr_model, lr_metrics = eta_models.train_linear_regression(X_train, y_train, X_val, y_val)
#     results['Linear Regression'] = lr_metrics
#     models['Linear Regression'] = lr_model
#     print(f"  MAPE: {lr_metrics['MAPE']}, R²: {lr_metrics['R2']}")
    
#     # 2. Random Forest
#     print("\nTraining Random Forest...")
#     rf_model, rf_metrics = eta_models.train_random_forest(X_train, y_train, X_val, y_val)
#     results['Random Forest'] = rf_metrics
#     models['Random Forest'] = rf_model
#     print(f"  MAPE: {rf_metrics['MAPE']}, R²: {rf_metrics['R2']}")
    
#     # 3. XGBoost
#     print("\nTraining XGBoost...")
#     xgb_model, xgb_metrics = eta_models.train_xgboost(X_train, y_train, X_val, y_val)
#     results['XGBoost'] = xgb_metrics
#     models['XGBoost'] = xgb_model
#     print(f"  MAPE: {xgb_metrics['MAPE']}, R²: {xgb_metrics['R2']}")
    
#     # 4. LightGBM
#     print("\nTraining LightGBM...")
#     lgb_model, lgb_metrics = eta_models.train_lightgbm(X_train, y_train, X_val, y_val)
#     results['LightGBM'] = lgb_metrics
#     models['LightGBM'] = lgb_model
#     print(f"  MAPE: {lgb_metrics['MAPE']}, R²: {lgb_metrics['R2']}")
    
#     # Compare
#     comparison = eta_models.compare_models(results)
#     save_comparison(results, 'eta_prediction')
    
#     # Feature importance
#     best_tree = 'XGBoost' if results['XGBoost']['MAPE'] <= results['LightGBM']['MAPE'] else 'LightGBM'
#     eta_models.get_feature_importance(models[best_tree], available_features, best_tree)
    
#     elapsed = time.time() - start
#     print(f"\nETA prediction completed in {elapsed:.0f}s")
    
#     return {'results': results, 'models': models, 'comparison': comparison}


def run_eta_prediction(data: dict) -> dict:
    """Run the ETA prediction pipeline."""
    print("\n" + "=" * 60)
    print("  PHASE 2: ETA PREDICTION")
    print("=" * 60)
    
    start = time.time()
    
    # Build features
    print("\nBuilding features...")
    df = build_eta_features(data['deliveries'], data['dates'])
    print(f"  Feature matrix: {df.shape[0]:,} rows x {df.shape[1]} columns")
    
    # Split data
    print("\nSplitting data (temporal)...")
    train, val, test = eta_split(df)
    
    features = eta_features()
    target = eta_target()
    
    # Handle missing features gracefully
    available_features = [f for f in features if f in train.columns]
    
    X_train = train[available_features].fillna(0)
    y_train = train[target]
    X_val = val[available_features].fillna(0)
    y_val = val[target]
    X_test = test[available_features].fillna(0)
    y_test = test[target]
    
    results = {}
    models = {}
    
    # 1. Linear Regression
    print("\nTraining Linear Regression...")
    lr_model, lr_metrics = eta_models.train_linear_regression(X_train, y_train, X_val, y_val)
    results['Linear Regression'] = lr_metrics
    models['Linear Regression'] = lr_model
    print(f"  MAPE: {lr_metrics['MAPE']}, R²: {lr_metrics['R2']}")
    
    # 2. XGBoost
    print("\nTraining XGBoost...")
    xgb_model, xgb_metrics = eta_models.train_xgboost(X_train, y_train, X_val, y_val)
    results['XGBoost'] = xgb_metrics
    models['XGBoost'] = xgb_model
    print(f"  MAPE: {xgb_metrics['MAPE']}, R²: {xgb_metrics['R2']}")
    
    # 3. LightGBM
    print("\nTraining LightGBM...")
    lgb_model, lgb_metrics = eta_models.train_lightgbm(X_train, y_train, X_val, y_val)
    results['LightGBM'] = lgb_metrics
    models['LightGBM'] = lgb_model
    print(f"  MAPE: {lgb_metrics['MAPE']}, R²: {lgb_metrics['R2']}")
    
    # Compare
    comparison = eta_models.compare_models(results)
    save_comparison(results, 'eta_prediction')
    
    # Feature importance
    best_tree = 'XGBoost' if results['XGBoost']['MAPE'] <= results['LightGBM']['MAPE'] else 'LightGBM'
    eta_models.get_feature_importance(models[best_tree], available_features, best_tree)
    
    elapsed = time.time() - start
    print(f"\nETA prediction completed in {elapsed:.0f}s")
    
    return {'results': results, 'models': models, 'comparison': comparison}


# def run_stockout_risk(data: dict) -> dict:
#     """Run the stockout risk prediction pipeline."""
#     print("\n" + "=" * 60)
#     print("  PHASE 3: STOCKOUT RISK PREDICTION")
#     print("=" * 60)
    
#     start = time.time()
    
#     # Build features
#     print("\nBuilding features...")
#     df = build_stockout_features(data['inventory'], data['dates'], data['products'])
#     print(f"  Feature matrix: {df.shape[0]:,} rows x {df.shape[1]} columns")
    
#     # Split data
#     print("\nSplitting data (temporal)...")
#     train, val, test = stockout_split(df)
    
#     features = stockout_features()
#     target = stockout_target()
    
#     available_features = [f for f in features if f in train.columns]
    
#     X_train = train[available_features].fillna(0)
#     y_train = train[target]
#     X_val = val[available_features].fillna(0)
#     y_val = val[target]
#     X_test = test[available_features].fillna(0)
#     y_test = test[target]
    
#     results = {}
#     models = {}
    
#     # 1. Logistic Regression
#     print("\nTraining Logistic Regression...")
#     lr_model, lr_metrics = stockout_models.train_logistic_regression(X_train, y_train, X_val, y_val)
#     results['Logistic Regression'] = lr_metrics
#     models['Logistic Regression'] = lr_model
#     print(f"  AUC-ROC: {lr_metrics['AUC-ROC']}, F1: {lr_metrics['F1-Score']}")
    
#     # 2. Random Forest
#     print("\nTraining Random Forest...")
#     rf_model, rf_metrics = stockout_models.train_random_forest(X_train, y_train, X_val, y_val)
#     results['Random Forest'] = rf_metrics
#     models['Random Forest'] = rf_model
#     print(f"  AUC-ROC: {rf_metrics['AUC-ROC']}, F1: {rf_metrics['F1-Score']}")
    
#     # 3. XGBoost
#     print("\nTraining XGBoost...")
#     xgb_model, xgb_metrics = stockout_models.train_xgboost(X_train, y_train, X_val, y_val)
#     results['XGBoost'] = xgb_metrics
#     models['XGBoost'] = xgb_model
#     print(f"  AUC-ROC: {xgb_metrics['AUC-ROC']}, F1: {xgb_metrics['F1-Score']}")
    
#     # 4. LightGBM
#     print("\nTraining LightGBM...")
#     lgb_model, lgb_metrics = stockout_models.train_lightgbm(X_train, y_train, X_val, y_val)
#     results['LightGBM'] = lgb_metrics
#     models['LightGBM'] = lgb_model
#     print(f"  AUC-ROC: {lgb_metrics['AUC-ROC']}, F1: {lgb_metrics['F1-Score']}")
    
#     # Compare
#     comparison = stockout_models.compare_models(results)
#     save_comparison(results, 'stockout_risk')
    
#     # Confusion matrix for best model
#     best_name = comparison.index[0]
#     best_model = models[best_name]
#     if best_name == 'Logistic Regression':
#         model_obj, scaler = best_model
#         y_pred_best = model_obj.predict(scaler.transform(X_val))
#     else:
#         y_pred_best = best_model.predict(X_val)
#     stockout_models.print_confusion_matrix(y_val, y_pred_best, best_name)
    
#     # Feature importance
#     if best_name in ['XGBoost', 'LightGBM', 'Random Forest']:
#         model_for_fi = best_model if best_name != 'Logistic Regression' else None
#         if model_for_fi:
#             stockout_models.get_feature_importance(model_for_fi, available_features, best_name)
    
#     elapsed = time.time() - start
#     print(f"\nStockout risk prediction completed in {elapsed:.0f}s")
    
#     return {'results': results, 'models': models, 'comparison': comparison}

def run_stockout_risk(data: dict) -> dict:
    """Run the stockout risk prediction pipeline."""
    print("\n" + "=" * 60)
    print("  PHASE 3: STOCKOUT RISK PREDICTION")
    print("=" * 60)
    
    start = time.time()
    
    # Build features
    print("\nBuilding features...")
    df = build_stockout_features(data['inventory'], data['dates'], data['products'])
    print(f"  Feature matrix: {df.shape[0]:,} rows x {df.shape[1]} columns")
    
    # Split data
    print("\nSplitting data (temporal)...")
    train, val, test = stockout_split(df)
    
    features = stockout_features()
    target = stockout_target()
    
    available_features = [f for f in features if f in train.columns]
    
    X_train = train[available_features].fillna(0)
    y_train = train[target]
    X_val = val[available_features].fillna(0)
    y_val = val[target]
    X_test = test[available_features].fillna(0)
    y_test = test[target]
    
    results = {}
    models = {}
    
    # 1. Logistic Regression
    print("\nTraining Logistic Regression...")
    lr_model, lr_metrics = stockout_models.train_logistic_regression(X_train, y_train, X_val, y_val)
    results['Logistic Regression'] = lr_metrics
    models['Logistic Regression'] = lr_model
    print(f"  AUC-ROC: {lr_metrics['AUC-ROC']}, F1: {lr_metrics['F1-Score']}")
    
    # 2. XGBoost
    print("\nTraining XGBoost...")
    xgb_model, xgb_metrics = stockout_models.train_xgboost(X_train, y_train, X_val, y_val)
    results['XGBoost'] = xgb_metrics
    models['XGBoost'] = xgb_model
    print(f"  AUC-ROC: {xgb_metrics['AUC-ROC']}, F1: {xgb_metrics['F1-Score']}")
    
    # 3. LightGBM
    print("\nTraining LightGBM...")
    lgb_model, lgb_metrics = stockout_models.train_lightgbm(X_train, y_train, X_val, y_val)
    results['LightGBM'] = lgb_metrics
    models['LightGBM'] = lgb_model
    print(f"  AUC-ROC: {lgb_metrics['AUC-ROC']}, F1: {lgb_metrics['F1-Score']}")
    
    # Compare
    comparison = stockout_models.compare_models(results)
    save_comparison(results, 'stockout_risk')
    
    # Confusion matrix for best model
    best_name = comparison.index[0]
    best_model = models[best_name]
    if best_name == 'Logistic Regression':
        model_obj, scaler = best_model
        y_pred_best = model_obj.predict(scaler.transform(X_val))
    else:
        y_pred_best = best_model.predict(X_val)
    stockout_models.print_confusion_matrix(y_val, y_pred_best, best_name)
    
    # Feature importance
    if best_name in ['XGBoost', 'LightGBM']:
        stockout_models.get_feature_importance(best_model, available_features, best_name)
    
    elapsed = time.time() - start
    print(f"\nStockout risk prediction completed in {elapsed:.0f}s")
    
    return {'results': results, 'models': models, 'comparison': comparison}

# def run_full_pipeline():
#     """Run all 3 ML pipelines end to end."""
#     print("=" * 60)
#     print("  FULFILLMENT PLATFORM — ML TRAINING PIPELINE")
#     print("=" * 60)
    
#     total_start = time.time()
    
#     # Load data
#     data = load_data_from_snowflake()
    
#     # Run all 3 pipelines
#     demand_results = run_demand_forecasting(data)
#     eta_results = run_eta_prediction(data)
#     stockout_results = run_stockout_risk(data)
    
#     # Final summary
#     total_elapsed = time.time() - total_start
    
#     print("\n" + "=" * 60)
#     print("  ML PIPELINE COMPLETE")
#     print("=" * 60)
#     print(f"  Total time: {total_elapsed:.0f}s ({total_elapsed/60:.1f} min)")
#     print(f"\n  Demand Forecasting: Best = {demand_results['comparison'].index[0]}")
#     print(f"  ETA Prediction:     Best = {eta_results['comparison'].index[0]}")
#     print(f"  Stockout Risk:      Best = {stockout_results['comparison'].index[0]}")
#     print(f"\n  Results saved to ml/results/")
#     print(f"  Next: Write predictions back to Snowflake mart tables")
    
#     return {
#         'demand': demand_results,
#         'eta': eta_results,
#         'stockout': stockout_results,
#     }


# if __name__ == "__main__":
#     run_full_pipeline()

def run_full_pipeline(phases: list = None, full_refresh: bool = False):
    """
    Run ML pipelines end to end.
    
    Args:
        phases: List of phases to run. Options: ['demand', 'eta', 'stockout'].
                If None, runs all 3.
        full_refresh: If True, wipe cache and pull fresh data from Snowflake.
    """
    all_phases = ['demand', 'eta', 'stockout']
    phases = phases or all_phases
    
    print("=" * 60)
    print("  FULFILLMENT PLATFORM — ML TRAINING PIPELINE")
    print(f"  Phases: {', '.join(phases)}")
    print("=" * 60)
    
    total_start = time.time()
    
    # Load data
    data = load_data_from_snowflake(full_refresh=full_refresh)
    
    results = {}
    
    # Run selected phases
    if 'demand' in phases:
        results['demand'] = run_demand_forecasting(data)
    
    if 'eta' in phases:
        results['eta'] = run_eta_prediction(data)
    
    if 'stockout' in phases:
        results['stockout'] = run_stockout_risk(data)
    
    # Final summary
    total_elapsed = time.time() - total_start
    
    print("\n" + "=" * 60)
    print("  ML PIPELINE COMPLETE")
    print("=" * 60)
    print(f"  Total time: {total_elapsed:.0f}s ({total_elapsed/60:.1f} min)")
    
    if 'demand' in results:
        print(f"\n  Demand Forecasting: Best = {results['demand']['comparison'].index[0]}")
    if 'eta' in results:
        print(f"  ETA Prediction:     Best = {results['eta']['comparison'].index[0]}")
    if 'stockout' in results:
        print(f"  Stockout Risk:      Best = {results['stockout']['comparison'].index[0]}")
    
    print(f"\n  Results saved to ml/results/")
    print(f"  Next: Write predictions back to Snowflake mart tables")

    # Save best models
    from ml.training.save_best_models import save_best_from_results
    save_best_from_results(results)
    
    return results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='ML Training Pipeline')
    parser.add_argument(
        '--phase',
        nargs='+',
        choices=['demand', 'eta', 'stockout'],
        default=None,
        help='Run specific phases. E.g. --phase eta stockout. Default: all.'
    )
    parser.add_argument(
        '--full-refresh',
        action='store_true',
        help='Wipe cache and pull fresh data from Snowflake.'
    )
    
    args = parser.parse_args()
    run_full_pipeline(phases=args.phase, full_refresh=args.full_refresh)