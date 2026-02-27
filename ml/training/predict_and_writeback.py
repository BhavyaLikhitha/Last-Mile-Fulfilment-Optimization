# """
# Prediction & Writeback Pipeline.
# Uses saved best models to:
# 1. Forecast demand and write to mart_daily_product_kpis
# 2. Predict ETA for deliveries
# 3. Score stockout risk for inventory
# 4. Write all predictions back to Snowflake

# Usage:
#     python -m ml.training.predict_and_writeback
#     python -m ml.training.predict_and_writeback --phase demand
#     python -m ml.training.predict_and_writeback --phase eta stockout
# """

# import os
# import sys
# import time
# import numpy as np
# import pandas as pd
# import warnings
# warnings.filterwarnings('ignore')

# sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# from ml.training.save_models import load_model, load_metadata
# from ml.features.demand_features import build_demand_features, get_feature_columns as demand_features
# from ml.features.eta_features import build_eta_features, get_feature_columns as eta_features
# from ml.features.stockout_features import build_stockout_features, get_feature_columns as stockout_features

# # Lookback: how many days of history to pull for lag features
# LOOKBACK_DAYS = 60
# # Predict window: predict for the last N days of available data
# PREDICT_WINDOW_DAYS = 90


# def get_snowflake_connection():
#     """Create Snowflake connection."""
#     from dotenv import load_dotenv
#     import snowflake.connector
    
#     load_dotenv()
    
#     return snowflake.connector.connect(
#         account=os.getenv('SNOWFLAKE_ACCOUNT'),
#         user=os.getenv('SNOWFLAKE_USER'),
#         password=os.getenv('SNOWFLAKE_PASSWORD'),
#         database=os.getenv('SNOWFLAKE_DATABASE', 'FULFILLMENT_DB'),
#         warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'FULFILLMENT_WH'),
#     )


# def get_data_date_range(conn, table: str, date_col: str) -> tuple:
#     """Get min and max dates from a table. Returns (max_date, predict_start_date)."""
#     result = pd.read_sql(f"SELECT MAX({date_col}) as max_date FROM {table}", conn)
#     max_date = pd.to_datetime(result['MAX_DATE'].iloc[0])
#     predict_start = max_date - pd.Timedelta(days=PREDICT_WINDOW_DAYS)
#     pull_start = predict_start - pd.Timedelta(days=LOOKBACK_DAYS)
#     return max_date, predict_start, pull_start


# def predict_demand():
#     """
#     Generate demand forecasts and write to mart_daily_product_kpis.
#     Updates demand_forecast and forecast_error columns.
#     """
#     print("\n" + "=" * 60)
#     print("  DEMAND FORECAST — PREDICT & WRITEBACK")
#     print("=" * 60)
    
#     start = time.time()
    
#     # Load best model
#     model = load_model('demand_best')
#     meta = load_metadata('demand_best')
#     features = meta.get('features', demand_features())
    
#     print(f"  Using model: {meta.get('model_name', 'unknown')}")
#     print(f"  Metrics: {meta.get('metrics', {})}")
    
#     # Connect to Snowflake
#     conn = get_snowflake_connection()
#     cur = conn.cursor()
    
#     # Get dynamic date range
#     max_date, predict_start, pull_start = get_data_date_range(
#         conn, 'MARTS.MART_DAILY_PRODUCT_KPIS', 'DATE'
#     )
#     print(f"\n  Data range: up to {max_date.date()}")
#     print(f"  Predict window: {predict_start.date()} → {max_date.date()}")
#     print(f"  Pulling data from: {pull_start.date()} (includes {LOOKBACK_DAYS}d lookback for lags)")
    
#     # Pull data
#     product_kpis = pd.read_sql(
#         f"SELECT * FROM MARTS.MART_DAILY_PRODUCT_KPIS WHERE DATE >= '{pull_start.date()}'",
#         conn
#     )
#     product_kpis.columns = [c.lower() for c in product_kpis.columns]
#     print(f"  Loaded {len(product_kpis):,} rows from product_kpis")
    
#     dates = pd.read_sql("SELECT * FROM STAGING.STG_DATES", conn)
#     dates.columns = [c.lower() for c in dates.columns]
    
#     products = pd.read_sql("SELECT * FROM STAGING.STG_PRODUCTS WHERE IS_CURRENT = TRUE", conn)
#     products.columns = [c.lower() for c in products.columns]
    
#     if len(product_kpis) == 0:
#         print("  No data found. Skipping.")
#         conn.close()
#         return
    
#     # Build features
#     print("  Building features...")
#     df = build_demand_features(product_kpis, dates, products)
    
#     # Only predict for the target window (not the lookback period)
#     df['date'] = pd.to_datetime(df['date'])
#     df_predict = df[df['date'] >= predict_start].copy()
    
#     if len(df_predict) == 0:
#         print("  No rows in prediction window after feature engineering. Skipping.")
#         conn.close()
#         return
    
#     print(f"  Predicting for {len(df_predict):,} rows")
    
#     available_features = [f for f in features if f in df_predict.columns]
#     X = df_predict[available_features].fillna(0)
    
#     # Generate predictions
#     print("  Generating predictions...")
#     df_predict.loc[:, 'demand_forecast'] = np.maximum(model.predict(X), 0).round(2)
#     df_predict.loc[:, 'forecast_error'] = (df_predict['total_units_sold'] - df_predict['demand_forecast']).round(2)
    
#     # Calculate MAPE
#     mask = df_predict['total_units_sold'] > 0
#     mape = np.mean(np.abs(df_predict.loc[mask, 'forecast_error'] / df_predict.loc[mask, 'total_units_sold'])) * 100
#     print(f"  Prediction MAPE: {mape:.2f}%")
    
#     # Write back to Snowflake
#     print("  Writing predictions to Snowflake...")
#     update_count = 0
#     total = len(df_predict)
    
#     for idx, (_, row) in enumerate(df_predict.iterrows()):
#         if idx % 5000 == 0 and idx > 0:
#             print(f"    Updated {idx:,} / {total:,} rows ({idx/total*100:.0f}%)")
#             conn.commit()
        
#         try:
#             date_str = str(row['date'].date()) if hasattr(row['date'], 'date') else str(row['date'])[:10]
#             cur.execute("""
#                 UPDATE MARTS.MART_DAILY_PRODUCT_KPIS 
#                 SET DEMAND_FORECAST = %s, FORECAST_ERROR = %s
#                 WHERE DATE = %s AND PRODUCT_ID = %s
#             """, (
#                 float(row['demand_forecast']),
#                 float(row['forecast_error']),
#                 date_str,
#                 str(row['product_id']),
#             ))
#             update_count += 1
#         except Exception as e:
#             pass
    
#     conn.commit()
#     cur.close()
#     conn.close()
    
#     elapsed = time.time() - start
#     print(f"\n  ✓ Updated {update_count:,} / {total:,} rows in mart_daily_product_kpis")
#     print(f"  ✓ Completed in {elapsed:.0f}s")
    
#     # Print sample
#     sample = df_predict[['date', 'product_id', 'total_units_sold', 'demand_forecast', 'forecast_error']].tail(10)
#     print(f"\n  Sample predictions:")
#     print(sample.to_string(index=False))


# def predict_eta():
#     """
#     Generate ETA predictions for recent deliveries.
#     Saves predictions locally as CSV.
#     """
#     print("\n" + "=" * 60)
#     print("  ETA PREDICTION — PREDICT & WRITEBACK")
#     print("=" * 60)
    
#     start = time.time()
    
#     # Load best model
#     model = load_model('eta_best')
#     meta = load_metadata('eta_best')
#     features = meta.get('features', eta_features())
    
#     print(f"  Using model: {meta.get('model_name', 'unknown')}")
    
#     # Connect to Snowflake
#     conn = get_snowflake_connection()
    
#     # Get dynamic date range
#     max_date, predict_start, pull_start = get_data_date_range(
#         conn, 'INTERMEDIATE.INT_DELIVERY_ENRICHED', 'DELIVERY_DATE'
#     )
#     print(f"\n  Data range: up to {max_date.date()}")
#     print(f"  Predict window: {predict_start.date()} → {max_date.date()}")
    
#     # Pull data
#     deliveries = pd.read_sql(
#         f"SELECT * FROM INTERMEDIATE.INT_DELIVERY_ENRICHED WHERE DELIVERY_DATE >= '{predict_start.date()}'",
#         conn
#     )
#     deliveries.columns = [c.lower() for c in deliveries.columns]
#     print(f"  Loaded {len(deliveries):,} deliveries")
    
#     dates = pd.read_sql("SELECT * FROM STAGING.STG_DATES", conn)
#     dates.columns = [c.lower() for c in dates.columns]
    
#     if len(deliveries) == 0:
#         print("  No data found. Skipping.")
#         conn.close()
#         return
    
#     # Build features
#     print("  Building features...")
#     df = build_eta_features(deliveries, dates)
    
#     if len(df) == 0:
#         print("  No delivered orders in window. Skipping.")
#         conn.close()
#         return
    
#     print(f"  Predicting for {len(df):,} deliveries")
    
#     available_features = [f for f in features if f in df.columns]
#     X = df[available_features].fillna(0)
    
#     # Generate predictions
#     print("  Generating ETA predictions...")
#     df.loc[:, 'predicted_eta'] = np.maximum(model.predict(X), 1).round(2)
#     df.loc[:, 'eta_error'] = (df['actual_delivery_minutes'] - df['predicted_eta']).round(2)
    
#     # Metrics
#     mask = df['actual_delivery_minutes'] > 0
#     mape = np.mean(np.abs(df.loc[mask, 'eta_error'] / df.loc[mask, 'actual_delivery_minutes'])) * 100
#     print(f"\n  ETA Prediction MAPE: {mape:.2f}%")
#     print(f"  Mean ETA Error: {df['eta_error'].mean():.1f} minutes")
#     print(f"  Median ETA Error: {df['eta_error'].median():.1f} minutes")
    
#     # Save predictions locally
#     output_path = 'ml/results/eta_predictions_latest.csv'
#     os.makedirs('ml/results', exist_ok=True)
#     output_cols = ['delivery_id', 'order_id', 'actual_delivery_minutes', 'predicted_eta', 'eta_error']
#     available_output = [c for c in output_cols if c in df.columns]
#     df[available_output].to_csv(output_path, index=False)
    
#     conn.close()
    
#     elapsed = time.time() - start
#     print(f"\n  ✓ Predictions saved to {output_path}")
#     print(f"  ✓ {len(df):,} deliveries scored in {elapsed:.0f}s")


# def predict_stockout():
#     """
#     Score current inventory for stockout risk.
#     Saves risk scores locally as CSV.
#     """
#     print("\n" + "=" * 60)
#     print("  STOCKOUT RISK — PREDICT & WRITEBACK")
#     print("=" * 60)
    
#     start = time.time()
    
#     # Load best model
#     model = load_model('stockout_best')
#     meta = load_metadata('stockout_best')
#     features = meta.get('features', stockout_features())
    
#     print(f"  Using model: {meta.get('model_name', 'unknown')}")
    
#     # Connect to Snowflake
#     conn = get_snowflake_connection()
    
#     # Get dynamic date range — only need recent data for risk scoring
#     max_date, predict_start, pull_start = get_data_date_range(
#         conn, 'INTERMEDIATE.INT_INVENTORY_ENRICHED', 'SNAPSHOT_DATE'
#     )
#     # For stockout, pull last 30 days + 14 day lookback for features
#     stockout_pull_start = max_date - pd.Timedelta(days=44)
#     stockout_predict_start = max_date - pd.Timedelta(days=30)
    
#     print(f"\n  Data range: up to {max_date.date()}")
#     print(f"  Scoring window: {stockout_predict_start.date()} → {max_date.date()}")
    
#     # Pull data
#     inventory = pd.read_sql(
#         f"SELECT * FROM INTERMEDIATE.INT_INVENTORY_ENRICHED WHERE SNAPSHOT_DATE >= '{stockout_pull_start.date()}'",
#         conn
#     )
#     inventory.columns = [c.lower() for c in inventory.columns]
#     print(f"  Loaded {len(inventory):,} inventory rows")
    
#     dates = pd.read_sql("SELECT * FROM STAGING.STG_DATES", conn)
#     dates.columns = [c.lower() for c in dates.columns]
    
#     products = pd.read_sql("SELECT * FROM STAGING.STG_PRODUCTS WHERE IS_CURRENT = TRUE", conn)
#     products.columns = [c.lower() for c in products.columns]
    
#     if len(inventory) == 0:
#         print("  No data found. Skipping.")
#         conn.close()
#         return
    
#     # Build features
#     print("  Building features...")
#     df = build_stockout_features(inventory, dates, products)
    
#     # Filter to prediction window
#     df['snapshot_date'] = pd.to_datetime(df['snapshot_date'])
#     df_predict = df[df['snapshot_date'] >= stockout_predict_start].copy()
    
#     if len(df_predict) == 0:
#         print("  No rows in prediction window. Skipping.")
#         conn.close()
#         return
    
#     print(f"  Scoring {len(df_predict):,} inventory rows")
    
#     available_features = [f for f in features if f in df_predict.columns]
#     X = df_predict[available_features].fillna(0)
    
#     # Generate risk scores
#     print("  Scoring stockout risk...")
#     if hasattr(model, 'predict_proba'):
#         df_predict.loc[:, 'stockout_risk_score'] = model.predict_proba(X)[:, 1].round(4)
#     else:
#         df_predict.loc[:, 'stockout_risk_score'] = model.predict(X)
    
#     df_predict.loc[:, 'stockout_prediction'] = (df_predict['stockout_risk_score'] >= 0.5).astype(int)
    
#     # Summary
#     high_risk = df_predict[df_predict['stockout_risk_score'] >= 0.7]
#     medium_risk = df_predict[(df_predict['stockout_risk_score'] >= 0.3) & (df_predict['stockout_risk_score'] < 0.7)]
#     low_risk = df_predict[df_predict['stockout_risk_score'] < 0.3]
    
#     print(f"\n  Risk Distribution:")
#     print(f"    High Risk (>= 0.7):   {len(high_risk):>8,} ({len(high_risk)/len(df_predict)*100:.1f}%)")
#     print(f"    Medium Risk (0.3-0.7): {len(medium_risk):>8,} ({len(medium_risk)/len(df_predict)*100:.1f}%)")
#     print(f"    Low Risk (< 0.3):      {len(low_risk):>8,} ({len(low_risk)/len(df_predict)*100:.1f}%)")
    
#     # Save predictions
#     output_path = 'ml/results/stockout_risk_latest.csv'
#     os.makedirs('ml/results', exist_ok=True)
    
#     output_cols = ['snapshot_date', 'warehouse_id', 'product_id', 'closing_stock',
#                    'days_of_supply', 'stockout_risk_score', 'stockout_prediction']
#     available_output = [c for c in output_cols if c in df_predict.columns]
#     df_predict[available_output].to_csv(output_path, index=False)
    
#     # Top 20 highest risk
#     print(f"\n  Top 20 Highest Risk Products:")
#     top_risk = df_predict.nlargest(20, 'stockout_risk_score')[available_output]
#     print(top_risk.to_string(index=False))
    
#     conn.close()
    
#     elapsed = time.time() - start
#     print(f"\n  ✓ Risk scores saved to {output_path}")
#     print(f"  ✓ {len(df_predict):,} rows scored in {elapsed:.0f}s")


# def run_writeback(phases: list = None):
#     """Run prediction and writeback for specified phases."""
#     all_phases = ['demand', 'eta', 'stockout']
#     phases = phases or all_phases
    
#     print("=" * 60)
#     print("  FULFILLMENT PLATFORM — PREDICTION & WRITEBACK")
#     print(f"  Phases: {', '.join(phases)}")
#     print("=" * 60)
    
#     total_start = time.time()
    
#     if 'demand' in phases:
#         predict_demand()
    
#     if 'eta' in phases:
#         predict_eta()
    
#     if 'stockout' in phases:
#         predict_stockout()
    
#     total_elapsed = time.time() - total_start
#     print(f"\n{'=' * 60}")
#     print(f"  All predictions complete in {total_elapsed:.0f}s")
#     print(f"{'=' * 60}")


# if __name__ == "__main__":
#     import argparse
    
#     parser = argparse.ArgumentParser(description='ML Prediction & Writeback')
#     parser.add_argument(
#         '--phase',
#         nargs='+',
#         choices=['demand', 'eta', 'stockout'],
#         default=None,
#         help='Run specific phases. Default: all.'
#     )
    
#     args = parser.parse_args()
#     run_writeback(phases=args.phase)

# new code 2/27/202 12.07 AM
"""
Prediction & Writeback Pipeline.
Uses saved best models to:
1. Forecast demand and write to mart_daily_product_kpis (bulk MERGE)
2. Predict ETA for deliveries (save to CSV)
3. Score stockout risk for inventory (save to CSV)

Usage:
    python -m ml.training.predict_and_writeback
    python -m ml.training.predict_and_writeback --phase demand
    python -m ml.training.predict_and_writeback --phase eta stockout
"""

import os
import sys
import time
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from ml.training.save_models import load_model, load_metadata
from ml.features.demand_features import build_demand_features, get_feature_columns as demand_features
from ml.features.eta_features import build_eta_features, get_feature_columns as eta_features
from ml.features.stockout_features import build_stockout_features, get_feature_columns as stockout_features

# Lookback: extra days pulled for lag feature computation
LOOKBACK_DAYS = 60
# Predict window: predict for the last N days of available data
PREDICT_WINDOW_DAYS = 90


def get_snowflake_connection():
    """Create Snowflake connection."""
    from dotenv import load_dotenv
    import snowflake.connector

    load_dotenv()

    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        database=os.getenv('SNOWFLAKE_DATABASE', 'FULFILLMENT_DB'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'FULFILLMENT_WH'),
    )


def get_data_date_range(conn, table: str, date_col: str) -> tuple:
    """Get max date from a table and compute predict/pull start dates."""
    result = pd.read_sql(f"SELECT MAX({date_col}) as max_date FROM {table}", conn)
    max_date = pd.to_datetime(result['MAX_DATE'].iloc[0])
    predict_start = max_date - pd.Timedelta(days=PREDICT_WINDOW_DAYS)
    pull_start = predict_start - pd.Timedelta(days=LOOKBACK_DAYS)
    return max_date, predict_start, pull_start


def predict_demand():
    """
    Generate demand forecasts and bulk MERGE into mart_daily_product_kpis.
    Updates demand_forecast and forecast_error columns.
    """
    print("\n" + "=" * 60)
    print("  DEMAND FORECAST — PREDICT & WRITEBACK")
    print("=" * 60)

    start = time.time()

    # Load best model
    model = load_model('demand_best')
    meta = load_metadata('demand_best')
    features = meta.get('features', demand_features())

    print(f"  Using model: {meta.get('model_name', 'unknown')}")
    print(f"  Metrics: {meta.get('metrics', {})}")

    # Connect to Snowflake
    conn = get_snowflake_connection()
    cur = conn.cursor()

    # Get dynamic date range
    max_date, predict_start, pull_start = get_data_date_range(
        conn, 'MARTS.MART_DAILY_PRODUCT_KPIS', 'DATE'
    )
    print(f"\n  Data range: up to {max_date.date()}")
    print(f"  Predict window: {predict_start.date()} → {max_date.date()}")
    print(f"  Pulling data from: {pull_start.date()} (includes {LOOKBACK_DAYS}d lookback for lags)")

    # Pull data
    product_kpis = pd.read_sql(
        f"SELECT * FROM MARTS.MART_DAILY_PRODUCT_KPIS WHERE DATE >= '{pull_start.date()}'",
        conn
    )
    product_kpis.columns = [c.lower() for c in product_kpis.columns]
    print(f"  Loaded {len(product_kpis):,} rows from product_kpis")

    dates = pd.read_sql("SELECT * FROM STAGING.STG_DATES", conn)
    dates.columns = [c.lower() for c in dates.columns]

    products = pd.read_sql("SELECT * FROM STAGING.STG_PRODUCTS WHERE IS_CURRENT = TRUE", conn)
    products.columns = [c.lower() for c in products.columns]

    if len(product_kpis) == 0:
        print("  No data found. Skipping.")
        conn.close()
        return

    # Build features
    print("  Building features...")
    df = build_demand_features(product_kpis, dates, products)

    # Only predict for the target window
    df['date'] = pd.to_datetime(df['date'])
    df_predict = df[df['date'] >= predict_start].copy()

    if len(df_predict) == 0:
        print("  No rows in prediction window after feature engineering. Skipping.")
        conn.close()
        return

    print(f"  Predicting for {len(df_predict):,} rows")

    available_features = [f for f in features if f in df_predict.columns]
    X = df_predict[available_features].fillna(0)

    # Generate predictions
    print("  Generating predictions...")
    df_predict.loc[:, 'demand_forecast'] = np.maximum(model.predict(X), 0).round(2)
    df_predict.loc[:, 'forecast_error'] = (df_predict['total_units_sold'] - df_predict['demand_forecast']).round(2)

    # Calculate MAPE
    mask = df_predict['total_units_sold'] > 0
    mape = np.mean(np.abs(df_predict.loc[mask, 'forecast_error'] / df_predict.loc[mask, 'total_units_sold'])) * 100
    print(f"  Prediction MAPE: {mape:.2f}%")

    # ── Bulk write to Snowflake using MERGE ──
    print("  Writing predictions to Snowflake (bulk MERGE)...")

    temp_path = os.path.abspath('ml/results/_temp_demand_predictions.csv').replace('\\', '/')
    os.makedirs('ml/results', exist_ok=True)

    writeback_df = df_predict[['date', 'product_id', 'demand_forecast', 'forecast_error']].copy()
    writeback_df['date'] = writeback_df['date'].dt.strftime('%Y-%m-%d')
    writeback_df.to_csv(temp_path, index=False)
    print(f"  Saved {len(writeback_df):,} predictions to temp CSV")

    cur.execute("USE SCHEMA MARTS")

    cur.execute("""
        CREATE OR REPLACE TEMPORARY TABLE _temp_demand_predictions (
            date DATE,
            product_id VARCHAR(20),
            demand_forecast DECIMAL(8,2),
            forecast_error DECIMAL(8,2)
        )
    """)

    cur.execute(f"PUT file://{temp_path} @%_temp_demand_predictions AUTO_COMPRESS=TRUE")

    cur.execute("""
        COPY INTO _temp_demand_predictions
        FROM @%_temp_demand_predictions
        FILE_FORMAT = (TYPE='CSV' SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"')
    """)

    cur.execute("""
        MERGE INTO MART_DAILY_PRODUCT_KPIS t
        USING _temp_demand_predictions s
        ON t.DATE = s.DATE AND t.PRODUCT_ID = s.PRODUCT_ID
        WHEN MATCHED THEN UPDATE SET
            t.DEMAND_FORECAST = s.DEMAND_FORECAST,
            t.FORECAST_ERROR = s.FORECAST_ERROR
    """)

    rows_updated = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()

    # Cleanup
    try:
        os.remove(temp_path)
    except:
        pass

    elapsed = time.time() - start
    print(f"\n  ✓ Merged {rows_updated:,} rows into mart_daily_product_kpis")
    print(f"  ✓ Completed in {elapsed:.0f}s")

    # Print sample
    sample = df_predict[['date', 'product_id', 'total_units_sold', 'demand_forecast', 'forecast_error']].tail(10)
    print(f"\n  Sample predictions:")
    print(sample.to_string(index=False))


def predict_eta():
    """
    Generate ETA predictions for recent deliveries.
    Saves predictions locally as CSV.
    """
    print("\n" + "=" * 60)
    print("  ETA PREDICTION — PREDICT & WRITEBACK")
    print("=" * 60)

    start = time.time()

    # Load best model
    model = load_model('eta_best')
    meta = load_metadata('eta_best')
    features = meta.get('features', eta_features())

    print(f"  Using model: {meta.get('model_name', 'unknown')}")

    # Connect to Snowflake
    conn = get_snowflake_connection()

    # Get dynamic date range
    max_date, predict_start, pull_start = get_data_date_range(
        conn, 'INTERMEDIATE.INT_DELIVERY_ENRICHED', 'DELIVERY_DATE'
    )
    print(f"\n  Data range: up to {max_date.date()}")
    print(f"  Predict window: {predict_start.date()} → {max_date.date()}")

    # Pull data
    print("  Pulling deliveries from Snowflake...")
    deliveries = pd.read_sql(
        f"SELECT * FROM INTERMEDIATE.INT_DELIVERY_ENRICHED WHERE DELIVERY_DATE >= '{predict_start.date()}'",
        conn
    )
    deliveries.columns = [c.lower() for c in deliveries.columns]
    print(f"  Loaded {len(deliveries):,} deliveries")

    dates = pd.read_sql("SELECT * FROM STAGING.STG_DATES", conn)
    dates.columns = [c.lower() for c in dates.columns]

    if len(deliveries) == 0:
        print("  No data found. Skipping.")
        conn.close()
        return

    # Build features
    print("  Building features...")
    df = build_eta_features(deliveries, dates)

    if len(df) == 0:
        print("  No delivered orders in window. Skipping.")
        conn.close()
        return

    print(f"  Predicting for {len(df):,} deliveries")

    available_features = [f for f in features if f in df.columns]
    X = df[available_features].fillna(0)

    # Generate predictions
    print("  Generating ETA predictions...")
    df.loc[:, 'predicted_eta'] = np.maximum(model.predict(X), 1).round(2)
    df.loc[:, 'eta_error'] = (df['actual_delivery_minutes'] - df['predicted_eta']).round(2)

    # Metrics
    mask = df['actual_delivery_minutes'] > 0
    mape = np.mean(np.abs(df.loc[mask, 'eta_error'] / df.loc[mask, 'actual_delivery_minutes'])) * 100
    print(f"\n  ETA Prediction MAPE: {mape:.2f}%")
    print(f"  Mean ETA Error: {df['eta_error'].mean():.1f} minutes")
    print(f"  Median ETA Error: {df['eta_error'].median():.1f} minutes")

    # Save predictions locally
    output_path = 'ml/results/eta_predictions_latest.csv'
    os.makedirs('ml/results', exist_ok=True)
    output_cols = ['delivery_id', 'order_id', 'actual_delivery_minutes', 'predicted_eta', 'eta_error']
    available_output = [c for c in output_cols if c in df.columns]
    df[available_output].to_csv(output_path, index=False)

    conn.close()

    elapsed = time.time() - start
    print(f"\n  ✓ Predictions saved to {output_path}")
    print(f"  ✓ {len(df):,} deliveries scored in {elapsed:.0f}s")


def predict_stockout():
    """
    Score current inventory for stockout risk.
    Saves risk scores locally as CSV.
    """
    print("\n" + "=" * 60)
    print("  STOCKOUT RISK — PREDICT & WRITEBACK")
    print("=" * 60)

    start = time.time()

    # Load best model
    model = load_model('stockout_best')
    meta = load_metadata('stockout_best')
    features = meta.get('features', stockout_features())

    print(f"  Using model: {meta.get('model_name', 'unknown')}")

    # Connect to Snowflake
    conn = get_snowflake_connection()

    # Get dynamic date range
    max_date, _, _ = get_data_date_range(
        conn, 'INTERMEDIATE.INT_INVENTORY_ENRICHED', 'SNAPSHOT_DATE'
    )
    stockout_pull_start = max_date - pd.Timedelta(days=44)
    stockout_predict_start = max_date - pd.Timedelta(days=30)

    print(f"\n  Data range: up to {max_date.date()}")
    print(f"  Scoring window: {stockout_predict_start.date()} → {max_date.date()}")

    # Pull data
    print("  Pulling inventory from Snowflake...")
    inventory = pd.read_sql(
        f"SELECT * FROM INTERMEDIATE.INT_INVENTORY_ENRICHED WHERE SNAPSHOT_DATE >= '{stockout_pull_start.date()}'",
        conn
    )
    inventory.columns = [c.lower() for c in inventory.columns]
    print(f"  Loaded {len(inventory):,} inventory rows")

    dates = pd.read_sql("SELECT * FROM STAGING.STG_DATES", conn)
    dates.columns = [c.lower() for c in dates.columns]

    products = pd.read_sql("SELECT * FROM STAGING.STG_PRODUCTS WHERE IS_CURRENT = TRUE", conn)
    products.columns = [c.lower() for c in products.columns]

    if len(inventory) == 0:
        print("  No data found. Skipping.")
        conn.close()
        return

    # Build features
    print("  Building features...")
    df = build_stockout_features(inventory, dates, products)

    # Filter to prediction window
    df['snapshot_date'] = pd.to_datetime(df['snapshot_date'])
    df_predict = df[df['snapshot_date'] >= stockout_predict_start].copy()

    if len(df_predict) == 0:
        print("  No rows in prediction window. Skipping.")
        conn.close()
        return

    print(f"  Scoring {len(df_predict):,} inventory rows")

    available_features = [f for f in features if f in df_predict.columns]
    X = df_predict[available_features].fillna(0)

    # Generate risk scores
    print("  Scoring stockout risk...")
    if hasattr(model, 'predict_proba'):
        df_predict.loc[:, 'stockout_risk_score'] = model.predict_proba(X)[:, 1].round(4)
    else:
        df_predict.loc[:, 'stockout_risk_score'] = model.predict(X)

    df_predict.loc[:, 'stockout_prediction'] = (df_predict['stockout_risk_score'] >= 0.5).astype(int)

    # Summary
    high_risk = df_predict[df_predict['stockout_risk_score'] >= 0.7]
    medium_risk = df_predict[(df_predict['stockout_risk_score'] >= 0.3) & (df_predict['stockout_risk_score'] < 0.7)]
    low_risk = df_predict[df_predict['stockout_risk_score'] < 0.3]

    print(f"\n  Risk Distribution:")
    print(f"    High Risk (>= 0.7):   {len(high_risk):>8,} ({len(high_risk)/len(df_predict)*100:.1f}%)")
    print(f"    Medium Risk (0.3-0.7): {len(medium_risk):>8,} ({len(medium_risk)/len(df_predict)*100:.1f}%)")
    print(f"    Low Risk (< 0.3):      {len(low_risk):>8,} ({len(low_risk)/len(df_predict)*100:.1f}%)")

    # Save predictions
    output_path = 'ml/results/stockout_risk_latest.csv'
    os.makedirs('ml/results', exist_ok=True)

    output_cols = ['snapshot_date', 'warehouse_id', 'product_id', 'closing_stock',
                   'days_of_supply', 'stockout_risk_score', 'stockout_prediction']
    available_output = [c for c in output_cols if c in df_predict.columns]
    df_predict[available_output].to_csv(output_path, index=False)

    # Top 20 highest risk
    print(f"\n  Top 20 Highest Risk Products:")
    top_risk = df_predict.nlargest(20, 'stockout_risk_score')[available_output]
    print(top_risk.to_string(index=False))

    conn.close()

    elapsed = time.time() - start
    print(f"\n  ✓ Risk scores saved to {output_path}")
    print(f"  ✓ {len(df_predict):,} rows scored in {elapsed:.0f}s")


def run_writeback(phases: list = None):
    """Run prediction and writeback for specified phases."""
    all_phases = ['demand', 'eta', 'stockout']
    phases = phases or all_phases

    print("=" * 60)
    print("  FULFILLMENT PLATFORM — PREDICTION & WRITEBACK")
    print(f"  Phases: {', '.join(phases)}")
    print("=" * 60)

    total_start = time.time()

    if 'demand' in phases:
        predict_demand()

    if 'eta' in phases:
        predict_eta()

    if 'stockout' in phases:
        predict_stockout()

    total_elapsed = time.time() - total_start
    print(f"\n{'=' * 60}")
    print(f"  All predictions complete in {total_elapsed:.0f}s")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='ML Prediction & Writeback')
    parser.add_argument(
        '--phase',
        nargs='+',
        choices=['demand', 'eta', 'stockout'],
        default=None,
        help='Run specific phases. Default: all.'
    )

    args = parser.parse_args()
    run_writeback(phases=args.phase)