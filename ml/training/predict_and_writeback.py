# # """
# # Prediction & Writeback Pipeline.
# # Uses saved best models to generate predictions and MERGE all results into Snowflake marts.

# # Writes to:
# #   - mart_daily_product_kpis   : demand_forecast, forecast_error, stockout_risk_score
# #   - mart_delivery_performance : predicted_eta, eta_error

# # Usage:
# #     python -m ml.training.predict_and_writeback
# #     python -m ml.training.predict_and_writeback --phase demand
# #     python -m ml.training.predict_and_writeback --phase eta stockout
# # """

# # import os
# # import sys
# # import time
# # import numpy as np
# # import pandas as pd
# # import warnings
# # warnings.filterwarnings('ignore')

# # sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# # from ml.training.save_models import load_model, load_metadata
# # from ml.features.demand_features import build_demand_features, get_feature_columns as demand_features
# # from ml.features.eta_features import build_eta_features, get_feature_columns as eta_features
# # from ml.features.stockout_features import build_stockout_features, get_feature_columns as stockout_features

# # # Lookback: extra days pulled for lag feature computation (demand only)
# # LOOKBACK_DAYS = 60


# # # ─────────────────────────────────────────────────────────────
# # #  CONNECTION
# # # ─────────────────────────────────────────────────────────────

# # def get_snowflake_connection():
# #     """Create Snowflake connection from .env credentials."""
# #     from dotenv import load_dotenv
# #     import snowflake.connector

# #     load_dotenv()

# #     return snowflake.connector.connect(
# #         account=os.getenv('SNOWFLAKE_ACCOUNT'),
# #         user=os.getenv('SNOWFLAKE_USER'),
# #         password=os.getenv('SNOWFLAKE_PASSWORD'),
# #         database=os.getenv('SNOWFLAKE_DATABASE', 'FULFILLMENT_DB'),
# #         warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'FULFILLMENT_WH'),
# #     )


# # def get_max_date(conn, table: str, date_col: str) -> pd.Timestamp:
# #     """Get max date from a table."""
# #     result = pd.read_sql(f"SELECT MAX({date_col}) as max_date FROM {table}", conn)
# #     return pd.to_datetime(result['MAX_DATE'].iloc[0])


# # # ─────────────────────────────────────────────────────────────
# # #  SHARED MERGE HELPER
# # # ─────────────────────────────────────────────────────────────

# # def bulk_merge(cur, df: pd.DataFrame, temp_table: str, temp_ddl: str,
# #                target_table: str, merge_sql: str, temp_path: str) -> int:
# #     """
# #     Generic bulk MERGE pattern:
# #       1. Write df to temp CSV
# #       2. PUT to Snowflake stage
# #       3. COPY INTO temp table
# #       4. MERGE into target mart
# #     Returns number of rows updated.
# #     """
# #     os.makedirs(os.path.dirname(temp_path), exist_ok=True)
# #     df.to_csv(temp_path, index=False)

# #     abs_path = os.path.abspath(temp_path).replace('\\', '/')

# #     cur.execute(f"CREATE OR REPLACE TEMPORARY TABLE {temp_table} ({temp_ddl})")
# #     cur.execute(f"PUT file://{abs_path} @%{temp_table} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
# #     cur.execute(f"""
# #         COPY INTO {temp_table}
# #         FROM @%{temp_table}
# #         FILE_FORMAT = (TYPE='CSV' SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"')
# #     """)
# #     cur.execute(merge_sql)
# #     rows_updated = cur.rowcount

# #     try:
# #         os.remove(temp_path)
# #     except OSError:
# #         pass

# #     return rows_updated


# # # ─────────────────────────────────────────────────────────────
# # #  DEMAND FORECAST
# # # ─────────────────────────────────────────────────────────────

# # def predict_demand():
# #     """
# #     Generate demand forecasts and bulk MERGE into mart_daily_product_kpis.
# #     Updates: demand_forecast, forecast_error
# #     """
# #     print("\n" + "=" * 60)
# #     print("  DEMAND FORECAST — PREDICT & WRITEBACK")
# #     print("=" * 60)
# #     start = time.time()

# #     model = load_model('demand_best')
# #     meta = load_metadata('demand_best')
# #     features = meta.get('features', demand_features())
# #     print(f"  Model  : {meta.get('model_name', 'unknown')}")
# #     print(f"  Metrics: {meta.get('metrics', {})}")

# #     conn = get_snowflake_connection()
# #     cur = conn.cursor()

# #     max_date = get_max_date(conn, 'MARTS.MART_DAILY_PRODUCT_KPIS', 'DATE')
# #     min_date = pd.Timestamp('2022-02-01')
# #     # Pull from 60 days before the start so lag features are valid from day 1
# #     pull_start = min_date - pd.Timedelta(days=LOOKBACK_DAYS)

# #     print(f"\n  Scoring full history: {min_date.date()} → {max_date.date()}")
# #     print(f"  Pull from: {pull_start.date()} (includes {LOOKBACK_DAYS}d lookback for lag features)")

# #     product_kpis = pd.read_sql(
# #         f"SELECT * FROM MARTS.MART_DAILY_PRODUCT_KPIS WHERE DATE >= '{pull_start.date()}'", conn
# #     )
# #     product_kpis.columns = [c.lower() for c in product_kpis.columns]

# #     dates = pd.read_sql("SELECT * FROM STAGING.STG_DATES", conn)
# #     dates.columns = [c.lower() for c in dates.columns]

# #     products = pd.read_sql("SELECT * FROM STAGING.STG_PRODUCTS WHERE IS_CURRENT = TRUE", conn)
# #     products.columns = [c.lower() for c in products.columns]

# #     if len(product_kpis) == 0:
# #         print("  No data found. Skipping.")
# #         conn.close()
# #         return

# #     print(f"  Loaded {len(product_kpis):,} rows from product_kpis")
# #     print("  Building features...")
# #     df = build_demand_features(product_kpis, dates, products)

# #     df['date'] = pd.to_datetime(df['date'])
# #     # Exclude the lookback buffer rows — only score from the actual data start
# #     df_predict = df[df['date'] >= min_date].copy()

# #     if len(df_predict) == 0:
# #         print("  No rows in prediction window after feature engineering. Skipping.")
# #         conn.close()
# #         return

# #     print(f"  Predicting for {len(df_predict):,} rows")
# #     available_features = [f for f in features if f in df_predict.columns]
# #     X = df_predict[available_features].fillna(0)

# #     df_predict['demand_forecast'] = np.maximum(model.predict(X), 0).round(2)
# #     df_predict['forecast_error'] = (df_predict['total_units_sold'] - df_predict['demand_forecast']).round(2)

# #     mask = df_predict['total_units_sold'] > 0
# #     mape = np.mean(np.abs(df_predict.loc[mask, 'forecast_error'] / df_predict.loc[mask, 'total_units_sold'])) * 100
# #     print(f"  MAPE: {mape:.2f}%")

# #     writeback = df_predict[['date', 'product_id', 'demand_forecast', 'forecast_error']].copy()
# #     writeback['date'] = writeback['date'].dt.strftime('%Y-%m-%d')

# #     cur.execute("USE SCHEMA MARTS")
# #     rows_updated = bulk_merge(
# #         cur=cur,
# #         df=writeback,
# #         temp_table="_temp_demand_predictions",
# #         temp_ddl="date DATE, product_id VARCHAR(20), demand_forecast DECIMAL(8,2), forecast_error DECIMAL(8,2)",
# #         target_table="MART_DAILY_PRODUCT_KPIS",
# #         merge_sql="""
# #             MERGE INTO MART_DAILY_PRODUCT_KPIS t
# #             USING _temp_demand_predictions s
# #             ON t.DATE = s.DATE AND t.PRODUCT_ID = s.PRODUCT_ID
# #             WHEN MATCHED THEN UPDATE SET
# #                 t.DEMAND_FORECAST = s.DEMAND_FORECAST,
# #                 t.FORECAST_ERROR  = s.FORECAST_ERROR
# #         """,
# #         temp_path='ml/results/_temp_demand_predictions.csv'
# #     )
# #     conn.commit()

# #     print(f"\n  Sample predictions:")
# #     print(df_predict[['date', 'product_id', 'total_units_sold', 'demand_forecast', 'forecast_error']].tail(10).to_string(index=False))

# #     cur.close()
# #     conn.close()
# #     print(f"\n  ✓ Merged {rows_updated:,} rows into mart_daily_product_kpis")
# #     print(f"  ✓ Completed in {time.time() - start:.0f}s")


# # # ─────────────────────────────────────────────────────────────
# # #  ETA PREDICTION
# # # ─────────────────────────────────────────────────────────────

# # def predict_eta():
# #     """
# #     Generate ETA predictions and bulk MERGE into mart_delivery_performance.
# #     Updates: predicted_eta, eta_error  (aggregated per warehouse per day)

# #     Processes year by year to avoid loading 5M+ rows at once.
# #     Each year is pulled, scored, aggregated to warehouse × day, and merged
# #     into mart_delivery_performance independently.
# #     """
# #     print("\n" + "=" * 60)
# #     print("  ETA PREDICTION — PREDICT & WRITEBACK")
# #     print("=" * 60)
# #     start = time.time()

# #     model = load_model('eta_best')
# #     meta = load_metadata('eta_best')
# #     features = meta.get('features', eta_features())
# #     print(f"  Model: {meta.get('model_name', 'unknown')}")
# #     print(f"  Metrics: {meta.get('metrics', {})}")

# #     conn = get_snowflake_connection()
# #     cur = conn.cursor()

# #     # Load dates once — reused across all year chunks
# #     print("\n  Loading date dimension...")
# #     dates = pd.read_sql("SELECT * FROM STAGING.STG_DATES", conn)
# #     dates.columns = [c.lower() for c in dates.columns]
# #     print(f"  Loaded {len(dates):,} date rows")

# #     # Year chunks: Feb 2022 → Feb 2025
# #     year_chunks = [
# #         ('2022-02-01', '2022-12-31'),
# #         ('2023-01-01', '2023-12-31'),
# #         ('2024-01-01', '2024-12-31'),
# #         ('2025-01-01', '2025-02-01'),
# #     ]

# #     total_deliveries_scored = 0
# #     total_rows_merged = 0
# #     all_mapes = []

# #     cur.execute("USE SCHEMA MARTS")

# #     for i, (chunk_start, chunk_end) in enumerate(year_chunks, 1):
# #         chunk_timer = time.time()
# #         print(f"\n  ── Chunk {i}/{len(year_chunks)}: {chunk_start} → {chunk_end} ──")

# #         # Pull deliveries for this chunk
# #         print(f"  Pulling deliveries from Snowflake...")
# #         chunk_df = pd.read_sql(
# #             f"""
# #             SELECT * FROM INTERMEDIATE.INT_DELIVERY_ENRICHED
# #             WHERE DELIVERY_DATE >= '{chunk_start}'
# #               AND DELIVERY_DATE <= '{chunk_end}'
# #             """,
# #             conn
# #         )
# #         chunk_df.columns = [c.lower() for c in chunk_df.columns]
# #         print(f"  Loaded {len(chunk_df):,} deliveries")

# #         if len(chunk_df) == 0:
# #             print(f"  No data for this chunk. Skipping.")
# #             continue

# #         # Build features
# #         print(f"  Building features...")
# #         df = build_eta_features(chunk_df, dates)

# #         if len(df) == 0:
# #             print(f"  No delivered orders after feature filtering. Skipping.")
# #             continue

# #         # Score
# #         print(f"  Scoring {len(df):,} delivered orders...")
# #         available_features = [f for f in features if f in df.columns]
# #         X = df[available_features].fillna(0)

# #         df['predicted_eta'] = np.maximum(model.predict(X), 1).round(2)
# #         df['eta_error'] = (df['actual_delivery_minutes'] - df['predicted_eta']).round(2)

# #         # Chunk MAPE
# #         mask = df['actual_delivery_minutes'] > 0
# #         chunk_mape = np.mean(
# #             np.abs(df.loc[mask, 'eta_error'] / df.loc[mask, 'actual_delivery_minutes'])
# #         ) * 100
# #         all_mapes.append(chunk_mape)
# #         print(f"  Chunk MAPE: {chunk_mape:.2f}%")

# #         # Aggregate to warehouse × day grain
# #         date_col = 'delivery_date' if 'delivery_date' in df.columns else 'order_date'
# #         warehouse_col = 'warehouse_id' if 'warehouse_id' in df.columns else 'assigned_warehouse_id'

# #         agg = (
# #             df.groupby([date_col, warehouse_col])
# #             .agg(
# #                 predicted_eta=('predicted_eta', 'mean'),
# #                 eta_error=('eta_error', 'mean')
# #             )
# #             .reset_index()
# #             .rename(columns={date_col: 'date', warehouse_col: 'warehouse_id'})
# #         )
# #         agg['predicted_eta'] = agg['predicted_eta'].round(2)
# #         agg['eta_error'] = agg['eta_error'].round(2)
# #         agg['date'] = pd.to_datetime(agg['date']).dt.strftime('%Y-%m-%d')
# #         print(f"  Aggregated to {len(agg):,} warehouse × day rows")

# #         # Merge this chunk into mart
# #         rows_updated = bulk_merge(
# #             cur=cur,
# #             df=agg,
# #             temp_table="_temp_eta_predictions",
# #             temp_ddl="date DATE, warehouse_id VARCHAR(20), predicted_eta DECIMAL(6,2), eta_error DECIMAL(6,2)",
# #             target_table="MART_DELIVERY_PERFORMANCE",
# #             merge_sql="""
# #                 MERGE INTO MART_DELIVERY_PERFORMANCE t
# #                 USING _temp_eta_predictions s
# #                 ON t.DATE = s.DATE AND t.WAREHOUSE_ID = s.WAREHOUSE_ID
# #                 WHEN MATCHED THEN UPDATE SET
# #                     t.PREDICTED_ETA = s.PREDICTED_ETA,
# #                     t.ETA_ERROR      = s.ETA_ERROR
# #             """,
# #             temp_path='ml/results/_temp_eta_predictions.csv'
# #         )
# #         conn.commit()

# #         total_deliveries_scored += len(df)
# #         total_rows_merged += rows_updated
# #         print(f"  ✓ Merged {rows_updated:,} rows | Chunk time: {time.time() - chunk_timer:.0f}s")

# #     cur.close()
# #     conn.close()

# #     overall_mape = np.mean(all_mapes) if all_mapes else 0
# #     print(f"\n  Overall MAPE    : {overall_mape:.2f}%")
# #     print(f"  Total deliveries: {total_deliveries_scored:,}")
# #     print(f"\n  ✓ Total merged {total_rows_merged:,} rows into mart_delivery_performance")
# #     print(f"  ✓ Completed in {time.time() - start:.0f}s")


# # # ─────────────────────────────────────────────────────────────
# # #  STOCKOUT RISK
# # # ─────────────────────────────────────────────────────────────

# # def predict_stockout():
# #     """
# #     Score current inventory for stockout risk and bulk MERGE into mart_daily_product_kpis.
# #     Updates: stockout_risk_score

# #     mart_daily_product_kpis is product × day.
# #     We aggregate stockout_risk_score as MAX per product per day across warehouses,
# #     representing the worst-case risk for that product on that day.
# #     """
# #     print("\n" + "=" * 60)
# #     print("  STOCKOUT RISK — PREDICT & WRITEBACK")
# #     print("=" * 60)
# #     start = time.time()

# #     model = load_model('stockout_best')
# #     meta = load_metadata('stockout_best')
# #     features = meta.get('features', stockout_features())
# #     print(f"  Model: {meta.get('model_name', 'unknown')}")

# #     conn = get_snowflake_connection()
# #     cur = conn.cursor()

# #     # Pull everything — 60d lookback buffer for lag features, score from data start
# #     min_date = pd.Timestamp('2022-02-01')
# #     pull_start = min_date - pd.Timedelta(days=60)

# #     print(f"\n  Scoring full history: {min_date.date()} → 2025-02-01")
# #     print(f"  Pull from: {pull_start.date()} (includes 60d lookback for lag features)")

# #     inventory = pd.read_sql(
# #         f"SELECT * FROM INTERMEDIATE.INT_INVENTORY_ENRICHED WHERE SNAPSHOT_DATE >= '{pull_start.date()}'",
# #         conn
# #     )
# #     inventory.columns = [c.lower() for c in inventory.columns]
# #     print(f"  Loaded {len(inventory):,} inventory rows")

# #     dates = pd.read_sql("SELECT * FROM STAGING.STG_DATES", conn)
# #     dates.columns = [c.lower() for c in dates.columns]

# #     products = pd.read_sql("SELECT * FROM STAGING.STG_PRODUCTS WHERE IS_CURRENT = TRUE", conn)
# #     products.columns = [c.lower() for c in products.columns]

# #     if len(inventory) == 0:
# #         print("  No data found. Skipping.")
# #         conn.close()
# #         return

# #     print("  Building features...")
# #     df = build_stockout_features(inventory, dates, products)

# #     df['snapshot_date'] = pd.to_datetime(df['snapshot_date'])
# #     # Exclude the lookback buffer — only score from actual data start
# #     df_predict = df[df['snapshot_date'] >= min_date].copy()

# #     if len(df_predict) == 0:
# #         print("  No rows in prediction window. Skipping.")
# #         conn.close()
# #         return

# #     print(f"  Scoring {len(df_predict):,} inventory rows")
# #     available_features = [f for f in features if f in df_predict.columns]
# #     X = df_predict[available_features].fillna(0)

# #     if hasattr(model, 'predict_proba'):
# #         df_predict['stockout_risk_score'] = model.predict_proba(X)[:, 1].round(4)
# #     else:
# #         df_predict['stockout_risk_score'] = model.predict(X)

# #     # Risk distribution summary
# #     high   = (df_predict['stockout_risk_score'] >= 0.7).sum()
# #     medium = ((df_predict['stockout_risk_score'] >= 0.3) & (df_predict['stockout_risk_score'] < 0.7)).sum()
# #     low    = (df_predict['stockout_risk_score'] < 0.3).sum()
# #     n = len(df_predict)
# #     print(f"\n  Risk distribution:")
# #     print(f"    High   (≥0.7) : {high:>8,} ({high/n*100:.1f}%)")
# #     print(f"    Medium (0.3–0.7): {medium:>8,} ({medium/n*100:.1f}%)")
# #     print(f"    Low    (<0.3) : {low:>8,} ({low/n*100:.1f}%)")

# #     # Aggregate to product × day grain (MAX risk score across warehouses)
# #     # mart_daily_product_kpis is product × day, not product × warehouse × day
# #     agg = (
# #         df_predict.groupby(['snapshot_date', 'product_id'])
# #         ['stockout_risk_score']
# #         .max()
# #         .reset_index()
# #         .rename(columns={'snapshot_date': 'date'})
# #     )
# #     agg['stockout_risk_score'] = agg['stockout_risk_score'].round(4)
# #     agg['date'] = agg['date'].dt.strftime('%Y-%m-%d')

# #     print(f"\n  Aggregated to {len(agg):,} product × day rows for mart")

# #     print(f"\n  Top 10 highest risk products:")
# #     top = df_predict.nlargest(10, 'stockout_risk_score')[
# #         ['snapshot_date', 'warehouse_id', 'product_id', 'closing_stock', 'days_of_supply', 'stockout_risk_score']
# #     ]
# #     print(top.to_string(index=False))

# #     cur.execute("USE SCHEMA MARTS")
# #     rows_updated = bulk_merge(
# #         cur=cur,
# #         df=agg,
# #         temp_table="_temp_stockout_risk",
# #         temp_ddl="date DATE, product_id VARCHAR(20), stockout_risk_score DECIMAL(6,4)",
# #         target_table="MART_DAILY_PRODUCT_KPIS",
# #         merge_sql="""
# #             MERGE INTO MART_DAILY_PRODUCT_KPIS t
# #             USING _temp_stockout_risk s
# #             ON t.DATE = s.DATE AND t.PRODUCT_ID = s.PRODUCT_ID
# #             WHEN MATCHED THEN UPDATE SET
# #                 t.STOCKOUT_RISK_SCORE = s.STOCKOUT_RISK_SCORE
# #         """,
# #         temp_path='ml/results/_temp_stockout_risk.csv'
# #     )
# #     conn.commit()
# #     cur.close()
# #     conn.close()

# #     print(f"\n  ✓ Merged {rows_updated:,} rows into mart_daily_product_kpis")
# #     print(f"  ✓ Completed in {time.time() - start:.0f}s")


# # # ─────────────────────────────────────────────────────────────
# # #  ENTRY POINT
# # # ─────────────────────────────────────────────────────────────

# # def run_writeback(phases: list = None):
# #     all_phases = ['demand', 'eta', 'stockout']
# #     phases = phases or all_phases

# #     print("=" * 60)
# #     print("  FULFILLMENT PLATFORM — PREDICTION & WRITEBACK")
# #     print(f"  Phases: {', '.join(phases)}")
# #     print("=" * 60)

# #     total_start = time.time()

# #     if 'demand' in phases:
# #         predict_demand()

# #     if 'eta' in phases:
# #         predict_eta()

# #     if 'stockout' in phases:
# #         predict_stockout()

# #     print(f"\n{'=' * 60}")
# #     print(f"  All predictions complete in {time.time() - total_start:.0f}s")
# #     print(f"{'=' * 60}")


# # if __name__ == "__main__":
# #     import argparse

# #     parser = argparse.ArgumentParser(description='ML Prediction & Writeback')
# #     parser.add_argument(
# #         '--phase',
# #         nargs='+',
# #         choices=['demand', 'eta', 'stockout'],
# #         default=None,
# #         help='Run specific phases (default: all)'
# #     )
# #     args = parser.parse_args()
# #     run_writeback(phases=args.phase)

# """
# Prediction & Writeback Pipeline.
# Uses saved best models to generate predictions and MERGE all results into Snowflake marts.

# Writes to:
#   - mart_daily_product_kpis   : demand_forecast, forecast_error, stockout_risk_score
#   - mart_delivery_performance : predicted_eta, eta_error

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

# # Lookback: extra days pulled for lag feature computation (demand only)
# LOOKBACK_DAYS = 60


# # ─────────────────────────────────────────────────────────────
# #  CONNECTION
# # ─────────────────────────────────────────────────────────────

# def get_snowflake_connection():
#     """Create Snowflake connection from .env credentials."""
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


# def fast_query(conn, sql: str) -> pd.DataFrame:
#     """
#     Fetch Snowflake query results via Arrow batches — 5-10x faster than pd.read_sql
#     for large tables. Falls back to fetchall if Arrow is unavailable.
#     """
#     cur = conn.cursor()
#     try:
#         cur.execute(sql)
#         try:
#             import pyarrow as pa
#             batches = cur.fetch_arrow_batches()
#             table = pa.concat_tables(list(batches))
#             df = table.to_pandas()
#         except Exception:
#             df = pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])
#     finally:
#         cur.close()
#     df.columns = [c.lower() for c in df.columns]
#     return df


# def get_max_date(conn, table: str, date_col: str) -> pd.Timestamp:
#     """Get max date from a table."""
#     result = fast_query(conn, f"SELECT MAX({date_col}) as max_date FROM {table}")
#     return pd.to_datetime(result['max_date'].iloc[0])


# # ─────────────────────────────────────────────────────────────
# #  SHARED MERGE HELPER
# # ─────────────────────────────────────────────────────────────

# def bulk_merge(cur, df: pd.DataFrame, temp_table: str, temp_ddl: str,
#                target_table: str, merge_sql: str, temp_path: str) -> int:
#     """
#     Generic bulk MERGE pattern:
#       1. Write df to temp CSV
#       2. PUT to Snowflake stage
#       3. COPY INTO temp table
#       4. MERGE into target mart
#     Returns number of rows updated.
#     """
#     os.makedirs(os.path.dirname(temp_path), exist_ok=True)
#     df.to_csv(temp_path, index=False)

#     abs_path = os.path.abspath(temp_path).replace('\\', '/')

#     cur.execute(f"CREATE OR REPLACE TEMPORARY TABLE {temp_table} ({temp_ddl})")
#     cur.execute(f"PUT file://{abs_path} @%{temp_table} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
#     cur.execute(f"""
#         COPY INTO {temp_table}
#         FROM @%{temp_table}
#         FILE_FORMAT = (TYPE='CSV' SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"')
#     """)
#     cur.execute(merge_sql)
#     rows_updated = cur.rowcount

#     try:
#         os.remove(temp_path)
#     except OSError:
#         pass

#     return rows_updated


# # ─────────────────────────────────────────────────────────────
# #  DEMAND FORECAST
# # ─────────────────────────────────────────────────────────────

# def predict_demand():
#     """
#     Generate demand forecasts and bulk MERGE into mart_daily_product_kpis.
#     Updates: demand_forecast, forecast_error
#     """
#     print("\n" + "=" * 60)
#     print("  DEMAND FORECAST — PREDICT & WRITEBACK")
#     print("=" * 60)
#     start = time.time()

#     model = load_model('demand_best')
#     meta = load_metadata('demand_best')
#     features = meta.get('features', demand_features())
#     print(f"  Model  : {meta.get('model_name', 'unknown')}")
#     print(f"  Metrics: {meta.get('metrics', {})}")

#     conn = get_snowflake_connection()
#     cur = conn.cursor()

#     max_date = get_max_date(conn, 'MARTS.MART_DAILY_PRODUCT_KPIS', 'DATE')
#     min_date = pd.Timestamp('2022-02-01')
#     # Pull from 60 days before the start so lag features are valid from day 1
#     pull_start = min_date - pd.Timedelta(days=LOOKBACK_DAYS)

#     print(f"\n  Scoring full history: {min_date.date()} → {max_date.date()}")
#     print(f"  Pull from: {pull_start.date()} (includes {LOOKBACK_DAYS}d lookback for lag features)")

#     product_kpis = fast_query(conn, f"SELECT * FROM MARTS.MART_DAILY_PRODUCT_KPIS WHERE DATE >= '{pull_start.date()}'")

#     dates = fast_query(conn, "SELECT * FROM STAGING.STG_DATES")

#     products = fast_query(conn, "SELECT * FROM STAGING.STG_PRODUCTS WHERE IS_CURRENT = TRUE")

#     if len(product_kpis) == 0:
#         print("  No data found. Skipping.")
#         conn.close()
#         return

#     print(f"  Loaded {len(product_kpis):,} rows from product_kpis")
#     print("  Building features...")
#     df = build_demand_features(product_kpis, dates, products)

#     df['date'] = pd.to_datetime(df['date'])
#     # Exclude the lookback buffer rows — only score from the actual data start
#     df_predict = df[df['date'] >= min_date].copy()

#     if len(df_predict) == 0:
#         print("  No rows in prediction window after feature engineering. Skipping.")
#         conn.close()
#         return

#     print(f"  Predicting for {len(df_predict):,} rows")
#     available_features = [f for f in features if f in df_predict.columns]
#     X = df_predict[available_features].fillna(0)

#     df_predict['demand_forecast'] = np.maximum(model.predict(X), 0).round(2)
#     df_predict['forecast_error'] = (df_predict['total_units_sold'] - df_predict['demand_forecast']).round(2)

#     mask = df_predict['total_units_sold'] > 0
#     mape = np.mean(np.abs(df_predict.loc[mask, 'forecast_error'] / df_predict.loc[mask, 'total_units_sold'])) * 100
#     print(f"  MAPE: {mape:.2f}%")

#     writeback = df_predict[['date', 'product_id', 'demand_forecast', 'forecast_error']].copy()
#     writeback['date'] = writeback['date'].dt.strftime('%Y-%m-%d')

#     cur.execute("USE SCHEMA MARTS")
#     rows_updated = bulk_merge(
#         cur=cur,
#         df=writeback,
#         temp_table="_temp_demand_predictions",
#         temp_ddl="date DATE, product_id VARCHAR(20), demand_forecast DECIMAL(8,2), forecast_error DECIMAL(8,2)",
#         target_table="MART_DAILY_PRODUCT_KPIS",
#         merge_sql="""
#             MERGE INTO MART_DAILY_PRODUCT_KPIS t
#             USING _temp_demand_predictions s
#             ON t.DATE = s.DATE AND t.PRODUCT_ID = s.PRODUCT_ID
#             WHEN MATCHED THEN UPDATE SET
#                 t.DEMAND_FORECAST = s.DEMAND_FORECAST,
#                 t.FORECAST_ERROR  = s.FORECAST_ERROR
#         """,
#         temp_path='ml/results/_temp_demand_predictions.csv'
#     )
#     conn.commit()

#     print(f"\n  Sample predictions:")
#     print(df_predict[['date', 'product_id', 'total_units_sold', 'demand_forecast', 'forecast_error']].tail(10).to_string(index=False))

#     cur.close()
#     conn.close()
#     print(f"\n  ✓ Merged {rows_updated:,} rows into mart_daily_product_kpis")
#     print(f"  ✓ Completed in {time.time() - start:.0f}s")


# # ─────────────────────────────────────────────────────────────
# #  ETA PREDICTION
# # ─────────────────────────────────────────────────────────────

# def predict_eta():
#     """
#     Generate ETA predictions and bulk MERGE into mart_delivery_performance.
#     Updates: predicted_eta, eta_error  (aggregated per warehouse per day)

#     Processes year by year to avoid loading 5M+ rows at once.
#     Each year is pulled, scored, aggregated to warehouse × day, and merged
#     into mart_delivery_performance independently.
#     """
#     print("\n" + "=" * 60)
#     print("  ETA PREDICTION — PREDICT & WRITEBACK")
#     print("=" * 60)
#     start = time.time()

#     model = load_model('eta_best')
#     meta = load_metadata('eta_best')
#     features = meta.get('features', eta_features())
#     print(f"  Model: {meta.get('model_name', 'unknown')}")
#     print(f"  Metrics: {meta.get('metrics', {})}")

#     conn = get_snowflake_connection()
#     cur = conn.cursor()

#     # Load dates once — reused across all year chunks
#     print("\n  Loading date dimension...")
#     dates = fast_query(conn, "SELECT * FROM STAGING.STG_DATES")
#     print(f"  Loaded {len(dates):,} date rows")

#     # Year chunks: Feb 2022 → Feb 2025
#     year_chunks = [
#         ('2022-02-01', '2022-12-31'),
#         ('2023-01-01', '2023-12-31'),
#         ('2024-01-01', '2024-12-31'),
#         ('2025-01-01', '2025-02-01'),
#     ]

#     total_deliveries_scored = 0
#     total_rows_merged = 0
#     all_mapes = []

#     cur.execute("USE SCHEMA MARTS")

#     for i, (chunk_start, chunk_end) in enumerate(year_chunks, 1):
#         chunk_timer = time.time()
#         print(f"\n  ── Chunk {i}/{len(year_chunks)}: {chunk_start} → {chunk_end} ──")

#         # Pull deliveries for this chunk
#         print(f"  Pulling deliveries from Snowflake...")
#         chunk_df = fast_query(conn, f"SELECT * FROM INTERMEDIATE.INT_DELIVERY_ENRICHED WHERE DELIVERY_DATE >= '{chunk_start}' AND DELIVERY_DATE <= '{chunk_end}'")
#         print(f"  Loaded {len(chunk_df):,} deliveries")

#         if len(chunk_df) == 0:
#             print(f"  No data for this chunk. Skipping.")
#             continue

#         # Build features
#         print(f"  Building features...")
#         df = build_eta_features(chunk_df, dates)

#         if len(df) == 0:
#             print(f"  No delivered orders after feature filtering. Skipping.")
#             continue

#         # Score
#         print(f"  Scoring {len(df):,} delivered orders...")
#         available_features = [f for f in features if f in df.columns]
#         X = df[available_features].fillna(0)

#         df['predicted_eta'] = np.maximum(model.predict(X), 1).round(2)
#         df['eta_error'] = (df['actual_delivery_minutes'] - df['predicted_eta']).round(2)

#         # Chunk MAPE
#         mask = df['actual_delivery_minutes'] > 0
#         chunk_mape = np.mean(
#             np.abs(df.loc[mask, 'eta_error'] / df.loc[mask, 'actual_delivery_minutes'])
#         ) * 100
#         all_mapes.append(chunk_mape)
#         print(f"  Chunk MAPE: {chunk_mape:.2f}%")

#         # Aggregate to warehouse × day grain
#         date_col = 'delivery_date' if 'delivery_date' in df.columns else 'order_date'
#         warehouse_col = 'warehouse_id' if 'warehouse_id' in df.columns else 'assigned_warehouse_id'

#         agg = (
#             df.groupby([date_col, warehouse_col])
#             .agg(
#                 predicted_eta=('predicted_eta', 'mean'),
#                 eta_error=('eta_error', 'mean')
#             )
#             .reset_index()
#             .rename(columns={date_col: 'date', warehouse_col: 'warehouse_id'})
#         )
#         agg['predicted_eta'] = agg['predicted_eta'].round(2)
#         agg['eta_error'] = agg['eta_error'].round(2)
#         agg['date'] = pd.to_datetime(agg['date']).dt.strftime('%Y-%m-%d')
#         print(f"  Aggregated to {len(agg):,} warehouse × day rows")

#         # Merge this chunk into mart
#         rows_updated = bulk_merge(
#             cur=cur,
#             df=agg,
#             temp_table="_temp_eta_predictions",
#             temp_ddl="date DATE, warehouse_id VARCHAR(20), predicted_eta DECIMAL(6,2), eta_error DECIMAL(6,2)",
#             target_table="MART_DELIVERY_PERFORMANCE",
#             merge_sql="""
#                 MERGE INTO MART_DELIVERY_PERFORMANCE t
#                 USING _temp_eta_predictions s
#                 ON t.DATE = s.DATE AND t.WAREHOUSE_ID = s.WAREHOUSE_ID
#                 WHEN MATCHED THEN UPDATE SET
#                     t.PREDICTED_ETA = s.PREDICTED_ETA,
#                     t.ETA_ERROR      = s.ETA_ERROR
#             """,
#             temp_path='ml/results/_temp_eta_predictions.csv'
#         )
#         conn.commit()

#         total_deliveries_scored += len(df)
#         total_rows_merged += rows_updated
#         print(f"  ✓ Merged {rows_updated:,} rows | Chunk time: {time.time() - chunk_timer:.0f}s")

#     cur.close()
#     conn.close()

#     overall_mape = np.mean(all_mapes) if all_mapes else 0
#     print(f"\n  Overall MAPE    : {overall_mape:.2f}%")
#     print(f"  Total deliveries: {total_deliveries_scored:,}")
#     print(f"\n  ✓ Total merged {total_rows_merged:,} rows into mart_delivery_performance")
#     print(f"  ✓ Completed in {time.time() - start:.0f}s")


# # ─────────────────────────────────────────────────────────────
# #  STOCKOUT RISK
# # ─────────────────────────────────────────────────────────────

# def predict_stockout():
#     """
#     Score current inventory for stockout risk and bulk MERGE into mart_daily_product_kpis.
#     Updates: stockout_risk_score

#     mart_daily_product_kpis is product × day.
#     We aggregate stockout_risk_score as MAX per product per day across warehouses,
#     representing the worst-case risk for that product on that day.
#     """
#     print("\n" + "=" * 60)
#     print("  STOCKOUT RISK — PREDICT & WRITEBACK")
#     print("=" * 60)
#     start = time.time()

#     model = load_model('stockout_best')
#     meta = load_metadata('stockout_best')
#     features = meta.get('features', stockout_features())
#     print(f"  Model: {meta.get('model_name', 'unknown')}")

#     conn = get_snowflake_connection()
#     cur = conn.cursor()

#     # Pull everything — 60d lookback buffer for lag features, score from data start
#     min_date = pd.Timestamp('2022-02-01')
#     pull_start = min_date - pd.Timedelta(days=60)

#     print(f"\n  Scoring full history: {min_date.date()} → 2025-02-01")
#     print(f"  Pull from: {pull_start.date()} (includes 60d lookback for lag features)")

#     inventory = fast_query(conn, f"SELECT * FROM INTERMEDIATE.INT_INVENTORY_ENRICHED WHERE SNAPSHOT_DATE >= '{pull_start.date()}'")
#     print(f"  Loaded {len(inventory):,} inventory rows")

#     dates = fast_query(conn, "SELECT * FROM STAGING.STG_DATES")

#     products = fast_query(conn, "SELECT * FROM STAGING.STG_PRODUCTS WHERE IS_CURRENT = TRUE")

#     if len(inventory) == 0:
#         print("  No data found. Skipping.")
#         conn.close()
#         return

#     print("  Building features...")
#     df = build_stockout_features(inventory, dates, products)

#     df['snapshot_date'] = pd.to_datetime(df['snapshot_date'])
#     # Exclude the lookback buffer — only score from actual data start
#     df_predict = df[df['snapshot_date'] >= min_date].copy()

#     if len(df_predict) == 0:
#         print("  No rows in prediction window. Skipping.")
#         conn.close()
#         return

#     print(f"  Scoring {len(df_predict):,} inventory rows")
#     available_features = [f for f in features if f in df_predict.columns]
#     X = df_predict[available_features].fillna(0)

#     if hasattr(model, 'predict_proba'):
#         df_predict['stockout_risk_score'] = model.predict_proba(X)[:, 1].round(4)
#     else:
#         df_predict['stockout_risk_score'] = model.predict(X)

#     # Risk distribution summary
#     high   = (df_predict['stockout_risk_score'] >= 0.7).sum()
#     medium = ((df_predict['stockout_risk_score'] >= 0.3) & (df_predict['stockout_risk_score'] < 0.7)).sum()
#     low    = (df_predict['stockout_risk_score'] < 0.3).sum()
#     n = len(df_predict)
#     print(f"\n  Risk distribution:")
#     print(f"    High   (≥0.7) : {high:>8,} ({high/n*100:.1f}%)")
#     print(f"    Medium (0.3–0.7): {medium:>8,} ({medium/n*100:.1f}%)")
#     print(f"    Low    (<0.3) : {low:>8,} ({low/n*100:.1f}%)")

#     # Aggregate to product × day grain (MAX risk score across warehouses)
#     # mart_daily_product_kpis is product × day, not product × warehouse × day
#     agg = (
#         df_predict.groupby(['snapshot_date', 'product_id'])
#         ['stockout_risk_score']
#         .max()
#         .reset_index()
#         .rename(columns={'snapshot_date': 'date'})
#     )
#     agg['stockout_risk_score'] = agg['stockout_risk_score'].round(4)
#     agg['date'] = agg['date'].dt.strftime('%Y-%m-%d')

#     print(f"\n  Aggregated to {len(agg):,} product × day rows for mart")

#     print(f"\n  Top 10 highest risk products:")
#     top = df_predict.nlargest(10, 'stockout_risk_score')[
#         ['snapshot_date', 'warehouse_id', 'product_id', 'closing_stock', 'days_of_supply', 'stockout_risk_score']
#     ]
#     print(top.to_string(index=False))

#     cur.execute("USE SCHEMA MARTS")
#     rows_updated = bulk_merge(
#         cur=cur,
#         df=agg,
#         temp_table="_temp_stockout_risk",
#         temp_ddl="date DATE, product_id VARCHAR(20), stockout_risk_score DECIMAL(6,4)",
#         target_table="MART_DAILY_PRODUCT_KPIS",
#         merge_sql="""
#             MERGE INTO MART_DAILY_PRODUCT_KPIS t
#             USING _temp_stockout_risk s
#             ON t.DATE = s.DATE AND t.PRODUCT_ID = s.PRODUCT_ID
#             WHEN MATCHED THEN UPDATE SET
#                 t.STOCKOUT_RISK_SCORE = s.STOCKOUT_RISK_SCORE
#         """,
#         temp_path='ml/results/_temp_stockout_risk.csv'
#     )
#     conn.commit()
#     cur.close()
#     conn.close()

#     print(f"\n  ✓ Merged {rows_updated:,} rows into mart_daily_product_kpis")
#     print(f"  ✓ Completed in {time.time() - start:.0f}s")


# # ─────────────────────────────────────────────────────────────
# #  ENTRY POINT
# # ─────────────────────────────────────────────────────────────

# def run_writeback(phases: list = None):
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

#     print(f"\n{'=' * 60}")
#     print(f"  All predictions complete in {time.time() - total_start:.0f}s")
#     print(f"{'=' * 60}")


# if __name__ == "__main__":
#     import argparse

#     parser = argparse.ArgumentParser(description='ML Prediction & Writeback')
#     parser.add_argument(
#         '--phase',
#         nargs='+',
#         choices=['demand', 'eta', 'stockout'],
#         default=None,
#         help='Run specific phases (default: all)'
#     )
#     args = parser.parse_args()
#     run_writeback(phases=args.phase)
"""
Prediction & Writeback Pipeline.
Uses saved best models to generate predictions and MERGE all results into Snowflake marts.

Writes to:
  - mart_daily_product_kpis   : demand_forecast, forecast_error, stockout_risk_score
  - mart_delivery_performance : predicted_eta, eta_error

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

# Lookback: extra days pulled for lag feature computation (demand only)
LOOKBACK_DAYS = 60


# ─────────────────────────────────────────────────────────────
#  CONNECTION
# ─────────────────────────────────────────────────────────────

def get_snowflake_connection():
    """Create Snowflake connection from .env credentials."""
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


def fast_query(conn, sql: str) -> pd.DataFrame:
    """
    Fetch Snowflake query results via Arrow batches — 5-10x faster than pd.read_sql
    for large tables. Falls back to fetchall if Arrow is unavailable.
    """
    cur = conn.cursor()
    try:
        cur.execute(sql)
        try:
            import pyarrow as pa
            batches = cur.fetch_arrow_batches()
            table = pa.concat_tables(list(batches))
            df = table.to_pandas()
        except Exception:
            df = pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])
    finally:
        cur.close()
    df.columns = [c.lower() for c in df.columns]
    return df


def get_max_date(conn, table: str, date_col: str) -> pd.Timestamp:
    """Get max date from a table."""
    result = fast_query(conn, f"SELECT MAX({date_col}) as max_date FROM {table}")
    return pd.to_datetime(result['max_date'].iloc[0])


# ─────────────────────────────────────────────────────────────
#  SHARED MERGE HELPER
# ─────────────────────────────────────────────────────────────

def bulk_merge(cur, df: pd.DataFrame, temp_table: str, temp_ddl: str,
               target_table: str, merge_sql: str, temp_path: str) -> int:
    """
    Generic bulk MERGE pattern:
      1. Write df to temp CSV
      2. PUT to Snowflake stage
      3. COPY INTO temp table
      4. MERGE into target mart
    Returns number of rows updated.
    """
    os.makedirs(os.path.dirname(temp_path), exist_ok=True)
    df.to_csv(temp_path, index=False)

    abs_path = os.path.abspath(temp_path).replace('\\', '/')

    cur.execute(f"CREATE OR REPLACE TEMPORARY TABLE {temp_table} ({temp_ddl})")
    cur.execute(f"PUT file://{abs_path} @%{temp_table} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
    cur.execute(f"""
        COPY INTO {temp_table}
        FROM @%{temp_table}
        FILE_FORMAT = (TYPE='CSV' SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"')
    """)
    cur.execute(merge_sql)
    rows_updated = cur.rowcount

    try:
        os.remove(temp_path)
    except OSError:
        pass

    return rows_updated


# ─────────────────────────────────────────────────────────────
#  DEMAND FORECAST
# ─────────────────────────────────────────────────────────────

def predict_demand():
    """
    Generate demand forecasts and bulk MERGE into mart_daily_product_kpis.
    Updates: demand_forecast, forecast_error
    """
    print("\n" + "=" * 60)
    print("  DEMAND FORECAST — PREDICT & WRITEBACK")
    print("=" * 60)
    start = time.time()

    model = load_model('demand_best')
    meta = load_metadata('demand_best')
    features = meta.get('features', demand_features())
    print(f"  Model  : {meta.get('model_name', 'unknown')}")
    print(f"  Metrics: {meta.get('metrics', {})}")

    conn = get_snowflake_connection()
    cur = conn.cursor()

    max_date = get_max_date(conn, 'MARTS.MART_DAILY_PRODUCT_KPIS', 'DATE')
    min_date = pd.Timestamp('2022-02-01')
    # Pull from 60 days before the start so lag features are valid from day 1
    pull_start = min_date - pd.Timedelta(days=LOOKBACK_DAYS)

    print(f"\n  Scoring full history: {min_date.date()} → {max_date.date()}")
    print(f"  Pull from: {pull_start.date()} (includes {LOOKBACK_DAYS}d lookback for lag features)")

    product_kpis = fast_query(conn, f"SELECT * FROM MARTS.MART_DAILY_PRODUCT_KPIS WHERE DATE >= '{pull_start.date()}'")

    dates = fast_query(conn, "SELECT * FROM STAGING.STG_DATES")

    products = fast_query(conn, "SELECT * FROM STAGING.STG_PRODUCTS WHERE IS_CURRENT = TRUE")

    if len(product_kpis) == 0:
        print("  No data found. Skipping.")
        conn.close()
        return

    print(f"  Loaded {len(product_kpis):,} rows from product_kpis")
    print("  Building features...")
    df = build_demand_features(product_kpis, dates, products)

    df['date'] = pd.to_datetime(df['date'])
    # Exclude the lookback buffer rows — only score from the actual data start
    df_predict = df[df['date'] >= min_date].copy()

    if len(df_predict) == 0:
        print("  No rows in prediction window after feature engineering. Skipping.")
        conn.close()
        return

    print(f"  Predicting for {len(df_predict):,} rows")
    available_features = [f for f in features if f in df_predict.columns]
    X = df_predict[available_features].fillna(0)

    df_predict['demand_forecast'] = np.maximum(model.predict(X), 0).round(2)
    df_predict['forecast_error'] = (df_predict['total_units_sold'] - df_predict['demand_forecast']).round(2)

    mask = df_predict['total_units_sold'] > 0
    mape = np.mean(np.abs(df_predict.loc[mask, 'forecast_error'] / df_predict.loc[mask, 'total_units_sold'])) * 100
    print(f"  MAPE: {mape:.2f}%")

    writeback = df_predict[['date', 'product_id', 'demand_forecast', 'forecast_error']].copy()
    writeback['date'] = writeback['date'].dt.strftime('%Y-%m-%d')

    cur.execute("USE SCHEMA MARTS")
    rows_updated = bulk_merge(
        cur=cur,
        df=writeback,
        temp_table="_temp_demand_predictions",
        temp_ddl="date DATE, product_id VARCHAR(20), demand_forecast DECIMAL(8,2), forecast_error DECIMAL(8,2)",
        target_table="MART_DAILY_PRODUCT_KPIS",
        merge_sql="""
            MERGE INTO MART_DAILY_PRODUCT_KPIS t
            USING _temp_demand_predictions s
            ON t.DATE = s.DATE AND t.PRODUCT_ID = s.PRODUCT_ID
            WHEN MATCHED THEN UPDATE SET
                t.DEMAND_FORECAST = s.DEMAND_FORECAST,
                t.FORECAST_ERROR  = s.FORECAST_ERROR
        """,
        temp_path='ml/results/_temp_demand_predictions.csv'
    )
    conn.commit()

    print(f"\n  Sample predictions:")
    print(df_predict[['date', 'product_id', 'total_units_sold', 'demand_forecast', 'forecast_error']].tail(10).to_string(index=False))

    cur.close()
    conn.close()
    print(f"\n  ✓ Merged {rows_updated:,} rows into mart_daily_product_kpis")
    print(f"  ✓ Completed in {time.time() - start:.0f}s")


# ─────────────────────────────────────────────────────────────
#  ETA PREDICTION
# ─────────────────────────────────────────────────────────────

def predict_eta():
    """
    Generate ETA predictions and bulk MERGE into mart_delivery_performance.
    Updates: predicted_eta, eta_error  (aggregated per warehouse per day)

    Processes year by year to avoid loading 5M+ rows at once.
    Each year is pulled, scored, aggregated to warehouse × day, and merged
    into mart_delivery_performance independently.
    """
    print("\n" + "=" * 60)
    print("  ETA PREDICTION — PREDICT & WRITEBACK")
    print("=" * 60)
    start = time.time()

    model = load_model('eta_best')
    meta = load_metadata('eta_best')
    features = meta.get('features', eta_features())
    print(f"  Model: {meta.get('model_name', 'unknown')}")
    print(f"  Metrics: {meta.get('metrics', {})}")

    conn = get_snowflake_connection()
    cur = conn.cursor()

    # Load dates once — reused across all year chunks
    print("\n  Loading date dimension...")
    dates = fast_query(conn, "SELECT * FROM STAGING.STG_DATES")
    print(f"  Loaded {len(dates):,} date rows")

    # Year chunks: Feb 2022 → Feb 2025
    year_chunks = [
        ('2022-02-01', '2022-12-31'),
        ('2023-01-01', '2023-12-31'),
        ('2024-01-01', '2024-12-31'),
        ('2025-01-01', '2025-02-01'),
    ]

    total_deliveries_scored = 0
    total_rows_merged = 0
    all_mapes = []

    cur.execute("USE SCHEMA MARTS")

    for i, (chunk_start, chunk_end) in enumerate(year_chunks, 1):
        chunk_timer = time.time()
        print(f"\n  ── Chunk {i}/{len(year_chunks)}: {chunk_start} → {chunk_end} ──")

        # Pull deliveries for this chunk
        print(f"  Pulling deliveries from Snowflake...")
        chunk_df = fast_query(conn, f"SELECT * FROM INTERMEDIATE.INT_DELIVERY_ENRICHED WHERE DELIVERY_DATE >= '{chunk_start}' AND DELIVERY_DATE <= '{chunk_end}'")
        print(f"  Loaded {len(chunk_df):,} deliveries")

        if len(chunk_df) == 0:
            print(f"  No data for this chunk. Skipping.")
            continue

        # Build features
        print(f"  Building features...")
        df = build_eta_features(chunk_df, dates)

        if len(df) == 0:
            print(f"  No delivered orders after feature filtering. Skipping.")
            continue

        # Score
        print(f"  Scoring {len(df):,} delivered orders...")
        available_features = [f for f in features if f in df.columns]
        X = df[available_features].fillna(0)

        df['predicted_eta'] = np.maximum(model.predict(X), 1).round(2)
        df['eta_error'] = (df['actual_delivery_minutes'] - df['predicted_eta']).round(2)

        # Chunk MAPE
        mask = df['actual_delivery_minutes'] > 0
        chunk_mape = np.mean(
            np.abs(df.loc[mask, 'eta_error'] / df.loc[mask, 'actual_delivery_minutes'])
        ) * 100
        all_mapes.append(chunk_mape)
        print(f"  Chunk MAPE: {chunk_mape:.2f}%")

        # Aggregate to warehouse × day grain
        date_col = 'delivery_date' if 'delivery_date' in df.columns else 'order_date'
        warehouse_col = 'warehouse_id' if 'warehouse_id' in df.columns else 'assigned_warehouse_id'

        agg = (
            df.groupby([date_col, warehouse_col])
            .agg(
                predicted_eta=('predicted_eta', 'mean'),
                eta_error=('eta_error', 'mean')
            )
            .reset_index()
            .rename(columns={date_col: 'date', warehouse_col: 'warehouse_id'})
        )
        agg['predicted_eta'] = agg['predicted_eta'].round(2)
        agg['eta_error'] = agg['eta_error'].round(2)
        agg['date'] = pd.to_datetime(agg['date']).dt.strftime('%Y-%m-%d')
        print(f"  Aggregated to {len(agg):,} warehouse × day rows")

        # Merge this chunk into mart
        rows_updated = bulk_merge(
            cur=cur,
            df=agg,
            temp_table="_temp_eta_predictions",
            temp_ddl="date DATE, warehouse_id VARCHAR(20), predicted_eta DECIMAL(6,2), eta_error DECIMAL(6,2)",
            target_table="MART_DELIVERY_PERFORMANCE",
            merge_sql="""
                MERGE INTO MART_DELIVERY_PERFORMANCE t
                USING _temp_eta_predictions s
                ON t.DATE = s.DATE AND t.WAREHOUSE_ID = s.WAREHOUSE_ID
                WHEN MATCHED THEN UPDATE SET
                    t.PREDICTED_ETA = s.PREDICTED_ETA,
                    t.ETA_ERROR      = s.ETA_ERROR
            """,
            temp_path='ml/results/_temp_eta_predictions.csv'
        )
        conn.commit()

        total_deliveries_scored += len(df)
        total_rows_merged += rows_updated
        print(f"  ✓ Merged {rows_updated:,} rows | Chunk time: {time.time() - chunk_timer:.0f}s")

    cur.close()
    conn.close()

    overall_mape = np.mean(all_mapes) if all_mapes else 0
    print(f"\n  Overall MAPE    : {overall_mape:.2f}%")
    print(f"  Total deliveries: {total_deliveries_scored:,}")
    print(f"\n  ✓ Total merged {total_rows_merged:,} rows into mart_delivery_performance")
    print(f"  ✓ Completed in {time.time() - start:.0f}s")


# ─────────────────────────────────────────────────────────────
#  STOCKOUT RISK
# ─────────────────────────────────────────────────────────────

def predict_stockout():
    """
    Score current inventory for stockout risk and bulk MERGE into mart_daily_product_kpis.
    Updates: stockout_risk_score

    mart_daily_product_kpis is product × day.
    We aggregate stockout_risk_score as MAX per product per day across warehouses,
    representing the worst-case risk for that product on that day.
    """
    print("\n" + "=" * 60)
    print("  STOCKOUT RISK — PREDICT & WRITEBACK")
    print("=" * 60)
    start = time.time()

    model = load_model('stockout_best')
    meta = load_metadata('stockout_best')
    features = meta.get('features', stockout_features())
    print(f"  Model: {meta.get('model_name', 'unknown')}")

    conn = get_snowflake_connection()
    cur = conn.cursor()

    # Pull everything — 60d lookback buffer for lag features, score from data start
    min_date = pd.Timestamp('2022-02-01')
    pull_start = min_date - pd.Timedelta(days=60)

    print(f"\n  Scoring full history: {min_date.date()} → 2025-02-01")
    print(f"  Pull from: {pull_start.date()} (includes 60d lookback for lag features)")

    inventory = fast_query(conn, f"SELECT * FROM INTERMEDIATE.INT_INVENTORY_ENRICHED WHERE SNAPSHOT_DATE >= '{pull_start.date()}'")
    print(f"  Loaded {len(inventory):,} inventory rows")

    dates = fast_query(conn, "SELECT * FROM STAGING.STG_DATES")

    products = fast_query(conn, "SELECT * FROM STAGING.STG_PRODUCTS WHERE IS_CURRENT = TRUE")

    if len(inventory) == 0:
        print("  No data found. Skipping.")
        conn.close()
        return

    print("  Building features...")
    df = build_stockout_features(inventory, dates, products)

    df['snapshot_date'] = pd.to_datetime(df['snapshot_date'])
    # Exclude the lookback buffer — only score from actual data start
    df_predict = df[df['snapshot_date'] >= min_date].copy()

    if len(df_predict) == 0:
        print("  No rows in prediction window. Skipping.")
        conn.close()
        return

    print(f"  Scoring {len(df_predict):,} inventory rows")
    available_features = [f for f in features if f in df_predict.columns]
    X = df_predict[available_features].fillna(0)

    if hasattr(model, 'predict_proba'):
        df_predict['stockout_risk_score'] = model.predict_proba(X)[:, 1].round(4)
    else:
        df_predict['stockout_risk_score'] = model.predict(X)

    # Risk distribution summary
    high   = (df_predict['stockout_risk_score'] >= 0.7).sum()
    medium = ((df_predict['stockout_risk_score'] >= 0.3) & (df_predict['stockout_risk_score'] < 0.7)).sum()
    low    = (df_predict['stockout_risk_score'] < 0.3).sum()
    n = len(df_predict)
    print(f"\n  Risk distribution:")
    print(f"    High   (≥0.7) : {high:>8,} ({high/n*100:.1f}%)")
    print(f"    Medium (0.3–0.7): {medium:>8,} ({medium/n*100:.1f}%)")
    print(f"    Low    (<0.3) : {low:>8,} ({low/n*100:.1f}%)")

    # Aggregate to product × day grain (MAX risk score across warehouses)
    # mart_daily_product_kpis is product × day, not product × warehouse × day
    agg = (
        df_predict.groupby(['snapshot_date', 'product_id'])
        ['stockout_risk_score']
        .max()
        .reset_index()
        .rename(columns={'snapshot_date': 'date'})
    )
    agg['stockout_risk_score'] = agg['stockout_risk_score'].round(4)
    agg['date'] = agg['date'].dt.strftime('%Y-%m-%d')

    print(f"\n  Aggregated to {len(agg):,} product × day rows for mart")

    print(f"\n  Top 10 highest risk products:")
    top = df_predict.nlargest(10, 'stockout_risk_score')[
        ['snapshot_date', 'warehouse_id', 'product_id', 'closing_stock', 'days_of_supply', 'stockout_risk_score']
    ]
    print(top.to_string(index=False))

    cur.execute("USE SCHEMA MARTS")
    rows_updated = bulk_merge(
        cur=cur,
        df=agg,
        temp_table="_temp_stockout_risk",
        temp_ddl="date DATE, product_id VARCHAR(20), stockout_risk_score DECIMAL(6,4)",
        target_table="MART_DAILY_PRODUCT_KPIS",
        merge_sql="""
            MERGE INTO MART_DAILY_PRODUCT_KPIS t
            USING _temp_stockout_risk s
            ON t.DATE = s.DATE AND t.PRODUCT_ID = s.PRODUCT_ID
            WHEN MATCHED THEN UPDATE SET
                t.STOCKOUT_RISK_SCORE = s.STOCKOUT_RISK_SCORE
        """,
        temp_path='ml/results/_temp_stockout_risk.csv'
    )
    conn.commit()
    cur.close()
    conn.close()

    print(f"\n  ✓ Merged {rows_updated:,} rows into mart_daily_product_kpis")
    print(f"  ✓ Completed in {time.time() - start:.0f}s")


# ─────────────────────────────────────────────────────────────
#  FUTURE DEMAND FORECAST
# ─────────────────────────────────────────────────────────────

def predict_future_demand(horizon_days: int = 180):
    """
    Generate forward-looking demand forecasts for 180 days past the last
    historical date, and INSERT into mart_daily_product_kpis.

    forecast_horizon column values:
      - Days  1-30  → 30
      - Days 31-60  → 60
      - Days 61-90  → 90
      - Days 91-180 → 180

    Future rows have:
      - total_units_sold = NULL  (no actuals)
      - demand_forecast  = model prediction
      - forecast_error   = NULL
      - is_forecast      = TRUE
      - forecast_horizon = 30 / 60 / 90 / 180

    Uses INSERT not MERGE — future rows are new, not updates.
    Re-running deletes existing future rows first to avoid duplicates.
    """
    print("\n" + "=" * 60)
    print("  FUTURE DEMAND FORECAST — PREDICT & INSERT")
    print("=" * 60)
    start = time.time()

    model = load_model('demand_best')
    meta  = load_metadata('demand_best')
    features = meta.get('features', demand_features())
    print(f"  Model  : {meta.get('model_name', 'unknown')}")
    print(f"  Horizon: {horizon_days} days")

    conn = get_snowflake_connection()
    cur  = conn.cursor()

    # Last historical date — forecast starts the day after
    hist_max = get_max_date(
        conn,
        "MARTS.MART_DAILY_PRODUCT_KPIS WHERE IS_FORECAST = FALSE",
        'DATE'
    )
    forecast_start = hist_max + pd.Timedelta(days=1)
    forecast_end   = hist_max + pd.Timedelta(days=horizon_days)

    print(f"\n  Historical data ends : {hist_max.date()}")
    print(f"  Forecast window      : {forecast_start.date()} → {forecast_end.date()}")

    # ── Pull seed data (last 60 days of history for lag features) ──
    seed_start = hist_max - pd.Timedelta(days=LOOKBACK_DAYS)
    print(f"\n  Pulling seed data ({seed_start.date()} → {hist_max.date()}) for lag features...")
    seed_df = fast_query(
        conn,
        f"""
        SELECT * FROM MARTS.MART_DAILY_PRODUCT_KPIS
        WHERE DATE >= '{seed_start.date()}'
          AND IS_FORECAST = FALSE
        """
    )
    print(f"  Loaded {len(seed_df):,} seed rows")

    # ── Pull product attributes from dimension table ──
    print("  Loading product attributes from stg_products...")
    products = fast_query(
        conn,
        "SELECT product_id, category, price_tier, subcategory, cost_price, selling_price, weight_kg, is_perishable FROM STAGING.STG_PRODUCTS WHERE IS_CURRENT = TRUE"
    )
    print(f"  Loaded {len(products):,} products")

    # ── Build future date spine ──
    future_dates = pd.date_range(start=forecast_start, end=forecast_end, freq='D')
    product_ids  = products['product_id'].unique()

    print(f"\n  Building future date spine...")
    print(f"  {len(future_dates)} days × {len(product_ids)} products = {len(future_dates) * len(product_ids):,} rows")

    # Cross join: every product × every future date
    future_spine = pd.MultiIndex.from_product(
        [future_dates, product_ids], names=['date', 'product_id']
    ).to_frame(index=False)

    # Attach product attributes
    future_spine = future_spine.merge(products, on='product_id', how='left')

    # Fill metric columns with NaN for future spine.
    # total_units_sold must be NaN (not 0) — lag features shift this column forward,
    # and zeros would corrupt the forecast by making the model think demand dropped to 0.
    # The lag features for the first future days will be seeded from the historical rows
    # in 'combined', so NaN here just means "no actuals yet".
    for col in ['total_units_sold', 'total_revenue', 'stockout_count',
                'avg_closing_stock', 'inventory_turnover', 'avg_days_of_supply',
                'total_holding_cost', 'total_inventory_value', 'demand_volatility',
                'demand_forecast', 'forecast_error', 'stockout_risk_score']:
        future_spine[col] = None

    # ── Combine seed + future for feature engineering ──
    # build_demand_features needs historical context to compute lag features
    # for the first future dates.
    # Keep only the columns that both seed_df and future_spine share —
    # specifically the mart metric columns. Extra columns like is_forecast,
    # forecast_horizon in seed_df would cause schema mismatches.
    core_cols = [
        'date', 'product_id', 'category', 'price_tier',
        'total_units_sold', 'total_revenue', 'stockout_count',
        'avg_closing_stock', 'inventory_turnover', 'avg_days_of_supply',
        'total_holding_cost', 'total_inventory_value',
        'demand_forecast', 'forecast_error', 'demand_volatility', 'stockout_risk_score'
    ]
    seed_core = seed_df[[c for c in core_cols if c in seed_df.columns]].copy()
    future_core = future_spine[[c for c in core_cols if c in future_spine.columns]].copy()

    combined = pd.concat([seed_core, future_core], ignore_index=True)
    combined['date'] = pd.to_datetime(combined['date'])
    combined = combined.sort_values(['product_id', 'date']).reset_index(drop=True)

    # Load dates dimension for calendar features
    dates_dim = fast_query(conn, "SELECT * FROM STAGING.STG_DATES")
    dates_dim['date'] = pd.to_datetime(dates_dim['date'])

    # stg_dates only covers Feb 2022 → Feb 2025.
    # Future dates (Feb 2025 → Aug 2025) don't exist in it — extend it synthetically.
    last_hist_date = dates_dim['date'].max()
    if forecast_end > last_hist_date:
        future_date_range = pd.date_range(
            start=last_hist_date + pd.Timedelta(days=1),
            end=forecast_end,
            freq='D'
        )
        future_dates_df = pd.DataFrame({'date': future_date_range})
        future_dates_df['day_of_week_num'] = future_dates_df['date'].dt.dayofweek + 1
        future_dates_df['month']           = future_dates_df['date'].dt.month
        future_dates_df['quarter']         = future_dates_df['date'].dt.quarter
        future_dates_df['year']            = future_dates_df['date'].dt.year
        future_dates_df['is_weekend']      = future_dates_df['date'].dt.dayofweek >= 5
        future_dates_df['is_holiday']      = False  # conservative — no holiday data for future
        future_dates_df['season']          = future_dates_df['month'].map({
            12: 'Winter', 1: 'Winter', 2: 'Winter',
            3: 'Spring',  4: 'Spring', 5: 'Spring',
            6: 'Summer',  7: 'Summer', 8: 'Summer',
            9: 'Fall',   10: 'Fall',  11: 'Fall'
        })
        dates_dim = pd.concat([dates_dim, future_dates_df], ignore_index=True)
        print(f"  Extended dates dimension to {forecast_end.date()} (+{len(future_dates_df)} rows)")

    print("  Building features for future rows...")
    df_features = build_demand_features(combined, dates_dim, products)

    # Keep only future rows for scoring
    df_features['date'] = pd.to_datetime(df_features['date'])
    df_future = df_features[df_features['date'] >= forecast_start].copy()

    # build_demand_features drops rows where demand_lag_28d is NaN.
    # For future rows, lag features beyond the seed window will be NaN.
    # Fill with rolling averages as a reasonable substitute — the rolling avg
    # captures recent trend and is a standard imputation for future lag features.
    lag_cols = [c for c in df_future.columns if c.startswith('demand_lag_')]
    for col in lag_cols:
        fallback = 'demand_rolling_avg_30d' if 'demand_rolling_avg_30d' in df_future.columns else None
        if fallback:
            df_future[col] = df_future[col].fillna(df_future[fallback])

    # If any future rows were dropped by dropna in build_demand_features,
    # rebuild them from the spine with filled features
    if len(df_future) == 0:
        print("  WARNING: All future rows dropped by dropna — using spine directly")
        df_future = future_spine.copy()
        df_future['date'] = pd.to_datetime(df_future['date'])

    print(f"  Scoring {len(df_future):,} future rows")

    # ── Score ──
    available_features = [f for f in features if f in df_future.columns]
    X = df_future[available_features].fillna(0)
    df_future['demand_forecast'] = np.maximum(model.predict(X), 0).round(2)

    # ── Assign forecast_horizon bucket ──
    days_out = (df_future['date'] - hist_max).dt.days
    df_future['forecast_horizon'] = pd.cut(
        days_out,
        bins=[0, 30, 60, 90, horizon_days],
        labels=[30, 60, 90, 180]
    ).astype(int)

    df_future['is_forecast']      = True
    df_future['forecast_error']   = None
    df_future['total_units_sold'] = None

    # ── Sample preview ──
    print(f"\n  Sample future predictions:")
    print(
        df_future[['date', 'product_id', 'demand_forecast', 'forecast_horizon']]
        .head(10)
        .to_string(index=False)
    )

    # Horizon distribution
    print(f"\n  Horizon distribution:")
    for h in [30, 60, 90, 180]:
        n = (df_future['forecast_horizon'] == h).sum()
        print(f"    {h:>3}d bucket: {n:>8,} rows")

    # ── Write to Snowflake ──
    # Delete existing future rows first to allow clean re-runs
    cur.execute("USE SCHEMA MARTS")
    cur.execute("DELETE FROM MART_DAILY_PRODUCT_KPIS WHERE IS_FORECAST = TRUE")
    deleted = cur.rowcount
    print(f"\n  Cleared {deleted:,} existing future rows")

    # price_tier is dropped by build_demand_features (it strips mart cols before merging products)
    # Re-attach it from the products dataframe
    price_tier_map = products[['product_id', 'price_tier']].drop_duplicates()
    df_future = df_future.merge(price_tier_map, on='product_id', how='left')

    # Prepare writeback dataframe — only columns that exist in the mart
    writeback = df_future[[
        'date', 'product_id', 'category', 'price_tier',
        'demand_forecast', 'forecast_horizon', 'is_forecast'
    ]].copy()
    writeback['date'] = writeback['date'].dt.strftime('%Y-%m-%d')
    writeback['forecast_error']        = None
    writeback['total_units_sold']      = None
    writeback['total_revenue']         = None
    writeback['stockout_count']        = None
    writeback['avg_closing_stock']     = None
    writeback['inventory_turnover']    = None
    writeback['avg_days_of_supply']    = None
    writeback['total_holding_cost']    = None
    writeback['total_inventory_value'] = None
    writeback['demand_volatility']     = None
    writeback['stockout_risk_score']   = None

    # Reorder to match mart column order
    writeback = writeback[[
        'date', 'product_id', 'category', 'price_tier',
        'total_units_sold', 'total_revenue', 'stockout_count',
        'avg_closing_stock', 'inventory_turnover', 'avg_days_of_supply',
        'total_holding_cost', 'total_inventory_value',
        'demand_forecast', 'forecast_error', 'demand_volatility',
        'stockout_risk_score', 'is_forecast', 'forecast_horizon'
    ]]

    temp_path = 'ml/results/_temp_future_demand.csv'
    os.makedirs('ml/results', exist_ok=True)
    writeback.to_csv(temp_path, index=False)
    abs_path = os.path.abspath(temp_path).replace('\\', '/')

    cur.execute("""
        CREATE OR REPLACE TEMPORARY TABLE _temp_future_demand (
            date                  DATE,
            product_id            VARCHAR(20),
            category              VARCHAR(50),
            price_tier            VARCHAR(20),
            total_units_sold      NUMBER(38,0),
            total_revenue         NUMBER(22,2),
            stockout_count        NUMBER(18,0),
            avg_closing_stock     NUMBER(38,2),
            inventory_turnover    NUMBER(38,2),
            avg_days_of_supply    NUMBER(24,2),
            total_holding_cost    NUMBER(22,2),
            total_inventory_value NUMBER(24,2),
            demand_forecast       NUMBER(8,2),
            forecast_error        NUMBER(8,2),
            demand_volatility     FLOAT,
            stockout_risk_score   NUMBER(6,4),
            is_forecast           BOOLEAN,
            forecast_horizon      INTEGER
        )
    """)
    cur.execute(f"PUT file://{abs_path} @%_temp_future_demand AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
    cur.execute("""
        COPY INTO _temp_future_demand
        FROM @%_temp_future_demand
        FILE_FORMAT = (
            TYPE='CSV'
            SKIP_HEADER=1
            FIELD_OPTIONALLY_ENCLOSED_BY='"'
            EMPTY_FIELD_AS_NULL=TRUE
        )
    """)
    cur.execute("INSERT INTO MART_DAILY_PRODUCT_KPIS SELECT * FROM _temp_future_demand")
    rows_inserted = cur.rowcount
    conn.commit()

    try:
        os.remove(temp_path)
    except OSError:
        pass

    cur.close()
    conn.close()

    print(f"\n  ✓ Inserted {rows_inserted:,} future forecast rows")
    print(f"  ✓ Completed in {time.time() - start:.0f}s")


# ─────────────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────────────

def run_writeback(phases: list = None):
    all_phases = ['demand', 'eta', 'stockout', 'future_demand']
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

    if 'future_demand' in phases:
        predict_future_demand(horizon_days=180)

    print(f"\n{'=' * 60}")
    print(f"  All predictions complete in {time.time() - total_start:.0f}s")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='ML Prediction & Writeback')
    parser.add_argument(
        '--phase',
        nargs='+',
        choices=['demand', 'eta', 'stockout', 'future_demand'],
        default=None,
        help='Run specific phases (default: all). future_demand generates 180-day forward forecast.'
    )
    args = parser.parse_args()
    run_writeback(phases=args.phase)