"""PySpark port of ml/features/demand_features.py.

Replaces pandas groupby/rolling with Spark Window functions for
distributed feature engineering on large datasets.

Target: total_units_sold (per product per day)
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_demand_features_spark(
    product_kpis: DataFrame,
    dates: DataFrame,
    products: DataFrame,
) -> DataFrame:
    """Build demand forecasting features using PySpark Window functions.

    Mirrors the exact feature set from ml/features/demand_features.py:
    lag features, rolling averages/std/min/max, trend, inventory, price, date, category.
    """
    df = product_kpis

    # Drop overlap columns that will come from products
    df = df.drop("category", "price_tier")

    # Cast numeric columns
    numeric_cols = [
        "total_units_sold",
        "total_revenue",
        "stockout_count",
        "avg_closing_stock",
        "inventory_turnover",
        "avg_days_of_supply",
        "total_holding_cost",
        "total_inventory_value",
        "demand_forecast",
        "forecast_error",
        "demand_volatility",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df = df.withColumn(col, F.coalesce(F.col(col).cast("double"), F.lit(0.0)))

    # ── Merge date features ──
    date_cols = ["date", "day_of_week_num", "month", "quarter", "year", "is_holiday", "is_weekend", "season"]
    df = df.join(dates.select(date_cols), on="date", how="left")

    # ── Merge product features ──
    product_cols = [
        "product_id",
        "category",
        "subcategory",
        "cost_price",
        "selling_price",
        "weight_kg",
        "is_perishable",
    ]
    df = df.join(products.select(product_cols), on="product_id", how="left")

    # Cast product numeric columns
    for col in ["cost_price", "selling_price", "weight_kg"]:
        df = df.withColumn(col, F.coalesce(F.col(col).cast("double"), F.lit(0.0)))

    # ── Window spec for per-product time series ──
    w = Window.partitionBy("product_id").orderBy("date")

    # ── Lag Features ──
    for lag in [1, 3, 7, 14, 28]:
        df = df.withColumn(f"demand_lag_{lag}d", F.lag("total_units_sold", lag).over(w))

    # ── Rolling Average and Std Features ──
    for window in [7, 14, 30]:
        w_roll = w.rowsBetween(-window + 1, 0)
        df = df.withColumn(f"demand_rolling_avg_{window}d", F.avg("total_units_sold").over(w_roll))
        df = df.withColumn(f"demand_rolling_std_{window}d", F.stddev("total_units_sold").over(w_roll))

    # ── Rolling Min/Max (7-day demand range) ──
    w_7d = w.rowsBetween(-6, 0)
    df = df.withColumn("demand_rolling_min_7d", F.min("total_units_sold").over(w_7d))
    df = df.withColumn("demand_rolling_max_7d", F.max("total_units_sold").over(w_7d))

    # ── Trend Features ──
    df = df.withColumn(
        "demand_wow_change",
        F.col("total_units_sold") - F.lag("total_units_sold", 7).over(w),
    )
    df = df.withColumn(
        "demand_mom_change",
        F.col("total_units_sold") - F.lag("total_units_sold", 30).over(w),
    )

    # ── Inventory Features ──
    df = df.withColumn("stockout_lag_1d", F.lag("stockout_count", 1).over(w))
    df = df.withColumn("avg_stock_lag_1d", F.lag("avg_closing_stock", 1).over(w))

    # ── Price Features ──
    df = df.withColumn(
        "price_ratio",
        F.col("selling_price") / F.when(F.col("cost_price") == 0, None).otherwise(F.col("cost_price")),
    )
    df = df.withColumn("profit_margin", F.col("selling_price") - F.col("cost_price"))

    # ── Encode Categoricals ──
    season_map = {"Winter": 0, "Spring": 1, "Summer": 2, "Fall": 3}
    season_expr = F.coalesce(
        *[F.when(F.col("season") == k, F.lit(v)) for k, v in season_map.items()],
        F.lit(0),
    )
    df = df.withColumn("season_encoded", season_expr)

    # Category encoding via dense_rank (deterministic label encoding)
    # cat_w = Window.orderBy("category")
    cat_w = Window.partitionBy("product_id").orderBy("date")
    df = df.withColumn("category_encoded", F.dense_rank().over(cat_w) - 1)

    # Boolean to int
    df = df.withColumn("is_holiday", F.coalesce(F.col("is_holiday").cast("int"), F.lit(0)))
    df = df.withColumn("is_weekend", F.coalesce(F.col("is_weekend").cast("int"), F.lit(0)))
    df = df.withColumn("is_perishable", F.coalesce(F.col("is_perishable").cast("int"), F.lit(0)))

    # ── Drop rows with NaN from lag features (first 28 days per product) ──
    df = df.filter(F.col("demand_lag_28d").isNotNull())

    return df


def get_feature_columns() -> list:
    """Same feature columns as ml/features/demand_features.py."""
    return [
        "demand_lag_1d",
        "demand_lag_3d",
        "demand_lag_7d",
        "demand_lag_14d",
        "demand_lag_28d",
        "demand_rolling_avg_7d",
        "demand_rolling_avg_14d",
        "demand_rolling_avg_30d",
        "demand_rolling_std_7d",
        "demand_rolling_std_14d",
        "demand_rolling_std_30d",
        "demand_rolling_min_7d",
        "demand_rolling_max_7d",
        "demand_wow_change",
        "demand_mom_change",
        "stockout_count",
        "stockout_lag_1d",
        "avg_closing_stock",
        "avg_stock_lag_1d",
        "inventory_turnover",
        "day_of_week_num",
        "month",
        "quarter",
        "year",
        "is_holiday",
        "is_weekend",
        "season_encoded",
        "category_encoded",
        "cost_price",
        "selling_price",
        "weight_kg",
        "is_perishable",
        "price_ratio",
        "profit_margin",
    ]
