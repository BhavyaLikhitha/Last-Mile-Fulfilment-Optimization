"""PySpark port of ml/features/eta_features.py.

Replaces pandas groupby aggregations with Spark distributed joins,
eliminating the need for year-by-year chunking on 5M+ delivery rows.

Target: actual_delivery_minutes
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_eta_features_spark(deliveries: DataFrame, dates: DataFrame) -> DataFrame:
    """Build ETA prediction features using PySpark.

    Mirrors the exact feature set from ml/features/eta_features.py:
    distance, driver stats, time-of-day, date, warehouse, priority.
    """
    # Filter to delivered orders with valid target
    df = deliveries.filter((F.col("delivery_status") == "Delivered") & F.col("actual_delivery_minutes").isNotNull())

    # Cast numeric columns
    numeric_cols = [
        "actual_delivery_minutes",
        "estimated_eta_minutes",
        "distance_km",
        "delivery_cost",
        "sla_minutes",
        "eta_accuracy_pct",
        "eta_error_minutes",
        "cost_per_km",
        "driver_experience_years",
        "pickup_wait_minutes",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df = df.withColumn(col, F.coalesce(F.col(col).cast("double"), F.lit(0.0)))

    # Parse timestamps
    df = df.withColumn("assigned_time", F.to_timestamp("assigned_time"))
    df = df.withColumn("date", F.to_date("assigned_time"))

    # ── Merge date features ──
    dates = dates.withColumn("date", F.to_date("date"))
    date_cols = ["date", "day_of_week_num", "month", "is_holiday", "is_weekend", "season"]
    df = df.join(dates.select(date_cols), on="date", how="left")

    # ── Time of day features ──
    df = df.withColumn("hour", F.hour("assigned_time"))
    df = df.withColumn(
        "is_peak_hour",
        F.when(F.col("hour").isin([8, 9, 10, 11, 12, 17, 18, 19]), 1).otherwise(0),
    )
    df = df.withColumn("is_morning", F.when(F.col("hour").between(6, 11), 1).otherwise(0))
    df = df.withColumn("is_afternoon", F.when(F.col("hour").between(12, 17), 1).otherwise(0))
    df = df.withColumn("is_evening", F.when(F.col("hour") >= 18, 1).otherwise(0))

    # ── Distance features ──
    df = df.withColumn("distance_squared", F.col("distance_km") ** 2)
    df = df.withColumn("distance_log", F.log1p("distance_km"))

    # ── Vehicle type encoding ──
    vehicle_map = {"Bike": 0, "Car": 1, "Van": 2, "Truck": 3}
    vehicle_expr = F.coalesce(
        *[F.when(F.col("vehicle_type") == k, F.lit(v)) for k, v in vehicle_map.items()],
        F.lit(1),
    )
    df = df.withColumn("vehicle_type_encoded", vehicle_expr)

    # ── Driver historical stats (aggregated across all deliveries) ──
    driver_stats = df.groupBy("driver_id").agg(
        F.avg("actual_delivery_minutes").alias("driver_avg_delivery"),
        F.avg("distance_km").alias("driver_avg_distance"),
        F.count("delivery_id").alias("driver_delivery_count"),
    )
    driver_stats = driver_stats.withColumn(
        "driver_hist_speed_kmh",
        F.least(
            F.lit(120.0),
            F.greatest(
                F.lit(0.0),
                F.col("driver_avg_distance") / (F.col("driver_avg_delivery") / 60),
            ),
        ),
    )
    df = df.join(
        driver_stats.select("driver_id", "driver_avg_delivery", "driver_hist_speed_kmh"),
        on="driver_id",
        how="left",
    )

    # ── ETA-based speed estimate ──
    df = df.withColumn(
        "eta_speed_estimate",
        F.col("distance_km")
        / F.when(F.col("estimated_eta_minutes") == 0, None).otherwise(F.col("estimated_eta_minutes") / 60),
    )

    # ── Simple ETA baseline ──
    df = df.withColumn("simple_eta", F.col("estimated_eta_minutes"))

    # ── Pickup wait ──
    df = df.withColumn("pickup_wait", F.coalesce(F.col("pickup_wait_minutes"), F.lit(0.0)))

    # ── Warehouse-level historical avg delivery time ──
    wh_stats = df.groupBy("warehouse_id").agg(
        F.avg("actual_delivery_minutes").alias("warehouse_avg_delivery"),
    )
    df = df.join(wh_stats, on="warehouse_id", how="left")

    # Fill missing with global mean
    global_mean = df.select(F.avg("actual_delivery_minutes")).first()[0] or 0.0
    df = df.withColumn("warehouse_avg_delivery", F.coalesce(F.col("warehouse_avg_delivery"), F.lit(global_mean)))
    df = df.withColumn("driver_avg_delivery", F.coalesce(F.col("driver_avg_delivery"), F.lit(global_mean)))
    df = df.withColumn("driver_hist_speed_kmh", F.coalesce(F.col("driver_hist_speed_kmh"), F.lit(30.0)))

    # ── Priority encoding ──
    priority_map = {"Standard": 0, "Express": 1, "Same-Day": 2}
    priority_expr = F.coalesce(
        *[F.when(F.col("order_priority") == k, F.lit(v)) for k, v in priority_map.items()],
        F.lit(0),
    )
    df = df.withColumn("priority_encoded", priority_expr)

    # ── Season encoding ──
    season_map = {"Winter": 0, "Spring": 1, "Summer": 2, "Fall": 3}
    season_expr = F.coalesce(
        *[F.when(F.col("season") == k, F.lit(v)) for k, v in season_map.items()],
        F.lit(0),
    )
    df = df.withColumn("season_encoded", season_expr)

    # Boolean to int
    df = df.withColumn("is_holiday", F.coalesce(F.col("is_holiday").cast("int"), F.lit(0)))
    df = df.withColumn("is_weekend", F.coalesce(F.col("is_weekend").cast("int"), F.lit(0)))

    # Drop rows with missing target or distance
    df = df.filter(F.col("actual_delivery_minutes").isNotNull() & F.col("distance_km").isNotNull())

    return df


def get_feature_columns() -> list:
    """Same feature columns as ml/features/eta_features.py."""
    return [
        "distance_km",
        "distance_squared",
        "distance_log",
        "vehicle_type_encoded",
        "driver_hist_speed_kmh",
        "eta_speed_estimate",
        "driver_experience_years",
        "driver_avg_delivery",
        "simple_eta",
        "pickup_wait",
        "warehouse_avg_delivery",
        "hour",
        "is_peak_hour",
        "is_morning",
        "is_afternoon",
        "is_evening",
        "day_of_week_num",
        "month",
        "is_holiday",
        "is_weekend",
        "season_encoded",
        "priority_encoded",
    ]
