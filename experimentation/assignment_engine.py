# """
# Assignment Engine
# Loads experiment assignments + raw order/delivery data from Snowflake,
# injects calibrated treatment effects per experiment type, and prepares
# per-observation DataFrames ready for statistical testing.

# Treatment effect sizes are calibrated from published fulfillment research:
#   - inventory_policy  : 8-15% cost reduction (SCM literature)
#   - routing_algorithm : 10-15% delivery time reduction (routing research)
#   - warehouse_allocation: 6-10% cost reduction (Amazon benchmarks)

# Seeded RNG (seed=42) ensures full reproducibility.
# """

# import numpy as np
# import pandas as pd

# # ── Treatment Effect Configuration ───────────────────────────
# TREATMENT_EFFECTS = {
#     'inventory_policy': {
#         'metric'         : 'total_fulfillment_cost',
#         'effect_range'   : (0.08, 0.15),   # 8-15% reduction
#         'noise_std'      : 0.02,
#         'active_scale'   : 0.50,           # active experiments get 50% of full effect
#     },
#     'routing_algorithm': {
#         'metric'         : 'actual_delivery_minutes',
#         'effect_range'   : (0.10, 0.15),   # 10-15% reduction
#         'noise_std'      : 0.02,
#         'active_scale'   : 0.50,
#     },
#     'warehouse_allocation': {
#         'metric'         : 'total_fulfillment_cost',
#         'effect_range'   : (0.06, 0.10),   # 6-10% reduction
#         'noise_std'      : 0.015,
#         'active_scale'   : 0.40,
#     },
# }

# # Per-experiment effect sizes (seeded for reproducibility)
# EXPERIMENT_EFFECTS = {
#     'EXP-001': 0.12,   # dynamic reorder — strong effect
#     'EXP-002': 0.09,   # safety stock — moderate effect
#     'EXP-003': 0.12,   # balanced routing
#     'EXP-004': 0.11,   # load balanced driver
#     'EXP-005': 0.08,   # cost optimal allocation
#     'EXP-006': 0.07,   # capacity aware
#     'EXP-007': 0.10,   # JIT reorder
#     'EXP-008': 0.13,   # priority routing — strong effect
#     'EXP-009': 0.04,   # active — partial effect (6% * 0.40 scale + noise)
#     'EXP-010': 0.05,   # active — partial effect (ml reorder promising but early)
# }


# # def load_experiment_data(conn) -> tuple:
# #     """
# #     Load all data needed for experimentation from Snowflake.

# #     Returns:
# #         experiments  : dim_experiments metadata
# #         assignments  : fact_experiment_assignments (order→group mapping)
# #         orders       : fact_orders with cost metrics
# #         deliveries   : fact_deliveries with time metrics
# #     """
# #     from ml.training.predict_and_writeback import fast_query

# #     print("  Loading dim_experiments...")
# #     experiments = fast_query(conn, "SELECT * FROM RAW.DIM_EXPERIMENTS")
# #     print(f"    {len(experiments):,} experiments")

# #     print("  Loading fact_experiment_assignments...")
# #     assignments = fast_query(conn, """
# #         SELECT assignment_id, experiment_id, order_id, group_name, assigned_at, warehouse_id
# #         FROM RAW.FACT_EXPERIMENT_ASSIGNMENTS
# #     """)
# #     print(f"    {len(assignments):,} assignments")

# #     print("  Loading fact_orders...")
# #     orders = fast_query(conn, """
# #         SELECT o.order_id, o.order_date, o.customer_id,
# #                o.assigned_warehouse_id, o.nearest_warehouse_id,
# #                o.order_priority, o.total_fulfillment_cost,
# #                o.total_amount, o.order_status,
# #                c.customer_segment
# #         FROM RAW.FACT_ORDERS o
# #         LEFT JOIN RAW.DIM_CUSTOMER c ON o.customer_id = c.customer_id
# #         WHERE o.order_status != 'Cancelled'
# #     """)
# #     print(f"    {len(orders):,} orders")

# #     print("  Loading fact_deliveries...")
# #     deliveries = fast_query(conn, """
# #         SELECT d.delivery_id, d.order_id, d.warehouse_id,
# #                d.actual_delivery_minutes, d.distance_km,
# #                d.on_time_flag, d.sla_breach_flag, d.delivery_status
# #         FROM RAW.FACT_DELIVERIES d
# #         WHERE d.delivery_status = 'Delivered'
# #           AND d.actual_delivery_minutes IS NOT NULL
# #     """)
# #     print(f"    {len(deliveries):,} deliveries")

# #     return experiments, assignments, orders, deliveries


# def load_experiment_data(conn) -> tuple:
#     """
#     Load experiment data from Snowflake.
#     Heavy joins are done in Snowflake to minimize memory usage.
#     Returns pre-joined observation data instead of raw tables.
#     """
#     from ml.training.predict_and_writeback import fast_query

#     print("  Loading dim_experiments...")
#     experiments = fast_query(conn, "SELECT * FROM RAW.DIM_EXPERIMENTS")
#     print(f"    {len(experiments):,} experiments")

#     print("  Loading pre-joined assignments + orders...")
#     assignments = fast_query(conn, """
#         SELECT a.assignment_id, a.experiment_id, a.order_id, a.group_name,
#                a.assigned_at, a.warehouse_id,
#                o.total_fulfillment_cost, o.order_priority,
#                o.assigned_warehouse_id, c.customer_segment
#         FROM RAW.FACT_EXPERIMENT_ASSIGNMENTS a
#         JOIN RAW.FACT_ORDERS o ON a.order_id = o.order_id
#         LEFT JOIN RAW.DIM_CUSTOMER c ON o.customer_id = c.customer_id
#         WHERE o.order_status != 'Cancelled'
#     """)
#     print(f"    {len(assignments):,} assignments")

#     print("  Loading delivery metrics for routing experiments...")
#     deliveries = fast_query(conn, """
#         SELECT a.order_id, d.delivery_id, d.actual_delivery_minutes,
#                d.distance_km, d.warehouse_id
#         FROM RAW.FACT_EXPERIMENT_ASSIGNMENTS a
#         JOIN RAW.FACT_DELIVERIES d ON a.order_id = d.order_id
#         JOIN RAW.DIM_EXPERIMENTS e ON a.experiment_id = e.experiment_id
#         WHERE e.experiment_type = 'routing_algorithm'
#           AND d.delivery_status = 'Delivered'
#           AND d.actual_delivery_minutes IS NOT NULL
#     """)
#     print(f"    {len(deliveries):,} deliveries")

#     return experiments, assignments, pd.DataFrame(), deliveries

# # def prepare_experiment_observations(
# #     experiment: pd.Series,
# #     assignments: pd.DataFrame,
# #     orders: pd.DataFrame,
# #     deliveries: pd.DataFrame
# # ) -> pd.DataFrame:
# #     """
# #     For a single experiment, build a DataFrame with one row per observation
# #     (order or delivery depending on experiment type), with Control/Treatment labels
# #     and the primary metric value.

# #     Args:
# #         experiment  : single row from dim_experiments
# #         assignments : all experiment assignments
# #         orders      : all orders
# #         deliveries  : all deliveries

# #     Returns:
# #         DataFrame with columns: observation_id, group_name, metric_value,
# #                                 customer_segment, order_priority, warehouse_id
# #     """
# #     exp_id   = experiment['experiment_id']
# #     exp_type = experiment['experiment_type']
# #     status   = experiment['status']

# #     # Filter assignments for this experiment
# #     exp_assignments = assignments[assignments['experiment_id'] == exp_id].copy()
# #     if len(exp_assignments) == 0:
# #         return pd.DataFrame()

# #     if exp_type in ('inventory_policy', 'warehouse_allocation'):
# #         # Join assignments → orders
# #         df = exp_assignments.merge(
# #             orders[['order_id', 'total_fulfillment_cost', 'customer_segment',
# #                     'order_priority', 'assigned_warehouse_id']],
# #             on='order_id', how='inner'
# #         )
# #         df = df.rename(columns={
# #             'order_id'             : 'observation_id',
# #             'total_fulfillment_cost': 'metric_value',
# #             'assigned_warehouse_id' : 'warehouse_id_obs'
# #         })
# #         df['metric_name'] = 'total_fulfillment_cost'

# #     elif exp_type == 'routing_algorithm':
# #         # Join assignments → orders → deliveries
# #         orders_with_seg = orders[['order_id', 'customer_segment', 'order_priority',
# #                                    'assigned_warehouse_id']]
# #         df = exp_assignments.merge(orders_with_seg, on='order_id', how='inner')
# #         df = df.merge(
# #             deliveries[['order_id', 'delivery_id', 'actual_delivery_minutes']],
# #             on='order_id', how='inner'
# #         )
# #         df = df.rename(columns={
# #             'delivery_id'             : 'observation_id',
# #             'actual_delivery_minutes' : 'metric_value',
# #             'assigned_warehouse_id'   : 'warehouse_id_obs'
# #         })
# #         df['metric_name'] = 'actual_delivery_minutes'
# #     else:
# #         return pd.DataFrame()

# #     # Keep only relevant columns
# #     df = df[['observation_id', 'group_name', 'metric_value',
# #              'customer_segment', 'order_priority', 'warehouse_id_obs']].copy()
# #     df = df.dropna(subset=['metric_value'])
# #     df['experiment_id'] = exp_id
# #     df['experiment_type'] = exp_type
# #     df['status'] = status

# #     return df

# def prepare_experiment_observations(
#     experiment: pd.Series,
#     assignments: pd.DataFrame,
#     orders: pd.DataFrame,
#     deliveries: pd.DataFrame
# ) -> pd.DataFrame:
#     exp_id   = experiment['experiment_id']
#     exp_type = experiment['experiment_type']
#     status   = experiment['status']

#     exp_assignments = assignments[assignments['experiment_id'] == exp_id].copy()
#     if len(exp_assignments) == 0:
#         return pd.DataFrame()

#     if exp_type in ('inventory_policy', 'warehouse_allocation'):
#         df = exp_assignments[['order_id', 'group_name', 'total_fulfillment_cost',
#                               'customer_segment', 'order_priority', 'assigned_warehouse_id']].copy()
#         df = df.rename(columns={
#             'order_id': 'observation_id',
#             'total_fulfillment_cost': 'metric_value',
#             'assigned_warehouse_id': 'warehouse_id_obs'
#         })
#         df['metric_name'] = 'total_fulfillment_cost'

#     elif exp_type == 'routing_algorithm':
#         df = exp_assignments[['order_id', 'group_name', 'customer_segment',
#                               'order_priority', 'assigned_warehouse_id']].copy()
#         df = df.merge(
#             deliveries[['order_id', 'delivery_id', 'actual_delivery_minutes']],
#             on='order_id', how='inner'
#         )
#         df = df.rename(columns={
#             'delivery_id': 'observation_id',
#             'actual_delivery_minutes': 'metric_value',
#             'assigned_warehouse_id': 'warehouse_id_obs'
#         })
#         df['metric_name'] = 'actual_delivery_minutes'
#     else:
#         return pd.DataFrame()

#     df = df[['observation_id', 'group_name', 'metric_value',
#              'customer_segment', 'order_priority', 'warehouse_id_obs']].copy()
#     df = df.dropna(subset=['metric_value'])
#     df['experiment_id'] = exp_id
#     df['experiment_type'] = exp_type
#     df['status'] = status

#     return df

# def inject_treatment_effects(df: pd.DataFrame, experiment_id: str, experiment_type: str, status: str) -> pd.DataFrame:
#     """
#     Apply calibrated treatment effect to Treatment group observations.
#     Control group is untouched.

#     The effect reduces the metric value for Treatment observations by a fixed
#     percentage plus Gaussian noise. This simulates what the new policy would
#     achieve based on published benchmarks.

#     Args:
#         df             : observations DataFrame with group_name and metric_value
#         experiment_id  : e.g. 'EXP-001'
#         experiment_type: 'inventory_policy', 'routing_algorithm', 'warehouse_allocation'
#         status         : 'Completed' or 'Active'

#     Returns:
#         DataFrame with metric_value adjusted for Treatment group
#     """
#     rng = np.random.default_rng(seed=int(experiment_id.split('-')[1]) * 42)

#     config      = TREATMENT_EFFECTS[experiment_type]
#     base_effect = EXPERIMENT_EFFECTS.get(experiment_id, 0.08)

#     # Active experiments get reduced effect (less data, earlier stage)
#     if status == 'Active':
#         base_effect = base_effect * config['active_scale']

#     result = df.copy()
#     treatment_mask = result['group_name'] == 'Treatment'
#     n_treatment = treatment_mask.sum()

#     if n_treatment == 0:
#         return result

#     # Per-observation noise: each Treatment observation gets slightly different effect
#     noise = rng.normal(0, config['noise_std'], n_treatment)
#     effective_effects = np.clip(base_effect + noise, 0.01, 0.25)

#     # Reduce metric value for Treatment (lower cost/time = better)
#     result.loc[treatment_mask, 'metric_value'] = (
#         result.loc[treatment_mask, 'metric_value'] * (1 - effective_effects)
#     )

#     return result


# def build_all_experiment_data(
#     experiments: pd.DataFrame,
#     assignments: pd.DataFrame,
#     orders: pd.DataFrame,
#     deliveries: pd.DataFrame
# ) -> dict:
#     """
#     Build observation DataFrames for all experiments with treatment effects injected.

#     Returns:
#         dict mapping experiment_id → DataFrame of observations
#     """
#     result = {}
#     for _, exp in experiments.iterrows():
#         exp_id = exp['experiment_id']
#         print(f"  Preparing {exp_id}: {exp['experiment_name']} ({exp['status']})")

#         df = prepare_experiment_observations(exp, assignments, orders, deliveries)
#         if len(df) == 0:
#             print(f"    No data found — skipping")
#             continue

#         df = inject_treatment_effects(df, exp_id, exp['experiment_type'], exp['status'])

#         n_control   = (df['group_name'] == 'Control').sum()
#         n_treatment = (df['group_name'] == 'Treatment').sum()
#         print(f"    {len(df):,} observations — Control: {n_control:,}, Treatment: {n_treatment:,}")

#         result[exp_id] = df

#     return result

"""
Assignment Engine (Memory-Optimized)
All heavy joins and aggregations are pushed to Snowflake.
Python only receives small aggregated DataFrames (~20-100 rows).

Treatment effects are applied via SQL-level multipliers rather than
per-observation noise, which is statistically equivalent for large N
(Central Limit Theorem) and uses zero memory.
"""

import numpy as np
import pandas as pd

# ── Treatment Effect Configuration ───────────────────────────
EXPERIMENT_EFFECTS = {
    'EXP-001': 0.12,
    'EXP-002': 0.09,
    'EXP-003': 0.12,
    'EXP-004': 0.11,
    'EXP-005': 0.08,
    'EXP-006': 0.07,
    'EXP-007': 0.10,
    'EXP-008': 0.13,
    'EXP-009': 0.04,
    'EXP-010': 0.05,
}

ACTIVE_SCALE = {
    'inventory_policy': 0.50,
    'routing_algorithm': 0.50,
    'warehouse_allocation': 0.40,
}

# Metric used per experiment type
METRIC_MAP = {
    'inventory_policy': 'total_fulfillment_cost',
    'routing_algorithm': 'actual_delivery_minutes',
    'warehouse_allocation': 'total_fulfillment_cost',
}

# Join table per experiment type
JOIN_SQL = {
    'inventory_policy': """
        SELECT a.experiment_id, a.group_name, o.total_fulfillment_cost AS metric_value,
               c.customer_segment, o.order_priority, o.assigned_warehouse_id AS warehouse_id
        FROM RAW.FACT_EXPERIMENT_ASSIGNMENTS a
        JOIN RAW.FACT_ORDERS o ON a.order_id = o.order_id
        LEFT JOIN RAW.DIM_CUSTOMER c ON o.customer_id = c.customer_id
        WHERE o.order_status != 'Cancelled'
          AND a.experiment_id = '{exp_id}'
    """,
    'warehouse_allocation': """
        SELECT a.experiment_id, a.group_name, o.total_fulfillment_cost AS metric_value,
               c.customer_segment, o.order_priority, o.assigned_warehouse_id AS warehouse_id
        FROM RAW.FACT_EXPERIMENT_ASSIGNMENTS a
        JOIN RAW.FACT_ORDERS o ON a.order_id = o.order_id
        LEFT JOIN RAW.DIM_CUSTOMER c ON o.customer_id = c.customer_id
        WHERE o.order_status != 'Cancelled'
          AND a.experiment_id = '{exp_id}'
    """,
    'routing_algorithm': """
        SELECT a.experiment_id, a.group_name, d.actual_delivery_minutes AS metric_value,
               c.customer_segment, o.order_priority, d.warehouse_id
        FROM RAW.FACT_EXPERIMENT_ASSIGNMENTS a
        JOIN RAW.FACT_ORDERS o ON a.order_id = o.order_id
        JOIN RAW.FACT_DELIVERIES d ON a.order_id = d.order_id
        LEFT JOIN RAW.DIM_CUSTOMER c ON o.customer_id = c.customer_id
        WHERE o.order_status != 'Cancelled'
          AND d.delivery_status = 'Delivered'
          AND d.actual_delivery_minutes IS NOT NULL
          AND a.experiment_id = '{exp_id}'
    """,
}


def get_effect_multiplier(experiment_id: str, experiment_type: str, status: str) -> float:
    """Get the treatment effect multiplier for a given experiment."""
    base = EXPERIMENT_EFFECTS.get(experiment_id, 0.08)
    if status == 'Active':
        base = base * ACTIVE_SCALE.get(experiment_type, 0.50)
    return 1.0 - base  # e.g. 0.88 means 12% reduction


def load_experiment_summary_stats(conn, fast_query_fn) -> tuple:
    """
    Load experiment metadata and compute per-experiment, per-group summary
    statistics entirely in Snowflake.

    Returns:
        experiments: DataFrame with experiment metadata
        stats: DataFrame with experiment_id, group_name, n, mean_value, var_value
    """
    print("  Loading dim_experiments...")
    experiments = fast_query_fn(conn, "SELECT * FROM RAW.DIM_EXPERIMENTS")
    print(f"    {len(experiments):,} experiments")

    all_stats = []

    for _, exp in experiments.iterrows():
        exp_id = exp['experiment_id']
        exp_type = exp['experiment_type']
        status = exp['status']
        metric = METRIC_MAP.get(exp_type)

        if exp_type not in JOIN_SQL:
            continue

        print(f"  Aggregating {exp_id} ({exp_type})...")

        # Get summary stats from Snowflake — only returns 2 rows per experiment
        base_sql = JOIN_SQL[exp_type].format(exp_id=exp_id)
        stats_sql = f"""
            SELECT
                experiment_id,
                group_name,
                COUNT(*) AS n,
                AVG(metric_value) AS mean_value,
                VARIANCE(metric_value) AS var_value,
                STDDEV(metric_value) AS std_value
            FROM ({base_sql})
            GROUP BY experiment_id, group_name
        """
        stats_df = fast_query_fn(conn, stats_sql)

        if len(stats_df) == 0:
            continue

        # Apply treatment effect to Treatment group stats
        multiplier = get_effect_multiplier(exp_id, exp_type, status)
        treat_mask = stats_df['group_name'] == 'Treatment'
        stats_df.loc[treat_mask, 'mean_value'] = stats_df.loc[treat_mask, 'mean_value'] * multiplier
        # Variance scales by multiplier^2
        stats_df.loc[treat_mask, 'var_value'] = stats_df.loc[treat_mask, 'var_value'] * (multiplier ** 2)
        stats_df.loc[treat_mask, 'std_value'] = stats_df.loc[treat_mask, 'std_value'] * abs(multiplier)

        stats_df['experiment_type'] = exp_type
        stats_df['status'] = status
        stats_df['metric_name'] = metric
        all_stats.append(stats_df)

    if all_stats:
        combined = pd.concat(all_stats, ignore_index=True)
    else:
        combined = pd.DataFrame()

    print(f"  Total: {len(combined)} stat rows for {len(experiments)} experiments")
    return experiments, combined


def load_segment_stats(conn, fast_query_fn, experiments: pd.DataFrame) -> pd.DataFrame:
    """
    Load segment-level summary stats for uplift analysis.
    All aggregation done in Snowflake — returns ~100-200 rows total.
    """
    all_segments = []

    for _, exp in experiments.iterrows():
        exp_id = exp['experiment_id']
        exp_type = exp['experiment_type']
        status = exp['status']

        if exp_type not in JOIN_SQL:
            continue

        base_sql = JOIN_SQL[exp_type].format(exp_id=exp_id)
        multiplier = get_effect_multiplier(exp_id, exp_type, status)

        # Customer segment breakdown
        for segment_col, segment_label in [
            ('customer_segment', 'customer_segment'),
            ('order_priority', 'order_priority'),
        ]:
            seg_sql = f"""
                SELECT
                    '{exp_id}' AS experiment_id,
                    '{segment_label}' AS segment_type,
                    {segment_col} AS segment_value,
                    group_name,
                    COUNT(*) AS n,
                    AVG(metric_value) AS mean_value,
                    VARIANCE(metric_value) AS var_value
                FROM ({base_sql})
                WHERE {segment_col} IS NOT NULL
                GROUP BY {segment_col}, group_name
                HAVING COUNT(*) >= 10
            """
            seg_df = fast_query_fn(conn, seg_sql)
            if len(seg_df) > 0:
                seg_df['experiment_type'] = exp_type
                seg_df['status'] = status
                # Apply treatment effect
                treat_mask = seg_df['group_name'] == 'Treatment'
                seg_df.loc[treat_mask, 'mean_value'] = seg_df.loc[treat_mask, 'mean_value'] * multiplier
                seg_df.loc[treat_mask, 'var_value'] = seg_df.loc[treat_mask, 'var_value'] * (multiplier ** 2)
                all_segments.append(seg_df)

        # Warehouse region breakdown
        region_sql = f"""
            SELECT
                '{exp_id}' AS experiment_id,
                'region' AS segment_type,
                w.region AS segment_value,
                sub.group_name,
                COUNT(*) AS n,
                AVG(sub.metric_value) AS mean_value,
                VARIANCE(sub.metric_value) AS var_value
            FROM ({base_sql}) sub
            JOIN RAW.DIM_WAREHOUSE w ON sub.warehouse_id = w.warehouse_id
            WHERE w.region IS NOT NULL
            GROUP BY w.region, sub.group_name
            HAVING COUNT(*) >= 10
        """
        reg_df = fast_query_fn(conn, region_sql)
        if len(reg_df) > 0:
            reg_df['experiment_type'] = exp_type
            reg_df['status'] = status
            treat_mask = reg_df['group_name'] == 'Treatment'
            reg_df.loc[treat_mask, 'mean_value'] = reg_df.loc[treat_mask, 'mean_value'] * multiplier
            reg_df.loc[treat_mask, 'var_value'] = reg_df.loc[treat_mask, 'var_value'] * (multiplier ** 2)
            all_segments.append(reg_df)

    if all_segments:
        return pd.concat(all_segments, ignore_index=True)
    return pd.DataFrame()