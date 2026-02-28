"""
Assignment Engine
Loads experiment assignments + raw order/delivery data from Snowflake,
injects calibrated treatment effects per experiment type, and prepares
per-observation DataFrames ready for statistical testing.

Treatment effect sizes are calibrated from published fulfillment research:
  - inventory_policy  : 8-15% cost reduction (SCM literature)
  - routing_algorithm : 10-15% delivery time reduction (routing research)
  - warehouse_allocation: 6-10% cost reduction (Amazon benchmarks)

Seeded RNG (seed=42) ensures full reproducibility.
"""

import numpy as np
import pandas as pd

# ── Treatment Effect Configuration ───────────────────────────
TREATMENT_EFFECTS = {
    'inventory_policy': {
        'metric'         : 'total_fulfillment_cost',
        'effect_range'   : (0.08, 0.15),   # 8-15% reduction
        'noise_std'      : 0.02,
        'active_scale'   : 0.50,           # active experiments get 50% of full effect
    },
    'routing_algorithm': {
        'metric'         : 'actual_delivery_minutes',
        'effect_range'   : (0.10, 0.15),   # 10-15% reduction
        'noise_std'      : 0.02,
        'active_scale'   : 0.50,
    },
    'warehouse_allocation': {
        'metric'         : 'total_fulfillment_cost',
        'effect_range'   : (0.06, 0.10),   # 6-10% reduction
        'noise_std'      : 0.015,
        'active_scale'   : 0.40,
    },
}

# Per-experiment effect sizes (seeded for reproducibility)
EXPERIMENT_EFFECTS = {
    'EXP-001': 0.12,   # dynamic reorder — strong effect
    'EXP-002': 0.09,   # safety stock — moderate effect
    'EXP-003': 0.12,   # balanced routing
    'EXP-004': 0.11,   # load balanced driver
    'EXP-005': 0.08,   # cost optimal allocation
    'EXP-006': 0.07,   # capacity aware
    'EXP-007': 0.10,   # JIT reorder
    'EXP-008': 0.13,   # priority routing — strong effect
    'EXP-009': 0.04,   # active — partial effect (6% * 0.40 scale + noise)
    'EXP-010': 0.05,   # active — partial effect (ml reorder promising but early)
}


def load_experiment_data(conn) -> tuple:
    """
    Load all data needed for experimentation from Snowflake.

    Returns:
        experiments  : dim_experiments metadata
        assignments  : fact_experiment_assignments (order→group mapping)
        orders       : fact_orders with cost metrics
        deliveries   : fact_deliveries with time metrics
    """
    from ml.training.predict_and_writeback import fast_query

    print("  Loading dim_experiments...")
    experiments = fast_query(conn, "SELECT * FROM RAW.DIM_EXPERIMENTS")
    print(f"    {len(experiments):,} experiments")

    print("  Loading fact_experiment_assignments...")
    assignments = fast_query(conn, """
        SELECT assignment_id, experiment_id, order_id, group_name, assigned_at, warehouse_id
        FROM RAW.FACT_EXPERIMENT_ASSIGNMENTS
    """)
    print(f"    {len(assignments):,} assignments")

    print("  Loading fact_orders...")
    orders = fast_query(conn, """
        SELECT o.order_id, o.order_date, o.customer_id,
               o.assigned_warehouse_id, o.nearest_warehouse_id,
               o.order_priority, o.total_fulfillment_cost,
               o.total_amount, o.order_status,
               c.customer_segment
        FROM RAW.FACT_ORDERS o
        LEFT JOIN RAW.DIM_CUSTOMER c ON o.customer_id = c.customer_id
        WHERE o.order_status != 'Cancelled'
    """)
    print(f"    {len(orders):,} orders")

    print("  Loading fact_deliveries...")
    deliveries = fast_query(conn, """
        SELECT d.delivery_id, d.order_id, d.warehouse_id,
               d.actual_delivery_minutes, d.distance_km,
               d.on_time_flag, d.sla_breach_flag, d.delivery_status
        FROM RAW.FACT_DELIVERIES d
        WHERE d.delivery_status = 'Delivered'
          AND d.actual_delivery_minutes IS NOT NULL
    """)
    print(f"    {len(deliveries):,} deliveries")

    return experiments, assignments, orders, deliveries


def prepare_experiment_observations(
    experiment: pd.Series,
    assignments: pd.DataFrame,
    orders: pd.DataFrame,
    deliveries: pd.DataFrame
) -> pd.DataFrame:
    """
    For a single experiment, build a DataFrame with one row per observation
    (order or delivery depending on experiment type), with Control/Treatment labels
    and the primary metric value.

    Args:
        experiment  : single row from dim_experiments
        assignments : all experiment assignments
        orders      : all orders
        deliveries  : all deliveries

    Returns:
        DataFrame with columns: observation_id, group_name, metric_value,
                                customer_segment, order_priority, warehouse_id
    """
    exp_id   = experiment['experiment_id']
    exp_type = experiment['experiment_type']
    status   = experiment['status']

    # Filter assignments for this experiment
    exp_assignments = assignments[assignments['experiment_id'] == exp_id].copy()
    if len(exp_assignments) == 0:
        return pd.DataFrame()

    if exp_type in ('inventory_policy', 'warehouse_allocation'):
        # Join assignments → orders
        df = exp_assignments.merge(
            orders[['order_id', 'total_fulfillment_cost', 'customer_segment',
                    'order_priority', 'assigned_warehouse_id']],
            on='order_id', how='inner'
        )
        df = df.rename(columns={
            'order_id'             : 'observation_id',
            'total_fulfillment_cost': 'metric_value',
            'assigned_warehouse_id' : 'warehouse_id_obs'
        })
        df['metric_name'] = 'total_fulfillment_cost'

    elif exp_type == 'routing_algorithm':
        # Join assignments → orders → deliveries
        orders_with_seg = orders[['order_id', 'customer_segment', 'order_priority',
                                   'assigned_warehouse_id']]
        df = exp_assignments.merge(orders_with_seg, on='order_id', how='inner')
        df = df.merge(
            deliveries[['order_id', 'delivery_id', 'actual_delivery_minutes']],
            on='order_id', how='inner'
        )
        df = df.rename(columns={
            'delivery_id'             : 'observation_id',
            'actual_delivery_minutes' : 'metric_value',
            'assigned_warehouse_id'   : 'warehouse_id_obs'
        })
        df['metric_name'] = 'actual_delivery_minutes'
    else:
        return pd.DataFrame()

    # Keep only relevant columns
    df = df[['observation_id', 'group_name', 'metric_value',
             'customer_segment', 'order_priority', 'warehouse_id_obs']].copy()
    df = df.dropna(subset=['metric_value'])
    df['experiment_id'] = exp_id
    df['experiment_type'] = exp_type
    df['status'] = status

    return df


def inject_treatment_effects(df: pd.DataFrame, experiment_id: str, experiment_type: str, status: str) -> pd.DataFrame:
    """
    Apply calibrated treatment effect to Treatment group observations.
    Control group is untouched.

    The effect reduces the metric value for Treatment observations by a fixed
    percentage plus Gaussian noise. This simulates what the new policy would
    achieve based on published benchmarks.

    Args:
        df             : observations DataFrame with group_name and metric_value
        experiment_id  : e.g. 'EXP-001'
        experiment_type: 'inventory_policy', 'routing_algorithm', 'warehouse_allocation'
        status         : 'Completed' or 'Active'

    Returns:
        DataFrame with metric_value adjusted for Treatment group
    """
    rng = np.random.default_rng(seed=int(experiment_id.split('-')[1]) * 42)

    config      = TREATMENT_EFFECTS[experiment_type]
    base_effect = EXPERIMENT_EFFECTS.get(experiment_id, 0.08)

    # Active experiments get reduced effect (less data, earlier stage)
    if status == 'Active':
        base_effect = base_effect * config['active_scale']

    result = df.copy()
    treatment_mask = result['group_name'] == 'Treatment'
    n_treatment = treatment_mask.sum()

    if n_treatment == 0:
        return result

    # Per-observation noise: each Treatment observation gets slightly different effect
    noise = rng.normal(0, config['noise_std'], n_treatment)
    effective_effects = np.clip(base_effect + noise, 0.01, 0.25)

    # Reduce metric value for Treatment (lower cost/time = better)
    result.loc[treatment_mask, 'metric_value'] = (
        result.loc[treatment_mask, 'metric_value'] * (1 - effective_effects)
    )

    return result


def build_all_experiment_data(
    experiments: pd.DataFrame,
    assignments: pd.DataFrame,
    orders: pd.DataFrame,
    deliveries: pd.DataFrame
) -> dict:
    """
    Build observation DataFrames for all experiments with treatment effects injected.

    Returns:
        dict mapping experiment_id → DataFrame of observations
    """
    result = {}
    for _, exp in experiments.iterrows():
        exp_id = exp['experiment_id']
        print(f"  Preparing {exp_id}: {exp['experiment_name']} ({exp['status']})")

        df = prepare_experiment_observations(exp, assignments, orders, deliveries)
        if len(df) == 0:
            print(f"    No data found — skipping")
            continue

        df = inject_treatment_effects(df, exp_id, exp['experiment_type'], exp['status'])

        n_control   = (df['group_name'] == 'Control').sum()
        n_treatment = (df['group_name'] == 'Treatment').sum()
        print(f"    {len(df):,} observations — Control: {n_control:,}, Treatment: {n_treatment:,}")

        result[exp_id] = df

    return result