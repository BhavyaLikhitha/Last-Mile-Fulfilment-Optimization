"""
Uplift Analysis
Computes segment-level uplift to answer: does the treatment work better
for certain customer types or order priorities?

Segments analyzed:
  - customer_segment : Premium, Regular, Occasional
  - order_priority   : Standard, Express, Same-Day
  - warehouse_region : Northeast, West, Midwest, South, Northwest, Southeast, Mountain, Mid-Atlantic

Results saved to experimentation/results/uplift_{experiment_id}.csv
"""

import os
import numpy as np
import pandas as pd
from scipy import stats


ALPHA = 0.05


def compute_segment_uplift(
    df: pd.DataFrame,
    segment_col: str,
    experiment_id: str
) -> pd.DataFrame:
    """
    Compute uplift (Treatment vs Control mean difference) for each value
    of a segment column.

    Args:
        df          : observations DataFrame with group_name, metric_value, segment_col
        segment_col : column to segment by (customer_segment, order_priority, etc.)
        experiment_id: for labeling

    Returns:
        DataFrame with one row per segment value showing uplift metrics
    """
    if segment_col not in df.columns:
        return pd.DataFrame()

    results = []
    for segment_val in df[segment_col].dropna().unique():
        seg_df    = df[df[segment_col] == segment_val]
        control   = seg_df[seg_df['group_name'] == 'Control']['metric_value'].values
        treatment = seg_df[seg_df['group_name'] == 'Treatment']['metric_value'].values

        if len(control) < 10 or len(treatment) < 10:
            continue

        mean_c    = control.mean()
        mean_t    = treatment.mean()
        diff      = mean_t - mean_c
        pct_change = (diff / mean_c * 100) if mean_c != 0 else 0

        try:
            _, p_value = stats.ttest_ind(control, treatment, equal_var=False)
        except Exception:
            p_value = None

        results.append({
            'experiment_id' : experiment_id,
            'segment_type'  : segment_col,
            'segment_value' : segment_val,
            'n_control'     : len(control),
            'n_treatment'   : len(treatment),
            'mean_control'  : round(mean_c, 4),
            'mean_treatment': round(mean_t, 4),
            'mean_difference': round(diff, 4),
            'pct_change'    : round(pct_change, 2),
            'p_value'       : round(float(p_value), 6) if p_value is not None else None,
            'is_significant': bool(p_value < ALPHA) if p_value is not None else None,
        })

    return pd.DataFrame(results)


def run_uplift_analysis(
    experiment_data: dict,
    warehouses: pd.DataFrame = None
) -> dict:
    """
    Run full uplift analysis for all experiments across all segment dimensions.

    Args:
        experiment_data: dict mapping experiment_id → observations DataFrame
        warehouses     : dim_warehouse for region mapping (optional)

    Returns:
        dict mapping experiment_id → combined uplift DataFrame
    """
    os.makedirs('experimentation/results', exist_ok=True)

    all_uplift = {}

    for exp_id, df in experiment_data.items():
        print(f"  Uplift analysis for {exp_id}...")
        segment_dfs = []

        # Segment 1: customer_segment
        seg_df = compute_segment_uplift(df, 'customer_segment', exp_id)
        if len(seg_df) > 0:
            segment_dfs.append(seg_df)
            print(f"    customer_segment: {len(seg_df)} segments")

        # Segment 2: order_priority
        seg_df = compute_segment_uplift(df, 'order_priority', exp_id)
        if len(seg_df) > 0:
            segment_dfs.append(seg_df)
            print(f"    order_priority: {len(seg_df)} segments")

        # Segment 3: warehouse region (if warehouse dimension provided)
        if warehouses is not None and 'warehouse_id_obs' in df.columns:
            df_with_region = df.merge(
                warehouses[['warehouse_id', 'region']],
                left_on='warehouse_id_obs', right_on='warehouse_id', how='left'
            )
            seg_df = compute_segment_uplift(df_with_region, 'region', exp_id)
            if len(seg_df) > 0:
                segment_dfs.append(seg_df)
                print(f"    warehouse_region: {len(seg_df)} segments")

        if segment_dfs:
            combined = pd.concat(segment_dfs, ignore_index=True)
            output_path = f'experimentation/results/uplift_{exp_id}.csv'
            combined.to_csv(output_path, index=False)
            print(f"    Saved to {output_path}")
            all_uplift[exp_id] = combined
        else:
            print(f"    No segment data available")

    return all_uplift


def print_uplift_highlights(all_uplift: dict, top_n: int = 3):
    """Print the top segment uplifts across all experiments."""
    print("\n" + "=" * 70)
    print("  UPLIFT HIGHLIGHTS — TOP SEGMENTS BY TREATMENT EFFECT")
    print("=" * 70)

    all_rows = []
    for exp_id, df in all_uplift.items():
        df['experiment_id'] = exp_id
        all_rows.append(df)

    if not all_rows:
        print("  No uplift data available")
        return

    combined = pd.concat(all_rows, ignore_index=True)
    combined = combined[combined['is_significant'] == True]
    combined = combined.sort_values('pct_change').head(top_n * 2)

    for _, row in combined.iterrows():
        direction = "reduction" if row['pct_change'] < 0 else "increase"
        print(f"  {row['experiment_id']} | {row['segment_type']}={row['segment_value']} | "
              f"{abs(row['pct_change']):.1f}% {direction} | p={row['p_value']:.4f}")
    print("=" * 70)