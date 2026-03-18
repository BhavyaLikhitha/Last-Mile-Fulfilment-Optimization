# """
# Uplift Analysis
# Computes segment-level uplift to answer: does the treatment work better
# for certain customer types or order priorities?

# Segments analyzed:
#   - customer_segment : Premium, Regular, Occasional
#   - order_priority   : Standard, Express, Same-Day
#   - warehouse_region : Northeast, West, Midwest, South, Northwest, Southeast, Mountain, Mid-Atlantic

# Results saved to experimentation/results/uplift_{experiment_id}.csv
# """

# import os
# import numpy as np
# import pandas as pd
# from scipy import stats


# ALPHA = 0.05


# def compute_segment_uplift(
#     df: pd.DataFrame,
#     segment_col: str,
#     experiment_id: str
# ) -> pd.DataFrame:
#     """
#     Compute uplift (Treatment vs Control mean difference) for each value
#     of a segment column.

#     Args:
#         df          : observations DataFrame with group_name, metric_value, segment_col
#         segment_col : column to segment by (customer_segment, order_priority, etc.)
#         experiment_id: for labeling

#     Returns:
#         DataFrame with one row per segment value showing uplift metrics
#     """
#     if segment_col not in df.columns:
#         return pd.DataFrame()

#     results = []
#     for segment_val in df[segment_col].dropna().unique():
#         seg_df    = df[df[segment_col] == segment_val]
#         control   = seg_df[seg_df['group_name'] == 'Control']['metric_value'].values
#         treatment = seg_df[seg_df['group_name'] == 'Treatment']['metric_value'].values

#         if len(control) < 10 or len(treatment) < 10:
#             continue

#         mean_c    = control.mean()
#         mean_t    = treatment.mean()
#         diff      = mean_t - mean_c
#         pct_change = (diff / mean_c * 100) if mean_c != 0 else 0

#         try:
#             _, p_value = stats.ttest_ind(control, treatment, equal_var=False)
#         except Exception:
#             p_value = None

#         results.append({
#             'experiment_id' : experiment_id,
#             'segment_type'  : segment_col,
#             'segment_value' : segment_val,
#             'n_control'     : len(control),
#             'n_treatment'   : len(treatment),
#             'mean_control'  : round(mean_c, 4),
#             'mean_treatment': round(mean_t, 4),
#             'mean_difference': round(diff, 4),
#             'pct_change'    : round(pct_change, 2),
#             'p_value'       : round(float(p_value), 6) if p_value is not None else None,
#             'is_significant': bool(p_value < ALPHA) if p_value is not None else None,
#         })

#     return pd.DataFrame(results)


# def run_uplift_analysis(
#     experiment_data: dict,
#     warehouses: pd.DataFrame = None
# ) -> dict:
#     """
#     Run full uplift analysis for all experiments across all segment dimensions.

#     Args:
#         experiment_data: dict mapping experiment_id → observations DataFrame
#         warehouses     : dim_warehouse for region mapping (optional)

#     Returns:
#         dict mapping experiment_id → combined uplift DataFrame
#     """
#     os.makedirs('experimentation/results', exist_ok=True)

#     all_uplift = {}

#     for exp_id, df in experiment_data.items():
#         print(f"  Uplift analysis for {exp_id}...")
#         segment_dfs = []

#         # Segment 1: customer_segment
#         seg_df = compute_segment_uplift(df, 'customer_segment', exp_id)
#         if len(seg_df) > 0:
#             segment_dfs.append(seg_df)
#             print(f"    customer_segment: {len(seg_df)} segments")

#         # Segment 2: order_priority
#         seg_df = compute_segment_uplift(df, 'order_priority', exp_id)
#         if len(seg_df) > 0:
#             segment_dfs.append(seg_df)
#             print(f"    order_priority: {len(seg_df)} segments")

#         # Segment 3: warehouse region (if warehouse dimension provided)
#         if warehouses is not None and 'warehouse_id_obs' in df.columns:
#             df_with_region = df.merge(
#                 warehouses[['warehouse_id', 'region']],
#                 left_on='warehouse_id_obs', right_on='warehouse_id', how='left'
#             )
#             seg_df = compute_segment_uplift(df_with_region, 'region', exp_id)
#             if len(seg_df) > 0:
#                 segment_dfs.append(seg_df)
#                 print(f"    warehouse_region: {len(seg_df)} segments")

#         if segment_dfs:
#             combined = pd.concat(segment_dfs, ignore_index=True)
#             output_path = f'experimentation/results/uplift_{exp_id}.csv'
#             combined.to_csv(output_path, index=False)
#             print(f"    Saved to {output_path}")
#             all_uplift[exp_id] = combined
#         else:
#             print(f"    No segment data available")

#     return all_uplift


# def print_uplift_highlights(all_uplift: dict, top_n: int = 3):
#     """Print the top segment uplifts across all experiments."""
#     print("\n" + "=" * 70)
#     print("  UPLIFT HIGHLIGHTS — TOP SEGMENTS BY TREATMENT EFFECT")
#     print("=" * 70)

#     all_rows = []
#     for exp_id, df in all_uplift.items():
#         df['experiment_id'] = exp_id
#         all_rows.append(df)

#     if not all_rows:
#         print("  No uplift data available")
#         return

#     combined = pd.concat(all_rows, ignore_index=True)
#     combined = combined[combined['is_significant'] == True]
#     combined = combined.sort_values('pct_change').head(top_n * 2)

#     for _, row in combined.iterrows():
#         direction = "reduction" if row['pct_change'] < 0 else "increase"
#         print(f"  {row['experiment_id']} | {row['segment_type']}={row['segment_value']} | "
#               f"{abs(row['pct_change']):.1f}% {direction} | p={row['p_value']:.4f}")
#     print("=" * 70)

"""
Uplift Analysis (Memory-Optimized)
Computes segment-level uplift from pre-aggregated summary statistics.
All heavy computation done in Snowflake — Python only processes ~100 rows.
"""

import os

import numpy as np
import pandas as pd
from scipy import stats

ALPHA = 0.05


def compute_uplift_from_stats(segment_stats: pd.DataFrame) -> pd.DataFrame:
    """
    Compute uplift metrics from pre-aggregated segment stats.
    Pivots Control vs Treatment stats and computes t-test per segment.

    Args:
        segment_stats: DataFrame with experiment_id, segment_type, segment_value,
                       group_name, n, mean_value, var_value

    Returns:
        DataFrame with one row per experiment × segment showing uplift metrics
    """
    if len(segment_stats) == 0:
        return pd.DataFrame()

    results = []
    groups = segment_stats.groupby(["experiment_id", "segment_type", "segment_value"])

    for (exp_id, seg_type, seg_val), grp in groups:
        control = grp[grp["group_name"] == "Control"]
        treatment = grp[grp["group_name"] == "Treatment"]

        if len(control) == 0 or len(treatment) == 0:
            continue

        c = control.iloc[0]
        t = treatment.iloc[0]

        n_c, mean_c, var_c = int(c["n"]), float(c["mean_value"]), float(c.get("var_value", 0))
        n_t, mean_t, var_t = int(t["n"]), float(t["mean_value"]), float(t.get("var_value", 0))

        if n_c < 10 or n_t < 10:
            continue

        diff = mean_t - mean_c
        pct_change = (diff / mean_c * 100) if mean_c != 0 else 0

        # Welch t-test from summary stats
        p_value = None
        var_c = max(var_c, 1e-10)
        var_t = max(var_t, 1e-10)
        se = np.sqrt(var_c / n_c + var_t / n_t)
        if se > 0:
            t_stat = diff / se
            num = (var_c / n_c + var_t / n_t) ** 2
            denom = (var_c / n_c) ** 2 / (n_c - 1) + (var_t / n_t) ** 2 / (n_t - 1)
            df = num / denom if denom > 0 else min(n_c, n_t) - 1
            p_value = float(2 * stats.t.sf(abs(t_stat), df))

        results.append(
            {
                "experiment_id": exp_id,
                "segment_type": seg_type,
                "segment_value": seg_val,
                "n_control": n_c,
                "n_treatment": n_t,
                "mean_control": round(mean_c, 4),
                "mean_treatment": round(mean_t, 4),
                "mean_difference": round(diff, 4),
                "pct_change": round(pct_change, 2),
                "p_value": round(p_value, 6) if p_value is not None else None,
                "is_significant": bool(p_value < ALPHA) if p_value is not None else None,
            }
        )

    return pd.DataFrame(results)


def run_uplift_analysis(segment_stats: pd.DataFrame) -> dict:
    """
    Run uplift analysis from pre-aggregated segment stats.
    Saves per-experiment CSVs to experimentation/results/.

    Args:
        segment_stats: pre-aggregated stats from assignment_engine.load_segment_stats

    Returns:
        dict mapping experiment_id → uplift DataFrame
    """
    os.makedirs("experimentation/results", exist_ok=True)

    uplift_df = compute_uplift_from_stats(segment_stats)
    if len(uplift_df) == 0:
        print("  No segment data available")
        return {}

    all_uplift = {}
    for exp_id in uplift_df["experiment_id"].unique():
        exp_uplift = uplift_df[uplift_df["experiment_id"] == exp_id].copy()
        output_path = f"experimentation/results/uplift_{exp_id}.csv"
        exp_uplift.to_csv(output_path, index=False)

        n_segments = len(exp_uplift)
        n_sig = exp_uplift["is_significant"].sum() if "is_significant" in exp_uplift.columns else 0
        print(f"  {exp_id}: {n_segments} segments, {n_sig} significant → {output_path}")

        all_uplift[exp_id] = exp_uplift

    return all_uplift


def print_uplift_highlights(all_uplift: dict, top_n: int = 3):
    print("\n" + "=" * 70)
    print("  UPLIFT HIGHLIGHTS — TOP SEGMENTS BY TREATMENT EFFECT")
    print("=" * 70)

    all_rows = []
    for exp_id, df in all_uplift.items():
        all_rows.append(df)

    if not all_rows:
        print("  No uplift data available")
        return

    combined = pd.concat(all_rows, ignore_index=True)
    combined = combined[combined["is_significant"]]
    combined = combined.sort_values("pct_change").head(top_n * 2)

    for _, row in combined.iterrows():
        direction = "reduction" if row["pct_change"] < 0 else "increase"
        print(
            f"  {row['experiment_id']} | {row['segment_type']}={row['segment_value']} | "
            f"{abs(row['pct_change']):.1f}% {direction} | p={row['p_value']:.4f}"
        )
    print("=" * 70)
