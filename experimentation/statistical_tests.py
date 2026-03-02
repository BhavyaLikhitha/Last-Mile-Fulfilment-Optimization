# """
# Statistical Tests
# Runs Welch's two-sample t-test and computes 95% confidence intervals
# for each experiment's Control vs Treatment comparison.

# Test: scipy.stats.ttest_ind with equal_var=False (Welch's t-test)
# CI  : 95% confidence interval on the mean difference (Treatment - Control)
# """

# import numpy as np
# import pandas as pd
# from scipy import stats



# ALPHA = 0.05   # significance threshold


# def welch_ttest(control: np.ndarray, treatment: np.ndarray) -> dict:
#     """
#     Run Welch's two-sample t-test and compute 95% CI on the mean difference.

#     Args:
#         control  : array of metric values for Control group
#         treatment: array of metric values for Treatment group

#     Returns:
#         dict with t_stat, p_value, ci_lower, ci_upper, is_significant,
#                    mean_control, mean_treatment, n_control, n_treatment,
#                    mean_difference, pct_change
#     """
#     control   = np.array(control, dtype=float)
#     treatment = np.array(treatment, dtype=float)

#     # Remove NaN
#     control   = control[~np.isnan(control)]
#     treatment = treatment[~np.isnan(treatment)]

#     if len(control) < 2 or len(treatment) < 2:
#         return _empty_result()

#     # Welch's t-test
#     t_stat, p_value = stats.ttest_ind(control, treatment, equal_var=False)

#     # 95% Confidence interval on the mean difference
#     mean_c  = control.mean()
#     mean_t  = treatment.mean()
#     mean_diff = mean_t - mean_c

#     se = np.sqrt(control.var(ddof=1) / len(control) + treatment.var(ddof=1) / len(treatment))

#     # Welch-Satterthwaite degrees of freedom
#     df = _welch_df(control, treatment)
#     t_crit = stats.t.ppf(1 - ALPHA / 2, df)

#     ci_lower = round(mean_diff - t_crit * se, 4)
#     ci_upper = round(mean_diff + t_crit * se, 4)

#     pct_change = ((mean_t - mean_c) / mean_c * 100) if mean_c != 0 else 0

#     return {
#         't_stat'         : round(float(t_stat), 4),
#         'p_value'        : round(float(p_value), 6),
#         'ci_lower'       : ci_lower,
#         'ci_upper'       : ci_upper,
#         'is_significant' : bool(p_value < ALPHA),
#         'mean_control'   : round(mean_c, 4),
#         'mean_treatment' : round(mean_t, 4),
#         'n_control'      : len(control),
#         'n_treatment'    : len(treatment),
#         'mean_difference': round(mean_diff, 4),
#         'pct_change'     : round(pct_change, 2),
#     }


# def _welch_df(a: np.ndarray, b: np.ndarray) -> float:
#     """Welch-Satterthwaite degrees of freedom."""
#     va, vb = a.var(ddof=1), b.var(ddof=1)
#     na, nb = len(a), len(b)
#     num    = (va / na + vb / nb) ** 2
#     denom  = (va / na) ** 2 / (na - 1) + (vb / nb) ** 2 / (nb - 1)
#     return num / denom if denom > 0 else min(na, nb) - 1


# def _empty_result() -> dict:
#     """Return empty result when insufficient data."""
#     return {
#         't_stat': None, 'p_value': None, 'ci_lower': None, 'ci_upper': None,
#         'is_significant': None, 'mean_control': None, 'mean_treatment': None,
#         'n_control': 0, 'n_treatment': 0, 'mean_difference': None, 'pct_change': None
#     }


# def run_all_tests(experiment_data: dict) -> pd.DataFrame:
#     """
#     Run Welch's t-test for all experiments.

#     Args:
#         experiment_data: dict mapping experiment_id → observations DataFrame
#                          (output of assignment_engine.build_all_experiment_data)

#     Returns:
#         DataFrame with one row per experiment containing test results
#     """
#     results = []

#     for exp_id, df in experiment_data.items():
#         print(f"  Testing {exp_id}...")

#         control   = df[df['group_name'] == 'Control']['metric_value'].values
#         treatment = df[df['group_name'] == 'Treatment']['metric_value'].values

#         test_result = welch_ttest(control, treatment)
#         test_result['experiment_id'] = exp_id
#         test_result['metric_name']   = df['metric_name'].iloc[0] if 'metric_name' in df.columns else 'unknown'
#         test_result['experiment_type'] = df['experiment_type'].iloc[0] if 'experiment_type' in df.columns else 'unknown'
#         test_result['status']        = df['status'].iloc[0] if 'status' in df.columns else 'unknown'

#         sig_label = "SIGNIFICANT" if test_result['is_significant'] else "not significant"
#         p_display = f"{test_result['p_value']:.4f}" if test_result['p_value'] is not None else "N/A"
#         print(f"    p={p_display} | {sig_label} | lift={test_result['pct_change']}%")

#         results.append(test_result)

#     return pd.DataFrame(results)


# def print_summary(results_df: pd.DataFrame):
#     """Print a formatted summary of all test results."""
#     print("\n" + "=" * 70)
#     print("  A/B TEST RESULTS SUMMARY")
#     print("=" * 70)
#     print(f"  {'ID':<10} {'Type':<25} {'p-value':<10} {'Lift %':<10} {'Significant'}")
#     print(f"  {'-'*10} {'-'*25} {'-'*10} {'-'*10} {'-'*11}")

#     for _, row in results_df.iterrows():
#         p_val = f"{row['p_value']:.4f}" if row['p_value'] is not None else "N/A"
#         lift  = f"{row['pct_change']:.2f}%" if row['pct_change'] is not None else "N/A"
#         sig   = "YES ***" if row['is_significant'] else "no"
#         print(f"  {row['experiment_id']:<10} {row['experiment_type']:<25} {p_val:<10} {lift:<10} {sig}")

#     if 'is_significant' in results_df.columns:
#         n_sig = results_df['is_significant'].sum()
#         n_total = len(results_df)
#         print(f"\n  {n_sig}/{n_total} experiments significant at alpha=0.05")
#     print("=" * 70)

"""
Statistical Tests (Memory-Optimized)
Runs Welch's t-test from pre-aggregated summary statistics
(n, mean, variance) instead of raw observation arrays.

This is mathematically equivalent to running ttest_ind on raw data
but uses ~0 MB instead of hundreds of MB.
"""

import numpy as np
import pandas as pd
from scipy import stats

ALPHA = 0.05


def welch_ttest_from_stats(
    n_c: int, mean_c: float, var_c: float,
    n_t: int, mean_t: float, var_t: float
) -> dict:
    """
    Welch's t-test from summary statistics.
    Mathematically identical to scipy.stats.ttest_ind(equal_var=False).

    Args:
        n_c, mean_c, var_c: Control group count, mean, variance
        n_t, mean_t, var_t: Treatment group count, mean, variance

    Returns:
        dict with t_stat, p_value, ci_lower, ci_upper, is_significant, etc.
    """
    if n_c < 2 or n_t < 2 or var_c is None or var_t is None:
        return _empty_result()

    # Handle zero/NaN variance
    var_c = max(float(var_c), 1e-10)
    var_t = max(float(var_t), 1e-10)

    mean_diff = float(mean_t) - float(mean_c)
    se = np.sqrt(var_c / n_c + var_t / n_t)

    if se == 0:
        return _empty_result()

    t_stat = mean_diff / se

    # Welch-Satterthwaite degrees of freedom
    num = (var_c / n_c + var_t / n_t) ** 2
    denom = (var_c / n_c) ** 2 / (n_c - 1) + (var_t / n_t) ** 2 / (n_t - 1)
    df = num / denom if denom > 0 else min(n_c, n_t) - 1

    p_value = 2 * stats.t.sf(abs(t_stat), df)

    t_crit = stats.t.ppf(1 - ALPHA / 2, df)
    ci_lower = round(mean_diff - t_crit * se, 4)
    ci_upper = round(mean_diff + t_crit * se, 4)

    pct_change = (mean_diff / float(mean_c) * 100) if float(mean_c) != 0 else 0

    return {
        't_stat'         : round(float(t_stat), 4),
        'p_value'        : round(float(p_value), 6),
        'ci_lower'       : ci_lower,
        'ci_upper'       : ci_upper,
        'is_significant' : bool(p_value < ALPHA),
        'mean_control'   : round(float(mean_c), 4),
        'mean_treatment' : round(float(mean_t), 4),
        'n_control'      : int(n_c),
        'n_treatment'    : int(n_t),
        'mean_difference': round(mean_diff, 4),
        'pct_change'     : round(pct_change, 2),
    }


def _empty_result() -> dict:
    return {
        't_stat': None, 'p_value': None, 'ci_lower': None, 'ci_upper': None,
        'is_significant': None, 'mean_control': None, 'mean_treatment': None,
        'n_control': 0, 'n_treatment': 0, 'mean_difference': None, 'pct_change': None
    }


def run_all_tests(summary_stats: pd.DataFrame) -> pd.DataFrame:
    """
    Run Welch's t-test for all experiments using pre-aggregated stats.

    Args:
        summary_stats: DataFrame with experiment_id, group_name, n, mean_value, var_value

    Returns:
        DataFrame with one row per experiment containing test results
    """
    results = []

    for exp_id in summary_stats['experiment_id'].unique():
        exp_df = summary_stats[summary_stats['experiment_id'] == exp_id]
        control = exp_df[exp_df['group_name'] == 'Control']
        treatment = exp_df[exp_df['group_name'] == 'Treatment']

        if len(control) == 0 or len(treatment) == 0:
            continue

        c = control.iloc[0]
        t = treatment.iloc[0]

        print(f"  Testing {exp_id}...")
        test_result = welch_ttest_from_stats(
            n_c=int(c['n']), mean_c=float(c['mean_value']), var_c=float(c['var_value']),
            n_t=int(t['n']), mean_t=float(t['mean_value']), var_t=float(t['var_value'])
        )
        test_result['experiment_id'] = exp_id
        test_result['metric_name'] = c.get('metric_name', 'unknown')
        test_result['experiment_type'] = c.get('experiment_type', 'unknown')
        test_result['status'] = c.get('status', 'unknown')

        sig_label = "SIGNIFICANT" if test_result['is_significant'] else "not significant"
        p_display = f"{test_result['p_value']:.4f}" if test_result['p_value'] is not None else "N/A"
        print(f"    p={p_display} | {sig_label} | lift={test_result['pct_change']}%")

        results.append(test_result)

    return pd.DataFrame(results)


def print_summary(results_df: pd.DataFrame):
    print("\n" + "=" * 70)
    print("  A/B TEST RESULTS SUMMARY")
    print("=" * 70)
    print(f"  {'ID':<10} {'Type':<25} {'p-value':<10} {'Lift %':<10} {'Significant'}")
    print(f"  {'-'*10} {'-'*25} {'-'*10} {'-'*10} {'-'*11}")

    for _, row in results_df.iterrows():
        p_val = f"{row['p_value']:.4f}" if row['p_value'] is not None else "N/A"
        lift = f"{row['pct_change']:.2f}%" if row['pct_change'] is not None else "N/A"
        sig = "YES ***" if row['is_significant'] else "no"
        print(f"  {row['experiment_id']:<10} {row['experiment_type']:<25} {p_val:<10} {lift:<10} {sig}")

    if 'is_significant' in results_df.columns:
        n_sig = results_df['is_significant'].sum()
        n_total = len(results_df)
        print(f"\n  {n_sig}/{n_total} experiments significant at alpha=0.05")
    print("=" * 70)