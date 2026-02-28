{{ config(severity='warn', tags=['marts','experiments','stats']) }}

/*
    Test: mart_experiment_significance_pvalue
    Flags rows where is_significant = TRUE but p_value >= 0.05.
    These two columns must agree — if the Python engine marks something significant,
    the p_value must back it up.
    Returns violating rows. Passes when 0 rows returned.
    Severity: warn (p_value is null until experimentation engine runs)
*/

select *
from {{ ref('mart_experiment_results') }}
where is_significant = true
  and p_value >= 0.05