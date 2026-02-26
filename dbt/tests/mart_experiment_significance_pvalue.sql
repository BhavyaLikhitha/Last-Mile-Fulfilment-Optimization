{{ config(severity='warn', tags=['marts','experiments','stats']) }}

select *
from {{ ref('mart_experiment_results') }}
where upper(is_significant) = 'TRUE'
  and try_to_number(p_value) >= 0.05