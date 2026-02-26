{{ config(severity='error', tags=['marts','cost','math']) }}

select *
from {{ ref('mart_cost_optimization') }}
where round(savings_amount, 2) != round(baseline_total_cost - optimized_total_cost, 2)