{{ config(severity='error', tags=['intermediate','experiments','integrity']) }}

select *
from {{ ref('int_order_enriched') }}
where is_experiment_order = true
  and (experiment_id is null or experiment_group is null)