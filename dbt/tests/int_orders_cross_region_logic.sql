{{ config(severity='warn', tags=['intermediate','allocation','logic']) }}

select *
from {{ ref('int_order_enriched') }}
where assigned_warehouse_id != nearest_warehouse_id
  and coalesce(is_cross_region, false) = false