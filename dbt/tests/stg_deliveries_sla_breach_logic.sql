{{ config(severity='error', tags=['staging','sla','logic']) }}

select *
from {{ ref('stg_deliveries') }}
where sla_breach_flag = true
  and actual_delivery_minutes <= sla_minutes