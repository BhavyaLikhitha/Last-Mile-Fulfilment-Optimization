{{ config(severity='error', tags=['staging','deliveries','time']) }}

select *
from {{ ref('stg_deliveries') }}
where delivered_time < pickup_time
   or pickup_time < assigned_time