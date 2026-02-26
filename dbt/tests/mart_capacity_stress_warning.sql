{{ config(severity='warn') }}

select *
from {{ ref('mart_daily_warehouse_kpis') }}
where capacity_utilization_pct > 110