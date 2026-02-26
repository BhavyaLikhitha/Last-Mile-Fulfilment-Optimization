{{ config(severity='error', tags=['staging','inventory','sanity']) }}

select *
from {{ ref('stg_inventory_snapshot') }}
where closing_stock < 0