{{ config(severity='error') }}

select *
from {{ ref('stg_inventory_snapshot') }}
where
    closing_stock != greatest(
        coalesce(opening_stock,0)
      + coalesce(units_received,0)
      + coalesce(units_returned,0)
      - coalesce(units_sold,0),
        0
    )