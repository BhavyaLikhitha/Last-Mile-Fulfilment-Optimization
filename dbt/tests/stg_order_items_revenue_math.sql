{{ config(severity='error', tags=['staging','revenue','math']) }}

select *
from {{ ref('stg_order_items') }}
where abs(revenue - ((quantity * unit_price) - coalesce(discount_amount, 0))) > 0.01