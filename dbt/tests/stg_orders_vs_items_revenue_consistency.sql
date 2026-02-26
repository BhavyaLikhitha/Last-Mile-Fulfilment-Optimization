{{ config(severity='error', tags=['staging','revenue','reconciliation']) }}

with item_totals as (
    select order_id, sum(revenue) as item_revenue
    from {{ ref('stg_order_items') }}
    group by 1
)
select o.order_id
from {{ ref('stg_orders') }} o
join item_totals i using (order_id)
where abs(o.total_amount - i.item_revenue) > 0.01