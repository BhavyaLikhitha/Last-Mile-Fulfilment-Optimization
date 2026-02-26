/*
    stg_order_items — Silver layer
    Source: RAW fact_order_items
    Transformations:
      - Type casting
      - Adds net_revenue (revenue - discount)
      - Adds has_discount flag
      - Filters invalid rows (quantity <= 0)
*/

with source as (
    select * from {{ source('raw', 'fact_order_items') }}
),

transformed as (
    select
        cast(order_item_id as varchar(30)) as order_item_id,
        cast(order_id as varchar(30)) as order_id,
        cast(product_id as varchar(20)) as product_id,
        cast(quantity as integer) as quantity,
        cast(unit_price as decimal(10,2)) as unit_price,
        cast(coalesce(discount_amount, 0) as decimal(10,2)) as discount_amount,
        cast(revenue as decimal(10,2)) as revenue,

        -- Derived: was a discount applied?
        case when coalesce(discount_amount, 0) > 0 then true else false end as has_discount,

        -- Derived: discount percentage
        case
            when unit_price * quantity > 0 then round(coalesce(discount_amount, 0) / (unit_price * quantity) * 100, 2)
            else 0
        end as discount_pct,

        -- Audit
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        cast(batch_id as varchar(50)) as batch_id

    from source
    where quantity > 0
      and unit_price > 0
)

select * from transformed