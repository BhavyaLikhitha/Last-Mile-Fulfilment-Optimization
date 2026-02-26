/*
    stg_products — Silver layer
    Source: SCD Type 2 snapshot (tracks price changes)
    Transformations:
      - Reads from snapshot for full history
      - Casts types explicitly
      - Adds profit_margin derived column
      - Adds price_tier classification
      - Filters out invalid records (negative prices)
      - is_current flag for latest version
*/

with snapshot as (
    select * from {{ ref('snap_product') }}
),

transformed as (
    select
        product_id,
        product_name,
        cast(category as varchar(50)) as category,
        cast(subcategory as varchar(50)) as subcategory,
        cast(cost_price as decimal(10,2)) as cost_price,
        cast(selling_price as decimal(10,2)) as selling_price,
        cast(weight_kg as decimal(5,2)) as weight_kg,
        cast(lead_time_days as integer) as lead_time_days,
        cast(reorder_point as integer) as reorder_point,
        cast(safety_stock as integer) as safety_stock,
        cast(is_perishable as boolean) as is_perishable,

        -- Derived: profit margin percentage
        round((selling_price - cost_price) / nullif(cost_price, 0) * 100, 2) as profit_margin_pct,

        -- Derived: price tier classification
        case
            when selling_price >= 200 then 'Premium'
            when selling_price >= 50 then 'Mid-Range'
            else 'Budget'
        end as price_tier,

        -- Derived: inventory criticality based on lead time
        case
            when lead_time_days >= 10 then 'High'
            when lead_time_days >= 5 then 'Medium'
            else 'Low'
        end as lead_time_risk,

        -- SCD Type 2 metadata
        {# dbt_valid_from,
        dbt_valid_to,
        case when dbt_valid_to is null then true else false end as is_current, #}

        dbt_valid_from as valid_from,
coalesce(dbt_valid_to, '9999-12-31'::timestamp) as valid_to,
case when dbt_valid_to is null then true else false end as is_current,

        created_at
    from snapshot
    where cost_price > 0
      and selling_price > 0
      and selling_price >= cost_price
)

select * from transformed