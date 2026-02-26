/*
    stg_inventory_snapshot — Silver layer
    Source: RAW fact_inventory_snapshot
    Transformations:
      - Type casting
      - Adds inventory_health classification
      - Adds stock_movement (net change for the day)
      - Caps days_of_supply at reasonable maximum
*/

with source as (
    select * from {{ source('raw', 'fact_inventory_snapshot') }}
),

transformed as (
    select
        cast(snapshot_date as date) as snapshot_date,
        cast(warehouse_id as varchar(20)) as warehouse_id,
        cast(product_id as varchar(20)) as product_id,
        cast(opening_stock as integer) as opening_stock,
        cast(units_sold as integer) as units_sold,
        cast(units_received as integer) as units_received,
        cast(units_returned as integer) as units_returned,
        cast(closing_stock as integer) as closing_stock,
        cast(stockout_flag as boolean) as stockout_flag,
        cast(below_safety_stock_flag as boolean) as below_safety_stock_flag,
        cast(reorder_triggered_flag as boolean) as reorder_triggered_flag,
        cast(units_on_order as integer) as units_on_order,
        least(cast(days_of_supply as decimal(5,2)), 99.99) as days_of_supply,
        cast(holding_cost as decimal(10,2)) as holding_cost,
        cast(inventory_value as decimal(12,2)) as inventory_value,

        -- Derived: net stock movement for the day
        (units_received + units_returned - units_sold) as net_stock_movement,

        -- Derived: inventory health status
        case
            when stockout_flag = true then 'Stockout'
            when below_safety_stock_flag = true then 'Critical'
            when days_of_supply < 7 then 'Low'
            when days_of_supply < 30 then 'Healthy'
            else 'Overstocked'
        end as inventory_health,

        -- Audit
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        cast(batch_id as varchar(50)) as batch_id

    from source
    where closing_stock >= 0
)

select * from transformed