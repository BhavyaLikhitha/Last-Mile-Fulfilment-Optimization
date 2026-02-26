/*
    stg_warehouses — Silver layer
    Source: RAW (no SCD — warehouse config is static)
    Transformations:
      - Type casting
      - Adds capacity_tier classification
      - Adds cost_tier classification
*/

with source as (
    select * from {{ source('raw', 'dim_warehouse') }}
),

transformed as (
    select
        cast(warehouse_id as varchar(20)) as warehouse_id,
        cast(warehouse_name as varchar(100)) as warehouse_name,
        cast(region as varchar(50)) as region,
        cast(city as varchar(50)) as city,
        cast(state as varchar(10)) as state,
        cast(latitude as decimal(9,6)) as latitude,
        cast(longitude as decimal(9,6)) as longitude,
        cast(capacity_units as integer) as capacity_units,
        cast(operating_cost_per_day as decimal(10,2)) as operating_cost_per_day,
        cast(is_active as boolean) as is_active,

        -- Derived: capacity tier
        case
            when capacity_units >= 75000 then 'Large'
            when capacity_units >= 65000 then 'Medium'
            else 'Small'
        end as capacity_tier,

        -- Derived: cost tier
        case
            when operating_cost_per_day >= 4500 then 'High Cost'
            when operating_cost_per_day >= 3800 then 'Medium Cost'
            else 'Low Cost'
        end as cost_tier

    from source
    where is_active = true
)

select * from transformed