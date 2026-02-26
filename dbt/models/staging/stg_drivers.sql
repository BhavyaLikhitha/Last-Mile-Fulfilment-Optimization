/*
    stg_drivers — Silver layer
    Source: SCD Type 2 snapshot (tracks warehouse transfers and status changes)
    Transformations:
      - Reads from snapshot for full history
      - Adds driver_experience_years derived column
      - Adds capacity_tier classification
*/

with snapshot as (
    select * from {{ ref('snap_driver') }}
),

transformed as (
    select
        driver_id,
        cast(warehouse_id as varchar(20)) as warehouse_id,
        cast(driver_name as varchar(100)) as driver_name,
        cast(vehicle_type as varchar(20)) as vehicle_type,
        cast(max_delivery_capacity as integer) as max_delivery_capacity,
        cast(avg_speed_kmh as decimal(5,2)) as avg_speed_kmh,
        cast(availability_status as varchar(20)) as availability_status,
        cast(hire_date as date) as hire_date,

        -- Derived: years of experience
        round(datediff(day, hire_date, coalesce(dbt_valid_to, current_date())) / 365.25, 1) as experience_years,

        -- Derived: capacity tier
        case
            when max_delivery_capacity >= 25 then 'High'
            when max_delivery_capacity >= 15 then 'Medium'
            else 'Low'
        end as capacity_tier,

        -- Derived: speed tier
        case
            when avg_speed_kmh >= 45 then 'Fast'
            when avg_speed_kmh >= 30 then 'Medium'
            else 'Slow'
        end as speed_tier,

        -- SCD Type 2 metadata
        dbt_valid_from,
        dbt_valid_to,
        case when dbt_valid_to is null then true else false end as is_current

    from snapshot
)

select * from transformed