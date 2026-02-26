/*
    stg_driver_activity — Silver layer
    Source: RAW fact_driver_activity
    Transformations:
      - Type casting
      - Adds productivity_tier
      - Adds avg_distance_per_delivery
      - Validates utilization within 0-100 range
*/

with source as (
    select * from {{ source('raw', 'fact_driver_activity') }}
),

transformed as (
    select
        cast(driver_id as varchar(20)) as driver_id,
        cast(activity_date as date) as activity_date,
        cast(warehouse_id as varchar(20)) as warehouse_id,
        cast(deliveries_completed as integer) as deliveries_completed,
        cast(total_distance_km as decimal(8,2)) as total_distance_km,
        cast(total_active_hours as decimal(5,2)) as total_active_hours,
        cast(idle_hours as decimal(5,2)) as idle_hours,
        least(cast(utilization_pct as decimal(5,2)), 100.0) as utilization_pct,

        -- Derived: average distance per delivery
        case
            when deliveries_completed > 0 then round(total_distance_km / deliveries_completed, 2)
            else 0
        end as avg_distance_per_delivery,

        -- Derived: deliveries per hour
        case
            when total_active_hours > 0 then round(deliveries_completed / total_active_hours, 2)
            else 0
        end as deliveries_per_hour,

        -- Derived: productivity tier
        case
            when utilization_pct >= 80 then 'High'
            when utilization_pct >= 50 then 'Medium'
            else 'Low'
        end as productivity_tier,

        -- Audit
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        cast(batch_id as varchar(50)) as batch_id

    from source
    where deliveries_completed >= 0
)

select * from transformed