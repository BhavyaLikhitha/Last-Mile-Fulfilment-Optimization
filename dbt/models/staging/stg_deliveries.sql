/*
    stg_deliveries — Silver layer
    Source: RAW fact_deliveries
    Transformations:
      - Type casting
      - Adds eta_accuracy_pct (how accurate was the estimate)
      - Adds delivery_speed_tier
      - Adds cost_per_km metric
      - Recalculates SLA breach for consistency
*/

with source as (
    select * from {{ source('raw', 'fact_deliveries') }}
),

transformed as (
    select
        cast(delivery_id as varchar(30)) as delivery_id,
        cast(order_id as varchar(30)) as order_id,
        cast(driver_id as varchar(20)) as driver_id,
        cast(warehouse_id as varchar(20)) as warehouse_id,
        cast(assigned_time as timestamp) as assigned_time,
        cast(pickup_time as timestamp) as pickup_time,
        cast(delivered_time as timestamp) as delivered_time,
        cast(estimated_eta_minutes as decimal(6,2)) as estimated_eta_minutes,
        cast(actual_delivery_minutes as decimal(6,2)) as actual_delivery_minutes,
        cast(distance_km as decimal(8,2)) as distance_km,
        cast(delivery_cost as decimal(10,2)) as delivery_cost,
        cast(delivery_status as varchar(20)) as delivery_status,
        cast(on_time_flag as boolean) as on_time_flag,
        cast(sla_minutes as integer) as sla_minutes,

        -- Derived: recalculated SLA breach
        case
            when actual_delivery_minutes is not null and actual_delivery_minutes > sla_minutes then true
            else false
        end as sla_breach_flag,

        -- Derived: ETA accuracy (how close was estimate to actual)
        case
            when estimated_eta_minutes > 0 and actual_delivery_minutes is not null
            then round(100 - abs(actual_delivery_minutes - estimated_eta_minutes) / estimated_eta_minutes * 100, 2)
            else null
        end as eta_accuracy_pct,

        -- Derived: ETA error in minutes
        case
            when actual_delivery_minutes is not null
            then round(actual_delivery_minutes - estimated_eta_minutes, 2)
            else null
        end as eta_error_minutes,

        -- Derived: delivery speed tier
        case
            when actual_delivery_minutes is not null and actual_delivery_minutes <= 30 then 'Express'
            when actual_delivery_minutes is not null and actual_delivery_minutes <= 60 then 'Standard'
            when actual_delivery_minutes is not null and actual_delivery_minutes <= 120 then 'Slow'
            when actual_delivery_minutes is not null then 'Very Slow'
            else null
        end as delivery_speed_tier,

        -- Derived: cost per km
        case when distance_km > 0 then round(delivery_cost / distance_km, 2) else 0 end as cost_per_km,

        -- Audit
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        cast(batch_id as varchar(50)) as batch_id

    from source
    where delivery_status is not null
)

select * from transformed