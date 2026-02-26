/*
    int_delivery_enriched — Intermediate layer
    Enriches deliveries with driver info, order priority, warehouse details.
    Single source for all delivery analysis before mart aggregation.
*/

with deliveries as (
    select * from {{ ref('stg_deliveries') }}
),

drivers as (
    select * from {{ ref('stg_drivers') }}
    where is_current = true
),

orders as (
    select
        order_id,
        order_priority,
        customer_id,
        order_value_tier
    from {{ ref('stg_orders') }}
),

warehouses as (
    select warehouse_id, warehouse_name, region
    from {{ ref('stg_warehouses') }}
)

select
    d.delivery_id,
    d.order_id,
    d.driver_id,
    d.warehouse_id,
    w.warehouse_name,
    w.region as warehouse_region,
    d.assigned_time,
    d.pickup_time,
    d.delivered_time,
    d.estimated_eta_minutes,
    d.actual_delivery_minutes,
    d.distance_km,
    d.delivery_cost,
    d.delivery_status,
    d.on_time_flag,
    d.sla_minutes,
    d.sla_breach_flag,
    d.eta_accuracy_pct,
    d.eta_error_minutes,
    d.delivery_speed_tier,
    d.cost_per_km,

    -- From driver
    dr.driver_name,
    dr.vehicle_type,
    dr.capacity_tier as driver_capacity_tier,
    dr.speed_tier as driver_speed_tier,
    dr.experience_years as driver_experience_years,

    -- From order
    o.order_priority,
    o.customer_id,
    o.order_value_tier,

    -- Derived: pickup wait time (minutes)
    case
        when d.pickup_time is not null and d.assigned_time is not null
        then datediff(minute, d.assigned_time, d.pickup_time)
        else null
    end as pickup_wait_minutes,

    -- Derived: date for aggregation
    cast(d.assigned_time as date) as delivery_date,

    d.batch_id

from deliveries d
left join drivers dr on d.driver_id = dr.driver_id
left join orders o on d.order_id = o.order_id
left join warehouses w on d.warehouse_id = w.warehouse_id