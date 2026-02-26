/*
    mart_allocation_efficiency — Gold layer
    Source: Intermediate enriched models
    Page: 5 (Warehouse Allocation Intelligence)
    Grain: One row per warehouse per day
    Incremental: merge on [date, warehouse_id]
*/

{{ config(
    materialized='incremental',
    unique_key=['date', 'warehouse_id'],
    incremental_strategy='merge'
) }}

with orders as (
    select * from {{ ref('int_order_enriched') }}
    {% if is_incremental() %}
    where order_date > (select max(date) from {{ this }})
    {% endif %}
),

deliveries as (
    select * from {{ ref('int_delivery_enriched') }}
    {% if is_incremental() %}
    where delivery_date > (select max(date) from {{ this }})
    {% endif %}
),

daily_allocation as (
    select
        order_date as date,
        assigned_warehouse_id as warehouse_id,
        count(*) as total_orders_assigned,
        count(case when assigned_warehouse_id = nearest_warehouse_id then 1 end) as orders_from_nearest,
        count(case when is_cross_region = true then 1 end) as orders_redirected_in,
        count(case when allocation_result = 'Cross Region' then 1 end) as cross_region_orders
    from orders
    where order_status != 'Cancelled'
    group by 1, 2
),

redirected_out as (
    select
        order_date as date,
        nearest_warehouse_id as warehouse_id,
        count(case when assigned_warehouse_id != nearest_warehouse_id then 1 end) as orders_redirected_out
    from orders
    where order_status != 'Cancelled'
    group by 1, 2
),

daily_delivery_stats as (
    select
        delivery_date as date,
        warehouse_id,
        avg(distance_km) as avg_delivery_distance_km,
        avg(delivery_cost) as avg_cost_per_order
    from deliveries
    group by 1, 2
)

select
    a.date,
    a.warehouse_id,
    a.total_orders_assigned,
    a.orders_from_nearest,
    a.orders_redirected_in,
    coalesce(ro.orders_redirected_out, 0) as orders_redirected_out,
    case when a.total_orders_assigned > 0 then round(a.orders_from_nearest * 100.0 / a.total_orders_assigned, 2) else 0 end as nearest_assignment_rate,
    round(coalesce(ds.avg_delivery_distance_km, 0), 2) as avg_delivery_distance_km,
    round(coalesce(ds.avg_cost_per_order, 0), 2) as avg_cost_per_order,
    case when a.total_orders_assigned > 0 then round(a.cross_region_orders * 100.0 / a.total_orders_assigned, 2) else 0 end as cross_region_pct

from daily_allocation a
left join redirected_out ro on a.date = ro.date and a.warehouse_id = ro.warehouse_id
left join daily_delivery_stats ds on a.date = ds.date and a.warehouse_id = ds.warehouse_id