/*
    mart_daily_warehouse_kpis — Gold layer
    Source: Intermediate enriched models
    Pages: 1 (Executive Overview), 5 (Warehouse Allocation)
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

inventory as (
    select * from {{ ref('int_inventory_enriched') }}
    {% if is_incremental() %}
    where snapshot_date > (select max(date) from {{ this }})
    {% endif %}
),

shipments as (
    select * from {{ ref('stg_shipments') }}
    {% if is_incremental() %}
    where shipment_date > (select max(date) from {{ this }})
    {% endif %}
),

warehouses as (
    select * from {{ ref('stg_warehouses') }}
),

daily_orders as (
    select
        order_date as date,
        assigned_warehouse_id as warehouse_id,
        count(*) as total_orders,
        sum(total_amount) as total_revenue,
        count(case when return_flag = true then 1 end) as return_count,
        count(case when is_experiment_order = true then 1 end) as experiment_orders
    from orders
    where order_status != 'Cancelled'
    group by 1, 2
),

daily_deliveries as (
    select
        delivery_date as date,
        warehouse_id,
        avg(case when delivery_status = 'Delivered' then actual_delivery_minutes end) as avg_delivery_time_min,
        sum(delivery_cost) as total_delivery_cost,
        count(case when delivery_status = 'Delivered' and on_time_flag = true then 1 end) as on_time_count,
        count(case when delivery_status = 'Delivered' then 1 end) as delivered_count,
        count(case when sla_breach_flag = true then 1 end) as sla_breach_count,
        count(*) as total_delivery_records
    from deliveries
    group by 1, 2
),

daily_inventory as (
    select
        snapshot_date as date,
        warehouse_id,
        sum(holding_cost) as total_holding_cost,
        sum(inventory_value) as total_inventory_value,
        count(case when stockout_flag = true then 1 end) as stockout_products,
        count(*) as total_products,
        sum(closing_stock) as total_closing_stock
    from inventory
    group by 1, 2
),

daily_shipments as (
    select
        shipment_date as date,
        warehouse_id,
        sum(shipment_cost) as total_shipment_cost
    from shipments
    group by 1, 2
)

select
    o.date,
    o.warehouse_id,
    o.total_orders,
    round(o.total_revenue, 2) as total_revenue,
    round(coalesce(i.total_holding_cost, 0) + coalesce(d.total_delivery_cost, 0) + coalesce(s.total_shipment_cost, 0) + w.operating_cost_per_day, 2) as total_cost,
    case when d.delivered_count > 0 then round(d.on_time_count * 100.0 / d.delivered_count, 2) else 0 end as service_level_pct,
    case when i.total_products > 0 then round(i.stockout_products * 1.0 / i.total_products, 4) else 0 end as stockout_rate,
    round(d.avg_delivery_time_min, 2) as avg_delivery_time_min,
    case when w.capacity_units > 0 then round(i.total_closing_stock * 100.0 / w.capacity_units, 2) else 0 end as capacity_utilization_pct,
    round(coalesce(i.total_holding_cost, 0), 2) as total_holding_cost,
    round(coalesce(d.total_delivery_cost, 0), 2) as total_delivery_cost,
    round(coalesce(s.total_shipment_cost, 0), 2) as total_shipment_cost,
    round(coalesce(i.total_inventory_value, 0), 2) as total_inventory_value,
    case when d.total_delivery_records > 0 then round(d.sla_breach_count * 1.0 / d.total_delivery_records, 4) else 0 end as sla_breach_rate,
    coalesce(o.return_count, 0) as return_count,
    coalesce(o.experiment_orders, 0) as experiment_orders

from daily_orders o
left join daily_deliveries d on o.date = d.date and o.warehouse_id = d.warehouse_id
left join daily_inventory i on o.date = i.date and o.warehouse_id = i.warehouse_id
left join daily_shipments s on o.date = s.date and o.warehouse_id = s.warehouse_id
left join warehouses w on o.warehouse_id = w.warehouse_id