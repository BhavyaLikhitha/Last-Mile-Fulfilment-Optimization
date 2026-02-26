/*
    mart_cost_optimization — Gold layer
    Source: Intermediate enriched models
    Pages: 1 (Executive Overview), 5 (Warehouse Allocation)
    Grain: One row per warehouse per day
    Incremental: merge on [date, warehouse_id]
    Note: optimized_* columns are placeholders (8% savings estimate)
          Updated by optimization engine with real values later
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

warehouses as (
    select * from {{ ref('stg_warehouses') }}
),

daily_holding as (
    select
        snapshot_date as date,
        warehouse_id,
        sum(holding_cost) as holding_cost
    from inventory
    group by 1, 2
),

daily_transport as (
    select
        delivery_date as date,
        warehouse_id,
        sum(delivery_cost) as transport_cost
    from deliveries
    group by 1, 2
),

daily_allocation_rate as (
    select
        order_date as date,
        assigned_warehouse_id as warehouse_id,
        count(*) as total,
        count(case when assigned_warehouse_id = nearest_warehouse_id then 1 end) as nearest_count
    from orders
    where order_status != 'Cancelled'
    group by 1, 2
)

select
    coalesce(h.date, t.date) as date,
    coalesce(h.warehouse_id, t.warehouse_id) as warehouse_id,
    round(coalesce(h.holding_cost, 0) + coalesce(t.transport_cost, 0) + w.operating_cost_per_day, 2) as baseline_total_cost,
    round((coalesce(h.holding_cost, 0) + coalesce(t.transport_cost, 0) + w.operating_cost_per_day) * 0.92, 2) as optimized_total_cost,
    round((coalesce(h.holding_cost, 0) + coalesce(t.transport_cost, 0) + w.operating_cost_per_day) * 0.08, 2) as savings_amount,
    8.00 as savings_pct,
    round(coalesce(h.holding_cost, 0), 2) as holding_cost_baseline,
    round(coalesce(h.holding_cost, 0) * 0.90, 2) as holding_cost_optimized,
    round(coalesce(t.transport_cost, 0), 2) as transport_cost_baseline,
    round(coalesce(t.transport_cost, 0) * 0.93, 2) as transport_cost_optimized,
    case when a.total > 0 then round(a.nearest_count * 100.0 / a.total, 2) else 0 end as allocation_efficiency_pct

from daily_holding h
full outer join daily_transport t on h.date = t.date and h.warehouse_id = t.warehouse_id
left join warehouses w on coalesce(h.warehouse_id, t.warehouse_id) = w.warehouse_id
left join daily_allocation_rate a on coalesce(h.date, t.date) = a.date and coalesce(h.warehouse_id, t.warehouse_id) = a.warehouse_id