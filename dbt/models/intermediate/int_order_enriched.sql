/*
    int_order_enriched — Intermediate layer
    Enriches orders with customer segment, warehouse region, item-level aggregates.
    This is the "one big table" for order analysis before final mart aggregation.
*/

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
    where is_current = true
),

warehouses as (
    select * from {{ ref('stg_warehouses') }}
),

nearest_warehouses as (
    select warehouse_id, region as nearest_region
    from {{ ref('stg_warehouses') }}
),

order_items_agg as (
    select
        order_id,
        count(*) as line_item_count,
        sum(quantity) as total_quantity,
        sum(revenue) as total_revenue,
        sum(discount_amount) as total_discount,
        avg(discount_pct) as avg_discount_pct,
        sum(case when has_discount = true then 1 else 0 end) as discounted_items
    from {{ ref('stg_order_items') }}
    group by 1
)

select
    o.order_id,
    o.order_date,
    o.order_timestamp,
    o.customer_id,
    c.customer_segment,
    c.engagement_tier,
    c.region as customer_region,
    o.assigned_warehouse_id,
    w.warehouse_name as assigned_warehouse_name,
    w.region as assigned_region,
    o.nearest_warehouse_id,
    nw.nearest_region,
    o.allocation_strategy,
    o.order_priority,
    o.total_items,
    o.total_amount,
    o.total_fulfillment_cost,
    o.order_status,
    o.return_flag,
    o.is_cross_region,
    o.order_value_tier,
    o.experiment_id,
    o.experiment_group,
    o.is_experiment_order,

    -- From item aggregation
    coalesce(oi.line_item_count, 0) as line_item_count,
    coalesce(oi.total_quantity, 0) as total_quantity,
    coalesce(oi.total_revenue, 0) as item_total_revenue,
    coalesce(oi.total_discount, 0) as total_discount,
    coalesce(oi.avg_discount_pct, 0) as avg_discount_pct,

    -- Derived: was the allocation optimal?
    case
        when o.assigned_warehouse_id = o.nearest_warehouse_id then 'Nearest'
        when w.region = nw.nearest_region then 'Same Region'
        else 'Cross Region'
    end as allocation_result,

    o.batch_id

from orders o
left join customers c on o.customer_id = c.customer_id
left join warehouses w on o.assigned_warehouse_id = w.warehouse_id
left join nearest_warehouses nw on o.nearest_warehouse_id = nw.warehouse_id
left join order_items_agg oi on o.order_id = oi.order_id